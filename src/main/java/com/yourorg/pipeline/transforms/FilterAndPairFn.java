package com.yourorg.pipeline.transforms;

import com.google.api.services.bigquery.model.TableRow;
import com.yourorg.pipeline.util.AvroSchemas;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Core eligibility and pairing transform.
 *
 * <p>One human payload is matched against every AI iteration for the same
 * {@code image_name}. Each AI iteration produces an independent MATCHED pair
 * and therefore an independent set of comparison rows in the output table.
 *
 * <p>Pairing rules (per {@code image_name} per run):
 * <ul>
 *   <li><b>Human + N AI iterations present</b> → emit N MATCHED pairs, one per AI
 *       iteration (sorted by {@code created_at}; iteration number = sort position).
 *       Human is re-pended so late-arriving AI iterations in future runs can still
 *       match against it.</li>
 *   <li><b>Human only</b> → pend human; wait for AI iterations.</li>
 *   <li><b>AI iterations only</b> → restored pending human (if present) is matched;
 *       otherwise pend each AI iteration independently and wait for human.</li>
 *   <li><b>Pending row(s) aged out (≥ MAX_WAIT_DAYS)</b> → route to dead-letter.</li>
 * </ul>
 *
 * <p>Multiple pending AI rows per image are supported. Each carries its own
 * {@code first_seen_at} and {@code retry_count}. Aging is evaluated per iteration.
 *
 * <p>Source payloads are Barricade-encrypted. {@code image_name} is already the
 * CoGroupByKey grouping key (extracted once upstream). Encrypted payloads are stored
 * as-is in {@link GenericRecord}; decryption happens in {@link FlattenAndCompareFn}.
 */
public class FilterAndPairFn
        extends DoFn<KV<String, CoGbkResult>,
                     KV<String, KV<GenericRecord, GenericRecord>>> {

    private static final Logger LOG = LoggerFactory.getLogger(FilterAndPairFn.class);

    public static final int MAX_WAIT_DAYS = 7;

    private final String aiMethod;
    private final String humanMethod;

    public FilterAndPairFn(String aiMethod, String humanMethod) {
        this.aiMethod    = aiMethod;
        this.humanMethod = humanMethod;
    }

    // ── CoGroupByKey input tags ───────────────────────────────────────────────

    public static final TupleTag<TableRow> SOURCE_TAG  = new TupleTag<>() {};
    public static final TupleTag<TableRow> PENDING_TAG = new TupleTag<>() {};

    // ── Output tags ──────────────────────────────────────────────────────────

    public static final TupleTag<KV<String, KV<GenericRecord, GenericRecord>>> MATCHED
            = new TupleTag<>() {};
    public static final TupleTag<GenericRecord> NEW_PENDING = new TupleTag<>() {};
    public static final TupleTag<GenericRecord> AGED_OUT    = new TupleTag<>() {};

    @ProcessElement
    public void processElement(ProcessContext ctx) {
        String imageId = ctx.element().getKey();
        CoGbkResult result = ctx.element().getValue();

        // ── Partition source rows by type ────────────────────────────────────
        List<GenericRecord> humanRows = new ArrayList<>();
        List<GenericRecord> aiRows    = new ArrayList<>();

        for (TableRow row : result.getAll(SOURCE_TAG)) {
            String encryptedPayload = (String) row.get("payload");
            String keyId            = (String) row.get("key_id");
            String method           = (String) row.get("method");

            String payloadType;
            if (aiMethod.equals(method)) {
                payloadType = "ai";
            } else if (humanMethod.equals(method)) {
                payloadType = "human";
            } else {
                LOG.warn("Unrecognised method '{}' for imageId={} — skipping", method, imageId);
                continue;
            }

            GenericRecord p = newPayloadRow(imageId, keyId, payloadType,
                    encryptedPayload, (String) row.get("created_at"));
            if ("human".equals(payloadType)) {
                humanRows.add(p);
            } else {
                aiRows.add(p);
            }
        }

        if (humanRows.size() > 1) {
            LOG.warn("imageId={} has {} human rows — using the earliest by created_at",
                    imageId, humanRows.size());
            humanRows.sort(Comparator.comparing(r -> str(r.get("created_at"))));
        }

        // ── Restore pending rows ──────────────────────────────────────────────
        // Key: "type:created_at" → original pending GenericRecord
        // Used to carry forward first_seen_at and retry_count per iteration.
        Map<String, GenericRecord> pendingMeta = new HashMap<>();

        for (TableRow pendingRow : result.getAll(PENDING_TAG)) {
            GenericRecord p     = toPendingRecord(pendingRow);
            String pendingType  = str(p.get("pending_type"));
            String pendingKeyId = str(p.get("key_id"));
            String createdAt    = str(p.get("created_at"));

            pendingMeta.put(pendingType + ":" + createdAt, p);

            if ("human".equals(pendingType) && humanRows.isEmpty()) {
                humanRows.add(newPayloadRow(imageId, pendingKeyId, "human",
                        str(p.get("payload")), createdAt));
                LOG.debug("Restored pending human payload for imageId={}", imageId);
            } else if ("ai".equals(pendingType)) {
                // Always restore every pending AI iteration — each is independent.
                aiRows.add(newPayloadRow(imageId, pendingKeyId, "ai",
                        str(p.get("payload")), createdAt));
                LOG.debug("Restored pending AI iteration (created_at={}) for imageId={}",
                        createdAt, imageId);
            }
        }

        Instant now          = Instant.now();
        boolean humanPresent = !humanRows.isEmpty();
        boolean anyAiPresent = !aiRows.isEmpty();

        // Sort AI rows by created_at for stable iteration numbering across all states.
        aiRows.sort(Comparator.comparing(r -> str(r.get("created_at"))));

        if (humanPresent && anyAiPresent) {
            // ── State 1: human + N AI iterations → emit one MATCHED per AI ──
            GenericRecord human = humanRows.get(0);
            for (int i = 0; i < aiRows.size(); i++) {
                String pairKey = imageId + "::" + (i + 1);
                ctx.output(MATCHED, KV.of(pairKey, KV.of(human, aiRows.get(i))));
            }
            LOG.info("Matched imageId={} — {} AI iteration(s) emitted for comparison",
                    imageId, aiRows.size());

            // Keep human in pending so late-arriving AI iterations in future runs
            // can still be matched against it (up to MAX_WAIT_DAYS).
            String        humanCreatedAt = str(human.get("created_at"));
            GenericRecord humanMeta      = pendingMeta.get("human:" + humanCreatedAt);
            Instant       firstSeen      = metaFirstSeen(humanMeta, now);
            long          retryCount     = metaRetryCount(humanMeta);
            long          daysWaited     = ChronoUnit.DAYS.between(firstSeen, now);
            emitPendingOrAgedOut(ctx, imageId, str(human.get("key_id")), "human",
                    str(human.get("payload")), humanCreatedAt,
                    firstSeen, now, retryCount, daysWaited);

        } else if (humanPresent) {
            // ── State 2: human present, no AI yet → pend human ──────────────
            GenericRecord human     = humanRows.get(0);
            String        createdAt = str(human.get("created_at"));
            GenericRecord meta      = pendingMeta.get("human:" + createdAt);
            Instant firstSeen       = metaFirstSeen(meta, now);
            long retryCount         = metaRetryCount(meta);
            long daysWaited         = ChronoUnit.DAYS.between(firstSeen, now);
            LOG.info("Human-only imageId={} — pending AI (days waited={})", imageId, daysWaited);
            emitPendingOrAgedOut(ctx, imageId, str(human.get("key_id")), "human",
                    str(human.get("payload")), createdAt,
                    firstSeen, now, retryCount, daysWaited);

        } else if (anyAiPresent) {
            // ── State 3: one or more AI iterations, no human → pend each ────
            for (int i = 0; i < aiRows.size(); i++) {
                GenericRecord ai        = aiRows.get(i);
                String        createdAt = str(ai.get("created_at"));
                GenericRecord meta      = pendingMeta.get("ai:" + createdAt);
                Instant firstSeen       = metaFirstSeen(meta, now);
                long retryCount         = metaRetryCount(meta);
                long daysWaited         = ChronoUnit.DAYS.between(firstSeen, now);
                LOG.info("AI-only imageId={} (created_at={}) — pending human (days waited={})",
                        imageId, createdAt, daysWaited);
                emitPendingOrAgedOut(ctx, imageId, str(ai.get("key_id")), "ai",
                        str(ai.get("payload")), createdAt,
                        firstSeen, now, retryCount, daysWaited);
            }

        } else if (!pendingMeta.isEmpty()) {
            // ── State 4: no source rows, pending rows exist but unrestorable ─
            // (e.g. duplicate human rows discarded above). Age out if stale.
            for (GenericRecord p : pendingMeta.values()) {
                Instant firstSeen = metaFirstSeen(p, now);
                long daysWaited   = ChronoUnit.DAYS.between(firstSeen, now);
                if (daysWaited >= MAX_WAIT_DAYS) {
                    LOG.warn("imageId={} pending row (type={}) aged out after {} days — dead-letter",
                            imageId, str(p.get("pending_type")), daysWaited);
                    ctx.output(AGED_OUT, newPendingRow(imageId,
                            str(p.get("key_id")), str(p.get("pending_type")),
                            str(p.get("payload")), str(p.get("created_at")),
                            firstSeen, now, metaRetryCount(p)));
                }
            }
        }
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private void emitPendingOrAgedOut(ProcessContext ctx,
                                       String imageId, String keyId, String pendingType,
                                       String payload, String createdAt,
                                       Instant firstSeen, Instant now,
                                       long retryCount, long daysWaited) {
        GenericRecord row = newPendingRow(imageId, keyId, pendingType, payload, createdAt,
                firstSeen, now, retryCount);
        if (daysWaited >= MAX_WAIT_DAYS) {
            ctx.output(AGED_OUT, row);
        } else {
            ctx.output(NEW_PENDING, row);
        }
    }

    /** Returns {@code first_seen_at} from a pending meta record, or {@code fallback} if absent. */
    private static Instant metaFirstSeen(GenericRecord meta, Instant fallback) {
        if (meta == null) return fallback;
        Instant parsed = parseInstant(str(meta.get("first_seen_at")));
        return parsed != null ? parsed : fallback;
    }

    /** Returns {@code retry_count + 1} from a pending meta record, or 0 for a new row. */
    private static long metaRetryCount(GenericRecord meta) {
        if (meta == null) return 0L;
        Object count = meta.get("retry_count");
        return count != null ? ((Number) count).longValue() + 1 : 0L;
    }

    private static GenericRecord newPayloadRow(String imageId, String keyId,
                                                String payloadType, String payload,
                                                String createdAt) {
        GenericRecord r = new GenericData.Record(AvroSchemas.PAYLOAD_ROW);
        r.put("image_id",     imageId);
        r.put("key_id",       keyId);
        r.put("payload_type", payloadType);
        r.put("payload",      payload);
        r.put("created_at",   createdAt);
        return r;
    }

    private static GenericRecord newPendingRow(String imageId, String keyId,
                                                String pendingType, String payload,
                                                String createdAt, Instant firstSeen,
                                                Instant now, long retryCount) {
        GenericRecord r = new GenericData.Record(AvroSchemas.PENDING_ROW);
        r.put("image_id",        imageId);
        r.put("key_id",          keyId);
        r.put("pending_type",    pendingType);
        r.put("payload",         payload);
        r.put("created_at",      createdAt);
        r.put("first_seen_at",   firstSeen != null ? firstSeen.toString() : null);
        r.put("last_retried_at", now.toString());
        r.put("retry_count",     retryCount);
        return r;
    }

    private static GenericRecord toPendingRecord(TableRow row) {
        GenericRecord r = new GenericData.Record(AvroSchemas.PENDING_ROW);
        r.put("image_id",        row.get("image_id"));
        r.put("key_id",          row.get("key_id"));
        r.put("pending_type",    row.get("pending_type"));
        r.put("payload",         row.get("payload"));
        r.put("created_at",      row.get("created_at"));
        r.put("first_seen_at",   row.get("first_seen_at") != null
                ? row.get("first_seen_at").toString() : null);
        r.put("last_retried_at", row.get("last_retried_at") != null
                ? row.get("last_retried_at").toString() : null);
        r.put("retry_count",     row.get("retry_count") != null
                ? ((Number) row.get("retry_count")).longValue() : 0L);
        return r;
    }

    private static String str(Object value) {
        return value != null ? value.toString() : null;
    }

    private static Instant parseInstant(String value) {
        if (value == null) return null;
        try {
            return Instant.parse(value);
        } catch (Exception e) {
            return null;
        }
    }
}
