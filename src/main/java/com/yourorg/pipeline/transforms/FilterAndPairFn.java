package com.yourorg.pipeline.transforms;

import com.google.api.services.bigquery.model.TableRow;
import com.yourorg.pipeline.util.TimestampUtil;
import org.apache.avro.Schema;
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
 * <p>Schemas for {@link GenericRecord} construction are injected via the constructor
 * rather than accessed from a static registry, keeping the DoFn self-contained and
 * testable without classpath schema files.
 */
public class FilterAndPairFn
        extends DoFn<KV<String, CoGbkResult>,
                     KV<String, KV<GenericRecord, GenericRecord>>> {

    private static final Logger LOG = LoggerFactory.getLogger(FilterAndPairFn.class);

    public static final int MAX_WAIT_DAYS = 7;

    private final String aiMethod;
    private final String humanMethod;
    private final Schema payloadSchema;
    private final Schema pendingSchema;

    /**
     * @param aiMethod      value of the {@code method} column identifying an AI row
     * @param humanMethod   value of the {@code method} column identifying a human row
     * @param payloadSchema Avro schema for intermediate {@code PayloadRow} records
     * @param pendingSchema Avro schema for intermediate {@code PendingRow} records
     */
    public FilterAndPairFn(String aiMethod,
                            String humanMethod,
                            Schema payloadSchema,
                            Schema pendingSchema) {
        this.aiMethod      = aiMethod;
        this.humanMethod   = humanMethod;
        this.payloadSchema = payloadSchema;
        this.pendingSchema = pendingSchema;
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

            // Normalize created_at on ingestion — source BQ rows may use
            // "YYYY-MM-DD HH:MM:SS UTC" or other formats; we write RFC 3339.
            String createdAtNorm = TimestampUtil.normalizeTimestamp((String) row.get("created_at"));
            GenericRecord p = newPayloadRow(imageId, keyId, payloadType,
                    encryptedPayload, createdAtNorm);
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
            for (GenericRecord ai : aiRows) {
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

    private static Instant metaFirstSeen(GenericRecord meta, Instant fallback) {
        if (meta == null) return fallback;
        Instant parsed = parseInstant(str(meta.get("first_seen_at")));
        return parsed != null ? parsed : fallback;
    }

    private static long metaRetryCount(GenericRecord meta) {
        if (meta == null) return 0L;
        Object count = meta.get("retry_count");
        return count != null ? ((Number) count).longValue() + 1 : 0L;
    }

    private GenericRecord newPayloadRow(String imageId, String keyId,
                                         String payloadType, String payload,
                                         String createdAt) {
        GenericRecord r = new GenericData.Record(payloadSchema);
        r.put("image_id",     imageId);
        r.put("key_id",       keyId);
        r.put("payload_type", payloadType);
        r.put("payload",      payload);
        r.put("created_at",   createdAt);
        return r;
    }

    private GenericRecord newPendingRow(String imageId, String keyId,
                                         String pendingType, String payload,
                                         String createdAt, Instant firstSeen,
                                         Instant now, long retryCount) {
        GenericRecord r = new GenericData.Record(pendingSchema);
        r.put("image_id",        imageId);
        r.put("key_id",          keyId);
        r.put("pending_type",    pendingType);
        r.put("payload",         payload);
        r.put("created_at",      createdAt);
        r.put("first_seen_at",   TimestampUtil.formatInstant(firstSeen));
        r.put("last_retried_at", TimestampUtil.formatInstant(now));
        r.put("retry_count",     retryCount);
        return r;
    }

    private GenericRecord toPendingRecord(TableRow row) {
        GenericRecord r = new GenericData.Record(pendingSchema);
        r.put("image_id",        row.get("image_id"));
        r.put("key_id",          row.get("key_id"));
        r.put("pending_type",    row.get("pending_type"));
        r.put("payload",         row.get("payload"));
        r.put("created_at",      TimestampUtil.normalizeTimestamp(str(row.get("created_at"))));
        r.put("first_seen_at",   TimestampUtil.normalizeTimestamp(str(row.get("first_seen_at"))));
        r.put("last_retried_at", TimestampUtil.normalizeTimestamp(str(row.get("last_retried_at"))));
        r.put("retry_count",     parseLong(row.get("retry_count")));
        return r;
    }

    /**
     * Safely converts a BigQuery cell value to a {@code long}.
     *
     * <p>BigQuery's {@code readTableRows()} returns INTEGER columns as {@link String},
     * not as {@link Number}, so a direct cast fails on the second (and subsequent) runs
     * when the pending row is round-tripped through BQ.  This helper handles both types.
     */
    private static long parseLong(Object value) {
        if (value == null) return 0L;
        if (value instanceof Number) return ((Number) value).longValue();
        try {
            return Long.parseLong(value.toString().trim());
        } catch (NumberFormatException e) {
            LOG.warn("Could not parse retry_count '{}' as long — defaulting to 0", value);
            return 0L;
        }
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
