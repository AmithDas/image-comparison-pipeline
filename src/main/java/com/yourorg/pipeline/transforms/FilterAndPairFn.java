package com.yourorg.pipeline.transforms;

import com.google.api.services.bigquery.model.TableRow;
import com.yourorg.pipeline.config.SegmentConfig;
import com.yourorg.pipeline.util.TimestampUtil;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.options.ValueProvider;
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
 * <p>Input key format: {@code "imageId::segment"}.  One human payload is matched
 * against every AI iteration for the same {@code imageId} within the same
 * {@code segment}.  Each AI iteration produces an independent MATCHED pair.
 *
 * <p>Pairing rules (per key per run):
 * <ul>
 *   <li><b>Human + N AI iterations present</b> → emit N MATCHED pairs (sorted by
 *       {@code created_at}; iteration number = sort position).
 *       Human is re-pended so late-arriving AI iterations can still match it.</li>
 *   <li><b>Human only</b> → pend human; wait for AI iterations.</li>
 *   <li><b>AI iterations only</b> → restored pending human (if present) is matched;
 *       otherwise pend each AI iteration and wait for human.</li>
 *   <li><b>Pending row(s) aged out (≥ MAX_WAIT_DAYS)</b> → route to dead-letter.</li>
 * </ul>
 *
 * <p>AI vs human classification uses the {@code method} column compared against the
 * {@code methodToSide} map supplied at construction time.
 */
public class FilterAndPairFn
        extends DoFn<KV<String, CoGbkResult>,
                     KV<String, KV<GenericRecord, GenericRecord>>> {

    private static final Logger LOG = LoggerFactory.getLogger(FilterAndPairFn.class);

    public static final int MAX_WAIT_DAYS = 7;

    private final ValueProvider<String> segmentConfigsJson;
    private final Schema payloadSchema;
    private final Schema pendingSchema;

    // Resolved in @Setup; transient so it is re-initialized on each worker.
    private transient Map<String, String> methodToSide;

    public FilterAndPairFn(ValueProvider<String> segmentConfigsJson,
                            Schema payloadSchema,
                            Schema pendingSchema) {
        this.segmentConfigsJson = segmentConfigsJson;
        this.payloadSchema      = payloadSchema;
        this.pendingSchema      = pendingSchema;
    }

    @Setup
    public void setup() {
        List<SegmentConfig> segments = SegmentConfig.parse(segmentConfigsJson.get());
        methodToSide = SegmentConfig.buildMethodToSideMap(segments);
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
        // Key format: "imageId::segment"
        String fullKey  = ctx.element().getKey();
        String[] keyParts = fullKey.split("::", 2);
        String imageId   = keyParts[0];
        String segment   = keyParts.length > 1 ? keyParts[1] : "main";
        CoGbkResult result = ctx.element().getValue();

        // ── Partition source rows by side ────────────────────────────────────
        List<GenericRecord> humanRows = new ArrayList<>();
        List<GenericRecord> aiRows    = new ArrayList<>();

        for (TableRow row : result.getAll(SOURCE_TAG)) {
            String encryptedPayload = (String) row.get("payload");
            String keyId            = (String) row.get("key_id");
            String method           = (String) row.get("method");

            String side = methodToSide.get(method);
            if (side == null) {
                LOG.warn("Unrecognised method '{}' for imageId={} segment={} — skipping",
                        method, imageId, segment);
                continue;
            }

            String createdAtNorm = TimestampUtil.normalizeTimestamp((String) row.get("created_at"));
            GenericRecord p = newPayloadRow(imageId, keyId, side, encryptedPayload, createdAtNorm);
            if ("human".equals(side)) {
                humanRows.add(p);
            } else {
                aiRows.add(p);
            }
        }

        if (humanRows.size() > 1) {
            LOG.warn("imageId={} segment={} has {} human rows — using the earliest by created_at",
                    imageId, segment, humanRows.size());
            humanRows.sort(Comparator.comparing(r -> str(r.get("created_at"))));
        }

        // ── Restore pending rows ──────────────────────────────────────────────
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
                LOG.debug("Restored pending human for imageId={} segment={}", imageId, segment);
            } else if ("ai".equals(pendingType)) {
                aiRows.add(newPayloadRow(imageId, pendingKeyId, "ai",
                        str(p.get("payload")), createdAt));
                LOG.debug("Restored pending AI (created_at={}) for imageId={} segment={}",
                        createdAt, imageId, segment);
            }
        }

        Instant now          = Instant.now();
        boolean humanPresent = !humanRows.isEmpty();
        boolean anyAiPresent = !aiRows.isEmpty();

        aiRows.sort(Comparator.comparing(r -> str(r.get("created_at"))));

        if (humanPresent && anyAiPresent) {
            // ── State 1: human + N AI → emit one MATCHED per AI ──────────────
            GenericRecord human = humanRows.get(0);
            for (int i = 0; i < aiRows.size(); i++) {
                // Pair key: "imageId::segment::iteration"
                String pairKey = imageId + "::" + segment + "::" + (i + 1);
                ctx.output(MATCHED, KV.of(pairKey, KV.of(human, aiRows.get(i))));
            }
            LOG.info("Matched imageId={} segment={} — {} AI iteration(s)",
                    imageId, segment, aiRows.size());

            // Re-pend human for late-arriving AI iterations.
            String        humanCreatedAt = str(human.get("created_at"));
            GenericRecord humanMeta      = pendingMeta.get("human:" + humanCreatedAt);
            Instant       firstSeen      = metaFirstSeen(humanMeta, now);
            long          retryCount     = metaRetryCount(humanMeta);
            long          daysWaited     = ChronoUnit.DAYS.between(firstSeen, now);
            emitPendingOrAgedOut(ctx, imageId, segment, str(human.get("key_id")), "human",
                    str(human.get("payload")), humanCreatedAt,
                    firstSeen, now, retryCount, daysWaited);

        } else if (humanPresent) {
            // ── State 2: human only → pend ───────────────────────────────────
            GenericRecord human     = humanRows.get(0);
            String        createdAt = str(human.get("created_at"));
            GenericRecord meta      = pendingMeta.get("human:" + createdAt);
            Instant firstSeen       = metaFirstSeen(meta, now);
            long retryCount         = metaRetryCount(meta);
            long daysWaited         = ChronoUnit.DAYS.between(firstSeen, now);
            LOG.info("Human-only imageId={} segment={} — pending AI (days waited={})",
                    imageId, segment, daysWaited);
            emitPendingOrAgedOut(ctx, imageId, segment, str(human.get("key_id")), "human",
                    str(human.get("payload")), createdAt,
                    firstSeen, now, retryCount, daysWaited);

        } else if (anyAiPresent) {
            // ── State 3: AI only → pend each iteration ───────────────────────
            for (GenericRecord ai : aiRows) {
                String        createdAt = str(ai.get("created_at"));
                GenericRecord meta      = pendingMeta.get("ai:" + createdAt);
                Instant firstSeen       = metaFirstSeen(meta, now);
                long retryCount         = metaRetryCount(meta);
                long daysWaited         = ChronoUnit.DAYS.between(firstSeen, now);
                LOG.info("AI-only imageId={} segment={} (created_at={}) — pending human (days waited={})",
                        imageId, segment, createdAt, daysWaited);
                emitPendingOrAgedOut(ctx, imageId, segment, str(ai.get("key_id")), "ai",
                        str(ai.get("payload")), createdAt,
                        firstSeen, now, retryCount, daysWaited);
            }

        } else if (!pendingMeta.isEmpty()) {
            // ── State 4: no source rows — keep or age-out each pending row ────
            for (GenericRecord p : pendingMeta.values()) {
                Instant firstSeen = metaFirstSeen(p, now);
                long daysWaited   = ChronoUnit.DAYS.between(firstSeen, now);
                if (daysWaited >= MAX_WAIT_DAYS) {
                    LOG.warn("imageId={} segment={} pending row (type={}) aged out after {} days",
                            imageId, segment, str(p.get("pending_type")), daysWaited);
                }
                emitPendingOrAgedOut(ctx, imageId, segment,
                        str(p.get("key_id")), str(p.get("pending_type")),
                        str(p.get("payload")), str(p.get("created_at")),
                        firstSeen, now, metaRetryCount(p), daysWaited);
            }
        }
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private void emitPendingOrAgedOut(ProcessContext ctx,
                                       String imageId, String segment,
                                       String keyId, String pendingType,
                                       String payload, String createdAt,
                                       Instant firstSeen, Instant now,
                                       long retryCount, long daysWaited) {
        GenericRecord row = newPendingRow(imageId, segment, keyId, pendingType,
                payload, createdAt, firstSeen, now, retryCount);
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

    private GenericRecord newPendingRow(String imageId, String segment,
                                         String keyId, String pendingType,
                                         String payload, String createdAt,
                                         Instant firstSeen, Instant now,
                                         long retryCount) {
        GenericRecord r = new GenericData.Record(pendingSchema);
        r.put("image_id",        imageId);
        r.put("key_id",          keyId);
        r.put("segment",         segment);
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
        r.put("segment",         row.get("segment") != null ? row.get("segment") : "main");
        r.put("pending_type",    row.get("pending_type"));
        r.put("payload",         row.get("payload"));
        r.put("created_at",      TimestampUtil.normalizeTimestamp(str(row.get("created_at"))));
        r.put("first_seen_at",   TimestampUtil.normalizeTimestamp(str(row.get("first_seen_at"))));
        r.put("last_retried_at", TimestampUtil.normalizeTimestamp(str(row.get("last_retried_at"))));
        r.put("retry_count",     parseLong(row.get("retry_count")));
        return r;
    }

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
