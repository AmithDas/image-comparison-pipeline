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

public class FilterAndPairFn
        extends DoFn<KV<String, CoGbkResult>,
                     KV<String, KV<GenericRecord, GenericRecord>>> {

    private static final Logger LOG = LoggerFactory.getLogger(FilterAndPairFn.class);

    public static final int MAX_WAIT_DAYS = 7;

    private final ValueProvider<String> segmentConfigsJson;
    private final Schema                payloadSchema;
    private final Schema                pendingSchema;

    private transient Map<String, String>        methodToSide;
    private transient Map<String, SegmentConfig> segmentByName;

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
        methodToSide  = SegmentConfig.buildMethodToSideMap(segments);
        segmentByName = SegmentConfig.buildNameMap(segments);
    }

    public static final TupleTag<TableRow> SOURCE_TAG  = new TupleTag<>() {};
    public static final TupleTag<TableRow> PENDING_TAG = new TupleTag<>() {};

    public static final TupleTag<KV<String, KV<GenericRecord, GenericRecord>>> MATCHED
            = new TupleTag<>() {};
    public static final TupleTag<GenericRecord> NEW_PENDING = new TupleTag<>() {};
    public static final TupleTag<GenericRecord> AGED_OUT    = new TupleTag<>() {};

    @ProcessElement
    public void processElement(ProcessContext ctx) {
        String   fullKey  = ctx.element().getKey();
        String[] keyParts = fullKey.split("::", 2);
        String   imageId  = keyParts[0];
        String   segment  = keyParts.length > 1 ? keyParts[1] : "main";
        CoGbkResult result = ctx.element().getValue();

        SegmentConfig seg = segmentByName.get(segment);

        Map<String, GenericRecord> humanBySubType = new HashMap<>();
        List<GenericRecord>        aiRows         = new ArrayList<>();

        for (TableRow row : result.getAll(SOURCE_TAG)) {
            String method = (String) row.get("method");
            String side   = methodToSide.get(method);
            if (side == null) {
                LOG.warn("Unrecognised method '{}' for imageId={} segment={} — skipping",
                        method, imageId, segment);
                continue;
            }
            String createdAt = TimestampUtil.normalizeTimestamp((String) row.get("created_at"));
            String keyId     = (String) row.get("key_id");
            String payload   = (String) row.get("payload");

            if ("human".equals(side)) {
                Object rawSubType = row.get("_human_sub_type");
                String subType = (rawSubType != null) ? rawSubType.toString() : "default";
                GenericRecord incoming = newPayloadRow(imageId, keyId, "human", payload, createdAt);
                humanBySubType.merge(subType, incoming, (existing, candidate) -> {
                    String existingCat = str(existing.get("created_at"));
                    String candidateCat = str(candidate.get("created_at"));
                    if (existingCat == null) return candidate;
                    if (candidateCat == null) return existing;
                    if (candidateCat.compareTo(existingCat) > 0) {
                        LOG.warn("imageId={} segment={} subType={} — duplicate human payload, "
                                + "keeping latest ({})", imageId, segment, subType, candidateCat);
                        return candidate;
                    }
                    LOG.warn("imageId={} segment={} subType={} — duplicate human payload, "
                            + "keeping latest ({})", imageId, segment, subType, existingCat);
                    return existing;
                });
            } else {
                aiRows.add(newPayloadRow(imageId, keyId, "ai", payload, createdAt));
            }
        }

        Instant now            = Instant.now();
        Instant groupFirstSeen = now;

        List<TableRow> pendingRows = new ArrayList<>();
        for (TableRow pr : result.getAll(PENDING_TAG)) {
            pendingRows.add(pr);
            String fsa = (String) pr.get("first_seen_at");
            if (fsa != null) {
                Instant parsed = parseInstant(TimestampUtil.normalizeTimestamp(fsa));
                if (parsed != null && parsed.isBefore(groupFirstSeen)) {
                    groupFirstSeen = parsed;
                }
            }
        }

        long groupDaysWaited = ChronoUnit.DAYS.between(groupFirstSeen, now);

        if (groupDaysWaited >= MAX_WAIT_DAYS && !pendingRows.isEmpty()) {
            for (TableRow pr : pendingRows) {
                GenericRecord p = toPendingRecord(pr);
                LOG.warn("imageId={} segment={} pending_type={} aged out (group age={} days)",
                        imageId, segment, str(p.get("pending_type")), groupDaysWaited);
                ctx.output(AGED_OUT, p);
            }
            pendingRows.clear();
            groupFirstSeen  = now;
            groupDaysWaited = 0;
        }

        Map<String, GenericRecord> pendingMeta = new HashMap<>();
        for (TableRow pr : pendingRows) {
            GenericRecord p         = toPendingRecord(pr);
            String        pType     = str(p.get("pending_type"));
            String        createdAt = str(p.get("created_at"));

            pendingMeta.put(pType + ":" + createdAt, p);

            if ("human:merged".equals(pType)) {
                humanBySubType.putIfAbsent("_merged",
                        newPayloadRow(imageId, str(p.get("key_id")), "human",
                                str(p.get("payload")), createdAt));
            } else if (pType != null && pType.startsWith("human:")) {
                String subType = pType.substring(6);
                humanBySubType.putIfAbsent(subType,
                        newPayloadRow(imageId, str(p.get("key_id")), "human",
                                str(p.get("payload")), createdAt));
            } else if ("human".equals(pType)) {
                humanBySubType.putIfAbsent("default",
                        newPayloadRow(imageId, str(p.get("key_id")), "human",
                                str(p.get("payload")), createdAt));
            } else if ("ai".equals(pType)) {
                aiRows.add(newPayloadRow(imageId, str(p.get("key_id")), "ai",
                        str(p.get("payload")), createdAt));
            }
        }

        aiRows.sort(Comparator.comparing(r -> str(r.get("created_at"))));

        boolean humanComplete = isHumanComplete(seg, humanBySubType);
        boolean aiPresent     = !aiRows.isEmpty();

        if (humanComplete && aiPresent) {
            GenericRecord humanRec   = resolveHumanRecord(imageId, segment, humanBySubType, seg);
            String        humanPType = resolvedPendingType(humanBySubType, seg);
            for (int i = 0; i < aiRows.size(); i++) {
                ctx.output(MATCHED, KV.of(
                        imageId + "::" + segment + "::" + (i + 1),
                        KV.of(humanRec, aiRows.get(i))));
            }
            LOG.info("Matched imageId={} segment={} — {} AI iteration(s)",
                    imageId, segment, aiRows.size());
            String hCAt = str(humanRec.get("created_at"));
            emitPendingOrAgedOut(ctx, imageId, segment, str(humanRec.get("key_id")),
                    humanPType, str(humanRec.get("payload")), hCAt,
                    groupFirstSeen, now,
                    metaRetryCount(pendingMeta.get(humanPType + ":" + hCAt)), groupDaysWaited);

        } else if (humanComplete) {
            GenericRecord humanRec   = resolveHumanRecord(imageId, segment, humanBySubType, seg);
            String        humanPType = resolvedPendingType(humanBySubType, seg);
            String        hCAt       = str(humanRec.get("created_at"));
            LOG.info("Human complete, no AI — imageId={} segment={}", imageId, segment);
            emitPendingOrAgedOut(ctx, imageId, segment, str(humanRec.get("key_id")),
                    humanPType, str(humanRec.get("payload")), hCAt,
                    groupFirstSeen, now,
                    metaRetryCount(pendingMeta.get(humanPType + ":" + hCAt)), groupDaysWaited);
            for (GenericRecord ai : aiRows) {
                String cAt = str(ai.get("created_at"));
                emitPendingOrAgedOut(ctx, imageId, segment, str(ai.get("key_id")),
                        "ai", str(ai.get("payload")), cAt,
                        groupFirstSeen, now, metaRetryCount(pendingMeta.get("ai:" + cAt)),
                        groupDaysWaited);
            }

        } else if (!humanBySubType.isEmpty() || aiPresent) {
            for (Map.Entry<String, GenericRecord> e : humanBySubType.entrySet()) {
                String subType = e.getKey();
                GenericRecord r = e.getValue();
                String cAt   = str(r.get("created_at"));
                String pType = "default".equals(subType) ? "human" : "human:" + subType;
                LOG.info("Partial human sub-type={} imageId={} segment={}", subType, imageId, segment);
                emitPendingOrAgedOut(ctx, imageId, segment, str(r.get("key_id")),
                        pType, str(r.get("payload")), cAt,
                        groupFirstSeen, now, metaRetryCount(pendingMeta.get(pType + ":" + cAt)),
                        groupDaysWaited);
            }
            for (GenericRecord ai : aiRows) {
                String cAt = str(ai.get("created_at"));
                emitPendingOrAgedOut(ctx, imageId, segment, str(ai.get("key_id")),
                        "ai", str(ai.get("payload")), cAt,
                        groupFirstSeen, now, metaRetryCount(pendingMeta.get("ai:" + cAt)),
                        groupDaysWaited);
            }

        } else if (!pendingMeta.isEmpty()) {
            for (GenericRecord p : pendingMeta.values()) {
                Instant firstSeen = metaFirstSeen(p, groupFirstSeen);
                long daysWaited   = ChronoUnit.DAYS.between(firstSeen, now);
                if (daysWaited >= MAX_WAIT_DAYS) {
                    LOG.warn("imageId={} segment={} pending_type={} aged out after {} days",
                            imageId, segment, str(p.get("pending_type")), daysWaited);
                }
                emitPendingOrAgedOut(ctx, imageId, segment,
                        str(p.get("key_id")), str(p.get("pending_type")),
                        str(p.get("payload")), str(p.get("created_at")),
                        firstSeen, now, metaRetryCount(p), daysWaited);
            }
        }
    }

    private boolean isHumanComplete(SegmentConfig seg, Map<String, GenericRecord> present) {
        if (present.containsKey("_merged")) return true;
        if (seg == null || !seg.requiresHumanMerge()) return present.containsKey("default");
        return seg.humanSubTypes.stream().allMatch(st -> present.containsKey(st.name));
    }

    private GenericRecord resolveHumanRecord(String imageId, String segment,
                                              Map<String, GenericRecord> humanBySubType,
                                              SegmentConfig seg) {
        if (humanBySubType.containsKey("_merged")) return humanBySubType.get("_merged");
        if (seg != null && seg.requiresHumanMerge())
            return HumanMerger.merge(imageId, segment, humanBySubType, seg, payloadSchema);
        return humanBySubType.get("default");
    }

    private static String resolvedPendingType(Map<String, GenericRecord> humanBySubType,
                                               SegmentConfig seg) {
        if (humanBySubType.containsKey("_merged")) return "human:merged";
        if (seg != null && seg.requiresHumanMerge()) return "human:merged";
        return "human";
    }

    private void emitPendingOrAgedOut(ProcessContext ctx,
                                       String imageId, String segment,
                                       String keyId, String pendingType,
                                       String payload, String createdAt,
                                       Instant groupFirstSeen, Instant now,
                                       long retryCount, long daysWaited) {
        GenericRecord row = newPendingRow(imageId, segment, keyId, pendingType,
                payload, createdAt, groupFirstSeen, now, retryCount);
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
        try { return Long.parseLong(value.toString().trim()); }
        catch (NumberFormatException e) { return 0L; }
    }

    private static String str(Object value) {
        return value != null ? value.toString() : null;
    }

    private static Instant parseInstant(String value) {
        if (value == null) return null;
        try { return Instant.parse(value); }
        catch (Exception e) { return null; }
    }
}
