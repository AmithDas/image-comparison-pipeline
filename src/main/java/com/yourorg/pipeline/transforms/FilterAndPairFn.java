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
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Groups source + durable pending state by {@code imageId::segment} and decides,
 * for each <b>case</b> (a human submission identified by {@code case_id}) within
 * that group, whether it can be matched against the group's shared AI history.
 *
 * <h3>Cases vs. AI history</h3>
 * A single image's AI payloads are shared across every case for that image+segment —
 * "case" is purely a human-side concept (see {@code case_id} on the human source
 * table). AI rows therefore never belong to a case: they live in a separate,
 * always-retained pool ({@link #AI_PENDING_TAG} / {@link #AI_PENDING} /
 * {@link #AI_AGED_OUT}) that is never pruned just because it matched — a case
 * discovered on a later run still needs to be compared against AI payloads that
 * arrived before it existed. Segments that never populate {@code case_id} fall
 * back to a single internal bucket ({@link #NO_CASE}), reproducing the old
 * single-human-record-per-group behaviour exactly (including sub-type merging).
 *
 * <h3>Per-case AI matching</h3>
 * Each case persists the set of AI payload identity keys it has already been
 * compared against ({@code matched_ai_keys} — see {@link #dedupKey}). Every run,
 * a case is paired against whichever AI rows from the shared pool aren't yet in
 * that set — this is what allows a newly-discovered case to "replay" the entire
 * AI history for its image, while a long-running case only sees what's new.
 * {@code ai_iteration} numbers are assigned in the order each AI row was
 * <em>discovered</em> for that case (append-only, immutable once emitted) rather
 * than a strict global {@code created_at} ordering — the two can only diverge
 * when an AI payload with an older {@code created_at} arrives on a later run than
 * one already matched, and immutability of already-emitted iterations takes
 * priority over retroactively renumbering them.
 *
 * <h3>Aging</h3>
 * {@code MAX_WAIT_DAYS} is evaluated independently per case and independently per
 * AI row (each against its own {@code first_seen_at}) — there is no group-wide
 * bulk age-out, so one stale case or AI backlog item can no longer force-expire
 * an unrelated, actively-progressing case sharing the same image+segment.
 */
public class FilterAndPairFn
        extends DoFn<KV<String, CoGbkResult>,
                     KV<String, KV<GenericRecord, GenericRecord>>> {

    private static final Logger LOG = LoggerFactory.getLogger(FilterAndPairFn.class);

    public static final int MAX_WAIT_DAYS = 7;

    /** Internal bucket key for human records with no real {@code case_id}. Never persisted as-is. */
    private static final String NO_CASE = "";

    private final ValueProvider<String> segmentConfigsJson;
    private final Schema                payloadSchema;
    private final Schema                pendingSchema;
    private final Schema                aiPendingSchema;

    private transient Map<String, String>        methodToSide;
    private transient Map<String, SegmentConfig> segmentByName;

    public FilterAndPairFn(ValueProvider<String> segmentConfigsJson,
                            Schema payloadSchema,
                            Schema pendingSchema,
                            Schema aiPendingSchema) {
        this.segmentConfigsJson = segmentConfigsJson;
        this.payloadSchema      = payloadSchema;
        this.pendingSchema      = pendingSchema;
        this.aiPendingSchema    = aiPendingSchema;
    }

    @Setup
    public void setup() {
        List<SegmentConfig> segments = SegmentConfig.parse(segmentConfigsJson.get());
        methodToSide  = SegmentConfig.buildMethodToSideMap(segments);
        segmentByName = SegmentConfig.buildNameMap(segments);
    }

    public static final TupleTag<TableRow> SOURCE_TAG       = new TupleTag<>() {};
    public static final TupleTag<TableRow> CASE_PENDING_TAG = new TupleTag<>() {};
    public static final TupleTag<TableRow> AI_PENDING_TAG   = new TupleTag<>() {};

    public static final TupleTag<KV<String, KV<GenericRecord, GenericRecord>>> MATCHED
            = new TupleTag<>() {};
    public static final TupleTag<GenericRecord> CASE_PENDING  = new TupleTag<>() {};
    public static final TupleTag<GenericRecord> CASE_AGED_OUT = new TupleTag<>() {};
    public static final TupleTag<GenericRecord> AI_PENDING    = new TupleTag<>() {};
    public static final TupleTag<GenericRecord> AI_AGED_OUT   = new TupleTag<>() {};

    @ProcessElement
    public void processElement(ProcessContext ctx) {
        String   fullKey  = ctx.element().getKey();
        String[] keyParts = fullKey.split("::", 2);
        String   imageId  = keyParts[0];
        String   segment  = keyParts.length > 1 ? keyParts[1] : "main";
        CoGbkResult result = ctx.element().getValue();

        SegmentConfig seg = segmentByName.get(segment);
        Instant now = Instant.now();

        // caseKey -> subType -> record
        Map<String, Map<String, GenericRecord>> humanByCase = new LinkedHashMap<>();
        List<GenericRecord> aiRows = new ArrayList<>();

        // ── Fresh SOURCE rows ────────────────────────────────────────────────
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
                String caseKey = normalizeCaseKey((String) row.get("case_id"));
                Object rawSubType = row.get("_human_sub_type");
                String subType = (rawSubType != null) ? rawSubType.toString() : "default";
                GenericRecord incoming = newPayloadRow(imageId, keyId, "human", payload, createdAt);
                mergeFreshHuman(humanByCase, caseKey, subType, incoming, imageId, segment);
            } else {
                aiRows.add(newPayloadRow(imageId, keyId, "ai", payload, createdAt));
            }
        }

        // ── Fold in the AI replay pool ──────────────────────────────────────
        // AI rows are always retained regardless of match status (see class doc),
        // so this run's aiRows is the fresh source rows above plus everything still
        // alive from ai_pending_comparisons.
        Map<String, GenericRecord> aiPendingMeta = new HashMap<>();
        for (TableRow pr : result.getAll(AI_PENDING_TAG)) {
            GenericRecord p         = toAiPendingRecord(pr);
            String        createdAt = str(p.get("created_at"));
            aiRows.add(newPayloadRow(imageId, str(p.get("key_id")), "ai",
                    str(p.get("payload")), createdAt));
            aiPendingMeta.put(dedupKey(str(p.get("payload")), createdAt), p);
        }

        // Deduplicate AI rows by (payload, created_at) — see FilterAndPairFn history:
        // the Barricade ciphertext is a random-IV envelope carried through unchanged,
        // so identical payload strings are strong evidence of the same row reappearing
        // (e.g. present in both the fresh source query and the retained AI pool).
        // created_at is a safety net against any incidental payload-string collision.
        // Source-query rows are inserted first above, so putIfAbsent preserves them.
        Map<String, GenericRecord> aiDedup = new LinkedHashMap<>();
        for (GenericRecord ai : aiRows) {
            aiDedup.putIfAbsent(dedupKey(ai), ai);
        }
        aiRows = new ArrayList<>(aiDedup.values());
        aiRows.sort(Comparator.comparing(r -> str(r.get("created_at"))));

        // ── Fold in existing case pending rows ──────────────────────────────
        Map<String, GenericRecord> casePendingMeta = new HashMap<>();
        for (TableRow pr : result.getAll(CASE_PENDING_TAG)) {
            GenericRecord p        = toCasePendingRecord(pr);
            String        caseKey  = normalizeCaseKey(str(p.get("case_id")));
            String        pType    = str(p.get("pending_type"));
            String        createdAt = str(p.get("created_at"));
            casePendingMeta.put(caseKey + "|" + pType + ":" + createdAt, p);

            GenericRecord rec = newPayloadRow(imageId, str(p.get("key_id")), "human",
                    str(p.get("payload")), createdAt);
            if ("human:merged".equals(pType)) {
                foldPendingHuman(humanByCase, caseKey, "_merged", rec);
            } else if (pType != null && pType.startsWith("human:")) {
                foldPendingHuman(humanByCase, caseKey, pType.substring(6), rec);
            } else if ("human".equals(pType)) {
                foldPendingHuman(humanByCase, caseKey, "default", rec);
            }
        }

        // ── Always re-pend every AI row, aged against its own first_seen_at ─────
        for (GenericRecord ai : aiRows) {
            GenericRecord meta = aiPendingMeta.get(dedupKey(ai));
            Instant firstSeen  = meta != null ? parseInstant(str(meta.get("first_seen_at"))) : null;
            if (firstSeen == null) firstSeen = now;
            long daysWaited = ChronoUnit.DAYS.between(firstSeen, now);
            emitAiPendingOrAgedOut(ctx, imageId, segment, str(ai.get("key_id")),
                    str(ai.get("payload")), str(ai.get("created_at")),
                    firstSeen, now, metaRetryCount(meta), daysWaited);
        }

        // ── Per-case matching ────────────────────────────────────────────────
        // humanByCase already contains every case with either fresh activity this
        // run or carried-forward pending state, so a single pass over it covers
        // every scenario the old code split into separate branches for.
        for (Map.Entry<String, Map<String, GenericRecord>> caseEntry : humanByCase.entrySet()) {
            String caseKey = caseEntry.getKey();
            Map<String, GenericRecord> subTypes = caseEntry.getValue();

            if (!isHumanComplete(seg, subTypes)) {
                for (Map.Entry<String, GenericRecord> e : subTypes.entrySet()) {
                    String subType = e.getKey();
                    GenericRecord r = e.getValue();
                    String cAt   = str(r.get("created_at"));
                    String pType = "default".equals(subType) ? "human" : "human:" + subType;
                    GenericRecord meta = casePendingMeta.get(caseKey + "|" + pType + ":" + cAt);
                    Instant firstSeen = meta != null ? parseInstant(str(meta.get("first_seen_at"))) : null;
                    if (firstSeen == null) firstSeen = now;
                    long daysWaited = ChronoUnit.DAYS.between(firstSeen, now);
                    LOG.info("Partial human case={} sub-type={} imageId={} segment={}",
                            displayCase(caseKey), subType, imageId, segment);
                    emitCasePendingOrAgedOut(ctx, imageId, segment, caseKey,
                            str(r.get("key_id")), pType, str(r.get("payload")), cAt,
                            firstSeen, now, metaRetryCount(meta), daysWaited,
                            meta != null ? str(meta.get("matched_ai_keys")) : null);
                }
                continue;
            }

            GenericRecord humanRec   = resolveHumanRecord(imageId, segment, subTypes, seg);
            String        humanPType = resolvedPendingType(subTypes, seg);
            String        hCAt       = str(humanRec.get("created_at"));

            GenericRecord meta = casePendingMeta.get(caseKey + "|" + humanPType + ":" + hCAt);
            Set<String> matchedKeys = parseKeys(meta != null ? str(meta.get("matched_ai_keys")) : null);
            Instant firstSeen = meta != null ? parseInstant(str(meta.get("first_seen_at"))) : null;
            if (firstSeen == null) firstSeen = now;
            long daysWaited = ChronoUnit.DAYS.between(firstSeen, now);

            List<GenericRecord> unmatched = new ArrayList<>();
            for (GenericRecord ai : aiRows) {
                if (!matchedKeys.contains(dedupKey(ai))) unmatched.add(ai);
            }

            if (!unmatched.isEmpty()) {
                long startIteration = matchedKeys.size();
                for (int i = 0; i < unmatched.size(); i++) {
                    long iteration = startIteration + i + 1;
                    ctx.output(MATCHED, KV.of(
                            imageId + "::" + segment + "::" + caseKey + "::" + iteration,
                            KV.of(humanRec, unmatched.get(i))));
                    matchedKeys.add(dedupKey(unmatched.get(i)));
                }
                LOG.info("Matched imageId={} segment={} case={} — {} new AI iteration(s), "
                        + "iterations {}-{}", imageId, segment, displayCase(caseKey),
                        unmatched.size(), startIteration + 1, matchedKeys.size());
            }

            emitCasePendingOrAgedOut(ctx, imageId, segment, caseKey,
                    str(humanRec.get("key_id")), humanPType, str(humanRec.get("payload")), hCAt,
                    firstSeen, now, metaRetryCount(meta), daysWaited, joinKeys(matchedKeys));
        }
    }

    // ── Case bucketing ───────────────────────────────────────────────────────

    private static String normalizeCaseKey(String rawCaseId) {
        return (rawCaseId != null && !rawCaseId.isBlank()) ? rawCaseId.trim() : NO_CASE;
    }

    private static String displayCase(String caseKey) {
        return caseKey.isEmpty() ? "<none>" : caseKey;
    }

    /** Folds a fresh SOURCE-tag human row in, keeping the latest on duplicate sub-type collision. */
    private static void mergeFreshHuman(Map<String, Map<String, GenericRecord>> humanByCase,
                                         String caseKey, String subType, GenericRecord incoming,
                                         String imageId, String segment) {
        humanByCase.computeIfAbsent(caseKey, k -> new LinkedHashMap<>())
                .merge(subType, incoming, (existing, candidate) -> {
                    String existingCat  = str(existing.get("created_at"));
                    String candidateCat = str(candidate.get("created_at"));
                    if (existingCat == null) return candidate;
                    if (candidateCat == null) return existing;
                    if (candidateCat.compareTo(existingCat) > 0) {
                        LOG.warn("imageId={} segment={} case={} subType={} — duplicate human "
                                        + "payload, keeping latest ({})",
                                imageId, segment, displayCase(caseKey), subType, candidateCat);
                        return candidate;
                    }
                    LOG.warn("imageId={} segment={} case={} subType={} — duplicate human payload, "
                                    + "keeping latest ({})",
                            imageId, segment, displayCase(caseKey), subType, existingCat);
                    return existing;
                });
    }

    /** Folds a carried-forward pending human row in — never overrides a fresh row from this run. */
    private static void foldPendingHuman(Map<String, Map<String, GenericRecord>> humanByCase,
                                          String caseKey, String subType, GenericRecord incoming) {
        humanByCase.computeIfAbsent(caseKey, k -> new LinkedHashMap<>())
                .putIfAbsent(subType, incoming);
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

    // ── AI identity keys ─────────────────────────────────────────────────────

    private static String dedupKey(GenericRecord ai) {
        return dedupKey(str(ai.get("payload")), str(ai.get("created_at")));
    }

    private static String dedupKey(String payload, String createdAt) {
        return strTrim(payload) + "|" + strTrim(createdAt);
    }

    private static Set<String> parseKeys(String joined) {
        Set<String> keys = new LinkedHashSet<>();
        if (joined != null && !joined.isBlank()) {
            for (String k : joined.split(";")) {
                if (!k.isEmpty()) keys.add(k);
            }
        }
        return keys;
    }

    private static String joinKeys(Set<String> keys) {
        return keys.isEmpty() ? null : String.join(";", keys);
    }

    // ── Emission ──────────────────────────────────────────────────────────────

    private void emitAiPendingOrAgedOut(ProcessContext ctx,
                                         String imageId, String segment,
                                         String keyId, String payload, String createdAt,
                                         Instant firstSeen, Instant now,
                                         long retryCount, long daysWaited) {
        GenericRecord row = newAiPendingRow(imageId, segment, keyId, payload, createdAt,
                firstSeen, now, retryCount);
        if (daysWaited >= MAX_WAIT_DAYS) {
            ctx.output(AI_AGED_OUT, row);
        } else {
            ctx.output(AI_PENDING, row);
        }
    }

    private void emitCasePendingOrAgedOut(ProcessContext ctx,
                                           String imageId, String segment, String caseKey,
                                           String keyId, String pendingType,
                                           String payload, String createdAt,
                                           Instant firstSeen, Instant now,
                                           long retryCount, long daysWaited,
                                           String matchedAiKeys) {
        GenericRecord row = newCasePendingRow(imageId, segment, caseKey, keyId, pendingType,
                payload, createdAt, firstSeen, now, retryCount, matchedAiKeys);
        if (daysWaited >= MAX_WAIT_DAYS) {
            ctx.output(CASE_AGED_OUT, row);
        } else {
            ctx.output(CASE_PENDING, row);
        }
    }

    private static Instant parseInstant(String value) {
        if (value == null) return null;
        try { return Instant.parse(value); }
        catch (Exception e) { return null; }
    }

    private static long metaRetryCount(GenericRecord meta) {
        if (meta == null) return 0L;
        Object count = meta.get("retry_count");
        return count != null ? ((Number) count).longValue() + 1 : 0L;
    }

    // ── Record construction ──────────────────────────────────────────────────

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

    private GenericRecord newAiPendingRow(String imageId, String segment, String keyId,
                                           String payload, String createdAt,
                                           Instant firstSeen, Instant now, long retryCount) {
        GenericRecord r = new GenericData.Record(aiPendingSchema);
        r.put("image_id",        imageId);
        r.put("key_id",          keyId);
        r.put("segment",         segment);
        r.put("payload",         payload);
        r.put("created_at",      createdAt);
        r.put("first_seen_at",   TimestampUtil.formatInstant(firstSeen));
        r.put("last_retried_at", TimestampUtil.formatInstant(now));
        r.put("retry_count",     retryCount);
        return r;
    }

    private GenericRecord newCasePendingRow(String imageId, String segment, String caseKey,
                                             String keyId, String pendingType, String payload,
                                             String createdAt, Instant firstSeen, Instant now,
                                             long retryCount, String matchedAiKeys) {
        GenericRecord r = new GenericData.Record(pendingSchema);
        r.put("image_id",        imageId);
        r.put("key_id",          keyId);
        r.put("segment",         segment);
        r.put("case_id",         caseKey.isEmpty() ? null : caseKey);
        r.put("pending_type",    pendingType);
        r.put("payload",         payload);
        r.put("created_at",      createdAt);
        r.put("first_seen_at",   TimestampUtil.formatInstant(firstSeen));
        r.put("last_retried_at", TimestampUtil.formatInstant(now));
        r.put("retry_count",     retryCount);
        r.put("matched_ai_keys", matchedAiKeys);
        return r;
    }

    private GenericRecord toAiPendingRecord(TableRow row) {
        GenericRecord r = new GenericData.Record(aiPendingSchema);
        r.put("image_id",        row.get("image_id"));
        r.put("key_id",          row.get("key_id"));
        r.put("segment",         row.get("segment") != null ? row.get("segment") : "main");
        r.put("payload",         row.get("payload"));
        r.put("created_at",      TimestampUtil.normalizeTimestamp(str(row.get("created_at"))));
        r.put("first_seen_at",   TimestampUtil.normalizeTimestamp(str(row.get("first_seen_at"))));
        r.put("last_retried_at", TimestampUtil.normalizeTimestamp(str(row.get("last_retried_at"))));
        r.put("retry_count",     parseLong(row.get("retry_count")));
        return r;
    }

    private GenericRecord toCasePendingRecord(TableRow row) {
        GenericRecord r = new GenericData.Record(pendingSchema);
        r.put("image_id",        row.get("image_id"));
        r.put("key_id",          row.get("key_id"));
        r.put("segment",         row.get("segment") != null ? row.get("segment") : "main");
        r.put("case_id",         row.get("case_id"));
        r.put("pending_type",    row.get("pending_type"));
        r.put("payload",         row.get("payload"));
        r.put("created_at",      TimestampUtil.normalizeTimestamp(str(row.get("created_at"))));
        r.put("first_seen_at",   TimestampUtil.normalizeTimestamp(str(row.get("first_seen_at"))));
        r.put("last_retried_at", TimestampUtil.normalizeTimestamp(str(row.get("last_retried_at"))));
        r.put("retry_count",     parseLong(row.get("retry_count")));
        r.put("matched_ai_keys", row.get("matched_ai_keys"));
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

    private static String strTrim(Object value) {
        return value != null ? value.toString().trim() : null;
    }
}
