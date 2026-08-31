package com.yourorg.pipeline.transforms;

import com.google.api.services.bigquery.model.TableRow;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.yourorg.pipeline.config.SegmentConfig;
import com.yourorg.pipeline.util.BarricadeEncryptionUtil;
import com.yourorg.pipeline.util.PayloadParser;
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
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * Groups source + durable pending state by {@code imageId::segment} and decides
 * whether the group's consolidated human payload can be matched against the
 * group's shared AI history.
 *
 * <h3>Case_id is lineage, not a partition</h3>
 * Every human payload for the same {@code imageId::segment} — regardless of
 * {@code case_id} — is merged into a single consolidated record before being
 * compared against AI (see {@link #mergeAcrossCases}). {@code case_id} is
 * tracked purely as provenance: which case contributed which field. A field
 * present in only one contributing case's payload is attributed to that case
 * directly; when two cases collide on the same field (or on an array item, for
 * fields configured in {@code mergeArrayFields}), provenance is embedded
 * directly in the merged JSON payload itself (reserved keys
 * {@code _caseIdByField} and {@code _sourceCaseId}) since that's the only
 * channel available once the payload crosses the pending-table/replay
 * boundary. {@link FlattenAndCompareFn} reads and strips these keys to resolve
 * {@code case_id} per output row.
 *
 * <p>Segments that use {@code humanSubTypes} (e.g. an "authentication" +
 * "docreview" combo) never carry a real {@code case_id} in practice — every
 * contribution lands in the internal {@link #NO_CASE} bucket, so this merge
 * path never triggers for them and {@link HumanMerger}'s cross-sub-type merge
 * behaves exactly as before.
 *
 * <h3>Cases vs. AI history</h3>
 * A single image's AI payloads are shared across every case for that image+segment.
 * AI rows never belong to a case: they live in a separate, always-retained pool
 * ({@link #AI_PENDING_TAG} / {@link #AI_PENDING} / {@link #AI_AGED_OUT}) that is
 * never pruned just because it matched.
 *
 * <h3>AI matching, per group</h3>
 * Each {@code imageId::segment} group persists the set of AI payload identity
 * keys it has already been compared against ({@code matched_ai_keys} — see
 * {@link #dedupKey}). Every run, the group is paired against whichever AI rows
 * from the shared pool aren't yet in that set. {@code ai_iteration} numbers are
 * assigned in the order each AI row was <em>discovered</em> for the group
 * (append-only, immutable once emitted).
 *
 * <h3>Always-increasing AI created_at</h3>
 * A genuinely new AI payload for an image+segment (not a replay of one already
 * known) is assigned {@code created_at = previousMax.plusSeconds(1)}, so several
 * new payloads discovered in the same run get strictly increasing, distinct
 * timestamps regardless of their original source timestamp. Because
 * {@code created_at} is now an assigned value rather than a passthrough of the
 * source, AI row identity ({@link #dedupKey}) is the payload alone — a replayed
 * row keeps whatever timestamp was assigned the first time it was seen.
 *
 * <h3>Aging</h3>
 * {@code MAX_WAIT_DAYS} is evaluated independently per group and independently
 * per AI row (each against its own {@code first_seen_at}).
 */
public class FilterAndPairFn
        extends DoFn<KV<String, CoGbkResult>,
                     KV<String, KV<GenericRecord, GenericRecord>>> {

    private static final Logger LOG = LoggerFactory.getLogger(FilterAndPairFn.class);

    public static final int MAX_WAIT_DAYS = 7;

    /** Seconds a genuinely new AI payload's created_at is bumped past the group's prior max. */
    public static final int AI_TIMESTAMP_BUMP_SECONDS = 1;

    /** Internal bucket key for human records with no real {@code case_id}. Never persisted as-is. */
    private static final String NO_CASE = "";

    /** Reserved JSON key: field name -> contributing case_id, for a section with >1 contributor. */
    private static final String CASE_ID_BY_FIELD_KEY = "_caseIdByField";

    /** Reserved JSON key stamped onto a merged array item: which case_id contributed it. */
    private static final String SOURCE_CASE_ID_KEY = "_sourceCaseId";

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

        // caseKey -> subType -> record (accumulation only; collapsed into one
        // consolidated group below via mergeAcrossCases).
        Map<String, Map<String, GenericRecord>> humanByCase = new LinkedHashMap<>();
        List<GenericRecord> aiRows = new ArrayList<>();

        // ── Fresh SOURCE rows ────────────────────────────────────────────────
        List<GenericRecord> freshAiCandidates = new ArrayList<>();
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
                incoming = stampCaseIdByField(incoming, caseKey, seg, imageId, segment);
                mergeFreshHuman(humanByCase, caseKey, subType, incoming, imageId, segment);
            } else {
                freshAiCandidates.add(newPayloadRow(imageId, keyId, "ai", payload, createdAt));
            }
        }

        // ── Fold in the AI replay pool, seeding the running max created_at ──────
        Map<String, GenericRecord> aiPendingMeta = new HashMap<>();
        Instant maxAiCreatedAt = null;
        for (TableRow pr : result.getAll(AI_PENDING_TAG)) {
            GenericRecord p         = toAiPendingRecord(pr);
            String        createdAt = str(p.get("created_at"));
            aiRows.add(newPayloadRow(imageId, str(p.get("key_id")), "ai",
                    str(p.get("payload")), createdAt));
            aiPendingMeta.put(dedupKey(str(p.get("payload"))), p);
            Instant parsed = parseInstant(createdAt);
            if (parsed != null && (maxAiCreatedAt == null || parsed.isAfter(maxAiCreatedAt))) {
                maxAiCreatedAt = parsed;
            }
        }

        // ── Assign always-increasing created_at to genuinely new AI payloads ────
        // Dedup fresh candidates by payload identity first (a single physical row
        // must only be bump-assigned once), then process in a stable order so the
        // bump assignment is deterministic across runs.
        Map<String, GenericRecord> dedupedFresh = new LinkedHashMap<>();
        for (GenericRecord c : freshAiCandidates) {
            dedupedFresh.putIfAbsent(dedupKey(str(c.get("payload"))), c);
        }
        List<GenericRecord> orderedFresh = new ArrayList<>(dedupedFresh.values());
        orderedFresh.sort(Comparator
                .comparing((GenericRecord c) -> str(c.get("created_at")) == null ? "" : str(c.get("created_at")))
                .thenComparing(c -> str(c.get("payload"))));

        for (GenericRecord candidate : orderedFresh) {
            String key = dedupKey(str(candidate.get("payload")));
            if (aiPendingMeta.containsKey(key)) {
                // Replay of an already-known row — the pending-pool copy above already
                // covers it with its previously-assigned created_at; don't re-bump it.
                continue;
            }
            Instant assigned;
            if (maxAiCreatedAt == null) {
                Instant original = parseInstant(str(candidate.get("created_at")));
                assigned = original != null ? original : now;
            } else {
                assigned = maxAiCreatedAt.plusSeconds(AI_TIMESTAMP_BUMP_SECONDS);
            }
            maxAiCreatedAt = assigned;
            aiRows.add(newPayloadRow(imageId, str(candidate.get("key_id")), "ai",
                    str(candidate.get("payload")), TimestampUtil.formatInstant(assigned)));
        }

        aiRows.sort(Comparator.comparing(r -> str(r.get("created_at"))));

        // ── Fold in existing pending rows (case dimension collapses here too) ───
        Map<String, GenericRecord> groupPendingMeta = new HashMap<>();
        for (TableRow pr : result.getAll(CASE_PENDING_TAG)) {
            GenericRecord p        = toCasePendingRecord(pr);
            String        caseKey  = normalizeCaseKey(str(p.get("case_id")));
            String        pType    = str(p.get("pending_type"));
            String        createdAt = str(p.get("created_at"));
            groupPendingMeta.merge(pType, p, FilterAndPairFn::mergeGroupMeta);

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
            GenericRecord meta = aiPendingMeta.get(dedupKey(str(ai.get("payload"))));
            Instant firstSeen  = meta != null ? parseInstant(str(meta.get("first_seen_at"))) : null;
            if (firstSeen == null) firstSeen = now;
            long daysWaited = ChronoUnit.DAYS.between(firstSeen, now);
            emitAiPendingOrAgedOut(ctx, imageId, segment, str(ai.get("key_id")),
                    str(ai.get("payload")), str(ai.get("created_at")),
                    firstSeen, now, metaRetryCount(meta), daysWaited);
        }

        // ── Consolidate every case's contribution into one group per subType ────
        // Per-field case_id attribution no longer needs to be tracked here — every fresh
        // record was already stamped with its own _caseIdByField at ingestion above (see
        // stampCaseIdByField), so mergeAcrossCases derives attribution purely from what's
        // already embedded in each side's JSON.
        Map<String, GenericRecord> humanBySubType   = new LinkedHashMap<>();
        Set<String>                allContributingCaseIds = new LinkedHashSet<>();
        String   canonicalCaseId = null;
        String   canonicalCreatedAt = null;

        for (Map.Entry<String, Map<String, GenericRecord>> caseEntry : humanByCase.entrySet()) {
            String caseKey = caseEntry.getKey();
            if (!caseKey.isEmpty()) allContributingCaseIds.add(caseKey);

            for (Map.Entry<String, GenericRecord> e : caseEntry.getValue().entrySet()) {
                String subType = e.getKey();
                GenericRecord incoming = e.getValue();

                if (!caseKey.isEmpty()) {
                    String incomingCreatedAt = str(incoming.get("created_at"));
                    if (canonicalCaseId == null
                            || (incomingCreatedAt != null
                                && (canonicalCreatedAt == null
                                    || incomingCreatedAt.compareTo(canonicalCreatedAt) < 0))) {
                        canonicalCaseId    = caseKey;
                        canonicalCreatedAt = incomingCreatedAt;
                    }
                }

                GenericRecord existing = humanBySubType.get(subType);
                humanBySubType.put(subType, existing == null
                        ? incoming
                        : mergeAcrossCases(existing, incoming, seg, imageId, segment));
            }
        }

        for (GenericRecord pendingMeta : groupPendingMeta.values()) {
            String persistedMerged = str(pendingMeta.get("merged_case_ids"));
            if (persistedMerged != null) allContributingCaseIds.addAll(parseKeys(persistedMerged));
        }
        String mergedCaseIds = allContributingCaseIds.isEmpty()
                ? null : String.join(";", new TreeSet<>(allContributingCaseIds));

        // ── Matching, once per group ─────────────────────────────────────────
        if (!isHumanComplete(seg, humanBySubType)) {
            for (Map.Entry<String, GenericRecord> e : humanBySubType.entrySet()) {
                String subType = e.getKey();
                GenericRecord r = e.getValue();
                String cAt   = str(r.get("created_at"));
                String pType = "default".equals(subType) ? "human" : "human:" + subType;
                GenericRecord meta = groupPendingMeta.get(pType);
                Instant firstSeen = meta != null ? parseInstant(str(meta.get("first_seen_at"))) : null;
                if (firstSeen == null) firstSeen = now;
                long daysWaited = ChronoUnit.DAYS.between(firstSeen, now);
                LOG.info("Partial human imageId={} segment={} sub-type={}", imageId, segment, subType);
                emitGroupPendingOrAgedOut(ctx, imageId, segment, canonicalCaseId,
                        str(r.get("key_id")), pType, str(r.get("payload")), cAt,
                        firstSeen, now, metaRetryCount(meta), daysWaited,
                        meta != null ? str(meta.get("matched_ai_keys")) : null,
                        metaNextIteration(meta), mergedCaseIds);
            }
            return;
        }

        GenericRecord humanRec   = resolveHumanRecord(imageId, segment, humanBySubType, seg);
        // Fallback case_id for FlattenAndCompareFn's per-row resolution — used only when a
        // field's section had a single contributor (the common case, and the only case for
        // segments using humanSubTypes), since no _caseIdByField/_sourceCaseId is embedded then.
        humanRec.put("case_id", (canonicalCaseId == null || canonicalCaseId.isEmpty())
                ? null : canonicalCaseId);
        String        humanPType = resolvedPendingType(humanBySubType, seg);
        String        hCAt       = str(humanRec.get("created_at"));

        GenericRecord meta = groupPendingMeta.get(humanPType);
        Set<String> matchedKeys = parseKeys(meta != null ? str(meta.get("matched_ai_keys")) : null);
        long nextIteration = metaNextIteration(meta);
        Instant firstSeen = meta != null ? parseInstant(str(meta.get("first_seen_at"))) : null;
        if (firstSeen == null) firstSeen = now;
        long daysWaited = ChronoUnit.DAYS.between(firstSeen, now);

        List<GenericRecord> unmatched = new ArrayList<>();
        for (GenericRecord ai : aiRows) {
            if (!matchedKeys.contains(dedupKey(str(ai.get("payload"))))) unmatched.add(ai);
        }

        if (!unmatched.isEmpty()) {
            long startIteration = nextIteration;
            for (int i = 0; i < unmatched.size(); i++) {
                long iteration = startIteration + i + 1;
                ctx.output(MATCHED, KV.of(
                        imageId + "::" + segment + "::" + iteration,
                        KV.of(humanRec, unmatched.get(i))));
                matchedKeys.add(dedupKey(str(unmatched.get(i).get("payload"))));
            }
            nextIteration += unmatched.size();
            LOG.info("Matched imageId={} segment={} — {} new AI iteration(s), iterations {}-{}",
                    imageId, segment, unmatched.size(), startIteration + 1, nextIteration);
        }

        emitGroupPendingOrAgedOut(ctx, imageId, segment, canonicalCaseId,
                str(humanRec.get("key_id")), humanPType, str(humanRec.get("payload")), hCAt,
                firstSeen, now, metaRetryCount(meta), daysWaited, joinKeys(matchedKeys),
                nextIteration, mergedCaseIds);
    }

    // ── Case bucketing (accumulation only) ─────────────────────────────────────

    private static String normalizeCaseKey(String rawCaseId) {
        return (rawCaseId != null && !rawCaseId.isBlank()) ? rawCaseId.trim() : NO_CASE;
    }

    private static GenericRecord mergeGroupMeta(GenericRecord left, GenericRecord right) {
        Set<String> matchedKeys = parseKeys(str(left.get("matched_ai_keys")));
        matchedKeys.addAll(parseKeys(str(right.get("matched_ai_keys"))));

        long nextIteration = Math.max(metaNextIteration(left), metaNextIteration(right));

        String leftFirstSeen  = str(left.get("first_seen_at"));
        String rightFirstSeen = str(right.get("first_seen_at"));
        if (rightFirstSeen != null
                && (leftFirstSeen == null || rightFirstSeen.compareTo(leftFirstSeen) < 0)) {
            left.put("first_seen_at", rightFirstSeen);
        }

        String leftRetried  = str(left.get("last_retried_at"));
        String rightRetried = str(right.get("last_retried_at"));
        if (rightRetried != null
                && (leftRetried == null || rightRetried.compareTo(leftRetried) > 0)) {
            left.put("last_retried_at", rightRetried);
        }

        left.put("retry_count", Math.max(
                parseLong(left.get("retry_count")),
                parseLong(right.get("retry_count"))));
        left.put("matched_ai_keys", joinKeys(matchedKeys));
        left.put("next_ai_iteration", nextIteration);

        Set<String> merged = parseKeys(str(left.get("merged_case_ids")));
        merged.addAll(parseKeys(str(right.get("merged_case_ids"))));
        left.put("merged_case_ids", joinKeys(merged));
        return left;
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

    private static String displayCase(String caseKey) {
        return caseKey.isEmpty() ? "<none>" : caseKey;
    }

    // ── Cross-case field-level merge ─────────────────────────────────────────

    /**
     * Embeds {@link #CASE_ID_BY_FIELD_KEY} covering every field of a fresh human record —
     * recursively, at every level of nesting — with its own originating {@code case_id},
     * right at ingestion, before this record has ever been through a cross-case merge. This
     * is what lets a field that's never actually contested by another case still carry
     * explicit, correct attribution: without it, such a field would fall back to the group's
     * canonical case_id at read time, which is only guaranteed correct by circumstance (the
     * group happening to have one case per bucket), not by design.
     *
     * <p>Recursion matters here specifically because {@link #mergeJsonObjects} also recurses
     * into nested objects rather than treating them as one opaque value (see its own doc) —
     * a nested object needs its own {@code _caseIdByField} so that recursive merge finds
     * attribution for its children directly, instead of losing the parent's attribution the
     * moment it descends a level.
     *
     * <p>A record with no real {@code case_id} (NO_CASE — e.g. every contribution to a
     * {@code humanSubTypes} segment like authentication/docreview) is left completely
     * untouched. Does not recurse into array elements — {@code mergeJsonObjects} attributes
     * those via {@code _sourceCaseId} on the item itself instead, only if/when the array is
     * actually concatenated across cases.
     */
    private GenericRecord stampCaseIdByField(GenericRecord record, String caseKey,
                                              SegmentConfig seg, String imageId, String segment) {
        if (caseKey == null || caseKey.isEmpty()) return record;
        String keyId = str(record.get("key_id"));
        JsonObject json = decryptToJson(keyId, str(record.get("payload")), seg);
        if (json == null) {
            LOG.warn("imageId={} segment={} case={} — could not parse human payload to stamp "
                            + "case attribution; falls back to the group's canonical case_id instead",
                    imageId, segment, caseKey);
            return record;
        }
        if (json.has(CASE_ID_BY_FIELD_KEY)) return record; // already stamped

        stampObjectRecursively(json, caseKey);

        record.put("payload", BarricadeEncryptionUtil.encrypt(keyId, json.toString()));
        return record;
    }

    /** Package-visible for direct unit testing (see {@link #mergeJsonObjects}). */
    static void stampObjectRecursively(JsonObject obj, String caseKey) {
        JsonObject caseIdByField = new JsonObject();
        for (String key : obj.keySet()) {
            caseIdByField.addProperty(key, caseKey);
        }
        obj.add(CASE_ID_BY_FIELD_KEY, caseIdByField);

        for (String key : obj.keySet()) {
            if (CASE_ID_BY_FIELD_KEY.equals(key)) continue;
            JsonElement child = obj.get(key);
            if (child.isJsonObject()) {
                stampObjectRecursively(child.getAsJsonObject(), caseKey);
            }
        }
    }

    /**
     * Merges two records that collide on the same subType bucket — either two
     * case_ids in the same run, or a fresh row colliding with a persisted pending
     * row from an earlier run (both routed through this same method). Performs a
     * field-level JSON merge rather than whole-record replacement:
     * <ul>
     *   <li>A field present on only one side is kept as-is.</li>
     *   <li>A {@code seg.mergeArrayFields}-configured array is concatenated, each
     *       item stamped with {@link #SOURCE_CASE_ID_KEY} (only if not already
     *       stamped, so repeated folds don't re-stamp).</li>
     *   <li>A scalar collision (same field, different value) is resolved by
     *       latest {@code created_at} — a correction/update should win over an
     *       older submission of the same field — WARN logged.</li>
     * </ul>
     * Provenance ({@link #CASE_ID_BY_FIELD_KEY}) is read from whatever's already embedded in
     * each side — every fresh record was stamped with it at ingestion by
     * {@link #stampCaseIdByField} — rather than passed in as a separate parameter, so this
     * works uniformly regardless of how many prior merges either side has already been
     * through. A normal single-contributor bucket (the only case for segments using
     * {@code humanSubTypes}, e.g. authentication/docreview) never gets stamped in the first
     * place, so it stays untouched here too.
     */
    private GenericRecord mergeAcrossCases(GenericRecord existing, GenericRecord incoming,
                                            SegmentConfig seg, String imageId, String segment) {
        String keyId = str(existing.get("key_id"));
        String existingCreatedAt = str(existing.get("created_at"));
        String incomingCreatedAt = str(incoming.get("created_at"));

        JsonObject existingJson = decryptToJson(keyId, str(existing.get("payload")), seg);
        JsonObject incomingJson = decryptToJson(keyId, str(incoming.get("payload")), seg);
        if (existingJson == null || incomingJson == null) {
            LOG.warn("imageId={} segment={} — could not parse a colliding human payload for "
                    + "cross-case merge, keeping the later record whole", imageId, segment);
            boolean incomingIsLater = existingCreatedAt == null
                    || (incomingCreatedAt != null && incomingCreatedAt.compareTo(existingCreatedAt) > 0);
            return incomingIsLater ? incoming : existing;
        }

        Set<String> mergeArrayFields = (seg != null && seg.mergeArrayFields != null)
                ? new HashSet<>(seg.mergeArrayFields) : new HashSet<>();

        JsonObject merged = mergeJsonObjects(existingJson, existingCreatedAt,
                incomingJson, incomingCreatedAt, mergeArrayFields, "", imageId, segment);

        String reEncrypted = BarricadeEncryptionUtil.encrypt(keyId, merged.toString());
        String earliestCreatedAt = minCreatedAt(existingCreatedAt, incomingCreatedAt);

        GenericRecord result = new GenericData.Record(payloadSchema);
        result.put("image_id",     imageId);
        result.put("key_id",       keyId);
        result.put("payload_type", "human");
        result.put("payload",      reEncrypted);
        result.put("created_at",   earliestCreatedAt);
        return result;
    }

    /**
     * Pure JSON-level merge, split out from {@link #mergeAcrossCases} so it's testable without
     * going through Barricade encrypt/decrypt (which needs live GCP KMS/Firestore access and
     * has no local test seam). See {@link #mergeAcrossCases} for the field-by-field rules.
     *
     * <h3>Granularity — collision happens at the deepest level that actually differs</h3>
     * A nested JSON object is never treated as one opaque value: when both sides have an
     * object at the same key, this method recurses into it instead of comparing the two
     * objects wholesale, so a difference in one nested field doesn't discard unrelated
     * sibling fields nested alongside it. Each recursion level tracks its own
     * {@link #CASE_ID_BY_FIELD_KEY} (covering only its own direct children), so provenance
     * is as fine-grained as the collision itself. {@code pathPrefix} is the dot-notation path
     * to the object currently being merged (empty at the root) — it's what lets
     * {@code mergeArrayFields} contain nested paths (e.g. {@code "credit.dob.disputeCodes"},
     * matching the dot-notation convention {@code ARRAY_MATCH_KEYS} already uses in
     * {@link FlattenAndCompareFn}) and still match an array several levels deep, not just a
     * top-level key.
     *
     * <p>A collision only actually fires — latest-created_at-wins, WARN logged, provenance
     * recorded — for a genuine leaf-level difference: a scalar, a mismatched type, or an
     * array not listed in {@code mergeArrayFields} at that path. Attribution for each side is
     * read from that side's own {@link #CASE_ID_BY_FIELD_KEY} (absent entirely for NO_CASE
     * data) rather than passed in as a flat case_id — both sides can be arbitrarily
     * multi-case (already-merged) records, not just fresh single-case rows.
     *
     * <p>Package-visible for direct unit testing.
     */
    static JsonObject mergeJsonObjects(JsonObject existingJson, String existingCreatedAt,
                                        JsonObject incomingJson, String incomingCreatedAt,
                                        Set<String> mergeArrayFields,
                                        String pathPrefix,
                                        String imageId, String segment) {
        existingJson = existingJson.deepCopy();
        incomingJson = incomingJson.deepCopy();

        JsonObject existingByField = existingJson.has(CASE_ID_BY_FIELD_KEY)
                ? existingJson.getAsJsonObject(CASE_ID_BY_FIELD_KEY) : new JsonObject();
        JsonObject incomingByField = incomingJson.has(CASE_ID_BY_FIELD_KEY)
                ? incomingJson.getAsJsonObject(CASE_ID_BY_FIELD_KEY) : new JsonObject();
        existingJson.remove(CASE_ID_BY_FIELD_KEY);
        incomingJson.remove(CASE_ID_BY_FIELD_KEY);

        // On a scalar value collision, the LATEST created_at wins — a case correcting/updating
        // a field should take precedence over an earlier submission of the same field. A known
        // timestamp still beats an unknown one either way (existingCreatedAt != null fallback).
        boolean existingWinsTies;
        if (existingCreatedAt != null && incomingCreatedAt != null) {
            existingWinsTies = existingCreatedAt.compareTo(incomingCreatedAt) >= 0;
        } else {
            existingWinsTies = existingCreatedAt != null;
        }

        JsonObject merged = new JsonObject();
        JsonObject caseIdByField = new JsonObject();
        Set<String> allKeys = new LinkedHashSet<>();
        allKeys.addAll(existingJson.keySet());
        allKeys.addAll(incomingJson.keySet());

        for (String key : allKeys) {
            boolean inE = existingJson.has(key);
            boolean inI = incomingJson.has(key);
            String  path = pathPrefix.isEmpty() ? key : pathPrefix + "." + key;

            if (inE && !inI) {
                merged.add(key, existingJson.get(key));
                copyAttribution(existingByField, key, caseIdByField);

            } else if (!inE && inI) {
                merged.add(key, incomingJson.get(key));
                copyAttribution(incomingByField, key, caseIdByField);

            } else {
                JsonElement ev = existingJson.get(key);
                JsonElement iv = incomingJson.get(key);

                if (mergeArrayFields.contains(path) && ev.isJsonArray() && iv.isJsonArray()) {
                    String existingArrayCase = attributionOf(existingByField, key);
                    String incomingArrayCase = attributionOf(incomingByField, key);
                    JsonArray combined = new JsonArray();
                    for (JsonElement el : ev.getAsJsonArray()) {
                        combined.add(stampSourceCaseId(el, existingArrayCase));
                    }
                    for (JsonElement el : iv.getAsJsonArray()) {
                        combined.add(stampSourceCaseId(el, incomingArrayCase));
                    }
                    merged.add(key, combined);
                    // Array-level attribution lives per-item (_sourceCaseId), not here.

                } else if (ev.isJsonObject() && iv.isJsonObject()) {
                    // Recurse rather than comparing wholesale — a difference in one nested
                    // field must not discard unrelated sibling fields at this level.
                    JsonObject nestedMerged = mergeJsonObjects(
                            ev.getAsJsonObject(), existingCreatedAt,
                            iv.getAsJsonObject(), incomingCreatedAt,
                            mergeArrayFields, path, imageId, segment);
                    merged.add(key, nestedMerged);
                    // Attribution now lives inside the nested object's own _caseIdByField.

                } else if (ev.equals(iv)) {
                    merged.add(key, ev);
                    // Identical value on both sides — still attribute to whichever case is
                    // LATEST, same tie-break direction as an actual collision below, falling
                    // back to the other side only if the preferred one has no attribution
                    // recorded at all.
                    JsonObject preferred = existingWinsTies ? existingByField : incomingByField;
                    JsonObject fallback  = existingWinsTies ? incomingByField : existingByField;
                    if (!copyAttribution(preferred, key, caseIdByField)) {
                        copyAttribution(fallback, key, caseIdByField);
                    }

                } else {
                    boolean keepExisting = existingWinsTies;
                    merged.add(key, keepExisting ? ev : iv);
                    copyAttribution(keepExisting ? existingByField : incomingByField, key, caseIdByField);
                    LOG.warn("imageId={} segment={} field={} — cross-case scalar collision, "
                                    + "keeping {} value", imageId, segment, path,
                            keepExisting ? "later" : "earlier");
                }
            }
        }

        if (caseIdByField.size() > 0) {
            merged.add(CASE_ID_BY_FIELD_KEY, caseIdByField);
        }
        return merged;
    }

    /** Copies {@code sourceByField[key]} into {@code target} if present. Returns whether it was. */
    private static boolean copyAttribution(JsonObject sourceByField, String key, JsonObject target) {
        if (sourceByField.has(key)) {
            target.add(key, sourceByField.get(key));
            return true;
        }
        return false;
    }

    private static String attributionOf(JsonObject byField, String key) {
        return byField.has(key) && byField.get(key).isJsonPrimitive()
                ? byField.get(key).getAsString() : null;
    }

    private static JsonElement stampSourceCaseId(JsonElement el, String caseId) {
        if (!el.isJsonObject()) return el;
        JsonObject obj = el.getAsJsonObject().deepCopy();
        if (caseId != null && !caseId.isEmpty() && !obj.has(SOURCE_CASE_ID_KEY)) {
            obj.addProperty(SOURCE_CASE_ID_KEY, caseId);
        }
        return obj;
    }

    private JsonObject decryptToJson(String keyId, String payload, SegmentConfig seg) {
        try {
            String decrypted = BarricadeEncryptionUtil.decrypt(keyId, payload);
            String json = decrypted;
            if (seg != null && seg.isIdRequestFormat()) {
                PayloadParser.Parsed p = PayloadParser.parse(decrypted);
                if (p == null) return null;
                json = p.json();
            }
            JsonElement parsed = JsonParser.parseString(json);
            return parsed.isJsonObject() ? parsed.getAsJsonObject() : null;
        } catch (Exception e) {
            LOG.warn("Could not decrypt/parse payload for cross-case merge: {}", e.getMessage());
            return null;
        }
    }

    private static String minCreatedAt(String a, String b) {
        if (a == null) return b;
        if (b == null) return a;
        return a.compareTo(b) <= 0 ? a : b;
    }

    // ── Human completeness / resolution ──────────────────────────────────────

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

    /**
     * AI row identity is the payload alone (trimmed ciphertext) — created_at can
     * no longer participate since it's now an assigned, monotonically-bumped
     * value rather than a passthrough of the source timestamp. The Barricade
     * ciphertext is a random-IV envelope carried through unchanged, so identical
     * payload strings are strong evidence of the same row reappearing.
     */
    private static String dedupKey(String payload) {
        return strTrim(payload);
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

    private void emitGroupPendingOrAgedOut(ProcessContext ctx,
                                            String imageId, String segment, String canonicalCaseId,
                                            String keyId, String pendingType,
                                            String payload, String createdAt,
                                            Instant firstSeen, Instant now,
                                            long retryCount, long daysWaited,
                                            String matchedAiKeys, long nextIteration,
                                            String mergedCaseIds) {
        GenericRecord row = newCasePendingRow(imageId, segment, canonicalCaseId, keyId, pendingType,
                payload, createdAt, firstSeen, now, retryCount, matchedAiKeys, nextIteration,
                mergedCaseIds);
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

    private static long metaNextIteration(GenericRecord meta) {
        return meta != null ? parseLong(meta.get("next_ai_iteration")) : 0L;
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
                                             long retryCount, String matchedAiKeys,
                                             long nextIteration, String mergedCaseIds) {
        GenericRecord r = new GenericData.Record(pendingSchema);
        r.put("image_id",          imageId);
        r.put("key_id",            keyId);
        r.put("segment",           segment);
        r.put("case_id",           (caseKey == null || caseKey.isEmpty()) ? null : caseKey);
        r.put("pending_type",      pendingType);
        r.put("payload",           payload);
        r.put("created_at",        createdAt);
        r.put("first_seen_at",     TimestampUtil.formatInstant(firstSeen));
        r.put("last_retried_at",   TimestampUtil.formatInstant(now));
        r.put("retry_count",       retryCount);
        r.put("matched_ai_keys",   matchedAiKeys);
        r.put("next_ai_iteration", nextIteration);
        r.put("merged_case_ids",   mergedCaseIds);
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
        r.put("retry_count",        parseLong(row.get("retry_count")));
        r.put("matched_ai_keys",    row.get("matched_ai_keys"));
        r.put("next_ai_iteration",  parseLong(row.get("next_ai_iteration")));
        r.put("merged_case_ids",    row.get("merged_case_ids"));
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
