package com.yourorg.pipeline.transforms;

import com.google.api.services.bigquery.model.TableRow;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.yourorg.pipeline.config.SegmentConfig;
import com.yourorg.pipeline.util.BarricadeEncryptionUtil;
import com.yourorg.pipeline.util.JsonFieldExtractor;
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

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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
 * <h3>AI matching: always the latest, only on change</h3>
 * A group is compared against the single <em>latest</em> AI payload only — never
 * a backlog of unmatched history. Each run computes a signature of
 * {@code (humanRec payload, latest AI payload)} (see {@link #dedupKey}) and
 * compares it against the group's persisted {@code last_compared_signature}; a
 * {@code MATCHED} pair is emitted only when that signature changed — i.e. when
 * either side's content actually differs from what was last compared. This means
 * a human update (a case merging in, a field being corrected) triggers a fresh
 * comparison against the current AI payload even if that exact AI payload was
 * already compared before. {@code comparison_version} is a simple counter
 * (incremented each time a new comparison actually fires) carried on the
 * {@code humanRec} record for {@link FlattenAndCompareFn} to write into
 * {@code ai_iteration} — it's an observability counter, not a replay index.
 * {@code matched_ai_keys}/{@code next_ai_iteration} are deprecated remnants of
 * the old full-history-replay design and are no longer populated.
 * {@code comparison_results} stays plain append-only — a superseded comparison's
 * rows are neither deleted nor hidden; the table can hold more than one
 * comparison per group over its history.
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

    /**
     * Sentinel {@code humanByCase} bucket key for the persisted pending row's OWN consolidated
     * group state — deliberately not a value any real {@code case_id} is expected to take
     * (double-underscore-wrapped, unlike any real case-management system's id); {@link #NO_CASE}
     * is separately {@code ""}.
     *
     * <p>The persisted pending row represents the WHOLE group's already-merged state (every
     * contributing case combined so far) — it is not "case1's own row." Folding it under the
     * group's {@code canonicalCaseId} (as if it belonged to just one case) used to collide with
     * a genuine fresh per-case row for that same case_id whenever both were visible in the same
     * run (e.g. case1's row re-surfacing via {@code humanLookbackDays} long after it first
     * arrived) — {@link #foldPendingHuman}'s {@code putIfAbsent} would then silently lose the
     * pending row (and with it, every OTHER case's contribution already folded into it) in
     * favor of case1's bare individual content. Using this sentinel instead means the pending
     * row's fold can never collide with any real case's fresh row, so it always survives to be
     * merged into the final consolidated record — see the "Consolidate" step in
     * {@link #processElement}.
     */
    private static final String PENDING_GROUP_KEY = "__pending_group_state__";

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
        // Colliding rows (more than one physical AI_PENDING_TAG row for the same payload) are
        // reconciled via mergeAiPendingMeta — the AI-side counterpart to mergeGroupMeta below.
        // A plain overwrite (or blindly adding one aiRows entry per physical row) would silently
        // lose whichever row's first_seen_at/retry_count didn't survive (incorrect aging), and
        // would re-emit every physical duplicate forever in the "always re-pend" loop below,
        // never collapsing them — the same class of bug mergeGroupMeta guards against for the
        // human pending row.
        Map<String, GenericRecord> aiPendingMeta = new HashMap<>();
        for (TableRow pr : result.getAll(AI_PENDING_TAG)) {
            GenericRecord p = toAiPendingRecord(pr);
            aiPendingMeta.merge(dedupKey(str(p.get("payload"))), p, FilterAndPairFn::mergeAiPendingMeta);
        }
        Instant maxAiCreatedAt = null;
        for (GenericRecord p : aiPendingMeta.values()) {
            String createdAt = str(p.get("created_at"));
            aiRows.add(newPayloadRow(imageId, str(p.get("key_id")), "ai",
                    str(p.get("payload")), createdAt));
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

        // ── Fold in existing pending rows ────────────────────────────────────────
        // Folded under PENDING_GROUP_KEY, NOT the persisted row's own case_id column — that
        // column is the group's canonicalCaseId (earliest-arriving contributor), but the row
        // itself is the WHOLE group's already-merged state, not just that one case's content.
        // Folding it under a real case's bucket would let it collide with (and lose to, via
        // foldPendingHuman's putIfAbsent) a genuine fresh re-read of that case's own row —
        // silently dropping every OTHER case's already-merged contribution. See
        // PENDING_GROUP_KEY's own doc.
        //
        // canonicalCaseId/canonicalCreatedAt are seeded here from each persisted row's OWN
        // case_id/created_at columns — even though its CONTENT is folded under the sentinel
        // bucket (excluded from case identity below), it still represents a real, already-known
        // contributing case_id whose arrival time must still count when picking the group's
        // canonical (earliest) case — otherwise a case known only via persisted state (not
        // freshly re-surfacing this run) would be forgotten and a later-arriving fresh case
        // could wrongly become canonical instead.
        Map<String, GenericRecord> groupPendingMeta = new HashMap<>();
        Set<String>                allContributingCaseIds = new LinkedHashSet<>();
        String   canonicalCaseId = null;
        String   canonicalCreatedAt = null;
        for (TableRow pr : result.getAll(CASE_PENDING_TAG)) {
            GenericRecord p        = toCasePendingRecord(pr);
            String        pType    = str(p.get("pending_type"));
            String        createdAt = str(p.get("created_at"));
            groupPendingMeta.merge(pType, p, FilterAndPairFn::mergeGroupMeta);

            GenericRecord rec = newPayloadRow(imageId, str(p.get("key_id")), "human",
                    str(p.get("payload")), createdAt);
            if ("human:merged".equals(pType)) {
                foldPendingHuman(humanByCase, PENDING_GROUP_KEY, "_merged", rec);
            } else if (pType != null && pType.startsWith("human:")) {
                foldPendingHuman(humanByCase, PENDING_GROUP_KEY, pType.substring(6), rec);
            } else if ("human".equals(pType)) {
                foldPendingHuman(humanByCase, PENDING_GROUP_KEY, "default", rec);
            }

            String persistedCaseKey = normalizeCaseKey(str(p.get("case_id")));
            if (!persistedCaseKey.isEmpty()) allContributingCaseIds.add(persistedCaseKey);
            if (!persistedCaseKey.isEmpty()
                    && (canonicalCaseId == null
                        || (createdAt != null
                            && (canonicalCreatedAt == null
                                || createdAt.compareTo(canonicalCreatedAt) < 0)))) {
                canonicalCaseId    = persistedCaseKey;
                canonicalCreatedAt = createdAt;
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
        //
        // humanByCase includes the PENDING_GROUP_KEY bucket (the persisted group's own
        // already-merged state, see above) alongside every real case's bucket — it's folded
        // into humanBySubType via the same mergeAcrossCases path as any other contributor, so
        // a case whose fresh row isn't visible this run (outside its own segment's relevant
        // window) still has its prior contribution represented. PENDING_GROUP_KEY is excluded
        // from case-identity bookkeeping (allContributingCaseIds/canonicalCaseId) — it isn't a
        // case; canonicalCaseId/allContributingCaseIds were already seeded from each persisted
        // row's own case_id column above, so a case known only via persisted state is still
        // correctly represented in the group's identity even when its fresh row isn't visible
        // this run.
        Map<String, GenericRecord> humanBySubType = new LinkedHashMap<>();

        for (Map.Entry<String, Map<String, GenericRecord>> caseEntry : humanByCase.entrySet()) {
            String  caseKey            = caseEntry.getKey();
            boolean isPendingGroupState = PENDING_GROUP_KEY.equals(caseKey);
            if (!isPendingGroupState && !caseKey.isEmpty()) allContributingCaseIds.add(caseKey);

            for (Map.Entry<String, GenericRecord> e : caseEntry.getValue().entrySet()) {
                String subType = e.getKey();
                GenericRecord incoming = e.getValue();

                if (!isPendingGroupState && !caseKey.isEmpty()) {
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
                        meta != null ? str(meta.get("last_compared_signature")) : null,
                        metaComparisonVersion(meta), mergedCaseIds, false);
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
        String persistedSignature = meta != null ? str(meta.get("last_compared_signature")) : null;
        long comparisonVersion = metaComparisonVersion(meta);
        Instant firstSeen = meta != null ? parseInstant(str(meta.get("first_seen_at"))) : null;
        if (firstSeen == null) firstSeen = now;
        long daysWaited = ChronoUnit.DAYS.between(firstSeen, now);

        // Always compare against the single latest AI payload, never a backlog of unmatched
        // ones — a comparison fires whenever EITHER side's content has changed since the last
        // one: a new/updated human contribution (a case merging in, a field being corrected)
        // or a genuinely new AI payload. See FilterAndPairFn class doc.
        //
        // The human side of the signature MUST be based on decrypted content, not the
        // ciphertext — humanRec.payload gets re-encrypted (fresh random IV) every time this
        // group is touched, including a no-op re-read of the same row caused purely by
        // --humanLookbackDays re-selecting it from source with zero actual content change.
        // Signing the ciphertext made every such re-read look like a genuine change, firing a
        // spurious comparison and appending duplicate rows every run for as long as the
        // lookback window kept re-selecting it. The AI side doesn't have this problem — AI
        // payloads are never re-encrypted after ingestion, so their ciphertext is already a
        // stable identity (see dedupKey's own doc).
        GenericRecord latestAi = aiRows.isEmpty() ? null : aiRows.get(aiRows.size() - 1);
        String currentSignature = latestAi != null
                ? humanContentSignature(humanRec, seg) + "|" + dedupKey(str(latestAi.get("payload")))
                : null;

        boolean justMatched = latestAi != null && !Objects.equals(currentSignature, persistedSignature);
        if (justMatched) {
            comparisonVersion += 1;
            humanRec.put("comparison_version", comparisonVersion);
            ctx.output(MATCHED, KV.of(imageId + "::" + segment, KV.of(humanRec, latestAi)));
            LOG.info("Compared imageId={} segment={} — signature changed, comparison_version={}",
                    imageId, segment, comparisonVersion);
        }

        emitGroupPendingOrAgedOut(ctx, imageId, segment, canonicalCaseId,
                str(humanRec.get("key_id")), humanPType, str(humanRec.get("payload")), hCAt,
                firstSeen, now, metaRetryCount(meta), daysWaited,
                currentSignature != null ? currentSignature : persistedSignature,
                comparisonVersion, mergedCaseIds, justMatched);
    }

    // ── Case bucketing (accumulation only) ─────────────────────────────────────

    private static String normalizeCaseKey(String rawCaseId) {
        return (rawCaseId != null && !rawCaseId.isBlank()) ? rawCaseId.trim() : NO_CASE;
    }

    private static GenericRecord mergeGroupMeta(GenericRecord left, GenericRecord right) {
        // matched_ai_keys/next_ai_iteration are deprecated (replay-based matching removed —
        // see FilterAndPairFn class doc); left as-is (unpopulated going forward) rather than
        // dropped, per this codebase's additive-only migration convention.

        // Two colliding persisted pending rows for the same group only happens as a
        // migration-transition edge case (leftover per-case rows from before case_id became
        // lineage). comparison_version and last_compared_signature MUST come from the SAME
        // row — they're a pair describing one specific comparison, and mixing version from one
        // row with the signature from a different (older/stale) row produces an internally
        // inconsistent state that can never match a freshly-computed signature again, causing
        // every future run to see "changed" and re-compare forever. Picking whichever side has
        // the higher comparison_version as the sole source for both fields keeps them
        // consistent; a tie keeps left (arbitrary but stable) since both would carry the same
        // version.
        boolean rightIsNewer = metaComparisonVersion(right) > metaComparisonVersion(left);
        GenericRecord newer = rightIsNewer ? right : left;
        long   comparisonVersion     = metaComparisonVersion(newer);
        String lastComparedSignature = str(newer.get("last_compared_signature"));

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
        left.put("last_compared_signature", lastComparedSignature);
        left.put("comparison_version", comparisonVersion);

        Set<String> merged = parseKeys(str(left.get("merged_case_ids")));
        merged.addAll(parseKeys(str(right.get("merged_case_ids"))));
        left.put("merged_case_ids", joinKeys(merged));
        return left;
    }

    /**
     * Reconciles two colliding persisted AI-pending rows for the same payload (same
     * {@link #dedupKey}) — the AI-side counterpart to {@link #mergeGroupMeta}. A plain
     * overwrite would silently lose whichever row's {@code first_seen_at}/{@code retry_count}
     * didn't survive, incorrectly resetting or extending that payload's aging clock depending
     * on read order; folding every physical row into {@code aiRows} one-for-one (rather than
     * once per distinct payload after this reconciliation) would also re-emit a duplicate row
     * forever in the "always re-pend every AI row" loop, never collapsing it.
     */
    private static GenericRecord mergeAiPendingMeta(GenericRecord left, GenericRecord right) {
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

        // created_at is the assigned, monotonically-bumped identity timestamp for this payload
        // (see AI_TIMESTAMP_BUMP_SECONDS) — keep whichever is earlier, consistent with
        // first_seen_at reflecting when this payload was originally discovered.
        String leftCreatedAt  = str(left.get("created_at"));
        String rightCreatedAt = str(right.get("created_at"));
        if (rightCreatedAt != null
                && (leftCreatedAt == null || rightCreatedAt.compareTo(leftCreatedAt) < 0)) {
            left.put("created_at", rightCreatedAt);
        }

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

    /**
     * Folds a carried-forward pending human row in — never overrides a fresh row already
     * present at the same {@code (caseKey, subType)} bucket. Always called with
     * {@code caseKey = }{@link #PENDING_GROUP_KEY} for the persisted group-state row (see its
     * own doc for why that dedicated bucket exists) — {@code putIfAbsent} still matters there
     * because a group can have more than one pending row per run only as a migration-transition
     * edge case (see {@link #mergeGroupMeta}), where the first one folded should win rather
     * than silently clobbering the other.
     */
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

        Set<String> atomicPaths = (seg != null && seg.atomicObjectFields != null)
                ? seg.atomicObjectFields.keySet() : Collections.emptySet();
        stampObjectRecursively(json, caseKey, "", atomicPaths);

        record.put("payload", BarricadeEncryptionUtil.encrypt(keyId, json.toString()));
        return record;
    }

    /** Package-visible for direct unit testing (see {@link #mergeJsonObjects}). */
    static void stampObjectRecursively(JsonObject obj, String caseKey) {
        stampObjectRecursively(obj, caseKey, "", Collections.emptySet());
    }

    /**
     * @param pathPrefix  dot-notation path to {@code obj} (empty at the root).
     * @param atomicPaths paths (see {@code SegmentConfig.atomicObjectFields}) that must NOT
     *                    be recursed into — stamped as a leaf (the key itself still gets
     *                    tagged in its parent's map) but its own contents are left alone,
     *                    since {@link #mergeAtomicWithSlots} — not the normal recursive
     *                    merge — is what handles those paths later.
     */
    static void stampObjectRecursively(JsonObject obj, String caseKey, String pathPrefix,
                                        Set<String> atomicPaths) {
        JsonObject caseIdByField = new JsonObject();
        for (String key : obj.keySet()) {
            caseIdByField.addProperty(key, caseKey);
        }
        obj.add(CASE_ID_BY_FIELD_KEY, caseIdByField);

        for (String key : obj.keySet()) {
            if (CASE_ID_BY_FIELD_KEY.equals(key)) continue;
            String path = pathPrefix.isEmpty() ? key : pathPrefix + "." + key;
            if (atomicPaths.contains(path)) continue;
            JsonElement child = obj.get(key);
            if (child.isJsonObject()) {
                stampObjectRecursively(child.getAsJsonObject(), caseKey, path, atomicPaths);
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
        Map<String, Set<String>> atomicObjectFields = new HashMap<>();
        if (seg != null && seg.atomicObjectFields != null) {
            for (Map.Entry<String, List<String>> e : seg.atomicObjectFields.entrySet()) {
                atomicObjectFields.put(e.getKey(), new HashSet<>(e.getValue()));
            }
        }
        Map<String, String> arrayItemPriorityField = (seg != null && seg.arrayItemPriorityField != null)
                ? seg.arrayItemPriorityField : Collections.emptyMap();
        Map<String, String> mergeItemKeyField = (seg != null && seg.mergeItemKeyField != null)
                ? seg.mergeItemKeyField : Collections.emptyMap();

        JsonObject merged = mergeJsonObjects(existingJson, existingCreatedAt,
                incomingJson, incomingCreatedAt, mergeArrayFields, atomicObjectFields,
                arrayItemPriorityField, mergeItemKeyField, "", imageId, segment);

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
     * <h3>Exception — {@code atomicObjectFields}</h3>
     * A path configured in {@code SegmentConfig.atomicObjectFields} (e.g.
     * {@code creditReportHeader}) skips the normal per-field recursion — see
     * {@link #mergeAtomicWithSlots} — because it represents one holistic record, not a set
     * of independently-collidable fields.
     *
     * <p>Package-visible for direct unit testing.
     */
    static JsonObject mergeJsonObjects(JsonObject existingJson, String existingCreatedAt,
                                        JsonObject incomingJson, String incomingCreatedAt,
                                        Set<String> mergeArrayFields,
                                        Map<String, Set<String>> atomicObjectFields,
                                        Map<String, String> arrayItemPriorityField,
                                        Map<String, String> mergeItemKeyField,
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
                    // Pair items by SegmentConfig.mergeItemKeyField when configured for this
                    // path, otherwise fall back to the same composite key FlattenAndCompareFn
                    // uses for AI-vs-human comparison (ARRAY_MATCH_KEYS) — the two can differ
                    // (e.g. addresses compares by content but merges by addressType). A key
                    // present on only one side is kept as-is; a key on both sides with
                    // identical content is deduped to one copy; a key on both sides with
                    // DIFFERENT content is a genuine item-level collision, resolved by the same
                    // latest-created_at-wins rule as a scalar collision (WARN logged) — not
                    // silently dropped. Falls back to plain concatenation (no pairing at all)
                    // for an array path with no configured match key either way.
                    String keySpec = mergeItemKeyField.containsKey(path)
                            ? mergeItemKeyField.get(path)
                            : FlattenAndCompareFn.ARRAY_MATCH_KEYS.get(path);
                    String priorityField = arrayItemPriorityField.get(path);
                    JsonArray combined = mergeArrayItems(ev.getAsJsonArray(), existingArrayCase,
                            iv.getAsJsonArray(), incomingArrayCase, keySpec, priorityField, path,
                            existingWinsTies, imageId, segment);
                    merged.add(key, combined);
                    // Array-level attribution lives per-item (_sourceCaseId), not here.

                } else if (atomicObjectFields.containsKey(path) && ev.isJsonObject() && iv.isJsonObject()) {
                    // This path represents one holistic record (e.g. creditReportHeader), not
                    // independently-collidable fields — the latest side's fields win wholesale
                    // except for a configured set of "slot" sub-objects, each resolved
                    // independently (latest wins if present, else carried forward from the
                    // other side). See mergeAtomicWithSlots.
                    String winnerCase = attributionOf(existingWinsTies ? existingByField : incomingByField, key);
                    JsonObject atomicMerged = mergeAtomicWithSlots(
                            ev.getAsJsonObject(), existingByField,
                            iv.getAsJsonObject(), incomingByField,
                            atomicObjectFields.get(path), existingWinsTies, key);
                    merged.add(key, atomicMerged);
                    // Non-slot fields need no per-field attribution — they all uniformly come
                    // from the latest case, so a single entry here lets resolveCaseId's walk-up
                    // fallback cover every one of them without duplicating winnerCase onto each
                    // field individually. Only a slot carried forward from the OTHER (older)
                    // case needs its own explicit override — see mergeAtomicWithSlots.
                    if (winnerCase != null && !winnerCase.isEmpty()) {
                        caseIdByField.addProperty(key, winnerCase);
                    }

                } else if (ev.isJsonObject() && iv.isJsonObject()) {
                    // Recurse rather than comparing wholesale — a difference in one nested
                    // field must not discard unrelated sibling fields at this level.
                    JsonObject nestedMerged = mergeJsonObjects(
                            ev.getAsJsonObject(), existingCreatedAt,
                            iv.getAsJsonObject(), incomingCreatedAt,
                            mergeArrayFields, atomicObjectFields, arrayItemPriorityField,
                            mergeItemKeyField, path, imageId, segment);
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

    /**
     * Merges a nested object configured in {@code SegmentConfig.atomicObjectFields} (e.g.
     * {@code creditReportHeader}) — one holistic record, not a set of independently-collidable
     * fields, except for a configured set of self-contained "slot" sub-objects (e.g.
     * {@code dateOfBirthRequested}, {@code currentNameRequested},
     * {@code socialSecurityNumberRequested} — each its own dispute-request record).
     * <ul>
     *   <li>Every field NOT in {@code slotKeys} is taken wholesale from whichever side is
     *       latest ({@code existingWinsTies}) — not blended field-by-field. A field the
     *       non-winning side had but the winning side doesn't is dropped, not carried
     *       forward: "taken from the latest case" means the latest case's own field set
     *       defines what survives. These fields carry no per-field attribution of their
     *       own — the caller records one entry for the whole object instead, and
     *       resolveCaseId's path walk-up applies it to every one of them.</li>
     *   <li>Each {@code slotKey} is resolved independently: the latest side's version wins
     *       if it has that slot; otherwise the OTHER side's version is carried forward
     *       (appended), not lost, since the latest side simply never touched that slot. A
     *       winner-sourced slot needs no attribution of its own either (same walk-up
     *       fallback as the non-slot fields); only a slot carried forward from the OLDER
     *       side needs an explicit override recorded here, since it must NOT inherit the
     *       object-level winnerCase default.</li>
     * </ul>
     * Each slot's own contents are copied wholesale too (not merged internally) — it's
     * treated as one atomic value, same spirit as the object it lives in.
     */
    private static JsonObject mergeAtomicWithSlots(JsonObject existingObj, JsonObject existingByFieldParent,
                                                     JsonObject incomingObj, JsonObject incomingByFieldParent,
                                                     Set<String> slotKeys, boolean existingWinsTies,
                                                     String parentKey) {
        JsonObject winnerObj = existingWinsTies ? existingObj : incomingObj;
        JsonObject loserObj  = existingWinsTies ? incomingObj : existingObj;
        String loserCase  = attributionOf(existingWinsTies ? incomingByFieldParent : existingByFieldParent, parentKey);

        JsonObject merged = new JsonObject();
        JsonObject caseIdByField = new JsonObject();

        for (String key : winnerObj.keySet()) {
            if (CASE_ID_BY_FIELD_KEY.equals(key) || slotKeys.contains(key)) continue;
            merged.add(key, winnerObj.get(key));
        }

        for (String slotKey : slotKeys) {
            if (winnerObj.has(slotKey)) {
                merged.add(slotKey, winnerObj.get(slotKey));
            } else if (loserObj.has(slotKey)) {
                merged.add(slotKey, loserObj.get(slotKey));
                if (loserCase != null && !loserCase.isEmpty()) caseIdByField.addProperty(slotKey, loserCase);
            }
        }

        if (caseIdByField.size() > 0) merged.add(CASE_ID_BY_FIELD_KEY, caseIdByField);
        return merged;
    }

    /**
     * Merges two arrays for a {@code mergeArrayFields} field, pairing items by the same
     * {@code ARRAY_MATCH_KEYS} composite key {@link FlattenAndCompareFn} uses for comparison.
     * <ul>
     *   <li>An item whose key appears on only one side is kept as-is.</li>
     *   <li>Both sides have an item with the same key, identical content — deduped to one
     *       copy, attributed to the latest side (no warning; nothing was actually lost).</li>
     *   <li>Both sides have an item with the same key, <em>different</em> content — a genuine
     *       item-level collision. If {@code priorityField} is configured for this array path
     *       (see {@code SegmentConfig.arrayItemPriorityField}) and exactly one side's item has
     *       that field present, that side wins outright, regardless of which case is latest —
     *       e.g. an {@code addresses} slot actively under dispute ({@code addressRequested}
     *       present) must not be silently overwritten by another case's unchanged resubmission
     *       of the same slot. Otherwise (no priority field configured, or both/neither side has
     *       it) falls back to the same latest-{@code created_at}-wins rule a scalar collision
     *       uses. Either way, WARN logged.</li>
     * </ul>
     * When {@code keySpec} is {@code null} (no configured match key for this array path) or an
     * item's key can't be computed, no pairing is attempted for it — falls back to plain
     * concatenation, same as before composite keys existed.
     */
    private static JsonArray mergeArrayItems(JsonArray existingArr, String existingArrayCase,
                                              JsonArray incomingArr, String incomingArrayCase,
                                              String keySpec, String priorityField, String arrayPath,
                                              boolean existingWinsTies,
                                              String imageId, String segment) {
        JsonArray combined = new JsonArray();

        if (keySpec == null) {
            for (JsonElement el : existingArr) combined.add(stampSourceCaseId(el, existingArrayCase));
            for (JsonElement el : incomingArr) combined.add(stampSourceCaseId(el, incomingArrayCase));
            return combined;
        }

        Map<String, JsonElement> existingByKey = new LinkedHashMap<>();
        for (JsonElement el : existingArr) {
            String itemKey = JsonFieldExtractor.extractKeyValue(el, keySpec, arrayPath);
            if (itemKey != null) existingByKey.put(itemKey, el);
            else combined.add(stampSourceCaseId(el, existingArrayCase)); // unkeyable — no pairing possible
        }
        Map<String, JsonElement> incomingByKey = new LinkedHashMap<>();
        for (JsonElement el : incomingArr) {
            String itemKey = JsonFieldExtractor.extractKeyValue(el, keySpec, arrayPath);
            if (itemKey != null) incomingByKey.put(itemKey, el);
            else combined.add(stampSourceCaseId(el, incomingArrayCase));
        }

        Set<String> allItemKeys = new LinkedHashSet<>();
        allItemKeys.addAll(existingByKey.keySet());
        allItemKeys.addAll(incomingByKey.keySet());

        for (String itemKey : allItemKeys) {
            JsonElement e1 = existingByKey.get(itemKey);
            JsonElement e2 = incomingByKey.get(itemKey);

            if (e1 != null && e2 == null) {
                combined.add(stampSourceCaseId(e1, existingArrayCase));

            } else if (e1 == null) {
                combined.add(stampSourceCaseId(e2, incomingArrayCase));

            } else if (e1.equals(e2)) {
                boolean keepExisting = existingWinsTies;
                combined.add(stampSourceCaseId(keepExisting ? e1 : e2,
                        keepExisting ? existingArrayCase : incomingArrayCase));

            } else {
                boolean e1HasPriority = priorityField != null
                        && e1.isJsonObject() && e1.getAsJsonObject().has(priorityField);
                boolean e2HasPriority = priorityField != null
                        && e2.isJsonObject() && e2.getAsJsonObject().has(priorityField);

                boolean keepExisting;
                String  reason;
                if (e1HasPriority != e2HasPriority) {
                    // Exactly one side has the priority marker (e.g. addressRequested) — it
                    // wins outright, regardless of which case is latest.
                    keepExisting = e1HasPriority;
                    reason = "has '" + priorityField + "'";
                } else {
                    keepExisting = existingWinsTies;
                    reason = keepExisting ? "later" : "earlier";
                }

                combined.add(stampSourceCaseId(keepExisting ? e1 : e2,
                        keepExisting ? existingArrayCase : incomingArrayCase));
                LOG.warn("imageId={} segment={} field={} itemKey={} — cross-case array item "
                                + "collision, keeping {} item ({})", imageId, segment, arrayPath, itemKey,
                        keepExisting ? "existing" : "incoming", reason);
            }
        }
        return combined;
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

    /**
     * Stable content-identity signature for the human side of a comparison, used to decide
     * whether a new comparison is needed (see the {@code currentSignature} computation above).
     * Deliberately based on the <em>decrypted</em> payload, not {@code humanRec.get("payload")}
     * (the ciphertext) — Barricade re-encryption uses a random IV, so the same logical content
     * produces a different ciphertext every time this record is touched, including a pure
     * lookback re-read with no actual change. Hashed (SHA-256), not returned as raw JSON, so no
     * decrypted PII ever lands in the plaintext {@code last_compared_signature} column.
     */
    private String humanContentSignature(GenericRecord humanRec, SegmentConfig seg) {
        String keyId = str(humanRec.get("key_id"));
        JsonObject json = decryptToJson(keyId, str(humanRec.get("payload")), seg);
        if (json == null) {
            // Could not decrypt/parse — fall back to ciphertext identity. Less stable across
            // lookback re-reads, but only reachable if the payload is malformed to begin with.
            return dedupKey(str(humanRec.get("payload")));
        }
        return sha256Hex(json.toString());
    }

    private static String sha256Hex(String s) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(s.getBytes(StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder(hash.length * 2);
            for (byte b : hash) sb.append(String.format("%02x", b));
            return sb.toString();
        } catch (NoSuchAlgorithmException e) {
            // SHA-256 is a mandatory JCE algorithm on every standard JVM — not reachable in practice.
            throw new IllegalStateException("SHA-256 unavailable", e);
        }
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

    /**
     * @param justMatched true when a MATCHED comparison was just emitted this same pass — a
     *                    group that just successfully compared is by definition no longer
     *                    "waiting" and must always go to CASE_PENDING (carrying forward its
     *                    fresh last_compared_signature/comparison_version), never
     *                    CASE_AGED_OUT, regardless of how long firstSeen says it waited.
     *                    Aging out on the same pass a group matches would drop that
     *                    bookkeeping from pending_comparisons, making the (unchanged) content
     *                    look "brand new" and re-trigger a spurious duplicate comparison on a
     *                    later run once humanLookbackDays/aiLookbackHours re-surface it.
     */
    private void emitGroupPendingOrAgedOut(ProcessContext ctx,
                                            String imageId, String segment, String canonicalCaseId,
                                            String keyId, String pendingType,
                                            String payload, String createdAt,
                                            Instant firstSeen, Instant now,
                                            long retryCount, long daysWaited,
                                            String lastComparedSignature, long comparisonVersion,
                                            String mergedCaseIds, boolean justMatched) {
        GenericRecord row = newCasePendingRow(imageId, segment, canonicalCaseId, keyId, pendingType,
                payload, createdAt, firstSeen, now, retryCount, lastComparedSignature,
                comparisonVersion, mergedCaseIds);
        if (daysWaited >= MAX_WAIT_DAYS && !justMatched) {
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

    private static long metaComparisonVersion(GenericRecord meta) {
        return meta != null ? parseLong(meta.get("comparison_version")) : 0L;
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
                                             long retryCount, String lastComparedSignature,
                                             long comparisonVersion, String mergedCaseIds) {
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
        // matched_ai_keys/next_ai_iteration are deprecated (replay-based matching removed, see
        // class doc) — still written since next_ai_iteration is a non-nullable Avro/BQ field,
        // but always these fixed values going forward rather than anything meaningful.
        r.put("matched_ai_keys",   (String) null);
        r.put("next_ai_iteration", 0L);
        r.put("last_compared_signature", lastComparedSignature);
        r.put("comparison_version",      comparisonVersion);
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
        r.put("last_compared_signature", row.get("last_compared_signature"));
        r.put("comparison_version",      parseLong(row.get("comparison_version")));
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
