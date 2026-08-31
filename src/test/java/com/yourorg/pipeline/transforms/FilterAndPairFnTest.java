package com.yourorg.pipeline.transforms;

import com.google.api.services.bigquery.model.TableRow;
import com.yourorg.pipeline.util.SchemaRegistry;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.junit.Rule;
import org.junit.Test;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.beam.sdk.options.ValueProvider;

import static org.junit.Assert.*;

public class FilterAndPairFnTest {

    @Rule
    public final TestPipeline pipeline = TestPipeline.create();

    private static final String AI_METHOD    = "aimetadata";
    private static final String HUMAN_METHOD = "controller.SubmitDispute";

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static String payloadFor(String imageId) {
        return "{\"image_name\":\"" + imageId + "\"}";
    }

    /**
     * Distinct AI payload content for a given marker. AI row identity is now the payload
     * string alone (see FilterAndPairFn.dedupKey) — real Barricade ciphertext differs per
     * submission via a random IV, so tests with more than one distinct AI event in the same
     * run must use distinguishable payload content, not just a different created_at.
     */
    private static String aiPayloadFor(String imageId, String marker) {
        return "{\"image_name\":\"" + imageId + "\",\"ai_marker\":\"" + marker + "\"}";
    }

    /** The AI identity key FilterAndPairFn derives internally: the payload string alone. */
    private static String aiDedupKey(String payload) {
        return payload;
    }

    /** Creates a source TableRow (payload is plain JSON — FilterAndPairFn never parses it). */
    private static TableRow sourceRow(String imageId, String method,
                                      String keyId, String createdAt) {
        return sourceRow(imageId, method, keyId, createdAt, null);
    }

    private static TableRow sourceRow(String imageId, String method,
                                      String keyId, String createdAt, String caseId) {
        return sourceRowWithPayload(imageId, method, keyId, createdAt, caseId, payloadFor(imageId));
    }

    private static TableRow sourceRowWithPayload(String imageId, String method, String keyId,
                                                  String createdAt, String caseId, String payload) {
        TableRow row = new TableRow()
                .set("payload",    payload)
                .set("key_id",     keyId)
                .set("method",     method)
                .set("created_at", createdAt);
        if (caseId != null) row.set("case_id", caseId);
        return row;
    }

    /** Creates a case (human-side) pending TableRow (defaults to segment "main"). */
    private static TableRow casePendingRow(String imageId, String pendingType,
                                           String keyId, String createdAt, String firstSeenAt) {
        return casePendingRow(imageId, pendingType, keyId, createdAt, firstSeenAt, 0L, null, null);
    }

    private static TableRow casePendingRow(String imageId, String pendingType,
                                           String keyId, String createdAt, String firstSeenAt,
                                           Object retryCount, String caseId, String matchedAiKeys) {
        // Default nextIteration to the key count — mirrors the (correct, in the absence of
        // any merge artifact) common case where every matched key corresponds to exactly one
        // real emission. Tests that need to demonstrate the two diverging use the explicit
        // overload below instead.
        long defaultNextIteration = matchedAiKeys == null || matchedAiKeys.isBlank()
                ? 0L : matchedAiKeys.split(";").length;
        return casePendingRow(imageId, pendingType, keyId, createdAt, firstSeenAt,
                retryCount, caseId, matchedAiKeys, defaultNextIteration);
    }

    private static TableRow casePendingRow(String imageId, String pendingType,
                                           String keyId, String createdAt, String firstSeenAt,
                                           Object retryCount, String caseId, String matchedAiKeys,
                                           long nextIteration) {
        TableRow row = new TableRow()
                .set("image_id",          imageId)
                .set("segment",           "main")
                .set("pending_type",      pendingType)
                .set("payload",           payloadFor(imageId))
                .set("key_id",            keyId)
                .set("created_at",        createdAt)
                .set("first_seen_at",     firstSeenAt)
                .set("last_retried_at",   firstSeenAt)
                .set("retry_count",       retryCount)
                .set("next_ai_iteration", nextIteration);
        if (caseId != null) row.set("case_id", caseId);
        if (matchedAiKeys != null) row.set("matched_ai_keys", matchedAiKeys);
        return row;
    }

    /** Creates an AI pending (replay pool) TableRow. */
    private static TableRow aiPendingRow(String imageId, String keyId,
                                         String createdAt, String firstSeenAt) {
        return aiPendingRow(imageId, keyId, createdAt, firstSeenAt, 0L);
    }

    private static TableRow aiPendingRow(String imageId, String keyId,
                                         String createdAt, String firstSeenAt, Object retryCount) {
        return aiPendingRowWithPayload(imageId, keyId, createdAt, firstSeenAt, retryCount,
                payloadFor(imageId));
    }

    private static TableRow aiPendingRowWithPayload(String imageId, String keyId,
                                         String createdAt, String firstSeenAt, Object retryCount,
                                         String payload) {
        return new TableRow()
                .set("image_id",        imageId)
                .set("segment",         "main")
                .set("payload",         payload)
                .set("key_id",          keyId)
                .set("created_at",      createdAt)
                .set("first_seen_at",   firstSeenAt)
                .set("last_retried_at", firstSeenAt)
                .set("retry_count",     retryCount);
    }

    /** Wires source + case pending + AI pending rows through CoGroupByKey → FilterAndPairFn. */
    private PCollectionTuple runPipeline(String imageId,
                                          List<TableRow> sourceRows,
                                          List<TableRow> casePendingRows,
                                          List<TableRow> aiPendingRows) {
        PCollection<KV<String, TableRow>> keyedSource = pipeline
                .apply("CreateSource", Create.of(sourceRows).withCoder(TableRowJsonCoder.of()))
                .apply("KeySource", WithKeys.<String, TableRow>of(r -> imageId)
                        .withKeyType(TypeDescriptors.strings()));

        PCollection<KV<String, TableRow>> keyedCasePending = keyRows(casePendingRows, imageId, "KeyCasePending");
        PCollection<KV<String, TableRow>> keyedAiPending   = keyRows(aiPendingRows, imageId, "KeyAiPending");

        SchemaRegistry registry = SchemaRegistry.getInstance();
        String segJson = "[{\"name\":\"main\",\"aiMethod\":\"" + AI_METHOD
                + "\",\"humanMethod\":\"" + HUMAN_METHOD + "\"}]";
        AvroCoder<GenericRecord> payloadCoder =
                AvroCoder.of(GenericRecord.class, registry.get(SchemaRegistry.PAYLOAD_ROW));
        AvroCoder<GenericRecord> pendingCoder =
                AvroCoder.of(GenericRecord.class, registry.get(SchemaRegistry.PENDING_ROW));
        AvroCoder<GenericRecord> aiPendingCoder =
                AvroCoder.of(GenericRecord.class, registry.get(SchemaRegistry.AI_PENDING_ROW));

        PCollectionTuple routed = KeyedPCollectionTuple
                .of(FilterAndPairFn.SOURCE_TAG, keyedSource)
                .and(FilterAndPairFn.CASE_PENDING_TAG, keyedCasePending)
                .and(FilterAndPairFn.AI_PENDING_TAG, keyedAiPending)
                .apply("CoGroup", CoGroupByKey.create())
                .apply("FilterAndPair",
                        ParDo.of(new FilterAndPairFn(
                                        ValueProvider.StaticValueProvider.of(segJson),
                                        registry.get(SchemaRegistry.PAYLOAD_ROW),
                                        registry.get(SchemaRegistry.PENDING_ROW),
                                        registry.get(SchemaRegistry.AI_PENDING_ROW)))
                             .withOutputTags(FilterAndPairFn.MATCHED,
                                     TupleTagList.of(FilterAndPairFn.CASE_PENDING)
                                                 .and(FilterAndPairFn.CASE_AGED_OUT)
                                                 .and(FilterAndPairFn.AI_PENDING)
                                                 .and(FilterAndPairFn.AI_AGED_OUT)));
        routed.get(FilterAndPairFn.MATCHED)
                .setCoder(KvCoder.of(StringUtf8Coder.of(), KvCoder.of(payloadCoder, payloadCoder)));
        routed.get(FilterAndPairFn.CASE_PENDING).setCoder(pendingCoder);
        routed.get(FilterAndPairFn.CASE_AGED_OUT).setCoder(pendingCoder);
        routed.get(FilterAndPairFn.AI_PENDING).setCoder(aiPendingCoder);
        routed.get(FilterAndPairFn.AI_AGED_OUT).setCoder(aiPendingCoder);
        return routed;
    }

    private PCollection<KV<String, TableRow>> keyRows(List<TableRow> rows, String imageId, String stepName) {
        if (rows.isEmpty()) {
            return pipeline.apply("Create" + stepName,
                    Create.empty(KvCoder.of(StringUtf8Coder.of(), TableRowJsonCoder.of())));
        }
        return pipeline
                .apply("Create" + stepName, Create.of(rows).withCoder(TableRowJsonCoder.of()))
                .apply(stepName, WithKeys.<String, TableRow>of(r -> imageId)
                        .withKeyType(TypeDescriptors.strings()));
    }

    private PCollection<KV<String, KV<GenericRecord, GenericRecord>>> matched(PCollectionTuple t) {
        return t.get(FilterAndPairFn.MATCHED);
    }

    private PCollection<GenericRecord> casePending(PCollectionTuple t) {
        return t.get(FilterAndPairFn.CASE_PENDING);
    }

    private PCollection<GenericRecord> caseAgedOut(PCollectionTuple t) {
        return t.get(FilterAndPairFn.CASE_AGED_OUT);
    }

    private PCollection<GenericRecord> aiPending(PCollectionTuple t) {
        return t.get(FilterAndPairFn.AI_PENDING);
    }

    private PCollection<GenericRecord> aiAgedOut(PCollectionTuple t) {
        return t.get(FilterAndPairFn.AI_AGED_OUT);
    }

    // ── Tests ─────────────────────────────────────────────────────────────────

    /**
     * Core scenario: 1 human + 2 AI iterations arrive in the same window, no case_id
     * (single-case segment).
     *
     * Expected:
     *  - MATCHED:      2 pairs — img001::main::::1 (AI@08:00) and img001::main::::2 (AI@09:00)
     *  - CASE_PENDING:  1 record — human re-pended for future AI iterations
     *  - AI_PENDING:    2 records — both AI rows are retained even though matched,
     *                    so a case discovered later could still replay against them
     *  - *_AGED_OUT:    empty
     */
    @Test
    public void oneHumanTwoAiIterationsEmitsTwoPairsAndRependHuman() {
        TableRow human = sourceRow("img001", HUMAN_METHOD, "key1", "2026-04-01T10:00:00Z");
        TableRow ai1   = sourceRowWithPayload("img001", AI_METHOD, "key1", "2026-04-01T08:00:00Z",
                null, aiPayloadFor("img001", "a")); // earlier → iter 1
        TableRow ai2   = sourceRowWithPayload("img001", AI_METHOD, "key1", "2026-04-01T09:00:00Z",
                null, aiPayloadFor("img001", "b")); // later   → iter 2

        PCollectionTuple routed = runPipeline("img001",
                List.of(human, ai1, ai2), List.of(), List.of());

        // ── MATCHED: exactly 2 pairs ──────────────────────────────────────────
        PAssert.that(matched(routed)).satisfies(pairs -> {
            List<KV<String, KV<GenericRecord, GenericRecord>>> list = new ArrayList<>();
            pairs.forEach(list::add);

            assertEquals("Expected 2 matched pairs (one per AI iteration)", 2, list.size());

            Set<String> keys = list.stream().map(KV::getKey).collect(Collectors.toSet());
            assertTrue("Missing pair key img001::main::1", keys.contains("img001::main::1"));
            assertTrue("Missing pair key img001::main::2", keys.contains("img001::main::2"));

            // Both pairs reference the same human payload
            for (KV<String, KV<GenericRecord, GenericRecord>> pair : list) {
                GenericRecord humanRec = pair.getValue().getKey();
                assertEquals("human", humanRec.get("payload_type").toString());
            }

            // AI iterations ordered by created_at. Iteration 1 (the group's first-ever AI
            // payload) keeps its own original timestamp; iteration 2 is a genuinely new AI
            // payload discovered in the same run, so it's bumped to exactly +1s past
            // iteration 1's created_at (see FilterAndPairFn.AI_TIMESTAMP_BUMP_SECONDS),
            // regardless of its own original (09:00) timestamp.
            Map<String, String> pairKeyToAiCreatedAt = new HashMap<>();
            for (KV<String, KV<GenericRecord, GenericRecord>> pair : list) {
                pairKeyToAiCreatedAt.put(pair.getKey(),
                        pair.getValue().getValue().get("created_at").toString());
            }
            assertEquals("Iteration 1 should keep its own original timestamp (08:00)",
                    "2026-04-01T08:00:00.000000Z", pairKeyToAiCreatedAt.get("img001::main::1"));
            assertEquals("Iteration 2 should be bumped +1s past iteration 1, not its own 09:00",
                    "2026-04-01T08:00:01.000000Z", pairKeyToAiCreatedAt.get("img001::main::2"));

            return null;
        });

        // ── CASE_PENDING: human kept alive for future AI iterations ────────────
        PAssert.that(casePending(routed)).satisfies(records -> {
            List<GenericRecord> list = new ArrayList<>();
            records.forEach(list::add);

            assertEquals("Human should be re-pended for future AI iterations", 1, list.size());
            assertEquals("human", list.get(0).get("pending_type").toString());
            assertEquals("img001", list.get(0).get("image_id").toString());

            return null;
        });

        // ── AI_PENDING: both AI rows retained even though matched ──────────────
        PAssert.that(aiPending(routed)).satisfies(records -> {
            List<GenericRecord> list = new ArrayList<>();
            records.forEach(list::add);
            assertEquals("Both AI rows should still be retained for future cases", 2, list.size());
            return null;
        });

        PAssert.that(caseAgedOut(routed)).empty();
        PAssert.that(aiAgedOut(routed)).empty();

        pipeline.run().waitUntilFinish();
    }

    /**
     * Human arrives first, AI is pending from a prior run.
     * Expected: 1 MATCHED pair, human re-pended, AI row still retained.
     */
    @Test
    public void pendingAiMatchesArrivingHuman() {
        TableRow human     = sourceRow("img002", HUMAN_METHOD, "key1", "2026-04-02T10:00:00Z");
        TableRow pendingAi = aiPendingRow("img002", "key1",
                "2026-04-01T08:00:00Z", Instant.now().minusSeconds(3600).toString());

        PCollectionTuple routed = runPipeline("img002",
                List.of(human), List.of(), List.of(pendingAi));

        PAssert.that(matched(routed)).satisfies(pairs -> {
            List<KV<String, KV<GenericRecord, GenericRecord>>> list = new ArrayList<>();
            pairs.forEach(list::add);
            assertEquals("Expected 1 matched pair", 1, list.size());
            assertEquals("img002::main::1", list.get(0).getKey());
            return null;
        });

        PAssert.that(casePending(routed)).satisfies(records -> {
            List<GenericRecord> list = new ArrayList<>();
            records.forEach(list::add);
            assertEquals("Human should be re-pended", 1, list.size());
            assertEquals("human", list.get(0).get("pending_type").toString());
            return null;
        });

        PAssert.that(aiPending(routed)).satisfies(records -> {
            List<GenericRecord> list = new ArrayList<>();
            records.forEach(list::add);
            assertEquals("Matched AI row should still be retained", 1, list.size());
            return null;
        });

        PAssert.that(caseAgedOut(routed)).empty();
        PAssert.that(aiAgedOut(routed)).empty();
        pipeline.run().waitUntilFinish();
    }

    /**
     * AI arrives alone — no human in source or pending.
     * Expected: 0 MATCHED, 1 AI_PENDING (the AI iteration), 0 CASE_PENDING.
     */
    @Test
    public void aiOnlyEmitsNewPending() {
        TableRow ai = sourceRow("img003", AI_METHOD, "key1", "2026-04-01T08:00:00Z");

        PCollectionTuple routed = runPipeline("img003", List.of(ai), List.of(), List.of());

        PAssert.that(matched(routed)).empty();
        PAssert.that(casePending(routed)).empty();

        PAssert.that(aiPending(routed)).satisfies(records -> {
            List<GenericRecord> list = new ArrayList<>();
            records.forEach(list::add);
            assertEquals("Expected 1 pending AI row", 1, list.size());
            return null;
        });

        PAssert.that(caseAgedOut(routed)).empty();
        PAssert.that(aiAgedOut(routed)).empty();
        pipeline.run().waitUntilFinish();
    }

    /**
     * AI has been pending for more than MAX_WAIT_DAYS with no case ever matching it.
     * Expected: 0 MATCHED, 0 AI_PENDING, 1 AI_AGED_OUT — aged independently of any case.
     */
    @Test
    public void agedOutAiRoutesToDeadLetter() {
        String oldTimestamp = Instant.now()
                .minus(FilterAndPairFn.MAX_WAIT_DAYS + 1, ChronoUnit.DAYS)
                .toString();
        TableRow staleAi = aiPendingRow("img004", "key1", "2026-03-01T08:00:00Z", oldTimestamp);

        PCollectionTuple routed = runPipeline("img004", List.of(), List.of(), List.of(staleAi));

        PAssert.that(matched(routed)).empty();
        PAssert.that(aiPending(routed)).empty();
        PAssert.that(casePending(routed)).empty();
        PAssert.that(caseAgedOut(routed)).empty();

        PAssert.that(aiAgedOut(routed)).satisfies(records -> {
            List<GenericRecord> list = new ArrayList<>();
            records.forEach(list::add);
            assertEquals("Expected 1 aged-out AI row", 1, list.size());
            return null;
        });

        pipeline.run().waitUntilFinish();
    }

    /**
     * BQ's readTableRows() returns INTEGER columns as String, not Long.
     * On the second execution the AI pending row read back from BQ will have
     * retry_count = "1" (String).  This must not throw a ClassCastException.
     *
     * Expected: 1 MATCHED pair, human re-pended with retry_count 0 (unrelated to the
     * AI row's own retry count).
     */
    @Test
    public void retryCountAsStringDoesNotThrow() {
        TableRow human = sourceRow("img005", HUMAN_METHOD, "key1", "2026-04-03T10:00:00Z");
        // Simulate BQ returning retry_count as a String (second run round-trip)
        TableRow pendingAi = aiPendingRow("img005", "key1",
                "2026-04-02T08:00:00Z", Instant.now().minusSeconds(3600).toString(),
                "1" /* String, not Long */);

        PCollectionTuple routed = runPipeline("img005",
                List.of(human), List.of(), List.of(pendingAi));

        PAssert.that(matched(routed)).satisfies(pairs -> {
            List<KV<String, KV<GenericRecord, GenericRecord>>> list = new ArrayList<>();
            pairs.forEach(list::add);
            assertEquals("Expected 1 matched pair", 1, list.size());
            assertEquals("img005::main::1", list.get(0).getKey());
            return null;
        });

        PAssert.that(casePending(routed)).satisfies(records -> {
            List<GenericRecord> list = new ArrayList<>();
            records.forEach(list::add);
            assertEquals(1, list.size());
            assertEquals("human", list.get(0).get("pending_type").toString());
            assertEquals(0L, list.get(0).get("retry_count"));
            return null;
        });

        PAssert.that(aiPending(routed)).satisfies(records -> {
            List<GenericRecord> list = new ArrayList<>();
            records.forEach(list::add);
            assertEquals("Matched AI row should still be retained, retry_count incremented",
                    1, list.size());
            assertEquals(2L, list.get(0).get("retry_count"));
            return null;
        });

        PAssert.that(caseAgedOut(routed)).empty();
        PAssert.that(aiAgedOut(routed)).empty();
        pipeline.run().waitUntilFinish();
    }

    /**
     * Two case_ids share one image+segment. Case-1's contribution already matched AI1 in
     * a prior run (persisted matched_ai_keys already contains AI1's identity key); case-2
     * shows up fresh this run, contributing a different case_id for the same image+segment.
     *
     * Case_id is lineage, not a matching partition (see FilterAndPairFn class doc) — case-1's
     * persisted contribution and case-2's fresh contribution merge into ONE consolidated
     * group. Since the group's matched_ai_keys already covers AI1 and no new AI arrives,
     * there must be no new match — the old per-case "case-2 replays independently" behavior
     * no longer applies. Expected: 0 matched pairs, 1 re-pended group row (not 2 separate
     * case rows) with case_id = case1 (the earlier-arriving, canonical contributor) and
     * merged_case_ids covering both.
     */
    @Test
    public void newCaseReplaysExistingAiHistory() {
        String aiCreatedAt = "2026-04-01T08:00:00.000000Z";
        String ai1Key = aiDedupKey(payloadFor("img006"));

        TableRow ai1Pending = aiPendingRow("img006", "key1", aiCreatedAt,
                Instant.now().minusSeconds(3600).toString());

        TableRow case1Pending = casePendingRow("img006", "human", "key1",
                "2026-03-30T10:00:00Z", Instant.now().minusSeconds(7200).toString(),
                0L, "case1", ai1Key);

        TableRow case2Fresh = sourceRow("img006", HUMAN_METHOD, "key1",
                "2026-04-01T11:00:00Z", "case2");

        PCollectionTuple routed = runPipeline("img006",
                List.of(case2Fresh), List.of(case1Pending), List.of(ai1Pending));

        PAssert.that(matched(routed)).satisfies(pairs -> {
            List<KV<String, KV<GenericRecord, GenericRecord>>> list = new ArrayList<>();
            pairs.forEach(list::add);
            assertEquals("Group already matched AI1 via case-1's carried-forward state — "
                    + "no new match for case-2's arrival", 0, list.size());
            return null;
        });

        PAssert.that(casePending(routed)).satisfies(records -> {
            List<GenericRecord> list = new ArrayList<>();
            records.forEach(list::add);
            assertEquals("One consolidated group row, not two separate case rows", 1, list.size());
            GenericRecord row = list.get(0);
            assertEquals("Canonical case_id is the earlier-arriving contributor",
                    "case1", row.get("case_id").toString());
            Set<String> mergedCaseIds = Set.of(row.get("merged_case_ids").toString().split(";"));
            assertEquals(Set.of("case1", "case2"), mergedCaseIds);
            return null;
        });

        PAssert.that(aiPending(routed)).satisfies(records -> {
            List<GenericRecord> list = new ArrayList<>();
            records.forEach(list::add);
            assertEquals("AI1 should still be retained for any future contribution", 1, list.size());
            return null;
        });

        PAssert.that(caseAgedOut(routed)).empty();
        PAssert.that(aiAgedOut(routed)).empty();
        pipeline.run().waitUntilFinish();
    }

    /**
     * Regression test for the AI lookback: an AI row a case already matched (its
     * dedup key is already in matched_ai_keys) reappears as a fresh SOURCE row on a
     * later run — e.g. re-selected by --aiLookbackHours even though it's no longer
     * present in the AI pending pool for this scenario. It must NOT be matched again.
     *
     * This is exactly the scenario that would have silently duplicated comparison_results
     * rows under the old matched_ai_count (a count, not an identity set) design, once an
     * AI source read could re-select an already-matched row across separate runs.
     */
    @Test
    public void reappearingMatchedAiProducesNoDuplicate() {
        String aiCreatedAt = "2026-04-01T08:00:00.000000Z";
        String matchedKey  = aiDedupKey(payloadFor("img007"));

        TableRow casePending = casePendingRow("img007", "human", "key1",
                "2026-03-30T10:00:00Z", Instant.now().minusSeconds(7200).toString(),
                0L, null, matchedKey);

        // Same AI payload reappears as a fresh source row (simulating a lookback re-read).
        TableRow aiAgain = sourceRow("img007", AI_METHOD, "key1", aiCreatedAt);

        PCollectionTuple routed = runPipeline("img007",
                List.of(aiAgain), List.of(casePending), List.of());

        PAssert.that(matched(routed)).empty();

        PAssert.that(casePending(routed)).satisfies(records -> {
            List<GenericRecord> list = new ArrayList<>();
            records.forEach(list::add);
            assertEquals(1, list.size());
            assertEquals("matched_ai_keys should be unchanged — no new match",
                    matchedKey, list.get(0).get("matched_ai_keys").toString());
            return null;
        });

        PAssert.that(aiPending(routed)).satisfies(records -> {
            List<GenericRecord> list = new ArrayList<>();
            records.forEach(list::add);
            assertEquals("Reappeared AI row is retained, not re-matched", 1, list.size());
            return null;
        });

        PAssert.that(aiPending(routed)).satisfies(records -> {
            List<GenericRecord> list = new ArrayList<>();
            records.forEach(list::add);
            assertEquals("Reappeared AI row should still be retained", 1, list.size());
            return null;
        });

        PAssert.that(caseAgedOut(routed)).empty();
        PAssert.that(aiAgedOut(routed)).empty();
        pipeline.run().waitUntilFinish();
    }

    /**
     * Same case_id receives a newer human payload after already matching AI1.
     * The newer human created_at must not reset matched_ai_keys or replay AI1.
     */
    @Test
    public void sameCaseHumanUpdateDoesNotReplayMatchedAi() {
        String aiCreatedAt = "2026-04-01T08:00:00.000000Z";
        String matchedKey  = aiDedupKey(payloadFor("img008"));

        TableRow oldCaseState = casePendingRow("img008", "human", "key1",
                "2026-03-30T10:00:00Z", Instant.now().minusSeconds(7200).toString(),
                3L, "case1", matchedKey);

        TableRow updatedHuman = sourceRow("img008", HUMAN_METHOD, "key1",
                "2026-04-02T10:00:00Z", "case1");
        TableRow aiAgain = sourceRow("img008", AI_METHOD, "key1", aiCreatedAt);

        PCollectionTuple routed = runPipeline("img008",
                List.of(updatedHuman, aiAgain), List.of(oldCaseState), List.of());

        PAssert.that(matched(routed)).empty();

        PAssert.that(casePending(routed)).satisfies(records -> {
            List<GenericRecord> list = new ArrayList<>();
            records.forEach(list::add);
            assertEquals(1, list.size());
            GenericRecord row = list.get(0);
            assertEquals("case1", row.get("case_id").toString());
            assertEquals("2026-04-02T10:00:00.000000Z", row.get("created_at").toString());
            assertEquals(matchedKey, row.get("matched_ai_keys").toString());
            return null;
        });

        PAssert.that(caseAgedOut(routed)).empty();
        PAssert.that(aiAgedOut(routed)).empty();
        pipeline.run().waitUntilFinish();
    }

    /**
     * Same case_id receives a newer human payload and a genuinely new AI payload.
     * Only the new AI should match, and its ai_iteration should continue after
     * the previously matched AI key.
     */
    @Test
    public void sameCaseHumanUpdateMatchesOnlyNewAiAtNextIteration() {
        String ai1CreatedAt = "2026-04-01T08:00:00.000000Z";
        String ai2CreatedAt = "2026-04-01T09:00:00.000000Z";
        String ai1Key       = aiDedupKey(payloadFor("img009"));
        String ai2Key       = aiDedupKey(aiPayloadFor("img009", "b"));

        TableRow oldCaseState = casePendingRow("img009", "human", "key1",
                "2026-03-30T10:00:00Z", Instant.now().minusSeconds(7200).toString(),
                3L, "case1", ai1Key);

        TableRow updatedHuman = sourceRow("img009", HUMAN_METHOD, "key1",
                "2026-04-02T10:00:00Z", "case1");
        TableRow ai1Again = sourceRow("img009", AI_METHOD, "key1", ai1CreatedAt);
        TableRow ai2New   = sourceRowWithPayload("img009", AI_METHOD, "key1", ai2CreatedAt,
                null, aiPayloadFor("img009", "b"));

        PCollectionTuple routed = runPipeline("img009",
                List.of(updatedHuman, ai1Again, ai2New), List.of(oldCaseState), List.of());

        PAssert.that(matched(routed)).satisfies(pairs -> {
            List<KV<String, KV<GenericRecord, GenericRecord>>> list = new ArrayList<>();
            pairs.forEach(list::add);
            assertEquals(1, list.size());
            assertEquals("img009::main::2", list.get(0).getKey());
            // ai1Again is not in ai_pending_comparisons in this scenario (simulating a
            // lookback re-read of an already-matched row, same as
            // reappearingMatchedAiProducesNoDuplicate), so it's treated as first-sighting
            // for bump-assignment purposes even though matched_ai_keys already excludes it
            // from matching — it gets the group's first-ever timestamp (its own original),
            // and ai2New (the genuinely new payload) is bumped +1s past it rather than
            // keeping its own original 09:00.
            assertEquals("2026-04-01T08:00:01.000000Z",
                    list.get(0).getValue().getValue().get("created_at").toString());
            assertEquals("2026-04-02T10:00:00.000000Z",
                    list.get(0).getValue().getKey().get("created_at").toString());
            return null;
        });

        PAssert.that(casePending(routed)).satisfies(records -> {
            List<GenericRecord> list = new ArrayList<>();
            records.forEach(list::add);
            assertEquals(1, list.size());
            String matched = list.get(0).get("matched_ai_keys").toString();
            assertTrue(matched.contains(ai1Key));
            assertTrue(matched.contains(ai2Key));
            return null;
        });

        PAssert.that(aiPending(routed)).satisfies(records -> {
            List<GenericRecord> list = new ArrayList<>();
            records.forEach(list::add);
            assertEquals("Both AI rows should be retained for future cases", 2, list.size());
            return null;
        });

        PAssert.that(caseAgedOut(routed)).empty();
        PAssert.that(aiAgedOut(routed)).empty();
        pipeline.run().waitUntilFinish();
    }

    /**
     * Regression test: the next ai_iteration must come from the explicit next_ai_iteration
     * counter, never from matched_ai_keys.size(). Simulates a case whose matched_ai_keys was
     * inflated to 2 entries by a defensive merge (mergeCaseMeta) without a second real match
     * ever happening — next_ai_iteration correctly still says only 1 real match occurred.
     *
     * If iteration were derived from matched_ai_keys.size() instead, a genuinely new AI row
     * would incorrectly be assigned iteration 3, skipping iteration 2 — this is exactly the
     * "ai_iteration=2 with no ai_iteration=1" class of symptom the counter field prevents.
     */
    @Test
    public void nextIterationComesFromCounterNotKeySetSize() {
        String ai3CreatedAt = "2026-04-01T10:00:00.000000Z";
        // Dummy identity keys for two hypothetical already-matched AI rows — not required to
        // correspond to any real row appearing this run, only to populate matched_ai_keys.
        String ai1Key = aiDedupKey(aiPayloadFor("img010", "a"));
        String ai2Key = aiDedupKey(aiPayloadFor("img010", "b"));

        // matched_ai_keys has 2 entries, but next_ai_iteration says only 1 real match happened —
        // simulating an inflated key set from a merge, not a normal case's state.
        TableRow inflatedCaseState = casePendingRow("img010", "human", "key1",
                "2026-03-30T10:00:00Z", Instant.now().minusSeconds(7200).toString(),
                0L, "case1", ai1Key + ";" + ai2Key, /* nextIteration */ 1L);

        TableRow updatedHuman = sourceRow("img010", HUMAN_METHOD, "key1",
                "2026-04-02T10:00:00Z", "case1");
        TableRow ai3New = sourceRowWithPayload("img010", AI_METHOD, "key1", ai3CreatedAt,
                null, aiPayloadFor("img010", "c"));

        PCollectionTuple routed = runPipeline("img010",
                List.of(updatedHuman, ai3New), List.of(inflatedCaseState), List.of());

        PAssert.that(matched(routed)).satisfies(pairs -> {
            List<KV<String, KV<GenericRecord, GenericRecord>>> list = new ArrayList<>();
            pairs.forEach(list::add);
            assertEquals("Only the genuinely new AI row should match", 1, list.size());
            assertEquals("Iteration should continue from the counter (1), not the key count (2)",
                    "img010::main::2", list.get(0).getKey());
            return null;
        });

        PAssert.that(casePending(routed)).satisfies(records -> {
            List<GenericRecord> list = new ArrayList<>();
            records.forEach(list::add);
            assertEquals(1, list.size());
            assertEquals(2L, list.get(0).get("next_ai_iteration"));
            return null;
        });

        PAssert.that(caseAgedOut(routed)).empty();
        PAssert.that(aiAgedOut(routed)).empty();
        pipeline.run().waitUntilFinish();
    }
}
