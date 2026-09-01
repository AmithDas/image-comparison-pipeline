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
import java.util.List;
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
     * Distinct payload content for a given marker. Row identity (AI dedup key, and now the
     * human side of the comparison signature) is the payload string alone — real Barricade
     * ciphertext differs per submission via a random IV, so any test with more than one
     * distinct human or AI event must use distinguishable payload content, not just a
     * different created_at.
     */
    private static String payloadWithMarker(String imageId, String marker) {
        return "{\"image_name\":\"" + imageId + "\",\"marker\":\"" + marker + "\"}";
    }

    /** The identity key FilterAndPairFn derives internally: the payload string alone. */
    private static String dedupKey(String payload) {
        return payload;
    }

    /** The comparison signature FilterAndPairFn derives: dedupKey(human) + "|" + dedupKey(ai). */
    private static String signature(String humanPayload, String aiPayload) {
        return dedupKey(humanPayload) + "|" + dedupKey(aiPayload);
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
        return casePendingRow(imageId, pendingType, keyId, createdAt, firstSeenAt,
                0L, null, null, 0L);
    }

    private static TableRow casePendingRow(String imageId, String pendingType,
                                           String keyId, String createdAt, String firstSeenAt,
                                           Object retryCount, String caseId,
                                           String lastComparedSignature, long comparisonVersion) {
        return casePendingRowWithPayload(imageId, pendingType, keyId, createdAt, firstSeenAt,
                retryCount, caseId, lastComparedSignature, comparisonVersion, payloadFor(imageId));
    }

    private static TableRow casePendingRowWithPayload(String imageId, String pendingType,
                                           String keyId, String createdAt, String firstSeenAt,
                                           Object retryCount, String caseId,
                                           String lastComparedSignature, long comparisonVersion,
                                           String payload) {
        TableRow row = new TableRow()
                .set("image_id",          imageId)
                .set("segment",           "main")
                .set("pending_type",      pendingType)
                .set("payload",           payload)
                .set("key_id",            keyId)
                .set("created_at",        createdAt)
                .set("first_seen_at",     firstSeenAt)
                .set("last_retried_at",   firstSeenAt)
                .set("retry_count",       retryCount)
                .set("comparison_version", comparisonVersion);
        if (caseId != null) row.set("case_id", caseId);
        if (lastComparedSignature != null) row.set("last_compared_signature", lastComparedSignature);
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
     * 1 human + 2 distinct AI payloads arrive in the same window, no case_id (single-case
     * segment). The group is always compared against only its single latest AI payload —
     * never a backlog of unmatched history.
     *
     * Expected:
     *  - MATCHED:      1 pair — img001::main, against whichever AI payload is latest (bumped
     *                    to created_at 08:00:01, +1s past the first-ever AI payload's 08:00)
     *  - CASE_PENDING:  1 record — comparison_version=1, last_compared_signature persisted
     *  - AI_PENDING:    2 records — both AI rows are retained even though only one was
     *                    compared, so aging is tracked for both independently
     *  - *_AGED_OUT:    empty
     */
    @Test
    public void oneHumanTwoAiPayloadsSameRunComparesOnlyAgainstLatest() {
        String humanPayload = payloadFor("img001");
        String ai1Payload   = payloadWithMarker("img001", "a");
        String ai2Payload   = payloadWithMarker("img001", "b");

        TableRow human = sourceRow("img001", HUMAN_METHOD, "key1", "2026-04-01T10:00:00Z");
        TableRow ai1   = sourceRowWithPayload("img001", AI_METHOD, "key1", "2026-04-01T08:00:00Z",
                null, ai1Payload); // earlier arrival
        TableRow ai2   = sourceRowWithPayload("img001", AI_METHOD, "key1", "2026-04-01T09:00:00Z",
                null, ai2Payload); // later arrival → this is "latest"

        PCollectionTuple routed = runPipeline("img001",
                List.of(human, ai1, ai2), List.of(), List.of());

        PAssert.that(matched(routed)).satisfies(pairs -> {
            List<KV<String, KV<GenericRecord, GenericRecord>>> list = new ArrayList<>();
            pairs.forEach(list::add);

            assertEquals("Exactly one comparison, against the latest AI payload only",
                    1, list.size());
            assertEquals("img001::main", list.get(0).getKey());

            GenericRecord humanRec = list.get(0).getValue().getKey();
            GenericRecord aiRec    = list.get(0).getValue().getValue();
            assertEquals("human", humanRec.get("payload_type").toString());
            assertEquals("Compared against ai2's content (the latest arrival)",
                    ai2Payload, aiRec.get("payload").toString());
            // ai2 is the group's second-ever AI payload discovered this run, so it's bumped
            // +1s past ai1's own original timestamp (see FilterAndPairFn.AI_TIMESTAMP_BUMP_SECONDS).
            assertEquals("2026-04-01T08:00:01.000000Z", aiRec.get("created_at").toString());
            assertEquals(1L, humanRec.get("comparison_version"));

            return null;
        });

        PAssert.that(casePending(routed)).satisfies(records -> {
            List<GenericRecord> list = new ArrayList<>();
            records.forEach(list::add);

            assertEquals("Human should be re-pended for future comparisons", 1, list.size());
            assertEquals("human", list.get(0).get("pending_type").toString());
            assertEquals("img001", list.get(0).get("image_id").toString());
            assertEquals(1L, list.get(0).get("comparison_version"));
            assertEquals(signature(humanPayload, ai2Payload),
                    list.get(0).get("last_compared_signature").toString());

            return null;
        });

        PAssert.that(aiPending(routed)).satisfies(records -> {
            List<GenericRecord> list = new ArrayList<>();
            records.forEach(list::add);
            assertEquals("Both AI rows should still be retained for independent aging",
                    2, list.size());
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
            assertEquals("img002::main", list.get(0).getKey());
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
     * Expected: 0 MATCHED, 1 AI_PENDING (the AI row), 0 CASE_PENDING.
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
     * AI has been pending for more than MAX_WAIT_DAYS with no human ever matching it.
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
            assertEquals("img005::main", list.get(0).getKey());
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
     * Two case_ids share one image+segment, arriving in different runs. Case-1's contribution
     * already compared against AI1 in a prior run (persisted last_compared_signature reflects
     * that). Case-2 shows up fresh this run, contributing distinct content under a different
     * case_id, with no new AI arriving.
     *
     * Case_id is lineage, not a matching partition (see FilterAndPairFn class doc) — case-1's
     * persisted contribution and case-2's fresh contribution fold into ONE consolidated group.
     * Because the group's human-side content changed (case-2's distinct data), the comparison
     * signature changes even though AI1 itself didn't — this is the core "always re-compare on
     * a human update" behavior. Expected: 1 NEW matched pair (comparison_version advances to 2)
     * against the still-unchanged AI1, and one consolidated CASE_PENDING row (not two separate
     * case rows) with case_id = case1 (the earlier-arriving, canonical contributor) and
     * merged_case_ids covering both.
     */
    @Test
    public void newCaseReplaysExistingAiHistory() {
        String aiCreatedAt = "2026-04-01T08:00:00.000000Z";
        String ai1Payload   = payloadFor("img006");
        String case1Payload = payloadFor("img006");
        String case2Payload = payloadWithMarker("img006", "case2-update");

        TableRow ai1Pending = aiPendingRow("img006", "key1", aiCreatedAt,
                Instant.now().minusSeconds(3600).toString());

        TableRow case1Pending = casePendingRow("img006", "human", "key1",
                "2026-03-30T10:00:00Z", Instant.now().minusSeconds(7200).toString(),
                0L, "case1", signature(case1Payload, ai1Payload), 1L);

        TableRow case2Fresh = sourceRowWithPayload("img006", HUMAN_METHOD, "key1",
                "2026-04-01T11:00:00Z", "case2", case2Payload);

        PCollectionTuple routed = runPipeline("img006",
                List.of(case2Fresh), List.of(case1Pending), List.of(ai1Pending));

        PAssert.that(matched(routed)).satisfies(pairs -> {
            List<KV<String, KV<GenericRecord, GenericRecord>>> list = new ArrayList<>();
            pairs.forEach(list::add);
            assertEquals("Human content changed (case-2's distinct data) — a new comparison "
                    + "fires even though AI1 itself is unchanged", 1, list.size());
            assertEquals("img006::main", list.get(0).getKey());
            assertEquals(2L, list.get(0).getValue().getKey().get("comparison_version"));
            return null;
        });

        PAssert.that(casePending(routed)).satisfies(records -> {
            List<GenericRecord> list = new ArrayList<>();
            records.forEach(list::add);
            assertEquals("One consolidated group row, not two separate case rows", 1, list.size());
            GenericRecord row = list.get(0);
            assertEquals("Canonical case_id is the earlier-arriving contributor",
                    "case1", row.get("case_id").toString());
            assertEquals(2L, row.get("comparison_version"));
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
     * Regression test: an AI row already reflected in the group's last_compared_signature
     * reappears as a fresh SOURCE row on a later run — e.g. re-selected by
     * --aiLookbackHours even though it's no longer present in the AI pending pool for this
     * scenario — with no human-side change either. Since neither side's content actually
     * changed, the signature is unchanged and no new comparison (and no duplicate
     * comparison_results rows) should be produced.
     */
    @Test
    public void reappearingMatchedAiProducesNoDuplicate() {
        String aiCreatedAt   = "2026-04-01T08:00:00.000000Z";
        String humanPayload  = payloadFor("img007");
        String aiPayload     = payloadFor("img007");

        TableRow casePending = casePendingRow("img007", "human", "key1",
                "2026-03-30T10:00:00Z", Instant.now().minusSeconds(7200).toString(),
                0L, null, signature(humanPayload, aiPayload), 1L);

        // Same AI payload reappears as a fresh source row (simulating a lookback re-read) —
        // no human-side change this run either.
        TableRow aiAgain = sourceRow("img007", AI_METHOD, "key1", aiCreatedAt);

        PCollectionTuple routed = runPipeline("img007",
                List.of(aiAgain), List.of(casePending), List.of());

        PAssert.that(matched(routed)).empty();

        PAssert.that(casePending(routed)).satisfies(records -> {
            List<GenericRecord> list = new ArrayList<>();
            records.forEach(list::add);
            assertEquals(1, list.size());
            assertEquals("last_compared_signature should be unchanged — no new comparison",
                    signature(humanPayload, aiPayload),
                    list.get(0).get("last_compared_signature").toString());
            assertEquals("comparison_version should not advance", 1L,
                    list.get(0).get("comparison_version"));
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
     * Same case_id resubmits with a newer created_at but byte-identical payload content (e.g.
     * a retry or a no-op resubmission), and the same AI payload also reappears unchanged.
     * Since the comparison signature is content-based (not timestamp-based), this must NOT
     * trigger a needless re-comparison — only a genuine content change should.
     */
    @Test
    public void noOpResubmissionWithUnchangedContentDoesNotReplayComparison() {
        String aiCreatedAt  = "2026-04-01T08:00:00.000000Z";
        String humanPayload = payloadFor("img008");
        String aiPayload    = payloadFor("img008");

        TableRow oldCaseState = casePendingRow("img008", "human", "key1",
                "2026-03-30T10:00:00Z", Instant.now().minusSeconds(7200).toString(),
                3L, "case1", signature(humanPayload, aiPayload), 1L);

        // Newer created_at, but identical payload content.
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
            assertEquals("Signature unchanged — content is identical",
                    signature(humanPayload, aiPayload),
                    row.get("last_compared_signature").toString());
            assertEquals("comparison_version must not advance on a no-op resubmission",
                    1L, row.get("comparison_version"));
            return null;
        });

        PAssert.that(caseAgedOut(routed)).empty();
        PAssert.that(aiAgedOut(routed)).empty();
        pipeline.run().waitUntilFinish();
    }

    /**
     * A genuinely new AI payload arrives for an already-compared group — even though the
     * human side hasn't changed, a new comparison must fire against the new AI payload.
     */
    @Test
    public void newAiPayloadTriggersFreshComparisonAgainstLatest() {
        String ai1Payload  = payloadFor("img009");
        String ai2Payload  = payloadWithMarker("img009", "b");
        String humanPayload = payloadFor("img009");

        TableRow oldCaseState = casePendingRow("img009", "human", "key1",
                "2026-03-30T10:00:00Z", Instant.now().minusSeconds(7200).toString(),
                3L, "case1", signature(humanPayload, ai1Payload), 1L);

        // Human content unchanged; ai1 reappears (lookback re-read), ai2 is genuinely new.
        TableRow humanAgain = sourceRow("img009", HUMAN_METHOD, "key1",
                "2026-04-02T10:00:00Z", "case1");
        TableRow ai1Again = sourceRow("img009", AI_METHOD, "key1", "2026-04-01T08:00:00Z");
        TableRow ai2New   = sourceRowWithPayload("img009", AI_METHOD, "key1",
                "2026-04-01T09:00:00Z", null, ai2Payload);

        PCollectionTuple routed = runPipeline("img009",
                List.of(humanAgain, ai1Again, ai2New), List.of(oldCaseState), List.of());

        PAssert.that(matched(routed)).satisfies(pairs -> {
            List<KV<String, KV<GenericRecord, GenericRecord>>> list = new ArrayList<>();
            pairs.forEach(list::add);
            assertEquals("New AI payload triggers exactly one fresh comparison", 1, list.size());
            assertEquals("img009::main", list.get(0).getKey());
            assertEquals("Compared against the new AI payload (ai2), not the reappearing ai1",
                    ai2Payload, list.get(0).getValue().getValue().get("payload").toString());
            assertEquals(2L, list.get(0).getValue().getKey().get("comparison_version"));
            return null;
        });

        PAssert.that(casePending(routed)).satisfies(records -> {
            List<GenericRecord> list = new ArrayList<>();
            records.forEach(list::add);
            assertEquals(1, list.size());
            assertEquals(2L, list.get(0).get("comparison_version"));
            assertEquals(signature(humanPayload, ai2Payload),
                    list.get(0).get("last_compared_signature").toString());
            return null;
        });

        PAssert.that(aiPending(routed)).satisfies(records -> {
            List<GenericRecord> list = new ArrayList<>();
            records.forEach(list::add);
            assertEquals("Both AI rows should be retained for independent aging", 2, list.size());
            return null;
        });

        PAssert.that(caseAgedOut(routed)).empty();
        PAssert.that(aiAgedOut(routed)).empty();
        pipeline.run().waitUntilFinish();
    }
}
