package com.yourorg.pipeline.transforms;

import com.google.api.services.bigquery.model.TableRow;
import com.yourorg.pipeline.util.AvroSchemas;
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

import static org.junit.Assert.*;

public class FilterAndPairFnTest {

    @Rule
    public final TestPipeline pipeline = TestPipeline.create();

    private static final String AI_METHOD    = "aimetadata";
    private static final String HUMAN_METHOD = "controller.SubmitDispute";

    // ── Helpers ───────────────────────────────────────────────────────────────

    /** Creates a source TableRow (payload is plain JSON — FilterAndPairFn never parses it). */
    private static TableRow sourceRow(String imageId, String method,
                                      String keyId, String createdAt) {
        return new TableRow()
                .set("payload",    "{\"image_name\":\"" + imageId + "\"}")
                .set("key_id",     keyId)
                .set("method",     method)
                .set("created_at", createdAt);
    }

    /** Creates a pending TableRow for a single pending record. */
    private static TableRow pendingRow(String imageId, String pendingType,
                                       String keyId, String createdAt, String firstSeenAt) {
        return new TableRow()
                .set("image_id",        imageId)
                .set("pending_type",    pendingType)
                .set("payload",         "{\"image_name\":\"" + imageId + "\"}")
                .set("key_id",          keyId)
                .set("created_at",      createdAt)
                .set("first_seen_at",   firstSeenAt)
                .set("last_retried_at", firstSeenAt)
                .set("retry_count",     0L);
    }

    /** Wires source + pending rows through CoGroupByKey → FilterAndPairFn. */
    private PCollectionTuple runPipeline(String imageId,
                                          List<TableRow> sourceRows,
                                          List<TableRow> pendingRows) {
        PCollection<KV<String, TableRow>> keyedSource = pipeline
                .apply("CreateSource", Create.of(sourceRows).withCoder(TableRowJsonCoder.of()))
                .apply("KeySource", WithKeys.<String, TableRow>of(r -> imageId)
                        .withKeyType(TypeDescriptors.strings()));

        PCollection<KV<String, TableRow>> keyedPending;
        if (pendingRows.isEmpty()) {
            keyedPending = pipeline.apply("CreatePending",
                    Create.empty(KvCoder.of(StringUtf8Coder.of(), TableRowJsonCoder.of())));
        } else {
            keyedPending = pipeline
                    .apply("CreatePending", Create.of(pendingRows).withCoder(TableRowJsonCoder.of()))
                    .apply("KeyPending", WithKeys.<String, TableRow>of(r -> imageId)
                            .withKeyType(TypeDescriptors.strings()));
        }

        return KeyedPCollectionTuple
                .of(FilterAndPairFn.SOURCE_TAG, keyedSource)
                .and(FilterAndPairFn.PENDING_TAG, keyedPending)
                .apply("CoGroup", CoGroupByKey.create())
                .apply("FilterAndPair",
                        ParDo.of(new FilterAndPairFn(AI_METHOD, HUMAN_METHOD))
                             .withOutputTags(FilterAndPairFn.MATCHED,
                                     TupleTagList.of(FilterAndPairFn.NEW_PENDING)
                                                 .and(FilterAndPairFn.AGED_OUT)));
    }

    private PCollection<KV<String, KV<GenericRecord, GenericRecord>>> matched(PCollectionTuple t) {
        AvroCoder<GenericRecord> payloadCoder = AvroCoder.of(GenericRecord.class, AvroSchemas.PAYLOAD_ROW);
        return t.get(FilterAndPairFn.MATCHED)
                .setCoder(KvCoder.of(StringUtf8Coder.of(),
                        KvCoder.of(payloadCoder, payloadCoder)));
    }

    private PCollection<GenericRecord> newPending(PCollectionTuple t) {
        return t.get(FilterAndPairFn.NEW_PENDING)
                .setCoder(AvroCoder.of(GenericRecord.class, AvroSchemas.PENDING_ROW));
    }

    private PCollection<GenericRecord> agedOut(PCollectionTuple t) {
        return t.get(FilterAndPairFn.AGED_OUT)
                .setCoder(AvroCoder.of(GenericRecord.class, AvroSchemas.PENDING_ROW));
    }

    // ── Tests ─────────────────────────────────────────────────────────────────

    /**
     * Core scenario: 1 human + 2 AI iterations arrive in the same window.
     *
     * Expected:
     *  - MATCHED:     2 pairs — img001::1 (human, AI@08:00) and img001::2 (human, AI@09:00)
     *  - NEW_PENDING: 1 record — human re-pended for future AI iterations
     *  - AGED_OUT:    empty
     */
    @Test
    public void oneHumanTwoAiIterationsEmitsTwoPairsAndRependHuman() {
        TableRow human = sourceRow("img001", HUMAN_METHOD, "key1", "2026-04-01T10:00:00Z");
        TableRow ai1   = sourceRow("img001", AI_METHOD,    "key1", "2026-04-01T08:00:00Z"); // earlier → iter 1
        TableRow ai2   = sourceRow("img001", AI_METHOD,    "key1", "2026-04-01T09:00:00Z"); // later   → iter 2

        PCollectionTuple routed = runPipeline("img001",
                List.of(human, ai1, ai2), List.of());

        // ── MATCHED: exactly 2 pairs ──────────────────────────────────────────
        PAssert.that(matched(routed)).satisfies(pairs -> {
            List<KV<String, KV<GenericRecord, GenericRecord>>> list = new ArrayList<>();
            pairs.forEach(list::add);

            assertEquals("Expected 2 matched pairs (one per AI iteration)", 2, list.size());

            Set<String> keys = list.stream().map(KV::getKey).collect(Collectors.toSet());
            assertTrue("Missing pair key img001::1", keys.contains("img001::1"));
            assertTrue("Missing pair key img001::2", keys.contains("img001::2"));

            // Both pairs reference the same human payload
            for (KV<String, KV<GenericRecord, GenericRecord>> pair : list) {
                GenericRecord humanRec = pair.getValue().getKey();
                assertEquals("human", humanRec.get("payload_type").toString());
            }

            // AI iterations ordered by created_at
            Map<String, String> pairKeyToAiCreatedAt = new HashMap<>();
            for (KV<String, KV<GenericRecord, GenericRecord>> pair : list) {
                pairKeyToAiCreatedAt.put(pair.getKey(),
                        pair.getValue().getValue().get("created_at").toString());
            }
            assertEquals("Iteration 1 should be the earlier AI (08:00)",
                    "2026-04-01T08:00:00Z", pairKeyToAiCreatedAt.get("img001::1"));
            assertEquals("Iteration 2 should be the later AI (09:00)",
                    "2026-04-01T09:00:00Z", pairKeyToAiCreatedAt.get("img001::2"));

            return null;
        });

        // ── NEW_PENDING: human kept alive for future AI iterations ────────────
        PAssert.that(newPending(routed)).satisfies(records -> {
            List<GenericRecord> list = new ArrayList<>();
            records.forEach(list::add);

            assertEquals("Human should be re-pended for future AI iterations", 1, list.size());
            assertEquals("human", list.get(0).get("pending_type").toString());
            assertEquals("img001", list.get(0).get("image_id").toString());

            return null;
        });

        // ── AGED_OUT: nothing should age out ─────────────────────────────────
        PAssert.that(agedOut(routed)).empty();

        pipeline.run().waitUntilFinish();
    }

    /**
     * Human arrives first, AI is pending from a prior run.
     * Expected: 1 MATCHED pair, human re-pended.
     */
    @Test
    public void pendingAiMatchesArrivingHuman() {
        TableRow human      = sourceRow("img002", HUMAN_METHOD, "key1", "2026-04-02T10:00:00Z");
        TableRow pendingAi  = pendingRow("img002", "ai", "key1",
                "2026-04-01T08:00:00Z", Instant.now().minusSeconds(3600).toString());

        PCollectionTuple routed = runPipeline("img002",
                List.of(human), List.of(pendingAi));

        PAssert.that(matched(routed)).satisfies(pairs -> {
            List<KV<String, KV<GenericRecord, GenericRecord>>> list = new ArrayList<>();
            pairs.forEach(list::add);
            assertEquals("Expected 1 matched pair", 1, list.size());
            assertEquals("img002::1", list.get(0).getKey());
            return null;
        });

        PAssert.that(newPending(routed)).satisfies(records -> {
            List<GenericRecord> list = new ArrayList<>();
            records.forEach(list::add);
            assertEquals("Human should be re-pended", 1, list.size());
            assertEquals("human", list.get(0).get("pending_type").toString());
            return null;
        });

        PAssert.that(agedOut(routed)).empty();
        pipeline.run().waitUntilFinish();
    }

    /**
     * AI arrives alone — no human in source or pending.
     * Expected: 0 MATCHED, 1 NEW_PENDING (the AI iteration).
     */
    @Test
    public void aiOnlyEmitsNewPending() {
        TableRow ai = sourceRow("img003", AI_METHOD, "key1", "2026-04-01T08:00:00Z");

        PCollectionTuple routed = runPipeline("img003", List.of(ai), List.of());

        PAssert.that(matched(routed)).empty();

        PAssert.that(newPending(routed)).satisfies(records -> {
            List<GenericRecord> list = new ArrayList<>();
            records.forEach(list::add);
            assertEquals("Expected 1 pending AI row", 1, list.size());
            assertEquals("ai", list.get(0).get("pending_type").toString());
            return null;
        });

        PAssert.that(agedOut(routed)).empty();
        pipeline.run().waitUntilFinish();
    }

    /**
     * AI has been pending for more than MAX_WAIT_DAYS with no human.
     * Expected: 0 MATCHED, 0 NEW_PENDING, 1 AGED_OUT.
     */
    @Test
    public void agedOutAiRoutesToDeadLetter() {
        String oldTimestamp = Instant.now()
                .minus(FilterAndPairFn.MAX_WAIT_DAYS + 1, ChronoUnit.DAYS)
                .toString();
        TableRow staleAi = pendingRow("img004", "ai", "key1",
                "2026-03-01T08:00:00Z", oldTimestamp);

        PCollectionTuple routed = runPipeline("img004", List.of(), List.of(staleAi));

        PAssert.that(matched(routed)).empty();
        PAssert.that(newPending(routed)).empty();

        PAssert.that(agedOut(routed)).satisfies(records -> {
            List<GenericRecord> list = new ArrayList<>();
            records.forEach(list::add);
            assertEquals("Expected 1 aged-out row", 1, list.size());
            assertEquals("ai", list.get(0).get("pending_type").toString());
            return null;
        });

        pipeline.run().waitUntilFinish();
    }
}
