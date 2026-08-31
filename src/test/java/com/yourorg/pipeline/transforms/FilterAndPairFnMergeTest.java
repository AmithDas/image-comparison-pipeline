package com.yourorg.pipeline.transforms;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.junit.Test;

import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Direct unit tests for {@link FilterAndPairFn#mergeJsonObjects}, the pure JSON-level merge
 * used for cross-case collisions on the same subType bucket. Tested directly (bypassing
 * FilterAndPairFn's Beam DoFn and Barricade encrypt/decrypt) since BarricadeEncryptionUtil
 * needs live GCP KMS/Firestore access with no local test seam — see FilterAndPairFnTest for
 * the end-to-end pipeline-level coverage of the surrounding matching/pending logic.
 */
public class FilterAndPairFnMergeTest {

    private static JsonObject obj(String json) {
        return JsonParser.parseString(json).getAsJsonObject();
    }

    /**
     * A field present in only one contributing case is kept as-is, and attributed to that
     * case in _caseIdByField — this bucket has two distinct contributors (CASE-1, CASE-2),
     * so provenance is recorded even though no single field's *value* was actually contested.
     */
    @Test
    public void disjointFieldsAreUnionedAndEachAttributedToItsContributor() {
        JsonObject existing = obj("{\"a\":\"1\"}");
        JsonObject incoming = obj("{\"b\":\"2\"}");

        JsonObject merged = FilterAndPairFn.mergeJsonObjects(
                existing, "CASE-1", "2026-01-01T00:00:00.000000Z",
                incoming, "CASE-2", "2026-01-02T00:00:00.000000Z",
                Set.of(), "img", "main");

        assertEquals("1", merged.get("a").getAsString());
        assertEquals("2", merged.get("b").getAsString());
        JsonObject byField = merged.getAsJsonObject("_caseIdByField");
        assertEquals("CASE-1", byField.get("a").getAsString());
        assertEquals("CASE-2", byField.get("b").getAsString());
    }

    /**
     * A single-contributor bucket (existingSoleCaseId blank, matching the NO_CASE sentinel used
     * for segments like authentication/docreview that never carry a real case_id) must stay
     * completely free of provenance metadata even when this method merges two records — this
     * is the "collision" that happens when NO_CASE resubmits, not a real cross-case merge.
     */
    @Test
    public void blankExistingSoleCaseIdAddsNoProvenanceForDisjointFields() {
        JsonObject existing = obj("{\"a\":\"1\"}");
        JsonObject incoming = obj("{\"b\":\"2\"}");

        JsonObject merged = FilterAndPairFn.mergeJsonObjects(
                existing, "", "2026-01-01T00:00:00.000000Z",
                incoming, "", "2026-01-02T00:00:00.000000Z",
                Set.of(), "img", "main");

        assertEquals("1", merged.get("a").getAsString());
        assertEquals("2", merged.get("b").getAsString());
        assertFalse("Both sides are NO_CASE — no real provenance to record",
                merged.has("_caseIdByField"));
    }

    /** A configured array field concatenates items from both sides and stamps each with its source case. */
    @Test
    public void configuredArrayFieldConcatenatesAndStampsSourceCaseId() {
        JsonObject existing = obj("{\"documentProofs\":[{\"document\":\"passport\"}]}");
        JsonObject incoming = obj("{\"documentProofs\":[{\"document\":\"utility_bill\"}]}");

        JsonObject merged = FilterAndPairFn.mergeJsonObjects(
                existing, "CASE-1", "2026-01-01T00:00:00.000000Z",
                incoming, "CASE-2", "2026-01-02T00:00:00.000000Z",
                Set.of("documentProofs"), "img", "main");

        var proofs = merged.getAsJsonArray("documentProofs");
        assertEquals(2, proofs.size());
        assertEquals("passport", proofs.get(0).getAsJsonObject().get("document").getAsString());
        assertEquals("CASE-1", proofs.get(0).getAsJsonObject().get("_sourceCaseId").getAsString());
        assertEquals("utility_bill", proofs.get(1).getAsJsonObject().get("document").getAsString());
        assertEquals("CASE-2", proofs.get(1).getAsJsonObject().get("_sourceCaseId").getAsString());
        assertFalse("Array-level attribution lives per-item, not in _caseIdByField",
                merged.has("_caseIdByField") && merged.getAsJsonObject("_caseIdByField").has("documentProofs"));
    }

    /** A scalar collision is resolved by earliest created_at, with provenance recorded. */
    @Test
    public void scalarCollisionKeepsEarliestAndRecordsProvenance() {
        JsonObject existing = obj("{\"firstName\":\"John\",\"dob\":\"1990-01-01\"}");
        JsonObject incoming = obj("{\"firstName\":\"Johnny\",\"middleName\":\"Q\"}");

        JsonObject merged = FilterAndPairFn.mergeJsonObjects(
                existing, "CASE-1", "2026-01-01T00:00:00.000000Z",
                incoming, "CASE-2", "2026-01-02T00:00:00.000000Z",
                Set.of(), "img", "main");

        assertEquals("Earlier case's value wins on scalar collision", "John",
                merged.get("firstName").getAsString());
        assertEquals("1990-01-01", merged.get("dob").getAsString());
        assertEquals("Q", merged.get("middleName").getAsString());

        JsonObject byField = merged.getAsJsonObject("_caseIdByField");
        assertEquals("CASE-1", byField.get("firstName").getAsString());
        assertEquals("CASE-1", byField.get("dob").getAsString());
        assertEquals("CASE-2", byField.get("middleName").getAsString());
    }

    /** Identical values on both sides need no collision resolution or provenance entry. */
    @Test
    public void identicalValuesAttributeToTheEarlierSideWithNoWarningNeeded() {
        JsonObject existing = obj("{\"status\":\"verified\"}");
        JsonObject incoming = obj("{\"status\":\"verified\"}");

        JsonObject merged = FilterAndPairFn.mergeJsonObjects(
                existing, "CASE-1", "2026-01-01T00:00:00.000000Z",
                incoming, "CASE-2", "2026-01-02T00:00:00.000000Z",
                Set.of(), "img", "main");

        assertEquals("verified", merged.get("status").getAsString());
    }

    /** A third case colliding on an already-merged bucket extends, not resets, provenance. */
    @Test
    public void thirdCaseColldingOnAlreadyMergedBucketExtendsProvenance() {
        JsonObject alreadyMerged = obj(
                "{\"firstName\":\"John\",\"middleName\":\"Q\","
                        + "\"_caseIdByField\":{\"firstName\":\"CASE-1\",\"middleName\":\"CASE-2\"}}");
        JsonObject caseThree = obj("{\"lastName\":\"Smith\"}");

        // existingSoleCaseId is null here because the bucket already has >1 contributor —
        // mirrors FilterAndPairFn's subTypeSoleCaseId being removed on first collision.
        JsonObject merged = FilterAndPairFn.mergeJsonObjects(
                alreadyMerged, null, "2026-01-01T00:00:00.000000Z",
                caseThree, "CASE-3", "2026-01-03T00:00:00.000000Z",
                Set.of(), "img", "main");

        assertEquals("John", merged.get("firstName").getAsString());
        assertEquals("Q", merged.get("middleName").getAsString());
        assertEquals("Smith", merged.get("lastName").getAsString());

        JsonObject byField = merged.getAsJsonObject("_caseIdByField");
        assertEquals("Pre-existing attribution must survive untouched",
                "CASE-1", byField.get("firstName").getAsString());
        assertEquals("CASE-2", byField.get("middleName").getAsString());
        assertEquals("CASE-3", byField.get("lastName").getAsString());
    }

    /** Re-merging an already-stamped array item must not double-stamp _sourceCaseId. */
    @Test
    public void reMergingDoesNotDoubleStampAlreadyTaggedArrayItems() {
        JsonObject alreadyMerged = obj(
                "{\"documentProofs\":[{\"document\":\"passport\",\"_sourceCaseId\":\"CASE-1\"}]}");
        JsonObject caseTwo = obj("{\"documentProofs\":[{\"document\":\"utility_bill\"}]}");

        JsonObject merged = FilterAndPairFn.mergeJsonObjects(
                alreadyMerged, null, "2026-01-01T00:00:00.000000Z",
                caseTwo, "CASE-2", "2026-01-02T00:00:00.000000Z",
                Set.of("documentProofs"), "img", "main");

        var proofs = merged.getAsJsonArray("documentProofs");
        assertEquals(2, proofs.size());
        assertEquals("CASE-1", proofs.get(0).getAsJsonObject().get("_sourceCaseId").getAsString());
        assertEquals("CASE-2", proofs.get(1).getAsJsonObject().get("_sourceCaseId").getAsString());
    }

    /** authentication/docreview never carry a real case_id — a single-contributor merge input
     *  (existingSoleCaseId null/blank, matching the NO_CASE sentinel) must never introduce
     *  _caseIdByField even when this method is invoked. */
    @Test
    public void blankCaseIdNeverIntroducesProvenanceMetadata() {
        JsonObject existing = obj("{\"a\":\"1\"}");
        JsonObject incoming = obj("{\"a\":\"2\"}");

        JsonObject merged = FilterAndPairFn.mergeJsonObjects(
                existing, "", "2026-01-01T00:00:00.000000Z",
                incoming, "", "2026-01-02T00:00:00.000000Z",
                Set.of(), "img", "main");

        assertEquals("1", merged.get("a").getAsString());
        assertNull("Blank case ids must never be recorded as provenance",
                merged.has("_caseIdByField") ? merged.getAsJsonObject("_caseIdByField").get("a") : null);
    }
}
