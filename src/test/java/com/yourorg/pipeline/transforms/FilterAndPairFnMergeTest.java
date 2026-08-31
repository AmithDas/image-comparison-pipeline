package com.yourorg.pipeline.transforms;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.junit.Test;

import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;

/**
 * Direct unit tests for {@link FilterAndPairFn#mergeJsonObjects}, the pure JSON-level merge
 * used for cross-case collisions on the same subType bucket. Tested directly (bypassing
 * FilterAndPairFn's Beam DoFn and Barricade encrypt/decrypt) since BarricadeEncryptionUtil
 * needs live GCP KMS/Firestore access with no local test seam — see FilterAndPairFnTest for
 * the end-to-end pipeline-level coverage of the surrounding matching/pending logic.
 *
 * <h3>Input convention</h3>
 * Attribution is read from whatever {@code _caseIdByField} is already embedded in each input
 * JsonObject — production code embeds this at ingestion via
 * {@link FilterAndPairFn#stampCaseIdByField} before a record ever reaches this method. A test
 * input with no {@code _caseIdByField} at all represents NO_CASE (e.g. authentication/
 * docreview, which never carry a real case_id and so are never stamped).
 */
public class FilterAndPairFnMergeTest {

    private static JsonObject obj(String json) {
        return JsonParser.parseString(json).getAsJsonObject();
    }

    /**
     * A stamped record — every field of {@code json}, recursively at every nesting level,
     * attributed to {@code caseId}. Mirrors exactly what
     * {@link FilterAndPairFn#stampCaseIdByField} does at ingestion in production.
     */
    private static JsonObject stamped(String json, String caseId) {
        JsonObject o = obj(json);
        FilterAndPairFn.stampObjectRecursively(o, caseId);
        return o;
    }

    /**
     * A field present in only one contributing case is kept as-is, and attributed to that
     * case — both sides are pre-stamped (as production ingestion would do), so provenance is
     * recorded per field even though no single field's *value* was actually contested.
     */
    @Test
    public void disjointFieldsAreUnionedAndEachAttributedToItsContributor() {
        JsonObject existing = stamped("{\"a\":\"1\"}", "CASE-1");
        JsonObject incoming = stamped("{\"b\":\"2\"}", "CASE-2");

        JsonObject merged = FilterAndPairFn.mergeJsonObjects(
                existing, "2026-01-01T00:00:00.000000Z",
                incoming, "2026-01-02T00:00:00.000000Z",
                Set.of(), "", "img", "main");

        assertEquals("1", merged.get("a").getAsString());
        assertEquals("2", merged.get("b").getAsString());
        JsonObject byField = merged.getAsJsonObject("_caseIdByField");
        assertEquals("CASE-1", byField.get("a").getAsString());
        assertEquals("CASE-2", byField.get("b").getAsString());
    }

    /**
     * Unstamped input (NO_CASE — matching authentication/docreview, which never carry a real
     * case_id and are never run through stampCaseIdByField) must stay completely free of
     * provenance metadata after merging.
     */
    @Test
    public void unstampedInputAddsNoProvenanceForDisjointFields() {
        JsonObject existing = obj("{\"a\":\"1\"}");
        JsonObject incoming = obj("{\"b\":\"2\"}");

        JsonObject merged = FilterAndPairFn.mergeJsonObjects(
                existing, "2026-01-01T00:00:00.000000Z",
                incoming, "2026-01-02T00:00:00.000000Z",
                Set.of(), "", "img", "main");

        assertEquals("1", merged.get("a").getAsString());
        assertEquals("2", merged.get("b").getAsString());
        assertFalse("Neither side was ever stamped — no real provenance to record",
                merged.has("_caseIdByField"));
    }

    /** A configured array field concatenates items from both sides and stamps each with its source case. */
    @Test
    public void configuredArrayFieldConcatenatesAndStampsSourceCaseId() {
        JsonObject existing = stamped("{\"documentProofs\":[{\"document\":\"passport\"}]}", "CASE-1");
        JsonObject incoming = stamped("{\"documentProofs\":[{\"document\":\"utility_bill\"}]}", "CASE-2");

        JsonObject merged = FilterAndPairFn.mergeJsonObjects(
                existing, "2026-01-01T00:00:00.000000Z",
                incoming, "2026-01-02T00:00:00.000000Z",
                Set.of("documentProofs"), "", "img", "main");

        var proofs = merged.getAsJsonArray("documentProofs");
        assertEquals(2, proofs.size());
        assertEquals("passport", proofs.get(0).getAsJsonObject().get("document").getAsString());
        assertEquals("CASE-1", proofs.get(0).getAsJsonObject().get("_sourceCaseId").getAsString());
        assertEquals("utility_bill", proofs.get(1).getAsJsonObject().get("document").getAsString());
        assertEquals("CASE-2", proofs.get(1).getAsJsonObject().get("_sourceCaseId").getAsString());
        assertFalse("Array-level attribution lives per-item, not in _caseIdByField",
                merged.has("_caseIdByField") && merged.getAsJsonObject("_caseIdByField").has("documentProofs"));
    }

    /** A scalar collision is resolved by latest created_at, with provenance recorded. */
    @Test
    public void scalarCollisionKeepsLatestAndRecordsProvenance() {
        JsonObject existing = stamped("{\"firstName\":\"John\",\"dob\":\"1990-01-01\"}", "CASE-1");
        JsonObject incoming = stamped("{\"firstName\":\"Johnny\",\"middleName\":\"Q\"}", "CASE-2");

        JsonObject merged = FilterAndPairFn.mergeJsonObjects(
                existing, "2026-01-01T00:00:00.000000Z",
                incoming, "2026-01-02T00:00:00.000000Z",
                Set.of(), "", "img", "main");

        assertEquals("Later case's value wins on scalar collision", "Johnny",
                merged.get("firstName").getAsString());
        assertEquals("1990-01-01", merged.get("dob").getAsString());
        assertEquals("Q", merged.get("middleName").getAsString());

        JsonObject byField = merged.getAsJsonObject("_caseIdByField");
        assertEquals("CASE-2", byField.get("firstName").getAsString());
        assertEquals("CASE-1", byField.get("dob").getAsString());
        assertEquals("CASE-2", byField.get("middleName").getAsString());
    }

    /** Identical values on both sides need no collision resolution, but still keep provenance. */
    @Test
    public void identicalValuesAttributeToTheLatestCase() {
        JsonObject existing = stamped("{\"status\":\"verified\"}", "CASE-1");
        JsonObject incoming = stamped("{\"status\":\"verified\"}", "CASE-2");

        JsonObject merged = FilterAndPairFn.mergeJsonObjects(
                existing, "2026-01-01T00:00:00.000000Z",
                incoming, "2026-01-02T00:00:00.000000Z",
                Set.of(), "", "img", "main");

        assertEquals("verified", merged.get("status").getAsString());
        assertEquals("Same tie-break direction as an actual collision — latest wins",
                "CASE-2", merged.getAsJsonObject("_caseIdByField").get("status").getAsString());
    }

    /** A third case colliding on an already-merged bucket extends, not resets, provenance. */
    @Test
    public void thirdCaseCollidingOnAlreadyMergedBucketExtendsProvenance() {
        JsonObject alreadyMerged = obj(
                "{\"firstName\":\"John\",\"middleName\":\"Q\","
                        + "\"_caseIdByField\":{\"firstName\":\"CASE-1\",\"middleName\":\"CASE-2\"}}");
        JsonObject caseThree = stamped("{\"lastName\":\"Smith\"}", "CASE-3");

        JsonObject merged = FilterAndPairFn.mergeJsonObjects(
                alreadyMerged, "2026-01-01T00:00:00.000000Z",
                caseThree, "2026-01-03T00:00:00.000000Z",
                Set.of(), "", "img", "main");

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
                "{\"documentProofs\":[{\"document\":\"passport\",\"_sourceCaseId\":\"CASE-1\"}],"
                        + "\"_caseIdByField\":{}}");
        JsonObject caseTwo = stamped("{\"documentProofs\":[{\"document\":\"utility_bill\"}]}", "CASE-2");

        JsonObject merged = FilterAndPairFn.mergeJsonObjects(
                alreadyMerged, "2026-01-01T00:00:00.000000Z",
                caseTwo, "2026-01-02T00:00:00.000000Z",
                Set.of("documentProofs"), "", "img", "main");

        var proofs = merged.getAsJsonArray("documentProofs");
        assertEquals(2, proofs.size());
        assertEquals("CASE-1", proofs.get(0).getAsJsonObject().get("_sourceCaseId").getAsString());
        assertEquals("CASE-2", proofs.get(1).getAsJsonObject().get("_sourceCaseId").getAsString());
    }

    /**
     * A difference nested inside an object must not discard unrelated sibling fields at that
     * same nesting level — only the field that actually differs collides; the rest is unioned.
     */
    @Test
    public void nestedObjectDifferenceOnlyCollidesTheFieldThatActuallyDiffers() {
        JsonObject existing = stamped("{\"address\":{\"city\":\"NY\",\"zip\":\"10001\"}}", "CASE-1");
        JsonObject incoming = stamped("{\"address\":{\"city\":\"NY\",\"zip\":\"20002\"}}", "CASE-2");

        JsonObject merged = FilterAndPairFn.mergeJsonObjects(
                existing, "2026-01-01T00:00:00.000000Z",
                incoming, "2026-01-02T00:00:00.000000Z",
                Set.of(), "", "img", "main");

        JsonObject address = merged.getAsJsonObject("address");
        assertEquals("Undisputed sibling field must survive the merge", "NY",
                address.get("city").getAsString());
        assertEquals("Later case wins the field that actually collided", "20002",
                address.get("zip").getAsString());

        assertFalse("No collision at the outer level — attribution lives inside 'address'",
                merged.has("_caseIdByField"));
        JsonObject byField = address.getAsJsonObject("_caseIdByField");
        assertEquals("Undisputed field still attributes to the latest case", "CASE-2",
                byField.get("city").getAsString());
        assertEquals("CASE-2", byField.get("zip").getAsString());
    }

    /** mergeArrayFields supports a dot-notation path to concatenate an array nested inside an object. */
    @Test
    public void mergeArrayFieldsMatchesANestedDotNotationPath() {
        JsonObject existing = stamped("{\"credit\":{\"disputeCodes\":[{\"code\":\"A\"}]}}", "CASE-1");
        JsonObject incoming = stamped("{\"credit\":{\"disputeCodes\":[{\"code\":\"B\"}]}}", "CASE-2");

        JsonObject merged = FilterAndPairFn.mergeJsonObjects(
                existing, "2026-01-01T00:00:00.000000Z",
                incoming, "2026-01-02T00:00:00.000000Z",
                Set.of("credit.disputeCodes"), "", "img", "main");

        var codes = merged.getAsJsonObject("credit").getAsJsonArray("disputeCodes");
        assertEquals(2, codes.size());
        assertEquals("A", codes.get(0).getAsJsonObject().get("code").getAsString());
        assertEquals("CASE-1", codes.get(0).getAsJsonObject().get("_sourceCaseId").getAsString());
        assertEquals("B", codes.get(1).getAsJsonObject().get("code").getAsString());
        assertEquals("CASE-2", codes.get(1).getAsJsonObject().get("_sourceCaseId").getAsString());
    }

    /**
     * An array field with the SAME bare name at two different nesting depths must not cross-match
     * — mergeArrayFields is matched by full path, so listing "credit.disputeCodes" must not
     * accidentally also concatenate an unrelated top-level "disputeCodes" array.
     */
    @Test
    public void mergeArrayFieldsPathMatchingDoesNotCrossNestingLevels() {
        JsonObject existing = stamped(
                "{\"disputeCodes\":[{\"code\":\"X\"}],\"credit\":{\"disputeCodes\":[{\"code\":\"A\"}]}}",
                "CASE-1");
        JsonObject incoming = stamped(
                "{\"disputeCodes\":[{\"code\":\"Y\"}],\"credit\":{\"disputeCodes\":[{\"code\":\"B\"}]}}",
                "CASE-2");

        JsonObject merged = FilterAndPairFn.mergeJsonObjects(
                existing, "2026-01-01T00:00:00.000000Z",
                incoming, "2026-01-02T00:00:00.000000Z",
                Set.of("credit.disputeCodes"), "", "img", "main");

        // Configured nested path concatenates.
        assertEquals(2, merged.getAsJsonObject("credit").getAsJsonArray("disputeCodes").size());
        // Unconfigured top-level array of the same bare name is a plain (non-array-equal)
        // collision instead — latest wins, wholesale, not concatenated.
        var topLevel = merged.getAsJsonArray("disputeCodes");
        assertEquals(1, topLevel.size());
        assertEquals("Y", topLevel.get(0).getAsJsonObject().get("code").getAsString());
    }
}
