package com.yourorg.pipeline.transforms;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.junit.Test;

import java.util.Map;
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
                Set.of(), Map.of(), Map.of(), Map.of(), "", "img", "main");

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
                Set.of(), Map.of(), Map.of(), Map.of(), "", "img", "main");

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
                Set.of("documentProofs"), Map.of(), Map.of(), Map.of(), "", "img", "main");

        var proofs = merged.getAsJsonArray("documentProofs");
        assertEquals(2, proofs.size());
        assertEquals("passport", proofs.get(0).getAsJsonObject().get("document").getAsString());
        assertEquals("CASE-1", proofs.get(0).getAsJsonObject().get("_sourceCaseId").getAsString());
        assertEquals("utility_bill", proofs.get(1).getAsJsonObject().get("document").getAsString());
        assertEquals("CASE-2", proofs.get(1).getAsJsonObject().get("_sourceCaseId").getAsString());
        assertFalse("Array-level attribution lives per-item, not in _caseIdByField",
                merged.has("_caseIdByField") && merged.getAsJsonObject("_caseIdByField").has("documentProofs"));
    }

    /**
     * Two items sharing the same composite key (ARRAY_MATCH_KEYS: "authenticationType-document"
     * for documentProofs) but with genuinely different content is a real item-level collision —
     * resolved by latest created_at, not silently dropped in favor of whichever was added first.
     */
    @Test
    public void sameCompositeKeyDifferentContentResolvesByLatestNotFirstAdded() {
        JsonObject existing = stamped(
                "{\"documentProofs\":[{\"authenticationType\":\"Authentication\",\"document\":\"passport\","
                        + "\"disputeCode\":\"013\"}]}", "CASE-1");
        JsonObject incoming = stamped(
                "{\"documentProofs\":[{\"authenticationType\":\"Authentication\",\"document\":\"passport\","
                        + "\"disputeCode\":\"001\"}]}", "CASE-2");

        JsonObject merged = FilterAndPairFn.mergeJsonObjects(
                existing, "2026-01-01T00:00:00.000000Z",
                incoming, "2026-01-02T00:00:00.000000Z",
                Set.of("documentProofs"), Map.of(), Map.of(), Map.of(), "", "img", "main");

        var proofs = merged.getAsJsonArray("documentProofs");
        assertEquals("Same key, different content collapses to ONE item, not two", 1, proofs.size());
        assertEquals("Later case's item wins, not whichever was added first", "001",
                proofs.get(0).getAsJsonObject().get("disputeCode").getAsString());
        assertEquals("CASE-2", proofs.get(0).getAsJsonObject().get("_sourceCaseId").getAsString());
    }

    /** Two items sharing the same composite key with IDENTICAL content dedupe with no data loss. */
    @Test
    public void sameCompositeKeyIdenticalContentDedupesToOneCopy() {
        JsonObject existing = stamped(
                "{\"documentProofs\":[{\"authenticationType\":\"Authentication\",\"document\":\"passport\","
                        + "\"disputeCode\":\"013\"}]}", "CASE-1");
        JsonObject incoming = stamped(
                "{\"documentProofs\":[{\"authenticationType\":\"Authentication\",\"document\":\"passport\","
                        + "\"disputeCode\":\"013\"}]}", "CASE-2");

        JsonObject merged = FilterAndPairFn.mergeJsonObjects(
                existing, "2026-01-01T00:00:00.000000Z",
                incoming, "2026-01-02T00:00:00.000000Z",
                Set.of("documentProofs"), Map.of(), Map.of(), Map.of(), "", "img", "main");

        var proofs = merged.getAsJsonArray("documentProofs");
        assertEquals(1, proofs.size());
        assertEquals("013", proofs.get(0).getAsJsonObject().get("disputeCode").getAsString());
        assertEquals("Identical content still attributes to the latest case", "CASE-2",
                proofs.get(0).getAsJsonObject().get("_sourceCaseId").getAsString());
    }

    /** A scalar collision is resolved by latest created_at, with provenance recorded. */
    @Test
    public void scalarCollisionKeepsLatestAndRecordsProvenance() {
        JsonObject existing = stamped("{\"firstName\":\"John\",\"dob\":\"1990-01-01\"}", "CASE-1");
        JsonObject incoming = stamped("{\"firstName\":\"Johnny\",\"middleName\":\"Q\"}", "CASE-2");

        JsonObject merged = FilterAndPairFn.mergeJsonObjects(
                existing, "2026-01-01T00:00:00.000000Z",
                incoming, "2026-01-02T00:00:00.000000Z",
                Set.of(), Map.of(), Map.of(), Map.of(), "", "img", "main");

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
                Set.of(), Map.of(), Map.of(), Map.of(), "", "img", "main");

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
                Set.of(), Map.of(), Map.of(), Map.of(), "", "img", "main");

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
                Set.of("documentProofs"), Map.of(), Map.of(), Map.of(), "", "img", "main");

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
                Set.of(), Map.of(), Map.of(), Map.of(), "", "img", "main");

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
                Set.of("credit.disputeCodes"), Map.of(), Map.of(), Map.of(), "", "img", "main");

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
                Set.of("credit.disputeCodes"), Map.of(), Map.of(), Map.of(), "", "img", "main");

        // Configured nested path concatenates.
        assertEquals(2, merged.getAsJsonObject("credit").getAsJsonArray("disputeCodes").size());
        // Unconfigured top-level array of the same bare name is a plain (non-array-equal)
        // collision instead — latest wins, wholesale, not concatenated.
        var topLevel = merged.getAsJsonArray("disputeCodes");
        assertEquals(1, topLevel.size());
        assertEquals("Y", topLevel.get(0).getAsJsonObject().get("code").getAsString());
    }

    // ── atomicObjectFields (e.g. creditReportHeader) ─────────────────────────

    private static final Map<String, Set<String>> CREDIT_REPORT_HEADER_ATOMIC = Map.of(
            "creditReportHeader", Set.of(
                    "dateOfBirthRequested", "currentNameRequested", "socialSecurityNumberRequested"));

    /**
     * Non-slot fields of an atomicObjectFields path are taken WHOLESALE from the latest case
     * — not blended field-by-field. A field only the earlier case had (and the latest case
     * doesn't) is dropped, not carried forward: "taken from the latest case" means the latest
     * case's own field set defines what survives, unlike the normal per-field recursive merge.
     */
    @Test
    public void atomicObjectFieldNonSlotFieldsComeWhollyFromLatestCase() {
        JsonObject existing = stamped(
                "{\"creditReportHeader\":{\"customerNumber\":\"OLD123\",\"age\":50,"
                        + "\"onlyInEarlier\":\"shouldBeDropped\"}}", "CASE-1");
        JsonObject incoming = stamped(
                "{\"creditReportHeader\":{\"customerNumber\":\"NEW456\",\"age\":51}}", "CASE-2");

        JsonObject merged = FilterAndPairFn.mergeJsonObjects(
                existing, "2026-01-01T00:00:00.000000Z",
                incoming, "2026-01-02T00:00:00.000000Z",
                Set.of(), CREDIT_REPORT_HEADER_ATOMIC, Map.of(), Map.of(), "", "img", "main");

        JsonObject header = merged.getAsJsonObject("creditReportHeader");
        assertEquals("NEW456", header.get("customerNumber").getAsString());
        assertEquals(51, header.get("age").getAsInt());
        assertFalse("A field only the earlier (non-latest) case had must be dropped, not merged",
                header.has("onlyInEarlier"));
        // Non-slot fields carry no per-field attribution of their own — one entry at the
        // parent level covers all of them via resolveCaseId's path walk-up.
        assertEquals("CASE-2", merged.getAsJsonObject("_caseIdByField").get("creditReportHeader").getAsString());
        assertFalse("Non-slot fields must not duplicate attribution onto every individual field",
                header.has("_caseIdByField") && header.getAsJsonObject("_caseIdByField").has("customerNumber"));
    }

    /** A slot present in the LATEST case wins outright — its own version, not blended with the earlier one. */
    @Test
    public void atomicObjectFieldSlotPresentInLatestCaseWinsOutright() {
        JsonObject existing = stamped(
                "{\"creditReportHeader\":{\"customerNumber\":\"X\","
                        + "\"currentNameRequested\":{\"disputeCodes\":[{\"code\":\"OLD\"}]}}}", "CASE-1");
        JsonObject incoming = stamped(
                "{\"creditReportHeader\":{\"customerNumber\":\"X\","
                        + "\"currentNameRequested\":{\"disputeCodes\":[{\"code\":\"NEW\"}]}}}", "CASE-2");

        JsonObject merged = FilterAndPairFn.mergeJsonObjects(
                existing, "2026-01-01T00:00:00.000000Z",
                incoming, "2026-01-02T00:00:00.000000Z",
                Set.of(), CREDIT_REPORT_HEADER_ATOMIC, Map.of(), Map.of(), "", "img", "main");

        JsonObject header = merged.getAsJsonObject("creditReportHeader");
        String code = header.getAsJsonObject("currentNameRequested")
                .getAsJsonArray("disputeCodes").get(0).getAsJsonObject().get("code").getAsString();
        assertEquals("NEW", code);
        // A winner-sourced slot needs no explicit attribution — it inherits CASE-2 from the
        // object-level entry the same way the non-slot fields do.
        assertEquals("CASE-2", merged.getAsJsonObject("_caseIdByField").get("creditReportHeader").getAsString());
        assertFalse("Winner-sourced slot must not need its own explicit override",
                header.has("_caseIdByField") && header.getAsJsonObject("_caseIdByField").has("currentNameRequested"));
    }

    /** A slot only an EARLIER (non-latest) case has is carried forward, not lost. */
    @Test
    public void atomicObjectFieldSlotOnlyInEarlierCaseIsCarriedForward() {
        JsonObject existing = stamped(
                "{\"creditReportHeader\":{\"customerNumber\":\"X\","
                        + "\"socialSecurityNumberRequested\":{\"disputeCodes\":[{\"code\":\"913\"}]}}}",
                "CASE-1");
        JsonObject incoming = stamped(
                "{\"creditReportHeader\":{\"customerNumber\":\"X\"}}", "CASE-2");

        JsonObject merged = FilterAndPairFn.mergeJsonObjects(
                existing, "2026-01-01T00:00:00.000000Z",
                incoming, "2026-01-02T00:00:00.000000Z",
                Set.of(), CREDIT_REPORT_HEADER_ATOMIC, Map.of(), Map.of(), "", "img", "main");

        JsonObject header = merged.getAsJsonObject("creditReportHeader");
        // Non-slot fields (customerNumber) inherit CASE-2 from the object-level entry.
        assertEquals("CASE-2", merged.getAsJsonObject("_caseIdByField").get("creditReportHeader").getAsString());
        String code = header.getAsJsonObject("socialSecurityNumberRequested")
                .getAsJsonArray("disputeCodes").get(0).getAsJsonObject().get("code").getAsString();
        assertEquals("913", code);
        assertEquals("Slot carried forward from the earlier case must still be attributed to it",
                "CASE-1", header.getAsJsonObject("_caseIdByField").get("socialSecurityNumberRequested").getAsString());
    }

    // ── arrayItemPriorityField (e.g. addresses keyed by addressType) ─────────

    private static final Map<String, String> ADDRESS_PRIORITY = Map.of("addresses", "addressRequested");

    /**
     * addresses is compared against AI by content (streetNumber-postalCode, the real
     * ARRAY_MATCH_KEYS entry) but merged across cases by addressType instead — mirrors
     * SegmentConfig.mergeItemKeyField, decoupled from the comparison key.
     */
    private static final Map<String, String> ADDRESS_MERGE_KEY = Map.of("addresses", "addressType");

    /**
     * Two cases' versions of the same address slot (paired by addressType, not content):
     * the EARLIER case's version carries addressRequested (actively disputed) while the
     * LATER case's version doesn't — the disputed version must win outright, overriding the
     * normal latest-created_at-wins rule.
     */
    @Test
    public void addressSlotWithPriorityFieldWinsEvenWhenEarlier() {
        JsonObject existing = stamped(
                "{\"addresses\":[{\"addressType\":\"Current\",\"streetNumber\":\"059\","
                        + "\"addressRequested\":{\"disputeCodes\":[{\"code\":\"021\"}]}}]}", "CASE-1");
        JsonObject incoming = stamped(
                "{\"addresses\":[{\"addressType\":\"Current\",\"streetNumber\":\"9148\"}]}", "CASE-2");

        JsonObject merged = FilterAndPairFn.mergeJsonObjects(
                existing, "2026-01-01T00:00:00.000000Z",
                incoming, "2026-01-02T00:00:00.000000Z",
                Set.of("addresses"), Map.of(), ADDRESS_PRIORITY, ADDRESS_MERGE_KEY, "", "img", "main");

        var addresses = merged.getAsJsonArray("addresses");
        assertEquals("Same slot (addressType=Current) collapses to one item, not two", 1, addresses.size());
        JsonObject current = addresses.get(0).getAsJsonObject();
        assertEquals("The disputed (earlier) version wins over the undisputed later one",
                "059", current.get("streetNumber").getAsString());
        assertEquals("CASE-1", current.get("_sourceCaseId").getAsString());
    }

    /** Neither side's version of the slot has the priority field — falls back to latest-wins. */
    @Test
    public void addressSlotWithNoPriorityFieldOnEitherSideFallsBackToLatestWins() {
        JsonObject existing = stamped(
                "{\"addresses\":[{\"addressType\":\"Current\",\"streetNumber\":\"059\"}]}", "CASE-1");
        JsonObject incoming = stamped(
                "{\"addresses\":[{\"addressType\":\"Current\",\"streetNumber\":\"9148\"}]}", "CASE-2");

        JsonObject merged = FilterAndPairFn.mergeJsonObjects(
                existing, "2026-01-01T00:00:00.000000Z",
                incoming, "2026-01-02T00:00:00.000000Z",
                Set.of("addresses"), Map.of(), ADDRESS_PRIORITY, ADDRESS_MERGE_KEY, "", "img", "main");

        JsonObject current = merged.getAsJsonArray("addresses").get(0).getAsJsonObject();
        assertEquals("9148", current.get("streetNumber").getAsString());
        assertEquals("CASE-2", current.get("_sourceCaseId").getAsString());
    }

    /** Different addressType slots (Current vs Former1) are unrelated — both simply kept. */
    @Test
    public void differentAddressSlotsAreBothKept() {
        JsonObject existing = stamped(
                "{\"addresses\":[{\"addressType\":\"Current\",\"streetNumber\":\"059\"}]}", "CASE-1");
        JsonObject incoming = stamped(
                "{\"addresses\":[{\"addressType\":\"Former1\",\"streetNumber\":\"9148\"}]}", "CASE-2");

        JsonObject merged = FilterAndPairFn.mergeJsonObjects(
                existing, "2026-01-01T00:00:00.000000Z",
                incoming, "2026-01-02T00:00:00.000000Z",
                Set.of("addresses"), Map.of(), ADDRESS_PRIORITY, ADDRESS_MERGE_KEY, "", "img", "main");

        assertEquals(2, merged.getAsJsonArray("addresses").size());
    }
}
