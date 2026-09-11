package com.yourorg.pipeline.transforms;

import com.google.gson.JsonElement;
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
     * attributed to {@code caseId}. Mirrors {@link FilterAndPairFn#stampCaseIdByField} for
     * tests with no {@code atomicObjectFields} path in play — for those, use {@link
     * #stampedWithAtomicPaths} instead, since this overload passes no {@code atomicPaths} and
     * so (unlike production) recurses into every level, including inside atomic-object slots.
     */
    private static JsonObject stamped(String json, String caseId) {
        JsonObject o = obj(json);
        FilterAndPairFn.stampObjectRecursively(o, caseId);
        return o;
    }

    /**
     * Production-faithful stamping for tests involving {@code atomicObjectFields} paths (e.g.
     * {@code creditReportHeader}) — matches exactly what {@link
     * FilterAndPairFn#stampCaseIdByField} does at ingestion, stopping recursion at each given
     * atomic path so no spurious inner {@code _caseIdByField} gets created inside a slot's own
     * content (which the plain {@link #stamped} above does, since it passes no atomicPaths).
     */
    private static JsonObject stampedWithAtomicPaths(String json, String caseId, Set<String> atomicPaths) {
        JsonObject o = obj(json);
        FilterAndPairFn.stampObjectRecursively(o, caseId, "", atomicPaths);
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

    /**
     * A persisted pending array item already has _sourceCaseId, while the same source row
     * reselected by lookback does not. Provenance metadata must not make identical business
     * content look different, or every batch run appends another comparison.
     */
    @Test
    public void sameCompositeKeyIgnoresSourceCaseIdWhenDeduping() {
        JsonObject alreadyMerged = obj(
                "{\"documentProofs\":[{\"authenticationType\":\"Authentication\","
                        + "\"document\":\"passport\",\"disputeCode\":\"013\","
                        + "\"_sourceCaseId\":\"CASE-1\"}]}");
        JsonObject sameFreshRow = stamped(
                "{\"documentProofs\":[{\"authenticationType\":\"Authentication\","
                        + "\"document\":\"passport\",\"disputeCode\":\"013\"}]}", "CASE-1");

        JsonObject merged = FilterAndPairFn.mergeJsonObjects(
                alreadyMerged, "2026-01-01T00:00:00.000000Z",
                sameFreshRow, "2026-01-01T00:00:00.000000Z",
                Set.of("documentProofs"), Map.of(), Map.of(), Map.of(), "", "img", "main");

        var proofs = merged.getAsJsonArray("documentProofs");
        assertEquals("Same business item must not be duplicated just because pending has provenance",
                1, proofs.size());
        assertEquals("CASE-1", proofs.get(0).getAsJsonObject().get("_sourceCaseId").getAsString());
    }

    /**
     * Unkeyed/positional merge arrays (no ARRAY_MATCH_KEYS entry) are deliberately NOT
     * deduped by content — every item from both sides is kept, matching this path's original
     * always-concatenate semantics. No currently-configured mergeArrayFields array actually
     * takes this path (all 20 have a keyed match spec), but the behavior itself is intentional:
     * for a genuinely positional array, two items with identical field values could still be
     * two distinct real submissions, so collapsing them would risk data loss.
     */
    @Test
    public void unkeyedArrayConcatenatesWithoutDedupingByContent() {
        JsonObject alreadyMerged = obj(
                "{\"attachments\":[{\"name\":\"a.pdf\",\"_sourceCaseId\":\"CASE-1\"}]}");
        JsonObject sameFreshRow = stamped("{\"attachments\":[{\"name\":\"a.pdf\"}]}", "CASE-1");

        JsonObject merged = FilterAndPairFn.mergeJsonObjects(
                alreadyMerged, "2026-01-01T00:00:00.000000Z",
                sameFreshRow, "2026-01-01T00:00:00.000000Z",
                Set.of("attachments"), Map.of(), Map.of(), Map.of(), "", "img", "main");

        var attachments = merged.getAsJsonArray("attachments");
        assertEquals("Unkeyed arrays concatenate everything, even exact business-content "
                        + "replays — no dedup without a configured match key",
                2, attachments.size());
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
        JsonObject existing = stampedWithAtomicPaths(
                "{\"creditReportHeader\":{\"customerNumber\":\"X\","
                        + "\"currentNameRequested\":{\"disputeCodes\":[{\"code\":\"OLD\"}]}}}", "CASE-1",
                CREDIT_REPORT_HEADER_ATOMIC.keySet());
        JsonObject incoming = stampedWithAtomicPaths(
                "{\"creditReportHeader\":{\"customerNumber\":\"X\","
                        + "\"currentNameRequested\":{\"disputeCodes\":[{\"code\":\"NEW\"}]}}}", "CASE-2",
                CREDIT_REPORT_HEADER_ATOMIC.keySet());

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

    /**
     * An atomicObjectFields path with an EMPTY slot list (e.g. resultOfInvestigation,
     * requestor — self-contained records with no slot-like sub-objects of their own) degrades
     * cleanly: the whole object is taken wholesale from the latest case, one attribution entry
     * at the parent level, and no {@code _caseIdByField} at all inside the object itself since
     * there are no slots to carry forward.
     */
    @Test
    public void atomicObjectFieldWithNoSlotsIsTakenWhollyFromLatestCase() {
        Map<String, Set<String>> resultOfInvestigationAtomic =
                Map.of("resultOfInvestigation", Set.of());
        JsonObject existing = stamped(
                "{\"resultOfInvestigation\":{\"mailingAddress\":{\"city\":\"OLDTOWN\"},"
                        + "\"type\":\"ResultsOnly\"}}", "CASE-1");
        JsonObject incoming = stamped(
                "{\"resultOfInvestigation\":{\"mailingAddress\":{\"city\":\"NEWTOWN\"},"
                        + "\"type\":\"ResultsOnly\"}}", "CASE-2");

        JsonObject merged = FilterAndPairFn.mergeJsonObjects(
                existing, "2026-01-01T00:00:00.000000Z",
                incoming, "2026-01-02T00:00:00.000000Z",
                Set.of(), resultOfInvestigationAtomic, Map.of(), Map.of(), "", "img", "main");

        JsonObject result = merged.getAsJsonObject("resultOfInvestigation");
        assertEquals("NEWTOWN", result.getAsJsonObject("mailingAddress").get("city").getAsString());
        assertEquals("CASE-2",
                merged.getAsJsonObject("_caseIdByField").get("resultOfInvestigation").getAsString());
        assertFalse("A zero-slot atomic object must carry no _caseIdByField of its own",
                result.has("_caseIdByField"));
    }

    /**
     * Regression reproduction: production observed a multi-case group where the LATER case's
     * own slot (its own field, needing no attribution) and the EARLIER case's carried-forward
     * slot (a DIFFERENT slot the latest case never submitted) both ended up attributed to the
     * same case in the final comparison, even though they came from two different cases. This
     * mirrors {@link #atomicObjectFieldSlotOnlyInEarlierCaseIsCarriedForward} but exercises TWO
     * distinct slots simultaneously — one the winner has (dateOfBirthRequested), one only the
     * loser has (currentNameRequested) — to confirm the slot loop resolves each independently
     * rather than one call's outcome leaking into the other.
     */
    @Test
    public void winnerAndLoserEachContributingADifferentSlotAreAttributedToTheirOwnCase() {
        JsonObject existing = stampedWithAtomicPaths(
                "{\"creditReportHeader\":{\"customerNumber\":\"X\","
                        + "\"dateOfBirthRequested\":{\"disputeCodes\":[{\"code\":\"DOB\"}]}}}",
                "CASE-31", CREDIT_REPORT_HEADER_ATOMIC.keySet());
        JsonObject incoming = stampedWithAtomicPaths(
                "{\"creditReportHeader\":{\"customerNumber\":\"X\","
                        + "\"currentNameRequested\":{\"disputeCodes\":[{\"code\":\"NAME\"}]}}}",
                "CASE-32", CREDIT_REPORT_HEADER_ATOMIC.keySet());

        // CASE-31 is later (winner); CASE-32 is earlier (loser) — matches production exactly.
        JsonObject merged = FilterAndPairFn.mergeJsonObjects(
                existing, "2026-01-02T00:00:00.000000Z",
                incoming, "2026-01-01T00:00:00.000000Z",
                Set.of(), CREDIT_REPORT_HEADER_ATOMIC, Map.of(), Map.of(), "", "img", "main");

        JsonObject header = merged.getAsJsonObject("creditReportHeader");
        assertEquals("Both slots must survive the merge",
                "DOB", header.getAsJsonObject("dateOfBirthRequested")
                        .getAsJsonArray("disputeCodes").get(0).getAsJsonObject().get("code").getAsString());
        assertEquals("Both slots must survive the merge",
                "NAME", header.getAsJsonObject("currentNameRequested")
                        .getAsJsonArray("disputeCodes").get(0).getAsJsonObject().get("code").getAsString());

        assertEquals("Object-level entry must point to the winner (CASE-31)",
                "CASE-31", merged.getAsJsonObject("_caseIdByField").get("creditReportHeader").getAsString());
        assertFalse("The winner's own slot (dateOfBirthRequested) needs no explicit override",
                header.has("_caseIdByField")
                        && header.getAsJsonObject("_caseIdByField").has("dateOfBirthRequested"));
        assertEquals("The loser's carried-forward slot (currentNameRequested) must be explicitly "
                        + "attributed to CASE-32, not fall back to the object-level CASE-31 entry",
                "CASE-32", header.getAsJsonObject("_caseIdByField").get("currentNameRequested").getAsString());
    }

    /**
     * Regression probe: a THIRD case arriving in a later, separate merge round, contributing
     * a new slot ({@code socialSecurityNumberRequested}), forces the two slots from the FIRST
     * round ({@code dateOfBirthRequested} from CASE-31, {@code currentNameRequested} from
     * CASE-32) to be carried forward AGAIN as loser-side content. {@code mergeAtomicWithSlots}
     * derives the tag for every carried-forward slot in one call from a single object-level
     * {@code loserCase} (the OTHER side's {@code creditReportHeader} -> case entry) rather than
     * each slot's own already-recorded inner tag — so if the loser side here is the ROUND-1
     * MERGED result (whose object-level entry now says "CASE-31", the round-1 winner, not
     * CASE-32), the currentNameRequested slot's already-correct CASE-32 attribution could be
     * silently overwritten with CASE-31 on this second round.
     */
    @Test
    public void thirdCaseInALaterRoundMustNotCorruptAnEarlierRoundsSlotAttribution() {
        // Round 1: CASE-31 (later) contributes dateOfBirthRequested, CASE-32 (earlier)
        // contributes currentNameRequested — same as the production scenario already verified.
        JsonObject case31 = stamped(
                "{\"creditReportHeader\":{\"customerNumber\":\"X\","
                        + "\"dateOfBirthRequested\":{\"disputeCodes\":[{\"code\":\"DOB\"}]}}}", "CASE-31");
        JsonObject case32 = stamped(
                "{\"creditReportHeader\":{\"customerNumber\":\"X\","
                        + "\"currentNameRequested\":{\"disputeCodes\":[{\"code\":\"912\"}]}}}", "CASE-32");
        JsonObject roundOneMerged = FilterAndPairFn.mergeJsonObjects(
                case31, "2026-01-02T00:00:00.000000Z",
                case32, "2026-01-01T00:00:00.000000Z",
                Set.of(), CREDIT_REPORT_HEADER_ATOMIC, Map.of(), Map.of(), "", "img", "main");

        JsonObject roundOneHeader = roundOneMerged.getAsJsonObject("creditReportHeader");
        assertEquals("Sanity check: round 1 must already have currentNameRequested tagged CASE-32",
                "CASE-32",
                roundOneHeader.getAsJsonObject("_caseIdByField").get("currentNameRequested").getAsString());

        // Round 2: CASE-33 arrives LATER than CASE-31, contributing socialSecurityNumberRequested
        // — a slot neither prior case had. CASE-33 becomes the new object-level winner.
        JsonObject case33 = stamped(
                "{\"creditReportHeader\":{\"customerNumber\":\"X\","
                        + "\"socialSecurityNumberRequested\":{\"disputeCodes\":[{\"code\":\"SSN\"}]}}}",
                "CASE-33");

        JsonObject roundTwoMerged = FilterAndPairFn.mergeJsonObjects(
                roundOneMerged, "2026-01-02T00:00:00.000000Z",
                case33, "2026-01-03T00:00:00.000000Z",
                Set.of(), CREDIT_REPORT_HEADER_ATOMIC, Map.of(), Map.of(), "", "img", "main");

        JsonObject header = roundTwoMerged.getAsJsonObject("creditReportHeader");
        assertEquals("CASE-33", roundTwoMerged.getAsJsonObject("_caseIdByField").get("creditReportHeader").getAsString());

        assertEquals("dateOfBirthRequested must still resolve to CASE-31 after round 2",
                "CASE-31", header.getAsJsonObject("_caseIdByField").get("dateOfBirthRequested").getAsString());
        assertEquals("currentNameRequested must STILL resolve to CASE-32 after round 2 — "
                        + "not be corrupted to CASE-31 (round 1's object-level winner) just because "
                        + "it had to be carried forward a second time",
                "CASE-32", header.getAsJsonObject("_caseIdByField").get("currentNameRequested").getAsString());
    }

    /**
     * Regression probe for the WINNER-side counterpart of the earlier loser-side fix: an
     * already-merged composite that carries its own inner {@code _caseIdByField} (e.g.
     * {@code roundOneMerged} from the test above, where CASE-31 won round 1 but CASE-32's
     * {@code currentNameRequested} slot was correctly carried forward and tagged) can itself
     * REMAIN the object-level winner of a LATER round against a third, older case — this
     * happens in production whenever a long {@code humanLookbackDays} window keeps re-reading
     * an older case fresh every run, forcing repeated self-merges. Before this fix,
     * {@code mergeAtomicWithSlots} only checked the LOSER side for a slot's own more specific
     * tag; the winner-side copy (`winnerObj.has(slotKey)`) blindly assumed "winner has it, no
     * tag needed" — silently discarding {@code currentNameRequested}'s already-correct
     * CASE-32 tag the moment the composite that carries it wins yet another round.
     */
    @Test
    public void winnerSideCompositeMustNotLoseASlotsTagItAlreadyCarriedFromAnEarlierRound() {
        // Production-faithful stamping (real atomicPaths, matching stampCaseIdByField) — the
        // plain stamped() helper over-recurses into creditReportHeader's own children, which
        // would add noise not present in real data for this specific test.
        Set<String> atomicPaths = Set.of("creditReportHeader");
        JsonObject case31 = JsonParser.parseString(
                "{\"creditReportHeader\":{\"customerNumber\":\"X\","
                        + "\"dateOfBirthRequested\":{\"disputeCodes\":[{\"code\":\"DOB\"}]}}}").getAsJsonObject();
        FilterAndPairFn.stampObjectRecursively(case31, "CASE-31", "", atomicPaths);
        JsonObject case32 = JsonParser.parseString(
                "{\"creditReportHeader\":{\"customerNumber\":\"X\","
                        + "\"currentNameRequested\":{\"disputeCodes\":[{\"code\":\"912\"}]}}}").getAsJsonObject();
        FilterAndPairFn.stampObjectRecursively(case32, "CASE-32", "", atomicPaths);

        // Round 1: CASE-31 (winner, dateOfBirthRequested) vs CASE-32 (loser, currentNameRequested).
        JsonObject roundOneMerged = FilterAndPairFn.mergeJsonObjects(
                case31, "2026-01-02T00:00:00.000000Z",
                case32, "2026-01-01T00:00:00.000000Z",
                Set.of(), CREDIT_REPORT_HEADER_ATOMIC, Map.of(), Map.of(), "", "img", "main");
        assertEquals("Sanity check: round 1 must tag currentNameRequested to CASE-32",
                "CASE-32",
                roundOneMerged.getAsJsonObject("creditReportHeader")
                        .getAsJsonObject("_caseIdByField").get("currentNameRequested").getAsString());
        assertFalse("Sanity check: round 1 must NOT tag the winner's own dateOfBirthRequested",
                roundOneMerged.getAsJsonObject("creditReportHeader")
                        .getAsJsonObject("_caseIdByField").has("dateOfBirthRequested"));

        // Round 2: roundOneMerged (created_at = CASE-31's, the latest so far, per the
        // maxCreatedAt fix) merges against a THIRD, OLDER case — roundOneMerged REMAINS the
        // object-level winner, since it's still the latest.
        JsonObject case34 = JsonParser.parseString(
                "{\"creditReportHeader\":{\"customerNumber\":\"X\","
                        + "\"socialSecurityNumberRequested\":{\"disputeCodes\":[{\"code\":\"SSN\"}]}}}")
                .getAsJsonObject();
        FilterAndPairFn.stampObjectRecursively(case34, "CASE-34", "", atomicPaths);
        JsonObject roundTwoMerged = FilterAndPairFn.mergeJsonObjects(
                roundOneMerged, "2026-01-02T00:00:00.000000Z",
                case34, "2025-12-31T00:00:00.000000Z",
                Set.of(), CREDIT_REPORT_HEADER_ATOMIC, Map.of(), Map.of(), "", "img", "main");

        JsonObject header = roundTwoMerged.getAsJsonObject("creditReportHeader");
        assertEquals("roundOneMerged must remain the object-level winner (still the latest)",
                "CASE-31", roundTwoMerged.getAsJsonObject("_caseIdByField").get("creditReportHeader").getAsString());
        assertEquals("SSN slot from the older CASE-34 must be carried forward and tagged to it",
                "CASE-34", header.getAsJsonObject("_caseIdByField").get("socialSecurityNumberRequested").getAsString());
        assertEquals("currentNameRequested must NOT lose its CASE-32 tag just because the "
                        + "composite that carries it won this round too — it is still on loan, "
                        + "not the current winner's own content",
                "CASE-32", header.getAsJsonObject("_caseIdByField").get("currentNameRequested").getAsString());
        assertFalse("dateOfBirthRequested is genuinely CASE-31's own field — still needs no tag",
                header.getAsJsonObject("_caseIdByField").has("dateOfBirthRequested"));
    }

    /**
     * Regression test: {@code FilterAndPairFn#mergeAcrossCases} must set the MERGED record's
     * own {@code created_at} to the LATEST of its two contributors' timestamps (via {@code
     * maxCreatedAt}) — this value feeds {@code existingWinsTies} in the NEXT merge round (via
     * {@code humanBySubType}'s sequential fold), so it must always reflect the group's true
     * most-recent contributor. Before the fix, this used the EARLIEST contributor instead,
     * which let a case that is chronologically earlier than the group's true latest
     * contributor still "win" a later round's non-slot wholesale content, simply because it
     * was later than the group's understated (earliest-of-two) reported timestamp. This test
     * simulates {@code mergeAcrossCases}'s bookkeeping directly against {@code
     * mergeJsonObjects} (bypassing encryption, same limitation as every other test in this
     * file) to confirm that no longer happens.
     */
    @Test
    public void groupCreatedAtTracksTheLatestContributorSoAnEarlierCaseCannotWinALaterRound() {
        // Round 1: CASE-31 (2026-01-02, later) beats CASE-32 (2026-01-01, earlier) — CASE-31
        // wins non-slot wholesale content (customerNumber = "FROM31").
        JsonObject case31 = stamped(
                "{\"creditReportHeader\":{\"customerNumber\":\"FROM31\"}}", "CASE-31");
        JsonObject case32 = stamped(
                "{\"creditReportHeader\":{\"customerNumber\":\"FROM32\"}}", "CASE-32");
        JsonObject roundOneMerged = FilterAndPairFn.mergeJsonObjects(
                case31, "2026-01-02T00:00:00.000000Z",
                case32, "2026-01-01T00:00:00.000000Z",
                Set.of(), CREDIT_REPORT_HEADER_ATOMIC, Map.of(), Map.of(), "", "img", "main");
        assertEquals("Sanity check: CASE-31 must win round 1's wholesale content",
                "FROM31",
                roundOneMerged.getAsJsonObject("creditReportHeader").get("customerNumber").getAsString());

        // Mirrors the FIXED FilterAndPairFn.mergeAcrossCases: the merged record's own
        // created_at is now the LATEST of the two contributors — CASE-31's timestamp.
        String roundOneGroupCreatedAt = "2026-01-02T00:00:00.000000Z";

        // Round 2: CASE-33 arrives with a timestamp BETWEEN CASE-32 and CASE-31 — chronologically
        // EARLIER than the group's true latest contributor (CASE-31).
        JsonObject case33 = stamped(
                "{\"creditReportHeader\":{\"customerNumber\":\"FROM33\"}}", "CASE-33");
        JsonObject roundTwoMerged = FilterAndPairFn.mergeJsonObjects(
                roundOneMerged, roundOneGroupCreatedAt,
                case33, "2026-01-01T12:00:00.000000Z",
                Set.of(), CREDIT_REPORT_HEADER_ATOMIC, Map.of(), Map.of(), "", "img", "main");

        assertEquals("CASE-31 must remain the wholesale-content winner — CASE-33 is "
                        + "chronologically earlier and must not win just because the group's "
                        + "reported created_at used to understate the true latest contributor",
                "FROM31",
                roundTwoMerged.getAsJsonObject("creditReportHeader").get("customerNumber").getAsString());
    }

    /**
     * Array-side analog of the atomicObjectFields probe above: a THIRD case arrives in a later,
     * separate merge round, contributing a brand-new address slot ("Former2"). Confirms the
     * array-merge path does NOT share the atomicObjectFields bug — items from round 1
     * ("Current" from CASE-1, "Former1" from CASE-2) must keep their own {@code _sourceCaseId}
     * after round 2, not get relabeled with whichever case contributed round 2's item.
     */
    @Test
    public void thirdCaseInALaterRoundMustNotCorruptEarlierArrayItemAttribution() {
        JsonObject case1 = stamped(
                "{\"addresses\":[{\"addressType\":\"Current\",\"streetNumber\":\"100\"}]}", "CASE-1");
        JsonObject case2 = stamped(
                "{\"addresses\":[{\"addressType\":\"Former1\",\"streetNumber\":\"200\"}]}", "CASE-2");

        JsonObject roundOneMerged = FilterAndPairFn.mergeJsonObjects(
                case1, "2026-01-02T00:00:00.000000Z",
                case2, "2026-01-01T00:00:00.000000Z",
                Set.of("addresses"), Map.of(), Map.of(),
                Map.of("addresses", "addressType"), "", "img", "main");

        JsonObject case3 = stamped(
                "{\"addresses\":[{\"addressType\":\"Former2\",\"streetNumber\":\"300\"}]}", "CASE-3");

        JsonObject roundTwoMerged = FilterAndPairFn.mergeJsonObjects(
                roundOneMerged, "2026-01-02T00:00:00.000000Z",
                case3, "2026-01-03T00:00:00.000000Z",
                Set.of("addresses"), Map.of(), Map.of(),
                Map.of("addresses", "addressType"), "", "img", "main");

        Map<String, String> caseByAddressType = new java.util.HashMap<>();
        for (JsonElement el : roundTwoMerged.getAsJsonArray("addresses")) {
            JsonObject item = el.getAsJsonObject();
            caseByAddressType.put(item.get("addressType").getAsString(), item.get("_sourceCaseId").getAsString());
        }

        assertEquals("Current (round 1, CASE-1) must keep its own attribution after round 2",
                "CASE-1", caseByAddressType.get("Current"));
        assertEquals("Former1 (round 1, CASE-2) must keep its own attribution after round 2",
                "CASE-2", caseByAddressType.get("Former1"));
        assertEquals("Former2 (round 2, CASE-3) must be attributed to CASE-3",
                "CASE-3", caseByAddressType.get("Former2"));
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

    // ── stripProvenanceRecursively (humanContentSignature's stability fix) ────

    /**
     * Regression test for a production incident: a multi-case group's payload is re-merged
     * from scratch every run (persisted state combined with fresh re-reads of the same
     * underlying case rows), and that re-merge can assign DIFFERENT _caseIdByField/
     * _sourceCaseId attribution for an identical-content tie between runs (e.g.
     * existingWinsTies depends on which side lands in the "existing" vs "incoming" role, which
     * can vary with BigQuery's unordered row-read order) even when every actual submitted field
     * value is unchanged. Observed in production as a group whose comparison_version/
     * ai_iteration climbed on every single run for days — every extractable field value was
     * verified identical between consecutive iterations, yet the signature kept changing.
     * stripProvenanceRecursively must remove _caseIdByField/_sourceCaseId at every nesting
     * level so two payloads differing ONLY in this bookkeeping hash identically.
     */
    @Test
    public void stripProvenanceRecursivelyRemovesAttributionAtEveryNestingLevel() {
        JsonObject withAttributionA = obj(
                "{\"firstName\":\"John\","
                        + "\"_caseIdByField\":{\"firstName\":\"CASE-1\"},"
                        + "\"address\":{\"city\":\"Austin\",\"_caseIdByField\":{\"city\":\"CASE-1\"}},"
                        + "\"documentProofs\":[{\"document\":\"passport\",\"_sourceCaseId\":\"CASE-1\"}]}");
        JsonObject withAttributionB = obj(
                "{\"firstName\":\"John\","
                        + "\"_caseIdByField\":{\"firstName\":\"CASE-2\"},"
                        + "\"address\":{\"city\":\"Austin\",\"_caseIdByField\":{\"city\":\"CASE-2\"}},"
                        + "\"documentProofs\":[{\"document\":\"passport\",\"_sourceCaseId\":\"CASE-2\"}]}");

        FilterAndPairFn.stripProvenanceRecursively(withAttributionA);
        FilterAndPairFn.stripProvenanceRecursively(withAttributionB);

        assertEquals("Two payloads differing ONLY in attribution bookkeeping must be "
                        + "byte-identical once stripped, so their signatures match",
                withAttributionA, withAttributionB);
        assertFalse(withAttributionA.has("_caseIdByField"));
        assertFalse(withAttributionA.getAsJsonObject("address").has("_caseIdByField"));
        assertFalse(withAttributionA.getAsJsonArray("documentProofs")
                .get(0).getAsJsonObject().has("_sourceCaseId"));
    }

    /**
     * Regression test for the second, independent source of the same production incident:
     * mergeJsonObjects/mergeArrayItems build their output by iterating LinkedHashSets seeded
     * from "existing" then "incoming" keys, and which side is "existing" vs "incoming" traces
     * back to the order result.getAll(SOURCE_TAG) returns rows — a plain SELECT with no
     * ORDER BY, so BigQuery does not guarantee the same row order across separate query
     * executions. Two payloads with the exact same fields/values but different OBJECT KEY
     * insertion order must canonicalize to the identical structure, so their signatures match
     * regardless of which order a given merge round happened to produce them in.
     */
    @Test
    public void canonicalizeIsIndifferentToObjectKeyOrder() {
        JsonObject orderA = obj("{\"firstName\":\"John\",\"lastName\":\"Doe\",\"age\":30}");
        JsonObject orderB = obj("{\"age\":30,\"lastName\":\"Doe\",\"firstName\":\"John\"}");

        assertEquals(FilterAndPairFn.canonicalize(orderA), FilterAndPairFn.canonicalize(orderB));
    }

    /** Same as above, but for array element order rather than object key order. */
    @Test
    public void canonicalizeIsIndifferentToArrayElementOrder() {
        JsonObject orderA = obj(
                "{\"tradelines\":[{\"accountNumber\":\"111\"},{\"accountNumber\":\"222\"}]}");
        JsonObject orderB = obj(
                "{\"tradelines\":[{\"accountNumber\":\"222\"},{\"accountNumber\":\"111\"}]}");

        assertEquals(FilterAndPairFn.canonicalize(orderA), FilterAndPairFn.canonicalize(orderB));
    }

    /** Canonicalize must still distinguish payloads that are genuinely different. */
    @Test
    public void canonicalizeStillDistinguishesGenuinelyDifferentContent() {
        JsonObject a = obj("{\"firstName\":\"John\"}");
        JsonObject b = obj("{\"firstName\":\"Johnny\"}");

        assertFalse(FilterAndPairFn.canonicalize(a).equals(FilterAndPairFn.canonicalize(b)));
    }
}
