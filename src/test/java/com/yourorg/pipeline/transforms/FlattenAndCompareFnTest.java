package com.yourorg.pipeline.transforms;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.yourorg.pipeline.util.JsonFieldExtractor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;

/**
 * Direct unit tests for {@link FlattenAndCompareFn#resolveCaseId} and for
 * {@link JsonFieldExtractor#flatten}'s per-value case_id propagation (the mechanism
 * {@code resolveCaseId}'s first-priority signal now relies on). Tested directly since the
 * surrounding DoFn needs live Barricade decryption with no local test seam (same limitation as
 * {@code FilterAndPairFnMergeTest}'s doc on why it tests {@code mergeJsonObjects} directly).
 */
public class FlattenAndCompareFnTest {

    /** The item's own case_id (carried directly on the FieldValue) always wins first. */
    @Test
    public void itemCaseIdTakesPrecedenceOverPathAndCanonical() {
        String result = FlattenAndCompareFn.resolveCaseId(
                "addresses.addressRequested.streetNumber", "CASE-34",
                Map.of("addresses", "CASE-PATH"), "CASE-CANONICAL");

        assertEquals("CASE-34", result);
    }

    /** With no item case_id, falls back to the path-walk. */
    @Test
    public void fallsBackToPathWalkWhenItemCaseIdAbsent() {
        Map<String, String> caseIdByPath = Map.of("addresses", "CASE-PATH");

        String result = FlattenAndCompareFn.resolveCaseId(
                "addresses.addressRequested.disputeCodes.code", null,
                caseIdByPath, "CASE-CANONICAL");

        assertEquals("CASE-PATH", result);
    }

    /** With no item case_id and no path match, falls back to canonical. */
    @Test
    public void fallsBackToCanonicalWhenNothingElseMatches() {
        String result = FlattenAndCompareFn.resolveCaseId(
                "someField", null, Map.of(), "CASE-CANONICAL");

        assertEquals("CASE-CANONICAL", result);
    }

    // ── End-to-end reproduction: creditReportHeader cross-case attribution ──────

    private static final Map<String, Set<String>> CREDIT_REPORT_HEADER_ATOMIC = Map.of(
            "creditReportHeader", Set.of(
                    "dateOfBirthRequested", "currentNameRequested", "socialSecurityNumberRequested"));

    /**
     * Full pipeline reproduction of the production scenario: two cases sharing the SAME
     * {@code creditReportHeader.customerNumber} (same consumer, same credit report), the later
     * case (CASE-31) contributing {@code dateOfBirthRequested}, the earlier case (CASE-32)
     * contributing {@code currentNameRequested} — exercised through the REAL chain production
     * uses: {@link FilterAndPairFn#mergeJsonObjects} (cross-case merge) →
     * {@link FlattenAndCompareFn#extractAndStripCaseIdByField} → {@link JsonFieldExtractor#flatten}
     * (using the real {@link FlattenAndCompareFn#ARRAY_MATCH_KEYS}, which configures
     * {@code creditReportHeader} itself as a "customerNumber" keying context — see
     * JsonFieldExtractor.flattenObject — so nested disputeCodes items inherit a matchKey
     * prefixed with the shared customerNumber, not just their own bare code value) → {@link
     * FlattenAndCompareFn#resolveCaseId}, reading each field's own {@code caseId} directly off
     * its {@link JsonFieldExtractor.FieldValue} (carried through flatten from {@code
     * _sourceCaseId} — no separate matchKey-keyed lookup involved). Confirms the loser's
     * carried-forward slot resolves to its own case even with this extra matchKey-prefixing
     * layer in play.
     */
    @Test
    public void creditReportHeaderLoserSlotResolvesToItsOwnCaseThroughFullPipeline() {
        JsonObject existing = JsonParser.parseString(
                "{\"creditReportHeader\":{\"customerNumber\":\"CUST1\","
                        + "\"dateOfBirthRequested\":{\"disputeCodes\":[{\"code\":\"DOB1\"}]}}}")
                .getAsJsonObject();
        FilterAndPairFn.stampObjectRecursively(existing, "CASE-31");
        JsonObject incoming = JsonParser.parseString(
                "{\"creditReportHeader\":{\"customerNumber\":\"CUST1\","
                        + "\"currentNameRequested\":{\"disputeCodes\":[{\"code\":\"912\"}]}}}")
                .getAsJsonObject();
        FilterAndPairFn.stampObjectRecursively(incoming, "CASE-32");

        // CASE-31 is later (winner); CASE-32 is earlier (loser) — matches production exactly.
        JsonObject merged = FilterAndPairFn.mergeJsonObjects(
                existing, "2026-01-02T00:00:00.000000Z",
                incoming, "2026-01-01T00:00:00.000000Z",
                Set.of(), CREDIT_REPORT_HEADER_ATOMIC, Map.of(), Map.of(), "", "img", "main");

        Map<String, String> caseIdByPath = new HashMap<>();
        String strippedPayload =
                FlattenAndCompareFn.extractAndStripCaseIdByField(merged.toString(), caseIdByPath);

        Map<String, List<JsonFieldExtractor.FieldValue>> humanFields =
                JsonFieldExtractor.flatten(strippedPayload, FlattenAndCompareFn.ARRAY_MATCH_KEYS);

        String field = "creditReportHeader.currentNameRequested.disputeCodes.code";
        List<JsonFieldExtractor.FieldValue> values = humanFields.get(field);
        assertEquals("Expected exactly one disputeCodes.code entry for the loser's slot",
                1, values.size());

        String resolved = FlattenAndCompareFn.resolveCaseId(
                field, values.get(0).caseId, caseIdByPath, "CASE-31");

        assertEquals("The earlier case's (CASE-32) carried-forward currentNameRequested slot "
                        + "must resolve to CASE-32, not the winner CASE-31 or canonical fallback",
                "CASE-32", resolved);

        // Also verify the WINNER's own slot (dateOfBirthRequested) resolves to CASE-31 through
        // this same full pipeline — not yet separately confirmed end-to-end, only at the pure
        // merge-output level. It has no inner tag of its own, so it must fall through to the
        // object-level "creditReportHeader" -> CASE-31 path entry.
        String dobField = "creditReportHeader.dateOfBirthRequested.disputeCodes.code";
        List<JsonFieldExtractor.FieldValue> dobValues = humanFields.get(dobField);
        assertEquals(1, dobValues.size());
        String dobResolved = FlattenAndCompareFn.resolveCaseId(
                dobField, dobValues.get(0).caseId, caseIdByPath, "CASE-31");
        assertEquals("The winner's own dateOfBirthRequested slot must resolve to CASE-31",
                "CASE-31", dobResolved);
    }

    // ── Regression: items whose content collides on the comparison-time matchKey ────

    /**
     * Regression test for a production bug: {@code addresses} is keyed for AI-vs-human
     * comparison by content ({@code streetNumber-postalCode}), which is NOT guaranteed unique
     * across different items of the array — a case's {@code former} address can legitimately
     * equal another case's {@code current} address. Reproduced exactly: case1's {@code current}
     * (streetNumber=8027, postalCode=78645) and case3's {@code former} (the same values)
     * produce the identical matchKey "8027-78645" despite being two different items
     * attributed to two different cases.
     *
     * <p>Before the fix, {@code _sourceCaseId} was extracted into a separate {@code matchKey ->
     * case_id} side map, so a shared matchKey meant one item's attribution silently overwrote
     * the other's (or, after an interim fix, both were left unresolved). Now each item's
     * {@code caseId} is carried directly on its own {@link JsonFieldExtractor.FieldValue} by
     * {@code flatten} itself — a matchKey collision can no longer cause cross-contamination,
     * because there is no shared lookup to collide in. Both items resolve correctly: {@code
     * current} to case1, {@code former} to case3.
     */
    @Test
    public void differentItemsSharingAMatchKeyEachResolveToTheirOwnCase() {
        JsonObject payload = JsonParser.parseString(
                "{\"addresses\":["
                        + "{\"addressType\":\"current\",\"streetNumber\":\"8027\","
                        + "\"postalCode\":\"78645\",\"addressRequested\":{\"disputeCodes\":"
                        + "[{\"code\":\"A1\"}]},\"_sourceCaseId\":\"case1\"},"
                        + "{\"addressType\":\"former\",\"streetNumber\":\"8027\","
                        + "\"postalCode\":\"78645\",\"_sourceCaseId\":\"case3\"}"
                        + "]}").getAsJsonObject();

        Map<String, List<JsonFieldExtractor.FieldValue>> humanFields =
                JsonFieldExtractor.flatten(payload.toString(), FlattenAndCompareFn.ARRAY_MATCH_KEYS);

        List<JsonFieldExtractor.FieldValue> addressTypes = humanFields.get("addresses.addressType");
        assertEquals(2, addressTypes.size());
        assertEquals("8027-78645", addressTypes.get(0).matchKey);
        assertEquals("8027-78645", addressTypes.get(1).matchKey);

        JsonFieldExtractor.FieldValue currentType = addressTypes.stream()
                .filter(fv -> "current".equals(fv.value)).findFirst().orElseThrow();
        JsonFieldExtractor.FieldValue formerType = addressTypes.stream()
                .filter(fv -> "former".equals(fv.value)).findFirst().orElseThrow();

        assertEquals("current's own case_id must survive despite sharing former's matchKey",
                "case1", currentType.caseId);
        assertEquals("former's own case_id must survive despite sharing current's matchKey",
                "case3", formerType.caseId);

        // The nested disputeCodes item (only present under `current`) must inherit `current`'s
        // case_id, not fall through to the group canonical or pick up `former`'s.
        List<JsonFieldExtractor.FieldValue> disputeCodes =
                humanFields.get("addresses.addressRequested.disputeCodes.code");
        assertEquals(1, disputeCodes.size());
        assertEquals("case1", disputeCodes.get(0).caseId);

        // resolveCaseId, as the production row-emission loop actually calls it: each item's
        // own caseId is used directly, taking precedence over any path/canonical fallback.
        assertEquals("case1", FlattenAndCompareFn.resolveCaseId(
                "addresses.addressType", currentType.caseId, Map.of(), "CASE-CANONICAL"));
        assertEquals("case3", FlattenAndCompareFn.resolveCaseId(
                "addresses.addressType", formerType.caseId, Map.of(), "CASE-CANONICAL"));
    }

    /** {@code _sourceCaseId} itself must never surface as a comparable field. */
    @Test
    public void sourceCaseIdIsNeverEmittedAsItsOwnComparableField() {
        JsonObject payload = JsonParser.parseString(
                "{\"addresses\":[{\"streetNumber\":\"8027\",\"postalCode\":\"78645\","
                        + "\"_sourceCaseId\":\"case1\"}]}").getAsJsonObject();

        Map<String, List<JsonFieldExtractor.FieldValue>> humanFields =
                JsonFieldExtractor.flatten(payload.toString(), FlattenAndCompareFn.ARRAY_MATCH_KEYS);

        assertFalse(humanFields.containsKey("addresses._sourceCaseId"));
    }

    /** A NO_CASE payload (no _sourceCaseId anywhere) leaves every FieldValue's caseId null. */
    @Test
    public void noCaseAttributionLeavesCaseIdNullThroughoutFlatten() {
        JsonObject payload = JsonParser.parseString(
                "{\"addresses\":[{\"streetNumber\":\"1\",\"postalCode\":\"2\"}]}")
                .getAsJsonObject();

        Map<String, List<JsonFieldExtractor.FieldValue>> humanFields =
                JsonFieldExtractor.flatten(payload.toString(), FlattenAndCompareFn.ARRAY_MATCH_KEYS);

        assertNull(humanFields.get("addresses.streetNumber").get(0).caseId);
    }
}
