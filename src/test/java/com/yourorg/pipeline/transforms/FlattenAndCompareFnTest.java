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

/**
 * Direct unit tests for {@link FlattenAndCompareFn#resolveCaseId}, the pure case-id resolution
 * logic used per output row. Tested directly since the surrounding DoFn needs live Barricade
 * decryption with no local test seam (same limitation as {@code FilterAndPairFnMergeTest}'s doc
 * on why it tests {@code mergeJsonObjects} directly).
 */
public class FlattenAndCompareFnTest {

    /**
     * A field directly under an array item (not itself inside a further nested keyed
     * sub-array) has a matchKey identical to the item's own key — the exact lookup succeeds
     * with no stripping needed.
     */
    @Test
    public void directArrayItemFieldResolvesByExactMatchKey() {
        Map<String, String> caseIdByMatchKey = Map.of("9148-52267", "CASE-34");

        String result = FlattenAndCompareFn.resolveCaseId(
                "addresses.addressRequested.streetNumber", "9148-52267",
                caseIdByMatchKey, Map.of(), "CASE-CANONICAL");

        assertEquals("CASE-34", result);
    }

    /**
     * Regression test: a field nested inside a keyed sub-array one level deeper than where
     * _sourceCaseId is stamped (e.g. addresses.addressRequested.disputeCodes.code) has a
     * matchKey that composites the parent item's own key with the sub-array item's own key
     * (see JsonFieldExtractor.flattenArray) — MORE SPECIFIC than any key caseIdByMatchKey
     * actually has, since _sourceCaseId is only ever stamped on the outermost array item.
     * Before the fix, an exact-only lookup always missed here and fell through all the way to
     * canonicalCaseId, misattributing every such field to the group's canonical case regardless
     * of which item it actually came from — this is exactly the bug that surfaced in production
     * as an address's disputeCodes rows resolving to the wrong case_id.
     */
    @Test
    public void nestedSubArrayFieldFallsBackToParentItemsOwnMatchKey() {
        Map<String, String> caseIdByMatchKey = Map.of(
                "9547-52684", "CASE-33",
                "9148-52267", "CASE-34");

        String result = FlattenAndCompareFn.resolveCaseId(
                "addresses.addressRequested.disputeCodes.code", "9547-52684-913-913",
                caseIdByMatchKey, Map.of(), "CASE-CANONICAL");

        assertEquals("Must fall back to the parent address item's own key (9547-52684), "
                        + "not the group's canonical case",
                "CASE-33", result);
    }

    /** When no matchKey entry exists at any stripped level, falls through to the path-walk. */
    @Test
    public void fallsBackToPathWalkWhenNoMatchKeyEntryFoundAtAnyLevel() {
        Map<String, String> caseIdByMatchKey = Map.of("9148-52267", "CASE-34");
        Map<String, String> caseIdByPath = Map.of("addresses", "CASE-PATH");

        String result = FlattenAndCompareFn.resolveCaseId(
                "addresses.addressRequested.disputeCodes.code", "9999-00000-111-111",
                caseIdByMatchKey, caseIdByPath, "CASE-CANONICAL");

        assertEquals("CASE-PATH", result);
    }

    /** With no matchKey, no path match, and no per-field attribution, falls back to canonical. */
    @Test
    public void fallsBackToCanonicalWhenNothingElseMatches() {
        String result = FlattenAndCompareFn.resolveCaseId(
                "someField", null, Map.of(), Map.of(), "CASE-CANONICAL");

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
     * prefixed with the shared customerNumber, not just their own bare code value) →
     * {@link FlattenAndCompareFn#extractAndStripSourceCaseId} → {@link
     * FlattenAndCompareFn#resolveCaseId}. Confirms the loser's carried-forward slot resolves to
     * its own case even with this extra matchKey-prefixing layer in play.
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
        Map<String, String> caseIdByMatchKey =
                FlattenAndCompareFn.extractAndStripSourceCaseId(humanFields);

        String field = "creditReportHeader.currentNameRequested.disputeCodes.code";
        List<JsonFieldExtractor.FieldValue> values = humanFields.get(field);
        assertEquals("Expected exactly one disputeCodes.code entry for the loser's slot",
                1, values.size());
        String matchKey = values.get(0).matchKey;

        String resolved = FlattenAndCompareFn.resolveCaseId(
                field, matchKey, caseIdByMatchKey, caseIdByPath, "CASE-31");

        assertEquals("The earlier case's (CASE-32) carried-forward currentNameRequested slot "
                        + "must resolve to CASE-32, not the winner CASE-31 or canonical fallback",
                "CASE-32", resolved);

        // Also verify the WINNER's own slot (dateOfBirthRequested) resolves to CASE-31 through
        // this same full pipeline — not yet separately confirmed end-to-end, only at the pure
        // merge-output level. It has no inner tag of its own, so it must fall through the
        // matchKey branch (customerNumber-prefixed key, same mechanism as currentNameRequested)
        // and land on the object-level "creditReportHeader" -> CASE-31 path entry.
        String dobField = "creditReportHeader.dateOfBirthRequested.disputeCodes.code";
        List<JsonFieldExtractor.FieldValue> dobValues = humanFields.get(dobField);
        assertEquals(1, dobValues.size());
        String dobResolved = FlattenAndCompareFn.resolveCaseId(
                dobField, dobValues.get(0).matchKey, caseIdByMatchKey, caseIdByPath, "CASE-31");
        assertEquals("The winner's own dateOfBirthRequested slot must resolve to CASE-31",
                "CASE-31", dobResolved);
    }
}
