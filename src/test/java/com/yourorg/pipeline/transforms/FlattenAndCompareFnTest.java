package com.yourorg.pipeline.transforms;

import org.junit.Test;

import java.util.Map;

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
}
