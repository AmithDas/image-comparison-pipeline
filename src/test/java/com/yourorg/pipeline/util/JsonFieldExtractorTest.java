package com.yourorg.pipeline.util;

import com.yourorg.pipeline.util.JsonFieldExtractor.FieldValue;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

public class JsonFieldExtractorTest {

    // ── helpers ───────────────────────────────────────────────────────────────

    /** Returns the single value for a scalar field (asserts exactly one entry). */
    private static String scalar(Map<String, List<FieldValue>> result, String field) {
        List<FieldValue> entries = result.get(field);
        assertNotNull("Field not found: " + field, entries);
        assertEquals("Expected scalar (1 entry) for field: " + field, 1, entries.size());
        assertNull("Expected null matchKey for scalar field: " + field, entries.get(0).matchKey);
        return entries.get(0).value;
    }

    /** Returns all values for a field (positional or keyed), in list order. */
    private static List<String> values(Map<String, List<FieldValue>> result, String field) {
        return result.get(field).stream().map(fv -> fv.value).collect(Collectors.toList());
    }

    /** Returns all matchKeys for a field, in list order. */
    private static List<String> keys(Map<String, List<FieldValue>> result, String field) {
        return result.get(field).stream().map(fv -> fv.matchKey).collect(Collectors.toList());
    }

    // ── flatten — scalars ─────────────────────────────────────────────────────

    @Test
    public void flatFlattensTopLevelScalars() {
        Map<String, List<FieldValue>> result =
                JsonFieldExtractor.flatten("{\"a\": \"val1\", \"b\": \"val2\"}");
        assertEquals("val1", scalar(result, "a"));
        assertEquals("val2", scalar(result, "b"));
        assertEquals(2, result.size());
    }

    @Test
    public void flatFlattensNestedFields() {
        Map<String, List<FieldValue>> result =
                JsonFieldExtractor.flatten("{\"a\": {\"b\": {\"c\": \"deep\"}}}");
        assertEquals("deep", scalar(result, "a.b.c"));
        assertEquals(1, result.size());
    }

    @Test
    public void flatHandlesNullValue() {
        Map<String, List<FieldValue>> result = JsonFieldExtractor.flatten("{\"a\": null}");
        assertNotNull(result.get("a"));
        assertNull(result.get("a").get(0).value);
        assertNull(result.get("a").get(0).matchKey);
    }

    @Test
    public void flatHandlesMixedNestedAndFlat() {
        Map<String, List<FieldValue>> result =
                JsonFieldExtractor.flatten("{\"a\":\"flat\",\"b\":{\"c\":\"nested\"},\"d\":null}");
        assertEquals("flat",   scalar(result, "a"));
        assertEquals("nested", scalar(result, "b.c"));
        assertNull(result.get("d").get(0).value);
        assertEquals(3, result.size());
    }

    @Test public void flatNullJsonReturnsEmpty()   { assertTrue(JsonFieldExtractor.flatten(null).isEmpty()); }
    @Test public void flatBlankJsonReturnsEmpty()  { assertTrue(JsonFieldExtractor.flatten("   ").isEmpty()); }
    @Test public void flatBadJsonReturnsEmpty()    { assertTrue(JsonFieldExtractor.flatten("{bad").isEmpty()); }

    // ── flatten — positional arrays (no match key) ────────────────────────────

    @Test
    public void flatPositionalPrimitiveArrayAccumulatesUnderSameKey() {
        Map<String, List<FieldValue>> result =
                JsonFieldExtractor.flatten("{\"tags\": [\"x\", \"y\"]}");
        assertEquals(1, result.size());
        // Both values under "tags", matchKey = null
        assertEquals(List.of("x", "y"), values(result, "tags"));
        assertTrue(keys(result, "tags").stream().allMatch(k -> k == null));
    }

    @Test
    public void flatPositionalArrayOrderIsInsensitive() {
        Map<String, List<FieldValue>> r1 =
                JsonFieldExtractor.flatten("{\"tags\": [\"y\", \"x\"]}");
        Map<String, List<FieldValue>> r2 =
                JsonFieldExtractor.flatten("{\"tags\": [\"x\", \"y\"]}");
        assertEquals(values(r1, "tags"), values(r2, "tags"));
    }

    @Test
    public void flatPositionalArrayOfObjectsGroupsFieldsUnderSameKey() {
        Map<String, List<FieldValue>> result = JsonFieldExtractor.flatten(
                "{\"terms\":[{\"code\":\"A\",\"msg\":\"hello\"},{\"code\":\"B\",\"msg\":\"world\"}]}");
        // No match key → positional; all values under "terms.code" / "terms.msg"
        assertTrue(values(result, "terms.code").containsAll(List.of("A", "B")));
        assertTrue(values(result, "terms.msg").containsAll(List.of("hello", "world")));
        // matchKey always null for positional
        assertTrue(keys(result, "terms.code").stream().allMatch(k -> k == null));
    }

    // ── flatten — key-based arrays ────────────────────────────────────────────

    @Test
    public void flatKeyedArrayTagsEachValueWithItsMatchKey() {
        Map<String, List<FieldValue>> result = JsonFieldExtractor.flatten(
                "{\"terms\":[{\"code\":\"A\",\"msg\":\"hello\"},{\"code\":\"B\",\"msg\":\"world\"}]}",
                Map.of("terms", "code"));

        // field_name stays plain — no index suffix
        List<FieldValue> codes = result.get("terms.code");
        List<FieldValue> msgs  = result.get("terms.msg");
        assertNotNull(codes);
        assertNotNull(msgs);
        assertEquals(2, codes.size());
        assertEquals(2, msgs.size());

        // array_key is the key field's value (e.g. "A", "B")
        assertEquals(List.of("A", "B"),       keys(result, "terms.code"));
        assertEquals(List.of("A", "B"),       values(result, "terms.code"));
        assertEquals(List.of("A", "B"),       keys(result, "terms.msg"));
        assertEquals(List.of("hello", "world"), values(result, "terms.msg"));
    }

    @Test
    public void flatKeyedArrayMissingKeyOnOneSideIsAbsentFromThatSidesMap() {
        // Human has A and B; AI has only A.
        // FlattenAndCompareFn will detect "terms.code"/"terms.msg" for key B
        // are absent in the AI map → null on AI side → mismatch.
        Map<String, List<FieldValue>> human = JsonFieldExtractor.flatten(
                "{\"terms\":[{\"code\":\"A\",\"msg\":\"hi\"},{\"code\":\"B\",\"msg\":\"bye\"}]}",
                Map.of("terms", "code"));
        Map<String, List<FieldValue>> ai = JsonFieldExtractor.flatten(
                "{\"terms\":[{\"code\":\"A\",\"msg\":\"hi\"}]}",
                Map.of("terms", "code"));

        // Human has both A and B
        assertEquals(List.of("A", "B"), keys(human, "terms.code"));
        // AI has only A
        assertEquals(List.of("A"), keys(ai, "terms.code"));

        // Build key→value maps to simulate FlattenAndCompareFn logic
        Map<String, String> humanByKey = human.get("terms.msg").stream()
                .collect(Collectors.toMap(fv -> fv.matchKey, fv -> fv.value));
        Map<String, String> aiByKey = ai.get("terms.msg").stream()
                .collect(Collectors.toMap(fv -> fv.matchKey, fv -> fv.value));

        assertEquals("hi",  humanByKey.get("A"));
        assertEquals("bye", humanByKey.get("B"));
        assertEquals("hi",  aiByKey.get("A"));
        assertNull(aiByKey.get("B")); // B absent in AI → mismatch
    }

    @Test
    public void flatCompositeKeyJoinsMultipleFieldsWithDash() {
        String json = "{\"tradeline\":[" +
                "{\"accountnumber\":\"ACC123\",\"customernumber\":\"CUST456\"," +
                " \"dateopened\":\"2020-01-01\",\"code\":\"TL01\",\"balance\":\"500\"}," +
                "{\"accountnumber\":\"ACC999\",\"customernumber\":\"CUST456\"," +
                " \"dateopened\":\"2021-06-15\",\"code\":\"TL02\",\"balance\":\"200\"}" +
                "]}";

        Map<String, List<FieldValue>> result = JsonFieldExtractor.flatten(json,
                Map.of("tradeline", "accountnumber-customernumber-dateopened-code"));

        // array_key is the composite of all four field values joined by "-", lower-cased
        assertEquals(List.of("acc123-cust456-2020-01-01-tl01",
                             "acc999-cust456-2021-06-15-tl02"),
                     keys(result, "tradeline.balance"));

        assertEquals(List.of("500", "200"), values(result, "tradeline.balance"));
    }

    @Test
    public void flatChildArrayInheritsParentKey() {
        // Only "tradeline" is in the map. "tradeline.paymentHistory" is NOT configured —
        // its elements should automatically inherit the parent tradeline key.
        String json = "{\"tradeline\":[{"
                + "\"accountnumber\":\"ACC1\",\"code\":\"TL01\","
                + "\"paymentHistory\":["
                + "  {\"date\":\"2024-01\",\"amount\":\"500\"},"
                + "  {\"date\":\"2024-02\",\"amount\":\"450\"}"
                + "]}]}";

        Map<String, List<FieldValue>> result = JsonFieldExtractor.flatten(
                json, Map.of("tradeline", "accountnumber-code"));

        // Top-level tradeline field carries the tradeline key
        assertEquals(List.of("acc1-tl01"), keys(result, "tradeline.accountnumber"));

        // Child paymentHistory fields inherit the parent tradeline key — both entries
        // share the same matchKey (the parent's composite key)
        List<String> phKeys = keys(result, "tradeline.paymentHistory.amount");
        assertEquals(2, phKeys.size());
        assertTrue("All child entries should carry parent key",
                phKeys.stream().allMatch(k -> "acc1-tl01".equals(k)));

        // Values are accumulated in sorted order (date ascending)
        List<String> amounts = values(result, "tradeline.paymentHistory.amount");
        assertEquals(2, amounts.size());
        assertTrue(amounts.containsAll(List.of("500", "450")));
    }

    @Test
    public void flatCompositeKeyCaseInsensitiveMatchesSameElement() {
        // Human has "ACC123", AI has "acc123" — should produce the same lower-cased key
        Map<String, List<FieldValue>> human = JsonFieldExtractor.flatten(
                "{\"tradeline\":[{\"accountnumber\":\"ACC123\",\"code\":\"TL01\",\"balance\":\"500\"}]}",
                Map.of("tradeline", "accountnumber-code"));
        Map<String, List<FieldValue>> ai = JsonFieldExtractor.flatten(
                "{\"tradeline\":[{\"accountnumber\":\"acc123\",\"code\":\"tl01\",\"balance\":\"500\"}]}",
                Map.of("tradeline", "accountnumber-code"));

        // Both produce the same lower-cased key → FlattenAndCompareFn sees a match
        assertEquals(keys(human, "tradeline.balance"), keys(ai, "tradeline.balance"));
        assertEquals(List.of("acc123-tl01"), keys(human, "tradeline.balance"));
    }

    @Test
    public void flatCompositeKeyMissingFieldUsesNullComponent() {
        // Element missing "code" — that component should be "null"
        String json = "{\"tradeline\":[" +
                "{\"accountnumber\":\"ACC123\",\"customernumber\":\"CUST1\",\"dateopened\":\"2020-01-01\"}]}";

        Map<String, List<FieldValue>> result = JsonFieldExtractor.flatten(json,
                Map.of("tradeline", "accountnumber-customernumber-dateopened-code"));

        assertEquals(List.of("acc123-cust1-2020-01-01-null"),
                     keys(result, "tradeline.accountnumber"));
    }

    @Test
    public void flatKeyedAndPositionalArraysCoexist() {
        String json = "{\"terms\":[{\"code\":\"X\",\"val\":\"1\"}],\"tags\":[\"a\",\"b\"]}";
        Map<String, List<FieldValue>> result =
                JsonFieldExtractor.flatten(json, Map.of("terms", "code"));

        // "terms" is key-based — array_key is the key field's value
        assertEquals(List.of("X"), keys(result, "terms.code"));
        assertEquals(List.of("X"), values(result, "terms.code"));

        // "tags" is positional
        assertTrue(keys(result, "tags").stream().allMatch(k -> k == null));
        assertEquals(List.of("a", "b"), values(result, "tags"));
    }

    @Test
    public void flatKeyedArrayKeyOnlyInAiShowsNullInHumanMap() {
        Map<String, List<FieldValue>> human = JsonFieldExtractor.flatten(
                "{\"terms\":[{\"code\":\"A\",\"msg\":\"hi\"}]}",
                Map.of("terms", "code"));
        Map<String, List<FieldValue>> ai = JsonFieldExtractor.flatten(
                "{\"terms\":[{\"code\":\"A\",\"msg\":\"hi\"},{\"code\":\"C\",\"msg\":\"new\"}]}",
                Map.of("terms", "code"));

        Map<String, String> humanByKey = human.get("terms.msg").stream()
                .collect(Collectors.toMap(fv -> fv.matchKey, fv -> fv.value));
        Map<String, String> aiByKey = ai.get("terms.msg").stream()
                .collect(Collectors.toMap(fv -> fv.matchKey, fv -> fv.value));

        assertNull(humanByKey.get("C")); // C absent in human → mismatch
        assertEquals("new", aiByKey.get("C"));
    }

    // ── extractField — plain dot-notation ─────────────────────────────────────

    @Test
    public void extractFieldRootLevel() {
        assertEquals("foo", JsonFieldExtractor.extractField("{\"name\":\"foo\"}", "name"));
    }

    @Test
    public void extractFieldNestedDotNotation() {
        assertEquals("img001", JsonFieldExtractor.extractField(
                "{\"metadata\":{\"image_name\":\"img001\"}}", "metadata.image_name"));
    }

    @Test
    public void extractFieldMissingKeyReturnsNull() {
        assertNull(JsonFieldExtractor.extractField("{\"a\":\"1\"}", "b"));
    }

    @Test
    public void extractFieldNullValueReturnsNull() {
        assertNull(JsonFieldExtractor.extractField("{\"name\":null}", "name"));
    }

    // ── extractField — array indexing ─────────────────────────────────────────

    @Test
    public void extractFieldArrayIndexFirstElement() {
        String json = "{\"queueImages\":[{\"fileName\":\"photo.jpg\"},{\"fileName\":\"thumb.jpg\"}]}";
        assertEquals("photo.jpg",
                JsonFieldExtractor.extractField(json, "queueImages[0].fileName"));
    }

    @Test
    public void extractFieldArrayIndexSecondElement() {
        String json = "{\"queueImages\":[{\"fileName\":\"photo.jpg\"},{\"fileName\":\"thumb.jpg\"}]}";
        assertEquals("thumb.jpg",
                JsonFieldExtractor.extractField(json, "queueImages[1].fileName"));
    }

    @Test
    public void extractFieldArrayIndexOutOfBoundsReturnsNull() {
        assertNull(JsonFieldExtractor.extractField(
                "{\"queueImages\":[{\"fileName\":\"photo.jpg\"}]}", "queueImages[5].fileName"));
    }

    @Test
    public void extractFieldArrayIndexWithNestedPath() {
        assertEquals("found", JsonFieldExtractor.extractField(
                "{\"a\":{\"items\":[{\"val\":\"found\"}]}}", "a.items[0].val"));
    }

    // ── extractField — null / blank / malformed ────────────────────────────────

    @Test public void extractFieldNullJsonReturnsNull()     { assertNull(JsonFieldExtractor.extractField(null,  "name")); }
    @Test public void extractFieldBlankJsonReturnsNull()    { assertNull(JsonFieldExtractor.extractField("  ", "name")); }
    @Test public void extractFieldMalformedJsonReturnsNull(){ assertNull(JsonFieldExtractor.extractField("{bad", "name")); }
}
