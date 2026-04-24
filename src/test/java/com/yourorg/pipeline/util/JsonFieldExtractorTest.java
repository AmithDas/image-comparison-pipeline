package com.yourorg.pipeline.util;

import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class JsonFieldExtractorTest {

    // ── flatten — scalars ─────────────────────────────────────────────────────

    @Test
    public void flatFlattensTopLevelFields() {
        String json = "{\"a\": \"val1\", \"b\": \"val2\"}";
        Map<String, List<String>> result = JsonFieldExtractor.flatten(json);
        assertEquals(List.of("val1"), result.get("a"));
        assertEquals(List.of("val2"), result.get("b"));
        assertEquals(2, result.size());
    }

    @Test
    public void flatFlattensNestedFields() {
        String json = "{\"a\": {\"b\": {\"c\": \"deep\"}}}";
        Map<String, List<String>> result = JsonFieldExtractor.flatten(json);
        assertEquals(List.of("deep"), result.get("a.b.c"));
        assertEquals(1, result.size());
    }

    @Test
    public void flatHandlesNullValues() {
        String json = "{\"a\": null}";
        Map<String, List<String>> result = JsonFieldExtractor.flatten(json);
        assertTrue(result.containsKey("a"));
        assertEquals(1, result.get("a").size());
        assertNull(result.get("a").get(0));
    }

    @Test
    public void flatHandlesMixedNestedAndFlat() {
        String json = "{\"a\": \"flat\", \"b\": {\"c\": \"nested\"}, \"d\": null}";
        Map<String, List<String>> result = JsonFieldExtractor.flatten(json);
        assertEquals(List.of("flat"),   result.get("a"));
        assertEquals(List.of("nested"), result.get("b.c"));
        assertEquals(1, result.get("d").size());
        assertNull(result.get("d").get(0));
        assertEquals(3, result.size());
    }

    @Test
    public void flatReturnsEmptyMapForNull() {
        assertTrue(JsonFieldExtractor.flatten(null).isEmpty());
    }

    @Test
    public void flatReturnsEmptyMapForBlank() {
        assertTrue(JsonFieldExtractor.flatten("   ").isEmpty());
    }

    @Test
    public void flatReturnsEmptyMapForMalformedJson() {
        assertTrue(JsonFieldExtractor.flatten("{bad json").isEmpty());
    }

    // ── flatten — primitive arrays ────────────────────────────────────────────

    @Test
    public void flatPrimitiveArrayCollectsAllValuesUnderSameKey() {
        // Elements are sorted before accumulation
        String json = "{\"tags\": [\"x\", \"y\"]}";
        Map<String, List<String>> result = JsonFieldExtractor.flatten(json);
        assertEquals(1, result.size());
        assertEquals(List.of("x", "y"), result.get("tags"));
    }

    @Test
    public void flatArrayOrderIsInsensitive() {
        // Different input order → same sorted list
        Map<String, List<String>> r1 = JsonFieldExtractor.flatten("{\"tags\": [\"y\", \"x\"]}");
        Map<String, List<String>> r2 = JsonFieldExtractor.flatten("{\"tags\": [\"x\", \"y\"]}");
        assertEquals(r1.get("tags"), r2.get("tags"));
    }

    @Test
    public void flatSingleElementArrayProducesSingleElementList() {
        String json = "{\"tags\": [\"only\"]}";
        Map<String, List<String>> result = JsonFieldExtractor.flatten(json);
        assertEquals(List.of("only"), result.get("tags"));
    }

    @Test
    public void flatArrayNullElementIncludedAsList() {
        // null sorts before non-null strings (null → "" in sortValue)
        String json = "{\"items\": [\"present\", null]}";
        Map<String, List<String>> result = JsonFieldExtractor.flatten(json);
        List<String> vals = result.get("items");
        assertEquals(2, vals.size());
        assertTrue(vals.contains(null));
        assertTrue(vals.contains("present"));
    }

    // ── flatten — object arrays ───────────────────────────────────────────────

    @Test
    public void flatArrayOfObjectsGroupsFieldsUnderSameKey() {
        // All values for the same field across array elements land in one list
        String json = "{\"terms\": [{\"code\": \"A\", \"message\": \"hello\"},"
                + "{\"code\": \"B\", \"message\": \"world\"}]}";
        Map<String, List<String>> result = JsonFieldExtractor.flatten(json);
        // Sorted by full-element JSON: "A"-object sorts before "B"-object
        assertEquals(List.of("A", "B"),          result.get("terms.code"));
        assertEquals(List.of("hello", "world"),  result.get("terms.message"));
        assertEquals(2, result.size());
    }

    @Test
    public void flatArrayWithSortKeyUsesConfiguredField() {
        String json = "{\"items\": [{\"id\": \"z\", \"val\": \"1\"}, {\"id\": \"a\", \"val\": \"2\"}]}";
        Map<String, List<String>> result = JsonFieldExtractor.flatten(json, Map.of("items", "id"));
        // Sorted by "id": "a" first, "z" second
        assertEquals(List.of("a", "z"), result.get("items.id"));
        assertEquals(List.of("2", "1"), result.get("items.val"));
    }

    @Test
    public void flatNestedArrayCollectsUnderParentPrefix() {
        // Array of arrays: each inner array's elements land in the same outer list
        String json = "{\"matrix\": [[\"a\", \"b\"], [\"c\"]]}";
        Map<String, List<String>> result = JsonFieldExtractor.flatten(json);
        List<String> vals = result.get("matrix");
        assertNotNull(vals);
        assertEquals(3, vals.size());
        assertTrue(vals.containsAll(List.of("a", "b", "c")));
    }

    // ── flatten — comparison symmetry ────────────────────────────────────────

    @Test
    public void flatScalarAndArraySizeMatchForComparison() {
        // Human has a scalar, AI has the same value — lists are both size 1
        Map<String, List<String>> human = JsonFieldExtractor.flatten("{\"status\": \"ok\"}");
        Map<String, List<String>> ai    = JsonFieldExtractor.flatten("{\"status\": \"ok\"}");
        assertEquals(human.get("status"), ai.get("status"));
    }

    @Test
    public void flatArraySizesDifferWhenElementCountsDiffer() {
        // Different number of items — lists have different sizes
        Map<String, List<String>> human = JsonFieldExtractor.flatten("{\"tags\": [\"a\", \"b\"]}");
        Map<String, List<String>> ai    = JsonFieldExtractor.flatten("{\"tags\": [\"a\"]}");
        assertEquals(2, human.get("tags").size());
        assertEquals(1, ai.get("tags").size());
    }

    // ── extractField — plain dot-notation ─────────────────────────────────────

    @Test
    public void extractFieldRootLevel() {
        assertEquals("foo", JsonFieldExtractor.extractField("{\"name\":\"foo\"}", "name"));
    }

    @Test
    public void extractFieldNestedDotNotation() {
        String json = "{\"metadata\":{\"image_name\":\"img001\"}}";
        assertEquals("img001", JsonFieldExtractor.extractField(json, "metadata.image_name"));
    }

    @Test
    public void extractFieldThreeLevelsDeep() {
        String json = "{\"a\":{\"b\":{\"c\":\"deep\"}}}";
        assertEquals("deep", JsonFieldExtractor.extractField(json, "a.b.c"));
    }

    @Test
    public void extractFieldMissingKeyReturnsNull() {
        assertNull(JsonFieldExtractor.extractField("{\"a\":\"1\"}", "b"));
    }

    @Test
    public void extractFieldMissingNestedKeyReturnsNull() {
        assertNull(JsonFieldExtractor.extractField("{\"a\":{\"b\":\"1\"}}", "a.c"));
    }

    @Test
    public void extractFieldNullValueReturnsNull() {
        assertNull(JsonFieldExtractor.extractField("{\"name\":null}", "name"));
    }

    // ── extractField — array indexing ─────────────────────────────────────────

    @Test
    public void extractFieldArrayIndexFirstElement() {
        String json = "{\"queueImages\":[{\"fileName\":\"photo.jpg\",\"size\":1024},"
                + "{\"fileName\":\"thumb.jpg\",\"size\":256}]}";
        assertEquals("photo.jpg",
                JsonFieldExtractor.extractField(json, "queueImages[0].fileName"));
    }

    @Test
    public void extractFieldArrayIndexSecondElement() {
        String json = "{\"queueImages\":[{\"fileName\":\"photo.jpg\"},"
                + "{\"fileName\":\"thumb.jpg\"}]}";
        assertEquals("thumb.jpg",
                JsonFieldExtractor.extractField(json, "queueImages[1].fileName"));
    }

    @Test
    public void extractFieldArrayIndexOutOfBoundsReturnsNull() {
        String json = "{\"queueImages\":[{\"fileName\":\"photo.jpg\"}]}";
        assertNull(JsonFieldExtractor.extractField(json, "queueImages[5].fileName"));
    }

    @Test
    public void extractFieldArrayMissingFieldAfterIndexReturnsNull() {
        String json = "{\"queueImages\":[{\"fileName\":\"photo.jpg\"}]}";
        assertNull(JsonFieldExtractor.extractField(json, "queueImages[0].nonExistent"));
    }

    @Test
    public void extractFieldArrayIndexWithNestedPath() {
        String json = "{\"a\":{\"items\":[{\"val\":\"found\"}]}}";
        assertEquals("found",
                JsonFieldExtractor.extractField(json, "a.items[0].val"));
    }

    // ── extractField — null / blank / malformed inputs ─────────────────────────

    @Test
    public void extractFieldNullJsonReturnsNull() {
        assertNull(JsonFieldExtractor.extractField(null, "name"));
    }

    @Test
    public void extractFieldBlankJsonReturnsNull() {
        assertNull(JsonFieldExtractor.extractField("  ", "name"));
    }

    @Test
    public void extractFieldMalformedJsonReturnsNull() {
        assertNull(JsonFieldExtractor.extractField("{bad", "name"));
    }
}
