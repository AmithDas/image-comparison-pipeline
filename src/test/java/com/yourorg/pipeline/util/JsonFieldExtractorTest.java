package com.yourorg.pipeline.util;

import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.*;

public class JsonFieldExtractorTest {

    // ── flatten ───────────────────────────────────────────────────────────────

    @Test
    public void flatFlattensTopLevelFields() {
        String json = "{\"a\": \"val1\", \"b\": \"val2\"}";
        Map<String, String> result = JsonFieldExtractor.flatten(json);
        assertEquals("val1", result.get("a"));
        assertEquals("val2", result.get("b"));
        assertEquals(2, result.size());
    }

    @Test
    public void flatFlattensNestedFields() {
        String json = "{\"a\": {\"b\": {\"c\": \"deep\"}}}";
        Map<String, String> result = JsonFieldExtractor.flatten(json);
        assertEquals("deep", result.get("a.b.c"));
        assertEquals(1, result.size());
    }

    @Test
    public void flatHandlesNullValues() {
        String json = "{\"a\": null}";
        Map<String, String> result = JsonFieldExtractor.flatten(json);
        assertTrue(result.containsKey("a"));
        assertNull(result.get("a"));
    }

    @Test
    public void flatPrimitiveArrayEmitsIndexedKeys() {
        String json = "{\"tags\": [\"x\", \"y\"]}";
        Map<String, String> result = JsonFieldExtractor.flatten(json);
        // Each element gets its own indexed key
        assertEquals("x", result.get("tags[0]"));
        assertEquals("y", result.get("tags[1]"));
        assertEquals(2, result.size());
    }

    @Test
    public void flatArrayOrderIsInsensitive() {
        // Elements are sorted before indexing, so the same logical set maps to the
        // same indexed keys regardless of the original order.
        Map<String, String> r1 = JsonFieldExtractor.flatten("{\"tags\": [\"y\", \"x\"]}");
        Map<String, String> r2 = JsonFieldExtractor.flatten("{\"tags\": [\"x\", \"y\"]}");
        assertEquals(r1.get("tags[0]"), r2.get("tags[0]"));
        assertEquals(r1.get("tags[1]"), r2.get("tags[1]"));
    }

    @Test
    public void flatArrayOfObjectsEmitsIndexedDotKeys() {
        String json = "{\"terms\": [{\"code\": \"A\", \"message\": \"hello\"},"
                + "{\"code\": \"B\", \"message\": \"world\"}]}";
        Map<String, String> result = JsonFieldExtractor.flatten(json);
        // Sorted by full-element JSON string: "A" object sorts before "B"
        assertEquals("A",     result.get("terms[0].code"));
        assertEquals("hello", result.get("terms[0].message"));
        assertEquals("B",     result.get("terms[1].code"));
        assertEquals("world", result.get("terms[1].message"));
        assertEquals(4, result.size());
    }

    @Test
    public void flatSingleElementArrayEmitsSingleIndexedKey() {
        String json = "{\"tags\": [\"only\"]}";
        Map<String, String> result = JsonFieldExtractor.flatten(json);
        assertEquals("only", result.get("tags[0]"));
        assertEquals(1, result.size());
    }

    @Test
    public void flatArrayNullElementEmitsIndexedNullKey() {
        String json = "{\"items\": [null, \"present\"]}";
        Map<String, String> result = JsonFieldExtractor.flatten(json);
        // null sorts before "present" as a string
        assertNull(result.get("items[0]"));
        assertEquals("present", result.get("items[1]"));
    }

    @Test
    public void flatNestedArrayEmitsDoublyIndexedKeys() {
        // Array of arrays
        String json = "{\"matrix\": [[\"a\", \"b\"], [\"c\"]]}";
        Map<String, String> result = JsonFieldExtractor.flatten(json);
        // Outer sorted by full JSON; inner sorted too
        assertTrue(result.containsKey("matrix[0][0]"));
        assertTrue(result.containsKey("matrix[0][1]"));
        assertTrue(result.containsKey("matrix[1][0]"));
    }

    @Test
    public void flatArrayWithSortKeyUsesConfiguredField() {
        String json = "{\"items\": [{\"id\": \"z\", \"val\": 1}, {\"id\": \"a\", \"val\": 2}]}";
        Map<String, String> result = JsonFieldExtractor.flatten(json, Map.of("items", "id"));
        // Sorted by "id": "a" object first, "z" object second
        assertEquals("a", result.get("items[0].id"));
        assertEquals("z", result.get("items[1].id"));
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

    @Test
    public void flatHandlesMixedNestedAndFlat() {
        String json = "{\"a\": \"flat\", \"b\": {\"c\": \"nested\"}, \"d\": null}";
        Map<String, String> result = JsonFieldExtractor.flatten(json);
        assertEquals("flat",   result.get("a"));
        assertEquals("nested", result.get("b.c"));
        assertNull(result.get("d"));
        assertEquals(3, result.size());
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
        // Canonical configured path: queueImages[0].fileName
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
