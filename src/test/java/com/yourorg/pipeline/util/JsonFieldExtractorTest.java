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
    public void flatHandlesArrayValues() {
        String json = "{\"tags\": [\"x\", \"y\"]}";
        Map<String, String> result = JsonFieldExtractor.flatten(json);
        assertEquals("[\"x\",\"y\"]", result.get("tags"));
    }

    @Test
    public void flatArrayOrderIsInsensitive() {
        Map<String, String> r1 = JsonFieldExtractor.flatten("{\"tags\": [\"y\", \"x\"]}");
        Map<String, String> r2 = JsonFieldExtractor.flatten("{\"tags\": [\"x\", \"y\"]}");
        assertEquals(r1.get("tags"), r2.get("tags"));
    }

    @Test
    public void flatArrayOfObjectsMergesFieldsUnderParentKey() {
        String json = "{\"terms\": [{\"code\": \"A\", \"message\": \"hello\"},"
                + "{\"code\": \"B\", \"message\": \"world\"}]}";
        Map<String, String> result = JsonFieldExtractor.flatten(json);
        assertTrue(result.containsKey("terms.code"));
        assertTrue(result.containsKey("terms.message"));
    }

    @Test
    public void flatSingleElementArrayStoredAsScalar() {
        String json = "{\"tags\": [\"only\"]}";
        Map<String, String> result = JsonFieldExtractor.flatten(json);
        assertEquals("only", result.get("tags"));
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
