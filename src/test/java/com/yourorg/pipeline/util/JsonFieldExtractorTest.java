package com.yourorg.pipeline.util;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class JsonFieldExtractorTest {

    @Test
    void flatFlattensTopLevelFields() {
        String json = "{\"a\": \"val1\", \"b\": \"val2\"}";
        Map<String, String> result = JsonFieldExtractor.flatten(json);
        assertEquals("val1", result.get("a"));
        assertEquals("val2", result.get("b"));
        assertEquals(2, result.size());
    }

    @Test
    void flatFlattensNestedFields() {
        String json = "{\"a\": {\"b\": {\"c\": \"deep\"}}}";
        Map<String, String> result = JsonFieldExtractor.flatten(json);
        assertEquals("deep", result.get("a.b.c"));
        assertEquals(1, result.size());
    }

    @Test
    void flatHandlesNullValues() {
        String json = "{\"a\": null}";
        Map<String, String> result = JsonFieldExtractor.flatten(json);
        assertTrue(result.containsKey("a"));
        assertNull(result.get("a"));
    }

    @Test
    void flatHandlesArrayValues() {
        String json = "{\"tags\": [\"x\", \"y\"]}";
        Map<String, String> result = JsonFieldExtractor.flatten(json);
        // Multiple array elements are collected into a JSON array string under the parent key.
        // Elements are sorted, so order is deterministic regardless of source order.
        assertEquals("[\"x\",\"y\"]", result.get("tags"));
    }

    @Test
    void flatArrayOrderIsInsensitive() {
        Map<String, String> r1 = JsonFieldExtractor.flatten("{\"tags\": [\"y\", \"x\"]}");
        Map<String, String> r2 = JsonFieldExtractor.flatten("{\"tags\": [\"x\", \"y\"]}");
        assertEquals(r1.get("tags"), r2.get("tags"));
    }

    @Test
    void flatArrayOfObjectsMergesFieldsUnderParentKey() {
        String json = "{\"terms\": [{\"code\": \"A\", \"message\": \"hello\"},"
                + "{\"code\": \"B\", \"message\": \"world\"}]}";
        Map<String, String> result = JsonFieldExtractor.flatten(json);
        // Both objects contribute to terms.code and terms.message;
        // values collected into JSON arrays (sorted).
        assertTrue(result.containsKey("terms.code"));
        assertTrue(result.containsKey("terms.message"));
    }

    @Test
    void flatSingleElementArrayStoredAsScalar() {
        String json = "{\"tags\": [\"only\"]}";
        Map<String, String> result = JsonFieldExtractor.flatten(json);
        // Single element → stored as plain string, not wrapped in an array
        assertEquals("only", result.get("tags"));
    }

    @Test
    void flatReturnsEmptyMapForNull() {
        assertTrue(JsonFieldExtractor.flatten(null).isEmpty());
    }

    @Test
    void flatReturnsEmptyMapForBlank() {
        assertTrue(JsonFieldExtractor.flatten("   ").isEmpty());
    }

    @Test
    void flatReturnsEmptyMapForMalformedJson() {
        assertTrue(JsonFieldExtractor.flatten("{bad json").isEmpty());
    }

    @Test
    void flatHandlesMixedNestedAndFlat() {
        String json = "{\"a\": \"flat\", \"b\": {\"c\": \"nested\"}, \"d\": null}";
        Map<String, String> result = JsonFieldExtractor.flatten(json);
        assertEquals("flat",   result.get("a"));
        assertEquals("nested", result.get("b.c"));
        assertNull(result.get("d"));
        assertEquals(3, result.size());
    }
}
