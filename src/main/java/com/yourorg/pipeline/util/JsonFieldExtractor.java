package com.yourorg.pipeline.util;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Utility that recursively flattens a JSON string into a map of
 * dot-notation field paths to string values.
 *
 * Examples:
 *   {"a": "val"}              → {"a": "val"}
 *   {"a": {"b": "val"}}       → {"a.b": "val"}
 *   {"a": [1, 2]}             → {"a": "[\"1\",\"2\"]"}  (multiple values collected into JSON array)
 *   {"a": [{"b": 1}]}         → {"a.b": "1"}
 *   {"a": [{"b":1},{"b":2}]}  → {"a.b": "[\"1\",\"2\"]"}
 *   {"a": null}               → {"a": null}
 */
public final class JsonFieldExtractor {

    private static final Logger LOG = LoggerFactory.getLogger(JsonFieldExtractor.class);

    private JsonFieldExtractor() {}

    /**
     * Extracts a single top-level string field from a JSON object.
     * Returns null if the input is null, blank, malformed, or the field is absent.
     */
    public static String extractField(String json, String fieldName) {
        if (json == null || json.isBlank()) return null;
        try {
            JsonElement root = JsonParser.parseString(json);
            if (root.isJsonObject()) {
                JsonElement el = root.getAsJsonObject().get(fieldName);
                if (el != null && !el.isJsonNull()) return el.getAsString();
            }
        } catch (Exception e) {
            LOG.warn("Failed to extract field '{}' from JSON. Error: {}", fieldName, e.getMessage());
        }
        return null;
    }

    /**
     * Flattens a JSON string into a map of dot-notation paths → string values.
     * Returns an empty map if the input is null, blank, or malformed.
     *
     * @param json raw JSON string
     * @return ordered map of flattened field paths to string values
     */
    public static Map<String, String> flatten(String json) {
        Map<String, String> result = new LinkedHashMap<>();
        if (json == null || json.isBlank()) {
            return result;
        }
        try {
            JsonElement root = JsonParser.parseString(json);
            if (root.isJsonObject()) {
                flattenObject(root.getAsJsonObject(), "", result);
            } else {
                // Top-level is a scalar or array — store under empty key
                result.put("$root", root.isJsonNull() ? null : root.toString());
            }
        } catch (Exception e) {
            LOG.warn("Failed to parse JSON payload, returning empty map. Error: {}", e.getMessage());
        }
        return result;
    }

    private static void flattenObject(JsonObject obj, String prefix,
                                      Map<String, String> result) {
        for (Map.Entry<String, JsonElement> entry : obj.entrySet()) {
            String key = prefix.isEmpty()
                    ? entry.getKey()
                    : prefix + "." + entry.getKey();
            JsonElement val = entry.getValue();

            if (val.isJsonObject()) {
                flattenObject(val.getAsJsonObject(), key, result);
            } else if (val.isJsonArray()) {
                flattenArray(val.getAsJsonArray(), key, result);
            } else if (val.isJsonNull()) {
                result.put(key, null);
            } else {
                result.put(key, val.getAsString());
            }
        }
    }

    private static void flattenArray(JsonArray arr, String prefix,
                                     Map<String, String> result) {
        // Sort by JSON string so [1,2] and [2,1] produce identical keys (order-insensitive).
        List<JsonElement> elements = new ArrayList<>();
        arr.forEach(elements::add);
        elements.sort(Comparator.comparing(JsonElement::toString));

        // Flatten each element into a temp map, then merge into an accumulator.
        // Keys that appear in multiple elements collect all values into a JSON array.
        Map<String, List<String>> accumulated = new LinkedHashMap<>();
        for (JsonElement el : elements) {
            Map<String, String> elResult = new LinkedHashMap<>();
            if (el.isJsonObject()) {
                flattenObject(el.getAsJsonObject(), prefix, elResult);
            } else if (el.isJsonArray()) {
                flattenArray(el.getAsJsonArray(), prefix, elResult);
            } else if (el.isJsonNull()) {
                elResult.put(prefix, null);
            } else {
                elResult.put(prefix, el.getAsString());
            }
            elResult.forEach((k, v) ->
                    accumulated.computeIfAbsent(k, x -> new ArrayList<>()).add(v));
        }

        // Write to result: single value as-is; multiple values as a JSON array string.
        accumulated.forEach((k, values) -> {
            if (values.size() == 1) {
                result.put(k, values.get(0));
            } else {
                JsonArray jsonArr = new JsonArray();
                values.forEach(v -> {
                    if (v == null) jsonArr.add(JsonNull.INSTANCE);
                    else jsonArr.add(v);
                });
                result.put(k, jsonArr.toString());
            }
        });
    }
}
