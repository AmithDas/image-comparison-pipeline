package com.yourorg.pipeline.util;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
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
 *
 * Array elements are sorted before accumulation to make comparison order-insensitive.
 * The sort field per array path can be configured via the arraySortKeys map (dot-notation
 * path → field name). Falls back to full element JSON string sort when no key is configured.
 */
public final class JsonFieldExtractor {

    private static final Logger LOG = LoggerFactory.getLogger(JsonFieldExtractor.class);

    private JsonFieldExtractor() {}

    /**
     * Extracts a single string field from a JSON document using a dot-notation path
     * with optional array indexing.
     *
     * <h3>Supported syntax</h3>
     * <pre>
     *   "name"                       → root field "name"
     *   "metadata.imageName"         → root.metadata.imageName
     *   "queueImages[0].fileName"    → root.queueImages (array) → element 0 → fileName
     *   "a.b[2].c"                   → root.a.b (array) → element 2 → c
     * </pre>
     *
     * <ul>
     *   <li>Segments are split on {@code .}.</li>
     *   <li>A segment of the form {@code field[n]} navigates into the named field
     *       (which must be a JSON array) and then selects the element at index {@code n}.</li>
     * </ul>
     *
     * Returns {@code null} if the input is null/blank/malformed, any segment is absent
     * or resolves to a JSON null, an array index is out of bounds, or a type mismatch
     * occurs (e.g. expected object, found array).
     */
    public static String extractField(String json, String fieldPath) {
        if (json == null || json.isBlank()) return null;
        try {
            JsonElement current = JsonParser.parseString(json);

            for (String segment : fieldPath.split("\\.")) {
                int bracketOpen = segment.indexOf('[');
                if (bracketOpen >= 0) {
                    // Segment like "queueImages[0]" — object field then array index.
                    int bracketClose = segment.indexOf(']', bracketOpen);
                    if (bracketClose < 0) {
                        LOG.warn("Malformed array index in segment '{}' of path '{}'",
                                segment, fieldPath);
                        return null;
                    }
                    String fieldName = segment.substring(0, bracketOpen);
                    int    index     = Integer.parseInt(
                            segment.substring(bracketOpen + 1, bracketClose));

                    // Step into the named field.
                    if (!current.isJsonObject()) {
                        LOG.warn("Expected JSON object at '{}' in path '{}', found: {}",
                                fieldName, fieldPath, current.getClass().getSimpleName());
                        return null;
                    }
                    current = current.getAsJsonObject().get(fieldName);
                    if (current == null || current.isJsonNull()) return null;

                    // Step into the array at the given index.
                    if (!current.isJsonArray()) {
                        LOG.warn("Expected JSON array for '{}' in path '{}', found: {}",
                                fieldName, fieldPath, current.getClass().getSimpleName());
                        return null;
                    }
                    JsonArray arr = current.getAsJsonArray();
                    if (index < 0 || index >= arr.size()) {
                        LOG.warn("Array index {} out of bounds (size={}) in path '{}'",
                                index, arr.size(), fieldPath);
                        return null;
                    }
                    current = arr.get(index);
                    if (current == null || current.isJsonNull()) return null;

                } else {
                    // Plain field navigation.
                    if (!current.isJsonObject()) {
                        LOG.warn("Expected JSON object at segment '{}' in path '{}', found: {}",
                                segment, fieldPath, current.getClass().getSimpleName());
                        return null;
                    }
                    current = current.getAsJsonObject().get(segment);
                    if (current == null || current.isJsonNull()) return null;
                }
            }

            return current.isJsonPrimitive() ? current.getAsString() : current.toString();

        } catch (Exception e) {
            LOG.warn("Failed to extract field '{}' from JSON. Error: {}", fieldPath, e.getMessage());
        }
        return null;
    }

    /**
     * Flattens a JSON string using default sort (full element JSON string).
     */
    public static Map<String, String> flatten(String json) {
        return flatten(json, Collections.emptyMap());
    }

    /**
     * Flattens a JSON string into a map of dot-notation paths → string values.
     *
     * @param json          raw JSON string
     * @param arraySortKeys map of dot-notation array path → field name to sort elements by
     * @return ordered map of flattened field paths to string values
     */
    public static Map<String, String> flatten(String json, Map<String, String> arraySortKeys) {
        Map<String, String> result = new LinkedHashMap<>();
        if (json == null || json.isBlank()) {
            return result;
        }
        try {
            JsonElement root = JsonParser.parseString(json);
            if (root.isJsonObject()) {
                flattenObject(root.getAsJsonObject(), "", result, arraySortKeys);
            } else {
                result.put("$root", root.isJsonNull() ? null : root.toString());
            }
        } catch (Exception e) {
            LOG.warn("Failed to parse JSON payload, returning empty map. Error: {}", e.getMessage());
        }
        return result;
    }

    private static void flattenObject(JsonObject obj, String prefix,
                                      Map<String, String> result,
                                      Map<String, String> arraySortKeys) {
        for (Map.Entry<String, JsonElement> entry : obj.entrySet()) {
            String key = prefix.isEmpty()
                    ? entry.getKey()
                    : prefix + "." + entry.getKey();
            JsonElement val = entry.getValue();

            if (val.isJsonObject()) {
                flattenObject(val.getAsJsonObject(), key, result, arraySortKeys);
            } else if (val.isJsonArray()) {
                flattenArray(val.getAsJsonArray(), key, result, arraySortKeys);
            } else if (val.isJsonNull()) {
                result.put(key, null);
            } else {
                result.put(key, val.getAsString());
            }
        }
    }

    private static void flattenArray(JsonArray arr, String prefix,
                                     Map<String, String> result,
                                     Map<String, String> arraySortKeys) {
        List<JsonElement> elements = new ArrayList<>();
        arr.forEach(elements::add);

        // Sort by configured field for this path, or fall back to full element string.
        String sortField = arraySortKeys.get(prefix);
        elements.sort(Comparator.comparing(el -> sortValue(el, sortField)));

        // Flatten each element into a temp map, then merge into an accumulator.
        // Keys that appear in multiple elements collect all values into a JSON array.
        Map<String, List<String>> accumulated = new LinkedHashMap<>();
        for (JsonElement el : elements) {
            Map<String, String> elResult = new LinkedHashMap<>();
            if (el.isJsonObject()) {
                flattenObject(el.getAsJsonObject(), prefix, elResult, arraySortKeys);
            } else if (el.isJsonArray()) {
                flattenArray(el.getAsJsonArray(), prefix, elResult, arraySortKeys);
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

    private static String sortValue(JsonElement el, String sortField) {
        if (sortField != null && el.isJsonObject()) {
            JsonElement field = el.getAsJsonObject().get(sortField);
            if (field != null && !field.isJsonNull()) return field.getAsString();
        }
        return el.toString();
    }
}
