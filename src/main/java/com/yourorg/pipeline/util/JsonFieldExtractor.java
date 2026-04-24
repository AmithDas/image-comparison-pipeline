package com.yourorg.pipeline.util;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
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
 * dot-notation field paths to lists of string values.
 *
 * <h3>Scalar fields</h3>
 * Each scalar field maps to a single-element list:
 * <pre>
 *   {"a": "val"}        → {"a": ["val"]}
 *   {"a": {"b": "val"}} → {"a.b": ["val"]}
 *   {"a": null}         → {"a": [null]}
 * </pre>
 *
 * <h3>Array fields</h3>
 * Every element in an array contributes one entry to the list for that field,
 * preserving the field name without appending any index:
 * <pre>
 *   {"tags": ["x", "y"]}              → {"tags": ["x", "y"]}
 *   {"items": [{"code":"A"},{"code":"B"}]}
 *                                     → {"items.code": ["A", "B"]}
 *   {"items": [{"code":"A","msg":"hi"},
 *              {"code":"B","msg":"yo"}]}
 *                                     → {"items.code": ["A", "B"],
 *                                        "items.msg":  ["hi","yo"]}
 * </pre>
 *
 * Elements are sorted before accumulation (by a configured sort key or by
 * the full element JSON string) so that comparison is order-insensitive.
 *
 * <h3>Usage in FlattenAndCompareFn</h3>
 * {@link com.yourorg.pipeline.transforms.FlattenAndCompareFn} iterates the
 * lists positionally — element 0 of human vs element 0 of AI — and emits one
 * BigQuery row per element, all under the same {@code field_name}.  If one side
 * has more elements than the other the extra positions are treated as {@code null}.
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
                    int bracketClose = segment.indexOf(']', bracketOpen);
                    if (bracketClose < 0) {
                        LOG.warn("Malformed array index in segment '{}' of path '{}'",
                                segment, fieldPath);
                        return null;
                    }
                    String fieldName = segment.substring(0, bracketOpen);
                    int    index     = Integer.parseInt(
                            segment.substring(bracketOpen + 1, bracketClose));

                    if (!current.isJsonObject()) {
                        LOG.warn("Expected JSON object at '{}' in path '{}', found: {}",
                                fieldName, fieldPath, current.getClass().getSimpleName());
                        return null;
                    }
                    current = current.getAsJsonObject().get(fieldName);
                    if (current == null || current.isJsonNull()) return null;

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
    public static Map<String, List<String>> flatten(String json) {
        return flatten(json, Collections.emptyMap());
    }

    /**
     * Flattens a JSON string into a map of dot-notation paths → ordered lists of
     * string values.
     *
     * <p>Scalar fields produce a single-element list.  Array fields produce a
     * multi-element list — one entry per array element — all under the same key
     * (no index suffix).  Elements are sorted before accumulation.
     *
     * @param json          raw JSON string
     * @param arraySortKeys map of dot-notation array path → field name to sort elements by;
     *                      falls back to full element JSON string when the path is absent
     * @return ordered map of flattened field paths to value lists
     */
    public static Map<String, List<String>> flatten(String json, Map<String, String> arraySortKeys) {
        Map<String, List<String>> result = new LinkedHashMap<>();
        if (json == null || json.isBlank()) {
            return result;
        }
        try {
            JsonElement root = JsonParser.parseString(json);
            if (root.isJsonObject()) {
                flattenObject(root.getAsJsonObject(), "", result, arraySortKeys);
            } else {
                List<String> val = new ArrayList<>();
                val.add(root.isJsonNull() ? null : root.toString());
                result.put("$root", val);
            }
        } catch (Exception e) {
            LOG.warn("Failed to parse JSON payload, returning empty map. Error: {}", e.getMessage());
        }
        return result;
    }

    // ── private helpers ───────────────────────────────────────────────────────

    private static void flattenObject(JsonObject obj, String prefix,
                                      Map<String, List<String>> result,
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
                result.computeIfAbsent(key, k -> new ArrayList<>()).add(null);
            } else {
                result.computeIfAbsent(key, k -> new ArrayList<>()).add(val.getAsString());
            }
        }
    }

    /**
     * Flattens an array into {@code result}, appending each element's value(s) to the
     * existing list for {@code prefix} (or to sub-keys for object elements).
     *
     * <p>Elements are sorted before accumulation so that positional comparison in
     * {@code FlattenAndCompareFn} is order-insensitive.
     */
    private static void flattenArray(JsonArray arr, String prefix,
                                     Map<String, List<String>> result,
                                     Map<String, String> arraySortKeys) {
        List<JsonElement> elements = new ArrayList<>();
        arr.forEach(elements::add);

        // Sort by configured field for this path, or fall back to full element string.
        String sortField = arraySortKeys.get(prefix);
        elements.sort(Comparator.comparing(el -> sortValue(el, sortField)));

        for (JsonElement el : elements) {
            if (el.isJsonObject()) {
                // Flatten the object into a temporary map, then merge each key's values
                // into the main result so all elements share the same field key.
                Map<String, List<String>> temp = new LinkedHashMap<>();
                flattenObject(el.getAsJsonObject(), prefix, temp, arraySortKeys);
                temp.forEach((k, vals) ->
                        result.computeIfAbsent(k, x -> new ArrayList<>()).addAll(vals));
            } else if (el.isJsonArray()) {
                flattenArray(el.getAsJsonArray(), prefix, result, arraySortKeys);
            } else if (el.isJsonNull()) {
                result.computeIfAbsent(prefix, k -> new ArrayList<>()).add(null);
            } else {
                result.computeIfAbsent(prefix, k -> new ArrayList<>()).add(el.getAsString());
            }
        }
    }

    private static String sortValue(JsonElement el, String sortField) {
        if (sortField != null && el.isJsonObject()) {
            JsonElement field = el.getAsJsonObject().get(sortField);
            if (field != null && !field.isJsonNull()) return field.getAsString();
        }
        return el.toString();
    }
}
