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
 * dot-notation field paths to lists of {@link FieldValue} entries.
 *
 * <h3>Scalar fields</h3>
 * Each scalar produces a single-element list with a {@code null} match key:
 * <pre>
 *   {"a": "val"}        → {"a": [FieldValue(key=null, value="val")]}
 *   {"a": {"b": "val"}} → {"a.b": [FieldValue(key=null, value="val")]}
 * </pre>
 *
 * <h3>Arrays — key-based matching</h3>
 * When a match key is configured for an array path via {@code arrayMatchKeys},
 * every field inside each element is stored with the element's key value as
 * the {@code matchKey}.  The field path stays plain — no index suffix:
 * <pre>
 *   arrayMatchKeys = {"terms": "code"}
 *   {"terms":[{"code":"A","msg":"hi"},{"code":"B","msg":"bye"}]}
 *     → {"terms.code": [FieldValue(key="A", value="A"),
 *                        FieldValue(key="B", value="B")],
 *        "terms.msg":  [FieldValue(key="A", value="hi"),
 *                        FieldValue(key="B", value="bye")]}
 * </pre>
 * {@code FlattenAndCompareFn} groups entries by {@code matchKey} and compares
 * human vs AI values for the same key.  A key present only on one side produces
 * a mismatch row with a {@code null} value on the other side.
 *
 * <h3>Arrays — positional matching (fallback)</h3>
 * When no match key is configured, elements are sorted by their full JSON string
 * and accumulated positionally into the list (all with {@code matchKey = null}).
 * {@code FlattenAndCompareFn} compares element 0 vs 0, element 1 vs 1, etc.,
 * emitting one BQ row per position under the same {@code field_name}.
 */
public final class JsonFieldExtractor {

    private static final Logger LOG = LoggerFactory.getLogger(JsonFieldExtractor.class);

    private JsonFieldExtractor() {}

    // ─────────────────────────────────────────────────────────────────────────

    /**
     * A single flattened value together with its array match key.
     *
     * <ul>
     *   <li>{@code matchKey} is {@code null} for scalars and positional-array elements.</li>
     *   <li>{@code matchKey} is the configured key field's value for keyed-array elements
     *       (e.g. {@code "A"} when the element's {@code code} field equals {@code "A"}).</li>
     *   <li>{@code value} is the field's string value and may itself be {@code null}
     *       when the JSON value is {@code null}.</li>
     * </ul>
     */
    public static final class FieldValue {
        public final String matchKey;
        public final String value;

        public FieldValue(String matchKey, String value) {
            this.matchKey = matchKey;
            this.value    = value;
        }

        @Override
        public String toString() {
            return "FieldValue{matchKey=" + matchKey + ", value=" + value + "}";
        }
    }

    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Extracts a single string field from a JSON document using a dot-notation path
     * with optional array indexing.
     *
     * <h3>Supported syntax</h3>
     * <pre>
     *   "name"                       → root field "name"
     *   "metadata.imageName"         → root.metadata.imageName
     *   "queueImages[0].fileName"    → root.queueImages → element 0 → fileName
     *   "a.b[2].c"                   → root.a.b → element 2 → c
     * </pre>
     *
     * Returns {@code null} if the input is null/blank/malformed, any segment is absent,
     * resolves to JSON null, an array index is out of bounds, or a type mismatch occurs.
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

    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Flattens a JSON string using positional comparison for all arrays
     * (no match keys configured).
     */
    public static Map<String, List<FieldValue>> flatten(String json) {
        return flatten(json, Collections.emptyMap());
    }

    /**
     * Flattens a JSON string into a map of dot-notation paths → lists of
     * {@link FieldValue} entries.
     *
     * @param json           raw JSON string
     * @param arrayMatchKeys map of dot-notation array path → field name to use as the
     *                       match key for that array section.  Arrays not listed here
     *                       fall back to positional comparison.
     * @return ordered map of flattened paths to {@link FieldValue} lists
     */
    public static Map<String, List<FieldValue>> flatten(String json,
                                                        Map<String, String> arrayMatchKeys) {
        Map<String, List<FieldValue>> result = new LinkedHashMap<>();
        if (json == null || json.isBlank()) return result;
        try {
            JsonElement root = JsonParser.parseString(json);
            if (root.isJsonObject()) {
                flattenObject(root.getAsJsonObject(), "", null, result, arrayMatchKeys);
            } else {
                List<FieldValue> list = new ArrayList<>();
                list.add(new FieldValue(null, root.isJsonNull() ? null : root.toString()));
                result.put("$root", list);
            }
        } catch (Exception e) {
            LOG.warn("Failed to parse JSON payload, returning empty map. Error: {}", e.getMessage());
        }
        return result;
    }

    // ── private helpers ───────────────────────────────────────────────────────

    /**
     * Recursively flattens a JSON object.
     *
     * @param inheritedMatchKey the match key propagated from the nearest enclosing
     *                          keyed-array element ({@code null} at the root or inside
     *                          a positional array)
     */
    private static void flattenObject(JsonObject obj, String prefix,
                                      String inheritedMatchKey,
                                      Map<String, List<FieldValue>> result,
                                      Map<String, String> arrayMatchKeys) {
        for (Map.Entry<String, JsonElement> entry : obj.entrySet()) {
            String      key = prefix.isEmpty() ? entry.getKey() : prefix + "." + entry.getKey();
            JsonElement val = entry.getValue();

            if (val.isJsonObject()) {
                flattenObject(val.getAsJsonObject(), key, inheritedMatchKey, result, arrayMatchKeys);
            } else if (val.isJsonArray()) {
                flattenArray(val.getAsJsonArray(), key, inheritedMatchKey, result, arrayMatchKeys);
            } else if (val.isJsonNull()) {
                result.computeIfAbsent(key, k -> new ArrayList<>())
                      .add(new FieldValue(inheritedMatchKey, null));
            } else {
                result.computeIfAbsent(key, k -> new ArrayList<>())
                      .add(new FieldValue(inheritedMatchKey, val.getAsString()));
            }
        }
    }

    /**
     * Flattens an array field.
     *
     * <p>If a match key is configured for {@code prefix} in {@code arrayMatchKeys},
     * each element is tagged with its own key value (key-based mode).  Otherwise
     * elements are sorted by full JSON string and tagged with {@code inheritedMatchKey}
     * (positional mode).
     */
    private static void flattenArray(JsonArray arr, String prefix,
                                     String inheritedMatchKey,
                                     Map<String, List<FieldValue>> result,
                                     Map<String, String> arrayMatchKeys) {
        List<JsonElement> elements = new ArrayList<>();
        arr.forEach(elements::add);

        String matchKeyField = arrayMatchKeys.get(prefix);

        if (matchKeyField != null) {
            // ── Key-based ────────────────────────────────────────────────────
            // Each element is tagged with its key value; the field path stays
            // plain (terms.code, not terms[A].code).
            // If a parent key was inherited, prepend it so the child key is
            // fully qualified (e.g. parent="sdf" + child="1" → "sdf-1").
            for (JsonElement el : elements) {
                String keyValue = extractKeyValue(el, matchKeyField, prefix);
                if (inheritedMatchKey != null) {
                    keyValue = inheritedMatchKey + "-" + keyValue;
                }
                if (el.isJsonObject()) {
                    flattenObject(el.getAsJsonObject(), prefix, keyValue, result, arrayMatchKeys);
                } else {
                    result.computeIfAbsent(prefix, k -> new ArrayList<>())
                          .add(new FieldValue(keyValue, el.isJsonNull() ? null : el.getAsString()));
                }
            }

        } else {
            // ── Positional (fallback) ────────────────────────────────────────
            // Sort elements by full JSON string for order-insensitive comparison,
            // then accumulate into per-field lists with the inherited match key.
            elements.sort(Comparator.comparing(JsonElement::toString));

            for (JsonElement el : elements) {
                if (el.isJsonObject()) {
                    Map<String, List<FieldValue>> temp = new LinkedHashMap<>();
                    flattenObject(el.getAsJsonObject(), prefix, inheritedMatchKey,
                                  temp, arrayMatchKeys);
                    temp.forEach((k, vals) ->
                            result.computeIfAbsent(k, x -> new ArrayList<>()).addAll(vals));
                } else if (el.isJsonArray()) {
                    flattenArray(el.getAsJsonArray(), prefix, inheritedMatchKey,
                                 result, arrayMatchKeys);
                } else if (el.isJsonNull()) {
                    result.computeIfAbsent(prefix, k -> new ArrayList<>())
                          .add(new FieldValue(inheritedMatchKey, null));
                } else {
                    result.computeIfAbsent(prefix, k -> new ArrayList<>())
                          .add(new FieldValue(inheritedMatchKey, el.getAsString()));
                }
            }
        }
    }

    /**
     * Derives the match-key value for an array element from a key specification.
     *
     * <h3>Key specification format</h3>
     * <ul>
     *   <li>Components are separated by {@code -}.</li>
     *   <li>Each component may be a simple field name <em>or</em> a dot-notation path
     *       that navigates into nested objects/arrays.  When a path segment resolves to
     *       an array, the <b>first element</b> of that array is used automatically.</li>
     * </ul>
     *
     * <pre>
     *   keySpec = "code"
     *       → element.code                                 e.g. "T1"
     *
     *   keySpec = "accountnumber-customernumber-dateopened-code"
     *       → element.accountnumber + "-" + element.customernumber + ...
     *                                                      e.g. "acc123-cust1-2020-01-01-tl01"
     *
     *   keySpec = "customerNumber-disputeCodes.code"
     *       → element.customerNumber + "-" + element.disputeCodes[0].code
     *                                                      e.g. "234-234"
     * </pre>
     *
     * <p>Missing or null components are substituted with the literal {@code "null"} so
     * the key always has a defined, consistent value on both sides.
     * The final key is lower-cased for case-insensitive matching.
     */
    private static String extractKeyValue(JsonElement el, String keySpec, String arrayPath) {
        String[] components = keySpec.split("-");
        StringBuilder composedKey = new StringBuilder();

        for (int i = 0; i < components.length; i++) {
            if (i > 0) composedKey.append("-");
            String component = components[i].trim();
            String value     = extractComponentValue(el, component, keySpec, arrayPath);
            composedKey.append(value != null ? value : "null");
        }

        // Lower-case for case-insensitive key matching.
        return composedKey.toString().toLowerCase();
    }

    /**
     * Extracts a single key component value by navigating a dot-notation path.
     *
     * <p>At each dot-segment:
     * <ol>
     *   <li>If the current element is an object, navigate into the named field.</li>
     *   <li>If the current element is an array, take its first element and then
     *       navigate into the named field of that element.</li>
     * </ol>
     *
     * <p>Returns {@code null} if the path cannot be resolved (missing field, empty
     * array, or type mismatch).
     */
    private static String extractComponentValue(JsonElement el, String dotPath,
                                                String keySpec, String arrayPath) {
        JsonElement current = el;

        for (String segment : dotPath.split("\\.")) {
            if (current == null || current.isJsonNull()) return null;

            // If we land on an array, take its first element before navigating further.
            if (current.isJsonArray()) {
                JsonArray arr = current.getAsJsonArray();
                if (arr.isEmpty()) {
                    LOG.warn("Array '{}': empty array at segment '{}' in key spec '{}'",
                            arrayPath, segment, keySpec);
                    return null;
                }
                current = arr.get(0);
                if (current == null || current.isJsonNull()) return null;
            }

            if (!current.isJsonObject()) {
                LOG.warn("Array '{}': expected object at segment '{}' in key spec '{}', found {}",
                        arrayPath, segment, keySpec, current.getClass().getSimpleName());
                return null;
            }

            current = current.getAsJsonObject().get(segment);
        }

        if (current == null || current.isJsonNull()) return null;

        // If the resolved value is still an array (e.g. path ended at an array field),
        // take the first element.
        if (current.isJsonArray()) {
            JsonArray arr = current.getAsJsonArray();
            if (arr.isEmpty()) return null;
            current = arr.get(0);
        }

        if (current == null || current.isJsonNull()) return null;
        return current.isJsonPrimitive() ? current.getAsString() : current.toString();
    }
}
