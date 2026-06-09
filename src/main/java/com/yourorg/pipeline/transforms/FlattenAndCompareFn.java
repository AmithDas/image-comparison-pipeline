package com.yourorg.pipeline.transforms;

import com.google.api.services.bigquery.model.TableRow;
import com.yourorg.pipeline.config.SegmentConfig;
import com.yourorg.pipeline.util.BarricadeEncryptionUtil;
import com.yourorg.pipeline.util.JsonFieldExtractor;
import com.yourorg.pipeline.util.JsonFieldExtractor.FieldValue;
import com.yourorg.pipeline.util.TimestampUtil;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Flattens both JSON payloads in a matched pair and emits one {@link TableRow}
 * per field value, including a {@code segment} column for downstream filtering.
 *
 * <h3>Pair key format</h3>
 * {@code "imageId::segment::iteration"} — all three parts are embedded in the
 * pair key set by {@link FilterAndPairFn}.
 *
 * <h3>Array comparison</h3>
 * <ul>
 *   <li><b>Keyed arrays</b> ({@code ARRAY_MATCH_KEYS}): matched by composite key.</li>
 *   <li><b>Positional arrays</b>: compared position-by-position.</li>
 *   <li><b>Scalar fields</b>: one row, {@code array_key} is {@code null}.</li>
 * </ul>
 *
 * <h3>AI field aliases</h3>
 * Per-segment {@code aiFieldAliases} in {@link SegmentConfig} rename AI payload
 * fields (dot-notation prefix) to the corresponding human field name before
 * comparison.  For example, {@code "documentType" → "document"} causes the AI
 * field {@code documentType} (and any nested path like {@code documentType.code})
 * to be compared against the human field {@code document}.
 *
 * <h3>String comparison</h3>
 * {@code is_match} is computed on plaintext (case-insensitive).
 * Values are re-encrypted before writing to BigQuery.
 */
public class FlattenAndCompareFn
        extends DoFn<KV<String, KV<GenericRecord, GenericRecord>>, TableRow> {

    private static final Logger LOG = LoggerFactory.getLogger(FlattenAndCompareFn.class);

    // ── Array match keys ──────────────────────────────────────────────────────

    static final Map<String, String> ARRAY_MATCH_KEYS = Map.of(
            "documentDetails",                               "docType",
            "docProofs",                                     "document",
            "tradelines",                                    "accountnumber-customernumber-dateopened",
            "tradelines.tradelineRequest.disputeCodes",      "code",
            "credit",                                        "customerNumber",
            "credit.dob.disputeCodes",                       "code"
            // "address",                                    "addresstype"
    );

    // ── Soft-match arrays ─────────────────────────────────────────────────────
    // Maps array path → parent array path.
    //
    // Arrays listed here use exact-first, positional-fallback matching WITHIN
    // each parent group:
    //   1. Items whose keys exist on both sides are paired as exact matches.
    //   2. Remaining unmatched items (per parent group, sorted by key) are paired
    //      positionally — so a human code 007 will pair with AI code 005 when
    //      no exact match exists, rather than producing two orphan rows.
    //   3. Any surplus items become orphan rows (null on the other side) as usual.
    //
    // Each key must also appear in ARRAY_MATCH_KEYS.
    // Value is the parent keyed array path, or "" when the dispute-code array has
    // no keyed parent (items are positional at the root level).
    static final Map<String, String> SOFT_MATCH_ARRAYS = Map.of(
            "tradelines.tradelineRequest.disputeCodes", "tradelines",
            "credit.dob.disputeCodes",                 "credit"
    );

    // ── Constructor ───────────────────────────────────────────────────────────

    private final ValueProvider<String> firestoreCollection;
    private final ValueProvider<String> kmsKeyPath;
    private final ValueProvider<String> segmentConfigsJson;

    // segment name → (aiField → humanField) from fieldMappings; resolved in @Setup
    private transient Map<String, Map<String, String>> aiToHumanBySegment;
    // segment name → (root node → segment_type label); resolved in @Setup
    private transient Map<String, Map<String, String>> rootToLabelBySegment;

    public FlattenAndCompareFn(ValueProvider<String> firestoreCollection,
                                ValueProvider<String> kmsKeyPath,
                                ValueProvider<String> segmentConfigsJson) {
        this.firestoreCollection = firestoreCollection;
        this.kmsKeyPath          = kmsKeyPath;
        this.segmentConfigsJson  = segmentConfigsJson;
    }

    @Setup
    public void setup() {
        BarricadeEncryptionUtil.configure(
                firestoreCollection.get(),
                kmsKeyPath.get());

        aiToHumanBySegment   = new HashMap<>();
        rootToLabelBySegment = new HashMap<>();
        List<SegmentConfig> segments = SegmentConfig.parse(segmentConfigsJson.get());
        for (SegmentConfig seg : segments) {
            if (seg.fieldMappings != null) {
                Map<String, String> aiToHuman = new HashMap<>();
                for (SegmentConfig.FieldMapping fm : seg.fieldMappings) {
                    if (fm.aiField != null && fm.humanField != null) {
                        aiToHuman.put(fm.aiField, fm.humanField);
                    }
                }
                if (!aiToHuman.isEmpty()) aiToHumanBySegment.put(seg.name, aiToHuman);
            }
            if (seg.segmentTypeMappings != null) {
                Map<String, String> rootToLabel = new HashMap<>();
                for (SegmentConfig.SegmentTypeMapping stm : seg.segmentTypeMappings) {
                    if (stm.roots != null && stm.segmentType != null) {
                        for (String root : stm.roots) {
                            rootToLabel.put(root, stm.segmentType);
                        }
                    }
                }
                if (!rootToLabel.isEmpty()) rootToLabelBySegment.put(seg.name, rootToLabel);
            }
        }
    }

    // ── Processing ────────────────────────────────────────────────────────────

    @ProcessElement
    public void processElement(ProcessContext ctx) {
        // Pair key format: "imageId::segment::iteration"
        String pairKey = ctx.element().getKey();
        String[] parts = pairKey.split("::", 3);

        if (parts.length != 3) {
            LOG.warn("Unexpected pairKey format: '{}' — skipping", pairKey);
            return;
        }

        String imageId  = parts[0];
        String segment  = parts[1];
        int    iteration;
        try {
            iteration = Integer.parseInt(parts[2]);
        } catch (NumberFormatException e) {
            LOG.warn("Could not parse iteration from pairKey '{}' — skipping", pairKey);
            return;
        }

        GenericRecord human = ctx.element().getValue().getKey();
        GenericRecord ai    = ctx.element().getValue().getValue();

        String humanKeyId = str(human.get("key_id"));
        String aiKeyId    = str(ai.get("key_id"));
        if (!nullSafeEquals(humanKeyId, aiKeyId)) {
            LOG.warn("imageId='{}' segment='{}' mismatched key_ids (human={}, ai={}) — using human",
                    imageId, segment, humanKeyId, aiKeyId);
        }
        String keyId = humanKeyId;

        String humanPayload = BarricadeEncryptionUtil.decrypt(keyId, str(human.get("payload")));
        String aiPayload    = BarricadeEncryptionUtil.decrypt(keyId, str(ai.get("payload")));

        Map<String, List<FieldValue>> humanFields =
                JsonFieldExtractor.flatten(humanPayload, ARRAY_MATCH_KEYS);
        Map<String, List<FieldValue>> aiFields =
                applyFieldMappings(JsonFieldExtractor.flatten(aiPayload, ARRAY_MATCH_KEYS), segment);

        // Soft-match: remap unmatched items on both sides to a combined key
        // "{parent}-{aiCode}/{humanCode}" so they compare as a pair instead of
        // two orphans, and array_key in the output shows both code values.
        Map<String, String>[] softRemaps = computeSoftMatchRemaps(humanFields, aiFields);
        if (!softRemaps[0].isEmpty()) humanFields = applyKeyRemap(humanFields, softRemaps[0]);
        if (!softRemaps[1].isEmpty()) aiFields    = applyKeyRemap(aiFields,    softRemaps[1]);

        if (humanFields.isEmpty() && aiFields.isEmpty()) {
            LOG.warn("Both payloads empty for imageId='{}' segment='{}' — skipping",
                    imageId, segment);
            return;
        }

        Set<String> allFields = new TreeSet<>();
        allFields.addAll(humanFields.keySet());
        allFields.addAll(aiFields.keySet());

        String aiCreatedAt    = TimestampUtil.normalizeTimestamp(str(ai.get("created_at")));
        String humanCreatedAt = TimestampUtil.normalizeTimestamp(str(human.get("created_at")));
        String comparedAt     = TimestampUtil.formatInstant(Instant.now());

        int rowsEmitted = 0;

        for (String field : allFields) {
            List<FieldValue> humanEntries =
                    humanFields.getOrDefault(field, Collections.emptyList());
            List<FieldValue> aiEntries =
                    aiFields.getOrDefault(field, Collections.emptyList());

            boolean keyed = humanEntries.stream().anyMatch(fv -> fv.matchKey != null)
                         || aiEntries.stream().anyMatch(fv -> fv.matchKey != null);

            if (keyed) {
                Map<String, List<String>> humanGroups = groupByKey(humanEntries);
                Map<String, List<String>> aiGroups    = groupByKey(aiEntries);
                Set<String> allMatchKeys = new TreeSet<>();
                allMatchKeys.addAll(humanGroups.keySet());
                allMatchKeys.addAll(aiGroups.keySet());

                for (String matchKey : allMatchKeys) {
                    List<String> humanVals =
                            humanGroups.getOrDefault(matchKey, Collections.emptyList());
                    List<String> aiVals =
                            aiGroups.getOrDefault(matchKey, Collections.emptyList());
                    int count = Math.max(humanVals.size(), aiVals.size());
                    for (int i = 0; i < count; i++) {
                        String humanVal = i < humanVals.size() ? humanVals.get(i) : null;
                        String aiVal    = i < aiVals.size()    ? aiVals.get(i)    : null;
                        emitRow(ctx, imageId, segment, keyId, iteration,
                                aiCreatedAt, humanCreatedAt, comparedAt,
                                field, matchKey, humanVal, aiVal);
                        rowsEmitted++;
                    }
                }
            } else {
                int count = Math.max(humanEntries.size(), aiEntries.size());
                for (int i = 0; i < count; i++) {
                    String humanVal = i < humanEntries.size() ? humanEntries.get(i).value : null;
                    String aiVal    = i < aiEntries.size()    ? aiEntries.get(i).value    : null;
                    emitRow(ctx, imageId, segment, keyId, iteration,
                            aiCreatedAt, humanCreatedAt, comparedAt,
                            field, null, humanVal, aiVal);
                    rowsEmitted++;
                }
            }
        }

        LOG.info("Compared imageId='{}' segment='{}' iteration={} — {} rows ({} fields)",
                imageId, segment, iteration, rowsEmitted, allFields.size());
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    /**
     * Renames AI field keys using the segment's fieldMappings so that
     * cross-named root objects (e.g. AI "consumer" ↔ human "verifiedData")
     * are compared against each other.
     *
     * Handles both exact matches ("consumer") and dot-prefixed children
     * ("consumer.firstName", "consumer.dob", …). The human field name is
     * used as the canonical key so field_name in the output matches the
     * human side.
     */
    private Map<String, List<FieldValue>> applyFieldMappings(
            Map<String, List<FieldValue>> aiFields, String segment) {
        Map<String, String> aiToHuman = aiToHumanBySegment.get(segment);
        if (aiToHuman == null || aiToHuman.isEmpty()) return aiFields;

        Map<String, List<FieldValue>> result = new LinkedHashMap<>();
        for (Map.Entry<String, List<FieldValue>> entry : aiFields.entrySet()) {
            String key         = entry.getKey();
            String renamedKey  = rename(key, aiToHuman);
            result.merge(renamedKey, entry.getValue(), (existing, incoming) -> {
                List<FieldValue> merged = new ArrayList<>(existing);
                merged.addAll(incoming);
                return merged;
            });
        }
        return result;
    }

    /**
     * Returns the segment_type for a flattened field, applying any configured
     * root-node label overrides for the current segment.
     *
     * Raw root is:
     *   "employments.test.test", null  → "employments"
     *   "documentDetails",       "A"   → "documentDetails"  (keyed array)
     *   "name",                  null  → null               (root scalar)
     *
     * If the raw root matches a segmentTypeMappings entry, the configured label
     * is returned instead (e.g. "verifiedData" → "authentication").
     */
    private static String resolveSegmentType(String field, String arrayKey,
                                              Map<String, String> rootToLabel) {
        int dot = field.indexOf('.');
        String root;
        if (dot > 0) {
            root = field.substring(0, dot);
        } else if (arrayKey != null) {
            root = field;
        } else {
            return null;
        }
        return rootToLabel.getOrDefault(root, root);
    }

    private static String rename(String key, Map<String, String> aiToHuman) {
        for (Map.Entry<String, String> mapping : aiToHuman.entrySet()) {
            String aiPrefix    = mapping.getKey();
            String humanPrefix = mapping.getValue();
            if (key.equals(aiPrefix)) {
                return humanPrefix;
            }
            if (key.startsWith(aiPrefix + ".")) {
                return humanPrefix + key.substring(aiPrefix.length());
            }
        }
        return key;
    }

    /**
     * Computes soft-match key remappings for both human and AI sides.
     *
     * <p>For each array in {@link #SOFT_MATCH_ARRAYS}, unmatched items on each side
     * are paired positionally within their parent group. Both sides are remapped to a
     * combined key {@code "{parent}-{aiCode}/{humanCode}"} so the {@code array_key}
     * in the output shows both code values, making it clear the row is a fallback-paired
     * comparison rather than an exact key match.
     *
     * <p>Returns a two-element array: {@code [humanRemap, aiRemap]}.
     */
    @SuppressWarnings("unchecked")
    private static Map<String, String>[] computeSoftMatchRemaps(
            Map<String, List<FieldValue>> humanFields,
            Map<String, List<FieldValue>> aiFields) {

        Map<String, String> humanRemap = new HashMap<>();
        Map<String, String> aiRemap    = new HashMap<>();

        for (Map.Entry<String, String> e : SOFT_MATCH_ARRAYS.entrySet()) {
            String arrayPath  = e.getKey();
            String parentPath = e.getValue();

            Set<String> humanArrayKeys = collectMatchKeys(humanFields, arrayPath);
            Set<String> aiArrayKeys    = collectMatchKeys(aiFields,    arrayPath);

            if (parentPath.isEmpty()) {
                // No keyed parent — treat all items as one group.
                processGroup("", new ArrayList<>(humanArrayKeys), new ArrayList<>(aiArrayKeys),
                             humanRemap, aiRemap);
            } else {
                // Group by parent key to prevent cross-group pairing.
                Stream.concat(
                        collectMatchKeys(humanFields, parentPath).stream(),
                        collectMatchKeys(aiFields,    parentPath).stream())
                    .collect(Collectors.toCollection(LinkedHashSet::new))
                    .forEach(parentKey -> {
                        String childPrefix = parentKey + "-";
                        List<String> humanChildKeys = humanArrayKeys.stream()
                                .filter(k -> k.startsWith(childPrefix))
                                .collect(Collectors.toList());
                        List<String> aiChildKeys = aiArrayKeys.stream()
                                .filter(k -> k.startsWith(childPrefix))
                                .collect(Collectors.toList());
                        processGroup(parentKey, humanChildKeys, aiChildKeys, humanRemap, aiRemap);
                    });
            }
        }

        return new Map[]{humanRemap, aiRemap};
    }

    /**
     * Applies exact-first, positional-fallback matching for one group of child keys.
     * Sorting is done here so callers can pass keys in any order.
     *
     * <ul>
     *   <li>Exact matches  → remapped to {@code "{parent}-{code}-{code}"}</li>
     *   <li>Fallback pairs → remapped to {@code "{parent}-{aiCode}-{humanCode}"}</li>
     *   <li>Surplus AI     → remapped to {@code "{parent}-{aiCode}-null"}</li>
     *   <li>Surplus human  → remapped to {@code "{parent}-null-{humanCode}"}</li>
     * </ul>
     */
    private static void processGroup(String parentKey,
                                      List<String> humanKeys,
                                      List<String> aiKeys,
                                      Map<String, String> humanRemap,
                                      Map<String, String> aiRemap) {
        List<String> sortedHuman = humanKeys.stream().sorted().collect(Collectors.toList());
        List<String> sortedAi    = aiKeys.stream().sorted().collect(Collectors.toList());

        Set<String> exact = sortedHuman.stream()
                .filter(new HashSet<>(sortedAi)::contains)
                .collect(Collectors.toSet());
        exact.forEach(k -> {
            String combined = combinedKey(parentKey, k, k);
            aiRemap.put(k,    combined);
            humanRemap.put(k, combined);
        });

        List<String> unmatchedHuman = sortedHuman.stream()
                .filter(k -> !exact.contains(k)).collect(Collectors.toList());
        List<String> unmatchedAi   = sortedAi.stream()
                .filter(k -> !exact.contains(k)).collect(Collectors.toList());

        int pairs = Math.min(unmatchedHuman.size(), unmatchedAi.size());
        IntStream.range(0, pairs).forEach(i -> {
            String aiKey    = unmatchedAi.get(i);
            String humanKey = unmatchedHuman.get(i);
            String combined = combinedKey(parentKey, aiKey, humanKey);
            aiRemap.put(aiKey,    combined);
            humanRemap.put(humanKey, combined);
        });

        unmatchedAi.stream().skip(pairs)
                .forEach(aiKey -> aiRemap.put(aiKey, combinedKey(parentKey, aiKey, null)));
        unmatchedHuman.stream().skip(pairs)
                .forEach(humanKey -> humanRemap.put(humanKey, combinedKey(parentKey, null, humanKey)));
    }

    /**
     * Builds a combined array key: {@code "{parent}-{aiCode}-{humanCode}"}.
     * When {@code parentKey} is empty (no keyed parent), only the code pair is returned.
     * Passing {@code null} for either key substitutes {@code "null"}.
     *
     * <p>Examples:
     * <ul>
     *   <li>{@code ("acc-cust-date", "acc-cust-date-005", "acc-cust-date-007")} → {@code "acc-cust-date-005-007"}</li>
     *   <li>{@code ("", "001", "002")}                                          → {@code "001-002"}</li>
     *   <li>{@code ("acc-cust-date", "acc-cust-date-005", null)}                → {@code "acc-cust-date-005-null"}</li>
     *   <li>{@code ("", null, "006")}                                           → {@code "null-006"}</li>
     * </ul>
     */
    private static String combinedKey(String parentKey, String aiKey, String humanKey) {
        String aiCode    = aiKey    != null ? lastSegment(aiKey)    : "null";
        String humanCode = humanKey != null ? lastSegment(humanKey) : "null";
        String codePart  = aiCode + "-" + humanCode;
        return parentKey.isEmpty() ? codePart : parentKey + "-" + codePart;
    }

    /** Extracts the portion after the last {@code -}, or the whole string if no {@code -} present. */
    private static String lastSegment(String key) {
        int dash = key.lastIndexOf('-');
        return dash >= 0 ? key.substring(dash + 1) : key;
    }

    /**
     * Returns the set of distinct non-null match keys used by all {@link FieldValue}
     * entries under {@code arrayPath} (exact path or any dot-prefixed child).
     */
    private static Set<String> collectMatchKeys(Map<String, List<FieldValue>> fields,
                                                 String arrayPath) {
        String prefix = arrayPath + ".";
        return fields.entrySet().stream()
                .filter(e -> e.getKey().equals(arrayPath) || e.getKey().startsWith(prefix))
                .flatMap(e -> e.getValue().stream())
                .filter(fv -> fv.matchKey != null)
                .map(fv -> fv.matchKey)
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    /**
     * Returns a new field map with match keys remapped according to {@code keyRemap}.
     * Fields whose match keys are not in the map are left unchanged.
     */
    private static Map<String, List<FieldValue>> applyKeyRemap(
            Map<String, List<FieldValue>> fields,
            Map<String, String> keyRemap) {
        return fields.entrySet().stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        e -> e.getValue().stream()
                                .map(fv -> new FieldValue(
                                        fv.matchKey != null
                                                ? keyRemap.getOrDefault(fv.matchKey, fv.matchKey)
                                                : null,
                                        fv.value))
                                .collect(Collectors.toList()),
                        (a, b) -> a,
                        LinkedHashMap::new));
    }

    private void emitRow(ProcessContext ctx,
                         String imageId, String segment, String keyId, int iteration,
                         String aiCreatedAt, String humanCreatedAt, String comparedAt,
                         String field, String arrayKey,
                         String humanVal, String aiVal) {

        final boolean isMatch = humanVal == null ? aiVal == null : humanVal.equalsIgnoreCase(aiVal);
        String encryptedHumanVal = BarricadeEncryptionUtil.encrypt(keyId, humanVal);
        String encryptedAiVal    = BarricadeEncryptionUtil.encrypt(keyId, aiVal);

        Map<String, String> rootToLabel = rootToLabelBySegment.getOrDefault(
                segment, Collections.emptyMap());

        ctx.output(new TableRow()
                .set("image_id",         imageId)
                .set("key_id",           keyId)
                .set("segment",          segment)
                .set("ai_iteration",     iteration)
                .set("ai_created_at",    aiCreatedAt)
                .set("human_created_at", humanCreatedAt)
                .set("field_name",       field)
                .set("array_key",        arrayKey)
                .set("segment_type",     resolveSegmentType(field, arrayKey, rootToLabel))
                .set("human_value",      encryptedHumanVal)
                .set("ai_value",         encryptedAiVal)
                .set("is_match",         isMatch)
                .set("compared_at",      comparedAt));
    }

    private static Map<String, List<String>> groupByKey(List<FieldValue> entries) {
        Map<String, List<String>> groups = new LinkedHashMap<>();
        for (FieldValue fv : entries) {
            if (fv.matchKey != null) {
                groups.computeIfAbsent(fv.matchKey, k -> new ArrayList<>()).add(fv.value);
            }
        }
        return groups;
    }

    private static boolean nullSafeEquals(String a, String b) {
        return a == null ? b == null : a.equals(b);
    }

    private static String str(Object value) {
        return value != null ? value.toString() : null;
    }
}
