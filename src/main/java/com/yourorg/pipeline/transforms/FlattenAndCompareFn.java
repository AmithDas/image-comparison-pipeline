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
import org.apache.beam.sdk.values.PCollectionView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

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
            "documentDetails", "docType",
            "docProofs",       "document"
            // "tradeline", "accountnumber-customernumber-dateopened-code",
            // "address",   "addresstype"
    );

    // ── Dispute code normalisation ────────────────────────────────────────────
    // Each entry is {arrayField, codeField} — the array whose match keys carry
    // dispute codes and the field within each element that holds the code value.
    // Add a new row here when a new dispute-code array is introduced.

    static final String[][] CODE_NORM_ENTRIES = {
            {"disputes", "disputeCode"},
            // {"otherArray", "otherCodeField"},
    };

    // Fully-qualified field paths for which is_match uses base-code comparison.
    static final Set<String> CODE_FIELD_PATHS;
    static {
        Set<String> s = new java.util.HashSet<>();
        for (String[] e : CODE_NORM_ENTRIES) s.add(e[0] + "." + e[1]);
        CODE_FIELD_PATHS = java.util.Collections.unmodifiableSet(s);
    }

    // CODE_GROUPINGS is loaded from BQ at worker startup — see @Setup.
    // Schema: variant_code STRING, base_code STRING.

    // ── Constructor ───────────────────────────────────────────────────────────

    private final ValueProvider<String>              firestoreCollection;
    private final ValueProvider<String>              kmsKeyPath;
    private final ValueProvider<String>              segmentConfigsJson;
    private final PCollectionView<Map<String, String>> codeGroupingsView;

    // segment name → (aiField → humanField) from fieldMappings; resolved in @Setup
    private transient Map<String, Map<String, String>> aiToHumanBySegment;
    // segment name → (root node → segment_type label); resolved in @Setup
    private transient Map<String, Map<String, String>> rootToLabelBySegment;
    // segment name → (aiFieldPrefix → humanFieldPrefix) from aiFieldAliases; resolved in @Setup
    private transient Map<String, Map<String, String>> fieldAliasMap;

    public FlattenAndCompareFn(ValueProvider<String> firestoreCollection,
                                ValueProvider<String> kmsKeyPath,
                                ValueProvider<String> segmentConfigsJson,
                                PCollectionView<Map<String, String>> codeGroupingsView) {
        this.firestoreCollection = firestoreCollection;
        this.kmsKeyPath          = kmsKeyPath;
        this.segmentConfigsJson  = segmentConfigsJson;
        this.codeGroupingsView   = codeGroupingsView;
    }

    @Setup
    public void setup() {
        BarricadeEncryptionUtil.configure(
                firestoreCollection.get(),
                kmsKeyPath.get());

        aiToHumanBySegment   = new HashMap<>();
        rootToLabelBySegment = new HashMap<>();
        fieldAliasMap        = new HashMap<>();
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
            if (seg.aiFieldAliases != null && !seg.aiFieldAliases.isEmpty()) {
                fieldAliasMap.put(seg.name, seg.aiFieldAliases);
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

        Map<String, String> aiToHuman = aiToHumanBySegment.getOrDefault(segment, Collections.emptyMap());

        // Build match key maps: start from the static base and add the code-normalisation
        // arrays from CODE_NORM_ENTRIES so they are keyed without touching ARRAY_MATCH_KEYS.
        Map<String, String> humanMatchKeys = new HashMap<>(ARRAY_MATCH_KEYS);
        for (String[] entry : CODE_NORM_ENTRIES) {
            humanMatchKeys.put(entry[0], entry[1]);
        }
        Map<String, String> aiMatchKeys = buildAiArrayMatchKeys(humanMatchKeys, aiToHuman);

        Map<String, List<FieldValue>> humanFields =
                JsonFieldExtractor.flatten(humanPayload, humanMatchKeys);
        Map<String, List<FieldValue>> aiFields =
                applyFieldMappings(JsonFieldExtractor.flatten(aiPayload, aiMatchKeys), segment);

        // Apply per-segment AI field aliases (e.g. documentType → document).
        Map<String, String> aliases = fieldAliasMap.getOrDefault(segment, Collections.emptyMap());
        if (!aliases.isEmpty()) {
            aiFields = applyAliases(aiFields, aliases);
        }

        // Normalise dispute-code match keys and values.
        // The side input is broadcast once per job by Beam and cached in worker memory —
        // ctx.sideInput() does not trigger a remote read per element.
        Map<String, String> codeGroupings = ctx.sideInput(codeGroupingsView);
        for (String[] entry : CODE_NORM_ENTRIES) {
            humanFields = applyCodeNormalization(humanFields, entry[0], entry[1], codeGroupings);
            aiFields    = applyCodeNormalization(aiFields,    entry[0], entry[1], codeGroupings);
        }

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
     * Derives an AI-side array match key map from the human-side base map,
     * accounting for both fieldMappings (aiToHuman) and aiFieldAliases.
     *
     * <p>For each entry in {@code base} (humanArrayRoot → humanMatchKeyField), if an
     * alias renames that root on the AI side (e.g. AI uses "documentType" where human
     * uses "document"), the result also contains the AI root so the extractor finds the
     * correct match-key field in the AI payload.
     *
     * <p>{@code aiToHuman} maps AI field prefix → human field prefix (from fieldMappings).
     * {@code aliases} maps AI field prefix → human field prefix (from aiFieldAliases).
     * Both are applied independently.
     */
    private static Map<String, String> buildAiArrayMatchKeys(Map<String, String> base,
                                                              Map<String, String> aiToHuman) {
        if (aiToHuman.isEmpty()) return base;
        Map<String, String> result = new HashMap<>(base);
        for (Map.Entry<String, String> entry : base.entrySet()) {
            String arrayPath     = entry.getKey();
            String humanKeyField = entry.getValue();
            String humanFullPath = arrayPath + "." + humanKeyField;
            for (Map.Entry<String, String> mapping : aiToHuman.entrySet()) {
                if (humanFullPath.equals(mapping.getValue())) {
                    String aiFullPath = mapping.getKey();
                    String prefix     = arrayPath + ".";
                    if (aiFullPath.startsWith(prefix)) {
                        result.put(arrayPath, aiFullPath.substring(prefix.length()));
                    }
                    break;
                }
            }
        }
        return result;
    }

    /**
     * Returns a new map with AI field keys renamed according to {@code aliases}.
     * Each alias entry maps an AI field prefix (dot-notation) to the human field prefix
     * that should be used as the canonical key in output rows.
     *
     * <p>Both exact matches ({@code "documentType"}) and dot-prefixed children
     * ({@code "documentType.code"}) are handled. Entries with no matching alias pass
     * through unchanged.
     */
    private static Map<String, List<FieldValue>> applyAliases(
            Map<String, List<FieldValue>> aiFields, Map<String, String> aliases) {
        if (aliases == null || aliases.isEmpty()) return aiFields;
        Map<String, List<FieldValue>> result = new LinkedHashMap<>();
        for (Map.Entry<String, List<FieldValue>> entry : aiFields.entrySet()) {
            String renamed = renameWithAliases(entry.getKey(), aliases);
            result.merge(renamed, entry.getValue(), (existing, incoming) -> {
                List<FieldValue> merged = new ArrayList<>(existing);
                merged.addAll(incoming);
                return merged;
            });
        }
        return result;
    }

    private static String renameWithAliases(String key, Map<String, String> aliases) {
        for (Map.Entry<String, String> alias : aliases.entrySet()) {
            String aiPrefix    = alias.getKey();
            String humanPrefix = alias.getValue();
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
     * Normalises match keys and code-field values for a single
     * {@link SegmentConfig.CodeNormalizationConfig}.
     *
     * <p>For every {@link FieldValue} whose field path belongs to {@code cfg.arrayField}:
     * <ul>
     *   <li>The match key is normalised: the last {@code -}-delimited segment is replaced
     *       with the base code from {@code cfg.codeGroupings}
     *       (e.g. {@code "abc-xyz-001"} → {@code "abc-xyz-005"}).</li>
     *   <li>For the code field itself ({@code arrayField.codeField}), the value is
     *       rewritten to {@code "originalCode/baseCode"}
     *       (e.g. {@code "001"} → {@code "001/005"}).</li>
     * </ul>
     * Fields outside the target array are passed through unchanged.
     */
    private static Map<String, List<FieldValue>> applyCodeNormalization(
            Map<String, List<FieldValue>> fields,
            String arrayField,
            String codeField,
            Map<String, String> codeGroupings) {
        if (codeGroupings.isEmpty()) return fields;

        String codeFieldPath = arrayField + "." + codeField;
        Map<String, List<FieldValue>> result = new LinkedHashMap<>();

        for (Map.Entry<String, List<FieldValue>> entry : fields.entrySet()) {
            String            fieldName = entry.getKey();
            List<FieldValue>  values    = entry.getValue();

            boolean inArray   = fieldName.equals(arrayField)
                                || fieldName.startsWith(arrayField + ".");
            boolean isCodeFld = fieldName.equals(codeFieldPath);

            if (!inArray) {
                result.put(fieldName, values);
                continue;
            }

            List<FieldValue> normalised = new ArrayList<>();
            for (FieldValue fv : values) {
                String normKey = normalizeCodeKey(fv.matchKey, codeGroupings);
                String normVal = isCodeFld
                        ? transformCodeValue(fv.value, codeGroupings)
                        : fv.value;
                normalised.add(new FieldValue(normKey, normVal));
            }

            // Merge in case two variant keys collapsed to the same base key.
            result.merge(fieldName, normalised, (existing, incoming) -> {
                List<FieldValue> merged = new ArrayList<>(existing);
                merged.addAll(incoming);
                return merged;
            });
        }
        return result;
    }

    /**
     * Replaces the last {@code -}-delimited segment of {@code matchKey} with the
     * base code from {@link #CODE_GROUPINGS}. Returns {@code matchKey} unchanged
     * when the extracted code has no mapping.
     */
    private static String normalizeCodeKey(String matchKey,
                                            Map<String, String> codeGroupings) {
        if (matchKey == null) return null;
        int lastDash = matchKey.lastIndexOf('-');
        String code  = lastDash >= 0 ? matchKey.substring(lastDash + 1) : matchKey;
        String base  = codeGroupings.get(code);
        if (base == null) return matchKey;
        return lastDash >= 0 ? matchKey.substring(0, lastDash + 1) + base : base;
    }

    /**
     * Rewrites a code field value to {@code "originalCode/baseCode"} format.
     * The code is extracted as the last {@code -}-delimited segment of {@code value}.
     * Returns {@code value} unchanged when there is no mapping for the extracted code.
     */
    private static String transformCodeValue(String value,
                                              Map<String, String> codeGroupings) {
        if (value == null) return null;
        int lastDash = value.lastIndexOf('-');
        String code  = lastDash >= 0 ? value.substring(lastDash + 1) : value;
        String base  = codeGroupings.get(code);
        return base != null ? code + "/" + base : value;
    }

    /**
     * Extracts the base code from a {@code "originalCode/baseCode"} value.
     * Returns the full value as-is for non-normalised values that contain no {@code /}.
     */
    private static String extractBaseCode(String value) {
        if (value == null) return null;
        int slash = value.indexOf('/');
        return slash >= 0 ? value.substring(slash + 1) : value;
    }

    private void emitRow(ProcessContext ctx,
                         String imageId, String segment, String keyId, int iteration,
                         String aiCreatedAt, String humanCreatedAt, String comparedAt,
                         String field, String arrayKey,
                         String humanVal, String aiVal) {

        // For code-normalised fields, compare base codes only ("001/005" vs "002/005" → match).
        // For all other fields, plain case-insensitive equality.
        final boolean isMatch;
        if (CODE_FIELD_PATHS.contains(field)) {
            String humanBase = extractBaseCode(humanVal);
            String aiBase    = extractBaseCode(aiVal);
            isMatch = humanBase == null ? aiBase == null : humanBase.equalsIgnoreCase(aiBase);
        } else {
            isMatch = humanVal == null ? aiVal == null : humanVal.equalsIgnoreCase(aiVal);
        }
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
