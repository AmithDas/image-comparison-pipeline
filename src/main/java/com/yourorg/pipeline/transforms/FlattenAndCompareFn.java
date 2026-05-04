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
 * <h3>String comparison</h3>
 * {@code is_match} is computed on plaintext (case-insensitive).
 * Values are re-encrypted before writing to BigQuery.
 */
public class FlattenAndCompareFn
        extends DoFn<KV<String, KV<GenericRecord, GenericRecord>>, TableRow> {

    private static final Logger LOG = LoggerFactory.getLogger(FlattenAndCompareFn.class);

    // ── Array match keys ──────────────────────────────────────────────────────

    static final Map<String, String> ARRAY_MATCH_KEYS = Map.of(
            "documentDetails", "docType"
            // "tradeline", "accountnumber-customernumber-dateopened-code",
            // "address",   "addresstype"
    );

    // ── Constructor ───────────────────────────────────────────────────────────

    private final ValueProvider<String> firestoreCollection;
    private final ValueProvider<String> kmsKeyPath;
    private final ValueProvider<String> segmentConfigsJson;

    // segment name → (aiField → humanField); resolved in @Setup
    private transient Map<String, Map<String, String>> aiToHumanBySegment;

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

        aiToHumanBySegment = new HashMap<>();
        List<SegmentConfig> segments = SegmentConfig.parse(segmentConfigsJson.get());
        for (SegmentConfig seg : segments) {
            if (seg.fieldMappings == null || seg.fieldMappings.isEmpty()) continue;
            Map<String, String> aiToHuman = new HashMap<>();
            for (SegmentConfig.FieldMapping fm : seg.fieldMappings) {
                if (fm.aiField != null && fm.humanField != null) {
                    aiToHuman.put(fm.aiField, fm.humanField);
                }
            }
            if (!aiToHuman.isEmpty()) {
                aiToHumanBySegment.put(seg.name, aiToHuman);
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

    private void emitRow(ProcessContext ctx,
                         String imageId, String segment, String keyId, int iteration,
                         String aiCreatedAt, String humanCreatedAt, String comparedAt,
                         String field, String arrayKey,
                         String humanVal, String aiVal) {

        boolean isMatch          = humanVal == null ? aiVal == null
                                                    : humanVal.equalsIgnoreCase(aiVal);
        String encryptedHumanVal = BarricadeEncryptionUtil.encrypt(keyId, humanVal);
        String encryptedAiVal    = BarricadeEncryptionUtil.encrypt(keyId, aiVal);

        ctx.output(new TableRow()
                .set("image_id",         imageId)
                .set("key_id",           keyId)
                .set("segment",          segment)
                .set("ai_iteration",     iteration)
                .set("ai_created_at",    aiCreatedAt)
                .set("human_created_at", humanCreatedAt)
                .set("field_name",       field)
                .set("array_key",        arrayKey)
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
