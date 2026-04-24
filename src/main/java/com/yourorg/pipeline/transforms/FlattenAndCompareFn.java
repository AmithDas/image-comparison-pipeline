package com.yourorg.pipeline.transforms;

import com.google.api.services.bigquery.model.TableRow;
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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * Flattens both JSON payloads in a matched pair and emits one
 * {@link TableRow} per field value for the image_comparison_results output table.
 *
 * <h3>Array comparison</h3>
 * <ul>
 *   <li><b>Top-level keyed segments</b> (configured in {@code ARRAY_MATCH_KEYS}): each
 *       element's composite key is derived from the configured field(s).  For each unique
 *       key across human and AI, comparison rows are emitted.  A key present on only one
 *       side produces mismatch rows with {@code null} on the other side.</li>
 *   <li><b>Child arrays</b> (nested inside a keyed segment, not in the map): elements
 *       automatically inherit the parent segment's key as {@code array_key}.  Within that
 *       key group, child elements are compared positionally after sorting.</li>
 *   <li><b>Positional arrays</b> (not in the map, no keyed parent): compared
 *       position-by-position after sorting.  {@code array_key} is {@code null}.</li>
 *   <li><b>Scalar fields</b>: one row, {@code array_key} is {@code null}.</li>
 * </ul>
 *
 * <h3>String comparison</h3>
 * {@code is_match} is computed on plaintext using case-insensitive equality.
 * {@code human_value} and {@code ai_value} are re-encrypted with Barricade before
 * writing to BigQuery.
 */
public class FlattenAndCompareFn
        extends DoFn<KV<String, KV<GenericRecord, GenericRecord>>, TableRow> {

    private static final Logger LOG = LoggerFactory.getLogger(FlattenAndCompareFn.class);

    /**
     * Top-level segment keys only.  Child arrays inside a segment automatically
     * inherit the segment's derived key — they do not need their own entry here.
     *
     * <h3>Map key</h3>
     * Dot-notation path to a top-level array segment, e.g. {@code "tradeline"},
     * {@code "address"}.
     *
     * <h3>Map value — key specification</h3>
     * A {@code -}-separated list of field names whose values are extracted from each
     * element and joined (lower-cased) to form the composite {@code array_key}:
     * <ul>
     *   <li>Single field:  {@code "code"}</li>
     *   <li>Composite:     {@code "accountnumber-customernumber-dateopened-code"}</li>
     * </ul>
     *
     * <p>Child arrays (e.g. {@code tradeline.paymentHistory}) are compared positionally
     * within each parent key group and share the parent's {@code array_key}.
     *
     * <p>Arrays not listed here (and with no keyed parent) fall back to global
     * positional comparison with {@code array_key = null}.
     */
    private static final Map<String, String> ARRAY_MATCH_KEYS = Map.of(
            // "tradeline", "accountnumber-customernumber-dateopened-code",
            // "address",   "addresstype"
    );

    private final ValueProvider<String> firestoreCollection;
    private final ValueProvider<String> kmsKeyPath;

    public FlattenAndCompareFn(ValueProvider<String> firestoreCollection,
                                ValueProvider<String> kmsKeyPath) {
        this.firestoreCollection = firestoreCollection;
        this.kmsKeyPath          = kmsKeyPath;
    }

    @Setup
    public void setup() {
        BarricadeEncryptionUtil.configure(
                firestoreCollection.get(),
                kmsKeyPath.get());
    }

    @ProcessElement
    public void processElement(ProcessContext ctx) {
        String pairKey = ctx.element().getKey();
        String[] parts = pairKey.split("::");

        if (parts.length != 2) {
            LOG.warn("Unexpected pairKey format: '{}' — skipping", pairKey);
            return;
        }

        String imageId = parts[0];
        int iteration;
        try {
            iteration = Integer.parseInt(parts[1]);
        } catch (NumberFormatException e) {
            LOG.warn("Could not parse iteration from pairKey '{}' — skipping", pairKey);
            return;
        }

        GenericRecord human = ctx.element().getValue().getKey();
        GenericRecord ai    = ctx.element().getValue().getValue();

        String humanKeyId = str(human.get("key_id"));
        String aiKeyId    = str(ai.get("key_id"));
        if (!nullSafeEquals(humanKeyId, aiKeyId)) {
            LOG.warn("imageId='{}' has mismatched key_ids (human={}, ai={}) — using human key_id",
                    imageId, humanKeyId, aiKeyId);
        }
        String keyId = humanKeyId;

        // ── Decrypt payloads, then flatten ────────────────────────────────────
        String humanPayload = BarricadeEncryptionUtil.decrypt(keyId, str(human.get("payload")));
        String aiPayload    = BarricadeEncryptionUtil.decrypt(keyId, str(ai.get("payload")));

        Map<String, List<FieldValue>> humanFields =
                JsonFieldExtractor.flatten(humanPayload, ARRAY_MATCH_KEYS);
        Map<String, List<FieldValue>> aiFields =
                JsonFieldExtractor.flatten(aiPayload, ARRAY_MATCH_KEYS);

        if (humanFields.isEmpty() && aiFields.isEmpty()) {
            LOG.warn("Both payloads empty or unparseable for imageId='{}' — skipping", imageId);
            return;
        }

        // ── Union of all field names ──────────────────────────────────────────
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

            // Determine mode: if any entry carries a non-null matchKey → key-based.
            boolean keyed = humanEntries.stream().anyMatch(fv -> fv.matchKey != null)
                         || aiEntries.stream().anyMatch(fv -> fv.matchKey != null);

            if (keyed) {
                // ── Key-based matching ────────────────────────────────────────
                // Group by matchKey.  Top-level keyed elements have exactly one
                // entry per key.  Child arrays that inherited a parent key may have
                // multiple entries under the same key — those are compared positionally
                // within the group.
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
                        emitRow(ctx, imageId, keyId, iteration,
                                aiCreatedAt, humanCreatedAt, comparedAt,
                                field, matchKey, humanVal, aiVal);
                        rowsEmitted++;
                    }
                }

            } else {
                // ── Positional matching ───────────────────────────────────────
                int count = Math.max(humanEntries.size(), aiEntries.size());
                for (int i = 0; i < count; i++) {
                    String humanVal = i < humanEntries.size() ? humanEntries.get(i).value : null;
                    String aiVal    = i < aiEntries.size()    ? aiEntries.get(i).value    : null;
                    emitRow(ctx, imageId, keyId, iteration,
                            aiCreatedAt, humanCreatedAt, comparedAt,
                            field, null, humanVal, aiVal);
                    rowsEmitted++;
                }
            }
        }

        LOG.info("Compared imageId='{}' iteration={} — {} rows emitted ({} distinct fields)",
                imageId, iteration, rowsEmitted, allFields.size());
    }

    // ── helpers ───────────────────────────────────────────────────────────────

    private void emitRow(ProcessContext ctx,
                         String imageId, String keyId, int iteration,
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
                .set("ai_iteration",     iteration)
                .set("ai_created_at",    aiCreatedAt)
                .set("human_created_at", humanCreatedAt)
                .set("field_name",       field)
                .set("array_key",        arrayKey)        // null for scalars / positional arrays
                .set("human_value",      encryptedHumanVal)
                .set("ai_value",         encryptedAiVal)
                .set("is_match",         isMatch)
                .set("compared_at",      comparedAt));
    }

    /**
     * Groups {@link FieldValue} entries by their {@code matchKey}.
     *
     * <p>Top-level keyed elements produce a single-entry list per key.
     * Child array elements that inherited a parent key produce a multi-entry list —
     * those are compared positionally within the group by the caller.
     */
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
