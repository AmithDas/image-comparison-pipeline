package com.yourorg.pipeline.transforms;

import com.google.api.services.bigquery.model.TableRow;
import com.yourorg.pipeline.util.BarricadeEncryptionUtil;
import com.yourorg.pipeline.util.JsonFieldExtractor;
import com.yourorg.pipeline.util.TimestampUtil;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * Flattens both JSON payloads in a matched pair and emits one
 * {@link TableRow} per field value for the image_comparison_results output table.
 *
 * <p>For scalar fields one row is emitted.  For array fields one row is emitted
 * per element (positional comparison after sorting), all under the same
 * {@code field_name} — no index suffix is added to the field name.
 *
 * <p>Payloads are Barricade-encrypted in the {@code GenericRecord}. This transform
 * decrypts them using {@code key_id} from the human record before flattening.
 * {@code is_match} is computed on plaintext (case-insensitive).
 * {@code human_value} and {@code ai_value} are re-encrypted before writing to BigQuery.
 *
 * <h3>ValueProvider fields</h3>
 * {@code firestoreCollection} and {@code kmsKeyPath} are {@link ValueProvider}s so
 * they can be wired directly from pipeline options and resolved on each Dataflow
 * worker in {@code @Setup}, before any {@code @ProcessElement} call.
 */
public class FlattenAndCompareFn
        extends DoFn<KV<String, KV<GenericRecord, GenericRecord>>, TableRow> {

    private static final Logger LOG = LoggerFactory.getLogger(FlattenAndCompareFn.class);

    private final Map<String, String>   arraySortKeys;
    private final ValueProvider<String> firestoreCollection;
    private final ValueProvider<String> kmsKeyPath;

    public FlattenAndCompareFn(Map<String, String> arraySortKeys,
                                ValueProvider<String> firestoreCollection,
                                ValueProvider<String> kmsKeyPath) {
        this.arraySortKeys       = arraySortKeys;
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

        Map<String, List<String>> humanFields = JsonFieldExtractor.flatten(humanPayload, arraySortKeys);
        Map<String, List<String>> aiFields    = JsonFieldExtractor.flatten(aiPayload, arraySortKeys);

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

        // ── Emit one row per field value ──────────────────────────────────────
        // Scalar fields → single row.
        // Array fields  → one row per element (positional, after sorting).
        // If one side has more elements than the other the extra positions are null.
        int rowsEmitted = 0;
        for (String field : allFields) {
            List<String> humanVals = humanFields.getOrDefault(field, Collections.emptyList());
            List<String> aiVals    = aiFields.getOrDefault(field, Collections.emptyList());
            int count = Math.max(humanVals.size(), aiVals.size());

            for (int i = 0; i < count; i++) {
                String humanVal = i < humanVals.size() ? humanVals.get(i) : null;
                String aiVal    = i < aiVals.size()    ? aiVals.get(i)    : null;

                boolean isMatch          = humanVal == null ? aiVal == null
                                                            : humanVal.equalsIgnoreCase(aiVal);
                String encryptedHumanVal = BarricadeEncryptionUtil.encrypt(keyId, humanVal);
                String encryptedAiVal    = BarricadeEncryptionUtil.encrypt(keyId, aiVal);

                TableRow row = new TableRow()
                        .set("image_id",         imageId)
                        .set("key_id",           keyId)
                        .set("ai_iteration",     iteration)
                        .set("ai_created_at",    aiCreatedAt)
                        .set("human_created_at", humanCreatedAt)
                        .set("field_name",       field)
                        .set("human_value",      encryptedHumanVal)
                        .set("ai_value",         encryptedAiVal)
                        .set("is_match",         isMatch)
                        .set("compared_at",      comparedAt);

                ctx.output(row);
                rowsEmitted++;
            }
        }

        LOG.info("Compared imageId='{}' iteration={} — {} rows emitted ({} distinct fields)",
                imageId, iteration, rowsEmitted, allFields.size());
    }

    private static boolean nullSafeEquals(String a, String b) {
        return a == null ? b == null : a.equals(b);
    }

    private static String str(Object value) {
        return value != null ? value.toString() : null;
    }
}
