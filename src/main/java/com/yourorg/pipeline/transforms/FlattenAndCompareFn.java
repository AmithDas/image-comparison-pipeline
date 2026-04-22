package com.yourorg.pipeline.transforms;

import com.google.api.services.bigquery.model.TableRow;
import com.yourorg.pipeline.util.BarricadeEncryptionUtil;
import com.yourorg.pipeline.util.JsonFieldExtractor;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

/**
 * Flattens both JSON payloads in a matched pair and emits one
 * {@link TableRow} per field for the image_comparison_results output table.
 *
 * <p>Payloads are Barricade-encrypted in the {@code GenericRecord}. This transform
 * decrypts them using {@code key_id} from the human record before flattening.
 * {@code is_match} is computed on plaintext. {@code human_value} and {@code ai_value}
 * are re-encrypted with Barricade before writing to BigQuery.
 */
public class FlattenAndCompareFn
        extends DoFn<KV<String, KV<GenericRecord, GenericRecord>>, TableRow> {

    private static final Logger LOG = LoggerFactory.getLogger(FlattenAndCompareFn.class);

    private final Map<String, String> arraySortKeys;
    private final String firestoreCollection;
    private final String kmsKeyPath;

    public FlattenAndCompareFn(Map<String, String> arraySortKeys,
                                String firestoreCollection,
                                String kmsKeyPath) {
        this.arraySortKeys       = arraySortKeys;
        this.firestoreCollection = firestoreCollection;
        this.kmsKeyPath          = kmsKeyPath;
    }

    @Setup
    public void setup() {
        BarricadeEncryptionUtil.configure(firestoreCollection, kmsKeyPath);
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

        String keyId      = str(human.get("key_id"));
        String humanKeyId = keyId;
        String aiKeyId    = str(ai.get("key_id"));
        if (!Objects.equals(humanKeyId, aiKeyId)) {
            LOG.warn("imageId='{}' has mismatched key_ids (human={}, ai={}) — using human key_id",
                    imageId, humanKeyId, aiKeyId);
        }

        // ── Decrypt payloads, then flatten ────────────────────────────────────
        String humanPayload = BarricadeEncryptionUtil.decrypt(keyId, str(human.get("payload")));
        String aiPayload    = BarricadeEncryptionUtil.decrypt(keyId, str(ai.get("payload")));

        Map<String, String> humanFields = JsonFieldExtractor.flatten(humanPayload, arraySortKeys);
        Map<String, String> aiFields    = JsonFieldExtractor.flatten(aiPayload, arraySortKeys);

        if (humanFields.isEmpty() && aiFields.isEmpty()) {
            LOG.warn("Both payloads empty or unparseable for imageId='{}' — skipping", imageId);
            return;
        }

        // ── Union of all field names ──────────────────────────────────────────
        Set<String> allFields = new TreeSet<>();
        allFields.addAll(humanFields.keySet());
        allFields.addAll(aiFields.keySet());

        String comparedAt = Instant.now().toString();

        // ── Emit one row per field ────────────────────────────────────────────
        for (String field : allFields) {
            String humanVal = humanFields.get(field);
            String aiVal    = aiFields.get(field);

            // Compare on plaintext; encrypt values for storage.
            boolean isMatch          = Objects.equals(humanVal, aiVal);
            String encryptedHumanVal = BarricadeEncryptionUtil.encrypt(keyId, humanVal);
            String encryptedAiVal    = BarricadeEncryptionUtil.encrypt(keyId, aiVal);

            TableRow row = new TableRow()
                    .set("image_id",         imageId)
                    .set("key_id",           keyId)
                    .set("ai_iteration",     iteration)
                    .set("ai_created_at",    str(ai.get("created_at")))
                    .set("human_created_at", str(human.get("created_at")))
                    .set("field_name",       field)
                    .set("human_value",      encryptedHumanVal)
                    .set("ai_value",         encryptedAiVal)
                    .set("is_match",         isMatch)
                    .set("compared_at",      comparedAt);

            ctx.output(row);
        }

        LOG.info("Compared imageId='{}' iteration={} — {} fields emitted",
                imageId, iteration, allFields.size());
    }

    private static String str(Object value) {
        return value != null ? value.toString() : null;
    }
}
