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
import java.util.List;
import java.util.Map;

/**
 * Produces field-level mismatch rows for orphaned (unmatched) payloads.
 *
 * <p>An orphaned payload is one that aged out of the pending table without ever
 * finding its counterpart.  Every field in the payload is emitted as a comparison
 * row where one side carries the actual value and the other side is {@code null},
 * making {@code is_match = false} for every row.
 *
 * <h3>Side assignment</h3>
 * <ul>
 *   <li>{@code pending_type = "human"} → {@code human_value} = field value,
 *       {@code ai_value} = {@code null}</li>
 *   <li>{@code pending_type = "ai"} → {@code human_value} = {@code null},
 *       {@code ai_value} = field value</li>
 * </ul>
 *
 * <h3>Routing</h3>
 * Rows are routed to the same named tables as {@link FlattenAndCompareFn} using
 * {@link FlattenAndCompareFn#SEGMENT_ROUTES} — the output type is identical
 * ({@code KV<String, TableRow>}).
 */
public class OrphanCompareFn extends DoFn<GenericRecord, KV<String, TableRow>> {

    private static final Logger LOG = LoggerFactory.getLogger(OrphanCompareFn.class);

    private final ValueProvider<String> firestoreCollection;
    private final ValueProvider<String> kmsKeyPath;

    public OrphanCompareFn(ValueProvider<String> firestoreCollection,
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
        GenericRecord record = ctx.element();

        String imageId    = str(record.get("image_id"));
        String keyId      = str(record.get("key_id"));
        String pendingType = str(record.get("pending_type")); // "human" or "ai"
        String payload    = str(record.get("payload"));
        String createdAt  = TimestampUtil.normalizeTimestamp(str(record.get("created_at")));
        String comparedAt = TimestampUtil.formatInstant(Instant.now());

        if (imageId == null || keyId == null || pendingType == null || payload == null) {
            LOG.warn("Skipping orphan record — missing required fields: imageId={}", imageId);
            return;
        }

        boolean isHuman = "human".equalsIgnoreCase(pendingType);

        // Decrypt and flatten the orphaned payload
        String decrypted = BarricadeEncryptionUtil.decrypt(keyId, payload);
        Map<String, List<FieldValue>> fields =
                JsonFieldExtractor.flatten(decrypted, FlattenAndCompareFn.ARRAY_MATCH_KEYS);

        if (fields.isEmpty()) {
            LOG.warn("Orphaned payload for imageId='{}' is empty or unparseable — skipping", imageId);
            return;
        }

        int rowsEmitted = 0;

        for (Map.Entry<String, List<FieldValue>> entry : fields.entrySet()) {
            String fieldName = entry.getKey();
            List<FieldValue> entries = entry.getValue();

            for (FieldValue fv : entries) {
                String encryptedValue = BarricadeEncryptionUtil.encrypt(keyId, fv.value);

                // Assign value to the correct side; the absent side is null.
                String humanValue = isHuman ? encryptedValue : null;
                String aiValue    = isHuman ? null : encryptedValue;

                String humanCreatedAt = isHuman ? createdAt : null;
                String aiCreatedAt    = isHuman ? null : createdAt;

                TableRow row = new TableRow()
                        .set("image_id",         imageId)
                        .set("key_id",           keyId)
                        .set("ai_iteration",     0)           // no matched AI iteration
                        .set("ai_created_at",    aiCreatedAt)
                        .set("human_created_at", humanCreatedAt)
                        .set("field_name",       fieldName)
                        .set("array_key",        fv.matchKey) // null for scalars/positional
                        .set("human_value",      humanValue)
                        .set("ai_value",         aiValue)
                        .set("is_match",         false)       // always false — one side absent
                        .set("compared_at",      comparedAt);

                // Route by top-level segment — same logic as FlattenAndCompareFn.
                String segment = fieldName.contains(".")
                        ? fieldName.substring(0, fieldName.indexOf('.'))
                        : fieldName;
                String route = FlattenAndCompareFn.SEGMENT_ROUTES.getOrDefault(
                        segment, FlattenAndCompareFn.ROUTE_MAIN);

                ctx.output(KV.of(route, row));
                rowsEmitted++;
            }
        }

        LOG.info("Orphan imageId='{}' pendingType='{}' — {} mismatch rows emitted",
                imageId, pendingType, rowsEmitted);
    }

    private static String str(Object value) {
        return value != null ? value.toString() : null;
    }
}
