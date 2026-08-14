package com.yourorg.pipeline.transforms;

import com.google.api.services.bigquery.model.TableRow;
import com.yourorg.pipeline.util.BarricadeEncryptionUtil;
import com.yourorg.pipeline.util.JsonFieldExtractor;
import com.yourorg.pipeline.util.JsonFieldExtractor.FieldValue;
import com.yourorg.pipeline.util.TimestampUtil;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * Produces field-level mismatch rows for orphaned (unmatched) payloads.
 *
 * <p>An orphaned payload is one that aged out of the pending table without ever
 * finding its counterpart. Every field in the payload is emitted as a comparison
 * row where one side carries the actual value and the other side is {@code null},
 * making {@code is_match = false} for every row.
 *
 * <p>The {@code segment} value is read from the pending {@link GenericRecord} so
 * that orphan rows can be filtered by segment in the shared comparison table.
 *
 * <h3>Side assignment</h3>
 * <ul>
 *   <li>{@code pending_type = "human"} → {@code human_value} = field value,
 *       {@code ai_value} = {@code null}</li>
 *   <li>{@code pending_type = "ai"} → {@code human_value} = {@code null},
 *       {@code ai_value} = field value</li>
 * </ul>
 */
public class OrphanCompareFn extends DoFn<GenericRecord, TableRow> {

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

        String imageId     = str(record.get("image_id"));
        String keyId       = str(record.get("key_id"));
        String segment     = str(record.get("segment"));
        String pendingType = str(record.get("pending_type")); // "human" or "ai"
        String payload     = str(record.get("payload"));
        String createdAt   = TimestampUtil.normalizeTimestamp(str(record.get("created_at")));
        String loadTime    = TimestampUtil.formatInstant(Instant.now());

        if (imageId == null || keyId == null || pendingType == null || payload == null) {
            LOG.warn("Skipping orphan record — missing required fields: imageId={}", imageId);
            return;
        }
        if (segment == null || segment.isBlank()) {
            segment = "main";
        }

        boolean isHuman = "human".equalsIgnoreCase(pendingType);

        String decrypted = BarricadeEncryptionUtil.decrypt(keyId, payload);
        Map<String, List<FieldValue>> fields =
                JsonFieldExtractor.flatten(decrypted, FlattenAndCompareFn.ARRAY_MATCH_KEYS);

        if (fields.isEmpty()) {
            LOG.warn("Orphaned payload for imageId='{}' segment='{}' is empty — skipping",
                    imageId, segment);
            return;
        }

        int rowsEmitted = 0;

        for (Map.Entry<String, List<FieldValue>> entry : fields.entrySet()) {
            String fieldName     = entry.getKey();
            List<FieldValue> fvs = entry.getValue();

            for (FieldValue fv : fvs) {
                String encryptedValue = BarricadeEncryptionUtil.encrypt(keyId, fv.value);

                String humanValue     = isHuman ? encryptedValue : null;
                String aiValue        = isHuman ? null : encryptedValue;
                String humanCreatedAt = isHuman ? createdAt : null;
                String aiCreatedAt    = isHuman ? null : createdAt;

                ctx.output(new TableRow()
                        .set("image_id",         imageId)
                        .set("key_id",           keyId)
                        .set("segment",          segment)
                        .set("ai_iteration",     0)
                        .set("ai_created_at",    aiCreatedAt)
                        .set("human_created_at", humanCreatedAt)
                        .set("field_name",       fieldName)
                        .set("array_key",        fv.matchKey)
                        .set("human_value",      humanValue)
                        .set("ai_value",         aiValue)
                        .set("is_match",         false)
                        .set("load_time",        loadTime));
                rowsEmitted++;
            }
        }

        LOG.info("Orphan imageId='{}' segment='{}' pendingType='{}' — {} mismatch rows",
                imageId, segment, pendingType, rowsEmitted);
    }

    private static String str(Object value) {
        return value != null ? value.toString() : null;
    }
}
