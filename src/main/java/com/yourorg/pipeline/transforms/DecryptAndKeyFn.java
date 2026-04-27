package com.yourorg.pipeline.transforms;

import com.google.api.services.bigquery.model.TableRow;
import com.yourorg.pipeline.util.BarricadeEncryptionUtil;
import com.yourorg.pipeline.util.JsonFieldExtractor;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Decrypts the {@code payload} of a source {@link TableRow}, applies an optional
 * payload-field filter, extracts the image identifier via a configurable path, and
 * emits {@code KV<"imageId::segment", row>} for downstream co-grouping.
 *
 * <p>The segment name is fixed at construction time — one instance is created per
 * segment, each fed from a dedicated BQ query filtered to that segment's method value.
 * This keeps routing explicit and avoids any runtime method-to-segment lookup.
 *
 * <h3>Usage</h3>
 * <pre>
 *   // AI rows for the "authentication" segment
 *   ParDo.of(DecryptAndKeyFn.forAi(firestoreCollection, kmsKeyPath,
 *                                   imageNameField, "authentication"))
 *
 *   // Human rows with optional payload filter
 *   ParDo.of(DecryptAndKeyFn.forHuman(firestoreCollection, kmsKeyPath,
 *                                      imageNameField, "authentication",
 *                                      filterField, filterValue))
 * </pre>
 *
 * <h3>Payload filter</h3>
 * When {@code filterField} is non-blank, only rows where
 * {@code filterField == filterValue} in the decrypted payload are forwarded.
 * Pass {@code null} for {@code filterField} to disable filtering.
 */
public class DecryptAndKeyFn extends DoFn<TableRow, KV<String, TableRow>> {

    private static final Logger LOG = LoggerFactory.getLogger(DecryptAndKeyFn.class);

    private final ValueProvider<String> firestoreCollection;
    private final ValueProvider<String> kmsKeyPath;
    private final ValueProvider<String> imageNameField;
    private final String                segment;       // fixed per instance
    private final String                filterField;   // null → no filter
    private final String                filterValue;

    private DecryptAndKeyFn(ValueProvider<String> firestoreCollection,
                             ValueProvider<String> kmsKeyPath,
                             ValueProvider<String> imageNameField,
                             String segment,
                             String filterField,
                             String filterValue) {
        this.firestoreCollection = firestoreCollection;
        this.kmsKeyPath          = kmsKeyPath;
        this.imageNameField      = imageNameField;
        this.segment             = segment;
        this.filterField         = filterField;
        this.filterValue         = filterValue;
    }

    // ── Static factories ──────────────────────────────────────────────────────

    /** No payload filter — suitable for AI rows. */
    public static DecryptAndKeyFn forAi(ValueProvider<String> firestoreCollection,
                                         ValueProvider<String> kmsKeyPath,
                                         ValueProvider<String> imageNameField,
                                         String segment) {
        return new DecryptAndKeyFn(
                firestoreCollection, kmsKeyPath, imageNameField, segment, null, null);
    }

    /**
     * With an optional payload filter — suitable for human rows.
     * Pass {@code null} for {@code filterField} to skip filtering.
     */
    public static DecryptAndKeyFn forHuman(ValueProvider<String> firestoreCollection,
                                            ValueProvider<String> kmsKeyPath,
                                            ValueProvider<String> imageNameField,
                                            String segment,
                                            String filterField,
                                            String filterValue) {
        return new DecryptAndKeyFn(
                firestoreCollection, kmsKeyPath, imageNameField,
                segment, filterField, filterValue);
    }

    // ── Beam lifecycle ────────────────────────────────────────────────────────

    @Setup
    public void setup() {
        BarricadeEncryptionUtil.configure(
                firestoreCollection.get(),
                kmsKeyPath.get());
    }

    @ProcessElement
    public void processElement(ProcessContext ctx) {
        TableRow row    = ctx.element();
        String   keyId  = (String) row.get("key_id");
        String decrypted = BarricadeEncryptionUtil.decrypt(keyId, (String) row.get("payload"));

        // Optional payload-field filter (typically applied to human rows).
        if (filterField != null && !filterField.isBlank()) {
            String actual = JsonFieldExtractor.extractField(decrypted, filterField);
            if (!filterValue.equals(actual)) {
                LOG.debug("Skipping row: payload.{} = '{}', expected '{}'",
                        filterField, actual, filterValue);
                return;
            }
        }

        String imageName = JsonFieldExtractor.extractField(decrypted, imageNameField.get());
        if (imageName == null || imageName.isBlank()) {
            LOG.warn("Skipping row: field '{}' is absent or null for key_id={}",
                    imageNameField.get(), keyId);
            return;
        }

        // Key format: "imageId::segment"
        ctx.output(KV.of(imageName + "::" + segment, row));
    }
}
