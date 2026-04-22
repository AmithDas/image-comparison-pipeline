package com.yourorg.pipeline.transforms;

import com.google.api.services.bigquery.model.TableRow;
import com.yourorg.pipeline.util.BarricadeEncryptionUtil;
import com.yourorg.pipeline.util.JsonFieldExtractor;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Decrypts the {@code payload} of each human source {@link TableRow}, applies an
 * optional payload-field filter, extracts the image identifier using a configurable
 * dot-notation path, and emits a {@code KV<imageId, row>} pair for downstream
 * co-grouping.
 *
 * <h3>Field path</h3>
 * {@code imageNameField} is a dot-notation path into the decrypted JSON, e.g.
 * {@code "metadata.image_name"} or {@code "header.imageId"}. Rows where the path
 * resolves to null or is absent are silently dropped with a WARN log.
 *
 * <h3>Filtering</h3>
 * When {@code humanFilterField} is non-blank, only rows whose decrypted payload
 * contains {@code humanFilterField == humanFilterValue} are forwarded. Rows that
 * do not match the filter are silently dropped. Pass {@code null} (or blank) for
 * {@code humanFilterField} to disable filtering.
 *
 * <h3>Encryption</h3>
 * The decryption key is resolved via {@link BarricadeEncryptionUtil} using the
 * row's {@code key_id} column. DEKs are loaded lazily on first use and cached in
 * memory for the lifetime of the worker JVM, so each {@code key_id} incurs at
 * most one Firestore read + one KMS call.
 *
 * <p>Decrypt, filter, and key happen in a single pass — the payload is decrypted
 * exactly once per row regardless of whether filtering is enabled.
 *
 * <p>Initialise once per worker with:
 * <pre>
 *   ParDo.of(new FilterAndKeyHumanByIdFn(
 *       firestoreCollection, kmsKeyPath, imageNameField,
 *       humanFilterField, humanFilterValue))
 * </pre>
 */
public class FilterAndKeyHumanByIdFn extends DoFn<TableRow, KV<String, TableRow>> {

    private static final Logger LOG = LoggerFactory.getLogger(FilterAndKeyHumanByIdFn.class);

    private final String firestoreCollection;
    private final String kmsKeyPath;
    /** Dot-notation path to the image identifier field, e.g. {@code "metadata.image_name"}. */
    private final String imageNameField;
    private final String humanFilterField;   // nullable / blank → no filter
    private final String humanFilterValue;   // required when humanFilterField is set

    public FilterAndKeyHumanByIdFn(String firestoreCollection,
                                    String kmsKeyPath,
                                    String imageNameField,
                                    String humanFilterField,
                                    String humanFilterValue) {
        this.firestoreCollection = firestoreCollection;
        this.kmsKeyPath          = kmsKeyPath;
        this.imageNameField      = imageNameField;
        this.humanFilterField    = humanFilterField;
        this.humanFilterValue    = humanFilterValue;
    }

    @Setup
    public void setup() {
        // Initialise once per worker JVM before the first decrypt call.
        BarricadeEncryptionUtil.configure(firestoreCollection, kmsKeyPath);
    }

    @ProcessElement
    public void processElement(ProcessContext ctx) {
        TableRow row       = ctx.element();
        String   keyId     = (String) row.get("key_id");
        String   decrypted = BarricadeEncryptionUtil.decrypt(keyId, (String) row.get("payload"));

        // Apply optional payload-field filter (single decrypt, shared with key extraction below).
        if (humanFilterField != null && !humanFilterField.isBlank()) {
            String actual = JsonFieldExtractor.extractField(decrypted, humanFilterField);
            if (!humanFilterValue.equals(actual)) {
                LOG.debug("Skipping human row: payload.{} = '{}', expected '{}'",
                        humanFilterField, actual, humanFilterValue);
                return;
            }
        }

        String imageName = JsonFieldExtractor.extractField(decrypted, imageNameField);
        if (imageName == null || imageName.isBlank()) {
            LOG.warn("Skipping human row: field '{}' is absent or null for key_id={}",
                    imageNameField, keyId);
            return;
        }

        ctx.output(KV.of(imageName, row));
    }
}
