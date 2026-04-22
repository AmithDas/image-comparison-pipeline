package com.yourorg.pipeline.transforms;

import com.google.api.services.bigquery.model.TableRow;
import com.yourorg.pipeline.util.BarricadeEncryptionUtil;
import com.yourorg.pipeline.util.JsonFieldExtractor;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Decrypts the {@code payload} of a source {@link TableRow}, applies an optional
 * payload-field filter, extracts the image identifier via a configurable path, and
 * emits {@code KV<imageId, row>} for downstream co-grouping.
 *
 * <h3>Usage</h3>
 * Use the static factories rather than the constructor directly:
 * <pre>
 *   // AI rows — no filter
 *   ParDo.of(DecryptAndKeyFn.forAi(firestoreCollection, kmsKeyPath, imageNameField))
 *
 *   // Human rows — with optional payload filter
 *   ParDo.of(DecryptAndKeyFn.forHuman(firestoreCollection, kmsKeyPath, imageNameField,
 *                                     filterField, filterValue))
 * </pre>
 *
 * <h3>Image name field path</h3>
 * {@code imageNameField} is a dot-notation path with optional array indexing, e.g.
 * {@code "queueImages[0].fileName"} or {@code "metadata.imageId"}.
 * Rows where the path resolves to null or is absent are dropped with a WARN log.
 *
 * <h3>Payload filter</h3>
 * When {@code filterField} is non-blank, only rows whose decrypted payload contains
 * {@code filterField == filterValue} are forwarded; others are silently dropped.
 * Pass {@code null} for {@code filterField} to disable filtering.
 * Filtering and key extraction share a single decrypt call per row.
 *
 * <h3>Encryption</h3>
 * DEKs are resolved via {@link BarricadeEncryptionUtil} using each row's
 * {@code key_id} column, loaded lazily on first use, and cached for the lifetime
 * of the worker JVM (at most one Firestore read + one KMS call per {@code key_id}).
 */
public class DecryptAndKeyFn extends DoFn<TableRow, KV<String, TableRow>> {

    private static final Logger LOG = LoggerFactory.getLogger(DecryptAndKeyFn.class);

    private final String firestoreCollection;
    private final String kmsKeyPath;
    private final String imageNameField;
    private final String filterField;   // null → no filter
    private final String filterValue;   // required when filterField is non-blank

    private DecryptAndKeyFn(String firestoreCollection,
                             String kmsKeyPath,
                             String imageNameField,
                             String filterField,
                             String filterValue) {
        this.firestoreCollection = firestoreCollection;
        this.kmsKeyPath          = kmsKeyPath;
        this.imageNameField      = imageNameField;
        this.filterField         = filterField;
        this.filterValue         = filterValue;
    }

    // ── Static factories ──────────────────────────────────────────────────────

    /** No payload filter — suitable for AI rows. */
    public static DecryptAndKeyFn forAi(String firestoreCollection,
                                         String kmsKeyPath,
                                         String imageNameField) {
        return new DecryptAndKeyFn(firestoreCollection, kmsKeyPath, imageNameField, null, null);
    }

    /**
     * With an optional payload filter — suitable for human rows.
     * Pass {@code null} for {@code filterField} to skip filtering.
     */
    public static DecryptAndKeyFn forHuman(String firestoreCollection,
                                            String kmsKeyPath,
                                            String imageNameField,
                                            String filterField,
                                            String filterValue) {
        return new DecryptAndKeyFn(
                firestoreCollection, kmsKeyPath, imageNameField, filterField, filterValue);
    }

    // ── Beam lifecycle ────────────────────────────────────────────────────────

    @Setup
    public void setup() {
        BarricadeEncryptionUtil.configure(firestoreCollection, kmsKeyPath);
    }

    @ProcessElement
    public void processElement(ProcessContext ctx) {
        TableRow row       = ctx.element();
        String   keyId     = (String) row.get("key_id");
        String   decrypted = BarricadeEncryptionUtil.decrypt(keyId, (String) row.get("payload"));

        // Optional payload-field filter — shares the decrypt call above.
        if (filterField != null && !filterField.isBlank()) {
            String actual = JsonFieldExtractor.extractField(decrypted, filterField);
            if (!filterValue.equals(actual)) {
                LOG.debug("Skipping row: payload.{} = '{}', expected '{}'",
                        filterField, actual, filterValue);
                return;
            }
        }

        String imageName = JsonFieldExtractor.extractField(decrypted, imageNameField);
        if (imageName == null || imageName.isBlank()) {
            LOG.warn("Skipping row: field '{}' is absent or null for key_id={}",
                    imageNameField, keyId);
            return;
        }

        ctx.output(KV.of(imageName, row));
    }
}
