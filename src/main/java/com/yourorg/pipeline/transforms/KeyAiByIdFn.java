package com.yourorg.pipeline.transforms;

import com.google.api.services.bigquery.model.TableRow;
import com.yourorg.pipeline.util.BarricadeEncryptionUtil;
import com.yourorg.pipeline.util.JsonFieldExtractor;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Decrypts the {@code payload} of each AI source {@link TableRow}, extracts the
 * image identifier from the plaintext JSON using a configurable dot-notation path,
 * and emits a {@code KV<imageId, row>} pair for downstream co-grouping.
 *
 * <p>Rows where the image identifier field is absent or null after decryption are
 * silently dropped with a WARN log — they cannot be joined without an identifier.
 *
 * <p>The DEK for each row is resolved via {@link BarricadeEncryptionUtil} using
 * the row's {@code key_id} column. DEKs are loaded lazily on first use and
 * cached in memory for the lifetime of the worker JVM, so each {@code key_id}
 * incurs at most one Firestore read + one KMS call.
 *
 * <p>Initialise once per worker with:
 * <pre>
 *   ParDo.of(new KeyAiByIdFn(firestoreCollection, kmsKeyPath, imageNameField))
 * </pre>
 */
public class KeyAiByIdFn extends DoFn<TableRow, KV<String, TableRow>> {

    private static final Logger LOG = LoggerFactory.getLogger(KeyAiByIdFn.class);

    private final String firestoreCollection;
    private final String kmsKeyPath;
    /** Dot-notation path to the image identifier field, e.g. {@code "metadata.image_name"}. */
    private final String imageNameField;

    public KeyAiByIdFn(String firestoreCollection, String kmsKeyPath, String imageNameField) {
        this.firestoreCollection = firestoreCollection;
        this.kmsKeyPath          = kmsKeyPath;
        this.imageNameField      = imageNameField;
    }

    @Setup
    public void setup() {
        BarricadeEncryptionUtil.configure(firestoreCollection, kmsKeyPath);
    }

    @ProcessElement
    public void processElement(ProcessContext ctx) {
        TableRow row       = ctx.element();
        String   keyId     = (String) row.get("key_id");
        String   decrypted = BarricadeEncryptionUtil.decrypt(keyId, (String) row.get("payload"));
        String   imageName = JsonFieldExtractor.extractField(decrypted, imageNameField);

        if (imageName == null || imageName.isBlank()) {
            LOG.warn("Skipping AI row: field '{}' is absent or null for key_id={}",
                    imageNameField, keyId);
            return;
        }

        ctx.output(KV.of(imageName, row));
    }
}
