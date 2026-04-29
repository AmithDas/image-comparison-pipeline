package com.yourorg.pipeline.transforms;

import com.google.api.services.bigquery.model.TableRow;
import com.yourorg.pipeline.config.SegmentConfig;
import com.yourorg.pipeline.util.BarricadeEncryptionUtil;
import com.yourorg.pipeline.util.JsonFieldExtractor;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Decrypts the {@code payload} of a source {@link TableRow}, applies an optional
 * payload-field filter, extracts the image identifier via a configurable path, and
 * emits {@code KV<"imageId::segment", row>} for downstream co-grouping.
 *
 * <p>The segment name is resolved at runtime by looking up the row's {@code method}
 * column in the segment config map. Rows whose method does not match any configured
 * segment are silently dropped.
 *
 * <h3>Usage</h3>
 * <pre>
 *   // AI rows — all segments in one pass
 *   ParDo.of(DecryptAndKeyFn.forAi(firestoreCollection, kmsKeyPath,
 *                                   imageNameField, segmentConfigs))
 *
 *   // Human rows with optional payload filter
 *   ParDo.of(DecryptAndKeyFn.forHuman(firestoreCollection, kmsKeyPath,
 *                                      imageNameField, segmentConfigs,
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
    private final ValueProvider<String> segmentConfigsJson;
    private final ValueProvider<String> filterField;  // null → no filter
    private final ValueProvider<String> filterValue;

    // Resolved in @Setup; transient so they are re-initialized on each worker.
    private transient Map<String, String> methodToSegment;
    private transient String resolvedFilterField;
    private transient String resolvedFilterValue;
    private transient String resolvedImageNameField;

    private DecryptAndKeyFn(ValueProvider<String> firestoreCollection,
                             ValueProvider<String> kmsKeyPath,
                             ValueProvider<String> imageNameField,
                             ValueProvider<String> segmentConfigsJson,
                             ValueProvider<String> filterField,
                             ValueProvider<String> filterValue) {
        this.firestoreCollection = firestoreCollection;
        this.kmsKeyPath          = kmsKeyPath;
        this.imageNameField      = imageNameField;
        this.segmentConfigsJson  = segmentConfigsJson;
        this.filterField         = filterField;
        this.filterValue         = filterValue;
    }

    // ── Static factories ──────────────────────────────────────────────────────

    /** No payload filter — suitable for AI rows. */
    public static DecryptAndKeyFn forAi(ValueProvider<String> firestoreCollection,
                                         ValueProvider<String> kmsKeyPath,
                                         ValueProvider<String> imageNameField,
                                         ValueProvider<String> segmentConfigsJson) {
        return new DecryptAndKeyFn(
                firestoreCollection, kmsKeyPath, imageNameField, segmentConfigsJson, null, null);
    }

    /**
     * With an optional payload filter — suitable for human rows.
     * Pass {@code null} for {@code filterField} to skip filtering.
     */
    public static DecryptAndKeyFn forHuman(ValueProvider<String> firestoreCollection,
                                            ValueProvider<String> kmsKeyPath,
                                            ValueProvider<String> imageNameField,
                                            ValueProvider<String> segmentConfigsJson,
                                            ValueProvider<String> filterField,
                                            ValueProvider<String> filterValue) {
        return new DecryptAndKeyFn(
                firestoreCollection, kmsKeyPath, imageNameField,
                segmentConfigsJson, filterField, filterValue);
    }

    // ── Beam lifecycle ────────────────────────────────────────────────────────

    @Setup
    public void setup() {
        BarricadeEncryptionUtil.configure(
                firestoreCollection.get(),
                kmsKeyPath.get());

        List<SegmentConfig> segments = SegmentConfig.parse(segmentConfigsJson.get());
        methodToSegment = new HashMap<>();
        for (SegmentConfig s : segments) {
            methodToSegment.put(s.aiMethod,    s.name);
            methodToSegment.put(s.humanMethod, s.name);
        }

        resolvedFilterField    = filterField  != null ? filterField.get()  : null;
        resolvedFilterValue    = filterValue  != null ? filterValue.get()  : null;
        resolvedImageNameField = imageNameField.get();
    }

    @ProcessElement
    public void processElement(ProcessContext ctx) {
        TableRow row    = ctx.element();
        String   method = (String) row.get("method");
        String   segment = methodToSegment.get(method);

        if (segment == null) {
            return; // method not in any configured segment — drop
        }

        String keyId     = (String) row.get("key_id");
        String decrypted = BarricadeEncryptionUtil.decrypt(keyId, (String) row.get("payload"));

        // Optional payload-field filter (typically applied to human rows).
        if (resolvedFilterField != null && !resolvedFilterField.isBlank()) {
            String actual = JsonFieldExtractor.extractField(decrypted, resolvedFilterField);
            if (!resolvedFilterValue.equals(actual)) {
                LOG.debug("Skipping row: payload.{} = '{}', expected '{}'",
                        resolvedFilterField, actual, resolvedFilterValue);
                return;
            }
        }

        String imageName = JsonFieldExtractor.extractField(decrypted, resolvedImageNameField);
        if (imageName == null || imageName.isBlank()) {
            LOG.warn("Skipping row: field '{}' is absent or null for key_id={} method={}",
                    resolvedImageNameField, keyId, method);
            return;
        }

        // Normalise case and whitespace so AI and human rows for the same image
        // always produce the same key regardless of how each system stores the name.
        String imageKey = imageName.trim().toLowerCase();
        LOG.debug("DecryptAndKey: key_id={} method={} imageKey={} segment={}",
                keyId, method, imageKey, segment);

        // Key format: "imageId::segment"
        ctx.output(KV.of(imageKey + "::" + segment, row));
    }
}
