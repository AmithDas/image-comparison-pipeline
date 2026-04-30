package com.yourorg.pipeline.transforms;

import com.google.api.services.bigquery.model.TableRow;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.yourorg.pipeline.config.SegmentConfig;
import com.yourorg.pipeline.util.BarricadeEncryptionUtil;
import com.yourorg.pipeline.util.JsonFieldExtractor;
import com.yourorg.pipeline.util.PayloadParser;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * Decrypts the {@code payload} of a source {@link TableRow}, resolves its segment
 * and side (AI vs human) from the {@code method} column, and emits
 * {@code KV<"imageId::segment", row>} for downstream co-grouping.
 *
 * <h3>Routing rules</h3>
 * <ul>
 *   <li><b>AI rows</b>: broadcast to every segment whose {@code aiMethod} matches.
 *       AI payloads are always plain JSON.</li>
 *   <li><b>Human rows</b>: routed to exactly one segment by checking
 *       {@code humanSubTypes[].discriminatorField} presence in the decrypted JSON.
 *       Rows with no matching segment/sub-type are silently dropped.</li>
 * </ul>
 *
 * <h3>Payload formats</h3>
 * <ul>
 *   <li><b>Plain JSON</b> (default): image name extracted via {@code imageNameField}.</li>
 *   <li><b>{@code id_request}</b>: payload has the form
 *       {@code id="<IMAGE_NAME>",request="<JSON>"}; image name comes from the
 *       {@code id=} field.  The field {@code _human_sub_type} is injected into the
 *       emitted row so {@link FilterAndPairFn} can route it to the correct sub-type.</li>
 * </ul>
 */
public class DecryptAndKeyFn extends DoFn<TableRow, KV<String, TableRow>> {

    private static final Logger LOG = LoggerFactory.getLogger(DecryptAndKeyFn.class);

    private final ValueProvider<String> firestoreCollection;
    private final ValueProvider<String> kmsKeyPath;
    private final ValueProvider<String> imageNameField;
    private final ValueProvider<String> segmentConfigsJson;
    private final ValueProvider<String> filterField;
    private final ValueProvider<String> filterValue;

    // Resolved in @Setup; transient so they are re-initialized on each worker.
    private transient Map<String, List<SegmentConfig>> methodToSegments;
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

    public static DecryptAndKeyFn forAi(ValueProvider<String> firestoreCollection,
                                         ValueProvider<String> kmsKeyPath,
                                         ValueProvider<String> imageNameField,
                                         ValueProvider<String> segmentConfigsJson) {
        return new DecryptAndKeyFn(
                firestoreCollection, kmsKeyPath, imageNameField, segmentConfigsJson, null, null);
    }

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
        methodToSegments = SegmentConfig.buildMethodToSegmentsMap(segments);

        resolvedFilterField    = filterField  != null ? filterField.get()  : null;
        resolvedFilterValue    = filterValue  != null ? filterValue.get()  : null;
        resolvedImageNameField = imageNameField.get();
    }

    @ProcessElement
    public void processElement(ProcessContext ctx) {
        TableRow row    = ctx.element();
        String   method = (String) row.get("method");
        List<SegmentConfig> candidates = methodToSegments.get(method);
        if (candidates == null) return;

        String keyId     = (String) row.get("key_id");
        String decrypted = BarricadeEncryptionUtil.decrypt(keyId, (String) row.get("payload"));

        for (SegmentConfig seg : candidates) {
            if (seg.aiMethod.equals(method)) {
                // AI row — broadcast to every segment that lists this aiMethod.
                // AI payloads are always plain JSON; no sub-type tagging needed.
                String imageKey = extractImageKeyPlainJson(decrypted, keyId, method, seg.name);
                if (imageKey == null) continue;
                ctx.output(KV.of(imageKey + "::" + seg.name, row));

            } else if (seg.humanMethod.equals(method)) {
                // Human row — resolve inner JSON, apply filter, route by discriminator.
                String json = resolveHumanJson(decrypted, seg, keyId, method);
                if (json == null) continue;

                if (!passesFilter(json)) continue;

                String subTypeName = resolveSubType(json, seg);
                if (subTypeName == null) continue;

                String imageKey = extractImageKey(decrypted, seg, keyId, method);
                if (imageKey == null) continue;

                TableRow emitted = row.clone();
                if (seg.requiresHumanMerge()) {
                    emitted.set("_human_sub_type", subTypeName);
                }
                ctx.output(KV.of(imageKey + "::" + seg.name, emitted));
            }
        }
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private String resolveHumanJson(String decrypted, SegmentConfig seg,
                                     String keyId, String method) {
        if (seg.isIdRequestFormat()) {
            PayloadParser.Parsed p = PayloadParser.parse(decrypted);
            if (p == null) {
                LOG.warn("Could not parse id_request payload for segment={} key_id={} method={}",
                        seg.name, keyId, method);
                return null;
            }
            return p.json();
        }
        return decrypted;
    }

    private String extractImageKey(String decrypted, SegmentConfig seg,
                                    String keyId, String method) {
        String name;
        if (seg.isIdRequestFormat()) {
            PayloadParser.Parsed p = PayloadParser.parse(decrypted);
            name = p != null ? p.imageId() : null;
        } else {
            name = JsonFieldExtractor.extractField(decrypted, resolvedImageNameField);
        }
        if (name == null || name.isBlank()) {
            LOG.warn("Skipping row: image name absent for key_id={} method={} segment={}",
                    keyId, method, seg.name);
            return null;
        }
        return name.trim().toLowerCase();
    }

    private String extractImageKeyPlainJson(String decrypted, String keyId,
                                             String method, String segName) {
        String name = JsonFieldExtractor.extractField(decrypted, resolvedImageNameField);
        if (name == null || name.isBlank()) {
            LOG.warn("Skipping AI row: field '{}' absent for key_id={} method={} segment={}",
                    resolvedImageNameField, keyId, method, segName);
            return null;
        }
        return name.trim().toLowerCase();
    }

    /**
     * Returns the sub-type name whose discriminatorField is present in the JSON.
     * Returns {@code "default"} for segments with no humanSubTypes.
     * Returns {@code null} if no sub-type matches (row is dropped).
     */
    private static String resolveSubType(String json, SegmentConfig seg) {
        if (!seg.requiresHumanMerge()) return "default";
        JsonObject obj;
        try {
            obj = JsonParser.parseString(json).getAsJsonObject();
        } catch (Exception e) {
            return null;
        }
        for (SegmentConfig.HumanSubType st : seg.humanSubTypes) {
            if (obj.has(st.discriminatorField)) {
                return st.name;
            }
        }
        return null;
    }

    private boolean passesFilter(String json) {
        if (resolvedFilterField == null || resolvedFilterField.isBlank()) return true;
        String actual = JsonFieldExtractor.extractField(json, resolvedFilterField);
        if (!resolvedFilterValue.equals(actual)) {
            LOG.debug("Skipping row: payload.{} = '{}', expected '{}'",
                    resolvedFilterField, actual, resolvedFilterValue);
            return false;
        }
        return true;
    }
}
