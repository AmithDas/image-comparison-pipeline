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
 *       Rows with no matching segment/sub-type are silently dropped. The row's plain
 *       {@code case_id} column (not inside the encrypted payload) is carried through
 *       unchanged so {@link FilterAndPairFn} can group multiple cases sharing one
 *       image's AI history separately.</li>
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
                // Resolve inner JSON and image key.
                // For id_request format (explicit via payloadFormat, or auto-detected when
                // humanSubTypes is defined), both come from PayloadParser.
                // For plain JSON, innerJson == decrypted and imageKey comes from imageNameField.
                boolean tryIdRequest = seg.isIdRequestFormat() || hasSubTypes(seg);
                PayloadParser.Parsed parsed = tryIdRequest ? PayloadParser.parse(decrypted) : null;
                boolean isIdRequest = (parsed != null);

                if (seg.isIdRequestFormat() && !isIdRequest) {
                    LOG.warn("Could not parse id_request payload for segment={} key_id={} method={}",
                            seg.name, keyId, method);
                    continue;
                }

                String innerJson = isIdRequest ? parsed.json() : decrypted;

                if (!passesFilter(innerJson)) continue;

                String subTypeName = resolveSubType(innerJson, seg);
                if (subTypeName == null) continue;

                String imageKey = isIdRequest
                        ? parsed.imageId()
                        : JsonFieldExtractor.extractField(decrypted, resolvedImageNameField);
                if (imageKey == null || imageKey.isBlank()) {
                    LOG.warn("Skipping row: image name absent for key_id={} method={} segment={}",
                            keyId, method, seg.name);
                    continue;
                }

                TableRow emitted = row.clone();
                emitted.set("_human_sub_type", subTypeName);
                Object caseId = row.get("case_id");
                if (caseId != null) emitted.set("case_id", caseId.toString());
                ctx.output(KV.of(imageKey.trim().toLowerCase() + "::" + seg.name, emitted));
            }
        }
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static boolean hasSubTypes(SegmentConfig seg) {
        return seg.humanSubTypes != null && !seg.humanSubTypes.isEmpty();
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
        if (seg.humanSubTypes == null || seg.humanSubTypes.isEmpty()) return "default";
        JsonObject obj;
        try {
            obj = JsonParser.parseString(json).getAsJsonObject();
        } catch (Exception e) {
            LOG.warn("Failed to parse human payload JSON for sub-type resolution: {}", e.getMessage());
            return null;
        }
        for (SegmentConfig.HumanSubType st : seg.humanSubTypes) {
            if (!obj.has(st.discriminatorField)) continue;
            if (st.negativeDiscriminatorField != null && obj.has(st.negativeDiscriminatorField)) {
                LOG.debug("Sub-type '{}' skipped — negativeDiscriminatorField '{}' is present",
                        st.name, st.negativeDiscriminatorField);
                continue;
            }
            LOG.debug("Resolved sub-type '{}' via discriminator '{}'", st.name, st.discriminatorField);
            return st.name;
        }
        LOG.warn("No humanSubType discriminator matched in payload. Segment has sub-types: {}",
                seg.humanSubTypes);
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
