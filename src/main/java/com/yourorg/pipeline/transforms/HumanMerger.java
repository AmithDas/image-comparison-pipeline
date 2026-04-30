package com.yourorg.pipeline.transforms;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.yourorg.pipeline.config.SegmentConfig;
import com.yourorg.pipeline.util.BarricadeEncryptionUtil;
import com.yourorg.pipeline.util.PayloadParser;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.Map;

/**
 * Merges multiple human sub-type {@link GenericRecord}s into a single record
 * whose {@code payload} is the top-level union of all sub-type JSON payloads.
 *
 * <p>Rules:
 * <ul>
 *   <li>{@code created_at}: earliest across all sub-types.</li>
 *   <li>{@code key_id}: taken from the first sub-type (alphabetical by sub-type name).</li>
 *   <li>Field collision: first sub-type wins; a WARN is logged if values differ.</li>
 *   <li>The merged payload is re-encrypted before being stored so downstream
 *       transforms ({@link FlattenAndCompareFn}, {@link OrphanCompareFn}) can
 *       decrypt it uniformly.</li>
 * </ul>
 */
public final class HumanMerger {

    private static final Logger LOG = LoggerFactory.getLogger(HumanMerger.class);

    private HumanMerger() {}

    /**
     * Merges the given sub-type records into one {@link GenericRecord}.
     *
     * @param imageId        the image identifier (written into the returned record)
     * @param segment        the segment name
     * @param humanBySubType map of sub-type name → GenericRecord (payload is encrypted)
     * @param seg            the SegmentConfig (provides payloadFormat)
     * @param payloadSchema  Avro schema for the returned record
     * @return merged GenericRecord with re-encrypted payload
     */
    public static GenericRecord merge(String imageId,
                                       String segment,
                                       Map<String, GenericRecord> humanBySubType,
                                       SegmentConfig seg,
                                       Schema payloadSchema) {
        // Sort sub-types for deterministic key_id and field-collision resolution.
        String[] subTypeNames = humanBySubType.keySet().stream()
                .sorted()
                .toArray(String[]::new);

        String keyId    = str(humanBySubType.get(subTypeNames[0]).get("key_id"));
        String earliest = humanBySubType.values().stream()
                .map(r -> str(r.get("created_at")))
                .filter(s -> s != null)
                .min(Comparator.naturalOrder())
                .orElse(null);

        JsonObject merged = new JsonObject();

        for (String subType : subTypeNames) {
            GenericRecord rec       = humanBySubType.get(subType);
            String        encrypted = str(rec.get("payload"));
            String        decrypted = BarricadeEncryptionUtil.decrypt(keyId, encrypted);
            String        json      = resolveJson(decrypted, seg, subType);
            if (json == null) continue;

            JsonObject obj;
            try {
                obj = JsonParser.parseString(json).getAsJsonObject();
            } catch (Exception e) {
                LOG.warn("HumanMerger: could not parse JSON for subType={} imageId={}", subType, imageId);
                continue;
            }

            for (Map.Entry<String, JsonElement> entry : obj.entrySet()) {
                if (merged.has(entry.getKey())) {
                    if (!merged.get(entry.getKey()).equals(entry.getValue())) {
                        LOG.warn("HumanMerger: field '{}' collision for imageId={} segment={} "
                                + "— keeping value from sub-type '{}'",
                                entry.getKey(), imageId, segment, subTypeNames[0]);
                    }
                } else {
                    merged.add(entry.getKey(), entry.getValue());
                }
            }
        }

        // Re-encrypt the merged payload so FlattenAndCompareFn can decrypt normally.
        String reEncrypted = BarricadeEncryptionUtil.encrypt(keyId, merged.toString());

        GenericRecord result = new GenericData.Record(payloadSchema);
        result.put("image_id",     imageId);
        result.put("key_id",       keyId);
        result.put("payload_type", "human");
        result.put("payload",      reEncrypted);
        result.put("created_at",   earliest);
        return result;
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static String resolveJson(String decrypted, SegmentConfig seg, String subType) {
        if (seg.isIdRequestFormat()) {
            PayloadParser.Parsed p = PayloadParser.parse(decrypted);
            if (p == null) {
                LOG.warn("HumanMerger: could not parse id_request for subType={}", subType);
                return null;
            }
            return p.json();
        }
        return decrypted;
    }

    private static String str(Object o) {
        return o != null ? o.toString() : null;
    }
}
