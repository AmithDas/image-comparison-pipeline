package com.yourorg.pipeline.transforms;

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
import java.util.LinkedHashMap;
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
        String[] subTypeNames = humanBySubType.keySet().stream()
                .sorted()
                .toArray(String[]::new);

        String keyId    = str(humanBySubType.get(subTypeNames[0]).get("key_id"));
        String earliest = humanBySubType.values().stream()
                .map(r -> str(r.get("created_at")))
                .filter(s -> s != null)
                .min(Comparator.naturalOrder())
                .orElse(null);

        // Build subType → discriminatorField lookup.
        Map<String, String> subTypeToDiscriminator = new LinkedHashMap<>();
        if (seg.humanSubTypes != null) {
            for (SegmentConfig.HumanSubType st : seg.humanSubTypes) {
                subTypeToDiscriminator.put(st.name, st.discriminatorField);
            }
        }

        // Decrypt and parse every sub-type payload up front.
        Map<String, JsonObject> parsedJsons = new LinkedHashMap<>();
        for (String subType : subTypeNames) {
            GenericRecord rec       = humanBySubType.get(subType);
            String        decrypted = BarricadeEncryptionUtil.decrypt(keyId, str(rec.get("payload")));
            String        json      = resolveJson(decrypted, seg, subType);
            if (json == null) continue;
            try {
                parsedJsons.put(subType, JsonParser.parseString(json).getAsJsonObject());
            } catch (Exception e) {
                LOG.warn("HumanMerger: could not parse JSON for subType={} imageId={}", subType, imageId);
            }
        }

        // Base sub-type: prefer explicit isBase=true in config; fall back to the sub-type
        // whose discriminatorField is a JSON array; last resort: alphabetically first.
        String baseSubType = null;
        if (seg.humanSubTypes != null) {
            for (SegmentConfig.HumanSubType st : seg.humanSubTypes) {
                if (st.isBase) {
                    baseSubType = st.name;
                    break;
                }
            }
        }
        if (baseSubType == null) {
            for (String subType : subTypeNames) {
                JsonObject obj  = parsedJsons.get(subType);
                String     disc = subTypeToDiscriminator.get(subType);
                if (obj != null && disc != null && obj.has(disc) && obj.get(disc).isJsonArray()) {
                    baseSubType = subType;
                    break;
                }
            }
        }
        if (baseSubType == null) baseSubType = subTypeNames[0];

        JsonObject merged = parsedJsons.getOrDefault(baseSubType, new JsonObject()).deepCopy();

        // Append each non-base sub-type's discriminatorField into the merged JSON.
        for (String subType : subTypeNames) {
            if (subType.equals(baseSubType)) continue;
            JsonObject obj  = parsedJsons.get(subType);
            String     disc = subTypeToDiscriminator.get(subType);
            if (obj == null || disc == null) continue;
            if (obj.has(disc)) {
                merged.add(disc, obj.get(disc));
            } else {
                LOG.warn("HumanMerger: subType='{}' missing discriminatorField='{}' for imageId={}",
                        subType, disc, imageId);
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
