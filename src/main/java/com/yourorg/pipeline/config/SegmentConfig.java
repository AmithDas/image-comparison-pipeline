package com.yourorg.pipeline.config;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import java.io.Serializable;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Configuration for a single pipeline segment.
 *
 * <p>A segment groups payloads that share a pair of {@code method} column values —
 * one for the AI payload and one for the human payload.  All segments are processed
 * by a single Dataflow job; results are written to one shared comparison table with
 * a {@code segment} column that identifies the origin.
 *
 * <h3>Simple segment (one human payload per image)</h3>
 * <pre>
 * {"name": "main", "aiMethod": "aimetadata_v2", "humanMethod": "controller.SubmitMain"}
 * </pre>
 *
 * <h3>Multi-sub-type segment (two human payloads merged before comparison)</h3>
 * <pre>
 * {
 *   "name": "combined",
 *   "aiMethod": "aimetadata",
 *   "humanMethod": "controller.SubmitDispute",
 *   "payloadFormat": "id_request",
 *   "humanSubTypes": [
 *     {"name": "auth",      "discriminatorField": "verifiedData"},
 *     {"name": "docreview", "discriminatorField": "documentDetails"}
 *   ]
 * }
 * </pre>
 *
 * <p>When {@code humanSubTypes} is present, all listed sub-types must arrive before
 * the human side is considered complete.  Their payloads are deep-merged into one
 * record before pairing with the AI payload.
 *
 * <p>When {@code payloadFormat} is {@code "id_request"}, the decrypted human payload
 * has the form {@code id="<IMAGE_NAME>",request="<JSON>"} and must be parsed by
 * {@link com.yourorg.pipeline.util.PayloadParser} before JSON extraction.
 */
public class SegmentConfig implements Serializable {

    public String           name;
    public String           aiMethod;
    public String           humanMethod;

    /** {@code "id_request"} or {@code null} (plain JSON). */
    public String           payloadFormat;

    /** Non-null when multiple human payloads must be merged before comparison. */
    public List<HumanSubType> humanSubTypes;

    /**
     * Cross-field name mappings for this segment.
     * Each entry maps a human payload field name to the corresponding AI payload field name
     * so they are compared against each other rather than emitted as unmatched orphans.
     * The human field name is used as {@code field_name} in the output.
     */
    public List<FieldMapping> fieldMappings;

    /**
     * Overrides the {@code segment_type} column value for specific root nodes.
     * When a flattened field's root node (e.g. {@code verifiedData},
     * {@code consumerInformation}) matches an entry here, the configured label
     * is written instead of the raw root name.
     * Root nodes with no entry keep the raw root name as {@code segment_type}.
     */
    public List<SegmentTypeMapping> segmentTypeMappings;

    // Gson requires a no-arg constructor for deserialization.
    public SegmentConfig() {
        this(null, null, null, null, null);
    }

    public SegmentConfig(String name, String aiMethod, String humanMethod) {
        this(name, aiMethod, humanMethod, null, null);
    }

    public SegmentConfig(String name, String aiMethod, String humanMethod,
                         String payloadFormat, List<HumanSubType> humanSubTypes) {
        this.name          = name;
        this.aiMethod      = aiMethod;
        this.humanMethod   = humanMethod;
        this.payloadFormat = payloadFormat;
        this.humanSubTypes = humanSubTypes;
    }

    public boolean requiresHumanMerge() {
        return humanSubTypes != null && humanSubTypes.size() > 1;
    }

    public boolean isIdRequestFormat() {
        return "id_request".equals(payloadFormat);
    }

    // ── Nested types ──────────────────────────────────────────────────────────

    public static class SegmentTypeMapping implements Serializable {
        /** Root node names (e.g. {@code "verifiedData"}, {@code "consumerInformation"}) that map to this label. */
        public List<String> roots;
        /** Label written to the {@code segment_type} column for these root nodes. */
        public String segmentType;

        public SegmentTypeMapping() {}

        public SegmentTypeMapping(List<String> roots, String segmentType) {
            this.roots       = roots;
            this.segmentType = segmentType;
        }
    }

    public static class FieldMapping implements Serializable {
        /** Field name in the human payload. Used as {@code field_name} in comparison output. */
        public String humanField;
        /** Corresponding field name in the AI payload. */
        public String aiField;

        public FieldMapping() {}

        public FieldMapping(String humanField, String aiField) {
            this.humanField = humanField;
            this.aiField    = aiField;
        }

        @Override
        public String toString() {
            return humanField + " ↔ " + aiField;
        }
    }

    public static class HumanSubType implements Serializable {

        public String name;
        /** Top-level JSON key whose presence identifies this sub-type in the human payload. */
        public String discriminatorField;
        /**
         * When {@code true}, this sub-type's full payload is used as the merge base.
         * All other sub-types contribute only their {@code discriminatorField} value on top.
         * At most one sub-type should set this to {@code true}; if none do, the merger
         * falls back to heuristic detection (array-valued discriminatorField wins).
         */
        public boolean isBase;

        public HumanSubType() {}

        public HumanSubType(String name, String discriminatorField, boolean isBase) {
            this.name              = name;
            this.discriminatorField = discriminatorField;
            this.isBase            = isBase;
        }

        @Override
        public String toString() {
            return "HumanSubType{name=" + name + ", discriminatorField=" + discriminatorField + "}";
        }
    }

    // ── Parsing ───────────────────────────────────────────────────────────────

    public static List<SegmentConfig> parse(String json) {
        if (json == null || json.isBlank()) {
            throw new IllegalArgumentException(
                    "--segmentConfigs must be a non-empty JSON array");
        }
        Type listType = new TypeToken<List<SegmentConfig>>() {}.getType();
        List<SegmentConfig> configs = new Gson().fromJson(json, listType);
        if (configs == null || configs.isEmpty()) {
            throw new IllegalArgumentException(
                    "--segmentConfigs parsed to an empty list — at least one segment is required");
        }
        validate(configs);
        return configs;
    }

    // ── Map builders ──────────────────────────────────────────────────────────

    /**
     * Builds {@code method → list of SegmentConfigs} covering both AI and human methods.
     * A method may map to multiple segments when segments share the same method values.
     */
    public static Map<String, List<SegmentConfig>> buildMethodToSegmentsMap(
            List<SegmentConfig> segments) {
        Map<String, List<SegmentConfig>> map = new HashMap<>();
        for (SegmentConfig s : segments) {
            map.computeIfAbsent(s.aiMethod,    k -> new ArrayList<>()).add(s);
            map.computeIfAbsent(s.humanMethod, k -> new ArrayList<>()).add(s);
        }
        return map;
    }

    /**
     * Builds {@code method → "ai" or "human"} for use in FilterAndPairFn.
     * When multiple segments share a method, the side classification is the same
     * for all of them so a flat map is sufficient.
     */
    public static Map<String, String> buildMethodToSideMap(List<SegmentConfig> segments) {
        Map<String, String> map = new HashMap<>();
        for (SegmentConfig s : segments) {
            map.put(s.aiMethod,    "ai");
            map.put(s.humanMethod, "human");
        }
        return map;
    }

    /**
     * Builds {@code segmentName → SegmentConfig} for fast lookup by name.
     */
    public static Map<String, SegmentConfig> buildNameMap(List<SegmentConfig> segments) {
        Map<String, SegmentConfig> map = new HashMap<>();
        for (SegmentConfig s : segments) {
            map.put(s.name, s);
        }
        return map;
    }

    // ── Validation ────────────────────────────────────────────────────────────

    private static void validate(List<SegmentConfig> configs) {
        for (SegmentConfig c : configs) {
            if (blank(c.name)) {
                throw new IllegalArgumentException("SegmentConfig missing 'name'");
            }
            if (blank(c.aiMethod)) {
                throw new IllegalArgumentException(
                        "SegmentConfig '" + c.name + "' missing 'aiMethod'");
            }
            if (blank(c.humanMethod)) {
                throw new IllegalArgumentException(
                        "SegmentConfig '" + c.name + "' missing 'humanMethod'");
            }
            if (c.humanSubTypes != null) {
                for (HumanSubType st : c.humanSubTypes) {
                    if (blank(st.name)) {
                        throw new IllegalArgumentException(
                                "SegmentConfig '" + c.name + "' has a humanSubType missing 'name'");
                    }
                    if (blank(st.discriminatorField)) {
                        throw new IllegalArgumentException(
                                "SegmentConfig '" + c.name + "' subType '" + st.name
                                        + "' missing 'discriminatorField'");
                    }
                }
            }
        }
    }

    private static boolean blank(String s) {
        return s == null || s.isBlank();
    }

    @Override
    public String toString() {
        return "SegmentConfig{name=" + name
                + ", aiMethod=" + aiMethod
                + ", humanMethod=" + humanMethod
                + ", payloadFormat=" + payloadFormat
                + ", humanSubTypes=" + humanSubTypes + "}";
    }
}
