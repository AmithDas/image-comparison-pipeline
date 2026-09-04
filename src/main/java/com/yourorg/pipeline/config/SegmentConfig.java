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

    /**
     * Array fields that are concatenated across all human sub-types during the merge (via
     * {@code HumanMerger}), and across cases during cross-case merge (via
     * {@code FilterAndPairFn.mergeAcrossCases}), rather than one side's array wholesale-
     * replacing the other's. Useful when the same array (e.g. {@code documentProofs})
     * appears in more than one sub-type/case payload and all items must be preserved.
     * <p><b>Not configurable via the DAG JSON</b> — populated in code by
     * {@link #applyMergeFieldDefaults}, by segment name, the same way
     * {@code FlattenAndCompareFn.ARRAY_MATCH_KEYS} is a Java constant rather than a config
     * knob. These field lists are tightly coupled to the real payload schema and to
     * {@code ARRAY_MATCH_KEYS}/{@code SOFT_MATCH_ARRAYS}, so keeping them in code (reviewed
     * and tested like any other logic change) avoids the DAG config and the Java merge code
     * silently drifting out of sync with each other.
     */
    public List<String> mergeArrayFields;

    /**
     * Nested objects (by dot-notation path) that should NOT be merged field-by-field
     * during cross-case merge, because they represent one holistic record rather than a
     * set of independently-collidable fields (e.g. {@code creditReportHeader} — a single
     * credit report header, not a collection of line items). For a configured path:
     * <ul>
     *   <li>Every field NOT listed as one of its "slot" keys is taken wholesale from
     *       whichever case is latest — not blended field-by-field.</li>
     *   <li>Each listed "slot" key (e.g. {@code dateOfBirthRequested},
     *       {@code currentNameRequested}, {@code socialSecurityNumberRequested} — each a
     *       self-contained dispute-request record) is resolved independently: the latest
     *       case's version wins if present; otherwise an earlier case's version is
     *       carried forward rather than lost.</li>
     * </ul>
     * An empty slot list is valid and means "fully atomic, no exceptions" — every field is
     * taken wholesale from the latest case (e.g. {@code resultOfInvestigation},
     * {@code requestor}: self-contained records with no slot-like sub-objects of their own).
     * <p><b>Not configurable via the DAG JSON</b> — see {@link #mergeArrayFields}.
     */
    public Map<String, List<String>> atomicObjectFields;

    /**
     * For a {@code mergeArrayFields} array path, the name of a field whose presence marks an
     * item as "disputed" and should win a same-key item-level collision outright — regardless
     * of which case is latest. Example: {@code addresses} items are keyed by {@code addressType}
     * ({@code Current}, {@code Former1}, ...); a non-disputed submission of a slot has no
     * {@code addressRequested} field, while a disputed one does. When two cases' versions of the
     * same slot differ, the version WITH {@code addressRequested} wins over the version without
     * it, even if the non-disputed one is the more recent submission — a case actively disputing
     * an address should not be silently overwritten by another case that merely re-submitted
     * that slot unchanged. Falls back to the normal latest-{@code created_at}-wins rule when
     * both sides have the priority field, both lack it, or no priority field is configured for
     * that array path at all.
     * <p><b>Not configurable via the DAG JSON</b> — see {@link #mergeArrayFields}.
     */
    public Map<String, String> arrayItemPriorityField;

    /**
     * For a {@code mergeArrayFields} array path, overrides the item-pairing key used during
     * cross-case MERGE only — independent of {@code FlattenAndCompareFn.ARRAY_MATCH_KEYS},
     * which drives AI-vs-human comparison pairing for the same array. Falls back to
     * {@code ARRAY_MATCH_KEYS} when a path has no entry here.
     * <p>Example: {@code addresses} is compared against AI by content
     * ({@code streetNumber-postalCode}), but merged across cases by {@code addressType}
     * (Current, Former1, Former2, ...) — every case submits the same fixed set of address
     * slots, and a disputed slot's street/postal can itself be the thing under correction, so
     * content can't reliably identify "the same slot" across cases for merge purposes.
     * <p><b>Not configurable via the DAG JSON</b> — see {@link #mergeArrayFields}.
     */
    public Map<String, String> mergeItemKeyField;

    // ── Hardcoded merge-field defaults, by segment name ─────────────────────────
    // These four maps are the single source of truth for mergeArrayFields,
    // atomicObjectFields, arrayItemPriorityField, and mergeItemKeyField — deliberately NOT
    // exposed in the DAG JSON. See the javadoc on mergeArrayFields above for why.

    private static final Map<String, List<String>> DEFAULT_MERGE_ARRAY_FIELDS = Map.of(
            "main", List.of(
                    "tradelines",
                    "addresses",
                    "collections",
                    "bankruptcies",
                    "fileInquiries",
                    "employments",
                    "alsoKnownAs",
                    "otherNames",
                    "otherIdentifications",
                    "documentProofs",
                    "nonreportedAddresses",
                    "nonreportedDatesOfBirth",
                    "nonreportedEmployments",
                    "nonreportedInquiries",
                    "nonreportedNames",
                    "nonreportedPublicRecords",
                    "nonreportedSsns",
                    "nonreportedPhoneNumbers",
                    "nonreportedTrades",
                    "nonreportedCollections"),
            "authanddocreview", List.of("documentProofs")
    );

    private static final Map<String, Map<String, List<String>>> DEFAULT_ATOMIC_OBJECT_FIELDS = Map.of(
            "main", Map.of(
                    "creditReportHeader", List.of(
                            "dateOfBirthRequested",
                            "currentNameRequested",
                            "socialSecurityNumberRequested"),
                    // Self-contained records with no slot-like sub-objects of their own —
                    // fully atomic, taken wholesale from the latest case.
                    "resultOfInvestigation", List.of(),
                    "requestor", List.of())
    );

    private static final Map<String, Map<String, String>> DEFAULT_MERGE_ITEM_KEY_FIELD = Map.of(
            "main", Map.of("addresses", "addressType")
    );

    private static final Map<String, Map<String, String>> DEFAULT_ARRAY_ITEM_PRIORITY_FIELD = Map.of(
            "main", Map.of("addresses", "addressRequested")
    );

    /**
     * Populates {@link #mergeArrayFields}, {@link #atomicObjectFields},
     * {@link #arrayItemPriorityField}, and {@link #mergeItemKeyField} from the hardcoded
     * defaults above, by {@link #name}. Called automatically by {@link #parse}. A segment with
     * no entry in a given default map simply gets nothing for that field (e.g.
     * {@code authanddocreview} has no {@code atomicObjectFields}/{@code mergeItemKeyField}/
     * {@code arrayItemPriorityField}) — not an error.
     */
    private void applyMergeFieldDefaults() {
        this.mergeArrayFields         = DEFAULT_MERGE_ARRAY_FIELDS.get(name);
        this.atomicObjectFields       = DEFAULT_ATOMIC_OBJECT_FIELDS.get(name);
        this.mergeItemKeyField        = DEFAULT_MERGE_ITEM_KEY_FIELD.get(name);
        this.arrayItemPriorityField   = DEFAULT_ARRAY_ITEM_PRIORITY_FIELD.get(name);
    }

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

        /**
         * Optional field that must be <em>absent</em> from the payload for this sub-type
         * to match.  Use when two sub-types share the same {@code discriminatorField} but
         * one of them also contains an additional top-level key that the other does not.
         * Example: both authentication and docreview payloads contain {@code documentProofs},
         * but only authentication also contains {@code verifiedData}.  Setting
         * {@code negativeDiscriminatorField: "verifiedData"} on docreview ensures it only
         * matches payloads that do <em>not</em> have {@code verifiedData}, making the
         * ordering of {@code humanSubTypes} in config irrelevant.
         */
        public String negativeDiscriminatorField;

        /**
         * Name of the JSON array field in this sub-type's payload that requires
         * {@code authenticationType} tagging before the merge (e.g. {@code "documentProofs"}).
         * Leave {@code null} to skip the transformation for this sub-type.
         */
        public String docProofsField;

        /**
         * The {@code authenticationType} value to stamp on each item in {@link #docProofsField}.
         * Supported values:
         * <ul>
         *   <li>{@code "Authentication"} — adds {@code authenticationType:"Authentication"} to
         *       every item in the array.</li>
         *   <li>{@code "Document_Review"} — removes items already tagged
         *       {@code "Authentication"}, then adds {@code authenticationType:"Document_Review"}
         *       to remaining items that lack it.</li>
         * </ul>
         * Ignored when {@link #docProofsField} is {@code null}.
         */
        public String docProofsLabel;

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
        for (SegmentConfig c : configs) {
            c.applyMergeFieldDefaults();
        }
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
