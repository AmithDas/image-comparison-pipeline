package com.yourorg.pipeline.transforms;

import com.google.api.services.bigquery.model.TableRow;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.yourorg.pipeline.config.SegmentConfig;
import com.yourorg.pipeline.util.BarricadeEncryptionUtil;
import com.yourorg.pipeline.util.JsonFieldExtractor;
import com.yourorg.pipeline.util.JsonFieldExtractor.FieldValue;
import com.yourorg.pipeline.util.TimestampUtil;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Flattens both JSON payloads in a matched pair and emits one {@link TableRow}
 * per field value, including a {@code segment} column for downstream filtering.
 *
 * <h3>Pair key format</h3>
 * {@code "imageId::segment"} — set by {@link FilterAndPairFn}. There is at most one
 * comparison per {@code imageId::segment} group per run — always against the
 * group's single latest AI payload, only when the human-or-AI content actually
 * changed since the last comparison (no more per-iteration replay). {@code case_id}
 * is not part of the key since human payloads for every case sharing an
 * image+segment are merged into one consolidated record before comparison (see
 * {@link FilterAndPairFn#mergeAcrossCases}). The {@code case_id} written to each
 * output row is instead resolved per field from provenance metadata embedded in
 * the merged payload JSON — see {@link #resolveCaseId}. {@code ai_iteration} in
 * each output row is a comparison-version counter (see
 * {@code human.get("comparison_version")}), not a replay index — a superseded
 * comparison's rows are left in place, not deleted or hidden;
 * {@code comparison_results} is plain append-only and can hold more than one
 * comparison per group over time.
 *
 * <h3>Array comparison</h3>
 * <ul>
 *   <li><b>Keyed arrays</b> ({@code ARRAY_MATCH_KEYS}): matched by composite key.</li>
 *   <li><b>Positional arrays</b>: compared position-by-position.</li>
 *   <li><b>Scalar fields</b>: one row, {@code array_key} is {@code null}.</li>
 * </ul>
 *
 * <h3>AI field aliases</h3>
 * Per-segment {@code aiFieldAliases} in {@link SegmentConfig} rename AI payload
 * fields (dot-notation prefix) to the corresponding human field name before
 * comparison.  For example, {@code "documentType" → "document"} causes the AI
 * field {@code documentType} (and any nested path like {@code documentType.code})
 * to be compared against the human field {@code document}.
 *
 * <h3>String comparison</h3>
 * {@code is_match} is computed on plaintext (case-insensitive).
 * Values are re-encrypted before writing to BigQuery.
 */
public class FlattenAndCompareFn
        extends DoFn<KV<String, KV<GenericRecord, GenericRecord>>, TableRow> {

    private static final Logger LOG = LoggerFactory.getLogger(FlattenAndCompareFn.class);

    // ── Array match keys ──────────────────────────────────────────────────────
    // "addresses" is keyed by content (streetNumber-postalCode) for AI-vs-human comparison
    // purposes. The cross-case MERGE, however, needs a different pairing key — every case
    // submits the same fixed set of address slots (Current, Former1, Former2, ...), and a
    // disputed slot's street/postal can itself be the thing under correction, so content can't
    // reliably identify "the same slot" across cases there. See
    // SegmentConfig.mergeItemKeyField, which overrides this key with "addressType" for the
    // merge only (FilterAndPairFn.mergeArrayItems), independent of this comparison key.

    static final Map<String, String> ARRAY_MATCH_KEYS = Map.ofEntries(
            Map.entry("tradelines",                                                   "accountNumber-customerNumber-dateOpened"),
            Map.entry("tradelines.tradelineRequested.disputeCodes",                    "code"),
            Map.entry("addresses",                                                     "streetNumber-postalCode"),
            Map.entry("addresses.addressRequested.disputeCodes",                       "code"),
            Map.entry("collections",                                                   "accountNumber-customerNumber-dateAssigned"),
            Map.entry("collections.collectionRequested.disputeCodes",                  "code"),
            Map.entry("bankruptcies",                                                  "caseNumber-courtCustomerOrder-dateFiled"),
            Map.entry("bankruptcies.bankruptcyRequested.disputeCodes",                 "code"),
            Map.entry("creditReportHeader",                                            "customerNumber"),
            Map.entry("creditReportHeader.dateOfBirthRequested.disputeCodes",          "code"),
            Map.entry("creditReportHeader.currentNameRequested.disputeCodes",          "code"),
            Map.entry("creditReportHeader.socialSecurityNumberRequested.disputeCodes", "code"),
            Map.entry("fileInquiries",                                                 "customerNumber-dateOfInquiry-endUserDescriptionOrAcisCaseNumber"),
            Map.entry("fileInquiries.fileInquiryRequested.disputeCodes",               "code"),
            Map.entry("employments",                                                   "employmentType-employer-occupation"),
            Map.entry("employments.employmentRequested.disputeCodes",                  "code"),
            Map.entry("alsoKnownAs",                                                   "firstName-lastName-middleName"),
            Map.entry("alsoKnownAs.alsoKnownAsRequested.disputeCodes",                 "code"),
            Map.entry("otherNames",                                                    "firstName-lastName-middleName"),
            Map.entry("otherNames.otherNameRequested.disputeCodes",                    "code"),
            Map.entry("otherIdentifications",                                          "typeCode-identificationNumber"),
            Map.entry("otherIdentifications.otherIdentificationRequested.disputeCodes","code"),
            Map.entry("nonreportedAddresses",                                          "streetNumber-zipCode"),
            Map.entry("nonreportedAddresses.disputeCodes",                             "code"),
            Map.entry("nonreportedAddresses.consumerCommunications",                   "code"),
            Map.entry("nonreportedDatesOfBirth",                                       "nonreportedDateOfBirth"),
            Map.entry("nonreportedDatesOfBirth.disputeCodes",                          "code"),
            Map.entry("nonreportedDatesOfBirth.consumerCommunications",                "code"),
            Map.entry("nonreportedEmployments",                                        "employer-occupation"),
            Map.entry("nonreportedEmployments.disputeCodes",                           "code"),
            Map.entry("nonreportedEmployments.consumerCommunications",                 "code"),
            Map.entry("nonreportedInquiries",                                          "date-memberName"),
            Map.entry("nonreportedInquiries.disputeCodes",                             "code"),
            Map.entry("nonreportedInquiries.consumerCommunications",                   "code"),
            Map.entry("nonreportedNames",                                              "firstName-lastName-middleName"),
            Map.entry("nonreportedNames.disputeCodes",                                 "code"),
            Map.entry("nonreportedNames.consumerCommunications",                       "code"),
            Map.entry("nonreportedPublicRecords",                                      "filedDate-caseNumber"),
            Map.entry("nonreportedPublicRecords.disputeCodes",                         "code"),
            Map.entry("nonreportedPublicRecords.consumerCommunications",               "code"),
            Map.entry("nonreportedSsns",                                               "nonreportedSsn"),
            Map.entry("nonreportedSsns.disputeCodes",                                  "code"),
            Map.entry("nonreportedSsns.consumerCommunications",                        "code"),
            Map.entry("nonreportedPhoneNumbers",                                       "nonreportedPhoneNumber"),
            Map.entry("nonreportedPhoneNumbers.disputeCodes",                          "code"),
            Map.entry("nonreportedPhoneNumbers.consumerCommunications",                "code"),
            Map.entry("nonreportedTrades",                                             "accountNumber-customerName-openedDate"),
            Map.entry("nonreportedTrades.disputeCodes",                                "code"),
            Map.entry("nonreportedTrades.consumerCommunications",                      "code"),
            Map.entry("nonreportedCollections",                                        "clientName-accountNumber"),
            Map.entry("nonreportedCollections.disputeCodes",                           "code"),
            Map.entry("nonreportedCollections.consumerCommunications",                 "code"),
            Map.entry("documentProofs",                                                "authenticationType-document")
    );

    // ── Soft-match arrays ─────────────────────────────────────────────────────
    // Maps array path → parent keyed-array path.
    //
    // Arrays listed here use exact-first, positional-fallback matching WITHIN
    // each parent group:
    //   1. Items whose keys exist on both sides are paired as exact matches.
    //   2. Remaining items (sorted by key) are paired positionally — so a human
    //      code 007 pairs with AI code 005 rather than producing two orphan rows.
    //   3. Any surplus items become orphan rows (null on the other side).
    //
    // Combined key format: {parent}-{aiCode}-{humanCode}
    //   exact match:  acc-cust-date-007-007
    //   fallback pair: acc-cust-date-005-007
    //   AI orphan:    acc-cust-date-005-null
    //   human orphan: acc-cust-date-null-007
    //
    // Each key must also appear in ARRAY_MATCH_KEYS.
    static final Map<String, String> SOFT_MATCH_ARRAYS = Map.ofEntries(
            Map.entry("tradelines.tradelineRequested.disputeCodes",                    "tradelines"),
            Map.entry("collections.collectionRequested.disputeCodes",                  "collections"),
            Map.entry("addresses.addressRequested.disputeCodes",                       "addresses"),
            Map.entry("bankruptcies.bankruptcyRequested.disputeCodes",                 "bankruptcies"),
            Map.entry("creditReportHeader.dateOfBirthRequested.disputeCodes",          "creditReportHeader"),
            Map.entry("creditReportHeader.currentNameRequested.disputeCodes",          "creditReportHeader"),
            Map.entry("creditReportHeader.socialSecurityNumberRequested.disputeCodes", "creditReportHeader"),
            Map.entry("fileInquiries.fileInquiryRequested.disputeCodes",               "fileInquiries"),
            Map.entry("employments.employmentRequested.disputeCodes",                  "employments"),
            Map.entry("alsoKnownAs.alsoKnownAsRequested.disputeCodes",                 "alsoKnownAs"),
            Map.entry("otherNames.otherNameRequested.disputeCodes",                    "otherNames"),
            Map.entry("otherIdentifications.otherIdentificationRequested.disputeCodes","otherIdentifications"),
            Map.entry("nonreportedAddresses.disputeCodes",                             "nonreportedAddresses"),
            Map.entry("nonreportedDatesOfBirth.disputeCodes",                          "nonreportedDatesOfBirth"),
            Map.entry("nonreportedEmployments.disputeCodes",                           "nonreportedEmployments"),
            Map.entry("nonreportedInquiries.disputeCodes",                             "nonreportedInquiries"),
            Map.entry("nonreportedNames.disputeCodes",                                 "nonreportedNames"),
            Map.entry("nonreportedPublicRecords.disputeCodes",                         "nonreportedPublicRecords"),
            Map.entry("nonreportedSsns.disputeCodes",                                  "nonreportedSsns"),
            Map.entry("nonreportedPhoneNumbers.disputeCodes",                          "nonreportedPhoneNumbers"),
            Map.entry("nonreportedTrades.disputeCodes",                                "nonreportedTrades"),
            Map.entry("nonreportedCollections.disputeCodes",                           "nonreportedCollections")
    );

    // ── Constructor ───────────────────────────────────────────────────────────

    private final ValueProvider<String> firestoreCollection;
    private final ValueProvider<String> kmsKeyPath;
    private final ValueProvider<String> segmentConfigsJson;

    // segment name → (aiField → humanField) from fieldMappings; resolved in @Setup
    private transient Map<String, Map<String, String>> aiToHumanBySegment;
    // segment name → (root node → segment_type label); resolved in @Setup
    private transient Map<String, Map<String, String>> rootToLabelBySegment;

    public FlattenAndCompareFn(ValueProvider<String> firestoreCollection,
                                ValueProvider<String> kmsKeyPath,
                                ValueProvider<String> segmentConfigsJson) {
        this.firestoreCollection = firestoreCollection;
        this.kmsKeyPath          = kmsKeyPath;
        this.segmentConfigsJson  = segmentConfigsJson;
    }

    @Setup
    public void setup() {
        BarricadeEncryptionUtil.configure(
                firestoreCollection.get(),
                kmsKeyPath.get());

        aiToHumanBySegment   = new HashMap<>();
        rootToLabelBySegment = new HashMap<>();
        List<SegmentConfig> segments = SegmentConfig.parse(segmentConfigsJson.get());
        for (SegmentConfig seg : segments) {
            if (seg.fieldMappings != null) {
                Map<String, String> aiToHuman = new HashMap<>();
                for (SegmentConfig.FieldMapping fm : seg.fieldMappings) {
                    if (fm.aiField != null && fm.humanField != null) {
                        aiToHuman.put(fm.aiField, fm.humanField);
                    }
                }
                if (!aiToHuman.isEmpty()) aiToHumanBySegment.put(seg.name, aiToHuman);
            }
            if (seg.segmentTypeMappings != null) {
                Map<String, String> rootToLabel = new HashMap<>();
                for (SegmentConfig.SegmentTypeMapping stm : seg.segmentTypeMappings) {
                    if (stm.roots != null && stm.segmentType != null) {
                        for (String root : stm.roots) {
                            rootToLabel.put(root, stm.segmentType);
                        }
                    }
                }
                if (!rootToLabel.isEmpty()) rootToLabelBySegment.put(seg.name, rootToLabel);
            }
        }
    }

    // ── Processing ────────────────────────────────────────────────────────────

    /** Reserved JSON keys FilterAndPairFn embeds for cross-case provenance — see mergeAcrossCases. */
    private static final String CASE_ID_BY_FIELD_KEY = "_caseIdByField";
    private static final String SOURCE_CASE_ID_KEY   = "_sourceCaseId";
    private static final String SOURCE_CASE_ID_SUFFIX = "." + SOURCE_CASE_ID_KEY;

    @ProcessElement
    public void processElement(ProcessContext ctx) {
        // Pair key format: "imageId::segment" — one comparison per group per run (always
        // against the latest AI payload, only when the signature changed — see
        // FilterAndPairFn). The comparison-version counter rides on the human record instead
        // (see FilterAndPairFn.stampCaseIdByField-adjacent humanRec.put("comparison_version", ...)).
        String pairKey = ctx.element().getKey();
        String[] parts = pairKey.split("::", 2);

        if (parts.length != 2) {
            LOG.warn("Unexpected pairKey format: '{}' — skipping", pairKey);
            return;
        }

        String imageId = parts[0];
        String segment = parts[1];

        GenericRecord human = ctx.element().getValue().getKey();
        GenericRecord ai    = ctx.element().getValue().getValue();

        Object comparisonVersionObj = human.get("comparison_version");
        int iteration = comparisonVersionObj != null
                ? ((Number) comparisonVersionObj).intValue() : 1;

        String humanKeyId = str(human.get("key_id"));
        String aiKeyId    = str(ai.get("key_id"));
        if (!nullSafeEquals(humanKeyId, aiKeyId)) {
            LOG.warn("imageId='{}' segment='{}' mismatched key_ids (human={}, ai={}) — using human",
                    imageId, segment, humanKeyId, aiKeyId);
        }
        String keyId = humanKeyId;

        // Fallback case_id when a field's section had a single contributor (the common case) —
        // see FilterAndPairFn.mergeAcrossCases / the humanRec.put("case_id", ...) call site.
        String canonicalCaseId = str(human.get("case_id"));

        String humanPayload = BarricadeEncryptionUtil.decrypt(keyId, str(human.get("payload")));
        String aiPayload    = BarricadeEncryptionUtil.decrypt(keyId, str(ai.get("payload")));

        // Strip every _caseIdByField provenance map — one may be present at any nesting level,
        // not just the root — before flattening, so it never gets compared as a real field.
        // Recorded under each field's fully-qualified dot-notation path.
        Map<String, String> caseIdByPath = new HashMap<>();
        humanPayload = extractAndStripCaseIdByField(humanPayload, caseIdByPath);

        Map<String, List<FieldValue>> humanFields =
                JsonFieldExtractor.flatten(humanPayload, ARRAY_MATCH_KEYS);
        Map<String, List<FieldValue>> aiFields =
                applyFieldMappings(JsonFieldExtractor.flatten(aiPayload, ARRAY_MATCH_KEYS), segment);

        // _sourceCaseId rides through flatten() as an ordinary sibling field of each merged
        // array item (sharing that item's matchKey) — pull it out into a matchKey -> case_id
        // lookup and drop it from the comparable field set.
        Map<String, String> caseIdByMatchKey = extractAndStripSourceCaseId(humanFields);

        Map<String, String> rootToLabel = rootToLabelBySegment.getOrDefault(
                segment, Collections.emptyMap());

        // Soft-match: for each configured array, compute exact-first / positional-fallback
        // remaps and apply them scoped to that array path only.  Processing each array
        // independently prevents two arrays that share the same parent key prefix (e.g.
        // credit.dob.disputeCodes and credit.ssn.disputeCodes both under "asdasd") from
        // clobbering each other's remaps or leaking remaps across sibling arrays.
        for (Map.Entry<String, String> softEntry : SOFT_MATCH_ARRAYS.entrySet()) {
            String arrayPath  = softEntry.getKey();
            String parentPath = softEntry.getValue();
            Map<String, String>[] remaps =
                    computeSoftMatchRemapsForArray(humanFields, aiFields, arrayPath, parentPath);
            if (!remaps[0].isEmpty()) humanFields = applyKeyRemap(humanFields, arrayPath, remaps[0]);
            if (!remaps[1].isEmpty()) aiFields    = applyKeyRemap(aiFields,    arrayPath, remaps[1]);
        }

        if (humanFields.isEmpty() && aiFields.isEmpty()) {
            LOG.warn("Both payloads empty for imageId='{}' segment='{}' — skipping",
                    imageId, segment);
            return;
        }

        Set<String> allFields = new TreeSet<>();
        allFields.addAll(humanFields.keySet());
        allFields.addAll(aiFields.keySet());

        String aiCreatedAt    = TimestampUtil.normalizeTimestamp(str(ai.get("created_at")));
        String humanCreatedAt = TimestampUtil.normalizeTimestamp(str(human.get("created_at")));
        String loadTime       = TimestampUtil.formatInstant(Instant.now());

        int rowsEmitted = 0;

        for (String field : allFields) {
            List<FieldValue> humanEntries =
                    humanFields.getOrDefault(field, Collections.emptyList());
            List<FieldValue> aiEntries =
                    aiFields.getOrDefault(field, Collections.emptyList());

            boolean keyed = humanEntries.stream().anyMatch(fv -> fv.matchKey != null)
                         || aiEntries.stream().anyMatch(fv -> fv.matchKey != null);

            if (keyed) {
                Map<String, List<String>> humanGroups = groupByKey(humanEntries);
                Map<String, List<String>> aiGroups    = groupByKey(aiEntries);
                Set<String> allMatchKeys = new TreeSet<>();
                allMatchKeys.addAll(humanGroups.keySet());
                allMatchKeys.addAll(aiGroups.keySet());

                for (String matchKey : allMatchKeys) {
                    List<String> humanVals =
                            humanGroups.getOrDefault(matchKey, Collections.emptyList());
                    List<String> aiVals =
                            aiGroups.getOrDefault(matchKey, Collections.emptyList());
                    int count = Math.max(humanVals.size(), aiVals.size());
                    for (int i = 0; i < count; i++) {
                        String humanVal = i < humanVals.size() ? humanVals.get(i) : null;
                        String aiVal    = i < aiVals.size()    ? aiVals.get(i)    : null;
                        String rowCaseId = resolveCaseId(field, matchKey, caseIdByMatchKey,
                                caseIdByPath, canonicalCaseId);
                        emitRow(ctx, imageId, segment, rowCaseId, keyId, iteration,
                                aiCreatedAt, humanCreatedAt, loadTime, rootToLabel,
                                field, matchKey, humanVal, aiVal);
                        rowsEmitted++;
                    }
                }
            } else {
                int count = Math.max(humanEntries.size(), aiEntries.size());
                for (int i = 0; i < count; i++) {
                    String humanVal = i < humanEntries.size() ? humanEntries.get(i).value : null;
                    String aiVal    = i < aiEntries.size()    ? aiEntries.get(i).value    : null;
                    String rowCaseId = resolveCaseId(field, null, caseIdByMatchKey,
                            caseIdByPath, canonicalCaseId);
                    emitRow(ctx, imageId, segment, rowCaseId, keyId, iteration,
                            aiCreatedAt, humanCreatedAt, loadTime, rootToLabel,
                            field, null, humanVal, aiVal);
                    rowsEmitted++;
                }
            }
        }

        LOG.info("Compared imageId='{}' segment='{}' iteration={} — {} rows ({} fields)",
                imageId, segment, iteration, rowsEmitted, allFields.size());
    }

    /**
     * Resolves the {@code case_id} to write for one output row, in precedence order:
     * <ol>
     *   <li>The array item's {@code _sourceCaseId} stamp, looked up by matchKey — for a
     *       {@code mergeArrayFields} array with more than one contributing case.</li>
     *   <li>{@code caseIdByPath}, walking from the field's own full path up through each
     *       ancestor path (removing one dot-segment at a time) until a match is found —
     *       {@code _caseIdByField} is recorded at whatever depth a collision actually
     *       happened (see {@code FilterAndPairFn.mergeJsonObjects}), so a leaf field with no
     *       collision of its own inherits its nearest ancestor's attribution (e.g. a whole
     *       nested object that came from a single case).</li>
     *   <li>{@code canonicalCaseId} — nothing recorded at any level: the field's section had
     *       a single contributor overall (the common case).</li>
     * </ol>
     */
    static String resolveCaseId(String field, String matchKey,
                                 Map<String, String> caseIdByMatchKey,
                                 Map<String, String> caseIdByPath,
                                 String canonicalCaseId) {
        // A field nested inside a keyed sub-array one level deeper than where _sourceCaseId is
        // stamped (e.g. addresses.addressRequested.disputeCodes.code, whose matchKey composites
        // the parent address item's own key with the disputeCodes item's own key — see
        // JsonFieldExtractor.flattenArray) has a matchKey MORE SPECIFIC than any entry
        // caseIdByMatchKey actually has, since _sourceCaseId is only ever stamped on the
        // OUTERMOST array item, not re-stamped on nested keyed sub-arrays. An exact-only lookup
        // therefore always misses for such fields, silently falling through to the path-walk
        // (which doesn't cover array-item provenance at all — see extractAndStripSourceCaseId)
        // and then to canonicalCaseId, misattributing every such field to the group's canonical
        // case regardless of which item it actually came from. Stripping trailing "-"-joined
        // components and retrying finds the correct, less-specific (parent item's own) key.
        if (matchKey != null) {
            String mk = matchKey;
            while (true) {
                String fromArrayItem = caseIdByMatchKey.get(mk);
                if (fromArrayItem != null) return fromArrayItem;
                int dash = mk.lastIndexOf('-');
                if (dash < 0) break;
                mk = mk.substring(0, dash);
            }
        }
        String path = field;
        while (true) {
            String fromPath = caseIdByPath.get(path);
            if (fromPath != null) return fromPath;
            int dot = path.lastIndexOf('.');
            if (dot < 0) break;
            path = path.substring(0, dot);
        }
        return canonicalCaseId;
    }

    /**
     * Recursively walks {@code payload}, removing every {@link #CASE_ID_BY_FIELD_KEY} map it
     * finds — one may be present at any nesting level, not just the root, since
     * {@code FilterAndPairFn.mergeJsonObjects} recurses into nested objects and records
     * provenance at whatever depth a collision actually occurred. Each entry is recorded into
     * {@code caseIdByPath} under its fully-qualified dot-notation path (parent path + field
     * name) so {@link #resolveCaseId} can look it up the same way {@link JsonFieldExtractor}
     * names flattened fields. Returns {@code payload} unchanged if it isn't valid JSON.
     */
    private static String extractAndStripCaseIdByField(String payload, Map<String, String> caseIdByPath) {
        try {
            JsonElement parsed = JsonParser.parseString(payload);
            if (!parsed.isJsonObject()) return payload;
            JsonObject obj = parsed.getAsJsonObject();
            stripCaseIdByField(obj, "", caseIdByPath);
            return obj.toString();
        } catch (Exception e) {
            LOG.warn("Could not parse human payload to extract {} — leaving as-is: {}",
                    CASE_ID_BY_FIELD_KEY, e.getMessage());
            return payload;
        }
    }

    private static void stripCaseIdByField(JsonObject obj, String pathPrefix,
                                            Map<String, String> caseIdByPath) {
        if (obj.has(CASE_ID_BY_FIELD_KEY)) {
            JsonObject map = obj.getAsJsonObject(CASE_ID_BY_FIELD_KEY);
            for (String field : map.keySet()) {
                JsonElement v = map.get(field);
                if (v.isJsonPrimitive()) {
                    String path = pathPrefix.isEmpty() ? field : pathPrefix + "." + field;
                    caseIdByPath.put(path, v.getAsString());
                }
            }
            obj.remove(CASE_ID_BY_FIELD_KEY);
        }
        for (String key : obj.keySet()) {
            JsonElement child = obj.get(key);
            if (child.isJsonObject()) {
                String path = pathPrefix.isEmpty() ? key : pathPrefix + "." + key;
                stripCaseIdByField(child.getAsJsonObject(), path, caseIdByPath);
            }
        }
    }

    /**
     * {@code _sourceCaseId} rides through {@link JsonFieldExtractor#flatten} as an ordinary
     * sibling field of each merged array item, sharing that item's matchKey. Pulls those
     * entries out into a {@code matchKey -> case_id} lookup and removes them from
     * {@code humanFields} so they're never treated as a comparable field.
     */
    private static Map<String, String> extractAndStripSourceCaseId(
            Map<String, List<FieldValue>> humanFields) {
        Map<String, String> caseIdByMatchKey = new HashMap<>();
        List<String> toRemove = new ArrayList<>();
        for (Map.Entry<String, List<FieldValue>> e : humanFields.entrySet()) {
            String key = e.getKey();
            if (!key.equals(SOURCE_CASE_ID_KEY) && !key.endsWith(SOURCE_CASE_ID_SUFFIX)) continue;
            toRemove.add(key);
            for (FieldValue fv : e.getValue()) {
                if (fv.matchKey != null && fv.value != null) {
                    caseIdByMatchKey.put(fv.matchKey, fv.value);
                }
            }
        }
        toRemove.forEach(humanFields::remove);
        return caseIdByMatchKey;
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    /**
     * Renames AI field keys using the segment's fieldMappings so that
     * cross-named root objects (e.g. AI "consumer" ↔ human "verifiedData")
     * are compared against each other.
     *
     * Handles both exact matches ("consumer") and dot-prefixed children
     * ("consumer.firstName", "consumer.dob", …). The human field name is
     * used as the canonical key so field_name in the output matches the
     * human side.
     */
    private Map<String, List<FieldValue>> applyFieldMappings(
            Map<String, List<FieldValue>> aiFields, String segment) {
        Map<String, String> aiToHuman = aiToHumanBySegment.get(segment);
        if (aiToHuman == null || aiToHuman.isEmpty()) return aiFields;

        Map<String, List<FieldValue>> result = new LinkedHashMap<>();
        for (Map.Entry<String, List<FieldValue>> entry : aiFields.entrySet()) {
            String key         = entry.getKey();
            String renamedKey  = rename(key, aiToHuman);
            result.merge(renamedKey, entry.getValue(), (existing, incoming) -> {
                List<FieldValue> merged = new ArrayList<>(existing);
                merged.addAll(incoming);
                return merged;
            });
        }
        return result;
    }

    /**
     * Returns the segment_type for a flattened field, applying any configured
     * root-node label overrides for the current segment.
     *
     * Raw root is:
     *   "employments.test.test", null  → "employments"
     *   "documentDetails",       "A"   → "documentDetails"  (keyed array)
     *   "name",                  null  → null               (root scalar)
     *
     * If the raw root matches a segmentTypeMappings entry, the configured label
     * is returned instead (e.g. "verifiedData" → "authentication").
     */
    private static String resolveSegmentType(String field, String arrayKey,
                                              Map<String, String> rootToLabel) {
        int dot = field.indexOf('.');
        String root;
        if (dot > 0) {
            root = field.substring(0, dot);
        } else if (arrayKey != null) {
            root = field;
        } else {
            return null;
        }
        return rootToLabel.getOrDefault(root, root);
    }

    private static String rename(String key, Map<String, String> aiToHuman) {
        for (Map.Entry<String, String> mapping : aiToHuman.entrySet()) {
            String aiPrefix    = mapping.getKey();
            String humanPrefix = mapping.getValue();
            if (key.equals(aiPrefix)) {
                return humanPrefix;
            }
            if (key.startsWith(aiPrefix + ".")) {
                return humanPrefix + key.substring(aiPrefix.length());
            }
        }
        return key;
    }

    /**
     * Computes soft-match key remappings for a single array.
     *
     * <p>Processes each array independently so sibling arrays that share the same
     * parent key prefix (e.g. {@code credit.dob.disputeCodes} and
     * {@code credit.ssn.disputeCodes} both under {@code "asdasd"}) cannot overwrite
     * each other's remaps.
     *
     * <p>Returns {@code [humanRemap, aiRemap]}.
     */
    @SuppressWarnings("unchecked")
    private static Map<String, String>[] computeSoftMatchRemapsForArray(
            Map<String, List<FieldValue>> humanFields,
            Map<String, List<FieldValue>> aiFields,
            String arrayPath,
            String parentPath) {

        Map<String, String> humanRemap = new HashMap<>();
        Map<String, String> aiRemap    = new HashMap<>();

        Set<String> humanArrayKeys = collectMatchKeys(humanFields, arrayPath);
        Set<String> aiArrayKeys    = collectMatchKeys(aiFields,    arrayPath);

        if (parentPath.isEmpty()) {
            processGroup("", new ArrayList<>(humanArrayKeys), new ArrayList<>(aiArrayKeys),
                         humanRemap, aiRemap);
        } else {
            Stream.concat(
                    collectMatchKeys(humanFields, parentPath).stream(),
                    collectMatchKeys(aiFields,    parentPath).stream())
                .collect(Collectors.toCollection(LinkedHashSet::new))
                .forEach(parentKey -> {
                    String childPrefix = parentKey + "-";
                    List<String> humanChildKeys = humanArrayKeys.stream()
                            .filter(k -> k.startsWith(childPrefix))
                            .collect(Collectors.toList());
                    List<String> aiChildKeys = aiArrayKeys.stream()
                            .filter(k -> k.startsWith(childPrefix))
                            .collect(Collectors.toList());
                    processGroup(parentKey, humanChildKeys, aiChildKeys, humanRemap, aiRemap);
                });
        }

        return new Map[]{humanRemap, aiRemap};
    }

    /**
     * Exact-first, positional-fallback matching for one parent group.
     *
     * <ul>
     *   <li>Exact matches   → {@code {parent}-{code}-{code}}</li>
     *   <li>Fallback pairs  → {@code {parent}-{aiCode}-{humanCode}}</li>
     *   <li>AI orphan       → {@code {parent}-{aiCode}-null}</li>
     *   <li>Human orphan    → {@code {parent}-null-{humanCode}}</li>
     * </ul>
     */
    private static void processGroup(String parentKey,
                                      List<String> humanKeys,
                                      List<String> aiKeys,
                                      Map<String, String> humanRemap,
                                      Map<String, String> aiRemap) {
        List<String> sortedHuman = humanKeys.stream().sorted().collect(Collectors.toList());
        List<String> sortedAi    = aiKeys.stream().sorted().collect(Collectors.toList());

        Set<String> exact = sortedHuman.stream()
                .filter(new HashSet<>(sortedAi)::contains)
                .collect(Collectors.toSet());
        exact.forEach(k -> {
            String combined = combinedKey(parentKey, k, k);
            aiRemap.put(k,    combined);
            humanRemap.put(k, combined);
        });

        List<String> unmatchedHuman = sortedHuman.stream()
                .filter(k -> !exact.contains(k)).collect(Collectors.toList());
        List<String> unmatchedAi   = sortedAi.stream()
                .filter(k -> !exact.contains(k)).collect(Collectors.toList());

        int pairs = Math.min(unmatchedHuman.size(), unmatchedAi.size());
        IntStream.range(0, pairs).forEach(i -> {
            String aiKey    = unmatchedAi.get(i);
            String humanKey = unmatchedHuman.get(i);
            String combined = combinedKey(parentKey, aiKey, humanKey);
            aiRemap.put(aiKey,       combined);
            humanRemap.put(humanKey, combined);
        });

        unmatchedAi.stream().skip(pairs)
                .forEach(ak -> aiRemap.put(ak, combinedKey(parentKey, ak, null)));
        unmatchedHuman.stream().skip(pairs)
                .forEach(hk -> humanRemap.put(hk, combinedKey(parentKey, null, hk)));
    }

    /**
     * Builds a combined array key: {@code "{parent}-{aiCode}-{humanCode}"}.
     * When {@code parentKey} is empty, only the code pair is returned.
     * {@code null} for either key substitutes the literal {@code "null"}.
     */
    private static String combinedKey(String parentKey, String aiKey, String humanKey) {
        String aiCode    = aiKey    != null ? lastSegment(aiKey)    : "XXX";
        String humanCode = humanKey != null ? lastSegment(humanKey) : "XXX";
        String codePart  = aiCode + "-" + humanCode;
        return parentKey.isEmpty() ? codePart : parentKey + "-" + codePart;
    }

    /** Returns the substring after the last {@code -}, or the whole string if none. */
    private static String lastSegment(String key) {
        int dash = key.lastIndexOf('-');
        return dash >= 0 ? key.substring(dash + 1) : key;
    }

    /**
     * Collects all distinct non-null match keys from fields at or under {@code arrayPath}.
     */
    private static Set<String> collectMatchKeys(Map<String, List<FieldValue>> fields,
                                                 String arrayPath) {
        String prefix = arrayPath + ".";
        return fields.entrySet().stream()
                .filter(e -> e.getKey().equals(arrayPath) || e.getKey().startsWith(prefix))
                .flatMap(e -> e.getValue().stream())
                .filter(fv -> fv.matchKey != null)
                .map(fv -> fv.matchKey)
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    /**
     * Returns a new field map with match keys remapped according to {@code keyRemap},
     * scoped to fields at or under {@code arrayPath} only.  Fields outside that path
     * are left unchanged, preventing sibling arrays from interfering with each other.
     */
    private static Map<String, List<FieldValue>> applyKeyRemap(
            Map<String, List<FieldValue>> fields,
            String arrayPath,
            Map<String, String> keyRemap) {
        String prefix = arrayPath + ".";
        return fields.entrySet().stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        e -> {
                            String field = e.getKey();
                            boolean inArray = field.equals(arrayPath) || field.startsWith(prefix);
                            if (!inArray) return e.getValue();
                            return e.getValue().stream()
                                    .map(fv -> new FieldValue(
                                            fv.matchKey != null
                                                    ? keyRemap.getOrDefault(fv.matchKey, fv.matchKey)
                                                    : null,
                                            fv.value))
                                    .collect(Collectors.toList());
                        },
                        (a, b) -> a,
                        LinkedHashMap::new));
    }

    private void emitRow(ProcessContext ctx,
                         String imageId, String segment, String caseId, String keyId, int iteration,
                         String aiCreatedAt, String humanCreatedAt, String loadTime,
                         Map<String, String> rootToLabel,
                         String field, String arrayKey,
                         String humanVal, String aiVal) {

        final boolean isMatch = humanVal == null ? aiVal == null : humanVal.equalsIgnoreCase(aiVal);
        String encryptedHumanVal = BarricadeEncryptionUtil.encrypt(keyId, humanVal);
        String encryptedAiVal    = BarricadeEncryptionUtil.encrypt(keyId, aiVal);

        ctx.output(new TableRow()
                .set("image_id",         imageId)
                .set("key_id",           keyId)
                .set("segment",          segment)
                .set("case_id",          caseId)
                .set("ai_iteration",     iteration)
                .set("ai_created_at",    aiCreatedAt)
                .set("human_created_at", humanCreatedAt)
                .set("field_name",       field)
                .set("array_key",        arrayKey)
                .set("segment_type",     resolveSegmentType(field, arrayKey, rootToLabel))
                .set("human_value",      encryptedHumanVal)
                .set("ai_value",         encryptedAiVal)
                .set("is_match",         isMatch)
                .set("load_time",        loadTime));
    }

    private static Map<String, List<String>> groupByKey(List<FieldValue> entries) {
        Map<String, List<String>> groups = new LinkedHashMap<>();
        for (FieldValue fv : entries) {
            if (fv.matchKey != null) {
                groups.computeIfAbsent(fv.matchKey, k -> new ArrayList<>()).add(fv.value);
            }
        }
        return groups;
    }

    private static boolean nullSafeEquals(String a, String b) {
        return a == null ? b == null : a.equals(b);
    }

    private static String str(Object value) {
        return value != null ? value.toString() : null;
    }
}
