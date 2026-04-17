package com.yourorg.pipeline.transforms;

import com.google.api.services.bigquery.model.TableRow;
import com.yourorg.pipeline.util.JsonFieldExtractor;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

/**
 * Flattens both JSON payloads in a matched pair and emits one
 * {@link TableRow} per field for the image_comparison_results output table.
 *
 * <p>Input key format: {@code "imageId::aiIteration"} (e.g. {@code "img123::1"}).
 * Input values are Avro {@link GenericRecord}s with the {@code PayloadRow} schema.
 *
 * <p>Field discovery is dynamic — all fields present in either the human or
 * AI payload are included. Fields absent in one side appear as null values.
 *
 * <p>Null equality: {@code null} == {@code null} is treated as a match
 * (both fields missing from their respective payloads).
 */
public class FlattenAndCompareFn
        extends DoFn<KV<String, KV<GenericRecord, GenericRecord>>, TableRow> {

    private static final Logger LOG = LoggerFactory.getLogger(FlattenAndCompareFn.class);

    @ProcessElement
    public void processElement(ProcessContext ctx) {
        String pairKey = ctx.element().getKey();
        String[] parts = pairKey.split("::");

        if (parts.length != 2) {
            LOG.warn("Unexpected pairKey format: '{}' — skipping", pairKey);
            return;
        }

        String imageId = parts[0];
        int iteration;
        try {
            iteration = Integer.parseInt(parts[1]);
        } catch (NumberFormatException e) {
            LOG.warn("Could not parse iteration from pairKey '{}' — skipping", pairKey);
            return;
        }

        GenericRecord human = ctx.element().getValue().getKey();
        GenericRecord ai    = ctx.element().getValue().getValue();

        // ── Flatten both payloads ─────────────────────────────────────────────
        Map<String, String> humanFields = JsonFieldExtractor.flatten(str(human.get("payload")));
        Map<String, String> aiFields    = JsonFieldExtractor.flatten(str(ai.get("payload")));

        if (humanFields.isEmpty() && aiFields.isEmpty()) {
            LOG.warn("Both payloads empty or unparseable for imageId='{}' — skipping", imageId);
            return;
        }

        // ── Union of all field names from both payloads ───────────────────────
        Set<String> allFields = new TreeSet<>();
        allFields.addAll(humanFields.keySet());
        allFields.addAll(aiFields.keySet());

        String comparedAt = Instant.now().toString();

        // ── Emit one TableRow per field ───────────────────────────────────────
        for (String field : allFields) {
            String humanVal = humanFields.get(field);
            String aiVal    = aiFields.get(field);
            boolean isMatch = Objects.equals(humanVal, aiVal);

            TableRow row = new TableRow()
                    .set("image_id",         imageId)
                    .set("ai_iteration",     iteration)
                    .set("ai_created_at",    str(ai.get("created_at")))
                    .set("human_created_at", str(human.get("created_at")))
                    .set("field_name",       field)
                    .set("human_value",      humanVal)
                    .set("ai_value",         aiVal)
                    .set("is_match",         isMatch)
                    .set("compared_at",      comparedAt);

            ctx.output(row);
        }

        LOG.info("Compared imageId='{}' iteration={} — {} fields emitted",
                imageId, iteration, allFields.size());
    }

    private static String str(Object value) {
        return value != null ? value.toString() : null;
    }
}
