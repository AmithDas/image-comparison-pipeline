package com.yourorg.pipeline;

import com.google.api.services.bigquery.model.TableRow;
import com.yourorg.pipeline.transforms.FilterAndPairFn;
import com.yourorg.pipeline.transforms.FlattenAndCompareFn;
import com.yourorg.pipeline.util.AvroSchemas;
import com.yourorg.pipeline.util.BarricadeEncryptionUtil;
import com.yourorg.pipeline.util.JsonFieldExtractor;
import com.yourorg.pipeline.util.SchemaUtil;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Collections;
import java.util.Map;

/**
 * Entry point for the Image Comparison Dataflow pipeline.
 *
 * <p>Pipeline overview:
 * <ol>
 *   <li>Read windowed rows from {@code image_payloads} source table.</li>
 *   <li>Read current pending state from {@code pending_comparisons}.</li>
 *   <li>CoGroupByKey on image name (extracted from payload) to join source and pending.</li>
 *   <li>Run {@link FilterAndPairFn} to merge source + pending, check eligibility,
 *       and route to MATCHED / NEW_PENDING / AGED_OUT.</li>
 *   <li>Run {@link FlattenAndCompareFn} on MATCHED pairs to produce field-level results.</li>
 *   <li>Write results to {@code image_comparison_results}.</li>
 *   <li>Overwrite {@code pending_comparisons} with only still-pending rows (WRITE_TRUNCATE).</li>
 *   <li>Append aged-out rows to {@code dead_letter_comparisons}.</li>
 * </ol>
 *
 * <p>Internal records use Avro {@link GenericRecord} with schemas defined in
 * {@code payload_row.avsc} and {@code pending_row.avsc}.
 *
 * <p>Run locally (DirectRunner):
 * <pre>
 *   mvn compile exec:java \
 *     -Dexec.mainClass=com.yourorg.pipeline.ImageComparisonPipeline \
 *     -Dexec.args="--runner=DirectRunner \
 *       --aiSourceTable=project:ai_dataset.ai_payloads \
 *       --humanSourceTable=project:human_dataset.human_payloads \
 *       --outputTable=project:dataset.image_comparison_results \
 *       --pendingTable=project:dataset.pending_comparisons \
 *       --deadLetterTable=project:dataset.dead_letter_comparisons \
 *       --windowStart=2026-04-16T00:00:00Z \
 *       --windowEnd=2026-04-17T00:00:00Z \
 *       --aiMethod=aimetadata \
 *       --humanMethod=controller.SubmitDispute"
 * </pre>
 */
public class ImageComparisonPipeline {

    private static final Logger LOG = LoggerFactory.getLogger(ImageComparisonPipeline.class);

    // ── Pipeline options ──────────────────────────────────────────────────────

    public interface Options extends PipelineOptions {

        @Description("BigQuery table containing AI payloads. Format: project:dataset.table")
        @Validation.Required
        String getAiSourceTable();
        void setAiSourceTable(String value);

        @Description("BigQuery table containing human payloads. Format: project:dataset.table")
        @Validation.Required
        String getHumanSourceTable();
        void setHumanSourceTable(String value);

        @Description("Output BigQuery table for field-level comparison results. "
                + "Format: project:dataset.table")
        @Validation.Required
        String getOutputTable();
        void setOutputTable(String value);

        @Description("BigQuery table used as a durable pending state store for "
                + "orphaned payloads awaiting their counterpart. "
                + "Format: project:dataset.table")
        @Validation.Required
        String getPendingTable();
        void setPendingTable(String value);

        @Description("BigQuery table for payloads that exceeded the maximum wait "
                + "threshold (default 7 days). Format: project:dataset.table")
        @Validation.Required
        String getDeadLetterTable();
        void setDeadLetterTable(String value);

        @Description("Inclusive start of the processing window (ISO-8601, e.g. 2026-04-16T00:00:00Z). "
                + "Only source rows with created_at >= windowStart are read.")
        @Validation.Required
        String getWindowStart();
        void setWindowStart(String value);

        @Description("Exclusive end of the processing window (ISO-8601, e.g. 2026-04-17T00:00:00Z). "
                + "Only source rows with created_at < windowEnd are read.")
        @Validation.Required
        String getWindowEnd();
        void setWindowEnd(String value);

        @Description("Value of the 'method' column that identifies an AI payload (e.g. 'aimetadata').")
        @Validation.Required
        String getAiMethod();
        void setAiMethod(String value);

        @Description("Value of the 'method' column that identifies a human payload (e.g. 'controller.SubmitDispute').")
        @Validation.Required
        String getHumanMethod();
        void setHumanMethod(String value);

        @Description("Comma-separated sort keys per array path, e.g. 'terms=code,items=id'. "
                + "Each entry specifies which field to sort array elements by at that dot-notation path. "
                + "Omit to sort by full element JSON string.")
        String getArraySortKeys();
        void setArraySortKeys(String value);
    }

    // ── Entry point ───────────────────────────────────────────────────────────

    public static void main(String[] args) {
        Options options = PipelineOptionsFactory
                .fromArgs(args)
                .withValidation()
                .as(Options.class);

        Pipeline pipeline = Pipeline.create(options);
        buildPipeline(pipeline, options);
        pipeline.run().waitUntilFinish();
    }

    /**
     * Builds the pipeline graph. Separated from {@link #main} for testability.
     */
    public static void buildPipeline(Pipeline pipeline, Options options) {

        // ── 1. Read AI and human source tables separately, then flatten ──────
        LOG.info("Reading AI source table: {} for window [{}, {})",
                options.getAiSourceTable(), options.getWindowStart(), options.getWindowEnd());
        String aiQuery = String.format(
                "SELECT * FROM `%s`"
                        + " WHERE created_at >= '%s' AND created_at < '%s'"
                        + " AND method = '%s'",
                options.getAiSourceTable().replace(':', '.'),
                options.getWindowStart(),
                options.getWindowEnd(),
                options.getAiMethod());
        PCollection<TableRow> aiRows = pipeline.apply(
                "ReadAiPayloads",
                BigQueryIO.readTableRows()
                        .fromQuery(aiQuery)
                        .usingStandardSql());

        LOG.info("Reading human source table: {} for window [{}, {})",
                options.getHumanSourceTable(), options.getWindowStart(), options.getWindowEnd());
        String humanQuery = String.format(
                "SELECT * FROM `%s`"
                        + " WHERE created_at >= '%s' AND created_at < '%s'"
                        + " AND method = '%s'",
                options.getHumanSourceTable().replace(':', '.'),
                options.getWindowStart(),
                options.getWindowEnd(),
                options.getHumanMethod());
        PCollection<TableRow> humanRows = pipeline.apply(
                "ReadHumanPayloads",
                BigQueryIO.readTableRows()
                        .fromQuery(humanQuery)
                        .usingStandardSql());

        PCollection<TableRow> rawRows = PCollectionList.of(aiRows).and(humanRows)
                .apply("FlattenSourcePayloads", Flatten.pCollections());

        // ── 2. Read pending table with partition filter (last MAX_WAIT_DAYS only) ─
        // Partitioned on DATE(first_seen_at) — filter eliminates partitions older
        // than the eviction window, avoiding a full table scan as the table grows.
        LOG.info("Reading pending table: {}", options.getPendingTable());
        String pendingQuery = String.format(
                "SELECT * FROM `%s`"
                        + " WHERE first_seen_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(),"
                        + " INTERVAL %d DAY)",
                options.getPendingTable().replace(':', '.'),
                FilterAndPairFn.MAX_WAIT_DAYS);
        PCollection<KV<String, TableRow>> keyedPending = pipeline
                .apply("ReadPendingTable",
                        BigQueryIO.readTableRows()
                                .fromQuery(pendingQuery)
                                .usingStandardSql())
                .apply("KeyPendingById",
                        WithKeys.of((TableRow row) -> (String) row.get("image_id"))
                                .withKeyType(TypeDescriptors.strings()));

        // ── 3. Key source rows by image_name extracted from decrypted payload ──
        PCollection<KV<String, TableRow>> keyedSource = rawRows
                .apply("KeySourceById",
                        WithKeys.of((TableRow row) -> {
                            String keyId     = (String) row.get("key_id");
                            String decrypted = BarricadeEncryptionUtil.decrypt(
                                    keyId, (String) row.get("payload"));
                            return JsonFieldExtractor.extractField(decrypted, "image_name");
                        })
                                .withKeyType(TypeDescriptors.strings()));

        // ── 4. Co-group source + pending by image name ────────────────────────
        PCollection<KV<String, CoGbkResult>> coGrouped =
                KeyedPCollectionTuple
                        .of(FilterAndPairFn.SOURCE_TAG, keyedSource)
                        .and(FilterAndPairFn.PENDING_TAG, keyedPending)
                        .apply("CoGroupByImageId", CoGroupByKey.create());

        // ── 5. Filter & pair ──────────────────────────────────────────────────
        PCollectionTuple routed = coGrouped.apply(
                "FilterAndPair",
                ParDo.of(new FilterAndPairFn(options.getAiMethod(), options.getHumanMethod()))
                     .withOutputTags(
                             FilterAndPairFn.MATCHED,
                             TupleTagList
                                     .of(FilterAndPairFn.NEW_PENDING)
                                     .and(FilterAndPairFn.AGED_OUT)));

        // Register Avro coders so Beam can serialise GenericRecord between steps.
        AvroCoder<GenericRecord> payloadCoder =
                AvroCoder.of(GenericRecord.class, AvroSchemas.PAYLOAD_ROW);
        AvroCoder<GenericRecord> pendingCoder =
                AvroCoder.of(GenericRecord.class, AvroSchemas.PENDING_ROW);

        PCollection<KV<String, KV<GenericRecord, GenericRecord>>> matched =
                routed.get(FilterAndPairFn.MATCHED)
                      .setCoder(KvCoder.of(StringUtf8Coder.of(),
                              KvCoder.of(payloadCoder, payloadCoder)));

        PCollection<GenericRecord> newPending =
                routed.get(FilterAndPairFn.NEW_PENDING).setCoder(pendingCoder);

        PCollection<GenericRecord> agedOut =
                routed.get(FilterAndPairFn.AGED_OUT).setCoder(pendingCoder);

        // ── 6. Flatten JSON + field-level comparison ──────────────────────────
        PCollection<TableRow> comparisonResults = matched
                .apply("FlattenAndCompare",
                        ParDo.of(new FlattenAndCompareFn(
                                parseArraySortKeys(options.getArraySortKeys()))));

        // ── 7. Write comparison results ───────────────────────────────────────
        comparisonResults.apply(
                "WriteComparisonResults",
                BigQueryIO.writeTableRows()
                        .to(options.getOutputTable())
                        .withSchema(SchemaUtil.comparisonResultsSchema())
                        .withWriteDisposition(WriteDisposition.WRITE_APPEND)
                        .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED));

        // ── 8. Overwrite pending table (WRITE_TRUNCATE = implicit cleanup) ────
        newPending
                .apply("MapPendingToTableRow",
                        MapElements
                            .into(TypeDescriptor.of(TableRow.class))
                            .via(r -> fromPendingRecord(r)))
                .apply("WritePendingTable",
                        BigQueryIO.writeTableRows()
                                .to(options.getPendingTable())
                                .withSchema(SchemaUtil.pendingSchema())
                                .withWriteDisposition(WriteDisposition.WRITE_TRUNCATE)
                                .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED));

        // ── 9. Append aged-out rows to dead-letter table ──────────────────────
        agedOut
                .apply("MapAgedOutToTableRow",
                        MapElements
                            .into(TypeDescriptor.of(TableRow.class))
                            .via(r -> fromAgedOutRecord(r)))
                .apply("WriteDeadLetterTable",
                        BigQueryIO.writeTableRows()
                                .to(options.getDeadLetterTable())
                                .withSchema(SchemaUtil.deadLetterSchema())
                                .withWriteDisposition(WriteDisposition.WRITE_APPEND)
                                .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED));

        LOG.info("Pipeline graph constructed successfully.");
    }

    // ── Row conversion helpers ────────────────────────────────────────────────

    private static TableRow fromPendingRecord(GenericRecord r) {
        return new TableRow()
                .set("image_id",        str(r.get("image_id")))
                .set("key_id",          str(r.get("key_id")))
                .set("pending_type",    str(r.get("pending_type")))
                .set("payload",         str(r.get("payload")))
                .set("created_at",      str(r.get("created_at")))
                .set("first_seen_at",   str(r.get("first_seen_at")))
                .set("last_retried_at", str(r.get("last_retried_at")))
                .set("retry_count",     r.get("retry_count") != null
                        ? ((Number) r.get("retry_count")).longValue() : 0L);
    }

    private static TableRow fromAgedOutRecord(GenericRecord r) {
        return new TableRow()
                .set("image_id",      str(r.get("image_id")))
                .set("key_id",        str(r.get("key_id")))
                .set("pending_type",  str(r.get("pending_type")))
                .set("payload",       str(r.get("payload")))
                .set("created_at",    str(r.get("created_at")))
                .set("first_seen_at", str(r.get("first_seen_at")))
                .set("aged_out_at",   Instant.now().toString())
                .set("reason",        "No counterpart payload after "
                        + FilterAndPairFn.MAX_WAIT_DAYS + " days");
    }

    private static String str(Object value) {
        return value != null ? value.toString() : null;
    }

    private static Map<String, String> parseArraySortKeys(String raw) {
        if (raw == null || raw.isBlank()) return Collections.emptyMap();
        Map<String, String> map = new java.util.HashMap<>();
        for (String entry : raw.split(",")) {
            String[] parts = entry.split("=", 2);
            if (parts.length == 2 && !parts[0].isBlank() && !parts[1].isBlank()) {
                map.put(parts[0].trim(), parts[1].trim());
            }
        }
        return map;
    }
}
