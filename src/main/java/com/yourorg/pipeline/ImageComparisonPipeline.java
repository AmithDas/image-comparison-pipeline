package com.yourorg.pipeline;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableResult;
import com.yourorg.pipeline.config.SegmentConfig;
import com.yourorg.pipeline.transforms.DecryptAndKeyFn;
import com.yourorg.pipeline.transforms.FilterAndPairFn;
import com.yourorg.pipeline.transforms.FlattenAndCompareFn;
import com.yourorg.pipeline.transforms.OrphanCompareFn;
import com.yourorg.pipeline.util.SchemaRegistry;
import com.yourorg.pipeline.util.SchemaUtil;
import com.yourorg.pipeline.util.TimestampUtil;
import org.apache.avro.Schema;
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
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.List;

/**
 * Entry point for the Image Comparison Dataflow pipeline.
 *
 * <p>All segments (main, authanddocreview, …) are processed in a single Dataflow job.
 * Source rows are routed to the correct segment by their {@code method} column value.
 * All comparison results are written to one shared table with a {@code segment} column.
 *
 * <h3>Window management</h3>
 * The pipeline reads its processing window from a BigQuery lookup table
 * ({@code --lookupTable}).  On startup it claims the earliest {@code PENDING} row,
 * marks it {@code RUNNING}, runs, then marks {@code DONE} or {@code FAILED}.
 * The DAG only needs to trigger the Dataflow job — no window/status logic needed there.
 *
 * <h3>Pipeline steps</h3>
 * <ol>
 *   <li>Claim window from lookup table; mark RUNNING.</li>
 *   <li>Read windowed AI and human rows from BigQuery source tables.</li>
 *   <li>Decrypt each row; derive segment from its {@code method} column; key as
 *       {@code "imageId::segment"}.</li>
 *   <li>Read pending state from {@code pending_comparisons}.</li>
 *   <li>Co-group source + pending; run {@link FilterAndPairFn}.</li>
 *   <li>Run {@link FlattenAndCompareFn} on MATCHED pairs.</li>
 *   <li>Write results, pending rows, dead-letter rows, snapshot.</li>
 *   <li>Mark lookup row DONE (or FAILED on exception).</li>
 * </ol>
 *
 * <p>Run locally (DirectRunner — requires a PENDING row in the lookup table):
 * <pre>
 *   mvn compile exec:java \
 *     -Dexec.mainClass=com.yourorg.pipeline.ImageComparisonPipeline \
 *     -Dexec.args="--runner=DirectRunner \
 *       --lookupTable=project:dataset.pipeline_windows \
 *       --aiSourceTable=project:ai_dataset.ai_payloads \
 *       --humanSourceTable=project:human_dataset.human_payloads \
 *       --outputTable=project:dataset.comparison_results \
 *       --pendingTable=project:dataset.pending_comparisons \
 *       --deadLetterTable=project:dataset.dead_letter_comparisons \
 *       --pendingSnapshotTable=project:dataset.pending_snapshot \
 *       --segmentConfigs='[...]' \
 *       --firestoreCollection=dek_store \
 *       --kmsKeyPath=projects/p/locations/l/keyRings/r/cryptoKeys/k"
 * </pre>
 */
public class ImageComparisonPipeline {

    private static final Logger LOG = LoggerFactory.getLogger(ImageComparisonPipeline.class);

    // ── Pipeline options ──────────────────────────────────────────────────────

    public interface Options extends PipelineOptions {

        @Description("BigQuery lookup table that controls processing windows. "
                + "The pipeline claims the earliest PENDING row and manages its status. "
                + "Schema: run_id STRING, window_start TIMESTAMP, window_end TIMESTAMP, status STRING. "
                + "Format: project:dataset.table")
        @Validation.Required
        ValueProvider<String> getLookupTable();
        void setLookupTable(ValueProvider<String> value);

        @Description("BigQuery table containing AI payloads. Format: project:dataset.table")
        @Validation.Required
        ValueProvider<String> getAiSourceTable();
        void setAiSourceTable(ValueProvider<String> value);

        @Description("BigQuery table containing human payloads. Format: project:dataset.table")
        @Validation.Required
        ValueProvider<String> getHumanSourceTable();
        void setHumanSourceTable(ValueProvider<String> value);

        @Description("Single output BigQuery table for all field-level comparison results. "
                + "Contains a 'segment' column to distinguish segments. "
                + "Format: project:dataset.table")
        @Validation.Required
        ValueProvider<String> getOutputTable();
        void setOutputTable(ValueProvider<String> value);

        @Description("BigQuery table for pending state (rows awaiting their counterpart). "
                + "Format: project:dataset.table")
        @Validation.Required
        ValueProvider<String> getPendingTable();
        void setPendingTable(ValueProvider<String> value);

        @Description("BigQuery table for payloads that exceeded the max wait threshold. "
                + "Format: project:dataset.table")
        @Validation.Required
        ValueProvider<String> getDeadLetterTable();
        void setDeadLetterTable(ValueProvider<String> value);

        @Description("BigQuery table for the still-pending payload snapshot (WRITE_TRUNCATE). "
                + "Format: project:dataset.table")
        @Validation.Required
        ValueProvider<String> getPendingSnapshotTable();
        void setPendingSnapshotTable(ValueProvider<String> value);

        @Description(
                "JSON array of segment configurations. Each entry must have: "
                + "name, aiMethod, humanMethod. Optional: payloadFormat, humanSubTypes. "
                + "Example: [{\"name\":\"main\",\"aiMethod\":\"aimetadata\","
                + "\"humanMethod\":\"controller.SubmitDispute\"}]")
        @Validation.Required
        ValueProvider<String> getSegmentConfigs();
        void setSegmentConfigs(ValueProvider<String> value);

        @Description("Dot-notation path to the image identifier field inside the decrypted payload.")
        @Default.String("queueImages[0].fileName")
        ValueProvider<String> getImageNameField();
        void setImageNameField(ValueProvider<String> value);

        @Description("Dot-notation JSON field name to filter human rows on. Omit to skip.")
        ValueProvider<String> getHumanFilterField();
        void setHumanFilterField(ValueProvider<String> value);

        @Description("Expected value for --humanFilterField.")
        ValueProvider<String> getHumanFilterValue();
        void setHumanFilterValue(ValueProvider<String> value);

        @Description("Firestore collection name that stores wrapped DEKs.")
        @Validation.Required
        ValueProvider<String> getFirestoreCollection();
        void setFirestoreCollection(ValueProvider<String> value);

        @Description("Full Cloud KMS CryptoKey resource path. "
                + "Format: projects/P/locations/L/keyRings/R/cryptoKeys/K")
        @Validation.Required
        ValueProvider<String> getKmsKeyPath();
        void setKmsKeyPath(ValueProvider<String> value);
    }

    // ── Window info (resolved from lookup table before graph construction) ────

    private static final class WindowInfo {
        final String runId;
        final String windowStart;
        final String windowEnd;

        WindowInfo(String runId, String windowStart, String windowEnd) {
            this.runId       = runId;
            this.windowStart = windowStart;
            this.windowEnd   = windowEnd;
        }
    }

    // ── Entry point ───────────────────────────────────────────────────────────

    public static void main(String[] args) {
        Options options = PipelineOptionsFactory
                .fromArgs(args)
                .withValidation()
                .as(Options.class);

        BigQuery bq = BigQueryOptions.getDefaultInstance().getService();

        WindowInfo window = fetchPendingWindow(options, bq);
        if (window == null) {
            LOG.info("No PENDING window found in lookup table — nothing to do.");
            return;
        }

        LOG.info("Claiming window: run_id={} start={} end={}",
                window.runId, window.windowStart, window.windowEnd);
        setStatus(options, bq, window.runId, "RUNNING");

        try {
            Pipeline pipeline = Pipeline.create(options);
            buildPipeline(pipeline, options, window.windowStart, window.windowEnd);
            pipeline.run().waitUntilFinish();
            setStatus(options, bq, window.runId, "DONE");
        } catch (Exception e) {
            LOG.error("Pipeline failed for run_id={}", window.runId, e);
            setStatus(options, bq, window.runId, "FAILED");
            throw e instanceof RuntimeException ? (RuntimeException) e : new RuntimeException(e);
        }
    }

    // ── Lookup table helpers ──────────────────────────────────────────────────

    private static WindowInfo fetchPendingWindow(Options options, BigQuery bq) {
        String table = options.getLookupTable().get().replace(':', '.');
        String sql = String.format(
                "SELECT run_id,"
                + " FORMAT_TIMESTAMP('%%Y-%%m-%%dT%%H:%%M:%%SZ', window_start) AS window_start,"
                + " FORMAT_TIMESTAMP('%%Y-%%m-%%dT%%H:%%M:%%SZ', window_end)   AS window_end"
                + " FROM `%s`"
                + " WHERE status = 'PENDING'"
                + " ORDER BY window_start ASC"
                + " LIMIT 1",
                table);
        try {
            TableResult result = bq.query(
                    QueryJobConfiguration.newBuilder(sql).setUseLegacySql(false).build());
            if (result.getTotalRows() == 0) return null;
            FieldValueList row = result.iterateAll().iterator().next();
            return new WindowInfo(
                    row.get("run_id").getStringValue(),
                    row.get("window_start").getStringValue(),
                    row.get("window_end").getStringValue());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted querying lookup table", e);
        }
    }

    private static void setStatus(Options options, BigQuery bq, String runId, String status) {
        String table = options.getLookupTable().get().replace(':', '.');
        String sql = String.format(
                "UPDATE `%s` SET status = '%s' WHERE run_id = '%s'",
                table, status, runId);
        try {
            bq.query(QueryJobConfiguration.newBuilder(sql).setUseLegacySql(false).build());
            LOG.info("Lookup table: run_id={} → {}", runId, status);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted updating status to " + status, e);
        }
    }

    // ── Pipeline graph ────────────────────────────────────────────────────────

    public static void buildPipeline(Pipeline pipeline, Options options,
                                      String windowStart, String windowEnd) {

        // ── Schemas ───────────────────────────────────────────────────────────
        SchemaRegistry registry = SchemaRegistry.getInstance();
        Schema payloadSchema    = registry.get(SchemaRegistry.PAYLOAD_ROW);
        Schema pendingSchema    = registry.get(SchemaRegistry.PENDING_ROW);

        // ── Parse segment configs ─────────────────────────────────────────────
        List<SegmentConfig> segments = SegmentConfig.parse(options.getSegmentConfigs().get());
        LOG.info("Pipeline configured with {} segment(s): {}",
                segments.size(), segments.stream().map(s -> s.name).toList());

        String aiTable      = options.getAiSourceTable().get().replace(':', '.');
        String humanTable   = options.getHumanSourceTable().get().replace(':', '.');
        String pendingTable = options.getPendingTable().get().replace(':', '.');

        // ── 1 & 2. Per-segment source and pending reads ───────────────────────
        PCollectionList<KV<String, TableRow>> sourceParts  = PCollectionList.empty(pipeline);
        PCollectionList<KV<String, TableRow>> pendingParts = PCollectionList.empty(pipeline);

        for (SegmentConfig seg : segments) {

            // ── AI source rows for this segment ───────────────────────────────
            String aiQuery = String.format(
                    "SELECT * FROM `%s`"
                            + " WHERE created_at >= '%s' AND created_at < '%s'"
                            + " AND method = '%s'",
                    aiTable, windowStart, windowEnd, seg.aiMethod);

            PCollection<KV<String, TableRow>> keyedAi = pipeline
                    .apply("ReadAi-" + seg.name,
                            BigQueryIO.readTableRows().fromQuery(aiQuery).usingStandardSql())
                    .apply("KeyAi-" + seg.name,
                            ParDo.of(DecryptAndKeyFn.forAi(
                                    options.getFirestoreCollection(),
                                    options.getKmsKeyPath(),
                                    options.getImageNameField(),
                                    options.getSegmentConfigs())));

            // ── Human source rows for this segment ────────────────────────────
            String humanQuery = String.format(
                    "SELECT * FROM `%s`"
                            + " WHERE created_at >= '%s' AND created_at < '%s'"
                            + " AND method = '%s'",
                    humanTable, windowStart, windowEnd, seg.humanMethod);

            PCollection<KV<String, TableRow>> keyedHuman = pipeline
                    .apply("ReadHuman-" + seg.name,
                            BigQueryIO.readTableRows().fromQuery(humanQuery).usingStandardSql())
                    .apply("KeyHuman-" + seg.name,
                            ParDo.of(DecryptAndKeyFn.forHuman(
                                    options.getFirestoreCollection(),
                                    options.getKmsKeyPath(),
                                    options.getImageNameField(),
                                    options.getSegmentConfigs(),
                                    options.getHumanFilterField(),
                                    options.getHumanFilterValue())));

            // ── Pending rows for this segment ─────────────────────────────────
            // QUALIFY deduplicates across WRITE_APPEND accumulation: pick the
            // latest re-pended version of each (image_id, segment) pair.
            String pendingQuery = String.format(
                    "SELECT * FROM `%s`"
                            + " WHERE segment = '%s'"
                            + " AND first_seen_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(),"
                            + " INTERVAL %d DAY)"
                            + " QUALIFY ROW_NUMBER() OVER"
                            + " (PARTITION BY image_id, segment ORDER BY retry_count DESC) = 1",
                    pendingTable, seg.name, FilterAndPairFn.MAX_WAIT_DAYS);

            PCollection<KV<String, TableRow>> keyedPending = pipeline
                    .apply("ReadPending-" + seg.name,
                            BigQueryIO.readTableRows()
                                    .fromQuery(pendingQuery)
                                    .usingStandardSql())
                    .apply("KeyPending-" + seg.name,
                            WithKeys.of((TableRow row) ->
                                    row.get("image_id") + "::" + seg.name)
                                    .withKeyType(TypeDescriptors.strings()));

            sourceParts  = sourceParts.and(keyedAi).and(keyedHuman);
            pendingParts = pendingParts.and(keyedPending);
        }

        PCollection<KV<String, TableRow>> keyedSource =
                sourceParts.apply("FlattenSourceRows", Flatten.pCollections());

        PCollection<KV<String, TableRow>> keyedPending =
                pendingParts.apply("FlattenPendingRows", Flatten.pCollections());

        // ── 4. Co-group source + pending by "imageId::segment" ────────────────
        PCollection<KV<String, CoGbkResult>> coGrouped =
                KeyedPCollectionTuple
                        .of(FilterAndPairFn.SOURCE_TAG, keyedSource)
                        .and(FilterAndPairFn.PENDING_TAG, keyedPending)
                        .apply("CoGroupByImageIdAndSegment", CoGroupByKey.create());

        // ── 5. Filter & pair ──────────────────────────────────────────────────
        PCollectionTuple routed = coGrouped.apply(
                "FilterAndPair",
                ParDo.of(new FilterAndPairFn(options.getSegmentConfigs(), payloadSchema, pendingSchema))
                     .withOutputTags(
                             FilterAndPairFn.MATCHED,
                             TupleTagList
                                     .of(FilterAndPairFn.NEW_PENDING)
                                     .and(FilterAndPairFn.AGED_OUT)));

        // ── Register Avro coders ──────────────────────────────────────────────
        AvroCoder<GenericRecord> payloadCoder = AvroCoder.of(GenericRecord.class, payloadSchema);
        AvroCoder<GenericRecord> pendingCoder = AvroCoder.of(GenericRecord.class, pendingSchema);

        PCollection<KV<String, KV<GenericRecord, GenericRecord>>> matched =
                routed.get(FilterAndPairFn.MATCHED)
                      .setCoder(KvCoder.of(StringUtf8Coder.of(),
                              KvCoder.of(payloadCoder, payloadCoder)));

        PCollection<GenericRecord> newPending =
                routed.get(FilterAndPairFn.NEW_PENDING).setCoder(pendingCoder);

        PCollection<GenericRecord> agedOut =
                routed.get(FilterAndPairFn.AGED_OUT).setCoder(pendingCoder);

        // ── 6. Flatten & compare matched pairs ────────────────────────────────
        PCollection<TableRow> comparisonResults = matched
                .apply("FlattenAndCompare",
                        ParDo.of(new FlattenAndCompareFn(
                                options.getFirestoreCollection(),
                                options.getKmsKeyPath())));

        // ── 7. Write comparison results ───────────────────────────────────────
        comparisonResults
                .apply("WriteResults",
                        BigQueryIO.writeTableRows()
                                .to(options.getOutputTable())
                                .withSchema(SchemaUtil.comparisonResultsSchema())
                                .withWriteDisposition(WriteDisposition.WRITE_APPEND)
                                .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED));

        // ── 8. Append new pending rows ────────────────────────────────────────
        newPending
                .apply("MapPendingToTableRow",
                        MapElements.into(TypeDescriptor.of(TableRow.class))
                                   .via(r -> fromPendingRecord(r)))
                .apply("WritePendingTable",
                        BigQueryIO.writeTableRows()
                                .to(options.getPendingTable())
                                .withSchema(SchemaUtil.pendingSchema())
                                .withWriteDisposition(WriteDisposition.WRITE_APPEND)
                                .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED));

        // ── 9. Append aged-out rows to dead-letter table ──────────────────────
        agedOut
                .apply("MapAgedOutToTableRow",
                        MapElements.into(TypeDescriptor.of(TableRow.class))
                                   .via(r -> fromAgedOutRecord(r)))
                .apply("WriteDeadLetterTable",
                        BigQueryIO.writeTableRows()
                                .to(options.getDeadLetterTable())
                                .withSchema(SchemaUtil.deadLetterSchema())
                                .withWriteDisposition(WriteDisposition.WRITE_APPEND)
                                .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED));

        // ── 10. Flatten aged-out payloads → mismatch rows ─────────────────────
        agedOut
                .apply("FlattenAgedOutPayloads",
                        ParDo.of(new OrphanCompareFn(
                                options.getFirestoreCollection(),
                                options.getKmsKeyPath())))
                .apply("WriteAgedOutResults",
                        BigQueryIO.writeTableRows()
                                .to(options.getOutputTable())
                                .withSchema(SchemaUtil.comparisonResultsSchema())
                                .withWriteDisposition(WriteDisposition.WRITE_APPEND)
                                .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED));

        // ── 11. Flatten still-pending payloads → snapshot table ───────────────
        newPending
                .apply("FlattenPendingPayloads",
                        ParDo.of(new OrphanCompareFn(
                                options.getFirestoreCollection(),
                                options.getKmsKeyPath())))
                .apply("WritePendingSnapshot",
                        BigQueryIO.writeTableRows()
                                .to(options.getPendingSnapshotTable())
                                .withSchema(SchemaUtil.comparisonResultsSchema())
                                .withWriteDisposition(WriteDisposition.WRITE_TRUNCATE)
                                .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED));

        LOG.info("Pipeline graph constructed. window={} → {}", windowStart, windowEnd);
    }

    // ── Row conversion helpers ────────────────────────────────────────────────

    private static TableRow fromPendingRecord(GenericRecord r) {
        return new TableRow()
                .set("image_id",        str(r.get("image_id")))
                .set("key_id",          str(r.get("key_id")))
                .set("segment",         str(r.get("segment")) != null
                        ? str(r.get("segment")) : "main")
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
                .set("segment",       str(r.get("segment")) != null
                        ? str(r.get("segment")) : "main")
                .set("pending_type",  str(r.get("pending_type")))
                .set("payload",       str(r.get("payload")))
                .set("created_at",    str(r.get("created_at")))
                .set("first_seen_at", str(r.get("first_seen_at")))
                .set("aged_out_at",   TimestampUtil.formatInstant(Instant.now()))
                .set("reason",        "No counterpart payload after "
                        + FilterAndPairFn.MAX_WAIT_DAYS + " days");
    }

    private static String str(Object value) {
        return value != null ? value.toString() : null;
    }
}
