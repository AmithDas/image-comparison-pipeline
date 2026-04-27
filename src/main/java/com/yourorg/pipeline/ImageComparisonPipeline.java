package com.yourorg.pipeline;

import com.google.api.services.bigquery.model.TableRow;
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
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;

/**
 * Entry point for the Image Comparison Dataflow pipeline.
 *
 * <p>The pipeline is intentionally route-agnostic: it processes one AI/human
 * method pair and writes results to one output table. Segment separation
 * (authentication, docproof, etc.) is handled by the DAG, which runs a
 * separate Dataflow job per segment, each with its own parameters.
 *
 * <p>Pipeline overview:
 * <ol>
 *   <li>Read windowed AI and human rows from BigQuery source tables.</li>
 *   <li>Read current pending state from {@code pending_comparisons}.</li>
 *   <li>Decrypt each source row, extract the image identifier, and co-group with pending.</li>
 *   <li>Run {@link FilterAndPairFn}: merge source + pending, check eligibility,
 *       route to MATCHED / NEW_PENDING / AGED_OUT.</li>
 *   <li>Run {@link FlattenAndCompareFn} on MATCHED pairs → field-level comparison rows.</li>
 *   <li>Write results to {@code --outputTable}.</li>
 *   <li>Overwrite {@code pending_comparisons} with still-pending rows (WRITE_TRUNCATE).</li>
 *   <li>Append aged-out rows to {@code dead_letter_comparisons}.</li>
 *   <li>Flatten orphaned payloads → mismatch rows in the output table.</li>
 *   <li>Flatten still-pending payloads → snapshot table (WRITE_TRUNCATE).</li>
 * </ol>
 *
 * <p>Run locally (DirectRunner):
 * <pre>
 *   mvn compile exec:java \
 *     -Dexec.mainClass=com.yourorg.pipeline.ImageComparisonPipeline \
 *     -Dexec.args="--runner=DirectRunner \
 *       --aiSourceTable=project:ai_dataset.ai_payloads \
 *       --humanSourceTable=project:human_dataset.human_payloads \
 *       --outputTable=project:dataset.comparison_results \
 *       --pendingTable=project:dataset.pending_comparisons \
 *       --deadLetterTable=project:dataset.dead_letter_comparisons \
 *       --pendingSnapshotTable=project:dataset.pending_snapshot \
 *       --windowStart=2026-04-16T00:00:00Z \
 *       --windowEnd=2026-04-17T00:00:00Z \
 *       --aiMethod=aimetadata \
 *       --humanMethod=controller.SubmitDispute \
 *       --firestoreCollection=dek_store \
 *       --kmsKeyPath=projects/p/locations/l/keyRings/r/cryptoKeys/k"
 * </pre>
 */
public class ImageComparisonPipeline {

    private static final Logger LOG = LoggerFactory.getLogger(ImageComparisonPipeline.class);

    // ── Pipeline options ──────────────────────────────────────────────────────

    public interface Options extends PipelineOptions {

        @Description("BigQuery table containing AI payloads. Format: project:dataset.table")
        @Validation.Required
        ValueProvider<String> getAiSourceTable();
        void setAiSourceTable(ValueProvider<String> value);

        @Description("BigQuery table containing human payloads. Format: project:dataset.table")
        @Validation.Required
        ValueProvider<String> getHumanSourceTable();
        void setHumanSourceTable(ValueProvider<String> value);

        @Description("Output BigQuery table for field-level comparison results. "
                + "Format: project:dataset.table")
        @Validation.Required
        ValueProvider<String> getOutputTable();
        void setOutputTable(ValueProvider<String> value);

        @Description("BigQuery table used as a durable pending state store. "
                + "Format: project:dataset.table")
        @Validation.Required
        ValueProvider<String> getPendingTable();
        void setPendingTable(ValueProvider<String> value);

        @Description("BigQuery table for payloads that exceeded the maximum wait "
                + "threshold (default 7 days). Format: project:dataset.table")
        @Validation.Required
        ValueProvider<String> getDeadLetterTable();
        void setDeadLetterTable(ValueProvider<String> value);

        @Description("BigQuery table for the still-pending payload snapshot. "
                + "Truncated and rewritten on every run. Format: project:dataset.table")
        @Validation.Required
        ValueProvider<String> getPendingSnapshotTable();
        void setPendingSnapshotTable(ValueProvider<String> value);

        @Description("Inclusive start of the processing window (ISO-8601, e.g. 2026-04-16T00:00:00Z).")
        @Validation.Required
        ValueProvider<String> getWindowStart();
        void setWindowStart(ValueProvider<String> value);

        @Description("Exclusive end of the processing window (ISO-8601, e.g. 2026-04-17T00:00:00Z).")
        @Validation.Required
        ValueProvider<String> getWindowEnd();
        void setWindowEnd(ValueProvider<String> value);

        @Description("Value of the 'method' column that identifies an AI payload.")
        @Validation.Required
        ValueProvider<String> getAiMethod();
        void setAiMethod(ValueProvider<String> value);

        @Description("Value of the 'method' column that identifies a human payload.")
        @Validation.Required
        ValueProvider<String> getHumanMethod();
        void setHumanMethod(ValueProvider<String> value);

        @Description("Dot-notation path to the image identifier field inside the decrypted payload, "
                + "e.g. 'queueImages[0].fileName'. Rows where this field is absent are skipped.")
        @Default.String("queueImages[0].fileName")
        ValueProvider<String> getImageNameField();
        void setImageNameField(ValueProvider<String> value);

        @Description("Dot-notation JSON field name to filter human rows on. "
                + "Only rows where this field equals --humanFilterValue are processed. "
                + "Omit to skip filtering.")
        ValueProvider<String> getHumanFilterField();
        void setHumanFilterField(ValueProvider<String> value);

        @Description("Expected value for --humanFilterField. Required when --humanFilterField is set.")
        ValueProvider<String> getHumanFilterValue();
        void setHumanFilterValue(ValueProvider<String> value);

        @Description("Firestore collection name that stores wrapped DEKs.")
        @Validation.Required
        ValueProvider<String> getFirestoreCollection();
        void setFirestoreCollection(ValueProvider<String> value);

        @Description("Full Cloud KMS CryptoKey resource path used to unwrap DEKs. "
                + "Format: projects/P/locations/L/keyRings/R/cryptoKeys/K")
        @Validation.Required
        ValueProvider<String> getKmsKeyPath();
        void setKmsKeyPath(ValueProvider<String> value);
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

    public static void buildPipeline(Pipeline pipeline, Options options) {

        // ── Schemas ───────────────────────────────────────────────────────────
        SchemaRegistry registry = SchemaRegistry.getInstance();
        Schema payloadSchema    = registry.get(SchemaRegistry.PAYLOAD_ROW);
        Schema pendingSchema    = registry.get(SchemaRegistry.PENDING_ROW);

        String aiTable     = options.getAiSourceTable().get().replace(':', '.');
        String humanTable  = options.getHumanSourceTable().get().replace(':', '.');
        String windowStart = options.getWindowStart().get();
        String windowEnd   = options.getWindowEnd().get();
        String aiMethod    = options.getAiMethod().get();
        String humanMethod = options.getHumanMethod().get();

        String filterField = options.getHumanFilterField() != null
                ? options.getHumanFilterField().get() : null;
        String filterValue = options.getHumanFilterValue() != null
                ? options.getHumanFilterValue().get() : null;

        // ── 1. Read AI source rows, key by imageId ────────────────────────────
        String aiQuery = String.format(
                "SELECT * FROM `%s` WHERE created_at >= '%s' AND created_at < '%s'"
                        + " AND method = '%s'",
                aiTable, windowStart, windowEnd, aiMethod);

        PCollection<KV<String, TableRow>> keyedAi = pipeline
                .apply("ReadAiSource",
                        BigQueryIO.readTableRows().fromQuery(aiQuery).usingStandardSql())
                .apply("KeyAiByImageId",
                        ParDo.of(DecryptAndKeyFn.forAi(
                                options.getFirestoreCollection(),
                                options.getKmsKeyPath(),
                                options.getImageNameField())));

        // ── 2. Read human source rows, key by imageId ─────────────────────────
        String humanQuery = String.format(
                "SELECT * FROM `%s` WHERE created_at >= '%s' AND created_at < '%s'"
                        + " AND method = '%s'",
                humanTable, windowStart, windowEnd, humanMethod);

        PCollection<KV<String, TableRow>> keyedHuman = pipeline
                .apply("ReadHumanSource",
                        BigQueryIO.readTableRows().fromQuery(humanQuery).usingStandardSql())
                .apply("KeyHumanByImageId",
                        ParDo.of(DecryptAndKeyFn.forHuman(
                                options.getFirestoreCollection(),
                                options.getKmsKeyPath(),
                                options.getImageNameField(),
                                filterField, filterValue)));

        // ── 3. Read pending table, key by imageId ─────────────────────────────
        String pendingQuery = String.format(
                "SELECT * FROM `%s`"
                        + " WHERE first_seen_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(),"
                        + " INTERVAL %d DAY)",
                options.getPendingTable().get().replace(':', '.'),
                FilterAndPairFn.MAX_WAIT_DAYS);

        PCollection<KV<String, TableRow>> keyedPending = pipeline
                .apply("ReadPendingTable",
                        BigQueryIO.readTableRows()
                                .fromQuery(pendingQuery)
                                .usingStandardSql())
                .apply("KeyPendingByImageId",
                        WithKeys.of((TableRow row) -> (String) row.get("image_id"))
                                .withKeyType(TypeDescriptors.strings()));

        // ── 4. Co-group source + pending by imageId ───────────────────────────
        PCollection<KV<String, CoGbkResult>> coGrouped =
                KeyedPCollectionTuple
                        .of(FilterAndPairFn.SOURCE_TAG, keyedAi)
                        .and(FilterAndPairFn.SOURCE_TAG, keyedHuman)
                        .and(FilterAndPairFn.PENDING_TAG, keyedPending)
                        .apply("CoGroupByImageId", CoGroupByKey.create());

        // ── 5. Filter & pair ──────────────────────────────────────────────────
        PCollectionTuple routed = coGrouped.apply(
                "FilterAndPair",
                ParDo.of(new FilterAndPairFn(aiMethod, humanMethod, payloadSchema, pendingSchema))
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

        // ── 8. Overwrite pending table (WRITE_TRUNCATE = implicit cleanup) ────
        newPending
                .apply("MapPendingToTableRow",
                        MapElements.into(TypeDescriptor.of(TableRow.class))
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
                .set("aged_out_at",   TimestampUtil.formatInstant(Instant.now()))
                .set("reason",        "No counterpart payload after "
                        + FilterAndPairFn.MAX_WAIT_DAYS + " days");
    }

    private static String str(Object value) {
        return value != null ? value.toString() : null;
    }
}
