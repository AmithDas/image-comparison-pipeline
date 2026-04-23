package com.yourorg.pipeline;

import com.google.api.services.bigquery.model.TableRow;
import com.yourorg.pipeline.transforms.DecryptAndKeyFn;
import com.yourorg.pipeline.transforms.FilterAndPairFn;
import com.yourorg.pipeline.transforms.FlattenAndCompareFn;
import com.yourorg.pipeline.util.SchemaRegistry;
import com.yourorg.pipeline.util.SchemaUtil;
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
import java.util.Collections;
import java.util.Map;

/**
 * Entry point for the Image Comparison Dataflow pipeline.
 *
 * <p>Pipeline overview:
 * <ol>
 *   <li>Read windowed rows from separate AI and human BigQuery source tables.</li>
 *   <li>Read current pending state from {@code pending_comparisons}.</li>
 *   <li>Decrypt each source row; extract the image identifier; co-group with pending.</li>
 *   <li>Run {@link FilterAndPairFn} to merge source + pending, check eligibility,
 *       and route to MATCHED / NEW_PENDING / AGED_OUT.</li>
 *   <li>Run {@link FlattenAndCompareFn} on MATCHED pairs to produce field-level results.</li>
 *   <li>Write results to {@code image_comparison_results}.</li>
 *   <li>Overwrite {@code pending_comparisons} with only still-pending rows (WRITE_TRUNCATE).</li>
 *   <li>Append aged-out rows to {@code dead_letter_comparisons}.</li>
 * </ol>
 *
 * <p>Avro schemas for internal records are loaded from the {@link SchemaRegistry}
 * (classpath {@code /avro/<name>.json}) and passed directly to DoFns that need them,
 * so no DoFn accesses the registry or any static schema constant.
 *
 * <p>All pipeline options use {@link ValueProvider} so that values can be resolved
 * lazily on Dataflow workers rather than only at graph-construction time.
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

        @Description("BigQuery table used as a durable pending state store for "
                + "orphaned payloads awaiting their counterpart. "
                + "Format: project:dataset.table")
        @Validation.Required
        ValueProvider<String> getPendingTable();
        void setPendingTable(ValueProvider<String> value);

        @Description("BigQuery table for payloads that exceeded the maximum wait "
                + "threshold (default 7 days). Format: project:dataset.table")
        @Validation.Required
        ValueProvider<String> getDeadLetterTable();
        void setDeadLetterTable(ValueProvider<String> value);

        @Description("Inclusive start of the processing window (ISO-8601, e.g. 2026-04-16T00:00:00Z). "
                + "Only source rows with created_at >= windowStart are read.")
        @Validation.Required
        ValueProvider<String> getWindowStart();
        void setWindowStart(ValueProvider<String> value);

        @Description("Exclusive end of the processing window (ISO-8601, e.g. 2026-04-17T00:00:00Z). "
                + "Only source rows with created_at < windowEnd are read.")
        @Validation.Required
        ValueProvider<String> getWindowEnd();
        void setWindowEnd(ValueProvider<String> value);

        @Description("Value of the 'method' column that identifies an AI payload (e.g. 'aimetadata').")
        @Validation.Required
        ValueProvider<String> getAiMethod();
        void setAiMethod(ValueProvider<String> value);

        @Description("Value of the 'method' column that identifies a human payload "
                + "(e.g. 'controller.SubmitDispute').")
        @Validation.Required
        ValueProvider<String> getHumanMethod();
        void setHumanMethod(ValueProvider<String> value);

        @Description("Comma-separated sort keys per array path, e.g. 'terms=code,items=id'. "
                + "Omit to sort by full element JSON string.")
        ValueProvider<String> getArraySortKeys();
        void setArraySortKeys(ValueProvider<String> value);

        @Description("Dot-notation path to the image identifier field inside the decrypted payload. "
                + "Supports nested fields and array indexing, e.g. 'queueImages[0].fileName'. "
                + "Rows where this field is absent or null are skipped.")
        @Default.String("queueImages[0].fileName")
        ValueProvider<String> getImageNameField();
        void setImageNameField(ValueProvider<String> value);

        @Description("Dot-notation JSON field name inside the decrypted human payload to filter on. "
                + "Only human rows where this field equals --humanFilterValue are processed. "
                + "Omit to skip filtering.")
        ValueProvider<String> getHumanFilterField();
        void setHumanFilterField(ValueProvider<String> value);

        @Description("Expected value for --humanFilterField. Required when --humanFilterField is set.")
        ValueProvider<String> getHumanFilterValue();
        void setHumanFilterValue(ValueProvider<String> value);

        @Description("Firestore collection name that stores wrapped DEKs. "
                + "Each document ID is a key_id; the document must contain a "
                + "'wrapped_dek' field with the base64-encoded KMS-wrapped DEK.")
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

    /**
     * Builds the pipeline graph. Separated from {@link #main} for testability.
     *
     * <p>Schemas are fetched from the {@link SchemaRegistry} here and passed to
     * DoFns that need them — no DoFn accesses the registry directly.
     */
    public static void buildPipeline(Pipeline pipeline, Options options) {

        // ── Schemas — fetched once here and injected into DoFns ───────────────
        SchemaRegistry registry    = SchemaRegistry.getInstance();
        Schema payloadSchema       = registry.get(SchemaRegistry.PAYLOAD_ROW);
        Schema pendingSchema       = registry.get(SchemaRegistry.PENDING_ROW);

        // ── 1. Read AI and human source tables separately ─────────────────────
        String aiQuery = String.format(
                "SELECT * FROM `%s`"
                        + " WHERE created_at >= '%s' AND created_at < '%s'"
                        + " AND method = '%s'",
                options.getAiSourceTable().get().replace(':', '.'),
                options.getWindowStart().get(),
                options.getWindowEnd().get(),
                options.getAiMethod().get());

        PCollection<TableRow> aiRows = pipeline.apply(
                "ReadAiPayloads",
                BigQueryIO.readTableRows()
                        .fromQuery(aiQuery)
                        .usingStandardSql());

        String humanQuery = String.format(
                "SELECT * FROM `%s`"
                        + " WHERE created_at >= '%s' AND created_at < '%s'"
                        + " AND method = '%s'",
                options.getHumanSourceTable().get().replace(':', '.'),
                options.getWindowStart().get(),
                options.getWindowEnd().get(),
                options.getHumanMethod().get());

        PCollection<TableRow> humanRows = pipeline.apply(
                "ReadHumanPayloads",
                BigQueryIO.readTableRows()
                        .fromQuery(humanQuery)
                        .usingStandardSql());

        // ── 2. Read pending table ─────────────────────────────────────────────
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
                .apply("KeyPendingById",
                        WithKeys.of((TableRow row) -> (String) row.get("image_id"))
                                .withKeyType(TypeDescriptors.strings()));

        // ── 3. Decrypt + key AI rows by image name ────────────────────────────
        PCollection<KV<String, TableRow>> keyedAi = aiRows
                .apply("KeyAiById",
                        ParDo.of(DecryptAndKeyFn.forAi(
                                options.getFirestoreCollection(),
                                options.getKmsKeyPath(),
                                options.getImageNameField())));

        // ── 4. Decrypt + filter + key human rows by image name ────────────────
        // filterField/filterValue resolved here; ValueProviders passed for runtime fields.
        String filterField = options.getHumanFilterField() != null
                ? options.getHumanFilterField().get() : null;
        String filterValue = options.getHumanFilterValue() != null
                ? options.getHumanFilterValue().get() : null;

        PCollection<KV<String, TableRow>> keyedHuman = humanRows
                .apply("KeyHumanById",
                        ParDo.of(DecryptAndKeyFn.forHuman(
                                options.getFirestoreCollection(),
                                options.getKmsKeyPath(),
                                options.getImageNameField(),
                                filterField,
                                filterValue)));

        // ── 5. Merge keyed AI + human into one source PCollection ─────────────
        PCollection<KV<String, TableRow>> keyedSource =
                PCollectionList.of(keyedAi).and(keyedHuman)
                        .apply("FlattenKeyedSource", Flatten.pCollections());

        // ── 6. Co-group source + pending by image name ────────────────────────
        PCollection<KV<String, CoGbkResult>> coGrouped =
                KeyedPCollectionTuple
                        .of(FilterAndPairFn.SOURCE_TAG, keyedSource)
                        .and(FilterAndPairFn.PENDING_TAG, keyedPending)
                        .apply("CoGroupByImageId", CoGroupByKey.create());

        // ── 7. Filter & pair — schemas injected, no static registry access ────
        PCollectionTuple routed = coGrouped.apply(
                "FilterAndPair",
                ParDo.of(new FilterAndPairFn(
                                options.getAiMethod().get(),
                                options.getHumanMethod().get(),
                                payloadSchema,
                                pendingSchema))
                     .withOutputTags(
                             FilterAndPairFn.MATCHED,
                             TupleTagList
                                     .of(FilterAndPairFn.NEW_PENDING)
                                     .and(FilterAndPairFn.AGED_OUT)));

        // ── Register Avro coders for GenericRecord serialisation ──────────────
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

        // ── 8. Flatten JSON + field-level comparison ──────────────────────────
        PCollection<TableRow> comparisonResults = matched
                .apply("FlattenAndCompare",
                        ParDo.of(new FlattenAndCompareFn(
                                parseArraySortKeys(options.getArraySortKeys()),
                                options.getFirestoreCollection(),
                                options.getKmsKeyPath())));

        // ── 9. Write comparison results ───────────────────────────────────────
        comparisonResults.apply(
                "WriteComparisonResults",
                BigQueryIO.writeTableRows()
                        .to(options.getOutputTable())
                        .withSchema(SchemaUtil.comparisonResultsSchema())
                        .withWriteDisposition(WriteDisposition.WRITE_APPEND)
                        .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED));

        // ── 10. Overwrite pending table (WRITE_TRUNCATE = implicit cleanup) ───
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

        // ── 11. Append aged-out rows to dead-letter table ─────────────────────
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

    private static Map<String, String> parseArraySortKeys(ValueProvider<String> raw) {
        if (raw == null || !raw.isAccessible() || raw.get() == null || raw.get().isBlank()) {
            return Collections.emptyMap();
        }
        Map<String, String> map = new java.util.HashMap<>();
        for (String entry : raw.get().split(",")) {
            String[] parts = entry.split("=", 2);
            if (parts.length == 2 && !parts[0].isBlank() && !parts[1].isBlank()) {
                map.put(parts[0].trim(), parts[1].trim());
            }
        }
        return map;
    }
}
