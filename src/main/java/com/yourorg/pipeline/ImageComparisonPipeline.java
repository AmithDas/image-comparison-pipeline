package com.yourorg.pipeline;

import com.google.api.services.bigquery.model.TableRow;
import com.yourorg.pipeline.config.RouteConfig;
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
import org.apache.beam.sdk.transforms.Filter;
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
 *   <li>Write results to each route's configured output table.</li>
 *   <li>Overwrite {@code pending_comparisons} with only still-pending rows (WRITE_TRUNCATE).</li>
 *   <li>Append aged-out rows to {@code dead_letter_comparisons}.</li>
 *   <li>Flatten orphaned (aged-out) payloads and write mismatch rows to route tables.</li>
 *   <li>Flatten still-pending payloads to a snapshot table (WRITE_TRUNCATE).</li>
 * </ol>
 *
 * <p>Routes are configured entirely via the {@code --routeConfigs} option — no code
 * changes are needed to add or remove a route.  Each route entry specifies its own
 * AI method name, human method name, and destination BigQuery table.
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
 *       --pendingTable=project:dataset.pending_comparisons \
 *       --deadLetterTable=project:dataset.dead_letter_comparisons \
 *       --pendingSnapshotTable=project:dataset.pending_snapshot \
 *       --windowStart=2026-04-16T00:00:00Z \
 *       --windowEnd=2026-04-17T00:00:00Z \
 *       --routeConfigs='[{\"route\":\"main\",\"aiMethod\":\"aimetadata\",\"humanMethod\":\"controller.SubmitDispute\",\"outputTable\":\"project:dataset.main_results\"},{\"route\":\"authentication\",\"aiMethod\":\"auth.ai\",\"humanMethod\":\"auth.human\",\"outputTable\":\"project:dataset.auth_results\"},{\"route\":\"docproof\",\"aiMethod\":\"docproof.ai\",\"humanMethod\":\"docproof.human\",\"outputTable\":\"project:dataset.docproof_results\"}]' \
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

        @Description("BigQuery table for still-pending payload snapshot. "
                + "Same schema as each route output table. Truncated and rewritten on every "
                + "run — records automatically disappear once their counterpart is matched. "
                + "Format: project:dataset.table")
        @Validation.Required
        ValueProvider<String> getPendingSnapshotTable();
        void setPendingSnapshotTable(ValueProvider<String> value);

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

        @Description(
                "JSON array of route configurations. Each entry must contain: "
                + "route (unique name), aiMethod (method column value for AI rows), "
                + "humanMethod (method column value for human rows), "
                + "outputTable (destination BQ table, format project:dataset.table). "
                + "Example: [{\"route\":\"main\",\"aiMethod\":\"aimetadata\","
                + "\"humanMethod\":\"controller.SubmitDispute\","
                + "\"outputTable\":\"project:dataset.main_results\"}]")
        @Validation.Required
        ValueProvider<String> getRouteConfigs();
        void setRouteConfigs(ValueProvider<String> value);

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
     * <p>Route configs are parsed eagerly here so that any configuration errors
     * (missing fields, duplicate routes, etc.) are caught at pipeline-construction
     * time rather than on workers.
     *
     * <p>Schemas are fetched from the {@link SchemaRegistry} and passed to DoFns
     * that need them — no DoFn accesses the registry directly.
     */
    public static void buildPipeline(Pipeline pipeline, Options options) {

        // ── Schemas — fetched once here and injected into DoFns ───────────────
        SchemaRegistry registry = SchemaRegistry.getInstance();
        Schema payloadSchema    = registry.get(SchemaRegistry.PAYLOAD_ROW);
        Schema pendingSchema    = registry.get(SchemaRegistry.PENDING_ROW);

        // ── Parse route configs ───────────────────────────────────────────────
        List<RouteConfig> routes = RouteConfig.parse(options.getRouteConfigs().get());
        Map<String, String> methodToSide = RouteConfig.buildMethodToSideMap(routes);
        LOG.info("Pipeline configured with {} route(s): {}",
                routes.size(), routes.stream().map(r -> r.route).toList());

        String aiTable     = options.getAiSourceTable().get().replace(':', '.');
        String humanTable  = options.getHumanSourceTable().get().replace(':', '.');
        String windowStart = options.getWindowStart().get();
        String windowEnd   = options.getWindowEnd().get();

        String filterField = options.getHumanFilterField() != null
                ? options.getHumanFilterField().get() : null;
        String filterValue = options.getHumanFilterValue() != null
                ? options.getHumanFilterValue().get() : null;

        // ── 1. Read + key source rows for every configured route ──────────────
        // Rows are keyed as "imageId::routeName" so FilterAndPairFn processes
        // each route's AI/human payloads independently.
        PCollectionList<KV<String, TableRow>> keyedSources = PCollectionList.empty(pipeline);
        for (RouteConfig route : routes) {
            keyedSources = addRouteSource(pipeline, keyedSources, options,
                    route.route, route.aiMethod, route.humanMethod,
                    aiTable, humanTable, windowStart, windowEnd,
                    filterField, filterValue);
        }

        PCollection<KV<String, TableRow>> keyedSource =
                keyedSources.apply("FlattenKeyedSource", Flatten.pCollections());

        // ── 2. Read pending table — key as "imageId::route" ───────────────────
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
                .apply("KeyPendingByIdAndRoute",
                        WithKeys.of((TableRow row) -> {
                            String imgId = (String) row.get("image_id");
                            String route = row.get("route") != null
                                    ? (String) row.get("route") : FlattenAndCompareFn.ROUTE_MAIN;
                            return imgId + "::" + route;
                        }).withKeyType(TypeDescriptors.strings()));

        // ── 3. Co-group source + pending by "imageId::route" ──────────────────
        PCollection<KV<String, CoGbkResult>> coGrouped =
                KeyedPCollectionTuple
                        .of(FilterAndPairFn.SOURCE_TAG, keyedSource)
                        .and(FilterAndPairFn.PENDING_TAG, keyedPending)
                        .apply("CoGroupByImageIdAndRoute", CoGroupByKey.create());

        // ── 4. Filter & pair ──────────────────────────────────────────────────
        // methodToSide covers every method across all routes so FilterAndPairFn
        // can classify AI vs human without knowing which route it's processing.
        PCollectionTuple routed = coGrouped.apply(
                "FilterAndPair",
                ParDo.of(new FilterAndPairFn(methodToSide, payloadSchema, pendingSchema))
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
        // Output: KV<routeName, TableRow> — routeName is extracted from the pair
        // key ("imageId::routeName::iteration") set by FilterAndPairFn.
        PCollection<KV<String, TableRow>> routedResults = matched
                .apply("FlattenAndCompare",
                        ParDo.of(new FlattenAndCompareFn(
                                options.getFirestoreCollection(),
                                options.getKmsKeyPath())));

        // ── 12. Flatten aged-out payloads → mismatch rows in route tables ─────
        // Each field in the orphaned payload is emitted with one side null
        // (is_match = false always) so downstream queries can see which fields
        // were present on the unmatched side.
        PCollection<KV<String, TableRow>> agedOutResults = agedOut
                .apply("FlattenAgedOutPayloads",
                        ParDo.of(new OrphanCompareFn(
                                options.getFirestoreCollection(),
                                options.getKmsKeyPath())));

        // ── 9 + 12a. Write matched and aged-out results per route ─────────────
        for (RouteConfig route : routes) {
            writeRoute(routedResults, route.route, route.outputTable,
                    "WriteResults-" + route.route);
            writeRoute(agedOutResults, route.route, route.outputTable,
                    "WriteAgedOutResults-" + route.route);
        }

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

        // ── 13. Flatten still-pending payloads → dedicated snapshot table ─────
        // Written with WRITE_TRUNCATE so the table always reflects the current
        // pending state. When a counterpart is found in a future run, the record
        // leaves the pending table and is automatically absent from the next
        // snapshot — no manual cleanup needed.
        newPending
                .apply("FlattenPendingPayloads",
                        ParDo.of(new OrphanCompareFn(
                                options.getFirestoreCollection(),
                                options.getKmsKeyPath())))
                .apply("ExtractPendingRows",
                        MapElements.into(TypeDescriptor.of(TableRow.class))
                                   .via(KV::getValue))
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
                .set("route",           str(r.get("route")) != null
                        ? str(r.get("route")) : FlattenAndCompareFn.ROUTE_MAIN)
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
                .set("route",         str(r.get("route")) != null
                        ? str(r.get("route")) : FlattenAndCompareFn.ROUTE_MAIN)
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

    // ── Route source helper ───────────────────────────────────────────────────

    /**
     * Reads AI + human rows for {@code routeName} (filtered by their respective
     * method values), keys them as {@code "imageId::routeName"}, and appends both
     * keyed PCollections to {@code list}.
     *
     * <p>{@code filterField} / {@code filterValue} are applied to human rows only
     * (e.g. to select rows with a specific status field value).  Pass {@code null}
     * to skip filtering.
     */
    private static PCollectionList<KV<String, TableRow>> addRouteSource(
            Pipeline pipeline,
            PCollectionList<KV<String, TableRow>> list,
            Options options,
            String routeName,
            String aiMethod, String humanMethod,
            String aiTable, String humanTable,
            String windowStart, String windowEnd,
            String filterField, String filterValue) {

        String aiQuery = String.format(
                "SELECT * FROM `%s` WHERE created_at >= '%s' AND created_at < '%s'"
                        + " AND method = '%s'",
                aiTable, windowStart, windowEnd, aiMethod);

        String humanQuery = String.format(
                "SELECT * FROM `%s` WHERE created_at >= '%s' AND created_at < '%s'"
                        + " AND method = '%s'",
                humanTable, windowStart, windowEnd, humanMethod);

        PCollection<KV<String, TableRow>> keyedAi = pipeline
                .apply("Read-" + routeName + "-Ai",
                        BigQueryIO.readTableRows().fromQuery(aiQuery).usingStandardSql())
                .apply("Key-" + routeName + "-Ai",
                        ParDo.of(DecryptAndKeyFn.forAi(
                                options.getFirestoreCollection(),
                                options.getKmsKeyPath(),
                                options.getImageNameField(),
                                routeName)));

        PCollection<KV<String, TableRow>> keyedHuman = pipeline
                .apply("Read-" + routeName + "-Human",
                        BigQueryIO.readTableRows().fromQuery(humanQuery).usingStandardSql())
                .apply("Key-" + routeName + "-Human",
                        ParDo.of(DecryptAndKeyFn.forHuman(
                                options.getFirestoreCollection(),
                                options.getKmsKeyPath(),
                                options.getImageNameField(),
                                routeName,
                                filterField, filterValue)));

        return list.and(keyedAi).and(keyedHuman);
    }

    // ── Route write helper ────────────────────────────────────────────────────

    /**
     * Filters {@code routedResults} to rows whose key equals {@code route} and
     * appends them to {@code outputTable}.
     *
     * @param routedResults keyed comparison rows (key = route name)
     * @param route         the route name to select
     * @param outputTable   destination BigQuery table (format: {@code project:dataset.table})
     * @param stepName      unique Beam step name prefix for this write
     */
    private static void writeRoute(
            PCollection<KV<String, TableRow>> routedResults,
            String route,
            String outputTable,
            String stepName) {
        routedResults
                .apply("Filter-" + stepName,
                        Filter.by(kv -> route.equals(kv.getKey())))
                .apply("Extract-" + stepName,
                        MapElements.into(TypeDescriptor.of(TableRow.class))
                                   .via(KV::getValue))
                .apply(stepName,
                        BigQueryIO.writeTableRows()
                                .to(outputTable)
                                .withSchema(SchemaUtil.comparisonResultsSchema())
                                .withWriteDisposition(WriteDisposition.WRITE_APPEND)
                                .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED));
    }
}
