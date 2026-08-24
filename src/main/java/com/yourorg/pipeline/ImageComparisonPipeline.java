package com.yourorg.pipeline;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.yourorg.pipeline.transforms.DecryptAndKeyFn;
import com.yourorg.pipeline.transforms.FilterAndPairFn;
import com.yourorg.pipeline.transforms.FlattenAndCompareFn;
import com.yourorg.pipeline.util.SchemaRegistry;
import com.yourorg.pipeline.util.SchemaUtil;
import com.yourorg.pipeline.util.TimestampUtil;
import com.yourorg.pipeline.util.WindowManager;
import com.yourorg.pipeline.util.WindowManager.WindowInfo;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Wait;
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

import java.io.Serializable;
import java.time.Instant;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Entry point for the Image Comparison Dataflow pipeline.
 *
 * <p>All segments (main, authanddocreview, …) are processed in a single Dataflow job.
 * Source rows are routed to the correct segment by their {@code method} column value.
 * All comparison results are written to one shared table with a {@code segment} column.
 *
 * <h3>Window management (Classic Template)</h3>
 * Because this pipeline runs as a Dataflow <em>Classic Template</em>, {@code main()}
 * executes only at <em>template build time</em>.  Window management must therefore
 * happen on workers (which run with the Dataflow SA that already has BigQuery access).
 *
 * <p>This is achieved via {@link WindowValueProvider}: a {@link Serializable}
 * {@link ValueProvider} whose {@link ValueProvider#get()} method lazily calls
 * {@link WindowManager#claimWindow()} + {@link WindowManager#getWindow()} the
 * first time it is invoked on each worker JVM.  The result is cached, so
 * subsequent calls on the same JVM return the same window bounds.
 *
 * <p>{@link WindowManager#claimWindow()} uses a conditional UPDATE
 * ({@code WHERE current_stop = last_extracted}) so that only the first worker
 * wins; all others see a no-op and read the same {@code current_stop} value.
 *
 * <p>After all BigQuery writes succeed, {@link AdvanceWindowFn} calls
 * {@link WindowManager#advance()} via a {@link Wait#on} step connected to the
 * write results.  If any write fails, the exception propagates and advance is
 * never called — so the next run retries the same window automatically (because
 * {@code current_stop} is still {@code > last_extracted}, making the next
 * conditional UPDATE a no-op that reuses the existing {@code current_stop}).
 *
 * <p>The Airflow DAG is a pure scheduler — it only submits the Dataflow job.
 * No BigQuery access is required from the DAG service account.
 *
 * <p>Run locally (DirectRunner):
 * <pre>
 *   mvn compile exec:java \
 *     -Dexec.mainClass=com.yourorg.pipeline.ImageComparisonPipeline \
 *     -Dexec.args="--runner=DirectRunner \
 *       --lookupTable=project:dataset.pipeline_config \
 *       --pipelineName=image_comparison \
 *       --aiSourceTable=project:ai_dataset.ai_payloads \
 *       --humanSourceTable=project:human_dataset.human_payloads \
 *       --outputTable=project:dataset.comparison_results \
 *       --pendingTable=project:dataset.pending_comparisons \
 *       --aiPendingTable=project:dataset.ai_pending_comparisons \
 *       --deadLetterTable=project:dataset.dead_letter_comparisons \
 *       --scopeEventsTable=project:dataset.comparison_scope_events \
 *       --segmentConfigs='[...]' \
 *       --firestoreCollection=dek_store \
 *       --kmsKeyPath=projects/p/locations/l/keyRings/r/cryptoKeys/k"
 * </pre>
 */
public class ImageComparisonPipeline {

    private static final Logger LOG = LoggerFactory.getLogger(ImageComparisonPipeline.class);

    // ── Pipeline options ──────────────────────────────────────────────────────

    public interface Options extends PipelineOptions {

        @Description("BigQuery lookup/config table that stores the processing window state. "
                + "Schema: table_name STRING, last_extracted TIMESTAMP, current_stop TIMESTAMP. "
                + "Format: project:dataset.table")
        @Validation.Required
        ValueProvider<String> getLookupTable();
        void setLookupTable(ValueProvider<String> value);

        @Description("Value of the table_name column in --lookupTable that identifies "
                + "this pipeline's config row (e.g. 'image_comparison').")
        @Validation.Required
        ValueProvider<String> getPipelineName();
        void setPipelineName(ValueProvider<String> value);

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

        @Description("BigQuery table for case (human-side) pending state — cases awaiting more "
                + "AI iterations. Format: project:dataset.table")
        @Validation.Required
        ValueProvider<String> getPendingTable();
        void setPendingTable(ValueProvider<String> value);

        @Description("BigQuery table for the AI replay pool — AI payloads retained regardless of "
                + "match status so late-arriving cases can still be compared against AI history. "
                + "Format: project:dataset.table")
        @Validation.Required
        ValueProvider<String> getAiPendingTable();
        void setAiPendingTable(ValueProvider<String> value);

        @Description("BigQuery table for payloads that exceeded the max wait threshold. "
                + "Format: project:dataset.table")
        @Validation.Required
        ValueProvider<String> getDeadLetterTable();
        void setDeadLetterTable(ValueProvider<String> value);

        @Description("Append-only BigQuery table of AI scope discovered and human scope covered "
                + "events. Used to derive currently unmatched AI scopes. "
                + "Format: project:dataset.table")
        @Validation.Required
        ValueProvider<String> getScopeEventsTable();
        void setScopeEventsTable(ValueProvider<String> value);

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

        @Description("How far back (in days) to read human source rows relative to windowEnd. "
                + "Allows late-arriving human payloads to match AI rows from earlier windows. "
                + "Default: 180.")
        @Default.Integer(180)
        ValueProvider<Integer> getHumanLookbackDays();
        void setHumanLookbackDays(ValueProvider<Integer> value);

        @Description("How far back (in hours) to read AI source rows relative to windowEnd. "
                + "Allows AI payloads written late due to source-table backlog (created_at falls "
                + "in an already-processed window, but the row isn't actually inserted until "
                + "later) to still be picked up. Re-read rows are deduped against "
                + "ai_pending_comparisons by (payload, created_at), so widening this does not "
                + "produce duplicate matches. Default: 6.")
        @Default.Integer(6)
        ValueProvider<Integer> getAiLookbackHours();
        void setAiLookbackHours(ValueProvider<Integer> value);

        @Description("Existing BigQuery dataset for temporary query materialisation tables. "
                + "Prevents Dataflow from auto-creating a new temp dataset on each run. "
                + "Format: project:dataset  (e.g. your-project:pipeline_temp)")
        @Validation.Required
        ValueProvider<String> getQueryTempDataset();
        void setQueryTempDataset(ValueProvider<String> value);

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

    // ── Pipeline graph ────────────────────────────────────────────────────────

    static void buildPipeline(Pipeline pipeline, Options options) {

        // ── Schemas ───────────────────────────────────────────────────────────
        SchemaRegistry registry = SchemaRegistry.getInstance();
        Schema payloadSchema    = registry.get(SchemaRegistry.PAYLOAD_ROW);
        Schema pendingSchema    = registry.get(SchemaRegistry.PENDING_ROW);
        Schema aiPendingSchema  = registry.get(SchemaRegistry.AI_PENDING_ROW);

        // No ValueProvider.get() calls here — the graph structure is fixed
        // (one AI read, one human read, one pending read) regardless of how many
        // segments are configured.  All runtime parameters are resolved on workers.

        // Lazy window provider: first .get() per worker JVM claims the window
        // in BigQuery using the Dataflow SA, then caches the result. Only windowEnd
        // is read externally — both AI and human reads use a trailing lookback from
        // windowEnd rather than a hard windowStart floor, so late-arriving rows from
        // either source aren't permanently missed once their window has passed.
        ValueProvider<String> windowEnd =
                new WindowValueProvider(options.getLookupTable(), options.getPipelineName(), false);

        // queryTempDataset is static infrastructure — resolved at template build time.
        String queryTempDataset = options.getQueryTempDataset().get();

        // ── AI source rows (all segments in one read) ─────────────────────────
        PCollection<KV<String, TableRow>> keyedAi = pipeline
                .apply("ReadAi",
                        BigQueryIO.readTableRows()
                                .fromQuery(new AiSourceQueryProvider(
                                        options.getAiSourceTable(), windowEnd,
                                        options.getAiLookbackHours()))
                                .usingStandardSql()
                                .withQueryTempDataset(queryTempDataset))
                .apply("KeyAi",
                        ParDo.of(DecryptAndKeyFn.forAi(
                                options.getFirestoreCollection(),
                                options.getKmsKeyPath(),
                                options.getImageNameField(),
                                options.getSegmentConfigs())));

        // ── Human source rows — 180-day lookback ─────────────────────────────
        // Human payloads may arrive well after the AI payload; reading back
        // humanLookbackDays from windowEnd ensures late-arriving human rows
        // can still match pending AI rows from earlier windows.
        PCollection<KV<String, TableRow>> keyedHuman = pipeline
                .apply("ReadHuman",
                        BigQueryIO.readTableRows()
                                .fromQuery(new HumanSourceQueryProvider(
                                        options.getHumanSourceTable(), windowEnd,
                                        options.getHumanLookbackDays()))
                                .usingStandardSql()
                                .withQueryTempDataset(queryTempDataset))
                .apply("KeyHuman",
                        ParDo.of(DecryptAndKeyFn.forHuman(
                                options.getFirestoreCollection(),
                                options.getKmsKeyPath(),
                                options.getImageNameField(),
                                options.getSegmentConfigs(),
                                options.getHumanFilterField(),
                                options.getHumanFilterValue())));

        PCollection<KV<String, TableRow>> keyedSource =
                PCollectionList.of(keyedAi).and(keyedHuman)
                               .apply("FlattenSourceRows", Flatten.pCollections());

        // ── Case pending rows (human side, all segments in one read) ──────────
        PCollection<KV<String, TableRow>> keyedCasePending = pipeline
                .apply("ReadCasePending",
                        BigQueryIO.readTableRows()
                                .fromQuery(new PendingQueryProvider(options.getPendingTable()))
                                .usingStandardSql()
                                .withQueryTempDataset(queryTempDataset))
                .apply("KeyCasePending",
                        WithKeys.of((TableRow row) ->
                                row.get("image_id") + "::" + row.get("segment"))
                                .withKeyType(TypeDescriptors.strings()));

        // ── AI replay pool (all segments in one read) ─────────────────────────
        // Retained regardless of match status — see FilterAndPairFn class doc.
        PCollection<KV<String, TableRow>> keyedAiPending = pipeline
                .apply("ReadAiPending",
                        BigQueryIO.readTableRows()
                                .fromQuery(new PendingQueryProvider(options.getAiPendingTable()))
                                .usingStandardSql()
                                .withQueryTempDataset(queryTempDataset))
                .apply("KeyAiPending",
                        WithKeys.of((TableRow row) ->
                                row.get("image_id") + "::" + row.get("segment"))
                                .withKeyType(TypeDescriptors.strings()));

        // ── Co-group source + case pending + AI pending by "imageId::segment" ──
        PCollection<KV<String, CoGbkResult>> coGrouped =
                KeyedPCollectionTuple
                        .of(FilterAndPairFn.SOURCE_TAG, keyedSource)
                        .and(FilterAndPairFn.CASE_PENDING_TAG, keyedCasePending)
                        .and(FilterAndPairFn.AI_PENDING_TAG, keyedAiPending)
                        .apply("CoGroupByImageIdAndSegment", CoGroupByKey.create());

        // ── Filter & pair ─────────────────────────────────────────────────────
        PCollectionTuple routed = coGrouped.apply(
                "FilterAndPair",
                ParDo.of(new FilterAndPairFn(
                                options.getSegmentConfigs(), payloadSchema, pendingSchema, aiPendingSchema))
                     .withOutputTags(
                             FilterAndPairFn.MATCHED,
                             TupleTagList
                                     .of(FilterAndPairFn.CASE_PENDING)
                                     .and(FilterAndPairFn.CASE_AGED_OUT)
                                     .and(FilterAndPairFn.AI_PENDING)
                                     .and(FilterAndPairFn.AI_AGED_OUT)));

        // ── Register Avro coders ──────────────────────────────────────────────
        AvroCoder<GenericRecord> payloadCoder   = AvroCoder.of(GenericRecord.class, payloadSchema);
        AvroCoder<GenericRecord> pendingCoder   = AvroCoder.of(GenericRecord.class, pendingSchema);
        AvroCoder<GenericRecord> aiPendingCoder = AvroCoder.of(GenericRecord.class, aiPendingSchema);

        PCollection<KV<String, KV<GenericRecord, GenericRecord>>> matched =
                routed.get(FilterAndPairFn.MATCHED)
                      .setCoder(KvCoder.of(StringUtf8Coder.of(),
                              KvCoder.of(payloadCoder, payloadCoder)));

        PCollection<GenericRecord> casePending =
                routed.get(FilterAndPairFn.CASE_PENDING).setCoder(pendingCoder);

        PCollection<GenericRecord> caseAgedOut =
                routed.get(FilterAndPairFn.CASE_AGED_OUT).setCoder(pendingCoder);

        PCollection<GenericRecord> aiPending =
                routed.get(FilterAndPairFn.AI_PENDING).setCoder(aiPendingCoder);

        PCollection<GenericRecord> aiAgedOut =
                routed.get(FilterAndPairFn.AI_AGED_OUT).setCoder(aiPendingCoder);

        // ── Flatten & compare matched pairs ───────────────────────────────────
        PCollection<TableRow> comparisonResults = matched
                .apply("FlattenAndCompare",
                        ParDo.of(new FlattenAndCompareFn(
                                options.getFirestoreCollection(),
                                options.getKmsKeyPath(),
                                options.getSegmentConfigs())));

        // ── Intermediate TableRow PCollections (also used as Write inputs) ────
        PCollection<TableRow> casePendingRows = casePending
                .apply("MapCasePendingToTableRow",
                        MapElements.into(TypeDescriptor.of(TableRow.class))
                                   .via(r -> fromCasePendingRecord(r)));

        PCollection<TableRow> aiPendingRows = aiPending
                .apply("MapAiPendingToTableRow",
                        MapElements.into(TypeDescriptor.of(TableRow.class))
                                   .via(r -> fromAiPendingRecord(r)));

        PCollection<TableRow> caseAgedOutRows = caseAgedOut
                .apply("MapCaseAgedOutToTableRow",
                        MapElements.into(TypeDescriptor.of(TableRow.class))
                                   .via(r -> fromCaseAgedOutRecord(r)));

        PCollection<TableRow> aiAgedOutRows = aiAgedOut
                .apply("MapAiAgedOutToTableRow",
                        MapElements.into(TypeDescriptor.of(TableRow.class))
                                   .via(r -> fromAiAgedOutRecord(r)));

        PCollection<TableRow> deadLetterRows =
                PCollectionList.of(caseAgedOutRows).and(aiAgedOutRows)
                               .apply("FlattenAgedOutRows", Flatten.pCollections());

        // ── Write all outputs; capture WriteResults for advance signal ─────────
        WriteResult compWrite = comparisonResults
                .apply("WriteResults",
                        BigQueryIO.writeTableRows()
                                .to(options.getOutputTable())
                                .withSchema(SchemaUtil.comparisonResultsSchema())
                                .withWriteDisposition(WriteDisposition.WRITE_APPEND)
                                .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED));

        WriteResult casePendingWrite = casePendingRows
                .apply("WritePendingTable",
                        BigQueryIO.writeTableRows()
                                .to(options.getPendingTable())
                                .withSchema(SchemaUtil.pendingSchema())
                                .withWriteDisposition(WriteDisposition.WRITE_TRUNCATE)
                                .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED));

        WriteResult aiPendingWrite = aiPendingRows
                .apply("WriteAiPendingTable",
                        BigQueryIO.writeTableRows()
                                .to(options.getAiPendingTable())
                                .withSchema(SchemaUtil.aiPendingSchema())
                                .withWriteDisposition(WriteDisposition.WRITE_TRUNCATE)
                                .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED));

        WriteResult deadLetterWrite = deadLetterRows
                .apply("WriteDeadLetterTable",
                        BigQueryIO.writeTableRows()
                                .to(options.getDeadLetterTable())
                                .withSchema(SchemaUtil.deadLetterSchema())
                                .withWriteDisposition(WriteDisposition.WRITE_APPEND)
                                .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED));

        WriteResult scopeEventWrite = comparisonResults
                .apply("BuildComparisonScopeEvents", ParDo.of(new ComparisonScopeEventFn()))
                .apply("WriteComparisonScopeEvents",
                        BigQueryIO.writeTableRows()
                                .to(options.getScopeEventsTable())
                                .withSchema(SchemaUtil.comparisonScopeEventSchema())
                                .withWriteDisposition(WriteDisposition.WRITE_APPEND)
                                .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED));

        // ── Advance window checkpoint after all writes succeed ─────────────────
        // Wait.on blocks the trigger element until all WriteResult signals are done.
        // getSuccessfulTableLoads() fires once per BQ load job (batch FILE_LOADS);
        // for empty outputs it emits nothing (immediately "done"), which is correct —
        // empty tables require no waiting.
        // If any write throws, the exception propagates and AdvanceWindowFn never runs,
        // so last_extracted stays unchanged and the next run retries the same window.
        pipeline.apply("AdvanceTrigger", Create.of("done"))
                .apply("WaitForAllWrites", Wait.on(
                        compWrite.getSuccessfulTableLoads(),
                        casePendingWrite.getSuccessfulTableLoads(),
                        aiPendingWrite.getSuccessfulTableLoads(),
                        deadLetterWrite.getSuccessfulTableLoads(),
                        scopeEventWrite.getSuccessfulTableLoads()))
                .apply("AdvanceWindow",
                        ParDo.of(new AdvanceWindowFn(
                                options.getLookupTable(), options.getPipelineName())));

        LOG.info("Pipeline graph constructed.");
    }

    // ── WindowValueProvider ───────────────────────────────────────────────────

    /**
     * A {@link Serializable} {@link ValueProvider} that resolves the processing
     * window start or end from the BigQuery lookup table at runtime (i.e. on
     * Dataflow workers, using the Dataflow service account).
     *
     * <p>On the first call to {@link #get()} within a JVM, this provider:
     * <ol>
     *   <li>Calls {@link WindowManager#claimWindow()} — a conditional UPDATE that
     *       is a no-op if another worker already claimed the window for this run.</li>
     *   <li>Calls {@link WindowManager#getWindow()} to read the window bounds.</li>
     *   <li>Caches the result in a static map; subsequent calls return immediately.</li>
     * </ol>
     *
     * <p>Because only the first worker's conditional UPDATE succeeds (the rest are
     * no-ops), all workers end up reading the same {@code current_stop} value and
     * therefore use identical window bounds.
     */
    private static final class WindowValueProvider
            implements ValueProvider<String>, Serializable {

        // Shared per JVM — ensures claimWindow() + getWindow() happen once per worker.
        private static final ConcurrentHashMap<String, WindowInfo> CACHE =
                new ConcurrentHashMap<>();

        private final ValueProvider<String> lookupTable;
        private final ValueProvider<String> pipelineName;
        private final boolean isStart;

        WindowValueProvider(ValueProvider<String> lookupTable,
                            ValueProvider<String> pipelineName,
                            boolean isStart) {
            this.lookupTable  = lookupTable;
            this.pipelineName = pipelineName;
            this.isStart      = isStart;
        }

        @Override
        public String get() {
            String name  = pipelineName.get();
            String table = lookupTable.get().replace(':', '.');
            WindowInfo info = CACHE.computeIfAbsent(name, k -> {
                BigQuery bq = BigQueryOptions.getDefaultInstance().getService();
                WindowManager wm = new WindowManager(bq, table, k);
                wm.claimWindow();
                return wm.getWindow();
            });
            return isStart ? info.windowStart : info.windowEnd;
        }

        @Override
        public boolean isAccessible() {
            return lookupTable.isAccessible() && pipelineName.isAccessible();
        }
    }

    // ── AiSourceQueryProvider ─────────────────────────────────────────────────

    /**
     * AI rows from {@code lookbackHours} before {@code windowEnd} up to {@code windowEnd}.
     *
     * <p>Uses a trailing lookback rather than a hard {@code windowStart} floor because the
     * AI source table can have insertion backlog: a row's {@code created_at} can fall inside
     * an already-processed window even though the row itself isn't written to the table
     * until later. A hard floor would permanently miss such rows once the window has
     * advanced past them. Re-reading a wider window is safe — rows already known are
     * deduped against {@code ai_pending_comparisons} by {@code (payload, created_at)} in
     * {@link FilterAndPairFn}, so this does not produce duplicate matches.
     */
    private static final class AiSourceQueryProvider
            implements ValueProvider<String>, Serializable {

        private final ValueProvider<String>  table;
        private final ValueProvider<String>  windowEnd;
        private final ValueProvider<Integer> lookbackHours;

        AiSourceQueryProvider(ValueProvider<String>  table,
                               ValueProvider<String>  windowEnd,
                               ValueProvider<Integer> lookbackHours) {
            this.table         = table;
            this.windowEnd     = windowEnd;
            this.lookbackHours = lookbackHours;
        }

        @Override
        public String get() {
            return String.format(
                    "SELECT * FROM `%s`"
                    + " WHERE created_at >= TIMESTAMP_SUB('%s', INTERVAL %d HOUR)"
                    + " AND created_at < '%s'",
                    table.get().replace(':', '.'),
                    windowEnd.get(), lookbackHours.get(),
                    windowEnd.get());
        }

        @Override
        public boolean isAccessible() {
            return table.isAccessible() && windowEnd.isAccessible() && lookbackHours.isAccessible();
        }
    }

    // ── HumanSourceQueryProvider ──────────────────────────────────────────────

    /**
     * Human rows from {@code lookbackDays} before {@code windowEnd} up to
     * {@code windowEnd}. The wide lookback allows late-arriving human payloads
     * to match pending AI rows from earlier windows.
     */
    private static final class HumanSourceQueryProvider
            implements ValueProvider<String>, Serializable {

        private final ValueProvider<String>  table;
        private final ValueProvider<String>  windowEnd;
        private final ValueProvider<Integer> lookbackDays;

        HumanSourceQueryProvider(ValueProvider<String>  table,
                                  ValueProvider<String>  windowEnd,
                                  ValueProvider<Integer> lookbackDays) {
            this.table        = table;
            this.windowEnd    = windowEnd;
            this.lookbackDays = lookbackDays;
        }

        @Override
        public String get() {
            return String.format(
                    "SELECT * FROM `%s`"
                    + " WHERE created_at >= TIMESTAMP_SUB('%s', INTERVAL %d DAY)"
                    + " AND created_at < '%s'",
                    table.get().replace(':', '.'),
                    windowEnd.get(), lookbackDays.get(),
                    windowEnd.get());
        }

        @Override
        public boolean isAccessible() {
            return table.isAccessible() && windowEnd.isAccessible() && lookbackDays.isAccessible();
        }
    }

    // ── PendingQueryProvider ──────────────────────────────────────────────────

    /**
     * All pending rows, unfiltered by age.
     *
     * <p>Age filtering must not happen here: {@link FilterAndPairFn} is what decides
     * whether a pending row has aged out (>= {@link FilterAndPairFn#MAX_WAIT_DAYS})
     * and routes it to {@code dead_letter_comparisons}. Since {@code pending_comparisons}
     * is {@code WRITE_TRUNCATE}d from only the rows this run reads, filtering rows out
     * of this query before they reach the DoFn would make them vanish silently instead
     * of being dead-lettered — the row is simply absent from both the pending rewrite
     * and the dead-letter output.
     */
    private static final class PendingQueryProvider
            implements ValueProvider<String>, Serializable {

        private final ValueProvider<String> table;

        PendingQueryProvider(ValueProvider<String> table) {
            this.table = table;
        }

        @Override
        public String get() {
            return String.format("SELECT * FROM `%s`", table.get().replace(':', '.'));
        }

        @Override
        public boolean isAccessible() {
            return table.isAccessible();
        }
    }

    // ── AdvanceWindowFn ───────────────────────────────────────────────────────

    /**
     * Advances the processing window checkpoint after all BigQuery writes succeed.
     * Runs after a {@link Wait#on} that blocks on all write results, so it is
     * guaranteed not to execute if any write fails.
     */
    private static final class AdvanceWindowFn extends DoFn<String, Void> {

        private final ValueProvider<String> lookupTable;
        private final ValueProvider<String> pipelineName;

        AdvanceWindowFn(ValueProvider<String> lookupTable,
                        ValueProvider<String> pipelineName) {
            this.lookupTable  = lookupTable;
            this.pipelineName = pipelineName;
        }

        @ProcessElement
        public void processElement() {
            String table = lookupTable.get().replace(':', '.');
            String name  = pipelineName.get();
            BigQuery bq  = BigQueryOptions.getDefaultInstance().getService();
            new WindowManager(bq, table, name).advance();
            LOG.info("WindowManager [{}]: checkpoint advanced (last_extracted = current_stop)", name);
        }
    }

    // ── ComparisonScopeEventFn ───────────────────────────────────────────────

    /**
     * Converts field-level comparison rows into append-only scope events. A scope
     * is the finest comparable unit available in the flattened output:
     * segment_type, segment_type+array_key, or a soft-array parent for dispute
     * code arrays whose emitted keys include AI/human code pairing suffixes.
     */
    private static final class ComparisonScopeEventFn extends DoFn<TableRow, TableRow> {

        private static final String AI_SCOPE_DISCOVERED = "AI_SCOPE_DISCOVERED";
        private static final String HUMAN_SCOPE_COVERED = "HUMAN_SCOPE_COVERED";
        private static final String SEGMENT_TYPE_ONLY = "__SEGMENT_TYPE_ONLY__";

        @ProcessElement
        public void processElement(ProcessContext ctx) {
            TableRow row = ctx.element();
            String segmentType = str(row.get("segment_type"));
            if (segmentType == null) return;

            String arrayKey = str(row.get("array_key"));
            String fieldName = str(row.get("field_name"));
            String scopeLevel = scopeLevel(fieldName, arrayKey);
            String scopeKey = scopeKey(fieldName, arrayKey);

            if (row.get("ai_value") != null) {
                ctx.output(scopeEvent(row, AI_SCOPE_DISCOVERED, null, scopeLevel, scopeKey));
            }
            if (row.get("human_value") != null) {
                ctx.output(scopeEvent(row, HUMAN_SCOPE_COVERED, str(row.get("case_id")),
                        scopeLevel, scopeKey));
            }
        }

        private static TableRow scopeEvent(TableRow row, String eventType, String caseId,
                                           String scopeLevel, String scopeKey) {
            return new TableRow()
                    .set("image_id",      str(row.get("image_id")))
                    .set("segment",       str(row.get("segment")))
                    .set("ai_created_at", str(row.get("ai_created_at")))
                    .set("segment_type",  str(row.get("segment_type")))
                    .set("scope_level",   scopeLevel)
                    .set("scope_key",     scopeKey)
                    .set("event_type",    eventType)
                    .set("case_id",       caseId)
                    .set("load_time",     str(row.get("load_time")));
        }

        private static String scopeLevel(String fieldName, String arrayKey) {
            if (arrayKey != null && isSoftArrayField(fieldName)) return "soft_array_parent";
            if (arrayKey != null) return "array_entity";
            return "segment_type";
        }

        private static String scopeKey(String fieldName, String arrayKey) {
            if (arrayKey != null && isSoftArrayField(fieldName)) return parentArrayKey(arrayKey);
            if (arrayKey != null) return arrayKey;
            return SEGMENT_TYPE_ONLY;
        }

        private static boolean isSoftArrayField(String fieldName) {
            return fieldName != null && (
                    fieldName.equals("tradelines.tradelineRequest.disputeCodes")
                    || fieldName.startsWith("tradelines.tradelineRequest.disputeCodes.")
                    || fieldName.equals("credit.dob.disputeCodes")
                    || fieldName.startsWith("credit.dob.disputeCodes.")
                    || fieldName.equals("credit.ssn.disputeCodes")
                    || fieldName.startsWith("credit.ssn.disputeCodes.")
                    || fieldName.equals("nonReported.disputeCodes")
                    || fieldName.startsWith("nonReported.disputeCodes."));
        }

        private static String parentArrayKey(String arrayKey) {
            int lastDash = arrayKey.lastIndexOf('-');
            if (lastDash < 0) return arrayKey;
            int secondLastDash = arrayKey.lastIndexOf('-', lastDash - 1);
            if (secondLastDash < 0) return arrayKey;
            return arrayKey.substring(0, secondLastDash);
        }
    }

    // ── Row conversion helpers ────────────────────────────────────────────────

    private static TableRow fromCasePendingRecord(GenericRecord r) {
        return new TableRow()
                .set("image_id",        str(r.get("image_id")))
                .set("key_id",          str(r.get("key_id")))
                .set("segment",         str(r.get("segment")) != null
                        ? str(r.get("segment")) : "main")
                .set("case_id",         str(r.get("case_id")))
                .set("pending_type",    str(r.get("pending_type")))
                .set("payload",         str(r.get("payload")))
                .set("created_at",      str(r.get("created_at")))
                .set("first_seen_at",   str(r.get("first_seen_at")))
                .set("last_retried_at", str(r.get("last_retried_at")))
                .set("retry_count",     r.get("retry_count") != null
                        ? ((Number) r.get("retry_count")).longValue() : 0L)
                .set("matched_ai_keys", str(r.get("matched_ai_keys")))
                .set("next_ai_iteration", r.get("next_ai_iteration") != null
                        ? ((Number) r.get("next_ai_iteration")).longValue() : 0L);
    }

    private static TableRow fromAiPendingRecord(GenericRecord r) {
        return new TableRow()
                .set("image_id",        str(r.get("image_id")))
                .set("key_id",          str(r.get("key_id")))
                .set("segment",         str(r.get("segment")) != null
                        ? str(r.get("segment")) : "main")
                .set("payload",         str(r.get("payload")))
                .set("created_at",      str(r.get("created_at")))
                .set("first_seen_at",   str(r.get("first_seen_at")))
                .set("last_retried_at", str(r.get("last_retried_at")))
                .set("retry_count",     r.get("retry_count") != null
                        ? ((Number) r.get("retry_count")).longValue() : 0L);
    }

    private static TableRow fromCaseAgedOutRecord(GenericRecord r) {
        return new TableRow()
                .set("image_id",      str(r.get("image_id")))
                .set("key_id",        str(r.get("key_id")))
                .set("segment",       str(r.get("segment")) != null
                        ? str(r.get("segment")) : "main")
                .set("case_id",       str(r.get("case_id")))
                .set("pending_type",  str(r.get("pending_type")))
                .set("payload",       str(r.get("payload")))
                .set("created_at",    str(r.get("created_at")))
                .set("first_seen_at", str(r.get("first_seen_at")))
                .set("aged_out_at",   TimestampUtil.formatInstant(Instant.now()))
                .set("reason",        "No matching AI iteration within "
                        + FilterAndPairFn.MAX_WAIT_DAYS + " days");
    }

    private static TableRow fromAiAgedOutRecord(GenericRecord r) {
        return new TableRow()
                .set("image_id",      str(r.get("image_id")))
                .set("key_id",        str(r.get("key_id")))
                .set("segment",       str(r.get("segment")) != null
                        ? str(r.get("segment")) : "main")
                .set("pending_type",  "ai")
                .set("payload",       str(r.get("payload")))
                .set("created_at",    str(r.get("created_at")))
                .set("first_seen_at", str(r.get("first_seen_at")))
                .set("aged_out_at",   TimestampUtil.formatInstant(Instant.now()))
                .set("reason",        "No matching case within "
                        + FilterAndPairFn.MAX_WAIT_DAYS + " days");
    }

    private static String str(Object value) {
        return value != null ? value.toString() : null;
    }
}
