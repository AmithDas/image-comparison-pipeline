package com.yourorg.pipeline;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.yourorg.pipeline.transforms.DecryptAndKeyFn;
import com.yourorg.pipeline.transforms.FilterAndPairFn;
import com.yourorg.pipeline.transforms.FlattenAndCompareFn;
import com.yourorg.pipeline.transforms.OrphanCompareFn;
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
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.Wait;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import java.util.Map;
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

        @Description("BigQuery table containing dispute code groupings loaded at worker startup. "
                + "Schema: variant_code STRING, base_code STRING. "
                + "Format: project:dataset.table")
        @Validation.Required
        ValueProvider<String> getCodeGroupingsTable();
        void setCodeGroupingsTable(ValueProvider<String> value);

        @Description("How far back (in days) to read human source rows relative to windowEnd. "
                + "Allows late-arriving human payloads to match AI rows from earlier windows. "
                + "Default: 180.")
        @Default.Integer(180)
        ValueProvider<Integer> getHumanLookbackDays();
        void setHumanLookbackDays(ValueProvider<Integer> value);

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

        // No ValueProvider.get() calls here — the graph structure is fixed
        // (one AI read, one human read, one pending read) regardless of how many
        // segments are configured.  All runtime parameters are resolved on workers.

        // Lazy window providers: first .get() per worker JVM claims the window
        // in BigQuery using the Dataflow SA, then caches the result.
        ValueProvider<String> windowStart =
                new WindowValueProvider(options.getLookupTable(), options.getPipelineName(), true);
        ValueProvider<String> windowEnd   =
                new WindowValueProvider(options.getLookupTable(), options.getPipelineName(), false);

        // queryTempDataset is static infrastructure — resolved at template build time.
        String queryTempDataset = options.getQueryTempDataset().get();

        // ── Dispute code groupings side input ─────────────────────────────────
        // Read once for the entire job; Beam broadcasts to all workers automatically.
        // Schema: variant_code STRING, base_code STRING.
        PCollectionView<Map<String, String>> codeGroupingsView = pipeline
                .apply("ReadCodeGroupings",
                        BigQueryIO.readTableRows()
                                .from(options.getCodeGroupingsTable())
                                .withMethod(BigQueryIO.TypedRead.Method.DIRECT_READ))
                .apply("CodeGroupingsToKV",
                        MapElements.into(TypeDescriptors.kvs(
                                        TypeDescriptors.strings(), TypeDescriptors.strings()))
                                .via(row -> KV.of(
                                        (String) row.get("variant_code"),
                                        (String) row.get("base_code"))))
                .apply("CodeGroupingsView", View.asMap());

        // ── AI source rows (all segments in one read) ─────────────────────────
        PCollection<KV<String, TableRow>> keyedAi = pipeline
                .apply("ReadAi",
                        BigQueryIO.readTableRows()
                                .fromQuery(new AiSourceQueryProvider(
                                        options.getAiSourceTable(), windowStart, windowEnd))
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

        // ── Pending rows (all segments in one read) ───────────────────────────
        PCollection<KV<String, TableRow>> keyedPending = pipeline
                .apply("ReadPending",
                        BigQueryIO.readTableRows()
                                .fromQuery(new PendingQueryProvider(options.getPendingTable()))
                                .usingStandardSql()
                                .withQueryTempDataset(queryTempDataset))
                .apply("KeyPending",
                        WithKeys.of((TableRow row) ->
                                row.get("image_id") + "::" + row.get("segment"))
                                .withKeyType(TypeDescriptors.strings()));

        // ── Co-group source + pending by "imageId::segment" ───────────────────
        PCollection<KV<String, CoGbkResult>> coGrouped =
                KeyedPCollectionTuple
                        .of(FilterAndPairFn.SOURCE_TAG, keyedSource)
                        .and(FilterAndPairFn.PENDING_TAG, keyedPending)
                        .apply("CoGroupByImageIdAndSegment", CoGroupByKey.create());

        // ── Filter & pair ─────────────────────────────────────────────────────
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

        // ── Flatten & compare matched pairs ───────────────────────────────────
        PCollection<TableRow> comparisonResults = matched
                .apply("FlattenAndCompare",
                        ParDo.of(new FlattenAndCompareFn(
                                options.getFirestoreCollection(),
                                options.getKmsKeyPath(),
                                options.getSegmentConfigs(),
                                codeGroupingsView))
                                .withSideInputs(codeGroupingsView));

        // ── Intermediate TableRow PCollections (also used as Write inputs) ────
        PCollection<TableRow> newPendingRows = newPending
                .apply("MapPendingToTableRow",
                        MapElements.into(TypeDescriptor.of(TableRow.class))
                                   .via(r -> fromPendingRecord(r)));

        PCollection<TableRow> agedOutRows = agedOut
                .apply("MapAgedOutToTableRow",
                        MapElements.into(TypeDescriptor.of(TableRow.class))
                                   .via(r -> fromAgedOutRecord(r)));

        PCollection<TableRow> agedOutResults = agedOut
                .apply("FlattenAgedOutPayloads",
                        ParDo.of(new OrphanCompareFn(
                                options.getFirestoreCollection(),
                                options.getKmsKeyPath())));

        PCollection<TableRow> pendingSnapshot = newPending
                .apply("FlattenPendingPayloads",
                        ParDo.of(new OrphanCompareFn(
                                options.getFirestoreCollection(),
                                options.getKmsKeyPath())));

        // ── Write all outputs; capture WriteResults for advance signal ─────────
        WriteResult compWrite = comparisonResults
                .apply("WriteResults",
                        BigQueryIO.writeTableRows()
                                .to(options.getOutputTable())
                                .withSchema(SchemaUtil.comparisonResultsSchema())
                                .withWriteDisposition(WriteDisposition.WRITE_APPEND)
                                .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED));

        WriteResult pendingWrite = newPendingRows
                .apply("WritePendingTable",
                        BigQueryIO.writeTableRows()
                                .to(options.getPendingTable())
                                .withSchema(SchemaUtil.pendingSchema())
                                .withWriteDisposition(WriteDisposition.WRITE_TRUNCATE)
                                .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED));

        WriteResult deadLetterWrite = agedOutRows
                .apply("WriteDeadLetterTable",
                        BigQueryIO.writeTableRows()
                                .to(options.getDeadLetterTable())
                                .withSchema(SchemaUtil.deadLetterSchema())
                                .withWriteDisposition(WriteDisposition.WRITE_APPEND)
                                .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED));

        WriteResult agedOutWrite = agedOutResults
                .apply("WriteAgedOutResults",
                        BigQueryIO.writeTableRows()
                                .to(options.getOutputTable())
                                .withSchema(SchemaUtil.comparisonResultsSchema())
                                .withWriteDisposition(WriteDisposition.WRITE_APPEND)
                                .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED));

        WriteResult snapshotWrite = pendingSnapshot
                .apply("WritePendingSnapshot",
                        BigQueryIO.writeTableRows()
                                .to(options.getPendingSnapshotTable())
                                .withSchema(SchemaUtil.comparisonResultsSchema())
                                .withWriteDisposition(WriteDisposition.WRITE_TRUNCATE)
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
                        pendingWrite.getSuccessfulTableLoads(),
                        deadLetterWrite.getSuccessfulTableLoads(),
                        agedOutWrite.getSuccessfulTableLoads(),
                        snapshotWrite.getSuccessfulTableLoads()))
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

    /** AI rows in the current processing window: {@code [windowStart, windowEnd)}. */
    private static final class AiSourceQueryProvider
            implements ValueProvider<String>, Serializable {

        private final ValueProvider<String> table;
        private final ValueProvider<String> windowStart;
        private final ValueProvider<String> windowEnd;

        AiSourceQueryProvider(ValueProvider<String> table,
                               ValueProvider<String> windowStart,
                               ValueProvider<String> windowEnd) {
            this.table       = table;
            this.windowStart = windowStart;
            this.windowEnd   = windowEnd;
        }

        @Override
        public String get() {
            return String.format(
                    "SELECT * FROM `%s` WHERE created_at >= '%s' AND created_at < '%s'",
                    table.get().replace(':', '.'), windowStart.get(), windowEnd.get());
        }

        @Override
        public boolean isAccessible() {
            return table.isAccessible() && windowStart.isAccessible() && windowEnd.isAccessible();
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

    /** All pending rows younger than {@link FilterAndPairFn#MAX_WAIT_DAYS}. */
    private static final class PendingQueryProvider
            implements ValueProvider<String>, Serializable {

        private final ValueProvider<String> table;

        PendingQueryProvider(ValueProvider<String> table) {
            this.table = table;
        }

        @Override
        public String get() {
            return String.format(
                    "SELECT * FROM `%s`"
                    + " WHERE first_seen_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(),"
                    + " INTERVAL %d DAY)",
                    table.get().replace(':', '.'), FilterAndPairFn.MAX_WAIT_DAYS);
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
