package com.yourorg.pipeline.transforms;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.QueryParameterValue;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Marks the currently-live {@code comparison_results} rows for a group as
 * {@code is_current = FALSE} right before {@link FlattenAndCompareFn} writes that
 * group's replacement rows.
 *
 * <h3>Why this exists</h3>
 * {@link FilterAndPairFn} now compares a group against only its single latest AI
 * payload, and only when the comparison signature actually changed (a human
 * update — a case merging in, a field being corrected — or a genuinely new AI
 * payload). When that happens, the previous comparison's rows are stale and
 * must stop showing up as the "current" result for that group — but rather
 * than deleting them, they're marked non-current so the full history (e.g. a
 * case's original value before another case's correction) stays recoverable
 * from the raw table. See {@code comparison_results_current_view}
 * (migration 006) for the filtered view most consumers should query.
 *
 * <h3>Placement and ordering</h3>
 * Pass-through: re-emits every element unchanged after issuing the {@code UPDATE},
 * so it sits directly in front of {@code FlattenAndCompareFn} in the pipeline
 * graph ({@code matched -> MarkSupersededComparisonsFn -> FlattenAndCompareFn ->
 * BigQueryIO.write()}). Beam processes a given element through a DoFn chain in
 * dependency order, and the {@code UPDATE} — issued synchronously via the
 * BigQuery client, mirroring {@link com.yourorg.pipeline.util.WindowManager}'s
 * pattern, the only other direct-BigQuery-client usage in this codebase — blocks
 * until the DML job completes. So the {@code UPDATE} for a group is guaranteed to
 * finish before that same element's new rows are computed and written
 * downstream: there's never a moment with two {@code is_current = TRUE} row sets
 * for the same group.
 *
 * <p>Because {@link FilterAndPairFn} only emits a {@code MATCHED} pair when the
 * signature actually changed, this only runs (and only marks rows superseded)
 * exactly when new rows are about to replace old ones. An {@code UPDATE} with
 * nothing to match (a group's first-ever comparison) is a harmless no-op.
 *
 * <h3>Operational note</h3>
 * This issues one DML {@code UPDATE} per group with a changed signature, per
 * run. Under high pipeline parallelism/fan-out this could approach BigQuery's
 * per-table concurrent-mutating-DML-job limits — if that shows up under real
 * load, batch multiple groups' updates into fewer statements per bundle
 * (accumulate {@code (image_id, segment)} pairs in {@code @StartBundle}/
 * {@code @FinishBundle} instead of one {@code UPDATE} per element) rather than
 * per-element.
 */
public class MarkSupersededComparisonsFn
        extends DoFn<KV<String, KV<GenericRecord, GenericRecord>>,
                      KV<String, KV<GenericRecord, GenericRecord>>> {

    private static final Logger LOG = LoggerFactory.getLogger(MarkSupersededComparisonsFn.class);

    private final ValueProvider<String> comparisonResultsTable;

    private transient BigQuery bq;

    public MarkSupersededComparisonsFn(ValueProvider<String> comparisonResultsTable) {
        this.comparisonResultsTable = comparisonResultsTable;
    }

    @Setup
    public void setup() {
        bq = BigQueryOptions.getDefaultInstance().getService();
    }

    @ProcessElement
    public void processElement(ProcessContext ctx) {
        String pairKey = ctx.element().getKey();
        String[] parts = pairKey.split("::", 2);
        if (parts.length == 2) {
            markSuperseded(parts[0], parts[1]);
        } else {
            LOG.warn("Unexpected pairKey format: '{}' — skipping supersede-mark", pairKey);
        }
        ctx.output(ctx.element());
    }

    private void markSuperseded(String imageId, String segment) {
        String table = comparisonResultsTable.get().replace(':', '.');
        String sql = "UPDATE `" + table + "`"
                + " SET is_current = FALSE"
                + " WHERE image_id = @imageId AND segment = @segment AND is_current = TRUE";
        QueryJobConfiguration config = QueryJobConfiguration.newBuilder(sql)
                .setUseLegacySql(false)
                .addNamedParameter("imageId", QueryParameterValue.string(imageId))
                .addNamedParameter("segment", QueryParameterValue.string(segment))
                .build();
        try {
            bq.query(config);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(
                    "Interrupted marking superseded comparison_results rows for imageId="
                            + imageId + " segment=" + segment, e);
        }
    }
}
