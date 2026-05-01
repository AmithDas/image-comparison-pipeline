package com.yourorg.pipeline.util;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages the processing window for the pipeline by reading from and updating
 * a shared lookup table.
 *
 * <h3>Lookup table schema</h3>
 * <pre>
 *   table_name     STRING     NOT NULL   Key identifying this pipeline's config row
 *   last_extracted TIMESTAMP  NOT NULL   Checkpoint: end of last successful window
 *   current_stop   TIMESTAMP             Working field: end of the current window being processed
 * </pre>
 *
 * <h3>Usage sequence</h3>
 * <pre>
 *   WindowManager wm = new WindowManager(bq, "project.dataset.config", "image_comparison");
 *
 *   wm.claimWindow();               // step 1: current_stop = CURRENT_TIMESTAMP()
 *   WindowInfo w = wm.getWindow();  // step 2+3: read window_start and window_end
 *   try {
 *       runPipeline(w.windowStart, w.windowEnd);
 *       wm.advance();               // step 5 (success): last_extracted = current_stop
 *   } catch (Exception e) {
 *       wm.revert();                // failure: current_stop = last_extracted (retry same window next run)
 *       throw e;
 *   }
 * </pre>
 */
public final class WindowManager {

    private static final Logger LOG = LoggerFactory.getLogger(WindowManager.class);

    // ── Window bounds returned to the caller ──────────────────────────────────

    public static final class WindowInfo {
        /** Inclusive window start: {@code TIMESTAMP_SUB(last_extracted, INTERVAL 1 HOUR)} */
        public final String windowStart;
        /** Exclusive window end: {@code current_stop} (set in {@link #claimWindow()}) */
        public final String windowEnd;

        public WindowInfo(String windowStart, String windowEnd) {
            this.windowStart = windowStart;
            this.windowEnd   = windowEnd;
        }

        @Override
        public String toString() {
            return windowStart + " → " + windowEnd;
        }
    }

    // ── State ─────────────────────────────────────────────────────────────────

    private final BigQuery bq;
    private final String   lookupTableRef;  // fully-qualified BQ path: project.dataset.table
    private final String   pipelineName;    // value of the table_name column for this pipeline

    public WindowManager(BigQuery bq, String lookupTableRef, String pipelineName) {
        this.bq             = bq;
        this.lookupTableRef = lookupTableRef;
        this.pipelineName   = pipelineName;
    }

    // ── Step 1: claim the window ──────────────────────────────────────────────

    /**
     * Sets {@code current_stop = CURRENT_TIMESTAMP()} for this pipeline's row.
     * Must be called before {@link #getWindow()} so the window end is fixed
     * before the source queries run.
     */
    public void claimWindow() {
        exec(String.format(
                "UPDATE `%s`"
                + " SET current_stop = CURRENT_TIMESTAMP()"
                + " WHERE table_name = '%s'",
                lookupTableRef, pipelineName));
        LOG.info("WindowManager [{}]: current_stop set to CURRENT_TIMESTAMP()", pipelineName);
    }

    // ── Steps 2 + 3: read the window ─────────────────────────────────────────

    /**
     * Reads the processing window from the lookup table.
     * <ul>
     *   <li>{@code window_start} = {@code TIMESTAMP_SUB(last_extracted, INTERVAL 1 HOUR)}</li>
     *   <li>{@code window_end}   = {@code current_stop} (as set by {@link #claimWindow()})</li>
     * </ul>
     *
     * @throws IllegalStateException if no row exists for {@code pipelineName}
     */
    public WindowInfo getWindow() {
        String sql = String.format(
                "SELECT"
                + " FORMAT_TIMESTAMP('%%Y-%%m-%%dT%%H:%%M:%%SZ',"
                + "   TIMESTAMP_SUB(last_extracted, INTERVAL 1 HOUR)) AS window_start,"
                + " FORMAT_TIMESTAMP('%%Y-%%m-%%dT%%H:%%M:%%SZ',"
                + "   current_stop) AS window_end"
                + " FROM `%s`"
                + " WHERE table_name = '%s'"
                + " LIMIT 1",
                lookupTableRef, pipelineName);

        try {
            TableResult result = bq.query(
                    QueryJobConfiguration.newBuilder(sql).setUseLegacySql(false).build());
            if (result.getTotalRows() == 0) {
                throw new IllegalStateException(
                        "No lookup row found for pipeline='" + pipelineName + "'"
                        + " in table " + lookupTableRef);
            }
            FieldValueList row = result.iterateAll().iterator().next();
            WindowInfo info = new WindowInfo(
                    row.get("window_start").getStringValue(),
                    row.get("window_end").getStringValue());
            LOG.info("WindowManager [{}]: window = {}", pipelineName, info);
            return info;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted reading window from lookup table", e);
        }
    }

    // ── Step 5 (success): advance the checkpoint ─────────────────────────────

    /**
     * Advances the checkpoint after a successful run:
     * {@code last_extracted = current_stop}.
     * The next run will start from {@code TIMESTAMP_SUB(current_stop, INTERVAL 1 HOUR)}.
     */
    public void advance() {
        exec(String.format(
                "UPDATE `%s`"
                + " SET last_extracted = current_stop"
                + " WHERE table_name = '%s'",
                lookupTableRef, pipelineName));
        LOG.info("WindowManager [{}]: checkpoint advanced (last_extracted = current_stop)",
                pipelineName);
    }

    // ── Failure path: revert the window ──────────────────────────────────────

    /**
     * Reverts {@code current_stop} back to {@code last_extracted} after a failed run.
     * Ensures the next run retries the same window rather than skipping ahead.
     */
    public void revert() {
        exec(String.format(
                "UPDATE `%s`"
                + " SET current_stop = last_extracted"
                + " WHERE table_name = '%s'",
                lookupTableRef, pipelineName));
        LOG.warn("WindowManager [{}]: reverted current_stop to last_extracted (run failed)",
                pipelineName);
    }

    // ── Internal ──────────────────────────────────────────────────────────────

    private void exec(String sql) {
        try {
            bq.query(QueryJobConfiguration.newBuilder(sql).setUseLegacySql(false).build());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted executing lookup table DML: " + sql, e);
        }
    }
}
