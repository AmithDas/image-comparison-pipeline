-- ============================================================
-- BigQuery DDL for Image Comparison Pipeline
-- Run these statements once before the first pipeline execution.
-- Replace `your_project.your_dataset` with your actual values.
--
-- Kept in sync with the Avro schemas under src/main/resources/avro/,
-- which are what SchemaUtil actually derives BigQuery TableSchemas from
-- at runtime (BigQueryIO.write(...).withSchema(...), CREATE_IF_NEEDED).
-- If a table already exists with a different shape than what's below,
-- writes will fail with a schema-mismatch error — CREATE_IF_NEEDED does
-- not reconcile an existing table's schema.
-- ============================================================


-- ------------------------------------------------------------
-- 1. Source tables
--    AI and human payloads are read from separate tables
--    (--aiSourceTable / --humanSourceTable), each with this shape.
--    method: identifies payload origin, mapped to a segment + side
--            (ai/human) via --segmentConfigs (see SegmentConfig).
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `your_project.ai_dataset.ai_payloads` (
  key_id        STRING    NOT NULL,   -- Barricade encryption key identifier
  method        STRING    NOT NULL,   -- e.g. 'aimetadata', 'auth.ai'
  payload       STRING    NOT NULL,   -- Barricade-encrypted JSON
  created_at    TIMESTAMP NOT NULL
)
PARTITION BY DATE(created_at)
CLUSTER BY method
OPTIONS (
  description = "Raw AI payloads per image. One row per payload arrival."
);

CREATE TABLE IF NOT EXISTS `your_project.human_dataset.human_payloads` (
  key_id        STRING    NOT NULL,   -- Barricade encryption key identifier
  method        STRING    NOT NULL,   -- e.g. 'controller.SubmitDispute', 'auth.human'
  payload       STRING    NOT NULL,   -- Barricade-encrypted JSON (or id_request wire format)
  created_at    TIMESTAMP NOT NULL
)
PARTITION BY DATE(created_at)
CLUSTER BY method
OPTIONS (
  description = "Raw human payloads per image. One row per payload arrival."
);


-- ------------------------------------------------------------
-- 2. Comparison results table
--    One row per flattened JSON field per AI iteration per image.
--    Mirrors src/main/resources/avro/comparison_result.json.
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `your_project.your_dataset.comparison_results` (
  image_id          STRING    NOT NULL,
  key_id            STRING    NOT NULL,   -- Barricade key used to encrypt human_value and ai_value
  segment           STRING    NOT NULL,   -- segment name from --segmentConfigs, e.g. 'main', 'auth', 'docreview'
  ai_iteration      INT64     NOT NULL,   -- 1-based, ordered by created_at ASC
  ai_created_at     TIMESTAMP,
  human_created_at  TIMESTAMP,
  field_name        STRING    NOT NULL,   -- dot-notation path e.g. "terms.code"
  array_key         STRING,               -- match-key value for keyed arrays (e.g. "A"); null for scalars
  segment_type      STRING,               -- root-node label, e.g. "authentication"; null for root scalars
  human_value       STRING,               -- Barricade-encrypted; null if field absent in human payload
  ai_value          STRING,               -- Barricade-encrypted; null if field absent in AI payload
  is_match          BOOL      NOT NULL,   -- compared on plaintext before encryption
  compared_at       TIMESTAMP NOT NULL
)
PARTITION BY DATE(compared_at)
CLUSTER BY image_id, field_name
OPTIONS (
  description = "Field-level comparison results between human and AI payloads, across all segments."
);


-- ------------------------------------------------------------
-- 3. Pending comparisons table (durable state store)
--    Stores orphaned payloads awaiting their counterpart.
--    Overwritten (WRITE_TRUNCATE) on every pipeline run — resolved rows
--    disappear automatically by not being re-emitted.
--    Mirrors src/main/resources/avro/pending_row.json.
--    pending_type: 'human', 'ai', 'human:merged', or 'human:<subTypeName>'
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `your_project.your_dataset.pending_comparisons` (
  image_id          STRING    NOT NULL,
  key_id            STRING    NOT NULL,   -- Barricade encryption key identifier
  segment           STRING    NOT NULL,   -- segment name from --segmentConfigs
  pending_type      STRING    NOT NULL,   -- 'human' | 'ai' | 'human:merged' | 'human:<subType>'
  payload           STRING    NOT NULL,   -- Barricade-encrypted payload
  created_at        TIMESTAMP,            -- original created_at from source table
  first_seen_at     TIMESTAMP NOT NULL,   -- when first written to pending
  last_retried_at   TIMESTAMP,            -- updated on each pipeline run
  retry_count       INT64     NOT NULL,   -- incremented on each retry
  matched_ai_count  INT64     NOT NULL    -- AI iterations already matched for this human row across prior runs
)
PARTITION BY DATE(first_seen_at)
OPTIONS (
  description = "Durable state table for orphaned payloads awaiting their counterpart. "
                "Full refresh (WRITE_TRUNCATE) on every pipeline run — resolved rows "
                "are automatically removed by not emitting them from the pipeline. "
                "Aging out to dead_letter_comparisons is decided by FilterAndPairFn, "
                "not by filtering this table's read query."
);


-- ------------------------------------------------------------
-- 4. Pending snapshot table
--    Field-level mismatch rows for payloads still awaiting their counterpart.
--    Truncated and rewritten on every pipeline run — records disappear
--    automatically once their counterpart is matched.
--    Same schema as comparison_results; ai_iteration = 0 for all rows.
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `your_project.your_dataset.pending_snapshot` (
  image_id          STRING    NOT NULL,
  key_id            STRING    NOT NULL,
  segment           STRING    NOT NULL,
  ai_iteration      INT64     NOT NULL,   -- always 0 for pending snapshots
  ai_created_at     TIMESTAMP,
  human_created_at  TIMESTAMP,
  field_name        STRING    NOT NULL,
  array_key         STRING,
  segment_type      STRING,
  human_value       STRING,               -- populated if pending_type = 'human'
  ai_value          STRING,               -- populated if pending_type = 'ai'
  is_match          BOOL      NOT NULL,   -- always false
  compared_at       TIMESTAMP NOT NULL
)
PARTITION BY DATE(compared_at)
CLUSTER BY image_id, field_name
OPTIONS (
  description = "Field-level mismatch snapshot for still-pending payloads. "
                "Full refresh (WRITE_TRUNCATE) on every pipeline run. "
                "Records disappear once their counterpart is matched."
);


-- ------------------------------------------------------------
-- 5. Dead-letter table
--    Payloads that exceeded MAX_WAIT_DAYS (FilterAndPairFn.MAX_WAIT_DAYS)
--    without a counterpart.
--    Mirrors src/main/resources/avro/dead_letter_row.json.
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `your_project.your_dataset.dead_letter_comparisons` (
  image_id      STRING    NOT NULL,
  key_id        STRING,
  segment       STRING    NOT NULL,   -- segment name from --segmentConfigs
  pending_type  STRING    NOT NULL,   -- which side was orphaned: 'human' or 'ai'
  payload       STRING,               -- the orphaned payload JSON (Barricade-encrypted)
  created_at    TIMESTAMP,
  first_seen_at TIMESTAMP,
  aged_out_at   TIMESTAMP NOT NULL,   -- when it was moved to dead-letter
  reason        STRING                -- human-readable reason
)
PARTITION BY DATE(aged_out_at)
OPTIONS (
  description = "Payloads that waited longer than MAX_WAIT_DAYS without a counterpart."
);


-- ------------------------------------------------------------
-- 6. Pipeline window lookup table
--    Drives the pipeline's own window management (see WindowManager /
--    ImageComparisonPipeline.WindowValueProvider). The Dataflow job — not
--    the Airflow DAG — claims and advances this row on each run:
--      1. claimWindow(): current_stop = CURRENT_TIMESTAMP(), only when
--         current_stop = last_extracted (idempotent across workers).
--      2. getWindow(): window_start = last_extracted - 1h, window_end = current_stop.
--      3. advance() on success: last_extracted = current_stop.
--         On failure, current_stop is left ahead of last_extracted, so the
--         next run's claimWindow() is a no-op and retries the same window.
--
--    Seed one row per pipeline before the first run, e.g.:
--
--    INSERT INTO `your_project.your_dataset.pipeline_config`
--      (table_name, last_extracted, current_stop)
--    VALUES
--      ('image_comparison', TIMESTAMP('2026-01-01T00:00:00Z'), TIMESTAMP('2026-01-01T00:00:00Z'));
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `your_project.your_dataset.pipeline_config` (
  table_name     STRING    NOT NULL,  -- matches --pipelineName, identifies this pipeline's row
  last_extracted TIMESTAMP NOT NULL,  -- checkpoint: end of last successfully processed window
  current_stop   TIMESTAMP           -- working field: end of the window currently being processed
)
OPTIONS (
  description = "Processing-window checkpoint for the image-comparison Dataflow pipeline. "
                "Claimed and advanced by the Dataflow job itself (WindowManager) — "
                "the Airflow DAG only triggers the job and does not touch this table."
);


-- ============================================================
-- Useful monitoring queries
-- ============================================================

-- Count of pending rows by type and age bucket
-- SELECT
--   pending_type,
--   CASE
--     WHEN TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), first_seen_at, DAY) < 1 THEN 'same day'
--     WHEN TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), first_seen_at, DAY) < 3 THEN '1-2 days'
--     WHEN TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), first_seen_at, DAY) < 7 THEN '3-6 days'
--     ELSE '7+ days'
--   END AS age_bucket,
--   COUNT(*) AS cnt
-- FROM `your_project.your_dataset.pending_comparisons`
-- GROUP BY 1, 2
-- ORDER BY 1, 2;

-- Overall match rate by field
-- SELECT
--   field_name,
--   COUNTIF(is_match)     AS matched,
--   COUNTIF(NOT is_match) AS mismatched,
--   ROUND(COUNTIF(is_match) * 100.0 / COUNT(*), 2) AS match_pct
-- FROM `your_project.your_dataset.comparison_results`
-- GROUP BY field_name
-- ORDER BY match_pct ASC;
