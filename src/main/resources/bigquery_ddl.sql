-- ============================================================
-- BigQuery DDL for Image Comparison Pipeline
-- Run these statements once before the first pipeline execution.
-- Replace `your_project.your_dataset` with your actual values.
-- ============================================================


-- ------------------------------------------------------------
-- 1. Source table
--    Stores human and AI payloads for each image.
--    payload_type: 'human' or 'ai'
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `your_project.your_dataset.image_payloads` (
  method        STRING    NOT NULL,   -- identifies payload origin (e.g. 'aimetadata' or 'controller.SubmitDispute')
  payload       STRING    NOT NULL,   -- raw JSON string; must contain "image_name" field
  created_at    TIMESTAMP NOT NULL
)
PARTITION BY DATE(created_at)
CLUSTER BY method
OPTIONS (
  description = "Raw human and AI payloads per image. One row per payload arrival."
);


-- ------------------------------------------------------------
-- 2. Comparison results table
--    One row per JSON field per AI iteration per image.
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `your_project.your_dataset.image_comparison_results` (
  image_id          STRING    NOT NULL,
  ai_iteration      INT64     NOT NULL,   -- 1-based, ordered by created_at ASC
  ai_created_at     TIMESTAMP,
  human_created_at  TIMESTAMP,
  field_name        STRING    NOT NULL,   -- dot-notation path e.g. "metadata.label"
  human_value       STRING,              -- null if field absent in human payload
  ai_value          STRING,              -- null if field absent in AI payload
  is_match          BOOL      NOT NULL,
  compared_at       TIMESTAMP NOT NULL
)
PARTITION BY DATE(compared_at)
CLUSTER BY image_id, field_name
OPTIONS (
  description = "Field-level comparison results between human and AI payloads."
);


-- ------------------------------------------------------------
-- 3. Pending comparisons table (durable state store)
--    Stores orphaned payloads awaiting their counterpart.
--    Overwritten (WRITE_TRUNCATE) on every pipeline run.
--    pending_type: 'human' or 'ai'
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `your_project.your_dataset.pending_comparisons` (
  image_id         STRING    NOT NULL,
  pending_type     STRING    NOT NULL,   -- 'human' or 'ai'
  payload          STRING    NOT NULL,   -- raw JSON string of the orphaned payload
  created_at       TIMESTAMP,           -- original created_at from source table
  first_seen_at    TIMESTAMP NOT NULL,  -- when first written to pending
  last_retried_at  TIMESTAMP,           -- updated on each pipeline run
  retry_count      INT64     NOT NULL   -- incremented on each retry
)
PARTITION BY DATE(first_seen_at)
OPTIONS (
  description = "Durable state table for orphaned payloads awaiting their counterpart. "
                "Full refresh (WRITE_TRUNCATE) on every pipeline run — resolved rows "
                "are automatically removed by not emitting them from the pipeline."
);


-- ------------------------------------------------------------
-- 4. Dead-letter table
--    Payloads that exceeded MAX_WAIT_DAYS without a counterpart.
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `your_project.your_dataset.dead_letter_comparisons` (
  image_id      STRING    NOT NULL,
  pending_type  STRING    NOT NULL,   -- which side was orphaned: 'human' or 'ai'
  payload       STRING,               -- the orphaned payload JSON
  created_at    TIMESTAMP,
  first_seen_at TIMESTAMP,
  aged_out_at   TIMESTAMP NOT NULL,  -- when it was moved to dead-letter
  reason        STRING                -- human-readable reason
)
PARTITION BY DATE(aged_out_at)
OPTIONS (
  description = "Payloads that waited longer than MAX_WAIT_DAYS without a counterpart."
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
-- FROM `your_project.your_dataset.image_comparison_results`
-- GROUP BY field_name
-- ORDER BY match_pct ASC;
