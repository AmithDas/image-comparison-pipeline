-- ============================================================
-- Migration 001: split pending_comparisons into case-only state
-- plus a dedicated ai_pending_comparisons replay pool.
--
-- Run this once, manually, before deploying the pipeline version that
-- introduces case_id / matched_ai_keys (replacing matched_ai_count) and
-- the ai_pending_comparisons table. See src/main/resources/bigquery_ddl.sql
-- for the target schema this migrates towards.
--
-- Safe to run against a live table: pending_comparisons is WRITE_TRUNCATEd
-- by every pipeline run, so this only needs to reconcile whatever is
-- currently pending, not historical data. comparison_results and
-- dead_letter_comparisons are unaffected by this migration — they only
-- gain a nullable case_id column, which CREATE_IF_NEEDED / a plain
-- `ALTER TABLE ... ADD COLUMN case_id STRING` handles without a rebuild.
--
-- matched_ai_keys is deliberately seeded NULL for every migrated case row,
-- not as an approximation: the old matched_ai_count never recorded *which*
-- AI payloads were matched, only how many, and every AI row it could have
-- referred to was already deleted from pending_comparisons the moment it
-- matched (the old "consume on match" behavior). That data does not exist
-- anywhere to migrate — an empty matched_ai_keys is the true state, not a
-- lossy stand-in for it.
--
-- Replace `your_project.your_dataset` with your actual values before running.
-- ============================================================


-- ------------------------------------------------------------
-- 1. Create the new AI pool table, if it doesn't already exist.
--    Mirrors src/main/resources/avro/ai_pending_row.json.
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `your_project.your_dataset.ai_pending_comparisons` (
  image_id        STRING    NOT NULL,
  key_id          STRING    NOT NULL,
  segment         STRING    NOT NULL,
  payload         STRING    NOT NULL,
  created_at      TIMESTAMP,
  first_seen_at   TIMESTAMP NOT NULL,
  last_retried_at TIMESTAMP,
  retry_count     INT64     NOT NULL
)
PARTITION BY DATE(first_seen_at)
OPTIONS (
  description = "Durable replay log of AI payloads awaiting (or already matched to) one or more "
                "cases. Full refresh (WRITE_TRUNCATE) on every pipeline run. A row only ages out "
                "based on its own first_seen_at, independent of pending_comparisons."
);


-- ------------------------------------------------------------
-- 2. Move currently-pending AI rows out of pending_comparisons
--    and into the new table.
-- ------------------------------------------------------------
INSERT INTO `your_project.your_dataset.ai_pending_comparisons`
  (image_id, key_id, segment, payload, created_at, first_seen_at, last_retried_at, retry_count)
SELECT
  image_id, key_id, segment, payload, created_at, first_seen_at, last_retried_at, retry_count
FROM `your_project.your_dataset.pending_comparisons`
WHERE pending_type = 'ai';


-- ------------------------------------------------------------
-- 3. Rebuild pending_comparisons: drop the now-migrated AI rows,
--    add case_id (always NULL — no pre-existing segment used cases)
--    and matched_ai_keys (always NULL — see note above), drop
--    matched_ai_count.
--
--    NOTE: CREATE OR REPLACE TABLE ... AS SELECT does not carry over
--    the source table's OPTIONS(description = ...) — re-add it below
--    if you want the description preserved. It does carry over the
--    PARTITION BY clause when re-specified as shown.
-- ------------------------------------------------------------
CREATE OR REPLACE TABLE `your_project.your_dataset.pending_comparisons`
PARTITION BY DATE(first_seen_at)
OPTIONS (
  description = "Durable state table for cases (human payloads) waiting for more AI iterations. "
                "Full refresh (WRITE_TRUNCATE) on every pipeline run. Aging is evaluated per case "
                "against its own first_seen_at, independent of other cases or of "
                "ai_pending_comparisons."
) AS
SELECT
  image_id,
  key_id,
  segment,
  CAST(NULL AS STRING) AS case_id,
  pending_type,
  payload,
  created_at,
  first_seen_at,
  last_retried_at,
  retry_count,
  CAST(NULL AS STRING) AS matched_ai_keys
FROM `your_project.your_dataset.pending_comparisons`
WHERE pending_type != 'ai';


-- ------------------------------------------------------------
-- 4. Add case_id to comparison_results and dead_letter_comparisons.
--    Purely additive — existing rows get case_id = NULL.
-- ------------------------------------------------------------
ALTER TABLE `your_project.your_dataset.comparison_results`
  ADD COLUMN IF NOT EXISTS case_id STRING;

ALTER TABLE `your_project.your_dataset.dead_letter_comparisons`
  ADD COLUMN IF NOT EXISTS case_id STRING;


-- ------------------------------------------------------------
-- 5. Add case_id to the human_payloads source table so the pipeline
--    can start reading it. Existing rows get case_id = NULL, which
--    the pipeline treats as "no case" (single-case segment) — the
--    same behavior those rows had before this migration.
-- ------------------------------------------------------------
ALTER TABLE `your_project.human_dataset.human_payloads`
  ADD COLUMN IF NOT EXISTS case_id STRING;


-- ------------------------------------------------------------
-- Verification queries — run after the migration to sanity-check.
-- ------------------------------------------------------------

-- Should return 0: no 'ai' rows left in pending_comparisons.
-- SELECT COUNT(*) FROM `your_project.your_dataset.pending_comparisons`
-- WHERE pending_type = 'ai';

-- Row count should match what step 2's SELECT would have returned.
-- SELECT COUNT(*) FROM `your_project.your_dataset.ai_pending_comparisons`;
