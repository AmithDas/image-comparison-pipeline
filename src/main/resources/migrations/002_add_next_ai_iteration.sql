-- ============================================================
-- Migration 002: add next_ai_iteration to pending_comparisons.
--
-- Run this once, manually, before deploying the pipeline version that
-- decouples ai_iteration numbering from matched_ai_keys.size(). See
-- src/main/resources/bigquery_ddl.sql for the target schema.
--
-- Background: matched_ai_keys.size() was being used as the source of the
-- next ai_iteration number. That's unsafe — matched_ai_keys can grow via
-- mergeCaseMeta's defensive union (when two pending rows collide on the
-- same case+pending_type) without a corresponding real match ever having
-- happened, which can inflate the derived iteration number past what was
-- actually emitted — the observed symptom being an image with several
-- case_ids where none of them ever produced an ai_iteration=1 row.
--
-- next_ai_iteration is an explicit, independent counter: incremented by
-- exactly the number of AI rows matched each run, resolved by MAX (not
-- union) on merge. matched_ai_keys keeps doing identity/dedup checks only.
--
-- This migration is purely additive — no data is moved or dropped.
-- Existing rows get next_ai_iteration seeded from the current
-- matched_ai_keys count, which is the best available estimate for rows
-- that predate this migration (an exact historical count isn't
-- recoverable any more precisely than that, same as with migration 001's
-- matched_ai_keys itself).
--
-- Replace `your_project.your_dataset` with your actual values before running.
-- ============================================================


-- ------------------------------------------------------------
-- 1. Add the column, additive (safe with a live table).
-- ------------------------------------------------------------
ALTER TABLE `your_project.your_dataset.pending_comparisons`
  ADD COLUMN IF NOT EXISTS next_ai_iteration INT64;


-- ------------------------------------------------------------
-- 2. Backfill from the current matched_ai_keys count.
-- ------------------------------------------------------------
UPDATE `your_project.your_dataset.pending_comparisons`
SET next_ai_iteration = IFNULL(ARRAY_LENGTH(SPLIT(matched_ai_keys, ';')), 0)
WHERE next_ai_iteration IS NULL;

UPDATE `your_project.your_dataset.pending_comparisons`
SET next_ai_iteration = 0
WHERE matched_ai_keys IS NULL AND next_ai_iteration IS NULL;


-- ------------------------------------------------------------
-- Verification query — run after the migration to sanity-check.
-- ------------------------------------------------------------

-- Should return 0: no rows left with a null counter.
-- SELECT COUNT(*) FROM `your_project.your_dataset.pending_comparisons`
-- WHERE next_ai_iteration IS NULL;
