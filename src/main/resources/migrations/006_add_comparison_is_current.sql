-- ============================================================
-- Migration 006: add is_current to comparison_results, and a
-- comparison_results_current_view over it.
--
-- Background: as of migration 005, a group's comparison is recomputed
-- whenever its content actually changes (a human update or a new AI payload),
-- always against the group's single latest AI payload. The previous
-- comparison's rows are now stale once that happens — this migration adds a
-- way to mark them non-current, via MarkSupersededComparisonsFn, which runs
-- immediately before FlattenAndCompareFn writes a group's replacement rows.
--
-- Rows are marked non-current, not deleted: comparison_results stays a genuine
-- append-only audit trail (e.g. a case's original value before another case's
-- correction stays recoverable), while comparison_results_current_view gives
-- consumers exactly one row per (image_id, segment, field_name, array_key) —
-- the live result — without needing to remember the is_current filter
-- themselves. Same spirit as the existing comparison_results_clean_view
-- (migration 003), which remains useful only for historical pre-cross-case-merge
-- rows.
--
-- This migration is purely additive — no data is moved or dropped. Existing
-- rows are backfilled to is_current = TRUE: each one is the only comparison on
-- record for its group so far, so TRUE is the correct starting state.
--
-- Replace `your_project.your_dataset` with the deployment project and dataset.
-- ============================================================


-- ------------------------------------------------------------
-- 1. Add the column, additive (safe with a live table).
-- ------------------------------------------------------------
ALTER TABLE `your_project.your_dataset.comparison_results`
  ADD COLUMN IF NOT EXISTS is_current BOOL;


-- ------------------------------------------------------------
-- 2. Backfill existing rows — each is the only comparison recorded so far for
--    its group, so TRUE is correct.
-- ------------------------------------------------------------
UPDATE `your_project.your_dataset.comparison_results`
SET is_current = TRUE
WHERE is_current IS NULL;


-- ------------------------------------------------------------
-- 3. Convenience view: the live comparison per group/field, no filter needed.
-- ------------------------------------------------------------
CREATE OR REPLACE VIEW `your_project.your_dataset.comparison_results_current_view` AS
SELECT *
FROM `your_project.your_dataset.comparison_results`
WHERE is_current = TRUE;


-- ------------------------------------------------------------
-- Verification query — run after the migration to sanity-check.
-- ------------------------------------------------------------

-- Should return 0: no row left with a null is_current.
-- SELECT COUNT(*) FROM `your_project.your_dataset.comparison_results`
-- WHERE is_current IS NULL;
