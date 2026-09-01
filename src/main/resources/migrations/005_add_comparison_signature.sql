-- ============================================================
-- Migration 005: replace replay-based AI matching with a
-- signature-based "always compare against the latest AI payload" model.
--
-- Background: the pipeline used to replay full AI history — every unmatched AI
-- payload for a group produced its own comparison iteration, tracked via
-- matched_ai_keys/next_ai_iteration. That's replaced with: compare only against
-- the group's single latest AI payload, and only when either side's content has
-- actually changed since the last comparison (a human update — a case merging
-- in, a field being corrected — or a genuinely new AI payload). See
-- FilterAndPairFn's class doc for the full mechanism.
--
-- matched_ai_keys/next_ai_iteration are deprecated by this change (left in the
-- schema, unpopulated going forward, per this codebase's additive-only
-- migration convention — see migration 002 for the same pattern). This
-- migration adds the two columns that replace them:
--   - last_compared_signature: signature of (human payload, latest AI payload)
--     as of the last comparison actually performed.
--   - comparison_version: an observability counter, incremented each time a
--     new comparison fires. Written into comparison_results.ai_iteration.
--
-- This migration is purely additive — no data is moved or dropped.
-- No backfill for last_compared_signature is possible or needed: NULL means
-- "never compared under this model," which correctly triggers a fresh
-- comparison for every currently-pending group on the first run after this
-- ships. Expect that first run to re-compare every group with a pending human
-- payload and at least one AI payload, even if nothing has actually changed
-- for most of them — a one-time cost of adopting the new model.
--
-- Replace `your_project.your_dataset` with the deployment project and dataset.
-- ============================================================


-- ------------------------------------------------------------
-- 1. Add the columns, additive (safe with a live table).
-- ------------------------------------------------------------
ALTER TABLE `your_project.your_dataset.pending_comparisons`
  ADD COLUMN IF NOT EXISTS last_compared_signature STRING;

ALTER TABLE `your_project.your_dataset.pending_comparisons`
  ADD COLUMN IF NOT EXISTS comparison_version INT64;


-- ------------------------------------------------------------
-- 2. Backfill comparison_version from the deprecated next_ai_iteration as a
--    best-available estimate, so existing groups don't restart their counter
--    at 0 (purely cosmetic — comparison_version is observability-only).
-- ------------------------------------------------------------
UPDATE `your_project.your_dataset.pending_comparisons`
SET comparison_version = IFNULL(next_ai_iteration, 0)
WHERE comparison_version IS NULL;


-- ------------------------------------------------------------
-- Verification query — run after the migration to sanity-check.
-- ------------------------------------------------------------

-- Should return 0: no row left with a null comparison_version.
-- SELECT COUNT(*) FROM `your_project.your_dataset.pending_comparisons`
-- WHERE comparison_version IS NULL;
