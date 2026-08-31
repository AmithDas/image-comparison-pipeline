-- ============================================================
-- Migration 004: add merged_case_ids to pending_comparisons; case_id semantics
-- change from a matching partition to lineage.
--
-- Background: human payloads for the same image_id+segment used to be isolated
-- per case_id — each case_id tracked its own matched_ai_keys/next_ai_iteration
-- and was compared independently (see 6a88342's excludeForeignSegmentTypes,
-- now removed). As of this migration, all case_ids sharing an image_id+segment
-- are always merged into one consolidated human payload and compared as a
-- single group — case_id is no longer a matching partition, it's lineage.
--
-- merged_case_ids records every case_id that has ever contributed to a group's
-- consolidated payload (semicolon-joined, same convention as matched_ai_keys).
-- It is bookkeeping only — per-row/per-field attribution in comparison_results
-- is resolved from metadata embedded directly in the merged payload JSON
-- (_caseIdByField / _sourceCaseId), not from this column.
--
-- This migration is purely additive — no data is moved or dropped. Existing
-- rows get merged_case_ids seeded from their own case_id, i.e. each becomes a
-- one-element lineage set. This is an approximation for rows that predate this
-- migration and represented an isolated per-case pending row rather than a
-- merged group — an exact historical contributor list isn't recoverable any
-- more precisely than that, same caveat as migration 002's backfill.
--
-- Replace `your_project.your_dataset` with your actual values before running.
-- ============================================================


-- ------------------------------------------------------------
-- 1. Add the column, additive (safe with a live table).
-- ------------------------------------------------------------
ALTER TABLE `your_project.your_dataset.pending_comparisons`
  ADD COLUMN IF NOT EXISTS merged_case_ids STRING;


-- ------------------------------------------------------------
-- 2. Backfill from the row's own case_id.
-- ------------------------------------------------------------
UPDATE `your_project.your_dataset.pending_comparisons`
SET merged_case_ids = case_id
WHERE merged_case_ids IS NULL AND case_id IS NOT NULL;


-- ------------------------------------------------------------
-- Verification query — run after the migration to sanity-check.
-- ------------------------------------------------------------

-- Should return 0: no case_id-bearing row left without a lineage set.
-- SELECT COUNT(*) FROM `your_project.your_dataset.pending_comparisons`
-- WHERE case_id IS NOT NULL AND merged_case_ids IS NULL;
