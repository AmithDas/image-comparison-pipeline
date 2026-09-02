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
  case_id       STRING,               -- distinguishes multiple cases sharing one image's AI
                                       -- payload; null/absent for segments with a single case
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
  case_id           STRING,               -- the case_id that actually contributed this row's field —
                                           -- resolved per row from metadata embedded in the merged
                                           -- human payload JSON (_sourceCaseId on an array item,
                                           -- _caseIdByField for a scalar field, falling back to the
                                           -- pending row's canonical case_id when the field's section
                                           -- had only one contributor). Null when no real case_id has
                                           -- ever been seen for this image+segment. See FilterAndPairFn
                                           -- and FlattenAndCompareFn — human payloads for the same
                                           -- image_id+segment are always merged across case_ids, so
                                           -- this is lineage, not a partition.
  ai_iteration      INT64     NOT NULL,   -- comparison-version counter for this image+segment
                                           -- group (see FilterAndPairFn.comparison_version) —
                                           -- incremented each time a comparison actually fires
                                           -- (human or AI content changed since the last one).
                                           -- NOT a replay index: the group is always compared
                                           -- against only its single latest AI payload, never a
                                           -- backlog of unmatched history.
  ai_created_at     TIMESTAMP,
  human_created_at  TIMESTAMP,
  field_name        STRING    NOT NULL,   -- dot-notation path e.g. "terms.code"
  array_key         STRING,               -- match-key value for keyed arrays (e.g. "A"); null for scalars
  segment_type      STRING,               -- root-node label, e.g. "authentication"; null for root scalars
  human_value       STRING,               -- Barricade-encrypted; null if field absent in human payload
  ai_value          STRING,               -- Barricade-encrypted; null if field absent in AI payload
  is_match          BOOL      NOT NULL,   -- compared on plaintext before encryption
  load_time         TIMESTAMP NOT NULL    -- append-only: a new comparison for a group (see
                                           -- ai_iteration/comparison_version above) is inserted
                                           -- alongside any prior ones for the same image+segment,
                                           -- not deduplicated or hidden — expect duplicates across
                                           -- comparison_version for a group with more than one
                                           -- comparison in its history.
)
PARTITION BY DATE(load_time)
CLUSTER BY image_id, field_name
OPTIONS (
  description = "Field-level comparison results between human and AI payloads, across all segments."
);


-- ------------------------------------------------------------
-- 3. Pending comparisons table (durable state store — case/human side only)
--    Stores incomplete or already-matched cases waiting for more AI iterations.
--    Overwritten (WRITE_TRUNCATE) on every pipeline run — resolved/aged rows
--    disappear automatically by not being re-emitted.
--    Mirrors src/main/resources/avro/pending_row.json.
--    pending_type: 'human', 'human:merged', or 'human:<subTypeName>'
--    (AI payloads live in ai_pending_comparisons instead — see below.)
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `your_project.your_dataset.pending_comparisons` (
  image_id          STRING    NOT NULL,
  key_id            STRING    NOT NULL,   -- Barricade encryption key identifier
  segment           STRING    NOT NULL,   -- segment name from --segmentConfigs
  case_id           STRING,               -- canonical (earliest-arriving) case_id contributing to
                                           -- this image+segment's consolidated human payload; null
                                           -- when no real case_id has ever been seen. Human payloads
                                           -- for the same image_id+segment are always merged across
                                           -- case_ids (see FilterAndPairFn) — case_id is lineage, not
                                           -- a matching partition. See merged_case_ids for the full
                                           -- contributor set.
  pending_type      STRING    NOT NULL,   -- 'human' | 'human:merged' | 'human:<subType>'
  payload           STRING    NOT NULL,   -- Barricade-encrypted payload
  created_at        TIMESTAMP,            -- original created_at from source table
  first_seen_at     TIMESTAMP NOT NULL,   -- when first written to pending
  last_retried_at   TIMESTAMP,            -- updated on each pipeline run
  retry_count       INT64     NOT NULL,   -- incremented on each retry
  matched_ai_keys   STRING,               -- DEPRECATED — unused, always null. Was the identity-key
                                           -- set for full-AI-history-replay matching; replaced by
                                           -- last_compared_signature below (the group now always
                                           -- compares against only its single latest AI payload).
                                           -- Left in the schema rather than dropped per this
                                           -- codebase's additive-only migration convention.
  next_ai_iteration INT64     NOT NULL,   -- DEPRECATED — unused, always 0. Was the replay-iteration
                                           -- counter; replaced by comparison_version below.
  merged_case_ids   STRING,               -- semicolon-joined set of every case_id that has ever
                                           -- contributed to this image+segment's consolidated human
                                           -- payload. Internal bookkeeping only — per-field/per-row
                                           -- attribution for comparison_results.case_id is resolved
                                           -- from metadata embedded in the payload JSON itself
                                           -- (_caseIdByField / _sourceCaseId), not from this column.
  last_compared_signature STRING,         -- signature of (human payload, latest AI payload) as of
                                           -- the last comparison actually performed for this group
                                           -- (see FilterAndPairFn.dedupKey) — a new comparison only
                                           -- fires when this changes, so a human update (a case
                                           -- merging in, a field being corrected) or a genuinely new
                                           -- AI payload both trigger a fresh comparison even against
                                           -- an AI payload already compared before. Null until the
                                           -- group's first-ever comparison.
  comparison_version INT64    NOT NULL    -- observability counter: incremented each time a new
                                           -- comparison actually fires for this group. Written into
                                           -- comparison_results.ai_iteration — not a replay index.
)
PARTITION BY DATE(first_seen_at)
OPTIONS (
  description = "Durable state table for cases (human payloads) waiting for more AI iterations. "
                "Full refresh (WRITE_TRUNCATE) on every pipeline run. Aging is evaluated per case "
                "against its own first_seen_at (see FilterAndPairFn), independent of other cases "
                "or of ai_pending_comparisons."
);


-- ------------------------------------------------------------
-- 4. AI pending table (durable replay log — AI side only)
--    An AI payload is retained here for as long as it hasn't individually
--    aged out, regardless of whether it has already been matched to one or
--    more cases — a case discovered later still needs to be compared against
--    AI payloads that arrived before it existed. Rows are never removed just
--    because they matched; see FilterAndPairFn's per-case matched_ai_keys.
--    Mirrors src/main/resources/avro/ai_pending_row.json.
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `your_project.your_dataset.ai_pending_comparisons` (
  image_id          STRING    NOT NULL,
  key_id            STRING    NOT NULL,   -- Barricade encryption key identifier
  segment           STRING    NOT NULL,   -- segment name from --segmentConfigs
  payload           STRING    NOT NULL,   -- Barricade-encrypted payload
  created_at        TIMESTAMP,            -- original created_at from source table
  first_seen_at     TIMESTAMP NOT NULL,   -- when first written to pending
  last_retried_at   TIMESTAMP,            -- updated on each pipeline run
  retry_count       INT64     NOT NULL    -- incremented on each retry
)
PARTITION BY DATE(first_seen_at)
OPTIONS (
  description = "Durable replay log of AI payloads awaiting (or already matched to) one or more "
                "cases. Full refresh (WRITE_TRUNCATE) on every pipeline run. A row only ages out "
                "based on its own first_seen_at, independent of pending_comparisons."
);


-- ------------------------------------------------------------
-- 5. Dead-letter table
--    Payloads that exceeded MAX_WAIT_DAYS (FilterAndPairFn.MAX_WAIT_DAYS)
--    without a counterpart, from either pending_comparisons or
--    ai_pending_comparisons.
--    Mirrors src/main/resources/avro/dead_letter_row.json.
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `your_project.your_dataset.dead_letter_comparisons` (
  image_id      STRING    NOT NULL,
  key_id        STRING,
  segment       STRING    NOT NULL,   -- segment name from --segmentConfigs
  case_id       STRING,               -- populated for aged-out case rows; null for aged-out AI rows
  pending_type  STRING    NOT NULL,   -- which side was orphaned: 'human'/'human:...' or 'ai'
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
