-- Migration 004: create append-only comparison scope events and a current
-- unmatched-AI-scope view.
--
-- comparison_scope_events is written by the Dataflow job. The current view is
-- intentionally derived from append-only events so a scope can move from
-- unmatched to covered when a later human case arrives, without deleting history.
--
-- Replace your_project.your_dataset with the deployment project and dataset.

CREATE TABLE IF NOT EXISTS `your_project.your_dataset.comparison_scope_events` (
  image_id      STRING    NOT NULL,
  segment       STRING    NOT NULL,
  ai_created_at TIMESTAMP,
  segment_type  STRING    NOT NULL,
  scope_level   STRING    NOT NULL,
  scope_key     STRING    NOT NULL,
  event_type    STRING    NOT NULL,
  case_id       STRING,
  load_time     TIMESTAMP NOT NULL
)
PARTITION BY DATE(load_time)
CLUSTER BY image_id, segment_type, scope_level, scope_key
OPTIONS (
  description = "Append-only scope event stream derived from comparison_results. "
                "Used to derive currently unmatched AI scopes."
);

CREATE OR REPLACE VIEW `your_project.your_dataset.unmatched_ai_scopes_current_view` AS
WITH discovered AS (
  SELECT DISTINCT
    image_id,
    segment,
    ai_created_at,
    segment_type,
    scope_level,
    scope_key
  FROM `your_project.your_dataset.comparison_scope_events`
  WHERE event_type = 'AI_SCOPE_DISCOVERED'
),

covered AS (
  SELECT DISTINCT
    image_id,
    segment,
    ai_created_at,
    segment_type,
    scope_level,
    scope_key
  FROM `your_project.your_dataset.comparison_scope_events`
  WHERE event_type = 'HUMAN_SCOPE_COVERED'
)

SELECT d.*
FROM discovered d
WHERE NOT EXISTS (
  SELECT 1
  FROM covered c
  WHERE c.image_id = d.image_id
    AND c.segment = d.segment
    AND IFNULL(c.ai_created_at, TIMESTAMP '0001-01-01 00:00:00+00')
        = IFNULL(d.ai_created_at, TIMESTAMP '0001-01-01 00:00:00+00')
    AND c.segment_type = d.segment_type
    AND c.scope_level = d.scope_level
    AND c.scope_key = d.scope_key
);
