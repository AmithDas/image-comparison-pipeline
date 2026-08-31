-- Migration 003: create a read-time cleanup view over comparison_results.
--
-- The raw comparison_results table stays append-only/auditable. This view hides
-- only AI-only false mismatches that are covered by another case for the same
-- image/segment/AI payload scope.
--
-- NOTE (post-004): as of migration 004, human payloads for the same
-- image_id+segment are always merged across case_ids upstream, at ingest —
-- the cross-case false-mismatch problem this view papers over no longer
-- occurs for rows written after that change shipped. This view remains
-- useful only for historical rows written before then; leave it in place
-- rather than dropping it.
--
-- Replace your_project.your_dataset with the deployment project and dataset.

CREATE OR REPLACE VIEW `your_project.your_dataset.comparison_results_clean_view` AS
WITH normalized AS (
  SELECT
    r.*,
    CASE
      WHEN r.array_key IS NOT NULL AND (
        r.field_name = 'tradelines.tradelineRequest.disputeCodes'
        OR STARTS_WITH(r.field_name, 'tradelines.tradelineRequest.disputeCodes.')
        OR r.field_name = 'credit.dob.disputeCodes'
        OR STARTS_WITH(r.field_name, 'credit.dob.disputeCodes.')
        OR r.field_name = 'credit.ssn.disputeCodes'
        OR STARTS_WITH(r.field_name, 'credit.ssn.disputeCodes.')
        OR r.field_name = 'nonReported.disputeCodes'
        OR STARTS_WITH(r.field_name, 'nonReported.disputeCodes.')
      ) THEN 'soft_array_parent'
      WHEN r.array_key IS NOT NULL THEN 'array_entity'
      ELSE 'segment_type'
    END AS scope_level,

    CASE
      WHEN r.array_key IS NOT NULL AND (
        r.field_name = 'tradelines.tradelineRequest.disputeCodes'
        OR STARTS_WITH(r.field_name, 'tradelines.tradelineRequest.disputeCodes.')
        OR r.field_name = 'credit.dob.disputeCodes'
        OR STARTS_WITH(r.field_name, 'credit.dob.disputeCodes.')
        OR r.field_name = 'credit.ssn.disputeCodes'
        OR STARTS_WITH(r.field_name, 'credit.ssn.disputeCodes.')
        OR r.field_name = 'nonReported.disputeCodes'
        OR STARTS_WITH(r.field_name, 'nonReported.disputeCodes.')
      ) THEN REGEXP_REPLACE(r.array_key, r'-[^-]+-[^-]+$', '')
      WHEN r.array_key IS NOT NULL THEN r.array_key
      ELSE '__SEGMENT_TYPE_ONLY__'
    END AS scope_key
  FROM `your_project.your_dataset.comparison_results` r
),

covered_scopes AS (
  SELECT DISTINCT
    image_id,
    segment,
    ai_created_at,
    segment_type,
    scope_level,
    scope_key,
    case_id
  FROM normalized
  WHERE human_value IS NOT NULL
    AND segment_type IS NOT NULL
    AND scope_key IS NOT NULL
)

SELECT r.* EXCEPT(scope_level, scope_key)
FROM normalized r
WHERE NOT (
  r.is_match = FALSE
  AND r.human_value IS NULL
  AND r.ai_value IS NOT NULL
  AND r.segment_type IS NOT NULL

  -- Hide the row only when some case covers this same segment_type/entity scope.
  AND EXISTS (
    SELECT 1
    FROM covered_scopes c
    WHERE c.image_id = r.image_id
      AND c.segment = r.segment
      AND c.ai_created_at = r.ai_created_at
      AND c.segment_type = r.segment_type
      AND c.scope_level = r.scope_level
      AND c.scope_key = r.scope_key
  )

  -- Keep true missing-field mismatches for the case that owns this scope.
  AND NOT EXISTS (
    SELECT 1
    FROM covered_scopes h
    WHERE h.image_id = r.image_id
      AND h.segment = r.segment
      AND h.ai_created_at = r.ai_created_at
      AND h.segment_type = r.segment_type
      AND h.scope_level = r.scope_level
      AND h.scope_key = r.scope_key
      AND IFNULL(h.case_id, '') = IFNULL(r.case_id, '')
  )
);
