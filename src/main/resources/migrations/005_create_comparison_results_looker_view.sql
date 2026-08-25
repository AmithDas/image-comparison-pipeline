-- Migration 005: create a Looker-facing comparison results view.
--
-- This view derives scope coverage directly from comparison_results. It does
-- not require comparison_scope_events. The view keeps true mismatches while
-- hiding AI-only false mismatches once another case covers the same scope.
--
-- Replace your_project.your_dataset with the deployment project and dataset.

CREATE OR REPLACE VIEW `your_project.your_dataset.comparison_results_looker_view` AS
WITH normalized_results AS (
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

covered_by_any_case AS (
  SELECT DISTINCT
    image_id,
    segment,
    ai_created_at,
    segment_type,
    scope_level,
    scope_key
  FROM normalized_results
  WHERE human_value IS NOT NULL
    AND segment_type IS NOT NULL
),

covered_by_same_case AS (
  SELECT DISTINCT
    image_id,
    segment,
    ai_created_at,
    segment_type,
    scope_level,
    scope_key,
    case_id
  FROM normalized_results
  WHERE human_value IS NOT NULL
    AND segment_type IS NOT NULL
),

final_rows AS (
  SELECT
    r.*,
    any_scope.image_id IS NOT NULL AS is_scope_covered_by_any_case,
    same_scope.image_id IS NOT NULL AS is_scope_covered_by_same_case
  FROM normalized_results r
  LEFT JOIN covered_by_any_case any_scope
    ON any_scope.image_id = r.image_id
    AND any_scope.segment = r.segment
    AND IFNULL(any_scope.ai_created_at, TIMESTAMP '0001-01-01 00:00:00+00')
        = IFNULL(r.ai_created_at, TIMESTAMP '0001-01-01 00:00:00+00')
    AND any_scope.segment_type = r.segment_type
    AND any_scope.scope_level = r.scope_level
    AND any_scope.scope_key = r.scope_key
  LEFT JOIN covered_by_same_case same_scope
    ON same_scope.image_id = r.image_id
    AND same_scope.segment = r.segment
    AND IFNULL(same_scope.ai_created_at, TIMESTAMP '0001-01-01 00:00:00+00')
        = IFNULL(r.ai_created_at, TIMESTAMP '0001-01-01 00:00:00+00')
    AND same_scope.segment_type = r.segment_type
    AND same_scope.scope_level = r.scope_level
    AND same_scope.scope_key = r.scope_key
    AND IFNULL(same_scope.case_id, '') = IFNULL(r.case_id, '')
)

SELECT * EXCEPT(scope_level, scope_key)
FROM final_rows
WHERE NOT (
  is_match = FALSE
  AND human_value IS NULL
  AND ai_value IS NOT NULL
  AND segment_type IS NOT NULL
  AND is_scope_covered_by_any_case = TRUE
  AND is_scope_covered_by_same_case = FALSE
);
