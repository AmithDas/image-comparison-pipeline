explore: acis_creditsvc_imagequeue {
  label: "APT Case Processing"

  always_filter: {
    filters: [gcs_salesforce_curr_event.confirmation_number_raw: "-NULL"]
  }

  # Mirrors the original ai_processed_img CTE's WHERE (final_action IS NOT NULL)
  # plus the outer query's WHERE clause on apt.
  sql_always_where:
    ${acis_creditsvc_imagequeue.is_latest_load}
    AND ${acis_creditsvc_imagequeue.outcome} IS NOT NULL
    AND ${gcs_salesforce_curr_case.parent_id} IS NULL
    AND ${gcs_salesforce_curr_event.confirmation_number_raw} IS NOT NULL
    AND ${gcs_salesforce_curr_event.end_time} IS NOT NULL
    AND ${gcs_salesforce_curr_image_attached.name} IS NOT NULL
    AND ${gcs_salesforce_curr_event.processing_time_seconds} IS NOT NULL
  ;;

  # Confirmed: gcs_salesforce_curr_image_attached.name is NOT unique — the same
  # processed image file can legitimately be attached to multiple different
  # Cases (different Case__c values sharing one Name), not duplicate/noise rows.
  # relationship must be one_to_many (not many_to_one) so Looker's symmetric
  # aggregates correctly protect acis_creditsvc_imagequeue measures (e.g. count)
  # from this fan-out, while case-level measures correctly reflect one row per
  # (image, case) pair.
  join: gcs_salesforce_curr_image_attached {
    type: left_outer
    relationship: one_to_many
    sql_on: ${acis_creditsvc_imagequeue.file_name} = ${gcs_salesforce_curr_image_attached.name} ;;
  }

  join: gcs_salesforce_curr_case {
    type: left_outer
    relationship: many_to_one
    sql_on: ${gcs_salesforce_curr_image_attached.case_id} = ${gcs_salesforce_curr_case.id} ;;
  }

  join: gcs_salesforce_curr_event {
    type: left_outer
    relationship: one_to_many
    sql_on: ${gcs_salesforce_curr_case.id} = ${gcs_salesforce_curr_event.what_id} ;;
  }

  # NOTE: the original query's LEFT JOIN UNNEST(SPLIT(Confirmation_Number__c, ';'))
  # is intentionally NOT reproduced here. Its output column was always the raw,
  # unsplit Confirmation_Number__c string (never the individual unnested value),
  # so the join only ever mattered for null-filtering, which sql_always_where's
  # confirmation_number_raw IS NOT NULL already covers with no join required.
  # Looker's symmetric aggregates cannot reliably protect measures on
  # gcs_salesforce_curr_case / gcs_salesforce_curr_event across a raw sql:-based
  # UNNEST join (no sql_on key for Looker to track), so keeping this join was
  # producing near-doubled counts/averages for no functional benefit. If a
  # dimension for individual confirmation numbers is ever needed, add the
  # confirmation_number view's join back deliberately and audit any measure
  # that mixes with it.

  join: gcs_salesforce_curr_user {
    type: left_outer
    relationship: many_to_one
    sql_on: ${gcs_salesforce_curr_event.owner_id} = ${gcs_salesforce_curr_user.id} ;;
  }

  join: gcs_salesforce_curr_user_role {
    type: left_outer
    relationship: many_to_one
    sql_on: ${gcs_salesforce_curr_user.user_role_id} = ${gcs_salesforce_curr_user_role.id} ;;
  }
}
