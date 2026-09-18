explore: case_processing {
  from: gcs_salesforce_curr_case
  label: "APT Case Processing"

  always_filter: {
    filters: [gcs_salesforce_curr_event.confirmation_number_raw: "-NULL"]
  }

  sql_always_where:
    ${gcs_salesforce_curr_case.parent_id} IS NULL
    AND ${gcs_salesforce_curr_event.confirmation_number_raw} IS NOT NULL
    AND ${gcs_salesforce_curr_event.end_time} IS NOT NULL
    AND ${gcs_salesforce_curr_image_attached.name} IS NOT NULL
    AND ${gcs_salesforce_curr_event.processing_time_seconds} IS NOT NULL
  ;;

  join: gcs_salesforce_curr_event {
    type: left_outer
    relationship: one_to_many
    sql_on: ${gcs_salesforce_curr_case.id} = ${gcs_salesforce_curr_event.what_id} ;;
  }

  # Mirrors: LEFT JOIN UNNEST(split(event.Confirmation_Number__c, ';')) AS confirmation_number
  # Fans out one row per semicolon-delimited value. This does NOT change what
  # gcs_salesforce_curr_event.confirmation_number (the raw, unsplit string) shows per
  # row — it duplicates that row once per split value, exactly like the source query.
  # Measures on gcs_salesforce_curr_case / gcs_salesforce_curr_event are protected from
  # this fan-out by Looker's symmetric aggregates, since both views declare primary_key: yes.
  join: confirmation_number {
    sql: UNNEST(SPLIT(${gcs_salesforce_curr_event.confirmation_number_raw}, ';')) ;;
    relationship: many_to_many
  }

  join: gcs_salesforce_curr_image_attached {
    type: left_outer
    relationship: many_to_one
    sql_on: ${gcs_salesforce_curr_case.id} = ${gcs_salesforce_curr_image_attached.case_id} ;;
  }

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

  join: acis_creditsvc_imagequeue {
    type: left_outer
    relationship: many_to_one
    sql_on: ${gcs_salesforce_curr_image_attached.name} = ${acis_creditsvc_imagequeue.file_name} ;;
  }
}
