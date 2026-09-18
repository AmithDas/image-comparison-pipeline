view: confirmation_number {
  # This view has no sql_table_name — it only ever exists as the target
  # of an UNNEST join (see case_processing.explore.lkml). Each row is one
  # semicolon-delimited value split out of gcs_salesforce_curr_event.confirmation_number_raw.

  dimension: value {
    type: string
    sql: ${TABLE} ;;
  }
}
