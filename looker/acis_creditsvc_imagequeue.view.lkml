view: acis_creditsvc_imagequeue {
  sql_table_name: `usis_iris_views.acis_creditsvc_imagequeue_and_process_and_activity_history_view` ;;

  dimension: file_name {
    type: string
    sql: ${TABLE}.file_name ;;
    primary_key: yes
  }

  dimension: queue {
    type: string
    sql: ${TABLE}.queue ;;
  }

  dimension: process_agent_id {
    type: string
    sql: ${TABLE}.process_agent_id ;;
  }

  dimension: received_time {
    type: date_time
    sql: ${TABLE}.receive_date_EST ;;
  }

  dimension: received_date {
    type: date
    sql: DATE(${received_time}) ;;
  }

  dimension: received_month {
    type: date
    sql: DATE_TRUNC(${received_date}, MONTH) ;;
  }

  dimension: load_time {
    type: date_time
    sql: ${TABLE}.load_date_EST ;;
  }

  dimension: load_date {
    type: date
    sql: DATE(${load_time}) ;;
  }

  dimension: load_month {
    type: date
    sql: DATE_TRUNC(${load_date}, MONTH) ;;
  }

  # acis_creditsvc_ai_metadata_summary_view has multiple rows per file_name
  # (not unique). Do NOT join it directly as one_to_one — that produced fan-out
  # that corrupted both the image count and the average processing time via
  # Looker's symmetric aggregates. The original requirements query resolved
  # this by keeping only the row where final_action IS NOT NULL, so we
  # reproduce that here as a correlated scalar subquery instead.
  dimension: outcome {
    type: string
    sql:
      (SELECT metadata.final_action
       FROM `usis_iris_views.acis_creditsvc_ai_metadata_summary_view` AS metadata
       WHERE metadata.file_name = ${TABLE}.file_name
         AND metadata.final_action IS NOT NULL
       LIMIT 1) ;;
  }

  measure: count {
    type: count
  }
}
