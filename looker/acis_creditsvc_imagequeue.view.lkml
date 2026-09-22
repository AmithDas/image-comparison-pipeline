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

  # The raw table has multiple rows per file_name (e.g. reprocessing attempts on
  # different dates) — the original CTE's GROUP BY 1,2,3,4 confirms this, since it
  # grouped on (file_name, received_date, load_date, outcome) together, not
  # file_name alone. file_name is therefore NOT a valid primary key by itself;
  # declaring it as one silently broke symmetric aggregates for every measure in
  # the explore (near-doubled counts/averages). Dedup to one row per file_name —
  # the most recent load_date_EST — via a correlated subquery instead of a
  # derived table. NOTE: if two rows for the same file_name share the exact same
  # load_date_EST, both will pass this check and the fan-out returns; if that
  # turns out to happen in practice, we need an additional tiebreaker column.
  dimension: is_latest_load {
    type: yesno
    hidden: yes
    sql: ${TABLE}.load_date_EST = (
           SELECT MAX(latest.load_date_EST)
           FROM `usis_iris_views.acis_creditsvc_imagequeue_and_process_and_activity_history_view` AS latest
           WHERE latest.file_name = ${TABLE}.file_name
         ) ;;
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
