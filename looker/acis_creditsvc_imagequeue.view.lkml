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

  measure: count {
    type: count
  }
}
