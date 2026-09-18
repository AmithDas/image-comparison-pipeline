view: acis_creditsvc_ai_metadata_summary {
  sql_table_name: `usis_iris_views.acis_creditsvc_ai_metadata_summary_view` ;;

  dimension: file_name {
    type: string
    sql: ${TABLE}.file_name ;;
    primary_key: yes
  }

  dimension: final_action {
    type: string
    sql: ${TABLE}.final_action ;;
  }

  measure: count {
    type: count
  }
}
