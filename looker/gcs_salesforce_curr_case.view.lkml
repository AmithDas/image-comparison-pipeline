view: gcs_salesforce_curr_case {
  sql_table_name: `corpsvc-fint-edh1-prd-de9f.corpsvc_fint_us_cda_iris_views_storage.gcs_salesforce_curr_case_view` ;;

  dimension: id {
    type: string
    sql: ${TABLE}.Id ;;
    primary_key: yes
  }

  dimension: case_number {
    type: string
    sql: ${TABLE}.CaseNumber ;;
  }

  dimension: parent_id {
    type: string
    sql: ${TABLE}.ParentId ;;
  }

  measure: count {
    type: count
  }
}
