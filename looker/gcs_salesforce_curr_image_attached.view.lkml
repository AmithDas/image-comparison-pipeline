view: gcs_salesforce_curr_image_attached {
  sql_table_name: `corpsvc-fint-edh1-prd-de9f.corpsvc_fint_us_cda_iris_views_storage.gcs_salesforce_curr_image_attached__c_view` ;;

  dimension: name {
    type: string
    sql: ${TABLE}.Name ;;
    primary_key: yes
  }

  dimension: case_id {
    type: string
    sql: ${TABLE}.Case__c ;;
  }

  measure: count {
    type: count
  }
}
