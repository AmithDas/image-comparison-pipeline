view: gcs_salesforce_curr_user {
  sql_table_name: `corpsvc-fint-edh1-prd-de9f.corpsvc_fint_us_cda_iris_views_storage.gcs_salesforce_curr_user_view` ;;

  dimension: id {
    type: string
    sql: ${TABLE}.Id ;;
    primary_key: yes
  }

  dimension: user_role_id {
    type: string
    sql: ${TABLE}.UserRoleId ;;
  }

  dimension: employee_number {
    type: string
    sql: ${TABLE}.EmployeeNumber ;;
  }

  dimension: user_name {
    type: string
    sql: CONCAT(${TABLE}.FirstName, " ", ${TABLE}.LastName) ;;
  }

  dimension: user_group_location {
    type: string
    sql: ${TABLE}.user_group_location__c ;;
  }

  dimension: company_name {
    type: string
    sql: ${TABLE}.companyName ;;
  }

  dimension: department {
    type: string
    sql: ${TABLE}.Department ;;
  }

  dimension: division {
    type: string
    sql: ${TABLE}.Division ;;
  }

  measure: count {
    type: count
  }
}
