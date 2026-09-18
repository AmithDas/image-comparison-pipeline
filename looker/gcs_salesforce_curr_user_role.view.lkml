view: gcs_salesforce_curr_user_role {
  sql_table_name: `corpsvc-fint-edh1-prd-de9f.corpsvc_fint_us_cda_iris_views_storage.gcs_salesforce_curr_user_role_view` ;;

  dimension: id {
    type: string
    sql: ${TABLE}.Id ;;
    primary_key: yes
  }

  dimension: name {
    type: string
    sql: ${TABLE}.Name ;;
  }

  dimension: rollupdescription {
    type: string
    sql: ${TABLE}.rollupdescription ;;
  }

  dimension: developername {
    type: string
    sql: ${TABLE}.developername ;;
  }

  # Processing-group location logic, evaluated here since it needs both
  # user and user_role fields; referenced from the explore via the join.
  dimension: processing_group {
    type: string
    sql:
      CASE
        WHEN LOWER(${gcs_salesforce_curr_user.user_group_location}) LIKE '%tp%'
          OR LOWER(${gcs_salesforce_curr_user.user_group_location}) LIKE '%teleperformance%'
          OR LOWER(${name}) LIKE '%tp%'
          OR LOWER(${name}) LIKE '%teleperformance%'
          OR LOWER(${rollupdescription}) LIKE '%tp%'
          OR LOWER(${rollupdescription}) LIKE '%teleperformance%'
          OR LOWER(${developername}) LIKE '%tp%'
          OR LOWER(${developername}) LIKE '%teleperformance%'
          OR LOWER(${gcs_salesforce_curr_user.company_name}) LIKE '%tp%'
          OR LOWER(${gcs_salesforce_curr_user.company_name}) LIKE '%teleperformance%'
          OR LOWER(${gcs_salesforce_curr_user.department}) LIKE '%tp%'
          OR LOWER(${gcs_salesforce_curr_user.department}) LIKE '%teleperformance%'
          OR LOWER(${gcs_salesforce_curr_user.division}) LIKE '%tp%'
          OR LOWER(${gcs_salesforce_curr_user.division}) LIKE '%teleperformance%'
        THEN 'Teleperformance'
        WHEN LOWER(${gcs_salesforce_curr_user.user_group_location}) LIKE '%atlanta%'
          OR LOWER(${name}) LIKE '%atlanta%'
          OR LOWER(${rollupdescription}) LIKE '%atlanta%'
          OR LOWER(${developername}) LIKE '%atlanta%'
          OR LOWER(${gcs_salesforce_curr_user.company_name}) LIKE '%atlanta%'
          OR LOWER(${gcs_salesforce_curr_user.department}) LIKE '%atlanta%'
          OR LOWER(${gcs_salesforce_curr_user.division}) LIKE '%atlanta%'
        THEN 'Atlanta'
        WHEN LOWER(${gcs_salesforce_curr_user.user_group_location}) LIKE '%costa rica%'
          OR LOWER(${gcs_salesforce_curr_user.user_group_location}) LIKE '%costa_rica%'
          OR LOWER(${gcs_salesforce_curr_user.user_group_location}) LIKE '%equifax cr%'
          OR LOWER(${name}) LIKE '%costa rica%'
          OR LOWER(${name}) LIKE '%costa_rica%'
          OR LOWER(${name}) LIKE '%equifax cr%'
          OR LOWER(${rollupdescription}) LIKE '%costa rica%'
          OR LOWER(${rollupdescription}) LIKE '%costa_rica%'
          OR LOWER(${rollupdescription}) LIKE '%equifax cr%'
          OR LOWER(${developername}) LIKE '%costa rica%'
          OR LOWER(${developername}) LIKE '%costa_rica%'
          OR LOWER(${developername}) LIKE '%equifax cr%'
          OR LOWER(${gcs_salesforce_curr_user.company_name}) LIKE '%costa rica%'
          OR LOWER(${gcs_salesforce_curr_user.company_name}) LIKE '%costa_rica%'
          OR LOWER(${gcs_salesforce_curr_user.company_name}) LIKE '%equifax cr%'
          OR LOWER(${gcs_salesforce_curr_user.department}) LIKE '%costa rica%'
          OR LOWER(${gcs_salesforce_curr_user.department}) LIKE '%costa_rica%'
          OR LOWER(${gcs_salesforce_curr_user.department}) LIKE '%equifax cr%'
          OR LOWER(${gcs_salesforce_curr_user.division}) LIKE '%costa rica%'
          OR LOWER(${gcs_salesforce_curr_user.division}) LIKE '%costa_rica%'
          OR LOWER(${gcs_salesforce_curr_user.division}) LIKE '%equifax cr%'
        THEN 'Costa Rica'
        ELSE 'Other'
      END ;;
  }

  measure: count {
    type: count
  }
}
