view: gcs_salesforce_curr_event {
  sql_table_name: `corpsvc-fint-edh1-prd-de9f.corpsvc_fint_us_cda_iris_views_storage.gcs_salesforce_curr_event_view` ;;

  dimension: id {
    type: string
    sql: ${TABLE}.Id ;;
    primary_key: yes
  }

  dimension: what_id {
    type: string
    sql: ${TABLE}.WhatId ;;
  }

  dimension: owner_id {
    type: string
    sql: ${TABLE}.OwnerId ;;
  }

  dimension: confirmation_number_raw {
    type: string
    sql: ${TABLE}.Confirmation_Number__c ;;
  }

  dimension: event_category {
    type: string
    sql: ${TABLE}.Event_Category__c ;;
  }

  dimension: end_time {
    type: date_time
    sql: DATETIME(SAFE_CAST(REPLACE(${TABLE}.End__c, '+0000', 'Z') AS TIMESTAMP), 'America/New_York') ;;
  }

  dimension: end_date {
    type: date
    sql: DATE(${end_time}) ;;
  }

  dimension: end_month {
    type: date
    sql: DATE_TRUNC(${end_date}, MONTH) ;;
  }

  dimension: end_datetime_raw {
    type: string
    sql: ${TABLE}.EndDateTime ;;
    hidden: yes
  }

  # --- Correlated scalar subqueries, in place of the doc_event / auth_event
  # --- derived-table subqueries. Each is evaluated per-row against ${what_id},
  # --- no separate derived table or extra explore join required.

  dimension: doc_review_end_time {
    type: date_time
    sql: DATETIME(
           SAFE_CAST(REPLACE(
             (SELECT MAX(doc_event.EndDateTime)
              FROM `corpsvc-fint-edh1-prd-de9f.corpsvc_fint_us_cda_iris_views_storage.gcs_salesforce_curr_event_view` AS doc_event
              WHERE doc_event.WhatId = ${what_id}
                AND doc_event.Event_Category__c = 'Document Review'),
             '+0000', 'Z') AS TIMESTAMP),
           'America/New_York'
         ) ;;
  }

  dimension: doc_review_end_date {
    type: date
    sql: DATE(${doc_review_end_time}) ;;
  }

  dimension: doc_review_end_month {
    type: date
    sql: DATE_TRUNC(${doc_review_end_date}, MONTH) ;;
  }

  dimension: assignment_to_doc_review_seconds {
    type: number
    sql: TIMESTAMP_DIFF(
           SAFE_CAST(
             (SELECT MAX(doc_event.EndDateTime)
              FROM `corpsvc-fint-edh1-prd-de9f.corpsvc_fint_us_cda_iris_views_storage.gcs_salesforce_curr_event_view` AS doc_event
              WHERE doc_event.WhatId = ${what_id}
                AND doc_event.Event_Category__c = 'Document Review')
             AS TIMESTAMP),
           SAFE_CAST(${assignment_date_time_start_raw} AS TIMESTAMP),
           SECOND
         ) ;;
  }

  dimension: auth_end_time {
    type: date_time
    sql: DATETIME(
           SAFE_CAST(REPLACE(
             (SELECT MAX(auth_event.EndDateTime)
              FROM `corpsvc-fint-edh1-prd-de9f.corpsvc_fint_us_cda_iris_views_storage.gcs_salesforce_curr_event_view` AS auth_event
              WHERE auth_event.WhatId = ${what_id}
                AND auth_event.Event_Category__c = 'Consumer Authentication'),
             '+0000', 'Z') AS TIMESTAMP),
           'America/New_York'
         ) ;;
  }

  dimension: auth_end_date {
    type: date
    sql: DATE(${auth_end_time}) ;;
  }

  dimension: auth_end_month {
    type: date
    sql: DATE_TRUNC(${auth_end_date}, MONTH) ;;
  }

  dimension: assignment_to_auth_seconds {
    type: number
    sql: TIMESTAMP_DIFF(
           SAFE_CAST(
             (SELECT MAX(auth_event.EndDateTime)
              FROM `corpsvc-fint-edh1-prd-de9f.corpsvc_fint_us_cda_iris_views_storage.gcs_salesforce_curr_event_view` AS auth_event
              WHERE auth_event.WhatId = ${what_id}
                AND auth_event.Event_Category__c = 'Consumer Authentication')
             AS TIMESTAMP),
           SAFE_CAST(${assignment_date_time_start_raw} AS TIMESTAMP),
           SECOND
         ) ;;
  }

  # --- Replaces the `hist` derived-table join: latest Owner-history CreatedDate
  # --- for this event's owner/case, before the event's own EndDateTime.

  dimension: assignment_date_time_start_raw {
    type: string
    hidden: yes
    sql: (SELECT MAX(hist.CreatedDate)
          FROM `corpsvc-fint-edh1-prd-de9f.corpsvc_fint_us_cda_iris_views_storage.gcs_salesforce_curr_casehistory_view` AS hist
          WHERE hist.field = "Owner"
            AND hist.NewValue = ${owner_id}
            AND hist.CaseId = ${what_id}
            AND hist.CreatedDate < ${end_datetime_raw}) ;;
  }

  dimension: assignment_time {
    type: date_time
    sql: DATETIME(SAFE_CAST(REPLACE(${assignment_date_time_start_raw}, '+0000', 'Z') AS TIMESTAMP), 'America/New_York') ;;
  }

  dimension: assignment_date {
    type: date
    sql: DATE(${assignment_time}) ;;
  }

  dimension: assignment_month {
    type: date
    sql: DATE_TRUNC(${assignment_date}, MONTH) ;;
  }

  dimension: processing_time_seconds {
    type: number
    sql: TIMESTAMP_DIFF(
           SAFE_CAST(${TABLE}.End__c AS TIMESTAMP),
           SAFE_CAST(${assignment_date_time_start_raw} AS TIMESTAMP),
           SECOND
         ) ;;
  }

  measure: count {
    type: count
  }

  measure: average_processing_time_seconds {
    type: average
    sql: ${processing_time_seconds} ;;
  }
}
