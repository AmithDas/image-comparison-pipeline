 """
Image Comparison Pipeline — Airflow DAG  (Classic Dataflow Template)
=====================================================================
Runs the image-comparison Dataflow Classic Template every 3 hours.
The processing window (windowStart / windowEnd) is driven by a BigQuery
lookup table rather than the Airflow schedule clock, so ops can adjust
windows, replay runs, or pause processing without touching the DAG code.

Lookup table contract
─────────────────────
Expected schema for the `lookup` table (DDL at the bottom of this file):

  run_id        STRING     NOT NULL   unique identifier, e.g. "run-2026-04-23-03"
  window_start  TIMESTAMP  NOT NULL   inclusive start of the processing window
  window_end    TIMESTAMP  NOT NULL   exclusive end of the processing window
  status        STRING     NOT NULL   PENDING | RUNNING | DONE | FAILED

Each DAG run picks the single PENDING row with the earliest window_start,
marks it RUNNING, submits the Dataflow job, then marks it DONE or FAILED.

DAG run lifecycle
─────────────────
  fetch_window  →  mark_running  →  run_dataflow  →  mark_done
                                          ↓ (on failure)
                                       mark_failed

  • If no PENDING row exists the fetch_window task is skipped and the entire
    DAG run completes as SKIPPED (no error, no alert noise).
  • max_active_runs=1 + wait_until_finished=True guarantees that the next
    Dataflow job never starts before the current one has fully finished.

Configuration
─────────────
Set these Airflow Variables (Admin → Variables).  Defaults work for local dev.

  Variable key                  │  Example value
  ──────────────────────────────┼────────────────────────────────────────────
  ic_project_id                 │  my-gcp-project
  ic_region                     │  us-central1
  ic_template_gcs_path          │  gs://my-bucket/templates/image-comparison
  ic_lookup_table               │  my-project:my_dataset.lookup
  ic_ai_source_table            │  my-project:ai_dataset.ai_payloads
  ic_human_source_table         │  my-project:human_dataset.human_payloads
  ic_output_table               │  my-project:results_dataset.image_comparison_results
  ic_pending_table              │  my-project:results_dataset.pending_comparisons
  ic_dead_letter_table          │  my-project:results_dataset.dead_letter_comparisons
  ic_ai_method                  │  aimetadata
  ic_human_method               │  controller.SubmitDispute
  ic_image_name_field           │  queueImages[0].fileName
  ic_firestore_collection       │  dek_store
  ic_kms_key_path               │  projects/p/locations/l/keyRings/r/cryptoKeys/k
  ic_dataflow_service_account   │  pipeline-sa@my-project.iam.gserviceaccount.com
  ic_dataflow_temp_location     │  gs://my-bucket/tmp
  ic_dataflow_staging_location  │  gs://my-bucket/staging
  ic_max_workers                │  20
  ic_human_filter_field         │  (optional) status
  ic_human_filter_value         │  (optional) APPROVED

Classic Template — how to compile and stage
───────────────────────────────────────────
  mvn compile exec:java \\
    -Dexec.mainClass=com.yourorg.pipeline.ImageComparisonPipeline \\
    -Dexec.args="--runner=DataflowRunner \\
      --project=<PROJECT> \\
      --stagingLocation=<STAGING_LOCATION> \\
      --templateLocation=<TEMPLATE_GCS_PATH>"
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.dataflow import (
    DataflowTemplatedJobStartOperator,
)
from airflow.utils.trigger_rule import TriggerRule

# ── Environment config ────────────────────────────────────────────────────────

PROJECT_ID       = Variable.get("ic_project_id",   default_var="your-gcp-project")
REGION           = Variable.get("ic_region",        default_var="us-central1")
TEMPLATE_GCS_PATH = Variable.get("ic_template_gcs_path",
                                  default_var="gs://your-bucket/templates/image-comparison")

# BigQuery lookup table  (format: project:dataset.table)
LOOKUP_TABLE     = Variable.get("ic_lookup_table",
                                 default_var="your-project:your_dataset.lookup")

AI_SOURCE_TABLE    = Variable.get("ic_ai_source_table",
                                   default_var="your-project:ai_dataset.ai_payloads")
HUMAN_SOURCE_TABLE = Variable.get("ic_human_source_table",
                                   default_var="your-project:human_dataset.human_payloads")
OUTPUT_TABLE       = Variable.get("ic_output_table",
                                   default_var="your-project:dataset.image_comparison_results")
PENDING_TABLE      = Variable.get("ic_pending_table",
                                   default_var="your-project:dataset.pending_comparisons")
DEAD_LETTER_TABLE  = Variable.get("ic_dead_letter_table",
                                   default_var="your-project:dataset.dead_letter_comparisons")

AI_METHOD        = Variable.get("ic_ai_method",    default_var="aimetadata")
HUMAN_METHOD     = Variable.get("ic_human_method", default_var="controller.SubmitDispute")
IMAGE_NAME_FIELD = Variable.get("ic_image_name_field",
                                 default_var="queueImages[0].fileName")

FIRESTORE_COLLECTION = Variable.get("ic_firestore_collection", default_var="dek_store")
KMS_KEY_PATH         = Variable.get("ic_kms_key_path",
                                     default_var="projects/p/locations/l/keyRings/r/cryptoKeys/k")

SERVICE_ACCOUNT   = Variable.get("ic_dataflow_service_account",
                                  default_var="pipeline-sa@your-project.iam.gserviceaccount.com")
TEMP_LOCATION     = Variable.get("ic_dataflow_temp_location",
                                  default_var="gs://your-bucket/tmp")
STAGING_LOCATION  = Variable.get("ic_dataflow_staging_location",
                                  default_var="gs://your-bucket/staging")
MAX_WORKERS       = int(Variable.get("ic_max_workers", default_var="20"))

HUMAN_FILTER_FIELD = Variable.get("ic_human_filter_field", default_var="")
HUMAN_FILTER_VALUE = Variable.get("ic_human_filter_value", default_var="")

# BQ table reference in standard SQL format  (project.dataset.table)
_LOOKUP_TABLE_SQL = LOOKUP_TABLE.replace(":", ".", 1)

# ── Task 1: fetch next pending window from the lookup table ───────────────────

def fetch_window(**context) -> None:
    """
    Queries the lookup table for the earliest PENDING run, then pushes
    run_id, window_start, and window_end into XCom for downstream tasks.

    Raises AirflowSkipException when no PENDING row exists so the DAG run
    completes as SKIPPED rather than FAILED — avoids alert noise when
    the lookup table is simply empty or fully caught up.
    """
    hook = BigQueryHook(gcp_conn_id="google_cloud_default", use_legacy_sql=False)

    # FORMAT_TIMESTAMP produces 'YYYY-MM-DDTHH:MM:SSZ' which BigQuery's
    # TIMESTAMP comparison and our Java pipeline both accept without conversion.
    query = f"""
        SELECT
            run_id,
            FORMAT_TIMESTAMP('%Y-%m-%dT%H:%M:%SZ', window_start) AS window_start,
            FORMAT_TIMESTAMP('%Y-%m-%dT%H:%M:%SZ', window_end)   AS window_end
        FROM `{_LOOKUP_TABLE_SQL}`
        WHERE status = 'PENDING'
        ORDER BY window_start ASC
        LIMIT 1
    """

    rows = hook.get_records(query)

    if not rows:
        raise AirflowSkipException(
            f"No PENDING rows found in {LOOKUP_TABLE} — skipping this DAG run."
        )

    run_id, window_start, window_end = rows[0]

    ti = context["ti"]
    ti.xcom_push(key="run_id",       value=run_id)
    ti.xcom_push(key="window_start", value=window_start)
    ti.xcom_push(key="window_end",   value=window_end)


# ── Helpers for the BigQuery DML statements ───────────────────────────────────

def _update_status_query(new_status: str) -> str:
    """Returns a DML UPDATE that sets status for the run_id fetched in task 1."""
    return f"""
        UPDATE `{_LOOKUP_TABLE_SQL}`
        SET    status = '{new_status}'
        WHERE  run_id = '{{{{ ti.xcom_pull(task_ids="fetch_window", key="run_id") }}}}'
    """


def _bq_query_config(sql: str) -> dict:
    return {
        "query": {
            "query":         sql,
            "useLegacySql":  False,
        }
    }


# ── Dataflow Classic Template parameters ──────────────────────────────────────

def _dataflow_parameters() -> dict:
    """
    All values that are fixed at DAG-parse time are resolved from Variables.
    windowStart / windowEnd are XCom Jinja references resolved at task-run time.
    """
    params = {
        "aiSourceTable":    AI_SOURCE_TABLE,
        "humanSourceTable": HUMAN_SOURCE_TABLE,
        "outputTable":      OUTPUT_TABLE,
        "pendingTable":     PENDING_TABLE,
        "deadLetterTable":  DEAD_LETTER_TABLE,
        # Window comes from the lookup table, not the Airflow schedule clock.
        "windowStart": "{{ ti.xcom_pull(task_ids='fetch_window', key='window_start') }}",
        "windowEnd":   "{{ ti.xcom_pull(task_ids='fetch_window', key='window_end') }}",
        "aiMethod":         AI_METHOD,
        "humanMethod":      HUMAN_METHOD,
        "imageNameField":   IMAGE_NAME_FIELD,
        "firestoreCollection": FIRESTORE_COLLECTION,
        "kmsKeyPath":       KMS_KEY_PATH,
    }
    if HUMAN_FILTER_FIELD and HUMAN_FILTER_VALUE:
        params["humanFilterField"] = HUMAN_FILTER_FIELD
        params["humanFilterValue"] = HUMAN_FILTER_VALUE
    return params


def _dataflow_options() -> dict:
    """Runtime environment options passed alongside the Classic Template."""
    return {
        "tempLocation":          TEMP_LOCATION,
        "stagingLocation":       STAGING_LOCATION,
        "serviceAccountEmail":   SERVICE_ACCOUNT,
        "maxWorkers":            str(MAX_WORKERS),
        "ipConfiguration":       "WORKER_IP_PRIVATE",
    }


# ── DAG ───────────────────────────────────────────────────────────────────────

default_args = {
    "owner":            "data-platform",
    "retries":          1,
    "retry_delay":      timedelta(minutes=10),
    "depends_on_past":  False,
    "email_on_failure": True,
    "email_on_retry":   False,
}

with DAG(
    dag_id="image_comparison_pipeline",
    description=(
        "Runs the image-comparison Dataflow Classic Template every 3 hours. "
        "Processing window is driven by the lookup BQ table, not the schedule clock."
    ),
    default_args=default_args,
    schedule_interval="0 */3 * * *",
    start_date=datetime(2026, 4, 23, 0, 0, 0),
    catchup=False,
    # One run at a time — the next job won't start until this one finishes.
    max_active_runs=1,
    # Hard ceiling per run: fail the DAG rather than stall forever.
    dagrun_timeout=timedelta(hours=2, minutes=30),
    tags=["dataflow", "image-comparison", "data-platform"],
) as dag:

    # ── 1. Fetch window from lookup table ─────────────────────────────────────
    fetch_window_task = PythonOperator(
        task_id="fetch_window",
        python_callable=fetch_window,
    )

    # ── 2. Mark row as RUNNING before submitting to Dataflow ──────────────────
    mark_running = BigQueryInsertJobOperator(
        task_id="mark_running",
        configuration=_bq_query_config(_update_status_query("RUNNING")),
        project_id=PROJECT_ID,
        gcp_conn_id="google_cloud_default",
    )

    # ── 3. Run the Classic Dataflow Template ──────────────────────────────────
    run_dataflow = DataflowTemplatedJobStartOperator(
        task_id="run_dataflow",
        # GCS path to the staged Classic Template (no .json extension needed)
        template=TEMPLATE_GCS_PATH,
        # Unique job name per window: ic-pipeline-20260423-0300
        job_name="ic-pipeline-{{ ti.xcom_pull(task_ids='fetch_window', key='window_start')"
                 "| replace(':', '-') | replace('T', '-') | replace('Z', '') | replace(' ', '-') }}",
        project_id=PROJECT_ID,
        location=REGION,
        parameters=_dataflow_parameters(),
        dataflow_default_options=_dataflow_options(),
        # Block until the Dataflow job reaches a terminal state.
        # Combined with max_active_runs=1 this ensures sequential windows.
        wait_until_finished=True,
        gcp_conn_id="google_cloud_default",
    )

    # ── 4a. Mark DONE on success ──────────────────────────────────────────────
    mark_done = BigQueryInsertJobOperator(
        task_id="mark_done",
        configuration=_bq_query_config(_update_status_query("DONE")),
        project_id=PROJECT_ID,
        gcp_conn_id="google_cloud_default",
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    # ── 4b. Mark FAILED if Dataflow job fails ─────────────────────────────────
    mark_failed = BigQueryInsertJobOperator(
        task_id="mark_failed",
        configuration=_bq_query_config(_update_status_query("FAILED")),
        project_id=PROJECT_ID,
        gcp_conn_id="google_cloud_default",
        trigger_rule=TriggerRule.ONE_FAILED,
    )

    # ── Dependency chain ──────────────────────────────────────────────────────
    fetch_window_task >> mark_running >> run_dataflow >> [mark_done, mark_failed]
