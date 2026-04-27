"""
Image Comparison Pipeline — Airflow DAG  (Classic Dataflow Template)
=====================================================================
Runs the image-comparison Dataflow Classic Template every 3 hours.
The processing window (windowStart / windowEnd) is driven by a BigQuery
lookup table rather than the Airflow schedule clock, so ops can adjust
windows, replay runs, or pause processing without touching the DAG code.

All pipeline settings live in:
  dags/config/image_comparison_config.json

Edit that file to change tables, methods, routes, or Dataflow options.
No Python changes are ever needed for routine configuration updates.

Lookup table contract
─────────────────────
  run_id        STRING     NOT NULL   e.g. "run-2026-04-23-03"
  window_start  TIMESTAMP  NOT NULL   inclusive start of the processing window
  window_end    TIMESTAMP  NOT NULL   exclusive end of the processing window
  status        STRING     NOT NULL   PENDING | RUNNING | DONE | FAILED

DAG run lifecycle
─────────────────
  fetch_window → mark_running → run_dataflow → mark_done
                                      ↓ (on failure)
                                   mark_failed

  • No PENDING row → task skipped; DAG run completes as SKIPPED (no alert).
  • max_active_runs=1 + wait_until_finished=True → sequential windows only.

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

import json
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.dataflow import (
    DataflowTemplatedJobStartOperator,
)
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

# ── Load config ───────────────────────────────────────────────────────────────

_CONFIG_PATH = os.path.join(os.path.dirname(__file__), "config", "image_comparison_config.json")

with open(_CONFIG_PATH) as _f:
    _CFG = json.load(_f)

_PIPE     = _CFG["pipeline"]
_DATAFLOW = _CFG["dataflow"]

# ── Derived constants ─────────────────────────────────────────────────────────

PROJECT_ID        = _CFG["project_id"]
REGION            = _CFG["region"]
TEMPLATE_GCS_PATH = _CFG["template_gcs_path"]
LOOKUP_TABLE      = _CFG["lookup_table"]

# Standard SQL format: project.dataset.table
_LOOKUP_TABLE_SQL = LOOKUP_TABLE.replace(":", ".", 1)

# route_configs is stored as a list in JSON; the Java pipeline expects a JSON string.
_ROUTE_CONFIGS_JSON = json.dumps(_PIPE["route_configs"])

# ── Task 1: fetch next pending window ────────────────────────────────────────

def fetch_window(**context) -> None:
    """
    Queries the lookup table for the earliest PENDING run and pushes
    run_id, window_start, and window_end into XCom.

    Raises AirflowSkipException when no PENDING row exists so the DAG run
    completes as SKIPPED rather than FAILED.
    """
    hook = BigQueryHook(gcp_conn_id="google_cloud_default", use_legacy_sql=False)

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


# ── BQ status update helpers ──────────────────────────────────────────────────

def _update_status_query(new_status: str) -> str:
    return f"""
        UPDATE `{_LOOKUP_TABLE_SQL}`
        SET    status = '{new_status}'
        WHERE  run_id = '{{{{ ti.xcom_pull(task_ids="fetch_window", key="run_id") }}}}'
    """


def _bq_query_config(sql: str) -> dict:
    return {"query": {"query": sql, "useLegacySql": False}}


# ── Dataflow parameters ───────────────────────────────────────────────────────

def _dataflow_parameters() -> dict:
    """
    Builds the Dataflow Classic Template parameter dict from the JSON config.
    windowStart / windowEnd are resolved at task-run time via Jinja XCom refs.
    """
    params = {
        "aiSourceTable":        _PIPE["ai_source_table"],
        "humanSourceTable":     _PIPE["human_source_table"],
        "pendingTable":         _PIPE["pending_table"],
        "deadLetterTable":      _PIPE["dead_letter_table"],
        "pendingSnapshotTable": _PIPE["pending_snapshot_table"],
        "windowStart": "{{ ti.xcom_pull(task_ids='fetch_window', key='window_start') }}",
        "windowEnd":   "{{ ti.xcom_pull(task_ids='fetch_window', key='window_end') }}",
        "routeConfigs":         _ROUTE_CONFIGS_JSON,
        "imageNameField":       _PIPE["image_name_field"],
        "firestoreCollection":  _PIPE["firestore_collection"],
        "kmsKeyPath":           _PIPE["kms_key_path"],
    }
    if _PIPE.get("human_filter_field") and _PIPE.get("human_filter_value"):
        params["humanFilterField"] = _PIPE["human_filter_field"]
        params["humanFilterValue"] = _PIPE["human_filter_value"]
    return params


def _dataflow_options() -> dict:
    return {
        "tempLocation":        _DATAFLOW["temp_location"],
        "stagingLocation":     _DATAFLOW["staging_location"],
        "serviceAccountEmail": _DATAFLOW["service_account"],
        "maxWorkers":          str(_DATAFLOW["max_workers"]),
        "ipConfiguration":     _DATAFLOW["ip_configuration"],
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
        "Config lives in dags/config/image_comparison_config.json."
    ),
    default_args=default_args,
    schedule_interval="0 */3 * * *",
    start_date=datetime(2026, 4, 23, 0, 0, 0),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=2, minutes=30),
    tags=["dataflow", "image-comparison", "data-platform"],
) as dag:

    fetch_window_task = PythonOperator(
        task_id="fetch_window",
        python_callable=fetch_window,
    )

    mark_running = BigQueryInsertJobOperator(
        task_id="mark_running",
        configuration=_bq_query_config(_update_status_query("RUNNING")),
        project_id=PROJECT_ID,
        gcp_conn_id="google_cloud_default",
    )

    run_dataflow = DataflowTemplatedJobStartOperator(
        task_id="run_dataflow",
        template=TEMPLATE_GCS_PATH,
        job_name=(
            "ic-pipeline-"
            "{{ ti.xcom_pull(task_ids='fetch_window', key='window_start')"
            "| replace(':', '-') | replace('T', '-') | replace('Z', '') | replace(' ', '-') }}"
        ),
        project_id=PROJECT_ID,
        location=REGION,
        parameters=_dataflow_parameters(),
        dataflow_default_options=_dataflow_options(),
        wait_until_finished=True,
        gcp_conn_id="google_cloud_default",
    )

    mark_done = BigQueryInsertJobOperator(
        task_id="mark_done",
        configuration=_bq_query_config(_update_status_query("DONE")),
        project_id=PROJECT_ID,
        gcp_conn_id="google_cloud_default",
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    mark_failed = BigQueryInsertJobOperator(
        task_id="mark_failed",
        configuration=_bq_query_config(_update_status_query("FAILED")),
        project_id=PROJECT_ID,
        gcp_conn_id="google_cloud_default",
        trigger_rule=TriggerRule.ONE_FAILED,
    )

    fetch_window_task >> mark_running >> run_dataflow >> [mark_done, mark_failed]
