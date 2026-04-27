"""
Image Comparison Pipeline — Airflow DAG  (Classic Dataflow Template)
=====================================================================
Runs one Dataflow job per segment (main, authentication, docproof, …) every
3 hours. Each job is completely independent — it has its own method pair,
output table, pending table, and pending-snapshot table.

All configuration lives in:
  dags/config/image_comparison_config.json

To add a new segment, append an entry to the "segments" array in that file.
No Python or Java changes are needed.

Lookup table contract
─────────────────────
  run_id        STRING     NOT NULL   e.g. "run-2026-04-23-03"
  window_start  TIMESTAMP  NOT NULL   inclusive start of the processing window
  window_end    TIMESTAMP  NOT NULL   exclusive end of the processing window
  status        STRING     NOT NULL   PENDING | RUNNING | DONE | FAILED

DAG run lifecycle  (per scheduled tick)
────────────────────────────────────────
  fetch_window
      │
      ├─ mark_running
      │       │
      │       ├─ run_dataflow_<segment1>  ─┐
      │       ├─ run_dataflow_<segment2>   ├─ mark_done
      │       └─ run_dataflow_<segmentN>  ─┘   (on all success)
      │                                     mark_failed
      │                                       (on any failure)

  • Segment Dataflow jobs run in parallel.
  • max_active_runs=1 prevents overlapping DAG runs.
  • wait_until_finished=True on each job ensures the DAG waits for all jobs
    to complete before marking the window DONE or FAILED.

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

_SHARED   = _CFG["shared"]
_DATAFLOW = _CFG["dataflow"]
_SEGMENTS = _CFG["segments"]

PROJECT_ID        = _CFG["project_id"]
REGION            = _CFG["region"]
TEMPLATE_GCS_PATH = _CFG["template_gcs_path"]
LOOKUP_TABLE      = _CFG["lookup_table"]

_LOOKUP_TABLE_SQL = LOOKUP_TABLE.replace(":", ".", 1)

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

def _dataflow_parameters(segment: dict) -> dict:
    """
    Builds parameters for one segment's Dataflow job.
    Shared settings come from config["shared"]; per-segment overrides come from
    the segment entry itself.
    """
    params = {
        # Shared source / infra
        "aiSourceTable":        _SHARED["ai_source_table"],
        "humanSourceTable":     _SHARED["human_source_table"],
        "deadLetterTable":      _SHARED["dead_letter_table"],
        "imageNameField":       _SHARED["image_name_field"],
        "firestoreCollection":  _SHARED["firestore_collection"],
        "kmsKeyPath":           _SHARED["kms_key_path"],
        # Window (XCom, resolved at task-run time)
        "windowStart": "{{ ti.xcom_pull(task_ids='fetch_window', key='window_start') }}",
        "windowEnd":   "{{ ti.xcom_pull(task_ids='fetch_window', key='window_end') }}",
        # Per-segment
        "aiMethod":             segment["ai_method"],
        "humanMethod":          segment["human_method"],
        "outputTable":          segment["output_table"],
        "pendingTable":         segment["pending_table"],
        "pendingSnapshotTable": segment["pending_snapshot_table"],
    }
    if _SHARED.get("human_filter_field") and _SHARED.get("human_filter_value"):
        params["humanFilterField"] = _SHARED["human_filter_field"]
        params["humanFilterValue"] = _SHARED["human_filter_value"]
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
        "Runs one Dataflow job per segment every 3 hours. "
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

    # ── 1. Fetch window ───────────────────────────────────────────────────────
    fetch_window_task = PythonOperator(
        task_id="fetch_window",
        python_callable=fetch_window,
    )

    # ── 2. Mark window as RUNNING ─────────────────────────────────────────────
    mark_running = BigQueryInsertJobOperator(
        task_id="mark_running",
        configuration=_bq_query_config(_update_status_query("RUNNING")),
        project_id=PROJECT_ID,
        gcp_conn_id="google_cloud_default",
    )

    # ── 3. One Dataflow job per segment (run in parallel) ─────────────────────
    dataflow_tasks = []
    for segment in _SEGMENTS:
        seg_name = segment["name"]
        task = DataflowTemplatedJobStartOperator(
            task_id=f"run_dataflow_{seg_name}",
            template=TEMPLATE_GCS_PATH,
            job_name=(
                f"ic-{seg_name}-"
                "{{ ti.xcom_pull(task_ids='fetch_window', key='window_start')"
                "| replace(':', '-') | replace('T', '-') | replace('Z', '') | replace(' ', '-') }}"
            ),
            project_id=PROJECT_ID,
            location=REGION,
            parameters=_dataflow_parameters(segment),
            dataflow_default_options=_dataflow_options(),
            wait_until_finished=True,
            gcp_conn_id="google_cloud_default",
        )
        dataflow_tasks.append(task)

    # ── 4a. Mark DONE when all segment jobs succeed ───────────────────────────
    mark_done = BigQueryInsertJobOperator(
        task_id="mark_done",
        configuration=_bq_query_config(_update_status_query("DONE")),
        project_id=PROJECT_ID,
        gcp_conn_id="google_cloud_default",
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    # ── 4b. Mark FAILED if any segment job fails ──────────────────────────────
    mark_failed = BigQueryInsertJobOperator(
        task_id="mark_failed",
        configuration=_bq_query_config(_update_status_query("FAILED")),
        project_id=PROJECT_ID,
        gcp_conn_id="google_cloud_default",
        trigger_rule=TriggerRule.ONE_FAILED,
    )

    # ── Dependency chain ──────────────────────────────────────────────────────
    fetch_window_task >> mark_running >> dataflow_tasks >> [mark_done, mark_failed]
