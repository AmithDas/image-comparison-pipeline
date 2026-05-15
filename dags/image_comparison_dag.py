"""
Image Comparison Pipeline — Airflow DAG  (Classic Dataflow Template)
=====================================================================
Triggers a single Dataflow job every 3 hours.

Window management is handled entirely by the Dataflow job itself using the
Dataflow service account (which already has BigQuery access for source reads):
  - WindowValueProvider lazily calls claimWindow() + getWindow() on each worker
  - A conditional UPDATE ensures only the first worker sets current_stop
  - AdvanceWindowFn calls advance() after all writes complete (via Wait.on)
  - On failure, advance is never called → next run retries the same window

The DAG is a pure scheduler — no BigQuery access is needed from the DAG SA.

All configuration lives in:
  dags/config/image_comparison_config.json

To add a new segment, append an entry to pipeline.segments in that file
and rebuild the Dataflow template (segment configs are baked into the graph
at template build time).

Classic Template — how to compile and stage
───────────────────────────────────────────
  mvn compile exec:java \\
    -Dexec.mainClass=com.yourorg.pipeline.ImageComparisonPipeline \\
    -Dexec.args="--runner=DataflowRunner \\
      --project=<PROJECT> \\
      --stagingLocation=<STAGING_LOCATION> \\
      --templateLocation=<TEMPLATE_GCS_PATH> \\
      --lookupTable=<LOOKUP_TABLE> \\
      --pipelineName=<PIPELINE_NAME> \\
      --segmentConfigs='<JSON_ARRAY>' \\
      ... (all other required options)"
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.google.cloud.operators.dataflow import (
    DataflowTemplatedJobStartOperator,
)

# ── Load config ───────────────────────────────────────────────────────────────

_CONFIG_PATH = os.path.join(os.path.dirname(__file__), "config", "image_comparison_config.json")

with open(_CONFIG_PATH) as _f:
    _CFG = json.load(_f)

_PIPE     = _CFG["pipeline"]
_DATAFLOW = _CFG["dataflow"]

PROJECT_ID        = _CFG["project_id"]
REGION            = _CFG["region"]
TEMPLATE_GCS_PATH = _CFG["template_gcs_path"]

# Strip comment keys (starting with '_') before passing segmentConfigs to Java.
_SEGMENT_CONFIGS_JSON = json.dumps([
    {k: v for k, v in seg.items() if not k.startswith("_")}
    for seg in _PIPE["segments"]
])


# ── Dataflow parameters ───────────────────────────────────────────────────────

def _dataflow_parameters() -> dict:
    """
    Builds the Dataflow Classic Template parameter dict from config.
    Window management (claimWindow, getWindow, advance) is done inside the
    Dataflow job via WindowValueProvider and AdvanceWindowFn — no window
    parameters are needed here.
    """
    params = {
        "lookupTable":          _PIPE["lookup_table"],
        "pipelineName":         _PIPE["pipeline_name"],
        "aiSourceTable":        _PIPE["ai_source_table"],
        "humanSourceTable":     _PIPE["human_source_table"],
        "outputTable":          _PIPE["output_table"],
        "pendingTable":         _PIPE["pending_table"],
        "deadLetterTable":      _PIPE["dead_letter_table"],
        "pendingSnapshotTable": _PIPE["pending_snapshot_table"],
        "segmentConfigs":       _SEGMENT_CONFIGS_JSON,
        "imageNameField":       _PIPE["image_name_field"],
        "humanLookbackDays":    str(_PIPE["human_lookback_days"]),
        "queryTempDataset":     _PIPE["query_temp_dataset"],
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
        "Triggers the Dataflow image comparison job every 3 hours. "
        "Window management (claim / advance) is handled inside the Dataflow job "
        "using the Dataflow service account — no BigQuery access needed from the DAG SA. "
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

    run_dataflow = DataflowTemplatedJobStartOperator(
        task_id="run_dataflow",
        template=TEMPLATE_GCS_PATH,
        job_name="ic-pipeline-{{ ts_nodash | lower }}",
        project_id=PROJECT_ID,
        location=REGION,
        parameters=_dataflow_parameters(),
        dataflow_default_options=_dataflow_options(),
        wait_until_finished=True,
        gcp_conn_id="google_cloud_default",
    )
