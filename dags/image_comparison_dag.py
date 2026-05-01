"""
Image Comparison Pipeline — Airflow DAG  (Classic Dataflow Template)
=====================================================================
Triggers a single Dataflow job every 3 hours.

The Dataflow job owns all window and status management:
  - Reads the earliest PENDING row from the lookup table
  - Marks it RUNNING, processes the window, marks DONE or FAILED
  - If no PENDING row exists, the job exits cleanly (no alert)

The DAG is a pure scheduler — no BigQuery status tasks needed here.

All configuration lives in:
  dags/config/image_comparison_config.json

To add a new segment, append an entry to pipeline.segments in that file.
No Python or Java changes are needed.

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

# The pipeline expects segmentConfigs as a JSON string — serialise the list from config.
# Strip comment keys (starting with '_') before passing to Java.
_SEGMENT_CONFIGS_JSON = json.dumps([
    {k: v for k, v in seg.items() if not k.startswith("_")}
    for seg in _PIPE["segments"]
])


# ── Dataflow parameters ───────────────────────────────────────────────────────

def _dataflow_parameters() -> dict:
    """
    Builds the Dataflow Classic Template parameter dict from config.
    windowStart / windowEnd are no longer passed here — the Dataflow job
    reads them directly from the lookup table at startup.
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
        "Window selection and status management are handled by the Dataflow job itself. "
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
