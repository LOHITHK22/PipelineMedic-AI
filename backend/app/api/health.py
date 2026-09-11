"""Real-time pipeline component health, derived from live Flink JobManager and
Airflow REST API polls (not from incident-severity heuristics).

This complements the container-level `/health` endpoint in main.py: that one
answers "is the FastAPI process up", this one answers "is the data pipeline
itself healthy" by querying the same tools the agent graph uses to diagnose
FLINK_FAILURE / AIRFLOW_FAILURE incidents.
"""
from __future__ import annotations

from fastapi import APIRouter

from app.tools.airflow_tools import GetAirflowDagStatusInput, GetAirflowDagStatusTool
from app.tools.flink_tools import GetFlinkJobStatusInput, GetFlinkJobStatusTool

router = APIRouter()

FLINK_JOB_NAME = "pipelinemedic-order-validator"
AIRFLOW_DAG_ID = "order_data_quality_dag"


def _flink_health() -> dict:
    result = GetFlinkJobStatusTool()._execute(GetFlinkJobStatusInput(job_name=FLINK_JOB_NAME))
    if result.state in ("UNREACHABLE", "UNKNOWN"):
        status = "UNKNOWN"
    elif result.state in ("RUNNING", "FINISHED"):
        status = "HEALTHY"
    else:
        # FAILED, CANCELED, RESTARTING, FAILING, etc.
        status = "DEGRADED"
    return {
        "job_id": result.job_id,
        "job_name": FLINK_JOB_NAME,
        "state": result.state,
        "restart_count": result.restart_count,
        "exceptions": result.exceptions,
        "error": result.error,
        "status": status,
    }


def _airflow_health() -> dict:
    result = GetAirflowDagStatusTool()._execute(GetAirflowDagStatusInput(dag_id=AIRFLOW_DAG_ID))
    if result.error is not None:
        status = "UNKNOWN"
    elif result.is_paused:
        status = "DEGRADED"
    elif result.latest_run_state in ("failed", "upstream_failed"):
        status = "DEGRADED"
    elif result.latest_run_state is None:
        status = "UNKNOWN"
    else:
        status = "HEALTHY"
    return {
        "dag_id": AIRFLOW_DAG_ID,
        "is_paused": result.is_paused,
        "latest_run_state": result.latest_run_state,
        "error": result.error,
        "status": status,
    }


@router.get("/health/pipeline")
def pipeline_health():
    """Live Flink + Airflow health, polled from their real REST APIs on every
    request. `overall` is HEALTHY only if both components report HEALTHY;
    UNKNOWN means the "full" profile for that component isn't running here
    (not itself a failure), and DEGRADED means a real problem was observed."""
    flink = _flink_health()
    airflow = _airflow_health()

    statuses = {flink["status"], airflow["status"]}
    if "DEGRADED" in statuses:
        overall = "DEGRADED"
    elif statuses == {"HEALTHY"}:
        overall = "HEALTHY"
    else:
        overall = "UNKNOWN"

    return {"overall": overall, "flink": flink, "airflow": airflow}
