from __future__ import annotations

import httpx
from pydantic import BaseModel

from app.config import settings
from app.models.schemas import ToolRiskLevel
from app.tools.base import BaseTool


def _auth():
    return (settings.airflow_username, settings.airflow_password)


class GetAirflowDagStatusInput(BaseModel):
    dag_id: str


class GetAirflowDagStatusOutput(BaseModel):
    dag_id: str
    is_paused: bool | None = None
    latest_run_state: str | None = None
    error: str | None = None


class GetAirflowDagStatusTool(BaseTool):
    name = "get_airflow_dag_status"
    risk_level = ToolRiskLevel.LOW
    input_model = GetAirflowDagStatusInput
    output_model = GetAirflowDagStatusOutput

    def _execute(self, tool_input: GetAirflowDagStatusInput) -> GetAirflowDagStatusOutput:
        try:
            with httpx.Client(timeout=5.0, auth=_auth()) as client:
                dag = client.get(f"{settings.airflow_base_url}/api/v1/dags/{tool_input.dag_id}").json()
                runs = client.get(
                    f"{settings.airflow_base_url}/api/v1/dags/{tool_input.dag_id}/dagRuns",
                    params={"order_by": "-execution_date", "limit": 1},
                ).json()
                latest = runs.get("dag_runs", [{}])[0].get("state") if runs.get("dag_runs") else None
                return GetAirflowDagStatusOutput(
                    dag_id=tool_input.dag_id, is_paused=dag.get("is_paused"), latest_run_state=latest
                )
        except Exception as e:
            return GetAirflowDagStatusOutput(dag_id=tool_input.dag_id, error=str(e))


class GetAirflowTaskLogsInput(BaseModel):
    dag_id: str
    task_id: str
    run_id: str
    try_number: int = 1


class GetAirflowTaskLogsOutput(BaseModel):
    logs_tail: str
    error: str | None = None


class GetAirflowTaskLogsTool(BaseTool):
    name = "get_airflow_task_logs"
    risk_level = ToolRiskLevel.LOW
    input_model = GetAirflowTaskLogsInput
    output_model = GetAirflowTaskLogsOutput

    def _execute(self, tool_input: GetAirflowTaskLogsInput) -> GetAirflowTaskLogsOutput:
        try:
            url = (
                f"{settings.airflow_base_url}/api/v1/dags/{tool_input.dag_id}/dagRuns/"
                f"{tool_input.run_id}/taskInstances/{tool_input.task_id}/logs/{tool_input.try_number}"
            )
            with httpx.Client(timeout=5.0, auth=_auth()) as client:
                resp = client.get(url)
                return GetAirflowTaskLogsOutput(logs_tail=resp.text[-4000:])
        except Exception as e:
            return GetAirflowTaskLogsOutput(logs_tail="", error=str(e))


class RerunAirflowTaskInput(BaseModel):
    dag_id: str
    task_id: str
    run_id: str


class RerunAirflowTaskOutput(BaseModel):
    status: str
    error: str | None = None


class RerunAirflowTaskTool(BaseTool):
    name = "rerun_airflow_task"
    risk_level = ToolRiskLevel.MEDIUM
    input_model = RerunAirflowTaskInput
    output_model = RerunAirflowTaskOutput

    def _execute(self, tool_input: RerunAirflowTaskInput) -> RerunAirflowTaskOutput:
        try:
            url = (
                f"{settings.airflow_base_url}/api/v1/dags/{tool_input.dag_id}/dagRuns/"
                f"{tool_input.run_id}/taskInstances/{tool_input.task_id}/clear"
            )
            with httpx.Client(timeout=10.0, auth=_auth()) as client:
                resp = client.post(url, json={"dry_run": False})
                resp.raise_for_status()
                return RerunAirflowTaskOutput(status="CLEARED_FOR_RERUN")
        except Exception as e:
            return RerunAirflowTaskOutput(status="FAILED", error=str(e))
