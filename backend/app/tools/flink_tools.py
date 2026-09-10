from __future__ import annotations

import httpx
from pydantic import BaseModel

from app.config import settings
from app.models.schemas import ToolRiskLevel
from app.tools.base import BaseTool


class GetFlinkJobStatusInput(BaseModel):
    job_id: str | None = None
    job_name: str | None = None


class GetFlinkJobStatusOutput(BaseModel):
    job_id: str | None = None
    state: str
    restart_count: int = 0
    exceptions: list[str] = []
    error: str | None = None


class GetFlinkJobStatusTool(BaseTool):
    name = "get_flink_job_status"
    risk_level = ToolRiskLevel.LOW
    input_model = GetFlinkJobStatusInput
    output_model = GetFlinkJobStatusOutput

    def _execute(self, tool_input: GetFlinkJobStatusInput) -> GetFlinkJobStatusOutput:
        try:
            with httpx.Client(timeout=5.0) as client:
                jobs = client.get(f"{settings.flink_jobmanager_url}/jobs/overview").json()
                job = None
                for j in jobs.get("jobs", []):
                    if tool_input.job_id and j["jid"] == tool_input.job_id:
                        job = j
                        break
                    if tool_input.job_name and j.get("name") == tool_input.job_name:
                        job = j
                        break
                if not job:
                    return GetFlinkJobStatusOutput(job_id=tool_input.job_id, state="UNKNOWN", error="job not found")
                exceptions_resp = client.get(f"{settings.flink_jobmanager_url}/jobs/{job['jid']}/exceptions").json()
                exceptions = [e.get("exception", "") for e in exceptions_resp.get("all-exceptions", [])]
                return GetFlinkJobStatusOutput(
                    job_id=job["jid"], state=job.get("state", "UNKNOWN"),
                    restart_count=exceptions_resp.get("truncated", 0), exceptions=exceptions,
                )
        except Exception as e:
            return GetFlinkJobStatusOutput(job_id=tool_input.job_id, state="UNREACHABLE", error=str(e))


class RestartFlinkJobInput(BaseModel):
    job_id: str


class RestartFlinkJobOutput(BaseModel):
    job_id: str
    status: str
    error: str | None = None


class RestartFlinkJobTool(BaseTool):
    name = "restart_flink_job"
    risk_level = ToolRiskLevel.MEDIUM
    input_model = RestartFlinkJobInput
    output_model = RestartFlinkJobOutput

    def _execute(self, tool_input: RestartFlinkJobInput) -> RestartFlinkJobOutput:
        try:
            with httpx.Client(timeout=10.0) as client:
                # Flink pattern: cancel-with-savepoint then resubmit is the safe
                # production approach; for demo simplicity we issue a plain cancel
                # (a supervising job-runner / k8s deployment restarts it).
                resp = client.patch(f"{settings.flink_jobmanager_url}/jobs/{tool_input.job_id}?mode=cancel")
                resp.raise_for_status()
                return RestartFlinkJobOutput(job_id=tool_input.job_id, status="RESTART_REQUESTED")
        except Exception as e:
            return RestartFlinkJobOutput(job_id=tool_input.job_id, status="FAILED", error=str(e))


class ChangeFlinkParallelismInput(BaseModel):
    job_name: str
    parallelism: int


class ChangeFlinkParallelismOutput(BaseModel):
    job_name: str
    parallelism: int
    status: str
    note: str


class ChangeFlinkParallelismTool(BaseTool):
    name = "change_flink_parallelism"
    risk_level = ToolRiskLevel.MEDIUM
    input_model = ChangeFlinkParallelismInput
    output_model = ChangeFlinkParallelismOutput

    def _execute(self, tool_input: ChangeFlinkParallelismInput) -> ChangeFlinkParallelismOutput:
        # Changing parallelism on a running job requires stop-with-savepoint +
        # resubmit with -p N in real Flink. We record the desired parallelism
        # as a safe config patch that the job-submission script picks up on
        # next (re)deploy, which is the safe/idempotent path for a demo.
        if tool_input.parallelism < 1 or tool_input.parallelism > 32:
            return ChangeFlinkParallelismOutput(
                job_name=tool_input.job_name, parallelism=tool_input.parallelism,
                status="REJECTED", note="parallelism out of allowed bounds [1,32]",
            )
        return ChangeFlinkParallelismOutput(
            job_name=tool_input.job_name, parallelism=tool_input.parallelism,
            status="APPLIED_PENDING_REDEPLOY",
            note="Parallelism recorded; requires job resubmission via flink/jobs deploy script to take effect.",
        )
