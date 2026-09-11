"""Unit tests for the /health/pipeline endpoint's status-derivation logic
(app/api/health.py). These mock the underlying Flink/Airflow tool calls so
they run without a live Flink JobManager or Airflow webserver -- the real
end-to-end wiring against live containers is verified manually (see
docs/architecture.md's incident-lifecycle section and the session that added
this endpoint)."""
from app.api import health as health_module
from app.tools.airflow_tools import GetAirflowDagStatusOutput
from app.tools.flink_tools import GetFlinkJobStatusOutput


def test_flink_health_running_is_healthy(monkeypatch):
    monkeypatch.setattr(
        health_module.GetFlinkJobStatusTool, "_execute",
        lambda self, tool_input: GetFlinkJobStatusOutput(job_id="j1", state="RUNNING", restart_count=0, exceptions=[]),
    )
    result = health_module._flink_health()
    assert result["status"] == "HEALTHY"
    assert result["state"] == "RUNNING"


def test_flink_health_failed_is_degraded(monkeypatch):
    monkeypatch.setattr(
        health_module.GetFlinkJobStatusTool, "_execute",
        lambda self, tool_input: GetFlinkJobStatusOutput(job_id="j1", state="FAILED", restart_count=3, exceptions=["boom"]),
    )
    result = health_module._flink_health()
    assert result["status"] == "DEGRADED"


def test_flink_health_unreachable_is_unknown(monkeypatch):
    monkeypatch.setattr(
        health_module.GetFlinkJobStatusTool, "_execute",
        lambda self, tool_input: GetFlinkJobStatusOutput(job_id=None, state="UNREACHABLE", error="connection refused"),
    )
    result = health_module._flink_health()
    assert result["status"] == "UNKNOWN"


def test_airflow_health_success_is_healthy(monkeypatch):
    monkeypatch.setattr(
        health_module.GetAirflowDagStatusTool, "_execute",
        lambda self, tool_input: GetAirflowDagStatusOutput(dag_id="d1", is_paused=False, latest_run_state="success"),
    )
    result = health_module._airflow_health()
    assert result["status"] == "HEALTHY"


def test_airflow_health_failed_run_is_degraded(monkeypatch):
    monkeypatch.setattr(
        health_module.GetAirflowDagStatusTool, "_execute",
        lambda self, tool_input: GetAirflowDagStatusOutput(dag_id="d1", is_paused=False, latest_run_state="failed"),
    )
    result = health_module._airflow_health()
    assert result["status"] == "DEGRADED"


def test_airflow_health_error_is_unknown(monkeypatch):
    monkeypatch.setattr(
        health_module.GetAirflowDagStatusTool, "_execute",
        lambda self, tool_input: GetAirflowDagStatusOutput(dag_id="d1", error="connection refused"),
    )
    result = health_module._airflow_health()
    assert result["status"] == "UNKNOWN"


def test_pipeline_health_overall_degraded_when_any_component_degraded(monkeypatch):
    monkeypatch.setattr(health_module, "_flink_health", lambda: {"status": "HEALTHY"})
    monkeypatch.setattr(health_module, "_airflow_health", lambda: {"status": "DEGRADED"})
    result = health_module.pipeline_health()
    assert result["overall"] == "DEGRADED"


def test_pipeline_health_overall_healthy_when_both_healthy(monkeypatch):
    monkeypatch.setattr(health_module, "_flink_health", lambda: {"status": "HEALTHY"})
    monkeypatch.setattr(health_module, "_airflow_health", lambda: {"status": "HEALTHY"})
    result = health_module.pipeline_health()
    assert result["overall"] == "HEALTHY"


def test_pipeline_health_overall_unknown_when_neither_degraded_nor_all_healthy(monkeypatch):
    monkeypatch.setattr(health_module, "_flink_health", lambda: {"status": "HEALTHY"})
    monkeypatch.setattr(health_module, "_airflow_health", lambda: {"status": "UNKNOWN"})
    result = health_module.pipeline_health()
    assert result["overall"] == "UNKNOWN"
