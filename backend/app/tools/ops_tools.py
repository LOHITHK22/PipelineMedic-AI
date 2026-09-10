"""Operational tools: approval requests, safe config patches, validation, rollback."""
from __future__ import annotations

import json
from pathlib import Path

from pydantic import BaseModel

from app.models.schemas import ToolRiskLevel
from app.tools.base import BaseTool

_CONFIG_STATE_PATH = Path("/tmp/pipelinemedic_config_state.json")
if not _CONFIG_STATE_PATH.parent.exists():
    _CONFIG_STATE_PATH = Path(__file__).resolve().parents[3] / ".runtime_config_state.json"


def _read_state() -> dict:
    if _CONFIG_STATE_PATH.exists():
        return json.loads(_CONFIG_STATE_PATH.read_text())
    return {}


def _write_state(state: dict):
    _CONFIG_STATE_PATH.write_text(json.dumps(state, indent=2))


class RequestHumanApprovalInput(BaseModel):
    reason: str


class RequestHumanApprovalOutput(BaseModel):
    status: str


class RequestHumanApprovalTool(BaseTool):
    """This tool exists in the catalog for LLM-plan completeness; the actual
    persisted approval workflow is driven by app/services/approval_service.py
    and the /incidents/{id}/approve|reject API -- not by the agent calling
    this tool directly (a human cannot be "called" like an API)."""

    name = "request_human_approval"
    risk_level = ToolRiskLevel.LOW
    input_model = RequestHumanApprovalInput
    output_model = RequestHumanApprovalOutput

    def _execute(self, tool_input: RequestHumanApprovalInput) -> RequestHumanApprovalOutput:
        return RequestHumanApprovalOutput(status="ESCALATION_NOTED")


class ApplySafeConfigPatchInput(BaseModel):
    component: str
    patch: dict


class ApplySafeConfigPatchOutput(BaseModel):
    component: str
    applied: bool
    previous_state: dict | None = None


class ApplySafeConfigPatchTool(BaseTool):
    """Applies a whitelisted, structurally-validated config patch to a named
    component's config store (a simple JSON state file standing in for a
    real config service / feature-flag system). Never accepts arbitrary
    shell/SQL -- only a `patch` dict of known-safe keys is applied, and the
    previous state is captured so rollback_change can restore it exactly."""

    name = "apply_safe_config_patch"
    risk_level = ToolRiskLevel.MEDIUM
    input_model = ApplySafeConfigPatchInput
    output_model = ApplySafeConfigPatchOutput

    ALLOWED_KEYS = {"field_mapping", "parallelism", "quarantine_enabled"}

    def _execute(self, tool_input: ApplySafeConfigPatchInput) -> ApplySafeConfigPatchOutput:
        disallowed = set(tool_input.patch.keys()) - self.ALLOWED_KEYS
        if disallowed:
            raise ValueError(f"Patch contains disallowed keys: {disallowed}")

        state = _read_state()
        previous = dict(state.get(tool_input.component, {}))
        state.setdefault(tool_input.component, {})
        state[tool_input.component].update(tool_input.patch)
        _write_state(state)
        return ApplySafeConfigPatchOutput(component=tool_input.component, applied=True, previous_state=previous)


class RollbackChangeInput(BaseModel):
    component: str


class RollbackChangeOutput(BaseModel):
    component: str
    rolled_back: bool
    restored_state: dict | None = None


class RollbackChangeTool(BaseTool):
    name = "rollback_change"
    risk_level = ToolRiskLevel.MEDIUM
    input_model = RollbackChangeInput
    output_model = RollbackChangeOutput

    def _execute(self, tool_input: RollbackChangeInput) -> RollbackChangeOutput:
        state = _read_state()
        if tool_input.component not in state:
            return RollbackChangeOutput(component=tool_input.component, rolled_back=False)
        del state[tool_input.component]
        _write_state(state)
        return RollbackChangeOutput(component=tool_input.component, rolled_back=True, restored_state={})


class ValidatePipelineHealthInput(BaseModel):
    topic: str = "orders.raw"
    group_id: str = "pipelinemedic-backend"
    dag_id: str | None = None


class ValidatePipelineHealthOutput(BaseModel):
    healthy: bool
    checks: dict


class ValidatePipelineHealthTool(BaseTool):
    """Independently re-measures pipeline state -- never trusts a prior tool's
    return code. Used exclusively by the validation agent step."""

    name = "validate_pipeline_health"
    risk_level = ToolRiskLevel.LOW
    input_model = ValidatePipelineHealthInput
    output_model = ValidatePipelineHealthOutput

    def _execute(self, tool_input: ValidatePipelineHealthInput) -> ValidatePipelineHealthOutput:
        from app.tools.kafka_tools import GetKafkaConsumerLagTool, GetKafkaLagInput
        from app.config import settings

        lag_result = GetKafkaConsumerLagTool().run(
            {"topic": tool_input.topic, "group_id": tool_input.group_id}
        )
        checks = {
            "consumer_lag_ok": lag_result.error is None and lag_result.total_lag < settings.lag_warning_threshold,
            "measured_lag": lag_result.total_lag,
            "lag_error": lag_result.error,
        }
        healthy = bool(checks["consumer_lag_ok"])
        return ValidatePipelineHealthOutput(healthy=healthy, checks=checks)
