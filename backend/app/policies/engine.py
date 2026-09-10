"""Policy engine: loads autonomy_policy.yaml and turns a RepairPlan +
incident severity into a deterministic RiskAssessment.

This is the single choke point for "should this run automatically, need
approval, or be blocked" -- no scattered if-statements elsewhere.
"""
from __future__ import annotations

import os
from pathlib import Path

import yaml

from app.models.schemas import AutonomyDecision, RepairPlan, RiskAssessment, RiskLevel, Severity

_POLICY_PATH = Path(__file__).with_name("autonomy_policy.yaml")


def _load_policy() -> dict:
    with open(_POLICY_PATH, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


_POLICY = _load_policy()

_RISK_ORDER = {RiskLevel.LOW: 0, RiskLevel.MEDIUM: 1, RiskLevel.HIGH: 2, RiskLevel.CRITICAL: 3}

_SEVERITY_TO_RISK_FLOOR = {
    Severity.LOW: RiskLevel.LOW,
    Severity.MEDIUM: RiskLevel.MEDIUM,
    Severity.HIGH: RiskLevel.MEDIUM,
    Severity.CRITICAL: RiskLevel.HIGH,
}


def reload_policy():
    """Exposed for tests that want to monkeypatch the policy file."""
    global _POLICY
    _POLICY = _load_policy()


def get_tool_risk(tool_name: str) -> RiskLevel:
    raw = _POLICY.get("tool_risk", {}).get(tool_name, "HIGH")
    return RiskLevel(raw)


def is_hard_blocked(tool_name: str) -> bool:
    return tool_name in _POLICY.get("hard_blocked_tools", [])


def assess_risk(plan: RepairPlan, incident_severity: Severity) -> RiskAssessment:
    """Combine: (a) the plan's own action-level tool risks, (b) the incident's
    severity floor, and (c) the LLM's self-reported risk (used only as a
    lower bound sanity signal, never trusted as authoritative) into one
    deterministic overall risk level and autonomy decision."""

    blocked_actions = [a.tool_name for a in plan.actions if is_hard_blocked(a.tool_name)]

    action_risk_levels = [get_tool_risk(a.tool_name) for a in plan.actions]
    max_action_risk = max(action_risk_levels, key=lambda r: _RISK_ORDER[r], default=RiskLevel.LOW)

    severity_floor = _SEVERITY_TO_RISK_FLOOR[incident_severity]

    overall = max([max_action_risk, severity_floor], key=lambda r: _RISK_ORDER[r])

    if blocked_actions:
        overall = RiskLevel.CRITICAL

    autonomy_raw = _POLICY.get("autonomy_by_risk", {}).get(overall.value, "APPROVAL_REQUIRED")
    autonomy_decision = AutonomyDecision(autonomy_raw)

    if blocked_actions:
        autonomy_decision = AutonomyDecision.BLOCKED

    rationale = (
        f"max tool risk={max_action_risk.value}, severity floor={severity_floor.value} "
        f"(from incident severity {incident_severity.value}) => overall={overall.value}."
    )
    if blocked_actions:
        rationale += f" HARD-BLOCKED actions present: {blocked_actions}."

    return RiskAssessment(
        risk_level=overall,
        rationale=rationale,
        autonomy_decision=autonomy_decision,
        blocked_actions=blocked_actions,
    )
