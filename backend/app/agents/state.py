"""Typed agent state.

We implement the agent workflow as an explicit, typed state machine rather
than depending on the `langgraph` package. Rationale (see docs/agent-design.md):
the graph topology here is a straight-line pipeline with exactly one
conditional branch (auto-execute vs. human-approval) and one loop-back
(validate -> rollback), which a ~150 line hand-rolled state machine expresses
just as clearly as a LangGraph `StateGraph` would, while avoiding a heavy
extra dependency and giving us full control over how "pause until approval"
is persisted (a row in `incidents`/`approval_requests`, not an in-memory
checkpointer) -- which is what this project actually needs for durable,
process-restart-safe human-in-the-loop pauses. The node functions, explicit
typed state, and conditional-edge pattern below are structured so that
swapping in real LangGraph later is a mechanical, low-risk change.
"""
from __future__ import annotations

from enum import Enum

from pydantic import BaseModel

from app.models.schemas import DiagnosisResult, Incident, RepairPlan, RiskAssessment


class AgentNode(str, Enum):
    RECEIVE_INCIDENT = "receive_incident"
    COLLECT_CONTEXT = "collect_context"
    DIAGNOSE = "diagnose"
    GENERATE_REPAIR_PLAN = "generate_repair_plan"
    CALCULATE_RISK = "calculate_risk"
    DECISION = "decision"
    AUTO_EXECUTE = "auto_execute"
    HUMAN_APPROVAL_WAIT = "human_approval_wait"
    EXECUTE = "execute"
    VALIDATE = "validate"
    RESOLVE = "resolve"
    ROLLBACK = "rollback"
    SUMMARIZE = "summarize"
    DONE = "done"


class AgentState(BaseModel):
    incident: Incident
    context: dict = {}
    diagnosis: DiagnosisResult | None = None
    plan: RepairPlan | None = None
    risk_assessment: RiskAssessment | None = None
    execution_id: str | None = None
    execution_result: dict | None = None
    validation_passed: bool | None = None
    rollback_performed: bool = False
    current_node: AgentNode = AgentNode.RECEIVE_INCIDENT
    summary: str | None = None
    error: str | None = None
