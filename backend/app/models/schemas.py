"""Typed Pydantic models shared across detectors, agents, tools, and the API.

These are the contracts that keep LLM output structured (never regex-parsed
free text) and keep tool calls type-safe.
"""
from __future__ import annotations

import enum
from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, Field


class IncidentType(str, enum.Enum):
    SCHEMA_DRIFT = "SCHEMA_DRIFT"
    KAFKA_LAG = "KAFKA_LAG"
    AIRFLOW_FAILURE = "AIRFLOW_FAILURE"
    FLINK_FAILURE = "FLINK_FAILURE"
    DATA_QUALITY = "DATA_QUALITY"
    POISON_MESSAGE = "POISON_MESSAGE"


class Severity(str, enum.Enum):
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"


class CompatibilityClass(str, enum.Enum):
    BACKWARD_COMPATIBLE = "BACKWARD_COMPATIBLE"
    FORWARD_COMPATIBLE = "FORWARD_COMPATIBLE"
    BREAKING = "BREAKING"
    UNKNOWN = "UNKNOWN"


class FieldChange(BaseModel):
    field_name: str
    change_type: Literal["ADDED", "REMOVED", "RENAMED", "TYPE_CHANGED", "NULLABLE_CHANGED"]
    old_type: str | None = None
    new_type: str | None = None
    old_nullable: bool | None = None
    new_nullable: bool | None = None
    renamed_to: str | None = None
    compatibility: CompatibilityClass


class SchemaDiffResult(BaseModel):
    subject: str
    old_version: int | None
    new_version: int | None
    changes: list[FieldChange] = Field(default_factory=list)
    overall_compatibility: CompatibilityClass


class Incident(BaseModel):
    """Standardized incident emitted by every detector."""

    id: str | None = None
    dedup_key: str
    incident_type: IncidentType
    severity: Severity
    source_component: str
    title: str
    description: str
    evidence: dict[str, Any] = Field(default_factory=dict)
    correlation_id: str
    detected_at: datetime = Field(default_factory=datetime.utcnow)


class RiskLevel(str, enum.Enum):
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"


class AutonomyDecision(str, enum.Enum):
    AUTO_EXECUTE = "AUTO_EXECUTE"
    APPROVAL_REQUIRED = "APPROVAL_REQUIRED"
    BLOCKED = "BLOCKED"


class DiagnosisResult(BaseModel):
    """Structured LLM diagnosis output. Never free text parsed by regex."""

    root_cause: str
    confidence: float = Field(ge=0.0, le=1.0)
    affected_components: list[str]
    reasoning: str
    recommended_action_summary: str


class RepairAction(BaseModel):
    """A single typed step within a repair plan, mapped 1:1 to an MCP-style tool."""

    tool_name: str
    tool_input: dict[str, Any]
    description: str
    is_reversible: bool
    rollback_tool_name: str | None = None
    rollback_input: dict[str, Any] | None = None


class RepairPlan(BaseModel):
    """Structured LLM repair plan output."""

    incident_id: str | None = None
    summary: str
    actions: list[RepairAction]
    expected_outcome: str
    estimated_risk_level: RiskLevel  # LLM's own opinion; policy engine has final say


class RiskAssessment(BaseModel):
    risk_level: RiskLevel
    rationale: str
    autonomy_decision: AutonomyDecision
    blocked_actions: list[str] = Field(default_factory=list)


class ToolRiskLevel(str, enum.Enum):
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"
