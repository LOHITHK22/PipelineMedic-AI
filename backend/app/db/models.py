"""SQLAlchemy ORM models for PipelineMedic AI.

We use `Base.metadata.create_all()` at startup instead of Alembic migrations for
this project. Rationale (documented in docs/architecture.md): the schema is
young, single-service-owned, and the team size (one) does not yet justify the
operational overhead of migration review gates. Alembic scaffolding is trivial
to add later (`alembic init`) once the schema stabilizes or multiple
contributors need coordinated migrations.
"""
import enum
import uuid
from datetime import datetime

from sqlalchemy import (
    Column, String, DateTime, Float, Integer, Boolean, ForeignKey, Text, Enum, JSON, Identity
)
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship

from app.db.base import Base


def gen_uuid():
    return str(uuid.uuid4())


class IncidentStatus(str, enum.Enum):
    DETECTED = "DETECTED"
    DIAGNOSING = "DIAGNOSING"
    PLAN_READY = "PLAN_READY"
    AWAITING_APPROVAL = "AWAITING_APPROVAL"
    EXECUTING = "EXECUTING"
    VALIDATING = "VALIDATING"
    RESOLVED = "RESOLVED"
    ROLLED_BACK = "ROLLED_BACK"
    FAILED = "FAILED"
    REJECTED = "REJECTED"


class RiskLevel(str, enum.Enum):
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"


class ApprovalDecision(str, enum.Enum):
    PENDING = "PENDING"
    APPROVED = "APPROVED"
    REJECTED = "REJECTED"


class Incident(Base):
    __tablename__ = "incidents"

    id = Column(UUID(as_uuid=False), primary_key=True, default=gen_uuid)
    dedup_key = Column(String(256), unique=True, nullable=False, index=True)
    incident_type = Column(String(64), nullable=False)  # SCHEMA_DRIFT, KAFKA_LAG, AIRFLOW_FAILURE, FLINK_FAILURE, DATA_QUALITY
    severity = Column(String(16), nullable=False, default="MEDIUM")
    status = Column(Enum(IncidentStatus), nullable=False, default=IncidentStatus.DETECTED)
    source_component = Column(String(64), nullable=False)
    title = Column(String(512), nullable=False)
    description = Column(Text, nullable=True)
    evidence = Column(JSON, nullable=False, default=dict)
    diagnosis = Column(JSON, nullable=True)
    correlation_id = Column(String(64), nullable=False, index=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    resolved_at = Column(DateTime, nullable=True)

    events = relationship("IncidentEvent", back_populates="incident", cascade="all, delete-orphan")
    plans = relationship("RepairPlan", back_populates="incident", cascade="all, delete-orphan")
    approvals = relationship("ApprovalRequest", back_populates="incident", cascade="all, delete-orphan")
    executions = relationship("RepairExecution", back_populates="incident", cascade="all, delete-orphan")
    validations = relationship("ValidationResult", back_populates="incident", cascade="all, delete-orphan")


class IncidentEvent(Base):
    __tablename__ = "incident_events"

    id = Column(UUID(as_uuid=False), primary_key=True, default=gen_uuid)
    # Monotonic insertion sequence, independent of `created_at`. Several
    # lifecycle events (e.g. CANARY_VALIDATION_PASSED + CANARY_EXPANDED) are
    # written and committed back-to-back within the same request, and
    # `datetime.utcnow()` has millisecond resolution -- not always enough to
    # avoid ties. `seq` is DB-assigned (a real sequence), so ordering the
    # timeline by it is always correct and matches actual write order, unlike
    # sorting by `created_at` alone.
    seq = Column(Integer, Identity(always=False), nullable=False)
    incident_id = Column(UUID(as_uuid=False), ForeignKey("incidents.id"), nullable=False)
    event_type = Column(String(64), nullable=False)
    payload = Column(JSON, nullable=False, default=dict)
    created_at = Column(DateTime, default=datetime.utcnow)

    incident = relationship("Incident", back_populates="events")


class RepairPlan(Base):
    __tablename__ = "repair_plans"

    id = Column(UUID(as_uuid=False), primary_key=True, default=gen_uuid)
    incident_id = Column(UUID(as_uuid=False), ForeignKey("incidents.id"), nullable=False)
    plan_json = Column(JSON, nullable=False)
    risk_level = Column(Enum(RiskLevel), nullable=False)
    risk_rationale = Column(Text, nullable=True)
    autonomy_decision = Column(String(32), nullable=False)  # AUTO_EXECUTE / APPROVAL_REQUIRED / BLOCKED
    created_at = Column(DateTime, default=datetime.utcnow)

    incident = relationship("Incident", back_populates="plans")


class ApprovalRequest(Base):
    __tablename__ = "approval_requests"

    id = Column(UUID(as_uuid=False), primary_key=True, default=gen_uuid)
    incident_id = Column(UUID(as_uuid=False), ForeignKey("incidents.id"), nullable=False)
    plan_id = Column(UUID(as_uuid=False), ForeignKey("repair_plans.id"), nullable=False)
    decision = Column(Enum(ApprovalDecision), nullable=False, default=ApprovalDecision.PENDING)
    requested_at = Column(DateTime, default=datetime.utcnow)
    decided_at = Column(DateTime, nullable=True)
    decided_by = Column(String(128), nullable=True)
    reason = Column(Text, nullable=True)

    incident = relationship("Incident", back_populates="approvals")


class RepairExecution(Base):
    __tablename__ = "repair_executions"

    id = Column(UUID(as_uuid=False), primary_key=True, default=gen_uuid)
    incident_id = Column(UUID(as_uuid=False), ForeignKey("incidents.id"), nullable=False)
    plan_id = Column(UUID(as_uuid=False), ForeignKey("repair_plans.id"), nullable=False)
    idempotency_key = Column(String(128), unique=True, nullable=False)
    tool_calls = Column(JSON, nullable=False, default=list)
    status = Column(String(32), nullable=False, default="PENDING")  # PENDING/SUCCESS/FAILED/ROLLED_BACK
    error = Column(Text, nullable=True)
    started_at = Column(DateTime, default=datetime.utcnow)
    finished_at = Column(DateTime, nullable=True)

    incident = relationship("Incident", back_populates="executions")


class ValidationResult(Base):
    __tablename__ = "validation_results"

    id = Column(UUID(as_uuid=False), primary_key=True, default=gen_uuid)
    incident_id = Column(UUID(as_uuid=False), ForeignKey("incidents.id"), nullable=False)
    execution_id = Column(UUID(as_uuid=False), ForeignKey("repair_executions.id"), nullable=True)
    passed = Column(Boolean, nullable=False)
    checks = Column(JSON, nullable=False, default=dict)
    created_at = Column(DateTime, default=datetime.utcnow)

    incident = relationship("Incident", back_populates="validations")


class AuditLog(Base):
    __tablename__ = "audit_log"

    id = Column(UUID(as_uuid=False), primary_key=True, default=gen_uuid)
    correlation_id = Column(String(64), nullable=False, index=True)
    incident_id = Column(UUID(as_uuid=False), nullable=True, index=True)
    actor = Column(String(64), nullable=False)  # "agent" / "human:<id>" / "system"
    action = Column(String(128), nullable=False)
    details = Column(JSON, nullable=False, default=dict)
    created_at = Column(DateTime, default=datetime.utcnow)


class IncidentMemory(Base):
    """One remembered, resolved incident + the repair that actually fixed it.

    Populated only when an incident reaches INCIDENT_RESOLVED with
    validation.passed == True (see app.agents.graph._execute_and_validate,
    app.agents.memory.remember_resolved_incident). Retrieval is a
    deterministic structured-field similarity search (see
    app.agents.memory.find_similar_resolved_incidents) -- no embeddings or
    vector DB, consistent with this project's "deterministic engineering for
    observable facts" philosophy documented in docs/agent-design.md.
    """

    __tablename__ = "incident_memory"

    id = Column(UUID(as_uuid=False), primary_key=True, default=gen_uuid)
    incident_id = Column(UUID(as_uuid=False), ForeignKey("incidents.id"), nullable=False, index=True)
    incident_type = Column(String(64), nullable=False, index=True)
    source_component = Column(String(64), nullable=False)
    evidence_signature = Column(JSON, nullable=False, default=list)  # sorted list[str] of signature tokens
    root_cause = Column(Text, nullable=False)
    repair_plan_json = Column(JSON, nullable=False)
    validated_success = Column(Boolean, nullable=False, default=False)
    created_at = Column(DateTime, default=datetime.utcnow)


class SchemaVersion(Base):
    __tablename__ = "schema_versions"

    id = Column(UUID(as_uuid=False), primary_key=True, default=gen_uuid)
    subject = Column(String(128), nullable=False, index=True)  # e.g. "orders.raw"
    version = Column(Integer, nullable=False)
    schema_json = Column(JSON, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)


class SystemEvent(Base):
    """A timestamped record of a system-level event that can plausibly have
    triggered downstream incidents -- schema version bumps, synthetic
    deployment markers from fault-injection scripts, config changes.

    This is deliberately a separate, append-only table from `incident_events`
    (which records an INCIDENT's own lifecycle): a SystemEvent may exist with
    no incident ever created from it, and correlation (see
    app.agents.correlation) reads this table to explain an incident's
    plausible trigger without ever mutating it.
    """

    __tablename__ = "system_events"

    id = Column(UUID(as_uuid=False), primary_key=True, default=gen_uuid)
    event_type = Column(String(64), nullable=False, index=True)  # SCHEMA_VERSION_CHANGE, DEPLOYMENT, CONFIG_CHANGE
    component = Column(String(128), nullable=False, index=True)  # e.g. "orders.raw", "flink-validator"
    payload = Column(JSON, nullable=False, default=dict)
    created_at = Column(DateTime, default=datetime.utcnow, index=True)


class LLMInvocation(Base):
    """Audit row for every LLM provider call attempted by the agent graph
    (diagnose / generate_repair_plan), real or deduplicated-skipped. This is
    the basis for the cost-protection metrics in app.observability.metrics
    and the GET /metrics/llm-usage endpoint -- see app.agents.cost_tracking.
    """

    __tablename__ = "llm_invocations"

    id = Column(UUID(as_uuid=False), primary_key=True, default=gen_uuid)
    incident_id = Column(UUID(as_uuid=False), ForeignKey("incidents.id"), nullable=False, index=True)
    correlation_id = Column(String(64), nullable=False, index=True)
    provider = Column(String(32), nullable=False)
    call_type = Column(String(32), nullable=False)  # diagnose | generate_repair_plan
    estimated_tokens = Column(Integer, nullable=False, default=0)
    skipped_dedup = Column(Boolean, nullable=False, default=False)
    reused_from_incident_id = Column(UUID(as_uuid=False), nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow, index=True)
