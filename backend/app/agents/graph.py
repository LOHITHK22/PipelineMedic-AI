"""The PipelineMedic agent 'graph' -- an explicit, persisted state machine.

Flow:
  receive_incident -> collect_context -> diagnose -> generate_repair_plan
  -> calculate_risk -> decision
        -> AUTO_EXECUTE:        execute -> validate -> resolve | rollback
        -> APPROVAL_REQUIRED:   [PERSIST + PAUSE] -- resumed by approval_service
        -> BLOCKED:             summarize (no execution) -> done
  -> summarize -> done

Durability: the pause point is a real Postgres row (Incident.status =
AWAITING_APPROVAL + ApprovalRequest.decision = PENDING). If the process
restarts, `resume_from_approval` reconstructs enough state from the DB to
continue -- there is no in-memory-only checkpoint that could be lost.
"""
from __future__ import annotations

import logging
import time
import uuid

from app.agents.context_collector import collect_context
from app.agents.memory import find_similar_resolved_incidents, remember_resolved_incident
from app.agents.state import AgentNode, AgentState
from app.db.base import SessionLocal
from app.db.models import (
    ApprovalDecision, ApprovalRequest, AuditLog, Incident as IncidentRow, IncidentEvent,
    IncidentStatus, RepairExecution, RiskLevel as DBRiskLevel, ValidationResult,
)
from app.llm.provider import get_llm_provider
from app.models.schemas import AutonomyDecision, Incident, RiskLevel
from app.observability.metrics import (
    AGENT_DIAGNOSIS_DURATION, PIPELINE_INCIDENTS_TOTAL, PIPELINE_REPAIRS_FAILED_TOTAL,
    PIPELINE_REPAIRS_SUCCESS_TOTAL, PIPELINE_REPAIRS_TOTAL,
)
from app.policies.engine import assess_risk
from app.tools.base import ToolPermissionError
from app.tools.registry import invoke_tool

logger = logging.getLogger("pipelinemedic.agent")


def _audit(db, correlation_id, incident_id, actor, action, details):
    db.add(AuditLog(correlation_id=correlation_id, incident_id=incident_id, actor=actor, action=action, details=details))


def _event(db, incident_id, event_type, payload=None):
    """Write a canonical incident_events row for the dashboard timeline.

    This is the single, centrally-enforced place lifecycle events are recorded.
    Every detector type funnels through the same graph functions below, so a
    future detector automatically gets a populated timeline without having to
    remember to call this itself -- unlike `_audit`, which is a free-form
    engineering log, `_event` is reserved for the fixed set of real lifecycle
    transitions (INCIDENT_DETECTED, CONTEXT_COLLECTED, DIAGNOSIS_CREATED, ...).
    """
    db.add(IncidentEvent(incident_id=incident_id, event_type=event_type, payload=payload or {}))


def receive_incident(incident: Incident) -> str:
    """Persist a new incident (or return existing incident id if this is a
    duplicate per dedup_key), then run the graph up to the decision point."""
    db = SessionLocal()
    try:
        existing = db.query(IncidentRow).filter_by(dedup_key=incident.dedup_key).first()
        if existing and existing.status not in (IncidentStatus.RESOLVED, IncidentStatus.ROLLED_BACK, IncidentStatus.REJECTED, IncidentStatus.FAILED):
            logger.info("Deduplicated incident %s (existing %s)", incident.dedup_key, existing.id)
            return existing.id

        row = IncidentRow(
            dedup_key=incident.dedup_key,
            incident_type=incident.incident_type.value,
            severity=incident.severity.value,
            status=IncidentStatus.DETECTED,
            source_component=incident.source_component,
            title=incident.title,
            description=incident.description,
            evidence=incident.evidence,
            correlation_id=incident.correlation_id,
        )
        db.add(row)
        db.commit()
        db.refresh(row)
        incident.id = row.id
        PIPELINE_INCIDENTS_TOTAL.labels(incident_type=incident.incident_type.value).inc()
        _audit(db, incident.correlation_id, row.id, "system", "incident_received", {"dedup_key": incident.dedup_key})
        _event(db, row.id, "INCIDENT_DETECTED", {
            "incident_type": incident.incident_type.value,
            "severity": incident.severity.value,
            "source_component": incident.source_component,
            "title": incident.title,
        })
        db.commit()
    finally:
        db.close()

    _run_until_decision(incident)
    return incident.id


def _run_until_decision(incident: Incident):
    db = SessionLocal()
    try:
        row = db.query(IncidentRow).get(incident.id)
        row.status = IncidentStatus.DIAGNOSING
        db.commit()

        # collect_context
        context = collect_context(incident)
        _event(db, row.id, "CONTEXT_COLLECTED", {"context_keys": list(context.keys())})
        db.commit()

        # memory retrieval: deterministic structured similarity search over
        # past, validated-successful incidents of the same type (see
        # app.agents.memory). Passed into diagnose/generate_repair_plan as
        # context only -- it never substitutes for fresh reasoning, risk
        # scoring, or validation (see docs/agent-design.md).
        similar_incidents = find_similar_resolved_incidents(db, incident, top_n=3)
        if similar_incidents:
            _event(db, row.id, "MEMORY_RETRIEVED", {
                "count": len(similar_incidents),
                "matches": [m.model_dump() for m in similar_incidents],
            })
            db.commit()

        # diagnose
        provider = get_llm_provider()
        start = time.time()
        diagnosis = provider.diagnose(incident, context, similar_incidents=similar_incidents)
        AGENT_DIAGNOSIS_DURATION.observe(time.time() - start)
        row.diagnosis = diagnosis.model_dump()
        _audit(db, incident.correlation_id, row.id, "agent", "diagnosis_complete", diagnosis.model_dump())
        _event(db, row.id, "DIAGNOSIS_CREATED", diagnosis.model_dump())
        if diagnosis.informed_by_memory:
            _audit(db, incident.correlation_id, row.id, "system", "diagnosis_informed_by_memory", {
                "similar_past_incidents": diagnosis.similar_past_incidents,
            })
        db.commit()

        # generate_repair_plan
        plan = provider.generate_repair_plan(incident, diagnosis, context, similar_incidents=similar_incidents)
        plan.incident_id = row.id
        _audit(db, incident.correlation_id, row.id, "agent", "repair_plan_generated", plan.model_dump())
        _event(db, row.id, "REPAIR_PLAN_CREATED", plan.model_dump())
        if plan.informed_by_memory:
            _audit(db, incident.correlation_id, row.id, "system", "repair_plan_informed_by_memory", {
                "similar_past_incidents": plan.similar_past_incidents,
            })
        db.commit()

        # calculate_risk
        risk = assess_risk(plan, incident.severity)
        _audit(db, incident.correlation_id, row.id, "agent", "risk_calculated", risk.model_dump())

        from app.db.models import RepairPlan as RepairPlanRow

        plan_row = RepairPlanRow(
            incident_id=row.id, plan_json=plan.model_dump(), risk_level=DBRiskLevel(risk.risk_level.value),
            risk_rationale=risk.rationale, autonomy_decision=risk.autonomy_decision.value,
        )
        db.add(plan_row)
        row.status = IncidentStatus.PLAN_READY
        db.commit()
        db.refresh(plan_row)

        # decision
        if risk.autonomy_decision == AutonomyDecision.BLOCKED:
            row.status = IncidentStatus.FAILED
            row.description = (row.description or "") + f"\n[BLOCKED] {risk.rationale}"
            _audit(db, incident.correlation_id, row.id, "system", "blocked", {"rationale": risk.rationale})
            _event(db, row.id, "INCIDENT_BLOCKED", {"rationale": risk.rationale})
            db.commit()
            return

        if risk.autonomy_decision == AutonomyDecision.AUTO_EXECUTE:
            db.commit()
            _execute_and_validate(incident.id, plan_row.id)
            return

        # APPROVAL_REQUIRED: persist and pause
        approval = ApprovalRequest(incident_id=row.id, plan_id=plan_row.id, decision=ApprovalDecision.PENDING)
        db.add(approval)
        row.status = IncidentStatus.AWAITING_APPROVAL
        _audit(db, incident.correlation_id, row.id, "system", "awaiting_approval", {"plan_id": plan_row.id})
        _event(db, row.id, "APPROVAL_REQUESTED", {"plan_id": plan_row.id, "risk_level": risk.risk_level.value})
        db.commit()
    finally:
        db.close()


def approve_incident(incident_id: str, decided_by: str, reason: str | None = None) -> dict:
    db = SessionLocal()
    try:
        row = db.query(IncidentRow).get(incident_id)
        if not row:
            raise KeyError("incident not found")
        approval = (
            db.query(ApprovalRequest)
            .filter_by(incident_id=incident_id, decision=ApprovalDecision.PENDING)
            .order_by(ApprovalRequest.requested_at.desc())
            .first()
        )
        if not approval:
            raise ValueError("no pending approval for this incident")
        approval.decision = ApprovalDecision.APPROVED
        from datetime import datetime
        approval.decided_at = datetime.utcnow()
        approval.decided_by = decided_by
        approval.reason = reason
        plan_id = approval.plan_id
        _audit(db, row.correlation_id, incident_id, f"human:{decided_by}", "approved", {"reason": reason})
        _event(db, incident_id, "HUMAN_APPROVED", {"decided_by": decided_by, "reason": reason})
        db.commit()
    finally:
        db.close()

    _execute_and_validate(incident_id, plan_id)
    db = SessionLocal()
    try:
        row = db.query(IncidentRow).get(incident_id)
        return {"incident_id": incident_id, "status": row.status.value}
    finally:
        db.close()


def reject_incident(incident_id: str, decided_by: str, reason: str | None = None) -> dict:
    db = SessionLocal()
    try:
        row = db.query(IncidentRow).get(incident_id)
        if not row:
            raise KeyError("incident not found")
        approval = (
            db.query(ApprovalRequest)
            .filter_by(incident_id=incident_id, decision=ApprovalDecision.PENDING)
            .order_by(ApprovalRequest.requested_at.desc())
            .first()
        )
        if not approval:
            raise ValueError("no pending approval for this incident")
        approval.decision = ApprovalDecision.REJECTED
        from datetime import datetime
        approval.decided_at = datetime.utcnow()
        approval.decided_by = decided_by
        approval.reason = reason
        row.status = IncidentStatus.REJECTED
        _audit(db, row.correlation_id, incident_id, f"human:{decided_by}", "rejected", {"reason": reason})
        _event(db, incident_id, "HUMAN_REJECTED", {"decided_by": decided_by, "reason": reason})
        db.commit()
        return {"incident_id": incident_id, "status": row.status.value}
    finally:
        db.close()


def _execute_and_validate(incident_id: str, plan_id: str):
    db = SessionLocal()
    try:
        row = db.query(IncidentRow).get(incident_id)
        from app.db.models import RepairPlan as RepairPlanRow
        plan_row = db.query(RepairPlanRow).get(plan_id)

        # Idempotency: dedupe by incident_id so retries/duplicate approvals never double-execute.
        idempotency_key = f"exec:{incident_id}"
        existing_exec = db.query(RepairExecution).filter_by(idempotency_key=idempotency_key).first()
        if existing_exec and existing_exec.status == "SUCCESS":
            logger.info("Execution already completed for incident %s; skipping (idempotent).", incident_id)
            return

        row.status = IncidentStatus.EXECUTING
        _event(db, incident_id, "REPAIR_STARTED", {"plan_id": plan_id})
        db.commit()

        execution = existing_exec or RepairExecution(
            incident_id=incident_id, plan_id=plan_id, idempotency_key=idempotency_key,
            tool_calls=[], status="PENDING",
        )
        if not existing_exec:
            db.add(execution)
            db.commit()
            db.refresh(execution)

        tool_calls_log = []
        exec_error = None
        try:
            for action in plan_row.plan_json.get("actions", []):
                result = invoke_tool(
                    action["tool_name"], action["tool_input"], row.correlation_id, incident_id,
                )
                tool_calls_log.append({"tool_name": action["tool_name"], "input": action["tool_input"], "output": result})
            execution.tool_calls = tool_calls_log
            execution.status = "SUCCESS"
        except ToolPermissionError as e:
            exec_error = str(e)
            execution.status = "FAILED"
            execution.error = exec_error
        except Exception as e:
            exec_error = str(e)
            execution.status = "FAILED"
            execution.error = exec_error

        from datetime import datetime
        execution.finished_at = datetime.utcnow()
        PIPELINE_REPAIRS_TOTAL.inc()
        db.commit()

        if exec_error:
            row.status = IncidentStatus.FAILED
            PIPELINE_REPAIRS_FAILED_TOTAL.inc()
            _audit(db, row.correlation_id, incident_id, "agent", "execution_failed", {"error": exec_error})
            _event(db, incident_id, "REPAIR_FAILED", {"error": exec_error})
            db.commit()
            return

        _event(db, incident_id, "REPAIR_COMPLETED", {"tool_calls": tool_calls_log})

        row.status = IncidentStatus.VALIDATING
        _event(db, incident_id, "VALIDATION_STARTED", {})
        db.commit()

        # validate: independently re-measure state, never trust tool return codes
        validation = invoke_tool(
            "validate_pipeline_health", {}, row.correlation_id, incident_id,
        )
        validation_row = ValidationResult(
            incident_id=incident_id, execution_id=execution.id,
            passed=bool(validation.get("healthy")), checks=validation.get("checks", {}),
        )
        db.add(validation_row)

        if validation.get("healthy"):
            row.status = IncidentStatus.RESOLVED
            from datetime import datetime as dt
            row.resolved_at = dt.utcnow()
            PIPELINE_REPAIRS_SUCCESS_TOTAL.inc()
            _audit(db, row.correlation_id, incident_id, "agent", "resolved", validation)
            _event(db, incident_id, "VALIDATION_PASSED", validation)
            _event(db, incident_id, "INCIDENT_RESOLVED", {})
            # Remember this confirmed-successful fix for future similarity
            # retrieval. Only ever written on a real validation pass -- a
            # rolled-back or failed repair is never remembered as a precedent.
            remember_resolved_incident(db, row, row.diagnosis, plan_row.plan_json)
        else:
            _event(db, incident_id, "VALIDATION_FAILED", validation)
            # rollback
            for action in reversed(plan_row.plan_json.get("actions", [])):
                if action.get("rollback_tool_name"):
                    try:
                        invoke_tool(
                            action["rollback_tool_name"], action.get("rollback_input") or {},
                            row.correlation_id, incident_id,
                        )
                    except Exception as e:
                        logger.error("Rollback of %s failed: %s", action["rollback_tool_name"], e)
            execution.status = "ROLLED_BACK"
            row.status = IncidentStatus.ROLLED_BACK
            PIPELINE_REPAIRS_FAILED_TOTAL.inc()
            _audit(db, row.correlation_id, incident_id, "agent", "rolled_back", validation)
            _event(db, incident_id, "INCIDENT_ROLLED_BACK", validation)

        db.commit()
    finally:
        db.close()
