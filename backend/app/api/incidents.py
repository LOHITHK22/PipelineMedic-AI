from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.orm import Session

from app.agents import graph
from app.agents.memory import find_similar_resolved_incidents
from app.db.base import get_db
from app.db.models import (
    ApprovalRequest,
    AuditLog,
    Incident as IncidentRow,
    IncidentEvent,
    IncidentStatus,
    RepairExecution,
    RepairPlan as RepairPlanRow,
    ValidationResult,
)
from app.models.schemas import Incident as IncidentSchema, IncidentType, Severity
from app.services.approval_tokens import consume_approval_token, validate_approval_token
from app.services.incident_qa import answer_incident_question

router = APIRouter()


def _serialize_incident(row: IncidentRow) -> dict:
    return {
        "id": row.id,
        "incident_type": row.incident_type,
        "severity": row.severity,
        "status": row.status.value,
        "source_component": row.source_component,
        "title": row.title,
        "description": row.description,
        "evidence": row.evidence,
        "diagnosis": row.diagnosis,
        "correlation_id": row.correlation_id,
        "created_at": row.created_at.isoformat() if row.created_at else None,
        "updated_at": row.updated_at.isoformat() if row.updated_at else None,
        "resolved_at": row.resolved_at.isoformat() if row.resolved_at else None,
    }


@router.get("/incidents")
def list_incidents(status: str | None = None, db: Session = Depends(get_db)):
    q = db.query(IncidentRow)
    if status:
        q = q.filter(IncidentRow.status == status)
    rows = q.order_by(IncidentRow.created_at.desc()).limit(200).all()
    return [_serialize_incident(r) for r in rows]


@router.get("/incidents/{incident_id}")
def get_incident(incident_id: str, db: Session = Depends(get_db)):
    row = db.query(IncidentRow).get(incident_id)
    if not row:
        raise HTTPException(404, "incident not found")
    plans = db.query(RepairPlanRow).filter_by(incident_id=incident_id).all()
    approvals = db.query(ApprovalRequest).filter_by(incident_id=incident_id).all()
    executions = db.query(RepairExecution).filter_by(incident_id=incident_id).all()
    validations = db.query(ValidationResult).filter_by(incident_id=incident_id).all()
    data = _serialize_incident(row)
    data["plans"] = [
        {"id": p.id, "plan_json": p.plan_json, "risk_level": p.risk_level.value,
         "risk_rationale": p.risk_rationale, "autonomy_decision": p.autonomy_decision}
        for p in plans
    ]
    data["approvals"] = [
        {"id": a.id, "decision": a.decision.value, "requested_at": a.requested_at.isoformat(),
         "decided_at": a.decided_at.isoformat() if a.decided_at else None, "decided_by": a.decided_by, "reason": a.reason}
        for a in approvals
    ]
    data["executions"] = [
        {"id": e.id, "status": e.status, "tool_calls": e.tool_calls, "error": e.error,
         "idempotency_key": e.idempotency_key}
        for e in executions
    ]
    data["validations"] = [
        {"id": v.id, "passed": v.passed, "checks": v.checks} for v in validations
    ]
    events = (
        db.query(IncidentEvent)
        .filter_by(incident_id=incident_id)
        .order_by(IncidentEvent.seq.asc())
        .all()
    )
    data["events"] = [
        {"id": e.id, "event_type": e.event_type, "payload": e.payload,
         "created_at": e.created_at.isoformat() if e.created_at else None}
        for e in events
    ]
    return data


@router.get("/incidents/{incident_id}/similar")
def get_similar_incidents(incident_id: str, top_n: int = 3, db: Session = Depends(get_db)):
    """Deterministic incident-memory retrieval for the given incident: past
    RESOLVED-with-passing-validation incidents ranked by structured
    similarity (incident type + component + evidence-signature Jaccard
    overlap). See app.agents.memory for the algorithm."""
    row = db.query(IncidentRow).get(incident_id)
    if not row:
        raise HTTPException(404, "incident not found")
    incident_schema = IncidentSchema(
        id=row.id,
        dedup_key=row.dedup_key,
        incident_type=IncidentType(row.incident_type),
        severity=Severity(row.severity),
        source_component=row.source_component,
        title=row.title,
        description=row.description or "",
        evidence=row.evidence or {},
        correlation_id=row.correlation_id,
    )
    matches = find_similar_resolved_incidents(db, incident_schema, top_n=top_n)
    return {"incident_id": incident_id, "similar_incidents": [m.model_dump() for m in matches]}


@router.get("/incidents/{incident_id}/correlation")
def get_incident_correlation(incident_id: str, window_seconds: int = 600, db: Session = Depends(get_db)):
    """Deterministic event correlation for the given incident: SystemEvents
    (schema bumps, synthetic deployments, config changes) and other incidents
    within `window_seconds` before this one's creation that plausibly
    triggered it. See app.agents.correlation for the matching rules."""
    row = db.query(IncidentRow).get(incident_id)
    if not row:
        raise HTTPException(404, "incident not found")
    from app.agents.correlation import find_correlated_events

    incident_schema = IncidentSchema(
        id=row.id,
        dedup_key=row.dedup_key,
        incident_type=IncidentType(row.incident_type),
        severity=Severity(row.severity),
        source_component=row.source_component,
        title=row.title,
        description=row.description or "",
        evidence=row.evidence or {},
        correlation_id=row.correlation_id,
        detected_at=row.created_at,
    )
    correlated = find_correlated_events(db, incident_schema, detected_at=row.created_at, window_seconds=window_seconds)
    return {
        "incident_id": incident_id,
        "window_seconds": window_seconds,
        "correlated_events": [c.to_dict() for c in correlated],
    }


@router.get("/metrics/llm-usage")
def get_llm_usage(db: Session = Depends(get_db)):
    """Cost-protection visibility: total LLM invocations, estimated tokens,
    and how many calls were skipped via incident-signature deduplication.
    Backed by the llm_invocations audit table -- see app.agents.cost_tracking."""
    from sqlalchemy import func

    from app.db.models import LLMInvocation

    total = db.query(func.count(LLMInvocation.id)).scalar() or 0
    skipped = db.query(func.count(LLMInvocation.id)).filter(LLMInvocation.skipped_dedup.is_(True)).scalar() or 0
    real = total - skipped
    total_tokens = db.query(func.coalesce(func.sum(LLMInvocation.estimated_tokens), 0)).filter(
        LLMInvocation.skipped_dedup.is_(False)
    ).scalar() or 0
    by_call_type = dict(
        db.query(LLMInvocation.call_type, func.count(LLMInvocation.id)).group_by(LLMInvocation.call_type).all()
    )
    return {
        "total_invocations_logged": total,
        "real_invocations": real,
        "skipped_via_dedup": skipped,
        "total_estimated_tokens": int(total_tokens),
        "by_call_type": by_call_type,
        "note": (
            "estimated_tokens is a deterministic size-based estimate (tiktoken if installed, else "
            "chars/4), not a real LLM token count -- no real LLM is invoked in mock mode."
        ),
    }


class ApprovalDecisionBody(BaseModel):
    decided_by: str = "operator"
    reason: str | None = None


@router.post("/incidents/{incident_id}/approve")
def approve(incident_id: str, body: ApprovalDecisionBody, db: Session = Depends(get_db)):
    try:
        return graph.approve_incident(incident_id, body.decided_by, body.reason)
    except (KeyError, ValueError) as e:
        raise HTTPException(400, str(e))


@router.post("/incidents/{incident_id}/reject")
def reject(incident_id: str, body: ApprovalDecisionBody, db: Session = Depends(get_db)):
    try:
        return graph.reject_incident(incident_id, body.decided_by, body.reason)
    except (KeyError, ValueError) as e:
        raise HTTPException(400, str(e))


@router.get("/approvals")
def list_approvals(pending_only: bool = True, db: Session = Depends(get_db)):
    q = db.query(ApprovalRequest)
    if pending_only:
        q = q.filter(ApprovalRequest.decision == "PENDING")
    rows = q.order_by(ApprovalRequest.requested_at.desc()).all()
    return [
        {"id": r.id, "incident_id": r.incident_id, "plan_id": r.plan_id, "decision": r.decision.value,
         "requested_at": r.requested_at.isoformat()}
        for r in rows
    ]


def _plan_summary_for(db: Session, plan_id: str) -> dict | None:
    plan_row = db.query(RepairPlanRow).get(plan_id)
    if not plan_row:
        return None
    return {
        "id": plan_row.id, "plan_json": plan_row.plan_json, "risk_level": plan_row.risk_level.value,
        "risk_rationale": plan_row.risk_rationale, "autonomy_decision": plan_row.autonomy_decision,
    }


@router.get("/approve-link/{token}")
def get_approve_link(token: str, db: Session = Depends(get_db)):
    """Validate the token (not expired, not consumed, matches a real
    incident still AWAITING_APPROVAL) and, if valid, return the incident
    summary WITHOUT approving/rejecting anything. Always returns 200 with an
    `error` field on failure so the dashboard's ApproveLink page can render
    a clear expired/used/invalid state without special-casing HTTP status
    codes -- see docs/safety-model.md for the approve-link security model.
    """
    result = validate_approval_token(db, token)
    if not result.valid:
        return {"valid": False, "error": result.error}

    row = db.query(IncidentRow).get(result.incident_id)
    if not row:
        return {"valid": False, "error": "invalid"}
    if row.status != IncidentStatus.AWAITING_APPROVAL:
        return {"valid": False, "error": "already_decided", "incident_status": row.status.value}

    plan = _plan_summary_for(db, result.plan_id)
    return {
        "valid": True,
        "incident": _serialize_incident(row),
        "plan": plan,
    }


class ApproveLinkDecisionBody(BaseModel):
    decision: str  # "approve" | "reject"
    reason: str | None = None


@router.post("/approve-link/{token}/decide")
def decide_approve_link(token: str, body: ApproveLinkDecisionBody, db: Session = Depends(get_db)):
    if body.decision not in ("approve", "reject"):
        raise HTTPException(400, "decision must be 'approve' or 'reject'")

    result = consume_approval_token(db, token)
    if not result.valid:
        db.commit()  # persist nothing changed, but keep session consistent
        raise HTTPException(400, f"token {result.error}")

    row = db.query(IncidentRow).get(result.incident_id)
    if not row or row.status != IncidentStatus.AWAITING_APPROVAL:
        db.commit()
        raise HTTPException(400, "incident is no longer awaiting approval")

    db.commit()  # persist token consumption before calling into the shared approve/reject logic

    try:
        if body.decision == "approve":
            return graph.approve_incident(result.incident_id, "approve-link", body.reason, decided_via="approve-link")
        return graph.reject_incident(result.incident_id, "approve-link", body.reason or "rejected via approve-link", decided_via="approve-link")
    except (KeyError, ValueError) as e:
        raise HTTPException(400, str(e))


class AskQuestionBody(BaseModel):
    question: str


@router.post("/incidents/{incident_id}/ask")
def ask_about_incident(incident_id: str, body: AskQuestionBody, db: Session = Depends(get_db)):
    """Deterministic natural-language Q&A over THIS incident's already-
    computed diagnosis/repair-plan/evidence. This is NOT a new agentic
    action surface: it never calls an LLM and never invokes a tool -- it
    only reads and rephrases existing diagnosis data (see
    app.services.incident_qa)."""
    if not body.question or not body.question.strip():
        raise HTTPException(400, "question must not be empty")

    row = db.query(IncidentRow).get(incident_id)
    if not row:
        raise HTTPException(404, "incident not found")

    plan_row = (
        db.query(RepairPlanRow)
        .filter_by(incident_id=incident_id)
        .order_by(RepairPlanRow.created_at.desc())
        .first()
    )
    plan = _plan_summary_for(db, plan_row.id) if plan_row else None

    answer = answer_incident_question(body.question, _serialize_incident(row), plan)
    return {"incident_id": incident_id, "question": body.question, "answer": answer}


@router.get("/audit")
def list_audit(limit: int = 200, db: Session = Depends(get_db)):
    rows = db.query(AuditLog).order_by(AuditLog.created_at.desc()).limit(limit).all()
    return [
        {
            "id": r.id,
            "correlation_id": r.correlation_id,
            "incident_id": r.incident_id,
            "actor": r.actor,
            "action": r.action,
            "details": r.details,
            "created_at": r.created_at.isoformat() if r.created_at else None,
        }
        for r in rows
    ]
