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
    RepairExecution,
    RepairPlan as RepairPlanRow,
    ValidationResult,
)
from app.models.schemas import Incident as IncidentSchema, IncidentType, Severity

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
        .order_by(IncidentEvent.created_at.asc())
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
