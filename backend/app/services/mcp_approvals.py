"""Server-side approval verification for approval-required MCP tool calls.

The MCP server (app.mcp_server) never trusts a client's claim that an action
was approved. An approval-required tool call is authorized only by one of:

1. An operator-approved `McpActionApproval` row (source="mcp_request"),
   referenced by the `approval_id` tool argument. It must be APPROVED, not
   expired, for the same tool name, and for the same SHA-256 hash of the
   canonical validated arguments. It is consumed atomically (single use).
   Rows are created PENDING by the server when a client first attempts the
   call, and can only be decided through the operator CLI at the bottom of
   this module -- there is no MCP tool, and no MCP code path, that approves.

2. An action that is part of an incident RepairPlan the existing workflow has
   already authorized: a human-APPROVED `ApprovalRequest` (the existing
   dashboard / API / approve-link flow) or a plan the policy engine marked
   AUTO_EXECUTE, while the incident is EXECUTING/VALIDATING. The incident id
   travels in the request `_meta`; each plan action (and its rollback) can be
   used at most once, enforced by a unique `grant_key`.

Operator CLI (run from backend/, with DATABASE_URL pointing at the app DB):

    python -m app.services.mcp_approvals list
    python -m app.services.mcp_approvals approve <approval_id> --by <name> [--reason ...]
    python -m app.services.mcp_approvals reject  <approval_id> --by <name> --reason ...
"""
from __future__ import annotations

import argparse
import hashlib
import json
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta

from pydantic import BaseModel
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.config import settings
from app.db.models import (
    ApprovalDecision, ApprovalRequest, AuditLog, Incident, IncidentStatus, McpActionApproval,
    RepairPlan as RepairPlanRow,
)
from app.observability.redaction import redact

_EXECUTION_WINDOW_STATUSES = (IncidentStatus.EXECUTING, IncidentStatus.VALIDATING)


@dataclass
class AuthorizationResult:
    authorized: bool
    reason: str
    approval_id: str | None = None
    source: str | None = None


def canonical_arguments(input_model: type[BaseModel], arguments: dict) -> dict:
    """Validated arguments with defaults filled in, JSON-normalized, so that
    `{}` and the explicit defaults hash identically."""
    return input_model.model_validate(arguments).model_dump(mode="json")


def arguments_hash(tool_name: str, canonical_args: dict) -> str:
    payload = json.dumps({"tool": tool_name, "arguments": canonical_args}, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _is_uuid(value: str | None) -> bool:
    try:
        uuid.UUID(str(value))
        return True
    except (ValueError, TypeError):
        return False


def create_pending_request(
    db: Session, *, tool_name: str, canonical_args: dict, incident_id: str | None,
    correlation_id: str | None, requested_by: str,
) -> McpActionApproval:
    now = datetime.utcnow()
    row = McpActionApproval(
        source="mcp_request",
        tool_name=tool_name,
        arguments=redact(canonical_args),
        arguments_sha256=arguments_hash(tool_name, canonical_args),
        status="PENDING",
        incident_id=incident_id if _is_uuid(incident_id) else None,
        correlation_id=correlation_id,
        requested_by=requested_by,
        requested_at=now,
        expires_at=now + timedelta(seconds=settings.mcp_approval_ttl_seconds),
    )
    db.add(row)
    db.commit()
    db.refresh(row)
    return row


def consume_request_approval(db: Session, *, approval_id: str, tool_name: str, canonical_args: dict) -> AuthorizationResult:
    if not _is_uuid(approval_id):
        return AuthorizationResult(False, "approval_id not found")
    now = datetime.utcnow()
    expected_hash = arguments_hash(tool_name, canonical_args)
    # Single atomic conditional UPDATE: two concurrent calls with the same
    # approval can never both see rowcount == 1.
    updated = (
        db.query(McpActionApproval)
        .filter(
            McpActionApproval.id == approval_id,
            McpActionApproval.source == "mcp_request",
            McpActionApproval.status == "APPROVED",
            McpActionApproval.tool_name == tool_name,
            McpActionApproval.arguments_sha256 == expected_hash,
            McpActionApproval.expires_at > now,
        )
        .update({"status": "CONSUMED", "consumed_at": now}, synchronize_session=False)
    )
    db.commit()
    if updated == 1:
        return AuthorizationResult(True, "operator approval consumed", approval_id=approval_id, source="mcp_request")

    row = db.get(McpActionApproval, approval_id)
    if row is None or row.source != "mcp_request":
        reason = "approval_id not found"
    elif row.tool_name != tool_name or row.arguments_sha256 != expected_hash:
        reason = "approval does not match this tool and these exact arguments"
    elif row.status == "CONSUMED":
        reason = "approval already used"
    elif row.status != "APPROVED":
        reason = f"approval is {row.status}, not APPROVED"
    else:
        reason = "approval expired"
    return AuthorizationResult(False, reason, approval_id=approval_id)


def _authorized_plans(db: Session, incident_id: str) -> list[RepairPlanRow]:
    approved_plan_ids = {
        a.plan_id for a in db.query(ApprovalRequest).filter_by(
            incident_id=incident_id, decision=ApprovalDecision.APPROVED
        ).all()
    }
    plans = db.query(RepairPlanRow).filter_by(incident_id=incident_id).all()
    return [p for p in plans if p.id in approved_plan_ids or p.autonomy_decision == "AUTO_EXECUTE"]


def consume_incident_plan_grant(
    db: Session, *, incident_id: str, tool_name: str, canonical_args: dict,
    input_model: type[BaseModel], correlation_id: str | None, requested_by: str,
) -> AuthorizationResult:
    if not _is_uuid(incident_id):
        return AuthorizationResult(False, "incident not found")
    incident = db.get(Incident, incident_id)
    if incident is None:
        return AuthorizationResult(False, "incident not found")
    if incident.status not in _EXECUTION_WINDOW_STATUSES:
        return AuthorizationResult(False, f"incident is {incident.status.value}, not in an approved execution window")

    expected_hash = arguments_hash(tool_name, canonical_args)
    now = datetime.utcnow()
    for plan in _authorized_plans(db, incident_id):
        for i, action in enumerate(plan.plan_json.get("actions", [])):
            entries = [(f"action:{i}", action.get("tool_name"), action.get("tool_input") or {})]
            if action.get("rollback_tool_name"):
                entries.append((f"rollback:{i}", action["rollback_tool_name"], action.get("rollback_input") or {}))
            for entry_key, entry_tool, entry_input in entries:
                if entry_tool != tool_name:
                    continue
                try:
                    if arguments_hash(tool_name, canonical_arguments(input_model, entry_input)) != expected_hash:
                        continue
                except Exception:
                    continue
                grant = McpActionApproval(
                    source="incident_plan",
                    grant_key=f"incident_plan:{plan.id}:{entry_key}",
                    tool_name=tool_name,
                    arguments=redact(canonical_args),
                    arguments_sha256=expected_hash,
                    status="CONSUMED",
                    incident_id=incident_id,
                    correlation_id=correlation_id,
                    requested_by=requested_by,
                    requested_at=now,
                    expires_at=now,
                    consumed_at=now,
                )
                db.add(grant)
                try:
                    db.commit()
                except IntegrityError:
                    db.rollback()  # this plan action was already used; try the next match
                    continue
                return AuthorizationResult(True, f"authorized by incident plan {plan.id} ({entry_key})",
                                           approval_id=grant.id, source="incident_plan")
    return AuthorizationResult(False, "no unused matching action in an approved plan for this incident")


def decide(db: Session, approval_id: str, *, approve: bool, decided_by: str, reason: str | None = None) -> McpActionApproval:
    """Operator decision. Only reachable from the CLI below (or Python code
    running with direct DB access) -- never from an MCP request."""
    if not decided_by or not decided_by.strip():
        raise ValueError("decided_by is required")
    if not _is_uuid(approval_id):
        raise KeyError("approval not found")
    row = db.get(McpActionApproval, approval_id)
    if row is None or row.source != "mcp_request":
        raise KeyError("approval not found")
    now = datetime.utcnow()
    if row.status != "PENDING":
        raise ValueError(f"approval is {row.status}, not PENDING")
    if row.expires_at <= now:
        raise ValueError("approval request expired")
    row.status = "APPROVED" if approve else "REJECTED"
    row.decided_at = now
    row.decided_by = decided_by
    row.reason = reason
    if approve:
        # The approved grant is usable for one TTL window from the decision.
        row.expires_at = now + timedelta(seconds=settings.mcp_approval_ttl_seconds)
    db.add(AuditLog(
        correlation_id=row.correlation_id or "-", incident_id=row.incident_id, actor=f"human:{decided_by}",
        action="mcp_approval_decided",
        details={"approval_id": row.id, "tool_name": row.tool_name, "decision": row.status, "reason": reason},
    ))
    db.commit()
    db.refresh(row)
    return row


def _row_summary(row: McpActionApproval) -> dict:
    return {
        "id": row.id, "status": row.status, "tool_name": row.tool_name, "arguments": row.arguments,
        "incident_id": row.incident_id, "requested_by": row.requested_by,
        "requested_at": str(row.requested_at), "expires_at": str(row.expires_at),
        "decided_by": row.decided_by, "reason": row.reason,
    }


def main(argv: list[str] | None = None) -> int:
    from app.db.base import Base, SessionLocal, engine

    parser = argparse.ArgumentParser(prog="python -m app.services.mcp_approvals",
                                     description="Operator CLI for PipelineMedic MCP action approvals.")
    sub = parser.add_subparsers(dest="cmd", required=True)
    p_list = sub.add_parser("list", help="list approval requests")
    p_list.add_argument("--status", default="PENDING", help="PENDING (default), APPROVED, REJECTED, CONSUMED, or ALL")
    for name in ("approve", "reject"):
        p = sub.add_parser(name)
        p.add_argument("approval_id")
        p.add_argument("--by", required=True, help="operator identity recorded in the audit log")
        p.add_argument("--reason", required=(name == "reject"))
    args = parser.parse_args(argv)

    Base.metadata.create_all(bind=engine, tables=[McpActionApproval.__table__, AuditLog.__table__])
    db = SessionLocal()
    try:
        if args.cmd == "list":
            q = db.query(McpActionApproval).filter(McpActionApproval.source == "mcp_request")
            if args.status.upper() != "ALL":
                q = q.filter(McpActionApproval.status == args.status.upper())
            for row in q.order_by(McpActionApproval.requested_at.asc()).all():
                print(json.dumps(_row_summary(row)))
            return 0
        try:
            row = decide(db, args.approval_id, approve=(args.cmd == "approve"), decided_by=args.by, reason=args.reason)
        except (KeyError, ValueError) as e:
            print(f"error: {e.args[0] if e.args else e}")
            return 1
        print(json.dumps(_row_summary(row)))
        return 0
    finally:
        db.close()


if __name__ == "__main__":
    raise SystemExit(main())
