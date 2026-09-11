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
from app.agents.cost_tracking import estimate_tokens, find_recent_duplicate, record_llm_invocation
from app.agents.memory import find_similar_resolved_incidents, remember_resolved_incident
from app.agents.state import AgentNode, AgentState
from app.db.base import SessionLocal
from app.db.models import (
    ApprovalDecision, ApprovalRequest, AuditLog, Incident as IncidentRow, IncidentEvent,
    IncidentStatus, RepairExecution, RepairPlan as RepairPlanRow, RiskLevel as DBRiskLevel, ValidationResult,
)
from app.llm.provider import get_llm_provider
from app.models.schemas import AutonomyDecision, DiagnosisResult, Incident, RepairPlan, RiskLevel
from app.observability.metrics import (
    AGENT_DIAGNOSIS_DURATION, CANARY_STARTED_TOTAL, CANARY_VALIDATION_FAILED_TOTAL,
    CANARY_VALIDATION_PASSED_TOTAL, LLM_ESTIMATED_TOKENS_TOTAL, LLM_INVOCATIONS_SKIPPED_TOTAL,
    LLM_INVOCATIONS_TOTAL, PIPELINE_INCIDENTS_TOTAL, PIPELINE_REPAIRS_FAILED_TOTAL,
    PIPELINE_REPAIRS_SUCCESS_TOTAL, PIPELINE_REPAIRS_TOTAL,
)
from app.policies.engine import assess_risk
from app.services.approval_tokens import generate_approval_token
from app.services.notifications import get_notification_service
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


def _notify(db, incident_id, correlation_id, notify_fn, payload_summary):
    """Call `notify_fn()` (a zero-arg closure over the concrete notifier
    call) and record a NOTIFICATION_SENT incident_event + audit_log entry,
    consistent with how every other lifecycle step is tracked. Notification
    failures are logged but never raised -- a broken notifier must not break
    the agent graph."""
    try:
        notify_fn()
        sent = True
        error = None
    except Exception as e:  # pragma: no cover - defensive; notifiers already catch their own errors
        logger.error("Notification failed for incident %s: %s", incident_id, e)
        sent = False
        error = str(e)

    _audit(db, correlation_id, incident_id, "system", "notification_sent", {**payload_summary, "sent": sent, "error": error})
    _event(db, incident_id, "NOTIFICATION_SENT", {**payload_summary, "sent": sent, "error": error})


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

        notifier = get_notification_service()
        incident_payload = {
            "id": row.id, "title": row.title, "incident_type": row.incident_type,
            "severity": row.severity, "source_component": row.source_component,
            "description": row.description,
        }
        _notify(
            db, row.id, incident.correlation_id,
            lambda: notifier.notify_incident_detected(incident_payload),
            {"notification_type": "INCIDENT_DETECTED", "channel": type(notifier).__name__},
        )
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

        # Cost protection: if an incident with the exact same evidence
        # signature (detector type + component + evidence fingerprint) was
        # already diagnosed very recently, reuse that diagnosis/plan instead
        # of invoking the LLM again. Guards against duplicate incidents or
        # retries burning LLM calls without ever skipping diagnosis for a
        # genuinely new incident (see app.agents.cost_tracking).
        provider = get_llm_provider()
        provider_name = type(provider).__name__
        duplicate_row = find_recent_duplicate(db, incident)

        if duplicate_row is not None:
            diagnosis = DiagnosisResult(**duplicate_row.diagnosis)
            dup_plan_row = (
                db.query(RepairPlanRow)
                .filter_by(incident_id=duplicate_row.id)
                .order_by(RepairPlanRow.created_at.desc())
                .first()
            )
            plan = RepairPlan(**dup_plan_row.plan_json)
            record_llm_invocation(
                db, row.id, incident.correlation_id, provider_name, "diagnose",
                estimated_tokens=0, skipped_dedup=True, reused_from_incident_id=duplicate_row.id,
            )
            record_llm_invocation(
                db, row.id, incident.correlation_id, provider_name, "generate_repair_plan",
                estimated_tokens=0, skipped_dedup=True, reused_from_incident_id=duplicate_row.id,
            )
            LLM_INVOCATIONS_SKIPPED_TOTAL.labels(call_type="diagnose").inc()
            LLM_INVOCATIONS_SKIPPED_TOTAL.labels(call_type="generate_repair_plan").inc()
            _audit(db, incident.correlation_id, row.id, "system", "llm_call_deduplicated", {
                "reused_from_incident_id": duplicate_row.id,
            })
            _event(db, row.id, "DIAGNOSIS_CREATED", diagnosis.model_dump())
            _event(db, row.id, "REPAIR_PLAN_CREATED", plan.model_dump())
            row.diagnosis = diagnosis.model_dump()
            db.commit()
        else:
            # diagnose
            start = time.time()
            diagnosis = provider.diagnose(incident, context, similar_incidents=similar_incidents)
            AGENT_DIAGNOSIS_DURATION.observe(time.time() - start)
            diag_tokens = estimate_tokens(incident.model_dump_json() + str(context))
            record_llm_invocation(db, row.id, incident.correlation_id, provider_name, "diagnose", diag_tokens)
            LLM_INVOCATIONS_TOTAL.labels(call_type="diagnose").inc()
            LLM_ESTIMATED_TOKENS_TOTAL.labels(call_type="diagnose").inc(diag_tokens)
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
            plan_tokens = estimate_tokens(diagnosis.model_dump_json() + str(context))
            record_llm_invocation(db, row.id, incident.correlation_id, provider_name, "generate_repair_plan", plan_tokens)
            LLM_INVOCATIONS_TOTAL.labels(call_type="generate_repair_plan").inc()
            LLM_ESTIMATED_TOKENS_TOTAL.labels(call_type="generate_repair_plan").inc(plan_tokens)
            plan.incident_id = row.id
            _audit(db, incident.correlation_id, row.id, "agent", "repair_plan_generated", plan.model_dump())
            _event(db, row.id, "REPAIR_PLAN_CREATED", plan.model_dump())
            if plan.informed_by_memory:
                _audit(db, incident.correlation_id, row.id, "system", "repair_plan_informed_by_memory", {
                    "similar_past_incidents": plan.similar_past_incidents,
                })
            db.commit()

        plan.incident_id = row.id

        # calculate_risk
        risk = assess_risk(plan, incident.severity)
        _audit(db, incident.correlation_id, row.id, "agent", "risk_calculated", risk.model_dump())

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

        # Mint a signed, single-use, time-limited approve-link token (see
        # app.services.approval_tokens) and notify with the real URL.
        token = generate_approval_token(db, row.id, plan_row.id)
        from app.config import settings as _settings
        approve_link_url = f"{_settings.dashboard_base_url}/approve-link/{token}"

        notifier = get_notification_service()
        incident_payload = {
            "id": row.id, "title": row.title, "incident_type": row.incident_type,
            "severity": row.severity, "source_component": row.source_component,
            "description": row.description,
        }
        _notify(
            db, row.id, incident.correlation_id,
            lambda: notifier.notify_approval_needed(incident_payload, approve_link_url),
            {"notification_type": "APPROVAL_REQUESTED", "channel": type(notifier).__name__},
        )
        db.commit()
    finally:
        db.close()


def approve_incident(incident_id: str, decided_by: str, reason: str | None = None, decided_via: str = "dashboard") -> dict:
    """`decided_via` records which mechanism was used to decide (e.g.
    "dashboard" or "approve-link") in the audit log for traceability. The
    underlying execute/validate path is identical regardless of mechanism --
    the approve-link decide endpoint calls this exact function rather than
    duplicating the logic."""
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
        _audit(db, row.correlation_id, incident_id, f"human:{decided_by}", "approved", {"reason": reason, "decided_via": decided_via})
        _event(db, incident_id, "HUMAN_APPROVED", {"decided_by": decided_by, "reason": reason, "decided_via": decided_via})
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


def reject_incident(incident_id: str, decided_by: str, reason: str | None = None, decided_via: str = "dashboard") -> dict:
    """See `approve_incident` for the meaning of `decided_via`."""
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
        _audit(db, row.correlation_id, incident_id, f"human:{decided_by}", "rejected", {"reason": reason, "decided_via": decided_via})
        _event(db, incident_id, "HUMAN_REJECTED", {"decided_by": decided_by, "reason": reason, "decided_via": decided_via})
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

        # Canary remediation: for MEDIUM/HIGH risk repairs, validate at a
        # restricted/canary scope BEFORE declaring the repair complete and
        # running the full validation pass, rather than committing to 100%
        # rollout immediately. LOW risk repairs (already auto-executed only
        # because they were assessed as low-risk) skip this and go straight
        # to the existing full validate -> resolve/rollback path.
        #
        # Honest scope of "canary" here (see docs/safety-model.md): our tools
        # operate on a single Kafka topic / single Flink job, not a
        # traffic-splittable fleet, so there is no infrastructure to route a
        # literal 5% of live traffic through the new code path. What we
        # *can* do honestly is apply the repair, then immediately run one
        # independent re-measurement of pipeline health (the same
        # validate_pipeline_health tool used for full validation) as a fast
        # canary check before declaring the repair complete -- if that
        # canary check fails, we roll back immediately without ever running
        # (or claiming to have run) the full validation pass. This is a
        # real gate, not a cosmetic one: a canary failure here changes the
        # outcome (ROLLED_BACK vs RESOLVED) and skips REPAIR_COMPLETED /
        # VALIDATION_STARTED entirely.
        if plan_row.risk_level in (DBRiskLevel.MEDIUM, DBRiskLevel.HIGH):
            _event(db, incident_id, "CANARY_STARTED", {
                "risk_level": plan_row.risk_level.value,
                "scope_note": "single-instance validation gate; see docs/safety-model.md for scope/limitations",
            })
            CANARY_STARTED_TOTAL.inc()
            db.commit()

            canary_validation = invoke_tool("validate_pipeline_health", {}, row.correlation_id, incident_id)
            canary_passed = bool(canary_validation.get("healthy"))
            db.add(ValidationResult(
                incident_id=incident_id, execution_id=execution.id,
                passed=canary_passed, checks={**canary_validation.get("checks", {}), "canary": True},
            ))

            if not canary_passed:
                CANARY_VALIDATION_FAILED_TOTAL.inc()
                _event(db, incident_id, "CANARY_VALIDATION_FAILED", canary_validation)
                _audit(db, row.correlation_id, incident_id, "agent", "canary_validation_failed", canary_validation)

                for action in reversed(plan_row.plan_json.get("actions", [])):
                    if action.get("rollback_tool_name"):
                        try:
                            invoke_tool(
                                action["rollback_tool_name"], action.get("rollback_input") or {},
                                row.correlation_id, incident_id,
                            )
                        except Exception as e:
                            logger.error("Canary rollback of %s failed: %s", action["rollback_tool_name"], e)
                execution.status = "ROLLED_BACK"
                row.status = IncidentStatus.ROLLED_BACK
                PIPELINE_REPAIRS_FAILED_TOTAL.inc()
                _audit(db, row.correlation_id, incident_id, "agent", "rolled_back", canary_validation)
                _event(db, incident_id, "INCIDENT_ROLLED_BACK", canary_validation)
                db.commit()
                return

            CANARY_VALIDATION_PASSED_TOTAL.inc()
            _event(db, incident_id, "CANARY_VALIDATION_PASSED", canary_validation)
            _event(db, incident_id, "CANARY_EXPANDED", {"expanded_scope": "full"})
            db.commit()

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
