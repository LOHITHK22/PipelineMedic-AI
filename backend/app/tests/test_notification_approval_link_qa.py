"""Tests for:
- Notification firing on INCIDENT_DETECTED / AWAITING_APPROVAL (asserted via
  incident_events + audit_log, not by mocking email -- ConsoleNotifier is
  the default and needs no external service).
- Approve-link token generation/validation/single-use/expiry.
- The approve-link decide endpoint changing incident status identically to
  the existing approve endpoint.
- The incident-QA endpoint's keyword-based answer selection.
"""
import time
import uuid

from fastapi.testclient import TestClient

from app.agents import graph
from app.db.base import SessionLocal
from app.db.models import AuditLog, Incident as IncidentRow, IncidentEvent, IncidentStatus
from app.detectors.base import build_incident
from app.main import app
from app.models.schemas import IncidentType, Severity
from app.services.approval_tokens import consume_approval_token, generate_approval_token, validate_approval_token
from app.services.incident_qa import answer_incident_question
from app.tools import registry as tool_registry

client = TestClient(app)


def _force_healthy_validation(monkeypatch):
    def fake_execute(self, tool_input):
        from app.tools.ops_tools import ValidatePipelineHealthOutput
        return ValidatePipelineHealthOutput(healthy=True, checks={"forced_healthy_for_test": True})

    monkeypatch.setattr(
        tool_registry._REGISTRY["validate_pipeline_health"].__class__, "_execute", fake_execute
    )


def _make_approval_required_incident():
    """SCHEMA_DRIFT + HIGH severity reliably produces an APPROVAL_REQUIRED
    plan in this repo's policy engine (see test_e2e.py)."""
    incident = build_incident(
        incident_type=IncidentType.SCHEMA_DRIFT,
        severity=Severity.HIGH,
        source_component="kafka:orders.raw",
        title=f"schema drift {uuid.uuid4()}",
        description="breaking field removal",
        evidence={
            "subject": "orders.raw",
            "changes": [
                {"field_name": "customer_id", "change_type": "RENAMED", "renamed_to": "customerId", "compatibility": "BREAKING"},
                {"field_name": "amount", "change_type": "TYPE_CHANGED", "old_type": "number", "new_type": "string", "compatibility": "BREAKING"},
            ],
        },
        correlation_id=str(uuid.uuid4()),
        discriminator="qa-test-" + str(uuid.uuid4()),
    )
    incident_id = graph.receive_incident(incident)
    return incident_id


def test_notifications_fire_and_are_audited(clean_db, monkeypatch):
    _force_healthy_validation(monkeypatch)
    incident_id = _make_approval_required_incident()

    db = SessionLocal()
    try:
        row = db.query(IncidentRow).get(incident_id)
        assert row.status == IncidentStatus.AWAITING_APPROVAL

        events = db.query(IncidentEvent).filter_by(incident_id=incident_id, event_type="NOTIFICATION_SENT").all()
        # One for INCIDENT_DETECTED, one for APPROVAL_REQUESTED.
        assert len(events) == 2
        notif_types = {e.payload.get("notification_type") for e in events}
        assert notif_types == {"INCIDENT_DETECTED", "APPROVAL_REQUESTED"}
        assert all(e.payload.get("sent") is True for e in events)
        assert all(e.payload.get("channel") == "ConsoleNotifier" for e in events)

        audit_rows = db.query(AuditLog).filter_by(incident_id=incident_id, action="notification_sent").all()
        assert len(audit_rows) == 2
    finally:
        db.close()


def test_approval_token_generate_validate_single_use(clean_db, monkeypatch):
    _force_healthy_validation(monkeypatch)
    incident_id = _make_approval_required_incident()

    db = SessionLocal()
    try:
        from app.db.models import RepairPlan as RepairPlanRow
        plan_row = db.query(RepairPlanRow).filter_by(incident_id=incident_id).first()
        token = generate_approval_token(db, incident_id, plan_row.id)
        db.commit()

        result = validate_approval_token(db, token)
        assert result.valid
        assert result.incident_id == incident_id
        assert result.plan_id == plan_row.id

        # Validating again (read-only) should still succeed -- not yet consumed.
        result2 = validate_approval_token(db, token)
        assert result2.valid

        consumed = consume_approval_token(db, token)
        db.commit()
        assert consumed.valid

        # Reusing the same token now fails cleanly.
        reused = validate_approval_token(db, token)
        assert not reused.valid
        assert reused.error == "already_used"
    finally:
        db.close()


def test_approval_token_expiry_fails_cleanly(clean_db, monkeypatch):
    _force_healthy_validation(monkeypatch)
    incident_id = _make_approval_required_incident()

    db = SessionLocal()
    try:
        from app.db.models import RepairPlan as RepairPlanRow
        plan_row = db.query(RepairPlanRow).filter_by(incident_id=incident_id).first()
        token = generate_approval_token(db, incident_id, plan_row.id)
        db.commit()

        # Rather than waiting 30 real minutes, validate with max_age_seconds=0
        # after a tiny real sleep -- deterministically exercises the same
        # SignatureExpired branch that a real 30-minute-old token would hit.
        time.sleep(2)
        result = validate_approval_token(db, token, max_age_seconds=1)
        assert not result.valid
        assert result.error == "expired"
    finally:
        db.close()


def test_approve_link_get_and_decide_end_to_end(clean_db, monkeypatch):
    _force_healthy_validation(monkeypatch)
    incident_id = _make_approval_required_incident()

    db = SessionLocal()
    try:
        from app.db.models import RepairPlan as RepairPlanRow
        plan_row = db.query(RepairPlanRow).filter_by(incident_id=incident_id).first()
        plan_id = plan_row.id
        token = generate_approval_token(db, incident_id, plan_id)
        db.commit()
    finally:
        db.close()

    get_resp = client.get(f"/approve-link/{token}")
    assert get_resp.status_code == 200
    body = get_resp.json()
    assert body["valid"] is True
    assert body["incident"]["id"] == incident_id
    assert body["plan"]["id"] == plan_id
    assert "diagnosis" in body["incident"]

    decide_resp = client.post(f"/approve-link/{token}/decide", json={"decision": "approve"})
    assert decide_resp.status_code == 200
    assert decide_resp.json()["status"] == "RESOLVED"

    db = SessionLocal()
    try:
        row = db.query(IncidentRow).get(incident_id)
        assert row.status == IncidentStatus.RESOLVED

        audit_rows = db.query(AuditLog).filter_by(incident_id=incident_id, action="approved").all()
        assert len(audit_rows) == 1
        assert audit_rows[0].details.get("decided_via") == "approve-link"
    finally:
        db.close()

    # Reusing the same token now fails cleanly (already consumed).
    reuse_resp = client.post(f"/approve-link/{token}/decide", json={"decision": "approve"})
    assert reuse_resp.status_code == 400
    assert "already_used" in reuse_resp.json()["detail"]

    # And the GET also now reports it as no longer usable.
    get_after = client.get(f"/approve-link/{token}")
    assert get_after.json()["valid"] is False


def test_approve_link_invalid_token_returns_clean_error(clean_db):
    resp = client.get("/approve-link/not-a-real-token")
    assert resp.status_code == 200
    assert resp.json()["valid"] is False
    assert resp.json()["error"] == "invalid"


def test_ask_endpoint_keyword_selection_differs(clean_db, monkeypatch):
    _force_healthy_validation(monkeypatch)
    incident_id = _make_approval_required_incident()

    why_resp = client.post(f"/incidents/{incident_id}/ask", json={"question": "why is this broken?"})
    assert why_resp.status_code == 200
    why_answer = why_resp.json()["answer"]

    fix_resp = client.post(f"/incidents/{incident_id}/ask", json={"question": "what's the fix?"})
    assert fix_resp.status_code == 200
    fix_answer = fix_resp.json()["answer"]

    assert why_answer != fix_answer
    # "why" should lead with root cause language; "fix" should lead with the
    # proposed repair -- both grounded in the same incident's real diagnosis.
    assert "Root cause" in why_answer
    assert "Proposed fix" in fix_answer


def test_incident_qa_is_deterministic_and_grounded():
    incident = {
        "id": "abc",
        "status": "AWAITING_APPROVAL",
        "diagnosis": {
            "root_cause": "consumer lag exceeded threshold",
            "reasoning": "lag grew past 5000 messages",
            "affected_components": ["orders.raw"],
            "confidence": 0.9,
        },
    }
    plan = {
        "risk_level": "MEDIUM",
        "risk_rationale": "restarts a live consumer group",
        "autonomy_decision": "APPROVAL_REQUIRED",
        "plan_json": {
            "summary": "restart the lagging consumer group",
            "actions": [{"tool_name": "restart_consumer_group", "description": "restart it"}],
            "expected_outcome": "lag drains back below threshold",
        },
    }

    why = answer_incident_question("why did this happen", incident, plan)
    assert "consumer lag exceeded threshold" in why

    fix = answer_incident_question("how do we fix this", incident, plan)
    assert "restart the lagging consumer group" in fix

    risk = answer_incident_question("is this safe to approve", incident, plan)
    assert "MEDIUM" in risk
