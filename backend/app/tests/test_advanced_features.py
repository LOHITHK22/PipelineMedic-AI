"""Tests for the three advanced features added on top of the base agent
graph: event correlation, canary remediation, and LLM cost protection.

Like test_e2e.py, these are real end-to-end tests against Postgres (skipped
automatically via the `clean_db` fixture if none is reachable) that exercise
the actual graph/service code paths -- not reimplementations of the logic.
"""
import uuid
from datetime import datetime, timedelta

from app.agents import graph
from app.agents.correlation import find_correlated_events
from app.agents.cost_tracking import find_recent_duplicate
from app.db.base import SessionLocal
from app.db.models import (
    Incident as IncidentRow, IncidentEvent, IncidentStatus, LLMInvocation, RepairPlan as RepairPlanRow,
    SystemEvent,
)
from app.detectors.base import build_incident
from app.models.schemas import Incident as IncidentSchema, IncidentType, Severity
from app.tools import registry as tool_registry


def _force_validate_healthy(monkeypatch, healthy: bool = True):
    def fake_execute(self, tool_input):
        from app.tools.ops_tools import ValidatePipelineHealthOutput
        return ValidatePipelineHealthOutput(healthy=healthy, checks={"forced_for_test": True})

    monkeypatch.setattr(tool_registry._REGISTRY["validate_pipeline_health"].__class__, "_execute", fake_execute)


# ---------------------------------------------------------------------------
# Feature 1: event correlation
# ---------------------------------------------------------------------------

def test_schema_version_bump_correlates_with_schema_drift_incident(clean_db):
    db = SessionLocal()
    try:
        db.add(SystemEvent(
            event_type="SCHEMA_VERSION_CHANGE",
            component="orders.raw",
            payload={"old_version": 4, "new_version": 5},
        ))
        db.commit()
    finally:
        db.close()

    incident = IncidentSchema(
        dedup_key="corr-test-" + str(uuid.uuid4()),
        incident_type=IncidentType.SCHEMA_DRIFT,
        severity=Severity.HIGH,
        source_component="orders.raw",
        title="t", description="d", evidence={"subject": "orders.raw"},
        correlation_id=str(uuid.uuid4()),
    )

    db = SessionLocal()
    try:
        correlated = find_correlated_events(db, incident)
    finally:
        db.close()

    assert len(correlated) == 1
    assert correlated[0].event_type == "SCHEMA_VERSION_CHANGE"
    assert correlated[0].seconds_before < 30
    assert "orders.raw" in correlated[0].reason or "SCHEMA_VERSION_CHANGE" in correlated[0].reason


def test_correlation_ignores_events_outside_window(clean_db):
    db = SessionLocal()
    try:
        old_event = SystemEvent(
            event_type="SCHEMA_VERSION_CHANGE", component="orders.raw", payload={},
        )
        db.add(old_event)
        db.commit()
        db.refresh(old_event)
        # Push it outside the default 10-minute window.
        old_event.created_at = datetime.utcnow() - timedelta(hours=2)
        db.commit()
    finally:
        db.close()

    incident = IncidentSchema(
        dedup_key="corr-window-" + str(uuid.uuid4()),
        incident_type=IncidentType.SCHEMA_DRIFT, severity=Severity.HIGH, source_component="orders.raw",
        title="t", description="d", evidence={}, correlation_id=str(uuid.uuid4()),
    )
    db = SessionLocal()
    try:
        correlated = find_correlated_events(db, incident, window_seconds=600)
    finally:
        db.close()
    assert correlated == []


def test_correlation_endpoint_exposes_correlated_events(clean_db, monkeypatch):
    _force_validate_healthy(monkeypatch, healthy=True)

    db = SessionLocal()
    try:
        db.add(SystemEvent(event_type="DEPLOYMENT", component="orders.raw", payload={"note": "test deploy"}))
        db.commit()
    finally:
        db.close()

    incident = build_incident(
        incident_type=IncidentType.KAFKA_LAG, severity=Severity.HIGH, source_component="orders.raw",
        title="lag", description="lag",
        evidence={"topic": "orders.raw", "group_id": "g1", "total_lag": 6000, "critical_threshold": 5000},
        correlation_id=str(uuid.uuid4()), discriminator="corr-endpoint-" + str(uuid.uuid4()),
    )
    incident_id = graph.receive_incident(incident)

    from app.api.incidents import get_incident_correlation
    db = SessionLocal()
    try:
        result = get_incident_correlation(incident_id, window_seconds=600, db=db)
    finally:
        db.close()

    assert result["incident_id"] == incident_id
    assert any(e["event_type"] == "DEPLOYMENT" for e in result["correlated_events"])


# ---------------------------------------------------------------------------
# Feature 2: canary remediation
# ---------------------------------------------------------------------------

def test_medium_risk_repair_goes_through_canary_states_then_resolves(clean_db, monkeypatch):
    _force_validate_healthy(monkeypatch, healthy=True)

    incident = build_incident(
        incident_type=IncidentType.KAFKA_LAG, severity=Severity.HIGH, source_component="kafka:orders.raw",
        title="lag", description="lag",
        evidence={"topic": "orders.raw", "group_id": "g1", "total_lag": 6000, "critical_threshold": 5000},
        correlation_id=str(uuid.uuid4()), discriminator="canary-pass-" + str(uuid.uuid4()),
    )
    incident_id = graph.receive_incident(incident)

    db = SessionLocal()
    try:
        row = db.query(IncidentRow).get(incident_id)
        assert row.status == IncidentStatus.AWAITING_APPROVAL  # change_flink_parallelism is MEDIUM risk
    finally:
        db.close()

    result = graph.approve_incident(incident_id, decided_by="test-operator", reason="canary test")
    assert result["status"] == "RESOLVED"

    db = SessionLocal()
    try:
        events = (
            db.query(IncidentEvent).filter_by(incident_id=incident_id)
            .order_by(IncidentEvent.seq.asc()).all()
        )
        event_types = [e.event_type for e in events]
    finally:
        db.close()

    assert "CANARY_STARTED" in event_types
    assert "CANARY_VALIDATION_PASSED" in event_types
    assert "CANARY_EXPANDED" in event_types
    assert event_types.index("CANARY_STARTED") < event_types.index("CANARY_VALIDATION_PASSED")
    assert event_types.index("CANARY_EXPANDED") < event_types.index("REPAIR_COMPLETED")
    assert "INCIDENT_RESOLVED" in event_types


def test_failed_canary_validation_rolls_back_without_full_validation(clean_db, monkeypatch):
    _force_validate_healthy(monkeypatch, healthy=False)

    incident = build_incident(
        incident_type=IncidentType.KAFKA_LAG, severity=Severity.HIGH, source_component="kafka:orders.raw",
        title="lag", description="lag",
        evidence={"topic": "orders.raw", "group_id": "g1", "total_lag": 6000, "critical_threshold": 5000},
        correlation_id=str(uuid.uuid4()), discriminator="canary-fail-" + str(uuid.uuid4()),
    )
    incident_id = graph.receive_incident(incident)
    result = graph.approve_incident(incident_id, decided_by="test-operator", reason="canary failure test")

    assert result["status"] == "ROLLED_BACK"

    db = SessionLocal()
    try:
        row = db.query(IncidentRow).get(incident_id)
        assert row.status == IncidentStatus.ROLLED_BACK

        events = (
            db.query(IncidentEvent).filter_by(incident_id=incident_id)
            .order_by(IncidentEvent.seq.asc()).all()
        )
        event_types = [e.event_type for e in events]
    finally:
        db.close()

    assert "CANARY_STARTED" in event_types
    assert "CANARY_VALIDATION_FAILED" in event_types
    assert "INCIDENT_ROLLED_BACK" in event_types
    # A canary failure must never proceed to claim the repair fully completed
    # or ran full validation -- that would be dishonest given it never passed
    # even the reduced canary check.
    assert "REPAIR_COMPLETED" not in event_types
    assert "CANARY_EXPANDED" not in event_types
    assert "VALIDATION_PASSED" not in event_types


# ---------------------------------------------------------------------------
# Feature 3: cost protection / LLM invocation tracking
# ---------------------------------------------------------------------------

def test_duplicate_incident_signature_skips_second_llm_call(clean_db, monkeypatch):
    _force_validate_healthy(monkeypatch, healthy=True)

    shared_evidence = {"topic": "orders.raw", "group_id": "g1", "total_lag": 6000, "critical_threshold": 5000}

    incident1 = build_incident(
        incident_type=IncidentType.KAFKA_LAG, severity=Severity.HIGH, source_component="kafka:orders.raw",
        title="lag", description="lag", evidence=shared_evidence,
        correlation_id=str(uuid.uuid4()), discriminator="cost-a-" + str(uuid.uuid4()),
    )
    incident2 = build_incident(
        incident_type=IncidentType.KAFKA_LAG, severity=Severity.HIGH, source_component="kafka:orders.raw",
        title="lag", description="lag", evidence=shared_evidence,
        correlation_id=str(uuid.uuid4()), discriminator="cost-b-" + str(uuid.uuid4()),  # different dedup_key
    )

    id1 = graph.receive_incident(incident1)
    id2 = graph.receive_incident(incident2)
    assert id1 != id2  # genuinely different incident rows (different dedup_key)

    db = SessionLocal()
    try:
        invocations = db.query(LLMInvocation).filter(
            LLMInvocation.incident_id.in_([id1, id2])
        ).all()
        by_incident = {}
        for inv in invocations:
            by_incident.setdefault(inv.incident_id, []).append(inv)
    finally:
        db.close()

    # incident1: two real calls (diagnose + generate_repair_plan)
    assert all(not inv.skipped_dedup for inv in by_incident[id1])
    assert {inv.call_type for inv in by_incident[id1]} == {"diagnose", "generate_repair_plan"}

    # incident2: same evidence signature within the dedup window -> both
    # calls skipped and reused from incident1.
    assert all(inv.skipped_dedup for inv in by_incident[id2])
    assert all(inv.reused_from_incident_id == id1 for inv in by_incident[id2])

    db = SessionLocal()
    try:
        row2 = db.query(IncidentRow).get(id2)
        assert row2.diagnosis is not None  # reused diagnosis was still recorded on the row
        plan2 = db.query(RepairPlanRow).filter_by(incident_id=id2).first()
        assert plan2 is not None  # a real plan row was still created for risk assessment/execution
    finally:
        db.close()


def test_different_evidence_does_not_get_deduplicated(clean_db):
    incident1 = build_incident(
        incident_type=IncidentType.KAFKA_LAG, severity=Severity.HIGH, source_component="kafka:orders.raw",
        title="lag", description="lag",
        evidence={"topic": "orders.raw", "group_id": "g1", "total_lag": 6000, "critical_threshold": 5000},
        correlation_id=str(uuid.uuid4()), discriminator="distinct-a-" + str(uuid.uuid4()),
    )
    incident2 = build_incident(
        incident_type=IncidentType.KAFKA_LAG, severity=Severity.HIGH, source_component="kafka:orders.raw",
        title="lag", description="lag",
        # different group_id -> different evidence signature -> must NOT be deduplicated
        evidence={"topic": "orders.raw", "group_id": "g2", "total_lag": 6000, "critical_threshold": 5000},
        correlation_id=str(uuid.uuid4()), discriminator="distinct-b-" + str(uuid.uuid4()),
    )
    id1 = graph.receive_incident(incident1)
    id2 = graph.receive_incident(incident2)

    db = SessionLocal()
    try:
        invocations2 = db.query(LLMInvocation).filter_by(incident_id=id2).all()
    finally:
        db.close()
    assert invocations2, "expected LLM invocation rows for the second, genuinely-distinct incident"
    assert all(not inv.skipped_dedup for inv in invocations2)


def test_find_recent_duplicate_requires_diagnosis_present(clean_db):
    incident = IncidentSchema(
        dedup_key="nodiag-" + str(uuid.uuid4()), incident_type=IncidentType.KAFKA_LAG, severity=Severity.HIGH,
        source_component="kafka:orders.raw", title="t", description="d",
        evidence={"topic": "orders.raw", "group_id": "g1", "total_lag": 6000, "critical_threshold": 5000},
        correlation_id=str(uuid.uuid4()),
    )
    db = SessionLocal()
    try:
        assert find_recent_duplicate(db, incident) is None
    finally:
        db.close()
