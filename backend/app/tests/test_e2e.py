"""True end-to-end test: inject schema drift evidence -> detect -> diagnose
-> approval created -> approve via the service layer (same code path the API
route uses) -> repair executes -> validate -> resolved.

Requires a reachable Postgres (DATABASE_URL). Automatically skipped if none
is available (see conftest.py) rather than failing CI environments without
Docker. Kafka-dependent tool calls degrade gracefully (return an `error`
field) when Kafka is unreachable, which is fine for this test since it
exercises orchestration logic, not live Kafka connectivity.
"""
import uuid

from app.agents import graph
from app.db.base import SessionLocal
from app.db.models import Incident as IncidentRow, IncidentStatus
from app.detectors.base import build_incident
from app.models.schemas import IncidentType, Severity
from app.tools import registry as tool_registry


def test_schema_drift_full_lifecycle(clean_db, monkeypatch):
    # Force validate_pipeline_health to report healthy, isolating this test
    # from real Kafka/broker availability so it exercises orchestration only.
    def fake_execute(self, tool_input):
        from app.tools.ops_tools import ValidatePipelineHealthOutput
        return ValidatePipelineHealthOutput(healthy=True, checks={"forced_healthy_for_test": True})

    monkeypatch.setattr(
        tool_registry._REGISTRY["validate_pipeline_health"].__class__, "_execute", fake_execute
    )

    incident = build_incident(
        incident_type=IncidentType.SCHEMA_DRIFT,
        severity=Severity.HIGH,
        source_component="kafka:orders.raw",
        title="Breaking schema drift detected on orders.raw",
        description="customer_id renamed to customerId; amount changed number->string",
        evidence={
            "subject": "orders.raw",
            "changes": [
                {"field_name": "customer_id", "change_type": "RENAMED", "renamed_to": "customerId", "compatibility": "BREAKING"},
                {"field_name": "amount", "change_type": "TYPE_CHANGED", "old_type": "number", "new_type": "string", "compatibility": "BREAKING"},
            ],
        },
        correlation_id=str(uuid.uuid4()),
        discriminator="e2e-test-" + str(uuid.uuid4()),
    )

    incident_id = graph.receive_incident(incident)
    assert incident_id is not None

    db = SessionLocal()
    try:
        row = db.query(IncidentRow).get(incident_id)
        assert row is not None
        # SCHEMA_DRIFT with breaking changes -> MEDIUM/HIGH plan risk -> APPROVAL_REQUIRED
        assert row.status == IncidentStatus.AWAITING_APPROVAL
    finally:
        db.close()

    result = graph.approve_incident(incident_id, decided_by="test-operator", reason="approved for e2e test")
    assert result["status"] == "RESOLVED"

    db = SessionLocal()
    try:
        row = db.query(IncidentRow).get(incident_id)
        assert row.status == IncidentStatus.RESOLVED
        assert row.resolved_at is not None
    finally:
        db.close()


def test_duplicate_incident_is_deduplicated(clean_db):
    disc = "dedup-test-" + str(uuid.uuid4())
    incident1 = build_incident(
        incident_type=IncidentType.KAFKA_LAG, severity=Severity.HIGH, source_component="kafka:orders.raw",
        title="lag", description="lag", evidence={"topic": "orders.raw", "group_id": "g1", "total_lag": 6000},
        correlation_id=str(uuid.uuid4()), discriminator=disc,
    )
    incident2 = build_incident(
        incident_type=IncidentType.KAFKA_LAG, severity=Severity.HIGH, source_component="kafka:orders.raw",
        title="lag", description="lag", evidence={"topic": "orders.raw", "group_id": "g1", "total_lag": 6000},
        correlation_id=str(uuid.uuid4()), discriminator=disc,
    )
    id1 = graph.receive_incident(incident1)
    id2 = graph.receive_incident(incident2)
    assert id1 == id2
