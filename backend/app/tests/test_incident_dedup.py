import uuid

from app.agents import graph
from app.db.base import SessionLocal
from app.db.models import Incident as IncidentRow, IncidentStatus
from app.detectors.base import build_incident, make_dedup_key
from app.models.schemas import IncidentType, Severity


def test_dedup_key_deterministic():
    key1 = make_dedup_key(IncidentType.KAFKA_LAG, "kafka:orders.raw", "5000")
    key2 = make_dedup_key(IncidentType.KAFKA_LAG, "kafka:orders.raw", "5000")
    assert key1 == key2


def test_dedup_key_differs_by_discriminator():
    key1 = make_dedup_key(IncidentType.KAFKA_LAG, "kafka:orders.raw", "5000")
    key2 = make_dedup_key(IncidentType.KAFKA_LAG, "kafka:orders.raw", "9000")
    assert key1 != key2


def test_build_incident_sets_dedup_key():
    incident = build_incident(
        incident_type=IncidentType.SCHEMA_DRIFT, severity=Severity.HIGH, source_component="kafka:orders.raw",
        title="t", description="d", evidence={}, correlation_id="corr-1", discriminator="disc-1",
    )
    expected = make_dedup_key(IncidentType.SCHEMA_DRIFT, "kafka:orders.raw", "disc-1")
    assert incident.dedup_key == expected


def test_recurring_issue_after_resolution_creates_new_incident_not_a_db_error(clean_db, monkeypatch):
    """Regression test for a real bug: `incidents.dedup_key` used to carry a
    DB-level UNIQUE constraint, but the application's dedup rule
    (`agents.graph.receive_incident`) only treats a dedup_key as "already
    open" when the existing row is non-terminal. A genuine recurrence of the
    same underlying issue (same detector, same discriminator -- e.g. the
    same DLQ topic hitting the same violation reasons again on a later
    batch) after the first incident reached RESOLVED used to raise an
    IntegrityError on insert instead of being accepted as a new incident.
    Fixed by relaxing the DB index to non-unique (see
    db.base.apply_lightweight_schema_patches) and relying solely on the
    application-level "one open incident per dedup_key" check.
    """
    def fake_execute(self, tool_input):
        from app.tools.ops_tools import ValidatePipelineHealthOutput
        return ValidatePipelineHealthOutput(healthy=True, checks={"forced_healthy_for_test": True})

    from app.tools import registry as tool_registry
    monkeypatch.setattr(
        tool_registry._REGISTRY["validate_pipeline_health"].__class__, "_execute", fake_execute
    )

    disc = "recurrence-test-" + str(uuid.uuid4())

    def make():
        return build_incident(
            incident_type=IncidentType.POISON_MESSAGE, severity=Severity.MEDIUM,
            source_component="kafka:orders.raw", title="poison", description="poison",
            evidence={"topic": "orders.raw", "count": 1, "samples": [{"reason": "MISSING_FIELDS"}]},
            correlation_id=str(uuid.uuid4()), discriminator=disc,
        )

    incident1 = make()
    assert incident1.dedup_key == make().dedup_key  # same discriminator -> same dedup_key

    id1 = graph.receive_incident(incident1)
    db = SessionLocal()
    try:
        row1 = db.query(IncidentRow).get(id1)
        needs_approval = row1.status == IncidentStatus.AWAITING_APPROVAL
    finally:
        db.close()

    if needs_approval:
        graph.approve_incident(id1, decided_by="test-operator", reason="approved for recurrence test")

    db = SessionLocal()
    try:
        row1 = db.query(IncidentRow).get(id1)
        # Whichever path (auto-execute or approve-then-execute), the first
        # occurrence must reach a terminal state for the recurrence check below.
        assert row1.status in (IncidentStatus.RESOLVED, IncidentStatus.ROLLED_BACK, IncidentStatus.FAILED)
    finally:
        db.close()

    # A brand new occurrence of the identical underlying issue (same
    # dedup_key) must be accepted as a new incident, not collide with the
    # now-terminal row from the same dedup_key.
    incident2 = make()
    id2 = graph.receive_incident(incident2)
    assert id2 != id1

    db = SessionLocal()
    try:
        row2 = db.query(IncidentRow).get(id2)
        assert row2 is not None
        assert row2.dedup_key == row1.dedup_key
    finally:
        db.close()
