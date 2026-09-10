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
