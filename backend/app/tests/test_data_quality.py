import json

from app.detectors.data_quality import DataQualityDetector


def test_valid_message_passes():
    d = DataQualityDetector()
    msg = json.dumps({
        "event_id": "e1", "order_id": "o1", "customer_id": "c1",
        "amount": 10.5, "currency": "USD", "event_time": "2024-01-01T00:00:00Z",
    })
    assert d.check_message(msg) is None


def test_invalid_json_is_flagged():
    d = DataQualityDetector()
    violation = d.check_message(b"{not json")
    assert violation["reason"] == "INVALID_JSON"


def test_missing_fields_flagged():
    d = DataQualityDetector()
    violation = d.check_message(json.dumps({"event_id": "e1"}))
    assert violation["reason"] == "MISSING_FIELDS"
    assert "order_id" in violation["missing_fields"]


def test_type_violation_flagged():
    d = DataQualityDetector()
    msg = json.dumps({
        "event_id": "e1", "order_id": "o1", "customer_id": "c1",
        "amount": "not-a-number", "currency": "USD", "event_time": "2024-01-01T00:00:00Z",
    })
    violation = d.check_message(msg)
    assert violation["reason"] == "TYPE_VIOLATION"


def test_incident_created_only_for_nonempty_batch():
    d = DataQualityDetector()
    assert d.detect_from_dlq_batch("orders.raw", []) is None
    incident = d.detect_from_dlq_batch("orders.raw", [{"reason": "INVALID_JSON"}])
    assert incident is not None
    assert incident.incident_type.value == "POISON_MESSAGE"
