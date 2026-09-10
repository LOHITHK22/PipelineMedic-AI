"""Deterministic data-quality checks (poison messages, malformed records, DLQ growth)."""
import json
import uuid

from app.detectors.base import build_incident
from app.models.schemas import Incident, IncidentType, Severity

REQUIRED_FIELDS = {"event_id", "order_id", "customer_id", "amount", "currency", "event_time"}


class DataQualityDetector:
    def check_message(self, raw_message: str | bytes) -> dict | None:
        """Returns a violation dict if the message is malformed/poison, else None."""
        try:
            if isinstance(raw_message, bytes):
                raw_message = raw_message.decode("utf-8")
            parsed = json.loads(raw_message)
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            return {"reason": "INVALID_JSON", "error": str(e), "raw": str(raw_message)[:500]}

        if not isinstance(parsed, dict):
            return {"reason": "NOT_AN_OBJECT", "raw": str(raw_message)[:500]}

        missing = REQUIRED_FIELDS - set(parsed.keys())
        if missing:
            return {"reason": "MISSING_FIELDS", "missing_fields": sorted(missing), "record": parsed}

        if "amount" in parsed and not isinstance(parsed["amount"], (int, float)):
            return {"reason": "TYPE_VIOLATION", "field": "amount", "value": parsed["amount"], "record": parsed}

        return None

    def detect_from_dlq_batch(self, topic: str, poison_messages: list[dict]) -> Incident | None:
        if not poison_messages:
            return None

        reasons = {m["reason"] for m in poison_messages}
        severity = Severity.HIGH if len(poison_messages) > 5 else Severity.MEDIUM

        return build_incident(
            incident_type=IncidentType.POISON_MESSAGE,
            severity=severity,
            source_component=f"kafka:{topic}",
            title=f"{len(poison_messages)} poison message(s) routed to DLQ from {topic}",
            description=f"Detected violation reasons: {', '.join(sorted(reasons))}.",
            evidence={"topic": topic, "count": len(poison_messages), "samples": poison_messages[:5]},
            correlation_id=str(uuid.uuid4()),
            discriminator=f"{topic}:{','.join(sorted(reasons))}",
        )
