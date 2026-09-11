"""Deterministic event correlation.

Goal: given an incident, find `SystemEvent`s (schema bumps, synthetic
deployment markers, config changes) and other `Incident`s that plausibly
*preceded and caused* it, so diagnosis can point at a concrete trigger
("schema version bumped from v4 to v5 47 seconds before failures began")
instead of describing symptoms in isolation.

This is matching on structured fields and time proximity only -- no LLM
guessing, no fuzzy text similarity -- consistent with this project's
"deterministic engineering for observable facts" philosophy (see
docs/agent-design.md). The LLM layer (mock or real) may use the *output* of
this module as evidence, but the correlation decision itself is made here,
deterministically, and is always independently inspectable/explainable.
"""
from __future__ import annotations

from datetime import datetime, timedelta

from sqlalchemy.orm import Session

from app.db.models import Incident as IncidentRow, SystemEvent
from app.models.schemas import Incident

# Default lookback window: how far back before an incident's detected_at we
# consider a system event or prior incident a plausible trigger. Configurable
# per-call for tests; kept small and deterministic (not learned) by design.
DEFAULT_WINDOW_SECONDS = 600  # 10 minutes


def _component_tokens(component: str) -> set[str]:
    """Loosely tokenize a component string ("kafka:orders.raw",
    "flink:order-validator") so correlation can match on the underlying
    resource name even when the two records prefix it differently."""
    if not component:
        return set()
    parts = component.replace("-", ".").replace(":", ".").split(".")
    return {p for p in parts if p}


def _components_relate(a: str, b: str) -> bool:
    if not a or not b:
        return False
    if a == b:
        return True
    return bool(_component_tokens(a) & _component_tokens(b))


def _type_relates_to_event(incident_type: str, event_type: str) -> bool:
    """Deterministic rule table: which SystemEvent types are plausible
    triggers for which incident types. Kept explicit and small rather than
    "anything within the window counts" so a correlation always has a
    structured, inspectable reason attached (see CorrelatedEvent.reason)."""
    if event_type == "SCHEMA_VERSION_CHANGE":
        return incident_type in ("SCHEMA_DRIFT", "POISON_MESSAGE", "AIRFLOW_FAILURE", "DATA_QUALITY")
    if event_type == "DEPLOYMENT":
        return True  # any incident type can plausibly follow a deployment
    if event_type == "CONFIG_CHANGE":
        return incident_type in ("KAFKA_LAG", "FLINK_FAILURE", "SCHEMA_DRIFT")
    return False


class CorrelatedEvent:
    def __init__(self, kind: str, event_type: str, component: str, occurred_at: datetime,
                 seconds_before: float, payload: dict, reason: str, related_incident_id: str | None = None):
        self.kind = kind  # "system_event" | "incident"
        self.event_type = event_type
        self.component = component
        self.occurred_at = occurred_at
        self.seconds_before = seconds_before
        self.payload = payload
        self.reason = reason
        self.related_incident_id = related_incident_id

    def to_dict(self) -> dict:
        return {
            "kind": self.kind,
            "event_type": self.event_type,
            "component": self.component,
            "occurred_at": self.occurred_at.isoformat() if self.occurred_at else None,
            "seconds_before": round(self.seconds_before, 3),
            "payload": self.payload,
            "reason": self.reason,
            "related_incident_id": self.related_incident_id,
        }


def find_correlated_events(
    db: Session,
    incident: Incident,
    detected_at: datetime | None = None,
    window_seconds: int = DEFAULT_WINDOW_SECONDS,
) -> list[CorrelatedEvent]:
    """Return SystemEvents and prior Incidents within `window_seconds` before
    `detected_at` (default: incident.detected_at, else now) that plausibly
    relate to this incident by component and/or a deterministic
    type-relationship rule. Sorted most-recent-first (closest, most likely
    trigger first)."""
    anchor = detected_at or incident.detected_at or datetime.utcnow()
    window_start = anchor - timedelta(seconds=window_seconds)

    results: list[CorrelatedEvent] = []

    events = (
        db.query(SystemEvent)
        .filter(SystemEvent.created_at >= window_start, SystemEvent.created_at <= anchor)
        .order_by(SystemEvent.created_at.desc())
        .limit(50)
        .all()
    )
    for ev in events:
        component_match = _components_relate(ev.component, incident.source_component)
        type_match = _type_relates_to_event(incident.incident_type.value, ev.event_type)
        if not (component_match or type_match):
            continue
        seconds_before = (anchor - ev.created_at).total_seconds()
        reason_bits = []
        if component_match:
            reason_bits.append(f"component '{ev.component}' relates to '{incident.source_component}'")
        if type_match:
            reason_bits.append(f"{ev.event_type} is a known plausible trigger for {incident.incident_type.value}")
        results.append(
            CorrelatedEvent(
                kind="system_event",
                event_type=ev.event_type,
                component=ev.component,
                occurred_at=ev.created_at,
                seconds_before=seconds_before,
                payload=ev.payload or {},
                reason="; ".join(reason_bits),
            )
        )

    prior_incidents = (
        db.query(IncidentRow)
        .filter(IncidentRow.created_at >= window_start, IncidentRow.created_at <= anchor)
        .order_by(IncidentRow.created_at.desc())
        .limit(50)
        .all()
    )
    for row in prior_incidents:
        if incident.id and row.id == incident.id:
            continue
        if not _components_relate(row.source_component, incident.source_component):
            continue
        seconds_before = (anchor - row.created_at).total_seconds()
        results.append(
            CorrelatedEvent(
                kind="incident",
                event_type=row.incident_type,
                component=row.source_component,
                occurred_at=row.created_at,
                seconds_before=seconds_before,
                payload={"title": row.title, "status": row.status.value if hasattr(row.status, "value") else row.status},
                reason=f"prior incident on related component '{row.source_component}' within the correlation window",
                related_incident_id=row.id,
            )
        )

    results.sort(key=lambda r: r.seconds_before)
    return results
