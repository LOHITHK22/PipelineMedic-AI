"""Shared helpers for detectors: incident construction with deterministic dedup keys."""
import hashlib

from app.models.schemas import Incident, IncidentType, Severity


def make_dedup_key(incident_type: IncidentType, source_component: str, discriminator: str) -> str:
    """Deterministic dedup key so re-detecting the same underlying issue does
    not create duplicate incidents (idempotency at detection time)."""
    raw = f"{incident_type.value}:{source_component}:{discriminator}"
    return hashlib.sha256(raw.encode()).hexdigest()[:32]


def build_incident(
    incident_type: IncidentType,
    severity: Severity,
    source_component: str,
    title: str,
    description: str,
    evidence: dict,
    correlation_id: str,
    discriminator: str,
) -> Incident:
    return Incident(
        dedup_key=make_dedup_key(incident_type, source_component, discriminator),
        incident_type=incident_type,
        severity=severity,
        source_component=source_component,
        title=title,
        description=description,
        evidence=evidence,
        correlation_id=correlation_id,
    )
