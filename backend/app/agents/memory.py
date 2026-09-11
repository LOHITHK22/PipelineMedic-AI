"""Incident memory: deterministic retrieval of similar, successfully-resolved
past incidents to inform (never blindly repeat) diagnosis and repair planning.

No vector embeddings, no external vector DB. Similarity is computed with
plain structured comparisons over a dedicated `incident_memory` table
(see app.db.models.IncidentMemory), populated only when an incident is
actually RESOLVED with validation.passed == True:

  - incident_type match (required to even be considered "similar")
  - source_component match (exact match, small bonus weight)
  - evidence signature overlap: a deterministic, detector-specific set of
    strings extracted from `incident.evidence` (e.g. schema-diff field names
    + change types for SCHEMA_DRIFT, DLQ reason codes for POISON_MESSAGE,
    topic/group for KAFKA_LAG, dag/task id for AIRFLOW_FAILURE), compared
    with Jaccard similarity (|A ∩ B| / |A ∪ B|).

See docs/agent-design.md ("Incident memory: retrieval design") for the full
rationale, including why we deliberately avoided embeddings for this project.
"""
from __future__ import annotations

from sqlalchemy.orm import Session

from app.db.models import IncidentMemory
from app.models.schemas import Incident, SimilarIncidentMatch

# Minimum Jaccard similarity for a past incident to be considered "highly
# similar" enough to bias the mock LLM's repair-plan choice toward it.
HIGH_SIMILARITY_THRESHOLD = 0.6


def evidence_signature(incident_type: str, evidence: dict) -> list[str]:
    """Deterministic, detector-specific structured summary of `evidence`,
    reduced to a sorted list of short tokens suitable for Jaccard comparison
    and for storage as JSON.

    This is intentionally simple and inspectable -- no embeddings -- so the
    similarity score used to influence an autonomous repair decision can
    always be explained by pointing at the exact tokens that overlapped.
    """
    sig: set[str] = set()
    evidence = evidence or {}

    if incident_type == "SCHEMA_DRIFT":
        for c in evidence.get("changes", []) or []:
            if not isinstance(c, dict):
                continue
            sig.add(f"field:{c.get('field_name')}")
            sig.add(f"change:{c.get('change_type')}")
            sig.add(f"compat:{c.get('compatibility')}")
        if evidence.get("subject"):
            sig.add(f"subject:{evidence['subject']}")

    elif incident_type == "POISON_MESSAGE":
        for s in evidence.get("samples", []) or []:
            if isinstance(s, dict) and s.get("reason"):
                sig.add(f"reason:{s['reason']}")
        if evidence.get("topic"):
            sig.add(f"topic:{evidence['topic']}")

    elif incident_type == "KAFKA_LAG":
        if evidence.get("topic"):
            sig.add(f"topic:{evidence['topic']}")
        if evidence.get("group_id"):
            sig.add(f"group:{evidence['group_id']}")
        total_lag = evidence.get("total_lag")
        if isinstance(total_lag, (int, float)):
            # Bucket lag magnitude so incidents of similar severity match
            # without requiring exact lag values.
            bucket = "critical" if total_lag >= evidence.get("critical_threshold", 5000) else "warning"
            sig.add(f"lag_bucket:{bucket}")

    elif incident_type == "AIRFLOW_FAILURE":
        if evidence.get("dag_id"):
            sig.add(f"dag:{evidence['dag_id']}")
        if evidence.get("task_id"):
            sig.add(f"task:{evidence['task_id']}")
        logs = (evidence.get("logs_tail") or "").lower()
        if "column" in logs and ("missing" in logs or "does not exist" in logs):
            sig.add("pattern:missing_column")

    elif incident_type == "FLINK_FAILURE":
        if evidence.get("job_name"):
            sig.add(f"job:{evidence['job_name']}")
        if evidence.get("state"):
            sig.add(f"state:{evidence['state']}")

    else:
        # Generic fallback: shallow scalar key:value pairs.
        for k, v in evidence.items():
            if isinstance(v, (str, int, float, bool)):
                sig.add(f"{k}:{v}")

    return sorted(sig)


def _jaccard(a: set[str], b: set[str]) -> float:
    union = a | b
    if not union:
        return 0.0
    return len(a & b) / len(union)


def remember_resolved_incident(db: Session, incident_row, diagnosis: dict, plan: dict) -> None:
    """Write a durable memory row for an incident that just reached
    INCIDENT_RESOLVED with validation.passed == True. Called from
    app.agents.graph._execute_and_validate. Never called for a rolled-back
    or failed incident -- memory only ever remembers confirmed fixes."""
    sig = evidence_signature(incident_row.incident_type, incident_row.evidence or {})
    db.add(
        IncidentMemory(
            incident_id=incident_row.id,
            incident_type=incident_row.incident_type,
            source_component=incident_row.source_component,
            evidence_signature=sig,
            root_cause=(diagnosis or {}).get("root_cause", ""),
            repair_plan_json=plan or {},
            validated_success=True,
        )
    )


def find_similar_resolved_incidents(
    db: Session, incident: Incident, top_n: int = 3
) -> list[SimilarIncidentMatch]:
    """Return up to `top_n` past validated-successful incidents of the same
    type, ranked by deterministic structured similarity.

    Only rows in `incident_memory` are considered, and every row there was
    written exclusively for incidents that reached RESOLVED with
    validation.passed == True (see `remember_resolved_incident`) -- so a
    fix that was rolled back or never validated can never be surfaced here.
    """
    candidates = (
        db.query(IncidentMemory)
        .filter(
            IncidentMemory.incident_type == incident.incident_type.value,
            IncidentMemory.validated_success.is_(True),
        )
        .order_by(IncidentMemory.created_at.desc())
        .limit(200)  # bounded scan; deterministic recency-biased pool
        .all()
    )

    current_sig = set(evidence_signature(incident.incident_type.value, incident.evidence))
    scored: list[SimilarIncidentMatch] = []
    for row in candidates:
        if row.incident_id == incident.id:
            continue
        past_sig = set(row.evidence_signature or [])
        sim = _jaccard(current_sig, past_sig)
        if row.source_component == incident.source_component:
            sim = min(1.0, sim + 0.1)
        if sim <= 0:
            continue
        scored.append(
            SimilarIncidentMatch(
                memory_id=row.id,
                incident_id=row.incident_id,
                similarity=round(sim, 4),
                incident_type=row.incident_type,
                source_component=row.source_component,
                root_cause=row.root_cause,
                repair_summary=(row.repair_plan_json or {}).get("summary", ""),
                validated_success=row.validated_success,
            )
        )

    scored.sort(key=lambda r: r.similarity, reverse=True)
    return scored[:top_n]
