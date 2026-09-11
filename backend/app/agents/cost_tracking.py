"""LLM cost protection: invocation/token audit trail + duplicate-incident
deduplication so retries or duplicate detections don't burn repeated LLM
calls.

Two independent mechanisms:

1. `record_llm_invocation` -- writes one `LLMInvocation` audit row per
   diagnose()/generate_repair_plan() call attempt, real or skipped, so total
   invocations/tokens/skips are always queryable (see GET /metrics/llm-usage
   and app.observability.metrics counters).

2. `find_recent_duplicate` -- before calling the LLM, look for another
   incident with the *same* incident_type + source_component + evidence
   signature (reusing app.agents.memory.evidence_signature, so it is the
   exact same deterministic fingerprint incident-memory already uses) created
   within `DEDUP_WINDOW_SECONDS`, that already has a diagnosis + plan. If
   found, the caller reuses that diagnosis/plan instead of invoking the LLM
   again. This only ever matches on an EXACT evidence-signature match (not a
   fuzzy similarity threshold like incident-memory retrieval uses) -- it is
   meant to catch retries/duplicate detections of the *same* underlying
   event, not to silently skip diagnosis for a genuinely new but
   similar-shaped incident.
"""
from __future__ import annotations

from datetime import datetime, timedelta

from sqlalchemy.orm import Session

from app.agents.memory import evidence_signature
from app.db.models import Incident as IncidentRow, LLMInvocation, RepairPlan as RepairPlanRow

DEDUP_WINDOW_SECONDS = 60


def estimate_tokens(text: str) -> int:
    """Deterministic token estimate. Uses tiktoken's cl100k_base encoding if
    installed (a real, meaningful estimate of input+expected-output size even
    though mock mode never calls a real LLM); otherwise falls back to a
    documented chars/4 heuristic. Either way this is NOT a real LLM's
    accounted token usage -- see the `note` field on GET /metrics/llm-usage."""
    try:
        import tiktoken

        enc = tiktoken.get_encoding("cl100k_base")
        return len(enc.encode(text))
    except Exception:
        return max(1, len(text) // 4)


def record_llm_invocation(
    db: Session, incident_id: str, correlation_id: str, provider: str, call_type: str,
    estimated_tokens: int, skipped_dedup: bool = False, reused_from_incident_id: str | None = None,
) -> None:
    db.add(
        LLMInvocation(
            incident_id=incident_id, correlation_id=correlation_id, provider=provider, call_type=call_type,
            estimated_tokens=estimated_tokens, skipped_dedup=skipped_dedup,
            reused_from_incident_id=reused_from_incident_id,
        )
    )


def find_recent_duplicate(db: Session, incident, window_seconds: int = DEDUP_WINDOW_SECONDS) -> IncidentRow | None:
    """Return the most recent OTHER incident row with an EXACT evidence
    signature match (same incident_type + source_component + evidence
    signature set) created within `window_seconds`, that already has a
    diagnosis recorded -- or None. Never matches an incident that has no
    diagnosis yet (nothing to reuse)."""
    # Anchor on wall-clock "now" (call time), not `incident.detected_at`.
    # `detected_at` is set once, at Incident construction time, by whatever
    # detector or test built the object -- for a fast synchronous retry (the
    # scenario this guards against), the *prior* incident may not even exist
    # in the DB yet at that timestamp. What actually matters for cost
    # protection is "was something with this exact signature diagnosed in
    # the last N seconds of real time", so we anchor on real time here.
    now_anchor = datetime.utcnow()
    window_start = now_anchor - timedelta(seconds=window_seconds)
    current_sig = set(evidence_signature(incident.incident_type.value, incident.evidence))
    if not current_sig:
        return None  # nothing structured to match on; never dedupe blindly

    candidates = (
        db.query(IncidentRow)
        .filter(
            IncidentRow.incident_type == incident.incident_type.value,
            IncidentRow.source_component == incident.source_component,
            IncidentRow.created_at >= window_start,
            IncidentRow.created_at <= now_anchor,
            IncidentRow.diagnosis.isnot(None),
        )
        .order_by(IncidentRow.created_at.desc())
        .limit(20)
        .all()
    )
    for row in candidates:
        if incident.id and row.id == incident.id:
            continue
        past_sig = set(evidence_signature(row.incident_type, row.evidence or {}))
        if past_sig == current_sig:
            has_plan = db.query(RepairPlanRow).filter_by(incident_id=row.id).first()
            if has_plan:
                return row
    return None
