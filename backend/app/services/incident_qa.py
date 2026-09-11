"""Deterministic "ask about this incident" answering.

IMPORTANT -- this is NOT a new agentic action surface. It never calls an
LLM, never invokes a tool, and cannot affect real infrastructure. All
reasoning already happened once, deterministically, when the agent graph
ran diagnose()/generate_repair_plan() for this incident (see
backend/app/agents/graph.py); this module only reads that already-persisted
structured data (Incident.diagnosis, RepairPlan.plan_json, the risk
assessment, and the incident's own evidence) and rephrases it into plain
English, optionally leading with a different section based on keywords in
the question. It is incapable of hallucinating beyond what was already
diagnosed because it never generates new content -- only templates over
existing fields.
"""
from __future__ import annotations

_WHY_KEYWORDS = ("why", "cause", "reason", "root cause", "what happened")
_FIX_KEYWORDS = ("fix", "solution", "repair", "resolve", "how do we", "how to")
_STATUS_KEYWORDS = ("status", "state", "progress", "done", "resolved")
_RISK_KEYWORDS = ("risk", "safe", "danger", "approve", "approval")


def _root_cause_section(diagnosis: dict | None) -> str:
    if not diagnosis:
        return "No diagnosis has been produced for this incident yet."
    parts = [f"Root cause: {diagnosis.get('root_cause', 'unknown')}."]
    if diagnosis.get("reasoning"):
        parts.append(diagnosis["reasoning"])
    if diagnosis.get("affected_components"):
        parts.append("Affected components: " + ", ".join(diagnosis["affected_components"]) + ".")
    if diagnosis.get("confidence") is not None:
        parts.append(f"Diagnosis confidence: {round(diagnosis['confidence'] * 100)}%.")
    return " ".join(parts)


def _repair_section(plan: dict | None) -> str:
    if not plan:
        return "No repair plan has been generated for this incident yet."
    plan_json = plan.get("plan_json", plan)
    parts = [f"Proposed fix: {plan_json.get('summary', 'n/a')}."]
    actions = plan_json.get("actions") or []
    if actions:
        action_descs = [f"{i+1}) {a.get('description', a.get('tool_name'))}" for i, a in enumerate(actions)]
        parts.append("Steps: " + "; ".join(action_descs) + ".")
    if plan_json.get("expected_outcome"):
        parts.append(f"Expected outcome: {plan_json['expected_outcome']}.")
    return " ".join(parts)


def _risk_section(plan: dict | None) -> str:
    if not plan:
        return "No risk assessment is available yet."
    parts = [f"Risk level: {plan.get('risk_level', 'unknown')}."]
    if plan.get("risk_rationale"):
        parts.append(plan["risk_rationale"])
    parts.append(f"Autonomy decision: {plan.get('autonomy_decision', 'unknown')}.")
    return " ".join(parts)


def _status_section(incident: dict) -> str:
    status = incident.get("status", "unknown")
    return f"Current status: {status}."


def answer_incident_question(question: str, incident: dict, plan: dict | None) -> str:
    """Build a plain-English answer to `question` using only this
    incident's already-computed diagnosis/plan/evidence -- no new LLM call,
    no tool invocation.

    `incident` is the serialized incident dict (as returned by
    GET /incidents/{id}), and `plan` is the most recent repair-plan dict
    (with plan_json/risk_level/risk_rationale/autonomy_decision) if one
    exists, else None.
    """
    q = (question or "").lower()
    diagnosis = incident.get("diagnosis")

    sections: list[str] = []
    if any(k in q for k in _FIX_KEYWORDS):
        sections = [_repair_section(plan), _root_cause_section(diagnosis)]
    elif any(k in q for k in _WHY_KEYWORDS):
        sections = [_root_cause_section(diagnosis), _repair_section(plan)]
    elif any(k in q for k in _RISK_KEYWORDS):
        sections = [_risk_section(plan), _status_section(incident)]
    elif any(k in q for k in _STATUS_KEYWORDS):
        sections = [_status_section(incident), _repair_section(plan)]
    else:
        # Balanced summary: status, root cause, fix.
        sections = [_status_section(incident), _root_cause_section(diagnosis), _repair_section(plan)]

    return "\n\n".join(s for s in sections if s)
