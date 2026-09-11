"""Synthetic incident-evaluation dataset.

Each scenario is a fully-formed `Incident` plus the outcome we expect the
REAL agent graph's reasoning (diagnose -> generate_repair_plan ->
calculate_risk) to produce, given the mock LLM provider and the real
`app.policies.engine.assess_risk` policy engine. Nothing here is executed
against live infra -- see app.eval.runner.

Scenario fields:
  incident: the Incident to feed the graph.
  expected_root_cause_contains: substring(s) the diagnosis root_cause must
      contain for the diagnosis to be scored correct (case-insensitive).
  expected_min_confidence / expected_max_confidence: bounds on
      diagnosis.confidence -- this is how we check that ambiguous or
      backward-compatible evidence does NOT produce an overconfident,
      over-escalated diagnosis.
  expected_primary_tool: the tool_name we expect as the plan's first action.
  expected_risk_level: the RiskLevel we expect app.policies.engine.assess_risk
      to compute (deterministic given the plan's actions + incident severity).
  expected_autonomy_decision: AUTO_EXECUTE / APPROVAL_REQUIRED / BLOCKED.
  is_edge_case: True for the deliberately tricky scenarios (ambiguous
      evidence, backward-compatible schema change, benign lag spike) that
      exist specifically to catch over-escalation / false positives.
"""
from __future__ import annotations

from dataclasses import dataclass, field

from app.detectors.base import build_incident
from app.models.schemas import Incident, IncidentType, RiskLevel, Severity


@dataclass
class EvalScenario:
    scenario_id: str
    incident: Incident
    expected_root_cause_contains: list[str]
    expected_min_confidence: float
    expected_max_confidence: float
    expected_primary_tool: str
    expected_risk_level: RiskLevel
    expected_autonomy_decision: str
    is_edge_case: bool = False
    notes: str = ""


def _inc(incident_type, severity, component, title, desc, evidence, disc) -> Incident:
    return build_incident(
        incident_type=incident_type, severity=severity, source_component=component,
        title=title, description=desc, evidence=evidence,
        correlation_id=f"eval-{disc}", discriminator=disc,
    )


def build_dataset() -> list[EvalScenario]:
    scenarios: list[EvalScenario] = []

    # 1. Clear-cut breaking schema drift (the "core demo" case).
    scenarios.append(EvalScenario(
        scenario_id="schema_drift_breaking",
        incident=_inc(
            IncidentType.SCHEMA_DRIFT, Severity.HIGH, "kafka:orders.raw",
            "Breaking schema drift", "customer_id renamed; amount type changed",
            {"subject": "orders.raw", "changes": [
                {"field_name": "customer_id", "change_type": "RENAMED", "renamed_to": "customerId", "compatibility": "BREAKING"},
                {"field_name": "amount", "change_type": "TYPE_CHANGED", "old_type": "number", "new_type": "string", "compatibility": "BREAKING"},
            ]},
            "schema-breaking-1",
        ),
        expected_root_cause_contains=["breaking", "orders.raw"],
        expected_min_confidence=0.85, expected_max_confidence=1.0,
        expected_primary_tool="quarantine_message",
        expected_risk_level=RiskLevel.MEDIUM,
        expected_autonomy_decision="APPROVAL_REQUIRED",
        notes="Two BREAKING field changes; must be diagnosed with high confidence and require approval.",
    ))

    # 2. Backward-compatible schema change -- edge case: must NOT be
    #    diagnosed as a breaking change / must NOT get inflated confidence.
    scenarios.append(EvalScenario(
        scenario_id="schema_drift_backward_compatible",
        incident=_inc(
            IncidentType.SCHEMA_DRIFT, Severity.LOW, "kafka:orders.raw",
            "Backward-compatible schema change", "new optional field added",
            {"subject": "orders.raw", "changes": [
                {"field_name": "promo_code", "change_type": "ADDED", "compatibility": "BACKWARD_COMPATIBLE"},
            ]},
            "schema-compat-1",
        ),
        expected_root_cause_contains=["0 breaking", "orders.raw"],
        expected_min_confidence=0.0, expected_max_confidence=0.7,
        expected_primary_tool="quarantine_message",
        expected_risk_level=RiskLevel.MEDIUM,  # plan actions still risk MEDIUM (config patch) -- see notes
        expected_autonomy_decision="APPROVAL_REQUIRED",
        is_edge_case=True,
        notes=(
            "0 breaking changes -> diagnosis confidence must stay LOW (<=0.7), unlike the "
            "breaking case's >=0.85. This is the signal we assert on for 'did not over-escalate "
            "the DIAGNOSIS'; the mock repair plan's actions are identical regardless of "
            "breaking-ness (a known simplification -- see docs/evaluation.md) so risk level is "
            "unchanged, but a real provider should also propose a lighter-weight plan here."
        ),
    ))

    # 3. Poison messages, LOW severity -> should be auto-executable.
    scenarios.append(EvalScenario(
        scenario_id="poison_message_low_severity",
        incident=_inc(
            IncidentType.POISON_MESSAGE, Severity.LOW, "kafka:orders.raw",
            "Poison messages in DLQ", "malformed JSON records",
            {"topic": "orders.raw", "samples": [{"reason": "INVALID_JSON"}, {"reason": "MISSING_REQUIRED_FIELD"}]},
            "poison-1",
        ),
        expected_root_cause_contains=["malformed", "orders.raw"],
        expected_min_confidence=0.8, expected_max_confidence=1.0,
        expected_primary_tool="quarantine_message",
        expected_risk_level=RiskLevel.LOW,
        expected_autonomy_decision="AUTO_EXECUTE",
        notes="Only a LOW-risk quarantine action + LOW severity -> should safely auto-execute.",
    ))

    # 4. Kafka lag, critical, HIGH severity -> approval required.
    scenarios.append(EvalScenario(
        scenario_id="kafka_lag_critical",
        incident=_inc(
            IncidentType.KAFKA_LAG, Severity.HIGH, "kafka:orders.raw",
            "Critical consumer lag", "lag far above threshold",
            {"topic": "orders.raw", "group_id": "order-validator", "total_lag": 25000, "critical_threshold": 5000},
            "lag-critical-1",
        ),
        expected_root_cause_contains=["falling behind", "order-validator"],
        expected_min_confidence=0.8, expected_max_confidence=1.0,
        expected_primary_tool="change_flink_parallelism",
        expected_risk_level=RiskLevel.MEDIUM,
        expected_autonomy_decision="APPROVAL_REQUIRED",
        notes="Large lag + HIGH severity -> scaling action needs approval, not silent auto-scale.",
    ))

    # 5. Benign lag spike, LOW severity, below critical threshold -- edge
    #    case: must NOT be treated as a critical incident.
    scenarios.append(EvalScenario(
        scenario_id="kafka_lag_benign_spike",
        incident=_inc(
            IncidentType.KAFKA_LAG, Severity.LOW, "kafka:orders.raw",
            "Minor consumer lag blip", "small transient lag",
            {"topic": "orders.raw", "group_id": "order-validator", "total_lag": 400, "critical_threshold": 5000},
            "lag-benign-1",
        ),
        expected_root_cause_contains=["falling behind", "order-validator"],
        expected_min_confidence=0.8, expected_max_confidence=1.0,
        expected_primary_tool="change_flink_parallelism",
        expected_risk_level=RiskLevel.MEDIUM,
        expected_autonomy_decision="APPROVAL_REQUIRED",
        is_edge_case=True,
        notes=(
            "total_lag=400 is far below critical_threshold=5000 -> root_cause text must NOT "
            "contain the 'critical: escalate to on-call' phrase the mock provider adds only "
            "when lag >= threshold. Overall risk is still MEDIUM because change_flink_parallelism "
            "is a MEDIUM-risk tool regardless of severity (policy engine floor) -- this scenario "
            "specifically checks the diagnosis text doesn't falsely claim criticality, not that "
            "autonomy changes."
        ),
    ))

    # 6. Airflow failure caused by missing column (schema-drift fallout).
    scenarios.append(EvalScenario(
        scenario_id="airflow_missing_column",
        incident=_inc(
            IncidentType.AIRFLOW_FAILURE, Severity.MEDIUM, "airflow:orders_etl",
            "Airflow task failed", "load_orders task failed",
            {"dag_id": "orders_etl", "task_id": "load_orders", "run_id": "run1",
             "logs_tail": "psycopg2.errors.UndefinedColumn: column \"customerId\" does not exist"},
            "airflow-1",
        ),
        expected_root_cause_contains=["missing", "orders_etl"],
        expected_min_confidence=0.8, expected_max_confidence=1.0,
        expected_primary_tool="rerun_airflow_task",
        expected_risk_level=RiskLevel.MEDIUM,
        expected_autonomy_decision="APPROVAL_REQUIRED",
        notes="Missing-column pattern in logs must be detected; rerun needs approval (irreversible action).",
    ))

    # 7. Ambiguous evidence: incident type provided but evidence doesn't
    #    match any detector-specific pattern well -- must NOT be diagnosed
    #    with false confidence, and must escalate to a human rather than
    #    guessing a repair.
    scenarios.append(EvalScenario(
        scenario_id="ambiguous_evidence_data_quality",
        incident=_inc(
            IncidentType.DATA_QUALITY, Severity.MEDIUM, "flink:order-validator",
            "Unclassified data quality anomaly", "evidence does not match a known pattern",
            {"anomaly_score": 0.42, "notes": "unclear signal, multiple weak correlations"},
            "ambiguous-1",
        ),
        expected_root_cause_contains=["unclassified", "insufficient evidence"],
        expected_min_confidence=0.0, expected_max_confidence=0.4,
        expected_primary_tool="request_human_approval",
        expected_risk_level=RiskLevel.MEDIUM,
        expected_autonomy_decision="APPROVAL_REQUIRED",
        is_edge_case=True,
        notes="No detector-specific pattern matched -> low confidence + escalate to human, never a guessed repair.",
    ))

    # 8. Flink job failure, MEDIUM severity.
    scenarios.append(EvalScenario(
        scenario_id="flink_job_failure",
        incident=_inc(
            IncidentType.FLINK_FAILURE, Severity.MEDIUM, "flink:order-validator",
            "Flink job failed", "job entered FAILED state after restarts",
            {"job_name": "order-validator", "job_id": "job-123", "state": "FAILED", "restart_count": 3},
            "flink-1",
        ),
        expected_root_cause_contains=["order-validator", "failed"],
        expected_min_confidence=0.7, expected_max_confidence=1.0,
        expected_primary_tool="restart_flink_job",
        expected_risk_level=RiskLevel.MEDIUM,
        expected_autonomy_decision="APPROVAL_REQUIRED",
        notes="Straightforward restart scenario, MEDIUM risk tool -> approval required.",
    ))

    return scenarios
