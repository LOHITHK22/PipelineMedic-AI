"""Deterministic, scenario-aware mock LLM provider.

No network calls. Pattern-matches on incident.evidence to produce structured
diagnoses and repair plans identical to what a well-behaved real LLM would
produce for these four demo scenarios: schema drift, poison messages,
consumer lag, and Airflow task failure. This lets the full agent graph run
end-to-end with LLM_PROVIDER=mock and no API key.
"""
from __future__ import annotations

from app.models.schemas import (
    DiagnosisResult, Incident, IncidentType, RepairAction, RepairPlan, RiskLevel,
)
from app.llm.provider import LLMProvider


class MockLLMProvider(LLMProvider):
    def diagnose(self, incident: Incident, context: dict) -> DiagnosisResult:
        ev = incident.evidence

        if incident.incident_type == IncidentType.SCHEMA_DRIFT:
            changes = ev.get("changes", [])
            breaking = [c for c in changes if c.get("compatibility") == "BREAKING"]
            return DiagnosisResult(
                root_cause=(
                    f"Upstream producer changed the schema of subject '{ev.get('subject')}': "
                    f"{len(breaking)} breaking field change(s) detected "
                    f"({', '.join(c['field_name'] for c in breaking) or 'n/a'})."
                ),
                confidence=0.93 if breaking else 0.6,
                affected_components=[incident.source_component, "flink-validator"],
                reasoning=(
                    "Deterministic schema diff classified these changes as BREAKING because they "
                    "either add a required field, remove a required field, narrow a type, or "
                    "tighten nullability -- all of which old-format consumers cannot handle "
                    "without failing validation."
                ),
                recommended_action_summary=(
                    "Quarantine drifted messages, add a compatibility shim/mapping in the Flink "
                    "validator for the renamed/changed fields, and register the new schema version."
                ),
            )

        if incident.incident_type == IncidentType.POISON_MESSAGE:
            reasons = ev.get("samples", [])
            reason_set = sorted({s.get("reason") for s in reasons if isinstance(s, dict)})
            return DiagnosisResult(
                root_cause=f"Malformed records reaching {ev.get('topic')} due to: {', '.join(reason_set) or 'unknown'}.",
                confidence=0.9,
                affected_components=[incident.source_component, "flink-validator"],
                reasoning=(
                    "The data-quality detector deterministically validated required fields and "
                    "types against the canonical order schema; violating records were routed to "
                    "the DLQ rather than crashing downstream consumers."
                ),
                recommended_action_summary="Quarantine the poison messages already in the DLQ and verify producer-side validation.",
            )

        if incident.incident_type == IncidentType.KAFKA_LAG:
            total_lag = ev.get("total_lag", 0)
            critical = total_lag >= ev.get("critical_threshold", 5000)
            return DiagnosisResult(
                root_cause=(
                    f"Consumer group '{ev.get('group_id')}' is falling behind on '{ev.get('topic')}' "
                    f"(lag={total_lag}), most likely due to reduced Flink task parallelism or a slow "
                    "downstream sink."
                ),
                confidence=0.85,
                affected_components=[ev.get("topic", ""), ev.get("group_id", "")],
                reasoning=(
                    "Lag was measured directly via get_kafka_consumer_lag across all partitions and "
                    "compared against configured thresholds; this is a direct metric, not an inference."
                ),
                recommended_action_summary=(
                    "Increase Flink job parallelism to drain the backlog"
                    + (" (critical: escalate to on-call if parallelism bump does not help)" if critical else "")
                ),
            )

        if incident.incident_type == IncidentType.AIRFLOW_FAILURE:
            logs = ev.get("logs_tail", "")
            missing_col = "column" in logs.lower() and ("missing" in logs.lower() or "does not exist" in logs.lower())
            return DiagnosisResult(
                root_cause=(
                    f"Airflow task {ev.get('dag_id')}.{ev.get('task_id')} failed"
                    + (" because an expected database column is missing (likely caused by the same upstream schema drift)." if missing_col else ".")
                ),
                confidence=0.88 if missing_col else 0.55,
                affected_components=[f"airflow:{ev.get('dag_id')}", "postgres"],
                reasoning="Root cause inferred from the tail of the task logs supplied as evidence, cross-referenced with recent schema-version history.",
                recommended_action_summary="Rerun the Airflow task after the underlying schema/data issue is repaired.",
            )

        if incident.incident_type == IncidentType.FLINK_FAILURE:
            return DiagnosisResult(
                root_cause=f"Flink job {ev.get('job_name')} entered state {ev.get('state')} after {ev.get('restart_count')} restarts.",
                confidence=0.8,
                affected_components=[f"flink:{ev.get('job_name')}"],
                reasoning="Job state and restart count were read directly from the Flink JobManager REST API.",
                recommended_action_summary="Restart the Flink job; if restarts persist, reduce parallelism to lower resource pressure.",
            )

        return DiagnosisResult(
            root_cause="Unclassified incident; insufficient evidence pattern match.",
            confidence=0.3,
            affected_components=[incident.source_component],
            reasoning="No specific mock diagnosis rule matched this incident type/evidence shape.",
            recommended_action_summary="Escalate to human on-call for manual triage.",
        )

    def generate_repair_plan(self, incident: Incident, diagnosis: DiagnosisResult, context: dict) -> RepairPlan:
        ev = incident.evidence

        if incident.incident_type == IncidentType.SCHEMA_DRIFT:
            actions = [
                RepairAction(
                    tool_name="quarantine_message",
                    tool_input={"topic": ev.get("subject", "orders.raw"), "reason": "schema_drift"},
                    description="Quarantine messages matching the drifted schema so they stop failing downstream validation.",
                    is_reversible=True,
                    rollback_tool_name=None,
                ),
                RepairAction(
                    tool_name="apply_safe_config_patch",
                    tool_input={
                        "component": "flink-validator",
                        "patch": {"field_mapping": _rename_mapping(ev.get("changes", []))},
                    },
                    description="Apply a field-mapping compatibility shim in the Flink validator for renamed/changed fields.",
                    is_reversible=True,
                    rollback_tool_name="rollback_change",
                    rollback_input={"component": "flink-validator"},
                ),
            ]
            return RepairPlan(
                summary="Quarantine drifted messages and apply a compatibility shim in the Flink validator.",
                actions=actions,
                expected_outcome="orders.raw messages validate successfully again; DLQ growth stops.",
                estimated_risk_level=RiskLevel.MEDIUM,
            )

        if incident.incident_type == IncidentType.POISON_MESSAGE:
            actions = [
                RepairAction(
                    tool_name="quarantine_message",
                    tool_input={"topic": ev.get("topic", "orders.raw"), "reason": "poison_message"},
                    description="Quarantine poison messages already identified in evidence.samples.",
                    is_reversible=True,
                )
            ]
            return RepairPlan(
                summary="Quarantine poison messages currently polluting the DLQ.",
                actions=actions,
                expected_outcome="DLQ stops growing from this cause; valid traffic resumes flowing.",
                estimated_risk_level=RiskLevel.LOW,
            )

        if incident.incident_type == IncidentType.KAFKA_LAG:
            actions = [
                RepairAction(
                    tool_name="change_flink_parallelism",
                    tool_input={"job_name": "order-validator", "parallelism": 4},
                    description="Increase Flink job parallelism to drain consumer lag faster.",
                    is_reversible=True,
                    rollback_tool_name="change_flink_parallelism",
                    rollback_input={"job_name": "order-validator", "parallelism": 2},
                )
            ]
            return RepairPlan(
                summary="Scale up Flink parallelism to drain backlog.",
                actions=actions,
                expected_outcome="Consumer lag trends back below the warning threshold within a few minutes.",
                estimated_risk_level=RiskLevel.MEDIUM,
            )

        if incident.incident_type == IncidentType.AIRFLOW_FAILURE:
            actions = [
                RepairAction(
                    tool_name="rerun_airflow_task",
                    tool_input={"dag_id": ev.get("dag_id"), "task_id": ev.get("task_id"), "run_id": ev.get("run_id")},
                    description="Rerun the failed Airflow task now that the upstream issue is expected to be fixed.",
                    is_reversible=False,
                )
            ]
            return RepairPlan(
                summary="Rerun the failed Airflow task.",
                actions=actions,
                expected_outcome="Task run transitions to success on rerun.",
                estimated_risk_level=RiskLevel.LOW,
            )

        if incident.incident_type == IncidentType.FLINK_FAILURE:
            actions = [
                RepairAction(
                    tool_name="restart_flink_job",
                    tool_input={"job_id": ev.get("job_id")},
                    description="Restart the failed Flink job from the last checkpoint.",
                    is_reversible=False,
                )
            ]
            return RepairPlan(
                summary="Restart the Flink job.",
                actions=actions,
                expected_outcome="Job returns to RUNNING state.",
                estimated_risk_level=RiskLevel.MEDIUM,
            )

        return RepairPlan(
            summary="No automated plan available; escalate to human.",
            actions=[
                RepairAction(
                    tool_name="request_human_approval",
                    tool_input={"reason": "Unclassified incident type"},
                    description="Escalate to a human operator.",
                    is_reversible=True,
                )
            ],
            expected_outcome="Human operator triages manually.",
            estimated_risk_level=RiskLevel.HIGH,
        )


def _rename_mapping(changes: list[dict]) -> dict[str, str]:
    mapping = {}
    for c in changes:
        if c.get("change_type") == "RENAMED" and c.get("renamed_to"):
            mapping[c["renamed_to"]] = c["field_name"]
    return mapping
