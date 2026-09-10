"""Deterministic Airflow DAG/task failure detection."""
import uuid

from app.detectors.base import build_incident
from app.models.schemas import Incident, IncidentType, Severity


class AirflowFailureDetector:
    def detect(self, dag_id: str, task_id: str, run_id: str, state: str, try_number: int, logs_tail: str) -> Incident | None:
        if state not in ("failed", "upstream_failed"):
            return None

        severity = Severity.HIGH if try_number >= 2 else Severity.MEDIUM
        return build_incident(
            incident_type=IncidentType.AIRFLOW_FAILURE,
            severity=severity,
            source_component=f"airflow:{dag_id}.{task_id}",
            title=f"Airflow task {dag_id}.{task_id} failed (run {run_id}, try {try_number})",
            description=f"Task state={state} after {try_number} attempt(s). See evidence.logs_tail for details.",
            evidence={
                "dag_id": dag_id,
                "task_id": task_id,
                "run_id": run_id,
                "state": state,
                "try_number": try_number,
                "logs_tail": logs_tail[-2000:],
            },
            correlation_id=str(uuid.uuid4()),
            discriminator=f"{dag_id}:{task_id}:{run_id}",
        )
