"""Deterministic Flink job-health detection."""
import uuid

from app.detectors.base import build_incident
from app.models.schemas import Incident, IncidentType, Severity


class FlinkFailureDetector:
    def detect(self, job_id: str, job_name: str, state: str, restart_count: int, exceptions: list[str]) -> Incident | None:
        if state in ("RUNNING", "FINISHED"):
            return None

        severity = Severity.CRITICAL if state in ("FAILED", "FAILING") else Severity.HIGH
        return build_incident(
            incident_type=IncidentType.FLINK_FAILURE,
            severity=severity,
            source_component=f"flink:{job_name}",
            title=f"Flink job {job_name} is in state {state} (restarts={restart_count})",
            description="Flink job is not healthy. See evidence.exceptions for the exception history.",
            evidence={
                "job_id": job_id,
                "job_name": job_name,
                "state": state,
                "restart_count": restart_count,
                "exceptions": exceptions[-10:],
            },
            correlation_id=str(uuid.uuid4()),
            discriminator=f"{job_id}:{state}:{restart_count}",
        )
