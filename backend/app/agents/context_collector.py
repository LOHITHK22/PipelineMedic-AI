"""collect_context node: gathers read-only tool evidence beyond what the
detector already captured, so diagnosis has richer grounding."""
from __future__ import annotations

from app.models.schemas import Incident, IncidentType
from app.tools.registry import invoke_tool


def collect_context(incident: Incident) -> dict:
    context: dict = {"detector_evidence": incident.evidence}
    try:
        if incident.incident_type == IncidentType.KAFKA_LAG:
            context["recent_errors"] = invoke_tool(
                "get_recent_pipeline_errors", {"topic": "pipeline.dlq", "limit": 10},
                incident.correlation_id, incident.id,
            )
        elif incident.incident_type in (IncidentType.SCHEMA_DRIFT, IncidentType.POISON_MESSAGE):
            context["recent_errors"] = invoke_tool(
                "get_recent_pipeline_errors", {"topic": "pipeline.dlq", "limit": 10},
                incident.correlation_id, incident.id,
            )
        elif incident.incident_type == IncidentType.AIRFLOW_FAILURE:
            dag_id = incident.evidence.get("dag_id")
            if dag_id:
                context["dag_status"] = invoke_tool(
                    "get_airflow_dag_status", {"dag_id": dag_id}, incident.correlation_id, incident.id
                )
        elif incident.incident_type == IncidentType.FLINK_FAILURE:
            job_id = incident.evidence.get("job_id")
            if job_id:
                context["flink_status"] = invoke_tool(
                    "get_flink_job_status", {"job_id": job_id}, incident.correlation_id, incident.id
                )
    except Exception as e:
        context["collection_error"] = str(e)
    return context
