"""Deterministic Kafka consumer-lag detection."""
import time
import uuid

from app.config import settings
from app.detectors.base import build_incident
from app.models.schemas import Incident, IncidentType, Severity


class KafkaLagDetector:
    def __init__(self, warning_threshold: int | None = None, critical_threshold: int | None = None):
        self.warning_threshold = warning_threshold or settings.lag_warning_threshold
        self.critical_threshold = critical_threshold or settings.lag_critical_threshold

    def detect(self, topic: str, group_id: str, lag_by_partition: dict[int, int]) -> Incident | None:
        total_lag = sum(lag_by_partition.values())
        if total_lag < self.warning_threshold:
            return None

        severity = Severity.CRITICAL if total_lag >= self.critical_threshold else Severity.HIGH
        return build_incident(
            incident_type=IncidentType.KAFKA_LAG,
            severity=severity,
            source_component=f"kafka:{topic}:{group_id}",
            title=f"Consumer lag on {topic} ({group_id}) is {total_lag}",
            description=(
                f"Total consumer lag across {len(lag_by_partition)} partitions is {total_lag}, "
                f"exceeding threshold {self.warning_threshold}."
            ),
            evidence={
                "topic": topic,
                "group_id": group_id,
                "lag_by_partition": lag_by_partition,
                "total_lag": total_lag,
                "warning_threshold": self.warning_threshold,
                "critical_threshold": self.critical_threshold,
                "measured_at": time.time(),
            },
            correlation_id=str(uuid.uuid4()),
            discriminator=f"{topic}:{group_id}:{total_lag // max(self.warning_threshold, 1)}",
        )
