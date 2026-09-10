"""Background pipeline monitor.

This service plays two roles:

1. It is the fallback "stream validator" for orders.raw -> orders.validated /
   pipeline.dlq if the PyFlink job (flink/jobs/order_validator_job.py) is not
   running (e.g. PyFlink container unavailable in this environment). It uses
   the exact same deterministic validation rules a real Flink job would.
2. It periodically runs the deterministic detectors (schema drift, consumer
   lag, data quality) against live Kafka/Postgres state and feeds any
   incidents into the agent graph via `receive_incident`.

It runs as a background thread started from FastAPI's lifespan handler so
`docker compose up` gives you a fully working, self-healing pipeline without
requiring a separate consumer process.
"""
from __future__ import annotations

import json
import logging
import threading
import time

from app.config import settings
from app.detectors.data_quality import DataQualityDetector, REQUIRED_FIELDS
from app.detectors.kafka_lag import KafkaLagDetector
from app.detectors.schema_drift import SchemaDriftDetector, diff_schemas
from app.detectors.airflow_failure import AirflowFailureDetector
from app.detectors.flink_failure import FlinkFailureDetector
from app.agents.graph import receive_incident
from app.models.schemas import CompatibilityClass, IncidentType, Severity
from app.db.base import SessionLocal
from app.db.models import SchemaVersion

logger = logging.getLogger("pipelinemedic.monitor")

CANONICAL_ORDER_SCHEMA = {
    "event_id": {"type": "string", "nullable": False},
    "order_id": {"type": "string", "nullable": False},
    "customer_id": {"type": "string", "nullable": False},
    "amount": {"type": "number", "nullable": False},
    "currency": {"type": "string", "nullable": False},
    "event_time": {"type": "string", "nullable": False},
}


def _ensure_baseline_schema_version():
    db = SessionLocal()
    try:
        existing = db.query(SchemaVersion).filter_by(subject="orders.raw").first()
        if not existing:
            db.add(SchemaVersion(subject="orders.raw", version=1, schema_json=CANONICAL_ORDER_SCHEMA))
            db.commit()
    finally:
        db.close()


class PipelineMonitor:
    def __init__(self):
        self._stop = threading.Event()
        self._threads: list[threading.Thread] = []
        self.dq_detector = DataQualityDetector()
        self.lag_detector = KafkaLagDetector()
        self.airflow_detector = AirflowFailureDetector()
        self.flink_detector = FlinkFailureDetector()
        self._seen_airflow_failures: set[str] = set()

    def start(self):
        _ensure_baseline_schema_version()
        self._threads = []
        if settings.pipeline_monitor_stream_validator_enabled:
            t1 = threading.Thread(target=self._run_stream_validator, daemon=True, name="stream-validator")
            t1.start()
            self._threads.append(t1)
        else:
            logger.info(
                "stream-validator thread disabled (pipeline_monitor_stream_validator_enabled=false); "
                "expecting the real Flink job to own orders.raw -> orders.validated/dlq validation."
            )
        t2 = threading.Thread(target=self._run_lag_monitor, daemon=True, name="lag-monitor")
        t2.start()
        self._threads.append(t2)
        t3 = threading.Thread(target=self._run_flink_health_monitor, daemon=True, name="flink-health-monitor")
        t3.start()
        self._threads.append(t3)
        t4 = threading.Thread(target=self._run_airflow_health_monitor, daemon=True, name="airflow-health-monitor")
        t4.start()
        self._threads.append(t4)
        logger.info("PipelineMonitor started (stream-validator=%s + lag-monitor + flink-health-monitor + airflow-health-monitor)",
                    settings.pipeline_monitor_stream_validator_enabled)

    def stop(self):
        self._stop.set()

    # ---- stream validator (Flink-fallback + poison/schema-drift detection) ----

    def _run_stream_validator(self):
        from kafka import KafkaConsumer, KafkaProducer

        for attempt in range(30):
            try:
                consumer = KafkaConsumer(
                    settings.kafka_topic_orders_raw,
                    bootstrap_servers=settings.kafka_bootstrap_servers,
                    group_id=settings.kafka_consumer_group,
                    enable_auto_commit=True,
                    auto_offset_reset="latest",
                    consumer_timeout_ms=2000,
                )
                producer = KafkaProducer(bootstrap_servers=settings.kafka_bootstrap_servers)
                break
            except Exception as e:
                logger.warning("Kafka not ready yet (attempt %s): %s", attempt, e)
                time.sleep(2)
        else:
            logger.error("Kafka never became available; stream validator not started.")
            return

        poison_batch: list[dict] = []
        sample_batch: list[dict] = []
        last_flush = time.time()

        while not self._stop.is_set():
            for msg in consumer:
                if self._stop.is_set():
                    break
                violation = self.dq_detector.check_message(msg.value)
                if violation:
                    poison_batch.append(violation)
                    producer.send(settings.kafka_topic_dlq, json.dumps(violation).encode("utf-8"))
                else:
                    record = json.loads(msg.value)
                    sample_batch.append(record)
                    producer.send(settings.kafka_topic_orders_validated, json.dumps(record).encode("utf-8"))

                if len(poison_batch) >= 3:
                    self._maybe_raise_data_quality_incident(poison_batch)
                    poison_batch = []

                if len(sample_batch) >= 20:
                    self._maybe_raise_schema_drift_incident(sample_batch)
                    sample_batch = sample_batch[-5:]  # keep a small rolling tail

            time.sleep(0.5)

    def _maybe_raise_data_quality_incident(self, poison_batch: list[dict]):
        incident = self.dq_detector.detect_from_dlq_batch(settings.kafka_topic_orders_raw, poison_batch)
        if incident:
            try:
                receive_incident(incident)
            except Exception as e:
                logger.exception("Failed to process data-quality incident: %s", e)

    def _maybe_raise_schema_drift_incident(self, sample_batch: list[dict]):
        db = SessionLocal()
        try:
            baseline = db.query(SchemaVersion).filter_by(subject="orders.raw").order_by(SchemaVersion.version.desc()).first()
            baseline_schema = baseline.schema_json if baseline else CANONICAL_ORDER_SCHEMA
        finally:
            db.close()

        inferred = SchemaDriftDetector.infer_schema(sample_batch)
        diff = diff_schemas("orders.raw", baseline_schema, inferred)
        if diff.overall_compatibility == CompatibilityClass.BREAKING:
            from app.detectors.base import build_incident
            import uuid

            incident = build_incident(
                incident_type=IncidentType.SCHEMA_DRIFT,
                severity=Severity.HIGH,
                source_component="kafka:orders.raw",
                title="Breaking schema drift detected on orders.raw",
                description=f"{len(diff.changes)} field change(s) detected, overall BREAKING.",
                evidence={"subject": "orders.raw", "changes": [c.model_dump() for c in diff.changes]},
                correlation_id=str(uuid.uuid4()),
                discriminator=",".join(sorted(c.field_name for c in diff.changes)),
            )
            try:
                receive_incident(incident)
            except Exception as e:
                logger.exception("Failed to process schema-drift incident: %s", e)

    # ---- periodic lag monitor ----

    def _run_lag_monitor(self):
        time.sleep(10)  # let Kafka + topics settle first
        while not self._stop.is_set():
            try:
                from app.tools.kafka_tools import GetKafkaConsumerLagTool
                result = GetKafkaConsumerLagTool().run(
                    {"topic": settings.kafka_topic_orders_raw, "group_id": settings.kafka_consumer_group}
                )
                from app.observability.metrics import KAFKA_CONSUMER_LAG
                KAFKA_CONSUMER_LAG.labels(topic=settings.kafka_topic_orders_raw, group_id=settings.kafka_consumer_group).set(result.total_lag)

                incident = self.lag_detector.detect(
                    settings.kafka_topic_orders_raw, settings.kafka_consumer_group, result.lag_by_partition
                )
                if incident:
                    receive_incident(incident)
            except Exception as e:
                logger.warning("Lag monitor iteration failed: %s", e)
            time.sleep(settings.consumer_poll_interval_seconds * 5)

    # ---- periodic Flink job health monitor (real JobManager REST API, with
    # graceful no-op if the "full" profile's Flink cluster isn't running) ----

    def _run_flink_health_monitor(self):
        time.sleep(15)  # let a real Flink cluster / job submission settle first
        from app.tools.flink_tools import GetFlinkJobStatusTool, GetFlinkJobStatusInput

        tool = GetFlinkJobStatusTool()
        while not self._stop.is_set():
            try:
                result = tool._execute(GetFlinkJobStatusInput(job_name="pipelinemedic-order-validator"))
                if result.state in ("UNREACHABLE", "UNKNOWN"):
                    # Flink's "full" profile isn't up in this environment (or the
                    # job hasn't been submitted yet) -- nothing to detect against,
                    # not an error. The Python fallback validator covers this case.
                    pass
                else:
                    incident = self.flink_detector.detect(
                        job_id=result.job_id or "unknown",
                        job_name="pipelinemedic-order-validator",
                        state=result.state,
                        restart_count=result.restart_count,
                        exceptions=result.exceptions,
                    )
                    if incident:
                        receive_incident(incident)
            except Exception as e:
                logger.warning("Flink health monitor iteration failed: %s", e)
            time.sleep(20)

    # ---- periodic Airflow DAG health monitor (real webserver REST API, with
    # graceful no-op if the "full" profile's Airflow isn't running) ----

    def _run_airflow_health_monitor(self):
        time.sleep(15)
        from app.tools.airflow_tools import GetAirflowDagStatusTool, GetAirflowDagStatusInput

        tool = GetAirflowDagStatusTool()
        dag_id = "order_data_quality_dag"
        while not self._stop.is_set():
            try:
                result = tool._execute(GetAirflowDagStatusInput(dag_id=dag_id))
                if result.error or result.latest_run_state is None:
                    # Airflow's "full" profile isn't up in this environment --
                    # nothing to detect against, not an error.
                    pass
                elif result.latest_run_state in ("failed", "upstream_failed"):
                    discriminator = f"{dag_id}:{result.latest_run_state}"
                    if discriminator not in self._seen_airflow_failures:
                        self._seen_airflow_failures.add(discriminator)
                        incident = self.airflow_detector.detect(
                            dag_id=dag_id,
                            task_id="check_orders_schema",
                            run_id="latest",
                            state=result.latest_run_state,
                            try_number=1,
                            logs_tail="(fetch via get_airflow_task_logs tool for full logs)",
                        )
                        if incident:
                            receive_incident(incident)
                else:
                    # Latest run recovered; allow a future failure to raise again.
                    self._seen_airflow_failures.discard(f"{dag_id}:failed")
                    self._seen_airflow_failures.discard(f"{dag_id}:upstream_failed")
            except Exception as e:
                logger.warning("Airflow health monitor iteration failed: %s", e)
            time.sleep(20)


monitor = PipelineMonitor()
