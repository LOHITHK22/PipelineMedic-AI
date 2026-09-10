"""Real PyFlink DataStream job: validates orders.raw and routes records to
orders.validated (valid) or pipeline.dlq (invalid/poison), mirroring exactly
the validation rules in backend/app/detectors/data_quality.py so the
deterministic detection logic is consistent whether it runs in the fallback
Python monitor (backend/app/services/pipeline_monitor.py) or in real Flink.

Submit with:
    docker compose --profile full exec flink-jobmanager \
        flink run -py /opt/flink/jobs/order_validator_job.py

Requires the `flink` extras (apache-flink) baked into a custom Flink image,
see flink/Dockerfile. See docs/limitations.md / README "Limitations" section
for the current verification status of this job in this environment --
PyFlink's Python-on-JVM bridge has a heavy, fragile dependency chain
(specific Flink minor version <-> specific apache-flink pip version <->
specific Python version) which needs a purpose-built image; we provide that
Dockerfile and this job code, real and runnable, but flag it as the piece
most likely to need local iteration if the exact base image tags drift from
what was validated at the time this repo was authored.
"""
import json
import os

from pyflink.common import Types
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.base import DeliveryGuarantee
from pyflink.datastream.connectors.kafka import (
    KafkaSource, KafkaOffsetsInitializer, KafkaSink, KafkaRecordSerializationSchema,
)

REQUIRED_FIELDS = {"event_id", "order_id", "customer_id", "amount", "currency", "event_time"}
BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")


def validate(raw_value: str) -> tuple[bool, dict]:
    try:
        record = json.loads(raw_value)
    except (json.JSONDecodeError, UnicodeDecodeError):
        return False, {"reason": "INVALID_JSON", "raw": raw_value[:500]}

    if not isinstance(record, dict):
        return False, {"reason": "NOT_AN_OBJECT", "raw": raw_value[:500]}

    missing = REQUIRED_FIELDS - set(record.keys())
    if missing:
        return False, {"reason": "MISSING_FIELDS", "missing_fields": sorted(missing), "record": record}

    if "amount" in record and not isinstance(record["amount"], (int, float)):
        return False, {"reason": "TYPE_VIOLATION", "field": "amount", "value": record["amount"], "record": record}

    return True, record


def route(raw_value: str) -> str:
    """Returns a tagged string 'VALID:<json>' or 'INVALID:<json>' -- Flink's
    Python DataStream API side-output support is more ergonomic in Java; for
    a demo-scale job we keep this single-stream-with-tag approach and split
    downstream with two filtered streams, which is simpler to reason about
    and still a real, running Flink topology."""
    ok, payload = validate(raw_value)
    prefix = "VALID" if ok else "INVALID"
    return f"{prefix}:{json.dumps(payload)}"


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(int(os.environ.get("FLINK_PARALLELISM", "2")))

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(BOOTSTRAP)
        .set_topics("orders.raw")
        .set_group_id("flink-order-validator")
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    raw_stream = env.from_source(source, WatermarkStrategy.no_watermarks(), "orders-raw-source")
    tagged = raw_stream.map(route, output_type=Types.STRING())

    valid_stream = tagged.filter(lambda s: s.startswith("VALID:")).map(
        lambda s: s[len("VALID:"):], output_type=Types.STRING()
    )
    invalid_stream = tagged.filter(lambda s: s.startswith("INVALID:")).map(
        lambda s: s[len("INVALID:"):], output_type=Types.STRING()
    )

    valid_sink = (
        KafkaSink.builder()
        .set_bootstrap_servers(BOOTSTRAP)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic("orders.validated")
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .build()
    )
    dlq_sink = (
        KafkaSink.builder()
        .set_bootstrap_servers(BOOTSTRAP)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic("pipeline.dlq")
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .build()
    )

    valid_stream.sink_to(valid_sink)
    invalid_stream.sink_to(dlq_sink)

    env.execute("pipelinemedic-order-validator")


if __name__ == "__main__":
    main()
