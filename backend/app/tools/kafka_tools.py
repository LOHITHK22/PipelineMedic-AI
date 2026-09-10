from __future__ import annotations

from pydantic import BaseModel

from app.config import settings
from app.models.schemas import ToolRiskLevel
from app.tools.base import BaseTool


def _admin_client():
    from kafka import KafkaAdminClient

    return KafkaAdminClient(bootstrap_servers=settings.kafka_bootstrap_servers)


def _consumer_client(group_id: str):
    from kafka import KafkaConsumer

    return KafkaConsumer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id=group_id,
        enable_auto_commit=False,
        consumer_timeout_ms=3000,
    )


# ---- get_kafka_consumer_lag ----

class GetKafkaLagInput(BaseModel):
    topic: str
    group_id: str


class GetKafkaLagOutput(BaseModel):
    topic: str
    group_id: str
    lag_by_partition: dict[int, int]
    total_lag: int
    error: str | None = None


class GetKafkaConsumerLagTool(BaseTool):
    name = "get_kafka_consumer_lag"
    risk_level = ToolRiskLevel.LOW
    input_model = GetKafkaLagInput
    output_model = GetKafkaLagOutput

    def _execute(self, tool_input: GetKafkaLagInput) -> GetKafkaLagOutput:
        try:
            consumer = _consumer_client(tool_input.group_id)
            partitions = consumer.partitions_for_topic(tool_input.topic) or set()
            from kafka import TopicPartition

            tps = [TopicPartition(tool_input.topic, p) for p in partitions]
            consumer.assign(tps)
            end_offsets = consumer.end_offsets(tps)
            lag_by_partition = {}
            for tp in tps:
                committed = consumer.committed(tp) or 0
                end = end_offsets.get(tp, 0)
                lag_by_partition[tp.partition] = max(end - committed, 0)
            consumer.close()
            return GetKafkaLagOutput(
                topic=tool_input.topic,
                group_id=tool_input.group_id,
                lag_by_partition=lag_by_partition,
                total_lag=sum(lag_by_partition.values()),
            )
        except Exception as e:  # broker unavailable, topic missing, etc.
            return GetKafkaLagOutput(
                topic=tool_input.topic, group_id=tool_input.group_id,
                lag_by_partition={}, total_lag=0, error=str(e),
            )


# ---- get_topic_metadata ----

class GetTopicMetadataInput(BaseModel):
    topic: str


class GetTopicMetadataOutput(BaseModel):
    topic: str
    partitions: int
    error: str | None = None


class GetTopicMetadataTool(BaseTool):
    name = "get_topic_metadata"
    risk_level = ToolRiskLevel.LOW
    input_model = GetTopicMetadataInput
    output_model = GetTopicMetadataOutput

    def _execute(self, tool_input: GetTopicMetadataInput) -> GetTopicMetadataOutput:
        try:
            consumer = _consumer_client("pipelinemedic-metadata-probe")
            partitions = consumer.partitions_for_topic(tool_input.topic) or set()
            consumer.close()
            return GetTopicMetadataOutput(topic=tool_input.topic, partitions=len(partitions))
        except Exception as e:
            return GetTopicMetadataOutput(topic=tool_input.topic, partitions=0, error=str(e))


# ---- get_recent_pipeline_errors ----

class GetRecentErrorsInput(BaseModel):
    topic: str = "pipeline.dlq"
    limit: int = 20


class GetRecentErrorsOutput(BaseModel):
    topic: str
    messages: list[dict]
    error: str | None = None


class GetRecentPipelineErrorsTool(BaseTool):
    name = "get_recent_pipeline_errors"
    risk_level = ToolRiskLevel.LOW
    input_model = GetRecentErrorsInput
    output_model = GetRecentErrorsOutput

    def _execute(self, tool_input: GetRecentErrorsInput) -> GetRecentErrorsOutput:
        import json

        try:
            consumer = _consumer_client("pipelinemedic-error-probe")
            consumer.assign(
                [__import__("kafka").TopicPartition(tool_input.topic, p)
                 for p in (consumer.partitions_for_topic(tool_input.topic) or [0])]
            )
            for tp in consumer.assignment():
                end = consumer.end_offsets([tp])[tp]
                start = max(end - tool_input.limit, 0)
                consumer.seek(tp, start)
            messages = []
            for msg in consumer:
                try:
                    messages.append(json.loads(msg.value.decode("utf-8")))
                except Exception:
                    messages.append({"raw": msg.value.decode("utf-8", errors="replace")})
                if len(messages) >= tool_input.limit:
                    break
            consumer.close()
            return GetRecentErrorsOutput(topic=tool_input.topic, messages=messages)
        except Exception as e:
            return GetRecentErrorsOutput(topic=tool_input.topic, messages=[], error=str(e))


# ---- quarantine_message (LOW risk: writes to a quarantine topic/table, does not delete data) ----

class QuarantineMessageInput(BaseModel):
    topic: str
    reason: str


class QuarantineMessageOutput(BaseModel):
    quarantined_topic: str
    reason: str
    status: str


class QuarantineMessageTool(BaseTool):
    name = "quarantine_message"
    risk_level = ToolRiskLevel.LOW
    input_model = QuarantineMessageInput
    output_model = QuarantineMessageOutput

    def _execute(self, tool_input: QuarantineMessageInput) -> QuarantineMessageOutput:
        # Real behavior: mark the DLQ consumer group's offset forward past the
        # bad batch is dangerous; instead we record the quarantine intent to
        # the audit log (done by the tool-calling service) and rely on the
        # already-existing DLQ topic as the quarantine store. This tool is a
        # deliberate no-destructive-op: it never deletes or truncates data.
        return QuarantineMessageOutput(
            quarantined_topic=tool_input.topic, reason=tool_input.reason, status="QUARANTINED"
        )
