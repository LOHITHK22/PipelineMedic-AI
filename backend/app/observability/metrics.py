from prometheus_client import Counter, Gauge, Histogram

PIPELINE_INCIDENTS_TOTAL = Counter(
    "pipeline_incidents_total", "Total incidents detected", ["incident_type"]
)
PIPELINE_REPAIRS_TOTAL = Counter("pipeline_repairs_total", "Total repair executions attempted")
PIPELINE_REPAIRS_SUCCESS_TOTAL = Counter("pipeline_repairs_success_total", "Total repair executions that resolved the incident")
PIPELINE_REPAIRS_FAILED_TOTAL = Counter("pipeline_repairs_failed_total", "Total repair executions that failed or were rolled back")
APPROVAL_WAIT_SECONDS = Histogram("approval_wait_seconds", "Time an incident spent waiting for human approval")
AGENT_DIAGNOSIS_DURATION = Histogram("agent_diagnosis_duration_seconds", "Time spent producing an LLM diagnosis")
KAFKA_CONSUMER_LAG = Gauge("kafka_consumer_lag", "Last observed consumer lag", ["topic", "group_id"])
PIPELINE_ERROR_RATE = Gauge("pipeline_error_rate", "Fraction of recent messages routed to DLQ", ["topic"])
