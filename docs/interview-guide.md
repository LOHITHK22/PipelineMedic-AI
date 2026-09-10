# Interview guide

**Why Kafka?** Durable, replayable, per-partition-ordered log semantics are
required to make lag a first-class measurable metric and to let DLQ/poison
scenarios be reproduced deterministically for a demo.

**Why Flink?** Stateful, low-latency, backpressure-aware stream processing
is what a real order-validation layer needs, and `change_flink_parallelism`
is a genuine lever for repairing a lag incident that a batch job can't
offer. (PyFlink itself wasn't run this session -- see limitations.)

**Why deterministic detection before the LLM?** So the trigger for any
action is always a directly measured fact (a diff, a threshold, a task
state), never an LLM guess. This bounds hallucination risk to the
*explanation and plan* stage, which is independently risk-scored and
gated, not the *decision that something is wrong* stage.

**Why LangGraph (conceptually), and why this repo hand-rolls the state
machine instead?** LangGraph's value is typed state + explicit
nodes/edges + durable interrupts for human-in-the-loop. This project's
graph topology is simple enough (one branch, one loop-back) that a ~250
line hand-rolled version gives the same properties with full control over
how the pause is persisted (a real Postgres row, not an opaque
checkpointer blob) -- important for a project whose thesis is auditability.
Swapping in real LangGraph is a mechanical follow-up.

**Why MCP-style tools instead of raw function calls?** Typed input/output
schemas, a declared risk tier, and audited execution are the parts of MCP
that matter for safety; the wire protocol is separable infrastructure. See
`docs/mcp-tools.md`.

**Hallucination control?** Structured Pydantic outputs only (never
regex-parsed text); tool inputs re-validated against each tool's own
schema at execution time; unknown tool names fail hard; deterministic
policy engine re-derives risk independently of the LLM's self-reported
risk level.

**Why human approval for medium/high risk?** Because the blast radius of
those actions (restarting a Flink job, rerunning an Airflow task, changing
parallelism) is real production impact, and the cost of a 30-second human
click is far lower than the cost of an autonomous agent taking a wrong
action on infrastructure it doesn't have complete visibility into.

**Rollback mechanics?** Each `RepairAction` may declare a
`rollback_tool_name`/`rollback_input`. If independent post-execution
validation fails, `_execute_and_validate` walks the plan's actions in
reverse and invokes each declared rollback tool, then marks the incident
`ROLLED_BACK` (not `FAILED`, since rollback itself succeeded).

**Idempotency?** Execution is keyed by `exec:<incident_id>`
(`RepairExecution.idempotency_key`, unique-constrained); a second approval
or retry for the same incident is a no-op if a `SUCCESS` execution already
exists. Detection-side, `dedup_key` (content hash of type + component +
discriminator) prevents duplicate incidents for the same underlying
condition.

**Scaling to 1000 pipelines?** Partition detectors and the agent worker
horizontally by `source_component` hash (Kafka-native fan-out via consumer
groups already does this for the stream validator). The policy engine and
tool layer are stateless and trivially horizontally scalable; the only
shared state is Postgres, which would need read replicas + connection
pooling (pgbouncer) at that scale, and the audit_log table would need
partitioning by time.

**Kubernetes deployment?** Backend as a Deployment behind a Service (stateless,
horizontally scalable); the stream-validator role moves to a real Flink
job running as a Flink Kubernetes Operator `FlinkDeployment`; Airflow via
the official Helm chart; Kafka via Strimzi or a managed service; Postgres
via a managed service or an operator (CloudNativePG). Secrets via
Kubernetes Secrets + an external secrets operator (see credential security
below), not baked into images or Helm values files.

**Credential security?** No credentials in code or committed files
(`.env.example` has none); in Kubernetes, mount via Secrets sourced from a
vault (External Secrets Operator + AWS Secrets Manager/Vault); the
backend's own DB/Kafka/Airflow credentials should be scoped to
least-privilege service accounts distinct from any interactive/admin
credentials, and the LLM API key should never appear in a prompt sent
back to the LLM (it doesn't -- it's used only for the outbound HTTP
Authorization header in `app/llm/provider.py`).

**Preventing agent-caused outages?** The five layers in
`docs/safety-model.md`: deterministic triggers, structured outputs,
independent risk engine, hard-coded CRITICAL block, human approval gate,
independent validation, and rollback. Additionally: idempotent execution
prevents retry storms, and every tool call is audited so a bad actor
pattern is detectable after the fact even if not prevented in the moment.

**LLM unavailability fallback?** `LLM_PROVIDER=mock` is not just a test
double -- it's a legitimate deterministic fallback mode: if the real
provider's API is down, an operator can flip `LLM_PROVIDER=mock` and the
system continues detecting and (with a pattern-matched, less nuanced but
still structured) diagnosing and planning known incident types, keeping
auto-execute-eligible repairs (e.g. quarantining poison messages) flowing
without a human in the loop for a total outage of the LLM vendor.

**Schema registry integration?** `backend/app/detectors/schema_drift.py`'s
`diff_schemas`/`SchemaDriftDetector` API is intentionally registry-agnostic
-- it operates on a flat `{field: {type, nullable}}` dict. Swapping the
in-house `schema_versions` Postgres table for Confluent Schema Registry (or
AWS Glue Schema Registry) is a matter of writing an adapter that fetches
Avro/JSON-Schema definitions and flattens them into that same dict shape;
the compatibility classification logic itself doesn't change.

**Extensibility to Databricks/Spark/Snowflake?** The detector/tool/policy
architecture doesn't assume Kafka+Flink+Airflow specifically: a
`DatabricksJobFailureDetector` or `SnowflakeQueryFailureDetector` would
follow the exact same `build_incident(...)` contract
(`backend/app/detectors/base.py`), and new tools
(`get_databricks_job_run_status`, `rerun_snowflake_task`) follow the exact
same `BaseTool` contract with a declared risk tier in
`autonomy_policy.yaml`. Nothing in `agents/graph.py` or `policies/engine.py`
needs to change to support new detector/tool sources.
