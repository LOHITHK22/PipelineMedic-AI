# Architecture

## Components

- **producer** (`producer/produce_orders.py`): emits realistic order events
  to `orders.raw`, with `SCENARIO` env var support for schema drift, poison
  messages, and lag injection.
- **stream validator**: consumes `orders.raw`, validates against the
  canonical order schema, writes valid records to `orders.validated` and
  invalid ones to `pipeline.dlq`. Two implementations exist with identical
  logic: a real PyFlink job (`flink/jobs/order_validator_job.py`, not run in
  this session -- see `docs/limitations.md`) and a Python fallback that runs
  inside the backend process (`backend/app/services/pipeline_monitor.py`),
  which is what actually ran during verification.
- **Airflow DAG** (`airflow/dags/order_data_quality_dag.py`): a two-task
  data-quality DAG (schema check, completeness check) against the shared
  Postgres `orders` table.
- **backend** (FastAPI, `backend/app/`): hosts the detectors, the agent
  state machine, the MCP-style tool layer, the policy engine, the incident
  API, and Prometheus metrics.
- **Postgres**: system-of-record for incidents, plans, approvals,
  executions, validations, audit log, and schema versions (see
  `backend/app/db/models.py`).

## Why this database schema strategy (create_all, not Alembic)

We use `Base.metadata.create_all()` at startup rather than Alembic
migrations. This is a deliberate, documented trade-off (also noted as a
docstring in `backend/app/db/models.py`): the schema is single-service-owned
and pre-1.0; Alembic's value (safe, reviewable, reversible migrations across
multiple deployed versions and contributors) doesn't yet outweigh its
operational overhead for a one-service, one-contributor demo project.
`alembic init` is a 10-minute addition once either condition changes.

## Why Kafka

Kafka gives durable, replayable, ordered-per-partition event log semantics,
which is exactly what fault injection + detection needs: you can replay a
schema-drifted batch, prove poison messages actually landed in the DLQ, and
measure consumer lag as a first-class, directly observable metric rather
than an inferred one. A simple queue (SQS/RabbitMQ) would lose the replay
and lag-visibility properties detectors 2 and 3 depend on.

## Why Flink (even though PyFlink wasn't run this session)

Flink is the realistic choice for the validation/transform layer because
production order-pipelines need stateful, exactly-once, low-latency stream
processing with backpressure-aware parallelism -- the same lever
(`change_flink_parallelism`) PipelineMedic AI's repair plans actually pull
when lag incidents occur. A batch-only tool (plain cron + pandas) can't
model or repair a live consumer-lag incident, which is one of the four core
demo scenarios.

## Data flow for one incident

1. `pipeline_monitor.py` (or PyFlink) observes bad data / lag / a failed
   Airflow task and constructs a typed `Incident` (see
   `backend/app/models/schemas.py`), computing a deterministic `dedup_key`.
2. `agents/graph.py::receive_incident` persists it (or dedups against an
   existing open incident with the same key) and calls
   `_run_until_decision`.
3. `collect_context` gathers supplementary read-only tool evidence.
4. `get_llm_provider().diagnose(...)` returns a structured `DiagnosisResult`.
5. `.generate_repair_plan(...)` returns a structured `RepairPlan` (list of
   typed `RepairAction`s, each mapped 1:1 to an MCP-style tool).
6. `policies/engine.py::assess_risk` combines tool-level risk, incident
   severity, and hard-block rules into a `RiskAssessment` with an
   `AutonomyDecision`.
7. `AUTO_EXECUTE` -> immediately runs `_execute_and_validate`.
   `APPROVAL_REQUIRED` -> persists an `ApprovalRequest` row and returns;
   the incident is durably paused. `BLOCKED` -> marks the incident `FAILED`
   with the block rationale and stops.
8. A human calls `POST /incidents/{id}/approve` (or `/reject`), which is the
   only way an `APPROVAL_REQUIRED` incident resumes -- there is no `input()`
   anywhere in this codebase.
9. `_execute_and_validate` runs each planned tool call (idempotent, keyed by
   `exec:<incident_id>`), then independently re-measures pipeline health via
   `validate_pipeline_health` (never trusting the executor's own success
   flag), then marks the incident `RESOLVED` or runs each action's declared
   rollback and marks it `ROLLED_BACK`.
10. Every tool call, human decision, and state transition is written to
    `audit_log` with `correlation_id`/`incident_id` for tracing.
