# PipelineMedic AI

An autonomous data-pipeline repair agent. It watches an order-processing
pipeline (Kafka -> [Flink] -> PostgreSQL, plus an Airflow data-quality DAG),
deterministically detects real failure modes (schema drift, poison messages,
consumer lag, Airflow task failures), asks a structured-output LLM to
diagnose root cause and propose a typed repair plan, scores the plan's risk
with a deterministic policy engine, auto-executes low-risk repairs or pauses
for durable human approval on medium/high risk ones, executes repairs
through typed MCP-style tools, independently validates recovery, rolls back
on failure, and records everything to a full Postgres audit trail.

```mermaid
flowchart LR
    subgraph Pipeline
        P[producer] --> K1[(orders.raw)]
        K1 --> V[stream validator\n(Flink job / Python fallback)]
        V --> K2[(orders.validated)]
        V --> DLQ[(pipeline.dlq)]
        K2 --> PG[(Postgres: orders)]
        AF[Airflow DAG\norder_data_quality_dag] --> PG
    end

    subgraph PipelineMedic AI
        MON[PipelineMonitor\ndetectors] --> AG[Agent graph]
        AG --> LLM[LLM provider\n(mock/openai/azure)]
        AG --> POL[Policy engine]
        POL -->|LOW risk| EXE[Execute tools]
        POL -->|MED/HIGH risk| APR[Persisted ApprovalRequest]
        APR -->|human approves via API| EXE
        EXE --> TOOLS[MCP-style tools]
        TOOLS --> VAL[Validate]
        VAL -->|healthy| RES[Resolved]
        VAL -->|unhealthy| RB[Rollback]
    end

    K1 -.observed by.-> MON
    DLQ -.observed by.-> MON
    AF -.observed by.-> MON
    TOOLS -.audited to.-> AUDIT[(audit_log)]
```

## What's real vs. what's documented-as-limitation

This is a from-scratch build, verified running with `docker compose up
--build` in this environment. Everything below was actually executed, not
just written:

- Kafka (KRaft, `apache/kafka:3.7.0`), Postgres 16, FastAPI backend, and the
  order producer all build and run via `docker compose up --build -d
  postgres kafka kafka-topic-init backend producer`.
- The producer emits realistic order events and supports `SCENARIO=normal|
  schema_drift|poison|lag`.
- `scripts/inject_schema_drift.py` and `scripts/inject_poison_message.py`
  were run against the live stack and produced real detected incidents,
  visible at `GET /incidents`, that were diagnosed by the deterministic mock
  LLM, planned, risk-scored to `APPROVAL_REQUIRED`, approved via `POST
  /incidents/{id}/approve`, executed via the `quarantine_message` tool,
  independently validated, and marked `RESOLVED` -- end to end, against
  real Kafka and Postgres, not mocked.
- `/metrics` (Prometheus) reflects real counters incremented by that run.
- 31/31 pytest tests pass, including a true DB-backed end-to-end test
  (`backend/app/tests/test_e2e.py`) exercising the full lifecycle described
  above via the same service functions the API uses.

**Flink and Airflow are real and verified too** (see `docs/limitations.md`
for the exact bugs found and fixed, and `docs/demo.md` for the commands):

- **PyFlink job** (`flink/jobs/order_validator_job.py`): runs as a real
  DataStream job (`KafkaSource` -> validate -> `KafkaSink` to
  `orders.validated`/`pipeline.dlq`) on a real Flink JobManager/TaskManager
  cluster (`docker compose --profile full up --build`), auto-submitted by
  the `flink-job-submitter` service. Verified by feeding a valid and a
  poison record into `orders.raw` and confirming they land in
  `orders.validated` and `pipeline.dlq` respectively, plus continuously
  processing the live `producer` container's traffic.
- **Airflow**: `airflow-webserver` + `airflow-scheduler` run against a
  dedicated `airflow` Postgres database; `order_data_quality_dag` is picked
  up by the scheduler and was triggered via the REST API end to end,
  including the failure path (`scripts/inject_airflow_failure.py` breaking
  the `orders` table's schema, the DAG run failing as expected, repairing
  it, and a rerun succeeding). `backend/app/services/pipeline_monitor.py`
  polls the real Flink and Airflow REST APIs on background threads and
  raises real `FLINK_FAILURE`/`AIRFLOW_FAILURE` incidents into the same
  agent graph -- observed live producing an `AIRFLOW_FAILURE` incident at
  `GET /incidents` from the injected failure above.
- Set `PIPELINE_MONITOR_STREAM_VALIDATOR_ENABLED=false` when running the
  `full` profile so the Python fallback validator doesn't also consume
  `orders.raw` alongside the real Flink job (it remains the default,
  lighter-weight path for environments that only run the core profile).
- **LLM providers other than mock**: `openai`/`azure_openai` are fully
  coded against structured outputs but require API keys not available here;
  only `LLM_PROVIDER=mock` is verified end-to-end (as the task required).

## Quick start

```bash
cp .env.example .env
make up              # postgres, kafka, backend, producer (core stack)
# wait ~15s for kafka + backend to be ready
make inject-schema-drift
curl http://localhost:8000/incidents
curl -X POST http://localhost:8000/incidents/<id>/approve -d '{"decided_by":"you"}' -H 'Content-Type: application/json'
curl http://localhost:8000/incidents/<id>   # see full lifecycle: plan, approval, execution, validation
open http://localhost:8000/                 # minimal status page
open http://localhost:8000/metrics          # Prometheus metrics

make up-full          # also attempts Flink + Airflow containers (see docs/limitations.md)
make down             # tear everything down
```

Postgres is published on host port **5433** (not 5432) to avoid colliding
with any local Postgres install; inside the Docker network services still
use the standard `postgres:5432`.

## API surface

- `GET /health`, `GET /metrics`, `GET /` (status page)
- `GET /incidents`, `GET /incidents/{id}`
- `POST /incidents/{id}/approve`, `POST /incidents/{id}/reject`
- `GET /approvals`
- `GET /tools` (MCP-style tool catalog with risk levels and JSON schemas)

## Repository layout

```
backend/app/
  api/          FastAPI routers
  agents/       typed AgentState + the persisted agent state machine (graph.py)
  detectors/    deterministic detectors (schema drift, lag, airflow, flink, data quality)
  models/       shared Pydantic schemas (Incident, DiagnosisResult, RepairPlan, ...)
  services/     pipeline_monitor.py (background detection + Flink fallback)
  tools/        MCP-style typed tools + registry + hard-blocked CRITICAL tools
  policies/     autonomy_policy.yaml + policy engine
  db/           SQLAlchemy models
  llm/          provider abstraction (mock / openai / azure_openai)
  observability/ structured logging + Prometheus metrics
  tests/        pytest unit + e2e tests
producer/       Kafka order-event producer with fault-injection scenarios
flink/jobs/     real PyFlink job (see limitations)
airflow/dags/   real Airflow DAG
scripts/        fault injection + reset scripts
docs/           architecture, agent design, MCP tools, safety model, demo, interview guide
```

## Interview talking points

See `docs/interview-guide.md` for full answers on: why Kafka/Flink, why
deterministic detection before LLM, why LangGraph-style state (and why this
repo hand-rolls it instead), why MCP-style tools, hallucination control, why
human approval, rollback mechanics, idempotency, scaling to 1000 pipelines,
Kubernetes deployment, credential security, preventing agent-caused
outages, LLM unavailability fallback, schema registry integration, and
extensibility to Databricks/Spark/Snowflake.
