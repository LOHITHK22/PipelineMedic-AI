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
- 50/50 pytest tests pass, including true DB-backed end-to-end tests
  (`backend/app/tests/test_e2e.py`, `backend/app/tests/test_advanced_features.py`)
  exercising the full lifecycle described above, plus event correlation,
  canary remediation, and LLM cost protection (see below), via the same
  service functions the API uses.

**Event correlation, canary remediation, and LLM cost protection** were
added on top of the above and verified against the same live stack, not just
unit-tested:
- Running `scripts/inject_schema_drift.py --record-deployment-event` against
  the live stack produced a real incident whose `GET
  /incidents/{id}/correlation` correctly surfaced the synthetic `DEPLOYMENT`
  `system_events` row recorded moments earlier as the plausible trigger, and
  whose diagnosis `reasoning` text referenced it directly.
- Approving that (MEDIUM-risk) incident produced the real event sequence
  `CANARY_STARTED -> CANARY_VALIDATION_PASSED -> CANARY_EXPANDED ->
  REPAIR_COMPLETED -> ... -> INCIDENT_RESOLVED` at `GET /incidents/{id}`,
  confirming the canary gate actually runs before full validation (see
  `docs/safety-model.md` for what "canary" honestly means here -- a
  re-measurement gate, not literal traffic splitting).
- `GET /metrics/llm-usage` reflected real, non-deduplicated `diagnose`/
  `generate_repair_plan` invocations and estimated token counts for that run.
  Duplicate-incident deduplication (skipping a repeat LLM call for an
  identical evidence signature within the dedup window) is covered by a real
  DB-backed test (`test_duplicate_incident_signature_skips_second_llm_call`)
  rather than the live stack, because the pre-existing poison-message
  detector's `dedup_key` is not time-varying and collides with itself on a
  second live injection after the first incident resolves -- a pre-existing
  bug unrelated to this feature, flagged separately, not fixed here.

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
- `GET /health/pipeline` — live Flink JobManager + Airflow REST API health (not
  derived from incident severity): `{overall, flink: {state, status, ...},
  airflow: {latest_run_state, status, ...}}` where `status` is
  `HEALTHY`/`DEGRADED`/`UNKNOWN` per component (`UNKNOWN` means that
  component isn't running in this profile, not that it's broken)
- `GET /incidents`, `GET /incidents/{id}` (includes plans, approvals, executions, validations, and `incident_events` timeline)
- `GET /incidents/{id}/similar` (fuzzy incident-memory retrieval)
- `GET /incidents/{id}/correlation` — deterministic event correlation: `SystemEvent`s (schema version bumps, synthetic deployment markers, config changes) and prior incidents within a time window that plausibly triggered this one (see `docs/agent-design.md`)
- `POST /incidents/{id}/approve`, `POST /incidents/{id}/reject`
- `GET /approvals`
- `GET /audit` (chronological SYSTEM/AGENT/HUMAN audit trail)
- `GET /tools` (MCP-style tool catalog with risk levels and JSON schemas)
- `GET /metrics/llm-usage` — LLM invocation/token-estimate audit trail plus how many calls were skipped via duplicate-incident deduplication (see `docs/safety-model.md` "Cost protection")
- `GET /approve-link/{token}` — read-only lookup of a signed, single-use, time-limited approve-link token: returns the incident summary/diagnosis/plan without deciding anything, or `{valid: false, error: "expired"|"invalid"|"already_used"|"already_decided"}` (see `docs/safety-model.md` "Approve-link security model")
- `POST /approve-link/{token}/decide` — `{decision: "approve"|"reject", reason?}`; validates + consumes the token, then calls the exact same service-layer approve/reject logic as `POST /incidents/{id}/approve`/`reject`
- `POST /incidents/{id}/ask` — `{question}` -> `{answer}`; deterministic natural-language Q&A over an incident's already-computed diagnosis/repair-plan, **not** a new agentic surface (never calls an LLM or a tool — see `docs/safety-model.md` "Ask about this incident")

CORS is enabled permissively (`allow_origins=["*"]`) in `backend/app/main.py`
for local development only, so the dashboard (a separately served React app)
can call the API from a different origin. This is not appropriate for
production and is intentionally excluded from the "production boundary"
hardening scope.

## Dashboard

A React + TypeScript + Vite dashboard lives in `dashboard/`. It polls the
live API (every 6-8s) and renders:

- **Overview** — real Flink/Airflow health from `GET /health/pipeline`, active/resolved incident counts, pending approvals, repair success rate
- **Incidents** — filterable/sortable incident list
- **Incident Detail** — evidence, diagnosis + confidence, repair plan(s), risk level, approval status, execution result, validation result, and the incident_events timeline (populated for every incident type — see "Incident lifecycle events" below)
- **Approvals** — Approve/Reject wired to the real endpoints (Reject requires a typed reason)
- **Audit Log** — chronological SYSTEM/AGENT/HUMAN feed
- **Incident Detail -> Ask about this incident** — a text box that answers plain-English questions using only that incident's already-computed diagnosis/plan (no new LLM call)
- **/approve-link/:token** (standalone page, no dashboard chrome) — the page a notification link lands on: incident summary + real Approve/Reject buttons wired to the token-based decide endpoint, and a clear expired/used/invalid state

Run it standalone against a running backend:

```bash
cd dashboard
npm install
npm run dev          # http://localhost:5173, talks to http://localhost:8000 by default
```

Or as part of the compose stack:

```bash
docker compose up -d --build dashboard   # http://localhost:4173
```

Set `VITE_API_BASE_URL` (dev, via `dashboard/.env`) or `DASHBOARD_API_BASE_URL`
(compose build arg) to point at a different backend URL.

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
dashboard/      React + TypeScript + Vite operator dashboard (see below)
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
