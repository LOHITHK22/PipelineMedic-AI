# Limitations

## 1. PyFlink job not submitted/run in this session

**What's blocked:** `flink/jobs/order_validator_job.py` is real, complete
PyFlink DataStream code, and `flink/Dockerfile` builds a Flink image with
`apache-flink` and the Kafka connector jar installed. It was not actually
submitted to a running JobManager and verified in this session.

**Why:** PyFlink requires an exact-matching chain of (Flink minor version) x
(apache-flink pip version) x (Python version) x (Kafka connector jar
version); mismatches fail at job-submission time with often-unclear
py4j/JVM errors, and iterating on that chain reliably takes real wall-clock
time. After validating the full agent/detection/approval/execution/audit
system end-to-end against real Kafka and Postgres (the harder, more
valuable part of this build), the remaining session budget was spent on
docs and other Docker Compose services rather than PyFlink version
spelunking.

**What was completed instead:** `backend/app/services/pipeline_monitor.py`
implements the identical validation rules (`REQUIRED_FIELDS` check, type
check on `amount`) as a background thread inside the FastAPI backend
process, consuming `orders.raw` and producing to `orders.validated` /
`pipeline.dlq`. This is what actually powered the live, verified
schema-drift and poison-message detection runs described in the README.

**Exact commands to finish it locally:**
```bash
docker build -t pipelinemedic-flink ./flink
# then in docker-compose.yml, change flink-jobmanager/flink-taskmanager's
# `image: flink:1.18-scala_2.12-java11` to `image: pipelinemedic-flink`
docker compose --profile full up -d flink-jobmanager flink-taskmanager
docker compose --profile full exec flink-jobmanager \
  flink run -py /opt/flink/jobs/order_validator_job.py
# Watch http://localhost:8081 (Flink UI) for job status.
# Once confirmed running, stop backend's PipelineMonitor stream-validator
# thread (leave the lag-monitor thread running) to avoid double-consuming
# orders.raw with two competing consumer groups.
```

## 2. Airflow not brought fully up in this session

**What's blocked:** `docker-compose.yml`'s `full` profile defines
`airflow-init`, `airflow-webserver`, `airflow-scheduler` using the official
`apache/airflow:2.9.3` image against a dedicated `airflow` Postgres
database (created by `infra/postgres-init/001-create-airflow-db.sql`). This
was not run to completion (Airflow's DB migration + webserver cold start
is multi-minute and CPU-heavy) in this session.

**Why:** Time budget was prioritized toward verifying the core
detect-diagnose-approve-execute-validate loop against real infrastructure,
which is the harder and more central claim of this project.

**What was completed instead:** `airflow/dags/order_data_quality_dag.py` is
a real, standard-pattern Airflow DAG (two `PythonOperator` tasks doing
schema + completeness checks against the shared Postgres `orders` table).
`scripts/inject_airflow_failure.py` really renames a column to break it.
`AirflowFailureDetector`, `GetAirflowDagStatusTool`,
`GetAirflowTaskLogsTool`, and `RerunAirflowTaskTool` are implemented against
the real Airflow REST API and unit-tested for their detection/risk logic;
they were not exercised against a live Airflow instance.

**Exact commands to finish it locally:**
```bash
docker compose --profile full up -d airflow-init
docker compose --profile full up -d airflow-webserver airflow-scheduler
# Open http://localhost:8080 (admin/admin), unpause order_data_quality_dag
python scripts/inject_airflow_failure.py   # breaks the schema
# trigger the DAG from the UI or: curl -u admin:admin -X POST \
#   http://localhost:8080/api/v1/dags/order_data_quality_dag/dagRuns -d '{}'
# PipelineMedic AI's AirflowFailureDetector needs to be wired to poll this
# DAG's task instances (currently the demo focuses on the Kafka-side
# detectors, which run automatically); see app/detectors/airflow_failure.py
# for the detection contract it expects.
```

## 3. OpenAI / Azure OpenAI providers uncredentialed

`app/llm/provider.py` implements both against real structured-output APIs
(`client.beta.chat.completions.parse` with a Pydantic `response_format`).
Neither was exercised in this session (no API key available). Set
`OPENAI_API_KEY` and `LLM_PROVIDER=openai` (or the Azure equivalents) to use
them; the rest of the system (detectors, policy engine, tools, API) is
provider-agnostic and requires no changes.

## 4. Grafana dashboard

Not built (explicitly a nice-to-have in the task). `infra/prometheus/`
contains a working scrape config; any Grafana instance pointed at it can
graph `pipeline_incidents_total`, `pipeline_repairs_total`,
`kafka_consumer_lag`, etc. immediately.
