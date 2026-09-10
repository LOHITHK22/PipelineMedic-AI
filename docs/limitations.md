# Limitations

## 1. PyFlink and Airflow are now real and verified

Earlier revisions of this doc described the PyFlink job and Airflow
webserver/scheduler as scaffolded-but-unverified. Both are now brought up by
`docker compose --profile full up --build` and were verified end-to-end
against live infrastructure (not just unit tests):

- **Flink**: `flink/jobs/order_validator_job.py` runs as a real PyFlink
  DataStream job on a real JobManager/TaskManager cluster
  (`flink-jobmanager`, `flink-taskmanager`), auto-submitted on startup by the
  `flink-job-submitter` one-shot service. It consumes `orders.raw` via
  `KafkaSource` and routes to `orders.validated` / `pipeline.dlq` via
  `KafkaSink`, verified by producing a valid and a poison test message
  directly to `orders.raw` and observing them land in the correct output
  topic (see "How to exercise it" below). It also processes the live
  `producer` container's continuous traffic, not just hand-fed test events.
- **Airflow**: `airflow-webserver` and `airflow-scheduler` run against a
  dedicated `airflow` database in the shared Postgres instance
  (`infra/postgres-init/001-create-airflow-db.sql`). `order_data_quality_dag`
  is picked up by the scheduler (`airflow dags list` shows it) and was
  triggered via the REST API, completing with `state: success`. The failure
  path (`scripts/inject_airflow_failure.py` renaming `orders.customer_id` ->
  `orders.customerId`) was also exercised live: the triggered run's
  `check_orders_schema` task failed and `check_orders_completeness` reported
  `upstream_failed`; after repairing the column name, clearing the run via
  the REST API produced a `success` re-run.
- These are wired into detection, not just reachable by hand:
  `backend/app/services/pipeline_monitor.py` runs `flink-health-monitor` and
  `airflow-health-monitor` background threads that poll the real Flink
  JobManager REST API (`get_flink_job_status`) and real Airflow REST API
  (`get_airflow_dag_status`) every ~20s and raise real `FLINK_FAILURE` /
  `AIRFLOW_FAILURE` incidents into the same agent graph used by the
  Kafka-side detectors. This was observed live: injecting the Airflow schema
  failure produced a real `AIRFLOW_FAILURE` incident in `GET /incidents`
  within one poll cycle, with no Flink/Airflow-specific code path skipped.
  If Flink/Airflow are unreachable (e.g. running only the core profile),
  these threads log a no-op each cycle rather than raising false incidents
  -- this is the "mock mode" fallback referred to elsewhere in this repo.

### What was actually broken, and the fixes

Two real bugs blocked PyFlink job submission, now fixed in
`flink/Dockerfile` and `flink/jobs/order_validator_job.py`:

1. **Wrong Kafka connector jar.** `flink-sql-connector-kafka` only relocates
   classes for the Table/SQL API; it does not provide the DataStream
   `KafkaSource`/`KafkaSink` classes. Fixed by adding the plain
   `flink-connector-kafka` jar (+ its `kafka-clients` dependency) instead.
2. **Deprecated connector API.** The job originally used
   `FlinkKafkaConsumer`/`FlinkKafkaProducer`, which were removed from Flink
   1.17+. Rewritten to use the modern `KafkaSource`/`KafkaSink` builder API.
3. **File permissions.** Docker's `ADD <url>` creates files as `root:root`
   mode `0600`; the Flink image runs as the non-root `flink` user, which
   silently could not read the added jars -- this manifested as a confusing
   `ClassNotFoundException` at job-graph initialization, not a permissions
   error. Fixed with an explicit `chown`/`chmod` in the Dockerfile.
4. **Airflow DB var name mismatch.** The DAG and
   `scripts/inject_airflow_failure.py` read `PIPELINEMEDIC_DB_DSN` (a libpq
   DSN), but docker-compose only set `PIPELINEMEDIC_DB_URL` (a SQLAlchemy
   URL) -- so the DAG would have failed to connect regardless of the
   webserver coming up. Fixed by setting `PIPELINEMEDIC_DB_DSN` in
   `docker-compose.yml`'s `x-airflow-common` block.
5. **Missing `orders` table and missing `psycopg2`.** Nothing created the
   `orders` table the DAG validates, and the stock Airflow image has no
   Postgres driver. Fixed with
   `infra/postgres-init/002-create-orders-table.sql` (seed rows so the
   completeness check passes on a fresh start) and
   `_PIP_ADDITIONAL_REQUIREMENTS: psycopg2-binary==2.9.9` on the Airflow
   services.

### How to exercise it

```bash
docker compose --profile full up -d --build
# Flink UI: http://localhost:8081 -- job "pipelinemedic-order-validator" should be RUNNING
# Airflow UI: http://localhost:8080 (admin/admin) -- order_data_quality_dag should be unpaused after the first schedule tick

# Feed a valid + a poison record straight into orders.raw and watch Flink route them:
docker compose --profile full exec -T kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 --topic orders.raw <<'EOF'
{"event_id":"e1","order_id":"o1","customer_id":"c1","amount":9.99,"currency":"USD","event_time":"2026-01-01T00:00:00Z"}
{"event_id":"e2","order_id":"o2"}
EOF
docker compose --profile full exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic orders.validated --from-beginning --timeout-ms 5000
docker compose --profile full exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic pipeline.dlq --from-beginning --timeout-ms 5000

# Break + repair the Airflow DAG's data source, and watch PipelineMedic AI raise + resolve an AIRFLOW_FAILURE incident:
python scripts/inject_airflow_failure.py --dsn "dbname=pipelinemedic user=pipelinemedic password=pipelinemedic host=localhost port=5433"
curl -s -u admin:admin -X POST http://localhost:8080/api/v1/dags/order_data_quality_dag/dagRuns -d '{}'
sleep 20
curl -s http://localhost:8000/incidents | python -m json.tool   # look for AIRFLOW_FAILURE
```

**Note on double-processing:** when the real Flink job is running, disable
the backend's Python fallback stream-validator (it would otherwise also
consume `orders.raw` under a different consumer group and double-write
`orders.validated`/`pipeline.dlq`):

```bash
PIPELINE_MONITOR_STREAM_VALIDATOR_ENABLED=false docker compose --profile full up -d --build
```

### Python fallback (lighter-weight alternative)

`backend/app/services/pipeline_monitor.py`'s `stream-validator` thread
remains available and is the default (`PIPELINE_MONITOR_STREAM_VALIDATOR_ENABLED`
defaults to `true`) for constrained environments -- e.g. CI, or a machine
where bringing up the full Flink/Airflow stack isn't practical. It implements
identical validation rules to the Flink job and requires only the core
`docker compose up` profile (postgres, kafka, backend, producer). Running
`--profile full` with the flag left at its default will double-write
`orders.validated`/`pipeline.dlq` -- harmless for the demo (duplicate,
identical records) but not how it should be run in earnest.

## 2. OpenAI / Azure OpenAI providers uncredentialed

`app/llm/provider.py` implements both against real structured-output APIs
(`client.beta.chat.completions.parse` with a Pydantic `response_format`).
Neither was exercised in this session (no API key available). Set
`OPENAI_API_KEY` and `LLM_PROVIDER=openai` (or the Azure equivalents) to use
them; the rest of the system (detectors, policy engine, tools, API) is
provider-agnostic and requires no changes.

## 3. Grafana dashboard

Not built (explicitly a nice-to-have in the task). `infra/prometheus/`
contains a working scrape config; any Grafana instance pointed at it can
graph `pipeline_incidents_total`, `pipeline_repairs_total`,
`kafka_consumer_lag`, etc. immediately.
