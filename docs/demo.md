# Demo script

```bash
cp .env.example .env
make up
sleep 15

# Scenario 1: schema drift (customer_id -> customerId, amount number -> string)
make inject-schema-drift
curl -s http://localhost:8000/incidents | python -m json.tool

# Grab the incident id from the output above, then:
INCIDENT_ID=<paste id>
curl -s http://localhost:8000/incidents/$INCIDENT_ID | python -m json.tool   # see plan + risk + AWAITING_APPROVAL
curl -s -X POST http://localhost:8000/incidents/$INCIDENT_ID/approve \
  -H 'Content-Type: application/json' -d '{"decided_by":"you","reason":"looks safe"}'
curl -s http://localhost:8000/incidents/$INCIDENT_ID | python -m json.tool   # RESOLVED, with executions + validations

# Scenario 2: poison messages
make inject-poison
curl -s http://localhost:8000/incidents | python -m json.tool

# Scenario 3: consumer lag (needs the lag-monitor's ~10s poll cycle to notice)
make inject-lag
sleep 20
curl -s http://localhost:8000/incidents | python -m json.tool

# Metrics
curl -s http://localhost:8000/metrics | grep pipeline_

# Reset between demo runs
make reset-demo
```

Note: `inject-lag` sends 10,000 messages by default -- override with
`python scripts/inject_lag.py --count 2000` for a faster demo loop.

## Scenario 4 & 5: real Flink and Airflow (the `full` profile)

The scenarios above run against the core profile (postgres, kafka, backend,
producer), which uses the in-process Python fallback validator instead of a
real Flink cluster, and doesn't bring up Airflow at all. To exercise the real
infrastructure paths (see `docs/limitations.md` for what was fixed to make
these work):

```bash
PIPELINE_MONITOR_STREAM_VALIDATOR_ENABLED=false docker compose --profile full up -d --build
# Flink UI:   http://localhost:8081  (job "pipelinemedic-order-validator" -> RUNNING)
# Airflow UI: http://localhost:8080  (admin/admin; order_data_quality_dag)
```

**Scenario 4 -- real Flink poison/valid routing:**

```bash
docker compose --profile full exec -T kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 --topic orders.raw <<'EOF'
{"event_id":"e1","order_id":"o1","customer_id":"c1","amount":9.99,"currency":"USD","event_time":"2026-01-01T00:00:00Z"}
{"event_id":"e2","order_id":"o2"}
EOF
docker compose --profile full exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic orders.validated --from-beginning --timeout-ms 5000
docker compose --profile full exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic pipeline.dlq --from-beginning --timeout-ms 5000
```

The valid record appears on `orders.validated`; the incomplete one appears
on `pipeline.dlq` tagged `MISSING_FIELDS`, routed there by the real Flink
job, not the Python fallback.

**Scenario 5 -- real Airflow DAG failure/repair (Scenario B):**

```bash
python scripts/inject_airflow_failure.py \
  --dsn "dbname=pipelinemedic user=pipelinemedic password=pipelinemedic host=localhost port=5433"
curl -s -u admin:admin -X POST http://localhost:8080/api/v1/dags/order_data_quality_dag/dagRuns -d '{}'
sleep 20
curl -s http://localhost:8000/incidents | python -m json.tool   # AIRFLOW_FAILURE incident, raised by
                                                                  # the real airflow-health-monitor thread
```

Repair it and confirm recovery:

```bash
docker compose --profile full exec -T postgres psql -U pipelinemedic -d pipelinemedic \
  -c 'ALTER TABLE orders RENAME COLUMN "customerId" TO customer_id;'
curl -s -u admin:admin -X POST http://localhost:8080/api/v1/dags/order_data_quality_dag/dagRuns -d '{}'
sleep 10
curl -s -u admin:admin "http://localhost:8080/api/v1/dags/order_data_quality_dag/dagRuns?order_by=-execution_date&limit=1"
```
