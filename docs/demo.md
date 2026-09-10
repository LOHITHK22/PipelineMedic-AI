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
