# Safety model

## Layers

1. **Deterministic detection.** Incidents are never LLM-hallucinated; every
   one traces back to a directly measured condition (a schema diff, a lag
   number vs. threshold, a DLQ classification, a task state).
2. **Structured LLM output.** Diagnoses and plans are typed Pydantic models,
   constraining what the LLM can even express (no shell commands, no SQL,
   no free-form "just run this" instructions -- only `tool_name` +
   `tool_input` pairs that must validate against that tool's own schema).
3. **Deterministic risk/policy engine.** `assess_risk` in
   `backend/app/policies/engine.py` is the sole authority on
   auto-execute/approval/blocked, driven by `autonomy_policy.yaml`, and it
   ignores the LLM's own self-reported risk level as anything but a
   secondary check -- the real decision is the max of (a) each planned
   tool's configured risk and (b) the incident's severity floor.
4. **Hard-coded CRITICAL block.** Five action *names* are permanently
   unimplemented and refused in code (`backend/app/tools/registry.py`),
   regardless of policy file contents: `drop_database_table`,
   `truncate_topic`, `delete_dag`, `run_arbitrary_sql`, `run_shell_command`.
5. **Durable human approval.** Anything MEDIUM/HIGH risk pauses as a real
   Postgres row; nothing executes until `POST /incidents/{id}/approve`.
6. **Independent validation.** `validate_pipeline_health` re-measures state
   after execution; a tool call returning HTTP 200 is never treated as proof
   the pipeline is actually healthy.
7. **Rollback.** Any plan action with a declared `rollback_tool_name` is
   rolled back, in reverse order, if validation fails.
8. **Idempotent execution.** Keyed by `exec:<incident_id>`; retries and
   double-approvals cannot double-execute a repair.
9. **Full audit trail.** Every tool call (success or failure), every human
   decision, and every state transition is written to `audit_log` with
   `correlation_id`/`incident_id`.

## What this does NOT protect against

- A compromised or buggy detector emitting false incidents at high volume
  (a DoS on the approval queue). Mitigation path: rate-limit incident
  creation per `source_component` (not implemented in this session).
- A compromised LLM API key/endpoint returning adversarial structured
  output. Mitigation: structured output still can't reference
  unregistered tool names (an unknown `tool_name` fails `get_tool()` with a
  `KeyError` at execution time) or unimplemented CRITICAL tools, but a
  malicious *MEDIUM*-risk plan could still be proposed -- this is exactly
  why MEDIUM/HIGH stays human-gated rather than being expanded to
  auto-execute over time without additional controls (anomaly detection on
  plan contents, plan diffing against historical plans, etc.).
- Credential compromise of the tool-calling identity itself (e.g. the
  backend's own Kafka/Airflow/DB credentials). Standard least-privilege
  service-account scoping applies; see `docs/interview-guide.md` "credential
  security".
