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

## Canary remediation (MEDIUM/HIGH risk repairs)

Auto-executed (LOW risk) repairs go straight through execute -> validate ->
resolve/rollback, as above. A MEDIUM/HIGH risk repair -- which by definition
already required and received human approval -- goes through one additional
gate before being declared resolved: `_execute_and_validate` in
`backend/app/agents/graph.py` applies the repair, then immediately runs one
independent `validate_pipeline_health` check as a fast canary gate
(`CANARY_STARTED` -> `CANARY_VALIDATION_PASSED`/`CANARY_VALIDATION_FAILED`
incident events) *before* the existing full `REPAIR_COMPLETED` /
`VALIDATION_STARTED` / full-validation sequence runs. A failed canary check
rolls back immediately (`INCIDENT_ROLLED_BACK`) and never reaches
`REPAIR_COMPLETED` or a full validation pass -- the outcome genuinely
changes, this is not a cosmetic extra event.

**Honest scope and limitations of "canary" here.** A textbook canary
deployment routes a small percentage of live traffic through a new code path
on a separate instance/pool and compares metrics before expanding. This
project's tools operate on a single Kafka topic and a single Flink job --
there is no traffic-splittable fleet to route a literal 5% of orders through,
and no infrastructure in this demo to build one honestly. What this
implementation actually does is apply the repair once, then insert an extra,
independent re-measurement of pipeline health as a hard gate before
declaring success, so a repair that looks superficially "applied" but didn't
actually fix anything gets caught and rolled back before the incident is ever
marked RESOLVED. It is a real correctness gate (see
`backend/app/tests/test_advanced_features.py::test_failed_canary_validation_rolls_back_without_full_validation`),
just not a literal traffic-percentage canary -- do not describe it as one in
customer-facing material without this caveat.

## Cost protection: LLM invocation tracking and deduplication

Every `diagnose()`/`generate_repair_plan()` attempt (real or deduplicated) is
recorded in the `llm_invocations` table (`backend/app/agents/cost_tracking.py`),
with a deterministic size-based token estimate (tiktoken's `cl100k_base` if
installed, else `len(text)//4` -- documented as an estimate, never a real
LLM's billed token count, since the mock provider makes no network call at
all). See `GET /metrics/llm-usage` and the `llm_invocations_total` /
`llm_invocations_skipped_total` / `llm_estimated_tokens_total` Prometheus
counters.

Before invoking the LLM, `find_recent_duplicate` looks for another incident
with the *exact same* detector-type + component + evidence-signature
fingerprint (the same deterministic signature incident-memory retrieval
already computes -- see `app.agents.memory.evidence_signature`) created
within the last 60 seconds that already has a diagnosis and plan. If found,
that diagnosis/plan is reused (and both `LLMInvocation` rows are marked
`skipped_dedup=True`, referencing the incident they were reused from) instead
of calling the LLM again. This intentionally requires an **exact** signature
match, not a similarity threshold -- it exists to stop retries/duplicate
detections of the same underlying event from burning repeated LLM calls, not
to silently skip diagnosis for a genuinely new incident that merely looks
similar (that case is what incident-memory's fuzzy Jaccard retrieval is for,
and it always still runs a fresh diagnosis).

## Event correlation

`backend/app/agents/correlation.py` deterministically matches a `SystemEvent`
(schema version bump, synthetic deployment marker, config change -- see the
`system_events` table) or a prior `Incident` against a new incident when they
share a related component and/or a hard-coded type-relationship rule (e.g.
`SCHEMA_VERSION_CHANGE` is a plausible trigger for `SCHEMA_DRIFT`,
`POISON_MESSAGE`, `AIRFLOW_FAILURE`, `DATA_QUALITY`) within a configurable
time window before the incident (default 600s). This is matching on
structured fields and timestamps only -- no fuzzy text similarity, no LLM
judgment call -- so a correlation can always be explained by pointing at the
exact fields and time delta that matched. The mock LLM provider surfaces the
closest correlated event in its diagnosis reasoning (and nudges confidence up
slightly when the trigger occurred within 120 seconds), and
`GET /incidents/{id}/correlation` exposes the full ranked list independently
of any LLM interpretation of it.

## Notifications

`backend/app/services/notifications.py` defines a `NotificationService`
abstraction with two implementations, selected by `NOTIFICATION_CHANNEL`
(default `console`):

- **ConsoleNotifier** (default) logs a structured message through the
  existing JSON logger on `INCIDENT_DETECTED` and on entering
  `AWAITING_APPROVAL`. This is what's actually exercised in this repo --
  every notification also writes a `NOTIFICATION_SENT` `incident_events` row
  and an `audit_log` entry, so notification delivery is auditable the same
  way every other lifecycle step is.
- **SMTPNotifier** is a fully implemented real `smtplib`/`email.mime`
  sender, reading `SMTP_HOST`/`PORT`/`USERNAME`/`PASSWORD`/`FROM_ADDR`/
  `TO_ADDR` from env (see `.env.example`). It requires the user's own SMTP
  credentials to actually deliver mail and has **not** been exercised
  against a real mailbox in this session -- if required settings are
  missing it logs an error and no-ops rather than raising, so a
  misconfigured SMTP notifier can't crash the agent graph.

A notifier failure of any kind is caught and logged, never re-raised into
the agent graph (see `app.agents.graph._notify`) -- a broken notification
channel must never block incident detection or repair execution.

## Approve-link security model

`GET /approve-link/{token}` / `POST /approve-link/{token}/decide` let
someone act on an `AWAITING_APPROVAL` incident directly from a notification
link, without having the main dashboard "open."

**There is no separate authentication system in this repo.** The token
itself is the credential -- exactly the same trust model as a
password-reset email link. Concretely (`backend/app/services/approval_tokens.py`):

- Tokens are signed with `itsdangerous.URLSafeTimedSerializer` (chosen over
  a hand-rolled HMAC scheme because it bundles signature + embedded
  timestamp + URL-safe encoding in one well-known, audited library).
  Tampering with the payload (incident id, plan id) invalidates the
  signature.
- Every token also has a durable `approval_tokens` DB row keyed by an opaque
  `token_id`, marked `consumed` at decision time -- so single-use survives
  process restarts and doesn't rely on in-memory state.
- Expiry is enforced both by the signed timestamp (`itsdangerous`
  `max_age`) and is configurable via `APPROVAL_TOKEN_MAX_AGE_SECONDS`
  (default 1800s / 30 minutes).
- `GET /approve-link/{token}` is read-only and safe to call repeatedly --
  it validates but never consumes the token.
- `POST /approve-link/{token}/decide` consumes the token (marks it
  `consumed`) as part of the same transaction that checks its validity,
  then calls the **exact same** `agents.graph.approve_incident` /
  `reject_incident` functions the main dashboard's `POST
  /incidents/{id}/approve`/`reject` endpoints call -- there is exactly one
  approve/reject implementation, not two independent ones. The audit log
  and `HUMAN_APPROVED`/`HUMAN_REJECTED` incident_events record which
  mechanism was used (`decided_via: "dashboard"` vs `"approve-link"`) for
  traceability.
- `APPROVAL_TOKEN_SECRET` MUST be overridden with a real secret outside
  local development -- the shipped default is intentionally an obvious
  placeholder (`dev-insecure-secret-change-me`).

Failure modes are always reported cleanly rather than as a generic 500: an
expired, already-used, malformed, or no-longer-`AWAITING_APPROVAL` token
returns `{valid: false, error: "expired"|"invalid"|"already_used"|"already_decided"}`
from the GET, and a structured 400 from the decide endpoint.

## Ask about this incident (deterministic Q&A, not a new agentic surface)

`POST /incidents/{id}/ask` and the dashboard's "Ask about this incident" box
answer plain-English questions about an incident. This is explicitly **not**
a new LLM chat integration and **not** a new agentic action surface:

- It never calls an LLM provider and never invokes a tool.
- `backend/app/services/incident_qa.py` only reads this incident's
  already-persisted `diagnosis` / `RepairPlan.plan_json` / risk fields
  (computed once, during the normal `diagnose()`/`generate_repair_plan()`
  agent-graph steps) and deterministically templates them into prose,
  optionally leading with a different section based on keywords in the
  question (`why`/`cause` -> root cause first; `fix`/`repair`/`solution` ->
  repair plan first; `risk`/`approve` -> risk assessment first; otherwise a
  balanced summary).
- Because it only rephrases existing structured fields and never generates
  new content, it is structurally incapable of hallucinating beyond what
  was already diagnosed, and cannot trigger or influence any real
  infrastructure action.

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
