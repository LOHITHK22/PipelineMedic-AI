# Agent design

## Why an explicit hand-rolled state machine instead of the `langgraph` package

The task graph is: `receive_incident -> collect_context -> diagnose ->
generate_repair_plan -> calculate_risk -> decision -> (auto_execute |
human_approval_wait) -> execute -> validate -> (resolve | rollback) ->
summarize`. Topologically this is a straight-line pipeline with exactly one
conditional branch (autonomy decision) and one loop-back (validate ->
rollback). `backend/app/agents/state.py` defines the same typed
`AgentState`/`AgentNode` vocabulary LangGraph would use, and
`backend/app/agents/graph.py` implements the transitions as plain Python
functions operating on that state, persisted to Postgres at every
transition.

The properties that matter for this project are:

1. **Typed state** -- yes, via Pydantic (`AgentState`, `Incident`,
   `DiagnosisResult`, `RepairPlan`, `RiskAssessment`).
2. **Durable pause/resume for human approval** -- yes, via a real Postgres
   row (`Incident.status = AWAITING_APPROVAL` + `ApprovalRequest.decision =
   PENDING`). If the FastAPI process restarts while an incident is awaiting
   approval, the row is still there; `POST /incidents/{id}/approve` picks up
   exactly where it left off by reading the plan back from `repair_plans`.
   This is arguably *more* explicit and easier to audit than an in-memory
   LangGraph checkpointer for a project whose core value proposition is
   auditability.
3. **Conditional edges** -- yes, the `decision` step's three-way branch
   (`AUTO_EXECUTE` / `APPROVAL_REQUIRED` / `BLOCKED`) is a plain `if/elif` on
   a `RiskAssessment.autonomy_decision` enum.

Swapping this for real LangGraph later is a mechanical refactor: the node
functions already take/return the same typed state shape a LangGraph node
would, and the persisted-approval pattern maps directly onto LangGraph's
`interrupt()` + a Postgres-backed checkpointer.

## Structured outputs only -- never regex-parsed text

`DiagnosisResult` and `RepairPlan` are Pydantic models. The mock provider
(`backend/app/llm/mock_provider.py`) constructs them directly from
evidence-pattern matches; the real OpenAI/Azure providers
(`backend/app/llm/provider.py`) use `client.beta.chat.completions.parse(...,
response_format=DiagnosisResult)`, i.e. the model's structured-output mode,
not free text later parsed with regex. This is a hallucination-control
measure: the model cannot emit a plan action that doesn't type-check against
`RepairAction`, and the tool executor further validates the `tool_input`
dict against that specific tool's own Pydantic input model before running
anything.

## Why deterministic detection happens before the LLM is ever called

Every incident is created by a deterministic detector (schema diff,
threshold comparison, DLQ classification, task-state check) reading
directly measured evidence -- never by asking the LLM "is something wrong?".
The LLM is only invoked to *explain* and *plan around* evidence a
deterministic system already collected and trusts. This bounds the blast
radius of a bad LLM diagnosis to the plan step, not the trigger step: at
worst, a wrong diagnosis produces a plan the risk engine still evaluates
independently and a human may still need to approve.

## Idempotency

Repair execution is keyed by `exec:<incident_id>`
(`backend/app/agents/graph.py::_execute_and_validate`). If `approve_incident`
is somehow called twice (retry, double-click, network retry from the UI),
the second call finds an existing `RepairExecution` row with `status ==
SUCCESS` for that key and returns without re-running any tool -- so
`change_flink_parallelism` or `rerun_airflow_task` can never fire twice for
one incident. Detection is separately deduplicated via a content-derived
`dedup_key` (`backend/app/detectors/base.py::make_dedup_key`) so re-observing
the same underlying condition (e.g. the lag monitor polling every few
seconds) doesn't create N duplicate incidents.

## Incident memory: retrieval design

`backend/app/agents/memory.py` gives the agent a durable memory of past,
*confirmed-successful* repairs, retrieved during `diagnose` and
`generate_repair_plan` (`backend/app/agents/graph.py::_run_until_decision`).

**Storage**: `incident_memory` (`backend/app/db/models.py::IncidentMemory`)
is a small, purpose-built table -- not a reuse of `incidents` +
`repair_plans` directly -- so a memory row is written exactly once, exactly
when it should be: from `_execute_and_validate`, only on the branch where
`validation.passed == True` and the incident reaches `RESOLVED`. A repair
that was rolled back, or an incident that failed, is never written there and
can therefore never be surfaced as a "similar successful precedent" --
memory only ever remembers things that were independently confirmed to
work, never things that were merely attempted.

**Retrieval is deliberately NOT vector/embedding-based.** No embeddings, no
external vector DB. Instead, `evidence_signature()` deterministically reduces
`incident.evidence` to a small set of short string tokens, specific to each
detector's evidence shape (e.g. `field:customer_id`, `change:RENAMED`,
`compat:BREAKING` for schema drift; `reason:INVALID_JSON` for poison
messages; `topic:...`, `group:...`, a lag-magnitude bucket for Kafka lag).
Similarity between the current incident and a remembered one is plain
Jaccard similarity (`|A ∩ B| / |A ∪ B|`) over these token sets, with a small
bonus when `source_component` matches exactly. This was chosen over
embeddings because:

  - The evidence is already structured (JSON), not prose -- there is no
    unstructured text that actually needs semantic embedding.
  - Every similarity score is fully explainable by pointing at the exact
    overlapping tokens, which matters when that score is allowed to
    influence an autonomous repair decision.
  - It adds zero new infrastructure (no vector DB, no embedding API/model)
    to a project that otherwise deliberately keeps every other decision
    (detection, risk) fully deterministic and inspectable.

**Memory only ever informs, never replaces, fresh reasoning.** The retrieved
matches (`SimilarIncidentMatch`, capped at `top_n=3`, most-similar first) are
passed into `LLMProvider.diagnose()` / `.generate_repair_plan()` as an
explicit, visible parameter -- never hidden state. `MockLLMProvider` still
computes the diagnosis/plan from the CURRENT incident's evidence first
(`_diagnose_raw` / `_generate_repair_plan_raw`); memory only *annotates* the
result afterward: it sets `informed_by_memory=True` and populates
`similar_past_incidents` (both visible fields on `DiagnosisResult` and
`RepairPlan`), and -- only when the best match clears
`HIGH_SIMILARITY_THRESHOLD` (0.6 Jaccard) -- nudges diagnosis confidence up
slightly (capped at 0.99) as corroborating evidence. It never substitutes a
past repair plan's actions for freshly-generated ones, and it never skips
`calculate_risk` or `validate` for the current incident -- every incident
still gets its own independent risk assessment and post-repair validation
regardless of how similar a past incident was.

When memory does inform a decision, `graph.py` writes both an
`AuditLog` row (`diagnosis_informed_by_memory` / `repair_plan_informed_by_memory`)
and a `MEMORY_RETRIEVED` `IncidentEvent` (visible in the incident timeline)
listing the matches and their similarity scores, so the influence is always
auditable after the fact. `GET /incidents/{id}/similar` exposes the same
retrieval on demand for any incident (resolved or not) for inspection.

## Event correlation: pointing at a plausible trigger, not just symptoms

`backend/app/agents/correlation.py::find_correlated_events`, wired into
`collect_context()` (`backend/app/agents/context_collector.py`) as
`context["correlated_events"]`, deterministically looks for `SystemEvent`
rows (schema version bumps, synthetic deployment markers, config changes --
see `backend/app/db/models.py::SystemEvent`) and other `Incident` rows within
a configurable window (default 600s) before the current incident that share
a related component and/or match a small, explicit type-relationship table
(e.g. a `SCHEMA_VERSION_CHANGE` is a plausible trigger for `SCHEMA_DRIFT`).
As with incident memory, this is structured-field + timestamp matching only
-- never fuzzy text similarity or an LLM judgment call -- so every match
carries an explicit, inspectable `reason` string. `MockLLMProvider.diagnose`
surfaces the single closest correlated event in its `reasoning` field (this
is the "schema version bumped ... 47 seconds before failures began" style
explainability from the original spec) and nudges confidence up slightly
when the trigger occurred within 120 seconds -- but the root cause itself is
still always computed first from the incident's own evidence;
correlation only ever annotates it. `GET /incidents/{id}/correlation`
exposes the full ranked correlation list independently of the LLM's
interpretation. See `docs/safety-model.md` for how schema-version bumps and
fault-injection scripts actually populate `system_events`.

## Canary remediation and LLM cost protection

See `docs/safety-model.md` ("Canary remediation" and "Cost protection") for
the two remaining advanced features: a canary validation gate for MEDIUM/HIGH
risk repairs (`backend/app/agents/graph.py::_execute_and_validate`), and
LLM-invocation audit tracking + exact-signature deduplication
(`backend/app/agents/cost_tracking.py`) to stop retries/duplicate incidents
from burning repeated LLM calls.
