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
