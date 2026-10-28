# MCP server and tools

PipelineMedic's typed tools are exposed through a **standards-compliant MCP
server** built on the official Model Context Protocol Python SDK. Any MCP
client (MCP Inspector, Claude Desktop, Cursor, Claude Code, the agent's own
MCP client adapter) can discover the tools, read their input/output schemas
and call them, behind a server-side policy gateway that a client cannot
bypass.

- Server: `backend/app/mcp_server.py` (`python -m app.mcp_server`)
- Approvals: `backend/app/services/mcp_approvals.py` (verification + operator CLI)
- Agent adapter: `backend/app/agents/tool_transport.py`
- Tests: `backend/app/tests/test_mcp_server.py`

## SDK and protocol

| | |
|---|---|
| SDK | `mcp` **2.3.0** (`mcp>=2.3.0,<3.0.0` in `backend/requirements.txt`), high-level `mcp.server.MCPServer` API |
| Transport | **stdio** (JSON-RPC over stdin/stdout; all logs go to stderr) |
| Protocol versions | negotiates `2024-11-05`, `2025-03-26`, `2025-06-18`, `2025-11-25` (handshake) and `2026-07-28` (latest) |
| Capabilities used | `tools/list`, `tools/call`, `inputSchema`, `outputSchema` + `structuredContent`, tool annotations, request `_meta` |
| Streamable HTTP | **not enabled** (see "Known limitations") |

MCP v2 forced two dependency bumps, both verified against the full existing
test suite: `pydantic` 2.9.2 → 2.14.1 (SDK needs ≥2.12) and `uvicorn` 0.30.6 →
0.54.0 (SDK needs ≥0.31.1). FastAPI 0.115.0 and starlette 0.38.6 are
unchanged.

## Architecture

Before: the agent graph decided risk/approval (`policies.engine.assess_risk`
+ a persisted `ApprovalRequest`) and then called `registry.invoke_tool()`
in-process. The registry itself only enforces the CRITICAL hard-block, which
was safe because the graph was its only caller.

After: the same tools are also reachable over MCP. Because an external MCP
client never passes through the agent graph, the MCP server enforces
authorization **itself**, on every call:

```mermaid
flowchart LR
    subgraph clients["AI clients"]
        INS["MCP Inspector"]
        CD["Claude Desktop / Cursor"]
        AGT["PipelineMedic agent<br/>(AGENT_TOOL_TRANSPORT=mcp)"]
    end

    subgraph server["MCP server: app/mcp_server.py (stdio)"]
        DISC["Tool discovery<br/>(only allowed tools listed)"]
        GW["PolicyGateway<br/>authorization + policy enforcement"]
    end

    subgraph policy["Authorization and policy enforcement"]
        BLK["Hard-block check<br/>(5 CRITICAL tools)"]
        VAL["Argument validation<br/>(unknown keys, identifiers,<br/>Pydantic input model)"]
        POL["Existing policy engine<br/>(autonomy_policy.yaml)"]
        APR["Persisted approval check<br/>(mcp_action_approvals,<br/>approved incident plans)"]
        AUD["audit_log row + metrics<br/>+ redaction"]
    end

    REG["Existing tool registry<br/>app/tools/* (BaseTool.run)"]

    subgraph infra["Infrastructure"]
        K[("Kafka")]
        F["Flink REST"]
        A["Airflow REST"]
        PG[("PostgreSQL")]
    end

    INS --> DISC
    CD --> DISC
    AGT --> DISC
    DISC --> GW
    GW --> BLK --> VAL --> POL --> APR --> REG
    GW -.-> AUD
    REG --> K
    REG --> F
    REG --> A
    REG --> PG
    OP["Operator CLI<br/>python -m app.services.mcp_approvals"] -.->|"approve / reject"| APR
```

No tool logic is duplicated. Each registry tool's existing Pydantic
`input_model` becomes the MCP `inputSchema`, its `output_model` becomes the
`outputSchema`, and execution calls the existing `BaseTool.run()`.

## Install and run

```bash
cd backend
python -m venv .venv && .venv/Scripts/activate      # Windows; use .venv/bin/activate on Linux/macOS
pip install -r requirements.txt

python -m app.mcp_server                             # stdio; read-only tools only
MCP_ALLOW_STATE_CHANGING_TOOLS=true python -m app.mcp_server   # also list state-changing tools
```

The server reads the same settings as the backend (`app/config.py`). The
`.env` file is read from the **current working directory**, and desktop
clients launch servers from an arbitrary directory, so pass settings as
explicit `env` entries in the client config. When the compose stack runs
(`make up`), the host-side endpoints are Postgres `localhost:5433`, Kafka
`localhost:9094`, Flink `localhost:8081` and Airflow `localhost:8080`.

The server needs the database at all times. Every call is audited before it
executes, and a call that cannot be audited is refused (`AUDIT_UNAVAILABLE`).
On startup it creates only the two tables it writes (`audit_log`,
`mcp_action_approvals`) if they are missing.

## Connecting clients

All examples use absolute paths. Replace `C:\\Pipeline Medic AI` with your
checkout path. They use the compose stack's dev credentials, which are
already in `docker-compose.yml`. Never put real credentials in a client config
that is committed or shared.

### MCP Inspector (verified)

CLI mode (headless, scriptable). This is how it was verified, with Inspector
2.10.1:

```bash
cd backend
npx -y @modelcontextprotocol/inspector@2.10.1 --cli --config ../docs/mcp-inspector.example.json \
    --server pipelinemedic --method tools/list
npx -y @modelcontextprotocol/inspector@2.10.1 --cli --config ../docs/mcp-inspector.example.json \
    --server pipelinemedic --method tools/call --tool-name get_topic_metadata --tool-arg topic=orders.raw
```

UI mode: `make mcp-inspector`, then open the printed local URL. That runs the same config in
the browser UI and was **not** exercised in verification. `docs/mcp-inspector.example.json`
uses `"command": "python"`, so run it from an activated venv or put the
absolute venv interpreter path there. Do not pass `-m app.mcp_server` as
positional arguments to `--cli`: Inspector consumes the `-m` flag, so use a
config file.

### Claude Desktop (configuration provided, not verified here)

`%APPDATA%\Claude\claude_desktop_config.json` (Windows) or
`~/Library/Application Support/Claude/claude_desktop_config.json` (macOS):

```json
{
  "mcpServers": {
    "pipelinemedic": {
      "command": "C:\\Pipeline Medic AI\\backend\\.venv\\Scripts\\python.exe",
      "args": ["-m", "app.mcp_server"],
      "env": {
        "PYTHONPATH": "C:\\Pipeline Medic AI\\backend",
        "DATABASE_URL": "postgresql+psycopg2://pipelinemedic:pipelinemedic@localhost:5433/pipelinemedic",
        "KAFKA_BOOTSTRAP_SERVERS": "localhost:9094",
        "FLINK_JOBMANAGER_URL": "http://localhost:8081",
        "AIRFLOW_BASE_URL": "http://localhost:8080",
        "MCP_ALLOW_STATE_CHANGING_TOOLS": "false"
      }
    }
  }
}
```

### Cursor (configuration provided, not verified here)

`~/.cursor/mcp.json` (global) or `.cursor/mcp.json` in the project. It uses the same
`mcpServers` object as Claude Desktop above.

### Running the server inside the backend container

When the compose stack is up, the container already has the right internal
endpoints (`postgres:5432`, `kafka:9092`). Use this as the client `command`:

```json
{
  "mcpServers": {
    "pipelinemedic": {
      "command": "docker",
      "args": ["compose", "-f", "C:\\Pipeline Medic AI\\docker-compose.yml",
               "exec", "-T", "backend", "python", "-m", "app.mcp_server"]
    }
  }
}
```

The equivalent `docker run -i --rm <backend image> python -m app.mcp_server`
pattern was verified with Inspector against an image built from this
`backend/Dockerfile`. The `docker compose exec` form itself was not run.

### Claude Code (not verified here)

```bash
claude mcp add pipelinemedic --env DATABASE_URL=postgresql+psycopg2://pipelinemedic:pipelinemedic@localhost:5433/pipelinemedic \
    -- "C:\Pipeline Medic AI\backend\.venv\Scripts\python.exe" -m app.mcp_server
```

## Tool catalog

17 executable tools. "Default" means listed with no configuration.
"Approval" means a persisted human approval is verified server-side before
execution.

| Tool | Policy risk | Kind | Listed by default | Approval |
|---|---|---|---|---|
| get_kafka_consumer_lag | LOW | read-only | yes | no |
| get_topic_metadata | LOW | read-only | yes | no |
| get_recent_pipeline_errors | LOW | read-only | yes | no |
| get_flink_job_status | LOW | read-only | yes | no |
| get_airflow_dag_status | LOW | read-only | yes | no |
| get_airflow_task_logs | LOW | read-only (secrets redacted) | yes | no |
| get_database_schema | LOW | read-only | yes | no |
| get_schema_versions | LOW | read-only | yes | no |
| compare_schema_versions | LOW | read-only | yes | no |
| validate_pipeline_health | LOW | read-only | yes | no |
| quarantine_message | LOW | state-changing (records intent; never deletes data) | no | no |
| request_human_approval | LOW | state-changing (records intent; does **not** approve anything) | no | no |
| restart_flink_job | MEDIUM | state-changing (Flink REST cancel) | no | **yes** |
| change_flink_parallelism | MEDIUM | state-changing (records desired parallelism 1-32) | no | **yes** |
| rerun_airflow_task | MEDIUM | state-changing (Airflow REST clear) | no | **yes** |
| apply_safe_config_patch | MEDIUM | state-changing (allowlisted keys only) | no | **yes** |
| rollback_change | MEDIUM | state-changing | no | **yes** |

Risk and approval come from the existing policy engine
(`backend/app/policies/autonomy_policy.yaml`). The MCP server evaluates each
tool as a one-action plan through `assess_risk`, so both pathways share the same
decision point. A tool's class-declared `risk_level` can only raise the result.
Approval-gated tools get one extra optional argument, `approval_id`. Each tool
also carries MCP annotations (`readOnlyHint`, `destructiveHint`, …) and
`_meta.pipelinemedic/risk_level`.

Tool semantics are unchanged. A degraded backend (Kafka or Flink unreachable)
still returns a normal structured result with an `error` field, exactly as on the
in-process pathway.

### Permanently blocked operations

`drop_database_table`, `truncate_topic`, `delete_dag`, `run_arbitrary_sql`,
`run_shell_command`. None of them has an implementation, and they are never
registered as MCP tools:

- `CRITICAL_HARD_BLOCK` in `backend/app/tools/registry.py` is a code literal.
  `build_server()` **refuses to start** if any hard-blocked name (that set or
  the YAML `hard_blocked_tools`) ever appears in the registry.
- The SDK only dispatches registered names. Aliases such as different case,
  hyphens, padding, zero-width characters, or names like `invoke_tool` and
  `_REGISTRY` all get `Unknown tool`. There is no dynamic dispatch by
  client-supplied name.
- The gateway re-checks the hard-block at call time anyway, and an attempt on
  any unexposed name is written to `audit_log` as `mcp_tool_call_rejected`.

## Security model

`PolicyGateway.call()` runs for every tool call, in this order:

1. **Audit first.** It inserts the call's `audit_log` row before doing anything
   else. If that fails, the call is refused (`AUDIT_UNAVAILABLE`).
2. **Exposure.** It rejects hard-blocked or unexposed names (`BLOCKED`,
   `PERMISSION_DENIED`).
3. **Policy.** It gets risk and autonomy from the policy engine. `BLOCKED` is
   refused. Any tool not in `READ_ONLY_TOOLS` is refused with
   `STATE_CHANGING_DISABLED` unless `MCP_ALLOW_STATE_CHANGING_TOOLS=true`. A new
   registry tool is therefore treated as state-changing until classified
   (fail-closed).
4. **Validation** (`VALIDATION_ERROR`). The handler takes no SDK-parsed
   arguments; the gateway validates the **raw** wire arguments itself:
   - unknown keys are rejected (the SDK would silently drop them);
   - `dag_id`, `task_id`, `run_id`, `job_id`, `topic`, `group_id`, `subject`,
     `component` and `table_name` must match `^[A-Za-z0-9_.:+\-]{1,250}$` with
     no `..`. Several of these are interpolated into Flink/Airflow REST URL
     paths sent with the backend's credentials, so `dag_id="x/../../config"`
     would otherwise reach other Airflow endpoints;
   - strings are capped at 2000 characters and nesting at 4 levels;
   - then the tool's own Pydantic input model validates the arguments. Error
     messages list field names, never the rejected values.
5. **Approval** for `APPROVAL_REQUIRED` tools, verified against persisted state
   (see below). Clients cannot assert approval. `_meta` flags and extra
   arguments are ignored or rejected.
6. **Execution** through `BaseTool.run()`. The tool's own rules still apply
   (for example, `apply_safe_config_patch` rejects non-allowlisted keys with
   `TOOL_REJECTED_INPUT`). An unexpected exception becomes a generic
   `BACKEND_ERROR` carrying the `audit_id`. The exception text is stored
   (redacted) only in the audit row.
7. **Redaction** of the result before it is returned (see "Audit and
   observability").

A refused call returns `isError: true` with one text block containing a JSON
object: `{"error_category", "message", "tool", "audit_id", ...}`. It
deliberately has no `structuredContent`. The spec requires structured content
to match the tool's `outputSchema`, and MCP Inspector validates it even on
errors. That was found during Inspector verification and fixed.

### Approval flow

An approval-required call is authorized only by one of two server-side
records.

**A. Operator-approved MCP request** (`mcp_action_approvals.source = mcp_request`):

1. The client calls, for example, `change_flink_parallelism {"job_name": "order-validator", "parallelism": 4}`.
   It gets `APPROVAL_REQUIRED` with `approval_request_id`, and a PENDING row stores
   the tool name, a SHA-256 of the canonical validated arguments, a redacted copy
   for review, and an expiry (`MCP_APPROVAL_TTL_SECONDS`, default 900).
2. An operator decides **out-of-band**:
   ```bash
   cd backend
   python -m app.services.mcp_approvals list
   python -m app.services.mcp_approvals approve <approval_request_id> --by <your-name> [--reason ...]
   python -m app.services.mcp_approvals reject  <approval_request_id> --by <your-name> --reason "..."
   ```
   The CLI needs shell and database access. No MCP tool or MCP code path can
   decide an approval. The decision is audited as `mcp_approval_decided`.
3. The client retries with the **same** arguments plus `approval_id`. The approval is consumed
   by a single conditional `UPDATE ... WHERE status='APPROVED' AND tool_name=... AND
   arguments_sha256=... AND expires_at > now`. It is single-use, bound to the tool and
   the exact arguments, time-limited, and race-safe. Pending, rejected,
   expired, mismatched, forged or reused IDs get `APPROVAL_INVALID`.

**B. Already-authorized incident plan** (`source = incident_plan`). This is how the
agent pathway works. The request `_meta` carries
`pipelinemedic/incident_id`. The gateway authorizes the call only if all of these hold:
- the incident is `EXECUTING` or `VALIDATING`;
- one of its RepairPlans was human-APPROVED through the existing
  dashboard/API/approve-link workflow, or marked `AUTO_EXECUTE` by the policy engine;
- that plan contains an action, or an action's rollback, with this tool and these
  canonical arguments.

Each plan action is usable **once**, enforced by a unique `grant_key`.

Every call, allowed or refused, is audited.

## Agent integration

`AGENT_TOOL_TRANSPORT` (default `registry`) selects how `app/agents/graph.py`
and `app/agents/context_collector.py` execute tools, via
`app/agents/tool_transport.invoke_tool`:

- `registry`: `registry.invoke_tool()` in-process, the original, unchanged behavior.
- `mcp`: each call starts `python -m app.mcp_server` over stdio with the
  official MCP client, sends the incident and correlation IDs in `_meta`, and
  goes through the same gateway as any external client. The agent-launched
  server enables state-changing tools, but MEDIUM actions still execute only
  if they match the human-approved plan (approval source B).

Failure semantics on `mcp`:
- a server refusal raises `ToolPermissionError`, the same as a hard-blocked
  tool on the registry path;
- a controlled tool error raises `McpToolError`;
- a server that fails to start, crashes or times out raises `McpTransportError`
  and is audited as `mcp_transport_error:<tool>`.

**None of these falls back to the registry.** The graph marks the execution
FAILED, so an action is never executed by another route. An unknown
`AGENT_TOOL_TRANSPORT` value also fails closed.

Audit on `mcp` is one `mcp_tool_call:<tool>` row per call, written by the
server. The adapter does not also call `registry.invoke_tool`, so there are no
duplicate `tool_call:` rows. The tests assert that both pathways produce
identical permitted results, the same denials, and an identical end-to-end
graph outcome.

## Audit and observability

- **`audit_log`**, one row per tool call (`action = mcp_tool_call:<tool>`,
  `actor = mcp:<clientInfo.name>`; the client name is a client-asserted label,
  never an authorization input). `details` contains `status` (SUCCESS / DENIED
  / INVALID / ERROR), `duration_ms`, `correlation_id` (from `_meta` or
  generated), `incident_id`, `client`, `risk_level`, `autonomy`, `approval`
  (NOT_REQUIRED / PENDING / INVALID / `APPROVED:<source>`), `approval_id`,
  `validation` (PASSED / FAILED), `error_category`, the redacted `input`, a
  redacted and size-bounded `output`, and `tool_reported_error`. Calls to
  unexposed names are audited as `mcp_tool_call_rejected`.
- **Prometheus** metrics, recorded in the MCP server process:
  `mcp_tool_calls_total{tool,outcome}` and
  `mcp_tool_call_duration_seconds{tool}`. The server is a separate process from
  FastAPI, so these are not on the backend's `/metrics`. Set `MCP_METRICS_PORT`
  to serve them on `127.0.0.1:<port>`.
- **Logs**: one structured JSON line per call on **stderr**. stdout carries
  only JSON-RPC frames.
- **Redaction** (`app/observability/redaction.py`) applies to every result
  returned to a client and to everything written to the audit row. It masks
  `user:password@` URLs, `password=` / `api_key:` / `secret=` / `token=` style
  pairs, `Bearer` / `Basic` credentials, and string values under
  sensitive-looking keys.

## Tests

```bash
cd backend
python -m pytest app/tests/test_mcp_server.py -m mcp_protocol   # SDK client, in-process transport, SQLite
python -m pytest app/tests/test_mcp_server.py -m mcp_stdio      # launches the real server subprocess
python -m pytest app/tests/test_mcp_server.py -m infra          # full agent graph; needs Postgres
python -m pytest app/tests                                       # everything
```

`infra` tests use the existing `clean_db` fixture, which **truncates all
tables**, so point `DATABASE_URL` at a disposable database. They are skipped
when Postgres is unreachable. Verification used a throwaway container:

```bash
docker run -d --rm --name pm-test-pg -e POSTGRES_USER=pipelinemedic -e POSTGRES_PASSWORD=pipelinemedic \
    -e POSTGRES_DB=pipelinemedic -p 55432:5432 postgres:16-alpine
docker run -d --rm --name pm-test-kafka -p 9092:9092 apache/kafka:3.7.0
DATABASE_URL=postgresql+psycopg2://pipelinemedic:pipelinemedic@localhost:55432/pipelinemedic \
KAFKA_BOOTSTRAP_SERVERS=localhost:9092 python -m pytest app/tests
```

## Known limitations

- **stdio only.** Streamable HTTP is not enabled. A remote endpoint would need
  real client authentication (the SDK supports OAuth resource-server
  configuration), and this repo has no identity system. Exposing it
  unauthenticated is explicitly out of scope.
- **The approver's identity is not authenticated.** `--by` is recorded as given.
  The trust boundary is "can run the CLI with database credentials", the same
  boundary as the existing dashboard approve endpoint, which is also
  unauthenticated (see `docs/safety-model.md`). There is no dashboard UI for MCP
  approval requests yet; they are decided through the CLI.
- **Incident-plan grants and the execution window.** Any MCP caller that knows
  an incident ID could, while that incident is EXECUTING, use one of its
  approved actions. It can only perform exactly the human-approved action
  with exactly the approved arguments, once, and doing so would make the agent's
  own call for that action fail.
- **Redaction is pattern-based.** It catches common credential shapes, not
  arbitrary sensitive business data. For example, order payloads read from the
  DLQ by `get_recent_pipeline_errors` are returned as-is.
- **Read-only tools fail closed when Postgres is down**, because every call
  must be audited.
- **Agent `mcp` mode** starts one server subprocess per tool call (measured 2.3-3.5 s
  each on the verification machine). That is fine for a handful of remediation
  calls, but not tuned for throughput.
- **Not verified here:** the Claude Desktop, Cursor and Claude Code GUI
  integrations (configs above, clients not installed), the Inspector **UI**
  mode, and the `docker compose exec` command form.
