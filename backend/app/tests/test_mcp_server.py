"""Tests for the MCP integration (app.mcp_server, app.services.mcp_approvals,
app.agents.tool_transport).

Three categories, selectable with `-m`:

- mcp_protocol: the official MCP SDK `Client` connected to the server
  in-process. Real JSON-RPC request/response handling, tool discovery,
  schemas and the policy gateway -- but no process boundary. Isolated SQLite
  database per test.
- mcp_stdio: launches `python -m app.mcp_server` as a real subprocess and
  talks to it over stdio, exactly as MCP Inspector / Claude Desktop would.
- infra: the full agent graph against a real Postgres (DATABASE_URL), through
  the MCP pathway. Skipped automatically when Postgres is unreachable.
"""
from __future__ import annotations

import asyncio
import json
import sys
import uuid
from datetime import datetime, timedelta
from pathlib import Path

import jsonschema
import pytest
from mcp import Client
from mcp.client.stdio import StdioServerParameters
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.agents import tool_transport
from app.config import settings
from app.db.base import Base
from app.db.models import (
    ApprovalDecision, ApprovalRequest, AuditLog, Incident, IncidentStatus, McpActionApproval,
    RepairPlan as RepairPlanRow, RiskLevel as DBRiskLevel, SchemaVersion,
)
from app.mcp_server import (
    APPROVAL_ARG, META_CORRELATION_ID, META_INCIDENT_ID, READ_ONLY_TOOLS, PolicyGateway, ToolCallRefused, build_server,
)
from app.services import mcp_approvals
from app.tools import registry
from app.tools.airflow_tools import GetAirflowTaskLogsOutput, GetAirflowTaskLogsTool
from app.tools.base import ToolPermissionError
from app.tools.flink_tools import ChangeFlinkParallelismTool
from app.tools.registry import CRITICAL_HARD_BLOCK

BACKEND_DIR = Path(__file__).resolve().parents[2]
PARALLELISM_ARGS = {"job_name": "order-validator", "parallelism": 4}


def run(coro):
    return asyncio.run(coro)


def payload(result):
    """structuredContent for a successful call; the JSON refusal body (the
    single text block) for an isError result."""
    if result.is_error:
        assert result.structured_content is None  # must not violate the tool's outputSchema
        return json.loads(result.content[0].text)
    return result.structured_content


async def _list(server):
    async with Client(server) as client:
        return (await client.list_tools()).tools


async def _call(server, name, arguments, meta=None):
    async with Client(server) as client:
        return await client.call_tool(name, arguments, meta=meta)


@pytest.fixture()
def sqlite_db(tmp_path, monkeypatch):
    """Isolated SQLite database for the audit log, approvals and the
    DB-reading tools; the ops-tools config state file is redirected too."""
    db_path = tmp_path / "mcp_test.db"
    eng = create_engine(f"sqlite:///{db_path.as_posix()}", future=True)
    Base.metadata.create_all(bind=eng)
    factory = sessionmaker(bind=eng, autoflush=False, autocommit=False, future=True)
    monkeypatch.setattr("app.db.base.SessionLocal", factory)
    monkeypatch.setattr("app.db.base.engine", eng)
    monkeypatch.setattr("app.tools.schema_tools.SessionLocal", factory)
    monkeypatch.setattr("app.tools.registry.SessionLocal", factory)
    monkeypatch.setattr("app.tools.ops_tools._CONFIG_STATE_PATH", tmp_path / "config_state.json")
    factory.db_url = f"sqlite:///{db_path.as_posix()}"
    yield factory
    eng.dispose()


@pytest.fixture()
def read_only_server(sqlite_db):
    return build_server(session_factory=sqlite_db, allow_state_changing=False)


@pytest.fixture()
def write_server(sqlite_db):
    return build_server(session_factory=sqlite_db, allow_state_changing=True)


def _audit_rows(factory, action_prefix=None):
    db = factory()
    try:
        rows = db.query(AuditLog).order_by(AuditLog.created_at.asc()).all()
        return [r for r in rows if action_prefix is None or r.action.startswith(action_prefix)]
    finally:
        db.close()


@pytest.fixture()
def execute_spy(monkeypatch):
    """Counts real executions of ChangeFlinkParallelismTool._execute."""
    calls = []
    original = ChangeFlinkParallelismTool._execute

    def spy(self, tool_input):
        calls.append(tool_input.model_dump())
        return original(self, tool_input)

    monkeypatch.setattr(ChangeFlinkParallelismTool, "_execute", spy)
    return calls


# ---------------------------------------------------------------------------
# 1-4. Startup, connection, discovery, blocked operations
# ---------------------------------------------------------------------------

@pytest.mark.mcp_protocol
def test_server_starts_and_client_connects(read_only_server):
    async def go():
        async with Client(read_only_server) as client:
            assert client.server_info.name == "pipelinemedic"
            assert client.protocol_version
            assert client.server_capabilities.tools is not None
    run(go())


@pytest.mark.mcp_protocol
def test_default_discovery_exposes_only_read_only_tools(read_only_server):
    names = {t.name for t in run(_list(read_only_server))}
    assert names == READ_ONLY_TOOLS
    assert len(names) == 10


@pytest.mark.mcp_protocol
def test_state_changing_enabled_exposes_all_17_registry_tools(write_server):
    tools = {t.name: t for t in run(_list(write_server))}
    assert set(tools) == set(registry._REGISTRY)
    assert len(tools) == 17
    for name, tool in tools.items():
        assert tool.annotations.read_only_hint is (name in READ_ONLY_TOOLS)
        approval_gated = registry._REGISTRY[name].risk_level.value != "LOW"
        assert (APPROVAL_ARG in tool.input_schema["properties"]) is approval_gated, name
        assert tool.meta["pipelinemedic/risk_level"] == registry._REGISTRY[name].risk_level.value


@pytest.mark.mcp_protocol
def test_input_and_output_schemas_mirror_existing_pydantic_models(write_server):
    for tool in run(_list(write_server)):
        impl = registry._REGISTRY[tool.name]
        props = set(tool.input_schema["properties"]) - {APPROVAL_ARG}
        assert props == set(impl.input_model.model_fields), tool.name
        required = {n for n, f in impl.input_model.model_fields.items() if f.is_required()}
        assert set(tool.input_schema.get("required", [])) == required, tool.name
        assert set(tool.output_schema["properties"]) == set(impl.output_model.model_fields), tool.name
        assert tool.description


@pytest.mark.mcp_protocol
@pytest.mark.parametrize("name", sorted(CRITICAL_HARD_BLOCK))
def test_blocked_destructive_tools_never_listed_and_not_callable(write_server, sqlite_db, name):
    assert name not in {t.name for t in run(_list(write_server))}
    result = run(_call(write_server, name, {"table": "orders", "sql": "DROP TABLE orders", "command": "rm -rf /"}))
    assert result.is_error
    assert "Unknown tool" in result.content[0].text
    rejected = _audit_rows(sqlite_db, "mcp_tool_call_rejected")
    assert len(rejected) == 1
    assert rejected[0].details["error_category"] == "BLOCKED"


@pytest.mark.mcp_protocol
@pytest.mark.parametrize("alias", [
    "DROP_DATABASE_TABLE", "drop-database-table", " drop_database_table", "drop_database_table ",
    "run_shell_command​", "tools/run_shell_command", "get_tool", "invoke_tool", "_REGISTRY",
])
def test_aliases_and_dynamic_dispatch_names_cannot_reach_execution(write_server, alias):
    result = run(_call(write_server, alias, {}))
    assert result.is_error
    assert "Unknown tool" in result.content[0].text


@pytest.mark.mcp_protocol
def test_server_refuses_to_start_if_blocked_tool_ever_enters_registry(monkeypatch, sqlite_db):
    monkeypatch.setitem(registry._REGISTRY, "run_shell_command", registry._REGISTRY["quarantine_message"])
    with pytest.raises(RuntimeError, match="hard-blocked"):
        build_server(session_factory=sqlite_db, allow_state_changing=True)


@pytest.mark.mcp_protocol
def test_gateway_rechecks_hard_block_at_call_time(sqlite_db):
    gateway = PolicyGateway(session_factory=sqlite_db, allow_state_changing=True)
    with pytest.raises(ToolCallRefused) as exc:
        gateway.call("truncate_topic", {"topic": "orders.raw"})
    assert exc.value.category == "BLOCKED"


# ---------------------------------------------------------------------------
# 5, 7. Read-only execution over the protocol, structured responses
# ---------------------------------------------------------------------------

@pytest.mark.mcp_protocol
def test_read_only_tools_execute_with_structured_output(read_only_server, sqlite_db):
    db = sqlite_db()
    db.add_all([
        SchemaVersion(subject="orders.raw", version=1, schema_json={
            "customer_id": {"type": "string", "nullable": False}, "amount": {"type": "number", "nullable": False}}),
        SchemaVersion(subject="orders.raw", version=2, schema_json={
            "customerId": {"type": "string", "nullable": False}, "amount": {"type": "string", "nullable": False}}),
    ])
    db.commit()
    db.close()
    tools = {t.name: t for t in run(_list(read_only_server))}

    result = run(_call(read_only_server, "get_schema_versions", {"subject": "orders.raw"}))
    assert not result.is_error
    assert [v["version"] for v in payload(result)["versions"]] == [1, 2]
    jsonschema.validate(payload(result), tools["get_schema_versions"].output_schema)
    assert json.loads(result.content[0].text) == payload(result)

    result = run(_call(read_only_server, "compare_schema_versions", {"subject": "orders.raw", "old_version": 1, "new_version": 2}))
    assert not result.is_error
    jsonschema.validate(payload(result), tools["compare_schema_versions"].output_schema)
    assert payload(result)["diff"]["changes"]

    result = run(_call(read_only_server, "get_database_schema", {"table_name": "schema_versions"}))
    assert not result.is_error
    assert {c["name"] for c in payload(result)["columns"]} >= {"subject", "version", "schema_json"}


@pytest.mark.mcp_protocol
def test_unreachable_backend_reported_in_tool_semantics(read_only_server, monkeypatch):
    # Existing tool semantics: an unreachable Flink is a normal result with
    # state=UNREACHABLE and an `error` field, not a protocol failure.
    monkeypatch.setattr(settings, "flink_jobmanager_url", "http://127.0.0.1:9")
    result = run(_call(read_only_server, "get_flink_job_status", {"job_name": "order-validator"}))
    assert not result.is_error
    assert payload(result)["state"] == "UNREACHABLE"
    assert payload(result)["error"]


# ---------------------------------------------------------------------------
# 6. Input validation
# ---------------------------------------------------------------------------

@pytest.mark.mcp_protocol
@pytest.mark.parametrize("arguments,fragment", [
    ({"task_id": "t", "run_id": "r"}, "dag_id"),                                            # missing required
    ({"dag_id": "d", "task_id": "t", "run_id": "r", "try_number": "abc"}, "try_number"),  # wrong type
    ({"dag_id": "d", "task_id": "t", "run_id": "r", "url": "http://evil"}, "Unknown argument"),
    ({"dag_id": "x/../../config", "task_id": "t", "run_id": "r"}, "dag_id"),              # path traversal
    ({"dag_id": "d", "task_id": "..", "run_id": "r"}, "task_id"),
    ({"dag_id": "d" * 5000, "task_id": "t", "run_id": "r"}, "dag_id"),                     # oversized
])
def test_invalid_parameters_rejected_before_execution(read_only_server, sqlite_db, monkeypatch, arguments, fragment):
    executed = []
    monkeypatch.setattr(GetAirflowTaskLogsTool, "_execute", lambda self, ti: executed.append(ti))
    result = run(_call(read_only_server, "get_airflow_task_logs", arguments))
    assert result.is_error
    assert payload(result)["error_category"] == "VALIDATION_ERROR"
    assert fragment in payload(result)["message"]
    assert executed == []
    (row,) = _audit_rows(sqlite_db, "mcp_tool_call:")
    assert row.details["status"] == "INVALID"
    assert row.details["validation"] == "FAILED"


@pytest.mark.mcp_protocol
def test_json_encoded_object_arguments_are_accepted(write_server, sqlite_db):
    # Claude Desktop tends to send object arguments as JSON strings.
    args = {"component": "flink-validator", "patch": json.dumps({"parallelism": 4})}
    approval_id = payload(run(_call(write_server, "apply_safe_config_patch", args)))["approval_request_id"]
    db = sqlite_db()
    mcp_approvals.decide(db, approval_id, approve=True, decided_by="op")
    db.close()
    result = run(_call(write_server, "apply_safe_config_patch",
                       {"component": "flink-validator", "patch": {"parallelism": 4}, APPROVAL_ARG: approval_id}))
    assert not result.is_error and payload(result)["applied"] is True


@pytest.mark.mcp_protocol
def test_validation_errors_do_not_echo_rejected_values(read_only_server):
    result = run(_call(read_only_server, "get_kafka_consumer_lag", {"topic": "t", "group_id": {"secret": "s3cr3t-value"}}))
    assert result.is_error
    assert "s3cr3t-value" not in result.content[0].text


# ---------------------------------------------------------------------------
# 8-9. State-changing authorization and the human-approval policy
# ---------------------------------------------------------------------------

@pytest.mark.mcp_protocol
def test_state_changing_tools_rejected_by_default(read_only_server, sqlite_db, execute_spy):
    for name, args in [("change_flink_parallelism", PARALLELISM_ARGS), ("quarantine_message", {"topic": "t", "reason": "r"})]:
        result = run(_call(read_only_server, name, args))
        assert result.is_error and "Unknown tool" in result.content[0].text
    assert execute_spy == []
    categories = {r.details["error_category"] for r in _audit_rows(sqlite_db, "mcp_tool_call_rejected")}
    assert categories == {"STATE_CHANGING_DISABLED"}

    # Defense in depth: the gateway itself refuses even if a handler were reached.
    gateway = PolicyGateway(session_factory=sqlite_db, allow_state_changing=False)
    with pytest.raises(ToolCallRefused) as exc:
        gateway.call("quarantine_message", {"topic": "t", "reason": "r"})
    assert exc.value.category == "PERMISSION_DENIED"


@pytest.mark.mcp_protocol
def test_low_risk_state_changing_tool_auto_executes_when_enabled(write_server):
    result = run(_call(write_server, "quarantine_message", {"topic": "orders.raw", "reason": "poison"}))
    assert not result.is_error
    assert payload(result)["status"] == "QUARANTINED"


@pytest.mark.mcp_protocol
def test_approval_required_and_cannot_be_bypassed(write_server, sqlite_db, execute_spy):
    # a. no approval -> denied, a PENDING request is persisted
    result = run(_call(write_server, "change_flink_parallelism", PARALLELISM_ARGS))
    assert result.is_error
    assert payload(result)["error_category"] == "APPROVAL_REQUIRED"
    approval_id = payload(result)["approval_request_id"]
    db = sqlite_db()
    assert db.get(McpActionApproval, approval_id).status == "PENDING"
    db.close()

    def attempt(args, meta=None):
        return payload(run(_call(write_server, "change_flink_parallelism", args, meta=meta)))

    # b. the pending (undecided) request id cannot be used
    assert attempt({**PARALLELISM_ARGS, APPROVAL_ARG: approval_id})["error_category"] == "APPROVAL_INVALID"
    # c. forged / malformed ids
    assert attempt({**PARALLELISM_ARGS, APPROVAL_ARG: str(uuid.uuid4())})["error_category"] == "APPROVAL_INVALID"
    assert attempt({**PARALLELISM_ARGS, APPROVAL_ARG: "approved"})["error_category"] == "APPROVAL_INVALID"
    assert attempt({**PARALLELISM_ARGS, APPROVAL_ARG: "' OR 1=1 --"})["error_category"] == "APPROVAL_INVALID"
    # d. client-asserted approval metadata / extra args are ignored or rejected
    assert attempt(PARALLELISM_ARGS, meta={"pipelinemedic/approved": True, "approved_by": "admin"})["error_category"] == "APPROVAL_REQUIRED"
    assert attempt({**PARALLELISM_ARGS, "approved": True})["error_category"] == "VALIDATION_ERROR"
    assert execute_spy == []

    # e. an operator approves out-of-band (never via MCP)
    db = sqlite_db()
    mcp_approvals.decide(db, approval_id, approve=True, decided_by="oncall-operator", reason="test")
    db.close()

    # f. the approval is bound to the exact arguments
    assert attempt({"job_name": "order-validator", "parallelism": 32, APPROVAL_ARG: approval_id})["error_category"] == "APPROVAL_INVALID"
    assert execute_spy == []

    # g. exact arguments -> executes once
    ok = run(_call(write_server, "change_flink_parallelism", {**PARALLELISM_ARGS, APPROVAL_ARG: approval_id}))
    assert not ok.is_error
    assert payload(ok)["status"] == "APPLIED_PENDING_REDEPLOY"
    assert execute_spy == [PARALLELISM_ARGS]

    # h. single use
    assert attempt({**PARALLELISM_ARGS, APPROVAL_ARG: approval_id})["message"].endswith("approval already used.")
    assert len(execute_spy) == 1


@pytest.mark.mcp_protocol
def test_approval_is_bound_to_tool_and_rejected_or_expired_approvals_fail(write_server, sqlite_db, monkeypatch):
    def request(name, args):
        return payload(run(_call(write_server, name, args)))["approval_request_id"]

    # approved for one tool, used for another
    approval_id = request("change_flink_parallelism", PARALLELISM_ARGS)
    db = sqlite_db()
    mcp_approvals.decide(db, approval_id, approve=True, decided_by="op")
    db.close()
    other = run(_call(write_server, "restart_flink_job", {"job_id": "abc", APPROVAL_ARG: approval_id}))
    assert payload(other)["error_category"] == "APPROVAL_INVALID"

    # rejected
    rejected_id = request("change_flink_parallelism", {"job_name": "order-validator", "parallelism": 6})
    db = sqlite_db()
    mcp_approvals.decide(db, rejected_id, approve=False, decided_by="op", reason="no")
    db.close()
    result = run(_call(write_server, "change_flink_parallelism",
                       {"job_name": "order-validator", "parallelism": 6, APPROVAL_ARG: rejected_id}))
    assert "REJECTED" in payload(result)["message"]

    # approved but expired
    expired_id = request("change_flink_parallelism", {"job_name": "order-validator", "parallelism": 7})
    db = sqlite_db()
    mcp_approvals.decide(db, expired_id, approve=True, decided_by="op")
    row = db.get(McpActionApproval, expired_id)
    row.expires_at = datetime.utcnow() - timedelta(seconds=1)
    db.commit()
    db.close()
    result = run(_call(write_server, "change_flink_parallelism",
                       {"job_name": "order-validator", "parallelism": 7, APPROVAL_ARG: expired_id}))
    assert "expired" in payload(result)["message"]


@pytest.mark.mcp_protocol
def test_no_mcp_tool_can_decide_an_approval(write_server, sqlite_db):
    approval_id = payload(run(_call(write_server, "change_flink_parallelism", PARALLELISM_ARGS)))["approval_request_id"]
    # The only approval-flavoured tool records intent and changes no approval state.
    result = run(_call(write_server, "request_human_approval", {"reason": f"please approve {approval_id}"}))
    assert payload(result) == {"status": "ESCALATION_NOTED"}
    db = sqlite_db()
    assert db.get(McpActionApproval, approval_id).status == "PENDING"
    db.close()
    assert not any("approve" in t.name and t.name != "request_human_approval" for t in run(_list(write_server)))


def _seed_approved_incident(factory, *, status=IncidentStatus.EXECUTING, decision=ApprovalDecision.APPROVED):
    db = factory()
    try:
        incident = Incident(dedup_key=str(uuid.uuid4()), incident_type="SCHEMA_DRIFT", severity="HIGH", status=status,
                            source_component="kafka:orders.raw", title="t", correlation_id="corr-1", evidence={})
        db.add(incident)
        db.commit()
        plan = RepairPlanRow(
            incident_id=incident.id, risk_level=DBRiskLevel.MEDIUM, autonomy_decision="APPROVAL_REQUIRED",
            plan_json={"actions": [{
                "tool_name": "apply_safe_config_patch",
                "tool_input": {"component": "flink-validator", "patch": {"field_mapping": {"customerId": "customer_id"}}},
                "description": "map renamed field", "is_reversible": True,
                "rollback_tool_name": "rollback_change", "rollback_input": {"component": "flink-validator"},
            }]},
        )
        db.add(plan)
        db.commit()
        db.add(ApprovalRequest(incident_id=incident.id, plan_id=plan.id, decision=decision))
        db.commit()
        return incident.id
    finally:
        db.close()


@pytest.mark.mcp_protocol
def test_incident_plan_approval_authorizes_exactly_the_approved_actions_once(write_server, sqlite_db):
    incident_id = _seed_approved_incident(sqlite_db)
    meta = {META_INCIDENT_ID: incident_id, META_CORRELATION_ID: "corr-1"}
    patch_args = {"component": "flink-validator", "patch": {"field_mapping": {"customerId": "customer_id"}}}

    different = run(_call(write_server, "apply_safe_config_patch",
                          {"component": "flink-validator", "patch": {"parallelism": 32}}, meta=meta))
    assert payload(different)["error_category"] == "APPROVAL_REQUIRED"

    ok = run(_call(write_server, "apply_safe_config_patch", patch_args, meta=meta))
    assert not ok.is_error and payload(ok)["applied"] is True

    replay = run(_call(write_server, "apply_safe_config_patch", patch_args, meta=meta))
    assert payload(replay)["error_category"] == "APPROVAL_REQUIRED"

    rollback = run(_call(write_server, "rollback_change", {"component": "flink-validator"}, meta=meta))
    assert not rollback.is_error and payload(rollback)["rolled_back"] is True

    (success_row,) = [r for r in _audit_rows(sqlite_db, "mcp_tool_call:apply_safe_config_patch") if r.details["status"] == "SUCCESS"]
    assert success_row.details["approval"] == "APPROVED:incident_plan"
    assert success_row.incident_id == incident_id


@pytest.mark.mcp_protocol
@pytest.mark.parametrize("status,decision", [
    (IncidentStatus.AWAITING_APPROVAL, ApprovalDecision.PENDING),
    (IncidentStatus.REJECTED, ApprovalDecision.REJECTED),
    (IncidentStatus.RESOLVED, ApprovalDecision.APPROVED),  # approval window is over
])
def test_incident_plan_grant_requires_approved_plan_in_execution_window(write_server, sqlite_db, status, decision):
    incident_id = _seed_approved_incident(sqlite_db, status=status, decision=decision)
    result = run(_call(write_server, "apply_safe_config_patch",
                       {"component": "flink-validator", "patch": {"field_mapping": {"customerId": "customer_id"}}},
                       meta={META_INCIDENT_ID: incident_id}))
    assert payload(result)["error_category"] == "APPROVAL_REQUIRED"


@pytest.mark.mcp_protocol
def test_tool_level_input_rules_still_enforced_after_approval(write_server, sqlite_db):
    args = {"component": "x", "patch": {"drop_table": True}}
    approval_id = payload(run(_call(write_server, "apply_safe_config_patch", args)))["approval_request_id"]
    db = sqlite_db()
    mcp_approvals.decide(db, approval_id, approve=True, decided_by="op")
    db.close()
    result = run(_call(write_server, "apply_safe_config_patch", {**args, APPROVAL_ARG: approval_id}))
    assert payload(result)["error_category"] == "TOOL_REJECTED_INPUT"
    assert "disallowed keys" in payload(result)["message"]


# ---------------------------------------------------------------------------
# 10-11. Audit, redaction, controlled backend failures
# ---------------------------------------------------------------------------

@pytest.mark.mcp_protocol
def test_each_call_writes_exactly_one_complete_audit_record(read_only_server, sqlite_db):
    incident_id = str(uuid.uuid4())
    meta = {META_CORRELATION_ID: "corr-audit-1", META_INCIDENT_ID: incident_id}
    run(_call(read_only_server, "get_schema_versions", {"subject": "orders.raw"}, meta=meta))
    rows = _audit_rows(sqlite_db)
    assert len(rows) == 1
    row = rows[0]
    assert row.action == "mcp_tool_call:get_schema_versions"
    assert row.correlation_id == "corr-audit-1"
    assert row.incident_id == incident_id
    assert row.actor == "mcp:mcp"  # the SDK client's default clientInfo name
    d = row.details
    assert d["status"] == "SUCCESS"
    assert d["risk_level"] == "LOW" and d["autonomy"] == "AUTO_EXECUTE"
    assert d["approval"] == "NOT_REQUIRED" and d["validation"] == "PASSED"
    assert d["error_category"] is None
    assert isinstance(d["duration_ms"], float)
    assert d["input"] == {"subject": "orders.raw"}


@pytest.mark.mcp_protocol
def test_secrets_redacted_from_results_and_audit(read_only_server, sqlite_db, monkeypatch):
    leaky = ("connecting to postgresql://pipelinemedic:hunter2@postgres:5432/db\n"
             "AIRFLOW__CORE__FERNET_KEY password=hunter2 api_key: sk-live-abc123\nAuthorization: Bearer eyJhbGciOi.x.y")
    monkeypatch.setattr(GetAirflowTaskLogsTool, "_execute", lambda self, ti: GetAirflowTaskLogsOutput(logs_tail=leaky))
    result = run(_call(read_only_server, "get_airflow_task_logs", {"dag_id": "d", "task_id": "t", "run_id": "r"}))
    assert not result.is_error
    text = payload(result)["logs_tail"]
    for secret in ("hunter2", "sk-live-abc123", "eyJhbGciOi"):
        assert secret not in text
        assert secret not in result.content[0].text
    assert "[REDACTED]" in text
    assert "hunter2" not in json.dumps(_audit_rows(sqlite_db)[0].details)


@pytest.mark.mcp_protocol
def test_backend_failure_returns_controlled_error(read_only_server, sqlite_db, monkeypatch, tmp_path):
    broken = sessionmaker(bind=create_engine(f"sqlite:///{(tmp_path / 'missing' / 'x.db').as_posix()}"))
    monkeypatch.setattr("app.tools.schema_tools.SessionLocal", broken)
    result = run(_call(read_only_server, "get_schema_versions", {"subject": "orders.raw"}))
    assert result.is_error
    assert payload(result)["error_category"] == "BACKEND_ERROR"
    assert "OperationalError" not in result.content[0].text and "sqlite" not in result.content[0].text
    (row,) = _audit_rows(sqlite_db)
    assert row.details["status"] == "ERROR"
    assert row.details["internal_error"].startswith("OperationalError")
    assert payload(result)["audit_id"] == row.id


@pytest.mark.mcp_protocol
def test_audit_store_unavailable_fails_closed(monkeypatch, tmp_path, execute_spy):
    broken = sessionmaker(bind=create_engine(f"sqlite:///{(tmp_path / 'missing' / 'x.db').as_posix()}"))
    server = build_server(session_factory=broken, allow_state_changing=True)
    result = run(_call(server, "quarantine_message", {"topic": "t", "reason": "r"}))
    assert result.is_error
    assert payload(result)["error_category"] == "AUDIT_UNAVAILABLE"


# ---------------------------------------------------------------------------
# 12-13. Existing internal pathway, equivalence, no unauthorized fallback
# ---------------------------------------------------------------------------

@pytest.mark.mcp_protocol
def test_default_agent_pathway_is_the_unchanged_registry(sqlite_db):
    assert settings.agent_tool_transport == "registry"
    out = tool_transport.invoke_tool("quarantine_message", {"topic": "orders.raw", "reason": "r"}, "corr", None)
    assert out == {"quarantined_topic": "orders.raw", "reason": "r", "status": "QUARANTINED"}
    assert [r.action for r in _audit_rows(sqlite_db)] == ["tool_call:quarantine_message"]


@pytest.fixture()
def mcp_transport_in_process(monkeypatch, sqlite_db):
    monkeypatch.setattr(settings, "agent_tool_transport", "mcp")
    monkeypatch.setattr(tool_transport, "_server_override", build_server(session_factory=sqlite_db, allow_state_changing=True))


@pytest.mark.mcp_protocol
@pytest.mark.parametrize("name,args", [
    ("quarantine_message", {"topic": "orders.raw", "reason": "poison"}),
    ("get_schema_versions", {"subject": "orders.raw"}),
    ("get_database_schema", {"table_name": "audit_log"}),
])
def test_equivalent_permitted_results_across_pathways(sqlite_db, monkeypatch, name, args):
    via_registry = registry.invoke_tool(name, args, "corr-r", None)
    monkeypatch.setattr(settings, "agent_tool_transport", "mcp")
    monkeypatch.setattr(tool_transport, "_server_override", build_server(session_factory=sqlite_db, allow_state_changing=True))
    via_mcp = tool_transport.invoke_tool(name, args, "corr-m", None)
    assert via_mcp == json.loads(json.dumps(via_registry))  # JSON-normalized (int dict keys etc.)
    # one audit row per logical call on each pathway, never two
    actions = [r.action for r in _audit_rows(sqlite_db)]
    assert actions == [f"tool_call:{name}", f"mcp_tool_call:{name}"]


@pytest.mark.mcp_protocol
@pytest.mark.parametrize("name", sorted(CRITICAL_HARD_BLOCK))
def test_equivalent_denied_results_for_blocked_tools(mcp_transport_in_process, name):
    with pytest.raises(ToolPermissionError):
        registry.invoke_tool(name, {}, "corr", None)
    with pytest.raises(ToolPermissionError):
        tool_transport.invoke_tool(name, {}, "corr", None)


@pytest.mark.mcp_protocol
def test_equivalent_denied_results_for_invalid_input(mcp_transport_in_process):
    with pytest.raises(ValueError):  # pydantic ValidationError
        registry.invoke_tool("quarantine_message", {"reason": "missing topic"}, "corr", None)
    with pytest.raises(tool_transport.McpToolError) as exc:
        tool_transport.invoke_tool("quarantine_message", {"reason": "missing topic"}, "corr", None)
    assert exc.value.category == "VALIDATION_ERROR"


@pytest.mark.mcp_protocol
def test_mcp_pathway_denies_unapproved_medium_risk_action(mcp_transport_in_process, execute_spy):
    # The registry pathway relies on the agent graph having obtained approval
    # before calling it; over MCP the server enforces it itself.
    with pytest.raises(ToolPermissionError, match="APPROVAL_REQUIRED"):
        tool_transport.invoke_tool("change_flink_parallelism", PARALLELISM_ARGS, "corr", str(uuid.uuid4()))
    assert execute_spy == []


@pytest.mark.mcp_protocol
@pytest.mark.parametrize("command,args", [
    ("pipelinemedic-no-such-binary-xyz", []),           # server cannot be launched
    (sys.executable, ["-c", "import sys; sys.exit(3)"]),  # server exits before handshake
])
def test_mcp_connection_failure_never_falls_back_to_registry(sqlite_db, monkeypatch, execute_spy, command, args):
    monkeypatch.setattr(settings, "agent_tool_transport", "mcp")
    monkeypatch.setattr(settings, "mcp_client_timeout_seconds", 10.0)
    monkeypatch.setattr(tool_transport, "server_parameters", lambda: StdioServerParameters(command=command, args=args))
    fallback_calls = []
    monkeypatch.setattr(registry, "invoke_tool", lambda *a, **k: fallback_calls.append(a))

    with pytest.raises(tool_transport.McpTransportError):
        tool_transport.invoke_tool("change_flink_parallelism", PARALLELISM_ARGS, "corr-fail", None)
    assert fallback_calls == []
    assert execute_spy == []
    (row,) = _audit_rows(sqlite_db)
    assert row.action == "mcp_transport_error:change_flink_parallelism"
    assert row.details["error_category"] == "TRANSPORT_ERROR"


@pytest.mark.mcp_protocol
def test_unknown_agent_transport_fails_closed(monkeypatch, execute_spy):
    monkeypatch.setattr(settings, "agent_tool_transport", "mcp-typo")
    with pytest.raises(ValueError):
        tool_transport.invoke_tool("change_flink_parallelism", PARALLELISM_ARGS, "corr", None)
    assert execute_spy == []


# ---------------------------------------------------------------------------
# Real stdio subprocess
# ---------------------------------------------------------------------------

def _stdio_params(db_url: str, **extra_env) -> StdioServerParameters:
    env = {"DATABASE_URL": db_url, "APP_ENV": "test", "LLM_PROVIDER": "mock",
           "FLINK_JOBMANAGER_URL": "http://127.0.0.1:9", **extra_env}
    return StdioServerParameters(command=sys.executable, args=["-m", "app.mcp_server"], cwd=str(BACKEND_DIR), env=env)


@pytest.mark.mcp_stdio
def test_stdio_server_discovery_call_and_rejections(sqlite_db):
    async def go():
        async with Client(_stdio_params(sqlite_db.db_url), read_timeout_seconds=60) as client:
            tools = (await client.list_tools()).tools
            assert {t.name for t in tools} == READ_ONLY_TOOLS
            ok = await client.call_tool("get_database_schema", {"table_name": "audit_log"},
                                        meta={META_CORRELATION_ID: "stdio-corr"})
            assert not ok.is_error
            assert "correlation_id" in {c["name"] for c in payload(ok)["columns"]}
            flink = await client.call_tool("get_flink_job_status", {"job_name": "order-validator"})
            assert payload(flink)["state"] == "UNREACHABLE"
            blocked = await client.call_tool("drop_database_table", {"table": "orders"})
            assert blocked.is_error
            hidden = await client.call_tool("restart_flink_job", {"job_id": "abc"})
            assert hidden.is_error
            invalid = await client.call_tool("get_airflow_task_logs", {"dag_id": "../x", "task_id": "t", "run_id": "r"})
            assert payload(invalid)["error_category"] == "VALIDATION_ERROR"
    run(go())
    # The subprocess wrote its audit rows to the shared database.
    rows = _audit_rows(sqlite_db)
    by_action = {r.action: r for r in rows}
    assert by_action["mcp_tool_call:get_database_schema"].correlation_id == "stdio-corr"
    assert by_action["mcp_tool_call:get_database_schema"].details["status"] == "SUCCESS"
    assert len(rows) == 5


@pytest.mark.mcp_stdio
def test_agent_adapter_over_real_stdio_matches_registry(sqlite_db, monkeypatch):
    monkeypatch.setenv("DATABASE_URL", sqlite_db.db_url)
    monkeypatch.setenv("FLINK_JOBMANAGER_URL", "http://127.0.0.1:9")
    args = {"topic": "orders.raw", "reason": "poison"}
    via_registry = registry.invoke_tool("quarantine_message", args, "corr", None)

    monkeypatch.setattr(settings, "agent_tool_transport", "mcp")
    assert tool_transport.invoke_tool("quarantine_message", args, "corr", None) == via_registry
    with pytest.raises(ToolPermissionError):
        tool_transport.invoke_tool("drop_database_table", {"table": "orders"}, "corr", None)
    with pytest.raises(ToolPermissionError, match="APPROVAL_REQUIRED"):
        tool_transport.invoke_tool("restart_flink_job", {"job_id": "abc"}, "corr", str(uuid.uuid4()))


# ---------------------------------------------------------------------------
# Infrastructure-backed: full agent graph on real Postgres via MCP
# ---------------------------------------------------------------------------

def _schema_drift_incident():
    from app.detectors.base import build_incident
    from app.models.schemas import IncidentType, Severity

    return build_incident(
        incident_type=IncidentType.SCHEMA_DRIFT, severity=Severity.HIGH, source_component="kafka:orders.raw",
        title="Breaking schema drift detected on orders.raw", description="customer_id renamed",
        evidence={"subject": "orders.raw", "changes": [
            {"field_name": "customer_id", "change_type": "RENAMED", "renamed_to": "customerId", "compatibility": "BREAKING"},
        ]},
        correlation_id=str(uuid.uuid4()), discriminator="mcp-infra-" + str(uuid.uuid4()),
    )


def _force_healthy(monkeypatch):
    from app.tools.ops_tools import ValidatePipelineHealthOutput, ValidatePipelineHealthTool

    monkeypatch.setattr(ValidatePipelineHealthTool, "_execute",
                        lambda self, ti: ValidatePipelineHealthOutput(healthy=True, checks={"forced_healthy_for_test": True}))


def _run_lifecycle():
    from app.agents import graph
    from app.db.base import SessionLocal
    from app.db.models import RepairExecution

    incident_id = graph.receive_incident(_schema_drift_incident())
    db = SessionLocal()
    try:
        assert db.get(Incident, incident_id).status == IncidentStatus.AWAITING_APPROVAL
    finally:
        db.close()
    graph.approve_incident(incident_id, decided_by="test-operator", reason="mcp infra test")
    db = SessionLocal()
    try:
        row = db.get(Incident, incident_id)
        execution = db.query(RepairExecution).filter_by(incident_id=incident_id).one()
        audit = db.query(AuditLog).filter_by(incident_id=incident_id).all()
        return row.status, execution.tool_calls, [a.action for a in audit], [a.details for a in audit]
    finally:
        db.close()


@pytest.mark.infra
def test_graph_lifecycle_equivalent_across_registry_and_mcp(clean_db, monkeypatch, tmp_path):
    from app.db.base import SessionLocal

    state_file = tmp_path / "config_state.json"
    monkeypatch.setattr("app.tools.ops_tools._CONFIG_STATE_PATH", state_file)
    _force_healthy(monkeypatch)

    status_r, calls_r, actions_r, _ = _run_lifecycle()

    state_file.unlink(missing_ok=True)  # both runs start from the same config state
    monkeypatch.setattr(settings, "agent_tool_transport", "mcp")
    monkeypatch.setattr(tool_transport, "_server_override", build_server(session_factory=SessionLocal, allow_state_changing=True))
    status_m, calls_m, actions_m, details_m = _run_lifecycle()

    assert status_r == status_m == IncidentStatus.RESOLVED
    assert [(c["tool_name"], c["output"]) for c in calls_r] == [(c["tool_name"], c["output"]) for c in calls_m]
    assert any(a.startswith("tool_call:") for a in actions_r)
    assert not any(a.startswith("tool_call:") for a in actions_m)  # no duplicate registry audit rows
    patch_rows = [d for a, d in zip(actions_m, details_m) if a == "mcp_tool_call:apply_safe_config_patch"]
    assert len(patch_rows) == 1 and patch_rows[0]["approval"] == "APPROVED:incident_plan"


@pytest.mark.infra
def test_graph_mcp_transport_failure_fails_incident_without_fallback(clean_db, monkeypatch, tmp_path):
    monkeypatch.setattr("app.tools.ops_tools._CONFIG_STATE_PATH", tmp_path / "config_state.json")
    monkeypatch.setattr(settings, "agent_tool_transport", "mcp")
    monkeypatch.setattr(tool_transport, "server_parameters",
                        lambda: StdioServerParameters(command="pipelinemedic-no-such-binary-xyz"))
    fallback_calls = []
    monkeypatch.setattr(registry, "invoke_tool", lambda *a, **k: fallback_calls.append(a))

    status, calls, actions, _ = _run_lifecycle()
    assert status == IncidentStatus.FAILED
    assert calls == []
    assert fallback_calls == []
    assert "mcp_transport_error:quarantine_message" in actions


@pytest.mark.infra
@pytest.mark.mcp_stdio
def test_graph_lifecycle_via_real_stdio_server_on_postgres(clean_db, monkeypatch):
    """No monkeypatching reaches the server subprocess: validation measures
    real Kafka lag (KAFKA_BOOTSTRAP_SERVERS), and the outcome -- RESOLVED or
    ROLLED_BACK -- is whatever the real pipeline state dictates. Either way
    every MEDIUM action must have been authorized by the approved plan."""
    from app.db.base import SessionLocal
    from app.db.models import McpActionApproval as Grant

    monkeypatch.setattr(settings, "agent_tool_transport", "mcp")
    status, calls, actions, details = _run_lifecycle()
    assert status in (IncidentStatus.RESOLVED, IncidentStatus.ROLLED_BACK)
    assert [c["tool_name"] for c in calls] == ["quarantine_message", "apply_safe_config_patch"]
    assert not any(a.startswith("tool_call:") for a in actions)
    medium = [d for a, d in zip(actions, details)
              if a in ("mcp_tool_call:apply_safe_config_patch", "mcp_tool_call:rollback_change")]
    assert medium and all(d["approval"] == "APPROVED:incident_plan" and d["status"] == "SUCCESS" for d in medium)
    db = SessionLocal()
    try:
        assert db.query(Grant).filter_by(source="incident_plan").count() == len(medium)
    finally:
        db.close()
