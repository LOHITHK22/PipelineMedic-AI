"""PipelineMedic AI MCP server (official MCP Python SDK v2, `mcp.server.MCPServer`).

Exposes the existing typed tools in app.tools.registry to MCP clients (MCP
Inspector, Claude Desktop, Cursor, the agent's own MCP client adapter) over
stdio. Run from backend/:

    python -m app.mcp_server

Each registry tool is registered with its existing Pydantic `input_model` as
the MCP inputSchema and its `output_model` as the outputSchema; execution
goes through the existing `BaseTool.run()` -- no tool logic is duplicated.

Why a policy gateway: `registry.invoke_tool()` only enforces the CRITICAL
hard-block. Risk/approval decisions are made by the agent graph
(`policies.engine.assess_risk` + a persisted ApprovalRequest) *before* it
calls the registry. An external MCP client never goes through the agent
graph, so exposing the registry directly would let any client execute
MEDIUM-risk actions with no approval. `PolicyGateway` closes that gap
server-side, for every call, regardless of who the client is:

  1. CRITICAL / hard-blocked tools are never registered (the server refuses
     to start if one ever appears in the registry), and are re-checked at
     call time.
  2. State-changing tools (anything not in READ_ONLY_TOOLS -- fail-closed for
     new tools) are not even listed unless MCP_ALLOW_STATE_CHANGING_TOOLS=true.
  3. Arguments are checked for unknown keys, identifier syntax (several
     fields are interpolated into Flink/Airflow REST URL paths) and size,
     then validated by the tool's own Pydantic model.
  4. Risk/autonomy comes from the existing policy engine. APPROVAL_REQUIRED
     tools need a persisted approval verified in the database by
     app.services.mcp_approvals -- an MCP client cannot create one.
  5. Exactly one audit_log row per tool call (written before execution and
     updated with the outcome; if it cannot be written the call is refused),
     plus Prometheus metrics and one structured stderr log line.
  6. Results and audit data are passed through app.observability.redaction.

stdout belongs to the JSON-RPC stream: all logging goes to stderr.
"""
from __future__ import annotations

import inspect
import json
import logging
import re
import sys
import time
import uuid
from typing import Annotated, Any

import anyio
from mcp.server import MCPServer
from mcp.server.mcpserver import Context
from mcp.server.mcpserver.tools import Tool
from mcp_types import CallToolResult, TextContent, ToolAnnotations
from pydantic import ValidationError

from app.config import settings
from app.db.base import SessionLocal
from app.db.models import AuditLog
from app.models.schemas import AutonomyDecision, RepairAction, RepairPlan, RiskLevel, Severity
from app.observability.metrics import MCP_TOOL_CALL_DURATION, MCP_TOOL_CALLS_TOTAL
from app.observability.redaction import redact, redact_text
from app.policies.engine import assess_risk, is_hard_blocked
from app.services import mcp_approvals
from app.tools.base import BaseTool, ToolPermissionError
from app.tools.registry import _REGISTRY, CRITICAL_HARD_BLOCK, get_tool

logger = logging.getLogger("pipelinemedic.mcp")

SERVER_NAME = "pipelinemedic"
SERVER_VERSION = "0.1.0"

# Tools that only read state. Every other registry tool is treated as
# state-changing (including the two LOW-risk "record intent" tools), so a
# tool added to the registry later is hidden by default until classified.
READ_ONLY_TOOLS = frozenset({
    "get_kafka_consumer_lag", "get_topic_metadata", "get_recent_pipeline_errors",
    "get_flink_job_status", "get_airflow_dag_status", "get_airflow_task_logs",
    "get_database_schema", "get_schema_versions", "compare_schema_versions",
    "validate_pipeline_health",
})

# Request `_meta` keys carrying PipelineMedic context (set by the agent's MCP
# client adapter; optional for other clients).
META_CORRELATION_ID = "pipelinemedic/correlation_id"
META_INCIDENT_ID = "pipelinemedic/incident_id"
APPROVAL_ARG = "approval_id"

TOOL_DESCRIPTIONS = {
    "get_kafka_consumer_lag": "Read-only. Per-partition and total consumer lag for a Kafka topic and consumer group.",
    "get_topic_metadata": "Read-only. Partition count for a Kafka topic.",
    "get_recent_pipeline_errors": "Read-only. Most recent messages on a dead-letter topic (default pipeline.dlq).",
    "quarantine_message": "State-changing (LOW risk, records intent only). Marks a topic's bad messages as "
                          "quarantined; never deletes or truncates data.",
    "get_flink_job_status": "Read-only. State, restart count and recent exceptions of a Flink job (by id or name).",
    "restart_flink_job": "State-changing (MEDIUM risk, approval required). Requests a cancel/restart of a Flink job "
                         "via the JobManager REST API.",
    "change_flink_parallelism": "State-changing (MEDIUM risk, approval required). Records a desired parallelism "
                                "(1-32) that takes effect on the next job redeploy.",
    "get_airflow_dag_status": "Read-only. Paused flag and latest run state of an Airflow DAG.",
    "get_airflow_task_logs": "Read-only. Tail of an Airflow task instance log (secrets redacted).",
    "rerun_airflow_task": "State-changing (MEDIUM risk, approval required). Clears an Airflow task instance so the "
                          "scheduler re-runs it.",
    "get_database_schema": "Read-only. Column names, types and nullability of a PipelineMedic database table.",
    "get_schema_versions": "Read-only. Registered schema versions for a subject (e.g. orders.raw).",
    "compare_schema_versions": "Read-only. Deterministic diff between two registered schema versions of a subject.",
    "request_human_approval": "State-changing (LOW risk, records intent only). Notes an escalation request. It does "
                              "NOT approve anything; approval-required tools return their own approval_request_id.",
    "apply_safe_config_patch": "State-changing (MEDIUM risk, approval required). Applies an allowlisted config patch "
                               "(keys: field_mapping, parallelism, quarantine_enabled) and captures previous state.",
    "rollback_change": "State-changing (MEDIUM risk, approval required). Restores a component's config state "
                       "captured by apply_safe_config_patch.",
    "validate_pipeline_health": "Read-only. Independently re-measures pipeline health (consumer lag vs threshold).",
}

_APPROVAL_HELP = (
    "Call it once without approval_id to get an approval_request_id; an operator approves it out-of-band "
    "(python -m app.services.mcp_approvals approve <id> --by <name>); then retry with the same arguments "
    "plus approval_id."
)

# Fields interpolated into Flink/Airflow REST URL paths or used as resource
# names. Restricting them to identifier syntax blocks path traversal like
# dag_id="x/../../config" reaching other endpoints with backend credentials.
_IDENTIFIER_FIELDS = {"dag_id", "task_id", "run_id", "job_id", "topic", "group_id", "subject", "component", "table_name"}
_IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9_.:+\-]{1,250}$")
_JOB_NAME_RE = re.compile(r"^[A-Za-z0-9_.:+\- ]{1,250}$")
_MAX_STRING_LEN = 2000
_MAX_DEPTH = 4
_MAX_AUDIT_OUTPUT_CHARS = 8000

# error_category values (also returned to clients in structuredContent).
VALIDATION_ERROR = "VALIDATION_ERROR"
TOOL_REJECTED_INPUT = "TOOL_REJECTED_INPUT"
PERMISSION_DENIED = "PERMISSION_DENIED"
BLOCKED = "BLOCKED"
STATE_CHANGING_DISABLED = "STATE_CHANGING_DISABLED"
APPROVAL_REQUIRED = "APPROVAL_REQUIRED"
APPROVAL_INVALID = "APPROVAL_INVALID"
BACKEND_ERROR = "BACKEND_ERROR"
AUDIT_UNAVAILABLE = "AUDIT_UNAVAILABLE"
UNKNOWN_TOOL = "UNKNOWN_TOOL"

DENIAL_CATEGORIES = frozenset({PERMISSION_DENIED, BLOCKED, STATE_CHANGING_DISABLED, APPROVAL_REQUIRED, APPROVAL_INVALID})
_INVALID_CATEGORIES = frozenset({VALIDATION_ERROR, TOOL_REJECTED_INPUT})

_RISK_RANK = {"LOW": 0, "MEDIUM": 1, "HIGH": 2, "CRITICAL": 3}


class ToolCallRefused(Exception):
    """A controlled, client-safe failure. `message` and `extra` are returned
    to the MCP client; nothing else from the underlying error is."""

    def __init__(self, category: str, message: str, **extra: Any):
        super().__init__(message)
        self.category = category
        self.message = message
        self.extra = extra


def tool_policy(tool_name: str) -> tuple[RiskLevel, AutonomyDecision]:
    """Risk + autonomy for a single tool call, from the existing policy engine.

    A one-action plan at the lowest incident-severity floor resolves to
    exactly this tool's own policy risk (tools missing from the YAML default
    to HIGH) and the YAML's autonomy decision for it, so the MCP path and the
    agent path share one choke point. The class-declared `risk_level` can
    only raise that (defense in depth), never lower it.
    """
    plan = RepairPlan(
        summary="mcp tool call",
        actions=[RepairAction(tool_name=tool_name, tool_input={}, description="mcp tool call", is_reversible=False)],
        expected_outcome="-",
        estimated_risk_level=RiskLevel.LOW,
    )
    assessment = assess_risk(plan, Severity.LOW)
    risk, autonomy = assessment.risk_level, assessment.autonomy_decision
    tool = _REGISTRY.get(tool_name)
    if tool is not None and _RISK_RANK[tool.risk_level.value] > _RISK_RANK[risk.value]:
        risk = RiskLevel(tool.risk_level.value)
        if autonomy == AutonomyDecision.AUTO_EXECUTE:
            autonomy = AutonomyDecision.APPROVAL_REQUIRED
    return risk, autonomy


def exposed_tool_names(allow_state_changing: bool) -> list[str]:
    names = []
    for name in _REGISTRY:
        if name in CRITICAL_HARD_BLOCK or is_hard_blocked(name):
            raise RuntimeError(f"Refusing to start: hard-blocked tool '{name}' is present in the tool registry.")
        _, autonomy = tool_policy(name)
        if autonomy == AutonomyDecision.BLOCKED:
            continue
        if name not in READ_ONLY_TOOLS and not allow_state_changing:
            continue
        names.append(name)
    return names


def _meta_str(meta: dict, key: str) -> str | None:
    value = meta.get(key)
    if isinstance(value, str) and 0 < len(value) <= 128:
        return value
    return None


_CLIENT_INFO_META = "io.modelcontextprotocol/clientInfo"


def _effective_meta(request_context) -> dict:
    """Request `_meta`, plus clientInfo from the `initialize` handshake for
    clients on protocol versions that don't repeat it per request."""
    meta = dict(request_context.meta or {})
    if _CLIENT_INFO_META not in meta:
        try:
            info = request_context.session.client_params.client_info
            meta[_CLIENT_INFO_META] = {"name": info.name}
        except AttributeError:
            pass
    return meta


def _client_label(meta: dict) -> str:
    info = meta.get(_CLIENT_INFO_META)
    name = info.get("name") if isinstance(info, dict) else None
    # Client-asserted and therefore only a label, never an authorization input.
    safe = re.sub(r"[^A-Za-z0-9_.\-]", "_", str(name))[:48] if name else "unknown"
    return f"mcp:{safe}"


def _is_uuid(value: str | None) -> bool:
    try:
        uuid.UUID(str(value))
        return True
    except (ValueError, TypeError):
        return False


def _check_argument_safety(arguments: dict) -> None:
    def walk(value: Any, path: str, depth: int):
        if depth > _MAX_DEPTH:
            raise ToolCallRefused(VALIDATION_ERROR, f"argument '{path}' is nested too deeply")
        if isinstance(value, str):
            if len(value) > _MAX_STRING_LEN:
                raise ToolCallRefused(VALIDATION_ERROR, f"argument '{path}' exceeds {_MAX_STRING_LEN} characters")
        elif isinstance(value, dict):
            for k, v in value.items():
                walk(v, f"{path}.{k}", depth + 1)
        elif isinstance(value, list):
            for i, v in enumerate(value):
                walk(v, f"{path}[{i}]", depth + 1)

    for key, value in arguments.items():
        if value is None:
            continue
        if key in _IDENTIFIER_FIELDS or key == "job_name":
            pattern = _JOB_NAME_RE if key == "job_name" else _IDENTIFIER_RE
            if not isinstance(value, str) or not pattern.fullmatch(value) or ".." in value:
                raise ToolCallRefused(
                    VALIDATION_ERROR,
                    f"argument '{key}' must be an identifier matching {pattern.pattern} without '..'",
                )
        walk(value, key, 0)


def _pre_parse_json_strings(tool: BaseTool, arguments: dict) -> dict:
    """Some clients (notably Claude Desktop) send object/array arguments as
    JSON-encoded strings. Mirror the SDK's own pre-parsing: for a field that is
    not typed `str`, a string that parses to an object/array is replaced by it."""
    for key, value in list(arguments.items()):
        field = tool.input_model.model_fields.get(key)
        if field is None or field.annotation is str or not isinstance(value, str):
            continue
        try:
            parsed = json.loads(value)
        except (ValueError, RecursionError):
            continue
        if isinstance(parsed, (dict, list)):
            arguments[key] = parsed
    return arguments


def _validation_summary(exc: ValidationError) -> str:
    # Field locations and messages only -- the rejected values are not echoed back.
    parts = [f"{'.'.join(str(p) for p in err['loc']) or '<root>'}: {err['msg']}" for err in exc.errors()]
    return "; ".join(parts)[:500]


def _bounded(value: Any) -> Any:
    try:
        text = json.dumps(value, default=str)
    except (TypeError, ValueError):
        return {"unserializable": True}
    if len(text) <= _MAX_AUDIT_OUTPUT_CHARS:
        return value
    return {"truncated": True, "chars": len(text), "keys": sorted(value.keys()) if isinstance(value, dict) else None}


class PolicyGateway:
    """Server-side authorization, validation, execution and audit for one MCP
    tool call. Transport-independent so it is unit-testable on its own."""

    def __init__(self, session_factory=None, allow_state_changing: bool | None = None):
        self.session_factory = session_factory or SessionLocal
        self.allow_state_changing = (
            settings.mcp_allow_state_changing_tools if allow_state_changing is None else allow_state_changing
        )
        self.exposed = frozenset(exposed_tool_names(self.allow_state_changing))

    # ---- audit -------------------------------------------------------------

    def _audit_insert(self, *, correlation_id: str, incident_id: str | None, actor: str, action: str, details: dict) -> str:
        db = self.session_factory()
        try:
            row = AuditLog(
                correlation_id=correlation_id, incident_id=incident_id if _is_uuid(incident_id) else None,
                actor=actor, action=action, details=details,
            )
            db.add(row)
            db.commit()
            return row.id
        finally:
            db.close()

    def _audit_update(self, audit_id: str, details: dict) -> None:
        db = self.session_factory()
        try:
            row = db.get(AuditLog, audit_id)
            if row is not None:
                row.details = details  # reassign (not mutate) so the JSON column is flagged dirty
                db.commit()
        finally:
            db.close()

    def audit_rejected_name(self, tool_name: str, meta: dict) -> None:
        """Audit a tools/call for a name this server does not expose (unknown,
        hidden, alias of a blocked tool, ...). The SDK answers it with
        "Unknown tool" before any tool handler runs."""
        hard_blocked = tool_name in CRITICAL_HARD_BLOCK or is_hard_blocked(tool_name)
        category = BLOCKED if hard_blocked else UNKNOWN_TOOL
        if not hard_blocked and tool_name in _REGISTRY and tool_name not in READ_ONLY_TOOLS:
            category = STATE_CHANGING_DISABLED
        details = {
            "transport": "mcp", "tool": tool_name[:128], "status": "DENIED", "error_category": category,
            "client": _client_label(meta), "correlation_id": _meta_str(meta, META_CORRELATION_ID),
        }
        try:
            self._audit_insert(
                correlation_id=_meta_str(meta, META_CORRELATION_ID) or f"mcp-{uuid.uuid4()}",
                incident_id=_meta_str(meta, META_INCIDENT_ID), actor=_client_label(meta),
                action="mcp_tool_call_rejected", details=details,
            )
        except Exception:
            logger.exception("Failed to audit rejected MCP tool name")
        MCP_TOOL_CALLS_TOTAL.labels(tool="_unexposed", outcome="DENIED").inc()
        logger.warning("mcp_tool_call", extra={"mcp": details})

    # ---- call --------------------------------------------------------------

    def call(self, tool_name: str, raw_arguments: dict, meta: dict | None = None) -> dict:
        """Authorize, validate, execute and audit. Returns the redacted
        output dict, or raises ToolCallRefused."""
        start = time.perf_counter()
        meta = meta or {}
        correlation_id = _meta_str(meta, META_CORRELATION_ID) or f"mcp-{uuid.uuid4()}"
        incident_id = _meta_str(meta, META_INCIDENT_ID)
        actor = _client_label(meta)
        record: dict[str, Any] = {
            "transport": "mcp", "tool": tool_name, "status": "STARTED", "correlation_id": correlation_id,
            "incident_id": incident_id, "client": actor, "risk_level": None, "autonomy": None,
            "approval": None, "validation": None, "error_category": None, "duration_ms": None,
            "input": _bounded(redact(raw_arguments)),
        }

        audit_id = None
        try:
            try:
                audit_id = self._audit_insert(
                    correlation_id=correlation_id, incident_id=incident_id, actor=actor,
                    action=f"mcp_tool_call:{tool_name}", details=dict(record),
                )
            except Exception:
                logger.exception("Audit store unavailable; refusing MCP tool call")
                raise ToolCallRefused(AUDIT_UNAVAILABLE, "Audit store unavailable; refusing to execute an unaudited tool call.")

            output = self._authorize_and_execute(tool_name, raw_arguments, record, correlation_id, incident_id, actor)
            record["status"] = "SUCCESS"
            record["output"] = _bounded(output)
            return output
        except ToolCallRefused as refused:
            record["error_category"] = refused.category
            record["status"] = (
                "DENIED" if refused.category in DENIAL_CATEGORIES
                else "INVALID" if refused.category in _INVALID_CATEGORIES else "ERROR"
            )
            record["error"] = redact_text(refused.message)
            refused.extra.setdefault("audit_id", audit_id)
            raise
        finally:
            elapsed = time.perf_counter() - start
            record["duration_ms"] = round(elapsed * 1000, 2)
            if audit_id is not None:
                try:
                    self._audit_update(audit_id, record)
                except Exception:
                    logger.exception("Failed to finalize MCP audit record %s", audit_id)
            MCP_TOOL_CALLS_TOTAL.labels(tool=tool_name, outcome=record["status"]).inc()
            MCP_TOOL_CALL_DURATION.labels(tool=tool_name).observe(elapsed)
            log_fields = {k: record.get(k) for k in (
                "tool", "status", "duration_ms", "correlation_id", "incident_id", "client", "risk_level",
                "approval", "validation", "error_category",
            )}
            logger.info("mcp_tool_call", extra={"mcp": {**log_fields, "audit_id": audit_id}})

    def _authorize_and_execute(self, tool_name, raw_arguments, record, correlation_id, incident_id, actor) -> dict:
        # 1. Never-executable names, re-checked even though they are never registered.
        if tool_name in CRITICAL_HARD_BLOCK or is_hard_blocked(tool_name):
            raise ToolCallRefused(BLOCKED, f"Tool '{tool_name}' is permanently blocked.")
        if tool_name not in self.exposed:
            raise ToolCallRefused(PERMISSION_DENIED, f"Tool '{tool_name}' is not available on this server.")
        tool: BaseTool = get_tool(tool_name)

        # 2. Policy (existing engine).
        risk, autonomy = tool_policy(tool_name)
        record["risk_level"], record["autonomy"] = risk.value, autonomy.value
        if autonomy == AutonomyDecision.BLOCKED:
            raise ToolCallRefused(BLOCKED, f"Tool '{tool_name}' is blocked by policy.")
        if tool_name not in READ_ONLY_TOOLS and not self.allow_state_changing:
            raise ToolCallRefused(STATE_CHANGING_DISABLED, "State-changing tools are disabled on this server.")

        # 3. Input validation.
        arguments = _pre_parse_json_strings(tool, dict(raw_arguments))
        approval_id = arguments.pop(APPROVAL_ARG, None) if autonomy == AutonomyDecision.APPROVAL_REQUIRED else None
        unknown = sorted(set(arguments) - set(tool.input_model.model_fields))
        if unknown:
            record["validation"] = "FAILED"
            raise ToolCallRefused(VALIDATION_ERROR, f"Unknown argument(s): {', '.join(str(u)[:64] for u in unknown)}")
        try:
            _check_argument_safety(arguments)
            canonical = mcp_approvals.canonical_arguments(tool.input_model, arguments)
        except ToolCallRefused:
            record["validation"] = "FAILED"
            raise
        except ValidationError as e:
            record["validation"] = "FAILED"
            raise ToolCallRefused(VALIDATION_ERROR, f"Invalid arguments: {_validation_summary(e)}")
        record["validation"] = "PASSED"
        record["input"] = _bounded(redact(canonical))

        # 4. Approval, verified against persisted state.
        if autonomy == AutonomyDecision.APPROVAL_REQUIRED:
            self._require_approval(tool, tool_name, canonical, approval_id, record, correlation_id, incident_id, actor)
        else:
            record["approval"] = "NOT_REQUIRED"

        # 5. Execute through the existing tool implementation.
        try:
            result = tool.run(canonical)
        except ToolPermissionError:
            raise ToolCallRefused(BLOCKED, f"Tool '{tool_name}' is permanently blocked.")
        except ValueError as e:  # the tool's own input rule (e.g. disallowed patch keys)
            raise ToolCallRefused(TOOL_REJECTED_INPUT, redact_text(str(e))[:300])
        except Exception as e:
            record["internal_error"] = redact_text(f"{type(e).__name__}: {e}")[:1000]
            raise ToolCallRefused(BACKEND_ERROR, "Tool execution failed due to a backend error. See the audit log for details.")

        output = redact(result.model_dump(mode="json"))
        record["tool_reported_error"] = bool(output.get("error")) if isinstance(output, dict) else False
        return output

    def _require_approval(self, tool, tool_name, canonical, approval_id, record, correlation_id, incident_id, actor):
        db = self.session_factory()
        try:
            if approval_id is not None:
                if not isinstance(approval_id, str):
                    record["approval"] = "INVALID"
                    raise ToolCallRefused(APPROVAL_INVALID, "approval_id must be a string.")
                result = mcp_approvals.consume_request_approval(
                    db, approval_id=approval_id, tool_name=tool_name, canonical_args=canonical,
                )
                if not result.authorized:
                    record["approval"] = "INVALID"
                    raise ToolCallRefused(APPROVAL_INVALID, f"Approval rejected: {result.reason}.")
            else:
                result = None
                if incident_id:
                    result = mcp_approvals.consume_incident_plan_grant(
                        db, incident_id=incident_id, tool_name=tool_name, canonical_args=canonical,
                        input_model=tool.input_model, correlation_id=correlation_id, requested_by=actor,
                    )
                if result is None or not result.authorized:
                    pending = mcp_approvals.create_pending_request(
                        db, tool_name=tool_name, canonical_args=canonical, incident_id=incident_id,
                        correlation_id=correlation_id, requested_by=actor,
                    )
                    record["approval"] = "PENDING"
                    record["approval_request_id"] = pending.id
                    detail = f" ({result.reason})" if result is not None else ""
                    raise ToolCallRefused(
                        APPROVAL_REQUIRED,
                        f"'{tool_name}' requires human approval{detail}. An operator must approve request "
                        f"{pending.id} out-of-band, then retry with the same arguments and approval_id.",
                        approval_request_id=pending.id,
                    )
            record["approval"] = f"APPROVED:{result.source}"
            record["approval_id"] = result.approval_id
        finally:
            db.close()


def _refusal_result(tool_name: str, refused: ToolCallRefused) -> CallToolResult:
    """isError result whose single text block is a JSON object (machine-
    readable for the agent adapter, readable for a model). Deliberately no
    structuredContent: the MCP spec requires structuredContent to conform to
    the tool's outputSchema, and strict clients (MCP Inspector) validate it
    even on error results."""
    payload = {"error_category": refused.category, "message": refused.message, "tool": tool_name, **refused.extra}
    return CallToolResult(content=[TextContent(type="text", text=json.dumps(payload))], is_error=True)


def parse_refusal(result: CallToolResult) -> dict:
    """The JSON body of a refusal produced by this server, or {} for any other
    error (e.g. the SDK's own "Unknown tool: ..." text)."""
    text = result.content[0].text if result.content and getattr(result.content[0], "type", None) == "text" else ""
    try:
        body = json.loads(text)
    except ValueError:
        return {}
    return body if isinstance(body, dict) and "error_category" in body else {}


def _input_schema(tool_name: str, tool: BaseTool, approval_gated: bool) -> dict:
    """The advertised inputSchema: the tool's existing Pydantic input model,
    as the SDK itself would render it from a typed signature (plus the
    optional approval_id for approval-gated tools)."""

    def signature_only(**kwargs):  # never called; only its signature is read
        raise NotImplementedError

    params = []
    for field_name, field in tool.input_model.model_fields.items():
        default = inspect.Parameter.empty if field.is_required() else field.default
        params.append(inspect.Parameter(
            field_name, inspect.Parameter.KEYWORD_ONLY, annotation=Annotated[field.annotation, field], default=default,
        ))
    if approval_gated:
        if APPROVAL_ARG in tool.input_model.model_fields:
            raise RuntimeError(f"Tool '{tool_name}' input model already defines '{APPROVAL_ARG}'.")
        params.append(inspect.Parameter(APPROVAL_ARG, inspect.Parameter.KEYWORD_ONLY, annotation=str | None, default=None))
    signature_only.__signature__ = inspect.Signature(params)
    signature_only.__name__ = tool_name  # schema title: "<tool_name>Arguments"
    return Tool.from_function(signature_only, name=tool_name, structured_output=False).parameters


def _make_tool(gateway: PolicyGateway, tool_name: str, tool: BaseTool, approval_gated: bool, **tool_kwargs) -> Tool:
    def handler(ctx: Context):
        # The handler deliberately takes no tool arguments, so the SDK does no
        # argument parsing of its own: it would silently drop unknown keys and
        # reject bad types before the gateway (and its audit record) ever saw
        # the call. The gateway validates the raw wire arguments itself.
        params = ctx.request_context.params or {}
        raw = params.get("arguments")
        try:
            return gateway.call(tool_name, dict(raw) if isinstance(raw, dict) else {}, _effective_meta(ctx.request_context))
        except ToolCallRefused as refused:
            return _refusal_result(tool_name, refused)

    handler.__signature__ = inspect.Signature(
        [inspect.Parameter("ctx", inspect.Parameter.KEYWORD_ONLY, annotation=Context)],
        return_annotation=tool.output_model,
    )
    handler.__name__ = tool_name
    mcp_tool = Tool.from_function(handler, name=tool_name, structured_output=True, **tool_kwargs)
    mcp_tool.parameters = _input_schema(tool_name, tool, approval_gated)
    return mcp_tool


class _RejectedNameAuditMiddleware:
    """Audits tools/call requests for names that are not registered here
    (the SDK rejects those before any tool handler runs)."""

    def __init__(self, gateway: PolicyGateway):
        self.gateway = gateway

    async def __call__(self, ctx, call_next):
        if ctx.method == "tools/call" and isinstance(ctx.params, dict):
            name = ctx.params.get("name")
            if isinstance(name, str) and name not in self.gateway.exposed:
                await anyio.to_thread.run_sync(self.gateway.audit_rejected_name, name, _effective_meta(ctx))
        return await call_next(ctx)


def build_server(*, session_factory=None, allow_state_changing: bool | None = None) -> MCPServer:
    gateway = PolicyGateway(session_factory=session_factory, allow_state_changing=allow_state_changing)
    tools = []
    for name in sorted(gateway.exposed):
        tool = _REGISTRY[name]
        risk, autonomy = tool_policy(name)
        approval_gated = autonomy == AutonomyDecision.APPROVAL_REQUIRED
        read_only = name in READ_ONLY_TOOLS
        description = TOOL_DESCRIPTIONS.get(name, f"PipelineMedic tool '{name}'.") + f" Policy risk: {risk.value}."
        if approval_gated:
            description += " " + _APPROVAL_HELP
        tools.append(_make_tool(
            gateway, name, tool, approval_gated,
            title=name.replace("_", " ").capitalize(),
            description=description,
            annotations=ToolAnnotations(
                read_only_hint=read_only, destructive_hint=False if read_only else risk != RiskLevel.LOW,
                idempotent_hint=read_only, open_world_hint=True,
            ),
            meta={"pipelinemedic/risk_level": risk.value, "pipelinemedic/autonomy": autonomy.value},
        ))
    mcp = MCPServer(
        SERVER_NAME,
        title="PipelineMedic AI",
        version=SERVER_VERSION,
        instructions=(
            "Diagnostic and remediation tools for the PipelineMedic Kafka/Flink/Airflow/PostgreSQL pipeline. "
            "Read-only tools run directly. State-changing tools are only listed when enabled by the operator; "
            "MEDIUM/HIGH risk tools additionally require a human approval verified by the server. "
            + _APPROVAL_HELP
        ),
        tools=tools,
        middleware=[_RejectedNameAuditMiddleware(gateway)],
    )
    mcp.gateway = gateway  # exposed for tests / introspection
    return mcp


class _StderrJsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        entry = {"ts": self.formatTime(record), "level": record.levelname, "logger": record.name, "message": record.getMessage()}
        if hasattr(record, "mcp"):
            entry["mcp"] = record.mcp
        if record.exc_info:
            entry["exc_type"] = record.exc_info[0].__name__ if record.exc_info[0] else None
        return json.dumps(entry, default=str)


def configure_stderr_logging() -> None:
    """stdio transport: stdout carries JSON-RPC frames only, so every log
    handler (ours, the SDK's, httpx's, kafka-python's) must write to stderr.
    Deliberately not app.observability.logging_config.configure_logging(),
    which writes to stdout for the FastAPI process."""
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(_StderrJsonFormatter())
    root = logging.getLogger()
    root.handlers = [handler]
    root.setLevel(settings.log_level)
    logging.getLogger("kafka").setLevel(logging.WARNING)


def main() -> None:
    configure_stderr_logging()
    from app.db.base import Base, engine
    from app.db.models import McpActionApproval

    try:
        # Only the tables this process writes; the FastAPI app owns the rest.
        Base.metadata.create_all(bind=engine, tables=[AuditLog.__table__, McpActionApproval.__table__])
    except Exception:
        logger.exception("Database unavailable at startup; tool calls will be refused until the audit store is reachable.")
    if settings.mcp_metrics_port:
        from prometheus_client import start_http_server

        start_http_server(settings.mcp_metrics_port, addr="127.0.0.1")
    server = build_server()
    logger.info("PipelineMedic MCP server starting (stdio)", extra={"mcp": {
        "tools": sorted(server.gateway.exposed), "state_changing_enabled": server.gateway.allow_state_changing,
    }})
    server.run(transport="stdio")


if __name__ == "__main__":
    main()
