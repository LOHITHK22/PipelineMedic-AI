"""Selects how the agent graph executes tools (settings.agent_tool_transport).

- "registry" (default): app.tools.registry.invoke_tool, in-process -- the
  original, unchanged behavior.
- "mcp": the same tool call is sent through the PipelineMedic MCP server
  (app.mcp_server) over stdio using the official MCP client. The server
  applies the same validation, risk policy and approval checks it applies to
  any external MCP client; the incident and correlation ids travel in the
  request `_meta`, which is how the server verifies that a MEDIUM-risk action
  belongs to a human-approved plan for this incident (see
  app.services.mcp_approvals).

Failure semantics for "mcp": a refusal by the server raises
ToolPermissionError (exactly like a hard-blocked tool on the registry path);
a controlled tool error raises McpToolError; a transport failure (server
cannot start, crashes, times out) raises McpTransportError. None of these
ever fall back to the registry pathway -- the agent graph treats them as a
failed execution, so a risky action is never silently executed another way.

Each "mcp" call starts a short-lived server subprocess (simple, isolated,
restart-safe; roughly a second of startup per call). Audit: the MCP server
writes the single audit_log row for each call, so this module does not call
registry.invoke_tool (which would write a second one); it only records
transport failures, where the server never saw the call.
"""
from __future__ import annotations

import asyncio
import concurrent.futures
import logging
import os
import sys
from pathlib import Path

from app.config import settings
from app.tools import registry
from app.tools.base import ToolPermissionError

logger = logging.getLogger("pipelinemedic.agent.tool_transport")

_BACKEND_DIR = Path(__file__).resolve().parents[2]

# Tests may point this at an in-process MCPServer instance: still the real MCP
# protocol layer and policy gateway, without a process boundary.
_server_override = None


class McpTransportError(RuntimeError):
    """The MCP server could not be reached or failed mid-call."""


class McpToolError(RuntimeError):
    """The MCP server returned a controlled, non-permission tool error."""

    def __init__(self, category: str, message: str):
        super().__init__(f"[{category}] {message}")
        self.category = category


def invoke_tool(name: str, tool_input: dict, correlation_id: str, incident_id: str | None, actor: str = "agent") -> dict:
    """Drop-in replacement for registry.invoke_tool used by app.agents.graph."""
    transport = settings.agent_tool_transport
    if transport == "registry":
        return registry.invoke_tool(name, tool_input, correlation_id, incident_id, actor=actor)
    if transport == "mcp":
        return invoke_tool_via_mcp(name, tool_input, correlation_id, incident_id)
    raise ValueError(f"Unknown AGENT_TOOL_TRANSPORT {transport!r}; expected 'registry' or 'mcp'.")


def server_parameters():
    from mcp.client.stdio import StdioServerParameters

    # The agent is the operator-configured execution pathway, so it enables
    # state-changing tools on its own server process. MEDIUM/HIGH tools are
    # still only executed if the server finds a matching approved plan.
    env = dict(os.environ)
    env["MCP_ALLOW_STATE_CHANGING_TOOLS"] = "true"
    env.setdefault("PYTHONUNBUFFERED", "1")
    return StdioServerParameters(command=sys.executable, args=["-m", "app.mcp_server"], cwd=str(_BACKEND_DIR), env=env)


async def _call(name: str, arguments: dict, meta: dict):
    from mcp import Client

    target = _server_override if _server_override is not None else server_parameters()
    async with Client(target, read_timeout_seconds=settings.mcp_client_timeout_seconds) as client:
        return await client.call_tool(name, arguments, meta=meta)


def _run_coroutine(coro):
    # Always run on a fresh thread + event loop so this works whether or not
    # the caller (a FastAPI threadpool worker, a monitor thread, a test) is
    # already inside a running loop.
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
        return pool.submit(asyncio.run, coro).result()


def _audit_transport_failure(name: str, correlation_id: str, incident_id: str | None, error: BaseException) -> None:
    from app.db.base import SessionLocal
    from app.db.models import AuditLog

    db = SessionLocal()
    try:
        db.add(AuditLog(
            correlation_id=correlation_id, incident_id=incident_id, actor="agent",
            action=f"mcp_transport_error:{name}",
            details={"transport": "mcp", "tool": name, "status": "ERROR", "error_category": "TRANSPORT_ERROR",
                     "error_type": type(error).__name__},
        ))
        db.commit()
    except Exception:
        logger.exception("Failed to audit MCP transport failure for %s", name)
    finally:
        db.close()


def invoke_tool_via_mcp(name: str, tool_input: dict, correlation_id: str, incident_id: str | None) -> dict:
    from app.mcp_server import DENIAL_CATEGORIES, META_CORRELATION_ID, META_INCIDENT_ID, parse_refusal

    meta = {META_CORRELATION_ID: correlation_id}
    if incident_id:
        meta[META_INCIDENT_ID] = incident_id

    try:
        result = _run_coroutine(_call(name, tool_input, meta))
    except Exception as e:
        logger.error("MCP transport failure calling %s: %s", name, type(e).__name__)
        _audit_transport_failure(name, correlation_id, incident_id, e)
        raise McpTransportError(f"MCP tool call '{name}' failed at the transport level ({type(e).__name__}); "
                                "not retried via any other pathway.") from e

    if result.is_error:
        payload = parse_refusal(result)
        text = result.content[0].text if result.content and hasattr(result.content[0], "text") else ""
        category = payload.get("error_category")
        message = payload.get("message") or text
        if category in DENIAL_CATEGORIES or (category is None and text.startswith("Unknown tool")):
            raise ToolPermissionError(f"MCP server refused '{name}' [{category or 'UNKNOWN_TOOL'}]: {message}")
        raise McpToolError(category or "UNKNOWN", message)

    return result.structured_content
