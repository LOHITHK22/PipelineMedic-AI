"""MCP-style typed tool abstraction.

We mirror MCP tool semantics (typed input schema, structured typed result,
audited execution, declared risk/permission tier) as plain Python classes
rather than wiring the actual MCP wire protocol. Rationale documented in
docs/mcp-tools.md: within this session, a genuine MCP client/server handshake
(stdio or SSE transport, tool discovery, JSON-RPC framing) adds real
integration surface without changing the actual safety or behavior story --
the important properties are typed inputs, typed outputs, permission
enforcement, and audit logging, all of which this abstraction provides and
enforces identically to how an MCP tool call would be enforced by the calling
agent. Swapping this for a real MCP server later only touches
app/tools/registry.py.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import ClassVar, Generic, TypeVar

from pydantic import BaseModel

from app.models.schemas import ToolRiskLevel
from app.policies.engine import is_hard_blocked

TIn = TypeVar("TIn", bound=BaseModel)
TOut = TypeVar("TOut", bound=BaseModel)


class ToolPermissionError(Exception):
    pass


class BaseTool(ABC, Generic[TIn, TOut]):
    name: ClassVar[str]
    risk_level: ClassVar[ToolRiskLevel]
    input_model: ClassVar[type[BaseModel]]
    output_model: ClassVar[type[BaseModel]]

    def run(self, tool_input: dict) -> BaseModel:
        if is_hard_blocked(self.name):
            raise ToolPermissionError(
                f"Tool '{self.name}' is CRITICAL and hard-blocked in code (app/tools/base.py). "
                "It can never be executed by the agent, regardless of policy config or approval."
            )
        validated_input = self.input_model.model_validate(tool_input)
        result = self._execute(validated_input)
        return result

    @abstractmethod
    def _execute(self, tool_input: TIn) -> TOut:
        ...
