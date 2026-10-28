"""Typed tool abstraction (typed input schema, structured typed result,
audited execution, declared risk/permission tier) as plain Python classes.

These classes are the single implementation of every tool. The agent calls
them in-process through app/tools/registry.py, and app/mcp_server.py exposes
the same classes over the real MCP protocol (official Python SDK, stdio):
each `input_model`/`output_model` becomes the MCP inputSchema/outputSchema
and `run()` is the handler, behind a server-side policy gateway. See
docs/mcp-tools.md.
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
