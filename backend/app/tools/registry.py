"""Tool registry + audited invocation.

CRITICAL_HARD_BLOCK is enforced here IN CODE (not merely via the YAML policy
file) so that even a corrupted or maliciously edited policy file cannot
re-enable a critical destructive action. Any tool name in this set is
refused unconditionally.
"""
from __future__ import annotations

from app.db.base import SessionLocal
from app.db.models import AuditLog
from app.tools.airflow_tools import GetAirflowDagStatusTool, GetAirflowTaskLogsTool, RerunAirflowTaskTool
from app.tools.base import BaseTool, ToolPermissionError
from app.tools.flink_tools import ChangeFlinkParallelismTool, GetFlinkJobStatusTool, RestartFlinkJobTool
from app.tools.kafka_tools import (
    GetKafkaConsumerLagTool, GetRecentPipelineErrorsTool, GetTopicMetadataTool, QuarantineMessageTool,
)
from app.tools.ops_tools import (
    ApplySafeConfigPatchTool, RequestHumanApprovalTool, RollbackChangeTool, ValidatePipelineHealthTool,
)
from app.tools.schema_tools import CompareSchemaVersionsTool, GetDatabaseSchemaTool, GetSchemaVersionsTool

# Hard-coded, never configurable at runtime. These tools are declared for
# catalog completeness / documentation of the permission model but are NOT
# implemented -- calling them always raises ToolPermissionError.
CRITICAL_HARD_BLOCK = {
    "drop_database_table",
    "truncate_topic",
    "delete_dag",
    "run_arbitrary_sql",
    "run_shell_command",
}

_REGISTRY: dict[str, BaseTool] = {
    t.name: t()
    for t in [
        GetKafkaConsumerLagTool, GetTopicMetadataTool, GetRecentPipelineErrorsTool, QuarantineMessageTool,
        GetFlinkJobStatusTool, RestartFlinkJobTool, ChangeFlinkParallelismTool,
        GetAirflowDagStatusTool, GetAirflowTaskLogsTool, RerunAirflowTaskTool,
        GetDatabaseSchemaTool, GetSchemaVersionsTool, CompareSchemaVersionsTool,
        RequestHumanApprovalTool, ApplySafeConfigPatchTool, RollbackChangeTool, ValidatePipelineHealthTool,
    ]
}


def get_tool(name: str) -> BaseTool:
    if name in CRITICAL_HARD_BLOCK:
        raise ToolPermissionError(f"Tool '{name}' is CRITICAL and permanently hard-blocked; it has no implementation.")
    if name not in _REGISTRY:
        raise KeyError(f"Unknown tool: {name}")
    return _REGISTRY[name]


def invoke_tool(name: str, tool_input: dict, correlation_id: str, incident_id: str | None, actor: str = "agent") -> dict:
    """Invoke a tool by name with full audit-trail logging (success or failure)."""
    db = SessionLocal()
    try:
        tool = get_tool(name)
        try:
            result = tool.run(tool_input)
            result_dict = result.model_dump()
            db.add(AuditLog(
                correlation_id=correlation_id, incident_id=incident_id, actor=actor,
                action=f"tool_call:{name}",
                details={"input": tool_input, "output": result_dict, "risk_level": tool.risk_level.value, "success": True},
            ))
            db.commit()
            return result_dict
        except Exception as e:
            db.add(AuditLog(
                correlation_id=correlation_id, incident_id=incident_id, actor=actor,
                action=f"tool_call:{name}",
                details={"input": tool_input, "error": str(e), "success": False},
            ))
            db.commit()
            raise
    finally:
        db.close()


def list_tool_catalog() -> list[dict]:
    catalog = []
    for name, tool in _REGISTRY.items():
        catalog.append({
            "name": name,
            "risk_level": tool.risk_level.value,
            "input_schema": tool.input_model.model_json_schema(),
            "output_schema": tool.output_model.model_json_schema(),
        })
    for name in CRITICAL_HARD_BLOCK:
        catalog.append({"name": name, "risk_level": "CRITICAL", "hard_blocked": True})
    return catalog
