import pytest

from app.tools.base import ToolPermissionError
from app.tools.registry import CRITICAL_HARD_BLOCK, get_tool


def test_critical_tools_are_never_registered_or_runnable():
    for name in CRITICAL_HARD_BLOCK:
        with pytest.raises(ToolPermissionError):
            get_tool(name)


def test_quarantine_message_tool_runs_and_validates_input():
    tool = get_tool("quarantine_message")
    result = tool.run({"topic": "orders.raw", "reason": "test"})
    assert result.status == "QUARANTINED"


def test_apply_safe_config_patch_rejects_disallowed_keys():
    tool = get_tool("apply_safe_config_patch")
    with pytest.raises(ValueError):
        tool.run({"component": "x", "patch": {"drop_table": True}})


def test_apply_safe_config_patch_allows_whitelisted_keys():
    tool = get_tool("apply_safe_config_patch")
    result = tool.run({"component": "flink-validator-test", "patch": {"parallelism": 4}})
    assert result.applied is True
