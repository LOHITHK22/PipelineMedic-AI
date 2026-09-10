from app.models.schemas import AutonomyDecision, RepairAction, RepairPlan, RiskLevel, Severity
from app.policies.engine import assess_risk, get_tool_risk, is_hard_blocked


def _plan_with_tools(*tool_names):
    return RepairPlan(
        summary="test plan",
        actions=[
            RepairAction(tool_name=t, tool_input={}, description="x", is_reversible=True)
            for t in tool_names
        ],
        expected_outcome="test",
        estimated_risk_level=RiskLevel.LOW,
    )


def test_low_risk_tool_low_severity_is_auto_execute():
    plan = _plan_with_tools("get_kafka_consumer_lag", "quarantine_message")
    assessment = assess_risk(plan, Severity.LOW)
    assert assessment.risk_level == RiskLevel.LOW
    assert assessment.autonomy_decision == AutonomyDecision.AUTO_EXECUTE


def test_medium_risk_tool_requires_approval():
    plan = _plan_with_tools("restart_flink_job")
    assessment = assess_risk(plan, Severity.LOW)
    assert assessment.risk_level == RiskLevel.MEDIUM
    assert assessment.autonomy_decision == AutonomyDecision.APPROVAL_REQUIRED


def test_high_severity_forces_at_least_approval_required():
    plan = _plan_with_tools("get_kafka_consumer_lag")  # LOW risk tool
    assessment = assess_risk(plan, Severity.CRITICAL)  # but CRITICAL incident severity
    assert assessment.autonomy_decision == AutonomyDecision.APPROVAL_REQUIRED


def test_hard_blocked_tool_forces_blocked_regardless_of_policy():
    plan = _plan_with_tools("run_arbitrary_sql")
    assessment = assess_risk(plan, Severity.LOW)
    assert assessment.autonomy_decision == AutonomyDecision.BLOCKED
    assert "run_arbitrary_sql" in assessment.blocked_actions


def test_is_hard_blocked_helper():
    assert is_hard_blocked("drop_database_table") is True
    assert is_hard_blocked("get_kafka_consumer_lag") is False


def test_unknown_tool_defaults_to_high_risk():
    assert get_tool_risk("some_totally_unknown_tool") == RiskLevel.HIGH
