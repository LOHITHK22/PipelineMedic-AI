from app.llm.mock_provider import MockLLMProvider
from app.models.schemas import DiagnosisResult, Incident, IncidentType, RepairPlan, Severity


def _incident(incident_type, evidence):
    return Incident(
        dedup_key="k1", incident_type=incident_type, severity=Severity.HIGH,
        source_component="test", title="t", description="d", evidence=evidence, correlation_id="c1",
    )


def test_diagnose_returns_structured_pydantic_for_all_scenarios():
    provider = MockLLMProvider()
    scenarios = [
        (IncidentType.SCHEMA_DRIFT, {"subject": "orders.raw", "changes": [{"field_name": "amount", "compatibility": "BREAKING"}]}),
        (IncidentType.POISON_MESSAGE, {"topic": "orders.raw", "samples": [{"reason": "INVALID_JSON"}]}),
        (IncidentType.KAFKA_LAG, {"topic": "orders.raw", "group_id": "g1", "total_lag": 6000, "critical_threshold": 5000}),
        (IncidentType.AIRFLOW_FAILURE, {"dag_id": "d", "task_id": "t", "logs_tail": "column missing"}),
    ]
    for incident_type, evidence in scenarios:
        incident = _incident(incident_type, evidence)
        diagnosis = provider.diagnose(incident, {})
        assert isinstance(diagnosis, DiagnosisResult)
        assert 0.0 <= diagnosis.confidence <= 1.0
        assert diagnosis.root_cause


def test_generate_repair_plan_returns_structured_pydantic():
    provider = MockLLMProvider()
    incident = _incident(IncidentType.KAFKA_LAG, {"topic": "orders.raw", "group_id": "g1", "total_lag": 6000})
    diagnosis = provider.diagnose(incident, {})
    plan = provider.generate_repair_plan(incident, diagnosis, {})
    assert isinstance(plan, RepairPlan)
    assert len(plan.actions) >= 1
    assert all(a.tool_name for a in plan.actions)


def test_mock_provider_is_deterministic():
    provider = MockLLMProvider()
    incident = _incident(IncidentType.KAFKA_LAG, {"topic": "orders.raw", "group_id": "g1", "total_lag": 6000, "critical_threshold": 5000})
    d1 = provider.diagnose(incident, {})
    d2 = provider.diagnose(incident, {})
    assert d1.model_dump() == d2.model_dump()
