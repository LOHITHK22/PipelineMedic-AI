"""Offline agent evaluation harness.

Feeds each synthetic scenario in app.eval.dataset through the REAL agent
reasoning path -- app.llm.mock_provider.MockLLMProvider.diagnose() and
.generate_repair_plan(), then the REAL app.policies.engine.assess_risk() --
and compares actual vs. expected outcomes.

Deliberately offline / infra-free: it does NOT go through
app.agents.context_collector (which calls live tools against Kafka/Flink/
Airflow) or app.agents.graph (which persists to Postgres and executes real
repairs). Rationale (see docs/evaluation.md): this harness evaluates the
agent's REASONING -- diagnosis and repair-plan generation -- which is a pure
function of (incident, context, similar_incidents) given the deterministic
mock LLM provider. Bringing up the full docker-compose stack just to feed it
empty/synthetic context would add infra flakiness to a test that is
conceptually about reasoning correctness, not infra integration (that's what
app/tests/test_e2e.py is for, against a real Postgres).

Timings are wall-clock measurements of local, in-process function calls
against the mock provider. They measure mock-mode overhead (Pydantic
validation, Python function-call cost), NOT real LLM latency -- never quote
these numbers as representative of production/real-provider performance.
"""
from __future__ import annotations

import argparse
import json
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path

from app.eval.dataset import EvalScenario, build_dataset
from app.llm.mock_provider import MockLLMProvider
from app.models.schemas import AutonomyDecision, RiskLevel
from app.policies.engine import assess_risk


@dataclass
class ScenarioResult:
    scenario_id: str
    is_edge_case: bool
    diagnosis_correct: bool
    repair_plan_correct: bool
    risk_level_correct: bool
    autonomy_decision_correct: bool
    unsafe_action: bool  # actual == AUTO_EXECUTE while expected != AUTO_EXECUTE
    false_positive: bool  # edge case where actual over-escalates vs expected
    diagnose_seconds: float
    plan_seconds: float
    actual_root_cause: str
    actual_confidence: float
    actual_primary_tool: str
    actual_risk_level: str
    actual_autonomy_decision: str
    notes: str


def _run_scenario(provider: MockLLMProvider, scenario: EvalScenario) -> ScenarioResult:
    incident = scenario.incident
    context: dict = {"detector_evidence": incident.evidence}  # synthetic context, no live tool calls

    t0 = time.perf_counter()
    diagnosis = provider.diagnose(incident, context, similar_incidents=[])
    diagnose_seconds = time.perf_counter() - t0

    t1 = time.perf_counter()
    plan = provider.generate_repair_plan(incident, diagnosis, context, similar_incidents=[])
    plan_seconds = time.perf_counter() - t1

    risk = assess_risk(plan, incident.severity)

    root_cause_lower = diagnosis.root_cause.lower()
    diagnosis_correct = (
        all(s.lower() in root_cause_lower for s in scenario.expected_root_cause_contains)
        and scenario.expected_min_confidence <= diagnosis.confidence <= scenario.expected_max_confidence
    )

    actual_primary_tool = plan.actions[0].tool_name if plan.actions else ""
    repair_plan_correct = actual_primary_tool == scenario.expected_primary_tool

    risk_level_correct = risk.risk_level == scenario.expected_risk_level
    autonomy_decision_correct = risk.autonomy_decision.value == scenario.expected_autonomy_decision

    unsafe_action = (
        risk.autonomy_decision == AutonomyDecision.AUTO_EXECUTE
        and scenario.expected_autonomy_decision != "AUTO_EXECUTE"
    )
    false_positive = scenario.is_edge_case and not diagnosis_correct

    return ScenarioResult(
        scenario_id=scenario.scenario_id,
        is_edge_case=scenario.is_edge_case,
        diagnosis_correct=diagnosis_correct,
        repair_plan_correct=repair_plan_correct,
        risk_level_correct=risk_level_correct,
        autonomy_decision_correct=autonomy_decision_correct,
        unsafe_action=unsafe_action,
        false_positive=false_positive,
        diagnose_seconds=diagnose_seconds,
        plan_seconds=plan_seconds,
        actual_root_cause=diagnosis.root_cause,
        actual_confidence=diagnosis.confidence,
        actual_primary_tool=actual_primary_tool,
        actual_risk_level=risk.risk_level.value,
        actual_autonomy_decision=risk.autonomy_decision.value,
        notes=scenario.notes,
    )


def run_eval(scenarios: list[EvalScenario] | None = None) -> dict:
    scenarios = scenarios if scenarios is not None else build_dataset()
    provider = MockLLMProvider()
    results = [_run_scenario(provider, s) for s in scenarios]

    n = len(results)
    diagnosis_accuracy = sum(r.diagnosis_correct for r in results) / n
    repair_plan_accuracy = sum(r.repair_plan_correct for r in results) / n
    autonomy_accuracy = sum(r.autonomy_decision_correct for r in results) / n
    unsafe_action_rate = sum(r.unsafe_action for r in results) / n

    edge_cases = [r for r in results if r.is_edge_case]
    false_positive_rate = (sum(r.false_positive for r in edge_cases) / len(edge_cases)) if edge_cases else 0.0

    mean_time_to_diagnose_ms = 1000 * sum(r.diagnose_seconds for r in results) / n
    mean_time_to_plan_ms = 1000 * sum(r.plan_seconds for r in results) / n

    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "note": (
            "Timings are wall-clock for in-process calls against the deterministic mock LLM "
            "provider (LLM_PROVIDER=mock). They measure mock-mode overhead, NOT real LLM "
            "latency, and must never be quoted as production numbers."
        ),
        "scenario_count": n,
        "edge_case_count": len(edge_cases),
        "metrics": {
            "diagnosis_accuracy": round(diagnosis_accuracy, 4),
            "repair_plan_accuracy": round(repair_plan_accuracy, 4),
            "autonomy_decision_accuracy": round(autonomy_accuracy, 4),
            "unsafe_action_rate": round(unsafe_action_rate, 4),
            "false_positive_rate_on_edge_cases": round(false_positive_rate, 4),
            "mean_time_to_diagnose_ms": round(mean_time_to_diagnose_ms, 4),
            "mean_time_to_repair_plan_ms": round(mean_time_to_plan_ms, 4),
        },
        "scenarios": [asdict(r) for r in results],
    }
    return report


def _print_console_report(report: dict) -> None:
    m = report["metrics"]
    print(f"\nPipelineMedic AI -- Agent Evaluation Report ({report['generated_at']})")
    print(f"Scenarios: {report['scenario_count']} (edge cases: {report['edge_case_count']})")
    print("-" * 72)
    print(f"{'Metric':<38}{'Value':>10}")
    print("-" * 72)
    for k, v in m.items():
        print(f"{k:<38}{v!s:>10}")
    print("-" * 72)
    for r in report["scenarios"]:
        status = "PASS" if (r["diagnosis_correct"] and r["repair_plan_correct"] and r["autonomy_decision_correct"]) else "FAIL"
        print(f"[{status}] {r['scenario_id']:<32} diag={r['diagnosis_correct']} plan={r['repair_plan_correct']} "
              f"autonomy={r['actual_autonomy_decision']:<18} (expected match={r['autonomy_decision_correct']})")
    print()
    print(report["note"])


def main():
    parser = argparse.ArgumentParser(description="Run the PipelineMedic AI offline agent evaluation harness.")
    parser.add_argument("--out", default=None, help="Path to write the JSON report (default: eval-results/<timestamp>.json)")
    parser.add_argument("--quiet", action="store_true", help="Suppress console report")
    args = parser.parse_args()

    report = run_eval()

    if not args.quiet:
        _print_console_report(report)

    out_path = Path(args.out) if args.out else Path(__file__).resolve().parents[3] / "eval-results" / "latest.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(f"\nReport written to {out_path}")
    return report


if __name__ == "__main__":
    main()
