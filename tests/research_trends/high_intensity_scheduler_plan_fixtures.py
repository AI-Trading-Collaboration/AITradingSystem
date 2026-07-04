from __future__ import annotations

from pathlib import Path

from high_intensity_runtime_dry_run_fixtures import (
    build_high_intensity_runtime_dry_run_fixture,
    read_json,
    write_json,
)

from ai_trading_system.high_intensity_risk_cap_observe_only_runtime_dry_run import (
    NEXT_2344_SCHEDULER_PLAN_TASK,
    run_high_intensity_risk_cap_observe_only_runtime_dry_run,
)

__all__ = [
    "build_high_intensity_scheduler_plan_fixture",
    "read_json",
    "write_json",
]


def build_high_intensity_scheduler_plan_fixture(tmp_path: Path) -> dict[str, Path]:
    fixture = build_high_intensity_runtime_dry_run_fixture(tmp_path)
    runtime_dry_run_dir = tmp_path / "runtime_dry_run"
    runtime_dry_run_docs_root = tmp_path / "runtime_dry_run_docs"
    run_high_intensity_risk_cap_observe_only_runtime_dry_run(
        runtime_integration_plan_dir=fixture["runtime_integration_plan_dir"],
        continue_decision_dir=fixture["continue_decision_dir"],
        event_logger_dir=fixture["event_logger_dir"],
        threshold_selection_dir=fixture["threshold_selection_dir"],
        forward_observe_plan_dir=fixture["forward_observe_plan_dir"],
        dynamic_dry_run_dir=fixture["dynamic_dry_run_dir"],
        output_dir=runtime_dry_run_dir,
        docs_root=runtime_dry_run_docs_root,
    )
    _normalize_runtime_dry_run_route_for_2344(runtime_dry_run_dir)
    return {
        **fixture,
        "runtime_dry_run_dir": runtime_dry_run_dir,
        "runtime_dry_run_docs_root": runtime_dry_run_docs_root,
    }


def _normalize_runtime_dry_run_route_for_2344(root: Path) -> None:
    readiness_status = "READY_FOR_2344_WITH_CAVEATS"
    warnings = [
        "DERIVED_RUNTIME_INPUT_FIELDS_USED",
        "MONTHLY_CONCENTRATION_MONITORING_REQUIRED",
        "MONTHLY_EVENT_CONCENTRATION_ABOVE_GUARDRAIL",
        "NO_PAPER_SHADOW",
        "NO_PRODUCTION",
        "OBSERVE_ONLY",
        "PARTIAL_COVERAGE_CAVEAT",
        "PIT_APPROXIMATION_CAVEAT",
    ]
    monthly_path = root / "high_intensity_runtime_monthly_concentration_monitoring_dry_run.json"
    monthly = read_json(monthly_path)
    monthly["monthly_monitoring_status"] = "PASS_WITH_WARNINGS"
    monthly["monthly_concentration_blocking_count"] = 0
    monthly["monitoring_blockers"] = []
    monthly["months_above_blocking_guardrail"] = []
    monthly["monitoring_warnings"] = ["MONTHLY_EVENT_CONCENTRATION_ABOVE_GUARDRAIL"]
    write_json(monthly_path, monthly)

    readiness_path = root / "high_intensity_2344_readiness_checklist.json"
    readiness = read_json(readiness_path)
    readiness["readiness_status"] = readiness_status
    readiness["readiness_blockers"] = []
    readiness["readiness_warnings"] = warnings
    write_json(readiness_path, readiness)

    route_path = root / "high_intensity_2344_task_route.json"
    route = read_json(route_path)
    route["readiness_status"] = readiness_status
    route["next_task"] = NEXT_2344_SCHEDULER_PLAN_TASK
    route["route_blockers"] = []
    route["route_caveats"] = warnings
    write_json(route_path, route)

    summary_path = root / "high_intensity_runtime_dry_run_summary.json"
    summary = read_json(summary_path)
    summary["2344_readiness_status"] = readiness_status
    summary["next_task"] = NEXT_2344_SCHEDULER_PLAN_TASK
    write_json(summary_path, summary)
