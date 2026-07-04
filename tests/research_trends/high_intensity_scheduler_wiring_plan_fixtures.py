from __future__ import annotations

from pathlib import Path

from high_intensity_scheduler_dry_run_fixtures import (
    build_high_intensity_scheduler_dry_run_fixture,
    read_json,
    write_json,
)

from ai_trading_system.high_intensity_risk_cap_scheduler_dry_run import (
    run_high_intensity_risk_cap_observe_only_scheduler_dry_run,
)

__all__ = [
    "build_high_intensity_scheduler_wiring_plan_fixture",
    "read_json",
    "write_json",
]


def build_high_intensity_scheduler_wiring_plan_fixture(
    tmp_path: Path,
) -> dict[str, Path]:
    fixture = build_high_intensity_scheduler_dry_run_fixture(tmp_path)
    scheduler_dry_run_dir = tmp_path / "scheduler_dry_run"
    scheduler_dry_run_docs_root = tmp_path / "scheduler_dry_run_docs"
    run_high_intensity_risk_cap_observe_only_scheduler_dry_run(
        scheduler_integration_plan_dir=fixture["scheduler_integration_plan_dir"],
        runtime_dry_run_dir=fixture["runtime_dry_run_dir"],
        runtime_integration_plan_dir=fixture["runtime_integration_plan_dir"],
        continue_decision_dir=fixture["continue_decision_dir"],
        event_logger_dir=fixture["event_logger_dir"],
        threshold_selection_dir=fixture["threshold_selection_dir"],
        forward_observe_plan_dir=fixture["forward_observe_plan_dir"],
        dynamic_dry_run_dir=fixture["dynamic_dry_run_dir"],
        output_dir=scheduler_dry_run_dir,
        docs_root=scheduler_dry_run_docs_root,
    )
    summary_path = scheduler_dry_run_dir / "high_intensity_scheduler_dry_run_summary.json"
    summary = read_json(summary_path)
    summary["status"] = (
        "OBSERVE_ONLY_SCHEDULER_DRY_RUN_READY_WITH_CAVEATS_PROMOTION_BLOCKED"
    )
    summary["scheduler_dry_run_status"] = summary["status"]
    summary["2346_readiness_status"] = "READY_FOR_2346_WITH_CAVEATS"
    summary["next_task"] = (
        "TRADING-2346_High_Intensity_Risk_Cap_Observe_Only_Scheduler_Wiring_Plan"
    )
    write_json(summary_path, summary)
    readiness_path = scheduler_dry_run_dir / "high_intensity_2346_readiness_checklist.json"
    readiness = read_json(readiness_path)
    readiness["readiness_status"] = "READY_FOR_2346_WITH_CAVEATS"
    readiness["readiness_warnings"] = sorted(
        {
            *readiness.get("readiness_warnings", []),
            "DEDUP_AGAINST_EXISTING_HISTORICAL_EVENT_LOG",
        }
    )
    write_json(readiness_path, readiness)
    route_path = scheduler_dry_run_dir / "high_intensity_2346_task_route.json"
    route = read_json(route_path)
    route["readiness_status"] = "READY_FOR_2346_WITH_CAVEATS"
    route["next_task"] = summary["next_task"]
    write_json(route_path, route)
    return {
        **fixture,
        "scheduler_dry_run_dir": scheduler_dry_run_dir,
        "scheduler_dry_run_docs_root": scheduler_dry_run_docs_root,
    }
