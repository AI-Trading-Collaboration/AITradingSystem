from __future__ import annotations

from pathlib import Path

import pytest
from high_intensity_scheduler_dry_run_fixtures import (
    build_high_intensity_scheduler_dry_run_fixture,
    read_json,
    write_json,
)

from ai_trading_system.high_intensity_risk_cap_scheduler_dry_run import (
    HighIntensitySchedulerDryRunError,
    load_high_intensity_scheduler_dry_run_inputs,
)


def _load(tmp_path: Path) -> dict[str, object]:
    fixture = build_high_intensity_scheduler_dry_run_fixture(tmp_path)
    return load_high_intensity_scheduler_dry_run_inputs(
        scheduler_integration_plan_dir=fixture["scheduler_integration_plan_dir"],
        runtime_dry_run_dir=fixture["runtime_dry_run_dir"],
        runtime_integration_plan_dir=fixture["runtime_integration_plan_dir"],
        continue_decision_dir=fixture["continue_decision_dir"],
        event_logger_dir=fixture["event_logger_dir"],
        threshold_selection_dir=fixture["threshold_selection_dir"],
        forward_observe_plan_dir=fixture["forward_observe_plan_dir"],
        dynamic_dry_run_dir=fixture["dynamic_dry_run_dir"],
    )


def test_scheduler_dry_run_loader_reads_required_artifacts(tmp_path: Path) -> None:
    loaded = _load(tmp_path)

    assert loaded["scheduler_plan"]["summary"]["next_task"] == (
        "TRADING-2345_High_Intensity_Risk_Cap_Observe_Only_Scheduler_Dry_Run"
    )
    assert loaded["scheduler_plan"]["event_detection_contract"]
    assert loaded["runtime_dry_run"]["summary"]["status"] == (
        "OBSERVE_ONLY_RUNTIME_DRY_RUN_READY_WITH_CAVEATS_PROMOTION_BLOCKED"
    )
    assert loaded["runtime_inputs"]["threshold_selection"]["selected_rule"][
        "selected_rule_id"
    ] == "COMPOSITE_HIGH_INTENSITY_RULE"
    assert loaded["runtime_inputs"]["event_logger"]["event_log"]["rows"]
    assert loaded["runtime_inputs"]["event_logger"]["cluster_registry"]["rows"]
    assert loaded["runtime_inputs"]["event_logger"]["pending_outcome_registry"]["rows"]


def test_scheduler_dry_run_loader_fails_closed_on_bad_2344_route(
    tmp_path: Path,
) -> None:
    fixture = build_high_intensity_scheduler_dry_run_fixture(tmp_path)
    route_path = fixture["scheduler_integration_plan_dir"] / "high_intensity_2345_task_route.json"
    route = read_json(route_path)
    route["next_task"] = "TRADING-2345_Bad_Route"
    write_json(route_path, route)

    with pytest.raises(HighIntensitySchedulerDryRunError):
        load_high_intensity_scheduler_dry_run_inputs(
            scheduler_integration_plan_dir=fixture["scheduler_integration_plan_dir"],
            runtime_dry_run_dir=fixture["runtime_dry_run_dir"],
            runtime_integration_plan_dir=fixture["runtime_integration_plan_dir"],
            continue_decision_dir=fixture["continue_decision_dir"],
            event_logger_dir=fixture["event_logger_dir"],
            threshold_selection_dir=fixture["threshold_selection_dir"],
            forward_observe_plan_dir=fixture["forward_observe_plan_dir"],
            dynamic_dry_run_dir=fixture["dynamic_dry_run_dir"],
        )


def test_scheduler_dry_run_loader_fails_closed_on_promotion_allowed(
    tmp_path: Path,
) -> None:
    fixture = build_high_intensity_scheduler_dry_run_fixture(tmp_path)
    summary_path = (
        fixture["scheduler_integration_plan_dir"]
        / "high_intensity_scheduler_integration_plan_summary.json"
    )
    summary = read_json(summary_path)
    summary["promotion_allowed"] = True
    write_json(summary_path, summary)

    with pytest.raises(HighIntensitySchedulerDryRunError):
        load_high_intensity_scheduler_dry_run_inputs(
            scheduler_integration_plan_dir=fixture["scheduler_integration_plan_dir"],
            runtime_dry_run_dir=fixture["runtime_dry_run_dir"],
            runtime_integration_plan_dir=fixture["runtime_integration_plan_dir"],
            continue_decision_dir=fixture["continue_decision_dir"],
            event_logger_dir=fixture["event_logger_dir"],
            threshold_selection_dir=fixture["threshold_selection_dir"],
            forward_observe_plan_dir=fixture["forward_observe_plan_dir"],
            dynamic_dry_run_dir=fixture["dynamic_dry_run_dir"],
        )


def test_scheduler_dry_run_loader_fails_closed_on_broker_action(
    tmp_path: Path,
) -> None:
    fixture = build_high_intensity_scheduler_dry_run_fixture(tmp_path)
    safety_path = (
        fixture["scheduler_integration_plan_dir"]
        / "high_intensity_scheduler_integration_safety_boundary.json"
    )
    safety = read_json(safety_path)
    safety["broker_action"] = "BUY"
    write_json(safety_path, safety)

    with pytest.raises(HighIntensitySchedulerDryRunError):
        load_high_intensity_scheduler_dry_run_inputs(
            scheduler_integration_plan_dir=fixture["scheduler_integration_plan_dir"],
            runtime_dry_run_dir=fixture["runtime_dry_run_dir"],
            runtime_integration_plan_dir=fixture["runtime_integration_plan_dir"],
            continue_decision_dir=fixture["continue_decision_dir"],
            event_logger_dir=fixture["event_logger_dir"],
            threshold_selection_dir=fixture["threshold_selection_dir"],
            forward_observe_plan_dir=fixture["forward_observe_plan_dir"],
            dynamic_dry_run_dir=fixture["dynamic_dry_run_dir"],
        )
