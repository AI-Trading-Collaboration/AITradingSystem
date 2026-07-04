from __future__ import annotations

from pathlib import Path

import pytest
from high_intensity_scheduler_plan_fixtures import (
    build_high_intensity_scheduler_plan_fixture,
    read_json,
    write_json,
)

from ai_trading_system.high_intensity_risk_cap_scheduler_integration_plan import (
    HighIntensitySchedulerIntegrationPlanError,
    load_high_intensity_scheduler_integration_plan_inputs,
)


def _load_fixture(tmp_path: Path) -> dict[str, object]:
    fixture = build_high_intensity_scheduler_plan_fixture(tmp_path)
    return load_high_intensity_scheduler_integration_plan_inputs(
        runtime_dry_run_dir=fixture["runtime_dry_run_dir"],
        runtime_integration_plan_dir=fixture["runtime_integration_plan_dir"],
        continue_decision_dir=fixture["continue_decision_dir"],
        event_logger_dir=fixture["event_logger_dir"],
        threshold_selection_dir=fixture["threshold_selection_dir"],
        forward_observe_plan_dir=fixture["forward_observe_plan_dir"],
    )


def test_scheduler_plan_loader_reads_required_artifacts(tmp_path: Path) -> None:
    loaded = _load_fixture(tmp_path)

    assert loaded["runtime_dry_run"]["summary"]["status"] == (
        "OBSERVE_ONLY_RUNTIME_DRY_RUN_READY_WITH_CAVEATS_PROMOTION_BLOCKED"
    )
    assert loaded["runtime_plan"]["summary"]["next_task"] == (
        "TRADING-2343_High_Intensity_Risk_Cap_Observe_Only_Runtime_Dry_Run"
    )
    assert loaded["threshold_selection"]["selected_rule"]["selected_rule_id"] == (
        "COMPOSITE_HIGH_INTENSITY_RULE"
    )
    assert loaded["event_logger"]["event_log"]["rows"]
    assert loaded["event_logger"]["cluster_registry"]["rows"]
    assert loaded["event_logger"]["pending_outcome_registry"]["rows"]
    assert loaded["runtime_dry_run"]["event_detection"]["rows"]


def test_scheduler_plan_loader_fails_closed_on_bad_2343_route(
    tmp_path: Path,
) -> None:
    fixture = build_high_intensity_scheduler_plan_fixture(tmp_path)
    route_path = fixture["runtime_dry_run_dir"] / "high_intensity_2344_task_route.json"
    route = read_json(route_path)
    route["next_task"] = "TRADING-2344_Archive_High_Intensity_Risk_Cap_Runtime_Line"
    write_json(route_path, route)

    with pytest.raises(HighIntensitySchedulerIntegrationPlanError):
        load_high_intensity_scheduler_integration_plan_inputs(
            runtime_dry_run_dir=fixture["runtime_dry_run_dir"],
            runtime_integration_plan_dir=fixture["runtime_integration_plan_dir"],
            continue_decision_dir=fixture["continue_decision_dir"],
            event_logger_dir=fixture["event_logger_dir"],
            threshold_selection_dir=fixture["threshold_selection_dir"],
            forward_observe_plan_dir=fixture["forward_observe_plan_dir"],
        )


def test_scheduler_plan_loader_fails_closed_on_promotion_allowed(
    tmp_path: Path,
) -> None:
    fixture = build_high_intensity_scheduler_plan_fixture(tmp_path)
    summary_path = fixture["runtime_dry_run_dir"] / "high_intensity_runtime_dry_run_summary.json"
    summary = read_json(summary_path)
    summary["promotion_allowed"] = True
    write_json(summary_path, summary)

    with pytest.raises(HighIntensitySchedulerIntegrationPlanError):
        load_high_intensity_scheduler_integration_plan_inputs(
            runtime_dry_run_dir=fixture["runtime_dry_run_dir"],
            runtime_integration_plan_dir=fixture["runtime_integration_plan_dir"],
            continue_decision_dir=fixture["continue_decision_dir"],
            event_logger_dir=fixture["event_logger_dir"],
            threshold_selection_dir=fixture["threshold_selection_dir"],
            forward_observe_plan_dir=fixture["forward_observe_plan_dir"],
        )


def test_scheduler_plan_loader_fails_closed_on_broker_action(
    tmp_path: Path,
) -> None:
    fixture = build_high_intensity_scheduler_plan_fixture(tmp_path)
    safety_path = (
        fixture["runtime_dry_run_dir"]
        / "high_intensity_runtime_dry_run_safety_boundary.json"
    )
    safety = read_json(safety_path)
    safety["broker_action"] = "submit_order"
    write_json(safety_path, safety)

    with pytest.raises(HighIntensitySchedulerIntegrationPlanError):
        load_high_intensity_scheduler_integration_plan_inputs(
            runtime_dry_run_dir=fixture["runtime_dry_run_dir"],
            runtime_integration_plan_dir=fixture["runtime_integration_plan_dir"],
            continue_decision_dir=fixture["continue_decision_dir"],
            event_logger_dir=fixture["event_logger_dir"],
            threshold_selection_dir=fixture["threshold_selection_dir"],
            forward_observe_plan_dir=fixture["forward_observe_plan_dir"],
        )
