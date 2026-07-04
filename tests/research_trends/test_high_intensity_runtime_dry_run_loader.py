from __future__ import annotations

from pathlib import Path

import pytest
from high_intensity_runtime_dry_run_fixtures import (
    build_high_intensity_runtime_dry_run_fixture,
    read_json,
    write_json,
)

from ai_trading_system.high_intensity_risk_cap_observe_only_runtime_dry_run import (
    HighIntensityRuntimeDryRunError,
    load_high_intensity_runtime_dry_run_inputs,
)


def test_runtime_dry_run_loader_reads_required_artifacts(tmp_path: Path) -> None:
    fixture = build_high_intensity_runtime_dry_run_fixture(tmp_path)

    loaded = load_high_intensity_runtime_dry_run_inputs(
        runtime_integration_plan_dir=fixture["runtime_integration_plan_dir"],
        continue_decision_dir=fixture["continue_decision_dir"],
        event_logger_dir=fixture["event_logger_dir"],
        threshold_selection_dir=fixture["threshold_selection_dir"],
        forward_observe_plan_dir=fixture["forward_observe_plan_dir"],
        dynamic_dry_run_dir=fixture["dynamic_dry_run_dir"],
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
    assert loaded["dynamic_dry_run"]["trigger_alignment"]["rows"]


def test_runtime_dry_run_loader_fails_closed_on_bad_2342_route(
    tmp_path: Path,
) -> None:
    fixture = build_high_intensity_runtime_dry_run_fixture(tmp_path)
    route_path = fixture["runtime_integration_plan_dir"] / "high_intensity_2343_task_route.json"
    route = read_json(route_path)
    route["next_task"] = "TRADING-2343_Archive_High_Intensity_Risk_Cap_Runtime_Line"
    write_json(route_path, route)

    with pytest.raises(HighIntensityRuntimeDryRunError):
        load_high_intensity_runtime_dry_run_inputs(
            runtime_integration_plan_dir=fixture["runtime_integration_plan_dir"],
            continue_decision_dir=fixture["continue_decision_dir"],
            event_logger_dir=fixture["event_logger_dir"],
            threshold_selection_dir=fixture["threshold_selection_dir"],
            forward_observe_plan_dir=fixture["forward_observe_plan_dir"],
            dynamic_dry_run_dir=fixture["dynamic_dry_run_dir"],
        )


def test_runtime_dry_run_loader_fails_closed_on_promotion_allowed(
    tmp_path: Path,
) -> None:
    fixture = build_high_intensity_runtime_dry_run_fixture(tmp_path)
    summary_path = (
        fixture["runtime_integration_plan_dir"]
        / "high_intensity_runtime_integration_plan_summary.json"
    )
    summary = read_json(summary_path)
    summary["promotion_allowed"] = True
    write_json(summary_path, summary)

    with pytest.raises(HighIntensityRuntimeDryRunError):
        load_high_intensity_runtime_dry_run_inputs(
            runtime_integration_plan_dir=fixture["runtime_integration_plan_dir"],
            continue_decision_dir=fixture["continue_decision_dir"],
            event_logger_dir=fixture["event_logger_dir"],
            threshold_selection_dir=fixture["threshold_selection_dir"],
            forward_observe_plan_dir=fixture["forward_observe_plan_dir"],
            dynamic_dry_run_dir=fixture["dynamic_dry_run_dir"],
        )


def test_runtime_dry_run_loader_fails_closed_on_broker_action(
    tmp_path: Path,
) -> None:
    fixture = build_high_intensity_runtime_dry_run_fixture(tmp_path)
    safety_path = (
        fixture["runtime_integration_plan_dir"]
        / "high_intensity_runtime_integration_safety_boundary.json"
    )
    safety = read_json(safety_path)
    safety["broker_action"] = "submit_order"
    write_json(safety_path, safety)

    with pytest.raises(HighIntensityRuntimeDryRunError):
        load_high_intensity_runtime_dry_run_inputs(
            runtime_integration_plan_dir=fixture["runtime_integration_plan_dir"],
            continue_decision_dir=fixture["continue_decision_dir"],
            event_logger_dir=fixture["event_logger_dir"],
            threshold_selection_dir=fixture["threshold_selection_dir"],
            forward_observe_plan_dir=fixture["forward_observe_plan_dir"],
            dynamic_dry_run_dir=fixture["dynamic_dry_run_dir"],
        )
