from __future__ import annotations

from pathlib import Path

import pytest
from high_intensity_scheduler_wiring_plan_fixtures import (
    build_high_intensity_scheduler_wiring_plan_fixture,
    read_json,
    write_json,
)

from ai_trading_system.high_intensity_risk_cap_scheduler_wiring_plan import (
    HighIntensitySchedulerWiringPlanError,
    load_high_intensity_scheduler_wiring_plan_inputs,
)


def _load(tmp_path: Path) -> dict[str, object]:
    fixture = build_high_intensity_scheduler_wiring_plan_fixture(tmp_path)
    return load_high_intensity_scheduler_wiring_plan_inputs(
        scheduler_dry_run_dir=fixture["scheduler_dry_run_dir"],
        scheduler_integration_plan_dir=fixture["scheduler_integration_plan_dir"],
        runtime_dry_run_dir=fixture["runtime_dry_run_dir"],
        runtime_integration_plan_dir=fixture["runtime_integration_plan_dir"],
        continue_decision_dir=fixture["continue_decision_dir"],
        event_logger_dir=fixture["event_logger_dir"],
        threshold_selection_dir=fixture["threshold_selection_dir"],
    )


def test_scheduler_wiring_plan_loader_reads_required_artifacts(
    tmp_path: Path,
) -> None:
    loaded = _load(tmp_path)

    assert loaded["scheduler_dry_run"]["summary"]["next_task"] == (
        "TRADING-2346_High_Intensity_Risk_Cap_Observe_Only_Scheduler_Wiring_Plan"
    )
    assert loaded["scheduler_dry_run"]["contract_validation"]
    assert loaded["scheduler_plan"]["disabled_policy"]
    assert loaded["threshold_selection"]["selected_rule"]["selected_rule_id"] == (
        "COMPOSITE_HIGH_INTENSITY_RULE"
    )
    assert loaded["event_logger"]["event_log"]["rows"]
    assert loaded["event_logger"]["cluster_registry"]["rows"]
    assert loaded["event_logger"]["pending_outcome_registry"]["rows"]


def test_scheduler_wiring_plan_loader_fails_closed_on_bad_2345_route(
    tmp_path: Path,
) -> None:
    fixture = build_high_intensity_scheduler_wiring_plan_fixture(tmp_path)
    route_path = fixture["scheduler_dry_run_dir"] / "high_intensity_2346_task_route.json"
    route = read_json(route_path)
    route["next_task"] = "TRADING-2346_Bad_Route"
    write_json(route_path, route)

    with pytest.raises(HighIntensitySchedulerWiringPlanError):
        load_high_intensity_scheduler_wiring_plan_inputs(
            scheduler_dry_run_dir=fixture["scheduler_dry_run_dir"],
            scheduler_integration_plan_dir=fixture["scheduler_integration_plan_dir"],
            runtime_dry_run_dir=fixture["runtime_dry_run_dir"],
            runtime_integration_plan_dir=fixture["runtime_integration_plan_dir"],
            continue_decision_dir=fixture["continue_decision_dir"],
            event_logger_dir=fixture["event_logger_dir"],
            threshold_selection_dir=fixture["threshold_selection_dir"],
        )


def test_scheduler_wiring_plan_loader_fails_closed_on_promotion_allowed(
    tmp_path: Path,
) -> None:
    fixture = build_high_intensity_scheduler_wiring_plan_fixture(tmp_path)
    summary_path = (
        fixture["scheduler_dry_run_dir"]
        / "high_intensity_scheduler_dry_run_summary.json"
    )
    summary = read_json(summary_path)
    summary["promotion_allowed"] = True
    write_json(summary_path, summary)

    with pytest.raises(HighIntensitySchedulerWiringPlanError):
        load_high_intensity_scheduler_wiring_plan_inputs(
            scheduler_dry_run_dir=fixture["scheduler_dry_run_dir"],
            scheduler_integration_plan_dir=fixture["scheduler_integration_plan_dir"],
            runtime_dry_run_dir=fixture["runtime_dry_run_dir"],
            runtime_integration_plan_dir=fixture["runtime_integration_plan_dir"],
            continue_decision_dir=fixture["continue_decision_dir"],
            event_logger_dir=fixture["event_logger_dir"],
            threshold_selection_dir=fixture["threshold_selection_dir"],
        )


def test_scheduler_wiring_plan_loader_fails_closed_on_broker_action(
    tmp_path: Path,
) -> None:
    fixture = build_high_intensity_scheduler_wiring_plan_fixture(tmp_path)
    safety_path = (
        fixture["scheduler_dry_run_dir"]
        / "high_intensity_scheduler_dry_run_safety_boundary.json"
    )
    safety = read_json(safety_path)
    safety["broker_action"] = "BUY"
    write_json(safety_path, safety)

    with pytest.raises(HighIntensitySchedulerWiringPlanError):
        load_high_intensity_scheduler_wiring_plan_inputs(
            scheduler_dry_run_dir=fixture["scheduler_dry_run_dir"],
            scheduler_integration_plan_dir=fixture["scheduler_integration_plan_dir"],
            runtime_dry_run_dir=fixture["runtime_dry_run_dir"],
            runtime_integration_plan_dir=fixture["runtime_integration_plan_dir"],
            continue_decision_dir=fixture["continue_decision_dir"],
            event_logger_dir=fixture["event_logger_dir"],
            threshold_selection_dir=fixture["threshold_selection_dir"],
        )
