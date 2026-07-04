from __future__ import annotations

from pathlib import Path

import pytest
from high_intensity_outcome_binder_fixtures import (
    build_high_intensity_outcome_binder_fixture,
    read_json,
    write_json,
)

from ai_trading_system.high_intensity_risk_cap_actual_path_outcome_binder import (
    HighIntensityOutcomeBinderError,
    load_high_intensity_outcome_binder_inputs,
)


def test_high_intensity_outcome_binder_loader_reads_2336_outputs(tmp_path: Path) -> None:
    fixture = build_high_intensity_outcome_binder_fixture(tmp_path)

    loaded = load_high_intensity_outcome_binder_inputs(
        event_logger_dir=fixture["event_logger_dir"],
        threshold_selection_dir=fixture["threshold_selection_dir"],
        forward_observe_plan_dir=fixture["forward_observe_plan_dir"],
        dynamic_dry_run_dir=fixture["dynamic_dry_run_dir"],
        dynamic_diagnostics_dir=fixture["dynamic_diagnostics_dir"],
    )

    event_log = read_json(fixture["event_logger_dir"] / "high_intensity_observe_event_log.json")
    assert loaded["event_logger"]["event_log"]["row_count"] == event_log["row_count"]
    assert loaded["event_logger"]["cluster_registry"]["row_count"] == 1
    assert loaded["event_logger"]["pending_registry"]["row_count"] == 4
    assert loaded["event_logger"]["outcome_schedule"]["row_count"] == 4


def test_high_intensity_outcome_binder_loader_fails_on_wrong_route(
    tmp_path: Path,
) -> None:
    fixture = build_high_intensity_outcome_binder_fixture(tmp_path)
    route_path = fixture["event_logger_dir"] / "high_intensity_2337_task_route.json"
    route = read_json(route_path)
    route["next_task"] = "TRADING-2337_Wrong_Route"
    write_json(route_path, route)

    with pytest.raises(HighIntensityOutcomeBinderError, match="route next_task"):
        load_high_intensity_outcome_binder_inputs(
            event_logger_dir=fixture["event_logger_dir"],
            threshold_selection_dir=fixture["threshold_selection_dir"],
            forward_observe_plan_dir=fixture["forward_observe_plan_dir"],
            dynamic_dry_run_dir=fixture["dynamic_dry_run_dir"],
            dynamic_diagnostics_dir=fixture["dynamic_diagnostics_dir"],
        )


def test_high_intensity_outcome_binder_loader_fails_on_unsafe_inputs(
    tmp_path: Path,
) -> None:
    fixture = build_high_intensity_outcome_binder_fixture(tmp_path)
    summary_path = fixture["event_logger_dir"] / "high_intensity_event_logger_summary.json"
    summary = read_json(summary_path)
    summary["promotion_allowed"] = True
    write_json(summary_path, summary)

    with pytest.raises(HighIntensityOutcomeBinderError, match="unsafe fields"):
        load_high_intensity_outcome_binder_inputs(
            event_logger_dir=fixture["event_logger_dir"],
            threshold_selection_dir=fixture["threshold_selection_dir"],
            forward_observe_plan_dir=fixture["forward_observe_plan_dir"],
            dynamic_dry_run_dir=fixture["dynamic_dry_run_dir"],
            dynamic_diagnostics_dir=fixture["dynamic_diagnostics_dir"],
        )


def test_high_intensity_outcome_binder_loader_fails_on_broker_action(
    tmp_path: Path,
) -> None:
    fixture = build_high_intensity_outcome_binder_fixture(tmp_path)
    summary_path = fixture["event_logger_dir"] / "high_intensity_event_logger_summary.json"
    summary = read_json(summary_path)
    summary["broker_action"] = "submit_order"
    write_json(summary_path, summary)

    with pytest.raises(HighIntensityOutcomeBinderError, match="broker_action"):
        load_high_intensity_outcome_binder_inputs(
            event_logger_dir=fixture["event_logger_dir"],
            threshold_selection_dir=fixture["threshold_selection_dir"],
            forward_observe_plan_dir=fixture["forward_observe_plan_dir"],
            dynamic_dry_run_dir=fixture["dynamic_dry_run_dir"],
            dynamic_diagnostics_dir=fixture["dynamic_diagnostics_dir"],
        )
