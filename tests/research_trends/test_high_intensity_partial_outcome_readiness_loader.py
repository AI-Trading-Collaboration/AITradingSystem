from __future__ import annotations

from pathlib import Path

import pytest
from high_intensity_partial_outcome_readiness_fixtures import (
    build_high_intensity_partial_outcome_readiness_fixture,
    read_json,
    write_json,
)

from ai_trading_system.high_intensity_risk_cap_partial_outcome_readiness_review import (
    HighIntensityPartialOutcomeReadinessError,
    load_high_intensity_partial_outcome_readiness_inputs,
)


def test_partial_outcome_readiness_loader_reads_required_artifacts(tmp_path: Path) -> None:
    fixture = build_high_intensity_partial_outcome_readiness_fixture(tmp_path)

    loaded = load_high_intensity_partial_outcome_readiness_inputs(
        outcome_binder_dir=fixture["outcome_binder_dir"],
        event_logger_dir=fixture["event_logger_dir"],
        threshold_selection_dir=fixture["threshold_selection_dir"],
        forward_observe_plan_dir=fixture["forward_observe_plan_dir"],
    )

    assert loaded["outcome_binder"]["summary"]["status"] == (
        "PARTIAL_OUTCOME_BINDING_WITH_NOT_DUE_HORIZONS"
    )
    assert len(loaded["outcome_binder"]["event_matrix"]["rows"]) == 240
    assert len(loaded["outcome_binder"]["cluster_matrix"]["rows"]) == 240
    assert loaded["outcome_binder"]["data_quality_report"]["validate_data_error_count"] == 0
    assert len(loaded["event_logger"]["cluster_registry"]["rows"]) == 60


def test_partial_outcome_readiness_loader_fails_closed_on_bad_route(
    tmp_path: Path,
) -> None:
    fixture = build_high_intensity_partial_outcome_readiness_fixture(tmp_path)
    route_path = fixture["outcome_binder_dir"] / "high_intensity_2339_task_route.json"
    route = read_json(route_path)
    route["next_task"] = "TRADING-9999_Wrong_Route"
    write_json(route_path, route)

    with pytest.raises(HighIntensityPartialOutcomeReadinessError):
        load_high_intensity_partial_outcome_readiness_inputs(
            outcome_binder_dir=fixture["outcome_binder_dir"],
            event_logger_dir=fixture["event_logger_dir"],
            threshold_selection_dir=fixture["threshold_selection_dir"],
            forward_observe_plan_dir=fixture["forward_observe_plan_dir"],
        )


def test_partial_outcome_readiness_loader_fails_closed_on_unsafe_inputs(
    tmp_path: Path,
) -> None:
    fixture = build_high_intensity_partial_outcome_readiness_fixture(tmp_path)
    summary_path = fixture["outcome_binder_dir"] / "high_intensity_outcome_binder_summary.json"
    summary = read_json(summary_path)
    summary["promotion_allowed"] = True
    write_json(summary_path, summary)

    with pytest.raises(HighIntensityPartialOutcomeReadinessError):
        load_high_intensity_partial_outcome_readiness_inputs(
            outcome_binder_dir=fixture["outcome_binder_dir"],
            event_logger_dir=fixture["event_logger_dir"],
            threshold_selection_dir=fixture["threshold_selection_dir"],
            forward_observe_plan_dir=fixture["forward_observe_plan_dir"],
        )


def test_partial_outcome_readiness_loader_fails_closed_on_broker_action(
    tmp_path: Path,
) -> None:
    fixture = build_high_intensity_partial_outcome_readiness_fixture(tmp_path)
    safety_path = (
        fixture["outcome_binder_dir"] / "high_intensity_outcome_binder_safety_boundary.json"
    )
    safety = read_json(safety_path)
    safety["broker_action"] = "submit_order"
    write_json(safety_path, safety)

    with pytest.raises(HighIntensityPartialOutcomeReadinessError):
        load_high_intensity_partial_outcome_readiness_inputs(
            outcome_binder_dir=fixture["outcome_binder_dir"],
            event_logger_dir=fixture["event_logger_dir"],
            threshold_selection_dir=fixture["threshold_selection_dir"],
            forward_observe_plan_dir=fixture["forward_observe_plan_dir"],
        )
