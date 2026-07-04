from __future__ import annotations

from pathlib import Path

import pytest
from high_intensity_forward_outcome_review_fixtures import (
    build_high_intensity_forward_outcome_review_fixture,
    read_json,
    write_json,
)

from ai_trading_system.high_intensity_risk_cap_forward_outcome_review import (
    HighIntensityForwardOutcomeReviewError,
    load_high_intensity_forward_outcome_review_inputs,
)


def test_forward_outcome_review_loader_reads_required_artifacts(tmp_path: Path) -> None:
    fixture = build_high_intensity_forward_outcome_review_fixture(tmp_path)

    loaded = load_high_intensity_forward_outcome_review_inputs(
        partial_readiness_dir=fixture["partial_readiness_dir"],
        outcome_binder_dir=fixture["outcome_binder_dir"],
        event_logger_dir=fixture["event_logger_dir"],
        threshold_selection_dir=fixture["threshold_selection_dir"],
        forward_observe_plan_dir=fixture["forward_observe_plan_dir"],
    )

    assert loaded["partial_readiness"]["summary"]["status"] == (
        "READY_FOR_2340_FORWARD_OUTCOME_REVIEW_WITH_PARTIAL_COVERAGE_CAVEAT"
    )
    assert len(loaded["outcome_binder"]["cluster_matrix"]["rows"]) == 240
    assert loaded["outcome_binder"]["false_warning_report"]["promotion_allowed"] is False
    assert loaded["outcome_binder"]["missed_upside_report"]["promotion_allowed"] is False
    assert loaded["outcome_binder"]["downside_capture_report"]["promotion_allowed"] is False
    assert loaded["outcome_binder"]["manual_review_report"]["promotion_allowed"] is False


def test_forward_outcome_review_loader_fails_closed_on_bad_2339_route(
    tmp_path: Path,
) -> None:
    fixture = build_high_intensity_forward_outcome_review_fixture(tmp_path)
    route_path = fixture["partial_readiness_dir"] / "high_intensity_2340_task_route.json"
    route = read_json(route_path)
    route["next_task"] = "TRADING-9999_Wrong_Route"
    write_json(route_path, route)

    with pytest.raises(HighIntensityForwardOutcomeReviewError):
        load_high_intensity_forward_outcome_review_inputs(
            partial_readiness_dir=fixture["partial_readiness_dir"],
            outcome_binder_dir=fixture["outcome_binder_dir"],
            event_logger_dir=fixture["event_logger_dir"],
            threshold_selection_dir=fixture["threshold_selection_dir"],
            forward_observe_plan_dir=fixture["forward_observe_plan_dir"],
        )


def test_forward_outcome_review_loader_fails_closed_on_unsafe_input(
    tmp_path: Path,
) -> None:
    fixture = build_high_intensity_forward_outcome_review_fixture(tmp_path)
    summary_path = fixture["partial_readiness_dir"] / (
        "high_intensity_partial_outcome_readiness_summary.json"
    )
    summary = read_json(summary_path)
    summary["promotion_allowed"] = True
    write_json(summary_path, summary)

    with pytest.raises(HighIntensityForwardOutcomeReviewError):
        load_high_intensity_forward_outcome_review_inputs(
            partial_readiness_dir=fixture["partial_readiness_dir"],
            outcome_binder_dir=fixture["outcome_binder_dir"],
            event_logger_dir=fixture["event_logger_dir"],
            threshold_selection_dir=fixture["threshold_selection_dir"],
            forward_observe_plan_dir=fixture["forward_observe_plan_dir"],
        )


def test_forward_outcome_review_loader_fails_closed_on_broker_action(
    tmp_path: Path,
) -> None:
    fixture = build_high_intensity_forward_outcome_review_fixture(tmp_path)
    safety_path = (
        fixture["outcome_binder_dir"] / "high_intensity_outcome_binder_safety_boundary.json"
    )
    safety = read_json(safety_path)
    safety["broker_action"] = "submit_order"
    write_json(safety_path, safety)

    with pytest.raises(HighIntensityForwardOutcomeReviewError):
        load_high_intensity_forward_outcome_review_inputs(
            partial_readiness_dir=fixture["partial_readiness_dir"],
            outcome_binder_dir=fixture["outcome_binder_dir"],
            event_logger_dir=fixture["event_logger_dir"],
            threshold_selection_dir=fixture["threshold_selection_dir"],
            forward_observe_plan_dir=fixture["forward_observe_plan_dir"],
        )
