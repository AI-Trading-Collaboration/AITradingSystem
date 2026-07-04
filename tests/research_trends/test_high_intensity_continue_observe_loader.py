from __future__ import annotations

from pathlib import Path

import pytest
from high_intensity_continue_observe_fixtures import (
    build_high_intensity_continue_observe_fixture,
    read_json,
    write_json,
)

from ai_trading_system.high_intensity_risk_cap_continue_forward_observe_decision import (
    HighIntensityContinueObserveDecisionError,
    load_high_intensity_continue_observe_decision_inputs,
)


def test_continue_observe_loader_reads_required_artifacts(tmp_path: Path) -> None:
    fixture = build_high_intensity_continue_observe_fixture(tmp_path)

    loaded = load_high_intensity_continue_observe_decision_inputs(
        forward_outcome_review_dir=fixture["forward_outcome_review_dir"],
        partial_readiness_dir=fixture["partial_readiness_dir"],
        outcome_binder_dir=fixture["outcome_binder_dir"],
        event_logger_dir=fixture["event_logger_dir"],
        threshold_selection_dir=fixture["threshold_selection_dir"],
        forward_observe_plan_dir=fixture["forward_observe_plan_dir"],
    )

    assert loaded["forward_outcome_review"]["summary"]["overall_recommendation"] == (
        "CONTINUE_HIGH_INTENSITY_FORWARD_OBSERVE"
    )
    assert loaded["partial_readiness"]["summary"]["not_due_outcome_count"] == 9
    assert loaded["outcome_binder"]["summary"]["validate_data_status"] == "PASS_WITH_WARNINGS"
    assert loaded["event_logger"]["summary"]["cluster_count"] == 60
    assert loaded["threshold_selection"]["selected_rule"]["selected_rule_id"] == (
        "COMPOSITE_HIGH_INTENSITY_RULE"
    )


def test_continue_observe_loader_fails_closed_on_bad_2340_recommendation(
    tmp_path: Path,
) -> None:
    fixture = build_high_intensity_continue_observe_fixture(tmp_path)
    summary_path = (
        fixture["forward_outcome_review_dir"]
        / "high_intensity_forward_outcome_review_summary.json"
    )
    summary = read_json(summary_path)
    summary["overall_recommendation"] = "ARCHIVE_HIGH_INTENSITY_RISK_CAP_LINE"
    write_json(summary_path, summary)

    with pytest.raises(HighIntensityContinueObserveDecisionError):
        load_high_intensity_continue_observe_decision_inputs(
            forward_outcome_review_dir=fixture["forward_outcome_review_dir"],
            partial_readiness_dir=fixture["partial_readiness_dir"],
            outcome_binder_dir=fixture["outcome_binder_dir"],
            event_logger_dir=fixture["event_logger_dir"],
            threshold_selection_dir=fixture["threshold_selection_dir"],
            forward_observe_plan_dir=fixture["forward_observe_plan_dir"],
        )


def test_continue_observe_loader_fails_closed_on_unsafe_input(
    tmp_path: Path,
) -> None:
    fixture = build_high_intensity_continue_observe_fixture(tmp_path)
    summary_path = (
        fixture["forward_outcome_review_dir"]
        / "high_intensity_forward_outcome_review_summary.json"
    )
    summary = read_json(summary_path)
    summary["promotion_allowed"] = True
    write_json(summary_path, summary)

    with pytest.raises(HighIntensityContinueObserveDecisionError):
        load_high_intensity_continue_observe_decision_inputs(
            forward_outcome_review_dir=fixture["forward_outcome_review_dir"],
            partial_readiness_dir=fixture["partial_readiness_dir"],
            outcome_binder_dir=fixture["outcome_binder_dir"],
            event_logger_dir=fixture["event_logger_dir"],
            threshold_selection_dir=fixture["threshold_selection_dir"],
            forward_observe_plan_dir=fixture["forward_observe_plan_dir"],
        )


def test_continue_observe_loader_fails_closed_on_broker_action(
    tmp_path: Path,
) -> None:
    fixture = build_high_intensity_continue_observe_fixture(tmp_path)
    safety_path = (
        fixture["forward_outcome_review_dir"]
        / "high_intensity_forward_outcome_safety_boundary.json"
    )
    safety = read_json(safety_path)
    safety["broker_action"] = "submit_order"
    write_json(safety_path, safety)

    with pytest.raises(HighIntensityContinueObserveDecisionError):
        load_high_intensity_continue_observe_decision_inputs(
            forward_outcome_review_dir=fixture["forward_outcome_review_dir"],
            partial_readiness_dir=fixture["partial_readiness_dir"],
            outcome_binder_dir=fixture["outcome_binder_dir"],
            event_logger_dir=fixture["event_logger_dir"],
            threshold_selection_dir=fixture["threshold_selection_dir"],
            forward_observe_plan_dir=fixture["forward_observe_plan_dir"],
        )
