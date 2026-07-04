from __future__ import annotations

from pathlib import Path

from high_intensity_continue_observe_fixtures import build_high_intensity_continue_observe_fixture

from ai_trading_system.high_intensity_risk_cap_continue_forward_observe_decision import (
    build_high_intensity_continue_observe_decision_matrix,
    load_high_intensity_continue_observe_decision_inputs,
)


def test_continue_decision_matrix_confirms_observe_with_caveats(
    tmp_path: Path,
) -> None:
    fixture = build_high_intensity_continue_observe_fixture(tmp_path)
    inputs = load_high_intensity_continue_observe_decision_inputs(
        forward_outcome_review_dir=fixture["forward_outcome_review_dir"],
        partial_readiness_dir=fixture["partial_readiness_dir"],
        outcome_binder_dir=fixture["outcome_binder_dir"],
        event_logger_dir=fixture["event_logger_dir"],
        threshold_selection_dir=fixture["threshold_selection_dir"],
        forward_observe_plan_dir=fixture["forward_observe_plan_dir"],
    )

    decision = build_high_intensity_continue_observe_decision_matrix(inputs)

    assert decision["decision_status"] == (
        "CONTINUE_DECISION_CONFIRMED_WITH_CAVEATS_PROMOTION_BLOCKED"
    )
    assert decision["overall_decision"] == "CONTINUE_OBSERVE_ONLY_WITH_PARTIAL_COVERAGE_CAVEAT"
    assert decision["partial_coverage_caveat_required"] is True
    assert decision["monthly_concentration_monitoring_required"] is True
    assert decision["promotion_allowed"] is False
    assert decision["paper_shadow_allowed"] is False
    assert decision["production_allowed"] is False
    assert decision["broker_action"] == "none"
