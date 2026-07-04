from __future__ import annotations

from pathlib import Path

from high_intensity_continue_observe_fixtures import build_high_intensity_continue_observe_fixture

from ai_trading_system.high_intensity_risk_cap_continue_forward_observe_decision import (
    build_high_intensity_manual_review_context_policy,
    build_high_intensity_monthly_concentration_monitoring_plan,
    build_high_intensity_outcome_update_policy,
    build_high_intensity_partial_coverage_carryforward_caveat,
    build_high_intensity_stop_refine_archive_policy,
    load_high_intensity_continue_observe_decision_inputs,
)


def test_monitoring_and_policy_artifacts_keep_caveats(tmp_path: Path) -> None:
    fixture = build_high_intensity_continue_observe_fixture(tmp_path)
    inputs = load_high_intensity_continue_observe_decision_inputs(
        forward_outcome_review_dir=fixture["forward_outcome_review_dir"],
        partial_readiness_dir=fixture["partial_readiness_dir"],
        outcome_binder_dir=fixture["outcome_binder_dir"],
        event_logger_dir=fixture["event_logger_dir"],
        threshold_selection_dir=fixture["threshold_selection_dir"],
        forward_observe_plan_dir=fixture["forward_observe_plan_dir"],
    )

    monthly = build_high_intensity_monthly_concentration_monitoring_plan(inputs)
    caveat = build_high_intensity_partial_coverage_carryforward_caveat(inputs)
    outcome_policy = build_high_intensity_outcome_update_policy()
    stop_policy = build_high_intensity_stop_refine_archive_policy(inputs)
    manual_policy = build_high_intensity_manual_review_context_policy()

    assert monthly["monitoring_required"] is True
    assert monthly["monitoring_status"] == "MONITORING_REQUIRED_WITH_STRICT_GUARDRAILS"
    assert caveat["partial_coverage_caveat_required"] is True
    assert caveat["caveat_materiality"] == "LOW"
    assert outcome_policy["outcome_update_mode"] == "scheduled_actual_path_binding"
    assert stop_policy["minimum_new_event_count_before_next_review"] == 20
    assert "reduce position instruction" in manual_policy["manual_review_context_not_allowed"]
    assert "reduce position instruction" not in manual_policy["manual_review_context_scope"]
