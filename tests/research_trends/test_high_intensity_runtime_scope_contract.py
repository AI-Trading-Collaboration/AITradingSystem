from __future__ import annotations

from pathlib import Path

from high_intensity_runtime_integration_plan_fixtures import (
    build_high_intensity_runtime_integration_plan_fixture,
)

from ai_trading_system.high_intensity_risk_cap_observe_only_runtime_integration_plan import (
    build_high_intensity_runtime_scope_contract,
    load_high_intensity_runtime_integration_plan_inputs,
)


def test_runtime_scope_contract_is_observe_only(tmp_path: Path) -> None:
    fixture = build_high_intensity_runtime_integration_plan_fixture(tmp_path)
    inputs = load_high_intensity_runtime_integration_plan_inputs(
        continue_decision_dir=fixture["continue_decision_dir"],
        forward_outcome_review_dir=fixture["forward_outcome_review_dir"],
        partial_readiness_dir=fixture["partial_readiness_dir"],
        outcome_binder_dir=fixture["outcome_binder_dir"],
        event_logger_dir=fixture["event_logger_dir"],
        threshold_selection_dir=fixture["threshold_selection_dir"],
        forward_observe_plan_dir=fixture["forward_observe_plan_dir"],
    )

    scope = build_high_intensity_runtime_scope_contract(inputs)

    assert scope["runtime_mode"] == "observe_only"
    assert scope["runtime_scheduler_enabled"] is False
    assert scope["automatic_exposure_cap_allowed"] is False
    assert scope["manual_review_context_allowed"] is True
    assert scope["selected_rule_id"] == "COMPOSITE_HIGH_INTENSITY_RULE"
    assert scope["partial_coverage_caveat_required"] is True
