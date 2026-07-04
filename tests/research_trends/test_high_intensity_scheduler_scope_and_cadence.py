from __future__ import annotations

from pathlib import Path

from high_intensity_scheduler_plan_fixtures import (
    build_high_intensity_scheduler_plan_fixture,
)

from ai_trading_system.high_intensity_risk_cap_scheduler_integration_plan import (
    build_high_intensity_scheduler_cadence_plan,
    build_high_intensity_scheduler_input_contract,
    build_high_intensity_scheduler_scope_contract,
    load_high_intensity_scheduler_integration_plan_inputs,
)


def _loaded_fixture(tmp_path: Path) -> dict[str, object]:
    fixture = build_high_intensity_scheduler_plan_fixture(tmp_path)
    return load_high_intensity_scheduler_integration_plan_inputs(
        runtime_dry_run_dir=fixture["runtime_dry_run_dir"],
        runtime_integration_plan_dir=fixture["runtime_integration_plan_dir"],
        continue_decision_dir=fixture["continue_decision_dir"],
        event_logger_dir=fixture["event_logger_dir"],
        threshold_selection_dir=fixture["threshold_selection_dir"],
        forward_observe_plan_dir=fixture["forward_observe_plan_dir"],
    )


def test_scheduler_scope_contract_is_observe_only(tmp_path: Path) -> None:
    inputs = _loaded_fixture(tmp_path)

    scope = build_high_intensity_scheduler_scope_contract(inputs)

    assert scope["selected_rule_id"] == "COMPOSITE_HIGH_INTENSITY_RULE"
    assert scope["scheduler_integration_plan_only"] is True
    assert scope["scheduler_enabled"] is False
    assert scope["scheduler_default_enabled"] is False
    assert scope["automatic_exposure_cap_allowed"] is False
    assert scope["target_weight_action_allowed"] is False
    assert scope["rebalance_instruction_allowed"] is False
    assert scope["paper_shadow_allowed"] is False
    assert scope["production_allowed"] is False
    assert scope["broker_action"] == "none"


def test_scheduler_cadence_plan_is_disabled_with_trading_calendar_gate() -> None:
    cadence = build_high_intensity_scheduler_cadence_plan()

    assert cadence["scheduler_enabled_in_2344"] is False
    assert cadence["scheduler_activation_allowed_in_2344"] is False
    assert cadence["calendar_policy"]["use_trading_calendar"] is True
    assert cadence["calendar_policy"]["skip_non_trading_days"] is True
    assert cadence["calendar_policy"]["next_session_policy"] is True
    assert cadence["candidate_cadence"]["event_detection"]["frequency"] == "trading_day"
    assert cadence["candidate_cadence"]["event_append"]["frequency"] == "trading_day"
    assert cadence["candidate_cadence"]["outcome_update"]["frequency"] == "trading_day"
    assert cadence["broker_action"] == "none"


def test_scheduler_input_contract_blocks_live_portfolio_inputs() -> None:
    contract = build_high_intensity_scheduler_input_contract()

    assert contract["input_validation_required"] is True
    assert contract["fail_closed_if_missing_required_input"] is True
    assert "selected_trigger_rule" in contract["required_scheduler_inputs"]
    assert "prior_event_log" in contract["required_scheduler_inputs"]
    assert "prior_cluster_registry" in contract["required_scheduler_inputs"]
    assert "prior_pending_outcome_registry" in contract["required_scheduler_inputs"]
    assert "trading_calendar" in contract["required_scheduler_inputs"]
    assert "broker_position" in contract["blocked_inputs"]
    assert "real_portfolio_weight" in contract["blocked_inputs"]
    assert contract["promotion_allowed"] is False
    assert contract["broker_action"] == "none"
