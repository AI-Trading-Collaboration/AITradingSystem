from __future__ import annotations

from pathlib import Path

from high_intensity_scheduler_plan_fixtures import (
    build_high_intensity_scheduler_plan_fixture,
)

from ai_trading_system.high_intensity_risk_cap_scheduler_integration_plan import (
    build_high_intensity_scheduler_cluster_update_job_contract,
    build_high_intensity_scheduler_event_append_job_contract,
    build_high_intensity_scheduler_event_detection_job_contract,
    build_high_intensity_scheduler_manual_review_context_contract,
    build_high_intensity_scheduler_monthly_concentration_monitoring_contract,
    build_high_intensity_scheduler_outcome_update_job_contract,
    build_high_intensity_scheduler_pending_outcome_update_job_contract,
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


def test_scheduler_event_detection_job_blocks_portfolio_actions(
    tmp_path: Path,
) -> None:
    inputs = _loaded_fixture(tmp_path)

    contract = build_high_intensity_scheduler_event_detection_job_contract(inputs)

    assert contract["selected_rule_id"] == "COMPOSITE_HIGH_INTENSITY_RULE"
    assert contract["scheduler_enabled_in_2344"] is False
    assert "risk_cap_triggered" in contract["required_fields"]
    assert "high_intensity_triggered" in contract["job_output"]
    assert "target_weight" in contract["blocked_output"]
    assert "rebalance_instruction" in contract["blocked_output"]
    assert contract["promotion_allowed"] is False
    assert contract["broker_action"] == "none"


def test_scheduler_append_cluster_and_pending_jobs_are_observe_only() -> None:
    append = build_high_intensity_scheduler_event_append_job_contract()
    cluster = build_high_intensity_scheduler_cluster_update_job_contract()
    pending = build_high_intensity_scheduler_pending_outcome_update_job_contract()

    assert append["append_mode"] == "append_only"
    assert append["event_status_on_create"] == "OBSERVE_PENDING"
    assert append["manual_review_observation_flag_on_create"] is True
    assert append["original_event_log_mutation_allowed"] is False
    assert "target_weight" in append["blocked_output"]
    assert cluster["cluster_update_mode"] == "append_or_extend_open_cluster"
    assert cluster["monthly_concentration_tracking_required"] is True
    assert pending["pending_registry_update_mode"] == "append_only"
    assert pending["outcome_status_on_create"] == "OUTCOME_PENDING"
    assert pending["horizons"] == ["1d", "5d", "10d", "20d"]
    assert pending["outcome_binding_allowed_in_event_append_job"] is False
    assert pending["broker_action"] == "none"


def test_scheduler_outcome_job_requires_future_validate_data() -> None:
    outcome = build_high_intensity_scheduler_outcome_update_job_contract()

    assert outcome["future_scheduler_candidate"] is True
    assert outcome["scheduler_enabled_in_2344"] is False
    assert outcome["requires_market_data"] is True
    assert outcome["requires_validate_data"] is True
    assert outcome["validate_data_policy"]["canonical_validate_data_required"] is True
    assert outcome["validate_data_policy"]["no_rule_relaxation"] is True
    assert outcome["original_event_log_mutation_allowed"] is False
    assert outcome["paper_shadow_allowed"] is False
    assert outcome["production_allowed"] is False
    assert outcome["broker_action"] == "none"


def test_scheduler_manual_and_monthly_contracts_preserve_review_context(
    tmp_path: Path,
) -> None:
    inputs = _loaded_fixture(tmp_path)

    manual = build_high_intensity_scheduler_manual_review_context_contract()
    monthly = build_high_intensity_scheduler_monthly_concentration_monitoring_contract(
        inputs
    )

    assert manual["manual_review_context_allowed"] is True
    assert manual["display_mode"] == "risk_warning_context_only"
    assert "event_id" in manual["allowed_display_fields"]
    assert "target_weight" in manual["blocked_display_fields"]
    assert "broker_action" in manual["blocked_display_fields"]
    assert monthly["monitoring_required"] is True
    assert monthly["inherited_warning"] == "MONTHLY_EVENT_CONCENTRATION_ABOVE_GUARDRAIL"
    assert "monthly_event_count" in monthly["runtime_metrics"]
    assert monthly["guardrail_action"]["blocking"] == (
        "pause_scheduler_integration_until_review"
    )
    assert monthly["broker_action"] == "none"
