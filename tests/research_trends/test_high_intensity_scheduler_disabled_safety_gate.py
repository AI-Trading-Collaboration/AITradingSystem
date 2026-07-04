from __future__ import annotations

from pathlib import Path

from high_intensity_scheduler_dry_run_fixtures import (
    build_high_intensity_scheduler_dry_run_fixture,
)

from ai_trading_system.high_intensity_risk_cap_scheduler_dry_run import (
    build_high_intensity_scheduler_disabled_policy_validation_report,
    build_high_intensity_scheduler_fail_closed_safety_gate_result,
    load_high_intensity_scheduler_dry_run_inputs,
)


def _inputs(tmp_path: Path) -> dict[str, object]:
    fixture = build_high_intensity_scheduler_dry_run_fixture(tmp_path)
    return load_high_intensity_scheduler_dry_run_inputs(
        scheduler_integration_plan_dir=fixture["scheduler_integration_plan_dir"],
        runtime_dry_run_dir=fixture["runtime_dry_run_dir"],
        runtime_integration_plan_dir=fixture["runtime_integration_plan_dir"],
        continue_decision_dir=fixture["continue_decision_dir"],
        event_logger_dir=fixture["event_logger_dir"],
        threshold_selection_dir=fixture["threshold_selection_dir"],
        forward_observe_plan_dir=fixture["forward_observe_plan_dir"],
        dynamic_dry_run_dir=fixture["dynamic_dry_run_dir"],
    )


def _candidate(**overrides: object) -> dict[str, object]:
    candidate = {
        "selected_rule_id": "COMPOSITE_HIGH_INTENSITY_RULE",
        "as_of_timestamp": "2026-06-30T09:00:00+09:00",
        "pit_policy": "PIT_APPROXIMATION_READY",
        "scheduler_enabled": False,
        "scheduler_default_enabled": False,
        "broker_action": "none",
        "promotion_allowed": False,
        "paper_shadow_allowed": False,
        "production_allowed": False,
    }
    candidate.update(overrides)
    return candidate


def test_scheduler_disabled_policy_validation_passes(tmp_path: Path) -> None:
    report = build_high_intensity_scheduler_disabled_policy_validation_report(
        _inputs(tmp_path)
    )

    assert report["disabled_policy_status"] == "PASS"
    assert report["scheduler_default_enabled"] is False
    assert report["scheduler_enabled_in_2345"] is False


def test_scheduler_safety_gate_passes_disabled_scheduler() -> None:
    result = build_high_intensity_scheduler_fail_closed_safety_gate_result(_candidate())

    assert result["safety_gate_status"] == "PASS"
    assert result["scheduler_enabled_blocked"] is False


def test_scheduler_safety_gate_blocks_target_weight() -> None:
    result = build_high_intensity_scheduler_fail_closed_safety_gate_result(
        _candidate(target_weight_generated=True)
    )

    assert result["safety_gate_status"] == "FAIL_CLOSED_TRIGGERED"
    assert result["target_weight_generated_blocked"] is True


def test_scheduler_safety_gate_blocks_rebalance_instruction() -> None:
    result = build_high_intensity_scheduler_fail_closed_safety_gate_result(
        _candidate(rebalance_instruction_generated=True)
    )

    assert result["rebalance_instruction_generated_blocked"] is True


def test_scheduler_safety_gate_blocks_broker_action() -> None:
    result = build_high_intensity_scheduler_fail_closed_safety_gate_result(
        _candidate(broker_action="SELL")
    )

    assert result["broker_action_requested_blocked"] is True


def test_scheduler_safety_gate_blocks_paper_shadow_and_production() -> None:
    paper = build_high_intensity_scheduler_fail_closed_safety_gate_result(
        _candidate(paper_shadow_enabled=True)
    )
    production = build_high_intensity_scheduler_fail_closed_safety_gate_result(
        _candidate(production_enabled=True)
    )

    assert paper["paper_shadow_enabled_blocked"] is True
    assert production["production_enabled_blocked"] is True


def test_scheduler_safety_gate_blocks_scheduler_enabled() -> None:
    result = build_high_intensity_scheduler_fail_closed_safety_gate_result(
        _candidate(scheduler_enabled=True)
    )

    assert result["scheduler_enabled_blocked"] is True
