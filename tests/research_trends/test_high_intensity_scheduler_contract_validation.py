from __future__ import annotations

from pathlib import Path

from high_intensity_scheduler_dry_run_fixtures import (
    build_high_intensity_scheduler_dry_run_fixture,
)

from ai_trading_system.high_intensity_risk_cap_scheduler_dry_run import (
    build_high_intensity_scheduler_contract_validation_report,
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


def test_scheduler_contract_validation_accepts_2344_contracts(tmp_path: Path) -> None:
    report = build_high_intensity_scheduler_contract_validation_report(
        _inputs(tmp_path)
    )

    assert report["contract_validation_status"] == "PASS_WITH_WARNINGS"
    assert report["scheduler_scope_contract_valid"] is True
    assert report["scheduler_cadence_plan_valid"] is True
    assert report["event_detection_job_contract_valid"] is True
    assert report["event_append_job_contract_valid"] is True
    assert report["disabled_by_default_policy_valid"] is True
    assert report["broker_action"] == "none"


def test_scheduler_contract_validation_fails_missing_blocked_output(
    tmp_path: Path,
) -> None:
    inputs = _inputs(tmp_path)
    inputs["scheduler_plan"]["event_detection_contract"]["blocked_output"] = []

    report = build_high_intensity_scheduler_contract_validation_report(inputs)

    assert report["contract_validation_status"] == "FAIL"
    assert report["event_detection_job_contract_valid"] is False


def test_scheduler_contract_validation_fails_scheduler_enabled(
    tmp_path: Path,
) -> None:
    inputs = _inputs(tmp_path)
    inputs["scheduler_plan"]["scope_contract"]["scheduler_enabled"] = True

    report = build_high_intensity_scheduler_contract_validation_report(inputs)

    assert report["contract_validation_status"] == "FAIL"
    assert report["scheduler_scope_contract_valid"] is False
