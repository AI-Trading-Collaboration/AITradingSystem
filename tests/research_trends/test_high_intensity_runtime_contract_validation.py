from __future__ import annotations

from pathlib import Path

from high_intensity_runtime_dry_run_fixtures import (
    build_high_intensity_runtime_dry_run_fixture,
)

from ai_trading_system.high_intensity_risk_cap_observe_only_runtime_dry_run import (
    build_high_intensity_runtime_contract_validation_report,
    load_high_intensity_runtime_dry_run_inputs,
)


def _loaded_fixture(tmp_path: Path) -> dict[str, object]:
    fixture = build_high_intensity_runtime_dry_run_fixture(tmp_path)
    return load_high_intensity_runtime_dry_run_inputs(
        runtime_integration_plan_dir=fixture["runtime_integration_plan_dir"],
        continue_decision_dir=fixture["continue_decision_dir"],
        event_logger_dir=fixture["event_logger_dir"],
        threshold_selection_dir=fixture["threshold_selection_dir"],
        forward_observe_plan_dir=fixture["forward_observe_plan_dir"],
        dynamic_dry_run_dir=fixture["dynamic_dry_run_dir"],
    )


def test_contract_validation_passes_with_expected_caveats(tmp_path: Path) -> None:
    inputs = _loaded_fixture(tmp_path)

    report = build_high_intensity_runtime_contract_validation_report(inputs)

    assert report["contract_validation_status"] == "PASS_WITH_WARNINGS"
    assert report["event_detection_contract_valid"] is True
    assert report["fail_closed_safety_gate_valid"] is True
    assert report["promotion_allowed"] is False
    assert report["broker_action"] == "none"


def test_contract_validation_fails_when_blocked_outputs_missing(
    tmp_path: Path,
) -> None:
    inputs = _loaded_fixture(tmp_path)
    inputs["runtime_plan"]["event_detection_contract"].pop("blocked_outputs")

    report = build_high_intensity_runtime_contract_validation_report(inputs)

    assert report["contract_validation_status"] == "FAIL"
    assert report["event_detection_contract_valid"] is False


def test_contract_validation_fails_when_safety_gate_open(tmp_path: Path) -> None:
    inputs = _loaded_fixture(tmp_path)
    inputs["runtime_plan"]["fail_closed_safety_gate"]["paper_shadow_allowed"] = True

    report = build_high_intensity_runtime_contract_validation_report(inputs)

    assert report["contract_validation_status"] == "FAIL"
    assert report["fail_closed_safety_gate_valid"] is False


def test_contract_validation_fails_when_broker_action_non_none(tmp_path: Path) -> None:
    inputs = _loaded_fixture(tmp_path)
    inputs["runtime_plan"]["event_append_contract"]["broker_action"] = "submit_order"

    report = build_high_intensity_runtime_contract_validation_report(inputs)

    assert report["contract_validation_status"] == "FAIL"
    assert report["event_append_contract_valid"] is False
