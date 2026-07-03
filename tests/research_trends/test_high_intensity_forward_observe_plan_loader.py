from __future__ import annotations

from pathlib import Path

import pytest
from high_intensity_forward_observe_plan_fixtures import (
    build_high_intensity_forward_observe_plan_fixture,
    read_json,
    write_json,
)

from ai_trading_system.high_intensity_risk_cap_forward_observe_plan import (
    HighIntensityForwardObservePlanError,
    load_high_intensity_forward_observe_plan_inputs,
)


def test_high_intensity_loader_reads_required_inputs(tmp_path: Path) -> None:
    fixture = build_high_intensity_forward_observe_plan_fixture(tmp_path)

    payload = load_high_intensity_forward_observe_plan_inputs(
        dynamic_diagnostics_dir=fixture["dynamic_diagnostics_dir"],
        dynamic_dry_run_dir=fixture["dynamic_dry_run_dir"],
        readiness_dir=fixture["readiness_dir"],
        timestamp_remediation_dir=fixture["timestamp_remediation_dir"],
        simulation_policy_dir=fixture["simulation_policy_dir"],
    )

    assert payload["dynamic_diagnostics"]["summary"]["overall_recommendation"] == (
        "HIGH_INTENSITY_ONLY_FORWARD_OBSERVE"
    )
    assert payload["dynamic_dry_run"]["data_quality_report"]["data_quality_status"] == (
        "PASS_WITH_WARNINGS"
    )
    assert payload["timestamp_remediation"]["known_at"]["known_at_policy"] == (
        "NEXT_SESSION_DECISION_POLICY"
    )


def test_high_intensity_loader_fails_when_2333_route_wrong(
    tmp_path: Path,
) -> None:
    fixture = build_high_intensity_forward_observe_plan_fixture(tmp_path)
    summary_path = (
        fixture["dynamic_diagnostics_dir"]
        / "dynamic_exposure_cap_diagnostics_review_summary.json"
    )
    payload = read_json(summary_path)
    payload["overall_recommendation"] = "ARCHIVE_CURRENT_EXPOSURE_CAP_MECHANICS"
    write_json(summary_path, payload)

    with pytest.raises(HighIntensityForwardObservePlanError, match="overall"):
        load_high_intensity_forward_observe_plan_inputs(
            dynamic_diagnostics_dir=fixture["dynamic_diagnostics_dir"],
            dynamic_dry_run_dir=fixture["dynamic_dry_run_dir"],
            readiness_dir=fixture["readiness_dir"],
            timestamp_remediation_dir=fixture["timestamp_remediation_dir"],
            simulation_policy_dir=fixture["simulation_policy_dir"],
        )


def test_high_intensity_loader_fails_on_missing_alignment(
    tmp_path: Path,
) -> None:
    fixture = build_high_intensity_forward_observe_plan_fixture(tmp_path)
    (
        fixture["dynamic_dry_run_dir"]
        / "dynamic_target_risk_cap_trigger_alignment_matrix.json"
    ).unlink()

    with pytest.raises(HighIntensityForwardObservePlanError, match="required"):
        load_high_intensity_forward_observe_plan_inputs(
            dynamic_diagnostics_dir=fixture["dynamic_diagnostics_dir"],
            dynamic_dry_run_dir=fixture["dynamic_dry_run_dir"],
            readiness_dir=fixture["readiness_dir"],
            timestamp_remediation_dir=fixture["timestamp_remediation_dir"],
            simulation_policy_dir=fixture["simulation_policy_dir"],
        )


def test_high_intensity_loader_fails_on_promotion_flag(tmp_path: Path) -> None:
    fixture = build_high_intensity_forward_observe_plan_fixture(tmp_path)
    summary_path = (
        fixture["dynamic_diagnostics_dir"]
        / "dynamic_exposure_cap_diagnostics_review_summary.json"
    )
    payload = read_json(summary_path)
    payload["promotion_allowed"] = True
    write_json(summary_path, payload)

    with pytest.raises(HighIntensityForwardObservePlanError, match="promotion"):
        load_high_intensity_forward_observe_plan_inputs(
            dynamic_diagnostics_dir=fixture["dynamic_diagnostics_dir"],
            dynamic_dry_run_dir=fixture["dynamic_dry_run_dir"],
            readiness_dir=fixture["readiness_dir"],
            timestamp_remediation_dir=fixture["timestamp_remediation_dir"],
            simulation_policy_dir=fixture["simulation_policy_dir"],
        )


def test_high_intensity_loader_fails_on_broker_action(tmp_path: Path) -> None:
    fixture = build_high_intensity_forward_observe_plan_fixture(tmp_path)
    boundary_path = (
        fixture["dynamic_dry_run_dir"]
        / "dynamic_target_pit_caveat_interpretation_boundary.json"
    )
    payload = read_json(boundary_path)
    payload["broker_action"] = "manual_order"
    write_json(boundary_path, payload)

    with pytest.raises(HighIntensityForwardObservePlanError, match="broker_action"):
        load_high_intensity_forward_observe_plan_inputs(
            dynamic_diagnostics_dir=fixture["dynamic_diagnostics_dir"],
            dynamic_dry_run_dir=fixture["dynamic_dry_run_dir"],
            readiness_dir=fixture["readiness_dir"],
            timestamp_remediation_dir=fixture["timestamp_remediation_dir"],
            simulation_policy_dir=fixture["simulation_policy_dir"],
        )
