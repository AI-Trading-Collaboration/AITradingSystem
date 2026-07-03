from __future__ import annotations

from pathlib import Path

import pytest
from dynamic_exposure_cap_diagnostics_review_fixtures import (
    build_dynamic_exposure_cap_diagnostics_review_fixture,
    read_json,
    write_json,
)

from ai_trading_system.dynamic_exposure_cap_diagnostics_review import (
    DynamicExposureCapDiagnosticsReviewError,
    load_dynamic_exposure_cap_diagnostics_review_inputs,
)


def test_dynamic_diagnostics_loader_reads_required_inputs(tmp_path: Path) -> None:
    fixture = build_dynamic_exposure_cap_diagnostics_review_fixture(tmp_path)

    payload = load_dynamic_exposure_cap_diagnostics_review_inputs(
        dynamic_dry_run_dir=fixture["dynamic_dry_run_dir"],
        static_diagnostics_dir=fixture["static_diagnostics_dir"],
        static_dry_run_dir=fixture["static_dry_run_dir"],
        readiness_dir=fixture["dry_run_readiness_dir"],
        timestamp_remediation_dir=fixture["timestamp_remediation_dir"],
        simulation_policy_dir=fixture["simulation_policy_dir"],
    )

    assert payload["dynamic_dry_run"]["summary"]["next_task"] == (
        "TRADING-2333_Dynamic_Exposure_Cap_vs_No_Cap_Diagnostics_Review"
    )
    assert payload["static_diagnostics"]["summary"]["task_id"] == (
        "TRADING-2327_EXPOSURE_CAP_VS_NO_CAP_DIAGNOSTICS_REVIEW"
    )
    assert payload["timestamp_remediation"]["known_at"]["known_at_policy"] == (
        "NEXT_SESSION_DECISION_POLICY"
    )


def test_dynamic_diagnostics_loader_fails_when_comparison_missing(
    tmp_path: Path,
) -> None:
    fixture = build_dynamic_exposure_cap_diagnostics_review_fixture(tmp_path)
    (fixture["dynamic_dry_run_dir"] / "dynamic_target_cap_vs_no_cap_comparison.json").unlink()

    with pytest.raises(DynamicExposureCapDiagnosticsReviewError, match="required"):
        load_dynamic_exposure_cap_diagnostics_review_inputs(
            dynamic_dry_run_dir=fixture["dynamic_dry_run_dir"],
            static_diagnostics_dir=fixture["static_diagnostics_dir"],
            static_dry_run_dir=fixture["static_dry_run_dir"],
            readiness_dir=fixture["dry_run_readiness_dir"],
            timestamp_remediation_dir=fixture["timestamp_remediation_dir"],
            simulation_policy_dir=fixture["simulation_policy_dir"],
        )


def test_dynamic_diagnostics_loader_fails_when_boundary_missing(
    tmp_path: Path,
) -> None:
    fixture = build_dynamic_exposure_cap_diagnostics_review_fixture(tmp_path)
    boundary = fixture["dynamic_dry_run_dir"] / (
        "dynamic_target_pit_caveat_interpretation_boundary.json"
    )
    boundary.unlink()

    with pytest.raises(DynamicExposureCapDiagnosticsReviewError, match="required"):
        load_dynamic_exposure_cap_diagnostics_review_inputs(
            dynamic_dry_run_dir=fixture["dynamic_dry_run_dir"],
            static_diagnostics_dir=fixture["static_diagnostics_dir"],
            static_dry_run_dir=fixture["static_dry_run_dir"],
            readiness_dir=fixture["dry_run_readiness_dir"],
            timestamp_remediation_dir=fixture["timestamp_remediation_dir"],
            simulation_policy_dir=fixture["simulation_policy_dir"],
        )


def test_dynamic_diagnostics_loader_fails_on_promotion_flag(tmp_path: Path) -> None:
    fixture = build_dynamic_exposure_cap_diagnostics_review_fixture(tmp_path)
    summary_path = (
        fixture["dynamic_dry_run_dir"] / "dynamic_target_exposure_cap_dry_run_summary.json"
    )
    payload = read_json(summary_path)
    payload["promotion_allowed"] = True
    write_json(summary_path, payload)

    with pytest.raises(DynamicExposureCapDiagnosticsReviewError, match="promotion"):
        load_dynamic_exposure_cap_diagnostics_review_inputs(
            dynamic_dry_run_dir=fixture["dynamic_dry_run_dir"],
            static_diagnostics_dir=fixture["static_diagnostics_dir"],
            static_dry_run_dir=fixture["static_dry_run_dir"],
            readiness_dir=fixture["dry_run_readiness_dir"],
            timestamp_remediation_dir=fixture["timestamp_remediation_dir"],
            simulation_policy_dir=fixture["simulation_policy_dir"],
        )


def test_dynamic_diagnostics_loader_fails_on_broker_action(tmp_path: Path) -> None:
    fixture = build_dynamic_exposure_cap_diagnostics_review_fixture(tmp_path)
    boundary_path = fixture["dynamic_dry_run_dir"] / (
        "dynamic_target_pit_caveat_interpretation_boundary.json"
    )
    payload = read_json(boundary_path)
    payload["broker_action"] = "manual_order"
    write_json(boundary_path, payload)

    with pytest.raises(DynamicExposureCapDiagnosticsReviewError, match="broker_action"):
        load_dynamic_exposure_cap_diagnostics_review_inputs(
            dynamic_dry_run_dir=fixture["dynamic_dry_run_dir"],
            static_diagnostics_dir=fixture["static_diagnostics_dir"],
            static_dry_run_dir=fixture["static_dry_run_dir"],
            readiness_dir=fixture["dry_run_readiness_dir"],
            timestamp_remediation_dir=fixture["timestamp_remediation_dir"],
            simulation_policy_dir=fixture["simulation_policy_dir"],
        )
