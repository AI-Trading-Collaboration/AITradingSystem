from __future__ import annotations

from pathlib import Path

import pytest
from high_intensity_threshold_selection_fixtures import (
    build_high_intensity_threshold_selection_fixture,
    read_json,
    write_json,
)

from ai_trading_system.high_intensity_risk_cap_threshold_selection import (
    HighIntensityThresholdSelectionError,
    load_high_intensity_threshold_selection_inputs,
)


def test_threshold_selection_loader_reads_required_inputs(tmp_path: Path) -> None:
    fixture = build_high_intensity_threshold_selection_fixture(tmp_path)

    payload = load_high_intensity_threshold_selection_inputs(
        forward_observe_plan_dir=fixture["forward_observe_plan_dir"],
        dynamic_diagnostics_dir=fixture["dynamic_diagnostics_dir"],
        dynamic_dry_run_dir=fixture["dynamic_dry_run_dir"],
        readiness_dir=fixture["readiness_dir"],
        timestamp_remediation_dir=fixture["timestamp_remediation_dir"],
    )

    assert payload["forward_observe_plan"]["summary"]["readiness_status"] == (
        "THRESHOLD_SELECTION_REQUIRED"
    )
    assert payload["forward_observe_plan"]["threshold_candidate_matrix"]["rows"]
    assert payload["dynamic_diagnostics"]["summary"]["overall_recommendation"] == (
        "HIGH_INTENSITY_ONLY_FORWARD_OBSERVE"
    )
    assert payload["dynamic_dry_run"]["data_quality_report"]["data_quality_status"] == (
        "PASS_WITH_WARNINGS"
    )


def test_threshold_selection_loader_fails_when_2334_route_wrong(
    tmp_path: Path,
) -> None:
    fixture = build_high_intensity_threshold_selection_fixture(tmp_path)
    route_path = fixture["forward_observe_plan_dir"] / "high_intensity_2335_task_route.json"
    payload = read_json(route_path)
    payload["next_task"] = "TRADING-2335_Archive_High_Intensity_Risk_Cap_Observe_Line"
    write_json(route_path, payload)

    with pytest.raises(HighIntensityThresholdSelectionError, match="task route"):
        load_high_intensity_threshold_selection_inputs(
            forward_observe_plan_dir=fixture["forward_observe_plan_dir"],
            dynamic_diagnostics_dir=fixture["dynamic_diagnostics_dir"],
            dynamic_dry_run_dir=fixture["dynamic_dry_run_dir"],
            readiness_dir=fixture["readiness_dir"],
            timestamp_remediation_dir=fixture["timestamp_remediation_dir"],
        )


def test_threshold_selection_loader_fails_on_promotion_flag(
    tmp_path: Path,
) -> None:
    fixture = build_high_intensity_threshold_selection_fixture(tmp_path)
    summary_path = (
        fixture["forward_observe_plan_dir"]
        / "high_intensity_forward_observe_plan_summary.json"
    )
    payload = read_json(summary_path)
    payload["promotion_allowed"] = True
    write_json(summary_path, payload)

    with pytest.raises(HighIntensityThresholdSelectionError, match="promotion"):
        load_high_intensity_threshold_selection_inputs(
            forward_observe_plan_dir=fixture["forward_observe_plan_dir"],
            dynamic_diagnostics_dir=fixture["dynamic_diagnostics_dir"],
            dynamic_dry_run_dir=fixture["dynamic_dry_run_dir"],
            readiness_dir=fixture["readiness_dir"],
            timestamp_remediation_dir=fixture["timestamp_remediation_dir"],
        )


def test_threshold_selection_loader_fails_on_broker_action(tmp_path: Path) -> None:
    fixture = build_high_intensity_threshold_selection_fixture(tmp_path)
    boundary_path = (
        fixture["dynamic_dry_run_dir"]
        / "dynamic_target_pit_caveat_interpretation_boundary.json"
    )
    payload = read_json(boundary_path)
    payload["broker_action"] = "manual_order"
    write_json(boundary_path, payload)

    with pytest.raises(HighIntensityThresholdSelectionError, match="broker_action"):
        load_high_intensity_threshold_selection_inputs(
            forward_observe_plan_dir=fixture["forward_observe_plan_dir"],
            dynamic_diagnostics_dir=fixture["dynamic_diagnostics_dir"],
            dynamic_dry_run_dir=fixture["dynamic_dry_run_dir"],
            readiness_dir=fixture["readiness_dir"],
            timestamp_remediation_dir=fixture["timestamp_remediation_dir"],
        )
