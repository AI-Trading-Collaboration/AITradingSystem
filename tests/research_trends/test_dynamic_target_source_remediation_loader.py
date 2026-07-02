from __future__ import annotations

from pathlib import Path

import pytest
from dynamic_target_source_remediation_fixtures import (
    build_dynamic_target_source_remediation_fixture,
    read_json,
    write_json,
)

from ai_trading_system.dynamic_target_baseline_source_remediation import (
    DynamicTargetBaselineSourceRemediationError,
    load_dynamic_target_source_remediation_inputs,
)


def test_loader_reads_2328_and_2327_outputs(tmp_path: Path) -> None:
    fixture = build_dynamic_target_source_remediation_fixture(tmp_path)

    payload = load_dynamic_target_source_remediation_inputs(
        dynamic_preparation_dir=fixture["dynamic_preparation_dir"],
        diagnostics_dir=fixture["diagnostics_dir"],
        static_dry_run_dir=fixture["static_dry_run_dir"],
        source_binding_dir=fixture["source_binding_dir"],
        simulation_policy_dir=fixture["simulation_policy_dir"],
    )

    assert sorted(payload) == [
        "diagnostics",
        "dynamic_preparation",
        "simulation_policy",
        "source_binding",
        "static_dry_run",
    ]
    assert payload["dynamic_preparation"]["summary"]["readiness_status"] == (
        "DYNAMIC_BASELINE_SOURCE_REMEDIATION_REQUIRED"
    )


def test_loader_fails_closed_when_2328_route_not_remediation(tmp_path: Path) -> None:
    fixture = build_dynamic_target_source_remediation_fixture(tmp_path)
    route_path = fixture["dynamic_preparation_dir"] / "dynamic_target_baseline_2329_task_route.json"
    route = read_json(route_path)
    route["next_task"] = "TRADING-2329_Continue_Static_Baseline_Only"
    write_json(route_path, route)

    with pytest.raises(DynamicTargetBaselineSourceRemediationError):
        load_dynamic_target_source_remediation_inputs(
            dynamic_preparation_dir=fixture["dynamic_preparation_dir"],
            diagnostics_dir=fixture["diagnostics_dir"],
            static_dry_run_dir=fixture["static_dry_run_dir"],
            source_binding_dir=fixture["source_binding_dir"],
            simulation_policy_dir=fixture["simulation_policy_dir"],
        )


def test_loader_fails_closed_on_promotion_allowed(tmp_path: Path) -> None:
    fixture = build_dynamic_target_source_remediation_fixture(tmp_path)
    summary_path = (
        fixture["dynamic_preparation_dir"]
        / "dynamic_target_baseline_preparation_summary.json"
    )
    summary = read_json(summary_path)
    summary["promotion_allowed"] = True
    write_json(summary_path, summary)

    with pytest.raises(DynamicTargetBaselineSourceRemediationError):
        load_dynamic_target_source_remediation_inputs(
            dynamic_preparation_dir=fixture["dynamic_preparation_dir"],
            diagnostics_dir=fixture["diagnostics_dir"],
            static_dry_run_dir=fixture["static_dry_run_dir"],
            source_binding_dir=fixture["source_binding_dir"],
            simulation_policy_dir=fixture["simulation_policy_dir"],
        )


def test_loader_fails_closed_on_broker_action(tmp_path: Path) -> None:
    fixture = build_dynamic_target_source_remediation_fixture(tmp_path)
    boundary_path = (
        fixture["dynamic_preparation_dir"]
        / "dynamic_target_baseline_safety_boundary.json"
    )
    boundary = read_json(boundary_path)
    boundary["broker_action"] = "manual_order"
    write_json(boundary_path, boundary)

    with pytest.raises(DynamicTargetBaselineSourceRemediationError):
        load_dynamic_target_source_remediation_inputs(
            dynamic_preparation_dir=fixture["dynamic_preparation_dir"],
            diagnostics_dir=fixture["diagnostics_dir"],
            static_dry_run_dir=fixture["static_dry_run_dir"],
            source_binding_dir=fixture["source_binding_dir"],
            simulation_policy_dir=fixture["simulation_policy_dir"],
        )
