from __future__ import annotations

from pathlib import Path

import pytest
from dynamic_target_baseline_preparation_fixtures import (
    build_dynamic_target_baseline_preparation_fixture,
    read_json,
    write_json,
)

from ai_trading_system.dynamic_target_baseline_preparation import (
    DynamicTargetBaselinePreparationError,
    load_dynamic_target_baseline_preparation_inputs,
)


def test_loader_reads_required_upstream_outputs(tmp_path: Path) -> None:
    fixture = build_dynamic_target_baseline_preparation_fixture(tmp_path)

    payload = load_dynamic_target_baseline_preparation_inputs(
        diagnostics_dir=fixture["diagnostics_dir"],
        static_dry_run_dir=fixture["static_dry_run_dir"],
        baseline_decision_dir=fixture["baseline_decision_dir"],
        source_binding_dir=fixture["source_binding_dir"],
        simulation_policy_dir=fixture["simulation_policy_dir"],
    )

    assert sorted(payload) == [
        "baseline_decision",
        "diagnostics",
        "simulation_policy",
        "source_binding",
        "static_dry_run",
    ]
    assert payload["diagnostics"]["summary"]["overall_recommendation"] == (
        "MOVE_TO_DYNAMIC_TARGET_BASELINE_PREPARATION"
    )


def test_loader_fails_closed_when_2327_route_is_not_dynamic_baseline(
    tmp_path: Path,
) -> None:
    fixture = build_dynamic_target_baseline_preparation_fixture(tmp_path)
    route_path = fixture["diagnostics_dir"] / "exposure_cap_2328_task_route.json"
    payload = read_json(route_path)
    payload["next_task"] = "TRADING-9999_Stop"
    write_json(route_path, payload)

    with pytest.raises(DynamicTargetBaselinePreparationError):
        load_dynamic_target_baseline_preparation_inputs(
            diagnostics_dir=fixture["diagnostics_dir"],
            static_dry_run_dir=fixture["static_dry_run_dir"],
            baseline_decision_dir=fixture["baseline_decision_dir"],
            source_binding_dir=fixture["source_binding_dir"],
            simulation_policy_dir=fixture["simulation_policy_dir"],
        )


def test_loader_fails_closed_on_open_safety_gate(tmp_path: Path) -> None:
    fixture = build_dynamic_target_baseline_preparation_fixture(tmp_path)
    summary_path = fixture["diagnostics_dir"] / "exposure_cap_diagnostics_review_summary.json"
    payload = read_json(summary_path)
    payload["promotion_allowed"] = True
    write_json(summary_path, payload)

    with pytest.raises(DynamicTargetBaselinePreparationError):
        load_dynamic_target_baseline_preparation_inputs(
            diagnostics_dir=fixture["diagnostics_dir"],
            static_dry_run_dir=fixture["static_dry_run_dir"],
            baseline_decision_dir=fixture["baseline_decision_dir"],
            source_binding_dir=fixture["source_binding_dir"],
            simulation_policy_dir=fixture["simulation_policy_dir"],
        )


def test_loader_fails_closed_on_broker_action(tmp_path: Path) -> None:
    fixture = build_dynamic_target_baseline_preparation_fixture(tmp_path)
    boundary_path = fixture["diagnostics_dir"] / "diagnostics_interpretation_boundary.json"
    payload = read_json(boundary_path)
    payload["broker_action"] = "manual_order"
    write_json(boundary_path, payload)

    with pytest.raises(DynamicTargetBaselinePreparationError):
        load_dynamic_target_baseline_preparation_inputs(
            diagnostics_dir=fixture["diagnostics_dir"],
            static_dry_run_dir=fixture["static_dry_run_dir"],
            baseline_decision_dir=fixture["baseline_decision_dir"],
            source_binding_dir=fixture["source_binding_dir"],
            simulation_policy_dir=fixture["simulation_policy_dir"],
        )
