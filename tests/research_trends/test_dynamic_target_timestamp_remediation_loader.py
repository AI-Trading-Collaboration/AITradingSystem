from __future__ import annotations

from pathlib import Path

import pytest
from dynamic_target_timestamp_remediation_fixtures import (
    build_dynamic_target_timestamp_remediation_fixture,
    read_json,
    write_json,
)

from ai_trading_system.dynamic_target_baseline_timestamp_remediation import (
    DynamicTargetBaselineTimestampRemediationError,
    load_dynamic_target_timestamp_remediation_inputs,
    run_dynamic_target_baseline_timestamp_remediation,
)


def test_loader_reads_2329_and_2328_outputs(tmp_path: Path) -> None:
    fixture = build_dynamic_target_timestamp_remediation_fixture(tmp_path)

    payload = load_dynamic_target_timestamp_remediation_inputs(
        source_remediation_dir=fixture["source_remediation_dir"],
        dynamic_preparation_dir=fixture["dynamic_preparation_dir"],
        diagnostics_dir=fixture["diagnostics_dir"],
        source_binding_dir=fixture["source_binding_dir"],
        simulation_policy_dir=fixture["simulation_policy_dir"],
    )

    assert sorted(payload) == [
        "diagnostics",
        "dynamic_preparation",
        "policy",
        "source_binding",
        "source_remediation",
    ]
    assert payload["source_remediation"]["task_route"]["next_task"] == (
        "TRADING-2330_Dynamic_Target_Baseline_Timestamp_Remediation"
    )
    assert payload["source_remediation"]["wrapper"]["rows"]


def test_loader_fails_closed_when_2329_route_not_timestamp_remediation(
    tmp_path: Path,
) -> None:
    fixture = build_dynamic_target_timestamp_remediation_fixture(tmp_path)
    route_path = fixture["source_remediation_dir"] / "dynamic_target_2330_task_route.json"
    route = read_json(route_path)
    route["next_task"] = "TRADING-2330_Continue_Static_Baseline_Only"
    write_json(route_path, route)

    with pytest.raises(DynamicTargetBaselineTimestampRemediationError):
        load_dynamic_target_timestamp_remediation_inputs(
            source_remediation_dir=fixture["source_remediation_dir"],
            dynamic_preparation_dir=fixture["dynamic_preparation_dir"],
            diagnostics_dir=fixture["diagnostics_dir"],
            source_binding_dir=fixture["source_binding_dir"],
            simulation_policy_dir=fixture["simulation_policy_dir"],
        )


def test_loader_fails_closed_on_wrapper_promotion_allowed(tmp_path: Path) -> None:
    fixture = build_dynamic_target_timestamp_remediation_fixture(tmp_path)
    wrapper_path = (
        fixture["source_remediation_dir"] / "dynamic_target_baseline_wrapper_artifact.json"
    )
    wrapper = read_json(wrapper_path)
    wrapper["rows"][0]["promotion_allowed"] = True
    write_json(wrapper_path, wrapper)

    with pytest.raises(DynamicTargetBaselineTimestampRemediationError):
        load_dynamic_target_timestamp_remediation_inputs(
            source_remediation_dir=fixture["source_remediation_dir"],
            dynamic_preparation_dir=fixture["dynamic_preparation_dir"],
            diagnostics_dir=fixture["diagnostics_dir"],
            source_binding_dir=fixture["source_binding_dir"],
            simulation_policy_dir=fixture["simulation_policy_dir"],
        )


def test_missing_wrapper_generates_blocked_report(tmp_path: Path) -> None:
    fixture = build_dynamic_target_timestamp_remediation_fixture(tmp_path)
    wrapper_path = (
        fixture["source_remediation_dir"] / "dynamic_target_baseline_wrapper_artifact.json"
    )
    wrapper_path.unlink()
    output_dir = tmp_path / "out"

    payload = run_dynamic_target_baseline_timestamp_remediation(
        source_remediation_dir=fixture["source_remediation_dir"],
        dynamic_preparation_dir=fixture["dynamic_preparation_dir"],
        diagnostics_dir=fixture["diagnostics_dir"],
        source_binding_dir=fixture["source_binding_dir"],
        simulation_policy_dir=fixture["simulation_policy_dir"],
        output_dir=output_dir,
        docs_root=tmp_path / "docs",
    )

    assert payload["status"] == "DYNAMIC_TARGET_BASELINE_TIMESTAMP_REMEDIATION_BLOCKED_NO_WRAPPER"
    blocked = read_json(output_dir / "dynamic_target_timestamp_remediation_blocked_report.json")
    assert blocked["timestamp_remediation_blocked"] is True
    assert payload["promotion_allowed"] is False
