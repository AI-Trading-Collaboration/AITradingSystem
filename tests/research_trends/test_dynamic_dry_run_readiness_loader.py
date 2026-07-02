from __future__ import annotations

from pathlib import Path

import pytest
from dynamic_dry_run_readiness_fixtures import (
    build_dynamic_dry_run_readiness_fixture,
    read_json,
    write_json,
)

from ai_trading_system.dynamic_target_baseline_dry_run_readiness import (
    DynamicTargetBaselineDryRunReadinessError,
    load_dynamic_target_dry_run_readiness_inputs,
    load_trading_2330_timestamp_remediation_outputs,
)


def test_loader_reads_2330_and_upstream_context(tmp_path: Path) -> None:
    fixture = build_dynamic_dry_run_readiness_fixture(tmp_path)

    payload = load_dynamic_target_dry_run_readiness_inputs(
        timestamp_remediation_dir=fixture["timestamp_remediation_dir"],
        source_remediation_dir=fixture["source_remediation_dir"],
        dynamic_preparation_dir=fixture["dynamic_preparation_dir"],
        source_binding_dir=fixture["source_binding_dir"],
        simulation_policy_dir=fixture["simulation_policy_dir"],
        static_dry_run_dir=fixture["static_dry_run_dir"],
    )

    assert sorted(payload) == [
        "dynamic_preparation",
        "policy",
        "source_binding",
        "source_remediation",
        "static_dry_run",
        "timestamp_remediation",
    ]
    assert payload["timestamp_remediation"]["task_route"]["next_task"] == (
        "TRADING-2331_Dynamic_Target_Baseline_Dry_Run_Readiness_With_PIT_Caveat"
    )
    assert payload["timestamp_remediation"]["wrapper"]["rows"]


def test_loader_fails_closed_when_2330_route_not_2331(tmp_path: Path) -> None:
    fixture = build_dynamic_dry_run_readiness_fixture(tmp_path)
    route_path = fixture["timestamp_remediation_dir"] / "dynamic_target_2331_task_route.json"
    route = read_json(route_path)
    route["next_task"] = "TRADING-2331_Continue_Static_Baseline_Only"
    write_json(route_path, route)

    with pytest.raises(DynamicTargetBaselineDryRunReadinessError):
        load_trading_2330_timestamp_remediation_outputs(
            fixture["timestamp_remediation_dir"]
        )


def test_loader_fails_closed_when_2331_not_allowed(tmp_path: Path) -> None:
    fixture = build_dynamic_dry_run_readiness_fixture(tmp_path)
    summary_path = (
        fixture["timestamp_remediation_dir"]
        / "dynamic_target_timestamp_remediation_summary.json"
    )
    summary = read_json(summary_path)
    summary["2331_allowed"] = False
    write_json(summary_path, summary)

    with pytest.raises(DynamicTargetBaselineDryRunReadinessError):
        load_trading_2330_timestamp_remediation_outputs(
            fixture["timestamp_remediation_dir"]
        )


def test_loader_fails_closed_on_wrapper_safety_gate(tmp_path: Path) -> None:
    fixture = build_dynamic_dry_run_readiness_fixture(tmp_path)
    wrapper_path = (
        fixture["timestamp_remediation_dir"]
        / "dynamic_target_timestamp_remediated_wrapper_artifact.json"
    )
    wrapper = read_json(wrapper_path)
    wrapper["rows"][0]["promotion_allowed"] = True
    write_json(wrapper_path, wrapper)

    with pytest.raises(DynamicTargetBaselineDryRunReadinessError):
        load_trading_2330_timestamp_remediation_outputs(
            fixture["timestamp_remediation_dir"]
        )
