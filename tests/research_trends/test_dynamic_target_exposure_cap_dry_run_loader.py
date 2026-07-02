from __future__ import annotations

from pathlib import Path

import pytest
from dynamic_target_exposure_cap_dry_run_fixtures import (
    build_dynamic_target_exposure_cap_dry_run_fixture,
    read_json,
    write_json,
)

from ai_trading_system.dynamic_target_exposure_cap_dry_run import (
    DynamicTargetExposureCapDryRunError,
    load_dynamic_target_exposure_cap_dry_run_inputs,
    load_trading_2331_dry_run_readiness_outputs,
)


def test_loader_reads_required_upstream_artifacts(tmp_path: Path) -> None:
    fixture = build_dynamic_target_exposure_cap_dry_run_fixture(tmp_path)

    payload = load_dynamic_target_exposure_cap_dry_run_inputs(
        dry_run_readiness_dir=fixture["dry_run_readiness_dir"],
        timestamp_remediation_dir=fixture["timestamp_remediation_dir"],
        source_remediation_dir=fixture["source_remediation_dir"],
        source_binding_dir=fixture["source_binding_dir"],
        simulation_policy_dir=fixture["simulation_policy_dir"],
        static_dry_run_dir=fixture["static_dry_run_dir"],
    )

    assert payload["dry_run_readiness"]["summary"]["2332_allowed"] is True
    assert payload["timestamp_remediation"]["summary"]["2331_allowed"] is True
    assert payload["source_binding"]["risk_cap_trigger_binding"]["source_path"]
    assert payload["static_dry_run"]["comparison"]["record_count"] > 0


def test_loader_fails_closed_when_2332_not_allowed(tmp_path: Path) -> None:
    fixture = build_dynamic_target_exposure_cap_dry_run_fixture(tmp_path)
    path = fixture["dry_run_readiness_dir"] / "dynamic_dry_run_readiness_summary.json"
    summary = read_json(path)
    summary["2332_allowed"] = False
    write_json(path, summary)

    with pytest.raises(DynamicTargetExposureCapDryRunError):
        load_trading_2331_dry_run_readiness_outputs(fixture["dry_run_readiness_dir"])


def test_loader_fails_closed_on_promotion_flag(tmp_path: Path) -> None:
    fixture = build_dynamic_target_exposure_cap_dry_run_fixture(tmp_path)
    path = fixture["dry_run_readiness_dir"] / "dynamic_dry_run_gate_checklist.json"
    payload = read_json(path)
    payload["promotion_allowed"] = True
    write_json(path, payload)

    with pytest.raises(DynamicTargetExposureCapDryRunError):
        load_trading_2331_dry_run_readiness_outputs(fixture["dry_run_readiness_dir"])


def test_loader_fails_closed_on_broker_action(tmp_path: Path) -> None:
    fixture = build_dynamic_target_exposure_cap_dry_run_fixture(tmp_path)
    path = fixture["dry_run_readiness_dir"] / "dynamic_dry_run_gate_checklist.json"
    payload = read_json(path)
    payload["broker_action"] = "manual_order"
    write_json(path, payload)

    with pytest.raises(DynamicTargetExposureCapDryRunError):
        load_trading_2331_dry_run_readiness_outputs(fixture["dry_run_readiness_dir"])
