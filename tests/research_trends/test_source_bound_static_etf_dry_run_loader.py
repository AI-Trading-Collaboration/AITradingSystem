from __future__ import annotations

import json
from pathlib import Path

import pytest
from source_bound_static_etf_dry_run_fixtures import (
    build_source_bound_static_etf_dry_run_fixture,
    read_json,
)

from ai_trading_system.source_bound_static_etf_dry_run import (
    SourceBoundStaticEtfDryRunError,
    load_baseline_decision_outputs,
    load_exposure_cap_policy,
    load_risk_cap_trigger_frame_from_source_binding,
    load_simulation_policy_outputs,
    load_source_binding_outputs,
    load_static_etf_config,
)


def test_source_bound_static_etf_loader_reads_required_inputs(tmp_path: Path) -> None:
    fixture = build_source_bound_static_etf_dry_run_fixture(tmp_path)

    baseline = load_baseline_decision_outputs(fixture["baseline_decision_dir"])
    source_binding = load_source_binding_outputs(fixture["source_binding_dir"])
    simulation_policy = load_simulation_policy_outputs(fixture["simulation_policy_dir"])
    policy = load_exposure_cap_policy(fixture["policy_path"])
    static_config = load_static_etf_config(
        fixture["portfolio_config_dir"],
        ["QQQ", "SPY", "SMH"],
    )
    trigger_frame = load_risk_cap_trigger_frame_from_source_binding(
        source_binding,
        ["QQQ", "SPY", "SMH"],
    )

    assert baseline["recommended_baseline"]["selected_for_2326"] == (
        "static_etf_allocation_baseline"
    )
    assert source_binding["risk_cap_trigger_binding"]["source_path"] == str(
        fixture["trigger_path"]
    )
    assert simulation_policy["summary"]["simulation_executed"] is False
    assert policy["cap_policy"]["max_allowed_exposure_by_intensity"]["high"] == 0.50
    assert static_config["total_weight"] == 1.0
    assert trigger_frame.attrs["source_path"] == str(fixture["trigger_path"])
    assert set(trigger_frame["target_asset"]) == {"QQQ", "SPY", "SMH"}


def test_source_bound_loader_fails_closed_when_2325_not_static(tmp_path: Path) -> None:
    fixture = build_source_bound_static_etf_dry_run_fixture(tmp_path)
    recommendation_path = (
        fixture["baseline_decision_dir"] / "recommended_exposure_cap_simulation_baseline.json"
    )
    payload = read_json(recommendation_path)
    payload["selected_for_2326"] = "synthetic_observe_only_baseline"
    recommendation_path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(SourceBoundStaticEtfDryRunError, match="requires"):
        load_baseline_decision_outputs(fixture["baseline_decision_dir"])


def test_source_bound_loader_fails_closed_on_unsafe_input_gate(tmp_path: Path) -> None:
    fixture = build_source_bound_static_etf_dry_run_fixture(tmp_path)
    summary_path = fixture["source_binding_dir"] / "exposure_cap_source_binding_summary.json"
    payload = read_json(summary_path)
    payload["promotion_allowed"] = True
    summary_path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(SourceBoundStaticEtfDryRunError, match="promotion"):
        load_source_binding_outputs(fixture["source_binding_dir"])
