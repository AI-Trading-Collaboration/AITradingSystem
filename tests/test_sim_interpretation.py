from __future__ import annotations

from pathlib import Path
from typing import Any

from dynamic_v3_backtest_sim_helpers import run_sim_interpretation_fixture

from ai_trading_system.etf_portfolio.dynamic_v3_backtest_simulation import (
    BACKTEST_SIM_VARIANTS,
    REPORT_LABEL_BACKTEST_SIMULATION,
    validate_sim_interpretation_artifact,
)


def test_sim_interpretation_explains_each_variant(
    tmp_path: Path, monkeypatch: Any
) -> None:
    fixture = run_sim_interpretation_fixture(tmp_path, monkeypatch)
    interpretation = fixture["interpretation"]
    matrix = interpretation["variant_interpretation_matrix"]
    findings = interpretation["key_findings"]["findings"]

    variants = {row["variant"]: row for row in matrix["variants"]}
    assert set(variants) == set(BACKTEST_SIM_VARIANTS)
    assert variants["limited_adjustment"]["role"] == "risk_aware_active_tilt"
    assert variants["consensus_target"]["recommended_usage"] == "upper_bound_reference_only"
    assert (
        variants["defensive_limited_adjustment"]["not_recommended_usage"]
        == "do_not_label_as_proven_defensive"
    )
    assert any(REPORT_LABEL_BACKTEST_SIMULATION in row["limitations"] for row in findings)
    assert interpretation["manifest"]["broker_action_allowed"] is False
    assert interpretation["manifest"]["production_effect"] == "none"

    validation = validate_sim_interpretation_artifact(
        interpretation_id=interpretation["interpretation_id"],
        output_dir=fixture["interpretation_dir"],
    )
    assert validation["status"] == "PASS"
