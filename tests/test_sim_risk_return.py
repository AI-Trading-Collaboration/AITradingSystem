from __future__ import annotations

from pathlib import Path
from typing import Any

from dynamic_v3_backtest_sim_helpers import run_sim_risk_return_fixture

from ai_trading_system.etf_portfolio.dynamic_v3_backtest_simulation import (
    ACTIVE_SIM_VARIANTS,
    RISK_RETURN_STATUSES,
    validate_sim_risk_return_artifact,
)


def test_sim_risk_return_separates_return_and_drawdown(
    tmp_path: Path, monkeypatch: Any
) -> None:
    fixture = run_sim_risk_return_fixture(tmp_path, monkeypatch)
    risk_return = fixture["risk_return"]
    rows = risk_return["risk_adjusted_summary"]["summary"]

    assert {row["variant"] for row in rows} == set(ACTIVE_SIM_VARIANTS)
    assert all(row["risk_return_status"] in RISK_RETURN_STATUSES for row in rows)
    assert all("return_improvement_20d_pp" in row for row in rows)
    assert all("drawdown_worsening_20d_pp" in row for row in rows)
    assert (
        risk_return["risk_return_dir"] / "active_variant_tradeoff_table.csv"
    ).exists()
    assert risk_return["manifest"]["auto_policy_apply"] is False
    assert risk_return["manifest"]["production_effect"] == "none"

    validation = validate_sim_risk_return_artifact(
        risk_return_id=risk_return["risk_return_id"],
        output_dir=fixture["risk_return_dir"],
    )
    assert validation["status"] == "PASS"
