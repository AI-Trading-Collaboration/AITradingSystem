from __future__ import annotations

from pathlib import Path
from typing import Any

from dynamic_v3_backtest_sim_helpers import run_sensitivity_fixture

from ai_trading_system.etf_portfolio.dynamic_v3_backtest_simulation import (
    validate_backtest_sim_sensitivity_artifact,
)


def test_backtest_sim_sensitivity_writes_adjustment_limit_diagnostics(
    tmp_path: Path, monkeypatch: Any
) -> None:
    fixture = run_sensitivity_fixture(tmp_path, monkeypatch)
    sensitivity = fixture["sensitivity"]
    warnings = sensitivity["overfit_warning_summary"]

    assert sensitivity["manifest"]["status"] in {
        "LOW_RISK",
        "REVIEW_REQUIRED",
        "HIGH_RISK",
        "INSUFFICIENT_DATA",
    }
    assert sensitivity["adjustment_limit_sensitivity"]["results"]
    assert (sensitivity["sensitivity_dir"] / "adjustment_limit_sensitivity.json").exists()
    assert warnings["strong_calibration_allowed"] is (
        warnings["simulation_overfit_status"] != "HIGH_RISK"
    )

    validation = validate_backtest_sim_sensitivity_artifact(
        sensitivity_id=sensitivity["sensitivity_id"],
        output_dir=fixture["sensitivity_dir"],
    )
    assert validation["status"] == "PASS"
