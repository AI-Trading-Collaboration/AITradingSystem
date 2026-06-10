from __future__ import annotations

from pathlib import Path
from typing import Any

from dynamic_v3_backtest_sim_helpers import run_sim_defensive_validation_fixture

from ai_trading_system.etf_portfolio.dynamic_v3_backtest_simulation import (
    DEFENSIVE_PRESSURE_REGIMES,
    DEFENSIVE_VALIDATION_STATUSES,
    validate_sim_defensive_validation_artifact,
)


def test_sim_defensive_validation_does_not_auto_prove_defensive(
    tmp_path: Path, monkeypatch: Any
) -> None:
    fixture = run_sim_defensive_validation_fixture(tmp_path, monkeypatch)
    defensive = fixture["defensive_validation"]
    matrix = defensive["defensive_regime_matrix"]
    summary = defensive["defensive_validation_summary"]

    assert {row["regime"] for row in matrix} >= DEFENSIVE_PRESSURE_REGIMES
    assert summary["defensive_limited_adjustment_status"] in {
        "NOT_PROVEN_DEFENSIVE",
        "PARTIALLY_DEFENSIVE",
        "PROVEN_DEFENSIVE",
    }
    assert all(
        row["defensive_limited_adjustment"]["status"] in DEFENSIVE_VALIDATION_STATUSES
        for row in matrix
    )
    assert summary["requires_forward_confirmation"] is True
    assert defensive["manifest"]["broker_action_taken"] is False

    validation = validate_sim_defensive_validation_artifact(
        defensive_validation_id=defensive["defensive_validation_id"],
        output_dir=fixture["defensive_validation_dir"],
    )
    assert validation["status"] == "PASS"
