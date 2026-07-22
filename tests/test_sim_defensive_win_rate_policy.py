from __future__ import annotations

from pathlib import Path

import pytest

from ai_trading_system.etf_portfolio import dynamic_v3_backtest_simulation as sim
from ai_trading_system.etf_portfolio.dynamic_v3_backtest_simulation import (
    DynamicV3BacktestSimulationError,
)


@pytest.mark.parametrize("value", ["-0.01", "1.01", ".nan", "invalid"])
def test_sim_defensive_validation_rejects_invalid_win_rate_policy(
    tmp_path: Path, value: str
) -> None:
    policy_path = tmp_path / "invalid_win_rate_policy.yaml"
    raw = sim.DEFAULT_SIM_DEFENSIVE_VALIDATION_POLICY_PATH.read_text(encoding="utf-8")
    policy_path.write_text(
        raw.replace(
            "minimum_win_rate_vs_no_trade: 0.50",
            f"minimum_win_rate_vs_no_trade: {value}",
        ),
        encoding="utf-8",
    )

    with pytest.raises(
        DynamicV3BacktestSimulationError,
        match="minimum_win_rate_vs_no_trade",
    ):
        sim._load_sim_defensive_validation_policy(policy_path)


def test_sim_defensive_validation_win_rate_boundary_controls_proven_status() -> None:
    policy = sim._load_sim_defensive_validation_policy(
        sim.DEFAULT_SIM_DEFENSIVE_VALIDATION_POLICY_PATH
    )
    common = {
        "regime": "tech_drawdown",
        "paired_event_count": policy["minimum_distinct_events_per_pressure_regime"],
        "avg_relative": 0.01,
        "avg_drawdown_delta": 0.01,
        "policy": policy,
    }

    assert (
        sim._defensive_regime_status(
            **common,
            win_rate_vs_no_trade=policy["minimum_win_rate_vs_no_trade"],
        )
        == "PROVEN_DEFENSIVE"
    )
    assert (
        sim._defensive_regime_status(**common, win_rate_vs_no_trade=0.499999)
        == "PARTIALLY_DEFENSIVE"
    )
