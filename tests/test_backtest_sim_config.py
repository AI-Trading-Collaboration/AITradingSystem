from __future__ import annotations

from pathlib import Path
from typing import Any

from dynamic_v3_backtest_sim_helpers import prepare_backtest_sim_environment

from ai_trading_system.etf_portfolio.dynamic_v3_backtest_simulation import (
    validate_backtest_simulation_config,
)


def test_backtest_sim_config_requires_simulation_safety(tmp_path: Path, monkeypatch: Any) -> None:
    paths = prepare_backtest_sim_environment(tmp_path, monkeypatch)
    payload = validate_backtest_simulation_config(config_path=paths["config_path"])

    assert payload["status"] == "PASS"
    assert payload["failed_check_count"] == 0
    assert {row["check_id"] for row in payload["checks"]} >= {
        "outcome_mode_backtest_simulation",
        "pit_safety_simulation_not_pit",
        "safety_no_broker",
        "safety_no_auto_policy",
    }
