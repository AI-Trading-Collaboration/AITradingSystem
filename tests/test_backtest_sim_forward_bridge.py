from __future__ import annotations

from pathlib import Path
from typing import Any

from dynamic_v3_backtest_sim_helpers import run_forward_bridge_fixture

from ai_trading_system.etf_portfolio.dynamic_v3_backtest_simulation import (
    validate_backtest_sim_forward_bridge_artifact,
)


def test_backtest_sim_forward_bridge_requires_forward_confirmation(
    tmp_path: Path, monkeypatch: Any
) -> None:
    fixture = run_forward_bridge_fixture(tmp_path, monkeypatch)
    bridge = fixture["bridge"]
    targets = bridge["forward_confirmation_targets"]["targets"]

    assert bridge["manifest"]["status"] == "PASS"
    assert bridge["manifest"]["next_action"] == "continue_forward_tracking"
    assert targets
    assert targets[0]["required_forward_events"] == 2
    assert targets[0]["success_criteria"]["win_rate_vs_no_trade_min"] == 0.55
    assert bridge["manifest"]["broker_action_allowed"] is False
    assert bridge["manifest"]["production_effect"] == "none"
    assert (bridge["bridge_dir"] / "reader_brief_section.md").exists()

    validation = validate_backtest_sim_forward_bridge_artifact(
        bridge_id=bridge["bridge_id"],
        output_dir=fixture["bridge_dir"],
    )
    assert validation["status"] == "PASS"
