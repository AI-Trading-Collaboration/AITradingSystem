from __future__ import annotations

from pathlib import Path
from typing import Any

from dynamic_v3_backtest_sim_helpers import run_paper_fixture

from ai_trading_system.etf_portfolio.dynamic_v3_backtest_simulation import (
    validate_backtest_sim_paper_artifact,
)


def test_backtest_sim_paper_rebuilds_ledger_without_broker_actions(
    tmp_path: Path, monkeypatch: Any
) -> None:
    fixture = run_paper_fixture(tmp_path, monkeypatch)
    paper = fixture["paper"]

    assert paper["manifest"]["status"] == "PASS"
    assert paper["manifest"]["broker_action_taken"] is False
    assert paper["performance_summary"]["variant"] == "limited_adjustment"
    assert paper["state_history"]
    assert paper["trade_ledger"]
    assert all(row["broker_action_taken"] is False for row in paper["trade_ledger"])

    validation = validate_backtest_sim_paper_artifact(
        sim_paper_id=paper["sim_paper_id"],
        output_dir=fixture["paper_dir"],
    )
    assert validation["status"] == "PASS"
