from __future__ import annotations

from pathlib import Path
from typing import Any

from dynamic_v3_backtest_sim_helpers import run_outcome_fixture

from ai_trading_system.etf_portfolio.dynamic_v3_backtest_simulation import (
    validate_backtest_sim_outcome_artifact,
)


def test_backtest_sim_outcome_marks_available_without_production_effect(
    tmp_path: Path, monkeypatch: Any
) -> None:
    fixture = run_outcome_fixture(tmp_path, monkeypatch)
    outcome = fixture["outcome"]
    rows = outcome["outcome_rows"]

    assert outcome["manifest"]["status"] == "AVAILABLE"
    assert outcome["manifest"]["available_count"] == len(rows)
    assert outcome["manifest"]["pending_count"] == 0
    assert outcome["manifest"]["data_quality_status"] == "SKIPPED_EXPLICIT_TEST_FIXTURE"
    assert all(row["outcome_mode"] == "BACKTEST_SIMULATION" for row in rows)
    assert all(row["broker_action_taken"] is False for row in rows)

    validation = validate_backtest_sim_outcome_artifact(
        sim_outcome_id=outcome["sim_outcome_id"],
        output_dir=fixture["outcome_dir"],
    )
    assert validation["status"] == "PASS"
