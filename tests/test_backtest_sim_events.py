from __future__ import annotations

from pathlib import Path
from typing import Any

from dynamic_v3_backtest_sim_helpers import run_event_fixture

from ai_trading_system.etf_portfolio.dynamic_v3_backtest_simulation import (
    validate_backtest_sim_events_artifact,
)


def test_backtest_sim_events_generate_non_pit_observations(
    tmp_path: Path, monkeypatch: Any
) -> None:
    fixture = run_event_fixture(tmp_path, monkeypatch)
    event = fixture["event"]

    assert event["manifest"]["status"] == "PASS"
    assert event["manifest"]["outcome_mode"] == "BACKTEST_SIMULATION"
    assert event["manifest"]["pit_safety_status"] == "SIMULATION_NOT_PIT"
    assert event["manifest"]["broker_action_taken"] is False
    assert len(event["events"]) == 5
    assert all(row["not_for_production"] is True for row in event["events"])

    validation = validate_backtest_sim_events_artifact(
        event_set_id=event["event_set_id"],
        output_dir=fixture["event_dir"],
    )
    assert validation["status"] == "PASS"
