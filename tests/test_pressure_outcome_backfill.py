from __future__ import annotations

from pathlib import Path

from dynamic_v3_pressure_validation_helpers import run_pressure_backfill_fixture

from ai_trading_system.etf_portfolio.dynamic_v3_pressure_validation import (
    validate_pressure_outcome_backfill_artifact,
)


def test_pressure_outcome_backfill_marks_simulation_research_only(
    tmp_path: Path,
) -> None:
    fixture = run_pressure_backfill_fixture(tmp_path)
    result = fixture["pressure_backfill"]
    inventory = result["pressure_outcome_inventory"]
    summary = result["pressure_source_summary"]

    assert len(inventory) == 2
    assert summary["by_source_mode"] == {
        "FORWARD_OUTCOME": 0,
        "HISTORICAL_REPLAY": 0,
        "BACKTEST_SIMULATION": 2,
    }
    assert summary["defensive_validation_relevant_count"] == 2
    assert summary["can_support_production_count"] == 0
    assert all(row["source_mode"] == "BACKTEST_SIMULATION" for row in inventory)
    assert all(row["evidence_quality"] == "SIMULATION_NOT_PIT" for row in inventory)
    assert all(row["can_support_production"] is False for row in inventory)
    assert (
        validate_pressure_outcome_backfill_artifact(
            backfill_id=result["pressure_backfill_id"],
            output_dir=fixture["pressure_backfill_dir"],
        )["status"]
        == "PASS"
    )
