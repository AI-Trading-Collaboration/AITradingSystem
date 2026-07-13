from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pytest
from dynamic_v3_pressure_validation_helpers import (
    GENERATED_AT,
    run_pressure_backfill_fixture,
    write_backtest_sim_outcome_fixture,
    write_pressure_tag_fixture,
)

from ai_trading_system.etf_portfolio.dynamic_v3_pressure_validation import (
    DynamicV3PressureValidationError,
    run_pressure_outcome_backfill,
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
    assert summary["distinct_event_count_by_source_mode"] == {
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


def test_pressure_outcome_backfill_excludes_pending_simulation_rows(tmp_path: Path) -> None:
    pressure = write_pressure_tag_fixture(tmp_path)
    simulation = write_backtest_sim_outcome_fixture(tmp_path)
    rows_path = simulation["sim_outcome_artifact_dir"] / "simulated_outcome_windows.jsonl"
    rows = [json.loads(line) for line in rows_path.read_text(encoding="utf-8").splitlines()]
    for row in rows:
        row["outcome_status"] = "PENDING"
    rows_path.write_text(
        "".join(json.dumps(row, sort_keys=True) + "\n" for row in rows),
        encoding="utf-8",
    )

    result = run_pressure_outcome_backfill(
        start=date(2026, 6, 1),
        end=date(2026, 6, 30),
        output_dir=tmp_path / "backfill",
        pressure_tag_dir=pressure["pressure_tag_dir"],
        advisory_outcome_dir=tmp_path / "advisory_outcome",
        backfilled_outcome_dir=tmp_path / "backfilled_outcome",
        backtest_sim_outcome_dir=simulation["backtest_sim_outcome_dir"],
        generated_at=GENERATED_AT,
    )

    assert result["pressure_outcome_inventory"] == []
    assert result["pressure_source_summary"]["defensive_validation_relevant_count"] == 0


def test_pressure_outcome_backfill_rejects_duplicate_source_before_output(
    tmp_path: Path,
) -> None:
    pressure = write_pressure_tag_fixture(tmp_path)
    simulation = write_backtest_sim_outcome_fixture(tmp_path)
    rows_path = simulation["sim_outcome_artifact_dir"] / "simulated_outcome_windows.jsonl"
    first = rows_path.read_text(encoding="utf-8").splitlines()[0]
    rows_path.write_text(rows_path.read_text(encoding="utf-8") + first + "\n", encoding="utf-8")
    output_dir = tmp_path / "backfill"

    with pytest.raises(DynamicV3PressureValidationError, match="duplicate simulation"):
        run_pressure_outcome_backfill(
            start=date(2026, 6, 1),
            end=date(2026, 6, 30),
            output_dir=output_dir,
            pressure_tag_dir=pressure["pressure_tag_dir"],
            advisory_outcome_dir=tmp_path / "advisory_outcome",
            backfilled_outcome_dir=tmp_path / "backfilled_outcome",
            backtest_sim_outcome_dir=simulation["backtest_sim_outcome_dir"],
            generated_at=GENERATED_AT,
        )

    assert not output_dir.exists()
