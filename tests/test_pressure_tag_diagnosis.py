from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest
from dynamic_v3_pressure_validation_helpers import (
    write_backtest_sim_outcome_fixture,
    write_pressure_tag_fixture,
)

from ai_trading_system.etf_portfolio.dynamic_v3_pressure_validation import (
    DynamicV3PressureValidationError,
    run_pressure_tag_diagnosis,
    validate_pressure_tag_diagnosis_artifact,
)


def test_pressure_tag_diagnosis_reports_mapping_gap_and_simulation_inventory(
    tmp_path: Path,
) -> None:
    pressure = write_pressure_tag_fixture(tmp_path)
    simulation = write_backtest_sim_outcome_fixture(tmp_path)
    diagnosis_dir = tmp_path / "pressure_tag_diagnosis"

    result = run_pressure_tag_diagnosis(
        tag_id=pressure["pressure_tag_id"],
        output_dir=diagnosis_dir,
        pressure_tag_dir=pressure["pressure_tag_dir"],
        backfilled_outcome_dir=tmp_path / "backfilled_outcome",
        backtest_sim_outcome_dir=simulation["backtest_sim_outcome_dir"],
        generated_at=datetime(2026, 6, 30, tzinfo=UTC),
    )

    summary = result["diagnosis_summary"]
    mapping = result["outcome_mapping_diagnostics"]
    assert summary["primary_reason"] == ("forward_outcome_mapping_gap_and_simulation_not_scanned")
    assert summary["simulation_not_scanned"] is True
    assert summary["near_miss_window_count"] >= 1
    assert mapping["pressure_relevant_outcomes"] == 0
    assert mapping["backtest_simulation_pressure_outcomes_available"] > 0
    assert result["manifest"]["policy_change_allowed"] is False
    assert (
        validate_pressure_tag_diagnosis_artifact(
            diagnosis_id=result["diagnosis_id"],
            output_dir=diagnosis_dir,
        )["status"]
        == "PASS"
    )

    Path(result["manifest"]["config_path"]).write_text("schema_version: drift\n", encoding="utf-8")
    assert (
        validate_pressure_tag_diagnosis_artifact(
            diagnosis_id=result["diagnosis_id"],
            output_dir=diagnosis_dir,
        )["status"]
        == "FAIL"
    )


def test_pressure_tag_diagnosis_rejects_naive_cutoff_before_output(tmp_path: Path) -> None:
    pressure = write_pressure_tag_fixture(tmp_path)
    output_dir = tmp_path / "pressure_tag_diagnosis"

    with pytest.raises(DynamicV3PressureValidationError, match="timezone-aware"):
        run_pressure_tag_diagnosis(
            tag_id=pressure["pressure_tag_id"],
            output_dir=output_dir,
            pressure_tag_dir=pressure["pressure_tag_dir"],
            generated_at=datetime(2026, 6, 30),
        )

    assert not output_dir.exists()
