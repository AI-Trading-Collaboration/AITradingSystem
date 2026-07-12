from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
from dynamic_v3_backtest_sim_helpers import run_outcome_fixture, run_sensitivity_fixture

from ai_trading_system.etf_portfolio import dynamic_v3_backtest_simulation as sim


def test_backtest_sim_sensitivity_writes_adjustment_limit_diagnostics(
    tmp_path: Path, monkeypatch: Any
) -> None:
    fixture = run_sensitivity_fixture(tmp_path, monkeypatch)
    sensitivity = fixture["sensitivity"]
    warnings = sensitivity["overfit_warning_summary"]

    assert sensitivity["manifest"]["status"] in {
        "LOW_RISK",
        "REVIEW_REQUIRED",
        "HIGH_RISK",
        "INSUFFICIENT_DATA",
    }
    assert sensitivity["adjustment_limit_sensitivity"]["results"]
    assert (sensitivity["sensitivity_dir"] / "adjustment_limit_sensitivity.json").exists()
    assert warnings["strong_calibration_allowed"] is (
        warnings["simulation_overfit_status"] == "LOW_RISK"
    )
    assert sensitivity["input_snapshot"]["outcome_bundle"]
    assert sensitivity["input_snapshot"]["outcome_validation"]["status"] == "PASS"
    assert warnings["source_event_count"] > 0
    assert warnings["source_window_count"] >= warnings["available_window_count"]
    assert warnings["result_row_count"] == 9

    validation = sim.validate_backtest_sim_sensitivity_artifact(
        sensitivity_id=sensitivity["sensitivity_id"],
        output_dir=fixture["sensitivity_dir"],
    )
    assert validation["status"] == "PASS"


def test_backtest_sim_sensitivity_rejects_naive_time_before_output(
    tmp_path: Path, monkeypatch: Any
) -> None:
    fixture = run_outcome_fixture(tmp_path, monkeypatch)
    with pytest.raises(sim.DynamicV3BacktestSimulationError, match="timezone-aware"):
        sim.run_backtest_sim_sensitivity(
            sim_outcome_id=fixture["outcome"]["sim_outcome_id"],
            outcome_dir=fixture["outcome_dir"],
            variant_dir=fixture["variant_dir"],
            event_dir=fixture["event_dir"],
            output_dir=fixture["sensitivity_dir"],
            generated_at=datetime(2026, 7, 31, 5),
        )
    assert not fixture["sensitivity_dir"].exists()


def test_backtest_sim_sensitivity_rejects_invalid_outcome_before_output(
    tmp_path: Path, monkeypatch: Any
) -> None:
    fixture = run_outcome_fixture(tmp_path, monkeypatch)
    source = fixture["outcome"]["sim_outcome_dir"] / "backtest_sim_outcome_report.md"
    source.write_text(source.read_text(encoding="utf-8") + "\n", encoding="utf-8")
    with pytest.raises(sim.DynamicV3BacktestSimulationError, match="outcome validation failed"):
        sim.run_backtest_sim_sensitivity(
            sim_outcome_id=fixture["outcome"]["sim_outcome_id"],
            outcome_dir=fixture["outcome_dir"],
            variant_dir=fixture["variant_dir"],
            event_dir=fixture["event_dir"],
            output_dir=fixture["sensitivity_dir"],
            generated_at=datetime(2026, 7, 31, 5, tzinfo=UTC),
        )
    assert not fixture["sensitivity_dir"].exists()


def test_backtest_sim_sensitivity_excludes_missing_dispersion_without_zero_fill() -> None:
    result = sim._threshold_sensitivity(
        [
            {
                "sim_event_id": "event-1",
                "variant": "limited_adjustment",
                "window_days": 5,
                "outcome_status": "AVAILABLE",
                "relative_to_no_trade": 0.1,
            }
        ],
        [{"sim_event_id": "event-1"}],
        {"consensus_dispersion_thresholds": {"base": 0.08}},
    )["results"][0]
    assert result["input_window_count"] == 1
    assert result["dispersion_missing_window_count"] == 1
    assert result["window_count"] == 0
    assert result["available_window_count"] == 0
    assert result["avg_relative_to_no_trade_5d"] is None
    assert result["result_status"] == "INSUFFICIENT_DATA"


@pytest.mark.parametrize(
    "artifact_name",
    [
        "sim_sensitivity_manifest.json",
        "threshold_sensitivity.json",
        "shortlist_sensitivity.json",
        "adjustment_limit_sensitivity.json",
        "event_frequency_sensitivity.json",
        "overfit_warning_summary.json",
        "sensitivity_input_snapshot.json",
        "backtest_sim_sensitivity_report.md",
    ],
)
def test_backtest_sim_sensitivity_validator_rejects_view_tamper(
    tmp_path: Path, monkeypatch: Any, artifact_name: str
) -> None:
    fixture = run_sensitivity_fixture(tmp_path, monkeypatch)
    sensitivity = fixture["sensitivity"]
    path = sensitivity["sensitivity_dir"] / artifact_name
    if path.suffix == ".md":
        path.write_text(path.read_text(encoding="utf-8") + "\n", encoding="utf-8")
    else:
        payload = json.loads(path.read_text(encoding="utf-8"))
        payload["tampered"] = True
        path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
    validation = sim.validate_backtest_sim_sensitivity_artifact(
        sensitivity_id=sensitivity["sensitivity_id"],
        output_dir=fixture["sensitivity_dir"],
    )
    assert validation["status"] == "FAIL"


def test_backtest_sim_sensitivity_validator_rejects_live_outcome_tamper(
    tmp_path: Path, monkeypatch: Any
) -> None:
    fixture = run_sensitivity_fixture(tmp_path, monkeypatch)
    sensitivity = fixture["sensitivity"]
    source = fixture["outcome"]["sim_outcome_dir"] / "backtest_sim_outcome_report.md"
    source.write_text(source.read_text(encoding="utf-8") + "\n", encoding="utf-8")
    validation = sim.validate_backtest_sim_sensitivity_artifact(
        sensitivity_id=sensitivity["sensitivity_id"],
        output_dir=fixture["sensitivity_dir"],
    )
    assert validation["status"] == "FAIL"
