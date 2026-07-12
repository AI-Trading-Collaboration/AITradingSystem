from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
from dynamic_v3_backtest_sim_helpers import run_outcome_fixture, run_regime_fixture

from ai_trading_system.etf_portfolio import dynamic_v3_backtest_simulation as sim


def test_backtest_sim_regime_review_uses_known_regime_buckets(
    tmp_path: Path, monkeypatch: Any
) -> None:
    fixture = run_regime_fixture(tmp_path, monkeypatch)
    regime = fixture["regime"]
    metrics = regime["variant_regime_metrics"]
    assert regime["manifest"]["status"] == "PASS"
    assert metrics
    assert {row["regime"] for row in metrics} <= sim.REGIME_BUCKETS
    assert regime["manifest"]["broker_action_taken"] is False
    assert (
        sim.validate_backtest_sim_regime_artifact(
            regime_review_id=regime["regime_review_id"], output_dir=fixture["regime_dir"]
        )["status"]
        == "PASS"
    )


def test_backtest_sim_regime_rejects_naive_time_before_output(
    tmp_path: Path, monkeypatch: Any
) -> None:
    fixture = run_outcome_fixture(tmp_path, monkeypatch)
    with pytest.raises(sim.DynamicV3BacktestSimulationError, match="timezone-aware"):
        sim.run_backtest_sim_regime_review(
            sim_outcome_id=fixture["outcome"]["sim_outcome_id"],
            outcome_dir=fixture["outcome_dir"],
            output_dir=fixture["regime_dir"],
            generated_at=datetime(2026, 7, 31, 4),
        )
    assert not fixture["regime_dir"].exists()


def test_backtest_sim_regime_rejects_invalid_outcome_before_output(
    tmp_path: Path, monkeypatch: Any
) -> None:
    fixture = run_outcome_fixture(tmp_path, monkeypatch)
    source = fixture["outcome"]["sim_outcome_dir"] / "backtest_sim_outcome_report.md"
    source.write_text(source.read_text(encoding="utf-8") + "\n", encoding="utf-8")
    with pytest.raises(sim.DynamicV3BacktestSimulationError, match="outcome validation"):
        sim.run_backtest_sim_regime_review(
            sim_outcome_id=fixture["outcome"]["sim_outcome_id"],
            outcome_dir=fixture["outcome_dir"],
            output_dir=fixture["regime_dir"],
            generated_at=datetime(2026, 7, 31, 4, tzinfo=UTC),
        )
    assert not fixture["regime_dir"].exists()


def test_backtest_sim_regime_missing_metrics_are_null() -> None:
    metrics = sim._regime_metrics(
        [{"variant": "limited_adjustment", "regime_label": "ai_trend", "outcome_status": "PENDING"}]
    )
    missing = next(row for row in metrics if row["regime"] == "ai_trend")
    for key in (
        "avg_return",
        "avg_relative_to_no_trade",
        "win_rate_vs_no_trade",
        "avg_drawdown",
        "avg_turnover",
    ):
        assert missing[key] is None
    assert missing["status"] == "INSUFFICIENT_DATA"


@pytest.mark.parametrize(
    "artifact_name",
    [
        "sim_regime_manifest.json",
        "regime_window_inventory.json",
        "variant_regime_metrics.jsonl",
        "regime_review_summary.json",
        "regime_input_snapshot.json",
        "backtest_sim_regime_report.md",
    ],
)
def test_backtest_sim_regime_validator_rejects_byte_tampering(
    tmp_path: Path, monkeypatch: Any, artifact_name: str
) -> None:
    fixture = run_regime_fixture(tmp_path, monkeypatch)
    path = fixture["regime"]["regime_review_dir"] / artifact_name
    path.write_text(path.read_text(encoding="utf-8") + "\n", encoding="utf-8")
    assert (
        sim.validate_backtest_sim_regime_artifact(
            regime_review_id=fixture["regime"]["regime_review_id"],
            output_dir=fixture["regime_dir"],
        )["status"]
        == "FAIL"
    )


def test_backtest_sim_regime_validator_rejects_live_outcome_tampering(
    tmp_path: Path, monkeypatch: Any
) -> None:
    fixture = run_regime_fixture(tmp_path, monkeypatch)
    source = fixture["outcome"]["sim_outcome_dir"] / "backtest_sim_outcome_report.md"
    source.write_text(source.read_text(encoding="utf-8") + "\ntampered\n", encoding="utf-8")
    assert (
        sim.validate_backtest_sim_regime_artifact(
            regime_review_id=fixture["regime"]["regime_review_id"],
            output_dir=fixture["regime_dir"],
        )["status"]
        == "FAIL"
    )
