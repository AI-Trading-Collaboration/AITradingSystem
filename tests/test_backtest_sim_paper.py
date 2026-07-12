from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pandas as pd
import pytest
from dynamic_v3_backtest_sim_helpers import run_paper_fixture, run_variant_fixture

from ai_trading_system.etf_portfolio import dynamic_v3_backtest_simulation as sim


def test_backtest_sim_paper_rebuilds_ledger_without_broker_actions(
    tmp_path: Path, monkeypatch: Any
) -> None:
    fixture = run_paper_fixture(tmp_path, monkeypatch)
    paper = fixture["paper"]

    assert paper["manifest"]["status"] == "PASS"
    assert paper["manifest"]["broker_action_taken"] is False
    assert paper["manifest"]["data_quality_status"] == "SKIPPED_EXPLICIT_TEST_FIXTURE"
    assert paper["performance_summary"]["variant"] == "limited_adjustment"
    assert paper["performance_summary"]["return_basis"] == "GROSS_BEFORE_COSTS"
    assert paper["performance_summary"]["cost_model_status"] == "NOT_CONFIGURED"
    assert paper["state_history"]
    assert paper["trade_ledger"]
    assert all(row["broker_action_taken"] is False for row in paper["trade_ledger"])
    assert (
        sim.validate_backtest_sim_paper_artifact(
            sim_paper_id=paper["sim_paper_id"], output_dir=fixture["paper_dir"]
        )["status"]
        == "PASS"
    )


def test_backtest_sim_paper_rejects_naive_generated_at_before_output(
    tmp_path: Path, monkeypatch: Any
) -> None:
    fixture = run_variant_fixture(tmp_path, monkeypatch)
    with pytest.raises(sim.DynamicV3BacktestSimulationError, match="timezone-aware"):
        sim.run_backtest_sim_paper(
            variant_set_id=fixture["variants"]["variant_set_id"],
            variant_dir=fixture["variant_dir"],
            event_dir=fixture["event_dir"],
            output_dir=fixture["paper_dir"],
            enforce_data_quality_gate=False,
            generated_at=datetime(2026, 7, 31, 3),
        )
    assert not fixture["paper_dir"].exists()


def test_backtest_sim_paper_rejects_invalid_variant_source_before_output(
    tmp_path: Path, monkeypatch: Any
) -> None:
    fixture = run_variant_fixture(tmp_path, monkeypatch)
    source = fixture["variants"]["variant_set_dir"] / "variant_generation_report.md"
    source.write_text(source.read_text(encoding="utf-8") + "\n", encoding="utf-8")
    with pytest.raises(sim.DynamicV3BacktestSimulationError, match="variant artifact"):
        sim.run_backtest_sim_paper(
            variant_set_id=fixture["variants"]["variant_set_id"],
            variant_dir=fixture["variant_dir"],
            event_dir=fixture["event_dir"],
            output_dir=fixture["paper_dir"],
            enforce_data_quality_gate=False,
            generated_at=datetime(2026, 7, 31, 3, tzinfo=UTC),
        )
    assert not fixture["paper_dir"].exists()


def test_backtest_sim_paper_quality_failure_leaves_no_partial_artifact(
    tmp_path: Path, monkeypatch: Any
) -> None:
    fixture = run_variant_fixture(tmp_path, monkeypatch)
    monkeypatch.setattr(
        sim,
        "_run_cached_quality_gate",
        lambda **kwargs: SimpleNamespace(passed=False, status="FAIL"),
    )
    with pytest.raises(sim.DynamicV3BacktestSimulationError, match="quality gate failed"):
        sim.run_backtest_sim_paper(
            variant_set_id=fixture["variants"]["variant_set_id"],
            variant_dir=fixture["variant_dir"],
            event_dir=fixture["event_dir"],
            output_dir=fixture["paper_dir"],
            generated_at=datetime(2026, 7, 31, 3, tzinfo=UTC),
        )
    assert not fixture["paper_dir"].exists()


def test_backtest_sim_paper_insufficient_metrics_are_null() -> None:
    history, ledger, summary = sim._paper_history(
        [{"variant_status": "INSUFFICIENT_DATA"}],
        variant="limited_adjustment",
        prices=pd.DataFrame(),
    )
    assert history == []
    assert ledger == []
    assert summary["simulation_status"] == "INSUFFICIENT_DATA"
    assert all(
        summary[key] is None
        for key in (
            "total_return",
            "annualized_return",
            "max_drawdown",
            "realized_volatility",
            "turnover",
            "relative_to_no_trade",
            "relative_to_baseline",
        )
    )


@pytest.mark.parametrize(
    "artifact_name",
    [
        "sim_paper_manifest.json",
        "sim_paper_state_history.jsonl",
        "sim_trade_ledger.jsonl",
        "sim_paper_performance_summary.json",
        "paper_input_snapshot.json",
        "backtest_sim_paper_report.md",
    ],
)
def test_backtest_sim_paper_validator_rejects_byte_tampering(
    tmp_path: Path, monkeypatch: Any, artifact_name: str
) -> None:
    fixture = run_paper_fixture(tmp_path, monkeypatch)
    path = fixture["paper"]["sim_paper_dir"] / artifact_name
    path.write_text(path.read_text(encoding="utf-8") + "\n", encoding="utf-8")
    assert (
        sim.validate_backtest_sim_paper_artifact(
            sim_paper_id=fixture["paper"]["sim_paper_id"], output_dir=fixture["paper_dir"]
        )["status"]
        == "FAIL"
    )


def test_backtest_sim_paper_validator_rejects_live_variant_tampering(
    tmp_path: Path, monkeypatch: Any
) -> None:
    fixture = run_paper_fixture(tmp_path, monkeypatch)
    source = fixture["variants"]["variant_set_dir"] / "variant_generation_report.md"
    source.write_text(source.read_text(encoding="utf-8") + "\ntampered\n", encoding="utf-8")
    assert (
        sim.validate_backtest_sim_paper_artifact(
            sim_paper_id=fixture["paper"]["sim_paper_id"], output_dir=fixture["paper_dir"]
        )["status"]
        == "FAIL"
    )
