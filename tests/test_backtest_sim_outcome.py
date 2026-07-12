from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from dynamic_v3_backtest_sim_helpers import (
    prepare_backtest_sim_environment,
    run_outcome_fixture,
    run_variant_fixture,
)

from ai_trading_system.etf_portfolio import dynamic_v3_backtest_simulation as sim


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
    assert (
        sim.validate_backtest_sim_outcome_artifact(
            sim_outcome_id=outcome["sim_outcome_id"],
            output_dir=fixture["outcome_dir"],
        )["status"]
        == "PASS"
    )


def test_backtest_sim_outcome_rejects_naive_generated_at_before_output(
    tmp_path: Path, monkeypatch: Any
) -> None:
    fixture = run_variant_fixture(tmp_path, monkeypatch)
    with pytest.raises(sim.DynamicV3BacktestSimulationError, match="timezone-aware"):
        sim.run_backtest_sim_outcome(
            variant_set_id=fixture["variants"]["variant_set_id"],
            variant_dir=fixture["variant_dir"],
            event_dir=fixture["event_dir"],
            output_dir=fixture["outcome_dir"],
            enforce_data_quality_gate=False,
            generated_at=datetime(2026, 7, 31, 2),
        )
    assert not fixture["outcome_dir"].exists()


def test_backtest_sim_outcome_rejects_invalid_variant_before_output(
    tmp_path: Path, monkeypatch: Any
) -> None:
    fixture = run_variant_fixture(tmp_path, monkeypatch)
    weights_path = fixture["variants"]["variant_set_dir"] / "simulated_variant_weights.jsonl"
    weights_path.write_text(weights_path.read_text(encoding="utf-8") + "\n", encoding="utf-8")
    with pytest.raises(sim.DynamicV3BacktestSimulationError, match="variant artifact"):
        sim.run_backtest_sim_outcome(
            variant_set_id=fixture["variants"]["variant_set_id"],
            variant_dir=fixture["variant_dir"],
            event_dir=fixture["event_dir"],
            output_dir=fixture["outcome_dir"],
            enforce_data_quality_gate=False,
            generated_at=datetime(2026, 7, 31, 2, tzinfo=UTC),
        )
    assert not fixture["outcome_dir"].exists()


def test_backtest_sim_outcome_quality_failure_leaves_no_partial_artifact(
    tmp_path: Path, monkeypatch: Any
) -> None:
    fixture = run_variant_fixture(tmp_path, monkeypatch)
    monkeypatch.setattr(
        sim,
        "_run_cached_quality_gate",
        lambda **kwargs: SimpleNamespace(passed=False, status="FAIL"),
    )
    with pytest.raises(sim.DynamicV3BacktestSimulationError, match="quality gate failed"):
        sim.run_backtest_sim_outcome(
            variant_set_id=fixture["variants"]["variant_set_id"],
            variant_dir=fixture["variant_dir"],
            event_dir=fixture["event_dir"],
            output_dir=fixture["outcome_dir"],
            generated_at=datetime(2026, 7, 31, 2, tzinfo=UTC),
        )
    assert not fixture["outcome_dir"].exists()


def test_backtest_sim_outcome_pending_metrics_are_null(tmp_path: Path, monkeypatch: Any) -> None:
    paths = prepare_backtest_sim_environment(tmp_path, monkeypatch)
    event = sim.generate_backtest_sim_events(
        config_path=paths["config_path"],
        output_dir=paths["event_dir"],
        enforce_data_quality_gate=False,
        generated_at=datetime(2026, 6, 30, 20, tzinfo=UTC),
    )
    variants = sim.generate_backtest_sim_variants(
        event_set_id=event["event_set_id"],
        event_dir=paths["event_dir"],
        output_dir=paths["variant_dir"],
        generated_at=datetime(2026, 6, 30, 21, tzinfo=UTC),
    )
    outcome = sim.run_backtest_sim_outcome(
        variant_set_id=variants["variant_set_id"],
        variant_dir=paths["variant_dir"],
        event_dir=paths["event_dir"],
        output_dir=paths["outcome_dir"],
        enforce_data_quality_gate=False,
        generated_at=datetime(2026, 6, 30, 22, tzinfo=UTC),
    )
    pending = [row for row in outcome["outcome_rows"] if row["outcome_status"] == "PENDING"]
    assert pending
    keys = (
        "return",
        "relative_to_no_trade",
        "relative_to_consensus_target",
        "relative_to_limited_adjustment",
        "max_drawdown",
        "realized_volatility",
    )
    assert all(row[key] is None for row in pending for key in keys)
    assert (
        sim.validate_backtest_sim_outcome_artifact(
            sim_outcome_id=outcome["sim_outcome_id"], output_dir=paths["outcome_dir"]
        )["status"]
        == "PASS"
    )


@pytest.mark.parametrize(
    "artifact_name",
    [
        "sim_outcome_manifest.json",
        "simulated_outcome_windows.jsonl",
        "simulated_variant_summary.json",
        "outcome_input_snapshot.json",
        "backtest_sim_outcome_report.md",
    ],
)
def test_backtest_sim_outcome_validator_rejects_byte_tampering(
    tmp_path: Path, monkeypatch: Any, artifact_name: str
) -> None:
    fixture = run_outcome_fixture(tmp_path, monkeypatch)
    path = fixture["outcome"]["sim_outcome_dir"] / artifact_name
    path.write_text(path.read_text(encoding="utf-8") + "\n", encoding="utf-8")
    assert (
        sim.validate_backtest_sim_outcome_artifact(
            sim_outcome_id=fixture["outcome"]["sim_outcome_id"],
            output_dir=fixture["outcome_dir"],
        )["status"]
        == "FAIL"
    )


def test_backtest_sim_outcome_validator_rejects_live_variant_tampering(
    tmp_path: Path, monkeypatch: Any
) -> None:
    fixture = run_outcome_fixture(tmp_path, monkeypatch)
    source_report = fixture["variants"]["variant_set_dir"] / "variant_generation_report.md"
    source_report.write_text(
        source_report.read_text(encoding="utf-8") + "\ntampered\n", encoding="utf-8"
    )
    assert (
        sim.validate_backtest_sim_outcome_artifact(
            sim_outcome_id=fixture["outcome"]["sim_outcome_id"],
            output_dir=fixture["outcome_dir"],
        )["status"]
        == "FAIL"
    )
