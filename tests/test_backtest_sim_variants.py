from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
from dynamic_v3_backtest_sim_helpers import run_variant_fixture

from ai_trading_system.etf_portfolio import dynamic_v3_backtest_simulation as sim
from ai_trading_system.etf_portfolio.dynamic_v3_backtest_simulation import (
    BACKTEST_SIM_VARIANT_SNAPSHOT_SCHEMA_VERSION,
    BACKTEST_SIM_VARIANTS,
    validate_backtest_sim_variants_artifact,
)


def test_backtest_sim_variants_apply_configured_adjustment_limits(
    tmp_path: Path, monkeypatch: Any
) -> None:
    fixture = run_variant_fixture(tmp_path, monkeypatch)
    variants = fixture["variants"]
    rows = variants["variant_rows"]

    assert set(variants["manifest"]["variants_generated"]) == set(BACKTEST_SIM_VARIANTS)
    assert variants["manifest"]["broker_action_taken"] is False
    assert (
        variants["input_snapshot"]["schema_version"] == BACKTEST_SIM_VARIANT_SNAPSHOT_SCHEMA_VERSION
    )
    assert {row["variant"] for row in rows} == set(BACKTEST_SIM_VARIANTS)
    limited = [row for row in rows if row["variant"] == "limited_adjustment"]
    assert limited
    assert max(row["turnover"] for row in limited) <= 0.10
    assert all(row["production_effect"] == "none" for row in rows)

    validation = validate_backtest_sim_variants_artifact(
        variant_set_id=variants["variant_set_id"],
        output_dir=fixture["variant_dir"],
    )
    assert validation["status"] == "PASS"


def test_backtest_sim_variants_reject_naive_generated_before_output(
    tmp_path: Path, monkeypatch: Any
) -> None:
    fixture = run_variant_fixture(tmp_path, monkeypatch)
    output_dir = tmp_path / "naive_variants"

    with pytest.raises(sim.DynamicV3BacktestSimulationError, match="timezone-aware"):
        sim.generate_backtest_sim_variants(
            event_set_id=fixture["event"]["event_set_id"],
            event_dir=fixture["event_dir"],
            output_dir=output_dir,
            generated_at=datetime(2026, 7, 31),
        )
    assert not output_dir.exists()


def test_backtest_sim_variants_reject_invalid_event_before_output(
    tmp_path: Path, monkeypatch: Any
) -> None:
    fixture = run_variant_fixture(tmp_path, monkeypatch)
    source_path = fixture["event"]["event_set_dir"] / "backtest_sim_event_manifest.json"
    payload = json.loads(source_path.read_text(encoding="utf-8"))
    payload["ready_count"] = 999
    source_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    output_dir = tmp_path / "invalid_source_variants"

    with pytest.raises(sim.DynamicV3BacktestSimulationError, match="validation failed"):
        sim.generate_backtest_sim_variants(
            event_set_id=fixture["event"]["event_set_id"],
            event_dir=fixture["event_dir"],
            output_dir=output_dir,
            generated_at=datetime(2026, 7, 31, 2, tzinfo=UTC),
        )
    assert not output_dir.exists()


@pytest.mark.parametrize(
    ("target", "mutate"),
    [
        ("variant_set_manifest.json", lambda payload: {**payload, "ready_count": 999}),
        (
            "variant_input_snapshot.json",
            lambda payload: {**payload, "enabled_variants": ["no_trade"]},
        ),
    ],
)
def test_backtest_sim_variant_validator_rejects_manifest_and_snapshot_tamper(
    tmp_path: Path, monkeypatch: Any, target: str, mutate: Any
) -> None:
    fixture = run_variant_fixture(tmp_path, monkeypatch)
    path = fixture["variants"]["variant_set_dir"] / target
    payload = json.loads(path.read_text(encoding="utf-8"))
    path.write_text(
        json.dumps(mutate(payload), ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    assert (
        validate_backtest_sim_variants_artifact(
            variant_set_id=fixture["variants"]["variant_set_id"],
            output_dir=fixture["variant_dir"],
        )["status"]
        == "FAIL"
    )


@pytest.mark.parametrize(
    "target",
    [
        "simulated_variant_weights.jsonl",
        "variant_action_ledger.jsonl",
        "variant_generation_report.md",
    ],
)
def test_backtest_sim_variant_validator_rejects_derived_view_tamper(
    tmp_path: Path, monkeypatch: Any, target: str
) -> None:
    fixture = run_variant_fixture(tmp_path, monkeypatch)
    path = fixture["variants"]["variant_set_dir"] / target
    path.write_text(path.read_text(encoding="utf-8") + "\n", encoding="utf-8")

    assert (
        validate_backtest_sim_variants_artifact(
            variant_set_id=fixture["variants"]["variant_set_id"],
            output_dir=fixture["variant_dir"],
        )["status"]
        == "FAIL"
    )


def test_backtest_sim_variant_validator_rejects_live_event_source_tamper(
    tmp_path: Path, monkeypatch: Any
) -> None:
    fixture = run_variant_fixture(tmp_path, monkeypatch)
    source_path = fixture["event"]["event_set_dir"] / "simulated_advisory_events.jsonl"
    source_path.write_text(source_path.read_text(encoding="utf-8") + "\n", encoding="utf-8")

    assert (
        validate_backtest_sim_variants_artifact(
            variant_set_id=fixture["variants"]["variant_set_id"],
            output_dir=fixture["variant_dir"],
        )["status"]
        == "FAIL"
    )
