from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
import yaml
from dynamic_v3_backtest_sim_helpers import prepare_backtest_sim_environment, run_event_fixture

from ai_trading_system.etf_portfolio import dynamic_v3_backtest_simulation as sim
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


def test_backtest_sim_config_rejects_empty_paths_and_duplicate_policy_values(
    tmp_path: Path, monkeypatch: Any
) -> None:
    paths = prepare_backtest_sim_environment(tmp_path, monkeypatch)
    config = yaml.safe_load(paths["config_path"].read_text(encoding="utf-8"))
    config["source"]["price_cache_path"] = ""
    config["variants"]["enabled"].append("no_trade")
    paths["config_path"].write_text(yaml.safe_dump(config, sort_keys=False), encoding="utf-8")

    validation = sim.validate_backtest_simulation_config(config_path=paths["config_path"])
    failed = {row["check_id"] for row in validation["checks"] if not row["passed"]}
    assert validation["status"] == "FAIL"
    assert {"price_cache_exists", "variants_supported"} <= failed


def test_backtest_sim_events_reject_future_range_before_output(
    tmp_path: Path, monkeypatch: Any
) -> None:
    paths = prepare_backtest_sim_environment(tmp_path, monkeypatch)

    with pytest.raises(sim.DynamicV3BacktestSimulationError, match="end exceeds generated"):
        sim.generate_backtest_sim_events(
            config_path=paths["config_path"],
            output_dir=paths["event_dir"],
            enforce_data_quality_gate=False,
            generated_at=datetime(2026, 6, 15, tzinfo=UTC),
        )
    assert not paths["event_dir"].exists()


def test_backtest_sim_events_data_quality_failure_leaves_no_partial_artifact(
    tmp_path: Path, monkeypatch: Any
) -> None:
    paths = prepare_backtest_sim_environment(tmp_path, monkeypatch)
    monkeypatch.setattr(
        sim,
        "_run_cached_quality_gate",
        lambda **_: SimpleNamespace(status="FAIL", passed=False),
    )

    with pytest.raises(sim.DynamicV3BacktestSimulationError, match="quality gate failed"):
        sim.generate_backtest_sim_events(
            config_path=paths["config_path"],
            output_dir=paths["event_dir"],
            enforce_data_quality_gate=True,
            generated_at=datetime(2026, 7, 31, tzinfo=UTC),
        )
    assert not paths["event_dir"].exists()


def test_backtest_sim_events_allow_explicit_empty_schedule(
    tmp_path: Path, monkeypatch: Any
) -> None:
    paths = prepare_backtest_sim_environment(tmp_path, monkeypatch)
    config = yaml.safe_load(paths["config_path"].read_text(encoding="utf-8"))
    config["date_range"]["start"] = "2026-06-06"
    config["date_range"]["end"] = "2026-06-07"
    paths["config_path"].write_text(yaml.safe_dump(config, sort_keys=False), encoding="utf-8")

    event = sim.generate_backtest_sim_events(
        config_path=paths["config_path"],
        output_dir=paths["event_dir"],
        enforce_data_quality_gate=False,
        generated_at=datetime(2026, 6, 7, tzinfo=UTC),
    )
    assert event["events"] == []
    assert event["manifest"]["status"] == "INSUFFICIENT_DATA"
    assert (
        validate_backtest_sim_events_artifact(
            event_set_id=event["event_set_id"], output_dir=paths["event_dir"]
        )["status"]
        == "PASS"
    )


@pytest.mark.parametrize(
    ("target", "mutate"),
    [
        (
            "backtest_sim_event_manifest.json",
            lambda payload: {**payload, "ready_count": 999},
        ),
        (
            "simulation_input_snapshot.json",
            lambda payload: {**payload, "requested_end": "2020-01-01"},
        ),
    ],
)
def test_backtest_sim_event_validator_rejects_output_and_snapshot_tamper(
    tmp_path: Path, monkeypatch: Any, target: str, mutate: Any
) -> None:
    fixture = run_event_fixture(tmp_path, monkeypatch)
    path = fixture["event"]["event_set_dir"] / target
    payload = json.loads(path.read_text(encoding="utf-8"))
    path.write_text(
        json.dumps(mutate(payload), ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    assert (
        validate_backtest_sim_events_artifact(
            event_set_id=fixture["event"]["event_set_id"], output_dir=fixture["event_dir"]
        )["status"]
        == "FAIL"
    )


def test_backtest_sim_event_validator_rejects_live_candidate_source_tamper(
    tmp_path: Path, monkeypatch: Any
) -> None:
    fixture = run_event_fixture(tmp_path, monkeypatch)
    snapshot = fixture["event"]["input_snapshot"]
    weights_path = Path(snapshot["candidate_sources"][0]["daily_weights_path"])
    weights_path.write_text(weights_path.read_text(encoding="utf-8") + "\n", encoding="utf-8")

    assert (
        validate_backtest_sim_events_artifact(
            event_set_id=fixture["event"]["event_set_id"], output_dir=fixture["event_dir"]
        )["status"]
        == "FAIL"
    )


@pytest.mark.parametrize("source_key", ["shortlist_source", "position_advisory_source"])
def test_backtest_sim_event_validator_rejects_governed_source_tamper(
    tmp_path: Path, monkeypatch: Any, source_key: str
) -> None:
    fixture = run_event_fixture(tmp_path, monkeypatch)
    source_path = Path(fixture["event"]["input_snapshot"][source_key]["path"])
    source_path.write_text(
        source_path.read_text(encoding="utf-8") + "\n# changed\n", encoding="utf-8"
    )

    assert (
        validate_backtest_sim_events_artifact(
            event_set_id=fixture["event"]["event_set_id"], output_dir=fixture["event_dir"]
        )["status"]
        == "FAIL"
    )
