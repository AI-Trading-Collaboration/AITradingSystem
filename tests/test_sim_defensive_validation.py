from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
from dynamic_v3_backtest_sim_helpers import (
    run_forward_bridge_fixture,
    run_sim_defensive_validation_fixture,
)

from ai_trading_system.etf_portfolio import dynamic_v3_backtest_simulation as sim
from ai_trading_system.etf_portfolio.dynamic_v3_backtest_simulation import (
    DEFENSIVE_PRESSURE_REGIMES,
    DEFENSIVE_VALIDATION_STATUSES,
    DynamicV3BacktestSimulationError,
    validate_sim_defensive_validation_artifact,
)


def test_sim_defensive_validation_does_not_auto_prove_defensive(
    tmp_path: Path, monkeypatch: Any
) -> None:
    fixture = run_sim_defensive_validation_fixture(tmp_path, monkeypatch)
    defensive = fixture["defensive_validation"]
    matrix = defensive["defensive_regime_matrix"]
    summary = defensive["defensive_validation_summary"]

    assert {row["regime"] for row in matrix} >= DEFENSIVE_PRESSURE_REGIMES
    assert summary["defensive_limited_adjustment_status"] in {
        "INSUFFICIENT_DATA",
        "NOT_PROVEN_DEFENSIVE",
        "PARTIALLY_DEFENSIVE",
        "PROVEN_DEFENSIVE",
    }
    assert all(
        row["defensive_limited_adjustment"]["status"] in DEFENSIVE_VALIDATION_STATUSES
        for row in matrix
    )
    assert all(row["paired_window_count"] >= row["paired_event_count"] for row in matrix)
    assert summary["requires_forward_confirmation"] is True
    assert defensive["manifest"]["broker_action_taken"] is False
    assert (
        defensive["defensive_validation_dir"]
        / "sim_defensive_validation_input_snapshot.json"
    ).exists()

    validation = validate_sim_defensive_validation_artifact(
        defensive_validation_id=defensive["defensive_validation_id"],
        output_dir=fixture["defensive_validation_dir"],
    )
    assert validation["status"] == "PASS"


def test_sim_defensive_validation_rejects_naive_cutoff_before_output(
    tmp_path: Path, monkeypatch: Any
) -> None:
    fixture = run_forward_bridge_fixture(tmp_path, monkeypatch)
    with pytest.raises(DynamicV3BacktestSimulationError, match="timezone-aware"):
        _run_defensive(fixture, tmp_path / "new", datetime(2026, 7, 31, 12))
    assert not (tmp_path / "new").exists()


def test_sim_defensive_validation_rejects_invalid_source_before_output(
    tmp_path: Path, monkeypatch: Any
) -> None:
    fixture = run_forward_bridge_fixture(tmp_path, monkeypatch)
    source = fixture["outcome"]["sim_outcome_dir"] / "backtest_sim_outcome_report.md"
    source.write_text(source.read_text(encoding="utf-8") + "tamper\n", encoding="utf-8")
    with pytest.raises(DynamicV3BacktestSimulationError, match="Outcome validation"):
        _run_defensive(
            fixture, tmp_path / "new", datetime(2026, 7, 31, 12, tzinfo=UTC)
        )
    assert not (tmp_path / "new").exists()


def test_sim_defensive_validation_missing_pairs_remain_null() -> None:
    policy = sim._load_sim_defensive_validation_policy(
        sim.DEFAULT_SIM_DEFENSIVE_VALIDATION_POLICY_PATH
    )
    matrix = sim._defensive_regime_matrix([], policy=policy)
    summary = sim._defensive_validation_summary(matrix, policy=policy)

    assert all(row["paired_event_count"] == 0 for row in matrix)
    assert all(row["paired_window_count"] == 0 for row in matrix)
    assert all(
        row["defensive_limited_adjustment"]["avg_return"] is None for row in matrix
    )
    assert all(
        row["defensive_limited_adjustment"]["avg_relative_to_no_trade"] is None
        for row in matrix
    )
    assert summary["defensive_limited_adjustment_status"] == "INSUFFICIENT_DATA"


def test_sim_defensive_validation_rejects_invalid_policy_before_output(
    tmp_path: Path, monkeypatch: Any
) -> None:
    fixture = run_forward_bridge_fixture(tmp_path, monkeypatch)
    policy_path = tmp_path / "invalid_policy.yaml"
    policy_path.write_text("schema_version: invalid\n", encoding="utf-8")
    with pytest.raises(DynamicV3BacktestSimulationError, match="policy schema"):
        _run_defensive(
            fixture,
            tmp_path / "new",
            datetime(2026, 7, 31, 12, tzinfo=UTC),
            policy_path=policy_path,
        )
    assert not (tmp_path / "new").exists()


@pytest.mark.parametrize(
    "artifact_name",
    [
        "defensive_validation_manifest.json",
        "defensive_regime_matrix.jsonl",
        "defensive_failure_cases.jsonl",
        "defensive_validation_summary.json",
        "sim_defensive_validation_input_snapshot.json",
        "defensive_validation_report.md",
    ],
)
def test_sim_defensive_validation_validator_rejects_output_tamper(
    tmp_path: Path, monkeypatch: Any, artifact_name: str
) -> None:
    fixture = run_sim_defensive_validation_fixture(tmp_path, monkeypatch)
    defensive = fixture["defensive_validation"]
    path = defensive["defensive_validation_dir"] / artifact_name
    if path.suffix == ".md":
        path.write_text(path.read_text(encoding="utf-8") + "tamper\n", encoding="utf-8")
    elif path.suffix == ".jsonl":
        lines = path.read_text(encoding="utf-8").splitlines()
        payload = json.loads(lines[0]) if lines else {"tampered": True}
        payload["tampered"] = True
        text = json.dumps(payload, ensure_ascii=False, sort_keys=True)
        path.write_text("\n".join([text, *lines[1:]]) + "\n", encoding="utf-8")
    else:
        payload = json.loads(path.read_text(encoding="utf-8"))
        payload["tampered"] = True
        path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
    validation = validate_sim_defensive_validation_artifact(
        defensive_validation_id=defensive["defensive_validation_id"],
        output_dir=fixture["defensive_validation_dir"],
    )
    assert validation["status"] == "FAIL"


def test_sim_defensive_validation_validator_rejects_live_outcome_drift(
    tmp_path: Path, monkeypatch: Any
) -> None:
    fixture = run_sim_defensive_validation_fixture(tmp_path, monkeypatch)
    defensive = fixture["defensive_validation"]
    source = fixture["outcome"]["sim_outcome_dir"] / "backtest_sim_outcome_report.md"
    source.write_text(source.read_text(encoding="utf-8") + "tamper\n", encoding="utf-8")
    validation = validate_sim_defensive_validation_artifact(
        defensive_validation_id=defensive["defensive_validation_id"],
        output_dir=fixture["defensive_validation_dir"],
    )
    assert validation["status"] == "FAIL"


def test_sim_defensive_validation_validator_rejects_live_policy_drift(
    tmp_path: Path, monkeypatch: Any
) -> None:
    fixture = run_forward_bridge_fixture(tmp_path, monkeypatch)
    policy_path = tmp_path / "policy.yaml"
    policy_path.write_text(
        sim.DEFAULT_SIM_DEFENSIVE_VALIDATION_POLICY_PATH.read_text(encoding="utf-8"),
        encoding="utf-8",
    )
    defensive = _run_defensive(
        fixture,
        fixture["defensive_validation_dir"],
        datetime(2026, 7, 31, 12, tzinfo=UTC),
        policy_path=policy_path,
    )
    policy_path.write_text(
        policy_path.read_text(encoding="utf-8") + "# drift\n", encoding="utf-8"
    )
    validation = validate_sim_defensive_validation_artifact(
        defensive_validation_id=defensive["defensive_validation_id"],
        output_dir=fixture["defensive_validation_dir"],
    )
    assert validation["status"] == "FAIL"


def _run_defensive(
    fixture: dict[str, Any],
    output_dir: Path,
    generated_at: datetime,
    *,
    policy_path: Path = sim.DEFAULT_SIM_DEFENSIVE_VALIDATION_POLICY_PATH,
) -> dict[str, Any]:
    return sim.run_sim_defensive_validation(
        outcome_id=fixture["outcome"]["sim_outcome_id"],
        outcome_dir=fixture["outcome_dir"],
        output_dir=output_dir,
        policy_path=policy_path,
        generated_at=generated_at,
    )
