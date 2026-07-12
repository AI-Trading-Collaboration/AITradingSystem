from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
from dynamic_v3_backtest_sim_helpers import (
    run_forward_bridge_fixture,
    run_sim_risk_return_fixture,
)

from ai_trading_system.etf_portfolio import dynamic_v3_backtest_simulation as sim
from ai_trading_system.etf_portfolio.dynamic_v3_backtest_simulation import (
    ACTIVE_SIM_VARIANTS,
    RISK_RETURN_STATUSES,
    DynamicV3BacktestSimulationError,
    validate_sim_risk_return_artifact,
)


def test_sim_risk_return_separates_return_and_drawdown(
    tmp_path: Path, monkeypatch: Any
) -> None:
    fixture = run_sim_risk_return_fixture(tmp_path, monkeypatch)
    risk_return = fixture["risk_return"]
    rows = risk_return["risk_adjusted_summary"]["summary"]

    assert {row["variant"] for row in rows} == set(ACTIVE_SIM_VARIANTS)
    assert all(row["risk_return_status"] in RISK_RETURN_STATUSES for row in rows)
    assert all(row["paired_event_count"] == row["paired_window_count"] for row in rows)
    assert all(row["paired_event_count"] > 0 for row in rows)
    assert all("return_improvement_20d_pp" in row for row in rows)
    assert all("drawdown_worsening_20d_pp" in row for row in rows)
    assert (
        risk_return["risk_return_dir"] / "active_variant_tradeoff_table.csv"
    ).exists()
    assert (
        risk_return["risk_return_dir"] / "sim_risk_return_input_snapshot.json"
    ).exists()
    assert risk_return["manifest"]["auto_policy_apply"] is False
    assert risk_return["manifest"]["production_effect"] == "none"

    validation = validate_sim_risk_return_artifact(
        risk_return_id=risk_return["risk_return_id"],
        output_dir=fixture["risk_return_dir"],
    )
    assert validation["status"] == "PASS"


def test_sim_risk_return_rejects_naive_cutoff_before_output(
    tmp_path: Path, monkeypatch: Any
) -> None:
    fixture = run_forward_bridge_fixture(tmp_path, monkeypatch)
    with pytest.raises(DynamicV3BacktestSimulationError, match="timezone-aware"):
        _run_risk_return(fixture, tmp_path / "new", datetime(2026, 7, 31, 11))
    assert not (tmp_path / "new").exists()


def test_sim_risk_return_rejects_invalid_source_before_output(
    tmp_path: Path, monkeypatch: Any
) -> None:
    fixture = run_forward_bridge_fixture(tmp_path, monkeypatch)
    source = fixture["outcome"]["sim_outcome_dir"] / "backtest_sim_outcome_report.md"
    source.write_text(source.read_text(encoding="utf-8") + "tamper\n", encoding="utf-8")
    with pytest.raises(DynamicV3BacktestSimulationError, match="Outcome validation"):
        _run_risk_return(
            fixture, tmp_path / "new", datetime(2026, 7, 31, 11, tzinfo=UTC)
        )
    assert not (tmp_path / "new").exists()


def test_sim_risk_return_missing_pairs_remain_insufficient() -> None:
    table = sim._risk_return_tradeoff_table([])
    summary = sim._risk_adjusted_summary(table)

    assert {row["variant"] for row in table} == set(ACTIVE_SIM_VARIANTS)
    assert all(row["evidence_status"] == "INSUFFICIENT_DATA" for row in table)
    assert all(row["paired_event_count"] == 0 for row in table)
    assert all(row["delta_20d_return_vs_no_trade"] is None for row in table)
    assert all(row["return_per_drawdown_worsening"] is None for row in table)
    assert all(
        row["risk_return_status"] == "INSUFFICIENT_DATA"
        for row in summary["summary"]
    )


@pytest.mark.parametrize(
    "artifact_name",
    [
        "risk_return_manifest.json",
        "active_variant_tradeoff_table.csv",
        "risk_adjusted_summary.json",
        "sim_risk_return_input_snapshot.json",
        "risk_return_report.md",
    ],
)
def test_sim_risk_return_validator_rejects_output_tamper(
    tmp_path: Path, monkeypatch: Any, artifact_name: str
) -> None:
    fixture = run_sim_risk_return_fixture(tmp_path, monkeypatch)
    risk_return = fixture["risk_return"]
    path = risk_return["risk_return_dir"] / artifact_name
    if path.suffix in {".md", ".csv"}:
        path.write_text(path.read_text(encoding="utf-8") + "tamper\n", encoding="utf-8")
    else:
        payload = json.loads(path.read_text(encoding="utf-8"))
        payload["tampered"] = True
        path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
    validation = validate_sim_risk_return_artifact(
        risk_return_id=risk_return["risk_return_id"],
        output_dir=fixture["risk_return_dir"],
    )
    assert validation["status"] == "FAIL"


def test_sim_risk_return_validator_rejects_live_source_drift(
    tmp_path: Path, monkeypatch: Any
) -> None:
    fixture = run_sim_risk_return_fixture(tmp_path, monkeypatch)
    risk_return = fixture["risk_return"]
    source = fixture["outcome"]["sim_outcome_dir"] / "backtest_sim_outcome_report.md"
    source.write_text(source.read_text(encoding="utf-8") + "tamper\n", encoding="utf-8")
    validation = validate_sim_risk_return_artifact(
        risk_return_id=risk_return["risk_return_id"],
        output_dir=fixture["risk_return_dir"],
    )
    assert validation["status"] == "FAIL"


def _run_risk_return(
    fixture: dict[str, Any], output_dir: Path, generated_at: datetime
) -> dict[str, Any]:
    return sim.run_sim_risk_return(
        outcome_id=fixture["outcome"]["sim_outcome_id"],
        outcome_dir=fixture["outcome_dir"],
        output_dir=output_dir,
        generated_at=generated_at,
    )
