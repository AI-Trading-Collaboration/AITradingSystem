from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
from dynamic_v3_backtest_sim_helpers import run_calibration_fixture, run_paper_fixture

from ai_trading_system.etf_portfolio import dynamic_v3_backtest_simulation as sim


def test_backtest_sim_calibration_pack_is_review_only(tmp_path: Path, monkeypatch: Any) -> None:
    fixture = run_calibration_fixture(tmp_path, monkeypatch)
    calibration = fixture["calibration"]
    proposals = calibration["proposed_advisory_rule_changes"]["proposals"]

    assert calibration["manifest"]["status"] == "PASS"
    assert calibration["manifest"]["auto_apply"] is False
    assert calibration["manifest"]["can_trigger_production"] is False
    assert calibration["simulation_evidence_summary"]["calibration_readiness"] in {
        "REVIEW_ONLY",
        "FORWARD_CONFIRMATION_REQUIRED",
    }
    assert proposals
    assert all(row["auto_apply"] is False for row in proposals)
    assert calibration["input_snapshot"]["source_bundles"]
    assert set(calibration["input_snapshot"]["source_validations"]) == {
        "outcome",
        "paper",
        "regime",
        "sensitivity",
    }
    assert (
        calibration["simulation_evidence_summary"]["finite_metric_count"]
        + calibration["simulation_evidence_summary"]["missing_metric_count"]
        == 7
    )

    validation = sim.validate_backtest_sim_calibration_artifact(
        calibration_pack_id=calibration["calibration_pack_id"],
        output_dir=fixture["calibration_dir"],
    )
    assert validation["status"] == "PASS"


def _run_sources(tmp_path: Path, monkeypatch: Any) -> dict[str, Any]:
    fixture = run_paper_fixture(tmp_path, monkeypatch)
    regime = sim.run_backtest_sim_regime_review(
        sim_outcome_id=fixture["outcome"]["sim_outcome_id"],
        outcome_dir=fixture["outcome_dir"],
        output_dir=fixture["regime_dir"],
        generated_at=datetime(2026, 7, 31, 6, tzinfo=UTC),
    )
    sensitivity = sim.run_backtest_sim_sensitivity(
        sim_outcome_id=fixture["outcome"]["sim_outcome_id"],
        outcome_dir=fixture["outcome_dir"],
        variant_dir=fixture["variant_dir"],
        event_dir=fixture["event_dir"],
        output_dir=fixture["sensitivity_dir"],
        generated_at=datetime(2026, 7, 31, 7, tzinfo=UTC),
    )
    return {**fixture, "regime": regime, "sensitivity": sensitivity}


def _run_calibration_from_sources(
    fixture: dict[str, Any], *, output_dir: Path, generated_at: datetime
) -> dict[str, Any]:
    return sim.run_backtest_sim_calibration_pack(
        sim_outcome_id=fixture["outcome"]["sim_outcome_id"],
        sim_paper_id=fixture["paper"]["sim_paper_id"],
        regime_review_id=fixture["regime"]["regime_review_id"],
        sensitivity_id=fixture["sensitivity"]["sensitivity_id"],
        outcome_dir=fixture["outcome_dir"],
        paper_dir=fixture["paper_dir"],
        regime_dir=fixture["regime_dir"],
        sensitivity_dir=fixture["sensitivity_dir"],
        output_dir=output_dir,
        generated_at=generated_at,
    )


def test_backtest_sim_calibration_rejects_naive_time_before_output(
    tmp_path: Path, monkeypatch: Any
) -> None:
    fixture = _run_sources(tmp_path, monkeypatch)
    with pytest.raises(sim.DynamicV3BacktestSimulationError, match="timezone-aware"):
        _run_calibration_from_sources(
            fixture,
            output_dir=tmp_path / "new_calibration",
            generated_at=datetime(2026, 7, 31, 8),
        )
    assert not (tmp_path / "new_calibration").exists()


def test_backtest_sim_calibration_rejects_invalid_source_before_output(
    tmp_path: Path, monkeypatch: Any
) -> None:
    fixture = _run_sources(tmp_path, monkeypatch)
    source = fixture["regime"]["regime_review_dir"] / "backtest_sim_regime_report.md"
    source.write_text(source.read_text(encoding="utf-8") + "\n", encoding="utf-8")
    with pytest.raises(sim.DynamicV3BacktestSimulationError, match="source validation failed"):
        _run_calibration_from_sources(
            fixture,
            output_dir=tmp_path / "new_calibration",
            generated_at=datetime(2026, 7, 31, 8, tzinfo=UTC),
        )
    assert not (tmp_path / "new_calibration").exists()


def test_backtest_sim_calibration_preserves_missing_metrics_as_null() -> None:
    evidence = sim._calibration_evidence({}, {}, {}, {})
    assert evidence["finite_metric_count"] == 0
    assert evidence["missing_metric_count"] == 7
    assert evidence["limited_adjustment_vs_no_trade_5d"] is None
    assert evidence["paper_total_return"] is None


def test_backtest_sim_calibration_positive_proposal_requires_low_risk() -> None:
    evidence = {"limited_adjustment_vs_no_trade_5d": 0.01}
    review = sim._calibration_proposals(evidence, {"simulation_overfit_status": "REVIEW_REQUIRED"})
    low = sim._calibration_proposals(evidence, {"simulation_overfit_status": "LOW_RISK"})
    assert "keep_limited_adjustment_default" not in {
        row["proposal_id"] for row in review["proposals"]
    }
    assert "keep_limited_adjustment_default" in {row["proposal_id"] for row in low["proposals"]}


@pytest.mark.parametrize(
    "artifact_name",
    [
        "sim_calibration_manifest.json",
        "simulation_evidence_summary.json",
        "proposed_advisory_rule_changes.json",
        "simulation_limitations.json",
        "calibration_input_snapshot.json",
        "backtest_sim_calibration_report.md",
        "reader_brief_section.md",
    ],
)
def test_backtest_sim_calibration_validator_rejects_view_tamper(
    tmp_path: Path, monkeypatch: Any, artifact_name: str
) -> None:
    fixture = run_calibration_fixture(tmp_path, monkeypatch)
    calibration = fixture["calibration"]
    path = calibration["calibration_pack_dir"] / artifact_name
    if path.suffix == ".md":
        path.write_text(path.read_text(encoding="utf-8") + "\n", encoding="utf-8")
    else:
        payload = json.loads(path.read_text(encoding="utf-8"))
        payload["tampered"] = True
        path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
    validation = sim.validate_backtest_sim_calibration_artifact(
        calibration_pack_id=calibration["calibration_pack_id"],
        output_dir=fixture["calibration_dir"],
    )
    assert validation["status"] == "FAIL"


def test_backtest_sim_calibration_validator_rejects_live_source_tamper(
    tmp_path: Path, monkeypatch: Any
) -> None:
    fixture = run_calibration_fixture(tmp_path, monkeypatch)
    calibration = fixture["calibration"]
    source = fixture["sensitivity"]["sensitivity_dir"] / "backtest_sim_sensitivity_report.md"
    source.write_text(source.read_text(encoding="utf-8") + "\n", encoding="utf-8")
    validation = sim.validate_backtest_sim_calibration_artifact(
        calibration_pack_id=calibration["calibration_pack_id"],
        output_dir=fixture["calibration_dir"],
    )
    assert validation["status"] == "FAIL"
