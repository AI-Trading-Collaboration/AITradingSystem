from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
from dynamic_v3_backtest_sim_helpers import (
    run_forward_bridge_fixture,
    run_sim_interpretation_fixture,
)

from ai_trading_system.etf_portfolio import dynamic_v3_backtest_simulation as sim
from ai_trading_system.etf_portfolio.dynamic_v3_backtest_simulation import (
    BACKTEST_SIM_VARIANTS,
    REPORT_LABEL_BACKTEST_SIMULATION,
    DynamicV3BacktestSimulationError,
    validate_sim_interpretation_artifact,
)


def test_sim_interpretation_explains_each_variant(
    tmp_path: Path, monkeypatch: Any
) -> None:
    fixture = run_sim_interpretation_fixture(tmp_path, monkeypatch)
    interpretation = fixture["interpretation"]
    matrix = interpretation["variant_interpretation_matrix"]
    findings = interpretation["key_findings"]["findings"]

    variants = {row["variant"]: row for row in matrix["variants"]}
    assert set(variants) == set(BACKTEST_SIM_VARIANTS)
    assert variants["limited_adjustment"]["role"] == "risk_aware_active_tilt"
    assert variants["limited_adjustment"]["evidence_status"] == "AVAILABLE"
    assert variants["consensus_target"]["recommended_usage"] == "upper_bound_reference_only"
    assert (
        variants["defensive_limited_adjustment"]["not_recommended_usage"]
        == "do_not_label_as_proven_defensive"
    )
    assert any(REPORT_LABEL_BACKTEST_SIMULATION in row["limitations"] for row in findings)
    assert interpretation["manifest"]["broker_action_allowed"] is False
    assert interpretation["manifest"]["production_effect"] == "none"

    validation = validate_sim_interpretation_artifact(
        interpretation_id=interpretation["interpretation_id"],
        output_dir=fixture["interpretation_dir"],
    )
    assert validation["status"] == "PASS"


def test_sim_interpretation_rejects_naive_cutoff_before_output(
    tmp_path: Path, monkeypatch: Any
) -> None:
    fixture = run_forward_bridge_fixture(tmp_path, monkeypatch)
    with pytest.raises(DynamicV3BacktestSimulationError, match="timezone-aware"):
        _run_interpretation(fixture, tmp_path / "new", datetime(2026, 7, 31, 10))
    assert not (tmp_path / "new").exists()


def test_sim_interpretation_rejects_invalid_source_before_output(
    tmp_path: Path, monkeypatch: Any
) -> None:
    fixture = run_forward_bridge_fixture(tmp_path, monkeypatch)
    source = fixture["bridge"]["bridge_dir"] / "sim_forward_bridge_report.md"
    source.write_text(source.read_text(encoding="utf-8") + "tamper\n", encoding="utf-8")
    with pytest.raises(DynamicV3BacktestSimulationError, match="source validation"):
        _run_interpretation(
            fixture, tmp_path / "new", datetime(2026, 7, 31, 10, tzinfo=UTC)
        )
    assert not (tmp_path / "new").exists()


def test_sim_interpretation_missing_pairs_remain_insufficient() -> None:
    matrix = sim._variant_interpretation_matrix({}, [])
    findings = sim._sim_key_findings(
        interpretation_matrix=matrix,
        calibration_evidence={},
        bridge_targets={"targets": []},
    )
    assert all(row["evidence_status"] == "INSUFFICIENT_DATA" for row in matrix["variants"])
    assert all(
        row["return_profile"]["avg_20d_return"] is None for row in matrix["variants"]
    )
    assert all(row["confidence"] == "INSUFFICIENT_DATA" for row in findings["findings"])


@pytest.mark.parametrize(
    "artifact_name",
    [
        "sim_interpretation_manifest.json",
        "variant_interpretation_matrix.json",
        "key_findings.json",
        "sim_interpretation_input_snapshot.json",
        "sim_interpretation_report.md",
    ],
)
def test_sim_interpretation_validator_rejects_output_tamper(
    tmp_path: Path, monkeypatch: Any, artifact_name: str
) -> None:
    fixture = run_sim_interpretation_fixture(tmp_path, monkeypatch)
    interpretation = fixture["interpretation"]
    path = interpretation["interpretation_dir"] / artifact_name
    if path.suffix == ".md":
        path.write_text(path.read_text(encoding="utf-8") + "tamper\n", encoding="utf-8")
    else:
        payload = json.loads(path.read_text(encoding="utf-8"))
        payload["tampered"] = True
        path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
    validation = validate_sim_interpretation_artifact(
        interpretation_id=interpretation["interpretation_id"],
        output_dir=fixture["interpretation_dir"],
    )
    assert validation["status"] == "FAIL"


def test_sim_interpretation_validator_rejects_live_source_drift(
    tmp_path: Path, monkeypatch: Any
) -> None:
    fixture = run_sim_interpretation_fixture(tmp_path, monkeypatch)
    interpretation = fixture["interpretation"]
    source = fixture["outcome"]["sim_outcome_dir"] / "backtest_sim_outcome_report.md"
    source.write_text(source.read_text(encoding="utf-8") + "tamper\n", encoding="utf-8")
    validation = validate_sim_interpretation_artifact(
        interpretation_id=interpretation["interpretation_id"],
        output_dir=fixture["interpretation_dir"],
    )
    assert validation["status"] == "FAIL"


def _run_interpretation(
    fixture: dict[str, Any], output_dir: Path, generated_at: datetime
) -> dict[str, Any]:
    return sim.run_sim_interpretation(
        outcome_id=fixture["outcome"]["sim_outcome_id"],
        calibration_id=fixture["calibration"]["calibration_pack_id"],
        bridge_id=fixture["bridge"]["bridge_id"],
        outcome_dir=fixture["outcome_dir"],
        calibration_dir=fixture["calibration_dir"],
        bridge_dir=fixture["bridge_dir"],
        output_dir=output_dir,
        generated_at=generated_at,
    )
