from __future__ import annotations

from pathlib import Path
from typing import Any

from dynamic_v3_backtest_sim_helpers import run_calibration_fixture

from ai_trading_system.etf_portfolio.dynamic_v3_backtest_simulation import (
    validate_backtest_sim_calibration_artifact,
)


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

    validation = validate_backtest_sim_calibration_artifact(
        calibration_pack_id=calibration["calibration_pack_id"],
        output_dir=fixture["calibration_dir"],
    )
    assert validation["status"] == "PASS"
