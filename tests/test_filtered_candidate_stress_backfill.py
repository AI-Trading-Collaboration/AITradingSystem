from __future__ import annotations

from pathlib import Path

from dynamic_v3_filtered_candidate_readiness_helpers import (
    assert_research_safe,
    run_filtered_candidate_stress_backfill_fixture,
)

from ai_trading_system.etf_portfolio import dynamic_v3_filtered_candidate_readiness as readiness


def test_filtered_candidate_stress_backfill_builds_and_validates(tmp_path: Path) -> None:
    fixture = run_filtered_candidate_stress_backfill_fixture(tmp_path)
    stress = fixture["filtered_candidate_stress_backfill"]
    validation = readiness.validate_filtered_candidate_stress_backfill_artifact(
        stress_backfill_id=stress["stress_backfill_id"],
        output_dir=tmp_path / "filtered_candidate_stress_backfill",
    )
    assert validation["status"] == "PASS"
    summary = stress["filtered_candidate_stress_summary"]
    assert summary["stress_windows_total"] >= 6
    assert summary["stress_robustness_status"] in {"STRONG", "MIXED"}
    regimes = {row["regime"] for row in stress["stress_window_metrics"]}
    assert {"tech_drawdown", "risk_off", "strong_recovery"} <= regimes
    assert_research_safe(stress["manifest"])
