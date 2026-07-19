from __future__ import annotations

from pathlib import Path

from dynamic_v3_filtered_candidate_readiness_helpers import (
    assert_research_safe,
    run_filtered_candidate_stress_backfill_fixture,
)

from ai_trading_system.etf_portfolio import dynamic_v3_filtered_candidate_readiness as readiness


def test_filtered_candidate_stress_backfill_builds_and_validates(
    tmp_path: Path, monkeypatch
) -> None:
    fixture = run_filtered_candidate_stress_backfill_fixture(tmp_path, monkeypatch)
    stress = fixture["filtered_candidate_stress_backfill"]
    validation = readiness.validate_filtered_candidate_stress_backfill_artifact(
        stress_backfill_id=stress["stress_backfill_id"],
        output_dir=tmp_path / "filtered_candidate_stress_backfill",
    )
    assert validation["status"] == "PASS"
    summary = stress["filtered_candidate_stress_summary"]
    assert summary["evidence_status"] == "INSUFFICIENT_DATA"
    assert summary["stress_windows_total"] == 0
    assert summary["stress_robustness_status"] == "INSUFFICIENT_DATA"
    assert stress["stress_window_inventory"] == []
    assert stress["stress_window_metrics"] == []
    assert_research_safe(stress)
