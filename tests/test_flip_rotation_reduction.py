from __future__ import annotations

from pathlib import Path

from dynamic_v3_filtered_candidate_readiness_helpers import (
    assert_research_safe,
    run_flip_rotation_reduction_fixture,
)

from ai_trading_system.etf_portfolio import dynamic_v3_filtered_candidate_readiness as readiness


def test_flip_rotation_reduction_builds_and_validates(tmp_path: Path, monkeypatch) -> None:
    fixture = run_flip_rotation_reduction_fixture(tmp_path, monkeypatch)
    flip = fixture["flip_rotation_reduction"]
    validation = readiness.validate_flip_rotation_reduction_artifact(
        flip_reduction_id=flip["flip_reduction_id"],
        output_dir=tmp_path / "flip_rotation_reduction",
    )
    assert validation["status"] == "PASS"
    summary = flip["flip_rotation_reduction_summary"]
    assert summary["evidence_status"] == "INSUFFICIENT_DATA"
    assert summary["direction_flip_before"] is None
    assert summary["direction_flip_after"] is None
    assert summary["top_candidate_rotation_before"] is None
    assert summary["top_candidate_rotation_after"] is None
    assert summary["signal_churn_before"] is None
    assert summary["signal_churn_after"] is None
    assert flip["flip_rotation_events"] == []
    assert_research_safe(flip)
