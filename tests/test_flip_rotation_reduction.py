from __future__ import annotations

from pathlib import Path

from dynamic_v3_filtered_candidate_readiness_helpers import (
    assert_research_safe,
    run_flip_rotation_reduction_fixture,
)

from ai_trading_system.etf_portfolio import dynamic_v3_filtered_candidate_readiness as readiness


def test_flip_rotation_reduction_builds_and_validates(tmp_path: Path) -> None:
    fixture = run_flip_rotation_reduction_fixture(tmp_path)
    flip = fixture["flip_rotation_reduction"]
    validation = readiness.validate_flip_rotation_reduction_artifact(
        flip_reduction_id=flip["flip_reduction_id"],
        output_dir=tmp_path / "flip_rotation_reduction",
    )
    assert validation["status"] == "PASS"
    summary = flip["flip_rotation_reduction_summary"]
    assert summary["direction_flip_after"] < summary["direction_flip_before"]
    assert summary["top_candidate_rotation_after"] < summary["top_candidate_rotation_before"]
    assert summary["signal_churn_after"] < summary["signal_churn_before"]
    assert_research_safe(flip["manifest"])
