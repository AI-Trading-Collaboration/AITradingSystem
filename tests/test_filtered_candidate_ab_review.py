from __future__ import annotations

from pathlib import Path

from dynamic_v3_filtered_candidate_readiness_helpers import (
    assert_research_safe,
    run_filtered_candidate_ab_review_fixture,
)

from ai_trading_system.etf_portfolio import dynamic_v3_filtered_candidate_readiness as readiness


def test_filtered_candidate_ab_review_builds_and_validates(tmp_path: Path) -> None:
    fixture = run_filtered_candidate_ab_review_fixture(tmp_path)
    ab = fixture["filtered_candidate_ab_review"]
    validation = readiness.validate_filtered_candidate_ab_review_artifact(
        ab_review_id=ab["ab_review_id"],
        output_dir=tmp_path / "filtered_candidate_ab_review",
    )
    assert validation["status"] == "PASS"
    summary = ab["ab_summary"]
    assert summary["candidate"] == readiness.TOP_FILTERED_CANDIDATE
    assert summary["recommended_next_action"] in {
        "formalization_readiness_gate",
        "continue_testing",
    }
    baselines = {row["baseline"] for row in ab["ab_method_comparison"]}
    assert {"smooth_weights_3d_limited_adjustment", "median_target_weights"} <= baselines
    assert_research_safe(ab["manifest"])
    assert "Filtered Candidate A/B Review" in ab["reader_brief_section"]
