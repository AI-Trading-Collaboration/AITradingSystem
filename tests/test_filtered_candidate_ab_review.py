from __future__ import annotations

from pathlib import Path

from dynamic_v3_filtered_candidate_readiness_helpers import (
    assert_research_safe,
    run_filtered_candidate_ab_review_fixture,
)

from ai_trading_system.etf_portfolio import dynamic_v3_filtered_candidate_readiness as readiness


def test_filtered_candidate_ab_review_builds_and_validates(tmp_path: Path, monkeypatch) -> None:
    fixture = run_filtered_candidate_ab_review_fixture(tmp_path, monkeypatch)
    ab = fixture["filtered_candidate_ab_review"]
    validation = readiness.validate_filtered_candidate_ab_review_artifact(
        ab_review_id=ab["ab_review_id"],
        output_dir=tmp_path / "filtered_candidate_ab_review",
    )
    assert validation["status"] == "PASS"
    summary = ab["ab_summary"]
    assert summary["candidate"] == readiness.TOP_FILTERED_CANDIDATE
    assert summary["evidence_status"] == "INSUFFICIENT_DATA"
    assert summary["overall_ab_status"] == "INSUFFICIENT_DATA"
    assert summary["winner"] is None
    assert summary["confidence"] is None
    assert summary["recommended_next_action"] == "COLLECT_VALIDATED_DATED_FILTERED_OUTCOMES"
    assert ab["ab_method_comparison"] == []
    assert_research_safe(ab)
    assert "Filtered Candidate A/B Review" in ab["reader_brief_section"]
