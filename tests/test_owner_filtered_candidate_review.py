from __future__ import annotations

from pathlib import Path

from dynamic_v3_filtered_candidate_readiness_helpers import (
    assert_research_safe,
    run_owner_filtered_candidate_review_fixture,
)

from ai_trading_system.etf_portfolio import dynamic_v3_filtered_candidate_readiness as readiness


def test_owner_filtered_candidate_review_builds_and_validates(tmp_path: Path) -> None:
    fixture = run_owner_filtered_candidate_review_fixture(tmp_path)
    owner_review = fixture["owner_filtered_candidate_review"]
    validation = readiness.validate_owner_filtered_candidate_review_artifact(
        owner_review_id=owner_review["owner_review_id"],
        output_dir=tmp_path / "owner_filtered_candidate_review",
    )
    assert validation["status"] == "PASS"
    summary = owner_review["owner_filtered_candidate_summary"]
    assert summary["recommended_owner_action"] in {
        "formalize_research_method",
        "defer_for_forward_confirmation",
    }
    checklist = owner_review["owner_filtered_candidate_checklist"]
    assert "official target weights" in checklist
    assert "no broker / no production" in checklist
    assert_research_safe(owner_review["manifest"])
    assert "Owner Filtered Candidate Review" in owner_review["reader_brief_section"]
