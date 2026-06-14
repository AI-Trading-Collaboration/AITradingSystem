from __future__ import annotations

from pathlib import Path

from dynamic_v3_filtered_candidate_readiness_helpers import (
    assert_research_safe,
    run_filtered_candidate_evidence_fixture,
)

from ai_trading_system.etf_portfolio import dynamic_v3_filtered_candidate_readiness as readiness


def test_filtered_candidate_evidence_builds_and_validates(tmp_path: Path) -> None:
    fixture = run_filtered_candidate_evidence_fixture(tmp_path)
    evidence = fixture["filtered_candidate_evidence"]
    validation = readiness.validate_filtered_candidate_evidence_artifact(
        evidence_id=evidence["evidence_id"],
        output_dir=tmp_path / "filtered_candidate_evidence",
    )
    assert validation["status"] == "PASS"
    summary = evidence["filtered_candidate_evidence_summary"]
    assert summary["candidate"] == readiness.TOP_FILTERED_CANDIDATE
    assert summary["evidence_status"] in {"PROMISING", "MIXED"}
    assert summary["requires_more_evidence"] is True
    assert "forward_confirmation_missing" in summary["primary_weaknesses"]
    assert len(evidence["evidence_component_breakdown"]["components"]) >= 4
    assert_research_safe(evidence["manifest"])
    assert "Filtered Candidate Evidence" in evidence["reader_brief_section"]
