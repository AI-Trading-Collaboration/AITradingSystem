from __future__ import annotations

from pathlib import Path

from dynamic_v3_filtered_candidate_readiness_helpers import (
    assert_research_safe,
    run_filtered_candidate_evidence_fixture,
)

from ai_trading_system.etf_portfolio import dynamic_v3_filtered_candidate_readiness as readiness


def test_filtered_candidate_evidence_builds_and_validates(tmp_path: Path, monkeypatch) -> None:
    fixture = run_filtered_candidate_evidence_fixture(tmp_path, monkeypatch)
    evidence = fixture["filtered_candidate_evidence"]
    validation = readiness.validate_filtered_candidate_evidence_artifact(
        evidence_id=evidence["evidence_id"],
        output_dir=tmp_path / "filtered_candidate_evidence",
    )
    assert validation["status"] == "PASS"
    summary = evidence["filtered_candidate_evidence_summary"]
    assert summary["candidate"] == readiness.TOP_FILTERED_CANDIDATE
    assert summary["evidence_status"] == "INSUFFICIENT_DATA"
    assert summary["observed_comparison_row_count"] == 0
    assert summary["requires_more_evidence"] is True
    assert summary["primary_weaknesses"] == ["validated_dated_filtered_outcomes_missing"]
    assert evidence["evidence_component_breakdown"]["components"] == []
    assert_research_safe(evidence)
    assert "Filtered Candidate Evidence" in evidence["reader_brief_section"]
