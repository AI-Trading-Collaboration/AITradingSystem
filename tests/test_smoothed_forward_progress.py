from __future__ import annotations

from dynamic_v3_system_target_helpers import run_smoothed_forward_ops_chain_fixture

from ai_trading_system.etf_portfolio import dynamic_v3_system_target as system_target


def test_smoothed_forward_progress_preserves_unregistered_null_candidate(tmp_path) -> None:
    fixture = run_smoothed_forward_ops_chain_fixture(tmp_path)
    progress = fixture["progress"]

    summary = progress["smoothed_forward_progress_summary"]
    assert summary["binding_id"] == fixture["binding"]["binding_id"]
    assert summary["candidate_method"] is None
    assert summary["binding_status"] == "NOT_REGISTERED"
    assert summary["progress_status"] == "NOT_REGISTERED"
    assert summary["targets_total"] == 0
    assert summary["available_forward_events_total"] == 0
    assert summary["required_forward_events_total"] == 0
    assert summary["available_sideways_events"] == 0
    assert summary["required_sideways_events"] == 0
    assert summary["available_recovery_events"] == 0
    assert summary["required_recovery_events"] == 0
    assert summary["summary_recommendation"] == "request_more_forward_data"

    assert progress["smoothed_target_progress"] == []
    assert progress["progress_evidence_commitments"]["target_ids"] == []
    assert progress["progress_evidence_commitments"]["updated_outcomes"] == []
    assert progress["progress_evidence_commitments"]["classified_events"] == []
    assert "Dynamic Rescue Smoothed Forward Progress" in progress["reader_brief_section"]

    validation = system_target.validate_smoothed_forward_progress_artifact(
        progress_id=progress["progress_id"],
        output_dir=tmp_path / "smoothed_forward_progress",
    )
    assert validation["status"] == "PASS"
