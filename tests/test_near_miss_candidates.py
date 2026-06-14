from __future__ import annotations

from dynamic_v3_weight_batch_search_helpers import run_near_miss_candidates_fixture

from ai_trading_system.etf_portfolio import dynamic_v3_weight_batch_search as weight_search


def test_near_miss_candidates_capture_research_followups(tmp_path) -> None:
    fixture = run_near_miss_candidates_fixture(tmp_path)
    near_miss = fixture["near_miss"]

    assert near_miss["manifest"]["status"] == "PASS"
    assert near_miss["manifest"]["candidate_count"] > 0
    assert near_miss["manifest"]["cash_buffer_10_near_miss"] is True
    assert near_miss["near_miss_family_summary"]["recommended_focus_families"]

    validation = weight_search.validate_near_miss_candidates_artifact(
        near_miss_id=near_miss["near_miss_id"],
        output_dir=tmp_path / "near_miss_candidates",
    )
    assert validation["status"] == "PASS"
