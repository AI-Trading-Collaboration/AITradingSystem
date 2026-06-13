from __future__ import annotations

from dynamic_v3_weight_batch_search_helpers import run_weight_search_dashboard_fixture

from ai_trading_system.etf_portfolio import dynamic_v3_weight_batch_search as weight_search


def test_weight_search_dashboard_summarizes_search_and_next_actions(tmp_path) -> None:
    fixture = run_weight_search_dashboard_fixture(tmp_path)
    dashboard = fixture["dashboard"]

    assert dashboard["manifest"]["status"] == "PASS"
    assert dashboard["search_summary"]["variants_total"] >= 50
    assert dashboard["top_candidates"]["top_overall_candidate"]
    assert dashboard["next_actions"]["recommended_next_action"]
    assert "Weight Optimization Batch Search" in dashboard["reader_brief_section"]

    validation = weight_search.validate_weight_search_dashboard_artifact(
        dashboard_id=dashboard["dashboard_id"],
        output_dir=tmp_path / "weight_search_dashboard",
    )
    assert validation["status"] == "PASS"
