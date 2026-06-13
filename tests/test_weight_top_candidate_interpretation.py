from __future__ import annotations

from dynamic_v3_weight_batch_search_helpers import run_weight_top_candidate_interpretation_fixture

from ai_trading_system.etf_portfolio import dynamic_v3_weight_batch_search as weight_search


def test_weight_top_candidate_interpretation_explains_tradeoffs(tmp_path) -> None:
    fixture = run_weight_top_candidate_interpretation_fixture(tmp_path)
    interpretation = fixture["interpretation"]

    assert interpretation["manifest"]["status"] == "PASS"
    assert interpretation["top_candidate_explanations"]
    assert interpretation["failure_mode_coverage"]["failure_modes"]
    assert "research_only" in interpretation["reader_brief_section"]

    validation = weight_search.validate_weight_top_candidate_interpretation_artifact(
        interpretation_id=interpretation["interpretation_id"],
        output_dir=tmp_path / "weight_top_candidate_interpretation",
    )
    assert validation["status"] == "PASS"
