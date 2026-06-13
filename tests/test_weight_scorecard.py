from __future__ import annotations

from dynamic_v3_weight_batch_search_helpers import run_weight_scorecard_fixture

from ai_trading_system.etf_portfolio import dynamic_v3_weight_batch_search as weight_search


def test_weight_scorecard_ranks_variants_with_hard_rejects_visible(tmp_path) -> None:
    fixture = run_weight_scorecard_fixture(tmp_path)
    scorecard = fixture["scorecard"]

    assert scorecard["manifest"]["status"] == "PASS"
    assert scorecard["variant_scorecard"]
    assert "candidates" in scorecard["pareto_frontier"]
    assert "promote_count" in scorecard["score_distribution"]
    assert all("hard_reject_flags" in row for row in scorecard["variant_scorecard"])

    validation = weight_search.validate_weight_scorecard_artifact(
        scorecard_id=scorecard["scorecard_id"],
        output_dir=tmp_path / "weight_scorecard",
    )
    assert validation["status"] == "PASS"
