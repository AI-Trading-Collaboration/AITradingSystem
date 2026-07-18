from __future__ import annotations

from dynamic_v3_weight_batch_search_helpers import run_no_promotion_review_fixture

from ai_trading_system.etf_portfolio import dynamic_v3_weight_batch_search as weight_search


def test_no_promotion_review_explains_zero_promotions(tmp_path) -> None:
    fixture = run_no_promotion_review_fixture(tmp_path, compact_test_matrix=True)
    review = fixture["no_promotion_review"]

    assert review["manifest"]["status"] == "PASS"
    assert review["manifest"]["promoted_candidate_count"] == 0
    assert review["no_promotion_reason_summary"]["primary_reasons"]
    assert review["manifest"]["broker_action_allowed"] is False

    validation = weight_search.validate_no_promotion_review_artifact(
        review_id=review["review_id"],
        output_dir=tmp_path / "no_promotion_review",
    )
    assert validation["status"] == "PASS"
