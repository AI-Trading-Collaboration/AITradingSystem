from __future__ import annotations

from dynamic_v3_weight_batch_search_helpers import run_candidate_promotion_v2_fixture

from ai_trading_system.etf_portfolio import dynamic_v3_weight_batch_search as weight_search


def test_candidate_promotion_v2_remains_owner_review_only(tmp_path) -> None:
    fixture = run_candidate_promotion_v2_fixture(tmp_path)
    promotion = fixture["promotion_v2"]
    decision = promotion["promotion_v2_decision"]

    assert promotion["manifest"]["status"] == "PASS"
    assert decision["decision"] in {
        "PROMOTE_CANDIDATE",
        "KEEP_TESTING",
        "RUN_ANOTHER_TARGETED_SEARCH",
        "NO_CANDIDATE",
    }
    assert decision["not_official_target_weights"] is True
    assert decision["broker_action_allowed"] is False

    validation = weight_search.validate_candidate_promotion_v2_artifact(
        promotion_v2_id=promotion["promotion_v2_id"],
        output_dir=tmp_path / "candidate_promotion_v2",
    )
    assert validation["status"] == "PASS"
