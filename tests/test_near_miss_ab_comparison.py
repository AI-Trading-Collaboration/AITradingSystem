from __future__ import annotations

from dynamic_v3_weight_batch_search_helpers import run_near_miss_ab_comparison_fixture

from ai_trading_system.etf_portfolio import dynamic_v3_weight_batch_search as weight_search
from ai_trading_system.platform.artifacts.validation_session import (
    with_artifact_validation_session,
)


@with_artifact_validation_session
def test_near_miss_ab_comparison_ranks_targeted_v3_against_parents(tmp_path) -> None:
    fixture = run_near_miss_ab_comparison_fixture(tmp_path)
    comparison = fixture["near_miss_ab"]

    assert comparison["manifest"]["status"] == "PASS"
    assert comparison["ab_comparison_matrix"]
    assert comparison["ab_winner_summary"]["best_v3_variant"]
    assert (
        comparison["ab_winner_summary"]["recommended_next_action"]
        == "promotion_threshold_sensitivity"
    )

    validation = weight_search.validate_near_miss_ab_comparison_artifact(
        ab_id=comparison["ab_id"],
        output_dir=tmp_path / "near_miss_ab_comparison",
    )
    assert validation["status"] == "PASS"
