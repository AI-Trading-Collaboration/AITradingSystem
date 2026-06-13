from __future__ import annotations

from dynamic_v3_weight_batch_search_helpers import run_weight_robustness_fixture

from ai_trading_system.etf_portfolio import dynamic_v3_weight_batch_search as weight_search


def test_weight_robustness_review_links_top_candidates_to_regime_and_stability(tmp_path) -> None:
    fixture = run_weight_robustness_fixture(tmp_path)
    robustness = fixture["robustness"]

    assert robustness["manifest"]["status"] == "PASS"
    assert robustness["rolling_robustness"]
    assert robustness["regime_robustness"]
    assert robustness["stability_robustness"]
    assert "recommended_next_action" in robustness["robustness_summary"]

    validation = weight_search.validate_weight_robustness_review_artifact(
        robustness_id=robustness["robustness_id"],
        output_dir=tmp_path / "weight_robustness_review",
    )
    assert validation["status"] == "PASS"
