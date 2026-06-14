from __future__ import annotations

from dynamic_v3_weight_batch_search_helpers import run_scorecard_attribution_fixture

from ai_trading_system.etf_portfolio import dynamic_v3_weight_batch_search as weight_search


def test_scorecard_attribution_records_rejected_variant_components(tmp_path) -> None:
    fixture = run_scorecard_attribution_fixture(tmp_path)
    attribution = fixture["scorecard_attribution"]

    assert attribution["manifest"]["status"] == "PASS"
    assert attribution["manifest"]["variant_count"] > 0
    assert attribution["rejected_variant_component_matrix"]
    assert attribution["family_component_weakness"]["families"]
    assert attribution["manifest"]["broker_action_allowed"] is False

    validation = weight_search.validate_scorecard_attribution_artifact(
        scorecard_attribution_id=attribution["scorecard_attribution_id"],
        output_dir=tmp_path / "scorecard_attribution",
    )
    assert validation["status"] == "PASS"
