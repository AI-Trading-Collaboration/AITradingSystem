from __future__ import annotations

from dynamic_v3_weight_batch_search_helpers import run_cash_buffer_attribution_fixture

from ai_trading_system.etf_portfolio import dynamic_v3_weight_batch_search as weight_search


def test_cash_buffer_attribution_records_tradeoff_and_recommendations(tmp_path) -> None:
    fixture = run_cash_buffer_attribution_fixture(tmp_path)
    attribution = fixture["cash_buffer_attribution"]

    assert attribution["manifest"]["status"] == "PASS"
    assert attribution["cash_buffer_effect_summary"]["variant_id"] == "cash_buffer_10"
    assert attribution["cash_buffer_failure_reason"]["primary_failure_reason"]
    assert attribution["cash_buffer_variant_recommendations"]["recommended_variants"]

    validation = weight_search.validate_cash_buffer_attribution_artifact(
        attribution_id=attribution["attribution_id"],
        output_dir=tmp_path / "cash_buffer_attribution",
    )
    assert validation["status"] == "PASS"
