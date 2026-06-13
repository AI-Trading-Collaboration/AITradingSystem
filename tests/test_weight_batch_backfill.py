from __future__ import annotations

from dynamic_v3_weight_batch_search_helpers import run_weight_batch_backfill_fixture

from ai_trading_system.etf_portfolio import dynamic_v3_weight_batch_search as weight_search


def test_weight_batch_backfill_runs_quality_gated_variant_paths(tmp_path) -> None:
    fixture = run_weight_batch_backfill_fixture(tmp_path)
    backfill = fixture["weight_backfill"]
    manifest = backfill["manifest"]

    assert manifest["status"] == "PASS"
    assert manifest["market_regime"] == "ai_after_chatgpt"
    assert manifest["data_quality_status"] in {"PASS", "PASS_WITH_WARNINGS"}
    assert manifest["latest_valid_as_of"] >= manifest["date_end"]
    assert manifest["variants_completed"] == manifest["variants_total"]
    assert backfill["variant_churn_metrics"]
    assert backfill["variant_lag_metrics"]
    assert manifest["broker_action_allowed"] is False

    validation = weight_search.validate_weight_batch_backfill_artifact(
        backfill_id=backfill["batch_backfill_id"],
        output_dir=tmp_path / "weight_batch_backfill",
    )
    assert validation["status"] == "PASS"
