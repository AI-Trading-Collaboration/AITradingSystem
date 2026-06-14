from __future__ import annotations

from dynamic_v3_weight_batch_search_helpers import run_micro_search_v4_backfill_fixture

from ai_trading_system.etf_portfolio import dynamic_v3_weight_batch_search as weight_search


def test_micro_search_v4_backfill_runs_data_quality_gate(tmp_path) -> None:
    fixture = run_micro_search_v4_backfill_fixture(tmp_path)
    backfill = fixture["v4_backfill"]

    assert backfill["manifest"]["status"] in {"PASS", "PASS_WITH_WARNINGS"}
    assert backfill["manifest"]["data_quality_status"] in {"PASS", "PASS_WITH_WARNINGS"}
    assert backfill["v4_backfill_progress"]["variants_completed"] > 0
    assert backfill["v4_variant_signal_metrics"]
    assert backfill["manifest"]["broker_action_allowed"] is False

    validation = weight_search.validate_micro_search_v4_backfill_artifact(
        v4_backfill_id=backfill["v4_backfill_id"],
        output_dir=tmp_path / "micro_search_v4_backfill",
    )
    assert validation["status"] == "PASS"
