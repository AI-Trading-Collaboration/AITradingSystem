from __future__ import annotations

from pathlib import Path

from dynamic_v3_weight_batch_search_helpers import run_targeted_v3_backfill_fixture

from ai_trading_system.etf_portfolio import dynamic_v3_weight_batch_search as weight_search


def test_targeted_v3_backfill_runs_data_gated_metrics(tmp_path) -> None:
    fixture = run_targeted_v3_backfill_fixture(tmp_path)
    backfill = fixture["targeted_v3_backfill"]

    assert backfill["manifest"]["status"] == "PASS"
    assert backfill["manifest"]["data_quality_status"] in {"PASS", "PASS_WITH_WARNINGS"}
    assert (
        backfill["manifest"]["variants_completed"]
        == fixture["targeted_v3"]["manifest"]["variant_count"]
    )
    assert Path(backfill["manifest"]["validate_data_quality_report_path"]).exists()

    validation = weight_search.validate_targeted_v3_backfill_artifact(
        v3_backfill_id=backfill["v3_backfill_id"],
        output_dir=tmp_path / "targeted_v3_backfill",
    )
    assert validation["status"] == "PASS"
