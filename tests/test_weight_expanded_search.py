from __future__ import annotations

from datetime import UTC, datetime

from dynamic_v3_weight_batch_search_helpers import run_weight_adaptive_branch_fixture

from ai_trading_system.etf_portfolio import dynamic_v3_weight_batch_search as weight_search


def test_weight_expanded_search_can_build_and_backfill_followup_matrix(tmp_path) -> None:
    fixture = run_weight_adaptive_branch_fixture(tmp_path)
    expanded = weight_search.build_weight_expanded_search(
        branch_id=fixture["branch"]["branch_id"],
        branch_dir=tmp_path / "weight_adaptive_branch",
        search_space_dir=tmp_path / "weight_search_space",
        output_dir=tmp_path / "weight_expanded_search",
        generated_at=datetime(2024, 3, 7, tzinfo=UTC),
    )
    expanded_backfill = weight_search.run_weight_expanded_search(
        expanded_matrix_id=expanded["matrix_id"],
        expanded_matrix_dir=tmp_path / "weight_expanded_search",
        baseline_backfill_dir=tmp_path / "paper_shadow_backfill",
        output_dir=tmp_path / "expanded_weight_batch_backfill",
        price_cache_path=fixture["prices_path"],
        rates_cache_path=fixture["rates_path"],
        generated_at=datetime(2024, 3, 3, tzinfo=UTC),
    )

    assert expanded["manifest"]["expanded"] is True
    assert expanded["manifest"]["variant_count"] <= 200
    assert expanded_backfill["manifest"]["status"] == "PASS"
    assert expanded_backfill["manifest"]["data_quality_status"] in {"PASS", "PASS_WITH_WARNINGS"}

    validation = weight_search.validate_weight_experiment_batch2_artifact(
        matrix_id=expanded["matrix_id"],
        output_dir=tmp_path / "weight_expanded_search",
    )
    assert validation["status"] == "PASS"
