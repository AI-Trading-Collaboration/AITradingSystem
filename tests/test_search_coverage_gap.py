from __future__ import annotations

from dynamic_v3_weight_batch_search_helpers import run_search_coverage_gap_fixture

from ai_trading_system.etf_portfolio import dynamic_v3_weight_batch_search as weight_search


def test_search_coverage_gap_bounds_targeted_v3_recommendations(tmp_path) -> None:
    fixture = run_search_coverage_gap_fixture(tmp_path, compact_test_matrix=True)
    coverage_gap = fixture["coverage_gap"]
    recommendations = coverage_gap["targeted_v3_recommendations"]

    assert coverage_gap["manifest"]["status"] == "PASS"
    assert "cash_buffer_smoothing_hybrid" in recommendations["recommended_focus"]
    assert recommendations["max_v3_variants"] <= weight_search.TARGETED_V3_MAX_VARIANTS
    assert coverage_gap["manifest"]["source_backfill_id"]

    validation = weight_search.validate_search_coverage_gap_artifact(
        coverage_gap_id=coverage_gap["coverage_gap_id"],
        output_dir=tmp_path / "search_coverage_gap",
    )
    assert validation["status"] == "PASS"
