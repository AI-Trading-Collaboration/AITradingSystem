from __future__ import annotations

from dynamic_v3_weight_batch_search_helpers import run_targeted_search_v3_fixture

from ai_trading_system.etf_portfolio import dynamic_v3_weight_batch_search as weight_search


def test_targeted_search_v3_builds_bounded_variant_matrix(tmp_path) -> None:
    fixture = run_targeted_search_v3_fixture(tmp_path)
    targeted_v3 = fixture["targeted_v3"]

    assert targeted_v3["manifest"]["status"] == "PASS"
    assert 60 <= targeted_v3["manifest"]["variant_count"] <= weight_search.TARGETED_V3_MAX_VARIANTS
    assert (
        "cash_buffer_smoothing_hybrid"
        in targeted_v3["v3_family_coverage"]["targeted_families_covered"]
    )
    assert (
        targeted_v3["manifest"]["cash_buffer_attribution_id"]
        == fixture["cash_buffer_attribution"]["attribution_id"]
    )

    validation = weight_search.validate_targeted_search_v3_artifact(
        v3_matrix_id=targeted_v3["v3_matrix_id"],
        output_dir=tmp_path / "targeted_search_v3",
    )
    assert validation["status"] == "PASS"
