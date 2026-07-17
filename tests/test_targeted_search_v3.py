from __future__ import annotations

import yaml
from dynamic_v3_weight_batch_search_helpers import (
    _RESEARCH_FOUNDATION_COMPLETE_PREFIX_VARIANTS,
    run_targeted_search_v3_fixture,
    write_compact_weight_diagnostics_policy,
    write_compact_weight_targeted_policy,
    write_weight_search_space_config,
)

from ai_trading_system.etf_portfolio import dynamic_v3_weight_batch_search as weight_search
from ai_trading_system.etf_portfolio import dynamic_v3_weight_search_foundation as foundation
from ai_trading_system.etf_portfolio import dynamic_v3_weight_search_targeted as targeted


def test_compact_targeted_fixture_preserves_default_bounds_and_complete_families(
    tmp_path,
) -> None:
    compact_search_path = write_weight_search_space_config(
        tmp_path,
        source_backfill_id="test-backfill",
        initial_batch_variants=_RESEARCH_FOUNDATION_COMPLETE_PREFIX_VARIANTS,
    )
    compact_diagnostics_path = write_compact_weight_diagnostics_policy(tmp_path)
    compact_targeted_path = write_compact_weight_targeted_policy(tmp_path)

    default_search = foundation.load_weight_search_space_config(
        foundation.DEFAULT_WEIGHT_SEARCH_SPACE_CONFIG_PATH
    )
    compact_search = foundation.load_weight_search_space_config(compact_search_path)
    default_targeted = targeted._policy(targeted.DEFAULT_WEIGHT_SEARCH_TARGETED_POLICY_PATH)
    compact_targeted = targeted._policy(compact_targeted_path)

    assert default_search["max_variants"]["initial_batch"] == 80
    assert compact_search["max_variants"]["initial_batch"] == 52
    assert default_targeted["matrix"]["minimum_variants"] == 60
    assert default_targeted["matrix"]["maximum_variants"] == 120
    assert compact_targeted["matrix"]["minimum_variants"] == 6
    assert compact_targeted["matrix"]["maximum_variants"] == 12
    assert foundation.validate_weight_search_space_config(compact_search_path)["status"] == "PASS"

    base_variants = foundation._generate_batch2_variants(compact_search, expanded=False)[:52]
    base_coverage = foundation._batch2_family_coverage(base_variants)
    assert len(base_variants) == 52
    assert set(base_coverage["families_covered"]) == set(
        foundation.SEARCH_REQUIRED_FAMILIES
    )
    assert len(base_coverage["families_covered"]) == 8

    diagnostics_policy = yaml.safe_load(compact_diagnostics_path.read_text(encoding="utf-8"))
    compact_ranges = {
        row["parameter"]: row["recommended_values"]
        for row in diagnostics_policy["coverage"]["parameter_gap_templates"]
    }
    targeted_variants = targeted._variant_specs(
        {"targeted_v3_recommendations": {"new_parameter_ranges": compact_ranges}},
        {"near_miss_candidates": [{"variant_id": "cash_buffer_10"}]},
        compact_targeted,
    )[: compact_targeted["matrix"]["maximum_variants"]]
    targeted_coverage = targeted._family_coverage(targeted_variants, compact_targeted)
    assert 6 <= len(targeted_variants) <= 12
    assert set(targeted_coverage["targeted_families_covered"]) == set(
        compact_targeted["matrix"]["required_targeted_families"]
    )
    assert len(targeted_coverage["targeted_families_covered"]) == 6


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
