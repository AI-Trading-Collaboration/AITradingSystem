from __future__ import annotations

from dynamic_v3_weight_batch_search_helpers import (
    _RESEARCH_FOUNDATION_COMPLETE_PREFIX_VARIANTS,
    run_weight_search_space_fixture,
    write_weight_search_space_config,
)

from ai_trading_system.etf_portfolio import dynamic_v3_weight_batch_search as weight_search
from ai_trading_system.etf_portfolio import dynamic_v3_weight_search_foundation as foundation


def test_research_foundation_prefix_preserves_default_and_required_families(tmp_path) -> None:
    (tmp_path / "default").mkdir()
    (tmp_path / "compact").mkdir()
    default_path = write_weight_search_space_config(
        tmp_path / "default",
        source_backfill_id="test-backfill",
    )
    compact_path = write_weight_search_space_config(
        tmp_path / "compact",
        source_backfill_id="test-backfill",
        initial_batch_variants=_RESEARCH_FOUNDATION_COMPLETE_PREFIX_VARIANTS,
    )
    default_config = foundation.load_weight_search_space_config(default_path)
    compact_config = foundation.load_weight_search_space_config(compact_path)

    assert default_config["max_variants"]["initial_batch"] == 80
    assert compact_config["max_variants"]["initial_batch"] == 52
    assert foundation.validate_weight_search_space_config(compact_path)["status"] == "PASS"

    variants = foundation._generate_batch2_variants(compact_config, expanded=False)[:52]
    coverage = foundation._batch2_family_coverage(variants)
    assert len(variants) == 52
    assert len(coverage["families_covered"]) == 8


def test_weight_search_space_config_and_artifact_are_auditable(tmp_path) -> None:
    fixture = run_weight_search_space_fixture(tmp_path)
    search_space = fixture["search_space"]

    assert search_space["manifest"]["status"] == "PASS"
    assert search_space["manifest"]["market_regime"] == "ai_after_chatgpt"
    assert len(search_space["manifest"]["families"]) >= 8
    assert search_space["manifest"]["broker_action_allowed"] is False
    assert search_space["manifest"]["production_effect"] == "none"

    validation = weight_search.validate_weight_search_space_artifact(
        search_space_id=search_space["search_space_id"],
        output_dir=tmp_path / "weight_search_space",
    )
    assert validation["status"] == "PASS"
