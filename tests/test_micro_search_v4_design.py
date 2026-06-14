from __future__ import annotations

from dynamic_v3_weight_batch_search_helpers import run_micro_search_v4_design_fixture

from ai_trading_system.etf_portfolio import dynamic_v3_weight_batch_search as weight_search


def test_micro_search_v4_design_stays_bounded_and_experimental(tmp_path) -> None:
    fixture = run_micro_search_v4_design_fixture(tmp_path)
    design = fixture["v4_design"]
    variant_ids = {row["variant_id"] for row in design["v4_variant_specs"]}

    assert design["manifest"]["status"] == "PASS"
    assert 20 <= design["manifest"]["variant_count"] <= 40
    assert "smooth_3d_plus_dispersion_gate" in variant_ids
    assert "median_consensus_plus_smooth_3d" in variant_ids
    assert design["manifest"]["broker_action_allowed"] is False

    validation = weight_search.validate_micro_search_v4_design_artifact(
        v4_design_id=design["v4_design_id"],
        output_dir=tmp_path / "micro_search_v4_design",
    )
    assert validation["status"] == "PASS"
