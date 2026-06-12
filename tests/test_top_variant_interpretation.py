from __future__ import annotations

from dynamic_v3_system_target_helpers import run_top_variant_interpretation_fixture

from ai_trading_system.etf_portfolio import dynamic_v3_system_target as system_target


def test_top_variant_interpretation_explains_promoted_variant_and_coverage(tmp_path) -> None:
    fixture = run_top_variant_interpretation_fixture(tmp_path)
    interpretation = fixture["interpretation"]
    manifest = interpretation["manifest"]
    explanations = interpretation["top_variant_explanations"]
    coverage = interpretation["variant_failure_mode_coverage"]
    promoted = next(row for row in explanations if row["recommended_promotion"] is True)

    assert manifest["status"] == "PASS"
    assert manifest["recommended_variant"] == "sideways_choppy_hold_previous"
    assert promoted["variant_id"] == "sideways_choppy_hold_previous"
    assert promoted["what_it_changes"]
    assert promoted["why_it_helped"]
    assert promoted["what_it_costs"]
    assert coverage["failure_modes"]
    assert "Dynamic Rescue Weight Experiment Top Variant Interpretation" in (
        interpretation["reader_brief_section"]
    )
    assert manifest["broker_action_allowed"] is False
    assert manifest["production_effect"] == "none"

    validation = system_target.validate_top_variant_interpretation_artifact(
        interpretation_id=interpretation["interpretation_id"],
        output_dir=tmp_path / "top_variant_interpretation",
    )
    assert validation["status"] == "PASS"
