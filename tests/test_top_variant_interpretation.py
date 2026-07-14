from __future__ import annotations

from dynamic_v3_system_target_helpers import run_top_variant_interpretation_fixture

from ai_trading_system.etf_portfolio import dynamic_v3_system_target as system_target


def test_top_variant_interpretation_explains_promoted_variant_and_coverage(tmp_path) -> None:
    fixture = run_top_variant_interpretation_fixture(tmp_path)
    interpretation = fixture["interpretation"]
    manifest = interpretation["manifest"]
    explanations = interpretation["top_variant_explanations"]
    coverage = interpretation["variant_failure_mode_coverage"]
    best = explanations[0]
    best_id = fixture["triage"]["variant_scorecard"][0]["variant_id"]

    assert manifest["status"] == "PASS"
    assert manifest["recommended_variant"] == best_id
    assert best["variant_id"] == best_id
    assert best["recommended_promotion"] is False
    assert best["what_it_changes"]
    assert best["why_it_helped"] == best["observed_screening_evidence"]
    assert best["what_it_costs"] == best["observed_screening_costs"]
    assert best["expected_benefit_hypothesis"]
    assert best["expected_cost_hypothesis"]
    assert coverage["failure_modes"]
    assert {row["coverage_status"] for row in coverage["failure_modes"]}.issubset(
        {"OBSERVED_SUPPORT", "HYPOTHESIS_COVERAGE_ONLY", "MISSING"}
    )
    assert (
        "Dynamic Rescue Weight Experiment Top Variant Interpretation"
        in (interpretation["reader_brief_section"])
    )
    assert manifest["broker_action_allowed"] is False
    assert manifest["production_effect"] == "none"

    validation = system_target.validate_top_variant_interpretation_artifact(
        interpretation_id=interpretation["interpretation_id"],
        output_dir=tmp_path / "top_variant_interpretation",
    )
    assert validation["status"] == "PASS"
