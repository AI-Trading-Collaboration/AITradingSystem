from __future__ import annotations

from dynamic_v3_weight_batch_search_helpers import run_promotion_threshold_sensitivity_fixture

from ai_trading_system.etf_portfolio import dynamic_v3_weight_batch_search as weight_search


def test_promotion_threshold_sensitivity_keeps_relaxed_scenarios_diagnostic(tmp_path) -> None:
    fixture = run_promotion_threshold_sensitivity_fixture(tmp_path)
    sensitivity = fixture["sensitivity"]
    relaxed = [
        row for row in sensitivity["threshold_scenarios"] if row["scenario"] != "base_threshold"
    ]

    assert sensitivity["manifest"]["status"] == "PASS"
    assert relaxed
    assert all(row["recommended"] is False for row in relaxed)
    assert (
        sensitivity["threshold_candidate_impact"]["policy_effect"]
        == "diagnostic_only_no_gate_change"
    )

    validation = weight_search.validate_promotion_threshold_sensitivity_artifact(
        sensitivity_id=sensitivity["sensitivity_id"],
        output_dir=tmp_path / "promotion_threshold_sensitivity",
    )
    assert validation["status"] == "PASS"
