from __future__ import annotations

from dynamic_v3_defensive_evidence_helpers import run_defensive_deep_dive_fixture

from ai_trading_system.etf_portfolio.dynamic_v3_defensive_evidence import (
    validate_defensive_hypothesis_deep_dive_artifact,
)


def test_defensive_hypothesis_deep_dive_splits_supporting_and_contradicting_cases(
    tmp_path,
):
    fixture = run_defensive_deep_dive_fixture(tmp_path)
    deep_dive = fixture["defensive_hypothesis_deep_dive"]
    manifest = deep_dive["manifest"]

    assert manifest["supporting_case_count"] > 0
    assert manifest["contradicting_case_count"] > 0
    assert manifest["source_mode_counts"]["BACKTEST_SIMULATION"] > 0
    assert manifest["can_support_rule_approval"] is False
    assert manifest["production_effect"] == "none"
    assert all(row["can_support_rule_approval"] is False for row in deep_dive["supporting_cases"])
    assert all(
        row["can_support_rule_approval"] is False
        for row in deep_dive["contradicting_cases"]
    )

    validation = validate_defensive_hypothesis_deep_dive_artifact(
        deep_dive_id=deep_dive["deep_dive_id"],
        output_dir=fixture["defensive_hypothesis_deep_dive_dir"],
    )

    assert validation["status"] == "PASS"
    assert validation["failed_check_count"] == 0
