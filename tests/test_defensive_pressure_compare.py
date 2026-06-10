from __future__ import annotations

from pathlib import Path

from dynamic_v3_pressure_validation_helpers import run_defensive_pressure_compare_fixture

from ai_trading_system.etf_portfolio.dynamic_v3_pressure_validation import (
    validate_defensive_pressure_compare_artifact,
)


def test_defensive_pressure_compare_blocks_approval_without_forward_samples(
    tmp_path: Path,
) -> None:
    fixture = run_defensive_pressure_compare_fixture(tmp_path)
    result = fixture["defensive_pressure_compare"]
    summary = result["defensive_pressure_summary"]
    pairwise = result["defensive_pairwise_comparison"]

    assert summary["defensive_status"] == "INSUFFICIENT_FORWARD_DATA"
    assert summary["source_mode_breakdown"]["BACKTEST_SIMULATION"] == "PROVEN_DEFENSIVE"
    assert summary["source_mode_breakdown"]["FORWARD_OUTCOME"] == "INSUFFICIENT_DATA"
    assert summary["can_support_rule_approval"] is False
    assert any(
        row["source_mode"] == "BACKTEST_SIMULATION"
        and row["sample_count"] > 0
        and row["conclusion"] == "variant_a_better"
        for row in pairwise["comparisons"]
    )
    assert (
        validate_defensive_pressure_compare_artifact(
            comparison_id=result["comparison_id"],
            output_dir=fixture["defensive_pressure_compare_dir"],
        )["status"]
        == "PASS"
    )
