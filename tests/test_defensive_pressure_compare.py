from __future__ import annotations

import json
from pathlib import Path

from dynamic_v3_pressure_validation_helpers import run_defensive_pressure_compare_fixture

from ai_trading_system.etf_portfolio.dynamic_v3_pressure_validation import (
    _source_mode_defensive_status,
    validate_defensive_pressure_compare_artifact,
)


def test_defensive_pressure_compare_blocks_approval_without_forward_samples(
    tmp_path: Path,
) -> None:
    fixture = run_defensive_pressure_compare_fixture(tmp_path)
    result = fixture["defensive_pressure_compare"]
    summary = result["defensive_pressure_summary"]
    pairwise = result["defensive_pairwise_comparison"]

    assert summary["defensive_status"] == "NOT_PROVEN_DEFENSIVE"
    assert summary["source_mode_breakdown"]["BACKTEST_SIMULATION"] == "INSUFFICIENT_DATA"
    assert summary["source_mode_breakdown"]["FORWARD_OUTCOME"] == "INSUFFICIENT_DATA"
    assert summary["can_support_rule_approval"] is False
    assert all(
        row["distinct_event_count"] < row["minimum_distinct_events_required"]
        for row in pairwise["comparisons"]
        if row["sample_count"] > 0
    )
    assert all(
        row["avg_return"] is None
        and row["avg_relative_to_no_trade"] is None
        and row["win_rate_vs_no_trade"] is None
        for row in result["pressure_variant_metrics"]
        if row["sample_count"] == 0
    )
    assert any(
        row["source_mode"] == "BACKTEST_SIMULATION"
        and row["sample_count"] > 0
        and row["conclusion"] == "insufficient_data"
        for row in pairwise["comparisons"]
    )
    assert (
        validate_defensive_pressure_compare_artifact(
            comparison_id=result["comparison_id"],
            output_dir=fixture["defensive_pressure_compare_dir"],
        )["status"]
        == "PASS"
    )

    summary_path = result["comparison_dir"] / "defensive_pressure_summary.json"
    tampered = json.loads(summary_path.read_text(encoding="utf-8"))
    tampered["defensive_status"] = "PROVEN_DEFENSIVE"
    summary_path.write_text(json.dumps(tampered, sort_keys=True), encoding="utf-8")
    assert (
        validate_defensive_pressure_compare_artifact(
            comparison_id=result["comparison_id"],
            output_dir=fixture["defensive_pressure_compare_dir"],
        )["status"]
        == "FAIL"
    )


def test_source_mode_requires_every_configured_pressure_regime() -> None:
    comparisons = [
        {
            "source_mode": "FORWARD_OUTCOME",
            "regime": regime,
            "evidence_status": "SUFFICIENT",
            "conclusion": "variant_a_better",
        }
        for regime in ("tech_drawdown", "semiconductor_pullback")
    ]

    assert (
        _source_mode_defensive_status(
            "FORWARD_OUTCOME",
            comparisons,
            required_regimes=("tech_drawdown", "risk_off", "semiconductor_pullback"),
        )
        == "INSUFFICIENT_DATA"
    )
