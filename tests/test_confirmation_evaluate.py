from __future__ import annotations

from pathlib import Path

from dynamic_v3_confirmation_cycle_helpers import evaluation_fixture

from ai_trading_system.etf_portfolio.dynamic_v3_confirmation_cycle import (
    confirmation_evaluation_report_payload,
    validate_confirmation_evaluation_artifact,
)


def test_confirmation_evaluate_not_ready_even_when_some_metrics_pass(tmp_path: Path) -> None:
    fixture = evaluation_fixture(tmp_path)
    evaluation = fixture["evaluation"]
    rows = {row["target_id"]: row for row in evaluation["target_evaluations"]}

    assert evaluation["confirmation_evaluation_summary"]["not_ready_count"] == 1
    assert evaluation["confirmation_evaluation_summary"]["success_count"] == 0
    assert evaluation["confirmation_evaluation_summary"]["failure_count"] == 0

    limited = rows["limited_adjustment_vs_no_trade"]
    assert limited["evaluation_status"] == "NOT_READY"
    assert limited["criteria_results"]["win_rate_vs_no_trade_min"]["status"] == (
        "INSUFFICIENT_DATA"
    )
    assert limited["criteria_results"]["avg_relative_return_min"]["status"] == ("INSUFFICIENT_DATA")
    assert all(
        row["status"] == "INSUFFICIENT_DATA"
        for row in limited["criteria_results"].values()
    )
    assert limited["recommendation"] == "continue_tracking"

    payload = confirmation_evaluation_report_payload(
        evaluation_id=evaluation["evaluation_id"],
        output_dir=fixture["evaluation_dir"],
    )
    assert payload["evaluation_id"] == evaluation["evaluation_id"]

    validation = validate_confirmation_evaluation_artifact(
        evaluation_id=evaluation["evaluation_id"],
        output_dir=fixture["evaluation_dir"],
    )
    assert validation["status"] == "PASS"
