from __future__ import annotations

from pathlib import Path

from dynamic_v3_filtered_candidate_readiness_helpers import (
    assert_research_safe,
    run_drawdown_mismatch_reduction_fixture,
)

from ai_trading_system.etf_portfolio import dynamic_v3_filtered_candidate_readiness as readiness


def test_drawdown_mismatch_reduction_builds_and_validates(tmp_path: Path) -> None:
    fixture = run_drawdown_mismatch_reduction_fixture(tmp_path)
    reduction = fixture["drawdown_mismatch_reduction"]
    validation = readiness.validate_drawdown_mismatch_reduction_artifact(
        reduction_id=reduction["reduction_id"],
        output_dir=tmp_path / "drawdown_mismatch_reduction",
    )
    assert validation["status"] == "PASS"
    summary = reduction["mismatch_reduction_summary"]
    assert summary["risk_increase_during_drawdown_before"] >= summary[
        "risk_increase_during_drawdown_after"
    ]
    assert summary["drawdown_mismatch_reduction_status"] == "IMPROVED"
    assert summary["blocked_signal_helpful_rate"] > summary["blocked_signal_harmful_rate"]
    assert_research_safe(reduction["manifest"])
