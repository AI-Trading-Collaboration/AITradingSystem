from __future__ import annotations

from pathlib import Path

from dynamic_v3_filtered_candidate_readiness_helpers import (
    assert_research_safe,
    run_drawdown_mismatch_reduction_fixture,
)

from ai_trading_system.etf_portfolio import dynamic_v3_filtered_candidate_readiness as readiness


def test_drawdown_mismatch_reduction_builds_and_validates(tmp_path: Path, monkeypatch) -> None:
    fixture = run_drawdown_mismatch_reduction_fixture(tmp_path, monkeypatch)
    reduction = fixture["drawdown_mismatch_reduction"]
    validation = readiness.validate_drawdown_mismatch_reduction_artifact(
        reduction_id=reduction["reduction_id"],
        output_dir=tmp_path / "drawdown_mismatch_reduction",
    )
    assert validation["status"] == "PASS"
    summary = reduction["mismatch_reduction_summary"]
    assert summary["evidence_status"] == "INSUFFICIENT_DATA"
    assert summary["risk_increase_during_drawdown_before"] is None
    assert summary["risk_increase_during_drawdown_after"] is None
    assert summary["blocked_signal_helpful_rate"] is None
    assert summary["blocked_signal_harmful_rate"] is None
    assert summary["drawdown_mismatch_reduction_status"] == "INSUFFICIENT_DATA"
    assert reduction["mismatch_reduction_events"] == []
    assert_research_safe(reduction)
