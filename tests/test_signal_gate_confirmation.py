from __future__ import annotations

from pathlib import Path

from dynamic_v3_filtered_candidate_readiness_helpers import (
    assert_research_safe,
    run_signal_gate_confirmation_fixture,
)

from ai_trading_system.etf_portfolio import dynamic_v3_filtered_candidate_readiness as readiness


def test_signal_gate_confirmation_builds_and_validates(tmp_path: Path) -> None:
    fixture = run_signal_gate_confirmation_fixture(tmp_path)
    confirmation = fixture["signal_gate_confirmation"]
    validation = readiness.validate_signal_gate_confirmation_artifact(
        confirmation_id=confirmation["confirmation_id"],
        output_dir=tmp_path / "signal_gate_confirmation",
    )
    assert validation["status"] == "PASS"
    targets = confirmation["signal_gate_confirmation_targets"]
    assert targets["auto_apply"] is False
    assert len(targets["targets"]) == 3
    assert {target["target_id"] for target in targets["targets"]} >= {
        "median_regime_filter_vs_median",
        "drawdown_mismatch_reduction_forward",
    }
    assert_research_safe(confirmation["manifest"])
