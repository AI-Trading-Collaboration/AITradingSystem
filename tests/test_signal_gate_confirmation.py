from __future__ import annotations

from pathlib import Path

from dynamic_v3_filtered_candidate_readiness_helpers import (
    assert_research_safe,
    run_signal_gate_confirmation_fixture,
)

from ai_trading_system.etf_portfolio import dynamic_v3_filtered_candidate_readiness as readiness


def test_signal_gate_confirmation_builds_and_validates(tmp_path: Path, monkeypatch) -> None:
    fixture = run_signal_gate_confirmation_fixture(tmp_path, monkeypatch)
    confirmation = fixture["signal_gate_confirmation"]
    validation = readiness.validate_signal_gate_confirmation_artifact(
        confirmation_id=confirmation["confirmation_id"],
        output_dir=tmp_path / "signal_gate_confirmation",
    )
    assert validation["status"] == "PASS"
    targets = confirmation["signal_gate_confirmation_targets"]
    assert targets["auto_apply"] is False
    assert targets["evidence_status"] == "INSUFFICIENT_DATA"
    assert targets["targets"] == []
    assert targets["registered_target_count"] == 0
    assert targets["completed_observation_count"] == 0
    assert_research_safe(confirmation)
