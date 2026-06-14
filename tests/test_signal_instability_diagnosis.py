from __future__ import annotations

from dynamic_v3_weight_batch_search_helpers import run_signal_instability_diagnosis_fixture

from ai_trading_system.etf_portfolio import dynamic_v3_weight_batch_search as weight_search


def test_signal_instability_diagnosis_exposes_signal_fix_flag(tmp_path) -> None:
    fixture = run_signal_instability_diagnosis_fixture(tmp_path)
    diagnosis = fixture["signal_diagnosis"]
    summary = diagnosis["signal_instability_summary"]

    assert diagnosis["manifest"]["status"] == "PASS"
    assert diagnosis["method_signal_stability"]
    assert summary["requires_signal_level_fix"] in {True, False}
    assert diagnosis["manifest"]["broker_action_allowed"] is False
    assert "Signal Instability Diagnosis" in diagnosis["reader_brief_section"]

    validation = weight_search.validate_signal_instability_diagnosis_artifact(
        signal_diagnosis_id=diagnosis["signal_diagnosis_id"],
        output_dir=tmp_path / "signal_instability_diagnosis",
    )
    assert validation["status"] == "PASS"
