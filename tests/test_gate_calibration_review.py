from __future__ import annotations

from dynamic_v3_weight_batch_search_helpers import run_gate_calibration_review_fixture

from ai_trading_system.etf_portfolio import dynamic_v3_weight_batch_search as weight_search


def test_gate_calibration_review_is_diagnostic_only(tmp_path) -> None:
    fixture = run_gate_calibration_review_fixture(tmp_path)
    review = fixture["gate_calibration"]

    assert review["manifest"]["status"] == "PASS"
    assert review["manifest"]["official_gate_changed"] is False
    assert review["gate_strictness_diagnosis"]["can_change_official_gate"] is False
    assert review["manifest"]["broker_action_allowed"] is False
    assert "Gate Calibration Review" in review["reader_brief_section"]

    validation = weight_search.validate_gate_calibration_review_artifact(
        gate_calibration_id=review["gate_calibration_id"],
        output_dir=tmp_path / "gate_calibration_review",
    )
    assert validation["status"] == "PASS"
