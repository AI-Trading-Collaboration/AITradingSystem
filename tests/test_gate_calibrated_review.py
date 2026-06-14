from __future__ import annotations

from dynamic_v3_weight_batch_search_helpers import run_gate_calibrated_review_fixture

from ai_trading_system.etf_portfolio import dynamic_v3_weight_batch_search as weight_search


def test_gate_calibrated_review_keeps_gate_policy_unchanged(tmp_path) -> None:
    fixture = run_gate_calibrated_review_fixture(tmp_path)
    review = fixture["gate_review"]

    assert review["manifest"]["status"] == "PASS"
    assert review["official_gate_results"]
    assert review["diagnostic_gate_results"]
    assert review["gate_calibrated_summary"]["gate_policy_change_recommended"] is False
    assert review["manifest"]["broker_action_allowed"] is False

    validation = weight_search.validate_gate_calibrated_review_artifact(
        gate_review_id=review["gate_review_id"],
        output_dir=tmp_path / "gate_calibrated_review",
    )
    assert validation["status"] == "PASS"
