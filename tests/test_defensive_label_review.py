from __future__ import annotations

from dynamic_v3_defensive_evidence_helpers import run_label_review_fixture

from ai_trading_system.etf_portfolio.dynamic_v3_defensive_evidence import (
    validate_defensive_label_review_artifact,
)


def test_defensive_label_review_keeps_label_change_owner_gated(tmp_path):
    fixture = run_label_review_fixture(tmp_path)
    review = fixture["defensive_label_review"]
    matrix = review["label_decision_matrix"]

    assert matrix["label_status"] == "POTENTIALLY_MISLEADING"
    assert matrix["recommended_label"] == "risk_aware_limited_adjustment"
    assert matrix["auto_rename"] is False
    assert matrix["config_change_allowed"] is False
    assert matrix["policy_change_allowed"] is False
    assert matrix["broker_action_allowed"] is False
    assert matrix["production_effect"] == "none"

    validation = validate_defensive_label_review_artifact(
        label_review_id=review["label_review_id"],
        output_dir=fixture["defensive_label_review_dir"],
    )

    assert validation["status"] == "PASS"
    assert validation["failed_check_count"] == 0
