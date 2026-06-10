from __future__ import annotations

from pathlib import Path

from dynamic_v3_pressure_validation_helpers import run_defensive_rule_review_fixture

from ai_trading_system.etf_portfolio.dynamic_v3_pressure_validation import (
    validate_defensive_rule_review_artifact,
)


def test_defensive_rule_review_keeps_rule_research_only_without_forward_evidence(
    tmp_path: Path,
) -> None:
    fixture = run_defensive_rule_review_fixture(tmp_path)
    result = fixture["defensive_rule_review"]
    matrix = result["defensive_rule_decision_matrix"]
    checklist = result["defensive_rule_owner_checklist"]

    assert matrix["recommended_status"] == "RESEARCH_ONLY"
    assert matrix["rule_approval_allowed"] is False
    assert matrix["auto_apply"] is False
    assert matrix["policy_change_allowed"] is False
    assert "active_limited_adjustment" in checklist
    assert "forward pressure samples" in checklist
    assert "no broker" in checklist
    assert (
        validate_defensive_rule_review_artifact(
            review_id=result["review_id"],
            output_dir=fixture["defensive_rule_review_dir"],
        )["status"]
        == "PASS"
    )
