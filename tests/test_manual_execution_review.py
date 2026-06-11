from __future__ import annotations

from pathlib import Path

from manual_portfolio_guardrail_helpers import (
    consensus_candidate_weights,
    manual_review_pack_fixture,
    report_index_for_manual_review,
)

from ai_trading_system.etf_portfolio.dynamic_v3_parameter_research import (
    validate_manual_execution_review_artifact,
)
from ai_trading_system.reports import reader_brief


def test_manual_execution_review_pack_generation_is_review_only(tmp_path: Path) -> None:
    fixture = manual_review_pack_fixture(
        tmp_path,
        candidate_weights=consensus_candidate_weights(),
    )
    review = fixture["review"]
    decision = review["manual_execution_decision"]
    validation = validate_manual_execution_review_artifact(
        review_id=review["manual_review_id"],
        output_dir=tmp_path / "manual_execution_review",
    )

    assert validation["status"] == "PASS"
    assert decision["recommended_action"] == "paper_adjustment_review_only"
    assert decision["order_ticket_generated"] is False
    assert decision["broker_action_allowed"] is False
    assert decision["broker_action_taken"] is False
    assert decision["owner_approval_required"] is True
    assert decision["production_effect"] == "none"


def test_reader_brief_summarizes_manual_execution_review(tmp_path: Path) -> None:
    fixture = manual_review_pack_fixture(
        tmp_path,
        candidate_weights=consensus_candidate_weights(),
    )
    report_index = report_index_for_manual_review(fixture)

    summary = reader_brief._etf_dynamic_v3_manual_execution_review_summary(
        report_index
    )

    assert summary["availability"] == "AVAILABLE"
    assert summary["manual_review_id"] == fixture["review"]["manual_review_id"]
    assert summary["snapshot_status"] == "PASS"
    assert summary["exposure_status"] == "PASS"
    assert summary["drift_status"] == "HIGH"
    assert summary["broker_action_allowed"] is False
    assert summary["order_ticket_generated"] is False
    assert summary["owner_approval_required"] is True
    assert summary["production_effect"] == "none"
    assert summary["safety_status"] != "SAFETY_REVIEW_REQUIRED"
