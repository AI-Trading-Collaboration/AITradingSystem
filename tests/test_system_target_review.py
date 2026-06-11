from __future__ import annotations

from dynamic_v3_system_target_helpers import (
    report_index_for_review_fixture,
    run_review_fixture,
)

from ai_trading_system.etf_portfolio import dynamic_v3_system_target as system_target
from ai_trading_system.reports import reader_brief


def test_system_target_review_pack_and_reader_brief_summary(tmp_path) -> None:
    fixture = run_review_fixture(tmp_path)
    review = fixture["review"]
    decision = review["system_target_decision"]

    assert decision["decision_status"] == "CONTINUE_OBSERVATION"
    assert decision["recommended_research_method"] == "limited_adjustment"
    assert decision["broker_action_allowed"] is False
    assert decision["broker_action_taken"] is False
    assert decision["not_official_target_weights"] is True

    validation = system_target.validate_system_target_review_artifact(
        review_id=review["review_id"],
        output_dir=tmp_path / "system_target_review",
    )
    assert validation["status"] == "PASS"

    summary = reader_brief._etf_dynamic_v3_system_target_summary(
        report_index_for_review_fixture(fixture)
    )
    assert summary["availability"] == "AVAILABLE"
    assert summary["review_id"] == review["review_id"]
    assert summary["recommended_research_method"] == "limited_adjustment"
    assert summary["data_quality_status"] in {"PASS", "PASS_WITH_WARNINGS"}
    assert summary["broker_action_allowed"] is False
    assert summary["not_official_target_weights"] is True
    assert summary["safety_status"].startswith("research_target_only=true")
