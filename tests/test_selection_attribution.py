from __future__ import annotations

from datetime import UTC, datetime

from dynamic_v3_system_target_helpers import run_selection_review_fixture

from ai_trading_system.etf_portfolio import dynamic_v3_system_target as system_target


def test_selection_attribution_explains_recommendation_and_review_required(tmp_path) -> None:
    fixture = run_selection_review_fixture(tmp_path)
    selection = fixture["selection"]

    attribution = system_target.run_selection_attribution(
        selection_review_id=selection["selection_review_id"],
        selection_review_dir=tmp_path / "system_target_selection_review",
        output_dir=tmp_path / "selection_attribution",
        generated_at=datetime(2026, 1, 7, 5, tzinfo=UTC),
    )

    rows = attribution["method_score_attribution"]
    recommendation = attribution["recommendation_reason_breakdown"]
    review = attribution["review_required_reason_breakdown"]
    recommended = recommendation["recommended_research_method"]
    recommended_row = next(row for row in rows if row["target_method"] == recommended)

    assert attribution["manifest"]["status"] == "PASS"
    assert recommended == selection["selection_decision"]["recommended_research_method"]
    assert recommended_row["selection_status"] == "recommended_research_method"
    assert "data_quality_penalty" in recommended_row["score_components"]
    assert review["decision_status"] == selection["selection_decision"]["decision_status"]
    assert review["can_trigger_official_target_weights"] is False
    assert review["can_trigger_production"] is False
    assert all(row["broker_action_allowed"] is False for row in rows)
    assert attribution["manifest"]["input_snapshot_schema"] == (
        "selection_attribution_input_snapshot.v2"
    )

    validation = system_target.validate_selection_attribution_artifact(
        attribution_id=attribution["attribution_id"],
        output_dir=tmp_path / "selection_attribution",
    )
    assert validation["status"] == "PASS"

    report_path = attribution["attribution_dir"] / "selection_attribution_report.md"
    report_path.write_text("tampered\n", encoding="utf-8")
    assert system_target.validate_selection_attribution_artifact(
        attribution_id=attribution["attribution_id"],
        output_dir=tmp_path / "selection_attribution",
    )["status"] == "FAIL"
