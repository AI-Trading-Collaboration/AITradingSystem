from __future__ import annotations

import pytest
from dynamic_v3_system_target_helpers import (
    report_index_for_review_fixture,
    run_backfill_fixture,
    run_review_fixture,
    run_selection_review_fixture,
)

from ai_trading_system.etf_portfolio import dynamic_v3_system_target as system_target
from ai_trading_system.reports import reader_brief


def test_system_target_selection_review_and_reader_brief_override(tmp_path) -> None:
    review_fixture = run_review_fixture(tmp_path / "review")
    selection_fixture = run_selection_review_fixture(tmp_path / "selection")
    selection = selection_fixture["selection"]
    decision = selection["selection_decision"]
    scorecard = selection["target_method_scorecard"]

    assert selection["manifest"]["status"] == "PASS"
    assert (
        selection["manifest"]["input_snapshot_schema"]
        == "system_target_selection_review_input_snapshot.v2"
    )
    assert selection["manifest"]["data_quality_status"] in {"PASS", "PASS_WITH_WARNINGS"}
    assert decision["recommended_research_method"] in system_target.TARGET_METHODS
    assert decision["decision_status"] in {"CONTINUE_OBSERVATION", "REVIEW_REQUIRED"}
    assert decision["recommended_research_method"] != "consensus_target"
    assert decision["not_official_target_weights"] is True
    assert decision["broker_action_allowed"] is False
    assert {row["target_method"] for row in scorecard["methods"]} == set(
        system_target.TARGET_METHODS
    )

    validation = system_target.validate_system_target_selection_review_artifact(
        selection_review_id=selection["selection_review_id"],
        output_dir=tmp_path / "selection" / "system_target_selection_review",
    )
    assert validation["status"] == "PASS"

    report_index = report_index_for_review_fixture(review_fixture)
    report_index["reports"].append(
        {
            "report_id": "etf_dynamic_v3_system_target_selection_review",
            "latest_artifact_path": str(
                selection["selection_review_dir"] / "system_target_selection_manifest.json"
            ),
        }
    )
    summary = reader_brief._etf_dynamic_v3_system_target_summary(report_index)

    assert summary["availability"] == "AVAILABLE"
    assert summary["selection_review_id"] == selection["selection_review_id"]
    assert summary["recommended_research_method"] == decision["recommended_research_method"]
    assert summary["decision_status"] == decision["decision_status"]
    assert summary["not_official_target_weights"] is True
    assert summary["broker_action_allowed"] is False
    assert "selection_review" in summary["selection_review_path"]


def test_system_target_selection_review_rejects_cross_backfill_lineage(tmp_path) -> None:
    selection_fixture = run_selection_review_fixture(tmp_path / "primary")
    other = run_backfill_fixture(tmp_path / "other")

    with pytest.raises(system_target.DynamicV3SystemTargetError, match="backfill"):
        system_target.run_system_target_selection_review(
            backfill_id=other["backfill"]["backfill_id"],
            rolling_eval_id=selection_fixture["rolling"]["rolling_eval_id"],
            regime_review_id=selection_fixture["regime"]["regime_review_id"],
            stability_id=selection_fixture["stability"]["stability_id"],
            backfill_dir=tmp_path / "other" / "paper_shadow_backfill",
            rolling_eval_dir=tmp_path / "primary" / "paper_shadow_rolling_eval",
            regime_review_dir=tmp_path / "primary" / "paper_shadow_regime_review",
            stability_dir=tmp_path / "primary" / "paper_shadow_stability",
            output_dir=tmp_path / "cross_lineage_selection",
        )
