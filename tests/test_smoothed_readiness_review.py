from __future__ import annotations

from dynamic_v3_system_target_helpers import run_smoothed_readiness_chain_fixture

from ai_trading_system.etf_portfolio import dynamic_v3_system_target as system_target
from ai_trading_system.reports import reader_brief


def test_smoothed_readiness_review_chain_outputs_all_artifacts(tmp_path) -> None:
    fixture = run_smoothed_readiness_chain_fixture(tmp_path)

    gap = fixture["gap"]
    gap_matrix = gap["missing_evidence_matrix"]
    gap_reasons = gap["evidence_gap_reason_summary"]
    assert gap_reasons["tradeoff_can_be_resolved_by_backfill"] is True
    assert gap_reasons["requires_new_target_method"] is False
    assert {
        row["evidence_type"] for row in gap_matrix["missing_evidence"]
    } >= {"weight_jump_reduction", "signal_churn_reduction"}
    gap_validation = system_target.validate_smoothed_evidence_gap_artifact(
        gap_id=gap["gap_id"],
        output_dir=tmp_path / "smoothed_evidence_gap",
    )
    assert gap_validation["status"] == "PASS"

    churn = fixture["churn"]
    metric_methods = {row["method"] for row in churn["churn_metrics_by_method"]}
    assert metric_methods >= {
        "smooth_weights_3d_limited_adjustment",
        "smooth_weights_5d_limited_adjustment",
        "limited_adjustment",
    }
    primary_churn = next(
        row
        for row in churn["churn_reduction_summary"]["methods"]
        if row["method"] == "smooth_weights_3d_limited_adjustment"
    )
    assert primary_churn["churn_reduction_status"] in {
        "STRONG",
        "MODERATE",
        "WEAK",
        "NONE",
        "INSUFFICIENT_DATA",
    }
    churn_validation = system_target.validate_smoothed_churn_backfill_artifact(
        churn_id=churn["churn_id"],
        output_dir=tmp_path / "smoothed_churn_backfill",
    )
    assert churn_validation["status"] == "PASS"

    sideways = fixture["sideways"]
    assert sideways["sideways_window_outcomes"]
    assert sideways["sideways_mixed_reason_summary"]["sideways_validation"] in {
        "MIXED",
        "IMPROVED",
        "WORSE",
        "INSUFFICIENT_DATA",
    }
    assert {
        row["method"] for row in sideways["sideways_3d_vs_5d_breakdown"]["methods"]
    } == set(system_target.SMOOTHED_METHOD_TO_VARIANT)
    sideways_validation = system_target.validate_sideways_mixed_attribution_artifact(
        sideways_attribution_id=sideways["sideways_attribution_id"],
        output_dir=tmp_path / "sideways_mixed_attribution",
    )
    assert sideways_validation["status"] == "PASS"

    scorecard = fixture["scorecard"]
    decision = scorecard["promotion_readiness_decision"]
    assert {row["method"] for row in scorecard["smoothed_method_scorecard"]["methods"]} == set(
        system_target.SMOOTHED_METHOD_TO_VARIANT
    )
    assert decision["decision"] in {
        "PROMOTE_FOR_REVIEW",
        "CONTINUE_OBSERVATION",
        "REVIEW_REQUIRED",
        "REJECT",
    }
    assert decision["auto_apply"] is False
    assert decision["broker_action_allowed"] is False
    scorecard_validation = system_target.validate_smoothed_readiness_scorecard_artifact(
        scorecard_id=scorecard["scorecard_id"],
        output_dir=tmp_path / "smoothed_readiness_scorecard",
    )
    assert scorecard_validation["status"] == "PASS"

    owner_update = fixture["owner_update"]
    options = owner_update["smoothed_owner_decision_options"]
    assert options["readiness_decision"] == decision["decision"]
    assert options["recommended_owner_action"] in {
        "review_for_manual_promotion_decision",
        "continue_observation",
        "continue_forward_observation",
        "reject_smoothed_promotion",
        "request_additional_evidence",
    }
    assert "Dynamic Rescue Smoothed Owner Review" in owner_update["reader_brief_section"]
    owner_validation = system_target.validate_smoothed_owner_review_update_artifact(
        owner_update_id=owner_update["owner_update_id"],
        output_dir=tmp_path / "smoothed_owner_review_update",
    )
    assert owner_validation["status"] == "PASS"

    report_index = {
        "reports": [
            {
                "report_id": "etf_dynamic_v3_smoothed_watch_pack",
                "latest_artifact_path": str(
                    fixture["watch"]["watch_pack_dir"] / "smoothed_watch_manifest.json"
                ),
            },
            {
                "report_id": "etf_dynamic_v3_smoothed_owner_review_update",
                "latest_artifact_path": str(
                    owner_update["owner_update_dir"]
                    / "smoothed_owner_update_manifest.json"
                ),
            },
        ]
    }
    summary = reader_brief._etf_dynamic_v3_system_target_summary(report_index)
    assert summary["smoothed_owner_update_id"] == owner_update["owner_update_id"]
    assert summary["smoothed_owner_readiness_decision"] == decision["decision"]
    assert summary["smoothed_owner_recommended_action"] == options["recommended_owner_action"]
    assert summary["production_effect"] == "none"
