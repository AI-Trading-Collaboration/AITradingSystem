from __future__ import annotations

from dynamic_v3_system_target_helpers import run_smoothed_promotion_chain_fixture

from ai_trading_system.etf_portfolio import dynamic_v3_system_target as system_target


def test_smoothed_promotion_review_pack_summarizes_evidence_and_blockers(tmp_path) -> None:
    fixture = run_smoothed_promotion_chain_fixture(tmp_path)
    review = fixture["promotion_review"]

    evidence = review["promotion_evidence_summary"]
    assert evidence["candidate_method"] == "smooth_weights_3d_limited_adjustment"
    assert evidence["secondary_method"] == "smooth_weights_5d_limited_adjustment"
    assert evidence["readiness_decision"] == "PROMOTE_FOR_REVIEW"
    assert evidence["decision_confidence"] == "LOW"
    assert {
        row["evidence_id"] for row in evidence["supporting_evidence"]
    } >= {
        "churn_reduction_strong",
        "sideways_churn_reduction_helped",
        "recovery_lag_low",
    }

    blocking = review["promotion_blocking_issues"]
    issues = {row["issue"]: row for row in blocking["blocking_issues"]}
    assert blocking["can_enter_owner_review"] is True
    assert blocking["can_become_paper_shadow_primary_candidate"] == "OWNER_DECISION_REQUIRED"
    assert blocking["can_write_official_target_weights"] is False
    assert blocking["can_trigger_production"] is False
    assert issues["forward_confirmation_in_progress"]["blocks_official_promotion"] is True
    assert (
        issues["forward_confirmation_in_progress"][
            "blocks_paper_shadow_primary_candidate"
        ]
        is False
    )
    assert review["manifest"]["broker_action_allowed"] is False
    assert "Dynamic Rescue Smoothed Promotion Review" in review["reader_brief_section"]

    validation = system_target.validate_smoothed_promotion_review_artifact(
        promotion_review_id=review["promotion_review_id"],
        output_dir=tmp_path / "smoothed_promotion_review",
    )
    assert validation["status"] == "PASS"
