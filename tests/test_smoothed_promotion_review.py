from __future__ import annotations

import json

from dynamic_v3_system_target_helpers import run_smoothed_promotion_chain_fixture

from ai_trading_system.etf_portfolio import dynamic_v3_system_target as system_target
from ai_trading_system.etf_portfolio import (
    dynamic_v3_system_target_smoothed_promotion as smoothed_promotion,
)


@smoothed_promotion._with_validation_session
def test_smoothed_promotion_review_pack_summarizes_evidence_and_blockers(tmp_path) -> None:
    fixture = run_smoothed_promotion_chain_fixture(tmp_path)
    review = fixture["promotion_review"]

    evidence = review["promotion_evidence_summary"]
    assert evidence["candidate_method"] is None
    assert evidence["secondary_method"] is None
    assert evidence["readiness_decision"] == "CONTINUE_OBSERVATION"
    assert evidence["decision_confidence"] == "LOW"
    assert evidence["supporting_evidence"] == []

    blocking = review["promotion_blocking_issues"]
    issues = {row["issue"]: row for row in blocking["blocking_issues"]}
    assert blocking["can_enter_owner_review"] is False
    assert blocking["can_become_paper_shadow_primary_candidate"] == "NOT_ELIGIBLE"
    assert blocking["can_write_official_target_weights"] is False
    assert blocking["can_trigger_production"] is False
    assert issues["no_eligible_candidate"]["blocks_paper_shadow_primary_candidate"] is True
    assert issues["forward_target_not_registered"]["blocks_official_promotion"] is True
    assert review["manifest"]["broker_action_allowed"] is False
    assert "Dynamic Rescue Smoothed Promotion Review" in review["reader_brief_section"]

    validation = system_target.validate_smoothed_promotion_review_artifact(
        promotion_review_id=review["promotion_review_id"],
        output_dir=tmp_path / "smoothed_promotion_review",
    )
    assert validation["status"] == "PASS"

    evidence_path = review["promotion_review_dir"] / "promotion_evidence_summary.json"
    original_evidence = evidence_path.read_bytes()
    tampered = json.loads(original_evidence)
    tampered["candidate_method"] = "smooth_weights_3d_limited_adjustment"
    evidence_path.write_text(json.dumps(tampered), encoding="utf-8")
    assert (
        system_target.validate_smoothed_promotion_review_artifact(
            promotion_review_id=review["promotion_review_id"],
            output_dir=tmp_path / "smoothed_promotion_review",
        )["status"]
        == "FAIL"
    )
    evidence_path.write_bytes(original_evidence)

    source_path = fixture["scorecard"]["scorecard_dir"] / "promotion_readiness_decision.json"
    original_source = source_path.read_bytes()
    source = json.loads(original_source)
    source["decision"] = "PROMOTE_FOR_REVIEW"
    source_path.write_text(json.dumps(source), encoding="utf-8")
    assert (
        system_target.validate_smoothed_promotion_review_artifact(
            promotion_review_id=review["promotion_review_id"],
            output_dir=tmp_path / "smoothed_promotion_review",
        )["status"]
        == "FAIL"
    )
    source_path.write_bytes(original_source)
    assert (
        system_target.validate_smoothed_promotion_review_artifact(
            promotion_review_id=review["promotion_review_id"],
            output_dir=tmp_path / "smoothed_promotion_review",
        )["status"]
        == "PASS"
    )
