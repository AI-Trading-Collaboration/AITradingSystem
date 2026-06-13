from __future__ import annotations

from datetime import UTC, datetime

from dynamic_v3_system_target_helpers import run_smoothed_promotion_chain_fixture

from ai_trading_system.etf_portfolio import dynamic_v3_system_target as system_target
from ai_trading_system.reports import reader_brief


def test_smoothed_owner_promotion_creation_recording_and_reader_brief(tmp_path) -> None:
    fixture = run_smoothed_promotion_chain_fixture(tmp_path)
    owner = fixture["owner_promotion"]

    decision = owner["owner_promotion_decision"]
    assert decision["owner_decision"] == "pending"
    assert decision["recommended_owner_action"] == "continue_observation"
    assert decision["paper_shadow_primary_candidate_change_allowed"] is False
    assert decision["paper_shadow_primary_candidate_change_requested"] is False
    assert decision["not_official_target_weights"] is True
    assert decision["broker_action_allowed"] is False
    assert decision["production_effect"] == "none"

    recorded = system_target.record_smoothed_owner_promotion_decision(
        decision_id=owner["decision_id"],
        decision="continue_observation",
        decision_reason="Forward confirmation remains in progress.",
        output_dir=tmp_path / "smoothed_owner_promotion",
        recorded_at=datetime(2024, 3, 12, tzinfo=UTC),
    )
    recorded_decision = recorded["owner_promotion_decision"]
    assert recorded_decision["owner_decision"] == "continue_observation"
    assert recorded_decision["recommended_owner_action"] == "continue_observation"
    assert recorded_decision["paper_shadow_primary_candidate_change_allowed"] is False
    assert recorded_decision["paper_shadow_primary_candidate_change_requested"] is False
    assert recorded_decision["actual_switch_executed"] is False
    assert "Dynamic Rescue Smoothed Promotion Decision" in recorded["reader_brief_section"]

    validation = system_target.validate_smoothed_owner_promotion_artifact(
        decision_id=owner["decision_id"],
        output_dir=tmp_path / "smoothed_owner_promotion",
    )
    assert validation["status"] == "PASS"

    report_index = {
        "reports": [
            {
                "report_id": "etf_dynamic_v3_smoothed_promotion_review",
                "latest_artifact_path": str(
                    fixture["promotion_review"]["promotion_review_dir"]
                    / "smoothed_promotion_review_manifest.json"
                ),
            },
            {
                "report_id": "etf_dynamic_v3_primary_research_candidate_gate",
                "latest_artifact_path": str(
                    fixture["gate"]["gate_dir"]
                    / "primary_research_candidate_gate_manifest.json"
                ),
            },
            {
                "report_id": "etf_dynamic_v3_smoothed_forward_binding",
                "latest_artifact_path": str(
                    fixture["binding"]["binding_dir"]
                    / "smoothed_forward_binding_manifest.json"
                ),
            },
            {
                "report_id": "etf_dynamic_v3_paper_shadow_primary_switch",
                "latest_artifact_path": str(
                    fixture["switch_plan"]["switch_plan_dir"]
                    / "paper_shadow_primary_switch_manifest.json"
                ),
            },
            {
                "report_id": "etf_dynamic_v3_smoothed_owner_promotion",
                "latest_artifact_path": str(
                    recorded["decision_dir"] / "smoothed_owner_promotion_manifest.json"
                ),
            },
        ]
    }
    summary = reader_brief._etf_dynamic_v3_system_target_summary(report_index)
    assert summary["smoothed_promotion_review_id"] == fixture["promotion_review"][
        "promotion_review_id"
    ]
    assert summary["primary_research_candidate_gate_decision"] == (
        "ELIGIBLE_FOR_OWNER_APPROVAL"
    )
    assert summary["smoothed_forward_binding_id"] == fixture["binding"]["binding_id"]
    assert summary["paper_shadow_primary_switch_auto_switch"] is False
    assert summary["smoothed_owner_promotion_decision"] == "continue_observation"
    assert summary["production_effect"] == "none"
