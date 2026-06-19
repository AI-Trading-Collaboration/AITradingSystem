from __future__ import annotations

import json
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
RESEARCH_DIR = PROJECT_ROOT / "docs" / "research"

B2_FINAL_DECISION_ARTIFACTS = (
    "b2_slow_drawdown_evidence_completion.json",
    "b2_slow_drawdown_edge_validation.json",
    "b2_fast_risk_role_deprecation_review.json",
    "b2_reentry_lag_design_implication.json",
    "b2_role_reclassification.json",
    "b2_final_research_gate.json",
    "b2_research_line_owner_packet.json",
    "b2_branch_snapshot_final.json",
)


def test_trading_597_to_604_outputs_expected_statuses() -> None:
    expected = {
        "b2_slow_drawdown_evidence_completion.json": (
            "B2_SLOW_DRAWDOWN_NO_ADDITIONAL_WINDOW"
        ),
        "b2_slow_drawdown_edge_validation.json": (
            "B2_SLOW_DRAWDOWN_EDGE_SINGLE_WINDOW_ONLY"
        ),
        "b2_fast_risk_role_deprecation_review.json": "B2_FAST_RISK_ROLE_DEPRECATED",
        "b2_reentry_lag_design_implication.json": (
            "B2_REENTRY_LAG_REQUIRES_DESIGN_REWORK"
        ),
        "b2_role_reclassification.json": "B2_RISK_OVERLAY_NEEDS_REDESIGN",
        "b2_final_research_gate.json": "B2_CURRENT_FORM_RETURN_TO_DESIGN",
        "b2_research_line_owner_packet.json": "B2_RESEARCH_LINE_OWNER_PACKET_READY",
        "b2_branch_snapshot_final.json": "RETURN_B2_TO_DESIGN",
    }

    for file_name, status in expected.items():
        assert _read_json(file_name)["status"] == status


def test_slow_drawdown_completion_does_not_invent_second_window() -> None:
    payload = _read_json("b2_slow_drawdown_evidence_completion.json")
    original = payload["original_slow_drawdown_evidence"]
    inventory = {row["window_id"]: row for row in payload["window_inventory"]}

    assert payload["additional_slow_drawdown_windows_found"] == 0
    assert payload["additional_slow_drawdown_evidence"] == []
    assert payload["B2_only_rerun_on_additional_window"] == (
        "NOT_RUN_NO_ADDITIONAL_INDEPENDENT_SLOW_DRAWDOWN_WINDOW"
    )
    assert payload["promising_from_single_window_allowed"] is False
    assert inventory["slow_drawdown"]["is_slow_drawdown_candidate"] is True
    assert inventory["slow_drawdown"]["independent_from_primary"] is False
    assert inventory["untouched_temporal_holdout"]["holdout_blocked"] is True
    assert inventory["rapid_drawdown"]["holdout_blocked"] is False
    assert original["trigger_date"] == "2025-03-10"
    assert original["risk_off_date"] == "2025-04-04"
    assert original["risk_trigger_count"] == 33
    assert original["drawdown_delta"] > 0
    assert original["return_delta"] < 0
    assert original["re_entry_lag"] == 14
    assert original["window_utility_classification"] == (
        "mixed_drawdown_help_with_utility_cost"
    )
    assert {row["exposure_scaler"] for row in original["exposure_reduction_path"]} == {
        0.55,
        0.85,
        1.0,
    }


def test_edge_validation_blocks_repeatable_and_promising_classification() -> None:
    payload = _read_json("b2_slow_drawdown_edge_validation.json")

    assert payload["completion_status"] == "B2_SLOW_DRAWDOWN_NO_ADDITIONAL_WINDOW"
    assert payload["independent_slow_drawdown_window_count"] == 1
    assert payload["protected_slow_drawdown_window_count"] == 1
    assert payload["repeatable_protection"] is False
    assert payload["meaningful_drawdown_protection"] is True
    assert payload["acceptable_opportunity_cost"] is False
    assert payload["single_window_only_blocks_promising"] is True
    assert payload["reentry_lag_review"] == "B2_REENTRY_LAG_SIGNAL_DRIVEN"


def test_fast_risk_is_deprecated_and_reentry_requires_design_rework() -> None:
    fast = _read_json("b2_fast_risk_role_deprecation_review.json")
    reentry = _read_json("b2_reentry_lag_design_implication.json")

    assert fast["failed_fast_risk_because_trigger_too_slow"] is False
    assert fast["failed_fast_risk_because_signal_not_sensitive"] is True
    assert fast["actual_fast_risk_trigger_count"] == 0
    assert fast["binding_issue_detected"] is False
    assert fast["fast_risk_better_as_future_module"] is True
    assert fast["requires_new_trigger_family_for_fast_risk"] is True
    assert "fast asymmetric risk overlay" in fast["deprecated_claims"]
    assert reentry["signal_recovery_timing"]["risk_on_observed_within_window"] is False
    assert reentry["exposure_recovery_timing"][
        "exposure_recovery_observed_within_window"
    ] is False
    assert reentry["exposure_recovery_timing"]["days_below_baseline_exposure"] == 33
    assert reentry["reentry_lag_by_window"][0]["source_reentry_lag"] == 14
    assert reentry["lag_systematic_assessment"] == "NOT_PROVEN_SINGLE_TRIGGER_WINDOW"
    assert reentry["logic_changed"] is False
    assert reentry["threshold_tuning_applied"] is False


def test_role_gate_owner_packet_and_snapshot_keep_downstream_blocked() -> None:
    role = _read_json("b2_role_reclassification.json")
    gate = _read_json("b2_final_research_gate.json")
    packet = _read_json("b2_research_line_owner_packet.json")
    snapshot = _read_json("b2_branch_snapshot_final.json")

    assert role["fast_asymmetric_role_allowed"] is False
    assert role["slow_drawdown_defensive_role_allowed"] is False
    assert role["current_supported_role"] == "none_promotable_current_form_needs_design_review"
    assert gate["decision_inputs"]["role_reclassification"] == (
        "B2_RISK_OVERLAY_NEEDS_REDESIGN"
    )
    assert gate["promising_requirements"]["repeatable_protection"] is False
    assert gate["promising_requirements"]["acceptable_reentry"] is False
    assert gate["promising_requirements"]["clear_role"] is False
    assert gate["promising_requirements"]["no_fast_risk_claim_if_unsupported"] is True
    assert packet["B2_original_intended_role"] == "fast asymmetric risk overlay"
    assert packet["owner_decision_appended"] is False
    assert "return B2 to design" in packet["recommended_owner_options"]
    assert snapshot["B2_role_classification"] == "B2_RISK_OVERLAY_NEEDS_REDESIGN"
    assert snapshot["B2_final_gate_result"] == "B2_CURRENT_FORM_RETURN_TO_DESIGN"
    assert snapshot["B2_owner_packet_status"] == "B2_RESEARCH_LINE_OWNER_PACKET_READY"
    assert snapshot["B3_status"] == "B3_SIGNAL_PRECHECK_RESOLUTION_READY"
    for payload in (gate, snapshot):
        assert payload["B4_retest_allowed"] is False
        assert payload["b5_allowed"] is False
        assert payload["b6_allowed"] is False
        assert payload["v3_allowed"] is False
        assert payload["paper_shadow_allowed"] is False


def test_b2_final_decision_artifacts_disclose_quality_sources_and_safety() -> None:
    expected_source_keys = {
        "b2_control_window_rerun",
        "b2_cost_benchmark_utility_review",
        "b2_fast_risk_no_trigger_audit",
        "b2_full_diagnostic_backfill",
        "b2_gate_v5",
        "b2_no_trigger_correctness_review",
        "b2_reentry_lag_root_cause_review",
        "b2_research_branch_snapshot_v2",
        "b2_role_narrowing_assessment",
        "b2_signal_robustness_trigger_stability",
        "b2_slow_drawdown_repeatability_study",
        "b2_targeted_evidence_backfill_v2",
        "b2_targeted_evidence_scorecard",
        "b2_targeted_evidence_window_lock",
        "b3_signal_precheck_resolution_plan",
        "research_window_catalog",
    }

    for file_name in B2_FINAL_DECISION_ARTIFACTS:
        payload = _read_json(file_name)
        safety = payload["safety_boundary"]

        assert payload["schema_version"] == 1
        assert payload["market_regime"] == "ai_after_chatgpt"
        assert payload["requested_date_range"]["start_date"] == "2022-12-01"
        assert payload["holdout_accessed"] is False
        assert payload["forbidden_outputs_absent"] is True
        assert payload["data_quality_gate"]["required_command"] == "aits validate-data"
        assert payload["data_quality_gate"]["passed"] is True
        assert set(payload["source_artifacts"]) == expected_source_keys
        assert payload["reader_brief"]["key_result"] == payload["status"]
        assert safety["research_only"] is True
        assert safety["manual_review_only"] is True
        assert safety["official_target_weights"] is False
        assert safety["paper_shadow_activation"] is False
        assert safety["paper_shadow_allowed"] is False
        assert safety["extended_shadow_allowed"] is False
        assert safety["live_trading_allowed"] is False
        assert safety["broker_action_allowed"] is False
        assert safety["order_ticket_generated"] is False
        assert safety["owner_decision_appended"] is False
        assert safety["production_effect"] == "none"


def _read_json(file_name: str) -> dict[str, object]:
    return json.loads((RESEARCH_DIR / file_name).read_text(encoding="utf-8"))
