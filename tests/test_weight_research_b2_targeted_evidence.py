from __future__ import annotations

import json
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
RESEARCH_DIR = PROJECT_ROOT / "docs" / "research"

B2_TARGETED_ARTIFACTS = (
    "b2_targeted_evidence_window_lock.json",
    "b2_fast_risk_no_trigger_audit.json",
    "b2_slow_drawdown_repeatability_study.json",
    "b2_reentry_lag_root_cause_review.json",
    "b2_role_narrowing_assessment.json",
    "b2_targeted_evidence_backfill_v2.json",
    "b2_targeted_evidence_scorecard.json",
    "b2_gate_v5.json",
    "b2_research_branch_snapshot_v2.json",
)


def test_trading_588_to_596_outputs_expected_statuses() -> None:
    expected = {
        "b2_targeted_evidence_window_lock.json": (
            "B2_TARGETED_EVIDENCE_WINDOWS_INCOMPLETE"
        ),
        "b2_fast_risk_no_trigger_audit.json": (
            "B2_FAST_RISK_NOT_SUPPORTED_BY_CURRENT_DESIGN"
        ),
        "b2_slow_drawdown_repeatability_study.json": (
            "B2_SLOW_DRAWDOWN_EDGE_SINGLE_WINDOW_ONLY"
        ),
        "b2_reentry_lag_root_cause_review.json": "B2_REENTRY_LAG_SIGNAL_DRIVEN",
        "b2_role_narrowing_assessment.json": "B2_FAST_RISK_OVERLAY_NOT_SUPPORTED",
        "b2_targeted_evidence_backfill_v2.json": (
            "B2_TARGETED_EVIDENCE_BACKFILL_PARTIAL"
        ),
        "b2_targeted_evidence_scorecard.json": "B2_TARGETED_EVIDENCE_MIXED",
        "b2_gate_v5.json": "B2_ONLY_CONTINUE_WITH_MORE_TARGETED_EVIDENCE",
        "b2_research_branch_snapshot_v2.json": "CONTINUE_B2_ONLY_RESEARCH",
    }

    for file_name, status in expected.items():
        assert _read_json(file_name)["status"] == status


def test_targeted_window_lock_records_required_groups_and_holdout_block() -> None:
    payload = _read_json("b2_targeted_evidence_window_lock.json")

    assert len(payload["locked_windows"]) == 8
    assert all(row["allowed_stage"] == "diagnostic" for row in payload["locked_windows"])
    assert all(row["holdout_allowed"] is False for row in payload["locked_windows"])
    groups = {row["window_group"]: row for row in payload["window_group_summary"]}
    assert groups["slow_drawdown_repeatability"] == {
        "available_count": 1,
        "required_count": 2,
        "status": "incomplete",
        "window_group": "slow_drawdown_repeatability",
    }
    assert groups["rapid_drawdown"]["status"] == "complete"
    assert groups["volatility_spike"]["status"] == "complete"
    assert groups["false_risk_off_shallow_pullback"]["available_count"] == 2
    assert payload["holdout_allowed"] is False


def test_fast_risk_audit_detects_no_trigger_without_binding_issue() -> None:
    payload = _read_json("b2_fast_risk_no_trigger_audit.json")

    assert payload["threshold_tuning_applied"] is False
    assert payload["binding_issue_detected"] is False
    assert payload["trigger_design_slow_drawdown_biased"] is True
    rows = {row["window_id"]: row for row in payload["audit_rows"]}
    assert set(rows) == {"rapid_drawdown_fast_risk", "volatility_spike_fast_risk"}
    for row in rows.values():
        assert row["actual_trigger_count"] == 0
        assert row["first_valid_trigger_date"] is None
        assert row["days_from_risk_onset_to_trigger"] is None
        assert row["binding_issue"] is False
        assert row["classification"] == "NO_TRIGGER_CALM_SIGNAL"
        assert row["risk_off_threshold_distance"] > 0
        assert row["elevated_threshold_distance"] > 0
        assert row["risk_signal_values_before_drawdown"]
        assert row["risk_signal_values_during_drawdown"]


def test_slow_repeatability_and_reentry_remain_not_promising() -> None:
    slow = _read_json("b2_slow_drawdown_repeatability_study.json")
    reentry = _read_json("b2_reentry_lag_root_cause_review.json")

    assert slow["available_slow_drawdown_window_count"] == 1
    assert slow["triggered_positive_window_count"] == 1
    assert slow["slow_drawdown_protection_stable"] is False
    assert slow["promising_classification_allowed"] is False
    assert slow["repeatability_rows"][0]["risk_trigger_count"] == 33
    assert slow["repeatability_rows"][0]["drawdown_delta"] > 0
    assert reentry["risk_off_date"] == "2025-03-10"
    assert reentry["risk_on_date"] == "NOT_OBSERVED_WITHIN_WINDOW"
    assert reentry["re_entry_trigger_date"] == "NOT_OBSERVED_WITHIN_WINDOW"
    assert reentry["days_below_baseline_exposure"] == 33
    assert reentry["source_reentry_lag"] == 14
    assert reentry["root_cause_flags"]["slow_signal_recovery"] is True
    assert reentry["root_cause_flags"]["hysteresis_rule"] is False
    assert reentry["logic_changed"] is False


def test_role_backfill_scorecard_and_gate_keep_b2_research_only() -> None:
    role = _read_json("b2_role_narrowing_assessment.json")
    backfill = _read_json("b2_targeted_evidence_backfill_v2.json")
    scorecard = _read_json("b2_targeted_evidence_scorecard.json")
    gate = _read_json("b2_gate_v5.json")
    snapshot = _read_json("b2_research_branch_snapshot_v2.json")

    assert role["general_fast_asymmetric_risk_overlay_supported"] is False
    assert role["slow_drawdown_overlay_validated"] is False
    assert role["requires_design_tradeoff_review"] is True
    assert backfill["parameter_tuning_applied"] is False
    assert backfill["comparison_sources"]["B2_vs_B0"] == (
        "computed_from_canonical_b2_full_diagnostic_rows"
    )
    assert backfill["comparison_sources"]["B2_vs_no_trade_baseline"] == (
        "NOT_AVAILABLE_NO_WINDOW_ALIGNED_SOURCE"
    )
    assert backfill["aggregate"]["risk_trigger_count"] == 33
    assert backfill["worst_window"] == "slow_drawdown_repeat_primary"
    assert scorecard["role_classification"] == "B2_FAST_RISK_OVERLAY_NOT_SUPPORTED"
    assert scorecard["remaining_uncertainty"]
    assert gate["promising_requirements"]["repeatable_protection"] is False
    assert gate["promising_requirements"]["acceptable_false_risk_off"] is True
    assert snapshot["B2_current_role"] == "candidate_slow_drawdown_overlay_not_validated"
    assert snapshot["B2_gate_v5_result"] == (
        "B2_ONLY_CONTINUE_WITH_MORE_TARGETED_EVIDENCE"
    )
    for payload in (gate, snapshot):
        assert payload["B4_retest_allowed"] is False
        assert payload["b5_allowed"] is False
        assert payload["b6_allowed"] is False
        assert payload["v3_allowed"] is False
        assert payload["paper_shadow_allowed"] is False


def test_b2_targeted_artifacts_disclose_quality_sources_and_safety() -> None:
    expected_source_keys = {
        "b2_control_window_rerun",
        "b2_cost_benchmark_utility_review",
        "b2_false_risk_off_reentry_cost_review",
        "b2_full_diagnostic_backfill",
        "b2_gate_v4_decision",
        "b2_next_evidence_plan",
        "b2_no_trigger_correctness_review",
        "b2_per_window_utility_scorecard",
        "b2_research_branch_snapshot",
        "b2_signal_robustness_trigger_stability",
        "b3_signal_precheck_resolution_plan",
        "research_window_catalog",
    }

    for file_name in B2_TARGETED_ARTIFACTS:
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
        assert safety["production_effect"] == "none"


def _read_json(file_name: str) -> dict[str, object]:
    return json.loads((RESEARCH_DIR / file_name).read_text(encoding="utf-8"))
