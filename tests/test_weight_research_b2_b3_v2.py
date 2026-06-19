from __future__ import annotations

import json
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
RESEARCH_DIR = PROJECT_ROOT / "docs" / "research"

B2_B3_V2_ARTIFACTS = (
    "b2_evidence_gap_ledger.json",
    "b2_risk_heavy_window_expansion_v2.json",
    "b2_risk_trigger_sensitivity_map.json",
    "b2_reentry_opportunity_cost_review.json",
    "b2_only_research_gate_v2.json",
    "b3_signal_direction_failure_taxonomy.json",
    "b3_redesign_candidate_precheck_v2.json",
    "branch_decision_after_b2_v2_b3_precheck_v2.json",
)


def test_trading_557_to_564_outputs_expected_statuses() -> None:
    expected = {
        "b2_evidence_gap_ledger.json": "B2_EVIDENCE_GAP_LEDGER_READY",
        "b2_risk_heavy_window_expansion_v2.json": (
            "B2_RISK_HEAVY_WINDOW_EXPANSION_V2_READY"
        ),
        "b2_risk_trigger_sensitivity_map.json": "B2_TRIGGER_COVERAGE_TOO_LOW",
        "b2_reentry_opportunity_cost_review.json": "B2_REENTRY_LAG_HIGH",
        "b2_only_research_gate_v2.json": "B2_ONLY_NEEDS_MORE_EVIDENCE",
        "b3_signal_direction_failure_taxonomy.json": (
            "B3_SIGNAL_DIRECTION_FAILURE_TAXONOMY_READY"
        ),
        "b3_redesign_candidate_precheck_v2.json": "B3_PRECHECK_MIXED",
        "branch_decision_after_b2_v2_b3_precheck_v2.json": (
            "CONTINUE_B2_ONLY_TO_FULL_DIAGNOSTIC"
        ),
    }

    for file_name, status in expected.items():
        assert _read_json(file_name)["status"] == status


def test_b2_evidence_gap_ledger_and_windows_cover_required_v2_questions() -> None:
    ledger = _read_json("b2_evidence_gap_ledger.json")
    windows = _read_json("b2_risk_heavy_window_expansion_v2.json")

    assert ledger["current_b2_gate"] == "B2_ONLY_NEEDS_MORE_EVIDENCE"
    for field in (
        "missing_risk_heavy_windows",
        "insufficient_trigger_coverage",
        "false_risk_off_uncertainty",
        "re_entry_lag_uncertainty",
        "V_shaped_recovery_opportunity_cost_uncertainty",
        "cost_benchmark_weakness_or_incompleteness",
        "signal_robustness_gaps",
        "window_stability_gaps",
    ):
        assert field in ledger

    assert [row["window_id"] for row in windows["windows"]] == [
        "rapid_drawdown",
        "slow_drawdown",
        "volatility_spike",
        "high_volatility_sideways",
        "semiconductor_correction",
        "v_shaped_recovery",
        "false_risk_off_cluster",
        "shallow_pullback_false_alarm",
    ]
    assert all(row["allowed_stage"] == "diagnostic" for row in windows["windows"])
    assert all(row["holdout_allowed"] is False for row in windows["windows"])
    assert all(row["parameter_tuning_allowed"] is False for row in windows["windows"])
    assert all("B3" in row["forbidden_modules"] for row in windows["windows"])
    assert windows["rules"]["untouched_holdout_used"] is False
    assert windows["rules"]["parameter_tuning"] is False
    assert windows["rules"]["B3_B5_B6_regime_absent"] is True


def test_b2_trigger_sensitivity_and_reentry_reviews_do_not_tune_thresholds() -> None:
    sensitivity = _read_json("b2_risk_trigger_sensitivity_map.json")
    reentry = _read_json("b2_reentry_opportunity_cost_review.json")
    gate = _read_json("b2_only_research_gate_v2.json")

    assert sensitivity["threshold_changed"] is False
    assert sensitivity["window_count"] == 8
    assert sensitivity["triggered_window_count"] == 1
    assert all(row["threshold_changed"] is False for row in sensitivity["window_rows"])
    assert any(
        row["window_id"] == "slow_drawdown" and row["risk_trigger_count"] > 0
        for row in sensitivity["window_rows"]
    )
    assert reentry["risk_off_dates"]
    assert reentry["re_entry_dates"]
    assert reentry["days_out_of_market_or_lower_exposure"] == reentry["lower_exposure_days"]
    assert "missed_rebound_return_proxy" in reentry
    assert reentry["V_shaped_recovery_lag"]["observed"] is False
    assert reentry["redesign_required"] is False
    assert gate["hard_rule"] == "Do not move to B4/B5/v3 from this gate."
    assert gate["B4_retest_allowed"] is False
    assert gate["b5_allowed"] is False
    assert gate["b6_allowed"] is False
    assert gate["v3_allowed"] is False


def test_b3_failure_taxonomy_and_precheck_v2_remain_signal_only() -> None:
    taxonomy = _read_json("b3_signal_direction_failure_taxonomy.json")
    precheck = _read_json("b3_redesign_candidate_precheck_v2.json")

    assert {row["failure_mode"] for row in taxonomy["taxonomy"]} == {
        "direction inversion",
        "lag",
        "noisy relative strength",
        "asset mapping issue",
        "trend reversal sensitivity",
        "sector leadership instability",
        "tilt cap issue",
        "window-specific weakness",
    }
    assert taxonomy["current_line_ready_for_combo"] is False
    assert precheck["taxonomy_status"] == "B3_SIGNAL_DIRECTION_FAILURE_TAXONOMY_READY"
    assert {row["hypothesis_id"] for row in precheck["tested_hypotheses"]} == {
        "smaller_tilt_cap",
        "baseline_shrinkage",
        "relative_strength_confirmation",
    }
    assert precheck["signal_only_precheck"] is True
    assert precheck["weight_generation"] is False
    assert precheck["backfill_executed"] is False
    assert precheck["B4_executed"] is False
    assert precheck["B5_executed"] is False
    assert precheck["v3_executed"] is False
    assert precheck["ready_for_mini_backfill"] is False
    assert all(row["weight_generated"] is False for row in precheck["tested_hypotheses"])
    assert all(row["backfill_run"] is False for row in precheck["tested_hypotheses"])
    assert all(row["B4_run"] is False for row in precheck["tested_hypotheses"])


def test_branch_decision_keeps_b4_b5_b6_v3_blocked() -> None:
    branch = _read_json("branch_decision_after_b2_v2_b3_precheck_v2.json")

    assert branch["selected_branch"] == "CONTINUE_B2_ONLY_TO_FULL_DIAGNOSTIC"
    assert branch["input_statuses"] == {
        "b2_only_research_gate_v2": "B2_ONLY_NEEDS_MORE_EVIDENCE",
        "b3_redesign_candidate_precheck_v2": "B3_PRECHECK_MIXED",
    }
    assert branch["B4_retest_allowed"] is False
    assert branch["b5_allowed"] is False
    assert branch["b6_allowed"] is False
    assert branch["v3_allowed"] is False
    assert all(row["status"] == "PASS" for row in branch["hard_rules"])


def test_b2_b3_v2_artifacts_disclose_quality_regime_sources_and_safety() -> None:
    expected_source_keys = {
        "b2_only_research_gate",
        "b2_risk_heavy_window_catalog",
        "b2_only_risk_heavy_diagnostic_backfill",
        "b2_false_risk_off_reentry_attribution",
        "b2_cost_benchmark_survival_review",
        "b3_signal_direction_precheck",
        "b3_slow_tilt_signal_direction_audit",
        "b3_redesign_hypothesis_ranking",
        "final_branch_decision_snapshot",
    }

    for file_name in B2_B3_V2_ARTIFACTS:
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
        assert safety["extended_shadow_allowed"] is False
        assert safety["live_trading_allowed"] is False
        assert safety["broker_action_allowed"] is False
        assert safety["order_ticket_generated"] is False
        assert safety["production_effect"] == "none"


def _read_json(file_name: str) -> dict[str, object]:
    return json.loads((RESEARCH_DIR / file_name).read_text(encoding="utf-8"))
