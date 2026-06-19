from __future__ import annotations

import json
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
RESEARCH_DIR = PROJECT_ROOT / "docs" / "research"

B2_FULL_DIAGNOSTIC_ARTIFACTS = (
    "b2_full_diagnostic_scope.json",
    "b2_full_diagnostic_windows.json",
    "b2_full_diagnostic_backfill.json",
    "b2_drawdown_protection_attribution.json",
    "b2_false_risk_off_reentry_cost_review.json",
    "b2_cost_benchmark_utility_review.json",
    "b2_signal_robustness_trigger_stability.json",
    "b2_only_full_diagnostic_gate.json",
    "b3_signal_precheck_resolution_plan.json",
    "b2_b3_branch_status_snapshot.json",
)


def test_trading_565_to_574_outputs_expected_statuses() -> None:
    expected = {
        "b2_full_diagnostic_scope.json": "B2_FULL_DIAGNOSTIC_SCOPE_READY",
        "b2_full_diagnostic_windows.json": "B2_FULL_DIAGNOSTIC_WINDOWS_READY",
        "b2_full_diagnostic_backfill.json": "B2_FULL_DIAGNOSTIC_PARTIAL",
        "b2_drawdown_protection_attribution.json": "B2_DRAWDOWN_PROTECTION_MIXED",
        "b2_false_risk_off_reentry_cost_review.json": "B2_REENTRY_LAG_HIGH",
        "b2_cost_benchmark_utility_review.json": "B2_UTILITY_MIXED",
        "b2_signal_robustness_trigger_stability.json": "B2_TRIGGER_STABILITY_WEAK",
        "b2_only_full_diagnostic_gate.json": "B2_ONLY_NEEDS_MORE_EVIDENCE",
        "b3_signal_precheck_resolution_plan.json": "B3_SIGNAL_PRECHECK_RESOLUTION_READY",
        "b2_b3_branch_status_snapshot.json": "CONTINUE_B2_ONLY_RESEARCH",
    }

    for file_name, status in expected.items():
        assert _read_json(file_name)["status"] == status


def test_b2_full_diagnostic_scope_freezes_allowed_and_forbidden_mechanisms() -> None:
    scope = _read_json("b2_full_diagnostic_scope.json")

    assert scope["allowed_mechanisms"] == [
        "B0 static baseline",
        "fast asymmetric risk overlay",
        "risk-off exposure scaler",
        "re-entry logic",
    ]
    assert set(scope["forbidden_mechanisms"]) == {
        "B3 slow tilt",
        "B5 confidence shrinkage",
        "B6 regime information",
        "P0 mixed allocator",
        "paper-shadow",
        "official target weights",
        "broker/order/live/production mutation",
    }
    assert scope["untouched_holdout_usage"] is False
    assert all(row["status"] == "PASS" for row in scope["validation"])


def test_b2_full_diagnostic_windows_include_required_non_holdout_set() -> None:
    windows = _read_json("b2_full_diagnostic_windows.json")

    assert [row["window_id"] for row in windows["windows"]] == [
        "rapid_drawdown",
        "slow_drawdown",
        "volatility_spike",
        "high_volatility_sideways",
        "semiconductor_correction",
        "v_shaped_recovery",
        "false_risk_off_cluster",
        "shallow_pullback_false_alarm",
        "normal_uptrend_control",
        "calm_market_control",
    ]
    assert windows["window_count"] == 10
    assert all(row["holdout_allowed"] is False for row in windows["windows"])
    assert all(row["data_quality_status"] == "PASS_WITH_WARNINGS" for row in windows["windows"])
    assert all(row["regime_label"] == "ai_after_chatgpt" for row in windows["windows"])


def test_b2_full_diagnostic_backfill_stays_partial_without_parameter_tuning() -> None:
    backfill = _read_json("b2_full_diagnostic_backfill.json")

    assert backfill["parameter_tuning_applied"] is False
    assert backfill["b1_optional_wrapper_config_enabled"] is False
    assert backfill["comparisons"]["B2_vs_B0"] == (
        "computed_from_canonical_b2_diagnostic_rows"
    )
    assert backfill["comparisons"]["B2_vs_B1_optional_wrapper"] == (
        "NOT_RUN_CONFIG_NOT_ENABLED"
    )
    assert backfill["comparisons"]["B2_vs_no_trade_baseline"] == (
        "NOT_AVAILABLE_NO_WINDOW_ALIGNED_SOURCE"
    )
    assert backfill["aggregate"]["risk_trigger_count"] == 33
    assert backfill["aggregate"]["triggered_window_count"] == 1
    assert backfill["aggregate"]["risk_off_days"] == 16
    assert any(
        row["window_result"] == "control_no_trigger_reference"
        for row in backfill["window_results"]
    )
    assert backfill["limitations"]


def test_b2_full_diagnostic_gate_keeps_b4_b5_b6_v3_blocked() -> None:
    gate = _read_json("b2_only_full_diagnostic_gate.json")
    snapshot = _read_json("b2_b3_branch_status_snapshot.json")

    assert gate["input_statuses"] == {
        "b2_full_diagnostic_backfill": "B2_FULL_DIAGNOSTIC_PARTIAL",
        "cost_benchmark_utility": "B2_UTILITY_MIXED",
        "drawdown_protection": "B2_DRAWDOWN_PROTECTION_MIXED",
        "reentry_cost": "B2_REENTRY_LAG_HIGH",
        "signal_robustness_trigger_stability": "B2_TRIGGER_STABILITY_WEAK",
    }
    assert gate["hard_rule"] == "Do not allow B4/B5/B6/v3 from this gate."
    assert gate["B4_retest_allowed"] is False
    assert gate["b5_allowed"] is False
    assert gate["b6_allowed"] is False
    assert gate["v3_allowed"] is False
    assert snapshot["B2_status"] == "B2_ONLY_NEEDS_MORE_EVIDENCE"
    assert snapshot["B4_retest_allowed"] is False
    assert snapshot["b5_allowed"] is False
    assert snapshot["b6_allowed"] is False
    assert snapshot["v3_allowed"] is False
    assert all(row["status"] == "PASS" for row in snapshot["hard_rules"])


def test_b3_resolution_plan_is_signal_only_after_mixed_precheck() -> None:
    plan = _read_json("b3_signal_precheck_resolution_plan.json")

    assert plan["source_taxonomy_status"] == "B3_SIGNAL_DIRECTION_FAILURE_TAXONOMY_READY"
    assert plan["source_precheck_v2_status"] == "B3_PRECHECK_MIXED"
    assert plan["classified_b3_state"] in {
        "B3_SIGNAL_LAYER_NEEDS_MORE_FEATURE_WORK",
        "B3_DIRECTION_RULE_NEEDS_REDESIGN",
        "B3_SIGNAL_TOO_NOISY",
        "B3_DROP_CURRENT_LINE",
    }
    assert plan["weight_generation"] is False
    assert plan["mini_backfill_run"] is False
    assert plan["B4_run"] is False
    assert plan["recommendation"] == "continue_signal_redesign_no_weights"


def test_b2_full_diagnostic_artifacts_disclose_quality_sources_and_safety() -> None:
    expected_source_keys = {
        "b2_only_research_gate_v2",
        "b2_risk_heavy_window_expansion_v2",
        "b2_only_risk_heavy_diagnostic_backfill",
        "b2_reentry_opportunity_cost_review",
        "b2_cost_benchmark_survival_review",
        "b2_risk_trigger_sensitivity_map",
        "b3_signal_direction_failure_taxonomy",
        "b3_redesign_candidate_precheck_v2",
        "branch_decision_after_b2_v2_b3_precheck_v2",
        "b1_wrapper_compatibility_with_b2",
        "research_window_catalog",
    }

    for file_name in B2_FULL_DIAGNOSTIC_ARTIFACTS:
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
