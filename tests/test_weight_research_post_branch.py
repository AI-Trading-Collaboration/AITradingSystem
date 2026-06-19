from __future__ import annotations

import json
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
RESEARCH_DIR = PROJECT_ROOT / "docs" / "research"

POST_BRANCH_ARTIFACTS = (
    "b2_only_research_scope.json",
    "b2_risk_heavy_window_catalog.json",
    "b2_only_risk_heavy_diagnostic_backfill.json",
    "b2_false_risk_off_reentry_attribution.json",
    "b2_cost_benchmark_survival_review.json",
    "b2_only_research_gate.json",
    "b3_redesign_constraints.json",
    "b3_redesign_hypothesis_ranking.json",
    "b3_signal_direction_precheck.json",
    "b3_redesigned_mini_backfill.json",
    "b3_redesign_gate.json",
    "b1_wrapper_compatibility_with_b2.json",
    "b1_wrapper_compatibility_with_redesigned_b3.json",
    "post_b2_b3_branch_synthesis.json",
    "retest_b4_with_redesigned_b3.json",
    "b5_readmission_after_redesigned_b4.json",
    "research_cadence_controller.json",
    "candidate_exploration_backlog_manager.json",
    "monthly_research_program_review.json",
    "final_branch_decision_snapshot.json",
)


def test_trading_537_to_556_outputs_expected_statuses() -> None:
    expected = {
        "b2_only_research_scope.json": "B2_ONLY_RESEARCH_SCOPE_PASS",
        "b2_risk_heavy_window_catalog.json": "B2_RISK_HEAVY_WINDOW_CATALOG_READY",
        "b2_only_risk_heavy_diagnostic_backfill.json": "B2_RISK_HEAVY_BACKFILL_COMPLETE",
        "b2_false_risk_off_reentry_attribution.json": "B2_PROTECTS_BUT_REENTRY_LAG_HIGH",
        "b2_cost_benchmark_survival_review.json": "B2_COST_BENCHMARK_MIXED",
        "b2_only_research_gate.json": "B2_ONLY_NEEDS_MORE_EVIDENCE",
        "b3_redesign_constraints.json": "B3_REDESIGN_CONSTRAINTS_FROZEN",
        "b3_redesign_hypothesis_ranking.json": "B3_REDESIGN_HYPOTHESES_RANKED",
        "b3_signal_direction_precheck.json": "B3_SIGNAL_DIRECTION_PRECHECK_MIXED",
        "b3_redesigned_mini_backfill.json": "B3_REDESIGNED_MINI_BACKFILL_BLOCKED",
        "b3_redesign_gate.json": "B3_REDESIGN_RETURN_TO_HYPOTHESIS",
        "b1_wrapper_compatibility_with_b2.json": "B1_WRAPPER_MIXED_WITH_B2",
        "b1_wrapper_compatibility_with_redesigned_b3.json": (
            "B1_B3_WRAPPER_TEST_BLOCKED_NO_VALID_B3"
        ),
        "post_b2_b3_branch_synthesis.json": "CONTINUE_B2_ONLY_RESEARCH",
        "retest_b4_with_redesigned_b3.json": "B4_REDESIGNED_RETEST_BLOCKED_NO_VALID_B3",
        "b5_readmission_after_redesigned_b4.json": "B5_READMISSION_BLOCKED",
        "research_cadence_controller.json": "RESEARCH_CADENCE_CONTROLLER_READY",
        "candidate_exploration_backlog_manager.json": "BACKLOG_READY",
        "monthly_research_program_review.json": "MONTHLY_RESEARCH_PROGRAM_REVIEW_READY",
        "final_branch_decision_snapshot.json": "CONTINUE_B2_ONLY_PATH",
    }

    for file_name, status in expected.items():
        assert _read_json(file_name)["status"] == status


def test_b2_catalog_and_gate_keep_scope_narrow_and_research_only() -> None:
    scope = _read_json("b2_only_research_scope.json")
    catalog = _read_json("b2_risk_heavy_window_catalog.json")
    backfill = _read_json("b2_only_risk_heavy_diagnostic_backfill.json")
    gate = _read_json("b2_only_research_gate.json")

    assert scope["b2_definition"] == (
        "B0 static strategic baseline + fast asymmetric risk scaler only"
    )
    assert "slow relative tilt" in scope["forbidden_mechanisms"]
    assert "P0 mixed dynamic strategy" in scope["forbidden_mechanisms"]
    assert set(catalog["required_coverage"]) == {
        "rapid_drawdown",
        "slow_drawdown",
        "high_volatility_sideways",
        "semiconductor_correction",
        "v_shaped_recovery",
        "false_risk_off_cluster",
        "risk_off_but_quick_recovery",
        "risk_signal_false_positive",
    }
    assert all(row["allowed_stage"] == "diagnostic" for row in catalog["windows"])
    assert all(row["holdout_allowed"] is False for row in catalog["windows"])
    assert backfill["aggregate"]["risk_trigger_count"] == 33
    assert backfill["aggregate"]["triggered_window_count"] == 1
    assert backfill["aggregate"]["drawdown_delta_vs_B0"] > 0
    assert backfill["aggregate"]["return_delta_vs_B0"] < 0
    assert gate["paper_shadow_allowed"] is False
    assert gate["combo_research_allowed"] is False


def test_b3_precheck_blocks_mini_backfill_b4_retest_and_b5_readmission() -> None:
    precheck = _read_json("b3_signal_direction_precheck.json")
    mini = _read_json("b3_redesigned_mini_backfill.json")
    gate = _read_json("b3_redesign_gate.json")
    b4 = _read_json("retest_b4_with_redesigned_b3.json")
    b5 = _read_json("b5_readmission_after_redesigned_b4.json")
    final = _read_json("final_branch_decision_snapshot.json")

    assert precheck["hypothetical_weight_generated"] is False
    assert precheck["wrong_direction_count"] > 0
    assert mini["mini_backfill_executed"] is False
    assert mini["blocked_reason"] == (
        "B3 signal-direction precheck is not PASS; no redesigned B3 weights are generated."
    )
    assert gate["reenter_b4_allowed"] is False
    assert b4["blocked_reason"] == "no_valid_redesigned_b3"
    assert b5["b5_allowed"] is False
    assert final["selected_branch"] == "CONTINUE_B2_ONLY_PATH"
    assert final["b5_allowed"] is False
    assert final["b6_allowed"] is False
    assert final["v3_allowed"] is False


def test_post_branch_artifacts_disclose_quality_regime_and_safety_boundary() -> None:
    for file_name in POST_BRANCH_ARTIFACTS:
        payload = _read_json(file_name)
        safety = payload["safety_boundary"]

        assert payload["schema_version"] == 1
        assert payload["market_regime"] == "ai_after_chatgpt"
        assert payload["requested_date_range"]["start_date"] == "2022-12-01"
        assert payload["holdout_accessed"] is False
        assert payload["forbidden_outputs_absent"] is True
        assert payload["data_quality_gate"]["required_command"] == "aits validate-data"
        assert payload["data_quality_gate"]["passed"] is True
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
