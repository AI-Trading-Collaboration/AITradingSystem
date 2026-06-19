from __future__ import annotations

import json
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
RESEARCH_DIR = PROJECT_ROOT / "docs" / "research"

B2_FOLLOWUP_ARTIFACTS = (
    "b2_needs_more_evidence_root_cause_drilldown.json",
    "b2_per_window_utility_scorecard.json",
    "b2_trigger_reentry_design_assessment.json",
    "b2_next_evidence_plan.json",
    "b2_gate_v4_decision.json",
    "b2_research_branch_snapshot.json",
)


def test_trading_582_to_587_outputs_expected_statuses() -> None:
    expected = {
        "b2_needs_more_evidence_root_cause_drilldown.json": (
            "B2_NEEDS_MORE_EVIDENCE_BUT_NO_STRUCTURAL_BLOCKER"
        ),
        "b2_per_window_utility_scorecard.json": "B2_WINDOW_UTILITY_MIXED",
        "b2_trigger_reentry_design_assessment.json": (
            "B2_DESIGN_ACCEPTABLE_NEEDS_MORE_EVIDENCE"
        ),
        "b2_next_evidence_plan.json": "RUN_MORE_B2_RISK_WINDOWS",
        "b2_gate_v4_decision.json": "B2_ONLY_CONTINUE_WITH_DEFINED_EVIDENCE_PLAN",
        "b2_research_branch_snapshot.json": (
            "CONTINUE_B2_ONLY_WITH_TARGETED_EVIDENCE"
        ),
    }

    for file_name, status in expected.items():
        assert _read_json(file_name)["status"] == status


def test_root_cause_drilldown_explains_more_evidence_without_structural_blocker() -> None:
    payload = _read_json("b2_needs_more_evidence_root_cause_drilldown.json")

    assert payload["structural_blocker_detected"] is False
    assert payload["completed_evidence_summary"] == {
        "control_false_risk_off_count": 0,
        "control_unnecessary_exposure_reduction_count": 0,
        "full_diagnostic_status": "B2_FULL_DIAGNOSTIC_COMPLETE",
        "risk_trigger_count": 33,
        "triggered_window_count": 1,
    }
    present = {
        row["category"]
        for row in payload["root_cause_classification"]
        if row["present"] is True
    }
    assert {
        "insufficient risk-heavy sample",
        "drawdown protection mixed",
        "trigger coverage too low",
        "re-entry lag uncertainty",
        "cost / benchmark utility mixed",
        "signal robustness uncertainty",
        "window dispersion too high",
        "no structural blocker but insufficient positive evidence",
    }.issubset(present)


def test_per_window_scorecard_identifies_mixed_utility_and_target_windows() -> None:
    payload = _read_json("b2_per_window_utility_scorecard.json")
    rows = {row["window_id"]: row for row in payload["scorecard_rows"]}

    assert len(rows) == 10
    assert rows["slow_drawdown"]["pass_mixed_fail"] == "mixed"
    assert rows["slow_drawdown"]["reentry_lag"] == 14
    assert rows["rapid_drawdown"]["pass_mixed_fail"] == "fail"
    assert rows["volatility_spike"]["pass_mixed_fail"] == "fail"
    assert rows["normal_uptrend_control"]["pass_mixed_fail"] == "pass"
    assert rows["calm_market_control"]["false_risk_off_count"] == 0
    assert payload["summary"]["windows_where_B2_helps"] == ["slow_drawdown"]
    assert payload["summary"]["windows_where_B2_hurts"] == ["slow_drawdown"]
    assert payload["summary"]["windows_where_B2_fails_to_trigger"] == [
        "rapid_drawdown",
        "volatility_spike",
    ]
    assert payload["summary"]["worst_window"] == "slow_drawdown"
    assert payload["summary"]["utility_dispersion"] > 0
    assert "not an official target-weight score" in payload["utility_score_policy"]


def test_design_assessment_and_plan_keep_b2_unchanged_for_targeted_evidence() -> None:
    design = _read_json("b2_trigger_reentry_design_assessment.json")
    plan = _read_json("b2_next_evidence_plan.json")

    assert design["diagnostic_only"] is True
    assert design["threshold_tuning_applied"] is False
    answers = {row["question"]: row["answer"] for row in design["assessment_questions"]}
    assert answers["Is B2 avoiding false risk-off in calm windows?"] == "yes"
    assert answers["Is B2 re-entering too slowly after risk events?"] == "observed_risk"
    assert answers["Is the current trigger threshold too insensitive?"] == (
        "possible_but_not_proven"
    )
    assert "Keep current B2 logic unchanged" in design["design_change_recommendation"]
    assert plan["estimated_minimum_evidence_count"]["total_targeted_cases"] == 10
    assert "additional_risk_heavy_windows" in plan["estimated_minimum_evidence_count"]
    assert any("trigger count" in metric for metric in plan["required_metrics"])
    assert any("false risk-off" in item for item in plan["kill_criteria"])


def test_gate_v4_and_branch_snapshot_keep_all_downstream_modules_blocked() -> None:
    gate = _read_json("b2_gate_v4_decision.json")
    snapshot = _read_json("b2_research_branch_snapshot.json")

    assert gate["promising_requirements"] == {
        "acceptable_reentry_cost": False,
        "acceptable_utility": False,
        "clear_drawdown_protection": False,
        "no_untouched_holdout_used": True,
        "stable_trigger_behavior": False,
    }
    assert gate["B4_retest_allowed"] is False
    assert gate["b5_allowed"] is False
    assert gate["b6_allowed"] is False
    assert gate["v3_allowed"] is False
    assert gate["paper_shadow_allowed"] is False
    assert snapshot["b2_full_diagnostic_status"] == "B2_FULL_DIAGNOSTIC_COMPLETE"
    assert snapshot["b2_gate_v4_status"] == "B2_ONLY_CONTINUE_WITH_DEFINED_EVIDENCE_PLAN"
    assert snapshot["b3_signal_status"] == "B3_SIGNAL_TOO_NOISY"
    assert snapshot["B4_retest_allowed"] is False
    assert snapshot["b5_allowed"] is False
    assert snapshot["b6_allowed"] is False
    assert snapshot["v3_allowed"] is False
    assert snapshot["paper_shadow_allowed"] is False
    assert all(row["status"] == "PASS" for row in snapshot["hard_rules"])


def test_b2_followup_artifacts_disclose_quality_sources_and_safety() -> None:
    expected_source_keys = {
        "b2_control_window_rerun",
        "b2_cost_benchmark_utility_review",
        "b2_drawdown_protection_attribution",
        "b2_false_risk_off_reentry_cost_review",
        "b2_full_diagnostic_backfill",
        "b2_full_diagnostic_with_control_windows",
        "b2_no_trigger_correctness_review",
        "b2_only_research_gate_v3",
        "b2_path_decision_snapshot",
        "b2_signal_robustness_trigger_stability",
        "b3_signal_precheck_resolution_plan",
    }

    for file_name in B2_FOLLOWUP_ARTIFACTS:
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
