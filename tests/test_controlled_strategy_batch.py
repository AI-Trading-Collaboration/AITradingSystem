from __future__ import annotations

import json
from datetime import date, timedelta
from pathlib import Path
from typing import Any

from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.controlled_strategy_batch import (
    run_controlled_strategy_batch_review,
    run_forward_evidence_daily_continuity_maturity_tracker,
    run_forward_evidence_maturity_tracker,
    run_gbdt_action_utility_baseline,
    run_gbdt_pivot_direction_selection,
    run_gbdt_pivot_review,
    run_regret_activation_inputs_from_value_surface_failures,
    run_regret_casebook_expansion_gate,
    run_regret_state_machine_controlled_prototype,
    run_simple_strategy_selector_pilot,
    run_utility_boundary_ranking_policy_audit,
    run_utility_ranking_robustness_pareto_audit,
    run_value_surface_controlled_expansion,
    run_value_surface_controlled_prototype,
    run_value_surface_warning_triage_review,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path
from scripts.run_validation_tier import TIER_SPECS

TEST_AS_OF = date(2023, 5, 17)


def test_value_surface_schema(tmp_path: Path) -> None:
    prices_path, marketstack_path, rates_path = _write_price_caches(tmp_path)
    payload = run_value_surface_controlled_prototype(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        output_root=tmp_path / "value_surface",
        as_of_date=TEST_AS_OF,
    )

    _assert_safety(payload)
    assert payload["report_type"] == "value_surface_controlled_prototype"
    assert payload["summary"]["value_surface_generated"] is True
    assert payload["summary"]["candidate_action_count"] >= payload["summary"]["configured_minimum"]
    assert payload["summary"]["horizon_count"] >= payload["summary"]["horizon_configured_minimum"]
    assert payload["summary"]["sample_quality_report_present"] is True
    assert (tmp_path / "value_surface" / "value_surface_controlled_prototype.json").exists()
    assert (tmp_path / "value_surface" / "value_surface_horizon_audit.json").exists()
    assert (tmp_path / "value_surface" / "value_surface_benchmark_comparison.json").exists()


def test_value_surface_no_future_horizon_selection(tmp_path: Path) -> None:
    prices_path, marketstack_path, rates_path = _write_price_caches(tmp_path)
    run_value_surface_controlled_prototype(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        output_root=tmp_path / "value_surface",
        as_of_date=TEST_AS_OF,
    )
    audit = _read_json(tmp_path / "value_surface" / "value_surface_horizon_audit.json")

    assert audit["summary"]["horizon_leakage_check_pass"] is True
    assert audit["summary"]["future_outcome_used_for_horizon_selection"] is False
    assert audit["future_outcome_policy"]["strategy_input_allowed"] is False


def test_value_surface_control_failure_blocks_recommendation(tmp_path: Path) -> None:
    prices_path, marketstack_path, rates_path = _write_price_caches(tmp_path)
    control_audit = _write_json(
        tmp_path / "control_audit_failed.json",
        {
            "summary": {
                "negative_control_promotion_count": 1,
                "future_leakage_trap_blocked": False,
            }
        },
    )
    payload = run_value_surface_controlled_prototype(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        control_audit_path=control_audit,
        output_root=tmp_path / "value_surface",
        as_of_date=TEST_AS_OF,
    )

    assert payload["status"] == "CONTROL_FAILED"
    assert payload["summary"]["value_surface_status"] == "CONTROL_FAILED"
    assert payload["summary"]["promotion_gate_allowed"] is False


def test_value_surface_benchmark_comparison_required(tmp_path: Path) -> None:
    prices_path, marketstack_path, rates_path = _write_price_caches(tmp_path)
    payload = run_value_surface_controlled_prototype(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        output_root=tmp_path / "value_surface",
        as_of_date=TEST_AS_OF,
    )

    assert payload["summary"]["benchmark_comparison_present"] is True
    assert payload["benchmark_comparison"]
    assert payload["summary"]["future_leakage_trap_blocked"] is True


def test_value_surface_promotion_false(tmp_path: Path) -> None:
    prices_path, marketstack_path, rates_path = _write_price_caches(tmp_path)
    payload = run_value_surface_controlled_prototype(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        output_root=tmp_path / "value_surface",
        as_of_date=TEST_AS_OF,
    )

    _assert_safety(payload)
    assert payload["summary"]["promotion_gate_allowed"] is False


def test_value_surface_controlled_expansion_schema(tmp_path: Path) -> None:
    prices_path, marketstack_path, rates_path = _write_price_caches(tmp_path)
    payload = run_value_surface_controlled_expansion(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        output_root=tmp_path / "value_expansion",
        as_of_date=TEST_AS_OF,
    )

    _assert_safety(payload)
    assert payload["report_type"] == "value_surface_controlled_expansion"
    assert payload["summary"]["value_surface_expansion_generated"] is True
    assert payload["summary"]["action_horizon_surface_present"] is True
    assert payload["summary"]["horizon_smoothness_audit_present"] is True
    assert payload["summary"]["horizon_leakage_check_pass"] is True
    assert payload["summary"]["by_cluster_breakdown_present"] is True
    assert payload["summary"]["gross_net_turnover_drawdown_present"] is True
    assert (tmp_path / "value_expansion" / "value_surface_controlled_expansion.json").exists()
    assert (
        tmp_path / "value_expansion" / "value_surface_expansion_horizon_smoothness_audit.json"
    ).exists()


def test_utility_boundary_audit_status_cap(tmp_path: Path) -> None:
    prices_path, marketstack_path, rates_path = _write_price_caches(tmp_path)
    run_value_surface_controlled_expansion(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        output_root=tmp_path / "value_expansion",
        as_of_date=TEST_AS_OF,
    )
    payload = run_utility_boundary_ranking_policy_audit(
        value_surface_expansion_path=(
            tmp_path / "value_expansion" / "value_surface_controlled_expansion.json"
        ),
        output_root=tmp_path / "utility",
    )

    _assert_safety(payload)
    assert payload["status"] == "SENSITIVITY_TESTED"
    assert payload["summary"]["validated_boundary_count"] == 0
    assert payload["summary"]["not_validated_utility_boundary"] is True
    assert payload["summary"]["profile_reversal_report_present"] is True
    assert payload["summary"]["pareto_frontier_present"] is True


def test_forward_evidence_maturity_tracker_append_only(tmp_path: Path) -> None:
    prices_path, marketstack_path, rates_path = _write_price_caches(tmp_path)
    expansion = run_value_surface_controlled_expansion(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        output_root=tmp_path / "value_expansion",
        as_of_date=TEST_AS_OF,
    )
    ledger_path = _write_forward_ledger(tmp_path)
    payload = run_forward_evidence_maturity_tracker(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        ledger_path=ledger_path,
        value_surface_expansion_path=Path(expansion["artifact_paths"]["json_path"]),
        output_root=tmp_path / "maturity",
        as_of_date=TEST_AS_OF,
    )

    _assert_safety(payload)
    assert payload["report_type"] == "forward_evidence_maturity_tracker"
    assert payload["summary"]["forward_maturity_tracker_generated"] is True
    assert payload["summary"]["future_outcomes_appended_only"] is True
    assert payload["summary"]["horizon_maturity_recorded"] is True
    assert {row["horizon"] for row in payload["horizon_maturity_summary"]} >= {
        "1d",
        "5d",
        "10d",
        "20d",
        "60d",
    }


def test_value_surface_warning_triage_decision_enum(tmp_path: Path) -> None:
    paths = _run_next_stage_inputs(tmp_path)
    payload = run_value_surface_warning_triage_review(
        value_surface_expansion_path=paths["value_expansion"],
        utility_boundary_audit_path=paths["utility"],
        forward_maturity_path=paths["maturity"],
        output_root=tmp_path / "warning_triage",
    )

    _assert_safety(payload)
    assert payload["report_type"] == "value_surface_warning_triage_review"
    assert payload["summary"]["warning_taxonomy_present"] is True
    assert payload["summary"]["decision_date_breakdown_present"] is True
    assert payload["summary"]["sample_concentration_present"] is True
    assert payload["summary"]["controlled_expansion_review_decision"] in {
        "CONTINUE",
        "WATCHLIST",
        "DATA_REQUIRED",
        "PAUSE",
        "KILL",
    }


def test_utility_ranking_robustness_remains_sensitivity_tested(tmp_path: Path) -> None:
    paths = _run_next_stage_inputs(tmp_path)
    payload = run_utility_ranking_robustness_pareto_audit(
        value_surface_expansion_path=paths["value_expansion"],
        utility_boundary_audit_path=paths["utility"],
        output_root=tmp_path / "utility_robustness",
    )

    _assert_safety(payload)
    assert payload["status"] == "SENSITIVITY_TESTED"
    assert payload["summary"]["not_validated_utility_boundary"] is True
    assert payload["summary"]["validated_boundary_count"] == 0
    assert payload["summary"]["pareto_frontier_stability_present"] is True
    assert payload["diagnostic_boundary_assessment"]["boundary_use"] == "diagnostic_only"


def test_forward_evidence_daily_continuity_tracker(tmp_path: Path) -> None:
    paths = _run_next_stage_inputs(tmp_path)
    prices_path, marketstack_path, rates_path = (
        paths["prices"],
        paths["marketstack"],
        paths["rates"],
    )
    payload = run_forward_evidence_daily_continuity_maturity_tracker(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        ledger_path=paths["ledger"],
        forward_maturity_path=paths["maturity"],
        value_surface_expansion_path=paths["value_expansion"],
        output_root=tmp_path / "continuity",
        as_of_date=TEST_AS_OF,
    )

    _assert_safety(payload)
    assert payload["report_type"] == "forward_evidence_daily_continuity_maturity_tracker"
    assert payload["summary"]["daily_continuity_checked"] is True
    assert payload["summary"]["append_only_integrity_pass"] is True
    assert payload["summary"]["horizon_maturity_recorded"] is True
    assert payload["summary"]["output_coverage_present"] is True


def test_regret_state_machine_schema(tmp_path: Path) -> None:
    prices_path, marketstack_path, rates_path = _write_price_caches(tmp_path)
    payload = run_regret_state_machine_controlled_prototype(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        output_root=tmp_path / "state_machine",
        as_of_date=TEST_AS_OF,
    )

    _assert_safety(payload)
    assert payload["report_type"] == "regret_state_machine_controlled_prototype"
    assert payload["summary"]["state_transition_explainable"] is True
    assert payload["state_transition_table"]
    assert (tmp_path / "state_machine" / "state_transition_casebook.json").exists()


def test_state_transition_has_explanation(tmp_path: Path) -> None:
    prices_path, marketstack_path, rates_path = _write_price_caches(tmp_path)
    payload = run_regret_state_machine_controlled_prototype(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        output_root=tmp_path / "state_machine",
        as_of_date=TEST_AS_OF,
    )

    assert all(row["explanation"] for row in payload["state_transition_table"])


def test_regret_type_mapping_required(tmp_path: Path) -> None:
    prices_path, marketstack_path, rates_path = _write_price_caches(tmp_path)
    payload = run_regret_state_machine_controlled_prototype(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        output_root=tmp_path / "state_machine",
        as_of_date=TEST_AS_OF,
    )

    assert payload["summary"]["regret_type_mapping_present"] is True
    assert payload["regret_type_coverage"]


def test_state_machine_turnover_guardrail_reported(tmp_path: Path) -> None:
    prices_path, marketstack_path, rates_path = _write_price_caches(tmp_path)
    payload = run_regret_state_machine_controlled_prototype(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        output_root=tmp_path / "state_machine",
        as_of_date=TEST_AS_OF,
    )

    assert payload["summary"]["turnover_guardrail_reported"] is True
    assert "turnover_not_worse_than_baseline_guardrail" in payload
    assert payload["summary"]["whipsaw_report_present"] is True


def test_state_machine_promotion_false(tmp_path: Path) -> None:
    prices_path, marketstack_path, rates_path = _write_price_caches(tmp_path)
    payload = run_regret_state_machine_controlled_prototype(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        output_root=tmp_path / "state_machine",
        as_of_date=TEST_AS_OF,
    )

    _assert_safety(payload)


def test_simple_strategy_selector_schema(tmp_path: Path) -> None:
    prices_path, marketstack_path, rates_path = _write_price_caches(tmp_path)
    payload = run_simple_strategy_selector_pilot(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        output_root=tmp_path / "simple",
        as_of_date=TEST_AS_OF,
    )

    _assert_safety(payload)
    assert payload["report_type"] == "simple_strategy_selector_pilot"
    assert payload["summary"]["simple_strategy_count"] >= payload["summary"]["configured_minimum"]
    assert payload["summary"]["selector_rules_present"] is True


def test_selector_compares_to_best_simple_strategy(tmp_path: Path) -> None:
    prices_path, marketstack_path, rates_path = _write_price_caches(tmp_path)
    payload = run_simple_strategy_selector_pilot(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        output_root=tmp_path / "simple",
        as_of_date=TEST_AS_OF,
    )

    assert payload["summary"]["best_simple_benchmark_comparison_present"] is True
    assert payload["best_single_strategy_comparison"]["best_single_strategy"]


def test_selector_overfit_warning_present(tmp_path: Path) -> None:
    prices_path, marketstack_path, rates_path = _write_price_caches(tmp_path)
    payload = run_simple_strategy_selector_pilot(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        output_root=tmp_path / "simple",
        as_of_date=TEST_AS_OF,
    )

    assert payload["summary"]["selector_overfit_warning_present"] is True
    assert payload["overfit_warning"]["not_validated_utility_boundary"] is True


def test_selector_promotion_false(tmp_path: Path) -> None:
    prices_path, marketstack_path, rates_path = _write_price_caches(tmp_path)
    payload = run_simple_strategy_selector_pilot(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        output_root=tmp_path / "simple",
        as_of_date=TEST_AS_OF,
    )

    _assert_safety(payload)


def test_gbdt_action_utility_schema(tmp_path: Path) -> None:
    prices_path, marketstack_path, rates_path = _write_price_caches(tmp_path)
    payload = run_gbdt_action_utility_baseline(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        output_root=tmp_path / "gbdt",
        as_of_date=TEST_AS_OF,
    )

    _assert_safety(payload)
    assert payload["report_type"] == "gbdt_action_utility_baseline"
    assert payload["summary"]["model_run_complete"] is True
    assert payload["action_utility_prediction"]


def test_gbdt_no_future_features(tmp_path: Path) -> None:
    prices_path, marketstack_path, rates_path = _write_price_caches(tmp_path)
    payload = run_gbdt_action_utility_baseline(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        output_root=tmp_path / "gbdt",
        as_of_date=TEST_AS_OF,
    )

    assert payload["summary"]["future_feature_violation_count"] == 0
    assert payload["future_feature_audit"]["future_outcome_role"] == "evaluation_label_only"
    assert payload["future_feature_audit"]["input_feature_policy"] == (
        "PIT_state_action_horizon_cost_only"
    )


def test_gbdt_negative_control_required(tmp_path: Path) -> None:
    prices_path, marketstack_path, rates_path = _write_price_caches(tmp_path)
    payload = run_gbdt_action_utility_baseline(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        output_root=tmp_path / "gbdt",
        as_of_date=TEST_AS_OF,
    )

    assert payload["summary"]["negative_control_pass"] is True
    assert (
        payload["negative_control_result"]["random_label_check"]["random_label_promoted"] is False
    )


def test_gbdt_feature_importance_present(tmp_path: Path) -> None:
    prices_path, marketstack_path, rates_path = _write_price_caches(tmp_path)
    payload = run_gbdt_action_utility_baseline(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        output_root=tmp_path / "gbdt",
        as_of_date=TEST_AS_OF,
    )

    assert payload["summary"]["feature_importance_report_present"] is True
    assert payload["feature_importance"]
    assert (tmp_path / "gbdt" / "gbdt_feature_importance.json").exists()


def test_gbdt_promotion_false(tmp_path: Path) -> None:
    prices_path, marketstack_path, rates_path = _write_price_caches(tmp_path)
    payload = run_gbdt_action_utility_baseline(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        output_root=tmp_path / "gbdt",
        as_of_date=TEST_AS_OF,
    )

    _assert_safety(payload)


def test_gbdt_pivot_review_design_only(tmp_path: Path) -> None:
    prices_path, marketstack_path, rates_path = _write_price_caches(tmp_path)
    gbdt = run_gbdt_action_utility_baseline(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        output_root=tmp_path / "gbdt",
        as_of_date=TEST_AS_OF,
    )
    payload = run_gbdt_pivot_review(
        gbdt_action_utility_path=Path(gbdt["artifact_paths"]["json_path"]),
        output_root=tmp_path / "gbdt_pivot",
    )

    _assert_safety(payload)
    assert payload["status"] == "PIVOT_REVIEW_READY"
    assert payload["summary"]["model_run_executed"] is False
    assert payload["summary"]["local_parameter_tuning_allowed"] is False
    assert payload["summary"]["pivot_option_count"] >= 4


def test_regret_casebook_expansion_gate_watchlist(tmp_path: Path) -> None:
    prices_path, marketstack_path, rates_path = _write_price_caches(tmp_path)
    state = run_regret_state_machine_controlled_prototype(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        output_root=tmp_path / "state",
        as_of_date=TEST_AS_OF,
    )
    expansion = run_value_surface_controlled_expansion(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        output_root=tmp_path / "value_expansion",
        as_of_date=TEST_AS_OF,
    )
    payload = run_regret_casebook_expansion_gate(
        regret_state_machine_path=Path(state["artifact_paths"]["json_path"]),
        state_transition_casebook_path=tmp_path / "state" / "state_transition_casebook.json",
        value_surface_expansion_path=Path(expansion["artifact_paths"]["json_path"]),
        output_root=tmp_path / "regret_gate",
    )

    _assert_safety(payload)
    assert payload["report_type"] == "regret_casebook_expansion_gate"
    assert payload["status"] == "WATCHLIST_NOT_READY"
    assert payload["summary"]["regret_casebook_expansion_allowed"] is False
    assert any(not row["passed"] for row in payload["activation_gate"])


def test_gbdt_pivot_direction_selection_no_training(tmp_path: Path) -> None:
    prices_path, marketstack_path, rates_path = _write_price_caches(tmp_path)
    gbdt = run_gbdt_action_utility_baseline(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        output_root=tmp_path / "gbdt",
        as_of_date=TEST_AS_OF,
    )
    pivot = run_gbdt_pivot_review(
        gbdt_action_utility_path=Path(gbdt["artifact_paths"]["json_path"]),
        output_root=tmp_path / "gbdt_pivot",
    )
    payload = run_gbdt_pivot_direction_selection(
        gbdt_pivot_review_path=Path(pivot["artifact_paths"]["json_path"]),
        output_root=tmp_path / "gbdt_selection",
    )

    _assert_safety(payload)
    assert payload["status"] == "PIVOT_DIRECTION_SELECTED"
    assert payload["summary"]["model_run_executed"] is False
    assert payload["summary"]["model_training_allowed"] is False
    assert payload["summary"]["selected_pivot_direction"] == "gbdt_value_surface_residual_model"
    assert all(row["minimum_viable_experiment"] for row in payload["candidate_directions"])


def test_regret_activation_inputs_stay_watchlist_when_incomplete(tmp_path: Path) -> None:
    prices_path, marketstack_path, rates_path = _write_price_caches(tmp_path)
    state = run_regret_state_machine_controlled_prototype(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        output_root=tmp_path / "state",
        as_of_date=TEST_AS_OF,
    )
    expansion = run_value_surface_controlled_expansion(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        output_root=tmp_path / "value_expansion",
        as_of_date=TEST_AS_OF,
    )
    gate = run_regret_casebook_expansion_gate(
        regret_state_machine_path=Path(state["artifact_paths"]["json_path"]),
        state_transition_casebook_path=tmp_path / "state" / "state_transition_casebook.json",
        value_surface_expansion_path=Path(expansion["artifact_paths"]["json_path"]),
        output_root=tmp_path / "regret_gate",
    )
    payload = run_regret_activation_inputs_from_value_surface_failures(
        value_surface_expansion_path=Path(expansion["artifact_paths"]["json_path"]),
        regret_casebook_expansion_gate_path=Path(gate["artifact_paths"]["json_path"]),
        output_root=tmp_path / "regret_inputs",
    )

    _assert_safety(payload)
    assert payload["report_type"] == "regret_activation_inputs_from_value_surface_failures"
    assert payload["status"] == "WATCHLIST_NOT_READY"
    assert payload["summary"]["regret_activation_ready"] is False
    assert payload["watchlist_decision"]["expand_state_machine_now"] is False


def test_controlled_strategy_batch_review_schema(tmp_path: Path) -> None:
    paths = _run_candidate_batch(tmp_path)
    review = run_controlled_strategy_batch_review(
        value_surface_path=paths["value_surface"],
        regret_state_machine_path=paths["state_machine"],
        simple_selector_path=paths["simple"],
        gbdt_action_utility_path=paths["gbdt"],
        output_root=tmp_path / "review",
    )

    _assert_safety(review)
    assert review["report_type"] == "controlled_strategy_batch_review"
    assert review["summary"]["all_candidates_have_decision"] is True
    assert review["status"] == "CONTROLLED_STRATEGY_RESEARCH_BATCH_1_COMPLETE"


def test_all_candidates_have_decision(tmp_path: Path) -> None:
    paths = _run_candidate_batch(tmp_path)
    review = run_controlled_strategy_batch_review(
        value_surface_path=paths["value_surface"],
        regret_state_machine_path=paths["state_machine"],
        simple_selector_path=paths["simple"],
        gbdt_action_utility_path=paths["gbdt"],
        output_root=tmp_path / "review",
    )

    decisions = {row["candidate_id"]: row["decision"] for row in review["candidate_decisions"]}
    assert decisions.keys() == {
        "value_surface",
        "regret_state_machine",
        "simple_strategy_selector",
        "gbdt_action_utility",
    }


def test_no_promotion_from_controlled_review(tmp_path: Path) -> None:
    paths = _run_candidate_batch(tmp_path)
    review = run_controlled_strategy_batch_review(
        value_surface_path=paths["value_surface"],
        regret_state_machine_path=paths["state_machine"],
        simple_selector_path=paths["simple"],
        gbdt_action_utility_path=paths["gbdt"],
        output_root=tmp_path / "review",
    )

    assert review["summary"]["no_candidate_promoted_without_policy"] is True
    assert all(row["promotion_gate_allowed"] is False for row in review["candidate_decisions"])


def test_kill_pause_pivot_enum(tmp_path: Path) -> None:
    paths = _run_candidate_batch(tmp_path)
    review = run_controlled_strategy_batch_review(
        value_surface_path=paths["value_surface"],
        regret_state_machine_path=paths["state_machine"],
        simple_selector_path=paths["simple"],
        gbdt_action_utility_path=paths["gbdt"],
        output_root=tmp_path / "review",
    )

    allowed = {"CONTINUE", "WATCHLIST", "DATA_REQUIRED", "PAUSE", "KILL", "PIVOT", "INFRA_REVIEW"}
    assert {row["decision"] for row in review["candidate_decisions"]} <= allowed
    assert review["summary"]["kill_pause_pivot_decisions_present"] is True


def test_next_batch_recommendation_present(tmp_path: Path) -> None:
    paths = _run_candidate_batch(tmp_path)
    review = run_controlled_strategy_batch_review(
        value_surface_path=paths["value_surface"],
        regret_state_machine_path=paths["state_machine"],
        simple_selector_path=paths["simple"],
        gbdt_action_utility_path=paths["gbdt"],
        output_root=tmp_path / "review",
    )

    assert review["summary"]["next_batch_recommendation_present"] is True
    assert review["next_batch_recommendation"]["promotion_gate_allowed"] is False


def test_controlled_strategy_batch_cli_smoke(tmp_path: Path) -> None:
    prices_path, marketstack_path, rates_path = _write_price_caches(tmp_path)
    ledger_path = _write_forward_ledger(tmp_path)
    runner = CliRunner()
    commands = [
        [
            "research",
            "strategies",
            "value-surface-controlled-prototype",
            "--prices-path",
            str(prices_path),
            "--marketstack-prices-path",
            str(marketstack_path),
            "--rates-path",
            str(rates_path),
            "--as-of",
            "2023-05-17",
            "--output-root",
            str(tmp_path / "cli_value"),
        ],
        [
            "research",
            "strategies",
            "value-surface-controlled-expansion",
            "--prices-path",
            str(prices_path),
            "--marketstack-prices-path",
            str(marketstack_path),
            "--rates-path",
            str(rates_path),
            "--as-of",
            "2023-05-17",
            "--output-root",
            str(tmp_path / "cli_value_expansion"),
        ],
        [
            "research",
            "strategies",
            "utility-boundary-ranking-policy-audit",
            "--value-surface-expansion",
            str(tmp_path / "cli_value_expansion" / "value_surface_controlled_expansion.json"),
            "--output-root",
            str(tmp_path / "cli_utility"),
        ],
        [
            "research",
            "strategies",
            "regret-state-machine-controlled-prototype",
            "--prices-path",
            str(prices_path),
            "--marketstack-prices-path",
            str(marketstack_path),
            "--rates-path",
            str(rates_path),
            "--as-of",
            "2023-05-17",
            "--output-root",
            str(tmp_path / "cli_state"),
        ],
        [
            "research",
            "strategies",
            "simple-strategy-selector-pilot",
            "--prices-path",
            str(prices_path),
            "--marketstack-prices-path",
            str(marketstack_path),
            "--rates-path",
            str(rates_path),
            "--as-of",
            "2023-05-17",
            "--output-root",
            str(tmp_path / "cli_simple"),
        ],
        [
            "research",
            "strategies",
            "gbdt-action-utility-baseline",
            "--prices-path",
            str(prices_path),
            "--marketstack-prices-path",
            str(marketstack_path),
            "--rates-path",
            str(rates_path),
            "--as-of",
            "2023-05-17",
            "--output-root",
            str(tmp_path / "cli_gbdt"),
        ],
        [
            "research",
            "strategies",
            "gbdt-pivot-review",
            "--gbdt-action-utility",
            str(tmp_path / "cli_gbdt" / "gbdt_action_utility_baseline.json"),
            "--output-root",
            str(tmp_path / "cli_gbdt"),
        ],
        [
            "research",
            "strategies",
            "gbdt-pivot-direction-selection",
            "--gbdt-pivot-review",
            str(tmp_path / "cli_gbdt" / "gbdt_pivot_review.json"),
            "--output-root",
            str(tmp_path / "cli_gbdt"),
        ],
        [
            "research",
            "strategies",
            "regret-casebook-expansion-gate",
            "--regret-state-machine",
            str(tmp_path / "cli_state" / "regret_state_machine_controlled_prototype.json"),
            "--state-transition-casebook",
            str(tmp_path / "cli_state" / "state_transition_casebook.json"),
            "--value-surface-expansion",
            str(tmp_path / "cli_value_expansion" / "value_surface_controlled_expansion.json"),
            "--output-root",
            str(tmp_path / "cli_state"),
        ],
        [
            "research",
            "strategies",
            "regret-activation-inputs-from-value-surface-failures",
            "--value-surface-expansion",
            str(tmp_path / "cli_value_expansion" / "value_surface_controlled_expansion.json"),
            "--regret-casebook-expansion-gate",
            str(tmp_path / "cli_state" / "regret_casebook_expansion_gate.json"),
            "--output-root",
            str(tmp_path / "cli_state"),
        ],
        [
            "forward-evidence",
            "maturity-tracker",
            "--prices-path",
            str(prices_path),
            "--marketstack-prices-path",
            str(marketstack_path),
            "--rates-path",
            str(rates_path),
            "--ledger-path",
            str(ledger_path),
            "--value-surface-expansion",
            str(tmp_path / "cli_value_expansion" / "value_surface_controlled_expansion.json"),
            "--as-of",
            "2023-05-17",
            "--output-root",
            str(tmp_path / "cli_maturity"),
        ],
        [
            "research",
            "strategies",
            "value-surface-warning-triage-review",
            "--value-surface-expansion",
            str(tmp_path / "cli_value_expansion" / "value_surface_controlled_expansion.json"),
            "--utility-boundary-audit",
            str(tmp_path / "cli_utility" / "utility_boundary_ranking_policy_audit.json"),
            "--forward-maturity",
            str(tmp_path / "cli_maturity" / "forward_evidence_maturity_tracker.json"),
            "--output-root",
            str(tmp_path / "cli_warning"),
        ],
        [
            "research",
            "strategies",
            "utility-ranking-robustness-pareto-audit",
            "--value-surface-expansion",
            str(tmp_path / "cli_value_expansion" / "value_surface_controlled_expansion.json"),
            "--utility-boundary-audit",
            str(tmp_path / "cli_utility" / "utility_boundary_ranking_policy_audit.json"),
            "--output-root",
            str(tmp_path / "cli_utility"),
        ],
        [
            "forward-evidence",
            "daily-continuity-maturity-tracker",
            "--prices-path",
            str(prices_path),
            "--marketstack-prices-path",
            str(marketstack_path),
            "--rates-path",
            str(rates_path),
            "--ledger-path",
            str(ledger_path),
            "--forward-maturity",
            str(tmp_path / "cli_maturity" / "forward_evidence_maturity_tracker.json"),
            "--value-surface-expansion",
            str(tmp_path / "cli_value_expansion" / "value_surface_controlled_expansion.json"),
            "--as-of",
            "2023-05-17",
            "--output-root",
            str(tmp_path / "cli_maturity"),
        ],
        [
            "research",
            "ops",
            "controlled-strategy-batch-review",
            "--value-surface",
            str(tmp_path / "cli_value" / "value_surface_controlled_prototype.json"),
            "--regret-state-machine",
            str(tmp_path / "cli_state" / "regret_state_machine_controlled_prototype.json"),
            "--simple-selector",
            str(tmp_path / "cli_simple" / "simple_strategy_selector_pilot.json"),
            "--gbdt-action-utility",
            str(tmp_path / "cli_gbdt" / "gbdt_action_utility_baseline.json"),
            "--output-root",
            str(tmp_path / "cli_review"),
        ],
    ]

    for command in commands:
        result = runner.invoke(app, command)
        assert result.exit_code == 0, result.output
        assert "production_effect=none" in result.output

    assert (tmp_path / "cli_review" / "controlled_strategy_batch_review.json").exists()
    assert (tmp_path / "cli_utility" / "utility_boundary_ranking_policy_audit.json").exists()
    assert (tmp_path / "cli_warning" / "value_surface_warning_triage_review.json").exists()
    assert (tmp_path / "cli_utility" / "utility_ranking_robustness_pareto_audit.json").exists()
    assert (tmp_path / "cli_maturity" / "forward_evidence_maturity_tracker.json").exists()
    assert (
        tmp_path / "cli_maturity" / "forward_evidence_daily_continuity_maturity_tracker.json"
    ).exists()
    assert (tmp_path / "cli_gbdt" / "gbdt_pivot_direction_selection.json").exists()
    assert (tmp_path / "cli_state" / "regret_casebook_expansion_gate.json").exists()
    assert (
        tmp_path / "cli_state" / "regret_activation_inputs_from_value_surface_failures.json"
    ).exists()


def test_controlled_strategy_batch_validation_tiers() -> None:
    test_path = "tests/test_controlled_strategy_batch.py"
    assert test_path in TIER_SPECS["fast-unit"].paths
    assert test_path in TIER_SPECS["contract-validation"].paths


def test_controlled_strategy_batch_registry_catalog_and_system_flow() -> None:
    registry = safe_load_yaml_path(PROJECT_ROOT / "config" / "report_registry.yaml")
    report_ids = {str(item.get("report_id")): item for item in registry["reports"]}
    for report_id in {
        "value_surface_controlled_prototype",
        "value_surface_controlled_expansion",
        "utility_boundary_ranking_policy_audit",
        "utility_ranking_robustness_pareto_audit",
        "forward_evidence_maturity_tracker",
        "forward_evidence_daily_continuity_maturity_tracker",
        "regret_state_machine_controlled_prototype",
        "simple_strategy_selector_pilot",
        "gbdt_action_utility_controlled_baseline",
        "gbdt_pivot_review",
        "gbdt_pivot_direction_selection",
        "regret_casebook_expansion_gate",
        "regret_activation_inputs_from_value_surface_failures",
        "value_surface_warning_triage_review",
        "controlled_strategy_batch_review",
    }:
        assert report_id in report_ids
        assert report_ids[report_id]["artifact_selection_policy"] == "latest_available"
        assert report_ids[report_id]["required_for_daily_reading"] is False

    catalog = (PROJECT_ROOT / "docs" / "artifact_catalog.md").read_text(encoding="utf-8")
    assert "value_surface_controlled_prototype.json/md" in catalog
    assert "value_surface_controlled_expansion.json/md" in catalog
    assert "value_surface_warning_triage_review.json/md" in catalog
    assert "forward_evidence_maturity_tracker.json/md" in catalog
    assert "forward_evidence_daily_continuity_maturity_tracker.json/md" in catalog
    assert "controlled_strategy_batch_review.json/md" in catalog
    assert "validated utility boundary" in catalog

    system_flow = (PROJECT_ROOT / "docs" / "system_flow.md").read_text(encoding="utf-8")
    assert "TRADING-770～774" in system_flow
    assert "TRADING-775～779" in system_flow
    assert "TRADING-780～784" in system_flow
    assert "aits research strategies value-surface-controlled-prototype" in system_flow
    assert "aits research strategies value-surface-controlled-expansion" in system_flow
    assert "aits research strategies value-surface-warning-triage-review" in system_flow
    assert "aits forward-evidence maturity-tracker" in system_flow
    assert "aits forward-evidence daily-continuity-maturity-tracker" in system_flow
    assert "CONTROLLED_STRATEGY_RESEARCH_BATCH_1_COMPLETE" in system_flow


def _run_next_stage_inputs(tmp_path: Path) -> dict[str, Path]:
    prices_path, marketstack_path, rates_path = _write_price_caches(tmp_path)
    expansion = run_value_surface_controlled_expansion(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        output_root=tmp_path / "value_expansion",
        as_of_date=TEST_AS_OF,
    )
    utility = run_utility_boundary_ranking_policy_audit(
        value_surface_expansion_path=Path(expansion["artifact_paths"]["json_path"]),
        output_root=tmp_path / "utility",
    )
    ledger_path = _write_forward_ledger(tmp_path)
    maturity = run_forward_evidence_maturity_tracker(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        ledger_path=ledger_path,
        value_surface_expansion_path=Path(expansion["artifact_paths"]["json_path"]),
        output_root=tmp_path / "maturity",
        as_of_date=TEST_AS_OF,
    )
    return {
        "prices": prices_path,
        "marketstack": marketstack_path,
        "rates": rates_path,
        "ledger": ledger_path,
        "value_expansion": Path(expansion["artifact_paths"]["json_path"]),
        "utility": Path(utility["artifact_paths"]["json_path"]),
        "maturity": Path(maturity["artifact_paths"]["json_path"]),
    }


def _run_candidate_batch(tmp_path: Path) -> dict[str, Path]:
    prices_path, marketstack_path, rates_path = _write_price_caches(tmp_path)
    value = run_value_surface_controlled_prototype(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        output_root=tmp_path / "value",
        as_of_date=TEST_AS_OF,
    )
    state = run_regret_state_machine_controlled_prototype(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        output_root=tmp_path / "state",
        as_of_date=TEST_AS_OF,
    )
    simple = run_simple_strategy_selector_pilot(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        output_root=tmp_path / "simple",
        as_of_date=TEST_AS_OF,
    )
    gbdt = run_gbdt_action_utility_baseline(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        output_root=tmp_path / "gbdt",
        as_of_date=TEST_AS_OF,
    )
    _assert_safety(value)
    _assert_safety(state)
    _assert_safety(simple)
    _assert_safety(gbdt)
    return {
        "value_surface": tmp_path / "value" / "value_surface_controlled_prototype.json",
        "state_machine": tmp_path / "state" / "regret_state_machine_controlled_prototype.json",
        "simple": tmp_path / "simple" / "simple_strategy_selector_pilot.json",
        "gbdt": tmp_path / "gbdt" / "gbdt_action_utility_baseline.json",
    }


def _write_price_caches(tmp_path: Path) -> tuple[Path, Path, Path]:
    universe = ["SPY", "QQQ", "SMH", "MSFT", "GOOGL", "NVDA", "AMD", "TSM"]
    dates = _business_dates(date(2022, 12, 1), 120)
    prices_path = tmp_path / "prices_daily.csv"
    marketstack_path = tmp_path / "prices_marketstack_daily.csv"
    rates_path = tmp_path / "rates_daily.csv"
    price_rows = ["date,ticker,open,high,low,close,adj_close,volume\n"]
    secondary_rows = ["date,ticker,open,high,low,close,adj_close,volume\n"]
    for ticker_index, ticker in enumerate(universe):
        base = 100.0 + ticker_index * 7.0
        for day_index, row_date in enumerate(dates):
            trend = 1.0 + day_index * 0.0015
            cycle = (day_index % 9 - 4) * 0.001
            drawdown = -0.05 if 45 <= day_index <= 55 else 0.0
            close = base * (trend + cycle + drawdown)
            row = (
                f"{row_date.isoformat()},{ticker},{close - 0.5:.4f},{close + 0.5:.4f},"
                f"{close - 1.0:.4f},{close:.4f},{close:.4f},{1000000 + ticker_index}\n"
            )
            price_rows.append(row)
            secondary_rows.append(row)
    rate_rows = ["date,series,value\n"]
    for day_index, row_date in enumerate(dates):
        rate_rows.append(f"{row_date.isoformat()},DGS10,{3.5 + day_index * 0.001:.4f}\n")
        rate_rows.append(f"{row_date.isoformat()},DGS2,{3.0 + day_index * 0.001:.4f}\n")
        rate_rows.append(f"{row_date.isoformat()},DTWEXBGS,{120.0 + day_index * 0.01:.4f}\n")
    prices_path.write_text("".join(price_rows), encoding="utf-8")
    marketstack_path.write_text("".join(secondary_rows), encoding="utf-8")
    rates_path.write_text("".join(rate_rows), encoding="utf-8")
    return prices_path, marketstack_path, rates_path


def _business_dates(start: date, count: int) -> list[date]:
    values: list[date] = []
    current = start
    while len(values) < count:
        if current.weekday() < 5:
            values.append(current)
        current += timedelta(days=1)
    return values


def _write_forward_ledger(tmp_path: Path) -> Path:
    archive_path = tmp_path / "forward_evidence_dry_run_2023-02-01.json"
    _write_json(
        archive_path,
        {
            "report_type": "forward_evidence_daily_dry_run_archive",
            "status": "PASS_WITH_WARNINGS",
            "as_of": "2023-02-01",
            "production_effect": "none",
            "broker_action": "none",
            "promotion_gate_allowed": False,
        },
    )
    ledger_path = tmp_path / "forward_evidence_dry_run_ledger.jsonl"
    rows = [
        {
            "archive_id": "forward_evidence_dry_run:2023-02-01",
            "archive_path": str(archive_path),
            "as_of": "2023-02-01",
            "outcome_append_only": True,
            "outcome_status": "pending",
            "production_effect": "none",
            "broker_action": "none",
            "promotion_gate_allowed": False,
            "paper_shadow_change_allowed": False,
            "production_weight_change_allowed": False,
        }
    ]
    ledger_path.write_text(
        "\n".join(json.dumps(row, ensure_ascii=False) for row in rows) + "\n",
        encoding="utf-8",
    )
    return ledger_path


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: dict[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def _assert_safety(payload: dict[str, Any]) -> None:
    assert payload["production_effect"] == "none"
    assert payload["broker_action"] == "none"
    assert payload["promotion_gate_allowed"] is False
    assert payload["paper_shadow_change_allowed"] is False
    assert payload["production_weight_change_allowed"] is False
    assert payload["lookahead_violation_count"] == 0
