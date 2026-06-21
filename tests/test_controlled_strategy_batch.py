from __future__ import annotations

import json
from datetime import date, timedelta
from pathlib import Path
from typing import Any

from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.controlled_strategy_batch import (
    run_ai_after_chatgpt_full_regime_attribution_review,
    run_benchmark_fallback_drawdown_guard_controlled_prototype,
    run_benchmark_first_tail_risk_policy_contract,
    run_conservative_horizon_risk_filter,
    run_controlled_strategy_batch_review,
    run_cost_aware_horizon_hysteresis,
    run_cost_turnover_aware_regime_conditioned_value_surface,
    run_forward_evidence_continuity_extension,
    run_forward_evidence_daily_continuity_maturity_tracker,
    run_forward_evidence_daily_continuity_review,
    run_forward_evidence_maturity_tracker,
    run_gbdt_action_utility_baseline,
    run_gbdt_pivot_direction_selection,
    run_gbdt_pivot_review,
    run_gbdt_residual_hypothesis_regime_conditioning,
    run_gbdt_residual_hypothesis_triage,
    run_gbdt_value_surface_residual_diagnostic_prototype,
    run_horizon_cliff_utility_ranking_stabilization_review,
    run_horizon_selector_controlled_prototype,
    run_horizon_selector_holdout_review,
    run_horizon_selector_problem_contract,
    run_long_horizon_quarantine_fallback_review,
    run_long_horizon_quarantine_selection_review,
    run_regime_conditioned_value_surface_controlled_review,
    run_regime_conditioned_value_surface_design,
    run_regime_conditioned_walk_forward_holdout,
    run_regime_horizon_loss_attribution_matrix,
    run_regret_activation_inputs_from_value_surface_failures,
    run_regret_casebook_activation_recheck,
    run_regret_casebook_expansion_gate,
    run_regret_state_machine_controlled_prototype,
    run_simple_strategy_selector_pilot,
    run_tail_loss_avoidance_classifier_prototype,
    run_tail_loss_guardrail_fallback_policy,
    run_tail_risk_policy_family_controlled_review,
    run_utility_boundary_ranking_policy_audit,
    run_utility_ranking_robustness_pareto_audit,
    run_value_surface_controlled_expansion,
    run_value_surface_controlled_prototype,
    run_value_surface_controlled_walk_forward_expansion,
    run_value_surface_direction_review,
    run_value_surface_failure_attribution,
    run_value_surface_policy_kill_diagnostic_downgrade,
    run_value_surface_utility_pareto_ranking_review,
    run_value_surface_v2_controlled_review,
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


def test_value_surface_controlled_walk_forward_expansion(tmp_path: Path) -> None:
    paths = _run_next_stage_inputs(tmp_path)
    warning = run_value_surface_warning_triage_review(
        value_surface_expansion_path=paths["value_expansion"],
        utility_boundary_audit_path=paths["utility"],
        forward_maturity_path=paths["maturity"],
        output_root=tmp_path / "warning_triage",
    )
    payload = run_value_surface_controlled_walk_forward_expansion(
        prices_path=paths["prices"],
        marketstack_prices_path=paths["marketstack"],
        rates_path=paths["rates"],
        value_surface_expansion_path=paths["value_expansion"],
        warning_triage_path=Path(warning["artifact_paths"]["json_path"]),
        output_root=tmp_path / "walk_forward",
        as_of_date=TEST_AS_OF,
    )

    _assert_safety(payload)
    assert payload["report_type"] == "value_surface_controlled_walk_forward_expansion"
    assert payload["summary"]["walk_forward_window_count"] >= 1
    assert payload["summary"]["decision_date_count"] >= 1
    assert payload["summary"]["benchmark_comparison_present"] is True
    assert payload["summary"]["negative_control_promotion_count"] == 0
    assert payload["summary"]["future_leakage_trap_blocked"] is True
    assert payload["summary"]["controlled_walk_forward_decision"] in {
        "CONTINUE",
        "WATCHLIST",
        "DATA_REQUIRED",
        "PAUSE",
        "KILL",
    }


def test_value_surface_utility_pareto_ranking_review(tmp_path: Path) -> None:
    paths = _run_next_stage_inputs(tmp_path)
    payload = run_value_surface_utility_pareto_ranking_review(
        value_surface_expansion_path=paths["value_expansion"],
        utility_boundary_audit_path=paths["utility"],
        output_root=tmp_path / "utility_pareto",
    )

    _assert_safety(payload)
    assert payload["report_type"] == "value_surface_utility_pareto_ranking_review"
    assert payload["status"] == "SENSITIVITY_TESTED"
    assert payload["summary"]["utility_profile_count"] >= 1
    assert payload["summary"]["pareto_candidate_count"] >= 1
    assert payload["summary"]["validated_boundary_count"] == 0
    assert payload["summary"]["not_validated_utility_boundary"] is True
    assert "horizon_cliff_count" in payload["summary"]


def test_forward_evidence_daily_continuity_review(tmp_path: Path) -> None:
    paths = _run_next_stage_inputs(tmp_path)
    payload = run_forward_evidence_daily_continuity_review(
        prices_path=paths["prices"],
        marketstack_prices_path=paths["marketstack"],
        rates_path=paths["rates"],
        ledger_path=paths["ledger"],
        value_surface_expansion_path=paths["value_expansion"],
        output_root=tmp_path / "continuity_review",
        as_of_date=TEST_AS_OF,
    )

    _assert_safety(payload)
    assert payload["report_type"] == "forward_evidence_daily_continuity_review"
    assert payload["summary"]["ledger_event_count"] == 1
    assert payload["summary"]["append_only_integrity_pass"] is True
    assert payload["summary"]["horizon_maturity_recorded"] is True
    assert payload["summary"]["output_coverage_present"] is True


def test_gbdt_value_surface_residual_diagnostic_no_strategy(tmp_path: Path) -> None:
    paths = _run_next_stage_inputs(tmp_path)
    payload = run_gbdt_value_surface_residual_diagnostic_prototype(
        value_surface_expansion_path=paths["value_expansion"],
        output_root=tmp_path / "gbdt_residual",
    )

    _assert_safety(payload)
    assert payload["report_type"] == "gbdt_value_surface_residual_diagnostic_prototype"
    assert payload["summary"]["residual_case_count"] > 0
    assert payload["summary"]["feature_importance_present"] is True
    assert payload["summary"]["strategy_signal_generated"] is False
    assert payload["diagnostic_boundary"]["direct_action_utility_prediction"] is False


def test_regret_casebook_activation_recheck_stays_watchlist(tmp_path: Path) -> None:
    paths = _run_next_stage_inputs(tmp_path)
    activation = run_regret_activation_inputs_from_value_surface_failures(
        value_surface_expansion_path=paths["value_expansion"],
        regret_casebook_expansion_gate_path=tmp_path / "missing_regret_gate.json",
        output_root=tmp_path / "regret",
    )
    payload = run_regret_casebook_activation_recheck(
        value_surface_expansion_path=paths["value_expansion"],
        regret_activation_inputs_path=Path(activation["artifact_paths"]["json_path"]),
        regret_casebook_expansion_gate_path=tmp_path / "missing_regret_gate.json",
        output_root=tmp_path / "regret",
    )

    _assert_safety(payload)
    assert payload["report_type"] == "regret_casebook_activation_recheck"
    assert payload["status"] == "WATCHLIST_NOT_READY"
    assert payload["summary"]["regret_casebook_expansion_allowed"] is False
    assert payload["activation_recheck_decision"]["expansion_executed"] is False


def test_value_surface_failure_attribution_explains_loss_delta(tmp_path: Path) -> None:
    paths = _run_direction_review_inputs(tmp_path)
    payload = run_value_surface_failure_attribution(
        prices_path=paths["prices"],
        marketstack_prices_path=paths["marketstack"],
        rates_path=paths["rates"],
        value_surface_expansion_path=paths["value_expansion"],
        walk_forward_path=paths["walk_forward"],
        output_root=tmp_path / "failure_attribution",
        as_of_date=TEST_AS_OF,
    )

    _assert_safety(payload)
    assert payload["report_type"] == "value_surface_failure_attribution"
    assert "winning_case_average_delta" in payload["summary"]
    assert "losing_case_average_delta" in payload["summary"]
    assert payload["top_losing_cases"]
    assert payload["tail_loss_contribution"]["tail_loss_share"] >= 0
    assert payload["benchmark_relative_downside_attribution"]["promotion_gate_allowed"] is False


def test_horizon_cliff_stabilization_review_diagnostic_only(tmp_path: Path) -> None:
    paths = _run_direction_review_inputs(tmp_path)
    payload = run_horizon_cliff_utility_ranking_stabilization_review(
        value_surface_expansion_path=paths["value_expansion"],
        utility_pareto_ranking_path=paths["utility_pareto"],
        output_root=tmp_path / "stabilization",
    )

    _assert_safety(payload)
    assert payload["report_type"] == "horizon_cliff_utility_ranking_stabilization_review"
    assert payload["status"] == "SENSITIVITY_TESTED"
    assert payload["summary"]["validated_boundary_count"] == 0
    assert payload["summary"]["not_validated_utility_boundary"] is True
    assert "ranking_jump_count" in payload["summary"]


def test_gbdt_residual_hypothesis_triage_no_strategy(tmp_path: Path) -> None:
    paths = _run_direction_review_inputs(tmp_path)
    payload = run_gbdt_residual_hypothesis_triage(
        value_surface_expansion_path=paths["value_expansion"],
        residual_diagnostic_path=paths["residual_diagnostic"],
        output_root=tmp_path / "residual_triage",
    )

    _assert_safety(payload)
    assert payload["report_type"] == "gbdt_residual_hypothesis_triage"
    assert payload["summary"]["residual_case_count"] > 0
    assert payload["summary"]["strategy_signal_generated"] is False
    assert payload["repair_rule_candidates"]
    assert payload["diagnostic_boundary"]["direct_action_policy_generated"] is False


def test_forward_evidence_continuity_extension(tmp_path: Path) -> None:
    paths = _run_direction_review_inputs(tmp_path)
    payload = run_forward_evidence_continuity_extension(
        prices_path=paths["prices"],
        marketstack_prices_path=paths["marketstack"],
        rates_path=paths["rates"],
        ledger_path=paths["ledger"],
        value_surface_expansion_path=paths["value_expansion"],
        output_root=tmp_path / "continuity_extension",
        as_of_date=TEST_AS_OF,
    )

    _assert_safety(payload)
    assert payload["report_type"] == "forward_evidence_continuity_extension"
    assert payload["summary"]["ledger_event_count"] == 1
    assert payload["summary"]["append_only_integrity_pass"] is True
    assert payload["forward_evidence_scope"]["paper_shadow_ready"] is False


def test_value_surface_direction_review_does_not_default_continue(tmp_path: Path) -> None:
    paths = _run_direction_review_inputs(tmp_path)
    failure = run_value_surface_failure_attribution(
        prices_path=paths["prices"],
        marketstack_prices_path=paths["marketstack"],
        rates_path=paths["rates"],
        value_surface_expansion_path=paths["value_expansion"],
        walk_forward_path=paths["walk_forward"],
        output_root=tmp_path / "direction",
        as_of_date=TEST_AS_OF,
    )
    horizon = run_horizon_cliff_utility_ranking_stabilization_review(
        value_surface_expansion_path=paths["value_expansion"],
        utility_pareto_ranking_path=paths["utility_pareto"],
        output_root=tmp_path / "direction",
    )
    residual = run_gbdt_residual_hypothesis_triage(
        value_surface_expansion_path=paths["value_expansion"],
        residual_diagnostic_path=paths["residual_diagnostic"],
        output_root=tmp_path / "direction",
    )
    forward = run_forward_evidence_continuity_extension(
        prices_path=paths["prices"],
        marketstack_prices_path=paths["marketstack"],
        rates_path=paths["rates"],
        ledger_path=paths["ledger"],
        value_surface_expansion_path=paths["value_expansion"],
        output_root=tmp_path / "direction",
        as_of_date=TEST_AS_OF,
    )
    payload = run_value_surface_direction_review(
        failure_attribution_path=Path(failure["artifact_paths"]["json_path"]),
        horizon_stabilization_path=Path(horizon["artifact_paths"]["json_path"]),
        residual_triage_path=Path(residual["artifact_paths"]["json_path"]),
        forward_continuity_extension_path=Path(forward["artifact_paths"]["json_path"]),
        walk_forward_path=paths["walk_forward"],
        output_root=tmp_path / "direction",
    )

    _assert_safety(payload)
    assert payload["report_type"] == "value_surface_direction_review"
    assert payload["summary"]["do_not_default_continue"] is True
    assert payload["summary"]["direction_decision"] in {
        "CONTINUE_LOCAL_FIX",
        "WATCHLIST",
        "PIVOT_TO_REGIME_CONDITIONED_VALUE_SURFACE",
        "PIVOT_TO_PARETO_FRONTIER_POLICY",
        "PIVOT_TO_TAIL_RISK_FILTER",
        "KILL_CURRENT_VALUE_SURFACE_VERSION",
    }


def test_regime_conditioned_value_surface_design_protocol(tmp_path: Path) -> None:
    paths = _run_regime_conditioning_inputs(tmp_path)
    payload = run_regime_conditioned_value_surface_design(
        failure_attribution_path=paths["failure"],
        horizon_stabilization_path=paths["horizon"],
        residual_triage_path=paths["residual_triage"],
        direction_review_path=paths["direction"],
        output_root=tmp_path / "regime_design",
    )

    _assert_safety(payload)
    assert payload["report_type"] == "regime_conditioned_value_surface_design"
    assert payload["summary"]["regime_variable_count"] > 0
    assert payload["regimes_keep_value_surface"]
    assert payload["regimes_fallback_to_benchmark"]
    assert payload["controlled_only_validation_plan"]


def test_tail_loss_guardrail_fallback_policy_compares_variants(tmp_path: Path) -> None:
    paths = _run_regime_conditioning_inputs(tmp_path)
    design = run_regime_conditioned_value_surface_design(
        failure_attribution_path=paths["failure"],
        horizon_stabilization_path=paths["horizon"],
        residual_triage_path=paths["residual_triage"],
        direction_review_path=paths["direction"],
        output_root=tmp_path / "guardrail",
    )
    payload = run_tail_loss_guardrail_fallback_policy(
        value_surface_expansion_path=paths["value_expansion"],
        failure_attribution_path=paths["failure"],
        horizon_stabilization_path=paths["horizon"],
        design_path=Path(design["artifact_paths"]["json_path"]),
        output_root=tmp_path / "guardrail",
    )

    _assert_safety(payload)
    assert payload["report_type"] == "tail_loss_guardrail_fallback_policy"
    variant_ids = {row["variant_id"] for row in payload["variant_metrics"]}
    assert {
        "original_value_surface",
        "regime_conditioned_value_surface",
        "tail_loss_guarded_value_surface",
        "benchmark_fallback_value_surface",
    }.issubset(variant_ids)
    assert "mean_delta_vs_benchmark" in payload["variant_metrics"][0]
    assert payload["guardrail_diagnostic_boundary"]["retrospective_ablation_only"] is True


def test_regime_horizon_loss_attribution_matrix(tmp_path: Path) -> None:
    paths = _run_regime_conditioning_inputs(tmp_path)
    payload = run_regime_horizon_loss_attribution_matrix(
        value_surface_expansion_path=paths["value_expansion"],
        failure_attribution_path=paths["failure"],
        output_root=tmp_path / "loss_matrix",
    )

    _assert_safety(payload)
    assert payload["report_type"] == "regime_horizon_loss_attribution_matrix"
    assert payload["summary"]["losing_case_count"] >= 0
    assert "max_loss_concentration_group" in payload["summary"]
    assert payload["loss_by_regime"]["summary"]["promotion_gate_allowed"] is False
    assert payload["loss_by_utility_profile"]["groups"] is not None


def test_gbdt_residual_regime_conditioning_no_strategy(tmp_path: Path) -> None:
    paths = _run_regime_conditioning_inputs(tmp_path)
    payload = run_gbdt_residual_hypothesis_regime_conditioning(
        value_surface_expansion_path=paths["value_expansion"],
        residual_triage_path=paths["residual_triage"],
        output_root=tmp_path / "residual_regime",
    )

    _assert_safety(payload)
    assert payload["report_type"] == "gbdt_residual_hypothesis_regime_conditioning"
    assert payload["summary"]["residual_case_count"] > 0
    assert payload["summary"]["strategy_signal_generated"] is False
    assert payload["top_residual_features"]
    assert payload["diagnostic_boundary"]["direct_action_policy_generated"] is False


def test_regime_conditioned_controlled_review_decision_enum(tmp_path: Path) -> None:
    paths = _run_regime_conditioning_inputs(tmp_path)
    design = run_regime_conditioned_value_surface_design(
        failure_attribution_path=paths["failure"],
        horizon_stabilization_path=paths["horizon"],
        residual_triage_path=paths["residual_triage"],
        direction_review_path=paths["direction"],
        output_root=tmp_path / "controlled_review",
    )
    guardrail = run_tail_loss_guardrail_fallback_policy(
        value_surface_expansion_path=paths["value_expansion"],
        failure_attribution_path=paths["failure"],
        horizon_stabilization_path=paths["horizon"],
        design_path=Path(design["artifact_paths"]["json_path"]),
        output_root=tmp_path / "controlled_review",
    )
    matrix = run_regime_horizon_loss_attribution_matrix(
        value_surface_expansion_path=paths["value_expansion"],
        failure_attribution_path=paths["failure"],
        output_root=tmp_path / "controlled_review",
    )
    residual = run_gbdt_residual_hypothesis_regime_conditioning(
        value_surface_expansion_path=paths["value_expansion"],
        residual_triage_path=paths["residual_triage"],
        output_root=tmp_path / "controlled_review",
    )
    payload = run_regime_conditioned_value_surface_controlled_review(
        design_path=Path(design["artifact_paths"]["json_path"]),
        guardrail_policy_path=Path(guardrail["artifact_paths"]["json_path"]),
        loss_matrix_path=Path(matrix["artifact_paths"]["json_path"]),
        residual_regime_path=Path(residual["artifact_paths"]["json_path"]),
        output_root=tmp_path / "controlled_review",
    )

    _assert_safety(payload)
    assert payload["report_type"] == "regime_conditioned_value_surface_controlled_review"
    assert payload["summary"]["controlled_review_decision"] in {
        "CONTINUE",
        "WATCHLIST",
        "KILL_CURRENT_VALUE_SURFACE",
        "PIVOT_TO_TAIL_RISK_POLICY",
        "PIVOT_TO_BENCHMARK_FALLBACK",
        "DATA_REQUIRED",
    }
    assert payload["review_decision"]["promotion_gate_allowed"] is False


def test_cost_turnover_aware_regime_conditioned_value_surface(tmp_path: Path) -> None:
    paths = _run_value_surface_v2_inputs(tmp_path)
    payload = run_cost_turnover_aware_regime_conditioned_value_surface(
        value_surface_expansion_path=paths["value_expansion"],
        failure_attribution_path=paths["failure"],
        horizon_stabilization_path=paths["horizon"],
        design_path=paths["design"],
        guardrail_policy_path=paths["guardrail"],
        output_root=tmp_path / "cost_turnover",
    )

    _assert_safety(payload)
    assert payload["report_type"] == "cost_turnover_aware_regime_conditioned_value_surface"
    variant_ids = {row["variant_id"] for row in payload["variant_metrics"]}
    assert "regime_conditioned_turnover_penalty" in variant_ids
    assert "regime_conditioned_action_hysteresis" in variant_ids
    assert "regime_conditioned_no_trade_band" in variant_ids
    assert payload["variant_metrics"][0]["action_flip_count"] >= 0
    assert "turnover_delta" in payload["variant_metrics"][1]
    assert payload["v2_score_policy"]["policy_source"].endswith(
        "cost_turnover_aware_regime_conditioned_value_surface"
    )
    assert payload["diagnostic_boundary"]["production_execution_rule"] is False


def test_long_horizon_quarantine_selection_review(tmp_path: Path) -> None:
    paths = _run_value_surface_v2_inputs(tmp_path)
    cost = run_cost_turnover_aware_regime_conditioned_value_surface(
        value_surface_expansion_path=paths["value_expansion"],
        failure_attribution_path=paths["failure"],
        horizon_stabilization_path=paths["horizon"],
        design_path=paths["design"],
        guardrail_policy_path=paths["guardrail"],
        output_root=tmp_path / "long_horizon",
    )
    payload = run_long_horizon_quarantine_selection_review(
        value_surface_expansion_path=paths["value_expansion"],
        failure_attribution_path=paths["failure"],
        horizon_stabilization_path=paths["horizon"],
        cost_turnover_path=Path(cost["artifact_paths"]["json_path"]),
        output_root=tmp_path / "long_horizon",
    )

    _assert_safety(payload)
    assert payload["report_type"] == "long_horizon_quarantine_selection_review"
    assert {"20d", "60d"}.issubset(set(payload["reviewed_horizons"]))
    assert payload["horizon_loss_matrix"]["summary"]["promotion_gate_allowed"] is False
    assert payload["disable_vs_downgrade_comparison"]


def test_ai_after_chatgpt_full_regime_attribution_review(tmp_path: Path) -> None:
    paths = _run_value_surface_v2_inputs(tmp_path)
    matrix = run_regime_horizon_loss_attribution_matrix(
        value_surface_expansion_path=paths["value_expansion"],
        failure_attribution_path=paths["failure"],
        output_root=tmp_path / "ai_regime",
    )
    payload = run_ai_after_chatgpt_full_regime_attribution_review(
        value_surface_expansion_path=paths["value_expansion"],
        failure_attribution_path=paths["failure"],
        loss_matrix_path=Path(matrix["artifact_paths"]["json_path"]),
        output_root=tmp_path / "ai_regime",
    )

    _assert_safety(payload)
    assert payload["report_type"] == "ai_after_chatgpt_full_regime_attribution_review"
    assert payload["summary"]["target_regime"] == "ai_after_chatgpt_full"
    assert "benchmark_more_stable" in payload["benchmark_stability"]
    assert payload["candidate_repairs"]


def test_regime_conditioned_walk_forward_holdout(tmp_path: Path) -> None:
    paths = _run_value_surface_v2_full_inputs(tmp_path)
    payload = run_regime_conditioned_walk_forward_holdout(
        value_surface_expansion_path=paths["value_expansion"],
        failure_attribution_path=paths["failure"],
        horizon_stabilization_path=paths["horizon"],
        design_path=paths["design"],
        cost_turnover_path=paths["cost_turnover"],
        horizon_quarantine_path=paths["long_horizon"],
        regime_attribution_path=paths["ai_regime"],
        output_root=tmp_path / "holdout",
    )

    _assert_safety(payload)
    assert payload["report_type"] == "regime_conditioned_walk_forward_holdout"
    assert payload["summary"]["holdout_case_count"] >= 0
    assert "overfit_risk" in payload["summary"]
    assert payload["leave_one_horizon_out"] is not None


def test_value_surface_v2_controlled_review_decision_enum(tmp_path: Path) -> None:
    paths = _run_value_surface_v2_full_inputs(tmp_path)
    holdout = run_regime_conditioned_walk_forward_holdout(
        value_surface_expansion_path=paths["value_expansion"],
        failure_attribution_path=paths["failure"],
        horizon_stabilization_path=paths["horizon"],
        design_path=paths["design"],
        cost_turnover_path=paths["cost_turnover"],
        horizon_quarantine_path=paths["long_horizon"],
        regime_attribution_path=paths["ai_regime"],
        output_root=tmp_path / "v2_review",
    )
    payload = run_value_surface_v2_controlled_review(
        cost_turnover_path=paths["cost_turnover"],
        horizon_quarantine_path=paths["long_horizon"],
        regime_attribution_path=paths["ai_regime"],
        holdout_path=Path(holdout["artifact_paths"]["json_path"]),
        output_root=tmp_path / "v2_review",
    )

    _assert_safety(payload)
    assert payload["report_type"] == "value_surface_v2_controlled_review"
    assert payload["summary"]["value_surface_v2_decision"] in {
        "CONTINUE_TO_LARGER_CONTROLLED_RESEARCH",
        "WATCHLIST",
        "PIVOT_TO_TAIL_RISK_POLICY",
        "PIVOT_TO_HORIZON_SELECTOR",
        "KILL_VALUE_SURFACE",
        "DATA_REQUIRED",
    }
    assert payload["review_decision"]["promotion_gate_allowed"] is False


def test_horizon_selector_problem_contract(tmp_path: Path) -> None:
    paths = _run_horizon_selector_inputs(tmp_path)
    payload = run_horizon_selector_problem_contract(
        v2_review_path=paths["v2_review"],
        long_horizon_review_path=paths["long_horizon"],
        output_root=tmp_path / "horizon_contract_check",
    )

    _assert_safety(payload)
    assert payload["report_type"] == "horizon_selector_problem_contract"
    assert {row["horizon"] for row in payload["candidate_horizons"]} == {
        "1d",
        "5d",
        "10d",
        "20d",
        "60d",
    }
    assert payload["target_horizon_is_holding_commitment"] is False
    assert payload["selector_output"]["review_interval"] == "daily"
    assert "invalidation_condition" in {row["field"] for row in payload["selector_output_schema"]}


def test_long_horizon_quarantine_fallback_review(tmp_path: Path) -> None:
    paths = _run_horizon_selector_inputs(tmp_path)
    payload = run_long_horizon_quarantine_fallback_review(
        value_surface_expansion_path=paths["value_expansion"],
        contract_path=paths["contract"],
        v2_review_path=paths["v2_review"],
        output_root=tmp_path / "horizon_fallback_check",
    )

    _assert_safety(payload)
    assert payload["report_type"] == "long_horizon_quarantine_fallback_review"
    variant_ids = {row["variant_id"] for row in payload["variant_metrics"]}
    assert "disable_60d" in variant_ids
    assert "disable_20d_60d" in variant_ids
    assert "long_horizon_fallback_to_5d_10d" in variant_ids
    assert "holdout_pass_rate" in payload["variant_metrics"][0]
    assert payload["review_summary"]["promotion_gate_allowed"] is False


def test_horizon_selector_controlled_prototype(tmp_path: Path) -> None:
    paths = _run_horizon_selector_inputs(tmp_path)
    payload = run_horizon_selector_controlled_prototype(
        value_surface_expansion_path=paths["value_expansion"],
        contract_path=paths["contract"],
        fallback_review_path=paths["fallback_review"],
        horizon_stabilization_path=paths["horizon"],
        output_root=tmp_path / "horizon_prototype_check",
    )

    _assert_safety(payload)
    assert payload["report_type"] == "horizon_selector_controlled_prototype"
    assert payload["summary"]["model_run_executed"] is False
    assert payload["horizon_decision_by_date"]
    assert payload["prototype_summary"]["fallback_count"] >= 0
    assert payload["selector_metric"]["promotion_gate_allowed"] is False


def test_cost_aware_horizon_hysteresis(tmp_path: Path) -> None:
    paths = _run_horizon_selector_full_inputs(tmp_path)
    payload = run_cost_aware_horizon_hysteresis(
        value_surface_expansion_path=paths["value_expansion"],
        contract_path=paths["contract"],
        prototype_path=paths["prototype"],
        horizon_stabilization_path=paths["horizon"],
        output_root=tmp_path / "horizon_hysteresis_check",
    )

    _assert_safety(payload)
    assert payload["report_type"] == "cost_aware_horizon_hysteresis"
    assert "horizon_switch_count" in payload["summary"]
    assert "utility_lost_to_hysteresis" in payload["summary"]
    assert payload["hysteresis_metric"]["promotion_gate_allowed"] is False


def test_horizon_selector_holdout_review_decision_enum(tmp_path: Path) -> None:
    paths = _run_horizon_selector_full_inputs(tmp_path)
    payload = run_horizon_selector_holdout_review(
        value_surface_expansion_path=paths["value_expansion"],
        contract_path=paths["contract"],
        fallback_review_path=paths["fallback_review"],
        prototype_path=paths["prototype"],
        hysteresis_path=paths["hysteresis"],
        horizon_stabilization_path=paths["horizon"],
        output_root=tmp_path / "horizon_holdout_check",
    )

    _assert_safety(payload)
    assert payload["report_type"] == "horizon_selector_holdout_review"
    assert payload["summary"]["horizon_selector_decision"] in {
        "CONTINUE",
        "WATCHLIST",
        "PIVOT_TO_TAIL_RISK_POLICY",
        "PIVOT_TO_BENCHMARK_SELECTOR",
        "KILL_VALUE_SURFACE_AS_ACTION_POLICY",
        "DATA_REQUIRED",
    }
    assert payload["review_decision"]["promotion_gate_allowed"] is False


def test_value_surface_policy_kill_diagnostic_downgrade(tmp_path: Path) -> None:
    paths = _run_tail_risk_policy_inputs(tmp_path)
    payload = run_value_surface_policy_kill_diagnostic_downgrade(
        horizon_selector_holdout_path=paths["horizon_holdout"],
        v2_review_path=paths["v2_review"],
        output_root=tmp_path / "tail_policy_kill_check",
    )

    _assert_safety(payload)
    assert payload["report_type"] == "value_surface_policy_kill_diagnostic_downgrade"
    assert payload["summary"]["action_policy_allowed"] is False
    assert payload["summary"]["promotion_gate_allowed"] is False
    assert payload["policy_downgrade"]["allowed_uses"] == [
        "diagnostic",
        "residual_analysis",
        "tail_loss_attribution",
        "horizon_risk_signal",
        "benchmark_fallback_trigger",
    ]
    assert "direct_action_policy" in payload["policy_downgrade"]["disallowed_uses"]


def test_benchmark_first_tail_risk_policy_contract(tmp_path: Path) -> None:
    paths = _run_tail_risk_policy_inputs(tmp_path)
    payload = run_benchmark_first_tail_risk_policy_contract(
        policy_kill_path=paths["policy_kill"],
        output_root=tmp_path / "tail_contract_check",
    )

    _assert_safety(payload)
    assert payload["report_type"] == "benchmark_first_tail_risk_policy_contract"
    contract = payload["policy_contract"]
    assert contract["base_policy"] == "benchmark_or_simple_trend_static_allocation"
    assert "risk_downshift" in contract["allowed_deviation"]
    assert contract["fallback_policy"] == "benchmark_first"
    assert contract["review_interval"] == "daily"
    assert contract["direct_position_policy"] is False


def test_tail_loss_avoidance_classifier_prototype_is_gate_only(tmp_path: Path) -> None:
    paths = _run_tail_risk_policy_inputs(tmp_path)
    payload = run_tail_loss_avoidance_classifier_prototype(
        value_surface_expansion_path=paths["value_expansion"],
        policy_kill_path=paths["policy_kill"],
        output_root=tmp_path / "tail_classifier_check",
    )

    _assert_safety(payload)
    assert payload["report_type"] == "tail_loss_avoidance_classifier_prototype"
    assert payload["summary"]["strategy_signal_generated"] is False
    assert payload["gate_semantics"]["direct_position_policy"] is False
    assert payload["summary"]["large_loss_case_count"] >= 0
    assert payload["summary"]["tail_loss_case_count"] >= 0
    assert payload["summary"]["benchmark_underperformance_case_count"] >= 0
    assert payload["summary"]["long_horizon_failure_case_count"] >= 0


def test_conservative_horizon_risk_filter_status(tmp_path: Path) -> None:
    paths = _run_tail_risk_policy_inputs(tmp_path)
    payload = run_conservative_horizon_risk_filter(
        value_surface_expansion_path=paths["value_expansion"],
        classifier_path=paths["classifier"],
        contract_path=paths["contract"],
        output_root=tmp_path / "horizon_filter_check",
    )

    _assert_safety(payload)
    assert payload["report_type"] == "conservative_horizon_risk_filter"
    status = {row["horizon"]: row["status"] for row in payload["horizon_status"]}
    assert status["1d"] == "ALLOWED"
    assert status["5d"] == "ALLOWED"
    assert status["10d"] == "ALLOWED"
    assert status["20d"] == "QUARANTINED"
    assert status["60d"] == "FALLBACK_ONLY"
    assert payload["selector_mode"] == "risk_filter_not_optimal_horizon_selector"


def test_benchmark_fallback_drawdown_guard_controlled_prototype(tmp_path: Path) -> None:
    paths = _run_tail_risk_policy_full_inputs(tmp_path)
    payload = run_benchmark_fallback_drawdown_guard_controlled_prototype(
        value_surface_expansion_path=paths["value_expansion"],
        classifier_path=paths["classifier"],
        horizon_filter_path=paths["horizon_filter"],
        contract_path=paths["contract"],
        output_root=tmp_path / "fallback_guard_check",
    )

    _assert_safety(payload)
    assert payload["report_type"] == "benchmark_fallback_drawdown_guard_controlled_prototype"
    variant_ids = {row["variant_id"] for row in payload["variant_metrics"]}
    assert "benchmark_first_baseline" in variant_ids
    assert "tail_risk_benchmark_fallback" in variant_ids
    assert "drawdown_guard_cash_fallback" in variant_ids
    assert "mean_delta_vs_benchmark" in payload["variant_metrics"][0]
    assert "tail_loss_contribution" in payload["variant_metrics"][0]
    assert "max_drawdown" in payload["variant_metrics"][0]
    assert "beat_rate_retention" in payload["variant_metrics"][0]


def test_tail_risk_policy_family_controlled_review_decision_enum(tmp_path: Path) -> None:
    paths = _run_tail_risk_policy_full_inputs(tmp_path)
    payload = run_tail_risk_policy_family_controlled_review(
        policy_kill_path=paths["policy_kill"],
        contract_path=paths["contract"],
        classifier_path=paths["classifier"],
        horizon_filter_path=paths["horizon_filter"],
        fallback_path=paths["fallback"],
        output_root=tmp_path / "tail_family_review_check",
    )

    _assert_safety(payload)
    assert payload["report_type"] == "tail_risk_policy_family_controlled_review"
    assert payload["summary"]["tail_risk_policy_decision"] in {
        "CONTINUE",
        "WATCHLIST",
        "KILL",
        "PIVOT",
        "DATA_REQUIRED",
    }
    assert payload["review_decision"]["promotion_gate_allowed"] is False


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
            "gbdt-value-surface-residual-diagnostic-prototype",
            "--value-surface-expansion",
            str(tmp_path / "cli_value_expansion" / "value_surface_controlled_expansion.json"),
            "--gbdt-pivot-selection",
            str(tmp_path / "cli_gbdt" / "gbdt_pivot_direction_selection.json"),
            "--output-root",
            str(tmp_path / "cli_gbdt"),
        ],
        [
            "research",
            "strategies",
            "gbdt-residual-hypothesis-triage",
            "--value-surface-expansion",
            str(tmp_path / "cli_value_expansion" / "value_surface_controlled_expansion.json"),
            "--residual-diagnostic",
            str(tmp_path / "cli_gbdt" / "gbdt_value_surface_residual_diagnostic_prototype.json"),
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
            "research",
            "strategies",
            "regret-casebook-activation-recheck",
            "--value-surface-expansion",
            str(tmp_path / "cli_value_expansion" / "value_surface_controlled_expansion.json"),
            "--regret-activation-inputs",
            str(
                tmp_path / "cli_state" / "regret_activation_inputs_from_value_surface_failures.json"
            ),
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
            "value-surface-controlled-walk-forward-expansion",
            "--prices-path",
            str(prices_path),
            "--marketstack-prices-path",
            str(marketstack_path),
            "--rates-path",
            str(rates_path),
            "--value-surface-expansion",
            str(tmp_path / "cli_value_expansion" / "value_surface_controlled_expansion.json"),
            "--warning-triage",
            str(tmp_path / "cli_warning" / "value_surface_warning_triage_review.json"),
            "--as-of",
            "2023-05-17",
            "--output-root",
            str(tmp_path / "cli_warning"),
        ],
        [
            "research",
            "strategies",
            "value-surface-failure-attribution",
            "--prices-path",
            str(prices_path),
            "--marketstack-prices-path",
            str(marketstack_path),
            "--rates-path",
            str(rates_path),
            "--value-surface-expansion",
            str(tmp_path / "cli_value_expansion" / "value_surface_controlled_expansion.json"),
            "--walk-forward",
            str(tmp_path / "cli_warning" / "value_surface_controlled_walk_forward_expansion.json"),
            "--as-of",
            "2023-05-17",
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
            "research",
            "strategies",
            "value-surface-utility-pareto-ranking-review",
            "--value-surface-expansion",
            str(tmp_path / "cli_value_expansion" / "value_surface_controlled_expansion.json"),
            "--utility-boundary-audit",
            str(tmp_path / "cli_utility" / "utility_boundary_ranking_policy_audit.json"),
            "--output-root",
            str(tmp_path / "cli_utility"),
        ],
        [
            "research",
            "strategies",
            "horizon-cliff-utility-ranking-stabilization-review",
            "--value-surface-expansion",
            str(tmp_path / "cli_value_expansion" / "value_surface_controlled_expansion.json"),
            "--utility-pareto-ranking",
            str(tmp_path / "cli_utility" / "value_surface_utility_pareto_ranking_review.json"),
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
            "forward-evidence",
            "daily-continuity-review",
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
            "forward-evidence",
            "continuity-extension",
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
            "value-surface-direction-review",
            "--failure-attribution",
            str(tmp_path / "cli_warning" / "value_surface_failure_attribution.json"),
            "--horizon-stabilization",
            str(
                tmp_path / "cli_utility" / "horizon_cliff_utility_ranking_stabilization_review.json"
            ),
            "--residual-triage",
            str(tmp_path / "cli_gbdt" / "gbdt_residual_hypothesis_triage.json"),
            "--forward-continuity-extension",
            str(tmp_path / "cli_maturity" / "forward_evidence_continuity_extension.json"),
            "--walk-forward",
            str(tmp_path / "cli_warning" / "value_surface_controlled_walk_forward_expansion.json"),
            "--output-root",
            str(tmp_path / "cli_warning"),
        ],
        [
            "research",
            "strategies",
            "regime-conditioned-value-surface-design",
            "--failure-attribution",
            str(tmp_path / "cli_warning" / "value_surface_failure_attribution.json"),
            "--horizon-stabilization",
            str(
                tmp_path / "cli_utility" / "horizon_cliff_utility_ranking_stabilization_review.json"
            ),
            "--residual-triage",
            str(tmp_path / "cli_gbdt" / "gbdt_residual_hypothesis_triage.json"),
            "--direction-review",
            str(tmp_path / "cli_warning" / "value_surface_direction_review.json"),
            "--output-root",
            str(tmp_path / "cli_warning"),
        ],
        [
            "research",
            "strategies",
            "tail-loss-guardrail-fallback-policy",
            "--value-surface-expansion",
            str(tmp_path / "cli_value_expansion" / "value_surface_controlled_expansion.json"),
            "--failure-attribution",
            str(tmp_path / "cli_warning" / "value_surface_failure_attribution.json"),
            "--horizon-stabilization",
            str(
                tmp_path / "cli_utility" / "horizon_cliff_utility_ranking_stabilization_review.json"
            ),
            "--design",
            str(tmp_path / "cli_warning" / "regime_conditioned_value_surface_design.json"),
            "--output-root",
            str(tmp_path / "cli_warning"),
        ],
        [
            "research",
            "strategies",
            "regime-horizon-loss-attribution-matrix",
            "--value-surface-expansion",
            str(tmp_path / "cli_value_expansion" / "value_surface_controlled_expansion.json"),
            "--failure-attribution",
            str(tmp_path / "cli_warning" / "value_surface_failure_attribution.json"),
            "--output-root",
            str(tmp_path / "cli_warning"),
        ],
        [
            "research",
            "strategies",
            "gbdt-residual-hypothesis-regime-conditioning",
            "--value-surface-expansion",
            str(tmp_path / "cli_value_expansion" / "value_surface_controlled_expansion.json"),
            "--residual-triage",
            str(tmp_path / "cli_gbdt" / "gbdt_residual_hypothesis_triage.json"),
            "--output-root",
            str(tmp_path / "cli_gbdt"),
        ],
        [
            "research",
            "strategies",
            "regime-conditioned-value-surface-controlled-review",
            "--design",
            str(tmp_path / "cli_warning" / "regime_conditioned_value_surface_design.json"),
            "--guardrail-policy",
            str(tmp_path / "cli_warning" / "tail_loss_guardrail_fallback_policy.json"),
            "--loss-matrix",
            str(tmp_path / "cli_warning" / "regime_horizon_loss_attribution_matrix.json"),
            "--residual-regime",
            str(tmp_path / "cli_gbdt" / "gbdt_residual_hypothesis_regime_conditioning.json"),
            "--output-root",
            str(tmp_path / "cli_warning"),
        ],
        [
            "research",
            "strategies",
            "cost-turnover-aware-regime-conditioned-value-surface",
            "--value-surface-expansion",
            str(tmp_path / "cli_value_expansion" / "value_surface_controlled_expansion.json"),
            "--failure-attribution",
            str(tmp_path / "cli_warning" / "value_surface_failure_attribution.json"),
            "--horizon-stabilization",
            str(
                tmp_path / "cli_utility" / "horizon_cliff_utility_ranking_stabilization_review.json"
            ),
            "--design",
            str(tmp_path / "cli_warning" / "regime_conditioned_value_surface_design.json"),
            "--guardrail-policy",
            str(tmp_path / "cli_warning" / "tail_loss_guardrail_fallback_policy.json"),
            "--output-root",
            str(tmp_path / "cli_warning"),
        ],
        [
            "research",
            "strategies",
            "long-horizon-quarantine-selection-review",
            "--value-surface-expansion",
            str(tmp_path / "cli_value_expansion" / "value_surface_controlled_expansion.json"),
            "--failure-attribution",
            str(tmp_path / "cli_warning" / "value_surface_failure_attribution.json"),
            "--horizon-stabilization",
            str(
                tmp_path / "cli_utility" / "horizon_cliff_utility_ranking_stabilization_review.json"
            ),
            "--cost-turnover",
            str(
                tmp_path
                / "cli_warning"
                / "cost_turnover_aware_regime_conditioned_value_surface.json"
            ),
            "--output-root",
            str(tmp_path / "cli_warning"),
        ],
        [
            "research",
            "strategies",
            "ai-after-chatgpt-full-regime-attribution-review",
            "--value-surface-expansion",
            str(tmp_path / "cli_value_expansion" / "value_surface_controlled_expansion.json"),
            "--failure-attribution",
            str(tmp_path / "cli_warning" / "value_surface_failure_attribution.json"),
            "--loss-matrix",
            str(tmp_path / "cli_warning" / "regime_horizon_loss_attribution_matrix.json"),
            "--output-root",
            str(tmp_path / "cli_warning"),
        ],
        [
            "research",
            "strategies",
            "regime-conditioned-walk-forward-holdout",
            "--value-surface-expansion",
            str(tmp_path / "cli_value_expansion" / "value_surface_controlled_expansion.json"),
            "--failure-attribution",
            str(tmp_path / "cli_warning" / "value_surface_failure_attribution.json"),
            "--horizon-stabilization",
            str(
                tmp_path / "cli_utility" / "horizon_cliff_utility_ranking_stabilization_review.json"
            ),
            "--design",
            str(tmp_path / "cli_warning" / "regime_conditioned_value_surface_design.json"),
            "--cost-turnover",
            str(
                tmp_path
                / "cli_warning"
                / "cost_turnover_aware_regime_conditioned_value_surface.json"
            ),
            "--horizon-quarantine",
            str(tmp_path / "cli_warning" / "long_horizon_quarantine_selection_review.json"),
            "--regime-attribution",
            str(tmp_path / "cli_warning" / "ai_after_chatgpt_full_regime_attribution_review.json"),
            "--output-root",
            str(tmp_path / "cli_warning"),
        ],
        [
            "research",
            "strategies",
            "value-surface-v2-controlled-review",
            "--cost-turnover",
            str(
                tmp_path
                / "cli_warning"
                / "cost_turnover_aware_regime_conditioned_value_surface.json"
            ),
            "--horizon-quarantine",
            str(tmp_path / "cli_warning" / "long_horizon_quarantine_selection_review.json"),
            "--regime-attribution",
            str(tmp_path / "cli_warning" / "ai_after_chatgpt_full_regime_attribution_review.json"),
            "--holdout",
            str(tmp_path / "cli_warning" / "regime_conditioned_walk_forward_holdout.json"),
            "--output-root",
            str(tmp_path / "cli_warning"),
        ],
        [
            "research",
            "strategies",
            "horizon-selector-problem-contract",
            "--v2-review",
            str(tmp_path / "cli_warning" / "value_surface_v2_controlled_review.json"),
            "--long-horizon-review",
            str(tmp_path / "cli_warning" / "long_horizon_quarantine_selection_review.json"),
            "--output-root",
            str(tmp_path / "cli_warning"),
        ],
        [
            "research",
            "strategies",
            "long-horizon-quarantine-fallback-review",
            "--value-surface-expansion",
            str(tmp_path / "cli_value_expansion" / "value_surface_controlled_expansion.json"),
            "--contract",
            str(tmp_path / "cli_warning" / "horizon_selector_problem_contract.json"),
            "--v2-review",
            str(tmp_path / "cli_warning" / "value_surface_v2_controlled_review.json"),
            "--output-root",
            str(tmp_path / "cli_warning"),
        ],
        [
            "research",
            "strategies",
            "horizon-selector-controlled-prototype",
            "--value-surface-expansion",
            str(tmp_path / "cli_value_expansion" / "value_surface_controlled_expansion.json"),
            "--contract",
            str(tmp_path / "cli_warning" / "horizon_selector_problem_contract.json"),
            "--fallback-review",
            str(tmp_path / "cli_warning" / "long_horizon_quarantine_fallback_review.json"),
            "--horizon-stabilization",
            str(
                tmp_path / "cli_utility" / "horizon_cliff_utility_ranking_stabilization_review.json"
            ),
            "--output-root",
            str(tmp_path / "cli_warning"),
        ],
        [
            "research",
            "strategies",
            "cost-aware-horizon-hysteresis",
            "--value-surface-expansion",
            str(tmp_path / "cli_value_expansion" / "value_surface_controlled_expansion.json"),
            "--contract",
            str(tmp_path / "cli_warning" / "horizon_selector_problem_contract.json"),
            "--prototype",
            str(tmp_path / "cli_warning" / "horizon_selector_controlled_prototype.json"),
            "--horizon-stabilization",
            str(
                tmp_path / "cli_utility" / "horizon_cliff_utility_ranking_stabilization_review.json"
            ),
            "--output-root",
            str(tmp_path / "cli_warning"),
        ],
        [
            "research",
            "strategies",
            "horizon-selector-holdout-review",
            "--value-surface-expansion",
            str(tmp_path / "cli_value_expansion" / "value_surface_controlled_expansion.json"),
            "--contract",
            str(tmp_path / "cli_warning" / "horizon_selector_problem_contract.json"),
            "--fallback-review",
            str(tmp_path / "cli_warning" / "long_horizon_quarantine_fallback_review.json"),
            "--prototype",
            str(tmp_path / "cli_warning" / "horizon_selector_controlled_prototype.json"),
            "--hysteresis",
            str(tmp_path / "cli_warning" / "cost_aware_horizon_hysteresis.json"),
            "--horizon-stabilization",
            str(
                tmp_path / "cli_utility" / "horizon_cliff_utility_ranking_stabilization_review.json"
            ),
            "--output-root",
            str(tmp_path / "cli_warning"),
        ],
        [
            "research",
            "strategies",
            "value-surface-policy-kill-diagnostic-downgrade",
            "--horizon-selector-holdout",
            str(tmp_path / "cli_warning" / "horizon_selector_holdout_review.json"),
            "--v2-review",
            str(tmp_path / "cli_warning" / "value_surface_v2_controlled_review.json"),
            "--output-root",
            str(tmp_path / "cli_warning"),
        ],
        [
            "research",
            "strategies",
            "benchmark-first-tail-risk-policy-contract",
            "--policy-kill",
            str(tmp_path / "cli_warning" / "value_surface_policy_kill_diagnostic_downgrade.json"),
            "--output-root",
            str(tmp_path / "cli_warning"),
        ],
        [
            "research",
            "strategies",
            "tail-loss-avoidance-classifier-prototype",
            "--value-surface-expansion",
            str(tmp_path / "cli_value_expansion" / "value_surface_controlled_expansion.json"),
            "--policy-kill",
            str(tmp_path / "cli_warning" / "value_surface_policy_kill_diagnostic_downgrade.json"),
            "--output-root",
            str(tmp_path / "cli_warning"),
        ],
        [
            "research",
            "strategies",
            "conservative-horizon-risk-filter",
            "--value-surface-expansion",
            str(tmp_path / "cli_value_expansion" / "value_surface_controlled_expansion.json"),
            "--classifier",
            str(tmp_path / "cli_warning" / "tail_loss_avoidance_classifier_prototype.json"),
            "--contract",
            str(tmp_path / "cli_warning" / "benchmark_first_tail_risk_policy_contract.json"),
            "--output-root",
            str(tmp_path / "cli_warning"),
        ],
        [
            "research",
            "strategies",
            "benchmark-fallback-drawdown-guard-prototype",
            "--value-surface-expansion",
            str(tmp_path / "cli_value_expansion" / "value_surface_controlled_expansion.json"),
            "--classifier",
            str(tmp_path / "cli_warning" / "tail_loss_avoidance_classifier_prototype.json"),
            "--horizon-filter",
            str(tmp_path / "cli_warning" / "conservative_horizon_risk_filter.json"),
            "--contract",
            str(tmp_path / "cli_warning" / "benchmark_first_tail_risk_policy_contract.json"),
            "--output-root",
            str(tmp_path / "cli_warning"),
        ],
        [
            "research",
            "strategies",
            "tail-risk-policy-family-controlled-review",
            "--policy-kill",
            str(tmp_path / "cli_warning" / "value_surface_policy_kill_diagnostic_downgrade.json"),
            "--contract",
            str(tmp_path / "cli_warning" / "benchmark_first_tail_risk_policy_contract.json"),
            "--classifier",
            str(tmp_path / "cli_warning" / "tail_loss_avoidance_classifier_prototype.json"),
            "--horizon-filter",
            str(tmp_path / "cli_warning" / "conservative_horizon_risk_filter.json"),
            "--fallback",
            str(tmp_path / "cli_warning" / "benchmark_fallback_drawdown_guard_prototype.json"),
            "--output-root",
            str(tmp_path / "cli_warning"),
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
    assert (
        tmp_path / "cli_warning" / "value_surface_controlled_walk_forward_expansion.json"
    ).exists()
    assert (tmp_path / "cli_warning" / "value_surface_failure_attribution.json").exists()
    assert (tmp_path / "cli_warning" / "value_surface_direction_review.json").exists()
    assert (tmp_path / "cli_warning" / "regime_conditioned_value_surface_design.json").exists()
    assert (tmp_path / "cli_warning" / "tail_loss_guardrail_fallback_policy.json").exists()
    assert (tmp_path / "cli_warning" / "regime_horizon_loss_attribution_matrix.json").exists()
    assert (
        tmp_path / "cli_warning" / "regime_conditioned_value_surface_controlled_review.json"
    ).exists()
    assert (
        tmp_path / "cli_warning" / "cost_turnover_aware_regime_conditioned_value_surface.json"
    ).exists()
    assert (tmp_path / "cli_warning" / "long_horizon_quarantine_selection_review.json").exists()
    assert (
        tmp_path / "cli_warning" / "ai_after_chatgpt_full_regime_attribution_review.json"
    ).exists()
    assert (tmp_path / "cli_warning" / "regime_conditioned_walk_forward_holdout.json").exists()
    assert (tmp_path / "cli_warning" / "value_surface_v2_controlled_review.json").exists()
    assert (tmp_path / "cli_warning" / "horizon_selector_problem_contract.json").exists()
    assert (tmp_path / "cli_warning" / "long_horizon_quarantine_fallback_review.json").exists()
    assert (tmp_path / "cli_warning" / "horizon_selector_controlled_prototype.json").exists()
    assert (tmp_path / "cli_warning" / "cost_aware_horizon_hysteresis.json").exists()
    assert (tmp_path / "cli_warning" / "horizon_selector_holdout_review.json").exists()
    assert (
        tmp_path / "cli_warning" / "value_surface_policy_kill_diagnostic_downgrade.json"
    ).exists()
    assert (tmp_path / "cli_warning" / "benchmark_first_tail_risk_policy_contract.json").exists()
    assert (tmp_path / "cli_warning" / "tail_loss_avoidance_classifier_prototype.json").exists()
    assert (tmp_path / "cli_warning" / "conservative_horizon_risk_filter.json").exists()
    assert (tmp_path / "cli_warning" / "benchmark_fallback_drawdown_guard_prototype.json").exists()
    assert (tmp_path / "cli_warning" / "tail_risk_policy_family_controlled_review.json").exists()
    assert (tmp_path / "cli_utility" / "utility_ranking_robustness_pareto_audit.json").exists()
    assert (tmp_path / "cli_utility" / "value_surface_utility_pareto_ranking_review.json").exists()
    assert (
        tmp_path / "cli_utility" / "horizon_cliff_utility_ranking_stabilization_review.json"
    ).exists()
    assert (tmp_path / "cli_maturity" / "forward_evidence_maturity_tracker.json").exists()
    assert (
        tmp_path / "cli_maturity" / "forward_evidence_daily_continuity_maturity_tracker.json"
    ).exists()
    assert (tmp_path / "cli_maturity" / "forward_evidence_daily_continuity_review.json").exists()
    assert (tmp_path / "cli_maturity" / "forward_evidence_continuity_extension.json").exists()
    assert (tmp_path / "cli_gbdt" / "gbdt_pivot_direction_selection.json").exists()
    assert (
        tmp_path / "cli_gbdt" / "gbdt_value_surface_residual_diagnostic_prototype.json"
    ).exists()
    assert (tmp_path / "cli_gbdt" / "gbdt_residual_hypothesis_triage.json").exists()
    assert (tmp_path / "cli_gbdt" / "gbdt_residual_hypothesis_regime_conditioning.json").exists()
    assert (tmp_path / "cli_state" / "regret_casebook_expansion_gate.json").exists()
    assert (
        tmp_path / "cli_state" / "regret_activation_inputs_from_value_surface_failures.json"
    ).exists()
    assert (tmp_path / "cli_state" / "regret_casebook_activation_recheck.json").exists()


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
        "value_surface_utility_pareto_ranking_review",
        "horizon_cliff_utility_ranking_stabilization_review",
        "forward_evidence_maturity_tracker",
        "forward_evidence_daily_continuity_maturity_tracker",
        "forward_evidence_daily_continuity_review",
        "forward_evidence_continuity_extension",
        "regret_state_machine_controlled_prototype",
        "simple_strategy_selector_pilot",
        "gbdt_action_utility_controlled_baseline",
        "gbdt_pivot_review",
        "gbdt_pivot_direction_selection",
        "gbdt_value_surface_residual_diagnostic_prototype",
        "gbdt_residual_hypothesis_triage",
        "regret_casebook_expansion_gate",
        "regret_activation_inputs_from_value_surface_failures",
        "regret_casebook_activation_recheck",
        "value_surface_warning_triage_review",
        "value_surface_controlled_walk_forward_expansion",
        "value_surface_failure_attribution",
        "value_surface_direction_review",
        "regime_conditioned_value_surface_design",
        "tail_loss_guardrail_fallback_policy",
        "regime_horizon_loss_attribution_matrix",
        "gbdt_residual_hypothesis_regime_conditioning",
        "regime_conditioned_value_surface_controlled_review",
        "cost_turnover_aware_regime_conditioned_value_surface",
        "long_horizon_quarantine_selection_review",
        "ai_after_chatgpt_full_regime_attribution_review",
        "regime_conditioned_walk_forward_holdout",
        "value_surface_v2_controlled_review",
        "horizon_selector_problem_contract",
        "long_horizon_quarantine_fallback_review",
        "horizon_selector_controlled_prototype",
        "cost_aware_horizon_hysteresis",
        "horizon_selector_holdout_review",
        "value_surface_policy_kill_diagnostic_downgrade",
        "benchmark_first_tail_risk_policy_contract",
        "tail_loss_avoidance_classifier_prototype",
        "conservative_horizon_risk_filter",
        "benchmark_fallback_drawdown_guard_prototype",
        "tail_risk_policy_family_controlled_review",
        "controlled_strategy_batch_review",
    }:
        assert report_id in report_ids
        assert report_ids[report_id]["artifact_selection_policy"] == "latest_available"
        assert report_ids[report_id]["required_for_daily_reading"] is False

    catalog = (PROJECT_ROOT / "docs" / "artifact_catalog.md").read_text(encoding="utf-8")
    assert "value_surface_controlled_prototype.json/md" in catalog
    assert "value_surface_controlled_expansion.json/md" in catalog
    assert "value_surface_warning_triage_review.json/md" in catalog
    assert "value_surface_controlled_walk_forward_expansion.json/md" in catalog
    assert "value_surface_failure_attribution.json/md" in catalog
    assert "value_surface_direction_review.json/md" in catalog
    assert "regime_conditioned_value_surface_design.json/md" in catalog
    assert "tail_loss_guardrail_fallback_policy.json/md" in catalog
    assert "regime_horizon_loss_attribution_matrix.json/md" in catalog
    assert "gbdt_residual_hypothesis_regime_conditioning.json/md" in catalog
    assert "regime_conditioned_value_surface_controlled_review.json/md" in catalog
    assert "cost_turnover_aware_regime_conditioned_value_surface.json/md" in catalog
    assert "long_horizon_quarantine_selection_review.json/md" in catalog
    assert "ai_after_chatgpt_full_regime_attribution_review.json/md" in catalog
    assert "regime_conditioned_walk_forward_holdout.json/md" in catalog
    assert "value_surface_v2_controlled_review.json/md" in catalog
    assert "horizon_selector_problem_contract.json/md" in catalog
    assert "long_horizon_quarantine_fallback_review.json/md" in catalog
    assert "horizon_selector_controlled_prototype.json/md" in catalog
    assert "cost_aware_horizon_hysteresis.json/md" in catalog
    assert "horizon_selector_holdout_review.json/md" in catalog
    assert "value_surface_policy_kill_diagnostic_downgrade.json/md" in catalog
    assert "benchmark_first_tail_risk_policy_contract.json/md" in catalog
    assert "tail_loss_avoidance_classifier_prototype.json/md" in catalog
    assert "conservative_horizon_risk_filter.json/md" in catalog
    assert "benchmark_fallback_drawdown_guard_prototype.json/md" in catalog
    assert "tail_risk_policy_family_controlled_review.json/md" in catalog
    assert "value_surface_utility_pareto_ranking_review.json/md" in catalog
    assert "horizon_cliff_utility_ranking_stabilization_review.json/md" in catalog
    assert "forward_evidence_maturity_tracker.json/md" in catalog
    assert "forward_evidence_daily_continuity_maturity_tracker.json/md" in catalog
    assert "forward_evidence_daily_continuity_review.json/md" in catalog
    assert "forward_evidence_continuity_extension.json/md" in catalog
    assert "gbdt_value_surface_residual_diagnostic_prototype.json/md" in catalog
    assert "gbdt_residual_hypothesis_triage.json/md" in catalog
    assert "regret_casebook_activation_recheck.json/md" in catalog
    assert "controlled_strategy_batch_review.json/md" in catalog
    assert "validated utility boundary" in catalog

    system_flow = (PROJECT_ROOT / "docs" / "system_flow.md").read_text(encoding="utf-8")
    assert "TRADING-770～774" in system_flow
    assert "TRADING-775～779" in system_flow
    assert "TRADING-780～784" in system_flow
    assert "TRADING-785～789" in system_flow
    assert "TRADING-790～794" in system_flow
    assert "TRADING-795～799" in system_flow
    assert "TRADING-800～804" in system_flow
    assert "TRADING-805～809" in system_flow
    assert "TRADING-810～815" in system_flow
    assert "aits research strategies value-surface-controlled-prototype" in system_flow
    assert "aits research strategies value-surface-controlled-expansion" in system_flow
    assert "aits research strategies value-surface-warning-triage-review" in system_flow
    assert "aits research strategies value-surface-controlled-walk-forward-expansion" in system_flow
    assert "aits research strategies value-surface-failure-attribution" in system_flow
    assert "aits research strategies value-surface-direction-review" in system_flow
    assert "aits research strategies regime-conditioned-value-surface-design" in system_flow
    assert "aits research strategies tail-loss-guardrail-fallback-policy" in system_flow
    assert "aits research strategies regime-horizon-loss-attribution-matrix" in system_flow
    assert "aits research strategies gbdt-residual-hypothesis-regime-conditioning" in system_flow
    assert (
        "aits research strategies regime-conditioned-value-surface-controlled-review" in system_flow
    )
    assert (
        "aits research strategies cost-turnover-aware-regime-conditioned-value-surface"
        in system_flow
    )
    assert "aits research strategies long-horizon-quarantine-selection-review" in system_flow
    assert "aits research strategies ai-after-chatgpt-full-regime-attribution-review" in system_flow
    assert "aits research strategies regime-conditioned-walk-forward-holdout" in system_flow
    assert "aits research strategies value-surface-v2-controlled-review" in system_flow
    assert "aits research strategies horizon-selector-problem-contract" in system_flow
    assert "aits research strategies long-horizon-quarantine-fallback-review" in system_flow
    assert "aits research strategies horizon-selector-controlled-prototype" in system_flow
    assert "aits research strategies cost-aware-horizon-hysteresis" in system_flow
    assert "aits research strategies horizon-selector-holdout-review" in system_flow
    assert "aits research strategies value-surface-policy-kill-diagnostic-downgrade" in system_flow
    assert "aits research strategies benchmark-first-tail-risk-policy-contract" in system_flow
    assert "aits research strategies tail-loss-avoidance-classifier-prototype" in system_flow
    assert "aits research strategies conservative-horizon-risk-filter" in system_flow
    assert "aits research strategies benchmark-fallback-drawdown-guard-prototype" in system_flow
    assert "aits research strategies tail-risk-policy-family-controlled-review" in system_flow
    assert "aits research strategies value-surface-utility-pareto-ranking-review" in system_flow
    assert (
        "aits research strategies horizon-cliff-utility-ranking-stabilization-review" in system_flow
    )
    assert "aits forward-evidence maturity-tracker" in system_flow
    assert "aits forward-evidence daily-continuity-maturity-tracker" in system_flow
    assert "aits forward-evidence daily-continuity-review" in system_flow
    assert "aits forward-evidence continuity-extension" in system_flow
    assert (
        "aits research strategies gbdt-value-surface-residual-diagnostic-prototype" in system_flow
    )
    assert "aits research strategies gbdt-residual-hypothesis-triage" in system_flow
    assert "aits research strategies regret-casebook-activation-recheck" in system_flow
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


def _run_direction_review_inputs(tmp_path: Path) -> dict[str, Path]:
    paths = _run_next_stage_inputs(tmp_path)
    warning = run_value_surface_warning_triage_review(
        value_surface_expansion_path=paths["value_expansion"],
        utility_boundary_audit_path=paths["utility"],
        forward_maturity_path=paths["maturity"],
        output_root=tmp_path / "direction_warning",
    )
    walk_forward = run_value_surface_controlled_walk_forward_expansion(
        prices_path=paths["prices"],
        marketstack_prices_path=paths["marketstack"],
        rates_path=paths["rates"],
        value_surface_expansion_path=paths["value_expansion"],
        warning_triage_path=Path(warning["artifact_paths"]["json_path"]),
        output_root=tmp_path / "direction_walk_forward",
        as_of_date=TEST_AS_OF,
    )
    utility_pareto = run_value_surface_utility_pareto_ranking_review(
        value_surface_expansion_path=paths["value_expansion"],
        utility_boundary_audit_path=paths["utility"],
        output_root=tmp_path / "direction_utility",
    )
    residual = run_gbdt_value_surface_residual_diagnostic_prototype(
        value_surface_expansion_path=paths["value_expansion"],
        gbdt_pivot_selection_path=tmp_path / "missing_gbdt_pivot_selection.json",
        output_root=tmp_path / "direction_gbdt",
    )
    return {
        **paths,
        "warning": Path(warning["artifact_paths"]["json_path"]),
        "walk_forward": Path(walk_forward["artifact_paths"]["json_path"]),
        "utility_pareto": Path(utility_pareto["artifact_paths"]["json_path"]),
        "residual_diagnostic": Path(residual["artifact_paths"]["json_path"]),
    }


def _run_regime_conditioning_inputs(tmp_path: Path) -> dict[str, Path]:
    paths = _run_direction_review_inputs(tmp_path)
    failure = run_value_surface_failure_attribution(
        prices_path=paths["prices"],
        marketstack_prices_path=paths["marketstack"],
        rates_path=paths["rates"],
        value_surface_expansion_path=paths["value_expansion"],
        walk_forward_path=paths["walk_forward"],
        output_root=tmp_path / "regime_failure",
        as_of_date=TEST_AS_OF,
    )
    horizon = run_horizon_cliff_utility_ranking_stabilization_review(
        value_surface_expansion_path=paths["value_expansion"],
        utility_pareto_ranking_path=paths["utility_pareto"],
        output_root=tmp_path / "regime_horizon",
    )
    residual_triage = run_gbdt_residual_hypothesis_triage(
        value_surface_expansion_path=paths["value_expansion"],
        residual_diagnostic_path=paths["residual_diagnostic"],
        output_root=tmp_path / "regime_residual_triage",
    )
    forward = run_forward_evidence_continuity_extension(
        prices_path=paths["prices"],
        marketstack_prices_path=paths["marketstack"],
        rates_path=paths["rates"],
        ledger_path=paths["ledger"],
        value_surface_expansion_path=paths["value_expansion"],
        output_root=tmp_path / "regime_forward",
        as_of_date=TEST_AS_OF,
    )
    direction = run_value_surface_direction_review(
        failure_attribution_path=Path(failure["artifact_paths"]["json_path"]),
        horizon_stabilization_path=Path(horizon["artifact_paths"]["json_path"]),
        residual_triage_path=Path(residual_triage["artifact_paths"]["json_path"]),
        forward_continuity_extension_path=Path(forward["artifact_paths"]["json_path"]),
        walk_forward_path=paths["walk_forward"],
        output_root=tmp_path / "regime_direction",
    )
    return {
        **paths,
        "failure": Path(failure["artifact_paths"]["json_path"]),
        "horizon": Path(horizon["artifact_paths"]["json_path"]),
        "residual_triage": Path(residual_triage["artifact_paths"]["json_path"]),
        "forward": Path(forward["artifact_paths"]["json_path"]),
        "direction": Path(direction["artifact_paths"]["json_path"]),
    }


def _run_value_surface_v2_inputs(tmp_path: Path) -> dict[str, Path]:
    paths = _run_regime_conditioning_inputs(tmp_path)
    design = run_regime_conditioned_value_surface_design(
        failure_attribution_path=paths["failure"],
        horizon_stabilization_path=paths["horizon"],
        residual_triage_path=paths["residual_triage"],
        direction_review_path=paths["direction"],
        output_root=tmp_path / "v2_design",
    )
    guardrail = run_tail_loss_guardrail_fallback_policy(
        value_surface_expansion_path=paths["value_expansion"],
        failure_attribution_path=paths["failure"],
        horizon_stabilization_path=paths["horizon"],
        design_path=Path(design["artifact_paths"]["json_path"]),
        output_root=tmp_path / "v2_guardrail",
    )
    return {
        **paths,
        "design": Path(design["artifact_paths"]["json_path"]),
        "guardrail": Path(guardrail["artifact_paths"]["json_path"]),
    }


def _run_value_surface_v2_full_inputs(tmp_path: Path) -> dict[str, Path]:
    paths = _run_value_surface_v2_inputs(tmp_path)
    cost = run_cost_turnover_aware_regime_conditioned_value_surface(
        value_surface_expansion_path=paths["value_expansion"],
        failure_attribution_path=paths["failure"],
        horizon_stabilization_path=paths["horizon"],
        design_path=paths["design"],
        guardrail_policy_path=paths["guardrail"],
        output_root=tmp_path / "v2_cost",
    )
    long_horizon = run_long_horizon_quarantine_selection_review(
        value_surface_expansion_path=paths["value_expansion"],
        failure_attribution_path=paths["failure"],
        horizon_stabilization_path=paths["horizon"],
        cost_turnover_path=Path(cost["artifact_paths"]["json_path"]),
        output_root=tmp_path / "v2_long_horizon",
    )
    matrix = run_regime_horizon_loss_attribution_matrix(
        value_surface_expansion_path=paths["value_expansion"],
        failure_attribution_path=paths["failure"],
        output_root=tmp_path / "v2_matrix",
    )
    ai_regime = run_ai_after_chatgpt_full_regime_attribution_review(
        value_surface_expansion_path=paths["value_expansion"],
        failure_attribution_path=paths["failure"],
        loss_matrix_path=Path(matrix["artifact_paths"]["json_path"]),
        output_root=tmp_path / "v2_ai_regime",
    )
    return {
        **paths,
        "cost_turnover": Path(cost["artifact_paths"]["json_path"]),
        "long_horizon": Path(long_horizon["artifact_paths"]["json_path"]),
        "loss_matrix": Path(matrix["artifact_paths"]["json_path"]),
        "ai_regime": Path(ai_regime["artifact_paths"]["json_path"]),
    }


def _run_horizon_selector_inputs(tmp_path: Path) -> dict[str, Path]:
    paths = _run_value_surface_v2_full_inputs(tmp_path)
    holdout = run_regime_conditioned_walk_forward_holdout(
        value_surface_expansion_path=paths["value_expansion"],
        failure_attribution_path=paths["failure"],
        horizon_stabilization_path=paths["horizon"],
        design_path=paths["design"],
        cost_turnover_path=paths["cost_turnover"],
        horizon_quarantine_path=paths["long_horizon"],
        regime_attribution_path=paths["ai_regime"],
        output_root=tmp_path / "horizon_v2_holdout",
    )
    v2_review = run_value_surface_v2_controlled_review(
        cost_turnover_path=paths["cost_turnover"],
        horizon_quarantine_path=paths["long_horizon"],
        regime_attribution_path=paths["ai_regime"],
        holdout_path=Path(holdout["artifact_paths"]["json_path"]),
        output_root=tmp_path / "horizon_v2_review",
    )
    contract = run_horizon_selector_problem_contract(
        v2_review_path=Path(v2_review["artifact_paths"]["json_path"]),
        long_horizon_review_path=paths["long_horizon"],
        output_root=tmp_path / "horizon_contract",
    )
    fallback = run_long_horizon_quarantine_fallback_review(
        value_surface_expansion_path=paths["value_expansion"],
        contract_path=Path(contract["artifact_paths"]["json_path"]),
        v2_review_path=Path(v2_review["artifact_paths"]["json_path"]),
        output_root=tmp_path / "horizon_fallback",
    )
    return {
        **paths,
        "v2_holdout": Path(holdout["artifact_paths"]["json_path"]),
        "v2_review": Path(v2_review["artifact_paths"]["json_path"]),
        "contract": Path(contract["artifact_paths"]["json_path"]),
        "fallback_review": Path(fallback["artifact_paths"]["json_path"]),
    }


def _run_horizon_selector_full_inputs(tmp_path: Path) -> dict[str, Path]:
    paths = _run_horizon_selector_inputs(tmp_path)
    prototype = run_horizon_selector_controlled_prototype(
        value_surface_expansion_path=paths["value_expansion"],
        contract_path=paths["contract"],
        fallback_review_path=paths["fallback_review"],
        horizon_stabilization_path=paths["horizon"],
        output_root=tmp_path / "horizon_prototype",
    )
    hysteresis = run_cost_aware_horizon_hysteresis(
        value_surface_expansion_path=paths["value_expansion"],
        contract_path=paths["contract"],
        prototype_path=Path(prototype["artifact_paths"]["json_path"]),
        horizon_stabilization_path=paths["horizon"],
        output_root=tmp_path / "horizon_hysteresis",
    )
    return {
        **paths,
        "prototype": Path(prototype["artifact_paths"]["json_path"]),
        "hysteresis": Path(hysteresis["artifact_paths"]["json_path"]),
    }


def _run_tail_risk_policy_inputs(tmp_path: Path) -> dict[str, Path]:
    paths = _run_horizon_selector_full_inputs(tmp_path)
    horizon_holdout = run_horizon_selector_holdout_review(
        value_surface_expansion_path=paths["value_expansion"],
        contract_path=paths["contract"],
        fallback_review_path=paths["fallback_review"],
        prototype_path=paths["prototype"],
        hysteresis_path=paths["hysteresis"],
        horizon_stabilization_path=paths["horizon"],
        output_root=tmp_path / "tail_horizon_holdout",
    )
    policy_kill = run_value_surface_policy_kill_diagnostic_downgrade(
        horizon_selector_holdout_path=Path(horizon_holdout["artifact_paths"]["json_path"]),
        v2_review_path=paths["v2_review"],
        output_root=tmp_path / "tail_policy_kill",
    )
    contract = run_benchmark_first_tail_risk_policy_contract(
        policy_kill_path=Path(policy_kill["artifact_paths"]["json_path"]),
        output_root=tmp_path / "tail_contract",
    )
    classifier = run_tail_loss_avoidance_classifier_prototype(
        value_surface_expansion_path=paths["value_expansion"],
        policy_kill_path=Path(policy_kill["artifact_paths"]["json_path"]),
        output_root=tmp_path / "tail_classifier",
    )
    return {
        **paths,
        "horizon_contract": paths["contract"],
        "horizon_holdout": Path(horizon_holdout["artifact_paths"]["json_path"]),
        "policy_kill": Path(policy_kill["artifact_paths"]["json_path"]),
        "contract": Path(contract["artifact_paths"]["json_path"]),
        "classifier": Path(classifier["artifact_paths"]["json_path"]),
    }


def _run_tail_risk_policy_full_inputs(tmp_path: Path) -> dict[str, Path]:
    paths = _run_tail_risk_policy_inputs(tmp_path)
    horizon_filter = run_conservative_horizon_risk_filter(
        value_surface_expansion_path=paths["value_expansion"],
        classifier_path=paths["classifier"],
        contract_path=paths["contract"],
        output_root=tmp_path / "tail_horizon_filter",
    )
    fallback = run_benchmark_fallback_drawdown_guard_controlled_prototype(
        value_surface_expansion_path=paths["value_expansion"],
        classifier_path=paths["classifier"],
        horizon_filter_path=Path(horizon_filter["artifact_paths"]["json_path"]),
        contract_path=paths["contract"],
        output_root=tmp_path / "tail_fallback",
    )
    return {
        **paths,
        "horizon_filter": Path(horizon_filter["artifact_paths"]["json_path"]),
        "fallback": Path(fallback["artifact_paths"]["json_path"]),
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
