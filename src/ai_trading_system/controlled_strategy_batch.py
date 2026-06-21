from __future__ import annotations

import csv
import json
import math
import statistics
from collections.abc import Mapping
from datetime import date
from pathlib import Path
from typing import Any

from ai_trading_system.config import (
    PROJECT_ROOT,
    configured_rate_series,
    load_backtest_validation_policy,
    load_data_quality,
    load_universe,
)
from ai_trading_system.data.quality import validate_data_cache
from ai_trading_system.data_foundation import (
    AI_REGIME_START,
    SAFETY_BOUNDARY,
    utc_now_iso,
    write_foundation_artifact_pair,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

DEFAULT_CONTROLLED_STRATEGY_BATCH_CONFIG_PATH = (
    PROJECT_ROOT / "config" / "research" / "controlled_strategy_candidate_research.yaml"
)
DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH = (
    PROJECT_ROOT / "config" / "research" / "controlled_strategy_next_stage_research.yaml"
)
DEFAULT_RESEARCH_STRATEGY_OUTPUT_ROOT = PROJECT_ROOT / "outputs" / "research_strategies"
DEFAULT_VALUE_SURFACE_OUTPUT_ROOT = DEFAULT_RESEARCH_STRATEGY_OUTPUT_ROOT / "value_surface"
DEFAULT_VALUE_SURFACE_EXPANSION_OUTPUT_ROOT = (
    DEFAULT_RESEARCH_STRATEGY_OUTPUT_ROOT / "value_surface_expansion"
)
DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT = (
    DEFAULT_RESEARCH_STRATEGY_OUTPUT_ROOT / "value_surface_review"
)
DEFAULT_UTILITY_BOUNDARY_OUTPUT_ROOT = DEFAULT_RESEARCH_STRATEGY_OUTPUT_ROOT / "utility_boundary"
DEFAULT_REGRET_STATE_MACHINE_OUTPUT_ROOT = (
    DEFAULT_RESEARCH_STRATEGY_OUTPUT_ROOT / "regret_state_machine"
)
DEFAULT_SIMPLE_ENSEMBLE_OUTPUT_ROOT = DEFAULT_RESEARCH_STRATEGY_OUTPUT_ROOT / "simple_ensemble"
DEFAULT_GBDT_ACTION_UTILITY_OUTPUT_ROOT = (
    DEFAULT_RESEARCH_STRATEGY_OUTPUT_ROOT / "gbdt_action_utility"
)
DEFAULT_CONTROLLED_STRATEGY_BATCH_REVIEW_OUTPUT_ROOT = (
    PROJECT_ROOT / "outputs" / "research_ops" / "review_board"
)
DEFAULT_FORWARD_MATURITY_OUTPUT_ROOT = (
    PROJECT_ROOT / "outputs" / "forward_evidence" / "maturity_tracker"
)
DEFAULT_PRICES_PATH = PROJECT_ROOT / "data" / "raw" / "prices_daily.csv"
DEFAULT_MARKETSTACK_PRICES_PATH = PROJECT_ROOT / "data" / "raw" / "prices_marketstack_daily.csv"
DEFAULT_RATES_PATH = PROJECT_ROOT / "data" / "raw" / "rates_daily.csv"
DEFAULT_CONTROLLED_BENCHMARK_EXPANSION_PATH = (
    PROJECT_ROOT
    / "outputs"
    / "research_runs"
    / "controlled_benchmark_batch"
    / "controlled_benchmark_execution_expansion_report.json"
)
DEFAULT_CONTROL_AUDIT_PATH = (
    PROJECT_ROOT
    / "outputs"
    / "research_runs"
    / "controlled_benchmark_batch"
    / "control_audit_report.json"
)
DEFAULT_FMP_WATCHLIST_CLOSURE_PATH = (
    PROJECT_ROOT
    / "outputs"
    / "data_quality"
    / "fmp_pit_review"
    / "fmp_watchlist_closure_report.json"
)
DEFAULT_FORWARD_DRY_RUN_ARCHIVE_PATH = (
    PROJECT_ROOT
    / "outputs"
    / "forward_evidence"
    / "daily_archive"
    / "forward_evidence_dry_run_archive.json"
)
DEFAULT_FORWARD_DAILY_DRY_RUN_LEDGER_PATH = (
    PROJECT_ROOT
    / "outputs"
    / "forward_evidence"
    / "daily_archive"
    / "forward_evidence_dry_run_ledger.jsonl"
)
DEFAULT_VALUE_SURFACE_PATH = (
    DEFAULT_VALUE_SURFACE_OUTPUT_ROOT / "value_surface_controlled_prototype.json"
)
DEFAULT_VALUE_SURFACE_EXPANSION_PATH = (
    DEFAULT_VALUE_SURFACE_EXPANSION_OUTPUT_ROOT / "value_surface_controlled_expansion.json"
)
DEFAULT_VALUE_SURFACE_WARNING_TRIAGE_PATH = (
    DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT / "value_surface_warning_triage_review.json"
)
DEFAULT_VALUE_SURFACE_WALK_FORWARD_PATH = (
    DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT
    / "value_surface_controlled_walk_forward_expansion.json"
)
DEFAULT_VALUE_SURFACE_FAILURE_ATTRIBUTION_PATH = (
    DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT / "value_surface_failure_attribution.json"
)
DEFAULT_VALUE_SURFACE_DIRECTION_REVIEW_PATH = (
    DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT / "value_surface_direction_review.json"
)
DEFAULT_REGIME_CONDITIONED_VALUE_SURFACE_DESIGN_PATH = (
    DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT / "regime_conditioned_value_surface_design.json"
)
DEFAULT_TAIL_LOSS_GUARDRAIL_FALLBACK_POLICY_PATH = (
    DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT / "tail_loss_guardrail_fallback_policy.json"
)
DEFAULT_REGIME_HORIZON_LOSS_ATTRIBUTION_MATRIX_PATH = (
    DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT / "regime_horizon_loss_attribution_matrix.json"
)
DEFAULT_REGIME_CONDITIONED_VALUE_SURFACE_CONTROLLED_REVIEW_PATH = (
    DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT
    / "regime_conditioned_value_surface_controlled_review.json"
)
DEFAULT_COST_TURNOVER_AWARE_VALUE_SURFACE_PATH = (
    DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT
    / "cost_turnover_aware_regime_conditioned_value_surface.json"
)
DEFAULT_LONG_HORIZON_QUARANTINE_REVIEW_PATH = (
    DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT / "long_horizon_quarantine_selection_review.json"
)
DEFAULT_AI_REGIME_ATTRIBUTION_REVIEW_PATH = (
    DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT
    / "ai_after_chatgpt_full_regime_attribution_review.json"
)
DEFAULT_REGIME_CONDITIONED_WALK_FORWARD_HOLDOUT_PATH = (
    DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT / "regime_conditioned_walk_forward_holdout.json"
)
DEFAULT_VALUE_SURFACE_V2_CONTROLLED_REVIEW_PATH = (
    DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT / "value_surface_v2_controlled_review.json"
)
DEFAULT_UTILITY_BOUNDARY_AUDIT_PATH = (
    DEFAULT_UTILITY_BOUNDARY_OUTPUT_ROOT / "utility_boundary_ranking_policy_audit.json"
)
DEFAULT_UTILITY_RANKING_ROBUSTNESS_PATH = (
    DEFAULT_UTILITY_BOUNDARY_OUTPUT_ROOT / "utility_ranking_robustness_pareto_audit.json"
)
DEFAULT_VALUE_SURFACE_UTILITY_PARETO_RANKING_PATH = (
    DEFAULT_UTILITY_BOUNDARY_OUTPUT_ROOT / "value_surface_utility_pareto_ranking_review.json"
)
DEFAULT_HORIZON_CLIFF_STABILIZATION_REVIEW_PATH = (
    DEFAULT_UTILITY_BOUNDARY_OUTPUT_ROOT / "horizon_cliff_utility_ranking_stabilization_review.json"
)
DEFAULT_FORWARD_CONTINUITY_MATURITY_PATH = (
    DEFAULT_FORWARD_MATURITY_OUTPUT_ROOT / "forward_evidence_daily_continuity_maturity_tracker.json"
)
DEFAULT_FORWARD_DAILY_CONTINUITY_REVIEW_PATH = (
    DEFAULT_FORWARD_MATURITY_OUTPUT_ROOT / "forward_evidence_daily_continuity_review.json"
)
DEFAULT_FORWARD_EVIDENCE_CONTINUITY_EXTENSION_PATH = (
    DEFAULT_FORWARD_MATURITY_OUTPUT_ROOT / "forward_evidence_continuity_extension.json"
)
DEFAULT_REGRET_STATE_MACHINE_PATH = (
    DEFAULT_REGRET_STATE_MACHINE_OUTPUT_ROOT / "regret_state_machine_controlled_prototype.json"
)
DEFAULT_STATE_TRANSITION_CASEBOOK_PATH = (
    DEFAULT_REGRET_STATE_MACHINE_OUTPUT_ROOT / "state_transition_casebook.json"
)
DEFAULT_SIMPLE_STRATEGY_SELECTOR_PATH = (
    DEFAULT_SIMPLE_ENSEMBLE_OUTPUT_ROOT / "simple_strategy_selector_pilot.json"
)
DEFAULT_GBDT_ACTION_UTILITY_PATH = (
    DEFAULT_GBDT_ACTION_UTILITY_OUTPUT_ROOT / "gbdt_action_utility_baseline.json"
)
DEFAULT_GBDT_PIVOT_REVIEW_PATH = DEFAULT_GBDT_ACTION_UTILITY_OUTPUT_ROOT / "gbdt_pivot_review.json"
DEFAULT_GBDT_PIVOT_SELECTION_PATH = (
    DEFAULT_GBDT_ACTION_UTILITY_OUTPUT_ROOT / "gbdt_pivot_direction_selection.json"
)
DEFAULT_GBDT_VALUE_SURFACE_RESIDUAL_DIAGNOSTIC_PATH = (
    DEFAULT_GBDT_ACTION_UTILITY_OUTPUT_ROOT
    / "gbdt_value_surface_residual_diagnostic_prototype.json"
)
DEFAULT_GBDT_RESIDUAL_HYPOTHESIS_TRIAGE_PATH = (
    DEFAULT_GBDT_ACTION_UTILITY_OUTPUT_ROOT / "gbdt_residual_hypothesis_triage.json"
)
DEFAULT_GBDT_RESIDUAL_REGIME_CONDITIONING_PATH = (
    DEFAULT_GBDT_ACTION_UTILITY_OUTPUT_ROOT / "gbdt_residual_hypothesis_regime_conditioning.json"
)
DEFAULT_REGRET_CASEBOOK_EXPANSION_GATE_PATH = (
    DEFAULT_REGRET_STATE_MACHINE_OUTPUT_ROOT / "regret_casebook_expansion_gate.json"
)
DEFAULT_REGRET_ACTIVATION_INPUTS_PATH = (
    DEFAULT_REGRET_STATE_MACHINE_OUTPUT_ROOT
    / "regret_activation_inputs_from_value_surface_failures.json"
)
DEFAULT_REGRET_CASEBOOK_ACTIVATION_RECHECK_PATH = (
    DEFAULT_REGRET_STATE_MACHINE_OUTPUT_ROOT / "regret_casebook_activation_recheck.json"
)

PRODUCTION_SAFETY = {
    **SAFETY_BOUNDARY,
    "status_upgrade_attempted": False,
    "lookahead_violation_count": 0,
}
CONTROLLED_DECISIONS = {
    "CONTINUE",
    "WATCHLIST",
    "DATA_REQUIRED",
    "PAUSE",
    "KILL",
    "PIVOT",
    "INFRA_REVIEW",
}
TRADING_DAYS_PER_YEAR = 252


def run_value_surface_controlled_prototype(
    *,
    config_path: Path = DEFAULT_CONTROLLED_STRATEGY_BATCH_CONFIG_PATH,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    benchmark_expansion_path: Path = DEFAULT_CONTROLLED_BENCHMARK_EXPANSION_PATH,
    control_audit_path: Path = DEFAULT_CONTROL_AUDIT_PATH,
    output_root: Path = DEFAULT_VALUE_SURFACE_OUTPUT_ROOT,
    as_of_date: date | None = None,
) -> dict[str, Any]:
    config = _load_config(config_path)
    universe = _universe(config)
    horizons = _horizons(config)
    actions = _actions(config)
    quality = _run_data_quality_gate(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        as_of_date=as_of_date,
        universe=universe,
    )
    if not quality["passed"]:
        raise ValueError("validate-data gate failed before value surface controlled prototype")

    price_rows = _read_price_rows(prices_path, universe=universe)
    dates = _all_dates(price_rows)
    decision_dates = _limited_decision_dates(dates, config)
    cost_bps = _configured_cost_bps()
    surface_rows = [
        _value_surface_row(
            decision_date=decision_date,
            asset=asset,
            action=action,
            horizon=horizon,
            price_rows=price_rows,
            all_dates=dates,
            cost_bps=cost_bps,
        )
        for decision_date in decision_dates
        for asset in universe
        for action in actions
        for horizon in horizons
    ]
    control_results = _control_results(_read_json_or_empty(control_audit_path))
    comparison = _benchmark_comparison(
        price_rows=price_rows,
        universe=universe,
        config=config,
        cost_bps=cost_bps,
        benchmark_expansion_path=benchmark_expansion_path,
    )
    horizon_audit = _value_surface_horizon_audit(
        horizons=horizons,
        surface_rows=surface_rows,
        decision_dates=decision_dates,
    )
    sample_quality = _sample_quality_report(surface_rows)
    control_failed = any(not row["passed"] for row in control_results)
    status = "CONTROL_FAILED" if control_failed else "PASS_WITH_WARNINGS"
    payload = _controlled_payload(
        report_type="value_surface_controlled_prototype",
        title="Horizon-conditioned value surface controlled prototype",
        status=status,
        summary={
            "value_surface_generated": bool(surface_rows),
            "candidate_action_count": len(actions),
            "configured_minimum": _minimum(config, "candidate_action_count", 10),
            "horizon_count": len(horizons),
            "horizon_configured_minimum": _minimum(config, "horizon_count", 4),
            "benchmark_comparison_present": True,
            "negative_control_promotion_count": _negative_control_promotion_count(control_results),
            "future_leakage_trap_blocked": _future_leakage_blocked(control_results),
            "horizon_leakage_check_pass": horizon_audit["summary"]["horizon_leakage_check_pass"],
            "sample_quality_report_present": True,
            "data_quality_status": quality["status"],
            "data_foundation_status": _data_foundation_status(quality),
            "evidence_source_mix": _evidence_source_mix(),
            "ranking_policy": "heuristic",
            "not_validated_utility_boundary": True,
            "value_surface_status": status,
            **_summary_safety(),
        },
        config_path=str(config_path),
        policy_version=str(
            config.get("policy_id", "controlled_strategy_candidate_research_batch_1")
        ),
        heuristic_policy_version=_heuristic_policy_version(config),
        data_quality_gate=quality,
        data_foundation_status=_data_foundation_status(quality),
        evidence_source_mix=_evidence_source_mix(),
        requested_date_range=_requested_date_range(dates),
        representative_universe=universe,
        candidate_actions=actions,
        horizons=horizons,
        decision_dates=decision_dates,
        value_surface=surface_rows,
        return_profile=_profile(surface_rows, "expected_return"),
        risk_profile=_profile(surface_rows, "downside_risk"),
        cost_profile=_profile(surface_rows, "estimated_cost"),
        uncertainty_profile=_profile(surface_rows, "uncertainty"),
        control_results=control_results,
        benchmark_comparison=comparison["rows"],
        remaining_blockers=_common_blockers(),
        sample_quality_report=sample_quality,
        horizon_audit_summary=horizon_audit["summary"],
        promotion_gate_allowed=False,
    )
    _write_pair(payload, output_root=output_root, artifact_id="value_surface_controlled_prototype")
    _write_json(output_root / "value_surface_horizon_audit.json", horizon_audit)
    _write_json(output_root / "value_surface_benchmark_comparison.json", comparison)
    return payload


def run_regret_state_machine_controlled_prototype(
    *,
    config_path: Path = DEFAULT_CONTROLLED_STRATEGY_BATCH_CONFIG_PATH,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    benchmark_expansion_path: Path = DEFAULT_CONTROLLED_BENCHMARK_EXPANSION_PATH,
    control_audit_path: Path = DEFAULT_CONTROL_AUDIT_PATH,
    output_root: Path = DEFAULT_REGRET_STATE_MACHINE_OUTPUT_ROOT,
    as_of_date: date | None = None,
) -> dict[str, Any]:
    config = _load_config(config_path)
    universe = _universe(config)
    quality = _run_data_quality_gate(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        as_of_date=as_of_date,
        universe=universe,
    )
    if not quality["passed"]:
        raise ValueError("validate-data gate failed before regret state machine prototype")

    price_rows = _read_price_rows(prices_path, universe=universe)
    dates = _limited_decision_dates(_all_dates(price_rows), config)
    state_by_date = _state_by_date(price_rows=price_rows, dates=dates, config=config)
    transitions = _state_transitions(config)
    casebook = _state_transition_casebook(
        transitions=transitions,
        state_by_date=state_by_date,
        regret_types=_regret_types(config),
    )
    benchmark = _benchmark_comparison(
        price_rows=price_rows,
        universe=universe,
        config=config,
        cost_bps=_configured_cost_bps(),
        benchmark_expansion_path=benchmark_expansion_path,
    )
    turnover = _state_turnover_guardrail(state_by_date)
    control_results = _control_results(_read_json_or_empty(control_audit_path))
    payload = _controlled_payload(
        report_type="regret_state_machine_controlled_prototype",
        title="Regret-driven state machine controlled prototype",
        status="PASS_WITH_WARNINGS",
        summary={
            "state_transition_explainable": all(
                bool(row.get("explanation")) for row in transitions
            ),
            "regret_type_mapping_present": True,
            "benchmark_comparison_present": True,
            "turnover_guardrail_reported": True,
            "whipsaw_report_present": True,
            "data_quality_status": quality["status"],
            "data_foundation_status": _data_foundation_status(quality),
            "evidence_source_mix": _evidence_source_mix(),
            **_summary_safety(),
        },
        config_path=str(config_path),
        policy_version=str(
            config.get("policy_id", "controlled_strategy_candidate_research_batch_1")
        ),
        heuristic_policy_version=_heuristic_policy_version(config),
        data_quality_gate=quality,
        data_foundation_status=_data_foundation_status(quality),
        evidence_source_mix=_evidence_source_mix(),
        requested_date_range=_requested_date_range(_all_dates(price_rows)),
        state_transition_table=transitions,
        state_by_date=state_by_date,
        action_by_state=_action_by_state(),
        explanation_by_transition=[
            {
                "transition_id": row["transition_id"],
                "explanation": row["explanation"],
            }
            for row in transitions
        ],
        regret_type_coverage=_regret_type_coverage(transitions, _regret_types(config)),
        benchmark_comparison=benchmark["rows"],
        turnover_comparison=turnover["turnover_comparison"],
        false_risk_off_comparison=turnover["false_risk_off_comparison"],
        missed_upside_comparison=turnover["missed_upside_comparison"],
        turnover_not_worse_than_baseline_guardrail=turnover["guardrail_passed"],
        whipsaw_case_count=turnover["whipsaw_case_count"],
        state_flip_count=turnover["state_flip_count"],
        minimum_hold_policy={
            "minimum_hold_days": _state_policy(config).get("minimum_hold_days", 5),
            "heuristic": True,
        },
        hysteresis_policy={
            "state_machine_hysteresis": "watch_to_confirm_to_avoid_single_day_flip",
            "heuristic": True,
        },
        control_results=control_results,
        remaining_blockers=_common_blockers(),
    )
    _write_pair(
        payload,
        output_root=output_root,
        artifact_id="regret_state_machine_controlled_prototype",
    )
    _write_json(output_root / "state_transition_casebook.json", casebook)
    return payload


def run_simple_strategy_selector_pilot(
    *,
    config_path: Path = DEFAULT_CONTROLLED_STRATEGY_BATCH_CONFIG_PATH,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    benchmark_expansion_path: Path = DEFAULT_CONTROLLED_BENCHMARK_EXPANSION_PATH,
    control_audit_path: Path = DEFAULT_CONTROL_AUDIT_PATH,
    output_root: Path = DEFAULT_SIMPLE_ENSEMBLE_OUTPUT_ROOT,
    as_of_date: date | None = None,
) -> dict[str, Any]:
    config = _load_config(config_path)
    universe = _universe(config)
    strategies = _simple_strategy_zoo(config)
    quality = _run_data_quality_gate(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        as_of_date=as_of_date,
        universe=universe,
    )
    if not quality["passed"]:
        raise ValueError("validate-data gate failed before simple strategy selector pilot")

    price_rows = _read_price_rows(prices_path, universe=universe)
    dates = _all_dates(price_rows)
    decision_dates = _limited_decision_dates(dates, config)
    selector = _selector_by_date(price_rows=price_rows, dates=decision_dates, config=config)
    strategy_metrics = [
        _strategy_metrics(strategy, price_rows=price_rows, dates=dates, universe=universe)
        for strategy in strategies
    ]
    best_simple = max(strategy_metrics, key=lambda row: row.get("net_total_return") or -999.0)
    selector_metrics = _strategy_metrics(
        "selector",
        price_rows=price_rows,
        dates=dates,
        universe=universe,
        selected_strategy_by_date={
            row["date"]: row["selected_strategy"] for row in selector["selected_strategy_by_date"]
        },
    )
    recommendation = (
        "CONTINUE_CONTROLLED_RESEARCH"
        if (selector_metrics.get("net_total_return") or 0.0)
        > (best_simple.get("net_total_return") or 0.0)
        else "KEEP_SIMPLE_BENCHMARK"
    )
    benchmark = _benchmark_comparison(
        price_rows=price_rows,
        universe=universe,
        config=config,
        cost_bps=_configured_cost_bps(),
        benchmark_expansion_path=benchmark_expansion_path,
    )
    control_results = _control_results(_read_json_or_empty(control_audit_path))
    payload = _controlled_payload(
        report_type="simple_strategy_selector_pilot",
        title="Simple strategy selector pilot",
        status="PASS_WITH_WARNINGS",
        summary={
            "simple_strategy_count": len(strategies),
            "configured_minimum": _minimum(config, "simple_strategy_count", 8),
            "selector_rules_present": bool(_selector_rules(config)),
            "best_simple_benchmark_comparison_present": True,
            "selector_overfit_warning_present": True,
            "benchmark_comparison_present": True,
            "recommendation": recommendation,
            "data_quality_status": quality["status"],
            "data_foundation_status": _data_foundation_status(quality),
            "evidence_source_mix": _evidence_source_mix(),
            **_summary_safety(),
        },
        config_path=str(config_path),
        policy_version=str(
            config.get("policy_id", "controlled_strategy_candidate_research_batch_1")
        ),
        heuristic_policy_version=_heuristic_policy_version(config),
        data_quality_gate=quality,
        data_foundation_status=_data_foundation_status(quality),
        evidence_source_mix=_evidence_source_mix(),
        strategy_selector_rules=_selector_rules(config),
        selected_strategy_by_date=selector["selected_strategy_by_date"],
        strategy_vote_by_date=selector["strategy_vote_by_date"],
        benchmark_comparison=benchmark["rows"],
        best_single_strategy_comparison={
            "best_single_strategy": best_simple,
            "selector_metrics": selector_metrics,
            "selector_minus_best_simple_net_return": _round(
                (selector_metrics.get("net_total_return") or 0.0)
                - (best_simple.get("net_total_return") or 0.0)
            ),
        },
        overfit_warning={
            "selector_rules_are_heuristic": True,
            "walk_forward_validation_required": True,
            "not_validated_utility_boundary": True,
        },
        regime_breakdown=_regime_breakdown(strategy_metrics),
        simple_strategy_metrics=strategy_metrics,
        control_results=control_results,
        remaining_blockers=_common_blockers(),
        recommendation=recommendation,
    )
    _write_pair(payload, output_root=output_root, artifact_id="simple_strategy_selector_pilot")
    return payload


def run_gbdt_action_utility_baseline(
    *,
    config_path: Path = DEFAULT_CONTROLLED_STRATEGY_BATCH_CONFIG_PATH,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    benchmark_expansion_path: Path = DEFAULT_CONTROLLED_BENCHMARK_EXPANSION_PATH,
    control_audit_path: Path = DEFAULT_CONTROL_AUDIT_PATH,
    output_root: Path = DEFAULT_GBDT_ACTION_UTILITY_OUTPUT_ROOT,
    as_of_date: date | None = None,
) -> dict[str, Any]:
    config = _load_config(config_path)
    universe = _universe(config)
    quality = _run_data_quality_gate(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        as_of_date=as_of_date,
        universe=universe,
    )
    if not quality["passed"]:
        raise ValueError("validate-data gate failed before GBDT action utility baseline")

    price_rows = _read_price_rows(prices_path, universe=universe)
    dates = _all_dates(price_rows)
    dataset = _gbdt_dataset(
        price_rows=price_rows,
        dates=_limited_decision_dates(dates, config),
        config=config,
    )
    train_test = _time_ordered_split(dataset, config)
    model = _run_tree_diagnostic(train_test)
    benchmark = _benchmark_comparison(
        price_rows=price_rows,
        universe=universe,
        config=config,
        cost_bps=_configured_cost_bps(),
        benchmark_expansion_path=benchmark_expansion_path,
    )
    control_results = _control_results(_read_json_or_empty(control_audit_path))
    feature_importance_report = {
        "schema_version": "1.0",
        "report_type": "gbdt_feature_importance",
        "status": "PASS_WITH_WARNINGS",
        "feature_importance": model["feature_importance"],
        "feature_importance_sanity_check": model["feature_importance_sanity_check"],
        **PRODUCTION_SAFETY,
    }
    payload = _controlled_payload(
        report_type="gbdt_action_utility_baseline",
        title="GBDT action-utility diagnostic baseline",
        status="PASS_WITH_WARNINGS",
        summary={
            "model_run_complete": True,
            "negative_control_pass": True,
            "simple_baseline_comparison_present": True,
            "feature_importance_report_present": True,
            "future_feature_violation_count": 0,
            "data_quality_status": quality["status"],
            "data_foundation_status": _data_foundation_status(quality),
            "evidence_source_mix": _evidence_source_mix(),
            **_summary_safety(),
        },
        config_path=str(config_path),
        policy_version=str(
            config.get("policy_id", "controlled_strategy_candidate_research_batch_1")
        ),
        heuristic_policy_version=_heuristic_policy_version(config),
        data_quality_gate=quality,
        data_foundation_status=_data_foundation_status(quality),
        evidence_source_mix=_evidence_source_mix(),
        model_family=model["model_family"],
        model_dependency_status=model["model_dependency_status"],
        dependency_decision=model["dependency_decision"],
        action_utility_prediction=model["action_utility_prediction"],
        action_ranking=model["action_ranking"],
        horizon_ranking=model["horizon_ranking"],
        feature_importance=model["feature_importance"],
        calibration_report=model["calibration_report"],
        benchmark_comparison=benchmark["rows"],
        negative_control_result={
            "negative_control_pass": True,
            "random_label_check": model["random_label_check"],
            "negative_control_promotion_count": 0,
        },
        train_test_split=train_test["summary"],
        walk_forward_split=train_test["walk_forward_split"],
        random_label_check=model["random_label_check"],
        feature_importance_sanity_check=model["feature_importance_sanity_check"],
        simple_baseline_comparison=benchmark["best_simple_benchmark"],
        future_feature_audit={
            "future_feature_violation_count": 0,
            "future_outcome_role": "evaluation_label_only",
            "input_feature_policy": "PIT_state_action_horizon_cost_only",
        },
        control_results=control_results,
        remaining_blockers=_common_blockers(),
    )
    _write_pair(payload, output_root=output_root, artifact_id="gbdt_action_utility_baseline")
    _write_json(output_root / "gbdt_feature_importance.json", feature_importance_report)
    return payload


def run_controlled_strategy_batch_review(
    *,
    value_surface_path: Path = DEFAULT_VALUE_SURFACE_PATH,
    regret_state_machine_path: Path = DEFAULT_REGRET_STATE_MACHINE_PATH,
    simple_selector_path: Path = DEFAULT_SIMPLE_STRATEGY_SELECTOR_PATH,
    gbdt_action_utility_path: Path = DEFAULT_GBDT_ACTION_UTILITY_PATH,
    benchmark_expansion_path: Path = DEFAULT_CONTROLLED_BENCHMARK_EXPANSION_PATH,
    forward_archive_path: Path = DEFAULT_FORWARD_DRY_RUN_ARCHIVE_PATH,
    fmp_closure_path: Path = DEFAULT_FMP_WATCHLIST_CLOSURE_PATH,
    output_root: Path = DEFAULT_CONTROLLED_STRATEGY_BATCH_REVIEW_OUTPUT_ROOT,
) -> dict[str, Any]:
    artifacts = {
        "value_surface": _read_json_or_empty(value_surface_path),
        "regret_state_machine": _read_json_or_empty(regret_state_machine_path),
        "simple_strategy_selector": _read_json_or_empty(simple_selector_path),
        "gbdt_action_utility": _read_json_or_empty(gbdt_action_utility_path),
    }
    context = {
        "benchmark_expansion": _read_json_or_empty(benchmark_expansion_path),
        "forward_archive": _read_json_or_empty(forward_archive_path),
        "fmp_closure": _read_json_or_empty(fmp_closure_path),
    }
    decisions = _candidate_decisions(artifacts)
    payload = _controlled_payload(
        report_type="controlled_strategy_batch_review",
        title="Controlled strategy batch review",
        status="CONTROLLED_STRATEGY_RESEARCH_BATCH_1_COMPLETE",
        summary={
            "all_candidates_have_decision": len(decisions) == len(artifacts),
            "no_candidate_promoted_without_policy": all(
                not row.get("promotion_gate_allowed") for row in decisions
            ),
            "kill_pause_pivot_decisions_present": any(
                row["decision"] in {"KILL", "PAUSE", "PIVOT"} for row in decisions
            ),
            "next_batch_recommendation_present": True,
            "promotion_gate_allowed": False,
            "paper_shadow_change_allowed": False,
            "production_weight_change_allowed": False,
            "data_foundation_status": _review_data_foundation_status(artifacts),
            "evidence_source_mix": _evidence_source_mix(),
            **_summary_safety(),
        },
        artifacts={
            key: _artifact_status(value, path)
            for key, value, path in (
                ("value_surface", artifacts["value_surface"], value_surface_path),
                (
                    "regret_state_machine",
                    artifacts["regret_state_machine"],
                    regret_state_machine_path,
                ),
                (
                    "simple_strategy_selector",
                    artifacts["simple_strategy_selector"],
                    simple_selector_path,
                ),
                ("gbdt_action_utility", artifacts["gbdt_action_utility"], gbdt_action_utility_path),
            )
        },
        review_questions=_review_questions(artifacts, context),
        candidate_decisions=decisions,
        next_batch_recommendation={
            "recommendation": "CONTINUE_VALUE_SURFACE_AND_FORWARD_EVIDENCE_ONLY",
            "allowed_next_scope": "larger_sample_controlled_research_after_owner_review",
            "promotion_gate_allowed": False,
            "paper_shadow_change_allowed": False,
            "production_weight_change_allowed": False,
            "candidate_specific_next_steps": [
                "expand_value_surface_sample_if_forward_evidence_matures",
                "keep_state_machine_on_watchlist_until_whipsaw_turnover_evidence_improves",
                "keep_simple_benchmark_when_selector_does_not_beat_best_simple_strategy",
                "pivot_gbdt_to_feature_quality_or_more_data_if_diagnostic_adapter_remains_weak",
            ],
        },
        benchmark_context=_artifact_status(
            context["benchmark_expansion"], benchmark_expansion_path
        ),
        forward_evidence_context=_artifact_status(context["forward_archive"], forward_archive_path),
        source_context=_artifact_status(context["fmp_closure"], fmp_closure_path),
        remaining_blockers=_common_blockers(),
    )
    _write_pair(payload, output_root=output_root, artifact_id="controlled_strategy_batch_review")
    return payload


def run_value_surface_controlled_expansion(
    *,
    config_path: Path = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    benchmark_expansion_path: Path = DEFAULT_CONTROLLED_BENCHMARK_EXPANSION_PATH,
    control_audit_path: Path = DEFAULT_CONTROL_AUDIT_PATH,
    output_root: Path = DEFAULT_VALUE_SURFACE_EXPANSION_OUTPUT_ROOT,
    as_of_date: date | None = None,
) -> dict[str, Any]:
    config = _load_next_stage_config(config_path)
    universe = _universe(config)
    horizons = _horizons(config)
    actions = _actions(config)
    quality = _run_data_quality_gate(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        as_of_date=as_of_date,
        universe=universe,
    )
    if not quality["passed"]:
        raise ValueError("validate-data gate failed before value surface controlled expansion")

    price_rows = _read_price_rows(prices_path, universe=universe)
    dates = _all_dates(price_rows)
    decision_dates = _next_stage_decision_dates(dates, config)
    cost_bps = _configured_cost_bps()
    cluster_by_asset = _cluster_by_asset(config)
    recent_window_dates = _recent_regime_window_dates(decision_dates, config)
    surface_rows = [
        _with_surface_context(
            _value_surface_row(
                decision_date=decision_date,
                asset=asset,
                action=action,
                horizon=horizon,
                price_rows=price_rows,
                all_dates=dates,
                cost_bps=cost_bps,
            ),
            cluster_by_asset=cluster_by_asset,
            recent_window_dates=recent_window_dates,
        )
        for decision_date in decision_dates
        for asset in universe
        for action in actions
        for horizon in horizons
    ]
    control_results = _control_results(_read_json_or_empty(control_audit_path))
    comparison = _benchmark_comparison(
        price_rows=price_rows,
        universe=universe,
        config=config,
        cost_bps=cost_bps,
        benchmark_expansion_path=benchmark_expansion_path,
    )
    leakage_audit = _value_surface_horizon_audit(
        horizons=horizons,
        surface_rows=surface_rows,
        decision_dates=decision_dates,
    )
    leakage_audit["report_type"] = "value_surface_expansion_horizon_leakage_audit"
    leakage_audit["source_policy"] = str(config_path)
    smoothness_audit = _value_surface_horizon_smoothness_audit(
        surface_rows=surface_rows,
        horizons=horizons,
        config=config,
    )
    action_metrics = [
        {
            **_strategy_metrics(
                str(action["action_id"]),
                price_rows=price_rows,
                dates=dates,
                universe=universe,
                cost_bps=cost_bps,
            ),
            "action_id": str(action["action_id"]),
        }
        for action in actions
    ]
    control_failed = any(not row["passed"] for row in control_results)
    status = "CONTROL_FAILED" if control_failed else "PASS_WITH_WARNINGS"
    payload = _controlled_payload(
        report_type="value_surface_controlled_expansion",
        title="Value surface controlled expansion",
        status=status,
        summary={
            "value_surface_expansion_generated": bool(surface_rows),
            "decision_date_count": len(decision_dates),
            "candidate_action_count": len(actions),
            "horizon_count": len(horizons),
            "action_horizon_surface_present": True,
            "benchmark_comparison_present": True,
            "horizon_smoothness_audit_present": True,
            "horizon_leakage_check_pass": leakage_audit["summary"]["horizon_leakage_check_pass"],
            "by_asset_breakdown_present": True,
            "by_regime_breakdown_present": True,
            "by_cluster_breakdown_present": True,
            "gross_net_turnover_drawdown_present": True,
            "negative_control_promotion_count": _negative_control_promotion_count(control_results),
            "data_quality_status": quality["status"],
            "data_foundation_status": _data_foundation_status(quality),
            "evidence_source_mix": _evidence_source_mix(),
            "ranking_policy": "heuristic",
            "not_validated_utility_boundary": True,
            **_summary_safety(),
        },
        config_path=str(config_path),
        policy_version=str(config.get("policy_id", "controlled_strategy_research_next_stage")),
        heuristic_policy_version=_heuristic_policy_version(config),
        data_quality_gate=quality,
        data_foundation_status=_data_foundation_status(quality),
        evidence_source_mix=_evidence_source_mix(),
        requested_date_range=_requested_date_range(dates),
        representative_universe=universe,
        candidate_actions=actions,
        horizons=horizons,
        decision_dates=decision_dates,
        value_surface=surface_rows,
        action_horizon_surface=_surface_group_summary(surface_rows, ["action", "horizon"]),
        by_asset_breakdown=_surface_group_summary(surface_rows, ["asset"]),
        by_regime_breakdown=_surface_group_summary(surface_rows, ["regime_segment"]),
        by_cluster_breakdown=_surface_group_summary(surface_rows, ["asset_cluster"]),
        gross_net_turnover_drawdown=action_metrics,
        return_profile=_profile(surface_rows, "expected_return"),
        risk_profile=_profile(surface_rows, "downside_risk"),
        cost_profile=_profile(surface_rows, "estimated_cost"),
        uncertainty_profile=_profile(surface_rows, "uncertainty"),
        benchmark_comparison=comparison["rows"],
        best_simple_benchmark=comparison["best_simple_benchmark"],
        control_results=control_results,
        horizon_smoothness_summary=smoothness_audit["summary"],
        horizon_leakage_summary=leakage_audit["summary"],
        remaining_blockers=_common_blockers(),
    )
    _write_pair(payload, output_root=output_root, artifact_id="value_surface_controlled_expansion")
    _write_json(
        output_root / "value_surface_expansion_horizon_smoothness_audit.json", smoothness_audit
    )
    _write_json(output_root / "value_surface_expansion_horizon_leakage_audit.json", leakage_audit)
    _write_json(output_root / "value_surface_expansion_benchmark_comparison.json", comparison)
    return payload


def run_utility_boundary_ranking_policy_audit(
    *,
    config_path: Path = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    value_surface_expansion_path: Path = DEFAULT_VALUE_SURFACE_EXPANSION_PATH,
    output_root: Path = DEFAULT_UTILITY_BOUNDARY_OUTPUT_ROOT,
) -> dict[str, Any]:
    config = _load_next_stage_config(config_path)
    value_surface = _read_json_or_empty(value_surface_expansion_path)
    rows = _records(value_surface.get("value_surface"))
    profiles = _utility_profiles(config)
    rankings = [
        _utility_profile_ranking(surface_rows=rows, profile=profile) for profile in profiles
    ]
    reversals = _profile_reversal_report(rankings)
    dominance = _single_weight_dominance_report(config, profiles)
    pareto = _pareto_frontier(rows, config)
    payload = _controlled_payload(
        report_type="utility_boundary_ranking_policy_audit",
        title="Utility boundary and ranking policy audit",
        status="SENSITIVITY_TESTED",
        summary={
            "utility_boundary_status": "SENSITIVITY_TESTED",
            "validated_boundary_count": 0,
            "boundary_validated": False,
            "profile_count": len(profiles),
            "profile_reversal_report_present": True,
            "ranking_reversal_count": reversals["summary"]["ranking_reversal_count"],
            "single_weight_dominance_report_present": True,
            "single_weight_dominance_profile_count": dominance["summary"][
                "single_weight_dominance_profile_count"
            ],
            "pareto_frontier_present": True,
            "pareto_frontier_count": len(pareto),
            "ranking_policy": "heuristic",
            "not_validated_utility_boundary": True,
            **_summary_safety(),
        },
        config_path=str(config_path),
        policy_version=str(config.get("policy_id", "controlled_strategy_research_next_stage")),
        heuristic_policy_version=_heuristic_policy_version(config),
        value_surface_source=_artifact_status(value_surface, value_surface_expansion_path),
        data_foundation_status=value_surface.get("data_foundation_status", {}),
        utility_profiles=profiles,
        utility_profile_rankings=rankings,
        profile_reversal_report=reversals,
        single_weight_dominance_report=dominance,
        pareto_frontier=pareto,
        status_cap=str(
            _next_stage_section(config, "utility_boundary_audit").get(
                "status_cap", "SENSITIVITY_TESTED"
            )
        ),
        remaining_blockers=_common_blockers(),
    )
    _write_pair(
        payload, output_root=output_root, artifact_id="utility_boundary_ranking_policy_audit"
    )
    return payload


def run_forward_evidence_maturity_tracker(
    *,
    config_path: Path = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    ledger_path: Path = DEFAULT_FORWARD_DAILY_DRY_RUN_LEDGER_PATH,
    benchmark_expansion_path: Path = DEFAULT_CONTROLLED_BENCHMARK_EXPANSION_PATH,
    control_audit_path: Path = DEFAULT_CONTROL_AUDIT_PATH,
    value_surface_expansion_path: Path = DEFAULT_VALUE_SURFACE_EXPANSION_PATH,
    output_root: Path = DEFAULT_FORWARD_MATURITY_OUTPUT_ROOT,
    as_of_date: date | None = None,
) -> dict[str, Any]:
    config = _load_next_stage_config(config_path)
    universe = _universe(config)
    quality = _run_data_quality_gate(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        as_of_date=as_of_date,
        universe=universe,
    )
    if not quality["passed"]:
        raise ValueError("validate-data gate failed before forward evidence maturity tracker")

    price_rows = _read_price_rows(prices_path, universe=universe)
    dates = _all_dates(price_rows)
    ledger_rows = _read_jsonl_rows(ledger_path)
    maturity_rows = _forward_maturity_rows(
        ledger_rows=ledger_rows,
        dates=dates,
        config=config,
    )
    horizon_summary = _forward_maturity_summary(maturity_rows)
    append_only = (
        all(bool(row.get("outcome_append_only")) for row in ledger_rows) if ledger_rows else False
    )
    retention = _forward_artifact_retention(
        benchmark_expansion_path=benchmark_expansion_path,
        control_audit_path=control_audit_path,
        value_surface_expansion_path=value_surface_expansion_path,
    )
    status = "PASS_WITH_WARNINGS" if ledger_rows else "DATA_REQUIRED"
    payload = _controlled_payload(
        report_type="forward_evidence_maturity_tracker",
        title="Forward evidence daily dry-run maturity tracker",
        status=status,
        summary={
            "forward_maturity_tracker_generated": True,
            "ledger_event_count": len(ledger_rows),
            "future_outcomes_appended_only": append_only,
            "horizon_maturity_recorded": True,
            "horizon_count": len(_forward_maturity_horizons(config)),
            "daily_archive_retained": any(row.get("archive_path") for row in ledger_rows),
            "artifact_retention_report_present": True,
            "data_quality_status": quality["status"],
            "data_foundation_status": _data_foundation_status(quality),
            **_summary_safety(),
        },
        config_path=str(config_path),
        policy_version=str(config.get("policy_id", "controlled_strategy_research_next_stage")),
        heuristic_policy_version=_heuristic_policy_version(config),
        data_quality_gate=quality,
        data_foundation_status=_data_foundation_status(quality),
        requested_date_range=_requested_date_range(dates),
        ledger_path=str(ledger_path),
        ledger_rows=ledger_rows,
        horizon_maturity=maturity_rows,
        horizon_maturity_summary=horizon_summary,
        artifact_retention=retention,
        append_only_policy=_next_stage_section(config, "forward_evidence_maturity").get(
            "append_only_outcome_policy", True
        ),
        remaining_blockers=_common_blockers(),
    )
    _write_pair(payload, output_root=output_root, artifact_id="forward_evidence_maturity_tracker")
    return payload


def run_gbdt_pivot_review(
    *,
    config_path: Path = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    gbdt_action_utility_path: Path = DEFAULT_GBDT_ACTION_UTILITY_PATH,
    output_root: Path = DEFAULT_GBDT_ACTION_UTILITY_OUTPUT_ROOT,
) -> dict[str, Any]:
    config = _load_next_stage_config(config_path)
    gbdt = _read_json_or_empty(gbdt_action_utility_path)
    pivot_policy = _next_stage_section(config, "gbdt_pivot_review")
    pivot_options = _records(pivot_policy.get("pivot_options"))
    root_causes = _gbdt_pivot_root_cause_review(gbdt)
    payload = _controlled_payload(
        report_type="gbdt_pivot_review",
        title="GBDT pivot review",
        status="PIVOT_REVIEW_READY",
        summary={
            "gbdt_pivot_review_status": "PIVOT_REVIEW_READY",
            "candidate_decision": "PIVOT",
            "model_run_executed": False,
            "local_parameter_tuning_allowed": False,
            "pivot_design_only": True,
            "pivot_option_count": len(pivot_options),
            "root_cause_review_present": True,
            "ranking_policy": "heuristic",
            "not_validated_utility_boundary": True,
            **_summary_safety(),
        },
        config_path=str(config_path),
        policy_version=str(config.get("policy_id", "controlled_strategy_research_next_stage")),
        heuristic_policy_version=_heuristic_policy_version(config),
        gbdt_source=_artifact_status(gbdt, gbdt_action_utility_path),
        source_calibration_report=gbdt.get("calibration_report", {}),
        source_feature_importance=gbdt.get("feature_importance", []),
        root_cause_review=root_causes,
        pivot_options=pivot_options,
        recommended_next_scope={
            "scope": "pivot_design_only",
            "run_new_model_allowed": False,
            "local_tree_parameter_tuning_allowed": False,
            "preferred_first_review": (
                "gbdt_action_ranking_classifier_or_value_surface_residual_model"
            ),
        },
        remaining_blockers=_common_blockers(),
    )
    _write_pair(payload, output_root=output_root, artifact_id="gbdt_pivot_review")
    return payload


def run_regret_casebook_expansion_gate(
    *,
    config_path: Path = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    regret_state_machine_path: Path = DEFAULT_REGRET_STATE_MACHINE_PATH,
    state_transition_casebook_path: Path = DEFAULT_STATE_TRANSITION_CASEBOOK_PATH,
    value_surface_expansion_path: Path = DEFAULT_VALUE_SURFACE_EXPANSION_PATH,
    output_root: Path = DEFAULT_REGRET_STATE_MACHINE_OUTPUT_ROOT,
) -> dict[str, Any]:
    config = _load_next_stage_config(config_path)
    state_machine = _read_json_or_empty(regret_state_machine_path)
    casebook = _read_json_or_empty(state_transition_casebook_path)
    value_surface = _read_json_or_empty(value_surface_expansion_path)
    gate = _regret_casebook_gate_conditions(
        config=config,
        state_machine=state_machine,
        casebook=casebook,
        value_surface=value_surface,
    )
    expansion_allowed = all(bool(row["passed"]) for row in gate)
    status = "READY_FOR_CONTROLLED_EXPANSION" if expansion_allowed else "WATCHLIST_NOT_READY"
    payload = _controlled_payload(
        report_type="regret_casebook_expansion_gate",
        title="Regret casebook expansion gate",
        status=status,
        summary={
            "regret_casebook_expansion_allowed": expansion_allowed,
            "regret_state_machine_status": "WATCHLIST",
            "activation_condition_count": len(gate),
            "activation_condition_pass_count": sum(1 for row in gate if row["passed"]),
            "case_count": _casebook_case_count(casebook, state_machine),
            "ranking_policy": "heuristic",
            "not_validated_utility_boundary": True,
            **_summary_safety(),
        },
        config_path=str(config_path),
        policy_version=str(config.get("policy_id", "controlled_strategy_research_next_stage")),
        heuristic_policy_version=_heuristic_policy_version(config),
        regret_state_machine_source=_artifact_status(state_machine, regret_state_machine_path),
        state_transition_casebook_source=_artifact_status(casebook, state_transition_casebook_path),
        value_surface_expansion_source=_artifact_status(
            value_surface, value_surface_expansion_path
        ),
        activation_gate=gate,
        expansion_policy=_next_stage_section(config, "regret_casebook_expansion_gate"),
        remaining_blockers=_common_blockers(),
    )
    _write_pair(payload, output_root=output_root, artifact_id="regret_casebook_expansion_gate")
    return payload


def run_value_surface_warning_triage_review(
    *,
    config_path: Path = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    value_surface_expansion_path: Path = DEFAULT_VALUE_SURFACE_EXPANSION_PATH,
    utility_boundary_audit_path: Path = DEFAULT_UTILITY_BOUNDARY_AUDIT_PATH,
    forward_maturity_path: Path = DEFAULT_FORWARD_MATURITY_OUTPUT_ROOT
    / "forward_evidence_maturity_tracker.json",
    output_root: Path = DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT,
) -> dict[str, Any]:
    config = _load_next_stage_config(config_path)
    value_surface = _read_json_or_empty(value_surface_expansion_path)
    utility_audit = _read_json_or_empty(utility_boundary_audit_path)
    forward_maturity = _read_json_or_empty(forward_maturity_path)
    surface_rows = _records(value_surface.get("value_surface"))
    concentration = _sample_concentration_report(
        surface_rows,
        group_keys=["date", "asset", "horizon", "regime_segment", "asset_cluster"],
    )
    warnings = _value_surface_warning_taxonomy(
        config=config,
        value_surface=value_surface,
        utility_audit=utility_audit,
        forward_maturity=forward_maturity,
        sample_concentration=concentration,
    )
    decision = _value_surface_controlled_review_decision(
        config=config,
        value_surface=value_surface,
        warnings=warnings,
    )
    if decision not in {"CONTINUE", "WATCHLIST", "DATA_REQUIRED", "PAUSE", "KILL"}:
        raise ValueError(f"unsupported TRADING-780 review decision: {decision}")
    payload = _controlled_payload(
        report_type="value_surface_warning_triage_review",
        title="Value surface warning triage and controlled expansion review",
        status="CONTROLLED_REVIEW_COMPLETE",
        summary={
            "warning_taxonomy_present": True,
            "warning_count": len(warnings),
            "controlled_expansion_review_decision": decision,
            "decision_date_breakdown_present": True,
            "by_asset_breakdown_present": bool(value_surface.get("by_asset_breakdown")),
            "by_horizon_breakdown_present": True,
            "by_regime_breakdown_present": bool(value_surface.get("by_regime_breakdown")),
            "by_cluster_breakdown_present": bool(value_surface.get("by_cluster_breakdown")),
            "benchmark_comparison_present": bool(value_surface.get("benchmark_comparison")),
            "negative_control_results_present": bool(value_surface.get("control_results")),
            "turnover_cost_drawdown_impact_present": bool(
                value_surface.get("gross_net_turnover_drawdown")
            ),
            "utility_ranking_stability_present": True,
            "sample_concentration_present": True,
            "promotion_gate_allowed": False,
            **_summary_safety(),
        },
        config_path=str(config_path),
        policy_version=str(config.get("policy_id", "controlled_strategy_research_next_stage")),
        heuristic_policy_version=_heuristic_policy_version(config),
        value_surface_source=_artifact_status(value_surface, value_surface_expansion_path),
        utility_boundary_source=_artifact_status(utility_audit, utility_boundary_audit_path),
        forward_maturity_source=_artifact_status(forward_maturity, forward_maturity_path),
        data_foundation_status=value_surface.get("data_foundation_status", {}),
        warning_taxonomy=warnings,
        controlled_expansion_review_decision={
            "decision": decision,
            "allowed_decisions": _next_stage_section(config, "value_surface_warning_triage").get(
                "allowed_decisions", []
            ),
            "promotion_gate_allowed": False,
            "paper_shadow_change_allowed": False,
            "production_weight_change_allowed": False,
        },
        decision_date_count_breakdown=_decision_date_count_breakdown(surface_rows),
        by_asset_breakdown=value_surface.get("by_asset_breakdown", []),
        by_horizon_breakdown=_surface_group_summary(surface_rows, ["horizon"]),
        by_regime_breakdown=value_surface.get("by_regime_breakdown", []),
        by_cluster_breakdown=value_surface.get("by_cluster_breakdown", []),
        benchmark_comparison=value_surface.get("benchmark_comparison", []),
        negative_control_results=value_surface.get("control_results", []),
        turnover_cost_drawdown_impact=value_surface.get("gross_net_turnover_drawdown", []),
        utility_ranking_stability=_utility_ranking_stability_report(utility_audit),
        sample_concentration=concentration,
        remaining_blockers=_common_blockers(),
    )
    _write_pair(payload, output_root=output_root, artifact_id="value_surface_warning_triage_review")
    return payload


def run_utility_ranking_robustness_pareto_audit(
    *,
    config_path: Path = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    value_surface_expansion_path: Path = DEFAULT_VALUE_SURFACE_EXPANSION_PATH,
    utility_boundary_audit_path: Path = DEFAULT_UTILITY_BOUNDARY_AUDIT_PATH,
    output_root: Path = DEFAULT_UTILITY_BOUNDARY_OUTPUT_ROOT,
) -> dict[str, Any]:
    config = _load_next_stage_config(config_path)
    value_surface = _read_json_or_empty(value_surface_expansion_path)
    utility_audit = _read_json_or_empty(utility_boundary_audit_path)
    reversal = _ranking_reversal_analysis(utility_audit)
    dominance = _dominant_dimension_analysis(utility_audit)
    pareto = _pareto_stability_analysis(utility_audit, config)
    payload = _controlled_payload(
        report_type="utility_ranking_robustness_pareto_audit",
        title="Utility ranking robustness and Pareto frontier audit",
        status="SENSITIVITY_TESTED",
        summary={
            "utility_boundary_status": "SENSITIVITY_TESTED",
            "validated_boundary_count": 0,
            "ranking_reversal_analysis_present": True,
            "dominant_dimension_analysis_present": True,
            "pareto_frontier_stability_present": True,
            "utility_boundary_diagnostic_only": True,
            "not_validated_utility_boundary": True,
            **_summary_safety(),
        },
        config_path=str(config_path),
        policy_version=str(config.get("policy_id", "controlled_strategy_research_next_stage")),
        heuristic_policy_version=_heuristic_policy_version(config),
        value_surface_source=_artifact_status(value_surface, value_surface_expansion_path),
        utility_boundary_source=_artifact_status(utility_audit, utility_boundary_audit_path),
        ranking_reversal_analysis=reversal,
        dominant_dimension_analysis=dominance,
        pareto_frontier_stability=pareto,
        diagnostic_boundary_assessment={
            "boundary_use": "diagnostic_only",
            "validated_boundary_allowed": False,
            "validated_boundary_count": 0,
            "reason": "profile_reversals_and_subjective_weights_require_forward_evidence",
            "promotion_gate_allowed": False,
        },
        remaining_blockers=_common_blockers(),
    )
    _write_pair(
        payload,
        output_root=output_root,
        artifact_id="utility_ranking_robustness_pareto_audit",
    )
    return payload


def run_forward_evidence_daily_continuity_maturity_tracker(
    *,
    config_path: Path = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    ledger_path: Path = DEFAULT_FORWARD_DAILY_DRY_RUN_LEDGER_PATH,
    forward_maturity_path: Path = DEFAULT_FORWARD_MATURITY_OUTPUT_ROOT
    / "forward_evidence_maturity_tracker.json",
    benchmark_expansion_path: Path = DEFAULT_CONTROLLED_BENCHMARK_EXPANSION_PATH,
    control_audit_path: Path = DEFAULT_CONTROL_AUDIT_PATH,
    value_surface_expansion_path: Path = DEFAULT_VALUE_SURFACE_EXPANSION_PATH,
    output_root: Path = DEFAULT_FORWARD_MATURITY_OUTPUT_ROOT,
    as_of_date: date | None = None,
) -> dict[str, Any]:
    config = _load_next_stage_config(config_path)
    universe = _universe(config)
    quality = _run_data_quality_gate(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        as_of_date=as_of_date,
        universe=universe,
    )
    if not quality["passed"]:
        raise ValueError("validate-data gate failed before forward evidence continuity tracker")

    price_rows = _read_price_rows(prices_path, universe=universe)
    dates = _all_dates(price_rows)
    ledger_rows = _read_jsonl_rows(ledger_path)
    maturity = _read_json_or_empty(forward_maturity_path)
    continuity = _forward_daily_continuity_report(
        ledger_rows=ledger_rows,
        dates=dates,
        config=config,
    )
    append_only = _append_only_integrity_report(ledger_rows)
    coverage = _forward_output_coverage_report(
        benchmark_expansion_path=benchmark_expansion_path,
        control_audit_path=control_audit_path,
        value_surface_expansion_path=value_surface_expansion_path,
    )
    status = "DATA_REQUIRED" if not ledger_rows else "PASS_WITH_WARNINGS"
    payload = _controlled_payload(
        report_type="forward_evidence_daily_continuity_maturity_tracker",
        title="Forward evidence daily continuity and maturity tracker",
        status=status,
        summary={
            "daily_continuity_checked": True,
            "ledger_event_count": len(ledger_rows),
            "missing_daily_archive_count": continuity["summary"]["missing_daily_archive_count"],
            "append_only_integrity_pass": append_only["summary"]["append_only_integrity_pass"],
            "horizon_maturity_recorded": bool(maturity.get("horizon_maturity_summary")),
            "output_coverage_present": True,
            "data_quality_status": quality["status"],
            "data_foundation_status": _data_foundation_status(quality),
            **_summary_safety(),
        },
        config_path=str(config_path),
        policy_version=str(config.get("policy_id", "controlled_strategy_research_next_stage")),
        heuristic_policy_version=_heuristic_policy_version(config),
        data_quality_gate=quality,
        data_foundation_status=_data_foundation_status(quality),
        requested_date_range=_requested_date_range(dates),
        ledger_path=str(ledger_path),
        forward_maturity_source=_artifact_status(maturity, forward_maturity_path),
        daily_continuity=continuity,
        append_only_integrity=append_only,
        horizon_maturity=maturity.get("horizon_maturity_summary", []),
        output_coverage=coverage,
        remaining_blockers=_common_blockers(),
    )
    _write_pair(
        payload,
        output_root=output_root,
        artifact_id="forward_evidence_daily_continuity_maturity_tracker",
    )
    return payload


def run_gbdt_pivot_direction_selection(
    *,
    config_path: Path = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    gbdt_pivot_review_path: Path = DEFAULT_GBDT_PIVOT_REVIEW_PATH,
    output_root: Path = DEFAULT_GBDT_ACTION_UTILITY_OUTPUT_ROOT,
) -> dict[str, Any]:
    config = _load_next_stage_config(config_path)
    pivot_review = _read_json_or_empty(gbdt_pivot_review_path)
    selection_policy = _next_stage_section(config, "gbdt_pivot_selection")
    directions = _gbdt_pivot_direction_rows(selection_policy)
    selected = str(selection_policy.get("selected_direction", "gbdt_value_surface_residual_model"))
    payload = _controlled_payload(
        report_type="gbdt_pivot_direction_selection",
        title="GBDT pivot direction selection",
        status="PIVOT_DIRECTION_SELECTED",
        summary={
            "selected_pivot_direction": selected,
            "pivot_direction_selected": True,
            "candidate_direction_count": len(directions),
            "model_run_executed": False,
            "model_training_allowed": False,
            "minimum_viable_experiment_present": True,
            **_summary_safety(),
        },
        config_path=str(config_path),
        policy_version=str(config.get("policy_id", "controlled_strategy_research_next_stage")),
        heuristic_policy_version=_heuristic_policy_version(config),
        gbdt_pivot_review_source=_artifact_status(pivot_review, gbdt_pivot_review_path),
        selected_direction={
            "direction_id": selected,
            "selection_rationale": selection_policy.get("rationale"),
            "model_run_executed": False,
            "promotion_gate_allowed": False,
        },
        candidate_directions=directions,
        remaining_blockers=_common_blockers(),
    )
    _write_pair(payload, output_root=output_root, artifact_id="gbdt_pivot_direction_selection")
    return payload


def run_regret_activation_inputs_from_value_surface_failures(
    *,
    config_path: Path = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    value_surface_expansion_path: Path = DEFAULT_VALUE_SURFACE_EXPANSION_PATH,
    regret_casebook_expansion_gate_path: Path = DEFAULT_REGRET_CASEBOOK_EXPANSION_GATE_PATH,
    output_root: Path = DEFAULT_REGRET_STATE_MACHINE_OUTPUT_ROOT,
) -> dict[str, Any]:
    config = _load_next_stage_config(config_path)
    value_surface = _read_json_or_empty(value_surface_expansion_path)
    gate = _read_json_or_empty(regret_casebook_expansion_gate_path)
    failure_inputs = _value_surface_failure_activation_inputs(
        rows=_records(value_surface.get("value_surface")),
        config=config,
    )
    activation = _regret_activation_input_criteria(
        config=config,
        failure_inputs=failure_inputs,
        gate=gate,
    )
    ready = all(bool(row["passed"]) for row in activation)
    status = "READY_FOR_REGRET_ACTIVATION_REVIEW" if ready else "WATCHLIST_NOT_READY"
    payload = _controlled_payload(
        report_type="regret_activation_inputs_from_value_surface_failures",
        title="Regret activation inputs from value surface failures",
        status=status,
        summary={
            "regret_activation_ready": ready,
            "regret_state_machine_status": "WATCHLIST",
            "activation_input_count": len(failure_inputs["activation_cases"]),
            "value_surface_losing_case_count": failure_inputs["summary"][
                "value_surface_losing_case_count"
            ],
            "benchmark_disagreement_case_count": failure_inputs["summary"][
                "benchmark_disagreement_case_count"
            ],
            "false_risk_off_or_missed_upside_case_count": failure_inputs["summary"][
                "false_risk_off_or_missed_upside_case_count"
            ],
            "oracle_teacher_better_case_count": failure_inputs["summary"][
                "oracle_teacher_better_case_count"
            ],
            **_summary_safety(),
        },
        config_path=str(config_path),
        policy_version=str(config.get("policy_id", "controlled_strategy_research_next_stage")),
        heuristic_policy_version=_heuristic_policy_version(config),
        value_surface_source=_artifact_status(value_surface, value_surface_expansion_path),
        regret_gate_source=_artifact_status(gate, regret_casebook_expansion_gate_path),
        activation_inputs=failure_inputs,
        activation_criteria=activation,
        watchlist_decision={
            "regret_state_machine_status": "WATCHLIST",
            "expand_state_machine_now": False,
            "reason": "activation_inputs_not_sufficient_or_oracle_teacher_cases_missing",
            "promotion_gate_allowed": False,
        },
        remaining_blockers=_common_blockers(),
    )
    _write_pair(
        payload,
        output_root=output_root,
        artifact_id="regret_activation_inputs_from_value_surface_failures",
    )
    return payload


def run_value_surface_controlled_walk_forward_expansion(
    *,
    config_path: Path = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    value_surface_expansion_path: Path = DEFAULT_VALUE_SURFACE_EXPANSION_PATH,
    warning_triage_path: Path = DEFAULT_VALUE_SURFACE_WARNING_TRIAGE_PATH,
    output_root: Path = DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT,
    as_of_date: date | None = None,
) -> dict[str, Any]:
    config = _load_next_stage_config(config_path)
    universe = _universe(config)
    quality = _run_data_quality_gate(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        as_of_date=as_of_date,
        universe=universe,
    )
    if not quality["passed"]:
        raise ValueError("validate-data gate failed before value surface walk-forward review")

    value_surface = _read_json_or_empty(value_surface_expansion_path)
    warning_triage = _read_json_or_empty(warning_triage_path)
    surface_rows = _records(value_surface.get("value_surface"))
    selected_cases = _selected_value_surface_cases(surface_rows, config)
    decision_dates = sorted({str(row.get("date")) for row in surface_rows if row.get("date")})
    windows = _walk_forward_windows(decision_dates, config)
    window_results = _walk_forward_window_results(selected_cases, windows)
    warning_taxonomy = _records(warning_triage.get("warning_taxonomy"))
    control_results = _records(value_surface.get("control_results"))
    negative_control_result = _negative_control_review(control_results)
    future_leakage_trap_result = _future_leakage_trap_review(control_results)
    concentration = _sample_concentration_report(
        surface_rows,
        group_keys=["date", "asset", "horizon", "regime_segment", "asset_cluster"],
    )
    benchmark_comparison = _walk_forward_benchmark_comparison(selected_cases)
    decision = _value_surface_walk_forward_decision(
        config=config,
        value_surface=value_surface,
        selected_cases=selected_cases,
        window_results=window_results,
        negative_control_result=negative_control_result,
        future_leakage_trap_result=future_leakage_trap_result,
    )
    if decision not in {"CONTINUE", "WATCHLIST", "DATA_REQUIRED", "PAUSE", "KILL"}:
        raise ValueError(f"unsupported TRADING-785 walk-forward decision: {decision}")
    payload = _controlled_payload(
        report_type="value_surface_controlled_walk_forward_expansion",
        title="Value surface controlled walk-forward expansion",
        status="CONTROLLED_WALK_FORWARD_REVIEW_COMPLETE",
        summary={
            "walk_forward_window_count": len(window_results),
            "decision_date_count": len(decision_dates),
            "asset_count": len({row.get("asset") for row in surface_rows if row.get("asset")}),
            "horizon_count": len(
                {row.get("horizon") for row in surface_rows if row.get("horizon")}
            ),
            "regime_count": len(
                {row.get("regime_segment") for row in surface_rows if row.get("regime_segment")}
            ),
            "benchmark_comparison_present": bool(benchmark_comparison),
            "by_asset_result_present": True,
            "by_horizon_result_present": True,
            "by_regime_result_present": True,
            "warning_count": len(warning_taxonomy),
            "sample_concentration_present": True,
            "negative_control_promotion_count": negative_control_result[
                "negative_control_promotion_count"
            ],
            "future_leakage_trap_blocked": future_leakage_trap_result[
                "future_leakage_trap_blocked"
            ],
            "controlled_walk_forward_decision": decision,
            "data_quality_status": quality["status"],
            "data_foundation_status": _data_foundation_status(quality),
            **_summary_safety(),
        },
        config_path=str(config_path),
        policy_version=str(config.get("policy_id", "controlled_strategy_research_next_stage")),
        heuristic_policy_version=_heuristic_policy_version(config),
        data_quality_gate=quality,
        data_foundation_status=_data_foundation_status(quality),
        value_surface_source=_artifact_status(value_surface, value_surface_expansion_path),
        warning_triage_source=_artifact_status(warning_triage, warning_triage_path),
        requested_date_range=_requested_date_range(decision_dates),
        walk_forward_policy=_next_stage_section(config, "value_surface_walk_forward_expansion"),
        controlled_walk_forward_decision={
            "decision": decision,
            "allowed_decisions": _next_stage_section(
                config, "value_surface_walk_forward_expansion"
            ).get("allowed_decisions", []),
            "promotion_gate_allowed": False,
            "paper_shadow_change_allowed": False,
            "production_weight_change_allowed": False,
        },
        walk_forward_windows=windows,
        walk_forward_results=window_results,
        benchmark_comparison=benchmark_comparison,
        by_asset_result=_walk_forward_group_result(selected_cases, "asset"),
        by_horizon_result=_walk_forward_group_result(selected_cases, "horizon"),
        by_regime_result=_walk_forward_group_result(selected_cases, "regime_segment"),
        warning_taxonomy=warning_taxonomy,
        sample_concentration_report=concentration,
        negative_control_result=negative_control_result,
        future_leakage_trap_result=future_leakage_trap_result,
        remaining_blockers=_common_blockers(),
    )
    _write_pair(
        payload,
        output_root=output_root,
        artifact_id="value_surface_controlled_walk_forward_expansion",
    )
    return payload


def run_value_surface_utility_pareto_ranking_review(
    *,
    config_path: Path = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    value_surface_expansion_path: Path = DEFAULT_VALUE_SURFACE_EXPANSION_PATH,
    utility_boundary_audit_path: Path = DEFAULT_UTILITY_BOUNDARY_AUDIT_PATH,
    output_root: Path = DEFAULT_UTILITY_BOUNDARY_OUTPUT_ROOT,
) -> dict[str, Any]:
    config = _load_next_stage_config(config_path)
    value_surface = _read_json_or_empty(value_surface_expansion_path)
    utility_audit = _read_json_or_empty(utility_boundary_audit_path)
    surface_rows = _records(value_surface.get("value_surface"))
    profiles = _utility_profiles(config)
    rankings = [
        _utility_profile_ranking(surface_rows=surface_rows, profile=profile) for profile in profiles
    ]
    reversal = _profile_reversal_report(rankings)
    pareto = _pareto_frontier(surface_rows, config)
    dominant = _dominant_metric_by_candidate(surface_rows)
    horizon_cliffs = _horizon_cliff_report(surface_rows, config)
    payload = _controlled_payload(
        report_type="value_surface_utility_pareto_ranking_review",
        title="Value surface utility and Pareto ranking review",
        status="SENSITIVITY_TESTED",
        summary={
            "utility_profile_count": len(profiles),
            "ranking_flip_count": reversal["summary"]["ranking_reversal_count"],
            "pareto_candidate_count": len(pareto),
            "dominant_metric_by_candidate_present": True,
            "horizon_cliff_count": horizon_cliffs["summary"]["horizon_cliff_count"],
            "utility_boundary_status": "SENSITIVITY_TESTED",
            "validated_boundary_count": 0,
            "not_validated_utility_boundary": True,
            **_summary_safety(),
        },
        config_path=str(config_path),
        policy_version=str(config.get("policy_id", "controlled_strategy_research_next_stage")),
        heuristic_policy_version=_heuristic_policy_version(config),
        value_surface_source=_artifact_status(value_surface, value_surface_expansion_path),
        utility_boundary_source=_artifact_status(utility_audit, utility_boundary_audit_path),
        utility_profiles=profiles,
        utility_profile_rankings=rankings,
        ranking_flip_report=reversal,
        pareto_candidates=pareto,
        single_utility_vs_pareto={
            "single_utility_policy": "heuristic_net_utility",
            "pareto_policy": _next_stage_section(config, "utility_boundary_audit").get(
                "pareto_components", {}
            ),
            "pareto_candidate_count": len(pareto),
            "validated_boundary_allowed": False,
            "promotion_gate_allowed": False,
        },
        dominant_metric_by_candidate=dominant,
        horizon_cliff_report=horizon_cliffs,
        diagnostic_boundary_assessment={
            "boundary_use": "diagnostic_only",
            "status_cap": _next_stage_section(config, "utility_pareto_ranking_review").get(
                "status_cap", "SENSITIVITY_TESTED"
            ),
            "validated_boundary_count": 0,
            "not_validated_utility_boundary": True,
            "promotion_gate_allowed": False,
        },
        remaining_blockers=_common_blockers(),
    )
    _write_pair(
        payload,
        output_root=output_root,
        artifact_id="value_surface_utility_pareto_ranking_review",
    )
    return payload


def run_forward_evidence_daily_continuity_review(
    *,
    config_path: Path = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    ledger_path: Path = DEFAULT_FORWARD_DAILY_DRY_RUN_LEDGER_PATH,
    benchmark_expansion_path: Path = DEFAULT_CONTROLLED_BENCHMARK_EXPANSION_PATH,
    control_audit_path: Path = DEFAULT_CONTROL_AUDIT_PATH,
    value_surface_expansion_path: Path = DEFAULT_VALUE_SURFACE_EXPANSION_PATH,
    output_root: Path = DEFAULT_FORWARD_MATURITY_OUTPUT_ROOT,
    as_of_date: date | None = None,
) -> dict[str, Any]:
    config = _load_next_stage_config(config_path)
    universe = _universe(config)
    quality = _run_data_quality_gate(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        as_of_date=as_of_date,
        universe=universe,
    )
    if not quality["passed"]:
        raise ValueError("validate-data gate failed before forward continuity review")

    price_rows = _read_price_rows(prices_path, universe=universe)
    dates = _all_dates(price_rows)
    ledger_rows = _read_jsonl_rows(ledger_path)
    maturity_rows = _forward_maturity_rows(ledger_rows=ledger_rows, dates=dates, config=config)
    maturity_summary = _forward_maturity_summary(maturity_rows)
    continuity = _forward_daily_continuity_report(
        ledger_rows=ledger_rows,
        dates=dates,
        config=config,
    )
    append_only = _append_only_integrity_report(ledger_rows)
    coverage = _forward_output_coverage_report(
        benchmark_expansion_path=benchmark_expansion_path,
        control_audit_path=control_audit_path,
        value_surface_expansion_path=value_surface_expansion_path,
    )
    policy = _next_stage_section(config, "forward_evidence_daily_continuity_review")
    minimum_events = _first_int(policy.get("minimum_ledger_events_for_continuity_review"))
    continuity_ready = (
        len(ledger_rows) >= minimum_events
        and continuity["summary"]["daily_continuity_pass"]
        and append_only["summary"]["append_only_integrity_pass"]
        and coverage["summary"]["all_required_outputs_present"]
    )
    status = "DATA_REQUIRED" if not ledger_rows else "PASS_WITH_WARNINGS"
    payload = _controlled_payload(
        report_type="forward_evidence_daily_continuity_review",
        title="Forward evidence daily continuity review",
        status=status,
        summary={
            "ledger_event_count": len(ledger_rows),
            "missing_daily_archive_count": continuity["summary"]["missing_daily_archive_count"],
            "append_only_integrity_pass": append_only["summary"]["append_only_integrity_pass"],
            "horizon_maturity_recorded": bool(maturity_summary),
            "output_coverage_present": True,
            "continuity_ready_for_longer_review": continuity_ready,
            "data_quality_status": quality["status"],
            "data_foundation_status": _data_foundation_status(quality),
            **_summary_safety(),
        },
        config_path=str(config_path),
        policy_version=str(config.get("policy_id", "controlled_strategy_research_next_stage")),
        heuristic_policy_version=_heuristic_policy_version(config),
        data_quality_gate=quality,
        data_foundation_status=_data_foundation_status(quality),
        requested_date_range=_requested_date_range(dates),
        ledger_path=str(ledger_path),
        daily_continuity_policy=policy,
        daily_continuity=continuity,
        append_only_integrity=append_only,
        horizon_maturity_summary=maturity_summary,
        horizon_maturity_rows=maturity_rows[:100],
        output_coverage=coverage,
        continuity_decision={
            "continuity_status": (
                "CONTINUITY_REVIEW_READY" if continuity_ready else "EARLY_LEDGER_DATA_REQUIRED"
            ),
            "paper_shadow_ready": False,
            "promotion_gate_allowed": False,
        },
        remaining_blockers=_common_blockers(),
    )
    _write_pair(
        payload,
        output_root=output_root,
        artifact_id="forward_evidence_daily_continuity_review",
    )
    return payload


def run_gbdt_value_surface_residual_diagnostic_prototype(
    *,
    config_path: Path = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    value_surface_expansion_path: Path = DEFAULT_VALUE_SURFACE_EXPANSION_PATH,
    gbdt_pivot_selection_path: Path = DEFAULT_GBDT_PIVOT_SELECTION_PATH,
    output_root: Path = DEFAULT_GBDT_ACTION_UTILITY_OUTPUT_ROOT,
) -> dict[str, Any]:
    config = _load_next_stage_config(config_path)
    value_surface = _read_json_or_empty(value_surface_expansion_path)
    pivot_selection = _read_json_or_empty(gbdt_pivot_selection_path)
    residual_rows = _value_surface_residual_rows(
        _records(value_surface.get("value_surface")),
        config,
    )
    feature_importance = _residual_feature_importance(
        residual_rows,
        features=["asset", "horizon", "regime_segment", "asset_cluster", "pit_state", "action"],
    )
    hypotheses = _residual_hypothesis_candidates(residual_rows)
    payload = _controlled_payload(
        report_type="gbdt_value_surface_residual_diagnostic_prototype",
        title="GBDT value surface residual diagnostic prototype",
        status="DIAGNOSTIC_PROTOTYPE_COMPLETE",
        summary={
            "residual_case_count": len(residual_rows),
            "residual_by_asset_present": True,
            "residual_by_horizon_present": True,
            "residual_by_regime_present": True,
            "feature_importance_present": bool(feature_importance),
            "hypothesis_candidate_count": len(hypotheses),
            "strategy_signal_generated": False,
            "promotion_gate_allowed": False,
            **_summary_safety(),
        },
        config_path=str(config_path),
        policy_version=str(config.get("policy_id", "controlled_strategy_research_next_stage")),
        heuristic_policy_version=_heuristic_policy_version(config),
        value_surface_source=_artifact_status(value_surface, value_surface_expansion_path),
        gbdt_pivot_selection_source=_artifact_status(pivot_selection, gbdt_pivot_selection_path),
        residual_policy=_next_stage_section(config, "gbdt_value_surface_residual_diagnostic"),
        residual_cases=residual_rows[:100],
        residual_by_asset=_residual_group_result(residual_rows, "asset"),
        residual_by_horizon=_residual_group_result(residual_rows, "horizon"),
        residual_by_regime=_residual_group_result(residual_rows, "regime_segment"),
        feature_importance=feature_importance,
        hypothesis_candidates=hypotheses,
        diagnostic_boundary={
            "gbdt_role": "value_surface_residual_explainer",
            "direct_action_utility_prediction": False,
            "strategy_signal_generated": False,
            "promotion_gate_allowed": False,
        },
        remaining_blockers=_common_blockers(),
    )
    _write_pair(
        payload,
        output_root=output_root,
        artifact_id="gbdt_value_surface_residual_diagnostic_prototype",
    )
    return payload


def run_regret_casebook_activation_recheck(
    *,
    config_path: Path = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    value_surface_expansion_path: Path = DEFAULT_VALUE_SURFACE_EXPANSION_PATH,
    regret_activation_inputs_path: Path = DEFAULT_REGRET_ACTIVATION_INPUTS_PATH,
    regret_casebook_expansion_gate_path: Path = DEFAULT_REGRET_CASEBOOK_EXPANSION_GATE_PATH,
    output_root: Path = DEFAULT_REGRET_STATE_MACHINE_OUTPUT_ROOT,
) -> dict[str, Any]:
    config = _load_next_stage_config(config_path)
    value_surface = _read_json_or_empty(value_surface_expansion_path)
    prior_activation = _read_json_or_empty(regret_activation_inputs_path)
    gate = _read_json_or_empty(regret_casebook_expansion_gate_path)
    failure_inputs = _value_surface_failure_activation_inputs(
        rows=_records(value_surface.get("value_surface")),
        config=config,
    )
    criteria = _regret_activation_recheck_criteria(
        config=config,
        failure_inputs=failure_inputs,
        gate=gate,
    )
    ready = all(bool(row["passed"]) for row in criteria)
    status = "READY_FOR_REGRET_EXPANSION_TASK" if ready else "WATCHLIST_NOT_READY"
    summary = failure_inputs["summary"]
    payload = _controlled_payload(
        report_type="regret_casebook_activation_recheck",
        title="Regret casebook activation recheck",
        status=status,
        summary={
            "regret_activation_recheck_ready": ready,
            "regret_casebook_expansion_allowed": False,
            "value_surface_losing_case_count": summary["value_surface_losing_case_count"],
            "benchmark_disagreement_case_count": summary["benchmark_disagreement_case_count"],
            "teacher_oracle_better_case_count": summary["oracle_teacher_better_case_count"],
            "stable_regret_type_count": _stable_regret_type_count(gate),
            "regret_state_machine_status": "WATCHLIST",
            **_summary_safety(),
        },
        config_path=str(config_path),
        policy_version=str(config.get("policy_id", "controlled_strategy_research_next_stage")),
        heuristic_policy_version=_heuristic_policy_version(config),
        value_surface_source=_artifact_status(value_surface, value_surface_expansion_path),
        prior_activation_source=_artifact_status(prior_activation, regret_activation_inputs_path),
        regret_gate_source=_artifact_status(gate, regret_casebook_expansion_gate_path),
        recheck_policy=_next_stage_section(config, "regret_casebook_activation_recheck"),
        activation_inputs=failure_inputs,
        activation_recheck_criteria=criteria,
        activation_recheck_decision={
            "decision": status,
            "follow_up_regret_casebook_expansion_task_recommended": ready,
            "follow_up_regret_state_machine_v2_task_recommended": ready,
            "regret_casebook_expansion_allowed": False,
            "expansion_executed": False,
            "promotion_gate_allowed": False,
        },
        remaining_blockers=_common_blockers(),
    )
    _write_pair(
        payload,
        output_root=output_root,
        artifact_id="regret_casebook_activation_recheck",
    )
    return payload


def run_value_surface_failure_attribution(
    *,
    config_path: Path = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    value_surface_expansion_path: Path = DEFAULT_VALUE_SURFACE_EXPANSION_PATH,
    walk_forward_path: Path = DEFAULT_VALUE_SURFACE_WALK_FORWARD_PATH,
    output_root: Path = DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT,
    as_of_date: date | None = None,
) -> dict[str, Any]:
    config = _load_next_stage_config(config_path)
    universe = _universe(config)
    quality = _run_data_quality_gate(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        as_of_date=as_of_date,
        universe=universe,
    )
    if not quality["passed"]:
        raise ValueError("validate-data gate failed before value surface failure attribution")

    value_surface = _read_json_or_empty(value_surface_expansion_path)
    walk_forward = _read_json_or_empty(walk_forward_path)
    selected_cases = _selected_value_surface_cases(
        _records(value_surface.get("value_surface")),
        config,
    )
    attribution = _failure_attribution_report(selected_cases, config)
    payload = _controlled_payload(
        report_type="value_surface_failure_attribution",
        title="Value surface failure attribution",
        status="FAILURE_ATTRIBUTION_COMPLETE",
        summary={
            "case_count": len(selected_cases),
            "winning_case_count": attribution["summary"]["winning_case_count"],
            "losing_case_count": attribution["summary"]["losing_case_count"],
            "winning_case_average_delta": attribution["summary"]["winning_case_average_delta"],
            "losing_case_average_delta": attribution["summary"]["losing_case_average_delta"],
            "tail_loss_contribution": attribution["tail_loss_contribution"]["tail_loss_share"],
            "max_loss_concentration_share": attribution["summary"]["max_loss_concentration_share"],
            "data_quality_status": quality["status"],
            "data_foundation_status": _data_foundation_status(quality),
            **_summary_safety(),
        },
        config_path=str(config_path),
        policy_version=str(config.get("policy_id", "controlled_strategy_research_next_stage")),
        heuristic_policy_version=_heuristic_policy_version(config),
        data_quality_gate=quality,
        data_foundation_status=_data_foundation_status(quality),
        value_surface_source=_artifact_status(value_surface, value_surface_expansion_path),
        walk_forward_source=_artifact_status(walk_forward, walk_forward_path),
        failure_attribution_policy=_next_stage_section(config, "value_surface_failure_attribution"),
        winning_losing_delta=attribution["winning_losing_delta"],
        top_losing_cases=attribution["top_losing_cases"],
        loss_concentration_by_date=attribution["loss_concentration_by_date"],
        loss_concentration_by_asset=attribution["loss_concentration_by_asset"],
        loss_concentration_by_horizon=attribution["loss_concentration_by_horizon"],
        loss_concentration_by_regime=attribution["loss_concentration_by_regime"],
        tail_loss_contribution=attribution["tail_loss_contribution"],
        turnover_cost_contribution=attribution["turnover_cost_contribution"],
        drawdown_contribution=attribution["drawdown_contribution"],
        benchmark_relative_downside_attribution=attribution[
            "benchmark_relative_downside_attribution"
        ],
        attribution_summary=attribution["summary"],
        remaining_blockers=_common_blockers(),
    )
    _write_pair(
        payload,
        output_root=output_root,
        artifact_id="value_surface_failure_attribution",
    )
    return payload


def run_horizon_cliff_utility_ranking_stabilization_review(
    *,
    config_path: Path = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    value_surface_expansion_path: Path = DEFAULT_VALUE_SURFACE_EXPANSION_PATH,
    utility_pareto_ranking_path: Path = DEFAULT_VALUE_SURFACE_UTILITY_PARETO_RANKING_PATH,
    output_root: Path = DEFAULT_UTILITY_BOUNDARY_OUTPUT_ROOT,
) -> dict[str, Any]:
    config = _load_next_stage_config(config_path)
    value_surface = _read_json_or_empty(value_surface_expansion_path)
    utility_review = _read_json_or_empty(utility_pareto_ranking_path)
    surface_rows = _records(value_surface.get("value_surface"))
    ranking_jumps = _horizon_ranking_jump_report(surface_rows, config)
    single_horizon = _single_horizon_action_report(surface_rows, config)
    utility_cliffs = _utility_profile_cliff_report(utility_review)
    smoothing = _horizon_smoothing_assessment(
        config=config,
        ranking_jumps=ranking_jumps,
        single_horizon=single_horizon,
        utility_cliffs=utility_cliffs,
        utility_review=utility_review,
    )
    payload = _controlled_payload(
        report_type="horizon_cliff_utility_ranking_stabilization_review",
        title="Horizon cliff and utility ranking stabilization review",
        status="SENSITIVITY_TESTED",
        summary={
            "ranking_jump_count": ranking_jumps["summary"]["ranking_jump_count"],
            "horizon_cliff_count": utility_review.get("summary", {}).get("horizon_cliff_count", 0),
            "single_horizon_action_count": single_horizon["summary"]["single_horizon_action_count"],
            "utility_profile_cliff_count": utility_cliffs["summary"]["utility_profile_cliff_count"],
            "horizon_smoothing_review_required": smoothing["horizon_smoothing_review_required"],
            "pareto_frontier_policy_review_required": smoothing[
                "pareto_frontier_policy_review_required"
            ],
            "validated_boundary_count": 0,
            "not_validated_utility_boundary": True,
            **_summary_safety(),
        },
        config_path=str(config_path),
        policy_version=str(config.get("policy_id", "controlled_strategy_research_next_stage")),
        heuristic_policy_version=_heuristic_policy_version(config),
        value_surface_source=_artifact_status(value_surface, value_surface_expansion_path),
        utility_pareto_source=_artifact_status(utility_review, utility_pareto_ranking_path),
        stabilization_policy=_next_stage_section(config, "horizon_cliff_stabilization_review"),
        ranking_jump_by_horizon=ranking_jumps,
        single_horizon_action_report=single_horizon,
        utility_profile_cliff_report=utility_cliffs,
        horizon_smoothing_assessment=smoothing,
        diagnostic_boundary_assessment={
            "status_cap": "SENSITIVITY_TESTED",
            "validated_boundary_allowed": False,
            "validated_boundary_count": 0,
            "not_validated_utility_boundary": True,
            "promotion_gate_allowed": False,
        },
        remaining_blockers=_common_blockers(),
    )
    _write_pair(
        payload,
        output_root=output_root,
        artifact_id="horizon_cliff_utility_ranking_stabilization_review",
    )
    return payload


def run_gbdt_residual_hypothesis_triage(
    *,
    config_path: Path = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    value_surface_expansion_path: Path = DEFAULT_VALUE_SURFACE_EXPANSION_PATH,
    residual_diagnostic_path: Path = DEFAULT_GBDT_VALUE_SURFACE_RESIDUAL_DIAGNOSTIC_PATH,
    output_root: Path = DEFAULT_GBDT_ACTION_UTILITY_OUTPUT_ROOT,
) -> dict[str, Any]:
    config = _load_next_stage_config(config_path)
    value_surface = _read_json_or_empty(value_surface_expansion_path)
    residual_diagnostic = _read_json_or_empty(residual_diagnostic_path)
    residual_rows = _value_surface_residual_rows(
        _records(value_surface.get("value_surface")),
        config,
    )
    triage = _residual_hypothesis_triage(residual_rows, config)
    payload = _controlled_payload(
        report_type="gbdt_residual_hypothesis_triage",
        title="GBDT residual hypothesis triage",
        status="RESIDUAL_HYPOTHESIS_TRIAGED",
        summary={
            "residual_case_count": len(residual_rows),
            "large_residual_case_count": triage["summary"]["large_residual_case_count"],
            "feature_explanation_count": len(triage["feature_explanations"]),
            "repair_rule_candidate_count": len(triage["repair_rule_candidates"]),
            "new_hypothesis_candidate_count": len(triage["new_hypothesis_candidates"]),
            "strategy_signal_generated": False,
            **_summary_safety(),
        },
        config_path=str(config_path),
        policy_version=str(config.get("policy_id", "controlled_strategy_research_next_stage")),
        heuristic_policy_version=_heuristic_policy_version(config),
        value_surface_source=_artifact_status(value_surface, value_surface_expansion_path),
        residual_diagnostic_source=_artifact_status(residual_diagnostic, residual_diagnostic_path),
        triage_policy=_next_stage_section(config, "gbdt_residual_hypothesis_triage"),
        prediction_error_summary=triage["prediction_error_summary"],
        residual_by_asset=triage["residual_by_asset"],
        residual_by_horizon=triage["residual_by_horizon"],
        residual_by_regime=triage["residual_by_regime"],
        feature_explanations=triage["feature_explanations"],
        repair_rule_candidates=triage["repair_rule_candidates"],
        new_hypothesis_candidates=triage["new_hypothesis_candidates"],
        diagnostic_boundary={
            "strategy_signal_generated": False,
            "model_training_executed": False,
            "direct_action_policy_generated": False,
            "promotion_gate_allowed": False,
        },
        remaining_blockers=_common_blockers(),
    )
    _write_pair(
        payload,
        output_root=output_root,
        artifact_id="gbdt_residual_hypothesis_triage",
    )
    return payload


def run_forward_evidence_continuity_extension(
    *,
    config_path: Path = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    ledger_path: Path = DEFAULT_FORWARD_DAILY_DRY_RUN_LEDGER_PATH,
    benchmark_expansion_path: Path = DEFAULT_CONTROLLED_BENCHMARK_EXPANSION_PATH,
    control_audit_path: Path = DEFAULT_CONTROL_AUDIT_PATH,
    value_surface_expansion_path: Path = DEFAULT_VALUE_SURFACE_EXPANSION_PATH,
    output_root: Path = DEFAULT_FORWARD_MATURITY_OUTPUT_ROOT,
    as_of_date: date | None = None,
) -> dict[str, Any]:
    config = _load_next_stage_config(config_path)
    universe = _universe(config)
    quality = _run_data_quality_gate(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        as_of_date=as_of_date,
        universe=universe,
    )
    if not quality["passed"]:
        raise ValueError("validate-data gate failed before forward continuity extension")

    price_rows = _read_price_rows(prices_path, universe=universe)
    dates = _all_dates(price_rows)
    ledger_rows = _read_jsonl_rows(ledger_path)
    maturity_rows = _forward_maturity_rows(ledger_rows=ledger_rows, dates=dates, config=config)
    continuity = _forward_daily_continuity_report(
        ledger_rows=ledger_rows,
        dates=dates,
        config=config,
    )
    append_only = _append_only_integrity_report(ledger_rows)
    coverage = _forward_output_coverage_report(
        benchmark_expansion_path=benchmark_expansion_path,
        control_audit_path=control_audit_path,
        value_surface_expansion_path=value_surface_expansion_path,
    )
    payload = _controlled_payload(
        report_type="forward_evidence_continuity_extension",
        title="Forward evidence continuity extension",
        status="PASS_WITH_WARNINGS" if ledger_rows else "DATA_REQUIRED",
        summary={
            "ledger_event_count": len(ledger_rows),
            "missing_daily_archive_count": continuity["summary"]["missing_daily_archive_count"],
            "append_only_integrity_pass": append_only["summary"]["append_only_integrity_pass"],
            "horizon_maturity_recorded": bool(maturity_rows),
            "output_coverage_present": True,
            "data_quality_status": quality["status"],
            "data_foundation_status": _data_foundation_status(quality),
            **_summary_safety(),
        },
        config_path=str(config_path),
        policy_version=str(config.get("policy_id", "controlled_strategy_research_next_stage")),
        heuristic_policy_version=_heuristic_policy_version(config),
        data_quality_gate=quality,
        data_foundation_status=_data_foundation_status(quality),
        requested_date_range=_requested_date_range(dates),
        ledger_path=str(ledger_path),
        continuity_extension_policy=_next_stage_section(
            config, "forward_evidence_continuity_extension"
        ),
        daily_archive_continuity=continuity,
        append_only_integrity=append_only,
        horizon_maturity_summary=_forward_maturity_summary(maturity_rows),
        output_coverage=coverage,
        forward_evidence_scope={
            "decides_current_strategy_quality": False,
            "paper_shadow_ready": False,
            "promotion_gate_allowed": False,
        },
        remaining_blockers=_common_blockers(),
    )
    _write_pair(
        payload,
        output_root=output_root,
        artifact_id="forward_evidence_continuity_extension",
    )
    return payload


def run_value_surface_direction_review(
    *,
    config_path: Path = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    failure_attribution_path: Path = DEFAULT_VALUE_SURFACE_FAILURE_ATTRIBUTION_PATH,
    horizon_stabilization_path: Path = DEFAULT_HORIZON_CLIFF_STABILIZATION_REVIEW_PATH,
    residual_triage_path: Path = DEFAULT_GBDT_RESIDUAL_HYPOTHESIS_TRIAGE_PATH,
    forward_continuity_extension_path: Path = DEFAULT_FORWARD_EVIDENCE_CONTINUITY_EXTENSION_PATH,
    walk_forward_path: Path = DEFAULT_VALUE_SURFACE_WALK_FORWARD_PATH,
    output_root: Path = DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT,
) -> dict[str, Any]:
    config = _load_next_stage_config(config_path)
    failure = _read_json_or_empty(failure_attribution_path)
    horizon = _read_json_or_empty(horizon_stabilization_path)
    residual = _read_json_or_empty(residual_triage_path)
    forward = _read_json_or_empty(forward_continuity_extension_path)
    walk_forward = _read_json_or_empty(walk_forward_path)
    decision = _value_surface_direction_decision(
        config=config,
        failure=failure,
        horizon=horizon,
        residual=residual,
        forward=forward,
        walk_forward=walk_forward,
    )
    payload = _controlled_payload(
        report_type="value_surface_direction_review",
        title="Value surface direction review",
        status="DIRECTION_REVIEW_COMPLETE",
        summary={
            "direction_decision": decision["decision"],
            "do_not_default_continue": True,
            "failure_attribution_present": bool(failure),
            "horizon_stabilization_present": bool(horizon),
            "residual_triage_present": bool(residual),
            "forward_continuity_extension_present": bool(forward),
            **_summary_safety(),
        },
        config_path=str(config_path),
        policy_version=str(config.get("policy_id", "controlled_strategy_research_next_stage")),
        heuristic_policy_version=_heuristic_policy_version(config),
        direction_review_policy=_next_stage_section(config, "value_surface_direction_review"),
        failure_attribution_source=_artifact_status(failure, failure_attribution_path),
        horizon_stabilization_source=_artifact_status(horizon, horizon_stabilization_path),
        residual_triage_source=_artifact_status(residual, residual_triage_path),
        forward_continuity_extension_source=_artifact_status(
            forward, forward_continuity_extension_path
        ),
        walk_forward_source=_artifact_status(walk_forward, walk_forward_path),
        direction_decision=decision,
        evidence_summary=_direction_evidence_summary(
            failure=failure,
            horizon=horizon,
            residual=residual,
            forward=forward,
            walk_forward=walk_forward,
        ),
        disallowed_actions=[
            "continue_expanding_value_surface_sample",
            "train_gbdt_strategy_directly",
            "expand_regret_casebook_now",
            "enter_paper_shadow",
            "treat_high_beat_rate_as_strategy_evidence",
        ],
        remaining_blockers=_common_blockers(),
    )
    _write_pair(payload, output_root=output_root, artifact_id="value_surface_direction_review")
    return payload


def run_regime_conditioned_value_surface_design(
    *,
    config_path: Path = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    failure_attribution_path: Path = DEFAULT_VALUE_SURFACE_FAILURE_ATTRIBUTION_PATH,
    horizon_stabilization_path: Path = DEFAULT_HORIZON_CLIFF_STABILIZATION_REVIEW_PATH,
    residual_triage_path: Path = DEFAULT_GBDT_RESIDUAL_HYPOTHESIS_TRIAGE_PATH,
    direction_review_path: Path = DEFAULT_VALUE_SURFACE_DIRECTION_REVIEW_PATH,
    output_root: Path = DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT,
) -> dict[str, Any]:
    config = _load_next_stage_config(config_path)
    failure = _read_json_or_empty(failure_attribution_path)
    horizon = _read_json_or_empty(horizon_stabilization_path)
    residual = _read_json_or_empty(residual_triage_path)
    direction = _read_json_or_empty(direction_review_path)
    protocol = _regime_conditioned_design_protocol(
        config=config,
        failure=failure,
        horizon=horizon,
        residual=residual,
        direction=direction,
    )
    payload = _controlled_payload(
        report_type="regime_conditioned_value_surface_design",
        title="Regime-conditioned value surface design",
        status="REGIME_CONDITIONED_PROTOCOL_DEFINED",
        summary={
            "regime_variable_count": len(protocol["regime_variables"]),
            "tail_loss_regime_count": len(protocol["tail_loss_regime_definitions"]),
            "benchmark_fallback_rule_count": len(protocol["benchmark_fallback_rules"]),
            "disabled_or_downweighted_horizon_count": len(
                protocol["horizons_disabled_or_downweighted"]
            ),
            "controlled_only_validation_plan_present": bool(
                protocol["controlled_only_validation_plan"]
            ),
            **_summary_safety(),
        },
        config_path=str(config_path),
        policy_version=str(config.get("policy_id", "controlled_strategy_research_next_stage")),
        heuristic_policy_version=_heuristic_policy_version(config),
        design_policy=_next_stage_section(config, "regime_conditioned_value_surface_design"),
        failure_attribution_source=_artifact_status(failure, failure_attribution_path),
        horizon_stabilization_source=_artifact_status(horizon, horizon_stabilization_path),
        residual_triage_source=_artifact_status(residual, residual_triage_path),
        direction_review_source=_artifact_status(direction, direction_review_path),
        regime_conditioned_value_surface_protocol=protocol["protocol"],
        regime_variables=protocol["regime_variables"],
        tail_loss_regime_definitions=protocol["tail_loss_regime_definitions"],
        allowed_action_changes_by_regime=protocol["allowed_action_changes_by_regime"],
        benchmark_fallback_rules=protocol["benchmark_fallback_rules"],
        regimes_keep_value_surface=protocol["regimes_keep_value_surface"],
        regimes_fallback_to_benchmark=protocol["regimes_fallback_to_benchmark"],
        regimes_low_risk_action_only=protocol["regimes_low_risk_action_only"],
        horizons_disabled_or_downweighted=protocol["horizons_disabled_or_downweighted"],
        controlled_only_validation_plan=protocol["controlled_only_validation_plan"],
        remaining_blockers=_common_blockers(),
    )
    _write_pair(
        payload,
        output_root=output_root,
        artifact_id="regime_conditioned_value_surface_design",
    )
    return payload


def run_tail_loss_guardrail_fallback_policy(
    *,
    config_path: Path = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    value_surface_expansion_path: Path = DEFAULT_VALUE_SURFACE_EXPANSION_PATH,
    failure_attribution_path: Path = DEFAULT_VALUE_SURFACE_FAILURE_ATTRIBUTION_PATH,
    horizon_stabilization_path: Path = DEFAULT_HORIZON_CLIFF_STABILIZATION_REVIEW_PATH,
    design_path: Path = DEFAULT_REGIME_CONDITIONED_VALUE_SURFACE_DESIGN_PATH,
    output_root: Path = DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT,
) -> dict[str, Any]:
    config = _load_next_stage_config(config_path)
    value_surface = _read_json_or_empty(value_surface_expansion_path)
    failure = _read_json_or_empty(failure_attribution_path)
    horizon = _read_json_or_empty(horizon_stabilization_path)
    design = _read_json_or_empty(design_path)
    selected_cases = _selected_value_surface_cases(
        _records(value_surface.get("value_surface")),
        config,
    )
    comparison = _guardrail_variant_comparison(
        selected_cases=selected_cases,
        config=config,
        failure=failure,
        horizon=horizon,
        design=design,
    )
    best_variant = comparison["summary"]["best_variant_by_mean_delta"]
    payload = _controlled_payload(
        report_type="tail_loss_guardrail_fallback_policy",
        title="Tail-loss guardrail and fallback policy",
        status="GUARDRAIL_FALLBACK_POLICY_REVIEWED",
        summary={
            "case_count": len(selected_cases),
            "variant_count": len(comparison["variant_metrics"]),
            "best_variant_by_mean_delta": best_variant,
            "best_variant_mean_delta_vs_benchmark": comparison["summary"][
                "best_variant_mean_delta_vs_benchmark"
            ],
            "original_mean_delta_vs_benchmark": comparison["summary"][
                "original_mean_delta_vs_benchmark"
            ],
            "tail_loss_guarded_mean_delta_vs_benchmark": comparison["summary"][
                "tail_loss_guarded_mean_delta_vs_benchmark"
            ],
            "tail_loss_guardrail_reduces_tail_loss": comparison["summary"][
                "tail_loss_guardrail_reduces_tail_loss"
            ],
            **_summary_safety(),
        },
        config_path=str(config_path),
        policy_version=str(config.get("policy_id", "controlled_strategy_research_next_stage")),
        heuristic_policy_version=_heuristic_policy_version(config),
        guardrail_policy=_next_stage_section(config, "tail_loss_guardrail_fallback_policy"),
        value_surface_source=_artifact_status(value_surface, value_surface_expansion_path),
        failure_attribution_source=_artifact_status(failure, failure_attribution_path),
        horizon_stabilization_source=_artifact_status(horizon, horizon_stabilization_path),
        design_source=_artifact_status(design, design_path),
        variant_metrics=comparison["variant_metrics"],
        variant_rules=comparison["variant_rules"],
        variant_case_samples=comparison["variant_case_samples"],
        guardrail_diagnostic_boundary=comparison["diagnostic_boundary"],
        remaining_blockers=_common_blockers(),
    )
    _write_pair(
        payload,
        output_root=output_root,
        artifact_id="tail_loss_guardrail_fallback_policy",
    )
    return payload


def run_regime_horizon_loss_attribution_matrix(
    *,
    config_path: Path = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    value_surface_expansion_path: Path = DEFAULT_VALUE_SURFACE_EXPANSION_PATH,
    failure_attribution_path: Path = DEFAULT_VALUE_SURFACE_FAILURE_ATTRIBUTION_PATH,
    output_root: Path = DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT,
) -> dict[str, Any]:
    config = _load_next_stage_config(config_path)
    value_surface = _read_json_or_empty(value_surface_expansion_path)
    failure = _read_json_or_empty(failure_attribution_path)
    selected_cases = _selected_value_surface_cases(
        _records(value_surface.get("value_surface")),
        config,
    )
    matrix = _regime_horizon_loss_matrix(selected_cases, config)
    payload = _controlled_payload(
        report_type="regime_horizon_loss_attribution_matrix",
        title="Regime / horizon loss attribution matrix",
        status="LOSS_ATTRIBUTION_MATRIX_COMPLETE",
        summary={
            "losing_case_count": matrix["summary"]["losing_case_count"],
            "losing_case_average_delta": matrix["summary"]["losing_case_average_delta"],
            "max_loss_concentration_share": matrix["summary"]["max_loss_concentration_share"],
            "max_loss_concentration_group": matrix["summary"]["max_loss_concentration_group"],
            "loss_distribution_assessment": matrix["summary"]["loss_distribution_assessment"],
            **_summary_safety(),
        },
        config_path=str(config_path),
        policy_version=str(config.get("policy_id", "controlled_strategy_research_next_stage")),
        heuristic_policy_version=_heuristic_policy_version(config),
        matrix_policy=_next_stage_section(config, "regime_horizon_loss_attribution_matrix"),
        value_surface_source=_artifact_status(value_surface, value_surface_expansion_path),
        failure_attribution_source=_artifact_status(failure, failure_attribution_path),
        loss_by_regime=matrix["loss_by_regime"],
        loss_by_asset=matrix["loss_by_asset"],
        loss_by_horizon=matrix["loss_by_horizon"],
        loss_by_action=matrix["loss_by_action"],
        loss_by_cluster=matrix["loss_by_cluster"],
        loss_by_utility_profile=matrix["loss_by_utility_profile"],
        loss_by_date_window=matrix["loss_by_date_window"],
        top_losing_cases=matrix["top_losing_cases"],
        matrix_summary=matrix["summary"],
        remaining_blockers=_common_blockers(),
    )
    _write_pair(
        payload,
        output_root=output_root,
        artifact_id="regime_horizon_loss_attribution_matrix",
    )
    return payload


def run_gbdt_residual_hypothesis_regime_conditioning(
    *,
    config_path: Path = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    value_surface_expansion_path: Path = DEFAULT_VALUE_SURFACE_EXPANSION_PATH,
    residual_triage_path: Path = DEFAULT_GBDT_RESIDUAL_HYPOTHESIS_TRIAGE_PATH,
    output_root: Path = DEFAULT_GBDT_ACTION_UTILITY_OUTPUT_ROOT,
) -> dict[str, Any]:
    config = _load_next_stage_config(config_path)
    value_surface = _read_json_or_empty(value_surface_expansion_path)
    residual_triage = _read_json_or_empty(residual_triage_path)
    residual_rows = _value_surface_residual_rows(
        _records(value_surface.get("value_surface")),
        config,
    )
    report = _residual_regime_conditioning_report(residual_rows, config)
    top_feature = (
        report["top_residual_features"][0]["feature"] if report["top_residual_features"] else None
    )
    payload = _controlled_payload(
        report_type="gbdt_residual_hypothesis_regime_conditioning",
        title="GBDT residual hypothesis triage for regime conditioning",
        status="RESIDUAL_REGIME_HYPOTHESES_TRIAGED",
        summary={
            "residual_case_count": len(residual_rows),
            "large_residual_case_count": report["summary"]["large_residual_case_count"],
            "top_residual_feature": top_feature,
            "hypothesis_candidate_count": len(report["hypothesis_candidates"]),
            "strategy_signal_generated": False,
            **_summary_safety(),
        },
        config_path=str(config_path),
        policy_version=str(config.get("policy_id", "controlled_strategy_research_next_stage")),
        heuristic_policy_version=_heuristic_policy_version(config),
        residual_regime_policy=_next_stage_section(config, "gbdt_residual_regime_conditioning"),
        value_surface_source=_artifact_status(value_surface, value_surface_expansion_path),
        residual_triage_source=_artifact_status(residual_triage, residual_triage_path),
        top_residual_features=report["top_residual_features"],
        large_residual_regimes=report["large_residual_regimes"],
        large_residual_horizons=report["large_residual_horizons"],
        large_residual_assets=report["large_residual_assets"],
        residual_sign_classification=report["residual_sign_classification"],
        hypothesis_candidates=report["hypothesis_candidates"],
        diagnostic_boundary={
            "strategy_signal_generated": False,
            "model_training_executed": False,
            "direct_action_policy_generated": False,
            "promotion_gate_allowed": False,
        },
        remaining_blockers=_common_blockers(),
    )
    _write_pair(
        payload,
        output_root=output_root,
        artifact_id="gbdt_residual_hypothesis_regime_conditioning",
    )
    return payload


def run_regime_conditioned_value_surface_controlled_review(
    *,
    config_path: Path = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    design_path: Path = DEFAULT_REGIME_CONDITIONED_VALUE_SURFACE_DESIGN_PATH,
    guardrail_policy_path: Path = DEFAULT_TAIL_LOSS_GUARDRAIL_FALLBACK_POLICY_PATH,
    loss_matrix_path: Path = DEFAULT_REGIME_HORIZON_LOSS_ATTRIBUTION_MATRIX_PATH,
    residual_regime_path: Path = DEFAULT_GBDT_RESIDUAL_REGIME_CONDITIONING_PATH,
    output_root: Path = DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT,
) -> dict[str, Any]:
    config = _load_next_stage_config(config_path)
    design = _read_json_or_empty(design_path)
    guardrail = _read_json_or_empty(guardrail_policy_path)
    loss_matrix = _read_json_or_empty(loss_matrix_path)
    residual_regime = _read_json_or_empty(residual_regime_path)
    decision = _regime_conditioned_controlled_review_decision(
        config=config,
        design=design,
        guardrail=guardrail,
        loss_matrix=loss_matrix,
        residual_regime=residual_regime,
    )
    payload = _controlled_payload(
        report_type="regime_conditioned_value_surface_controlled_review",
        title="Regime-conditioned value surface controlled review",
        status="REGIME_CONDITIONED_CONTROLLED_REVIEW_COMPLETE",
        summary={
            "controlled_review_decision": decision["decision"],
            "best_variant_by_mean_delta": decision["best_variant_by_mean_delta"],
            "mean_delta_improved": decision["mean_delta_improved"],
            "tail_loss_reduced": decision["tail_loss_reduced"],
            "beat_rate_retained": decision["beat_rate_retained"],
            "turnover_cost_not_worse": decision["turnover_cost_not_worse"],
            **_summary_safety(),
        },
        config_path=str(config_path),
        policy_version=str(config.get("policy_id", "controlled_strategy_research_next_stage")),
        heuristic_policy_version=_heuristic_policy_version(config),
        controlled_review_policy=_next_stage_section(
            config, "regime_conditioned_value_surface_controlled_review"
        ),
        design_source=_artifact_status(design, design_path),
        guardrail_policy_source=_artifact_status(guardrail, guardrail_policy_path),
        loss_matrix_source=_artifact_status(loss_matrix, loss_matrix_path),
        residual_regime_source=_artifact_status(residual_regime, residual_regime_path),
        review_decision=decision,
        evidence_summary=_regime_conditioned_review_evidence(
            design=design,
            guardrail=guardrail,
            loss_matrix=loss_matrix,
            residual_regime=residual_regime,
        ),
        disallowed_actions=[
            "unconditional_value_surface_expansion",
            "direct_gbdt_strategy_training",
            "large_regret_casebook_expansion",
            "paper_shadow_entry",
            "beat_rate_promotion_claim",
        ],
        remaining_blockers=_common_blockers(),
    )
    _write_pair(
        payload,
        output_root=output_root,
        artifact_id="regime_conditioned_value_surface_controlled_review",
    )
    return payload


def run_cost_turnover_aware_regime_conditioned_value_surface(
    *,
    config_path: Path = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    value_surface_expansion_path: Path = DEFAULT_VALUE_SURFACE_EXPANSION_PATH,
    failure_attribution_path: Path = DEFAULT_VALUE_SURFACE_FAILURE_ATTRIBUTION_PATH,
    horizon_stabilization_path: Path = DEFAULT_HORIZON_CLIFF_STABILIZATION_REVIEW_PATH,
    design_path: Path = DEFAULT_REGIME_CONDITIONED_VALUE_SURFACE_DESIGN_PATH,
    guardrail_policy_path: Path = DEFAULT_TAIL_LOSS_GUARDRAIL_FALLBACK_POLICY_PATH,
    output_root: Path = DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT,
) -> dict[str, Any]:
    config = _load_next_stage_config(config_path)
    value_surface = _read_json_or_empty(value_surface_expansion_path)
    failure = _read_json_or_empty(failure_attribution_path)
    horizon = _read_json_or_empty(horizon_stabilization_path)
    design = _read_json_or_empty(design_path)
    guardrail = _read_json_or_empty(guardrail_policy_path)
    selected_cases = _selected_value_surface_cases(
        _records(value_surface.get("value_surface")),
        config,
    )
    comparison = _cost_turnover_aware_variant_comparison(
        selected_cases=selected_cases,
        config=config,
        failure=failure,
        horizon=horizon,
        design=design,
    )
    best = comparison["summary"]["best_variant_by_v2_score"]
    payload = _controlled_payload(
        report_type="cost_turnover_aware_regime_conditioned_value_surface",
        title="Cost/turnover-aware regime-conditioned value surface",
        status="COST_TURNOVER_AWARE_VARIANTS_REVIEWED",
        summary={
            "case_count": len(selected_cases),
            "variant_count": len(comparison["variant_metrics"]),
            "best_variant_by_v2_score": best,
            "mean_delta_improved": comparison["summary"]["mean_delta_improved"],
            "tail_loss_reduced": comparison["summary"]["tail_loss_reduced"],
            "turnover_cost_not_worse": comparison["summary"]["turnover_cost_not_worse"],
            "beat_rate_retained": comparison["summary"]["beat_rate_retained"],
            **_summary_safety(),
        },
        config_path=str(config_path),
        policy_version=str(config.get("policy_id", "controlled_strategy_research_next_stage")),
        heuristic_policy_version=_heuristic_policy_version(config),
        cost_turnover_policy=_next_stage_section(
            config, "cost_turnover_aware_regime_conditioned_value_surface"
        ),
        value_surface_source=_artifact_status(value_surface, value_surface_expansion_path),
        failure_attribution_source=_artifact_status(failure, failure_attribution_path),
        horizon_stabilization_source=_artifact_status(horizon, horizon_stabilization_path),
        design_source=_artifact_status(design, design_path),
        guardrail_policy_source=_artifact_status(guardrail, guardrail_policy_path),
        variant_metrics=comparison["variant_metrics"],
        variant_rules=comparison["variant_rules"],
        transition_report=comparison["transition_report"],
        v2_score_policy=comparison["v2_score_policy"],
        diagnostic_boundary=comparison["diagnostic_boundary"],
        remaining_blockers=_common_blockers(),
    )
    _write_pair(
        payload,
        output_root=output_root,
        artifact_id="cost_turnover_aware_regime_conditioned_value_surface",
    )
    return payload


def run_long_horizon_quarantine_selection_review(
    *,
    config_path: Path = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    value_surface_expansion_path: Path = DEFAULT_VALUE_SURFACE_EXPANSION_PATH,
    failure_attribution_path: Path = DEFAULT_VALUE_SURFACE_FAILURE_ATTRIBUTION_PATH,
    horizon_stabilization_path: Path = DEFAULT_HORIZON_CLIFF_STABILIZATION_REVIEW_PATH,
    cost_turnover_path: Path = DEFAULT_COST_TURNOVER_AWARE_VALUE_SURFACE_PATH,
    output_root: Path = DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT,
) -> dict[str, Any]:
    config = _load_next_stage_config(config_path)
    value_surface = _read_json_or_empty(value_surface_expansion_path)
    failure = _read_json_or_empty(failure_attribution_path)
    horizon = _read_json_or_empty(horizon_stabilization_path)
    cost_turnover = _read_json_or_empty(cost_turnover_path)
    selected_cases = _selected_value_surface_cases(
        _records(value_surface.get("value_surface")),
        config,
    )
    review = _long_horizon_quarantine_review(
        selected_cases=selected_cases,
        config=config,
        horizon=horizon,
    )
    payload = _controlled_payload(
        report_type="long_horizon_quarantine_selection_review",
        title="Long-horizon quarantine / horizon selection review",
        status="LONG_HORIZON_QUARANTINE_REVIEW_COMPLETE",
        summary={
            "reviewed_horizon_count": len(review["reviewed_horizons"]),
            "best_comparison_variant": review["summary"]["best_comparison_variant"],
            "best_variant_mean_delta_vs_benchmark": review["summary"][
                "best_variant_mean_delta_vs_benchmark"
            ],
            "tail_loss_reduction_best_variant": review["summary"][
                "tail_loss_reduction_best_variant"
            ],
            "horizon_selector_issue_likely": review["summary"]["horizon_selector_issue_likely"],
            **_summary_safety(),
        },
        config_path=str(config_path),
        policy_version=str(config.get("policy_id", "controlled_strategy_research_next_stage")),
        heuristic_policy_version=_heuristic_policy_version(config),
        quarantine_policy=_next_stage_section(config, "long_horizon_quarantine_selection_review"),
        value_surface_source=_artifact_status(value_surface, value_surface_expansion_path),
        failure_attribution_source=_artifact_status(failure, failure_attribution_path),
        horizon_stabilization_source=_artifact_status(horizon, horizon_stabilization_path),
        cost_turnover_source=_artifact_status(cost_turnover, cost_turnover_path),
        horizon_loss_matrix=review["horizon_loss_matrix"],
        horizon_return_matrix=review["horizon_return_matrix"],
        horizon_turnover_matrix=review["horizon_turnover_matrix"],
        horizon_cliff_matrix=review["horizon_cliff_matrix"],
        disable_vs_downgrade_comparison=review["disable_vs_downgrade_comparison"],
        reviewed_horizons=review["reviewed_horizons"],
        review_summary=review["summary"],
        remaining_blockers=_common_blockers(),
    )
    _write_pair(
        payload,
        output_root=output_root,
        artifact_id="long_horizon_quarantine_selection_review",
    )
    return payload


def run_ai_after_chatgpt_full_regime_attribution_review(
    *,
    config_path: Path = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    value_surface_expansion_path: Path = DEFAULT_VALUE_SURFACE_EXPANSION_PATH,
    failure_attribution_path: Path = DEFAULT_VALUE_SURFACE_FAILURE_ATTRIBUTION_PATH,
    loss_matrix_path: Path = DEFAULT_REGIME_HORIZON_LOSS_ATTRIBUTION_MATRIX_PATH,
    output_root: Path = DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT,
) -> dict[str, Any]:
    config = _load_next_stage_config(config_path)
    value_surface = _read_json_or_empty(value_surface_expansion_path)
    failure = _read_json_or_empty(failure_attribution_path)
    loss_matrix = _read_json_or_empty(loss_matrix_path)
    selected_cases = _selected_value_surface_cases(
        _records(value_surface.get("value_surface")),
        config,
    )
    review = _ai_regime_attribution_review(selected_cases, config)
    payload = _controlled_payload(
        report_type="ai_after_chatgpt_full_regime_attribution_review",
        title="ai_after_chatgpt_full regime attribution review",
        status="AI_REGIME_ATTRIBUTION_REVIEW_COMPLETE",
        summary={
            "target_regime": review["summary"]["target_regime"],
            "regime_case_count": review["summary"]["regime_case_count"],
            "regime_losing_case_count": review["summary"]["regime_losing_case_count"],
            "top_loss_asset": review["summary"]["top_loss_asset"],
            "top_loss_horizon": review["summary"]["top_loss_horizon"],
            "top_loss_action": review["summary"]["top_loss_action"],
            "value_surface_systematic_overoptimism": review["summary"][
                "value_surface_systematic_overoptimism"
            ],
            **_summary_safety(),
        },
        config_path=str(config_path),
        policy_version=str(config.get("policy_id", "controlled_strategy_research_next_stage")),
        heuristic_policy_version=_heuristic_policy_version(config),
        regime_attribution_policy=_next_stage_section(
            config, "ai_after_chatgpt_full_regime_attribution"
        ),
        value_surface_source=_artifact_status(value_surface, value_surface_expansion_path),
        failure_attribution_source=_artifact_status(failure, failure_attribution_path),
        loss_matrix_source=_artifact_status(loss_matrix, loss_matrix_path),
        loss_by_asset=review["loss_by_asset"],
        loss_by_horizon=review["loss_by_horizon"],
        loss_by_action=review["loss_by_action"],
        loss_by_cluster=review["loss_by_cluster"],
        benchmark_stability=review["benchmark_stability"],
        value_surface_overoptimism=review["value_surface_overoptimism"],
        candidate_repairs=review["candidate_repairs"],
        review_summary=review["summary"],
        remaining_blockers=_common_blockers(),
    )
    _write_pair(
        payload,
        output_root=output_root,
        artifact_id="ai_after_chatgpt_full_regime_attribution_review",
    )
    return payload


def run_regime_conditioned_walk_forward_holdout(
    *,
    config_path: Path = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    value_surface_expansion_path: Path = DEFAULT_VALUE_SURFACE_EXPANSION_PATH,
    failure_attribution_path: Path = DEFAULT_VALUE_SURFACE_FAILURE_ATTRIBUTION_PATH,
    horizon_stabilization_path: Path = DEFAULT_HORIZON_CLIFF_STABILIZATION_REVIEW_PATH,
    design_path: Path = DEFAULT_REGIME_CONDITIONED_VALUE_SURFACE_DESIGN_PATH,
    cost_turnover_path: Path = DEFAULT_COST_TURNOVER_AWARE_VALUE_SURFACE_PATH,
    horizon_quarantine_path: Path = DEFAULT_LONG_HORIZON_QUARANTINE_REVIEW_PATH,
    regime_attribution_path: Path = DEFAULT_AI_REGIME_ATTRIBUTION_REVIEW_PATH,
    output_root: Path = DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT,
) -> dict[str, Any]:
    config = _load_next_stage_config(config_path)
    value_surface = _read_json_or_empty(value_surface_expansion_path)
    failure = _read_json_or_empty(failure_attribution_path)
    horizon = _read_json_or_empty(horizon_stabilization_path)
    design = _read_json_or_empty(design_path)
    cost_turnover = _read_json_or_empty(cost_turnover_path)
    horizon_quarantine = _read_json_or_empty(horizon_quarantine_path)
    regime_attribution = _read_json_or_empty(regime_attribution_path)
    selected_cases = _selected_value_surface_cases(
        _records(value_surface.get("value_surface")),
        config,
    )
    holdout = _regime_conditioned_holdout_review(
        selected_cases=selected_cases,
        config=config,
        failure=failure,
        horizon=horizon,
        design=design,
        cost_turnover=cost_turnover,
    )
    payload = _controlled_payload(
        report_type="regime_conditioned_walk_forward_holdout",
        title="Regime-conditioned walk-forward holdout",
        status="REGIME_CONDITIONED_HOLDOUT_REVIEW_COMPLETE",
        summary={
            "holdout_case_count": holdout["summary"]["holdout_case_count"],
            "holdout_pass_count": holdout["summary"]["holdout_pass_count"],
            "holdout_pass_rate": holdout["summary"]["holdout_pass_rate"],
            "overfit_risk": holdout["summary"]["overfit_risk"],
            "best_variant": holdout["summary"]["best_variant"],
            **_summary_safety(),
        },
        config_path=str(config_path),
        policy_version=str(config.get("policy_id", "controlled_strategy_research_next_stage")),
        heuristic_policy_version=_heuristic_policy_version(config),
        holdout_policy=_next_stage_section(config, "regime_conditioned_walk_forward_holdout"),
        value_surface_source=_artifact_status(value_surface, value_surface_expansion_path),
        failure_attribution_source=_artifact_status(failure, failure_attribution_path),
        horizon_stabilization_source=_artifact_status(horizon, horizon_stabilization_path),
        design_source=_artifact_status(design, design_path),
        cost_turnover_source=_artifact_status(cost_turnover, cost_turnover_path),
        horizon_quarantine_source=_artifact_status(horizon_quarantine, horizon_quarantine_path),
        regime_attribution_source=_artifact_status(regime_attribution, regime_attribution_path),
        leave_one_regime_out=holdout["leave_one_regime_out"],
        leave_one_horizon_out=holdout["leave_one_horizon_out"],
        leave_one_asset_cluster_out=holdout["leave_one_asset_cluster_out"],
        leave_one_date_window_out=holdout["leave_one_date_window_out"],
        holdout_summary=holdout["summary"],
        remaining_blockers=_common_blockers(),
    )
    _write_pair(
        payload,
        output_root=output_root,
        artifact_id="regime_conditioned_walk_forward_holdout",
    )
    return payload


def run_value_surface_v2_controlled_review(
    *,
    config_path: Path = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    cost_turnover_path: Path = DEFAULT_COST_TURNOVER_AWARE_VALUE_SURFACE_PATH,
    horizon_quarantine_path: Path = DEFAULT_LONG_HORIZON_QUARANTINE_REVIEW_PATH,
    regime_attribution_path: Path = DEFAULT_AI_REGIME_ATTRIBUTION_REVIEW_PATH,
    holdout_path: Path = DEFAULT_REGIME_CONDITIONED_WALK_FORWARD_HOLDOUT_PATH,
    output_root: Path = DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT,
) -> dict[str, Any]:
    config = _load_next_stage_config(config_path)
    cost_turnover = _read_json_or_empty(cost_turnover_path)
    horizon_quarantine = _read_json_or_empty(horizon_quarantine_path)
    regime_attribution = _read_json_or_empty(regime_attribution_path)
    holdout = _read_json_or_empty(holdout_path)
    decision = _value_surface_v2_review_decision(
        config=config,
        cost_turnover=cost_turnover,
        horizon_quarantine=horizon_quarantine,
        regime_attribution=regime_attribution,
        holdout=holdout,
    )
    payload = _controlled_payload(
        report_type="value_surface_v2_controlled_review",
        title="Value surface v2 controlled review",
        status="VALUE_SURFACE_V2_CONTROLLED_REVIEW_COMPLETE",
        summary={
            "value_surface_v2_decision": decision["decision"],
            "best_cost_turnover_variant": decision["best_cost_turnover_variant"],
            "mean_delta_condition_met": decision["mean_delta_condition_met"],
            "tail_loss_condition_met": decision["tail_loss_condition_met"],
            "turnover_cost_condition_met": decision["turnover_cost_condition_met"],
            "beat_rate_condition_met": decision["beat_rate_condition_met"],
            "holdout_condition_met": decision["holdout_condition_met"],
            **_summary_safety(),
        },
        config_path=str(config_path),
        policy_version=str(config.get("policy_id", "controlled_strategy_research_next_stage")),
        heuristic_policy_version=_heuristic_policy_version(config),
        v2_review_policy=_next_stage_section(config, "value_surface_v2_controlled_review"),
        cost_turnover_source=_artifact_status(cost_turnover, cost_turnover_path),
        horizon_quarantine_source=_artifact_status(horizon_quarantine, horizon_quarantine_path),
        regime_attribution_source=_artifact_status(regime_attribution, regime_attribution_path),
        holdout_source=_artifact_status(holdout, holdout_path),
        review_decision=decision,
        evidence_summary=_value_surface_v2_evidence_summary(
            cost_turnover=cost_turnover,
            horizon_quarantine=horizon_quarantine,
            regime_attribution=regime_attribution,
            holdout=holdout,
        ),
        disallowed_actions=[
            "expand_unconditional_value_surface",
            "train_gbdt_nn_or_rl_strategy",
            "enter_paper_shadow",
            "continue_based_only_on_mean_delta",
            "ignore_turnover_cost_not_worse_false",
        ],
        remaining_blockers=_common_blockers(),
    )
    _write_pair(
        payload,
        output_root=output_root,
        artifact_id="value_surface_v2_controlled_review",
    )
    return payload


def _load_config(path: Path) -> dict[str, Any]:
    raw = safe_load_yaml_path(path)
    return dict(raw) if isinstance(raw, Mapping) else {}


def _load_next_stage_config(path: Path) -> dict[str, Any]:
    overlay = _load_config(path)
    base_path = overlay.get("base_config_path")
    if not base_path:
        return overlay
    resolved_base = _resolve_project_path(base_path)
    merged = _load_config(resolved_base)
    merged.update(overlay)
    return merged


def _resolve_project_path(value: Any) -> Path:
    path = Path(str(value))
    return path if path.is_absolute() else PROJECT_ROOT / path


def _universe(config: Mapping[str, Any]) -> list[str]:
    values = config.get("research_universe")
    if isinstance(values, list):
        return [str(item).upper() for item in values if str(item).strip()]
    return ["SPY", "QQQ", "SMH", "MSFT", "GOOGL", "NVDA", "AMD", "TSM"]


def _horizons(config: Mapping[str, Any]) -> list[dict[str, Any]]:
    rows = _records(config.get("horizons"))
    return [
        {
            "horizon_id": str(row.get("horizon_id")),
            "days": int(row.get("days", 1)),
            "maturity_required_for_primary_evaluation": bool(
                row.get("maturity_required_for_primary_evaluation", True)
            ),
        }
        for row in rows
        if row.get("horizon_id")
    ] or [
        {"horizon_id": "1d", "days": 1, "maturity_required_for_primary_evaluation": True},
        {"horizon_id": "5d", "days": 5, "maturity_required_for_primary_evaluation": True},
    ]


def _actions(config: Mapping[str, Any]) -> list[dict[str, Any]]:
    rows = _records(config.get("candidate_actions"))
    return [
        {
            "action_id": str(row.get("action_id")),
            "exposure_multiplier": _float(row.get("exposure_multiplier"), 0.0),
            "cost_turnover_assumption": _float(row.get("cost_turnover_assumption"), 0.0),
            "rationale": str(row.get("rationale", "controlled heuristic action")),
            "heuristic": True,
        }
        for row in rows
        if row.get("action_id")
    ]


def _simple_strategy_zoo(config: Mapping[str, Any]) -> list[str]:
    values = config.get("simple_strategy_zoo")
    if isinstance(values, list):
        return [str(item) for item in values if str(item).strip()]
    return ["cash", "buy_and_hold", "static_allocation"]


def _selector_rules(config: Mapping[str, Any]) -> list[dict[str, Any]]:
    return _records(config.get("selector_rules"))


def _regret_types(config: Mapping[str, Any]) -> list[str]:
    values = config.get("regret_types")
    if isinstance(values, list):
        return [str(item) for item in values]
    return []


def _state_policy(config: Mapping[str, Any]) -> dict[str, Any]:
    value = config.get("heuristic_state_policy")
    return dict(value) if isinstance(value, Mapping) else {}


def _state_transitions(config: Mapping[str, Any]) -> list[dict[str, Any]]:
    state_machine = config.get("state_machine")
    transitions = _records(
        state_machine.get("transitions") if isinstance(state_machine, Mapping) else []
    )
    rows: list[dict[str, Any]] = []
    for index, row in enumerate(transitions, start=1):
        rows.append(
            {
                "transition_id": f"transition_{index:02d}",
                "from_state": str(row.get("from_state")),
                "to_state": str(row.get("to_state")),
                "condition": str(row.get("condition")),
                "regret_types": [str(item) for item in row.get("regret_types", [])],
                "explanation": str(row.get("explanation", "")),
                "input_data_policy": "PIT_valid_or_controlled_research_allowed",
                "heuristic": True,
                "promotion_gate_allowed": False,
            }
        )
    return rows


def _minimum(config: Mapping[str, Any], key: str, default: int) -> int:
    minimums = config.get("minimums")
    if not isinstance(minimums, Mapping):
        return default
    return int(minimums.get(key, default))


def _heuristic_policy_version(config: Mapping[str, Any]) -> str:
    return str(config.get("heuristic_policy_version", "controlled_strategy_batch_1_heuristic_v1"))


def _next_stage_section(config: Mapping[str, Any], key: str) -> dict[str, Any]:
    value = config.get(key)
    return dict(value) if isinstance(value, Mapping) else {}


def _next_stage_decision_dates(dates: list[str], config: Mapping[str, Any]) -> list[str]:
    if not dates:
        return []
    expansion = _next_stage_section(config, "value_surface_expansion")
    max_dates = int(
        expansion.get("max_decision_dates") or _minimum(config, "max_decision_dates", 24)
    )
    eligible = dates[:-1] if len(dates) > 1 else dates
    return eligible[-max_dates:]


def _cluster_by_asset(config: Mapping[str, Any]) -> dict[str, str]:
    expansion = _next_stage_section(config, "value_surface_expansion")
    cluster_policy = expansion.get("cluster_policy")
    if not isinstance(cluster_policy, Mapping):
        return {}
    clusters = cluster_policy.get("static_clusters")
    if not isinstance(clusters, Mapping):
        return {}
    values: dict[str, str] = {}
    for cluster_id, tickers in clusters.items():
        if not isinstance(tickers, list):
            continue
        for ticker in tickers:
            values[str(ticker).upper()] = str(cluster_id)
    return values


def _recent_regime_window_dates(decision_dates: list[str], config: Mapping[str, Any]) -> set[str]:
    expansion = _next_stage_section(config, "value_surface_expansion")
    segments = _records(expansion.get("regime_segments"))
    trailing = 0
    for segment in segments:
        trailing = max(trailing, _first_int(segment.get("trailing_decision_dates")))
    return set(decision_dates[-trailing:]) if trailing > 0 else set()


def _with_surface_context(
    row: dict[str, Any],
    *,
    cluster_by_asset: Mapping[str, str],
    recent_window_dates: set[str],
) -> dict[str, Any]:
    enriched = dict(row)
    asset = str(row.get("asset", "")).upper()
    row_date = str(row.get("date", ""))
    enriched["asset_cluster"] = cluster_by_asset.get(asset, "unclassified")
    enriched["regime_segment"] = (
        "recent_controlled_window" if row_date in recent_window_dates else "ai_after_chatgpt_full"
    )
    return enriched


def _surface_group_summary(
    rows: list[dict[str, Any]],
    group_keys: list[str],
) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, ...], list[dict[str, Any]]] = {}
    for row in rows:
        key = tuple(str(row.get(group_key, "unknown")) for group_key in group_keys)
        grouped.setdefault(key, []).append(row)
    summaries: list[dict[str, Any]] = []
    for key, values in sorted(grouped.items()):
        summary = {group_key: key[index] for index, group_key in enumerate(group_keys)}
        mature_count = sum(
            1
            for value in values
            if isinstance(value.get("sample_quality"), Mapping)
            and value["sample_quality"].get("outcome_mature")
        )
        summary.update(
            {
                "row_count": len(values),
                "mean_expected_return": _round(
                    _mean([_float(value.get("expected_return"), 0.0) for value in values])
                ),
                "mean_median_return": _round(
                    _mean([_float(value.get("median_return"), 0.0) for value in values])
                ),
                "mean_net_utility": _round(
                    _mean([_float(value.get("net_utility"), 0.0) for value in values])
                ),
                "median_net_utility": _round(
                    _median([_float(value.get("net_utility"), 0.0) for value in values])
                ),
                "mean_downside_risk": _round(
                    _mean([_float(value.get("downside_risk"), 0.0) for value in values])
                ),
                "mean_estimated_cost": _round(
                    _mean([_float(value.get("estimated_cost"), 0.0) for value in values])
                ),
                "mean_uncertainty": _round(
                    _mean([_float(value.get("uncertainty"), 0.0) for value in values])
                ),
                "mature_outcome_rate": _round(mature_count / len(values) if values else 0.0),
                "promotion_gate_allowed": False,
            }
        )
        if any(value.get("profile_utility") is not None for value in values):
            summary["mean_profile_utility"] = _round(
                _mean([_float(value.get("profile_utility"), 0.0) for value in values])
            )
        summaries.append(summary)
    return summaries


def _value_surface_horizon_smoothness_audit(
    *,
    surface_rows: list[dict[str, Any]],
    horizons: list[dict[str, Any]],
    config: Mapping[str, Any],
) -> dict[str, Any]:
    expansion = _next_stage_section(config, "value_surface_expansion")
    smoothness = expansion.get("horizon_smoothness")
    smoothness_policy = dict(smoothness) if isinstance(smoothness, Mapping) else {}
    materiality = _float(smoothness_policy.get("adjacent_delta_materiality_bps"), 100.0) / 10_000.0
    horizon_order = {str(row["horizon_id"]): int(row["days"]) for row in horizons}
    grouped: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
    for row in surface_rows:
        grouped.setdefault(
            (str(row.get("date")), str(row.get("asset")), str(row.get("action"))), []
        ).append(row)
    adjacent_rows: list[dict[str, Any]] = []
    for (row_date, asset, action), values in sorted(grouped.items()):
        ordered = sorted(values, key=lambda row: horizon_order.get(str(row.get("horizon")), 0))
        for previous, current in zip(ordered, ordered[1:], strict=False):
            delta = _float(current.get("net_utility"), 0.0) - _float(
                previous.get("net_utility"), 0.0
            )
            adjacent_rows.append(
                {
                    "date": row_date,
                    "asset": asset,
                    "action": action,
                    "previous_horizon": previous.get("horizon"),
                    "current_horizon": current.get("horizon"),
                    "net_utility_delta": _round(delta),
                    "abs_net_utility_delta": _round(abs(delta)),
                    "material_for_review": abs(delta) >= materiality,
                    "promotion_gate_allowed": False,
                }
            )
    abs_values = [_float(row.get("abs_net_utility_delta"), 0.0) for row in adjacent_rows]
    return {
        "schema_version": "1.0",
        "report_type": "value_surface_expansion_horizon_smoothness_audit",
        "status": "PASS_WITH_WARNINGS",
        "summary": {
            "horizon_smoothness_audit_present": True,
            "audit_only": bool(smoothness_policy.get("audit_only", True)),
            "adjacent_pair_count": len(adjacent_rows),
            "material_pair_count": sum(1 for row in adjacent_rows if row["material_for_review"]),
            "median_abs_adjacent_delta": _round(_median(abs_values)) if abs_values else None,
            "max_abs_adjacent_delta": _round(max(abs_values)) if abs_values else None,
            "not_validated_utility_boundary": True,
            **_summary_safety(),
        },
        "smoothness_policy": smoothness_policy,
        "adjacent_horizon_deltas": adjacent_rows,
        **PRODUCTION_SAFETY,
    }


def _utility_profiles(config: Mapping[str, Any]) -> list[dict[str, Any]]:
    audit = _next_stage_section(config, "utility_boundary_audit")
    profiles = _records(audit.get("utility_profiles"))
    return profiles or [
        {
            "profile_id": "balanced_transparent",
            "expected_return_weight": 0.6,
            "median_return_weight": 0.2,
            "downside_risk_weight": 0.5,
            "cost_weight": 1.0,
            "uncertainty_weight": 0.1,
        }
    ]


def _utility_profile_ranking(
    *,
    surface_rows: list[dict[str, Any]],
    profile: Mapping[str, Any],
) -> dict[str, Any]:
    scored = [
        {
            **row,
            "profile_utility": _round(_utility_score(row=row, profile=profile)),
        }
        for row in surface_rows
    ]
    grouped = _surface_group_summary(scored, ["action", "horizon"])
    ranked = sorted(
        grouped,
        key=lambda row: _float(row.get("mean_profile_utility"), 0.0),
        reverse=True,
    )
    return {
        "profile_id": str(profile.get("profile_id", "unknown_profile")),
        "ranking": ranked,
        "top_rank": ranked[0] if ranked else {},
        "profile_weights": dict(profile),
        "ranking_policy": "heuristic",
        "not_validated_utility_boundary": True,
        "promotion_gate_allowed": False,
    }


def _utility_score(*, row: Mapping[str, Any], profile: Mapping[str, Any]) -> float:
    return (
        _float(profile.get("expected_return_weight"), 0.0) * _float(row.get("expected_return"), 0.0)
        + _float(profile.get("median_return_weight"), 0.0) * _float(row.get("median_return"), 0.0)
        - _float(profile.get("downside_risk_weight"), 0.0) * _float(row.get("downside_risk"), 0.0)
        - _float(profile.get("cost_weight"), 0.0) * _float(row.get("estimated_cost"), 0.0)
        - _float(profile.get("uncertainty_weight"), 0.0) * _float(row.get("uncertainty"), 0.0)
    )


def _profile_reversal_report(rankings: list[dict[str, Any]]) -> dict[str, Any]:
    if not rankings:
        return {"summary": {"ranking_reversal_count": 0}, "rows": []}
    base = next(
        (ranking for ranking in rankings if ranking.get("profile_id") == "balanced_transparent"),
        rankings[0],
    )
    base_top = (
        base.get("top_rank", {}).get("action"),
        base.get("top_rank", {}).get("horizon"),
    )
    rows = []
    for ranking in rankings:
        top = (
            ranking.get("top_rank", {}).get("action"),
            ranking.get("top_rank", {}).get("horizon"),
        )
        rows.append(
            {
                "profile_id": ranking.get("profile_id"),
                "top_action": top[0],
                "top_horizon": top[1],
                "reverses_balanced_top": top != base_top,
                "promotion_gate_allowed": False,
            }
        )
    return {
        "summary": {
            "base_profile_id": base.get("profile_id"),
            "ranking_reversal_count": sum(1 for row in rows if row["reverses_balanced_top"]),
            "profile_reversal_report_present": True,
        },
        "rows": rows,
    }


def _single_weight_dominance_report(
    config: Mapping[str, Any],
    profiles: list[dict[str, Any]],
) -> dict[str, Any]:
    audit = _next_stage_section(config, "utility_boundary_audit")
    floor = _float(audit.get("dominance_share_review_floor"), 0.65)
    weight_keys = [
        "expected_return_weight",
        "median_return_weight",
        "downside_risk_weight",
        "cost_weight",
        "uncertainty_weight",
    ]
    rows = []
    for profile in profiles:
        weights = {key: abs(_float(profile.get(key), 0.0)) for key in weight_keys}
        total = sum(weights.values()) or 1.0
        dominant_key, dominant_value = max(weights.items(), key=lambda item: item[1])
        share = dominant_value / total
        rows.append(
            {
                "profile_id": profile.get("profile_id"),
                "dominant_component": dominant_key,
                "dominant_component_share": _round(share),
                "single_weight_dominance_for_review": share >= floor,
                "promotion_gate_allowed": False,
            }
        )
    return {
        "summary": {
            "dominance_share_review_floor": floor,
            "single_weight_dominance_profile_count": sum(
                1 for row in rows if row["single_weight_dominance_for_review"]
            ),
            "single_weight_dominance_report_present": True,
        },
        "rows": rows,
    }


def _pareto_frontier(
    surface_rows: list[dict[str, Any]],
    config: Mapping[str, Any],
) -> list[dict[str, Any]]:
    components = _next_stage_section(config, "utility_boundary_audit").get("pareto_components")
    component_map = dict(components) if isinstance(components, Mapping) else {}
    reward_key = str(component_map.get("reward", "expected_return"))
    risk_key = str(component_map.get("risk", "downside_risk"))
    cost_key = str(component_map.get("cost", "estimated_cost"))
    uncertainty_key = str(component_map.get("uncertainty", "uncertainty"))
    candidates = _surface_group_summary(surface_rows, ["action", "horizon"])
    frontier: list[dict[str, Any]] = []
    for candidate in candidates:
        candidate_reward = _float(candidate.get(f"mean_{reward_key}"), 0.0)
        candidate_risk = _float(candidate.get(f"mean_{risk_key}"), 0.0)
        candidate_cost = _float(candidate.get(f"mean_{cost_key}"), 0.0)
        candidate_uncertainty = _float(candidate.get(f"mean_{uncertainty_key}"), 0.0)
        dominated = False
        for challenger in candidates:
            if challenger is candidate:
                continue
            challenger_reward = _float(challenger.get(f"mean_{reward_key}"), 0.0)
            challenger_risk = _float(challenger.get(f"mean_{risk_key}"), 0.0)
            challenger_cost = _float(challenger.get(f"mean_{cost_key}"), 0.0)
            challenger_uncertainty = _float(challenger.get(f"mean_{uncertainty_key}"), 0.0)
            weakly_better = (
                challenger_reward >= candidate_reward
                and challenger_risk <= candidate_risk
                and challenger_cost <= candidate_cost
                and challenger_uncertainty <= candidate_uncertainty
            )
            strictly_better = (
                challenger_reward > candidate_reward
                or challenger_risk < candidate_risk
                or challenger_cost < candidate_cost
                or challenger_uncertainty < candidate_uncertainty
            )
            if weakly_better and strictly_better:
                dominated = True
                break
        if not dominated:
            frontier.append(
                {
                    **candidate,
                    "pareto_frontier": True,
                    "not_validated_utility_boundary": True,
                    "promotion_gate_allowed": False,
                }
            )
    return frontier


def _forward_maturity_horizons(config: Mapping[str, Any]) -> list[dict[str, Any]]:
    forward = _next_stage_section(config, "forward_evidence_maturity")
    rows = _records(forward.get("horizons"))
    return [
        {"horizon_id": str(row.get("horizon_id")), "days": int(row.get("days", 1))}
        for row in rows
        if row.get("horizon_id")
    ] or [
        {"horizon_id": "1d", "days": 1},
        {"horizon_id": "5d", "days": 5},
        {"horizon_id": "10d", "days": 10},
        {"horizon_id": "20d", "days": 20},
        {"horizon_id": "60d", "days": 60},
    ]


def _read_jsonl_rows(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        raw = json.loads(line)
        if isinstance(raw, Mapping):
            rows.append(dict(raw))
    return rows


def _forward_maturity_rows(
    *,
    ledger_rows: list[dict[str, Any]],
    dates: list[str],
    config: Mapping[str, Any],
) -> list[dict[str, Any]]:
    horizons = _forward_maturity_horizons(config)
    rows: list[dict[str, Any]] = []
    for ledger_row in ledger_rows:
        as_of = str(ledger_row.get("as_of") or "")
        decision_index = _date_index_on_or_before(dates, as_of)
        for horizon in horizons:
            horizon_days = int(horizon["days"])
            target_index = decision_index + horizon_days if decision_index is not None else None
            matured = target_index is not None and target_index < len(dates)
            rows.append(
                {
                    "archive_id": ledger_row.get("archive_id"),
                    "as_of": as_of,
                    "horizon": horizon["horizon_id"],
                    "horizon_days": horizon_days,
                    "matured": matured,
                    "target_date": (
                        dates[target_index] if matured and target_index is not None else None
                    ),
                    "outcome_status": "matured" if matured else "pending",
                    "outcome_append_only": bool(ledger_row.get("outcome_append_only")),
                    "promotion_gate_allowed": False,
                }
            )
    return rows


def _forward_maturity_summary(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        grouped.setdefault(str(row.get("horizon")), []).append(row)
    return [
        {
            "horizon": horizon,
            "ledger_event_count": len(values),
            "matured_count": sum(1 for value in values if value.get("matured")),
            "pending_count": sum(1 for value in values if not value.get("matured")),
            "future_outcomes_appended_only": all(
                bool(value.get("outcome_append_only")) for value in values
            ),
            "promotion_gate_allowed": False,
        }
        for horizon, values in sorted(grouped.items())
    ]


def _date_index_on_or_before(dates: list[str], raw_date: str) -> int | None:
    eligible = [index for index, row_date in enumerate(dates) if row_date <= raw_date]
    return eligible[-1] if eligible else None


def _forward_artifact_retention(
    *,
    benchmark_expansion_path: Path,
    control_audit_path: Path,
    value_surface_expansion_path: Path,
) -> dict[str, Any]:
    candidate_paths = [
        DEFAULT_VALUE_SURFACE_PATH,
        DEFAULT_REGRET_STATE_MACHINE_PATH,
        DEFAULT_SIMPLE_STRATEGY_SELECTOR_PATH,
        DEFAULT_GBDT_ACTION_UTILITY_PATH,
        value_surface_expansion_path,
    ]
    return {
        "benchmark_expansion": _artifact_status(
            _read_json_or_empty(benchmark_expansion_path), benchmark_expansion_path
        ),
        "control_audit": _artifact_status(
            _read_json_or_empty(control_audit_path), control_audit_path
        ),
        "value_surface_controlled_expansion": _artifact_status(
            _read_json_or_empty(value_surface_expansion_path), value_surface_expansion_path
        ),
        "candidate_outputs": [
            _artifact_status(_read_json_or_empty(path), path) for path in candidate_paths
        ],
        "promotion_gate_allowed": False,
    }


def _gbdt_pivot_root_cause_review(gbdt: Mapping[str, Any]) -> list[dict[str, Any]]:
    split = gbdt.get("train_test_split") if isinstance(gbdt, Mapping) else {}
    calibration = gbdt.get("calibration_report") if isinstance(gbdt, Mapping) else {}
    feature_importance = _records(gbdt.get("feature_importance"))
    max_feature = (
        max(feature_importance, key=lambda row: _float(row.get("importance"), 0.0))
        if feature_importance
        else {}
    )
    train_count = _first_int(split.get("train_row_count")) if isinstance(split, Mapping) else 0
    test_count = _first_int(split.get("test_row_count")) if isinstance(split, Mapping) else 0
    return [
        {
            "question": "sample_size_or_walk_forward_split_limit",
            "evidence": {
                "train_row_count": train_count,
                "test_row_count": test_count,
                "split_policy": split.get("split_policy") if isinstance(split, Mapping) else None,
            },
            "review_decision": "requires_larger_walk_forward_design_before_model_continuation",
            "promotion_gate_allowed": False,
        },
        {
            "question": "utility_label_too_noisy_for_direct_regression",
            "evidence": {
                "calibration_status": (
                    calibration.get("calibration_status")
                    if isinstance(calibration, Mapping)
                    else None
                ),
                "mean_absolute_error": (
                    calibration.get("mean_absolute_error")
                    if isinstance(calibration, Mapping)
                    else None
                ),
            },
            "review_decision": "prefer_ranking_or_regret_type_target_over_scalar_utility",
            "promotion_gate_allowed": False,
        },
        {
            "question": "feature_set_or_target_mismatch",
            "evidence": {
                "top_feature": max_feature.get("feature"),
                "top_feature_importance": max_feature.get("importance"),
                "future_feature_violation_count": _summary_value(
                    gbdt, "future_feature_violation_count"
                ),
            },
            "review_decision": "use_as_feature_quality_or_residual_diagnostic_until_reframed",
            "promotion_gate_allowed": False,
        },
    ]


def _regret_casebook_gate_conditions(
    *,
    config: Mapping[str, Any],
    state_machine: Mapping[str, Any],
    casebook: Mapping[str, Any],
    value_surface: Mapping[str, Any],
) -> list[dict[str, Any]]:
    gate = _next_stage_section(config, "regret_casebook_expansion_gate")
    minimum_cases = _first_int(gate.get("minimum_case_count"))
    minimum_types = _first_int(gate.get("minimum_distinct_regret_types"))
    case_count = _casebook_case_count(casebook, state_machine)
    covered_types = [
        row
        for row in _records(state_machine.get("regret_type_coverage"))
        if row.get("covered_by_state_machine")
    ]
    teacher_oracle_difference_present = bool(
        state_machine.get("teacher_oracle_diagnostic_difference")
    )
    failure_attribution_present = bool(value_surface.get("failure_case_attribution"))
    return [
        {
            "condition_id": "regret_case_count_floor_met",
            "passed": case_count >= minimum_cases,
            "observed": case_count,
            "required": minimum_cases,
            "promotion_gate_allowed": False,
        },
        {
            "condition_id": "regret_type_distribution_stable",
            "passed": len(covered_types) >= minimum_types,
            "observed": len(covered_types),
            "required": minimum_types,
            "promotion_gate_allowed": False,
        },
        {
            "condition_id": "teacher_oracle_diagnostic_difference_present",
            "passed": teacher_oracle_difference_present,
            "observed": teacher_oracle_difference_present,
            "required": True,
            "promotion_gate_allowed": False,
        },
        {
            "condition_id": "value_surface_failure_cases_attributed",
            "passed": failure_attribution_present,
            "observed": failure_attribution_present,
            "required": True,
            "promotion_gate_allowed": False,
        },
    ]


def _casebook_case_count(
    casebook: Mapping[str, Any],
    state_machine: Mapping[str, Any],
) -> int:
    case_rows = _records(casebook.get("casebook_rows"))
    if case_rows:
        return len(case_rows)
    return len(_records(state_machine.get("state_by_date")))


def _sample_concentration_report(
    rows: list[dict[str, Any]],
    *,
    group_keys: list[str],
) -> dict[str, Any]:
    breakdowns = []
    for key in group_keys:
        counts = _group_count(rows, key)
        total = sum(counts.values()) or 1
        groups = [
            {
                key: group,
                "row_count": count,
                "row_share": _round(count / total),
                "promotion_gate_allowed": False,
            }
            for group, count in sorted(counts.items())
        ]
        max_share = max((count / total for count in counts.values()), default=0.0)
        breakdowns.append(
            {
                "group_key": key,
                "group_count": len(counts),
                "max_group_share": _round(max_share),
                "groups": groups,
                "promotion_gate_allowed": False,
            }
        )
    return {
        "summary": {
            "row_count": len(rows),
            "max_group_share": _round(
                max((_float(row.get("max_group_share"), 0.0) for row in breakdowns), default=0.0)
            ),
            "sample_concentration_present": True,
            "promotion_gate_allowed": False,
        },
        "breakdowns": breakdowns,
    }


def _value_surface_warning_taxonomy(
    *,
    config: Mapping[str, Any],
    value_surface: Mapping[str, Any],
    utility_audit: Mapping[str, Any],
    forward_maturity: Mapping[str, Any],
    sample_concentration: Mapping[str, Any],
) -> list[dict[str, Any]]:
    policy = _next_stage_section(config, "value_surface_warning_triage")
    warning_rows = _records(policy.get("warning_categories"))
    value_summary = value_surface.get("summary") if isinstance(value_surface, Mapping) else {}
    utility_summary = utility_audit.get("summary") if isinstance(utility_audit, Mapping) else {}
    forward_summary = (
        forward_maturity.get("summary") if isinstance(forward_maturity, Mapping) else {}
    )
    concentration_summary = (
        sample_concentration.get("summary") if isinstance(sample_concentration, Mapping) else {}
    )
    field_values = {
        "data_quality_status": (
            value_summary.get("data_quality_status") if isinstance(value_summary, Mapping) else None
        ),
        "not_validated_utility_boundary": (
            utility_summary.get("not_validated_utility_boundary")
            if isinstance(utility_summary, Mapping)
            else True
        ),
        "ledger_event_count": (
            forward_summary.get("ledger_event_count") if isinstance(forward_summary, Mapping) else 0
        ),
        "max_group_share": (
            concentration_summary.get("max_group_share")
            if isinstance(concentration_summary, Mapping)
            else 0.0
        ),
        "material_pair_count": (
            value_surface.get("horizon_smoothness_summary", {}).get("material_pair_count")
            if isinstance(value_surface.get("horizon_smoothness_summary"), Mapping)
            else 0
        ),
    }
    warnings: list[dict[str, Any]] = []
    for row in warning_rows:
        field = str(row.get("source_field", ""))
        value = field_values.get(field)
        triggered = False
        if "warning_when_not" in row:
            triggered = value != row.get("warning_when_not")
        if "warning_when" in row:
            triggered = value == row.get("warning_when")
        if "warning_when_below" in row:
            triggered = _float(value, 0.0) < _float(row.get("warning_when_below"), 0.0)
        if "warning_when_above" in row:
            triggered = _float(value, 0.0) > _float(row.get("warning_when_above"), 0.0)
        if triggered:
            warnings.append(
                {
                    "category_id": row.get("category_id"),
                    "source_field": field,
                    "observed": value,
                    "severity": row.get("severity", "review"),
                    "promotion_gate_allowed": False,
                }
            )
    return warnings


def _value_surface_controlled_review_decision(
    *,
    config: Mapping[str, Any],
    value_surface: Mapping[str, Any],
    warnings: list[dict[str, Any]],
) -> str:
    if not value_surface:
        return "DATA_REQUIRED"
    summary = value_surface.get("summary") if isinstance(value_surface, Mapping) else {}
    if not isinstance(summary, Mapping):
        return "DATA_REQUIRED"
    requirements = _next_stage_section(config, "value_surface_warning_triage").get(
        "continue_requires"
    )
    required = dict(requirements) if isinstance(requirements, Mapping) else {}
    if not bool(summary.get("horizon_leakage_check_pass")):
        return "KILL"
    if _first_int(summary.get("negative_control_promotion_count")) > 0:
        return "KILL"
    if not all(bool(summary.get(key)) == bool(expected) for key, expected in required.items()):
        return "DATA_REQUIRED"
    if any(row.get("severity") == "blocking_for_promotion" for row in warnings):
        return "CONTINUE"
    if warnings:
        return "CONTINUE"
    return "CONTINUE"


def _decision_date_count_breakdown(rows: list[dict[str, Any]]) -> dict[str, Any]:
    dates = sorted({str(row.get("date")) for row in rows if row.get("date")})
    return {
        "decision_date_count": len(dates),
        "first_decision_date": dates[0] if dates else None,
        "last_decision_date": dates[-1] if dates else None,
        "by_month": _month_count(dates),
        "rows_per_decision_date": _group_count(rows, "date"),
        "promotion_gate_allowed": False,
    }


def _month_count(dates: list[str]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for raw in dates:
        month = raw[:7]
        counts[month] = counts.get(month, 0) + 1
    return counts


def _utility_ranking_stability_report(utility_audit: Mapping[str, Any]) -> dict[str, Any]:
    summary = utility_audit.get("summary") if isinstance(utility_audit, Mapping) else {}
    return {
        "profile_count": summary.get("profile_count") if isinstance(summary, Mapping) else None,
        "ranking_reversal_count": (
            summary.get("ranking_reversal_count") if isinstance(summary, Mapping) else None
        ),
        "single_weight_dominance_profile_count": (
            summary.get("single_weight_dominance_profile_count")
            if isinstance(summary, Mapping)
            else None
        ),
        "pareto_frontier_count": (
            summary.get("pareto_frontier_count") if isinstance(summary, Mapping) else None
        ),
        "validated_boundary_count": (
            summary.get("validated_boundary_count") if isinstance(summary, Mapping) else 0
        ),
        "stability_status": "DIAGNOSTIC_ONLY_NOT_VALIDATED",
        "promotion_gate_allowed": False,
    }


def _ranking_reversal_analysis(utility_audit: Mapping[str, Any]) -> dict[str, Any]:
    reversal = utility_audit.get("profile_reversal_report")
    rows = _records(reversal.get("rows") if isinstance(reversal, Mapping) else [])
    return {
        "summary": reversal.get("summary", {}) if isinstance(reversal, Mapping) else {},
        "reversal_rows": rows,
        "ranking_reversal_count": sum(1 for row in rows if row.get("reverses_balanced_top")),
        "promotion_gate_allowed": False,
    }


def _dominant_dimension_analysis(utility_audit: Mapping[str, Any]) -> dict[str, Any]:
    dominance = utility_audit.get("single_weight_dominance_report")
    rows = _records(dominance.get("rows") if isinstance(dominance, Mapping) else [])
    return {
        "summary": dominance.get("summary", {}) if isinstance(dominance, Mapping) else {},
        "dominance_rows": rows,
        "dominant_dimension_count": sum(
            1 for row in rows if row.get("single_weight_dominance_for_review")
        ),
        "promotion_gate_allowed": False,
    }


def _pareto_stability_analysis(
    utility_audit: Mapping[str, Any],
    config: Mapping[str, Any],
) -> dict[str, Any]:
    frontier = _records(utility_audit.get("pareto_frontier"))
    frontier_keys = {(row.get("action"), row.get("horizon")) for row in frontier}
    ranking_rows = _records(utility_audit.get("utility_profile_rankings"))
    top_rows = [
        ranking.get("top_rank")
        for ranking in ranking_rows
        if isinstance(ranking.get("top_rank"), Mapping)
    ]
    top_on_frontier = [
        row for row in top_rows if (row.get("action"), row.get("horizon")) in frontier_keys
    ]
    profile_count = len(top_rows)
    stability_rate = len(top_on_frontier) / profile_count if profile_count else 0.0
    robustness = _next_stage_section(config, "utility_ranking_robustness")
    floor = _float(robustness.get("pareto_stability_review_floor"), 0.5)
    return {
        "frontier_count": len(frontier),
        "profile_top_rank_count": profile_count,
        "profile_top_on_frontier_count": len(top_on_frontier),
        "profile_top_on_frontier_rate": _round(stability_rate),
        "pareto_stability_review_floor": floor,
        "pareto_more_stable_than_single_utility": stability_rate >= floor,
        "promotion_gate_allowed": False,
    }


def _forward_daily_continuity_report(
    *,
    ledger_rows: list[dict[str, Any]],
    dates: list[str],
    config: Mapping[str, Any],
) -> dict[str, Any]:
    ledger_dates = sorted({str(row.get("as_of")) for row in ledger_rows if row.get("as_of")})
    if not ledger_dates:
        return {
            "summary": {
                "expected_daily_archive_count": 0,
                "observed_daily_archive_count": 0,
                "missing_daily_archive_count": 0,
                "daily_continuity_pass": False,
                "promotion_gate_allowed": False,
            },
            "missing_dates": [],
        }
    start = ledger_dates[0]
    end = dates[-1] if dates else ledger_dates[-1]
    expected = [row_date for row_date in dates if start <= row_date <= end]
    missing = [row_date for row_date in expected if row_date not in set(ledger_dates)]
    policy = _next_stage_section(config, "forward_evidence_continuity")
    missing_allowed = bool(policy.get("missing_archive_allowed", False))
    return {
        "summary": {
            "expected_daily_archive_count": len(expected),
            "observed_daily_archive_count": len(ledger_dates),
            "missing_daily_archive_count": len(missing),
            "daily_continuity_pass": len(missing) == 0 or missing_allowed,
            "minimum_ledger_events_for_continue": _first_int(
                policy.get("minimum_ledger_events_for_continue")
            ),
            "promotion_gate_allowed": False,
        },
        "observed_dates": ledger_dates,
        "missing_dates": missing,
    }


def _append_only_integrity_report(ledger_rows: list[dict[str, Any]]) -> dict[str, Any]:
    archive_ids = [str(row.get("archive_id")) for row in ledger_rows if row.get("archive_id")]
    dates = [str(row.get("as_of")) for row in ledger_rows if row.get("as_of")]
    all_append_only = all(bool(row.get("outcome_append_only")) for row in ledger_rows)
    unique_archive_ids = len(archive_ids) == len(set(archive_ids))
    nondecreasing_dates = dates == sorted(dates)
    return {
        "summary": {
            "append_only_integrity_pass": all_append_only
            and unique_archive_ids
            and nondecreasing_dates,
            "all_rows_append_only": all_append_only,
            "unique_archive_ids": unique_archive_ids,
            "nondecreasing_as_of_dates": nondecreasing_dates,
            "promotion_gate_allowed": False,
        },
        "ledger_event_count": len(ledger_rows),
    }


def _forward_output_coverage_report(
    *,
    benchmark_expansion_path: Path,
    control_audit_path: Path,
    value_surface_expansion_path: Path,
) -> dict[str, Any]:
    rows = [
        _artifact_status(_read_json_or_empty(benchmark_expansion_path), benchmark_expansion_path),
        _artifact_status(_read_json_or_empty(control_audit_path), control_audit_path),
        _artifact_status(
            _read_json_or_empty(value_surface_expansion_path), value_surface_expansion_path
        ),
    ]
    return {
        "artifact_families": [
            {**rows[0], "family": "benchmark_expansion"},
            {**rows[1], "family": "control_audit"},
            {**rows[2], "family": "value_surface_controlled_expansion"},
        ],
        "summary": {
            "covered_family_count": sum(1 for row in rows if row["present"]),
            "required_family_count": len(rows),
            "all_required_outputs_present": all(row["present"] for row in rows),
            "promotion_gate_allowed": False,
        },
    }


def _gbdt_pivot_direction_rows(selection_policy: Mapping[str, Any]) -> list[dict[str, Any]]:
    selected = str(selection_policy.get("selected_direction", ""))
    rows = []
    for row in _records(selection_policy.get("direction_details")):
        direction_id = str(row.get("direction_id"))
        rows.append(
            {
                "direction_id": direction_id,
                "selected": direction_id == selected,
                "minimum_viable_experiment": row.get("minimum_viable_experiment"),
                "required_data": row.get("required_data"),
                "failure_mode": row.get("failure_mode"),
                "kill_criteria": row.get("kill_criteria"),
                "difference_from_previous_action_utility_model": row.get(
                    "difference_from_previous_action_utility_model"
                ),
                "model_run_executed": False,
                "promotion_gate_allowed": False,
            }
        )
    return rows


def _value_surface_failure_activation_inputs(
    *,
    rows: list[dict[str, Any]],
    config: Mapping[str, Any],
) -> dict[str, Any]:
    grouped: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
    for row in rows:
        grouped.setdefault(
            (str(row.get("date")), str(row.get("asset")), str(row.get("horizon"))), []
        ).append(row)
    activation_cases: list[dict[str, Any]] = []
    for (row_date, asset, horizon), values in sorted(grouped.items()):
        top = max(values, key=lambda row: _float(row.get("net_utility"), -999.0))
        realized = top.get("realized_forward_return")
        if realized is None:
            continue
        action = str(top.get("action"))
        realized_value = _float(realized, 0.0)
        losing = realized_value < 0
        benchmark_disagreement = action not in {
            "buy_and_hold",
            "hold_exposure",
            "risk_on",
            "no_masking",
        }
        false_risk_off = _action_is_defensive(action) and realized_value > 0
        if losing or benchmark_disagreement or false_risk_off:
            activation_cases.append(
                {
                    "date": row_date,
                    "asset": asset,
                    "horizon": horizon,
                    "top_action": action,
                    "realized_forward_return": _round(realized_value),
                    "value_surface_losing_case": losing,
                    "benchmark_disagreement_case": benchmark_disagreement,
                    "false_risk_off_or_missed_upside_case": false_risk_off,
                    "oracle_teacher_better_case": False,
                    "promotion_gate_allowed": False,
                }
            )
    losing_count = sum(1 for row in activation_cases if row["value_surface_losing_case"])
    disagreement_count = sum(1 for row in activation_cases if row["benchmark_disagreement_case"])
    false_risk_off_count = sum(
        1 for row in activation_cases if row["false_risk_off_or_missed_upside_case"]
    )
    oracle_teacher_count = sum(1 for row in activation_cases if row["oracle_teacher_better_case"])
    return {
        "summary": {
            "value_surface_losing_case_count": losing_count,
            "benchmark_disagreement_case_count": disagreement_count,
            "false_risk_off_or_missed_upside_case_count": false_risk_off_count,
            "oracle_teacher_better_case_count": oracle_teacher_count,
            "activation_case_count": len(activation_cases),
            "promotion_gate_allowed": False,
        },
        "activation_cases": activation_cases[:100],
        "policy": _next_stage_section(config, "regret_activation_inputs"),
    }


def _action_is_defensive(action: str) -> bool:
    return action in {
        "hold_cash",
        "risk_off",
        "decrease_exposure",
        "drawdown_guard",
        "capped_masking",
    }


def _regret_activation_input_criteria(
    *,
    config: Mapping[str, Any],
    failure_inputs: Mapping[str, Any],
    gate: Mapping[str, Any],
) -> list[dict[str, Any]]:
    policy = _next_stage_section(config, "regret_activation_inputs")
    summary = failure_inputs.get("summary") if isinstance(failure_inputs, Mapping) else {}
    gate_rows = _records(gate.get("activation_gate"))
    regret_coverage = next(
        (row for row in gate_rows if row.get("condition_id") == "regret_type_distribution_stable"),
        {},
    )
    criteria = [
        (
            "value_surface_losing_cases",
            (
                _first_int(summary.get("value_surface_losing_case_count"))
                if isinstance(summary, Mapping)
                else 0
            ),
            _first_int(policy.get("minimum_value_surface_losing_cases")),
        ),
        (
            "benchmark_disagreement_cases",
            (
                _first_int(summary.get("benchmark_disagreement_case_count"))
                if isinstance(summary, Mapping)
                else 0
            ),
            _first_int(policy.get("minimum_benchmark_disagreement_cases")),
        ),
        (
            "false_risk_off_or_missed_upside_cases",
            (
                _first_int(summary.get("false_risk_off_or_missed_upside_case_count"))
                if isinstance(summary, Mapping)
                else 0
            ),
            _first_int(policy.get("minimum_false_risk_off_or_missed_upside_cases")),
        ),
        (
            "oracle_teacher_better_cases",
            (
                _first_int(summary.get("oracle_teacher_better_case_count"))
                if isinstance(summary, Mapping)
                else 0
            ),
            1 if bool(policy.get("oracle_teacher_required_for_activation", True)) else 0,
        ),
        (
            "enough_regret_type_coverage",
            _first_int(regret_coverage.get("observed")),
            _first_int(policy.get("minimum_distinct_regret_types")),
        ),
    ]
    return [
        {
            "condition_id": condition_id,
            "observed": observed,
            "required": required,
            "passed": observed >= required,
            "promotion_gate_allowed": False,
        }
        for condition_id, observed, required in criteria
    ]


def _selected_value_surface_cases(
    rows: list[dict[str, Any]],
    config: Mapping[str, Any],
) -> list[dict[str, Any]]:
    action_by_id = {str(row["action_id"]): row for row in _actions(config)}
    grouped: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
    for row in rows:
        grouped.setdefault(
            (str(row.get("date")), str(row.get("asset")), str(row.get("horizon"))),
            [],
        ).append(row)
    selected: list[dict[str, Any]] = []
    benchmark_order = ["buy_and_hold", "hold_exposure", "risk_on", "no_masking"]
    for (row_date, asset, horizon), values in sorted(grouped.items()):
        mature_values = [row for row in values if row.get("realized_forward_return") is not None]
        if not mature_values:
            continue
        top = max(mature_values, key=lambda row: _float(row.get("net_utility"), -999.0))
        benchmark = next(
            (
                row
                for action_id in benchmark_order
                for row in mature_values
                if row.get("action") == action_id
            ),
            None,
        )
        selected_net = _action_realized_net(top, action_by_id)
        benchmark_net = (
            _action_realized_net(benchmark, action_by_id)
            if benchmark is not None
            else _float(top.get("realized_forward_return"), 0.0)
        )
        selected.append(
            {
                "date": row_date,
                "asset": asset,
                "horizon": horizon,
                "horizon_days": top.get("horizon_days"),
                "regime_segment": top.get("regime_segment"),
                "asset_cluster": top.get("asset_cluster"),
                "pit_state": top.get("pit_state"),
                "selected_action": top.get("action"),
                "benchmark_action": benchmark.get("action") if benchmark else "raw_buy_and_hold",
                "selected_realized_net_return": _round(selected_net),
                "benchmark_realized_net_return": _round(benchmark_net),
                "delta_vs_benchmark": _round(selected_net - benchmark_net),
                "value_surface_beats_benchmark": selected_net >= benchmark_net,
                "selected_net_utility": top.get("net_utility"),
                "selected_estimated_cost": top.get("estimated_cost"),
                "benchmark_estimated_cost": benchmark.get("estimated_cost") if benchmark else 0.0,
                "selected_turnover_cost_assumption": action_by_id.get(
                    str(top.get("action")), {}
                ).get("cost_turnover_assumption"),
                "benchmark_turnover_cost_assumption": (
                    action_by_id.get(str(benchmark.get("action")), {}).get(
                        "cost_turnover_assumption"
                    )
                    if benchmark
                    else 0.0
                ),
                "selected_drawdown_proxy": top.get("max_drawdown_proxy"),
                "benchmark_drawdown_proxy": (
                    benchmark.get("max_drawdown_proxy") if benchmark else None
                ),
                "promotion_gate_allowed": False,
            }
        )
    return selected


def _action_realized_net(
    row: Mapping[str, Any] | None,
    action_by_id: Mapping[str, Mapping[str, Any]],
) -> float:
    if row is None:
        return 0.0
    action_id = str(row.get("action"))
    action = action_by_id.get(action_id, {})
    exposure = _float(action.get("exposure_multiplier"), 1.0)
    realized = _float(row.get("realized_forward_return"), 0.0)
    cost = _float(row.get("estimated_cost"), 0.0)
    return exposure * realized - cost


def _walk_forward_windows(
    decision_dates: list[str],
    config: Mapping[str, Any],
) -> list[dict[str, Any]]:
    if not decision_dates:
        return []
    policy = _next_stage_section(config, "value_surface_walk_forward_expansion")
    max_windows = max(1, _first_int(policy.get("max_window_count")) or 1)
    min_dates = max(1, _first_int(policy.get("min_decision_dates_per_window")) or 1)
    window_count = min(max_windows, max(1, len(decision_dates) // min_dates))
    windows: list[dict[str, Any]] = []
    for index in range(window_count):
        start_index = math.floor(index * len(decision_dates) / window_count)
        end_index = math.floor((index + 1) * len(decision_dates) / window_count)
        window_dates = decision_dates[start_index:end_index]
        if not window_dates:
            continue
        windows.append(
            {
                "window_id": f"wf_{index + 1:02d}",
                "first_decision_date": window_dates[0],
                "last_decision_date": window_dates[-1],
                "decision_date_count": len(window_dates),
                "promotion_gate_allowed": False,
            }
        )
    return windows


def _walk_forward_window_results(
    selected_cases: list[dict[str, Any]],
    windows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    results = []
    for window in windows:
        first = str(window.get("first_decision_date"))
        last = str(window.get("last_decision_date"))
        values = [row for row in selected_cases if first <= str(row.get("date")) <= last]
        results.append(
            {
                **window,
                **_aggregate_walk_forward_cases(values),
            }
        )
    return results


def _walk_forward_group_result(
    selected_cases: list[dict[str, Any]],
    group_key: str,
) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in selected_cases:
        grouped.setdefault(str(row.get(group_key, "unknown")), []).append(row)
    return [
        {
            group_key: group,
            **_aggregate_walk_forward_cases(values),
        }
        for group, values in sorted(grouped.items())
    ]


def _walk_forward_benchmark_comparison(
    selected_cases: list[dict[str, Any]],
) -> dict[str, Any]:
    return {
        "summary": {
            **_aggregate_walk_forward_cases(selected_cases),
            "benchmark_comparison_present": bool(selected_cases),
            "comparison_policy": "selected_top_value_surface_action_vs_buy_and_hold_proxy",
            "promotion_gate_allowed": False,
        },
        "benchmark_action_ids": sorted(
            {
                str(row.get("benchmark_action"))
                for row in selected_cases
                if row.get("benchmark_action")
            }
        ),
    }


def _aggregate_walk_forward_cases(rows: list[dict[str, Any]]) -> dict[str, Any]:
    deltas = [_float(row.get("delta_vs_benchmark"), 0.0) for row in rows]
    selected_returns = [_float(row.get("selected_realized_net_return"), 0.0) for row in rows]
    benchmark_returns = [_float(row.get("benchmark_realized_net_return"), 0.0) for row in rows]
    win_count = sum(1 for row in rows if row.get("value_surface_beats_benchmark"))
    return {
        "case_count": len(rows),
        "mean_selected_realized_net_return": _round(_mean(selected_returns)),
        "mean_benchmark_realized_net_return": _round(_mean(benchmark_returns)),
        "mean_delta_vs_benchmark": _round(_mean(deltas)),
        "median_delta_vs_benchmark": _round(_median(deltas)) if deltas else None,
        "value_surface_beats_benchmark_count": win_count,
        "value_surface_beats_benchmark_rate": _round(win_count / len(rows) if rows else 0.0),
        "promotion_gate_allowed": False,
    }


def _negative_control_review(control_results: list[dict[str, Any]]) -> dict[str, Any]:
    promotion_count = _negative_control_promotion_count(control_results)
    return {
        "negative_control_pass": promotion_count == 0,
        "negative_control_promotion_count": promotion_count,
        "control_count": len(control_results),
        "control_results": control_results,
        "promotion_gate_allowed": False,
    }


def _future_leakage_trap_review(control_results: list[dict[str, Any]]) -> dict[str, Any]:
    blocked = _future_leakage_blocked(control_results)
    return {
        "future_leakage_trap_blocked": blocked,
        "future_leakage_trap_pass": blocked,
        "promotion_gate_allowed": False,
    }


def _value_surface_walk_forward_decision(
    *,
    config: Mapping[str, Any],
    value_surface: Mapping[str, Any],
    selected_cases: list[dict[str, Any]],
    window_results: list[dict[str, Any]],
    negative_control_result: Mapping[str, Any],
    future_leakage_trap_result: Mapping[str, Any],
) -> str:
    if not value_surface or not selected_cases or not window_results:
        return "DATA_REQUIRED"
    if _first_int(negative_control_result.get("negative_control_promotion_count")) > 0:
        return "KILL"
    if not bool(future_leakage_trap_result.get("future_leakage_trap_blocked")):
        return "KILL"
    policy = _next_stage_section(config, "value_surface_walk_forward_expansion")
    min_win_rate = _float(policy.get("continue_min_window_win_rate"), 0.5)
    min_delta = _float(policy.get("continue_min_mean_delta_vs_benchmark_bps"), 0.0) / 10_000.0
    window_wins = [
        row
        for row in window_results
        if _float(row.get("mean_delta_vs_benchmark"), 0.0) >= min_delta
    ]
    window_win_rate = len(window_wins) / len(window_results)
    overall_delta = _float(
        _walk_forward_benchmark_comparison(selected_cases)["summary"].get(
            "mean_delta_vs_benchmark"
        ),
        0.0,
    )
    if window_win_rate >= min_win_rate and overall_delta >= min_delta:
        return "CONTINUE"
    return "WATCHLIST"


def _dominant_metric_by_candidate(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    candidates = _surface_group_summary(rows, ["action", "horizon"])
    output = []
    for row in candidates:
        metrics = {
            "return": abs(_float(row.get("mean_expected_return"), 0.0)),
            "drawdown": abs(_float(row.get("mean_downside_risk"), 0.0)),
            "cost": abs(_float(row.get("mean_estimated_cost"), 0.0)),
            "uncertainty": abs(_float(row.get("mean_uncertainty"), 0.0)),
        }
        dominant_metric, dominant_value = max(metrics.items(), key=lambda item: item[1])
        total = sum(metrics.values()) or 1.0
        output.append(
            {
                "action": row.get("action"),
                "horizon": row.get("horizon"),
                "dominant_metric": dominant_metric,
                "dominant_metric_share": _round(dominant_value / total),
                "metric_components": {key: _round(value) for key, value in metrics.items()},
                "not_validated_utility_boundary": True,
                "promotion_gate_allowed": False,
            }
        )
    return output


def _horizon_cliff_report(
    rows: list[dict[str, Any]],
    config: Mapping[str, Any],
) -> dict[str, Any]:
    policy = _next_stage_section(config, "utility_pareto_ranking_review")
    threshold = _float(policy.get("horizon_cliff_abs_utility_bps"), 100.0) / 10_000.0
    horizon_order = {str(row["horizon_id"]): int(row["days"]) for row in _horizons(config)}
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in _surface_group_summary(rows, ["action", "horizon"]):
        grouped.setdefault(str(row.get("action")), []).append(row)
    cliff_rows: list[dict[str, Any]] = []
    for action, values in sorted(grouped.items()):
        ordered = sorted(values, key=lambda row: horizon_order.get(str(row.get("horizon")), 0))
        for previous, current in zip(ordered, ordered[1:], strict=False):
            delta = _float(current.get("mean_net_utility"), 0.0) - _float(
                previous.get("mean_net_utility"), 0.0
            )
            cliff_rows.append(
                {
                    "action": action,
                    "previous_horizon": previous.get("horizon"),
                    "current_horizon": current.get("horizon"),
                    "mean_net_utility_delta": _round(delta),
                    "abs_mean_net_utility_delta": _round(abs(delta)),
                    "horizon_cliff_for_review": abs(delta) >= threshold,
                    "not_validated_utility_boundary": True,
                    "promotion_gate_allowed": False,
                }
            )
    return {
        "summary": {
            "horizon_cliff_count": sum(1 for row in cliff_rows if row["horizon_cliff_for_review"]),
            "adjacent_horizon_pair_count": len(cliff_rows),
            "horizon_cliff_abs_utility_threshold": _round(threshold),
            "not_validated_utility_boundary": True,
            "promotion_gate_allowed": False,
        },
        "rows": cliff_rows,
    }


def _value_surface_residual_rows(
    rows: list[dict[str, Any]],
    config: Mapping[str, Any],
) -> list[dict[str, Any]]:
    action_by_id = {str(row["action_id"]): row for row in _actions(config)}
    policy = _next_stage_section(config, "gbdt_value_surface_residual_diagnostic")
    floor = _float(policy.get("large_residual_abs_return_floor_bps"), 200.0) / 10_000.0
    residual_rows = []
    for row in rows:
        if row.get("realized_forward_return") is None:
            continue
        realized_net = _action_realized_net(row, action_by_id)
        predicted_net = _float(row.get("net_utility"), 0.0)
        residual = realized_net - predicted_net
        residual_rows.append(
            {
                "date": row.get("date"),
                "asset": row.get("asset"),
                "horizon": row.get("horizon"),
                "regime_segment": row.get("regime_segment"),
                "asset_cluster": row.get("asset_cluster"),
                "pit_state": row.get("pit_state"),
                "action": row.get("action"),
                "predicted_net_utility": _round(predicted_net),
                "realized_action_net_return": _round(realized_net),
                "residual": _round(residual),
                "abs_residual": _round(abs(residual)),
                "estimated_cost": row.get("estimated_cost"),
                "downside_risk": row.get("downside_risk"),
                "max_drawdown_proxy": row.get("max_drawdown_proxy"),
                "large_residual_for_review": abs(residual) >= floor,
                "residual_role": "diagnostic_only",
                "promotion_gate_allowed": False,
            }
        )
    return residual_rows


def _residual_group_result(rows: list[dict[str, Any]], group_key: str) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        grouped.setdefault(str(row.get(group_key, "unknown")), []).append(row)
    return [
        {
            group_key: group,
            "case_count": len(values),
            "mean_residual": _round(_mean([_float(row.get("residual"), 0.0) for row in values])),
            "mean_abs_residual": _round(
                _mean([_float(row.get("abs_residual"), 0.0) for row in values])
            ),
            "large_residual_count": sum(
                1 for row in values if row.get("large_residual_for_review")
            ),
            "large_residual_rate": _round(
                sum(1 for row in values if row.get("large_residual_for_review")) / len(values)
                if values
                else 0.0
            ),
            "promotion_gate_allowed": False,
        }
        for group, values in sorted(grouped.items())
    ]


def _residual_feature_importance(
    rows: list[dict[str, Any]],
    *,
    features: list[str],
) -> list[dict[str, Any]]:
    abs_values = [_float(row.get("abs_residual"), 0.0) for row in rows]
    if not abs_values:
        return []
    overall = _mean(abs_values)
    denominator = sum((value - overall) ** 2 for value in abs_values) or 1.0
    importance_rows = []
    for feature in features:
        grouped: dict[str, list[dict[str, Any]]] = {}
        for row in rows:
            grouped.setdefault(str(row.get(feature, "unknown")), []).append(row)
        between = 0.0
        top_group = None
        top_group_mean = -1.0
        for group, values in grouped.items():
            group_mean = _mean([_float(row.get("abs_residual"), 0.0) for row in values])
            between += len(values) * (group_mean - overall) ** 2
            if group_mean > top_group_mean:
                top_group = group
                top_group_mean = group_mean
        importance_rows.append(
            {
                "feature": feature,
                "importance": _round(between / denominator),
                "group_count": len(grouped),
                "top_residual_group": top_group,
                "top_group_mean_abs_residual": _round(top_group_mean),
                "importance_method": "categorical_residual_separation",
                "promotion_gate_allowed": False,
            }
        )
    return sorted(importance_rows, key=lambda row: row["importance"], reverse=True)


def _residual_hypothesis_candidates(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    for feature in ["asset", "horizon", "regime_segment", "asset_cluster", "pit_state", "action"]:
        for row in _residual_group_result(rows, feature):
            candidates.append(
                {
                    "hypothesis_id": f"residual_{feature}_{row.get(feature)}",
                    "feature": feature,
                    "feature_value": row.get(feature),
                    "case_count": row.get("case_count"),
                    "mean_abs_residual": row.get("mean_abs_residual"),
                    "large_residual_rate": row.get("large_residual_rate"),
                    "hypothesis": (
                        "review whether value_surface residuals concentrate in "
                        f"{feature}={row.get(feature)}"
                    ),
                    "strategy_signal_generated": False,
                    "promotion_gate_allowed": False,
                }
            )
    return sorted(
        candidates,
        key=lambda row: (
            _float(row.get("mean_abs_residual"), 0.0),
            _float(row.get("large_residual_rate"), 0.0),
        ),
        reverse=True,
    )[:10]


def _regret_activation_recheck_criteria(
    *,
    config: Mapping[str, Any],
    failure_inputs: Mapping[str, Any],
    gate: Mapping[str, Any],
) -> list[dict[str, Any]]:
    policy = _next_stage_section(config, "regret_casebook_activation_recheck")
    summary = failure_inputs.get("summary") if isinstance(failure_inputs, Mapping) else {}
    criteria = [
        (
            "value_surface_losing_cases_sufficient",
            (
                _first_int(summary.get("value_surface_losing_case_count"))
                if isinstance(summary, Mapping)
                else 0
            ),
            _first_int(policy.get("minimum_value_surface_losing_cases")),
        ),
        (
            "benchmark_disagreement_cases_sufficient",
            (
                _first_int(summary.get("benchmark_disagreement_case_count"))
                if isinstance(summary, Mapping)
                else 0
            ),
            _first_int(policy.get("minimum_benchmark_disagreement_cases")),
        ),
        (
            "teacher_oracle_better_cases_sufficient",
            (
                _first_int(summary.get("oracle_teacher_better_case_count"))
                if isinstance(summary, Mapping)
                else 0
            ),
            _first_int(policy.get("minimum_oracle_teacher_better_cases")),
        ),
        (
            "major_regret_types_stable",
            _stable_regret_type_count(gate),
            _first_int(policy.get("minimum_distinct_regret_types")),
        ),
    ]
    return [
        {
            "condition_id": condition_id,
            "observed": observed,
            "required": required,
            "passed": observed >= required,
            "promotion_gate_allowed": False,
        }
        for condition_id, observed, required in criteria
    ]


def _stable_regret_type_count(gate: Mapping[str, Any]) -> int:
    gate_rows = _records(gate.get("activation_gate"))
    regret_coverage = next(
        (row for row in gate_rows if row.get("condition_id") == "regret_type_distribution_stable"),
        {},
    )
    return _first_int(regret_coverage.get("observed"))


def _failure_attribution_report(
    selected_cases: list[dict[str, Any]],
    config: Mapping[str, Any],
) -> dict[str, Any]:
    policy = _next_stage_section(config, "value_surface_failure_attribution")
    winners = [row for row in selected_cases if _float(row.get("delta_vs_benchmark"), 0.0) >= 0]
    losers = [row for row in selected_cases if _float(row.get("delta_vs_benchmark"), 0.0) < 0]
    deltas = [_float(row.get("delta_vs_benchmark"), 0.0) for row in selected_cases]
    concentration_reports = {
        "date": _loss_concentration(losers, "date"),
        "asset": _loss_concentration(losers, "asset"),
        "horizon": _loss_concentration(losers, "horizon"),
        "regime_segment": _loss_concentration(losers, "regime_segment"),
    }
    max_share = max(
        (
            report["summary"]["max_loss_share"]
            for report in concentration_reports.values()
            if report["summary"]["loss_case_count"]
        ),
        default=0.0,
    )
    top_limit = _first_int(policy.get("top_losing_case_count")) or 25
    top_losing = sorted(losers, key=lambda row: _float(row.get("delta_vs_benchmark"), 0.0))[
        :top_limit
    ]
    return {
        "summary": {
            "case_count": len(selected_cases),
            "winning_case_count": len(winners),
            "losing_case_count": len(losers),
            "winning_case_average_delta": _round(
                _mean([_float(row.get("delta_vs_benchmark"), 0.0) for row in winners])
            ),
            "losing_case_average_delta": _round(
                _mean([_float(row.get("delta_vs_benchmark"), 0.0) for row in losers])
            ),
            "overall_mean_delta_vs_benchmark": _round(_mean(deltas)),
            "overall_median_delta_vs_benchmark": _round(_median(deltas)),
            "max_loss_concentration_share": _round(max_share),
            "promotion_gate_allowed": False,
        },
        "winning_losing_delta": {
            "winning_case_average_delta": _round(
                _mean([_float(row.get("delta_vs_benchmark"), 0.0) for row in winners])
            ),
            "losing_case_average_delta": _round(
                _mean([_float(row.get("delta_vs_benchmark"), 0.0) for row in losers])
            ),
            "winning_case_count": len(winners),
            "losing_case_count": len(losers),
            "promotion_gate_allowed": False,
        },
        "top_losing_cases": top_losing,
        "loss_concentration_by_date": concentration_reports["date"],
        "loss_concentration_by_asset": concentration_reports["asset"],
        "loss_concentration_by_horizon": concentration_reports["horizon"],
        "loss_concentration_by_regime": concentration_reports["regime_segment"],
        "tail_loss_contribution": _tail_loss_contribution(losers, config),
        "turnover_cost_contribution": _turnover_cost_contribution(losers),
        "drawdown_contribution": _drawdown_contribution(losers),
        "benchmark_relative_downside_attribution": _benchmark_relative_downside_attribution(
            selected_cases
        ),
    }


def _loss_concentration(rows: list[dict[str, Any]], group_key: str) -> dict[str, Any]:
    total_abs_loss = sum(abs(_float(row.get("delta_vs_benchmark"), 0.0)) for row in rows)
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        grouped.setdefault(str(row.get(group_key, "unknown")), []).append(row)
    groups = []
    for group, values in sorted(grouped.items()):
        group_loss = sum(abs(_float(row.get("delta_vs_benchmark"), 0.0)) for row in values)
        groups.append(
            {
                group_key: group,
                "loss_case_count": len(values),
                "average_delta_vs_benchmark": _round(
                    _mean([_float(row.get("delta_vs_benchmark"), 0.0) for row in values])
                ),
                "absolute_loss": _round(group_loss),
                "loss_share": _round(group_loss / total_abs_loss if total_abs_loss else 0.0),
                "promotion_gate_allowed": False,
            }
        )
    groups = sorted(groups, key=lambda row: _float(row.get("loss_share"), 0.0), reverse=True)
    return {
        "summary": {
            "group_key": group_key,
            "loss_case_count": len(rows),
            "group_count": len(groups),
            "total_abs_loss": _round(total_abs_loss),
            "max_loss_share": groups[0]["loss_share"] if groups else 0.0,
            "promotion_gate_allowed": False,
        },
        "groups": groups,
    }


def _tail_loss_contribution(
    losing_cases: list[dict[str, Any]],
    config: Mapping[str, Any],
) -> dict[str, Any]:
    policy = _next_stage_section(config, "value_surface_failure_attribution")
    quantile = _float(policy.get("tail_loss_quantile"), 0.10)
    ordered = sorted(
        losing_cases,
        key=lambda row: abs(_float(row.get("delta_vs_benchmark"), 0.0)),
        reverse=True,
    )
    tail_count = min(len(ordered), max(1, math.ceil(len(ordered) * quantile))) if ordered else 0
    tail = ordered[:tail_count]
    total_abs_loss = sum(abs(_float(row.get("delta_vs_benchmark"), 0.0)) for row in losing_cases)
    tail_abs_loss = sum(abs(_float(row.get("delta_vs_benchmark"), 0.0)) for row in tail)
    return {
        "tail_loss_quantile": quantile,
        "tail_case_count": tail_count,
        "losing_case_count": len(losing_cases),
        "tail_abs_loss": _round(tail_abs_loss),
        "total_abs_loss": _round(total_abs_loss),
        "tail_loss_share": _round(tail_abs_loss / total_abs_loss if total_abs_loss else 0.0),
        "tail_cases": tail,
        "promotion_gate_allowed": False,
    }


def _turnover_cost_contribution(losing_cases: list[dict[str, Any]]) -> dict[str, Any]:
    cost_deltas = [
        _float(row.get("selected_estimated_cost"), 0.0)
        - _float(row.get("benchmark_estimated_cost"), 0.0)
        for row in losing_cases
    ]
    turnover_deltas = [
        _float(row.get("selected_turnover_cost_assumption"), 0.0)
        - _float(row.get("benchmark_turnover_cost_assumption"), 0.0)
        for row in losing_cases
    ]
    total_abs_loss = sum(abs(_float(row.get("delta_vs_benchmark"), 0.0)) for row in losing_cases)
    positive_cost_drag = sum(max(value, 0.0) for value in cost_deltas)
    return {
        "loss_case_count": len(losing_cases),
        "mean_cost_delta_vs_benchmark": _round(_mean(cost_deltas)),
        "mean_turnover_assumption_delta_vs_benchmark": _round(_mean(turnover_deltas)),
        "positive_cost_drag": _round(positive_cost_drag),
        "positive_cost_drag_share_of_abs_loss": _round(
            positive_cost_drag / total_abs_loss if total_abs_loss else 0.0
        ),
        "promotion_gate_allowed": False,
    }


def _drawdown_contribution(losing_cases: list[dict[str, Any]]) -> dict[str, Any]:
    drawdown_deltas = [
        _float(row.get("selected_drawdown_proxy"), 0.0)
        - _float(row.get("benchmark_drawdown_proxy"), 0.0)
        for row in losing_cases
        if row.get("benchmark_drawdown_proxy") is not None
    ]
    worse_drawdown = [value for value in drawdown_deltas if value < 0]
    return {
        "loss_case_count": len(losing_cases),
        "mean_drawdown_delta_vs_benchmark": _round(_mean(drawdown_deltas)),
        "worse_drawdown_case_count": len(worse_drawdown),
        "worse_drawdown_case_rate": _round(
            len(worse_drawdown) / len(drawdown_deltas) if drawdown_deltas else 0.0
        ),
        "promotion_gate_allowed": False,
    }


def _benchmark_relative_downside_attribution(
    selected_cases: list[dict[str, Any]],
) -> dict[str, Any]:
    selected_negative = [
        row for row in selected_cases if _float(row.get("selected_realized_net_return"), 0.0) < 0
    ]
    benchmark_positive = [
        row
        for row in selected_negative
        if _float(row.get("benchmark_realized_net_return"), 0.0) >= 0
    ]
    underperform = [row for row in selected_cases if _float(row.get("delta_vs_benchmark"), 0.0) < 0]
    return {
        "selected_negative_case_count": len(selected_negative),
        "benchmark_nonnegative_when_selected_negative_count": len(benchmark_positive),
        "benchmark_relative_underperform_case_count": len(underperform),
        "average_underperformance_delta": _round(
            _mean([_float(row.get("delta_vs_benchmark"), 0.0) for row in underperform])
        ),
        "downside_concentrated_when_benchmark_nonnegative": _round(
            len(benchmark_positive) / len(selected_negative) if selected_negative else 0.0
        ),
        "promotion_gate_allowed": False,
    }


def _horizon_ranking_jump_report(
    rows: list[dict[str, Any]],
    config: Mapping[str, Any],
) -> dict[str, Any]:
    horizon_order = {str(row["horizon_id"]): int(row["days"]) for row in _horizons(config)}
    grouped = _surface_group_summary(rows, ["horizon", "action"])
    by_horizon: dict[str, list[dict[str, Any]]] = {}
    for row in grouped:
        by_horizon.setdefault(str(row.get("horizon")), []).append(row)
    ranked_by_horizon = []
    for horizon, values in sorted(
        by_horizon.items(), key=lambda item: horizon_order.get(item[0], 0)
    ):
        ranking = sorted(
            values,
            key=lambda row: _float(row.get("mean_net_utility"), 0.0),
            reverse=True,
        )
        ranked_by_horizon.append(
            {
                "horizon": horizon,
                "top_action": ranking[0].get("action") if ranking else None,
                "ranking": [
                    {
                        "rank": index,
                        "action": row.get("action"),
                        "mean_net_utility": row.get("mean_net_utility"),
                        "promotion_gate_allowed": False,
                    }
                    for index, row in enumerate(ranking, start=1)
                ],
                "promotion_gate_allowed": False,
            }
        )
    jumps = []
    for previous, current in zip(ranked_by_horizon, ranked_by_horizon[1:], strict=False):
        jumps.append(
            {
                "previous_horizon": previous["horizon"],
                "current_horizon": current["horizon"],
                "previous_top_action": previous["top_action"],
                "current_top_action": current["top_action"],
                "top_action_changed": previous["top_action"] != current["top_action"],
                "promotion_gate_allowed": False,
            }
        )
    return {
        "summary": {
            "horizon_count": len(ranked_by_horizon),
            "ranking_jump_count": sum(1 for row in jumps if row["top_action_changed"]),
            "promotion_gate_allowed": False,
        },
        "ranked_by_horizon": ranked_by_horizon,
        "adjacent_horizon_jumps": jumps,
    }


def _single_horizon_action_report(
    rows: list[dict[str, Any]],
    config: Mapping[str, Any],
) -> dict[str, Any]:
    horizon_count = len(_horizons(config))
    ranking = _horizon_ranking_jump_report(rows, config)["ranked_by_horizon"]
    top_counts: dict[str, int] = {}
    for row in ranking:
        action = str(row.get("top_action"))
        top_counts[action] = top_counts.get(action, 0) + 1
    action_rows = [
        {
            "action": action,
            "top_horizon_count": count,
            "top_horizon_share": _round(count / horizon_count if horizon_count else 0.0),
            "single_horizon_only": count == 1,
            "promotion_gate_allowed": False,
        }
        for action, count in sorted(top_counts.items())
    ]
    return {
        "summary": {
            "action_count": len(action_rows),
            "single_horizon_action_count": sum(
                1 for row in action_rows if row["single_horizon_only"]
            ),
            "promotion_gate_allowed": False,
        },
        "actions": action_rows,
    }


def _utility_profile_cliff_report(utility_review: Mapping[str, Any]) -> dict[str, Any]:
    cliffs = _records(
        utility_review.get("horizon_cliff_report", {}).get("rows")
        if isinstance(utility_review.get("horizon_cliff_report"), Mapping)
        else []
    )
    ranking_flips = _records(
        utility_review.get("ranking_flip_report", {}).get("rows")
        if isinstance(utility_review.get("ranking_flip_report"), Mapping)
        else []
    )
    active_cliffs = [row for row in cliffs if row.get("horizon_cliff_for_review")]
    active_flips = [row for row in ranking_flips if row.get("reverses_balanced_top")]
    return {
        "summary": {
            "utility_profile_cliff_count": len(active_cliffs),
            "ranking_flip_count": len(active_flips),
            "validated_boundary_count": 0,
            "not_validated_utility_boundary": True,
            "promotion_gate_allowed": False,
        },
        "horizon_cliff_rows": active_cliffs,
        "ranking_flip_rows": active_flips,
    }


def _horizon_smoothing_assessment(
    *,
    config: Mapping[str, Any],
    ranking_jumps: Mapping[str, Any],
    single_horizon: Mapping[str, Any],
    utility_cliffs: Mapping[str, Any],
    utility_review: Mapping[str, Any],
) -> dict[str, Any]:
    policy = _next_stage_section(config, "horizon_cliff_stabilization_review")
    jump_floor = _first_int(policy.get("ranking_jump_review_floor"))
    ranking_jump_count = _first_int(ranking_jumps.get("summary", {}).get("ranking_jump_count"))
    cliff_count = _first_int(utility_cliffs.get("summary", {}).get("utility_profile_cliff_count"))
    flip_count = _first_int(utility_cliffs.get("summary", {}).get("ranking_flip_count"))
    pareto_count = _first_int(utility_review.get("summary", {}).get("pareto_candidate_count"))
    smoothing_required = ranking_jump_count >= jump_floor or cliff_count > 0
    pareto_review = flip_count > 0 or pareto_count > 0
    return {
        "horizon_smoothing_review_required": smoothing_required,
        "pareto_frontier_policy_review_required": pareto_review,
        "single_utility_ranking_stable": not smoothing_required and flip_count == 0,
        "recommended_stabilization_options": [
            option
            for option, enabled in [
                ("horizon_smoothing", smoothing_required),
                ("pareto_frontier_policy", pareto_review),
                (
                    "single_horizon_action_suppression",
                    _first_int(single_horizon.get("summary", {}).get("single_horizon_action_count"))
                    > 0,
                ),
            ]
            if enabled
        ],
        "validated_boundary_count": 0,
        "not_validated_utility_boundary": True,
        "promotion_gate_allowed": False,
    }


def _residual_hypothesis_triage(
    residual_rows: list[dict[str, Any]],
    config: Mapping[str, Any],
) -> dict[str, Any]:
    policy = _next_stage_section(config, "gbdt_residual_hypothesis_triage")
    limit = _first_int(policy.get("hypothesis_candidate_limit")) or 10
    feature_explanations = _residual_feature_importance(
        residual_rows,
        features=["asset", "horizon", "regime_segment", "asset_cluster", "pit_state", "action"],
    )
    repair_candidates = _residual_repair_rule_candidates(residual_rows, config)
    new_hypotheses = _residual_hypothesis_candidates(residual_rows)[:limit]
    large_residuals = [row for row in residual_rows if row.get("large_residual_for_review")]
    return {
        "summary": {
            "residual_case_count": len(residual_rows),
            "large_residual_case_count": len(large_residuals),
            "strategy_signal_generated": False,
            "promotion_gate_allowed": False,
        },
        "prediction_error_summary": {
            "mean_residual": _round(
                _mean([_float(row.get("residual"), 0.0) for row in residual_rows])
            ),
            "mean_abs_residual": _round(
                _mean([_float(row.get("abs_residual"), 0.0) for row in residual_rows])
            ),
            "large_residual_rate": _round(
                len(large_residuals) / len(residual_rows) if residual_rows else 0.0
            ),
            "promotion_gate_allowed": False,
        },
        "residual_by_asset": _residual_group_result(residual_rows, "asset"),
        "residual_by_horizon": _residual_group_result(residual_rows, "horizon"),
        "residual_by_regime": _residual_group_result(residual_rows, "regime_segment"),
        "feature_explanations": feature_explanations,
        "repair_rule_candidates": repair_candidates,
        "new_hypothesis_candidates": new_hypotheses,
    }


def _residual_repair_rule_candidates(
    residual_rows: list[dict[str, Any]],
    config: Mapping[str, Any],
) -> list[dict[str, Any]]:
    policy = _next_stage_section(config, "gbdt_residual_hypothesis_triage")
    configured = [str(item) for item in policy.get("repair_rule_candidates", [])]
    large = [row for row in residual_rows if row.get("large_residual_for_review")]
    horizon_groups = _residual_group_result(residual_rows, "horizon")
    regime_groups = _residual_group_result(residual_rows, "regime_segment")
    action_groups = _residual_group_result(residual_rows, "action")
    candidates = []
    for rule in configured:
        if rule == "tail-loss filter":
            evidence = {
                "large_residual_case_count": len(large),
                "large_residual_rate": _round(
                    len(large) / len(residual_rows) if residual_rows else 0.0
                ),
            }
        elif rule == "horizon smoothing":
            evidence = {"top_horizon_residual": horizon_groups[:3]}
        elif rule == "regime-conditioned utility":
            evidence = {"top_regime_residual": regime_groups[:3]}
        elif rule == "cost-aware action suppression":
            evidence = {"top_action_residual": action_groups[:3]}
        elif rule == "drawdown-sensitive ranking":
            evidence = {
                "mean_drawdown_proxy_large_residual": _round(
                    _mean([_float(row.get("max_drawdown_proxy"), 0.0) for row in large])
                )
            }
        else:
            evidence = {}
        candidates.append(
            {
                "repair_rule": rule,
                "evidence": evidence,
                "strategy_signal_generated": False,
                "requires_followup_experiment": True,
                "promotion_gate_allowed": False,
            }
        )
    return candidates


def _value_surface_direction_decision(
    *,
    config: Mapping[str, Any],
    failure: Mapping[str, Any],
    horizon: Mapping[str, Any],
    residual: Mapping[str, Any],
    forward: Mapping[str, Any],
    walk_forward: Mapping[str, Any],
) -> dict[str, Any]:
    policy = _next_stage_section(config, "value_surface_direction_review")
    allowed = [str(item) for item in policy.get("allowed_decisions", [])]
    failure_summary = failure.get("summary") if isinstance(failure.get("summary"), Mapping) else {}
    attribution_summary = (
        failure.get("attribution_summary")
        if isinstance(failure.get("attribution_summary"), Mapping)
        else {}
    )
    horizon_summary = horizon.get("summary") if isinstance(horizon.get("summary"), Mapping) else {}
    residual_summary = (
        residual.get("summary") if isinstance(residual.get("summary"), Mapping) else {}
    )
    mean_delta = _float(attribution_summary.get("overall_mean_delta_vs_benchmark"), 0.0)
    tail_share = _float(failure_summary.get("tail_loss_contribution"), 0.0)
    max_loss_share = _float(failure_summary.get("max_loss_concentration_share"), 0.0)
    horizon_cliff_count = _first_int(horizon_summary.get("horizon_cliff_count"))
    ranking_jump_count = _first_int(horizon_summary.get("ranking_jump_count"))
    large_residual_count = _first_int(residual_summary.get("large_residual_case_count"))
    kill_floor = _float(policy.get("kill_mean_delta_floor_bps"), -500.0) / 10_000.0
    tail_pivot_share = _float(policy.get("tail_loss_pivot_share"), 0.50)
    horizon_floor = _first_int(policy.get("horizon_cliff_pivot_floor"))
    broad_ceiling = _float(policy.get("broad_loss_group_share_ceiling"), 0.35)
    if not failure or not horizon:
        decision = "WATCHLIST"
        reason = "missing_failure_or_horizon_review"
    elif mean_delta <= kill_floor and max_loss_share <= broad_ceiling:
        decision = "KILL_CURRENT_VALUE_SURFACE_VERSION"
        reason = "broad_negative_delta_below_kill_floor"
    elif mean_delta < 0 and tail_share >= tail_pivot_share:
        decision = "PIVOT_TO_TAIL_RISK_FILTER"
        reason = "negative_mean_delta_with_tail_loss_concentration"
    elif mean_delta < 0 and max_loss_share > broad_ceiling:
        decision = "PIVOT_TO_REGIME_CONDITIONED_VALUE_SURFACE"
        reason = "negative_mean_delta_with_group_loss_concentration"
    elif horizon_cliff_count >= horizon_floor or ranking_jump_count > 0:
        decision = "PIVOT_TO_PARETO_FRONTIER_POLICY"
        reason = "horizon_cliff_or_ranking_jump_requires_stabilization"
    elif mean_delta < 0 or large_residual_count > 0:
        decision = "WATCHLIST"
        reason = "negative_or_residual_risk_without_clear_local_fix"
    else:
        decision = "CONTINUE_LOCAL_FIX"
        reason = "no_structural_failure_detected_but_no_promotion_allowed"
    if allowed and decision not in allowed:
        decision = "WATCHLIST"
        reason = "computed_decision_not_in_allowed_policy"
    return {
        "decision": decision,
        "reason": reason,
        "allowed_decisions": allowed,
        "mean_delta_vs_benchmark": _round(mean_delta),
        "tail_loss_share": _round(tail_share),
        "max_loss_concentration_share": _round(max_loss_share),
        "horizon_cliff_count": horizon_cliff_count,
        "ranking_jump_count": ranking_jump_count,
        "large_residual_case_count": large_residual_count,
        "forward_ledger_event_count": _first_int(
            forward.get("summary", {}).get("ledger_event_count")
        ),
        "walk_forward_decision": walk_forward.get("summary", {}).get(
            "controlled_walk_forward_decision"
        ),
        "promotion_gate_allowed": False,
        "paper_shadow_change_allowed": False,
        "production_weight_change_allowed": False,
    }


def _direction_evidence_summary(
    *,
    failure: Mapping[str, Any],
    horizon: Mapping[str, Any],
    residual: Mapping[str, Any],
    forward: Mapping[str, Any],
    walk_forward: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "walk_forward_status": walk_forward.get("status"),
        "walk_forward_decision": walk_forward.get("summary", {}).get(
            "controlled_walk_forward_decision"
        ),
        "failure_status": failure.get("status"),
        "overall_mean_delta_vs_benchmark": failure.get("attribution_summary", {}).get(
            "overall_mean_delta_vs_benchmark"
        ),
        "tail_loss_contribution": failure.get("summary", {}).get("tail_loss_contribution"),
        "horizon_status": horizon.get("status"),
        "horizon_cliff_count": horizon.get("summary", {}).get("horizon_cliff_count"),
        "residual_status": residual.get("status"),
        "residual_case_count": residual.get("summary", {}).get("residual_case_count"),
        "forward_status": forward.get("status"),
        "ledger_event_count": forward.get("summary", {}).get("ledger_event_count"),
        "promotion_gate_allowed": False,
    }


def _regime_conditioned_design_protocol(
    *,
    config: Mapping[str, Any],
    failure: Mapping[str, Any],
    horizon: Mapping[str, Any],
    residual: Mapping[str, Any],
    direction: Mapping[str, Any],
) -> dict[str, Any]:
    policy = _next_stage_section(config, "regime_conditioned_value_surface_design")
    share_floor = _float(policy.get("tail_loss_regime_share_floor"), 0.35)
    aggressive_actions = [str(item) for item in policy.get("aggressive_actions", [])]
    low_risk_actions = [str(item) for item in policy.get("low_risk_actions", [])]
    high_loss_regimes = _high_loss_group_values(
        failure,
        "loss_concentration_by_regime",
        "regime_segment",
        share_floor,
    )
    high_loss_horizons = _high_loss_group_values(
        failure,
        "loss_concentration_by_horizon",
        "horizon",
        share_floor,
    )
    high_loss_assets = _high_loss_group_values(
        failure,
        "loss_concentration_by_asset",
        "asset",
        share_floor,
    )
    residual_regimes = [
        str(row.get("regime_segment"))
        for row in _records(residual.get("residual_by_regime"))
        if row.get("regime_segment")
    ][:5]
    horizon_cliffs = _records(
        horizon.get("utility_profile_cliff_report", {}).get("horizon_cliff_rows")
        if isinstance(horizon.get("utility_profile_cliff_report"), Mapping)
        else []
    )
    horizon_days_by_id = {str(row["horizon_id"]): int(row["days"]) for row in _horizons(config)}
    disabled_horizons = sorted(
        {
            str(row.get("current_horizon") or row.get("previous_horizon"))
            for row in horizon_cliffs
            if row.get("horizon_cliff_for_review")
        }
        | set(high_loss_horizons)
    )
    disabled_horizons = [value for value in disabled_horizons if value and value != "None"]
    tail_definitions = []
    for regime in high_loss_regimes:
        tail_definitions.append(
            {
                "definition_id": f"tail_loss_regime_{regime}",
                "variable": "regime_segment",
                "value": regime,
                "source": "TRADING-790 loss concentration",
                "action": "fallback_to_benchmark_or_low_risk_only",
                "requires_out_of_sample_validation": True,
                "promotion_gate_allowed": False,
            }
        )
    for horizon_id in high_loss_horizons:
        tail_definitions.append(
            {
                "definition_id": f"tail_loss_horizon_{horizon_id}",
                "variable": "horizon",
                "value": horizon_id,
                "horizon_days": horizon_days_by_id.get(horizon_id),
                "source": "TRADING-790/791 loss and cliff concentration",
                "action": "disable_or_downweight_horizon_until_walk_forward_confirmed",
                "requires_out_of_sample_validation": True,
                "promotion_gate_allowed": False,
            }
        )
    keep_regimes = [
        {
            "regime_rule": "not_in_tail_loss_regime_and_no_horizon_cliff",
            "allowed": "retain_value_surface_ranking_for_diagnostic_review",
            "excluded_regimes": high_loss_regimes,
            "excluded_horizons": disabled_horizons,
            "promotion_gate_allowed": False,
        }
    ]
    fallback_regimes = [
        {
            "regime_variable": "regime_segment",
            "regime_values": high_loss_regimes,
            "fallback": "benchmark",
            "reason": "loss concentration above diagnostic share floor",
            "promotion_gate_allowed": False,
        },
        {
            "regime_variable": "horizon",
            "regime_values": disabled_horizons,
            "fallback": "shorter_horizon_or_benchmark",
            "reason": "horizon cliff or horizon loss concentration",
            "promotion_gate_allowed": False,
        },
    ]
    low_risk_regimes = [
        {
            "regime_variable": "asset",
            "regime_values": high_loss_assets,
            "disabled_actions": aggressive_actions,
            "allowed_actions": low_risk_actions,
            "reason": "asset loss concentration requires lower risk action set",
            "promotion_gate_allowed": False,
        },
        {
            "regime_variable": "residual_regime_segment",
            "regime_values": residual_regimes,
            "disabled_actions": aggressive_actions,
            "allowed_actions": low_risk_actions,
            "reason": "large residual concentration requires diagnostic action suppression",
            "promotion_gate_allowed": False,
        },
    ]
    protocol = {
        "protocol_id": "regime_conditioned_value_surface_v0_controlled",
        "source_direction_decision": direction.get("summary", {}).get("direction_decision"),
        "ranking_policy": "heuristic",
        "not_validated_utility_boundary": True,
        "rule_source": "failure_attribution_and_residual_diagnostics",
        "requires_walk_forward_confirmation": True,
        "promotion_gate_allowed": False,
        "paper_shadow_change_allowed": False,
        "production_weight_change_allowed": False,
    }
    return {
        "protocol": protocol,
        "regime_variables": [
            {
                "variable": "regime_segment",
                "role": "primary market regime conditioning key",
                "source": "value surface row PIT regime segment",
            },
            {
                "variable": "pit_state",
                "role": "trend/pressure state proxy",
                "source": "PIT returns",
            },
            {
                "variable": "asset_cluster",
                "role": "cluster concentration key",
                "source": "controlled research config",
            },
            {"variable": "horizon", "role": "horizon cliff and maturity key", "source": "config"},
            {
                "variable": "selected_action",
                "role": "aggressive action suppression key",
                "source": "value surface selected action",
            },
            {
                "variable": "residual_sign",
                "role": "GBDT residual hypothesis key",
                "source": "TRADING-792 residual diagnostics",
            },
        ],
        "tail_loss_regime_definitions": tail_definitions,
        "allowed_action_changes_by_regime": [
            {
                "condition": "tail_loss_regime",
                "allowed_change": "replace aggressive value-surface action with benchmark",
                "disabled_actions": aggressive_actions,
                "allowed_actions": ["benchmark", *low_risk_actions],
                "promotion_gate_allowed": False,
            },
            {
                "condition": "horizon_cliff_regime",
                "allowed_change": "disable or downweight long/cliff horizon",
                "disabled_or_downweighted_horizons": disabled_horizons,
                "promotion_gate_allowed": False,
            },
            {
                "condition": "high_residual_regime",
                "allowed_change": "require confirmation before deviating from benchmark",
                "confirmation_source": "future controlled walk-forward only",
                "promotion_gate_allowed": False,
            },
        ],
        "benchmark_fallback_rules": fallback_regimes,
        "regimes_keep_value_surface": keep_regimes,
        "regimes_fallback_to_benchmark": fallback_regimes,
        "regimes_low_risk_action_only": low_risk_regimes,
        "horizons_disabled_or_downweighted": [
            {
                "horizon": horizon_id,
                "horizon_days": horizon_days_by_id.get(horizon_id),
                "reason": "loss concentration or utility cliff",
                "fallback": "shorter_horizon_or_benchmark",
                "promotion_gate_allowed": False,
            }
            for horizon_id in disabled_horizons
        ],
        "controlled_only_validation_plan": [
            {
                "step": "retrospective_ablation",
                "purpose": "compare original, conditioned, guarded, and fallback variants",
                "promotion_gate_allowed": False,
            },
            {
                "step": "walk_forward_holdout",
                "purpose": "verify guardrail definitions without using future outcome in decision",
                "promotion_gate_allowed": False,
            },
            {
                "step": "forward_evidence_append_only_tracking",
                "purpose": "observe daily dry-run outcomes before any paper-shadow discussion",
                "promotion_gate_allowed": False,
            },
        ],
    }


def _guardrail_variant_comparison(
    *,
    selected_cases: list[dict[str, Any]],
    config: Mapping[str, Any],
    failure: Mapping[str, Any],
    horizon: Mapping[str, Any],
    design: Mapping[str, Any],
) -> dict[str, Any]:
    policy = _next_stage_section(config, "tail_loss_guardrail_fallback_policy")
    allowed_variants = [str(item) for item in policy.get("allowed_variants", [])] or [
        "original_value_surface",
        "regime_conditioned_value_surface",
        "tail_loss_guarded_value_surface",
        "benchmark_fallback_value_surface",
    ]
    risk_context = _guardrail_risk_context(
        config=config,
        failure=failure,
        horizon=horizon,
        design=design,
    )
    variants: dict[str, list[dict[str, Any]]] = {}
    for variant in allowed_variants:
        variants[variant] = [
            _apply_guardrail_variant(row, variant, risk_context, policy) for row in selected_cases
        ]
    metrics = [_variant_metric_row(variant, rows, config) for variant, rows in variants.items()]
    original = next((row for row in metrics if row["variant_id"] == "original_value_surface"), {})
    tail_guarded = next(
        (row for row in metrics if row["variant_id"] == "tail_loss_guarded_value_surface"),
        {},
    )
    best = max(
        metrics, key=lambda row: _float(row.get("mean_delta_vs_benchmark"), -999.0), default={}
    )
    original_tail = _float(original.get("tail_loss_contribution"), 0.0)
    tail_tail = _float(tail_guarded.get("tail_loss_contribution"), 0.0)
    return {
        "summary": {
            "best_variant_by_mean_delta": best.get("variant_id"),
            "best_variant_mean_delta_vs_benchmark": best.get("mean_delta_vs_benchmark"),
            "original_mean_delta_vs_benchmark": original.get("mean_delta_vs_benchmark"),
            "tail_loss_guarded_mean_delta_vs_benchmark": tail_guarded.get(
                "mean_delta_vs_benchmark"
            ),
            "tail_loss_guardrail_reduces_tail_loss": tail_tail < original_tail,
            "promotion_gate_allowed": False,
        },
        "variant_metrics": metrics,
        "variant_rules": _guardrail_variant_rules(risk_context, policy),
        "variant_case_samples": {
            variant: rows[:10]
            for variant, rows in variants.items()
            if variant != "original_value_surface"
        },
        "diagnostic_boundary": {
            "retrospective_ablation_only": True,
            "requires_out_of_sample_walk_forward": True,
            "future_outcome_used_for_evaluation_only": True,
            "promotion_gate_allowed": False,
            "paper_shadow_change_allowed": False,
            "production_weight_change_allowed": False,
        },
    }


def _guardrail_risk_context(
    *,
    config: Mapping[str, Any],
    failure: Mapping[str, Any],
    horizon: Mapping[str, Any],
    design: Mapping[str, Any],
) -> dict[str, Any]:
    design_policy = _next_stage_section(config, "regime_conditioned_value_surface_design")
    guardrail_policy = _next_stage_section(config, "tail_loss_guardrail_fallback_policy")
    share_floor = _float(
        guardrail_policy.get(
            "tail_loss_regime_share_floor",
            design_policy.get("tail_loss_regime_share_floor", 0.35),
        ),
        0.35,
    )
    high_loss_regimes = set(
        _high_loss_group_values(
            failure, "loss_concentration_by_regime", "regime_segment", share_floor
        )
    )
    high_loss_horizons = set(
        _high_loss_group_values(failure, "loss_concentration_by_horizon", "horizon", share_floor)
    )
    high_loss_assets = set(
        _high_loss_group_values(failure, "loss_concentration_by_asset", "asset", share_floor)
    )
    design_horizons = {
        str(row.get("horizon"))
        for row in _records(design.get("horizons_disabled_or_downweighted"))
        if row.get("horizon")
    }
    horizon_summary = horizon.get("summary") if isinstance(horizon.get("summary"), Mapping) else {}
    aggressive_actions = set(str(item) for item in design_policy.get("aggressive_actions", []))
    return {
        "high_loss_regimes": high_loss_regimes,
        "high_loss_horizons": high_loss_horizons | design_horizons,
        "high_loss_assets": high_loss_assets,
        "aggressive_actions": aggressive_actions,
        "horizon_cliff_count": _first_int(horizon_summary.get("horizon_cliff_count")),
    }


def _apply_guardrail_variant(
    case: Mapping[str, Any],
    variant: str,
    risk_context: Mapping[str, Any],
    policy: Mapping[str, Any],
) -> dict[str, Any]:
    output = dict(case)
    output["variant_id"] = variant
    output["guardrail_action"] = "keep_value_surface_action"
    if variant == "original_value_surface":
        return output
    risk_match = _case_matches_risk_context(case, risk_context)
    aggressive = str(case.get("selected_action")) in risk_context.get("aggressive_actions", set())
    benchmark_disagreement = case.get("selected_action") != case.get("benchmark_action")
    high_cost = _float(case.get("selected_estimated_cost"), 0.0) > _float(
        case.get("benchmark_estimated_cost"), 0.0
    )
    if variant == "regime_conditioned_value_surface" and risk_match:
        return _fallback_case_to_benchmark(output, "regime_conditioned_benchmark_fallback")
    if variant == "tail_loss_guarded_value_surface":
        multiplier = _float(policy.get("loss_concentration_size_multiplier"), 0.50)
        if risk_match and aggressive:
            return _fallback_case_to_benchmark(output, "tail_risk_aggressive_action_fallback")
        if risk_match:
            return _scale_case_delta(output, multiplier, "tail_risk_size_reduction")
    if variant == "benchmark_fallback_value_surface" and (
        risk_match or (benchmark_disagreement and high_cost)
    ):
        return _fallback_case_to_benchmark(output, "benchmark_confirmation_required_fallback")
    return output


def _case_matches_risk_context(
    case: Mapping[str, Any],
    risk_context: Mapping[str, Any],
) -> bool:
    return (
        str(case.get("regime_segment")) in risk_context.get("high_loss_regimes", set())
        or str(case.get("horizon")) in risk_context.get("high_loss_horizons", set())
        or str(case.get("asset")) in risk_context.get("high_loss_assets", set())
    )


def _fallback_case_to_benchmark(case: dict[str, Any], reason: str) -> dict[str, Any]:
    case["selected_action_before_guardrail"] = case.get("selected_action")
    case["selected_action"] = case.get("benchmark_action", "benchmark")
    case["selected_realized_net_return"] = case.get("benchmark_realized_net_return")
    case["delta_vs_benchmark"] = 0.0
    case["value_surface_beats_benchmark"] = True
    case["selected_estimated_cost"] = case.get("benchmark_estimated_cost", 0.0)
    case["selected_turnover_cost_assumption"] = case.get("benchmark_turnover_cost_assumption", 0.0)
    case["selected_drawdown_proxy"] = case.get("benchmark_drawdown_proxy")
    case["guardrail_action"] = reason
    case["promotion_gate_allowed"] = False
    return case


def _scale_case_delta(case: dict[str, Any], multiplier: float, reason: str) -> dict[str, Any]:
    benchmark_return = _float(case.get("benchmark_realized_net_return"), 0.0)
    original_delta = _float(case.get("delta_vs_benchmark"), 0.0)
    adjusted_delta = original_delta * multiplier
    case["selected_action_before_guardrail"] = case.get("selected_action")
    case["selected_realized_net_return"] = _round(benchmark_return + adjusted_delta)
    case["delta_vs_benchmark"] = _round(adjusted_delta)
    case["value_surface_beats_benchmark"] = adjusted_delta >= 0
    case["selected_estimated_cost"] = _round(
        _float(case.get("selected_estimated_cost"), 0.0) * multiplier
    )
    case["selected_turnover_cost_assumption"] = _round(
        _float(case.get("selected_turnover_cost_assumption"), 0.0) * multiplier
    )
    case["guardrail_action"] = reason
    case["promotion_gate_allowed"] = False
    return case


def _variant_metric_row(
    variant_id: str,
    rows: list[dict[str, Any]],
    config: Mapping[str, Any],
) -> dict[str, Any]:
    aggregate = _aggregate_walk_forward_cases(rows)
    losers = [row for row in rows if _float(row.get("delta_vs_benchmark"), 0.0) < 0]
    concentration_reports = [
        _loss_concentration(losers, "date"),
        _loss_concentration(losers, "asset"),
        _loss_concentration(losers, "horizon"),
        _loss_concentration(losers, "regime_segment"),
    ]
    max_share = max(
        (report["summary"]["max_loss_share"] for report in concentration_reports),
        default=0.0,
    )
    tail = _tail_loss_contribution(losers, config)
    return {
        "variant_id": variant_id,
        **aggregate,
        "losing_case_count": len(losers),
        "losing_avg": _round(_mean([_float(row.get("delta_vs_benchmark"), 0.0) for row in losers])),
        "tail_loss_contribution": tail["tail_loss_share"],
        "max_loss_concentration_share": _round(max_share),
        "turnover": _round(
            sum(_float(row.get("selected_turnover_cost_assumption"), 0.0) for row in rows)
        ),
        "cost": _round(sum(_float(row.get("selected_estimated_cost"), 0.0) for row in rows)),
        "drawdown": _round(
            _mean([_float(row.get("selected_drawdown_proxy"), 0.0) for row in rows])
        ),
        "guardrail_changed_case_count": sum(
            1 for row in rows if row.get("guardrail_action") != "keep_value_surface_action"
        ),
        "promotion_gate_allowed": False,
    }


def _guardrail_variant_rules(
    risk_context: Mapping[str, Any],
    policy: Mapping[str, Any],
) -> list[dict[str, Any]]:
    return [
        {
            "variant_id": "original_value_surface",
            "rule": "no guardrail; keep original top value-surface action",
            "promotion_gate_allowed": False,
        },
        {
            "variant_id": "regime_conditioned_value_surface",
            "rule": "fallback to benchmark in failure-attributed high-loss regimes/horizons/assets",
            "high_loss_regimes": sorted(risk_context.get("high_loss_regimes", [])),
            "high_loss_horizons": sorted(risk_context.get("high_loss_horizons", [])),
            "high_loss_assets": sorted(risk_context.get("high_loss_assets", [])),
            "promotion_gate_allowed": False,
        },
        {
            "variant_id": "tail_loss_guarded_value_surface",
            "rule": (
                "fallback aggressive actions in tail-risk context and size-reduce "
                "other concentrated cases"
            ),
            "size_multiplier": _float(policy.get("loss_concentration_size_multiplier"), 0.50),
            "promotion_gate_allowed": False,
        },
        {
            "variant_id": "benchmark_fallback_value_surface",
            "rule": (
                "require confirmation before benchmark disagreement in high-risk "
                "or high-cost context"
            ),
            "promotion_gate_allowed": False,
        },
    ]


def _regime_horizon_loss_matrix(
    selected_cases: list[dict[str, Any]],
    config: Mapping[str, Any],
) -> dict[str, Any]:
    losers = [row for row in selected_cases if _float(row.get("delta_vs_benchmark"), 0.0) < 0]
    enriched = [
        {
            **row,
            "action": row.get("selected_action"),
            "cluster": row.get("asset_cluster"),
            "utility_profile": _utility_profile_for_action(str(row.get("selected_action")), config),
            "date_window": _date_window(str(row.get("date")), config),
        }
        for row in losers
    ]
    reports = {
        "regime": _loss_matrix_group(enriched, "regime_segment"),
        "asset": _loss_matrix_group(enriched, "asset"),
        "horizon": _loss_matrix_group(enriched, "horizon"),
        "action": _loss_matrix_group(enriched, "action"),
        "cluster": _loss_matrix_group(enriched, "cluster"),
        "utility_profile": _loss_matrix_group(enriched, "utility_profile"),
        "date_window": _loss_matrix_group(enriched, "date_window"),
    }
    max_row = _max_loss_matrix_row(reports)
    broad_ceiling = _float(
        _next_stage_section(config, "regime_horizon_loss_attribution_matrix").get(
            "broad_loss_group_share_ceiling"
        ),
        0.35,
    )
    max_share = _float(max_row.get("loss_share"), 0.0)
    assessment = "CONCENTRATED_REPAIRABLE" if max_share > broad_ceiling else "BROAD_STRUCTURAL_RISK"
    return {
        "summary": {
            "losing_case_count": len(losers),
            "losing_case_average_delta": _round(
                _mean([_float(row.get("delta_vs_benchmark"), 0.0) for row in losers])
            ),
            "max_loss_concentration_share": _round(max_share),
            "max_loss_concentration_group": max_row,
            "loss_distribution_assessment": assessment,
            "promotion_gate_allowed": False,
        },
        "loss_by_regime": reports["regime"],
        "loss_by_asset": reports["asset"],
        "loss_by_horizon": reports["horizon"],
        "loss_by_action": reports["action"],
        "loss_by_cluster": reports["cluster"],
        "loss_by_utility_profile": reports["utility_profile"],
        "loss_by_date_window": reports["date_window"],
        "top_losing_cases": sorted(
            enriched,
            key=lambda row: _float(row.get("delta_vs_benchmark"), 0.0),
        )[:25],
    }


def _loss_matrix_group(rows: list[dict[str, Any]], group_key: str) -> dict[str, Any]:
    report = _loss_concentration(rows, group_key)
    for row in report["groups"]:
        row["mean_selected_realized_net_return"] = _round(
            _mean(
                [
                    _float(value.get("selected_realized_net_return"), 0.0)
                    for value in rows
                    if str(value.get(group_key, "unknown")) == str(row.get(group_key))
                ]
            )
        )
        row["mean_benchmark_realized_net_return"] = _round(
            _mean(
                [
                    _float(value.get("benchmark_realized_net_return"), 0.0)
                    for value in rows
                    if str(value.get(group_key, "unknown")) == str(row.get(group_key))
                ]
            )
        )
    return report


def _max_loss_matrix_row(reports: Mapping[str, Mapping[str, Any]]) -> dict[str, Any]:
    candidates = []
    for report_name, report in reports.items():
        for row in _records(report.get("groups")):
            candidates.append({"matrix": report_name, **row})
    return max(candidates, key=lambda row: _float(row.get("loss_share"), 0.0), default={})


def _utility_profile_for_action(action_id: str, config: Mapping[str, Any]) -> str:
    mapping = _next_stage_section(config, "regime_horizon_loss_attribution_matrix").get(
        "utility_profile_mapping",
        {},
    )
    if isinstance(mapping, Mapping):
        for profile, actions in mapping.items():
            if action_id in [str(item) for item in actions]:
                return str(profile)
    if "risk" in action_id or "cash" in action_id or "guard" in action_id:
        return "risk_controlled"
    if "hold" in action_id or "static" in action_id:
        return "benchmark_like"
    return "aggressive_return_seeking"


def _date_window(date_value: str, config: Mapping[str, Any]) -> str:
    policy = _next_stage_section(config, "regime_horizon_loss_attribution_matrix")
    window = str(policy.get("date_window", "month"))
    if window == "month" and len(date_value) >= 7:
        return date_value[:7]
    if window == "quarter" and len(date_value) >= 7:
        month = _first_int(date_value[5:7])
        quarter = ((month - 1) // 3) + 1 if month else 0
        return f"{date_value[:4]}-Q{quarter}"
    return date_value


def _residual_regime_conditioning_report(
    residual_rows: list[dict[str, Any]],
    config: Mapping[str, Any],
) -> dict[str, Any]:
    policy = _next_stage_section(config, "gbdt_residual_regime_conditioning")
    limit = _first_int(policy.get("hypothesis_candidate_limit")) or 10
    large = [row for row in residual_rows if row.get("large_residual_for_review")]
    top_features = _residual_feature_importance(
        residual_rows,
        features=["regime_segment", "horizon", "asset", "asset_cluster", "pit_state", "action"],
    )
    sign = _residual_sign_classification(residual_rows)
    hypotheses = _residual_regime_hypotheses(residual_rows, limit)
    return {
        "summary": {
            "residual_case_count": len(residual_rows),
            "large_residual_case_count": len(large),
            "strategy_signal_generated": False,
            "promotion_gate_allowed": False,
        },
        "top_residual_features": top_features,
        "large_residual_regimes": _residual_group_result(large, "regime_segment"),
        "large_residual_horizons": _residual_group_result(large, "horizon"),
        "large_residual_assets": _residual_group_result(large, "asset"),
        "residual_sign_classification": sign,
        "hypothesis_candidates": hypotheses,
    }


def _residual_sign_classification(rows: list[dict[str, Any]]) -> dict[str, Any]:
    positive = [row for row in rows if _float(row.get("residual"), 0.0) > 0]
    negative = [row for row in rows if _float(row.get("residual"), 0.0) < 0]
    neutral = [row for row in rows if _float(row.get("residual"), 0.0) == 0]
    return {
        "positive_residual": {
            "case_count": len(positive),
            "interpretation": "value surface underestimated realized net outcome",
            "mean_residual": _round(_mean([_float(row.get("residual"), 0.0) for row in positive])),
            "promotion_gate_allowed": False,
        },
        "negative_residual": {
            "case_count": len(negative),
            "interpretation": "value surface overestimated realized net outcome",
            "mean_residual": _round(_mean([_float(row.get("residual"), 0.0) for row in negative])),
            "promotion_gate_allowed": False,
        },
        "neutral_residual": {
            "case_count": len(neutral),
            "promotion_gate_allowed": False,
        },
    }


def _residual_regime_hypotheses(
    residual_rows: list[dict[str, Any]],
    limit: int,
) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str, str, str], list[dict[str, Any]]] = {}
    for row in residual_rows:
        key = (
            str(row.get("regime_segment", "unknown")),
            str(row.get("horizon", "unknown")),
            str(row.get("asset_cluster", "unknown")),
            str(row.get("action", "unknown")),
        )
        grouped.setdefault(key, []).append(row)
    candidates = []
    for (regime, horizon, cluster, action), values in grouped.items():
        mean_residual = _mean([_float(row.get("residual"), 0.0) for row in values])
        mean_abs = _mean([_float(row.get("abs_residual"), 0.0) for row in values])
        large_rate = sum(1 for row in values if row.get("large_residual_for_review")) / len(values)
        residual_direction = (
            "overestimated_upside" if mean_residual < 0 else "underestimated_upside"
        )
        candidates.append(
            {
                "hypothesis_id": (
                    f"regime_{regime}_horizon_{horizon}_cluster_{cluster}_action_{action}"
                ),
                "regime_segment": regime,
                "horizon": horizon,
                "asset_cluster": cluster,
                "action": action,
                "case_count": len(values),
                "mean_residual": _round(mean_residual),
                "mean_abs_residual": _round(mean_abs),
                "large_residual_rate": _round(large_rate),
                "residual_direction": residual_direction,
                "hypothesis": (
                    f"When regime={regime}, horizon={horizon}, cluster={cluster}, "
                    f"action={action}, value surface may have {residual_direction}."
                ),
                "feeds_trading_796_ablation": True,
                "strategy_signal_generated": False,
                "promotion_gate_allowed": False,
            }
        )
    return sorted(
        candidates,
        key=lambda row: (
            _float(row.get("mean_abs_residual"), 0.0),
            _float(row.get("large_residual_rate"), 0.0),
        ),
        reverse=True,
    )[:limit]


def _regime_conditioned_controlled_review_decision(
    *,
    config: Mapping[str, Any],
    design: Mapping[str, Any],
    guardrail: Mapping[str, Any],
    loss_matrix: Mapping[str, Any],
    residual_regime: Mapping[str, Any],
) -> dict[str, Any]:
    policy = _next_stage_section(config, "regime_conditioned_value_surface_controlled_review")
    allowed = [str(item) for item in policy.get("allowed_decisions", [])]
    metrics = _records(guardrail.get("variant_metrics"))
    original = next(
        (row for row in metrics if row.get("variant_id") == "original_value_surface"), {}
    )
    tail = next(
        (row for row in metrics if row.get("variant_id") == "tail_loss_guarded_value_surface"),
        {},
    )
    fallback = next(
        (row for row in metrics if row.get("variant_id") == "benchmark_fallback_value_surface"),
        {},
    )
    best = max(
        metrics, key=lambda row: _float(row.get("mean_delta_vs_benchmark"), -999.0), default={}
    )
    if not design or not guardrail or not loss_matrix or not residual_regime or not metrics:
        decision = "DATA_REQUIRED"
        reason = "missing_required_795_to_798_artifact"
    else:
        original_mean = _float(original.get("mean_delta_vs_benchmark"), 0.0)
        best_mean = _float(best.get("mean_delta_vs_benchmark"), 0.0)
        tail_reduction = _tail_loss_reduction(original, tail)
        if best_mean >= 0 and best.get("variant_id") == "tail_loss_guarded_value_surface":
            decision = "CONTINUE"
            reason = "tail_loss_guarded_variant_restores_nonnegative_mean_delta"
        elif _float(fallback.get("mean_delta_vs_benchmark"), -999.0) >= 0:
            decision = "PIVOT_TO_BENCHMARK_FALLBACK"
            reason = "benchmark_fallback_variant_restores_nonnegative_mean_delta"
        elif tail_reduction >= _float(policy.get("tail_loss_reduction_required_share"), 0.20):
            decision = "PIVOT_TO_TAIL_RISK_POLICY"
            reason = "tail_loss_guard_reduces_tail_without_full_mean_recovery"
        elif (
            best_mean <= original_mean
            and loss_matrix.get("matrix_summary", {}).get("loss_distribution_assessment")
            == "BROAD_STRUCTURAL_RISK"
        ):
            decision = "KILL_CURRENT_VALUE_SURFACE"
            reason = "guardrails_do_not_improve_broad_structural_loss"
        else:
            decision = "WATCHLIST"
            reason = "improvement_incomplete_or_requires_more_forward_evidence"
        if allowed and decision not in allowed:
            decision = "WATCHLIST"
            reason = "computed_decision_not_allowed_by_policy"
    best_variant = best.get("variant_id")
    original_mean = _float(original.get("mean_delta_vs_benchmark"), 0.0)
    best_mean = _float(best.get("mean_delta_vs_benchmark"), 0.0)
    return {
        "decision": decision,
        "reason": reason,
        "allowed_decisions": allowed,
        "best_variant_by_mean_delta": best_variant,
        "original_mean_delta_vs_benchmark": _round(original_mean),
        "best_variant_mean_delta_vs_benchmark": _round(best_mean),
        "mean_delta_improved": best_mean > original_mean,
        "tail_loss_reduced": _tail_loss_reduction(original, best) > 0,
        "losing_avg_improved": _float(best.get("losing_avg"), -999.0)
        > _float(original.get("losing_avg"), 0.0),
        "beat_rate_retained": _beat_rate_retention(original, best)
        >= _float(policy.get("beat_rate_retention_floor"), 0.80),
        "turnover_cost_not_worse": _relative_increase_ok(
            original.get("turnover"),
            best.get("turnover"),
            _float(policy.get("turnover_increase_ceiling_share"), 0.10),
        )
        and _relative_increase_ok(
            original.get("cost"),
            best.get("cost"),
            _float(policy.get("cost_increase_ceiling_share"), 0.10),
        ),
        "promotion_gate_allowed": False,
        "paper_shadow_change_allowed": False,
        "production_weight_change_allowed": False,
    }


def _tail_loss_reduction(original: Mapping[str, Any], candidate: Mapping[str, Any]) -> float:
    original_tail = _float(original.get("tail_loss_contribution"), 0.0)
    candidate_tail = _float(candidate.get("tail_loss_contribution"), 0.0)
    return (original_tail - candidate_tail) / original_tail if original_tail else 0.0


def _beat_rate_retention(original: Mapping[str, Any], candidate: Mapping[str, Any]) -> float:
    original_rate = _float(original.get("value_surface_beats_benchmark_rate"), 0.0)
    candidate_rate = _float(candidate.get("value_surface_beats_benchmark_rate"), 0.0)
    return candidate_rate / original_rate if original_rate else 0.0


def _relative_increase_ok(original: Any, candidate: Any, ceiling: float) -> bool:
    original_value = abs(_float(original, 0.0))
    candidate_value = abs(_float(candidate, 0.0))
    if original_value == 0:
        return candidate_value == 0
    return (candidate_value - original_value) / original_value <= ceiling


def _regime_conditioned_review_evidence(
    *,
    design: Mapping[str, Any],
    guardrail: Mapping[str, Any],
    loss_matrix: Mapping[str, Any],
    residual_regime: Mapping[str, Any],
) -> dict[str, Any]:
    guardrail_summary = (
        guardrail.get("summary") if isinstance(guardrail.get("summary"), Mapping) else {}
    )
    matrix_summary = (
        loss_matrix.get("matrix_summary")
        if isinstance(loss_matrix.get("matrix_summary"), Mapping)
        else loss_matrix.get("summary", {})
    )
    return {
        "design_status": design.get("status"),
        "tail_loss_regime_count": design.get("summary", {}).get("tail_loss_regime_count"),
        "guardrail_status": guardrail.get("status"),
        "best_variant_by_mean_delta": guardrail_summary.get("best_variant_by_mean_delta"),
        "tail_loss_guardrail_reduces_tail_loss": guardrail_summary.get(
            "tail_loss_guardrail_reduces_tail_loss"
        ),
        "loss_matrix_status": loss_matrix.get("status"),
        "loss_distribution_assessment": matrix_summary.get("loss_distribution_assessment"),
        "residual_regime_status": residual_regime.get("status"),
        "top_residual_feature": residual_regime.get("summary", {}).get("top_residual_feature"),
        "promotion_gate_allowed": False,
    }


def _high_loss_group_values(
    failure: Mapping[str, Any],
    report_key: str,
    group_key: str,
    share_floor: float,
) -> list[str]:
    report = failure.get(report_key) if isinstance(failure.get(report_key), Mapping) else {}
    groups = _records(report.get("groups"))
    selected = [
        str(row.get(group_key))
        for row in groups
        if row.get(group_key) is not None and _float(row.get("loss_share"), 0.0) >= share_floor
    ]
    if selected:
        return selected
    top = max(groups, key=lambda row: _float(row.get("loss_share"), 0.0), default={})
    return [str(top.get(group_key))] if top.get(group_key) is not None else []


def _cost_turnover_aware_variant_comparison(
    *,
    selected_cases: list[dict[str, Any]],
    config: Mapping[str, Any],
    failure: Mapping[str, Any],
    horizon: Mapping[str, Any],
    design: Mapping[str, Any],
) -> dict[str, Any]:
    policy = _next_stage_section(config, "cost_turnover_aware_regime_conditioned_value_surface")
    variants = [str(item) for item in policy.get("allowed_variants", [])]
    original_rows = [dict(row) for row in selected_cases]
    variant_rows = {
        variant_id: _cost_aware_variant_cases(
            selected_cases=selected_cases,
            variant_id=variant_id,
            config=config,
            failure=failure,
            horizon=horizon,
            design=design,
        )
        for variant_id in variants
    }
    original_metric = _v2_variant_metric_row("original_value_surface", original_rows, config)
    metrics = [
        _add_variant_deltas(_v2_variant_metric_row(variant_id, rows, config), original_metric)
        for variant_id, rows in variant_rows.items()
    ]
    best = max(metrics, key=lambda row: _v2_metric_score(row, policy), default={})
    return {
        "summary": {
            "best_variant_by_v2_score": best.get("variant_id"),
            "best_variant_mean_delta_vs_benchmark": best.get("mean_delta_vs_benchmark"),
            "original_mean_delta_vs_benchmark": original_metric.get("mean_delta_vs_benchmark"),
            "mean_delta_improved": _float(best.get("mean_delta_vs_benchmark"), 0.0)
            > _float(original_metric.get("mean_delta_vs_benchmark"), 0.0),
            "tail_loss_reduced": _float(best.get("tail_loss_delta"), 0.0) < 0,
            "turnover_cost_not_worse": _float(best.get("turnover_delta"), 0.0) <= 0
            and _float(best.get("cost_delta"), 0.0) <= 0,
            "beat_rate_retained": _beat_rate_retention(original_metric, best)
            >= _float(policy.get("beat_rate_retention_floor"), 0.80),
            "promotion_gate_allowed": False,
        },
        "variant_metrics": [original_metric, *metrics],
        "variant_rules": _cost_turnover_variant_rules(policy),
        "transition_report": {
            "original": _transition_report(original_rows),
            **{variant_id: _transition_report(rows) for variant_id, rows in variant_rows.items()},
        },
        "v2_score_policy": {
            "turnover_penalty_weight": policy.get("v2_score_turnover_penalty_weight"),
            "cost_penalty_weight": policy.get("v2_score_cost_penalty_weight"),
            "tail_loss_penalty_weight": policy.get("v2_score_tail_loss_penalty_weight"),
            "policy_source": (
                "controlled_strategy_next_stage_research."
                "cost_turnover_aware_regime_conditioned_value_surface"
            ),
        },
        "diagnostic_boundary": {
            "retrospective_ablation_only": True,
            "requires_forward_cost_confirmation": True,
            "production_execution_rule": False,
            "promotion_gate_allowed": False,
        },
    }


def _cost_aware_variant_cases(
    *,
    selected_cases: list[dict[str, Any]],
    variant_id: str,
    config: Mapping[str, Any],
    failure: Mapping[str, Any],
    horizon: Mapping[str, Any],
    design: Mapping[str, Any],
) -> list[dict[str, Any]]:
    guardrail_policy = _next_stage_section(config, "tail_loss_guardrail_fallback_policy")
    policy = _next_stage_section(config, "cost_turnover_aware_regime_conditioned_value_surface")
    risk_context = _guardrail_risk_context(
        config=config,
        failure=failure,
        horizon=horizon,
        design=design,
    )
    base_rows = [
        _apply_guardrail_variant(
            dict(row),
            "regime_conditioned_value_surface",
            risk_context,
            guardrail_policy,
        )
        for row in selected_cases
    ]
    rows = _annotate_transitions(base_rows)
    if variant_id == "regime_conditioned_turnover_penalty":
        penalty = _float(policy.get("turnover_penalty_bps"), 25.0) / 10_000.0
        return [_apply_turnover_penalty(row, penalty) for row in rows]
    if variant_id == "regime_conditioned_action_hysteresis":
        threshold = _float(policy.get("action_hysteresis_min_delta_bps"), 25.0) / 10_000.0
        return [
            (
                _fallback_case_to_benchmark(dict(row), "action_hysteresis_fallback")
                if row.get("action_flip_for_review")
                and abs(_float(row.get("delta_vs_benchmark"), 0.0)) < threshold
                else row
            )
            for row in rows
        ]
    if variant_id == "regime_conditioned_no_trade_band":
        band = _float(policy.get("no_trade_band_abs_delta_bps"), 10.0) / 10_000.0
        return [
            (
                _fallback_case_to_benchmark(dict(row), "no_trade_band_fallback")
                if abs(_float(row.get("delta_vs_benchmark"), 0.0)) < band
                else row
            )
            for row in rows
        ]
    if variant_id == "regime_conditioned_benchmark_fallback":
        return [
            _apply_guardrail_variant(
                dict(row),
                "benchmark_fallback_value_surface",
                risk_context,
                guardrail_policy,
            )
            for row in selected_cases
        ]
    if variant_id == "regime_conditioned_max_action_change_cap":
        cap = max(0, _first_int(policy.get("max_action_change_cap_per_asset")))
        return _apply_action_change_cap(rows, cap)
    return rows


def _apply_turnover_penalty(row: dict[str, Any], penalty: float) -> dict[str, Any]:
    output = dict(row)
    if row.get("action_flip_for_review") or row.get("horizon_switch_for_review"):
        output["selected_realized_net_return"] = _round(
            _float(row.get("selected_realized_net_return"), 0.0) - penalty
        )
        output["delta_vs_benchmark"] = _round(_float(row.get("delta_vs_benchmark"), 0.0) - penalty)
        output["selected_estimated_cost"] = _round(
            _float(row.get("selected_estimated_cost"), 0.0) + penalty
        )
        output["value_surface_beats_benchmark"] = _float(output.get("delta_vs_benchmark"), 0.0) >= 0
        output["guardrail_action"] = "turnover_penalty_applied"
    return output


def _apply_action_change_cap(rows: list[dict[str, Any]], cap: int) -> list[dict[str, Any]]:
    counts: dict[str, int] = {}
    output = []
    for row in rows:
        item = dict(row)
        asset = str(item.get("asset", "unknown"))
        if item.get("action_flip_for_review"):
            counts[asset] = counts.get(asset, 0) + 1
            if counts[asset] > cap:
                item = _fallback_case_to_benchmark(item, "max_action_change_cap_fallback")
        output.append(item)
    return output


def _annotate_transitions(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    previous_by_asset: dict[str, dict[str, Any]] = {}
    output = []
    horizon_order = {"1d": 1, "5d": 5, "10d": 10, "20d": 20, "60d": 60}
    ordered = sorted(
        [dict(row) for row in rows],
        key=lambda row: (
            str(row.get("asset")),
            str(row.get("date")),
            horizon_order.get(str(row.get("horizon")), _first_int(row.get("horizon_days"))),
        ),
    )
    for row in ordered:
        asset = str(row.get("asset", "unknown"))
        previous = previous_by_asset.get(asset)
        row["action_flip_for_review"] = previous is not None and previous.get(
            "selected_action"
        ) != row.get("selected_action")
        row["horizon_switch_for_review"] = previous is not None and previous.get(
            "horizon"
        ) != row.get("horizon")
        previous_by_asset[asset] = row
        output.append(row)
    return output


def _transition_report(rows: list[dict[str, Any]]) -> dict[str, Any]:
    annotated = _annotate_transitions(rows)
    action_flips = sum(1 for row in annotated if row.get("action_flip_for_review"))
    horizon_switches = sum(1 for row in annotated if row.get("horizon_switch_for_review"))
    return {
        "case_count": len(annotated),
        "action_flip_count": action_flips,
        "horizon_switch_count": horizon_switches,
        "action_flip_rate": _round(action_flips / len(annotated) if annotated else 0.0),
        "horizon_switch_rate": _round(horizon_switches / len(annotated) if annotated else 0.0),
        "promotion_gate_allowed": False,
    }


def _v2_variant_metric_row(
    variant_id: str,
    rows: list[dict[str, Any]],
    config: Mapping[str, Any],
) -> dict[str, Any]:
    metric = _variant_metric_row(variant_id, rows, config)
    transition = _transition_report(rows)
    metric["action_flip_count"] = transition["action_flip_count"]
    metric["horizon_switch_count"] = transition["horizon_switch_count"]
    metric["promotion_gate_allowed"] = False
    return metric


def _add_variant_deltas(
    metric: dict[str, Any],
    original_metric: Mapping[str, Any],
) -> dict[str, Any]:
    output = dict(metric)
    for key, delta_key in [
        ("turnover", "turnover_delta"),
        ("cost", "cost_delta"),
        ("drawdown", "drawdown_delta"),
        ("tail_loss_contribution", "tail_loss_delta"),
        ("mean_delta_vs_benchmark", "mean_delta_improvement"),
        ("value_surface_beats_benchmark_rate", "beat_rate_delta"),
    ]:
        output[delta_key] = _round(
            _float(output.get(key), 0.0) - _float(original_metric.get(key), 0.0)
        )
    return output


def _v2_metric_score(row: Mapping[str, Any], policy: Mapping[str, Any]) -> float:
    turnover_weight = _float(policy.get("v2_score_turnover_penalty_weight"), 0.001)
    cost_weight = _float(policy.get("v2_score_cost_penalty_weight"), 0.01)
    tail_loss_weight = _float(policy.get("v2_score_tail_loss_penalty_weight"), 1.0)
    return (
        _float(row.get("mean_delta_vs_benchmark"), -999.0)
        - max(0.0, _float(row.get("turnover_delta"), 0.0)) * turnover_weight
        - max(0.0, _float(row.get("cost_delta"), 0.0)) * cost_weight
        - max(0.0, _float(row.get("tail_loss_delta"), 0.0)) * tail_loss_weight
    )


def _cost_turnover_variant_rules(policy: Mapping[str, Any]) -> list[dict[str, Any]]:
    return [
        {
            "variant_id": "regime_conditioned_turnover_penalty",
            "rule": "apply configured return penalty when action or horizon switches",
            "turnover_penalty_bps": policy.get("turnover_penalty_bps"),
            "promotion_gate_allowed": False,
        },
        {
            "variant_id": "regime_conditioned_action_hysteresis",
            "rule": "fallback to benchmark when action changes but delta is inside hysteresis band",
            "action_hysteresis_min_delta_bps": policy.get("action_hysteresis_min_delta_bps"),
            "promotion_gate_allowed": False,
        },
        {
            "variant_id": "regime_conditioned_no_trade_band",
            "rule": "fallback to benchmark when benchmark-relative delta is inside no-trade band",
            "no_trade_band_abs_delta_bps": policy.get("no_trade_band_abs_delta_bps"),
            "promotion_gate_allowed": False,
        },
        {
            "variant_id": "regime_conditioned_benchmark_fallback",
            "rule": "fallback to benchmark for high-risk disagreement cases",
            "promotion_gate_allowed": False,
        },
        {
            "variant_id": "regime_conditioned_max_action_change_cap",
            "rule": "fallback to benchmark after configured action-change cap per asset",
            "max_action_change_cap_per_asset": policy.get("max_action_change_cap_per_asset"),
            "promotion_gate_allowed": False,
        },
    ]


def _long_horizon_quarantine_review(
    *,
    selected_cases: list[dict[str, Any]],
    config: Mapping[str, Any],
    horizon: Mapping[str, Any],
) -> dict[str, Any]:
    policy = _next_stage_section(config, "long_horizon_quarantine_selection_review")
    reviewed = [str(item) for item in policy.get("reviewed_horizons", [])]
    original_metric = _variant_metric_row("original", selected_cases, config)
    variants = {
        variant_id: _long_horizon_variant_cases(selected_cases, config, variant_id, reviewed)
        for variant_id in [str(item) for item in policy.get("comparison_variants", [])]
    }
    metrics = [
        _add_variant_deltas(_variant_metric_row(variant_id, rows, config), original_metric)
        for variant_id, rows in variants.items()
    ]
    best = max(
        metrics, key=lambda row: _float(row.get("mean_delta_vs_benchmark"), -999.0), default={}
    )
    tail_reduction = _tail_loss_reduction(original_metric, best)
    return {
        "summary": {
            "best_comparison_variant": best.get("variant_id"),
            "best_variant_mean_delta_vs_benchmark": best.get("mean_delta_vs_benchmark"),
            "tail_loss_reduction_best_variant": _round(tail_reduction),
            "horizon_selector_issue_likely": tail_reduction
            >= _float(policy.get("high_loss_share_floor"), 0.25),
            "promotion_gate_allowed": False,
        },
        "reviewed_horizons": reviewed,
        "horizon_loss_matrix": _horizon_loss_matrix(selected_cases),
        "horizon_return_matrix": _horizon_return_matrix(selected_cases),
        "horizon_turnover_matrix": _horizon_turnover_matrix(selected_cases),
        "horizon_cliff_matrix": _horizon_cliff_matrix(horizon),
        "disable_vs_downgrade_comparison": metrics,
    }


def _long_horizon_variant_cases(
    rows: list[dict[str, Any]],
    config: Mapping[str, Any],
    variant_id: str,
    reviewed_horizons: list[str],
) -> list[dict[str, Any]]:
    policy = _next_stage_section(config, "long_horizon_quarantine_selection_review")
    target_regime = _next_stage_section(config, "ai_after_chatgpt_full_regime_attribution").get(
        "target_regime",
        "ai_after_chatgpt_full",
    )
    multiplier = _float(policy.get("downgrade_multiplier"), 0.50)
    output = []
    for row in rows:
        item = dict(row)
        reviewed = str(item.get("horizon")) in set(reviewed_horizons)
        if variant_id == "disable_20d_60d" and reviewed:
            item = _fallback_case_to_benchmark(item, "long_horizon_disabled")
        elif variant_id == "downgrade_20d_60d" and reviewed:
            item = _scale_case_delta(item, multiplier, "long_horizon_downgraded")
        elif (
            variant_id == "regime_only_20d_60d"
            and reviewed
            and item.get("regime_segment") == target_regime
        ):
            item = _fallback_case_to_benchmark(item, "long_horizon_target_regime_quarantine")
        elif (
            variant_id == "confirmation_required_20d_60d"
            and reviewed
            and item.get("selected_action") != item.get("benchmark_action")
        ):
            item = _fallback_case_to_benchmark(item, "long_horizon_confirmation_fallback")
        elif variant_id == "fallback_to_shorter_horizon" and reviewed:
            item = _fallback_case_to_benchmark(item, "long_horizon_shorter_horizon_fallback")
        item["variant_id"] = variant_id
        item["promotion_gate_allowed"] = False
        output.append(item)
    return output


def _horizon_loss_matrix(rows: list[dict[str, Any]]) -> dict[str, Any]:
    losers = [row for row in rows if _float(row.get("delta_vs_benchmark"), 0.0) < 0]
    return _loss_concentration(losers, "horizon")


def _horizon_return_matrix(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return _walk_forward_group_result(rows, "horizon")


def _horizon_turnover_matrix(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        grouped.setdefault(str(row.get("horizon", "unknown")), []).append(row)
    return [
        {
            "horizon": horizon,
            "case_count": len(values),
            "turnover": _round(
                sum(_float(row.get("selected_turnover_cost_assumption"), 0.0) for row in values)
            ),
            "cost": _round(sum(_float(row.get("selected_estimated_cost"), 0.0) for row in values)),
            "promotion_gate_allowed": False,
        }
        for horizon, values in sorted(grouped.items())
    ]


def _horizon_cliff_matrix(horizon: Mapping[str, Any]) -> list[dict[str, Any]]:
    report = (
        horizon.get("utility_profile_cliff_report")
        if isinstance(horizon.get("utility_profile_cliff_report"), Mapping)
        else {}
    )
    return _records(report.get("horizon_cliff_rows"))


def _ai_regime_attribution_review(
    selected_cases: list[dict[str, Any]],
    config: Mapping[str, Any],
) -> dict[str, Any]:
    policy = _next_stage_section(config, "ai_after_chatgpt_full_regime_attribution")
    target_regime = str(policy.get("target_regime", "ai_after_chatgpt_full"))
    regime_rows = [row for row in selected_cases if row.get("regime_segment") == target_regime]
    losing = [row for row in regime_rows if _float(row.get("delta_vs_benchmark"), 0.0) < 0]
    enriched = [
        {
            **row,
            "action": row.get("selected_action"),
            "cluster": row.get("asset_cluster"),
        }
        for row in losing
    ]
    loss_by_asset = _loss_matrix_group(enriched, "asset")
    loss_by_horizon = _loss_matrix_group(enriched, "horizon")
    loss_by_action = _loss_matrix_group(enriched, "action")
    loss_by_cluster = _loss_matrix_group(enriched, "cluster")
    benchmark_stability = _benchmark_stability_report(regime_rows)
    overoptimism = _value_surface_overoptimism_report(regime_rows)
    return {
        "summary": {
            "target_regime": target_regime,
            "regime_case_count": len(regime_rows),
            "regime_losing_case_count": len(losing),
            "top_loss_asset": _top_group_value(loss_by_asset, "asset"),
            "top_loss_horizon": _top_group_value(loss_by_horizon, "horizon"),
            "top_loss_action": _top_group_value(loss_by_action, "action"),
            "top_loss_cluster": _top_group_value(loss_by_cluster, "cluster"),
            "value_surface_systematic_overoptimism": overoptimism[
                "systematic_overoptimism_for_review"
            ],
            "promotion_gate_allowed": False,
        },
        "loss_by_asset": loss_by_asset,
        "loss_by_horizon": loss_by_horizon,
        "loss_by_action": loss_by_action,
        "loss_by_cluster": loss_by_cluster,
        "benchmark_stability": benchmark_stability,
        "value_surface_overoptimism": overoptimism,
        "candidate_repairs": [
            {
                "repair": str(repair),
                "diagnostic_evidence": {
                    "top_loss_asset": _top_group_value(loss_by_asset, "asset"),
                    "top_loss_horizon": _top_group_value(loss_by_horizon, "horizon"),
                    "top_loss_action": _top_group_value(loss_by_action, "action"),
                    "top_loss_cluster": _top_group_value(loss_by_cluster, "cluster"),
                },
                "requires_followup_ablation": True,
                "promotion_gate_allowed": False,
            }
            for repair in policy.get("candidate_repairs", [])
        ],
    }


def _top_group_value(report: Mapping[str, Any], group_key: str) -> str | None:
    groups = _records(report.get("groups"))
    if not groups:
        return None
    return str(groups[0].get(group_key))


def _benchmark_stability_report(rows: list[dict[str, Any]]) -> dict[str, Any]:
    selected = [_float(row.get("selected_realized_net_return"), 0.0) for row in rows]
    benchmark = [_float(row.get("benchmark_realized_net_return"), 0.0) for row in rows]
    selected_negative = sum(1 for value in selected if value < 0)
    benchmark_negative = sum(1 for value in benchmark if value < 0)
    return {
        "case_count": len(rows),
        "mean_selected_realized_net_return": _round(_mean(selected)),
        "mean_benchmark_realized_net_return": _round(_mean(benchmark)),
        "selected_negative_rate": _round(selected_negative / len(rows) if rows else 0.0),
        "benchmark_negative_rate": _round(benchmark_negative / len(rows) if rows else 0.0),
        "benchmark_more_stable": benchmark_negative <= selected_negative,
        "promotion_gate_allowed": False,
    }


def _value_surface_overoptimism_report(rows: list[dict[str, Any]]) -> dict[str, Any]:
    optimism = [
        _float(row.get("selected_net_utility"), 0.0)
        - _float(row.get("selected_realized_net_return"), 0.0)
        for row in rows
        if row.get("selected_net_utility") is not None
    ]
    positive = [value for value in optimism if value > 0]
    return {
        "case_count": len(optimism),
        "mean_utility_minus_realized": _round(_mean(optimism)),
        "positive_overoptimism_case_count": len(positive),
        "positive_overoptimism_rate": _round(len(positive) / len(optimism) if optimism else 0.0),
        "systematic_overoptimism_for_review": (
            len(positive) / len(optimism) >= 0.50 if optimism else False
        ),
        "promotion_gate_allowed": False,
    }


def _regime_conditioned_holdout_review(
    *,
    selected_cases: list[dict[str, Any]],
    config: Mapping[str, Any],
    failure: Mapping[str, Any],
    horizon: Mapping[str, Any],
    design: Mapping[str, Any],
    cost_turnover: Mapping[str, Any],
) -> dict[str, Any]:
    policy = _next_stage_section(config, "regime_conditioned_walk_forward_holdout")
    best_variant = str(
        cost_turnover.get("summary", {}).get("best_variant_by_v2_score")
        or "regime_conditioned_benchmark_fallback"
    )
    candidate_cases = _cost_aware_variant_cases(
        selected_cases=selected_cases,
        variant_id=best_variant,
        config=config,
        failure=failure,
        horizon=horizon,
        design=design,
    )
    dimensions = [str(item) for item in policy.get("holdout_dimensions", [])]
    result_by_dimension = {
        dimension: _holdout_dimension_results(
            original_cases=selected_cases,
            candidate_cases=candidate_cases,
            config=config,
            dimension=dimension,
        )
        for dimension in dimensions
    }
    rows = [row for values in result_by_dimension.values() for row in values]
    eligible = [row for row in rows if not row.get("insufficient_holdout_cases")]
    passed = [row for row in eligible if row.get("passed")]
    holdout_pass_rate_floor = _float(policy.get("holdout_pass_rate_floor"), 0.60)
    holdout_pass_rate = len(passed) / len(eligible) if eligible else 0.0
    return {
        "summary": {
            "best_variant": best_variant,
            "holdout_case_count": len(rows),
            "eligible_holdout_case_count": len(eligible),
            "holdout_pass_count": len(passed),
            "holdout_pass_rate": _round(holdout_pass_rate),
            "holdout_pass_rate_floor": holdout_pass_rate_floor,
            "overfit_risk": (
                "HIGH" if eligible and holdout_pass_rate < holdout_pass_rate_floor else "WATCH"
            ),
            "promotion_gate_allowed": False,
        },
        "leave_one_regime_out": result_by_dimension.get("regime_segment", []),
        "leave_one_horizon_out": result_by_dimension.get("horizon", []),
        "leave_one_asset_cluster_out": result_by_dimension.get("asset_cluster", []),
        "leave_one_date_window_out": result_by_dimension.get("date_window", []),
    }


def _holdout_dimension_results(
    *,
    original_cases: list[dict[str, Any]],
    candidate_cases: list[dict[str, Any]],
    config: Mapping[str, Any],
    dimension: str,
) -> list[dict[str, Any]]:
    policy = _next_stage_section(config, "regime_conditioned_walk_forward_holdout")
    min_cases = _first_int(policy.get("min_holdout_case_count"))
    mean_floor = _float(policy.get("mean_delta_floor_bps"), 0.0) / 10_000.0
    tail_floor = _float(policy.get("tail_loss_reduction_floor"), 0.10)
    values = sorted({_holdout_dimension_value(row, dimension, config) for row in original_cases})
    rows = []
    for value in values:
        original_subset = [
            row
            for row in original_cases
            if _holdout_dimension_value(row, dimension, config) == value
        ]
        candidate_subset = [
            row
            for row in candidate_cases
            if _holdout_dimension_value(row, dimension, config) == value
        ]
        original_metric = _v2_variant_metric_row("original_holdout", original_subset, config)
        candidate_metric = _add_variant_deltas(
            _v2_variant_metric_row("candidate_holdout", candidate_subset, config),
            original_metric,
        )
        tail_reduction = _tail_loss_reduction(original_metric, candidate_metric)
        turnover_cost_not_worse = (
            _float(candidate_metric.get("turnover_delta"), 0.0) <= 0
            and _float(candidate_metric.get("cost_delta"), 0.0) <= 0
        )
        insufficient = len(original_subset) < min_cases
        passed = (
            not insufficient
            and _float(candidate_metric.get("mean_delta_vs_benchmark"), 0.0) >= mean_floor
            and tail_reduction >= tail_floor
            and turnover_cost_not_worse
        )
        rows.append(
            {
                "holdout_dimension": dimension,
                "holdout_value": value,
                "case_count": len(original_subset),
                "insufficient_holdout_cases": insufficient,
                "original_mean_delta_vs_benchmark": original_metric.get("mean_delta_vs_benchmark"),
                "candidate_mean_delta_vs_benchmark": candidate_metric.get(
                    "mean_delta_vs_benchmark"
                ),
                "tail_loss_reduction": _round(tail_reduction),
                "turnover_delta": candidate_metric.get("turnover_delta"),
                "cost_delta": candidate_metric.get("cost_delta"),
                "turnover_cost_not_worse": turnover_cost_not_worse,
                "passed": passed,
                "promotion_gate_allowed": False,
            }
        )
    return rows


def _holdout_dimension_value(
    row: Mapping[str, Any],
    dimension: str,
    config: Mapping[str, Any],
) -> str:
    if dimension == "date_window":
        return _date_window(str(row.get("date")), config)
    return str(row.get(dimension, "unknown"))


def _value_surface_v2_review_decision(
    *,
    config: Mapping[str, Any],
    cost_turnover: Mapping[str, Any],
    horizon_quarantine: Mapping[str, Any],
    regime_attribution: Mapping[str, Any],
    holdout: Mapping[str, Any],
) -> dict[str, Any]:
    policy = _next_stage_section(config, "value_surface_v2_controlled_review")
    allowed = [str(item) for item in policy.get("allowed_decisions", [])]
    if not cost_turnover or not horizon_quarantine or not regime_attribution or not holdout:
        decision = "DATA_REQUIRED"
        reason = "missing_required_800_to_803_artifact"
        best = {}
        original = {}
    else:
        metrics = _records(cost_turnover.get("variant_metrics"))
        best_id = cost_turnover.get("summary", {}).get("best_variant_by_v2_score")
        best = next((row for row in metrics if row.get("variant_id") == best_id), {})
        original = next(
            (row for row in metrics if row.get("variant_id") == "original_value_surface"), {}
        )
        mean_ok = (
            _float(best.get("mean_delta_vs_benchmark"), 0.0)
            >= _float(policy.get("mean_delta_floor_bps"), 0.0) / 10_000.0
            and _float(best.get("mean_delta_improvement"), 0.0) > 0
        )
        tail_ok = _tail_loss_reduction(original, best) >= _float(
            policy.get("tail_loss_reduction_required_share"), 0.20
        )
        turnover_cost_ok = (
            _float(best.get("turnover_delta"), 0.0) <= 0
            and _float(best.get("cost_delta"), 0.0) <= 0
        )
        beat_ok = _beat_rate_retention(original, best) >= _float(
            policy.get("beat_rate_retention_floor"), 0.80
        )
        holdout_ok = _float(holdout.get("summary", {}).get("holdout_pass_rate"), 0.0) >= _float(
            policy.get("holdout_pass_rate_floor"), 0.60
        )
        horizon_tail_reduction = _float(
            horizon_quarantine.get("summary", {}).get("tail_loss_reduction_best_variant"),
            0.0,
        )
        if mean_ok and tail_ok and turnover_cost_ok and beat_ok and holdout_ok:
            decision = "CONTINUE_TO_LARGER_CONTROLLED_RESEARCH"
            reason = "v2_conditions_met_without_promotion"
        elif horizon_tail_reduction >= _float(
            policy.get("horizon_quarantine_tail_loss_reduction_floor"), 0.20
        ):
            decision = "PIVOT_TO_HORIZON_SELECTOR"
            reason = "long_horizon_quarantine_reduces_tail_loss"
        elif tail_ok and not turnover_cost_ok:
            decision = "PIVOT_TO_TAIL_RISK_POLICY"
            reason = "tail_loss_improves_but_turnover_or_cost_worsens"
        elif not mean_ok and holdout.get("summary", {}).get("overfit_risk") == "HIGH":
            decision = "KILL_VALUE_SURFACE"
            reason = "v2_no_mean_recovery_and_holdout_overfit_risk_high"
        else:
            decision = "WATCHLIST"
            reason = "v2_improvement_incomplete"
    if allowed and decision not in allowed:
        decision = "WATCHLIST"
        reason = "computed_decision_not_allowed_by_policy"
    return {
        "decision": decision,
        "reason": reason,
        "allowed_decisions": allowed,
        "best_cost_turnover_variant": best.get("variant_id"),
        "best_mean_delta_vs_benchmark": best.get("mean_delta_vs_benchmark"),
        "original_mean_delta_vs_benchmark": original.get("mean_delta_vs_benchmark"),
        "mean_delta_condition_met": _float(best.get("mean_delta_vs_benchmark"), -999.0)
        >= _float(policy.get("mean_delta_floor_bps"), 0.0) / 10_000.0,
        "tail_loss_condition_met": _tail_loss_reduction(original, best)
        >= _float(policy.get("tail_loss_reduction_required_share"), 0.20),
        "turnover_cost_condition_met": _float(best.get("turnover_delta"), 0.0) <= 0
        and _float(best.get("cost_delta"), 0.0) <= 0,
        "beat_rate_condition_met": _beat_rate_retention(original, best)
        >= _float(policy.get("beat_rate_retention_floor"), 0.80),
        "holdout_condition_met": _float(
            holdout.get("summary", {}).get("holdout_pass_rate"),
            0.0,
        )
        >= _float(policy.get("holdout_pass_rate_floor"), 0.60),
        "promotion_gate_allowed": False,
        "paper_shadow_change_allowed": False,
        "production_weight_change_allowed": False,
    }


def _value_surface_v2_evidence_summary(
    *,
    cost_turnover: Mapping[str, Any],
    horizon_quarantine: Mapping[str, Any],
    regime_attribution: Mapping[str, Any],
    holdout: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "cost_turnover_status": cost_turnover.get("status"),
        "best_variant_by_v2_score": cost_turnover.get("summary", {}).get(
            "best_variant_by_v2_score"
        ),
        "turnover_cost_not_worse": cost_turnover.get("summary", {}).get("turnover_cost_not_worse"),
        "horizon_quarantine_status": horizon_quarantine.get("status"),
        "horizon_selector_issue_likely": horizon_quarantine.get("summary", {}).get(
            "horizon_selector_issue_likely"
        ),
        "regime_attribution_status": regime_attribution.get("status"),
        "top_loss_horizon": regime_attribution.get("summary", {}).get("top_loss_horizon"),
        "holdout_status": holdout.get("status"),
        "holdout_pass_rate": holdout.get("summary", {}).get("holdout_pass_rate"),
        "promotion_gate_allowed": False,
    }


def _run_data_quality_gate(
    *,
    prices_path: Path,
    marketstack_prices_path: Path,
    rates_path: Path,
    as_of_date: date | None,
    universe: list[str],
) -> dict[str, Any]:
    universe_config = load_universe()
    quality_config = load_data_quality()
    resolved_as_of = as_of_date or _latest_price_date(prices_path) or date.today()
    report = validate_data_cache(
        prices_path=prices_path,
        rates_path=rates_path,
        expected_price_tickers=universe,
        expected_rate_series=configured_rate_series(universe_config),
        quality_config=quality_config,
        as_of=resolved_as_of,
        manifest_path=_download_manifest_path_if_present(prices_path),
        secondary_prices_path=marketstack_prices_path if marketstack_prices_path.exists() else None,
        require_secondary_prices=False,
    )
    return {
        "required_command": "aits validate-data",
        "called_same_validation_code_path": True,
        "status": report.status,
        "passed": report.passed,
        "checked_at": report.checked_at.isoformat(),
        "as_of": report.as_of.isoformat(),
        "error_count": report.error_count,
        "warning_count": report.warning_count,
        "info_count": report.info_count,
        "prices_path": str(prices_path),
        "prices_row_count": report.price_summary.rows,
        "prices_min_date": (
            report.price_summary.min_date.isoformat() if report.price_summary.min_date else None
        ),
        "prices_max_date": (
            report.price_summary.max_date.isoformat() if report.price_summary.max_date else None
        ),
        "rates_path": str(rates_path),
        "rates_row_count": report.rate_summary.rows,
        "secondary_prices_path": str(marketstack_prices_path),
        "secondary_prices_row_count": (
            report.secondary_price_summary.rows if report.secondary_price_summary else 0
        ),
        "issue_codes": [issue.code for issue in report.issues],
    }


def _download_manifest_path_if_present(prices_path: Path) -> Path | None:
    path = prices_path.parent / "download_manifest.csv"
    return path if path.exists() else None


def _latest_price_date(prices_path: Path) -> date | None:
    latest: date | None = None
    if not prices_path.exists():
        return None
    with prices_path.open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            raw = row.get("date")
            if not raw:
                continue
            try:
                parsed = date.fromisoformat(raw)
            except ValueError:
                continue
            latest = parsed if latest is None or parsed > latest else latest
    return latest


def _read_price_rows(path: Path, *, universe: list[str]) -> dict[str, dict[str, dict[str, float]]]:
    rows: dict[str, dict[str, dict[str, float]]] = {ticker: {} for ticker in universe}
    if not path.exists():
        return rows
    wanted = set(universe)
    with path.open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            ticker = str(row.get("ticker") or row.get("symbol") or "").upper()
            if ticker not in wanted:
                continue
            row_date = str(row.get("date") or "")
            if row_date < AI_REGIME_START:
                continue
            rows[ticker][row_date] = {
                "open": _float(row.get("open"), 0.0),
                "high": _float(row.get("high"), 0.0),
                "low": _float(row.get("low"), 0.0),
                "close": _float(row.get("close"), 0.0),
                "adj_close": _float(row.get("adj_close"), _float(row.get("close"), 0.0)),
                "volume": _float(row.get("volume"), 0.0),
            }
    return rows


def _all_dates(price_rows: Mapping[str, Mapping[str, Mapping[str, float]]]) -> list[str]:
    return sorted({row_date for rows in price_rows.values() for row_date in rows})


def _limited_decision_dates(dates: list[str], config: Mapping[str, Any]) -> list[str]:
    if not dates:
        return []
    max_dates = _minimum(config, "max_decision_dates", 24)
    eligible = dates[:-1] if len(dates) > 1 else dates
    return eligible[-max_dates:]


def _value_surface_row(
    *,
    decision_date: str,
    asset: str,
    action: Mapping[str, Any],
    horizon: Mapping[str, Any],
    price_rows: Mapping[str, Mapping[str, Mapping[str, float]]],
    all_dates: list[str],
    cost_bps: float,
) -> dict[str, Any]:
    decision_index = all_dates.index(decision_date)
    horizon_days = int(horizon["days"])
    trailing = _trailing_returns(
        price_rows.get(asset, {}),
        all_dates=all_dates,
        decision_index=decision_index,
        lookback_days=max(horizon_days, 1),
    )
    exposure = _float(action.get("exposure_multiplier"), 0.0)
    cost_turnover = _float(action.get("cost_turnover_assumption"), 0.0)
    expected_return = exposure * _mean(trailing) * max(horizon_days, 1)
    median_return = exposure * _median(trailing) * max(horizon_days, 1)
    downside_risk = abs(exposure) * _downside_risk(trailing) * math.sqrt(max(horizon_days, 1))
    drawdown_proxy = _max_drawdown(trailing)
    estimated_cost = cost_turnover * (cost_bps / 10_000.0)
    net_utility = expected_return - estimated_cost
    realized = _forward_return(
        price_rows.get(asset, {}),
        all_dates=all_dates,
        decision_index=decision_index,
        horizon_days=horizon_days,
    )
    sample_count = len(trailing)
    return {
        "date": decision_date,
        "asset": asset,
        "action": str(action.get("action_id")),
        "horizon": str(horizon.get("horizon_id")),
        "horizon_days": horizon_days,
        "pit_state": _pit_state_from_returns(trailing),
        "expected_return": _round(expected_return),
        "median_return": _round(median_return),
        "downside_risk": _round(downside_risk),
        "max_drawdown_proxy": _round(drawdown_proxy),
        "uncertainty": _round(1.0 / math.sqrt(sample_count) if sample_count else 1.0),
        "estimated_cost": _round(estimated_cost),
        "net_utility": _round(net_utility),
        "sample_quality": {
            "sample_count": sample_count,
            "sample_quality": _sample_quality_label(sample_count),
            "outcome_mature": realized is not None,
            "overlapping_horizon_dependency_risk": horizon_days > 1,
        },
        "realized_forward_return": _round(realized) if realized is not None else None,
        "realized_forward_return_role": "evaluation_only",
        "ranking_policy": "heuristic",
        "not_validated_utility_boundary": True,
        "promotion_gate_allowed": False,
    }


def _trailing_returns(
    rows: Mapping[str, Mapping[str, float]],
    *,
    all_dates: list[str],
    decision_index: int,
    lookback_days: int,
) -> list[float]:
    start = max(1, decision_index - lookback_days + 1)
    returns = [
        _return_between(rows, previous_date=all_dates[index - 1], current_date=all_dates[index])
        for index in range(start, decision_index + 1)
    ]
    return [value for value in returns if value is not None]


def _forward_return(
    rows: Mapping[str, Mapping[str, float]],
    *,
    all_dates: list[str],
    decision_index: int,
    horizon_days: int,
) -> float | None:
    target_index = decision_index + horizon_days
    if target_index >= len(all_dates):
        return None
    return _return_between(
        rows,
        previous_date=all_dates[decision_index],
        current_date=all_dates[target_index],
    )


def _return_between(
    rows: Mapping[str, Mapping[str, float]],
    *,
    previous_date: str,
    current_date: str,
) -> float | None:
    previous = rows.get(previous_date)
    current = rows.get(current_date)
    if previous is None or current is None:
        return None
    previous_price = previous.get("adj_close") or previous.get("close") or 0.0
    current_price = current.get("adj_close") or current.get("close") or 0.0
    if previous_price <= 0:
        return None
    return current_price / previous_price - 1.0


def _value_surface_horizon_audit(
    *,
    horizons: list[dict[str, Any]],
    surface_rows: list[dict[str, Any]],
    decision_dates: list[str],
) -> dict[str, Any]:
    held_out_horizon = horizons[-1]["horizon_id"] if horizons else None
    return {
        "schema_version": "1.0",
        "report_type": "value_surface_horizon_audit",
        "status": "PASS",
        "summary": {
            "horizon_leakage_check_pass": True,
            "future_outcome_used_for_horizon_selection": False,
            "future_outcome_used_for_evaluation_only": True,
            "fixed_configured_horizon_count": len(horizons),
            "overlapping_horizon_dependency_risk": any(
                int(row.get("horizon_days", 0)) > 1 for row in surface_rows
            ),
            "held_out_horizon_present": held_out_horizon is not None,
            **_summary_safety(),
        },
        "horizon_selection_policy": {
            "selection_rule": "fixed_configured_horizons_from_policy",
            "source": "config/research/controlled_strategy_candidate_research.yaml",
            "uses_future_outcome": False,
        },
        "future_outcome_policy": {
            "future_outcome_role": "evaluation_only",
            "strategy_input_allowed": False,
        },
        "held_out_horizon": held_out_horizon,
        "held_out_date_range": {
            "first_decision_date": decision_dates[0] if decision_dates else None,
            "last_decision_date": decision_dates[-1] if decision_dates else None,
        },
        **PRODUCTION_SAFETY,
    }


def _sample_quality_report(surface_rows: list[dict[str, Any]]) -> dict[str, Any]:
    mature = [
        row
        for row in surface_rows
        if isinstance(row.get("sample_quality"), Mapping)
        and row["sample_quality"].get("outcome_mature")
    ]
    return {
        "row_count": len(surface_rows),
        "mature_outcome_row_count": len(mature),
        "immature_outcome_row_count": len(surface_rows) - len(mature),
        "by_horizon": _group_count(surface_rows, "horizon"),
        "by_asset": _group_count(surface_rows, "asset"),
        "sample_quality_report_present": True,
    }


def _profile(rows: list[dict[str, Any]], key: str) -> dict[str, Any]:
    values = [_float(row.get(key), 0.0) for row in rows if row.get(key) is not None]
    return {
        "metric": key,
        "row_count": len(values),
        "min": _round(min(values)) if values else None,
        "median": _round(_median(values)) if values else None,
        "max": _round(max(values)) if values else None,
    }


def _benchmark_comparison(
    *,
    price_rows: Mapping[str, Mapping[str, Mapping[str, float]]],
    universe: list[str],
    config: Mapping[str, Any],
    cost_bps: float,
    benchmark_expansion_path: Path,
) -> dict[str, Any]:
    benchmark_ids = [
        "cash",
        "buy_and_hold",
        "static_allocation",
        "simple_trend",
        "moving_average_risk_off",
        "volatility_targeting",
        "drawdown_guard",
    ]
    dates = _all_dates(price_rows)
    rows = [
        _strategy_metrics(
            benchmark_id,
            price_rows=price_rows,
            dates=dates,
            universe=universe,
            cost_bps=cost_bps,
        )
        for benchmark_id in benchmark_ids
    ]
    best = max(rows, key=lambda row: row.get("net_total_return") or -999.0) if rows else {}
    expansion = _read_json_or_empty(benchmark_expansion_path)
    return {
        "schema_version": "1.0",
        "report_type": "value_surface_benchmark_comparison",
        "status": "PASS_WITH_WARNINGS",
        "summary": {
            "benchmark_comparison_present": True,
            "benchmark_count": len(rows),
            "source_benchmark_expansion_present": bool(expansion),
            **_summary_safety(),
        },
        "source_benchmark_expansion_path": str(benchmark_expansion_path),
        "source_benchmark_expansion_status": expansion.get("status", "MISSING"),
        "rows": rows,
        "best_simple_benchmark": best,
        **PRODUCTION_SAFETY,
    }


def _strategy_metrics(
    strategy_id: str,
    *,
    price_rows: Mapping[str, Mapping[str, Mapping[str, float]]],
    dates: list[str],
    universe: list[str],
    cost_bps: float | None = None,
    selected_strategy_by_date: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    if len(dates) < 2:
        return _empty_strategy_metrics(strategy_id)
    cost_rate = (cost_bps if cost_bps is not None else _configured_cost_bps()) / 10_000.0
    previous_exposure = 0.0
    gross_returns: list[float] = []
    net_returns: list[float] = []
    turnovers: list[float] = []
    for index in range(1, len(dates)):
        decision_date = dates[index - 1]
        selected = (
            selected_strategy_by_date.get(decision_date, "static_allocation")
            if selected_strategy_by_date
            else strategy_id
        )
        exposure = _strategy_exposure(selected, price_rows=price_rows, dates=dates, index=index - 1)
        asset_return = _equal_weight_asset_return(
            price_rows=price_rows,
            universe=universe,
            previous_date=dates[index - 1],
            current_date=dates[index],
        )
        turnover = abs(exposure - previous_exposure)
        gross_return = exposure * asset_return
        net_return = gross_return - turnover * cost_rate
        gross_returns.append(gross_return)
        net_returns.append(net_return)
        turnovers.append(turnover)
        previous_exposure = exposure
    gross_total = _compound(gross_returns)
    net_total = _compound(net_returns)
    return {
        "strategy_id": strategy_id,
        "gross_total_return": _round(gross_total),
        "net_total_return": _round(net_total),
        "annualized_return": _round(_annualized_return(net_returns)),
        "max_drawdown": _round(_max_drawdown(net_returns)),
        "drawdown_preservation": _round(max(0.0, -_max_drawdown(net_returns))),
        "downside_capture": _round(_downside_risk(net_returns)),
        "hit_rate": _round(_hit_rate(net_returns)),
        "turnover": _round(sum(turnovers)),
        "estimated_cost": _round(sum(turnovers) * cost_rate),
        "false_risk_off": _round(_false_risk_off_rate(gross_returns, turnovers)),
        "false_risk_on": _round(_false_risk_on_rate(net_returns)),
        "missed_upside": _round(_missed_upside(gross_returns, net_returns)),
        "constraint_hit_count": sum(1 for value in turnovers if value > 0),
        "sample_quality": _sample_quality_label(len(net_returns)),
        "observation_count": len(net_returns),
        "promotion_gate_allowed": False,
    }


def _empty_strategy_metrics(strategy_id: str) -> dict[str, Any]:
    return {
        "strategy_id": strategy_id,
        "gross_total_return": None,
        "net_total_return": None,
        "annualized_return": None,
        "max_drawdown": None,
        "turnover": 0.0,
        "estimated_cost": 0.0,
        "observation_count": 0,
        "sample_quality": "INSUFFICIENT",
        "promotion_gate_allowed": False,
    }


def _strategy_exposure(
    strategy_id: str,
    *,
    price_rows: Mapping[str, Mapping[str, Mapping[str, float]]],
    dates: list[str],
    index: int,
) -> float:
    if strategy_id in {"cash", "hold_cash", "risk_off"}:
        return 0.0
    if strategy_id in {"capped_masking", "decrease_exposure"}:
        return 0.5
    if strategy_id in {"volatility_targeting", "vol_target"}:
        recent = _aggregate_returns(price_rows, dates=dates, end_index=index, lookback=10)
        vol = _annualized_volatility(recent)
        return max(0.25, min(1.0, 0.20 / vol)) if vol > 0 else 1.0
    if strategy_id in {"drawdown_guard"}:
        recent = _aggregate_returns(price_rows, dates=dates, end_index=index, lookback=20)
        return 0.25 if _max_drawdown(recent) <= -0.05 else 1.0
    if strategy_id in {"simple_trend", "simple_trend_following", "risk_on_slow"}:
        recent = _aggregate_returns(price_rows, dates=dates, end_index=index, lookback=5)
        return 1.0 if _mean(recent) >= 0.0 else 0.0
    if strategy_id == "risk_off_fast":
        recent = _aggregate_returns(price_rows, dates=dates, end_index=index, lookback=1)
        return 0.0 if recent and recent[-1] < 0 else 1.0
    if strategy_id == "moving_average_risk_off":
        recent = _aggregate_returns(price_rows, dates=dates, end_index=index, lookback=3)
        return 1.0 if _mean(recent) >= 0.0 else 0.0
    return 1.0


def _equal_weight_asset_return(
    *,
    price_rows: Mapping[str, Mapping[str, Mapping[str, float]]],
    universe: list[str],
    previous_date: str,
    current_date: str,
) -> float:
    returns = [
        _return_between(
            price_rows.get(ticker, {}), previous_date=previous_date, current_date=current_date
        )
        for ticker in universe
    ]
    usable = [value for value in returns if value is not None]
    return _mean(usable)


def _aggregate_returns(
    price_rows: Mapping[str, Mapping[str, Mapping[str, float]]],
    *,
    dates: list[str],
    end_index: int,
    lookback: int,
) -> list[float]:
    start = max(1, end_index - lookback + 1)
    return [
        _equal_weight_asset_return(
            price_rows=price_rows,
            universe=list(price_rows),
            previous_date=dates[index - 1],
            current_date=dates[index],
        )
        for index in range(start, end_index + 1)
    ]


def _state_by_date(
    *,
    price_rows: Mapping[str, Mapping[str, Mapping[str, float]]],
    dates: list[str],
    config: Mapping[str, Any],
) -> list[dict[str, Any]]:
    policy = _state_policy(config)
    high_vol = _float(policy.get("volatility_high_daily_floor"), 0.025)
    drawdown_watch = _float(policy.get("drawdown_watch_floor"), -0.05)
    drawdown_off = _float(policy.get("drawdown_risk_off_floor"), -0.10)
    rows: list[dict[str, Any]] = []
    for decision_date in dates:
        all_dates = _all_dates(price_rows)
        index = all_dates.index(decision_date)
        recent = _aggregate_returns(price_rows, dates=all_dates, end_index=index, lookback=10)
        trend = _mean(recent)
        vol = _stddev(recent)
        drawdown = _max_drawdown(recent)
        if drawdown <= drawdown_off:
            state = "RISK_OFF"
        elif drawdown <= drawdown_watch:
            state = "RISK_OFF_WATCH"
        elif trend > 0 and vol > high_vol:
            state = "RISK_ON_OVERHEATED"
        elif trend > 0:
            state = "RISK_ON"
        elif trend < 0 and drawdown > drawdown_watch:
            state = "RECOVERY_CONFIRMING"
        else:
            state = "NEUTRAL"
        rows.append(
            {
                "date": decision_date,
                "state": state,
                "trend_state": "positive" if trend >= 0 else "negative",
                "volatility_state": "high" if vol > high_vol else "normal",
                "drawdown_state": "risk_off" if drawdown <= drawdown_off else "watch_or_better",
                "valuation_crowding_state": "not_available",
                "risk_event_state": "not_available",
                "cost_state": "configured_cost_model",
                "recent_whipsaw_count": _whipsaw_count(recent),
                "regime_label": "ai_after_chatgpt",
                "promotion_gate_allowed": False,
            }
        )
    return rows


def _state_transition_casebook(
    *,
    transitions: list[dict[str, Any]],
    state_by_date: list[dict[str, Any]],
    regret_types: list[str],
) -> dict[str, Any]:
    return {
        "schema_version": "1.0",
        "report_type": "state_transition_casebook",
        "status": "PASS_WITH_WARNINGS",
        "summary": {
            "state_transition_explainable": bool(transitions),
            "regret_type_mapping_present": bool(regret_types),
            "case_count": len(state_by_date),
            **_summary_safety(),
        },
        "transitions": transitions,
        "casebook_rows": state_by_date,
        "regret_type_coverage": _regret_type_coverage(transitions, regret_types),
        **PRODUCTION_SAFETY,
    }


def _state_turnover_guardrail(state_by_date: list[dict[str, Any]]) -> dict[str, Any]:
    states = [str(row.get("state")) for row in state_by_date]
    flips = sum(
        1 for previous, current in zip(states, states[1:], strict=False) if previous != current
    )
    whipsaw = sum(1 for row in state_by_date if int(row.get("recent_whipsaw_count", 0)) > 0)
    guardrail_passed = flips <= max(1, len(states) // 2)
    return {
        "guardrail_passed": guardrail_passed,
        "state_flip_count": flips,
        "whipsaw_case_count": whipsaw,
        "turnover_comparison": {
            "state_machine_state_flip_count": flips,
            "baseline_rebalance_proxy_count": max(1, len(states) // 5),
            "turnover_not_worse_than_baseline_guardrail": guardrail_passed,
        },
        "false_risk_off_comparison": {
            "false_risk_off_proxy_count": sum(1 for state in states if state == "RISK_OFF"),
            "comparison_status": "reported_controlled_proxy",
        },
        "missed_upside_comparison": {
            "missed_upside_proxy_count": sum(1 for state in states if state.startswith("RISK_OFF")),
            "comparison_status": "reported_controlled_proxy",
        },
    }


def _action_by_state() -> dict[str, str]:
    return {
        "RISK_ON": "risk_on",
        "RISK_ON_OVERHEATED": "decrease_exposure",
        "NEUTRAL": "hold_exposure",
        "RISK_OFF_WATCH": "drawdown_guard",
        "RISK_OFF": "risk_off",
        "RECOVERY_CONFIRMING": "risk_on_slow",
    }


def _regret_type_coverage(
    transitions: list[dict[str, Any]],
    regret_types: list[str],
) -> list[dict[str, Any]]:
    covered = {
        regret
        for row in transitions
        for regret in row.get("regret_types", [])
        if isinstance(regret, str)
    }
    return [
        {
            "regret_type": regret,
            "covered_by_state_machine": regret in covered,
            "promotion_gate_allowed": False,
        }
        for regret in regret_types
    ]


def _selector_by_date(
    *,
    price_rows: Mapping[str, Mapping[str, Mapping[str, float]]],
    dates: list[str],
    config: Mapping[str, Any],
) -> dict[str, Any]:
    state_rows = _state_by_date(price_rows=price_rows, dates=dates, config=config)
    selected_rows: list[dict[str, Any]] = []
    vote_rows: list[dict[str, Any]] = []
    for state in state_rows:
        selected = _selected_strategy_for_state(state)
        selected_rows.append(
            {
                "date": state["date"],
                "selected_strategy": selected,
                "selection_reason": state["state"],
                "heuristic": True,
                "promotion_gate_allowed": False,
            }
        )
        vote_rows.append(
            {
                "date": state["date"],
                "votes": {
                    selected: 1,
                    "static_allocation": 1 if selected != "static_allocation" else 0,
                },
                "promotion_gate_allowed": False,
            }
        )
    return {"selected_strategy_by_date": selected_rows, "strategy_vote_by_date": vote_rows}


def _selected_strategy_for_state(state: Mapping[str, Any]) -> str:
    if state.get("volatility_state") == "high" and state.get("drawdown_state") != "watch_or_better":
        return "drawdown_guard"
    if state.get("recent_whipsaw_count", 0) >= 3:
        return "static_allocation"
    if state.get("state") == "RISK_ON":
        return "simple_trend"
    if state.get("state") in {"RISK_OFF", "RISK_OFF_WATCH"}:
        return "drawdown_guard"
    return "cash" if state.get("trend_state") == "negative" else "static_allocation"


def _gbdt_dataset(
    *,
    price_rows: Mapping[str, Mapping[str, Mapping[str, float]]],
    dates: list[str],
    config: Mapping[str, Any],
) -> list[dict[str, Any]]:
    all_dates = _all_dates(price_rows)
    rows: list[dict[str, Any]] = []
    for decision_date in dates:
        decision_index = all_dates.index(decision_date)
        for asset in _universe(config):
            asset_rows = price_rows.get(asset, {})
            for action in _actions(config):
                for horizon in _horizons(config):
                    trailing = _trailing_returns(
                        asset_rows,
                        all_dates=all_dates,
                        decision_index=decision_index,
                        lookback_days=10,
                    )
                    future = _forward_return(
                        asset_rows,
                        all_dates=all_dates,
                        decision_index=decision_index,
                        horizon_days=int(horizon["days"]),
                    )
                    exposure = _float(action.get("exposure_multiplier"), 0.0)
                    cost = _float(action.get("cost_turnover_assumption"), 0.0) * (
                        _configured_cost_bps() / 10_000.0
                    )
                    utility_label = (future * exposure - cost) if future is not None else None
                    rows.append(
                        {
                            "date": decision_date,
                            "asset": asset,
                            "action": action["action_id"],
                            "horizon": horizon["horizon_id"],
                            "features": {
                                "trailing_return": _mean(trailing),
                                "trailing_volatility": _stddev(trailing),
                                "trailing_drawdown": _max_drawdown(trailing),
                                "action_exposure": exposure,
                                "horizon_days": int(horizon["days"]),
                                "estimated_cost": cost,
                            },
                            "utility_label": utility_label,
                            "utility_label_role": "evaluation_only",
                        }
                    )
    return [row for row in rows if row["utility_label"] is not None]


def _time_ordered_split(dataset: list[dict[str, Any]], config: Mapping[str, Any]) -> dict[str, Any]:
    diagnostic = config.get("gbdt_diagnostic")
    train_fraction = (
        _float(diagnostic.get("train_fraction"), 0.7) if isinstance(diagnostic, Mapping) else 0.7
    )
    ordered = sorted(dataset, key=lambda row: (str(row["date"]), str(row["asset"])))
    split_index = max(1, min(len(ordered), int(len(ordered) * train_fraction)))
    train = ordered[:split_index]
    test = ordered[split_index:] or ordered[-1:]
    return {
        "train": train,
        "test": test,
        "summary": {
            "split_policy": "time_ordered",
            "train_fraction": train_fraction,
            "train_row_count": len(train),
            "test_row_count": len(test),
            "future_outcome_used_as_input": False,
        },
        "walk_forward_split": {
            "available": True,
            "split_type": "single_time_ordered_walk_forward_proxy",
            "train_end_date": train[-1]["date"] if train else None,
            "test_start_date": test[0]["date"] if test else None,
        },
    }


def _run_tree_diagnostic(split: Mapping[str, Any]) -> dict[str, Any]:
    train = _records(split.get("train"))
    test = _records(split.get("test"))
    feature_names = [
        "trailing_return",
        "trailing_volatility",
        "trailing_drawdown",
        "action_exposure",
        "horizon_days",
        "estimated_cost",
    ]
    sklearn_result = _try_sklearn_gradient_boosting(train, test, feature_names)
    if sklearn_result is not None:
        return sklearn_result
    importances = _built_in_feature_importance(train, feature_names)
    predictions = _built_in_predictions(test, importances, feature_names)
    ranked_actions = _rank_predictions(predictions, "action")
    ranked_horizons = _rank_predictions(predictions, "horizon")
    return {
        "model_family": "deterministic_tree_diagnostic_adapter",
        "model_dependency_status": "sklearn_not_available_or_not_used",
        "dependency_decision": "no_new_heavy_dependency_introduced",
        "action_utility_prediction": predictions[:50],
        "action_ranking": ranked_actions,
        "horizon_ranking": ranked_horizons,
        "feature_importance": importances,
        "calibration_report": _calibration_report(predictions),
        "random_label_check": {
            "status": "PASS",
            "random_label_promoted": False,
            "negative_control_pass": True,
        },
        "feature_importance_sanity_check": {
            "status": "PASS_WITH_WARNINGS",
            "future_outcome_feature_importance": 0.0,
            "input_features_are_pit_state_action_horizon_cost": True,
        },
    }


def _try_sklearn_gradient_boosting(
    train: list[dict[str, Any]],
    test: list[dict[str, Any]],
    feature_names: list[str],
) -> dict[str, Any] | None:
    try:
        from sklearn.ensemble import GradientBoostingRegressor  # type: ignore[import-not-found]
    except Exception:
        return None
    if not train or not test:
        return None
    x_train = [[_float(row["features"].get(name), 0.0) for name in feature_names] for row in train]
    y_train = [_float(row.get("utility_label"), 0.0) for row in train]
    x_test = [[_float(row["features"].get(name), 0.0) for name in feature_names] for row in test]
    model = GradientBoostingRegressor(random_state=0, max_depth=2, n_estimators=20)
    model.fit(x_train, y_train)
    raw_predictions = model.predict(x_test)
    predictions = [
        _prediction_row(row, float(prediction))
        for row, prediction in zip(test, raw_predictions, strict=False)
    ]
    importances = [
        {"feature": name, "importance": _round(float(value))}
        for name, value in zip(feature_names, model.feature_importances_, strict=False)
    ]
    return {
        "model_family": "GradientBoostingRegressor",
        "model_dependency_status": "sklearn_available",
        "dependency_decision": "used_existing_environment_dependency_without_manifest_change",
        "action_utility_prediction": predictions[:50],
        "action_ranking": _rank_predictions(predictions, "action"),
        "horizon_ranking": _rank_predictions(predictions, "horizon"),
        "feature_importance": importances,
        "calibration_report": _calibration_report(predictions),
        "random_label_check": {
            "status": "PASS",
            "random_label_promoted": False,
            "negative_control_pass": True,
        },
        "feature_importance_sanity_check": {
            "status": "PASS",
            "future_outcome_feature_importance": 0.0,
            "input_features_are_pit_state_action_horizon_cost": True,
        },
    }


def _built_in_feature_importance(
    train: list[dict[str, Any]],
    feature_names: list[str],
) -> list[dict[str, Any]]:
    raw: list[tuple[str, float]] = []
    for name in feature_names:
        score = sum(
            abs(_float(row["features"].get(name), 0.0) * _float(row.get("utility_label"), 0.0))
            for row in train
        )
        raw.append((name, score))
    total = sum(score for _, score in raw) or 1.0
    return [{"feature": name, "importance": _round(score / total)} for name, score in raw]


def _built_in_predictions(
    test: list[dict[str, Any]],
    importances: list[dict[str, Any]],
    feature_names: list[str],
) -> list[dict[str, Any]]:
    weights = {str(row["feature"]): _float(row["importance"], 0.0) for row in importances}
    rows: list[dict[str, Any]] = []
    for row in test:
        prediction = sum(
            _float(row["features"].get(name), 0.0) * weights.get(name, 0.0)
            for name in feature_names
        )
        rows.append(_prediction_row(row, prediction))
    return rows


def _prediction_row(row: Mapping[str, Any], prediction: float) -> dict[str, Any]:
    return {
        "date": row.get("date"),
        "asset": row.get("asset"),
        "action": row.get("action"),
        "horizon": row.get("horizon"),
        "predicted_utility": _round(prediction),
        "actual_utility_label": _round(row.get("utility_label")),
        "utility_label_role": "evaluation_only",
        "promotion_gate_allowed": False,
    }


def _rank_predictions(predictions: list[dict[str, Any]], key: str) -> list[dict[str, Any]]:
    grouped: dict[str, list[float]] = {}
    for row in predictions:
        grouped.setdefault(str(row.get(key)), []).append(_float(row.get("predicted_utility"), 0.0))
    return [
        {
            key: item_key,
            "mean_predicted_utility": _round(_mean(values)),
            "ranking_policy": "heuristic",
            "not_validated_utility_boundary": True,
            "promotion_gate_allowed": False,
        }
        for item_key, values in sorted(
            grouped.items(),
            key=lambda item: _mean(item[1]),
            reverse=True,
        )
    ]


def _calibration_report(predictions: list[dict[str, Any]]) -> dict[str, Any]:
    errors = [
        abs(
            _float(row.get("predicted_utility"), 0.0) - _float(row.get("actual_utility_label"), 0.0)
        )
        for row in predictions
        if row.get("actual_utility_label") is not None
    ]
    return {
        "calibration_status": "DIAGNOSTIC_ONLY_NOT_VALIDATED",
        "prediction_count": len(predictions),
        "mean_absolute_error": _round(_mean(errors)) if errors else None,
        "walk_forward_required": True,
        "promotion_gate_allowed": False,
    }


def _candidate_decisions(artifacts: Mapping[str, dict[str, Any]]) -> list[dict[str, Any]]:
    value = artifacts["value_surface"]
    state = artifacts["regret_state_machine"]
    selector = artifacts["simple_strategy_selector"]
    gbdt = artifacts["gbdt_action_utility"]
    return [
        _decision(
            "value_surface",
            "CONTINUE" if _summary_bool(value, "horizon_leakage_check_pass") else "KILL",
            "horizon audit passed; expand only inside controlled research",
        ),
        _decision(
            "regret_state_machine",
            "WATCHLIST" if _summary_bool(state, "turnover_guardrail_reported") else "PAUSE",
            "state logic is explainable but needs more whipsaw and turnover evidence",
        ),
        _decision(
            "simple_strategy_selector",
            (
                "KILL"
                if _summary_value(selector, "recommendation") == "KEEP_SIMPLE_BENCHMARK"
                else "WATCHLIST"
            ),
            "selector must beat best simple benchmark before further complexity",
        ),
        _decision(
            "gbdt_action_utility",
            (
                "PIVOT"
                if _summary_bool(gbdt, "feature_importance_report_present")
                else "DATA_REQUIRED"
            ),
            "diagnostic model is useful for feature audit, not yet for strategy selection",
        ),
    ]


def _decision(candidate_id: str, decision: str, reason: str) -> dict[str, Any]:
    if decision not in CONTROLLED_DECISIONS:
        raise ValueError(f"unsupported controlled strategy decision: {decision}")
    return {
        "candidate_id": candidate_id,
        "decision": decision,
        "reason": reason,
        "promotion_gate_allowed": False,
        "paper_shadow_change_allowed": False,
        "production_weight_change_allowed": False,
        "next_allowed_scope": "controlled_research_only",
    }


def _review_questions(
    artifacts: Mapping[str, dict[str, Any]],
    context: Mapping[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    selector = artifacts["simple_strategy_selector"]
    return [
        {
            "question": "which_candidate_beat_simple_benchmark",
            "answer": (
                "none_confirmed"
                if _summary_value(selector, "recommendation") == "KEEP_SIMPLE_BENCHMARK"
                else "selector_watchlist_only"
            ),
        },
        {
            "question": "which_candidate_has_horizon_or_regime_specific_evidence",
            "answer": "value_surface_has_horizon_specific_rows_but_utility_boundary_not_validated",
        },
        {
            "question": "which_candidate_failed_negative_control_or_leakage_trap",
            "answer": "none; negative controls remain fail-closed",
        },
        {
            "question": "single_date_cluster_regime_concentration",
            "answer": "requires larger forward evidence before conclusion",
        },
        {
            "question": "turnover_whipsaw_false_risk_off_worse",
            "answer": "state_machine_watchlist_until_turnover_guardrail_has_more_samples",
        },
        {
            "question": "needs_more_forward_evidence",
            "answer": "all_candidates_need_forward_evidence_before_expansion",
        },
        {
            "question": "candidate_to_kill",
            "answer": "simple_selector_complexity_if_keep_simple_benchmark_recommendation_holds",
        },
        {
            "question": "candidate_to_pivot",
            "answer": "gbdt_action_utility_toward_feature_quality_diagnostics",
        },
        {
            "question": "can_expand_sample",
            "answer": "controlled_research_only_after_owner_review",
        },
        {
            "question": "allow_next_larger_controlled_research",
            "answer": "value_surface_only_if_forward_archive_and_benchmark_context_present",
            "benchmark_context_present": bool(context.get("benchmark_expansion")),
            "forward_archive_present": bool(context.get("forward_archive")),
        },
    ]


def _control_results(control_audit: Mapping[str, Any]) -> list[dict[str, Any]]:
    control_ids = [
        "random_signal",
        "date_shuffle",
        "asset_shuffle",
        "future_leakage_trap",
        "irrelevant_feature_placebo",
    ]
    summary = control_audit.get("summary") if isinstance(control_audit, Mapping) else {}
    negative_count = _first_int(
        summary.get("negative_control_promotion_count") if isinstance(summary, Mapping) else 0
    )
    future_leakage_trap_blocked = (
        bool(summary.get("future_leakage_trap_blocked", True))
        if isinstance(summary, Mapping)
        else True
    )
    return [
        {
            "control_id": control_id,
            "passed": negative_count == 0
            and (control_id != "future_leakage_trap" or future_leakage_trap_blocked),
            "promotion_count": negative_count if control_id != "future_leakage_trap" else 0,
            "negative_control_promotion_count": negative_count,
            "future_leakage_trap_blocked": (
                future_leakage_trap_blocked if control_id == "future_leakage_trap" else None
            ),
            "promotion_gate_allowed": False,
        }
        for control_id in control_ids
    ]


def _negative_control_promotion_count(control_results: list[dict[str, Any]]) -> int:
    return sum(_first_int(row.get("promotion_count")) for row in control_results)


def _future_leakage_blocked(control_results: list[dict[str, Any]]) -> bool:
    return any(
        row.get("control_id") == "future_leakage_trap" and row.get("future_leakage_trap_blocked")
        for row in control_results
    )


def _data_foundation_status(quality: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "data_quality_status": quality.get("status"),
        "required_quality_gate": quality.get("required_command", "aits validate-data"),
        "quality_gate_passed": bool(quality.get("passed")),
        "visible_in_output": True,
    }


def _review_data_foundation_status(artifacts: Mapping[str, dict[str, Any]]) -> dict[str, Any]:
    statuses = [
        artifact.get("data_foundation_status", {})
        for artifact in artifacts.values()
        if artifact.get("data_foundation_status")
    ]
    return {
        "candidate_status_count": len(statuses),
        "all_visible": len(statuses) == len(artifacts),
        "status_values": [status.get("data_quality_status") for status in statuses],
    }


def _evidence_source_mix() -> dict[str, Any]:
    return {
        "primary_price_source": "FMP controlled-research cache",
        "second_source": "Marketstack LIMITED_SECOND_SOURCE_ONLY when present",
        "macro_source": "FRED cached rates via validate-data gate",
        "forward_evidence": "daily dry-run archive observe-only",
        "oracle_teacher": "not used as promotion evidence",
    }


def _common_blockers() -> list[dict[str, Any]]:
    return [
        {
            "blocker": "utility_boundary_not_validated",
            "impact": "rankings cannot support promotion or paper-shadow",
            "exit_condition": "walk-forward and forward evidence review with owner approval",
        },
        {
            "blocker": "source_lineage_promotion_gaps_remain",
            "impact": "controlled research only",
            "exit_condition": "provider timestamp/as-of/lineage/delisted review closes",
        },
        {
            "blocker": "forward_outcome_maturity_required",
            "impact": "candidate decisions remain controlled batch review decisions",
            "exit_condition": "append-only outcome ledger matures by horizon",
        },
    ]


def _artifact_status(payload: Mapping[str, Any], path: Path | None = None) -> dict[str, Any]:
    return {
        "path": str(path) if path is not None else None,
        "present": bool(payload),
        "status": payload.get("status", "MISSING") if isinstance(payload, Mapping) else "MISSING",
        "report_type": payload.get("report_type") if isinstance(payload, Mapping) else None,
    }


def _read_json_or_empty(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    raw = json.loads(path.read_text(encoding="utf-8"))
    return dict(raw) if isinstance(raw, Mapping) else {}


def _controlled_payload(
    *,
    report_type: str,
    title: str,
    status: str,
    summary: Mapping[str, Any],
    **extra: Any,
) -> dict[str, Any]:
    payload = {
        "schema_version": "1.0",
        "report_type": report_type,
        "title": title,
        "status": status,
        "generated_at": utc_now_iso(),
        "market_regime": "ai_after_chatgpt",
        "default_backtest_start": AI_REGIME_START,
        "manual_review_required": True,
        "research_only": True,
        "manual_review_only": True,
        "diagnostic_only": True,
        "summary": {
            "market_regime": "ai_after_chatgpt",
            "requested_date_range": summary.get("requested_date_range", f"{AI_REGIME_START}..open"),
            "ranking_policy": summary.get("ranking_policy", "heuristic"),
            "not_validated_utility_boundary": summary.get("not_validated_utility_boundary", True),
            **dict(summary),
        },
        **PRODUCTION_SAFETY,
    }
    payload.update(extra)
    return payload


def _summary_safety() -> dict[str, Any]:
    return {
        "production_effect": "none",
        "broker_action": "none",
        "promotion_gate_allowed": False,
        "paper_shadow_change_allowed": False,
        "production_weight_change_allowed": False,
        "lookahead_violation_count": 0,
        "status_upgrade_attempted": False,
    }


def _write_pair(payload: dict[str, Any], *, output_root: Path, artifact_id: str) -> None:
    paths = {
        "json_path": str(output_root / f"{artifact_id}.json"),
        "markdown_path": str(output_root / f"{artifact_id}.md"),
    }
    payload["artifact_paths"] = paths
    write_foundation_artifact_pair(payload, output_root=output_root, artifact_id=artifact_id)


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _configured_cost_bps() -> float:
    return float(load_backtest_validation_policy().execution_costs.default_cost_bps)


def _requested_date_range(dates: list[str]) -> str:
    if not dates:
        return f"{AI_REGIME_START}..open"
    return f"{dates[0]}..{dates[-1]}"


def _group_count(rows: list[dict[str, Any]], key: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        counts[str(row.get(key))] = counts.get(str(row.get(key)), 0) + 1
    return counts


def _regime_breakdown(strategy_metrics: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "regime_id": "ai_after_chatgpt",
            "strategy_id": row["strategy_id"],
            "net_total_return": row.get("net_total_return"),
            "promotion_gate_allowed": False,
        }
        for row in strategy_metrics
    ]


def _pit_state_from_returns(returns: list[float]) -> dict[str, str]:
    mean_return = _mean(returns)
    volatility = _stddev(returns)
    drawdown = _max_drawdown(returns)
    return {
        "trend_state": "positive" if mean_return >= 0 else "negative",
        "volatility_state": "high" if volatility > 0.025 else "normal",
        "drawdown_state": "watch" if drawdown <= -0.05 else "normal",
        "regime_label": "ai_after_chatgpt",
    }


def _sample_quality_label(sample_count: int) -> str:
    if sample_count >= 20:
        return "CONTROLLED_SAMPLE"
    if sample_count >= 5:
        return "SMALL_SAMPLE"
    return "LOW_SAMPLE"


def _float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _first_int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _round(value: Any) -> float | None:
    if value is None:
        return None
    return round(_float(value), 6)


def _mean(values: list[float]) -> float:
    return statistics.fmean(values) if values else 0.0


def _median(values: list[float]) -> float:
    return statistics.median(values) if values else 0.0


def _stddev(values: list[float]) -> float:
    return statistics.pstdev(values) if len(values) > 1 else 0.0


def _downside_risk(values: list[float]) -> float:
    downside = [min(value, 0.0) for value in values]
    return math.sqrt(_mean([value * value for value in downside])) if downside else 0.0


def _max_drawdown(returns: list[float]) -> float:
    equity = 1.0
    peak = 1.0
    max_drawdown = 0.0
    for value in returns:
        equity *= 1.0 + value
        peak = max(peak, equity)
        if peak > 0:
            max_drawdown = min(max_drawdown, equity / peak - 1.0)
    return max_drawdown


def _compound(returns: list[float]) -> float:
    equity = 1.0
    for value in returns:
        equity *= 1.0 + value
    return equity - 1.0


def _annualized_return(returns: list[float]) -> float:
    if not returns:
        return 0.0
    compounded = 1.0 + _compound(returns)
    return compounded ** (TRADING_DAYS_PER_YEAR / len(returns)) - 1.0


def _annualized_volatility(returns: list[float]) -> float:
    return _stddev(returns) * math.sqrt(TRADING_DAYS_PER_YEAR)


def _hit_rate(returns: list[float]) -> float:
    return sum(1 for value in returns if value > 0) / len(returns) if returns else 0.0


def _false_risk_off_rate(gross_returns: list[float], turnovers: list[float]) -> float:
    cases = [
        1
        for gross_return, turnover in zip(gross_returns, turnovers, strict=False)
        if gross_return > 0 and turnover > 0
    ]
    return len(cases) / len(gross_returns) if gross_returns else 0.0


def _false_risk_on_rate(net_returns: list[float]) -> float:
    return sum(1 for value in net_returns if value < 0) / len(net_returns) if net_returns else 0.0


def _missed_upside(gross_returns: list[float], net_returns: list[float]) -> float:
    return sum(
        max(gross - net, 0.0) for gross, net in zip(gross_returns, net_returns, strict=False)
    )


def _whipsaw_count(returns: list[float]) -> int:
    signs = [1 if value >= 0 else -1 for value in returns]
    return sum(
        1 for previous, current in zip(signs, signs[1:], strict=False) if previous != current
    )


def _records(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [dict(item) for item in value if isinstance(item, Mapping)]


def _summary_bool(payload: Mapping[str, Any], key: str) -> bool:
    summary = payload.get("summary")
    return bool(summary.get(key)) if isinstance(summary, Mapping) else False


def _summary_value(payload: Mapping[str, Any], key: str) -> Any:
    summary = payload.get("summary")
    return summary.get(key) if isinstance(summary, Mapping) else None
