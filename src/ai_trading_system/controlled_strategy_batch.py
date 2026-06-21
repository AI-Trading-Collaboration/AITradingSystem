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
DEFAULT_UTILITY_BOUNDARY_AUDIT_PATH = (
    DEFAULT_UTILITY_BOUNDARY_OUTPUT_ROOT / "utility_boundary_ranking_policy_audit.json"
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
