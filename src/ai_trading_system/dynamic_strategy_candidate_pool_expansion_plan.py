from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import date
from pathlib import Path
from typing import Any

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.data_foundation import utc_now_iso
from ai_trading_system.dynamic_strategy_cost_turnover_cooldown_sensitivity import (
    DEFAULT_DYNAMIC_STRATEGY_COST_TURNOVER_COOLDOWN_SENSITIVITY_OUTPUT_ROOT,
    DEFAULT_SOURCE_CANDIDATE_RANKING_PATH,
    PRIMARY_EXECUTION_CADENCE,
)
from ai_trading_system.dynamic_strategy_cost_turnover_cooldown_sensitivity import (
    READY_STATUS as SOURCE_2366_READY_STATUS,
)
from ai_trading_system.dynamic_strategy_event_driven_retest import (
    READY_STATUS as SOURCE_2365_READY_STATUS,
)
from ai_trading_system.dynamic_strategy_guarded_variant_owner_review_decision import (
    BEST_GUARDED_VARIANT,
    DEFAULT_DYNAMIC_STRATEGY_GUARDED_VARIANT_OWNER_REVIEW_DECISION_OUTPUT_ROOT,
)
from ai_trading_system.dynamic_strategy_guarded_variant_owner_review_decision import (
    NEXT_ROUTE as SOURCE_2384_EXPECTED_ROUTE,
)
from ai_trading_system.dynamic_strategy_guarded_variant_owner_review_decision import (
    OWNER_DECISION as SOURCE_2384_OWNER_DECISION,
)
from ai_trading_system.dynamic_strategy_guarded_variant_owner_review_decision import (
    READY_STATUS as SOURCE_2384_READY_STATUS,
)
from ai_trading_system.dynamic_strategy_ranking_top_guarded_turnover_retest_plan import (
    BEST_LOWER_TURNOVER_VARIANT,
)
from ai_trading_system.dynamic_strategy_ranking_top_guarded_variant_retest import (
    DEFAULT_DYNAMIC_STRATEGY_RANKING_TOP_GUARDED_VARIANT_RETEST_OUTPUT_ROOT,
    RANKING_TOP_CANDIDATE,
)
from ai_trading_system.dynamic_strategy_ranking_top_guarded_variant_retest import (
    NEXT_ROUTE as SOURCE_2383_EXPECTED_ROUTE,
)
from ai_trading_system.dynamic_strategy_ranking_top_guarded_variant_retest import (
    READY_STATUS as SOURCE_2383_READY_STATUS,
)
from ai_trading_system.dynamic_strategy_slice_robustness_optimized_variant_retest import (
    BASE_CANDIDATE_ID,
    DEFAULT_DYNAMIC_STRATEGY_SLICE_ROBUSTNESS_OPTIMIZED_VARIANT_RETEST_OUTPUT_ROOT,
    RANKING_TOP_REFERENCE,
)
from ai_trading_system.dynamic_strategy_slice_robustness_optimized_variant_retest import (
    NEXT_ROUTE as SOURCE_2379_EXPECTED_ROUTE,
)
from ai_trading_system.dynamic_strategy_slice_robustness_optimized_variant_retest import (
    READY_STATUS as SOURCE_2379_READY_STATUS,
)
from ai_trading_system.execution_semantics import AI_REGIME_SUMMARY, _file_sha256

TASK_ID = "TRADING-2385"
TASK_REGISTER_ID = (
    "TRADING-2385_DYNAMIC_STRATEGY_CANDIDATE_POOL_EXPANSION_AND_SIGNAL_FAMILY_"
    "DIVERSIFICATION_PLAN"
)
REPORT_TYPE = "dynamic_strategy_candidate_pool_expansion_plan"
SCHEMA_VERSION = (
    "dynamic_strategy_candidate_pool_expansion_and_signal_family_"
    "diversification_plan.v1"
)
READY_STATUS = (
    "DYNAMIC_STRATEGY_CANDIDATE_POOL_EXPANSION_AND_SIGNAL_FAMILY_"
    "DIVERSIFICATION_PLAN_READY"
)
BLOCKED_SOURCE_STATUS = (
    "DYNAMIC_STRATEGY_CANDIDATE_POOL_EXPANSION_AND_SIGNAL_FAMILY_"
    "DIVERSIFICATION_PLAN_BLOCKED_SOURCE_ARTIFACT"
)
NEXT_ROUTE = (
    "TRADING-2386_Dynamic_Strategy_Expanded_Candidate_Pool_Retest_And_Screening"
)
NEXT_DIRECTION = "EXPANDED_CANDIDATE_POOL_RETEST_AND_SCREENING"
DATA_QUALITY_GATE_REASON = (
    "NOT_APPLICABLE_PRIOR_ARTIFACT_PLAN_ONLY_NO_FRESH_MARKET_DATA"
)
SOURCE_TASKS: tuple[str, ...] = (
    "TRADING-2365",
    "TRADING-2366",
    "TRADING-2379",
    "TRADING-2380",
    "TRADING-2383",
    "TRADING-2384",
)
REFERENCE_CANDIDATE_IDS: tuple[str, ...] = (
    "static_baseline",
    RANKING_TOP_CANDIDATE,
    BASE_CANDIDATE_ID,
    BEST_LOWER_TURNOVER_VARIANT,
    BEST_GUARDED_VARIANT,
)
SIGNAL_FAMILY_IDS: tuple[str, ...] = (
    "regime_transition_family",
    "trend_confirmation_family",
    "volatility_aware_family",
    "signal_age_valid_until_family",
    "turnover_budget_family",
    "risk_cap_interaction_family",
)
CANDIDATE_BUDGET: dict[str, Any] = {
    "max_new_candidate_families": 6,
    "max_candidates_per_family": 3,
    "max_total_new_candidates_for_2386": 12,
    "include_reference_candidates": True,
    "require_family_hypothesis": True,
    "require_predefined_acceptance_criteria": True,
    "forbid_post_hoc_metric_cherry_picking": True,
}
FORBIDDEN_PATHS: tuple[str, ...] = (
    "generate_unbounded_parameter_grid",
    "optimize_only_for_total_return",
    "use_monthly_rebalance_as_primary",
    "ignore_cost_and_slippage",
    "ignore_turnover",
    "ignore_time_slice_failures",
    "ignore_regime_slice_failures",
    "accept_candidate_without_static_baseline_comparison",
    "accept_candidate_without_lower_turnover_reference_comparison",
    "accept_candidate_without_ranking_top_reference_comparison",
)
NON_APPROVED_PATHS: tuple[str, ...] = (
    "research_only_observation",
    "paper_shadow",
    "paper_trade",
    "shadow_position",
    "event_append",
    "outcome_binding",
    "scheduler",
    "scheduled_task",
    "daily_report",
    "production",
    "broker",
    "order",
    "new_backtest",
    "new_signal",
    "scoring",
)
SAFETY_FALSE_FIELDS: tuple[str, ...] = (
    "scheduler_enabled",
    "scheduled_task_created",
    "event_append_enabled",
    "historical_event_log_mutated",
    "outcome_binding_enabled",
    "outcome_store_mutated",
    "paper_shadow_enabled",
    "paper_trade_created",
    "shadow_position_created",
    "production_enabled",
    "broker_action_enabled",
    "order_generated",
    "daily_report_generated",
)
LOCAL_OPTIMIZATION_LIMITATIONS: tuple[str, ...] = (
    "lower_turnover_line_remains_continue_optimization_after_2379_and_2380",
    "ranking_top_guarded_line_remains_continue_optimization_after_2383",
    "neither_line_reached_research_only_observation_threshold",
    "further_micro_tuning_risks_overfitting_without_signal_family_diversity",
    "owner_decision_2384_requires_candidate_pool_expansion_before_observation",
)
ANTIOVERFIT_GUARDRAILS: tuple[str, ...] = (
    "pre_register_candidate_family_hypotheses_before_2386",
    "cap_new_candidates_at_12_for_default_2386_run",
    "compare_every_candidate_to_static_lower_turnover_and_ranking_top_references",
    "evaluate_cost_turnover_time_slice_and_regime_slice_before_owner_review",
    "forbid_post_hoc_metric_cherry_picking",
    "keep_monthly_rebalance_out_of_primary_decision_path",
)

DEFAULT_DYNAMIC_STRATEGY_CANDIDATE_POOL_EXPANSION_PLAN_OUTPUT_ROOT = (
    PROJECT_ROOT / "outputs" / "research_strategies" / REPORT_TYPE
)
DEFAULT_DYNAMIC_STRATEGY_CANDIDATE_POOL_EXPANSION_PLAN_DOCS_ROOT = (
    PROJECT_ROOT / "docs" / "research"
)
DEFAULT_SOURCE_2384_OWNER_REVIEW_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_GUARDED_VARIANT_OWNER_REVIEW_DECISION_OUTPUT_ROOT
    / "owner_review_decision.json"
)
DEFAULT_SOURCE_2384_NEXT_DIRECTION_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_GUARDED_VARIANT_OWNER_REVIEW_DECISION_OUTPUT_ROOT
    / "next_research_direction_decision.json"
)
DEFAULT_SOURCE_2383_GUARDED_VARIANT_RETEST_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_RANKING_TOP_GUARDED_VARIANT_RETEST_OUTPUT_ROOT
    / "guarded_variant_retest_result.json"
)
DEFAULT_SOURCE_2383_GUARDED_VARIANT_RANKING_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_RANKING_TOP_GUARDED_VARIANT_RETEST_OUTPUT_ROOT
    / "guarded_variant_ranking.json"
)
DEFAULT_SOURCE_2383_DECISION_UPDATE_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_RANKING_TOP_GUARDED_VARIANT_RETEST_OUTPUT_ROOT
    / "decision_update.json"
)
DEFAULT_SOURCE_2379_VARIANT_RETEST_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_SLICE_ROBUSTNESS_OPTIMIZED_VARIANT_RETEST_OUTPUT_ROOT
    / "variant_retest_result.json"
)
DEFAULT_SOURCE_2379_OPTIMIZED_VARIANT_RANKING_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_SLICE_ROBUSTNESS_OPTIMIZED_VARIANT_RETEST_OUTPUT_ROOT
    / "optimized_variant_ranking.json"
)
DEFAULT_SOURCE_2366_SENSITIVITY_RESULT_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_COST_TURNOVER_COOLDOWN_SENSITIVITY_OUTPUT_ROOT
    / "sensitivity_result.json"
)
DEFAULT_SOURCE_2366_DECISION_UPDATE_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_COST_TURNOVER_COOLDOWN_SENSITIVITY_OUTPUT_ROOT
    / "decision_update.json"
)


def run_dynamic_strategy_candidate_pool_expansion_plan(
    *,
    source_owner_review_path: Path = DEFAULT_SOURCE_2384_OWNER_REVIEW_PATH,
    source_next_direction_path: Path = DEFAULT_SOURCE_2384_NEXT_DIRECTION_PATH,
    source_guarded_variant_retest_path: Path = (
        DEFAULT_SOURCE_2383_GUARDED_VARIANT_RETEST_PATH
    ),
    source_guarded_variant_ranking_path: Path = (
        DEFAULT_SOURCE_2383_GUARDED_VARIANT_RANKING_PATH
    ),
    source_guarded_decision_update_path: Path = (
        DEFAULT_SOURCE_2383_DECISION_UPDATE_PATH
    ),
    source_variant_retest_path: Path = DEFAULT_SOURCE_2379_VARIANT_RETEST_PATH,
    source_optimized_variant_ranking_path: Path = (
        DEFAULT_SOURCE_2379_OPTIMIZED_VARIANT_RANKING_PATH
    ),
    source_candidate_ranking_path: Path = DEFAULT_SOURCE_CANDIDATE_RANKING_PATH,
    source_sensitivity_result_path: Path = DEFAULT_SOURCE_2366_SENSITIVITY_RESULT_PATH,
    source_sensitivity_decision_update_path: Path = (
        DEFAULT_SOURCE_2366_DECISION_UPDATE_PATH
    ),
    output_root: Path = (
        DEFAULT_DYNAMIC_STRATEGY_CANDIDATE_POOL_EXPANSION_PLAN_OUTPUT_ROOT
    ),
    docs_root: Path = DEFAULT_DYNAMIC_STRATEGY_CANDIDATE_POOL_EXPANSION_PLAN_DOCS_ROOT,
    as_of_date: date | None = None,
) -> dict[str, Any]:
    sources = _load_sources(
        source_owner_review_path=source_owner_review_path,
        source_next_direction_path=source_next_direction_path,
        source_guarded_variant_retest_path=source_guarded_variant_retest_path,
        source_guarded_variant_ranking_path=source_guarded_variant_ranking_path,
        source_guarded_decision_update_path=source_guarded_decision_update_path,
        source_variant_retest_path=source_variant_retest_path,
        source_optimized_variant_ranking_path=source_optimized_variant_ranking_path,
        source_candidate_ranking_path=source_candidate_ranking_path,
        source_sensitivity_result_path=source_sensitivity_result_path,
        source_sensitivity_decision_update_path=(
            source_sensitivity_decision_update_path
        ),
    )
    ready = not sources["source_validation_errors"]
    resolved_as_of = _resolve_as_of(as_of_date, sources)
    family_plan = _signal_family_diversification_plan(ready=ready)
    selected_candidates = _selected_new_candidates_for_2386(family_plan)
    candidate_plan = _candidate_pool_expansion_plan(
        sources=sources,
        family_plan=family_plan,
        selected_candidates=selected_candidates,
        ready=ready,
    )
    budget_guardrails = _candidate_budget_guardrails(
        family_plan=family_plan,
        selected_candidates=selected_candidates,
        ready=ready,
    )
    retest_plan = _retest_plan_2386(
        selected_candidates=selected_candidates,
        budget_guardrails=budget_guardrails,
        ready=ready,
    )
    payload = _base_payload(
        status=READY_STATUS if ready else BLOCKED_SOURCE_STATUS,
        as_of_date=resolved_as_of,
        sources=sources,
        candidate_plan=candidate_plan,
        family_plan=family_plan,
        budget_guardrails=budget_guardrails,
        retest_plan=retest_plan,
        ready=ready,
    )
    _write_outputs(payload=payload, output_root=output_root, docs_root=docs_root)
    return payload


def _load_sources(
    *,
    source_owner_review_path: Path,
    source_next_direction_path: Path,
    source_guarded_variant_retest_path: Path,
    source_guarded_variant_ranking_path: Path,
    source_guarded_decision_update_path: Path,
    source_variant_retest_path: Path,
    source_optimized_variant_ranking_path: Path,
    source_candidate_ranking_path: Path,
    source_sensitivity_result_path: Path,
    source_sensitivity_decision_update_path: Path,
) -> dict[str, Any]:
    owner_review = _load_json_document(source_owner_review_path)
    next_direction = _load_json_document(source_next_direction_path)
    guarded_variant_retest = _load_json_document(source_guarded_variant_retest_path)
    guarded_variant_ranking = _load_json_document(source_guarded_variant_ranking_path)
    guarded_decision_update = _load_json_document(source_guarded_decision_update_path)
    variant_retest = _load_json_document(source_variant_retest_path)
    optimized_variant_ranking = _load_json_document(
        source_optimized_variant_ranking_path
    )
    candidate_ranking = _load_json_document(source_candidate_ranking_path)
    sensitivity_result = _load_json_document(source_sensitivity_result_path)
    sensitivity_decision_update = _load_json_document(
        source_sensitivity_decision_update_path
    )
    source_status = {
        "owner_review": owner_review.get("status"),
        "next_direction": next_direction.get("status"),
        "guarded_variant_retest": guarded_variant_retest.get("status"),
        "guarded_variant_ranking": guarded_variant_ranking.get("status"),
        "guarded_decision_update": guarded_decision_update.get("status"),
        "variant_retest": variant_retest.get("status"),
        "optimized_variant_ranking": optimized_variant_ranking.get("status"),
        "candidate_ranking": candidate_ranking.get("status"),
        "sensitivity_result": sensitivity_result.get("status"),
        "sensitivity_decision_update": sensitivity_decision_update.get("status"),
    }
    source_files = {
        "owner_review": str(source_owner_review_path),
        "next_direction": str(source_next_direction_path),
        "guarded_variant_retest": str(source_guarded_variant_retest_path),
        "guarded_variant_ranking": str(source_guarded_variant_ranking_path),
        "guarded_decision_update": str(source_guarded_decision_update_path),
        "variant_retest": str(source_variant_retest_path),
        "optimized_variant_ranking": str(source_optimized_variant_ranking_path),
        "candidate_ranking": str(source_candidate_ranking_path),
        "sensitivity_result": str(source_sensitivity_result_path),
        "sensitivity_decision_update": str(source_sensitivity_decision_update_path),
    }
    sources: dict[str, Any] = {
        "owner_review": owner_review,
        "next_direction": next_direction,
        "guarded_variant_retest": guarded_variant_retest,
        "guarded_variant_ranking": guarded_variant_ranking,
        "guarded_decision_update": guarded_decision_update,
        "variant_retest": variant_retest,
        "optimized_variant_ranking": optimized_variant_ranking,
        "candidate_ranking": candidate_ranking,
        "sensitivity_result": sensitivity_result,
        "sensitivity_decision_update": sensitivity_decision_update,
        "source_status": source_status,
        "source_files": source_files,
        "source_hashes": {
            key: _file_sha256(Path(path)) for key, path in source_files.items()
        },
        "owner_decision_from_2384": owner_review.get("owner_decision"),
        "next_direction_from_2384": _extract_next_direction(next_direction),
        "lower_turnover_base_candidate": _coalesce_string(
            variant_retest.get("base_candidate"),
            owner_review.get("lower_turnover_line", {}).get("base_candidate")
            if isinstance(owner_review.get("lower_turnover_line"), Mapping)
            else None,
        ),
        "lower_turnover_best_variant": _coalesce_string(
            variant_retest.get("best_variant_after_retest"),
            optimized_variant_ranking.get("best_variant_after_retest"),
            owner_review.get("best_variant_from_2379"),
        ),
        "lower_turnover_decision": _coalesce_string(
            variant_retest.get("best_variant_decision"),
            optimized_variant_ranking.get("best_variant_decision"),
            owner_review.get("lower_turnover_line", {}).get("decision")
            if isinstance(owner_review.get("lower_turnover_line"), Mapping)
            else None,
        ),
        "ranking_top_candidate": _coalesce_string(
            guarded_variant_retest.get("ranking_top_candidate"),
            _top_candidate_from_candidate_ranking(candidate_ranking),
        ),
        "best_guarded_variant": _coalesce_string(
            guarded_variant_retest.get("best_guarded_variant"),
            guarded_variant_ranking.get("best_guarded_variant"),
            _as_mapping(guarded_decision_update.get("decision_update")).get(
                "best_guarded_variant"
            ),
            owner_review.get("best_guarded_variant_from_2383"),
        ),
        "best_guarded_variant_decision": _coalesce_string(
            guarded_variant_retest.get("best_guarded_variant_decision"),
            guarded_variant_ranking.get("best_guarded_variant_decision"),
            _as_mapping(guarded_decision_update.get("decision_update")).get(
                "best_guarded_variant_decision"
            ),
            _as_mapping(owner_review.get("ranking_top_guarded_line")).get("decision"),
        ),
        "primary_execution_cadence": _coalesce_string(
            owner_review.get("primary_execution_cadence"),
            guarded_variant_retest.get("primary_execution_cadence"),
            variant_retest.get("primary_execution_cadence"),
            candidate_ranking.get("primary_execution_cadence"),
            _top_candidate_primary_cadence(candidate_ranking),
            sensitivity_result.get("primary_execution_cadence"),
            _sensitivity_primary_cadence(sensitivity_result),
            _sensitivity_primary_cadence(sensitivity_decision_update),
        ),
        "sensitivity_matrix_count": len(
            sensitivity_result.get("sensitivity_matrix", [])
            if isinstance(sensitivity_result.get("sensitivity_matrix"), list)
            else []
        ),
        "source_validation_errors": [],
    }
    sources["source_validation_errors"] = _source_validation_errors(sources)
    return sources


def _source_validation_errors(sources: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    status_expectations = {
        "owner_review": SOURCE_2384_READY_STATUS,
        "next_direction": SOURCE_2384_READY_STATUS,
        "guarded_variant_retest": SOURCE_2383_READY_STATUS,
        "guarded_variant_ranking": SOURCE_2383_READY_STATUS,
        "guarded_decision_update": SOURCE_2383_READY_STATUS,
        "variant_retest": SOURCE_2379_READY_STATUS,
        "optimized_variant_ranking": SOURCE_2379_READY_STATUS,
        "candidate_ranking": SOURCE_2365_READY_STATUS,
        "sensitivity_result": SOURCE_2366_READY_STATUS,
        "sensitivity_decision_update": SOURCE_2366_READY_STATUS,
    }
    source_status = _as_mapping(sources.get("source_status"))
    for key, expected in status_expectations.items():
        actual = source_status.get(key)
        if actual != expected:
            errors.append(f"{key}.status expected {expected}, got {actual!r}")

    _expect_equal(
        errors,
        "owner_decision_from_2384",
        sources.get("owner_decision_from_2384"),
        SOURCE_2384_OWNER_DECISION,
    )
    _expect_equal(
        errors,
        "next_direction_from_2384",
        sources.get("next_direction_from_2384"),
        "OPTION_C_EXPAND_CANDIDATE_POOL_AND_SIGNAL_FAMILIES",
    )
    _expect_equal(
        errors,
        "lower_turnover_base_candidate",
        sources.get("lower_turnover_base_candidate"),
        BASE_CANDIDATE_ID,
    )
    _expect_equal(
        errors,
        "lower_turnover_best_variant",
        sources.get("lower_turnover_best_variant"),
        BEST_LOWER_TURNOVER_VARIANT,
    )
    _expect_equal(
        errors,
        "lower_turnover_decision",
        sources.get("lower_turnover_decision"),
        "CONTINUE_OPTIMIZATION",
    )
    _expect_equal(
        errors,
        "ranking_top_candidate",
        sources.get("ranking_top_candidate"),
        RANKING_TOP_CANDIDATE,
    )
    _expect_equal(
        errors,
        "ranking_top_reference",
        RANKING_TOP_REFERENCE,
        RANKING_TOP_CANDIDATE,
    )
    _expect_equal(
        errors,
        "best_guarded_variant",
        sources.get("best_guarded_variant"),
        BEST_GUARDED_VARIANT,
    )
    _expect_equal(
        errors,
        "best_guarded_variant_decision",
        sources.get("best_guarded_variant_decision"),
        "CONTINUE_OPTIMIZATION",
    )
    _expect_equal(
        errors,
        "primary_execution_cadence",
        sources.get("primary_execution_cadence"),
        PRIMARY_EXECUTION_CADENCE,
    )

    owner_review = _as_mapping(sources.get("owner_review"))
    if owner_review.get("recommended_next_research_task") != SOURCE_2384_EXPECTED_ROUTE:
        errors.append(
            "owner_review.recommended_next_research_task expected "
            f"{SOURCE_2384_EXPECTED_ROUTE}, "
            f"got {owner_review.get('recommended_next_research_task')!r}"
        )
    for field in (
        "candidate_pool_expansion_recommended",
        "signal_family_diversification_recommended",
    ):
        if owner_review.get(field) is not True:
            errors.append(f"owner_review.{field} must be true")
    for field in (
        "continue_local_optimization_allowed",
        "research_only_observation_approved",
    ):
        if _find_nested_bool(owner_review, field) is not False:
            errors.append(f"owner_review.{field} must be false")

    next_direction = _as_mapping(sources.get("next_direction"))
    next_direction_payload = _as_mapping(
        next_direction.get("next_research_direction_decision")
    )
    if (
        next_direction_payload.get("recommended_next_research_task")
        != SOURCE_2384_EXPECTED_ROUTE
    ):
        errors.append("2384 next direction must route to TRADING-2385")
    if next_direction_payload.get("candidate_pool_expansion_recommended") is not True:
        errors.append("2384 next direction must recommend candidate pool expansion")
    if (
        next_direction_payload.get("signal_family_diversification_recommended")
        is not True
    ):
        errors.append("2384 next direction must recommend signal family diversity")

    guarded_variant_retest = _as_mapping(sources.get("guarded_variant_retest"))
    if guarded_variant_retest.get("recommended_next_research_task") != (
        SOURCE_2383_EXPECTED_ROUTE
    ):
        errors.append("2383 guarded retest must route to TRADING-2384")
    if guarded_variant_retest.get("data_quality_gate_executed") is not True:
        errors.append("2383 data_quality_gate_executed must be true")
    if guarded_variant_retest.get("data_quality_passed") is not True:
        errors.append("2383 data_quality_passed must be true")
    if (
        _find_nested_bool(
            guarded_variant_retest,
            "candidate_ready_for_research_only_observation",
        )
        is not False
    ):
        errors.append("2383 candidate_ready_for_research_only_observation is not false")

    variant_retest = _as_mapping(sources.get("variant_retest"))
    if variant_retest.get("recommended_next_research_task") != (
        SOURCE_2379_EXPECTED_ROUTE
    ):
        errors.append("2379 variant retest must route to TRADING-2380")
    if variant_retest.get("data_quality_gate_executed") is not True:
        errors.append("2379 data_quality_gate_executed must be true")
    if variant_retest.get("data_quality_passed") is not True:
        errors.append("2379 data_quality_passed must be true")
    if (
        _find_nested_bool(
            variant_retest,
            "candidate_ready_for_research_only_observation",
        )
        is not False
    ):
        errors.append("2379 candidate_ready_for_research_only_observation is not false")

    candidate_ranking = _as_mapping(sources.get("candidate_ranking"))
    if not isinstance(candidate_ranking.get("candidate_ranking"), list):
        errors.append("2365 candidate_ranking must be present")
    if _top_candidate_from_candidate_ranking(candidate_ranking) != RANKING_TOP_CANDIDATE:
        errors.append("2365 top candidate must remain ranking top reference")

    if sources.get("sensitivity_matrix_count", 0) <= 0:
        errors.append("2366 sensitivity_result.sensitivity_matrix must be non-empty")
    sensitivity_decision_update = _as_mapping(
        sources.get("sensitivity_decision_update")
    )
    if not isinstance(sensitivity_decision_update.get("decision_update"), Mapping):
        errors.append("2366 sensitivity decision_update must be present")

    for source_name in status_expectations:
        document = _as_mapping(sources.get(source_name))
        for field in SAFETY_FALSE_FIELDS:
            if document.get(field) is True:
                errors.append(f"{source_name}.{field} must remain false")
        if document.get("broker_action") not in {None, "none"}:
            errors.append(f"{source_name}.broker_action must remain none")
        if document.get("production_effect") not in {None, "none"}:
            errors.append(f"{source_name}.production_effect must remain none")
    return errors


def _candidate_pool_expansion_plan(
    *,
    sources: Mapping[str, Any],
    family_plan: Mapping[str, Any],
    selected_candidates: list[dict[str, Any]],
    ready: bool,
) -> dict[str, Any]:
    deferred_candidates = [
        {
            "family_id": family["family_id"],
            "candidate_id": candidate,
            "defer_reason": "default_2386_budget_caps_new_candidates_at_12",
        }
        for family in family_plan.get("signal_family_details", [])
        if isinstance(family, Mapping)
        for candidate in family.get("candidate_templates", [])[2:]
    ]
    return {
        "schema_version": "dynamic_strategy_candidate_pool_expansion_plan.v1",
        "status": READY_STATUS if ready else BLOCKED_SOURCE_STATUS,
        "candidate_pool_expansion_plan_ready": ready,
        "candidate_pool_expansion_recommended": True,
        "signal_family_diversification_recommended": True,
        "owner_decision_from_2384": sources.get("owner_decision_from_2384"),
        "why_local_optimization_is_no_longer_enough": list(
            LOCAL_OPTIMIZATION_LIMITATIONS
        ),
        "existing_candidate_line_recap": {
            "lower_turnover_line": {
                "source_tasks": ["TRADING-2379", "TRADING-2380"],
                "base_candidate": sources.get("lower_turnover_base_candidate"),
                "best_variant": sources.get("lower_turnover_best_variant"),
                "decision": sources.get("lower_turnover_decision"),
                "observation_approved": False,
                "local_optimization_allowed": False,
            },
            "ranking_top_guarded_line": {
                "source_tasks": ["TRADING-2383", "TRADING-2384"],
                "base_candidate": sources.get("ranking_top_candidate"),
                "best_variant": sources.get("best_guarded_variant"),
                "decision": sources.get("best_guarded_variant_decision"),
                "observation_approved": False,
                "local_optimization_allowed": False,
            },
        },
        "reference_candidates": list(REFERENCE_CANDIDATE_IDS),
        "reference_candidate_details": _reference_candidate_details(),
        "signal_families": list(SIGNAL_FAMILY_IDS),
        "signal_family_details": list(family_plan.get("signal_family_details", [])),
        "new_candidates_selected_for_2386": selected_candidates,
        "new_candidate_ids_selected_for_2386": [
            candidate["candidate_id"] for candidate in selected_candidates
        ],
        "deferred_candidate_templates": deferred_candidates,
        "candidate_budget": dict(CANDIDATE_BUDGET),
        "recommended_next_research_task": NEXT_ROUTE,
    }


def _signal_family_diversification_plan(*, ready: bool) -> dict[str, Any]:
    details = [
        {
            "family_id": "regime_transition_family",
            "purpose": (
                "improve behavior around risk-off to recovery transitions"
            ),
            "candidate_templates": [
                "dynamic_regime_reentry_accelerated_v1",
                "dynamic_regime_recovery_confirmation_v1",
                "dynamic_regime_risk_off_exit_decay_v1",
            ],
            "hypotheses": [
                "recovery re-entry lag is a major source of return gap",
                "controlled re-entry can improve upside without exploding turnover",
                "risk-off exit should be gradual but not excessively slow",
            ],
            "required_guardrails": [
                "valid_until_window",
                "max_single_step_weight_delta",
                "cooldown_limited_event_driven_comparison",
                "risk_cap_preserved",
            ],
            "must_use_valid_until_window": True,
        },
        {
            "family_id": "trend_confirmation_family",
            "purpose": "allow growth tilt only under confirmed trend conditions",
            "candidate_templates": [
                "dynamic_trend_confirmed_growth_tilt_v1",
                "dynamic_trend_confirmed_low_turnover_v1",
                "dynamic_ai_trend_confirmed_guarded_v1",
            ],
            "hypotheses": [
                "ranking top return advantage may come from growth tilt",
                "growth tilt should be gated by trend confirmation",
                "false risk-on can be reduced with confirmation filters",
            ],
            "required_guardrails": [
                "no_stale_signal_execution",
                "volatility_state_check",
                "turnover_cap",
                "conservative_cost_stress",
            ],
            "must_use_valid_until_window": True,
        },
        {
            "family_id": "volatility_aware_family",
            "purpose": (
                "improve high-volatility drawdown behavior while preserving "
                "low-volatility upside"
            ),
            "candidate_templates": [
                "dynamic_volatility_scaled_growth_tilt_v1",
                "dynamic_volatility_floor_adjusted_v1",
                "dynamic_high_vol_risk_cap_strict_v1",
            ],
            "hypotheses": [
                "high volatility phases need stricter risk cap",
                "low volatility phases can allow modest upside capture",
                "volatility-aware scaling may reduce drawdown without fully "
                "suppressing returns",
            ],
            "required_guardrails": [
                "high_volatility_slice_test",
                "low_volatility_slice_test",
                "drawdown_gap_check",
                "cost_adjusted_result",
            ],
            "must_use_valid_until_window": True,
        },
        {
            "family_id": "signal_age_valid_until_family",
            "purpose": "tune signal decay and expiry behavior",
            "candidate_templates": [
                "dynamic_signal_age_decay_v1",
                "dynamic_valid_until_expiry_strict_v1",
                "dynamic_valid_until_near_expiry_de_risk_v1",
            ],
            "hypotheses": [
                "valid-until window is necessary but may need decay tuning",
                "near-expiry signals should have reduced weight impact",
                "stale signal carry-forward must remain forbidden",
            ],
            "required_guardrails": [
                "no_signal_after_expiry",
                "stale_signal_execution_count",
                "valid_until_window_preserved",
                "signal_to_execution_lag_days",
            ],
            "must_use_valid_until_window": True,
        },
        {
            "family_id": "turnover_budget_family",
            "purpose": "convert turnover control into an explicit optimization constraint",
            "candidate_templates": [
                "dynamic_turnover_budgeted_growth_tilt_v1",
                "dynamic_turnover_budgeted_regime_overlay_v1",
                "dynamic_turnover_budgeted_reentry_v1",
            ],
            "hypotheses": [
                "turnover budget may improve robustness better than ad-hoc cooldown",
                "explicit turnover caps can preserve cost-adjusted edge",
                "dynamic budget allocation can allow selective high-conviction changes",
            ],
            "required_guardrails": [
                "max_turnover_per_month",
                "max_single_step_weight_delta",
                "transaction_cost_drag",
                "turnover_adjusted_score",
            ],
            "must_use_valid_until_window": True,
        },
        {
            "family_id": "risk_cap_interaction_family",
            "purpose": (
                "test whether risk cap is too strict or too loose under different "
                "signals"
            ),
            "candidate_templates": [
                "dynamic_risk_cap_adaptive_v1",
                "dynamic_risk_cap_trend_conditioned_v1",
                "dynamic_risk_cap_recovery_relaxed_v1",
            ],
            "hypotheses": [
                "current risk cap may suppress recovery upside",
                "adaptive risk cap may preserve downside protection while improving "
                "re-entry",
                "trend-conditioned risk cap may reduce false risk-off",
            ],
            "required_guardrails": [
                "drawdown_not_materially_worse",
                "high_volatility_behavior_not_degraded",
                "downside_capture_check",
                "owner_review_trigger_if_drawdown_worse",
            ],
            "must_use_valid_until_window": True,
        },
    ]
    return {
        "schema_version": "dynamic_strategy_signal_family_diversification_plan.v1",
        "status": READY_STATUS if ready else BLOCKED_SOURCE_STATUS,
        "signal_family_diversification_plan_ready": ready,
        "signal_family_diversification_recommended": True,
        "signal_families": list(SIGNAL_FAMILY_IDS),
        "signal_family_details": details,
        "family_count": len(details),
        "all_families_require_hypotheses": True,
        "all_families_preserve_valid_until_window": True,
        "recommended_next_research_task": NEXT_ROUTE,
    }


def _selected_new_candidates_for_2386(
    family_plan: Mapping[str, Any],
) -> list[dict[str, Any]]:
    selected: list[dict[str, Any]] = []
    for family in family_plan.get("signal_family_details", []):
        if not isinstance(family, Mapping):
            continue
        family_id = str(family.get("family_id"))
        templates = family.get("candidate_templates", [])
        if not isinstance(templates, list):
            continue
        guardrails = list(family.get("required_guardrails", []))
        for candidate_id in templates[:2]:
            selected.append(
                {
                    "candidate_id": candidate_id,
                    "family_id": family_id,
                    "source_task": TASK_ID,
                    "selection_role": "default_2386_screening_candidate",
                    "primary_execution_cadence": PRIMARY_EXECUTION_CADENCE,
                    "required_guardrails": guardrails,
                    "hypothesis_source": "family_pre_registered_hypothesis",
                }
            )
    return selected[: CANDIDATE_BUDGET["max_total_new_candidates_for_2386"]]


def _candidate_budget_guardrails(
    *,
    family_plan: Mapping[str, Any],
    selected_candidates: list[dict[str, Any]],
    ready: bool,
) -> dict[str, Any]:
    family_details = [
        family
        for family in family_plan.get("signal_family_details", [])
        if isinstance(family, Mapping)
    ]
    selected_by_family = {
        family["family_id"]: [
            candidate["candidate_id"]
            for candidate in selected_candidates
            if candidate["family_id"] == family["family_id"]
        ]
        for family in family_details
    }
    budget_check = {
        "new_candidate_family_count": len(family_details),
        "selected_new_candidate_count": len(selected_candidates),
        "max_new_candidate_families": CANDIDATE_BUDGET[
            "max_new_candidate_families"
        ],
        "max_total_new_candidates_for_2386": CANDIDATE_BUDGET[
            "max_total_new_candidates_for_2386"
        ],
        "within_family_budget": all(
            len(candidates) <= CANDIDATE_BUDGET["max_candidates_per_family"]
            for candidates in selected_by_family.values()
        ),
        "within_total_budget": len(selected_candidates)
        <= CANDIDATE_BUDGET["max_total_new_candidates_for_2386"],
        "at_least_one_candidate_per_family": all(
            len(candidates) >= 1 for candidates in selected_by_family.values()
        ),
    }
    return {
        "schema_version": "dynamic_strategy_candidate_budget_guardrails.v1",
        "status": READY_STATUS if ready else BLOCKED_SOURCE_STATUS,
        "candidate_budget_ready": ready,
        "anti_overfit_guardrails_ready": ready,
        "candidate_budget": dict(CANDIDATE_BUDGET),
        "budget_check": budget_check,
        "selected_candidates_by_family": selected_by_family,
        "forbidden_paths": list(FORBIDDEN_PATHS),
        "anti_overfit_guardrails": list(ANTIOVERFIT_GUARDRAILS),
        "post_hoc_metric_cherry_picking_forbidden": True,
        "unbounded_parameter_grid_forbidden": True,
        "monthly_rebalance_primary_decision_forbidden": True,
        "recommended_next_research_task": NEXT_ROUTE,
    }


def _retest_plan_2386(
    *,
    selected_candidates: list[dict[str, Any]],
    budget_guardrails: Mapping[str, Any],
    ready: bool,
) -> dict[str, Any]:
    return {
        "schema_version": "dynamic_strategy_2386_retest_plan.v1",
        "status": READY_STATUS if ready else BLOCKED_SOURCE_STATUS,
        "retest_plan_2386_ready": ready,
        "required_candidates": {
            "reference": list(REFERENCE_CANDIDATE_IDS),
            "new_candidates": [
                candidate["candidate_id"] for candidate in selected_candidates
            ],
            "new_candidate_selection_policy": (
                "at_least_one_from_each_signal_family_no_more_than_budget"
            ),
        },
        "candidate_count": {
            "reference_candidate_count": len(REFERENCE_CANDIDATE_IDS),
            "new_candidate_count": len(selected_candidates),
            "total_candidate_count": len(REFERENCE_CANDIDATE_IDS)
            + len(selected_candidates),
        },
        "candidate_budget_guardrails": dict(budget_guardrails),
        "primary_execution_cadence": PRIMARY_EXECUTION_CADENCE,
        "comparison_cadences": [
            "valid_until_window",
            "cooldown_limited_event_driven",
            "signal_event_driven",
        ],
        "monthly_rebalance": {
            "allowed_for_reference": True,
            "allowed_for_primary_decision": False,
        },
        "stress_tests": {
            "cost_stress": ["base", "realistic", "conservative", "harsh"],
            "time_slices": [
                "full_available_window",
                "recent_period",
                "post_2023_ai_cycle",
                "high_volatility_periods",
                "drawdown_recovery_periods",
            ],
            "regime_slices": [
                "risk_on",
                "risk_off",
                "high_volatility",
                "low_volatility",
                "trend_confirmed",
                "recovery",
            ],
            "turnover_constraints": [
                "max_turnover_per_month",
                "max_single_step_weight_delta",
                "cooldown_days",
            ],
        },
        "acceptance_criteria": _acceptance_criteria(),
        "explicit_non_goals": list(NON_APPROVED_PATHS),
        "recommended_next_research_task": NEXT_ROUTE,
    }


def _base_payload(
    *,
    status: str,
    as_of_date: date,
    sources: Mapping[str, Any],
    candidate_plan: Mapping[str, Any],
    family_plan: Mapping[str, Any],
    budget_guardrails: Mapping[str, Any],
    retest_plan: Mapping[str, Any],
    ready: bool,
) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "task_id": TASK_ID,
        "task_register_id": TASK_REGISTER_ID,
        "report_type": REPORT_TYPE,
        "status": status,
        "generated_at": utc_now_iso(),
        "as_of": as_of_date.isoformat(),
        "market_regime": AI_REGIME_SUMMARY.get("market_regime", "ai_after_chatgpt"),
        "market_regime_summary": AI_REGIME_SUMMARY,
        "source_tasks": list(SOURCE_TASKS),
        "source_files": dict(_as_mapping(sources.get("source_files"))),
        "source_hashes": dict(_as_mapping(sources.get("source_hashes"))),
        "source_status": dict(_as_mapping(sources.get("source_status"))),
        "source_validation_errors": list(sources.get("source_validation_errors", [])),
        "source_ready_for_candidate_pool_expansion_plan": ready,
        "data_quality_gate_executed": False,
        "data_quality_gate_reason": DATA_QUALITY_GATE_REASON,
        "fresh_market_data_read": False,
        "backtest_run": False,
        "new_signal_generated": False,
        "scoring_run": False,
        "research_only": True,
        "observe_only": True,
        "owner_decision_from_2384": sources.get("owner_decision_from_2384"),
        "candidate_pool_expansion_recommended": True,
        "signal_family_diversification_recommended": True,
        "candidate_pool_expansion_plan_ready": ready,
        "candidate_pool_expansion_plan": dict(candidate_plan),
        "signal_family_diversification_plan_ready": ready,
        "signal_family_diversification_plan": dict(family_plan),
        "candidate_budget_ready": ready,
        "anti_overfit_guardrails_ready": ready,
        "candidate_budget_guardrails": dict(budget_guardrails),
        "retest_plan_2386_ready": ready,
        "retest_plan_2386": dict(retest_plan),
        "primary_execution_cadence": sources.get("primary_execution_cadence"),
        "monthly_rebalance": dict(_as_mapping(retest_plan.get("monthly_rebalance"))),
        "reference_candidates": list(REFERENCE_CANDIDATE_IDS),
        "reference_candidate_details": _reference_candidate_details(),
        "signal_families": list(SIGNAL_FAMILY_IDS),
        "signal_family_details": list(family_plan.get("signal_family_details", [])),
        "new_candidates_for_2386": list(
            candidate_plan.get("new_candidate_ids_selected_for_2386", [])
        ),
        "recommended_next_research_task": NEXT_ROUTE,
        "manual_review_required": True,
        "promotion_allowed": False,
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "production_effect": "none",
        "broker_action": "none",
        "scheduler_enabled": False,
        "scheduled_task_created": False,
        "event_append_enabled": False,
        "historical_event_log_mutated": False,
        "outcome_binding_enabled": False,
        "outcome_store_mutated": False,
        "paper_shadow_enabled": False,
        "paper_trade_created": False,
        "shadow_position_created": False,
        "production_enabled": False,
        "broker_action_enabled": False,
        "order_generated": False,
        "daily_report_generated": False,
        "explicit_non_approval_list": list(NON_APPROVED_PATHS),
        "summary_findings": [
            (
                "2384 owner decision rejects observation and requires candidate "
                "pool expansion."
            ),
            (
                f"2386 default plan keeps {len(REFERENCE_CANDIDATE_IDS)} "
                "reference candidates and "
                f"{len(candidate_plan.get('new_candidate_ids_selected_for_2386', []))} "
                "new candidates."
            ),
            (
                "valid_until_window remains the primary execution cadence; "
                "monthly rebalance is comparison-only."
            ),
        ],
        "artifact_paths": {},
    }


def _write_outputs(
    *,
    payload: dict[str, Any],
    output_root: Path,
    docs_root: Path,
) -> None:
    output_root.mkdir(parents=True, exist_ok=True)
    docs_root.mkdir(parents=True, exist_ok=True)
    plan_json = output_root / "candidate_pool_expansion_plan.json"
    family_json = output_root / "signal_family_diversification_plan.json"
    budget_json = output_root / "candidate_budget_guardrails.json"
    retest_json = output_root / "retest_plan_2386.json"
    markdown_path = docs_root / "dynamic_strategy_candidate_pool_expansion_plan.md"
    family_markdown = docs_root / "dynamic_strategy_signal_family_diversification_plan.md"
    budget_markdown = (
        docs_root / "dynamic_strategy_candidate_budget_and_anti_overfit_guardrails.md"
    )
    route_markdown = docs_root / "dynamic_strategy_2386_route.md"
    payload["artifact_paths"] = {
        "json_path": str(plan_json),
        "candidate_pool_expansion_plan_json": str(plan_json),
        "signal_family_diversification_plan_json": str(family_json),
        "candidate_budget_guardrails_json": str(budget_json),
        "retest_plan_2386_json": str(retest_json),
        "markdown_path": str(markdown_path),
        "signal_family_diversification_markdown": str(family_markdown),
        "candidate_budget_guardrails_markdown": str(budget_markdown),
        "next_route_markdown": str(route_markdown),
    }
    plan_json.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    family_json.write_text(
        json.dumps(
            {
                "generated_at": payload["generated_at"],
                "report_type": REPORT_TYPE,
                "schema_version": (
                    "dynamic_strategy_signal_family_diversification_plan.v1"
                ),
                "status": payload["status"],
                "production_effect": "none",
                "broker_action": "none",
                "signal_family_diversification_plan": payload[
                    "signal_family_diversification_plan"
                ],
                "signal_families": payload["signal_families"],
                "recommended_next_research_task": NEXT_ROUTE,
            },
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    budget_json.write_text(
        json.dumps(
            {
                "generated_at": payload["generated_at"],
                "report_type": REPORT_TYPE,
                "schema_version": "dynamic_strategy_candidate_budget_guardrails.v1",
                "status": payload["status"],
                "production_effect": "none",
                "broker_action": "none",
                "candidate_budget_guardrails": payload["candidate_budget_guardrails"],
                "recommended_next_research_task": NEXT_ROUTE,
            },
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    retest_json.write_text(
        json.dumps(
            {
                "generated_at": payload["generated_at"],
                "report_type": REPORT_TYPE,
                "schema_version": "dynamic_strategy_2386_retest_plan.v1",
                "status": payload["status"],
                "production_effect": "none",
                "broker_action": "none",
                "retest_plan_2386": payload["retest_plan_2386"],
                "recommended_next_research_task": NEXT_ROUTE,
            },
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    markdown_path.write_text(_main_markdown(payload), encoding="utf-8")
    family_markdown.write_text(_family_markdown(payload), encoding="utf-8")
    budget_markdown.write_text(_budget_markdown(payload), encoding="utf-8")
    route_markdown.write_text(_route_markdown(payload), encoding="utf-8")


def _main_markdown(payload: Mapping[str, Any]) -> str:
    candidate_plan = _as_mapping(payload.get("candidate_pool_expansion_plan"))
    line_recap = _as_mapping(candidate_plan.get("existing_candidate_line_recap"))
    max_total_new_candidates = CANDIDATE_BUDGET[
        "max_total_new_candidates_for_2386"
    ]
    return "\n".join(
        [
            "# Dynamic strategy candidate pool expansion plan",
            "",
            "## Executive summary",
            "",
            f"- status：`{payload['status']}`",
            f"- owner decision from 2384：`{payload['owner_decision_from_2384']}`",
            "- candidate pool expansion recommended：`true`",
            "- signal family diversification recommended：`true`",
            f"- primary execution cadence：`{payload['primary_execution_cadence']}`",
            "- monthly rebalance primary decision：`false`",
            f"- next route：`{payload['recommended_next_research_task']}`",
            "",
            "## Why local optimization is no longer enough",
            "",
            _markdown_bullets(LOCAL_OPTIMIZATION_LIMITATIONS),
            "",
            "## Existing candidate line recap",
            "",
            _candidate_line_markdown(line_recap),
            "",
            "## Candidate pool expansion plan",
            "",
            f"- reference candidates：`{len(payload['reference_candidates'])}`",
            f"- signal families：`{len(payload['signal_families'])}`",
            f"- new candidates selected for 2386：`{len(payload['new_candidates_for_2386'])}`",
            "",
            "## Signal family diversification plan",
            "",
            _family_summary_markdown(payload),
            "",
            "## Candidate budget and anti-overfit guardrails",
            "",
            f"- max total new candidates for 2386：`{max_total_new_candidates}`",
            "- post-hoc metric cherry-picking forbidden：`true`",
            "- unbounded parameter grid forbidden：`true`",
            "",
            "## 2386 retest plan",
            "",
            "- required reference candidates：",
            _markdown_bullets(payload["reference_candidates"]),
            "- required new candidates：",
            _markdown_bullets(payload["new_candidates_for_2386"]),
            "",
            "## Acceptance criteria",
            "",
            _acceptance_markdown(),
            "",
            "## Explicit non-goals",
            "",
            _markdown_bullets(NON_APPROVED_PATHS),
            "",
            "## Data quality gate boundary",
            "",
            "- data_quality_gate_executed：`false`",
            f"- data_quality_gate_reason：`{DATA_QUALITY_GATE_REASON}`",
            "- reason：本任务只读取 prior artifacts，不读取 fresh cached market data，"
            "不重新 backtest，不生成新 signal / scoring / daily report。",
            "",
            "## Recommended next route",
            "",
            f"`{NEXT_ROUTE}`",
            "",
        ]
    )


def _family_markdown(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# Dynamic strategy signal family diversification plan",
            "",
            "## Summary",
            "",
            f"- status：`{payload['status']}`",
            "- all families preserve `valid_until_window`：`true`",
            "- all families require pre-registered hypotheses：`true`",
            "",
            "## Signal families",
            "",
            _family_detail_markdown(payload),
            "",
            "## Next route",
            "",
            f"`{NEXT_ROUTE}`",
            "",
        ]
    )


def _budget_markdown(payload: Mapping[str, Any]) -> str:
    budget = _as_mapping(payload.get("candidate_budget_guardrails"))
    budget_check = _as_mapping(budget.get("budget_check"))
    max_new_families = CANDIDATE_BUDGET["max_new_candidate_families"]
    max_per_family = CANDIDATE_BUDGET["max_candidates_per_family"]
    max_total_new_candidates = CANDIDATE_BUDGET[
        "max_total_new_candidates_for_2386"
    ]
    return "\n".join(
        [
            "# Dynamic strategy candidate budget and anti-overfit guardrails",
            "",
            "## Budget",
            "",
            f"- max new candidate families：`{max_new_families}`",
            f"- max candidates per family：`{max_per_family}`",
            f"- max total new candidates for 2386：`{max_total_new_candidates}`",
            f"- selected new candidates：`{budget_check.get('selected_new_candidate_count')}`",
            f"- within total budget：`{str(budget_check.get('within_total_budget')).lower()}`",
            "",
            "## Anti-overfit guardrails",
            "",
            _markdown_bullets(ANTIOVERFIT_GUARDRAILS),
            "",
            "## Forbidden paths",
            "",
            _markdown_bullets(FORBIDDEN_PATHS),
            "",
        ]
    )


def _route_markdown(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# Dynamic strategy 2386 route",
            "",
            f"- current task：`{TASK_REGISTER_ID}`",
            f"- status：`{payload['status']}`",
            f"- next route：`{NEXT_ROUTE}`",
            "- next route type：expanded candidate pool retest and screening",
            "- primary execution cadence：`valid_until_window`",
            "- monthly rebalance primary decision：`false`",
            "- scheduler enabled：`false`",
            "- event append enabled：`false`",
            "- outcome binding enabled：`false`",
            "- paper shadow enabled：`false`",
            "- production enabled：`false`",
            "- broker action enabled：`false`",
            "- daily report generated：`false`",
            "",
            "TRADING-2386 只能在 cached-data quality gate 通过后执行 expanded "
            "candidate pool retest；2385 本身不批准 observation、paper-shadow、"
            "production 或 broker/order。",
            "",
        ]
    )


def _reference_candidate_details() -> list[dict[str, Any]]:
    return [
        {
            "candidate_id": "static_baseline",
            "source_task": "STATIC_BASELINE",
            "role": "baseline_reference",
        },
        {
            "candidate_id": RANKING_TOP_CANDIDATE,
            "source_task": "TRADING-2365",
            "role": "return_advantage_reference",
        },
        {
            "candidate_id": BASE_CANDIDATE_ID,
            "source_task": "TRADING-2376",
            "role": "robustness_reference",
        },
        {
            "candidate_id": BEST_LOWER_TURNOVER_VARIANT,
            "source_task": "TRADING-2379",
            "role": "robust_variant_reference",
        },
        {
            "candidate_id": BEST_GUARDED_VARIANT,
            "source_task": "TRADING-2383",
            "role": "guarded_return_reference",
        },
    ]


def _acceptance_criteria() -> dict[str, list[str]]:
    return {
        "must": [
            "cost_adjusted_return_above_static",
            "survives_realistic_cost",
            "survives_conservative_cost",
            "valid_until_window_preserved",
            "no_stale_signal_carry_forward",
            "turnover_within_budget",
            "max_drawdown_not_materially_worse_than_reference",
        ],
        "should": [
            "improve_return_vs_lower_turnover_reference",
            "reduce_turnover_vs_original_ranking_top",
            "improve_time_slice_pass_rate",
            "improve_regime_slice_pass_rate",
            "preserve_most_of_ranking_top_upside",
        ],
        "must_not": [
            "rely_on_monthly_rebalance",
            "require_scheduler",
            "require_event_append",
            "require_outcome_binding",
            "require_paper_shadow",
            "require_production_or_broker",
        ],
    }


def _acceptance_markdown() -> str:
    criteria = _acceptance_criteria()
    return "\n".join(
        [
            "- must：",
            _markdown_bullets(criteria["must"]),
            "- should：",
            _markdown_bullets(criteria["should"]),
            "- must_not：",
            _markdown_bullets(criteria["must_not"]),
        ]
    )


def _candidate_line_markdown(line_recap: Mapping[str, Any]) -> str:
    lower = _as_mapping(line_recap.get("lower_turnover_line"))
    ranking = _as_mapping(line_recap.get("ranking_top_guarded_line"))
    return "\n".join(
        [
            f"- lower-turnover base：`{lower.get('base_candidate')}`",
            f"- lower-turnover best variant：`{lower.get('best_variant')}`",
            f"- lower-turnover decision：`{lower.get('decision')}`",
            "- lower-turnover observation approved：`false`",
            f"- ranking-top guarded base：`{ranking.get('base_candidate')}`",
            f"- ranking-top guarded best variant：`{ranking.get('best_variant')}`",
            f"- ranking-top guarded decision：`{ranking.get('decision')}`",
            "- ranking-top guarded observation approved：`false`",
        ]
    )


def _family_summary_markdown(payload: Mapping[str, Any]) -> str:
    families = payload.get("signal_family_details", [])
    if not isinstance(families, list):
        return "- 无"
    return "\n".join(
        f"- `{family.get('family_id')}`：{family.get('purpose')}"
        for family in families
        if isinstance(family, Mapping)
    )


def _family_detail_markdown(payload: Mapping[str, Any]) -> str:
    families = payload.get("signal_family_details", [])
    if not isinstance(families, list):
        return "- 无"
    lines: list[str] = []
    for family in families:
        if not isinstance(family, Mapping):
            continue
        lines.extend(
            [
                f"### {family.get('family_id')}",
                "",
                f"- purpose：{family.get('purpose')}",
                "- candidate templates：",
                _markdown_bullets(family.get("candidate_templates", [])),
                "- hypotheses：",
                _markdown_bullets(family.get("hypotheses", [])),
                "- required guardrails：",
                _markdown_bullets(family.get("required_guardrails", [])),
                "",
            ]
        )
    return "\n".join(lines)


def _load_json_document(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Required source artifact not found: {path}")
    document = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(document, dict):
        raise ValueError(f"Source artifact must be a JSON object: {path}")
    return document


def _resolve_as_of(as_of_date: date | None, sources: Mapping[str, Any]) -> date:
    if as_of_date is not None:
        return as_of_date
    for source_name in (
        "owner_review",
        "guarded_variant_retest",
        "variant_retest",
        "sensitivity_result",
    ):
        source = _as_mapping(sources.get(source_name))
        value = source.get("as_of")
        if isinstance(value, str) and value:
            return date.fromisoformat(value)
    return date.today()


def _extract_next_direction(next_direction_document: Mapping[str, Any]) -> str | None:
    next_direction = _as_mapping(
        next_direction_document.get("next_research_direction_decision")
    )
    return _coalesce_string(
        next_direction.get("next_direction"),
        next_direction.get("recommended_default_direction"),
    )


def _top_candidate_from_candidate_ranking(
    candidate_ranking_document: Mapping[str, Any],
) -> str | None:
    ranking = candidate_ranking_document.get("candidate_ranking")
    if not isinstance(ranking, list) or not ranking:
        return None
    first = ranking[0]
    if not isinstance(first, Mapping):
        return None
    return _coalesce_string(first.get("candidate_id"), first.get("strategy_id"))


def _top_candidate_primary_cadence(
    candidate_ranking_document: Mapping[str, Any],
) -> str | None:
    ranking = candidate_ranking_document.get("candidate_ranking")
    if not isinstance(ranking, list) or not ranking:
        return None
    first = ranking[0]
    if not isinstance(first, Mapping):
        return None
    return _coalesce_string(first.get("primary_execution_cadence"))


def _sensitivity_primary_cadence(document: Mapping[str, Any]) -> str | None:
    decision_update = _as_mapping(document.get("decision_update"))
    combined = decision_update.get("combined_stress_results")
    if not isinstance(combined, list):
        combined = document.get("combined_stress_results")
    if not isinstance(combined, list):
        return None
    for row in combined:
        if not isinstance(row, Mapping):
            continue
        cadence = _coalesce_string(row.get("primary_execution_cadence"))
        if cadence:
            return cadence
    return None


def _find_nested_bool(document: Mapping[str, Any], field: str) -> bool | None:
    value = document.get(field)
    if isinstance(value, bool):
        return value
    for nested in document.values():
        if isinstance(nested, Mapping):
            found = _find_nested_bool(nested, field)
            if isinstance(found, bool):
                return found
        elif isinstance(nested, list):
            for item in nested:
                if isinstance(item, Mapping):
                    found = _find_nested_bool(item, field)
                    if isinstance(found, bool):
                        return found
    return None


def _expect_equal(
    errors: list[str],
    field: str,
    actual: object,
    expected: object,
) -> None:
    if actual != expected:
        errors.append(f"{field} expected {expected!r}, got {actual!r}")


def _coalesce_string(*values: object) -> str | None:
    for value in values:
        if isinstance(value, str) and value:
            return value
    return None


def _as_mapping(value: object) -> Mapping[str, Any]:
    if isinstance(value, Mapping):
        return value
    return {}


def _markdown_bullets(values: object) -> str:
    if not isinstance(values, list | tuple):
        return "- 无"
    return "\n".join(f"- `{item}`" for item in values)
