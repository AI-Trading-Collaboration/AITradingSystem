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
)
from ai_trading_system.dynamic_strategy_cost_turnover_cooldown_sensitivity import (
    READY_STATUS as SOURCE_2366_READY_STATUS,
)
from ai_trading_system.dynamic_strategy_event_driven_retest import (
    READY_STATUS as SOURCE_2365_READY_STATUS,
)
from ai_trading_system.dynamic_strategy_optimization_plateau_next_candidate_decision import (
    DEFAULT_DYNAMIC_STRATEGY_OPTIMIZATION_PLATEAU_NEXT_CANDIDATE_DECISION_OUTPUT_ROOT,
)
from ai_trading_system.dynamic_strategy_optimization_plateau_next_candidate_decision import (
    NEXT_DIRECTION_DECISION as SOURCE_2381_NEXT_DIRECTION,
)
from ai_trading_system.dynamic_strategy_optimization_plateau_next_candidate_decision import (
    NEXT_ROUTE as SOURCE_2381_EXPECTED_ROUTE,
)
from ai_trading_system.dynamic_strategy_optimization_plateau_next_candidate_decision import (
    READY_STATUS as SOURCE_2381_READY_STATUS,
)
from ai_trading_system.dynamic_strategy_optimized_variant_owner_review_decision import (
    DEFAULT_DYNAMIC_STRATEGY_OPTIMIZED_VARIANT_OWNER_REVIEW_DECISION_OUTPUT_ROOT,
)
from ai_trading_system.dynamic_strategy_optimized_variant_owner_review_decision import (
    READY_STATUS as SOURCE_2380_READY_STATUS,
)
from ai_trading_system.dynamic_strategy_slice_robustness_optimized_variant_retest import (
    BASE_CANDIDATE_ID,
    DEFAULT_DYNAMIC_STRATEGY_SLICE_ROBUSTNESS_OPTIMIZED_VARIANT_RETEST_OUTPUT_ROOT,
    RANKING_TOP_REFERENCE,
)
from ai_trading_system.dynamic_strategy_slice_robustness_optimized_variant_retest import (
    READY_STATUS as SOURCE_2379_READY_STATUS,
)
from ai_trading_system.execution_semantics import AI_REGIME_SUMMARY, _file_sha256

TASK_ID = "TRADING-2382"
TASK_REGISTER_ID = (
    "TRADING-2382_DYNAMIC_STRATEGY_RANKING_TOP_GUARDED_TURNOVER_RETEST_PLAN"
)
REPORT_TYPE = "dynamic_strategy_ranking_top_guarded_turnover_retest_plan"
SCHEMA_VERSION = "dynamic_strategy_ranking_top_guarded_turnover_retest_plan.v1"
READY_STATUS = "DYNAMIC_STRATEGY_RANKING_TOP_GUARDED_TURNOVER_RETEST_PLAN_READY"
BLOCKED_SOURCE_STATUS = (
    "DYNAMIC_STRATEGY_RANKING_TOP_GUARDED_TURNOVER_RETEST_PLAN_BLOCKED_SOURCE_"
    "ARTIFACT"
)
DATA_QUALITY_GATE_REASON = (
    "NOT_APPLICABLE_PRIOR_ARTIFACT_RETEST_PLAN_ONLY_NO_FRESH_MARKET_DATA"
)
PRIMARY_EXECUTION_CADENCE = "valid_until_window"
RANKING_TOP_CANDIDATE = RANKING_TOP_REFERENCE
LOWER_TURNOVER_REFERENCE = BASE_CANDIDATE_ID
BEST_LOWER_TURNOVER_VARIANT = "dynamic_regime_overlay_v0_4_cooldown_balanced_v1"
NEXT_ROUTE = "TRADING-2383_Dynamic_Strategy_Ranking_Top_Guarded_Variant_Retest"
SOURCE_TASKS: tuple[str, ...] = (
    "TRADING-2365",
    "TRADING-2366",
    "TRADING-2379",
    "TRADING-2380",
    "TRADING-2381",
)
GUARDED_VARIANT_IDS: tuple[str, ...] = (
    "equal_risk_growth_tilt_guarded_turnover_v1",
    "equal_risk_growth_tilt_guarded_cooldown_v1",
    "equal_risk_growth_tilt_guarded_risk_cap_v1",
    "equal_risk_growth_tilt_guarded_valid_until_decay_v1",
    "equal_risk_growth_tilt_lower_turnover_fusion_v1",
    "equal_risk_growth_tilt_lower_turnover_conservative_fusion_v1",
)
FORBIDDEN_OPTIMIZATION_PATHS: tuple[str, ...] = (
    "use_monthly_rebalance_as_primary",
    "remove_valid_until_window",
    "allow_stale_signal_carry_forward",
    "increase_growth_tilt_without_risk_cap",
    "remove_cooldown_entirely_as_final_candidate",
    "ignore_transaction_costs",
    "ignore_slippage",
    "optimize_only_for_total_return",
    "accept_variant_that_improves_return_but_materially_worsens_drawdown",
    "accept_variant_that_requires_scheduler_or_paper_shadow",
)
TRANSFERABLE_GUARDRAILS: tuple[dict[str, object], ...] = (
    {
        "guardrail": "valid_until_window",
        "purpose": "prevent_stale_signal_carry_forward",
        "required": True,
    },
    {
        "guardrail": "cooldown_balancing",
        "purpose": "reduce_over_trading_and_noise_response",
        "required": True,
    },
    {
        "guardrail": "max_single_step_weight_delta",
        "purpose": "limit_single_step_weight_jump",
        "required": True,
    },
    {
        "guardrail": "turnover_cap",
        "purpose": "control_monthly_turnover",
        "required": True,
    },
    {
        "guardrail": "risk_cap_preservation",
        "purpose": "avoid_growth_tilt_drawdown_amplification_in_high_risk_regimes",
        "required": True,
    },
    {
        "guardrail": "trend_confirmation_gate",
        "purpose": "allow_higher_growth_tilt_only_when_trend_is_confirmed",
        "required": True,
    },
    {
        "guardrail": "no_stale_signal_execution",
        "purpose": "block_execution_after_signal_expiry",
        "required": True,
    },
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
    "fresh_cached_market_data",
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

DEFAULT_DYNAMIC_STRATEGY_RANKING_TOP_GUARDED_TURNOVER_RETEST_PLAN_OUTPUT_ROOT = (
    PROJECT_ROOT / "outputs" / "research_strategies" / REPORT_TYPE
)
DEFAULT_DYNAMIC_STRATEGY_RANKING_TOP_GUARDED_TURNOVER_RETEST_PLAN_DOCS_ROOT = (
    PROJECT_ROOT / "docs" / "research"
)
DEFAULT_SOURCE_2365_CANDIDATE_RANKING_PATH = DEFAULT_SOURCE_CANDIDATE_RANKING_PATH
DEFAULT_SOURCE_2366_SENSITIVITY_RESULT_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_COST_TURNOVER_COOLDOWN_SENSITIVITY_OUTPUT_ROOT
    / "sensitivity_result.json"
)
DEFAULT_SOURCE_2366_DECISION_UPDATE_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_COST_TURNOVER_COOLDOWN_SENSITIVITY_OUTPUT_ROOT
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
DEFAULT_SOURCE_2380_OWNER_REVIEW_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_OPTIMIZED_VARIANT_OWNER_REVIEW_DECISION_OUTPUT_ROOT
    / "owner_review_decision.json"
)
DEFAULT_SOURCE_2380_OBSERVATION_REJECTION_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_OPTIMIZED_VARIANT_OWNER_REVIEW_DECISION_OUTPUT_ROOT
    / "observation_rejection_rationale.json"
)
DEFAULT_SOURCE_2381_PLATEAU_DECISION_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_OPTIMIZATION_PLATEAU_NEXT_CANDIDATE_DECISION_OUTPUT_ROOT
    / "optimization_plateau_decision.json"
)
DEFAULT_SOURCE_2381_NEXT_DIRECTION_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_OPTIMIZATION_PLATEAU_NEXT_CANDIDATE_DECISION_OUTPUT_ROOT
    / "next_candidate_direction.json"
)


def run_dynamic_strategy_ranking_top_guarded_turnover_retest_plan(
    *,
    source_candidate_ranking_path: Path = DEFAULT_SOURCE_2365_CANDIDATE_RANKING_PATH,
    source_sensitivity_result_path: Path = DEFAULT_SOURCE_2366_SENSITIVITY_RESULT_PATH,
    source_sensitivity_decision_update_path: Path = (
        DEFAULT_SOURCE_2366_DECISION_UPDATE_PATH
    ),
    source_variant_retest_path: Path = DEFAULT_SOURCE_2379_VARIANT_RETEST_PATH,
    source_optimized_variant_ranking_path: Path = (
        DEFAULT_SOURCE_2379_OPTIMIZED_VARIANT_RANKING_PATH
    ),
    source_owner_review_path: Path = DEFAULT_SOURCE_2380_OWNER_REVIEW_PATH,
    source_observation_rejection_path: Path = (
        DEFAULT_SOURCE_2380_OBSERVATION_REJECTION_PATH
    ),
    source_plateau_decision_path: Path = DEFAULT_SOURCE_2381_PLATEAU_DECISION_PATH,
    source_next_direction_path: Path = DEFAULT_SOURCE_2381_NEXT_DIRECTION_PATH,
    output_root: Path = (
        DEFAULT_DYNAMIC_STRATEGY_RANKING_TOP_GUARDED_TURNOVER_RETEST_PLAN_OUTPUT_ROOT
    ),
    docs_root: Path = (
        DEFAULT_DYNAMIC_STRATEGY_RANKING_TOP_GUARDED_TURNOVER_RETEST_PLAN_DOCS_ROOT
    ),
    as_of_date: date | None = None,
) -> dict[str, Any]:
    sources = _load_sources(
        source_candidate_ranking_path=source_candidate_ranking_path,
        source_sensitivity_result_path=source_sensitivity_result_path,
        source_sensitivity_decision_update_path=source_sensitivity_decision_update_path,
        source_variant_retest_path=source_variant_retest_path,
        source_optimized_variant_ranking_path=source_optimized_variant_ranking_path,
        source_owner_review_path=source_owner_review_path,
        source_observation_rejection_path=source_observation_rejection_path,
        source_plateau_decision_path=source_plateau_decision_path,
        source_next_direction_path=source_next_direction_path,
    )
    ready = not sources["source_validation_errors"]
    resolved_as_of = _resolve_as_of(as_of_date, sources["plateau_decision"])
    source_findings = _source_findings(sources)
    fragility = _ranking_top_fragility_diagnosis(source_findings, ready=ready)
    variant_plan = _guarded_variant_plan(source_findings, ready=ready)
    evaluation_plan = _variant_evaluation_plan(ready=ready)
    payload = _base_payload(
        status=READY_STATUS if ready else BLOCKED_SOURCE_STATUS,
        as_of_date=resolved_as_of,
        sources=sources,
        source_findings=source_findings,
        fragility_diagnosis=fragility,
        guarded_variant_plan=variant_plan,
        variant_evaluation_plan=evaluation_plan,
        ready=ready,
    )
    _write_outputs(payload=payload, output_root=output_root, docs_root=docs_root)
    return payload


def _load_sources(
    *,
    source_candidate_ranking_path: Path,
    source_sensitivity_result_path: Path,
    source_sensitivity_decision_update_path: Path,
    source_variant_retest_path: Path,
    source_optimized_variant_ranking_path: Path,
    source_owner_review_path: Path,
    source_observation_rejection_path: Path,
    source_plateau_decision_path: Path,
    source_next_direction_path: Path,
) -> dict[str, Any]:
    candidate_ranking = _load_json_document(source_candidate_ranking_path)
    sensitivity_result = _load_json_document(source_sensitivity_result_path)
    sensitivity_decision_update = _load_json_document(
        source_sensitivity_decision_update_path
    )
    variant_retest = _load_json_document(source_variant_retest_path)
    optimized_variant_ranking = _load_json_document(
        source_optimized_variant_ranking_path
    )
    owner_review = _load_json_document(source_owner_review_path)
    observation_rejection = _load_json_document(source_observation_rejection_path)
    plateau_decision = _load_json_document(source_plateau_decision_path)
    next_direction = _load_json_document(source_next_direction_path)
    source_files = {
        "candidate_ranking": str(source_candidate_ranking_path),
        "sensitivity_result": str(source_sensitivity_result_path),
        "sensitivity_decision_update": str(source_sensitivity_decision_update_path),
        "variant_retest": str(source_variant_retest_path),
        "optimized_variant_ranking": str(source_optimized_variant_ranking_path),
        "owner_review": str(source_owner_review_path),
        "observation_rejection": str(source_observation_rejection_path),
        "plateau_decision": str(source_plateau_decision_path),
        "next_direction": str(source_next_direction_path),
    }
    source_status = {
        "candidate_ranking": _as_mapping(candidate_ranking).get("status"),
        "sensitivity_result": _as_mapping(sensitivity_result).get("status"),
        "sensitivity_decision_update": _as_mapping(
            sensitivity_decision_update
        ).get("status"),
        "variant_retest": _as_mapping(variant_retest).get("status"),
        "optimized_variant_ranking": _as_mapping(optimized_variant_ranking).get(
            "status"
        ),
        "owner_review": _as_mapping(owner_review).get("status"),
        "observation_rejection": _as_mapping(observation_rejection).get("status"),
        "plateau_decision": _as_mapping(plateau_decision).get("status"),
        "next_direction": _as_mapping(next_direction).get("status"),
    }
    sources: dict[str, Any] = {
        "candidate_ranking": candidate_ranking,
        "sensitivity_result": sensitivity_result,
        "sensitivity_decision_update": sensitivity_decision_update,
        "variant_retest": variant_retest,
        "optimized_variant_ranking": optimized_variant_ranking,
        "owner_review": owner_review,
        "observation_rejection": observation_rejection,
        "plateau_decision": plateau_decision,
        "next_direction": next_direction,
        "source_files": source_files,
        "source_hashes": {
            key: _file_sha256(Path(path)) for key, path in source_files.items()
        },
        "source_status": source_status,
        "ranking_top_candidate": _ranking_top(candidate_ranking).get("candidate_id"),
        "lower_turnover_reference": BASE_CANDIDATE_ID,
        "best_lower_turnover_variant": _coalesce_string(
            _as_mapping(plateau_decision).get("best_variant_from_2379"),
            _as_mapping(variant_retest).get("best_variant_after_retest"),
            _as_mapping(optimized_variant_ranking).get("best_variant_after_retest"),
        ),
        "next_direction_from_2381": _coalesce_string(
            _as_mapping(plateau_decision).get("next_direction_decision"),
            _as_mapping(
                _as_mapping(next_direction).get("next_candidate_direction")
            ).get("next_direction_decision"),
        ),
        "recommended_next_task_from_2381": _coalesce_string(
            _as_mapping(plateau_decision).get("recommended_next_research_task"),
            _as_mapping(
                _as_mapping(next_direction).get("next_candidate_direction")
            ).get("recommended_next_research_task"),
        ),
        "primary_execution_cadence": _coalesce_string(
            _as_mapping(plateau_decision).get("primary_execution_cadence"),
            _as_mapping(variant_retest).get("primary_execution_cadence"),
            _as_mapping(candidate_ranking).get("primary_execution_cadence"),
            _ranking_top(candidate_ranking).get("primary_execution_cadence"),
        ),
        "source_validation_errors": [],
    }
    sources["source_validation_errors"] = _source_validation_errors(sources)
    return sources


def _source_validation_errors(sources: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    expected_status = {
        "candidate_ranking": SOURCE_2365_READY_STATUS,
        "sensitivity_result": SOURCE_2366_READY_STATUS,
        "sensitivity_decision_update": SOURCE_2366_READY_STATUS,
        "variant_retest": SOURCE_2379_READY_STATUS,
        "optimized_variant_ranking": SOURCE_2379_READY_STATUS,
        "owner_review": SOURCE_2380_READY_STATUS,
        "observation_rejection": SOURCE_2380_READY_STATUS,
        "plateau_decision": SOURCE_2381_READY_STATUS,
        "next_direction": SOURCE_2381_READY_STATUS,
    }
    source_status = _as_mapping(sources.get("source_status"))
    for key, expected in expected_status.items():
        actual = source_status.get(key)
        if actual != expected:
            errors.append(f"{key}.status expected {expected}, got {actual!r}")

    if sources.get("ranking_top_candidate") != RANKING_TOP_CANDIDATE:
        errors.append(
            f"ranking_top_candidate expected {RANKING_TOP_CANDIDATE}, "
            f"got {sources.get('ranking_top_candidate')!r}"
        )
    if sources.get("best_lower_turnover_variant") != BEST_LOWER_TURNOVER_VARIANT:
        errors.append(
            f"best_lower_turnover_variant expected {BEST_LOWER_TURNOVER_VARIANT}, "
            f"got {sources.get('best_lower_turnover_variant')!r}"
        )
    if sources.get("next_direction_from_2381") != SOURCE_2381_NEXT_DIRECTION:
        errors.append(
            f"next_direction_from_2381 expected {SOURCE_2381_NEXT_DIRECTION}, "
            f"got {sources.get('next_direction_from_2381')!r}"
        )
    if sources.get("recommended_next_task_from_2381") != SOURCE_2381_EXPECTED_ROUTE:
        errors.append(
            f"recommended_next_task_from_2381 expected {SOURCE_2381_EXPECTED_ROUTE}, "
            f"got {sources.get('recommended_next_task_from_2381')!r}"
        )
    if sources.get("primary_execution_cadence") != PRIMARY_EXECUTION_CADENCE:
        errors.append(
            f"primary_execution_cadence expected {PRIMARY_EXECUTION_CADENCE}, "
            f"got {sources.get('primary_execution_cadence')!r}"
        )

    candidate_ranking = _as_mapping(sources.get("candidate_ranking"))
    if not _candidate_row(candidate_ranking, LOWER_TURNOVER_REFERENCE):
        errors.append(f"candidate_ranking missing {LOWER_TURNOVER_REFERENCE}")
    sensitivity_result = _as_mapping(sources.get("sensitivity_result"))
    if not _robustness_row(sensitivity_result, RANKING_TOP_CANDIDATE):
        errors.append(f"sensitivity_result missing {RANKING_TOP_CANDIDATE}")
    if not _robustness_row(sensitivity_result, LOWER_TURNOVER_REFERENCE):
        errors.append(f"sensitivity_result missing {LOWER_TURNOVER_REFERENCE}")

    plateau_decision = _as_mapping(sources.get("plateau_decision"))
    if plateau_decision.get("observation_approved_from_2380") is not False:
        errors.append("plateau_decision.observation_approved_from_2380 must be false")
    if plateau_decision.get("optimization_plateau_review_ready") is not True:
        errors.append("plateau_decision.optimization_plateau_review_ready must be true")
    owner_review = _as_mapping(sources.get("owner_review"))
    if owner_review.get("research_only_observation_approved") is not False:
        errors.append("owner_review.research_only_observation_approved must be false")
    observation_rejection = _as_mapping(sources.get("observation_rejection"))
    observation_rejection_payload = _as_mapping(
        observation_rejection.get("observation_rejection_rationale")
    )
    observation_approved = observation_rejection.get(
        "research_only_observation_approved"
    )
    if observation_approved is None:
        observation_approved = observation_rejection_payload.get(
            "research_only_observation_approved"
        )
    if observation_approved is not False:
        errors.append(
            "observation_rejection.research_only_observation_approved must be false"
        )

    for source_name in (
        "sensitivity_result",
        "variant_retest",
        "owner_review",
        "plateau_decision",
    ):
        document = _as_mapping(sources.get(source_name))
        for field in SAFETY_FALSE_FIELDS:
            if document.get(field) is True:
                errors.append(f"{source_name}.{field} must remain false")
        if document.get("broker_action") not in {None, "none"}:
            errors.append(f"{source_name}.broker_action must remain none")
        if document.get("production_effect") not in {None, "none"}:
            errors.append(f"{source_name}.production_effect must remain none")
    return errors


def _source_findings(sources: Mapping[str, Any]) -> dict[str, Any]:
    candidate_ranking = _as_mapping(sources.get("candidate_ranking"))
    sensitivity_result = _as_mapping(sources.get("sensitivity_result"))
    variant_retest = _as_mapping(sources.get("variant_retest"))
    plateau_decision = _as_mapping(sources.get("plateau_decision"))
    ranking_top = _ranking_top(candidate_ranking)
    lower_turnover_row = _candidate_row(candidate_ranking, LOWER_TURNOVER_REFERENCE)
    ranking_top_sensitivity = _robustness_row(
        sensitivity_result, RANKING_TOP_CANDIDATE
    )
    lower_turnover_sensitivity = _robustness_row(
        sensitivity_result, LOWER_TURNOVER_REFERENCE
    )
    best_metrics = _as_mapping(_as_mapping(variant_retest.get("decision_update")).get(
        "best_variant_metrics"
    ))
    ranking_return = _float_or_none(ranking_top.get("cost_adjusted_return"))
    lower_return = _float_or_none(lower_turnover_row.get("cost_adjusted_return"))
    best_variant_return = _float_or_none(best_metrics.get("annualized_return"))
    return {
        "ranking_top_candidate": RANKING_TOP_CANDIDATE,
        "ranking_top_row_from_2365": dict(ranking_top),
        "lower_turnover_row_from_2365": dict(lower_turnover_row),
        "ranking_top_sensitivity_row": dict(ranking_top_sensitivity),
        "lower_turnover_sensitivity_row": dict(lower_turnover_sensitivity),
        "best_lower_turnover_variant": sources.get("best_lower_turnover_variant"),
        "best_lower_turnover_variant_metrics": dict(best_metrics),
        "next_direction_from_2381": sources.get("next_direction_from_2381"),
        "optimization_plateau_detected_from_2381": plateau_decision.get(
            "optimization_plateau_detected"
        ),
        "primary_execution_cadence": sources.get("primary_execution_cadence"),
        "ranking_top_cost_adjusted_return": ranking_return,
        "lower_turnover_cost_adjusted_return": lower_return,
        "best_lower_turnover_variant_return": best_variant_return,
        "ranking_top_return_advantage_vs_lower_turnover": _subtract(
            ranking_return, lower_return
        ),
        "ranking_top_return_advantage_vs_best_variant": _subtract(
            ranking_return, best_variant_return
        ),
        "ranking_top_turnover": _float_or_none(ranking_top.get("turnover")),
        "lower_turnover_turnover": _float_or_none(lower_turnover_row.get("turnover")),
        "ranking_top_rebalance_count": _int_or_none(ranking_top.get("rebalance_count")),
        "lower_turnover_rebalance_count": _int_or_none(
            lower_turnover_row.get("rebalance_count")
        ),
        "ranking_top_max_drawdown": _float_or_none(ranking_top.get("max_drawdown")),
        "lower_turnover_max_drawdown": _float_or_none(
            lower_turnover_row.get("max_drawdown")
        ),
        "ranking_top_has_return_advantage": bool(
            _as_mapping(plateau_decision.get("source_findings")).get(
                "ranking_top_still_has_return_advantage"
            )
            or (
                ranking_return is not None
                and lower_return is not None
                and ranking_return > lower_return
            )
        ),
        "ranking_top_requires_guardrails": True,
        "lower_turnover_guardrails_transferred": True,
        "valid_until_window_remains_default": True,
        "paper_shadow_remains_disabled": True,
    }


def _ranking_top_fragility_diagnosis(
    source_findings: Mapping[str, Any],
    *,
    ready: bool,
) -> dict[str, Any]:
    ranking_top = _as_mapping(source_findings.get("ranking_top_row_from_2365"))
    sensitivity = _as_mapping(source_findings.get("ranking_top_sensitivity_row"))
    return {
        "schema_version": "dynamic_strategy_ranking_top_fragility_diagnosis.v1",
        "status": READY_STATUS if ready else BLOCKED_SOURCE_STATUS,
        "ranking_top_fragility_diagnosis_ready": ready,
        "ranking_top_candidate": RANKING_TOP_CANDIDATE,
        "diagnosis": {
            "turnover_risk": {
                "evaluate": True,
                "questions": [
                    "does_candidate_rely_on_high_rebalance_count",
                    "does_candidate_rely_on_high_monthly_turnover",
                    "does_return_decay_when_turnover_is_constrained",
                ],
                "evidence": {
                    "turnover": source_findings.get("ranking_top_turnover"),
                    "rebalance_count": source_findings.get(
                        "ranking_top_rebalance_count"
                    ),
                    "relies_on_high_turnover": ranking_top.get(
                        "relies_on_high_turnover"
                    ),
                    "robust_rank": sensitivity.get("robust_rank"),
                },
                "retest_requirement": "stress monthly turnover caps and single-step deltas",
            },
            "drawdown_risk": {
                "evaluate": True,
                "questions": [
                    "does_growth_tilt_amplify_drawdown_in_high_volatility",
                    "is_risk_off_reaction_too_slow",
                ],
                "evidence": {
                    "max_drawdown": source_findings.get("ranking_top_max_drawdown"),
                    "lower_turnover_max_drawdown": source_findings.get(
                        "lower_turnover_max_drawdown"
                    ),
                    "false_risk_off_count": ranking_top.get("false_risk_off_count"),
                },
                "retest_requirement": "compare drawdown against lower-turnover reference",
            },
            "cooldown_fragility": {
                "evaluate": True,
                "questions": [
                    "does_return_drop_after_adding_cooldown",
                    "does_candidate_depend_on_short_term_high_frequency_opportunities",
                ],
                "evidence": {
                    "cooldown_vs_event_driven_gap": ranking_top.get(
                        "cooldown_vs_event_driven_gap"
                    ),
                    "cooldown_block_count": ranking_top.get("cooldown_block_count"),
                    "survives_cooldown_constraints": ranking_top.get(
                        "survives_cooldown_constraints"
                    ),
                },
                "retest_requirement": "test cooldown and min-holding variants",
            },
            "cost_fragility": {
                "evaluate": True,
                "questions": [
                    "does_candidate_survive_realistic_and_conservative_cost",
                    "does_edge_survive_wider_slippage",
                ],
                "evidence": {
                    "cost_adjusted_return": source_findings.get(
                        "ranking_top_cost_adjusted_return"
                    ),
                    "survives_cost_adjustment": ranking_top.get(
                        "survives_cost_adjustment"
                    ),
                    "realistic_cost_adjusted_return": sensitivity.get(
                        "realistic_cost_adjusted_return"
                    ),
                },
                "retest_requirement": "run base, realistic, conservative and harsh cost stress",
            },
            "stale_signal_risk": {
                "evaluate": True,
                "questions": [
                    "does_candidate_carry_signal_after_valid_until_expiry",
                    "does_candidate_over_trade_near_signal_expiry",
                ],
                "evidence": {
                    "primary_execution_cadence": source_findings.get(
                        "primary_execution_cadence"
                    ),
                    "stale_signal_count": ranking_top.get("stale_signal_count"),
                    "valid_until_vs_monthly_gap": ranking_top.get(
                        "valid_until_vs_monthly_gap"
                    ),
                },
                "retest_requirement": "preserve valid-until and test expiry decay",
            },
        },
    }


def _guarded_variant_plan(
    source_findings: Mapping[str, Any],
    *,
    ready: bool,
) -> dict[str, Any]:
    variants = [
        {
            "candidate_id": RANKING_TOP_CANDIDATE,
            "role": "base_ranking_top_reference",
            "purpose": "2365 ranking top reference, not guarded",
            "guarded": False,
            "include_in_2383_reference": True,
            "changes": [],
        },
        {
            "candidate_id": "equal_risk_growth_tilt_guarded_turnover_v1",
            "role": "guarded_turnover_v1",
            "purpose": "ranking top + max turnover cap + max single-step weight delta",
            "guarded": True,
            "include_in_2383_retest": True,
            "changes": [
                "apply_monthly_turnover_cap",
                "apply_max_single_step_weight_delta",
                "preserve_valid_until_window",
            ],
        },
        {
            "candidate_id": "equal_risk_growth_tilt_guarded_cooldown_v1",
            "role": "guarded_cooldown_v1",
            "purpose": "ranking top + cooldown / min holding constraints",
            "guarded": True,
            "include_in_2383_retest": True,
            "changes": [
                "add_cooldown_days",
                "add_min_holding_days",
                "block_stale_signal_after_valid_until",
            ],
        },
        {
            "candidate_id": "equal_risk_growth_tilt_guarded_risk_cap_v1",
            "role": "guarded_risk_cap_v1",
            "purpose": (
                "ranking top + stricter risk cap in high volatility / risk-off states"
            ),
            "guarded": True,
            "include_in_2383_retest": True,
            "changes": [
                "stricter_risk_cap",
                "reduce_growth_tilt_when_volatility_high",
                "preserve_downside_protection",
            ],
        },
        {
            "candidate_id": "equal_risk_growth_tilt_guarded_valid_until_decay_v1",
            "role": "guarded_valid_until_decay_v1",
            "purpose": "ranking top + signal decay near valid-until expiry",
            "guarded": True,
            "include_in_2383_retest": True,
            "changes": [
                "decay_signal_near_valid_until_expiry",
                "block_signal_after_expiry",
                "avoid_stale_signal_execution",
            ],
        },
        {
            "candidate_id": "equal_risk_growth_tilt_lower_turnover_fusion_v1",
            "role": "guarded_fusion_v1",
            "purpose": "ranking top return engine + lower-turnover execution guardrails",
            "guarded": True,
            "include_in_2383_retest": True,
            "changes": [
                "valid_until_window",
                "cooldown_balancing",
                "max_single_step_weight_delta",
                "monthly_turnover_cap",
                "risk_cap_preserved",
                "trend_confirmation_gate",
            ],
        },
        {
            "candidate_id": (
                "equal_risk_growth_tilt_lower_turnover_conservative_fusion_v1"
            ),
            "role": "guarded_fusion_conservative_v1",
            "purpose": "conservative guarded fusion for robustness testing",
            "guarded": True,
            "include_in_2383_retest": True,
            "changes": [
                "stricter_turnover_cap",
                "longer_cooldown",
                "lower_growth_tilt_intensity",
                "stricter_high_volatility_risk_cap",
            ],
        },
    ]
    return {
        "schema_version": "dynamic_strategy_guarded_ranking_top_variant_plan.v1",
        "status": READY_STATUS if ready else BLOCKED_SOURCE_STATUS,
        "guarded_variant_plan_ready": ready,
        "ranking_top_candidate": RANKING_TOP_CANDIDATE,
        "guardrail_reference_candidates": [
            LOWER_TURNOVER_REFERENCE,
            BEST_LOWER_TURNOVER_VARIANT,
        ],
        "transferable_guardrails": [dict(item) for item in TRANSFERABLE_GUARDRAILS],
        "variants": variants,
        "planned_variants": list(GUARDED_VARIANT_IDS),
        "all_variant_ids_for_2383": [RANKING_TOP_CANDIDATE, *GUARDED_VARIANT_IDS],
        "source_findings_used": {
            "ranking_top_return_advantage_vs_lower_turnover": source_findings.get(
                "ranking_top_return_advantage_vs_lower_turnover"
            ),
            "ranking_top_return_advantage_vs_best_variant": source_findings.get(
                "ranking_top_return_advantage_vs_best_variant"
            ),
            "ranking_top_turnover": source_findings.get("ranking_top_turnover"),
            "lower_turnover_turnover": source_findings.get("lower_turnover_turnover"),
        },
    }


def _variant_evaluation_plan(*, ready: bool) -> dict[str, Any]:
    return {
        "schema_version": "dynamic_strategy_guarded_variant_evaluation_plan.v1",
        "status": READY_STATUS if ready else BLOCKED_SOURCE_STATUS,
        "variant_evaluation_plan_ready": ready,
        "recommended_next_research_task": NEXT_ROUTE,
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
        "cost_stress": {
            "base": {"transaction_cost_bps": 2, "slippage_bps": 2},
            "realistic": {"transaction_cost_bps": 5, "slippage_bps": 5},
            "conservative": {"transaction_cost_bps": 10, "slippage_bps": 10},
            "harsh": {"transaction_cost_bps": 20, "slippage_bps": 10},
        },
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
        "turnover_constraint_stress": {
            "max_turnover_per_month": ["unlimited", 1.0, 0.5, 0.25],
            "max_single_step_weight_delta": ["unrestricted", 0.30, 0.20, 0.10],
            "cooldown_days": [0, 1, 3, 5, 10],
        },
        "acceptance_criteria": {
            "must": [
                "cost_adjusted_return_above_static",
                "survives_realistic_cost",
                "survives_conservative_cost",
                "return_gap_vs_original_ranking_top_not_too_large",
                "drawdown_not_materially_worse_than_lower_turnover_reference",
                "turnover_materially_lower_than_original_ranking_top",
                "valid_until_window_preserved",
                "no_stale_signal_carry_forward",
            ],
            "should": [
                "preserve_most_of_ranking_top_upside",
                "improve_robustness_vs_original_ranking_top",
                "improve_return_vs_lower_turnover_reference",
                "not_degrade_high_volatility_behavior",
                "not_depend_on_single_time_slice",
            ],
            "must_not": [
                "rely_on_monthly_rebalance",
                "require_event_append",
                "require_outcome_binding",
                "require_paper_shadow",
                "require_scheduler",
                "require_production_or_broker",
            ],
        },
        "forbidden_optimization_paths": list(FORBIDDEN_OPTIMIZATION_PATHS),
    }


def _base_payload(
    *,
    status: str,
    as_of_date: date,
    sources: Mapping[str, Any],
    source_findings: Mapping[str, Any],
    fragility_diagnosis: Mapping[str, Any],
    guarded_variant_plan: Mapping[str, Any],
    variant_evaluation_plan: Mapping[str, Any],
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
        "source_ready_for_retest_plan": ready,
        "data_quality_gate_executed": False,
        "data_quality_gate_reason": DATA_QUALITY_GATE_REASON,
        "fresh_market_data_read": False,
        "backtest_run": False,
        "new_signal_generated": False,
        "scoring_generated": False,
        "research_only": True,
        "observe_only": True,
        "next_direction_from_2381": sources.get("next_direction_from_2381"),
        "ranking_top_candidate": RANKING_TOP_CANDIDATE,
        "guardrail_reference_candidates": [
            LOWER_TURNOVER_REFERENCE,
            BEST_LOWER_TURNOVER_VARIANT,
        ],
        "primary_execution_cadence": PRIMARY_EXECUTION_CADENCE,
        "retest_plan_ready": ready,
        "ranking_top_fragility_diagnosis_ready": ready,
        "guarded_variant_plan_ready": ready,
        "variant_evaluation_plan_ready": ready,
        "source_findings": dict(source_findings),
        "ranking_top_fragility_diagnosis": dict(fragility_diagnosis),
        "guardrail_transfer_plan": [dict(item) for item in TRANSFERABLE_GUARDRAILS],
        "guarded_variant_plan": dict(guarded_variant_plan),
        "variant_evaluation_plan": dict(variant_evaluation_plan),
        "planned_variants": list(GUARDED_VARIANT_IDS),
        "all_variant_ids_for_2383": [RANKING_TOP_CANDIDATE, *GUARDED_VARIANT_IDS],
        "forbidden_optimization_paths": list(FORBIDDEN_OPTIMIZATION_PATHS),
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
        "explicit_non_goals": list(NON_APPROVED_PATHS),
        "summary_findings": {
            "ranking_top_has_return_advantage": source_findings[
                "ranking_top_has_return_advantage"
            ],
            "ranking_top_requires_guardrails": True,
            "lower_turnover_guardrails_transferred": True,
            "valid_until_window_remains_default": True,
            "paper_shadow_remains_disabled": True,
        },
        "artifact_paths": {},
    }


def _write_outputs(*, payload: dict[str, Any], output_root: Path, docs_root: Path) -> None:
    output_root.mkdir(parents=True, exist_ok=True)
    docs_root.mkdir(parents=True, exist_ok=True)
    result_json = output_root / "retest_plan_result.json"
    fragility_json = output_root / "ranking_top_fragility_diagnosis.json"
    variant_plan_json = output_root / "guarded_variant_plan.json"
    evaluation_plan_json = output_root / "variant_evaluation_plan.json"
    markdown_path = docs_root / "dynamic_strategy_ranking_top_guarded_turnover_retest_plan.md"
    fragility_markdown = docs_root / "dynamic_strategy_ranking_top_fragility_diagnosis.md"
    variant_markdown = docs_root / "dynamic_strategy_guarded_ranking_top_variant_plan.md"
    route_markdown = docs_root / "dynamic_strategy_2383_route.md"
    payload["artifact_paths"] = {
        "json_path": str(result_json),
        "retest_plan_result_json": str(result_json),
        "ranking_top_fragility_diagnosis_json": str(fragility_json),
        "guarded_variant_plan_json": str(variant_plan_json),
        "variant_evaluation_plan_json": str(evaluation_plan_json),
        "markdown_path": str(markdown_path),
        "ranking_top_fragility_diagnosis_markdown": str(fragility_markdown),
        "guarded_variant_plan_markdown": str(variant_markdown),
        "next_route_markdown": str(route_markdown),
    }
    result_json.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    fragility_json.write_text(
        json.dumps(
            payload["ranking_top_fragility_diagnosis"],
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    variant_plan_json.write_text(
        json.dumps(
            payload["guarded_variant_plan"],
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    evaluation_plan_json.write_text(
        json.dumps(
            payload["variant_evaluation_plan"],
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    markdown_path.write_text(_main_markdown(payload), encoding="utf-8")
    fragility_markdown.write_text(_fragility_markdown(payload), encoding="utf-8")
    variant_markdown.write_text(_variant_plan_markdown(payload), encoding="utf-8")
    route_markdown.write_text(_route_markdown(payload), encoding="utf-8")


def _main_markdown(payload: Mapping[str, Any]) -> str:
    plan = _as_mapping(payload.get("variant_evaluation_plan"))
    return "\n".join(
        [
            "# Dynamic strategy ranking top guarded-turnover retest plan",
            "",
            "## Executive summary",
            "",
            f"- status：`{payload['status']}`",
            f"- ranking top candidate：`{payload['ranking_top_candidate']}`",
            f"- next direction from 2381：`{payload['next_direction_from_2381']}`",
            f"- primary execution cadence：`{payload['primary_execution_cadence']}`",
            f"- planned guarded variants：`{len(payload['planned_variants'])}`",
            f"- recommended route：`{payload['recommended_next_research_task']}`",
            "- paper-shadow / production / broker：`false` / `false` / `none`",
            "",
            "## Source decision from TRADING-2381",
            "",
            "2381 已确认 lower-turnover 优化线进入局部 plateau，默认下一步切到 "
            "`OPTION_B_SWITCH_TO_RANKING_TOP_GUARDED_VARIANT`。",
            "",
            "## Why switch back to ranking top",
            "",
            "2365 ranking top 仍保留收益优势，但不得裸用；2382 只规划 guarded variants，"
            "要求在 2383 中用 turnover、cooldown、risk cap 和 valid-until guardrails 重测。",
            "",
            "## Ranking top fragility diagnosis",
            "",
            _fragility_summary(payload.get("ranking_top_fragility_diagnosis")),
            "",
            "## Lower-turnover guardrail transfer plan",
            "",
            _guardrail_table(payload.get("guardrail_transfer_plan")),
            "",
            "## Guarded ranking-top variant plan",
            "",
            _variant_table(_as_mapping(payload.get("guarded_variant_plan")).get("variants")),
            "",
            "## Forbidden optimization paths",
            "",
            _markdown_bullets(payload.get("forbidden_optimization_paths")),
            "",
            "## 2383 retest plan",
            "",
            f"- primary execution cadence：`{plan.get('primary_execution_cadence')}`",
            "- monthly rebalance allowed for primary decision：`false`",
            f"- comparison cadences：`{', '.join(plan.get('comparison_cadences', []))}`",
            "",
            "## Acceptance criteria",
            "",
            _acceptance_markdown(plan.get("acceptance_criteria")),
            "",
            "## Explicit non-goals",
            "",
            _markdown_bullets(payload.get("explicit_non_goals")),
            "",
            "## Recommended next route",
            "",
            f"`{payload['recommended_next_research_task']}`",
            "",
        ]
    )


def _fragility_markdown(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# Dynamic strategy ranking top fragility diagnosis",
            "",
            f"- status：`{payload['status']}`",
            f"- ranking top candidate：`{payload['ranking_top_candidate']}`",
            "",
            _fragility_summary(payload.get("ranking_top_fragility_diagnosis")),
            "",
        ]
    )


def _variant_plan_markdown(payload: Mapping[str, Any]) -> str:
    variant_plan = _as_mapping(payload.get("guarded_variant_plan"))
    return "\n".join(
        [
            "# Dynamic strategy guarded ranking top variant plan",
            "",
            f"- status：`{payload['status']}`",
            f"- ranking top candidate：`{payload['ranking_top_candidate']}`",
            "",
            "## Guardrail references",
            "",
            _markdown_bullets(payload.get("guardrail_reference_candidates")),
            "",
            "## Variants",
            "",
            _variant_table(variant_plan.get("variants")),
            "",
        ]
    )


def _route_markdown(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# TRADING-2383 route",
            "",
            f"- source task：`{TASK_ID}`",
            f"- status：`{payload['status']}`",
            f"- next route：`{payload['recommended_next_research_task']}`",
            "- route reason：2382 is a retest plan; guarded ranking-top variants "
            "must be tested before any observation, paper-shadow, production or broker path.",
            "",
        ]
    )


def _load_json_document(path: Path) -> dict[str, Any]:
    try:
        loaded = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise FileNotFoundError(f"Required source artifact is missing: {path}") from exc
    if not isinstance(loaded, dict):
        raise TypeError(f"Expected JSON object at {path}")
    return loaded


def _resolve_as_of(as_of_date: date | None, plateau_decision: Mapping[str, Any]) -> date:
    if as_of_date is not None:
        return as_of_date
    value = plateau_decision.get("as_of")
    if isinstance(value, str):
        return date.fromisoformat(value)
    return date.today()


def _ranking_top(candidate_ranking: Mapping[str, Any]) -> Mapping[str, Any]:
    rows = candidate_ranking.get("candidate_ranking")
    if isinstance(rows, list) and rows:
        first = rows[0]
        if isinstance(first, Mapping):
            return first
    return {}


def _candidate_row(
    candidate_ranking: Mapping[str, Any],
    candidate_id: str,
) -> Mapping[str, Any]:
    rows = candidate_ranking.get("candidate_ranking")
    if not isinstance(rows, list):
        return {}
    for row in rows:
        if isinstance(row, Mapping) and row.get("candidate_id") == candidate_id:
            return row
    return {}


def _robustness_row(
    sensitivity_result: Mapping[str, Any],
    candidate_id: str,
) -> Mapping[str, Any]:
    rows = sensitivity_result.get("robustness_ranking")
    if not isinstance(rows, list):
        return {}
    for row in rows:
        if isinstance(row, Mapping) and row.get("candidate_id") == candidate_id:
            return row
    return {}


def _subtract(left: float | None, right: float | None) -> float | None:
    if left is None or right is None:
        return None
    return round(left - right, 6)


def _float_or_none(value: object) -> float | None:
    if isinstance(value, int | float):
        return float(value)
    return None


def _int_or_none(value: object) -> int | None:
    if isinstance(value, int):
        return value
    if isinstance(value, float) and value.is_integer():
        return int(value)
    return None


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


def _guardrail_table(values: object) -> str:
    if not isinstance(values, list | tuple):
        return "|guardrail|required|purpose|\n|---|---|---|"
    lines = ["|guardrail|required|purpose|", "|---|---|---|"]
    for row in values:
        if not isinstance(row, Mapping):
            continue
        lines.append(
            "|"
            f"`{row.get('guardrail')}`|"
            f"`{row.get('required')}`|"
            f"`{row.get('purpose')}`|"
        )
    return "\n".join(lines)


def _variant_table(values: object) -> str:
    if not isinstance(values, list | tuple):
        return "|candidate|role|guarded|purpose|\n|---|---|---|---|"
    lines = ["|candidate|role|guarded|purpose|", "|---|---|---|---|"]
    for row in values:
        if not isinstance(row, Mapping):
            continue
        lines.append(
            "|"
            f"`{row.get('candidate_id')}`|"
            f"`{row.get('role')}`|"
            f"`{row.get('guarded')}`|"
            f"{row.get('purpose')}|"
        )
    return "\n".join(lines)


def _fragility_summary(diagnosis: object) -> str:
    mapping = _as_mapping(diagnosis)
    items = _as_mapping(mapping.get("diagnosis"))
    if not items:
        return "- 无"
    lines: list[str] = []
    for key, item in items.items():
        row = _as_mapping(item)
        lines.append(
            f"- `{key}`：`evaluate={row.get('evaluate')}`；"
            f"{row.get('retest_requirement')}"
        )
    return "\n".join(lines)


def _acceptance_markdown(criteria: object) -> str:
    mapping = _as_mapping(criteria)
    lines: list[str] = []
    for key in ("must", "should", "must_not"):
        values = mapping.get(key)
        lines.append(f"### {key}")
        lines.append("")
        lines.append(_markdown_bullets(values))
        lines.append("")
    return "\n".join(lines).rstrip()
