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
from ai_trading_system.dynamic_strategy_optimized_candidate_targeted_retest import (
    DECISION_CONTINUE_OPTIMIZATION,
    DEFAULT_DYNAMIC_STRATEGY_OPTIMIZED_CANDIDATE_TARGETED_RETEST_OUTPUT_ROOT,
)
from ai_trading_system.dynamic_strategy_optimized_candidate_targeted_retest import (
    READY_STATUS as SOURCE_2376_READY_STATUS,
)
from ai_trading_system.dynamic_strategy_optimized_variant_owner_review_decision import (
    DEFAULT_DYNAMIC_STRATEGY_OPTIMIZED_VARIANT_OWNER_REVIEW_DECISION_OUTPUT_ROOT,
)
from ai_trading_system.dynamic_strategy_optimized_variant_owner_review_decision import (
    NEXT_ROUTE as SOURCE_2380_EXPECTED_ROUTE,
)
from ai_trading_system.dynamic_strategy_optimized_variant_owner_review_decision import (
    OWNER_DECISION as SOURCE_2380_OWNER_DECISION,
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

TASK_ID = "TRADING-2381"
TASK_REGISTER_ID = (
    "TRADING-2381_DYNAMIC_STRATEGY_OPTIMIZATION_PLATEAU_AND_NEXT_CANDIDATE_"
    "DECISION"
)
REPORT_TYPE = "dynamic_strategy_optimization_plateau_next_candidate_decision"
SCHEMA_VERSION = "dynamic_strategy_optimization_plateau_next_candidate_decision.v1"
READY_STATUS = (
    "DYNAMIC_STRATEGY_OPTIMIZATION_PLATEAU_AND_NEXT_CANDIDATE_DECISION_READY"
)
BLOCKED_SOURCE_STATUS = (
    "DYNAMIC_STRATEGY_OPTIMIZATION_PLATEAU_AND_NEXT_CANDIDATE_DECISION_"
    "BLOCKED_SOURCE_ARTIFACT"
)
BEST_VARIANT_EXPECTED = "dynamic_regime_overlay_v0_4_cooldown_balanced_v1"
PRIMARY_EXECUTION_CADENCE = "valid_until_window"
DATA_QUALITY_GATE_REASON = (
    "NOT_APPLICABLE_PRIOR_ARTIFACT_PLATEAU_DECISION_ONLY_NO_FRESH_MARKET_DATA"
)
OPTIMIZATION_PLATEAU_DETECTED = "LOWER_TURNOVER_LOCAL_PLATEAU_DETECTED"
NEXT_DIRECTION_DECISION = "OPTION_B_SWITCH_TO_RANKING_TOP_GUARDED_VARIANT"
RECOMMENDED_DEFAULT_DIRECTION = NEXT_DIRECTION_DECISION
NEXT_ROUTE = "TRADING-2382_Dynamic_Strategy_Ranking_Top_Guarded_Turnover_Retest_Plan"
SOURCE_TASKS: tuple[str, ...] = (
    "TRADING-2365",
    "TRADING-2366",
    "TRADING-2376",
    "TRADING-2379",
    "TRADING-2380",
)
DECISION_OPTIONS: tuple[str, ...] = (
    "OPTION_A_CONTINUE_LOWER_TURNOVER_OPTIMIZATION",
    "OPTION_B_SWITCH_TO_RANKING_TOP_GUARDED_VARIANT",
    "OPTION_C_EXPAND_CANDIDATE_POOL",
    "OPTION_D_PAUSE_AND_IMPROVE_DATA_PIT_COVERAGE",
    "OPTION_E_STOP_DYNAMIC_STRATEGY_LINE_FOR_NOW",
)
PLATEAU_EVIDENCE: tuple[str, ...] = (
    "LOWER_TURNOVER_LINE_HAS_MULTIPLE_POST_2376_OPTIMIZATION_PASSES",
    "BEST_VARIANT_REMAINS_CONTINUE_OPTIMIZATION",
    "OBSERVATION_REJECTED_BY_2380_OWNER_REVIEW",
    "TIME_AND_REGIME_SLICE_ROBUSTNESS_STILL_FAIL",
    "RANKING_TOP_RETURN_ADVANTAGE_REMAINS",
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

DEFAULT_DYNAMIC_STRATEGY_OPTIMIZATION_PLATEAU_NEXT_CANDIDATE_DECISION_OUTPUT_ROOT = (
    PROJECT_ROOT / "outputs" / "research_strategies" / REPORT_TYPE
)
DEFAULT_DYNAMIC_STRATEGY_OPTIMIZATION_PLATEAU_NEXT_CANDIDATE_DECISION_DOCS_ROOT = (
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
DEFAULT_SOURCE_2376_TARGETED_RETEST_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_OPTIMIZED_CANDIDATE_TARGETED_RETEST_OUTPUT_ROOT
    / "targeted_retest_result.json"
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


def run_dynamic_strategy_optimization_plateau_next_candidate_decision(
    *,
    source_candidate_ranking_path: Path = DEFAULT_SOURCE_2365_CANDIDATE_RANKING_PATH,
    source_sensitivity_result_path: Path = DEFAULT_SOURCE_2366_SENSITIVITY_RESULT_PATH,
    source_sensitivity_decision_update_path: Path = (
        DEFAULT_SOURCE_2366_DECISION_UPDATE_PATH
    ),
    source_targeted_retest_path: Path = DEFAULT_SOURCE_2376_TARGETED_RETEST_PATH,
    source_variant_retest_path: Path = DEFAULT_SOURCE_2379_VARIANT_RETEST_PATH,
    source_optimized_variant_ranking_path: Path = (
        DEFAULT_SOURCE_2379_OPTIMIZED_VARIANT_RANKING_PATH
    ),
    source_owner_review_path: Path = DEFAULT_SOURCE_2380_OWNER_REVIEW_PATH,
    source_observation_rejection_path: Path = (
        DEFAULT_SOURCE_2380_OBSERVATION_REJECTION_PATH
    ),
    output_root: Path = (
        DEFAULT_DYNAMIC_STRATEGY_OPTIMIZATION_PLATEAU_NEXT_CANDIDATE_DECISION_OUTPUT_ROOT
    ),
    docs_root: Path = (
        DEFAULT_DYNAMIC_STRATEGY_OPTIMIZATION_PLATEAU_NEXT_CANDIDATE_DECISION_DOCS_ROOT
    ),
    as_of_date: date | None = None,
) -> dict[str, Any]:
    sources = _load_sources(
        source_candidate_ranking_path=source_candidate_ranking_path,
        source_sensitivity_result_path=source_sensitivity_result_path,
        source_sensitivity_decision_update_path=source_sensitivity_decision_update_path,
        source_targeted_retest_path=source_targeted_retest_path,
        source_variant_retest_path=source_variant_retest_path,
        source_optimized_variant_ranking_path=source_optimized_variant_ranking_path,
        source_owner_review_path=source_owner_review_path,
        source_observation_rejection_path=source_observation_rejection_path,
    )
    ready = not sources["source_validation_errors"]
    resolved_as_of = _resolve_as_of(as_of_date, sources["owner_review"])
    source_findings = _source_findings(sources)
    plateau_review = _optimization_plateau_review(source_findings, ready=ready)
    direction = _next_candidate_direction(source_findings, plateau_review, ready=ready)
    payload = _base_payload(
        status=READY_STATUS if ready else BLOCKED_SOURCE_STATUS,
        as_of_date=resolved_as_of,
        sources=sources,
        source_findings=source_findings,
        plateau_review=plateau_review,
        next_direction=direction,
        ready=ready,
    )
    _write_outputs(payload=payload, output_root=output_root, docs_root=docs_root)
    return payload


def _load_sources(
    *,
    source_candidate_ranking_path: Path,
    source_sensitivity_result_path: Path,
    source_sensitivity_decision_update_path: Path,
    source_targeted_retest_path: Path,
    source_variant_retest_path: Path,
    source_optimized_variant_ranking_path: Path,
    source_owner_review_path: Path,
    source_observation_rejection_path: Path,
) -> dict[str, Any]:
    candidate_ranking = _load_json_document(source_candidate_ranking_path)
    sensitivity_result = _load_json_document(source_sensitivity_result_path)
    sensitivity_decision_update = _load_json_document(
        source_sensitivity_decision_update_path
    )
    targeted_retest = _load_json_document(source_targeted_retest_path)
    variant_retest = _load_json_document(source_variant_retest_path)
    optimized_variant_ranking = _load_json_document(
        source_optimized_variant_ranking_path
    )
    owner_review = _load_json_document(source_owner_review_path)
    observation_rejection = _load_json_document(source_observation_rejection_path)
    source_files = {
        "candidate_ranking": str(source_candidate_ranking_path),
        "sensitivity_result": str(source_sensitivity_result_path),
        "sensitivity_decision_update": str(source_sensitivity_decision_update_path),
        "targeted_retest": str(source_targeted_retest_path),
        "variant_retest": str(source_variant_retest_path),
        "optimized_variant_ranking": str(source_optimized_variant_ranking_path),
        "owner_review": str(source_owner_review_path),
        "observation_rejection": str(source_observation_rejection_path),
    }
    source_status = {
        "candidate_ranking": _as_mapping(candidate_ranking).get("status"),
        "sensitivity_result": _as_mapping(sensitivity_result).get("status"),
        "sensitivity_decision_update": _as_mapping(
            sensitivity_decision_update
        ).get("status"),
        "targeted_retest": _as_mapping(targeted_retest).get("status"),
        "variant_retest": _as_mapping(variant_retest).get("status"),
        "optimized_variant_ranking": _as_mapping(optimized_variant_ranking).get(
            "status"
        ),
        "owner_review": _as_mapping(owner_review).get("status"),
        "observation_rejection": _as_mapping(observation_rejection).get("status"),
    }
    sources: dict[str, Any] = {
        "candidate_ranking": candidate_ranking,
        "sensitivity_result": sensitivity_result,
        "sensitivity_decision_update": sensitivity_decision_update,
        "targeted_retest": targeted_retest,
        "variant_retest": variant_retest,
        "optimized_variant_ranking": optimized_variant_ranking,
        "owner_review": owner_review,
        "observation_rejection": observation_rejection,
        "source_files": source_files,
        "source_hashes": {
            key: _file_sha256(Path(path)) for key, path in source_files.items()
        },
        "source_status": source_status,
        "base_candidate": _coalesce_string(
            _as_mapping(variant_retest).get("base_candidate"),
            _as_mapping(owner_review).get("base_candidate"),
            _as_mapping(targeted_retest).get("primary_candidate"),
        ),
        "ranking_top_reference": _coalesce_string(
            _as_mapping(variant_retest).get("ranking_top_reference"),
            _as_mapping(owner_review).get("ranking_top_reference"),
            _ranking_top(candidate_ranking).get("candidate_id"),
        ),
        "best_variant_from_2379": _coalesce_string(
            _as_mapping(owner_review).get("best_variant_from_2379"),
            _as_mapping(variant_retest).get("best_variant_after_retest"),
            _as_mapping(optimized_variant_ranking).get("best_variant_after_retest"),
        ),
        "best_variant_decision_from_2379": _coalesce_string(
            _as_mapping(owner_review).get("best_variant_decision_from_2379"),
            _as_mapping(variant_retest).get("best_variant_decision"),
            _as_mapping(optimized_variant_ranking).get("best_variant_decision"),
        ),
        "primary_execution_cadence": _coalesce_string(
            _as_mapping(owner_review).get("primary_execution_cadence"),
            _as_mapping(variant_retest).get("primary_execution_cadence"),
            _as_mapping(candidate_ranking).get("primary_execution_cadence"),
            _as_mapping(sensitivity_result).get("primary_execution_cadence"),
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
        "targeted_retest": SOURCE_2376_READY_STATUS,
        "variant_retest": SOURCE_2379_READY_STATUS,
        "optimized_variant_ranking": SOURCE_2379_READY_STATUS,
        "owner_review": SOURCE_2380_READY_STATUS,
        "observation_rejection": SOURCE_2380_READY_STATUS,
    }
    source_status = _as_mapping(sources.get("source_status"))
    for key, expected in expected_status.items():
        actual = source_status.get(key)
        if actual != expected:
            errors.append(f"{key}.status expected {expected}, got {actual!r}")

    if sources.get("base_candidate") != BASE_CANDIDATE_ID:
        errors.append(
            f"base_candidate expected {BASE_CANDIDATE_ID}, "
            f"got {sources.get('base_candidate')!r}"
        )
    if sources.get("ranking_top_reference") != RANKING_TOP_REFERENCE:
        errors.append(
            f"ranking_top_reference expected {RANKING_TOP_REFERENCE}, "
            f"got {sources.get('ranking_top_reference')!r}"
        )
    if sources.get("best_variant_from_2379") != BEST_VARIANT_EXPECTED:
        errors.append(
            f"best_variant_from_2379 expected {BEST_VARIANT_EXPECTED}, "
            f"got {sources.get('best_variant_from_2379')!r}"
        )
    if sources.get("best_variant_decision_from_2379") != DECISION_CONTINUE_OPTIMIZATION:
        errors.append(
            "best_variant_decision_from_2379 expected "
            f"{DECISION_CONTINUE_OPTIMIZATION}, "
            f"got {sources.get('best_variant_decision_from_2379')!r}"
        )
    if sources.get("primary_execution_cadence") != PRIMARY_EXECUTION_CADENCE:
        errors.append(
            f"primary_execution_cadence expected {PRIMARY_EXECUTION_CADENCE}, "
            f"got {sources.get('primary_execution_cadence')!r}"
        )

    owner_review = _as_mapping(sources.get("owner_review"))
    if owner_review.get("recommended_next_research_task") != SOURCE_2380_EXPECTED_ROUTE:
        errors.append(
            "owner_review.recommended_next_research_task expected "
            f"{SOURCE_2380_EXPECTED_ROUTE}, "
            f"got {owner_review.get('recommended_next_research_task')!r}"
        )
    if owner_review.get("owner_decision") != SOURCE_2380_OWNER_DECISION:
        errors.append(
            f"owner_review.owner_decision expected {SOURCE_2380_OWNER_DECISION}, "
            f"got {owner_review.get('owner_decision')!r}"
        )
    if owner_review.get("research_only_observation_approved") is not False:
        errors.append("owner_review.research_only_observation_approved must be false")
    if owner_review.get("optimization_plateau_review_required") is not True:
        errors.append("owner_review.optimization_plateau_review_required must be true")

    for source_name in (
        "sensitivity_result",
        "targeted_retest",
        "variant_retest",
        "owner_review",
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
    owner_review = _as_mapping(sources.get("owner_review"))
    ranking_top = _ranking_top(candidate_ranking)
    base_row = _candidate_row(candidate_ranking, BASE_CANDIDATE_ID)
    robust_top = _robustness_top(sensitivity_result)
    ranking_top_sensitivity = _robustness_row(sensitivity_result, RANKING_TOP_REFERENCE)
    best_metrics = _as_mapping(_as_mapping(variant_retest.get("decision_update")).get(
        "best_variant_metrics"
    ))
    return_gap_reduction = _float_or_none(
        best_metrics.get("return_gap_reduction_vs_base")
    )
    variant_vs_ranking_gap = _float_or_none(
        best_metrics.get("variant_vs_ranking_top_gap")
    )
    best_return = _float_or_none(best_metrics.get("annualized_return"))
    ranking_top_return = _float_or_none(ranking_top.get("cost_adjusted_return"))
    return {
        "base_candidate": sources.get("base_candidate"),
        "ranking_top_reference": sources.get("ranking_top_reference"),
        "best_variant_from_2379": sources.get("best_variant_from_2379"),
        "best_variant_decision_from_2379": sources.get(
            "best_variant_decision_from_2379"
        ),
        "observation_approved_from_2380": bool(
            owner_review.get("research_only_observation_approved")
        ),
        "primary_execution_cadence": sources.get("primary_execution_cadence"),
        "ranking_top_row": dict(ranking_top),
        "base_candidate_row_from_2365": dict(base_row),
        "robustness_top_from_2366": robust_top.get("candidate_id"),
        "robustness_top_row": dict(robust_top),
        "ranking_top_sensitivity_row": dict(ranking_top_sensitivity),
        "best_variant_metrics": dict(best_metrics),
        "return_gap_reduction_vs_base": return_gap_reduction,
        "variant_vs_ranking_top_gap": variant_vs_ranking_gap,
        "best_variant_annualized_return": best_return,
        "ranking_top_cost_adjusted_return_from_2365": ranking_top_return,
        "time_slice_pass_rate": _float_or_none(best_metrics.get("time_slice_pass_rate")),
        "regime_slice_pass_rate": _float_or_none(
            best_metrics.get("regime_slice_pass_rate")
        ),
        "turnover_profile_preserved": bool(
            best_metrics.get("turnover_profile_preserved")
        ),
        "survives_realistic_cost": bool(best_metrics.get("realistic_cost_passed")),
        "survives_conservative_cost": bool(
            best_metrics.get("conservative_cost_passed")
        ),
        "ranking_top_still_has_return_advantage": _ranking_top_has_return_advantage(
            ranking_top_return=ranking_top_return,
            best_return=best_return,
            variant_vs_ranking_gap=variant_vs_ranking_gap,
        ),
        "lower_turnover_line_has_value": bool(
            return_gap_reduction is not None
            and return_gap_reduction > 0
            and best_metrics.get("realistic_cost_passed") is True
        ),
        "lower_turnover_line_not_accepted_for_observation": bool(
            sources.get("best_variant_decision_from_2379")
            == DECISION_CONTINUE_OPTIMIZATION
            and owner_review.get("research_only_observation_approved") is False
        ),
    }


def _optimization_plateau_review(
    source_findings: Mapping[str, Any],
    *,
    ready: bool,
) -> dict[str, Any]:
    return {
        "schema_version": "dynamic_strategy_optimization_plateau_review.v1",
        "status": READY_STATUS if ready else BLOCKED_SOURCE_STATUS,
        "optimization_plateau_review_ready": ready,
        "optimization_plateau_detected": OPTIMIZATION_PLATEAU_DETECTED,
        "plateau_scope": "lower_turnover_local_optimization_line",
        "plateau_evidence": list(PLATEAU_EVIDENCE),
        "lower_turnover_line_has_value": source_findings[
            "lower_turnover_line_has_value"
        ],
        "lower_turnover_line_not_accepted_for_observation": source_findings[
            "lower_turnover_line_not_accepted_for_observation"
        ],
        "ranking_top_still_has_return_advantage": source_findings[
            "ranking_top_still_has_return_advantage"
        ],
        "primary_blockers": [
            "TIME_SLICE_ROBUSTNESS_NOT_READY",
            "REGIME_SLICE_ROBUSTNESS_NOT_READY",
            "RETURN_GAP_VS_RANKING_TOP_REMAINS",
            "OBSERVATION_ACCEPTANCE_CRITERIA_NOT_MET",
        ],
        "acceptance_criteria_review": {
            "criteria_relaxed": False,
            "reason": (
                "2381 does not change observation acceptance criteria; it routes "
                "to a different candidate direction because the current line has "
                "not satisfied existing criteria."
            ),
        },
    }


def _next_candidate_direction(
    source_findings: Mapping[str, Any],
    plateau_review: Mapping[str, Any],
    *,
    ready: bool,
) -> dict[str, Any]:
    return {
        "schema_version": "dynamic_strategy_next_candidate_direction.v1",
        "status": READY_STATUS if ready else BLOCKED_SOURCE_STATUS,
        "next_direction_decision": NEXT_DIRECTION_DECISION,
        "recommended_default_direction": RECOMMENDED_DEFAULT_DIRECTION,
        "recommended_next_research_task": NEXT_ROUTE,
        "decision_options": [
            {
                "option": "OPTION_A_CONTINUE_LOWER_TURNOVER_OPTIMIZATION",
                "decision": "DEFER",
                "reason": (
                    "Current lower-turnover line has value, but repeated local "
                    "variants still failed observation criteria."
                ),
            },
            {
                "option": "OPTION_B_SWITCH_TO_RANKING_TOP_GUARDED_VARIANT",
                "decision": "SELECT",
                "reason": (
                    "Ranking top still has return advantage; next work should test "
                    "guarded-turnover and risk-cap controls around that candidate."
                ),
            },
            {
                "option": "OPTION_C_EXPAND_CANDIDATE_POOL",
                "decision": "DEFER",
                "reason": "Useful after guarded ranking-top retest clarifies local evidence.",
            },
            {
                "option": "OPTION_D_PAUSE_AND_IMPROVE_DATA_PIT_COVERAGE",
                "decision": "NOT_SELECTED",
                "reason": (
                    "No new data-quality blocker is introduced by this prior-artifact "
                    "decision task."
                ),
            },
            {
                "option": "OPTION_E_STOP_DYNAMIC_STRATEGY_LINE_FOR_NOW",
                "decision": "NOT_SELECTED",
                "reason": (
                    "Evidence is insufficient for observation but still supports "
                    "research-only candidate work."
                ),
            },
        ],
        "reasoning": {
            "lower_turnover_line_has_value": source_findings[
                "lower_turnover_line_has_value"
            ],
            "lower_turnover_line_not_accepted_for_observation": source_findings[
                "lower_turnover_line_not_accepted_for_observation"
            ],
            "ranking_top_still_has_return_advantage": source_findings[
                "ranking_top_still_has_return_advantage"
            ],
            "need_guarded_ranking_top_retest": True,
            "optimization_plateau_detected": plateau_review[
                "optimization_plateau_detected"
            ],
        },
    }


def _base_payload(
    *,
    status: str,
    as_of_date: date,
    sources: Mapping[str, Any],
    source_findings: Mapping[str, Any],
    plateau_review: Mapping[str, Any],
    next_direction: Mapping[str, Any],
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
        "source_ready_for_plateau_decision": ready,
        "data_quality_gate_executed": False,
        "data_quality_gate_reason": DATA_QUALITY_GATE_REASON,
        "fresh_market_data_read": False,
        "backtest_run": False,
        "new_signal_generated": False,
        "research_only": True,
        "observe_only": True,
        "base_candidate": sources.get("base_candidate"),
        "ranking_top_reference": sources.get("ranking_top_reference"),
        "best_variant_from_2379": sources.get("best_variant_from_2379"),
        "best_variant_decision_from_2379": sources.get(
            "best_variant_decision_from_2379"
        ),
        "observation_approved_from_2380": source_findings[
            "observation_approved_from_2380"
        ],
        "primary_execution_cadence": sources.get("primary_execution_cadence"),
        "optimization_plateau_review_ready": ready,
        "optimization_plateau_detected": plateau_review[
            "optimization_plateau_detected"
        ],
        "optimization_plateau_review": dict(plateau_review),
        "source_findings": dict(source_findings),
        "next_direction_decision": next_direction["next_direction_decision"],
        "recommended_default_direction": next_direction[
            "recommended_default_direction"
        ],
        "next_candidate_direction": dict(next_direction),
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
        "summary_findings": [
            (
                "Lower-turnover line remains research-relevant, but the latest best "
                "variant still failed observation acceptance."
            ),
            (
                "The current line is treated as a local optimization plateau; "
                "2381 routes to guarded ranking-top retest planning."
            ),
            (
                "Paper-shadow, event append, outcome binding, scheduler, daily "
                "report, production and broker paths remain disabled."
            ),
        ],
        "artifact_paths": {},
    }


def _write_outputs(*, payload: dict[str, Any], output_root: Path, docs_root: Path) -> None:
    output_root.mkdir(parents=True, exist_ok=True)
    docs_root.mkdir(parents=True, exist_ok=True)
    plateau_json = output_root / "optimization_plateau_decision.json"
    direction_json = output_root / "next_candidate_direction.json"
    markdown_path = (
        docs_root / "dynamic_strategy_optimization_plateau_next_candidate_decision.md"
    )
    plateau_markdown = docs_root / "dynamic_strategy_plateau_review.md"
    direction_markdown = docs_root / "dynamic_strategy_next_candidate_direction.md"
    route_markdown = docs_root / "dynamic_strategy_2382_route.md"
    payload["artifact_paths"] = {
        "json_path": str(plateau_json),
        "optimization_plateau_decision_json": str(plateau_json),
        "next_candidate_direction_json": str(direction_json),
        "markdown_path": str(markdown_path),
        "plateau_review_markdown": str(plateau_markdown),
        "next_candidate_direction_markdown": str(direction_markdown),
        "next_route_markdown": str(route_markdown),
    }
    plateau_json.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    direction_json.write_text(
        json.dumps(
            {
                "generated_at": payload["generated_at"],
                "report_type": REPORT_TYPE,
                "schema_version": "dynamic_strategy_next_candidate_direction.v1",
                "status": payload["status"],
                "production_effect": "none",
                "broker_action": "none",
                "next_candidate_direction": payload["next_candidate_direction"],
            },
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    markdown_path.write_text(_main_markdown(payload), encoding="utf-8")
    plateau_markdown.write_text(_plateau_markdown(payload), encoding="utf-8")
    direction_markdown.write_text(_direction_markdown(payload), encoding="utf-8")
    route_markdown.write_text(_route_markdown(payload), encoding="utf-8")


def _main_markdown(payload: Mapping[str, Any]) -> str:
    source = _as_mapping(payload.get("source_findings"))
    plateau = _as_mapping(payload.get("optimization_plateau_review"))
    return "\n".join(
        [
            "# Dynamic strategy optimization plateau and next candidate decision",
            "",
            "## Executive summary",
            "",
            f"- status：`{payload['status']}`",
            f"- optimization plateau：`{payload['optimization_plateau_detected']}`",
            f"- next direction：`{payload['next_direction_decision']}`",
            f"- recommended route：`{payload['recommended_next_research_task']}`",
            "- research-only observation approved：`false`",
            "- paper-shadow / production / broker：`false` / `false` / `none`",
            "",
            "## Source findings from TRADING-2379 / 2380",
            "",
            f"- best variant from 2379：`{payload['best_variant_from_2379']}`",
            f"- best variant decision：`{payload['best_variant_decision_from_2379']}`",
            f"- observation approved from 2380：`{payload['observation_approved_from_2380']}`",
            f"- return gap reduction vs base：`{source.get('return_gap_reduction_vs_base')}`",
            f"- variant vs ranking top gap：`{source.get('variant_vs_ranking_top_gap')}`",
            f"- time slice pass rate：`{source.get('time_slice_pass_rate')}`",
            f"- regime slice pass rate：`{source.get('regime_slice_pass_rate')}`",
            "",
            "## Lower-turnover optimization review",
            "",
            "当前 lower-turnover 线仍有研究价值，但多轮优化后 best decision 仍为 "
            "`CONTINUE_OPTIMIZATION`，不满足 observation 门槛。",
            "",
            "## Plateau assessment",
            "",
            f"- plateau scope：`{plateau.get('plateau_scope')}`",
            _markdown_bullets(plateau.get("plateau_evidence", [])),
            "",
            "## Why observation is still not approved",
            "",
            _markdown_bullets(plateau.get("primary_blockers", [])),
            "",
            "## Next-direction options",
            "",
            _option_table(payload.get("next_candidate_direction")),
            "",
            "## Recommended next direction",
            "",
            "`OPTION_B_SWITCH_TO_RANKING_TOP_GUARDED_VARIANT`。下一步应回到 2365 收益 top，"
            "用 lower-turnover guardrail、risk cap 和 cooldown 约束修复其脆弱性。",
            "",
            "## Explicit non-goals",
            "",
            _markdown_bullets(payload.get("explicit_non_goals", [])),
            "",
            "## Guardrail summary",
            "",
            "- scheduler_enabled：`false`",
            "- event_append_enabled：`false`",
            "- outcome_binding_enabled：`false`",
            "- paper_shadow_enabled：`false`",
            "- production_enabled：`false`",
            "- broker_action_enabled：`false`",
            "- daily_report_generated：`false`",
            "",
            "## Recommended next route",
            "",
            f"`{payload['recommended_next_research_task']}`",
            "",
        ]
    )


def _plateau_markdown(payload: Mapping[str, Any]) -> str:
    plateau = _as_mapping(payload.get("optimization_plateau_review"))
    return "\n".join(
        [
            "# Dynamic strategy plateau review",
            "",
            f"- status：`{payload['status']}`",
            f"- plateau detected：`{payload['optimization_plateau_detected']}`",
            f"- best variant：`{payload['best_variant_from_2379']}`",
            f"- 2379 decision：`{payload['best_variant_decision_from_2379']}`",
            "",
            "## Evidence",
            "",
            _markdown_bullets(plateau.get("plateau_evidence", [])),
            "",
            "## Blockers",
            "",
            _markdown_bullets(plateau.get("primary_blockers", [])),
            "",
        ]
    )


def _direction_markdown(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# Dynamic strategy next candidate direction",
            "",
            f"- status：`{payload['status']}`",
            f"- next direction：`{payload['next_direction_decision']}`",
            f"- default direction：`{payload['recommended_default_direction']}`",
            f"- next route：`{payload['recommended_next_research_task']}`",
            "",
            "## Options",
            "",
            _option_table(payload.get("next_candidate_direction")),
            "",
        ]
    )


def _route_markdown(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# TRADING-2382 route",
            "",
            f"- source task：`{TASK_ID}`",
            f"- status：`{payload['status']}`",
            f"- next direction：`{payload['next_direction_decision']}`",
            f"- next route：`{payload['recommended_next_research_task']}`",
            "- route reason：lower-turnover local optimization reached plateau; "
            "ranking top still has return advantage and needs guarded-turnover retest plan.",
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


def _resolve_as_of(as_of_date: date | None, owner_review: Mapping[str, Any]) -> date:
    if as_of_date is not None:
        return as_of_date
    value = owner_review.get("as_of")
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


def _robustness_top(sensitivity_result: Mapping[str, Any]) -> Mapping[str, Any]:
    rows = sensitivity_result.get("robustness_ranking")
    if isinstance(rows, list) and rows:
        first = rows[0]
        if isinstance(first, Mapping):
            return first
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


def _ranking_top_has_return_advantage(
    *,
    ranking_top_return: float | None,
    best_return: float | None,
    variant_vs_ranking_gap: float | None,
) -> bool:
    if variant_vs_ranking_gap is not None:
        return variant_vs_ranking_gap < 0
    if ranking_top_return is None or best_return is None:
        return False
    return ranking_top_return > best_return


def _float_or_none(value: object) -> float | None:
    if isinstance(value, int | float):
        return float(value)
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


def _option_table(direction: object) -> str:
    rows = _as_mapping(direction).get("decision_options")
    if not isinstance(rows, list):
        return "|option|decision|reason|\n|---|---|---|"
    lines = ["|option|decision|reason|", "|---|---|---|"]
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        lines.append(
            "|"
            f"`{row.get('option')}`|"
            f"`{row.get('decision')}`|"
            f"{row.get('reason')}|"
        )
    return "\n".join(lines)
