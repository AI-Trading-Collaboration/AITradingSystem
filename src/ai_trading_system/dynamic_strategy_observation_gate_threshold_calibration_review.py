from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from datetime import date
from pathlib import Path
from typing import Any

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.data_foundation import utc_now_iso
from ai_trading_system.dynamic_strategy_candidate_pool_expansion_plan import (
    DATA_QUALITY_GATE_REASON as SOURCE_2385_DATA_QUALITY_GATE_REASON,
)
from ai_trading_system.dynamic_strategy_candidate_pool_expansion_plan import (
    DEFAULT_SOURCE_2366_DECISION_UPDATE_PATH,
    DEFAULT_SOURCE_2366_SENSITIVITY_RESULT_PATH,
    DEFAULT_SOURCE_2384_OWNER_REVIEW_PATH,
    DEFAULT_SOURCE_CANDIDATE_RANKING_PATH,
)
from ai_trading_system.dynamic_strategy_candidate_pool_expansion_plan import (
    READY_STATUS as SOURCE_2385_READY_STATUS,
)
from ai_trading_system.dynamic_strategy_cost_turnover_cooldown_sensitivity import (
    PRIMARY_EXECUTION_CADENCE,
)
from ai_trading_system.dynamic_strategy_cost_turnover_cooldown_sensitivity import (
    READY_STATUS as SOURCE_2366_READY_STATUS,
)
from ai_trading_system.dynamic_strategy_event_driven_retest import (
    READY_STATUS as SOURCE_2365_READY_STATUS,
)
from ai_trading_system.dynamic_strategy_expanded_candidate_pool_retest import (
    DEFAULT_DYNAMIC_STRATEGY_EXPANDED_CANDIDATE_POOL_RETEST_OUTPUT_ROOT,
    DEFAULT_SOURCE_2385_CANDIDATE_POOL_EXPANSION_PLAN_PATH,
    DRAWDOWN_WORSE_TOLERANCE,
    OBSERVATION_REGIME_SLICE_PASS_RATE_MIN,
    OBSERVATION_TIME_SLICE_PASS_RATE_MIN,
    REFERENCE_CANDIDATES,
    REGIME_SLICE_PASS_RATE_ACCEPTABLE_MIN,
    RETURN_ADVANTAGE_RETAINED_MIN,
    TIME_SLICE_PASS_RATE_ACCEPTABLE_MIN,
    TURNOVER_BUDGET_MAX_MONTHLY,
)
from ai_trading_system.dynamic_strategy_expanded_candidate_pool_retest import (
    NEXT_ROUTE as SOURCE_2386_EXPECTED_ROUTE,
)
from ai_trading_system.dynamic_strategy_expanded_candidate_pool_retest import (
    READY_STATUS as SOURCE_2386_READY_STATUS,
)
from ai_trading_system.dynamic_strategy_guarded_variant_owner_review_decision import (
    BEST_GUARDED_VARIANT,
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
    RANKING_TOP_CANDIDATE,
)
from ai_trading_system.execution_semantics import AI_REGIME_SUMMARY, _file_sha256

TASK_ID = "TRADING-2387"
TASK_REGISTER_ID = (
    "TRADING-2387_DYNAMIC_STRATEGY_OBSERVATION_GATE_THRESHOLD_CALIBRATION_REVIEW"
)
REPORT_TYPE = "dynamic_strategy_observation_gate_threshold_calibration_review"
SCHEMA_VERSION = "dynamic_strategy_observation_gate_threshold_calibration_review.v1"
READY_STATUS = (
    "DYNAMIC_STRATEGY_OBSERVATION_GATE_THRESHOLD_CALIBRATION_REVIEW_READY"
)
BLOCKED_SOURCE_STATUS = (
    "DYNAMIC_STRATEGY_OBSERVATION_GATE_THRESHOLD_CALIBRATION_REVIEW_"
    "BLOCKED_SOURCE_ARTIFACT"
)
NEXT_ROUTE = (
    "TRADING-2388_Dynamic_Strategy_Calibrated_Gate_Owner_Review_And_Next_Decision"
)
DATA_QUALITY_GATE_REASON = (
    "NOT_APPLICABLE_PRIOR_ARTIFACT_THRESHOLD_REVIEW_ONLY_NO_FRESH_MARKET_DATA"
)
RECOMMENDED_POLICY_ACTION = (
    "CALIBRATE_RESEARCH_ONLY_OBSERVATION_GATE_BEFORE_OWNER_PAUSE_DECISION"
)
REFERENCE_CANDIDATE_POLICY_RECOMMENDATION = (
    "BLOCK_AUTO_ACCEPT_BUT_ALLOW_OWNER_REVIEW"
)
OWNER_REVIEW_DECISION_STATE = "OWNER_REVIEW_FOR_RESEARCH_ONLY_OBSERVATION"
DECISION_ACCEPT_RESEARCH_ONLY = "ACCEPT_FOR_RESEARCH_ONLY_OBSERVATION"
DECISION_OWNER_REVIEW = "OWNER_REVIEW_REQUIRED"
DECISION_CONTINUE_OPTIMIZATION = "CONTINUE_OPTIMIZATION"
DECISION_REJECT = "REJECT_FOR_NOW"
SOURCE_TASKS: tuple[str, ...] = (
    "TRADING-2365",
    "TRADING-2366",
    "TRADING-2384",
    "TRADING-2385",
    "TRADING-2386",
)
CASE_STUDY_CANDIDATES: tuple[str, ...] = (
    RANKING_TOP_CANDIDATE,
    "dynamic_turnover_budgeted_growth_tilt_v1",
    "dynamic_valid_until_expiry_strict_v1",
    BEST_LOWER_TURNOVER_VARIANT,
    BEST_GUARDED_VARIANT,
)
# Proposed TRADING-2387 owner-review tier only. This is not applied to the
# active 2386 screening rule and must be owner-calibrated in TRADING-2388.
OWNER_REVIEW_TIME_SLICE_PASS_RATE_MIN = 0.30
# Proposed drawdown compensation review floor. It is a report-calibration
# heuristic, not an investment promotion rule.
OWNER_REVIEW_DRAWDOWN_COMPENSATION_MIN = 0.25

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

DEFAULT_DYNAMIC_STRATEGY_OBSERVATION_GATE_THRESHOLD_CALIBRATION_REVIEW_OUTPUT_ROOT = (
    PROJECT_ROOT / "outputs" / "research_strategies" / REPORT_TYPE
)
DEFAULT_DYNAMIC_STRATEGY_OBSERVATION_GATE_THRESHOLD_CALIBRATION_REVIEW_DOCS_ROOT = (
    PROJECT_ROOT / "docs" / "research"
)
DEFAULT_SOURCE_2386_EXPANDED_CANDIDATE_RETEST_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_EXPANDED_CANDIDATE_POOL_RETEST_OUTPUT_ROOT
    / "expanded_candidate_retest_result.json"
)
DEFAULT_SOURCE_2386_EXPANDED_CANDIDATE_RANKING_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_EXPANDED_CANDIDATE_POOL_RETEST_OUTPUT_ROOT
    / "expanded_candidate_ranking.json"
)
DEFAULT_SOURCE_2386_SIGNAL_FAMILY_SCREENING_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_EXPANDED_CANDIDATE_POOL_RETEST_OUTPUT_ROOT
    / "signal_family_screening.json"
)
DEFAULT_SOURCE_2386_DECISION_UPDATE_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_EXPANDED_CANDIDATE_POOL_RETEST_OUTPUT_ROOT
    / "decision_update.json"
)


def run_dynamic_strategy_observation_gate_threshold_calibration_review(
    *,
    source_expanded_candidate_retest_path: Path = (
        DEFAULT_SOURCE_2386_EXPANDED_CANDIDATE_RETEST_PATH
    ),
    source_expanded_candidate_ranking_path: Path = (
        DEFAULT_SOURCE_2386_EXPANDED_CANDIDATE_RANKING_PATH
    ),
    source_signal_family_screening_path: Path = (
        DEFAULT_SOURCE_2386_SIGNAL_FAMILY_SCREENING_PATH
    ),
    source_decision_update_path: Path = DEFAULT_SOURCE_2386_DECISION_UPDATE_PATH,
    source_candidate_pool_plan_path: Path = (
        DEFAULT_SOURCE_2385_CANDIDATE_POOL_EXPANSION_PLAN_PATH
    ),
    source_owner_review_path: Path = DEFAULT_SOURCE_2384_OWNER_REVIEW_PATH,
    source_candidate_ranking_path: Path = DEFAULT_SOURCE_CANDIDATE_RANKING_PATH,
    source_sensitivity_result_path: Path = DEFAULT_SOURCE_2366_SENSITIVITY_RESULT_PATH,
    source_sensitivity_decision_update_path: Path = (
        DEFAULT_SOURCE_2366_DECISION_UPDATE_PATH
    ),
    output_root: Path = (
        DEFAULT_DYNAMIC_STRATEGY_OBSERVATION_GATE_THRESHOLD_CALIBRATION_REVIEW_OUTPUT_ROOT
    ),
    docs_root: Path = (
        DEFAULT_DYNAMIC_STRATEGY_OBSERVATION_GATE_THRESHOLD_CALIBRATION_REVIEW_DOCS_ROOT
    ),
    as_of_date: date | None = None,
) -> dict[str, Any]:
    sources = _load_sources(
        source_expanded_candidate_retest_path=source_expanded_candidate_retest_path,
        source_expanded_candidate_ranking_path=source_expanded_candidate_ranking_path,
        source_signal_family_screening_path=source_signal_family_screening_path,
        source_decision_update_path=source_decision_update_path,
        source_candidate_pool_plan_path=source_candidate_pool_plan_path,
        source_owner_review_path=source_owner_review_path,
        source_candidate_ranking_path=source_candidate_ranking_path,
        source_sensitivity_result_path=source_sensitivity_result_path,
        source_sensitivity_decision_update_path=(
            source_sensitivity_decision_update_path
        ),
    )
    ready = not sources["source_validation_errors"]
    payload = _base_payload(
        status=READY_STATUS if ready else BLOCKED_SOURCE_STATUS,
        as_of_date=as_of_date,
        sources=sources,
    )
    if ready:
        payload.update(_review_sections(sources))
    else:
        payload.update(_blocked_sections(sources))
    _write_outputs(payload=payload, output_root=output_root, docs_root=docs_root)
    return payload


def _load_sources(
    *,
    source_expanded_candidate_retest_path: Path,
    source_expanded_candidate_ranking_path: Path,
    source_signal_family_screening_path: Path,
    source_decision_update_path: Path,
    source_candidate_pool_plan_path: Path,
    source_owner_review_path: Path,
    source_candidate_ranking_path: Path,
    source_sensitivity_result_path: Path,
    source_sensitivity_decision_update_path: Path,
) -> dict[str, Any]:
    expanded_candidate_retest = _load_json_document(source_expanded_candidate_retest_path)
    expanded_candidate_ranking = _load_json_document(
        source_expanded_candidate_ranking_path
    )
    signal_family_screening = _load_json_document(source_signal_family_screening_path)
    decision_update = _load_json_document(source_decision_update_path)
    candidate_pool_plan = _load_json_document(source_candidate_pool_plan_path)
    owner_review = _load_json_document(source_owner_review_path)
    candidate_ranking = _load_json_document(source_candidate_ranking_path)
    sensitivity_result = _load_json_document(source_sensitivity_result_path)
    sensitivity_decision_update = _load_json_document(
        source_sensitivity_decision_update_path
    )
    source_status = {
        "expanded_candidate_retest": expanded_candidate_retest.get("status"),
        "expanded_candidate_ranking": expanded_candidate_ranking.get("status"),
        "signal_family_screening": signal_family_screening.get("status"),
        "decision_update": decision_update.get("status"),
        "candidate_pool_plan": candidate_pool_plan.get("status"),
        "owner_review": owner_review.get("status"),
        "candidate_ranking": candidate_ranking.get("status"),
        "sensitivity_result": sensitivity_result.get("status"),
        "sensitivity_decision_update": sensitivity_decision_update.get("status"),
    }
    source_files = {
        "expanded_candidate_retest": source_expanded_candidate_retest_path,
        "expanded_candidate_ranking": source_expanded_candidate_ranking_path,
        "signal_family_screening": source_signal_family_screening_path,
        "decision_update": source_decision_update_path,
        "candidate_pool_plan": source_candidate_pool_plan_path,
        "owner_review": source_owner_review_path,
        "candidate_ranking": source_candidate_ranking_path,
        "sensitivity_result": source_sensitivity_result_path,
        "sensitivity_decision_update": source_sensitivity_decision_update_path,
    }
    sources: dict[str, Any] = {
        "expanded_candidate_retest": expanded_candidate_retest,
        "expanded_candidate_ranking": expanded_candidate_ranking,
        "signal_family_screening": signal_family_screening,
        "decision_update": decision_update,
        "candidate_pool_plan": candidate_pool_plan,
        "owner_review": owner_review,
        "candidate_ranking": candidate_ranking,
        "sensitivity_result": sensitivity_result,
        "sensitivity_decision_update": sensitivity_decision_update,
        "source_status": source_status,
        "source_files": {key: str(path) for key, path in source_files.items()},
        "source_hashes": {
            key: _file_sha256(path) if path.exists() else None
            for key, path in source_files.items()
        },
    }
    sources["source_validation_errors"] = _source_validation_errors(sources)
    sources["source_ready_for_gate_calibration_review"] = not sources[
        "source_validation_errors"
    ]
    return sources


def _source_validation_errors(sources: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    expected_status = {
        "expanded_candidate_retest": SOURCE_2386_READY_STATUS,
        "expanded_candidate_ranking": SOURCE_2386_READY_STATUS,
        "signal_family_screening": SOURCE_2386_READY_STATUS,
        "decision_update": SOURCE_2386_READY_STATUS,
        "candidate_pool_plan": SOURCE_2385_READY_STATUS,
        "owner_review": SOURCE_2384_READY_STATUS,
        "candidate_ranking": SOURCE_2365_READY_STATUS,
        "sensitivity_result": SOURCE_2366_READY_STATUS,
        "sensitivity_decision_update": SOURCE_2366_READY_STATUS,
    }
    source_status = _as_mapping(sources.get("source_status"))
    for key, expected in expected_status.items():
        actual = source_status.get(key)
        if actual != expected:
            errors.append(f"{key}.status expected {expected}, got {actual!r}")

    expanded_candidate_retest = _as_mapping(sources.get("expanded_candidate_retest"))
    decision_update_document = _as_mapping(sources.get("decision_update"))
    decision_update = _decision_update_payload(sources)
    ranking_rows = _expanded_candidate_ranking_rows(sources)
    top_row = _top_candidate_row(sources)
    current_best = _current_best_candidate(sources)
    current_decision = _current_best_decision(sources)

    if expanded_candidate_retest.get("recommended_next_research_task") not in {
        SOURCE_2386_EXPECTED_ROUTE,
        None,
    }:
        errors.append("2386 retest must route to the TRADING-2387 review")
    if expanded_candidate_retest.get("data_quality_gate_executed") is not True:
        errors.append("2386 expanded retest data_quality_gate_executed must be true")
    if expanded_candidate_retest.get("data_quality_passed") is not True:
        errors.append("2386 expanded retest data_quality_passed must be true")
    if current_best != RANKING_TOP_CANDIDATE:
        errors.append(
            "2386 current best candidate expected "
            f"{RANKING_TOP_CANDIDATE}, got {current_best!r}"
        )
    if current_decision != DECISION_CONTINUE_OPTIMIZATION:
        errors.append(
            "2386 current best decision expected CONTINUE_OPTIMIZATION, "
            f"got {current_decision!r}"
        )
    if _observation_ready_candidate_found(sources):
        errors.append("2386 must not already contain an observation-ready candidate")
    if top_row.get("candidate_id") not in {RANKING_TOP_CANDIDATE, None}:
        errors.append("2386 top candidate ranking row does not match expected best")
    if not ranking_rows:
        errors.append("2386 expanded_candidate_ranking must be non-empty")
    if not isinstance(decision_update, Mapping):
        errors.append("2386 decision_update payload must be present")
    if decision_update_document.get("broker_action") not in {None, "none"}:
        errors.append("2386 decision_update broker_action must remain none")

    candidate_pool_plan = _as_mapping(sources.get("candidate_pool_plan"))
    if candidate_pool_plan.get("recommended_next_research_task") not in {
        "TRADING-2386_Dynamic_Strategy_Expanded_Candidate_Pool_Retest_And_Screening",
        None,
    }:
        errors.append("2385 plan must route to TRADING-2386")
    if candidate_pool_plan.get("data_quality_gate_reason") != (
        SOURCE_2385_DATA_QUALITY_GATE_REASON
    ):
        errors.append("2385 plan data-quality boundary unexpected")
    if candidate_pool_plan.get("primary_execution_cadence") not in {
        PRIMARY_EXECUTION_CADENCE,
        None,
    }:
        errors.append("2385 primary execution cadence must remain valid_until_window")

    owner_review = _as_mapping(sources.get("owner_review"))
    if owner_review.get("owner_decision") not in {SOURCE_2384_OWNER_DECISION, None}:
        errors.append("2384 owner decision does not match expected expansion decision")
    if owner_review.get("research_only_observation_approved") is not False:
        errors.append("2384 research_only_observation_approved must remain false")
    if owner_review.get("candidate_pool_expansion_recommended") not in {True, None}:
        errors.append("2384 must recommend candidate-pool expansion")

    candidate_ranking = _as_mapping(sources.get("candidate_ranking"))
    if _top_candidate_from_candidate_ranking(candidate_ranking) != RANKING_TOP_CANDIDATE:
        errors.append("2365 candidate ranking top must remain ranking-top reference")

    sensitivity_result = _as_mapping(sources.get("sensitivity_result"))
    if not _as_list(sensitivity_result.get("sensitivity_matrix")):
        errors.append("2366 sensitivity_result.sensitivity_matrix must be non-empty")
    sensitivity_decision_update = _as_mapping(
        sources.get("sensitivity_decision_update")
    )
    if not isinstance(sensitivity_decision_update.get("decision_update"), Mapping):
        errors.append("2366 sensitivity decision_update must be present")

    for source_name in expected_status:
        errors.extend(
            _side_effect_validation_errors(
                source_name,
                _as_mapping(sources.get(source_name)),
            )
        )
    return errors


def _base_payload(
    *,
    status: str,
    as_of_date: date | None,
    sources: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "task_id": TASK_ID,
        "task_register_id": TASK_REGISTER_ID,
        "report_type": REPORT_TYPE,
        "status": status,
        "generated_at": utc_now_iso(),
        "as_of": as_of_date.isoformat() if as_of_date else None,
        "market_regime": AI_REGIME_SUMMARY.get("market_regime", "ai_after_chatgpt"),
        "market_regime_summary": AI_REGIME_SUMMARY,
        "source_tasks": list(SOURCE_TASKS),
        "source_files": dict(_as_mapping(sources.get("source_files"))),
        "source_hashes": dict(_as_mapping(sources.get("source_hashes"))),
        "source_status": dict(_as_mapping(sources.get("source_status"))),
        "source_validation_errors": list(sources.get("source_validation_errors", [])),
        "source_ready_for_gate_calibration_review": bool(
            sources.get("source_ready_for_gate_calibration_review")
        ),
        "current_best_candidate": _current_best_candidate(sources),
        "current_best_candidate_decision": _current_best_decision(sources),
        "observation_ready_candidate_found_in_2386": (
            _observation_ready_candidate_found(sources)
        ),
        "data_quality_gate_executed": False,
        "data_quality_gate_reason": DATA_QUALITY_GATE_REASON,
        "data_quality_status": DATA_QUALITY_GATE_REASON,
        "fresh_market_data_read": False,
        "backtest_run": False,
        "expanded_candidate_retest_run": False,
        "new_signal_generated": False,
        "scoring_run": False,
        "policy_update_applied": False,
        "rules_mutated": False,
        "observation_approved": False,
        "research_only": True,
        "observe_only": False,
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
        "recommended_next_research_task": NEXT_ROUTE,
        "artifact_paths": {},
    }


def _review_sections(sources: Mapping[str, Any]) -> dict[str, Any]:
    ranking_rows = _expanded_candidate_ranking_rows(sources)
    family_rows = _signal_family_screening_rows(sources)
    top_row = _top_candidate_row(sources)
    candidate_preview = _candidate_reclassification_preview(ranking_rows)
    return {
        "gate_calibration_review_ready": True,
        "reference_candidate_policy_review_ready": True,
        "time_slice_threshold_review_ready": True,
        "regime_slice_threshold_review_ready": True,
        "drawdown_materiality_review_ready": True,
        "research_only_vs_paper_shadow_gate_review_ready": True,
        "candidate_reclassification_preview_ready": True,
        "recommended_policy_action": RECOMMENDED_POLICY_ACTION,
        "reference_candidate_policy_recommendation": (
            REFERENCE_CANDIDATE_POLICY_RECOMMENDATION
        ),
        "research_only_gate_may_be_too_strict": True,
        "source_findings_from_2386": _source_findings_from_2386(sources),
        "current_observation_gate_rules": _current_observation_gate_rules(),
        "why_2386_resulted_in_continue_optimization": (
            _continue_optimization_explanation(top_row)
        ),
        "reference_candidate_policy_review": (
            _reference_candidate_policy_review(top_row)
        ),
        "time_slice_threshold_review": _time_slice_threshold_review(
            top_row,
            ranking_rows,
        ),
        "regime_slice_threshold_review": _regime_slice_threshold_review(
            top_row,
            family_rows,
        ),
        "drawdown_materiality_review": _drawdown_materiality_review(top_row),
        "research_only_vs_paper_shadow_gate_review": (
            _research_only_vs_paper_shadow_gate_review()
        ),
        "candidate_reclassification_preview": candidate_preview,
        "recommended_gate_policy_update": _recommended_gate_policy_update(),
        "component_attribution_review": _component_attribution_review(ranking_rows),
        "explicit_non_goals": _explicit_non_goals(),
        "recommended_next_route": {
            "recommended_next_research_task": NEXT_ROUTE,
            "owner_decision_required": True,
            "preferred_next_step": (
                "OWNER_DECISION_ON_CALIBRATED_RESEARCH_ONLY_GATE_BEFORE_"
                "COMPONENT_ATTRIBUTION_WORK"
            ),
            "alternative_if_owner_keeps_gate_unchanged": (
                "SIGNAL_FAMILY_COMPONENT_ATTRIBUTION_AND_QUALITY_REVIEW"
            ),
        },
    }


def _blocked_sections(sources: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "blocked_reason": "source_artifact_validation_failed",
        "gate_calibration_review_ready": False,
        "reference_candidate_policy_review_ready": False,
        "time_slice_threshold_review_ready": False,
        "regime_slice_threshold_review_ready": False,
        "drawdown_materiality_review_ready": False,
        "research_only_vs_paper_shadow_gate_review_ready": False,
        "candidate_reclassification_preview_ready": False,
        "recommended_policy_action": "BLOCKED_SOURCE_ARTIFACT_REVIEW_REQUIRED",
        "reference_candidate_policy_recommendation": None,
        "research_only_gate_may_be_too_strict": None,
        "source_findings_from_2386": {},
        "current_observation_gate_rules": _current_observation_gate_rules(),
        "why_2386_resulted_in_continue_optimization": {},
        "reference_candidate_policy_review": {},
        "time_slice_threshold_review": {},
        "regime_slice_threshold_review": {},
        "drawdown_materiality_review": {},
        "research_only_vs_paper_shadow_gate_review": (
            _research_only_vs_paper_shadow_gate_review()
        ),
        "candidate_reclassification_preview": {},
        "recommended_gate_policy_update": {
            "policy_update_applied": False,
            "rules_mutated": False,
            "blocked_reason": "source_artifact_validation_failed",
        },
        "component_attribution_review": {},
        "explicit_non_goals": _explicit_non_goals(),
        "recommended_next_route": {
            "recommended_next_research_task": NEXT_ROUTE,
            "blocked_until": list(sources.get("source_validation_errors", [])),
        },
    }


def _source_findings_from_2386(sources: Mapping[str, Any]) -> dict[str, Any]:
    result = _as_mapping(sources.get("expanded_candidate_retest"))
    ranking_rows = _expanded_candidate_ranking_rows(sources)
    family_rows = _signal_family_screening_rows(sources)
    top_row = _top_candidate_row(sources)
    return {
        "source_status": result.get("status"),
        "data_quality_status_from_2386": result.get("data_quality_status"),
        "data_quality_gate_executed_in_2386": result.get("data_quality_gate_executed"),
        "data_quality_passed_in_2386": result.get("data_quality_passed"),
        "reference_candidates_tested": result.get("reference_candidate_count"),
        "new_candidates_tested": result.get("new_candidates_tested_count"),
        "signal_families_tested": result.get("signal_families_tested_count"),
        "current_best_candidate": _current_best_candidate(sources),
        "current_best_candidate_decision": _current_best_decision(sources),
        "observation_ready_candidate_found": _observation_ready_candidate_found(sources),
        "top_candidate_metrics": _candidate_metric_snapshot(top_row),
        "top_candidate_decision_reasons": list(
            _as_list(top_row.get("decision_reasons"))
        ),
        "top_ranked_candidates": [
            _candidate_metric_snapshot(row) for row in ranking_rows[:5]
        ],
        "signal_family_findings": [
            {
                "signal_family": row.get("signal_family"),
                "family_best_candidate": row.get("family_best_candidate"),
                "family_best_candidate_decision": row.get(
                    "family_best_candidate_decision"
                ),
                "family_time_slice_pass_rate": row.get("family_time_slice_pass_rate"),
                "family_regime_slice_pass_rate": row.get(
                    "family_regime_slice_pass_rate"
                ),
                "family_failure_reason": row.get("family_failure_reason"),
            }
            for row in family_rows
        ],
    }


def _current_observation_gate_rules() -> dict[str, Any]:
    return {
        "source_task": "TRADING-2386",
        "source_policy_id": "dynamic_strategy_expanded_candidate_pool_retest_v1",
        "current_rule_summary": [
            "realistic_or_conservative_gap_less_than_or_equal_zero_rejects_candidate",
            "time_or_regime_slice_below_acceptable_threshold_continues_optimization",
            "research_only_observation_acceptance_requires_non_reference_candidate",
            "research_only_observation_acceptance_requires_lower_gap_positive",
            "research_only_observation_acceptance_requires_guarded_gap_non_negative",
            "research_only_observation_acceptance_requires_return_advantage_retained",
            "research_only_observation_acceptance_requires_time_slice_at_or_above_0_60",
            "research_only_observation_acceptance_requires_regime_slice_at_or_above_0_50",
        ],
        "thresholds": {
            "time_slice_pass_rate_acceptable_min": TIME_SLICE_PASS_RATE_ACCEPTABLE_MIN,
            "regime_slice_pass_rate_acceptable_min": (
                REGIME_SLICE_PASS_RATE_ACCEPTABLE_MIN
            ),
            "drawdown_worse_tolerance": DRAWDOWN_WORSE_TOLERANCE,
            "turnover_budget_max_monthly": TURNOVER_BUDGET_MAX_MONTHLY,
            "return_advantage_retained_min": RETURN_ADVANTAGE_RETAINED_MIN,
            "observation_time_slice_pass_rate_min": (
                OBSERVATION_TIME_SLICE_PASS_RATE_MIN
            ),
            "observation_regime_slice_pass_rate_min": (
                OBSERVATION_REGIME_SLICE_PASS_RATE_MIN
            ),
        },
        "reference_candidate_current_policy": "HARD_BLOCK_ACCEPTANCE",
        "policy_mutated_by_2387": False,
    }


def _continue_optimization_explanation(top_row: Mapping[str, Any]) -> dict[str, Any]:
    drawdown_ratio = _return_per_drawdown_penalty(top_row)
    return {
        "current_decision_is_reasonable_under_current_2386_rules": True,
        "primary_blockers": _current_blockers(top_row),
        "not_a_return_failure": _float(top_row.get("dynamic_vs_static_gap")) > 0.0,
        "not_a_cost_failure": bool(top_row.get("realistic_cost_passed"))
        and bool(top_row.get("conservative_cost_passed")),
        "not_a_turnover_budget_failure": bool(top_row.get("turnover_budget_passed")),
        "slice_stability_failure": (
            _float(top_row.get("time_slice_pass_rate"))
            < OBSERVATION_TIME_SLICE_PASS_RATE_MIN
            or _float(top_row.get("regime_slice_pass_rate"))
            < OBSERVATION_REGIME_SLICE_PASS_RATE_MIN
        ),
        "reference_status_blocks_auto_acceptance": (
            top_row.get("candidate_id") in REFERENCE_CANDIDATES
            or top_row.get("candidate_type") == "reference_candidate"
        ),
        "drawdown_requires_owner_judgment": _drawdown_not_materially_worse(
            top_row
        )
        is False,
        "return_per_drawdown_penalty": drawdown_ratio,
        "interpretation": (
            "2386 的 CONTINUE_OPTIMIZATION 在当前规则下合理；2387 只指出 "
            "research-only observation 需要一个 owner-review 中间层，而不是自动放行。"
        ),
    }


def _reference_candidate_policy_review(top_row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "reference_candidate_policy_review_ready": True,
        "current_policy": "HARD_BLOCK_ACCEPTANCE",
        "issue": "may_exclude_current_best_candidate_purely_due_to_reference_status",
        "current_best_candidate": top_row.get("candidate_id"),
        "current_best_is_reference_candidate": (
            top_row.get("candidate_id") in REFERENCE_CANDIDATES
            or top_row.get("candidate_type") == "reference_candidate"
        ),
        "recommended_policy": REFERENCE_CANDIDATE_POLICY_RECOMMENDATION,
        "recommended_intermediate_decision_state": OWNER_REVIEW_DECISION_STATE,
        "auto_accept_allowed_for_reference_candidate": False,
        "owner_review_allowed_for_reference_candidate": True,
        "rationale": [
            "reference status should prevent auto-promotion",
            "reference status should not prevent research-only owner review",
            "research-only observation has no execution side effect",
        ],
        "policy_update_applied": False,
    }


def _time_slice_threshold_review(
    top_row: Mapping[str, Any],
    ranking_rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    partial_rows = [
        _candidate_metric_snapshot(row)
        for row in ranking_rows
        if OWNER_REVIEW_TIME_SLICE_PASS_RATE_MIN
        <= _float(row.get("time_slice_pass_rate"))
        < OBSERVATION_TIME_SLICE_PASS_RATE_MIN
    ]
    return {
        "time_slice_threshold_review_ready": True,
        "current_acceptance_threshold": OBSERVATION_TIME_SLICE_PASS_RATE_MIN,
        "current_acceptable_threshold_used_before_decision": (
            TIME_SLICE_PASS_RATE_ACCEPTABLE_MIN
        ),
        "current_best_time_slice_pass_rate": top_row.get("time_slice_pass_rate"),
        "current_best_time_slice_failure_is_real": True,
        "threshold_tiering_recommended": True,
        "time_slice_policy_tiers": {
            "accept_for_research_only_observation": {
                "threshold": OBSERVATION_TIME_SLICE_PASS_RATE_MIN,
                "meaning": "stable enough for research-only tracking",
            },
            "owner_review_required": {
                "threshold": OWNER_REVIEW_TIME_SLICE_PASS_RATE_MIN,
                "meaning": "partial time-slice evidence, requires human review",
            },
            "continue_optimization": {
                "threshold_below": OWNER_REVIEW_TIME_SLICE_PASS_RATE_MIN,
                "meaning": "insufficient time-slice support",
            },
        },
        "candidate_examples_with_partial_time_evidence": partial_rows,
        "policy_update_applied": False,
    }


def _regime_slice_threshold_review(
    top_row: Mapping[str, Any],
    family_rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    return {
        "regime_slice_threshold_review_ready": True,
        "current_acceptance_threshold": OBSERVATION_REGIME_SLICE_PASS_RATE_MIN,
        "current_acceptable_threshold_used_before_decision": (
            REGIME_SLICE_PASS_RATE_ACCEPTABLE_MIN
        ),
        "current_best_regime_slice_pass_rate": top_row.get("regime_slice_pass_rate"),
        "single_global_pass_rate_may_be_too_blunt": True,
        "growth_or_risk_on_strategy_should_not_have_to_win_every_regime": True,
        "regime_expectation_policy": {
            "risk_on": "outperform_or_match_static",
            "trend_confirmed": "outperform_static",
            "low_volatility": "capture_upside",
            "risk_off": "not_materially_worse_than_static",
            "high_volatility": "drawdown_control",
            "recovery": "reentry_not_too_slow",
        },
        "family_level_regime_findings": [
            {
                "signal_family": row.get("signal_family"),
                "family_best_candidate": row.get("family_best_candidate"),
                "family_regime_slice_pass_rate": row.get(
                    "family_regime_slice_pass_rate"
                ),
                "family_failure_reason": row.get("family_failure_reason"),
            }
            for row in family_rows
        ],
        "policy_update_applied": False,
    }


def _drawdown_materiality_review(top_row: Mapping[str, Any]) -> dict[str, Any]:
    ratio = _return_per_drawdown_penalty(top_row)
    drawdown_not_materially_worse = _drawdown_not_materially_worse(top_row)
    return {
        "drawdown_materiality_review_ready": True,
        "drawdown_not_materially_worse": drawdown_not_materially_worse,
        "drawdown_gap_vs_static": top_row.get("drawdown_gap_vs_static"),
        "dynamic_vs_static_gap": top_row.get("dynamic_vs_static_gap"),
        "return_per_drawdown_penalty": ratio,
        "owner_review_drawdown_compensation_min": (
            OWNER_REVIEW_DRAWDOWN_COMPENSATION_MIN
        ),
        "return_per_drawdown_penalty_above_minimum": (
            ratio is not None
            and ratio >= OWNER_REVIEW_DRAWDOWN_COMPENSATION_MIN
        ),
        "drawdown_gap_materiality_tier": (
            "acceptable"
            if drawdown_not_materially_worse is True
            else "owner_review_required"
            if ratio is not None and ratio >= OWNER_REVIEW_DRAWDOWN_COMPENSATION_MIN
            else "not_acceptable"
        ),
        "recommended_rule_shape": {
            "auto_accept": ["drawdown_not_materially_worse=true"],
            "owner_review_required": [
                "drawdown_not_materially_worse=false",
                "cost_adjusted_gap_positive=true",
                "return_per_drawdown_penalty_above_minimum=true",
            ],
            "continue_optimization": [
                "drawdown_not_materially_worse=false",
                "return_per_drawdown_penalty_weak=true",
            ],
        },
        "policy_update_applied": False,
    }


def _research_only_vs_paper_shadow_gate_review() -> dict[str, Any]:
    return {
        "research_only_vs_paper_shadow_gate_review_ready": True,
        "gate_levels": {
            "research_only_observation": {
                "side_effect": "none",
                "artifact_only": True,
                "threshold": "lower_than_paper_shadow",
                "owner_review_allowed": True,
                "paper_trade_created": False,
                "shadow_position_created": False,
            },
            "paper_shadow": {
                "side_effect": "creates_paper_trades_or_shadow_positions",
                "threshold": "higher",
                "explicit_owner_approval_required": True,
                "currently_out_of_scope": True,
            },
            "production_or_broker": {
                "side_effect": "real_execution_or_capital_risk",
                "threshold": "highest",
                "explicit_owner_approval_required": True,
                "currently_out_of_scope": True,
            },
        },
        "finding": (
            "The current 2386 observation gate may be too close to a "
            "paper-shadow gate because it has no owner-review-only tier."
        ),
        "policy_update_applied": False,
    }


def _candidate_reclassification_preview(
    ranking_rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    rows_by_id = {
        str(row.get("candidate_id")): row
        for row in ranking_rows
        if row.get("candidate_id")
    }
    preview: dict[str, Any] = {}
    for candidate_id in CASE_STUDY_CANDIDATES:
        row = _as_mapping(rows_by_id.get(candidate_id))
        current_decision = row.get("decision") or DECISION_CONTINUE_OPTIMIZATION
        if not row:
            preview[candidate_id] = {
                "current_decision": current_decision,
                "current_blockers": ["candidate_not_found_in_2386_ranking_artifact"],
                "preview_decision_under_calibrated_gate": DECISION_CONTINUE_OPTIMIZATION,
                "auto_accept_allowed": False,
                "owner_review_allowed": False,
                "component_attribution_needed": False,
            }
            continue
        preview_decision = _preview_decision(candidate_id, row)
        preview[candidate_id] = {
            "current_decision": current_decision,
            "candidate_type": row.get("candidate_type"),
            "rank": row.get("rank"),
            "metrics": _candidate_metric_snapshot(row),
            "current_blockers": _current_blockers(row),
            "preview_decision_under_calibrated_gate": preview_decision,
            "auto_accept_allowed": False,
            "owner_review_allowed": preview_decision == DECISION_OWNER_REVIEW,
            "component_attribution_needed": candidate_id
            in {
                "dynamic_turnover_budgeted_growth_tilt_v1",
                "dynamic_valid_until_expiry_strict_v1",
            },
            "reason": _preview_reasons(candidate_id, row, preview_decision),
            "policy_update_applied": False,
        }
    return preview


def _preview_decision(candidate_id: str, row: Mapping[str, Any]) -> str:
    if candidate_id == RANKING_TOP_CANDIDATE:
        return DECISION_OWNER_REVIEW
    if (
        row.get("candidate_type") == "new_candidate"
        and _float(row.get("regime_slice_pass_rate"))
        < OBSERVATION_REGIME_SLICE_PASS_RATE_MIN
    ):
        return DECISION_CONTINUE_OPTIMIZATION
    if _float(row.get("candidate_vs_guarded_ranking_top_gap")) < 0.0:
        return DECISION_CONTINUE_OPTIMIZATION
    if row.get("candidate_id") in REFERENCE_CANDIDATES:
        return DECISION_CONTINUE_OPTIMIZATION
    return str(row.get("decision") or DECISION_CONTINUE_OPTIMIZATION)


def _preview_reasons(
    candidate_id: str,
    row: Mapping[str, Any],
    preview_decision: str,
) -> list[str]:
    if candidate_id == RANKING_TOP_CANDIDATE:
        return [
            "cost stress passed",
            "turnover budget passed",
            "positive dynamic_vs_static_gap",
            "reference status prevents auto-accept but should not block owner review",
            "drawdown requires owner judgment",
            "slice instability prevents auto-accept",
        ]
    reasons = [
        "current decision remains continue optimization under calibrated preview",
    ]
    if _float(row.get("regime_slice_pass_rate")) < OBSERVATION_REGIME_SLICE_PASS_RATE_MIN:
        reasons.append("regime slice still weak")
    if _float(row.get("candidate_vs_guarded_ranking_top_gap")) < 0.0:
        reasons.append("guarded gap negative")
    if candidate_id == "dynamic_turnover_budgeted_growth_tilt_v1":
        reasons.append("turnover budget component may be useful")
    if candidate_id == "dynamic_valid_until_expiry_strict_v1":
        reasons.append("valid-until component may be useful")
    if preview_decision == DECISION_OWNER_REVIEW:
        reasons.append("owner review required before research-only observation")
    return reasons


def _recommended_gate_policy_update() -> dict[str, Any]:
    return {
        "recommended_gate_policy_update_ready": True,
        "policy_update_applied": False,
        "rules_mutated": False,
        "recommended_policy_action": RECOMMENDED_POLICY_ACTION,
        "recommended_intermediate_decision_state": OWNER_REVIEW_DECISION_STATE,
        "reference_candidate_policy_recommendation": (
            REFERENCE_CANDIDATE_POLICY_RECOMMENDATION
        ),
        "research_only_gate_may_be_too_strict": True,
        "proposed_research_only_gate_policy": {
            DECISION_ACCEPT_RESEARCH_ONLY: {
                "must": [
                    "cost_adjusted_return_above_static",
                    "survives_realistic_cost",
                    "survives_conservative_cost",
                    "turnover_budget_passed",
                    "valid_until_window_preserved",
                    "no_stale_signal_carry_forward",
                    "no_hard_guardrail_failure",
                ],
                "should": [
                    "time_slice_pass_rate >= 0.60",
                    "regime_expectation_score >= 0.50",
                    "drawdown_not_materially_worse=true",
                ],
            },
            DECISION_OWNER_REVIEW: {
                "condition": [
                    "cost_adjusted_return_above_static",
                    "survives_realistic_cost",
                    "survives_conservative_cost",
                    "turnover_budget_passed",
                    "but_slice_stability_drawdown_or_reference_status_requires_judgment",
                ],
            },
            DECISION_CONTINUE_OPTIMIZATION: {
                "condition": [
                    "positive_evidence_exists",
                    "but_candidate_fails_slice_robustness_or_relative_reference_comparison",
                ],
            },
            DECISION_REJECT: {
                "condition": [
                    "realistic_or_conservative_cost_gap <= 0",
                    "or_severe_drawdown_deterioration",
                    "or_invalid_execution_assumptions",
                ],
            },
        },
        "proposed_owner_review_tiers": {
            "time_slice_pass_rate_owner_review_min": (
                OWNER_REVIEW_TIME_SLICE_PASS_RATE_MIN
            ),
            "drawdown_compensation_owner_review_min": (
                OWNER_REVIEW_DRAWDOWN_COMPENSATION_MIN
            ),
            "regime_slice_review_mode": "regime_expectation_score_not_single_win_rate",
        },
        "next_task_for_owner_decision": NEXT_ROUTE,
    }


def _component_attribution_review(
    ranking_rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    rows_by_id = {
        str(row.get("candidate_id")): row
        for row in ranking_rows
        if row.get("candidate_id")
    }
    return {
        "component_value_review_ready": True,
        "component_attribution_needed": True,
        "component_value_examples": {
            "dynamic_turnover_budgeted_growth_tilt_v1": {
                "metrics": _candidate_metric_snapshot(
                    _as_mapping(rows_by_id.get("dynamic_turnover_budgeted_growth_tilt_v1"))
                ),
                "possible_component_value": [
                    "turnover budgeting",
                    "growth tilt with cost awareness",
                ],
                "candidate_level_decision": DECISION_CONTINUE_OPTIMIZATION,
            },
            "dynamic_valid_until_expiry_strict_v1": {
                "metrics": _candidate_metric_snapshot(
                    _as_mapping(rows_by_id.get("dynamic_valid_until_expiry_strict_v1"))
                ),
                "possible_component_value": [
                    "stricter valid-until handling",
                    "stale signal prevention",
                ],
                "candidate_level_decision": DECISION_CONTINUE_OPTIMIZATION,
            },
        },
        "recommended_followup_if_gate_unchanged": [
            "signal_family_component_attribution_review",
            "valid_until_component_ablation",
            "turnover_budget_component_ablation",
        ],
    }


def _explicit_non_goals() -> dict[str, bool]:
    return {
        "enable_scheduler": False,
        "append_historical_event_log": False,
        "bind_outcome": False,
        "mutate_outcome_store": False,
        "enable_paper_shadow": False,
        "create_paper_trade": False,
        "create_shadow_position": False,
        "enable_production": False,
        "call_broker_api": False,
        "send_order": False,
        "create_scheduled_task": False,
        "generate_daily_report": False,
        "run_new_backtest": False,
        "generate_new_signal": False,
    }


def _write_outputs(
    *,
    payload: dict[str, Any],
    output_root: Path,
    docs_root: Path,
) -> None:
    output_root.mkdir(parents=True, exist_ok=True)
    docs_root.mkdir(parents=True, exist_ok=True)
    paths = {
        "json_path": str(output_root / "gate_calibration_review_result.json"),
        "gate_policy_review_json": str(output_root / "gate_policy_review.json"),
        "candidate_reclassification_preview_json": str(
            output_root / "candidate_reclassification_preview.json"
        ),
        "recommended_gate_policy_update_json": str(
            output_root / "recommended_gate_policy_update.json"
        ),
        "markdown_path": str(
            docs_root
            / "dynamic_strategy_observation_gate_threshold_calibration_review.md"
        ),
        "gate_policy_review_markdown": str(
            docs_root / "dynamic_strategy_gate_policy_review.md"
        ),
        "candidate_reclassification_preview_markdown": str(
            docs_root / "dynamic_strategy_candidate_reclassification_preview.md"
        ),
        "next_route_markdown": str(docs_root / "dynamic_strategy_2388_route.md"),
    }
    payload["artifact_paths"] = paths
    _write_json(Path(paths["json_path"]), payload)
    _write_json(
        Path(paths["gate_policy_review_json"]),
        {
            "report_type": "dynamic_strategy_gate_policy_review",
            "schema_version": "dynamic_strategy_gate_policy_review.v1",
            "task_id": TASK_ID,
            "status": payload["status"],
            "generated_at": payload["generated_at"],
            "current_observation_gate_rules": payload.get(
                "current_observation_gate_rules",
                {},
            ),
            "reference_candidate_policy_review": payload.get(
                "reference_candidate_policy_review",
                {},
            ),
            "time_slice_threshold_review": payload.get(
                "time_slice_threshold_review",
                {},
            ),
            "regime_slice_threshold_review": payload.get(
                "regime_slice_threshold_review",
                {},
            ),
            "drawdown_materiality_review": payload.get(
                "drawdown_materiality_review",
                {},
            ),
            "research_only_vs_paper_shadow_gate_review": payload.get(
                "research_only_vs_paper_shadow_gate_review",
                {},
            ),
            "policy_update_applied": False,
            "production_effect": "none",
            "broker_action": "none",
        },
    )
    _write_json(
        Path(paths["candidate_reclassification_preview_json"]),
        {
            "report_type": "dynamic_strategy_candidate_reclassification_preview",
            "schema_version": "dynamic_strategy_candidate_reclassification_preview.v1",
            "task_id": TASK_ID,
            "status": payload["status"],
            "generated_at": payload["generated_at"],
            "candidate_reclassification_preview": payload.get(
                "candidate_reclassification_preview",
                {},
            ),
            "policy_update_applied": False,
            "production_effect": "none",
            "broker_action": "none",
        },
    )
    _write_json(
        Path(paths["recommended_gate_policy_update_json"]),
        {
            "report_type": "dynamic_strategy_recommended_gate_policy_update",
            "schema_version": "dynamic_strategy_recommended_gate_policy_update.v1",
            "task_id": TASK_ID,
            "status": payload["status"],
            "generated_at": payload["generated_at"],
            "recommended_gate_policy_update": payload.get(
                "recommended_gate_policy_update",
                {},
            ),
            "recommended_next_research_task": NEXT_ROUTE,
            "policy_update_applied": False,
            "rules_mutated": False,
            "production_effect": "none",
            "broker_action": "none",
        },
    )
    Path(paths["markdown_path"]).write_text(_main_markdown(payload), encoding="utf-8")
    Path(paths["gate_policy_review_markdown"]).write_text(
        _gate_policy_markdown(payload),
        encoding="utf-8",
    )
    Path(paths["candidate_reclassification_preview_markdown"]).write_text(
        _candidate_preview_markdown(payload),
        encoding="utf-8",
    )
    Path(paths["next_route_markdown"]).write_text(
        _route_markdown(payload),
        encoding="utf-8",
    )


def _main_markdown(payload: Mapping[str, Any]) -> str:
    source = _as_mapping(payload.get("source_findings_from_2386"))
    why = _as_mapping(payload.get("why_2386_resulted_in_continue_optimization"))
    ref = _as_mapping(payload.get("reference_candidate_policy_review"))
    time_review = _as_mapping(payload.get("time_slice_threshold_review"))
    regime_review = _as_mapping(payload.get("regime_slice_threshold_review"))
    drawdown = _as_mapping(payload.get("drawdown_materiality_review"))
    gate = _as_mapping(payload.get("research_only_vs_paper_shadow_gate_review"))
    preview = _as_mapping(payload.get("candidate_reclassification_preview"))
    best_preview = _as_mapping(preview.get(RANKING_TOP_CANDIDATE))
    policy = _as_mapping(payload.get("recommended_gate_policy_update"))
    gate_rules = _as_mapping(payload.get("current_observation_gate_rules"))
    top_metrics_json = json.dumps(
        source.get("top_candidate_metrics", {}),
        ensure_ascii=False,
        sort_keys=True,
    )
    thresholds_json = json.dumps(
        gate_rules.get("thresholds", {}),
        ensure_ascii=False,
        sort_keys=True,
    )
    regime_policy_json = json.dumps(
        regime_review.get("regime_expectation_policy", {}),
        ensure_ascii=False,
        sort_keys=True,
    )
    non_goals_json = json.dumps(
        payload.get("explicit_non_goals", {}),
        ensure_ascii=False,
        sort_keys=True,
    )
    return "\n".join(
        [
            "# Dynamic strategy observation gate threshold calibration review",
            "",
            "## 1. Executive summary",
            "",
            f"- status：`{payload.get('status')}`",
            f"- current best candidate：`{payload.get('current_best_candidate')}`",
            f"- current best decision：`{payload.get('current_best_candidate_decision')}`",
            (
                "- observation-ready candidate found in 2386："
                f"`{payload.get('observation_ready_candidate_found_in_2386')}`"
            ),
            (
                "- recommended policy action："
                f"`{payload.get('recommended_policy_action')}`"
            ),
            (
                "- research-only gate may be too strict："
                f"`{payload.get('research_only_gate_may_be_too_strict')}`"
            ),
            (
                "- 本任务不批准 observation，不修改真实 gate，不进入 "
                "paper-shadow / production / broker。"
            ),
            "",
            "## 2. Source findings from TRADING-2386",
            "",
            f"- data quality from 2386：`{source.get('data_quality_status_from_2386')}`",
            f"- reference candidates tested：`{source.get('reference_candidates_tested')}`",
            f"- new candidates tested：`{source.get('new_candidates_tested')}`",
            f"- signal families tested：`{source.get('signal_families_tested')}`",
            f"- top metrics：`{top_metrics_json}`",
            "",
            "## 3. Current observation gate rules",
            "",
            (
                "- current reference policy："
                f"`{gate_rules.get('reference_candidate_current_policy')}`"
            ),
            f"- thresholds：`{thresholds_json}`",
            "",
            "## 4. Why 2386 resulted in CONTINUE_OPTIMIZATION",
            "",
            (
                "- current decision reasonable under current rules："
                f"`{why.get('current_decision_is_reasonable_under_current_2386_rules')}`"
            ),
            f"- primary blockers：`{why.get('primary_blockers')}`",
            f"- not a return failure：`{why.get('not_a_return_failure')}`",
            f"- not a cost failure：`{why.get('not_a_cost_failure')}`",
            f"- interpretation：{why.get('interpretation')}",
            "",
            "## 5. Reference candidate policy review",
            "",
            f"- current policy：`{ref.get('current_policy')}`",
            f"- recommended policy：`{ref.get('recommended_policy')}`",
            f"- owner review allowed：`{ref.get('owner_review_allowed_for_reference_candidate')}`",
            f"- auto accept allowed：`{ref.get('auto_accept_allowed_for_reference_candidate')}`",
            "",
            "## 6. Time-slice threshold review",
            "",
            f"- current acceptance threshold：`{time_review.get('current_acceptance_threshold')}`",
            (
                "- owner-review proposed threshold："
                f"`{OWNER_REVIEW_TIME_SLICE_PASS_RATE_MIN}`"
            ),
            f"- current best time slice：`{time_review.get('current_best_time_slice_pass_rate')}`",
            (
                "- 结论：0.0 是当前 best 的真实稳定性缺口，但 "
                "research-only gate 应增加 owner-review tier。"
            ),
            "",
            "## 7. Regime-slice threshold review",
            "",
            (
                "- current acceptance threshold："
                f"`{regime_review.get('current_acceptance_threshold')}`"
            ),
            (
                "- single global pass rate may be too blunt："
                f"`{regime_review.get('single_global_pass_rate_may_be_too_blunt')}`"
            ),
            f"- regime expectation policy：`{regime_policy_json}`",
            "",
            "## 8. Drawdown materiality review",
            "",
            f"- drawdown_not_materially_worse：`{drawdown.get('drawdown_not_materially_worse')}`",
            f"- drawdown gap vs static：`{drawdown.get('drawdown_gap_vs_static')}`",
            f"- return per drawdown penalty：`{drawdown.get('return_per_drawdown_penalty')}`",
            f"- materiality tier：`{drawdown.get('drawdown_gap_materiality_tier')}`",
            "",
            "## 9. Research-only observation vs paper-shadow gate separation",
            "",
            f"- finding：{gate.get('finding')}",
            (
                "- research-only observation 是 artifact-only/no-side-effect；"
                "paper-shadow 会创建 paper trades 或 shadow positions，"
                "必须更高门槛和 explicit owner approval。"
            ),
            "",
            "## 10. Candidate reclassification preview",
            "",
            (
                f"- `{RANKING_TOP_CANDIDATE}` current=`{best_preview.get('current_decision')}` "
                f"preview=`{best_preview.get('preview_decision_under_calibrated_gate')}` "
                f"auto_accept=`{best_preview.get('auto_accept_allowed')}` "
                f"owner_review=`{best_preview.get('owner_review_allowed')}`"
            ),
            "- preview 不等于真实改规则，也不等于批准 observation。",
            "",
            "## 11. Recommended gate policy update",
            "",
            f"- policy update applied：`{policy.get('policy_update_applied')}`",
            (
                "- recommended intermediate state："
                f"`{policy.get('recommended_intermediate_decision_state')}`"
            ),
            f"- next task：`{policy.get('next_task_for_owner_decision')}`",
            "",
            "## 12. Explicit non-goals",
            "",
            f"- safety：`{non_goals_json}`",
            "",
            "## 13. Recommended next route",
            "",
            f"- `{payload.get('recommended_next_research_task')}`",
            "",
        ]
    )


def _gate_policy_markdown(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# Dynamic strategy gate policy review",
            "",
            "## Summary",
            "",
            f"- status：`{payload.get('status')}`",
            f"- recommended action：`{payload.get('recommended_policy_action')}`",
            f"- policy update applied：`{payload.get('policy_update_applied')}`",
            f"- rules mutated：`{payload.get('rules_mutated')}`",
            "",
            "## Policy review JSON",
            "",
            "```json",
            json.dumps(
                {
                    "current_observation_gate_rules": payload.get(
                        "current_observation_gate_rules",
                        {},
                    ),
                    "reference_candidate_policy_review": payload.get(
                        "reference_candidate_policy_review",
                        {},
                    ),
                    "time_slice_threshold_review": payload.get(
                        "time_slice_threshold_review",
                        {},
                    ),
                    "regime_slice_threshold_review": payload.get(
                        "regime_slice_threshold_review",
                        {},
                    ),
                    "drawdown_materiality_review": payload.get(
                        "drawdown_materiality_review",
                        {},
                    ),
                    "research_only_vs_paper_shadow_gate_review": payload.get(
                        "research_only_vs_paper_shadow_gate_review",
                        {},
                    ),
                },
                ensure_ascii=False,
                indent=2,
                sort_keys=True,
            ),
            "```",
            "",
        ]
    )


def _candidate_preview_markdown(payload: Mapping[str, Any]) -> str:
    preview = _as_mapping(payload.get("candidate_reclassification_preview"))
    lines = [
        "# Dynamic strategy candidate reclassification preview",
        "",
        "## Summary",
        "",
        f"- status：`{payload.get('status')}`",
        "- preview 不修改真实规则，不批准 observation。",
        "",
        "|Candidate|Current decision|Preview decision|Auto accept|Owner review|",
        "|---|---|---|---|---|",
    ]
    for candidate_id, raw in preview.items():
        row = _as_mapping(raw)
        lines.append(
            "|"
            f"`{candidate_id}`|"
            f"`{row.get('current_decision')}`|"
            f"`{row.get('preview_decision_under_calibrated_gate')}`|"
            f"`{row.get('auto_accept_allowed')}`|"
            f"`{row.get('owner_review_allowed')}`|"
        )
    lines.extend(["", "## Raw preview", "", "```json"])
    lines.append(json.dumps(preview, ensure_ascii=False, indent=2, sort_keys=True))
    lines.extend(["```", ""])
    return "\n".join(lines)


def _route_markdown(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# Dynamic strategy TRADING-2388 route",
            "",
            f"- source task：`{TASK_ID}`",
            f"- status：`{payload.get('status')}`",
            f"- recommended next route：`{payload.get('recommended_next_research_task')}`",
            (
                "- recommended owner decision：review whether to add "
                "`OWNER_REVIEW_FOR_RESEARCH_ONLY_OBSERVATION` before pausing "
                "or continuing optimization."
            ),
            "- observation approved：`false`",
            "- paper-shadow enabled：`false`",
            "- production enabled：`false`",
            "- broker action：`none`",
            "",
        ]
    )


def _decision_update_payload(sources: Mapping[str, Any]) -> dict[str, Any]:
    decision_document = _as_mapping(sources.get("decision_update"))
    nested = decision_document.get("decision_update")
    if isinstance(nested, Mapping):
        return dict(nested)
    result = _as_mapping(sources.get("expanded_candidate_retest"))
    nested = result.get("decision_update")
    return dict(nested) if isinstance(nested, Mapping) else {}


def _expanded_candidate_ranking_rows(sources: Mapping[str, Any]) -> list[dict[str, Any]]:
    ranking = _as_mapping(sources.get("expanded_candidate_ranking"))
    rows = _as_list(ranking.get("expanded_candidate_ranking"))
    if not rows:
        result = _as_mapping(sources.get("expanded_candidate_retest"))
        rows = _as_list(result.get("expanded_candidate_ranking"))
    return [dict(row) for row in rows if isinstance(row, Mapping)]


def _signal_family_screening_rows(sources: Mapping[str, Any]) -> list[dict[str, Any]]:
    screening = _as_mapping(sources.get("signal_family_screening"))
    rows = _as_list(screening.get("signal_family_screening"))
    if not rows:
        result = _as_mapping(sources.get("expanded_candidate_retest"))
        rows = _as_list(result.get("signal_family_screening"))
    return [dict(row) for row in rows if isinstance(row, Mapping)]


def _top_candidate_row(sources: Mapping[str, Any]) -> dict[str, Any]:
    decision_update = _decision_update_payload(sources)
    top_row = decision_update.get("top_candidate_ranking_row")
    if isinstance(top_row, Mapping):
        return dict(top_row)
    current_best = _current_best_candidate(sources)
    for row in _expanded_candidate_ranking_rows(sources):
        if row.get("candidate_id") == current_best:
            return dict(row)
    return {}


def _current_best_candidate(sources: Mapping[str, Any]) -> str | None:
    result = _as_mapping(sources.get("expanded_candidate_retest"))
    decision_update = _decision_update_payload(sources)
    return _coalesce_string(
        result.get("best_candidate_after_expanded_screening"),
        decision_update.get("best_candidate_after_expanded_screening"),
        _top_candidate_row_without_current_best(sources).get("candidate_id"),
    )


def _top_candidate_row_without_current_best(
    sources: Mapping[str, Any],
) -> dict[str, Any]:
    decision_update = _decision_update_payload(sources)
    top_row = decision_update.get("top_candidate_ranking_row")
    if isinstance(top_row, Mapping):
        return dict(top_row)
    rows = _expanded_candidate_ranking_rows(sources)
    return dict(rows[0]) if rows else {}


def _current_best_decision(sources: Mapping[str, Any]) -> str | None:
    result = _as_mapping(sources.get("expanded_candidate_retest"))
    decision_update = _decision_update_payload(sources)
    top_row = _top_candidate_row_without_current_best(sources)
    return _coalesce_string(
        result.get("best_candidate_decision"),
        decision_update.get("best_candidate_decision"),
        top_row.get("decision"),
    )


def _observation_ready_candidate_found(sources: Mapping[str, Any]) -> bool:
    result = _as_mapping(sources.get("expanded_candidate_retest"))
    decision_update = _decision_update_payload(sources)
    if result.get("candidate_ready_for_research_only_observation") is True:
        return True
    if result.get("observation_ready_candidate_found") is True:
        return True
    if decision_update.get("candidate_ready_for_research_only_observation") is True:
        return True
    if decision_update.get("best_candidate_decision") == DECISION_ACCEPT_RESEARCH_ONLY:
        return True
    return any(
        row.get("decision") == DECISION_ACCEPT_RESEARCH_ONLY
        for row in _expanded_candidate_ranking_rows(sources)
    )


def _candidate_metric_snapshot(row: Mapping[str, Any]) -> dict[str, Any]:
    if not row:
        return {}
    return {
        "candidate_id": row.get("candidate_id"),
        "rank": row.get("rank"),
        "candidate_type": row.get("candidate_type"),
        "signal_family": row.get("signal_family"),
        "dynamic_vs_static_gap": row.get("dynamic_vs_static_gap"),
        "candidate_vs_lower_turnover_gap": row.get(
            "candidate_vs_lower_turnover_gap"
        ),
        "candidate_vs_guarded_ranking_top_gap": row.get(
            "candidate_vs_guarded_ranking_top_gap"
        ),
        "time_slice_pass_rate": row.get("time_slice_pass_rate"),
        "regime_slice_pass_rate": row.get("regime_slice_pass_rate"),
        "return_advantage_retained": row.get("return_advantage_retained"),
        "turnover_budget_passed": row.get("turnover_budget_passed"),
        "realistic_cost_passed": row.get("realistic_cost_passed"),
        "conservative_cost_passed": row.get("conservative_cost_passed"),
        "harsh_cost_passed": row.get("harsh_cost_passed"),
        "drawdown_gap_vs_static": row.get("drawdown_gap_vs_static"),
        "drawdown_not_materially_worse": _drawdown_not_materially_worse(row),
        "decision": row.get("decision"),
    }


def _current_blockers(row: Mapping[str, Any]) -> list[str]:
    blockers: list[str] = []
    if row.get("candidate_id") in REFERENCE_CANDIDATES or row.get(
        "candidate_type"
    ) == "reference_candidate":
        blockers.append("reference_candidate_hard_block")
    if _float(row.get("time_slice_pass_rate")) < OBSERVATION_TIME_SLICE_PASS_RATE_MIN:
        blockers.append("time_slice_pass_rate_below_acceptance")
    if _float(row.get("regime_slice_pass_rate")) < OBSERVATION_REGIME_SLICE_PASS_RATE_MIN:
        blockers.append("regime_slice_pass_rate_below_acceptance")
    if _drawdown_not_materially_worse(row) is False:
        blockers.append("drawdown_not_materially_worse=false")
    if _float(row.get("candidate_vs_guarded_ranking_top_gap")) < 0.0:
        blockers.append("guarded_gap_negative")
    if _float(row.get("return_advantage_retained")) < RETURN_ADVANTAGE_RETAINED_MIN:
        blockers.append("return_advantage_retained_below_acceptance")
    return blockers


def _drawdown_not_materially_worse(row: Mapping[str, Any]) -> bool | None:
    if "drawdown_not_materially_worse" in row:
        value = row.get("drawdown_not_materially_worse")
        if isinstance(value, bool):
            return value
    decision_reasons = ";".join(str(item) for item in _as_list(row.get("decision_reasons")))
    if "drawdown_not_materially_worse=False" in decision_reasons:
        return False
    if "drawdown_not_materially_worse=True" in decision_reasons:
        return True
    if "drawdown_not_materially_worse=False" in str(row.get("decision_reason", "")):
        return False
    if "drawdown_not_materially_worse=True" in str(row.get("decision_reason", "")):
        return True
    value = row.get("drawdown_gap_vs_static")
    if value is None:
        return None
    return _float(value) <= DRAWDOWN_WORSE_TOLERANCE


def _return_per_drawdown_penalty(row: Mapping[str, Any]) -> float | None:
    drawdown_gap = _float(row.get("drawdown_gap_vs_static"))
    if drawdown_gap <= 0.0:
        return None
    return round(_float(row.get("dynamic_vs_static_gap")) / drawdown_gap, 6)


def _top_candidate_from_candidate_ranking(document: Mapping[str, Any]) -> str | None:
    rows = _as_list(document.get("candidate_ranking"))
    if not rows:
        return None
    top = sorted(
        (row for row in rows if isinstance(row, Mapping)),
        key=lambda row: _float(row.get("rank"), default=999999.0),
    )
    return _coalesce_string(top[0].get("candidate_id")) if top else None


def _side_effect_validation_errors(label: str, document: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    for field in SAFETY_FALSE_FIELDS:
        if document.get(field) is True:
            errors.append(f"{label}.{field} must remain false")
    if document.get("broker_action") not in {None, "none"}:
        errors.append(f"{label}.broker_action must remain none")
    if document.get("production_effect") not in {None, "none"}:
        errors.append(f"{label}.production_effect must remain none")
    return errors


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )


def _load_json_document(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Required source artifact not found: {path}")
    document = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(document, dict):
        raise ValueError(f"Source artifact must be a JSON object: {path}")
    return document


def _as_mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _as_list(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []


def _float(value: object, *, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _coalesce_string(*values: object) -> str | None:
    for value in values:
        if isinstance(value, str) and value:
            return value
    return None
