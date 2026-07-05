from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import date
from pathlib import Path
from typing import Any

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.data_foundation import utc_now_iso
from ai_trading_system.dynamic_strategy_candidate_optimization_divergence_review import (
    DEFAULT_DYNAMIC_STRATEGY_CANDIDATE_OPTIMIZATION_DIVERGENCE_REVIEW_OUTPUT_ROOT,
)
from ai_trading_system.dynamic_strategy_candidate_optimization_divergence_review import (
    READY_STATUS as SOURCE_2375_READY_STATUS,
)
from ai_trading_system.dynamic_strategy_cost_turnover_cooldown_sensitivity import (
    DEFAULT_DYNAMIC_STRATEGY_COST_TURNOVER_COOLDOWN_SENSITIVITY_OUTPUT_ROOT,
    PRIMARY_EXECUTION_CADENCE,
)
from ai_trading_system.dynamic_strategy_cost_turnover_cooldown_sensitivity import (
    READY_STATUS as SOURCE_2366_READY_STATUS,
)
from ai_trading_system.dynamic_strategy_event_driven_retest import (
    DEFAULT_DYNAMIC_STRATEGY_EVENT_DRIVEN_RETEST_OUTPUT_ROOT,
)
from ai_trading_system.dynamic_strategy_event_driven_retest import (
    READY_STATUS as SOURCE_2365_READY_STATUS,
)
from ai_trading_system.dynamic_strategy_execution_cadence_bias_audit import (
    DEFAULT_DYNAMIC_STRATEGY_EXECUTION_CADENCE_BIAS_AUDIT_OUTPUT_ROOT,
)
from ai_trading_system.dynamic_strategy_execution_cadence_bias_audit import (
    READY_STATUS as SOURCE_2364_READY_STATUS,
)
from ai_trading_system.dynamic_strategy_expanded_candidate_pool_retest import (
    DEFAULT_DYNAMIC_STRATEGY_EXPANDED_CANDIDATE_POOL_RETEST_OUTPUT_ROOT,
    DRAWDOWN_WORSE_TOLERANCE,
    OBSERVATION_REGIME_SLICE_PASS_RATE_MIN,
    OBSERVATION_TIME_SLICE_PASS_RATE_MIN,
    REGIME_SLICE_PASS_RATE_ACCEPTABLE_MIN,
    RETURN_ADVANTAGE_RETAINED_MIN,
    TIME_SLICE_PASS_RATE_ACCEPTABLE_MIN,
    TURNOVER_BUDGET_MAX_MONTHLY,
)
from ai_trading_system.dynamic_strategy_expanded_candidate_pool_retest import (
    READY_STATUS as SOURCE_2386_READY_STATUS,
)
from ai_trading_system.dynamic_strategy_observation_gate_threshold_calibration_review import (
    DEFAULT_DYNAMIC_STRATEGY_OBSERVATION_GATE_THRESHOLD_CALIBRATION_REVIEW_OUTPUT_ROOT,
    REFERENCE_CANDIDATE_POLICY_RECOMMENDATION,
)
from ai_trading_system.dynamic_strategy_observation_gate_threshold_calibration_review import (
    READY_STATUS as SOURCE_2387_READY_STATUS,
)
from ai_trading_system.dynamic_strategy_optimized_candidate_targeted_retest import (
    DEFAULT_DYNAMIC_STRATEGY_OPTIMIZED_CANDIDATE_TARGETED_RETEST_OUTPUT_ROOT,
)
from ai_trading_system.dynamic_strategy_optimized_candidate_targeted_retest import (
    READY_STATUS as SOURCE_2376_READY_STATUS,
)
from ai_trading_system.dynamic_strategy_ranking_top_guarded_turnover_retest_plan import (
    BEST_LOWER_TURNOVER_VARIANT,
)
from ai_trading_system.dynamic_strategy_ranking_top_guarded_variant_retest import (
    DEFAULT_DYNAMIC_STRATEGY_RANKING_TOP_GUARDED_VARIANT_RETEST_OUTPUT_ROOT,
    RANKING_TOP_CANDIDATE,
)
from ai_trading_system.dynamic_strategy_ranking_top_guarded_variant_retest import (
    READY_STATUS as SOURCE_2383_READY_STATUS,
)
from ai_trading_system.dynamic_strategy_report_common import (
    write_json_artifact,
    write_markdown_artifact,
)
from ai_trading_system.dynamic_strategy_slice_robustness_optimized_variant_retest import (
    BASE_CANDIDATE_ID,
    DEFAULT_DYNAMIC_STRATEGY_SLICE_ROBUSTNESS_OPTIMIZED_VARIANT_RETEST_OUTPUT_ROOT,
)
from ai_trading_system.dynamic_strategy_slice_robustness_optimized_variant_retest import (
    READY_STATUS as SOURCE_2379_READY_STATUS,
)
from ai_trading_system.execution_semantics import AI_REGIME_SUMMARY, _file_sha256

TASK_ID = "TRADING-2388"
TASK_REGISTER_ID = (
    "TRADING-2388_DYNAMIC_STRATEGY_RESEARCH_FILTER_THRESHOLD_METHODOLOGY_REVIEW"
)
REPORT_TYPE = "dynamic_strategy_research_filter_threshold_methodology_review"
SCHEMA_VERSION = "dynamic_strategy_research_filter_threshold_methodology_review.v1"
READY_STATUS = (
    "DYNAMIC_STRATEGY_RESEARCH_FILTER_THRESHOLD_METHODOLOGY_REVIEW_READY"
)
BLOCKED_SOURCE_STATUS = (
    "DYNAMIC_STRATEGY_RESEARCH_FILTER_THRESHOLD_METHODOLOGY_REVIEW_"
    "BLOCKED_SOURCE_ARTIFACT"
)
NEXT_ROUTE = (
    "TRADING-2389_Dynamic_Strategy_Calibrated_Gate_Owner_Review_And_Next_Decision"
)
DATA_QUALITY_GATE_REASON = (
    "NOT_APPLICABLE_PRIOR_ARTIFACT_THRESHOLD_METHODOLOGY_ONLY_NO_FRESH_MARKET_DATA"
)
SOURCE_TASKS: tuple[str, ...] = (
    "TRADING-2364",
    "TRADING-2365",
    "TRADING-2366",
    "TRADING-2375",
    "TRADING-2376",
    "TRADING-2379",
    "TRADING-2383",
    "TRADING-2386",
    "TRADING-2387",
)
THRESHOLDS_REQUIRING_STATISTICAL_CALIBRATION: tuple[str, ...] = (
    "time_slice_pass_rate",
    "regime_expectation_score",
    "drawdown_materiality",
    "return_per_drawdown_penalty",
    "owner_review_required_vs_continue_optimization_boundary",
)
FUTURE_STATISTICAL_CALIBRATION_NEEDED: tuple[str, ...] = (
    "time_slice_pass_rate_threshold",
    "regime_expectation_score_threshold",
    "drawdown_materiality_threshold",
    "return_per_drawdown_penalty_threshold",
    "turnover_budget_materiality",
    "reference_candidate_reclassification_policy",
    "owner_review_required_vs_continue_optimization_boundary",
)
REQUIRED_CANDIDATES: tuple[str, ...] = (
    RANKING_TOP_CANDIDATE,
    BASE_CANDIDATE_ID,
    BEST_LOWER_TURNOVER_VARIANT,
    "equal_risk_growth_tilt_guarded_turnover_v1",
    "dynamic_turnover_budgeted_growth_tilt_v1",
    "dynamic_valid_until_expiry_strict_v1",
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

DEFAULT_DYNAMIC_STRATEGY_RESEARCH_FILTER_THRESHOLD_METHODOLOGY_REVIEW_OUTPUT_ROOT = (
    PROJECT_ROOT / "outputs" / "research_strategies" / REPORT_TYPE
)
DEFAULT_DYNAMIC_STRATEGY_RESEARCH_FILTER_THRESHOLD_METHODOLOGY_REVIEW_DOCS_ROOT = (
    PROJECT_ROOT / "docs" / "research"
)
DEFAULT_SOURCE_2364_CADENCE_AUDIT_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_EXECUTION_CADENCE_BIAS_AUDIT_OUTPUT_ROOT
    / "execution_cadence_bias_audit.json"
)
DEFAULT_SOURCE_2365_EVENT_RETEST_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_EVENT_DRIVEN_RETEST_OUTPUT_ROOT
    / "event_driven_retest_result.json"
)
DEFAULT_SOURCE_2365_CANDIDATE_RANKING_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_EVENT_DRIVEN_RETEST_OUTPUT_ROOT / "candidate_ranking.json"
)
DEFAULT_SOURCE_2366_SENSITIVITY_RESULT_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_COST_TURNOVER_COOLDOWN_SENSITIVITY_OUTPUT_ROOT
    / "sensitivity_result.json"
)
DEFAULT_SOURCE_2366_DECISION_UPDATE_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_COST_TURNOVER_COOLDOWN_SENSITIVITY_OUTPUT_ROOT
    / "decision_update.json"
)
DEFAULT_SOURCE_2375_DIVERGENCE_REVIEW_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_CANDIDATE_OPTIMIZATION_DIVERGENCE_REVIEW_OUTPUT_ROOT
    / "divergence_review_result.json"
)
DEFAULT_SOURCE_2375_DECISION_UPDATE_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_CANDIDATE_OPTIMIZATION_DIVERGENCE_REVIEW_OUTPUT_ROOT
    / "candidate_decision_update.json"
)
DEFAULT_SOURCE_2376_TARGETED_RETEST_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_OPTIMIZED_CANDIDATE_TARGETED_RETEST_OUTPUT_ROOT
    / "targeted_retest_result.json"
)
DEFAULT_SOURCE_2376_DECISION_UPDATE_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_OPTIMIZED_CANDIDATE_TARGETED_RETEST_OUTPUT_ROOT
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
DEFAULT_SOURCE_2379_DECISION_UPDATE_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_SLICE_ROBUSTNESS_OPTIMIZED_VARIANT_RETEST_OUTPUT_ROOT
    / "decision_update.json"
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
DEFAULT_SOURCE_2386_EXPANDED_CANDIDATE_RETEST_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_EXPANDED_CANDIDATE_POOL_RETEST_OUTPUT_ROOT
    / "expanded_candidate_retest_result.json"
)
DEFAULT_SOURCE_2386_EXPANDED_CANDIDATE_RANKING_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_EXPANDED_CANDIDATE_POOL_RETEST_OUTPUT_ROOT
    / "expanded_candidate_ranking.json"
)
DEFAULT_SOURCE_2386_DECISION_UPDATE_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_EXPANDED_CANDIDATE_POOL_RETEST_OUTPUT_ROOT
    / "decision_update.json"
)
DEFAULT_SOURCE_2387_GATE_CALIBRATION_REVIEW_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_OBSERVATION_GATE_THRESHOLD_CALIBRATION_REVIEW_OUTPUT_ROOT
    / "gate_calibration_review_result.json"
)
DEFAULT_SOURCE_2387_GATE_POLICY_REVIEW_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_OBSERVATION_GATE_THRESHOLD_CALIBRATION_REVIEW_OUTPUT_ROOT
    / "gate_policy_review.json"
)
DEFAULT_SOURCE_2387_CANDIDATE_RECLASSIFICATION_PREVIEW_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_OBSERVATION_GATE_THRESHOLD_CALIBRATION_REVIEW_OUTPUT_ROOT
    / "candidate_reclassification_preview.json"
)
DEFAULT_SOURCE_2387_RECOMMENDED_POLICY_UPDATE_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_OBSERVATION_GATE_THRESHOLD_CALIBRATION_REVIEW_OUTPUT_ROOT
    / "recommended_gate_policy_update.json"
)


def run_dynamic_strategy_research_filter_threshold_methodology_review(
    *,
    source_cadence_audit_path: Path = DEFAULT_SOURCE_2364_CADENCE_AUDIT_PATH,
    source_event_retest_path: Path = DEFAULT_SOURCE_2365_EVENT_RETEST_PATH,
    source_candidate_ranking_path: Path = DEFAULT_SOURCE_2365_CANDIDATE_RANKING_PATH,
    source_sensitivity_result_path: Path = DEFAULT_SOURCE_2366_SENSITIVITY_RESULT_PATH,
    source_sensitivity_decision_update_path: Path = (
        DEFAULT_SOURCE_2366_DECISION_UPDATE_PATH
    ),
    source_divergence_review_path: Path = DEFAULT_SOURCE_2375_DIVERGENCE_REVIEW_PATH,
    source_divergence_decision_update_path: Path = (
        DEFAULT_SOURCE_2375_DECISION_UPDATE_PATH
    ),
    source_targeted_retest_path: Path = DEFAULT_SOURCE_2376_TARGETED_RETEST_PATH,
    source_targeted_decision_update_path: Path = (
        DEFAULT_SOURCE_2376_DECISION_UPDATE_PATH
    ),
    source_variant_retest_path: Path = DEFAULT_SOURCE_2379_VARIANT_RETEST_PATH,
    source_optimized_variant_ranking_path: Path = (
        DEFAULT_SOURCE_2379_OPTIMIZED_VARIANT_RANKING_PATH
    ),
    source_variant_decision_update_path: Path = (
        DEFAULT_SOURCE_2379_DECISION_UPDATE_PATH
    ),
    source_guarded_variant_retest_path: Path = (
        DEFAULT_SOURCE_2383_GUARDED_VARIANT_RETEST_PATH
    ),
    source_guarded_variant_ranking_path: Path = (
        DEFAULT_SOURCE_2383_GUARDED_VARIANT_RANKING_PATH
    ),
    source_guarded_decision_update_path: Path = DEFAULT_SOURCE_2383_DECISION_UPDATE_PATH,
    source_expanded_candidate_retest_path: Path = (
        DEFAULT_SOURCE_2386_EXPANDED_CANDIDATE_RETEST_PATH
    ),
    source_expanded_candidate_ranking_path: Path = (
        DEFAULT_SOURCE_2386_EXPANDED_CANDIDATE_RANKING_PATH
    ),
    source_expanded_decision_update_path: Path = (
        DEFAULT_SOURCE_2386_DECISION_UPDATE_PATH
    ),
    source_gate_calibration_review_path: Path = (
        DEFAULT_SOURCE_2387_GATE_CALIBRATION_REVIEW_PATH
    ),
    source_gate_policy_review_path: Path = DEFAULT_SOURCE_2387_GATE_POLICY_REVIEW_PATH,
    source_candidate_reclassification_preview_path: Path = (
        DEFAULT_SOURCE_2387_CANDIDATE_RECLASSIFICATION_PREVIEW_PATH
    ),
    source_recommended_policy_update_path: Path = (
        DEFAULT_SOURCE_2387_RECOMMENDED_POLICY_UPDATE_PATH
    ),
    output_root: Path = (
        DEFAULT_DYNAMIC_STRATEGY_RESEARCH_FILTER_THRESHOLD_METHODOLOGY_REVIEW_OUTPUT_ROOT
    ),
    docs_root: Path = (
        DEFAULT_DYNAMIC_STRATEGY_RESEARCH_FILTER_THRESHOLD_METHODOLOGY_REVIEW_DOCS_ROOT
    ),
    as_of_date: date | None = None,
) -> dict[str, Any]:
    sources = _load_sources(
        source_cadence_audit_path=source_cadence_audit_path,
        source_event_retest_path=source_event_retest_path,
        source_candidate_ranking_path=source_candidate_ranking_path,
        source_sensitivity_result_path=source_sensitivity_result_path,
        source_sensitivity_decision_update_path=source_sensitivity_decision_update_path,
        source_divergence_review_path=source_divergence_review_path,
        source_divergence_decision_update_path=source_divergence_decision_update_path,
        source_targeted_retest_path=source_targeted_retest_path,
        source_targeted_decision_update_path=source_targeted_decision_update_path,
        source_variant_retest_path=source_variant_retest_path,
        source_optimized_variant_ranking_path=source_optimized_variant_ranking_path,
        source_variant_decision_update_path=source_variant_decision_update_path,
        source_guarded_variant_retest_path=source_guarded_variant_retest_path,
        source_guarded_variant_ranking_path=source_guarded_variant_ranking_path,
        source_guarded_decision_update_path=source_guarded_decision_update_path,
        source_expanded_candidate_retest_path=source_expanded_candidate_retest_path,
        source_expanded_candidate_ranking_path=source_expanded_candidate_ranking_path,
        source_expanded_decision_update_path=source_expanded_decision_update_path,
        source_gate_calibration_review_path=source_gate_calibration_review_path,
        source_gate_policy_review_path=source_gate_policy_review_path,
        source_candidate_reclassification_preview_path=(
            source_candidate_reclassification_preview_path
        ),
        source_recommended_policy_update_path=source_recommended_policy_update_path,
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


def _load_sources(**paths: Path) -> dict[str, Any]:
    source_files = {
        key.removeprefix("source_").removesuffix("_path"): path
        for key, path in paths.items()
    }
    documents = {key: _load_json_document(path) for key, path in source_files.items()}
    sources: dict[str, Any] = {
        **documents,
        "source_files": {key: str(path) for key, path in source_files.items()},
        "source_hashes": {
            key: _file_sha256(path) if path.exists() else None
            for key, path in source_files.items()
        },
        "source_status": {
            key: _as_mapping(document).get("status")
            for key, document in documents.items()
        },
    }
    sources["source_validation_errors"] = _source_validation_errors(sources)
    sources["source_ready_for_threshold_methodology_review"] = not sources[
        "source_validation_errors"
    ]
    return sources


def _source_validation_errors(sources: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    expected_status = {
        "cadence_audit": SOURCE_2364_READY_STATUS,
        "event_retest": SOURCE_2365_READY_STATUS,
        "candidate_ranking": SOURCE_2365_READY_STATUS,
        "sensitivity_result": SOURCE_2366_READY_STATUS,
        "sensitivity_decision_update": SOURCE_2366_READY_STATUS,
        "divergence_review": SOURCE_2375_READY_STATUS,
        "divergence_decision_update": SOURCE_2375_READY_STATUS,
        "targeted_retest": SOURCE_2376_READY_STATUS,
        "targeted_decision_update": SOURCE_2376_READY_STATUS,
        "variant_retest": SOURCE_2379_READY_STATUS,
        "optimized_variant_ranking": SOURCE_2379_READY_STATUS,
        "variant_decision_update": SOURCE_2379_READY_STATUS,
        "guarded_variant_retest": SOURCE_2383_READY_STATUS,
        "guarded_variant_ranking": SOURCE_2383_READY_STATUS,
        "guarded_decision_update": SOURCE_2383_READY_STATUS,
        "expanded_candidate_retest": SOURCE_2386_READY_STATUS,
        "expanded_candidate_ranking": SOURCE_2386_READY_STATUS,
        "expanded_decision_update": SOURCE_2386_READY_STATUS,
        "gate_calibration_review": SOURCE_2387_READY_STATUS,
        "gate_policy_review": SOURCE_2387_READY_STATUS,
        "candidate_reclassification_preview": SOURCE_2387_READY_STATUS,
        "recommended_policy_update": SOURCE_2387_READY_STATUS,
    }
    source_status = _as_mapping(sources.get("source_status"))
    for key, expected in expected_status.items():
        actual = source_status.get(key)
        if actual != expected:
            errors.append(f"{key}.status expected {expected}, got {actual!r}")

    cadence_audit = _as_mapping(sources.get("cadence_audit"))
    if cadence_audit.get("data_quality_gate_executed") is not True:
        errors.append("2364 cadence audit must carry data_quality_gate_executed=true")
    event_retest = _as_mapping(sources.get("event_retest"))
    if event_retest.get("recommended_next_research_task") not in {
        "TRADING-2366_Dynamic_Strategy_Cost_Turnover_And_Cooldown_Sensitivity_Analysis",
        None,
    }:
        errors.append("2365 event retest route must remain TRADING-2366")
    candidate_ranking = _as_mapping(sources.get("candidate_ranking"))
    if _top_candidate_from_candidate_ranking(candidate_ranking) != RANKING_TOP_CANDIDATE:
        errors.append("2365 top candidate must remain ranking-top reference")

    if not _as_list(_as_mapping(sources.get("sensitivity_result")).get("sensitivity_matrix")):
        errors.append("2366 sensitivity matrix must be non-empty")
    if not isinstance(
        _as_mapping(sources.get("sensitivity_decision_update")).get("decision_update"),
        Mapping,
    ):
        errors.append("2366 decision_update must be present")

    divergence_review = _as_mapping(sources.get("divergence_review"))
    if divergence_review.get("recommended_next_research_task") not in {
        "TRADING-2376_Dynamic_Strategy_Optimized_Candidate_Targeted_Retest",
        None,
    }:
        errors.append("2375 divergence review route must remain TRADING-2376")
    targeted_retest = _as_mapping(sources.get("targeted_retest"))
    if targeted_retest.get("primary_candidate") not in {BASE_CANDIDATE_ID, None}:
        errors.append("2376 primary candidate must remain lower-turnover reference")
    if targeted_retest.get("candidate_decision_after_targeted_retest") not in {
        "CONTINUE_OPTIMIZATION",
        None,
    }:
        errors.append("2376 candidate decision must remain CONTINUE_OPTIMIZATION")

    variant_retest = _as_mapping(sources.get("variant_retest"))
    if variant_retest.get("best_variant_after_retest") not in {
        BEST_LOWER_TURNOVER_VARIANT,
        None,
    }:
        errors.append("2379 best variant must remain cooldown-balanced variant")
    if variant_retest.get("candidate_ready_for_research_only_observation") is True:
        errors.append("2379 must not already approve research-only observation")

    guarded_retest = _as_mapping(sources.get("guarded_variant_retest"))
    if guarded_retest.get("best_guarded_variant") not in {
        "equal_risk_growth_tilt_guarded_turnover_v1",
        None,
    }:
        errors.append("2383 guarded best variant mismatch")
    if guarded_retest.get("candidate_ready_for_research_only_observation") is True:
        errors.append("2383 must not already approve research-only observation")

    expanded_retest = _as_mapping(sources.get("expanded_candidate_retest"))
    if expanded_retest.get("best_candidate_after_expanded_screening") != (
        RANKING_TOP_CANDIDATE
    ):
        errors.append("2386 current best candidate mismatch")
    if expanded_retest.get("best_candidate_decision") != "CONTINUE_OPTIMIZATION":
        errors.append("2386 current best decision must remain CONTINUE_OPTIMIZATION")
    if expanded_retest.get("candidate_ready_for_research_only_observation") is True:
        errors.append("2386 must not already approve research-only observation")

    gate_review = _as_mapping(sources.get("gate_calibration_review"))
    if gate_review.get("recommended_next_research_task") not in {
        "TRADING-2388_Dynamic_Strategy_Calibrated_Gate_Owner_Review_And_Next_Decision",
        None,
        "TRADING-2388_Dynamic_Strategy_Research_Filter_Threshold_Methodology_Review",
    }:
        errors.append("2387 route must point to a TRADING-2388 review")
    if gate_review.get("observation_approved") is True:
        errors.append("2387 must not approve observation")
    if gate_review.get("policy_update_applied") is True:
        errors.append("2387 must not apply a policy update")
    if (
        gate_review.get("reference_candidate_policy_recommendation")
        != REFERENCE_CANDIDATE_POLICY_RECOMMENDATION
    ):
        errors.append("2387 reference-candidate recommendation mismatch")

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
        "source_ready_for_threshold_methodology_review": bool(
            sources.get("source_ready_for_threshold_methodology_review")
        ),
        "data_quality_gate_executed": False,
        "data_quality_gate_reason": DATA_QUALITY_GATE_REASON,
        "data_quality_status": DATA_QUALITY_GATE_REASON,
        "fresh_market_data_read": False,
        "backtest_run": False,
        "new_signal_generated": False,
        "scoring_run": False,
        "observation_approved": False,
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
    inventory = _threshold_inventory()
    gate_taxonomy = _gate_taxonomy()
    matrix = _candidate_threshold_outcome_matrix(sources)
    proposal = _recommended_gate_policy_proposal()
    return {
        "threshold_methodology_review_ready": True,
        "threshold_inventory_ready": True,
        "gate_taxonomy_ready": True,
        "candidate_threshold_outcome_matrix_ready": True,
        "recommended_gate_policy_proposal_ready": True,
        "research_only_vs_paper_shadow_gate_separated": True,
        "current_gate_may_be_too_strict_for_research_only_observation": True,
        "reference_candidate_policy_recommendation": (
            REFERENCE_CANDIDATE_POLICY_RECOMMENDATION
        ),
        "thresholds_requiring_statistical_calibration": list(
            THRESHOLDS_REQUIRING_STATISTICAL_CALIBRATION
        ),
        "future_statistical_calibration_needed": list(
            FUTURE_STATISTICAL_CALIBRATION_NEEDED
        ),
        "future_statistical_calibration_optional_route": (
            "TRADING-2390_Dynamic_Strategy_Threshold_Meta_Dataset_And_"
            "Historical_Gate_Backtest"
        ),
        "threshold_inventory": inventory,
        "threshold_source_classification": _threshold_source_classification(),
        "gate_taxonomy": gate_taxonomy,
        "candidate_threshold_outcome_matrix": matrix,
        "recommended_gate_policy_proposal": proposal,
        "required_answers": _required_answers(),
        "explicit_non_goals": _explicit_non_goals(),
        "recommended_next_route": {
            "recommended_next_research_task": NEXT_ROUTE,
            "owner_decision_required": True,
            "use_2387_and_2388_together": True,
        },
    }


def _blocked_sections(sources: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "blocked_reason": "source_artifact_validation_failed",
        "threshold_methodology_review_ready": False,
        "threshold_inventory_ready": False,
        "gate_taxonomy_ready": False,
        "candidate_threshold_outcome_matrix_ready": False,
        "recommended_gate_policy_proposal_ready": False,
        "research_only_vs_paper_shadow_gate_separated": False,
        "current_gate_may_be_too_strict_for_research_only_observation": None,
        "reference_candidate_policy_recommendation": None,
        "thresholds_requiring_statistical_calibration": [],
        "future_statistical_calibration_needed": [],
        "threshold_inventory": {},
        "threshold_source_classification": _threshold_source_classification(),
        "gate_taxonomy": _gate_taxonomy(),
        "candidate_threshold_outcome_matrix": [],
        "recommended_gate_policy_proposal": {},
        "required_answers": {},
        "explicit_non_goals": _explicit_non_goals(),
        "recommended_next_route": {
            "recommended_next_research_task": NEXT_ROUTE,
            "blocked_until": list(sources.get("source_validation_errors", [])),
        },
    }


def _threshold_inventory() -> dict[str, Any]:
    return {
        "execution_cadence_thresholds": {
            "valid_until_window_required": _threshold(
                current_status="required",
                source="TRADING-2364",
                evidence_level="project_empirical",
                threshold_type="research_quality_gate",
                needs_calibration=False,
            ),
            "monthly_rebalance_not_primary": _threshold(
                current_status="required",
                source="TRADING-2364",
                evidence_level="project_empirical",
                threshold_type="research_quality_gate",
                needs_calibration=False,
            ),
            "no_stale_signal_carry_forward": _threshold(
                current_status="required",
                source="TRADING-2357/TRADING-2358/TRADING-2364",
                evidence_level="engineering_and_research_prior",
                threshold_type="hard_research_guardrail",
                needs_calibration=False,
            ),
        },
        "cost_turnover_thresholds": {
            "realistic_cost_passed": _threshold(
                current_status="required_for_continue",
                source="TRADING-2366",
                evidence_level="project_empirical",
                threshold_type="research_quality_gate",
                needs_calibration=False,
            ),
            "conservative_cost_passed": _threshold(
                current_status="required_for_observation_consideration",
                source="TRADING-2366",
                evidence_level="project_empirical",
                threshold_type="research_quality_gate",
                needs_calibration=False,
            ),
            "harsh_cost_passed": _threshold(
                current_status="informative_not_required",
                source="TRADING-2366",
                evidence_level="stress_test_heuristic",
                threshold_type="owner_review_signal",
                needs_calibration=True,
            ),
            "turnover_budget_passed": _threshold(
                current_status="required_for_observation_consideration",
                source="TRADING-2366/TRADING-2386",
                evidence_level="project_empirical",
                threshold_type="research_quality_gate",
                needs_calibration=True,
                current_threshold=TURNOVER_BUDGET_MAX_MONTHLY,
            ),
        },
        "slice_stability_thresholds": {
            "time_slice_pass_rate": _threshold(
                current_status="required_for_auto_accept",
                source="TRADING-2386/TRADING-2387",
                evidence_level="conservative_heuristic",
                threshold_type="research_quality_gate",
                needs_calibration=True,
                current_threshold=OBSERVATION_TIME_SLICE_PASS_RATE_MIN,
            ),
            "regime_slice_pass_rate": _threshold(
                current_status="required_for_auto_accept",
                source="TRADING-2386/TRADING-2387",
                evidence_level="conservative_heuristic",
                threshold_type="research_quality_gate",
                needs_calibration=True,
                current_threshold=OBSERVATION_REGIME_SLICE_PASS_RATE_MIN,
            ),
            "regime_expectation_score": _threshold(
                current_status="proposed",
                source="TRADING-2387",
                evidence_level="methodology_proposal",
                threshold_type="calibrated_research_gate",
                needs_calibration=True,
            ),
        },
        "drawdown_thresholds": {
            "drawdown_not_materially_worse": _threshold(
                current_status="required_for_auto_accept",
                source="TRADING-2386/TRADING-2387",
                evidence_level="conservative_heuristic",
                threshold_type="owner_review_gate",
                needs_calibration=True,
                current_threshold=DRAWDOWN_WORSE_TOLERANCE,
            ),
            "drawdown_gap_vs_static": _threshold(
                current_status="measured",
                source="TRADING-2386",
                evidence_level="metric",
                threshold_type="research_quality_signal",
                needs_calibration=True,
            ),
            "return_per_drawdown_penalty": _threshold(
                current_status="proposed",
                source="TRADING-2387",
                evidence_level="methodology_proposal",
                threshold_type="owner_review_gate",
                needs_calibration=True,
            ),
        },
        "reference_candidate_thresholds": {
            "reference_candidate_cannot_accept": _threshold(
                current_status="hard_block",
                source="TRADING-2386",
                evidence_level="conservative_process_rule",
                threshold_type="process_gate",
                needs_calibration=True,
            ),
            "proposed_reference_policy": {
                "recommended_policy": REFERENCE_CANDIDATE_POLICY_RECOMMENDATION,
                "threshold_type": "owner_review_gate",
                "source": "TRADING-2387",
                "needs_calibration": True,
            },
        },
        "relative_candidate_thresholds": {
            "must_beat_static": _threshold(
                current_status="required",
                source="TRADING-2365/TRADING-2386",
                evidence_level="research_common_sense",
                threshold_type="research_quality_gate",
                needs_calibration=False,
            ),
            "must_compare_against_lower_turnover_reference": _threshold(
                current_status="required",
                source="TRADING-2376/TRADING-2379/TRADING-2386",
                evidence_level="project_empirical",
                threshold_type="robustness_reference_gate",
                needs_calibration=False,
            ),
            "must_compare_against_ranking_top_reference": _threshold(
                current_status="required",
                source="TRADING-2365/TRADING-2375/TRADING-2386",
                evidence_level="project_empirical",
                threshold_type="return_reference_gate",
                needs_calibration=False,
            ),
            "must_beat_guarded_reference": _threshold(
                current_status="currently_used_in_some_gates",
                source="TRADING-2383/TRADING-2386",
                evidence_level="conservative_heuristic",
                threshold_type="owner_review_gate",
                needs_calibration=True,
            ),
        },
        "current_2386_threshold_constants": {
            "time_slice_pass_rate_acceptable_min": TIME_SLICE_PASS_RATE_ACCEPTABLE_MIN,
            "regime_slice_pass_rate_acceptable_min": REGIME_SLICE_PASS_RATE_ACCEPTABLE_MIN,
            "return_advantage_retained_min": RETURN_ADVANTAGE_RETAINED_MIN,
            "primary_execution_cadence": PRIMARY_EXECUTION_CADENCE,
        },
    }


def _threshold(
    *,
    current_status: str,
    source: str,
    evidence_level: str,
    threshold_type: str,
    needs_calibration: bool,
    current_threshold: float | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "current_status": current_status,
        "source": source,
        "evidence_level": evidence_level,
        "threshold_type": threshold_type,
        "needs_calibration": needs_calibration,
    }
    if current_threshold is not None:
        payload["current_threshold"] = current_threshold
    return payload


def _threshold_source_classification() -> dict[str, Any]:
    return {
        "project_empirical": {
            "meaning": "已由项目内实验支持，例如 2364 证明 monthly rebalance 有偏。",
            "examples": [
                "valid_until_window_required",
                "monthly_rebalance_not_primary",
                "realistic_cost_passed",
                "conservative_cost_passed",
            ],
        },
        "engineering_safety": {
            "meaning": "防止系统副作用，例如 no broker / no paper-shadow。",
            "examples": [
                "broker_action_enabled=false",
                "paper_shadow_enabled=false",
                "event_append_enabled=false",
            ],
        },
        "conservative_heuristic": {
            "meaning": "人工保守设置，尚未统计校准。",
            "examples": [
                "time_slice_pass_rate >= 0.60",
                "regime_slice_pass_rate >= 0.50",
                "reference_candidate_cannot_accept",
            ],
        },
        "owner_preference": {
            "meaning": "反映 owner 风险偏好，需要 owner review 记录。",
            "examples": ["drawdown_materiality", "paper_shadow_gate_threshold"],
        },
        "methodology_proposal": {
            "meaning": "新提出但未验证的方法论。",
            "examples": [
                "regime_expectation_score",
                "return_per_drawdown_penalty",
                "OWNER_REVIEW_FOR_RESEARCH_ONLY_OBSERVATION",
            ],
        },
        "needs_statistical_calibration": {
            "meaning": "应在后续用 historical candidate outcome matrix 校准。",
            "examples": list(THRESHOLDS_REQUIRING_STATISTICAL_CALIBRATION),
        },
    }


def _gate_taxonomy() -> dict[str, Any]:
    return {
        "research_only_observation_gate": {
            "side_effect": "none",
            "artifact_only": True,
            "event_append": False,
            "outcome_binding": False,
            "paper_trade": False,
            "broker_action": False,
            "threshold_level": "moderate",
            "owner_review_allowed": True,
            "auto_accept_allowed": "very_limited",
            "principle": (
                "Research-only observation should not use paper-shadow-like "
                "thresholds because it observes without execution side effects."
            ),
        },
        "paper_shadow_gate": {
            "side_effect": "creates_paper_trade_or_shadow_position",
            "threshold_level": "high",
            "explicit_owner_approval_required": True,
            "stable_slice_evidence_required": True,
            "event_outcome_policy_required": True,
            "broker_must_remain_disabled": True,
            "paper_shadow_enabled": False,
            "paper_shadow_in_scope": False,
        },
        "production_broker_gate": {
            "side_effect": "real_execution_or_capital_risk",
            "threshold_level": "highest",
            "explicit_owner_approval_required": True,
            "currently_out_of_scope": True,
            "production_enabled": False,
            "broker_action_enabled": False,
        },
    }


def _candidate_threshold_outcome_matrix(
    sources: Mapping[str, Any],
) -> list[dict[str, Any]]:
    rows_by_id = {
        str(row.get("candidate_id")): row
        for row in _expanded_candidate_ranking_rows(sources)
        if row.get("candidate_id")
    }
    preview_by_id = _candidate_preview_by_id(sources)
    matrix = []
    for candidate_id in REQUIRED_CANDIDATES:
        row = _as_mapping(rows_by_id.get(candidate_id))
        preview = _as_mapping(preview_by_id.get(candidate_id))
        matrix.append(
            {
                "candidate_id": candidate_id,
                "source_task": _candidate_source_task(candidate_id),
                "latest_decision": _candidate_latest_decision(
                    candidate_id,
                    row,
                    sources,
                ),
                "dynamic_vs_static_gap": row.get("dynamic_vs_static_gap"),
                "realistic_cost_passed": row.get("realistic_cost_passed"),
                "conservative_cost_passed": row.get("conservative_cost_passed"),
                "harsh_cost_passed": row.get("harsh_cost_passed"),
                "turnover_budget_passed": row.get("turnover_budget_passed"),
                "time_slice_pass_rate": row.get("time_slice_pass_rate"),
                "regime_slice_pass_rate": row.get("regime_slice_pass_rate"),
                "drawdown_not_materially_worse": _drawdown_not_materially_worse(row),
                "drawdown_gap_vs_static": row.get("drawdown_gap_vs_static"),
                "reference_candidate": row.get("candidate_type") == "reference_candidate",
                "current_gate_blockers": preview.get(
                    "current_blockers",
                    _candidate_blockers(row),
                ),
                "candidate_value_type": _candidate_value_type(candidate_id),
                "likely_reclassification_under_calibrated_gate": preview.get(
                    "preview_decision_under_calibrated_gate",
                    "CONTINUE_OPTIMIZATION",
                ),
            }
        )
    return matrix


def _recommended_gate_policy_proposal() -> dict[str, Any]:
    return {
        "policy_update_applied": False,
        "rules_mutated": False,
        "reference_candidate_policy": {
            "current": "HARD_BLOCK_ACCEPTANCE",
            "recommended": REFERENCE_CANDIDATE_POLICY_RECOMMENDATION,
        },
        "research_only_observation": {
            "auto_accept": {
                "allowed": True,
                "requirements": [
                    "cost_adjusted_return_above_static",
                    "realistic_cost_passed",
                    "conservative_cost_passed",
                    "turnover_budget_passed",
                    "no_hard_guardrail_failure",
                    "time_slice_pass_rate >= calibrated_threshold",
                    "regime_expectation_score >= calibrated_threshold",
                    "drawdown_risk_acceptable",
                ],
            },
            "owner_review_required": {
                "allowed": True,
                "conditions": [
                    "cost_and_turnover_pass",
                    "positive_dynamic_vs_static_gap",
                    "but_slice_or_drawdown_or_reference_status_requires_judgment",
                ],
            },
            "continue_optimization": {
                "conditions": [
                    "positive_evidence_exists",
                    "but_slice_robustness_relative_reference_or_drawdown_tradeoff_insufficient",
                ],
            },
            "reject_for_now": {
                "conditions": [
                    "realistic_or_conservative_cost_failed",
                    "severe_drawdown_deterioration",
                    "invalid_execution_assumption",
                    "no_positive_dynamic_vs_static_gap",
                ],
            },
        },
        "future_statistical_calibration_needed": list(
            FUTURE_STATISTICAL_CALIBRATION_NEEDED
        ),
        "next_owner_decision_route": NEXT_ROUTE,
    }


def _required_answers() -> dict[str, Any]:
    return {
        "current_acceptance_thresholds_fully_historical": False,
        "thresholds_with_project_empirical_support": [
            "valid_until_window_required",
            "monthly_rebalance_not_primary",
            "realistic_cost_passed",
            "conservative_cost_passed",
            "lower_turnover_reference_comparison",
            "ranking_top_reference_comparison",
        ],
        "thresholds_that_are_conservative_heuristics": [
            "time_slice_pass_rate >= 0.60",
            "regime_slice_pass_rate >= 0.50",
            "reference_candidate_cannot_accept",
            "drawdown_not_materially_worse for auto-accept",
        ],
        "research_only_observation_should_be_lower_than_paper_shadow": True,
        "reference_candidate_should_be_hard_blocked": False,
        "reference_candidate_recommendation": (
            REFERENCE_CANDIDATE_POLICY_RECOMMENDATION
        ),
        "time_regime_drawdown_thresholds_need_statistical_calibration": True,
        "observation_approved_by_2388": False,
        "real_gate_modified_by_2388": False,
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
        "approve_observation": False,
        "approve_paper_shadow": False,
        "approve_broker_action": False,
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
        "json_path": str(output_root / "threshold_methodology_review_result.json"),
        "threshold_inventory_json": str(output_root / "threshold_inventory.json"),
        "gate_taxonomy_json": str(output_root / "gate_taxonomy.json"),
        "candidate_threshold_outcome_matrix_json": str(
            output_root / "candidate_threshold_outcome_matrix.json"
        ),
        "recommended_gate_policy_proposal_json": str(
            output_root / "recommended_gate_policy_proposal.json"
        ),
        "markdown_path": str(
            docs_root
            / "dynamic_strategy_research_filter_threshold_methodology_review.md"
        ),
        "threshold_inventory_markdown": str(
            docs_root / "dynamic_strategy_threshold_inventory.md"
        ),
        "gate_taxonomy_markdown": str(docs_root / "dynamic_strategy_gate_taxonomy.md"),
        "candidate_threshold_outcome_matrix_markdown": str(
            docs_root / "dynamic_strategy_candidate_threshold_outcome_matrix.md"
        ),
        "next_route_markdown": str(docs_root / "dynamic_strategy_2389_route.md"),
    }
    payload["artifact_paths"] = paths
    write_json_artifact(Path(paths["json_path"]), payload)
    write_json_artifact(
        Path(paths["threshold_inventory_json"]),
        {
            "report_type": "dynamic_strategy_threshold_inventory",
            "schema_version": "dynamic_strategy_threshold_inventory.v1",
            "task_id": TASK_ID,
            "status": payload["status"],
            "generated_at": payload["generated_at"],
            "threshold_inventory": payload.get("threshold_inventory", {}),
            "threshold_source_classification": payload.get(
                "threshold_source_classification",
                {},
            ),
            "production_effect": "none",
            "broker_action": "none",
        },
    )
    write_json_artifact(
        Path(paths["gate_taxonomy_json"]),
        {
            "report_type": "dynamic_strategy_gate_taxonomy",
            "schema_version": "dynamic_strategy_gate_taxonomy.v1",
            "task_id": TASK_ID,
            "status": payload["status"],
            "generated_at": payload["generated_at"],
            "gate_taxonomy": payload.get("gate_taxonomy", {}),
            "production_effect": "none",
            "broker_action": "none",
        },
    )
    write_json_artifact(
        Path(paths["candidate_threshold_outcome_matrix_json"]),
        {
            "report_type": "dynamic_strategy_candidate_threshold_outcome_matrix",
            "schema_version": "dynamic_strategy_candidate_threshold_outcome_matrix.v1",
            "task_id": TASK_ID,
            "status": payload["status"],
            "generated_at": payload["generated_at"],
            "candidate_threshold_outcome_matrix": payload.get(
                "candidate_threshold_outcome_matrix",
                [],
            ),
            "production_effect": "none",
            "broker_action": "none",
        },
    )
    write_json_artifact(
        Path(paths["recommended_gate_policy_proposal_json"]),
        {
            "report_type": "dynamic_strategy_recommended_gate_policy_proposal",
            "schema_version": "dynamic_strategy_recommended_gate_policy_proposal.v1",
            "task_id": TASK_ID,
            "status": payload["status"],
            "generated_at": payload["generated_at"],
            "recommended_gate_policy_proposal": payload.get(
                "recommended_gate_policy_proposal",
                {},
            ),
            "recommended_next_research_task": NEXT_ROUTE,
            "policy_update_applied": False,
            "rules_mutated": False,
            "production_effect": "none",
            "broker_action": "none",
        },
    )
    write_markdown_artifact(Path(paths["markdown_path"]), _main_markdown(payload))
    write_markdown_artifact(
        Path(paths["threshold_inventory_markdown"]),
        _threshold_inventory_markdown(payload),
    )
    write_markdown_artifact(
        Path(paths["gate_taxonomy_markdown"]),
        _gate_taxonomy_markdown(payload),
    )
    write_markdown_artifact(
        Path(paths["candidate_threshold_outcome_matrix_markdown"]),
        _candidate_matrix_markdown(payload),
    )
    write_markdown_artifact(
        Path(paths["next_route_markdown"]),
        _route_markdown(payload),
    )


def _main_markdown(payload: Mapping[str, Any]) -> str:
    answers = _as_mapping(payload.get("required_answers"))
    source_types = _as_mapping(payload.get("threshold_source_classification"))
    project_empirical_examples = source_types.get("project_empirical", {}).get("examples")
    conservative_examples = source_types.get("conservative_heuristic", {}).get(
        "examples"
    )
    methodology_examples = source_types.get("methodology_proposal", {}).get(
        "examples"
    )
    time_regime_drawdown_calibration = answers.get(
        "time_regime_drawdown_thresholds_need_statistical_calibration"
    )
    calibration_thresholds = payload.get("thresholds_requiring_statistical_calibration")
    explicit_non_goals = json.dumps(
        payload.get("explicit_non_goals", {}),
        ensure_ascii=False,
        sort_keys=True,
    )
    return "\n".join(
        [
            "# Dynamic strategy research filter threshold methodology review",
            "",
            "## 1. Executive summary",
            "",
            f"- status：`{payload.get('status')}`",
            (
                "- current gate may be too strict for research-only observation："
                f"`{payload.get('current_gate_may_be_too_strict_for_research_only_observation')}`"
            ),
            (
                "- reference candidate policy recommendation："
                f"`{payload.get('reference_candidate_policy_recommendation')}`"
            ),
            f"- next route：`{payload.get('recommended_next_research_task')}`",
            (
                "- 2388 不批准 observation，不修改真实 gate，"
                "不进入 paper-shadow / production / broker。"
            ),
            "",
            "## 2. Why threshold methodology matters",
            "",
            (
                "- 当前验收标准并非完全基于历史样本统计；它混合了风险先验、"
                "工程安全边界、项目内回测经验、人工保守判断和 owner risk preference。"
            ),
            "- 因此 threshold 本身需要被版本化、分类、解释和校准。",
            "",
            "## 3. Current threshold inventory",
            "",
            "```json",
            json.dumps(
                payload.get("threshold_inventory", {}),
                ensure_ascii=False,
                indent=2,
                sort_keys=True,
            ),
            "```",
            "",
            "## 4. Threshold source classification",
            "",
            f"- project_empirical examples：`{project_empirical_examples}`",
            f"- conservative_heuristic examples：`{conservative_examples}`",
            f"- methodology_proposal examples：`{methodology_examples}`",
            "",
            "## 5. Gate taxonomy",
            "",
            "```json",
            json.dumps(
                payload.get("gate_taxonomy", {}),
                ensure_ascii=False,
                indent=2,
                sort_keys=True,
            ),
            "```",
            "",
            "## 6. Candidate threshold outcome matrix",
            "",
            _candidate_matrix_table(payload.get("candidate_threshold_outcome_matrix", [])),
            "",
            "## 7. Research-only observation vs paper-shadow separation",
            "",
            (
                "- research-only observation 是 artifact-only/no-side-effect，"
                "门槛应低于 paper-shadow gate，并允许 `OWNER_REVIEW_REQUIRED`。"
            ),
            (
                "- paper-shadow 会创建 paper trades 或 shadow positions，"
                "必须保持更高门槛和 explicit owner approval。"
            ),
            "",
            "## 8. Reference candidate policy review",
            "",
            (
                "- reference candidate 是否应 hard-block："
                f"`{answers.get('reference_candidate_should_be_hard_blocked')}`"
            ),
            f"- recommendation：`{answers.get('reference_candidate_recommendation')}`",
            "",
            "## 9. Time / regime / drawdown threshold methodology",
            "",
            (
                "- time/regime/drawdown thresholds need calibration："
                f"`{time_regime_drawdown_calibration}`"
            ),
            f"- thresholds requiring calibration：`{calibration_thresholds}`",
            "",
            "## 10. Recommended gate policy proposal",
            "",
            "```json",
            json.dumps(
                payload.get("recommended_gate_policy_proposal", {}),
                ensure_ascii=False,
                indent=2,
                sort_keys=True,
            ),
            "```",
            "",
            "## 11. Future statistical calibration needs",
            "",
            f"- `{payload.get('future_statistical_calibration_needed')}`",
            (
                "- optional future route："
                f"`{payload.get('future_statistical_calibration_optional_route')}`"
            ),
            "",
            "## 12. Explicit non-goals",
            "",
            f"- `{explicit_non_goals}`",
            "",
            "## 13. Recommended next route",
            "",
            f"- `{payload.get('recommended_next_research_task')}`",
            "",
        ]
    )


def _threshold_inventory_markdown(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# Dynamic strategy threshold inventory",
            "",
            f"- status：`{payload.get('status')}`",
            "",
            "```json",
            json.dumps(
                payload.get("threshold_inventory", {}),
                ensure_ascii=False,
                indent=2,
                sort_keys=True,
            ),
            "```",
            "",
        ]
    )


def _gate_taxonomy_markdown(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# Dynamic strategy gate taxonomy",
            "",
            f"- status：`{payload.get('status')}`",
            "- research-only observation / paper-shadow / production-broker gates are separated.",
            "",
            "```json",
            json.dumps(
                payload.get("gate_taxonomy", {}),
                ensure_ascii=False,
                indent=2,
                sort_keys=True,
            ),
            "```",
            "",
        ]
    )


def _candidate_matrix_markdown(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# Dynamic strategy candidate threshold outcome matrix",
            "",
            f"- status：`{payload.get('status')}`",
            "",
            _candidate_matrix_table(payload.get("candidate_threshold_outcome_matrix", [])),
            "",
        ]
    )


def _route_markdown(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# Dynamic strategy TRADING-2389 route",
            "",
            f"- source task：`{TASK_ID}`",
            f"- status：`{payload.get('status')}`",
            f"- recommended next route：`{payload.get('recommended_next_research_task')}`",
            (
                "- 2389 should record owner decision using both TRADING-2387 "
                "gate calibration and TRADING-2388 threshold methodology."
            ),
            "- observation approved：`false`",
            "- paper-shadow enabled：`false`",
            "- production enabled：`false`",
            "- broker action：`none`",
            "",
        ]
    )


def _candidate_matrix_table(rows: object) -> str:
    table_rows = [
        "|Candidate|Decision|Static gap|Time|Regime|Blockers|Reclassification|",
        "|---|---|---:|---:|---:|---|---|",
    ]
    for row in _as_list(rows):
        if not isinstance(row, Mapping):
            continue
        table_rows.append(
            "|"
            f"`{row.get('candidate_id')}`|"
            f"`{row.get('latest_decision')}`|"
            f"`{row.get('dynamic_vs_static_gap')}`|"
            f"`{row.get('time_slice_pass_rate')}`|"
            f"`{row.get('regime_slice_pass_rate')}`|"
            f"`{row.get('current_gate_blockers')}`|"
            f"`{row.get('likely_reclassification_under_calibrated_gate')}`|"
        )
    return "\n".join(table_rows)


def _expanded_candidate_ranking_rows(sources: Mapping[str, Any]) -> list[dict[str, Any]]:
    ranking = _as_mapping(sources.get("expanded_candidate_ranking"))
    rows = _as_list(ranking.get("expanded_candidate_ranking"))
    if not rows:
        expanded = _as_mapping(sources.get("expanded_candidate_retest"))
        rows = _as_list(expanded.get("expanded_candidate_ranking"))
    return [dict(row) for row in rows if isinstance(row, Mapping)]


def _candidate_preview_by_id(sources: Mapping[str, Any]) -> dict[str, Any]:
    preview_doc = _as_mapping(sources.get("candidate_reclassification_preview"))
    preview = preview_doc.get("candidate_reclassification_preview")
    if isinstance(preview, Mapping):
        return dict(preview)
    gate_review = _as_mapping(sources.get("gate_calibration_review"))
    preview = gate_review.get("candidate_reclassification_preview")
    return dict(preview) if isinstance(preview, Mapping) else {}


def _candidate_latest_decision(
    candidate_id: str,
    row: Mapping[str, Any],
    sources: Mapping[str, Any],
) -> str | None:
    if row.get("decision"):
        return str(row.get("decision"))
    if candidate_id == BASE_CANDIDATE_ID:
        return _as_mapping(sources.get("targeted_retest")).get(
            "candidate_decision_after_targeted_retest"
        )
    if candidate_id == BEST_LOWER_TURNOVER_VARIANT:
        return _as_mapping(sources.get("variant_retest")).get("best_variant_decision")
    if candidate_id == "equal_risk_growth_tilt_guarded_turnover_v1":
        return _as_mapping(sources.get("guarded_variant_retest")).get(
            "best_guarded_variant_decision"
        )
    return None


def _candidate_source_task(candidate_id: str) -> str:
    if candidate_id == RANKING_TOP_CANDIDATE:
        return "TRADING-2386"
    if candidate_id == BASE_CANDIDATE_ID:
        return "TRADING-2376"
    if candidate_id == BEST_LOWER_TURNOVER_VARIANT:
        return "TRADING-2379"
    if candidate_id == "equal_risk_growth_tilt_guarded_turnover_v1":
        return "TRADING-2383"
    return "TRADING-2386"


def _candidate_value_type(candidate_id: str) -> str:
    mapping = {
        RANKING_TOP_CANDIDATE: "current_best_reference_return_leader",
        BASE_CANDIDATE_ID: "lower_turnover_reference",
        BEST_LOWER_TURNOVER_VARIANT: "robustness_repair_variant",
        "equal_risk_growth_tilt_guarded_turnover_v1": "guarded_return_reference",
        "dynamic_turnover_budgeted_growth_tilt_v1": "turnover_budget_component_value",
        "dynamic_valid_until_expiry_strict_v1": "valid_until_component_value",
    }
    return mapping.get(candidate_id, "candidate")


def _candidate_blockers(row: Mapping[str, Any]) -> list[str]:
    blockers: list[str] = []
    if row.get("candidate_type") == "reference_candidate":
        blockers.append("reference_candidate_hard_block")
    if _float(row.get("time_slice_pass_rate")) < OBSERVATION_TIME_SLICE_PASS_RATE_MIN:
        blockers.append("time_slice_pass_rate_below_acceptance")
    if _float(row.get("regime_slice_pass_rate")) < OBSERVATION_REGIME_SLICE_PASS_RATE_MIN:
        blockers.append("regime_slice_pass_rate_below_acceptance")
    if _drawdown_not_materially_worse(row) is False:
        blockers.append("drawdown_not_materially_worse=false")
    if _float(row.get("candidate_vs_guarded_ranking_top_gap")) < 0.0:
        blockers.append("guarded_gap_negative")
    return blockers


def _drawdown_not_materially_worse(row: Mapping[str, Any]) -> bool | None:
    if "drawdown_not_materially_worse" in row and isinstance(
        row.get("drawdown_not_materially_worse"),
        bool,
    ):
        return bool(row.get("drawdown_not_materially_worse"))
    reason_text = ";".join(str(item) for item in _as_list(row.get("decision_reasons")))
    reason_text += str(row.get("decision_reason", ""))
    if "drawdown_not_materially_worse=False" in reason_text:
        return False
    if "drawdown_not_materially_worse=True" in reason_text:
        return True
    if row.get("drawdown_gap_vs_static") is None:
        return None
    return _float(row.get("drawdown_gap_vs_static")) <= DRAWDOWN_WORSE_TOLERANCE


def _top_candidate_from_candidate_ranking(document: Mapping[str, Any]) -> str | None:
    rows = _as_list(document.get("candidate_ranking"))
    valid = [row for row in rows if isinstance(row, Mapping)]
    if not valid:
        return None
    valid.sort(key=lambda row: _float(row.get("rank"), default=999999.0))
    candidate = valid[0].get("candidate_id")
    return str(candidate) if isinstance(candidate, str) else None


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
