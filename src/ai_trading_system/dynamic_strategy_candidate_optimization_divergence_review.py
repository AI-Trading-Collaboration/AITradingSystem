from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.data_foundation import utc_now_iso
from ai_trading_system.dynamic_strategy_cost_turnover_cooldown_sensitivity import (
    CURRENT_DYNAMIC_DEFAULT_CANDIDATE_ID,
    DEFAULT_DYNAMIC_STRATEGY_COST_TURNOVER_COOLDOWN_SENSITIVITY_OUTPUT_ROOT,
    DEFAULT_SOURCE_CADENCE_MATRIX_PATH,
    DEFAULT_SOURCE_CANDIDATE_RANKING_PATH,
    DEFAULT_SOURCE_EVENT_DRIVEN_RETEST_PATH,
    PRIMARY_EXECUTION_CADENCE,
    _apply_relative_sensitivity_metrics,
    _scenario_policy_for_sensitivity,
    _sensitivity_matrix_row,
    _sensitivity_policy,
    _static_matrix_row,
)
from ai_trading_system.dynamic_strategy_cost_turnover_cooldown_sensitivity import (
    READY_STATUS as SOURCE_SENSITIVITY_READY_STATUS,
)
from ai_trading_system.dynamic_strategy_event_driven_retest import (
    READY_STATUS as SOURCE_RETEST_READY_STATUS,
)
from ai_trading_system.dynamic_strategy_execution_cadence_bias_audit import (
    STATIC_BASELINE_STRATEGY_ID,
    _benchmark_metrics,
    _scenario_metric_row,
    _signal_validity_profile,
    _static_baseline_row,
)
from ai_trading_system.dynamic_strategy_research_only_observation_owner_reassessment import (
    DEFAULT_DYNAMIC_STRATEGY_RESEARCH_ONLY_OBSERVATION_OWNER_REASSESSMENT_OUTPUT_ROOT,
)
from ai_trading_system.dynamic_strategy_research_only_observation_owner_reassessment import (
    FINAL_ROUTE as SOURCE_REASSESSMENT_FINAL_ROUTE,
)
from ai_trading_system.dynamic_strategy_research_only_observation_owner_reassessment import (
    READY_STATUS as SOURCE_REASSESSMENT_READY_STATUS,
)
from ai_trading_system.dynamic_strategy_top_candidate_owner_review_gate import (
    DEFAULT_DYNAMIC_STRATEGY_TOP_CANDIDATE_OWNER_REVIEW_GATE_OUTPUT_ROOT,
)
from ai_trading_system.dynamic_strategy_top_candidate_owner_review_gate import (
    READY_STATUS as SOURCE_OWNER_GATE_READY_STATUS,
)
from ai_trading_system.execution_semantics import (
    AI_REGIME_SUMMARY,
    DEFAULT_AI_REGIME_BACKTEST_START,
    DEFAULT_EXECUTION_POLICY_REGISTRY_PATH,
    DEFAULT_MARKETSTACK_PRICES_PATH,
    DEFAULT_PRICES_PATH,
    DEFAULT_RATES_PATH,
    DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    SAFETY_BOUNDARY,
    _actual_position_path,
    _attach_path_return_columns,
    _ensure_weight_columns,
    _execution_materiality_thresholds,
    _file_sha256,
    _float,
    _int,
    _load_execution_price_matrix,
    _load_policy_registry,
    _mapping,
    _policies_by_id,
    _policy_cost_bps,
    _signal_target_weight_frame,
)
from ai_trading_system.simple_baseline_portfolio_control import (
    _data_quality_gate,
    _load_registry,
)

TASK_ID = "TRADING-2375"
TASK_REGISTER_ID = (
    "TRADING-2375_DYNAMIC_STRATEGY_CANDIDATE_OPTIMIZATION_AND_"
    "RANKING_ROBUSTNESS_DIVERGENCE_REVIEW"
)
REPORT_TYPE = "dynamic_strategy_candidate_optimization_divergence_review"
SCHEMA_VERSION = "dynamic_strategy_candidate_optimization_divergence_review.v1"
READY_STATUS = (
    "DYNAMIC_STRATEGY_CANDIDATE_OPTIMIZATION_AND_"
    "RANKING_ROBUSTNESS_DIVERGENCE_REVIEW_READY"
)
BLOCKED_DATA_QUALITY_STATUS = (
    "DYNAMIC_STRATEGY_CANDIDATE_OPTIMIZATION_AND_"
    "RANKING_ROBUSTNESS_DIVERGENCE_REVIEW_BLOCKED_DATA_QUALITY"
)
BLOCKED_SOURCE_STATUS = (
    "DYNAMIC_STRATEGY_CANDIDATE_OPTIMIZATION_AND_"
    "RANKING_ROBUSTNESS_DIVERGENCE_REVIEW_BLOCKED_SOURCE_ARTIFACT"
)
NEXT_ROUTE = "TRADING-2376_Dynamic_Strategy_Optimized_Candidate_Targeted_Retest"
SELECTED_OWNER_DIRECTION = (
    "RETURN_TO_CANDIDATE_OPTIMIZATION_AND_COMPARE_RANKING_ROBUSTNESS_DIVERGENCE"
)

DECISION_ACCEPT_RESEARCH_ONLY = "ACCEPT_FOR_RESEARCH_ONLY_OBSERVATION"
DECISION_OWNER_REVIEW = "OWNER_REVIEW_REQUIRED"
DECISION_CONTINUE_OPTIMIZATION = "CONTINUE_OPTIMIZATION"
DECISION_REJECT = "REJECT_FOR_NOW"
DECISION_DEPRECATED = "DEPRECATED_BY_DIVERGENCE_REVIEW"

RANKING_TOP_FALLBACK = "equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1"
ROBUSTNESS_TOP_FALLBACK = "dynamic_regime_overlay_v0_4_lower_turnover"
FUSION_CANDIDATES: tuple[str, ...] = (
    "dynamic_regime_growth_tilt_lower_turnover_fusion_v1",
    "dynamic_regime_growth_tilt_valid_until_cooldown_v1",
    "equal_risk_growth_tilt_lower_turnover_guarded_v1",
)

DEFAULT_DYNAMIC_STRATEGY_CANDIDATE_OPTIMIZATION_DIVERGENCE_REVIEW_OUTPUT_ROOT = (
    PROJECT_ROOT / "outputs" / "research_strategies" / REPORT_TYPE
)
DEFAULT_DYNAMIC_STRATEGY_CANDIDATE_OPTIMIZATION_DIVERGENCE_REVIEW_DOCS_ROOT = (
    PROJECT_ROOT / "docs" / "research"
)
DEFAULT_SOURCE_SENSITIVITY_RESULT_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_COST_TURNOVER_COOLDOWN_SENSITIVITY_OUTPUT_ROOT
    / "sensitivity_result.json"
)
DEFAULT_SOURCE_SENSITIVITY_MATRIX_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_COST_TURNOVER_COOLDOWN_SENSITIVITY_OUTPUT_ROOT
    / "sensitivity_matrix.json"
)
DEFAULT_SOURCE_DECISION_UPDATE_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_COST_TURNOVER_COOLDOWN_SENSITIVITY_OUTPUT_ROOT
    / "decision_update.json"
)
DEFAULT_SOURCE_OWNER_REVIEW_GATE_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_TOP_CANDIDATE_OWNER_REVIEW_GATE_OUTPUT_ROOT
    / "owner_review_gate_result.json"
)
DEFAULT_SOURCE_CANDIDATE_OWNER_REVIEW_COMPARISON_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_TOP_CANDIDATE_OWNER_REVIEW_GATE_OUTPUT_ROOT
    / "candidate_owner_review_comparison.json"
)
DEFAULT_SOURCE_OWNER_REASSESSMENT_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_RESEARCH_ONLY_OBSERVATION_OWNER_REASSESSMENT_OUTPUT_ROOT
    / "owner_reassessment_result.json"
)


def run_dynamic_strategy_candidate_optimization_divergence_review(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    simple_config_path: Path = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    policy_registry_path: Path = DEFAULT_EXECUTION_POLICY_REGISTRY_PATH,
    source_event_retest_path: Path = DEFAULT_SOURCE_EVENT_DRIVEN_RETEST_PATH,
    source_candidate_ranking_path: Path = DEFAULT_SOURCE_CANDIDATE_RANKING_PATH,
    source_cadence_matrix_path: Path = DEFAULT_SOURCE_CADENCE_MATRIX_PATH,
    source_sensitivity_result_path: Path = DEFAULT_SOURCE_SENSITIVITY_RESULT_PATH,
    source_sensitivity_matrix_path: Path = DEFAULT_SOURCE_SENSITIVITY_MATRIX_PATH,
    source_decision_update_path: Path = DEFAULT_SOURCE_DECISION_UPDATE_PATH,
    source_owner_review_gate_path: Path = DEFAULT_SOURCE_OWNER_REVIEW_GATE_PATH,
    source_candidate_owner_review_comparison_path: Path = (
        DEFAULT_SOURCE_CANDIDATE_OWNER_REVIEW_COMPARISON_PATH
    ),
    source_owner_reassessment_path: Path = DEFAULT_SOURCE_OWNER_REASSESSMENT_PATH,
    output_root: Path = (
        DEFAULT_DYNAMIC_STRATEGY_CANDIDATE_OPTIMIZATION_DIVERGENCE_REVIEW_OUTPUT_ROOT
    ),
    docs_root: Path = (
        DEFAULT_DYNAMIC_STRATEGY_CANDIDATE_OPTIMIZATION_DIVERGENCE_REVIEW_DOCS_ROOT
    ),
    as_of_date: date | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
) -> dict[str, Any]:
    resolved_start = start_date or DEFAULT_AI_REGIME_BACKTEST_START
    sources = _load_sources(
        source_event_retest_path=source_event_retest_path,
        source_candidate_ranking_path=source_candidate_ranking_path,
        source_cadence_matrix_path=source_cadence_matrix_path,
        source_sensitivity_result_path=source_sensitivity_result_path,
        source_sensitivity_matrix_path=source_sensitivity_matrix_path,
        source_decision_update_path=source_decision_update_path,
        source_owner_review_gate_path=source_owner_review_gate_path,
        source_candidate_owner_review_comparison_path=(
            source_candidate_owner_review_comparison_path
        ),
        source_owner_reassessment_path=source_owner_reassessment_path,
    )
    config = _load_registry(simple_config_path)
    data_quality = _data_quality_gate(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        config=config,
        as_of_date=as_of_date,
        expected_tickers=sorted({"QQQ", "TQQQ", "SGOV"}),
    )
    policy_registry = _load_policy_registry(policy_registry_path)
    optimization_policy = _optimization_policy()
    payload = _base_payload(
        status=READY_STATUS,
        as_of_date=as_of_date,
        start_date=resolved_start,
        end_date=end_date,
        data_quality=data_quality,
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        policy_registry_path=policy_registry_path,
        sources=sources,
        optimization_policy=optimization_policy,
    )
    if not bool(data_quality.get("passed")):
        payload["status"] = BLOCKED_DATA_QUALITY_STATUS
        payload.update(_blocked_sections("data_quality_gate_failed", sources))
        _write_outputs(payload=payload, output_root=output_root, docs_root=docs_root)
        return payload
    if not bool(sources["source_ready_for_optimization_review"]):
        payload["status"] = BLOCKED_SOURCE_STATUS
        payload.update(_blocked_sections("source_artifact_validation_failed", sources))
        _write_outputs(payload=payload, output_root=output_root, docs_root=docs_root)
        return payload

    prices = _load_execution_price_matrix(
        prices_path,
        config,
        start_date=resolved_start,
        end_date=end_date,
    )
    thresholds = _execution_materiality_thresholds(policy_registry)
    policies = _policies_by_id(policy_registry)
    static_row = _static_baseline_row(
        prices=prices,
        cost_constraint_policy=optimization_policy,
        thresholds=thresholds,
    )
    static_performance = _mapping(static_row.get("performance"))
    qqq_metrics = _benchmark_metrics(prices)
    ranking_top = str(sources["ranking_top_from_2365"])
    robustness_top = str(sources["robustness_top_from_2366"])
    candidate_ids = _candidate_ids(ranking_top=ranking_top, robustness_top=robustness_top)
    optimization_grid = _optimization_grid()
    matrix_rows = _run_optimization_matrix(
        prices=prices,
        policies=policies,
        thresholds=thresholds,
        static_row=static_row,
        static_performance=static_performance,
        qqq_metrics=qqq_metrics,
        optimization_policy=optimization_policy,
        candidate_ids=candidate_ids,
        ranking_top=ranking_top,
        robustness_top=robustness_top,
        optimization_grid=optimization_grid,
    )
    decision_update = _candidate_decision_update(
        matrix_rows=matrix_rows,
        sources=sources,
        candidate_ids=candidate_ids,
        ranking_top=ranking_top,
        robustness_top=robustness_top,
    )
    divergence_explanation = _divergence_explanation(
        sources=sources,
        matrix_rows=matrix_rows,
        ranking_top=ranking_top,
        robustness_top=robustness_top,
        best_candidate=str(decision_update["best_candidate_after_optimization"]),
    )
    payload.update(
        {
            "ranking_top_from_2365": ranking_top,
            "robustness_top_from_2366": robustness_top,
            "ranking_robustness_divergence_detected": ranking_top != robustness_top,
            "optimization_mode": "research_only_target_path_variant_backtest",
            "optimization_grid": optimization_grid,
            "baseline_candidates": {
                "static_baseline": "static_baseline",
                "static_baseline_source_strategy_id": STATIC_BASELINE_STRATEGY_ID,
                "ranking_top": ranking_top,
                "robustness_top": robustness_top,
                "current_dynamic_default": CURRENT_DYNAMIC_DEFAULT_CANDIDATE_ID,
            },
            "fusion_candidates": list(FUSION_CANDIDATES),
            "optimization_matrix": matrix_rows,
            "candidate_decision_update": decision_update,
            "divergence_explanation": divergence_explanation,
            "best_candidate_after_optimization": decision_update[
                "best_candidate_after_optimization"
            ],
            "recommended_decision_after_optimization": decision_update[
                "recommended_decision_after_optimization"
            ],
            "recommended_next_research_task": NEXT_ROUTE,
            "summary_findings": _summary_findings(
                sources=sources,
                decision_update=decision_update,
                divergence_explanation=divergence_explanation,
                ranking_top=ranking_top,
                robustness_top=robustness_top,
            ),
            "required_outputs_ready": _required_outputs_ready(True),
            "optimization_review_ready": True,
            "divergence_explanation_ready": True,
            "fusion_candidates_generated": True,
            "candidate_decision_update_ready": True,
            "backtest_run": True,
            "research_quality_status": (
                "OPTIMIZATION_DIVERGENCE_REVIEW_READY_REQUIRES_2376_TARGETED_RETEST"
            ),
        }
    )
    _write_outputs(payload=payload, output_root=output_root, docs_root=docs_root)
    return payload


def _base_payload(
    *,
    status: str,
    as_of_date: date | None,
    start_date: date,
    end_date: date | None,
    data_quality: Mapping[str, Any],
    prices_path: Path,
    marketstack_prices_path: Path,
    rates_path: Path,
    policy_registry_path: Path,
    sources: Mapping[str, Any],
    optimization_policy: Mapping[str, Any],
) -> dict[str, Any]:
    actual_end = str(data_quality.get("as_of") or end_date or "")
    return {
        "task_id": TASK_ID,
        "task_register_id": TASK_REGISTER_ID,
        "report_type": REPORT_TYPE,
        "schema_version": SCHEMA_VERSION,
        "status": status,
        "generated_at": utc_now_iso(),
        "as_of": as_of_date.isoformat() if as_of_date else data_quality.get("as_of"),
        "source_tasks": ["TRADING-2365", "TRADING-2366", "TRADING-2367", "TRADING-2374"],
        "source_artifacts": sources["source_artifacts"],
        "source_status": sources["source_status"],
        "source_ready_for_optimization_review": sources[
            "source_ready_for_optimization_review"
        ],
        "source_validation_errors": sources["source_validation_errors"],
        "owner_reassessment_source": "TRADING-2374",
        "selected_owner_direction": SELECTED_OWNER_DIRECTION,
        "primary_execution_cadence": PRIMARY_EXECUTION_CADENCE,
        "comparison_cadences": [
            "static_baseline",
            PRIMARY_EXECUTION_CADENCE,
            "cooldown_limited_event_driven",
            "signal_event_driven",
        ],
        "monthly_rebalance": {
            "allowed_for_reference": True,
            "allowed_for_primary_ranking": False,
        },
        "market_regime": AI_REGIME_SUMMARY["market_regime"],
        "market_regime_summary": dict(AI_REGIME_SUMMARY),
        "requested_date_range": {
            "start": start_date.isoformat(),
            "end": end_date.isoformat() if end_date else actual_end,
        },
        "data_quality": dict(data_quality),
        "data_quality_gate_executed": True,
        "data_sources": {
            "prices_path": str(prices_path),
            "marketstack_prices_path": str(marketstack_prices_path),
            "rates_path": str(rates_path),
            "policy_registry_path": str(policy_registry_path),
            "prices_checksum": data_quality.get("price_checksum"),
            "rates_checksum": data_quality.get("rate_checksum"),
            "policy_registry_sha256": _file_sha256(policy_registry_path),
            "download_timestamp": data_quality.get("checked_at"),
            "price_row_count": data_quality.get("price_row_count"),
            "rate_row_count": data_quality.get("rate_row_count"),
        },
        "optimization_policy": dict(optimization_policy),
        "research_only": SAFETY_BOUNDARY["research_only"],
        "observe_only": SAFETY_BOUNDARY["observe_only"],
        "manual_review_required": SAFETY_BOUNDARY["manual_review_required"],
        "promotion_allowed": False,
        "paper_shadow_allowed": False,
        "paper_shadow_enabled": False,
        "paper_shadow_attempted": False,
        "paper_shadow_approved": False,
        "paper_trade_created": False,
        "shadow_position_created": False,
        "scheduler_enabled": False,
        "scheduler_attempted": False,
        "scheduled_task_created": False,
        "event_append_enabled": False,
        "event_append_attempted": False,
        "event_append_approved": False,
        "historical_event_log_mutated": False,
        "outcome_binding_enabled": False,
        "outcome_binding_attempted": False,
        "outcome_binding_approved": False,
        "outcome_store_mutated": False,
        "production_allowed": False,
        "production_enabled": False,
        "production_approved": False,
        "production_effect": "none",
        "broker_action": "none",
        "broker_action_enabled": False,
        "broker_action_attempted": False,
        "order_generated": False,
        "daily_report_generated": False,
        "next_route": NEXT_ROUTE,
        "artifact_paths": {},
    }


def _blocked_sections(reason: str, sources: Mapping[str, Any]) -> dict[str, Any]:
    ranking_top = str(sources.get("ranking_top_from_2365") or RANKING_TOP_FALLBACK)
    robustness_top = str(
        sources.get("robustness_top_from_2366") or ROBUSTNESS_TOP_FALLBACK
    )
    return {
        "ranking_top_from_2365": ranking_top,
        "robustness_top_from_2366": robustness_top,
        "ranking_robustness_divergence_detected": ranking_top != robustness_top,
        "optimization_mode": "blocked_fail_closed",
        "optimization_grid": _optimization_grid(),
        "baseline_candidates": {
            "static_baseline": "static_baseline",
            "static_baseline_source_strategy_id": STATIC_BASELINE_STRATEGY_ID,
            "ranking_top": ranking_top,
            "robustness_top": robustness_top,
            "current_dynamic_default": CURRENT_DYNAMIC_DEFAULT_CANDIDATE_ID,
        },
        "fusion_candidates": list(FUSION_CANDIDATES),
        "optimization_matrix": [],
        "candidate_decision_update": {
            "schema_version": "dynamic_strategy_candidate_decision_update.v1",
            "decision_update_ready": False,
            "blocked_reason": reason,
            "recommended_next_research_task": NEXT_ROUTE,
        },
        "divergence_explanation": {
            "blocked_reason": reason,
            "divergence_explanation_ready": False,
        },
        "best_candidate_after_optimization": None,
        "recommended_decision_after_optimization": DECISION_OWNER_REVIEW,
        "recommended_next_research_task": NEXT_ROUTE,
        "summary_findings": {
            "blocked_reason": reason,
            "valid_until_window_remains_default": True,
            "paper_shadow_remains_disabled": True,
        },
        "required_outputs_ready": _required_outputs_ready(False),
        "optimization_review_ready": False,
        "divergence_explanation_ready": False,
        "fusion_candidates_generated": False,
        "candidate_decision_update_ready": False,
        "backtest_run": False,
        "research_quality_status": "BLOCKED_FAIL_CLOSED",
    }


def _load_sources(
    *,
    source_event_retest_path: Path,
    source_candidate_ranking_path: Path,
    source_cadence_matrix_path: Path,
    source_sensitivity_result_path: Path,
    source_sensitivity_matrix_path: Path,
    source_decision_update_path: Path,
    source_owner_review_gate_path: Path,
    source_candidate_owner_review_comparison_path: Path,
    source_owner_reassessment_path: Path,
) -> dict[str, Any]:
    event_retest = _load_json_document(source_event_retest_path)
    candidate_ranking = _load_json_document(source_candidate_ranking_path)
    cadence_matrix = _load_json_document(source_cadence_matrix_path)
    sensitivity_result = _load_json_document(source_sensitivity_result_path)
    sensitivity_matrix = _load_json_document(source_sensitivity_matrix_path)
    decision_update = _load_json_document(source_decision_update_path)
    owner_review_gate = _load_json_document(source_owner_review_gate_path)
    candidate_owner_review_comparison = _load_json_document(
        source_candidate_owner_review_comparison_path
    )
    owner_reassessment = _load_json_document(source_owner_reassessment_path)
    ranking_rows = _as_list(
        _first_present(
            _as_mapping(candidate_ranking).get("candidate_ranking"),
            _as_mapping(event_retest).get("candidate_ranking"),
        )
    )
    decision_payload = _as_mapping(
        _first_present(
            _as_mapping(decision_update).get("decision_update"),
            _as_mapping(sensitivity_result).get("decision_update"),
        )
    )
    robustness_rows = _as_list(
        _first_present(
            decision_payload.get("robustness_ranking"),
            _as_mapping(sensitivity_result).get("robustness_ranking"),
        )
    )
    owner_rows = _as_list(
        _first_present(
            _as_mapping(candidate_owner_review_comparison).get(
                "candidate_review_comparison"
            ),
            _as_mapping(owner_review_gate).get("candidate_review_comparison"),
        )
    )
    ranking_top = _extract_ranking_top(ranking_rows, owner_review_gate, owner_reassessment)
    robustness_top = _extract_robustness_top(
        robustness_rows,
        decision_payload,
        owner_review_gate,
        owner_reassessment,
    )
    source_status = {
        "event_retest": _as_mapping(event_retest).get("status"),
        "candidate_ranking": _as_mapping(candidate_ranking).get("status"),
        "cadence_matrix": _as_mapping(cadence_matrix).get("status"),
        "sensitivity_result": _as_mapping(sensitivity_result).get("status"),
        "sensitivity_matrix": _as_mapping(sensitivity_matrix).get("status"),
        "decision_update": _as_mapping(decision_update).get("status"),
        "owner_review_gate": _as_mapping(owner_review_gate).get("status"),
        "candidate_owner_review_comparison": _as_mapping(
            candidate_owner_review_comparison
        ).get("status"),
        "owner_reassessment": _as_mapping(owner_reassessment).get("status"),
    }
    validation_errors = _source_validation_errors(
        source_status=source_status,
        event_retest=event_retest,
        owner_review_gate=owner_review_gate,
        owner_reassessment=owner_reassessment,
        ranking_rows=ranking_rows,
        robustness_rows=robustness_rows,
        owner_rows=owner_rows,
        ranking_top=ranking_top,
        robustness_top=robustness_top,
    )
    return {
        "event_retest": event_retest,
        "candidate_ranking": candidate_ranking,
        "cadence_matrix": cadence_matrix,
        "sensitivity_result": sensitivity_result,
        "sensitivity_matrix": sensitivity_matrix,
        "decision_update": decision_update,
        "decision_update_payload": decision_payload,
        "owner_review_gate": owner_review_gate,
        "candidate_owner_review_comparison": candidate_owner_review_comparison,
        "owner_reassessment": owner_reassessment,
        "ranking_rows": ranking_rows,
        "robustness_rows": robustness_rows,
        "candidate_review_comparison": owner_rows,
        "ranking_top_from_2365": ranking_top,
        "robustness_top_from_2366": robustness_top,
        "source_status": source_status,
        "source_validation_errors": validation_errors,
        "source_ready_for_optimization_review": not validation_errors,
        "source_artifacts": {
            "event_retest": _source_artifact(source_event_retest_path, event_retest),
            "candidate_ranking": _source_artifact(
                source_candidate_ranking_path,
                candidate_ranking,
            ),
            "cadence_matrix": _source_artifact(source_cadence_matrix_path, cadence_matrix),
            "sensitivity_result": _source_artifact(
                source_sensitivity_result_path,
                sensitivity_result,
            ),
            "sensitivity_matrix": _source_artifact(
                source_sensitivity_matrix_path,
                sensitivity_matrix,
            ),
            "decision_update": _source_artifact(
                source_decision_update_path,
                decision_update,
            ),
            "owner_review_gate": _source_artifact(
                source_owner_review_gate_path,
                owner_review_gate,
            ),
            "candidate_owner_review_comparison": _source_artifact(
                source_candidate_owner_review_comparison_path,
                candidate_owner_review_comparison,
            ),
            "owner_reassessment": _source_artifact(
                source_owner_reassessment_path,
                owner_reassessment,
            ),
        },
    }


def _source_validation_errors(
    *,
    source_status: Mapping[str, Any],
    event_retest: Any,
    owner_review_gate: Any,
    owner_reassessment: Any,
    ranking_rows: Sequence[Any],
    robustness_rows: Sequence[Any],
    owner_rows: Sequence[Any],
    ranking_top: str,
    robustness_top: str,
) -> list[str]:
    errors: list[str] = []
    for key in ("event_retest", "candidate_ranking", "cadence_matrix"):
        if source_status.get(key) != SOURCE_RETEST_READY_STATUS:
            errors.append(f"{key}_status_not_ready")
    for key in ("sensitivity_result", "sensitivity_matrix", "decision_update"):
        if source_status.get(key) != SOURCE_SENSITIVITY_READY_STATUS:
            errors.append(f"{key}_status_not_ready")
    if source_status.get("owner_review_gate") != SOURCE_OWNER_GATE_READY_STATUS:
        errors.append("owner_review_gate_status_not_ready")
    if (
        source_status.get("candidate_owner_review_comparison")
        != SOURCE_OWNER_GATE_READY_STATUS
    ):
        errors.append("candidate_owner_review_comparison_status_not_ready")
    if source_status.get("owner_reassessment") != SOURCE_REASSESSMENT_READY_STATUS:
        errors.append("owner_reassessment_status_not_ready")
    if _as_mapping(event_retest).get("primary_execution_cadence") != PRIMARY_EXECUTION_CADENCE:
        errors.append("event_retest_primary_execution_cadence_not_valid_until_window")
    if not ranking_rows:
        errors.append("candidate_ranking_empty")
    if not robustness_rows:
        errors.append("robustness_ranking_empty")
    if not owner_rows:
        errors.append("candidate_owner_review_comparison_empty")
    if not ranking_top:
        errors.append("ranking_top_from_2365_missing")
    if not robustness_top:
        errors.append("robustness_top_from_2366_missing")
    owner_gate = _as_mapping(owner_review_gate)
    if owner_gate.get("ranking_robustness_divergence_detected") is not True:
        errors.append("owner_review_gate_divergence_not_detected")
    if owner_gate.get("ranking_top_from_2365") not in {None, ranking_top}:
        errors.append("owner_review_gate_ranking_top_mismatch")
    if owner_gate.get("robustness_top_from_2366") not in {None, robustness_top}:
        errors.append("owner_review_gate_robustness_top_mismatch")
    reassessment = _as_mapping(owner_reassessment)
    if reassessment.get("final_route") != SOURCE_REASSESSMENT_FINAL_ROUTE:
        errors.append("owner_reassessment_final_route_not_trading_2375_checkpoint")
    if reassessment.get("continue_linear_observation_tasks") is not False:
        errors.append("owner_reassessment_continue_linear_observation_not_false")
    if reassessment.get("trading_2375_auto_created") is not False:
        errors.append("owner_reassessment_trading_2375_auto_created_not_false")
    for field in _side_effect_false_fields():
        if bool(owner_gate.get(field)):
            errors.append(f"owner_review_gate_{field}_true")
        if bool(reassessment.get(field)):
            errors.append(f"owner_reassessment_{field}_true")
    if owner_gate.get("broker_action") not in (None, "none"):
        errors.append("owner_review_gate_broker_action_not_none")
    if reassessment.get("broker_action") not in (None, "none"):
        errors.append("owner_reassessment_broker_action_not_none")
    return errors


def _optimization_policy() -> dict[str, Any]:
    policy = _sensitivity_policy(turnover_penalty=0.0, risk_cap_enabled=True)
    policy.update(
        {
            "policy_id": "dynamic_strategy_candidate_optimization_divergence_review_v1",
            "status": "pilot_research_only_target_path_variant_backtest",
            "owner": "research_governance",
            "rationale": (
                "TRADING-2375 must explain why 2365 ranking top and 2366 robustness "
                "top diverged, then test bounded target-path variants before 2376."
            ),
            "intended_effect": (
                "Prefer candidates that preserve valid-until edge while reducing "
                "drawdown, turnover pressure and cooldown fragility."
            ),
            "validation_evidence": (
                "Source artifacts from TRADING-2365/2366/2367/2374, current "
                "cached-data quality gate, focused tests, and real CLI run."
            ),
            "review_condition": (
                "Replace pilot scoring with TRADING-2376 targeted retest evidence "
                "before any research-only observation or paper-shadow discussion."
            ),
            "score_policy": {
                "score_components": [
                    "conservative_dynamic_vs_static_gap",
                    "harsh_dynamic_vs_static_gap",
                    "risk_adjusted_return",
                    "drawdown_guard",
                    "turnover_guard",
                ],
                "decision_boundary": (
                    "Positive conservative and harsh gaps are required before a "
                    "candidate can be selected for targeted retest."
                ),
            },
        }
    )
    return policy


def _optimization_grid() -> dict[str, Any]:
    scenarios = [
        _scenario(
            "base",
            transaction_cost_bps=2,
            slippage_bps=2,
            cooldown_days=1,
            min_holding_days=1,
            max_turnover_per_month=None,
        ),
        _scenario(
            "realistic",
            transaction_cost_bps=5,
            slippage_bps=5,
            cooldown_days=3,
            min_holding_days=3,
            max_turnover_per_month=1.0,
        ),
        _scenario(
            "conservative",
            transaction_cost_bps=10,
            slippage_bps=10,
            cooldown_days=5,
            min_holding_days=5,
            max_turnover_per_month=0.5,
        ),
        _scenario(
            "harsh",
            transaction_cost_bps=20,
            slippage_bps=10,
            cooldown_days=10,
            min_holding_days=10,
            max_turnover_per_month=0.25,
        ),
    ]
    return {
        "grid_type": "candidate_optimization_stress_grid",
        "primary_execution_cadence": PRIMARY_EXECUTION_CADENCE,
        "monthly_rebalance_allowed_for_primary_ranking": False,
        "scenario_count": len(scenarios),
        "scenarios": scenarios,
        "optimization_axes": {
            "ranking_top_optimization_axes": [
                "increase_cooldown_days",
                "increase_min_holding_days",
                "lower_max_single_step_weight_delta",
                "apply_monthly_turnover_cap",
                "stricter_risk_cap",
                "no_stale_signal_carry_forward",
            ],
            "robustness_top_optimization_axes": [
                "modest_growth_tilt_when_signal_confirmed",
                "allow_small_risk_on_increment_under_low_volatility",
                "shorten_reentry_delay_after_risk_off",
                "preserve_valid_until_window",
            ],
            "fusion_rules": [
                "primary_execution_cadence=valid_until_window",
                "growth_tilt_allowed_only_when_trend_confirmed",
                "turnover_guardrails_apply",
                "no_signal_after_valid_until",
            ],
        },
    }


def _scenario(
    scenario_id: str,
    *,
    transaction_cost_bps: float,
    slippage_bps: float,
    cooldown_days: int,
    min_holding_days: int,
    max_turnover_per_month: float | None,
) -> dict[str, Any]:
    return {
        "scenario_id": scenario_id,
        "scenario_group": "optimization_stress",
        "transaction_cost_bps": float(transaction_cost_bps),
        "slippage_bps": float(slippage_bps),
        "cooldown_days": int(cooldown_days),
        "min_holding_days": int(min_holding_days),
        "max_turnover_per_month": max_turnover_per_month,
        "max_turnover_per_month_label": _label(max_turnover_per_month, "unlimited"),
        "max_single_step_weight_delta": None,
        "max_single_step_weight_delta_label": "unrestricted",
    }


def _run_optimization_matrix(
    *,
    prices: pd.DataFrame,
    policies: Mapping[str, Mapping[str, Any]],
    thresholds: Mapping[str, float],
    static_row: Mapping[str, Any],
    static_performance: Mapping[str, Any],
    qqq_metrics: Mapping[str, Any],
    optimization_policy: Mapping[str, Any],
    candidate_ids: Sequence[str],
    ranking_top: str,
    robustness_top: str,
    optimization_grid: Mapping[str, Any],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for scenario in _as_list(optimization_grid.get("scenarios")):
        scenario_map = _as_mapping(scenario)
        static_matrix_row = _static_matrix_row(
            scenario=scenario_map,
            static_row=static_row,
            static_performance=static_performance,
        )
        static_matrix_row["candidate_id"] = "static_baseline"
        static_matrix_row["strategy_id"] = "static_baseline"
        static_matrix_row["source_strategy_id"] = STATIC_BASELINE_STRATEGY_ID
        static_matrix_row["optimization_role"] = "static_baseline"
        rows.append(static_matrix_row)
        for candidate_id in candidate_ids:
            target_weights = _candidate_target_weights(
                candidate_id=candidate_id,
                ranking_top=ranking_top,
                robustness_top=robustness_top,
                prices=prices,
            )
            policy = _scenario_policy_for_sensitivity(
                cadence=PRIMARY_EXECUTION_CADENCE,
                scenario=scenario_map,
                policies=policies,
            )
            actual_weights, path_rows = _actual_position_path(
                strategy_id=candidate_id,
                execution_policy_id=str(policy["execution_policy_id"]),
                target_weights=target_weights,
                policy=policy,
                signal_validity_profile=_signal_validity_profile(
                    PRIMARY_EXECUTION_CADENCE,
                    policy,
                ),
                enable_staleness_filter=True,
                stale_action="suppress_rebalance",
            )
            _attach_path_return_columns(
                prices=prices,
                target_weights=target_weights,
                actual_weights=actual_weights,
                path_rows=path_rows,
                cost_bps=_policy_cost_bps(policy),
            )
            metric_row = _scenario_metric_row(
                prices=prices,
                strategy_id=candidate_id,
                scenario_id=PRIMARY_EXECUTION_CADENCE,
                policy=policy,
                target_weights=target_weights,
                actual_weights=actual_weights,
                path_rows=path_rows,
                static_baseline=static_row,
                qqq_metrics=qqq_metrics,
                thresholds=thresholds,
                cost_constraint_policy=optimization_policy,
            )
            matrix_row = _sensitivity_matrix_row(
                scenario=scenario_map,
                cadence=PRIMARY_EXECUTION_CADENCE,
                row=metric_row,
                prices=prices,
                actual_weights=actual_weights,
                policy=policy,
                static_performance=static_performance,
                turnover_penalty=0.0,
            )
            matrix_row["optimization_role"] = _candidate_role(
                candidate_id,
                ranking_top=ranking_top,
                robustness_top=robustness_top,
            )
            rows.append(matrix_row)
    _apply_relative_sensitivity_metrics(rows)
    return rows


def _candidate_target_weights(
    *,
    candidate_id: str,
    ranking_top: str,
    robustness_top: str,
    prices: pd.DataFrame,
) -> pd.DataFrame:
    ranking = _signal_target_weight_frame(ranking_top, prices)
    robustness = _signal_target_weight_frame(robustness_top, prices)
    if candidate_id == ranking_top:
        return ranking
    if candidate_id == robustness_top:
        return robustness
    if candidate_id == CURRENT_DYNAMIC_DEFAULT_CANDIDATE_ID:
        return _signal_target_weight_frame(CURRENT_DYNAMIC_DEFAULT_CANDIDATE_ID, prices)
    drawdown = prices["QQQ"] / prices["QQQ"].cummax() - 1.0
    if candidate_id == "dynamic_regime_growth_tilt_lower_turnover_fusion_v1":
        weights = ranking * 0.58 + robustness * 0.42
        weights = weights.rolling(5, min_periods=1).mean()
        weights.loc[drawdown <= -0.10, "QQQ"] = robustness.loc[drawdown <= -0.10, "QQQ"]
        return _normalized_qqq_sgov(weights, lower=0.18, upper=0.82)
    if candidate_id == "dynamic_regime_growth_tilt_valid_until_cooldown_v1":
        weights = ranking * 0.35 + robustness * 0.65
        weights = weights.rolling(10, min_periods=1).mean()
        weights.loc[drawdown <= -0.12, "QQQ"] = robustness.loc[drawdown <= -0.12, "QQQ"]
        return _normalized_qqq_sgov(weights, lower=0.20, upper=0.78)
    if candidate_id == "equal_risk_growth_tilt_lower_turnover_guarded_v1":
        weights = ranking * 0.70 + robustness * 0.30
        weights = weights.rolling(8, min_periods=1).mean()
        weights.loc[drawdown <= -0.08, "QQQ"] = (
            weights.loc[drawdown <= -0.08, "QQQ"] * 0.80
        )
        return _normalized_qqq_sgov(weights, lower=0.15, upper=0.85)
    return _signal_target_weight_frame(candidate_id, prices)


def _normalized_qqq_sgov(
    weights: pd.DataFrame,
    *,
    lower: float,
    upper: float,
) -> pd.DataFrame:
    qqq = weights["QQQ"].clip(lower=lower, upper=upper).fillna(0.5)
    return _ensure_weight_columns(
        pd.DataFrame(
            {
                "QQQ": qqq,
                "TQQQ": 0.0,
                "SGOV": 1.0 - qqq,
            },
            index=weights.index,
        )
    )


def _candidate_decision_update(
    *,
    matrix_rows: Sequence[Mapping[str, Any]],
    sources: Mapping[str, Any],
    candidate_ids: Sequence[str],
    ranking_top: str,
    robustness_top: str,
) -> dict[str, Any]:
    candidate_summaries = [
        _candidate_summary(
            candidate_id=candidate_id,
            matrix_rows=matrix_rows,
            sources=sources,
            ranking_top=ranking_top,
            robustness_top=robustness_top,
        )
        for candidate_id in candidate_ids
    ]
    eligible = [
        row
        for row in candidate_summaries
        if row["survives_realistic_cost"] and row["survives_conservative_cost"]
    ]
    selectable = eligible or candidate_summaries
    best = sorted(
        selectable,
        key=lambda row: (
            _float(row.get("optimization_score")),
            _float(row.get("conservative_dynamic_vs_static_gap")),
            _float(row.get("harsh_dynamic_vs_static_gap")),
        ),
        reverse=True,
    )[0]
    recommended_decision = _recommended_decision_after_optimization(best)
    return {
        "schema_version": "dynamic_strategy_candidate_decision_update.v1",
        "decision_update_ready": True,
        "ranking_top_from_2365": ranking_top,
        "robustness_top_from_2366": robustness_top,
        "ranking_robustness_divergence_detected": ranking_top != robustness_top,
        "candidate_decisions": candidate_summaries,
        "best_candidate_after_optimization": best["candidate_id"],
        "recommended_decision_after_optimization": recommended_decision,
        "recommended_next_research_task": NEXT_ROUTE,
        "monthly_rebalance_allowed_for_primary_ranking": False,
        "paper_shadow_enabled": False,
        "production_enabled": False,
        "broker_action_enabled": False,
    }


def _candidate_summary(
    *,
    candidate_id: str,
    matrix_rows: Sequence[Mapping[str, Any]],
    sources: Mapping[str, Any],
    ranking_top: str,
    robustness_top: str,
) -> dict[str, Any]:
    rows = {
        str(row.get("scenario_id")): _as_mapping(row)
        for row in matrix_rows
        if row.get("candidate_id") == candidate_id
        and row.get("execution_cadence") == PRIMARY_EXECUTION_CADENCE
    }
    base = rows.get("base", {})
    realistic = rows.get("realistic", {})
    conservative = rows.get("conservative", {})
    harsh = rows.get("harsh", {})
    cost = _as_mapping(realistic.get("cost_metrics"))
    perf = _as_mapping(realistic.get("performance_metrics"))
    turnover = _as_mapping(realistic.get("turnover_metrics"))
    cooldown = _as_mapping(realistic.get("cooldown_metrics"))
    base_gap = _dynamic_gap(base)
    realistic_gap = _dynamic_gap(realistic)
    conservative_gap = _dynamic_gap(conservative)
    harsh_gap = _dynamic_gap(harsh)
    max_drawdown = _float(perf.get("max_drawdown"))
    annual_return = _float(perf.get("annualized_return"))
    turnover_value = _float(turnover.get("turnover"))
    ranking_row = _source_ranking_row(sources, candidate_id)
    robustness_row = _source_robustness_row(sources, candidate_id)
    optimization_score = _optimization_score(
        conservative_gap=conservative_gap,
        harsh_gap=harsh_gap,
        annual_return=annual_return,
        max_drawdown=max_drawdown,
        turnover=turnover_value,
    )
    decision = _candidate_decision(
        candidate_id=candidate_id,
        realistic_gap=realistic_gap,
        conservative_gap=conservative_gap,
        harsh_gap=harsh_gap,
        optimization_score=optimization_score,
        ranking_top=ranking_top,
        robustness_top=robustness_top,
    )
    return {
        "candidate_id": candidate_id,
        "optimization_role": _candidate_role(
            candidate_id,
            ranking_top=ranking_top,
            robustness_top=robustness_top,
        ),
        "ranking_rank_from_2365": _first_number(ranking_row.get("rank")),
        "robustness_rank_from_2366": _first_number(robustness_row.get("robust_rank")),
        "ranking_score": _ranking_score(ranking_row),
        "robustness_score": _first_number(robustness_row.get("robustness_score")),
        "base_dynamic_vs_static_gap": base_gap,
        "realistic_dynamic_vs_static_gap": realistic_gap,
        "conservative_dynamic_vs_static_gap": conservative_gap,
        "harsh_dynamic_vs_static_gap": harsh_gap,
        "cost_adjusted_return": cost.get("cost_adjusted_return"),
        "annualized_return": annual_return,
        "total_return": perf.get("total_return"),
        "max_drawdown": max_drawdown,
        "volatility": perf.get("volatility"),
        "sharpe_or_sortino_if_available": _first_present(
            perf.get("sortino"),
            perf.get("sharpe"),
        ),
        "downside_capture": perf.get("downside_capture"),
        "upside_capture": perf.get("upside_capture"),
        "gross_return": cost.get("gross_return"),
        "transaction_cost_drag": cost.get("transaction_cost_drag"),
        "slippage_drag": cost.get("slippage_drag"),
        "turnover": turnover_value,
        "rebalance_count": turnover.get("rebalance_count"),
        "average_holding_days": turnover.get("average_holding_days"),
        "max_monthly_turnover": realistic.get("max_turnover_per_month"),
        "turnover_adjusted_score": turnover.get("turnover_adjusted_score"),
        "cooldown_block_count": cooldown.get("cooldown_block_count"),
        "missed_signal_due_to_cooldown": cooldown.get("missed_signal_due_to_cooldown"),
        "stale_signal_prevented_count": cooldown.get("stale_signal_prevented_count"),
        "cooldown_adjusted_return_gap": cooldown.get("cooldown_adjusted_return_gap"),
        "ranking_vs_robustness_gap": _ranking_vs_robustness_gap(
            ranking_row,
            robustness_row,
        ),
        "upside_vs_turnover_tradeoff": _safe_ratio(annual_return, turnover_value),
        "return_vs_drawdown_tradeoff": _safe_ratio(annual_return, abs(max_drawdown)),
        "cost_fragility_score": round(base_gap - harsh_gap, 6),
        "optimization_score": optimization_score,
        "survives_realistic_cost": realistic_gap > 0.0,
        "survives_conservative_cost": conservative_gap > 0.0,
        "survives_harsh_cost": harsh_gap > 0.0,
        "improvement_over_static": realistic_gap > 0.0,
        "improvement_over_current_dynamic_default": _improves_current_default(
            candidate_id=candidate_id,
            matrix_rows=matrix_rows,
            realistic_gap=realistic_gap,
        ),
        "recommended_decision": decision,
    }


def _optimization_score(
    *,
    conservative_gap: float,
    harsh_gap: float,
    annual_return: float,
    max_drawdown: float,
    turnover: float,
) -> float:
    drawdown_guard = max(0.0, 0.18 - abs(max_drawdown))
    turnover_guard = max(0.0, 2.0 - turnover) / 10.0
    risk_adjusted_return = _safe_ratio(annual_return, abs(max_drawdown))
    return round(
        conservative_gap
        + harsh_gap
        + risk_adjusted_return * 0.01
        + drawdown_guard
        + turnover_guard,
        6,
    )


def _candidate_decision(
    *,
    candidate_id: str,
    realistic_gap: float,
    conservative_gap: float,
    harsh_gap: float,
    optimization_score: float,
    ranking_top: str,
    robustness_top: str,
) -> str:
    if realistic_gap <= 0.0:
        return DECISION_REJECT
    if conservative_gap <= 0.0:
        return DECISION_CONTINUE_OPTIMIZATION
    if harsh_gap <= 0.0:
        return DECISION_OWNER_REVIEW
    if candidate_id in FUSION_CANDIDATES and optimization_score > 0.0:
        return DECISION_CONTINUE_OPTIMIZATION
    if candidate_id in {ranking_top, robustness_top}:
        return DECISION_OWNER_REVIEW
    return DECISION_DEPRECATED


def _recommended_decision_after_optimization(best: Mapping[str, Any]) -> str:
    if best.get("recommended_decision") == DECISION_REJECT:
        return DECISION_REJECT
    if best.get("candidate_id") in FUSION_CANDIDATES:
        return DECISION_CONTINUE_OPTIMIZATION
    return DECISION_OWNER_REVIEW


def _divergence_explanation(
    *,
    sources: Mapping[str, Any],
    matrix_rows: Sequence[Mapping[str, Any]],
    ranking_top: str,
    robustness_top: str,
    best_candidate: str,
) -> dict[str, Any]:
    ranking_source = _source_ranking_row(sources, ranking_top)
    robustness_source = _source_ranking_row(sources, robustness_top)
    ranking_review = _source_review_row(sources, ranking_top)
    robustness_review = _source_review_row(sources, robustness_top)
    ranking_summary = _candidate_summary(
        candidate_id=ranking_top,
        matrix_rows=matrix_rows,
        sources=sources,
        ranking_top=ranking_top,
        robustness_top=robustness_top,
    )
    robustness_summary = _candidate_summary(
        candidate_id=robustness_top,
        matrix_rows=matrix_rows,
        sources=sources,
        ranking_top=ranking_top,
        robustness_top=robustness_top,
    )
    fusion_summary = _candidate_summary(
        candidate_id=best_candidate,
        matrix_rows=matrix_rows,
        sources=sources,
        ranking_top=ranking_top,
        robustness_top=robustness_top,
    )
    return {
        "schema_version": "dynamic_strategy_ranking_robustness_divergence_review.v1",
        "divergence_explanation_ready": True,
        "ranking_top_why_leads": (
            "2365 ranking top leads because it has the largest valid-until "
            "cost-adjusted return and dynamic-vs-static gap, but it accepts "
            "higher drawdown and more frequent rebalances."
        ),
        "robustness_top_why_robust": (
            "2366 robustness top ranks first because it keeps lower drawdown, "
            "longer average holding days, fewer false risk-off events and positive "
            "stress survival, while giving up upside."
        ),
        "ranking_top_decomposition": {
            "candidate_id": ranking_top,
            "source_rank": ranking_source.get("rank"),
            "source_cost_adjusted_return": ranking_source.get("cost_adjusted_return"),
            "source_dynamic_vs_static_gap": ranking_source.get("dynamic_vs_static_gap"),
            "source_turnover": ranking_source.get("turnover"),
            "source_rebalance_count": ranking_source.get("rebalance_count"),
            "source_max_drawdown": ranking_source.get("max_drawdown"),
            "review_fragility_reason": ranking_review.get("fragility_reason"),
            "optimization_summary": ranking_summary,
        },
        "robustness_top_decomposition": {
            "candidate_id": robustness_top,
            "source_rank": robustness_source.get("rank"),
            "source_cost_adjusted_return": robustness_source.get("cost_adjusted_return"),
            "source_dynamic_vs_static_gap": robustness_source.get("dynamic_vs_static_gap"),
            "source_turnover": robustness_source.get("turnover"),
            "source_rebalance_count": robustness_source.get("rebalance_count"),
            "source_max_drawdown": robustness_source.get("max_drawdown"),
            "review_fragility_reason": robustness_review.get("fragility_reason"),
            "optimization_summary": robustness_summary,
        },
        "fusion_candidate_results": fusion_summary,
        "ranking_top_can_be_made_robust": _yes_no(
            ranking_summary["survives_conservative_cost"]
            and ranking_summary["optimization_score"] > 0.0
        ),
        "robustness_top_can_capture_more_upside": _yes_no(
            robustness_summary["survives_conservative_cost"]
            and robustness_summary["annualized_return"]
            < fusion_summary["annualized_return"]
        ),
        "fusion_candidate_outperforms_both": _yes_no(
            best_candidate in FUSION_CANDIDATES
            and fusion_summary["optimization_score"]
            >= ranking_summary["optimization_score"]
            and fusion_summary["optimization_score"]
            >= robustness_summary["optimization_score"]
        ),
        "valid_until_window_remains_default": True,
        "paper_shadow_remains_disabled": True,
        "next_route": NEXT_ROUTE,
    }


def _summary_findings(
    *,
    sources: Mapping[str, Any],
    decision_update: Mapping[str, Any],
    divergence_explanation: Mapping[str, Any],
    ranking_top: str,
    robustness_top: str,
) -> dict[str, Any]:
    owner_gate = _as_mapping(sources.get("owner_review_gate"))
    return {
        "ranking_top_can_be_made_robust": divergence_explanation.get(
            "ranking_top_can_be_made_robust"
        ),
        "robustness_top_can_capture_more_upside": divergence_explanation.get(
            "robustness_top_can_capture_more_upside"
        ),
        "fusion_candidate_outperforms_both": divergence_explanation.get(
            "fusion_candidate_outperforms_both"
        ),
        "valid_until_window_remains_default": True,
        "paper_shadow_remains_disabled": True,
        "ranking_robustness_divergence_detected": ranking_top != robustness_top,
        "source_owner_gate_recommended_candidate": owner_gate.get(
            "recommended_gate_candidate"
        ),
        "best_candidate_after_optimization": decision_update.get(
            "best_candidate_after_optimization"
        ),
        "recommended_decision_after_optimization": decision_update.get(
            "recommended_decision_after_optimization"
        ),
        "recommended_next_research_task": NEXT_ROUTE,
    }


def _candidate_ids(*, ranking_top: str, robustness_top: str) -> list[str]:
    result: list[str] = []
    for candidate_id in (
        ranking_top,
        robustness_top,
        CURRENT_DYNAMIC_DEFAULT_CANDIDATE_ID,
        *FUSION_CANDIDATES,
    ):
        if candidate_id and candidate_id not in result:
            result.append(candidate_id)
    return result


def _candidate_role(
    candidate_id: str,
    *,
    ranking_top: str,
    robustness_top: str,
) -> str:
    if candidate_id == ranking_top:
        return "ranking_top_from_2365"
    if candidate_id == robustness_top:
        return "robustness_top_from_2366"
    if candidate_id == CURRENT_DYNAMIC_DEFAULT_CANDIDATE_ID:
        return "current_dynamic_default"
    if candidate_id in FUSION_CANDIDATES:
        return "fusion_candidate"
    return "optimization_candidate"


def _write_outputs(
    *,
    payload: dict[str, Any],
    output_root: Path,
    docs_root: Path,
) -> None:
    output_root.mkdir(parents=True, exist_ok=True)
    docs_root.mkdir(parents=True, exist_ok=True)
    artifact_paths = {
        "json_path": str(output_root / "divergence_review_result.json"),
        "optimization_matrix_json": str(output_root / "optimization_matrix.json"),
        "candidate_decision_update_json": str(
            output_root / "candidate_decision_update.json"
        ),
        "markdown_path": str(
            docs_root / "dynamic_strategy_candidate_optimization_divergence_review.md"
        ),
        "divergence_markdown": str(
            docs_root / "dynamic_strategy_ranking_vs_robustness_divergence_review.md"
        ),
        "optimization_matrix_markdown": str(
            docs_root / "dynamic_strategy_candidate_optimization_matrix.md"
        ),
        "next_route_markdown": str(docs_root / "dynamic_strategy_2376_route.md"),
    }
    payload["artifact_paths"] = artifact_paths
    _write_json(Path(artifact_paths["json_path"]), payload)
    _write_json(
        Path(artifact_paths["optimization_matrix_json"]),
        {
            "report_type": "dynamic_strategy_candidate_optimization_matrix",
            "schema_version": "dynamic_strategy_candidate_optimization_matrix.v1",
            "status": payload["status"],
            "generated_at": payload["generated_at"],
            "primary_execution_cadence": payload["primary_execution_cadence"],
            "monthly_rebalance": payload["monthly_rebalance"],
            "optimization_grid": payload.get("optimization_grid", {}),
            "optimization_matrix": payload.get("optimization_matrix", []),
            "production_effect": "none",
            "broker_action": "none",
            "paper_shadow_allowed": False,
            "production_allowed": False,
        },
    )
    _write_json(
        Path(artifact_paths["candidate_decision_update_json"]),
        {
            "report_type": "dynamic_strategy_candidate_decision_update",
            "schema_version": "dynamic_strategy_candidate_decision_update.v1",
            "status": payload["status"],
            "generated_at": payload["generated_at"],
            "candidate_decision_update": payload.get("candidate_decision_update", {}),
            "summary_findings": payload.get("summary_findings", {}),
            "production_effect": "none",
            "broker_action": "none",
            "paper_shadow_allowed": False,
            "production_allowed": False,
        },
    )
    Path(artifact_paths["markdown_path"]).write_text(
        _main_markdown(payload),
        encoding="utf-8",
    )
    Path(artifact_paths["divergence_markdown"]).write_text(
        _divergence_markdown(payload),
        encoding="utf-8",
    )
    Path(artifact_paths["optimization_matrix_markdown"]).write_text(
        _optimization_markdown(payload),
        encoding="utf-8",
    )
    Path(artifact_paths["next_route_markdown"]).write_text(
        _route_markdown(payload),
        encoding="utf-8",
    )


def _main_markdown(payload: Mapping[str, Any]) -> str:
    summary = _as_mapping(payload.get("summary_findings"))
    data_quality = _as_mapping(payload.get("data_quality"))
    return "\n".join(
        [
            "# 动态策略 candidate optimization and ranking-robustness divergence review",
            "",
            "## Executive summary",
            "",
            f"- status：`{payload.get('status')}`",
            f"- data quality：`{data_quality.get('status')}`",
            f"- primary execution cadence：`{payload.get('primary_execution_cadence')}`",
            f"- 2365 ranking top：`{payload.get('ranking_top_from_2365')}`",
            f"- 2366 robustness top：`{payload.get('robustness_top_from_2366')}`",
            (
                "- divergence detected："
                f"`{payload.get('ranking_robustness_divergence_detected')}`"
            ),
            (
                "- best candidate after optimization："
                f"`{payload.get('best_candidate_after_optimization')}`"
            ),
            (
                "- recommended decision："
                f"`{payload.get('recommended_decision_after_optimization')}`"
            ),
            f"- next route：`{payload.get('recommended_next_research_task')}`",
            "",
            "## Required answers",
            "",
            (
                "- 收益 top 为什么领先："
                f"{_as_mapping(payload.get('divergence_explanation')).get('ranking_top_why_leads')}"
            ),
            (
                "- 稳健 top 为什么更稳："
                f"{_as_mapping(payload.get('divergence_explanation')).get('robustness_top_why_robust')}"
            ),
            (
                "- 收益 top 是否可通过降风险变稳："
                f"`{summary.get('ranking_top_can_be_made_robust')}`"
            ),
            (
                "- 稳健 top 是否可通过 growth tilt 提升收益："
                f"`{summary.get('robustness_top_can_capture_more_upside')}`"
            ),
            (
                "- fusion candidate 是否优于二者："
                f"`{summary.get('fusion_candidate_outperforms_both')}`"
            ),
            "- valid_until_window 是否继续作为默认执行口径：`YES`",
            "- 是否允许 paper-shadow / production / broker：`NO`",
            f"- 下一步：`{payload.get('recommended_next_research_task')}`",
            "",
            "## Updated candidate decision table",
            "",
            _decision_table(payload),
            "",
            "## Safety boundary",
            "",
            "- 本报告只生成 strategy research evidence。",
            "- monthly rebalance 只允许作为旧口径 reference，不允许进入 primary ranking。",
            (
                "- scheduler、event append、outcome binding、paper-shadow、production、"
                "broker/order 和 daily report 全部保持 disabled / false / none。"
            ),
        ]
    )


def _divergence_markdown(payload: Mapping[str, Any]) -> str:
    explanation = _as_mapping(payload.get("divergence_explanation"))
    return "\n".join(
        [
            "# 动态策略 ranking vs robustness divergence review",
            "",
            f"- ranking top：`{payload.get('ranking_top_from_2365')}`",
            f"- robustness top：`{payload.get('robustness_top_from_2366')}`",
            f"- divergence：`{payload.get('ranking_robustness_divergence_detected')}`",
            "",
            "## Explanation",
            "",
            f"- ranking top：{explanation.get('ranking_top_why_leads')}",
            f"- robustness top：{explanation.get('robustness_top_why_robust')}",
            "",
            "## Summary flags",
            "",
            (
                "- ranking_top_can_be_made_robust："
                f"`{explanation.get('ranking_top_can_be_made_robust')}`"
            ),
            (
                "- robustness_top_can_capture_more_upside："
                f"`{explanation.get('robustness_top_can_capture_more_upside')}`"
            ),
            (
                "- fusion_candidate_outperforms_both："
                f"`{explanation.get('fusion_candidate_outperforms_both')}`"
            ),
            "- paper_shadow_remains_disabled：`true`",
        ]
    )


def _optimization_markdown(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# 动态策略 candidate optimization matrix",
            "",
            f"- status：`{payload.get('status')}`",
            f"- primary execution cadence：`{payload.get('primary_execution_cadence')}`",
            "- monthly rebalance primary ranking：`false`",
            "",
            _matrix_table(payload),
        ]
    )


def _route_markdown(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# TRADING-2376 route",
            "",
            f"- current status：`{payload.get('status')}`",
            f"- best candidate：`{payload.get('best_candidate_after_optimization')}`",
            (
                "- recommended decision："
                f"`{payload.get('recommended_decision_after_optimization')}`"
            ),
            f"- next route：`{payload.get('recommended_next_research_task')}`",
            (
                "- route boundary：optimized candidate targeted retest；不是 "
                "paper-shadow、production、broker 或 daily report approval。"
            ),
        ]
    )


def _decision_table(payload: Mapping[str, Any]) -> str:
    lines = [
        "|candidate|role|realistic_gap|conservative_gap|harsh_gap|mdd|turnover|score|decision|",
        "|---|---|---:|---:|---:|---:|---:|---:|---|",
    ]
    decision = _as_mapping(payload.get("candidate_decision_update"))
    for row in _as_list(decision.get("candidate_decisions")):
        item = _as_mapping(row)
        lines.append(
            "|"
            + "|".join(
                [
                    f"`{item.get('candidate_id')}`",
                    str(item.get("optimization_role")),
                    _fmt(item.get("realistic_dynamic_vs_static_gap")),
                    _fmt(item.get("conservative_dynamic_vs_static_gap")),
                    _fmt(item.get("harsh_dynamic_vs_static_gap")),
                    _fmt(item.get("max_drawdown")),
                    _fmt(item.get("turnover")),
                    _fmt(item.get("optimization_score")),
                    f"`{item.get('recommended_decision')}`",
                ]
            )
            + "|"
        )
    return "\n".join(lines)


def _matrix_table(payload: Mapping[str, Any]) -> str:
    lines = [
        "|candidate|scenario|role|cost_adj|gap|mdd|turnover|holding_days|cooldown_blocks|",
        "|---|---|---|---:|---:|---:|---:|---:|---:|",
    ]
    for row in _as_list(payload.get("optimization_matrix")):
        item = _as_mapping(row)
        if item.get("scenario_id") not in {"base", "realistic", "conservative", "harsh"}:
            continue
        cost = _as_mapping(item.get("cost_metrics"))
        perf = _as_mapping(item.get("performance_metrics"))
        turnover = _as_mapping(item.get("turnover_metrics"))
        cooldown = _as_mapping(item.get("cooldown_metrics"))
        lines.append(
            "|"
            + "|".join(
                [
                    f"`{item.get('candidate_id')}`",
                    str(item.get("scenario_id")),
                    str(item.get("optimization_role", "")),
                    _fmt(cost.get("cost_adjusted_return")),
                    _fmt(cost.get("cost_adjusted_dynamic_vs_static_gap")),
                    _fmt(perf.get("max_drawdown")),
                    _fmt(turnover.get("turnover")),
                    _fmt(turnover.get("average_holding_days")),
                    _fmt(cooldown.get("cooldown_block_count")),
                ]
            )
            + "|"
        )
    return "\n".join(lines)


def _required_outputs_ready(ready: bool) -> dict[str, bool]:
    return {
        "ranking_top_from_2365": ready,
        "robustness_top_from_2366": ready,
        "divergence_explanation": ready,
        "ranking_top_optimization_results": ready,
        "robustness_top_optimization_results": ready,
        "fusion_candidate_results": ready,
        "cost_turnover_cooldown_stress_results": ready,
        "candidate_decision_update": ready,
        "best_candidate_after_optimization": ready,
        "recommended_decision_after_optimization": ready,
        "recommended_next_research_task": True,
    }


def _extract_ranking_top(
    ranking_rows: Sequence[Any],
    owner_review_gate: Any,
    owner_reassessment: Any,
) -> str:
    owner_gate = _as_mapping(owner_review_gate)
    reassessment = _as_mapping(owner_reassessment)
    if owner_gate.get("ranking_top_from_2365"):
        return str(owner_gate["ranking_top_from_2365"])
    if reassessment.get("ranking_top_from_2365"):
        return str(reassessment["ranking_top_from_2365"])
    ranked = sorted(
        (_as_mapping(row) for row in ranking_rows if _as_mapping(row).get("candidate_id")),
        key=lambda row: _int(row.get("rank"), 999),
    )
    if ranked:
        return str(ranked[0]["candidate_id"])
    return RANKING_TOP_FALLBACK


def _extract_robustness_top(
    robustness_rows: Sequence[Any],
    decision_payload: Mapping[str, Any],
    owner_review_gate: Any,
    owner_reassessment: Any,
) -> str:
    owner_gate = _as_mapping(owner_review_gate)
    reassessment = _as_mapping(owner_reassessment)
    for value in (
        owner_gate.get("robustness_top_from_2366"),
        reassessment.get("robustness_top_from_2366"),
        decision_payload.get("top_candidate_after_sensitivity"),
    ):
        if value:
            return str(value)
    ranked = sorted(
        (
            _as_mapping(row)
            for row in robustness_rows
            if _as_mapping(row).get("candidate_id")
        ),
        key=lambda row: _int(row.get("robust_rank"), 999),
    )
    if ranked:
        return str(ranked[0]["candidate_id"])
    return ROBUSTNESS_TOP_FALLBACK


def _source_ranking_row(sources: Mapping[str, Any], candidate_id: str) -> dict[str, Any]:
    for row in _as_list(sources.get("ranking_rows")):
        item = _as_mapping(row)
        if item.get("candidate_id") == candidate_id:
            return item
    return {}


def _source_robustness_row(sources: Mapping[str, Any], candidate_id: str) -> dict[str, Any]:
    for row in _as_list(sources.get("robustness_rows")):
        item = _as_mapping(row)
        if item.get("candidate_id") == candidate_id:
            return item
    return {}


def _source_review_row(sources: Mapping[str, Any], candidate_id: str) -> dict[str, Any]:
    for row in _as_list(sources.get("candidate_review_comparison")):
        item = _as_mapping(row)
        if item.get("candidate_id") == candidate_id:
            return item
    return {}


def _dynamic_gap(row: Mapping[str, Any]) -> float:
    return _float(
        _as_mapping(row.get("cost_metrics")).get("cost_adjusted_dynamic_vs_static_gap")
    )


def _ranking_score(ranking_row: Mapping[str, Any]) -> float | None:
    if not ranking_row:
        return None
    rank = _int(ranking_row.get("rank"), 999)
    return float(max(0, 10 - rank))


def _ranking_vs_robustness_gap(
    ranking_row: Mapping[str, Any],
    robustness_row: Mapping[str, Any],
) -> float | None:
    rank = _first_number(ranking_row.get("rank"))
    robust_rank = _first_number(robustness_row.get("robust_rank"))
    if rank is None or robust_rank is None:
        return None
    return round(rank - robust_rank, 6)


def _improves_current_default(
    *,
    candidate_id: str,
    matrix_rows: Sequence[Mapping[str, Any]],
    realistic_gap: float,
) -> bool:
    if candidate_id == CURRENT_DYNAMIC_DEFAULT_CANDIDATE_ID:
        return False
    for row in matrix_rows:
        if (
            row.get("candidate_id") == CURRENT_DYNAMIC_DEFAULT_CANDIDATE_ID
            and row.get("scenario_id") == "realistic"
        ):
            return realistic_gap > _dynamic_gap(row)
    return False


def _source_artifact(path: Path, payload: Any) -> dict[str, Any]:
    return {
        "path": str(path),
        "exists": path.exists(),
        "sha256": _file_sha256(path) if path.exists() else None,
        "status": _as_mapping(payload).get("status"),
    }


def _load_json_document(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )


def _as_mapping(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    return {}


def _as_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    return []


def _first_present(*values: Any) -> Any:
    for value in values:
        if value is not None:
            return value
    return None


def _first_number(*values: Any) -> float | None:
    for value in values:
        if value is None:
            continue
        try:
            return float(value)
        except (TypeError, ValueError):
            continue
    return None


def _safe_ratio(numerator: float, denominator: float) -> float | None:
    if denominator == 0.0:
        return None
    return round(numerator / denominator, 6)


def _yes_no(value: bool) -> str:
    return "YES" if value else "NO"


def _label(value: float | None, none_label: str) -> str:
    if value is None:
        return none_label
    text = f"{float(value):.2f}".rstrip("0").rstrip(".")
    return text.replace(".", "p")


def _fmt(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return str(value).lower()
    try:
        return f"{float(value):.6f}".rstrip("0").rstrip(".")
    except (TypeError, ValueError):
        return str(value)


def _side_effect_false_fields() -> tuple[str, ...]:
    return (
        "scheduler_enabled",
        "scheduler_attempted",
        "scheduled_task_created",
        "event_append_enabled",
        "event_append_attempted",
        "event_append_approved",
        "historical_event_log_mutated",
        "outcome_binding_enabled",
        "outcome_binding_attempted",
        "outcome_binding_approved",
        "outcome_store_mutated",
        "paper_shadow_enabled",
        "paper_shadow_attempted",
        "paper_shadow_approved",
        "paper_trade_created",
        "shadow_position_created",
        "production_enabled",
        "production_approved",
        "broker_action_enabled",
        "broker_action_attempted",
        "order_generated",
        "daily_report_generated",
    )
