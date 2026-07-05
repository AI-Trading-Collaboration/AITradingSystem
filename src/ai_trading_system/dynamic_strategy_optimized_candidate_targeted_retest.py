from __future__ import annotations

import json
import math
from collections.abc import Mapping, Sequence
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.data_foundation import utc_now_iso
from ai_trading_system.dynamic_strategy_candidate_optimization_divergence_review import (
    DEFAULT_DYNAMIC_STRATEGY_CANDIDATE_OPTIMIZATION_DIVERGENCE_REVIEW_OUTPUT_ROOT,
    RANKING_TOP_FALLBACK,
    ROBUSTNESS_TOP_FALLBACK,
    _candidate_target_weights,
)
from ai_trading_system.dynamic_strategy_candidate_optimization_divergence_review import (
    NEXT_ROUTE as SOURCE_2375_EXPECTED_ROUTE,
)
from ai_trading_system.dynamic_strategy_candidate_optimization_divergence_review import (
    READY_STATUS as SOURCE_2375_READY_STATUS,
)
from ai_trading_system.dynamic_strategy_cost_turnover_cooldown_sensitivity import (
    CURRENT_DYNAMIC_DEFAULT_CANDIDATE_ID,
    DEFAULT_DYNAMIC_STRATEGY_COST_TURNOVER_COOLDOWN_SENSITIVITY_OUTPUT_ROOT,
    DEFAULT_SOURCE_CADENCE_MATRIX_PATH,
    DEFAULT_SOURCE_CANDIDATE_RANKING_PATH,
    DEFAULT_SOURCE_EVENT_DRIVEN_RETEST_PATH,
    PRIMARY_EXECUTION_CADENCE,
    _scenario_policy_for_sensitivity,
)
from ai_trading_system.dynamic_strategy_cost_turnover_cooldown_sensitivity import (
    READY_STATUS as SOURCE_2366_READY_STATUS,
)
from ai_trading_system.dynamic_strategy_event_driven_retest import (
    READY_STATUS as SOURCE_2365_READY_STATUS,
)
from ai_trading_system.dynamic_strategy_execution_cadence_bias_audit import (
    ASSET_COLUMNS,
    STATIC_BASELINE_STRATEGY_ID,
    STATIC_BASELINE_WEIGHTS,
    _portfolio_return_series,
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
    _constant_weight_frame,
    _execution_materiality_thresholds,
    _file_sha256,
    _float,
    _int,
    _load_execution_price_matrix,
    _load_policy_registry,
    _mapping,
    _policies_by_id,
    _policy_cost_bps,
)
from ai_trading_system.simple_baseline_portfolio_control import (
    _data_quality_gate,
    _load_registry,
)

TASK_ID = "TRADING-2376"
TASK_REGISTER_ID = "TRADING-2376_DYNAMIC_STRATEGY_OPTIMIZED_CANDIDATE_TARGETED_RETEST"
REPORT_TYPE = "dynamic_strategy_optimized_candidate_targeted_retest"
SCHEMA_VERSION = "dynamic_strategy_optimized_candidate_targeted_retest.v1"
READY_STATUS = "DYNAMIC_STRATEGY_OPTIMIZED_CANDIDATE_TARGETED_RETEST_READY"
BLOCKED_DATA_QUALITY_STATUS = (
    "DYNAMIC_STRATEGY_OPTIMIZED_CANDIDATE_TARGETED_RETEST_BLOCKED_DATA_QUALITY"
)
BLOCKED_SOURCE_STATUS = (
    "DYNAMIC_STRATEGY_OPTIMIZED_CANDIDATE_TARGETED_RETEST_BLOCKED_SOURCE_ARTIFACT"
)
NEXT_ROUTE = (
    "TRADING-2377_Dynamic_Strategy_Targeted_Retest_Owner_Review_And_"
    "Observation_Decision"
)

PRIMARY_CANDIDATE_ID = ROBUSTNESS_TOP_FALLBACK
DECISION_ACCEPT_RESEARCH_ONLY = "ACCEPT_FOR_RESEARCH_ONLY_OBSERVATION"
DECISION_OWNER_REVIEW = "OWNER_REVIEW_REQUIRED"
DECISION_CONTINUE_OPTIMIZATION = "CONTINUE_OPTIMIZATION"
DECISION_REJECT = "REJECT_FOR_NOW"
DECISION_DEPRECATED = "DEPRECATED_BY_TARGETED_RETEST"

DEFAULT_DYNAMIC_STRATEGY_OPTIMIZED_CANDIDATE_TARGETED_RETEST_OUTPUT_ROOT = (
    PROJECT_ROOT / "outputs" / "research_strategies" / REPORT_TYPE
)
DEFAULT_DYNAMIC_STRATEGY_OPTIMIZED_CANDIDATE_TARGETED_RETEST_DOCS_ROOT = (
    PROJECT_ROOT / "docs" / "research"
)
DEFAULT_SOURCE_2375_RESULT_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_CANDIDATE_OPTIMIZATION_DIVERGENCE_REVIEW_OUTPUT_ROOT
    / "divergence_review_result.json"
)
DEFAULT_SOURCE_2375_OPTIMIZATION_MATRIX_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_CANDIDATE_OPTIMIZATION_DIVERGENCE_REVIEW_OUTPUT_ROOT
    / "optimization_matrix.json"
)
DEFAULT_SOURCE_2375_DECISION_UPDATE_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_CANDIDATE_OPTIMIZATION_DIVERGENCE_REVIEW_OUTPUT_ROOT
    / "candidate_decision_update.json"
)
DEFAULT_SOURCE_2366_RESULT_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_COST_TURNOVER_COOLDOWN_SENSITIVITY_OUTPUT_ROOT
    / "sensitivity_result.json"
)
DEFAULT_SOURCE_2366_MATRIX_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_COST_TURNOVER_COOLDOWN_SENSITIVITY_OUTPUT_ROOT
    / "sensitivity_matrix.json"
)
DEFAULT_SOURCE_2366_DECISION_UPDATE_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_COST_TURNOVER_COOLDOWN_SENSITIVITY_OUTPUT_ROOT
    / "decision_update.json"
)


def run_dynamic_strategy_optimized_candidate_targeted_retest(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    simple_config_path: Path = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    policy_registry_path: Path = DEFAULT_EXECUTION_POLICY_REGISTRY_PATH,
    source_event_retest_path: Path = DEFAULT_SOURCE_EVENT_DRIVEN_RETEST_PATH,
    source_candidate_ranking_path: Path = DEFAULT_SOURCE_CANDIDATE_RANKING_PATH,
    source_cadence_matrix_path: Path = DEFAULT_SOURCE_CADENCE_MATRIX_PATH,
    source_sensitivity_result_path: Path = DEFAULT_SOURCE_2366_RESULT_PATH,
    source_sensitivity_matrix_path: Path = DEFAULT_SOURCE_2366_MATRIX_PATH,
    source_sensitivity_decision_update_path: Path = (
        DEFAULT_SOURCE_2366_DECISION_UPDATE_PATH
    ),
    source_optimization_review_path: Path = DEFAULT_SOURCE_2375_RESULT_PATH,
    source_optimization_matrix_path: Path = DEFAULT_SOURCE_2375_OPTIMIZATION_MATRIX_PATH,
    source_optimization_decision_update_path: Path = (
        DEFAULT_SOURCE_2375_DECISION_UPDATE_PATH
    ),
    output_root: Path = (
        DEFAULT_DYNAMIC_STRATEGY_OPTIMIZED_CANDIDATE_TARGETED_RETEST_OUTPUT_ROOT
    ),
    docs_root: Path = (
        DEFAULT_DYNAMIC_STRATEGY_OPTIMIZED_CANDIDATE_TARGETED_RETEST_DOCS_ROOT
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
        source_sensitivity_decision_update_path=(
            source_sensitivity_decision_update_path
        ),
        source_optimization_review_path=source_optimization_review_path,
        source_optimization_matrix_path=source_optimization_matrix_path,
        source_optimization_decision_update_path=(
            source_optimization_decision_update_path
        ),
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
    retest_policy = _targeted_retest_policy()
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
        retest_policy=retest_policy,
    )
    if not bool(data_quality.get("passed")):
        payload["status"] = BLOCKED_DATA_QUALITY_STATUS
        payload.update(_blocked_sections("data_quality_gate_failed", sources))
        _write_outputs(payload=payload, output_root=output_root, docs_root=docs_root)
        return payload
    if not bool(sources["source_ready_for_targeted_retest"]):
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
    policies = _policies_by_id(policy_registry)
    thresholds = _execution_materiality_thresholds(policy_registry)
    primary_candidate = str(sources["primary_candidate"])
    ranking_top = str(sources["ranking_top_from_2365"])
    robustness_top = str(sources["robustness_top_from_2366"])
    comparison_candidates = _comparison_candidate_ids(
        primary_candidate=primary_candidate,
        ranking_top=ranking_top,
        robustness_top=robustness_top,
    )
    full_retest = _run_targeted_retest(
        prices=prices,
        policies=policies,
        thresholds=thresholds,
        retest_policy=retest_policy,
        primary_candidate=primary_candidate,
        ranking_top=ranking_top,
        robustness_top=robustness_top,
        comparison_candidates=comparison_candidates,
    )
    decision_update = _candidate_decision_update(
        primary_candidate=primary_candidate,
        ranking_top=ranking_top,
        sources=sources,
        full_retest=full_retest,
    )
    summary = _summary_findings(
        primary_candidate=primary_candidate,
        ranking_top=ranking_top,
        decision_update=decision_update,
        full_retest=full_retest,
    )
    payload.update(
        {
            "primary_candidate": primary_candidate,
            "decision_from_2375": sources["decision_from_2375"],
            "ranking_top_from_2365": ranking_top,
            "robustness_top_from_2366": robustness_top,
            "comparison_candidates": comparison_candidates,
            "targeted_retest_design": _targeted_retest_design(retest_policy),
            "targeted_retest_result": full_retest["targeted_retest_result"],
            "time_slice_retest_result": full_retest["time_slice_retest_result"],
            "regime_slice_retest_result": full_retest["regime_slice_retest_result"],
            "cost_stress_result": full_retest["cost_stress_result"],
            "execution_constraint_stress_result": full_retest[
                "execution_constraint_stress_result"
            ],
            "ablation_test_result": full_retest["ablation_test_result"],
            "candidate_decision_after_targeted_retest": decision_update[
                "candidate_decision_after_targeted_retest"
            ],
            "decision_update": decision_update,
            "summary_findings": summary,
            "required_outputs_ready": _required_outputs_ready(True),
            "targeted_retest_ready": True,
            "time_slice_retest_ready": True,
            "regime_slice_retest_ready": True,
            "cost_stress_retest_ready": True,
            "execution_constraint_stress_ready": True,
            "ablation_tests_ready": True,
            "candidate_decision_update_ready": True,
            "backtest_run": True,
            "recommended_next_research_task": NEXT_ROUTE,
            "research_quality_status": (
                "TARGETED_RETEST_READY_REQUIRES_2377_OWNER_REVIEW_DECISION"
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
    retest_policy: Mapping[str, Any],
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
        "source_tasks": ["TRADING-2365", "TRADING-2366", "TRADING-2375"],
        "source_artifacts": sources["source_artifacts"],
        "source_status": sources["source_status"],
        "source_ready_for_targeted_retest": sources[
            "source_ready_for_targeted_retest"
        ],
        "source_validation_errors": sources["source_validation_errors"],
        "primary_execution_cadence": PRIMARY_EXECUTION_CADENCE,
        "comparison_cadences": [
            PRIMARY_EXECUTION_CADENCE,
            "cooldown_limited_event_driven",
            "signal_event_driven",
        ],
        "monthly_rebalance": {
            "allowed_for_reference": True,
            "allowed_for_primary_decision": False,
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
        "targeted_retest_policy": dict(retest_policy),
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
    return {
        "primary_candidate": sources.get("primary_candidate") or PRIMARY_CANDIDATE_ID,
        "decision_from_2375": sources.get("decision_from_2375"),
        "ranking_top_from_2365": sources.get("ranking_top_from_2365")
        or RANKING_TOP_FALLBACK,
        "robustness_top_from_2366": sources.get("robustness_top_from_2366")
        or ROBUSTNESS_TOP_FALLBACK,
        "comparison_candidates": [],
        "targeted_retest_design": _targeted_retest_design(_targeted_retest_policy()),
        "targeted_retest_result": [],
        "time_slice_retest_result": [],
        "regime_slice_retest_result": [],
        "cost_stress_result": [],
        "execution_constraint_stress_result": [],
        "ablation_test_result": [],
        "candidate_decision_after_targeted_retest": DECISION_OWNER_REVIEW,
        "decision_update": {
            "schema_version": "dynamic_strategy_targeted_retest_decision_update.v1",
            "decision_update_ready": False,
            "blocked_reason": reason,
            "recommended_next_research_task": NEXT_ROUTE,
        },
        "summary_findings": {
            "blocked_reason": reason,
            "candidate_ready_for_research_only_observation": "NO_BLOCKED",
            "paper_shadow_remains_disabled": True,
        },
        "required_outputs_ready": _required_outputs_ready(False),
        "targeted_retest_ready": False,
        "time_slice_retest_ready": False,
        "regime_slice_retest_ready": False,
        "cost_stress_retest_ready": False,
        "execution_constraint_stress_ready": False,
        "ablation_tests_ready": False,
        "candidate_decision_update_ready": False,
        "backtest_run": False,
        "recommended_next_research_task": NEXT_ROUTE,
        "research_quality_status": "BLOCKED_FAIL_CLOSED",
    }


def _load_sources(
    *,
    source_event_retest_path: Path,
    source_candidate_ranking_path: Path,
    source_cadence_matrix_path: Path,
    source_sensitivity_result_path: Path,
    source_sensitivity_matrix_path: Path,
    source_sensitivity_decision_update_path: Path,
    source_optimization_review_path: Path,
    source_optimization_matrix_path: Path,
    source_optimization_decision_update_path: Path,
) -> dict[str, Any]:
    event_retest = _load_json_document(source_event_retest_path)
    candidate_ranking = _load_json_document(source_candidate_ranking_path)
    cadence_matrix = _load_json_document(source_cadence_matrix_path)
    sensitivity_result = _load_json_document(source_sensitivity_result_path)
    sensitivity_matrix = _load_json_document(source_sensitivity_matrix_path)
    sensitivity_decision_update = _load_json_document(
        source_sensitivity_decision_update_path
    )
    optimization_review = _load_json_document(source_optimization_review_path)
    optimization_matrix = _load_json_document(source_optimization_matrix_path)
    optimization_decision_update = _load_json_document(
        source_optimization_decision_update_path
    )
    optimization_decision = _as_mapping(
        _first_present(
            _as_mapping(optimization_review).get("candidate_decision_update"),
            _as_mapping(optimization_decision_update).get("candidate_decision_update"),
        )
    )
    ranking_rows = _as_list(
        _first_present(
            _as_mapping(candidate_ranking).get("candidate_ranking"),
            _as_mapping(event_retest).get("candidate_ranking"),
        )
    )
    robustness_rows = _as_list(
        _first_present(
            _as_mapping(sensitivity_result).get("robustness_ranking"),
            _as_mapping(
                _first_present(
                    _as_mapping(sensitivity_decision_update).get("decision_update"),
                    _as_mapping(sensitivity_result).get("decision_update"),
                )
            ).get("robustness_ranking"),
        )
    )
    primary_candidate = str(
        _first_present(
            _as_mapping(optimization_review).get("best_candidate_after_optimization"),
            optimization_decision.get("best_candidate_after_optimization"),
            PRIMARY_CANDIDATE_ID,
        )
    )
    decision_from_2375 = str(
        _first_present(
            _as_mapping(optimization_review).get(
                "recommended_decision_after_optimization"
            ),
            optimization_decision.get("recommended_decision_after_optimization"),
            DECISION_OWNER_REVIEW,
        )
    )
    ranking_top = str(
        _first_present(
            _as_mapping(optimization_review).get("ranking_top_from_2365"),
            optimization_decision.get("ranking_top_from_2365"),
            _extract_ranked_candidate(ranking_rows, rank_field="rank"),
            RANKING_TOP_FALLBACK,
        )
    )
    robustness_top = str(
        _first_present(
            _as_mapping(optimization_review).get("robustness_top_from_2366"),
            optimization_decision.get("robustness_top_from_2366"),
            _extract_ranked_candidate(robustness_rows, rank_field="robust_rank"),
            ROBUSTNESS_TOP_FALLBACK,
        )
    )
    source_status = {
        "event_retest": _as_mapping(event_retest).get("status"),
        "candidate_ranking": _as_mapping(candidate_ranking).get("status"),
        "cadence_matrix": _as_mapping(cadence_matrix).get("status"),
        "sensitivity_result": _as_mapping(sensitivity_result).get("status"),
        "sensitivity_matrix": _as_mapping(sensitivity_matrix).get("status"),
        "sensitivity_decision_update": _as_mapping(
            sensitivity_decision_update
        ).get("status"),
        "optimization_review": _as_mapping(optimization_review).get("status"),
        "optimization_matrix": _as_mapping(optimization_matrix).get("status"),
        "optimization_decision_update": _as_mapping(
            optimization_decision_update
        ).get("status"),
    }
    validation_errors = _source_validation_errors(
        source_status=source_status,
        event_retest=event_retest,
        sensitivity_result=sensitivity_result,
        optimization_review=optimization_review,
        optimization_matrix=optimization_matrix,
        optimization_decision_update=optimization_decision_update,
        primary_candidate=primary_candidate,
        decision_from_2375=decision_from_2375,
        ranking_top=ranking_top,
        robustness_top=robustness_top,
        ranking_rows=ranking_rows,
        robustness_rows=robustness_rows,
    )
    return {
        "event_retest": event_retest,
        "candidate_ranking": candidate_ranking,
        "cadence_matrix": cadence_matrix,
        "sensitivity_result": sensitivity_result,
        "sensitivity_matrix": sensitivity_matrix,
        "sensitivity_decision_update": sensitivity_decision_update,
        "optimization_review": optimization_review,
        "optimization_matrix": optimization_matrix,
        "optimization_decision_update": optimization_decision_update,
        "ranking_rows": ranking_rows,
        "robustness_rows": robustness_rows,
        "primary_candidate": primary_candidate,
        "decision_from_2375": decision_from_2375,
        "ranking_top_from_2365": ranking_top,
        "robustness_top_from_2366": robustness_top,
        "source_status": source_status,
        "source_validation_errors": validation_errors,
        "source_ready_for_targeted_retest": not validation_errors,
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
            "sensitivity_decision_update": _source_artifact(
                source_sensitivity_decision_update_path,
                sensitivity_decision_update,
            ),
            "optimization_review": _source_artifact(
                source_optimization_review_path,
                optimization_review,
            ),
            "optimization_matrix": _source_artifact(
                source_optimization_matrix_path,
                optimization_matrix,
            ),
            "optimization_decision_update": _source_artifact(
                source_optimization_decision_update_path,
                optimization_decision_update,
            ),
        },
    }


def _source_validation_errors(
    *,
    source_status: Mapping[str, Any],
    event_retest: Any,
    sensitivity_result: Any,
    optimization_review: Any,
    optimization_matrix: Any,
    optimization_decision_update: Any,
    primary_candidate: str,
    decision_from_2375: str,
    ranking_top: str,
    robustness_top: str,
    ranking_rows: Sequence[Any],
    robustness_rows: Sequence[Any],
) -> list[str]:
    errors: list[str] = []
    for key in ("event_retest", "candidate_ranking", "cadence_matrix"):
        if source_status.get(key) != SOURCE_2365_READY_STATUS:
            errors.append(f"{key}_status_not_ready")
    for key in ("sensitivity_result", "sensitivity_matrix", "sensitivity_decision_update"):
        if source_status.get(key) != SOURCE_2366_READY_STATUS:
            errors.append(f"{key}_status_not_ready")
    for key in ("optimization_review", "optimization_matrix", "optimization_decision_update"):
        if source_status.get(key) != SOURCE_2375_READY_STATUS:
            errors.append(f"{key}_status_not_ready")
    if _as_mapping(event_retest).get("primary_execution_cadence") != PRIMARY_EXECUTION_CADENCE:
        errors.append("event_retest_primary_execution_cadence_not_valid_until_window")
    if (
        _as_mapping(sensitivity_result).get("primary_execution_cadence")
        != PRIMARY_EXECUTION_CADENCE
    ):
        errors.append("sensitivity_primary_execution_cadence_not_valid_until_window")
    if (
        _as_mapping(optimization_review).get("primary_execution_cadence")
        != PRIMARY_EXECUTION_CADENCE
    ):
        errors.append("optimization_primary_execution_cadence_not_valid_until_window")
    if (
        _as_mapping(optimization_review).get("recommended_next_research_task")
        != SOURCE_2375_EXPECTED_ROUTE
    ):
        errors.append("optimization_review_route_not_trading_2376")
    if primary_candidate != PRIMARY_CANDIDATE_ID:
        errors.append("primary_candidate_not_expected_optimized_candidate")
    if decision_from_2375 != DECISION_OWNER_REVIEW:
        errors.append("decision_from_2375_not_owner_review_required")
    if ranking_top == "":
        errors.append("ranking_top_missing")
    if robustness_top == "":
        errors.append("robustness_top_missing")
    if not ranking_rows:
        errors.append("candidate_ranking_empty")
    if not robustness_rows:
        errors.append("robustness_ranking_empty")
    if not _as_list(_as_mapping(optimization_matrix).get("optimization_matrix")):
        errors.append("optimization_matrix_empty")
    if not _as_mapping(optimization_decision_update).get("candidate_decision_update"):
        errors.append("optimization_decision_update_missing")
    for field in _side_effect_false_fields():
        for label, source in (
            ("event_retest", event_retest),
            ("sensitivity_result", sensitivity_result),
            ("optimization_review", optimization_review),
        ):
            value = _as_mapping(source).get(field)
            if value is True:
                errors.append(f"{label}_{field}_true")
    for label, source in (
        ("event_retest", event_retest),
        ("sensitivity_result", sensitivity_result),
        ("optimization_review", optimization_review),
    ):
        value = _as_mapping(source).get("broker_action")
        if value not in (None, "none"):
            errors.append(f"{label}_broker_action_not_none")
    return errors


def _targeted_retest_policy() -> dict[str, Any]:
    return {
        "policy_id": "dynamic_strategy_optimized_candidate_targeted_retest_v1",
        "status": "pilot_research_only_targeted_retest",
        "owner": "research_governance",
        "version": "v1",
        "rationale": (
            "TRADING-2376 must test whether the 2375 optimized candidate remains "
            "credible across slices, cost pressure, execution constraints and "
            "diagnostic ablations before owner observation review."
        ),
        "intended_effect": (
            "Separate a broad robust candidate from an artifact that depends on "
            "one time slice, one regime or one unexamined guardrail."
        ),
        "validation_evidence": (
            "Source artifacts from TRADING-2365/2366/2375, current cached-data "
            "quality gate, focused tests and real CLI run."
        ),
        "review_condition": (
            "TRADING-2377 owner review must decide any research-only observation "
            "step; no scheduler, paper-shadow, production or broker action is "
            "authorized by this pilot policy."
        ),
        "slice_policy": {
            "time_split": "full_sample_plus_three_equal_row_count_periods",
            "post_2023_ai_cycle_start": "2024-01-01",
            "rolling_volatility_window_sessions": 20,
            "trend_window_sessions": 60,
            "high_volatility_quantile": 0.75,
            "drawdown_threshold": -0.08,
            "risk_off_drawdown_threshold": -0.10,
        },
        "decision_policy": {
            "accept_requires": [
                "positive realistic and conservative cost-adjusted gap",
                "majority time-slice pass rate",
                "majority regime-slice pass rate",
                "drawdown not materially worse than static baseline",
                "guardrail ablation contribution is meaningful",
                "no unresolved ranking-vs-robustness conflict",
            ],
            "owner_review_when": (
                "The candidate is mostly robust but still has a meaningful "
                "turnover, ranking-top conflict or slice/regime tradeoff."
            ),
        },
    }


def _targeted_retest_design(policy: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "primary_candidate": PRIMARY_CANDIDATE_ID,
        "primary_execution_cadence": PRIMARY_EXECUTION_CADENCE,
        "comparison_cadences": [
            PRIMARY_EXECUTION_CADENCE,
            "cooldown_limited_event_driven",
            "signal_event_driven",
        ],
        "monthly_rebalance": {
            "allowed_for_reference": True,
            "allowed_for_primary_decision": False,
        },
        "time_slices": [
            "full_available_window",
            "early_period",
            "middle_period",
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
            "trend_uncertain",
            "drawdown",
            "recovery",
        ],
        "cost_stress_scenarios": _cost_stress_scenarios(),
        "execution_constraint_axes": _execution_constraint_axes(),
        "ablation_tests": _ablation_definitions(),
        "policy_id": policy.get("policy_id"),
    }


def _run_targeted_retest(
    *,
    prices: pd.DataFrame,
    policies: Mapping[str, Mapping[str, Any]],
    thresholds: Mapping[str, float],
    retest_policy: Mapping[str, Any],
    primary_candidate: str,
    ranking_top: str,
    robustness_top: str,
    comparison_candidates: Sequence[str],
) -> dict[str, Any]:
    static_bundle = _static_bundle(prices)
    base_scenario = _cost_stress_scenarios()[1]
    base_bundles = {
        candidate_id: _candidate_bundle(
            prices=prices,
            policies=policies,
            candidate_id=candidate_id,
            ranking_top=ranking_top,
            robustness_top=robustness_top,
            cadence=PRIMARY_EXECUTION_CADENCE,
            scenario=base_scenario,
            ablation_id=None,
        )
        for candidate_id in comparison_candidates
        if candidate_id != "static_baseline"
    }
    primary_bundle = base_bundles[primary_candidate]
    ranking_bundle = base_bundles.get(ranking_top)
    time_rows = _slice_rows(
        prices=prices,
        slice_group="time_slice",
        slice_definitions=_time_slice_definitions(prices, retest_policy),
        bundles=base_bundles,
        static_bundle=static_bundle,
        ranking_bundle=ranking_bundle,
        primary_candidate=primary_candidate,
    )
    regime_rows = _slice_rows(
        prices=prices,
        slice_group="regime_slice",
        slice_definitions=_regime_slice_definitions(prices, retest_policy),
        bundles=base_bundles,
        static_bundle=static_bundle,
        ranking_bundle=ranking_bundle,
        primary_candidate=primary_candidate,
    )
    cost_rows = _cost_stress_rows(
        prices=prices,
        policies=policies,
        comparison_candidates=comparison_candidates,
        static_bundle=static_bundle,
        ranking_top=ranking_top,
        robustness_top=robustness_top,
        primary_candidate=primary_candidate,
    )
    execution_rows = _execution_constraint_rows(
        prices=prices,
        policies=policies,
        static_bundle=static_bundle,
        ranking_bundle=ranking_bundle,
        primary_candidate=primary_candidate,
        ranking_top=ranking_top,
        robustness_top=robustness_top,
    )
    ablation_rows = _ablation_rows(
        prices=prices,
        policies=policies,
        static_bundle=static_bundle,
        primary_bundle=primary_bundle,
        ranking_bundle=ranking_bundle,
        primary_candidate=primary_candidate,
        ranking_top=ranking_top,
        robustness_top=robustness_top,
    )
    targeted_rows = [
        _full_sample_result_row(
            candidate_id=primary_candidate,
            role="primary_candidate",
            bundle=primary_bundle,
            static_bundle=static_bundle,
            ranking_bundle=ranking_bundle,
        )
    ]
    if ranking_bundle is not None and ranking_top != primary_candidate:
        targeted_rows.append(
            _full_sample_result_row(
                candidate_id=ranking_top,
                role="ranking_top_from_2365",
                bundle=ranking_bundle,
                static_bundle=static_bundle,
                ranking_bundle=ranking_bundle,
            )
        )
    return {
        "targeted_retest_result": targeted_rows,
        "time_slice_retest_result": time_rows,
        "regime_slice_retest_result": regime_rows,
        "cost_stress_result": cost_rows,
        "execution_constraint_stress_result": execution_rows,
        "ablation_test_result": ablation_rows,
    }


def _candidate_bundle(
    *,
    prices: pd.DataFrame,
    policies: Mapping[str, Mapping[str, Any]],
    candidate_id: str,
    ranking_top: str,
    robustness_top: str,
    cadence: str,
    scenario: Mapping[str, Any],
    ablation_id: str | None,
) -> dict[str, Any]:
    target_weights = _candidate_target_weights(
        candidate_id=candidate_id,
        ranking_top=ranking_top,
        robustness_top=robustness_top,
        prices=prices,
    )
    if ablation_id:
        target_weights = _ablation_target_weights(
            ablation_id=ablation_id,
            primary_weights=target_weights,
            ranking_weights=_candidate_target_weights(
                candidate_id=ranking_top,
                ranking_top=ranking_top,
                robustness_top=robustness_top,
                prices=prices,
            ),
            prices=prices,
        )
    policy = _scenario_policy_for_sensitivity(
        cadence=cadence,
        scenario=scenario,
        policies=policies,
    )
    actual_weights, path_rows = _actual_position_path(
        strategy_id=candidate_id if ablation_id is None else f"{candidate_id}:{ablation_id}",
        execution_policy_id=str(policy["execution_policy_id"]),
        target_weights=target_weights,
        policy=policy,
        signal_validity_profile=_signal_validity_profile_for_retest(cadence, policy),
        enable_staleness_filter=cadence == PRIMARY_EXECUTION_CADENCE,
        stale_action="suppress_rebalance" if cadence == PRIMARY_EXECUTION_CADENCE else None,
    )
    cost_bps = _policy_cost_bps(policy)
    _attach_path_return_columns(
        prices=prices,
        target_weights=target_weights,
        actual_weights=actual_weights,
        path_rows=path_rows,
        cost_bps=cost_bps,
    )
    returns = _portfolio_return_series(prices, actual_weights, cost_bps=cost_bps)
    gross_returns = _portfolio_return_series(prices, actual_weights, cost_bps=0.0)
    return {
        "candidate_id": candidate_id,
        "ablation_id": ablation_id,
        "cadence": cadence,
        "scenario": dict(scenario),
        "policy": policy,
        "target_weights": target_weights,
        "actual_weights": actual_weights,
        "path_rows": path_rows,
        "returns": returns,
        "gross_returns": gross_returns,
        "benchmark_returns": _benchmark_returns(prices),
        "cost_bps": cost_bps,
    }


def _static_bundle(prices: pd.DataFrame) -> dict[str, Any]:
    weights = _constant_weight_frame(prices, STATIC_BASELINE_WEIGHTS)
    returns = _portfolio_return_series(prices, weights, cost_bps=0.0)
    return {
        "candidate_id": "static_baseline",
        "source_strategy_id": STATIC_BASELINE_STRATEGY_ID,
        "cadence": "static_baseline",
        "actual_weights": weights,
        "target_weights": weights,
        "path_rows": [],
        "returns": returns,
        "gross_returns": returns,
        "benchmark_returns": _benchmark_returns(prices),
        "cost_bps": 0.0,
    }


def _cost_stress_rows(
    *,
    prices: pd.DataFrame,
    policies: Mapping[str, Mapping[str, Any]],
    comparison_candidates: Sequence[str],
    static_bundle: Mapping[str, Any],
    ranking_top: str,
    robustness_top: str,
    primary_candidate: str,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    ranking_bundle_by_scenario: dict[str, Mapping[str, Any]] = {}
    for scenario in _cost_stress_scenarios():
        if ranking_top != "static_baseline":
            ranking_bundle_by_scenario[str(scenario["scenario_id"])] = _candidate_bundle(
                prices=prices,
                policies=policies,
                candidate_id=ranking_top,
                ranking_top=ranking_top,
                robustness_top=robustness_top,
                cadence=PRIMARY_EXECUTION_CADENCE,
                scenario=scenario,
                ablation_id=None,
            )
    for scenario in _cost_stress_scenarios():
        scenario_id = str(scenario["scenario_id"])
        rows.append(
            _stress_result_row(
                candidate_id="static_baseline",
                role="static_baseline",
                scenario=scenario,
                bundle=static_bundle,
                static_bundle=static_bundle,
                ranking_bundle=ranking_bundle_by_scenario.get(scenario_id),
                primary_candidate=primary_candidate,
            )
        )
        for candidate_id in comparison_candidates:
            if candidate_id == "static_baseline":
                continue
            bundle = (
                ranking_bundle_by_scenario[scenario_id]
                if candidate_id == ranking_top
                else _candidate_bundle(
                    prices=prices,
                    policies=policies,
                    candidate_id=candidate_id,
                    ranking_top=ranking_top,
                    robustness_top=robustness_top,
                    cadence=PRIMARY_EXECUTION_CADENCE,
                    scenario=scenario,
                    ablation_id=None,
                )
            )
            rows.append(
                _stress_result_row(
                    candidate_id=candidate_id,
                    role=_candidate_role(
                        candidate_id,
                        primary_candidate=primary_candidate,
                        ranking_top=ranking_top,
                    ),
                    scenario=scenario,
                    bundle=bundle,
                    static_bundle=static_bundle,
                    ranking_bundle=ranking_bundle_by_scenario.get(scenario_id),
                    primary_candidate=primary_candidate,
                )
            )
    return rows


def _execution_constraint_rows(
    *,
    prices: pd.DataFrame,
    policies: Mapping[str, Mapping[str, Any]],
    static_bundle: Mapping[str, Any],
    ranking_bundle: Mapping[str, Any] | None,
    primary_candidate: str,
    ranking_top: str,
    robustness_top: str,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for scenario in _execution_constraint_scenarios():
        bundle = _candidate_bundle(
            prices=prices,
            policies=policies,
            candidate_id=primary_candidate,
            ranking_top=ranking_top,
            robustness_top=robustness_top,
            cadence=PRIMARY_EXECUTION_CADENCE,
            scenario=scenario,
            ablation_id=None,
        )
        row = _stress_result_row(
            candidate_id=primary_candidate,
            role="primary_candidate_execution_constraint_stress",
            scenario=scenario,
            bundle=bundle,
            static_bundle=static_bundle,
            ranking_bundle=ranking_bundle,
            primary_candidate=primary_candidate,
        )
        execution = _as_mapping(row.get("execution_metrics"))
        max_cap = scenario.get("max_turnover_per_month")
        row["constraint_axis"] = scenario["scenario_group"]
        row["constraint_value"] = scenario["constraint_value"]
        row["max_turnover_per_month_enforcement_mode"] = "diagnostic_limit_check"
        row["observed_max_monthly_turnover"] = execution.get("max_monthly_turnover")
        row["constraint_passed"] = (
            max_cap is None or _float(execution.get("max_monthly_turnover")) <= _float(max_cap)
        ) and row["stress_passed"]
        rows.append(row)
    return rows


def _ablation_rows(
    *,
    prices: pd.DataFrame,
    policies: Mapping[str, Mapping[str, Any]],
    static_bundle: Mapping[str, Any],
    primary_bundle: Mapping[str, Any],
    ranking_bundle: Mapping[str, Any] | None,
    primary_candidate: str,
    ranking_top: str,
    robustness_top: str,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    primary_row = _full_sample_result_row(
        candidate_id=primary_candidate,
        role="primary_candidate",
        bundle=primary_bundle,
        static_bundle=static_bundle,
        ranking_bundle=ranking_bundle,
    )
    for definition in _ablation_definitions():
        scenario = _ablation_scenario(definition["ablation_id"])
        cadence = (
            "signal_event_driven"
            if definition["ablation_id"] == "no_valid_until_window"
            else PRIMARY_EXECUTION_CADENCE
        )
        bundle = _candidate_bundle(
            prices=prices,
            policies=policies,
            candidate_id=primary_candidate,
            ranking_top=ranking_top,
            robustness_top=robustness_top,
            cadence=cadence,
            scenario=scenario,
            ablation_id=str(definition["ablation_id"]),
        )
        row = _stress_result_row(
            candidate_id=primary_candidate,
            role="primary_candidate_ablation",
            scenario=scenario,
            bundle=bundle,
            static_bundle=static_bundle,
            ranking_bundle=ranking_bundle,
            primary_candidate=primary_candidate,
        )
        row["ablation_id"] = definition["ablation_id"]
        row["purpose"] = definition["purpose"]
        row["ablation_expected_component"] = definition["component"]
        row["ablation_dependency_score"] = _ablation_dependency_score(
            primary_row,
            row,
        )
        row["performance_degraded_vs_primary"] = _float(
            _as_mapping(primary_row.get("performance_metrics")).get("annualized_return")
        ) > _float(_as_mapping(row.get("performance_metrics")).get("annualized_return"))
        row["drawdown_degraded_vs_primary"] = abs(
            _float(_as_mapping(row.get("performance_metrics")).get("max_drawdown"))
        ) > abs(_float(_as_mapping(primary_row.get("performance_metrics")).get("max_drawdown")))
        row["turnover_degraded_vs_primary"] = _float(
            _as_mapping(row.get("execution_metrics")).get("turnover")
        ) > _float(_as_mapping(primary_row.get("execution_metrics")).get("turnover"))
        row["ablation_supports_guardrail"] = row["ablation_dependency_score"] > 0.0
        rows.append(row)
    return rows


def _slice_rows(
    *,
    prices: pd.DataFrame,
    slice_group: str,
    slice_definitions: Sequence[Mapping[str, Any]],
    bundles: Mapping[str, Mapping[str, Any]],
    static_bundle: Mapping[str, Any],
    ranking_bundle: Mapping[str, Any] | None,
    primary_candidate: str,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    full_metrics_by_candidate = {
        candidate_id: _metrics_for_returns(
            _series(bundle["returns"]),
            benchmark_returns=_benchmark_returns(prices),
        )
        for candidate_id, bundle in bundles.items()
    }
    for definition in slice_definitions:
        mask = _series(definition["mask"]).astype(bool)
        for candidate_id, bundle in bundles.items():
            static_metrics = _metrics_for_returns(
                _series(static_bundle["returns"]).loc[mask],
                benchmark_returns=_benchmark_returns(prices).loc[mask],
            )
            ranking_metrics = (
                _metrics_for_returns(
                    _series(ranking_bundle["returns"]).loc[mask],
                    benchmark_returns=_benchmark_returns(prices).loc[mask],
                )
                if ranking_bundle is not None
                else None
            )
            metrics = _metrics_for_returns(
                _series(bundle["returns"]).loc[mask],
                benchmark_returns=_benchmark_returns(prices).loc[mask],
            )
            execution = _execution_metrics_for_path(bundle["path_rows"], mask.index[mask])
            full_metrics = full_metrics_by_candidate[candidate_id]
            row = _result_row(
                candidate_id=candidate_id,
                role=_candidate_role(
                    candidate_id,
                    primary_candidate=primary_candidate,
                    ranking_top=str(ranking_bundle["candidate_id"])
                    if ranking_bundle is not None
                    else "",
                ),
                result_group=slice_group,
                scenario_id=str(definition["slice_id"]),
                scenario_group=slice_group,
                execution_cadence=str(bundle["cadence"]),
                scenario=_as_mapping(bundle.get("scenario")),
                performance=metrics,
                static_performance=static_metrics,
                ranking_performance=ranking_metrics,
                execution=execution,
                full_sample_performance=full_metrics,
                primary_candidate=primary_candidate,
            )
            row["slice_purpose"] = definition.get("purpose")
            row["slice_observation_count"] = int(mask.sum())
            row["slice_date_range"] = _date_range_for_mask(mask)
            rows.append(row)
    return rows


def _full_sample_result_row(
    *,
    candidate_id: str,
    role: str,
    bundle: Mapping[str, Any],
    static_bundle: Mapping[str, Any],
    ranking_bundle: Mapping[str, Any] | None,
) -> dict[str, Any]:
    returns = _series(bundle["returns"])
    static_returns = _series(static_bundle["returns"]).reindex(returns.index).fillna(0.0)
    benchmark = _series(bundle.get("benchmark_returns")).reindex(returns.index).fillna(0.0)
    ranking_returns = (
        _series(ranking_bundle["returns"]).reindex(returns.index).fillna(0.0)
        if ranking_bundle is not None
        else None
    )
    performance = _metrics_for_returns(
        returns,
        benchmark_returns=benchmark,
    )
    static_performance = _metrics_for_returns(
        static_returns,
        benchmark_returns=benchmark,
    )
    ranking_performance = (
        _metrics_for_returns(
            ranking_returns,
            benchmark_returns=benchmark,
        )
        if ranking_returns is not None
        else None
    )
    return _result_row(
        candidate_id=candidate_id,
        role=role,
        result_group="targeted_retest",
        scenario_id="full_sample_realistic_cost",
        scenario_group="targeted_retest",
        execution_cadence=str(bundle["cadence"]),
        scenario=_as_mapping(bundle.get("scenario")),
        performance=performance,
        static_performance=static_performance,
        ranking_performance=ranking_performance,
        execution=_execution_metrics_for_path(bundle["path_rows"], returns.index),
        full_sample_performance=performance,
        primary_candidate=str(bundle["candidate_id"]),
    )


def _stress_result_row(
    *,
    candidate_id: str,
    role: str,
    scenario: Mapping[str, Any],
    bundle: Mapping[str, Any],
    static_bundle: Mapping[str, Any],
    ranking_bundle: Mapping[str, Any] | None,
    primary_candidate: str,
) -> dict[str, Any]:
    returns = _series(bundle["returns"])
    benchmark = _series(bundle.get("benchmark_returns")).reindex(returns.index).fillna(0.0)
    performance = _metrics_for_returns(returns, benchmark_returns=benchmark)
    static_performance = _metrics_for_returns(
        _series(static_bundle["returns"]).reindex(returns.index).fillna(0.0),
        benchmark_returns=benchmark,
    )
    ranking_performance = (
        _metrics_for_returns(
            _series(ranking_bundle["returns"]).reindex(returns.index).fillna(0.0),
            benchmark_returns=benchmark,
        )
        if ranking_bundle is not None
        else None
    )
    row = _result_row(
        candidate_id=candidate_id,
        role=role,
        result_group=str(scenario.get("scenario_group")),
        scenario_id=str(scenario.get("scenario_id")),
        scenario_group=str(scenario.get("scenario_group")),
        execution_cadence=str(bundle["cadence"]),
        scenario=scenario,
        performance=performance,
        static_performance=static_performance,
        ranking_performance=ranking_performance,
        execution=_execution_metrics_for_path(bundle["path_rows"], returns.index),
        full_sample_performance=performance,
        primary_candidate=primary_candidate,
    )
    row["transaction_cost_bps"] = scenario.get("transaction_cost_bps")
    row["slippage_bps"] = scenario.get("slippage_bps")
    row["cooldown_days"] = scenario.get("cooldown_days")
    row["min_holding_days"] = scenario.get("min_holding_days")
    row["max_turnover_per_month"] = scenario.get("max_turnover_per_month")
    row["max_turnover_per_month_label"] = scenario.get(
        "max_turnover_per_month_label"
    )
    row["max_single_step_weight_delta"] = scenario.get("max_single_step_weight_delta")
    row["max_single_step_weight_delta_label"] = scenario.get(
        "max_single_step_weight_delta_label"
    )
    return row


def _result_row(
    *,
    candidate_id: str,
    role: str,
    result_group: str,
    scenario_id: str,
    scenario_group: str,
    execution_cadence: str,
    scenario: Mapping[str, Any],
    performance: Mapping[str, Any],
    static_performance: Mapping[str, Any],
    ranking_performance: Mapping[str, Any] | None,
    execution: Mapping[str, Any],
    full_sample_performance: Mapping[str, Any],
    primary_candidate: str,
) -> dict[str, Any]:
    annual = _float(performance.get("annualized_return"))
    static_annual = _float(static_performance.get("annualized_return"))
    ranking_annual = _float(_mapping(ranking_performance).get("annualized_return"))
    full_annual = _float(full_sample_performance.get("annualized_return"))
    static_drawdown = _float(static_performance.get("max_drawdown"))
    max_drawdown = _float(performance.get("max_drawdown"))
    dynamic_gap = round(annual - static_annual, 6)
    ranking_gap = round(annual - ranking_annual, 6) if ranking_performance else None
    drawdown_not_worse = abs(max_drawdown) <= abs(static_drawdown) + 0.000001
    slice_passed = candidate_id == "static_baseline" or (
        dynamic_gap > 0.0 and drawdown_not_worse
    )
    fragility = _fragility_reasons(
        dynamic_gap=dynamic_gap,
        ranking_gap=ranking_gap,
        max_drawdown=max_drawdown,
        static_drawdown=static_drawdown,
        execution=execution,
    )
    return {
        "candidate_id": candidate_id,
        "role": role,
        "result_group": result_group,
        "scenario_id": scenario_id,
        "scenario_group": scenario_group,
        "execution_cadence": execution_cadence,
        "primary_execution_cadence": PRIMARY_EXECUTION_CADENCE,
        "monthly_rebalance_allowed_for_primary_decision": False,
        "is_primary_candidate": candidate_id == primary_candidate,
        "transaction_cost_bps": scenario.get("transaction_cost_bps"),
        "slippage_bps": scenario.get("slippage_bps"),
        "performance_metrics": {
            "total_return": performance.get("total_return"),
            "annualized_return": performance.get("annualized_return"),
            "max_drawdown": performance.get("max_drawdown"),
            "volatility": performance.get("volatility"),
            "sharpe_or_sortino_if_available": _first_present(
                performance.get("sortino"),
                performance.get("sharpe"),
            ),
            "sharpe": performance.get("sharpe"),
            "sortino": performance.get("sortino"),
            "downside_capture": performance.get("downside_capture"),
            "upside_capture": performance.get("upside_capture"),
        },
        "relative_metrics": {
            "dynamic_vs_static_gap": dynamic_gap,
            "dynamic_vs_ranking_top_gap": ranking_gap,
            "retest_slice_vs_full_sample_gap": round(annual - full_annual, 6),
            "cost_adjusted_dynamic_vs_static_gap": dynamic_gap,
        },
        "execution_metrics": dict(execution),
        "robustness_metrics": {
            "slice_passed": slice_passed,
            "stress_passed": dynamic_gap > 0.0,
            "ablation_dependency_score": None,
            "fragility_reason": "; ".join(fragility) if fragility else "none",
            "owner_review_triggered": bool(fragility),
        },
        "slice_passed": slice_passed,
        "stress_passed": dynamic_gap > 0.0,
        "fragility_reason": "; ".join(fragility) if fragility else "none",
        "owner_review_triggered": bool(fragility),
    }


def _metrics_for_returns(
    returns: pd.Series,
    *,
    benchmark_returns: pd.Series,
) -> dict[str, Any]:
    aligned = returns.dropna().astype(float)
    benchmark = benchmark_returns.reindex(aligned.index).fillna(0.0).astype(float)
    if aligned.empty:
        return {
            "total_return": 0.0,
            "annualized_return": 0.0,
            "max_drawdown": 0.0,
            "volatility": 0.0,
            "sharpe": 0.0,
            "sortino": 0.0,
            "downside_capture": 0.0,
            "upside_capture": 0.0,
        }
    equity = (1.0 + aligned).cumprod()
    total_return = float(equity.iloc[-1] - 1.0)
    annual_return = float(equity.iloc[-1] ** (252 / max(1, len(aligned))) - 1.0)
    annual_vol = float(aligned.std(ddof=0) * math.sqrt(252))
    downside = aligned[aligned < 0]
    downside_vol = float(downside.std(ddof=0) * math.sqrt(252)) if not downside.empty else 0.0
    drawdown = equity / equity.cummax() - 1.0
    upside_mask = benchmark > 0
    downside_mask = benchmark < 0
    return {
        "total_return": round(total_return, 6),
        "annualized_return": round(annual_return, 6),
        "max_drawdown": round(float(drawdown.min()), 6),
        "volatility": round(annual_vol, 6),
        "sharpe": round(_safe_ratio(annual_return, annual_vol), 6),
        "sortino": round(_safe_ratio(annual_return, downside_vol), 6),
        "downside_capture": round(
            _safe_ratio(
                float(aligned.loc[downside_mask].sum()),
                float(benchmark.loc[downside_mask].sum()),
            ),
            6,
        ),
        "upside_capture": round(
            _safe_ratio(
                float(aligned.loc[upside_mask].sum()),
                float(benchmark.loc[upside_mask].sum()),
            ),
            6,
        ),
    }


def _execution_metrics_for_path(
    path_rows: Sequence[Mapping[str, Any]],
    dates: Sequence[Any],
) -> dict[str, Any]:
    selected = {pd.Timestamp(item).date().isoformat() for item in dates}
    rows = [row for row in path_rows if str(row.get("date")) in selected]
    executed_indices = [
        index for index, row in enumerate(rows) if row.get("rebalance_executed") is True
    ]
    gaps = [
        current - previous
        for previous, current in zip(executed_indices, executed_indices[1:], strict=False)
    ]
    turnover = sum(_float(row.get("turnover")) for row in rows)
    max_monthly_turnover = 0.0
    if rows:
        frame = pd.DataFrame(rows)
        if "date" in frame.columns and "turnover" in frame.columns:
            frame["month"] = pd.to_datetime(frame["date"]).dt.to_period("M")
            max_monthly_turnover = float(
                frame.groupby("month")["turnover"].sum().max()
            )
    execution_lags = [
        _float(row.get("execution_lag_bdays"))
        for row in rows
        if row.get("rebalance_executed") is True
    ]
    return {
        "turnover": round(turnover, 6),
        "rebalance_count": len(executed_indices),
        "average_holding_days": round(_mean(gaps), 3),
        "max_monthly_turnover": round(max_monthly_turnover, 6),
        "signal_to_execution_lag_days": round(_mean(execution_lags), 3),
        "stale_signal_execution_count": sum(
            1
            for row in rows
            if row.get("rebalance_executed") is True and row.get("is_signal_stale") is True
        ),
        "missed_signal_count": sum(
            1
            for row in rows
            if row.get("rebalance_allowed") is True
            and row.get("rebalance_executed") is not True
            and _path_row_weight_gap(row) > 0.000001
        ),
        "cooldown_block_count": sum(
            1
            for row in rows
            if row.get("rebalance_executed") is not True
            and _int(row.get("signal_age_at_execution_days"), 999999)
            < _int(row.get("cooldown_days"), 0)
        ),
        "constraint_hit_count": sum(
            1
            for row in rows
            if row.get("rebalance_executed") is True
            and _float(row.get("turnover")) >= 0.999999
        ),
    }


def _time_slice_definitions(
    prices: pd.DataFrame,
    policy: Mapping[str, Any],
) -> list[dict[str, Any]]:
    index = prices.index
    count = len(index)
    first_cut = max(1, count // 3)
    second_cut = max(first_cut + 1, (count * 2) // 3)
    base = pd.Series(False, index=index)
    qqq_returns = prices["QQQ"].pct_change().fillna(0.0)
    rolling_vol = qqq_returns.rolling(
        _int(_mapping(policy.get("slice_policy")).get("rolling_volatility_window_sessions"), 20),
        min_periods=5,
    ).std()
    high_vol_threshold = rolling_vol.quantile(
        _float(_mapping(policy.get("slice_policy")).get("high_volatility_quantile"), 0.75)
    )
    drawdown = prices["QQQ"] / prices["QQQ"].cummax() - 1.0
    recovery = drawdown.lt(0.0) & qqq_returns.rolling(20, min_periods=5).sum().gt(0.0)
    post_start = pd.Timestamp(
        str(_mapping(policy.get("slice_policy")).get("post_2023_ai_cycle_start", "2024-01-01"))
    )
    return [
        _slice("full_available_window", "全样本复核", pd.Series(True, index=index)),
        _slice("early_period", "检查早期样本稳定性", _positional_mask(base, 0, first_cut)),
        _slice(
            "middle_period",
            "检查中段样本稳定性",
            _positional_mask(base, first_cut, second_cut),
        ),
        _slice(
            "recent_period",
            "检查最近阶段表现",
            _positional_mask(base, second_cut, count),
        ),
        _slice(
            "post_2023_ai_cycle",
            "检查 AI / 半导体行情后半段表现",
            pd.Series(index >= post_start, index=index),
        ),
        _slice(
            "high_volatility_periods",
            "检查高波动阶段表现",
            rolling_vol.ge(high_vol_threshold).fillna(False),
        ),
        _slice(
            "drawdown_recovery_periods",
            "检查回撤后恢复阶段表现",
            recovery.fillna(False),
        ),
    ]


def _regime_slice_definitions(
    prices: pd.DataFrame,
    policy: Mapping[str, Any],
) -> list[dict[str, Any]]:
    slice_policy = _mapping(policy.get("slice_policy"))
    qqq_returns = prices["QQQ"].pct_change().fillna(0.0)
    drawdown = prices["QQQ"] / prices["QQQ"].cummax() - 1.0
    rolling_vol = qqq_returns.rolling(
        _int(slice_policy.get("rolling_volatility_window_sessions"), 20),
        min_periods=5,
    ).std()
    trend_window = _int(slice_policy.get("trend_window_sessions"), 60)
    trend_return = prices["QQQ"].pct_change(trend_window).fillna(0.0)
    high_vol = rolling_vol.ge(rolling_vol.median()).fillna(False)
    low_vol = rolling_vol.lt(rolling_vol.median()).fillna(False)
    risk_off = (
        trend_return.le(0.0)
        | drawdown.le(_float(slice_policy.get("risk_off_drawdown_threshold"), -0.10))
    )
    risk_on = (
        trend_return.gt(0.0)
        & drawdown.gt(_float(slice_policy.get("risk_off_drawdown_threshold"), -0.10))
    )
    trend_confirmed = (
        prices["QQQ"].gt(prices["QQQ"].rolling(trend_window, min_periods=5).mean())
        & trend_return.gt(0.0)
    ).fillna(False)
    drawdown_mask = drawdown.le(_float(slice_policy.get("drawdown_threshold"), -0.08))
    recovery = drawdown.lt(0.0) & qqq_returns.rolling(20, min_periods=5).sum().gt(0.0)
    return [
        _slice("risk_on", "检查上涨行情是否过度保守", risk_on.fillna(False)),
        _slice("risk_off", "检查风险阶段防御能力", risk_off.fillna(False)),
        _slice("high_volatility", "检查高波动下 risk cap 是否有效", high_vol),
        _slice("low_volatility", "检查低波动下是否能捕捉 upside", low_vol),
        _slice("trend_confirmed", "检查趋势确认时的收益弹性", trend_confirmed),
        _slice(
            "trend_uncertain",
            "检查趋势不确定时是否降低误判",
            (~trend_confirmed & ~risk_off).fillna(False),
        ),
        _slice("drawdown", "检查回撤控制", drawdown_mask.fillna(False)),
        _slice("recovery", "检查恢复阶段是否过慢 re-entry", recovery.fillna(False)),
    ]


def _slice(slice_id: str, purpose: str, mask: pd.Series) -> dict[str, Any]:
    return {
        "slice_id": slice_id,
        "purpose": purpose,
        "mask": mask.astype(bool),
    }


def _positional_mask(mask: pd.Series, start: int, end: int) -> pd.Series:
    result = mask.copy()
    result.iloc[start:end] = True
    return result


def _cost_stress_scenarios() -> list[dict[str, Any]]:
    return [
        _scenario(
            "base",
            "cost_stress",
            transaction_cost_bps=2,
            slippage_bps=2,
            cooldown_days=1,
            min_holding_days=1,
            max_turnover_per_month=None,
            max_single_step_weight_delta=None,
        ),
        _scenario(
            "realistic",
            "cost_stress",
            transaction_cost_bps=5,
            slippage_bps=5,
            cooldown_days=3,
            min_holding_days=3,
            max_turnover_per_month=1.0,
            max_single_step_weight_delta=None,
        ),
        _scenario(
            "conservative",
            "cost_stress",
            transaction_cost_bps=10,
            slippage_bps=10,
            cooldown_days=5,
            min_holding_days=5,
            max_turnover_per_month=0.5,
            max_single_step_weight_delta=None,
        ),
        _scenario(
            "harsh",
            "cost_stress",
            transaction_cost_bps=20,
            slippage_bps=10,
            cooldown_days=10,
            min_holding_days=10,
            max_turnover_per_month=0.25,
            max_single_step_weight_delta=None,
        ),
    ]


def _execution_constraint_axes() -> dict[str, list[Any]]:
    return {
        "cooldown_days": [0, 1, 3, 5, 10],
        "min_holding_days": [0, 1, 3, 5, 10],
        "max_turnover_per_month": ["unlimited", 1.0, 0.5, 0.25],
        "max_single_step_weight_delta": ["unrestricted", 0.30, 0.20, 0.10],
    }


def _execution_constraint_scenarios() -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for value in (0, 1, 3, 5, 10):
        rows.append(
            _scenario(
                f"cooldown_days_{value}",
                "cooldown_days",
                transaction_cost_bps=5,
                slippage_bps=5,
                cooldown_days=value,
                min_holding_days=3,
                max_turnover_per_month=1.0,
                max_single_step_weight_delta=None,
                constraint_value=value,
            )
        )
    for value in (0, 1, 3, 5, 10):
        rows.append(
            _scenario(
                f"min_holding_days_{value}",
                "min_holding_days",
                transaction_cost_bps=5,
                slippage_bps=5,
                cooldown_days=3,
                min_holding_days=value,
                max_turnover_per_month=1.0,
                max_single_step_weight_delta=None,
                constraint_value=value,
            )
        )
    for value in (None, 1.0, 0.5, 0.25):
        label = _label(value, "unlimited")
        rows.append(
            _scenario(
                f"max_turnover_per_month_{label}",
                "max_turnover_per_month",
                transaction_cost_bps=5,
                slippage_bps=5,
                cooldown_days=3,
                min_holding_days=3,
                max_turnover_per_month=value,
                max_single_step_weight_delta=None,
                constraint_value=label,
            )
        )
    for value in (None, 0.30, 0.20, 0.10):
        label = _label(value, "unrestricted")
        rows.append(
            _scenario(
                f"max_single_step_weight_delta_{label}",
                "max_single_step_weight_delta",
                transaction_cost_bps=5,
                slippage_bps=5,
                cooldown_days=3,
                min_holding_days=3,
                max_turnover_per_month=1.0,
                max_single_step_weight_delta=value,
                constraint_value=label,
            )
        )
    return rows


def _ablation_definitions() -> list[dict[str, str]]:
    return [
        {
            "ablation_id": "no_lower_turnover_guardrail",
            "component": "lower_turnover_guardrail",
            "purpose": "验证 lower-turnover guardrail 是否是稳健性来源",
        },
        {
            "ablation_id": "no_valid_until_window",
            "component": "valid_until_window",
            "purpose": "验证 valid-until window 是否防止 stale signal",
        },
        {
            "ablation_id": "no_cooldown",
            "component": "cooldown",
            "purpose": "验证 cooldown 是否主要降低过度交易",
        },
        {
            "ablation_id": "no_risk_cap",
            "component": "risk_cap",
            "purpose": "验证 risk cap 对 drawdown 控制贡献",
        },
        {
            "ablation_id": "no_constraint_filter",
            "component": "constraint_filter",
            "purpose": "验证 constraint filter 是否防止无效调仓",
        },
        {
            "ablation_id": "no_growth_tilt_or_risk_overlay",
            "component": "growth_tilt_or_risk_overlay",
            "purpose": "验证收益弹性来源",
        },
    ]


def _ablation_scenario(ablation_id: str) -> dict[str, Any]:
    if ablation_id in {"no_cooldown", "no_constraint_filter"}:
        return _scenario(
            ablation_id,
            "ablation_test",
            transaction_cost_bps=5,
            slippage_bps=5,
            cooldown_days=0,
            min_holding_days=0,
            max_turnover_per_month=None,
            max_single_step_weight_delta=None,
        )
    return _scenario(
        ablation_id,
        "ablation_test",
        transaction_cost_bps=5,
        slippage_bps=5,
        cooldown_days=3,
        min_holding_days=3,
        max_turnover_per_month=1.0,
        max_single_step_weight_delta=None,
    )


def _scenario(
    scenario_id: str,
    scenario_group: str,
    *,
    transaction_cost_bps: float,
    slippage_bps: float,
    cooldown_days: int,
    min_holding_days: int,
    max_turnover_per_month: float | None,
    max_single_step_weight_delta: float | None,
    constraint_value: Any | None = None,
) -> dict[str, Any]:
    return {
        "scenario_id": scenario_id,
        "scenario_group": scenario_group,
        "transaction_cost_bps": float(transaction_cost_bps),
        "slippage_bps": float(slippage_bps),
        "cooldown_days": int(cooldown_days),
        "min_holding_days": int(min_holding_days),
        "max_turnover_per_month": max_turnover_per_month,
        "max_turnover_per_month_label": _label(max_turnover_per_month, "unlimited"),
        "max_single_step_weight_delta": max_single_step_weight_delta,
        "max_single_step_weight_delta_label": _label(
            max_single_step_weight_delta,
            "unrestricted",
        ),
        "constraint_value": constraint_value,
    }


def _ablation_target_weights(
    *,
    ablation_id: str,
    primary_weights: pd.DataFrame,
    ranking_weights: pd.DataFrame,
    prices: pd.DataFrame,
) -> pd.DataFrame:
    primary = primary_weights.copy()
    ranking = ranking_weights.reindex(primary.index).ffill().fillna(primary)
    if ablation_id == "no_lower_turnover_guardrail":
        qqq = (primary["QQQ"] * 0.35 + ranking["QQQ"] * 0.65).clip(0.0, 0.95)
    elif ablation_id == "no_risk_cap":
        qqq = pd.concat([primary["QQQ"], ranking["QQQ"]], axis=1).max(axis=1).clip(0.0, 0.95)
    elif ablation_id == "no_growth_tilt_or_risk_overlay":
        qqq = pd.Series(STATIC_BASELINE_WEIGHTS["QQQ"], index=primary.index)
    else:
        qqq = primary["QQQ"].copy()
    if ablation_id == "no_constraint_filter":
        qqq = qqq.rolling(2, min_periods=1).mean()
    if ablation_id == "no_cooldown":
        qqq = qqq.rolling(3, min_periods=1).mean()
    if ablation_id == "no_valid_until_window":
        qqq = qqq.ffill()
    return pd.DataFrame(
        {
            "QQQ": qqq.astype(float),
            "TQQQ": 0.0,
            "SGOV": 1.0 - qqq.astype(float),
        },
        index=prices.index,
    ).reindex(columns=list(ASSET_COLUMNS)).fillna(0.0)


def _candidate_decision_update(
    *,
    primary_candidate: str,
    ranking_top: str,
    sources: Mapping[str, Any],
    full_retest: Mapping[str, Any],
) -> dict[str, Any]:
    cost_rows = [
        _as_mapping(row)
        for row in _as_list(full_retest.get("cost_stress_result"))
        if _as_mapping(row).get("candidate_id") == primary_candidate
    ]
    time_rows = [
        _as_mapping(row)
        for row in _as_list(full_retest.get("time_slice_retest_result"))
        if _as_mapping(row).get("candidate_id") == primary_candidate
    ]
    regime_rows = [
        _as_mapping(row)
        for row in _as_list(full_retest.get("regime_slice_retest_result"))
        if _as_mapping(row).get("candidate_id") == primary_candidate
    ]
    ablation_rows = [
        _as_mapping(row) for row in _as_list(full_retest.get("ablation_test_result"))
    ]
    scenario_by_id = {str(row.get("scenario_id")): row for row in cost_rows}
    realistic = scenario_by_id.get("realistic", {})
    conservative = scenario_by_id.get("conservative", {})
    harsh = scenario_by_id.get("harsh", {})
    time_pass_rate = _pass_rate(time_rows, "slice_passed")
    regime_pass_rate = _pass_rate(regime_rows, "slice_passed")
    ablation_support_rate = _pass_rate(ablation_rows, "ablation_supports_guardrail")
    realistic_gap = _float(
        _as_mapping(realistic.get("relative_metrics")).get("dynamic_vs_static_gap")
    )
    conservative_gap = _float(
        _as_mapping(conservative.get("relative_metrics")).get(
            "dynamic_vs_static_gap"
        )
    )
    harsh_gap = _float(_as_mapping(harsh.get("relative_metrics")).get("dynamic_vs_static_gap"))
    primary_targeted = next(
        (
            _as_mapping(row)
            for row in _as_list(full_retest.get("targeted_retest_result"))
            if _as_mapping(row).get("candidate_id") == primary_candidate
        ),
        {},
    )
    turnover = _float(_as_mapping(primary_targeted.get("execution_metrics")).get("turnover"))
    ranking_gap = _float(
        _as_mapping(primary_targeted.get("relative_metrics")).get(
            "dynamic_vs_ranking_top_gap"
        )
    )
    decision = _decision_after_targeted_retest(
        realistic_gap=realistic_gap,
        conservative_gap=conservative_gap,
        harsh_gap=harsh_gap,
        time_pass_rate=time_pass_rate,
        regime_pass_rate=regime_pass_rate,
        ablation_support_rate=ablation_support_rate,
        turnover=turnover,
        ranking_gap=ranking_gap,
    )
    return {
        "schema_version": "dynamic_strategy_targeted_retest_decision_update.v1",
        "decision_update_ready": True,
        "primary_candidate": primary_candidate,
        "decision_from_2375": sources.get("decision_from_2375"),
        "ranking_top_from_2365": ranking_top,
        "realistic_dynamic_vs_static_gap": realistic_gap,
        "conservative_dynamic_vs_static_gap": conservative_gap,
        "harsh_dynamic_vs_static_gap": harsh_gap,
        "time_slice_pass_rate": time_pass_rate,
        "regime_slice_pass_rate": regime_pass_rate,
        "ablation_support_rate": ablation_support_rate,
        "turnover": turnover,
        "dynamic_vs_ranking_top_gap": ranking_gap,
        "candidate_decision_after_targeted_retest": decision,
        "candidate_ready_for_research_only_observation": (
            decision == DECISION_ACCEPT_RESEARCH_ONLY
        ),
        "recommended_next_research_task": NEXT_ROUTE,
        "monthly_rebalance_allowed_for_primary_decision": False,
        "paper_shadow_enabled": False,
        "production_enabled": False,
        "broker_action_enabled": False,
        "decision_reasons": _decision_reasons(
            decision=decision,
            realistic_gap=realistic_gap,
            conservative_gap=conservative_gap,
            harsh_gap=harsh_gap,
            time_pass_rate=time_pass_rate,
            regime_pass_rate=regime_pass_rate,
            ablation_support_rate=ablation_support_rate,
            turnover=turnover,
            ranking_gap=ranking_gap,
        ),
    }


def _decision_after_targeted_retest(
    *,
    realistic_gap: float,
    conservative_gap: float,
    harsh_gap: float,
    time_pass_rate: float,
    regime_pass_rate: float,
    ablation_support_rate: float,
    turnover: float,
    ranking_gap: float,
) -> str:
    if realistic_gap <= 0.0:
        return DECISION_REJECT
    if conservative_gap <= 0.0 or time_pass_rate < 0.50 or regime_pass_rate < 0.50:
        return DECISION_CONTINUE_OPTIMIZATION
    if (
        harsh_gap > 0.0
        and time_pass_rate >= 0.85
        and regime_pass_rate >= 0.75
        and ablation_support_rate >= 0.50
        and turnover <= 2.0
        and ranking_gap >= 0.0
    ):
        return DECISION_ACCEPT_RESEARCH_ONLY
    return DECISION_OWNER_REVIEW


def _summary_findings(
    *,
    primary_candidate: str,
    ranking_top: str,
    decision_update: Mapping[str, Any],
    full_retest: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "primary_candidate_remains_best_research_candidate": _yes_no(
            decision_update.get("candidate_decision_after_targeted_retest")
            in {DECISION_ACCEPT_RESEARCH_ONLY, DECISION_OWNER_REVIEW}
        ),
        "candidate_survives_time_slices": _yes_no(
            _float(decision_update.get("time_slice_pass_rate")) >= 0.50
        ),
        "candidate_survives_regime_slices": _yes_no(
            _float(decision_update.get("regime_slice_pass_rate")) >= 0.50
        ),
        "candidate_survives_realistic_cost": _yes_no(
            _float(decision_update.get("realistic_dynamic_vs_static_gap")) > 0.0
        ),
        "candidate_survives_conservative_cost": _yes_no(
            _float(decision_update.get("conservative_dynamic_vs_static_gap")) > 0.0
        ),
        "candidate_ablation_supports_guardrails": _yes_no(
            _float(decision_update.get("ablation_support_rate")) >= 0.50
        ),
        "candidate_ready_for_research_only_observation": _yes_no(
            bool(decision_update.get("candidate_ready_for_research_only_observation"))
        ),
        "valid_until_window_remains_necessary": _yes_no(
            _valid_until_ablation_supports(full_retest)
        ),
        "lower_turnover_guardrail_has_actual_contribution": _yes_no(
            _ablation_supports(full_retest, "no_lower_turnover_guardrail")
        ),
        "ranking_top_conflict_resolved": _yes_no(
            primary_candidate == ranking_top
            or _float(decision_update.get("dynamic_vs_ranking_top_gap")) >= 0.0
        ),
        "candidate_decision_after_targeted_retest": decision_update.get(
            "candidate_decision_after_targeted_retest"
        ),
        "recommended_next_research_task": NEXT_ROUTE,
        "paper_shadow_remains_disabled": True,
        "production_remains_disabled": True,
        "broker_remains_disabled": True,
    }


def _write_outputs(
    *,
    payload: dict[str, Any],
    output_root: Path,
    docs_root: Path,
) -> None:
    output_root.mkdir(parents=True, exist_ok=True)
    docs_root.mkdir(parents=True, exist_ok=True)
    artifact_paths = {
        "json_path": str(output_root / "targeted_retest_result.json"),
        "time_regime_slice_matrix_json": str(
            output_root / "time_regime_slice_matrix.json"
        ),
        "ablation_test_report_json": str(output_root / "ablation_test_report.json"),
        "decision_update_json": str(output_root / "decision_update.json"),
        "markdown_path": str(
            docs_root / "dynamic_strategy_optimized_candidate_targeted_retest.md"
        ),
        "slice_markdown": str(
            docs_root / "dynamic_strategy_targeted_retest_slice_report.md"
        ),
        "ablation_markdown": str(
            docs_root / "dynamic_strategy_targeted_retest_ablation_report.md"
        ),
        "next_route_markdown": str(docs_root / "dynamic_strategy_2377_route.md"),
    }
    payload["artifact_paths"] = artifact_paths
    _write_json(Path(artifact_paths["json_path"]), payload)
    _write_json(
        Path(artifact_paths["time_regime_slice_matrix_json"]),
        {
            "report_type": "dynamic_strategy_targeted_retest_slice_matrix",
            "schema_version": "dynamic_strategy_targeted_retest_slice_matrix.v1",
            "status": payload["status"],
            "generated_at": payload["generated_at"],
            "primary_candidate": payload.get("primary_candidate"),
            "primary_execution_cadence": payload.get("primary_execution_cadence"),
            "time_slice_retest_result": payload.get("time_slice_retest_result", []),
            "regime_slice_retest_result": payload.get("regime_slice_retest_result", []),
            "production_effect": "none",
            "broker_action": "none",
            "paper_shadow_allowed": False,
            "production_allowed": False,
        },
    )
    _write_json(
        Path(artifact_paths["ablation_test_report_json"]),
        {
            "report_type": "dynamic_strategy_targeted_retest_ablation_report",
            "schema_version": "dynamic_strategy_targeted_retest_ablation_report.v1",
            "status": payload["status"],
            "generated_at": payload["generated_at"],
            "primary_candidate": payload.get("primary_candidate"),
            "ablation_test_result": payload.get("ablation_test_result", []),
            "production_effect": "none",
            "broker_action": "none",
            "paper_shadow_allowed": False,
            "production_allowed": False,
        },
    )
    _write_json(
        Path(artifact_paths["decision_update_json"]),
        {
            "report_type": "dynamic_strategy_targeted_retest_decision_update",
            "schema_version": "dynamic_strategy_targeted_retest_decision_update.v1",
            "status": payload["status"],
            "generated_at": payload["generated_at"],
            "decision_update": payload.get("decision_update", {}),
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
    Path(artifact_paths["slice_markdown"]).write_text(
        _slice_markdown(payload),
        encoding="utf-8",
    )
    Path(artifact_paths["ablation_markdown"]).write_text(
        _ablation_markdown(payload),
        encoding="utf-8",
    )
    Path(artifact_paths["next_route_markdown"]).write_text(
        _route_markdown(payload),
        encoding="utf-8",
    )


def _main_markdown(payload: Mapping[str, Any]) -> str:
    summary = _as_mapping(payload.get("summary_findings"))
    decision = _as_mapping(payload.get("decision_update"))
    data_quality = _as_mapping(payload.get("data_quality"))
    return "\n".join(
        [
            "# 动态策略 optimized candidate targeted retest",
            "",
            "## Executive summary",
            "",
            f"- status：`{payload.get('status')}`",
            f"- data quality：`{data_quality.get('status')}`",
            f"- primary candidate：`{payload.get('primary_candidate')}`",
            f"- decision from 2375：`{payload.get('decision_from_2375')}`",
            f"- primary execution cadence：`{payload.get('primary_execution_cadence')}`",
            (
                "- candidate decision after targeted retest："
                f"`{payload.get('candidate_decision_after_targeted_retest')}`"
            ),
            f"- next route：`{payload.get('recommended_next_research_task')}`",
            "",
            "## Required answers",
            "",
            (
                "- 是否仍优于 static baseline："
                f"`{summary.get('candidate_survives_realistic_cost')}` "
                "(realistic cost-adjusted basis)"
            ),
            f"- 是否穿越 time slices：`{summary.get('candidate_survives_time_slices')}`",
            f"- 是否穿越 market regimes：`{summary.get('candidate_survives_regime_slices')}`",
            (
                "- 是否穿越 realistic / conservative cost："
                f"`{summary.get('candidate_survives_realistic_cost')}` / "
                f"`{summary.get('candidate_survives_conservative_cost')}`"
            ),
            (
                "- lower-turnover guardrail 是否有贡献："
                f"`{summary.get('lower_turnover_guardrail_has_actual_contribution')}`"
            ),
            (
                "- valid_until_window 是否仍必要："
                f"`{summary.get('valid_until_window_remains_necessary')}`"
            ),
            (
                "- ablation 是否支持 guardrails："
                f"`{summary.get('candidate_ablation_supports_guardrails')}`"
            ),
            (
                "- 是否可升级到 research-only observation："
                f"`{summary.get('candidate_ready_for_research_only_observation')}`"
            ),
            "- 是否允许 paper-shadow / production / broker：`NO`",
            "",
            "## Decision update",
            "",
            f"- time slice pass rate：`{decision.get('time_slice_pass_rate')}`",
            f"- regime slice pass rate：`{decision.get('regime_slice_pass_rate')}`",
            f"- ablation support rate：`{decision.get('ablation_support_rate')}`",
            f"- realistic gap：`{decision.get('realistic_dynamic_vs_static_gap')}`",
            f"- conservative gap：`{decision.get('conservative_dynamic_vs_static_gap')}`",
            f"- harsh gap：`{decision.get('harsh_dynamic_vs_static_gap')}`",
            f"- decision reasons：`{'; '.join(_as_list(decision.get('decision_reasons')))}`",
            "",
            "## Safety boundary",
            "",
            "- 本报告只生成 strategy research evidence。",
            "- monthly rebalance 只允许作为旧口径 reference，不允许进入 primary decision。",
            (
                "- scheduler、event append、outcome binding、paper-shadow、production、"
                "broker/order 和 daily report 全部保持 disabled / false / none。"
            ),
        ]
    )


def _slice_markdown(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# 动态策略 targeted retest slice report",
            "",
            f"- status：`{payload.get('status')}`",
            f"- primary candidate：`{payload.get('primary_candidate')}`",
            "",
            "## Time slices",
            "",
            _slice_table(payload, "time_slice_retest_result"),
            "",
            "## Regime slices",
            "",
            _slice_table(payload, "regime_slice_retest_result"),
        ]
    )


def _ablation_markdown(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# 动态策略 targeted retest ablation report",
            "",
            f"- status：`{payload.get('status')}`",
            f"- primary candidate：`{payload.get('primary_candidate')}`",
            "",
            _ablation_table(payload),
            "",
            (
                "- ablation rows 是 research-only diagnostic target-path "
                "transformation，不是正式策略配置。"
            ),
            (
                "- 任何 observation / paper-shadow / production / broker 讨论仍需 "
                "TRADING-2377 owner review。"
            ),
        ]
    )


def _route_markdown(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# TRADING-2377 route",
            "",
            f"- current status：`{payload.get('status')}`",
            f"- primary candidate：`{payload.get('primary_candidate')}`",
            (
                "- decision after targeted retest："
                f"`{payload.get('candidate_decision_after_targeted_retest')}`"
            ),
            f"- next route：`{payload.get('recommended_next_research_task')}`",
            (
                "- route boundary：targeted retest owner review and observation "
                "decision；不是 paper-shadow、production、broker 或 daily report approval。"
            ),
        ]
    )


def _slice_table(payload: Mapping[str, Any], key: str) -> str:
    lines = [
        "|candidate|slice|annual|gap_static|gap_ranking|mdd|turnover|pass|fragility|",
        "|---|---|---:|---:|---:|---:|---:|---|---|",
    ]
    for row in _as_list(payload.get(key)):
        item = _as_mapping(row)
        if item.get("candidate_id") != payload.get("primary_candidate"):
            continue
        perf = _as_mapping(item.get("performance_metrics"))
        rel = _as_mapping(item.get("relative_metrics"))
        execution = _as_mapping(item.get("execution_metrics"))
        lines.append(
            "|"
            + "|".join(
                [
                    f"`{item.get('candidate_id')}`",
                    f"`{item.get('scenario_id')}`",
                    _fmt(perf.get("annualized_return")),
                    _fmt(rel.get("dynamic_vs_static_gap")),
                    _fmt(rel.get("dynamic_vs_ranking_top_gap")),
                    _fmt(perf.get("max_drawdown")),
                    _fmt(execution.get("turnover")),
                    f"`{item.get('slice_passed')}`",
                    str(item.get("fragility_reason")),
                ]
            )
            + "|"
        )
    return "\n".join(lines)


def _ablation_table(payload: Mapping[str, Any]) -> str:
    lines = [
        "|ablation|annual|gap_static|mdd|turnover|dependency|supports_guardrail|fragility|",
        "|---|---:|---:|---:|---:|---:|---|---|",
    ]
    for row in _as_list(payload.get("ablation_test_result")):
        item = _as_mapping(row)
        perf = _as_mapping(item.get("performance_metrics"))
        rel = _as_mapping(item.get("relative_metrics"))
        execution = _as_mapping(item.get("execution_metrics"))
        lines.append(
            "|"
            + "|".join(
                [
                    f"`{item.get('ablation_id')}`",
                    _fmt(perf.get("annualized_return")),
                    _fmt(rel.get("dynamic_vs_static_gap")),
                    _fmt(perf.get("max_drawdown")),
                    _fmt(execution.get("turnover")),
                    _fmt(item.get("ablation_dependency_score")),
                    f"`{item.get('ablation_supports_guardrail')}`",
                    str(item.get("fragility_reason")),
                ]
            )
            + "|"
        )
    return "\n".join(lines)


def _required_outputs_ready(ready: bool) -> dict[str, bool]:
    return {
        "primary_candidate": ready,
        "targeted_retest_result": ready,
        "time_slice_retest_result": ready,
        "regime_slice_retest_result": ready,
        "cost_stress_result": ready,
        "execution_constraint_stress_result": ready,
        "ablation_test_result": ready,
        "candidate_decision_after_targeted_retest": ready,
        "recommended_next_research_task": True,
    }


def _comparison_candidate_ids(
    *,
    primary_candidate: str,
    ranking_top: str,
    robustness_top: str,
) -> list[str]:
    result: list[str] = []
    for candidate_id in (
        "static_baseline",
        primary_candidate,
        ranking_top,
        robustness_top,
        CURRENT_DYNAMIC_DEFAULT_CANDIDATE_ID,
    ):
        if candidate_id and candidate_id not in result:
            result.append(candidate_id)
    return result


def _candidate_role(
    candidate_id: str,
    *,
    primary_candidate: str,
    ranking_top: str,
) -> str:
    if candidate_id == "static_baseline":
        return "static_baseline"
    if candidate_id == primary_candidate:
        return "primary_candidate"
    if candidate_id == ranking_top:
        return "ranking_top_from_2365"
    if candidate_id == CURRENT_DYNAMIC_DEFAULT_CANDIDATE_ID:
        return "current_dynamic_default_if_available"
    return "comparison_candidate"


def _signal_validity_profile_for_retest(
    cadence: str,
    policy: Mapping[str, Any],
) -> dict[str, Any]:
    validity_days = max(1, _int(policy.get("validity_period_days"), 10))
    return {
        "primary_signal_class": "dynamic_strategy_target_weight",
        "confirmation_required": False,
        "min_validity_days_required_for_execution": 1,
        "stale_after_days": validity_days,
        "near_stale_within_days": 1,
        "stale_action": "suppress_rebalance"
        if cadence == PRIMARY_EXECUTION_CADENCE
        else "hold_previous_position",
        "actual_path_only": True,
    }


def _fragility_reasons(
    *,
    dynamic_gap: float,
    ranking_gap: float | None,
    max_drawdown: float,
    static_drawdown: float,
    execution: Mapping[str, Any],
) -> list[str]:
    reasons: list[str] = []
    if dynamic_gap <= 0.0:
        reasons.append("cost_adjusted_static_gap_non_positive")
    if ranking_gap is not None and ranking_gap < 0.0:
        reasons.append("underperforms_2365_ranking_top_on_return")
    if abs(max_drawdown) > abs(static_drawdown) + 0.000001:
        reasons.append("drawdown_worse_than_static")
    if _float(execution.get("stale_signal_execution_count")) > 0:
        reasons.append("stale_signal_execution_detected")
    if _float(execution.get("missed_signal_count")) > 0:
        reasons.append("missed_signal_count_nonzero")
    return reasons


def _decision_reasons(
    *,
    decision: str,
    realistic_gap: float,
    conservative_gap: float,
    harsh_gap: float,
    time_pass_rate: float,
    regime_pass_rate: float,
    ablation_support_rate: float,
    turnover: float,
    ranking_gap: float,
) -> list[str]:
    reasons = [
        f"realistic_gap={realistic_gap}",
        f"conservative_gap={conservative_gap}",
        f"harsh_gap={harsh_gap}",
        f"time_slice_pass_rate={time_pass_rate}",
        f"regime_slice_pass_rate={regime_pass_rate}",
        f"ablation_support_rate={ablation_support_rate}",
        f"turnover={turnover}",
        f"dynamic_vs_ranking_top_gap={ranking_gap}",
    ]
    if decision == DECISION_OWNER_REVIEW:
        reasons.append("mostly robust but retains owner-review tradeoffs")
    return reasons


def _ablation_dependency_score(
    primary_row: Mapping[str, Any],
    ablation_row: Mapping[str, Any],
) -> float:
    primary_perf = _as_mapping(primary_row.get("performance_metrics"))
    ablation_perf = _as_mapping(ablation_row.get("performance_metrics"))
    primary_exec = _as_mapping(primary_row.get("execution_metrics"))
    ablation_exec = _as_mapping(ablation_row.get("execution_metrics"))
    annual_penalty = max(
        0.0,
        _float(primary_perf.get("annualized_return"))
        - _float(ablation_perf.get("annualized_return")),
    )
    drawdown_penalty = max(
        0.0,
        abs(_float(ablation_perf.get("max_drawdown")))
        - abs(_float(primary_perf.get("max_drawdown"))),
    )
    turnover_penalty = max(
        0.0,
        _float(ablation_exec.get("turnover")) - _float(primary_exec.get("turnover")),
    ) * 0.01
    return round(annual_penalty + drawdown_penalty + turnover_penalty, 6)


def _valid_until_ablation_supports(full_retest: Mapping[str, Any]) -> bool:
    return _ablation_supports(full_retest, "no_valid_until_window")


def _ablation_supports(full_retest: Mapping[str, Any], ablation_id: str) -> bool:
    row = next(
        (
            _as_mapping(item)
            for item in _as_list(full_retest.get("ablation_test_result"))
            if _as_mapping(item).get("ablation_id") == ablation_id
        ),
        {},
    )
    return bool(row.get("ablation_supports_guardrail"))


def _pass_rate(rows: Sequence[Mapping[str, Any]], field: str) -> float:
    if not rows:
        return 0.0
    return round(sum(1 for row in rows if bool(row.get(field))) / len(rows), 6)


def _benchmark_returns(prices: pd.DataFrame) -> pd.Series:
    return prices["QQQ"].pct_change().fillna(0.0)


def _path_row_weight_gap(row: Mapping[str, Any]) -> float:
    gaps = [
        abs(
            _float(row.get(f"target_weight_{ticker.lower()}"))
            - _float(row.get(f"actual_weight_{ticker.lower()}"))
        )
        for ticker in ASSET_COLUMNS
    ]
    return max(gaps) if gaps else 0.0


def _date_range_for_mask(mask: pd.Series) -> dict[str, str | None]:
    selected = mask.index[mask]
    if len(selected) == 0:
        return {"start": None, "end": None}
    return {
        "start": pd.Timestamp(selected[0]).date().isoformat(),
        "end": pd.Timestamp(selected[-1]).date().isoformat(),
    }


def _extract_ranked_candidate(rows: Sequence[Any], *, rank_field: str) -> str | None:
    ranked = sorted(
        (
            _as_mapping(row)
            for row in rows
            if _as_mapping(row).get("candidate_id")
        ),
        key=lambda row: _int(row.get(rank_field), 999),
    )
    if not ranked:
        return None
    return str(ranked[0]["candidate_id"])


def _source_artifact(path: Path, document: Any) -> dict[str, Any]:
    return {
        "path": str(path),
        "exists": path.exists(),
        "sha256": _file_sha256(path) if path.exists() else None,
        "status": _as_mapping(document).get("status"),
        "schema_version": _as_mapping(document).get("schema_version"),
    }


def _load_json_document(path: Path) -> Any:
    if not path.exists():
        return {"status": "MISSING", "path": str(path)}
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )


def _side_effect_false_fields() -> tuple[str, ...]:
    return (
        "scheduler_enabled",
        "event_append_enabled",
        "outcome_binding_enabled",
        "outcome_store_mutated",
        "paper_shadow_enabled",
        "paper_trade_created",
        "shadow_position_created",
        "production_enabled",
        "broker_action_enabled",
        "daily_report_generated",
    )


def _as_mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _as_list(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []


def _first_present(*values: Any) -> Any:
    for value in values:
        if value is not None:
            return value
    return None


def _series(value: Any) -> pd.Series:
    if isinstance(value, pd.Series):
        return value
    return pd.Series(dtype=float)


def _mean(values: Sequence[float] | Any) -> float:
    data = [float(value) for value in values if value is not None]
    if not data:
        return 0.0
    return sum(data) / len(data)


def _safe_ratio(numerator: float, denominator: float) -> float:
    if denominator == 0:
        return 0.0
    return numerator / denominator


def _label(value: float | None, none_label: str) -> str:
    if value is None:
        return none_label
    text = f"{float(value):.2f}".rstrip("0").rstrip(".")
    return text.replace(".", "p")


def _yes_no(value: bool) -> str:
    return "YES" if bool(value) else "NO"


def _fmt(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float):
        return f"{value:.6f}".rstrip("0").rstrip(".")
    return str(value)
