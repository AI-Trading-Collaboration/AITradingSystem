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
    NEXT_ROUTE as SOURCE_2375_EXPECTED_ROUTE,
)
from ai_trading_system.dynamic_strategy_candidate_optimization_divergence_review import (
    READY_STATUS as SOURCE_2375_READY_STATUS,
)
from ai_trading_system.dynamic_strategy_cost_turnover_cooldown_sensitivity import (
    DEFAULT_DYNAMIC_STRATEGY_COST_TURNOVER_COOLDOWN_SENSITIVITY_OUTPUT_ROOT,
    DEFAULT_SOURCE_CANDIDATE_RANKING_PATH,
    DEFAULT_SOURCE_EVENT_DRIVEN_RETEST_PATH,
    PRIMARY_EXECUTION_CADENCE,
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
    PRIMARY_CANDIDATE_ID,
)
from ai_trading_system.dynamic_strategy_optimized_candidate_targeted_retest import (
    NEXT_ROUTE as SOURCE_2376_EXPECTED_ROUTE,
)
from ai_trading_system.dynamic_strategy_optimized_candidate_targeted_retest import (
    READY_STATUS as SOURCE_2376_READY_STATUS,
)
from ai_trading_system.execution_semantics import AI_REGIME_SUMMARY, _file_sha256

TASK_ID = "TRADING-2377"
TASK_REGISTER_ID = (
    "TRADING-2377_DYNAMIC_STRATEGY_TARGETED_RETEST_OWNER_REVIEW_AND_"
    "OPTIMIZATION_DECISION"
)
REPORT_TYPE = "dynamic_strategy_targeted_retest_owner_review_decision"
SCHEMA_VERSION = (
    "dynamic_strategy_targeted_retest_owner_review_and_optimization_decision.v1"
)
READY_STATUS = (
    "DYNAMIC_STRATEGY_TARGETED_RETEST_OWNER_REVIEW_AND_"
    "OPTIMIZATION_DECISION_READY"
)
BLOCKED_SOURCE_STATUS = (
    "DYNAMIC_STRATEGY_TARGETED_RETEST_OWNER_REVIEW_AND_"
    "OPTIMIZATION_DECISION_BLOCKED_SOURCE_ARTIFACT"
)
OWNER_DECISION = "KEEP_RESEARCH_ONLY_AND_CONTINUE_OPTIMIZATION"
NEXT_ROUTE = (
    "TRADING-2378_Dynamic_Strategy_Slice_Robustness_And_"
    "Return_Gap_Optimization_Plan"
)
RANKING_TOP_FALLBACK = "equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1"
SOURCE_TASKS: tuple[str, ...] = (
    "TRADING-2365",
    "TRADING-2366",
    "TRADING-2375",
    "TRADING-2376",
)
OPTIMIZATION_FOCUS: tuple[str, ...] = (
    "time_slice_robustness_improvement",
    "regime_slice_robustness_improvement",
    "return_gap_repair_vs_ranking_top",
    "upside_capture_without_turnover_increase",
    "valid_until_window_parameter_tuning",
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
)
DATA_QUALITY_GATE_REASON = (
    "NOT_APPLICABLE_PRIOR_ARTIFACT_OWNER_REVIEW_ONLY_NO_FRESH_MARKET_DATA"
)

DEFAULT_DYNAMIC_STRATEGY_TARGETED_RETEST_OWNER_REVIEW_DECISION_OUTPUT_ROOT = (
    PROJECT_ROOT / "outputs" / "research_strategies" / REPORT_TYPE
)
DEFAULT_DYNAMIC_STRATEGY_TARGETED_RETEST_OWNER_REVIEW_DECISION_DOCS_ROOT = (
    PROJECT_ROOT / "docs" / "research"
)
DEFAULT_SOURCE_2365_EVENT_RETEST_PATH = DEFAULT_SOURCE_EVENT_DRIVEN_RETEST_PATH
DEFAULT_SOURCE_2365_CANDIDATE_RANKING_PATH = DEFAULT_SOURCE_CANDIDATE_RANKING_PATH
DEFAULT_SOURCE_2366_SENSITIVITY_RESULT_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_COST_TURNOVER_COOLDOWN_SENSITIVITY_OUTPUT_ROOT
    / "sensitivity_result.json"
)
DEFAULT_SOURCE_2366_SENSITIVITY_DECISION_UPDATE_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_COST_TURNOVER_COOLDOWN_SENSITIVITY_OUTPUT_ROOT
    / "decision_update.json"
)
DEFAULT_SOURCE_2375_OPTIMIZATION_REVIEW_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_CANDIDATE_OPTIMIZATION_DIVERGENCE_REVIEW_OUTPUT_ROOT
    / "divergence_review_result.json"
)
DEFAULT_SOURCE_2375_OPTIMIZATION_DECISION_UPDATE_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_CANDIDATE_OPTIMIZATION_DIVERGENCE_REVIEW_OUTPUT_ROOT
    / "candidate_decision_update.json"
)
DEFAULT_SOURCE_2376_TARGETED_RETEST_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_OPTIMIZED_CANDIDATE_TARGETED_RETEST_OUTPUT_ROOT
    / "targeted_retest_result.json"
)
DEFAULT_SOURCE_2376_TARGETED_DECISION_UPDATE_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_OPTIMIZED_CANDIDATE_TARGETED_RETEST_OUTPUT_ROOT
    / "decision_update.json"
)


def run_dynamic_strategy_targeted_retest_owner_review_decision(
    *,
    source_event_retest_path: Path = DEFAULT_SOURCE_2365_EVENT_RETEST_PATH,
    source_candidate_ranking_path: Path = DEFAULT_SOURCE_2365_CANDIDATE_RANKING_PATH,
    source_sensitivity_result_path: Path = DEFAULT_SOURCE_2366_SENSITIVITY_RESULT_PATH,
    source_sensitivity_decision_update_path: Path = (
        DEFAULT_SOURCE_2366_SENSITIVITY_DECISION_UPDATE_PATH
    ),
    source_optimization_review_path: Path = DEFAULT_SOURCE_2375_OPTIMIZATION_REVIEW_PATH,
    source_optimization_decision_update_path: Path = (
        DEFAULT_SOURCE_2375_OPTIMIZATION_DECISION_UPDATE_PATH
    ),
    source_targeted_retest_path: Path = DEFAULT_SOURCE_2376_TARGETED_RETEST_PATH,
    source_targeted_decision_update_path: Path = (
        DEFAULT_SOURCE_2376_TARGETED_DECISION_UPDATE_PATH
    ),
    output_root: Path = (
        DEFAULT_DYNAMIC_STRATEGY_TARGETED_RETEST_OWNER_REVIEW_DECISION_OUTPUT_ROOT
    ),
    docs_root: Path = (
        DEFAULT_DYNAMIC_STRATEGY_TARGETED_RETEST_OWNER_REVIEW_DECISION_DOCS_ROOT
    ),
    as_of_date: date | None = None,
) -> dict[str, Any]:
    sources = _load_sources(
        source_event_retest_path=source_event_retest_path,
        source_candidate_ranking_path=source_candidate_ranking_path,
        source_sensitivity_result_path=source_sensitivity_result_path,
        source_sensitivity_decision_update_path=(
            source_sensitivity_decision_update_path
        ),
        source_optimization_review_path=source_optimization_review_path,
        source_optimization_decision_update_path=(
            source_optimization_decision_update_path
        ),
        source_targeted_retest_path=source_targeted_retest_path,
        source_targeted_decision_update_path=source_targeted_decision_update_path,
    )
    ready = not sources["source_validation_errors"]
    resolved_as_of = _resolve_as_of(as_of_date, sources["targeted_retest"])
    targeted_summary = _targeted_retest_summary(sources)
    owner_record = _owner_review_decision_record(
        sources=sources,
        targeted_summary=targeted_summary,
        as_of_date=resolved_as_of,
        ready=ready,
    )
    continue_gate = _continue_optimization_gate(
        sources=sources,
        targeted_summary=targeted_summary,
        owner_record=owner_record,
        ready=ready,
    )
    evidence = _no_side_effect_evidence(owner_record=owner_record)
    payload = _base_payload(
        status=READY_STATUS if ready else BLOCKED_SOURCE_STATUS,
        as_of_date=resolved_as_of,
        sources=sources,
        targeted_summary=targeted_summary,
        owner_record=owner_record,
        continue_gate=continue_gate,
        evidence=evidence,
        ready=ready,
    )
    _write_outputs(payload=payload, output_root=output_root, docs_root=docs_root)
    return payload


def _load_sources(
    *,
    source_event_retest_path: Path,
    source_candidate_ranking_path: Path,
    source_sensitivity_result_path: Path,
    source_sensitivity_decision_update_path: Path,
    source_optimization_review_path: Path,
    source_optimization_decision_update_path: Path,
    source_targeted_retest_path: Path,
    source_targeted_decision_update_path: Path,
) -> dict[str, Any]:
    event_retest = _load_json_document(source_event_retest_path)
    candidate_ranking = _load_json_document(source_candidate_ranking_path)
    sensitivity_result = _load_json_document(source_sensitivity_result_path)
    sensitivity_decision_update = _load_json_document(
        source_sensitivity_decision_update_path
    )
    optimization_review = _load_json_document(source_optimization_review_path)
    optimization_decision_update = _load_json_document(
        source_optimization_decision_update_path
    )
    targeted_retest = _load_json_document(source_targeted_retest_path)
    targeted_decision_update = _load_json_document(source_targeted_decision_update_path)
    source_status = {
        "event_retest": _as_mapping(event_retest).get("status"),
        "candidate_ranking": _as_mapping(candidate_ranking).get("status"),
        "sensitivity_result": _as_mapping(sensitivity_result).get("status"),
        "sensitivity_decision_update": _as_mapping(
            sensitivity_decision_update
        ).get("status"),
        "optimization_review": _as_mapping(optimization_review).get("status"),
        "optimization_decision_update": _as_mapping(
            optimization_decision_update
        ).get("status"),
        "targeted_retest": _as_mapping(targeted_retest).get("status"),
        "targeted_decision_update": _as_mapping(targeted_decision_update).get("status"),
    }
    primary_candidate = _primary_candidate(targeted_retest, optimization_review)
    ranking_top = _ranking_top(
        event_retest=event_retest,
        candidate_ranking=candidate_ranking,
        optimization_review=optimization_review,
        optimization_decision_update=optimization_decision_update,
        targeted_retest=targeted_retest,
    )
    robustness_top = _robustness_top(
        sensitivity_result=sensitivity_result,
        sensitivity_decision_update=sensitivity_decision_update,
        optimization_review=optimization_review,
        optimization_decision_update=optimization_decision_update,
        targeted_retest=targeted_retest,
    )
    decision_from_2376 = _decision_from_2376(
        targeted_retest=targeted_retest,
        targeted_decision_update=targeted_decision_update,
    )
    decision_from_2375 = _decision_from_2375(
        optimization_review=optimization_review,
        optimization_decision_update=optimization_decision_update,
        targeted_retest=targeted_retest,
    )
    validation_errors = _source_validation_errors(
        source_status=source_status,
        event_retest=event_retest,
        candidate_ranking=candidate_ranking,
        sensitivity_result=sensitivity_result,
        sensitivity_decision_update=sensitivity_decision_update,
        optimization_review=optimization_review,
        optimization_decision_update=optimization_decision_update,
        targeted_retest=targeted_retest,
        targeted_decision_update=targeted_decision_update,
        primary_candidate=primary_candidate,
        ranking_top=ranking_top,
        robustness_top=robustness_top,
        decision_from_2375=decision_from_2375,
        decision_from_2376=decision_from_2376,
    )
    return {
        "event_retest": event_retest,
        "candidate_ranking": candidate_ranking,
        "sensitivity_result": sensitivity_result,
        "sensitivity_decision_update": sensitivity_decision_update,
        "optimization_review": optimization_review,
        "optimization_decision_update": optimization_decision_update,
        "targeted_retest": targeted_retest,
        "targeted_decision_update": targeted_decision_update,
        "source_status": source_status,
        "source_validation_errors": validation_errors,
        "source_ready_for_owner_review_decision": not validation_errors,
        "primary_candidate": primary_candidate,
        "ranking_top_from_2365": ranking_top,
        "robustness_top_from_2366": robustness_top,
        "decision_from_2375": decision_from_2375,
        "decision_from_2376": decision_from_2376,
        "source_artifacts": {
            "event_retest": _source_artifact(source_event_retest_path, event_retest),
            "candidate_ranking": _source_artifact(
                source_candidate_ranking_path,
                candidate_ranking,
            ),
            "sensitivity_result": _source_artifact(
                source_sensitivity_result_path,
                sensitivity_result,
            ),
            "sensitivity_decision_update": _source_artifact(
                source_sensitivity_decision_update_path,
                sensitivity_decision_update,
            ),
            "optimization_review": _source_artifact(
                source_optimization_review_path,
                optimization_review,
            ),
            "optimization_decision_update": _source_artifact(
                source_optimization_decision_update_path,
                optimization_decision_update,
            ),
            "targeted_retest": _source_artifact(
                source_targeted_retest_path,
                targeted_retest,
            ),
            "targeted_decision_update": _source_artifact(
                source_targeted_decision_update_path,
                targeted_decision_update,
            ),
        },
    }


def _source_validation_errors(
    *,
    source_status: Mapping[str, Any],
    event_retest: Any,
    candidate_ranking: Any,
    sensitivity_result: Any,
    sensitivity_decision_update: Any,
    optimization_review: Any,
    optimization_decision_update: Any,
    targeted_retest: Any,
    targeted_decision_update: Any,
    primary_candidate: str,
    ranking_top: str,
    robustness_top: str,
    decision_from_2375: str,
    decision_from_2376: str,
) -> list[str]:
    errors: list[str] = []
    for key in ("event_retest", "candidate_ranking"):
        if source_status.get(key) != SOURCE_2365_READY_STATUS:
            errors.append(f"{key}_status_not_ready")
    for key in ("sensitivity_result", "sensitivity_decision_update"):
        if source_status.get(key) != SOURCE_2366_READY_STATUS:
            errors.append(f"{key}_status_not_ready")
    for key in ("optimization_review", "optimization_decision_update"):
        if source_status.get(key) != SOURCE_2375_READY_STATUS:
            errors.append(f"{key}_status_not_ready")
    for key in ("targeted_retest", "targeted_decision_update"):
        if source_status.get(key) != SOURCE_2376_READY_STATUS:
            errors.append(f"{key}_status_not_ready")
    if primary_candidate != PRIMARY_CANDIDATE_ID:
        errors.append("primary_candidate_not_dynamic_regime_overlay_v0_4_lower_turnover")
    if robustness_top != PRIMARY_CANDIDATE_ID:
        errors.append("robustness_top_from_2366_not_primary_candidate")
    if not ranking_top:
        errors.append("ranking_top_from_2365_missing")
    if decision_from_2375 != "OWNER_REVIEW_REQUIRED":
        errors.append("decision_from_2375_not_owner_review_required")
    if decision_from_2376 != DECISION_CONTINUE_OPTIMIZATION:
        errors.append("decision_from_2376_not_continue_optimization")
    if _as_mapping(targeted_retest).get("primary_execution_cadence") != (
        PRIMARY_EXECUTION_CADENCE
    ):
        errors.append("targeted_retest_primary_cadence_not_valid_until_window")
    if _as_mapping(event_retest).get("primary_execution_cadence") != (
        PRIMARY_EXECUTION_CADENCE
    ):
        errors.append("event_retest_primary_cadence_not_valid_until_window")
    if _as_mapping(sensitivity_result).get("primary_execution_cadence") != (
        PRIMARY_EXECUTION_CADENCE
    ):
        errors.append("sensitivity_primary_cadence_not_valid_until_window")
    if _as_mapping(optimization_review).get("primary_execution_cadence") != (
        PRIMARY_EXECUTION_CADENCE
    ):
        errors.append("optimization_primary_cadence_not_valid_until_window")
    if _as_mapping(targeted_retest).get("recommended_next_research_task") not in (
        SOURCE_2376_EXPECTED_ROUTE,
        None,
    ):
        errors.append("targeted_retest_next_route_not_trading_2377")
    if _as_mapping(optimization_review).get("recommended_next_research_task") not in (
        SOURCE_2375_EXPECTED_ROUTE,
        None,
    ):
        errors.append("optimization_next_route_not_trading_2376")
    monthly_rebalance = _as_mapping(_as_mapping(targeted_retest).get("monthly_rebalance"))
    if monthly_rebalance.get("allowed_for_primary_decision") is not False:
        errors.append("monthly_rebalance_allowed_for_primary_decision_not_false")
    decision_update = _targeted_decision_update(
        targeted_retest=targeted_retest,
        targeted_decision_update=targeted_decision_update,
    )
    if decision_update.get("decision_update_ready") is not True:
        errors.append("targeted_decision_update_not_ready")
    if decision_update.get("candidate_ready_for_research_only_observation") is True:
        errors.append("candidate_ready_for_research_only_observation_true")
    for source_name, source in (
        ("event_retest", event_retest),
        ("candidate_ranking", candidate_ranking),
        ("sensitivity_result", sensitivity_result),
        ("sensitivity_decision_update", sensitivity_decision_update),
        ("optimization_review", optimization_review),
        ("optimization_decision_update", optimization_decision_update),
        ("targeted_retest", targeted_retest),
        ("targeted_decision_update", targeted_decision_update),
    ):
        errors.extend(_side_effect_validation_errors(source_name, _as_mapping(source)))
    return errors


def _targeted_retest_summary(sources: Mapping[str, Any]) -> dict[str, Any]:
    targeted_retest = _as_mapping(sources["targeted_retest"])
    targeted_decision_update = _as_mapping(sources["targeted_decision_update"])
    decision = _targeted_decision_update(
        targeted_retest=targeted_retest,
        targeted_decision_update=targeted_decision_update,
    )
    summary = _as_mapping(
        _first_present(
            targeted_retest.get("summary_findings"),
            targeted_decision_update.get("summary_findings"),
        )
    )
    dynamic_vs_ranking_top_gap = _float(decision.get("dynamic_vs_ranking_top_gap"))
    return {
        "survives_realistic_cost": _yes(summary.get("candidate_survives_realistic_cost"))
        or _positive(decision.get("realistic_dynamic_vs_static_gap")),
        "survives_conservative_cost": _yes(
            summary.get("candidate_survives_conservative_cost")
        )
        or _positive(decision.get("conservative_dynamic_vs_static_gap")),
        "survives_harsh_cost": _positive(
            decision.get("harsh_dynamic_vs_static_gap")
        ),
        "time_slice_retest_insufficient": not _yes(
            summary.get("candidate_survives_time_slices")
        ),
        "regime_slice_retest_insufficient": not _yes(
            summary.get("candidate_survives_regime_slices")
        ),
        "return_gap_vs_ranking_top_remains": (
            dynamic_vs_ranking_top_gap is not None and dynamic_vs_ranking_top_gap < 0
        ),
        "valid_until_window_remains_necessary": _yes(
            summary.get("valid_until_window_remains_necessary")
        ),
        "lower_turnover_guardrail_has_actual_contribution": _yes(
            summary.get("lower_turnover_guardrail_has_actual_contribution")
        ),
        "candidate_ablation_supports_guardrails": _yes(
            summary.get("candidate_ablation_supports_guardrails")
        ),
        "candidate_ready_for_research_only_observation": bool(
            decision.get("candidate_ready_for_research_only_observation")
        ),
        "time_slice_pass_rate": decision.get("time_slice_pass_rate"),
        "regime_slice_pass_rate": decision.get("regime_slice_pass_rate"),
        "ablation_support_rate": decision.get("ablation_support_rate"),
        "realistic_dynamic_vs_static_gap": decision.get(
            "realistic_dynamic_vs_static_gap"
        ),
        "conservative_dynamic_vs_static_gap": decision.get(
            "conservative_dynamic_vs_static_gap"
        ),
        "harsh_dynamic_vs_static_gap": decision.get("harsh_dynamic_vs_static_gap"),
        "dynamic_vs_ranking_top_gap": decision.get("dynamic_vs_ranking_top_gap"),
        "decision_reasons": _as_list(decision.get("decision_reasons")),
    }


def _owner_review_decision_record(
    *,
    sources: Mapping[str, Any],
    targeted_summary: Mapping[str, Any],
    as_of_date: date,
    ready: bool,
) -> dict[str, Any]:
    return {
        "schema_version": "dynamic_strategy_targeted_retest_owner_decision.v1",
        "decision_id": f"{TASK_ID}_{as_of_date.isoformat()}",
        "generated_by_task": TASK_ID,
        "as_of": as_of_date.isoformat(),
        "source_tasks": list(SOURCE_TASKS),
        "source_2376_status": sources["source_status"].get("targeted_retest"),
        "source_2376_route_confirmed": (
            _as_mapping(sources["targeted_retest"]).get("recommended_next_research_task")
            == SOURCE_2376_EXPECTED_ROUTE
        ),
        "owner_review_decision_recorded": ready,
        "owner_decision": OWNER_DECISION,
        "decision_from_2376": sources["decision_from_2376"],
        "primary_candidate": sources["primary_candidate"],
        "ranking_top_from_2365": sources["ranking_top_from_2365"],
        "robustness_top_from_2366": sources["robustness_top_from_2366"],
        "research_only_observation_approved": False,
        "continue_optimization_approved": ready,
        "paper_shadow_approved": False,
        "event_append_approved": False,
        "outcome_binding_approved": False,
        "scheduler_approved": False,
        "production_approved": False,
        "broker_approved": False,
        "daily_report_approved": False,
        "non_approved_paths": list(NON_APPROVED_PATHS),
        "decision_reasons": _owner_decision_reasons(targeted_summary),
        "recommended_next_research_task": NEXT_ROUTE,
    }


def _continue_optimization_gate(
    *,
    sources: Mapping[str, Any],
    targeted_summary: Mapping[str, Any],
    owner_record: Mapping[str, Any],
    ready: bool,
) -> dict[str, Any]:
    return {
        "schema_version": "dynamic_strategy_continue_optimization_gate.v1",
        "status": "PASS" if ready else "BLOCKED_SOURCE_ARTIFACT",
        "source_task": TASK_ID,
        "primary_candidate": sources["primary_candidate"],
        "owner_decision": owner_record.get("owner_decision"),
        "continue_optimization_gate_ready": ready,
        "candidate_remains_worth_optimizing": ready,
        "research_only_observation_approved": False,
        "continue_optimization_approved": ready,
        "primary_execution_cadence": PRIMARY_EXECUTION_CADENCE,
        "optimization_focus": list(OPTIMIZATION_FOCUS),
        "blocking_issues": [
            "time_slice_retest_insufficient",
            "regime_slice_retest_insufficient",
            "return_gap_vs_ranking_top_remains",
        ],
        "targeted_retest_summary": dict(targeted_summary),
        "recommended_next_research_task": NEXT_ROUTE,
        "paper_shadow_enabled": False,
        "event_append_enabled": False,
        "outcome_binding_enabled": False,
        "scheduler_enabled": False,
        "production_enabled": False,
        "broker_action_enabled": False,
        "daily_report_generated": False,
    }


def _no_side_effect_evidence(
    *,
    owner_record: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "schema_version": (
            "dynamic_strategy_targeted_retest_owner_review_no_side_effect.v1"
        ),
        "status": "PASS",
        "decision_id": owner_record.get("decision_id"),
        "owner_decision": owner_record.get("owner_decision"),
        "owner_review_decision_only": True,
        "fresh_market_data_read": False,
        "backtest_run": False,
        "event_append_enabled": False,
        "event_append_approved": False,
        "event_append_attempted": False,
        "historical_event_log_mutated": False,
        "outcome_binding_enabled": False,
        "outcome_binding_approved": False,
        "outcome_binding_attempted": False,
        "outcome_store_mutated": False,
        "paper_shadow_enabled": False,
        "paper_shadow_approved": False,
        "paper_shadow_attempted": False,
        "paper_trade_created": False,
        "shadow_position_created": False,
        "scheduler_enabled": False,
        "scheduler_approved": False,
        "scheduler_attempted": False,
        "scheduled_task_created": False,
        "daily_report_generated": False,
        "production_enabled": False,
        "production_approved": False,
        "broker_action_enabled": False,
        "broker_action_attempted": False,
        "order_generated": False,
    }


def _base_payload(
    *,
    status: str,
    as_of_date: date,
    sources: Mapping[str, Any],
    targeted_summary: Mapping[str, Any],
    owner_record: Mapping[str, Any],
    continue_gate: Mapping[str, Any],
    evidence: Mapping[str, Any],
    ready: bool,
) -> dict[str, Any]:
    source_targeted = _as_mapping(sources["targeted_retest"])
    return {
        "task_id": TASK_ID,
        "task_register_id": TASK_REGISTER_ID,
        "report_type": REPORT_TYPE,
        "schema_version": SCHEMA_VERSION,
        "status": status,
        "generated_at": utc_now_iso(),
        "as_of": as_of_date.isoformat(),
        "source_tasks": list(SOURCE_TASKS),
        "source_artifacts": sources["source_artifacts"],
        "source_status": sources["source_status"],
        "source_ready_for_owner_review_decision": ready,
        "source_validation_errors": sources["source_validation_errors"],
        "primary_candidate": sources["primary_candidate"],
        "ranking_top_from_2365": sources["ranking_top_from_2365"],
        "robustness_top_from_2366": sources["robustness_top_from_2366"],
        "decision_from_2375": sources["decision_from_2375"],
        "decision_from_2376": sources["decision_from_2376"],
        "owner_review_decision_recorded": ready,
        "owner_decision": OWNER_DECISION,
        "research_only_observation_approved": False,
        "continue_optimization_approved": ready,
        "targeted_retest_summary": dict(targeted_summary),
        "optimization_focus": list(OPTIMIZATION_FOCUS),
        "primary_execution_cadence": PRIMARY_EXECUTION_CADENCE,
        "monthly_rebalance": {
            "allowed_for_reference": True,
            "allowed_for_primary_decision": False,
        },
        "market_regime": "ai_after_chatgpt",
        "market_regime_summary": AI_REGIME_SUMMARY,
        "requested_date_range": _as_mapping(source_targeted.get("requested_date_range")),
        "data_quality": _as_mapping(source_targeted.get("data_quality")),
        "data_quality_gate_executed": False,
        "data_quality_gate_reason": DATA_QUALITY_GATE_REASON,
        "research_only": True,
        "observe_only": True,
        "owner_review_decision_only": True,
        "manual_review_required": True,
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
        "owner_review_decision_record": dict(owner_record),
        "continue_optimization_gate": dict(continue_gate),
        "no_side_effect_evidence": dict(evidence),
        "recommended_next_research_task": NEXT_ROUTE,
        "next_route": NEXT_ROUTE,
        "summary_findings": {
            "owner_decision_recorded": ready,
            "owner_decision": OWNER_DECISION,
            "candidate_not_approved_for_observation": True,
            "candidate_remains_worth_optimizing": ready,
            "continue_optimization_approved": ready,
            "time_slice_retest_insufficient": targeted_summary.get(
                "time_slice_retest_insufficient"
            ),
            "regime_slice_retest_insufficient": targeted_summary.get(
                "regime_slice_retest_insufficient"
            ),
            "return_gap_vs_ranking_top_remains": targeted_summary.get(
                "return_gap_vs_ranking_top_remains"
            ),
            "valid_until_window_remains_default": True,
            "paper_shadow_remains_disallowed": True,
            "event_and_outcome_paths_remain_disabled": True,
            "scheduler_and_daily_report_remain_disabled": True,
            "broker_path_remains_disabled": True,
            "next_route": NEXT_ROUTE,
        },
        "required_outputs_ready": _required_outputs_ready(ready),
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
    artifact_paths = {
        "json_path": str(output_root / "owner_review_decision.json"),
        "owner_review_decision_json": str(output_root / "owner_review_decision.json"),
        "continue_optimization_gate_json": str(
            output_root / "continue_optimization_gate.json"
        ),
        "markdown_path": str(
            docs_root / "dynamic_strategy_targeted_retest_owner_review_decision.md"
        ),
        "continue_optimization_markdown": str(
            docs_root / "dynamic_strategy_continue_optimization_decision.md"
        ),
        "next_route_markdown": str(docs_root / "dynamic_strategy_2378_route.md"),
    }
    payload["artifact_paths"] = artifact_paths
    _write_json(Path(artifact_paths["json_path"]), payload)
    _write_json(
        Path(artifact_paths["continue_optimization_gate_json"]),
        {
            "report_type": "dynamic_strategy_continue_optimization_gate",
            "schema_version": "dynamic_strategy_continue_optimization_gate.v1",
            "status": payload["status"],
            "generated_at": payload["generated_at"],
            "continue_optimization_gate": payload["continue_optimization_gate"],
            "targeted_retest_summary": payload["targeted_retest_summary"],
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
    Path(artifact_paths["continue_optimization_markdown"]).write_text(
        _continue_optimization_markdown(payload),
        encoding="utf-8",
    )
    Path(artifact_paths["next_route_markdown"]).write_text(
        _route_markdown(payload),
        encoding="utf-8",
    )


def _main_markdown(payload: Mapping[str, Any]) -> str:
    summary = _as_mapping(payload.get("targeted_retest_summary"))
    return "\n".join(
        [
            "# 动态策略 targeted retest owner review decision",
            "",
            "## Executive summary",
            "",
            f"- status：`{payload.get('status')}`",
            f"- primary candidate：`{payload.get('primary_candidate')}`",
            f"- decision from 2376：`{payload.get('decision_from_2376')}`",
            f"- owner decision：`{payload.get('owner_decision')}`",
            (
                "- research-only observation approved："
                f"`{payload.get('research_only_observation_approved')}`"
            ),
            (
                "- continue optimization approved："
                f"`{payload.get('continue_optimization_approved')}`"
            ),
            f"- primary execution cadence：`{payload.get('primary_execution_cadence')}`",
            f"- next route：`{payload.get('recommended_next_research_task')}`",
            "",
            "## Source findings from TRADING-2376",
            "",
            (
                "- cost stress survived realistic / conservative / harsh："
                f"`{summary.get('survives_realistic_cost')}` / "
                f"`{summary.get('survives_conservative_cost')}` / "
                f"`{summary.get('survives_harsh_cost')}`"
            ),
            (
                "- time / regime slice insufficient："
                f"`{summary.get('time_slice_retest_insufficient')}` / "
                f"`{summary.get('regime_slice_retest_insufficient')}`"
            ),
            (
                "- return gap vs ranking top remains："
                f"`{summary.get('return_gap_vs_ranking_top_remains')}`"
            ),
            (
                "- valid_until_window remains necessary："
                f"`{summary.get('valid_until_window_remains_necessary')}`"
            ),
            "",
            "## Owner review decision",
            "",
            "- 不批准进入 research-only observation。",
            "- 不批准进入 paper-shadow。",
            "- 不批准 event append / outcome binding。",
            "- 不批准 scheduler / production / broker。",
            "- 批准继续优化，但只能在 research-only strategy research 范围内执行。",
            "",
            "## Why candidate is not approved for observation",
            "",
            "- time-slice retest 未穿越，样本稳定性不足。",
            "- regime-slice retest 未穿越，regime 稳健性不足。",
            "- 相对 TRADING-2365 ranking top 的 return gap 仍未修复。",
            "",
            "## Why candidate remains worth optimizing",
            "",
            "- realistic / conservative / harsh cost stress 下仍保留正 gap。",
            "- lower-turnover guardrail 和 valid_until_window 仍有研究价值。",
            "- 风险控制表现优于简单收益排名候选，但需要修复收益差距。",
            "",
            "## Optimization focus areas",
            "",
            "\n".join(f"- `{item}`" for item in _as_list(payload.get("optimization_focus"))),
            "",
            "## Explicit non-approval list",
            "",
            "\n".join(f"- `{item}`" for item in NON_APPROVED_PATHS),
            "",
            "## Guardrail summary",
            "",
            f"- data quality gate：`{payload.get('data_quality_gate_reason')}`",
            "- 本任务不读取 fresh market data，不重新 backtest，不生成 daily report。",
            (
                "- paper-shadow、event append、outcome binding、scheduler、"
                "production、broker/order 全部 false / none。"
            ),
            "",
            "## Recommended next route",
            "",
            f"`{payload.get('recommended_next_research_task')}`",
        ]
    )


def _continue_optimization_markdown(payload: Mapping[str, Any]) -> str:
    gate = _as_mapping(payload.get("continue_optimization_gate"))
    summary = _as_mapping(payload.get("targeted_retest_summary"))
    return "\n".join(
        [
            "# 动态策略 continue optimization decision",
            "",
            f"- status：`{payload.get('status')}`",
            f"- owner decision：`{payload.get('owner_decision')}`",
            f"- gate ready：`{gate.get('continue_optimization_gate_ready')}`",
            f"- primary candidate：`{payload.get('primary_candidate')}`",
            f"- primary execution cadence：`{payload.get('primary_execution_cadence')}`",
            "",
            "## Optimization gate",
            "",
            (
                "- candidate remains worth optimizing："
                f"`{gate.get('candidate_remains_worth_optimizing')}`"
            ),
            (
                "- research-only observation approved："
                f"`{gate.get('research_only_observation_approved')}`"
            ),
            f"- continue optimization approved：`{gate.get('continue_optimization_approved')}`",
            "",
            "## Blocking issues",
            "",
            "\n".join(f"- `{item}`" for item in _as_list(gate.get("blocking_issues"))),
            "",
            "## Targeted retest evidence",
            "",
            f"- time slice pass rate：`{summary.get('time_slice_pass_rate')}`",
            f"- regime slice pass rate：`{summary.get('regime_slice_pass_rate')}`",
            f"- ablation support rate：`{summary.get('ablation_support_rate')}`",
            f"- dynamic vs ranking top gap：`{summary.get('dynamic_vs_ranking_top_gap')}`",
            "",
            "## Next optimization focus",
            "",
            "\n".join(f"- `{item}`" for item in _as_list(gate.get("optimization_focus"))),
        ]
    )


def _route_markdown(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# TRADING-2378 route",
            "",
            f"- current status：`{payload.get('status')}`",
            f"- primary candidate：`{payload.get('primary_candidate')}`",
            f"- owner decision：`{payload.get('owner_decision')}`",
            f"- next route：`{payload.get('recommended_next_research_task')}`",
            (
                "- route boundary：slice robustness and return-gap optimization "
                "plan；不是 research-only observation、paper-shadow、scheduler、"
                "production、broker 或 daily report approval。"
            ),
        ]
    )


def _owner_decision_reasons(targeted_summary: Mapping[str, Any]) -> list[str]:
    reasons = [
        "2376 targeted retest returned CONTINUE_OPTIMIZATION",
        "realistic/conservative/harsh cost stress remains positive",
        "time-slice retest evidence is insufficient",
        "regime-slice retest evidence is insufficient",
        "return gap vs 2365 ranking top remains unresolved",
        "valid_until_window remains the default execution cadence",
    ]
    reasons.extend(str(item) for item in _as_list(targeted_summary.get("decision_reasons")))
    return reasons


def _side_effect_validation_errors(
    source_name: str,
    source: Mapping[str, Any],
) -> list[str]:
    errors: list[str] = []
    for field in _side_effect_false_fields():
        if bool(source.get(field)):
            errors.append(f"{source_name}_{field}_true")
    for field in (
        "paper_shadow_allowed",
        "production_allowed",
        "promotion_allowed",
        "research_only_observation_approved",
        "paper_shadow_approved",
    ):
        if source.get(field) is True:
            errors.append(f"{source_name}_{field}_true")
    if source.get("broker_action") not in (None, "none"):
        errors.append(f"{source_name}_broker_action_not_none")
    if source.get("production_effect") not in (None, "none"):
        errors.append(f"{source_name}_production_effect_not_none")
    return errors


def _side_effect_false_fields() -> tuple[str, ...]:
    return (
        "paper_shadow_enabled",
        "paper_shadow_attempted",
        "paper_trade_created",
        "shadow_position_created",
        "event_append_enabled",
        "event_append_attempted",
        "historical_event_log_mutated",
        "outcome_binding_enabled",
        "outcome_binding_attempted",
        "outcome_store_mutated",
        "scheduler_enabled",
        "scheduler_attempted",
        "scheduled_task_created",
        "production_enabled",
        "broker_action_enabled",
        "broker_action_attempted",
        "order_generated",
        "daily_report_generated",
    )


def _required_outputs_ready(ready: bool) -> dict[str, bool]:
    return {
        "owner_review_decision": ready,
        "continue_optimization_gate": ready,
        "targeted_retest_summary": ready,
        "optimization_focus": ready,
        "research_only_observation_approved_false": ready,
        "continue_optimization_approved_true": ready,
        "paper_shadow_enabled_false": ready,
        "paper_trade_created_false": ready,
        "shadow_position_created_false": ready,
        "event_append_enabled_false": ready,
        "outcome_binding_enabled_false": ready,
        "scheduler_enabled_false": ready,
        "production_enabled_false": ready,
        "broker_action_enabled_false": ready,
        "daily_report_generated_false": ready,
        "next_route": ready,
    }


def _primary_candidate(targeted_retest: Any, optimization_review: Any) -> str:
    return str(
        _first_present(
            _as_mapping(targeted_retest).get("primary_candidate"),
            _as_mapping(optimization_review).get("best_candidate_after_optimization"),
            PRIMARY_CANDIDATE_ID,
        )
    )


def _ranking_top(
    *,
    event_retest: Any,
    candidate_ranking: Any,
    optimization_review: Any,
    optimization_decision_update: Any,
    targeted_retest: Any,
) -> str:
    decision_update = _as_mapping(
        _as_mapping(optimization_decision_update).get("candidate_decision_update")
    )
    return str(
        _first_present(
            _as_mapping(targeted_retest).get("ranking_top_from_2365"),
            _as_mapping(optimization_review).get("ranking_top_from_2365"),
            decision_update.get("ranking_top_from_2365"),
            _extract_ranked_candidate(
                _as_list(_as_mapping(candidate_ranking).get("candidate_ranking")),
                rank_field="rank",
            ),
            _extract_ranked_candidate(
                _as_list(_as_mapping(event_retest).get("candidate_ranking")),
                rank_field="rank",
            ),
            RANKING_TOP_FALLBACK,
        )
    )


def _robustness_top(
    *,
    sensitivity_result: Any,
    sensitivity_decision_update: Any,
    optimization_review: Any,
    optimization_decision_update: Any,
    targeted_retest: Any,
) -> str:
    optimization_decision = _as_mapping(
        _as_mapping(optimization_decision_update).get("candidate_decision_update")
    )
    sensitivity_decision = _as_mapping(
        _as_mapping(sensitivity_decision_update).get("decision_update")
    )
    return str(
        _first_present(
            _as_mapping(targeted_retest).get("robustness_top_from_2366"),
            _as_mapping(optimization_review).get("robustness_top_from_2366"),
            optimization_decision.get("robustness_top_from_2366"),
            sensitivity_decision.get("top_candidate_after_sensitivity"),
            _extract_ranked_candidate(
                _as_list(_as_mapping(sensitivity_result).get("robustness_ranking")),
                rank_field="robust_rank",
            ),
            PRIMARY_CANDIDATE_ID,
        )
    )


def _decision_from_2375(
    *,
    optimization_review: Any,
    optimization_decision_update: Any,
    targeted_retest: Any,
) -> str:
    decision_update = _as_mapping(
        _as_mapping(optimization_decision_update).get("candidate_decision_update")
    )
    return str(
        _first_present(
            _as_mapping(targeted_retest).get("decision_from_2375"),
            _as_mapping(optimization_review).get(
                "recommended_decision_after_optimization"
            ),
            decision_update.get("recommended_decision_after_optimization"),
            "",
        )
    )


def _decision_from_2376(
    *,
    targeted_retest: Any,
    targeted_decision_update: Any,
) -> str:
    decision = _targeted_decision_update(
        targeted_retest=targeted_retest,
        targeted_decision_update=targeted_decision_update,
    )
    return str(
        _first_present(
            _as_mapping(targeted_retest).get("candidate_decision_after_targeted_retest"),
            decision.get("candidate_decision_after_targeted_retest"),
            "",
        )
    )


def _targeted_decision_update(
    *,
    targeted_retest: Any,
    targeted_decision_update: Any,
) -> Mapping[str, Any]:
    return _as_mapping(
        _first_present(
            _as_mapping(targeted_retest).get("decision_update"),
            _as_mapping(targeted_decision_update).get("decision_update"),
        )
    )


def _extract_ranked_candidate(rows: list[Any], *, rank_field: str) -> str | None:
    best_row: Mapping[str, Any] | None = None
    best_rank: float | None = None
    for row in rows:
        item = _as_mapping(row)
        candidate_id = item.get("candidate_id")
        rank = _float(item.get(rank_field))
        if not candidate_id or rank is None:
            continue
        if best_rank is None or rank < best_rank:
            best_rank = rank
            best_row = item
    if best_row is None:
        return None
    return str(best_row.get("candidate_id"))


def _source_artifact(path: Path, payload: Any) -> dict[str, Any]:
    return {
        "path": str(path),
        "status": _as_mapping(payload).get("status"),
        "sha256": _file_sha256(path),
        "size_bytes": path.stat().st_size,
    }


def _resolve_as_of(as_of_date: date | None, source: Mapping[str, Any]) -> date:
    if as_of_date is not None:
        return as_of_date
    raw = source.get("as_of")
    if isinstance(raw, str):
        try:
            return date.fromisoformat(raw[:10])
        except ValueError:
            pass
    return date.today()


def _load_json_document(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )


def _as_mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _as_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _first_present(*values: Any) -> Any:
    for value in values:
        if value is not None:
            return value
    return None


def _yes(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).upper() == "YES"


def _positive(value: Any) -> bool:
    numeric = _float(value)
    return numeric is not None and numeric > 0


def _float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None
