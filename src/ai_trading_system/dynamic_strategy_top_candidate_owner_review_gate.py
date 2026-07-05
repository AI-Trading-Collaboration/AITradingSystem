from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from datetime import date
from pathlib import Path
from typing import Any

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.data_foundation import utc_now_iso
from ai_trading_system.dynamic_strategy_cost_turnover_cooldown_sensitivity import (
    CURRENT_DYNAMIC_DEFAULT_CANDIDATE_ID,
    DEFAULT_DYNAMIC_STRATEGY_COST_TURNOVER_COOLDOWN_SENSITIVITY_OUTPUT_ROOT,
    DEFAULT_SOURCE_CADENCE_MATRIX_PATH,
    DEFAULT_SOURCE_CANDIDATE_RANKING_PATH,
    DEFAULT_SOURCE_EVENT_DRIVEN_RETEST_PATH,
)
from ai_trading_system.dynamic_strategy_cost_turnover_cooldown_sensitivity import (
    NEXT_ROUTE as SOURCE_SENSITIVITY_EXPECTED_NEXT_ROUTE,
)
from ai_trading_system.dynamic_strategy_cost_turnover_cooldown_sensitivity import (
    READY_STATUS as SOURCE_SENSITIVITY_READY_STATUS,
)
from ai_trading_system.dynamic_strategy_event_driven_retest import (
    PRIMARY_EXECUTION_CADENCE,
)
from ai_trading_system.dynamic_strategy_event_driven_retest import (
    READY_STATUS as SOURCE_RETEST_READY_STATUS,
)
from ai_trading_system.execution_semantics import (
    AI_REGIME_SUMMARY,
    DEFAULT_AI_REGIME_BACKTEST_START,
    _file_sha256,
)

TASK_ID = "TRADING-2367"
TASK_REGISTER_ID = (
    "TRADING-2367_DYNAMIC_STRATEGY_TOP_CANDIDATE_OWNER_REVIEW_AND_SHADOW_RESEARCH_GATE"
)
REPORT_TYPE = "dynamic_strategy_top_candidate_owner_review_gate"
SCHEMA_VERSION = "dynamic_strategy_top_candidate_owner_review_gate.v1"
READY_STATUS = (
    "DYNAMIC_STRATEGY_TOP_CANDIDATE_OWNER_REVIEW_AND_SHADOW_RESEARCH_GATE_READY"
)
BLOCKED_SOURCE_STATUS = (
    "DYNAMIC_STRATEGY_TOP_CANDIDATE_OWNER_REVIEW_AND_SHADOW_RESEARCH_GATE_BLOCKED_SOURCE_ARTIFACT"
)
NEXT_ROUTE = "TRADING-2368_Dynamic_Strategy_Research_Only_Shadow_Observation_Protocol"

DECISION_ACCEPT_RESEARCH_ONLY = "ACCEPT_FOR_RESEARCH_ONLY_SHADOW_OBSERVATION"
DECISION_OWNER_REVIEW = "OWNER_REVIEW_REQUIRED"
DECISION_CONTINUE_RESEARCH = "CONTINUE_RESEARCH"
DECISION_REJECT = "REJECT_FOR_NOW"
DECISION_DEPRECATED = "DEPRECATED_BY_SENSITIVITY"

STATIC_BASELINE_CANDIDATE_ID = "static_baseline"

DEFAULT_DYNAMIC_STRATEGY_TOP_CANDIDATE_OWNER_REVIEW_GATE_OUTPUT_ROOT = (
    PROJECT_ROOT / "outputs" / "research_strategies" / REPORT_TYPE
)
DEFAULT_DYNAMIC_STRATEGY_TOP_CANDIDATE_OWNER_REVIEW_GATE_DOCS_ROOT = (
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


def run_dynamic_strategy_top_candidate_owner_review_gate(
    *,
    source_event_retest_path: Path = DEFAULT_SOURCE_EVENT_DRIVEN_RETEST_PATH,
    source_candidate_ranking_path: Path = DEFAULT_SOURCE_CANDIDATE_RANKING_PATH,
    source_cadence_matrix_path: Path = DEFAULT_SOURCE_CADENCE_MATRIX_PATH,
    source_sensitivity_result_path: Path = DEFAULT_SOURCE_SENSITIVITY_RESULT_PATH,
    source_sensitivity_matrix_path: Path = DEFAULT_SOURCE_SENSITIVITY_MATRIX_PATH,
    source_decision_update_path: Path = DEFAULT_SOURCE_DECISION_UPDATE_PATH,
    output_root: Path = DEFAULT_DYNAMIC_STRATEGY_TOP_CANDIDATE_OWNER_REVIEW_GATE_OUTPUT_ROOT,
    docs_root: Path = DEFAULT_DYNAMIC_STRATEGY_TOP_CANDIDATE_OWNER_REVIEW_GATE_DOCS_ROOT,
    as_of_date: date | None = None,
) -> dict[str, Any]:
    sources = _load_sources(
        source_event_retest_path=source_event_retest_path,
        source_candidate_ranking_path=source_candidate_ranking_path,
        source_cadence_matrix_path=source_cadence_matrix_path,
        source_sensitivity_result_path=source_sensitivity_result_path,
        source_sensitivity_matrix_path=source_sensitivity_matrix_path,
        source_decision_update_path=source_decision_update_path,
    )
    resolved_as_of = _resolve_as_of(as_of_date, sources)
    if not bool(sources["ready_for_owner_review_gate"]):
        payload = _base_payload(
            status=BLOCKED_SOURCE_STATUS,
            as_of_date=resolved_as_of,
            sources=sources,
        )
        payload.update(
            {
                "owner_review_required": True,
                "candidate_review_comparison_ready": False,
                "candidate_review_comparison": [],
                "owner_review_checklist": _blocked_owner_checklist(sources),
                "shadow_research_gate_decision_ready": False,
                "shadow_research_gate_decision": _blocked_gate_decision(sources),
                "recommended_gate_candidate": None,
                "recommended_gate_decision": DECISION_OWNER_REVIEW,
                "research_only_shadow_observation_allowed": False,
                "summary_findings": _blocked_summary_findings(),
                "required_outputs_ready": _required_outputs_ready(False),
            }
        )
        _write_outputs(payload=payload, output_root=output_root, docs_root=docs_root)
        return payload

    comparison = _build_candidate_review_comparison(sources)
    ranking_top = str(sources["ranking_top_from_2365"])
    robustness_top = str(sources["robustness_top_from_2366"])
    divergence_detected = ranking_top != robustness_top
    recommended_candidate = _recommended_candidate(
        comparison=comparison,
        ranking_top=ranking_top,
        robustness_top=robustness_top,
    )
    recommended_row = _row_by_candidate(comparison, recommended_candidate)
    recommended_decision = str(
        recommended_row.get("recommended_gate_decision", DECISION_OWNER_REVIEW)
        if recommended_row
        else DECISION_OWNER_REVIEW
    )
    owner_review_required = bool(
        divergence_detected or recommended_decision == DECISION_OWNER_REVIEW
    )
    research_only_allowed = recommended_decision in {
        DECISION_ACCEPT_RESEARCH_ONLY,
        DECISION_OWNER_REVIEW,
    }
    gate_decision = _shadow_research_gate_decision(
        sources=sources,
        comparison=comparison,
        recommended_candidate=recommended_candidate,
        recommended_decision=recommended_decision,
        owner_review_required=owner_review_required,
        research_only_allowed=research_only_allowed,
    )
    summary_findings = _summary_findings(
        ranking_top=ranking_top,
        robustness_top=robustness_top,
        recommended_candidate=recommended_candidate,
        recommended_decision=recommended_decision,
        owner_review_required=owner_review_required,
        research_only_allowed=research_only_allowed,
        comparison=comparison,
    )

    payload = _base_payload(
        status=READY_STATUS,
        as_of_date=resolved_as_of,
        sources=sources,
    )
    payload.update(
        {
            "ranking_top_from_2365": ranking_top,
            "ranking_top_decision_from_2365": sources[
                "ranking_top_decision_from_2365"
            ],
            "robustness_top_from_2366": robustness_top,
            "robustness_top_decision_after_2366": sources[
                "robustness_top_decision_after_2366"
            ],
            "ranking_robustness_divergence_detected": divergence_detected,
            "owner_review_required": owner_review_required,
            "candidate_review_comparison_ready": True,
            "candidate_review_comparison": comparison,
            "owner_review_checklist": _owner_review_checklist(
                sources=sources,
                comparison=comparison,
                recommended_candidate=recommended_candidate,
                owner_review_required=owner_review_required,
            ),
            "shadow_research_gate_decision_ready": True,
            "shadow_research_gate_decision": gate_decision,
            "recommended_gate_candidate": recommended_candidate,
            "recommended_gate_decision": recommended_decision,
            "recommended_next_research_task": NEXT_ROUTE,
            "research_only_shadow_observation_allowed": research_only_allowed,
            "research_only_shadow_observation_requires_owner_review": (
                owner_review_required
            ),
            "summary_findings": summary_findings,
            "required_outputs_ready": _required_outputs_ready(True),
        }
    )
    _write_outputs(payload=payload, output_root=output_root, docs_root=docs_root)
    return payload


def _base_payload(
    *,
    status: str,
    as_of_date: date,
    sources: Mapping[str, Any],
) -> dict[str, Any]:
    source_data_quality = _source_data_quality(sources)
    requested_range = _requested_date_range(sources)
    return {
        "task_id": TASK_ID,
        "task_register_id": TASK_REGISTER_ID,
        "report_type": REPORT_TYPE,
        "schema_version": SCHEMA_VERSION,
        "status": status,
        "generated_at": utc_now_iso(),
        "as_of": as_of_date.isoformat(),
        "source_tasks": ["TRADING-2365", "TRADING-2366"],
        "source_artifacts": sources["source_artifacts"],
        "source_status": sources["source_status"],
        "source_ready_for_owner_review_gate": sources[
            "ready_for_owner_review_gate"
        ],
        "source_validation_errors": sources["source_validation_errors"],
        "primary_execution_cadence": PRIMARY_EXECUTION_CADENCE,
        "market_regime": "ai_after_chatgpt",
        "market_regime_summary": AI_REGIME_SUMMARY,
        "requested_date_range": requested_range,
        "data_quality": source_data_quality,
        "data_quality_gate_executed": False,
        "data_quality_gate_reason": (
            "NOT_APPLICABLE_PRIOR_ARTIFACT_REVIEW_ONLY_NO_FRESH_MARKET_DATA"
        ),
        "research_only": True,
        "observe_only": True,
        "manual_review_required": True,
        "promotion_allowed": False,
        "paper_shadow_allowed": False,
        "paper_shadow_enabled": False,
        "paper_shadow_attempted": False,
        "paper_trade_created": False,
        "shadow_position_created": False,
        "scheduler_enabled": False,
        "scheduler_attempted": False,
        "event_append_enabled": False,
        "event_append_attempted": False,
        "outcome_binding_enabled": False,
        "outcome_binding_attempted": False,
        "outcome_store_mutated": False,
        "production_allowed": False,
        "production_enabled": False,
        "production_effect": "none",
        "broker_action": "none",
        "broker_action_enabled": False,
        "broker_action_attempted": False,
        "daily_report_generated": False,
        "next_route": NEXT_ROUTE,
    }


def _load_sources(
    *,
    source_event_retest_path: Path,
    source_candidate_ranking_path: Path,
    source_cadence_matrix_path: Path,
    source_sensitivity_result_path: Path,
    source_sensitivity_matrix_path: Path,
    source_decision_update_path: Path,
) -> dict[str, Any]:
    event_retest = _load_json_document(source_event_retest_path)
    candidate_ranking = _load_json_document(source_candidate_ranking_path)
    cadence_matrix = _load_json_document(source_cadence_matrix_path)
    sensitivity_result = _load_json_document(source_sensitivity_result_path)
    sensitivity_matrix = _load_json_document(source_sensitivity_matrix_path)
    decision_update = _load_json_document(source_decision_update_path)

    ranking_rows = _as_list(
        _first_present(
            _as_mapping(candidate_ranking).get("candidate_ranking"),
            _as_mapping(event_retest).get("candidate_ranking"),
        )
    )
    cadence_rows = _as_list(
        _first_present(
            _as_mapping(cadence_matrix).get("cadence_comparison_matrix"),
            _as_mapping(event_retest).get("cadence_comparison_matrix"),
        )
    )
    sensitivity_rows = _as_list(
        _first_present(
            _as_mapping(sensitivity_matrix).get("sensitivity_matrix"),
            _as_mapping(sensitivity_result).get("sensitivity_matrix"),
        )
    )
    decision_update_payload = _as_mapping(
        _first_present(
            _as_mapping(decision_update).get("decision_update"),
            _as_mapping(sensitivity_result).get("decision_update"),
        )
    )
    robustness_rows = _as_list(
        _first_present(
            decision_update_payload.get("robustness_ranking"),
            _as_mapping(sensitivity_result).get("robustness_ranking"),
        )
    )

    ranking_top_row = _ranking_top_row(ranking_rows)
    ranking_top = str(ranking_top_row.get("candidate_id", "")) if ranking_top_row else ""
    robustness_top = str(
        _first_present(
            decision_update_payload.get("top_candidate_after_sensitivity"),
            _as_mapping(sensitivity_result)
            .get("summary", {})
            .get("top_candidate_after_sensitivity")
            if isinstance(_as_mapping(sensitivity_result).get("summary"), Mapping)
            else None,
            robustness_rows[0].get("candidate_id") if robustness_rows else None,
        )
        or ""
    )
    source_status = {
        "event_retest": _as_mapping(event_retest).get("status"),
        "candidate_ranking": _as_mapping(candidate_ranking).get("status"),
        "cadence_matrix": _as_mapping(cadence_matrix).get("status"),
        "sensitivity_result": _as_mapping(sensitivity_result).get("status"),
        "sensitivity_matrix": _as_mapping(sensitivity_matrix).get("status"),
        "decision_update": _as_mapping(decision_update).get("status"),
    }
    validation_errors = _source_validation_errors(
        source_status=source_status,
        sensitivity_result=sensitivity_result,
        ranking_rows=ranking_rows,
        robustness_rows=robustness_rows,
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
        "decision_update_payload": decision_update_payload,
        "ranking_rows": ranking_rows,
        "cadence_rows": cadence_rows,
        "sensitivity_rows": sensitivity_rows,
        "robustness_rows": robustness_rows,
        "ranking_top_from_2365": ranking_top,
        "ranking_top_decision_from_2365": ranking_top_row.get("decision")
        if ranking_top_row
        else None,
        "robustness_top_from_2366": robustness_top,
        "robustness_top_decision_after_2366": _robust_decision(
            robustness_rows, robustness_top
        ),
        "source_status": source_status,
        "source_validation_errors": validation_errors,
        "ready_for_owner_review_gate": not validation_errors,
        "source_artifacts": {
            "event_retest": _source_artifact(source_event_retest_path, event_retest),
            "candidate_ranking": _source_artifact(
                source_candidate_ranking_path, candidate_ranking
            ),
            "cadence_matrix": _source_artifact(
                source_cadence_matrix_path, cadence_matrix
            ),
            "sensitivity_result": _source_artifact(
                source_sensitivity_result_path, sensitivity_result
            ),
            "sensitivity_matrix": _source_artifact(
                source_sensitivity_matrix_path, sensitivity_matrix
            ),
            "decision_update": _source_artifact(
                source_decision_update_path, decision_update
            ),
        },
    }


def _source_validation_errors(
    *,
    source_status: Mapping[str, Any],
    sensitivity_result: Any,
    ranking_rows: Sequence[Any],
    robustness_rows: Sequence[Any],
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
    if _as_mapping(sensitivity_result).get("next_route") not in {
        SOURCE_SENSITIVITY_EXPECTED_NEXT_ROUTE,
        None,
    }:
        errors.append("sensitivity_result_next_route_not_trading_2367")
    if not ranking_rows:
        errors.append("candidate_ranking_empty")
    if not robustness_rows:
        errors.append("robustness_ranking_empty")
    if not ranking_top:
        errors.append("ranking_top_from_2365_missing")
    if not robustness_top:
        errors.append("robustness_top_from_2366_missing")
    return errors


def _build_candidate_review_comparison(
    sources: Mapping[str, Any],
) -> list[dict[str, Any]]:
    ranking_rows = [_as_mapping(row) for row in _as_list(sources["ranking_rows"])]
    cadence_rows = [_as_mapping(row) for row in _as_list(sources["cadence_rows"])]
    sensitivity_rows = [
        _as_mapping(row) for row in _as_list(sources["sensitivity_rows"])
    ]
    robustness_rows = [
        _as_mapping(row) for row in _as_list(sources["robustness_rows"])
    ]
    ranking_by_candidate = {
        str(row.get("candidate_id")): row
        for row in ranking_rows
        if row.get("candidate_id")
    }
    robustness_by_candidate = {
        str(row.get("candidate_id")): row
        for row in robustness_rows
        if row.get("candidate_id")
    }
    primary_cadence_by_candidate = _primary_cadence_rows(cadence_rows)
    realistic_by_candidate = _sensitivity_rows_by_scenario(
        sensitivity_rows, "combined_realistic"
    )
    harsh_by_candidate = _sensitivity_rows_by_scenario(
        sensitivity_rows, "combined_harsh"
    )
    candidate_ids = _candidate_ids(
        sources=sources,
        ranking_rows=ranking_rows,
        robustness_rows=robustness_rows,
        sensitivity_rows=sensitivity_rows,
    )
    static_row = primary_cadence_by_candidate.get(STATIC_BASELINE_CANDIDATE_ID, {})
    rows: list[dict[str, Any]] = []
    for candidate_id in candidate_ids:
        ranking_row = ranking_by_candidate.get(candidate_id, {})
        robustness_row = robustness_by_candidate.get(candidate_id, {})
        primary_row = primary_cadence_by_candidate.get(candidate_id, {})
        realistic_row = realistic_by_candidate.get(candidate_id, {})
        harsh_row = harsh_by_candidate.get(candidate_id, {})
        row = _candidate_review_row(
            candidate_id=candidate_id,
            ranking_row=ranking_row,
            robustness_row=robustness_row,
            primary_row=primary_row,
            realistic_row=realistic_row,
            harsh_row=harsh_row,
            static_row=static_row,
            sources=sources,
        )
        rows.append(row)
    return sorted(
        rows,
        key=lambda item: (
            _sort_rank(item.get("review_priority_rank")),
            _sort_rank(item.get("ranking_rank_from_2365")),
            str(item.get("candidate_id")),
        ),
    )


def _candidate_review_row(
    *,
    candidate_id: str,
    ranking_row: Mapping[str, Any],
    robustness_row: Mapping[str, Any],
    primary_row: Mapping[str, Any],
    realistic_row: Mapping[str, Any],
    harsh_row: Mapping[str, Any],
    static_row: Mapping[str, Any],
    sources: Mapping[str, Any],
) -> dict[str, Any]:
    roles = _candidate_roles(candidate_id, sources)
    realistic_cost = _as_mapping(realistic_row.get("cost_metrics"))
    realistic_perf = _as_mapping(realistic_row.get("performance_metrics"))
    realistic_turnover = _as_mapping(realistic_row.get("turnover_metrics"))
    realistic_cooldown = _as_mapping(realistic_row.get("cooldown_metrics"))
    realistic_robustness = _as_mapping(realistic_row.get("robustness_metrics"))
    harsh_cost = _as_mapping(harsh_row.get("cost_metrics"))
    is_static = candidate_id == STATIC_BASELINE_CANDIDATE_ID
    dynamic_vs_static_gap = _first_number(
        robustness_row.get("realistic_dynamic_vs_static_gap"),
        realistic_cost.get("cost_adjusted_dynamic_vs_static_gap"),
        ranking_row.get("dynamic_vs_static_gap"),
        primary_row.get("dynamic_vs_static_annualized_return_gap"),
        0.0 if is_static else None,
    )
    total_return = _first_number(
        realistic_perf.get("total_return"),
        primary_row.get("total_return"),
        harsh_cost.get("gross_return"),
    )
    max_drawdown = _first_number(
        robustness_row.get("realistic_max_drawdown"),
        realistic_perf.get("max_drawdown"),
        primary_row.get("max_drawdown"),
    )
    turnover = _first_number(
        robustness_row.get("realistic_turnover"),
        realistic_turnover.get("turnover"),
        ranking_row.get("turnover"),
        primary_row.get("turnover"),
        0.0 if is_static else None,
    )
    survives_realistic = _bool_or_none(
        robustness_row.get("survives_realistic_cost")
    )
    survives_conservative = _bool_or_none(
        robustness_row.get("survives_conservative_cost")
    )
    survives_harsh = _bool_or_none(robustness_row.get("survives_harsh_cost"))
    gate_decision, gate_reason = _candidate_gate_decision(
        candidate_id=candidate_id,
        ranking_row=ranking_row,
        robustness_row=robustness_row,
        dynamic_vs_static_gap=dynamic_vs_static_gap,
        survives_realistic=survives_realistic,
        survives_conservative=survives_conservative,
        is_static=is_static,
        sources=sources,
    )
    return {
        "candidate_id": candidate_id,
        "roles": roles,
        "review_priority_rank": _review_priority_rank(candidate_id, sources),
        "total_return": total_return,
        "cost_adjusted_return": _first_number(
            robustness_row.get("realistic_cost_adjusted_return"),
            realistic_cost.get("cost_adjusted_return"),
            ranking_row.get("cost_adjusted_return"),
            primary_row.get("cost_adjusted_return"),
        ),
        "dynamic_vs_static_gap": dynamic_vs_static_gap,
        "max_drawdown": max_drawdown,
        "drawdown_vs_static_gap": _first_number(
            primary_row.get("dynamic_vs_static_drawdown_gap"),
            _drawdown_gap(max_drawdown, _first_number(static_row.get("max_drawdown"))),
            0.0 if is_static else None,
        ),
        "volatility": _first_number(
            realistic_perf.get("volatility"), primary_row.get("volatility")
        ),
        "turnover": turnover,
        "rebalance_count": _first_number(
            realistic_turnover.get("rebalance_count"),
            ranking_row.get("rebalance_count"),
            primary_row.get("rebalance_count"),
        ),
        "average_holding_days": _first_number(
            realistic_turnover.get("average_holding_days"),
            primary_row.get("average_holding_days"),
        ),
        "transaction_cost_drag": _first_number(
            realistic_cost.get("transaction_cost_drag"), 0.0 if is_static else None
        ),
        "slippage_drag": _first_number(
            realistic_cost.get("slippage_drag"), 0.0 if is_static else None
        ),
        "cooldown_block_count": _first_number(
            realistic_cooldown.get("cooldown_block_count"),
            ranking_row.get("cooldown_block_count"),
            primary_row.get("cooldown_block_count"),
        ),
        "cooldown_fragility": robustness_row.get("cooldown_fragility")
        or ("NOT_APPLICABLE_STATIC_BASELINE" if is_static else "UNKNOWN"),
        "survives_realistic_cost": survives_realistic,
        "survives_conservative_cost": survives_conservative,
        "survives_harsh_cost": survives_harsh,
        "constraint_hit_count": _first_number(
            realistic_robustness.get("constraint_hit_count"),
            ranking_row.get("constraint_hit_count"),
            primary_row.get("constraint_hit_count"),
        ),
        "stale_signal_count": _first_number(
            ranking_row.get("stale_signal_count"),
            primary_row.get("stale_signal_count"),
            realistic_robustness.get("stale_signal_execution_count"),
        ),
        "false_risk_off_count": _first_number(
            ranking_row.get("false_risk_off_count"),
            primary_row.get("false_risk_off_count"),
        ),
        "missed_upside_count": _first_number(
            ranking_row.get("missed_upside_count"),
            primary_row.get("missed_upside_count"),
        ),
        "robustness_rank": _first_number(robustness_row.get("robust_rank")),
        "ranking_rank_from_2365": _first_number(ranking_row.get("rank")),
        "decision_from_2365": ranking_row.get("decision")
        or ("STATIC_BASELINE_REFERENCE" if is_static else "NOT_IN_2365_RANKING"),
        "decision_after_2366": robustness_row.get("decision_update")
        or ("STATIC_BASELINE_REFERENCE" if is_static else "NOT_IN_2366_SENSITIVITY"),
        "turnover_acceptable_after_2366": _bool_or_none(
            robustness_row.get("turnover_acceptable")
        ),
        "fragility_reason": robustness_row.get("fragility_reason"),
        "recommended_gate_decision": gate_decision,
        "recommended_gate_reason": gate_reason,
        "eligible_for_research_only_shadow_observation": gate_decision
        in {DECISION_ACCEPT_RESEARCH_ONLY, DECISION_OWNER_REVIEW},
    }


def _candidate_gate_decision(
    *,
    candidate_id: str,
    ranking_row: Mapping[str, Any],
    robustness_row: Mapping[str, Any],
    dynamic_vs_static_gap: float | None,
    survives_realistic: bool | None,
    survives_conservative: bool | None,
    is_static: bool,
    sources: Mapping[str, Any],
) -> tuple[str, str]:
    if is_static:
        return (
            DECISION_CONTINUE_RESEARCH,
            "static baseline 是 review reference，不是 shadow observation candidate。",
        )
    if not robustness_row:
        if ranking_row.get("decision") == DECISION_REJECT:
            return DECISION_REJECT, "candidate 在 TRADING-2365 已被拒绝。"
        return (
            DECISION_CONTINUE_RESEARCH,
            "candidate 未进入 TRADING-2366 sensitivity set，需要继续候选优化。",
        )
    if robustness_row.get("decision_update") == DECISION_REJECT:
        return DECISION_REJECT, str(robustness_row.get("decision_update_reason"))
    if dynamic_vs_static_gap is not None and dynamic_vs_static_gap <= 0:
        return DECISION_REJECT, "realistic cost-adjusted comparison 未优于 static。"
    turnover_acceptable = _bool_or_none(robustness_row.get("turnover_acceptable"))
    cooldown_fragility = str(robustness_row.get("cooldown_fragility", "UNKNOWN"))
    ranking_top = str(sources["ranking_top_from_2365"])
    robustness_top = str(sources["robustness_top_from_2366"])
    divergence = ranking_top != robustness_top
    if divergence and candidate_id in {ranking_top, robustness_top}:
        return (
            DECISION_OWNER_REVIEW,
            "ranking top 与 robustness top 分歧，需要 owner 判断收益 / 稳健性取舍。",
        )
    if survives_realistic and survives_conservative and turnover_acceptable:
        return (
            DECISION_ACCEPT_RESEARCH_ONLY,
            (
                "candidate 通过 realistic / conservative cost 和 turnover screen，"
                "可进入 research-only observation。"
            ),
        )
    if survives_realistic and survives_conservative:
        return (
            DECISION_OWNER_REVIEW,
            "candidate 通过成本压力，但 turnover 或风险偏好仍需 owner 判断。",
        )
    if survives_realistic and not survives_conservative:
        return (
            DECISION_OWNER_REVIEW,
            "candidate 只通过 realistic cost，未通过 conservative stress。",
        )
    if cooldown_fragility not in {"NOT_SEVERE", "UNKNOWN"}:
        return DECISION_CONTINUE_RESEARCH, "cooldown fragility 需要继续优化。"
    return DECISION_DEPRECATED, "sensitivity 后稳健性不足或证据不完整。"


def _shadow_research_gate_decision(
    *,
    sources: Mapping[str, Any],
    comparison: Sequence[Mapping[str, Any]],
    recommended_candidate: str,
    recommended_decision: str,
    owner_review_required: bool,
    research_only_allowed: bool,
) -> dict[str, Any]:
    ranking_top = str(sources["ranking_top_from_2365"])
    robustness_top = str(sources["robustness_top_from_2366"])
    recommended_row = _row_by_candidate(comparison, recommended_candidate) or {}
    return {
        "schema_version": "dynamic_strategy_shadow_research_gate_decision.v1",
        "recommended_gate_candidate": recommended_candidate,
        "recommended_gate_decision": recommended_decision,
        "recommended_gate_reason": recommended_row.get("recommended_gate_reason"),
        "ranking_top_from_2365": ranking_top,
        "robustness_top_from_2366": robustness_top,
        "ranking_robustness_divergence_detected": ranking_top != robustness_top,
        "owner_review_required": owner_review_required,
        "research_only_shadow_observation_allowed": research_only_allowed,
        "research_only_shadow_observation_requires_owner_review": (
            owner_review_required
        ),
        "paper_shadow_enabled": False,
        "paper_trade_created": False,
        "shadow_position_created": False,
        "event_append_enabled": False,
        "outcome_binding_enabled": False,
        "production_enabled": False,
        "broker_action_enabled": False,
        "next_route": NEXT_ROUTE,
    }


def _owner_review_checklist(
    *,
    sources: Mapping[str, Any],
    comparison: Sequence[Mapping[str, Any]],
    recommended_candidate: str,
    owner_review_required: bool,
) -> list[dict[str, Any]]:
    ranking_top = str(sources["ranking_top_from_2365"])
    robustness_top = str(sources["robustness_top_from_2366"])
    recommended_row = _row_by_candidate(comparison, recommended_candidate) or {}
    ranking_row = _row_by_candidate(comparison, ranking_top) or {}
    return [
        {
            "check_id": "RANKING_ROBUSTNESS_DIVERGENCE",
            "owner_review_required": ranking_top != robustness_top,
            "evidence": (
                f"2365 ranking top={ranking_top}; 2366 robustness top={robustness_top}"
            ),
            "required_decision": "确认是否优先选择 robustness top 进入 research-only observation。",
        },
        {
            "check_id": "TURNOVER_ACCEPTABILITY",
            "owner_review_required": True,
            "evidence": (
                f"recommended_candidate turnover_acceptable="
                f"{recommended_row.get('turnover_acceptable_after_2366')}"
            ),
            "required_decision": "确认 turnover 是否可接受，或是否先继续优化执行约束。",
        },
        {
            "check_id": "DRAWDOWN_TOLERANCE",
            "owner_review_required": True,
            "evidence": (
                f"ranking_top max_drawdown={ranking_row.get('max_drawdown')}; "
                f"recommended max_drawdown={recommended_row.get('max_drawdown')}"
            ),
            "required_decision": "确认收益排名 top 的 drawdown 是否仍可接受。",
        },
        {
            "check_id": "RESEARCH_ONLY_SHADOW_BOUNDARY",
            "owner_review_required": owner_review_required,
            "evidence": "paper_shadow_enabled=false; broker_action_enabled=false",
            "required_decision": (
                "确认下一步仅进入 research-only protocol，不创建 paper trade / shadow position。"
            ),
        },
    ]


def _blocked_owner_checklist(sources: Mapping[str, Any]) -> list[dict[str, Any]]:
    return [
        {
            "check_id": "SOURCE_ARTIFACT_VALIDATION",
            "owner_review_required": True,
            "evidence": "; ".join(_as_list(sources["source_validation_errors"])),
            "required_decision": "修复 source artifact 后重新运行 2367。",
        }
    ]


def _blocked_gate_decision(sources: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": "dynamic_strategy_shadow_research_gate_decision.v1",
        "recommended_gate_candidate": None,
        "recommended_gate_decision": DECISION_OWNER_REVIEW,
        "source_validation_errors": sources["source_validation_errors"],
        "research_only_shadow_observation_allowed": False,
        "paper_shadow_enabled": False,
        "paper_trade_created": False,
        "shadow_position_created": False,
        "event_append_enabled": False,
        "outcome_binding_enabled": False,
        "production_enabled": False,
        "broker_action_enabled": False,
        "next_route": NEXT_ROUTE,
    }


def _summary_findings(
    *,
    ranking_top: str,
    robustness_top: str,
    recommended_candidate: str,
    recommended_decision: str,
    owner_review_required: bool,
    research_only_allowed: bool,
    comparison: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    ranking_row = _row_by_candidate(comparison, ranking_top) or {}
    robustness_row = _row_by_candidate(comparison, robustness_top) or {}
    ranking_top_survives = _yes_no_unknown(
        ranking_row.get("eligible_for_research_only_shadow_observation")
    )
    if (
        ranking_top_survives == "YES"
        and ranking_row.get("recommended_gate_decision") == DECISION_OWNER_REVIEW
    ):
        ranking_top_survives = "YES_WITH_OWNER_REVIEW"
    return {
        "ranking_top_survives_owner_review": ranking_top_survives,
        "robustness_top_preferred_over_ranking_top": (
            "YES_RESEARCH_ONLY" if recommended_candidate == robustness_top else "NO"
        ),
        "ranking_robustness_divergence_detected": _yes_no(
            ranking_top != robustness_top
        ),
        "recommended_gate_candidate": recommended_candidate,
        "recommended_gate_decision": recommended_decision,
        "shadow_observation_should_start": (
            "OWNER_REVIEW_REQUIRED_BEFORE_START"
            if owner_review_required and research_only_allowed
            else _yes_no(research_only_allowed)
        ),
        "ranking_top_turnover_acceptable": _yes_no_unknown(
            ranking_row.get("turnover_acceptable_after_2366")
        ),
        "robustness_top_turnover_acceptable": _yes_no_unknown(
            robustness_row.get("turnover_acceptable_after_2366")
        ),
        "paper_shadow_remains_disabled": True,
        "broker_path_remains_disabled": True,
    }


def _blocked_summary_findings() -> dict[str, Any]:
    return {
        "ranking_top_survives_owner_review": "UNKNOWN_SOURCE_BLOCKED",
        "robustness_top_preferred_over_ranking_top": "UNKNOWN_SOURCE_BLOCKED",
        "ranking_robustness_divergence_detected": "UNKNOWN_SOURCE_BLOCKED",
        "recommended_gate_candidate": None,
        "recommended_gate_decision": DECISION_OWNER_REVIEW,
        "shadow_observation_should_start": "NO_SOURCE_BLOCKED",
        "paper_shadow_remains_disabled": True,
        "broker_path_remains_disabled": True,
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
        "json_path": str(output_root / "owner_review_gate_result.json"),
        "candidate_owner_review_comparison_json": str(
            output_root / "candidate_owner_review_comparison.json"
        ),
        "shadow_research_gate_decision_json": str(
            output_root / "shadow_research_gate_decision.json"
        ),
        "markdown_path": str(
            docs_root / "dynamic_strategy_top_candidate_owner_review_gate.md"
        ),
        "candidate_owner_review_comparison_markdown": str(
            docs_root / "dynamic_strategy_candidate_owner_review_comparison.md"
        ),
        "shadow_research_gate_decision_markdown": str(
            docs_root / "dynamic_strategy_shadow_research_gate_decision.md"
        ),
        "next_route_markdown": str(docs_root / "dynamic_strategy_2368_route.md"),
    }
    payload["artifact_paths"] = artifact_paths
    _write_json(Path(artifact_paths["json_path"]), payload)
    _write_json(
        Path(artifact_paths["candidate_owner_review_comparison_json"]),
        {
            "report_type": "dynamic_strategy_candidate_owner_review_comparison",
            "schema_version": "dynamic_strategy_candidate_owner_review_comparison.v1",
            "status": payload["status"],
            "generated_at": payload["generated_at"],
            "candidate_review_comparison": payload.get(
                "candidate_review_comparison", []
            ),
            "owner_review_checklist": payload.get("owner_review_checklist", []),
            "production_effect": "none",
            "broker_action": "none",
            "paper_shadow_allowed": False,
            "production_allowed": False,
        },
    )
    _write_json(
        Path(artifact_paths["shadow_research_gate_decision_json"]),
        {
            "report_type": "dynamic_strategy_shadow_research_gate_decision",
            "schema_version": "dynamic_strategy_shadow_research_gate_decision.v1",
            "status": payload["status"],
            "generated_at": payload["generated_at"],
            "shadow_research_gate_decision": payload.get(
                "shadow_research_gate_decision", {}
            ),
            "summary_findings": payload.get("summary_findings", {}),
            "production_effect": "none",
            "broker_action": "none",
            "paper_shadow_allowed": False,
            "production_allowed": False,
        },
    )
    Path(artifact_paths["markdown_path"]).write_text(
        _main_markdown(payload), encoding="utf-8"
    )
    Path(artifact_paths["candidate_owner_review_comparison_markdown"]).write_text(
        _comparison_markdown(payload), encoding="utf-8"
    )
    Path(artifact_paths["shadow_research_gate_decision_markdown"]).write_text(
        _gate_markdown(payload), encoding="utf-8"
    )
    Path(artifact_paths["next_route_markdown"]).write_text(
        _route_markdown(payload), encoding="utf-8"
    )


def _main_markdown(payload: Mapping[str, Any]) -> str:
    summary = _as_mapping(payload.get("summary_findings"))
    data_quality_status = _as_mapping(payload.get("data_quality")).get("status")
    return "\n".join(
        [
            "# 动态策略 top candidate owner review and shadow research gate",
            "",
            "## Executive summary",
            "",
            f"- status：`{payload.get('status')}`",
            f"- 2365 ranking top：`{payload.get('ranking_top_from_2365')}`",
            f"- 2366 robustness top：`{payload.get('robustness_top_from_2366')}`",
            (
                "- ranking / robustness divergence："
                f"`{payload.get('ranking_robustness_divergence_detected')}`"
            ),
            f"- recommended gate candidate：`{payload.get('recommended_gate_candidate')}`",
            f"- recommended gate decision：`{payload.get('recommended_gate_decision')}`",
            (
                "- research-only shadow observation allowed："
                f"`{payload.get('research_only_shadow_observation_allowed')}`"
            ),
            f"- owner review required：`{payload.get('owner_review_required')}`",
            f"- data quality carried forward：`{data_quality_status}`",
            "",
            "## Required answers",
            "",
            (
                "- 2365 收益 top 是否仍然推荐："
                f"`{summary.get('ranking_top_survives_owner_review')}`"
            ),
            (
                "- 2366 robustness top 是否应替代收益 top："
                f"`{summary.get('robustness_top_preferred_over_ranking_top')}`"
            ),
            (
                "- 是否存在 ranking / robustness divergence："
                f"`{summary.get('ranking_robustness_divergence_detected')}`"
            ),
            (
                "- 哪个候选最适合 research-only shadow observation："
                f"`{payload.get('recommended_gate_candidate')}`"
            ),
            "- 是否允许真正 paper-shadow：`NO`",
            "- 是否允许 broker / production：`NO`",
            f"- 下一步：`{payload.get('next_route')}`",
            "",
            "## Candidate owner review table",
            "",
            _comparison_table(payload),
            "",
            "## 安全边界",
            "",
            "- 本报告只生成 research-only owner review evidence。",
            "- `research_only_shadow_observation_allowed=true` 不等于 paper-shadow execution。",
            (
                "- paper trade、shadow position、event append、outcome binding、"
                "production 和 broker/order 全部保持 disabled / false / none。"
            ),
        ]
    )


def _comparison_markdown(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# 动态策略 candidate owner review comparison",
            "",
            _comparison_table(payload),
            "",
            "## Owner review checklist",
            "",
            *[
                f"- `{item.get('check_id')}`：owner_review_required="
                f"`{item.get('owner_review_required')}`；{item.get('required_decision')}"
                for item in _as_list(payload.get("owner_review_checklist"))
            ],
        ]
    )


def _gate_markdown(payload: Mapping[str, Any]) -> str:
    decision = _as_mapping(payload.get("shadow_research_gate_decision"))
    return "\n".join(
        [
            "# 动态策略 shadow research gate decision",
            "",
            f"- recommended_gate_candidate：`{decision.get('recommended_gate_candidate')}`",
            f"- recommended_gate_decision：`{decision.get('recommended_gate_decision')}`",
            f"- owner_review_required：`{decision.get('owner_review_required')}`",
            (
                "- research_only_shadow_observation_allowed："
                f"`{decision.get('research_only_shadow_observation_allowed')}`"
            ),
            "- paper_shadow_enabled：`false`",
            "- paper_trade_created：`false`",
            "- shadow_position_created：`false`",
            "- event_append_enabled：`false`",
            "- outcome_binding_enabled：`false`",
            "- production_enabled：`false`",
            "- broker_action_enabled：`false`",
            f"- next_route：`{decision.get('next_route')}`",
        ]
    )


def _route_markdown(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# TRADING-2368 route",
            "",
            f"- current status：`{payload.get('status')}`",
            f"- next route：`{payload.get('next_route')}`",
            f"- recommended candidate：`{payload.get('recommended_gate_candidate')}`",
            f"- gate decision：`{payload.get('recommended_gate_decision')}`",
            (
                "- route boundary：research-only shadow observation protocol；"
                "不是 paper-shadow execution、production 或 broker。"
            ),
        ]
    )


def _comparison_table(payload: Mapping[str, Any]) -> str:
    lines = [
        "|candidate|roles|rank2365|robust_rank|gate_decision|cost_adj|gap|mdd|turnover|fragility|",
        "|---|---|---:|---:|---|---:|---:|---:|---:|---|",
    ]
    for row in _as_list(payload.get("candidate_review_comparison")):
        item = _as_mapping(row)
        lines.append(
            "|"
            + "|".join(
                [
                    f"`{item.get('candidate_id')}`",
                    ", ".join(str(role) for role in _as_list(item.get("roles"))),
                    _fmt(item.get("ranking_rank_from_2365")),
                    _fmt(item.get("robustness_rank")),
                    f"`{item.get('recommended_gate_decision')}`",
                    _fmt(item.get("cost_adjusted_return")),
                    _fmt(item.get("dynamic_vs_static_gap")),
                    _fmt(item.get("max_drawdown")),
                    _fmt(item.get("turnover")),
                    str(item.get("cooldown_fragility")),
                ]
            )
            + "|"
        )
    return "\n".join(lines)


def _candidate_ids(
    *,
    sources: Mapping[str, Any],
    ranking_rows: Sequence[Mapping[str, Any]],
    robustness_rows: Sequence[Mapping[str, Any]],
    sensitivity_rows: Sequence[Mapping[str, Any]],
) -> list[str]:
    values: list[str] = [
        str(sources["ranking_top_from_2365"]),
        str(sources["robustness_top_from_2366"]),
        STATIC_BASELINE_CANDIDATE_ID,
        CURRENT_DYNAMIC_DEFAULT_CANDIDATE_ID,
    ]
    for row in ranking_rows:
        if row.get("candidate_id"):
            values.append(str(row["candidate_id"]))
    for row in robustness_rows:
        if row.get("candidate_id"):
            values.append(str(row["candidate_id"]))
    for row in sensitivity_rows:
        candidate_id = _sensitivity_candidate_id(row)
        if candidate_id:
            values.append(candidate_id)
    return _dedupe([value for value in values if value and value != "None"])


def _candidate_roles(candidate_id: str, sources: Mapping[str, Any]) -> list[str]:
    roles: list[str] = []
    if candidate_id == str(sources["ranking_top_from_2365"]):
        roles.append("ranking_top_from_2365")
    if candidate_id == str(sources["robustness_top_from_2366"]):
        roles.append("robustness_top_from_2366")
    if candidate_id == STATIC_BASELINE_CANDIDATE_ID:
        roles.append("static_baseline")
    if candidate_id == CURRENT_DYNAMIC_DEFAULT_CANDIDATE_ID:
        roles.append("current_dynamic_default")
    return roles or ["review_candidate"]


def _review_priority_rank(candidate_id: str, sources: Mapping[str, Any]) -> int:
    if candidate_id == str(sources["robustness_top_from_2366"]):
        return 1
    if candidate_id == str(sources["ranking_top_from_2365"]):
        return 2
    if candidate_id == STATIC_BASELINE_CANDIDATE_ID:
        return 3
    if candidate_id == CURRENT_DYNAMIC_DEFAULT_CANDIDATE_ID:
        return 4
    return 10


def _recommended_candidate(
    *,
    comparison: Sequence[Mapping[str, Any]],
    ranking_top: str,
    robustness_top: str,
) -> str:
    robustness_row = _row_by_candidate(comparison, robustness_top)
    ranking_row = _row_by_candidate(comparison, ranking_top)
    if not robustness_row:
        return ranking_top
    if robustness_top != ranking_top:
        robustness_survives = bool(
            robustness_row.get("survives_realistic_cost")
            and robustness_row.get("survives_conservative_cost")
        )
        ranking_drawdown = _first_number(
            ranking_row.get("max_drawdown") if ranking_row else None
        )
        robustness_drawdown = _first_number(robustness_row.get("max_drawdown"))
        drawdown_improves = (
            ranking_drawdown is None
            or robustness_drawdown is None
            or robustness_drawdown > ranking_drawdown
        )
        if robustness_survives and drawdown_improves:
            return robustness_top
    return ranking_top


def _primary_cadence_rows(
    cadence_rows: Sequence[Mapping[str, Any]],
) -> dict[str, Mapping[str, Any]]:
    rows: dict[str, Mapping[str, Any]] = {}
    for row in cadence_rows:
        scenario_id = row.get("scenario_id")
        if scenario_id == "static_baseline":
            rows[STATIC_BASELINE_CANDIDATE_ID] = row
        if scenario_id != PRIMARY_EXECUTION_CADENCE:
            continue
        candidate_id = row.get("candidate_id") or row.get("strategy_id")
        if candidate_id:
            rows[str(candidate_id)] = row
    return rows


def _sensitivity_rows_by_scenario(
    sensitivity_rows: Sequence[Mapping[str, Any]],
    scenario_id: str,
) -> dict[str, Mapping[str, Any]]:
    rows: dict[str, Mapping[str, Any]] = {}
    for row in sensitivity_rows:
        if row.get("scenario_id") != scenario_id:
            continue
        candidate_id = _sensitivity_candidate_id(row)
        if candidate_id and candidate_id not in rows:
            rows[candidate_id] = row
    return rows


def _sensitivity_candidate_id(row: Mapping[str, Any]) -> str | None:
    if row.get("is_static_baseline") or row.get("scenario_id") == "static_baseline":
        return STATIC_BASELINE_CANDIDATE_ID
    candidate_id = row.get("candidate_id") or row.get("strategy_id")
    return str(candidate_id) if candidate_id else None


def _ranking_top_row(ranking_rows: Sequence[Any]) -> Mapping[str, Any]:
    rows = [_as_mapping(row) for row in ranking_rows]
    if not rows:
        return {}
    return sorted(rows, key=lambda row: _sort_rank(row.get("rank")))[0]


def _robust_decision(robustness_rows: Sequence[Any], candidate_id: str) -> Any:
    row = _row_by_candidate(
        [_as_mapping(item) for item in robustness_rows], candidate_id
    )
    return row.get("decision_update") if row else None


def _row_by_candidate(
    rows: Sequence[Mapping[str, Any]],
    candidate_id: str,
) -> Mapping[str, Any] | None:
    for row in rows:
        if row.get("candidate_id") == candidate_id:
            return row
    return None


def _source_data_quality(sources: Mapping[str, Any]) -> dict[str, Any]:
    sensitivity_quality = _as_mapping(
        _as_mapping(sources.get("sensitivity_result")).get("data_quality")
    )
    event_quality = _as_mapping(
        _as_mapping(sources.get("event_retest")).get("data_quality")
    )
    status = (
        sensitivity_quality.get("status")
        or event_quality.get("status")
        or sensitivity_quality.get("quality_status")
        or event_quality.get("quality_status")
        or "UNKNOWN"
    )
    return {
        "status": status,
        "source": "TRADING-2366" if sensitivity_quality else "TRADING-2365",
        "error_count": _first_number(
            sensitivity_quality.get("error_count"), event_quality.get("error_count")
        ),
        "warning_count": _first_number(
            sensitivity_quality.get("warning_count"),
            event_quality.get("warning_count"),
        ),
        "carried_forward_from_prior_artifacts": True,
    }


def _requested_date_range(sources: Mapping[str, Any]) -> dict[str, Any]:
    sensitivity_range = _as_mapping(
        _as_mapping(sources.get("sensitivity_result")).get("requested_date_range")
    )
    event_range = _as_mapping(
        _as_mapping(sources.get("event_retest")).get("requested_date_range")
    )
    start = (
        sensitivity_range.get("start")
        or sensitivity_range.get("start_date")
        or event_range.get("start")
        or event_range.get("start_date")
        or DEFAULT_AI_REGIME_BACKTEST_START.isoformat()
    )
    end = (
        sensitivity_range.get("end")
        or sensitivity_range.get("end_date")
        or event_range.get("end")
        or event_range.get("end_date")
    )
    return {"start": start, "end": end}


def _resolve_as_of(as_of_date: date | None, sources: Mapping[str, Any]) -> date:
    if as_of_date is not None:
        return as_of_date
    for key in ("sensitivity_result", "event_retest"):
        raw = _as_mapping(sources.get(key)).get("as_of")
        if isinstance(raw, str):
            try:
                return date.fromisoformat(raw[:10])
            except ValueError:
                continue
    return date.today()


def _source_artifact(path: Path, document: Any) -> dict[str, Any]:
    return {
        "path": str(path),
        "sha256": _safe_sha256(path),
        "status": _as_mapping(document).get("status"),
        "load_error": _as_mapping(document).get("_load_error"),
    }


def _safe_sha256(path: Path) -> str | None:
    try:
        return _file_sha256(path)
    except OSError:
        return None


def _load_json_document(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        return {"_load_error": str(exc)}


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _required_outputs_ready(ready: bool) -> dict[str, bool]:
    return {
        "ranking_top_from_2365": ready,
        "robustness_top_from_2366": ready,
        "ranking_robustness_divergence_detected": ready,
        "owner_review_required": ready,
        "candidate_review_comparison": ready,
        "recommended_gate_candidate": ready,
        "recommended_gate_decision": ready,
        "research_only_shadow_observation_allowed": ready,
        "paper_shadow_enabled_false": ready,
        "production_enabled_false": ready,
        "broker_action_enabled_false": ready,
        "recommended_next_research_task": ready,
    }


def _as_mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _as_list(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []


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
            return round(float(value), 6)
        except (TypeError, ValueError):
            continue
    return None


def _bool_or_none(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    return None


def _drawdown_gap(
    candidate_drawdown: float | None,
    static_drawdown: float | None,
) -> float | None:
    if candidate_drawdown is None or static_drawdown is None:
        return None
    return round(candidate_drawdown - static_drawdown, 6)


def _sort_rank(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 999999


def _dedupe(values: Sequence[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        result.append(value)
    return result


def _yes_no(value: bool) -> str:
    return "YES" if value else "NO"


def _yes_no_unknown(value: Any) -> str:
    if value is True:
        return "YES"
    if value is False:
        return "NO"
    return "UNKNOWN"


def _fmt(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float):
        return f"{value:.6g}"
    return str(value)
