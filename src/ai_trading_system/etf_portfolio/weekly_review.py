from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime, timedelta
from hashlib import sha256
from pathlib import Path
from typing import Any

import pandas as pd

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.etf_portfolio.models import (
    DEFAULT_ETF_REPORT_DIR,
    DEFAULT_ETF_TARGET_PATH,
    load_etf_config_bundle,
)
from ai_trading_system.reports.report_index import (
    DEFAULT_REPORT_REGISTRY_PATH,
    build_report_index_payload,
    load_report_registry,
)

WEEKLY_REVIEW_AGGREGATION_SCHEMA_VERSION = "etf_weekly_review_aggregation_v1"
WEEKLY_REVIEW_SCHEMA_VERSION = "etf_weekly_review_v1"
WEEKLY_REVIEW_VALIDATION_SCHEMA_VERSION = "etf_weekly_review_validation_v1"

DEFAULT_ETF_WEEKLY_REVIEW_DIR = DEFAULT_ETF_REPORT_DIR / "weekly_review"
DEFAULT_ETF_WEEKLY_REVIEW_AGGREGATION_DIR = DEFAULT_ETF_WEEKLY_REVIEW_DIR / "aggregation"
DEFAULT_ETF_WEEKLY_REVIEW_VALIDATION_DIR = DEFAULT_ETF_WEEKLY_REVIEW_DIR / "validation"

WEEKLY_REVIEW_SAFETY = {
    "observe_only": True,
    "candidate_only": True,
    "production_effect": "none",
    "broker_action": "none",
    "manual_review_required": True,
}

SOURCE_REPORT_IDS: tuple[str, ...] = (
    "etf_portfolio_brief",
    "etf_data_quality",
    "etf_experiment_candidate_selection",
    "etf_shadow_candidates",
    "etf_experiment_weekly_review",
    "etf_forward_dashboard",
    "etf_forward_weekly_review",
    "etf_forward_watchlist",
    "etf_ai_confirmation_report",
    "etf_satellite_replacement_report",
    "etf_credibility_gate",
    "etf_experiment_validation",
    "etf_forward_validation",
    "etf_ai_confirmation_validation",
    "etf_satellite_validation",
)

VALIDATION_REPORT_IDS = frozenset(
    {
        "etf_credibility_gate",
        "etf_experiment_validation",
        "etf_forward_validation",
        "etf_ai_confirmation_validation",
        "etf_satellite_validation",
    }
)

ALLOWED_SHADOW_ACTIONS = frozenset(
    {
        "continue_shadow",
        "needs_more_data",
        "watch",
        "reject_pending_review",
        "archive_after_review",
    }
)
ALLOWED_MANUAL_ACTION_TYPES = frozenset(
    {
        "continue_observation",
        "review_candidate",
        "mark_candidate_watch",
        "consider_reject_after_review",
        "review_data_gap",
        "review_event_risk",
        "start_new_experiment",
        "defer_decision",
    }
)
UNSAFE_ACTION_TYPES = frozenset(
    {
        "place_order",
        "promote_to_production",
        "change_production_weights",
        "disable_risk_gate",
        "enable_broker_action",
        "auto_promote_candidate",
        "auto_reject_without_review",
    }
)
UNSAFE_OUTPUT_KEYS = frozenset(
    {
        "production_weight_update",
        "broker_order",
        "auto_promote_candidate",
        "auto_reject_without_review",
        "production_weights",
        "broker_orders",
    }
)

SOURCE_MODULE_BY_REPORT_ID = {
    "etf_portfolio_brief": "etf_allocation",
    "etf_data_quality": "etf_data_quality",
    "etf_experiment_candidate_selection": "etf_experiment_comparison",
    "etf_shadow_candidates": "shadow_enrollment",
    "etf_experiment_weekly_review": "etf_experiment_weekly_review",
    "etf_forward_dashboard": "etf_forward_dashboard",
    "etf_forward_weekly_review": "etf_forward_weekly_review",
    "etf_forward_watchlist": "risk_watchlist",
    "etf_ai_confirmation_report": "ai_confirmation",
    "etf_satellite_replacement_report": "satellite_replacement",
    "etf_credibility_gate": "credibility_gate",
    "etf_experiment_validation": "experiment_validation_gate",
    "etf_forward_validation": "forward_validation_gate",
    "etf_ai_confirmation_validation": "ai_confirmation_validation_gate",
    "etf_satellite_validation": "satellite_validation_gate",
}


def build_weekly_review_aggregation(
    *,
    as_of: date,
    report_index_payload: Mapping[str, Any] | None = None,
    report_index_path: Path | None = None,
    report_registry_path: Path = DEFAULT_REPORT_REGISTRY_PATH,
    project_root: Path = PROJECT_ROOT,
    target_weights_path: Path = DEFAULT_ETF_TARGET_PATH,
    required_report_ids: Sequence[str] | None = None,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(tz=UTC)
    required = {str(item) for item in (required_report_ids or [])}
    report_index = _load_or_build_report_index(
        as_of=as_of,
        report_index_payload=report_index_payload,
        report_index_path=report_index_path,
        report_registry_path=report_registry_path,
        project_root=project_root,
    )
    source_reports = [
        _source_report_record(
            report_index,
            report_id=report_id,
            required=report_id in required,
        )
        for report_id in SOURCE_REPORT_IDS
    ]
    portfolio_state = _load_portfolio_state(as_of=as_of, target_weights_path=target_weights_path)
    if portfolio_state["status"] == "missing_data":
        source_reports.append(
            {
                "report_id": "etf_target_weights",
                "source_module": "etf_allocation",
                "title": "ETF Target Weights",
                "status": "missing_data",
                "reason_code": portfolio_state["reason_code"],
                "required": False,
                "loaded": False,
                "source_report_path": str(target_weights_path),
                "artifact_status": "MISSING",
                "freshness_status": "MISSING",
                "source_metric": "target_weights",
                "time_window": {"as_of": as_of.isoformat()},
                "payload": {},
            }
        )
    missing_sections = [
        _source_report_public(record)
        for record in source_reports
        if record["status"] in {"missing_data", "unreadable"}
    ]
    loaded_sections = [
        str(record["report_id"])
        for record in source_reports
        if record["status"] in {"loaded", "available_unstructured"}
    ]
    required_missing = [
        record
        for record in source_reports
        if record.get("required") is True and record["status"] != "loaded"
    ]
    safety_issues = [
        issue
        for record in source_reports
        for issue in _source_safety_issues(record)
        if record.get("payload")
    ]
    warnings = _aggregation_warnings(missing_sections, safety_issues)
    status = "PASS"
    if required_missing or safety_issues:
        status = "FAIL"
    elif warnings:
        status = "PASS_WITH_WARNINGS"
    public_sources = [_source_report_public(record) for record in source_reports]
    data_snapshot = _data_snapshot(
        source_reports=public_sources,
        portfolio_state=portfolio_state,
    )
    config = _safe_config_bundle()
    return {
        "schema_version": WEEKLY_REVIEW_AGGREGATION_SCHEMA_VERSION,
        "report_type": "etf_weekly_review_aggregation",
        "aggregation_status": status,
        "as_of": as_of.isoformat(),
        "generated_at": generated.isoformat(),
        "model_version": _model_version(portfolio_state, config),
        "config_hash": _config_hash(portfolio_state, config),
        "data_hash_or_snapshot": data_snapshot,
        "report_index_status": _text(report_index.get("status"), "UNKNOWN"),
        "report_index_path": "" if report_index_path is None else str(report_index_path),
        "source_reports": public_sources,
        "loaded_sections": loaded_sections,
        "missing_sections": missing_sections,
        "warnings": warnings,
        "portfolio_state": portfolio_state,
        "source_payloads": {
            str(record["report_id"]): record.get("payload", {}) for record in source_reports
        },
        "safety_flags": dict(WEEKLY_REVIEW_SAFETY),
        **WEEKLY_REVIEW_SAFETY,
    }


def build_weekly_review_report(
    *,
    as_of: date,
    aggregation_payload: Mapping[str, Any],
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(tz=UTC)
    review_start = as_of - timedelta(days=6)
    portfolio_summary = build_portfolio_decision_summary(aggregation_payload)
    shadow_review = build_shadow_candidate_review_section(aggregation_payload)
    ai_review = build_ai_confirmation_review_section(aggregation_payload)
    satellite_review = build_satellite_replacement_review_section(aggregation_payload)
    risk_summary = build_risk_watchlist_constraint_summary(
        aggregation_payload=aggregation_payload,
        shadow_review=shadow_review,
        ai_review=ai_review,
        satellite_review=satellite_review,
    )
    manual_actions = build_manual_review_action_items(
        as_of=as_of,
        risk_summary=risk_summary,
        shadow_review=shadow_review,
        ai_review=ai_review,
        satellite_review=satellite_review,
        generated_at=generated,
    )
    validate_weekly_review_action_items(manual_actions)
    next_week_watch_items = _next_week_watch_items(
        risk_summary=risk_summary,
        shadow_review=shadow_review,
        satellite_review=satellite_review,
    )
    validation_status = _validation_status_from_sources(aggregation_payload)
    overall_status = _overall_review_status(
        portfolio_summary=portfolio_summary,
        shadow_review=shadow_review,
        ai_review=ai_review,
        satellite_review=satellite_review,
        risk_summary=risk_summary,
    )
    payload = {
        "schema_version": WEEKLY_REVIEW_SCHEMA_VERSION,
        "report_type": "etf_weekly_review",
        "review_id": f"etf-weekly-review-{as_of.isoformat()}",
        "review_start_date": review_start.isoformat(),
        "review_end_date": as_of.isoformat(),
        "generated_at": generated.isoformat(),
        "model_version": _text(aggregation_payload.get("model_version"), "UNKNOWN"),
        "config_hash": _text(aggregation_payload.get("config_hash"), ""),
        "data_hash_or_snapshot": dict(_mapping(aggregation_payload.get("data_hash_or_snapshot"))),
        "market_regime": portfolio_summary.get("current_market_regime"),
        "requested_date_range": {
            "start": review_start.isoformat(),
            "end": as_of.isoformat(),
            "market_regime": "ai_after_chatgpt",
        },
        "status": overall_status,
        "aggregation_status": _text(
            aggregation_payload.get("aggregation_status"),
            "UNKNOWN",
        ),
        "safety": dict(WEEKLY_REVIEW_SAFETY),
        "safety_banner": _safety_banner(),
        "sections": {
            "portfolio_decision_summary": portfolio_summary,
            "shadow_candidate_review": shadow_review,
            "ai_confirmation_review": ai_review,
            "satellite_replacement_review": satellite_review,
            "risk_watchlist_constraints": risk_summary,
        },
        "manual_review_actions": manual_actions,
        "watchlist_items": list(risk_summary.get("watchlist_items", [])),
        "next_week_watch_items": next_week_watch_items,
        "source_reports": list(aggregation_payload.get("source_reports", [])),
        "validation_status": validation_status,
        **WEEKLY_REVIEW_SAFETY,
    }
    validate_weekly_review_payload_safety(payload)
    return payload


def build_portfolio_decision_summary(
    aggregation_payload: Mapping[str, Any],
) -> dict[str, Any]:
    state = _mapping(aggregation_payload.get("portfolio_state"))
    weights = _records(state.get("target_weights"))
    if not weights:
        return {
            "summary_status": "insufficient_data",
            "key_findings": ["ETF target weights are missing; weekly portfolio state is limited."],
            "portfolio_state": state,
            "weight_change_summary": {
                "changed_symbol_count": 0,
                "largest_change": "MISSING",
                "total_abs_trade_delta": None,
            },
            "benchmark_context": {"status": "missing_weights"},
            "actionability_note": _actionability_note(),
            "safety_banner": _safety_banner(),
        }
    changes = _weight_change_summary(weights)
    equity_exposure = sum(
        _float_or_default(row.get("target_weight"), 0.0)
        for row in weights
        if _text(row.get("symbol")) != "CASH"
    )
    cash_weight = sum(
        _float_or_default(row.get("target_weight"), 0.0)
        for row in weights
        if _text(row.get("symbol")) == "CASH"
    )
    data_quality_status = _text(state.get("data_quality_status"), "UNKNOWN")
    summary_status = "stable_observe"
    if data_quality_status not in {"PASS", "PASS_WITH_WARNINGS", "UNKNOWN", ""}:
        summary_status = "risk_watch"
    return {
        "summary_status": summary_status,
        "key_findings": [
            f"Market regime: {_text(state.get('market_regime'), 'UNKNOWN')}.",
            f"Equity exposure: {_fmt_pct(equity_exposure)}; cash: {_fmt_pct(cash_weight)}.",
            f"Weight changes: {changes['largest_change']}.",
            f"Data quality: {data_quality_status}.",
        ],
        "current_market_regime": _text(state.get("market_regime"), "UNKNOWN"),
        "portfolio_state": {
            "as_of": state.get("as_of"),
            "target_weights": weights,
            "cash_weight": round(cash_weight, 8),
            "equity_exposure": round(equity_exposure, 8),
            "data_quality_status": data_quality_status,
            "model_version": state.get("model_version"),
            "config_hash": state.get("config_hash"),
        },
        "weight_change_summary": changes,
        "benchmark_context": _benchmark_context(weights),
        "actionability_note": _actionability_note(),
        "safety_banner": _safety_banner(),
    }


def build_shadow_candidate_review_section(
    aggregation_payload: Mapping[str, Any],
) -> dict[str, Any]:
    sources = _mapping(aggregation_payload.get("source_payloads"))
    dashboard = _mapping(sources.get("etf_forward_dashboard"))
    forward_weekly = _mapping(sources.get("etf_forward_weekly_review"))
    experiment_weekly = _mapping(sources.get("etf_experiment_weekly_review"))
    shadow_registry = _mapping(sources.get("etf_shadow_candidates"))
    dashboard_rows = _records(dashboard.get("candidate_summary_table"))
    active_candidates = (
        dashboard_rows
        or _records(forward_weekly.get("active_candidates"))
        or _records(experiment_weekly.get("active_shadow_candidates"))
        or _records(shadow_registry.get("candidates"))
    )
    status_changes = _records(forward_weekly.get("candidate_status_changes"))
    source_path = _source_path(aggregation_payload, "etf_forward_dashboard")
    reviews = [
        _shadow_candidate_row(row, status_changes=status_changes, source_path=source_path)
        for row in active_candidates
    ]
    for row in reviews:
        _validate_shadow_action(_text(row.get("recommended_observation_action")))
    weak = [
        row
        for row in reviews
        if _text(row.get("recommended_observation_action")) in {"watch", "reject_pending_review"}
    ]
    strong = [
        row
        for row in reviews
        if _float_or_none(row.get("excess_return_vs_baseline")) is not None
        and (_float_or_none(row.get("excess_return_vs_baseline")) or 0.0) > 0
    ]
    status = "no_active_candidates" if not reviews else "available"
    if weak:
        status = "candidate_review_required"
    return {
        "section_status": status,
        "active_shadow_candidates": reviews,
        "new_candidates": _new_shadow_candidates(reviews),
        "strong_candidates": strong,
        "weak_candidates": weak,
        "candidate_status_changes": status_changes,
        "allowed_actions": sorted(ALLOWED_SHADOW_ACTIONS),
        "source_report_path": source_path,
        "summary": {
            "active_candidate_count": len(reviews),
            "candidate_requiring_review_count": len(weak),
        },
        "safety_banner": _safety_banner(),
        "production_promotion_allowed": False,
    }


def build_ai_confirmation_review_section(
    aggregation_payload: Mapping[str, Any],
) -> dict[str, Any]:
    report = _mapping(
        _mapping(aggregation_payload.get("source_payloads")).get("etf_ai_confirmation_report")
    )
    source_path = _source_path(aggregation_payload, "etf_ai_confirmation_report")
    if not report:
        return {
            "section_status": "insufficient_data",
            "reason_code": "REPORT_NOT_FOUND",
            "AIConfirmationScore": None,
            "score_band": "MISSING",
            "component_scores": {},
            "event_risk": {},
            "data_coverage": {},
            "interpretation": (
                "AI confirmation report missing; candidate-only overlay not reviewed."
            ),
            "impact_on_candidate_only_overlays": "none; missing report",
            "source_report_path": source_path,
            "safety_banner": _safety_banner(),
        }
    score = _mapping(report.get("AIConfirmationScore"))
    action_hint = _text(score.get("action_hint"), "insufficient_data")
    status = (
        action_hint
        if action_hint
        in {
            "supports_ai_overweight_candidate",
            "supports_neutral_ai_exposure",
            "warns_against_ai_overweight",
            "insufficient_data",
        }
        else "insufficient_data"
    )
    return {
        "section_status": status,
        "AIConfirmationScore": score.get("score_value"),
        "score_band": score.get("score_band"),
        "component_scores": dict(_mapping(report.get("component_scores"))),
        "component_score_changes": "not_available_in_latest_report",
        "semiconductor_breadth": dict(_mapping(report.get("semiconductor_breadth"))),
        "mega_cap_ai_score": dict(_mapping(report.get("mega_cap_ai_confirmation"))),
        "relative_strength_score": dict(_mapping(report.get("ai_semiconductor_relative_strength"))),
        "event_risk": dict(_mapping(report.get("event_risk_overlay"))),
        "data_coverage": dict(_mapping(report.get("data_coverage"))),
        "interpretation": _ai_interpretation(status),
        "impact_on_candidate_only_overlays": (
            "candidate_only_review_input; no production mutation"
        ),
        "source_report_path": source_path,
        "evidence": [
            _evidence(
                source_module="ai_confirmation",
                source_report_path=source_path,
                source_metric="AIConfirmationScore",
                time_window=_text(report.get("date")),
                reason_code=status,
            )
        ],
        "safety_banner": _safety_banner(),
    }


def build_satellite_replacement_review_section(
    aggregation_payload: Mapping[str, Any],
) -> dict[str, Any]:
    report = _mapping(
        _mapping(aggregation_payload.get("source_payloads")).get("etf_satellite_replacement_report")
    )
    source_path = _source_path(aggregation_payload, "etf_satellite_replacement_report")
    if not report:
        return {
            "section_status": "insufficient_data",
            "reason_code": "REPORT_NOT_FOUND",
            "eligible_satellite_stocks": [],
            "watchlist_satellite_stocks": [],
            "fallback_to_etf_stocks": [],
            "replacement_plan_summary": {},
            "constraints_applied": [],
            "source_report_path": source_path,
            "safety_banner": _safety_banner(),
        }
    eligible = _texts(report.get("eligible_stocks"))
    watchlist = _texts(report.get("watchlist"))
    fallback = _texts(report.get("fallback_to_etf_stocks"))
    plan = _mapping(report.get("replacement_plan"))
    if eligible:
        status = "eligible_candidates_present"
    elif watchlist:
        status = "watch_only"
    elif fallback:
        status = "fallback_preferred"
    else:
        status = "no_eligible_replacement"
    return {
        "section_status": status,
        "eligible_satellite_stocks": eligible,
        "watchlist_satellite_stocks": watchlist,
        "fallback_to_etf_stocks": fallback,
        "replacement_plan_summary": {
            "replacement_plan_id": plan.get("replacement_plan_id"),
            "total_replaced_weight": plan.get("total_replaced_weight"),
            "satellite_allocations": _records(plan.get("satellite_allocations")),
            "fallback_positions": _records(plan.get("fallback_positions")),
        },
        "stock_vs_etf_relative_strength": _records(report.get("stock_vs_etf_features")),
        "constraints_applied": list(plan.get("constraints_applied", [])),
        "risk_constraints": dict(_mapping(report.get("risk_constraints"))),
        "ai_confirmation_context": dict(_mapping(report.get("ai_confirmation_context"))),
        "event_risk_context": _satellite_event_risk_context(report),
        "candidate_only_replacement_impact": (
            "candidate/shadow/hypothetical only; official ETF target weights unchanged"
        ),
        "source_report_path": source_path,
        "evidence": [
            _evidence(
                source_module="satellite_replacement",
                source_report_path=source_path,
                source_metric="replacement_eligibility",
                time_window=_text(report.get("date")),
                reason_code=status,
            )
        ],
        "safety_banner": _safety_banner(),
    }


def build_risk_watchlist_constraint_summary(
    *,
    aggregation_payload: Mapping[str, Any],
    shadow_review: Mapping[str, Any],
    ai_review: Mapping[str, Any],
    satellite_review: Mapping[str, Any],
) -> dict[str, Any]:
    watchlist_items: list[dict[str, Any]] = []
    watchlist_items.extend(_forward_watchlist_items(aggregation_payload))
    watchlist_items.extend(_missing_or_stale_source_items(aggregation_payload))
    watchlist_items.extend(_validation_gate_items(aggregation_payload))
    watchlist_items.extend(_candidate_lifecycle_items(shadow_review))
    watchlist_items.extend(_ai_event_risk_items(ai_review))
    watchlist_items.extend(_satellite_constraint_items(satellite_review))
    watchlist_items = _dedupe_watch_items(watchlist_items)
    severity_counts = {
        "critical": sum(1 for item in watchlist_items if item["severity"] == "critical"),
        "warning": sum(1 for item in watchlist_items if item["severity"] == "warning"),
        "info": sum(1 for item in watchlist_items if item["severity"] == "info"),
    }
    status = "clear"
    if severity_counts["critical"]:
        status = "critical_review_required"
    elif severity_counts["warning"]:
        status = "risk_watch"
    return {
        "section_status": status,
        "severity_counts": severity_counts,
        "risk_warnings": [
            item for item in watchlist_items if item["severity"] in {"critical", "warning"}
        ],
        "watchlist_items": watchlist_items,
        "constraint_hits": _constraint_hits(aggregation_payload, satellite_review),
        "candidate_lifecycle_warnings": _candidate_lifecycle_items(shadow_review),
        "high_event_risk_windows": _ai_event_risk_items(ai_review),
        "data_quality_warnings": _data_quality_items(aggregation_payload),
        "validation_gate_failures": _validation_gate_items(aggregation_payload),
        "source_report_path": _source_path(aggregation_payload, "etf_forward_watchlist"),
        "safety_banner": _safety_banner(),
    }


def build_manual_review_action_items(
    *,
    as_of: date,
    risk_summary: Mapping[str, Any],
    shadow_review: Mapping[str, Any],
    ai_review: Mapping[str, Any],
    satellite_review: Mapping[str, Any],
    generated_at: datetime | None = None,
) -> list[dict[str, Any]]:
    generated = generated_at or datetime.now(tz=UTC)
    actions: list[dict[str, Any]] = []
    for item in _records(risk_summary.get("watchlist_items")):
        action_type = _action_type_for_watch_item(item)
        actions.append(
            _manual_action(
                action_type=action_type,
                priority=_priority_for_severity(_text(item.get("severity"))),
                source_module=_text(item.get("source_module"), "risk_watchlist"),
                evidence=[_watch_item_evidence(item)],
                recommended_reason=_text(item.get("issue"), "review risk item"),
                as_of=as_of,
                generated_at=generated,
            )
        )
    for row in _records(shadow_review.get("active_shadow_candidates")):
        action = _text(row.get("recommended_observation_action"))
        if action == "watch":
            action_type = "mark_candidate_watch"
        elif action == "reject_pending_review":
            action_type = "consider_reject_after_review"
        elif action == "needs_more_data":
            action_type = "defer_decision"
        else:
            action_type = "continue_observation"
        actions.append(
            _manual_action(
                action_type=action_type,
                priority="high" if action == "reject_pending_review" else "medium",
                source_module="etf_forward_dashboard",
                evidence=list(row.get("evidence", [])),
                recommended_reason=(
                    f"Review shadow candidate {_text(row.get('candidate_id'), 'UNKNOWN')} "
                    f"with action {action}."
                ),
                as_of=as_of,
                generated_at=generated,
            )
        )
    if _text(ai_review.get("section_status")) == "warns_against_ai_overweight":
        actions.append(
            _manual_action(
                action_type="review_event_risk",
                priority="high",
                source_module="ai_confirmation",
                evidence=list(ai_review.get("evidence", [])),
                recommended_reason="AI confirmation warns against AI overweight.",
                as_of=as_of,
                generated_at=generated,
            )
        )
    if _text(satellite_review.get("section_status")) == "eligible_candidates_present":
        actions.append(
            _manual_action(
                action_type="review_candidate",
                priority="medium",
                source_module="satellite_replacement",
                evidence=list(satellite_review.get("evidence", [])),
                recommended_reason="Satellite replacement has eligible candidate-only stocks.",
                as_of=as_of,
                generated_at=generated,
            )
        )
    if not actions:
        actions.append(
            _manual_action(
                action_type="continue_observation",
                priority="low",
                source_module="weekly_review",
                evidence=[
                    _evidence(
                        source_module="weekly_review",
                        source_report_path="",
                        source_metric="weekly_review_status",
                        time_window=as_of.isoformat(),
                        reason_code="no_blocking_action_items",
                    )
                ],
                recommended_reason="No blocking weekly review items; continue observation.",
                as_of=as_of,
                generated_at=generated,
            )
        )
    return _dedupe_actions(actions)


def validate_weekly_review_action_items(actions: Sequence[Mapping[str, Any]]) -> None:
    for action in actions:
        action_type = _text(action.get("action_type"))
        if action_type in UNSAFE_ACTION_TYPES or action_type not in ALLOWED_MANUAL_ACTION_TYPES:
            raise ValueError(f"unsafe or unsupported weekly review action type: {action_type}")
        if action.get("requires_manual_review") is not True:
            raise ValueError("weekly review action items must require manual review")
        if not _records(action.get("evidence")):
            raise ValueError("weekly review action items must include evidence")


def validate_weekly_review_payload_safety(payload: Mapping[str, Any]) -> None:
    for key, expected in WEEKLY_REVIEW_SAFETY.items():
        if payload.get(key) != expected:
            raise ValueError(f"weekly review safety violation: {key} must be {expected}")
    if _contains_unsafe_key(payload):
        raise ValueError("weekly review payload contains disallowed production output key")
    validate_weekly_review_action_items(_records(payload.get("manual_review_actions")))


def build_weekly_review_validation_report(
    *,
    report_registry_path: Path = DEFAULT_REPORT_REGISTRY_PATH,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(tz=UTC)
    checks: list[dict[str, Any]] = []
    sample_aggregation = _sample_aggregation(generated)
    sample_report = build_weekly_review_report(
        as_of=generated.date(),
        aggregation_payload=sample_aggregation,
        generated_at=generated,
    )
    _append_validation_check(
        checks,
        "aggregator_available",
        True,
        "Weekly review data aggregator is available.",
        {"schema_version": sample_aggregation["schema_version"]},
    )
    _append_validation_check(
        checks,
        "portfolio_decision_summary_available",
        bool(_mapping(sample_report.get("sections")).get("portfolio_decision_summary")),
        "Portfolio decision summary is available.",
    )
    _append_validation_check(
        checks,
        "shadow_candidate_section_available",
        bool(_mapping(sample_report.get("sections")).get("shadow_candidate_review")),
        "Shadow candidate review section is available.",
    )
    _append_validation_check(
        checks,
        "ai_confirmation_section_available",
        bool(_mapping(sample_report.get("sections")).get("ai_confirmation_review")),
        "AI confirmation section is available.",
    )
    _append_validation_check(
        checks,
        "satellite_replacement_section_available",
        bool(_mapping(sample_report.get("sections")).get("satellite_replacement_review")),
        "Satellite replacement section is available.",
    )
    _append_validation_check(
        checks,
        "risk_watchlist_section_available",
        bool(_mapping(sample_report.get("sections")).get("risk_watchlist_constraints")),
        "Risk/watchlist section is available.",
    )
    _append_validation_check(
        checks,
        "manual_review_actions_available",
        bool(_records(sample_report.get("manual_review_actions"))),
        "Manual review action items are available.",
    )
    _append_validation_check(
        checks,
        "weekly_report_generator_available",
        sample_report.get("schema_version") == WEEKLY_REVIEW_SCHEMA_VERSION,
        "Weekly review report generator emits stable schema.",
    )
    registry = _safe_report_registry(report_registry_path)
    registry_ids = {
        _text(entry.get("report_id")) for entry in _records(_mapping(registry).get("reports"))
    }
    _append_validation_check(
        checks,
        "reader_brief_integration_available",
        "etf_weekly_review" in registry_ids,
        "Report registry exposes weekly review for Reader Brief navigation.",
        {"report_registry_path": str(report_registry_path)},
    )
    _append_validation_check(
        checks,
        "source_report_traceability_available",
        all(
            "source_report_path" in record and "source_module" in record
            for record in _records(sample_report.get("source_reports"))
        ),
        "Source report traceability fields are present.",
    )
    _append_validation_check(
        checks,
        "unsafe_actions_blocked",
        _unsafe_action_validation_passes(),
        "Unsafe weekly review actions are rejected.",
    )
    _append_validation_check(
        checks,
        "production_effect_none",
        sample_report.get("production_effect") == "none",
        "Weekly review production_effect remains none.",
    )
    _append_validation_check(
        checks,
        "broker_action_none",
        sample_report.get("broker_action") == "none",
        "Weekly review broker_action remains none.",
    )
    _append_validation_check(
        checks,
        "manual_review_required_true",
        sample_report.get("manual_review_required") is True,
        "Weekly review requires manual review.",
    )
    status = "PASS" if all(check["status"] == "PASS" for check in checks) else "FAIL"
    return {
        "schema_version": WEEKLY_REVIEW_VALIDATION_SCHEMA_VERSION,
        "report_type": "etf_weekly_review_validation",
        "status": status,
        "generated_at": generated.isoformat(),
        "checks": checks,
        "safety": dict(WEEKLY_REVIEW_SAFETY),
        "production_weights_mutated": False,
        "broker_action_enabled": False,
        **WEEKLY_REVIEW_SAFETY,
    }


def write_weekly_review_aggregation(payload: Mapping[str, Any], output_path: Path) -> Path:
    _write_json(payload, output_path)
    return output_path


def write_weekly_review_report(
    payload: Mapping[str, Any],
    *,
    json_path: Path,
    markdown_path: Path,
) -> tuple[Path, Path]:
    _write_json(payload, json_path)
    markdown_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_path.write_text(render_weekly_review_markdown(payload), encoding="utf-8")
    return json_path, markdown_path


def write_weekly_review_validation_report(
    payload: Mapping[str, Any],
    *,
    json_path: Path,
    markdown_path: Path,
) -> tuple[Path, Path]:
    _write_json(payload, json_path)
    markdown_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_path.write_text(render_weekly_review_validation_markdown(payload), encoding="utf-8")
    return json_path, markdown_path


def render_weekly_review_markdown(payload: Mapping[str, Any]) -> str:
    sections = _mapping(payload.get("sections"))
    portfolio = _mapping(sections.get("portfolio_decision_summary"))
    shadow = _mapping(sections.get("shadow_candidate_review"))
    ai = _mapping(sections.get("ai_confirmation_review"))
    satellite = _mapping(sections.get("satellite_replacement_review"))
    risk = _mapping(sections.get("risk_watchlist_constraints"))
    lines = [
        f"# ETF Portfolio Weekly Review - {payload.get('review_end_date')}",
        "",
        "## Safety Banner",
        "",
        "- observe_only = true",
        "- candidate_only = true",
        "- production_effect = none",
        "- broker_action = none",
        "- manual_review_required = true",
        "- 本报告只汇总已有 evidence，不能自动修改 production weights 或触发 broker action。",
        "",
        "## Review Metadata",
        "",
        f"- Review ID: {payload.get('review_id')}",
        f"- Review Window: {payload.get('review_start_date')} to {payload.get('review_end_date')}",
        f"- Market Regime: {payload.get('market_regime')}",
        f"- Status: {payload.get('status')}",
        f"- Aggregation Status: {payload.get('aggregation_status')}",
        f"- Model Version: {payload.get('model_version')}",
        f"- Config Hash: `{payload.get('config_hash')}`",
        "",
        "## Portfolio Decision Summary",
        "",
        f"- Summary Status: {portfolio.get('summary_status')}",
        *[f"- {item}" for item in _texts(portfolio.get("key_findings"))],
        f"- Actionability: {portfolio.get('actionability_note')}",
        "",
        "## ETF Baseline State",
        "",
        "| Symbol | Target | Previous | Delta | Composite |",
        "|---|---:|---:|---:|---:|",
    ]
    for row in _records(_mapping(portfolio.get("portfolio_state")).get("target_weights")):
        lines.append(
            "| "
            f"{row.get('symbol')} | "
            f"{_fmt_pct(row.get('target_weight'))} | "
            f"{_fmt_pct(row.get('previous_weight'))} | "
            f"{_fmt_pct(row.get('trade_delta'))} | "
            f"{_fmt_number(row.get('composite_score'))} |"
        )
    lines.extend(
        [
            "",
            "## Shadow Candidate Review",
            "",
            f"- Section Status: {shadow.get('section_status')}",
            f"- Active Candidates: {_mapping(shadow.get('summary')).get('active_candidate_count')}",
            f"- Candidates Requiring Review: "
            f"{_mapping(shadow.get('summary')).get('candidate_requiring_review_count')}",
            "",
            "| Candidate | Status | Return | Excess vs Baseline | Action |",
            "|---|---|---:|---:|---|",
        ]
    )
    for row in _records(shadow.get("active_shadow_candidates")):
        lines.append(
            "| "
            f"{row.get('candidate_id')} | "
            f"{row.get('lifecycle_status')} | "
            f"{_fmt_pct(row.get('return_since_enrollment'))} | "
            f"{_fmt_pct(row.get('excess_return_vs_baseline'))} | "
            f"{row.get('recommended_observation_action')} |"
        )
    if not _records(shadow.get("active_shadow_candidates")):
        lines.append("| none | no_active_candidates | n/a | n/a | continue_observation |")
    lines.extend(
        [
            "",
            "## AI Confirmation Review",
            "",
            f"- Status: {ai.get('section_status')}",
            f"- AIConfirmationScore: {_fmt_number(ai.get('AIConfirmationScore'))}",
            f"- Score Band: {ai.get('score_band')}",
            f"- Interpretation: {ai.get('interpretation')}",
            f"- Candidate-only impact: {ai.get('impact_on_candidate_only_overlays')}",
            "",
            "## Satellite Replacement Review",
            "",
            f"- Status: {satellite.get('section_status')}",
            "- Eligible Stocks: "
            f"{_join(_texts(satellite.get('eligible_satellite_stocks'))) or 'none'}",
            "- Watchlist Stocks: "
            f"{_join(_texts(satellite.get('watchlist_satellite_stocks'))) or 'none'}",
            "- Fallback-to-ETF Stocks: "
            f"{_join(_texts(satellite.get('fallback_to_etf_stocks'))) or 'none'}",
            f"- Candidate-only impact: {satellite.get('candidate_only_replacement_impact')}",
            "",
            "## Risk / Watchlist / Constraints",
            "",
            f"- Status: {risk.get('section_status')}",
            f"- Critical: {_mapping(risk.get('severity_counts')).get('critical')}",
            f"- Warning: {_mapping(risk.get('severity_counts')).get('warning')}",
            f"- Info: {_mapping(risk.get('severity_counts')).get('info')}",
            "",
            "| Severity | Source | Issue | Manual Action |",
            "|---|---|---|---|",
        ]
    )
    for item in _records(risk.get("watchlist_items"))[:20]:
        lines.append(
            "| "
            f"{item.get('severity')} | "
            f"{item.get('source_module')} | "
            f"{item.get('issue')} | "
            f"{item.get('recommended_manual_action')} |"
        )
    if not _records(risk.get("watchlist_items")):
        lines.append("| info | weekly_review | no active warning | continue_observation |")
    lines.extend(
        [
            "",
            "## Manual Review Action Items",
            "",
            "| Priority | Action Type | Reason | Status |",
            "|---|---|---|---|",
        ]
    )
    for action in _records(payload.get("manual_review_actions")):
        lines.append(
            "| "
            f"{action.get('priority')} | "
            f"{action.get('action_type')} | "
            f"{action.get('recommended_reason')} | "
            f"{action.get('status')} |"
        )
    lines.extend(
        [
            "",
            "## Source Report Links",
            "",
            "| Source Module | Report ID | Status | Path |",
            "|---|---|---|---|",
        ]
    )
    for source in _records(payload.get("source_reports")):
        lines.append(
            "| "
            f"{source.get('source_module')} | "
            f"{source.get('report_id')} | "
            f"{source.get('status')} | "
            f"{source.get('source_report_path')} |"
        )
    lines.extend(
        [
            "",
            "## Validation Status",
            "",
            f"- Validation Status: {_mapping(payload.get('validation_status')).get('status')}",
            f"- Validation Summary: {_mapping(payload.get('validation_status')).get('summary')}",
            "",
            "## Next-Week Watch Items",
            "",
        ]
    )
    next_items = _records(payload.get("next_week_watch_items"))
    if not next_items:
        lines.append("- Continue observation; no explicit next-week watch item generated.")
    else:
        for item in next_items:
            lines.append(f"- {item.get('watch_id')}: {item.get('issue')}")
    return "\n".join(lines) + "\n"


def render_weekly_review_validation_markdown(payload: Mapping[str, Any]) -> str:
    lines = [
        "# ETF Weekly Review Validation Gate",
        "",
        f"- Status: {payload.get('status')}",
        "- Safety: observe_only=true, candidate_only=true, production_effect=none, "
        "broker_action=none, manual_review_required=true",
        "",
        "| Check | Status | Message |",
        "|---|---|---|",
    ]
    for check in _records(payload.get("checks")):
        lines.append(
            f"| {check.get('check_id')} | {check.get('status')} | {check.get('message')} |"
        )
    return "\n".join(lines) + "\n"


def _load_or_build_report_index(
    *,
    as_of: date,
    report_index_payload: Mapping[str, Any] | None,
    report_index_path: Path | None,
    report_registry_path: Path,
    project_root: Path,
) -> Mapping[str, Any]:
    if report_index_payload is not None:
        return report_index_payload
    if report_index_path is not None and report_index_path.exists():
        return _read_json_object(report_index_path)
    return build_report_index_payload(
        as_of=as_of,
        project_root=project_root,
        registry_path=report_registry_path,
    )


def _source_report_record(
    report_index: Mapping[str, Any],
    *,
    report_id: str,
    required: bool,
) -> dict[str, Any]:
    index_record = _report_index_record(report_index, report_id)
    path = _path_or_none(index_record.get("latest_artifact_path"))
    payload = _read_json_payload_for_artifact(path)
    exists = path is not None and path.exists()
    if payload:
        status = "loaded"
        reason = "LOADED"
        loaded = True
    elif exists:
        status = "available_unstructured"
        reason = "STRUCTURED_JSON_NOT_AVAILABLE"
        loaded = False
    else:
        status = "missing_data"
        reason = "REPORT_NOT_FOUND"
        loaded = False
    return {
        "report_id": report_id,
        "source_module": SOURCE_MODULE_BY_REPORT_ID.get(report_id, report_id),
        "title": _text(index_record.get("title"), report_id),
        "status": status,
        "reason_code": reason,
        "required": required,
        "loaded": loaded,
        "source_report_path": "" if path is None else str(path),
        "artifact_status": _text(index_record.get("artifact_status"), "MISSING"),
        "freshness_status": _text(index_record.get("freshness_status"), "MISSING"),
        "source_metric": _source_metric(report_id),
        "time_window": {
            "artifact_date": _text(index_record.get("artifact_date")),
        },
        "payload": payload,
    }


def _source_report_public(record: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "report_id": record.get("report_id"),
        "source_module": record.get("source_module"),
        "title": record.get("title"),
        "status": record.get("status"),
        "reason_code": record.get("reason_code"),
        "required": record.get("required"),
        "loaded": record.get("loaded"),
        "source_report_path": record.get("source_report_path"),
        "artifact_status": record.get("artifact_status"),
        "freshness_status": record.get("freshness_status"),
        "source_metric": record.get("source_metric"),
        "time_window": record.get("time_window"),
    }


def _source_safety_issues(record: Mapping[str, Any]) -> list[str]:
    payload = _mapping(record.get("payload"))
    if not payload:
        return []
    issues: list[str] = []
    for key, expected in WEEKLY_REVIEW_SAFETY.items():
        if key not in payload:
            continue
        if payload.get(key) != expected:
            issues.append(f"{record.get('report_id')}:{key}!={expected}")
    if payload.get("production_promotion_allowed") is True:
        issues.append(f"{record.get('report_id')}:production_promotion_allowed=true")
    return issues


def _load_portfolio_state(*, as_of: date, target_weights_path: Path) -> dict[str, Any]:
    if not target_weights_path.exists():
        return {
            "status": "missing_data",
            "reason_code": "TARGET_WEIGHTS_NOT_FOUND",
            "target_weights": [],
            "source_report_path": str(target_weights_path),
        }
    try:
        frame = pd.read_csv(target_weights_path)
    except Exception as exc:  # noqa: BLE001 - weekly review must report unreadable data.
        return {
            "status": "missing_data",
            "reason_code": f"TARGET_WEIGHTS_UNREADABLE:{exc}",
            "target_weights": [],
            "source_report_path": str(target_weights_path),
        }
    if frame.empty or "date" not in frame.columns:
        return {
            "status": "missing_data",
            "reason_code": "TARGET_WEIGHTS_EMPTY",
            "target_weights": [],
            "source_report_path": str(target_weights_path),
        }
    dates = pd.to_datetime(frame["date"], errors="coerce").dt.date
    eligible = frame.loc[dates <= as_of].copy()
    if eligible.empty:
        return {
            "status": "missing_data",
            "reason_code": "NO_TARGET_WEIGHTS_ON_OR_BEFORE_AS_OF",
            "target_weights": [],
            "source_report_path": str(target_weights_path),
        }
    eligible["_parsed_date"] = pd.to_datetime(eligible["date"], errors="coerce").dt.date
    latest_date = max(item for item in eligible["_parsed_date"] if item is not None)
    latest = eligible.loc[eligible["_parsed_date"] == latest_date].copy()
    rows = [_target_weight_record(row) for _, row in latest.sort_values("symbol").iterrows()]
    return {
        "status": "loaded",
        "reason_code": "LOADED",
        "as_of": latest_date.isoformat(),
        "target_weights": rows,
        "market_regime": _first_text(rows, "regime"),
        "data_quality_status": _first_text(rows, "data_quality_status"),
        "model_version": _first_text(rows, "model_version"),
        "config_hash": _first_text(rows, "config_hash"),
        "source_report_path": str(target_weights_path),
        "row_count": len(rows),
        "checksum": _hash_payload(rows),
    }


def _target_weight_record(row: pd.Series) -> dict[str, Any]:
    return {
        "symbol": _text(row.get("symbol")),
        "target_weight": _float_or_none(row.get("target_weight")),
        "previous_weight": _float_or_none(row.get("previous_weight")),
        "trade_delta": _float_or_none(row.get("trade_delta")),
        "composite_score": _float_or_none(row.get("composite_score")),
        "regime": _text(row.get("regime")),
        "reason_codes": _json_list(row.get("reason_codes")),
        "constraints_applied": _json_list(row.get("constraints_applied")),
        "model_version": _text(row.get("model_version")),
        "config_hash": _text(row.get("config_hash")),
        "data_quality_status": _text(row.get("data_quality_status")),
    }


def _weight_change_summary(weights: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    rows = list(weights)
    deltas = [
        (_text(row.get("symbol")), _float_or_default(row.get("trade_delta"), 0.0)) for row in rows
    ]
    changed = [(symbol, delta) for symbol, delta in deltas if abs(delta) > 1e-12]
    largest = max(deltas, key=lambda item: abs(item[1])) if deltas else ("MISSING", 0.0)
    return {
        "changed_symbol_count": len(changed),
        "largest_change": f"{largest[0]} {largest[1]:+.2%}",
        "total_abs_trade_delta": round(sum(abs(delta) for _, delta in deltas), 8),
        "changes": [{"symbol": symbol, "trade_delta": delta} for symbol, delta in changed],
    }


def _benchmark_context(weights: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    symbols = [_text(row.get("symbol")) for row in weights if _text(row.get("symbol"))]
    benchmarks = [symbol for symbol in symbols if symbol in {"SPY", "QQQ", "SMH", "SOXX"}]
    return {
        "status": "available" if benchmarks else "missing_benchmark_symbols",
        "primary_context": (
            "ETF baseline reviewed against SPY / QQQ / SMH where artifacts provide " "comparisons."
        ),
        "symbols": benchmarks,
    }


def _shadow_candidate_row(
    row: Mapping[str, Any],
    *,
    status_changes: Sequence[Mapping[str, Any]],
    source_path: str,
) -> dict[str, Any]:
    candidate_id = _text(row.get("candidate_id"), _text(row.get("experiment_id"), "UNKNOWN"))
    status = _text(row.get("status"), "needs_more_data")
    action = _shadow_action_from_status(status, row, status_changes)
    return {
        "shadow_id": row.get("shadow_id"),
        "candidate_id": candidate_id,
        "experiment_id": row.get("experiment_id"),
        "lifecycle_status": status,
        "days_since_enrollment": row.get("days_since_enrollment"),
        "return_since_enrollment": row.get("return_since_enrollment"),
        "excess_return_vs_baseline": row.get("excess_return_vs_baseline"),
        "excess_return_vs_QQQ": row.get("excess_return_vs_QQQ"),
        "excess_return_vs_SPY": row.get("excess_return_vs_SPY"),
        "excess_return_vs_SMH": row.get("excess_return_vs_SMH"),
        "max_drawdown_since_enrollment": row.get("max_drawdown_since_enrollment"),
        "rolling_metrics": row.get("rolling_metrics", {}),
        "candidate_status_change": _matching_status_change(row, status_changes),
        "recommended_observation_action": action,
        "source_report_path": source_path,
        "evidence": [
            _evidence(
                source_module="etf_forward_dashboard",
                source_report_path=source_path,
                source_metric="candidate_summary_table",
                time_window=_text(row.get("last_evaluated_date")),
                reason_code=action,
            )
        ],
    }


def _shadow_action_from_status(
    status: str,
    row: Mapping[str, Any],
    status_changes: Sequence[Mapping[str, Any]],
) -> str:
    if _text(row.get("recommended_action")) in ALLOWED_SHADOW_ACTIONS:
        return _text(row.get("recommended_action"))
    change = _matching_status_change(row, status_changes)
    if _text(change.get("recommended_action")) in ALLOWED_SHADOW_ACTIONS:
        return _text(change.get("recommended_action"))
    if status in {"reject_pending_review", "watch", "needs_more_data"}:
        return status
    return "continue_shadow"


def _matching_status_change(
    row: Mapping[str, Any],
    status_changes: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    shadow_id = _text(row.get("shadow_id"))
    candidate_id = _text(row.get("candidate_id"))
    for change in status_changes:
        if (
            _text(change.get("shadow_id")) == shadow_id
            or _text(change.get("candidate_id")) == candidate_id
        ):
            return dict(change)
    return {}


def _new_shadow_candidates(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    return [
        dict(row) for row in rows if (_float_or_none(row.get("days_since_enrollment")) or 9999) <= 7
    ]


def _validate_shadow_action(action: str) -> None:
    if action not in ALLOWED_SHADOW_ACTIONS:
        raise ValueError(f"unsafe shadow candidate action: {action}")


def _ai_interpretation(status: str) -> str:
    if status == "supports_ai_overweight_candidate":
        return "AI confirmation supports candidate-only AI/semiconductor overweight review."
    if status == "supports_neutral_ai_exposure":
        return "AI confirmation supports neutral AI exposure; continue observation."
    if status == "warns_against_ai_overweight":
        return "AI confirmation warns against adding AI/semiconductor overweight."
    return "AI confirmation has insufficient data; no overlay recommendation."


def _satellite_event_risk_context(report: Mapping[str, Any]) -> dict[str, Any]:
    eligibility = _records(report.get("replacement_eligibility"))
    blockers = sorted(
        {
            str(blocker)
            for row in eligibility
            for blocker in _texts(row.get("blockers"))
            if "event" in str(blocker).lower()
        }
    )
    return {"event_risk_blockers": blockers, "status": "watch" if blockers else "clear"}


def _forward_watchlist_items(aggregation_payload: Mapping[str, Any]) -> list[dict[str, Any]]:
    watchlist = _mapping(
        _mapping(aggregation_payload.get("source_payloads")).get("etf_forward_watchlist")
    )
    source_path = _source_path(aggregation_payload, "etf_forward_watchlist")
    items = []
    for index, item in enumerate(_records(watchlist.get("attention_required")), start=1):
        items.append(
            _watch_item(
                watch_id=f"forward-{index}",
                source_module="etf_forward_watchlist",
                severity=_severity(_text(item.get("severity"), "warning")),
                issue=_text(item.get("issue"), "forward watchlist item"),
                affected_assets_or_candidates=_texts(item.get("affected_assets_or_candidates"))
                or [_text(item.get("candidate_id"), "UNKNOWN")],
                since_date=_text(item.get("since_date"), _text(watchlist.get("as_of"))),
                recommended_manual_action=_text(
                    item.get("recommended_action"),
                    "review_candidate",
                ),
                source_report_path=source_path,
                source_metric="attention_required",
                reason_code=_text(item.get("reason_code"), "forward_watchlist"),
            )
        )
    return items


def _missing_or_stale_source_items(
    aggregation_payload: Mapping[str, Any],
) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for source in _records(aggregation_payload.get("source_reports")):
        status = _text(source.get("status"))
        freshness = _text(source.get("freshness_status"))
        if status == "missing_data":
            severity = "warning"
            if source.get("required") is True:
                severity = "critical"
            items.append(
                _watch_item(
                    watch_id=f"missing-{source.get('report_id')}",
                    source_module=_text(source.get("source_module")),
                    severity=severity,
                    issue=f"{source.get('report_id')} missing or unavailable.",
                    affected_assets_or_candidates=[],
                    since_date=_text(_mapping(source.get("time_window")).get("artifact_date")),
                    recommended_manual_action="review_data_gap",
                    source_report_path=_text(source.get("source_report_path")),
                    source_metric=_text(source.get("source_metric")),
                    reason_code=_text(source.get("reason_code"), "REPORT_NOT_FOUND"),
                )
            )
        elif freshness == "STALE":
            items.append(
                _watch_item(
                    watch_id=f"stale-{source.get('report_id')}",
                    source_module=_text(source.get("source_module")),
                    severity="warning",
                    issue=f"{source.get('report_id')} stale relative to weekly review.",
                    affected_assets_or_candidates=[],
                    since_date=_text(_mapping(source.get("time_window")).get("artifact_date")),
                    recommended_manual_action="review_data_gap",
                    source_report_path=_text(source.get("source_report_path")),
                    source_metric=_text(source.get("source_metric")),
                    reason_code="STALE_REPORT",
                )
            )
    return items


def _validation_gate_items(aggregation_payload: Mapping[str, Any]) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    sources = _mapping(aggregation_payload.get("source_payloads"))
    for report_id in sorted(VALIDATION_REPORT_IDS):
        payload = _mapping(sources.get(report_id))
        if not payload:
            continue
        status = _text(payload.get("status"), _text(payload.get("gate_status"), "UNKNOWN"))
        if status == "PASS":
            continue
        items.append(
            _watch_item(
                watch_id=f"validation-{report_id}",
                source_module=SOURCE_MODULE_BY_REPORT_ID.get(report_id, report_id),
                severity="critical",
                issue=f"{report_id} status is {status}.",
                affected_assets_or_candidates=[],
                since_date=_text(payload.get("generated_at")),
                recommended_manual_action="review_data_gap",
                source_report_path=_source_path(aggregation_payload, report_id),
                source_metric="status",
                reason_code=f"VALIDATION_{status}",
            )
        )
    return items


def _candidate_lifecycle_items(shadow_review: Mapping[str, Any]) -> list[dict[str, Any]]:
    items = []
    for row in _records(shadow_review.get("active_shadow_candidates")):
        action = _text(row.get("recommended_observation_action"))
        if action not in {"watch", "reject_pending_review", "needs_more_data"}:
            continue
        severity = "critical" if action == "reject_pending_review" else "warning"
        items.append(
            _watch_item(
                watch_id=f"candidate-{row.get('candidate_id')}-{action}",
                source_module="etf_forward_dashboard",
                severity=severity,
                issue=f"Shadow candidate {row.get('candidate_id')} action {action}.",
                affected_assets_or_candidates=[_text(row.get("candidate_id"))],
                since_date=_text(row.get("last_evaluated_date")),
                recommended_manual_action="review_candidate",
                source_report_path=_text(row.get("source_report_path")),
                source_metric="recommended_observation_action",
                reason_code=action,
            )
        )
    return items


def _ai_event_risk_items(ai_review: Mapping[str, Any]) -> list[dict[str, Any]]:
    event_risk = _mapping(ai_review.get("event_risk"))
    risk_band = _text(event_risk.get("risk_band"))
    if risk_band not in {"high", "critical"}:
        return []
    return [
        _watch_item(
            watch_id=f"ai-event-risk-{risk_band}",
            source_module="ai_confirmation",
            severity="critical" if risk_band == "critical" else "warning",
            issue=f"AI confirmation event risk is {risk_band}.",
            affected_assets_or_candidates=_texts(event_risk.get("affected_groups")),
            since_date="",
            recommended_manual_action="review_event_risk",
            source_report_path=_text(ai_review.get("source_report_path")),
            source_metric="event_risk_overlay",
            reason_code=f"event_risk_{risk_band}",
        )
    ]


def _satellite_constraint_items(satellite_review: Mapping[str, Any]) -> list[dict[str, Any]]:
    constraints = _texts(satellite_review.get("constraints_applied"))
    if not constraints:
        return []
    return [
        _watch_item(
            watch_id="satellite-constraints",
            source_module="satellite_replacement",
            severity="info",
            issue=f"Satellite constraints applied: {_join(constraints)}.",
            affected_assets_or_candidates=_texts(satellite_review.get("eligible_satellite_stocks")),
            since_date="",
            recommended_manual_action="review_candidate",
            source_report_path=_text(satellite_review.get("source_report_path")),
            source_metric="constraints_applied",
            reason_code="SATELLITE_CONSTRAINTS_APPLIED",
        )
    ]


def _data_quality_items(aggregation_payload: Mapping[str, Any]) -> list[dict[str, Any]]:
    state = _mapping(aggregation_payload.get("portfolio_state"))
    status = _text(state.get("data_quality_status"))
    if status in {"", "PASS", "PASS_WITH_WARNINGS"}:
        return []
    return [
        _watch_item(
            watch_id="portfolio-data-quality",
            source_module="etf_allocation",
            severity="warning",
            issue=f"Portfolio data quality status is {status}.",
            affected_assets_or_candidates=[],
            since_date=_text(state.get("as_of")),
            recommended_manual_action="review_data_gap",
            source_report_path=_text(state.get("source_report_path")),
            source_metric="data_quality_status",
            reason_code=f"DATA_QUALITY_{status}",
        )
    ]


def _constraint_hits(
    aggregation_payload: Mapping[str, Any],
    satellite_review: Mapping[str, Any],
) -> list[dict[str, Any]]:
    dashboard = _mapping(
        _mapping(aggregation_payload.get("source_payloads")).get("etf_forward_dashboard")
    )
    hits: list[dict[str, Any]] = []
    constraint_summary = _mapping(dashboard.get("constraint_hit_summary"))
    if constraint_summary:
        hits.append(
            {
                "source_module": "etf_forward_dashboard",
                "constraint_summary": dict(constraint_summary),
                "source_report_path": _source_path(aggregation_payload, "etf_forward_dashboard"),
            }
        )
    if _texts(satellite_review.get("constraints_applied")):
        hits.append(
            {
                "source_module": "satellite_replacement",
                "constraints_applied": _texts(satellite_review.get("constraints_applied")),
                "source_report_path": satellite_review.get("source_report_path"),
            }
        )
    return hits


def _watch_item(
    *,
    watch_id: str,
    source_module: str,
    severity: str,
    issue: str,
    affected_assets_or_candidates: Sequence[str],
    since_date: str,
    recommended_manual_action: str,
    source_report_path: str,
    source_metric: str,
    reason_code: str,
) -> dict[str, Any]:
    return {
        "watch_id": watch_id,
        "source_module": source_module,
        "severity": _severity(severity),
        "issue": issue,
        "affected_assets_or_candidates": list(affected_assets_or_candidates),
        "since_date": since_date,
        "recommended_manual_action": recommended_manual_action,
        "source_report_path": source_report_path,
        "source_metric": source_metric,
        "reason_code": reason_code,
    }


def _manual_action(
    *,
    action_type: str,
    priority: str,
    source_module: str,
    evidence: Sequence[Mapping[str, Any]],
    recommended_reason: str,
    as_of: date,
    generated_at: datetime,
) -> dict[str, Any]:
    safe_type = _text(action_type)
    if safe_type in UNSAFE_ACTION_TYPES or safe_type not in ALLOWED_MANUAL_ACTION_TYPES:
        raise ValueError(f"unsafe or unsupported weekly review action type: {safe_type}")
    basis = "|".join(
        [
            safe_type,
            priority,
            source_module,
            recommended_reason,
            as_of.isoformat(),
        ]
    )
    return {
        "action_id": "weekly-action-" + sha256(basis.encode("utf-8")).hexdigest()[:12],
        "action_type": safe_type,
        "priority": priority,
        "source_module": source_module,
        "evidence": [dict(item) for item in evidence],
        "recommended_reason": recommended_reason,
        "requires_manual_review": True,
        "status": "open",
        "created_at": generated_at.isoformat(),
    }


def _action_type_for_watch_item(item: Mapping[str, Any]) -> str:
    recommended = _text(item.get("recommended_manual_action"))
    if "event" in recommended or "event" in _text(item.get("issue")).lower():
        return "review_event_risk"
    if "candidate" in recommended:
        return "review_candidate"
    return "review_data_gap"


def _priority_for_severity(severity: str) -> str:
    if severity == "critical":
        return "high"
    if severity == "warning":
        return "medium"
    return "low"


def _watch_item_evidence(item: Mapping[str, Any]) -> dict[str, Any]:
    return _evidence(
        source_module=_text(item.get("source_module")),
        source_report_path=_text(item.get("source_report_path")),
        source_metric=_text(item.get("source_metric")),
        time_window=_text(item.get("since_date")),
        reason_code=_text(item.get("reason_code")),
    )


def _evidence(
    *,
    source_module: str,
    source_report_path: str,
    source_metric: str,
    time_window: str,
    reason_code: str,
) -> dict[str, Any]:
    return {
        "source_module": source_module,
        "source_report_path": source_report_path,
        "source_metric": source_metric,
        "time_window": time_window,
        "reason_code": reason_code,
    }


def _next_week_watch_items(
    *,
    risk_summary: Mapping[str, Any],
    shadow_review: Mapping[str, Any],
    satellite_review: Mapping[str, Any],
) -> list[dict[str, Any]]:
    items = [
        dict(item)
        for item in _records(risk_summary.get("watchlist_items"))
        if item.get("severity") in {"critical", "warning"}
    ][:10]
    if _records(shadow_review.get("active_shadow_candidates")):
        items.append(
            {
                "watch_id": "shadow-candidate-forward-window",
                "source_module": "etf_forward_dashboard",
                "severity": "info",
                "issue": "Review next weekly forward window for active shadow candidates.",
                "recommended_manual_action": "continue_observation",
                "source_report_path": shadow_review.get("source_report_path"),
            }
        )
    if _text(satellite_review.get("section_status")) in {
        "eligible_candidates_present",
        "watch_only",
    }:
        items.append(
            {
                "watch_id": "satellite-replacement-watch",
                "source_module": "satellite_replacement",
                "severity": "info",
                "issue": "Recheck satellite replacement eligibility and ETF fallback next week.",
                "recommended_manual_action": "review_candidate",
                "source_report_path": satellite_review.get("source_report_path"),
            }
        )
    return _dedupe_watch_items(items)


def _validation_status_from_sources(aggregation_payload: Mapping[str, Any]) -> dict[str, Any]:
    sources = _mapping(aggregation_payload.get("source_payloads"))
    rows = []
    for report_id in sorted(VALIDATION_REPORT_IDS):
        payload = _mapping(sources.get(report_id))
        rows.append(
            {
                "report_id": report_id,
                "status": _text(payload.get("status"), "MISSING") if payload else "MISSING",
                "source_report_path": _source_path(aggregation_payload, report_id),
            }
        )
    failed = [row for row in rows if row["status"] not in {"PASS", "MISSING"}]
    missing = [row for row in rows if row["status"] == "MISSING"]
    if failed:
        status = "FAIL"
    elif missing:
        status = "PASS_WITH_WARNINGS"
    else:
        status = "PASS"
    return {
        "status": status,
        "summary": f"{len(failed)} failed validation gates; {len(missing)} missing gates.",
        "gates": rows,
    }


def _overall_review_status(
    *,
    portfolio_summary: Mapping[str, Any],
    shadow_review: Mapping[str, Any],
    ai_review: Mapping[str, Any],
    satellite_review: Mapping[str, Any],
    risk_summary: Mapping[str, Any],
) -> str:
    if _text(portfolio_summary.get("summary_status")) == "insufficient_data":
        return "insufficient_data"
    if _mapping(risk_summary.get("severity_counts")).get("critical"):
        return "risk_watch"
    if _text(shadow_review.get("section_status")) == "candidate_review_required":
        return "candidate_review_required"
    if _text(satellite_review.get("section_status")) == "eligible_candidates_present":
        return "candidate_review_required"
    if _text(ai_review.get("section_status")) == "warns_against_ai_overweight":
        return "risk_watch"
    return "stable_observe"


def _append_validation_check(
    checks: list[dict[str, Any]],
    check_id: str,
    passed: bool,
    message: str,
    details: Mapping[str, Any] | None = None,
) -> None:
    checks.append(
        {
            "check_id": check_id,
            "status": "PASS" if passed else "FAIL",
            "message": message,
            "details": dict(details or {}),
        }
    )


def _sample_aggregation(generated: datetime) -> dict[str, Any]:
    as_of = generated.date()
    return {
        "schema_version": WEEKLY_REVIEW_AGGREGATION_SCHEMA_VERSION,
        "report_type": "etf_weekly_review_aggregation",
        "aggregation_status": "PASS",
        "as_of": as_of.isoformat(),
        "generated_at": generated.isoformat(),
        "model_version": "validation_fixture",
        "config_hash": "validation_fixture_hash",
        "data_hash_or_snapshot": {"sample": True},
        "source_reports": [
            {
                "report_id": "validation_fixture",
                "source_module": "weekly_review_validation",
                "status": "loaded",
                "source_report_path": "validation_fixture.json",
                "source_metric": "sample",
                "reason_code": "VALIDATION_FIXTURE",
            }
        ],
        "portfolio_state": {
            "status": "loaded",
            "as_of": as_of.isoformat(),
            "market_regime": "ai_after_chatgpt",
            "data_quality_status": "PASS",
            "model_version": "validation_fixture",
            "config_hash": "validation_fixture_hash",
            "target_weights": [
                {"symbol": "SPY", "target_weight": 0.3, "previous_weight": 0.3, "trade_delta": 0.0},
                {"symbol": "QQQ", "target_weight": 0.4, "previous_weight": 0.4, "trade_delta": 0.0},
                {
                    "symbol": "SMH",
                    "target_weight": 0.15,
                    "previous_weight": 0.15,
                    "trade_delta": 0.0,
                },
                {
                    "symbol": "CASH",
                    "target_weight": 0.15,
                    "previous_weight": 0.15,
                    "trade_delta": 0.0,
                },
            ],
        },
        "source_payloads": {
            "etf_forward_dashboard": {
                "status": "NO_ACTIVE_SHADOW_CANDIDATES",
                "candidate_summary_table": [],
                **WEEKLY_REVIEW_SAFETY,
            }
        },
        **WEEKLY_REVIEW_SAFETY,
    }


def _unsafe_action_validation_passes() -> bool:
    try:
        validate_weekly_review_action_items(
            [
                {
                    "action_type": "place_order",
                    "requires_manual_review": True,
                    "evidence": [{"source_module": "test"}],
                }
            ]
        )
    except ValueError:
        return True
    return False


def _contains_unsafe_key(value: object) -> bool:
    if isinstance(value, Mapping):
        for key, child in value.items():
            if str(key) in UNSAFE_OUTPUT_KEYS:
                return True
            if _contains_unsafe_key(child):
                return True
    elif isinstance(value, list):
        return any(_contains_unsafe_key(item) for item in value)
    return False


def _aggregation_warnings(
    missing_sections: Sequence[Mapping[str, Any]],
    safety_issues: Sequence[str],
) -> list[dict[str, Any]]:
    warnings: list[dict[str, Any]] = []
    for section in missing_sections:
        warnings.append(
            {
                "warning_type": "missing_data",
                "source_module": section.get("source_module"),
                "report_id": section.get("report_id"),
                "reason_code": section.get("reason_code"),
                "source_report_path": section.get("source_report_path"),
            }
        )
    for issue in safety_issues:
        warnings.append({"warning_type": "safety_review_required", "issue": issue})
    return warnings


def _data_snapshot(
    *,
    source_reports: Sequence[Mapping[str, Any]],
    portfolio_state: Mapping[str, Any],
) -> dict[str, Any]:
    paths = [
        _text(record.get("source_report_path"))
        for record in source_reports
        if _text(record.get("source_report_path"))
    ]
    snapshot = {
        "source_report_count": len(paths),
        "source_report_paths": sorted(paths),
        "target_weights_checksum": _text(portfolio_state.get("checksum")),
    }
    snapshot["checksum"] = _hash_payload(snapshot)
    return snapshot


def _report_index_record(payload: Mapping[str, Any], report_id: str) -> dict[str, Any]:
    for report in _records(payload.get("reports")):
        if _text(report.get("report_id")) == report_id:
            return dict(report)
    return {"report_id": report_id, "freshness_status": "MISSING", "artifact_status": "MISSING"}


def _read_json_payload_for_artifact(path: Path | None) -> dict[str, Any]:
    if path is None or not path.exists():
        return {}
    if path.suffix.lower() == ".json":
        return _read_json_object(path)
    companion = path.with_suffix(".json")
    if companion.exists():
        return _read_json_object(companion)
    return {}


def _read_json_object(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return dict(payload) if isinstance(payload, Mapping) else {}


def _write_json(payload: Mapping[str, Any], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _safe_report_registry(path: Path) -> dict[str, Any]:
    try:
        return load_report_registry(path)
    except Exception:  # noqa: BLE001 - validation report records fail through checks.
        return {}


def _safe_config_bundle() -> Any | None:
    try:
        return load_etf_config_bundle()
    except Exception:  # noqa: BLE001 - weekly report can still summarize artifacts.
        return None


def _model_version(portfolio_state: Mapping[str, Any], config: Any | None) -> str:
    model = (
        getattr(getattr(config, "strategy", None), "model", None) if config is not None else None
    )
    return _text(
        portfolio_state.get("model_version"),
        _text(getattr(model, "version", ""), "UNKNOWN"),
    )


def _config_hash(portfolio_state: Mapping[str, Any], config: Any | None) -> str:
    return _text(portfolio_state.get("config_hash"), _text(getattr(config, "config_hash", "")))


def _source_metric(report_id: str) -> str:
    return {
        "etf_portfolio_brief": "daily_brief_markdown",
        "etf_data_quality": "data_quality_status",
        "etf_experiment_candidate_selection": "candidates",
        "etf_shadow_candidates": "candidates",
        "etf_experiment_weekly_review": "candidate_reviews",
        "etf_forward_dashboard": "candidate_summary_table",
        "etf_forward_weekly_review": "candidate_status_changes",
        "etf_forward_watchlist": "attention_required",
        "etf_ai_confirmation_report": "AIConfirmationScore",
        "etf_satellite_replacement_report": "replacement_eligibility",
        "etf_credibility_gate": "status",
        "etf_experiment_validation": "status",
        "etf_forward_validation": "status",
        "etf_ai_confirmation_validation": "status",
        "etf_satellite_validation": "status",
    }.get(report_id, "status")


def _source_path(aggregation_payload: Mapping[str, Any], report_id: str) -> str:
    for source in _records(aggregation_payload.get("source_reports")):
        if _text(source.get("report_id")) == report_id:
            return _text(source.get("source_report_path"))
    return ""


def _path_or_none(value: object) -> Path | None:
    text = _text(value)
    return None if not text else Path(text)


def _mapping(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _records(value: object) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [dict(item) for item in value if isinstance(item, Mapping)]


def _texts(value: object) -> list[str]:
    if isinstance(value, list):
        return [_text(item) for item in value if _text(item)]
    if isinstance(value, tuple):
        return [_text(item) for item in value if _text(item)]
    if _text(value):
        return [_text(value)]
    return []


def _text(value: object, default: str = "") -> str:
    if value is None:
        return default
    text = str(value).strip()
    return text if text else default


def _float_or_none(value: object) -> float | None:
    if value is None or value == "":
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if pd.isna(parsed):
        return None
    return parsed


def _float_or_default(value: object, default: float) -> float:
    parsed = _float_or_none(value)
    return default if parsed is None else parsed


def _json_list(value: object) -> list[str]:
    if isinstance(value, list):
        return [str(item) for item in value]
    if not isinstance(value, str) or not value.strip():
        return []
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError:
        return [value]
    if isinstance(parsed, list):
        return [str(item) for item in parsed]
    return [str(parsed)]


def _first_text(rows: Sequence[Mapping[str, Any]], key: str) -> str:
    for row in rows:
        if _text(row.get(key)):
            return _text(row.get(key))
    return ""


def _hash_payload(payload: object) -> str:
    return sha256(
        json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str).encode("utf-8")
    ).hexdigest()


def _severity(value: str) -> str:
    normalized = value.lower()
    if normalized in {"critical", "warning", "info"}:
        return normalized
    return "warning"


def _dedupe_watch_items(items: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    seen: set[str] = set()
    result: list[dict[str, Any]] = []
    order = {"critical": 0, "warning": 1, "info": 2}
    for item in sorted(
        (dict(item) for item in items),
        key=lambda row: (order.get(_text(row.get("severity")), 9), _text(row.get("watch_id"))),
    ):
        key = _text(item.get("watch_id")) or _hash_payload(item)
        if key in seen:
            continue
        seen.add(key)
        result.append(item)
    return result


def _dedupe_actions(actions: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    seen: set[str] = set()
    result: list[dict[str, Any]] = []
    order = {"high": 0, "medium": 1, "low": 2}
    for action in sorted(
        (dict(action) for action in actions),
        key=lambda row: (order.get(_text(row.get("priority")), 9), _text(row.get("action_id"))),
    ):
        key = _text(action.get("action_id")) or _hash_payload(action)
        if key in seen:
            continue
        seen.add(key)
        result.append(action)
    return result


def _actionability_note() -> str:
    return "decision-support only; no production weight mutation; manual review required."


def _safety_banner() -> str:
    return (
        "observe_only=true; candidate_only=true; production_effect=none; "
        "broker_action=none; manual_review_required=true"
    )


def _fmt_pct(value: object) -> str:
    parsed = _float_or_none(value)
    return "n/a" if parsed is None else f"{parsed:.2%}"


def _fmt_number(value: object) -> str:
    parsed = _float_or_none(value)
    return "n/a" if parsed is None else f"{parsed:.2f}"


def _join(values: Sequence[str]) -> str:
    return ", ".join(str(item) for item in values if str(item))
