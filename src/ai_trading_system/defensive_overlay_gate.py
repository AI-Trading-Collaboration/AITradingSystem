from __future__ import annotations

import json
import math
import re
import subprocess
from collections.abc import Mapping, Sequence
from datetime import date
from hashlib import sha256
from pathlib import Path
from typing import Any

import pandas as pd
import yaml

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.data_foundation import AI_REGIME_START, utc_now_iso
from ai_trading_system.expanded_allocation_universe import (
    DEFAULT_ACTUAL_PATH_OUTPUT_ROOT,
    DEFAULT_EXPANDED_UNIVERSE_CONFIG_PATH,
    DEFAULT_FAILURE_MATRIX_CSV_PATH,
    DEFAULT_MARKETSTACK_PRICES_PATH,
    DEFAULT_PRICES_PATH,
    DEFAULT_RATES_PATH,
    _constant_target_frame,
    _data_quality_gate,
    _load_price_matrix,
    _simulate_rebalanced_portfolio,
    _slice_prices,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

DEFAULT_DEFENSIVE_OVERLAY_CONFIG_PATH = (
    PROJECT_ROOT / "config" / "research" / "defensive_overlay_gate.yaml"
)
DEFAULT_DEFENSIVE_OVERLAY_OUTPUT_ROOT = (
    PROJECT_ROOT / "outputs" / "research_strategies" / "defensive_overlay"
)
DEFAULT_OVERLAY_METRICS_CSV_PATH = DEFAULT_DEFENSIVE_OVERLAY_OUTPUT_ROOT / "overlay_metrics.csv"

DEFAULT_NO_SURVIVOR_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "expanded_universe_no_survivor_diagnosis.md"
)
DEFAULT_NO_SURVIVOR_YAML_PATH = (
    PROJECT_ROOT / "inputs" / "research_reviews" / "expanded_universe_no_survivor_diagnosis.yaml"
)
DEFAULT_RECLASSIFICATION_MATRIX_PATH = (
    PROJECT_ROOT / "inputs" / "research_reviews" / "expanded_candidate_reclassification_matrix.yaml"
)
DEFAULT_RISK_OFF_ATTRIBUTION_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "defensive_overlay_risk_off_attribution_review.md"
)
DEFAULT_RISK_OFF_ATTRIBUTION_YAML_PATH = (
    PROJECT_ROOT / "inputs" / "research_reviews" / "defensive_overlay_risk_off_attribution.yaml"
)
DEFAULT_RE_RISK_TIMING_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "defensive_overlay_re_risk_timing_review.md"
)
DEFAULT_DOWNSIDE_STRESS_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "defensive_overlay_downside_stress_review.md"
)
DEFAULT_STATIC_FRONTIER_INTERPRETATION_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "static_frontier_domination_overlay_interpretation.md"
)
DEFAULT_TQQQ_OVERLAY_SAFETY_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "tqqq_overlay_safety_review.md"
)
DEFAULT_OVERLAY_CANDIDATE_SET_PATH = (
    PROJECT_ROOT / "inputs" / "research_reviews" / "defensive_overlay_candidate_set.yaml"
)
DEFAULT_OVERLAY_SURVIVAL_MATRIX_PATH = (
    PROJECT_ROOT / "inputs" / "research_reviews" / "defensive_overlay_survival_matrix.yaml"
)
DEFAULT_OVERLAY_SURVIVAL_REVIEW_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "defensive_overlay_survival_review.md"
)
DEFAULT_NET_COST_REVIEW_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "defensive_overlay_net_of_cost_review.md"
)
DEFAULT_STRESS_GATE_PATH = (
    PROJECT_ROOT / "inputs" / "research_reviews" / "defensive_overlay_stress_gate.yaml"
)
DEFAULT_WALK_FORWARD_SPLIT_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "expanded_universe_walk_forward_split_evidence_review.md"
)
DEFAULT_WALK_FORWARD_SPLIT_YAML_PATH = (
    PROJECT_ROOT
    / "inputs"
    / "research_reviews"
    / "expanded_universe_walk_forward_split_evidence.yaml"
)
DEFAULT_OWNER_REVIEW_PACK_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "defensive_overlay_owner_review_pack.md"
)
DEFAULT_FORWARD_WATCH_PLAN_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "defensive_overlay_forward_watch_plan.md"
)

DEFAULT_AI_REGIME_BACKTEST_START = (
    AI_REGIME_START
    if isinstance(AI_REGIME_START, date)
    else date.fromisoformat(str(AI_REGIME_START))
)
GRID_ROUND_DIGITS = 6
SAFETY_BOUNDARY: dict[str, Any] = {
    "research_only": True,
    "defensive_overlay_only": True,
    "watch_only": True,
    "full_allocation_promotion_allowed": False,
    "promotion_allowed": False,
    "paper_shadow_allowed": False,
    "production_allowed": False,
    "broker_action": "none",
    "production_effect": "none",
    "manual_review_required": True,
    "dynamic_promotion_status": "BLOCKED",
    "target_path_metrics_role": "diagnostic_only",
}


def run_defensive_overlay_research_pack(
    *,
    config_path: Path = DEFAULT_DEFENSIVE_OVERLAY_CONFIG_PATH,
    expanded_config_path: Path = DEFAULT_EXPANDED_UNIVERSE_CONFIG_PATH,
    failure_matrix_path: Path = DEFAULT_FAILURE_MATRIX_CSV_PATH,
    actual_path_root: Path = DEFAULT_ACTUAL_PATH_OUTPUT_ROOT,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    as_of_date: date | None = None,
    output_root: Path = DEFAULT_DEFENSIVE_OVERLAY_OUTPUT_ROOT,
    overlay_metrics_csv_path: Path = DEFAULT_OVERLAY_METRICS_CSV_PATH,
    no_survivor_doc_path: Path = DEFAULT_NO_SURVIVOR_DOC_PATH,
    no_survivor_yaml_path: Path = DEFAULT_NO_SURVIVOR_YAML_PATH,
    reclassification_yaml_path: Path = DEFAULT_RECLASSIFICATION_MATRIX_PATH,
    risk_off_doc_path: Path = DEFAULT_RISK_OFF_ATTRIBUTION_DOC_PATH,
    risk_off_yaml_path: Path = DEFAULT_RISK_OFF_ATTRIBUTION_YAML_PATH,
    re_risk_doc_path: Path = DEFAULT_RE_RISK_TIMING_DOC_PATH,
    downside_stress_doc_path: Path = DEFAULT_DOWNSIDE_STRESS_DOC_PATH,
    static_frontier_doc_path: Path = DEFAULT_STATIC_FRONTIER_INTERPRETATION_DOC_PATH,
    tqqq_safety_doc_path: Path = DEFAULT_TQQQ_OVERLAY_SAFETY_DOC_PATH,
    candidate_set_yaml_path: Path = DEFAULT_OVERLAY_CANDIDATE_SET_PATH,
    survival_yaml_path: Path = DEFAULT_OVERLAY_SURVIVAL_MATRIX_PATH,
    survival_doc_path: Path = DEFAULT_OVERLAY_SURVIVAL_REVIEW_DOC_PATH,
    net_cost_doc_path: Path = DEFAULT_NET_COST_REVIEW_DOC_PATH,
    stress_gate_yaml_path: Path = DEFAULT_STRESS_GATE_PATH,
    walk_forward_doc_path: Path = DEFAULT_WALK_FORWARD_SPLIT_DOC_PATH,
    walk_forward_yaml_path: Path = DEFAULT_WALK_FORWARD_SPLIT_YAML_PATH,
    owner_doc_path: Path = DEFAULT_OWNER_REVIEW_PACK_DOC_PATH,
    forward_watch_doc_path: Path = DEFAULT_FORWARD_WATCH_PLAN_DOC_PATH,
) -> dict[str, Any]:
    config = load_defensive_overlay_config(config_path)
    expanded_config = _load_yaml_mapping(expanded_config_path)
    missing = [str(path) for path in (failure_matrix_path, config_path) if not path.exists()]
    if missing:
        payload = _payload(
            report_type="defensive_overlay_research_pack",
            title="Defensive Overlay Research Pack",
            status="DEFENSIVE_OVERLAY_PACK_BLOCKED",
            summary={"missing_input_count": len(missing)},
            blockers=missing,
        )
        _write_markdown(owner_doc_path, _render_generic_doc(payload))
        return payload

    failure_rows = _read_failure_matrix(failure_matrix_path)
    data_gate = _data_quality_gate(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        config=expanded_config,
        as_of_date=as_of_date,
        expected_tickers=_string_list(
            _mapping(config.get("data_quality")).get("expected_price_tickers")
        )
        or ["QQQ", "SGOV", "TQQQ"],
    )
    if not data_gate["passed"]:
        payload = _payload(
            report_type="defensive_overlay_research_pack",
            title="Defensive Overlay Research Pack",
            status="DEFENSIVE_OVERLAY_PACK_BLOCKED_DATA_QUALITY",
            summary={
                "data_quality_status": data_gate["status"],
                "candidate_count": len(failure_rows),
            },
            data_quality=data_gate,
            blockers=["cached data quality gate failed"],
        )
        _write_markdown(owner_doc_path, _render_generic_doc(payload))
        return payload

    overlay_rows = build_overlay_metrics_rows(
        failure_rows=failure_rows,
        config=config,
        prices_path=prices_path,
        actual_path_root=actual_path_root,
    )
    output_root.mkdir(parents=True, exist_ok=True)
    _write_overlay_metrics_csv(overlay_metrics_csv_path, overlay_rows)

    no_survivor = _no_survivor_payload(
        rows=failure_rows,
        config_path=config_path,
        failure_matrix_path=failure_matrix_path,
        data_gate=data_gate,
    )
    reclassification = _reclassification_payload(
        rows=failure_rows,
        overlay_rows=overlay_rows,
        config_path=config_path,
        failure_matrix_path=failure_matrix_path,
    )
    risk_off = _section_payload(
        report_type="defensive_overlay_risk_off_attribution",
        title="Defensive Overlay Risk-Off Attribution",
        status="DEFENSIVE_OVERLAY_RISK_OFF_ATTRIBUTION_READY_PROMOTION_BLOCKED",
        rows=overlay_rows,
        summary=_risk_off_summary(overlay_rows),
        config_path=config_path,
    )
    re_risk = _section_payload(
        report_type="defensive_overlay_re_risk_timing_review",
        title="Defensive Overlay Re-Risk Timing Review",
        status="DEFENSIVE_OVERLAY_RE_RISK_TIMING_READY_PROMOTION_BLOCKED",
        rows=overlay_rows,
        summary=_re_risk_summary(overlay_rows),
        config_path=config_path,
    )
    downside = _section_payload(
        report_type="defensive_overlay_downside_stress_review",
        title="Defensive Overlay Downside and Stress Review",
        status="DEFENSIVE_OVERLAY_DOWNSIDE_STRESS_READY_PROMOTION_BLOCKED",
        rows=overlay_rows,
        summary=_downside_summary(overlay_rows),
        config_path=config_path,
    )
    static_interpretation = _section_payload(
        report_type="static_frontier_domination_overlay_interpretation",
        title="Static Frontier Domination Overlay Interpretation",
        status="STATIC_FRONTIER_OVERLAY_INTERPRETATION_READY_PROMOTION_BLOCKED",
        rows=overlay_rows,
        summary=_static_frontier_summary(failure_rows, overlay_rows),
        config_path=config_path,
    )
    tqqq_safety = _section_payload(
        report_type="tqqq_overlay_safety_review",
        title="TQQQ Overlay Safety Review",
        status="TQQQ_OVERLAY_SAFETY_REVIEW_READY_RESEARCH_ONLY",
        rows=overlay_rows,
        summary=_tqqq_safety_summary(overlay_rows),
        config_path=config_path,
    )
    candidate_set = _candidate_set_payload(overlay_rows=overlay_rows, config_path=config_path)
    survival = _section_payload(
        report_type="defensive_overlay_survival_matrix",
        title="Defensive Overlay Survival Matrix",
        status="DEFENSIVE_OVERLAY_SURVIVAL_READY_PROMOTION_BLOCKED",
        rows=overlay_rows,
        summary=_survival_summary(overlay_rows),
        config_path=config_path,
    )
    net_cost = _section_payload(
        report_type="defensive_overlay_net_of_cost_review",
        title="Defensive Overlay Net-of-Cost Review",
        status="DEFENSIVE_OVERLAY_NET_OF_COST_READY_PROMOTION_BLOCKED",
        rows=overlay_rows,
        summary=_net_cost_summary(overlay_rows),
        config_path=config_path,
    )
    stress_gate = _section_payload(
        report_type="defensive_overlay_stress_gate",
        title="Defensive Overlay Stress Gate",
        status="DEFENSIVE_OVERLAY_STRESS_GATE_READY_PROMOTION_BLOCKED",
        rows=overlay_rows,
        summary=_stress_gate_summary(overlay_rows),
        config_path=config_path,
    )
    walk_forward = _section_payload(
        report_type="expanded_universe_walk_forward_split_evidence",
        title="Expanded Universe Walk-Forward Split Evidence",
        status="WALK_FORWARD_SPLIT_EVIDENCE_PENDING_PROMOTION_BLOCKED",
        rows=overlay_rows,
        summary=_walk_forward_summary(overlay_rows),
        config_path=config_path,
    )
    owner_pack = _owner_pack_payload(
        no_survivor=no_survivor,
        candidate_set=candidate_set,
        survival=survival,
        overlay_rows=overlay_rows,
        config_path=config_path,
        artifact_paths={
            "overlay_metrics_csv": str(overlay_metrics_csv_path),
            "no_survivor_doc": str(no_survivor_doc_path),
            "no_survivor_yaml": str(no_survivor_yaml_path),
            "reclassification_yaml": str(reclassification_yaml_path),
            "risk_off_doc": str(risk_off_doc_path),
            "risk_off_yaml": str(risk_off_yaml_path),
            "re_risk_doc": str(re_risk_doc_path),
            "downside_stress_doc": str(downside_stress_doc_path),
            "static_frontier_doc": str(static_frontier_doc_path),
            "tqqq_safety_doc": str(tqqq_safety_doc_path),
            "candidate_set_yaml": str(candidate_set_yaml_path),
            "survival_yaml": str(survival_yaml_path),
            "survival_doc": str(survival_doc_path),
            "net_cost_doc": str(net_cost_doc_path),
            "stress_gate_yaml": str(stress_gate_yaml_path),
            "walk_forward_doc": str(walk_forward_doc_path),
            "walk_forward_yaml": str(walk_forward_yaml_path),
            "owner_doc": str(owner_doc_path),
            "forward_watch_doc": str(forward_watch_doc_path),
        },
    )
    forward_watch = _forward_watch_payload(
        overlay_rows=overlay_rows,
        candidate_set=candidate_set,
        config_path=config_path,
    )

    _write_yaml(no_survivor_yaml_path, no_survivor)
    _write_markdown(no_survivor_doc_path, _render_no_survivor_doc(no_survivor))
    _write_yaml(reclassification_yaml_path, reclassification)
    _write_yaml(risk_off_yaml_path, risk_off)
    _write_markdown(risk_off_doc_path, _render_section_doc(risk_off))
    _write_markdown(re_risk_doc_path, _render_section_doc(re_risk))
    _write_markdown(downside_stress_doc_path, _render_section_doc(downside))
    _write_markdown(static_frontier_doc_path, _render_section_doc(static_interpretation))
    _write_markdown(tqqq_safety_doc_path, _render_section_doc(tqqq_safety))
    _write_yaml(candidate_set_yaml_path, candidate_set)
    _write_yaml(survival_yaml_path, survival)
    _write_markdown(survival_doc_path, _render_section_doc(survival))
    _write_markdown(net_cost_doc_path, _render_section_doc(net_cost))
    _write_yaml(stress_gate_yaml_path, stress_gate)
    _write_yaml(walk_forward_yaml_path, walk_forward)
    _write_markdown(walk_forward_doc_path, _render_section_doc(walk_forward))
    _write_markdown(owner_doc_path, _render_owner_pack_doc(owner_pack))
    _write_markdown(forward_watch_doc_path, _render_forward_watch_doc(forward_watch))
    return owner_pack


def load_defensive_overlay_config(
    path: Path = DEFAULT_DEFENSIVE_OVERLAY_CONFIG_PATH,
) -> dict[str, Any]:
    return _load_yaml_mapping(path)


def build_overlay_metrics_rows(
    *,
    failure_rows: Sequence[Mapping[str, Any]],
    config: Mapping[str, Any],
    prices_path: Path,
    actual_path_root: Path,
) -> list[dict[str, Any]]:
    prices = _slice_prices(
        _load_price_matrix(prices_path, ["QQQ", "SGOV", "TQQQ"]),
        start_date=DEFAULT_AI_REGIME_BACKTEST_START,
        end_date=None,
    )
    rows: list[dict[str, Any]] = []
    for failure_row in failure_rows:
        candidate_id = str(failure_row.get("candidate_id") or failure_row.get("strategy_id"))
        position_path = actual_path_root / f"{candidate_id}_actual_daily_positions.csv"
        candidate_path = _load_position_path(position_path)
        baseline_weights = _parse_simplex_weights(str(failure_row.get("same_risk_baseline")))
        baseline_path = _simulate_static_baseline_path(prices, baseline_weights)
        metrics = _overlay_metric_row(
            candidate_id=candidate_id,
            failure_row=failure_row,
            candidate_path=candidate_path,
            baseline_path=baseline_path,
            position_path=position_path,
            config=config,
        )
        evaluation = evaluate_defensive_overlay_candidate(
            failure_row,
            metrics,
            config,
            evidence_source="actual_path",
        )
        rows.append({**metrics, **evaluation})
    return rows


def evaluate_defensive_overlay_candidate(
    candidate_row: Mapping[str, Any],
    overlay_metrics: Mapping[str, Any],
    config: Mapping[str, Any] | None = None,
    *,
    evidence_source: str = "actual_path",
) -> dict[str, Any]:
    policy = _mapping((config or {}).get("overlay_gate"))
    stress_policy = _mapping((config or {}).get("stress_gate"))
    classification_policy = _mapping((config or {}).get("classification_policy"))
    next_actions = _mapping(classification_policy.get("next_action_by_classification"))
    required_source = str(policy.get("evidence_source_required", "actual_path"))
    allow_target_path = _bool(policy.get("allow_target_path_metrics"), default=False)
    candidate_id = str(candidate_row.get("candidate_id") or candidate_row.get("strategy_id"))
    candidate_family = str(candidate_row.get("candidate_family", ""))
    blockers: list[str] = []
    if evidence_source != required_source:
        blockers.append("actual_path_evidence_required")
    if evidence_source == "target_path" and not allow_target_path:
        blockers.append("target_path_metrics_not_allowed_for_overlay_gate")
    if not _bool(overlay_metrics.get("actual_position_path_available"), default=False):
        blockers.append("actual_position_path_missing")

    drawdown_improvement = _float(overlay_metrics.get("drawdown_improvement"))
    annual_return_edge = _float(overlay_metrics.get("annual_return_edge"))
    net_return_edge = _float(overlay_metrics.get("net_annual_return_edge"))
    calmar_edge = _float(overlay_metrics.get("calmar_edge"))
    sharpe_edge = _float(overlay_metrics.get("sharpe_edge"))
    max_tqqq = _float(overlay_metrics.get("tqqq_max_weight"))
    qqq_exposure = _float(overlay_metrics.get("qqq_equivalent_exposure"))
    stress_blocked = _bool(candidate_row.get("stress_risk_too_high"))
    walk_forward_failed = _bool(candidate_row.get("walk_forward_failed"))

    drawdown_ok = drawdown_improvement >= _float(policy.get("drawdown_improvement_min_abs"))
    return_ok = annual_return_edge >= _float(policy.get("annual_return_edge_tolerance"))
    net_cost_ok = net_return_edge >= _float(policy.get("net_annual_return_edge_tolerance"))
    calmar_ok = calmar_edge >= _float(policy.get("calmar_edge_min"))
    sharpe_ok = sharpe_edge >= _float(policy.get("sharpe_edge_min"))
    tqqq_watch_ok = max_tqqq <= _float(stress_policy.get("max_tqqq_weight_for_watch"), default=0.15)
    primary_tqqq_ok = max_tqqq <= _float(
        stress_policy.get("max_tqqq_weight_for_primary_watch"), default=0.0
    )
    exposure_watch_ok = qqq_exposure <= _float(
        stress_policy.get("max_qqq_equivalent_exposure_for_watch"), default=1.2
    )
    primary_exposure_ok = qqq_exposure <= _float(
        stress_policy.get("max_qqq_equivalent_exposure_for_primary_watch"), default=0.8
    )
    if stress_blocked:
        blockers.append("stress_risk_too_high")
    if not tqqq_watch_ok or not exposure_watch_ok:
        blockers.append("overlay_stress_watch_cap_exceeded")

    static_family = str(
        classification_policy.get("static_reference_family", "static_frontier_candidate")
    )
    primary_ids = set(_string_list(classification_policy.get("primary_watch_candidate_ids")))
    tqqq_diagnostic_ids = set(
        _string_list(classification_policy.get("tqqq_diagnostic_candidate_ids"))
    )
    defensive_benefit = drawdown_ok and (calmar_ok or sharpe_ok)
    overlay_economics_ok = defensive_benefit and return_ok and net_cost_ok
    overlay_primary_ok = overlay_economics_ok and primary_tqqq_ok and primary_exposure_ok

    if "target_path_metrics_not_allowed_for_overlay_gate" in blockers:
        classification = "TARGET_PATH_REJECTED"
        allowed_use = "diagnostic_rejected"
    elif candidate_family == static_family or candidate_id.startswith("static_"):
        classification = "STATIC_REFERENCE_ONLY"
        allowed_use = "static_reference_only"
    elif (
        candidate_id in tqqq_diagnostic_ids
        or stress_blocked
        or not tqqq_watch_ok
        or not exposure_watch_ok
    ):
        classification = "TQQQ_OVERLAY_DIAGNOSTIC_RESEARCH_ONLY"
        allowed_use = "tqqq_research_diagnostic_only"
    elif candidate_id in primary_ids and overlay_primary_ok and not stress_blocked:
        classification = "DEFENSIVE_OVERLAY_WATCH_PENDING_SPLIT_EVIDENCE"
        allowed_use = "research_overlay_watch_only"
    elif overlay_economics_ok and not stress_blocked and tqqq_watch_ok and exposure_watch_ok:
        classification = "DEFENSIVE_OVERLAY_WATCH_PENDING_SPLIT_EVIDENCE"
        allowed_use = "research_overlay_watch_only"
    elif defensive_benefit:
        classification = "DRAWDOWN_CONTROL_DIAGNOSTIC_WITH_COST_OR_RETURN_BLOCKER"
        allowed_use = "risk_control_diagnostic_only"
    else:
        classification = "ARCHIVE_REFERENCE_ONLY"
        allowed_use = "archive_reference_only"

    if walk_forward_failed and classification.startswith("DEFENSIVE_OVERLAY_WATCH"):
        blockers.append("walk_forward_split_evidence_pending")
    overlay_gate_passed = (
        classification == "DEFENSIVE_OVERLAY_WATCH_CANDIDATE"
        and not blockers
        and not walk_forward_failed
    )
    watchlist_allowed = classification == "DEFENSIVE_OVERLAY_WATCH_PENDING_SPLIT_EVIDENCE"
    next_action = str(next_actions.get(classification, "KEEP_RESEARCH_ONLY_NO_ACTION"))
    return {
        "overlay_classification": classification,
        "overlay_gate_passed": overlay_gate_passed,
        "watchlist_allowed": watchlist_allowed,
        "allowed_use": allowed_use,
        "overlay_blockers": blockers,
        "overlay_failure_reason": " | ".join(blockers) if blockers else "none",
        "overlay_next_action": next_action,
        "broker_universe_eligible": False,
        **SAFETY_BOUNDARY,
    }


def defensive_overlay_candidate_can_enter_broker_universe(
    evaluation: Mapping[str, Any],
) -> bool:
    return (
        _bool(evaluation.get("promotion_allowed"))
        and _bool(evaluation.get("paper_shadow_allowed"))
        and _bool(evaluation.get("production_allowed"))
        and str(evaluation.get("broker_action")) != "none"
    )


def _overlay_metric_row(
    *,
    candidate_id: str,
    failure_row: Mapping[str, Any],
    candidate_path: dict[str, Any],
    baseline_path: dict[str, Any],
    position_path: Path,
    config: Mapping[str, Any],
) -> dict[str, Any]:
    candidate_returns = _series(candidate_path.get("returns"))
    baseline_returns = (
        _series(baseline_path.get("returns")).reindex(candidate_returns.index).fillna(0.0)
    )
    candidate_equity = _series(candidate_path.get("equity"))
    baseline_equity = _series(baseline_path.get("equity")).reindex(candidate_returns.index).ffill()
    candidate_exposure = _series(candidate_path.get("qqq_equivalent_exposure"))
    baseline_exposure = (
        _series(baseline_path.get("qqq_equivalent_exposure"))
        .reindex(candidate_returns.index)
        .ffill()
    )
    reduction_min = _float(
        _mapping(config.get("overlay_gate")).get("risk_off_exposure_reduction_min")
    )
    risk_off_mask = candidate_exposure <= (baseline_exposure - reduction_min)
    baseline_positive = baseline_returns > 0.0
    downside_mask = baseline_returns < 0.0
    risk_off_candidate_return = _period_return(candidate_returns.loc[risk_off_mask])
    risk_off_baseline_return = _period_return(baseline_returns.loc[risk_off_mask])
    delta = candidate_returns - baseline_returns
    parsed_delta = _mapping(_parse_jsonish(failure_row.get("delta_vs_same_risk_baseline")))
    tqqq_profile = _mapping(_parse_jsonish(failure_row.get("tqqq_weight_profile")))
    baseline_metrics = _mapping(_parse_jsonish(failure_row.get("same_risk_baseline_metrics")))
    drawdown_improvement = abs(_float(baseline_metrics.get("max_drawdown"))) - abs(
        _float(failure_row.get("max_dd"))
    )
    return {
        "candidate_id": candidate_id,
        "candidate_family": str(failure_row.get("candidate_family", "")),
        "full_allocation_verdict": str(failure_row.get("verdict", "")),
        "same_risk_baseline": str(failure_row.get("same_risk_baseline", "")),
        "actual_position_path": str(position_path),
        "actual_position_path_available": bool(candidate_path.get("available")),
        "metric_source": "actual_path",
        "qqq_equivalent_exposure": _float(failure_row.get("qqq_equivalent_exposure")),
        "tqqq_max_weight": _float(tqqq_profile.get("tqqq_max_weight")),
        "tqqq_avg_weight": _float(tqqq_profile.get("tqqq_avg_weight")),
        "annual_return_edge": _float(parsed_delta.get("annual_return_edge")),
        "net_annual_return_edge": _float(parsed_delta.get("net_annual_return_edge")),
        "drawdown_delta": _float(parsed_delta.get("drawdown_delta")),
        "drawdown_improvement": round(drawdown_improvement, GRID_ROUND_DIGITS),
        "sharpe_edge": _float(parsed_delta.get("sharpe_edge")),
        "calmar_edge": _float(parsed_delta.get("calmar_edge")),
        "risk_off_day_count": int(risk_off_mask.sum()),
        "risk_off_day_share": round(
            _ratio(risk_off_mask.sum(), len(risk_off_mask)), GRID_ROUND_DIGITS
        ),
        "risk_off_candidate_return": round(risk_off_candidate_return, GRID_ROUND_DIGITS),
        "risk_off_baseline_return": round(risk_off_baseline_return, GRID_ROUND_DIGITS),
        "risk_off_return_delta": round(
            risk_off_candidate_return - risk_off_baseline_return, GRID_ROUND_DIGITS
        ),
        "downside_capture": round(
            _ratio(
                candidate_returns.loc[downside_mask].sum(),
                baseline_returns.loc[downside_mask].sum(),
            ),
            GRID_ROUND_DIGITS,
        ),
        "upside_capture": round(
            _ratio(
                candidate_returns.loc[baseline_positive].sum(),
                baseline_returns.loc[baseline_positive].sum(),
            ),
            GRID_ROUND_DIGITS,
        ),
        "missed_upside_during_risk_off": round(
            abs(float(delta.loc[risk_off_mask & baseline_positive].clip(upper=0.0).sum())),
            GRID_ROUND_DIGITS,
        ),
        "re_risk_delay_days": int((risk_off_mask & baseline_positive).sum()),
        "worst_1d_loss_reduction": round(
            abs(float(baseline_returns.min())) - abs(float(candidate_returns.min())),
            GRID_ROUND_DIGITS,
        ),
        "worst_5d_loss_reduction": round(
            abs(_worst_window_return(baseline_returns, 5))
            - abs(_worst_window_return(candidate_returns, 5)),
            GRID_ROUND_DIGITS,
        ),
        "worst_20d_loss_reduction": round(
            abs(_worst_window_return(baseline_returns, 20))
            - abs(_worst_window_return(candidate_returns, 20)),
            GRID_ROUND_DIGITS,
        ),
        "candidate_total_return": round(_period_return(candidate_returns), GRID_ROUND_DIGITS),
        "baseline_total_return": round(_period_return(baseline_returns), GRID_ROUND_DIGITS),
        "candidate_max_drawdown_from_path": round(
            _max_drawdown(candidate_equity), GRID_ROUND_DIGITS
        ),
        "baseline_max_drawdown_from_path": round(_max_drawdown(baseline_equity), GRID_ROUND_DIGITS),
        "walk_forward_status": str(failure_row.get("walk_forward_status", "UNKNOWN")),
        "stress_status": str(failure_row.get("stress_status", "UNKNOWN")),
        "net_of_cost_failed_full_allocation": _bool(failure_row.get("net_of_cost_failed")),
        "same_risk_not_advantaged_full_allocation": _bool(
            failure_row.get("same_risk_not_advantaged")
        ),
    }


def _load_position_path(path: Path) -> dict[str, Any]:
    if not path.exists():
        empty_index = pd.DatetimeIndex([])
        return {
            "available": False,
            "returns": pd.Series(dtype=float, index=empty_index),
            "equity": pd.Series(dtype=float, index=empty_index),
            "qqq_equivalent_exposure": pd.Series(dtype=float, index=empty_index),
        }
    frame = pd.read_csv(path, parse_dates=["date"])
    frame = frame.sort_values("date").set_index("date")
    equity = pd.to_numeric(frame["equity"], errors="coerce").ffill()
    returns = equity.pct_change().fillna(0.0)
    qqq = pd.to_numeric(frame.get("actual_weight_QQQ", 0.0), errors="coerce").fillna(0.0)
    tqqq = pd.to_numeric(frame.get("actual_weight_TQQQ", 0.0), errors="coerce").fillna(0.0)
    return {
        "available": True,
        "returns": returns,
        "equity": equity,
        "qqq_equivalent_exposure": qqq + 3.0 * tqqq,
    }


def _simulate_static_baseline_path(
    prices: pd.DataFrame,
    weights: Mapping[str, float],
) -> dict[str, Any]:
    target = _constant_target_frame(prices.index, prices.columns, weights)
    sim = _simulate_rebalanced_portfolio(
        prices,
        target,
        rebalance="monthly",
        transaction_cost_bps=0.0,
    )
    applied = pd.DataFrame(sim["applied_weights"]).reindex(columns=prices.columns).fillna(0.0)
    qqq = pd.to_numeric(applied.get("QQQ", 0.0), errors="coerce").fillna(0.0)
    tqqq = pd.to_numeric(applied.get("TQQQ", 0.0), errors="coerce").fillna(0.0)
    return {
        "available": True,
        "returns": pd.Series(sim["daily_returns"]).fillna(0.0),
        "equity": pd.Series(sim["equity"]).fillna(1.0),
        "qqq_equivalent_exposure": qqq + 3.0 * tqqq,
    }


def _parse_simplex_weights(strategy_id: str) -> dict[str, float]:
    matches = re.findall(r"(qqq|sgov|tqqq)(\d{4})", strategy_id.lower())
    weights = {"QQQ": 0.0, "SGOV": 0.0, "TQQQ": 0.0}
    for asset, value in matches:
        weights[asset.upper()] = round(float(value) / 1000.0, GRID_ROUND_DIGITS)
    return weights


def _read_failure_matrix(path: Path) -> list[dict[str, Any]]:
    frame = pd.read_csv(path)
    rows: list[dict[str, Any]] = []
    for record in frame.to_dict("records"):
        rows.append({str(key): _jsonish_value(value) for key, value in record.items()})
    return rows


def _no_survivor_payload(
    *,
    rows: Sequence[Mapping[str, Any]],
    config_path: Path,
    failure_matrix_path: Path,
    data_gate: Mapping[str, Any],
) -> dict[str, Any]:
    counts = _value_counts(rows, "verdict")
    full_survivors = [
        row for row in rows if str(row.get("verdict")) == "SURVIVES_EXPANDED_UNIVERSE"
    ]
    return _payload(
        report_type="expanded_universe_no_survivor_diagnosis",
        title="Expanded Universe No-Survivor Diagnosis",
        status="EXPANDED_UNIVERSE_NO_SURVIVOR_DIAGNOSIS_READY",
        summary={
            "candidate_count": len(rows),
            "full_allocation_survivor_count": len(full_survivors),
            "static_frontier_dominates_count": int(counts.get("STATIC_FRONTIER_DOMINATES", 0)),
            "no_material_improvement_count": int(counts.get("NO_MATERIAL_IMPROVEMENT", 0)),
            "walk_forward_failed_count": sum(_bool(row.get("walk_forward_failed")) for row in rows),
            "stress_risk_too_high_count": sum(
                _bool(row.get("stress_risk_too_high")) for row in rows
            ),
            "net_of_cost_failed_count": sum(_bool(row.get("net_of_cost_failed")) for row in rows),
            "data_quality_status": data_gate.get("status"),
        },
        config_hash=_file_sha256(config_path),
        failure_matrix_path=str(failure_matrix_path),
        data_quality=data_gate,
        rows=[_compact_failure_row(row) for row in rows],
    )


def _reclassification_payload(
    *,
    rows: Sequence[Mapping[str, Any]],
    overlay_rows: Sequence[Mapping[str, Any]],
    config_path: Path,
    failure_matrix_path: Path,
) -> dict[str, Any]:
    overlay_by_id = {str(row.get("candidate_id")): row for row in overlay_rows}
    matrix = []
    for row in rows:
        candidate_id = str(row.get("candidate_id") or row.get("strategy_id"))
        overlay = overlay_by_id.get(candidate_id, {})
        matrix.append(
            {
                "candidate_id": candidate_id,
                "full_allocation_verdict": str(row.get("verdict")),
                "same_risk_baseline": str(row.get("same_risk_baseline")),
                "overlay_classification": str(overlay.get("overlay_classification", "")),
                "overlay_gate_passed": _bool(overlay.get("overlay_gate_passed")),
                "allowed_use": str(overlay.get("allowed_use", "")),
                "overlay_blockers": _list(overlay.get("overlay_blockers")),
                "next_action": str(overlay.get("overlay_next_action", "")),
                **SAFETY_BOUNDARY,
            }
        )
    return _payload(
        report_type="expanded_candidate_reclassification_matrix",
        title="Expanded Candidate Reclassification Matrix",
        status="EXPANDED_CANDIDATE_RECLASSIFICATION_READY_PROMOTION_BLOCKED",
        summary={
            "candidate_count": len(matrix),
            "watch_pending_count": sum(
                str(row.get("overlay_classification")).startswith("DEFENSIVE_OVERLAY_WATCH")
                for row in matrix
            ),
            "overlay_gate_pass_count": sum(_bool(row.get("overlay_gate_passed")) for row in matrix),
        },
        config_hash=_file_sha256(config_path),
        failure_matrix_path=str(failure_matrix_path),
        rows=matrix,
    )


def _candidate_set_payload(
    *,
    overlay_rows: Sequence[Mapping[str, Any]],
    config_path: Path,
) -> dict[str, Any]:
    primary = _ids_by_class(overlay_rows, "DEFENSIVE_OVERLAY_WATCH_PENDING_SPLIT_EVIDENCE")
    drawdown = _ids_by_class(
        overlay_rows, "DRAWDOWN_CONTROL_DIAGNOSTIC_WITH_COST_OR_RETURN_BLOCKER"
    )
    tqqq = _ids_by_class(overlay_rows, "TQQQ_OVERLAY_DIAGNOSTIC_RESEARCH_ONLY")
    static = _ids_by_class(overlay_rows, "STATIC_REFERENCE_ONLY")
    archive = _ids_by_class(overlay_rows, "ARCHIVE_REFERENCE_ONLY")
    return _payload(
        report_type="defensive_overlay_candidate_set",
        title="Defensive Overlay Candidate Set",
        status="DEFENSIVE_OVERLAY_CANDIDATE_SET_READY_PROMOTION_BLOCKED",
        summary={
            "primary_watch_count": len(primary),
            "drawdown_control_diagnostic_count": len(drawdown),
            "tqqq_diagnostic_count": len(tqqq),
            "static_reference_count": len(static),
            "archive_count": len(archive),
        },
        config_hash=_file_sha256(config_path),
        primary_watch_candidates=primary,
        drawdown_control_diagnostics=drawdown,
        tqqq_diagnostic_candidates=tqqq,
        static_references=static,
        archive_references=archive,
    )


def _section_payload(
    *,
    report_type: str,
    title: str,
    status: str,
    rows: Sequence[Mapping[str, Any]],
    summary: Mapping[str, Any],
    config_path: Path,
) -> dict[str, Any]:
    return _payload(
        report_type=report_type,
        title=title,
        status=status,
        summary=dict(summary),
        config_hash=_file_sha256(config_path),
        rows=[dict(row) for row in rows],
    )


def _owner_pack_payload(
    *,
    no_survivor: Mapping[str, Any],
    candidate_set: Mapping[str, Any],
    survival: Mapping[str, Any],
    overlay_rows: Sequence[Mapping[str, Any]],
    config_path: Path,
    artifact_paths: Mapping[str, str],
) -> dict[str, Any]:
    return _payload(
        report_type="defensive_overlay_owner_review_pack",
        title="Defensive Overlay Owner Review Pack",
        status="DEFENSIVE_OVERLAY_OWNER_REVIEW_PACK_READY_PROMOTION_BLOCKED",
        summary={
            "full_allocation_survivor_count": _int(
                _mapping(no_survivor.get("summary")).get("full_allocation_survivor_count")
            ),
            "primary_watch_count": _int(
                _mapping(candidate_set.get("summary")).get("primary_watch_count")
            ),
            "overlay_gate_pass_count": _int(
                _mapping(survival.get("summary")).get("overlay_gate_pass_count")
            ),
            "walk_forward_pending_count": sum(
                "walk_forward" in str(row.get("overlay_failure_reason")) for row in overlay_rows
            ),
            "owner_recommendation": (
                "KEEP_DEFENSIVE_OVERLAY_RESEARCH_ONLY_WATCH_PENDING_SPLIT_EVIDENCE"
            ),
        },
        config_hash=_file_sha256(config_path),
        artifact_paths=dict(artifact_paths),
        candidate_set_summary=_mapping(candidate_set.get("summary")),
        survival_summary=_mapping(survival.get("summary")),
        rows=[dict(row) for row in overlay_rows],
    )


def _forward_watch_payload(
    *,
    overlay_rows: Sequence[Mapping[str, Any]],
    candidate_set: Mapping[str, Any],
    config_path: Path,
) -> dict[str, Any]:
    watch_ids = _string_list(candidate_set.get("primary_watch_candidates"))
    return _payload(
        report_type="defensive_overlay_forward_watch_plan",
        title="Defensive Overlay Forward Watch Plan",
        status="DEFENSIVE_OVERLAY_FORWARD_WATCH_PLAN_READY_PROMOTION_BLOCKED",
        summary={
            "watch_candidate_count": len(watch_ids),
            "minimum_next_evidence": "walk_forward_split_then_forward_watch_observations",
            "paper_shadow_allowed": False,
            "production_allowed": False,
        },
        config_hash=_file_sha256(config_path),
        watch_candidates=[
            {
                "candidate_id": row.get("candidate_id"),
                "classification": row.get("overlay_classification"),
                "next_action": row.get("overlay_next_action"),
            }
            for row in overlay_rows
            if str(row.get("candidate_id")) in watch_ids
        ],
        required_evidence=[
            "complete walk-forward split evidence before any overlay gate pass",
            "track missed-upside and re-risk delay in forward observations",
            "keep TQQQ overlay rows research-only until separate owner review",
        ],
    )


def _payload(
    *,
    report_type: str,
    title: str,
    status: str,
    summary: Mapping[str, Any],
    **extra: Any,
) -> dict[str, Any]:
    return {
        "schema_version": f"{report_type}.v1",
        "report_type": report_type,
        "title": title,
        "status": status,
        "generated_at": utc_now_iso(),
        "source_commit": _source_commit(),
        "market_regime": "ai_after_chatgpt",
        "anchor_event": "ChatGPT public launch",
        "anchor_date": "2022-11-30",
        "default_backtest_start": DEFAULT_AI_REGIME_BACKTEST_START.isoformat(),
        "summary": {
            "market_regime": "ai_after_chatgpt",
            "default_backtest_start": DEFAULT_AI_REGIME_BACKTEST_START.isoformat(),
            **dict(summary),
        },
        **SAFETY_BOUNDARY,
        **extra,
    }


def _risk_off_summary(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    return {
        "candidate_count": len(rows),
        "risk_off_day_count_max": max(
            (_int(row.get("risk_off_day_count")) for row in rows), default=0
        ),
        "best_risk_off_return_delta_candidate": _best_id(rows, "risk_off_return_delta"),
        "best_drawdown_improvement_candidate": _best_id(rows, "drawdown_improvement"),
    }


def _re_risk_summary(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    return {
        "candidate_count": len(rows),
        "max_re_risk_delay_days": max(
            (_int(row.get("re_risk_delay_days")) for row in rows), default=0
        ),
        "largest_missed_upside_candidate": _best_id(rows, "missed_upside_during_risk_off"),
    }


def _downside_summary(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    return {
        "candidate_count": len(rows),
        "best_worst_20d_loss_reduction_candidate": _best_id(rows, "worst_20d_loss_reduction"),
        "stress_blocked_count": sum(
            str(row.get("stress_status")) == "STRESS_RISK_BLOCKS_PROMOTION" for row in rows
        ),
    }


def _static_frontier_summary(
    failure_rows: Sequence[Mapping[str, Any]],
    overlay_rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    return {
        "static_frontier_dominates_count": sum(
            str(row.get("verdict")) == "STATIC_FRONTIER_DOMINATES" for row in failure_rows
        ),
        "watch_pending_despite_full_allocation_failure_count": sum(
            str(row.get("overlay_classification")).startswith("DEFENSIVE_OVERLAY_WATCH")
            for row in overlay_rows
        ),
        "interpretation": "full_allocation_static_dominance_does_not_equal_overlay_pass",
    }


def _tqqq_safety_summary(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    tqqq_rows = [row for row in rows if _float(row.get("tqqq_max_weight")) > 0.0]
    return {
        "tqqq_positive_candidate_count": len(tqqq_rows),
        "max_tqqq_weight": max((_float(row.get("tqqq_max_weight")) for row in rows), default=0.0),
        "tqqq_research_only_count": sum(
            str(row.get("overlay_classification")) == "TQQQ_OVERLAY_DIAGNOSTIC_RESEARCH_ONLY"
            for row in rows
        ),
    }


def _survival_summary(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    return {
        "candidate_count": len(rows),
        "overlay_gate_pass_count": sum(_bool(row.get("overlay_gate_passed")) for row in rows),
        "watch_pending_count": sum(
            str(row.get("overlay_classification")).startswith("DEFENSIVE_OVERLAY_WATCH")
            for row in rows
        ),
        "promotion_status": "BLOCKED",
    }


def _net_cost_summary(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    return {
        "candidate_count": len(rows),
        "net_cost_within_overlay_tolerance_count": sum(
            _float(row.get("net_annual_return_edge")) >= -0.01 for row in rows
        ),
        "worst_net_annual_return_edge": min(
            (_float(row.get("net_annual_return_edge")) for row in rows), default=0.0
        ),
    }


def _stress_gate_summary(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    return {
        "candidate_count": len(rows),
        "stress_risk_blocked_count": sum(
            str(row.get("stress_status")) == "STRESS_RISK_BLOCKS_PROMOTION" for row in rows
        ),
        "max_qqq_equivalent_exposure": max(
            (_float(row.get("qqq_equivalent_exposure")) for row in rows), default=0.0
        ),
        "max_tqqq_weight": max((_float(row.get("tqqq_max_weight")) for row in rows), default=0.0),
    }


def _walk_forward_summary(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    return {
        "candidate_count": len(rows),
        "walk_forward_pending_count": sum(
            str(row.get("walk_forward_status")) == "WALK_FORWARD_BLOCKED_PENDING_SPLIT_EVIDENCE"
            for row in rows
        ),
        "overlay_gate_pass_count_after_split_evidence": 0,
    }


def _render_no_survivor_doc(payload: Mapping[str, Any]) -> str:
    summary = _mapping(payload.get("summary"))
    lines = [
        "# Expanded Universe No-Survivor Diagnosis",
        "",
        f"- Status: `{payload.get('status')}`",
        f"- Market regime: `{payload.get('market_regime')}`",
        f"- Candidate count: `{summary.get('candidate_count')}`",
        f"- Full allocation survivor count: `{summary.get('full_allocation_survivor_count')}`",
        f"- Static frontier dominates: `{summary.get('static_frontier_dominates_count')}`",
        f"- No material improvement: `{summary.get('no_material_improvement_count')}`",
        f"- Walk-forward failed or pending: `{summary.get('walk_forward_failed_count')}`",
        "",
        (
            "结论：full allocation gate 继续 `BLOCKED`。本报告只把失败原因转成 "
            "defensive overlay 诊断输入，不恢复 promotion、paper-shadow、production 或 broker。"
        ),
        "",
        "## Candidate Rows",
        "",
        "|candidate_id|verdict|same_risk_baseline|walk_forward_failed|stress_risk_too_high|net_of_cost_failed|",
        "|---|---|---|---|---|---|",
    ]
    for row in _records(payload.get("rows")):
        lines.append(
            "|{candidate_id}|{verdict}|{same_risk_baseline}|{walk_forward_failed}|{stress_risk_too_high}|{net_of_cost_failed}|".format(
                **row
            )
        )
    return "\n".join(lines) + "\n"


def _render_section_doc(payload: Mapping[str, Any]) -> str:
    summary = _mapping(payload.get("summary"))
    rows = _records(payload.get("rows"))
    lines = [
        f"# {payload.get('title')}",
        "",
        f"- Status: `{payload.get('status')}`",
        f"- Market regime: `{payload.get('market_regime')}`",
        f"- Dynamic promotion: `{payload.get('dynamic_promotion_status')}`",
        f"- Paper-shadow allowed: `{payload.get('paper_shadow_allowed')}`",
        f"- Production allowed: `{payload.get('production_allowed')}`",
        f"- Broker action: `{payload.get('broker_action')}`",
        "",
        "## Summary",
        "",
    ]
    for key, value in summary.items():
        lines.append(f"- {key}: `{value}`")
    lines.extend(
        [
            "",
            "## Candidate Matrix",
            "",
            "|candidate_id|classification|gate_passed|drawdown_improvement|annual_return_edge|net_edge|next_action|",
            "|---|---|---|---|---|---|---|",
        ]
    )
    for row in rows:
        lines.append(
            "|{candidate_id}|{overlay_classification}|{overlay_gate_passed}|{drawdown_improvement}|{annual_return_edge}|{net_annual_return_edge}|{overlay_next_action}|".format(
                **row
            )
        )
    lines.append("")
    lines.append(
        "解释：该 review 只用于 defensive overlay research-only 诊断，不构成 allocation promotion。"
    )
    return "\n".join(lines) + "\n"


def _render_owner_pack_doc(payload: Mapping[str, Any]) -> str:
    summary = _mapping(payload.get("summary"))
    lines = [
        "# Defensive Overlay Owner Review Pack",
        "",
        f"- Status: `{payload.get('status')}`",
        f"- Owner recommendation: `{summary.get('owner_recommendation')}`",
        f"- Full allocation survivor count: `{summary.get('full_allocation_survivor_count')}`",
        f"- Primary watch count: `{summary.get('primary_watch_count')}`",
        f"- Overlay gate pass count: `{summary.get('overlay_gate_pass_count')}`",
        f"- Dynamic promotion: `{payload.get('dynamic_promotion_status')}`",
        f"- Paper-shadow allowed: `{payload.get('paper_shadow_allowed')}`",
        f"- Production allowed: `{payload.get('production_allowed')}`",
        f"- Broker action: `{payload.get('broker_action')}`",
        "",
        "## Owner Interpretation",
        "",
        (
            "- Full allocation gate 继续 blocked，因为 static frontier、net-of-cost "
            "和 walk-forward blockers 仍未解除。"
        ),
        (
            "- Defensive overlay gate 目前最多只有 watch-only research 价值；"
            "当前 rows 因 split evidence pending 不能 pass。"
        ),
        (
            "- `limited_adjustment` 是 pilot overlay tolerance 下唯一 primary watch row，"
            "且只能 research-only / watch-only。"
        ),
        (
            "- 含 TQQQ 的 overlay rows 继续 diagnostic-only，直到单独的 TQQQ safety "
            "和 stress review 获得 owner 复核。"
        ),
        "",
        "## Candidate Matrix",
        "",
        "|candidate_id|classification|gate_passed|allowed_use|blockers|",
        "|---|---|---|---|---|",
    ]
    for row in _records(payload.get("rows")):
        blockers = "; ".join(_list(row.get("overlay_blockers")))
        lines.append(
            f"|{row.get('candidate_id')}|{row.get('overlay_classification')}|"
            f"{row.get('overlay_gate_passed')}|{row.get('allowed_use')}|{blockers}|"
        )
    return "\n".join(lines) + "\n"


def _render_forward_watch_doc(payload: Mapping[str, Any]) -> str:
    summary = _mapping(payload.get("summary"))
    lines = [
        "# Defensive Overlay Forward Watch Plan",
        "",
        f"- Status: `{payload.get('status')}`",
        f"- Watch candidate count: `{summary.get('watch_candidate_count')}`",
        f"- Minimum next evidence: `{summary.get('minimum_next_evidence')}`",
        "- Paper-shadow allowed: `False`",
        "- Production allowed: `False`",
        "- Broker action: `none`",
        "",
        "## Required Evidence",
        "",
    ]
    for item in _list(payload.get("required_evidence")):
        lines.append(f"- {item}")
    lines.extend(["", "## Watch Candidates", ""])
    for row in _records(payload.get("watch_candidates")):
        candidate_id = row.get("candidate_id")
        classification = row.get("classification")
        next_action = row.get("next_action")
        lines.append(
            f"- `{candidate_id}`: `{classification}` / `{next_action}`"
        )
    return "\n".join(lines) + "\n"


def _render_generic_doc(payload: Mapping[str, Any]) -> str:
    summary = _mapping(payload.get("summary"))
    lines = [f"# {payload.get('title')}", "", f"- Status: `{payload.get('status')}`"]
    for key, value in summary.items():
        lines.append(f"- {key}: `{value}`")
    return "\n".join(lines) + "\n"


def _write_overlay_metrics_csv(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame([_json_scalar(row) for row in rows]).to_csv(path, index=False)


def _write_yaml(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        yaml.safe_dump(_json_scalar(payload), allow_unicode=True, sort_keys=False),
        encoding="utf-8",
    )


def _write_markdown(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _load_yaml_mapping(path: Path) -> dict[str, Any]:
    raw = safe_load_yaml_path(path)
    if not isinstance(raw, Mapping):
        raise ValueError(f"YAML config must be a mapping: {path}")
    return dict(raw)


def _compact_failure_row(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "candidate_id": str(row.get("candidate_id") or row.get("strategy_id")),
        "verdict": str(row.get("verdict")),
        "same_risk_baseline": str(row.get("same_risk_baseline")),
        "same_risk_not_advantaged": _bool(row.get("same_risk_not_advantaged")),
        "walk_forward_failed": _bool(row.get("walk_forward_failed")),
        "stress_risk_too_high": _bool(row.get("stress_risk_too_high")),
        "net_of_cost_failed": _bool(row.get("net_of_cost_failed")),
        "next_action": str(row.get("next_action")),
    }


def _best_id(rows: Sequence[Mapping[str, Any]], key: str) -> str:
    if not rows:
        return ""
    return str(max(rows, key=lambda item: _float(item.get(key))).get("candidate_id", ""))


def _ids_by_class(rows: Sequence[Mapping[str, Any]], classification: str) -> list[str]:
    return [
        str(row.get("candidate_id"))
        for row in rows
        if str(row.get("overlay_classification")) == classification
    ]


def _value_counts(rows: Sequence[Mapping[str, Any]], key: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        value = str(row.get(key, ""))
        counts[value] = counts.get(value, 0) + 1
    return counts


def _period_return(returns: pd.Series) -> float:
    if returns.empty:
        return 0.0
    return float((1.0 + pd.to_numeric(returns, errors="coerce").fillna(0.0)).prod() - 1.0)


def _max_drawdown(equity: pd.Series) -> float:
    if equity.empty:
        return 0.0
    numeric = pd.to_numeric(equity, errors="coerce").ffill().fillna(1.0)
    drawdown = numeric / numeric.cummax() - 1.0
    return float(drawdown.min()) if not drawdown.empty else 0.0


def _worst_window_return(returns: pd.Series, window: int) -> float:
    numeric = pd.to_numeric(returns, errors="coerce").fillna(0.0)
    if len(numeric) < window:
        return float(numeric.min()) if len(numeric) else 0.0
    rolled = (1.0 + numeric).rolling(window).apply(lambda values: float(values.prod() - 1.0))
    return float(rolled.min()) if not rolled.dropna().empty else 0.0


def _ratio(numerator: object, denominator: object) -> float:
    denom = _float(denominator)
    if abs(denom) <= 1e-12:
        return 0.0
    return _float(numerator) / denom


def _series(value: object) -> pd.Series:
    if isinstance(value, pd.Series):
        return pd.to_numeric(value, errors="coerce").fillna(0.0)
    return pd.Series(dtype=float)


def _mapping(value: object) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _records(value: object) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [dict(item) for item in value if isinstance(item, Mapping)]


def _string_list(value: object) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(item) for item in value]


def _list(value: object) -> list[str]:
    if isinstance(value, list):
        return [str(item) for item in value]
    if value in (None, ""):
        return []
    return [str(value)]


def _jsonish_value(value: object) -> Any:
    parsed = _parse_jsonish(value)
    if isinstance(parsed, str) and parsed in {"True", "False"}:
        return parsed == "True"
    return parsed


def _parse_jsonish(value: object) -> Any:
    if isinstance(value, str):
        text = value.strip()
        if (text.startswith("{") and text.endswith("}")) or (
            text.startswith("[") and text.endswith("]")
        ):
            try:
                return json.loads(text)
            except json.JSONDecodeError:
                return value
    return value


def _bool(value: object, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        if value.lower() in {"true", "1", "yes"}:
            return True
        if value.lower() in {"false", "0", "no"}:
            return False
    if value is None:
        return default
    return bool(value)


def _float(value: object, default: float = 0.0) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return default
    if math.isnan(number) or math.isinf(number):
        return default
    return number


def _int(value: object, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _file_sha256(path: Path) -> str:
    if not path.exists():
        return ""
    return sha256(path.read_bytes()).hexdigest()


def _source_commit() -> str:
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=PROJECT_ROOT,
            text=True,
            stderr=subprocess.DEVNULL,
        ).strip()
    except (subprocess.CalledProcessError, FileNotFoundError):
        return "UNKNOWN"


def _json_scalar(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _json_scalar(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_scalar(item) for item in value]
    if isinstance(value, tuple):
        return [_json_scalar(item) for item in value]
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if hasattr(value, "item"):
        try:
            return value.item()
        except ValueError:
            return str(value)
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return 0.0
    return value
