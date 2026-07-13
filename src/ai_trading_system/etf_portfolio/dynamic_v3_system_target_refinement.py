from __future__ import annotations

import math
from collections.abc import Callable, Mapping, Sequence
from datetime import date, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from ai_trading_system.etf_portfolio import dynamic_v3_system_target as legacy
from ai_trading_system.etf_portfolio import dynamic_v3_system_target_hardening as hardening
from ai_trading_system.etf_portfolio import dynamic_v3_system_target_history as history
from ai_trading_system.etf_portfolio import dynamic_v3_system_target_portfolio as target_core
from ai_trading_system.etf_portfolio.dynamic_v3_confirmation_operations import (
    _file_bytes_match,
    _json_bytes,
    _jsonl_bytes,
    _validate_operations_source_bundle,
)
from ai_trading_system.etf_portfolio.dynamic_v3_historical_replay import (
    _artifact_dir_from_latest,
    _check,
    _mapping,
    _read_json,
    _read_jsonl,
    _records,
    _stable_id,
    _text,
    _unique_dir,
    _update_latest_pointer,
)
from ai_trading_system.etf_portfolio.dynamic_v3_pressure_validation import (
    _write_views_atomic,
)

LIMITED_INSTABILITY_SNAPSHOT_SCHEMA = "limited_instability_input_snapshot.v2"
LIMITED_RISK_ATTRIBUTION_SNAPSHOT_SCHEMA = "limited_risk_attribution_input_snapshot.v2"
DATA_WARNING_REPAIR_SNAPSHOT_SCHEMA = "data_warning_repair_plan_input_snapshot.v2"
ALTERNATIVE_METHOD_REVIEW_SNAPSHOT_SCHEMA = "alternative_method_review_input_snapshot.v2"
REFINED_METHOD_PROPOSAL_SNAPSHOT_SCHEMA = "refined_method_proposal_input_snapshot.v2"

DEFAULT_LIMITED_INSTABILITY_DIR = legacy.DEFAULT_LIMITED_INSTABILITY_DIR
DEFAULT_LIMITED_RISK_ATTRIBUTION_DIR = legacy.DEFAULT_LIMITED_RISK_ATTRIBUTION_DIR
DEFAULT_DATA_WARNING_REPAIR_PLAN_DIR = legacy.DEFAULT_DATA_WARNING_REPAIR_PLAN_DIR
DEFAULT_ALTERNATIVE_METHOD_REVIEW_DIR = legacy.DEFAULT_ALTERNATIVE_METHOD_REVIEW_DIR
DEFAULT_REFINED_METHOD_PROPOSAL_DIR = legacy.DEFAULT_REFINED_METHOD_PROPOSAL_DIR
DEFAULT_PAPER_SHADOW_BACKFILL_DIR = history.DEFAULT_PAPER_SHADOW_BACKFILL_DIR
DEFAULT_PAPER_SHADOW_ROLLING_EVAL_DIR = history.DEFAULT_PAPER_SHADOW_ROLLING_EVAL_DIR
DEFAULT_LIMITED_CONSISTENCY_DIR = hardening.DEFAULT_LIMITED_CONSISTENCY_DIR
DEFAULT_DATA_WARNING_IMPACT_DIR = hardening.DEFAULT_DATA_WARNING_IMPACT_DIR
SYSTEM_TARGET_SAFETY = history.SYSTEM_TARGET_SAFETY


class DynamicV3SystemTargetRefinementError(ValueError):
    """Raised when refinement evidence is not reproducible or lineage-safe."""


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise DynamicV3SystemTargetRefinementError(message)


def _generated_at(value: datetime | None) -> datetime:
    try:
        return target_core._generated_at(value)
    except Exception as exc:  # noqa: BLE001
        raise DynamicV3SystemTargetRefinementError(str(exc)) from exc


def _finite(value: Any) -> bool:
    return target_core._finite(value)


def _number(value: Any, *, digits: int = 10) -> float | None:
    return round(float(value), digits) if _finite(value) else None


def _date(value: Any, *, field: str) -> date:
    try:
        return date.fromisoformat(_text(value))
    except ValueError as exc:
        raise DynamicV3SystemTargetRefinementError(f"{field} must be ISO date") from exc


def _validation_payload(
    report_type: str,
    artifact_id: str,
    checks: Sequence[Mapping[str, Any]],
    *,
    artifact_id_key: str,
) -> dict[str, Any]:
    return target_core._validation_payload(
        report_type,
        artifact_id,
        checks,
        artifact_id_key=artifact_id_key,
    )


def _write(root: Path, views: Mapping[str, bytes], pointer: str, manifest_name: str) -> None:
    _write_views_atomic(root, views)
    _update_latest_pointer(pointer, root.name, root / manifest_name)


def _view_errors(root: Path, views: Mapping[str, bytes]) -> list[str]:
    return [name for name, payload in views.items() if not _file_bytes_match(root / name, payload)]


def _report_payload(root: Path, files: Mapping[str, str]) -> dict[str, Any]:
    return {
        key: _read_jsonl(root / name) if name.endswith(".jsonl") else _read_json(root / name)
        for key, name in files.items()
    }


def _bundle_json(binding: Mapping[str, Any], name: str) -> dict[str, Any]:
    return hardening._bundle_json(binding, name)


def _bundle_jsonl(binding: Mapping[str, Any], name: str) -> list[dict[str, Any]]:
    return hardening._bundle_jsonl(binding, name)


def _manifest_generated(binding: Mapping[str, Any], name: str) -> datetime:
    return hardening._manifest_generated(binding, name)


def _backfill_binding(backfill_id: str, root: Path) -> dict[str, Any]:
    return history._backfill_binding(backfill_id, root)


def _consistency_binding(consistency_id: str, root: Path) -> dict[str, Any]:
    return hardening._consistency_binding(consistency_id, root)


def _rolling_binding(rolling_eval_id: str, root: Path) -> dict[str, Any]:
    return hardening._rolling_binding(rolling_eval_id, root)


def _warning_binding(impact_id: str, root: Path) -> dict[str, Any]:
    return hardening._warning_binding(impact_id, root)


def _new_source_binding(
    *,
    kind: str,
    artifact_id: str,
    artifact_root: Path,
    validator: Callable[..., dict[str, Any]],
    validator_key: str,
    json_views: Sequence[str],
    jsonl_views: Sequence[str] = (),
    text_views: Sequence[str] = (),
) -> dict[str, Any]:
    return hardening._source_binding(
        kind=kind,
        artifact_id=artifact_id,
        artifact_root=artifact_root,
        validator=validator,
        validator_key=validator_key,
        json_views=json_views,
        jsonl_views=jsonl_views,
        text_views=text_views,
    )


def _instability_binding(instability_id: str, root: Path) -> dict[str, Any]:
    return _new_source_binding(
        kind="limited_instability",
        artifact_id=instability_id,
        artifact_root=root,
        validator=validate_limited_instability_artifact,
        validator_key="instability_id",
        json_views=(
            "limited_instability_input_snapshot.json",
            "limited_instability_manifest.json",
            "instability_reason_summary.json",
            "rolling_failure_pattern.json",
        ),
        jsonl_views=("unstable_window_inventory.jsonl",),
        text_views=("limited_instability_report.md",),
    )


def _risk_binding(risk_attribution_id: str, root: Path) -> dict[str, Any]:
    return _new_source_binding(
        kind="limited_risk_attribution",
        artifact_id=risk_attribution_id,
        artifact_root=root,
        validator=validate_limited_risk_attribution_artifact,
        validator_key="risk_attribution_id",
        json_views=(
            "limited_risk_attribution_input_snapshot.json",
            "limited_risk_attribution_manifest.json",
            "return_contribution_by_symbol.json",
            "drawdown_contribution_by_symbol.json",
            "exposure_shift_attribution.json",
        ),
        jsonl_views=("risk_worsening_events.jsonl",),
        text_views=("limited_risk_attribution_report.md",),
    )


def _repair_binding(repair_plan_id: str, root: Path) -> dict[str, Any]:
    return _new_source_binding(
        kind="data_warning_repair_plan",
        artifact_id=repair_plan_id,
        artifact_root=root,
        validator=validate_data_warning_repair_plan_artifact,
        validator_key="repair_plan_id",
        json_views=(
            "data_warning_repair_plan_input_snapshot.json",
            "data_warning_repair_plan_manifest.json",
            "warning_blocking_matrix.json",
        ),
        jsonl_views=("warning_repair_actions.jsonl",),
        text_views=("data_warning_repair_plan_report.md",),
    )


def _alternative_binding(alt_review_id: str, root: Path) -> dict[str, Any]:
    return _new_source_binding(
        kind="alternative_method_review",
        artifact_id=alt_review_id,
        artifact_root=root,
        validator=validate_alternative_method_review_artifact,
        validator_key="alt_review_id",
        json_views=(
            "alternative_method_review_input_snapshot.json",
            "alternative_method_review_manifest.json",
            "alternative_method_candidates.json",
            "alternative_method_scorecard.json",
        ),
        text_views=("alternative_method_review_report.md",),
    )


def _validate_source_binding(binding: Mapping[str, Any]) -> list[str]:
    kind = _text(binding.get("kind"))
    if kind in {
        "paper_shadow_backfill",
        "paper_shadow_rolling_eval",
        "limited_consistency",
        "data_warning_impact",
    }:
        return hardening._validate_source_binding(binding)
    table: dict[str, tuple[Callable[..., dict[str, Any]], str]] = {
        "limited_instability": (validate_limited_instability_artifact, "instability_id"),
        "limited_risk_attribution": (
            validate_limited_risk_attribution_artifact,
            "risk_attribution_id",
        ),
        "data_warning_repair_plan": (
            validate_data_warning_repair_plan_artifact,
            "repair_plan_id",
        ),
        "alternative_method_review": (
            validate_alternative_method_review_artifact,
            "alt_review_id",
        ),
    }
    errors = _validate_operations_source_bundle(_mapping(binding.get("bundle")))
    try:
        _require(kind in table, f"unknown refinement source kind: {kind}")
        validator, key = table[kind]
        artifact_id = _text(binding.get("artifact_id"))
        source_dir = Path(_text(_mapping(binding.get("bundle")).get("source_dir")))
        actual = validator(**{key: artifact_id, "output_dir": source_dir.parent})
        if actual != _mapping(binding.get("validation")):
            errors.append(f"{kind} source validation drift")
    except Exception as exc:  # noqa: BLE001
        errors.append(str(exc))
    return errors


def _refinement_policy(backfill_binding: Mapping[str, Any]) -> dict[str, Any]:
    policy = hardening._policy_from_backfill_binding(backfill_binding)
    for field in (
        "policy_version",
        "policy_status",
        "policy_owner",
        "reviewed_at",
        "review_condition",
    ):
        _require(bool(_text(policy.get(field))), f"method_hardening_policy.{field} required")
    refinement = _mapping(policy.get("method_refinement"))
    instability = _mapping(refinement.get("instability"))
    risk = _mapping(refinement.get("risk_events"))
    alternative = _mapping(refinement.get("alternative_review"))
    instability_fields = (
        "return_underperformance_tolerance",
        "drawdown_worse_tolerance",
        "high_severity_return_delta",
        "high_severity_drawdown_delta",
        "turnover_high_threshold",
        "weight_jump_threshold",
    )
    risk_fields = (
        "window_days",
        "drawdown_delta_threshold",
        "volatility_delta_threshold",
        "turnover_delta_threshold",
        "high_semiconductor_weight",
        "low_cash_weight",
    )
    for field in instability_fields:
        _require(_finite(instability.get(field)), f"method_refinement.instability.{field} invalid")
    for field in risk_fields:
        _require(_finite(risk.get(field)), f"method_refinement.risk_events.{field} invalid")
    _require(int(float(risk["window_days"])) >= 2, "risk event window_days must be >=2")
    _require(
        float(instability["turnover_high_threshold"]) >= 0.0
        and float(instability["weight_jump_threshold"]) >= 0.0,
        "instability positive thresholds invalid",
    )
    _require(
        0.0 <= float(risk["high_semiconductor_weight"]) <= 1.0
        and 0.0 <= float(risk["low_cash_weight"]) <= 1.0,
        "risk exposure thresholds invalid",
    )
    _require(
        _finite(alternative.get("high_total_return_threshold")),
        "alternative high_total_return_threshold invalid",
    )
    for field in ("existing_methods", "conceptual_methods", "primary_research_candidates"):
        values = [_text(item) for item in alternative.get(field, [])]
        _require(bool(values) and len(values) == len(set(values)), f"alternative {field} invalid")
    conceptual = set(_text(item) for item in alternative.get("conceptual_methods", []))
    primary = set(_text(item) for item in alternative.get("primary_research_candidates", []))
    _require(primary <= conceptual, "primary candidates must be conceptual methods")
    return dict(policy)


def _backfill_payload(binding: Mapping[str, Any]) -> dict[str, Any]:
    return {
        **_bundle_json(binding, "paper_shadow_backfill_manifest.json"),
        "paper_shadow_backfill_input_snapshot": _bundle_json(
            binding, "paper_shadow_backfill_input_snapshot.json"
        ),
        "backfill_method_states": _bundle_jsonl(binding, "backfill_method_states.jsonl"),
        "backfill_rebalance_events": _bundle_jsonl(binding, "backfill_rebalance_events.jsonl"),
        "backfill_daily_portfolio": _bundle_jsonl(binding, "backfill_daily_portfolio.jsonl"),
    }


def _strict_state_rows(states: Sequence[Mapping[str, Any]], method: str) -> list[dict[str, Any]]:
    rows = sorted(
        [dict(row) for row in states if _text(row.get("target_method")) == method],
        key=lambda row: _text(row.get("date")),
    )
    seen: set[str] = set()
    for row in rows:
        day = _text(row.get("date"))
        _date(day, field=f"{method} state date")
        _require(day not in seen, f"duplicate {method} state date: {day}")
        seen.add(day)
        for field in ("portfolio_value", "daily_return", "turnover"):
            _require(_finite(row.get(field)), f"{method}.{field} must be finite")
        weights = _mapping(row.get("weights"))
        _require(bool(weights), f"{method} weights required")
        _require(
            all(_finite(value) and float(value) >= 0.0 for value in weights.values()),
            f"{method} weights must be finite nonnegative",
        )
    return rows


def _mean(values: Sequence[float]) -> float | None:
    return sum(values) / len(values) if values else None


def _stddev(values: Sequence[float]) -> float | None:
    if len(values) < 2:
        return None
    mean = sum(values) / len(values)
    return math.sqrt(sum((value - mean) ** 2 for value in values) / (len(values) - 1))


def _drawdown(values: Sequence[float]) -> float | None:
    if not values:
        return None
    equity = 1.0
    peak = 1.0
    worst = 0.0
    for value in values:
        equity *= 1.0 + value
        peak = max(peak, equity)
        worst = min(worst, equity / peak - 1.0)
    return worst


def _path_metrics(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    if len(rows) < 2:
        return {
            "observation_count": len(rows),
            "total_return": None,
            "max_drawdown": None,
            "realized_volatility": None,
            "turnover": None,
        }
    values = [float(row["portfolio_value"]) for row in rows]
    daily = [float(row["daily_return"]) for row in rows]
    volatility = _stddev(daily)
    return {
        "observation_count": len(rows),
        "total_return": round(values[-1] / values[0] - 1.0, 10) if values[0] > 0 else None,
        "max_drawdown": _number(_drawdown(daily)),
        "realized_volatility": _number(
            volatility * math.sqrt(252.0) if volatility is not None else None
        ),
        "turnover": round(sum(float(row["turnover"]) for row in rows), 10),
    }


def _max_weight_jump(
    states: Sequence[Mapping[str, Any]], *, start: date, end: date, method: str
) -> float | None:
    selected = [
        row
        for row in _strict_state_rows(states, method)
        if start <= _date(row.get("date"), field="state date") <= end
    ]
    if len(selected) < 2:
        return None
    jumps: list[float] = []
    for previous, current in zip(selected, selected[1:], strict=False):
        before = _mapping(previous.get("weights"))
        after = _mapping(current.get("weights"))
        jumps.append(
            max(
                abs(float(after.get(symbol, 0.0)) - float(before.get(symbol, 0.0)))
                for symbol in set(before) | set(after)
            )
        )
    return _number(max(jumps))


def _failure_reasons(
    *,
    limited: Mapping[str, Any],
    drawdown_delta: float | None,
    risk_adjusted_delta: float | None,
    method_count: int,
    weight_jump: float | None,
    policy: Mapping[str, Any],
) -> list[str]:
    thresholds = _mapping(_mapping(policy.get("method_refinement")).get("instability"))
    reasons: list[str] = []
    relative_static = _number(limited.get("relative_to_static_baseline"))
    relative_no_trade = _number(limited.get("relative_to_no_trade_baseline"))
    tolerance = float(thresholds["return_underperformance_tolerance"])
    if relative_static is None or relative_no_trade is None:
        reasons.append("baseline_missing")
    elif relative_static < tolerance or relative_no_trade < tolerance:
        reasons.append("return_underperformance")
    if drawdown_delta is None:
        reasons.append("drawdown_unknown")
    elif drawdown_delta < float(thresholds["drawdown_worse_tolerance"]):
        reasons.append("drawdown_worse")
    rank = _number(limited.get("rank_by_risk_adjusted"))
    if risk_adjusted_delta is None or rank is None:
        reasons.append("risk_adjusted_unknown")
    elif risk_adjusted_delta < 0.0 or rank > max(1, method_count - 2):
        reasons.append("risk_adjusted_worse")
    turnover = _number(limited.get("turnover"))
    if turnover is None:
        reasons.append("turnover_unknown")
    elif turnover >= float(thresholds["turnover_high_threshold"]):
        reasons.append("turnover_high")
    if weight_jump is None:
        reasons.append("weight_jump_unknown")
    elif weight_jump >= float(thresholds["weight_jump_threshold"]):
        reasons.append("weight_jump_linked")
    return sorted(set(reasons))


def _failure_type(reasons: Sequence[str]) -> str:
    priority = (
        "return_underperformance",
        "drawdown_worse",
        "risk_adjusted_worse",
        "turnover_high",
    )
    material = [reason for reason in reasons if reason in priority]
    return material[0] if len(material) == 1 else "mixed"


def _failure_severity(
    *,
    reasons: Sequence[str],
    return_delta: float | None,
    drawdown_delta: float | None,
    policy: Mapping[str, Any],
) -> str:
    thresholds = _mapping(_mapping(policy.get("method_refinement")).get("instability"))
    material = [
        reason
        for reason in reasons
        if reason
        not in {
            "weight_jump_linked",
            "weight_jump_unknown",
            "turnover_unknown",
            "risk_adjusted_unknown",
            "drawdown_unknown",
            "baseline_missing",
        }
    ]
    if (
        len(material) >= 2
        or (
            return_delta is not None
            and return_delta <= float(thresholds["high_severity_return_delta"])
        )
        or (
            drawdown_delta is not None
            and drawdown_delta <= float(thresholds["high_severity_drawdown_delta"])
        )
    ):
        return "HIGH"
    return "MEDIUM" if material else "LOW"


def _unstable_window_inventory(
    *,
    rolling: Sequence[Mapping[str, Any]],
    states: Sequence[Mapping[str, Any]],
    config: Mapping[str, Any],
    policy: Mapping[str, Any],
) -> list[dict[str, Any]]:
    by_window: dict[str, list[Mapping[str, Any]]] = {}
    for row in rolling:
        if row.get("status") == "INSUFFICIENT_DATA":
            continue
        window_id = _text(row.get("window_id"))
        _require(bool(window_id), "rolling window_id required")
        by_window.setdefault(window_id, []).append(row)
    labels = legacy._regime_labels_from_states(states, config)
    rows: list[dict[str, Any]] = []
    for window_id, window_rows in sorted(by_window.items()):
        limited = next(
            (dict(row) for row in window_rows if row.get("target_method") == "limited_adjustment"),
            {},
        )
        if not limited:
            continue
        static = next(
            (dict(row) for row in window_rows if row.get("target_method") == "static_baseline"),
            {},
        )
        no_trade = next(
            (dict(row) for row in window_rows if row.get("target_method") == "no_trade_baseline"),
            {},
        )
        start = _date(limited.get("start_date"), field="rolling start_date")
        end = _date(limited.get("end_date"), field="rolling end_date")
        limited_drawdown = _number(limited.get("max_drawdown"))
        static_drawdown = _number(static.get("max_drawdown"))
        limited_risk_adjusted = _number(limited.get("risk_adjusted_return_to_volatility"))
        static_risk_adjusted = _number(static.get("risk_adjusted_return_to_volatility"))
        drawdown_delta = (
            round(limited_drawdown - static_drawdown, 10)
            if limited_drawdown is not None and static_drawdown is not None
            else None
        )
        risk_adjusted_delta = (
            round(limited_risk_adjusted - static_risk_adjusted, 10)
            if limited_risk_adjusted is not None and static_risk_adjusted is not None
            else None
        )
        weight_jump = _max_weight_jump(states, start=start, end=end, method="limited_adjustment")
        method_count = len({_text(row.get("target_method")) for row in window_rows})
        reasons = _failure_reasons(
            limited=limited,
            drawdown_delta=drawdown_delta,
            risk_adjusted_delta=risk_adjusted_delta,
            method_count=method_count,
            weight_jump=weight_jump,
            policy=policy,
        )
        if not reasons:
            continue
        regime_tags = legacy._window_regime_tags(labels, start=start, end=end)
        return_delta = _number(limited.get("relative_to_static_baseline"))
        rows.append(
            {
                "window_id": window_id,
                "window_type": limited.get("window_type"),
                "start_date": start.isoformat(),
                "end_date": end.isoformat(),
                "limited_adjustment_rank_return": _number(limited.get("rank_by_return")),
                "limited_adjustment_rank_risk_adjusted": _number(
                    limited.get("rank_by_risk_adjusted")
                ),
                "relative_to_static_baseline": return_delta,
                "relative_to_no_trade_baseline": _number(
                    limited.get("relative_to_no_trade_baseline")
                ),
                "drawdown_delta_vs_static": drawdown_delta,
                "risk_adjusted_delta_vs_static": risk_adjusted_delta,
                "turnover": _number(limited.get("turnover")),
                "max_weight_jump": weight_jump,
                "regime_tags": regime_tags,
                "failure_reasons": reasons,
                "failure_type": _failure_type(reasons),
                "severity": _failure_severity(
                    reasons=reasons,
                    return_delta=return_delta,
                    drawdown_delta=drawdown_delta,
                    policy=policy,
                ),
                "static_total_return": _number(static.get("total_return")),
                "no_trade_total_return": _number(no_trade.get("total_return")),
                "missing_metrics_remain_null": True,
                **SYSTEM_TARGET_SAFETY,
            }
        )
    return rows


def _instability_summary(
    consistency: Mapping[str, Any],
    inventory: Sequence[Mapping[str, Any]],
    policy: Mapping[str, Any],
) -> dict[str, Any]:
    reason_counts: dict[str, int] = {}
    regime_counts: dict[str, int] = {}
    for row in inventory:
        for reason in row.get("failure_reasons", []):
            reason_counts[_text(reason)] = reason_counts.get(_text(reason), 0) + 1
        for regime in row.get("regime_tags", []):
            regime_counts[_text(regime)] = regime_counts.get(_text(regime), 0) + 1
    total = len(inventory)
    top_reasons = (
        [
            {"reason": reason, "count": count, "share": round(count / total, 6)}
            for reason, count in sorted(reason_counts.items(), key=lambda item: (-item[1], item[0]))
        ]
        if total
        else []
    )
    dominant = legacy._dominant_failure_regime(regime_counts)
    pressure = set(_text(item) for item in policy.get("pressure_regimes", []))
    normalized_pressure = pressure | {"semiconductor_pullback"}
    if total == 0:
        recommendation = "insufficient_data"
    elif dominant in normalized_pressure:
        recommendation = "consider_regime_gate"
    elif any(row["reason"] == "drawdown_worse" for row in top_reasons):
        recommendation = "consider_risk_cap"
    else:
        recommendation = "continue_diagnosis"
    rolling = _mapping(consistency.get("rolling_consistency_summary"))
    return {
        "target_method": _text(policy.get("candidate_method")),
        "rolling_consistency_status": _text(
            rolling.get("rolling_consistency_status"), "INSUFFICIENT_DATA"
        ),
        "unstable_window_count": total,
        "top_reasons": top_reasons,
        "dominant_failure_regime": dominant,
        "recommendation": recommendation,
        "policy_version": policy.get("policy_version"),
        "missing_metrics_remain_null": True,
        **SYSTEM_TARGET_SAFETY,
    }


def _failure_pattern(
    inventory: Sequence[Mapping[str, Any]], policy: Mapping[str, Any]
) -> dict[str, Any]:
    thresholds = _mapping(_mapping(policy.get("method_refinement")).get("instability"))
    pressure = set(_text(item) for item in policy.get("pressure_regimes", []))
    pressure_rows = [
        row
        for row in inventory
        if pressure & set(_text(item) for item in row.get("regime_tags", []))
    ]
    return_good_drawdown_worse = [
        row
        for row in inventory
        if _number(row.get("relative_to_static_baseline")) is not None
        and float(row["relative_to_static_baseline"]) > 0.0
        and _number(row.get("drawdown_delta_vs_static")) is not None
        and float(row["drawdown_delta_vs_static"]) < float(thresholds["drawdown_worse_tolerance"])
    ]
    turnover = [row for row in inventory if "turnover_high" in set(row.get("failure_reasons", []))]
    jumps = [
        row for row in inventory if "weight_jump_linked" in set(row.get("failure_reasons", []))
    ]
    return {
        "patterns": [
            {
                "pattern": "underperforms_in_pressure_regime",
                "supporting_windows": len(pressure_rows),
                "description": "limited_adjustment weakness overlaps reviewed pressure regimes.",
                "possible_fix": "regime_gate_risk_increase",
            },
            {
                "pattern": "return_good_but_drawdown_worse",
                "supporting_windows": len(return_good_drawdown_worse),
                "description": "limited_adjustment can improve return while worsening drawdown.",
                "possible_fix": "risk_cap_or_drawdown_guard",
            },
            {
                "pattern": "turnover_or_weight_jump_linked",
                "supporting_windows": len(turnover) + len(jumps),
                "description": "Weak windows can coincide with turnover or weight jumps.",
                "possible_fix": "lower_turnover_or_smoother_adjustment",
            },
        ],
        "policy_version": policy.get("policy_version"),
        **SYSTEM_TARGET_SAFETY,
    }


def _returns_from_backfill(binding: Mapping[str, Any]) -> pd.DataFrame:
    snapshot = _bundle_json(binding, "paper_shadow_backfill_input_snapshot.json")
    symbols = [_text(item) for item in snapshot.get("expected_symbols", [])]
    rows = _records(snapshot.get("price_rows"))
    _require(bool(symbols) and bool(rows), "backfill bounded price rows required")
    frame = pd.DataFrame(rows)
    _require(
        {"date", "ticker", "adj_close"} <= set(frame.columns),
        "backfill bounded price row schema invalid",
    )
    frame["date"] = pd.to_datetime(frame["date"], errors="raise")
    frame["adj_close"] = pd.to_numeric(frame["adj_close"], errors="raise")
    _require(
        bool(frame["adj_close"].map(lambda value: _finite(value) and float(value) > 0.0).all()),
        "backfill bounded prices must be finite positive",
    )
    pivot = frame.pivot(index="date", columns="ticker", values="adj_close").sort_index()
    _require(not (set(symbols) - set(pivot.columns)), "backfill bounded price symbols missing")
    start = _date(snapshot.get("actual_start_date"), field="backfill actual_start_date")
    end = _date(snapshot.get("actual_end_date"), field="backfill actual_end_date")
    common = pivot.loc[(pivot.index.date >= start) & (pivot.index.date <= end), symbols].dropna(
        how="any"
    )
    _require(len(common) >= 2, "backfill common price dates insufficient")
    returns = common.pct_change(fill_method=None)
    returns.index = [index.date().isoformat() for index in returns.index]
    return returns


def _symbol_contribution(
    rows: Sequence[Mapping[str, Any]], returns: pd.DataFrame
) -> dict[str, float | None]:
    contribution = {str(symbol): 0.0 for symbol in returns.columns}
    observations = {str(symbol): 0 for symbol in returns.columns}
    for row in rows:
        day = _text(row.get("date"))
        if day not in returns.index:
            continue
        weights = _mapping(row.get("weights"))
        for symbol in returns.columns:
            daily_return = returns.loc[day, symbol]
            if not _finite(daily_return):
                continue
            weight = weights.get(str(symbol), 0.0)
            _require(_finite(weight), f"weight must be finite: {day}/{symbol}")
            contribution[str(symbol)] += float(weight) * float(daily_return)
            observations[str(symbol)] += 1
    return {
        symbol: round(value, 10) if observations[symbol] else None
        for symbol, value in contribution.items()
    }


def _average_weight(rows: Sequence[Mapping[str, Any]], symbol: str) -> float | None:
    values = [float(_mapping(row.get("weights")).get(symbol, 0.0)) for row in rows]
    return _number(_mean(values))


def _return_contribution(
    states: Sequence[Mapping[str, Any]], returns: pd.DataFrame, policy: Mapping[str, Any]
) -> dict[str, Any]:
    candidate = _text(policy.get("candidate_method"))
    baselines = [_text(item) for item in policy.get("comparison_baselines", [])]
    _require("static_baseline" in baselines, "static_baseline comparison required")
    limited_rows = _strict_state_rows(states, candidate)
    static_rows = _strict_state_rows(states, "static_baseline")
    limited = _symbol_contribution(limited_rows, returns)
    static = _symbol_contribution(static_rows, returns)
    symbols = sorted(set(limited) | set(static))
    rows = []
    for symbol in symbols:
        limited_value = limited.get(symbol)
        static_value = static.get(symbol)
        rows.append(
            {
                "symbol": symbol,
                "avg_weight": _average_weight(limited_rows, symbol),
                "return_contribution": limited_value,
                "relative_contribution_vs_static": (
                    round(float(limited_value) - float(static_value), 10)
                    if limited_value is not None and static_value is not None
                    else None
                ),
            }
        )
    comparable = [row for row in rows if _finite(row.get("relative_contribution_vs_static"))]
    positives = [
        row["symbol"]
        for row in sorted(
            comparable,
            key=lambda row: float(row["relative_contribution_vs_static"]),
            reverse=True,
        )
        if float(row["relative_contribution_vs_static"]) > 0.0
    ][:3]
    negatives = [
        row["symbol"]
        for row in sorted(comparable, key=lambda row: float(row["relative_contribution_vs_static"]))
        if float(row["relative_contribution_vs_static"]) < 0.0
    ][:3]
    return {
        "target_method": candidate,
        "symbols": rows,
        "top_positive_contributors": positives,
        "top_negative_contributors": negatives,
        "policy_version": policy.get("policy_version"),
        "missing_metrics_remain_null": True,
        **SYSTEM_TARGET_SAFETY,
    }


def _max_drawdown_window(rows: Sequence[Mapping[str, Any]]) -> tuple[date | None, date | None]:
    if not rows:
        return None, None
    peak_value = float(rows[0]["portfolio_value"])
    peak_date = _date(rows[0].get("date"), field="portfolio state date")
    worst = 0.0
    worst_start = peak_date
    worst_end = peak_date
    for row in rows:
        value = float(row["portfolio_value"])
        current = _date(row.get("date"), field="portfolio state date")
        if value > peak_value:
            peak_value = value
            peak_date = current
        drawdown = value / peak_value - 1.0 if peak_value > 0.0 else 0.0
        if drawdown < worst:
            worst = drawdown
            worst_start = peak_date
            worst_end = current
    return worst_start, worst_end


def _drawdown_contribution(
    states: Sequence[Mapping[str, Any]], returns: pd.DataFrame, policy: Mapping[str, Any]
) -> dict[str, Any]:
    candidate = _text(policy.get("candidate_method"))
    limited_rows = _strict_state_rows(states, candidate)
    static_rows = _strict_state_rows(states, "static_baseline")
    start, end = _max_drawdown_window(limited_rows)
    if start is None or end is None:
        limited_window: list[dict[str, Any]] = []
        static_window: list[dict[str, Any]] = []
    else:
        limited_window = [
            row
            for row in limited_rows
            if start <= _date(row.get("date"), field="state date") <= end
        ]
        static_window = [
            row for row in static_rows if start <= _date(row.get("date"), field="state date") <= end
        ]
    limited = _symbol_contribution(limited_window, returns)
    static = _symbol_contribution(static_window, returns)
    symbols = sorted(set(limited) | set(static))
    rows = []
    for symbol in symbols:
        limited_value = limited.get(symbol)
        static_value = static.get(symbol)
        rows.append(
            {
                "symbol": symbol,
                "drawdown_contribution": limited_value,
                "weight_during_drawdown": _average_weight(limited_window, symbol),
                "relative_to_static": (
                    round(float(limited_value) - float(static_value), 10)
                    if limited_value is not None and static_value is not None
                    else None
                ),
            }
        )
    top = [
        row["symbol"]
        for row in sorted(
            [row for row in rows if _finite(row.get("drawdown_contribution"))],
            key=lambda row: float(row["drawdown_contribution"]),
        )
        if float(row["drawdown_contribution"]) < 0.0
    ][:3]
    return {
        "target_method": candidate,
        "max_drawdown_window": {
            "start_date": start.isoformat() if start is not None else None,
            "end_date": end.isoformat() if end is not None else None,
        },
        "symbols": rows,
        "top_drawdown_contributors": top,
        "policy_version": policy.get("policy_version"),
        "missing_metrics_remain_null": True,
        **SYSTEM_TARGET_SAFETY,
    }


def _exposure_summary(
    rows: Sequence[Mapping[str, Any]], policy: Mapping[str, Any]
) -> dict[str, float | None]:
    if not rows:
        return {
            "avg_risk_asset_weight": None,
            "avg_semiconductor_weight": None,
            "avg_cash_weight": None,
        }
    risk_symbols = set(_text(item) for item in policy.get("risk_exposure_symbols", []))
    semiconductor = set(_text(item) for item in policy.get("semiconductor_symbols", []))
    risk_values: list[float] = []
    semiconductor_values: list[float] = []
    cash_values: list[float] = []
    for row in rows:
        weights = _mapping(row.get("weights"))
        risk_values.append(sum(float(weights.get(symbol, 0.0)) for symbol in risk_symbols))
        semiconductor_values.append(
            sum(float(weights.get(symbol, 0.0)) for symbol in semiconductor)
        )
        cash_values.append(float(weights.get("CASH", 0.0)))
    return {
        "avg_risk_asset_weight": _number(_mean(risk_values)),
        "avg_semiconductor_weight": _number(_mean(semiconductor_values)),
        "avg_cash_weight": _number(_mean(cash_values)),
    }


def _exposure_attribution(
    states: Sequence[Mapping[str, Any]], policy: Mapping[str, Any]
) -> dict[str, Any]:
    candidate = _text(policy.get("candidate_method"))
    limited = _exposure_summary(_strict_state_rows(states, candidate), policy)
    static = _exposure_summary(_strict_state_rows(states, "static_baseline"), policy)
    tolerance = float(policy["exposure_similarity_tolerance"])

    def delta(field: str) -> float | None:
        left = limited.get(field)
        right = static.get(field)
        return (
            round(float(left) - float(right), 10)
            if left is not None and right is not None
            else None
        )

    risk_delta = delta("avg_risk_asset_weight")
    semi_delta = delta("avg_semiconductor_weight")
    cash_delta = delta("avg_cash_weight")
    sources: list[str] = []
    if semi_delta is not None and semi_delta > tolerance:
        sources.append("higher_semiconductor_exposure")
    if risk_delta is not None and risk_delta > tolerance:
        sources.append("higher_risk_asset_exposure")
    if cash_delta is not None and cash_delta < -tolerance:
        sources.append("lower_cash")
    if any(value is None for value in (risk_delta, semi_delta, cash_delta)):
        source = "unknown"
    elif len(sources) > 1:
        source = "mixed"
    elif sources:
        source = sources[0]
    else:
        source = "unknown"
    return {
        "target_method": candidate,
        "avg_risk_asset_weight": limited.get("avg_risk_asset_weight"),
        "avg_risk_asset_delta_vs_static": risk_delta,
        "avg_semiconductor_weight": limited.get("avg_semiconductor_weight"),
        "avg_semiconductor_delta_vs_static": semi_delta,
        "avg_cash_weight": limited.get("avg_cash_weight"),
        "avg_cash_delta_vs_static": cash_delta,
        "risk_worsening_source": source,
        "policy_version": policy.get("policy_version"),
        "missing_metrics_remain_null": True,
        **SYSTEM_TARGET_SAFETY,
    }


def _risk_likely_cause(exposure: Mapping[str, Any], policy: Mapping[str, Any]) -> str:
    thresholds = _mapping(_mapping(policy.get("method_refinement")).get("risk_events"))
    semiconductor = _number(exposure.get("avg_semiconductor_weight"))
    cash = _number(exposure.get("avg_cash_weight"))
    if semiconductor is None or cash is None:
        return "unknown"
    if semiconductor >= float(thresholds["high_semiconductor_weight"]):
        return "risk_exposure_too_high"
    if cash <= float(thresholds["low_cash_weight"]):
        return "late_risk_reduction"
    return "unknown"


def _risk_events(
    states: Sequence[Mapping[str, Any]], policy: Mapping[str, Any]
) -> list[dict[str, Any]]:
    candidate = _text(policy.get("candidate_method"))
    limited = _strict_state_rows(states, candidate)
    static = _strict_state_rows(states, "static_baseline")
    static_by_date = {_text(row.get("date")): row for row in static}
    thresholds = _mapping(_mapping(policy.get("method_refinement")).get("risk_events"))
    window_days = int(float(thresholds["window_days"]))
    rows: list[dict[str, Any]] = []
    for index in range(window_days - 1, len(limited)):
        window = limited[index - window_days + 1 : index + 1]
        static_window = [
            static_by_date[_text(row.get("date"))]
            for row in window
            if _text(row.get("date")) in static_by_date
        ]
        if len(static_window) != window_days:
            continue
        limited_returns = [float(row["daily_return"]) for row in window]
        static_returns = [float(row["daily_return"]) for row in static_window]
        limited_drawdown = _drawdown(limited_returns)
        static_drawdown = _drawdown(static_returns)
        limited_vol = _stddev(limited_returns)
        static_vol = _stddev(static_returns)
        if None in {limited_drawdown, static_drawdown, limited_vol, static_vol}:
            continue
        drawdown_delta = round(float(limited_drawdown) - float(static_drawdown), 10)
        vol_delta = round(
            float(limited_vol) * math.sqrt(252.0) - float(static_vol) * math.sqrt(252.0),
            10,
        )
        turnover_delta = round(
            sum(float(row["turnover"]) for row in window)
            - sum(float(row["turnover"]) for row in static_window),
            10,
        )
        reasons: list[str] = []
        if drawdown_delta < float(thresholds["drawdown_delta_threshold"]):
            reasons.append("drawdown_deeper")
        if vol_delta > float(thresholds["volatility_delta_threshold"]):
            reasons.append("volatility_higher")
        if turnover_delta > float(thresholds["turnover_delta_threshold"]):
            reasons.append("turnover_higher")
        if not reasons:
            continue
        exposure = _exposure_summary(window, policy)
        event_type = reasons[0] if len(reasons) == 1 else "mixed"
        event_date = _text(window[-1].get("date"))
        rows.append(
            {
                "event_id": _stable_id(
                    "risk-worsening-event-v2", event_date, event_type, policy["policy_version"]
                ),
                "date": event_date,
                "window": f"{window_days}d",
                "risk_worsening_type": event_type,
                "risk_asset_weight": exposure.get("avg_risk_asset_weight"),
                "semiconductor_weight": exposure.get("avg_semiconductor_weight"),
                "cash_weight": exposure.get("avg_cash_weight"),
                "relative_drawdown_vs_static": drawdown_delta,
                "volatility_delta_vs_static": vol_delta,
                "turnover_delta_vs_static": turnover_delta,
                "likely_cause": _risk_likely_cause(exposure, policy),
                "policy_version": policy.get("policy_version"),
                **SYSTEM_TARGET_SAFETY,
            }
        )
    return rows


def _warning_actions(impact: Mapping[str, Any]) -> list[dict[str, Any]]:
    actions = legacy._warning_repair_actions(impact)
    _require(bool(actions), "warning repair actions required; missing detail must remain unknown")
    return [dict(row) for row in actions]


def _warning_matrix(
    impact: Mapping[str, Any], actions: Sequence[Mapping[str, Any]]
) -> dict[str, Any]:
    return {
        "schema_version": legacy.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_data_warning_blocking_matrix",
        "status": "PASS",
        **legacy._warning_blocking_matrix(impact, actions),
        **SYSTEM_TARGET_SAFETY,
    }


_CONCEPTUAL_METHOD_DESCRIPTIONS: dict[str, dict[str, Any]] = {
    "risk_capped_limited_adjustment": {
        "description": "limited_adjustment with reviewed risk-asset increase caps.",
        "expected_benefit": "reduce drawdown worsening",
        "expected_cost": "may reduce return improvement",
    },
    "regime_gated_limited_adjustment": {
        "description": "allow limited adjustment only outside reviewed pressure regimes.",
        "expected_benefit": "improve pressure regime behavior",
        "expected_cost": "may miss rebound after drawdown",
    },
    "lower_turnover_limited_adjustment": {
        "description": "smaller adjustment step or slower rebalance cadence.",
        "expected_benefit": "reduce turnover and weight jumps",
        "expected_cost": "slower response to target evidence",
    },
    "cash_buffered_limited_adjustment": {
        "description": "retain a larger cash floor while the method remains under review.",
        "expected_benefit": "reduce drawdown sensitivity",
        "expected_cost": "may reduce upside capture",
    },
}


def _alternative_candidates(policy: Mapping[str, Any]) -> dict[str, Any]:
    alternative = _mapping(_mapping(policy.get("method_refinement")).get("alternative_review"))
    methods = [_text(item) for item in alternative.get("conceptual_methods", [])]
    _require(set(methods) <= set(_CONCEPTUAL_METHOD_DESCRIPTIONS), "unknown conceptual method")
    return {
        "candidates": [
            {
                "method": method,
                "status": "PROPOSED",
                **_CONCEPTUAL_METHOD_DESCRIPTIONS[method],
                "requires_new_implementation": True,
                "auto_apply": False,
                "policy_version": policy.get("policy_version"),
                **SYSTEM_TARGET_SAFETY,
            }
            for method in methods
        ],
        "policy_version": policy.get("policy_version"),
        **SYSTEM_TARGET_SAFETY,
    }


def _return_expectation(metrics: Mapping[str, Any], policy: Mapping[str, Any]) -> str:
    total = _number(metrics.get("total_return"))
    if total is None:
        return "UNKNOWN"
    threshold = float(
        _mapping(_mapping(policy.get("method_refinement")).get("alternative_review"))[
            "high_total_return_threshold"
        ]
    )
    if total > threshold:
        return "HIGHER"
    return "MEDIUM" if total > 0.0 else "LOWER"


def _risk_expectation(metrics: Mapping[str, Any], static: Mapping[str, Any]) -> str:
    drawdown = _number(metrics.get("max_drawdown"))
    static_drawdown = _number(static.get("max_drawdown"))
    if drawdown is None or static_drawdown is None:
        return "UNKNOWN"
    return "BETTER" if drawdown >= static_drawdown else "WORSE"


def _recommended_alternative(
    risk: Mapping[str, Any], instability: Mapping[str, Any], policy: Mapping[str, Any]
) -> str:
    primary = [
        _text(item)
        for item in _mapping(
            _mapping(policy.get("method_refinement")).get("alternative_review")
        ).get("primary_research_candidates", [])
    ]
    exposure = _mapping(risk.get("exposure_shift_attribution"))
    summary = _mapping(instability.get("instability_reason_summary"))
    if (
        _text(exposure.get("risk_worsening_source"))
        in {
            "higher_risk_asset_exposure",
            "higher_semiconductor_exposure",
            "lower_cash",
            "mixed",
        }
        and "risk_capped_limited_adjustment" in primary
    ):
        return "risk_capped_limited_adjustment"
    if (
        summary.get("recommendation") == "consider_regime_gate"
        and "regime_gated_limited_adjustment" in primary
    ):
        return "regime_gated_limited_adjustment"
    return primary[0]


def _alternative_scorecard(
    backfill: Mapping[str, Any],
    risk: Mapping[str, Any],
    instability: Mapping[str, Any],
    candidates: Mapping[str, Any],
    policy: Mapping[str, Any],
) -> dict[str, Any]:
    states = _records(backfill.get("backfill_method_states"))
    alternative = _mapping(_mapping(policy.get("method_refinement")).get("alternative_review"))
    existing = [_text(item) for item in alternative.get("existing_methods", [])]
    static_metrics = _path_metrics(_strict_state_rows(states, "static_baseline"))
    methods: list[dict[str, Any]] = []
    for method in existing:
        rows = _strict_state_rows(states, method)
        metrics = _path_metrics(rows)
        methods.append(
            {
                "method": method,
                "current_status": (
                    "REVIEW_REQUIRED" if method == policy["candidate_method"] else "EXISTING"
                ),
                "return_expectation": _return_expectation(metrics, policy),
                "risk_expectation": _risk_expectation(metrics, static_metrics),
                "stability_expectation": (
                    _text(
                        _mapping(instability.get("instability_reason_summary")).get(
                            "rolling_consistency_status"
                        ),
                        "UNKNOWN",
                    )
                    if method == policy["candidate_method"]
                    else "UNKNOWN"
                ),
                "implementation_status": "EXISTING",
                "recommendation": (
                    "KEEP_AS_SECONDARY_OR_REFINE"
                    if method == policy["candidate_method"]
                    else "KEEP_AS_REFERENCE"
                ),
                "metrics": metrics,
            }
        )
    primary = set(_text(item) for item in alternative.get("primary_research_candidates", []))
    for row in _records(candidates.get("candidates")):
        method = _text(row.get("method"))
        methods.append(
            {
                "method": method,
                "current_status": "PROPOSED",
                "return_expectation": "UNKNOWN",
                "risk_expectation": "UNKNOWN",
                "stability_expectation": "UNKNOWN",
                "implementation_status": "NOT_IMPLEMENTED",
                "recommendation": (
                    "IMPLEMENT_AS_RESEARCH_CANDIDATE"
                    if method in primary
                    else "CONSIDER_AFTER_PRIMARY_REFINEMENT"
                ),
                "metrics": {
                    "observation_count": 0,
                    "total_return": None,
                    "max_drawdown": None,
                    "realized_volatility": None,
                    "turnover": None,
                },
            }
        )
    return {
        "methods": methods,
        "recommended_alternative": _recommended_alternative(risk, instability, policy),
        "policy_version": policy.get("policy_version"),
        "conceptual_metrics_remain_null": True,
        **SYSTEM_TARGET_SAFETY,
    }


def _refined_decision(
    instability: Mapping[str, Any],
    risk: Mapping[str, Any],
    repair: Mapping[str, Any],
    alt_review: Mapping[str, Any],
    policy: Mapping[str, Any],
) -> dict[str, Any]:
    instability_summary = _mapping(instability.get("instability_reason_summary"))
    exposure = _mapping(risk.get("exposure_shift_attribution"))
    matrix = _mapping(repair.get("warning_blocking_matrix"))
    scorecard = _mapping(alt_review.get("alternative_method_scorecard"))
    recommended = _text(scorecard.get("recommended_alternative"))
    warning_after = _text(matrix.get("hardening_allowed_after_repair"), "UNKNOWN")
    if warning_after == "NO":
        next_step = "REPAIR_DATA_WARNINGS_FIRST"
    elif instability_summary.get("recommendation") == "insufficient_data":
        next_step = "CONTINUE_OBSERVATION"
    elif recommended == "risk_capped_limited_adjustment":
        next_step = "IMPLEMENT_RISK_CAPPED_RESEARCH_METHOD"
    elif recommended == "regime_gated_limited_adjustment":
        next_step = "IMPLEMENT_REGIME_GATED_RESEARCH_METHOD"
    else:
        next_step = "DEFER"
    confidence = (
        "LOW"
        if warning_after == "UNKNOWN"
        else (
            "MEDIUM"
            if next_step
            in {
                "IMPLEMENT_RISK_CAPPED_RESEARCH_METHOD",
                "IMPLEMENT_REGIME_GATED_RESEARCH_METHOD",
            }
            else "LOW"
        )
    )
    return {
        "schema_version": legacy.SCHEMA_VERSION,
        "proposal_id": "",
        "current_method": _text(policy.get("candidate_method")),
        "current_hardening_status": "REVIEW_REQUIRED",
        "recommended_next_step": next_step,
        "reason": (
            "limited_adjustment did not harden because rolling consistency is "
            f"{instability_summary.get('rolling_consistency_status')}, risk worsening source is "
            f"{exposure.get('risk_worsening_source')}, and data warning repair outcome is "
            f"{warning_after}."
        ),
        "confidence": confidence,
        "policy_version": policy.get("policy_version"),
        "auto_apply": False,
        "research_target_only": True,
        "not_official_target_weights": True,
        "broker_action_allowed": False,
        "production_effect": "none",
        "data_warnings_need_repair_before_hardening": warning_after != "YES",
        "limited_adjustment_secondary_research_method": True,
        "next_action": (
            "implement_research_only_refined_method_candidate"
            if next_step.startswith("IMPLEMENT_")
            else "owner_review_required"
        ),
        **SYSTEM_TARGET_SAFETY,
    }


def _proposed_methods(decision: Mapping[str, Any], policy: Mapping[str, Any]) -> dict[str, Any]:
    primary = [
        _text(item)
        for item in _mapping(
            _mapping(policy.get("method_refinement")).get("alternative_review")
        ).get("primary_research_candidates", [])
    ]
    preferred_step = _text(decision.get("recommended_next_step"))
    preferred_method = {
        "IMPLEMENT_RISK_CAPPED_RESEARCH_METHOD": "risk_capped_limited_adjustment",
        "IMPLEMENT_REGIME_GATED_RESEARCH_METHOD": "regime_gated_limited_adjustment",
    }.get(preferred_step)
    validation = {
        "risk_capped_limited_adjustment": [
            "historical backfill",
            "rolling consistency",
            "pressure regime review",
            "forward confirmation",
        ],
        "regime_gated_limited_adjustment": [
            "regime-specific simulation",
            "forward pressure samples",
        ],
    }
    improvement = {
        "risk_capped_limited_adjustment": (
            "reduce drawdown worsening while preserving part of return improvement"
        ),
        "regime_gated_limited_adjustment": "avoid active risk increase in pressure regimes",
    }
    return {
        "methods": [
            {
                "method": method,
                "priority": "HIGH" if method == preferred_method else "MEDIUM",
                "implementation_scope": "research_only",
                "expected_improvement": improvement.get(method, "UNKNOWN"),
                "required_validation": validation.get(method, []),
                "auto_apply": False,
                **SYSTEM_TARGET_SAFETY,
            }
            for method in primary
        ],
        "policy_version": policy.get("policy_version"),
        **SYSTEM_TARGET_SAFETY,
    }


def _consistency_payload(binding: Mapping[str, Any]) -> dict[str, Any]:
    return {
        **_bundle_json(binding, "limited_consistency_manifest.json"),
        "rolling_consistency_summary": _bundle_json(binding, "rolling_consistency_summary.json"),
        "regime_consistency_summary": _bundle_json(binding, "regime_consistency_summary.json"),
        "stability_consistency_summary": _bundle_json(
            binding, "stability_consistency_summary.json"
        ),
    }


def _rolling_payload(binding: Mapping[str, Any]) -> dict[str, Any]:
    return {
        **_bundle_json(binding, "rolling_eval_manifest.json"),
        "rolling_window_inventory": _bundle_json(binding, "rolling_window_inventory.json"),
        "rolling_method_metrics": _bundle_jsonl(binding, "rolling_method_metrics.jsonl"),
        "rolling_rank_stability": _bundle_json(binding, "rolling_rank_stability.json"),
    }


def _impact_payload(binding: Mapping[str, Any]) -> dict[str, Any]:
    return {
        **_bundle_json(binding, "data_warning_impact_manifest.json"),
        "data_warning_inventory": _bundle_json(binding, "data_warning_inventory.json"),
        "affected_metrics": _bundle_json(binding, "affected_metrics.json"),
        "recommendation_sensitivity_to_warnings": _bundle_json(
            binding, "recommendation_sensitivity_to_warnings.json"
        ),
    }


def _instability_payload(binding: Mapping[str, Any]) -> dict[str, Any]:
    return {
        **_bundle_json(binding, "limited_instability_manifest.json"),
        "limited_instability_input_snapshot": _bundle_json(
            binding, "limited_instability_input_snapshot.json"
        ),
        "unstable_window_inventory": _bundle_jsonl(binding, "unstable_window_inventory.jsonl"),
        "instability_reason_summary": _bundle_json(binding, "instability_reason_summary.json"),
        "rolling_failure_pattern": _bundle_json(binding, "rolling_failure_pattern.json"),
    }


def _risk_payload(binding: Mapping[str, Any]) -> dict[str, Any]:
    return {
        **_bundle_json(binding, "limited_risk_attribution_manifest.json"),
        "limited_risk_attribution_input_snapshot": _bundle_json(
            binding, "limited_risk_attribution_input_snapshot.json"
        ),
        "return_contribution_by_symbol": _bundle_json(
            binding, "return_contribution_by_symbol.json"
        ),
        "drawdown_contribution_by_symbol": _bundle_json(
            binding, "drawdown_contribution_by_symbol.json"
        ),
        "exposure_shift_attribution": _bundle_json(binding, "exposure_shift_attribution.json"),
        "risk_worsening_events": _bundle_jsonl(binding, "risk_worsening_events.jsonl"),
    }


def _repair_payload(binding: Mapping[str, Any]) -> dict[str, Any]:
    return {
        **_bundle_json(binding, "data_warning_repair_plan_manifest.json"),
        "data_warning_repair_plan_input_snapshot": _bundle_json(
            binding, "data_warning_repair_plan_input_snapshot.json"
        ),
        "warning_repair_actions": _bundle_jsonl(binding, "warning_repair_actions.jsonl"),
        "warning_blocking_matrix": _bundle_json(binding, "warning_blocking_matrix.json"),
    }


def _alternative_payload(binding: Mapping[str, Any]) -> dict[str, Any]:
    return {
        **_bundle_json(binding, "alternative_method_review_manifest.json"),
        "alternative_method_review_input_snapshot": _bundle_json(
            binding, "alternative_method_review_input_snapshot.json"
        ),
        "alternative_method_candidates": _bundle_json(
            binding, "alternative_method_candidates.json"
        ),
        "alternative_method_scorecard": _bundle_json(binding, "alternative_method_scorecard.json"),
    }


def _instability_views(
    snapshot: Mapping[str, Any], *, instability_id: str, output_dir: Path
) -> tuple[dict[str, bytes], dict[str, Any]]:
    backfill_source = _mapping(snapshot.get("backfill_source"))
    consistency_source = _mapping(snapshot.get("consistency_source"))
    rolling_source = _mapping(snapshot.get("rolling_source"))
    backfill = _backfill_payload(backfill_source)
    consistency = _consistency_payload(consistency_source)
    rolling = _rolling_payload(rolling_source)
    policy = _mapping(snapshot.get("policy"))
    config = _mapping(
        _mapping(
            _bundle_json(backfill_source, "paper_shadow_backfill_input_snapshot.json").get(
                "config_binding"
            )
        ).get("payload")
    )
    states = _records(backfill.get("backfill_method_states"))
    inventory = _unstable_window_inventory(
        rolling=_records(rolling.get("rolling_method_metrics")),
        states=states,
        config=config,
        policy=policy,
    )
    summary = _instability_summary(consistency, inventory, policy)
    pattern = _failure_pattern(inventory, policy)
    backfill_manifest = _bundle_json(backfill_source, "paper_shadow_backfill_manifest.json")
    manifest = {
        "schema_version": 2,
        "report_type": "etf_dynamic_v3_limited_instability_manifest",
        "instability_id": instability_id,
        "backfill_id": backfill_source.get("artifact_id"),
        "consistency_id": consistency_source.get("artifact_id"),
        "rolling_eval_id": rolling_source.get("artifact_id"),
        "generated_at": snapshot["generated_at"],
        "status": "PASS",
        "market_regime": backfill_manifest.get("market_regime"),
        "date_start": backfill_manifest.get("date_start"),
        "date_end": backfill_manifest.get("date_end"),
        "data_quality_status": backfill_manifest.get("data_quality_status"),
        "policy_version": policy.get("policy_version"),
        "input_snapshot_schema": LIMITED_INSTABILITY_SNAPSHOT_SCHEMA,
        "limited_instability_input_snapshot_path": str(
            output_dir / "limited_instability_input_snapshot.json"
        ),
        "limited_instability_manifest_path": str(output_dir / "limited_instability_manifest.json"),
        "unstable_window_inventory_path": str(output_dir / "unstable_window_inventory.jsonl"),
        "instability_reason_summary_path": str(output_dir / "instability_reason_summary.json"),
        "rolling_failure_pattern_path": str(output_dir / "rolling_failure_pattern.json"),
        "limited_instability_report_path": str(output_dir / "limited_instability_report.md"),
        "exact_same_backfill_lineage": True,
        "missing_metrics_remain_null": True,
        **SYSTEM_TARGET_SAFETY,
    }
    views = {
        "limited_instability_input_snapshot.json": _json_bytes(snapshot),
        "limited_instability_manifest.json": _json_bytes(manifest),
        "unstable_window_inventory.jsonl": _jsonl_bytes(inventory),
        "instability_reason_summary.json": _json_bytes(summary),
        "rolling_failure_pattern.json": _json_bytes(pattern),
        "limited_instability_report.md": target_core._text_bytes(
            legacy.render_limited_instability_report(manifest, inventory, summary, pattern)
        ),
    }
    return views, {
        "manifest": manifest,
        "unstable_window_inventory": inventory,
        "instability_reason_summary": summary,
        "rolling_failure_pattern": pattern,
    }


def _validate_instability_snapshot(snapshot: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    try:
        _require(
            snapshot.get("schema_version") == LIMITED_INSTABILITY_SNAPSHOT_SCHEMA,
            "instability snapshot schema invalid",
        )
        backfill = _mapping(snapshot.get("backfill_source"))
        consistency = _mapping(snapshot.get("consistency_source"))
        rolling = _mapping(snapshot.get("rolling_source"))
        for binding in (backfill, consistency, rolling):
            errors.extend(_validate_source_binding(binding))
        backfill_id = backfill.get("artifact_id")
        consistency_manifest = _bundle_json(consistency, "limited_consistency_manifest.json")
        rolling_manifest = _bundle_json(rolling, "rolling_eval_manifest.json")
        _require(
            consistency_manifest.get("backfill_id") == backfill_id
            and rolling_manifest.get("backfill_id") == backfill_id,
            "instability cross-backfill lineage",
        )
        _require(
            consistency_manifest.get("rolling_eval_id") == rolling.get("artifact_id"),
            "instability consistency-to-rolling lineage mismatch",
        )
        generated = target_core._datetime(
            snapshot.get("generated_at"), field="instability generated_at"
        )
        _require(
            _manifest_generated(backfill, "paper_shadow_backfill_manifest.json")
            <= _manifest_generated(rolling, "rolling_eval_manifest.json")
            <= _manifest_generated(consistency, "limited_consistency_manifest.json")
            <= generated,
            "instability chronology invalid",
        )
        _require(
            _mapping(snapshot.get("policy")) == _refinement_policy(backfill),
            "instability policy binding drift",
        )
    except Exception as exc:  # noqa: BLE001
        errors.append(str(exc))
    return errors


def run_limited_instability_diagnosis(
    *,
    backfill_id: str,
    consistency_id: str,
    backfill_dir: Path = DEFAULT_PAPER_SHADOW_BACKFILL_DIR,
    consistency_dir: Path = DEFAULT_LIMITED_CONSISTENCY_DIR,
    rolling_eval_dir: Path = DEFAULT_PAPER_SHADOW_ROLLING_EVAL_DIR,
    output_dir: Path = DEFAULT_LIMITED_INSTABILITY_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = _generated_at(generated_at)
    backfill = _backfill_binding(backfill_id, backfill_dir)
    consistency = _consistency_binding(consistency_id, consistency_dir)
    consistency_manifest = _bundle_json(consistency, "limited_consistency_manifest.json")
    rolling_eval_id = _text(consistency_manifest.get("rolling_eval_id"))
    _require(bool(rolling_eval_id), "consistency rolling_eval_id required")
    rolling = _rolling_binding(rolling_eval_id, rolling_eval_dir)
    snapshot = {
        "schema_version": LIMITED_INSTABILITY_SNAPSHOT_SCHEMA,
        "generated_at": generated.isoformat(),
        "backfill_source": backfill,
        "consistency_source": consistency,
        "rolling_source": rolling,
        "policy": _refinement_policy(backfill),
        "missing_metrics_remain_null": True,
        **SYSTEM_TARGET_SAFETY,
    }
    errors = _validate_instability_snapshot(snapshot)
    _require(not errors, "; ".join(errors))
    artifact_id = _stable_id(
        "limited-instability-v2", backfill_id, consistency_id, generated.isoformat()
    )
    root = _unique_dir(output_dir / artifact_id)
    views, payload = _instability_views(snapshot, instability_id=root.name, output_dir=root)
    _write(root, views, "latest_limited_instability", "limited_instability_manifest.json")
    return {"instability_id": root.name, "instability_dir": root, **payload}


def limited_instability_report_payload(
    *,
    instability_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_LIMITED_INSTABILITY_DIR,
) -> dict[str, Any]:
    root = _artifact_dir_from_latest(
        output_dir=output_dir,
        artifact_id=None if latest else instability_id,
        pointer_name="latest_limited_instability",
    )
    return {
        **_read_json(root / "limited_instability_manifest.json"),
        **_report_payload(
            root,
            {
                "limited_instability_input_snapshot": "limited_instability_input_snapshot.json",
                "unstable_window_inventory": "unstable_window_inventory.jsonl",
                "instability_reason_summary": "instability_reason_summary.json",
                "rolling_failure_pattern": "rolling_failure_pattern.json",
            },
        ),
        "instability_dir": str(root),
    }


def validate_limited_instability_artifact(
    *, instability_id: str, output_dir: Path = DEFAULT_LIMITED_INSTABILITY_DIR
) -> dict[str, Any]:
    root = output_dir / instability_id
    checks: list[dict[str, Any]] = []
    try:
        snapshot = _read_json(root / "limited_instability_input_snapshot.json")
        errors = _validate_instability_snapshot(snapshot)
        views, payload = _instability_views(
            snapshot, instability_id=instability_id, output_dir=root
        )
        drift = _view_errors(root, views)
        summary = payload["instability_reason_summary"]
        inventory = payload["unstable_window_inventory"]
        checks.extend(
            [
                _check("snapshot_live_sources_policy_lineage", not errors, "; ".join(errors)),
                _check("content_derived_views", not drift, ",".join(drift)),
                _check(
                    "unstable_window_count_matches",
                    summary.get("unstable_window_count") == len(inventory),
                    str(len(inventory)),
                ),
                _check(
                    "broker_forbidden",
                    legacy._payload_safe(payload["manifest"], summary, *inventory),
                    "",
                ),
            ]
        )
    except Exception as exc:  # noqa: BLE001
        checks.append(_check("limited_instability_rebuild", False, str(exc)))
    return _validation_payload(
        "etf_dynamic_v3_limited_instability_validation",
        instability_id,
        checks,
        artifact_id_key="instability_id",
    )


def _risk_views(
    snapshot: Mapping[str, Any], *, risk_attribution_id: str, output_dir: Path
) -> tuple[dict[str, bytes], dict[str, Any]]:
    backfill_source = _mapping(snapshot.get("backfill_source"))
    backfill = _backfill_payload(backfill_source)
    policy = _mapping(snapshot.get("policy"))
    states = _records(backfill.get("backfill_method_states"))
    returns = _returns_from_backfill(backfill_source)
    return_contribution = _return_contribution(states, returns, policy)
    drawdown_contribution = _drawdown_contribution(states, returns, policy)
    exposure = _exposure_attribution(states, policy)
    events = _risk_events(states, policy)
    backfill_manifest = _bundle_json(backfill_source, "paper_shadow_backfill_manifest.json")
    backfill_snapshot = _bundle_json(backfill_source, "paper_shadow_backfill_input_snapshot.json")
    quality = _mapping(backfill_snapshot.get("data_quality"))
    manifest = {
        "schema_version": 2,
        "report_type": "etf_dynamic_v3_limited_risk_attribution_manifest",
        "risk_attribution_id": risk_attribution_id,
        "attribution_id": risk_attribution_id,
        "backfill_id": backfill_source.get("artifact_id"),
        "generated_at": snapshot["generated_at"],
        "status": "PASS",
        "market_regime": backfill_manifest.get("market_regime"),
        "date_start": backfill_manifest.get("date_start"),
        "date_end": backfill_manifest.get("date_end"),
        "data_quality_status": quality.get("status"),
        "source_backfill_data_quality_status": backfill_manifest.get("data_quality_status"),
        "data_quality_checked_at": quality.get("checked_at"),
        "data_quality_report_visible": True,
        "cache_commitments_visible": bool(backfill_snapshot.get("cache_bindings")),
        "policy_version": policy.get("policy_version"),
        "input_snapshot_schema": LIMITED_RISK_ATTRIBUTION_SNAPSHOT_SCHEMA,
        "limited_risk_attribution_input_snapshot_path": str(
            output_dir / "limited_risk_attribution_input_snapshot.json"
        ),
        "limited_risk_attribution_manifest_path": str(
            output_dir / "limited_risk_attribution_manifest.json"
        ),
        "return_contribution_by_symbol_path": str(
            output_dir / "return_contribution_by_symbol.json"
        ),
        "drawdown_contribution_by_symbol_path": str(
            output_dir / "drawdown_contribution_by_symbol.json"
        ),
        "exposure_shift_attribution_path": str(output_dir / "exposure_shift_attribution.json"),
        "risk_worsening_events_path": str(output_dir / "risk_worsening_events.jsonl"),
        "limited_risk_attribution_report_path": str(
            output_dir / "limited_risk_attribution_report.md"
        ),
        "bounded_price_rows_used": True,
        "missing_metrics_remain_null": True,
        **SYSTEM_TARGET_SAFETY,
    }
    views = {
        "limited_risk_attribution_input_snapshot.json": _json_bytes(snapshot),
        "limited_risk_attribution_manifest.json": _json_bytes(manifest),
        "return_contribution_by_symbol.json": _json_bytes(return_contribution),
        "drawdown_contribution_by_symbol.json": _json_bytes(drawdown_contribution),
        "exposure_shift_attribution.json": _json_bytes(exposure),
        "risk_worsening_events.jsonl": _jsonl_bytes(events),
        "limited_risk_attribution_report.md": target_core._text_bytes(
            legacy.render_limited_risk_attribution_report(
                manifest, return_contribution, drawdown_contribution, exposure, events
            )
        ),
    }
    return views, {
        "manifest": manifest,
        "return_contribution_by_symbol": return_contribution,
        "drawdown_contribution_by_symbol": drawdown_contribution,
        "exposure_shift_attribution": exposure,
        "risk_worsening_events": events,
    }


def _validate_risk_snapshot(snapshot: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    try:
        _require(
            snapshot.get("schema_version") == LIMITED_RISK_ATTRIBUTION_SNAPSHOT_SCHEMA,
            "risk attribution snapshot schema invalid",
        )
        backfill = _mapping(snapshot.get("backfill_source"))
        errors.extend(_validate_source_binding(backfill))
        generated = target_core._datetime(
            snapshot.get("generated_at"), field="risk attribution generated_at"
        )
        _require(
            _manifest_generated(backfill, "paper_shadow_backfill_manifest.json") <= generated,
            "risk attribution chronology invalid",
        )
        _require(
            _mapping(snapshot.get("policy")) == _refinement_policy(backfill),
            "risk attribution policy binding drift",
        )
        backfill_snapshot = _bundle_json(backfill, "paper_shadow_backfill_input_snapshot.json")
        _require(
            bool(backfill_snapshot.get("cache_bindings"))
            and _mapping(backfill_snapshot.get("data_quality")).get("status")
            in {"PASS", "PASS_WITH_WARNINGS"},
            "risk attribution requires visible passed DQ/cache commitments",
        )
        _returns_from_backfill(backfill)
    except Exception as exc:  # noqa: BLE001
        errors.append(str(exc))
    return errors


def run_limited_risk_attribution(
    *,
    backfill_id: str,
    backfill_dir: Path = DEFAULT_PAPER_SHADOW_BACKFILL_DIR,
    output_dir: Path = DEFAULT_LIMITED_RISK_ATTRIBUTION_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = _generated_at(generated_at)
    backfill = _backfill_binding(backfill_id, backfill_dir)
    snapshot = {
        "schema_version": LIMITED_RISK_ATTRIBUTION_SNAPSHOT_SCHEMA,
        "generated_at": generated.isoformat(),
        "backfill_source": backfill,
        "policy": _refinement_policy(backfill),
        "bounded_price_rows_used": True,
        "missing_metrics_remain_null": True,
        **SYSTEM_TARGET_SAFETY,
    }
    errors = _validate_risk_snapshot(snapshot)
    _require(not errors, "; ".join(errors))
    artifact_id = _stable_id("limited-risk-attribution-v2", backfill_id, generated.isoformat())
    root = _unique_dir(output_dir / artifact_id)
    views, payload = _risk_views(snapshot, risk_attribution_id=root.name, output_dir=root)
    _write(
        root,
        views,
        "latest_limited_risk_attribution",
        "limited_risk_attribution_manifest.json",
    )
    return {
        "risk_attribution_id": root.name,
        "attribution_id": root.name,
        "risk_attribution_dir": root,
        "attribution_dir": root,
        **payload,
    }


def limited_risk_attribution_report_payload(
    *,
    attribution_id: str | None = None,
    risk_attribution_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_LIMITED_RISK_ATTRIBUTION_DIR,
) -> dict[str, Any]:
    resolved_id = risk_attribution_id or attribution_id
    root = _artifact_dir_from_latest(
        output_dir=output_dir,
        artifact_id=None if latest else resolved_id,
        pointer_name="latest_limited_risk_attribution",
    )
    return {
        **_read_json(root / "limited_risk_attribution_manifest.json"),
        **_report_payload(
            root,
            {
                "limited_risk_attribution_input_snapshot": (
                    "limited_risk_attribution_input_snapshot.json"
                ),
                "return_contribution_by_symbol": "return_contribution_by_symbol.json",
                "drawdown_contribution_by_symbol": "drawdown_contribution_by_symbol.json",
                "exposure_shift_attribution": "exposure_shift_attribution.json",
                "risk_worsening_events": "risk_worsening_events.jsonl",
            },
        ),
        "risk_attribution_dir": str(root),
        "attribution_dir": str(root),
    }


def validate_limited_risk_attribution_artifact(
    *,
    attribution_id: str | None = None,
    risk_attribution_id: str | None = None,
    output_dir: Path = DEFAULT_LIMITED_RISK_ATTRIBUTION_DIR,
) -> dict[str, Any]:
    resolved_id = risk_attribution_id or attribution_id
    if not resolved_id:
        raise DynamicV3SystemTargetRefinementError("risk attribution id is required")
    root = output_dir / resolved_id
    checks: list[dict[str, Any]] = []
    try:
        snapshot = _read_json(root / "limited_risk_attribution_input_snapshot.json")
        errors = _validate_risk_snapshot(snapshot)
        views, payload = _risk_views(snapshot, risk_attribution_id=resolved_id, output_dir=root)
        drift = _view_errors(root, views)
        checks.extend(
            [
                _check("snapshot_live_backfill_policy_dq_cache", not errors, "; ".join(errors)),
                _check("content_derived_views", not drift, ",".join(drift)),
                _check(
                    "target_method_limited",
                    all(
                        payload[name].get("target_method")
                        == _mapping(snapshot.get("policy")).get("candidate_method")
                        for name in (
                            "return_contribution_by_symbol",
                            "drawdown_contribution_by_symbol",
                            "exposure_shift_attribution",
                        )
                    ),
                    "",
                ),
                _check(
                    "broker_forbidden",
                    legacy._payload_safe(
                        payload["manifest"],
                        payload["return_contribution_by_symbol"],
                        payload["drawdown_contribution_by_symbol"],
                        payload["exposure_shift_attribution"],
                        *payload["risk_worsening_events"],
                    ),
                    "",
                ),
            ]
        )
    except Exception as exc:  # noqa: BLE001
        checks.append(_check("limited_risk_attribution_rebuild", False, str(exc)))
    return _validation_payload(
        "etf_dynamic_v3_limited_risk_attribution_validation",
        resolved_id,
        checks,
        artifact_id_key="risk_attribution_id",
    )


def _repair_views(
    snapshot: Mapping[str, Any], *, repair_plan_id: str, output_dir: Path
) -> tuple[dict[str, bytes], dict[str, Any]]:
    impact_source = _mapping(snapshot.get("impact_source"))
    impact = _impact_payload(impact_source)
    impact_manifest = _bundle_json(impact_source, "data_warning_impact_manifest.json")
    actions = _warning_actions(impact)
    matrix = _warning_matrix(impact, actions)
    manifest = {
        "schema_version": 2,
        "report_type": "etf_dynamic_v3_data_warning_repair_plan_manifest",
        "repair_plan_id": repair_plan_id,
        "impact_id": impact_source.get("artifact_id"),
        "backfill_id": impact_manifest.get("backfill_id"),
        "selection_review_id": impact_manifest.get("selection_review_id"),
        "generated_at": snapshot["generated_at"],
        "status": "PASS",
        "market_regime": impact_manifest.get("market_regime"),
        "date_start": impact_manifest.get("date_start"),
        "date_end": impact_manifest.get("date_end"),
        "data_quality_status": impact_manifest.get("data_quality_status"),
        "input_snapshot_schema": DATA_WARNING_REPAIR_SNAPSHOT_SCHEMA,
        "data_warning_repair_plan_input_snapshot_path": str(
            output_dir / "data_warning_repair_plan_input_snapshot.json"
        ),
        "data_warning_repair_plan_manifest_path": str(
            output_dir / "data_warning_repair_plan_manifest.json"
        ),
        "warning_repair_actions_path": str(output_dir / "warning_repair_actions.jsonl"),
        "warning_blocking_matrix_path": str(output_dir / "warning_blocking_matrix.json"),
        "data_warning_repair_plan_report_path": str(
            output_dir / "data_warning_repair_plan_report.md"
        ),
        "auto_repair_executed": False,
        "unknown_warning_detail_preserved": True,
        **SYSTEM_TARGET_SAFETY,
    }
    views = {
        "data_warning_repair_plan_input_snapshot.json": _json_bytes(snapshot),
        "data_warning_repair_plan_manifest.json": _json_bytes(manifest),
        "warning_repair_actions.jsonl": _jsonl_bytes(actions),
        "warning_blocking_matrix.json": _json_bytes(matrix),
        "data_warning_repair_plan_report.md": target_core._text_bytes(
            legacy.render_data_warning_repair_plan_report(manifest, actions, matrix)
        ),
    }
    return views, {
        "manifest": manifest,
        "warning_repair_actions": actions,
        "warning_blocking_matrix": matrix,
    }


def _validate_repair_snapshot(snapshot: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    try:
        _require(
            snapshot.get("schema_version") == DATA_WARNING_REPAIR_SNAPSHOT_SCHEMA,
            "repair snapshot schema invalid",
        )
        impact = _mapping(snapshot.get("impact_source"))
        errors.extend(_validate_source_binding(impact))
        generated = target_core._datetime(snapshot.get("generated_at"), field="repair generated_at")
        _require(
            _manifest_generated(impact, "data_warning_impact_manifest.json") <= generated,
            "repair chronology invalid",
        )
        impact_manifest = _bundle_json(impact, "data_warning_impact_manifest.json")
        _require(
            bool(impact_manifest.get("backfill_id"))
            and bool(impact_manifest.get("selection_review_id")),
            "repair source lineage incomplete",
        )
    except Exception as exc:  # noqa: BLE001
        errors.append(str(exc))
    return errors


def run_data_warning_repair_plan(
    *,
    impact_id: str,
    data_warning_impact_dir: Path = DEFAULT_DATA_WARNING_IMPACT_DIR,
    output_dir: Path = DEFAULT_DATA_WARNING_REPAIR_PLAN_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = _generated_at(generated_at)
    impact = _warning_binding(impact_id, data_warning_impact_dir)
    snapshot = {
        "schema_version": DATA_WARNING_REPAIR_SNAPSHOT_SCHEMA,
        "generated_at": generated.isoformat(),
        "impact_source": impact,
        "unknown_warning_detail_preserved": True,
        "auto_repair_executed": False,
        **SYSTEM_TARGET_SAFETY,
    }
    errors = _validate_repair_snapshot(snapshot)
    _require(not errors, "; ".join(errors))
    artifact_id = _stable_id("data-warning-repair-plan-v2", impact_id, generated.isoformat())
    root = _unique_dir(output_dir / artifact_id)
    views, payload = _repair_views(snapshot, repair_plan_id=root.name, output_dir=root)
    _write(
        root,
        views,
        "latest_data_warning_repair_plan",
        "data_warning_repair_plan_manifest.json",
    )
    return {"repair_plan_id": root.name, "repair_plan_dir": root, **payload}


def data_warning_repair_plan_report_payload(
    *,
    repair_plan_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_DATA_WARNING_REPAIR_PLAN_DIR,
) -> dict[str, Any]:
    root = _artifact_dir_from_latest(
        output_dir=output_dir,
        artifact_id=None if latest else repair_plan_id,
        pointer_name="latest_data_warning_repair_plan",
    )
    return {
        **_read_json(root / "data_warning_repair_plan_manifest.json"),
        **_report_payload(
            root,
            {
                "data_warning_repair_plan_input_snapshot": (
                    "data_warning_repair_plan_input_snapshot.json"
                ),
                "warning_repair_actions": "warning_repair_actions.jsonl",
                "warning_blocking_matrix": "warning_blocking_matrix.json",
            },
        ),
        "repair_plan_dir": str(root),
    }


def validate_data_warning_repair_plan_artifact(
    *, repair_plan_id: str, output_dir: Path = DEFAULT_DATA_WARNING_REPAIR_PLAN_DIR
) -> dict[str, Any]:
    root = output_dir / repair_plan_id
    checks: list[dict[str, Any]] = []
    try:
        snapshot = _read_json(root / "data_warning_repair_plan_input_snapshot.json")
        errors = _validate_repair_snapshot(snapshot)
        views, payload = _repair_views(snapshot, repair_plan_id=repair_plan_id, output_dir=root)
        drift = _view_errors(root, views)
        actions = payload["warning_repair_actions"]
        matrix = payload["warning_blocking_matrix"]
        checks.extend(
            [
                _check("snapshot_live_impact_lineage", not errors, "; ".join(errors)),
                _check("content_derived_views", not drift, ",".join(drift)),
                _check(
                    "no_auto_repair",
                    payload["manifest"].get("auto_repair_executed") is False
                    and all(row.get("auto_repair_allowed") is False for row in actions),
                    "",
                ),
                _check(
                    "unknown_preserved",
                    matrix.get("hardening_allowed_after_repair") in {"UNKNOWN", "YES", "NO"},
                    _text(matrix.get("hardening_allowed_after_repair")),
                ),
                _check(
                    "broker_forbidden",
                    legacy._payload_safe(payload["manifest"], matrix, *actions),
                    "",
                ),
            ]
        )
    except Exception as exc:  # noqa: BLE001
        checks.append(_check("data_warning_repair_plan_rebuild", False, str(exc)))
    return _validation_payload(
        "etf_dynamic_v3_data_warning_repair_plan_validation",
        repair_plan_id,
        checks,
        artifact_id_key="repair_plan_id",
    )


def _alternative_views(
    snapshot: Mapping[str, Any], *, alt_review_id: str, output_dir: Path
) -> tuple[dict[str, bytes], dict[str, Any]]:
    backfill_source = _mapping(snapshot.get("backfill_source"))
    risk_source = _mapping(snapshot.get("risk_source"))
    instability_source = _mapping(snapshot.get("instability_source"))
    backfill = _backfill_payload(backfill_source)
    risk = _risk_payload(risk_source)
    instability = _instability_payload(instability_source)
    policy = _mapping(snapshot.get("policy"))
    candidates = _alternative_candidates(policy)
    scorecard = _alternative_scorecard(backfill, risk, instability, candidates, policy)
    backfill_manifest = _bundle_json(backfill_source, "paper_shadow_backfill_manifest.json")
    manifest = {
        "schema_version": 2,
        "report_type": "etf_dynamic_v3_alternative_method_review_manifest",
        "alt_review_id": alt_review_id,
        "backfill_id": backfill_source.get("artifact_id"),
        "risk_attribution_id": risk_source.get("artifact_id"),
        "instability_id": instability_source.get("artifact_id"),
        "generated_at": snapshot["generated_at"],
        "status": "PASS",
        "market_regime": backfill_manifest.get("market_regime"),
        "date_start": backfill_manifest.get("date_start"),
        "date_end": backfill_manifest.get("date_end"),
        "data_quality_status": backfill_manifest.get("data_quality_status"),
        "policy_version": policy.get("policy_version"),
        "input_snapshot_schema": ALTERNATIVE_METHOD_REVIEW_SNAPSHOT_SCHEMA,
        "alternative_method_review_input_snapshot_path": str(
            output_dir / "alternative_method_review_input_snapshot.json"
        ),
        "alternative_method_review_manifest_path": str(
            output_dir / "alternative_method_review_manifest.json"
        ),
        "alternative_method_candidates_path": str(
            output_dir / "alternative_method_candidates.json"
        ),
        "alternative_method_scorecard_path": str(output_dir / "alternative_method_scorecard.json"),
        "alternative_method_review_report_path": str(
            output_dir / "alternative_method_review_report.md"
        ),
        "exact_same_backfill_lineage": True,
        "conceptual_metrics_remain_null": True,
        **SYSTEM_TARGET_SAFETY,
    }
    views = {
        "alternative_method_review_input_snapshot.json": _json_bytes(snapshot),
        "alternative_method_review_manifest.json": _json_bytes(manifest),
        "alternative_method_candidates.json": _json_bytes(candidates),
        "alternative_method_scorecard.json": _json_bytes(scorecard),
        "alternative_method_review_report.md": target_core._text_bytes(
            legacy.render_alternative_method_review_report(manifest, candidates, scorecard)
        ),
    }
    return views, {
        "manifest": manifest,
        "alternative_method_candidates": candidates,
        "alternative_method_scorecard": scorecard,
    }


def _validate_alternative_snapshot(snapshot: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    try:
        _require(
            snapshot.get("schema_version") == ALTERNATIVE_METHOD_REVIEW_SNAPSHOT_SCHEMA,
            "alternative review snapshot schema invalid",
        )
        backfill = _mapping(snapshot.get("backfill_source"))
        risk = _mapping(snapshot.get("risk_source"))
        instability = _mapping(snapshot.get("instability_source"))
        for binding in (backfill, risk, instability):
            errors.extend(_validate_source_binding(binding))
        backfill_id = backfill.get("artifact_id")
        risk_manifest = _bundle_json(risk, "limited_risk_attribution_manifest.json")
        instability_manifest = _bundle_json(instability, "limited_instability_manifest.json")
        _require(
            risk_manifest.get("backfill_id") == backfill_id
            and instability_manifest.get("backfill_id") == backfill_id,
            "alternative review cross-backfill lineage",
        )
        generated = target_core._datetime(
            snapshot.get("generated_at"), field="alternative review generated_at"
        )
        for binding, name in (
            (backfill, "paper_shadow_backfill_manifest.json"),
            (risk, "limited_risk_attribution_manifest.json"),
            (instability, "limited_instability_manifest.json"),
        ):
            _require(
                _manifest_generated(binding, name) <= generated,
                "alternative review chronology invalid",
            )
        _require(
            _mapping(snapshot.get("policy")) == _refinement_policy(backfill),
            "alternative review policy binding drift",
        )
    except Exception as exc:  # noqa: BLE001
        errors.append(str(exc))
    return errors


def run_alternative_method_review(
    *,
    backfill_id: str,
    risk_attribution_id: str,
    instability_id: str,
    backfill_dir: Path = DEFAULT_PAPER_SHADOW_BACKFILL_DIR,
    risk_attribution_dir: Path = DEFAULT_LIMITED_RISK_ATTRIBUTION_DIR,
    instability_dir: Path = DEFAULT_LIMITED_INSTABILITY_DIR,
    output_dir: Path = DEFAULT_ALTERNATIVE_METHOD_REVIEW_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = _generated_at(generated_at)
    backfill = _backfill_binding(backfill_id, backfill_dir)
    risk = _risk_binding(risk_attribution_id, risk_attribution_dir)
    instability = _instability_binding(instability_id, instability_dir)
    snapshot = {
        "schema_version": ALTERNATIVE_METHOD_REVIEW_SNAPSHOT_SCHEMA,
        "generated_at": generated.isoformat(),
        "backfill_source": backfill,
        "risk_source": risk,
        "instability_source": instability,
        "policy": _refinement_policy(backfill),
        "conceptual_metrics_remain_null": True,
        **SYSTEM_TARGET_SAFETY,
    }
    errors = _validate_alternative_snapshot(snapshot)
    _require(not errors, "; ".join(errors))
    artifact_id = _stable_id(
        "alternative-method-review-v2",
        backfill_id,
        risk_attribution_id,
        instability_id,
        generated.isoformat(),
    )
    root = _unique_dir(output_dir / artifact_id)
    views, payload = _alternative_views(snapshot, alt_review_id=root.name, output_dir=root)
    _write(
        root,
        views,
        "latest_alternative_method_review",
        "alternative_method_review_manifest.json",
    )
    return {"alt_review_id": root.name, "alt_review_dir": root, **payload}


def alternative_method_review_report_payload(
    *,
    alt_review_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_ALTERNATIVE_METHOD_REVIEW_DIR,
) -> dict[str, Any]:
    root = _artifact_dir_from_latest(
        output_dir=output_dir,
        artifact_id=None if latest else alt_review_id,
        pointer_name="latest_alternative_method_review",
    )
    return {
        **_read_json(root / "alternative_method_review_manifest.json"),
        **_report_payload(
            root,
            {
                "alternative_method_review_input_snapshot": (
                    "alternative_method_review_input_snapshot.json"
                ),
                "alternative_method_candidates": "alternative_method_candidates.json",
                "alternative_method_scorecard": "alternative_method_scorecard.json",
            },
        ),
        "alt_review_dir": str(root),
    }


def validate_alternative_method_review_artifact(
    *, alt_review_id: str, output_dir: Path = DEFAULT_ALTERNATIVE_METHOD_REVIEW_DIR
) -> dict[str, Any]:
    root = output_dir / alt_review_id
    checks: list[dict[str, Any]] = []
    try:
        snapshot = _read_json(root / "alternative_method_review_input_snapshot.json")
        errors = _validate_alternative_snapshot(snapshot)
        views, payload = _alternative_views(snapshot, alt_review_id=alt_review_id, output_dir=root)
        drift = _view_errors(root, views)
        candidates = payload["alternative_method_candidates"]
        scorecard = payload["alternative_method_scorecard"]
        methods = {_text(row.get("method")) for row in _records(candidates.get("candidates"))}
        policy_primary = set(
            _text(item)
            for item in _mapping(
                _mapping(_mapping(snapshot.get("policy")).get("method_refinement")).get(
                    "alternative_review"
                )
            ).get("primary_research_candidates", [])
        )
        checks.extend(
            [
                _check("snapshot_live_sources_policy_lineage", not errors, "; ".join(errors)),
                _check("content_derived_views", not drift, ",".join(drift)),
                _check("primary_candidates_present", policy_primary <= methods, ",".join(methods)),
                _check(
                    "conceptual_metrics_null",
                    all(
                        _mapping(row.get("metrics")).get("total_return") is None
                        for row in _records(scorecard.get("methods"))
                        if row.get("implementation_status") == "NOT_IMPLEMENTED"
                    ),
                    "",
                ),
                _check(
                    "broker_forbidden",
                    legacy._payload_safe(payload["manifest"], candidates, scorecard),
                    "",
                ),
            ]
        )
    except Exception as exc:  # noqa: BLE001
        checks.append(_check("alternative_method_review_rebuild", False, str(exc)))
    return _validation_payload(
        "etf_dynamic_v3_alternative_method_review_validation",
        alt_review_id,
        checks,
        artifact_id_key="alt_review_id",
    )


def _proposal_views(
    snapshot: Mapping[str, Any], *, proposal_id: str, output_dir: Path
) -> tuple[dict[str, bytes], dict[str, Any]]:
    instability_source = _mapping(snapshot.get("instability_source"))
    risk_source = _mapping(snapshot.get("risk_source"))
    repair_source = _mapping(snapshot.get("repair_source"))
    alternative_source = _mapping(snapshot.get("alternative_source"))
    instability = _instability_payload(instability_source)
    risk = _risk_payload(risk_source)
    repair = _repair_payload(repair_source)
    alternative = _alternative_payload(alternative_source)
    policy = _mapping(snapshot.get("policy"))
    decision = _refined_decision(instability, risk, repair, alternative, policy)
    decision["proposal_id"] = proposal_id
    methods = _proposed_methods(decision, policy)
    alt_manifest = _bundle_json(alternative_source, "alternative_method_review_manifest.json")
    repair_manifest = _bundle_json(repair_source, "data_warning_repair_plan_manifest.json")
    manifest = {
        "schema_version": 2,
        "report_type": "etf_dynamic_v3_refined_method_proposal_manifest",
        "proposal_id": proposal_id,
        "backfill_id": alt_manifest.get("backfill_id"),
        "selection_review_id": repair_manifest.get("selection_review_id"),
        "instability_id": instability_source.get("artifact_id"),
        "risk_attribution_id": risk_source.get("artifact_id"),
        "repair_plan_id": repair_source.get("artifact_id"),
        "alt_review_id": alternative_source.get("artifact_id"),
        "generated_at": snapshot["generated_at"],
        "status": "PASS",
        "recommended_next_step": decision.get("recommended_next_step"),
        "confidence": decision.get("confidence"),
        "policy_version": policy.get("policy_version"),
        "input_snapshot_schema": REFINED_METHOD_PROPOSAL_SNAPSHOT_SCHEMA,
        "refined_method_proposal_input_snapshot_path": str(
            output_dir / "refined_method_proposal_input_snapshot.json"
        ),
        "refined_method_proposal_manifest_path": str(
            output_dir / "refined_method_proposal_manifest.json"
        ),
        "refined_method_decision_path": str(output_dir / "refined_method_decision.json"),
        "proposed_next_methods_path": str(output_dir / "proposed_next_methods.json"),
        "owner_refined_method_checklist_path": str(
            output_dir / "owner_refined_method_checklist.md"
        ),
        "refined_method_proposal_report_path": str(
            output_dir / "refined_method_proposal_report.md"
        ),
        "reader_brief_section_path": str(output_dir / "reader_brief_section.md"),
        "exact_source_lineage": True,
        "conceptual_metrics_remain_null": True,
        **SYSTEM_TARGET_SAFETY,
    }
    views = {
        "refined_method_proposal_input_snapshot.json": _json_bytes(snapshot),
        "refined_method_proposal_manifest.json": _json_bytes(manifest),
        "refined_method_decision.json": _json_bytes(decision),
        "proposed_next_methods.json": _json_bytes(methods),
        "owner_refined_method_checklist.md": target_core._text_bytes(
            legacy.render_refined_owner_checklist(decision)
        ),
        "refined_method_proposal_report.md": target_core._text_bytes(
            legacy.render_refined_method_proposal_report(
                manifest,
                decision,
                methods,
                instability,
                risk,
                repair,
                alternative,
            )
        ),
        "reader_brief_section.md": target_core._text_bytes(
            legacy.render_refined_method_reader_brief(decision, methods)
        ),
    }
    return views, {
        "manifest": manifest,
        "refined_method_decision": decision,
        "proposed_next_methods": methods,
    }


def _validate_proposal_snapshot(snapshot: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    try:
        _require(
            snapshot.get("schema_version") == REFINED_METHOD_PROPOSAL_SNAPSHOT_SCHEMA,
            "refined proposal snapshot schema invalid",
        )
        instability = _mapping(snapshot.get("instability_source"))
        risk = _mapping(snapshot.get("risk_source"))
        repair = _mapping(snapshot.get("repair_source"))
        alternative = _mapping(snapshot.get("alternative_source"))
        for binding in (instability, risk, repair, alternative):
            errors.extend(_validate_source_binding(binding))
        instability_manifest = _bundle_json(instability, "limited_instability_manifest.json")
        risk_manifest = _bundle_json(risk, "limited_risk_attribution_manifest.json")
        repair_manifest = _bundle_json(repair, "data_warning_repair_plan_manifest.json")
        alt_manifest = _bundle_json(alternative, "alternative_method_review_manifest.json")
        backfill_ids = {
            instability_manifest.get("backfill_id"),
            risk_manifest.get("backfill_id"),
            repair_manifest.get("backfill_id"),
            alt_manifest.get("backfill_id"),
        }
        _require(len(backfill_ids) == 1 and None not in backfill_ids, "proposal cross-backfill")
        _require(
            alt_manifest.get("instability_id") == instability.get("artifact_id")
            and alt_manifest.get("risk_attribution_id") == risk.get("artifact_id"),
            "proposal alternative source lineage mismatch",
        )
        _require(
            bool(repair_manifest.get("selection_review_id")),
            "proposal repair selection lineage missing",
        )
        generated = target_core._datetime(
            snapshot.get("generated_at"), field="refined proposal generated_at"
        )
        instability_generated = _manifest_generated(
            instability, "limited_instability_manifest.json"
        )
        risk_generated = _manifest_generated(risk, "limited_risk_attribution_manifest.json")
        repair_generated = _manifest_generated(repair, "data_warning_repair_plan_manifest.json")
        alt_generated = _manifest_generated(alternative, "alternative_method_review_manifest.json")
        _require(
            instability_generated <= alt_generated <= generated
            and risk_generated <= alt_generated <= generated
            and repair_generated <= generated,
            "refined proposal chronology invalid",
        )
        alt_snapshot = _bundle_json(alternative, "alternative_method_review_input_snapshot.json")
        _require(
            _mapping(snapshot.get("policy")) == _mapping(alt_snapshot.get("policy")),
            "refined proposal policy lineage drift",
        )
    except Exception as exc:  # noqa: BLE001
        errors.append(str(exc))
    return errors


def run_refined_method_proposal(
    *,
    instability_id: str,
    risk_attribution_id: str,
    repair_plan_id: str,
    alt_review_id: str,
    instability_dir: Path = DEFAULT_LIMITED_INSTABILITY_DIR,
    risk_attribution_dir: Path = DEFAULT_LIMITED_RISK_ATTRIBUTION_DIR,
    repair_plan_dir: Path = DEFAULT_DATA_WARNING_REPAIR_PLAN_DIR,
    alt_review_dir: Path = DEFAULT_ALTERNATIVE_METHOD_REVIEW_DIR,
    output_dir: Path = DEFAULT_REFINED_METHOD_PROPOSAL_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = _generated_at(generated_at)
    instability = _instability_binding(instability_id, instability_dir)
    risk = _risk_binding(risk_attribution_id, risk_attribution_dir)
    repair = _repair_binding(repair_plan_id, repair_plan_dir)
    alternative = _alternative_binding(alt_review_id, alt_review_dir)
    alt_snapshot = _bundle_json(alternative, "alternative_method_review_input_snapshot.json")
    snapshot = {
        "schema_version": REFINED_METHOD_PROPOSAL_SNAPSHOT_SCHEMA,
        "generated_at": generated.isoformat(),
        "instability_source": instability,
        "risk_source": risk,
        "repair_source": repair,
        "alternative_source": alternative,
        "policy": _mapping(alt_snapshot.get("policy")),
        "conceptual_metrics_remain_null": True,
        **SYSTEM_TARGET_SAFETY,
    }
    errors = _validate_proposal_snapshot(snapshot)
    _require(not errors, "; ".join(errors))
    artifact_id = _stable_id(
        "refined-method-proposal-v2",
        instability_id,
        risk_attribution_id,
        repair_plan_id,
        alt_review_id,
        generated.isoformat(),
    )
    root = _unique_dir(output_dir / artifact_id)
    views, payload = _proposal_views(snapshot, proposal_id=root.name, output_dir=root)
    _write(
        root,
        views,
        "latest_refined_method_proposal",
        "refined_method_proposal_manifest.json",
    )
    return {"proposal_id": root.name, "proposal_dir": root, **payload}


def refined_method_proposal_report_payload(
    *,
    proposal_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_REFINED_METHOD_PROPOSAL_DIR,
) -> dict[str, Any]:
    root = _artifact_dir_from_latest(
        output_dir=output_dir,
        artifact_id=None if latest else proposal_id,
        pointer_name="latest_refined_method_proposal",
    )
    return {
        **_read_json(root / "refined_method_proposal_manifest.json"),
        **_report_payload(
            root,
            {
                "refined_method_proposal_input_snapshot": (
                    "refined_method_proposal_input_snapshot.json"
                ),
                "refined_method_decision": "refined_method_decision.json",
                "proposed_next_methods": "proposed_next_methods.json",
            },
        ),
        "proposal_dir": str(root),
    }


def validate_refined_method_proposal_artifact(
    *, proposal_id: str, output_dir: Path = DEFAULT_REFINED_METHOD_PROPOSAL_DIR
) -> dict[str, Any]:
    root = output_dir / proposal_id
    checks: list[dict[str, Any]] = []
    try:
        snapshot = _read_json(root / "refined_method_proposal_input_snapshot.json")
        errors = _validate_proposal_snapshot(snapshot)
        views, payload = _proposal_views(snapshot, proposal_id=proposal_id, output_dir=root)
        drift = _view_errors(root, views)
        decision = payload["refined_method_decision"]
        methods = payload["proposed_next_methods"]
        checks.extend(
            [
                _check("snapshot_live_sources_policy_lineage", not errors, "; ".join(errors)),
                _check("content_derived_views", not drift, ",".join(drift)),
                _check(
                    "recommended_next_step_valid",
                    decision.get("recommended_next_step")
                    in {
                        "IMPLEMENT_RISK_CAPPED_RESEARCH_METHOD",
                        "IMPLEMENT_REGIME_GATED_RESEARCH_METHOD",
                        "CONTINUE_OBSERVATION",
                        "REPAIR_DATA_WARNINGS_FIRST",
                        "DEFER",
                    },
                    _text(decision.get("recommended_next_step")),
                ),
                _check("next_methods_present", bool(_records(methods.get("methods"))), ""),
                _check(
                    "research_only_no_auto",
                    decision.get("auto_apply") is False
                    and decision.get("research_target_only") is True
                    and all(
                        row.get("implementation_scope") == "research_only"
                        and row.get("auto_apply") is False
                        for row in _records(methods.get("methods"))
                    ),
                    "",
                ),
                _check(
                    "broker_forbidden",
                    legacy._payload_safe(payload["manifest"], decision, methods),
                    "",
                ),
            ]
        )
    except Exception as exc:  # noqa: BLE001
        checks.append(_check("refined_method_proposal_rebuild", False, str(exc)))
    return _validation_payload(
        "etf_dynamic_v3_refined_method_proposal_validation",
        proposal_id,
        checks,
        artifact_id_key="proposal_id",
    )
