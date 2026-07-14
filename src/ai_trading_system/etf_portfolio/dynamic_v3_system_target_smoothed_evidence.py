from __future__ import annotations

import math
from collections.abc import Callable, Mapping, Sequence
from datetime import datetime
from pathlib import Path
from typing import Any

from ai_trading_system.etf_portfolio import dynamic_v3_system_target as legacy
from ai_trading_system.etf_portfolio import dynamic_v3_system_target_hardening as hardening
from ai_trading_system.etf_portfolio import dynamic_v3_system_target_history as history
from ai_trading_system.etf_portfolio import dynamic_v3_system_target_portfolio as target_core
from ai_trading_system.etf_portfolio import dynamic_v3_system_target_smoothed_method as method
from ai_trading_system.etf_portfolio.dynamic_v3_confirmation_operations import (
    _file_bytes_match,
    _json_bytes,
)
from ai_trading_system.etf_portfolio.dynamic_v3_historical_replay import (
    _check,
    _mapping,
    _read_json,
    _records,
    _stable_id,
    _text,
    _unique_dir,
    _update_latest_pointer,
)
from ai_trading_system.etf_portfolio.dynamic_v3_pressure_validation import (
    _write_views_atomic,
)

SMOOTHED_ATTRIBUTION_SNAPSHOT_SCHEMA = "smoothed_attribution_input_snapshot.v2"
SMOOTHING_BENEFIT_LAG_SNAPSHOT_SCHEMA = "smoothing_benefit_lag_input_snapshot.v2"
SMOOTHED_REGIME_SNAPSHOT_SCHEMA = "smoothed_regime_validation_input_snapshot.v2"
SMOOTHED_CONFIRMATION_SNAPSHOT_SCHEMA = "smoothed_confirmation_input_snapshot.v2"
SMOOTHED_WATCH_SNAPSHOT_SCHEMA = "smoothed_watch_input_snapshot.v2"

DEFAULT_SMOOTHED_LIMITED_CONFIG_PATH = method.DEFAULT_SMOOTHED_LIMITED_CONFIG_PATH
DEFAULT_SMOOTHED_REVIEW_DIR = method.DEFAULT_SMOOTHED_REVIEW_DIR
DEFAULT_SMOOTHED_COMPARISON_DIR = method.DEFAULT_SMOOTHED_COMPARISON_DIR
DEFAULT_SMOOTHED_BACKFILL_DIR = method.DEFAULT_SMOOTHED_BACKFILL_DIR
DEFAULT_PAPER_SHADOW_BACKFILL_DIR = history.DEFAULT_PAPER_SHADOW_BACKFILL_DIR
DEFAULT_SMOOTHED_REVIEW_ATTRIBUTION_DIR = legacy.DEFAULT_SMOOTHED_REVIEW_ATTRIBUTION_DIR
DEFAULT_SMOOTHING_BENEFIT_LAG_DIR = legacy.DEFAULT_SMOOTHING_BENEFIT_LAG_DIR
DEFAULT_SMOOTHED_REGIME_VALIDATION_DIR = legacy.DEFAULT_SMOOTHED_REGIME_VALIDATION_DIR
DEFAULT_SMOOTHED_FORWARD_CONFIRMATION_DIR = legacy.DEFAULT_SMOOTHED_FORWARD_CONFIRMATION_DIR
DEFAULT_SMOOTHED_WATCH_PACK_DIR = legacy.DEFAULT_SMOOTHED_WATCH_PACK_DIR
SMOOTHED_METHOD_TO_VARIANT = method.SMOOTHED_METHOD_TO_VARIANT
SYSTEM_TARGET_SAFETY = method.SYSTEM_TARGET_SAFETY


class DynamicV3SmoothedEvidenceError(ValueError):
    """Raised when smoothed evidence is invalid, ambiguous, or not reproducible."""


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise DynamicV3SmoothedEvidenceError(message)


def _generated_at(value: datetime | None) -> datetime:
    try:
        return target_core._generated_at(value)
    except Exception as exc:  # noqa: BLE001
        raise DynamicV3SmoothedEvidenceError(str(exc)) from exc


def _finite(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)


def _nullable_float(value: Any) -> float | None:
    return float(value) if _finite(value) else None


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


def _bundle_json(binding: Mapping[str, Any], name: str) -> dict[str, Any]:
    return hardening._bundle_json(binding, name)


def _bundle_jsonl(binding: Mapping[str, Any], name: str) -> list[dict[str, Any]]:
    return hardening._bundle_jsonl(binding, name)


def _policy_binding(path: Path) -> dict[str, Any]:
    return target_core._config_binding(path, kind="smoothed_evidence_policy")


def _evidence_policy(config: Mapping[str, Any]) -> dict[str, Any]:
    target_core._policy_metadata(
        {"policy_metadata": _mapping(config.get("evidence_policy_metadata"))},
        name="smoothed evidence",
    )
    policy = _mapping(config.get("evidence_policy"))
    integer_fields = {
        "minimum_path_observations",
        "minimum_regime_observations",
        "required_forward_events",
        "required_sideways_events",
        "required_recovery_events",
    }
    numeric_fields = {
        "benefit_reduction_floor",
        "acceptable_return_delta_floor",
        "drawdown_improvement_floor",
        "turnover_improvement_ceiling",
        "jump_improvement_ceiling",
        "forward_return_delta_floor",
        "forward_drawdown_delta_floor",
        "forward_turnover_delta_ceiling",
        "sideways_signal_churn_delta_ceiling",
        "sideways_weight_jump_delta_ceiling",
        "sideways_turnover_delta_ceiling",
    }
    required = integer_fields | numeric_fields | {"confirmation_windows"}
    _require(set(policy) == required, "smoothed evidence_policy fields must be exact")
    for field in sorted(integer_fields | numeric_fields):
        _require(_finite(policy.get(field)), f"evidence_policy.{field} must be finite")
    for field in sorted(integer_fields):
        value = float(policy[field])
        _require(value.is_integer() and value >= 1.0, f"{field} must be a positive integer")
    windows = list(policy.get("confirmation_windows") or [])
    _require(
        bool(windows)
        and len(windows) == len(set(windows))
        and all(
            _finite(value) and float(value).is_integer() and float(value) >= 1.0
            for value in windows
        ),
        "confirmation_windows must be unique positive integers",
    )
    _require(
        -1.0 <= float(policy["acceptable_return_delta_floor"]) <= 0.0,
        "acceptable_return_delta_floor must be between -1 and 0",
    )
    _require(
        -1.0 <= float(policy["forward_return_delta_floor"]) <= 0.0,
        "forward_return_delta_floor must be between -1 and 0",
    )
    return {
        **{field: float(policy[field]) for field in sorted(numeric_fields)},
        **{field: int(policy[field]) for field in sorted(integer_fields)},
        "confirmation_windows": [int(value) for value in windows],
    }


def _policy(snapshot: Mapping[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    binding = _mapping(snapshot.get("policy_binding"))
    config = _mapping(binding.get("payload"))
    return config, _evidence_policy(config)


def _validate_policy_binding(binding: Mapping[str, Any]) -> list[str]:
    errors = target_core._validate_config_binding(binding)
    try:
        _require(binding.get("kind") == "smoothed_evidence_policy", "policy binding kind invalid")
        config = _mapping(binding.get("payload"))
        method._evaluation_policy(config)
        _evidence_policy(config)
    except Exception as exc:  # noqa: BLE001
        errors.append(str(exc))
    return errors


def _source_binding(
    *,
    kind: str,
    artifact_id: str,
    root: Path,
    validator: Callable[..., dict[str, Any]],
    validator_key: str,
    json_views: Sequence[str],
    jsonl_views: Sequence[str] = (),
    text_views: Sequence[str] = (),
) -> dict[str, Any]:
    return hardening._source_binding(
        kind=kind,
        artifact_id=artifact_id,
        artifact_root=root,
        validator=validator,
        validator_key=validator_key,
        json_views=json_views,
        jsonl_views=jsonl_views,
        text_views=text_views,
    )


def _validate_binding(
    binding: Mapping[str, Any],
    *,
    kind: str,
    validator: Callable[..., dict[str, Any]],
    validator_key: str,
) -> list[str]:
    return method._validate_custom_binding(
        binding,
        kind=kind,
        validator=validator,
        validator_key=validator_key,
    )


def _review_binding(review_id: str, root: Path) -> dict[str, Any]:
    return _source_binding(
        kind="smoothed_review",
        artifact_id=review_id,
        root=root,
        validator=method.validate_smoothed_review_artifact,
        validator_key="review_id",
        json_views=(
            "smoothed_review_manifest.json",
            "smoothed_decision.json",
        ),
        text_views=(
            "owner_smoothed_checklist.md",
            "smoothed_review_report.md",
            "reader_brief_section.md",
        ),
    )


def _review_payload(binding: Mapping[str, Any]) -> dict[str, Any]:
    return {
        **_bundle_json(binding, "smoothed_review_manifest.json"),
        "smoothed_decision": _bundle_json(binding, "smoothed_decision.json"),
    }


def _comparison_payload(binding: Mapping[str, Any]) -> dict[str, Any]:
    return {
        **_bundle_json(binding, "smoothed_comparison_manifest.json"),
        "smoothed_vs_limited_metrics": _bundle_json(
            binding, "smoothed_vs_limited_metrics.json"
        ),
        "smoothed_regime_comparison": _bundle_json(
            binding, "smoothed_regime_comparison.json"
        ),
        "smoothed_rolling_comparison": _bundle_json(
            binding, "smoothed_rolling_comparison.json"
        ),
        "smoothed_stability_comparison": _bundle_json(
            binding, "smoothed_stability_comparison.json"
        ),
        "smoothing_lag_cost_analysis": _bundle_json(
            binding, "smoothing_lag_cost_analysis.json"
        ),
    }


def _smoothed_backfill_payload(binding: Mapping[str, Any]) -> dict[str, Any]:
    return {
        **_bundle_json(binding, "smoothed_backfill_manifest.json"),
        "smoothed_backfill_summary": _bundle_json(binding, "smoothed_backfill_summary.json"),
        "smoothed_method_states": _bundle_jsonl(binding, "smoothed_method_states.jsonl"),
        "smoothed_trade_ledger": _bundle_jsonl(binding, "smoothed_trade_ledger.jsonl"),
    }


def _baseline_payload(binding: Mapping[str, Any]) -> dict[str, Any]:
    return {
        **_bundle_json(binding, "paper_shadow_backfill_manifest.json"),
        "paper_shadow_backfill_input_snapshot": _bundle_json(
            binding, "paper_shadow_backfill_input_snapshot.json"
        ),
        "backfill_method_states": _bundle_jsonl(binding, "backfill_method_states.jsonl"),
        "backfill_trade_ledger": _bundle_jsonl(binding, "backfill_trade_ledger.jsonl"),
        "backfill_data_quality": _bundle_json(binding, "backfill_data_quality.json"),
    }


def _manifest_generated(payload: Mapping[str, Any], field: str) -> datetime:
    return target_core._datetime(payload.get("generated_at"), field=field)


def _lineage(
    review: Mapping[str, Any],
    comparison: Mapping[str, Any],
    smoothed: Mapping[str, Any],
) -> tuple[str, str, str]:
    comparison_id = _text(review.get("comparison_id"))
    smoothed_id = _text(review.get("smoothed_backfill_id"))
    baseline_id = _text(smoothed.get("source_paper_shadow_backfill_id"))
    _require(comparison_id == comparison.get("comparison_id"), "review/comparison lineage mismatch")
    _require(
        smoothed_id == comparison.get("smoothed_backfill_id"),
        "review/comparison smoothed backfill mismatch",
    )
    _require(
        smoothed_id == smoothed.get("smoothed_backfill_id"),
        "review/smoothed backfill mismatch",
    )
    _require(
        comparison.get("baseline_backfill_id") == baseline_id,
        "comparison/baseline backfill mismatch",
    )
    return comparison_id, smoothed_id, baseline_id


def _chronology(generated: datetime, *sources: Mapping[str, Any]) -> None:
    for source in sources:
        _require(
            _manifest_generated(source, "smoothed evidence source generated_at") <= generated,
            "smoothed evidence source is newer than generated_at",
        )


def _method_row(payload: Mapping[str, Any], key: str, method_name: str) -> dict[str, Any]:
    rows = _records(_mapping(payload.get(key)).get("methods"))
    selected = [row for row in rows if row.get("method") == method_name]
    _require(len(selected) == 1, f"{key} must contain one row for {method_name}")
    return selected[0]


def _comparison_row(comparison: Mapping[str, Any], method_name: str) -> dict[str, Any]:
    rows = _records(
        _mapping(comparison.get("smoothed_vs_limited_metrics")).get("comparisons")
    )
    selected = [
        row
        for row in rows
        if row.get("method_a") == method_name and row.get("method_b") == "limited_adjustment"
    ]
    _require(len(selected) == 1, f"comparison must contain one limited row for {method_name}")
    return selected[0]


def _rolling_row(comparison: Mapping[str, Any], method_name: str) -> dict[str, Any]:
    rows = _records(
        _mapping(comparison.get("smoothed_rolling_comparison")).get("methods")
    )
    selected = [row for row in rows if row.get("method") == method_name]
    _require(len(selected) == 1, f"rolling must contain one row for {method_name}")
    return selected[0]


def _stability_row(comparison: Mapping[str, Any], method_name: str) -> dict[str, Any]:
    rows = _records(
        _mapping(comparison.get("smoothed_stability_comparison")).get("methods")
    )
    selected = [row for row in rows if row.get("method") == method_name]
    _require(len(selected) == 1, f"stability must contain one row for {method_name}")
    return selected[0]


def _lag_row(comparison: Mapping[str, Any], method_name: str) -> dict[str, Any]:
    rows = _records(
        _mapping(comparison.get("smoothing_lag_cost_analysis")).get("methods")
    )
    selected = [row for row in rows if row.get("method") == method_name]
    _require(len(selected) == 1, f"lag must contain one row for {method_name}")
    return selected[0]


def _metric_statuses(comparison: Mapping[str, Any], method_name: str) -> dict[str, Any]:
    primary = _comparison_row(comparison, method_name)
    rolling = _rolling_row(comparison, method_name)
    stability = _stability_row(comparison, method_name)
    lag = _lag_row(comparison, method_name)
    return {
        "total_return_delta": primary.get("total_return_delta"),
        "annualized_return_delta": primary.get("annualized_return_delta"),
        "max_drawdown_delta": primary.get("max_drawdown_delta"),
        "turnover_delta": primary.get("turnover_delta"),
        "large_jump_count_delta": primary.get("large_jump_count_delta"),
        "rolling_consistency": rolling.get("rolling_consistency_delta"),
        "stability": stability.get("stability_conclusion"),
        "lag_cost": lag.get("lag_cost_status"),
    }


def _support_matrix(
    comparison: Mapping[str, Any], review: Mapping[str, Any]
) -> dict[str, Any]:
    candidates = {
        row.get("method"): row
        for row in _records(_mapping(review.get("smoothed_decision")).get("candidate_evidence"))
    }
    rows = []
    for method_name in SMOOTHED_METHOD_TO_VARIANT:
        candidate = _mapping(candidates.get(method_name))
        statuses = _mapping(candidate.get("statuses"))
        rows.append(
            {
                "method": method_name,
                "statuses": {
                    "return_preservation": statuses.get("return_preservation"),
                    "drawdown": statuses.get("drawdown"),
                    "turnover": statuses.get("turnover"),
                    "weight_jump": statuses.get("weight_jumps"),
                    "rolling_consistency": statuses.get("rolling_consistency"),
                    "stability": statuses.get("stability"),
                    "lag_cost": statuses.get("lag_risk"),
                },
                "raw_sources": _metric_statuses(comparison, method_name),
                **SYSTEM_TARGET_SAFETY,
            }
        )
    return {"methods": rows, **SYSTEM_TARGET_SAFETY}


def _attribution_breakdown(
    review: Mapping[str, Any],
    comparison: Mapping[str, Any],
    smoothed: Mapping[str, Any],
) -> dict[str, Any]:
    decision = _mapping(review.get("smoothed_decision"))
    candidates = _records(decision.get("candidate_evidence"))
    supporting: list[dict[str, Any]] = []
    for candidate in candidates:
        statuses = _mapping(candidate.get("statuses"))
        positive = {
            key: value
            for key, value in statuses.items()
            if value in {"GOOD", "ACCEPTABLE", "IMPROVED", "LOW"}
        }
        if positive:
            supporting.append(
                {
                    "method": candidate.get("method"),
                    "reason": "observed_metric_support",
                    "statuses": positive,
                    **SYSTEM_TARGET_SAFETY,
                }
            )
    blocking = [
        {
            "reason": "requires_independent_forward_confirmation",
            "blocking": True,
            **SYSTEM_TARGET_SAFETY,
        }
    ]
    if decision.get("recommended_method") is None:
        blocking.append(
            {
                "reason": "no_evidence_backed_recommended_method",
                "blocking": True,
                **SYSTEM_TARGET_SAFETY,
            }
        )
    summary = _mapping(smoothed.get("smoothed_backfill_summary"))
    if summary.get("data_quality") == "PASS_WITH_WARNINGS":
        blocking.append(
            {
                "reason": "data_quality_pass_with_warnings",
                "blocking": False,
                **SYSTEM_TARGET_SAFETY,
            }
        )
    why_not_promote = [
        "forward confirmation target events have not matured",
        f"review_decision={decision.get('decision')}",
        f"recommended_method={decision.get('recommended_method')}",
    ]
    why_not_reject = (
        ["at least one candidate retains non-rejecting observed metric support"]
        if supporting
        else ["insufficient independent evidence prevents a causal rejection claim"]
    )
    return {
        "review_id": review.get("review_id"),
        "decision": decision.get("decision"),
        "confidence": decision.get("decision_confidence"),
        "candidate_methods": list(decision.get("candidate_methods") or []),
        "recommended_method": decision.get("recommended_method"),
        "secondary_method": decision.get("secondary_method"),
        "supporting_reasons": supporting,
        "blocking_reasons": blocking,
        "why_not_promote": why_not_promote,
        "why_not_reject": why_not_reject,
        "next_required_evidence": [
            "independent forward observations",
            "PIT-safe replay evidence",
            "data-quality and cost sensitivity",
            "holdout validation",
        ],
        **SYSTEM_TARGET_SAFETY,
    }


def _benefit_status(
    reductions: Sequence[float | None],
    rolling_status: Any,
    *,
    floor: float,
) -> str:
    if any(value is None for value in reductions) or rolling_status in {
        None,
        "",
        "INSUFFICIENT_DATA",
    }:
        return "INSUFFICIENT_DATA"
    positive = sum(float(value) > floor for value in reductions if value is not None)
    if rolling_status == "IMPROVED" and positive >= 2:
        return "STRONG"
    if rolling_status != "WORSE" and positive >= 1:
        return "MODERATE"
    return "WEAK"


def _benefit_lag(
    comparison: Mapping[str, Any], policy: Mapping[str, Any]
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    benefit_rows: list[dict[str, Any]] = []
    lag_rows: list[dict[str, Any]] = []
    tradeoff_rows: list[dict[str, Any]] = []
    for method_name in SMOOTHED_METHOD_TO_VARIANT:
        primary = _comparison_row(comparison, method_name)
        rolling = _rolling_row(comparison, method_name)
        stability = _stability_row(comparison, method_name)
        lag = _lag_row(comparison, method_name)
        observation_count = (
            int(primary["sample_count"])
            if _finite(primary.get("sample_count"))
            else None
        )
        complete = (
            primary.get("evidence_status") == "PASS"
            and observation_count is not None
            and observation_count >= int(policy["minimum_path_observations"])
        )
        jump_delta = _nullable_float(primary.get("large_jump_count_delta")) if complete else None
        turnover_delta = _nullable_float(primary.get("turnover_delta")) if complete else None
        churn_delta = (
            _nullable_float(stability.get("weight_flip_count_delta_vs_limited"))
            if complete
            else None
        )
        reductions = (
            -jump_delta if jump_delta is not None else None,
            -turnover_delta if turnover_delta is not None else None,
            -churn_delta if churn_delta is not None else None,
        )
        rolling_status = rolling.get("rolling_consistency_delta") if complete else None
        benefit_status = _benefit_status(
            reductions,
            rolling_status,
            floor=float(policy["benefit_reduction_floor"]),
        )
        lag_status = lag.get("lag_cost_status") if complete else "INSUFFICIENT_DATA"
        lag_status = (
            lag_status
            if lag_status in {"LOW", "MEDIUM", "HIGH"}
            else "INSUFFICIENT_DATA"
        )
        tradeoff_status = (
            "INSUFFICIENT_DATA"
            if "INSUFFICIENT_DATA" in {benefit_status, lag_status}
            else "UNFAVORABLE"
            if lag_status == "HIGH" or benefit_status == "WEAK"
            else "FAVORABLE"
            if lag_status == "LOW" and benefit_status in {"STRONG", "MODERATE"}
            else "MIXED"
        )
        recommendation = (
            "insufficient_evidence"
            if tradeoff_status == "INSUFFICIENT_DATA"
            else "reject"
            if tradeoff_status == "UNFAVORABLE"
            else "continue_observation"
            if tradeoff_status == "MIXED"
            else "needs_forward_confirmation"
        )
        benefit_rows.append(
            {
                "method": method_name,
                "common_return_observation_count": observation_count,
                "weight_jump_reduction": reductions[0],
                "turnover_reduction": reductions[1],
                "signal_churn_reduction": reductions[2],
                "rolling_consistency_delta": rolling_status,
                "benefit_status": benefit_status,
                **SYSTEM_TARGET_SAFETY,
            }
        )
        lag_rows.append(
            {
                "method": method_name,
                "strong_recovery_lag_cost": (
                    _nullable_float(lag.get("strong_recovery_lag_cost")) if complete else None
                ),
                "fast_regime_change_lag_cost": (
                    _nullable_float(lag.get("fast_regime_change_lag_cost"))
                    if complete
                    else None
                ),
                "missed_upside_count": (
                    int(lag.get("missed_upside_count"))
                    if complete and _finite(lag.get("missed_upside_count"))
                    else None
                ),
                "lag_cost_status": lag_status,
                **SYSTEM_TARGET_SAFETY,
            }
        )
        tradeoff_rows.append(
            {
                "method": method_name,
                "benefit_status": benefit_status,
                "lag_cost_status": lag_status,
                "tradeoff_status": tradeoff_status,
                "recommendation": recommendation,
                **SYSTEM_TARGET_SAFETY,
            }
        )
    return (
        {"methods": benefit_rows, **SYSTEM_TARGET_SAFETY},
        {"methods": lag_rows, **SYSTEM_TARGET_SAFETY},
        {"methods": tradeoff_rows, **SYSTEM_TARGET_SAFETY},
    )


def _paired_rows(
    method_rows: Sequence[Mapping[str, Any]],
    baseline_rows: Sequence[Mapping[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    method_by_date = {_text(row.get("date")): dict(row) for row in method_rows}
    baseline_by_date = {_text(row.get("date")): dict(row) for row in baseline_rows}
    dates = sorted(set(method_by_date) & set(baseline_by_date))
    return [method_by_date[day] for day in dates], [baseline_by_date[day] for day in dates]


def _return_metrics(rows: Sequence[Mapping[str, Any]]) -> tuple[float, float]:
    metrics = legacy._sample_return_metrics(rows, min_sample=1)
    total_return = _nullable_float(metrics.get("total_return"))
    drawdown = _nullable_float(metrics.get("max_drawdown"))
    _require(total_return is not None and drawdown is not None, "regime returns must be finite")
    return total_return, drawdown


def _regime_summaries(
    smoothed: Mapping[str, Any],
    baseline: Mapping[str, Any],
    policy: Mapping[str, Any],
    evaluation_policy: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    baseline_snapshot = _mapping(baseline.get("paper_shadow_backfill_input_snapshot"))
    baseline_config = _mapping(_mapping(baseline_snapshot.get("config_binding")).get("payload"))
    _require(bool(baseline_config), "baseline backfill config commitment missing")
    baseline_states = _records(baseline.get("backfill_method_states"))
    smoothed_states = _records(smoothed.get("smoothed_method_states"))
    labels = legacy._regime_labels_from_states(
        [*baseline_states, *smoothed_states],
        baseline_config,
    )
    limited_rows = [
        row for row in baseline_states if row.get("target_method") == "limited_adjustment"
    ]
    ledger = _records(smoothed.get("smoothed_trade_ledger"))
    minimum = int(policy["minimum_regime_observations"])
    sideways_rows: list[dict[str, Any]] = []
    recovery_rows: list[dict[str, Any]] = []
    for method_name in SMOOTHED_METHOD_TO_VARIANT:
        method_states = [
            row for row in smoothed_states if row.get("target_method") == method_name
        ]
        for regime_name, destination in (
            ("sideways_choppy", sideways_rows),
            ("strong_recovery", recovery_rows),
        ):
            dates = {day for day, label in labels.items() if label == regime_name}
            paired_method, paired_limited = _paired_rows(
                [row for row in method_states if _text(row.get("date")) in dates],
                [row for row in limited_rows if _text(row.get("date")) in dates],
            )
            sample_count = len(paired_method)
            if sample_count < minimum:
                if regime_name == "sideways_choppy":
                    destination.append(
                        {
                            "method": method_name,
                            "sample_count": sample_count,
                            "return_delta_vs_limited": None,
                            "drawdown_delta_vs_limited": None,
                            "turnover_delta_vs_limited": None,
                            "signal_churn_delta_vs_limited": None,
                            "weight_jump_delta_vs_limited": None,
                            "sideways_status": "INSUFFICIENT_DATA",
                            **SYSTEM_TARGET_SAFETY,
                        }
                    )
                else:
                    destination.append(
                        {
                            "method": method_name,
                            "sample_count": sample_count,
                            "return_delta_vs_limited": None,
                            "risk_on_response_delay_days": None,
                            "missed_upside": None,
                            "lag_status": "INSUFFICIENT_DATA",
                            **SYSTEM_TARGET_SAFETY,
                        }
                    )
                continue
            method_return, method_drawdown = _return_metrics(paired_method)
            limited_return, limited_drawdown = _return_metrics(paired_limited)
            return_delta = round(method_return - limited_return, 10)
            drawdown_delta = round(method_drawdown - limited_drawdown, 10)
            if regime_name == "sideways_choppy":
                method_turnover = [row.get("turnover") for row in paired_method]
                limited_turnover = [row.get("turnover") for row in paired_limited]
                _require(
                    all(_finite(value) for value in [*method_turnover, *limited_turnover]),
                    f"{regime_name} {method_name} turnover observations must be finite",
                )
                turnover_delta = round(
                    sum(float(value) for value in method_turnover)
                    - sum(float(value) for value in limited_turnover),
                    10,
                )
                churn_delta = legacy._weight_flip_count(paired_method) - legacy._weight_flip_count(
                    paired_limited
                )
                jump_delta = legacy._large_jump_count(paired_method) - legacy._large_jump_count(
                    paired_limited
                )
                improved = (
                    return_delta >= float(policy["acceptable_return_delta_floor"])
                    and drawdown_delta >= float(policy["drawdown_improvement_floor"])
                    and turnover_delta <= float(policy["turnover_improvement_ceiling"])
                    and churn_delta <= float(policy["sideways_signal_churn_delta_ceiling"])
                    and jump_delta <= float(policy["jump_improvement_ceiling"])
                )
                worse = (
                    return_delta < float(policy["acceptable_return_delta_floor"])
                    and drawdown_delta < float(policy["drawdown_improvement_floor"])
                )
                destination.append(
                    {
                        "method": method_name,
                        "sample_count": sample_count,
                        "return_delta_vs_limited": return_delta,
                        "drawdown_delta_vs_limited": drawdown_delta,
                        "turnover_delta_vs_limited": turnover_delta,
                        "signal_churn_delta_vs_limited": churn_delta,
                        "weight_jump_delta_vs_limited": jump_delta,
                        "sideways_status": (
                            "IMPROVED" if improved else "WORSE" if worse else "MIXED"
                        ),
                        **SYSTEM_TARGET_SAFETY,
                    }
                )
            else:
                delay = legacy._risk_on_delay_proxy_days(ledger, method_name, dates)
                lag_status = (
                    "HIGH"
                    if return_delta
                    <= float(evaluation_policy["lag_cost_high_threshold"])
                    else "MEDIUM"
                    if return_delta
                    <= float(evaluation_policy["lag_cost_medium_threshold"])
                    else "LOW"
                )
                destination.append(
                    {
                        "method": method_name,
                        "sample_count": sample_count,
                        "return_delta_vs_limited": return_delta,
                        "risk_on_response_delay_days": float(delay),
                        "missed_upside": round(max(0.0, -return_delta), 10),
                        "lag_status": lag_status,
                        **SYSTEM_TARGET_SAFETY,
                    }
                )
    return (
        {"regime": "sideways_choppy", "methods": sideways_rows, **SYSTEM_TARGET_SAFETY},
        {"regime": "strong_recovery", "methods": recovery_rows, **SYSTEM_TARGET_SAFETY},
    )


def _recommended_method(review: Mapping[str, Any]) -> str | None:
    decision = _mapping(review.get("smoothed_decision"))
    candidate = decision.get("recommended_method")
    if candidate is None:
        return None
    _require(candidate in SMOOTHED_METHOD_TO_VARIANT, "recommended method is not smoothed")
    rows = [
        row
        for row in _records(decision.get("candidate_evidence"))
        if row.get("method") == candidate and row.get("promotion_eligible") is True
    ]
    _require(len(rows) == 1, "recommended method lacks unique eligible evidence")
    return _text(candidate)


def _confirmation_targets(
    review: Mapping[str, Any],
    regime: Mapping[str, Any],
    policy: Mapping[str, Any],
) -> dict[str, Any]:
    decision = _mapping(review.get("smoothed_decision"))
    candidate = _recommended_method(review)
    if candidate is None:
        return {
            "schema_version": 2,
            "report_type": "etf_dynamic_v3_smoothed_confirmation_targets",
            "status": "INSUFFICIENT_EVIDENCE",
            "review_id": review.get("review_id"),
            "decision": decision.get("decision"),
            "confidence": decision.get("decision_confidence"),
            "candidate_method": None,
            "targets": [],
            "auto_apply": False,
            **SYSTEM_TARGET_SAFETY,
        }
    sideways = _method_row(regime, "sideways_validation_summary", candidate)
    recovery = _method_row(regime, "recovery_lag_validation_summary", candidate)
    windows = list(policy["confirmation_windows"])
    prefix = candidate.removesuffix("_limited_adjustment")
    targets = [
        {
            "target_id": f"{prefix}_vs_limited",
            "method": candidate,
            "baseline": "limited_adjustment",
            "required_forward_events": policy["required_forward_events"],
            "windows": windows,
            "success_criteria": {
                "return_delta_floor": policy["forward_return_delta_floor"],
                "drawdown_delta_floor": policy["forward_drawdown_delta_floor"],
                "turnover_delta_ceiling": policy["forward_turnover_delta_ceiling"],
            },
            "status": "IN_PROGRESS",
            **SYSTEM_TARGET_SAFETY,
        },
        {
            "target_id": f"{prefix}_vs_static_baseline",
            "method": candidate,
            "baseline": "static_baseline",
            "required_forward_events": policy["required_forward_events"],
            "windows": windows,
            "success_criteria": {
                "return_delta_floor": policy["forward_return_delta_floor"],
                "drawdown_delta_floor": policy["forward_drawdown_delta_floor"],
                "turnover_delta_ceiling": policy["forward_turnover_delta_ceiling"],
            },
            "status": "IN_PROGRESS",
            **SYSTEM_TARGET_SAFETY,
        },
        {
            "target_id": f"{prefix}_sideways_choppy_improvement",
            "method": candidate,
            "baseline": "limited_adjustment",
            "required_sideways_events": policy["required_sideways_events"],
            "current_backtest_sideways_status": sideways.get("sideways_status"),
            "success_criteria": {
                "signal_churn_delta_ceiling": policy[
                    "sideways_signal_churn_delta_ceiling"
                ],
                "weight_jump_delta_ceiling": policy[
                    "sideways_weight_jump_delta_ceiling"
                ],
                "turnover_delta_ceiling": policy["sideways_turnover_delta_ceiling"],
            },
            "status": "IN_PROGRESS",
            **SYSTEM_TARGET_SAFETY,
        },
        {
            "target_id": f"{prefix}_recovery_lag_watch",
            "method": candidate,
            "baseline": "limited_adjustment",
            "required_recovery_events": policy["required_recovery_events"],
            "current_backtest_lag_status": recovery.get("lag_status"),
            "status": "WATCH_ONLY",
            **SYSTEM_TARGET_SAFETY,
        },
    ]
    return {
        "schema_version": 2,
        "report_type": "etf_dynamic_v3_smoothed_confirmation_targets",
        "status": "PASS",
        "review_id": review.get("review_id"),
        "decision": decision.get("decision"),
        "confidence": decision.get("decision_confidence"),
        "candidate_method": candidate,
        "targets": targets,
        "auto_apply": False,
        **SYSTEM_TARGET_SAFETY,
    }


def _watch_summary(
    attribution: Mapping[str, Any],
    benefit: Mapping[str, Any],
    regime: Mapping[str, Any],
    confirmation: Mapping[str, Any],
) -> dict[str, Any]:
    breakdown = _mapping(attribution.get("smoothed_decision_reason_breakdown"))
    targets = _mapping(confirmation.get("smoothed_confirmation_targets"))
    candidate = targets.get("candidate_method")
    forward_status = (
        "IN_PROGRESS"
        if _records(targets.get("targets"))
        else "NOT_REGISTERED"
    )
    tradeoff = (
        _method_row(benefit, "benefit_lag_tradeoff_matrix", _text(candidate))
        if candidate
        else {}
    )
    sideways = (
        _method_row(regime, "sideways_validation_summary", _text(candidate))
        if candidate
        else {}
    )
    recovery = (
        _method_row(regime, "recovery_lag_validation_summary", _text(candidate))
        if candidate
        else {}
    )
    action = (
        "owner_review_required"
        if recovery.get("lag_status") == "HIGH"
        or tradeoff.get("tradeoff_status") == "UNFAVORABLE"
        else "continue_observation"
    )
    return {
        "candidate_method": candidate,
        "secondary_method": breakdown.get("secondary_method"),
        "current_decision": breakdown.get("decision"),
        "confidence": breakdown.get("confidence"),
        "benefit_lag_tradeoff": tradeoff.get(
            "tradeoff_status", "INSUFFICIENT_EVIDENCE"
        ),
        "sideways_validation_status": sideways.get(
            "sideways_status", "INSUFFICIENT_EVIDENCE"
        ),
        "recovery_lag_status": recovery.get("lag_status", "INSUFFICIENT_EVIDENCE"),
        "forward_confirmation_status": forward_status,
        "recommended_action": action,
        **SYSTEM_TARGET_SAFETY,
    }


def _render_attribution(
    manifest: Mapping[str, Any],
    breakdown: Mapping[str, Any],
    matrix: Mapping[str, Any],
) -> str:
    return "\n".join(
        [
            f"# Smoothed Review Attribution {manifest.get('attribution_id')}",
            "",
            f"- review_id: {manifest.get('review_id')}",
            f"- comparison_id: {manifest.get('comparison_id')}",
            f"- smoothed_backfill_id: {manifest.get('smoothed_backfill_id')}",
            f"- baseline_backfill_id: {manifest.get('baseline_backfill_id')}",
            f"- decision: {breakdown.get('decision')}",
            f"- confidence: {breakdown.get('confidence')}",
            f"- recommended_method: {breakdown.get('recommended_method')}",
            f"- secondary_method: {breakdown.get('secondary_method')}",
            f"- supporting_reasons: {breakdown.get('supporting_reasons')}",
            f"- blocking_reasons: {breakdown.get('blocking_reasons')}",
            f"- why_not_promote: {breakdown.get('why_not_promote')}",
            f"- why_not_reject: {breakdown.get('why_not_reject')}",
            f"- metric_support_matrix: {matrix.get('methods')}",
            "- not_pit_safe: true",
            "- broker_action_allowed: false",
            "- production_effect: none",
            "",
            "本报告按同一Review→Comparison→Backfill lineage解释证据；没有经Review选出的"
            "recommended method时，不补造3d候选或forward readiness。",
            "",
        ]
    )


def _render_benefit(
    manifest: Mapping[str, Any],
    benefit: Mapping[str, Any],
    lag: Mapping[str, Any],
    tradeoff: Mapping[str, Any],
) -> str:
    return "\n".join(
        [
            f"# Smoothing Benefit Lag Drilldown {manifest.get('drilldown_id')}",
            "",
            f"- comparison_id: {manifest.get('comparison_id')}",
            f"- smoothed_backfill_id: {manifest.get('smoothed_backfill_id')}",
            f"- baseline_backfill_id: {manifest.get('baseline_backfill_id')}",
            f"- benefit_methods: {benefit.get('methods')}",
            f"- lag_methods: {lag.get('methods')}",
            f"- tradeoff_methods: {tradeoff.get('methods')}",
            "- candidate_role_fixed: false",
            "- not_pit_safe: true",
            "- broker_action_allowed: false",
            "- production_effect: none",
            "",
            "每个method独立披露benefit与lag；样本或指标不足保持null/INSUFFICIENT_DATA。",
            "",
        ]
    )


def _render_regime(
    manifest: Mapping[str, Any],
    sideways: Mapping[str, Any],
    recovery: Mapping[str, Any],
) -> str:
    return "\n".join(
        [
            f"# Smoothed Regime Validation {manifest.get('regime_validation_id')}",
            "",
            f"- smoothed_backfill_id: {manifest.get('smoothed_backfill_id')}",
            f"- baseline_backfill_id: {manifest.get('baseline_backfill_id')}",
            f"- sideways_methods: {sideways.get('methods')}",
            f"- recovery_methods: {recovery.get('methods')}",
            "- requires_forward_confirmation: true",
            "- not_pit_safe: true",
            "- broker_action_allowed: false",
            "- production_effect: none",
            "",
            "样本数低于reviewed policy floor时不计算return/drawdown/lag结论。",
            "",
        ]
    )


def _render_confirmation(
    manifest: Mapping[str, Any], targets: Mapping[str, Any]
) -> str:
    return "\n".join(
        [
            f"# Smoothed Forward Confirmation {manifest.get('confirmation_id')}",
            "",
            f"- review_id: {manifest.get('review_id')}",
            f"- regime_validation_id: {manifest.get('regime_validation_id')}",
            f"- status: {targets.get('status')}",
            f"- candidate_method: {targets.get('candidate_method')}",
            "- registered_targets: "
            f"{[row.get('target_id') for row in _records(targets.get('targets'))]}",
            "- auto_apply: false",
            "- broker_action_allowed: false",
            "- production_effect: none",
            "",
            "只有Review真实选出的evidence-backed recommended method才能登记forward targets；"
            "当前没有候选时target_count为0。",
            "",
        ]
    )


def _render_watch_checklist(summary: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# Owner Smoothed Watch Checklist",
            "",
            "- [ ] 是否已有evidence-backed recommended method？",
            "- [ ] 是否需要继续积累独立forward/PIT/DQ/cost/holdout证据？",
            "- [ ] 是否存在recovery lag warning？",
            "- [ ] 是否确认不写official target weights？",
            "- [ ] 是否确认no broker / no production？",
            "",
            f"- candidate_method: {summary.get('candidate_method')}",
            f"- recommended_action: {summary.get('recommended_action')}",
            f"- forward_confirmation_status: {summary.get('forward_confirmation_status')}",
            "",
        ]
    )


def _render_watch_report(
    manifest: Mapping[str, Any],
    summary: Mapping[str, Any],
    attribution: Mapping[str, Any],
    benefit: Mapping[str, Any],
    regime: Mapping[str, Any],
    confirmation: Mapping[str, Any],
) -> str:
    return "\n".join(
        [
            f"# Smoothed Watch Pack {manifest.get('watch_pack_id')}",
            "",
            f"- review_id: {manifest.get('review_id')}",
            f"- comparison_id: {manifest.get('comparison_id')}",
            f"- smoothed_backfill_id: {manifest.get('smoothed_backfill_id')}",
            f"- baseline_backfill_id: {manifest.get('baseline_backfill_id')}",
            f"- candidate_method: {summary.get('candidate_method')}",
            f"- current_decision: {summary.get('current_decision')}",
            f"- confidence: {summary.get('confidence')}",
            f"- benefit_lag_tradeoff: {summary.get('benefit_lag_tradeoff')}",
            f"- sideways_validation_status: {summary.get('sideways_validation_status')}",
            f"- recovery_lag_status: {summary.get('recovery_lag_status')}",
            f"- forward_confirmation_status: {summary.get('forward_confirmation_status')}",
            f"- recommended_action: {summary.get('recommended_action')}",
            f"- attribution_id: {attribution.get('attribution_id')}",
            f"- drilldown_id: {benefit.get('drilldown_id')}",
            f"- regime_validation_id: {regime.get('regime_validation_id')}",
            f"- confirmation_id: {confirmation.get('confirmation_id')}",
            "- not_pit_safe: true",
            "- broker_action_allowed: false",
            "- production_effect: none",
            "",
            "Watch Pack只投影同一lineage的证据；candidate为空表示尚无可登记forward target的"
            "recommended method，不表示workflow失败或策略被拒绝。",
            "",
        ]
    )


def _render_watch_reader(summary: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "## Dynamic Rescue Smoothed Method Watch",
            "",
            f"- candidate_method: {summary.get('candidate_method')}",
            f"- current_decision: {summary.get('current_decision')}",
            f"- benefit_lag_tradeoff: {summary.get('benefit_lag_tradeoff')}",
            f"- sideways_validation_status: {summary.get('sideways_validation_status')}",
            f"- recovery_lag_status: {summary.get('recovery_lag_status')}",
            f"- forward_confirmation_status: {summary.get('forward_confirmation_status')}",
            f"- recommended_action: {summary.get('recommended_action')}",
            "- broker_action_allowed: false",
            "- production_effect: none",
            "",
        ]
    )


def _attribution_views(
    snapshot: Mapping[str, Any], *, attribution_id: str, root: Path
) -> tuple[dict[str, bytes], dict[str, Any]]:
    review = _review_payload(_mapping(snapshot.get("review_source")))
    comparison = _comparison_payload(_mapping(snapshot.get("comparison_source")))
    smoothed = _smoothed_backfill_payload(_mapping(snapshot.get("smoothed_backfill_source")))
    _config, policy = _policy(snapshot)
    comparison_id, smoothed_id, baseline_id = _lineage(review, comparison, smoothed)
    breakdown = _attribution_breakdown(review, comparison, smoothed)
    matrix = _support_matrix(comparison, review)
    manifest = {
        "schema_version": 2,
        "report_type": "etf_dynamic_v3_smoothed_review_attribution_manifest",
        "attribution_id": attribution_id,
        "review_id": review.get("review_id"),
        "comparison_id": comparison_id,
        "smoothed_backfill_id": smoothed_id,
        "baseline_backfill_id": baseline_id,
        "generated_at": snapshot.get("generated_at"),
        "status": "PASS",
        "market_regime": smoothed.get("market_regime"),
        "date_start": smoothed.get("date_start"),
        "date_end": smoothed.get("date_end"),
        "data_quality_status": _mapping(smoothed.get("smoothed_backfill_summary")).get(
            "data_quality"
        ),
        "policy_id": _mapping(_config.get("evidence_policy_metadata")).get("policy_id"),
        "minimum_path_observations": policy["minimum_path_observations"],
        "smoothed_review_attribution_input_snapshot_path": str(
            root / "smoothed_review_attribution_input_snapshot.json"
        ),
        "smoothed_review_attribution_manifest_path": str(
            root / "smoothed_review_attribution_manifest.json"
        ),
        "smoothed_decision_reason_breakdown_path": str(
            root / "smoothed_decision_reason_breakdown.json"
        ),
        "smoothed_metric_support_matrix_path": str(
            root / "smoothed_metric_support_matrix.json"
        ),
        "smoothed_review_attribution_report_path": str(
            root / "smoothed_review_attribution_report.md"
        ),
        **SYSTEM_TARGET_SAFETY,
    }
    report = _render_attribution(manifest, breakdown, matrix)
    views = {
        "smoothed_review_attribution_input_snapshot.json": _json_bytes(dict(snapshot)),
        "smoothed_review_attribution_manifest.json": _json_bytes(manifest),
        "smoothed_decision_reason_breakdown.json": _json_bytes(breakdown),
        "smoothed_metric_support_matrix.json": _json_bytes(matrix),
        "smoothed_review_attribution_report.md": report.encode("utf-8"),
    }
    return views, {
        "manifest": manifest,
        "smoothed_decision_reason_breakdown": breakdown,
        "smoothed_metric_support_matrix": matrix,
    }


def _validate_attribution_snapshot(snapshot: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    try:
        _require(
            snapshot.get("schema_version") == SMOOTHED_ATTRIBUTION_SNAPSHOT_SCHEMA,
            "attribution snapshot schema invalid",
        )
        review_binding = _mapping(snapshot.get("review_source"))
        comparison_binding = _mapping(snapshot.get("comparison_source"))
        backfill_binding = _mapping(snapshot.get("smoothed_backfill_source"))
        errors.extend(
            _validate_binding(
                review_binding,
                kind="smoothed_review",
                validator=method.validate_smoothed_review_artifact,
                validator_key="review_id",
            )
        )
        errors.extend(method._validate_custom_binding(
            comparison_binding,
            kind="smoothed_comparison",
            validator=method.validate_smoothed_comparison_artifact,
            validator_key="comparison_id",
        ))
        errors.extend(method._validate_smoothed_backfill_binding(backfill_binding))
        errors.extend(_validate_policy_binding(_mapping(snapshot.get("policy_binding"))))
        generated = target_core._datetime(
            snapshot.get("generated_at"), field="attribution generated_at"
        )
        review = _review_payload(review_binding)
        comparison = _comparison_payload(comparison_binding)
        smoothed = _smoothed_backfill_payload(backfill_binding)
        _lineage(review, comparison, smoothed)
        _chronology(generated, review, comparison, smoothed)
    except Exception as exc:  # noqa: BLE001
        errors.append(str(exc))
    return errors


def run_smoothed_review_attribution(
    *,
    review_id: str,
    comparison_id: str,
    backfill_id: str,
    review_dir: Path = DEFAULT_SMOOTHED_REVIEW_DIR,
    comparison_dir: Path = DEFAULT_SMOOTHED_COMPARISON_DIR,
    backfill_dir: Path = DEFAULT_SMOOTHED_BACKFILL_DIR,
    output_dir: Path = DEFAULT_SMOOTHED_REVIEW_ATTRIBUTION_DIR,
    config_path: Path = DEFAULT_SMOOTHED_LIMITED_CONFIG_PATH,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = _generated_at(generated_at)
    snapshot = {
        "schema_version": SMOOTHED_ATTRIBUTION_SNAPSHOT_SCHEMA,
        "generated_at": generated.isoformat(),
        "review_source": _review_binding(review_id, review_dir),
        "comparison_source": method._comparison_binding(comparison_id, comparison_dir),
        "smoothed_backfill_source": method._smoothed_backfill_binding(backfill_id, backfill_dir),
        "policy_binding": _policy_binding(config_path),
        "production_effect": "none",
    }
    errors = _validate_attribution_snapshot(snapshot)
    _require(not errors, "; ".join(errors))
    artifact_id = _stable_id("smoothed-review-attribution", snapshot)
    root = _unique_dir(output_dir / artifact_id)
    views, payload = _attribution_views(snapshot, attribution_id=root.name, root=root)
    _write(
        root,
        views,
        "latest_smoothed_review_attribution",
        "smoothed_review_attribution_manifest.json",
    )
    return {"attribution_id": root.name, "attribution_dir": root, **payload}


def smoothed_review_attribution_report_payload(
    *,
    attribution_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_SMOOTHED_REVIEW_ATTRIBUTION_DIR,
) -> dict[str, Any]:
    root = hardening._artifact_dir_from_latest(
        output_dir=output_dir,
        artifact_id=attribution_id if not latest else None,
        pointer_name="latest_smoothed_review_attribution",
    )
    return {
        **_read_json(root / "smoothed_review_attribution_manifest.json"),
        "smoothed_decision_reason_breakdown": _read_json(
            root / "smoothed_decision_reason_breakdown.json"
        ),
        "smoothed_metric_support_matrix": _read_json(
            root / "smoothed_metric_support_matrix.json"
        ),
        "input_snapshot": _read_json(
            root / "smoothed_review_attribution_input_snapshot.json"
        ),
        "attribution_dir": str(root),
    }


def validate_smoothed_review_attribution_artifact(
    *,
    attribution_id: str,
    output_dir: Path = DEFAULT_SMOOTHED_REVIEW_ATTRIBUTION_DIR,
) -> dict[str, Any]:
    root = output_dir / attribution_id
    snapshot = legacy._read_optional_json(
        root / "smoothed_review_attribution_input_snapshot.json"
    ) or {}
    errors = _validate_attribution_snapshot(snapshot)
    mismatches: list[str] = []
    try:
        views, _ = _attribution_views(snapshot, attribution_id=attribution_id, root=root)
        mismatches = _view_errors(root, views)
    except Exception as exc:  # noqa: BLE001
        errors.append(str(exc))
    return _validation_payload(
        "etf_dynamic_v3_smoothed_review_attribution_validation",
        attribution_id,
        [
            _check("snapshot_and_live_inputs", not errors, "; ".join(errors)),
            _check("content_derived_views", not mismatches, ",".join(mismatches)),
        ],
        artifact_id_key="attribution_id",
    )


def _benefit_views(
    snapshot: Mapping[str, Any], *, drilldown_id: str, root: Path
) -> tuple[dict[str, bytes], dict[str, Any]]:
    comparison = _comparison_payload(_mapping(snapshot.get("comparison_source")))
    smoothed = _smoothed_backfill_payload(_mapping(snapshot.get("smoothed_backfill_source")))
    config, policy = _policy(snapshot)
    _require(
        comparison.get("smoothed_backfill_id") == smoothed.get("smoothed_backfill_id"),
        "benefit/comparison smoothed backfill mismatch",
    )
    _require(
        comparison.get("baseline_backfill_id")
        == smoothed.get("source_paper_shadow_backfill_id"),
        "benefit baseline lineage mismatch",
    )
    benefit, lag, tradeoff = _benefit_lag(comparison, policy)
    manifest = {
        "schema_version": 2,
        "report_type": "etf_dynamic_v3_smoothing_benefit_lag_manifest",
        "drilldown_id": drilldown_id,
        "comparison_id": comparison.get("comparison_id"),
        "smoothed_backfill_id": smoothed.get("smoothed_backfill_id"),
        "baseline_backfill_id": smoothed.get("source_paper_shadow_backfill_id"),
        "generated_at": snapshot.get("generated_at"),
        "status": "PASS",
        "market_regime": smoothed.get("market_regime"),
        "date_start": smoothed.get("date_start"),
        "date_end": smoothed.get("date_end"),
        "policy_id": _mapping(config.get("evidence_policy_metadata")).get("policy_id"),
        "smoothing_benefit_lag_input_snapshot_path": str(
            root / "smoothing_benefit_lag_input_snapshot.json"
        ),
        "smoothing_benefit_lag_manifest_path": str(
            root / "smoothing_benefit_lag_manifest.json"
        ),
        "smoothing_benefit_summary_path": str(root / "smoothing_benefit_summary.json"),
        "lag_cost_summary_path": str(root / "lag_cost_summary.json"),
        "benefit_lag_tradeoff_matrix_path": str(
            root / "benefit_lag_tradeoff_matrix.json"
        ),
        "smoothing_benefit_lag_report_path": str(
            root / "smoothing_benefit_lag_report.md"
        ),
        **SYSTEM_TARGET_SAFETY,
    }
    report = _render_benefit(manifest, benefit, lag, tradeoff)
    views = {
        "smoothing_benefit_lag_input_snapshot.json": _json_bytes(dict(snapshot)),
        "smoothing_benefit_lag_manifest.json": _json_bytes(manifest),
        "smoothing_benefit_summary.json": _json_bytes(benefit),
        "lag_cost_summary.json": _json_bytes(lag),
        "benefit_lag_tradeoff_matrix.json": _json_bytes(tradeoff),
        "smoothing_benefit_lag_report.md": report.encode("utf-8"),
    }
    return views, {
        "manifest": manifest,
        "smoothing_benefit_summary": benefit,
        "lag_cost_summary": lag,
        "benefit_lag_tradeoff_matrix": tradeoff,
    }


def _validate_benefit_snapshot(snapshot: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    try:
        _require(
            snapshot.get("schema_version") == SMOOTHING_BENEFIT_LAG_SNAPSHOT_SCHEMA,
            "benefit snapshot schema invalid",
        )
        comparison_binding = _mapping(snapshot.get("comparison_source"))
        backfill_binding = _mapping(snapshot.get("smoothed_backfill_source"))
        errors.extend(method._validate_custom_binding(
            comparison_binding,
            kind="smoothed_comparison",
            validator=method.validate_smoothed_comparison_artifact,
            validator_key="comparison_id",
        ))
        errors.extend(method._validate_smoothed_backfill_binding(backfill_binding))
        errors.extend(_validate_policy_binding(_mapping(snapshot.get("policy_binding"))))
        generated = target_core._datetime(
            snapshot.get("generated_at"), field="benefit generated_at"
        )
        comparison = _comparison_payload(comparison_binding)
        smoothed = _smoothed_backfill_payload(backfill_binding)
        _require(
            comparison.get("smoothed_backfill_id") == smoothed.get("smoothed_backfill_id"),
            "benefit smoothed lineage mismatch",
        )
        _require(
            comparison.get("baseline_backfill_id")
            == smoothed.get("source_paper_shadow_backfill_id"),
            "benefit baseline lineage mismatch",
        )
        _chronology(generated, comparison, smoothed)
    except Exception as exc:  # noqa: BLE001
        errors.append(str(exc))
    return errors


def run_smoothing_benefit_lag_drilldown(
    *,
    smoothed_backfill_id: str,
    comparison_id: str,
    backfill_dir: Path = DEFAULT_SMOOTHED_BACKFILL_DIR,
    comparison_dir: Path = DEFAULT_SMOOTHED_COMPARISON_DIR,
    output_dir: Path = DEFAULT_SMOOTHING_BENEFIT_LAG_DIR,
    config_path: Path = DEFAULT_SMOOTHED_LIMITED_CONFIG_PATH,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = _generated_at(generated_at)
    snapshot = {
        "schema_version": SMOOTHING_BENEFIT_LAG_SNAPSHOT_SCHEMA,
        "generated_at": generated.isoformat(),
        "smoothed_backfill_source": method._smoothed_backfill_binding(
            smoothed_backfill_id, backfill_dir
        ),
        "comparison_source": method._comparison_binding(comparison_id, comparison_dir),
        "policy_binding": _policy_binding(config_path),
        "production_effect": "none",
    }
    errors = _validate_benefit_snapshot(snapshot)
    _require(not errors, "; ".join(errors))
    artifact_id = _stable_id("smoothing-benefit-lag", snapshot)
    root = _unique_dir(output_dir / artifact_id)
    views, payload = _benefit_views(snapshot, drilldown_id=root.name, root=root)
    _write(root, views, "latest_smoothing_benefit_lag", "smoothing_benefit_lag_manifest.json")
    return {"drilldown_id": root.name, "drilldown_dir": root, **payload}


def smoothing_benefit_lag_report_payload(
    *,
    drilldown_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_SMOOTHING_BENEFIT_LAG_DIR,
) -> dict[str, Any]:
    root = hardening._artifact_dir_from_latest(
        output_dir=output_dir,
        artifact_id=drilldown_id if not latest else None,
        pointer_name="latest_smoothing_benefit_lag",
    )
    return {
        **_read_json(root / "smoothing_benefit_lag_manifest.json"),
        "smoothing_benefit_summary": _read_json(root / "smoothing_benefit_summary.json"),
        "lag_cost_summary": _read_json(root / "lag_cost_summary.json"),
        "benefit_lag_tradeoff_matrix": _read_json(root / "benefit_lag_tradeoff_matrix.json"),
        "input_snapshot": _read_json(root / "smoothing_benefit_lag_input_snapshot.json"),
        "drilldown_dir": str(root),
    }


def validate_smoothing_benefit_lag_artifact(
    *,
    drilldown_id: str,
    output_dir: Path = DEFAULT_SMOOTHING_BENEFIT_LAG_DIR,
) -> dict[str, Any]:
    root = output_dir / drilldown_id
    snapshot = legacy._read_optional_json(root / "smoothing_benefit_lag_input_snapshot.json") or {}
    errors = _validate_benefit_snapshot(snapshot)
    mismatches: list[str] = []
    try:
        views, _ = _benefit_views(snapshot, drilldown_id=drilldown_id, root=root)
        mismatches = _view_errors(root, views)
    except Exception as exc:  # noqa: BLE001
        errors.append(str(exc))
    return _validation_payload(
        "etf_dynamic_v3_smoothing_benefit_lag_validation",
        drilldown_id,
        [
            _check("snapshot_and_live_inputs", not errors, "; ".join(errors)),
            _check("content_derived_views", not mismatches, ",".join(mismatches)),
        ],
        artifact_id_key="drilldown_id",
    )


def _regime_views(
    snapshot: Mapping[str, Any], *, regime_validation_id: str, root: Path
) -> tuple[dict[str, bytes], dict[str, Any]]:
    smoothed = _smoothed_backfill_payload(_mapping(snapshot.get("smoothed_backfill_source")))
    baseline = _baseline_payload(_mapping(snapshot.get("baseline_backfill_source")))
    config, policy = _policy(snapshot)
    _require(
        smoothed.get("source_paper_shadow_backfill_id") == baseline.get("backfill_id"),
        "regime baseline lineage mismatch",
    )
    sideways, recovery = _regime_summaries(
        smoothed,
        baseline,
        policy,
        method._evaluation_policy(config),
    )
    manifest = {
        "schema_version": 2,
        "report_type": "etf_dynamic_v3_smoothed_regime_validation_manifest",
        "regime_validation_id": regime_validation_id,
        "smoothed_backfill_id": smoothed.get("smoothed_backfill_id"),
        "baseline_backfill_id": baseline.get("backfill_id"),
        "generated_at": snapshot.get("generated_at"),
        "status": "PASS",
        "market_regime": smoothed.get("market_regime"),
        "date_start": smoothed.get("date_start"),
        "date_end": smoothed.get("date_end"),
        "policy_id": _mapping(config.get("evidence_policy_metadata")).get("policy_id"),
        "minimum_regime_observations": policy["minimum_regime_observations"],
        "smoothed_regime_validation_input_snapshot_path": str(
            root / "smoothed_regime_validation_input_snapshot.json"
        ),
        "smoothed_regime_validation_manifest_path": str(
            root / "smoothed_regime_validation_manifest.json"
        ),
        "sideways_validation_summary_path": str(
            root / "sideways_validation_summary.json"
        ),
        "recovery_lag_validation_summary_path": str(
            root / "recovery_lag_validation_summary.json"
        ),
        "smoothed_regime_validation_report_path": str(
            root / "smoothed_regime_validation_report.md"
        ),
        **SYSTEM_TARGET_SAFETY,
    }
    report = _render_regime(manifest, sideways, recovery)
    views = {
        "smoothed_regime_validation_input_snapshot.json": _json_bytes(dict(snapshot)),
        "smoothed_regime_validation_manifest.json": _json_bytes(manifest),
        "sideways_validation_summary.json": _json_bytes(sideways),
        "recovery_lag_validation_summary.json": _json_bytes(recovery),
        "smoothed_regime_validation_report.md": report.encode("utf-8"),
    }
    return views, {
        "manifest": manifest,
        "sideways_validation_summary": sideways,
        "recovery_lag_validation_summary": recovery,
    }


def _validate_regime_snapshot(snapshot: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    try:
        _require(
            snapshot.get("schema_version") == SMOOTHED_REGIME_SNAPSHOT_SCHEMA,
            "regime snapshot schema invalid",
        )
        smoothed_binding = _mapping(snapshot.get("smoothed_backfill_source"))
        baseline_binding = _mapping(snapshot.get("baseline_backfill_source"))
        errors.extend(method._validate_smoothed_backfill_binding(smoothed_binding))
        errors.extend(history._validate_history_binding(baseline_binding))
        errors.extend(_validate_policy_binding(_mapping(snapshot.get("policy_binding"))))
        generated = target_core._datetime(
            snapshot.get("generated_at"), field="regime generated_at"
        )
        smoothed = _smoothed_backfill_payload(smoothed_binding)
        baseline = _baseline_payload(baseline_binding)
        _require(
            smoothed.get("source_paper_shadow_backfill_id") == baseline.get("backfill_id"),
            "regime baseline lineage mismatch",
        )
        _chronology(generated, smoothed, baseline)
    except Exception as exc:  # noqa: BLE001
        errors.append(str(exc))
    return errors


def run_smoothed_regime_validation(
    *,
    smoothed_backfill_id: str,
    smoothed_backfill_dir: Path = DEFAULT_SMOOTHED_BACKFILL_DIR,
    baseline_backfill_dir: Path = DEFAULT_PAPER_SHADOW_BACKFILL_DIR,
    output_dir: Path = DEFAULT_SMOOTHED_REGIME_VALIDATION_DIR,
    config_path: Path = DEFAULT_SMOOTHED_LIMITED_CONFIG_PATH,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = _generated_at(generated_at)
    smoothed_binding = method._smoothed_backfill_binding(
        smoothed_backfill_id, smoothed_backfill_dir
    )
    smoothed = _smoothed_backfill_payload(smoothed_binding)
    baseline_id = _text(smoothed.get("source_paper_shadow_backfill_id"))
    snapshot = {
        "schema_version": SMOOTHED_REGIME_SNAPSHOT_SCHEMA,
        "generated_at": generated.isoformat(),
        "smoothed_backfill_source": smoothed_binding,
        "baseline_backfill_source": history._backfill_binding(
            baseline_id, baseline_backfill_dir
        ),
        "policy_binding": _policy_binding(config_path),
        "production_effect": "none",
    }
    errors = _validate_regime_snapshot(snapshot)
    _require(not errors, "; ".join(errors))
    artifact_id = _stable_id("smoothed-regime-validation", snapshot)
    root = _unique_dir(output_dir / artifact_id)
    views, payload = _regime_views(snapshot, regime_validation_id=root.name, root=root)
    _write(
        root,
        views,
        "latest_smoothed_regime_validation",
        "smoothed_regime_validation_manifest.json",
    )
    return {"regime_validation_id": root.name, "regime_validation_dir": root, **payload}


def smoothed_regime_validation_report_payload(
    *,
    regime_validation_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_SMOOTHED_REGIME_VALIDATION_DIR,
) -> dict[str, Any]:
    root = hardening._artifact_dir_from_latest(
        output_dir=output_dir,
        artifact_id=regime_validation_id if not latest else None,
        pointer_name="latest_smoothed_regime_validation",
    )
    return {
        **_read_json(root / "smoothed_regime_validation_manifest.json"),
        "sideways_validation_summary": _read_json(root / "sideways_validation_summary.json"),
        "recovery_lag_validation_summary": _read_json(
            root / "recovery_lag_validation_summary.json"
        ),
        "input_snapshot": _read_json(
            root / "smoothed_regime_validation_input_snapshot.json"
        ),
        "regime_validation_dir": str(root),
    }


def validate_smoothed_regime_validation_artifact(
    *,
    regime_validation_id: str,
    output_dir: Path = DEFAULT_SMOOTHED_REGIME_VALIDATION_DIR,
) -> dict[str, Any]:
    root = output_dir / regime_validation_id
    snapshot = legacy._read_optional_json(
        root / "smoothed_regime_validation_input_snapshot.json"
    ) or {}
    errors = _validate_regime_snapshot(snapshot)
    mismatches: list[str] = []
    try:
        views, _ = _regime_views(
            snapshot, regime_validation_id=regime_validation_id, root=root
        )
        mismatches = _view_errors(root, views)
    except Exception as exc:  # noqa: BLE001
        errors.append(str(exc))
    return _validation_payload(
        "etf_dynamic_v3_smoothed_regime_validation_validation",
        regime_validation_id,
        [
            _check("snapshot_and_live_inputs", not errors, "; ".join(errors)),
            _check("content_derived_views", not mismatches, ",".join(mismatches)),
        ],
        artifact_id_key="regime_validation_id",
    )


def _regime_binding(regime_validation_id: str, root: Path) -> dict[str, Any]:
    return _source_binding(
        kind="smoothed_regime_validation",
        artifact_id=regime_validation_id,
        root=root,
        validator=validate_smoothed_regime_validation_artifact,
        validator_key="regime_validation_id",
        json_views=(
            "smoothed_regime_validation_manifest.json",
            "sideways_validation_summary.json",
            "recovery_lag_validation_summary.json",
        ),
        text_views=("smoothed_regime_validation_report.md",),
    )


def _regime_payload(binding: Mapping[str, Any]) -> dict[str, Any]:
    return {
        **_bundle_json(binding, "smoothed_regime_validation_manifest.json"),
        "sideways_validation_summary": _bundle_json(
            binding, "sideways_validation_summary.json"
        ),
        "recovery_lag_validation_summary": _bundle_json(
            binding, "recovery_lag_validation_summary.json"
        ),
    }


def _confirmation_views(
    snapshot: Mapping[str, Any], *, confirmation_id: str, root: Path
) -> tuple[dict[str, bytes], dict[str, Any]]:
    review = _review_payload(_mapping(snapshot.get("review_source")))
    regime = _regime_payload(_mapping(snapshot.get("regime_source")))
    config, policy = _policy(snapshot)
    comparison_id = _text(review.get("comparison_id"))
    smoothed_id = _text(review.get("smoothed_backfill_id"))
    baseline_id = _text(regime.get("baseline_backfill_id"))
    _require(
        bool(comparison_id and smoothed_id and baseline_id),
        "confirmation source lineage is incomplete",
    )
    _require(
        regime.get("smoothed_backfill_id") == smoothed_id
        and regime.get("baseline_backfill_id") == baseline_id,
        "confirmation review/regime lineage mismatch",
    )
    targets = _confirmation_targets(review, regime, policy)
    manifest = {
        "schema_version": 2,
        "report_type": "etf_dynamic_v3_smoothed_confirmation_manifest",
        "confirmation_id": confirmation_id,
        "review_id": review.get("review_id"),
        "comparison_id": comparison_id,
        "smoothed_backfill_id": smoothed_id,
        "baseline_backfill_id": baseline_id,
        "regime_validation_id": regime.get("regime_validation_id"),
        "generated_at": snapshot.get("generated_at"),
        "status": "PASS",
        "evidence_status": targets.get("status"),
        "candidate_method": targets.get("candidate_method"),
        "target_count": len(_records(targets.get("targets"))),
        "policy_id": _mapping(config.get("evidence_policy_metadata")).get("policy_id"),
        "smoothed_confirmation_input_snapshot_path": str(
            root / "smoothed_confirmation_input_snapshot.json"
        ),
        "smoothed_confirmation_manifest_path": str(
            root / "smoothed_confirmation_manifest.json"
        ),
        "smoothed_confirmation_targets_path": str(
            root / "smoothed_confirmation_targets.json"
        ),
        "smoothed_confirmation_report_path": str(
            root / "smoothed_confirmation_report.md"
        ),
        **SYSTEM_TARGET_SAFETY,
    }
    report = _render_confirmation(manifest, targets)
    views = {
        "smoothed_confirmation_input_snapshot.json": _json_bytes(dict(snapshot)),
        "smoothed_confirmation_manifest.json": _json_bytes(manifest),
        "smoothed_confirmation_targets.json": _json_bytes(targets),
        "smoothed_confirmation_report.md": report.encode("utf-8"),
    }
    return views, {"manifest": manifest, "smoothed_confirmation_targets": targets}


def _validate_confirmation_snapshot(snapshot: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    try:
        _require(
            snapshot.get("schema_version") == SMOOTHED_CONFIRMATION_SNAPSHOT_SCHEMA,
            "confirmation snapshot schema invalid",
        )
        review_binding = _mapping(snapshot.get("review_source"))
        regime_binding = _mapping(snapshot.get("regime_source"))
        errors.extend(_validate_binding(
            review_binding,
            kind="smoothed_review",
            validator=method.validate_smoothed_review_artifact,
            validator_key="review_id",
        ))
        errors.extend(_validate_binding(
            regime_binding,
            kind="smoothed_regime_validation",
            validator=validate_smoothed_regime_validation_artifact,
            validator_key="regime_validation_id",
        ))
        errors.extend(_validate_policy_binding(_mapping(snapshot.get("policy_binding"))))
        generated = target_core._datetime(
            snapshot.get("generated_at"), field="confirmation generated_at"
        )
        review = _review_payload(review_binding)
        regime = _regime_payload(regime_binding)
        _require(bool(review.get("comparison_id")), "confirmation comparison lineage missing")
        smoothed_id = _text(review.get("smoothed_backfill_id"))
        baseline_id = _text(regime.get("baseline_backfill_id"))
        _require(bool(smoothed_id and baseline_id), "confirmation backfill lineage missing")
        _require(
            regime.get("smoothed_backfill_id") == smoothed_id
            and regime.get("baseline_backfill_id") == baseline_id,
            "confirmation lineage mismatch",
        )
        _chronology(generated, review, regime)
    except Exception as exc:  # noqa: BLE001
        errors.append(str(exc))
    return errors


def register_smoothed_confirmation_targets(
    *,
    review_id: str,
    regime_validation_id: str,
    review_dir: Path = DEFAULT_SMOOTHED_REVIEW_DIR,
    regime_validation_dir: Path = DEFAULT_SMOOTHED_REGIME_VALIDATION_DIR,
    output_dir: Path = DEFAULT_SMOOTHED_FORWARD_CONFIRMATION_DIR,
    config_path: Path = DEFAULT_SMOOTHED_LIMITED_CONFIG_PATH,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = _generated_at(generated_at)
    snapshot = {
        "schema_version": SMOOTHED_CONFIRMATION_SNAPSHOT_SCHEMA,
        "generated_at": generated.isoformat(),
        "review_source": _review_binding(review_id, review_dir),
        "regime_source": _regime_binding(regime_validation_id, regime_validation_dir),
        "policy_binding": _policy_binding(config_path),
        "production_effect": "none",
    }
    errors = _validate_confirmation_snapshot(snapshot)
    _require(not errors, "; ".join(errors))
    artifact_id = _stable_id("smoothed-confirmation", snapshot)
    root = _unique_dir(output_dir / artifact_id)
    views, payload = _confirmation_views(snapshot, confirmation_id=root.name, root=root)
    _write(root, views, "latest_smoothed_confirmation", "smoothed_confirmation_manifest.json")
    return {"confirmation_id": root.name, "confirmation_dir": root, **payload}


def smoothed_confirmation_report_payload(
    *,
    confirmation_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_SMOOTHED_FORWARD_CONFIRMATION_DIR,
) -> dict[str, Any]:
    root = hardening._artifact_dir_from_latest(
        output_dir=output_dir,
        artifact_id=confirmation_id if not latest else None,
        pointer_name="latest_smoothed_confirmation",
    )
    return {
        **_read_json(root / "smoothed_confirmation_manifest.json"),
        "smoothed_confirmation_targets": _read_json(
            root / "smoothed_confirmation_targets.json"
        ),
        "input_snapshot": _read_json(root / "smoothed_confirmation_input_snapshot.json"),
        "confirmation_dir": str(root),
    }


def validate_smoothed_confirmation_artifact(
    *,
    confirmation_id: str,
    output_dir: Path = DEFAULT_SMOOTHED_FORWARD_CONFIRMATION_DIR,
) -> dict[str, Any]:
    root = output_dir / confirmation_id
    snapshot = legacy._read_optional_json(root / "smoothed_confirmation_input_snapshot.json") or {}
    errors = _validate_confirmation_snapshot(snapshot)
    mismatches: list[str] = []
    try:
        views, payload = _confirmation_views(
            snapshot, confirmation_id=confirmation_id, root=root
        )
        mismatches = _view_errors(root, views)
        targets = _mapping(payload.get("smoothed_confirmation_targets"))
        candidate = targets.get("candidate_method")
        _require(
            (candidate is None and not _records(targets.get("targets")))
            or (
                candidate in SMOOTHED_METHOD_TO_VARIANT
                and bool(_records(targets.get("targets")))
            ),
            "confirmation candidate/target semantics invalid",
        )
    except Exception as exc:  # noqa: BLE001
        errors.append(str(exc))
    return _validation_payload(
        "etf_dynamic_v3_smoothed_confirmation_validation",
        confirmation_id,
        [
            _check("snapshot_and_live_inputs", not errors, "; ".join(errors)),
            _check("content_derived_views", not mismatches, ",".join(mismatches)),
        ],
        artifact_id_key="confirmation_id",
    )


def _attribution_binding(attribution_id: str, root: Path) -> dict[str, Any]:
    return _source_binding(
        kind="smoothed_review_attribution",
        artifact_id=attribution_id,
        root=root,
        validator=validate_smoothed_review_attribution_artifact,
        validator_key="attribution_id",
        json_views=(
            "smoothed_review_attribution_manifest.json",
            "smoothed_decision_reason_breakdown.json",
            "smoothed_metric_support_matrix.json",
        ),
        text_views=("smoothed_review_attribution_report.md",),
    )


def _benefit_binding(drilldown_id: str, root: Path) -> dict[str, Any]:
    return _source_binding(
        kind="smoothing_benefit_lag",
        artifact_id=drilldown_id,
        root=root,
        validator=validate_smoothing_benefit_lag_artifact,
        validator_key="drilldown_id",
        json_views=(
            "smoothing_benefit_lag_manifest.json",
            "smoothing_benefit_summary.json",
            "lag_cost_summary.json",
            "benefit_lag_tradeoff_matrix.json",
        ),
        text_views=("smoothing_benefit_lag_report.md",),
    )


def _confirmation_binding(confirmation_id: str, root: Path) -> dict[str, Any]:
    return _source_binding(
        kind="smoothed_confirmation",
        artifact_id=confirmation_id,
        root=root,
        validator=validate_smoothed_confirmation_artifact,
        validator_key="confirmation_id",
        json_views=(
            "smoothed_confirmation_manifest.json",
            "smoothed_confirmation_targets.json",
        ),
        text_views=("smoothed_confirmation_report.md",),
    )


def _simple_payload(
    binding: Mapping[str, Any],
    manifest_name: str,
    payloads: Mapping[str, str],
) -> dict[str, Any]:
    return {
        **_bundle_json(binding, manifest_name),
        **{key: _bundle_json(binding, name) for key, name in payloads.items()},
    }


def _watch_views(
    snapshot: Mapping[str, Any], *, watch_pack_id: str, root: Path
) -> tuple[dict[str, bytes], dict[str, Any]]:
    attribution = _simple_payload(
        _mapping(snapshot.get("attribution_source")),
        "smoothed_review_attribution_manifest.json",
        {
            "smoothed_decision_reason_breakdown": "smoothed_decision_reason_breakdown.json",
            "smoothed_metric_support_matrix": "smoothed_metric_support_matrix.json",
        },
    )
    benefit = _simple_payload(
        _mapping(snapshot.get("benefit_lag_source")),
        "smoothing_benefit_lag_manifest.json",
        {
            "smoothing_benefit_summary": "smoothing_benefit_summary.json",
            "lag_cost_summary": "lag_cost_summary.json",
            "benefit_lag_tradeoff_matrix": "benefit_lag_tradeoff_matrix.json",
        },
    )
    regime = _regime_payload(_mapping(snapshot.get("regime_source")))
    confirmation = _simple_payload(
        _mapping(snapshot.get("confirmation_source")),
        "smoothed_confirmation_manifest.json",
        {"smoothed_confirmation_targets": "smoothed_confirmation_targets.json"},
    )
    lineage = {
        "review_id": attribution.get("review_id"),
        "comparison_id": attribution.get("comparison_id"),
        "smoothed_backfill_id": attribution.get("smoothed_backfill_id"),
        "baseline_backfill_id": attribution.get("baseline_backfill_id"),
    }
    for source, fields in (
        (benefit, ("comparison_id", "smoothed_backfill_id", "baseline_backfill_id")),
        (regime, ("smoothed_backfill_id", "baseline_backfill_id")),
        (
            confirmation,
            ("review_id", "comparison_id", "smoothed_backfill_id", "baseline_backfill_id"),
        ),
    ):
        for field in fields:
            _require(source.get(field) == lineage[field], f"watch {field} lineage mismatch")
    summary = _watch_summary(attribution, benefit, regime, confirmation)
    manifest = {
        "schema_version": 2,
        "report_type": "etf_dynamic_v3_smoothed_watch_manifest",
        "watch_pack_id": watch_pack_id,
        "review_attribution_id": attribution.get("attribution_id"),
        "benefit_lag_id": benefit.get("drilldown_id"),
        "regime_validation_id": regime.get("regime_validation_id"),
        "confirmation_id": confirmation.get("confirmation_id"),
        **lineage,
        "generated_at": snapshot.get("generated_at"),
        "status": "PASS",
        "smoothed_watch_input_snapshot_path": str(
            root / "smoothed_watch_input_snapshot.json"
        ),
        "smoothed_watch_manifest_path": str(root / "smoothed_watch_manifest.json"),
        "smoothed_watch_summary_path": str(root / "smoothed_watch_summary.json"),
        "owner_smoothed_watch_checklist_path": str(
            root / "owner_smoothed_watch_checklist.md"
        ),
        "smoothed_watch_pack_report_path": str(
            root / "smoothed_watch_pack_report.md"
        ),
        "reader_brief_section_path": str(root / "reader_brief_section.md"),
        **SYSTEM_TARGET_SAFETY,
    }
    checklist = _render_watch_checklist(summary)
    report = _render_watch_report(
        manifest, summary, attribution, benefit, regime, confirmation
    )
    reader = _render_watch_reader(summary)
    views = {
        "smoothed_watch_input_snapshot.json": _json_bytes(dict(snapshot)),
        "smoothed_watch_manifest.json": _json_bytes(manifest),
        "smoothed_watch_summary.json": _json_bytes(summary),
        "owner_smoothed_watch_checklist.md": checklist.encode("utf-8"),
        "smoothed_watch_pack_report.md": report.encode("utf-8"),
        "reader_brief_section.md": reader.encode("utf-8"),
    }
    return views, {
        "manifest": manifest,
        "smoothed_watch_summary": summary,
        "reader_brief_section": reader,
    }


def _validate_watch_snapshot(snapshot: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    try:
        _require(
            snapshot.get("schema_version") == SMOOTHED_WATCH_SNAPSHOT_SCHEMA,
            "watch snapshot schema invalid",
        )
        specs = (
            (
                "attribution_source",
                "smoothed_review_attribution",
                validate_smoothed_review_attribution_artifact,
                "attribution_id",
            ),
            (
                "benefit_lag_source",
                "smoothing_benefit_lag",
                validate_smoothing_benefit_lag_artifact,
                "drilldown_id",
            ),
            (
                "regime_source",
                "smoothed_regime_validation",
                validate_smoothed_regime_validation_artifact,
                "regime_validation_id",
            ),
            (
                "confirmation_source",
                "smoothed_confirmation",
                validate_smoothed_confirmation_artifact,
                "confirmation_id",
            ),
        )
        for field, kind, validator, key in specs:
            errors.extend(_validate_binding(
                _mapping(snapshot.get(field)),
                kind=kind,
                validator=validator,
                validator_key=key,
            ))
        generated = target_core._datetime(
            snapshot.get("generated_at"), field="watch generated_at"
        )
        manifests = [
            _bundle_json(_mapping(snapshot.get(field)), name)
            for field, name in (
                ("attribution_source", "smoothed_review_attribution_manifest.json"),
                ("benefit_lag_source", "smoothing_benefit_lag_manifest.json"),
                ("regime_source", "smoothed_regime_validation_manifest.json"),
                ("confirmation_source", "smoothed_confirmation_manifest.json"),
            )
        ]
        _chronology(generated, *manifests)
        lineage = manifests[0]
        for source, fields in (
            (manifests[1], ("comparison_id", "smoothed_backfill_id", "baseline_backfill_id")),
            (manifests[2], ("smoothed_backfill_id", "baseline_backfill_id")),
            (
                manifests[3],
                ("review_id", "comparison_id", "smoothed_backfill_id", "baseline_backfill_id"),
            ),
        ):
            for field in fields:
                _require(source.get(field) == lineage.get(field), f"watch {field} mismatch")
    except Exception as exc:  # noqa: BLE001
        errors.append(str(exc))
    return errors


def run_smoothed_watch_pack(
    *,
    review_attribution_id: str,
    benefit_lag_id: str,
    regime_validation_id: str,
    confirmation_id: str,
    attribution_dir: Path = DEFAULT_SMOOTHED_REVIEW_ATTRIBUTION_DIR,
    benefit_lag_dir: Path = DEFAULT_SMOOTHING_BENEFIT_LAG_DIR,
    regime_validation_dir: Path = DEFAULT_SMOOTHED_REGIME_VALIDATION_DIR,
    confirmation_dir: Path = DEFAULT_SMOOTHED_FORWARD_CONFIRMATION_DIR,
    output_dir: Path = DEFAULT_SMOOTHED_WATCH_PACK_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = _generated_at(generated_at)
    snapshot = {
        "schema_version": SMOOTHED_WATCH_SNAPSHOT_SCHEMA,
        "generated_at": generated.isoformat(),
        "attribution_source": _attribution_binding(review_attribution_id, attribution_dir),
        "benefit_lag_source": _benefit_binding(benefit_lag_id, benefit_lag_dir),
        "regime_source": _regime_binding(regime_validation_id, regime_validation_dir),
        "confirmation_source": _confirmation_binding(confirmation_id, confirmation_dir),
        "production_effect": "none",
    }
    errors = _validate_watch_snapshot(snapshot)
    _require(not errors, "; ".join(errors))
    artifact_id = _stable_id("smoothed-watch-pack", snapshot)
    root = _unique_dir(output_dir / artifact_id)
    views, payload = _watch_views(snapshot, watch_pack_id=root.name, root=root)
    _write(root, views, "latest_smoothed_watch_pack", "smoothed_watch_manifest.json")
    return {"watch_pack_id": root.name, "watch_pack_dir": root, **payload}


def smoothed_watch_pack_report_payload(
    *,
    watch_pack_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_SMOOTHED_WATCH_PACK_DIR,
) -> dict[str, Any]:
    root = hardening._artifact_dir_from_latest(
        output_dir=output_dir,
        artifact_id=watch_pack_id if not latest else None,
        pointer_name="latest_smoothed_watch_pack",
    )
    return {
        **_read_json(root / "smoothed_watch_manifest.json"),
        "smoothed_watch_summary": _read_json(root / "smoothed_watch_summary.json"),
        "reader_brief_section": (root / "reader_brief_section.md").read_text(
            encoding="utf-8"
        ),
        "input_snapshot": _read_json(root / "smoothed_watch_input_snapshot.json"),
        "watch_pack_dir": str(root),
    }


def validate_smoothed_watch_pack_artifact(
    *,
    watch_pack_id: str,
    output_dir: Path = DEFAULT_SMOOTHED_WATCH_PACK_DIR,
) -> dict[str, Any]:
    root = output_dir / watch_pack_id
    snapshot = legacy._read_optional_json(root / "smoothed_watch_input_snapshot.json") or {}
    errors = _validate_watch_snapshot(snapshot)
    mismatches: list[str] = []
    try:
        views, _ = _watch_views(snapshot, watch_pack_id=watch_pack_id, root=root)
        mismatches = _view_errors(root, views)
    except Exception as exc:  # noqa: BLE001
        errors.append(str(exc))
    return _validation_payload(
        "etf_dynamic_v3_smoothed_watch_pack_validation",
        watch_pack_id,
        [
            _check("snapshot_and_live_inputs", not errors, "; ".join(errors)),
            _check("content_derived_views", not mismatches, ",".join(mismatches)),
        ],
        artifact_id_key="watch_pack_id",
    )
