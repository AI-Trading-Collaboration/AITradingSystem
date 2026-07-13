from __future__ import annotations

import math
from collections.abc import Callable, Mapping, Sequence
from datetime import date, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from ai_trading_system.config import DEFAULT_DATA_QUALITY_CONFIG_PATH
from ai_trading_system.etf_portfolio import dynamic_v3_system_target as legacy
from ai_trading_system.etf_portfolio import dynamic_v3_system_target_portfolio as target_core
from ai_trading_system.etf_portfolio.dynamic_v3_confirmation_operations import (
    _file_bytes_match,
    _json_bytes,
    _jsonl_bytes,
    _operations_source_bundle,
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

PAPER_SHADOW_BACKFILL_SNAPSHOT_SCHEMA = "paper_shadow_backfill_input_snapshot.v2"
ROLLING_EVAL_SNAPSHOT_SCHEMA = "paper_shadow_rolling_eval_input_snapshot.v2"
REGIME_REVIEW_SNAPSHOT_SCHEMA = "paper_shadow_regime_review_input_snapshot.v2"
STABILITY_SNAPSHOT_SCHEMA = "paper_shadow_stability_input_snapshot.v2"
SELECTION_REVIEW_SNAPSHOT_SCHEMA = "system_target_selection_review_input_snapshot.v2"

DEFAULT_PAPER_SHADOW_BACKFILL_CONFIG_PATH = legacy.DEFAULT_PAPER_SHADOW_BACKFILL_CONFIG_PATH
DEFAULT_PAPER_SHADOW_BACKFILL_DIR = legacy.DEFAULT_PAPER_SHADOW_BACKFILL_DIR
DEFAULT_PAPER_SHADOW_ROLLING_EVAL_DIR = legacy.DEFAULT_PAPER_SHADOW_ROLLING_EVAL_DIR
DEFAULT_PAPER_SHADOW_REGIME_REVIEW_DIR = legacy.DEFAULT_PAPER_SHADOW_REGIME_REVIEW_DIR
DEFAULT_PAPER_SHADOW_STABILITY_DIR = legacy.DEFAULT_PAPER_SHADOW_STABILITY_DIR
DEFAULT_SYSTEM_TARGET_SELECTION_REVIEW_DIR = legacy.DEFAULT_SYSTEM_TARGET_SELECTION_REVIEW_DIR
DEFAULT_PRICE_CACHE_PATH = legacy.DEFAULT_PRICE_CACHE_PATH
DEFAULT_RATES_CACHE_PATH = legacy.DEFAULT_RATES_CACHE_PATH
TARGET_METHODS = legacy.TARGET_METHODS
SYSTEM_TARGET_SAFETY = legacy.SYSTEM_TARGET_SAFETY


class DynamicV3SystemTargetHistoryError(ValueError):
    """Raised when historical target-method evidence is not reproducible."""


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise DynamicV3SystemTargetHistoryError(message)


def _generated_at(value: datetime | None) -> datetime:
    try:
        return target_core._generated_at(value)
    except Exception as exc:  # noqa: BLE001
        raise DynamicV3SystemTargetHistoryError(str(exc)) from exc


def _date(value: Any, *, field: str) -> date:
    try:
        return date.fromisoformat(_text(value))
    except ValueError as exc:
        raise DynamicV3SystemTargetHistoryError(f"{field} must be ISO date") from exc


def _finite(value: Any) -> bool:
    return target_core._finite(value)


def _optional(value: Any, *, digits: int = 10) -> float | None:
    return round(float(value), digits) if _finite(value) else None


def _resolve(value: Any, default: Path) -> Path:
    path = Path(_text(value)) if _text(value) else default
    return path if path.is_absolute() else legacy.PROJECT_ROOT / path


def _validation_payload(
    report_type: str,
    artifact_id: str,
    checks: Sequence[Mapping[str, Any]],
    *,
    artifact_id_key: str,
    extra: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    return target_core._validation_payload(
        report_type,
        artifact_id,
        checks,
        artifact_id_key=artifact_id_key,
        extra=extra,
    )


def _validate_backfill_config_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    _require(payload.get("schema_version") == 1, "backfill schema_version must be 1")
    backfill = _mapping(payload.get("backfill"))
    _require(backfill.get("mode") == "BACKTEST_SIMULATION", "backfill mode invalid")
    _require(backfill.get("not_pit_safe") is True, "not_pit_safe guard required")
    _require(backfill.get("research_target_only") is True, "research-only guard required")
    _require(backfill.get("paper_shadow_only") is True, "paper-shadow guard required")
    metadata = target_core._policy_metadata(payload, name="paper shadow backfill")

    date_range = _mapping(payload.get("date_range"))
    start = _date(date_range.get("start"), field="backfill start")
    _require(start >= legacy.AI_AFTER_CHATGPT_START, "backfill start before AI regime")
    configured_end = _text(date_range.get("end"), "latest_available")
    if configured_end != "latest_available":
        _require(_date(configured_end, field="backfill end") >= start, "backfill end before start")
    _require(_text(date_range.get("rebalance_frequency")) == "weekly", "weekly required")
    _require(
        _text(date_range.get("rebalance_day")) in {"MON", "TUE", "WED", "THU", "FRI"},
        "rebalance day invalid",
    )
    warmup = date_range.get("min_history_days_before_first_rebalance")
    _require(
        isinstance(warmup, int) and not isinstance(warmup, bool) and warmup >= 0, "warmup invalid"
    )

    source = _mapping(payload.get("source"))
    for field in (
        "model_target_config",
        "model_target_dir",
        "paper_shadow_config",
        "price_cache_path",
    ):
        _require(bool(_text(source.get(field))), f"source.{field} required")

    enabled = [_text(item) for item in _mapping(payload.get("target_methods")).get("enabled", [])]
    _require(
        bool(enabled) and len(enabled) == len(set(enabled)), "target methods missing/duplicate"
    )
    _require(not (set(enabled) - set(TARGET_METHODS)), "unknown target method")
    _require(
        {"static_baseline", "no_trade_baseline", "limited_adjustment"}.issubset(enabled),
        "required target methods missing",
    )

    costs = _mapping(payload.get("costs"))
    for field in ("transaction_cost_bps", "slippage_bps"):
        value = costs.get(field)
        _require(_finite(value) and float(value) >= 0.0, f"cost {field} invalid")

    evaluation = _mapping(payload.get("evaluation"))
    minimum = evaluation.get("min_observations_per_window")
    _require(
        isinstance(minimum, int) and not isinstance(minimum, bool) and minimum >= 2,
        "minimum observations invalid",
    )
    rank_policy = _mapping(evaluation.get("rank_stability"))
    top_n = rank_policy.get("top_n")
    _require(isinstance(top_n, int) and not isinstance(top_n, bool) and top_n >= 1, "top_n invalid")
    for field in (
        "stable_top_frequency_min",
        "stable_bottom_frequency_max",
        "unstable_bottom_frequency_min",
    ):
        value = rank_policy.get(field)
        _require(_finite(value) and 0.0 <= float(value) <= 1.0, f"rank policy {field} invalid")

    regime = _mapping(payload.get("regime_policy"))
    regime_minimum = regime.get("min_sample_count")
    _require(
        isinstance(regime_minimum, int)
        and not isinstance(regime_minimum, bool)
        and regime_minimum >= 2,
        "regime min sample invalid",
    )
    for field in (
        "risk_off_symbol",
        "tech_drawdown_symbol",
        "semiconductor_pullback_symbol",
        "ai_trend_symbol",
        "strong_recovery_symbol",
    ):
        _require(bool(_text(regime.get(field))), f"regime policy {field} required")
    thresholds = {
        field: regime.get(field)
        for field in (
            "risk_off_return_threshold",
            "tech_drawdown_return_threshold",
            "semiconductor_pullback_return_threshold",
            "ai_trend_return_threshold",
            "strong_recovery_return_threshold",
        )
    }
    _require(all(_finite(value) for value in thresholds.values()), "regime threshold invalid")
    _require(
        float(thresholds["risk_off_return_threshold"])
        <= float(thresholds["semiconductor_pullback_return_threshold"])
        <= float(thresholds["tech_drawdown_return_threshold"])
        < 0.0,
        "negative regime threshold ordering invalid",
    )
    _require(
        0.0
        < float(thresholds["ai_trend_return_threshold"])
        <= float(thresholds["strong_recovery_return_threshold"]),
        "positive regime threshold ordering invalid",
    )

    stability = _mapping(payload.get("stability_policy"))
    for field in (
        "large_jump_threshold",
        "high_jump_threshold",
        "stable_max_daily_weight_change",
        "unstable_max_daily_weight_change",
        "moderate_annualized_turnover",
        "high_annualized_turnover",
    ):
        value = stability.get(field)
        _require(_finite(value) and float(value) >= 0.0, f"stability {field} invalid")
    _require(
        float(stability["large_jump_threshold"]) <= float(stability["high_jump_threshold"]),
        "jump threshold ordering invalid",
    )
    _require(
        float(stability["stable_max_daily_weight_change"])
        <= float(stability["unstable_max_daily_weight_change"]),
        "stability threshold ordering invalid",
    )
    _require(
        float(stability["moderate_annualized_turnover"])
        <= float(stability["high_annualized_turnover"]),
        "turnover threshold ordering invalid",
    )

    selection = _mapping(payload.get("selection_policy"))
    preferred = [_text(item) for item in selection.get("preferred_method_order", [])]
    reference = [_text(item) for item in selection.get("reference_only_methods", [])]
    _require(len(preferred) == len(set(preferred)), "preferred method order duplicate")
    _require(not ((set(preferred) | set(reference)) - set(enabled)), "selection method unknown")
    for field in (
        "preferred_method_score_tolerance",
        "continue_observation_score",
        "review_required_score",
    ):
        value = selection.get(field)
        _require(_finite(value) and 0.0 <= float(value) <= 1.0, f"selection {field} invalid")
    _require(
        float(selection["continue_observation_score"]) >= float(selection["review_required_score"]),
        "selection score ordering invalid",
    )
    weights = _mapping(selection.get("score_weights"))
    positive_fields = ("return", "drawdown", "risk_adjusted", "regime", "stability")
    _require(
        all(
            _finite(weights.get(field)) and float(weights[field]) >= 0.0
            for field in positive_fields
        ),
        "selection score weights invalid",
    )
    _require(
        abs(sum(float(weights[field]) for field in positive_fields) - 1.0) <= 1e-8,
        "positive selection weights must sum to one",
    )
    penalty = weights.get("turnover_penalty")
    _require(_finite(penalty) and 0.0 <= float(penalty) <= 1.0, "turnover penalty invalid")
    status_scores = _mapping(selection.get("stability_status_scores"))
    for status in ("STABLE", "MODERATE", "UNSTABLE"):
        _require(
            _finite(status_scores.get(status)) and 0.0 <= float(status_scores[status]) <= 1.0,
            f"stability status score missing: {status}",
        )

    _require(legacy._safety_config_locked(_mapping(payload.get("safety"))), "safety invalid")
    return {
        "policy_metadata": metadata,
        "start": start,
        "configured_end": configured_end,
        "enabled_methods": enabled,
        "minimum_observations": minimum,
        "regime_minimum": regime_minimum,
    }


def load_paper_shadow_backfill_config(
    path: Path = DEFAULT_PAPER_SHADOW_BACKFILL_CONFIG_PATH,
) -> dict[str, Any]:
    payload = target_core._yaml(path)
    _validate_backfill_config_payload(payload)
    return payload


def validate_paper_shadow_backfill_config(
    path: Path = DEFAULT_PAPER_SHADOW_BACKFILL_CONFIG_PATH,
) -> dict[str, Any]:
    checks: list[dict[str, Any]] = []
    summary: dict[str, Any] = {}
    try:
        payload = load_paper_shadow_backfill_config(path)
        summary = _validate_backfill_config_payload(payload)
        checks.append(_check("config_contract", True, ""))
    except Exception as exc:  # noqa: BLE001
        checks.append(_check("config_contract", False, str(exc)))
    return _validation_payload(
        "etf_dynamic_v3_paper_shadow_backfill_config_validation",
        "paper_shadow_backfill_config",
        checks,
        artifact_id_key="config_id",
        extra={
            "config_path": str(path),
            "mode": "BACKTEST_SIMULATION" if summary else "",
            "not_pit_safe": bool(summary),
        },
    )


def _cache_binding(path: Path, *, kind: str, required: bool = True) -> dict[str, Any]:
    return target_core._cache_binding(path, kind=kind, required=required)


def _config_bindings(config: Mapping[str, Any]) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    source = _mapping(config.get("source"))
    model_path = _resolve(
        source.get("model_target_config"), legacy.DEFAULT_MODEL_TARGET_CONFIG_PATH
    )
    paper_path = _resolve(
        source.get("paper_shadow_config"), legacy.DEFAULT_PAPER_SHADOW_CONFIG_PATH
    )
    model = target_core._config_binding(model_path, kind="model_target_policy")
    paper = target_core._config_binding(paper_path, kind="paper_shadow_policy")
    target_core._validate_model_config_payload(_mapping(model.get("payload")))
    target_core._validate_paper_config_payload(_mapping(paper.get("payload")))
    linked = target_core._linked_model_bindings(_mapping(model.get("payload")))
    return paper, [model, *linked]


def _target_weights(
    binding: Mapping[str, Any], enabled: Sequence[str]
) -> dict[str, dict[str, float]]:
    bundle = _mapping(binding.get("bundle"))
    rows = _records(_mapping(bundle.get("jsonl")).get("method_target_weights.jsonl"))
    by_method: dict[str, dict[str, float]] = {}
    for row in rows:
        method = _text(row.get("target_method"))
        _require(method and method not in by_method, "model target methods duplicate")
        by_method[method] = target_core._weights(row.get("weights"), field=f"target {method}")
    missing = set(enabled) - set(by_method)
    _require(not missing, f"model target methods missing: {','.join(sorted(missing))}")
    return {method: by_method[method] for method in enabled}


def _dq_summary(quality: Any, *, as_of: date) -> dict[str, Any]:
    return {
        "status": quality.status,
        "checked_at": quality.checked_at.isoformat(),
        "as_of": as_of.isoformat(),
        "error_count": quality.error_count,
        "warning_count": quality.warning_count,
        "gate_enforced": True,
    }


def _common_price_pivot(
    rows: Sequence[Mapping[str, Any]], *, symbols: Sequence[str]
) -> tuple[pd.DataFrame, list[str]]:
    frame = pd.DataFrame(rows)
    _require(not frame.empty, "backfill price rows empty")
    frame["date"] = pd.to_datetime(frame["date"], errors="raise")
    pivot = frame.pivot(index="date", columns="ticker", values="adj_close").sort_index()
    _require(not (set(symbols) - set(pivot.columns)), "backfill price symbols missing")
    incomplete = [
        index.date().isoformat()
        for index, row in pivot[list(symbols)].iterrows()
        if row.isna().any()
    ]
    common = pivot[list(symbols)].dropna(how="any")
    _require(len(common) >= 2, "backfill common finite price dates insufficient")
    return common, incomplete


def _backfill_snapshot(
    *,
    config_path: Path,
    price_cache_path: Path,
    rates_cache_path: Path,
    generated: datetime,
) -> dict[str, Any]:
    config_binding = target_core._config_binding(config_path, kind="paper_shadow_backfill_policy")
    config = _mapping(config_binding.get("payload"))
    summary = _validate_backfill_config_payload(config)
    source = _mapping(config.get("source"))
    target_root = _resolve(source.get("model_target_dir"), legacy.DEFAULT_MODEL_TARGET_DIR)
    target_dir, target_manifest = target_core._select_model_target(target_root, generated=generated)
    target_validation = target_core.validate_model_target_artifact(
        target_id=target_dir.name, output_dir=target_root
    )
    target_binding = target_core._artifact_binding(
        kind="model_target",
        artifact_dir=target_dir,
        artifact_id=target_dir.name,
        validation=target_validation,
        json_views=[
            "model_target_input_snapshot.json",
            "model_target_manifest.json",
            "model_target_weights.json",
            "target_constraint_checks.json",
        ],
        jsonl_views=["method_target_weights.jsonl"],
    )
    _require(
        Path(_text(target_manifest.get("config_path"))).resolve(strict=False)
        == _resolve(
            source.get("model_target_config"), legacy.DEFAULT_MODEL_TARGET_CONFIG_PATH
        ).resolve(strict=False),
        "model target config does not match backfill policy",
    )
    enabled = list(summary["enabled_methods"])
    weights = _target_weights(target_binding, enabled)
    symbols = sorted(
        {
            symbol
            for method_weights in weights.values()
            for symbol, weight in method_weights.items()
            if symbol != "CASH" and weight > 0.0
        }
    )
    _require(bool(symbols), "backfill has no priced symbols")
    start = summary["start"]
    configured_end = summary["configured_end"]
    requested_end = (
        generated.date()
        if configured_end == "latest_available"
        else _date(configured_end, field="backfill end")
    )
    _require(requested_end <= generated.date(), "backfill end exceeds generated cutoff")
    price_rows = target_core._market_price_rows(
        price_cache_path, symbols=symbols, start=start, end=requested_end
    )
    pivot, incomplete_dates = _common_price_pivot(price_rows, symbols=symbols)
    actual_start = pivot.index[0].date()
    actual_end = pivot.index[-1].date()
    quality = legacy._run_data_quality_gate(
        price_cache_path=price_cache_path,
        rates_cache_path=rates_cache_path,
        expected_symbols=symbols,
        as_of=actual_end,
    )
    _require(quality.passed, f"data quality gate failed: {quality.status}")
    paper_binding, model_bindings = _config_bindings(config)
    cache_bindings = [
        _cache_binding(price_cache_path, kind="prices", required=True),
        _cache_binding(rates_cache_path, kind="rates", required=True),
        _cache_binding(DEFAULT_DATA_QUALITY_CONFIG_PATH, kind="data_quality_policy", required=True),
        _cache_binding(
            price_cache_path.parent / "download_manifest.csv",
            kind="download_manifest",
            required=False,
        ),
        _cache_binding(
            price_cache_path.parent / "prices_marketstack_daily.csv",
            kind="secondary_prices",
            required=False,
        ),
    ]
    return {
        "schema_version": PAPER_SHADOW_BACKFILL_SNAPSHOT_SCHEMA,
        "generated_at": generated.isoformat(),
        "config_binding": config_binding,
        "paper_config_binding": paper_binding,
        "model_config_bindings": model_bindings,
        "model_target_source": target_binding,
        "model_target_selection": {
            "root": str(target_root),
            "artifact_id": target_dir.name,
            "cutoff_generated_at": generated.isoformat(),
            "selection_rule": "unique latest model-target as_of at or before cutoff",
        },
        "cache_bindings": cache_bindings,
        "expected_symbols": symbols,
        "price_rows": price_rows,
        "requested_start_date": start.isoformat(),
        "requested_end_date": configured_end,
        "effective_end_cutoff": requested_end.isoformat(),
        "actual_start_date": actual_start.isoformat(),
        "actual_end_date": actual_end.isoformat(),
        "excluded_incomplete_price_dates": incomplete_dates,
        "data_quality": _dq_summary(quality, as_of=actual_end),
        "current_definition_replayed_historically": True,
        "not_pit_safe": True,
        "missing_metrics_remain_null": True,
        **SYSTEM_TARGET_SAFETY,
    }


def _binding_payload(bindings: Sequence[Mapping[str, Any]], kind: str) -> dict[str, Any]:
    for binding in bindings:
        if binding.get("kind") == kind:
            return _mapping(binding.get("payload"))
    return {}


def _initial_weights(snapshot: Mapping[str, Any]) -> dict[str, float]:
    paper = _mapping(_mapping(snapshot.get("paper_config_binding")).get("payload"))
    return target_core._weights(
        _mapping(paper.get("baseline")).get("static_weights"), field="backfill initial baseline"
    )


def _backfill_report(
    manifest: Mapping[str, Any], calendar: Mapping[str, Any], data_quality: Mapping[str, Any]
) -> str:
    return "\n".join(
        [
            f"# Paper Shadow Historical Backfill {manifest.get('backfill_id')}",
            "",
            f"- market_regime: {manifest.get('market_regime')}",
            "- requested_date_range: "
            f"{manifest.get('requested_start_date')} to {manifest.get('requested_end_date')}",
            f"- actual_date_range: {manifest.get('date_start')} to {manifest.get('date_end')}",
            f"- source_model_target_id: {manifest.get('source_model_target_id')}",
            f"- rebalance_events: {calendar.get('rebalance_count')}",
            f"- tracked_methods: {', '.join(manifest.get('tracked_methods', []))}",
            f"- data_quality: {data_quality.get('data_quality')}",
            "- excluded_incomplete_price_date_count: "
            f"{data_quality.get('excluded_incomplete_price_date_count')}",
            f"- total_cost_bps: {manifest.get('total_cost_bps')}",
            "- mode: BACKTEST_SIMULATION",
            "- current_definition_replayed_historically: true",
            "- not_pit_safe: true",
            "- missing_metrics_remain_null: true",
            "- not_official_target_weights: true",
            "- broker_action_allowed: false",
            "- production_effect: none",
            "",
            "该报告使用当前已验证方法定义进行历史研究重放，不是PIT-safe或因果回测，"
            "不构成official target、production或broker授权。",
            "",
        ]
    )


def _dq_report(snapshot: Mapping[str, Any]) -> str:
    dq = _mapping(snapshot.get("data_quality"))
    return "\n".join(
        [
            "# Paper Shadow Historical Backfill Data Quality",
            "",
            f"- status: {dq.get('status')}",
            f"- checked_at: {dq.get('checked_at')}",
            f"- as_of: {dq.get('as_of')}",
            f"- error_count: {dq.get('error_count')}",
            f"- warning_count: {dq.get('warning_count')}",
            "- gate_enforced: true",
            "- production_effect: none",
            "",
        ]
    )


def _backfill_views(
    snapshot: Mapping[str, Any], *, backfill_id: str, output_dir: Path
) -> tuple[dict[str, bytes], dict[str, Any]]:
    config = _mapping(_mapping(snapshot.get("config_binding")).get("payload"))
    summary = _validate_backfill_config_payload(config)
    target_weights = _target_weights(
        _mapping(snapshot.get("model_target_source")), summary["enabled_methods"]
    )
    symbols = [_text(item) for item in snapshot.get("expected_symbols", [])]
    pivot, incomplete_dates = _common_price_pivot(
        _records(snapshot.get("price_rows")), symbols=symbols
    )
    _require(
        incomplete_dates == list(snapshot.get("excluded_incomplete_price_dates", [])),
        "incomplete price-date view drift",
    )
    returns = pivot.pct_change(fill_method=None).fillna(0.0)
    trading_dates = [index.date() for index in returns.index]
    date_range = _mapping(config.get("date_range"))
    rebalance_dates = legacy._backfill_rebalance_dates(
        trading_dates,
        frequency=_text(date_range.get("rebalance_frequency")),
        rebalance_day=_text(date_range.get("rebalance_day")),
        min_history_days=int(date_range.get("min_history_days_before_first_rebalance")),
    )
    initial = _initial_weights(snapshot)
    model_bindings = _records(snapshot.get("model_config_bindings"))
    model_config = _binding_payload(model_bindings, "model_target_policy")
    risk_config = _binding_payload(model_bindings, "risk_capped_policy")
    smoothed_config = _binding_payload(model_bindings, "smoothed_policy")
    costs = _mapping(config.get("costs"))
    total_cost_bps = float(costs["transaction_cost_bps"]) + float(costs["slippage_bps"])
    method_state = {
        method: {"weights": dict(initial), "portfolio_value": 1.0, "peak_value": 1.0}
        for method in target_weights
    }
    states: list[dict[str, Any]] = []
    ledger: list[dict[str, Any]] = []
    for timestamp, return_row in returns.iterrows():
        current_date = timestamp.date()
        is_rebalance = current_date in rebalance_dates
        for method, target in target_weights.items():
            state = method_state[method]
            before_weights = legacy._normalize_weights(_mapping(state["weights"]))
            gross_return = legacy._portfolio_return(before_weights, return_row)
            _require(_finite(gross_return) and gross_return > -1.0, "backfill gross return invalid")
            drifted = legacy._drift_weights(before_weights, return_row, gross_return)
            after_weights = dict(drifted)
            turnover = 0.0
            cap_events: list[dict[str, Any]] = []
            reallocation_events: list[dict[str, Any]] = []
            cap_reason_summary: dict[str, Any] = {}
            smoothing_events: list[dict[str, Any]] = []
            lag_events: list[dict[str, Any]] = []
            if is_rebalance and method != "no_trade_baseline":
                if method == "risk_capped_limited_adjustment":
                    _require(bool(risk_config), "risk-capped policy binding missing")
                    result = legacy._apply_risk_capped_limited_adjustment(
                        as_of=current_date,
                        base_weights=target_weights["limited_adjustment"],
                        previous_weights=drifted,
                        risk_config=risk_config,
                        model_config=model_config,
                        regime_context=legacy._risk_capped_regime_context_for_return(
                            return_row, config
                        ),
                    )
                    after_weights = target_core._weights(
                        result["capped_weights"], field=f"{method} after"
                    )
                    cap_events = _records(result.get("cap_events"))
                    reallocation_events = _records(result.get("reallocation_events"))
                    cap_reason_summary = _mapping(result.get("cap_reason_summary"))
                elif method in legacy.SMOOTHED_METHOD_TO_VARIANT:
                    _require(bool(smoothed_config), "smoothed policy binding missing")
                    result = legacy._apply_smoothed_limited_adjustment(
                        as_of=current_date,
                        base_weights=target_weights["limited_adjustment"],
                        previous_smoothed_weights=drifted,
                        smoothed_config=smoothed_config,
                        model_config=model_config,
                        variant_id=legacy.SMOOTHED_METHOD_TO_VARIANT[method],
                        regime_context=legacy._risk_capped_regime_context_for_return(
                            return_row, config
                        ),
                    )
                    after_weights = target_core._weights(
                        result["smoothed_weights"], field=f"{method} after"
                    )
                    smoothing_events = _records(result.get("smoothing_events"))
                    lag_events = _records(result.get("lag_events"))
                else:
                    after_weights = dict(target)
                turnover = legacy._turnover(drifted, after_weights)
            cost = turnover * total_cost_bps / 10000.0
            _require(_finite(turnover) and 0.0 <= turnover <= 2.0, "backfill turnover invalid")
            _require(_finite(cost) and 0.0 <= cost < 1.0, "backfill cost invalid")
            net_factor = (1.0 + gross_return) * (1.0 - cost)
            net_return = net_factor - 1.0
            portfolio_value = float(state["portfolio_value"]) * net_factor
            peak = max(float(state["peak_value"]), portfolio_value)
            drawdown = portfolio_value / peak - 1.0
            if is_rebalance:
                ledger.append(
                    {
                        "date": current_date.isoformat(),
                        "target_method": method,
                        "before_weights": drifted,
                        "target_weights": after_weights,
                        "after_weights": after_weights,
                        "deltas": legacy._weight_deltas(drifted, after_weights),
                        "turnover": round(turnover, 10),
                        "transaction_cost": round(cost, 10),
                        "total_cost_bps": round(total_cost_bps, 6),
                        "trade_type": "paper_no_trade"
                        if method == "no_trade_baseline"
                        else "paper_rebalance",
                        "cap_events": cap_events,
                        "reallocation_events": reallocation_events,
                        "cap_reason_summary": cap_reason_summary,
                        "smoothing_events": smoothing_events,
                        "lag_events": lag_events,
                        "broker_action_taken": False,
                        "order_ticket_generated": False,
                        **SYSTEM_TARGET_SAFETY,
                    }
                )
            state["weights"] = after_weights
            state["portfolio_value"] = portfolio_value
            state["peak_value"] = peak
            states.append(
                {
                    "date": current_date.isoformat(),
                    "target_method": method,
                    "weights": after_weights,
                    "portfolio_value": round(portfolio_value, 10),
                    "gross_daily_return": round(gross_return, 10),
                    "transaction_cost": round(cost, 10),
                    "daily_return": round(net_return, 10),
                    "drawdown": round(drawdown, 10),
                    "turnover": round(turnover, 10),
                    "rebalance_event": bool(is_rebalance and method != "no_trade_baseline"),
                    "calendar_rebalance_date": bool(is_rebalance),
                    "cap_event_count": len(cap_events),
                    "smoothing_event_count": len(smoothing_events),
                    "lag_event_count": len(lag_events),
                    **SYSTEM_TARGET_SAFETY,
                }
            )
    calendar = {
        "schema_version": 2,
        "backfill_id": backfill_id,
        "rebalance_frequency": date_range["rebalance_frequency"],
        "rebalance_day": date_range["rebalance_day"],
        "rebalance_dates": [item.isoformat() for item in sorted(rebalance_dates)],
        "rebalance_count": len(rebalance_dates),
        **SYSTEM_TARGET_SAFETY,
    }
    dq = _mapping(snapshot.get("data_quality"))
    data_quality = {
        "schema_version": 2,
        "backfill_id": backfill_id,
        "date_start": snapshot["actual_start_date"],
        "date_end": snapshot["actual_end_date"],
        "price_source_status": dq.get("status"),
        "excluded_incomplete_price_dates": list(
            snapshot.get("excluded_incomplete_price_dates", [])
        ),
        "excluded_incomplete_price_date_count": len(
            snapshot.get("excluded_incomplete_price_dates", [])
        ),
        "missing_symbols": [],
        "data_quality": dq.get("status"),
        "data_quality_checked_at": dq.get("checked_at"),
        "common_finite_date_policy": "ALL_REQUIRED_SYMBOLS_PRESENT_AND_FINITE",
        **SYSTEM_TARGET_SAFETY,
    }
    manifest = {
        "schema_version": 2,
        "report_type": "etf_dynamic_v3_paper_shadow_backfill_manifest",
        "backfill_id": backfill_id,
        "generated_at": snapshot["generated_at"],
        "status": "PASS",
        "market_regime": "ai_after_chatgpt",
        "requested_start_date": snapshot["requested_start_date"],
        "requested_end_date": snapshot["requested_end_date"],
        "date_start": snapshot["actual_start_date"],
        "date_end": snapshot["actual_end_date"],
        "rebalance_count": len(rebalance_dates),
        "tracked_methods": list(target_weights),
        "data_quality_status": dq.get("status"),
        "mode": "BACKTEST_SIMULATION",
        "not_pit_safe": True,
        "current_definition_replayed_historically": True,
        "missing_metrics_remain_null": True,
        "source_model_target_id": _mapping(snapshot.get("model_target_source")).get("artifact_id"),
        "source_model_target_as_of": _mapping(
            _mapping(_mapping(snapshot.get("model_target_source")).get("bundle")).get("json")
        )
        .get("model_target_manifest.json", {})
        .get("as_of"),
        "config_path": _text(
            _mapping(_mapping(snapshot.get("config_binding")).get("bundle")).get("source_dir")
        )
        + "/"
        + next(
            iter(
                _mapping(
                    _mapping(_mapping(snapshot.get("config_binding")).get("bundle")).get("files")
                )
            ),
            "",
        ),
        "total_cost_bps": round(total_cost_bps, 6),
        "input_snapshot_schema": PAPER_SHADOW_BACKFILL_SNAPSHOT_SCHEMA,
        "paper_shadow_backfill_input_snapshot_path": str(
            output_dir / "paper_shadow_backfill_input_snapshot.json"
        ),
        "paper_shadow_backfill_manifest_path": str(
            output_dir / "paper_shadow_backfill_manifest.json"
        ),
        "backfill_rebalance_calendar_path": str(output_dir / "backfill_rebalance_calendar.json"),
        "backfill_method_states_path": str(output_dir / "backfill_method_states.jsonl"),
        "backfill_trade_ledger_path": str(output_dir / "backfill_trade_ledger.jsonl"),
        "backfill_data_quality_path": str(output_dir / "backfill_data_quality.json"),
        "validate_data_quality_report_path": str(output_dir / "validate_data_quality_report.md"),
        "paper_shadow_backfill_report_path": str(output_dir / "paper_shadow_backfill_report.md"),
        **SYSTEM_TARGET_SAFETY,
    }
    views = {
        "paper_shadow_backfill_input_snapshot.json": _json_bytes(snapshot),
        "paper_shadow_backfill_manifest.json": _json_bytes(manifest),
        "backfill_rebalance_calendar.json": _json_bytes(calendar),
        "backfill_method_states.jsonl": _jsonl_bytes(states),
        "backfill_trade_ledger.jsonl": _jsonl_bytes(ledger),
        "backfill_data_quality.json": _json_bytes(data_quality),
        "validate_data_quality_report.md": target_core._text_bytes(_dq_report(snapshot)),
        "paper_shadow_backfill_report.md": target_core._text_bytes(
            _backfill_report(manifest, calendar, data_quality)
        ),
    }
    return views, {
        "manifest": manifest,
        "backfill_rebalance_calendar": calendar,
        "backfill_method_states": states,
        "backfill_trade_ledger": ledger,
        "backfill_data_quality": data_quality,
    }


def run_paper_shadow_backfill(
    *,
    config_path: Path = DEFAULT_PAPER_SHADOW_BACKFILL_CONFIG_PATH,
    output_dir: Path = DEFAULT_PAPER_SHADOW_BACKFILL_DIR,
    price_cache_path: Path | None = None,
    rates_cache_path: Path = DEFAULT_RATES_CACHE_PATH,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = _generated_at(generated_at)
    config = load_paper_shadow_backfill_config(config_path)
    source = _mapping(config.get("source"))
    prices = price_cache_path or _resolve(source.get("price_cache_path"), DEFAULT_PRICE_CACHE_PATH)
    rates = rates_cache_path
    snapshot = _backfill_snapshot(
        config_path=config_path,
        price_cache_path=prices,
        rates_cache_path=rates,
        generated=generated,
    )
    backfill_id = _stable_id(
        "paper-shadow-backfill-v2",
        _mapping(snapshot.get("model_target_source")).get("artifact_id"),
        snapshot["actual_start_date"],
        snapshot["actual_end_date"],
        generated.isoformat(),
    )
    root = _unique_dir(output_dir / backfill_id)
    views, payload = _backfill_views(snapshot, backfill_id=root.name, output_dir=root)
    _write_views_atomic(root, views)
    _update_latest_pointer(
        "latest_paper_shadow_backfill", root.name, root / "paper_shadow_backfill_manifest.json"
    )
    return {"backfill_id": root.name, "backfill_dir": root, **payload}


def _report_payload(root: Path, files: Mapping[str, str]) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    for key, name in files.items():
        payload[key] = (
            _read_jsonl(root / name) if name.endswith(".jsonl") else _read_json(root / name)
        )
    return payload


def paper_shadow_backfill_report_payload(
    *,
    backfill_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_PAPER_SHADOW_BACKFILL_DIR,
) -> dict[str, Any]:
    root = _artifact_dir_from_latest(
        output_dir=output_dir,
        artifact_id=None if latest else backfill_id,
        pointer_name="latest_paper_shadow_backfill",
    )
    manifest = _read_json(root / "paper_shadow_backfill_manifest.json")
    return {
        **manifest,
        **_report_payload(
            root,
            {
                "paper_shadow_backfill_input_snapshot": "paper_shadow_backfill_input_snapshot.json",
                "backfill_rebalance_calendar": "backfill_rebalance_calendar.json",
                "backfill_method_states": "backfill_method_states.jsonl",
                "backfill_trade_ledger": "backfill_trade_ledger.jsonl",
                "backfill_data_quality": "backfill_data_quality.json",
            },
        ),
        "backfill_dir": str(root),
    }


def _history_binding(
    *,
    kind: str,
    artifact_dir: Path,
    artifact_id: str,
    validation: Mapping[str, Any],
    json_views: Sequence[str],
    jsonl_views: Sequence[str] = (),
    text_views: Sequence[str] = (),
) -> dict[str, Any]:
    _require(validation.get("status") == "PASS", f"{kind} validation failed")
    return {
        "kind": kind,
        "artifact_id": artifact_id,
        "validation": dict(validation),
        "bundle": _operations_source_bundle(
            source_dir=artifact_dir,
            json_views=json_views,
            jsonl_views=jsonl_views,
            text_views=text_views,
        ),
    }


def _history_validator(kind: str) -> Callable[..., dict[str, Any]]:
    validators: dict[str, Callable[..., dict[str, Any]]] = {
        "paper_shadow_backfill": validate_paper_shadow_backfill_artifact,
        "paper_shadow_rolling_eval": validate_paper_shadow_rolling_eval_artifact,
        "paper_shadow_regime_review": validate_paper_shadow_regime_review_artifact,
        "paper_shadow_stability": validate_paper_shadow_stability_artifact,
    }
    _require(kind in validators, f"unknown history source kind: {kind}")
    return validators[kind]


def _validate_history_binding(binding: Mapping[str, Any]) -> list[str]:
    errors = _validate_operations_source_bundle(_mapping(binding.get("bundle")))
    try:
        kind = _text(binding.get("kind"))
        artifact_id = _text(binding.get("artifact_id"))
        source_dir = Path(_text(_mapping(binding.get("bundle")).get("source_dir")))
        validator = _history_validator(kind)
        key = {
            "paper_shadow_backfill": "backfill_id",
            "paper_shadow_rolling_eval": "rolling_eval_id",
            "paper_shadow_regime_review": "regime_review_id",
            "paper_shadow_stability": "stability_id",
        }[kind]
        actual = validator(**{key: artifact_id, "output_dir": source_dir.parent})
        if actual != _mapping(binding.get("validation")):
            errors.append(f"{kind} source validation drift")
    except Exception as exc:  # noqa: BLE001
        errors.append(str(exc))
    return errors


def _backfill_binding(backfill_id: str, backfill_dir: Path) -> dict[str, Any]:
    validation = validate_paper_shadow_backfill_artifact(
        backfill_id=backfill_id, output_dir=backfill_dir
    )
    return _history_binding(
        kind="paper_shadow_backfill",
        artifact_dir=backfill_dir / backfill_id,
        artifact_id=backfill_id,
        validation=validation,
        json_views=[
            "paper_shadow_backfill_input_snapshot.json",
            "paper_shadow_backfill_manifest.json",
            "backfill_rebalance_calendar.json",
            "backfill_data_quality.json",
        ],
        jsonl_views=["backfill_method_states.jsonl", "backfill_trade_ledger.jsonl"],
        text_views=["paper_shadow_backfill_report.md", "validate_data_quality_report.md"],
    )


def _validate_backfill_snapshot(snapshot: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    try:
        _require(
            snapshot.get("schema_version") == PAPER_SHADOW_BACKFILL_SNAPSHOT_SCHEMA,
            "backfill snapshot schema invalid",
        )
        for binding in (
            snapshot.get("config_binding"),
            snapshot.get("paper_config_binding"),
            *_records(snapshot.get("model_config_bindings")),
        ):
            errors.extend(target_core._validate_config_binding(_mapping(binding)))
        errors.extend(
            target_core._validate_artifact_binding(_mapping(snapshot.get("model_target_source")))
        )
        for binding in _records(snapshot.get("cache_bindings")):
            errors.extend(target_core._validate_cache_binding(binding))
        generated = _generated_at(
            target_core._datetime(snapshot.get("generated_at"), field="backfill generated_at")
        )
        selection = _mapping(snapshot.get("model_target_selection"))
        selected, _ = target_core._select_model_target(
            Path(_text(selection.get("root"))), generated=generated
        )
        _require(
            selected.name == selection.get("artifact_id"), "model target semantic selection drift"
        )
        config_path = target_core._binding_path(_mapping(snapshot.get("config_binding")))
        prices = next(
            Path(_text(row.get("path")))
            for row in _records(snapshot.get("cache_bindings"))
            if row.get("kind") == "prices"
        )
        rates = next(
            Path(_text(row.get("path")))
            for row in _records(snapshot.get("cache_bindings"))
            if row.get("kind") == "rates"
        )
        rebuilt = _backfill_snapshot(
            config_path=config_path,
            price_cache_path=prices,
            rates_cache_path=rates,
            generated=generated,
        )
        rebuilt_quality = _mapping(rebuilt.get("data_quality"))
        recorded_quality = _mapping(snapshot.get("data_quality"))
        # The quality gate records its wall-clock execution time. Revalidation
        # must prove the live status/counts again without requiring the second
        # run to reproduce the first run's clock value byte-for-byte.
        rebuilt_quality["checked_at"] = recorded_quality.get("checked_at")
        rebuilt["data_quality"] = rebuilt_quality
        if rebuilt != snapshot:
            errors.append("backfill input snapshot drift")
    except Exception as exc:  # noqa: BLE001
        errors.append(str(exc))
    return errors


def _view_errors(root: Path, views: Mapping[str, bytes]) -> list[str]:
    return [name for name, payload in views.items() if not _file_bytes_match(root / name, payload)]


def validate_paper_shadow_backfill_artifact(
    *,
    backfill_id: str,
    output_dir: Path = DEFAULT_PAPER_SHADOW_BACKFILL_DIR,
) -> dict[str, Any]:
    root = output_dir / backfill_id
    checks: list[dict[str, Any]] = []
    try:
        snapshot = _read_json(root / "paper_shadow_backfill_input_snapshot.json")
        errors = _validate_backfill_snapshot(snapshot)
        views, _ = _backfill_views(snapshot, backfill_id=backfill_id, output_dir=root)
        drift = _view_errors(root, views)
        checks.extend(
            [
                _check("snapshot_and_live_sources", not errors, "; ".join(errors)),
                _check("content_derived_views", not drift, ",".join(drift)),
                _check(
                    "backfill_identity",
                    _read_json(root / "paper_shadow_backfill_manifest.json").get("backfill_id")
                    == backfill_id,
                    "",
                ),
            ]
        )
    except Exception as exc:  # noqa: BLE001
        checks.append(_check("backfill_rebuild", False, str(exc)))
    return _validation_payload(
        "etf_dynamic_v3_paper_shadow_backfill_validation",
        backfill_id,
        checks,
        artifact_id_key="backfill_id",
    )


def _state_rows(binding: Mapping[str, Any]) -> list[dict[str, Any]]:
    rows = _records(
        _mapping(_mapping(binding.get("bundle")).get("jsonl")).get("backfill_method_states.jsonl")
    )
    identities: set[tuple[str, str]] = set()
    methods: set[str] = set()
    for row in rows:
        identity = (_text(row.get("date")), _text(row.get("target_method")))
        _require(all(identity) and identity not in identities, "backfill state identity duplicate")
        identities.add(identity)
        methods.add(identity[1])
        _date(identity[0], field="backfill state date")
        target_core._weights(row.get("weights"), field=f"state {identity}")
        for field in ("portfolio_value", "daily_return", "drawdown", "turnover"):
            _require(_finite(row.get(field)), f"backfill state {field} invalid")
    _require(bool(rows) and bool(methods), "backfill states empty")
    return rows


def _window_inventory(states: Sequence[Mapping[str, Any]], minimum: int) -> list[dict[str, Any]]:
    dates = sorted({_date(row.get("date"), field="state date") for row in states})
    _require(bool(dates), "rolling dates empty")
    windows: list[dict[str, Any]] = []

    def add(window_id: str, window_type: str, selected: Sequence[date]) -> None:
        if not selected:
            return
        windows.append(
            {
                "window_id": window_id,
                "window_type": window_type,
                "start_date": selected[0].isoformat(),
                "end_date": selected[-1].isoformat(),
                "observation_count": len(selected),
                "status": "PASS" if len(selected) >= minimum else "INSUFFICIENT_DATA",
            }
        )

    add(f"full_{dates[0]}_{dates[-1]}", "full", dates)
    for year in sorted({item.year for item in dates}):
        selected = [item for item in dates if item.year == year]
        add(f"yearly_{year}", "yearly", selected)
    month_starts = sorted({date(item.year, item.month, 1) for item in dates})
    for months in (3, 6, 12):
        for start in month_starts:
            end = (pd.Timestamp(start) + pd.DateOffset(months=months) - pd.Timedelta(days=1)).date()
            selected = [item for item in dates if start <= item <= end]
            if selected and selected[-1] <= dates[-1]:
                add(
                    f"rolling_{months}m_{selected[0]:%Y_%m}_{selected[-1]:%Y_%m}",
                    f"rolling_{months}m",
                    selected,
                )
    identities = [row["window_id"] for row in windows]
    _require(len(identities) == len(set(identities)), "rolling window identity duplicate")
    return windows


def _path_metrics(rows: Sequence[Mapping[str, Any]], minimum: int) -> dict[str, Any]:
    ordered = sorted(rows, key=lambda row: _text(row.get("date")))
    count = len(ordered)
    turnover = round(sum(float(row["turnover"]) for row in ordered), 10)
    if count < minimum:
        return {
            "total_return": None,
            "annualized_return": None,
            "max_drawdown": None,
            "realized_volatility": None,
            "turnover": turnover,
            "risk_adjusted_return_to_volatility": None,
            "observation_count": count,
            "status": "INSUFFICIENT_DATA",
        }
    returns = [float(row["daily_return"]) for row in ordered]
    _require(all(value > -1.0 for value in returns), "rolling daily return invalid")
    cumulative = 1.0
    peak = 1.0
    drawdowns: list[float] = []
    for value in returns:
        cumulative *= 1.0 + value
        peak = max(peak, cumulative)
        drawdowns.append(cumulative / peak - 1.0)
    total_return = cumulative - 1.0
    annualized = legacy._annualized_return(total_return, count)
    volatility = legacy._stddev(returns) * math.sqrt(252.0)
    risk_adjusted = annualized / volatility if volatility > 0.0 else annualized
    return {
        "total_return": round(total_return, 10),
        "annualized_return": round(annualized, 10),
        "max_drawdown": round(min(drawdowns or [0.0]), 10),
        "realized_volatility": round(volatility, 10),
        "turnover": turnover,
        "risk_adjusted_return_to_volatility": round(risk_adjusted, 10),
        "observation_count": count,
        "status": "PASS",
    }


def _rank(rows: Sequence[dict[str, Any]], field: str, rank_field: str) -> None:
    eligible = [row for row in rows if row.get("status") == "PASS" and _finite(row.get(field))]
    ordered = sorted(eligible, key=lambda row: float(row[field]), reverse=True)
    for rank, row in enumerate(ordered, start=1):
        row[rank_field] = rank


def _rolling_metrics(
    states: Sequence[Mapping[str, Any]], windows: Sequence[Mapping[str, Any]], minimum: int
) -> list[dict[str, Any]]:
    methods = sorted({_text(row.get("target_method")) for row in states})
    metrics: list[dict[str, Any]] = []
    for window in windows:
        start = _date(window.get("start_date"), field="window start")
        end = _date(window.get("end_date"), field="window end")
        current: list[dict[str, Any]] = []
        for method in methods:
            rows = [
                row
                for row in states
                if row.get("target_method") == method
                and start <= _date(row.get("date"), field="state date") <= end
            ]
            current.append(
                {
                    "window_id": window["window_id"],
                    "window_type": window["window_type"],
                    "start_date": window["start_date"],
                    "end_date": window["end_date"],
                    "target_method": method,
                    **_path_metrics(rows, minimum),
                    "relative_to_static_baseline": None,
                    "relative_to_no_trade_baseline": None,
                    "rank_by_return": None,
                    "rank_by_drawdown": None,
                    "rank_by_risk_adjusted": None,
                    **SYSTEM_TARGET_SAFETY,
                }
            )
        baseline = next((row for row in current if row["target_method"] == "static_baseline"), {})
        no_trade = next((row for row in current if row["target_method"] == "no_trade_baseline"), {})
        for row in current:
            if _finite(row.get("total_return")) and _finite(baseline.get("total_return")):
                row["relative_to_static_baseline"] = round(
                    float(row["total_return"]) - float(baseline["total_return"]), 10
                )
            if _finite(row.get("total_return")) and _finite(no_trade.get("total_return")):
                row["relative_to_no_trade_baseline"] = round(
                    float(row["total_return"]) - float(no_trade["total_return"]), 10
                )
        _rank(current, "total_return", "rank_by_return")
        _rank(current, "max_drawdown", "rank_by_drawdown")
        _rank(current, "risk_adjusted_return_to_volatility", "rank_by_risk_adjusted")
        metrics.extend(current)
    return metrics


def _rank_stability(
    metrics: Sequence[Mapping[str, Any]], config: Mapping[str, Any]
) -> dict[str, Any]:
    methods = sorted({_text(row.get("target_method")) for row in metrics})
    method_count = len(methods)
    policy = _mapping(_mapping(config.get("evaluation")).get("rank_stability"))
    top_n = min(int(policy["top_n"]), method_count)
    rows: list[dict[str, Any]] = []
    for method in methods:
        selected = [
            row
            for row in metrics
            if row.get("target_method") == method and isinstance(row.get("rank_by_return"), int)
        ]
        if not selected:
            rows.append(
                {
                    "target_method": method,
                    "avg_rank_return": None,
                    "avg_rank_drawdown": None,
                    "avg_rank_risk_adjusted": None,
                    "top_n": top_n,
                    "top_n_frequency": None,
                    "bottom_n_frequency": None,
                    "eligible_window_count": 0,
                    "rank_stability_status": "INSUFFICIENT_DATA",
                    **SYSTEM_TARGET_SAFETY,
                }
            )
            continue
        avg_return = sum(int(row["rank_by_return"]) for row in selected) / len(selected)
        avg_drawdown = sum(int(row["rank_by_drawdown"]) for row in selected) / len(selected)
        avg_risk = sum(int(row["rank_by_risk_adjusted"]) for row in selected) / len(selected)
        top_frequency = sum(1 for row in selected if int(row["rank_by_return"]) <= top_n) / len(
            selected
        )
        bottom_frequency = sum(
            1 for row in selected if int(row["rank_by_return"]) > method_count - top_n
        ) / len(selected)
        if top_frequency >= float(policy["stable_top_frequency_min"]) and bottom_frequency <= float(
            policy["stable_bottom_frequency_max"]
        ):
            status = "STABLE"
        elif bottom_frequency >= float(policy["unstable_bottom_frequency_min"]):
            status = "UNSTABLE"
        else:
            status = "MIXED"
        rows.append(
            {
                "target_method": method,
                "avg_rank_return": round(avg_return, 6),
                "avg_rank_drawdown": round(avg_drawdown, 6),
                "avg_rank_risk_adjusted": round(avg_risk, 6),
                "top_n": top_n,
                "top_n_frequency": round(top_frequency, 6),
                "bottom_n_frequency": round(bottom_frequency, 6),
                "eligible_window_count": len(selected),
                "rank_stability_status": status,
                **SYSTEM_TARGET_SAFETY,
            }
        )
    return {"schema_version": 2, "methods": rows, "policy": policy, **SYSTEM_TARGET_SAFETY}


def _rolling_report(manifest: Mapping[str, Any], stability: Mapping[str, Any]) -> str:
    rows = _records(stability.get("methods"))
    eligible = [row for row in rows if _finite(row.get("avg_rank_return"))]
    best = (
        min(eligible, key=lambda row: float(row["avg_rank_return"]))["target_method"]
        if eligible
        else "INSUFFICIENT_DATA"
    )
    return "\n".join(
        [
            f"# Paper Shadow Rolling Evaluation {manifest.get('rolling_eval_id')}",
            "",
            f"- backfill_id: {manifest.get('backfill_id')}",
            f"- window_count: {manifest.get('window_count')}",
            f"- metric_row_count: {manifest.get('metric_row_count')}",
            f"- best_average_rank_method: {best}",
            "- insufficient_metrics_remain_null: true",
            "- broker_action_allowed: false",
            "- production_effect: none",
            "",
        ]
    )


def _rolling_views(
    snapshot: Mapping[str, Any], *, rolling_id: str, output_dir: Path
) -> tuple[dict[str, bytes], dict[str, Any]]:
    source = _mapping(snapshot.get("backfill_source"))
    bundle = _mapping(source.get("bundle"))
    states = _state_rows(source)
    config = _mapping(_mapping(snapshot.get("config_binding")).get("payload"))
    summary = _validate_backfill_config_payload(config)
    windows = _window_inventory(states, int(summary["minimum_observations"]))
    metrics = _rolling_metrics(states, windows, int(summary["minimum_observations"]))
    stability = _rank_stability(metrics, config)
    backfill_manifest = _mapping(
        _mapping(bundle.get("json")).get("paper_shadow_backfill_manifest.json")
    )
    manifest = {
        "schema_version": 2,
        "report_type": "etf_dynamic_v3_paper_shadow_rolling_eval_manifest",
        "rolling_eval_id": rolling_id,
        "backfill_id": source.get("artifact_id"),
        "generated_at": snapshot["generated_at"],
        "status": "PASS" if metrics else "FAIL",
        "window_count": len(windows),
        "metric_row_count": len(metrics),
        "market_regime": backfill_manifest.get("market_regime"),
        "input_snapshot_schema": ROLLING_EVAL_SNAPSHOT_SCHEMA,
        "rolling_eval_input_snapshot_path": str(output_dir / "rolling_eval_input_snapshot.json"),
        "rolling_eval_manifest_path": str(output_dir / "rolling_eval_manifest.json"),
        "rolling_window_inventory_path": str(output_dir / "rolling_window_inventory.json"),
        "rolling_method_metrics_path": str(output_dir / "rolling_method_metrics.jsonl"),
        "rolling_rank_stability_path": str(output_dir / "rolling_rank_stability.json"),
        "paper_shadow_rolling_eval_report_path": str(
            output_dir / "paper_shadow_rolling_eval_report.md"
        ),
        "missing_metrics_remain_null": True,
        **SYSTEM_TARGET_SAFETY,
    }
    inventory = {
        "schema_version": 2,
        "rolling_eval_id": rolling_id,
        "windows": windows,
        "window_count": len(windows),
        **SYSTEM_TARGET_SAFETY,
    }
    views = {
        "rolling_eval_input_snapshot.json": _json_bytes(snapshot),
        "rolling_eval_manifest.json": _json_bytes(manifest),
        "rolling_window_inventory.json": _json_bytes(inventory),
        "rolling_method_metrics.jsonl": _jsonl_bytes(metrics),
        "rolling_rank_stability.json": _json_bytes(stability),
        "paper_shadow_rolling_eval_report.md": target_core._text_bytes(
            _rolling_report(manifest, stability)
        ),
    }
    return views, {
        "manifest": manifest,
        "rolling_window_inventory": inventory,
        "rolling_method_metrics": metrics,
        "rolling_rank_stability": stability,
    }


def _downstream_snapshot(
    schema: str, generated: datetime, source: Mapping[str, Any]
) -> dict[str, Any]:
    source_snapshot = _mapping(
        _mapping(_mapping(source.get("bundle")).get("json")).get(
            "paper_shadow_backfill_input_snapshot.json"
        )
    )
    return {
        "schema_version": schema,
        "generated_at": generated.isoformat(),
        "backfill_source": source,
        "config_binding": source_snapshot.get("config_binding"),
        "source_backfill_generated_at": _mapping(
            _mapping(_mapping(source.get("bundle")).get("json")).get(
                "paper_shadow_backfill_manifest.json"
            )
        ).get("generated_at"),
        "same_backfill_lineage_required": True,
        "missing_metrics_remain_null": True,
        **SYSTEM_TARGET_SAFETY,
    }


def run_paper_shadow_rolling_eval(
    *,
    backfill_id: str,
    backfill_dir: Path = DEFAULT_PAPER_SHADOW_BACKFILL_DIR,
    output_dir: Path = DEFAULT_PAPER_SHADOW_ROLLING_EVAL_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = _generated_at(generated_at)
    source = _backfill_binding(backfill_id, backfill_dir)
    source_generated = target_core._datetime(
        _mapping(
            _mapping(_mapping(source.get("bundle")).get("json")).get(
                "paper_shadow_backfill_manifest.json"
            )
        ).get("generated_at"),
        field="backfill generated_at",
    )
    _require(source_generated <= generated, "rolling generated before backfill")
    snapshot = _downstream_snapshot(ROLLING_EVAL_SNAPSHOT_SCHEMA, generated, source)
    rolling_id = _stable_id("paper-shadow-rolling-eval-v2", backfill_id, generated.isoformat())
    root = _unique_dir(output_dir / rolling_id)
    views, payload = _rolling_views(snapshot, rolling_id=root.name, output_dir=root)
    _write_views_atomic(root, views)
    _update_latest_pointer(
        "latest_paper_shadow_rolling_eval", root.name, root / "rolling_eval_manifest.json"
    )
    return {"rolling_eval_id": root.name, "rolling_eval_dir": root, **payload}


def paper_shadow_rolling_eval_report_payload(
    *,
    rolling_eval_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_PAPER_SHADOW_ROLLING_EVAL_DIR,
) -> dict[str, Any]:
    root = _artifact_dir_from_latest(
        output_dir=output_dir,
        artifact_id=None if latest else rolling_eval_id,
        pointer_name="latest_paper_shadow_rolling_eval",
    )
    return {
        **_read_json(root / "rolling_eval_manifest.json"),
        **_report_payload(
            root,
            {
                "rolling_eval_input_snapshot": "rolling_eval_input_snapshot.json",
                "rolling_window_inventory": "rolling_window_inventory.json",
                "rolling_method_metrics": "rolling_method_metrics.jsonl",
                "rolling_rank_stability": "rolling_rank_stability.json",
            },
        ),
        "rolling_eval_dir": str(root),
    }


def _validate_downstream_snapshot(snapshot: Mapping[str, Any], schema: str) -> list[str]:
    errors: list[str] = []
    try:
        _require(snapshot.get("schema_version") == schema, "downstream snapshot schema invalid")
        errors.extend(_validate_history_binding(_mapping(snapshot.get("backfill_source"))))
        errors.extend(
            target_core._validate_config_binding(_mapping(snapshot.get("config_binding")))
        )
        source = _mapping(snapshot.get("backfill_source"))
        bundle = _mapping(source.get("bundle"))
        source_manifest = _mapping(
            _mapping(bundle.get("json")).get("paper_shadow_backfill_manifest.json")
        )
        generated = target_core._datetime(
            snapshot.get("generated_at"), field="downstream generated_at"
        )
        source_generated = target_core._datetime(
            source_manifest.get("generated_at"), field="backfill generated_at"
        )
        _require(source_generated <= generated, "downstream chronology invalid")
    except Exception as exc:  # noqa: BLE001
        errors.append(str(exc))
    return errors


def validate_paper_shadow_rolling_eval_artifact(
    *, rolling_eval_id: str, output_dir: Path = DEFAULT_PAPER_SHADOW_ROLLING_EVAL_DIR
) -> dict[str, Any]:
    root = output_dir / rolling_eval_id
    checks: list[dict[str, Any]] = []
    try:
        snapshot = _read_json(root / "rolling_eval_input_snapshot.json")
        errors = _validate_downstream_snapshot(snapshot, ROLLING_EVAL_SNAPSHOT_SCHEMA)
        views, _ = _rolling_views(snapshot, rolling_id=rolling_eval_id, output_dir=root)
        drift = _view_errors(root, views)
        checks.extend(
            [
                _check("snapshot_and_backfill_source", not errors, "; ".join(errors)),
                _check("content_derived_views", not drift, ",".join(drift)),
            ]
        )
    except Exception as exc:  # noqa: BLE001
        checks.append(_check("rolling_rebuild", False, str(exc)))
    return _validation_payload(
        "etf_dynamic_v3_paper_shadow_rolling_eval_validation",
        rolling_eval_id,
        checks,
        artifact_id_key="rolling_eval_id",
    )


def _configured_regimes() -> tuple[str, ...]:
    return (
        "ai_trend",
        "tech_drawdown",
        "semiconductor_pullback",
        "risk_off",
        "sideways_choppy",
        "strong_recovery",
    )


def _regime_labels(snapshot: Mapping[str, Any], config: Mapping[str, Any]) -> list[dict[str, Any]]:
    source = _mapping(snapshot.get("backfill_source"))
    backfill_input = _mapping(
        _mapping(_mapping(source.get("bundle")).get("json")).get(
            "paper_shadow_backfill_input_snapshot.json"
        )
    )
    symbols = [_text(item) for item in backfill_input.get("expected_symbols", [])]
    returns = target_core._returns_from_rows(
        _records(backfill_input.get("price_rows")), symbols=symbols
    )
    policy = _mapping(config.get("regime_policy"))
    symbol_fields = {
        name: _text(policy.get(f"{name}_symbol"))
        for name in (
            "risk_off",
            "tech_drawdown",
            "semiconductor_pullback",
            "ai_trend",
            "strong_recovery",
        )
    }
    _require(
        not (set(symbol_fields.values()) - set(returns.columns)),
        "regime policy symbol absent from prices",
    )
    labels: list[dict[str, Any]] = []
    for timestamp, row in returns.iterrows():
        values = {name: float(row[symbol]) for name, symbol in symbol_fields.items()}
        if values["risk_off"] <= float(policy["risk_off_return_threshold"]):
            label = "risk_off"
        elif values["semiconductor_pullback"] <= float(
            policy["semiconductor_pullback_return_threshold"]
        ):
            label = "semiconductor_pullback"
        elif values["tech_drawdown"] <= float(policy["tech_drawdown_return_threshold"]):
            label = "tech_drawdown"
        elif values["strong_recovery"] >= float(policy["strong_recovery_return_threshold"]):
            label = "strong_recovery"
        elif values["ai_trend"] >= float(policy["ai_trend_return_threshold"]):
            label = "ai_trend"
        else:
            label = "sideways_choppy"
        labels.append(
            {
                "date": timestamp.date().isoformat(),
                "regime": label,
                "signal_returns": {name: round(value, 10) for name, value in values.items()},
            }
        )
    return labels


def _regime_metrics(
    states: Sequence[Mapping[str, Any]], labels: Sequence[Mapping[str, Any]], minimum: int
) -> list[dict[str, Any]]:
    by_regime = {
        regime: {_text(row.get("date")) for row in labels if row.get("regime") == regime}
        for regime in _configured_regimes()
    }
    methods = sorted({_text(row.get("target_method")) for row in states})
    metrics: list[dict[str, Any]] = []
    for regime in _configured_regimes():
        current: list[dict[str, Any]] = []
        dates = by_regime[regime]
        for method in methods:
            rows = [
                row
                for row in states
                if row.get("target_method") == method and row.get("date") in dates
            ]
            result = _path_metrics(rows, minimum)
            current.append(
                {
                    "regime": regime,
                    "target_method": method,
                    **result,
                    "relative_to_no_trade": None,
                    **SYSTEM_TARGET_SAFETY,
                }
            )
        no_trade = next((row for row in current if row["target_method"] == "no_trade_baseline"), {})
        for row in current:
            if _finite(row.get("total_return")) and _finite(no_trade.get("total_return")):
                row["relative_to_no_trade"] = round(
                    float(row["total_return"]) - float(no_trade["total_return"]), 10
                )
        metrics.extend(current)
    return metrics


def _best_available(rows: Sequence[Mapping[str, Any]], field: str) -> str | None:
    eligible = [row for row in rows if row.get("status") == "PASS" and _finite(row.get(field))]
    return (
        _text(max(eligible, key=lambda row: float(row[field])).get("target_method"))
        if eligible
        else None
    )


def _regime_summary(metrics: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    regimes: list[dict[str, Any]] = []
    for regime in _configured_regimes():
        rows = [row for row in metrics if row.get("regime") == regime]
        available = [row for row in rows if row.get("status") == "PASS"]
        regimes.append(
            {
                "regime": regime,
                "sample_count": max(
                    (int(row.get("observation_count", 0)) for row in rows), default=0
                ),
                "status": "PASS" if available else "INSUFFICIENT_DATA",
                "best_return_method": _best_available(rows, "total_return"),
                "best_drawdown_method": _best_available(rows, "max_drawdown"),
                "best_risk_adjusted_method": _best_available(
                    rows, "risk_adjusted_return_to_volatility"
                ),
            }
        )
    pressure = {"risk_off", "tech_drawdown", "semiconductor_pullback"}
    evidence = [
        row
        for row in metrics
        if row.get("regime") in pressure
        and row.get("target_method") == "defensive_limited_adjustment"
        and row.get("status") == "PASS"
    ]
    if not evidence:
        defensive = "INSUFFICIENT_DATA"
    else:
        wins = sum(
            1
            for row in evidence
            if _finite(row.get("relative_to_no_trade"))
            and float(row["relative_to_no_trade"]) >= 0.0
        )
        defensive = "PASS" if wins == len(evidence) else "FAIL" if wins == 0 else "MIXED"
    return {
        "schema_version": 2,
        "regimes": regimes,
        "defensive_limited_adjustment_status": defensive,
        "missing_metrics_remain_null": True,
        **SYSTEM_TARGET_SAFETY,
    }


def _regime_report(manifest: Mapping[str, Any], summary: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            f"# Paper Shadow Regime Review {manifest.get('regime_review_id')}",
            "",
            f"- backfill_id: {manifest.get('backfill_id')}",
            "- defensive_limited_adjustment_status: "
            f"{summary.get('defensive_limited_adjustment_status')}",
            "- regime_classifier: reviewed_symbol_threshold_policy",
            "- missing_metrics_remain_null: true",
            "- no_auto_defensive_rule_approval: true",
            "- broker_action_allowed: false",
            "- production_effect: none",
            "",
        ]
    )


def _regime_views(
    snapshot: Mapping[str, Any], *, regime_id: str, output_dir: Path
) -> tuple[dict[str, bytes], dict[str, Any]]:
    source = _mapping(snapshot.get("backfill_source"))
    states = _state_rows(source)
    config = _mapping(_mapping(snapshot.get("config_binding")).get("payload"))
    minimum = int(_validate_backfill_config_payload(config)["regime_minimum"])
    labels = _regime_labels(snapshot, config)
    metrics = _regime_metrics(states, labels, minimum)
    summary = _regime_summary(metrics)
    inventory = {
        "schema_version": 2,
        "regime_review_id": regime_id,
        "regimes": [
            {"regime": regime, "sample_count": sum(1 for row in labels if row["regime"] == regime)}
            for regime in _configured_regimes()
        ],
        "classifier_policy": _mapping(config.get("regime_policy")),
        **SYSTEM_TARGET_SAFETY,
    }
    backfill_manifest = _mapping(
        _mapping(_mapping(source.get("bundle")).get("json")).get(
            "paper_shadow_backfill_manifest.json"
        )
    )
    manifest = {
        "schema_version": 2,
        "report_type": "etf_dynamic_v3_paper_shadow_regime_review_manifest",
        "regime_review_id": regime_id,
        "backfill_id": source.get("artifact_id"),
        "generated_at": snapshot["generated_at"],
        "status": "PASS" if metrics else "FAIL",
        "market_regime": backfill_manifest.get("market_regime"),
        "input_snapshot_schema": REGIME_REVIEW_SNAPSHOT_SCHEMA,
        "paper_shadow_regime_input_snapshot_path": str(
            output_dir / "paper_shadow_regime_input_snapshot.json"
        ),
        "paper_shadow_regime_manifest_path": str(output_dir / "paper_shadow_regime_manifest.json"),
        "regime_date_labels_path": str(output_dir / "regime_date_labels.jsonl"),
        "regime_window_inventory_path": str(output_dir / "regime_window_inventory.json"),
        "method_regime_metrics_path": str(output_dir / "method_regime_metrics.jsonl"),
        "regime_method_summary_path": str(output_dir / "regime_method_summary.json"),
        "paper_shadow_regime_review_report_path": str(
            output_dir / "paper_shadow_regime_review_report.md"
        ),
        "missing_metrics_remain_null": True,
        **SYSTEM_TARGET_SAFETY,
    }
    views = {
        "paper_shadow_regime_input_snapshot.json": _json_bytes(snapshot),
        "paper_shadow_regime_manifest.json": _json_bytes(manifest),
        "regime_date_labels.jsonl": _jsonl_bytes(labels),
        "regime_window_inventory.json": _json_bytes(inventory),
        "method_regime_metrics.jsonl": _jsonl_bytes(metrics),
        "regime_method_summary.json": _json_bytes(summary),
        "paper_shadow_regime_review_report.md": target_core._text_bytes(
            _regime_report(manifest, summary)
        ),
    }
    return views, {
        "manifest": manifest,
        "regime_date_labels": labels,
        "regime_window_inventory": inventory,
        "method_regime_metrics": metrics,
        "regime_method_summary": summary,
    }


def run_paper_shadow_regime_review(
    *,
    backfill_id: str,
    backfill_dir: Path = DEFAULT_PAPER_SHADOW_BACKFILL_DIR,
    output_dir: Path = DEFAULT_PAPER_SHADOW_REGIME_REVIEW_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = _generated_at(generated_at)
    source = _backfill_binding(backfill_id, backfill_dir)
    snapshot = _downstream_snapshot(REGIME_REVIEW_SNAPSHOT_SCHEMA, generated, source)
    errors = _validate_downstream_snapshot(snapshot, REGIME_REVIEW_SNAPSHOT_SCHEMA)
    _require(not errors, "; ".join(errors))
    regime_id = _stable_id("paper-shadow-regime-review-v2", backfill_id, generated.isoformat())
    root = _unique_dir(output_dir / regime_id)
    views, payload = _regime_views(snapshot, regime_id=root.name, output_dir=root)
    _write_views_atomic(root, views)
    _update_latest_pointer(
        "latest_paper_shadow_regime_review", root.name, root / "paper_shadow_regime_manifest.json"
    )
    return {"regime_review_id": root.name, "regime_review_dir": root, **payload}


def paper_shadow_regime_review_report_payload(
    *,
    regime_review_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_PAPER_SHADOW_REGIME_REVIEW_DIR,
) -> dict[str, Any]:
    root = _artifact_dir_from_latest(
        output_dir=output_dir,
        artifact_id=None if latest else regime_review_id,
        pointer_name="latest_paper_shadow_regime_review",
    )
    return {
        **_read_json(root / "paper_shadow_regime_manifest.json"),
        **_report_payload(
            root,
            {
                "paper_shadow_regime_input_snapshot": "paper_shadow_regime_input_snapshot.json",
                "regime_date_labels": "regime_date_labels.jsonl",
                "regime_window_inventory": "regime_window_inventory.json",
                "method_regime_metrics": "method_regime_metrics.jsonl",
                "regime_method_summary": "regime_method_summary.json",
            },
        ),
        "regime_review_dir": str(root),
    }


def validate_paper_shadow_regime_review_artifact(
    *, regime_review_id: str, output_dir: Path = DEFAULT_PAPER_SHADOW_REGIME_REVIEW_DIR
) -> dict[str, Any]:
    root = output_dir / regime_review_id
    checks: list[dict[str, Any]] = []
    try:
        snapshot = _read_json(root / "paper_shadow_regime_input_snapshot.json")
        errors = _validate_downstream_snapshot(snapshot, REGIME_REVIEW_SNAPSHOT_SCHEMA)
        views, _ = _regime_views(snapshot, regime_id=regime_review_id, output_dir=root)
        drift = _view_errors(root, views)
        checks.extend(
            [
                _check("snapshot_and_backfill_source", not errors, "; ".join(errors)),
                _check("content_derived_views", not drift, ",".join(drift)),
            ]
        )
    except Exception as exc:  # noqa: BLE001
        checks.append(_check("regime_rebuild", False, str(exc)))
    return _validation_payload(
        "etf_dynamic_v3_paper_shadow_regime_review_validation",
        regime_review_id,
        checks,
        artifact_id_key="regime_review_id",
    )


def _stability_diagnostics(
    states: Sequence[Mapping[str, Any]], config: Mapping[str, Any]
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    policy = _mapping(config.get("stability_policy"))
    minimum = int(_mapping(config.get("evaluation"))["min_observations_per_window"])
    methods = sorted({_text(row.get("target_method")) for row in states})
    metrics: list[dict[str, Any]] = []
    jumps: list[dict[str, Any]] = []
    turnover_rows: list[dict[str, Any]] = []
    for method in methods:
        rows = sorted(
            [row for row in states if row.get("target_method") == method],
            key=lambda row: _text(row.get("date")),
        )
        if len(rows) < minimum:
            metrics.append(
                {
                    "target_method": method,
                    "avg_daily_weight_change": None,
                    "max_daily_weight_change": None,
                    "avg_rebalance_turnover": None,
                    "max_rebalance_turnover": None,
                    "rebalance_count": 0,
                    "large_jump_count": 0,
                    "cash_weight_volatility": None,
                    "risk_asset_weight_volatility": None,
                    "stability_status": "INSUFFICIENT_DATA",
                    **SYSTEM_TARGET_SAFETY,
                }
            )
            turnover_rows.append(
                {
                    "target_method": method,
                    "total_turnover": None,
                    "annualized_turnover": None,
                    "turnover_status": "INSUFFICIENT_DATA",
                    "warning": [],
                    **SYSTEM_TARGET_SAFETY,
                }
            )
            continue
        changes: list[float] = []
        cash: list[float] = []
        risk: list[float] = []
        turnovers: list[float] = []
        previous: dict[str, float] | None = None
        for row in rows:
            weights = target_core._weights(row.get("weights"), field=f"stability {method}")
            cash.append(weights.get("CASH", 0.0))
            risk.append(sum(value for symbol, value in weights.items() if symbol != "CASH"))
            if row.get("rebalance_event") is True:
                turnovers.append(float(row["turnover"]))
            if previous is not None:
                deltas = legacy._weight_deltas(previous, weights)
                total_abs = sum(abs(value) for value in deltas.values())
                changes.append(total_abs)
                if total_abs >= float(policy["large_jump_threshold"]):
                    symbol, delta = max(deltas.items(), key=lambda item: abs(item[1]))
                    jumps.append(
                        {
                            "date": row["date"],
                            "target_method": method,
                            "total_abs_weight_change": round(total_abs, 10),
                            "largest_symbol_delta": {"symbol": symbol, "delta": round(delta, 10)},
                            "jump_reason": "target_method_rebalance"
                            if row.get("rebalance_event") is True
                            else "weight_drift",
                            "severity": "HIGH"
                            if total_abs >= float(policy["high_jump_threshold"])
                            else "MEDIUM",
                            "broker_action_taken": False,
                            **SYSTEM_TARGET_SAFETY,
                        }
                    )
            previous = weights
        avg_change = sum(changes) / len(changes)
        max_change = max(changes)
        if max_change <= float(policy["stable_max_daily_weight_change"]) and all(
            value < float(policy["large_jump_threshold"]) for value in changes
        ):
            status = "STABLE"
        elif max_change >= float(policy["unstable_max_daily_weight_change"]):
            status = "UNSTABLE"
        else:
            status = "MODERATE"
        total_turnover = sum(turnovers)
        annualized_turnover = total_turnover / (len(rows) / 252.0)
        if annualized_turnover >= float(policy["high_annualized_turnover"]):
            turnover_status = "HIGH"
        elif annualized_turnover >= float(policy["moderate_annualized_turnover"]):
            turnover_status = "MODERATE"
        else:
            turnover_status = "LOW"
        metrics.append(
            {
                "target_method": method,
                "avg_daily_weight_change": round(avg_change, 10),
                "max_daily_weight_change": round(max_change, 10),
                "avg_rebalance_turnover": round(sum(turnovers) / len(turnovers), 10)
                if turnovers
                else 0.0,
                "max_rebalance_turnover": round(max(turnovers), 10) if turnovers else 0.0,
                "rebalance_count": len(turnovers),
                "large_jump_count": sum(
                    1 for value in changes if value >= float(policy["large_jump_threshold"])
                ),
                "cash_weight_volatility": round(legacy._stddev(cash), 10),
                "risk_asset_weight_volatility": round(legacy._stddev(risk), 10),
                "stability_status": status,
                **SYSTEM_TARGET_SAFETY,
            }
        )
        turnover_rows.append(
            {
                "target_method": method,
                "total_turnover": round(total_turnover, 10),
                "annualized_turnover": round(annualized_turnover, 10),
                "turnover_status": turnover_status,
                "warning": ["high_turnover"] if turnover_status == "HIGH" else [],
                **SYSTEM_TARGET_SAFETY,
            }
        )
    return (
        metrics,
        jumps,
        {"schema_version": 2, "methods": turnover_rows, "policy": policy, **SYSTEM_TARGET_SAFETY},
    )


def _stability_report(manifest: Mapping[str, Any], metrics: Sequence[Mapping[str, Any]]) -> str:
    return "\n".join(
        [
            f"# Paper Shadow Stability Diagnostics {manifest.get('stability_id')}",
            "",
            f"- backfill_id: {manifest.get('backfill_id')}",
            f"- large_jump_count: {sum(int(row.get('large_jump_count', 0)) for row in metrics)}",
            "- missing_metrics_remain_null: true",
            "- broker_action_allowed: false",
            "- production_effect: none",
            "",
        ]
    )


def _stability_views(
    snapshot: Mapping[str, Any], *, stability_id: str, output_dir: Path
) -> tuple[dict[str, bytes], dict[str, Any]]:
    source = _mapping(snapshot.get("backfill_source"))
    states = _state_rows(source)
    config = _mapping(_mapping(snapshot.get("config_binding")).get("payload"))
    _validate_backfill_config_payload(config)
    metrics, jumps, turnover = _stability_diagnostics(states, config)
    manifest = {
        "schema_version": 2,
        "report_type": "etf_dynamic_v3_paper_shadow_stability_manifest",
        "stability_id": stability_id,
        "backfill_id": source.get("artifact_id"),
        "generated_at": snapshot["generated_at"],
        "status": "PASS" if metrics else "FAIL",
        "input_snapshot_schema": STABILITY_SNAPSHOT_SCHEMA,
        "paper_shadow_stability_input_snapshot_path": str(
            output_dir / "paper_shadow_stability_input_snapshot.json"
        ),
        "paper_shadow_stability_manifest_path": str(
            output_dir / "paper_shadow_stability_manifest.json"
        ),
        "method_stability_metrics_path": str(output_dir / "method_stability_metrics.jsonl"),
        "weight_path_jump_events_path": str(output_dir / "weight_path_jump_events.jsonl"),
        "turnover_diagnostics_path": str(output_dir / "turnover_diagnostics.json"),
        "paper_shadow_stability_report_path": str(output_dir / "paper_shadow_stability_report.md"),
        "missing_metrics_remain_null": True,
        **SYSTEM_TARGET_SAFETY,
    }
    views = {
        "paper_shadow_stability_input_snapshot.json": _json_bytes(snapshot),
        "paper_shadow_stability_manifest.json": _json_bytes(manifest),
        "method_stability_metrics.jsonl": _jsonl_bytes(metrics),
        "weight_path_jump_events.jsonl": _jsonl_bytes(jumps),
        "turnover_diagnostics.json": _json_bytes(turnover),
        "paper_shadow_stability_report.md": target_core._text_bytes(
            _stability_report(manifest, metrics)
        ),
    }
    return views, {
        "manifest": manifest,
        "method_stability_metrics": metrics,
        "weight_path_jump_events": jumps,
        "turnover_diagnostics": turnover,
    }


def run_paper_shadow_stability(
    *,
    backfill_id: str,
    backfill_dir: Path = DEFAULT_PAPER_SHADOW_BACKFILL_DIR,
    output_dir: Path = DEFAULT_PAPER_SHADOW_STABILITY_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = _generated_at(generated_at)
    source = _backfill_binding(backfill_id, backfill_dir)
    snapshot = _downstream_snapshot(STABILITY_SNAPSHOT_SCHEMA, generated, source)
    errors = _validate_downstream_snapshot(snapshot, STABILITY_SNAPSHOT_SCHEMA)
    _require(not errors, "; ".join(errors))
    stability_id = _stable_id("paper-shadow-stability-v2", backfill_id, generated.isoformat())
    root = _unique_dir(output_dir / stability_id)
    views, payload = _stability_views(snapshot, stability_id=root.name, output_dir=root)
    _write_views_atomic(root, views)
    _update_latest_pointer(
        "latest_paper_shadow_stability", root.name, root / "paper_shadow_stability_manifest.json"
    )
    return {"stability_id": root.name, "stability_dir": root, **payload}


def paper_shadow_stability_report_payload(
    *,
    stability_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_PAPER_SHADOW_STABILITY_DIR,
) -> dict[str, Any]:
    root = _artifact_dir_from_latest(
        output_dir=output_dir,
        artifact_id=None if latest else stability_id,
        pointer_name="latest_paper_shadow_stability",
    )
    return {
        **_read_json(root / "paper_shadow_stability_manifest.json"),
        **_report_payload(
            root,
            {
                "paper_shadow_stability_input_snapshot": (
                    "paper_shadow_stability_input_snapshot.json"
                ),
                "method_stability_metrics": "method_stability_metrics.jsonl",
                "weight_path_jump_events": "weight_path_jump_events.jsonl",
                "turnover_diagnostics": "turnover_diagnostics.json",
            },
        ),
        "stability_dir": str(root),
    }


def validate_paper_shadow_stability_artifact(
    *, stability_id: str, output_dir: Path = DEFAULT_PAPER_SHADOW_STABILITY_DIR
) -> dict[str, Any]:
    root = output_dir / stability_id
    checks: list[dict[str, Any]] = []
    try:
        snapshot = _read_json(root / "paper_shadow_stability_input_snapshot.json")
        errors = _validate_downstream_snapshot(snapshot, STABILITY_SNAPSHOT_SCHEMA)
        views, _ = _stability_views(snapshot, stability_id=stability_id, output_dir=root)
        drift = _view_errors(root, views)
        checks.extend(
            [
                _check("snapshot_and_backfill_source", not errors, "; ".join(errors)),
                _check("content_derived_views", not drift, ",".join(drift)),
            ]
        )
    except Exception as exc:  # noqa: BLE001
        checks.append(_check("stability_rebuild", False, str(exc)))
    return _validation_payload(
        "etf_dynamic_v3_paper_shadow_stability_validation",
        stability_id,
        checks,
        artifact_id_key="stability_id",
    )


def _component_rank_score(value: Any, method_count: int) -> float | None:
    if not _finite(value) or float(value) <= 0.0 or method_count <= 1:
        return None
    return max(0.0, min(1.0, 1.0 - (float(value) - 1.0) / (method_count - 1.0)))


def _selection_scorecard(snapshot: Mapping[str, Any], config: Mapping[str, Any]) -> dict[str, Any]:
    rolling = _mapping(snapshot.get("rolling_source"))
    regime = _mapping(snapshot.get("regime_source"))
    stability = _mapping(snapshot.get("stability_source"))
    rank_rows = _records(
        _mapping(_mapping(rolling.get("bundle")).get("json")).get("rolling_rank_stability.json")
    )
    # The bundle stores the full JSON object, not its methods list.
    rank_rows = _records(
        _mapping(
            _mapping(_mapping(rolling.get("bundle")).get("json")).get("rolling_rank_stability.json")
        ).get("methods")
    )
    regime_rows = _records(
        _mapping(
            _mapping(_mapping(regime.get("bundle")).get("json")).get("regime_method_summary.json")
        ).get("regimes")
    )
    stability_rows = _records(
        _mapping(_mapping(stability.get("bundle")).get("jsonl")).get(
            "method_stability_metrics.jsonl"
        )
    )
    turnover_rows = _records(
        _mapping(
            _mapping(_mapping(stability.get("bundle")).get("json")).get("turnover_diagnostics.json")
        ).get("methods")
    )
    methods = sorted(
        {_text(row.get("target_method")) for row in [*rank_rows, *stability_rows, *turnover_rows]}
    )
    method_count = len(methods)
    selection = _mapping(config.get("selection_policy"))
    weights = _mapping(selection.get("score_weights"))
    reference = set(_text(item) for item in selection.get("reference_only_methods", []))
    status_scores = _mapping(selection.get("stability_status_scores"))
    turnover_high = float(_mapping(config.get("stability_policy"))["high_annualized_turnover"])
    rows: list[dict[str, Any]] = []
    for method in methods:
        rank = next((row for row in rank_rows if row.get("target_method") == method), {})
        stable = next((row for row in stability_rows if row.get("target_method") == method), {})
        turnover = next((row for row in turnover_rows if row.get("target_method") == method), {})
        return_score = _component_rank_score(rank.get("avg_rank_return"), method_count)
        drawdown_score = _component_rank_score(rank.get("avg_rank_drawdown"), method_count)
        risk_score = _component_rank_score(rank.get("avg_rank_risk_adjusted"), method_count)
        eligible_regimes = [row for row in regime_rows if row.get("status") == "PASS"]
        regime_score = None
        if eligible_regimes:
            points = sum(
                1
                for row in eligible_regimes
                for field in (
                    "best_return_method",
                    "best_drawdown_method",
                    "best_risk_adjusted_method",
                )
                if row.get(field) == method
            )
            regime_score = points / (3.0 * len(eligible_regimes))
        stability_score = status_scores.get(_text(stable.get("stability_status")))
        turnover_penalty = (
            min(1.0, float(turnover["annualized_turnover"]) / turnover_high)
            if _finite(turnover.get("annualized_turnover"))
            else None
        )
        components = (
            return_score,
            drawdown_score,
            risk_score,
            regime_score,
            stability_score,
            turnover_penalty,
        )
        if not all(_finite(value) for value in components):
            overall = None
            status = "INSUFFICIENT_DATA"
        else:
            overall = (
                float(return_score) * float(weights["return"])
                + float(drawdown_score) * float(weights["drawdown"])
                + float(risk_score) * float(weights["risk_adjusted"])
                + float(regime_score) * float(weights["regime"])
                + float(stability_score) * float(weights["stability"])
                - float(turnover_penalty) * float(weights["turnover_penalty"])
            )
            if method in reference:
                status = "REFERENCE_ONLY"
            elif overall >= float(selection["continue_observation_score"]):
                status = "CONTINUE_OBSERVATION"
            elif overall >= float(selection["review_required_score"]):
                status = "REVIEW_REQUIRED"
            else:
                status = "NOT_RECOMMENDED"
        rows.append(
            {
                "target_method": method,
                "return_score": _optional(return_score, digits=6),
                "drawdown_score": _optional(drawdown_score, digits=6),
                "risk_adjusted_score": _optional(risk_score, digits=6),
                "regime_score": _optional(regime_score, digits=6),
                "eligible_regime_count": len(eligible_regimes),
                "stability_score": _optional(stability_score, digits=6),
                "turnover_penalty": _optional(turnover_penalty, digits=6),
                "overall_score": _optional(max(0.0, overall), digits=6)
                if overall is not None
                else None,
                "status": status,
                **SYSTEM_TARGET_SAFETY,
            }
        )
    return {
        "schema_version": 2,
        "methods": rows,
        "missing_components_remain_null": True,
        "reference_only_never_recommended": True,
        **SYSTEM_TARGET_SAFETY,
    }


def _selection_decision(
    scorecard: Mapping[str, Any], config: Mapping[str, Any], selection_id: str
) -> dict[str, Any]:
    rows = _records(scorecard.get("methods"))
    policy = _mapping(config.get("selection_policy"))
    reference = set(_text(item) for item in policy.get("reference_only_methods", []))
    eligible = [
        row
        for row in rows
        if row.get("status") in {"CONTINUE_OBSERVATION", "REVIEW_REQUIRED", "NOT_RECOMMENDED"}
        and row.get("target_method") not in reference
        and _finite(row.get("overall_score"))
    ]
    if not eligible:
        recommended: str | None = None
        decision_status = "INSUFFICIENT_DATA"
    else:
        best_score = max(float(row["overall_score"]) for row in eligible)
        preferred = [_text(item) for item in policy.get("preferred_method_order", [])]
        preferred_candidates = [
            row
            for method in preferred
            for row in eligible
            if row.get("target_method") == method
            and row.get("status") != "NOT_RECOMMENDED"
            and float(row["overall_score"])
            >= best_score - float(policy["preferred_method_score_tolerance"])
        ]
        selected = (
            preferred_candidates[0]
            if preferred_candidates
            else max(eligible, key=lambda row: float(row["overall_score"]))
        )
        recommended = _text(selected.get("target_method"))
        decision_status = (
            "CONTINUE_OBSERVATION"
            if selected.get("status") == "CONTINUE_OBSERVATION"
            else "REVIEW_REQUIRED"
        )
    secondary = [
        _text(row.get("target_method"))
        for row in sorted(eligible, key=lambda row: float(row["overall_score"]), reverse=True)
        if row.get("target_method") != recommended and row.get("status") != "NOT_RECOMMENDED"
    ][:2]
    return {
        "schema_version": 2,
        "selection_review_id": selection_id,
        "recommended_research_method": recommended,
        "secondary_research_methods": secondary,
        "reference_only_methods": sorted(reference),
        "not_recommended_methods": [
            _text(row.get("target_method"))
            for row in rows
            if row.get("status") == "NOT_RECOMMENDED"
        ],
        "decision_status": decision_status,
        "selection_basis": "REVIEWED_POLICY_SCORECARD",
        "performance_winner_claimed": False,
        "historical_simulation_only": True,
        "reason": (
            "Reviewed rolling, regime, stability and turnover evidence only; "
            "reference-only methods are excluded from recommendation and missing components "
            "are not scored."
        ),
        "next_action": "continue_paper_shadow_observation"
        if recommended
        else "collect_more_historical_evidence",
        **SYSTEM_TARGET_SAFETY,
    }


def _selection_report(manifest: Mapping[str, Any], decision: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            f"# System Target Method Selection Review {manifest.get('selection_review_id')}",
            "",
            f"- market_regime: {manifest.get('market_regime')}",
            f"- date_range: {manifest.get('date_start')} to {manifest.get('date_end')}",
            f"- recommended_research_method: {decision.get('recommended_research_method')}",
            f"- decision_status: {decision.get('decision_status')}",
            f"- selection_basis: {decision.get('selection_basis')}",
            "- reference_only_never_recommended: true",
            "- missing_components_remain_null: true",
            "- performance_winner_claimed: false",
            "- official_target_weights_allowed: false",
            "- broker_action_allowed: false",
            "- production_effect: none",
            "",
            _text(decision.get("reason")),
            "",
        ]
    )


def _owner_checklist(decision: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            f"# Owner Research Checklist {decision.get('selection_review_id')}",
            "",
            "- 是否接受当前research method继续观察？",
            "- 是否确认reference-only method未被推荐？",
            "- 是否确认历史模拟不是PIT-safe或production evidence？",
            "- 是否继续禁止official weights、order与broker？",
            "",
            f"- recommended_research_method: {decision.get('recommended_research_method')}",
            f"- decision_status: {decision.get('decision_status')}",
            "- broker_action_allowed: false",
            "- production_effect: none",
            "",
        ]
    )


def _reader_brief(decision: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "## Dynamic Rescue System Target Selection Review",
            "",
            f"- recommended_research_method: {decision.get('recommended_research_method')}",
            f"- secondary_methods: {', '.join(decision.get('secondary_research_methods', []))}",
            f"- reference_only_methods: {', '.join(decision.get('reference_only_methods', []))}",
            f"- decision_status: {decision.get('decision_status')}",
            "- historical_simulation_only: true",
            "- performance_winner_claimed: false",
            "- research_target_only: true",
            f"- next_action: {decision.get('next_action')}",
            "",
        ]
    )


def _selection_source(kind: str, artifact_id: str, root: Path) -> dict[str, Any]:
    if kind == "paper_shadow_rolling_eval":
        validation = validate_paper_shadow_rolling_eval_artifact(
            rolling_eval_id=artifact_id, output_dir=root
        )
        return _history_binding(
            kind=kind,
            artifact_dir=root / artifact_id,
            artifact_id=artifact_id,
            validation=validation,
            json_views=[
                "rolling_eval_input_snapshot.json",
                "rolling_eval_manifest.json",
                "rolling_window_inventory.json",
                "rolling_rank_stability.json",
            ],
            jsonl_views=["rolling_method_metrics.jsonl"],
            text_views=["paper_shadow_rolling_eval_report.md"],
        )
    if kind == "paper_shadow_regime_review":
        validation = validate_paper_shadow_regime_review_artifact(
            regime_review_id=artifact_id, output_dir=root
        )
        return _history_binding(
            kind=kind,
            artifact_dir=root / artifact_id,
            artifact_id=artifact_id,
            validation=validation,
            json_views=[
                "paper_shadow_regime_input_snapshot.json",
                "paper_shadow_regime_manifest.json",
                "regime_window_inventory.json",
                "regime_method_summary.json",
            ],
            jsonl_views=["regime_date_labels.jsonl", "method_regime_metrics.jsonl"],
            text_views=["paper_shadow_regime_review_report.md"],
        )
    validation = validate_paper_shadow_stability_artifact(stability_id=artifact_id, output_dir=root)
    return _history_binding(
        kind=kind,
        artifact_dir=root / artifact_id,
        artifact_id=artifact_id,
        validation=validation,
        json_views=[
            "paper_shadow_stability_input_snapshot.json",
            "paper_shadow_stability_manifest.json",
            "turnover_diagnostics.json",
        ],
        jsonl_views=["method_stability_metrics.jsonl", "weight_path_jump_events.jsonl"],
        text_views=["paper_shadow_stability_report.md"],
    )


def _selection_snapshot(
    *,
    backfill_id: str,
    rolling_eval_id: str,
    regime_review_id: str,
    stability_id: str,
    backfill_dir: Path,
    rolling_eval_dir: Path,
    regime_review_dir: Path,
    stability_dir: Path,
    generated: datetime,
) -> dict[str, Any]:
    backfill = _backfill_binding(backfill_id, backfill_dir)
    rolling = _selection_source("paper_shadow_rolling_eval", rolling_eval_id, rolling_eval_dir)
    regime = _selection_source("paper_shadow_regime_review", regime_review_id, regime_review_dir)
    stability = _selection_source("paper_shadow_stability", stability_id, stability_dir)
    for source in (rolling, regime, stability):
        source_manifest_name = {
            "paper_shadow_rolling_eval": "rolling_eval_manifest.json",
            "paper_shadow_regime_review": "paper_shadow_regime_manifest.json",
            "paper_shadow_stability": "paper_shadow_stability_manifest.json",
        }[source["kind"]]
        manifest = _mapping(
            _mapping(_mapping(source.get("bundle")).get("json")).get(source_manifest_name)
        )
        _require(
            manifest.get("backfill_id") == backfill_id, "selection source cross-backfill lineage"
        )
        _require(
            target_core._datetime(
                manifest.get("generated_at"), field="selection source generated_at"
            )
            <= generated,
            "selection source generated after review",
        )
    backfill_input = _mapping(
        _mapping(_mapping(backfill.get("bundle")).get("json")).get(
            "paper_shadow_backfill_input_snapshot.json"
        )
    )
    return {
        "schema_version": SELECTION_REVIEW_SNAPSHOT_SCHEMA,
        "generated_at": generated.isoformat(),
        "backfill_source": backfill,
        "rolling_source": rolling,
        "regime_source": regime,
        "stability_source": stability,
        "config_binding": backfill_input.get("config_binding"),
        "exact_same_backfill_lineage_required": True,
        "reference_only_never_recommended": True,
        "missing_components_remain_null": True,
        **SYSTEM_TARGET_SAFETY,
    }


def _selection_views(
    snapshot: Mapping[str, Any], *, selection_id: str, output_dir: Path
) -> tuple[dict[str, bytes], dict[str, Any]]:
    config = _mapping(_mapping(snapshot.get("config_binding")).get("payload"))
    _validate_backfill_config_payload(config)
    scorecard = _selection_scorecard(snapshot, config)
    decision = _selection_decision(scorecard, config, selection_id)
    backfill_manifest = _mapping(
        _mapping(_mapping(_mapping(snapshot.get("backfill_source")).get("bundle")).get("json")).get(
            "paper_shadow_backfill_manifest.json"
        )
    )
    manifest = {
        "schema_version": 2,
        "report_type": "etf_dynamic_v3_system_target_selection_manifest",
        "selection_review_id": selection_id,
        "backfill_id": _mapping(snapshot.get("backfill_source")).get("artifact_id"),
        "rolling_eval_id": _mapping(snapshot.get("rolling_source")).get("artifact_id"),
        "regime_review_id": _mapping(snapshot.get("regime_source")).get("artifact_id"),
        "stability_id": _mapping(snapshot.get("stability_source")).get("artifact_id"),
        "generated_at": snapshot["generated_at"],
        "status": "PASS",
        "market_regime": backfill_manifest.get("market_regime"),
        "date_start": backfill_manifest.get("date_start"),
        "date_end": backfill_manifest.get("date_end"),
        "data_quality_status": backfill_manifest.get("data_quality_status"),
        "input_snapshot_schema": SELECTION_REVIEW_SNAPSHOT_SCHEMA,
        "system_target_selection_input_snapshot_path": str(
            output_dir / "system_target_selection_input_snapshot.json"
        ),
        "system_target_selection_manifest_path": str(
            output_dir / "system_target_selection_manifest.json"
        ),
        "target_method_scorecard_path": str(output_dir / "target_method_scorecard.json"),
        "selection_decision_path": str(output_dir / "selection_decision.json"),
        "owner_research_checklist_path": str(output_dir / "owner_research_checklist.md"),
        "system_target_selection_review_report_path": str(
            output_dir / "system_target_selection_review_report.md"
        ),
        "reader_brief_section_path": str(output_dir / "reader_brief_section.md"),
        "missing_components_remain_null": True,
        **SYSTEM_TARGET_SAFETY,
    }
    views = {
        "system_target_selection_input_snapshot.json": _json_bytes(snapshot),
        "system_target_selection_manifest.json": _json_bytes(manifest),
        "target_method_scorecard.json": _json_bytes(scorecard),
        "selection_decision.json": _json_bytes(decision),
        "owner_research_checklist.md": target_core._text_bytes(_owner_checklist(decision)),
        "system_target_selection_review_report.md": target_core._text_bytes(
            _selection_report(manifest, decision)
        ),
        "reader_brief_section.md": target_core._text_bytes(_reader_brief(decision)),
    }
    return views, {
        "manifest": manifest,
        "target_method_scorecard": scorecard,
        "selection_decision": decision,
    }


def run_system_target_selection_review(
    *,
    backfill_id: str,
    rolling_eval_id: str,
    regime_review_id: str,
    stability_id: str,
    backfill_dir: Path = DEFAULT_PAPER_SHADOW_BACKFILL_DIR,
    rolling_eval_dir: Path = DEFAULT_PAPER_SHADOW_ROLLING_EVAL_DIR,
    regime_review_dir: Path = DEFAULT_PAPER_SHADOW_REGIME_REVIEW_DIR,
    stability_dir: Path = DEFAULT_PAPER_SHADOW_STABILITY_DIR,
    output_dir: Path = DEFAULT_SYSTEM_TARGET_SELECTION_REVIEW_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = _generated_at(generated_at)
    snapshot = _selection_snapshot(
        backfill_id=backfill_id,
        rolling_eval_id=rolling_eval_id,
        regime_review_id=regime_review_id,
        stability_id=stability_id,
        backfill_dir=backfill_dir,
        rolling_eval_dir=rolling_eval_dir,
        regime_review_dir=regime_review_dir,
        stability_dir=stability_dir,
        generated=generated,
    )
    selection_id = _stable_id(
        "system-target-selection-review-v2",
        backfill_id,
        rolling_eval_id,
        regime_review_id,
        stability_id,
        generated.isoformat(),
    )
    root = _unique_dir(output_dir / selection_id)
    views, payload = _selection_views(snapshot, selection_id=root.name, output_dir=root)
    _write_views_atomic(root, views)
    _update_latest_pointer(
        "latest_system_target_selection_review",
        root.name,
        root / "system_target_selection_manifest.json",
    )
    return {"selection_review_id": root.name, "selection_review_dir": root, **payload}


def system_target_selection_review_report_payload(
    *,
    selection_review_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_SYSTEM_TARGET_SELECTION_REVIEW_DIR,
) -> dict[str, Any]:
    root = _artifact_dir_from_latest(
        output_dir=output_dir,
        artifact_id=None if latest else selection_review_id,
        pointer_name="latest_system_target_selection_review",
    )
    return {
        **_read_json(root / "system_target_selection_manifest.json"),
        **_report_payload(
            root,
            {
                "system_target_selection_input_snapshot": (
                    "system_target_selection_input_snapshot.json"
                ),
                "target_method_scorecard": "target_method_scorecard.json",
                "selection_decision": "selection_decision.json",
            },
        ),
        "selection_review_dir": str(root),
    }


def _validate_selection_snapshot(snapshot: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    try:
        _require(
            snapshot.get("schema_version") == SELECTION_REVIEW_SNAPSHOT_SCHEMA,
            "selection snapshot schema invalid",
        )
        errors.extend(_validate_history_binding(_mapping(snapshot.get("backfill_source"))))
        for field in ("rolling_source", "regime_source", "stability_source"):
            errors.extend(_validate_history_binding(_mapping(snapshot.get(field))))
        errors.extend(
            target_core._validate_config_binding(_mapping(snapshot.get("config_binding")))
        )
        backfill_id = _mapping(snapshot.get("backfill_source")).get("artifact_id")
        generated = target_core._datetime(
            snapshot.get("generated_at"), field="selection generated_at"
        )
        for field, manifest_name in (
            ("rolling_source", "rolling_eval_manifest.json"),
            ("regime_source", "paper_shadow_regime_manifest.json"),
            ("stability_source", "paper_shadow_stability_manifest.json"),
        ):
            manifest = _mapping(
                _mapping(_mapping(_mapping(snapshot.get(field)).get("bundle")).get("json")).get(
                    manifest_name
                )
            )
            _require(manifest.get("backfill_id") == backfill_id, "selection source cross-backfill")
            _require(
                target_core._datetime(manifest.get("generated_at"), field="source generated_at")
                <= generated,
                "selection chronology invalid",
            )
    except Exception as exc:  # noqa: BLE001
        errors.append(str(exc))
    return errors


def validate_system_target_selection_review_artifact(
    *, selection_review_id: str, output_dir: Path = DEFAULT_SYSTEM_TARGET_SELECTION_REVIEW_DIR
) -> dict[str, Any]:
    root = output_dir / selection_review_id
    checks: list[dict[str, Any]] = []
    try:
        snapshot = _read_json(root / "system_target_selection_input_snapshot.json")
        errors = _validate_selection_snapshot(snapshot)
        views, _ = _selection_views(snapshot, selection_id=selection_review_id, output_dir=root)
        drift = _view_errors(root, views)
        decision = _read_json(root / "selection_decision.json")
        checks.extend(
            [
                _check("snapshot_and_exact_lineage", not errors, "; ".join(errors)),
                _check("content_derived_views", not drift, ",".join(drift)),
                _check(
                    "reference_only_not_recommended",
                    decision.get("recommended_research_method")
                    not in set(decision.get("reference_only_methods", [])),
                    "",
                ),
            ]
        )
    except Exception as exc:  # noqa: BLE001
        checks.append(_check("selection_rebuild", False, str(exc)))
    return _validation_payload(
        "etf_dynamic_v3_system_target_selection_review_validation",
        selection_review_id,
        checks,
        artifact_id_key="selection_review_id",
    )
