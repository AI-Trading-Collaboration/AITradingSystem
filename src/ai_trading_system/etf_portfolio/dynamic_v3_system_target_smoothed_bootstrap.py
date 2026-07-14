from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from datetime import date, datetime, timedelta
from hashlib import sha256
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

import pandas as pd

from ai_trading_system.data.quality import PRICE_REQUIRED_COLUMNS, RATE_REQUIRED_COLUMNS
from ai_trading_system.etf_portfolio import dynamic_v3_system_target as legacy
from ai_trading_system.etf_portfolio import dynamic_v3_system_target_hardening as hardening
from ai_trading_system.etf_portfolio import dynamic_v3_system_target_portfolio as target_core
from ai_trading_system.etf_portfolio import (
    dynamic_v3_system_target_smoothed_operations as operations,
)
from ai_trading_system.etf_portfolio import (
    dynamic_v3_system_target_smoothed_promotion as promotion,
)
from ai_trading_system.etf_portfolio.dynamic_v3_confirmation_operations import (
    _file_bytes_match,
    _json_bytes,
    _jsonl_bytes,
)
from ai_trading_system.etf_portfolio.dynamic_v3_historical_replay import (
    _mapping,
    _read_json,
    _read_jsonl,
    _records,
    _stable_id,
    _text,
    _unique_dir,
)

DAILY_SNAPSHOT_SCHEMA = "smoothed_daily_emission_input_snapshot.v2"
DUE_SNAPSHOT_SCHEMA = "smoothed_outcome_due_input_snapshot.v2"
UPDATE_SNAPSHOT_SCHEMA = "smoothed_outcome_update_input_snapshot.v2"
CLASSIFICATION_SNAPSHOT_SCHEMA = "smoothed_forward_classification_input_snapshot.v2"
WEEKLY_SNAPSHOT_SCHEMA = "smoothed_forward_weekly_run_input_snapshot.v2"

DEFAULT_MODEL_TARGET_DIR = target_core.DEFAULT_MODEL_TARGET_DIR
DEFAULT_PRICE_CACHE_PATH = legacy.DEFAULT_PRICE_CACHE_PATH
DEFAULT_RATES_CACHE_PATH = legacy.DEFAULT_RATES_CACHE_PATH
DEFAULT_SMOOTHED_FORWARD_BINDING_DIR = promotion.DEFAULT_SMOOTHED_FORWARD_BINDING_DIR
DEFAULT_PAPER_SHADOW_PRIMARY_SWITCH_DIR = promotion.DEFAULT_PAPER_SHADOW_PRIMARY_SWITCH_DIR
DEFAULT_SMOOTHED_OWNER_PROMOTION_DIR = promotion.DEFAULT_SMOOTHED_OWNER_PROMOTION_DIR
DEFAULT_SMOOTHED_DAILY_EMISSION_DIR = legacy.DEFAULT_SMOOTHED_DAILY_EMISSION_DIR
DEFAULT_SMOOTHED_OUTCOME_DUE_DIR = legacy.DEFAULT_SMOOTHED_OUTCOME_DUE_DIR
DEFAULT_SMOOTHED_OUTCOME_UPDATE_DIR = legacy.DEFAULT_SMOOTHED_OUTCOME_UPDATE_DIR
DEFAULT_SMOOTHED_FORWARD_CLASSIFICATION_DIR = legacy.DEFAULT_SMOOTHED_FORWARD_CLASSIFICATION_DIR
DEFAULT_SMOOTHED_FORWARD_WEEKLY_RUN_DIR = legacy.DEFAULT_SMOOTHED_FORWARD_WEEKLY_RUN_DIR
DEFAULT_SMOOTHED_FORWARD_PROGRESS_DIR = operations.DEFAULT_SMOOTHED_FORWARD_PROGRESS_DIR
DEFAULT_SMOOTHED_WEEKLY_DASHBOARD_DIR = operations.DEFAULT_SMOOTHED_WEEKLY_DASHBOARD_DIR
DEFAULT_SMOOTHED_EVENT_MONITOR_DIR = operations.DEFAULT_SMOOTHED_EVENT_MONITOR_DIR
DEFAULT_SMOOTHED_SWITCH_READINESS_DIR = operations.DEFAULT_SMOOTHED_SWITCH_READINESS_DIR
DEFAULT_SMOOTHED_OWNER_RENEWAL_DIR = operations.DEFAULT_SMOOTHED_OWNER_RENEWAL_DIR

SYSTEM_TARGET_SAFETY = promotion.SYSTEM_TARGET_SAFETY

# TRADING-274 reporting-only regime proxies. These named invariants classify
# research evidence; they do not approve a method, size a position, or permit an order.
SIDEWAYS_ABS_RETURN_THRESHOLD = 0.01
STRONG_RECOVERY_RETURN_THRESHOLD = 0.02
FAST_REGIME_CHANGE_ABS_RETURN_THRESHOLD = 0.04
LAG_WARNING_DELTA_THRESHOLD = -0.01


class DynamicV3SmoothedBootstrapError(ValueError):
    """Raised when forward sample evidence cannot be reproduced exactly."""


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise DynamicV3SmoothedBootstrapError(message)


def _generated_at(value: datetime | None) -> datetime:
    try:
        return target_core._generated_at(value)
    except Exception as exc:  # noqa: BLE001
        raise DynamicV3SmoothedBootstrapError(str(exc)) from exc


def _validation_payload(
    report_type: str,
    artifact_id: str,
    errors: Sequence[str],
    mismatches: Sequence[str],
    *,
    artifact_id_key: str,
) -> dict[str, Any]:
    return operations._validation_payload(
        report_type,
        artifact_id,
        [
            legacy._check("snapshot_and_live_inputs", not errors, "; ".join(errors)),
            legacy._check("content_derived_views", not mismatches, ",".join(mismatches)),
        ],
        artifact_id_key=artifact_id_key,
    )


def _write(root: Path, views: Mapping[str, bytes], pointer: str, manifest: str) -> None:
    operations._write(root, views, pointer, manifest)


def _view_errors(root: Path, views: Mapping[str, bytes]) -> list[str]:
    return [name for name, payload in views.items() if not _file_bytes_match(root / name, payload)]


def _artifact_root(
    output_dir: Path,
    artifact_id: str | None,
    latest: bool,
    pointer: str,
) -> Path:
    return hardening._artifact_dir_from_latest(
        output_dir=output_dir,
        artifact_id=artifact_id if not latest else None,
        pointer_name=pointer,
    )


def _finite(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)


def _number(value: Any, *, field: str) -> float:
    _require(_finite(value), f"{field} must be finite")
    return float(value)


def _iso_date(value: Any, *, field: str) -> date:
    try:
        return date.fromisoformat(_text(value))
    except (TypeError, ValueError) as exc:
        raise DynamicV3SmoothedBootstrapError(f"{field} must be ISO date") from exc


def _source_json(source: Mapping[str, Any], name: str) -> dict[str, Any]:
    return hardening._bundle_json(source, name)


def _source_jsonl(source: Mapping[str, Any], name: str) -> list[dict[str, Any]]:
    return hardening._bundle_jsonl(source, name)


def _safe(*payloads: Mapping[str, Any]) -> bool:
    return all(
        payload.get("broker_action_allowed") is False
        and payload.get("not_official_target_weights") is True
        and payload.get("production_effect") == "none"
        for payload in payloads
    )


def _chronology(generated: datetime, *manifests: Mapping[str, Any]) -> None:
    for index, manifest in enumerate(manifests):
        source_time = target_core._datetime(
            manifest.get("generated_at"), field=f"bootstrap source {index} generated_at"
        )
        _require(source_time <= generated, "bootstrap source generated after consumer")


def _select_latest_valid(
    *,
    root: Path,
    manifest_name: str,
    id_key: str,
    validator: Any,
    cutoff: datetime,
) -> str | None:
    candidates: list[tuple[datetime, str]] = []
    if root.exists():
        for child in sorted(path for path in root.iterdir() if path.is_dir()):
            path = child / manifest_name
            if not path.is_file():
                continue
            manifest = _read_json(path)
            artifact_id = _text(manifest.get(id_key))
            _require(artifact_id == child.name, f"{manifest_name} artifact id mismatch")
            generated = target_core._datetime(
                manifest.get("generated_at"), field=f"{manifest_name} generated_at"
            )
            if generated <= cutoff:
                candidates.append((generated, artifact_id))
    if not candidates:
        return None
    candidates.sort(key=lambda item: (item[0], item[1]))
    selected = candidates[-1][1]
    validation = validator(**{id_key: selected, "output_dir": root})
    _require(validation.get("status") == "PASS", f"latest relevant {manifest_name} invalid")
    return selected


def _binding_source(
    binding_id: str | None,
    binding_dir: Path,
    generated: datetime,
) -> dict[str, Any] | None:
    resolved = binding_id or _select_latest_valid(
        root=binding_dir,
        manifest_name="smoothed_forward_binding_manifest.json",
        id_key="binding_id",
        validator=promotion.validate_smoothed_forward_binding_artifact,
        cutoff=generated,
    )
    return promotion._binding_binding(resolved, binding_dir) if resolved else None


def _bound(binding_source: Mapping[str, Any] | None) -> dict[str, Any]:
    if binding_source is None:
        return {
            "binding_id": None,
            "candidate_method": None,
            "binding_status": "NOT_REGISTERED",
            "targets": [],
        }
    return _mapping(promotion._binding_payload(binding_source).get("bound_confirmation_targets"))


def _model_target_source(target_id: str, root: Path) -> dict[str, Any]:
    return operations._local_source_binding(
        kind="model_target",
        artifact_id=target_id,
        root=root,
        validator=target_core.validate_model_target_artifact,
        validator_key="target_id",
        json_views=(
            "model_target_manifest.json",
            "model_target_weights.json",
            "target_constraint_checks.json",
        ),
        jsonl_views=("method_target_weights.jsonl",),
    )


def _resolve_model_target(
    target_id: str | None,
    root: Path,
    generated: datetime,
) -> dict[str, Any]:
    resolved = target_id
    if resolved is None:
        selected = target_core._select_model_target(root, generated=generated)
        resolved = selected[0].name
    return _model_target_source(resolved, root)


def _emission_source(emission_id: str, root: Path) -> dict[str, Any]:
    return operations._local_source_binding(
        kind="smoothed_daily_emission",
        artifact_id=emission_id,
        root=root,
        validator=validate_smoothed_daily_emission_artifact,
        validator_key="emission_id",
        json_views=(
            "smoothed_daily_emission_manifest.json",
            "smoothed_event_weights.json",
            "smoothed_emission_data_quality.json",
        ),
        jsonl_views=("smoothed_forward_events.jsonl",),
        text_views=("smoothed_daily_emission_report.md", "reader_brief_section.md"),
    )


def _due_source(due_id: str, root: Path) -> dict[str, Any]:
    return operations._local_source_binding(
        kind="smoothed_outcome_due",
        artifact_id=due_id,
        root=root,
        validator=validate_smoothed_outcome_due_artifact,
        validator_key="due_id",
        json_views=("smoothed_outcome_due_manifest.json", "due_summary.json"),
        jsonl_views=("due_windows.jsonl",),
        text_views=("smoothed_outcome_due_report.md",),
    )


def _update_source(update_id: str, root: Path) -> dict[str, Any]:
    return operations._local_source_binding(
        kind="smoothed_outcome_update",
        artifact_id=update_id,
        root=root,
        validator=validate_smoothed_outcome_update_artifact,
        validator_key="update_id",
        json_views=(
            "smoothed_outcome_update_manifest.json",
            "smoothed_outcome_delta_summary.json",
        ),
        jsonl_views=("updated_smoothed_outcomes.jsonl", "skipped_smoothed_outcomes.jsonl"),
        text_views=("smoothed_outcome_update_report.md", "reader_brief_section.md"),
    )


def _classification_source(classification_id: str, root: Path) -> dict[str, Any]:
    return operations._local_source_binding(
        kind="smoothed_forward_classification",
        artifact_id=classification_id,
        root=root,
        validator=validate_smoothed_forward_classification_artifact,
        validator_key="classification_id",
        json_views=(
            "smoothed_forward_classification_manifest.json",
            "smoothed_forward_classification_summary.json",
            "classification_summary.json",
        ),
        jsonl_views=("classified_forward_events.jsonl",),
        text_views=("smoothed_forward_classification_report.md",),
    )


def _sha(path: Path) -> str:
    return sha256(path.read_bytes()).hexdigest()


def _quality_summary(prices: pd.DataFrame, rates: pd.DataFrame) -> dict[str, Any]:
    with TemporaryDirectory(prefix="aits-smoothed-bootstrap-") as directory:
        root = Path(directory)
        prices_path = root / "prices_daily.csv"
        rates_path = root / "rates_daily.csv"
        prices.to_csv(prices_path, index=False)
        rates.to_csv(rates_path, index=False)
        report = legacy._smoothed_preflight_data_quality_report(
            prices_path=prices_path,
            rates_path=rates_path,
            as_of=max(pd.to_datetime(prices["date"]).dt.date),
        )
    return {
        "status": report.status,
        "error_count": report.error_count,
        "warning_count": report.warning_count,
        "price_rows": report.price_summary.rows,
        "rate_rows": report.rate_summary.rows,
        "price_min_date": report.price_summary.min_date.isoformat()
        if report.price_summary.min_date
        else None,
        "price_max_date": report.price_summary.max_date.isoformat()
        if report.price_summary.max_date
        else None,
        "rate_min_date": report.rate_summary.min_date.isoformat()
        if report.rate_summary.min_date
        else None,
        "rate_max_date": report.rate_summary.max_date.isoformat()
        if report.rate_summary.max_date
        else None,
        "issues": sorted(
            {
                (str(issue.severity), issue.code, issue.rows, str(issue.source))
                for issue in report.issues
            },
            key=lambda item: (item[0], item[1], -1 if item[2] is None else item[2], item[3]),
        ),
    }


def _market_binding(
    *,
    price_cache_path: Path,
    rates_cache_path: Path,
    symbols: Sequence[str],
    start: date,
    cutoff: date,
) -> dict[str, Any]:
    _require(price_cache_path.is_file(), "price cache missing")
    _require(rates_cache_path.is_file(), "rates cache missing")
    prices = pd.read_csv(price_cache_path)
    rates = pd.read_csv(rates_cache_path)
    required_prices = set(PRICE_REQUIRED_COLUMNS)
    required_rates = set(RATE_REQUIRED_COLUMNS)
    _require(required_prices.issubset(prices.columns), "price cache schema invalid")
    _require(required_rates.issubset(rates.columns), "rates cache schema invalid")
    prices["date"] = pd.to_datetime(prices["date"], errors="raise").dt.date
    rates["date"] = pd.to_datetime(rates["date"], errors="raise").dt.date
    symbol_set = {_text(symbol).upper() for symbol in symbols}
    selected_prices = prices.loc[
        prices["ticker"].astype(str).str.upper().isin(symbol_set)
        & prices["date"].between(start, cutoff)
    ].copy()
    selected_rates = rates.loc[rates["date"] <= cutoff].copy()
    _require(not selected_prices.empty, "bounded price slice empty")
    _require(not selected_rates.empty, "bounded rates slice empty")
    _require(
        not selected_prices.duplicated(["date", "ticker"]).any(),
        "duplicate bounded price key",
    )
    for field in ("open", "high", "low", "close", "adj_close"):
        selected_prices[field] = pd.to_numeric(selected_prices[field], errors="coerce")
        _require(
            selected_prices[field].map(lambda value: math.isfinite(value) and value > 0).all(),
            f"bounded price {field} invalid",
        )
    _require(
        set(selected_prices.loc[selected_prices["date"] == cutoff, "ticker"].str.upper())
        == symbol_set,
        "requested cutoff prices incomplete",
    )
    quality = _quality_summary(selected_prices, selected_rates)
    _require(quality.get("status") != "FAIL", "validate-data failed for bounded cache")
    rows = [
        {
            "date": row.date.isoformat(),
            "ticker": _text(row.ticker).upper(),
            "adj_close": float(row.adj_close),
        }
        for row in selected_prices.sort_values(["date", "ticker"]).itertuples(index=False)
    ]
    return {
        "schema_version": "smoothed_market_data_binding.v2",
        "price_source_path": str(price_cache_path.resolve()),
        "price_source_sha256": _sha(price_cache_path),
        "rates_source_path": str(rates_cache_path.resolve()),
        "rates_source_sha256": _sha(rates_cache_path),
        "symbols": sorted(symbol_set),
        "start": start.isoformat(),
        "cutoff": cutoff.isoformat(),
        "price_rows": rows,
        "quality": quality,
    }


def _validate_market_binding(binding: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    try:
        expected = _market_binding(
            price_cache_path=Path(_text(binding.get("price_source_path"))),
            rates_cache_path=Path(_text(binding.get("rates_source_path"))),
            symbols=[_text(item) for item in binding.get("symbols", [])],
            start=_iso_date(binding.get("start"), field="market start"),
            cutoff=_iso_date(binding.get("cutoff"), field="market cutoff"),
        )
        _require(expected == dict(binding), "market data binding drift")
    except Exception as exc:  # noqa: BLE001
        errors.append(str(exc))
    return errors


def _pivot(binding: Mapping[str, Any]) -> pd.DataFrame:
    rows = _records(binding.get("price_rows"))
    if not rows:
        return pd.DataFrame()
    frame = pd.DataFrame(rows)
    frame["date"] = pd.to_datetime(frame["date"])
    return frame.pivot(index="date", columns="ticker", values="adj_close").sort_index()


def _normalize_weights(value: Any, *, field: str) -> dict[str, float]:
    payload = _mapping(value)
    _require(bool(payload), f"{field} weights missing")
    result: dict[str, float] = {}
    for symbol, raw in payload.items():
        weight = _number(raw, field=f"{field}.{symbol}")
        _require(weight >= 0.0, f"{field}.{symbol} negative")
        result[_text(symbol).upper()] = weight
    _require(abs(sum(result.values()) - 1.0) <= 1e-8, f"{field} weights do not sum to one")
    return {key: round(value, 12) for key, value in sorted(result.items())}


def _method_weights(source: Mapping[str, Any]) -> dict[str, dict[str, float]]:
    payload = _source_json(source, "model_target_weights.json")
    methods = _mapping(payload.get("method_weights"))
    return {
        _text(method): _normalize_weights(weights, field=f"method_weights.{method}")
        for method, weights in methods.items()
    }


def _symbols(methods: Mapping[str, Mapping[str, Any]]) -> list[str]:
    return sorted(
        {
            _text(symbol).upper()
            for weights in methods.values()
            for symbol in weights
            if _text(symbol).upper() != "CASH"
        }
    )


def _target_windows(target: Mapping[str, Any]) -> list[int]:
    windows = target.get("windows", [])
    _require(
        isinstance(windows, Sequence) and not isinstance(windows, (str, bytes)),
        "target windows invalid",
    )
    result = [int(item) for item in windows if isinstance(item, int) and not isinstance(item, bool)]
    _require(
        bool(result) and len(result) == len(set(result)) and min(result) > 0,
        "target windows invalid",
    )
    return sorted(result)


def _target_kind(target: Mapping[str, Any]) -> str:
    keys = [
        key
        for key in (
            "required_forward_events",
            "required_sideways_events",
            "required_recovery_events",
        )
        if key in target
    ]
    _require(len(keys) == 1, "target requirement identity invalid")
    return keys[0].removeprefix("required_").removesuffix("_events")


def _regime_context(pivot: pd.DataFrame, as_of: date) -> str:
    history = pivot.loc[pivot.index.date <= as_of].tail(6)
    if len(history) < 2:
        return "unknown"
    returns = history.pct_change(fill_method=None).dropna(how="all")
    basket_columns = [column for column in ("QQQ", "SMH") if column in returns]
    if not basket_columns:
        return "unknown"
    basket = returns[basket_columns].mean(axis=1).dropna()
    if basket.empty:
        return "unknown"
    total = float((1.0 + basket).prod() - 1.0)
    if abs(total) <= SIDEWAYS_ABS_RETURN_THRESHOLD:
        return "sideways_choppy"
    if total >= STRONG_RECOVERY_RETURN_THRESHOLD:
        return "strong_recovery"
    if total <= -STRONG_RECOVERY_RETURN_THRESHOLD:
        return "tech_drawdown"
    return "ai_trend" if total > 0.0 else "unknown"


def _daily_snapshot(
    *,
    as_of: date,
    target_id: str | None,
    binding_id: str | None,
    model_target_dir: Path,
    binding_dir: Path,
    price_cache_path: Path,
    rates_cache_path: Path,
    generated: datetime,
) -> dict[str, Any]:
    binding_source = _binding_source(binding_id, binding_dir, generated)
    bound = _bound(binding_source)
    candidate = bound.get("candidate_method")
    targets = _records(bound.get("targets"))
    _require(
        (candidate is None and not targets and bound.get("binding_status") == "NOT_REGISTERED")
        or (
            isinstance(candidate, str)
            and bool(candidate)
            and bool(targets)
            and all(row.get("method") == candidate for row in targets)
        ),
        "daily binding candidate/targets semantics invalid",
    )
    target_source: dict[str, Any] | None = None
    market_source: dict[str, Any] | None = None
    if targets:
        target_source = _resolve_model_target(target_id, model_target_dir, generated)
        manifest = _source_json(target_source, "model_target_manifest.json")
        _require(
            _iso_date(manifest.get("as_of"), field="model target as_of") == as_of,
            "model target as_of must equal emission as_of",
        )
        methods = _method_weights(target_source)
        required_methods = {candidate} | {_text(row.get("baseline")) for row in targets}
        _require(
            all(method and method in methods for method in required_methods),
            "binding target method weights missing",
        )
        market_source = _market_binding(
            price_cache_path=price_cache_path,
            rates_cache_path=rates_cache_path,
            symbols=_symbols(methods),
            start=as_of - timedelta(days=60),
            cutoff=as_of,
        )
    return {
        "schema_version": DAILY_SNAPSHOT_SCHEMA,
        "generated_at": generated.isoformat(),
        "as_of": as_of.isoformat(),
        "binding_source": binding_source,
        "model_target_source": target_source,
        "market_data_source": market_source,
        "production_effect": "none",
    }


def _daily_payloads(
    snapshot: Mapping[str, Any],
) -> tuple[list[dict[str, Any]], dict[str, Any], dict[str, Any]]:
    binding_source = snapshot.get("binding_source")
    bound = _bound(_mapping(binding_source) if isinstance(binding_source, Mapping) else None)
    candidate = bound.get("candidate_method")
    targets = _records(bound.get("targets"))
    target_source = snapshot.get("model_target_source")
    market_source = snapshot.get("market_data_source")
    as_of = _iso_date(snapshot.get("as_of"), field="daily as_of")
    methods = (
        _method_weights(_mapping(target_source))
        if isinstance(target_source, Mapping) and targets
        else {}
    )
    pivot = (
        _pivot(_mapping(market_source)) if isinstance(market_source, Mapping) else pd.DataFrame()
    )
    regime = _regime_context(pivot, as_of) if not pivot.empty else "unknown"
    binding_id = bound.get("binding_id")
    events: list[dict[str, Any]] = []
    event_weights: list[dict[str, Any]] = []
    for target in targets:
        target_id = _text(target.get("target_id"))
        baseline = _text(target.get("baseline"))
        event_id = _stable_id(
            "smoothed-forward-event-v2",
            binding_id,
            target_id,
            as_of.isoformat(),
            methods,
        )
        event = {
            "schema_version": 2,
            "event_id": event_id,
            "target_id": target_id,
            "target_kind": _target_kind(target),
            "binding_id": binding_id,
            "as_of": as_of.isoformat(),
            "event_type": "SMOOTHED_FORWARD_OBSERVATION",
            "candidate_method": candidate,
            "baseline_method": baseline,
            "outcome_windows": _target_windows(target),
            "regime_context": regime,
            "event_status": "ACTIVE",
            "skip_reasons": [],
            "source_model_target_id": _text(
                _source_json(_mapping(target_source), "model_target_manifest.json").get("target_id")
            ),
            "requested_as_of": as_of.isoformat(),
            "future_data_used": False,
            **SYSTEM_TARGET_SAFETY,
        }
        events.append(event)
        event_weights.append(
            {
                "schema_version": 2,
                "event_id": event_id,
                "target_id": target_id,
                "binding_id": binding_id,
                "candidate_method": candidate,
                "baseline_method": baseline,
                "weights": methods,
                **SYSTEM_TARGET_SAFETY,
            }
        )
    weights_payload = {
        "schema_version": 2,
        "binding_id": binding_id,
        "candidate_method": candidate,
        "event_weights": event_weights,
        "weight_validation": {
            "event_weight_count": len(event_weights),
            "all_required_methods_present": bool(event_weights) or not targets,
            "all_weights_sum_to_one": all(
                abs(sum(row.values()) - 1.0) <= 1e-8
                for event_row in event_weights
                for row in _mapping(event_row.get("weights")).values()
            ),
            "no_negative_weights": all(
                value >= 0.0
                for event_row in event_weights
                for row in _mapping(event_row.get("weights")).values()
                for value in _mapping(row).values()
            ),
            "constraint_status": "PASS",
        },
        **SYSTEM_TARGET_SAFETY,
    }
    quality = {
        "schema_version": 2,
        "as_of": as_of.isoformat(),
        "binding_status": bound.get("binding_status"),
        "target_count": len(targets),
        "data_quality": (
            _mapping(_mapping(market_source).get("quality")).get("status")
            if isinstance(market_source, Mapping)
            else "NOT_REQUIRED_NO_REGISTERED_TARGET"
        ),
        "future_data_used": False,
        **SYSTEM_TARGET_SAFETY,
    }
    return events, weights_payload, quality


def _render_daily_report(
    manifest: Mapping[str, Any],
    events: Sequence[Mapping[str, Any]],
    quality: Mapping[str, Any],
) -> str:
    return "\n".join(
        [
            f"# Smoothed Daily Emission {manifest.get('emission_id')}",
            "",
            f"- as_of: {manifest.get('as_of')}",
            f"- binding_id: {manifest.get('binding_id')}",
            f"- candidate_method: {manifest.get('candidate_method')}",
            f"- binding_status: {manifest.get('binding_status')}",
            f"- emitted_event_count: {len(events)}",
            f"- event_status: {manifest.get('event_status')}",
            f"- data_quality: {quality.get('data_quality')}",
            "- future_data_used: false",
            "- broker_action_allowed: false",
            "- production_effect: none",
            "",
            "Candidate 与 targets 只来自 validated Smoothed Forward Binding；"
            "零 target 不补造样本。",
            "",
        ]
    )


def _render_daily_reader(manifest: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "## Dynamic Rescue Smoothed Daily Emission",
            "",
            f"- emission_id: {manifest.get('emission_id')}",
            f"- binding_id: {manifest.get('binding_id')}",
            f"- candidate_method: {manifest.get('candidate_method')}",
            f"- emitted_event_count: {manifest.get('emitted_event_count')}",
            f"- event_status: {manifest.get('event_status')}",
            "- future_data_used: false",
            "- broker_action_allowed: false",
            "- production_effect: none",
            "",
        ]
    )


def _daily_views(
    snapshot: Mapping[str, Any], *, emission_id: str, root: Path
) -> tuple[dict[str, bytes], dict[str, Any]]:
    bound = _bound(
        _mapping(snapshot.get("binding_source"))
        if isinstance(snapshot.get("binding_source"), Mapping)
        else None
    )
    events, weights, quality = _daily_payloads(snapshot)
    status = "ACTIVE" if events else "NOT_REGISTERED"
    manifest = {
        "schema_version": 2,
        "report_type": "etf_dynamic_v3_smoothed_daily_emission_manifest",
        "emission_id": emission_id,
        "binding_id": bound.get("binding_id"),
        "candidate_method": bound.get("candidate_method"),
        "binding_status": bound.get("binding_status"),
        "as_of": snapshot.get("as_of"),
        "generated_at": snapshot.get("generated_at"),
        "status": "PASS",
        "emitted_event_count": len(events),
        "event_status": status,
        "data_quality": quality.get("data_quality"),
        "future_data_used": False,
        "smoothed_daily_emission_input_snapshot_path": str(
            root / "smoothed_daily_emission_input_snapshot.json"
        ),
        "smoothed_daily_emission_manifest_path": str(
            root / "smoothed_daily_emission_manifest.json"
        ),
        "smoothed_forward_events_path": str(root / "smoothed_forward_events.jsonl"),
        "smoothed_event_weights_path": str(root / "smoothed_event_weights.json"),
        "smoothed_emission_data_quality_path": str(root / "smoothed_emission_data_quality.json"),
        "smoothed_daily_emission_report_path": str(root / "smoothed_daily_emission_report.md"),
        "reader_brief_section_path": str(root / "reader_brief_section.md"),
        **SYSTEM_TARGET_SAFETY,
    }
    report = _render_daily_report(manifest, events, quality)
    reader = _render_daily_reader(manifest)
    views = {
        "smoothed_daily_emission_input_snapshot.json": _json_bytes(dict(snapshot)),
        "smoothed_daily_emission_manifest.json": _json_bytes(manifest),
        "smoothed_forward_events.jsonl": _jsonl_bytes(events),
        "smoothed_event_weights.json": _json_bytes(weights),
        "smoothed_emission_data_quality.json": _json_bytes(quality),
        "smoothed_daily_emission_report.md": report.encode("utf-8"),
        "reader_brief_section.md": reader.encode("utf-8"),
    }
    return views, {
        "manifest": manifest,
        "smoothed_forward_events": events,
        "smoothed_event_weights": weights,
        "smoothed_emission_data_quality": quality,
        "reader_brief_section": reader,
    }


def _validate_daily_snapshot(snapshot: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    try:
        _require(
            snapshot.get("schema_version") == DAILY_SNAPSHOT_SCHEMA, "daily snapshot schema invalid"
        )
        generated = target_core._datetime(snapshot.get("generated_at"), field="daily generated_at")
        as_of = _iso_date(snapshot.get("as_of"), field="daily as_of")
        _require(as_of <= generated.date(), "daily as_of after generated cutoff")
        binding_source = snapshot.get("binding_source")
        if isinstance(binding_source, Mapping):
            errors.extend(
                promotion._validate_binding(
                    _mapping(binding_source),
                    kind="smoothed_forward_binding",
                    validator=promotion.validate_smoothed_forward_binding_artifact,
                    validator_key="binding_id",
                )
            )
            _chronology(
                generated,
                _source_json(_mapping(binding_source), "smoothed_forward_binding_manifest.json"),
            )
        bound = _bound(_mapping(binding_source) if isinstance(binding_source, Mapping) else None)
        targets = _records(bound.get("targets"))
        target_source = snapshot.get("model_target_source")
        market_source = snapshot.get("market_data_source")
        if targets:
            _require(isinstance(target_source, Mapping), "registered target requires model target")
            _require(isinstance(market_source, Mapping), "registered target requires market data")
            errors.extend(
                operations._validate_local_binding(
                    _mapping(target_source),
                    kind="model_target",
                    validator=target_core.validate_model_target_artifact,
                    validator_key="target_id",
                )
            )
            target_manifest = _source_json(_mapping(target_source), "model_target_manifest.json")
            _chronology(generated, target_manifest)
            _require(
                _iso_date(target_manifest.get("as_of"), field="model target as_of") == as_of,
                "model target/emission as_of mismatch",
            )
            errors.extend(_validate_market_binding(_mapping(market_source)))
        else:
            _require(
                target_source is None and market_source is None, "candidate-less daily has sources"
            )
        _require(snapshot.get("production_effect") == "none", "daily production boundary invalid")
        _daily_payloads(snapshot)
    except Exception as exc:  # noqa: BLE001
        errors.append(str(exc))
    return errors


@promotion._with_validation_session
def run_smoothed_daily_emission(
    *,
    as_of: date,
    target_id: str | None = None,
    binding_id: str | None = None,
    model_target_dir: Path = DEFAULT_MODEL_TARGET_DIR,
    binding_dir: Path = DEFAULT_SMOOTHED_FORWARD_BINDING_DIR,
    output_dir: Path = DEFAULT_SMOOTHED_DAILY_EMISSION_DIR,
    price_cache_path: Path = DEFAULT_PRICE_CACHE_PATH,
    rates_cache_path: Path = DEFAULT_RATES_CACHE_PATH,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = _generated_at(generated_at)
    snapshot = _daily_snapshot(
        as_of=as_of,
        target_id=target_id,
        binding_id=binding_id,
        model_target_dir=model_target_dir,
        binding_dir=binding_dir,
        price_cache_path=price_cache_path,
        rates_cache_path=rates_cache_path,
        generated=generated,
    )
    errors = _validate_daily_snapshot(snapshot)
    _require(not errors, "; ".join(errors))
    artifact_id = _stable_id("smoothed-daily-emission", snapshot)
    root = _unique_dir(output_dir / artifact_id)
    views, payload = _daily_views(snapshot, emission_id=root.name, root=root)
    _write(root, views, "latest_smoothed_daily_emission", "smoothed_daily_emission_manifest.json")
    return {"emission_id": root.name, "emission_dir": root, **payload}


def smoothed_daily_emission_report_payload(
    *,
    emission_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_SMOOTHED_DAILY_EMISSION_DIR,
) -> dict[str, Any]:
    root = _artifact_root(output_dir, emission_id, latest, "latest_smoothed_daily_emission")
    return {
        **_read_json(root / "smoothed_daily_emission_manifest.json"),
        "smoothed_forward_events": _read_jsonl(root / "smoothed_forward_events.jsonl"),
        "smoothed_event_weights": _read_json(root / "smoothed_event_weights.json"),
        "smoothed_emission_data_quality": _read_json(root / "smoothed_emission_data_quality.json"),
        "reader_brief_section": (root / "reader_brief_section.md").read_text(encoding="utf-8"),
        "input_snapshot": _read_json(root / "smoothed_daily_emission_input_snapshot.json"),
        "emission_dir": str(root),
    }


@promotion._with_validation_session
def validate_smoothed_daily_emission_artifact(
    *,
    emission_id: str,
    output_dir: Path = DEFAULT_SMOOTHED_DAILY_EMISSION_DIR,
) -> dict[str, Any]:
    root = output_dir / emission_id
    snapshot = (
        legacy._read_optional_json(root / "smoothed_daily_emission_input_snapshot.json") or {}
    )
    errors = _validate_daily_snapshot(snapshot)
    mismatches: list[str] = []
    try:
        views, payload = _daily_views(snapshot, emission_id=emission_id, root=root)
        mismatches = _view_errors(root, views)
        _require(
            _safe(
                _mapping(payload.get("manifest")),
                _mapping(payload.get("smoothed_event_weights")),
                _mapping(payload.get("smoothed_emission_data_quality")),
                *_records(payload.get("smoothed_forward_events")),
            ),
            "daily safety fields invalid",
        )
    except Exception as exc:  # noqa: BLE001
        errors.append(str(exc))
    return _validation_payload(
        "etf_dynamic_v3_smoothed_daily_emission_validation",
        emission_id,
        errors,
        mismatches,
        artifact_id_key="emission_id",
    )


def _select_emission_sources(
    *,
    root: Path,
    binding_id: str | None,
    candidate: str | None,
    scanner_as_of: date,
    generated: datetime,
) -> list[dict[str, Any]]:
    selected: list[tuple[date, datetime, str]] = []
    if root.exists():
        for child in sorted(path for path in root.iterdir() if path.is_dir()):
            manifest_path = child / "smoothed_daily_emission_manifest.json"
            if not manifest_path.is_file():
                continue
            manifest = _read_json(manifest_path)
            artifact_id = _text(manifest.get("emission_id"))
            _require(artifact_id == child.name, "daily emission directory/id mismatch")
            source_generated = target_core._datetime(
                manifest.get("generated_at"), field="emission generated_at"
            )
            source_as_of = _iso_date(manifest.get("as_of"), field="emission as_of")
            if (
                source_generated <= generated
                and source_as_of <= scanner_as_of
                and manifest.get("binding_id") == binding_id
                and manifest.get("candidate_method") == candidate
                and int(manifest.get("emitted_event_count", 0)) > 0
            ):
                selected.append((source_as_of, source_generated, artifact_id))
    sources: list[dict[str, Any]] = []
    for _as_of, _generated, artifact_id in sorted(selected):
        source = _emission_source(artifact_id, root)
        manifest = _source_json(source, "smoothed_daily_emission_manifest.json")
        _require(manifest.get("binding_id") == binding_id, "emission binding mismatch")
        _require(manifest.get("candidate_method") == candidate, "emission candidate mismatch")
        sources.append(source)
    return sources


def _events_and_weights(
    sources: Sequence[Mapping[str, Any]],
) -> tuple[list[dict[str, Any]], dict[str, dict[str, Any]]]:
    events: list[dict[str, Any]] = []
    weights: dict[str, dict[str, Any]] = {}
    seen: set[str] = set()
    for source in sources:
        manifest = _source_json(source, "smoothed_daily_emission_manifest.json")
        source_events = _source_jsonl(source, "smoothed_forward_events.jsonl")
        payload = _source_json(source, "smoothed_event_weights.json")
        rows = _records(payload.get("event_weights"))
        by_id = {_text(row.get("event_id")): row for row in rows}
        _require(len(by_id) == len(rows), "duplicate emission event weight")
        for event in source_events:
            event_id = _text(event.get("event_id"))
            _require(event_id and event_id not in seen, "duplicate forward event id")
            _require(event_id in by_id, "forward event weight missing")
            _require(event.get("event_status") == "ACTIVE", "non-active event in active emission")
            seen.add(event_id)
            events.append({**dict(event), "source_emission_id": manifest.get("emission_id")})
            weights[event_id] = dict(by_id[event_id])
    return events, weights


def _expected_end(event_as_of: date, window_days: int, trading_dates: Sequence[date]) -> date:
    future = [item for item in sorted(set(trading_dates)) if item > event_as_of]
    if len(future) >= window_days:
        return future[window_days - 1]
    current = event_as_of
    remaining = window_days
    while remaining:
        current += timedelta(days=1)
        if current.weekday() < 5:
            remaining -= 1
    return current


def _due_rows(
    *,
    events: Sequence[Mapping[str, Any]],
    market: Mapping[str, Any] | None,
    scanner_as_of: date,
) -> list[dict[str, Any]]:
    pivot = _pivot(_mapping(market)) if isinstance(market, Mapping) else pd.DataFrame()
    trading_dates = [item.date() for item in pivot.index]
    rows: list[dict[str, Any]] = []
    seen: set[tuple[str, int, str]] = set()
    for event in events:
        event_id = _text(event.get("event_id"))
        target_id = _text(event.get("target_id"))
        event_as_of = _iso_date(event.get("as_of"), field="event as_of")
        for window in event.get("outcome_windows", []):
            _require(
                isinstance(window, int) and not isinstance(window, bool) and window > 0,
                "window invalid",
            )
            key = (event_id, window, target_id)
            _require(key not in seen, "duplicate event/window/target")
            seen.add(key)
            expected = _expected_end(event_as_of, window, trading_dates)
            available = not pivot.empty and (pivot.index.date == expected).any()
            if expected > scanner_as_of:
                due_status = "NOT_DUE"
                reasons: list[str] = []
            elif not available:
                due_status = "PRICE_MISSING"
                reasons = ["expected_end_price_missing"]
            else:
                due_status = "DUE"
                reasons = []
            rows.append(
                {
                    "schema_version": 2,
                    "source_emission_id": event.get("source_emission_id"),
                    "binding_id": event.get("binding_id"),
                    "candidate_method": event.get("candidate_method"),
                    "target_id": target_id,
                    "event_id": event_id,
                    "as_of": event_as_of.isoformat(),
                    "window_days": window,
                    "expected_end_date": expected.isoformat(),
                    "scanner_as_of": scanner_as_of.isoformat(),
                    "due_status": due_status,
                    "price_available": due_status == "DUE",
                    "can_update": due_status == "DUE",
                    "block_reasons": reasons,
                    **SYSTEM_TARGET_SAFETY,
                }
            )
    return rows


def _due_summary(as_of: date, rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    statuses = [_text(row.get("due_status")) for row in rows]
    return {
        "schema_version": 2,
        "due_id": "",
        "scanner_as_of": as_of.isoformat(),
        "events_scanned": len({_text(row.get("event_id")) for row in rows}),
        "total_windows_scanned": len(rows),
        "due_windows": statuses.count("DUE"),
        "not_due_windows": statuses.count("NOT_DUE"),
        "price_missing_windows": statuses.count("PRICE_MISSING"),
        "blocked_future_as_of": 0,
        "update_ready_count": sum(row.get("can_update") is True for row in rows),
        **SYSTEM_TARGET_SAFETY,
    }


def _due_snapshot(
    *,
    as_of: date,
    binding_id: str | None,
    binding_dir: Path,
    emission_dir: Path,
    price_cache_path: Path,
    rates_cache_path: Path,
    generated: datetime,
) -> dict[str, Any]:
    binding_source = _binding_source(binding_id, binding_dir, generated)
    bound = _bound(binding_source)
    candidate = bound.get("candidate_method")
    sources = _select_emission_sources(
        root=emission_dir,
        binding_id=bound.get("binding_id"),
        candidate=candidate,
        scanner_as_of=as_of,
        generated=generated,
    )
    events, weights = _events_and_weights(sources)
    _require(
        bool(events) or not _records(bound.get("targets")), "registered binding has no emissions"
    )
    market: dict[str, Any] | None = None
    if events:
        methods = {
            method: row
            for item in weights.values()
            for method, row in _mapping(item.get("weights")).items()
        }
        market = _market_binding(
            price_cache_path=price_cache_path,
            rates_cache_path=rates_cache_path,
            symbols=_symbols(methods),
            start=min(_iso_date(event.get("as_of"), field="event as_of") for event in events),
            cutoff=as_of,
        )
    return {
        "schema_version": DUE_SNAPSHOT_SCHEMA,
        "generated_at": generated.isoformat(),
        "scanner_as_of": as_of.isoformat(),
        "binding_source": binding_source,
        "emission_sources": sources,
        "market_data_source": market,
        "production_effect": "none",
    }


def _render_due_report(
    manifest: Mapping[str, Any], summary: Mapping[str, Any], rows: Sequence[Mapping[str, Any]]
) -> str:
    return "\n".join(
        [
            f"# Smoothed Outcome Due {manifest.get('due_id')}",
            "",
            f"- binding_id: {manifest.get('binding_id')}",
            f"- candidate_method: {manifest.get('candidate_method')}",
            f"- scanner_as_of: {manifest.get('scanner_as_of')}",
            f"- events_scanned: {summary.get('events_scanned')}",
            f"- total_windows_scanned: {len(rows)}",
            f"- update_ready_count: {summary.get('update_ready_count')}",
            "- broker_action_allowed: false",
            "- production_effect: none",
            "",
            "Due windows 只来自 snapshot 中明确绑定且 validator PASS 的同 Binding emissions。",
            "",
        ]
    )


def _due_views(
    snapshot: Mapping[str, Any], *, due_id: str, root: Path
) -> tuple[dict[str, bytes], dict[str, Any]]:
    binding_source = snapshot.get("binding_source")
    bound = _bound(_mapping(binding_source) if isinstance(binding_source, Mapping) else None)
    events, _weights = _events_and_weights(_records(snapshot.get("emission_sources")))
    rows = _due_rows(
        events=events,
        market=_mapping(snapshot.get("market_data_source"))
        if isinstance(snapshot.get("market_data_source"), Mapping)
        else None,
        scanner_as_of=_iso_date(snapshot.get("scanner_as_of"), field="scanner as_of"),
    )
    summary = _due_summary(_iso_date(snapshot.get("scanner_as_of"), field="scanner as_of"), rows)
    summary["due_id"] = due_id
    emission_ids = [
        _text(_source_json(source, "smoothed_daily_emission_manifest.json").get("emission_id"))
        for source in _records(snapshot.get("emission_sources"))
    ]
    manifest = {
        "schema_version": 2,
        "report_type": "etf_dynamic_v3_smoothed_outcome_due_manifest",
        "due_id": due_id,
        "binding_id": bound.get("binding_id"),
        "candidate_method": bound.get("candidate_method"),
        "binding_status": bound.get("binding_status"),
        "emission_ids": emission_ids,
        "scanner_as_of": snapshot.get("scanner_as_of"),
        "generated_at": snapshot.get("generated_at"),
        "status": "PASS",
        "smoothed_outcome_due_input_snapshot_path": str(
            root / "smoothed_outcome_due_input_snapshot.json"
        ),
        "smoothed_outcome_due_manifest_path": str(root / "smoothed_outcome_due_manifest.json"),
        "due_windows_path": str(root / "due_windows.jsonl"),
        "due_summary_path": str(root / "due_summary.json"),
        "smoothed_outcome_due_report_path": str(root / "smoothed_outcome_due_report.md"),
        **SYSTEM_TARGET_SAFETY,
    }
    report = _render_due_report(manifest, summary, rows)
    views = {
        "smoothed_outcome_due_input_snapshot.json": _json_bytes(dict(snapshot)),
        "smoothed_outcome_due_manifest.json": _json_bytes(manifest),
        "due_windows.jsonl": _jsonl_bytes(rows),
        "due_summary.json": _json_bytes(summary),
        "smoothed_outcome_due_report.md": report.encode("utf-8"),
    }
    return views, {"manifest": manifest, "due_windows": rows, "due_summary": summary}


def _validate_due_snapshot(snapshot: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    try:
        _require(
            snapshot.get("schema_version") == DUE_SNAPSHOT_SCHEMA, "due snapshot schema invalid"
        )
        generated = target_core._datetime(snapshot.get("generated_at"), field="due generated_at")
        scanner_as_of = _iso_date(snapshot.get("scanner_as_of"), field="scanner as_of")
        _require(scanner_as_of <= generated.date(), "scanner as_of after generated cutoff")
        binding_source = snapshot.get("binding_source")
        if isinstance(binding_source, Mapping):
            errors.extend(
                promotion._validate_binding(
                    _mapping(binding_source),
                    kind="smoothed_forward_binding",
                    validator=promotion.validate_smoothed_forward_binding_artifact,
                    validator_key="binding_id",
                )
            )
        bound = _bound(_mapping(binding_source) if isinstance(binding_source, Mapping) else None)
        manifests: list[Mapping[str, Any]] = []
        for source in _records(snapshot.get("emission_sources")):
            errors.extend(
                operations._validate_local_binding(
                    source,
                    kind="smoothed_daily_emission",
                    validator=validate_smoothed_daily_emission_artifact,
                    validator_key="emission_id",
                )
            )
            manifest = _source_json(source, "smoothed_daily_emission_manifest.json")
            _require(
                manifest.get("binding_id") == bound.get("binding_id"),
                "due emission binding mismatch",
            )
            _require(
                manifest.get("candidate_method") == bound.get("candidate_method"),
                "due emission candidate mismatch",
            )
            _require(
                _iso_date(manifest.get("as_of"), field="emission as_of") <= scanner_as_of,
                "future emission selected",
            )
            manifests.append(manifest)
        if _records(bound.get("targets")):
            _require(bool(manifests), "registered due scan requires emissions")
            _require(
                isinstance(snapshot.get("market_data_source"), Mapping), "due market data missing"
            )
            errors.extend(_validate_market_binding(_mapping(snapshot.get("market_data_source"))))
        else:
            _require(not manifests, "candidate-less due scan consumed emissions")
            _require(
                snapshot.get("market_data_source") is None,
                "candidate-less due scan consumed market",
            )
        _chronology(generated, *manifests)
        _events_and_weights(_records(snapshot.get("emission_sources")))
        _require(snapshot.get("production_effect") == "none", "due production boundary invalid")
    except Exception as exc:  # noqa: BLE001
        errors.append(str(exc))
    return errors


@promotion._with_validation_session
def scan_smoothed_outcome_due(
    *,
    as_of: date,
    binding_id: str | None = None,
    binding_dir: Path = DEFAULT_SMOOTHED_FORWARD_BINDING_DIR,
    emission_dir: Path = DEFAULT_SMOOTHED_DAILY_EMISSION_DIR,
    output_dir: Path = DEFAULT_SMOOTHED_OUTCOME_DUE_DIR,
    price_cache_path: Path = DEFAULT_PRICE_CACHE_PATH,
    rates_cache_path: Path = DEFAULT_RATES_CACHE_PATH,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = _generated_at(generated_at)
    snapshot = _due_snapshot(
        as_of=as_of,
        binding_id=binding_id,
        binding_dir=binding_dir,
        emission_dir=emission_dir,
        price_cache_path=price_cache_path,
        rates_cache_path=rates_cache_path,
        generated=generated,
    )
    errors = _validate_due_snapshot(snapshot)
    _require(not errors, "; ".join(errors))
    artifact_id = _stable_id("smoothed-outcome-due", snapshot)
    root = _unique_dir(output_dir / artifact_id)
    views, payload = _due_views(snapshot, due_id=root.name, root=root)
    _write(root, views, "latest_smoothed_outcome_due", "smoothed_outcome_due_manifest.json")
    return {"due_id": root.name, "due_dir": root, **payload}


def smoothed_outcome_due_report_payload(
    *,
    due_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_SMOOTHED_OUTCOME_DUE_DIR,
) -> dict[str, Any]:
    root = _artifact_root(output_dir, due_id, latest, "latest_smoothed_outcome_due")
    return {
        **_read_json(root / "smoothed_outcome_due_manifest.json"),
        "due_windows": _read_jsonl(root / "due_windows.jsonl"),
        "due_summary": _read_json(root / "due_summary.json"),
        "input_snapshot": _read_json(root / "smoothed_outcome_due_input_snapshot.json"),
        "due_dir": str(root),
    }


@promotion._with_validation_session
def validate_smoothed_outcome_due_artifact(
    *, due_id: str, output_dir: Path = DEFAULT_SMOOTHED_OUTCOME_DUE_DIR
) -> dict[str, Any]:
    root = output_dir / due_id
    snapshot = legacy._read_optional_json(root / "smoothed_outcome_due_input_snapshot.json") or {}
    errors = _validate_due_snapshot(snapshot)
    mismatches: list[str] = []
    try:
        views, payload = _due_views(snapshot, due_id=due_id, root=root)
        mismatches = _view_errors(root, views)
        _require(
            _safe(
                _mapping(payload.get("manifest")),
                _mapping(payload.get("due_summary")),
                *_records(payload.get("due_windows")),
            ),
            "due safety fields invalid",
        )
    except Exception as exc:  # noqa: BLE001
        errors.append(str(exc))
    return _validation_payload(
        "etf_dynamic_v3_smoothed_outcome_due_validation",
        due_id,
        errors,
        mismatches,
        artifact_id_key="due_id",
    )


def _portfolio_return(
    weights: Mapping[str, Any], pivot: pd.DataFrame, start: date, end: date
) -> float | None:
    start_rows = pivot.loc[pivot.index.date == start]
    end_rows = pivot.loc[pivot.index.date == end]
    if start_rows.empty or end_rows.empty:
        return None
    start_row = start_rows.iloc[-1]
    end_row = end_rows.iloc[-1]
    total = 0.0
    for symbol, raw_weight in weights.items():
        weight = _number(raw_weight, field=f"weight.{symbol}")
        if symbol == "CASH":
            continue
        if symbol not in pivot.columns:
            return None
        start_price = start_row.get(symbol)
        end_price = end_row.get(symbol)
        if not _finite(start_price) or not _finite(end_price) or float(start_price) <= 0.0:
            return None
        total += weight * (float(end_price) / float(start_price) - 1.0)
    return round(total, 10)


def _portfolio_drawdown(
    weights: Mapping[str, Any], pivot: pd.DataFrame, start: date, end: date
) -> float | None:
    window = pivot.loc[(pivot.index.date >= start) & (pivot.index.date <= end)]
    if len(window) < 2:
        return None
    noncash = [symbol for symbol in weights if symbol != "CASH"]
    if any(symbol not in window or window[symbol].isna().any() for symbol in noncash):
        return None
    returns = window[noncash].pct_change(fill_method=None).iloc[1:]
    if returns.empty or not returns.map(math.isfinite).all(axis=None):
        return None
    portfolio = pd.Series(0.0, index=returns.index)
    for symbol in noncash:
        portfolio = portfolio + returns[symbol] * _number(
            weights.get(symbol), field=f"weight.{symbol}"
        )
    equity = pd.concat([pd.Series([1.0], index=[window.index[0]]), (1.0 + portfolio).cumprod()])
    drawdown = equity / equity.cummax() - 1.0
    value = float(drawdown.min())
    return round(value, 10) if math.isfinite(value) else None


def _relative(left: float | None, right: float | None) -> float | None:
    return round(left - right, 10) if left is not None and right is not None else None


def _update_rows(
    *,
    due_rows: Sequence[Mapping[str, Any]],
    emission_sources: Sequence[Mapping[str, Any]],
    market: Mapping[str, Any] | None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    events, weights_by_id = _events_and_weights(emission_sources)
    event_by_id = {_text(row.get("event_id")): row for row in events}
    pivot = _pivot(_mapping(market)) if isinstance(market, Mapping) else pd.DataFrame()
    updated: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    for row in due_rows:
        event_id = _text(row.get("event_id"))
        if row.get("can_update") is not True:
            skipped.append(
                {
                    "schema_version": 2,
                    "event_id": event_id,
                    "target_id": row.get("target_id"),
                    "window_days": row.get("window_days"),
                    "skip_reason": _text(row.get("due_status"), "DATA_QUALITY_FAIL"),
                    "old_status": "PENDING",
                    **SYSTEM_TARGET_SAFETY,
                }
            )
            continue
        event = _mapping(event_by_id.get(event_id))
        weight_payload = _mapping(weights_by_id.get(event_id))
        methods = _mapping(weight_payload.get("weights"))
        start = _iso_date(row.get("as_of"), field="outcome start")
        end = _iso_date(row.get("expected_end_date"), field="outcome end")
        method_returns: dict[str, float] = {}
        drawdowns: dict[str, float] = {}
        missing = False
        for method, raw_weights in methods.items():
            normalized = _normalize_weights(raw_weights, field=f"outcome.{method}")
            result = _portfolio_return(normalized, pivot, start, end)
            drawdown = _portfolio_drawdown(normalized, pivot, start, end)
            if result is None or drawdown is None:
                missing = True
                break
            method_returns[_text(method)] = result
            drawdowns[_text(method)] = drawdown
        if missing or not method_returns:
            skipped.append(
                {
                    "schema_version": 2,
                    "event_id": event_id,
                    "target_id": row.get("target_id"),
                    "window_days": row.get("window_days"),
                    "skip_reason": "PRICE_MISSING_OR_NON_FINITE",
                    "old_status": "PENDING",
                    **SYSTEM_TARGET_SAFETY,
                }
            )
            continue
        candidate = _text(event.get("candidate_method"))
        baseline = _text(event.get("baseline_method"))
        candidate_return = method_returns.get(candidate)
        baseline_return = method_returns.get(baseline)
        candidate_drawdown = drawdowns.get(candidate)
        baseline_drawdown = drawdowns.get(baseline)
        relative = {
            "candidate_vs_baseline": _relative(candidate_return, baseline_return),
            "smooth_3d_vs_limited": _relative(
                method_returns.get("smooth_weights_3d_limited_adjustment"),
                method_returns.get("limited_adjustment"),
            ),
            "smooth_3d_vs_static": _relative(
                method_returns.get("smooth_weights_3d_limited_adjustment"),
                method_returns.get("static_baseline"),
            ),
            "smooth_3d_vs_no_trade": _relative(
                method_returns.get("smooth_weights_3d_limited_adjustment"),
                method_returns.get("no_trade_baseline"),
            ),
            "smooth_5d_vs_smooth_3d": _relative(
                method_returns.get("smooth_weights_5d_limited_adjustment"),
                method_returns.get("smooth_weights_3d_limited_adjustment"),
            ),
        }
        updated.append(
            {
                "schema_version": 2,
                "source_emission_id": row.get("source_emission_id"),
                "binding_id": row.get("binding_id"),
                "candidate_method": candidate,
                "baseline_method": baseline,
                "target_id": row.get("target_id"),
                "event_id": event_id,
                "as_of": start.isoformat(),
                "window_days": row.get("window_days"),
                "start_date": start.isoformat(),
                "end_date": end.isoformat(),
                "regime_context": event.get("regime_context"),
                "method_returns": method_returns,
                "relative_metrics": relative,
                "drawdown_metrics": {
                    "candidate_drawdown": candidate_drawdown,
                    "baseline_drawdown": baseline_drawdown,
                    "candidate_drawdown_delta_vs_baseline": _relative(
                        candidate_drawdown, baseline_drawdown
                    ),
                    "smooth_3d_drawdown": drawdowns.get("smooth_weights_3d_limited_adjustment"),
                    "limited_drawdown": drawdowns.get("limited_adjustment"),
                    "smooth_3d_drawdown_delta_vs_limited": _relative(
                        drawdowns.get("smooth_weights_3d_limited_adjustment"),
                        drawdowns.get("limited_adjustment"),
                    ),
                },
                "outcome_status": "AVAILABLE",
                "future_data_used": False,
                **SYSTEM_TARGET_SAFETY,
            }
        )
    return updated, skipped


def _update_summary(
    updated: Sequence[Mapping[str, Any]], skipped: Sequence[Mapping[str, Any]]
) -> dict[str, Any]:
    relative = [
        _mapping(row.get("relative_metrics")).get("candidate_vs_baseline") for row in updated
    ]
    finite_relative = [float(value) for value in relative if _finite(value)]
    drawdown = [
        _mapping(row.get("drawdown_metrics")).get("candidate_drawdown_delta_vs_baseline")
        for row in updated
    ]
    finite_drawdown = [float(value) for value in drawdown if _finite(value)]
    return {
        "schema_version": 2,
        "update_id": "",
        "updated_count": len(updated),
        "skipped_count": len(skipped),
        "available_forward_events_after_update": len(
            {_text(row.get("event_id")) for row in updated}
        ),
        "candidate_win_rate_vs_baseline": (
            round(sum(value > 0.0 for value in finite_relative) / len(finite_relative), 10)
            if finite_relative
            else None
        ),
        "avg_candidate_relative_return_vs_baseline": (
            round(sum(finite_relative) / len(finite_relative), 10) if finite_relative else None
        ),
        "avg_candidate_drawdown_delta_vs_baseline": (
            round(sum(finite_drawdown) / len(finite_drawdown), 10) if finite_drawdown else None
        ),
        "summary_recommendation": "continue_tracking",
        **SYSTEM_TARGET_SAFETY,
    }


def _update_snapshot(
    *,
    due_id: str,
    due_dir: Path,
    emission_dir: Path,
    price_cache_path: Path,
    rates_cache_path: Path,
    generated: datetime,
) -> dict[str, Any]:
    due_source = _due_source(due_id, due_dir)
    due_manifest = _source_json(due_source, "smoothed_outcome_due_manifest.json")
    emission_sources = [
        _emission_source(_text(item), emission_dir) for item in due_manifest.get("emission_ids", [])
    ]
    due_rows = _source_jsonl(due_source, "due_windows.jsonl")
    ready = [row for row in due_rows if row.get("can_update") is True]
    market: dict[str, Any] | None = None
    if ready:
        _events, weight_map = _events_and_weights(emission_sources)
        methods = {
            method: weights
            for item in weight_map.values()
            for method, weights in _mapping(item.get("weights")).items()
        }
        market = _market_binding(
            price_cache_path=price_cache_path,
            rates_cache_path=rates_cache_path,
            symbols=_symbols(methods),
            start=min(_iso_date(row.get("as_of"), field="due start") for row in ready),
            cutoff=max(_iso_date(row.get("expected_end_date"), field="due end") for row in ready),
        )
    return {
        "schema_version": UPDATE_SNAPSHOT_SCHEMA,
        "generated_at": generated.isoformat(),
        "due_source": due_source,
        "emission_sources": emission_sources,
        "market_data_source": market,
        "production_effect": "none",
    }


def _render_update_report(
    manifest: Mapping[str, Any], summary: Mapping[str, Any], updated: Sequence[Mapping[str, Any]]
) -> str:
    return "\n".join(
        [
            f"# Smoothed Outcome Update {manifest.get('update_id')}",
            "",
            f"- due_id: {manifest.get('due_id')}",
            f"- binding_id: {manifest.get('binding_id')}",
            f"- candidate_method: {manifest.get('candidate_method')}",
            f"- updated_count: {len(updated)}",
            f"- skipped_count: {summary.get('skipped_count')}",
            "- future_data_used: false",
            "- broker_action_allowed: false",
            "- production_effect: none",
            "",
            "Return 与 drawdown 均由 snapshot 中的固定 event weights 和 bounded prices 重算。",
            "",
        ]
    )


def _render_update_reader(summary: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "## Dynamic Rescue Smoothed Outcome Update",
            "",
            f"- updated_windows: {summary.get('updated_count')}",
            f"- skipped_windows: {summary.get('skipped_count')}",
            f"- available_forward_events: {summary.get('available_forward_events_after_update')}",
            "- future_data_used: false",
            "- broker_action_allowed: false",
            "- production_effect: none",
            "",
        ]
    )


def _update_views(
    snapshot: Mapping[str, Any], *, update_id: str, root: Path
) -> tuple[dict[str, bytes], dict[str, Any]]:
    due_source = _mapping(snapshot.get("due_source"))
    due_manifest = _source_json(due_source, "smoothed_outcome_due_manifest.json")
    due_rows = _source_jsonl(due_source, "due_windows.jsonl")
    updated, skipped = _update_rows(
        due_rows=due_rows,
        emission_sources=_records(snapshot.get("emission_sources")),
        market=_mapping(snapshot.get("market_data_source"))
        if isinstance(snapshot.get("market_data_source"), Mapping)
        else None,
    )
    summary = _update_summary(updated, skipped)
    summary["update_id"] = update_id
    manifest = {
        "schema_version": 2,
        "report_type": "etf_dynamic_v3_smoothed_outcome_update_manifest",
        "update_id": update_id,
        "due_id": due_manifest.get("due_id"),
        "binding_id": due_manifest.get("binding_id"),
        "candidate_method": due_manifest.get("candidate_method"),
        "binding_status": due_manifest.get("binding_status"),
        "emission_ids": due_manifest.get("emission_ids", []),
        "generated_at": snapshot.get("generated_at"),
        "status": "PASS",
        "future_data_used": False,
        "smoothed_outcome_update_input_snapshot_path": str(
            root / "smoothed_outcome_update_input_snapshot.json"
        ),
        "smoothed_outcome_update_manifest_path": str(
            root / "smoothed_outcome_update_manifest.json"
        ),
        "updated_smoothed_outcomes_path": str(root / "updated_smoothed_outcomes.jsonl"),
        "skipped_smoothed_outcomes_path": str(root / "skipped_smoothed_outcomes.jsonl"),
        "smoothed_outcome_delta_summary_path": str(root / "smoothed_outcome_delta_summary.json"),
        "smoothed_outcome_update_report_path": str(root / "smoothed_outcome_update_report.md"),
        "reader_brief_section_path": str(root / "reader_brief_section.md"),
        **SYSTEM_TARGET_SAFETY,
    }
    report = _render_update_report(manifest, summary, updated)
    reader = _render_update_reader(summary)
    views = {
        "smoothed_outcome_update_input_snapshot.json": _json_bytes(dict(snapshot)),
        "smoothed_outcome_update_manifest.json": _json_bytes(manifest),
        "updated_smoothed_outcomes.jsonl": _jsonl_bytes(updated),
        "skipped_smoothed_outcomes.jsonl": _jsonl_bytes(skipped),
        "smoothed_outcome_delta_summary.json": _json_bytes(summary),
        "smoothed_outcome_update_report.md": report.encode("utf-8"),
        "reader_brief_section.md": reader.encode("utf-8"),
    }
    return views, {
        "manifest": manifest,
        "updated_smoothed_outcomes": updated,
        "skipped_smoothed_outcomes": skipped,
        "smoothed_outcome_delta_summary": summary,
        "reader_brief_section": reader,
    }


def _validate_update_snapshot(snapshot: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    try:
        _require(
            snapshot.get("schema_version") == UPDATE_SNAPSHOT_SCHEMA,
            "update snapshot schema invalid",
        )
        generated = target_core._datetime(snapshot.get("generated_at"), field="update generated_at")
        due_source = _mapping(snapshot.get("due_source"))
        errors.extend(
            operations._validate_local_binding(
                due_source,
                kind="smoothed_outcome_due",
                validator=validate_smoothed_outcome_due_artifact,
                validator_key="due_id",
            )
        )
        due_manifest = _source_json(due_source, "smoothed_outcome_due_manifest.json")
        _chronology(generated, due_manifest)
        expected_ids = [_text(item) for item in due_manifest.get("emission_ids", [])]
        actual_ids: list[str] = []
        manifests: list[Mapping[str, Any]] = []
        for source in _records(snapshot.get("emission_sources")):
            errors.extend(
                operations._validate_local_binding(
                    source,
                    kind="smoothed_daily_emission",
                    validator=validate_smoothed_daily_emission_artifact,
                    validator_key="emission_id",
                )
            )
            manifest = _source_json(source, "smoothed_daily_emission_manifest.json")
            actual_ids.append(_text(manifest.get("emission_id")))
            _require(
                manifest.get("binding_id") == due_manifest.get("binding_id"),
                "update emission binding mismatch",
            )
            _require(
                manifest.get("candidate_method") == due_manifest.get("candidate_method"),
                "update emission candidate mismatch",
            )
            manifests.append(manifest)
        _require(actual_ids == expected_ids, "update emission source inventory mismatch")
        ready = [
            row
            for row in _source_jsonl(due_source, "due_windows.jsonl")
            if row.get("can_update") is True
        ]
        if ready:
            _require(
                isinstance(snapshot.get("market_data_source"), Mapping),
                "update market data missing",
            )
            errors.extend(_validate_market_binding(_mapping(snapshot.get("market_data_source"))))
        else:
            _require(snapshot.get("market_data_source") is None, "no-ready update consumed market")
        _chronology(generated, *manifests)
        _require(snapshot.get("production_effect") == "none", "update production boundary invalid")
    except Exception as exc:  # noqa: BLE001
        errors.append(str(exc))
    return errors


@promotion._with_validation_session
def run_smoothed_outcome_update(
    *,
    due_id: str,
    due_dir: Path = DEFAULT_SMOOTHED_OUTCOME_DUE_DIR,
    emission_dir: Path = DEFAULT_SMOOTHED_DAILY_EMISSION_DIR,
    output_dir: Path = DEFAULT_SMOOTHED_OUTCOME_UPDATE_DIR,
    price_cache_path: Path = DEFAULT_PRICE_CACHE_PATH,
    rates_cache_path: Path = DEFAULT_RATES_CACHE_PATH,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = _generated_at(generated_at)
    snapshot = _update_snapshot(
        due_id=due_id,
        due_dir=due_dir,
        emission_dir=emission_dir,
        price_cache_path=price_cache_path,
        rates_cache_path=rates_cache_path,
        generated=generated,
    )
    errors = _validate_update_snapshot(snapshot)
    _require(not errors, "; ".join(errors))
    artifact_id = _stable_id("smoothed-outcome-update", snapshot)
    root = _unique_dir(output_dir / artifact_id)
    views, payload = _update_views(snapshot, update_id=root.name, root=root)
    _write(root, views, "latest_smoothed_outcome_update", "smoothed_outcome_update_manifest.json")
    return {"update_id": root.name, "update_dir": root, **payload}


def smoothed_outcome_update_report_payload(
    *,
    update_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_SMOOTHED_OUTCOME_UPDATE_DIR,
) -> dict[str, Any]:
    root = _artifact_root(output_dir, update_id, latest, "latest_smoothed_outcome_update")
    return {
        **_read_json(root / "smoothed_outcome_update_manifest.json"),
        "updated_smoothed_outcomes": _read_jsonl(root / "updated_smoothed_outcomes.jsonl"),
        "skipped_smoothed_outcomes": _read_jsonl(root / "skipped_smoothed_outcomes.jsonl"),
        "smoothed_outcome_delta_summary": _read_json(root / "smoothed_outcome_delta_summary.json"),
        "reader_brief_section": (root / "reader_brief_section.md").read_text(encoding="utf-8"),
        "input_snapshot": _read_json(root / "smoothed_outcome_update_input_snapshot.json"),
        "update_dir": str(root),
    }


@promotion._with_validation_session
def validate_smoothed_outcome_update_artifact(
    *, update_id: str, output_dir: Path = DEFAULT_SMOOTHED_OUTCOME_UPDATE_DIR
) -> dict[str, Any]:
    root = output_dir / update_id
    snapshot = (
        legacy._read_optional_json(root / "smoothed_outcome_update_input_snapshot.json") or {}
    )
    errors = _validate_update_snapshot(snapshot)
    mismatches: list[str] = []
    try:
        views, payload = _update_views(snapshot, update_id=update_id, root=root)
        mismatches = _view_errors(root, views)
        _require(
            _safe(
                _mapping(payload.get("manifest")),
                _mapping(payload.get("smoothed_outcome_delta_summary")),
                *_records(payload.get("updated_smoothed_outcomes")),
                *_records(payload.get("skipped_smoothed_outcomes")),
            ),
            "update safety fields invalid",
        )
    except Exception as exc:  # noqa: BLE001
        errors.append(str(exc))
    return _validation_payload(
        "etf_dynamic_v3_smoothed_outcome_update_validation",
        update_id,
        errors,
        mismatches,
        artifact_id_key="update_id",
    )


def _turnover(left: Mapping[str, Any], right: Mapping[str, Any]) -> float:
    symbols = set(left) | set(right)
    return round(
        0.5
        * sum(
            abs(
                _number(left.get(symbol, 0.0), field=f"left.{symbol}")
                - _number(right.get(symbol, 0.0), field=f"right.{symbol}")
            )
            for symbol in symbols
        ),
        10,
    )


def _classes(outcome: Mapping[str, Any]) -> tuple[list[str], str]:
    context = _text(outcome.get("regime_context"), "unknown")
    if context in {"sideways_choppy", "strong_recovery"}:
        return [context], "HIGH"
    if context in {"tech_drawdown", "semiconductor_pullback"}:
        return ["fast_regime_change"], "MEDIUM"
    returns = _mapping(outcome.get("method_returns"))
    candidate = returns.get(_text(outcome.get("candidate_method")))
    baseline = returns.get(_text(outcome.get("baseline_method")))
    if not _finite(candidate) or not _finite(baseline):
        return ["unknown"], "LOW"
    proxy = max(abs(float(candidate)), abs(float(baseline)))
    if proxy <= SIDEWAYS_ABS_RETURN_THRESHOLD:
        return ["sideways_choppy"], "LOW"
    if max(float(candidate), float(baseline)) >= STRONG_RECOVERY_RETURN_THRESHOLD:
        return ["strong_recovery"], "LOW"
    if proxy >= FAST_REGIME_CHANGE_ABS_RETURN_THRESHOLD:
        return ["fast_regime_change"], "LOW"
    return ["normal"], "LOW"


def _classified_rows(
    outcomes: Sequence[Mapping[str, Any]], emission_sources: Sequence[Mapping[str, Any]]
) -> list[dict[str, Any]]:
    events, weights_by_id = _events_and_weights(emission_sources)
    event_ids = {_text(row.get("event_id")) for row in events}
    rows: list[dict[str, Any]] = []
    for outcome in outcomes:
        event_id = _text(outcome.get("event_id"))
        _require(event_id in event_ids, "classification outcome event missing")
        weights = _mapping(_mapping(weights_by_id.get(event_id)).get("weights"))
        candidate = _text(outcome.get("candidate_method"))
        baseline = _text(outcome.get("baseline_method"))
        _require(candidate in weights and baseline in weights, "classification weights missing")
        classes, confidence = _classes(outcome)
        relative = _mapping(outcome.get("relative_metrics")).get("candidate_vs_baseline")
        recovery = any(item in {"strong_recovery", "fast_regime_change"} for item in classes)
        rows.append(
            {
                "schema_version": 2,
                "binding_id": outcome.get("binding_id"),
                "candidate_method": candidate,
                "baseline_method": baseline,
                "target_id": outcome.get("target_id"),
                "event_id": event_id,
                "as_of": outcome.get("as_of"),
                "window_days": outcome.get("window_days"),
                "regime_classification": classes,
                "classification_confidence": confidence,
                "sideways_relevant": "sideways_choppy" in classes,
                "recovery_lag_relevant": recovery,
                "candidate_vs_baseline": relative,
                "smooth_3d_vs_limited": _mapping(outcome.get("relative_metrics")).get(
                    "smooth_3d_vs_limited"
                ),
                "turnover_delta": _turnover(
                    _mapping(weights.get(candidate)), _mapping(weights.get(baseline))
                ),
                "signal_churn_delta": None,
                "lag_warning": recovery
                and _finite(relative)
                and float(relative) < LAG_WARNING_DELTA_THRESHOLD,
                "event_status": "AVAILABLE",
                **SYSTEM_TARGET_SAFETY,
            }
        )
    return rows


def _classification_summary(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    sideways = [row for row in rows if row.get("sideways_relevant") is True]
    recovery = [row for row in rows if row.get("recovery_lag_relevant") is True]
    fast = [row for row in rows if "fast_regime_change" in row.get("regime_classification", [])]
    return {
        "schema_version": 2,
        "classification_id": "",
        "events_classified": len(rows),
        "sideways_events_available": len({_text(row.get("event_id")) for row in sideways}),
        "recovery_events_available": len({_text(row.get("event_id")) for row in recovery}),
        "fast_regime_change_events_available": len({_text(row.get("event_id")) for row in fast}),
        "lag_warning_count": sum(row.get("lag_warning") is True for row in rows),
        "sideways_progress_delta": len(sideways),
        "recovery_progress_delta": len(recovery),
        "threshold_policy": {
            "sideways_abs_return": SIDEWAYS_ABS_RETURN_THRESHOLD,
            "strong_recovery_return": STRONG_RECOVERY_RETURN_THRESHOLD,
            "fast_regime_change_abs_return": FAST_REGIME_CHANGE_ABS_RETURN_THRESHOLD,
            "lag_warning_delta": LAG_WARNING_DELTA_THRESHOLD,
            "role": "reporting_only_invariant",
        },
        **SYSTEM_TARGET_SAFETY,
    }


def _classification_snapshot(
    *,
    update_id: str,
    update_dir: Path,
    emission_dir: Path,
    generated: datetime,
) -> dict[str, Any]:
    update_source = _update_source(update_id, update_dir)
    manifest = _source_json(update_source, "smoothed_outcome_update_manifest.json")
    emissions = [
        _emission_source(_text(item), emission_dir) for item in manifest.get("emission_ids", [])
    ]
    return {
        "schema_version": CLASSIFICATION_SNAPSHOT_SCHEMA,
        "generated_at": generated.isoformat(),
        "update_source": update_source,
        "emission_sources": emissions,
        "threshold_policy": {
            "sideways_abs_return": SIDEWAYS_ABS_RETURN_THRESHOLD,
            "strong_recovery_return": STRONG_RECOVERY_RETURN_THRESHOLD,
            "fast_regime_change_abs_return": FAST_REGIME_CHANGE_ABS_RETURN_THRESHOLD,
            "lag_warning_delta": LAG_WARNING_DELTA_THRESHOLD,
            "role": "reporting_only_invariant",
        },
        "production_effect": "none",
    }


def _render_classification_report(manifest: Mapping[str, Any], summary: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            f"# Smoothed Forward Classification {manifest.get('classification_id')}",
            "",
            f"- update_id: {manifest.get('update_id')}",
            f"- binding_id: {manifest.get('binding_id')}",
            f"- candidate_method: {manifest.get('candidate_method')}",
            f"- events_classified: {summary.get('events_classified')}",
            f"- sideways_events: {summary.get('sideways_events_available')}",
            f"- recovery_events: {summary.get('recovery_events_available')}",
            f"- lag_warning_count: {summary.get('lag_warning_count')}",
            "- threshold_role: reporting_only_invariant",
            "- broker_action_allowed: false",
            "- production_effect: none",
            "",
        ]
    )


def _classification_views(
    snapshot: Mapping[str, Any], *, classification_id: str, root: Path
) -> tuple[dict[str, bytes], dict[str, Any]]:
    update_source = _mapping(snapshot.get("update_source"))
    update_manifest = _source_json(update_source, "smoothed_outcome_update_manifest.json")
    outcomes = _source_jsonl(update_source, "updated_smoothed_outcomes.jsonl")
    rows = _classified_rows(outcomes, _records(snapshot.get("emission_sources")))
    summary = _classification_summary(rows)
    summary["classification_id"] = classification_id
    manifest = {
        "schema_version": 2,
        "report_type": "etf_dynamic_v3_smoothed_forward_classification_manifest",
        "classification_id": classification_id,
        "update_id": update_manifest.get("update_id"),
        "binding_id": update_manifest.get("binding_id"),
        "candidate_method": update_manifest.get("candidate_method"),
        "binding_status": update_manifest.get("binding_status"),
        "emission_ids": update_manifest.get("emission_ids", []),
        "generated_at": snapshot.get("generated_at"),
        "status": "PASS",
        "smoothed_forward_classification_input_snapshot_path": str(
            root / "smoothed_forward_classification_input_snapshot.json"
        ),
        "smoothed_forward_classification_manifest_path": str(
            root / "smoothed_forward_classification_manifest.json"
        ),
        "classified_forward_events_path": str(root / "classified_forward_events.jsonl"),
        "classification_summary_path": str(root / "classification_summary.json"),
        "smoothed_forward_classification_summary_path": str(
            root / "smoothed_forward_classification_summary.json"
        ),
        "smoothed_forward_classification_report_path": str(
            root / "smoothed_forward_classification_report.md"
        ),
        **SYSTEM_TARGET_SAFETY,
    }
    report = _render_classification_report(manifest, summary)
    views = {
        "smoothed_forward_classification_input_snapshot.json": _json_bytes(dict(snapshot)),
        "smoothed_forward_classification_manifest.json": _json_bytes(manifest),
        "classified_forward_events.jsonl": _jsonl_bytes(rows),
        "classification_summary.json": _json_bytes(summary),
        "smoothed_forward_classification_summary.json": _json_bytes(summary),
        "smoothed_forward_classification_report.md": report.encode("utf-8"),
    }
    return views, {
        "manifest": manifest,
        "classified_forward_events": rows,
        "classification_summary": summary,
    }


def _validate_classification_snapshot(snapshot: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    try:
        _require(
            snapshot.get("schema_version") == CLASSIFICATION_SNAPSHOT_SCHEMA,
            "classification snapshot schema invalid",
        )
        generated = target_core._datetime(
            snapshot.get("generated_at"), field="classification generated_at"
        )
        update_source = _mapping(snapshot.get("update_source"))
        errors.extend(
            operations._validate_local_binding(
                update_source,
                kind="smoothed_outcome_update",
                validator=validate_smoothed_outcome_update_artifact,
                validator_key="update_id",
            )
        )
        update_manifest = _source_json(update_source, "smoothed_outcome_update_manifest.json")
        _chronology(generated, update_manifest)
        expected_ids = [_text(item) for item in update_manifest.get("emission_ids", [])]
        actual_ids: list[str] = []
        manifests: list[Mapping[str, Any]] = []
        for source in _records(snapshot.get("emission_sources")):
            errors.extend(
                operations._validate_local_binding(
                    source,
                    kind="smoothed_daily_emission",
                    validator=validate_smoothed_daily_emission_artifact,
                    validator_key="emission_id",
                )
            )
            manifest = _source_json(source, "smoothed_daily_emission_manifest.json")
            actual_ids.append(_text(manifest.get("emission_id")))
            _require(
                manifest.get("binding_id") == update_manifest.get("binding_id"),
                "classification emission binding mismatch",
            )
            _require(
                manifest.get("candidate_method") == update_manifest.get("candidate_method"),
                "classification emission candidate mismatch",
            )
            manifests.append(manifest)
        _require(actual_ids == expected_ids, "classification emission inventory mismatch")
        _require(
            _mapping(snapshot.get("threshold_policy"))
            == _classification_summary([]).get("threshold_policy"),
            "classification threshold policy drift",
        )
        _chronology(generated, *manifests)
        _classified_rows(
            _source_jsonl(update_source, "updated_smoothed_outcomes.jsonl"),
            _records(snapshot.get("emission_sources")),
        )
        _require(
            snapshot.get("production_effect") == "none",
            "classification production boundary invalid",
        )
    except Exception as exc:  # noqa: BLE001
        errors.append(str(exc))
    return errors


@promotion._with_validation_session
def run_smoothed_forward_classification(
    *,
    update_id: str,
    update_dir: Path = DEFAULT_SMOOTHED_OUTCOME_UPDATE_DIR,
    emission_dir: Path = DEFAULT_SMOOTHED_DAILY_EMISSION_DIR,
    output_dir: Path = DEFAULT_SMOOTHED_FORWARD_CLASSIFICATION_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = _generated_at(generated_at)
    snapshot = _classification_snapshot(
        update_id=update_id,
        update_dir=update_dir,
        emission_dir=emission_dir,
        generated=generated,
    )
    errors = _validate_classification_snapshot(snapshot)
    _require(not errors, "; ".join(errors))
    artifact_id = _stable_id("smoothed-forward-classification", snapshot)
    root = _unique_dir(output_dir / artifact_id)
    views, payload = _classification_views(snapshot, classification_id=root.name, root=root)
    _write(
        root,
        views,
        "latest_smoothed_forward_classification",
        "smoothed_forward_classification_manifest.json",
    )
    return {"classification_id": root.name, "classification_dir": root, **payload}


def smoothed_forward_classification_report_payload(
    *,
    classification_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_SMOOTHED_FORWARD_CLASSIFICATION_DIR,
) -> dict[str, Any]:
    root = _artifact_root(
        output_dir, classification_id, latest, "latest_smoothed_forward_classification"
    )
    return {
        **_read_json(root / "smoothed_forward_classification_manifest.json"),
        "classified_forward_events": _read_jsonl(root / "classified_forward_events.jsonl"),
        "classification_summary": _read_json(root / "classification_summary.json"),
        "input_snapshot": _read_json(root / "smoothed_forward_classification_input_snapshot.json"),
        "classification_dir": str(root),
    }


@promotion._with_validation_session
def validate_smoothed_forward_classification_artifact(
    *,
    classification_id: str,
    output_dir: Path = DEFAULT_SMOOTHED_FORWARD_CLASSIFICATION_DIR,
) -> dict[str, Any]:
    root = output_dir / classification_id
    snapshot = (
        legacy._read_optional_json(root / "smoothed_forward_classification_input_snapshot.json")
        or {}
    )
    errors = _validate_classification_snapshot(snapshot)
    mismatches: list[str] = []
    try:
        views, payload = _classification_views(
            snapshot, classification_id=classification_id, root=root
        )
        mismatches = _view_errors(root, views)
        _require(
            _safe(
                _mapping(payload.get("manifest")),
                _mapping(payload.get("classification_summary")),
                *_records(payload.get("classified_forward_events")),
            ),
            "classification safety fields invalid",
        )
    except Exception as exc:  # noqa: BLE001
        errors.append(str(exc))
    return _validation_payload(
        "etf_dynamic_v3_smoothed_forward_classification_validation",
        classification_id,
        errors,
        mismatches,
        artifact_id_key="classification_id",
    )


def _renewal_source(renewal_id: str, root: Path) -> dict[str, Any]:
    return operations._local_source_binding(
        kind="smoothed_owner_renewal",
        artifact_id=renewal_id,
        root=root,
        validator=operations.validate_smoothed_owner_renewal_artifact,
        validator_key="renewal_id",
        json_views=("smoothed_owner_renewal_manifest.json", "owner_renewal_options.json"),
        text_views=(
            "owner_renewal_checklist.md",
            "smoothed_owner_renewal_report.md",
            "reader_brief_section.md",
        ),
    )


def _resolve_required_id(
    *,
    explicit: str | None,
    root: Path,
    manifest_name: str,
    id_key: str,
    validator: Any,
    generated: datetime,
) -> str:
    resolved = explicit or _select_latest_valid(
        root=root,
        manifest_name=manifest_name,
        id_key=id_key,
        validator=validator,
        cutoff=generated,
    )
    _require(bool(resolved), f"{manifest_name} source required")
    return _text(resolved)


def _weekly_step_source(
    step: str,
    artifact_id: str,
    *,
    emission_dir: Path,
    due_dir: Path,
    update_dir: Path,
    classification_dir: Path,
    progress_dir: Path,
    dashboard_dir: Path,
    monitor_dir: Path,
    recheck_dir: Path,
    renewal_dir: Path,
) -> dict[str, Any]:
    builders = {
        "daily_emission": lambda: _emission_source(artifact_id, emission_dir),
        "outcome_due_scan": lambda: _due_source(artifact_id, due_dir),
        "outcome_update": lambda: _update_source(artifact_id, update_dir),
        "forward_classification": lambda: _classification_source(artifact_id, classification_dir),
        "progress_update": lambda: operations._progress_binding(artifact_id, progress_dir),
        "weekly_dashboard": lambda: operations._dashboard_binding(artifact_id, dashboard_dir),
        "event_monitor": lambda: operations._monitor_binding(artifact_id, monitor_dir),
        "switch_readiness": lambda: operations._recheck_binding(artifact_id, recheck_dir),
        "owner_renewal": lambda: _renewal_source(artifact_id, renewal_dir),
    }
    _require(step in builders, f"unknown weekly step: {step}")
    return {"step": step, "artifact_id": artifact_id, "source": builders[step]()}


def _weekly_manifest(source: Mapping[str, Any], name: str) -> dict[str, Any]:
    return _source_json(_mapping(source.get("source")), name)


def _weekly_snapshot(
    *,
    week_ending: date,
    target_id: str | None,
    binding_id: str | None,
    switch_plan_id: str | None,
    owner_promotion_id: str | None,
    model_target_dir: Path,
    emission_dir: Path,
    due_dir: Path,
    update_dir: Path,
    classification_dir: Path,
    binding_dir: Path,
    progress_dir: Path,
    dashboard_dir: Path,
    monitor_dir: Path,
    switch_plan_dir: Path,
    recheck_dir: Path,
    owner_promotion_dir: Path,
    renewal_dir: Path,
    price_cache_path: Path,
    rates_cache_path: Path,
    generated: datetime,
) -> dict[str, Any]:
    resolved_binding = _resolve_required_id(
        explicit=binding_id,
        root=binding_dir,
        manifest_name="smoothed_forward_binding_manifest.json",
        id_key="binding_id",
        validator=promotion.validate_smoothed_forward_binding_artifact,
        generated=generated,
    )
    resolved_switch = _resolve_required_id(
        explicit=switch_plan_id,
        root=switch_plan_dir,
        manifest_name="paper_shadow_primary_switch_manifest.json",
        id_key="switch_plan_id",
        validator=promotion.validate_paper_shadow_primary_switch_artifact,
        generated=generated,
    )
    resolved_owner = _resolve_required_id(
        explicit=owner_promotion_id,
        root=owner_promotion_dir,
        manifest_name="smoothed_owner_promotion_manifest.json",
        id_key="decision_id",
        validator=promotion.validate_smoothed_owner_promotion_artifact,
        generated=generated,
    )
    binding_source = promotion._binding_binding(resolved_binding, binding_dir)
    bound = _bound(binding_source)
    candidate = bound.get("candidate_method")

    emission = run_smoothed_daily_emission(
        as_of=week_ending,
        target_id=target_id,
        binding_id=resolved_binding,
        model_target_dir=model_target_dir,
        binding_dir=binding_dir,
        output_dir=emission_dir,
        price_cache_path=price_cache_path,
        rates_cache_path=rates_cache_path,
        generated_at=generated,
    )
    due = scan_smoothed_outcome_due(
        as_of=week_ending,
        binding_id=resolved_binding,
        binding_dir=binding_dir,
        emission_dir=emission_dir,
        output_dir=due_dir,
        price_cache_path=price_cache_path,
        rates_cache_path=rates_cache_path,
        generated_at=generated,
    )
    update = run_smoothed_outcome_update(
        due_id=due["due_id"],
        due_dir=due_dir,
        emission_dir=emission_dir,
        output_dir=update_dir,
        price_cache_path=price_cache_path,
        rates_cache_path=rates_cache_path,
        generated_at=generated,
    )
    classification = run_smoothed_forward_classification(
        update_id=update["update_id"],
        update_dir=update_dir,
        emission_dir=emission_dir,
        output_dir=classification_dir,
        generated_at=generated,
    )
    progress = operations.update_smoothed_forward_progress(
        binding_id=resolved_binding,
        binding_dir=binding_dir,
        output_dir=progress_dir,
        outcome_update_dir=update_dir,
        classification_dir=classification_dir,
        outcome_update_ids=(update["update_id"],) if candidate is not None else (),
        classification_ids=(classification["classification_id"],) if candidate is not None else (),
        generated_at=generated,
    )
    dashboard = operations.build_smoothed_weekly_dashboard(
        progress_id=progress["progress_id"],
        progress_dir=progress_dir,
        output_dir=dashboard_dir,
        generated_at=generated,
    )
    monitor = operations.update_smoothed_event_monitor(
        progress_id=progress["progress_id"],
        progress_dir=progress_dir,
        output_dir=monitor_dir,
        generated_at=generated,
    )
    recheck = operations.recheck_smoothed_switch_readiness(
        dashboard_id=dashboard["dashboard_id"],
        monitor_id=monitor["monitor_id"],
        switch_plan_id=resolved_switch,
        dashboard_dir=dashboard_dir,
        monitor_dir=monitor_dir,
        switch_plan_dir=switch_plan_dir,
        output_dir=recheck_dir,
        generated_at=generated,
    )
    renewal = operations.build_smoothed_owner_renewal_pack(
        recheck_id=recheck["recheck_id"],
        owner_promotion_id=resolved_owner,
        recheck_dir=recheck_dir,
        owner_promotion_dir=owner_promotion_dir,
        output_dir=renewal_dir,
        generated_at=generated,
    )
    ids = (
        ("daily_emission", emission["emission_id"]),
        ("outcome_due_scan", due["due_id"]),
        ("outcome_update", update["update_id"]),
        ("forward_classification", classification["classification_id"]),
        ("progress_update", progress["progress_id"]),
        ("weekly_dashboard", dashboard["dashboard_id"]),
        ("event_monitor", monitor["monitor_id"]),
        ("switch_readiness", recheck["recheck_id"]),
        ("owner_renewal", renewal["renewal_id"]),
    )
    step_sources = [
        _weekly_step_source(
            step,
            artifact_id,
            emission_dir=emission_dir,
            due_dir=due_dir,
            update_dir=update_dir,
            classification_dir=classification_dir,
            progress_dir=progress_dir,
            dashboard_dir=dashboard_dir,
            monitor_dir=monitor_dir,
            recheck_dir=recheck_dir,
            renewal_dir=renewal_dir,
        )
        for step, artifact_id in ids
    ]
    return {
        "schema_version": WEEKLY_SNAPSHOT_SCHEMA,
        "generated_at": generated.isoformat(),
        "week_ending": week_ending.isoformat(),
        "binding_source": binding_source,
        "switch_source": promotion._switch_binding(resolved_switch, switch_plan_dir),
        "owner_promotion_source": operations._owner_promotion_binding(
            resolved_owner, owner_promotion_dir
        ),
        "step_sources": step_sources,
        "production_effect": "none",
    }


def _weekly_source_map(snapshot: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    rows = _records(snapshot.get("step_sources"))
    result = {_text(row.get("step")): row for row in rows}
    _require(len(result) == len(rows), "weekly duplicate step source")
    return result


def _weekly_summary(snapshot: Mapping[str, Any], *, weekly_run_id: str) -> dict[str, Any]:
    sources = _weekly_source_map(snapshot)
    emission = _weekly_manifest(sources["daily_emission"], "smoothed_daily_emission_manifest.json")
    due = _source_json(_mapping(sources["outcome_due_scan"].get("source")), "due_summary.json")
    update = _source_json(
        _mapping(sources["outcome_update"].get("source")),
        "smoothed_outcome_delta_summary.json",
    )
    classification = _source_json(
        _mapping(sources["forward_classification"].get("source")),
        "smoothed_forward_classification_summary.json",
    )
    progress = _source_json(
        _mapping(sources["progress_update"].get("source")),
        "smoothed_forward_progress_summary.json",
    )
    recheck = _source_json(
        _mapping(sources["switch_readiness"].get("source")),
        "switch_readiness_decision.json",
    )
    renewal = _source_json(
        _mapping(sources["owner_renewal"].get("source")), "owner_renewal_options.json"
    )
    return {
        "schema_version": 2,
        "weekly_run_id": weekly_run_id,
        "week_ending": snapshot.get("week_ending"),
        "binding_id": emission.get("binding_id"),
        "candidate_method": emission.get("candidate_method"),
        "emitted_events": emission.get("emitted_event_count", 0),
        "due_windows": due.get("due_windows", 0),
        "updated_windows": update.get("updated_count", 0),
        "classified_events": classification.get("events_classified", 0),
        "available_forward_events": progress.get("available_forward_events_total", 0),
        "required_forward_events": progress.get("required_forward_events_total", 0),
        "available_sideways_events": progress.get("available_sideways_events", 0),
        "required_sideways_events": progress.get("required_sideways_events", 0),
        "available_recovery_events": progress.get("available_recovery_events", 0),
        "required_recovery_events": progress.get("required_recovery_events", 0),
        "switch_readiness_status": recheck.get("recheck_decision"),
        "owner_recommendation": renewal.get("recommended_owner_action"),
        "can_execute_switch": False,
        "weekly_recommendation": "continue_observation",
        **SYSTEM_TARGET_SAFETY,
    }


def _render_weekly_report(
    manifest: Mapping[str, Any], summary: Mapping[str, Any], steps: Sequence[Mapping[str, Any]]
) -> str:
    return "\n".join(
        [
            f"# Smoothed Forward Weekly Run {manifest.get('weekly_run_id')}",
            "",
            f"- week_ending: {manifest.get('week_ending')}",
            f"- binding_id: {summary.get('binding_id')}",
            f"- candidate_method: {summary.get('candidate_method')}",
            f"- emitted_events: {summary.get('emitted_events')}",
            f"- updated_windows: {summary.get('updated_windows')}",
            f"- switch_readiness_status: {summary.get('switch_readiness_status')}",
            f"- owner_recommendation: {summary.get('owner_recommendation')}",
            f"- step_count: {len(steps)}",
            "- can_execute_switch: false",
            "- broker_action_allowed: false",
            "- production_effect: none",
            "",
            "九段 artifact 均以精确 id/source commitment 绑定；workflow PASS 不等于投资结论。",
            "",
        ]
    )


def _render_weekly_reader(summary: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "## Dynamic Rescue Smoothed Forward Weekly Run",
            "",
            f"- weekly_run_id: {summary.get('weekly_run_id')}",
            f"- week_ending: {summary.get('week_ending')}",
            f"- candidate_method: {summary.get('candidate_method')}",
            f"- emitted_events: {summary.get('emitted_events')}",
            f"- updated_windows: {summary.get('updated_windows')}",
            f"- owner_recommendation: {summary.get('owner_recommendation')}",
            "- can_execute_switch: false",
            "- broker_action_allowed: false",
            "- production_effect: none",
            "",
        ]
    )


def _weekly_views(
    snapshot: Mapping[str, Any], *, weekly_run_id: str, root: Path
) -> tuple[dict[str, bytes], dict[str, Any]]:
    sources = _weekly_source_map(snapshot)
    steps = [
        {"step": step, "status": "PASS", "artifact_id": row.get("artifact_id")}
        for step, row in sources.items()
    ]
    artifacts = {step: {"artifact_id": row.get("artifact_id")} for step, row in sources.items()}
    summary = _weekly_summary(snapshot, weekly_run_id=weekly_run_id)
    manifest = {
        "schema_version": 2,
        "report_type": "etf_dynamic_v3_smoothed_forward_weekly_run_manifest",
        "weekly_run_id": weekly_run_id,
        "binding_id": summary.get("binding_id"),
        "candidate_method": summary.get("candidate_method"),
        "week_ending": snapshot.get("week_ending"),
        "generated_at": snapshot.get("generated_at"),
        "status": "PASS",
        "smoothed_forward_weekly_run_input_snapshot_path": str(
            root / "smoothed_forward_weekly_run_input_snapshot.json"
        ),
        "smoothed_forward_weekly_run_manifest_path": str(
            root / "smoothed_forward_weekly_run_manifest.json"
        ),
        "weekly_run_steps_path": str(root / "weekly_run_steps.json"),
        "weekly_run_artifacts_path": str(root / "weekly_run_artifacts.json"),
        "weekly_run_summary_path": str(root / "weekly_run_summary.json"),
        "smoothed_forward_weekly_run_report_path": str(
            root / "smoothed_forward_weekly_run_report.md"
        ),
        "reader_brief_section_path": str(root / "reader_brief_section.md"),
        **SYSTEM_TARGET_SAFETY,
    }
    step_payload = {"schema_version": 2, "steps": steps, **SYSTEM_TARGET_SAFETY}
    artifact_payload = {
        "schema_version": 2,
        "artifacts": artifacts,
        **SYSTEM_TARGET_SAFETY,
    }
    report = _render_weekly_report(manifest, summary, steps)
    reader = _render_weekly_reader(summary)
    views = {
        "smoothed_forward_weekly_run_input_snapshot.json": _json_bytes(dict(snapshot)),
        "smoothed_forward_weekly_run_manifest.json": _json_bytes(manifest),
        "weekly_run_steps.json": _json_bytes(step_payload),
        "weekly_run_artifacts.json": _json_bytes(artifact_payload),
        "weekly_run_summary.json": _json_bytes(summary),
        "smoothed_forward_weekly_run_report.md": report.encode("utf-8"),
        "reader_brief_section.md": reader.encode("utf-8"),
    }
    return views, {
        "manifest": manifest,
        "weekly_run_steps": step_payload,
        "weekly_run_artifacts": artifact_payload,
        "weekly_run_summary": summary,
        "reader_brief_section": reader,
    }


def _validate_weekly_snapshot(snapshot: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    try:
        _require(
            snapshot.get("schema_version") == WEEKLY_SNAPSHOT_SCHEMA,
            "weekly snapshot schema invalid",
        )
        generated = target_core._datetime(snapshot.get("generated_at"), field="weekly generated_at")
        week_ending = _iso_date(snapshot.get("week_ending"), field="week ending")
        _require(week_ending <= generated.date(), "week ending after generated cutoff")
        authority_specs = (
            (
                "binding_source",
                "smoothed_forward_binding",
                promotion.validate_smoothed_forward_binding_artifact,
                "binding_id",
                "smoothed_forward_binding_manifest.json",
            ),
            (
                "switch_source",
                "paper_shadow_primary_switch",
                promotion.validate_paper_shadow_primary_switch_artifact,
                "switch_plan_id",
                "paper_shadow_primary_switch_manifest.json",
            ),
            (
                "owner_promotion_source",
                "smoothed_owner_promotion",
                promotion.validate_smoothed_owner_promotion_artifact,
                "decision_id",
                "smoothed_owner_promotion_manifest.json",
            ),
        )
        authority_manifests: list[Mapping[str, Any]] = []
        for field, kind, validator, key, manifest_name in authority_specs:
            source = _mapping(snapshot.get(field))
            if field in {"binding_source", "switch_source"}:
                errors.extend(
                    promotion._validate_binding(
                        source, kind=kind, validator=validator, validator_key=key
                    )
                )
            else:
                errors.extend(
                    operations._validate_local_binding(
                        source, kind=kind, validator=validator, validator_key=key
                    )
                )
            authority_manifests.append(_source_json(source, manifest_name))
        sources = _weekly_source_map(snapshot)
        required = {
            "daily_emission",
            "outcome_due_scan",
            "outcome_update",
            "forward_classification",
            "progress_update",
            "weekly_dashboard",
            "event_monitor",
            "switch_readiness",
            "owner_renewal",
        }
        _require(set(sources) == required, "weekly step source inventory invalid")
        specs = {
            "daily_emission": (
                "smoothed_daily_emission",
                validate_smoothed_daily_emission_artifact,
                "emission_id",
                "smoothed_daily_emission_manifest.json",
            ),
            "outcome_due_scan": (
                "smoothed_outcome_due",
                validate_smoothed_outcome_due_artifact,
                "due_id",
                "smoothed_outcome_due_manifest.json",
            ),
            "outcome_update": (
                "smoothed_outcome_update",
                validate_smoothed_outcome_update_artifact,
                "update_id",
                "smoothed_outcome_update_manifest.json",
            ),
            "forward_classification": (
                "smoothed_forward_classification",
                validate_smoothed_forward_classification_artifact,
                "classification_id",
                "smoothed_forward_classification_manifest.json",
            ),
            "progress_update": (
                "smoothed_forward_progress",
                operations.validate_smoothed_forward_progress_artifact,
                "progress_id",
                "smoothed_forward_progress_manifest.json",
            ),
            "weekly_dashboard": (
                "smoothed_weekly_dashboard",
                operations.validate_smoothed_weekly_dashboard_artifact,
                "dashboard_id",
                "smoothed_weekly_dashboard_manifest.json",
            ),
            "event_monitor": (
                "smoothed_event_monitor",
                operations.validate_smoothed_event_monitor_artifact,
                "monitor_id",
                "smoothed_event_monitor_manifest.json",
            ),
            "switch_readiness": (
                "smoothed_switch_readiness",
                operations.validate_smoothed_switch_readiness_artifact,
                "recheck_id",
                "smoothed_switch_readiness_manifest.json",
            ),
            "owner_renewal": (
                "smoothed_owner_renewal",
                operations.validate_smoothed_owner_renewal_artifact,
                "renewal_id",
                "smoothed_owner_renewal_manifest.json",
            ),
        }
        manifests: dict[str, Mapping[str, Any]] = {}
        for step, row in sources.items():
            kind, validator, key, manifest_name = specs[step]
            source = _mapping(row.get("source"))
            errors.extend(
                operations._validate_local_binding(
                    source, kind=kind, validator=validator, validator_key=key
                )
            )
            _require(
                row.get("artifact_id") == source.get("artifact_id"), "weekly artifact id mismatch"
            )
            manifests[step] = _source_json(source, manifest_name)
        binding_id = authority_manifests[0].get("binding_id")
        candidate = authority_manifests[0].get("candidate_method")
        for step in (
            "daily_emission",
            "outcome_due_scan",
            "outcome_update",
            "forward_classification",
            "progress_update",
        ):
            _require(
                manifests[step].get("binding_id") == binding_id, f"weekly {step} binding mismatch"
            )
            _require(
                manifests[step].get("candidate_method") == candidate,
                f"weekly {step} candidate mismatch",
            )
        _require(
            manifests["outcome_update"].get("due_id")
            == manifests["outcome_due_scan"].get("due_id"),
            "weekly due/update lineage mismatch",
        )
        _require(
            manifests["forward_classification"].get("update_id")
            == manifests["outcome_update"].get("update_id"),
            "weekly update/classification lineage mismatch",
        )
        _chronology(generated, *authority_manifests, *manifests.values())
        summary = _weekly_summary(snapshot, weekly_run_id="validation")
        if candidate is None:
            _require(summary.get("emitted_events") == 0, "candidate-less weekly emitted events")
            _require(summary.get("updated_windows") == 0, "candidate-less weekly updated outcomes")
        _require(summary.get("can_execute_switch") is False, "weekly switch execution enabled")
        _require(snapshot.get("production_effect") == "none", "weekly production boundary invalid")
    except Exception as exc:  # noqa: BLE001
        errors.append(str(exc))
    return errors


@promotion._with_validation_session
def run_smoothed_forward_weekly_run(
    *,
    week_ending: date,
    target_id: str | None = None,
    binding_id: str | None = None,
    switch_plan_id: str | None = None,
    owner_promotion_id: str | None = None,
    model_target_dir: Path = DEFAULT_MODEL_TARGET_DIR,
    emission_dir: Path = DEFAULT_SMOOTHED_DAILY_EMISSION_DIR,
    due_dir: Path = DEFAULT_SMOOTHED_OUTCOME_DUE_DIR,
    update_dir: Path = DEFAULT_SMOOTHED_OUTCOME_UPDATE_DIR,
    classification_dir: Path = DEFAULT_SMOOTHED_FORWARD_CLASSIFICATION_DIR,
    binding_dir: Path = DEFAULT_SMOOTHED_FORWARD_BINDING_DIR,
    progress_dir: Path = DEFAULT_SMOOTHED_FORWARD_PROGRESS_DIR,
    dashboard_dir: Path = DEFAULT_SMOOTHED_WEEKLY_DASHBOARD_DIR,
    monitor_dir: Path = DEFAULT_SMOOTHED_EVENT_MONITOR_DIR,
    switch_plan_dir: Path = DEFAULT_PAPER_SHADOW_PRIMARY_SWITCH_DIR,
    recheck_dir: Path = DEFAULT_SMOOTHED_SWITCH_READINESS_DIR,
    owner_promotion_dir: Path = DEFAULT_SMOOTHED_OWNER_PROMOTION_DIR,
    renewal_dir: Path = DEFAULT_SMOOTHED_OWNER_RENEWAL_DIR,
    output_dir: Path = DEFAULT_SMOOTHED_FORWARD_WEEKLY_RUN_DIR,
    price_cache_path: Path = DEFAULT_PRICE_CACHE_PATH,
    rates_cache_path: Path = DEFAULT_RATES_CACHE_PATH,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = _generated_at(generated_at)
    snapshot = _weekly_snapshot(
        week_ending=week_ending,
        target_id=target_id,
        binding_id=binding_id,
        switch_plan_id=switch_plan_id,
        owner_promotion_id=owner_promotion_id,
        model_target_dir=model_target_dir,
        emission_dir=emission_dir,
        due_dir=due_dir,
        update_dir=update_dir,
        classification_dir=classification_dir,
        binding_dir=binding_dir,
        progress_dir=progress_dir,
        dashboard_dir=dashboard_dir,
        monitor_dir=monitor_dir,
        switch_plan_dir=switch_plan_dir,
        recheck_dir=recheck_dir,
        owner_promotion_dir=owner_promotion_dir,
        renewal_dir=renewal_dir,
        price_cache_path=price_cache_path,
        rates_cache_path=rates_cache_path,
        generated=generated,
    )
    errors = _validate_weekly_snapshot(snapshot)
    _require(not errors, "; ".join(errors))
    artifact_id = _stable_id("smoothed-forward-weekly-run", snapshot)
    root = _unique_dir(output_dir / artifact_id)
    views, payload = _weekly_views(snapshot, weekly_run_id=root.name, root=root)
    _write(
        root,
        views,
        "latest_smoothed_forward_weekly_run",
        "smoothed_forward_weekly_run_manifest.json",
    )
    return {"weekly_run_id": root.name, "weekly_run_dir": root, **payload}


def smoothed_forward_weekly_run_report_payload(
    *,
    weekly_run_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_SMOOTHED_FORWARD_WEEKLY_RUN_DIR,
) -> dict[str, Any]:
    root = _artifact_root(output_dir, weekly_run_id, latest, "latest_smoothed_forward_weekly_run")
    return {
        **_read_json(root / "smoothed_forward_weekly_run_manifest.json"),
        "weekly_run_steps": _read_json(root / "weekly_run_steps.json"),
        "weekly_run_artifacts": _read_json(root / "weekly_run_artifacts.json"),
        "weekly_run_summary": _read_json(root / "weekly_run_summary.json"),
        "reader_brief_section": (root / "reader_brief_section.md").read_text(encoding="utf-8"),
        "input_snapshot": _read_json(root / "smoothed_forward_weekly_run_input_snapshot.json"),
        "weekly_run_dir": str(root),
    }


@promotion._with_validation_session
def validate_smoothed_forward_weekly_run_artifact(
    *,
    weekly_run_id: str,
    output_dir: Path = DEFAULT_SMOOTHED_FORWARD_WEEKLY_RUN_DIR,
) -> dict[str, Any]:
    root = output_dir / weekly_run_id
    snapshot = (
        legacy._read_optional_json(root / "smoothed_forward_weekly_run_input_snapshot.json") or {}
    )
    errors = _validate_weekly_snapshot(snapshot)
    mismatches: list[str] = []
    try:
        views, payload = _weekly_views(snapshot, weekly_run_id=weekly_run_id, root=root)
        mismatches = _view_errors(root, views)
        _require(
            _safe(
                _mapping(payload.get("manifest")),
                _mapping(payload.get("weekly_run_steps")),
                _mapping(payload.get("weekly_run_artifacts")),
                _mapping(payload.get("weekly_run_summary")),
            ),
            "weekly safety fields invalid",
        )
    except Exception as exc:  # noqa: BLE001
        errors.append(str(exc))
    return _validation_payload(
        "etf_dynamic_v3_smoothed_forward_weekly_run_validation",
        weekly_run_id,
        errors,
        mismatches,
        artifact_id_key="weekly_run_id",
    )
