from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, date, datetime
from hashlib import sha256
from pathlib import Path
from typing import Any

import pandas as pd

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.shadow.lineage import git_commit_sha, sha256_file
from ai_trading_system.trading_engine.parameters.parameter_loader import (
    DEFAULT_SHADOW_BACKTEST_CONFIG_PATH,
    load_production_parameters,
    load_shadow_backtest_config,
    resolve_project_path,
)

SIGNAL_SNAPSHOT_SCHEMA_VERSION = 1
SIGNAL_SNAPSHOT_REPORT_TYPE = "signal_snapshot"
SIGNAL_SNAPSHOT_ALIAS_REPORT_TYPE = "signal_snapshot_report"

REQUIRED_SIGNALS: tuple[str, ...] = (
    "macro_liquidity",
    "trend_momentum",
    "sector_strength",
    "earnings_quality",
    "valuation_risk",
    "event_risk",
)
PRICE_DERIVED_SIGNALS: tuple[str, ...] = ("trend_momentum", "sector_strength")
PROXY_SIGNALS: tuple[str, ...] = ("macro_liquidity",)
NEUTRAL_FALLBACK_SIGNALS: tuple[str, ...] = (
    "earnings_quality",
    "valuation_risk",
    "event_risk",
)
SIGNAL_STATUS_VALUES: set[str] = {"OK", "LIMITED", "NEUTRAL_FALLBACK", "MISSING", "FAILED"}

# TRADING-050 v0.1 pilot policy: 0.5 is the explicitly documented neutral signal value.
NEUTRAL_SIGNAL_VALUE = 0.5

# TRADING-050 v0.1 pilot normalization bands. They convert price-derived percentage
# moves into 0..1 scores around neutral without claiming calibrated alpha.
RETURN_20D_NORMALIZATION_BAND = 0.20
RETURN_60D_NORMALIZATION_BAND = 0.35
RETURN_120D_NORMALIZATION_BAND = 0.50
MA_RELATIVE_NORMALIZATION_BAND = 0.25
RELATIVE_STRENGTH_NORMALIZATION_BAND = 0.25

TREND_RETURN_WINDOWS: tuple[int, ...] = (20, 60, 120)
TREND_MA_FAST_WINDOW = 50
TREND_MA_SLOW_WINDOW = 200
SECTOR_RELATIVE_WINDOWS: tuple[int, ...] = (20, 60, 120)
SECTOR_STRENGTH_BENCHMARK = "QQQ"


@dataclass(frozen=True)
class SignalSnapshotRun:
    as_of: date
    payload: dict[str, Any]
    json_path: Path
    markdown_path: Path


def default_signal_snapshot_root() -> Path:
    return PROJECT_ROOT / "artifacts" / "signal_snapshots"


def default_signal_snapshot_dir(output_root: Path, as_of: date) -> Path:
    return output_root / as_of.isoformat()


def default_signal_snapshot_json_path(output_root: Path, as_of: date) -> Path:
    return default_signal_snapshot_dir(output_root, as_of) / "signal_snapshot.json"


def default_signal_snapshot_markdown_path(output_root: Path, as_of: date) -> Path:
    return default_signal_snapshot_dir(output_root, as_of) / "signal_snapshot.md"


def latest_signal_snapshot_path(output_root: Path | None = None) -> Path | None:
    root = output_root or default_signal_snapshot_root()
    candidates = sorted(root.glob("*/signal_snapshot.json"))
    if not candidates:
        return None
    return max(candidates, key=lambda path: path.stat().st_mtime)


def run_signal_snapshot_build(
    *,
    as_of: date | None = None,
    config_path: Path | str = DEFAULT_SHADOW_BACKTEST_CONFIG_PATH,
    output_root: Path | None = None,
    generated_at: datetime | None = None,
    dry_run: bool = False,
    price_derived_only: bool = False,
) -> SignalSnapshotRun:
    root = output_root or (
        PROJECT_ROOT / "outputs" / "dry_runs" / "signal_snapshots"
        if dry_run
        else default_signal_snapshot_root()
    )
    payload = build_signal_snapshot_payload(
        as_of=as_of,
        config_path=config_path,
        generated_at=generated_at,
        price_derived_only=price_derived_only,
    )
    resolved_as_of = date.fromisoformat(str(payload["metadata"]["as_of"]))
    json_path = default_signal_snapshot_json_path(root, resolved_as_of)
    markdown_path = default_signal_snapshot_markdown_path(root, resolved_as_of)
    write_signal_snapshot(payload, json_path, markdown_path)
    return SignalSnapshotRun(
        as_of=resolved_as_of,
        payload=payload,
        json_path=json_path,
        markdown_path=markdown_path,
    )


def build_signal_snapshot_payload(
    *,
    as_of: date | None = None,
    config_path: Path | str = DEFAULT_SHADOW_BACKTEST_CONFIG_PATH,
    generated_at: datetime | None = None,
    price_derived_only: bool = False,
    prices: pd.DataFrame | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(tz=UTC)
    resolved_config_path = resolve_project_path(config_path)
    config = load_shadow_backtest_config(resolved_config_path)
    baseline = load_production_parameters(resolve_project_path(config.baseline_parameters_path))
    prices_path = resolve_project_path(config.data.prices_path)
    price_frame = prices if prices is not None else _read_prices(prices_path)
    resolved_as_of = as_of or _latest_price_date(price_frame) or generated.date()
    assets = tuple(asset for asset in baseline.flattened_asset_universe() if asset != "CASH")
    panel = _prepare_price_panel(price_frame, assets, resolved_as_of)
    requested_signals = PRICE_DERIVED_SIGNALS if price_derived_only else REQUIRED_SIGNALS

    signals: dict[str, Any] = {}
    if "trend_momentum" in requested_signals:
        signals["trend_momentum"] = _build_trend_momentum_signal(panel, assets)
    if "sector_strength" in requested_signals:
        signals["sector_strength"] = _build_sector_strength_signal(panel, assets)
    if "macro_liquidity" in requested_signals:
        signals["macro_liquidity"] = _build_neutral_signal(
            panel,
            assets,
            status="LIMITED",
            quality="proxy_or_neutral",
            method="neutral_fallback_v0_1",
            reason="Reliable PIT macro-liquidity dataset is not available in v0.1.",
        )
    for signal in NEUTRAL_FALLBACK_SIGNALS:
        if signal in requested_signals:
            signals[signal] = _build_neutral_signal(
                panel,
                assets,
                status="NEUTRAL_FALLBACK",
                quality="neutral_fallback",
                method="neutral_fallback_v0_1",
                reason=_neutral_reason(signal),
            )

    overall = _overall_quality(signals)
    data_start, data_end = _panel_date_range(panel, resolved_as_of)
    status = str(overall["status"])
    return {
        "schema_version": SIGNAL_SNAPSHOT_SCHEMA_VERSION,
        "report_type": SIGNAL_SNAPSHOT_REPORT_TYPE,
        "metadata": {
            "snapshot_id": f"signal-snapshot-{resolved_as_of.isoformat()}",
            "as_of": resolved_as_of.isoformat(),
            "generated_at": generated.isoformat(),
            "status": status,
            "production_effect": "none",
            "manual_review_required": True,
            "auto_promotion": False,
            "price_derived_only": price_derived_only,
            "market_regime": config.market_regime.id,
            "market_regime_anchor": config.market_regime.anchor_date.isoformat(),
            "market_regime_anchor_event": config.market_regime.anchor_event,
            "data_range": {"start": data_start.isoformat(), "end": data_end.isoformat()},
            "asset_universe": list(assets),
            "required_signals": list(REQUIRED_SIGNALS),
            "config_path": str(resolved_config_path),
            "config_hash": _config_hash(resolved_config_path),
            "price_data_path": str(prices_path),
            "code_version": git_commit_sha() or "unknown",
        },
        "signals": signals,
        "overall_quality": overall,
    }


def write_signal_snapshot(
    payload: dict[str, Any],
    json_path: Path,
    markdown_path: Path,
) -> tuple[Path, Path]:
    json_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    markdown_path.write_text(render_signal_snapshot_markdown(payload), encoding="utf-8")
    return json_path, markdown_path


def render_signal_snapshot_markdown(payload: dict[str, Any]) -> str:
    metadata = _mapping(payload.get("metadata"))
    overall = signal_snapshot_summary(payload)
    signals = _mapping(payload.get("signals"))
    lines = [
        "# Signal Snapshot",
        "",
        "## 1. Summary",
        "",
        f"- snapshot_id：`{metadata.get('snapshot_id', 'UNKNOWN')}`",
        f"- as_of：`{metadata.get('as_of', 'UNKNOWN')}`",
        f"- status：`{overall.get('status', 'UNKNOWN')}`",
        f"- production_effect：`{metadata.get('production_effect', 'none')}`",
        f"- manual_review_required：`{metadata.get('manual_review_required', True)}`",
        f"- auto_promotion：`{metadata.get('auto_promotion', False)}`",
        f"- real_signals：`{overall.get('real_signal_count', 0)}`",
        f"- proxy_signals：`{overall.get('proxy_signal_count', 0)}`",
        f"- fallback_signals：`{overall.get('fallback_signal_count', 0)}`",
        f"- missing_signals：`{overall.get('missing_signal_count', 0)}`",
        f"- can_run_full_signal_backtest：`{overall.get('can_run_full_signal_backtest', False)}`",
        f"- can_promote_candidate：`{overall.get('can_promote_candidate', False)}`",
        "",
        "## 2. Signal Quality",
        "",
        "| Signal | Status | Quality | Method | Coverage | Values |",
        "|---|---|---|---|---:|---:|",
    ]
    for signal in REQUIRED_SIGNALS:
        item = _mapping(signals.get(signal))
        values = _records(item.get("values"))
        lines.append(
            "| "
            f"`{signal}` | "
            f"`{item.get('status', 'MISSING')}` | "
            f"`{item.get('quality', 'missing')}` | "
            f"`{item.get('method', 'missing')}` | "
            f"{_format_ratio(item.get('coverage'))} | "
            f"{len(values)} |"
        )
    lines.extend(
        [
            "",
            "## 3. Limitations",
            "",
            "- `trend_momentum` 和 `sector_strength` 使用价格序列派生，适合验证回测链路。",
            "- `macro_liquidity`、`earnings_quality`、`valuation_risk` 和 `event_risk` "
            "在 v0.1 仍包含 proxy / neutral fallback，不得解释为完整信号质量 OK。",
            "- 本 artifact 只读生成，不能修改 production 参数或触发交易。",
        ]
    )
    return "\n".join(lines).rstrip() + "\n"


def load_signal_snapshot_payload(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def validate_signal_snapshot_payload(payload: dict[str, Any]) -> list[str]:
    issues: list[str] = []
    if payload.get("schema_version") != SIGNAL_SNAPSHOT_SCHEMA_VERSION:
        issues.append("schema_version mismatch")
    if payload.get("report_type") != SIGNAL_SNAPSHOT_REPORT_TYPE:
        issues.append("report_type mismatch")
    metadata = _mapping(payload.get("metadata"))
    if metadata.get("production_effect") != "none":
        issues.append("production_effect must be none")
    if metadata.get("manual_review_required") is not True:
        issues.append("manual_review_required must be true")
    if metadata.get("auto_promotion") is not False:
        issues.append("auto_promotion must be false")
    signals = _mapping(payload.get("signals"))
    missing = [signal for signal in REQUIRED_SIGNALS if signal not in signals]
    if missing:
        issues.append("missing required signals: " + ", ".join(missing))
    for signal, item in signals.items():
        if not isinstance(item, dict):
            issues.append(f"{signal} must be an object")
            continue
        status = str(item.get("status") or "MISSING")
        if status not in SIGNAL_STATUS_VALUES:
            issues.append(f"{signal} has unsupported status: {status}")
        if not isinstance(item.get("values"), list):
            issues.append(f"{signal} values must be a list")
    return issues


def signal_snapshot_summary(payload: dict[str, Any]) -> dict[str, Any]:
    overall = _mapping(payload.get("overall_quality"))
    signals = _mapping(payload.get("signals"))
    real = _strings(overall.get("real_signals")) or [
        signal
        for signal, item in signals.items()
        if _mapping(item).get("quality") == "price_derived"
    ]
    proxy = _strings(overall.get("proxy_signals")) or [
        signal
        for signal, item in signals.items()
        if _mapping(item).get("quality") in {"proxy_or_neutral", "price_proxy"}
    ]
    fallback = _strings(overall.get("neutral_fallback_signals")) or [
        signal
        for signal, item in signals.items()
        if _mapping(item).get("status") == "NEUTRAL_FALLBACK"
    ]
    missing = _strings(overall.get("missing_signals")) or [
        signal for signal in REQUIRED_SIGNALS if signal not in signals
    ]
    failed = _strings(overall.get("failed_signals")) or [
        signal for signal, item in signals.items() if _mapping(item).get("status") == "FAILED"
    ]
    status = str(
        overall.get("status") or _status_from_signal_groups(missing, failed, fallback, proxy)
    )
    return {
        "status": status,
        "real_signals": real,
        "proxy_signals": proxy,
        "neutral_fallback_signals": fallback,
        "missing_signals": missing,
        "failed_signals": failed,
        "real_signal_count": len(real),
        "proxy_signal_count": len(proxy),
        "fallback_signal_count": len(fallback),
        "missing_signal_count": len(missing),
        "failed_signal_count": len(failed),
        "coverage": _overall_coverage(signals),
        "can_run_full_signal_backtest": bool(
            overall.get("can_run_full_signal_backtest", not missing and not failed)
        ),
        "can_promote_candidate": bool(
            overall.get("can_promote_candidate", status == "OK" and not missing and not failed)
        ),
    }


def signal_snapshot_frames(payload: dict[str, Any]) -> dict[str, pd.DataFrame]:
    frames: dict[str, pd.DataFrame] = {}
    for signal, item in _mapping(payload.get("signals")).items():
        values = _records(_mapping(item).get("values"))
        rows: list[dict[str, Any]] = []
        for value in values:
            rows.append(
                {
                    "date": value.get("date"),
                    "asset": value.get("asset"),
                    "value": value.get("value"),
                }
            )
        if not rows:
            continue
        frame = pd.DataFrame(rows)
        frame["_date"] = pd.to_datetime(frame["date"], errors="coerce")
        frame["_value"] = pd.to_numeric(frame["value"], errors="coerce")
        frame = frame.loc[frame["_date"].notna() & frame["_value"].notna()]
        if frame.empty:
            continue
        frames[str(signal)] = (
            frame.pivot_table(index="_date", columns="asset", values="_value", aggfunc="last")
            .sort_index()
            .clip(lower=0.0, upper=1.0)
        )
    return frames


def report_alias_paths(reports_dir: Path, as_of: date) -> tuple[Path, Path]:
    return (
        reports_dir / f"signal_snapshot_{as_of.isoformat()}.json",
        reports_dir / f"signal_snapshot_{as_of.isoformat()}.md",
    )


def write_signal_snapshot_report_alias(
    payload: dict[str, Any],
    reports_dir: Path,
    as_of: date,
) -> tuple[Path, Path]:
    alias_payload = {
        **payload,
        "report_type": SIGNAL_SNAPSHOT_ALIAS_REPORT_TYPE,
        "source_report_type": SIGNAL_SNAPSHOT_REPORT_TYPE,
    }
    json_path, markdown_path = report_alias_paths(reports_dir, as_of)
    return write_signal_snapshot(alias_payload, json_path, markdown_path)


def _build_trend_momentum_signal(panel: pd.DataFrame, assets: tuple[str, ...]) -> dict[str, Any]:
    if panel.empty:
        return _failed_signal("price data unavailable for trend_momentum")
    returns = {
        "return_20d": panel.pct_change(20),
        "return_60d": panel.pct_change(60),
        "return_120d": panel.pct_change(120),
    }
    ma_fast = panel.rolling(TREND_MA_FAST_WINDOW, min_periods=5).mean()
    ma_slow = panel.rolling(TREND_MA_SLOW_WINDOW, min_periods=20).mean()
    features = {
        **returns,
        "ma_50_vs_200": ma_fast.div(ma_slow).sub(1.0),
        "price_vs_200ma": panel.div(ma_slow).sub(1.0),
    }
    scores = [
        _normalize_signed_feature(returns["return_20d"], RETURN_20D_NORMALIZATION_BAND),
        _normalize_signed_feature(returns["return_60d"], RETURN_60D_NORMALIZATION_BAND),
        _normalize_signed_feature(returns["return_120d"], RETURN_120D_NORMALIZATION_BAND),
        _normalize_signed_feature(features["ma_50_vs_200"], MA_RELATIVE_NORMALIZATION_BAND),
        _normalize_signed_feature(features["price_vs_200ma"], MA_RELATIVE_NORMALIZATION_BAND),
    ]
    score = sum(scores) / len(scores)
    score = score.reindex(columns=list(assets)).fillna(NEUTRAL_SIGNAL_VALUE)
    values = _value_rows(score, features)
    return {
        "status": "OK",
        "quality": "price_derived",
        "method": "moving_average_and_return_momentum",
        "coverage": _coverage(values, panel, assets),
        "values": values,
    }


def _build_sector_strength_signal(panel: pd.DataFrame, assets: tuple[str, ...]) -> dict[str, Any]:
    if panel.empty:
        return _failed_signal("price data unavailable for sector_strength")
    benchmark = (
        panel[SECTOR_STRENGTH_BENCHMARK]
        if SECTOR_STRENGTH_BENCHMARK in panel.columns
        else panel.mean(axis=1)
    )
    features: dict[str, pd.DataFrame] = {}
    scores: list[pd.DataFrame] = []
    for window, band in (
        (20, RETURN_20D_NORMALIZATION_BAND),
        (60, RETURN_60D_NORMALIZATION_BAND),
        (120, RETURN_120D_NORMALIZATION_BAND),
    ):
        relative = panel.pct_change(window).sub(benchmark.pct_change(window), axis=0)
        name = f"relative_return_{window}d"
        features[name] = relative
        scores.append(
            _normalize_signed_feature(
                relative,
                min(band, RELATIVE_STRENGTH_NORMALIZATION_BAND),
            )
        )
    score = sum(scores) / len(scores)
    score = score.reindex(columns=list(assets)).fillna(NEUTRAL_SIGNAL_VALUE)
    values = _value_rows(score, features)
    return {
        "status": "OK",
        "quality": "price_derived",
        "method": f"relative_strength_vs_{SECTOR_STRENGTH_BENCHMARK}",
        "coverage": _coverage(values, panel, assets),
        "values": values,
    }


def _build_neutral_signal(
    panel: pd.DataFrame,
    assets: tuple[str, ...],
    *,
    status: str,
    quality: str,
    method: str,
    reason: str,
) -> dict[str, Any]:
    values: list[dict[str, Any]] = []
    for timestamp in panel.index:
        signal_date = pd.Timestamp(timestamp).date().isoformat()
        for asset in assets:
            values.append(
                {
                    "date": signal_date,
                    "asset": asset,
                    "value": NEUTRAL_SIGNAL_VALUE,
                    "features": {"neutral_value": NEUTRAL_SIGNAL_VALUE},
                    "reason": reason,
                }
            )
    return {
        "status": status,
        "quality": quality,
        "method": method,
        "coverage": _coverage(values, panel, assets),
        "reason": reason,
        "values": values,
    }


def _overall_quality(signals: dict[str, Any]) -> dict[str, Any]:
    missing = [signal for signal in REQUIRED_SIGNALS if signal not in signals]
    failed = [
        signal for signal, item in signals.items() if _mapping(item).get("status") == "FAILED"
    ]
    real = [
        signal
        for signal, item in signals.items()
        if _mapping(item).get("status") == "OK" and _mapping(item).get("quality") == "price_derived"
    ]
    proxy = [
        signal
        for signal, item in signals.items()
        if _mapping(item).get("status") == "LIMITED"
        and _mapping(item).get("quality") in {"proxy_or_neutral", "price_proxy"}
    ]
    fallback = [
        signal
        for signal, item in signals.items()
        if _mapping(item).get("status") == "NEUTRAL_FALLBACK"
        or _mapping(item).get("quality") == "neutral_fallback"
    ]
    status = _status_from_signal_groups(missing, failed, fallback, proxy)
    return {
        "status": status,
        "real_signals": real,
        "proxy_signals": proxy,
        "neutral_fallback_signals": fallback,
        "missing_signals": missing,
        "failed_signals": failed,
        "real_signal_count": len(real),
        "proxy_signal_count": len(proxy),
        "fallback_signal_count": len(fallback),
        "missing_signal_count": len(missing),
        "failed_signal_count": len(failed),
        "coverage": _overall_coverage(signals),
        "can_run_full_signal_backtest": not missing and not failed,
        "can_promote_candidate": status == "OK",
    }


def _status_from_signal_groups(
    missing: list[str],
    failed: list[str],
    fallback: list[str],
    proxy: list[str],
) -> str:
    if failed:
        return "FAILED"
    if missing:
        return "LIMITED"
    if fallback or proxy:
        return "LIMITED"
    return "OK"


def _prepare_price_panel(
    prices: pd.DataFrame,
    assets: tuple[str, ...],
    as_of: date,
) -> pd.DataFrame:
    if prices.empty or not {"date", "ticker", "adj_close"}.issubset(prices.columns):
        return pd.DataFrame()
    frame = prices.loc[prices["ticker"].astype(str).isin(assets)].copy()
    frame["_date"] = pd.to_datetime(frame["date"], errors="coerce")
    frame["_adj_close"] = pd.to_numeric(frame["adj_close"], errors="coerce")
    frame = frame.loc[frame["_date"].notna() & frame["_adj_close"].notna()]
    frame = frame.loc[frame["_date"].dt.date <= as_of]
    if frame.empty:
        return pd.DataFrame()
    return (
        frame.pivot_table(index="_date", columns="ticker", values="_adj_close", aggfunc="last")
        .sort_index()
        .reindex(columns=list(assets))
        .ffill()
    )


def _normalize_signed_feature(frame: pd.DataFrame, band: float) -> pd.DataFrame:
    return (NEUTRAL_SIGNAL_VALUE + frame / (2.0 * band)).clip(lower=0.0, upper=1.0)


def _value_rows(score: pd.DataFrame, features: dict[str, pd.DataFrame]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for timestamp in score.index:
        signal_date = pd.Timestamp(timestamp).date().isoformat()
        for asset in score.columns:
            feature_values = {
                name: _rounded_or_none(frame.reindex_like(score).loc[timestamp, asset])
                for name, frame in features.items()
            }
            rows.append(
                {
                    "date": signal_date,
                    "asset": str(asset),
                    "value": _rounded_or_neutral(score.loc[timestamp, asset]),
                    "features": feature_values,
                }
            )
    return rows


def _failed_signal(reason: str) -> dict[str, Any]:
    return {
        "status": "FAILED",
        "quality": "missing",
        "method": "not_built",
        "coverage": 0.0,
        "reason": reason,
        "values": [],
    }


def _coverage(values: list[dict[str, Any]], panel: pd.DataFrame, assets: tuple[str, ...]) -> float:
    expected = len(panel.index) * len(assets)
    if expected <= 0:
        return 0.0
    return round(min(1.0, len(values) / expected), 6)


def _overall_coverage(signals: dict[str, Any]) -> float:
    coverages = []
    for item in signals.values():
        try:
            coverages.append(float(_mapping(item).get("coverage")))
        except (TypeError, ValueError):
            continue
    if not coverages:
        return 0.0
    return round(min(coverages), 6)


def _panel_date_range(panel: pd.DataFrame, fallback: date) -> tuple[date, date]:
    if panel.empty:
        return fallback, fallback
    return pd.Timestamp(panel.index.min()).date(), pd.Timestamp(panel.index.max()).date()


def _neutral_reason(signal: str) -> str:
    return {
        "earnings_quality": "PIT earnings dataset not available in v0.1.",
        "valuation_risk": "PIT valuation dataset is not wired into signal snapshots in v0.1.",
        "event_risk": "No PIT event dataset available in v0.1.",
    }.get(signal, "Signal dataset not available in v0.1.")


def _read_prices(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except (OSError, pd.errors.ParserError):
        return pd.DataFrame()


def _latest_price_date(prices: pd.DataFrame) -> date | None:
    if prices.empty or "date" not in prices:
        return None
    parsed = pd.to_datetime(prices["date"], errors="coerce").dropna()
    if parsed.empty:
        return None
    return pd.Timestamp(parsed.max()).date()


def _rounded_or_neutral(value: object) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return NEUTRAL_SIGNAL_VALUE
    if pd.isna(number):
        return NEUTRAL_SIGNAL_VALUE
    return round(max(0.0, min(1.0, number)), 6)


def _rounded_or_none(value: object) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if pd.isna(number):
        return None
    return round(number, 6)


def _format_ratio(value: object) -> str:
    try:
        return f"{float(value):.4f}"
    except (TypeError, ValueError):
        return "NA"


def _config_hash(config_path: Path) -> str:
    if not config_path.exists():
        return "missing"
    digest = sha256()
    digest.update(str(config_path).encode("utf-8"))
    digest.update(sha256_file(config_path).encode("utf-8"))
    return digest.hexdigest()


def _mapping(value: object) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _records(value: object) -> list[dict[str, Any]]:
    return [item for item in value if isinstance(item, dict)] if isinstance(value, list) else []


def _strings(value: object) -> list[str]:
    if isinstance(value, list | tuple):
        return [str(item) for item in value if str(item)]
    return []
