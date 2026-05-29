from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
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
from ai_trading_system.trading_engine.parameters.parameter_schema import (
    DEFAULT_BACKTEST_INPUT_CACHE_FRESHNESS_MAX_AGE_DAYS,
    ShadowBacktestConfig,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

BACKTEST_INPUT_DIAGNOSTICS_SCHEMA_VERSION = 1
BACKTEST_INPUT_DIAGNOSTICS_REPORT_TYPE = "backtest_input_diagnostics"
BACKTEST_INPUT_MANIFEST_SCHEMA_VERSION = 1
BACKTEST_INPUT_MANIFEST_REPORT_TYPE = "backtest_input_manifest"
DEFAULT_REQUIRED_BACKTEST_ASSETS: tuple[str, ...] = (
    "QQQ",
    "SMH",
    "NVDA",
    "TSM",
    "MSFT",
    "GOOGL",
    "BRK.B",
    "SGOV",
)
REQUIRED_SIGNAL_SNAPSHOTS: tuple[str, ...] = (
    "macro_liquidity",
    "trend_momentum",
    "sector_strength",
    "earnings_quality",
    "valuation_risk",
    "event_risk",
)
PRICE_PROXY_SIGNALS: tuple[str, ...] = ("trend_momentum", "sector_strength")


@dataclass(frozen=True)
class BacktestInputDiagnosticsRun:
    as_of: date
    payload: dict[str, Any]
    manifest: dict[str, Any]
    json_path: Path
    markdown_path: Path
    manifest_path: Path


def default_backtest_input_diagnostics_dir(output_root: Path, as_of: date) -> Path:
    return output_root / "data_quality" / as_of.isoformat()


def default_backtest_input_diagnostics_json_path(output_root: Path, as_of: date) -> Path:
    return default_backtest_input_diagnostics_dir(output_root, as_of) / (
        "backtest_input_diagnostics.json"
    )


def default_backtest_input_diagnostics_markdown_path(output_root: Path, as_of: date) -> Path:
    return default_backtest_input_diagnostics_dir(output_root, as_of) / (
        "backtest_input_diagnostics.md"
    )


def default_backtest_input_manifest_path(output_root: Path, as_of: date) -> Path:
    return output_root / "backtest_snapshots" / as_of.isoformat() / "backtest_input_manifest.json"


def latest_backtest_input_diagnostics_path(output_root: Path | None = None) -> Path | None:
    root = (output_root or (PROJECT_ROOT / "artifacts")) / "data_quality"
    candidates = sorted(root.glob("*/backtest_input_diagnostics.json"))
    if not candidates:
        return None
    return max(candidates, key=lambda path: path.stat().st_mtime)


def load_backtest_input_diagnostics(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def run_backtest_input_diagnostics(
    *,
    as_of: date | None = None,
    config_path: Path | str = DEFAULT_SHADOW_BACKTEST_CONFIG_PATH,
    output_root: Path | None = None,
    generated_at: datetime | None = None,
) -> BacktestInputDiagnosticsRun:
    root = output_root or (PROJECT_ROOT / "artifacts")
    generated = generated_at or datetime.now().astimezone()
    resolved_config_path = resolve_project_path(config_path)
    config = load_shadow_backtest_config(resolved_config_path)
    raw_config = _load_raw_config(resolved_config_path)
    prices_path = resolve_project_path(config.data.prices_path)
    prices = _read_prices(prices_path)
    resolved_as_of = as_of or _latest_price_date(prices) or generated.date()
    json_path = default_backtest_input_diagnostics_json_path(root, resolved_as_of)
    markdown_path = default_backtest_input_diagnostics_markdown_path(root, resolved_as_of)
    manifest_path = default_backtest_input_manifest_path(root, resolved_as_of)
    payload = build_backtest_input_diagnostics_payload(
        as_of=resolved_as_of,
        generated_at=generated,
        config=config,
        raw_config=raw_config,
        config_path=resolved_config_path,
        prices=prices,
        diagnostic_json_path=json_path,
    )
    manifest = build_backtest_input_manifest(
        as_of=resolved_as_of,
        generated_at=generated,
        config=config,
        raw_config=raw_config,
        config_path=resolved_config_path,
        diagnostic_json_path=json_path,
        diagnostics_payload=payload,
    )
    write_backtest_input_diagnostics(payload, json_path, markdown_path)
    write_backtest_input_manifest(manifest, manifest_path)
    return BacktestInputDiagnosticsRun(
        as_of=resolved_as_of,
        payload=payload,
        manifest=manifest,
        json_path=json_path,
        markdown_path=markdown_path,
        manifest_path=manifest_path,
    )


def build_backtest_input_diagnostics_payload(
    *,
    as_of: date,
    generated_at: datetime,
    config: ShadowBacktestConfig,
    raw_config: dict[str, Any],
    config_path: Path,
    prices: pd.DataFrame | None = None,
    diagnostic_json_path: Path | None = None,
) -> dict[str, Any]:
    price_frame = (
        prices
        if prices is not None
        else _read_prices(resolve_project_path(config.data.prices_path))
    )
    required_assets = _required_assets(config)
    required_signals = _required_signals(config)
    required_start = _required_start_date(
        price_frame,
        required_assets,
        as_of,
        config.walk_forward.min_history_days,
    )
    required_end = as_of
    asset_coverage = _asset_coverage(price_frame, required_assets, as_of)
    date_coverage = _date_coverage(
        price_frame,
        required_assets,
        required_start=required_start,
        required_end=required_end,
    )
    price_data = _price_data_completeness(
        price_frame,
        required_assets,
        required_start=required_start,
        required_end=required_end,
        max_allowed_missing_ratio=config.data_quality_rules.missing_price_data.max_missing_ratio,
    )
    signal_snapshots = _signal_snapshot_availability(
        raw_config,
        price_frame,
        required_signals=required_signals,
        as_of=as_of,
    )
    cache_freshness = _cache_freshness(
        config,
        raw_config,
        generated_at=generated_at,
        signal_snapshot_files=_signal_snapshot_files(raw_config, required_signals, as_of),
    )
    checks = {
        "asset_coverage": asset_coverage,
        "date_coverage": date_coverage,
        "price_data": price_data,
        "signal_snapshots": signal_snapshots,
        "cache_freshness": cache_freshness,
    }
    repair_plan = _repair_plan(checks, required_start=required_start, required_end=required_end)
    summary = _summary(checks)
    summary["blocking_reasons"] = [
        reason.removeprefix("- ").strip() for reason in _blocking_error_lines(checks)
    ]
    status = str(summary["overall_status"])
    return {
        "schema_version": BACKTEST_INPUT_DIAGNOSTICS_SCHEMA_VERSION,
        "report_type": BACKTEST_INPUT_DIAGNOSTICS_REPORT_TYPE,
        "metadata": {
            "run_id": f"backtest-input-diagnostics-{as_of.isoformat()}",
            "generated_at": generated_at.isoformat(),
            "status": status,
            "production_effect": "none",
            "manual_review_required": status != "OK",
            "config_path": str(config_path),
            "config_hash": _config_hash(config_path),
            "code_version": git_commit_sha() or "unknown",
            "market_regime": config.market_regime.id,
            "market_regime_anchor": config.market_regime.anchor_date.isoformat(),
            "market_regime_anchor_event": config.market_regime.anchor_event,
            "requested_date_range": {
                "start": required_start.isoformat(),
                "end": required_end.isoformat(),
            },
            "diagnostic_report": "" if diagnostic_json_path is None else str(diagnostic_json_path),
        },
        "summary": summary,
        "checks": checks,
        "repair_plan": repair_plan,
    }


def build_backtest_input_manifest(
    *,
    as_of: date,
    generated_at: datetime,
    config: ShadowBacktestConfig,
    raw_config: dict[str, Any],
    config_path: Path,
    diagnostic_json_path: Path,
    diagnostics_payload: dict[str, Any],
) -> dict[str, Any]:
    checks = _mapping(diagnostics_payload.get("checks"))
    date_coverage = _mapping(checks.get("date_coverage"))
    summary = _mapping(diagnostics_payload.get("summary"))
    required_assets = _strings(_mapping(checks.get("asset_coverage")).get("required_assets"))
    required_start = _text(date_coverage.get("required_start_date"), as_of.isoformat())
    required_end = _text(date_coverage.get("required_end_date"), as_of.isoformat())
    prices_path = resolve_project_path(config.data.prices_path)
    signal_files = _signal_snapshot_files(raw_config, tuple(REQUIRED_SIGNAL_SNAPSHOTS), as_of)
    symbol_mapping = _price_symbol_mapping(prices_path, required_assets)
    return {
        "schema_version": BACKTEST_INPUT_MANIFEST_SCHEMA_VERSION,
        "report_type": BACKTEST_INPUT_MANIFEST_REPORT_TYPE,
        "snapshot_id": f"backtest-input-{as_of.isoformat()}",
        "generated_at": generated_at.isoformat(),
        "status": _text(summary.get("overall_status"), "UNKNOWN"),
        "production_effect": "none",
        "assets": required_assets,
        "symbol_mapping": symbol_mapping,
        "date_range": {"start": required_start, "end": required_end},
        "price_data_files": [str(prices_path)] if prices_path.exists() else [],
        "signal_snapshot_files": [str(path) for path in signal_files],
        "data_quality_report": str(diagnostic_json_path),
        "config_hash": _config_hash(config_path),
        "code_version": git_commit_sha() or "unknown",
    }


def write_backtest_input_diagnostics(
    payload: dict[str, Any],
    json_path: Path,
    markdown_path: Path,
) -> tuple[Path, Path]:
    json_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    markdown_path.write_text(render_backtest_input_diagnostics_markdown(payload), encoding="utf-8")
    return json_path, markdown_path


def write_backtest_input_manifest(manifest: dict[str, Any], output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    return output_path


def render_backtest_input_diagnostics_markdown(payload: dict[str, Any]) -> str:
    metadata = _mapping(payload.get("metadata"))
    summary = _mapping(payload.get("summary"))
    checks = _mapping(payload.get("checks"))
    asset_coverage = _mapping(checks.get("asset_coverage"))
    date_coverage = _mapping(checks.get("date_coverage"))
    price_data = _mapping(checks.get("price_data"))
    signal_snapshots = _mapping(checks.get("signal_snapshots"))
    cache_freshness = _mapping(checks.get("cache_freshness"))
    repair_plan = _mapping(payload.get("repair_plan"))
    blocking = _blocking_error_lines(checks)
    lines = [
        "# Backtest Input Diagnostics",
        "",
        "## 1. Executive Summary",
        "",
        f"- 状态：`{summary.get('overall_status', 'UNKNOWN')}`",
        f"- blocking_errors：`{summary.get('blocking_errors', 0)}`",
        f"- warnings：`{summary.get('warnings', 0)}`",
        f"- backtest_mode：`{summary.get('backtest_mode', 'UNKNOWN')}`",
        f"- can_run_shadow_backtest：`{summary.get('can_run_shadow_backtest', False)}`",
        f"- can_promote_candidate：`{summary.get('can_promote_candidate', False)}`",
        f"- production_effect：`{metadata.get('production_effect', 'none')}`",
        "",
        "## 2. Blocking Errors",
        "",
        *(blocking or ["- 无阻断错误。"]),
        "",
        "## 3. Asset Coverage",
        "",
        f"- 状态：`{asset_coverage.get('status', 'UNKNOWN')}`",
        f"- required_assets：{_join_text(asset_coverage.get('required_assets'))}",
        f"- available_assets：{_join_text(asset_coverage.get('available_assets'))}",
        f"- missing_assets：{_join_text(asset_coverage.get('missing_assets'))}",
        "",
        "## 4. Date Coverage",
        "",
        _definition_lines(
            date_coverage,
            (
                "status",
                "required_start_date",
                "required_end_date",
                "available_start_date",
                "available_end_date",
                "missing_start_gap_days",
                "missing_end_gap_days",
            ),
        ),
        "",
        "## 5. Price Data Completeness",
        "",
        f"- 状态：`{price_data.get('status', 'UNKNOWN')}`",
        f"- max_allowed_missing_ratio：`{price_data.get('max_allowed_missing_ratio', 'UNKNOWN')}`",
        "",
        "| Symbol | Status | Missing Ratio | Missing Dates Sample |",
        "|---|---|---:|---|",
    ]
    for asset in _records(price_data.get("assets")):
        lines.append(
            "| "
            f"`{asset.get('symbol', '')}` | "
            f"`{asset.get('status', 'UNKNOWN')}` | "
            f"{_format_ratio(asset.get('missing_ratio'))} | "
            f"{', '.join(_strings(asset.get('missing_dates_sample'))) or 'none'} |"
        )
    lines.extend(
        [
            "",
            "## 6. Signal Snapshot Availability",
            "",
            f"- 状态：`{signal_snapshots.get('status', 'UNKNOWN')}`",
            f"- required_signals：{_join_text(signal_snapshots.get('required_signals'))}",
            f"- available_signals：{_join_text(signal_snapshots.get('available_signals'))}",
            f"- missing_signals：{_join_text(signal_snapshots.get('missing_signals'))}",
            f"- fallback_mode：`{signal_snapshots.get('fallback_mode', 'none')}`",
            "",
            "## 7. Cache Freshness",
            "",
            f"- 状态：`{cache_freshness.get('status', 'UNKNOWN')}`",
            "",
            "| Item | Last Updated | Max Age Days | Actual Age Days | Status |",
            "|---|---|---:|---:|---|",
        ]
    )
    for item in _records(cache_freshness.get("items")):
        lines.append(
            "| "
            f"`{item.get('name', '')}` | "
            f"{item.get('last_updated') or 'MISSING'} | "
            f"{item.get('max_age_days', '')} | "
            f"{item.get('actual_age_days', '')} | "
            f"`{item.get('status', 'UNKNOWN')}` |"
        )
    lines.extend(
        [
            "",
            "## 8. Repair Plan",
            "",
            f"- 状态：`{repair_plan.get('status', 'UNKNOWN')}`",
            "",
        ]
    )
    for step in _records(repair_plan.get("steps")):
        lines.append(
            "- "
            f"step {step.get('step')}: `{step.get('action')}`；"
            f"required=`{step.get('required', False)}`；"
            f"assets={', '.join(_strings(step.get('assets'))) or 'n/a'}；"
            f"signals={', '.join(_strings(step.get('signals'))) or 'n/a'}；"
            f"items={', '.join(_strings(step.get('items'))) or 'n/a'}"
        )
    if not _records(repair_plan.get("steps")):
        lines.append("- 当前无需 repair。")
    impact_line = (
        "- Shadow backtest input gate passed; candidate evaluation may proceed as " "observe-only."
        if summary.get("can_run_shadow_backtest")
        and summary.get("backtest_mode") != "price_only_shadow_backtest"
        else (
            "- Price-only shadow backtest may run, but candidate promotion remains disabled "
            "until full signal snapshots are available."
            if summary.get("backtest_mode") == "price_only_shadow_backtest"
            else (
                "- Shadow backtest input gate failed or is limited; repair cached inputs "
                "before interpreting candidate performance."
            )
        )
    )
    lines.extend(
        [
            "",
            "## 9. Impact on Shadow Backtest",
            "",
            impact_line,
            "- Candidate promotion remains disabled unless this diagnostic status is `OK`.",
            "",
            "## 10. Manual Review Checklist",
            "",
            "- 确认缺失资产是否应补数据，还是应由 owner 调整 shadow asset universe。",
            "- 确认缓存 freshness 是否来自真实下载失败、休市日边界或供应商延迟。",
            "- 确认缺失 signal snapshot 是否可接受 price-only shadow fallback。",
            "- repair 前不要修改 production 参数或 promotion criteria。",
        ]
    )
    return "\n".join(lines).rstrip() + "\n"


def _asset_coverage(
    prices: pd.DataFrame, required_assets: tuple[str, ...], as_of: date
) -> dict[str, Any]:
    if prices.empty or "ticker" not in prices or "date" not in prices:
        available_assets: list[str] = []
    else:
        frame = prices.copy()
        frame["_date"] = pd.to_datetime(frame["date"], errors="coerce")
        frame = frame.loc[frame["_date"].notna() & (frame["_date"].dt.date <= as_of)]
        available = set(frame["ticker"].astype(str).unique())
        available_assets = [asset for asset in required_assets if asset in available]
    missing_assets = [asset for asset in required_assets if asset not in available_assets]
    return {
        "status": "FAILED" if missing_assets else "OK",
        "required_assets": list(required_assets),
        "available_assets": available_assets,
        "missing_assets": missing_assets,
    }


def _date_coverage(
    prices: pd.DataFrame,
    required_assets: tuple[str, ...],
    *,
    required_start: date,
    required_end: date,
) -> dict[str, Any]:
    available_dates = _available_common_dates(prices, required_assets, required_end)
    available_start = available_dates[0] if available_dates else None
    available_end = available_dates[-1] if available_dates else None
    missing_start_gap = (
        max(0, (available_start - required_start).days) if available_start is not None else None
    )
    missing_end_gap = (
        max(0, (required_end - available_end).days) if available_end is not None else None
    )
    if available_start is None or available_end is None:
        status = "FAILED"
    elif available_start > required_start or available_end < required_end:
        status = "INSUFFICIENT_DATA"
    else:
        status = "OK"
    return {
        "status": status,
        "required_start_date": required_start.isoformat(),
        "required_end_date": required_end.isoformat(),
        "available_start_date": None if available_start is None else available_start.isoformat(),
        "available_end_date": None if available_end is None else available_end.isoformat(),
        "missing_start_gap_days": missing_start_gap,
        "missing_end_gap_days": missing_end_gap,
    }


def _price_data_completeness(
    prices: pd.DataFrame,
    required_assets: tuple[str, ...],
    *,
    required_start: date,
    required_end: date,
    max_allowed_missing_ratio: float,
) -> dict[str, Any]:
    expected_dates = _expected_price_dates(
        prices,
        required_assets,
        required_start,
        required_end,
    )
    assets: list[dict[str, Any]] = []
    any_gap_failure = False
    any_missing_asset = False
    for asset in required_assets:
        valid_dates = _valid_asset_dates(prices, asset, required_start, required_end)
        missing_dates = [item for item in expected_dates if item not in valid_dates]
        missing_ratio = len(missing_dates) / len(expected_dates) if expected_dates else 1.0
        has_any_data = bool(valid_dates)
        status = "OK"
        if not has_any_data:
            status = "FAILED"
            any_missing_asset = True
        elif missing_ratio > max_allowed_missing_ratio:
            status = "FAILED"
            any_gap_failure = True
        assets.append(
            {
                "symbol": asset,
                "status": status,
                "missing_ratio": round(missing_ratio, 6),
                "missing_dates_sample": [item.isoformat() for item in missing_dates[:5]],
            }
        )
    if any_missing_asset:
        status = "FAILED"
    elif any_gap_failure:
        status = "LIMITED"
    else:
        status = "OK"
    return {
        "status": status,
        "max_allowed_missing_ratio": max_allowed_missing_ratio,
        "expected_observation_count": len(expected_dates),
        "assets": assets,
    }


def _signal_snapshot_availability(
    raw_config: dict[str, Any],
    prices: pd.DataFrame,
    *,
    required_signals: tuple[str, ...],
    as_of: date,
) -> dict[str, Any]:
    discovered_files = _signal_snapshot_files(raw_config, required_signals, as_of)
    available = set(_signals_from_files(discovered_files, required_signals))
    if not discovered_files and not prices.empty:
        available.update(PRICE_PROXY_SIGNALS)
    available_signals = [signal for signal in required_signals if signal in available]
    missing_signals = [signal for signal in required_signals if signal not in available]
    return {
        "status": "OK" if not missing_signals else "LIMITED",
        "required_signals": list(required_signals),
        "available_signals": available_signals,
        "missing_signals": missing_signals,
        "fallback_mode": "none" if not missing_signals else "price_only_shadow_backtest",
        "snapshot_files": [str(path) for path in discovered_files],
    }


def _cache_freshness(
    config: ShadowBacktestConfig,
    raw_config: dict[str, Any],
    *,
    generated_at: datetime,
    signal_snapshot_files: list[Path],
) -> dict[str, Any]:
    max_age_days = _cache_freshness_max_age_days(config, raw_config)
    manifest_path = resolve_project_path(config.data.download_manifest_path)
    price_path = resolve_project_path(config.data.prices_path)
    rates_path = resolve_project_path(config.data.rates_path)
    items = [
        _freshness_item(
            "price_data",
            _last_updated_for_path(price_path, manifest_path),
            max_age_days["price_data"],
            generated_at,
        ),
        _freshness_item(
            "signal_snapshot",
            _latest_mtime(signal_snapshot_files),
            max_age_days["signal_snapshot"],
            generated_at,
        ),
        _freshness_item(
            "macro_data",
            _last_updated_for_path(rates_path, manifest_path),
            max_age_days["macro_data"],
            generated_at,
        ),
    ]
    status = "OK"
    if any(item["status"] in {"STALE", "MISSING"} for item in items):
        status = "STALE"
    return {"status": status, "items": items}


def _repair_plan(
    checks: dict[str, Any],
    *,
    required_start: date,
    required_end: date,
) -> dict[str, Any]:
    steps: list[dict[str, Any]] = []
    asset_coverage = _mapping(checks.get("asset_coverage"))
    missing_assets = _strings(asset_coverage.get("missing_assets"))
    if missing_assets:
        steps.append(
            {
                "step": len(steps) + 1,
                "action": "download_missing_price_history",
                "assets": missing_assets,
                "date_range": {
                    "start": required_start.isoformat(),
                    "end": required_end.isoformat(),
                },
                "required": True,
            }
        )
    date_coverage = _mapping(checks.get("date_coverage"))
    if date_coverage.get("status") == "INSUFFICIENT_DATA":
        steps.append(
            {
                "step": len(steps) + 1,
                "action": "extend_price_history",
                "assets": _strings(asset_coverage.get("required_assets")),
                "date_range": {
                    "start": required_start.isoformat(),
                    "end": _text(
                        date_coverage.get("available_start_date"), required_end.isoformat()
                    ),
                },
                "required": True,
            }
        )
    price_assets = [
        _text(asset.get("symbol"))
        for asset in _records(_mapping(checks.get("price_data")).get("assets"))
        if asset.get("status") == "FAILED" and _text(asset.get("symbol")) not in missing_assets
    ]
    if price_assets:
        steps.append(
            {
                "step": len(steps) + 1,
                "action": "repair_price_history_gaps",
                "assets": price_assets,
                "date_range": {
                    "start": required_start.isoformat(),
                    "end": required_end.isoformat(),
                },
                "required": True,
            }
        )
    missing_signals = _strings(_mapping(checks.get("signal_snapshots")).get("missing_signals"))
    if missing_signals:
        steps.append(
            {
                "step": len(steps) + 1,
                "action": "generate_missing_signal_snapshots",
                "signals": missing_signals,
                "required": False,
                "blocks_promotion": True,
            }
        )
    stale_items = [
        _text(item.get("name"))
        for item in _records(_mapping(checks.get("cache_freshness")).get("items"))
        if item.get("status") in {"STALE", "MISSING"}
    ]
    if stale_items:
        steps.append(
            {
                "step": len(steps) + 1,
                "action": "refresh_stale_local_cache",
                "items": stale_items,
                "required": any(item in {"price_data", "macro_data"} for item in stale_items),
            }
        )
    return {"status": "AVAILABLE" if steps else "NOT_REQUIRED", "steps": steps}


def _summary(checks: dict[str, Any]) -> dict[str, Any]:
    blocking_errors = 0
    warnings = 0
    asset_status = _text(_mapping(checks.get("asset_coverage")).get("status"))
    if asset_status == "FAILED":
        blocking_errors += 1
    date_status = _text(_mapping(checks.get("date_coverage")).get("status"))
    if date_status in {"FAILED", "INSUFFICIENT_DATA"}:
        blocking_errors += 1
    price_data = _mapping(checks.get("price_data"))
    price_status = _text(price_data.get("status"))
    failed_price_assets = [
        asset for asset in _records(price_data.get("assets")) if asset.get("status") == "FAILED"
    ]
    if failed_price_assets and asset_status != "FAILED":
        blocking_errors += 1
    elif price_status == "LIMITED":
        warnings += 1
    signal_status = _text(_mapping(checks.get("signal_snapshots")).get("status"))
    if signal_status == "LIMITED":
        warnings += 1
    freshness_items = _records(_mapping(checks.get("cache_freshness")).get("items"))
    stale_required = [
        item
        for item in freshness_items
        if item.get("name") in {"price_data", "macro_data"}
        and item.get("status") in {"STALE", "MISSING"}
    ]
    if stale_required:
        blocking_errors += 1
    elif _text(_mapping(checks.get("cache_freshness")).get("status")) == "STALE":
        warnings += 1
    overall = "FAILED" if blocking_errors else "LIMITED" if warnings else "OK"
    can_run_shadow_backtest = blocking_errors == 0
    can_promote_candidate = overall == "OK"
    return {
        "overall_status": overall,
        "asset_coverage_status": asset_status,
        "date_coverage_status": date_status,
        "price_data_status": price_status,
        "signal_snapshots_status": signal_status,
        "backtest_mode": _backtest_mode(
            can_run_shadow_backtest=can_run_shadow_backtest,
            signal_snapshots_status=signal_status,
        ),
        "blocking_errors": blocking_errors,
        "warnings": warnings,
        "can_run_shadow_backtest": can_run_shadow_backtest,
        "can_promote_candidate": can_promote_candidate,
    }


def _backtest_mode(
    *,
    can_run_shadow_backtest: bool,
    signal_snapshots_status: str,
) -> str:
    if not can_run_shadow_backtest:
        return "blocked"
    if signal_snapshots_status == "LIMITED":
        return "price_only_shadow_backtest"
    return "full_signal_backtest"


def _required_assets(config: ShadowBacktestConfig) -> tuple[str, ...]:
    try:
        baseline = load_production_parameters(resolve_project_path(config.baseline_parameters_path))
    except (OSError, ValueError):
        return DEFAULT_REQUIRED_BACKTEST_ASSETS
    assets = tuple(asset for asset in baseline.flattened_asset_universe() if asset != "CASH")
    return assets or DEFAULT_REQUIRED_BACKTEST_ASSETS


def _required_signals(config: ShadowBacktestConfig) -> tuple[str, ...]:
    weights = tuple(config.search.search_space.keys()) or tuple(config.point_in_time_status.keys())
    ordered = [signal for signal in REQUIRED_SIGNAL_SNAPSHOTS if signal in weights]
    extra = [signal for signal in weights if signal not in ordered]
    return tuple(ordered + extra) or REQUIRED_SIGNAL_SNAPSHOTS


def _required_start_date(
    prices: pd.DataFrame,
    required_assets: tuple[str, ...],
    as_of: date,
    min_history_days: int,
) -> date:
    dates = _observed_required_asset_dates(prices, required_assets, None, as_of)
    if len(dates) >= min_history_days:
        return dates[-min_history_days]
    if dates:
        return dates[-1] - timedelta(days=min_history_days - 1)
    return as_of - timedelta(days=min_history_days)


def _available_common_dates(
    prices: pd.DataFrame,
    required_assets: tuple[str, ...],
    end: date,
) -> list[date]:
    if prices.empty or not {"date", "ticker", "adj_close"}.issubset(prices.columns):
        return []
    available_assets = [
        asset for asset in required_assets if not _asset_rows(prices, asset, None, end).empty
    ]
    if not available_assets:
        return []
    counts: dict[date, int] = {}
    for asset in available_assets:
        for item in _valid_asset_dates(prices, asset, None, end):
            counts[item] = counts.get(item, 0) + 1
    required_count = len(available_assets)
    return sorted(day for day, count in counts.items() if count >= required_count)


def _expected_price_dates(
    prices: pd.DataFrame,
    required_assets: tuple[str, ...],
    start: date,
    end: date,
) -> list[date]:
    dates = _observed_required_asset_dates(prices, required_assets, start, end)
    if dates:
        return dates
    dates = [item for item in _all_price_dates(prices, end=end) if item >= start]
    if dates:
        return dates
    return [start + timedelta(days=offset) for offset in range((end - start).days + 1)]


def _observed_required_asset_dates(
    prices: pd.DataFrame,
    required_assets: tuple[str, ...],
    start: date | None,
    end: date,
) -> list[date]:
    dates: set[date] = set()
    for asset in required_assets:
        dates.update(_valid_asset_dates(prices, asset, start, end))
    return sorted(dates)


def _all_price_dates(prices: pd.DataFrame, *, end: date) -> list[date]:
    if prices.empty or "date" not in prices:
        return []
    parsed = pd.to_datetime(prices["date"], errors="coerce").dropna()
    dates = sorted(
        {pd.Timestamp(item).date() for item in parsed if pd.Timestamp(item).date() <= end}
    )
    return dates


def _valid_asset_dates(
    prices: pd.DataFrame,
    asset: str,
    start: date | None,
    end: date,
) -> set[date]:
    frame = _asset_rows(prices, asset, start, end)
    if frame.empty:
        return set()
    frame["_adj_close"] = pd.to_numeric(frame["adj_close"], errors="coerce")
    frame = frame.loc[frame["_adj_close"].notna()]
    return {pd.Timestamp(item).date() for item in frame["_date"]}


def _asset_rows(prices: pd.DataFrame, asset: str, start: date | None, end: date) -> pd.DataFrame:
    if prices.empty or not {"date", "ticker", "adj_close"}.issubset(prices.columns):
        return pd.DataFrame()
    frame = prices.loc[prices["ticker"].astype(str) == asset].copy()
    if frame.empty:
        return frame
    frame["_date"] = pd.to_datetime(frame["date"], errors="coerce")
    frame = frame.loc[frame["_date"].notna()]
    if start is not None:
        frame = frame.loc[frame["_date"].dt.date >= start]
    return frame.loc[frame["_date"].dt.date <= end].copy()


def _signal_snapshot_files(
    raw_config: dict[str, Any],
    required_signals: tuple[str, ...],
    as_of: date,
) -> list[Path]:
    files: list[Path] = []
    explicit_files = _mapping(_mapping(raw_config.get("data")).get("signal_snapshot_files"))
    for value in explicit_files.values():
        path = resolve_project_path(str(value))
        if path.exists():
            files.append(path)
    for directory in _signal_snapshot_dirs(raw_config, as_of):
        if not directory.exists():
            continue
        for signal in required_signals:
            patterns = (f"{signal}.json", f"{signal}.csv", f"{signal}_*.json", f"{signal}_*.csv")
            for pattern in patterns:
                files.extend(path for path in directory.glob(pattern) if path.is_file())
    return sorted(dict.fromkeys(files))


def _signal_snapshot_dirs(raw_config: dict[str, Any], as_of: date) -> list[Path]:
    data_config = _mapping(raw_config.get("data"))
    raw_dirs = [
        data_config.get("signal_snapshot_dir"),
        raw_config.get("signal_snapshot_dir"),
        _mapping(raw_config.get("signal_snapshots")).get("dir"),
    ]
    dirs = [resolve_project_path(str(path)) for path in raw_dirs if str(path or "").strip()]
    dated_dirs: list[Path] = []
    for directory in dirs:
        dated_dirs.append(directory)
        dated_dirs.append(directory / as_of.isoformat())
    default_root = PROJECT_ROOT / "artifacts" / "signal_snapshots"
    dated_dirs.extend([default_root, default_root / as_of.isoformat()])
    return dated_dirs


def _signals_from_files(files: list[Path], required_signals: tuple[str, ...]) -> list[str]:
    available: set[str] = set()
    for path in files:
        stem = path.stem
        for signal in required_signals:
            if stem == signal or stem.startswith(f"{signal}_"):
                available.add(signal)
        if path.suffix.lower() == ".json":
            payload = load_backtest_input_diagnostics(path)
            if isinstance(payload.get("signals"), dict):
                available.update(
                    signal for signal in required_signals if signal in payload["signals"]
                )
    return sorted(available)


def _cache_freshness_max_age_days(
    config: ShadowBacktestConfig,
    raw_config: dict[str, Any],
) -> dict[str, int]:
    configured = dict(config.data_quality_rules.cache_freshness.max_age_days)
    raw_rule = _mapping(_mapping(raw_config.get("cache_freshness")).get("max_age_days"))
    raw_quality_rule = _mapping(
        _mapping(_mapping(raw_config.get("data_quality_rules")).get("cache_freshness")).get(
            "max_age_days"
        )
    )
    for source in (raw_rule, raw_quality_rule):
        for key, value in source.items():
            try:
                configured[str(key)] = int(value)
            except (TypeError, ValueError):
                continue
    merged = dict(DEFAULT_BACKTEST_INPUT_CACHE_FRESHNESS_MAX_AGE_DAYS)
    merged.update(configured)
    return merged


def _last_updated_for_path(path: Path, manifest_path: Path) -> datetime | None:
    manifest_time = _manifest_last_updated(path, manifest_path)
    if manifest_time is not None:
        return manifest_time
    if not path.exists():
        return None
    return datetime.fromtimestamp(path.stat().st_mtime, tz=UTC)


def _manifest_last_updated(path: Path, manifest_path: Path) -> datetime | None:
    if not manifest_path.exists():
        return None
    try:
        manifest = pd.read_csv(manifest_path)
    except Exception:
        return None
    if "downloaded_at" not in manifest:
        return None
    candidates = manifest
    if "output_path" in candidates:
        target = str(path)
        candidates = candidates.loc[candidates["output_path"].astype(str) == target]
    if candidates.empty:
        return None
    parsed = pd.to_datetime(candidates["downloaded_at"], errors="coerce", utc=True).dropna()
    if parsed.empty:
        return None
    return pd.Timestamp(parsed.max()).to_pydatetime()


def _latest_mtime(paths: list[Path]) -> datetime | None:
    existing = [path for path in paths if path.exists()]
    if not existing:
        return None
    return max(datetime.fromtimestamp(path.stat().st_mtime, tz=UTC) for path in existing)


def _freshness_item(
    name: str,
    last_updated: datetime | None,
    max_age_days: int,
    generated_at: datetime,
) -> dict[str, Any]:
    if last_updated is None:
        return {
            "name": name,
            "last_updated": None,
            "max_age_days": max_age_days,
            "actual_age_days": None,
            "status": "MISSING",
        }
    if generated_at.tzinfo is None:
        generated = generated_at.replace(tzinfo=UTC)
    else:
        generated = generated_at
    updated = last_updated if last_updated.tzinfo is not None else last_updated.replace(tzinfo=UTC)
    actual_age_days = max(0, (generated.date() - updated.date()).days)
    return {
        "name": name,
        "last_updated": updated.isoformat(),
        "max_age_days": max_age_days,
        "actual_age_days": actual_age_days,
        "status": "STALE" if actual_age_days > max_age_days else "OK",
    }


def _blocking_error_lines(checks: dict[str, Any]) -> list[str]:
    lines: list[str] = []
    missing_assets = _strings(_mapping(checks.get("asset_coverage")).get("missing_assets"))
    if missing_assets:
        lines.append("- Missing required price history for " + ", ".join(missing_assets) + ".")
    date_coverage = _mapping(checks.get("date_coverage"))
    if date_coverage.get("status") in {"FAILED", "INSUFFICIENT_DATA"}:
        required_start = date_coverage.get("required_start_date")
        required_end = date_coverage.get("required_end_date")
        available_start = date_coverage.get("available_start_date")
        available_end = date_coverage.get("available_end_date")
        lines.append(
            "- Insufficient date coverage: required "
            f"{required_start} to {required_end}, available {available_start} to "
            f"{available_end}."
        )
    price_failures = [
        _text(asset.get("symbol"))
        for asset in _records(_mapping(checks.get("price_data")).get("assets"))
        if asset.get("status") == "FAILED" and _text(asset.get("symbol")) not in missing_assets
    ]
    if price_failures:
        lines.append("- Price missing ratio is too high for " + ", ".join(price_failures) + ".")
    stale_required = [
        _text(item.get("name"))
        for item in _records(_mapping(checks.get("cache_freshness")).get("items"))
        if item.get("name") in {"price_data", "macro_data"}
        and item.get("status") in {"STALE", "MISSING"}
    ]
    if stale_required:
        lines.append(
            "- Required local cache is stale or missing: " + ", ".join(stale_required) + "."
        )
    return lines


def _definition_lines(payload: dict[str, Any], keys: tuple[str, ...]) -> str:
    return "\n".join(f"- {key}：`{payload.get(key, 'UNKNOWN')}`" for key in keys)


def _read_prices(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except Exception:
        return pd.DataFrame()


def _latest_price_date(prices: pd.DataFrame) -> date | None:
    if prices.empty or "date" not in prices:
        return None
    parsed = pd.to_datetime(prices["date"], errors="coerce").dropna()
    if parsed.empty:
        return None
    return pd.Timestamp(parsed.max()).date()


def _price_symbol_mapping(prices_path: Path, required_assets: list[str]) -> dict[str, Any]:
    prices = _read_prices(prices_path)
    if prices.empty or "source_symbol" not in prices.columns:
        return {}
    canonical_column = "canonical_symbol" if "canonical_symbol" in prices.columns else "ticker"
    if canonical_column not in prices.columns:
        return {}
    mapping: dict[str, Any] = {}
    for asset in required_assets:
        rows = prices.loc[prices[canonical_column].astype(str) == asset]
        if rows.empty and "ticker" in prices.columns:
            rows = prices.loc[prices["ticker"].astype(str) == asset]
        if rows.empty:
            continue
        source_symbols = [
            str(value)
            for value in rows["source_symbol"].dropna().unique()
            if str(value) and str(value) != asset
        ]
        if source_symbols:
            mapping[asset] = {
                "source_symbol": source_symbols[0],
                "canonical_symbol": asset,
            }
    return mapping


def _load_raw_config(path: Path) -> dict[str, Any]:
    try:
        payload = safe_load_yaml_path(path)
    except (OSError, ValueError):
        return {}
    return payload if isinstance(payload, dict) else {}


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


def _join_text(value: object) -> str:
    return ", ".join(_strings(value)) or "none"


def _text(value: object, default: str = "") -> str:
    if value is None:
        return default
    text = str(value)
    return text if text else default


def _format_ratio(value: object) -> str:
    try:
        return f"{float(value):.4f}"
    except (TypeError, ValueError):
        return "NA"
