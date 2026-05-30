from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, date, datetime
from hashlib import sha256
from pathlib import Path
from typing import Any

import pandas as pd

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.shadow.lineage import git_commit_sha, git_worktree_dirty
from ai_trading_system.trading_engine.backtest_input_diagnostics import (
    BacktestInputDiagnosticsRun,
    run_backtest_input_diagnostics,
)
from ai_trading_system.trading_engine.data.price_history_repair import (
    PRICE_CACHE_OUTPUT_COLUMNS,
    source_symbol_for_price_repair,
    upsert_price_history_cache,
)
from ai_trading_system.trading_engine.data_registry_consistency import (
    build_data_registry_consistency_payload,
    run_data_registry_consistency,
)
from ai_trading_system.trading_engine.parameters.parameter_loader import (
    DEFAULT_SHADOW_BACKTEST_CONFIG_PATH,
    load_shadow_backtest_config,
    resolve_project_path,
)

PRICE_CACHE_RECONCILE_SCHEMA_VERSION = 1
PRICE_CACHE_RECONCILE_REPORT_TYPE = "price_cache_reconcile"
PRICE_CACHE_REGISTRY_SCHEMA_VERSION = 1
PRICE_CACHE_REGISTRY_REPORT_TYPE = "price_cache_registry"
FMP_PROVIDER = "Financial Modeling Prep"
FMP_EOD_CACHE_FAMILY = "eod_daily_prices"


@dataclass(frozen=True)
class PriceCacheReconcileRun:
    as_of: date
    payload: dict[str, Any]
    json_path: Path | None
    markdown_path: Path | None
    registry_path: Path
    refreshed_manifest_path: Path | None


@dataclass(frozen=True)
class BacktestManifestRefreshRun:
    as_of: date
    payload: dict[str, Any]
    diagnostic_run: BacktestInputDiagnosticsRun | None


def default_price_cache_reconcile_dir(output_root: Path, as_of: date) -> Path:
    return output_root / "data_quality" / as_of.isoformat()


def default_price_cache_reconcile_json_path(output_root: Path, as_of: date) -> Path:
    return default_price_cache_reconcile_dir(
        output_root,
        as_of,
    ) / "price_cache_reconcile_summary.json"


def default_price_cache_reconcile_markdown_path(output_root: Path, as_of: date) -> Path:
    return default_price_cache_reconcile_dir(
        output_root,
        as_of,
    ) / "price_cache_reconcile_summary.md"


def default_price_cache_registry_path(output_root: Path | None = None) -> Path:
    root = output_root or (PROJECT_ROOT / "artifacts")
    return root / "data_registry" / "price_cache_registry.json"


def run_price_cache_reconcile(
    *,
    as_of: date | None = None,
    config_path: Path | str = DEFAULT_SHADOW_BACKTEST_CONFIG_PATH,
    output_root: Path | None = None,
    dry_run: bool = False,
    refresh_manifest_only: bool = False,
    register_repaired_only: bool = False,
    symbols: tuple[str, ...] = (),
    generated_at: datetime | None = None,
) -> PriceCacheReconcileRun:
    root = output_root or (PROJECT_ROOT / "artifacts")
    generated = generated_at or datetime.now(tz=UTC)
    resolved_config_path = resolve_project_path(config_path)
    config = load_shadow_backtest_config(resolved_config_path)
    prices_path = resolve_project_path(config.data.prices_path)
    before = build_data_registry_consistency_payload(
        as_of=as_of,
        config_path=resolved_config_path,
        output_root=root,
        generated_at=generated,
    )
    target_date = _target_manifest_date(before, as_of=as_of, generated_at=generated)
    latest_context = _latest_context(before, target_manifest_date=target_date)
    required_assets = tuple(_strings(before.get("required_assets")))
    selected_symbols = _selected_symbols(
        requested=symbols,
        required_assets=required_assets,
        before_payload=before,
    )
    required_start = _required_start_date(before, fallback=target_date)
    min_history_days = int(config.walk_forward.min_history_days)
    inspections = [
        _inspect_repaired_artifact(
            canonical_symbol=symbol,
            required_start=required_start,
            target_date=target_date,
            latest_valid_manifest_date=_parse_date(
                _mapping(before.get("latest_resolution")).get(
                    "resolved_backtest_manifest_date",
                )
            ),
            min_history_days=min_history_days,
        )
        for symbol in selected_symbols
    ]
    planned_actions = _planned_actions(
        inspections=inspections,
        refresh_manifest_only=refresh_manifest_only,
        register_repaired_only=register_repaired_only,
    )
    registered_frames: list[pd.DataFrame] = []
    registration: list[dict[str, Any]] = []
    refreshed: BacktestInputDiagnosticsRun | None = None
    fallback_refreshed: BacktestInputDiagnosticsRun | None = None
    registry_path = default_price_cache_registry_path(root)

    if not dry_run:
        if not refresh_manifest_only:
            for inspection in inspections:
                frame = inspection.pop("_frame", pd.DataFrame())
                if inspection["status"] not in {"OK", "STALE_FOR_TARGET_DATE"}:
                    registration.append(
                        {
                            "canonical_symbol": inspection["canonical_symbol"],
                            "source_symbol": inspection["source_symbol"],
                            "action": "not_registered",
                            "status": "FAILED",
                            "error_code": inspection["error_code"],
                        }
                    )
                    continue
                if frame.empty:
                    registration.append(
                        {
                            "canonical_symbol": inspection["canonical_symbol"],
                            "source_symbol": inspection["source_symbol"],
                            "action": "not_registered",
                            "status": "FAILED",
                            "error_code": "REPAIRED_ARTIFACT_EMPTY",
                        }
                    )
                    continue
                registered_frames.append(frame)
                registration.append(
                    {
                        "canonical_symbol": inspection["canonical_symbol"],
                        "source_symbol": inspection["source_symbol"],
                        "action": "registered_repaired_artifact",
                        "status": "OK",
                        "rows": len(frame),
                        "date_range": inspection["date_range"],
                    }
                )
            if registered_frames:
                repaired = pd.concat(registered_frames, ignore_index=True)
                merged = upsert_price_history_cache(prices_path, repaired)
                _write_price_cache(prices_path, merged)
                _write_price_cache_registry(
                    path=registry_path,
                    generated_at=generated,
                    prices_path=prices_path,
                    inspections=inspections,
                    registration=registration,
                )
        if not register_repaired_only:
            refreshed = run_backtest_input_diagnostics(
                as_of=target_date,
                config_path=resolved_config_path,
                output_root=root,
                generated_at=generated,
            )
            if _text(refreshed.manifest.get("status")).upper() == "FAILED":
                fallback_date = _latest_common_required_asset_date(
                    _read_prices(prices_path),
                    required_assets,
                    target_date,
                )
                if fallback_date is not None and fallback_date < target_date:
                    fallback_refreshed = run_backtest_input_diagnostics(
                        as_of=fallback_date,
                        config_path=resolved_config_path,
                        output_root=root,
                        generated_at=generated,
                    )
    else:
        for inspection in inspections:
            inspection.pop("_frame", None)

    after = (
        before
        if dry_run
        else run_data_registry_consistency(
            as_of=None,
            config_path=resolved_config_path,
            output_root=root,
            generated_at=generated,
        ).payload
    )
    payload = _reconcile_payload(
        generated_at=generated,
        config_path=resolved_config_path,
        prices_path=prices_path,
        before=before,
        after=after,
        latest_context=latest_context,
        required_assets=required_assets,
        selected_symbols=selected_symbols,
        inspections=inspections,
        planned_actions=planned_actions,
        registration=registration,
        refreshed=refreshed,
        fallback_refreshed=fallback_refreshed,
        dry_run=dry_run,
        refresh_manifest_only=refresh_manifest_only,
        register_repaired_only=register_repaired_only,
    )
    resolved_as_of = _parse_date(_mapping(payload.get("metadata")).get("as_of")) or target_date
    json_path = None
    markdown_path = None
    if not dry_run:
        json_path = default_price_cache_reconcile_json_path(root, resolved_as_of)
        markdown_path = default_price_cache_reconcile_markdown_path(root, resolved_as_of)
        write_price_cache_reconcile_report(payload, json_path, markdown_path)
    refreshed_manifest_path = (
        fallback_refreshed.manifest_path
        if fallback_refreshed is not None
        else refreshed.manifest_path
        if refreshed is not None
        else None
    )
    return PriceCacheReconcileRun(
        as_of=resolved_as_of,
        payload=payload,
        json_path=json_path,
        markdown_path=markdown_path,
        registry_path=registry_path,
        refreshed_manifest_path=refreshed_manifest_path,
    )


def refresh_backtest_manifest(
    *,
    as_of: date | None = None,
    config_path: Path | str = DEFAULT_SHADOW_BACKTEST_CONFIG_PATH,
    output_root: Path | None = None,
    dry_run: bool = False,
    generated_at: datetime | None = None,
) -> BacktestManifestRefreshRun:
    root = output_root or (PROJECT_ROOT / "artifacts")
    generated = generated_at or datetime.now(tz=UTC)
    resolved_config_path = resolve_project_path(config_path)
    before = build_data_registry_consistency_payload(
        as_of=as_of,
        config_path=resolved_config_path,
        output_root=root,
        generated_at=generated,
    )
    target_date = _target_manifest_date(before, as_of=as_of, generated_at=generated)
    if dry_run:
        payload = {
            "schema_version": PRICE_CACHE_RECONCILE_SCHEMA_VERSION,
            "report_type": "backtest_manifest_refresh",
            "metadata": _metadata(
                run_id=f"backtest-manifest-refresh-{target_date.isoformat()}",
                generated_at=generated,
                status="DRY_RUN",
                as_of=target_date,
                config_path=resolved_config_path,
            ),
            "target_manifest_date": target_date.isoformat(),
            "would_write_manifest": str(
                root
                / "backtest_snapshots"
                / target_date.isoformat()
                / "backtest_input_manifest.json"
            ),
            "production_effect": "none",
            "manual_review_required": True,
            "auto_promotion": False,
        }
        return BacktestManifestRefreshRun(as_of=target_date, payload=payload, diagnostic_run=None)
    diagnostic = run_backtest_input_diagnostics(
        as_of=target_date,
        config_path=resolved_config_path,
        output_root=root,
        generated_at=generated,
    )
    manifest_status = _text(diagnostic.manifest.get("status"), "UNKNOWN")
    status = "FAILED" if manifest_status.upper() == "FAILED" else "OK"
    payload = {
        "schema_version": PRICE_CACHE_RECONCILE_SCHEMA_VERSION,
        "report_type": "backtest_manifest_refresh",
        "metadata": _metadata(
            run_id=f"backtest-manifest-refresh-{target_date.isoformat()}",
            generated_at=generated,
            status=status,
            as_of=target_date,
            config_path=resolved_config_path,
        ),
        "target_manifest_date": target_date.isoformat(),
        "manifest_status": manifest_status,
        "diagnostic_report": str(diagnostic.json_path),
        "manifest": str(diagnostic.manifest_path),
        "production_effect": "none",
        "manual_review_required": True,
        "auto_promotion": False,
    }
    return BacktestManifestRefreshRun(as_of=target_date, payload=payload, diagnostic_run=diagnostic)


def write_price_cache_reconcile_report(
    payload: dict[str, Any],
    json_path: Path,
    markdown_path: Path,
) -> tuple[Path, Path]:
    json_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    markdown_path.write_text(render_price_cache_reconcile_markdown(payload), encoding="utf-8")
    return json_path, markdown_path


def render_price_cache_reconcile_markdown(payload: dict[str, Any]) -> str:
    metadata = _mapping(payload.get("metadata"))
    before = _mapping(payload.get("before"))
    after = _mapping(payload.get("after"))
    actions = _mapping(payload.get("actions"))
    inspections = _records(payload.get("repaired_artifact_inspection"))
    lines = [
        "# Price Cache Reconcile Summary",
        "",
        "## 1. Executive Summary",
        "",
        f"- 状态：`{metadata.get('status', 'UNKNOWN')}`",
        f"- production_effect：`{metadata.get('production_effect', 'none')}`",
        f"- manual_review_required：`{metadata.get('manual_review_required', True)}`",
        f"- auto_promotion：`{metadata.get('auto_promotion', False)}`",
        "",
        "## 2. Before State",
        "",
        _definition_lines(
            before,
            (
                "market_data_date",
                "manifest_date",
                "latest_resolution",
                "data_registry_status",
            ),
        ),
        "",
        "## 3. Actions Taken",
        "",
        _definition_lines(
            actions,
            (
                "mode",
                "registered_repaired_artifacts",
                "refreshed_manifest",
                "target_manifest_date",
                "fallback_manifest_date",
            ),
        ),
        "",
        "## 4. Symbol Mapping Applied",
        "",
        _definition_lines(_mapping(actions.get("symbol_mappings_applied")), ("BRK.B",)),
        "",
        "## 5. Manifest Refresh",
        "",
        _definition_lines(_mapping(payload.get("manifest_refresh")), ("status", "manifest")),
        "",
        "## 6. After State",
        "",
        _definition_lines(
            after,
            (
                "market_data_date",
                "manifest_date",
                "latest_resolution",
                "data_registry_status",
            ),
        ),
        "",
        "## 7. Remaining Limitations",
        "",
        *[f"- {item}" for item in _strings(payload.get("remaining_limitations"))],
        "",
        "## 8. Impact on Portfolio Sensitivity",
        "",
        f"- {_mapping(payload.get('impact_on_portfolio_sensitivity')).get('summary', '')}",
        "",
        "## 9. Impact on Shadow Backtest",
        "",
        f"- {_mapping(payload.get('impact_on_shadow_backtest')).get('summary', '')}",
        "",
        "## 10. Repaired Artifact Inspection",
        "",
        "| Symbol | Source | Status | Rows | Date Range | Error |",
        "|---|---|---|---:|---|---|",
    ]
    for item in inspections:
        date_range = _mapping(item.get("date_range"))
        lines.append(
            "| "
            f"`{item.get('canonical_symbol', '')}` | "
            f"`{item.get('source_symbol', '')}` | "
            f"`{item.get('status', '')}` | "
            f"{item.get('rows', 0)} | "
            f"{date_range.get('start', '')} to {date_range.get('end', '')} | "
            f"{_escape_table(_text(item.get('error_code')))} |"
        )
    lines.extend(
        [
            "",
            "## 11. Manual Review Checklist",
            "",
            "- 确认 registry 只引用真实缓存或已审计 raw response，不补造价格。",
            "- 确认 `BRK.B` 保持 canonical `BRK.B` / source `BRK-B` mapping。",
            "- 确认 portfolio sensitivity 只在 data gate 通过后解释结果。",
            "- 不修改 `config/parameters/production/current.yaml`，不解除 candidate promotion。",
        ]
    )
    return "\n".join(lines).rstrip() + "\n"


def _target_manifest_date(
    payload: dict[str, Any],
    *,
    as_of: date | None,
    generated_at: datetime,
) -> date:
    if as_of is not None:
        return as_of
    latest = _mapping(payload.get("latest_resolution"))
    for key in ("resolved_market_data_date", "resolved_backtest_manifest_date"):
        parsed = _parse_date(latest.get(key))
        if parsed is not None:
            return parsed
    return generated_at.date()


def _latest_context(payload: dict[str, Any], *, target_manifest_date: date) -> dict[str, Any]:
    latest = _mapping(payload.get("latest_resolution"))
    return {
        "market_data_date": _text(latest.get("resolved_market_data_date")),
        "latest_manifest_date": _text(latest.get("resolved_backtest_manifest_date")),
        "target_manifest_date": target_manifest_date.isoformat(),
        "latest_resolution_before": _text(latest.get("status"), "UNKNOWN"),
    }


def _selected_symbols(
    *,
    requested: tuple[str, ...],
    required_assets: tuple[str, ...],
    before_payload: dict[str, Any],
) -> tuple[str, ...]:
    normalized = _normalize_symbols(requested)
    if normalized:
        return normalized
    mismatched = [
        _text(item.get("canonical_symbol"))
        for item in _records(before_payload.get("asset_registry"))
        if _text(item.get("error_code"), "OK") != "OK"
    ]
    return _normalize_symbols(tuple(mismatched))


def _required_start_date(payload: dict[str, Any], *, fallback: date) -> date:
    manifest = _mapping(payload.get("backtest_manifest"))
    date_range = _mapping(manifest.get("date_range"))
    return _parse_date(date_range.get("start")) or fallback


def _inspect_repaired_artifact(
    *,
    canonical_symbol: str,
    required_start: date,
    target_date: date,
    latest_valid_manifest_date: date | None,
    min_history_days: int,
) -> dict[str, Any]:
    source_symbol = source_symbol_for_price_repair(canonical_symbol)
    frame, artifacts = _cached_fmp_repaired_prices(
        canonical_symbol=canonical_symbol,
        source_symbol=source_symbol,
        required_start=required_start,
        target_date=target_date,
    )
    date_range = _frame_date_range(frame)
    status = "OK"
    error_code = "OK"
    mismatch_reasons: list[str] = []
    if frame.empty:
        status = "FAILED"
        error_code = "REPAIRED_ARTIFACT_MISSING"
        mismatch_reasons.append("repaired_artifact_not_registered")
    elif len(frame) < min_history_days:
        status = "FAILED"
        error_code = "REPAIRED_ARTIFACT_INSUFFICIENT_HISTORY"
        mismatch_reasons.append("primary_cache_date_range_mismatch")
    elif date_range["start"] and date.fromisoformat(date_range["start"]) > required_start:
        status = "FAILED"
        error_code = "REPAIRED_ARTIFACT_START_DATE_MISMATCH"
        mismatch_reasons.append("primary_cache_date_range_mismatch")
    elif date_range["end"] and date.fromisoformat(date_range["end"]) < target_date:
        if latest_valid_manifest_date is None or date.fromisoformat(
            date_range["end"],
        ) < latest_valid_manifest_date:
            status = "FAILED"
            error_code = "REPAIRED_ARTIFACT_STALE"
            mismatch_reasons.append("primary_cache_date_range_mismatch")
        else:
            status = "STALE_FOR_TARGET_DATE"
            error_code = "LATEST_DATE_MISMATCH"
            mismatch_reasons.append("manifest_date_behind_market_data")
    result: dict[str, Any] = {
        "canonical_symbol": canonical_symbol,
        "source_symbol": source_symbol,
        "status": status,
        "error_code": error_code,
        "rows": len(frame),
        "date_range": date_range,
        "required_start": required_start.isoformat(),
        "target_date": target_date.isoformat(),
        "mismatch_reasons": mismatch_reasons,
        "source_artifacts": artifacts,
        "_frame": frame,
    }
    return result


def _cached_fmp_repaired_prices(
    *,
    canonical_symbol: str,
    source_symbol: str,
    required_start: date,
    target_date: date,
) -> tuple[pd.DataFrame, list[dict[str, Any]]]:
    root = (
        PROJECT_ROOT
        / "data"
        / "raw"
        / "external_request_cache"
        / FMP_PROVIDER.replace(" ", "_")
        / FMP_EOD_CACHE_FAMILY
    )
    candidates: list[tuple[date, datetime, Path, dict[str, Any], pd.DataFrame]] = []
    artifacts: list[dict[str, Any]] = []
    if not root.exists():
        return pd.DataFrame(columns=PRICE_CACHE_OUTPUT_COLUMNS), artifacts
    for metadata_path in root.rglob("metadata.json"):
        metadata = _read_json(metadata_path)
        params = _mapping(_mapping(metadata.get("request_identity")).get("params"))
        if _text(params.get("symbol")) != source_symbol:
            continue
        body_path = Path(_text(metadata.get("body_path")))
        if not body_path.exists() or int(metadata.get("status_code") or 0) != 200:
            continue
        payload = _read_json(body_path)
        if not isinstance(payload, list):
            continue
        raw = pd.DataFrame(payload)
        frame = _normalize_cached_price_rows(
            raw,
            canonical_symbol=canonical_symbol,
            source_symbol=source_symbol,
            required_start=required_start,
            target_date=target_date,
            updated_at=_text(metadata.get("created_at"), datetime.now(tz=UTC).isoformat()),
        )
        if frame.empty:
            continue
        end_date = pd.Timestamp(pd.to_datetime(frame["date"], errors="coerce").max()).date()
        created = _parse_datetime(metadata.get("created_at")) or datetime.min.replace(tzinfo=UTC)
        candidates.append((end_date, created, metadata_path, metadata, frame))
    if not candidates:
        return pd.DataFrame(columns=PRICE_CACHE_OUTPUT_COLUMNS), artifacts
    candidates.sort(key=lambda item: (item[0], item[1], str(item[2])), reverse=True)
    end_date, _, metadata_path, metadata, frame = candidates[0]
    body_path = Path(_text(metadata.get("body_path")))
    artifacts.append(
        {
            "provider": FMP_PROVIDER,
            "endpoint": _text(metadata.get("endpoint"), _text(metadata.get("url"))),
            "metadata_path": str(metadata_path),
            "body_path": str(body_path),
            "body_sha256": _text(metadata.get("body_sha256"), _sha256_file(body_path)),
            "row_count": len(frame),
            "latest_date": end_date.isoformat(),
        }
    )
    return frame, artifacts


def _normalize_cached_price_rows(
    raw: pd.DataFrame,
    *,
    canonical_symbol: str,
    source_symbol: str,
    required_start: date,
    target_date: date,
    updated_at: str,
) -> pd.DataFrame:
    if raw.empty or "date" not in raw.columns:
        return pd.DataFrame(columns=PRICE_CACHE_OUTPUT_COLUMNS)
    frame = raw.copy()
    rename = {
        "adjOpen": "open",
        "adjHigh": "high",
        "adjLow": "low",
        "adjClose": "adj_close",
    }
    for source, target in rename.items():
        if target not in frame.columns and source in frame.columns:
            frame[target] = frame[source]
    if "close" not in frame.columns and "adj_close" in frame.columns:
        frame["close"] = frame["adj_close"]
    for column in ("open", "high", "low"):
        if column not in frame.columns and "close" in frame.columns:
            frame[column] = frame["close"]
    if "volume" not in frame.columns:
        frame["volume"] = pd.NA
    frame["date"] = pd.to_datetime(frame["date"], errors="coerce")
    frame = frame.loc[
        frame["date"].notna()
        & (frame["date"].dt.date >= required_start)
        & (frame["date"].dt.date <= target_date)
    ].copy()
    for column in ("open", "high", "low", "close", "adj_close", "volume"):
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    frame = frame.loc[frame["close"].notna() & frame["adj_close"].notna()].copy()
    if frame.empty:
        return pd.DataFrame(columns=PRICE_CACHE_OUTPUT_COLUMNS)
    frame["date"] = frame["date"].dt.strftime("%Y-%m-%d")
    frame["ticker"] = canonical_symbol
    frame["symbol"] = canonical_symbol
    frame["source"] = "price_cache_reconcile:fmp_external_request_cache"
    frame["updated_at"] = updated_at
    frame["source_symbol"] = source_symbol
    frame["canonical_symbol"] = canonical_symbol
    return (
        frame[list(PRICE_CACHE_OUTPUT_COLUMNS)]
        .sort_values("date")
        .drop_duplicates(subset=["date", "ticker"], keep="last")
        .reset_index(drop=True)
    )


def _planned_actions(
    *,
    inspections: list[dict[str, Any]],
    refresh_manifest_only: bool,
    register_repaired_only: bool,
) -> list[dict[str, Any]]:
    actions: list[dict[str, Any]] = []
    if not refresh_manifest_only and inspections:
        actions.append(
            {
                "action": "register_repaired_artifacts",
                "symbols": [item["canonical_symbol"] for item in inspections],
                "required": True,
            }
        )
    if not register_repaired_only:
        actions.append(
            {
                "action": "refresh_backtest_input_manifest",
                "symbols": [],
                "required": True,
            }
        )
    return actions


def _write_price_cache(path: Path, frame: pd.DataFrame) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(path, index=False)
    return path


def _write_price_cache_registry(
    *,
    path: Path,
    generated_at: datetime,
    prices_path: Path,
    inspections: list[dict[str, Any]],
    registration: list[dict[str, Any]],
) -> Path:
    assets: dict[str, Any] = {}
    by_symbol = {_text(item.get("canonical_symbol")): item for item in registration}
    for inspection in inspections:
        symbol = _text(inspection.get("canonical_symbol"))
        registered = _mapping(by_symbol.get(symbol))
        if registered.get("status") != "OK":
            continue
        assets[symbol] = {
            "canonical_symbol": symbol,
            "source_symbol": inspection.get("source_symbol"),
            "status": "REGISTERED",
            "rows": inspection.get("rows", 0),
            "date_range": inspection.get("date_range", {}),
            "source_artifacts": inspection.get("source_artifacts", []),
            "registered_at": generated_at.isoformat(),
        }
    status = "OK" if assets else "MISSING"
    payload = {
        "schema_version": PRICE_CACHE_REGISTRY_SCHEMA_VERSION,
        "report_type": PRICE_CACHE_REGISTRY_REPORT_TYPE,
        "metadata": {
            "generated_at": generated_at.isoformat(),
            "status": status,
            "production_effect": "none",
            "manual_review_required": True,
            "auto_promotion": False,
        },
        "price_cache_path": str(prices_path),
        "assets": assets,
        "safety": {
            "production_effect": "none",
            "manual_review_required": True,
            "auto_promotion": False,
            "fake_price_rows_generated": False,
            "data_quality_gate_lowered": False,
        },
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def _reconcile_payload(
    *,
    generated_at: datetime,
    config_path: Path,
    prices_path: Path,
    before: dict[str, Any],
    after: dict[str, Any],
    latest_context: dict[str, Any],
    required_assets: tuple[str, ...],
    selected_symbols: tuple[str, ...],
    inspections: list[dict[str, Any]],
    planned_actions: list[dict[str, Any]],
    registration: list[dict[str, Any]],
    refreshed: BacktestInputDiagnosticsRun | None,
    fallback_refreshed: BacktestInputDiagnosticsRun | None,
    dry_run: bool,
    refresh_manifest_only: bool,
    register_repaired_only: bool,
) -> dict[str, Any]:
    sanitized_inspections = [_without_private_keys(item) for item in inspections]
    status = _reconcile_status(
        before=before,
        after=after,
        inspections=sanitized_inspections,
        dry_run=dry_run,
    )
    as_of = _parse_date(
        _mapping(after.get("latest_resolution")).get("resolved_backtest_manifest_date"),
    )
    if as_of is None:
        as_of = _parse_date(latest_context.get("target_manifest_date")) or generated_at.date()
    registered_symbols = [
        _text(item.get("canonical_symbol"))
        for item in registration
        if item.get("status") == "OK"
    ]
    fallback_date = None if fallback_refreshed is None else fallback_refreshed.as_of.isoformat()
    refresh_status = "SKIPPED" if register_repaired_only else "DRY_RUN" if dry_run else "OK"
    refresh_manifest_path = ""
    if fallback_refreshed is not None:
        refresh_manifest_path = str(fallback_refreshed.manifest_path)
        refresh_status = "FALLBACK_VALID_MANIFEST"
    elif refreshed is not None:
        refresh_manifest_path = str(refreshed.manifest_path)
        if _text(refreshed.manifest.get("status")).upper() == "FAILED":
            refresh_status = "FAILED"
    remaining_limitations = _remaining_limitations(
        status=status,
        inspections=sanitized_inspections,
        refresh_status=refresh_status,
        after=after,
    )
    return {
        "schema_version": PRICE_CACHE_RECONCILE_SCHEMA_VERSION,
        "report_type": PRICE_CACHE_RECONCILE_REPORT_TYPE,
        "metadata": _metadata(
            run_id=f"price-cache-reconcile-{as_of.isoformat()}",
            generated_at=generated_at,
            status=status,
            as_of=as_of,
            config_path=config_path,
        ),
        "latest_context": latest_context,
        "before": _before_after_payload(before),
        "required_assets": list(required_assets),
        "selected_symbols": list(selected_symbols),
        "repaired_artifact_inspection": sanitized_inspections,
        "planned_actions": planned_actions,
        "actions": {
            "mode": "dry_run"
            if dry_run
            else "refresh_manifest_only"
            if refresh_manifest_only
            else "register_repaired_only"
            if register_repaired_only
            else "reconcile",
            "registered_repaired_artifacts": registered_symbols,
            "refreshed_manifest": bool(refreshed is not None or fallback_refreshed is not None),
            "target_manifest_date": latest_context.get("target_manifest_date", ""),
            "fallback_manifest_date": fallback_date or "",
            "symbol_mappings_applied": {
                symbol: source_symbol_for_price_repair(symbol)
                for symbol in selected_symbols
                if source_symbol_for_price_repair(symbol) != symbol
            },
        },
        "registration": registration,
        "manifest_refresh": {
            "status": refresh_status,
            "manifest": refresh_manifest_path,
            "diagnostic_report": ""
            if refreshed is None
            else str(refreshed.json_path),
        },
        "after": _before_after_payload(after),
        "price_cache_path": str(prices_path),
        "remaining_limitations": remaining_limitations,
        "impact_on_portfolio_sensitivity": _portfolio_impact(status, after),
        "impact_on_shadow_backtest": _shadow_backtest_impact(status, after),
        "promotion_impact": {
            "can_support_candidate_promotion": False,
            "reason": (
                "Data registry reconciliation is advisory; candidate promotion remains disabled "
                "until signal quality and promotion gates pass."
            ),
        },
        "safety": {
            "production_effect": "none",
            "manual_review_required": True,
            "auto_promotion": False,
            "production_parameters_modified": False,
            "data_quality_gate_lowered": False,
            "fake_price_rows_generated": False,
        },
    }


def _reconcile_status(
    *,
    before: dict[str, Any],
    after: dict[str, Any],
    inspections: list[dict[str, Any]],
    dry_run: bool,
) -> str:
    if dry_run:
        return "DRY_RUN"
    if any(_text(item.get("status")) == "FAILED" for item in inspections):
        return "FAILED"
    after_status = _text(_mapping(after.get("metadata")).get("status"), "UNKNOWN")
    if after_status == "FAILED":
        return "FAILED"
    if after_status == "LIMITED":
        return "LIMITED"
    before_status = _text(_mapping(before.get("metadata")).get("status"), "UNKNOWN")
    return "OK" if before_status != "OK" else "NOT_REQUIRED"


def _remaining_limitations(
    *,
    status: str,
    inspections: list[dict[str, Any]],
    refresh_status: str,
    after: dict[str, Any],
) -> list[str]:
    limitations: list[str] = []
    for item in inspections:
        if item.get("status") == "STALE_FOR_TARGET_DATE":
            limitations.append(
                f"{item.get('canonical_symbol')} repaired artifact is valid only through "
                f"{_mapping(item.get('date_range')).get('end')}."
            )
        elif item.get("status") == "FAILED":
            limitations.append(
                f"{item.get('canonical_symbol')} reconcile failed with {item.get('error_code')}."
            )
    if refresh_status == "FALLBACK_VALID_MANIFEST":
        limitations.append(
            "Latest market date could not be refreshed with complete repaired assets; "
            "reconciled latest resolution falls back to the latest valid manifest."
        )
    after_status = _text(_mapping(after.get("metadata")).get("status"), "UNKNOWN")
    if after_status == "LIMITED":
        limitations.append("Data registry remains LIMITED after reconcile.")
    if status == "OK" and not limitations:
        limitations.append("signal_snapshots_status may remain LIMITED; promotion stays disabled.")
    return limitations


def _before_after_payload(payload: dict[str, Any]) -> dict[str, Any]:
    latest = _mapping(payload.get("latest_resolution"))
    registry = {
        _text(item.get("canonical_symbol")): _text(item.get("error_code"), "OK")
        for item in _records(payload.get("asset_registry"))
    }
    return {
        "market_data_date": _text(latest.get("resolved_market_data_date")),
        "manifest_date": _text(latest.get("resolved_backtest_manifest_date")),
        "latest_resolution": _text(latest.get("status"), "UNKNOWN"),
        "data_registry_status": _text(_mapping(payload.get("metadata")).get("status"), "UNKNOWN"),
        "blocking_errors": sorted({code for code in registry.values() if code != "OK"}),
        "asset_status": registry,
    }


def _portfolio_impact(status: str, after: dict[str, Any]) -> dict[str, Any]:
    after_status = _text(_mapping(after.get("metadata")).get("status"), "UNKNOWN")
    if status in {"OK", "LIMITED"} and after_status in {"OK", "LIMITED"}:
        summary = (
            "Price cache reconciliation removed hard manifest/cache asset mismatches; "
            "portfolio sensitivity may run if validate-data also passes."
        )
    else:
        summary = (
            "Portfolio sensitivity remains blocked by unresolved price cache or manifest "
            "consistency errors."
        )
    return {"status": after_status, "summary": summary}


def _shadow_backtest_impact(status: str, after: dict[str, Any]) -> dict[str, Any]:
    after_status = _text(_mapping(after.get("metadata")).get("status"), "UNKNOWN")
    if status in {"OK", "LIMITED"} and after_status in {"OK", "LIMITED"}:
        summary = (
            "Shadow backtest can reference the reconciled manifest context, but promotion "
            "remains rejected unless all signal and promotion gates pass."
        )
    else:
        summary = "Shadow backtest remains blocked by data registry consistency."
    return {"status": after_status, "summary": summary}


def _metadata(
    *,
    run_id: str,
    generated_at: datetime,
    status: str,
    as_of: date,
    config_path: Path,
) -> dict[str, Any]:
    return {
        "run_id": run_id,
        "as_of": as_of.isoformat(),
        "generated_at": generated_at.isoformat(),
        "status": status,
        "production_effect": "none",
        "manual_review_required": True,
        "auto_promotion": False,
        "config_path": str(config_path),
        "code_version": git_commit_sha() or "unknown",
        "git_worktree_dirty": git_worktree_dirty(),
    }


def _latest_common_required_asset_date(
    prices: pd.DataFrame,
    required_assets: tuple[str, ...],
    target_date: date,
) -> date | None:
    if prices.empty or not required_assets or not {"date", "ticker"}.issubset(prices.columns):
        return None
    common_dates: set[date] | None = None
    for asset in required_assets:
        rows = prices.loc[prices["ticker"].astype(str) == asset]
        if rows.empty:
            return None
        parsed = pd.to_datetime(rows["date"], errors="coerce").dropna()
        dates = {
            pd.Timestamp(value).date()
            for value in parsed
            if pd.Timestamp(value).date() <= target_date
        }
        if not dates:
            return None
        common_dates = dates if common_dates is None else common_dates & dates
        if not common_dates:
            return None
    return max(common_dates) if common_dates else None


def _read_prices(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except Exception:
        return pd.DataFrame()


def _read_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}


def _frame_date_range(frame: pd.DataFrame) -> dict[str, str]:
    if frame.empty or "date" not in frame.columns:
        return {"start": "", "end": ""}
    parsed = pd.to_datetime(frame["date"], errors="coerce").dropna()
    if parsed.empty:
        return {"start": "", "end": ""}
    return {
        "start": pd.Timestamp(parsed.min()).date().isoformat(),
        "end": pd.Timestamp(parsed.max()).date().isoformat(),
    }


def _normalize_symbols(symbols: tuple[str, ...]) -> tuple[str, ...]:
    seen: set[str] = set()
    normalized: list[str] = []
    for symbol in symbols:
        value = str(symbol).strip().upper()
        if not value or value in seen:
            continue
        seen.add(value)
        normalized.append(value)
    return tuple(normalized)


def _without_private_keys(payload: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in payload.items() if not key.startswith("_")}


def _sha256_file(path: Path) -> str:
    if not path.exists():
        return ""
    digest = sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _parse_datetime(value: object) -> datetime | None:
    try:
        parsed = datetime.fromisoformat(str(value))
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed


def _parse_date(value: object) -> date | None:
    try:
        return date.fromisoformat(str(value))
    except (TypeError, ValueError):
        return None


def _definition_lines(payload: dict[str, Any], keys: tuple[str, ...]) -> str:
    return "\n".join(f"- {key}：`{payload.get(key, 'UNKNOWN')}`" for key in keys)


def _mapping(value: object) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _records(value: object) -> list[dict[str, Any]]:
    return [item for item in value if isinstance(item, dict)] if isinstance(value, list) else []


def _strings(value: object) -> list[str]:
    if isinstance(value, list | tuple):
        return [str(item) for item in value if str(item)]
    return []


def _text(value: object, default: str = "") -> str:
    if value is None:
        return default
    text = str(value)
    return text if text else default


def _escape_table(value: str) -> str:
    return value.replace("|", "\\|").replace("\n", " ")
