from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.shadow.lineage import git_commit_sha, git_worktree_dirty
from ai_trading_system.trading_engine.backtest_input_diagnostics import (
    DEFAULT_REQUIRED_BACKTEST_ASSETS,
)
from ai_trading_system.trading_engine.data.symbol_resolver import (
    SymbolResolution,
    resolve_symbol,
    symbol_mapping_payload,
)
from ai_trading_system.trading_engine.parameters.parameter_loader import (
    DEFAULT_SHADOW_BACKTEST_CONFIG_PATH,
    load_production_parameters,
    load_shadow_backtest_config,
    resolve_project_path,
)
from ai_trading_system.trading_engine.parameters.parameter_schema import (
    ShadowBacktestConfig,
)

DATA_REGISTRY_CONSISTENCY_SCHEMA_VERSION = 1
DATA_REGISTRY_CONSISTENCY_REPORT_TYPE = "data_registry_consistency"
PRICE_CACHE_REGISTRY_REPORT_TYPE = "price_cache_registry"
DATA_GATE_ERROR_CODES: tuple[str, ...] = (
    "MISSING_PRICE_SOURCE",
    "UNREGISTERED_REPAIR_ARTIFACT",
    "SYMBOL_MAPPING_MISSING",
    "LATEST_DATE_MISMATCH",
    "MANIFEST_PRICE_CACHE_MISMATCH",
    "STALE_CACHE",
)


@dataclass(frozen=True)
class DataRegistryConsistencyRun:
    as_of: date
    payload: dict[str, Any]
    json_path: Path
    markdown_path: Path


@dataclass(frozen=True)
class BacktestManifestContext:
    path: Path | None
    payload: dict[str, Any]
    manifest_date: date | None
    status: str
    validation_status: str
    validation_errors: tuple[str, ...]

    @property
    def exists(self) -> bool:
        return self.path is not None and self.path.exists()

    @property
    def price_data_files(self) -> tuple[Path, ...]:
        return tuple(Path(str(item)) for item in _strings(self.payload.get("price_data_files")))

    @property
    def assets(self) -> tuple[str, ...]:
        return tuple(_strings(self.payload.get("assets")))

    @property
    def symbol_mapping(self) -> dict[str, object]:
        return _mapping(self.payload.get("symbol_mapping"))

    @property
    def date_range(self) -> dict[str, Any]:
        return _mapping(self.payload.get("date_range"))


def default_data_registry_consistency_dir(output_root: Path, as_of: date) -> Path:
    return output_root / "data_quality" / as_of.isoformat()


def default_data_registry_consistency_json_path(output_root: Path, as_of: date) -> Path:
    return (
        default_data_registry_consistency_dir(
            output_root,
            as_of,
        )
        / "data_registry_consistency.json"
    )


def default_data_registry_consistency_markdown_path(output_root: Path, as_of: date) -> Path:
    return (
        default_data_registry_consistency_dir(
            output_root,
            as_of,
        )
        / "data_registry_consistency.md"
    )


def run_data_registry_consistency(
    *,
    as_of: date | None = None,
    config_path: Path | str = DEFAULT_SHADOW_BACKTEST_CONFIG_PATH,
    output_root: Path | None = None,
    generated_at: datetime | None = None,
) -> DataRegistryConsistencyRun:
    root = output_root or (PROJECT_ROOT / "artifacts")
    payload = build_data_registry_consistency_payload(
        as_of=as_of,
        config_path=config_path,
        output_root=root,
        generated_at=generated_at,
    )
    resolved_as_of = _payload_date(payload)
    json_path = default_data_registry_consistency_json_path(root, resolved_as_of)
    markdown_path = default_data_registry_consistency_markdown_path(root, resolved_as_of)
    write_data_registry_consistency_report(payload, json_path, markdown_path)
    return DataRegistryConsistencyRun(
        as_of=resolved_as_of,
        payload=payload,
        json_path=json_path,
        markdown_path=markdown_path,
    )


def build_data_registry_consistency_payload(
    *,
    as_of: date | None = None,
    config_path: Path | str = DEFAULT_SHADOW_BACKTEST_CONFIG_PATH,
    output_root: Path | None = None,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(tz=UTC)
    root = output_root or (PROJECT_ROOT / "artifacts")
    resolved_config_path = resolve_project_path(str(config_path))
    config = load_shadow_backtest_config(resolved_config_path)
    prices_path = resolve_project_path(config.data.prices_path)
    prices = _read_prices(prices_path)
    raw_market_data_date = _latest_price_date(prices)
    target_date = as_of or raw_market_data_date or generated.date()
    manifest = latest_valid_backtest_manifest_context(
        output_root=root,
        as_of=target_date,
        expected_prices_path=prices_path,
    )
    required_assets = _required_assets(config, manifest)
    market_data_date = (
        _latest_required_asset_common_date(prices, required_assets) or raw_market_data_date
    )
    latest_resolution = _latest_resolution(
        requested=target_date if as_of else None,
        market_data_date=market_data_date,
        manifest=manifest,
        output_root=root,
    )
    asset_registry = _asset_registry(
        required_assets=required_assets,
        prices=prices,
        prices_path=prices_path,
        manifest=manifest,
    )
    path_consistency = _path_consistency(
        prices_path=prices_path,
        config=config,
        manifest=manifest,
    )
    repair_plan = _repair_plan(asset_registry, latest_resolution, path_consistency)
    status = _overall_status(
        latest_resolution=latest_resolution,
        asset_registry=asset_registry,
        path_consistency=path_consistency,
    )
    resolved_manifest_date = manifest.manifest_date
    resolved_date = as_of or resolved_manifest_date or market_data_date or generated.date()
    return {
        "schema_version": DATA_REGISTRY_CONSISTENCY_SCHEMA_VERSION,
        "report_type": DATA_REGISTRY_CONSISTENCY_REPORT_TYPE,
        "metadata": {
            "run_id": f"data-registry-consistency-{resolved_date.isoformat()}",
            "generated_at": generated.isoformat(),
            "status": status,
            "production_effect": "none",
            "manual_review_required": True,
            "auto_promotion": False,
            "config_path": str(resolved_config_path),
            "code_version": git_commit_sha() or "unknown",
            "git_worktree_dirty": git_worktree_dirty(),
        },
        "latest_resolution": latest_resolution,
        "required_assets": list(required_assets),
        "asset_registry": asset_registry,
        "symbol_mapping": symbol_mapping_payload(
            required_assets,
            manifest_mapping=manifest.symbol_mapping,
            used_by=("repair", "diagnostics", "validate_data", "portfolio_sensitivity"),
        ),
        "price_cache_registry": _price_cache_registry_payload(root),
        "path_consistency": path_consistency,
        "backtest_manifest": _manifest_payload(manifest),
        "data_gate_error_codes": list(DATA_GATE_ERROR_CODES),
        "repair_plan": repair_plan,
        "impact_on_portfolio_sensitivity": _portfolio_sensitivity_impact(status, repair_plan),
        "safety": {
            "production_effect": "none",
            "manual_review_required": True,
            "auto_promotion": False,
            "production_parameters_modified": False,
            "data_quality_gate_lowered": False,
        },
    }


def write_data_registry_consistency_report(
    payload: dict[str, Any],
    json_path: Path,
    markdown_path: Path,
) -> tuple[Path, Path]:
    json_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    markdown_path.write_text(render_data_registry_consistency_markdown(payload), encoding="utf-8")
    return json_path, markdown_path


def render_data_registry_consistency_markdown(payload: dict[str, Any]) -> str:
    metadata = _mapping(payload.get("metadata"))
    latest = _mapping(payload.get("latest_resolution"))
    path_consistency = _mapping(payload.get("path_consistency"))
    manifest = _mapping(payload.get("backtest_manifest"))
    cache_registry = _mapping(payload.get("price_cache_registry"))
    repair_plan = _records(payload.get("repair_plan"))
    asset_registry = _records(payload.get("asset_registry"))
    lines = [
        "# Data Registry Consistency Report",
        "",
        "## 1. Executive Summary",
        "",
        f"- 状态：`{metadata.get('status', 'UNKNOWN')}`",
        f"- production_effect：`{metadata.get('production_effect', 'none')}`",
        f"- manual_review_required：`{metadata.get('manual_review_required', True)}`",
        f"- auto_promotion：`{metadata.get('auto_promotion', False)}`",
        "",
        "## 2. Latest Resolution",
        "",
        _definition_lines(
            latest,
            (
                "requested",
                "resolved_artifact_date",
                "resolved_market_data_date",
                "resolved_backtest_manifest_date",
                "status",
                "warning",
            ),
        ),
        "",
        "## 3. Required Asset Registry",
        "",
        (
            "| Symbol | Source Symbol | Repair Manifest | Validate Data | "
            "Portfolio Sensitivity | Diagnosis |"
        ),
        "|---|---|---|---|---|---|",
    ]
    for asset in asset_registry:
        lines.append(
            "| "
            f"`{asset.get('canonical_symbol', '')}` | "
            f"`{asset.get('source_symbol', '')}` | "
            f"`{asset.get('repair_manifest_status', '')}` | "
            f"`{asset.get('validate_data_status', '')}` | "
            f"`{asset.get('portfolio_sensitivity_status', '')}` | "
            f"{_escape_table(str(asset.get('diagnosis', '')))} |"
        )
    lines.extend(
        [
            "",
            "## 4. Price Cache Path Consistency",
            "",
            _definition_lines(
                path_consistency,
                (
                    "status",
                    "repair_write_path",
                    "validate_data_read_path",
                    "diagnostics_read_path",
                    "portfolio_sensitivity_read_path",
                    "reason",
                ),
            ),
            "",
            "## 5. Price Cache Registry",
            "",
            _definition_lines(
                cache_registry,
                (
                    "path",
                    "exists",
                    "status",
                    "registered_asset_count",
                ),
            ),
            "",
            "## 6. Symbol Mapping Consistency",
            "",
            "| Canonical | Source | Status | Used By |",
            "|---|---|---|---|",
        ]
    )
    for symbol, item in _mapping(payload.get("symbol_mapping")).items():
        record = _mapping(item)
        lines.append(
            "| "
            f"`{symbol}` | "
            f"`{record.get('source_symbol', '')}` | "
            f"`{record.get('mapping_status', '')}` | "
            f"{', '.join(_strings(record.get('used_by'))) or 'n/a'} |"
        )
    lines.extend(
        [
            "",
            "## 7. Backtest Manifest Consistency",
            "",
            _definition_lines(
                manifest,
                (
                    "path",
                    "exists",
                    "status",
                    "validation_status",
                    "manifest_date",
                ),
            ),
            "",
            "## 8. Blocking Errors",
            "",
        ]
    )
    blocking = [
        str(asset.get("error_code")) + ": " + str(asset.get("diagnosis"))
        for asset in asset_registry
        if str(asset.get("error_code", "OK")) != "OK"
    ]
    if latest.get("status") == "MISMATCH":
        blocking.append("LATEST_DATE_MISMATCH: " + str(latest.get("warning", "")))
    if path_consistency.get("status") != "OK":
        blocking.append(str(path_consistency.get("error_code", "MANIFEST_PRICE_CACHE_MISMATCH")))
    lines.extend([f"- {item}" for item in blocking] or ["- 无阻断错误。"])
    lines.extend(
        [
            "",
            "## 9. Repair / Reconcile Plan",
            "",
        ]
    )
    if repair_plan:
        for step in repair_plan:
            lines.append(
                "- "
                f"action=`{step.get('action', '')}`；"
                f"symbols={', '.join(_strings(step.get('symbols'))) or 'n/a'}；"
                f"required=`{step.get('required', False)}`"
            )
    else:
        lines.append("- 当前无需 reconcile。")
    lines.extend(
        [
            "",
            "## 10. Impact on Portfolio Sensitivity",
            "",
            f"- {_mapping(payload.get('impact_on_portfolio_sensitivity')).get('summary', '')}",
            "",
            "## 11. Manual Review Checklist",
            "",
            (
                "- 确认 latest backtest manifest、market data latest date 和 "
                "portfolio sensitivity date 是否一致。"
            ),
            (
                "- 确认 `BRK.B` 的 source symbol `BRK-B` 在 repair、diagnostics、"
                "validate-data 和 sensitivity 中一致。"
            ),
            "- 只有 registry 与 data quality gate 一致后，才解释 portfolio sensitivity 结论。",
            "- 不修改 `config/parameters/production/current.yaml`，不放宽 data quality gate。",
        ]
    )
    return "\n".join(lines).rstrip() + "\n"


def latest_valid_backtest_manifest_context(
    *,
    output_root: Path | None = None,
    as_of: date | None = None,
    expected_prices_path: Path | None = None,
) -> BacktestManifestContext:
    root = output_root or (PROJECT_ROOT / "artifacts")
    candidates: list[tuple[date, Path, dict[str, Any], tuple[str, ...]]] = []
    for path in (root / "backtest_snapshots").glob("*/backtest_input_manifest.json"):
        manifest_date = _date_from_parent(path)
        if manifest_date is None or (as_of is not None and manifest_date > as_of):
            continue
        payload = load_backtest_manifest_payload(path)
        if not payload:
            continue
        errors = _manifest_validation_errors(
            path,
            payload,
            expected_prices_path=expected_prices_path,
        )
        if errors:
            continue
        candidates.append((manifest_date, path, payload, errors))
    if candidates:
        manifest_date, path, payload, errors = max(
            candidates,
            key=lambda item: (item[0], item[1].stat().st_mtime),
        )
        return BacktestManifestContext(
            path=path,
            payload=payload,
            manifest_date=manifest_date,
            status=_text(payload.get("status"), "UNKNOWN"),
            validation_status="OK",
            validation_errors=errors,
        )
    latest = latest_backtest_manifest_context(
        output_root=root,
        as_of=as_of,
        expected_prices_path=expected_prices_path,
    )
    if latest.path is None:
        return latest
    return BacktestManifestContext(
        path=latest.path,
        payload=latest.payload,
        manifest_date=latest.manifest_date,
        status=latest.status,
        validation_status="FAILED",
        validation_errors=latest.validation_errors,
    )


def latest_backtest_manifest_context(
    *,
    output_root: Path | None = None,
    as_of: date | None = None,
    expected_prices_path: Path | None = None,
) -> BacktestManifestContext:
    root = output_root or (PROJECT_ROOT / "artifacts")
    candidates: list[tuple[date, Path]] = []
    for path in (root / "backtest_snapshots").glob("*/backtest_input_manifest.json"):
        manifest_date = _date_from_parent(path)
        if manifest_date is None or (as_of is not None and manifest_date > as_of):
            continue
        candidates.append((manifest_date, path))
    if not candidates:
        return BacktestManifestContext(
            path=None,
            payload={},
            manifest_date=None,
            status="MISSING",
            validation_status="MISSING",
            validation_errors=("backtest input manifest not found",),
        )
    manifest_date, path = max(candidates, key=lambda item: (item[0], item[1].stat().st_mtime))
    payload = load_backtest_manifest_payload(path)
    errors = _manifest_validation_errors(path, payload, expected_prices_path=expected_prices_path)
    return BacktestManifestContext(
        path=path,
        payload=payload,
        manifest_date=manifest_date,
        status=_text(payload.get("status"), "UNKNOWN"),
        validation_status="OK" if not errors else "FAILED",
        validation_errors=errors,
    )


def load_backtest_manifest_payload(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def validate_backtest_manifest_consistency(
    *,
    as_of: date | None = None,
    config_path: Path | str = DEFAULT_SHADOW_BACKTEST_CONFIG_PATH,
    output_root: Path | None = None,
) -> dict[str, Any]:
    payload = build_data_registry_consistency_payload(
        as_of=as_of,
        config_path=config_path,
        output_root=output_root,
    )
    return {
        "status": _mapping(payload.get("metadata")).get("status", "UNKNOWN"),
        "latest_resolution": payload.get("latest_resolution", {}),
        "asset_registry": payload.get("asset_registry", []),
        "backtest_manifest": payload.get("backtest_manifest", {}),
        "safety": payload.get("safety", {}),
    }


def reconcile_price_cache_plan(
    *,
    as_of: date | None = None,
    config_path: Path | str = DEFAULT_SHADOW_BACKTEST_CONFIG_PATH,
    output_root: Path | None = None,
    dry_run: bool = True,
) -> dict[str, Any]:
    payload = build_data_registry_consistency_payload(
        as_of=as_of,
        config_path=config_path,
        output_root=output_root,
    )
    plan = _records(payload.get("repair_plan"))
    status = "DRY_RUN" if dry_run else "NOT_IMPLEMENTED"
    if dry_run and not plan:
        status = "NOT_REQUIRED"
    return {
        "status": status,
        "production_effect": "none",
        "manual_review_required": True,
        "auto_promotion": False,
        "repair_plan": plan,
        "data_registry_status": _mapping(payload.get("metadata")).get("status", "UNKNOWN"),
        "latest_resolution": payload.get("latest_resolution", {}),
        "message": (
            "实际 price cache reconcile 尚未实现；请先人工复核 repair plan。"
            if not dry_run
            else "dry-run 只生成 reconcile plan，不下载或改写数据。"
        ),
    }


def portfolio_sensitivity_data_gate_context(
    *,
    as_of: date | None,
    config_path: Path | str,
    output_root: Path | None = None,
) -> dict[str, Any]:
    payload = build_data_registry_consistency_payload(
        as_of=as_of,
        config_path=config_path,
        output_root=output_root,
    )
    metadata = _mapping(payload.get("metadata"))
    manifest = _mapping(payload.get("backtest_manifest"))
    latest = _mapping(payload.get("latest_resolution"))
    asset_errors = [
        record
        for record in _records(payload.get("asset_registry"))
        if _text(record.get("error_code"), "OK") != "OK"
    ]
    registry_status = _text(metadata.get("status"), "UNKNOWN")
    status = (
        "OK"
        if registry_status in {"OK", "LIMITED"}
        and not asset_errors
        and manifest.get("validation_status") == "OK"
        else "FAILED"
    )
    error = _primary_error(asset_errors, latest, manifest)
    return {
        "status": status,
        "source": "backtest_input_manifest",
        "manifest": manifest.get("path", ""),
        "latest_resolution_status": latest.get("status", "UNKNOWN"),
        "data_registry_consistency": metadata.get("status", "UNKNOWN"),
        "price_cache_registry": "OK" if not asset_errors else error["error_code"],
        "symbol_mapping": _symbol_mapping_status(payload),
        **({} if status == "OK" else error),
    }


def manifest_context_from_path(path: Path | None) -> BacktestManifestContext:
    if path is None:
        return BacktestManifestContext(
            path=None,
            payload={},
            manifest_date=None,
            status="MISSING",
            validation_status="MISSING",
            validation_errors=("backtest input manifest not provided",),
        )
    payload = load_backtest_manifest_payload(path)
    errors = _manifest_validation_errors(path, payload, expected_prices_path=None)
    return BacktestManifestContext(
        path=path,
        payload=payload,
        manifest_date=_date_from_parent(path),
        status=_text(payload.get("status"), "UNKNOWN"),
        validation_status="OK" if not errors else "FAILED",
        validation_errors=errors,
    )


def _asset_registry(
    *,
    required_assets: tuple[str, ...],
    prices: pd.DataFrame,
    prices_path: Path,
    manifest: BacktestManifestContext,
) -> list[dict[str, Any]]:
    registry: list[dict[str, Any]] = []
    for asset in required_assets:
        resolution = resolve_symbol(
            asset,
            manifest_mapping=manifest.symbol_mapping,
            used_by=("repair", "diagnostics", "validate_data", "portfolio_sensitivity"),
        )
        present = _price_asset_present(prices, resolution)
        manifest_has_asset = asset in manifest.assets
        repair_status = "OK" if manifest_has_asset else "MISSING"
        validate_status = "OK" if present else "MISSING"
        sensitivity_status = "OK" if present and manifest_has_asset else "MISSING"
        error_code, diagnosis = _asset_diagnosis(
            resolution=resolution,
            present=present,
            manifest_has_asset=manifest_has_asset,
            manifest=manifest,
            prices_path=prices_path,
        )
        registry.append(
            {
                "canonical_symbol": asset,
                "source_symbol": resolution.source_symbol,
                "mapping_status": resolution.mapping_status,
                "repair_manifest_status": repair_status,
                "validate_data_status": validate_status,
                "portfolio_sensitivity_status": sensitivity_status,
                "error_code": error_code,
                "diagnosis": diagnosis,
                "validate_data_read_path": str(prices_path),
                "manifest_path": "" if manifest.path is None else str(manifest.path),
            }
        )
    return registry


def _asset_diagnosis(
    *,
    resolution: SymbolResolution,
    present: bool,
    manifest_has_asset: bool,
    manifest: BacktestManifestContext,
    prices_path: Path,
) -> tuple[str, str]:
    if resolution.mapping_status != "OK":
        return (
            "SYMBOL_MAPPING_MISSING",
            f"{resolution.canonical_symbol} requires source symbol {resolution.source_symbol}.",
        )
    if present and manifest_has_asset:
        if resolution.canonical_symbol != resolution.source_symbol:
            return (
                "OK",
                f"{resolution.canonical_symbol} OK via {resolution.source_symbol}.",
            )
        return "OK", "Price cache and backtest manifest agree."
    if not present and manifest_has_asset:
        return (
            "MANIFEST_PRICE_CACHE_MISMATCH",
            "Backtest manifest declares repaired/required price history, but "
            f"validate-data read path has no rows for {resolution.canonical_symbol}: "
            f"{prices_path}",
        )
    if present and not manifest_has_asset:
        return (
            "UNREGISTERED_REPAIR_ARTIFACT",
            "Primary price cache has rows but latest valid backtest manifest does not register "
            f"{resolution.canonical_symbol}.",
        )
    if manifest.path is None:
        return (
            "MISSING_PRICE_SOURCE",
            "No valid backtest input manifest is available for price cache validation.",
        )
    return (
        "MISSING_PRICE_SOURCE",
        f"Missing primary price source for {resolution.canonical_symbol} at {prices_path}.",
    )


def _price_asset_present(prices: pd.DataFrame, resolution: SymbolResolution) -> bool:
    if prices.empty:
        return False
    candidates = [
        ("ticker", resolution.canonical_symbol),
        ("canonical_symbol", resolution.canonical_symbol),
        ("source_symbol", resolution.source_symbol),
    ]
    for column, value in candidates:
        if column in prices.columns and (prices[column].astype(str) == value).any():
            return True
    return False


def _latest_resolution(
    *,
    requested: date | None,
    market_data_date: date | None,
    manifest: BacktestManifestContext,
    output_root: Path,
) -> dict[str, Any]:
    artifact_date = _latest_artifact_date(output_root)
    manifest_date = manifest.manifest_date
    requested_label = requested.isoformat() if requested is not None else "--latest"
    status = "OK"
    warning = ""
    if (
        requested is None
        and market_data_date
        and manifest_date
        and market_data_date != manifest_date
    ):
        status = "MISMATCH"
        warning = (
            "Portfolio sensitivity latest should use the latest valid backtest manifest "
            f"({manifest_date.isoformat()}) while market data latest is "
            f"{market_data_date.isoformat()}."
        )
    if manifest.validation_status != "OK":
        status = "MISMATCH"
        warning = "; ".join(manifest.validation_errors) or "No valid backtest manifest."
    return {
        "requested": requested_label,
        "resolved_artifact_date": "" if artifact_date is None else artifact_date.isoformat(),
        "resolved_market_data_date": (
            "" if market_data_date is None else market_data_date.isoformat()
        ),
        "resolved_backtest_manifest_date": (
            "" if manifest_date is None else manifest_date.isoformat()
        ),
        "status": status,
        "warning": warning,
    }


def _path_consistency(
    *,
    prices_path: Path,
    config: ShadowBacktestConfig,
    manifest: BacktestManifestContext,
) -> dict[str, Any]:
    manifest_paths = manifest.price_data_files
    diagnostics_path = "" if manifest.path is None else str(manifest.path)
    manifest_matches = any(_same_path(path, prices_path) for path in manifest_paths)
    status = "OK" if manifest_matches and manifest.validation_status == "OK" else "FAILED"
    reason = ""
    error_code = "OK"
    if not manifest_paths:
        reason = "Backtest manifest does not contain price_data_files."
        error_code = "UNREGISTERED_REPAIR_ARTIFACT"
    elif not manifest_matches:
        reason = "Backtest manifest price_data_files do not match validate-data read path."
        error_code = "MANIFEST_PRICE_CACHE_MISMATCH"
    elif manifest.validation_status != "OK":
        reason = "; ".join(manifest.validation_errors)
        error_code = "MANIFEST_PRICE_CACHE_MISMATCH"
    return {
        "status": status,
        "repair_write_path": str(prices_path),
        "validate_data_read_path": str(prices_path),
        "diagnostics_read_path": diagnostics_path,
        "portfolio_sensitivity_read_path": str(resolve_project_path(config.data.prices_path)),
        "manifest_price_data_files": [str(path) for path in manifest_paths],
        "reason": reason,
        "error_code": error_code,
    }


def _repair_plan(
    asset_registry: list[dict[str, Any]],
    latest_resolution: dict[str, Any],
    path_consistency: dict[str, Any],
) -> list[dict[str, Any]]:
    steps: list[dict[str, Any]] = []
    grouped: dict[str, list[str]] = {}
    for record in asset_registry:
        code = _text(record.get("error_code"), "OK")
        if code == "OK":
            continue
        grouped.setdefault(code, []).append(_text(record.get("canonical_symbol")))
    action_by_code = {
        "MISSING_PRICE_SOURCE": "repair_missing_primary_price_history",
        "UNREGISTERED_REPAIR_ARTIFACT": "register_repaired_price_history_in_primary_cache_manifest",
        "SYMBOL_MAPPING_MISSING": "apply_symbol_mapping_to_validate_data",
        "MANIFEST_PRICE_CACHE_MISMATCH": "rebuild_backtest_input_manifest_from_primary_cache",
    }
    for code, symbols in grouped.items():
        steps.append(
            {
                "action": action_by_code.get(code, "investigate_data_registry_error"),
                "error_code": code,
                "symbols": [symbol for symbol in symbols if symbol],
                "required": True,
            }
        )
    if latest_resolution.get("status") == "MISMATCH":
        steps.append(
            {
                "action": "rerun_backtest_input_diagnostics_for_latest_market_data_date",
                "error_code": "LATEST_DATE_MISMATCH",
                "symbols": [],
                "required": True,
            }
        )
    if path_consistency.get("status") != "OK":
        steps.append(
            {
                "action": "align_repair_validate_data_and_portfolio_sensitivity_price_paths",
                "error_code": path_consistency.get("error_code", "MANIFEST_PRICE_CACHE_MISMATCH"),
                "symbols": [],
                "required": True,
            }
        )
    unique: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for step in steps:
        key = (_text(step.get("action")), _text(step.get("error_code")))
        if key in seen:
            continue
        seen.add(key)
        unique.append(step)
    return unique


def _overall_status(
    *,
    latest_resolution: dict[str, Any],
    asset_registry: list[dict[str, Any]],
    path_consistency: dict[str, Any],
) -> str:
    if any(_text(item.get("error_code"), "OK") != "OK" for item in asset_registry):
        return "FAILED"
    if path_consistency.get("status") != "OK":
        return "FAILED"
    if latest_resolution.get("status") == "MISMATCH":
        return "LIMITED"
    return "OK"


def _manifest_payload(manifest: BacktestManifestContext) -> dict[str, Any]:
    return {
        "path": "" if manifest.path is None else str(manifest.path),
        "exists": manifest.exists,
        "status": manifest.status,
        "validation_status": manifest.validation_status,
        "validation_errors": list(manifest.validation_errors),
        "manifest_date": (
            "" if manifest.manifest_date is None else manifest.manifest_date.isoformat()
        ),
        "assets": list(manifest.assets),
        "price_data_files": [str(path) for path in manifest.price_data_files],
        "date_range": manifest.date_range,
    }


def _portfolio_sensitivity_impact(status: str, repair_plan: list[dict[str, Any]]) -> dict[str, Any]:
    if status == "OK":
        summary = (
            "Portfolio sensitivity data registry is consistent. Repaired price history is "
            "recognized by validate-data and sensitivity can use the latest valid manifest."
        )
    else:
        summary = (
            "Portfolio sensitivity remains blocked or limited by data registry consistency; "
            "run reconcile/diagnostics before interpreting sensitivity results."
        )
    return {"status": status, "summary": summary, "repair_plan_count": len(repair_plan)}


def _primary_error(
    asset_errors: list[dict[str, Any]],
    latest: dict[str, Any],
    manifest: dict[str, Any],
) -> dict[str, str]:
    if asset_errors:
        first = asset_errors[0]
        code = _text(first.get("error_code"), "MISSING_PRICE_SOURCE")
        return {
            "error_code": code,
            "reason": _text(first.get("diagnosis"), "Data registry consistency failed."),
            "suggested_action": _suggested_action(code),
        }
    if latest.get("status") == "MISMATCH":
        return {
            "error_code": "LATEST_DATE_MISMATCH",
            "reason": _text(latest.get("warning"), "Latest date resolution mismatch."),
            "suggested_action": "Run aits data inspect-registry --latest.",
        }
    if manifest.get("validation_status") != "OK":
        return {
            "error_code": "MANIFEST_PRICE_CACHE_MISMATCH",
            "reason": "; ".join(_strings(manifest.get("validation_errors"))),
            "suggested_action": "Run aits data validate-backtest-manifest --latest.",
        }
    return {
        "error_code": "MISSING_PRICE_SOURCE",
        "reason": "Data registry consistency failed.",
        "suggested_action": "Run aits data inspect-registry --latest.",
    }


def _suggested_action(code: str) -> str:
    if code == "SYMBOL_MAPPING_MISSING":
        return "Review symbol resolver mapping and rerun backtest input diagnostics."
    if code == "MANIFEST_PRICE_CACHE_MISMATCH":
        return "Run aits data validate-backtest-manifest --latest."
    if code == "UNREGISTERED_REPAIR_ARTIFACT":
        return "Run aits data reconcile-price-cache --latest."
    return "Run aits data reconcile-price-cache --latest --dry-run."


def _symbol_mapping_status(payload: dict[str, Any]) -> str:
    mapping = _mapping(payload.get("symbol_mapping"))
    statuses = {_text(_mapping(item).get("mapping_status"), "UNKNOWN") for item in mapping.values()}
    return "FAILED" if "SYMBOL_MAPPING_MISSING" in statuses else "OK"


def _required_assets(
    config: ShadowBacktestConfig,
    manifest: BacktestManifestContext,
) -> tuple[str, ...]:
    if manifest.assets:
        return manifest.assets
    try:
        baseline = load_production_parameters(resolve_project_path(config.baseline_parameters_path))
    except (OSError, ValueError):
        return DEFAULT_REQUIRED_BACKTEST_ASSETS
    assets = tuple(asset for asset in baseline.flattened_asset_universe() if asset != "CASH")
    return assets or DEFAULT_REQUIRED_BACKTEST_ASSETS


def _manifest_validation_errors(
    path: Path,
    payload: dict[str, Any],
    *,
    expected_prices_path: Path | None,
) -> tuple[str, ...]:
    errors: list[str] = []
    if not payload:
        return ("manifest is unreadable or empty",)
    if payload.get("report_type") != "backtest_input_manifest":
        errors.append("manifest report_type is not backtest_input_manifest")
    if _text(payload.get("status")).upper() == "FAILED":
        errors.append("manifest status is FAILED")
    price_files = [Path(str(item)) for item in _strings(payload.get("price_data_files"))]
    if not price_files:
        errors.append("manifest has no price_data_files")
    missing = [str(item) for item in price_files if not item.exists()]
    if missing:
        errors.append("manifest price_data_files do not exist: " + ", ".join(missing))
    if expected_prices_path is not None and price_files:
        if not any(_same_path(path_item, expected_prices_path) for path_item in price_files):
            errors.append(
                "manifest price_data_files do not match configured prices_path: "
                f"{expected_prices_path}"
            )
    return tuple(errors)


def _latest_artifact_date(output_root: Path) -> date | None:
    roots = (
        output_root / "portfolio_sensitivity",
        output_root / "backtest_snapshots",
        output_root / "data_quality",
    )
    dates: list[date] = []
    for root in roots:
        if not root.exists():
            continue
        for child in root.iterdir():
            if child.is_dir():
                parsed = _parse_date(child.name)
                if parsed is not None:
                    dates.append(parsed)
    return max(dates) if dates else None


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


def _latest_required_asset_common_date(
    prices: pd.DataFrame,
    required_assets: tuple[str, ...],
) -> date | None:
    if prices.empty or not required_assets or not {"date", "ticker"}.issubset(prices.columns):
        return None
    common_dates: set[date] | None = None
    for asset in required_assets:
        rows = prices.loc[prices["ticker"].astype(str) == asset]
        if rows.empty:
            return None
        parsed = pd.to_datetime(rows["date"], errors="coerce").dropna()
        dates = {pd.Timestamp(item).date() for item in parsed}
        if not dates:
            return None
        common_dates = dates if common_dates is None else common_dates & dates
        if not common_dates:
            return None
    return max(common_dates) if common_dates else None


def _price_cache_registry_payload(output_root: Path) -> dict[str, Any]:
    path = output_root / "data_registry" / "price_cache_registry.json"
    payload = load_backtest_manifest_payload(path) if path.exists() else {}
    metadata = _mapping(payload.get("metadata"))
    assets = _mapping(payload.get("assets"))
    return {
        "path": str(path),
        "exists": path.exists(),
        "status": _text(metadata.get("status"), "MISSING") if payload else "MISSING",
        "registered_asset_count": len(assets),
        "registered_assets": sorted(assets),
        "report_type": _text(payload.get("report_type")),
    }


def _payload_date(payload: dict[str, Any]) -> date:
    run_id = _text(_mapping(payload.get("metadata")).get("run_id"))
    raw = run_id.removeprefix("data-registry-consistency-")
    parsed = _parse_date(raw)
    if parsed is not None:
        return parsed
    latest = _mapping(payload.get("latest_resolution"))
    for key in ("resolved_backtest_manifest_date", "resolved_market_data_date"):
        parsed = _parse_date(_text(latest.get(key)))
        if parsed is not None:
            return parsed
    return datetime.now(tz=UTC).date()


def _same_path(left: Path, right: Path) -> bool:
    try:
        return left.resolve() == right.resolve()
    except OSError:
        return left.absolute() == right.absolute()


def _date_from_parent(path: Path) -> date | None:
    return _parse_date(path.parent.name)


def _parse_date(value: str) -> date | None:
    try:
        return date.fromisoformat(value)
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
