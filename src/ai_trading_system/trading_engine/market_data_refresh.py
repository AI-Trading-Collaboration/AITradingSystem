from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from hashlib import sha256
from pathlib import Path
from typing import Any

import pandas as pd

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.data.market_data import PriceDataProvider, PriceRequest
from ai_trading_system.external_request_cache import sanitize_diagnostic_text
from ai_trading_system.shadow.lineage import git_commit_sha, git_worktree_dirty
from ai_trading_system.trading_engine.data.price_history_repair import (
    PRICE_CACHE_OUTPUT_COLUMNS,
    build_price_history_repair_provider,
    normalize_repaired_price_history,
    source_symbol_for_price_repair,
    upsert_price_history_cache,
)
from ai_trading_system.trading_engine.market_data_freshness import (
    DEFAULT_MARKET_DATA_FRESHNESS_CONFIG_PATH,
    FRESHNESS_ACCEPTABLE_LAG,
    FRESHNESS_MISSING,
    FRESHNESS_NON_TRADING_DAY,
    FRESHNESS_OK,
    FRESHNESS_STALE,
    default_market_data_freshness_json_path,
    default_market_data_freshness_root,
    latest_market_data_freshness_path,
    load_market_data_freshness_payload,
    run_market_data_freshness,
)
from ai_trading_system.trading_engine.parameters.parameter_loader import (
    DEFAULT_SHADOW_BACKTEST_CONFIG_PATH,
    load_shadow_backtest_config,
    resolve_project_path,
)
from ai_trading_system.trading_engine.portfolio_candidate_tracking import (
    DEFAULT_PORTFOLIO_CANDIDATE_TRACKING_CONFIG_PATH,
    latest_portfolio_candidate_tracking_path_on_or_before,
    load_portfolio_candidate_tracking_payload,
    run_portfolio_candidate_tracking,
)
from ai_trading_system.trading_engine.price_cache_reconcile import refresh_backtest_manifest
from ai_trading_system.yaml_loader import safe_load_yaml_path

MARKET_DATA_REFRESH_SCHEMA_VERSION = 1
MARKET_DATA_REFRESH_REPORT_TYPE = "market_data_refresh"
MARKET_DATA_REFRESH_PLAN_REPORT_TYPE = "market_data_refresh_plan"
MARKET_DATA_REFRESH_ALIAS_REPORT_TYPE = "market_data_refresh_report"
DEFAULT_MARKET_DATA_REFRESH_CONFIG_PATH = (
    PROJECT_ROOT / "config" / "data" / "market_data_refresh.yaml"
)

REFRESH_NOT_NEEDED = "NOT_NEEDED"
REFRESH_PLANNED = "PLANNED"
REFRESH_RUNNING = "RUNNING"
REFRESH_OK = "OK"
REFRESH_PARTIAL = "PARTIAL"
REFRESH_SOURCE_DELAYED = "SOURCE_DELAYED"
REFRESH_FAILED = "FAILED"
REFRESH_BLOCKED = "BLOCKED"

MARKET_DATA_REFRESH_STATUSES = {
    REFRESH_NOT_NEEDED,
    REFRESH_PLANNED,
    REFRESH_RUNNING,
    REFRESH_OK,
    REFRESH_PARTIAL,
    REFRESH_SOURCE_DELAYED,
    REFRESH_FAILED,
    REFRESH_BLOCKED,
}

_REFRESHABLE_FRESHNESS_STATUSES = {FRESHNESS_STALE, FRESHNESS_MISSING}
_FRESHNESS_NO_REFRESH_STATUSES = {
    FRESHNESS_OK,
    FRESHNESS_NON_TRADING_DAY,
    FRESHNESS_ACCEPTABLE_LAG,
}
_FMP_PROVIDER = "Financial Modeling Prep"
_FMP_EOD_CACHE_FAMILY = "eod_daily_prices"


@dataclass(frozen=True)
class MarketDataRefreshRun:
    as_of: date
    payload: dict[str, Any]
    plan_path: Path
    json_path: Path | None
    markdown_path: Path | None


def default_market_data_refresh_root() -> Path:
    return PROJECT_ROOT / "artifacts" / "data_refresh"


def default_market_data_refresh_dir(output_root: Path, as_of: date) -> Path:
    return output_root / as_of.isoformat()


def default_market_data_refresh_plan_path(output_root: Path, as_of: date) -> Path:
    return default_market_data_refresh_dir(output_root, as_of) / "market_data_refresh_plan.json"


def default_market_data_refresh_json_path(output_root: Path, as_of: date) -> Path:
    return default_market_data_refresh_dir(output_root, as_of) / "market_data_refresh_summary.json"


def default_market_data_refresh_markdown_path(output_root: Path, as_of: date) -> Path:
    return default_market_data_refresh_dir(output_root, as_of) / "market_data_refresh_summary.md"


def market_data_refresh_report_alias_paths(
    reports_dir: Path,
    as_of: date,
) -> tuple[Path, Path]:
    return (
        reports_dir / f"market_data_refresh_{as_of.isoformat()}.json",
        reports_dir / f"market_data_refresh_{as_of.isoformat()}.md",
    )


def latest_market_data_refresh_path(output_root: Path | None = None) -> Path | None:
    root = output_root or default_market_data_refresh_root()
    candidates = sorted(root.glob("*/market_data_refresh_summary.json"))
    if not candidates:
        return None
    return max(candidates, key=lambda path: path.stat().st_mtime)


def latest_market_data_refresh_path_on_or_before(
    as_of: date,
    output_root: Path | None = None,
) -> Path | None:
    root = output_root or default_market_data_refresh_root()
    candidates: list[tuple[date, Path]] = []
    for path in root.glob("*/market_data_refresh_summary.json"):
        parsed = _parse_date(path.parent.name)
        if parsed is not None and parsed <= as_of:
            candidates.append((parsed, path))
    if not candidates:
        return None
    return max(candidates, key=lambda item: (item[1].stat().st_mtime, item[0]))[1]


def load_market_data_refresh_config(
    path: Path | str = DEFAULT_MARKET_DATA_REFRESH_CONFIG_PATH,
) -> dict[str, Any]:
    payload = safe_load_yaml_path(Path(path))
    if not isinstance(payload, dict):
        raise ValueError(f"market data refresh config must be a mapping: {path}")
    _validate_config(payload)
    return payload


def run_market_data_refresh(
    *,
    as_of: date | None = None,
    symbols: tuple[str, ...] = (),
    config_path: Path | str = DEFAULT_MARKET_DATA_REFRESH_CONFIG_PATH,
    dry_run: bool = False,
    plan_only: bool = False,
    generated_at: datetime | None = None,
    price_providers: dict[str, PriceDataProvider] | None = None,
) -> MarketDataRefreshRun:
    config = load_market_data_refresh_config(config_path)
    generated = _generated_at(generated_at)
    output_root = _output_root(config, dry_run=dry_run)
    freshness_path = _resolve_freshness_path(config, as_of=as_of)
    freshness_payload = (
        load_market_data_freshness_payload(freshness_path)
        if freshness_path is not None and freshness_path.exists()
        else {}
    )
    plan = build_market_data_refresh_plan(
        freshness_payload=freshness_payload,
        freshness_path=freshness_path,
        config=config,
        config_path=Path(config_path),
        requested_symbols=symbols,
        dry_run=dry_run,
        generated_at=generated,
    )
    target_date = _target_date_from_plan(plan, generated.date())
    plan_path = default_market_data_refresh_plan_path(output_root, target_date)
    write_market_data_refresh_plan(plan, plan_path)
    if dry_run or plan_only:
        return MarketDataRefreshRun(
            as_of=target_date,
            payload=plan,
            plan_path=plan_path,
            json_path=None,
            markdown_path=None,
        )

    payload = _execute_market_data_refresh(
        plan=plan,
        config=config,
        config_path=Path(config_path),
        generated_at=generated,
        price_providers=price_providers,
    )
    json_path = default_market_data_refresh_json_path(output_root, target_date)
    markdown_path = default_market_data_refresh_markdown_path(output_root, target_date)
    payload = {
        **payload,
        "output_artifacts": {
            "plan_json": str(plan_path),
            "summary_json": str(json_path),
            "summary_markdown": str(markdown_path),
        },
    }
    write_market_data_refresh_summary(payload, json_path, markdown_path)
    return MarketDataRefreshRun(
        as_of=target_date,
        payload=payload,
        plan_path=plan_path,
        json_path=json_path,
        markdown_path=markdown_path,
    )


def build_market_data_refresh_plan(
    *,
    freshness_payload: dict[str, Any],
    freshness_path: Path | None,
    config: dict[str, Any],
    config_path: Path,
    requested_symbols: tuple[str, ...],
    dry_run: bool,
    generated_at: datetime,
) -> dict[str, Any]:
    freshness = _mapping(freshness_payload.get("freshness"))
    metadata = _mapping(freshness_payload.get("metadata"))
    data_dates = _mapping(freshness_payload.get("data_dates"))
    coverage = _mapping(freshness_payload.get("asset_coverage"))
    freshness_status = str(freshness.get("status") or metadata.get("status") or "MISSING")
    target_date = (
        _parse_date(data_dates.get("expected_data_date"))
        or _parse_date(data_dates.get("tracking_date"))
        or generated_at.date()
    )
    required_assets = _required_assets(config)
    missing_expected = _strings(coverage.get("missing_expected_date_assets"))
    refresh_targets = _refresh_targets(
        requested_symbols=requested_symbols,
        missing_expected=tuple(missing_expected),
        required_assets=required_assets,
        freshness_status=freshness_status,
    )
    status = (
        REFRESH_PLANNED
        if freshness_status in _REFRESHABLE_FRESHNESS_STATUSES
        else REFRESH_NOT_NEEDED
    )
    if not freshness_payload:
        status = REFRESH_BLOCKED
    elif not bool(_mapping(config.get("refresh")).get("enabled", True)):
        status = REFRESH_BLOCKED
    actions = _refresh_actions(
        target_date=target_date,
        refresh_targets=refresh_targets,
        config=config,
        enabled=status == REFRESH_PLANNED,
    )
    return {
        "schema_version": MARKET_DATA_REFRESH_SCHEMA_VERSION,
        "report_type": MARKET_DATA_REFRESH_PLAN_REPORT_TYPE,
        "metadata": {
            "run_id": f"market-data-refresh-plan-{target_date.isoformat()}",
            "generated_at": generated_at.isoformat(),
            "status": status,
            "production_effect": "none",
            "manual_review_required": True,
            "auto_promotion": False,
            "observe_only": True,
            "dry_run": dry_run,
            "config_path": str(resolve_project_path(str(config_path))),
            "git_commit": git_commit_sha(),
            "git_worktree_dirty": git_worktree_dirty(),
        },
        "freshness_input": {
            "freshness_status": freshness_status,
            "freshness_report": "" if freshness_path is None else str(freshness_path),
            "tracking_date": str(data_dates.get("tracking_date") or ""),
            "effective_data_date": str(data_dates.get("effective_data_date") or ""),
            "required_target_date": target_date.isoformat(),
            "tracking_readiness": str(
                _mapping(freshness_payload.get("tracking_readiness")).get("readiness") or "unknown"
            ),
        },
        "required_assets": list(required_assets),
        "refresh_targets": list(refresh_targets),
        "symbol_mapping": {
            symbol: {
                "canonical_symbol": symbol,
                "source_symbol": source_symbol_for_price_repair(symbol),
            }
            for symbol in required_assets
        },
        "refresh_actions": actions,
        "safety": {
            "production_effect": "none",
            "manual_review_required": True,
            "auto_promotion": False,
            "production_write_allowed": False,
            "mock_prices_allowed": False,
            "synthetic_latest_bar_allowed": False,
            "data_quality_gate_lowered": False,
        },
    }


def write_market_data_refresh_plan(payload: dict[str, Any], path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def write_market_data_refresh_summary(
    payload: dict[str, Any],
    json_path: Path,
    markdown_path: Path,
) -> tuple[Path, Path]:
    json_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    markdown_path.write_text(render_market_data_refresh_markdown(payload), encoding="utf-8")
    return json_path, markdown_path


def write_market_data_refresh_report_alias(
    payload: dict[str, Any],
    reports_dir: Path,
    as_of: date,
) -> tuple[Path, Path]:
    alias_payload = {
        **payload,
        "report_type": MARKET_DATA_REFRESH_ALIAS_REPORT_TYPE,
        "source_report_type": MARKET_DATA_REFRESH_REPORT_TYPE,
    }
    json_path, markdown_path = market_data_refresh_report_alias_paths(reports_dir, as_of)
    return write_market_data_refresh_summary(alias_payload, json_path, markdown_path)


def load_market_data_refresh_payload(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def validate_market_data_refresh_payload(payload: dict[str, Any]) -> list[str]:
    issues: list[str] = []
    if payload.get("schema_version") != MARKET_DATA_REFRESH_SCHEMA_VERSION:
        issues.append("schema_version mismatch")
    if payload.get("report_type") not in {
        MARKET_DATA_REFRESH_REPORT_TYPE,
        MARKET_DATA_REFRESH_ALIAS_REPORT_TYPE,
    }:
        issues.append("report_type mismatch")
    metadata = _mapping(payload.get("metadata"))
    safety = _mapping(payload.get("safety"))
    status = str(metadata.get("status") or "")
    if status not in MARKET_DATA_REFRESH_STATUSES:
        issues.append("refresh status is invalid")
    if metadata.get("production_effect") != "none":
        issues.append("production_effect must be none")
    if metadata.get("manual_review_required") is not True:
        issues.append("manual_review_required must be true")
    if metadata.get("auto_promotion") is not False:
        issues.append("auto_promotion must be false")
    if safety.get("production_write_allowed") is not False:
        issues.append("production_write_allowed must be false")
    if safety.get("fake_price_rows_generated") is not False:
        issues.append("fake_price_rows_generated must be false")
    if safety.get("synthetic_latest_bar_generated") is not False:
        issues.append("synthetic_latest_bar_generated must be false")
    if safety.get("data_quality_gate_lowered") is not False:
        issues.append("data_quality_gate_lowered must be false")
    if safety.get("production_effect") != "none":
        issues.append("safety production_effect must be none")
    if safety.get("manual_review_required") is not True:
        issues.append("safety manual_review_required must be true")
    if safety.get("auto_promotion") is not False:
        issues.append("safety auto_promotion must be false")
    after_status = str(_mapping(payload.get("after")).get("freshness_status") or "")
    if status == REFRESH_OK and after_status and after_status != FRESHNESS_OK:
        issues.append("refresh status OK requires after freshness OK")
    return issues


def market_data_refresh_payload_date(payload: dict[str, Any], source_path: Path) -> date:
    metadata = _mapping(payload.get("metadata"))
    run_id = str(metadata.get("run_id") or "")
    if run_id.startswith("market-data-refresh-"):
        parsed = _parse_date(run_id.removeprefix("market-data-refresh-"))
        if parsed is not None:
            return parsed
    actions = _mapping(payload.get("actions"))
    parsed = _parse_date(actions.get("target_date"))
    if parsed is not None:
        return parsed
    parsed = _parse_date(source_path.parent.name)
    if parsed is not None:
        return parsed
    raise ValueError(f"cannot infer market data refresh date from {source_path}")


def render_market_data_refresh_markdown(payload: dict[str, Any]) -> str:
    metadata = _mapping(payload.get("metadata"))
    before = _mapping(payload.get("before"))
    actions = _mapping(payload.get("actions"))
    after = _mapping(payload.get("after"))
    plan = _mapping(payload.get("refresh_plan"))
    safety = _mapping(payload.get("safety"))
    asset_results = _records(payload.get("asset_results"))
    lines = [
        "# Market Data Refresh Summary",
        "",
        "## 1. Executive Summary",
        "",
        f"- refresh_status: `{metadata.get('status', 'UNKNOWN')}`",
        f"- production_effect: `{metadata.get('production_effect', 'none')}`",
        f"- manual_review_required: `{metadata.get('manual_review_required', True)}`",
        f"- auto_promotion: `{metadata.get('auto_promotion', False)}`",
        f"- reason: {metadata.get('reason', '')}",
        "",
        "## 2. Before State",
        "",
    ]
    for key in (
        "freshness_status",
        "tracking_date",
        "effective_data_date",
        "tracking_readiness",
    ):
        lines.append(f"- `{key}`: `{before.get(key, '')}`")
    lines.extend(["", "## 3. Refresh Plan", ""])
    for action in _records(plan.get("refresh_actions")):
        lines.append(
            "- "
            f"action=`{action.get('action', '')}`；"
            f"target_date=`{action.get('target_date', '')}`；"
            f"required=`{action.get('required', False)}`"
        )
    if not _records(plan.get("refresh_actions")):
        lines.append("- 当前无需 refresh action。")
    lines.extend(
        [
            "",
            "## 4. Assets Refreshed",
            "",
            "| Asset | Source Symbol | Status | Source | Rows | Error |",
            "|---|---|---|---|---:|---|",
        ]
    )
    for item in asset_results:
        lines.append(
            "| "
            f"`{item.get('symbol', '')}` | "
            f"`{item.get('source_symbol', '')}` | "
            f"`{item.get('status', '')}` | "
            f"`{item.get('source', '')}` | "
            f"{item.get('rows_written', 0)} | "
            f"{_escape_table(str(item.get('error', '')))} |"
        )
    lines.extend(["", "## 5. Source and Symbol Mapping", ""])
    for symbol, item in _mapping(plan.get("symbol_mapping")).items():
        record = _mapping(item)
        lines.append(f"- `{symbol}` -> `{record.get('source_symbol', symbol)}`")
    lines.extend(["", "## 6. Registry Update", ""])
    artifacts = _mapping(payload.get("supporting_artifacts"))
    lines.append(
        "- updated_price_cache_registry: " f"`{actions.get('updated_price_cache_registry', False)}`"
    )
    lines.append(f"- registry: `{artifacts.get('price_cache_registry', '')}`")
    lines.extend(["", "## 7. Manifest Refresh", ""])
    lines.append(
        "- refreshed_backtest_manifest: " f"`{actions.get('refreshed_backtest_manifest', False)}`"
    )
    lines.append(f"- manifest: `{artifacts.get('backtest_input_manifest', '')}`")
    lines.extend(["", "## 8. Freshness Recovery", ""])
    for key in ("freshness_status", "effective_data_date", "tracking_readiness"):
        lines.append(f"- `{key}`: `{after.get(key, '')}`")
    lines.extend(["", "## 9. Candidate Tracking Recovery", ""])
    lines.append(f"- candidate_tracking_status: `{after.get('candidate_tracking_status', '')}`")
    lines.extend(["", "## 10. Remaining Limitations", ""])
    limitations = _strings(payload.get("remaining_limitations"))
    if limitations:
        for limitation in limitations:
            lines.append(f"- {limitation}")
    else:
        lines.append("- 当前无新增 refresh limitation；promotion 仍由独立 gate 控制。")
    lines.extend(["", "## 11. Manual Review Checklist", ""])
    lines.append("- 确认 target date 日线来自 audited raw cache 或显式 provider。")
    lines.append("- 确认 registry、manifest、freshness 和 tracking summary 均已刷新。")
    lines.append("- source delayed 时不要手工补造 target date 价格。")
    lines.append("- 不修改 production 参数，不解除 candidate promotion 禁止。")
    lines.extend(["", "## 12. Safety", ""])
    for key, value in safety.items():
        lines.append(f"- `{key}`: `{value}`")
    return "\n".join(lines).rstrip() + "\n"


def _execute_market_data_refresh(
    *,
    plan: dict[str, Any],
    config: dict[str, Any],
    config_path: Path,
    generated_at: datetime,
    price_providers: dict[str, PriceDataProvider] | None,
) -> dict[str, Any]:
    plan_status = str(_mapping(plan.get("metadata")).get("status") or "UNKNOWN")
    target_date = _target_date_from_plan(plan, generated_at.date())
    required_assets = tuple(_strings(plan.get("required_assets")))
    refresh_targets = tuple(_strings(plan.get("refresh_targets")))
    before = _before_state(plan)
    prices_path = _prices_path(config)
    price_cache_before_sha = _sha256_file(prices_path)
    if plan_status != REFRESH_PLANNED:
        status = (
            plan_status if plan_status in {REFRESH_NOT_NEEDED, REFRESH_BLOCKED} else REFRESH_BLOCKED
        )
        payload = _summary_payload(
            status=status,
            reason=_not_planned_reason(plan_status),
            generated_at=generated_at,
            config_path=config_path,
            plan=plan,
            before=before,
            asset_results=[],
            actions={},
            after={
                "freshness_status": before.get("freshness_status", ""),
                "effective_data_date": before.get("effective_data_date", ""),
                "tracking_readiness": before.get("tracking_readiness", ""),
                "candidate_tracking_status": _latest_candidate_tracking_status(target_date),
            },
            remaining_limitations=[_not_planned_reason(plan_status)],
            supporting_artifacts=_supporting_artifacts(config, plan=plan),
            price_cache_before_sha=price_cache_before_sha,
            price_cache_after_sha=price_cache_before_sha,
        )
        return payload

    asset_results: list[dict[str, Any]] = []
    fetched_frames: list[pd.DataFrame] = []
    source_records: dict[str, dict[str, Any]] = {}
    prices_before = _read_price_cache(prices_path)
    required_start = _required_history_start_date(
        config=config,
        prices=prices_before,
        required_assets=required_assets,
        target_date=target_date,
    )
    expected_history_dates = _expected_history_dates(
        prices_before,
        required_assets=required_assets,
        start_date=required_start,
        target_date=target_date,
    )
    for symbol in refresh_targets:
        source_symbol = source_symbol_for_price_repair(symbol)
        if _asset_has_required_history(
            prices_before,
            symbol,
            source_symbol,
            start_date=required_start,
            target_date=target_date,
            expected_dates=expected_history_dates,
        ):
            result = _already_current_result(
                prices_before,
                symbol=symbol,
                source_symbol=source_symbol,
                target_date=target_date,
            )
            asset_results.append(result)
            source_records[symbol] = result
            continue
        result, frame = _fetch_required_price_history(
            symbol=symbol,
            source_symbol=source_symbol,
            prices_before=prices_before,
            required_assets=required_assets,
            start_date=required_start,
            target_date=target_date,
            expected_dates=expected_history_dates,
            config=config,
            generated_at=generated_at,
            price_providers=price_providers,
        )
        asset_results.append(result)
        if frame is not None and not frame.empty:
            fetched_frames.append(frame)
            source_records[symbol] = result

    updated_price_cache = False
    updated_registry = False
    manifest_run = None
    freshness_after: dict[str, Any] = {}
    tracking_payload: dict[str, Any] = {}
    tracking_error = ""
    write_error = ""
    try:
        if fetched_frames:
            repaired = pd.concat(fetched_frames, ignore_index=True)
            merged = upsert_price_history_cache(prices_path, repaired)
            _write_price_cache(prices_path, merged)
            updated_price_cache = True
        prices_after_write = _read_price_cache(prices_path)
        all_ready = _all_required_assets_have_required_history(
            prices_after_write,
            required_assets,
            start_date=required_start,
            target_date=target_date,
            expected_dates=expected_history_dates,
        )
        should_update_registry = bool(fetched_frames) or all_ready
        if should_update_registry and bool(
            _mapping(config.get("refresh")).get("require_registry_update", True)
        ):
            _write_price_cache_registry(
                path=_price_cache_registry_path(config),
                generated_at=generated_at,
                prices_path=prices_path,
                required_assets=required_assets,
                prices=prices_after_write,
                source_records=source_records,
                target_date=target_date,
            )
            updated_registry = True
        require_manifest = bool(
            _mapping(config.get("refresh")).get("require_manifest_refresh", True)
        )
        if all_ready and require_manifest:
            manifest_run = refresh_backtest_manifest(
                as_of=target_date,
                config_path=_shadow_backtest_config_path(config),
                output_root=PROJECT_ROOT / "artifacts",
                dry_run=False,
                generated_at=generated_at,
            )
        freshness_after = run_market_data_freshness(
            as_of=target_date,
            config_path=_market_data_freshness_config_path(config),
            dry_run=False,
            generated_at=generated_at,
        ).payload
        try:
            tracking_payload = run_portfolio_candidate_tracking(
                as_of=target_date,
                config_path=_portfolio_candidate_tracking_config_path(config),
                dry_run=False,
                generated_at=generated_at,
            ).payload
        except Exception as exc:  # pragma: no cover - surfaced in summary for real runs
            tracking_error = str(exc)
    except Exception as exc:  # pragma: no cover - defensive fail-closed path
        write_error = str(exc)

    price_cache_after_sha = _sha256_file(prices_path)
    prices_after = _read_price_cache(prices_path)
    status = _refresh_status(
        asset_results=asset_results,
        required_assets=required_assets,
        prices=prices_after,
        target_date=target_date,
        write_error=write_error,
        config=config,
        freshness_after=freshness_after,
    )
    reason = _refresh_reason(
        status=status,
        asset_results=asset_results,
        write_error=write_error,
        freshness_after=freshness_after,
    )
    actions = {
        "fetched_assets": [
            str(item.get("symbol")) for item in asset_results if item.get("status") == "FETCHED"
        ],
        "required_start_date": required_start.isoformat(),
        "target_date": target_date.isoformat(),
        "source": _combined_source(asset_results),
        "updated_price_cache": updated_price_cache,
        "updated_price_cache_registry": updated_registry,
        "refreshed_backtest_manifest": manifest_run is not None,
        "reran_freshness": bool(freshness_after),
        "reran_candidate_tracking": bool(tracking_payload),
    }
    supporting = _supporting_artifacts(
        config,
        plan=plan,
        manifest_payload=None if manifest_run is None else manifest_run.payload,
        freshness_payload=freshness_after,
        tracking_payload=tracking_payload,
    )
    remaining_limitations = _remaining_limitations(
        status=status,
        asset_results=asset_results,
        freshness_after=freshness_after,
        tracking_error=tracking_error,
        write_error=write_error,
    )
    return _summary_payload(
        status=status,
        reason=reason,
        generated_at=generated_at,
        config_path=config_path,
        plan=plan,
        before=before,
        asset_results=asset_results,
        actions=actions,
        after=_after_from_freshness(freshness_after, tracking_payload=tracking_payload),
        remaining_limitations=remaining_limitations,
        supporting_artifacts=supporting,
        price_cache_before_sha=price_cache_before_sha,
        price_cache_after_sha=price_cache_after_sha,
    )


def _fetch_required_price_history(
    *,
    symbol: str,
    source_symbol: str,
    prices_before: pd.DataFrame,
    required_assets: tuple[str, ...],
    start_date: date,
    target_date: date,
    expected_dates: list[date],
    config: dict[str, Any],
    generated_at: datetime,
    price_providers: dict[str, PriceDataProvider] | None,
) -> tuple[dict[str, Any], pd.DataFrame | None]:
    attempts: list[dict[str, Any]] = []
    for source in _source_order(config):
        if source == "audited_fmp_raw_cache":
            if not bool(_mapping(config.get("refresh")).get("allow_audited_raw_cache", True)):
                attempts.append({"source": source, "status": "SKIPPED", "reason": "disabled"})
                continue
            frame, artifact = _fetch_from_audited_fmp_raw_cache(
                canonical_symbol=symbol,
                source_symbol=source_symbol,
                start_date=start_date,
                target_date=target_date,
                config=config,
                generated_at=generated_at,
            )
            if _fetched_history_completes_asset(
                prices_before,
                frame,
                required_assets=required_assets,
                canonical_symbol=symbol,
                source_symbol=source_symbol,
                start_date=start_date,
                target_date=target_date,
                expected_dates=expected_dates,
            ):
                result = _asset_result(
                    symbol=symbol,
                    source_symbol=source_symbol,
                    status="FETCHED",
                    source=source,
                    rows_written=len(frame),
                    attempts=attempts,
                    source_artifacts=artifact,
                )
                return result, frame
            if not frame.empty:
                attempts.append(
                    {
                        "source": source,
                        "status": REFRESH_SOURCE_DELAYED,
                        "reason": "audited cache history does not cover required range",
                        "rows_available": len(frame),
                        "source_artifacts": artifact,
                    }
                )
                continue
            attempts.append(
                {
                    "source": source,
                    "status": REFRESH_SOURCE_DELAYED,
                    "reason": "audited cache has no required history rows",
                }
            )
            continue
        if source in {"fmp", "yahoo"}:
            if not bool(_mapping(config.get("refresh")).get("allow_external_fetch", True)):
                attempts.append({"source": source, "status": "SKIPPED", "reason": "disabled"})
                continue
            frame, attempt = _fetch_from_provider(
                provider_name=source,
                symbol=symbol,
                source_symbol=source_symbol,
                start_date=start_date,
                target_date=target_date,
                config=config,
                generated_at=generated_at,
                price_provider=None if price_providers is None else price_providers.get(source),
            )
            attempts.append(attempt)
            if frame is not None and _fetched_history_completes_asset(
                prices_before,
                frame,
                required_assets=required_assets,
                canonical_symbol=symbol,
                source_symbol=source_symbol,
                start_date=start_date,
                target_date=target_date,
                expected_dates=expected_dates,
            ):
                result = _asset_result(
                    symbol=symbol,
                    source_symbol=source_symbol,
                    status="FETCHED",
                    source=source,
                    rows_written=len(frame),
                    attempts=attempts,
                    source_artifacts=_records(attempt.get("source_artifacts")),
                )
                return result, frame
            if frame is not None and not frame.empty:
                attempts[-1] = {
                    **attempt,
                    "status": REFRESH_SOURCE_DELAYED,
                    "reason": "provider history does not cover required range",
                    "rows_available": len(frame),
                }
    terminal_status = _terminal_asset_status(attempts)
    return (
        _asset_result(
            symbol=symbol,
            source_symbol=source_symbol,
            status=terminal_status,
            source="",
            rows_written=0,
            attempts=attempts,
            error=_attempt_error_summary(attempts),
        ),
        None,
    )


def _fetch_from_audited_fmp_raw_cache(
    *,
    canonical_symbol: str,
    source_symbol: str,
    start_date: date,
    target_date: date,
    config: dict[str, Any],
    generated_at: datetime,
) -> tuple[pd.DataFrame, list[dict[str, Any]]]:
    root = (
        _external_request_cache_dir(config)
        / _FMP_PROVIDER.replace(" ", "_")
        / _FMP_EOD_CACHE_FAMILY
    )
    frames: list[pd.DataFrame] = []
    artifacts: list[dict[str, Any]] = []
    if not root.exists():
        return pd.DataFrame(columns=PRICE_CACHE_OUTPUT_COLUMNS), []
    for metadata_path in root.rglob("metadata.json"):
        metadata = _read_json(metadata_path)
        params = _mapping(_mapping(metadata.get("request_identity")).get("params"))
        if str(params.get("symbol") or "") != source_symbol:
            continue
        if int(metadata.get("status_code") or 0) != 200:
            continue
        body_path = _body_path(metadata, metadata_path)
        if not body_path.exists():
            continue
        payload = _read_json(body_path)
        if not isinstance(payload, list):
            continue
        frame = _normalize_audited_fmp_rows(
            pd.DataFrame(payload),
            canonical_symbol=canonical_symbol,
            source_symbol=source_symbol,
            start_date=start_date,
            target_date=target_date,
            updated_at=str(metadata.get("created_at") or generated_at.isoformat()),
        )
        if frame.empty:
            continue
        created = _parse_datetime(metadata.get("created_at")) or datetime.min.replace(tzinfo=UTC)
        body_path = _body_path(metadata, metadata_path)
        frame["_source_created"] = created.isoformat()
        frame["_source_metadata_path"] = str(metadata_path)
        frames.append(frame)
        date_range = _frame_date_range(frame)
        artifacts.append(
            {
                "provider": _FMP_PROVIDER,
                "endpoint": str(metadata.get("endpoint") or metadata.get("url") or ""),
                "metadata_path": str(metadata_path),
                "body_path": str(body_path),
                "body_sha256": str(metadata.get("body_sha256") or _sha256_file(body_path)),
                "row_count": len(frame),
                "date_range": date_range,
                "latest_date": date_range.get("end", ""),
            }
        )
    if not frames:
        return pd.DataFrame(columns=PRICE_CACHE_OUTPUT_COLUMNS), []
    combined = pd.concat(frames, ignore_index=True)
    combined = (
        combined.sort_values(["date", "_source_created", "_source_metadata_path"])
        .drop_duplicates(subset=["date", "ticker"], keep="last")
        .drop(columns=["_source_created", "_source_metadata_path"])
        .reset_index(drop=True)
    )
    artifacts.sort(
        key=lambda item: (
            str(_mapping(item.get("date_range")).get("start") or ""),
            str(_mapping(item.get("date_range")).get("end") or ""),
            str(item.get("metadata_path") or ""),
        )
    )
    return combined, artifacts


def _fetch_from_provider(
    *,
    provider_name: str,
    symbol: str,
    source_symbol: str,
    start_date: date,
    target_date: date,
    config: dict[str, Any],
    generated_at: datetime,
    price_provider: PriceDataProvider | None,
) -> tuple[pd.DataFrame | None, dict[str, Any]]:
    try:
        provider = price_provider or _build_provider(provider_name, config)
    except ValueError as exc:
        return (
            None,
            {
                "source": provider_name,
                "status": REFRESH_BLOCKED,
                "error": _sanitize_provider_error(str(exc), config),
            },
        )
    try:
        downloaded = provider.download_prices(
            PriceRequest(
                tickers=[source_symbol],
                start=start_date,
                end=target_date,
                interval="1d",
            )
        )
        normalized = normalize_repaired_price_history(
            downloaded,
            canonical_symbol=symbol,
            source_symbol=source_symbol,
            source=_provider_label(provider_name),
            updated_at=generated_at.isoformat(),
            start=start_date,
            end=target_date,
        )
        if normalized.empty:
            return (
                None,
                {
                    "source": provider_name,
                    "status": REFRESH_SOURCE_DELAYED,
                    "rows_downloaded": len(downloaded),
                    "error": "provider returned no valid rows for required date range",
                },
            )
        return (
            normalized,
            {
                "source": provider_name,
                "status": REFRESH_OK,
                "rows_downloaded": len(downloaded),
                "rows_written": len(normalized),
                "source_artifacts": [],
            },
        )
    except Exception as exc:
        return (
            None,
            {
                "source": provider_name,
                "status": REFRESH_FAILED,
                "error": _sanitize_provider_error(str(exc), config),
            },
        )


def _summary_payload(
    *,
    status: str,
    reason: str,
    generated_at: datetime,
    config_path: Path,
    plan: dict[str, Any],
    before: dict[str, Any],
    asset_results: list[dict[str, Any]],
    actions: dict[str, Any],
    after: dict[str, Any],
    remaining_limitations: list[str],
    supporting_artifacts: dict[str, str],
    price_cache_before_sha: str,
    price_cache_after_sha: str,
) -> dict[str, Any]:
    target_date = _target_date_from_plan(plan, generated_at.date())
    return {
        "schema_version": MARKET_DATA_REFRESH_SCHEMA_VERSION,
        "report_type": MARKET_DATA_REFRESH_REPORT_TYPE,
        "metadata": {
            "run_id": f"market-data-refresh-{target_date.isoformat()}",
            "generated_at": generated_at.isoformat(),
            "status": status,
            "reason": reason,
            "production_effect": "none",
            "manual_review_required": True,
            "auto_promotion": False,
            "observe_only": True,
            "config_path": str(resolve_project_path(str(config_path))),
            "git_commit": git_commit_sha(),
            "git_worktree_dirty": git_worktree_dirty(),
        },
        "before": before,
        "refresh_plan": plan,
        "asset_results": asset_results,
        "actions": {
            "fetched_assets": [],
            "target_date": target_date.isoformat(),
            "source": "",
            "updated_price_cache": False,
            "updated_price_cache_registry": False,
            "refreshed_backtest_manifest": False,
            "reran_freshness": False,
            "reran_candidate_tracking": False,
            **actions,
        },
        "after": after,
        "remaining_limitations": remaining_limitations,
        "promotion_impact": {
            "can_support_candidate_promotion": False,
            "reason": (
                "Market data refresh is a recovery workflow only. Production promotion remains "
                "disabled until signal quality, tracking review, and promotion gates pass."
            ),
        },
        "supporting_artifacts": supporting_artifacts,
        "diagnostics": {
            "price_cache_before_sha256": price_cache_before_sha,
            "price_cache_after_sha256": price_cache_after_sha,
            "price_cache_changed": price_cache_before_sha != price_cache_after_sha,
        },
        "safety": {
            "production_effect": "none",
            "manual_review_required": True,
            "auto_promotion": False,
            "production_write_allowed": False,
            "fake_price_rows_generated": False,
            "synthetic_latest_bar_generated": False,
            "data_quality_gate_lowered": False,
            "production_parameters_modified": False,
        },
    }


def _write_price_cache(path: Path, frame: pd.DataFrame) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(path, index=False)
    return path


def _write_price_cache_registry(
    *,
    path: Path,
    generated_at: datetime,
    prices_path: Path,
    required_assets: tuple[str, ...],
    prices: pd.DataFrame,
    source_records: dict[str, dict[str, Any]],
    target_date: date,
) -> Path:
    assets: dict[str, Any] = {}
    for symbol in required_assets:
        source_symbol = source_symbol_for_price_repair(symbol)
        rows = _asset_rows(prices, symbol, source_symbol)
        date_range = _frame_date_range(rows)
        latest_date = date_range.get("end", "")
        source_record = _mapping(source_records.get(symbol))
        status = "REGISTERED" if not rows.empty else "MISSING"
        if latest_date and _parse_date(latest_date) is not None:
            if _parse_date(latest_date) < target_date:
                status = "STALE"
        assets[symbol] = {
            "canonical_symbol": symbol,
            "source_symbol": source_symbol,
            "status": status,
            "rows": len(rows),
            "latest_date": latest_date,
            "date_range": date_range,
            "source": source_record.get("source") or _latest_source(rows),
            "schema_status": _schema_status(rows),
            "source_artifacts": _records(source_record.get("source_artifacts")),
            "registered_at": generated_at.isoformat(),
        }
    registry_status = (
        "OK" if all(item["status"] == "REGISTERED" for item in assets.values()) else "LIMITED"
    )
    payload = {
        "schema_version": 1,
        "report_type": "price_cache_registry",
        "metadata": {
            "generated_at": generated_at.isoformat(),
            "status": registry_status,
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
            "synthetic_latest_bar_generated": False,
            "data_quality_gate_lowered": False,
        },
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def _normalize_audited_fmp_rows(
    raw: pd.DataFrame,
    *,
    canonical_symbol: str,
    source_symbol: str,
    start_date: date,
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
        & (frame["date"].dt.date >= start_date)
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
    frame["source"] = "market_data_refresh:audited_fmp_raw_cache"
    frame["updated_at"] = updated_at
    frame["source_symbol"] = source_symbol
    frame["canonical_symbol"] = canonical_symbol
    return (
        frame[list(PRICE_CACHE_OUTPUT_COLUMNS)]
        .sort_values("date")
        .drop_duplicates(subset=["date", "ticker"], keep="last")
        .reset_index(drop=True)
    )


def _refresh_actions(
    *,
    target_date: date,
    refresh_targets: tuple[str, ...],
    config: dict[str, Any],
    enabled: bool,
) -> list[dict[str, Any]]:
    if not enabled:
        return []
    return [
        {
            "action": "fetch_required_price_history",
            "target_date": target_date.isoformat(),
            "symbols": list(refresh_targets),
            "source_order": list(_source_order(config)),
            "required": True,
        },
        {"action": "validate_price_schema", "required": True},
        {"action": "update_price_cache_registry", "required": True},
        {
            "action": "refresh_backtest_input_manifest",
            "target_date": target_date.isoformat(),
            "required": True,
        },
        {"action": "rerun_market_data_freshness", "required": True},
        {"action": "rerun_portfolio_candidate_tracking", "required": True},
    ]


def _asset_result(
    *,
    symbol: str,
    source_symbol: str,
    status: str,
    source: str,
    rows_written: int,
    attempts: list[dict[str, Any]],
    source_artifacts: list[dict[str, Any]] | None = None,
    error: str = "",
) -> dict[str, Any]:
    return {
        "symbol": symbol,
        "source_symbol": source_symbol,
        "status": status,
        "source": source,
        "rows_written": rows_written,
        "attempts": attempts,
        "source_artifacts": source_artifacts or [],
        "error": error,
    }


def _already_current_result(
    prices: pd.DataFrame,
    *,
    symbol: str,
    source_symbol: str,
    target_date: date,
) -> dict[str, Any]:
    rows = _asset_rows(prices, symbol, source_symbol)
    latest_row = rows.loc[rows["_parsed_date"] == target_date].tail(1)
    source = _latest_source(latest_row)
    return _asset_result(
        symbol=symbol,
        source_symbol=source_symbol,
        status="ALREADY_CURRENT",
        source=source,
        rows_written=0,
        attempts=[],
    )


def _refresh_status(
    *,
    asset_results: list[dict[str, Any]],
    required_assets: tuple[str, ...],
    prices: pd.DataFrame,
    target_date: date,
    write_error: str,
    config: dict[str, Any],
    freshness_after: dict[str, Any],
) -> str:
    if write_error:
        return REFRESH_FAILED
    if not asset_results:
        return REFRESH_BLOCKED
    statuses = {str(item.get("status") or "") for item in asset_results}
    if REFRESH_BLOCKED in statuses:
        return REFRESH_BLOCKED
    after_status = str(_mapping(freshness_after.get("freshness")).get("status") or "")
    if after_status and after_status != FRESHNESS_OK:
        if REFRESH_SOURCE_DELAYED in statuses:
            return REFRESH_SOURCE_DELAYED
        if REFRESH_FAILED in statuses:
            return REFRESH_FAILED
        return REFRESH_FAILED
    if _all_required_assets_have_target_date(prices, required_assets, target_date):
        return REFRESH_OK
    has_current_or_fetched = any(status in {"FETCHED", "ALREADY_CURRENT"} for status in statuses)
    if REFRESH_FAILED in statuses and not has_current_or_fetched:
        return REFRESH_FAILED
    if bool(_mapping(config.get("refresh")).get("allow_partial_refresh", False)):
        if any(status in {"FETCHED", "ALREADY_CURRENT"} for status in statuses):
            return REFRESH_PARTIAL
    return REFRESH_SOURCE_DELAYED


def _refresh_reason(
    *,
    status: str,
    asset_results: list[dict[str, Any]],
    write_error: str,
    freshness_after: dict[str, Any],
) -> str:
    if write_error:
        return f"Market data refresh failed while writing cache or artifacts: {write_error}"
    if status == REFRESH_OK:
        return "Market data freshness recovered for required assets."
    if status == REFRESH_NOT_NEEDED:
        return "Market data freshness does not require refresh."
    if status == REFRESH_BLOCKED:
        return (
            "Market data refresh is blocked by missing input, disabled config, "
            "or provider access."
        )
    if status == REFRESH_PARTIAL:
        return "Only part of the required assets refreshed; tracking remains gated by freshness."
    delayed = [
        str(item.get("symbol"))
        for item in asset_results
        if item.get("status") == REFRESH_SOURCE_DELAYED
    ]
    if delayed:
        return (
            "Market data source has not provided target date rows for " + ", ".join(delayed) + "."
        )
    after_status = str(_mapping(freshness_after.get("freshness")).get("status") or "")
    if after_status and after_status != FRESHNESS_OK:
        return f"Refresh ran, but freshness remains {after_status}."
    return "Market data refresh did not recover all required target date rows."


def _remaining_limitations(
    *,
    status: str,
    asset_results: list[dict[str, Any]],
    freshness_after: dict[str, Any],
    tracking_error: str,
    write_error: str,
) -> list[str]:
    limitations: list[str] = []
    if write_error:
        limitations.append(f"refresh artifact/cache update failed: {write_error}")
    delayed = [
        str(item.get("symbol"))
        for item in asset_results
        if item.get("status") == REFRESH_SOURCE_DELAYED
    ]
    if delayed:
        limitations.append(
            "market data source has not provided target date rows for: " + ", ".join(delayed)
        )
    if tracking_error:
        limitations.append(f"candidate tracking rerun failed: {tracking_error}")
    freshness = _mapping(freshness_after.get("freshness"))
    after_status = str(freshness.get("status") or "")
    if after_status and after_status != FRESHNESS_OK:
        limitations.append(
            f"market data freshness remains {after_status}: {freshness.get('reason', '')}"
        )
    if status == REFRESH_OK:
        limitations.append(
            "signal_snapshot_status remains LIMITED if upstream signal quality is limited."
        )
    limitations.append("candidate promotion remains disabled.")
    return list(dict.fromkeys(item for item in limitations if item))


def _before_state(plan: dict[str, Any]) -> dict[str, Any]:
    freshness_input = _mapping(plan.get("freshness_input"))
    return {
        "freshness_status": freshness_input.get("freshness_status", "UNKNOWN"),
        "tracking_date": freshness_input.get("tracking_date", ""),
        "effective_data_date": freshness_input.get("effective_data_date", ""),
        "tracking_readiness": freshness_input.get("tracking_readiness", "unknown"),
    }


def _after_from_freshness(
    freshness_payload: dict[str, Any],
    *,
    tracking_payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    freshness = _mapping(freshness_payload.get("freshness"))
    dates = _mapping(freshness_payload.get("data_dates"))
    readiness = _mapping(freshness_payload.get("tracking_readiness"))
    candidate = _mapping((tracking_payload or {}).get("candidate"))
    return {
        "freshness_status": freshness.get("status", ""),
        "effective_data_date": dates.get("effective_data_date", ""),
        "tracking_readiness": readiness.get("readiness", ""),
        "candidate_tracking_status": candidate.get("tracking_status", ""),
    }


def _latest_candidate_tracking_status(as_of: date) -> str:
    path = latest_portfolio_candidate_tracking_path_on_or_before(as_of)
    if path is None:
        return ""
    payload = load_portfolio_candidate_tracking_payload(path)
    return str(_mapping(payload.get("candidate")).get("tracking_status") or "")


def _supporting_artifacts(
    config: dict[str, Any],
    *,
    plan: dict[str, Any],
    manifest_payload: dict[str, Any] | None = None,
    freshness_payload: dict[str, Any] | None = None,
    tracking_payload: dict[str, Any] | None = None,
) -> dict[str, str]:
    plan_input = _mapping(plan.get("freshness_input"))
    artifacts = {
        "market_data_freshness_before": str(plan_input.get("freshness_report") or ""),
        "prices": str(_prices_path(config)),
        "price_cache_registry": str(_price_cache_registry_path(config)),
    }
    target_date = _parse_date(plan_input.get("required_target_date"))
    if target_date is not None:
        manifest_path = (
            PROJECT_ROOT
            / "artifacts"
            / "backtest_snapshots"
            / target_date.isoformat()
            / "backtest_input_manifest.json"
        )
        if manifest_path.exists():
            artifacts["backtest_input_manifest"] = str(manifest_path)
    manifest_artifacts = _mapping((manifest_payload or {}).get("output_artifacts"))
    if manifest_payload:
        manifest_value = str(
            manifest_artifacts.get("manifest")
            or (manifest_payload or {}).get("would_write_manifest")
            or ""
        )
        if manifest_value:
            artifacts["backtest_input_manifest"] = manifest_value
    freshness_artifacts = _mapping((freshness_payload or {}).get("output_artifacts"))
    if freshness_artifacts.get("summary_json"):
        artifacts["market_data_freshness_after"] = str(freshness_artifacts.get("summary_json"))
    tracking_artifacts = _mapping((tracking_payload or {}).get("output_artifacts"))
    if tracking_artifacts.get("summary_json"):
        artifacts["portfolio_candidate_tracking"] = str(tracking_artifacts.get("summary_json"))
    return artifacts


def _not_planned_reason(status: str) -> str:
    if status == REFRESH_NOT_NEEDED:
        return "Freshness status does not require market data refresh."
    if status == REFRESH_BLOCKED:
        return "Market data refresh could not start because freshness input is missing or disabled."
    return f"Market data refresh plan is not executable: {status}."


def _refresh_targets(
    *,
    requested_symbols: tuple[str, ...],
    missing_expected: tuple[str, ...],
    required_assets: tuple[str, ...],
    freshness_status: str,
) -> tuple[str, ...]:
    requested = _normalize_symbols(requested_symbols)
    if requested:
        return requested
    if freshness_status in _REFRESHABLE_FRESHNESS_STATUSES and missing_expected:
        return _normalize_symbols(missing_expected)
    if freshness_status in _REFRESHABLE_FRESHNESS_STATUSES:
        return required_assets
    return ()


def _terminal_asset_status(attempts: list[dict[str, Any]]) -> str:
    statuses = {str(item.get("status") or "") for item in attempts}
    if REFRESH_SOURCE_DELAYED in statuses:
        return REFRESH_SOURCE_DELAYED
    if REFRESH_BLOCKED in statuses:
        return REFRESH_BLOCKED
    if REFRESH_FAILED in statuses:
        return REFRESH_FAILED
    return REFRESH_SOURCE_DELAYED


def _attempt_error_summary(attempts: list[dict[str, Any]]) -> str:
    parts = []
    for attempt in attempts:
        source = str(attempt.get("source") or "")
        status = str(attempt.get("status") or "")
        reason = str(attempt.get("reason") or attempt.get("error") or "")
        if source or status or reason:
            parts.append(f"{source}:{status}:{reason}".strip(":"))
    return "; ".join(parts)


def _combined_source(asset_results: list[dict[str, Any]]) -> str:
    sources = [
        str(item.get("source") or "")
        for item in asset_results
        if item.get("status") in {"FETCHED", "ALREADY_CURRENT"} and item.get("source")
    ]
    return ", ".join(dict.fromkeys(sources))


def _all_required_assets_have_target_date(
    prices: pd.DataFrame,
    required_assets: tuple[str, ...],
    target_date: date,
) -> bool:
    if prices.empty:
        return False
    for symbol in required_assets:
        source_symbol = source_symbol_for_price_repair(symbol)
        if not _asset_has_date(prices, symbol, source_symbol, target_date):
            return False
    return True


def _read_price_cache(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(columns=PRICE_CACHE_OUTPUT_COLUMNS)
    try:
        frame = pd.read_csv(path)
    except Exception:
        return pd.DataFrame(columns=PRICE_CACHE_OUTPUT_COLUMNS)
    if "ticker" not in frame.columns and "symbol" in frame.columns:
        frame["ticker"] = frame["symbol"]
    if "symbol" not in frame.columns and "ticker" in frame.columns:
        frame["symbol"] = frame["ticker"]
    if "canonical_symbol" not in frame.columns and "ticker" in frame.columns:
        frame["canonical_symbol"] = frame["ticker"]
    if "source_symbol" not in frame.columns and "ticker" in frame.columns:
        frame["source_symbol"] = frame["ticker"]
    if "source" not in frame.columns:
        frame["source"] = "existing_cache"
    if "updated_at" not in frame.columns:
        frame["updated_at"] = ""
    if "date" in frame.columns:
        frame["_parsed_date"] = pd.to_datetime(frame["date"], errors="coerce").dt.date
    return frame


def _asset_rows(prices: pd.DataFrame, canonical_symbol: str, source_symbol: str) -> pd.DataFrame:
    if prices.empty:
        return prices.copy()
    mask = pd.Series(False, index=prices.index)
    for column, value in (
        ("ticker", canonical_symbol),
        ("symbol", canonical_symbol),
        ("canonical_symbol", canonical_symbol),
        ("source_symbol", source_symbol),
        ("ticker", source_symbol),
        ("symbol", source_symbol),
    ):
        if column in prices.columns:
            mask = mask | (prices[column].astype(str) == value)
    return prices.loc[mask].copy()


def _asset_has_date(
    prices: pd.DataFrame,
    canonical_symbol: str,
    source_symbol: str,
    target_date: date,
) -> bool:
    rows = _asset_rows(prices, canonical_symbol, source_symbol)
    return "_parsed_date" in rows.columns and target_date in set(rows["_parsed_date"])


def _asset_has_required_history(
    prices: pd.DataFrame,
    canonical_symbol: str,
    source_symbol: str,
    *,
    start_date: date,
    target_date: date,
    expected_dates: list[date],
) -> bool:
    dates = _valid_asset_history_dates(prices, canonical_symbol, source_symbol)
    if not dates or target_date not in dates or min(dates) > start_date:
        return False
    if expected_dates and not set(expected_dates).issubset(dates):
        return False
    return True


def _fetched_history_completes_asset(
    prices_before: pd.DataFrame,
    fetched: pd.DataFrame,
    *,
    required_assets: tuple[str, ...],
    canonical_symbol: str,
    source_symbol: str,
    start_date: date,
    target_date: date,
    expected_dates: list[date],
) -> bool:
    if fetched is None or fetched.empty:
        return False
    combined = pd.concat([prices_before, fetched], ignore_index=True)
    combined_expected = expected_dates or _expected_history_dates(
        combined,
        required_assets=required_assets,
        start_date=start_date,
        target_date=target_date,
    )
    return _asset_has_required_history(
        combined,
        canonical_symbol,
        source_symbol,
        start_date=start_date,
        target_date=target_date,
        expected_dates=combined_expected,
    )


def _all_required_assets_have_required_history(
    prices: pd.DataFrame,
    required_assets: tuple[str, ...],
    *,
    start_date: date,
    target_date: date,
    expected_dates: list[date],
) -> bool:
    if prices.empty:
        return False
    for symbol in required_assets:
        source_symbol = source_symbol_for_price_repair(symbol)
        if not _asset_has_required_history(
            prices,
            symbol,
            source_symbol,
            start_date=start_date,
            target_date=target_date,
            expected_dates=expected_dates,
        ):
            return False
    return True


def _valid_asset_history_dates(
    prices: pd.DataFrame,
    canonical_symbol: str,
    source_symbol: str,
) -> set[date]:
    rows = _asset_rows(prices, canonical_symbol, source_symbol)
    if rows.empty or "date" not in rows.columns:
        return set()
    parsed = pd.to_datetime(rows["date"], errors="coerce")
    valid = rows.loc[parsed.notna()].copy()
    if valid.empty:
        return set()
    valid["_parsed_history_date"] = parsed.loc[parsed.notna()].dt.date
    if "adj_close" in valid.columns:
        valid["_adj_close"] = pd.to_numeric(valid["adj_close"], errors="coerce")
        valid = valid.loc[valid["_adj_close"].notna()]
    return set(valid["_parsed_history_date"])


def _expected_history_dates(
    prices: pd.DataFrame,
    *,
    required_assets: tuple[str, ...],
    start_date: date,
    target_date: date,
) -> list[date]:
    dates = _observed_history_dates(
        prices,
        required_assets=required_assets,
        start_date=start_date,
        target_date=target_date,
    )
    if dates:
        return dates
    return [
        start_date + timedelta(days=offset) for offset in range((target_date - start_date).days + 1)
    ]


def _observed_history_dates(
    prices: pd.DataFrame,
    *,
    required_assets: tuple[str, ...],
    start_date: date | None,
    target_date: date,
) -> list[date]:
    dates: set[date] = set()
    for symbol in required_assets:
        source_symbol = source_symbol_for_price_repair(symbol)
        for current in _valid_asset_history_dates(prices, symbol, source_symbol):
            if (start_date is None or current >= start_date) and current <= target_date:
                dates.add(current)
    return sorted(dates)


def _required_history_start_date(
    *,
    config: dict[str, Any],
    prices: pd.DataFrame,
    required_assets: tuple[str, ...],
    target_date: date,
) -> date:
    min_history_days = _shadow_min_history_days(config)
    observed_dates = _observed_history_dates(
        prices,
        required_assets=required_assets,
        start_date=None,
        target_date=target_date,
    )
    if len(observed_dates) >= min_history_days:
        return observed_dates[-min_history_days]
    if observed_dates:
        return observed_dates[-1] - timedelta(days=min_history_days - 1)
    return target_date - timedelta(days=min_history_days)


def _shadow_min_history_days(config: dict[str, Any]) -> int:
    shadow_config = load_shadow_backtest_config(_shadow_backtest_config_path(config))
    return int(shadow_config.walk_forward.min_history_days)


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


def _schema_status(frame: pd.DataFrame) -> str:
    if frame.empty:
        return "MISSING"
    required = {"date", "ticker", "open", "high", "low", "close", "adj_close", "volume"}
    missing = required - set(frame.columns)
    if missing:
        return "MISSING_COLUMNS:" + ",".join(sorted(missing))
    numeric = frame[["open", "high", "low", "close", "adj_close"]].apply(
        pd.to_numeric,
        errors="coerce",
    )
    if numeric.isna().any().any():
        return "INVALID_NUMERIC_VALUES"
    return "OK"


def _latest_source(frame: pd.DataFrame) -> str:
    if frame.empty or "source" not in frame.columns:
        return ""
    values = [str(item) for item in frame["source"].dropna().tolist() if str(item)]
    return values[-1] if values else ""


def _required_assets(config: dict[str, Any]) -> tuple[str, ...]:
    configured = _strings(_mapping(config.get("assets")).get("required"))
    return _normalize_symbols(tuple(configured))


def _source_order(config: dict[str, Any]) -> tuple[str, ...]:
    configured = _strings(_mapping(config.get("sources")).get("preferred_order"))
    return tuple(configured or ["audited_fmp_raw_cache", "fmp", "yahoo"])


def _build_provider(provider_name: str, config: dict[str, Any]) -> PriceDataProvider:
    if provider_name == "fmp":
        api_key_env = str(_mapping(config.get("sources")).get("fmp_api_key_env") or "FMP_API_KEY")
        return build_price_history_repair_provider(
            provider_name="fmp",
            fmp_api_key=os.getenv(api_key_env, ""),
        )
    if provider_name == "yahoo":
        return build_price_history_repair_provider(provider_name="yahoo")
    raise ValueError(f"unsupported refresh provider: {provider_name}")


def _sanitize_provider_error(message: str, config: dict[str, Any]) -> str:
    api_key_env = str(_mapping(config.get("sources")).get("fmp_api_key_env") or "FMP_API_KEY")
    return sanitize_diagnostic_text(message, extra_secrets=(os.getenv(api_key_env, ""),))


def _provider_label(provider_name: str) -> str:
    if provider_name == "fmp":
        return "Financial Modeling Prep"
    if provider_name == "yahoo":
        return "Yahoo Finance via yfinance"
    return provider_name


def _resolve_freshness_path(config: dict[str, Any], *, as_of: date | None) -> Path | None:
    root = _market_data_freshness_root(config)
    if as_of is not None:
        return default_market_data_freshness_json_path(root, as_of)
    return latest_market_data_freshness_path(root)


def _target_date_from_plan(plan: dict[str, Any], default: date) -> date:
    parsed = _parse_date(_mapping(plan.get("freshness_input")).get("required_target_date"))
    return parsed or default


def _prices_path(config: dict[str, Any]) -> Path:
    raw = _input(config, "prices_path")
    return (
        resolve_project_path(str(raw))
        if raw
        else PROJECT_ROOT / "data" / "raw" / "prices_daily.csv"
    )


def _price_cache_registry_path(config: dict[str, Any]) -> Path:
    raw = _input(config, "price_cache_registry_path")
    return (
        resolve_project_path(str(raw))
        if raw
        else PROJECT_ROOT / "artifacts" / "data_registry" / "price_cache_registry.json"
    )


def _market_data_freshness_root(config: dict[str, Any]) -> Path:
    raw = _input(config, "market_data_freshness_dir")
    return resolve_project_path(str(raw)) if raw else default_market_data_freshness_root()


def _market_data_freshness_config_path(config: dict[str, Any]) -> Path:
    raw = _input(config, "market_data_freshness_config_path")
    return resolve_project_path(str(raw)) if raw else DEFAULT_MARKET_DATA_FRESHNESS_CONFIG_PATH


def _shadow_backtest_config_path(config: dict[str, Any]) -> Path:
    raw = _input(config, "shadow_backtest_config_path")
    return resolve_project_path(str(raw)) if raw else DEFAULT_SHADOW_BACKTEST_CONFIG_PATH


def _portfolio_candidate_tracking_config_path(config: dict[str, Any]) -> Path:
    raw = _input(config, "portfolio_candidate_tracking_config_path")
    return (
        resolve_project_path(str(raw)) if raw else DEFAULT_PORTFOLIO_CANDIDATE_TRACKING_CONFIG_PATH
    )


def _external_request_cache_dir(config: dict[str, Any]) -> Path:
    raw = _input(config, "external_request_cache_dir")
    return (
        resolve_project_path(str(raw))
        if raw
        else PROJECT_ROOT / "data" / "raw" / "external_request_cache"
    )


def _output_root(config: dict[str, Any], *, dry_run: bool) -> Path:
    key = "dry_run_dir" if dry_run else "market_data_refresh_dir"
    default = (
        PROJECT_ROOT / "outputs" / "dry_runs" / "data_refresh"
        if dry_run
        else default_market_data_refresh_root()
    )
    raw = _mapping(config.get("output")).get(key)
    return resolve_project_path(str(raw)) if raw else default


def _input(config: dict[str, Any], key: str) -> object:
    return _mapping(config.get("input")).get(key)


def _body_path(metadata: dict[str, Any], metadata_path: Path) -> Path:
    raw = Path(str(metadata.get("body_path") or ""))
    return raw if raw.is_absolute() else metadata_path.parent / raw


def _read_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, UnicodeDecodeError):
        return {}


def _sha256_file(path: Path) -> str:
    if not path.exists() or path.is_dir():
        return ""
    digest = sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _validate_config(payload: dict[str, Any]) -> None:
    if payload.get("production_effect") != "none":
        raise ValueError("market data refresh production_effect must be none")
    if payload.get("manual_review_required") is not True:
        raise ValueError("market data refresh manual_review_required must be true")
    if payload.get("auto_promotion") is not False:
        raise ValueError("market data refresh auto_promotion must be false")
    safety = _mapping(payload.get("safety"))
    if safety.get("production_write_allowed") is not False:
        raise ValueError("market data refresh must not write production parameters")
    if safety.get("forbid_mock_prices") is not True:
        raise ValueError("market data refresh must forbid mock prices")
    if safety.get("forbid_synthetic_latest_bar") is not True:
        raise ValueError("market data refresh must forbid synthetic latest bars")
    if safety.get("data_quality_gate_lowered") is not False:
        raise ValueError("market data refresh must not lower data quality gates")
    if not _strings(_mapping(payload.get("assets")).get("required")):
        raise ValueError("market data refresh requires configured assets")


def _generated_at(value: datetime | None) -> datetime:
    generated = value or datetime.now(tz=UTC)
    if generated.tzinfo is None or generated.utcoffset() is None:
        return generated.replace(tzinfo=UTC)
    return generated


def _parse_date(value: object) -> date | None:
    try:
        text = str(value or "")
        if not text:
            return None
        return date.fromisoformat(text)
    except (TypeError, ValueError):
        return None


def _parse_datetime(value: object) -> datetime | None:
    try:
        parsed = datetime.fromisoformat(str(value))
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed


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


def _mapping(value: object) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _records(value: object) -> list[dict[str, Any]]:
    return [item for item in value if isinstance(item, dict)] if isinstance(value, list) else []


def _strings(value: object) -> list[str]:
    if isinstance(value, list | tuple):
        return [str(item) for item in value if str(item)]
    return []


def _escape_table(value: str) -> str:
    return value.replace("|", "\\|").replace("\n", " ")
