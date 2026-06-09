from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import pandas as pd

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.shadow.lineage import git_commit_sha, git_worktree_dirty
from ai_trading_system.trading_calendar import (
    NYSE_REGULAR_HOLIDAY_CALENDAR_SOURCE,
    previous_us_equity_trading_day,
    us_equity_market_session,
)
from ai_trading_system.trading_engine.backtest_input_diagnostics import (
    DEFAULT_REQUIRED_BACKTEST_ASSETS,
)
from ai_trading_system.trading_engine.data.symbol_resolver import resolve_symbol
from ai_trading_system.trading_engine.data_registry_consistency import (
    latest_valid_backtest_manifest_context,
)
from ai_trading_system.trading_engine.parameters.parameter_loader import (
    DEFAULT_SHADOW_BACKTEST_CONFIG_PATH,
    load_shadow_backtest_config,
    resolve_project_path,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

MARKET_DATA_FRESHNESS_SCHEMA_VERSION = 1
MARKET_DATA_FRESHNESS_REPORT_TYPE = "market_data_freshness"
MARKET_DATA_FRESHNESS_ALIAS_REPORT_TYPE = "market_data_freshness_report"
DEFAULT_MARKET_DATA_FRESHNESS_CONFIG_PATH = (
    PROJECT_ROOT / "config" / "data" / "market_data_freshness.yaml"
)

FRESHNESS_OK = "OK"
FRESHNESS_ACCEPTABLE_LAG = "ACCEPTABLE_LAG"
FRESHNESS_NON_TRADING_DAY = "NON_TRADING_DAY"
FRESHNESS_STALE = "STALE"
FRESHNESS_MISSING = "MISSING"
FRESHNESS_SOURCE_DELAYED = "SOURCE_DELAYED"
FRESHNESS_MARKET_CALENDAR_UNKNOWN = "MARKET_CALENDAR_UNKNOWN"
FRESHNESS_FAILED = "FAILED"

MARKET_DATA_FRESHNESS_STATUSES = {
    FRESHNESS_OK,
    FRESHNESS_ACCEPTABLE_LAG,
    FRESHNESS_NON_TRADING_DAY,
    FRESHNESS_STALE,
    FRESHNESS_MISSING,
    FRESHNESS_SOURCE_DELAYED,
    FRESHNESS_MARKET_CALENDAR_UNKNOWN,
    FRESHNESS_FAILED,
}

TRACKING_STATUS_BY_FRESHNESS = {
    FRESHNESS_OK: "active_tracking",
    FRESHNESS_NON_TRADING_DAY: "active_tracking",
    FRESHNESS_ACCEPTABLE_LAG: "degraded_tracking",
    FRESHNESS_SOURCE_DELAYED: "degraded_tracking",
    FRESHNESS_STALE: "tracking_blocked",
    FRESHNESS_MISSING: "tracking_blocked",
    FRESHNESS_MARKET_CALENDAR_UNKNOWN: "tracking_blocked",
    FRESHNESS_FAILED: "tracking_blocked",
}
TRACKING_ALLOWED_FRESHNESS = {
    FRESHNESS_OK,
    FRESHNESS_NON_TRADING_DAY,
    FRESHNESS_ACCEPTABLE_LAG,
    FRESHNESS_SOURCE_DELAYED,
}


@dataclass(frozen=True)
class MarketDataFreshnessRun:
    as_of: date
    payload: dict[str, Any]
    json_path: Path
    markdown_path: Path


def default_market_data_freshness_root() -> Path:
    return PROJECT_ROOT / "artifacts" / "data_freshness"


def default_market_data_freshness_dir(output_root: Path, as_of: date) -> Path:
    return output_root / as_of.isoformat()


def default_market_data_freshness_json_path(output_root: Path, as_of: date) -> Path:
    return (
        default_market_data_freshness_dir(output_root, as_of) / "market_data_freshness_summary.json"
    )


def default_market_data_freshness_markdown_path(output_root: Path, as_of: date) -> Path:
    return (
        default_market_data_freshness_dir(output_root, as_of) / "market_data_freshness_summary.md"
    )


def market_data_freshness_report_alias_paths(
    reports_dir: Path,
    as_of: date,
) -> tuple[Path, Path]:
    return (
        reports_dir / f"market_data_freshness_{as_of.isoformat()}.json",
        reports_dir / f"market_data_freshness_{as_of.isoformat()}.md",
    )


def latest_market_data_freshness_path(output_root: Path | None = None) -> Path | None:
    root = output_root or default_market_data_freshness_root()
    candidates = sorted(root.glob("*/market_data_freshness_summary.json"))
    if not candidates:
        return None
    return max(candidates, key=lambda path: path.stat().st_mtime)


def latest_market_data_freshness_path_on_or_before(
    as_of: date,
    output_root: Path | None = None,
) -> Path | None:
    root = output_root or default_market_data_freshness_root()
    candidates: list[tuple[date, Path]] = []
    for path in root.glob("*/market_data_freshness_summary.json"):
        parsed = _parse_date(path.parent.name)
        if parsed is not None and parsed <= as_of:
            candidates.append((parsed, path))
    if not candidates:
        return None
    return max(candidates, key=lambda item: (item[1].stat().st_mtime, item[0]))[1]


def load_market_data_freshness_config(
    path: Path | str = DEFAULT_MARKET_DATA_FRESHNESS_CONFIG_PATH,
) -> dict[str, Any]:
    payload = safe_load_yaml_path(Path(path))
    if not isinstance(payload, dict):
        raise ValueError(f"market data freshness config must be a mapping: {path}")
    _validate_config(payload)
    return payload


def run_market_data_freshness(
    *,
    as_of: date | None = None,
    market: str = "US",
    config_path: Path | str = DEFAULT_MARKET_DATA_FRESHNESS_CONFIG_PATH,
    dry_run: bool = False,
    generated_at: datetime | None = None,
) -> MarketDataFreshnessRun:
    config = load_market_data_freshness_config(config_path)
    output_root = _output_root(config, dry_run=dry_run)
    payload = build_market_data_freshness_payload(
        as_of=as_of,
        market=market,
        config=config,
        config_path=Path(config_path),
        output_root=output_root,
        dry_run=dry_run,
        generated_at=generated_at,
    )
    tracking_date = market_data_freshness_payload_date(
        payload,
        default_market_data_freshness_json_path(output_root, datetime.now(tz=UTC).date()),
    )
    json_path = default_market_data_freshness_json_path(output_root, tracking_date)
    markdown_path = default_market_data_freshness_markdown_path(output_root, tracking_date)
    payload = {
        **payload,
        "output_artifacts": {
            "summary_json": str(json_path),
            "summary_markdown": str(markdown_path),
        },
    }
    write_market_data_freshness_summary(payload, json_path, markdown_path)
    return MarketDataFreshnessRun(
        as_of=tracking_date,
        payload=payload,
        json_path=json_path,
        markdown_path=markdown_path,
    )


def build_market_data_freshness_payload(
    *,
    as_of: date | None,
    market: str,
    config: dict[str, Any],
    config_path: Path,
    output_root: Path,
    dry_run: bool,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(tz=UTC)
    if generated.tzinfo is None or generated.utcoffset() is None:
        generated = generated.replace(tzinfo=UTC)
    try:
        timezone_name = str(_mapping(config.get("market")).get("timezone", "America/New_York"))
        market_tz = ZoneInfo(timezone_name)
    except ZoneInfoNotFoundError:
        market_tz = ZoneInfo("UTC")
    resolved_market = market or str(_mapping(config.get("market")).get("id") or "US")
    prices_path = _prices_path(config)
    prices = _read_prices(prices_path)
    raw_latest_price_date = _latest_price_date(prices)
    tracking_date = as_of or raw_latest_price_date or generated.astimezone(market_tz).date()
    shadow_config_path = resolve_project_path(
        str(_input(config, "shadow_backtest_config_path") or DEFAULT_SHADOW_BACKTEST_CONFIG_PATH)
    )
    required_assets = _required_assets(config, shadow_config_path)
    manifest = latest_valid_backtest_manifest_context(
        output_root=_artifact_root(config),
        as_of=tracking_date,
        expected_prices_path=prices_path,
    )
    asset_dates = _asset_latest_dates(prices, required_assets, manifest.symbol_mapping)
    latest_common_date = _latest_common_date(prices, required_assets, manifest.symbol_mapping)
    latest_manifest_date = manifest.manifest_date
    effective_data_date = _effective_data_date(latest_common_date, latest_manifest_date)
    registry_payload = _price_cache_registry_payload(config)
    latest_registry_date = _latest_registry_date(registry_payload, required_assets)
    calendar = _calendar_payload(
        tracking_date=tracking_date,
        market=resolved_market,
        generated_at=generated,
        market_tz=market_tz,
        config=config,
    )
    expected_data_date = _expected_data_date(calendar, tracking_date)
    asset_coverage = _asset_coverage_payload(
        prices=prices,
        required_assets=required_assets,
        manifest_mapping=manifest.symbol_mapping,
        effective_data_date=effective_data_date,
        expected_data_date=expected_data_date,
    )
    lag_calendar_days = _lag_calendar_days(expected_data_date, effective_data_date)
    lag_trading_days = _lag_trading_days(expected_data_date, effective_data_date, resolved_market)
    status, reason = _freshness_status(
        tracking_date=tracking_date,
        effective_data_date=effective_data_date,
        expected_data_date=expected_data_date,
        calendar=calendar,
        asset_coverage=asset_coverage,
        lag_calendar_days=lag_calendar_days,
        lag_trading_days=lag_trading_days,
        generated_at=generated,
        config=config,
    )
    tracking_readiness = _tracking_readiness(status)
    suggested_actions = _suggested_actions(
        status=status,
        calendar=calendar,
        missing_expected_assets=_strings(asset_coverage.get("missing_expected_date_assets")),
    )
    return {
        "schema_version": MARKET_DATA_FRESHNESS_SCHEMA_VERSION,
        "report_type": MARKET_DATA_FRESHNESS_REPORT_TYPE,
        "metadata": {
            "run_id": f"market-data-freshness-{tracking_date.isoformat()}",
            "generated_at": generated.isoformat(),
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
        "calendar": calendar,
        "data_dates": {
            "tracking_date": tracking_date.isoformat(),
            "expected_data_date": (
                "" if expected_data_date is None else expected_data_date.isoformat()
            ),
            "effective_data_date": (
                "" if effective_data_date is None else effective_data_date.isoformat()
            ),
            "latest_raw_price_date": (
                "" if raw_latest_price_date is None else raw_latest_price_date.isoformat()
            ),
            "latest_required_asset_common_date": (
                "" if latest_common_date is None else latest_common_date.isoformat()
            ),
            "latest_registry_date": (
                "" if latest_registry_date is None else latest_registry_date.isoformat()
            ),
            "latest_manifest_date": (
                "" if latest_manifest_date is None else latest_manifest_date.isoformat()
            ),
        },
        "freshness": {
            "status": status,
            "lag_trading_days": lag_trading_days,
            "lag_calendar_days": lag_calendar_days,
            "reason": reason,
        },
        "asset_coverage": asset_coverage,
        "tracking_readiness": tracking_readiness,
        "suggested_actions": suggested_actions,
        "supporting_artifacts": {
            "prices": str(prices_path),
            "backtest_input_manifest": "" if manifest.path is None else str(manifest.path),
            "price_cache_registry": str(_price_cache_registry_path(config)),
            "download_manifest": str(_download_manifest_path(config)),
        },
        "safety": {
            "production_effect": "none",
            "manual_review_required": True,
            "auto_promotion": False,
            "production_write_allowed": False,
            "data_downloaded_by_freshness_check": False,
            "fake_price_rows_generated": False,
            "data_quality_gate_lowered": False,
        },
        "diagnostics": {
            "asset_latest_dates": {
                symbol: "" if value is None else value.isoformat()
                for symbol, value in asset_dates.items()
            },
            "manifest_status": manifest.status,
            "manifest_validation_status": manifest.validation_status,
            "manifest_validation_errors": list(manifest.validation_errors),
        },
    }


def write_market_data_freshness_summary(
    payload: dict[str, Any],
    json_path: Path,
    markdown_path: Path,
) -> tuple[Path, Path]:
    json_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    markdown_path.write_text(render_market_data_freshness_markdown(payload), encoding="utf-8")
    return json_path, markdown_path


def write_market_data_freshness_report_alias(
    payload: dict[str, Any],
    reports_dir: Path,
    as_of: date,
) -> tuple[Path, Path]:
    alias_payload = {
        **payload,
        "report_type": MARKET_DATA_FRESHNESS_ALIAS_REPORT_TYPE,
        "source_report_type": MARKET_DATA_FRESHNESS_REPORT_TYPE,
    }
    json_path, markdown_path = market_data_freshness_report_alias_paths(reports_dir, as_of)
    return write_market_data_freshness_summary(alias_payload, json_path, markdown_path)


def load_market_data_freshness_payload(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def validate_market_data_freshness_payload(payload: dict[str, Any]) -> list[str]:
    issues: list[str] = []
    if payload.get("schema_version") != MARKET_DATA_FRESHNESS_SCHEMA_VERSION:
        issues.append("schema_version mismatch")
    if payload.get("report_type") not in {
        MARKET_DATA_FRESHNESS_REPORT_TYPE,
        MARKET_DATA_FRESHNESS_ALIAS_REPORT_TYPE,
    }:
        issues.append("report_type mismatch")
    metadata = _mapping(payload.get("metadata"))
    freshness = _mapping(payload.get("freshness"))
    tracking = _mapping(payload.get("tracking_readiness"))
    safety = _mapping(payload.get("safety"))
    status = str(freshness.get("status") or metadata.get("status") or "")
    if status not in MARKET_DATA_FRESHNESS_STATUSES:
        issues.append("freshness status is invalid")
    if metadata.get("production_effect") != "none":
        issues.append("production_effect must be none")
    if metadata.get("manual_review_required") is not True:
        issues.append("manual_review_required must be true")
    if metadata.get("auto_promotion") is not False:
        issues.append("auto_promotion must be false")
    if tracking.get("tracking_status_recommendation") not in set(
        TRACKING_STATUS_BY_FRESHNESS.values()
    ):
        issues.append("tracking_status_recommendation is invalid")
    if safety.get("production_write_allowed") is not False:
        issues.append("production_write_allowed must be false")
    if safety.get("data_downloaded_by_freshness_check") is not False:
        issues.append("data_downloaded_by_freshness_check must be false")
    if safety.get("fake_price_rows_generated") is not False:
        issues.append("fake_price_rows_generated must be false")
    if safety.get("data_quality_gate_lowered") is not False:
        issues.append("data_quality_gate_lowered must be false")
    if safety.get("production_effect") != "none":
        issues.append("safety production_effect must be none")
    if safety.get("manual_review_required") is not True:
        issues.append("safety manual_review_required must be true")
    if safety.get("auto_promotion") is not False:
        issues.append("safety auto_promotion must be false")
    return issues


def market_data_freshness_payload_date(payload: dict[str, Any], source_path: Path) -> date:
    metadata = _mapping(payload.get("metadata"))
    run_id = str(metadata.get("run_id") or "")
    if run_id.startswith("market-data-freshness-"):
        parsed = _parse_date(run_id.removeprefix("market-data-freshness-"))
        if parsed is not None:
            return parsed
    data_dates = _mapping(payload.get("data_dates"))
    parsed = _parse_date(str(data_dates.get("tracking_date") or ""))
    if parsed is not None:
        return parsed
    parsed = _parse_date(source_path.parent.name)
    if parsed is not None:
        return parsed
    raise ValueError(f"cannot infer market data freshness date from {source_path}")


def render_market_data_freshness_markdown(payload: dict[str, Any]) -> str:
    metadata = _mapping(payload.get("metadata"))
    calendar = _mapping(payload.get("calendar"))
    data_dates = _mapping(payload.get("data_dates"))
    freshness = _mapping(payload.get("freshness"))
    coverage = _mapping(payload.get("asset_coverage"))
    readiness = _mapping(payload.get("tracking_readiness"))
    safety = _mapping(payload.get("safety"))
    lines = [
        "# Market Data Freshness Summary",
        "",
        "## 1. Executive Summary",
        "",
        f"- freshness_status: `{freshness.get('status', metadata.get('status', 'UNKNOWN'))}`",
        f"- production_effect: `{metadata.get('production_effect', 'none')}`",
        f"- manual_review_required: `{metadata.get('manual_review_required', True)}`",
        f"- auto_promotion: `{metadata.get('auto_promotion', False)}`",
        f"- reason: {freshness.get('reason', '')}",
        "",
        "## 2. Market Calendar",
        "",
    ]
    for key in (
        "market",
        "tracking_date",
        "is_trading_day",
        "previous_trading_day",
        "market_close_time",
        "expected_data_ready_time",
        "calendar_status",
        "calendar_source",
    ):
        lines.append(f"- `{key}`: `{calendar.get(key, '')}`")
    lines.extend(["", "## 3. Tracking Date vs Effective Data Date", ""])
    for key in (
        "tracking_date",
        "expected_data_date",
        "effective_data_date",
        "latest_raw_price_date",
        "latest_required_asset_common_date",
        "latest_registry_date",
        "latest_manifest_date",
    ):
        lines.append(f"- `{key}`: `{data_dates.get(key, '')}`")
    lines.extend(["", "## 4. Asset Coverage", "", f"- status: `{coverage.get('status', '')}`"])
    lines.extend(["", "| Asset | Status | Latest Date | Reason |", "|---|---|---|---|"])
    for symbol, item in _mapping(coverage.get("assets")).items():
        record = _mapping(item)
        lines.append(
            "| "
            f"`{symbol}` | "
            f"`{record.get('status', '')}` | "
            f"`{record.get('latest_date', '')}` | "
            f"{_escape_table(str(record.get('reason', '')))} |"
        )
    lines.extend(
        [
            "",
            "## 5. Freshness Status",
            "",
            f"- status: `{freshness.get('status', '')}`",
            f"- lag_trading_days: `{freshness.get('lag_trading_days', '')}`",
            f"- lag_calendar_days: `{freshness.get('lag_calendar_days', '')}`",
            f"- reason: {freshness.get('reason', '')}",
            "",
            "## 6. Tracking Readiness",
            "",
        ]
    )
    for key in ("can_track", "tracking_status_recommendation", "readiness", "reason"):
        lines.append(f"- `{key}`: `{readiness.get(key, '')}`")
    lines.extend(["", "## 7. Suggested Actions", ""])
    actions = _records(payload.get("suggested_actions"))
    if actions:
        for action in actions:
            lines.append(
                "- "
                f"action=`{action.get('action', '')}`；"
                f"required=`{action.get('required', False)}`"
            )
    else:
        lines.append("- 当前无需 freshness repair action。")
    lines.extend(
        [
            "",
            "## 8. Impact on Shadow Candidate Tracking",
            "",
            (
                "- "
                "Tracking recommendation is "
                f"`{readiness.get('tracking_status_recommendation', '')}`; "
                "freshness does not enable production promotion."
            ),
            "",
            "## 9. Manual Review Checklist",
            "",
            "- 确认 tracking date、expected data date 和 effective data date 是否符合运行时间。",
            "- 如果状态为 STALE，先刷新 market data cache 并重建 manifest，再运行 tracking。",
            "- 如果状态为 MISSING，先修复 required asset 覆盖，不得伪造价格。",
            "- 不修改 `config/parameters/production/current.yaml`，不放宽 data quality gate。",
            "",
            "## 10. Safety",
            "",
        ]
    )
    for key, value in safety.items():
        lines.append(f"- `{key}`: `{value}`")
    return "\n".join(lines).rstrip() + "\n"


def _freshness_status(
    *,
    tracking_date: date,
    effective_data_date: date | None,
    expected_data_date: date | None,
    calendar: dict[str, Any],
    asset_coverage: dict[str, Any],
    lag_calendar_days: int | None,
    lag_trading_days: int | None,
    generated_at: datetime,
    config: dict[str, Any],
) -> tuple[str, str]:
    if calendar.get("calendar_status") == FRESHNESS_MARKET_CALENDAR_UNKNOWN:
        return (
            FRESHNESS_MARKET_CALENDAR_UNKNOWN,
            "Market calendar could not be resolved for the requested market.",
        )
    if asset_coverage.get("status") == FRESHNESS_MISSING or effective_data_date is None:
        return (
            FRESHNESS_MISSING,
            "One or more required assets are missing at the effective data date.",
        )
    if expected_data_date is None:
        return FRESHNESS_FAILED, "Expected data date could not be determined."
    if effective_data_date >= expected_data_date:
        if calendar.get("is_trading_day") is False:
            return (
                FRESHNESS_NON_TRADING_DAY,
                "Tracking date is not a U.S. trading day; latest previous trading day "
                "data is available.",
            )
        return FRESHNESS_OK, "Tracking date data is available for all required assets."
    if calendar.get("is_trading_day") is False:
        return (
            FRESHNESS_STALE,
            "Tracking date is non-trading, but latest previous trading day data is not "
            "fully available.",
        )
    ready_time = _parse_datetime(str(calendar.get("expected_data_ready_time") or ""))
    within_lag = _within_lag_limits(lag_calendar_days, lag_trading_days, config)
    allow_before_ready = bool(
        _mapping(config.get("market")).get("allow_previous_trading_day_before_ready_time", True)
    )
    if (
        allow_before_ready
        and ready_time is not None
        and generated_at.astimezone(ready_time.tzinfo) < ready_time
        and within_lag
    ):
        return (
            FRESHNESS_ACCEPTABLE_LAG,
            (
                "Latest run occurred before expected data readiness window for "
                f"{tracking_date.isoformat()} daily bars."
            ),
        )
    return (
        FRESHNESS_STALE,
        "Market data should be available for the expected data date, but effective data is older.",
    )


def _tracking_readiness(status: str) -> dict[str, Any]:
    can_track = status in TRACKING_ALLOWED_FRESHNESS
    recommendation = TRACKING_STATUS_BY_FRESHNESS.get(status, "tracking_blocked")
    if can_track:
        reason = (
            "Market data freshness allows shadow candidate tracking. "
            "Production promotion remains disabled."
        )
    else:
        reason = "Market data freshness blocks or degrades tracking until data is refreshed."
    return {
        "can_track": can_track,
        "readiness": "can_track" if can_track else "cannot_track",
        "tracking_status_recommendation": recommendation,
        "reason": reason,
    }


def _suggested_actions(
    *,
    status: str,
    calendar: dict[str, Any],
    missing_expected_assets: list[str],
) -> list[dict[str, Any]]:
    if status == FRESHNESS_ACCEPTABLE_LAG:
        return [{"action": "rerun_after_expected_data_ready_time", "required": False}]
    if status == FRESHNESS_STALE:
        return [
            {
                "action": "refresh_market_data_cache",
                "required": True,
                "symbols": missing_expected_assets,
            },
            {"action": "refresh_backtest_manifest", "required": True},
        ]
    if status == FRESHNESS_MISSING:
        return [
            {
                "action": "repair_missing_market_data",
                "required": True,
                "symbols": missing_expected_assets,
            }
        ]
    if status == FRESHNESS_MARKET_CALENDAR_UNKNOWN:
        return [{"action": "review_market_calendar_configuration", "required": True}]
    if status == FRESHNESS_NON_TRADING_DAY:
        return [{"action": "continue_with_previous_trading_day_data", "required": False}]
    if status == FRESHNESS_FAILED:
        return [{"action": "inspect_market_data_freshness_failure", "required": True}]
    if calendar.get("is_trading_day") is True:
        return []
    return [{"action": "no_refresh_required_for_non_trading_day", "required": False}]


def _calendar_payload(
    *,
    tracking_date: date,
    market: str,
    generated_at: datetime,
    market_tz: ZoneInfo,
    config: dict[str, Any],
) -> dict[str, Any]:
    close_time = _parse_time(str(_mapping(config.get("market")).get("close_time") or "16:00"))
    ready_minutes = _int(
        _mapping(config.get("market")).get("expected_data_ready_after_close_minutes"),
        180,
    )
    close_dt = datetime.combine(tracking_date, close_time, tzinfo=market_tz)
    ready_dt = close_dt + timedelta(minutes=ready_minutes)
    if market.upper() != "US":
        previous = previous_us_equity_trading_day(tracking_date)
        return {
            "market": market,
            "tracking_date": tracking_date.isoformat(),
            "is_trading_day": None,
            "previous_trading_day": previous.isoformat(),
            "market_close_time": close_dt.isoformat(),
            "expected_data_ready_time": ready_dt.isoformat(),
            "generated_at_market_time": generated_at.astimezone(market_tz).isoformat(),
            "calendar_status": FRESHNESS_MARKET_CALENDAR_UNKNOWN,
            "calendar_source": "unsupported_market_calendar",
            "reason": "Only US market calendar is configured for this gate.",
        }
    session = us_equity_market_session(tracking_date)
    return {
        "market": market,
        "tracking_date": tracking_date.isoformat(),
        "is_trading_day": session.is_trading_day,
        "previous_trading_day": session.previous_trading_day.isoformat(),
        "market_close_time": close_dt.isoformat(),
        "expected_data_ready_time": ready_dt.isoformat(),
        "generated_at_market_time": generated_at.astimezone(market_tz).isoformat(),
        "calendar_status": "TRADING_DAY" if session.is_trading_day else "NON_TRADING_DAY",
        "calendar_source": NYSE_REGULAR_HOLIDAY_CALENDAR_SOURCE,
        "reason": session.reason,
    }


def _expected_data_date(calendar: dict[str, Any], tracking_date: date) -> date | None:
    if calendar.get("calendar_status") == FRESHNESS_MARKET_CALENDAR_UNKNOWN:
        return None
    if calendar.get("is_trading_day") is False:
        return _parse_date(str(calendar.get("previous_trading_day") or ""))
    return tracking_date


def _asset_coverage_payload(
    *,
    prices: pd.DataFrame,
    required_assets: tuple[str, ...],
    manifest_mapping: dict[str, object],
    effective_data_date: date | None,
    expected_data_date: date | None,
) -> dict[str, Any]:
    assets: dict[str, dict[str, Any]] = {}
    missing_effective: list[str] = []
    missing_expected: list[str] = []
    for symbol in required_assets:
        resolution = resolve_symbol(symbol, manifest_mapping=manifest_mapping)
        latest_date = _asset_latest_date(
            prices,
            resolution.canonical_symbol,
            resolution.source_symbol,
        )
        has_effective = effective_data_date is not None and _asset_has_date(
            prices,
            resolution.canonical_symbol,
            resolution.source_symbol,
            effective_data_date,
        )
        has_expected = expected_data_date is not None and _asset_has_date(
            prices,
            resolution.canonical_symbol,
            resolution.source_symbol,
            expected_data_date,
        )
        if not has_effective:
            missing_effective.append(symbol)
        if not has_expected:
            missing_expected.append(symbol)
        status = "OK" if has_effective else FRESHNESS_MISSING
        if status == "OK" and resolution.canonical_symbol != resolution.source_symbol:
            reason = f"OK via {resolution.source_symbol}"
        elif status == "OK":
            reason = "OK"
        else:
            reason = "Missing required asset coverage at effective data date."
        assets[symbol] = {
            "status": status,
            "source_symbol": resolution.source_symbol,
            "latest_date": "" if latest_date is None else latest_date.isoformat(),
            "has_effective_data": has_effective,
            "has_expected_date_data": has_expected,
            "reason": reason,
        }
    return {
        "status": FRESHNESS_MISSING if missing_effective else "OK",
        "required_assets": list(required_assets),
        "missing_effective_assets": missing_effective,
        "missing_expected_date_assets": missing_expected,
        "assets": assets,
    }


def _asset_latest_dates(
    prices: pd.DataFrame,
    required_assets: tuple[str, ...],
    manifest_mapping: dict[str, object],
) -> dict[str, date | None]:
    latest: dict[str, date | None] = {}
    for symbol in required_assets:
        resolution = resolve_symbol(symbol, manifest_mapping=manifest_mapping)
        latest[symbol] = _asset_latest_date(
            prices,
            resolution.canonical_symbol,
            resolution.source_symbol,
        )
    return latest


def _latest_common_date(
    prices: pd.DataFrame,
    required_assets: tuple[str, ...],
    manifest_mapping: dict[str, object],
) -> date | None:
    common_dates: set[date] | None = None
    for symbol in required_assets:
        resolution = resolve_symbol(symbol, manifest_mapping=manifest_mapping)
        dates = _asset_dates(prices, resolution.canonical_symbol, resolution.source_symbol)
        if not dates:
            return None
        common_dates = dates if common_dates is None else common_dates & dates
        if not common_dates:
            return None
    return max(common_dates) if common_dates else None


def _effective_data_date(common_date: date | None, manifest_date: date | None) -> date | None:
    dates = [item for item in (common_date, manifest_date) if item is not None]
    return min(dates) if dates else None


def _lag_calendar_days(
    expected_data_date: date | None,
    effective_data_date: date | None,
) -> int | None:
    if expected_data_date is None or effective_data_date is None:
        return None
    return max((expected_data_date - effective_data_date).days, 0)


def _lag_trading_days(
    expected_data_date: date | None,
    effective_data_date: date | None,
    market: str,
) -> int | None:
    if expected_data_date is None or effective_data_date is None:
        return None
    if effective_data_date >= expected_data_date:
        return 0
    if market.upper() != "US":
        return None
    count = 0
    cursor = expected_data_date
    while cursor > effective_data_date:
        if us_equity_market_session(cursor).is_trading_day:
            count += 1
        cursor -= timedelta(days=1)
    return count


def _within_lag_limits(
    lag_calendar_days: int | None,
    lag_trading_days: int | None,
    config: dict[str, Any],
) -> bool:
    freshness = _mapping(config.get("freshness"))
    max_trading = _int(freshness.get("max_acceptable_lag_trading_days"), 1)
    max_calendar = _int(freshness.get("max_acceptable_lag_calendar_days"), 3)
    if lag_calendar_days is None or lag_calendar_days > max_calendar:
        return False
    if lag_trading_days is not None and lag_trading_days > max_trading:
        return False
    return True


def _required_assets(config: dict[str, Any], shadow_config_path: Path) -> tuple[str, ...]:
    configured = _strings(_mapping(config.get("assets")).get("required"))
    if configured:
        return tuple(dict.fromkeys(configured))
    try:
        shadow_config = load_shadow_backtest_config(shadow_config_path)
    except (OSError, ValueError):
        return DEFAULT_REQUIRED_BACKTEST_ASSETS
    try:
        baseline_path = resolve_project_path(shadow_config.baseline_parameters_path)
        from ai_trading_system.trading_engine.parameters.parameter_loader import (
            load_production_parameters,
        )

        baseline = load_production_parameters(baseline_path)
    except (OSError, ValueError):
        return DEFAULT_REQUIRED_BACKTEST_ASSETS
    assets = tuple(asset for asset in baseline.flattened_asset_universe() if asset != "CASH")
    return assets or DEFAULT_REQUIRED_BACKTEST_ASSETS


def _read_prices(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        frame = pd.read_csv(path)
    except Exception:
        return pd.DataFrame()
    if "date" in frame.columns:
        frame = frame.copy()
        frame["_parsed_date"] = pd.to_datetime(frame["date"], errors="coerce").dt.date
    return frame


def _latest_price_date(prices: pd.DataFrame) -> date | None:
    if prices.empty or "_parsed_date" not in prices.columns:
        return None
    values = [item for item in prices["_parsed_date"].tolist() if isinstance(item, date)]
    return max(values) if values else None


def _asset_dates(prices: pd.DataFrame, canonical_symbol: str, source_symbol: str) -> set[date]:
    if prices.empty or "_parsed_date" not in prices.columns:
        return set()
    rows = _asset_rows(prices, canonical_symbol, source_symbol)
    return {item for item in rows["_parsed_date"].tolist() if isinstance(item, date)}


def _asset_latest_date(
    prices: pd.DataFrame,
    canonical_symbol: str,
    source_symbol: str,
) -> date | None:
    dates = _asset_dates(prices, canonical_symbol, source_symbol)
    return max(dates) if dates else None


def _asset_has_date(
    prices: pd.DataFrame,
    canonical_symbol: str,
    source_symbol: str,
    value: date,
) -> bool:
    return value in _asset_dates(prices, canonical_symbol, source_symbol)


def _asset_rows(prices: pd.DataFrame, canonical_symbol: str, source_symbol: str) -> pd.DataFrame:
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
    return prices.loc[mask]


def _price_cache_registry_payload(config: dict[str, Any]) -> dict[str, Any]:
    path = _price_cache_registry_path(config)
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _latest_registry_date(payload: dict[str, Any], required_assets: tuple[str, ...]) -> date | None:
    assets = _mapping(payload.get("assets"))
    dates: list[date] = []
    for symbol in required_assets:
        record = _mapping(assets.get(symbol))
        date_range = _mapping(record.get("date_range"))
        parsed = _parse_date(str(date_range.get("end") or ""))
        if parsed is not None:
            dates.append(parsed)
    return min(dates) if dates else None


def _prices_path(config: dict[str, Any]) -> Path:
    raw = _input(config, "prices_path")
    if raw:
        return resolve_project_path(str(raw))
    return PROJECT_ROOT / "data" / "raw" / "prices_daily.csv"


def _artifact_root(config: dict[str, Any]) -> Path:
    raw = _input(config, "backtest_snapshot_dir")
    if raw:
        path = resolve_project_path(str(raw))
        return path.parent if path.name == "backtest_snapshots" else path
    return PROJECT_ROOT / "artifacts"


def _price_cache_registry_path(config: dict[str, Any]) -> Path:
    raw = _input(config, "price_cache_registry_path")
    return (
        resolve_project_path(str(raw))
        if raw
        else PROJECT_ROOT / "artifacts" / "data_registry" / "price_cache_registry.json"
    )


def _download_manifest_path(config: dict[str, Any]) -> Path:
    raw = _input(config, "download_manifest_path")
    return (
        resolve_project_path(str(raw))
        if raw
        else PROJECT_ROOT / "data" / "raw" / "download_manifest.csv"
    )


def _output_root(config: dict[str, Any], *, dry_run: bool) -> Path:
    key = "dry_run_dir" if dry_run else "market_data_freshness_dir"
    default = (
        PROJECT_ROOT / "outputs" / "dry_runs" / "data_freshness"
        if dry_run
        else default_market_data_freshness_root()
    )
    raw = _mapping(config.get("output")).get(key)
    return resolve_project_path(str(raw)) if raw else default


def _validate_config(payload: dict[str, Any]) -> None:
    if payload.get("production_effect") != "none":
        raise ValueError("market data freshness production_effect must be none")
    if payload.get("manual_review_required") is not True:
        raise ValueError("market data freshness manual_review_required must be true")
    if payload.get("auto_promotion") is not False:
        raise ValueError("market data freshness auto_promotion must be false")
    safety = _mapping(payload.get("safety"))
    if safety.get("production_write_allowed") is not False:
        raise ValueError("market data freshness must not allow production writes")
    if safety.get("data_download_allowed") is not False:
        raise ValueError("market data freshness must not download data")
    if safety.get("fake_price_rows_allowed") is not False:
        raise ValueError("market data freshness must not allow fake price rows")
    if not _strings(_mapping(payload.get("assets")).get("required")):
        raise ValueError("market data freshness requires configured assets")


def _input(config: dict[str, Any], key: str) -> object:
    return _mapping(config.get("input")).get(key)


def _mapping(value: object) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _records(value: object) -> list[dict[str, Any]]:
    return [item for item in value if isinstance(item, dict)] if isinstance(value, list) else []


def _strings(value: object) -> list[str]:
    if isinstance(value, list | tuple):
        return [str(item) for item in value if str(item)]
    return []


def _int(value: object, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _parse_date(value: str) -> date | None:
    try:
        return date.fromisoformat(value)
    except (TypeError, ValueError):
        return None


def _parse_time(value: str) -> time:
    try:
        hour, minute = value.split(":", maxsplit=1)
        return time(int(hour), int(minute))
    except (ValueError, TypeError):
        return time(16, 0)


def _parse_datetime(value: str) -> datetime | None:
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def _escape_table(value: str) -> str:
    return value.replace("|", "\\|").replace("\n", " ")
