from __future__ import annotations

import hashlib
import json
import math
import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd

from ai_trading_system.config import (
    UniverseConfig,
    configured_price_tickers,
    configured_rate_series,
    load_data_source_request_budget_policy,
)
from ai_trading_system.data.market_data import (
    CBOE_VIX_TICKER,
    CboeVixPriceProvider,
    CsvDataCache,
    FmpPriceProvider,
    FredRateProvider,
    MarketstackPriceProvider,
    PriceDataProvider,
    PriceRequest,
    ProviderDownloadError,
    ProviderRequestDiagnostic,
    RateDataProvider,
    RateRequest,
    YFinancePriceProvider,
)
from ai_trading_system.external_request_cache import (
    ExternalRequestCacheEvent,
    default_external_request_cache_dir,
    external_request_cache_trace,
    sanitize_diagnostic_text,
)
from ai_trading_system.trading_calendar import is_us_equity_trading_day

_PRICE_COLUMNS = ("date", "ticker", "open", "high", "low", "close", "adj_close", "volume")


@dataclass(frozen=True)
class DataDownloadSummary:
    prices_path: Path
    rates_path: Path
    manifest_path: Path
    price_rows: int
    rate_rows: int
    price_tickers: tuple[str, ...]
    rate_series: tuple[str, ...]
    secondary_prices_path: Path | None = None
    secondary_price_rows: int = 0
    request_cache_summaries: tuple[dict[str, object], ...] = ()
    request_budget_statuses: tuple[dict[str, object], ...] = ()


@dataclass(frozen=True)
class IncrementalPriceWindow:
    tickers: tuple[str, ...]
    start: date
    end: date

    def to_payload(self) -> dict[str, object]:
        return {
            "tickers": list(self.tickers),
            "start": self.start.isoformat(),
            "end": self.end.isoformat(),
        }


@dataclass(frozen=True)
class IncrementalPriceDownload:
    prices: pd.DataFrame
    supported_tickers: tuple[str, ...]
    skipped_tickers: tuple[str, ...]
    fetch_windows: tuple[IncrementalPriceWindow, ...]
    reused_row_count: int
    fetched_row_count: int
    output_row_count: int
    request_budget_status: dict[str, object] | None = None


class ProviderQuotaBudgetError(RuntimeError):
    """Raised before live provider calls when cached quota metadata says they cannot finish."""

    def __init__(
        self,
        message: str,
        *,
        budget_status: Mapping[str, object] | None = None,
    ) -> None:
        super().__init__(message)
        self.budget_status = dict(budget_status or {})


_MARKETSTACK_DAILY_OVERAGE_PROFILE = "owner_approved_overage"
_MARKETSTACK_TAIL_CATCH_UP_PROFILE = "owner_approved_tail_catch_up"
_MARKETSTACK_QUOTA_CYCLE_RESET_PROFILE = "quota_cycle_reset"
_MARKETSTACK_BLOCKED_STATUS = "BLOCKED_QUOTA_INSUFFICIENT"
_MARKETSTACK_CALENDAR_WINDOW_BLOCKER = "calendar_window_exceeds_owner_approved_limit"


def default_download_failure_report_path(reports_dir: Path, as_of: date) -> Path:
    return reports_dir / f"download_data_diagnostics_{as_of.isoformat()}.md"


def write_download_failure_report(
    *,
    output_path: Path,
    start: date,
    end: date,
    raw_output_dir: Path,
    include_full_ai_chain: bool,
    price_provider_name: str,
    with_marketstack: bool,
    error: BaseException,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    generated_at = datetime.now(tz=UTC)
    diagnostic = _provider_diagnostic_from_error(error)
    budget_status = _provider_budget_status_from_error(error)
    lines = [
        "# download-data 失败诊断报告",
        "",
        "- 状态：FAIL",
        f"- 生成时间：{generated_at.isoformat()}",
        f"- 下载开始日期：{start.isoformat()}",
        f"- 下载结束日期：{end.isoformat()}",
        f"- Raw 输出目录：`{raw_output_dir}`",
        f"- 主价格源：`{price_provider_name}`",
        f"- Full universe：{include_full_ai_chain}",
        f"- Marketstack 第二源：{with_marketstack}",
        "- 安全边界：本报告不保存 API key、token、Cookie、User-Agent、"
        "stdout/stderr 原文或供应商响应正文。",
        "",
        "## 失败摘要",
        "",
        f"- Exception type：`{type(error).__name__}`",
        f"- Sanitized message：{sanitize_diagnostic_text(str(error))}",
    ]
    if budget_status:
        lines.extend(_render_provider_budget_status(budget_status))
    if diagnostic is None:
        lines.extend(
            [
                "",
                "## Provider 诊断",
                "",
                "未捕获到结构化 provider 诊断。请优先检查子命令 stderr 的脱敏摘要，"
                "或补充对应 provider adapter 的诊断上下文。",
            ]
        )
    else:
        lines.extend(_render_provider_diagnostic(diagnostic))
    lines.extend(
        [
            "",
            "## 下游影响",
            "",
            "- `download-data` 已 fail closed；不得把可能部分刷新的 CSV 当作完整可审计输入。",
            "- 未成功写入本轮下载审计 manifest 时，`daily-run` 必须停止 PIT、SEC、"
            "valuation 和 `score-daily` 下游步骤。",
        ]
    )
    output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return output_path


def download_daily_data(
    config: UniverseConfig,
    start: date,
    end: date,
    output_dir: Path,
    include_full_ai_chain: bool = False,
    price_provider: PriceDataProvider | None = None,
    vix_price_provider: PriceDataProvider | None = None,
    secondary_price_provider: PriceDataProvider | None = None,
    rate_provider: RateDataProvider | None = None,
    marketstack_tail_catch_up: bool = True,
) -> DataDownloadSummary:
    if start > end:
        raise ValueError("start date must be earlier than or equal to end date")

    price_tickers = configured_price_tickers(config, include_full_ai_chain=include_full_ai_chain)
    rate_series = configured_rate_series(config)

    if not price_tickers:
        raise ValueError("price ticker list must not be empty")
    if not rate_series:
        raise ValueError("rate series list must not be empty")

    price_provider = price_provider or YFinancePriceProvider()
    rate_provider = rate_provider or FredRateProvider()
    cache = CsvDataCache(output_dir)
    prices_path_target = output_dir / "prices_daily.csv"
    secondary_prices_path_target = output_dir / "prices_marketstack_daily.csv"

    price_request = PriceRequest(tickers=price_tickers, start=start, end=end, interval="1d")
    rate_request = RateRequest(series_ids=rate_series, start=start, end=end)

    with external_request_cache_trace() as request_cache_events:
        primary_tickers = _supported_price_tickers(price_provider, price_tickers)
        primary_download = _download_incremental_prices(
            provider=price_provider,
            request=PriceRequest(
                tickers=list(primary_tickers),
                start=start,
                end=end,
                interval="1d",
            ),
            existing_path=prices_path_target,
        )
        price_frames = [primary_download.prices]
        vix_download: IncrementalPriceDownload | None = None
        vix_request: PriceRequest | None = None
        if CBOE_VIX_TICKER in price_tickers and CBOE_VIX_TICKER not in set(
            primary_download.prices["ticker"].astype(str)
        ):
            vix_price_provider = vix_price_provider or CboeVixPriceProvider()
            vix_request = PriceRequest(
                tickers=[CBOE_VIX_TICKER],
                start=start,
                end=end,
                interval="1d",
            )
            vix_download = _download_incremental_prices(
                provider=vix_price_provider,
                request=vix_request,
                existing_path=prices_path_target,
            )
            price_frames.append(vix_download.prices)
        prices = _merge_price_frames(
            price_frames,
            tickers=price_tickers,
            start=start,
            end=end,
        )
        rates = rate_provider.download_rates(rate_request)

        secondary_download: IncrementalPriceDownload | None = None
        if secondary_price_provider is not None:
            secondary_tickers = _supported_price_tickers(secondary_price_provider, price_tickers)
            secondary_download = _download_incremental_prices(
                provider=secondary_price_provider,
                request=PriceRequest(
                    tickers=list(secondary_tickers),
                    start=start,
                    end=end,
                    interval="1d",
                ),
                existing_path=secondary_prices_path_target,
                marketstack_tail_catch_up=marketstack_tail_catch_up,
            )

    prices_path = cache.write_prices(prices)
    rates_path = cache.write_rates(rates)
    secondary_prices_path: Path | None = None
    secondary_price_rows = 0
    request_cache_summaries = tuple(_request_cache_summary_records(request_cache_events))
    request_budget_statuses = tuple(
        status
        for status in (
            primary_download.request_budget_status,
            None if vix_download is None else vix_download.request_budget_status,
            None if secondary_download is None else secondary_download.request_budget_status,
        )
        if status is not None
    )
    manifest_records = [
        _manifest_record_for_prices(
            price_provider,
            price_request,
            prices_path,
            len(primary_download.prices),
            incremental_download=primary_download,
            request_cache_summaries=request_cache_summaries,
        ),
        _manifest_record_for_rates(
            rate_provider,
            rate_request,
            rates_path,
            len(rates),
            request_cache_summaries=request_cache_summaries,
        ),
    ]
    if vix_price_provider is not None and vix_request is not None and vix_download is not None:
        manifest_records.append(
            _manifest_record_for_prices(
                vix_price_provider,
                vix_request,
                prices_path,
                len(vix_download.prices),
                incremental_download=vix_download,
                request_cache_summaries=request_cache_summaries,
            )
        )

    if secondary_price_provider is not None and secondary_download is not None:
        secondary_prices_path = cache.write_prices(
            secondary_download.prices,
            filename="prices_marketstack_daily.csv",
        )
        secondary_price_rows = len(secondary_download.prices)
        manifest_records.append(
            _manifest_record_for_prices(
                secondary_price_provider,
                price_request,
                secondary_prices_path,
                secondary_price_rows,
                incremental_download=secondary_download,
                request_cache_summaries=request_cache_summaries,
            )
        )

    manifest_path = write_download_manifest(
        output_dir=output_dir,
        records=tuple(manifest_records),
    )

    return DataDownloadSummary(
        prices_path=prices_path,
        rates_path=rates_path,
        manifest_path=manifest_path,
        price_rows=len(prices),
        rate_rows=len(rates),
        price_tickers=tuple(price_tickers),
        rate_series=tuple(rate_series),
        secondary_prices_path=secondary_prices_path,
        secondary_price_rows=secondary_price_rows,
        request_cache_summaries=request_cache_summaries,
        request_budget_statuses=request_budget_statuses,
    )


def _download_incremental_prices(
    *,
    provider: PriceDataProvider,
    request: PriceRequest,
    existing_path: Path,
    marketstack_tail_catch_up: bool = False,
) -> IncrementalPriceDownload:
    supported_tickers = _supported_price_tickers(provider, request.tickers)
    skipped_tickers = tuple(ticker for ticker in request.tickers if ticker not in supported_tickers)
    existing = _read_existing_price_cache(
        existing_path,
        tickers=supported_tickers,
        start=request.start,
        end=request.end,
    )
    fetch_windows = _price_fetch_windows(
        existing,
        tickers=supported_tickers,
        start=request.start,
        end=request.end,
    )
    fetch_windows, request_budget_status = _marketstack_fetch_windows_and_budget_status(
        provider=provider,
        request=request,
        fetch_windows=fetch_windows,
        allow_tail_catch_up=marketstack_tail_catch_up,
    )

    fetched_frames: list[pd.DataFrame] = []
    for window in fetch_windows:
        fetched = provider.download_prices(
            PriceRequest(
                tickers=list(window.tickers),
                start=window.start,
                end=window.end,
                interval=request.interval,
            )
        )
        fetched_frames.append(fetched)

    prices = _merge_price_frames(
        [existing, *fetched_frames],
        tickers=supported_tickers,
        start=request.start,
        end=request.end,
    )
    return IncrementalPriceDownload(
        prices=prices,
        supported_tickers=supported_tickers,
        skipped_tickers=skipped_tickers,
        fetch_windows=fetch_windows,
        reused_row_count=len(existing),
        fetched_row_count=sum(len(frame) for frame in fetched_frames),
        output_row_count=len(prices),
        request_budget_status=request_budget_status,
    )


def _supported_price_tickers(
    provider: PriceDataProvider,
    tickers: list[str] | tuple[str, ...],
) -> tuple[str, ...]:
    if isinstance(provider, FmpPriceProvider):
        return tuple(
            ticker for ticker in tickers if provider.provider_symbol_for(ticker) is not None
        )
    if isinstance(provider, MarketstackPriceProvider):
        return tuple(
            ticker
            for ticker in tickers
            if provider.symbol_aliases.get(ticker, ticker) is not None
        )
    if isinstance(provider, CboeVixPriceProvider):
        return tuple(ticker for ticker in tickers if ticker == provider.ticker)
    return tuple(tickers)


def _read_existing_price_cache(
    path: Path,
    *,
    tickers: tuple[str, ...],
    start: date,
    end: date,
) -> pd.DataFrame:
    if not tickers:
        return _empty_price_frame()
    if not path.exists():
        return _empty_price_frame()
    try:
        frame = pd.read_csv(path)
    except Exception as exc:
        raise ValueError(f"Existing price cache is unreadable: {path}") from exc
    missing = {"date", "ticker"} - set(frame.columns)
    if missing:
        raise ValueError(
            "Existing price cache missing required columns: " + ", ".join(sorted(missing))
        )
    filtered = frame.loc[frame["ticker"].astype(str).isin(tickers)].copy()
    if filtered.empty:
        return _empty_price_frame()
    parsed_dates = pd.to_datetime(filtered["date"], errors="coerce")
    invalid_count = int(parsed_dates.isna().sum())
    if invalid_count:
        raise ValueError(
            f"Existing price cache has {invalid_count} invalid date values for requested tickers"
        )
    filtered["date"] = parsed_dates.dt.strftime("%Y-%m-%d")
    filtered = filtered.loc[
        (parsed_dates.dt.date >= start) & (parsed_dates.dt.date <= end)
    ].copy()
    return _normalize_price_frame_columns(filtered)


def _price_fetch_windows(
    existing: pd.DataFrame,
    *,
    tickers: tuple[str, ...],
    start: date,
    end: date,
) -> tuple[IncrementalPriceWindow, ...]:
    if not tickers:
        return ()
    date_ranges_by_ticker = _price_date_ranges(existing)
    grouped: dict[date, list[str]] = {}
    for ticker in tickers:
        date_range = date_ranges_by_ticker.get(ticker)
        if date_range is None:
            fetch_start = start
        else:
            _earliest, latest = date_range
            # Daily refresh only fills tail gaps; head gaps are often holidays or pre-listing
            # periods and require an explicit repair/backfill workflow.
            if latest >= end:
                continue
            else:
                fetch_start = _next_us_equity_trading_day(latest)
        if fetch_start <= end:
            grouped.setdefault(fetch_start, []).append(ticker)
    return tuple(
        IncrementalPriceWindow(
            tickers=tuple(sorted(grouped[fetch_start])),
            start=fetch_start,
            end=end,
        )
        for fetch_start in sorted(grouped)
    )


def _next_us_equity_trading_day(value: date) -> date:
    candidate = value + timedelta(days=1)
    while not is_us_equity_trading_day(candidate):
        candidate += timedelta(days=1)
    return candidate


def _price_date_ranges(frame: pd.DataFrame) -> dict[str, tuple[date, date]]:
    if frame.empty:
        return {}
    parsed_dates = pd.to_datetime(frame["date"], errors="coerce")
    dates = parsed_dates.dt.date
    ranges: dict[str, tuple[date, date]] = {}
    for ticker, ticker_dates in dates.groupby(frame["ticker"].astype(str)):
        min_date = ticker_dates.min()
        max_date = ticker_dates.max()
        if pd.notna(min_date) and pd.notna(max_date):
            ranges[str(ticker)] = (min_date, max_date)
    return ranges


def _merge_price_frames(
    frames: list[pd.DataFrame],
    *,
    tickers: list[str] | tuple[str, ...],
    start: date,
    end: date,
) -> pd.DataFrame:
    non_empty = [frame for frame in frames if frame is not None and not frame.empty]
    if not non_empty:
        return _empty_price_frame()
    merged = pd.concat(non_empty, ignore_index=True)
    merged = _normalize_price_frame_columns(merged)
    parsed_dates = pd.to_datetime(merged["date"], errors="coerce")
    merged = merged.loc[parsed_dates.notna()].copy()
    parsed_dates = pd.to_datetime(merged["date"], errors="coerce")
    ticker_set = set(tickers)
    merged = merged.loc[
        merged["ticker"].astype(str).isin(ticker_set)
        & (parsed_dates.dt.date >= start)
        & (parsed_dates.dt.date <= end)
    ].copy()
    if merged.empty:
        return _empty_price_frame()
    merged["date"] = pd.to_datetime(merged["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    merged["ticker"] = merged["ticker"].astype(str)
    merged = merged.drop_duplicates(subset=["ticker", "date"], keep="last")
    ordered_columns = list(_PRICE_COLUMNS) + [
        column for column in merged.columns if column not in _PRICE_COLUMNS
    ]
    return merged[ordered_columns].sort_values(["ticker", "date"]).reset_index(drop=True)


def _normalize_price_frame_columns(frame: pd.DataFrame) -> pd.DataFrame:
    normalized = frame.copy()
    for column in _PRICE_COLUMNS:
        if column not in normalized.columns:
            normalized[column] = pd.NA
    return normalized


def _empty_price_frame() -> pd.DataFrame:
    return pd.DataFrame(columns=list(_PRICE_COLUMNS))


def _marketstack_fetch_windows_and_budget_status(
    *,
    provider: PriceDataProvider,
    request: PriceRequest,
    fetch_windows: tuple[IncrementalPriceWindow, ...],
    allow_tail_catch_up: bool,
) -> tuple[tuple[IncrementalPriceWindow, ...], dict[str, object] | None]:
    request_budget_status = _provider_request_budget_status(
        provider,
        fetch_windows,
        raise_on_block=False,
    )
    if not isinstance(provider, MarketstackPriceProvider):
        return fetch_windows, request_budget_status
    quota_cycle_reset_status = _marketstack_quota_cycle_reset_status(
        request_budget_status,
        fetch_windows=fetch_windows,
        request_start=request.start,
    )
    if quota_cycle_reset_status is not None:
        if quota_cycle_reset_status.get("status") == _MARKETSTACK_BLOCKED_STATUS:
            _raise_provider_quota_budget_error(quota_cycle_reset_status)
        return fetch_windows, quota_cycle_reset_status
    if (
        allow_tail_catch_up
        and request_budget_status is not None
        and _should_attempt_marketstack_tail_catch_up(
            request_budget_status,
            fetch_windows=fetch_windows,
            request_start=request.start,
        )
    ):
        split_windows = _split_marketstack_tail_catch_up_windows(fetch_windows)
        catch_up_status = _provider_request_budget_status(
            provider,
            split_windows,
            approval_profile=_MARKETSTACK_TAIL_CATCH_UP_PROFILE,
            raise_on_block=False,
        )
        if catch_up_status is not None:
            catch_up_status["tail_catch_up"] = {
                "applied": catch_up_status.get("status") != _MARKETSTACK_BLOCKED_STATUS,
                "source_budget_profile": _MARKETSTACK_DAILY_OVERAGE_PROFILE,
                "catch_up_budget_profile": _MARKETSTACK_TAIL_CATCH_UP_PROFILE,
                "original_fetch_windows": [window.to_payload() for window in fetch_windows],
                "split_fetch_windows": [window.to_payload() for window in split_windows],
                "original_violation_reasons": _budget_violation_reasons(
                    request_budget_status
                ),
            }
        if (
            catch_up_status is not None
            and catch_up_status.get("status") != _MARKETSTACK_BLOCKED_STATUS
        ):
            return split_windows, catch_up_status
        if catch_up_status is not None:
            _raise_provider_quota_budget_error(catch_up_status)
    if (
        request_budget_status is not None
        and request_budget_status.get("status") == _MARKETSTACK_BLOCKED_STATUS
    ):
        _raise_provider_quota_budget_error(request_budget_status)
    return fetch_windows, request_budget_status


def _marketstack_quota_cycle_reset_status(
    request_budget_status: Mapping[str, object] | None,
    *,
    fetch_windows: tuple[IncrementalPriceWindow, ...],
    request_start: date,
) -> dict[str, object] | None:
    if (
        request_budget_status is None
        or request_budget_status.get("status") != _MARKETSTACK_BLOCKED_STATUS
    ):
        return None
    policy = load_data_source_request_budget_policy()
    cycle_policy = policy.marketstack.eod_daily_prices.quota_cycle_reset
    if not cycle_policy.enabled:
        return None
    observed_at = _parse_quota_observed_at(
        request_budget_status.get("latest_quota_observed_at")
    )
    if observed_at is None:
        return None
    cycle_start, next_cycle_start = _marketstack_billing_cycle_bounds(
        datetime.now(tz=UTC),
        reset_day_of_month=cycle_policy.reset_day_of_month,
    )
    if observed_at >= cycle_start:
        return None
    if not fetch_windows or not all(window.start > request_start for window in fetch_windows):
        return None

    estimated_units = int(request_budget_status.get("estimated_increment_usage") or 0)
    window_calendar_days = tuple(
        max(1, (window.end - window.start).days + 1) for window in fetch_windows
    )
    violation_reasons: list[str] = []
    if estimated_units <= 0:
        violation_reasons.append("no_live_request_needed")
    if estimated_units > cycle_policy.max_estimated_increment_usage:
        violation_reasons.append("estimated_usage_exceeds_quota_cycle_reset_limit")
    if len(fetch_windows) > cycle_policy.max_fetch_window_count:
        violation_reasons.append("fetch_window_count_exceeds_quota_cycle_reset_limit")
    if any(
        days > cycle_policy.max_calendar_days_per_window
        for days in window_calendar_days
    ):
        violation_reasons.append("calendar_window_exceeds_quota_cycle_reset_limit")

    approved = not violation_reasons
    payload = dict(request_budget_status)
    payload.update(
        {
            "budget_profile": _MARKETSTACK_QUOTA_CYCLE_RESET_PROFILE,
            "status": (
                cycle_policy.allowed_status if approved else _MARKETSTACK_BLOCKED_STATUS
            ),
            "quota_cycle_reset": {
                "approved": approved,
                "approval_profile": _MARKETSTACK_QUOTA_CYCLE_RESET_PROFILE,
                "policy_version": policy.policy_version,
                "policy_status": policy.policy_metadata.status,
                "allowed_status": cycle_policy.allowed_status,
                "evidence_id": cycle_policy.evidence_id,
                "timezone": cycle_policy.timezone,
                "reset_day_of_month": cycle_policy.reset_day_of_month,
                "current_cycle_start": cycle_start.date().isoformat(),
                "current_cycle_end": (next_cycle_start.date() - timedelta(days=1)).isoformat(),
                "latest_quota_observed_at": observed_at.isoformat(),
                "stale_header_status": "STALE_PREVIOUS_BILLING_CYCLE",
                "stale_quota_limit": request_budget_status.get("quota_limit"),
                "stale_quota_remaining": request_budget_status.get("quota_remaining"),
                "estimated_increment_usage": estimated_units,
                "fetch_window_count": len(fetch_windows),
                "max_estimated_increment_usage": (
                    cycle_policy.max_estimated_increment_usage
                ),
                "max_fetch_window_count": cycle_policy.max_fetch_window_count,
                "max_calendar_days_per_window": (
                    cycle_policy.max_calendar_days_per_window
                ),
                "window_calendar_days": list(window_calendar_days),
                "violation_reasons": violation_reasons,
                "reason": cycle_policy.reason,
                "behavioral_impact": cycle_policy.behavioral_impact,
                "risk": cycle_policy.risk,
                "validation_coverage": cycle_policy.validation_coverage,
                "review_condition": cycle_policy.review_condition,
            },
        }
    )
    return payload


def _parse_quota_observed_at(value: object) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        observed_at = datetime.fromisoformat(value)
    except ValueError:
        return None
    if observed_at.tzinfo is None:
        observed_at = observed_at.replace(tzinfo=UTC)
    return observed_at.astimezone(UTC)


def _marketstack_billing_cycle_bounds(
    now: datetime,
    *,
    reset_day_of_month: int,
) -> tuple[datetime, datetime]:
    now_utc = now.astimezone(UTC)
    candidate = datetime(
        now_utc.year,
        now_utc.month,
        reset_day_of_month,
        tzinfo=UTC,
    )
    if now_utc < candidate:
        previous_year = now_utc.year - 1 if now_utc.month == 1 else now_utc.year
        previous_month = 12 if now_utc.month == 1 else now_utc.month - 1
        cycle_start = datetime(
            previous_year,
            previous_month,
            reset_day_of_month,
            tzinfo=UTC,
        )
    else:
        cycle_start = candidate
    next_year = cycle_start.year + 1 if cycle_start.month == 12 else cycle_start.year
    next_month = 1 if cycle_start.month == 12 else cycle_start.month + 1
    next_cycle_start = datetime(
        next_year,
        next_month,
        reset_day_of_month,
        tzinfo=UTC,
    )
    return cycle_start, next_cycle_start


def _should_attempt_marketstack_tail_catch_up(
    request_budget_status: Mapping[str, object],
    *,
    fetch_windows: tuple[IncrementalPriceWindow, ...],
    request_start: date,
) -> bool:
    if request_budget_status.get("status") != _MARKETSTACK_BLOCKED_STATUS:
        return False
    reasons = set(_budget_violation_reasons(request_budget_status))
    if reasons != {_MARKETSTACK_CALENDAR_WINDOW_BLOCKER}:
        return False
    if not fetch_windows:
        return False
    return all(window.start > request_start for window in fetch_windows)


def _split_marketstack_tail_catch_up_windows(
    fetch_windows: tuple[IncrementalPriceWindow, ...],
) -> tuple[IncrementalPriceWindow, ...]:
    split_windows: list[IncrementalPriceWindow] = []
    for window in fetch_windows:
        current = window.start
        while current <= window.end:
            if is_us_equity_trading_day(current):
                split_windows.append(
                    IncrementalPriceWindow(
                        tickers=window.tickers,
                        start=current,
                        end=current,
                    )
                )
            current += timedelta(days=1)
    return tuple(split_windows) or fetch_windows


def _provider_request_budget_status(
    provider: PriceDataProvider,
    fetch_windows: tuple[IncrementalPriceWindow, ...],
    *,
    approval_profile: str = _MARKETSTACK_DAILY_OVERAGE_PROFILE,
    raise_on_block: bool = True,
) -> dict[str, object] | None:
    if not isinstance(provider, MarketstackPriceProvider):
        return None
    estimated_units = _estimate_marketstack_increment_usage(provider, fetch_windows)
    cache_dir = default_external_request_cache_dir(
        requests_module=provider.requests_module,
        explicit_cache_dir=provider.request_cache_dir,
    )
    latest_metadata = _latest_external_request_metadata(
        provider="Marketstack",
        api_family="eod_daily_prices",
        cache_dir=None if cache_dir is None else Path(cache_dir),
    )
    quota_limit = _metadata_header_int(latest_metadata, "x-quota-limit")
    quota_remaining = _metadata_header_int(latest_metadata, "x-quota-remaining")
    latest_observed_at = (
        str(latest_metadata.get("created_at"))
        if isinstance(latest_metadata, Mapping) and latest_metadata.get("created_at")
        else None
    )
    status = "NO_LIVE_REQUEST_NEEDED" if estimated_units == 0 else "PASS"
    owner_approved_overage: dict[str, object] | None = None
    if estimated_units > 0 and quota_remaining is not None and quota_remaining < estimated_units:
        owner_approved_overage = _marketstack_owner_approved_budget_status(
            estimated_units=estimated_units,
            fetch_windows=fetch_windows,
            quota_limit=quota_limit,
            quota_remaining=quota_remaining,
            approval_profile=approval_profile,
        )
        if owner_approved_overage["approved"]:
            status = str(owner_approved_overage["allowed_status"])
        else:
            status = _MARKETSTACK_BLOCKED_STATUS
    approval_status_key = (
        "owner_approved_tail_catch_up"
        if approval_profile == _MARKETSTACK_TAIL_CATCH_UP_PROFILE
        else "owner_approved_overage"
    )
    payload: dict[str, object] = {
        "provider": "Marketstack",
        "api_family": "eod_daily_prices",
        "budget_profile": approval_profile,
        "status": status,
        "estimated_increment_usage": estimated_units,
        "quota_limit": quota_limit,
        "quota_remaining": quota_remaining,
        "latest_quota_observed_at": latest_observed_at,
        "fetch_window_count": len(fetch_windows),
    }
    if owner_approved_overage is not None:
        payload[approval_status_key] = owner_approved_overage
    if status == _MARKETSTACK_BLOCKED_STATUS and raise_on_block:
        _raise_provider_quota_budget_error(payload)
    return payload


def _marketstack_owner_approved_overage_status(
    *,
    estimated_units: int,
    fetch_windows: tuple[IncrementalPriceWindow, ...],
    quota_limit: int | None,
    quota_remaining: int,
) -> dict[str, object]:
    return _marketstack_owner_approved_budget_status(
        estimated_units=estimated_units,
        fetch_windows=fetch_windows,
        quota_limit=quota_limit,
        quota_remaining=quota_remaining,
        approval_profile=_MARKETSTACK_DAILY_OVERAGE_PROFILE,
    )


def _marketstack_owner_approved_budget_status(
    *,
    estimated_units: int,
    fetch_windows: tuple[IncrementalPriceWindow, ...],
    quota_limit: int | None,
    quota_remaining: int,
    approval_profile: str,
) -> dict[str, object]:
    policy = load_data_source_request_budget_policy()
    if approval_profile == _MARKETSTACK_DAILY_OVERAGE_PROFILE:
        approval = policy.marketstack.eod_daily_prices.owner_approved_overage
    elif approval_profile == _MARKETSTACK_TAIL_CATCH_UP_PROFILE:
        approval = policy.marketstack.eod_daily_prices.owner_approved_tail_catch_up
    else:
        raise ValueError(f"Unknown Marketstack approval profile: {approval_profile}")
    window_calendar_days = tuple(
        max(1, (window.end - window.start).days + 1) for window in fetch_windows
    )
    violation_reasons: list[str] = []
    if not approval.enabled:
        violation_reasons.append("owner_approved_overage_disabled")
    if estimated_units <= 0:
        violation_reasons.append("no_live_request_needed")
    if estimated_units > approval.max_estimated_increment_usage:
        violation_reasons.append("estimated_usage_exceeds_owner_approved_limit")
    quota_shortfall = max(0, estimated_units - quota_remaining)
    quota_overage_ratio = (
        None
        if quota_limit is None or quota_limit <= 0
        else quota_shortfall / quota_limit
    )
    if quota_overage_ratio is None:
        violation_reasons.append("quota_limit_missing_for_owner_approved_overage")
    elif quota_overage_ratio > approval.max_quota_overage_ratio:
        violation_reasons.append("quota_overage_ratio_exceeds_owner_approved_limit")
    if len(fetch_windows) > approval.max_fetch_window_count:
        violation_reasons.append("fetch_window_count_exceeds_owner_approved_limit")
    if any(days > approval.max_calendar_days_per_window for days in window_calendar_days):
        violation_reasons.append("calendar_window_exceeds_owner_approved_limit")

    return {
        "approved": not violation_reasons,
        "approval_profile": approval_profile,
        "policy_version": policy.policy_version,
        "policy_status": policy.policy_metadata.status,
        "allowed_status": approval.allowed_status,
        "quota_shortfall": quota_shortfall,
        "quota_overage_ratio": quota_overage_ratio,
        "max_estimated_increment_usage": approval.max_estimated_increment_usage,
        "max_fetch_window_count": approval.max_fetch_window_count,
        "max_calendar_days_per_window": approval.max_calendar_days_per_window,
        "max_quota_overage_ratio": approval.max_quota_overage_ratio,
        "window_calendar_days": list(window_calendar_days),
        "violation_reasons": violation_reasons,
        "reason": approval.reason,
        "behavioral_impact": approval.behavioral_impact,
        "risk": approval.risk,
        "validation_coverage": approval.validation_coverage,
        "exit_condition": approval.exit_condition,
    }


def _raise_provider_quota_budget_error(payload: Mapping[str, object]) -> None:
    reasons = _budget_violation_reasons(payload)
    reason_suffix = ""
    if reasons:
        reason_suffix = ", violation_reasons=" + ",".join(reasons)
    raise ProviderQuotaBudgetError(
        "Marketstack quota preflight blocked download: "
        f"estimated_increment_usage={payload.get('estimated_increment_usage')}, "
        f"quota_remaining={payload.get('quota_remaining')}, "
        f"quota_limit={payload.get('quota_limit')}, "
        f"budget_profile={payload.get('budget_profile')}"
        f"{reason_suffix}",
        budget_status=payload,
    )


def _budget_violation_reasons(payload: Mapping[str, object]) -> list[str]:
    for key in (
        "quota_cycle_reset",
        "owner_approved_tail_catch_up",
        "owner_approved_overage",
    ):
        approval = payload.get(key)
        if isinstance(approval, Mapping):
            reasons = approval.get("violation_reasons")
            if isinstance(reasons, list):
                return [str(reason) for reason in reasons]
    return []


def _estimate_marketstack_increment_usage(
    provider: MarketstackPriceProvider,
    fetch_windows: tuple[IncrementalPriceWindow, ...],
) -> int:
    total = 0
    for window in fetch_windows:
        provider_symbols, _provider_to_ticker = provider._provider_symbols(list(window.tickers))
        if not provider_symbols:
            continue
        calendar_days = max(1, (window.end - window.start).days + 1)
        estimated_records = max(1, calendar_days * len(provider_symbols))
        estimated_pages = max(1, math.ceil(estimated_records / provider.page_limit))
        total += len(provider_symbols) * estimated_pages
    return total


def _latest_external_request_metadata(
    *,
    provider: str,
    api_family: str,
    cache_dir: Path | None,
) -> Mapping[str, Any] | None:
    if cache_dir is None:
        return None
    root = cache_dir / _safe_cache_path_token(provider) / _safe_cache_path_token(api_family)
    if not root.exists():
        return None
    latest: tuple[datetime, Mapping[str, Any]] | None = None
    for path in root.rglob("metadata.json"):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if not isinstance(payload, Mapping):
            continue
        if payload.get("provider") != provider or payload.get("api_family") != api_family:
            continue
        created_at_raw = payload.get("created_at")
        if not isinstance(created_at_raw, str):
            continue
        try:
            created_at = datetime.fromisoformat(created_at_raw)
        except ValueError:
            continue
        if latest is None or created_at > latest[0]:
            latest = (created_at, payload)
    return None if latest is None else latest[1]


def _metadata_header_int(metadata: Mapping[str, Any] | None, header_name: str) -> int | None:
    if not isinstance(metadata, Mapping):
        return None
    headers = metadata.get("response_headers")
    if not isinstance(headers, Mapping):
        return None
    wanted = header_name.lower()
    for key, value in headers.items():
        if str(key).lower() == wanted:
            try:
                return int(str(value))
            except ValueError:
                return None
    return None


def _safe_cache_path_token(value: str) -> str:
    token = "".join(char if char.isalnum() or char in {"-", "_"} else "_" for char in value)
    return token[:80] or "unknown"


def _request_cache_summary_records(
    events: list[ExternalRequestCacheEvent],
) -> list[dict[str, object]]:
    grouped: dict[tuple[str, str], list[ExternalRequestCacheEvent]] = {}
    for event in events:
        grouped.setdefault((event.provider, event.api_family), []).append(event)
    records: list[dict[str, object]] = []
    for (provider, api_family), group in sorted(grouped.items()):
        latest_quota_event = next(
            (
                event
                for event in reversed(group)
                if event.quota_limit is not None or event.quota_remaining is not None
            ),
            None,
        )
        records.append(
            {
                "provider": provider,
                "api_family": api_family,
                "cache_hits": sum(1 for event in group if event.from_cache),
                "cache_misses": sum(1 for event in group if not event.from_cache),
                "live_request_count": sum(1 for event in group if not event.from_cache),
                "increment_usage_sum": sum(event.increment_usage or 0 for event in group),
                "quota_limit": (
                    None if latest_quota_event is None else latest_quota_event.quota_limit
                ),
                "quota_remaining": (
                    None if latest_quota_event is None else latest_quota_event.quota_remaining
                ),
            }
        )
    return records


def _cache_summaries_for_provider(
    request_cache_summaries: tuple[dict[str, object], ...],
    provider_name: str,
) -> list[dict[str, object]]:
    return [
        dict(summary)
        for summary in request_cache_summaries
        if summary.get("provider") == provider_name
    ]


def _provider_diagnostic_from_error(error: BaseException) -> ProviderRequestDiagnostic | None:
    current: BaseException | None = error
    while current is not None:
        if isinstance(current, ProviderDownloadError):
            return current.diagnostic
        current = current.__cause__ or current.__context__
    return None


def _provider_budget_status_from_error(error: BaseException) -> Mapping[str, object] | None:
    current: BaseException | None = error
    while current is not None:
        if isinstance(current, ProviderQuotaBudgetError) and current.budget_status:
            return current.budget_status
        current = current.__cause__ or current.__context__
    return None


def _render_provider_budget_status(budget_status: Mapping[str, object]) -> list[str]:
    safe_status = json.dumps(
        budget_status,
        ensure_ascii=False,
        indent=2,
        sort_keys=True,
        default=str,
    )
    rows = [
        ("Provider", budget_status.get("provider", "")),
        ("API family", budget_status.get("api_family", "")),
        ("Status", budget_status.get("status", "")),
        ("Budget profile", budget_status.get("budget_profile", "")),
        ("Estimated increment usage", budget_status.get("estimated_increment_usage", "")),
        ("Quota limit", budget_status.get("quota_limit", "")),
        ("Quota remaining", budget_status.get("quota_remaining", "")),
        ("Fetch window count", budget_status.get("fetch_window_count", "")),
        ("Violation reasons", ", ".join(_budget_violation_reasons(budget_status))),
    ]
    lines = [
        "",
        "## Provider 请求预算",
        "",
        "| 字段 | 值 |",
        "|---|---|",
    ]
    lines.extend(f"| {key} | {_escape_markdown_table(value)} |" for key, value in rows)
    lines.extend(
        [
            "",
            "### 请求预算 JSON",
            "",
            "```json",
            safe_status,
            "```",
        ]
    )
    return lines


def _render_provider_diagnostic(diagnostic: ProviderRequestDiagnostic) -> list[str]:
    request_parameters = json.dumps(
        diagnostic.request_parameters,
        ensure_ascii=False,
        sort_keys=True,
    )
    rows = [
        ("Provider", diagnostic.provider),
        ("API family", diagnostic.api_family),
        ("Stage", diagnostic.stage),
        ("Method", diagnostic.method),
        ("Endpoint", diagnostic.endpoint),
        ("Cache status", diagnostic.cache_status),
        ("Cache key", diagnostic.cache_key or ""),
        (
            "Cache metadata path",
            "" if diagnostic.cache_metadata_path is None else str(diagnostic.cache_metadata_path),
        ),
        ("HTTP status", "" if diagnostic.http_status is None else str(diagnostic.http_status)),
        ("Provider error code", diagnostic.error_code or ""),
        (
            "Response body sha256",
            diagnostic.response_body_sha256 or "",
        ),
        (
            "Response body size bytes",
            (
                ""
                if diagnostic.response_body_size_bytes is None
                else str(diagnostic.response_body_size_bytes)
            ),
        ),
        (
            "Rows before failure",
            (
                ""
                if diagnostic.row_count_before_failure is None
                else str(diagnostic.row_count_before_failure)
            ),
        ),
        (
            "Attempt count",
            "" if diagnostic.attempt_count is None else str(diagnostic.attempt_count),
        ),
        (
            "Max attempts",
            "" if diagnostic.max_attempts is None else str(diagnostic.max_attempts),
        ),
        (
            "Timeout seconds",
            "" if diagnostic.timeout_seconds is None else str(diagnostic.timeout_seconds),
        ),
        ("Exception type", diagnostic.exception_type or ""),
        ("Exception message", diagnostic.exception_message or ""),
    ]
    lines = [
        "",
        "## Provider 诊断",
        "",
        "| 字段 | 值 |",
        "|---|---|",
    ]
    lines.extend(f"| {key} | {_escape_markdown_table(value)} |" for key, value in rows)
    lines.extend(
        [
            "",
            "### 脱敏请求参数",
            "",
            "```json",
            request_parameters,
            "```",
        ]
    )
    return lines


def _escape_markdown_table(value: object) -> str:
    return str(value).replace("|", "\\|").replace("\n", " ")


def write_download_manifest(
    output_dir: Path,
    records: tuple[dict[str, object], ...],
    filename: str = "download_manifest.csv",
) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / filename
    new_frame = pd.DataFrame(records)

    if output_path.exists():
        existing = pd.read_csv(output_path)
        new_frame = pd.concat([existing, new_frame], ignore_index=True)

    new_frame.to_csv(output_path, index=False)
    return output_path


def _manifest_record_for_prices(
    provider: PriceDataProvider,
    request: PriceRequest,
    output_path: Path,
    row_count: int,
    *,
    incremental_download: IncrementalPriceDownload | None = None,
    request_cache_summaries: tuple[dict[str, object], ...] = (),
) -> dict[str, object]:
    source_id, provider_name, endpoint = _price_provider_metadata(provider)
    request_parameters: dict[str, object] = {
        "tickers": request.tickers,
        "start": request.start.isoformat(),
        "end": request.end.isoformat(),
        "interval": request.interval,
    }
    provider_symbol_aliases = _price_provider_symbol_aliases(provider, request.tickers)
    if provider_symbol_aliases:
        request_parameters["provider_symbol_aliases"] = provider_symbol_aliases
    if incremental_download is not None:
        request_parameters["incremental_refresh"] = {
            "mode": "coverage_gap",
            "supported_tickers": list(incremental_download.supported_tickers),
            "skipped_tickers": list(incremental_download.skipped_tickers),
            "fetch_windows": [
                window.to_payload() for window in incremental_download.fetch_windows
            ],
            "fetch_window_count": len(incremental_download.fetch_windows),
            "reused_row_count": incremental_download.reused_row_count,
            "fetched_row_count": incremental_download.fetched_row_count,
            "output_row_count": incremental_download.output_row_count,
            "request_budget_status": incremental_download.request_budget_status,
        }
    cache_summaries = _cache_summaries_for_provider(request_cache_summaries, provider_name)
    if cache_summaries:
        request_parameters["external_request_cache_summary"] = cache_summaries
    return _manifest_record(
        source_id=source_id,
        provider=provider_name,
        endpoint=endpoint,
        request_parameters=request_parameters,
        output_path=output_path,
        row_count=row_count,
    )


def _manifest_record_for_rates(
    provider: RateDataProvider,
    request: RateRequest,
    output_path: Path,
    row_count: int,
    *,
    request_cache_summaries: tuple[dict[str, object], ...] = (),
) -> dict[str, object]:
    source_id, provider_name, endpoint = _rate_provider_metadata(provider)
    request_parameters: dict[str, object] = {
        "series_ids": request.series_ids,
        "start": request.start.isoformat(),
        "end": request.end.isoformat(),
    }
    cache_summaries = _cache_summaries_for_provider(request_cache_summaries, provider_name)
    if cache_summaries:
        request_parameters["external_request_cache_summary"] = cache_summaries
    return _manifest_record(
        source_id=source_id,
        provider=provider_name,
        endpoint=endpoint,
        request_parameters=request_parameters,
        output_path=output_path,
        row_count=row_count,
    )


def _manifest_record(
    source_id: str,
    provider: str,
    endpoint: str,
    request_parameters: dict[str, object],
    output_path: Path,
    row_count: int,
) -> dict[str, object]:
    return {
        "downloaded_at": datetime.now(tz=UTC).isoformat(),
        "source_id": source_id,
        "provider": provider,
        "endpoint": endpoint,
        "request_parameters": json.dumps(
            request_parameters,
            ensure_ascii=False,
            sort_keys=True,
        ),
        "output_path": str(output_path),
        "row_count": row_count,
        "checksum_sha256": _sha256_file(output_path),
    }


def _price_provider_metadata(provider: PriceDataProvider) -> tuple[str, str, str]:
    if isinstance(provider, CboeVixPriceProvider):
        return ("cboe_vix_daily_prices", "Cboe Global Markets", provider.base_url)
    if isinstance(provider, FmpPriceProvider):
        return ("fmp_eod_daily_prices", "Financial Modeling Prep", provider.endpoint_summary())
    if isinstance(provider, YFinancePriceProvider):
        return ("yahoo_finance_daily_prices", "Yahoo Finance via yfinance", "yfinance.download")
    if isinstance(provider, MarketstackPriceProvider):
        return ("marketstack_eod_daily_prices", "Marketstack", provider.base_url)
    provider_name = provider.__class__.__name__
    return (_source_id_from_provider(provider_name), provider_name, provider_name)


def _price_provider_symbol_aliases(
    provider: PriceDataProvider,
    tickers: list[str],
) -> dict[str, str | None]:
    if isinstance(provider, FmpPriceProvider):
        return {
            ticker: provider.provider_symbol_for(ticker)
            for ticker in tickers
            if provider.provider_symbol_for(ticker) != ticker
        }
    return {}


def _rate_provider_metadata(provider: RateDataProvider) -> tuple[str, str, str]:
    if isinstance(provider, FredRateProvider):
        return ("fred_daily_rates", "Federal Reserve Economic Data", provider.base_url)
    provider_name = provider.__class__.__name__
    return (_source_id_from_provider(provider_name), provider_name, provider_name)


def _source_id_from_provider(provider_name: str) -> str:
    snake_name = re.sub(r"(?<!^)(?=[A-Z])", "_", provider_name).lower()
    normalized = re.sub(r"[^a-z0-9]+", "_", snake_name).strip("_")
    return normalized or "unknown_provider"


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()
