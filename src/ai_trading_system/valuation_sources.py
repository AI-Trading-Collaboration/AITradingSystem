from __future__ import annotations

import csv
import hashlib
import json
import math
import re
import time
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from importlib import import_module
from pathlib import Path
from typing import Any, Protocol, cast

import yaml
from pydantic import ValidationError

from ai_trading_system.valuation import (
    CrowdingSignal,
    CrowdingStatus,
    LoadedValuationSnapshot,
    SnapshotMetric,
    ValuationIssueSeverity,
    ValuationSnapshot,
    load_valuation_snapshot_store,
)

REQUIRED_COLUMNS = frozenset(
    {
        "snapshot_id",
        "ticker",
        "as_of",
        "source_type",
        "source_name",
        "captured_at",
    }
)
OPTIONAL_COLUMNS = frozenset(
    {
        "source_url",
        "point_in_time_class",
        "history_source_class",
        "confidence_level",
        "confidence_reason",
        "backtest_use",
        "valuation_percentile",
        "overall_assessment",
        "notes",
        "crowding_status",
        "crowding_signal_name",
        "crowding_evidence_source",
        "crowding_updated_at",
        "crowding_notes",
    }
)
METRIC_COLUMNS = frozenset(
    {
        "forward_pe",
        "ev_sales",
        "peg",
        "revenue_growth_next_12m_pct",
        "eps_revision_90d_pct",
    }
)
KNOWN_COLUMNS = REQUIRED_COLUMNS | OPTIONAL_COLUMNS | METRIC_COLUMNS
VALID_CROWDING_STATUSES = frozenset({"normal", "elevated", "extreme", "unknown"})

_VALUATION_METRIC_DEFINITIONS = {
    "forward_pe": ("ratio", "next_12m"),
    "ev_sales": ("ratio", "next_12m"),
    "peg": ("ratio", "next_12m"),
}
_EXPECTATION_METRIC_DEFINITIONS = {
    "revenue_growth_next_12m_pct": ("percent", "next_12m"),
    "eps_revision_90d_pct": ("percent", "trailing_90d"),
}
FMP_SOURCE_NAME = "Financial Modeling Prep"
FMP_BASE_URL = "https://financialmodelingprep.com/stable"
FMP_ENDPOINTS = (
    "quote-short",
    "key-metrics-ttm",
    "ratios-ttm",
    "analyst-estimates",
)
FMP_HISTORICAL_VALUATION_ENDPOINTS = (
    "key-metrics",
    "ratios",
)
FMP_SYMBOL_ALIASES = {
    "GOOG": "GOOGL",
}
FMP_ANALYST_ESTIMATE_PERIOD = "annual"
FMP_ANALYST_ESTIMATE_PAGE = 0
EODHD_SOURCE_NAME = "EODHD Earnings Trends"
EODHD_BASE_URL = "https://eodhd.com/api"
EODHD_EARNINGS_TRENDS_ENDPOINT = "calendar/trends"
EODHD_SYMBOL_ALIASES: dict[str, str] = {}
DEFAULT_EPS_REVISION_LOOKBACK_DAYS = 90
DEFAULT_EPS_REVISION_TOLERANCE_DAYS = 15
DEFAULT_MINIMUM_VALUATION_HISTORY_POINTS = 3


@dataclass(frozen=True)
class ValuationImportIssue:
    severity: ValuationIssueSeverity
    code: str
    message: str
    row_number: int | None = None
    snapshot_id: str | None = None
    ticker: str | None = None
    column: str | None = None
    path: Path | None = None


@dataclass(frozen=True)
class ValuationCsvImportReport:
    source_path: Path
    row_count: int
    checksum_sha256: str
    snapshots: tuple[ValuationSnapshot, ...]
    issues: tuple[ValuationImportIssue, ...] = field(default_factory=tuple)

    @property
    def imported_count(self) -> int:
        return len(self.snapshots)

    @property
    def error_count(self) -> int:
        return sum(1 for issue in self.issues if issue.severity == ValuationIssueSeverity.ERROR)

    @property
    def warning_count(self) -> int:
        return sum(1 for issue in self.issues if issue.severity == ValuationIssueSeverity.WARNING)

    @property
    def passed(self) -> bool:
        return self.error_count == 0

    @property
    def status(self) -> str:
        if self.error_count:
            return "FAIL"
        if self.warning_count:
            return "PASS_WITH_WARNINGS"
        return "PASS"


@dataclass(frozen=True)
class FmpValuationFetchIssue:
    severity: ValuationIssueSeverity
    code: str
    message: str
    ticker: str | None = None
    endpoint: str | None = None


@dataclass(frozen=True)
class FmpAnalystEstimateHistorySnapshot:
    ticker: str
    as_of: date
    captured_at: date
    downloaded_at: datetime
    endpoint: str
    request_parameters: dict[str, object]
    row_count: int
    checksum_sha256: str
    records: tuple[dict[str, Any], ...]
    source_path: Path | None = None


@dataclass(frozen=True)
class FmpAnalystHistoryValidationIssue:
    severity: ValuationIssueSeverity
    code: str
    message: str
    path: Path | None = None
    ticker: str | None = None
    estimate_date: date | None = None


@dataclass(frozen=True)
class FmpAnalystHistoryValidationReport:
    as_of: date
    input_path: Path
    snapshots: tuple[FmpAnalystEstimateHistorySnapshot, ...]
    issues: tuple[FmpAnalystHistoryValidationIssue, ...] = field(default_factory=tuple)

    @property
    def snapshot_count(self) -> int:
        return len(self.snapshots)

    @property
    def ticker_count(self) -> int:
        return len({snapshot.ticker for snapshot in self.snapshots})

    @property
    def record_count(self) -> int:
        return sum(snapshot.row_count for snapshot in self.snapshots)

    @property
    def error_count(self) -> int:
        return sum(1 for issue in self.issues if issue.severity == ValuationIssueSeverity.ERROR)

    @property
    def warning_count(self) -> int:
        return sum(1 for issue in self.issues if issue.severity == ValuationIssueSeverity.WARNING)

    @property
    def passed(self) -> bool:
        return self.error_count == 0

    @property
    def status(self) -> str:
        if self.error_count:
            return "FAIL"
        if self.warning_count:
            return "PASS_WITH_WARNINGS"
        return "PASS"


@dataclass(frozen=True)
class FmpValuationFetchReport:
    as_of: date
    captured_at: date
    downloaded_at: datetime
    requested_tickers: tuple[str, ...]
    analyst_estimate_limit: int
    analyst_history_input_path: Path | None
    pit_normalized_input_path: Path | None
    historical_analyst_snapshot_count: int
    pit_analyst_snapshot_count: int
    valuation_history_input_path: Path | None
    historical_valuation_snapshot_count: int
    minimum_valuation_history_points: int
    analyst_estimate_history_snapshots: tuple[FmpAnalystEstimateHistorySnapshot, ...]
    row_count: int
    checksum_sha256: str
    snapshots: tuple[ValuationSnapshot, ...]
    issues: tuple[FmpValuationFetchIssue, ...] = field(default_factory=tuple)
    source_name: str = FMP_SOURCE_NAME
    source_type: str = "paid_vendor"

    @property
    def imported_count(self) -> int:
        return len(self.snapshots)

    @property
    def error_count(self) -> int:
        return sum(1 for issue in self.issues if issue.severity == ValuationIssueSeverity.ERROR)

    @property
    def warning_count(self) -> int:
        return sum(1 for issue in self.issues if issue.severity == ValuationIssueSeverity.WARNING)

    @property
    def passed(self) -> bool:
        return self.error_count == 0

    @property
    def status(self) -> str:
        if self.error_count:
            return "FAIL"
        if self.warning_count:
            return "PASS_WITH_WARNINGS"
        return "PASS"


@dataclass(frozen=True)
class FmpHistoricalValuationRawPayload:
    ticker: str
    as_of: date
    captured_at: date
    downloaded_at: datetime
    period: str
    limit: int
    endpoint_records: dict[str, tuple[dict[str, Any], ...]]
    request_parameters_by_endpoint: dict[str, dict[str, object]]
    checksum_sha256: str
    source_path: Path | None = None

    @property
    def row_count(self) -> int:
        return sum(len(records) for records in self.endpoint_records.values())


@dataclass(frozen=True)
class FmpHistoricalValuationFetchReport:
    as_of: date
    captured_at: date
    downloaded_at: datetime
    requested_tickers: tuple[str, ...]
    period: str
    limit: int
    raw_payloads: tuple[FmpHistoricalValuationRawPayload, ...]
    row_count: int
    checksum_sha256: str
    snapshots: tuple[ValuationSnapshot, ...]
    issues: tuple[FmpValuationFetchIssue, ...] = field(default_factory=tuple)
    source_name: str = FMP_SOURCE_NAME
    source_type: str = "paid_vendor"

    @property
    def imported_count(self) -> int:
        return len(self.snapshots)

    @property
    def error_count(self) -> int:
        return sum(1 for issue in self.issues if issue.severity == ValuationIssueSeverity.ERROR)

    @property
    def warning_count(self) -> int:
        return sum(1 for issue in self.issues if issue.severity == ValuationIssueSeverity.WARNING)

    @property
    def passed(self) -> bool:
        return self.error_count == 0

    @property
    def status(self) -> str:
        if self.error_count:
            return "FAIL"
        if self.warning_count:
            return "PASS_WITH_WARNINGS"
        return "PASS"


@dataclass(frozen=True)
class EodhdEarningsTrendsRawPayload:
    as_of: date
    captured_at: date
    downloaded_at: datetime
    requested_tickers: tuple[str, ...]
    provider_symbols: tuple[str, ...]
    endpoint: str
    request_parameters: dict[str, object]
    records_by_ticker: dict[str, tuple[dict[str, Any], ...]]
    checksum_sha256: str
    source_path: Path | None = None

    @property
    def row_count(self) -> int:
        return sum(len(records) for records in self.records_by_ticker.values())


@dataclass(frozen=True)
class EodhdEarningsTrendsFetchReport:
    as_of: date
    captured_at: date
    downloaded_at: datetime
    requested_tickers: tuple[str, ...]
    provider_symbols: tuple[str, ...]
    base_valuation_input_path: Path | None
    base_valuation_snapshot_count: int
    raw_payload: EodhdEarningsTrendsRawPayload | None
    row_count: int
    checksum_sha256: str
    snapshots: tuple[ValuationSnapshot, ...]
    issues: tuple[FmpValuationFetchIssue, ...] = field(default_factory=tuple)
    source_name: str = EODHD_SOURCE_NAME
    source_type: str = "paid_vendor"

    @property
    def imported_count(self) -> int:
        return len(self.snapshots)

    @property
    def error_count(self) -> int:
        return sum(1 for issue in self.issues if issue.severity == ValuationIssueSeverity.ERROR)

    @property
    def warning_count(self) -> int:
        return sum(1 for issue in self.issues if issue.severity == ValuationIssueSeverity.WARNING)

    @property
    def passed(self) -> bool:
        return self.error_count == 0

    @property
    def status(self) -> str:
        if self.error_count:
            return "FAIL"
        if self.warning_count:
            return "PASS_WITH_WARNINGS"
        return "PASS"


class FmpValuationProvider(Protocol):
    def fetch_quote_short(self, ticker: str) -> list[dict[str, Any]]:
        """Fetch current quote records for one ticker."""

    def fetch_key_metrics_ttm(self, ticker: str) -> list[dict[str, Any]]:
        """Fetch TTM key metrics records for one ticker."""

    def fetch_ratios_ttm(self, ticker: str) -> list[dict[str, Any]]:
        """Fetch TTM ratio records for one ticker."""

    def fetch_analyst_estimates(
        self,
        ticker: str,
        *,
        period: str,
        limit: int,
    ) -> list[dict[str, Any]]:
        """Fetch analyst estimate records for one ticker."""


class FmpHistoricalValuationProvider(Protocol):
    def fetch_key_metrics(
        self,
        ticker: str,
        *,
        period: str,
        limit: int,
    ) -> list[dict[str, Any]]:
        """Fetch historical key metric records for one ticker."""

    def fetch_ratios(
        self,
        ticker: str,
        *,
        period: str,
        limit: int,
    ) -> list[dict[str, Any]]:
        """Fetch historical ratio records for one ticker."""


class EodhdEarningsTrendsProvider(Protocol):
    def fetch_earnings_trends(self, provider_symbols: tuple[str, ...]) -> dict[str, Any]:
        """Fetch EODHD earnings trends records for one or more provider symbols."""


class FmpHttpValuationProvider:
    def __init__(
        self,
        api_key: str,
        *,
        base_url: str = FMP_BASE_URL,
        timeout: int = 30,
        max_attempts: int = 3,
        retry_backoff_seconds: float = 0.5,
        requests_module: Any | None = None,
    ) -> None:
        if not api_key.strip():
            raise ValueError("FMP API key must not be empty")
        if max_attempts < 1:
            raise ValueError("FMP max_attempts must be at least 1")
        if retry_backoff_seconds < 0:
            raise ValueError("FMP retry_backoff_seconds must be non-negative")
        self._api_key = api_key.strip()
        self._base_url = base_url.rstrip("/")
        self._timeout = timeout
        self._max_attempts = max_attempts
        self._retry_backoff_seconds = retry_backoff_seconds
        self._requests_module = requests_module

    def fetch_quote_short(self, ticker: str) -> list[dict[str, Any]]:
        return self._get("quote-short", {"symbol": ticker})

    def fetch_key_metrics_ttm(self, ticker: str) -> list[dict[str, Any]]:
        return self._get("key-metrics-ttm", {"symbol": ticker})

    def fetch_ratios_ttm(self, ticker: str) -> list[dict[str, Any]]:
        return self._get("ratios-ttm", {"symbol": ticker})

    def fetch_key_metrics(
        self,
        ticker: str,
        *,
        period: str,
        limit: int,
    ) -> list[dict[str, Any]]:
        return self._get(
            "key-metrics",
            {"symbol": ticker, "period": period, "limit": limit},
        )

    def fetch_ratios(
        self,
        ticker: str,
        *,
        period: str,
        limit: int,
    ) -> list[dict[str, Any]]:
        return self._get(
            "ratios",
            {"symbol": ticker, "period": period, "limit": limit},
        )

    def fetch_analyst_estimates(
        self,
        ticker: str,
        *,
        period: str,
        limit: int,
    ) -> list[dict[str, Any]]:
        return self._get(
            "analyst-estimates",
            {
                "symbol": ticker,
                "period": period,
                "page": 0,
                "limit": limit,
            },
        )

    def fetch_price_target_summary(self, ticker: str) -> list[dict[str, Any]]:
        return self._get("price-target-summary", {"symbol": ticker})

    def fetch_price_target_consensus(self, ticker: str) -> list[dict[str, Any]]:
        return self._get("price-target-consensus", {"symbol": ticker})

    def fetch_grades(self, ticker: str) -> list[dict[str, Any]]:
        return self._get("grades", {"symbol": ticker})

    def fetch_grades_consensus(self, ticker: str) -> list[dict[str, Any]]:
        return self._get("grades-consensus", {"symbol": ticker})

    def fetch_ratings_snapshot(self, ticker: str) -> list[dict[str, Any]]:
        return self._get("ratings-snapshot", {"symbol": ticker})

    def fetch_earnings_calendar(
        self,
        *,
        from_date: date,
        to_date: date,
    ) -> list[dict[str, Any]]:
        return self._get(
            "earnings-calendar",
            {"from": from_date.isoformat(), "to": to_date.isoformat()},
        )

    def endpoint_for(self, endpoint: str) -> str:
        return f"{self._base_url}/{endpoint}"

    def _get(self, endpoint: str, params: dict[str, object]) -> list[dict[str, Any]]:
        requests = self._requests_module or cast(Any, import_module("requests"))
        request_params = {**params, "apikey": self._api_key}
        response: Any | None = None
        for attempt in range(1, self._max_attempts + 1):
            try:
                response = requests.get(
                    self.endpoint_for(endpoint),
                    params=request_params,
                    timeout=self._timeout,
                )
                response.raise_for_status()
                break
            except Exception as exc:
                if not _should_retry_fmp_http_error(exc) or attempt >= self._max_attempts:
                    raise
                if self._retry_backoff_seconds:
                    time.sleep(self._retry_backoff_seconds * attempt)
        if response is None:
            raise RuntimeError(f"FMP {endpoint} request did not return a response")
        data = response.json()
        if isinstance(data, list):
            if not all(isinstance(item, dict) for item in data):
                raise TypeError(f"FMP {endpoint} response contained non-object rows")
            return cast(list[dict[str, Any]], data)
        if isinstance(data, dict):
            if "Error Message" in data or "error" in data:
                message = str(data.get("Error Message") or data.get("error"))
                raise ValueError(f"FMP {endpoint} returned an error: {message}")
            return [cast(dict[str, Any], data)]
        raise TypeError(f"FMP {endpoint} response was not JSON object/list")


class EodhdHttpEarningsTrendsProvider:
    def __init__(
        self,
        api_key: str,
        *,
        base_url: str = EODHD_BASE_URL,
        timeout: int = 30,
        max_attempts: int = 3,
        retry_backoff_seconds: float = 0.5,
        requests_module: Any | None = None,
    ) -> None:
        if not api_key.strip():
            raise ValueError("EODHD API key must not be empty")
        if max_attempts < 1:
            raise ValueError("EODHD max_attempts must be at least 1")
        if retry_backoff_seconds < 0:
            raise ValueError("EODHD retry_backoff_seconds must be non-negative")
        self._api_key = api_key.strip()
        self._base_url = base_url.rstrip("/")
        self._timeout = timeout
        self._max_attempts = max_attempts
        self._retry_backoff_seconds = retry_backoff_seconds
        self._requests_module = requests_module

    def fetch_earnings_trends(self, provider_symbols: tuple[str, ...]) -> dict[str, Any]:
        return self._get(
            EODHD_EARNINGS_TRENDS_ENDPOINT,
            {
                "symbols": ",".join(provider_symbols),
                "fmt": "json",
            },
        )

    def endpoint_for(self, endpoint: str) -> str:
        return f"{self._base_url}/{endpoint}"

    def _get(self, endpoint: str, params: dict[str, object]) -> dict[str, Any]:
        requests = self._requests_module or cast(Any, import_module("requests"))
        request_params = {**params, "api_token": self._api_key}
        response: Any | None = None
        for attempt in range(1, self._max_attempts + 1):
            try:
                response = requests.get(
                    self.endpoint_for(endpoint),
                    params=request_params,
                    timeout=self._timeout,
                )
                response.raise_for_status()
                break
            except Exception as exc:
                if not _should_retry_fmp_http_error(exc) or attempt >= self._max_attempts:
                    raise
                if self._retry_backoff_seconds:
                    time.sleep(self._retry_backoff_seconds * attempt)
        if response is None:
            raise RuntimeError(f"EODHD {endpoint} request did not return a response")
        data = response.json()
        if isinstance(data, dict):
            if "Error Message" in data or "error" in data:
                message = str(data.get("Error Message") or data.get("error"))
                raise ValueError(f"EODHD {endpoint} returned an error: {message}")
            return cast(dict[str, Any], data)
        raise TypeError(f"EODHD {endpoint} response was not a JSON object")


def fetch_fmp_valuation_snapshots(
    tickers: list[str] | tuple[str, ...],
    api_key: str,
    as_of: date,
    *,
    provider: FmpValuationProvider | None = None,
    analyst_history_dir: Path | str | None = None,
    pit_normalized_path: Path | str | None = None,
    valuation_history_dir: Path | str | None = None,
    captured_at: date | None = None,
    downloaded_at: datetime | None = None,
    analyst_estimate_limit: int = 10,
    eps_revision_lookback_days: int = DEFAULT_EPS_REVISION_LOOKBACK_DAYS,
    eps_revision_tolerance_days: int = DEFAULT_EPS_REVISION_TOLERANCE_DAYS,
    minimum_valuation_history_points: int = DEFAULT_MINIMUM_VALUATION_HISTORY_POINTS,
) -> FmpValuationFetchReport:
    normalized_tickers = tuple(_normalize_tickers(tickers))
    if not normalized_tickers:
        raise ValueError("FMP valuation fetch requires at least one ticker")
    if not api_key.strip():
        raise ValueError("FMP API key must not be empty")
    if analyst_estimate_limit < 2:
        raise ValueError("analyst_estimate_limit must be at least 2")
    if eps_revision_lookback_days <= 0:
        raise ValueError("eps_revision_lookback_days must be positive")
    if eps_revision_tolerance_days < 0:
        raise ValueError("eps_revision_tolerance_days must be non-negative")
    if minimum_valuation_history_points <= 0:
        raise ValueError("minimum_valuation_history_points must be positive")

    fetch_provider = provider or FmpHttpValuationProvider(api_key)
    download_time = downloaded_at or datetime.now(tz=UTC)
    fetch_date = captured_at or download_time.date()
    history_path = Path(analyst_history_dir) if analyst_history_dir is not None else None
    legacy_historical_estimates = (
        load_fmp_analyst_estimate_history_snapshots(history_path, normalized_tickers)
        if history_path is not None
        else ()
    )
    pit_path = Path(pit_normalized_path) if pit_normalized_path is not None else None
    pit_historical_estimates = (
        load_fmp_forward_pit_analyst_estimate_history(
            pit_path,
            tickers=normalized_tickers,
            decision_time=_daily_decision_time(as_of),
        )
        if pit_path is not None
        else ()
    )
    historical_estimates = (
        pit_historical_estimates if pit_path is not None else legacy_historical_estimates
    )
    issues: list[FmpValuationFetchIssue] = []
    valuation_history_path = (
        Path(valuation_history_dir) if valuation_history_dir is not None else None
    )
    valuation_history = (
        _load_fmp_valuation_history(
            valuation_history_path,
            normalized_tickers,
            issues,
        )
        if valuation_history_path is not None
        else ()
    )
    snapshots: list[ValuationSnapshot] = []
    current_analyst_history: list[FmpAnalystEstimateHistorySnapshot] = []
    raw_payload: dict[str, dict[str, list[dict[str, Any]]]] = {}
    row_count = 0

    for ticker in normalized_tickers:
        provider_symbol = _fmp_provider_symbol(ticker)
        try:
            ticker_payload = {
                "quote-short": fetch_provider.fetch_quote_short(provider_symbol),
                "key-metrics-ttm": fetch_provider.fetch_key_metrics_ttm(provider_symbol),
                "ratios-ttm": fetch_provider.fetch_ratios_ttm(provider_symbol),
                "analyst-estimates": fetch_provider.fetch_analyst_estimates(
                    provider_symbol,
                    period=FMP_ANALYST_ESTIMATE_PERIOD,
                    limit=analyst_estimate_limit,
                ),
            }
        except Exception as exc:
            issues.append(
                FmpValuationFetchIssue(
                    severity=ValuationIssueSeverity.ERROR,
                    code="fmp_request_failed",
                    ticker=ticker,
                    message=f"FMP 请求失败：{_sanitize_fmp_error_message(exc)}",
                )
            )
            continue

        raw_payload[ticker] = ticker_payload
        row_count += sum(len(rows) for rows in ticker_payload.values())
        analyst_rows = tuple(ticker_payload["analyst-estimates"])
        current_analyst_history.append(
            _build_fmp_analyst_history_snapshot(
                ticker=ticker,
                as_of=as_of,
                captured_at=fetch_date,
                downloaded_at=download_time,
                limit=analyst_estimate_limit,
                provider_symbol=provider_symbol,
                records=analyst_rows,
            )
        )
        snapshot = _fmp_snapshot_from_payload(
            ticker=ticker,
            as_of=as_of,
            captured_at=fetch_date,
            payload=ticker_payload,
            historical_estimates=historical_estimates,
            valuation_history=valuation_history,
            eps_revision_lookback_days=eps_revision_lookback_days,
            eps_revision_tolerance_days=eps_revision_tolerance_days,
            minimum_valuation_history_points=minimum_valuation_history_points,
            issues=issues,
        )
        if snapshot is not None:
            snapshots.append(snapshot)

    checksum = _json_checksum(raw_payload)
    return FmpValuationFetchReport(
        as_of=as_of,
        captured_at=fetch_date,
        downloaded_at=download_time,
        requested_tickers=normalized_tickers,
        analyst_estimate_limit=analyst_estimate_limit,
        analyst_history_input_path=history_path,
        pit_normalized_input_path=pit_path,
        historical_analyst_snapshot_count=len(historical_estimates),
        pit_analyst_snapshot_count=len(pit_historical_estimates),
        valuation_history_input_path=valuation_history_path,
        historical_valuation_snapshot_count=len(valuation_history),
        minimum_valuation_history_points=minimum_valuation_history_points,
        analyst_estimate_history_snapshots=tuple(current_analyst_history),
        row_count=row_count,
        checksum_sha256=checksum,
        snapshots=tuple(snapshots),
        issues=tuple(issues),
    )


def fetch_fmp_historical_valuation_snapshots(
    tickers: list[str] | tuple[str, ...],
    api_key: str,
    as_of: date,
    *,
    provider: FmpHistoricalValuationProvider | None = None,
    captured_at: date | None = None,
    downloaded_at: datetime | None = None,
    period: str = "annual",
    limit: int = 5,
) -> FmpHistoricalValuationFetchReport:
    normalized_tickers = tuple(_normalize_tickers(tickers))
    if not normalized_tickers:
        raise ValueError("FMP historical valuation fetch requires at least one ticker")
    if not api_key.strip():
        raise ValueError("FMP API key must not be empty")
    normalized_period = period.strip().lower()
    if normalized_period not in {"annual", "quarter"}:
        raise ValueError("FMP historical valuation period must be annual or quarter")
    if limit < 3:
        raise ValueError("FMP historical valuation limit must be at least 3")

    fetch_provider = provider or FmpHttpValuationProvider(api_key)
    download_time = downloaded_at or datetime.now(tz=UTC)
    fetch_date = captured_at or download_time.date()
    issues: list[FmpValuationFetchIssue] = []
    raw_payloads: list[FmpHistoricalValuationRawPayload] = []
    snapshots: list[ValuationSnapshot] = []
    raw_payload_for_checksum: dict[str, dict[str, list[dict[str, Any]]]] = {}

    for ticker in normalized_tickers:
        provider_symbol = _fmp_provider_symbol(ticker)
        try:
            endpoint_records = {
                "key-metrics": tuple(
                    fetch_provider.fetch_key_metrics(
                        provider_symbol,
                        period=normalized_period,
                        limit=limit,
                    )
                ),
                "ratios": tuple(
                    fetch_provider.fetch_ratios(
                        provider_symbol,
                        period=normalized_period,
                        limit=limit,
                    )
                ),
            }
        except Exception as exc:
            issues.append(
                FmpValuationFetchIssue(
                    severity=ValuationIssueSeverity.ERROR,
                    code="fmp_historical_request_failed",
                    ticker=ticker,
                    message=f"FMP 历史估值请求失败：{_sanitize_fmp_error_message(exc)}",
                )
            )
            continue

        raw_payload_for_checksum[ticker] = {
            endpoint: list(records) for endpoint, records in endpoint_records.items()
        }
        request_parameters_by_endpoint = {
            endpoint: {
                "symbol": provider_symbol,
                "period": normalized_period,
                "limit": limit,
            }
            for endpoint in FMP_HISTORICAL_VALUATION_ENDPOINTS
        }
        raw_payloads.append(
            FmpHistoricalValuationRawPayload(
                ticker=ticker,
                as_of=as_of,
                captured_at=fetch_date,
                downloaded_at=download_time,
                period=normalized_period,
                limit=limit,
                endpoint_records=endpoint_records,
                request_parameters_by_endpoint=request_parameters_by_endpoint,
                checksum_sha256=_json_checksum(endpoint_records),
            )
        )
        ticker_snapshots = _fmp_historical_snapshots_from_payload(
            ticker=ticker,
            captured_at=fetch_date,
            downloaded_at=download_time,
            period=normalized_period,
            endpoint_records=endpoint_records,
            issues=issues,
        )
        if not ticker_snapshots:
            issues.append(
                FmpValuationFetchIssue(
                    severity=ValuationIssueSeverity.WARNING,
                    code="missing_fmp_historical_valuation_metrics",
                    ticker=ticker,
                    message="FMP historical key-metrics/ratios 未生成可用历史估值快照。",
                )
            )
        snapshots.extend(ticker_snapshots)

    checksum = _json_checksum(raw_payload_for_checksum)
    return FmpHistoricalValuationFetchReport(
        as_of=as_of,
        captured_at=fetch_date,
        downloaded_at=download_time,
        requested_tickers=normalized_tickers,
        period=normalized_period,
        limit=limit,
        raw_payloads=tuple(raw_payloads),
        row_count=sum(payload.row_count for payload in raw_payloads),
        checksum_sha256=checksum,
        snapshots=tuple(sorted(snapshots, key=lambda item: item.snapshot_id)),
        issues=tuple(issues),
    )


def fetch_eodhd_earnings_trend_snapshots(
    tickers: list[str] | tuple[str, ...],
    api_key: str,
    as_of: date,
    *,
    provider: EodhdEarningsTrendsProvider | None = None,
    base_valuation_dir: Path | str | None = None,
    captured_at: date | None = None,
    downloaded_at: datetime | None = None,
) -> EodhdEarningsTrendsFetchReport:
    normalized_tickers = tuple(_normalize_tickers(tickers))
    if not normalized_tickers:
        raise ValueError("EODHD earnings trends fetch requires at least one ticker")
    if not api_key.strip():
        raise ValueError("EODHD API key must not be empty")

    provider_symbols = tuple(_eodhd_provider_symbol(ticker) for ticker in normalized_tickers)
    fetch_provider = provider or EodhdHttpEarningsTrendsProvider(api_key)
    download_time = downloaded_at or datetime.now(tz=UTC)
    fetch_date = captured_at or download_time.date()
    issues: list[FmpValuationFetchIssue] = []
    base_path = Path(base_valuation_dir) if base_valuation_dir is not None else None
    base_snapshots = (
        _latest_visible_base_valuation_snapshots(base_path, normalized_tickers, as_of, issues)
        if base_path is not None
        else {}
    )

    try:
        raw_response = fetch_provider.fetch_earnings_trends(provider_symbols)
    except Exception as exc:
        issue = FmpValuationFetchIssue(
            severity=ValuationIssueSeverity.ERROR,
            code="eodhd_earnings_trends_request_failed",
            endpoint=EODHD_EARNINGS_TRENDS_ENDPOINT,
            message=f"EODHD Earnings Trends 请求失败：{_sanitize_eodhd_error_message(exc)}",
        )
        return EodhdEarningsTrendsFetchReport(
            as_of=as_of,
            captured_at=fetch_date,
            downloaded_at=download_time,
            requested_tickers=normalized_tickers,
            provider_symbols=provider_symbols,
            base_valuation_input_path=base_path,
            base_valuation_snapshot_count=len(base_snapshots),
            raw_payload=None,
            row_count=0,
            checksum_sha256=_json_checksum({}),
            snapshots=(),
            issues=(issue,),
        )

    records_by_ticker = _eodhd_trend_records_by_ticker(
        raw_response=raw_response,
        tickers=normalized_tickers,
        provider_symbols=provider_symbols,
        issues=issues,
    )
    request_parameters = {
        "symbols": ",".join(provider_symbols),
        "fmt": "json",
    }
    raw_payload = EodhdEarningsTrendsRawPayload(
        as_of=as_of,
        captured_at=fetch_date,
        downloaded_at=download_time,
        requested_tickers=normalized_tickers,
        provider_symbols=provider_symbols,
        endpoint=f"{EODHD_BASE_URL}/{EODHD_EARNINGS_TRENDS_ENDPOINT}",
        request_parameters=request_parameters,
        records_by_ticker=records_by_ticker,
        checksum_sha256=_json_checksum(raw_response),
    )

    snapshots: list[ValuationSnapshot] = []
    for ticker in normalized_tickers:
        base = base_snapshots.get(ticker)
        if base is None:
            issues.append(
                FmpValuationFetchIssue(
                    severity=ValuationIssueSeverity.WARNING,
                    code="missing_base_valuation_snapshot",
                    ticker=ticker,
                    message=(
                        "缺少当前可见基础估值快照，EODHD trend 不能单独生成评分快照；"
                        "请先运行 FMP 或 CSV 估值快照导入。"
                    ),
                )
            )
            continue
        trend_metric = _eodhd_eps_revision_metric(
            ticker=ticker,
            as_of=as_of,
            records=records_by_ticker.get(ticker, ()),
            issues=issues,
        )
        if trend_metric is None:
            continue
        snapshots.append(
            _merged_eodhd_trend_snapshot(
                base=base.snapshot,
                as_of=as_of,
                captured_at=fetch_date,
                downloaded_at=download_time,
                trend_metric=trend_metric,
            )
        )

    return EodhdEarningsTrendsFetchReport(
        as_of=as_of,
        captured_at=fetch_date,
        downloaded_at=download_time,
        requested_tickers=normalized_tickers,
        provider_symbols=provider_symbols,
        base_valuation_input_path=base_path,
        base_valuation_snapshot_count=len(base_snapshots),
        raw_payload=raw_payload,
        row_count=raw_payload.row_count,
        checksum_sha256=raw_payload.checksum_sha256,
        snapshots=tuple(sorted(snapshots, key=lambda item: item.snapshot_id)),
        issues=tuple(issues),
    )


def import_valuation_snapshots_from_csv(csv_path: Path | str) -> ValuationCsvImportReport:
    path = Path(csv_path)
    content = path.read_bytes()
    checksum = hashlib.sha256(content).hexdigest()
    text = content.decode("utf-8-sig")

    issues: list[ValuationImportIssue] = []
    snapshots: list[ValuationSnapshot] = []
    reader = csv.DictReader(text.splitlines())
    fieldnames = tuple(reader.fieldnames or ())
    missing_columns = sorted(REQUIRED_COLUMNS - set(fieldnames))
    for column in missing_columns:
        issues.append(
            ValuationImportIssue(
                severity=ValuationIssueSeverity.ERROR,
                code="missing_required_column",
                column=column,
                path=path,
                message=f"CSV 缺少必填列：{column}",
            )
        )

    unknown_columns = sorted(set(fieldnames) - KNOWN_COLUMNS)
    for column in unknown_columns:
        issues.append(
            ValuationImportIssue(
                severity=ValuationIssueSeverity.WARNING,
                code="unknown_column",
                column=column,
                path=path,
                message=f"CSV 包含未识别列：{column}；导入器将忽略该列。",
            )
        )

    row_count = 0
    if missing_columns:
        return ValuationCsvImportReport(
            source_path=path,
            row_count=0,
            checksum_sha256=checksum,
            snapshots=(),
            issues=tuple(issues),
        )

    for row_number, row in enumerate(reader, start=2):
        row_count += 1
        row_issues: list[ValuationImportIssue] = []
        row_snapshot = _snapshot_from_row(path, row_number, row, row_issues)
        issues.extend(row_issues)
        if row_snapshot is not None and not any(
            issue.severity == ValuationIssueSeverity.ERROR for issue in row_issues
        ):
            snapshots.append(row_snapshot)

    return ValuationCsvImportReport(
        source_path=path,
        row_count=row_count,
        checksum_sha256=checksum,
        snapshots=tuple(snapshots),
        issues=tuple(issues),
    )


def render_valuation_csv_import_report(report: ValuationCsvImportReport) -> str:
    lines = [
        "# 估值 CSV 导入报告",
        "",
        f"- 状态：{report.status}",
        f"- 来源文件：`{report.source_path}`",
        f"- CSV 行数：{report.row_count}",
        f"- 导入快照数：{report.imported_count}",
        f"- SHA256：`{report.checksum_sha256}`",
        f"- 错误数：{report.error_count}",
        f"- 警告数：{report.warning_count}",
        "",
        "## 快照概览",
        "",
    ]
    if not report.snapshots:
        lines.append("未导入估值快照。")
    else:
        lines.extend(
            [
                "| Snapshot | Ticker | 日期 | 来源类型 | 来源 | 指标数 | 拥挤度信号 |",
                "|---|---|---|---|---|---:|---:|",
            ]
        )
        for snapshot in sorted(report.snapshots, key=lambda value: value.snapshot_id):
            metric_count = len(snapshot.valuation_metrics) + len(snapshot.expectation_metrics)
            lines.append(
                "| "
                f"{snapshot.snapshot_id} | "
                f"{snapshot.ticker} | "
                f"{snapshot.as_of.isoformat()} | "
                f"{snapshot.source_type} | "
                f"{_escape_markdown_table(snapshot.source_name)} | "
                f"{metric_count} | "
                f"{len(snapshot.crowding_signals)} |"
            )

    lines.extend(["", "## 问题", ""])
    if not report.issues:
        lines.append("未发现问题。")
    else:
        lines.extend(
            [
                "| 级别 | Code | 行 | Snapshot | Ticker | 列 | 说明 |",
                "|---|---|---:|---|---|---|---|",
            ]
        )
        for issue in report.issues:
            level = "错误" if issue.severity == ValuationIssueSeverity.ERROR else "警告"
            lines.append(
                "| "
                f"{level} | "
                f"{issue.code} | "
                f"{issue.row_number or ''} | "
                f"{issue.snapshot_id or ''} | "
                f"{issue.ticker or ''} | "
                f"{issue.column or ''} | "
                f"{_escape_markdown_table(issue.message)} |"
            )

    return "\n".join(lines) + "\n"


def write_valuation_csv_import_report(
    report: ValuationCsvImportReport,
    output_path: Path,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(render_valuation_csv_import_report(report), encoding="utf-8")
    return output_path


def load_fmp_analyst_estimate_history_snapshots(
    input_path: Path | str,
    tickers: list[str] | tuple[str, ...] | None = None,
) -> tuple[FmpAnalystEstimateHistorySnapshot, ...]:
    path = Path(input_path)
    if not path.exists():
        return ()
    selected_tickers = None if tickers is None else set(_normalize_tickers(tickers))
    snapshots: list[FmpAnalystEstimateHistorySnapshot] = []
    paths = [path] if path.is_file() else sorted(path.rglob("fmp_analyst_estimates_*.json"))
    for history_path in paths:
        raw = json.loads(history_path.read_text(encoding="utf-8"))
        snapshot = _fmp_analyst_history_snapshot_from_raw(raw, history_path)
        if selected_tickers is None or snapshot.ticker in selected_tickers:
            snapshots.append(snapshot)
    return tuple(sorted(snapshots, key=lambda item: (item.ticker, item.captured_at)))


def load_fmp_forward_pit_analyst_estimate_history(
    input_path: Path | str,
    *,
    tickers: list[str] | tuple[str, ...] | None = None,
    decision_time: datetime,
) -> tuple[FmpAnalystEstimateHistorySnapshot, ...]:
    path = Path(input_path)
    if not path.exists():
        return ()
    selected_tickers = None if tickers is None else set(_normalize_tickers(tickers))
    grouped_records: dict[
        tuple[str, date, datetime, str],
        list[dict[str, Any]],
    ] = {}
    metadata: dict[tuple[str, date, datetime, str], dict[str, Any]] = {}
    for csv_path in _fmp_forward_pit_normalized_paths(path):
        with csv_path.open(encoding="utf-8", newline="") as file:
            for row in csv.DictReader(file):
                if row.get("endpoint_category") != "analyst_estimates":
                    continue
                ticker = (row.get("canonical_ticker") or "").strip().upper()
                if selected_tickers is not None and ticker not in selected_tickers:
                    continue
                available_time = _parse_optional_iso_datetime(
                    row.get("available_time") or ""
                )
                if available_time is None or available_time > decision_time.astimezone(UTC):
                    continue
                try:
                    captured_at = date.fromisoformat(row.get("captured_at") or "")
                    downloaded_at = _parse_optional_iso_datetime(
                        row.get("downloaded_at") or ""
                    )
                    record = json.loads(row.get("normalized_values_json") or "{}")
                except (ValueError, json.JSONDecodeError):
                    continue
                if downloaded_at is None or not isinstance(record, dict):
                    continue
                raw_payload_path = row.get("raw_payload_path") or str(csv_path)
                key = (ticker, captured_at, downloaded_at, raw_payload_path)
                grouped_records.setdefault(key, []).append(record)
                metadata[key] = {
                    "as_of": row.get("as_of") or captured_at.isoformat(),
                    "endpoint": row.get("endpoint") or f"{FMP_BASE_URL}/analyst-estimates",
                    "provider_symbol": row.get("provider_symbol") or ticker,
                    "raw_payload_sha256": row.get("raw_payload_sha256") or "",
                    "source_path": Path(raw_payload_path),
                }

    snapshots: list[FmpAnalystEstimateHistorySnapshot] = []
    for key, records in grouped_records.items():
        ticker, captured_at, downloaded_at, _raw_payload_path = key
        item_metadata = metadata[key]
        try:
            as_of_date = date.fromisoformat(str(item_metadata["as_of"]))
        except ValueError:
            as_of_date = captured_at
        snapshots.append(
            FmpAnalystEstimateHistorySnapshot(
                ticker=ticker,
                as_of=as_of_date,
                captured_at=captured_at,
                downloaded_at=downloaded_at,
                endpoint=str(item_metadata["endpoint"]),
                request_parameters={
                    "symbol": str(item_metadata["provider_symbol"]),
                    "period": FMP_ANALYST_ESTIMATE_PERIOD,
                    "page": FMP_ANALYST_ESTIMATE_PAGE,
                    "source": "fmp_forward_pit_normalized",
                },
                row_count=len(records),
                checksum_sha256=str(item_metadata["raw_payload_sha256"]),
                records=tuple(records),
                source_path=cast(Path, item_metadata["source_path"]),
            )
        )
    return tuple(sorted(snapshots, key=lambda item: (item.ticker, item.captured_at)))


def validate_fmp_analyst_estimate_history(
    input_path: Path | str,
    as_of: date,
    *,
    tickers: list[str] | tuple[str, ...] | None = None,
    max_snapshot_age_days: int = 7,
) -> FmpAnalystHistoryValidationReport:
    path = Path(input_path)
    selected_tickers = None if tickers is None else set(_normalize_tickers(tickers))
    issues: list[FmpAnalystHistoryValidationIssue] = []
    snapshots: list[FmpAnalystEstimateHistorySnapshot] = []

    if max_snapshot_age_days < 0:
        raise ValueError("max_snapshot_age_days must be non-negative")
    if not path.exists():
        issues.append(
            FmpAnalystHistoryValidationIssue(
                severity=ValuationIssueSeverity.WARNING,
                code="fmp_history_path_missing",
                path=path,
                message="FMP analyst-estimates 历史缓存目录或文件不存在。",
            )
        )
        return FmpAnalystHistoryValidationReport(
            as_of=as_of,
            input_path=path,
            snapshots=(),
            issues=tuple(issues),
        )

    for history_path in _fmp_analyst_history_paths(path):
        try:
            raw = json.loads(history_path.read_text(encoding="utf-8"))
            snapshot = _fmp_analyst_history_snapshot_from_raw(raw, history_path)
        except (OSError, ValueError, json.JSONDecodeError) as exc:
            issues.append(
                FmpAnalystHistoryValidationIssue(
                    severity=ValuationIssueSeverity.ERROR,
                    code="fmp_history_load_error",
                    path=history_path,
                    message=str(exc),
                )
            )
            continue
        if selected_tickers is not None and snapshot.ticker not in selected_tickers:
            continue
        snapshots.append(snapshot)
        _check_fmp_analyst_history_snapshot(
            snapshot=snapshot,
            as_of=as_of,
            max_snapshot_age_days=max_snapshot_age_days,
            issues=issues,
        )

    has_errors = any(issue.severity == ValuationIssueSeverity.ERROR for issue in issues)
    if not snapshots and not has_errors:
        issues.append(
            FmpAnalystHistoryValidationIssue(
                severity=ValuationIssueSeverity.WARNING,
                code="no_fmp_history_snapshots",
                path=path,
                message="未发现可读取的 FMP analyst-estimates 历史快照。",
            )
        )

    _check_duplicate_fmp_history_snapshots(snapshots, issues)

    return FmpAnalystHistoryValidationReport(
        as_of=as_of,
        input_path=path,
        snapshots=tuple(sorted(snapshots, key=lambda item: (item.ticker, item.captured_at))),
        issues=tuple(issues),
    )


def render_fmp_analyst_history_validation_report(
    report: FmpAnalystHistoryValidationReport,
) -> str:
    lines = [
        "# FMP Analyst Estimates 历史缓存校验报告",
        "",
        f"- 状态：{report.status}",
        f"- 评估日期：{report.as_of.isoformat()}",
        f"- 输入路径：`{report.input_path}`",
        f"- 快照数量：{report.snapshot_count}",
        f"- 覆盖标的数：{report.ticker_count}",
        f"- 记录数：{report.record_count}",
        f"- 错误数：{report.error_count}",
        f"- 警告数：{report.warning_count}",
        "",
        "## 快照概览",
        "",
    ]
    if not report.snapshots:
        lines.append("未发现可读取的 FMP analyst-estimates 历史快照。")
    else:
        lines.extend(
            [
                "| Ticker | Captured At | Downloaded At | Rows | Checksum | 文件 |",
                "|---|---|---|---:|---|---|",
            ]
        )
        for snapshot in report.snapshots:
            checksum = snapshot.checksum_sha256[:12]
            lines.append(
                "| "
                f"{snapshot.ticker} | "
                f"{snapshot.captured_at.isoformat()} | "
                f"{snapshot.downloaded_at.isoformat()} | "
                f"{snapshot.row_count} | "
                f"`{checksum}` | "
                f"{_escape_markdown_table(str(snapshot.source_path or ''))} |"
            )

    lines.extend(["", "## 问题", ""])
    if not report.issues:
        lines.append("未发现问题。")
    else:
        lines.extend(
            [
                "| 级别 | Code | Ticker | Estimate Date | 文件 | 说明 |",
                "|---|---|---|---|---|---|",
            ]
        )
        for issue in report.issues:
            level = "错误" if issue.severity == ValuationIssueSeverity.ERROR else "警告"
            lines.append(
                "| "
                f"{level} | "
                f"{issue.code} | "
                f"{issue.ticker or ''} | "
                f"{issue.estimate_date.isoformat() if issue.estimate_date else ''} | "
                f"{_escape_markdown_table(str(issue.path or ''))} | "
                f"{_escape_markdown_table(issue.message)} |"
            )

    lines.extend(
        [
            "",
            "## 方法说明",
            "",
            "- 校验对象是 `aits valuation fetch-fmp` 写入的原始 annual analyst-estimates JSON。",
            (
                "- 校验会检查 JSON schema、checksum、row_count、ticker、"
                "请求参数、日期和重复 estimate date。"
            ),
            "- `eps_revision_90d_pct` 依赖这些历史快照，缓存错误时不能静默参与估值评分。",
        ]
    )
    return "\n".join(lines) + "\n"


def write_fmp_analyst_history_validation_report(
    report: FmpAnalystHistoryValidationReport,
    output_path: Path,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        render_fmp_analyst_history_validation_report(report),
        encoding="utf-8",
    )
    return output_path


def default_fmp_analyst_history_validation_report_path(
    output_dir: Path,
    as_of: date,
) -> Path:
    return output_dir / f"fmp_analyst_history_validation_{as_of.isoformat()}.md"


def write_fmp_analyst_estimate_history_snapshots(
    snapshots: tuple[FmpAnalystEstimateHistorySnapshot, ...]
    | list[FmpAnalystEstimateHistorySnapshot],
    output_dir: Path | str,
) -> tuple[Path, ...]:
    directory = Path(output_dir)
    written: list[Path] = []
    for snapshot in snapshots:
        ticker_dir = directory / snapshot.ticker.lower()
        ticker_dir.mkdir(parents=True, exist_ok=True)
        output_path = ticker_dir / _fmp_analyst_estimate_history_raw_filename(snapshot)
        output_path.write_text(
            json.dumps(
                _fmp_analyst_history_snapshot_to_raw(snapshot),
                ensure_ascii=False,
                indent=2,
                sort_keys=False,
                default=str,
            )
            + "\n",
            encoding="utf-8",
        )
        written.append(output_path)
    return tuple(written)


def _fmp_analyst_estimate_history_raw_filename(
    snapshot: FmpAnalystEstimateHistorySnapshot,
) -> str:
    checksum = snapshot.checksum_sha256 or _json_checksum(snapshot.records)
    downloaded_token = (
        snapshot.downloaded_at.astimezone(UTC).strftime("%Y-%m-%dT%H-%M-%SZ")
    )
    return (
        f"fmp_analyst_estimates_{snapshot.ticker.lower()}_"
        f"{snapshot.captured_at.isoformat()}_{downloaded_token}_{checksum[:12]}.json"
    )


def write_fmp_historical_valuation_raw_payloads(
    payloads: tuple[FmpHistoricalValuationRawPayload, ...]
    | list[FmpHistoricalValuationRawPayload],
    output_dir: Path | str,
) -> tuple[Path, ...]:
    directory = Path(output_dir)
    written: list[Path] = []
    for payload in payloads:
        ticker_dir = directory / payload.ticker.lower()
        ticker_dir.mkdir(parents=True, exist_ok=True)
        output_path = ticker_dir / (
            f"fmp_historical_valuation_{payload.ticker.lower()}_"
            f"{payload.captured_at.isoformat()}.json"
        )
        output_path.write_text(
            json.dumps(
                _fmp_historical_valuation_payload_to_raw(payload),
                ensure_ascii=False,
                indent=2,
                sort_keys=False,
                default=str,
            )
            + "\n",
            encoding="utf-8",
        )
        written.append(output_path)
    return tuple(written)


def render_fmp_historical_valuation_fetch_report(
    report: FmpHistoricalValuationFetchReport,
) -> str:
    alias_summary = _fmp_symbol_alias_summary(report.requested_tickers)
    lines = [
        "# FMP 历史估值拉取报告",
        "",
        f"- 状态：{report.status}",
        f"- 来源：{report.source_name}",
        f"- 来源类型：{report.source_type}",
        f"- 评估日期：{report.as_of.isoformat()}",
        f"- 采集日期：{report.captured_at.isoformat()}",
        f"- 下载时间：{report.downloaded_at.isoformat()}",
        f"- 请求标的：{', '.join(report.requested_tickers)}",
        f"- Provider symbol aliases：{alias_summary or '无'}",
        f"- Endpoint：{', '.join(FMP_HISTORICAL_VALUATION_ENDPOINTS)}",
        f"- 请求参数：period={report.period}, limit={report.limit}",
        f"- 原始 payload 数：{len(report.raw_payloads)}",
        f"- 返回记录数：{report.row_count}",
        f"- 生成历史估值快照数：{report.imported_count}",
        f"- SHA256：`{report.checksum_sha256}`",
        f"- 错误数：{report.error_count}",
        f"- 警告数：{report.warning_count}",
        "",
        "## 快照概览",
        "",
    ]
    if not report.snapshots:
        lines.append("未生成历史估值快照。")
    else:
        lines.extend(
            [
                "| Snapshot | Ticker | 日期 | 指标 | 说明 |",
                "|---|---|---|---|---|",
            ]
        )
        for snapshot in sorted(report.snapshots, key=lambda value: value.snapshot_id):
            valuation_ids = ", ".join(
                metric.metric_id for metric in snapshot.valuation_metrics
            )
            lines.append(
                "| "
                f"{snapshot.snapshot_id} | "
                f"{snapshot.ticker} | "
                f"{snapshot.as_of.isoformat()} | "
                f"{_escape_markdown_table(valuation_ids)} | "
                f"{_escape_markdown_table(snapshot.notes)} |"
            )

    lines.extend(["", "## 问题", ""])
    if not report.issues:
        lines.append("未发现问题。")
    else:
        lines.extend(
            [
                "| 级别 | Code | Ticker | Endpoint | 说明 |",
                "|---|---|---|---|---|",
            ]
        )
        for issue in report.issues:
            level = "错误" if issue.severity == ValuationIssueSeverity.ERROR else "警告"
            lines.append(
                "| "
                f"{level} | "
                f"{issue.code} | "
                f"{issue.ticker or ''} | "
                f"{issue.endpoint or ''} | "
                f"{_escape_markdown_table(issue.message)} |"
            )

    lines.extend(
        [
            "",
            "## 方法说明",
            "",
            (
                "- 本报告拉取 FMP historical `key-metrics` 和 `ratios`，"
                "保存原始响应后生成 paid_vendor 历史估值快照。"
            ),
            (
                "- 历史快照只用于当前 `fetch-fmp` 计算 `valuation_percentile` 的"
                "本地估值历史分布；不用于伪造 `eps_revision_90d_pct`。"
            ),
            (
                "- `ev_sales` 来自 `key-metrics.evToSales`；`peg` 优先来自 "
                "`ratios.forwardPriceToEarningsGrowthRatio`，缺失时使用 "
                "`ratios.priceToEarningsGrowthRatio`。"
            ),
            (
                "- `captured_at` 固定为本次采集日期，历史回测在该日期之前不会看到"
                "这些由当前供应商历史接口回填的快照。"
            ),
        ]
    )
    return "\n".join(lines) + "\n"


def write_fmp_historical_valuation_fetch_report(
    report: FmpHistoricalValuationFetchReport,
    output_path: Path,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(render_fmp_historical_valuation_fetch_report(report), encoding="utf-8")
    return output_path


def default_fmp_historical_valuation_fetch_report_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"fmp_historical_valuation_fetch_{as_of.isoformat()}.md"


def default_fmp_historical_valuation_raw_dir(output_dir: Path) -> Path:
    return output_dir / "fmp_historical_valuation"


def render_fmp_valuation_fetch_report(report: FmpValuationFetchReport) -> str:
    alias_summary = _fmp_symbol_alias_summary(report.requested_tickers)
    lines = [
        "# FMP 估值与预期拉取报告",
        "",
        f"- 状态：{report.status}",
        f"- 来源：{report.source_name}",
        f"- 来源类型：{report.source_type}",
        f"- 评估日期：{report.as_of.isoformat()}",
        f"- 采集日期：{report.captured_at.isoformat()}",
        f"- 下载时间：{report.downloaded_at.isoformat()}",
        f"- 请求标的：{', '.join(report.requested_tickers)}",
        f"- Provider symbol aliases：{alias_summary or '无'}",
        f"- Endpoint：{', '.join(FMP_ENDPOINTS)}",
        (
            "- 请求参数：analyst-estimates period=annual, page=0, "
            f"limit={report.analyst_estimate_limit}"
        ),
        (
            f"- Analyst history 输入："
            f"`{report.analyst_history_input_path}`"
            if report.analyst_history_input_path is not None
            else "- Analyst history 输入：未配置"
        ),
        (
            f"- PIT normalized 输入："
            f"`{report.pit_normalized_input_path}`"
            if report.pit_normalized_input_path is not None
            else "- PIT normalized 输入：未配置"
        ),
        f"- 已读取历史 analyst 快照数：{report.historical_analyst_snapshot_count}",
        f"- 已读取 PIT analyst 快照数：{report.pit_analyst_snapshot_count}",
        f"- 本次待写入 analyst 快照数：{len(report.analyst_estimate_history_snapshots)}",
        (
            f"- Valuation history 输入："
            f"`{report.valuation_history_input_path}`"
            if report.valuation_history_input_path is not None
            else "- Valuation history 输入：未配置"
        ),
        f"- 已读取历史 valuation 快照数：{report.historical_valuation_snapshot_count}",
        f"- 估值分位最低历史样本数：{report.minimum_valuation_history_points}",
        f"- 返回记录数：{report.row_count}",
        f"- 生成快照数：{report.imported_count}",
        f"- SHA256：`{report.checksum_sha256}`",
        f"- 错误数：{report.error_count}",
        f"- 警告数：{report.warning_count}",
        "",
        "## 快照概览",
        "",
    ]
    if not report.snapshots:
        lines.append("未生成估值快照。")
    else:
        lines.extend(
            [
                "| Snapshot | Ticker | 估值指标 | 预期指标 | 说明 |",
                "|---|---|---|---|---|",
            ]
        )
        for snapshot in sorted(report.snapshots, key=lambda value: value.snapshot_id):
            valuation_ids = ", ".join(
                metric.metric_id for metric in snapshot.valuation_metrics
            )
            expectation_ids = ", ".join(
                metric.metric_id for metric in snapshot.expectation_metrics
            )
            lines.append(
                "| "
                f"{snapshot.snapshot_id} | "
                f"{snapshot.ticker} | "
                f"{_escape_markdown_table(valuation_ids)} | "
                f"{_escape_markdown_table(expectation_ids or 'n/a')} | "
                f"{_escape_markdown_table(snapshot.notes)} |"
            )

    lines.extend(["", "## 问题", ""])
    if not report.issues:
        lines.append("未发现问题。")
    else:
        lines.extend(
            [
                "| 级别 | Code | Ticker | Endpoint | 说明 |",
                "|---|---|---|---|---|",
            ]
        )
        for issue in report.issues:
            level = "错误" if issue.severity == ValuationIssueSeverity.ERROR else "警告"
            lines.append(
                "| "
                f"{level} | "
                f"{issue.code} | "
                f"{issue.ticker or ''} | "
                f"{issue.endpoint or ''} | "
                f"{_escape_markdown_table(issue.message)} |"
            )

    lines.extend(
        [
            "",
            "## 方法说明",
            "",
            (
                "- `forward_pe` 使用 `quote-short.price / analyst-estimates.epsAvg`，"
                "选择评估日之后最近的 annual estimate。"
            ),
            "- `ev_sales` 使用 `key-metrics-ttm.evToSalesTTM`。",
            (
                "- `peg` 优先使用 `ratios-ttm.forwardPriceToEarningsGrowthRatioTTM`，"
                "缺失时才使用 `priceToEarningsGrowthRatioTTM`。"
            ),
            (
                "- `revenue_growth_next_12m_pct` 是 annual estimate 代理口径："
                "最近未来 annual revenueAvg 相对上一年度 revenueAvg。"
            ),
            (
                "- 配置 PIT normalized 输入时，`eps_revision_90d_pct` 只使用 "
                "`available_time <= decision_time` 的自建 forward-only analyst-estimates "
                "as-of 索引；未配置时保留 legacy analyst history 路径。"
            ),
            (
                "- `eps_revision_90d_pct` 使用同一 fiscal estimate date 的历史 "
                "analyst-estimates.epsAvg，与当前值比较；历史快照必须接近 90 天回看窗口，"
                "样本不足时明确降级。"
            ),
            (
                "- `valuation_percentile` 使用本地 point-in-time 估值快照历史计算；"
                "每个 metric 至少需要达到最低样本数。"
            ),
        ]
    )
    return "\n".join(lines) + "\n"


def write_fmp_valuation_fetch_report(
    report: FmpValuationFetchReport,
    output_path: Path,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(render_fmp_valuation_fetch_report(report), encoding="utf-8")
    return output_path


def default_fmp_valuation_fetch_report_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"fmp_valuation_fetch_{as_of.isoformat()}.md"


def default_fmp_analyst_estimate_history_dir(output_dir: Path) -> Path:
    return output_dir / "fmp_analyst_estimates"


def render_eodhd_earnings_trends_fetch_report(
    report: EodhdEarningsTrendsFetchReport,
) -> str:
    alias_summary = _eodhd_symbol_alias_summary(report.requested_tickers)
    lines = [
        "# EODHD Earnings Trends 拉取报告",
        "",
        f"- 状态：{report.status}",
        f"- 来源：{report.source_name}",
        f"- 来源类型：{report.source_type}",
        f"- 评估日期：{report.as_of.isoformat()}",
        f"- 采集日期：{report.captured_at.isoformat()}",
        f"- 下载时间：{report.downloaded_at.isoformat()}",
        f"- 请求标的：{', '.join(report.requested_tickers)}",
        f"- Provider symbols：{', '.join(report.provider_symbols)}",
        f"- Provider symbol aliases：{alias_summary or '无'}",
        f"- Endpoint：{EODHD_BASE_URL}/{EODHD_EARNINGS_TRENDS_ENDPOINT}",
        "- 请求参数：symbols=<provider_symbols>, fmt=json",
        (
            f"- 基础估值快照输入：`{report.base_valuation_input_path}`"
            if report.base_valuation_input_path is not None
            else "- 基础估值快照输入：未配置"
        ),
        f"- 已读取基础估值快照数：{report.base_valuation_snapshot_count}",
        f"- 返回 trend 记录数：{report.row_count}",
        f"- 生成合并估值快照数：{report.imported_count}",
        f"- SHA256：`{report.checksum_sha256}`",
        f"- 错误数：{report.error_count}",
        f"- 警告数：{report.warning_count}",
        "",
        "## 快照概览",
        "",
    ]
    if not report.snapshots:
        lines.append("未生成 EODHD trend 合并快照。")
    else:
        lines.extend(
            [
                "| Snapshot | Ticker | 估值指标 | 预期指标 | 说明 |",
                "|---|---|---|---|---|",
            ]
        )
        for snapshot in report.snapshots:
            valuation_ids = ", ".join(
                metric.metric_id for metric in snapshot.valuation_metrics
            )
            expectation_ids = ", ".join(
                metric.metric_id for metric in snapshot.expectation_metrics
            )
            lines.append(
                "| "
                f"{snapshot.snapshot_id} | "
                f"{snapshot.ticker} | "
                f"{_escape_markdown_table(valuation_ids)} | "
                f"{_escape_markdown_table(expectation_ids or 'n/a')} | "
                f"{_escape_markdown_table(snapshot.notes)} |"
            )

    lines.extend(["", "## 问题", ""])
    if not report.issues:
        lines.append("未发现问题。")
    else:
        lines.extend(
            [
                "| 级别 | Code | Ticker | Endpoint | 说明 |",
                "|---|---|---|---|---|",
            ]
        )
        for issue in report.issues:
            level = "错误" if issue.severity == ValuationIssueSeverity.ERROR else "警告"
            lines.append(
                "| "
                f"{level} | "
                f"{issue.code} | "
                f"{issue.ticker or ''} | "
                f"{issue.endpoint or ''} | "
                f"{_escape_markdown_table(issue.message)} |"
            )

    lines.extend(
        [
            "",
            "## 方法说明",
            "",
            (
                "- 本报告使用 EODHD Earnings Trends 的 `epsTrendCurrent` 和 "
                "`epsTrend90daysAgo` 生成 `eps_revision_90d_pct`。"
            ),
            (
                "- EODHD trend 只增强当前可见基础估值快照；估值倍数、估值分位和拥挤度"
                "继承基础快照，不由 EODHD trends 推断。"
            ),
            (
                "- 生成快照标记为采集日后可见，不能作为采集日前严格 point-in-time "
                "回测输入，也不能替代真实 estimates archive。"
            ),
        ]
    )
    return "\n".join(lines) + "\n"


def write_eodhd_earnings_trends_fetch_report(
    report: EodhdEarningsTrendsFetchReport,
    output_path: Path,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        render_eodhd_earnings_trends_fetch_report(report),
        encoding="utf-8",
    )
    return output_path


def default_eodhd_earnings_trends_fetch_report_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"eodhd_earnings_trends_fetch_{as_of.isoformat()}.md"


def default_eodhd_earnings_trends_raw_dir(output_dir: Path) -> Path:
    return output_dir / "eodhd_earnings_trends"


def write_eodhd_earnings_trends_raw_payload(
    payload: EodhdEarningsTrendsRawPayload | None,
    output_dir: Path | str,
) -> tuple[Path, ...]:
    if payload is None:
        return ()
    directory = Path(output_dir)
    tickers_token = "_".join(ticker.lower() for ticker in payload.requested_tickers)
    output_path = (
        directory
        / tickers_token
        / f"eodhd_earnings_trends_{tickers_token}_{payload.captured_at.isoformat()}.json"
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(
            _eodhd_earnings_trends_payload_to_raw(payload),
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    return (output_path,)


def write_valuation_snapshots_as_yaml(
    snapshots: tuple[ValuationSnapshot, ...] | list[ValuationSnapshot],
    output_dir: Path | str,
) -> tuple[Path, ...]:
    directory = Path(output_dir)
    directory.mkdir(parents=True, exist_ok=True)
    written: list[Path] = []
    for snapshot in snapshots:
        output_path = directory / f"{snapshot.snapshot_id}.yaml"
        output_path.write_text(
            yaml.safe_dump(
                snapshot.model_dump(mode="json"),
                allow_unicode=True,
                sort_keys=False,
            ),
            encoding="utf-8",
        )
        written.append(output_path)
    return tuple(written)


def _load_fmp_valuation_history(
    input_path: Path,
    tickers: tuple[str, ...],
    issues: list[FmpValuationFetchIssue],
) -> tuple[LoadedValuationSnapshot, ...]:
    store = load_valuation_snapshot_store(input_path)
    for load_error in store.load_errors:
        issues.append(
            FmpValuationFetchIssue(
                severity=ValuationIssueSeverity.WARNING,
                code="valuation_history_load_error",
                message=f"{load_error.path}: {load_error.message}",
            )
        )
    ticker_set = set(tickers)
    return tuple(
        loaded for loaded in store.loaded if loaded.snapshot.ticker.upper() in ticker_set
    )


def _fmp_analyst_history_paths(path: Path) -> list[Path]:
    if path.is_file():
        return [path]
    return sorted(path.rglob("fmp_analyst_estimates_*.json"))


def _fmp_forward_pit_normalized_paths(path: Path) -> list[Path]:
    if path.is_file():
        return [path]
    return sorted(path.rglob("fmp_forward_pit_*.csv"))


def _daily_decision_time(as_of: date) -> datetime:
    return datetime(
        as_of.year,
        as_of.month,
        as_of.day,
        23,
        59,
        59,
        999999,
        tzinfo=UTC,
    )


def _check_fmp_analyst_history_snapshot(
    *,
    snapshot: FmpAnalystEstimateHistorySnapshot,
    as_of: date,
    max_snapshot_age_days: int,
    issues: list[FmpAnalystHistoryValidationIssue],
) -> None:
    path = snapshot.source_path
    if not snapshot.ticker:
        issues.append(
            FmpAnalystHistoryValidationIssue(
                severity=ValuationIssueSeverity.ERROR,
                code="missing_ticker",
                path=path,
                message="历史快照缺少 ticker。",
            )
        )
    if snapshot.as_of > as_of or snapshot.captured_at > as_of:
        issues.append(
            FmpAnalystHistoryValidationIssue(
                severity=ValuationIssueSeverity.ERROR,
                code="fmp_history_date_in_future",
                path=path,
                ticker=snapshot.ticker,
                message="历史快照 as_of 或 captured_at 晚于评估日期。",
            )
        )
    if snapshot.downloaded_at.date() > as_of:
        issues.append(
            FmpAnalystHistoryValidationIssue(
                severity=ValuationIssueSeverity.ERROR,
                code="fmp_history_download_in_future",
                path=path,
                ticker=snapshot.ticker,
                message="历史快照 downloaded_at 晚于评估日期。",
            )
        )
    if (as_of - snapshot.captured_at).days > max_snapshot_age_days:
        issues.append(
            FmpAnalystHistoryValidationIssue(
                severity=ValuationIssueSeverity.WARNING,
                code="fmp_history_snapshot_stale",
                path=path,
                ticker=snapshot.ticker,
                message="FMP analyst-estimates 历史快照超过新鲜度阈值。",
            )
        )
    if snapshot.row_count != len(snapshot.records):
        issues.append(
            FmpAnalystHistoryValidationIssue(
                severity=ValuationIssueSeverity.ERROR,
                code="row_count_mismatch",
                path=path,
                ticker=snapshot.ticker,
                message=(
                    f"row_count={snapshot.row_count} 与 records 数量 "
                    f"{len(snapshot.records)} 不一致。"
                ),
            )
        )
    if not snapshot.records:
        issues.append(
            FmpAnalystHistoryValidationIssue(
                severity=ValuationIssueSeverity.WARNING,
                code="empty_fmp_history_records",
                path=path,
                ticker=snapshot.ticker,
                message="历史快照 records 为空，不能用于 EPS revision。",
            )
        )

    _check_fmp_history_request_parameters(snapshot, issues)
    _check_fmp_history_records(snapshot, issues)


def _check_fmp_history_request_parameters(
    snapshot: FmpAnalystEstimateHistorySnapshot,
    issues: list[FmpAnalystHistoryValidationIssue],
) -> None:
    path = snapshot.source_path
    params = snapshot.request_parameters
    expected = {
        "symbol": _fmp_provider_symbol(snapshot.ticker),
        "period": FMP_ANALYST_ESTIMATE_PERIOD,
        "page": FMP_ANALYST_ESTIMATE_PAGE,
    }
    for key, expected_value in expected.items():
        actual = params.get(key)
        if str(actual).upper() != str(expected_value).upper():
            issues.append(
                FmpAnalystHistoryValidationIssue(
                    severity=ValuationIssueSeverity.ERROR,
                    code="unexpected_request_parameter",
                    path=path,
                    ticker=snapshot.ticker,
                    message=(
                        f"request_parameters.{key}={actual!r}，"
                        f"预期为 {expected_value!r}。"
                    ),
                )
            )
    limit = params.get("limit")
    if not isinstance(limit, int) or limit <= 0:
        issues.append(
            FmpAnalystHistoryValidationIssue(
                severity=ValuationIssueSeverity.ERROR,
                code="invalid_request_limit",
                path=path,
                ticker=snapshot.ticker,
                message="request_parameters.limit 必须是正整数。",
            )
        )


def _check_fmp_history_records(
    snapshot: FmpAnalystEstimateHistorySnapshot,
    issues: list[FmpAnalystHistoryValidationIssue],
) -> None:
    seen_dates: set[date] = set()
    duplicate_dates: set[date] = set()
    expected_symbol = _fmp_provider_symbol(snapshot.ticker)
    for record in snapshot.records:
        raw_symbol = str(record.get("symbol") or "").upper()
        raw_date = str(record.get("date") or "").strip()
        try:
            estimate_date = date.fromisoformat(raw_date)
        except ValueError:
            issues.append(
                FmpAnalystHistoryValidationIssue(
                    severity=ValuationIssueSeverity.ERROR,
                    code="invalid_estimate_date",
                    path=snapshot.source_path,
                    ticker=snapshot.ticker,
                    message=f"records.date 无法解析：{raw_date}",
                )
            )
            continue
        if estimate_date in seen_dates:
            duplicate_dates.add(estimate_date)
        seen_dates.add(estimate_date)

        if raw_symbol != expected_symbol:
            issues.append(
                FmpAnalystHistoryValidationIssue(
                    severity=ValuationIssueSeverity.ERROR,
                    code="record_ticker_mismatch",
                    path=snapshot.source_path,
                    ticker=snapshot.ticker,
                    estimate_date=estimate_date,
                    message=(
                        f"records.symbol={raw_symbol} 与 provider symbol "
                        f"{expected_symbol} 不一致。"
                    ),
                )
            )
        eps_avg = _numeric_fmp_field(record, "epsAvg")
        if eps_avg is None or eps_avg <= 0:
            issues.append(
                FmpAnalystHistoryValidationIssue(
                    severity=ValuationIssueSeverity.WARNING,
                    code="missing_or_invalid_eps_avg",
                    path=snapshot.source_path,
                    ticker=snapshot.ticker,
                    estimate_date=estimate_date,
                    message="records.epsAvg 缺失或非正数，不能用于 EPS revision。",
                )
            )

    for estimate_date in sorted(duplicate_dates):
        issues.append(
            FmpAnalystHistoryValidationIssue(
                severity=ValuationIssueSeverity.ERROR,
                code="duplicate_estimate_date",
                path=snapshot.source_path,
                ticker=snapshot.ticker,
                estimate_date=estimate_date,
                message="同一历史快照内存在重复 analyst estimate date。",
            )
        )


def _check_duplicate_fmp_history_snapshots(
    snapshots: list[FmpAnalystEstimateHistorySnapshot],
    issues: list[FmpAnalystHistoryValidationIssue],
) -> None:
    paths_by_key: dict[tuple[str, date], list[Path | None]] = {}
    for snapshot in snapshots:
        key = (snapshot.ticker, snapshot.captured_at)
        paths_by_key.setdefault(key, []).append(snapshot.source_path)

    for (ticker, captured_at), paths in sorted(paths_by_key.items()):
        if len(paths) <= 1:
            continue
        issues.append(
            FmpAnalystHistoryValidationIssue(
                severity=ValuationIssueSeverity.ERROR,
                code="duplicate_history_snapshot",
                path=paths[0],
                ticker=ticker,
                message=f"同一 ticker/captured_at 存在重复历史快照：{captured_at.isoformat()}。",
            )
        )


def _build_fmp_analyst_history_snapshot(
    *,
    ticker: str,
    as_of: date,
    captured_at: date,
    downloaded_at: datetime,
    limit: int,
    provider_symbol: str,
    records: tuple[dict[str, Any], ...],
) -> FmpAnalystEstimateHistorySnapshot:
    return FmpAnalystEstimateHistorySnapshot(
        ticker=ticker,
        as_of=as_of,
        captured_at=captured_at,
        downloaded_at=downloaded_at,
        endpoint=f"{FMP_BASE_URL}/analyst-estimates",
        request_parameters={
            "symbol": provider_symbol,
            "period": FMP_ANALYST_ESTIMATE_PERIOD,
            "page": FMP_ANALYST_ESTIMATE_PAGE,
            "limit": limit,
        },
        row_count=len(records),
        checksum_sha256=_json_checksum(records),
        records=records,
    )


def _fmp_historical_snapshots_from_payload(
    *,
    ticker: str,
    captured_at: date,
    downloaded_at: datetime,
    period: str,
    endpoint_records: dict[str, tuple[dict[str, Any], ...]],
    issues: list[FmpValuationFetchIssue],
) -> list[ValuationSnapshot]:
    key_metrics_by_date = _fmp_records_by_date(
        ticker=ticker,
        endpoint="key-metrics",
        records=endpoint_records.get("key-metrics", ()),
        issues=issues,
    )
    ratios_by_date = _fmp_records_by_date(
        ticker=ticker,
        endpoint="ratios",
        records=endpoint_records.get("ratios", ()),
        issues=issues,
    )
    snapshots: list[ValuationSnapshot] = []
    for snapshot_date in sorted(set(key_metrics_by_date) | set(ratios_by_date)):
        if snapshot_date >= captured_at:
            issues.append(
                FmpValuationFetchIssue(
                    severity=ValuationIssueSeverity.WARNING,
                    code="fmp_historical_valuation_future_or_current_row",
                    ticker=ticker,
                    message=(
                        "FMP historical valuation row date is not before captured_at; "
                        f"skipped {snapshot_date.isoformat()}."
                    ),
                )
            )
            continue
        metrics = _fmp_historical_valuation_metrics(
            ticker=ticker,
            period=period,
            key_metrics=key_metrics_by_date.get(snapshot_date),
            ratios=ratios_by_date.get(snapshot_date),
            issues=issues,
        )
        if not metrics:
            continue
        date_token = snapshot_date.isoformat().replace("-", "_")
        captured_token = captured_at.isoformat().replace("-", "_")
        snapshots.append(
            ValuationSnapshot(
                snapshot_id=(
                    f"fmp_{ticker.lower()}_historical_valuation_"
                    f"{date_token}_captured_{captured_token}"
                ),
                ticker=ticker,
                as_of=snapshot_date,
                source_type="paid_vendor",
                source_name=FMP_SOURCE_NAME,
                source_url=", ".join(
                    f"{FMP_BASE_URL}/{endpoint}"
                    for endpoint in FMP_HISTORICAL_VALUATION_ENDPOINTS
                ),
                captured_at=captured_at,
                point_in_time_class="backfilled_history_distribution",
                history_source_class="vendor_historical_endpoint",
                confidence_level="low",
                confidence_reason=(
                    "FMP historical key-metrics/ratios 是采集日回填的历史分布，"
                    "不是当时可见的 vendor archive。"
                ),
                backtest_use="captured_at_forward_only",
                valuation_metrics=metrics,
                expectation_metrics=[],
                valuation_percentile=None,
                overall_assessment="unknown",
                notes=(
                    "FMP historical key-metrics/ratios backfill captured on "
                    f"{downloaded_at.date().isoformat()}; period={period}. "
                    "Used as local valuation history for current percentile calculation."
                ),
            )
        )
    return snapshots


def _fmp_records_by_date(
    *,
    ticker: str,
    endpoint: str,
    records: tuple[dict[str, Any], ...],
    issues: list[FmpValuationFetchIssue],
) -> dict[date, dict[str, Any]]:
    records_by_date: dict[date, dict[str, Any]] = {}
    for record in records:
        raw_date = str(record.get("date") or "").strip()
        try:
            record_date = date.fromisoformat(raw_date)
        except ValueError:
            issues.append(
                FmpValuationFetchIssue(
                    severity=ValuationIssueSeverity.WARNING,
                    code="invalid_historical_valuation_date",
                    ticker=ticker,
                    endpoint=endpoint,
                    message=f"FMP historical valuation date 无法解析：{raw_date}",
                )
            )
            continue
        if record_date in records_by_date:
            issues.append(
                FmpValuationFetchIssue(
                    severity=ValuationIssueSeverity.WARNING,
                    code="duplicate_historical_valuation_date",
                    ticker=ticker,
                    endpoint=endpoint,
                    message=(
                        "FMP historical valuation response has duplicate date; "
                        f"using the latest row for {record_date.isoformat()}."
                    ),
                )
            )
        records_by_date[record_date] = record
    return records_by_date


def _fmp_historical_valuation_metrics(
    *,
    ticker: str,
    period: str,
    key_metrics: dict[str, Any] | None,
    ratios: dict[str, Any] | None,
    issues: list[FmpValuationFetchIssue],
) -> list[SnapshotMetric]:
    metrics: list[SnapshotMetric] = []
    _append_fmp_historical_positive_metric(
        ticker=ticker,
        record=key_metrics,
        field_name="evToSales",
        metric_id="ev_sales",
        source_field="key-metrics.evToSales",
        endpoint="key-metrics",
        period=period,
        metrics=metrics,
        issues=issues,
    )
    peg_value_field = "forwardPriceToEarningsGrowthRatio"
    if _numeric_fmp_field(ratios, peg_value_field) is None:
        peg_value_field = "priceToEarningsGrowthRatio"
    _append_fmp_historical_positive_metric(
        ticker=ticker,
        record=ratios,
        field_name=peg_value_field,
        metric_id="peg",
        source_field=f"ratios.{peg_value_field}",
        endpoint="ratios",
        period=period,
        metrics=metrics,
        issues=issues,
    )
    return metrics


def _append_fmp_historical_positive_metric(
    *,
    ticker: str,
    record: dict[str, Any] | None,
    field_name: str,
    metric_id: str,
    source_field: str,
    endpoint: str,
    period: str,
    metrics: list[SnapshotMetric],
    issues: list[FmpValuationFetchIssue],
) -> None:
    value = _numeric_fmp_field(record, field_name)
    if value is None:
        return
    if value <= 0:
        issues.append(
            FmpValuationFetchIssue(
                severity=ValuationIssueSeverity.WARNING,
                code="invalid_fmp_historical_valuation_multiple",
                ticker=ticker,
                endpoint=endpoint,
                message=(
                    f"FMP historical {source_field} is non-positive; "
                    f"skipped {metric_id}."
                ),
            )
        )
        return
    metrics.append(
        SnapshotMetric(
            metric_id=metric_id,
            value=value,
            unit="ratio",
            period=f"historical_{period}",
            source_field=source_field,
        )
    )


def _fmp_analyst_history_snapshot_to_raw(
    snapshot: FmpAnalystEstimateHistorySnapshot,
) -> dict[str, Any]:
    checksum = snapshot.checksum_sha256 or _json_checksum(snapshot.records)
    return {
        "provider": FMP_SOURCE_NAME,
        "source_type": "paid_vendor",
        "ticker": snapshot.ticker,
        "as_of": snapshot.as_of.isoformat(),
        "captured_at": snapshot.captured_at.isoformat(),
        "downloaded_at": snapshot.downloaded_at.isoformat(),
        "endpoint": snapshot.endpoint,
        "request_parameters": snapshot.request_parameters,
        "row_count": len(snapshot.records),
        "checksum_sha256": checksum,
        "records": list(snapshot.records),
    }


def _fmp_historical_valuation_payload_to_raw(
    payload: FmpHistoricalValuationRawPayload,
) -> dict[str, Any]:
    checksum = payload.checksum_sha256 or _json_checksum(payload.endpoint_records)
    return {
        "provider": FMP_SOURCE_NAME,
        "source_type": "paid_vendor",
        "ticker": payload.ticker,
        "as_of": payload.as_of.isoformat(),
        "captured_at": payload.captured_at.isoformat(),
        "downloaded_at": payload.downloaded_at.isoformat(),
        "endpoints": [f"{FMP_BASE_URL}/{endpoint}" for endpoint in payload.endpoint_records],
        "request_parameters_by_endpoint": payload.request_parameters_by_endpoint,
        "row_count": payload.row_count,
        "checksum_sha256": checksum,
        "records_by_endpoint": {
            endpoint: list(records)
            for endpoint, records in payload.endpoint_records.items()
        },
    }


def _eodhd_earnings_trends_payload_to_raw(
    payload: EodhdEarningsTrendsRawPayload,
) -> dict[str, Any]:
    checksum = payload.checksum_sha256 or _json_checksum(payload.records_by_ticker)
    return {
        "provider": EODHD_SOURCE_NAME,
        "source_type": "paid_vendor",
        "as_of": payload.as_of.isoformat(),
        "captured_at": payload.captured_at.isoformat(),
        "downloaded_at": payload.downloaded_at.isoformat(),
        "requested_tickers": list(payload.requested_tickers),
        "provider_symbols": list(payload.provider_symbols),
        "endpoint": payload.endpoint,
        "request_parameters": payload.request_parameters,
        "row_count": payload.row_count,
        "checksum_sha256": checksum,
        "records_by_ticker": {
            ticker: list(records)
            for ticker, records in sorted(payload.records_by_ticker.items())
        },
    }


def _latest_visible_base_valuation_snapshots(
    input_path: Path,
    tickers: tuple[str, ...],
    as_of: date,
    issues: list[FmpValuationFetchIssue],
) -> dict[str, LoadedValuationSnapshot]:
    store = load_valuation_snapshot_store(input_path)
    for load_error in store.load_errors:
        issues.append(
            FmpValuationFetchIssue(
                severity=ValuationIssueSeverity.WARNING,
                code="base_valuation_load_error",
                message=f"{load_error.path}: {load_error.message}",
            )
        )
    ticker_set = set(tickers)
    latest_by_ticker: dict[str, LoadedValuationSnapshot] = {}
    for loaded in store.loaded:
        snapshot = loaded.snapshot
        ticker = snapshot.ticker.upper()
        if (
            ticker not in ticker_set
            or snapshot.as_of > as_of
            or snapshot.captured_at > as_of
            or snapshot.source_type == "public_convenience"
            or _is_eodhd_trend_merged_snapshot(snapshot)
        ):
            continue
        current = latest_by_ticker.get(ticker)
        if current is None or _valuation_snapshot_sort_key(snapshot) > _valuation_snapshot_sort_key(
            current.snapshot
        ):
            latest_by_ticker[ticker] = loaded
    return latest_by_ticker


def _is_eodhd_trend_merged_snapshot(snapshot: ValuationSnapshot) -> bool:
    return snapshot.snapshot_id.startswith("merged_") and EODHD_SOURCE_NAME in snapshot.source_name


def _valuation_snapshot_sort_key(snapshot: ValuationSnapshot) -> tuple[date, date, str]:
    return (snapshot.as_of, snapshot.captured_at, snapshot.snapshot_id)


def _eodhd_trend_records_by_ticker(
    *,
    raw_response: dict[str, Any],
    tickers: tuple[str, ...],
    provider_symbols: tuple[str, ...],
    issues: list[FmpValuationFetchIssue],
) -> dict[str, tuple[dict[str, Any], ...]]:
    raw_trends = raw_response.get("trends")
    if not isinstance(raw_trends, list):
        issues.append(
            FmpValuationFetchIssue(
                severity=ValuationIssueSeverity.ERROR,
                code="invalid_eodhd_trends_schema",
                endpoint=EODHD_EARNINGS_TRENDS_ENDPOINT,
                message="EODHD Earnings Trends response 缺少 trends array。",
            )
        )
        return {}

    records_by_ticker: dict[str, tuple[dict[str, Any], ...]] = {}
    for index, ticker in enumerate(tickers):
        provider_symbol = provider_symbols[index]
        raw_records = raw_trends[index] if index < len(raw_trends) else []
        if not isinstance(raw_records, list):
            issues.append(
                FmpValuationFetchIssue(
                    severity=ValuationIssueSeverity.WARNING,
                    code="invalid_eodhd_ticker_trend_records",
                    ticker=ticker,
                    endpoint=EODHD_EARNINGS_TRENDS_ENDPOINT,
                    message="EODHD trends 中该 ticker 的 records 不是数组，已跳过。",
                )
            )
            records_by_ticker[ticker] = ()
            continue
        records: list[dict[str, Any]] = []
        for raw_record in raw_records:
            if not isinstance(raw_record, dict):
                issues.append(
                    FmpValuationFetchIssue(
                        severity=ValuationIssueSeverity.WARNING,
                        code="invalid_eodhd_trend_record",
                        ticker=ticker,
                        endpoint=EODHD_EARNINGS_TRENDS_ENDPOINT,
                        message="EODHD trend record 不是 object，已跳过。",
                    )
                )
                continue
            code = str(raw_record.get("code") or "").upper()
            if code and code not in {provider_symbol.upper(), ticker.upper()}:
                issues.append(
                    FmpValuationFetchIssue(
                        severity=ValuationIssueSeverity.WARNING,
                        code="eodhd_trend_symbol_mismatch",
                        ticker=ticker,
                        endpoint=EODHD_EARNINGS_TRENDS_ENDPOINT,
                        message=(
                            f"EODHD trend code={code} 与请求 symbol "
                            f"{provider_symbol} 不一致，仍按响应顺序保留。"
                        ),
                    )
                )
            records.append(cast(dict[str, Any], raw_record))
        records_by_ticker[ticker] = tuple(records)
    return records_by_ticker


def _eodhd_eps_revision_metric(
    *,
    ticker: str,
    as_of: date,
    records: tuple[dict[str, Any], ...],
    issues: list[FmpValuationFetchIssue],
) -> SnapshotMetric | None:
    trend_record = _select_eodhd_eps_trend_record(records, as_of)
    if trend_record is None:
        issues.append(
            FmpValuationFetchIssue(
                severity=ValuationIssueSeverity.WARNING,
                code="missing_eodhd_eps_trend_record",
                ticker=ticker,
                endpoint=EODHD_EARNINGS_TRENDS_ENDPOINT,
                message="EODHD trends 未返回可用 annual/quarter EPS trend record。",
            )
        )
        return None
    current_eps = _numeric_fmp_field(trend_record, "epsTrendCurrent")
    base_eps = _numeric_fmp_field(trend_record, "epsTrend90daysAgo")
    if current_eps is None or current_eps <= 0:
        issues.append(
            FmpValuationFetchIssue(
                severity=ValuationIssueSeverity.WARNING,
                code="missing_eodhd_eps_trend_current",
                ticker=ticker,
                endpoint=EODHD_EARNINGS_TRENDS_ENDPOINT,
                message="EODHD trends 缺少正数 epsTrendCurrent，未生成 eps_revision_90d_pct。",
            )
        )
        return None
    if base_eps is None or base_eps <= 0:
        issues.append(
            FmpValuationFetchIssue(
                severity=ValuationIssueSeverity.WARNING,
                code="missing_eodhd_eps_trend_90d",
                ticker=ticker,
                endpoint=EODHD_EARNINGS_TRENDS_ENDPOINT,
                message="EODHD trends 缺少正数 epsTrend90daysAgo，未生成 eps_revision_90d_pct。",
            )
        )
        return None
    period = str(trend_record.get("period") or "").strip() or "unknown"
    estimate_date = str(trend_record.get("date") or "").strip() or "unknown"
    return SnapshotMetric(
        metric_id="eps_revision_90d_pct",
        value=(current_eps / base_eps - 1.0) * 100.0,
        unit="percent",
        period="trailing_90d",
        source_field="calendar/trends.epsTrendCurrent / epsTrend90daysAgo",
        notes=(
            f"EODHD trend period={period}; estimate date={estimate_date}; "
            f"baseline epsTrend90daysAgo={base_eps}."
        ),
    )


def _select_eodhd_eps_trend_record(
    records: tuple[dict[str, Any], ...],
    as_of: date,
) -> dict[str, Any] | None:
    dated_records: list[tuple[date | None, str, dict[str, Any]]] = []
    for record in records:
        raw_date = str(record.get("date") or "").strip()
        record_date: date | None = None
        if raw_date:
            try:
                record_date = date.fromisoformat(raw_date)
            except ValueError:
                record_date = None
        period = str(record.get("period") or "").strip()
        if period not in {"+1y", "0y", "+1q", "0q"}:
            continue
        dated_records.append((record_date, period, record))
    if not dated_records:
        return None

    future_or_current = [
        item for item in dated_records if item[0] is None or item[0] >= as_of
    ]
    candidates = future_or_current or dated_records
    period_rank = {"+1y": 0, "0y": 1, "+1q": 2, "0q": 3}
    return sorted(
        candidates,
        key=lambda item: (
            period_rank.get(item[1], 99),
            item[0] or date.max,
        ),
    )[0][2]


def _merged_eodhd_trend_snapshot(
    *,
    base: ValuationSnapshot,
    as_of: date,
    captured_at: date,
    downloaded_at: datetime,
    trend_metric: SnapshotMetric,
) -> ValuationSnapshot:
    expectation_metrics = [
        metric
        for metric in base.expectation_metrics
        if metric.metric_id != trend_metric.metric_id
    ]
    expectation_metrics.append(trend_metric)
    date_token = as_of.isoformat().replace("-", "_")
    source_url = "; ".join(
        item
        for item in (
            base.source_url,
            f"{EODHD_BASE_URL}/{EODHD_EARNINGS_TRENDS_ENDPOINT}",
        )
        if item
    )
    base_note = f"base_snapshot={base.snapshot_id}"
    notes = (
        f"{base.notes} "
        if base.notes
        else ""
    ) + (
        "EODHD Earnings Trends overlay refreshed eps_revision_90d_pct; "
        f"{base_note}; downloaded_at={downloaded_at.isoformat()}."
    )
    return base.model_copy(
        update={
            "snapshot_id": (
                f"merged_{base.ticker.lower()}_valuation_eodhd_trends_{date_token}"
            ),
            "as_of": as_of,
            "source_type": "paid_vendor",
            "source_name": f"{EODHD_SOURCE_NAME} + {base.source_name}",
            "source_url": source_url,
            "captured_at": captured_at,
            "point_in_time_class": "captured_snapshot",
            "history_source_class": "vendor_current_trend",
            "confidence_reason": (
                "估值指标、估值分位和拥挤度继承基础快照；"
                "eps_revision_90d_pct 来自 EODHD 当前 trend 字段，"
                "只表示采集日可见的供应商趋势摘要，不是严格 PIT estimates archive。"
            ),
            "backtest_use": "captured_at_forward_only",
            "expectation_metrics": expectation_metrics,
            "notes": notes,
        }
    )


def _fmp_analyst_history_snapshot_from_raw(
    raw: Any,
    source_path: Path,
) -> FmpAnalystEstimateHistorySnapshot:
    if not isinstance(raw, dict):
        raise ValueError(f"FMP analyst estimate history is not an object: {source_path}")
    records = raw.get("records")
    if not isinstance(records, list) or not all(isinstance(item, dict) for item in records):
        raise ValueError(f"FMP analyst estimate history records are invalid: {source_path}")
    record_tuple = tuple(cast(dict[str, Any], item) for item in records)
    checksum = _json_checksum(record_tuple)
    recorded_checksum = str(raw.get("checksum_sha256") or "")
    if recorded_checksum and recorded_checksum != checksum:
        raise ValueError(f"FMP analyst estimate history checksum mismatch: {source_path}")
    raw_request_parameters = raw.get("request_parameters")
    request_parameters = (
        cast(dict[str, object], raw_request_parameters)
        if isinstance(raw_request_parameters, dict)
        else {}
    )
    return FmpAnalystEstimateHistorySnapshot(
        ticker=str(raw.get("ticker") or "").upper(),
        as_of=date.fromisoformat(str(raw.get("as_of"))),
        captured_at=date.fromisoformat(str(raw.get("captured_at"))),
        downloaded_at=_parse_iso_datetime(str(raw.get("downloaded_at"))),
        endpoint=str(raw.get("endpoint") or f"{FMP_BASE_URL}/analyst-estimates"),
        request_parameters=request_parameters,
        row_count=int(raw.get("row_count") or len(record_tuple)),
        checksum_sha256=recorded_checksum or checksum,
        records=record_tuple,
        source_path=source_path,
    )


def _fmp_snapshot_from_payload(
    *,
    ticker: str,
    as_of: date,
    captured_at: date,
    payload: dict[str, list[dict[str, Any]]],
    historical_estimates: tuple[FmpAnalystEstimateHistorySnapshot, ...],
    valuation_history: tuple[LoadedValuationSnapshot, ...],
    eps_revision_lookback_days: int,
    eps_revision_tolerance_days: int,
    minimum_valuation_history_points: int,
    issues: list[FmpValuationFetchIssue],
) -> ValuationSnapshot | None:
    quote = _first_fmp_record(payload.get("quote-short", ()), ticker)
    key_metrics = _first_fmp_record(payload.get("key-metrics-ttm", ()), ticker)
    ratios = _first_fmp_record(payload.get("ratios-ttm", ()), ticker)
    estimates = _dated_fmp_estimates(
        ticker,
        payload.get("analyst-estimates", ()),
        issues,
    )
    next_estimate = _first_estimate_on_or_after(estimates, as_of)
    previous_estimate = (
        _previous_estimate_before(estimates, next_estimate[0])
        if next_estimate is not None
        else None
    )

    valuation_metrics: list[SnapshotMetric] = []
    expectation_metrics: list[SnapshotMetric] = []
    _append_fmp_forward_pe_metric(
        ticker=ticker,
        quote=quote,
        next_estimate=next_estimate,
        metrics=valuation_metrics,
        issues=issues,
    )
    _append_fmp_ev_sales_metric(
        ticker=ticker,
        key_metrics=key_metrics,
        metrics=valuation_metrics,
        issues=issues,
    )
    _append_fmp_peg_metric(
        ticker=ticker,
        ratios=ratios,
        metrics=valuation_metrics,
        issues=issues,
    )
    _append_fmp_revenue_growth_metric(
        ticker=ticker,
        next_estimate=next_estimate,
        previous_estimate=previous_estimate,
        metrics=expectation_metrics,
        issues=issues,
    )
    _append_fmp_eps_revision_metric(
        ticker=ticker,
        as_of=as_of,
        next_estimate=next_estimate,
        history=historical_estimates,
        metrics=expectation_metrics,
        issues=issues,
        lookback_days=eps_revision_lookback_days,
        tolerance_days=eps_revision_tolerance_days,
    )
    valuation_percentile = _fmp_local_valuation_percentile(
        ticker=ticker,
        as_of=as_of,
        current_metrics=valuation_metrics,
        history=valuation_history,
        minimum_points=minimum_valuation_history_points,
        issues=issues,
    )

    if not valuation_metrics:
        issues.append(
            FmpValuationFetchIssue(
                severity=ValuationIssueSeverity.ERROR,
                code="missing_fmp_valuation_metrics",
                ticker=ticker,
                message="FMP 返回中没有可用估值倍数，未生成估值快照。",
            )
        )
        return None

    return ValuationSnapshot(
        snapshot_id=f"fmp_{ticker.lower()}_valuation_{as_of.isoformat().replace('-', '_')}",
        ticker=ticker,
        as_of=as_of,
        source_type="paid_vendor",
        source_name=FMP_SOURCE_NAME,
        source_url=", ".join(f"{FMP_BASE_URL}/{endpoint}" for endpoint in FMP_ENDPOINTS),
        captured_at=captured_at,
        point_in_time_class="captured_snapshot",
        history_source_class=(
            "captured_snapshot_history"
            if valuation_percentile is not None
            else "none"
        ),
        confidence_level="medium",
        confidence_reason=(
            "当前估值是采集日供应商快照；估值分位只使用本地可见历史快照，"
            "不等同于供应商真实 PIT archive。"
        ),
        backtest_use="captured_at_forward_only",
        valuation_metrics=valuation_metrics,
        expectation_metrics=expectation_metrics,
        valuation_percentile=valuation_percentile,
        overall_assessment="unknown",
        notes=(
            "FMP API 拉取；forward_pe 使用当前 quote 与最近未来 annual EPS estimate；"
            "revenue_growth_next_12m_pct 为 annual estimate 代理口径；"
            "eps_revision_90d_pct 使用历史 analyst estimate 快照计算。"
        ),
    )


def _fmp_local_valuation_percentile(
    *,
    ticker: str,
    as_of: date,
    current_metrics: list[SnapshotMetric],
    history: tuple[LoadedValuationSnapshot, ...],
    minimum_points: int,
    issues: list[FmpValuationFetchIssue],
) -> float | None:
    metric_percentiles: list[float] = []
    available_points = 0
    for metric in current_metrics:
        historical_values = _valuation_history_values(
            ticker=ticker,
            metric_id=metric.metric_id,
            as_of=as_of,
            history=history,
        )
        available_points = max(available_points, len(historical_values))
        if len(historical_values) < minimum_points:
            continue
        lower_or_equal_count = sum(value <= metric.value for value in historical_values)
        metric_percentiles.append(lower_or_equal_count / len(historical_values) * 100.0)

    if not metric_percentiles:
        issues.append(
            FmpValuationFetchIssue(
                severity=ValuationIssueSeverity.WARNING,
                code="missing_valuation_percentile_history",
                ticker=ticker,
                message=(
                    "本地历史估值快照不足，无法生成 valuation_percentile；"
                    f"需要每个 metric 至少 {minimum_points} 个历史点，"
                    f"当前最多 {available_points} 个。"
                ),
            )
        )
        return None

    return _median_float(metric_percentiles)


def _valuation_history_values(
    *,
    ticker: str,
    metric_id: str,
    as_of: date,
    history: tuple[LoadedValuationSnapshot, ...],
) -> list[float]:
    values: list[float] = []
    for loaded in history:
        snapshot = loaded.snapshot
        if (
            snapshot.ticker.upper() != ticker
            or snapshot.source_type == "public_convenience"
            or snapshot.as_of >= as_of
            or snapshot.captured_at > as_of
        ):
            continue
        for metric in snapshot.valuation_metrics:
            if metric.metric_id == metric_id and math.isfinite(metric.value):
                values.append(metric.value)
    return values


def _median_float(values: list[float]) -> float:
    ordered = sorted(values)
    midpoint = len(ordered) // 2
    if len(ordered) % 2:
        return float(ordered[midpoint])
    return float((ordered[midpoint - 1] + ordered[midpoint]) / 2)


def _append_fmp_forward_pe_metric(
    *,
    ticker: str,
    quote: dict[str, Any] | None,
    next_estimate: tuple[date, dict[str, Any]] | None,
    metrics: list[SnapshotMetric],
    issues: list[FmpValuationFetchIssue],
) -> None:
    price = _numeric_fmp_field(quote, "price")
    eps_avg = _numeric_fmp_field(next_estimate[1] if next_estimate else None, "epsAvg")
    if price is None or eps_avg is None or eps_avg <= 0:
        issues.append(
            FmpValuationFetchIssue(
                severity=ValuationIssueSeverity.WARNING,
                code="missing_forward_pe_inputs",
                ticker=ticker,
                endpoint="quote-short, analyst-estimates",
                message="缺少 price 或未来 annual epsAvg，无法计算 forward_pe。",
            )
        )
        return

    estimate_date = next_estimate[0].isoformat() if next_estimate else ""
    metrics.append(
        SnapshotMetric(
            metric_id="forward_pe",
            value=price / eps_avg,
            unit="ratio",
            period="next_annual_estimate",
            source_field="quote-short.price / analyst-estimates.epsAvg",
            notes=f"EPS estimate date: {estimate_date}.",
        )
    )


def _append_fmp_ev_sales_metric(
    *,
    ticker: str,
    key_metrics: dict[str, Any] | None,
    metrics: list[SnapshotMetric],
    issues: list[FmpValuationFetchIssue],
) -> None:
    ev_sales = _numeric_fmp_field(key_metrics, "evToSalesTTM")
    if ev_sales is None:
        issues.append(
            FmpValuationFetchIssue(
                severity=ValuationIssueSeverity.WARNING,
                code="missing_ev_sales",
                ticker=ticker,
                endpoint="key-metrics-ttm",
                message="缺少 evToSalesTTM，未生成 ev_sales 指标。",
            )
        )
        return
    if ev_sales < 0:
        issues.append(
            FmpValuationFetchIssue(
                severity=ValuationIssueSeverity.WARNING,
                code="invalid_fmp_valuation_multiple",
                ticker=ticker,
                endpoint="key-metrics-ttm",
                message="FMP 返回负数 evToSalesTTM，估值倍数不可用，未生成 ev_sales 指标。",
            )
        )
        return
    metrics.append(
        SnapshotMetric(
            metric_id="ev_sales",
            value=ev_sales,
            unit="ratio",
            period="ttm",
            source_field="key-metrics-ttm.evToSalesTTM",
        )
    )


def _append_fmp_peg_metric(
    *,
    ticker: str,
    ratios: dict[str, Any] | None,
    metrics: list[SnapshotMetric],
    issues: list[FmpValuationFetchIssue],
) -> None:
    source_field = "forwardPriceToEarningsGrowthRatioTTM"
    peg = _numeric_fmp_field(ratios, source_field)
    period = "next_12m"
    if peg is None:
        source_field = "priceToEarningsGrowthRatioTTM"
        peg = _numeric_fmp_field(ratios, source_field)
        period = "ttm"
    if peg is None:
        issues.append(
            FmpValuationFetchIssue(
                severity=ValuationIssueSeverity.WARNING,
                code="missing_peg",
                ticker=ticker,
                endpoint="ratios-ttm",
                message="缺少 PEG 字段，未生成 peg 指标。",
            )
        )
        return
    if peg < 0:
        issues.append(
            FmpValuationFetchIssue(
                severity=ValuationIssueSeverity.WARNING,
                code="invalid_fmp_valuation_multiple",
                ticker=ticker,
                endpoint="ratios-ttm",
                message=f"FMP 返回负数 {source_field}，估值倍数不可用，未生成 peg 指标。",
            )
        )
        return
    metrics.append(
        SnapshotMetric(
            metric_id="peg",
            value=peg,
            unit="ratio",
            period=period,
            source_field=f"ratios-ttm.{source_field}",
        )
    )


def _append_fmp_revenue_growth_metric(
    *,
    ticker: str,
    next_estimate: tuple[date, dict[str, Any]] | None,
    previous_estimate: tuple[date, dict[str, Any]] | None,
    metrics: list[SnapshotMetric],
    issues: list[FmpValuationFetchIssue],
) -> None:
    next_revenue = _numeric_fmp_field(next_estimate[1] if next_estimate else None, "revenueAvg")
    base_revenue = _numeric_fmp_field(
        previous_estimate[1] if previous_estimate else None,
        "revenueAvg",
    )
    if (
        next_estimate is None
        or previous_estimate is None
        or next_revenue is None
        or base_revenue is None
        or base_revenue <= 0
    ):
        issues.append(
            FmpValuationFetchIssue(
                severity=ValuationIssueSeverity.WARNING,
                code="missing_revenue_growth_inputs",
                ticker=ticker,
                endpoint="analyst-estimates",
                message=(
                    "缺少最近未来 annual revenueAvg 或上一年度 revenueAvg，"
                    "未生成 revenue_growth_next_12m_pct。"
                ),
            )
        )
        return

    growth_pct = (next_revenue / base_revenue - 1.0) * 100.0
    metrics.append(
        SnapshotMetric(
            metric_id="revenue_growth_next_12m_pct",
            value=growth_pct,
            unit="percent",
            period="next_annual_estimate",
            source_field="analyst-estimates.revenueAvg",
            notes=(
                f"Proxy from {previous_estimate[0].isoformat()} to "
                f"{next_estimate[0].isoformat()} annual estimates."
            ),
        )
    )
    issues.append(
        FmpValuationFetchIssue(
            severity=ValuationIssueSeverity.WARNING,
            code="revenue_growth_proxy",
            ticker=ticker,
            endpoint="analyst-estimates",
            message=(
                "revenue_growth_next_12m_pct 使用 annual estimates 的最近未来年度相对上一年度，"
                "不是滚动 NTM 共识增速。"
            ),
        )
    )


def _append_fmp_eps_revision_metric(
    *,
    ticker: str,
    as_of: date,
    next_estimate: tuple[date, dict[str, Any]] | None,
    history: tuple[FmpAnalystEstimateHistorySnapshot, ...],
    metrics: list[SnapshotMetric],
    issues: list[FmpValuationFetchIssue],
    lookback_days: int,
    tolerance_days: int,
) -> None:
    current_eps = _numeric_fmp_field(next_estimate[1] if next_estimate else None, "epsAvg")
    if next_estimate is None or current_eps is None or current_eps <= 0:
        issues.append(
            FmpValuationFetchIssue(
                severity=ValuationIssueSeverity.WARNING,
                code="missing_eps_revision_current_estimate",
                ticker=ticker,
                endpoint="analyst-estimates",
                message="缺少当前未来 annual epsAvg，无法计算 eps_revision_90d_pct。",
            )
        )
        return

    base = _select_eps_revision_base(
        ticker=ticker,
        estimate_date=next_estimate[0],
        as_of=as_of,
        history=history,
        lookback_days=lookback_days,
        tolerance_days=tolerance_days,
    )
    if base is None:
        baseline_date = as_of.toordinal() - lookback_days
        issues.append(
            FmpValuationFetchIssue(
                severity=ValuationIssueSeverity.WARNING,
                code="missing_eps_revision_history",
                ticker=ticker,
                endpoint="analyst-estimates",
                message=(
                    "缺少同一 fiscal estimate date 且接近 "
                    f"{date.fromordinal(baseline_date).isoformat()} 的历史 epsAvg，"
                    "未生成 eps_revision_90d_pct。"
                ),
            )
        )
        return

    base_eps = _numeric_fmp_field(base.record, "epsAvg")
    if base_eps is None or base_eps <= 0:
        issues.append(
            FmpValuationFetchIssue(
                severity=ValuationIssueSeverity.WARNING,
                code="invalid_eps_revision_history",
                ticker=ticker,
                endpoint="analyst-estimates",
                message="历史 analyst-estimates epsAvg 缺失或非正数，未生成 eps_revision_90d_pct。",
            )
        )
        return

    metrics.append(
        SnapshotMetric(
            metric_id="eps_revision_90d_pct",
            value=(current_eps / base_eps - 1.0) * 100.0,
            unit="percent",
            period="trailing_90d",
            source_field="analyst-estimates.epsAvg",
            notes=(
                f"Estimate date: {next_estimate[0].isoformat()}; "
                f"baseline captured_at: {base.captured_at.isoformat()}; "
                f"baseline epsAvg: {base_eps}."
            ),
        )
    )


@dataclass(frozen=True)
class _FmpEpsRevisionBase:
    captured_at: date
    record: dict[str, Any]
    source_path: Path | None


def _select_eps_revision_base(
    *,
    ticker: str,
    estimate_date: date,
    as_of: date,
    history: tuple[FmpAnalystEstimateHistorySnapshot, ...],
    lookback_days: int,
    tolerance_days: int,
) -> _FmpEpsRevisionBase | None:
    candidates: list[tuple[int, date, dict[str, Any], Path | None]] = []
    for snapshot in history:
        if snapshot.ticker != ticker or snapshot.captured_at >= as_of:
            continue
        age_days = (as_of - snapshot.captured_at).days
        distance = abs(age_days - lookback_days)
        if distance > tolerance_days:
            continue
        record = _estimate_record_for_date(snapshot.records, estimate_date)
        if record is None:
            continue
        candidates.append((distance, snapshot.captured_at, record, snapshot.source_path))

    if not candidates:
        return None

    distance, captured_at, record, source_path = sorted(
        candidates,
        key=lambda item: (item[0], -item[1].toordinal()),
    )[0]
    _ = distance
    return _FmpEpsRevisionBase(
        captured_at=captured_at,
        record=record,
        source_path=source_path,
    )


def _estimate_record_for_date(
    records: tuple[dict[str, Any], ...] | list[dict[str, Any]],
    estimate_date: date,
) -> dict[str, Any] | None:
    for record in records:
        raw_date = str(record.get("date") or "").strip()
        if raw_date == estimate_date.isoformat():
            return record
    return None


def _first_fmp_record(
    records: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    ticker: str,
) -> dict[str, Any] | None:
    if not records:
        return None
    provider_symbol = _fmp_provider_symbol(ticker)
    for record in records:
        symbol = str(record.get("symbol") or record.get("ticker") or "").upper()
        if symbol in {ticker, provider_symbol}:
            return record
    return records[0]


def _dated_fmp_estimates(
    ticker: str,
    records: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    issues: list[FmpValuationFetchIssue],
) -> list[tuple[date, dict[str, Any]]]:
    estimates: list[tuple[date, dict[str, Any]]] = []
    for record in records:
        raw_date = str(record.get("date") or "").strip()
        try:
            estimate_date = date.fromisoformat(raw_date)
        except ValueError:
            issues.append(
                FmpValuationFetchIssue(
                    severity=ValuationIssueSeverity.WARNING,
                    code="invalid_estimate_date",
                    ticker=ticker,
                    endpoint="analyst-estimates",
                    message=f"analyst-estimates.date 无法解析：{raw_date}",
                )
            )
            continue
        estimates.append((estimate_date, record))
    return sorted(estimates, key=lambda item: item[0])


def _first_estimate_on_or_after(
    estimates: list[tuple[date, dict[str, Any]]],
    as_of: date,
) -> tuple[date, dict[str, Any]] | None:
    for estimate in estimates:
        if estimate[0] >= as_of:
            return estimate
    return None


def _previous_estimate_before(
    estimates: list[tuple[date, dict[str, Any]]],
    target_date: date,
) -> tuple[date, dict[str, Any]] | None:
    previous = [estimate for estimate in estimates if estimate[0] < target_date]
    return previous[-1] if previous else None


def _numeric_fmp_field(record: dict[str, Any] | None, field_name: str) -> float | None:
    if record is None:
        return None
    raw_value = record.get(field_name)
    if raw_value is None or raw_value == "":
        return None
    try:
        value = float(raw_value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(value):
        return None
    return value


def _normalize_tickers(tickers: list[str] | tuple[str, ...]) -> list[str]:
    seen: set[str] = set()
    normalized: list[str] = []
    for ticker in tickers:
        value = ticker.strip().upper()
        if not value or value in seen:
            continue
        normalized.append(value)
        seen.add(value)
    return normalized


def _fmp_provider_symbol(ticker: str) -> str:
    normalized = ticker.strip().upper()
    return FMP_SYMBOL_ALIASES.get(normalized, normalized)


def _eodhd_provider_symbol(ticker: str) -> str:
    normalized = ticker.strip().upper()
    if normalized in EODHD_SYMBOL_ALIASES:
        return EODHD_SYMBOL_ALIASES[normalized]
    if "." in normalized:
        return normalized
    return f"{normalized}.US"


def _fmp_symbol_alias_summary(tickers: tuple[str, ...]) -> str:
    aliases = [
        f"{ticker}->{_fmp_provider_symbol(ticker)}"
        for ticker in tickers
        if _fmp_provider_symbol(ticker) != ticker
    ]
    return ", ".join(aliases)


def _eodhd_symbol_alias_summary(tickers: tuple[str, ...]) -> str:
    aliases = [
        f"{ticker}->{_eodhd_provider_symbol(ticker)}"
        for ticker in tickers
        if _eodhd_provider_symbol(ticker) != ticker
    ]
    return ", ".join(aliases)


def _json_checksum(payload: object) -> str:
    encoded = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _parse_iso_datetime(value: str) -> datetime:
    normalized = value.strip()
    if normalized.endswith("Z"):
        normalized = f"{normalized[:-1]}+00:00"
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed


def _parse_optional_iso_datetime(value: str) -> datetime | None:
    try:
        return _parse_iso_datetime(value)
    except ValueError:
        return None


def _sanitize_fmp_error_message(exc: Exception) -> str:
    message = str(exc)
    return re.sub(
        r"(apikey=)[^&\s]+",
        r"\1<redacted>",
        message,
        flags=re.IGNORECASE,
    )


def _sanitize_eodhd_error_message(exc: Exception) -> str:
    message = str(exc)
    return re.sub(
        r"(api_token=)[^&\s]+",
        r"\1<redacted>",
        message,
        flags=re.IGNORECASE,
    )


def _should_retry_fmp_http_error(exc: Exception) -> bool:
    response = getattr(exc, "response", None)
    status_code = getattr(response, "status_code", None)
    if isinstance(status_code, int):
        return status_code == 429 or status_code >= 500

    exception_name = type(exc).__name__.lower()
    if any(token in exception_name for token in ("ssl", "timeout", "connection")):
        return True

    message = str(exc).lower()
    return any(token in message for token in ("ssl", "timed out", "connection reset"))


def _snapshot_from_row(
    path: Path,
    row_number: int,
    row: dict[str, str | None],
    issues: list[ValuationImportIssue],
) -> ValuationSnapshot | None:
    snapshot_id = _cell(row, "snapshot_id")
    ticker = _cell(row, "ticker").upper()
    for column in REQUIRED_COLUMNS:
        if not _cell(row, column):
            _append_row_issue(
                issues,
                path,
                row_number,
                row,
                column,
                "missing_required_value",
                f"必填列 {column} 为空。",
            )

    as_of = _parse_date(path, row_number, row, "as_of", issues)
    captured_at = _parse_date(path, row_number, row, "captured_at", issues)
    valuation_percentile = _parse_optional_float(
        path,
        row_number,
        row,
        "valuation_percentile",
        issues,
    )
    valuation_metrics = _parse_metrics(
        path,
        row_number,
        row,
        _VALUATION_METRIC_DEFINITIONS,
        issues,
    )
    expectation_metrics = _parse_metrics(
        path,
        row_number,
        row,
        _EXPECTATION_METRIC_DEFINITIONS,
        issues,
    )
    crowding_signal = _parse_crowding_signal(path, row_number, row, issues)

    if any(issue.severity == ValuationIssueSeverity.ERROR for issue in issues):
        return None

    raw_snapshot: dict[str, Any] = {
        "snapshot_id": snapshot_id,
        "ticker": ticker,
        "as_of": as_of,
        "source_type": _cell(row, "source_type"),
        "source_name": _cell(row, "source_name"),
        "source_url": _cell(row, "source_url"),
        "captured_at": captured_at,
        "point_in_time_class": _cell(row, "point_in_time_class") or "captured_snapshot",
        "history_source_class": _cell(row, "history_source_class") or "unknown",
        "confidence_level": _cell(row, "confidence_level") or "medium",
        "confidence_reason": _cell(row, "confidence_reason"),
        "backtest_use": _cell(row, "backtest_use") or "captured_at_forward_only",
        "valuation_metrics": valuation_metrics,
        "expectation_metrics": expectation_metrics,
        "crowding_signals": [crowding_signal] if crowding_signal is not None else [],
        "valuation_percentile": valuation_percentile,
        "overall_assessment": _cell(row, "overall_assessment") or "unknown",
        "notes": _cell(row, "notes"),
    }
    try:
        snapshot = ValuationSnapshot.model_validate(raw_snapshot)
    except ValidationError as exc:
        first_error = exc.errors()[0] if exc.errors() else None
        schema_column: str | None = None
        if first_error is not None:
            schema_column = (
                ".".join(str(part) for part in first_error.get("loc", ())) or None
            )
        issues.append(
            ValuationImportIssue(
                severity=ValuationIssueSeverity.ERROR,
                code="snapshot_schema_invalid",
                row_number=row_number,
                snapshot_id=snapshot_id or None,
                ticker=ticker or None,
                column=schema_column,
                path=path,
                message=_compact_validation_error(exc),
            )
        )
        return None

    if snapshot.source_type == "public_convenience":
        issues.append(
            ValuationImportIssue(
                severity=ValuationIssueSeverity.WARNING,
                code="public_convenience_source",
                row_number=row_number,
                snapshot_id=snapshot.snapshot_id,
                ticker=snapshot.ticker,
                column="source_type",
                path=path,
                message="公开便利来源可以导入，但只能作为辅助，不能由导入器标记为自动评分可用。",
            )
        )
    return snapshot


def _parse_metrics(
    path: Path,
    row_number: int,
    row: dict[str, str | None],
    definitions: dict[str, tuple[str, str]],
    issues: list[ValuationImportIssue],
) -> list[SnapshotMetric]:
    metrics: list[SnapshotMetric] = []
    for column, (unit, period) in definitions.items():
        raw_value = _cell(row, column)
        if not raw_value:
            continue
        value = _parse_float(path, row_number, row, column, issues)
        if value is None:
            continue
        metrics.append(
            SnapshotMetric(
                metric_id=column,
                value=value,
                unit=unit,
                period=period,
                source_field=column,
            )
        )
    return metrics


def _parse_crowding_signal(
    path: Path,
    row_number: int,
    row: dict[str, str | None],
    issues: list[ValuationImportIssue],
) -> CrowdingSignal | None:
    status = _cell(row, "crowding_status").lower()
    evidence_source = _cell(row, "crowding_evidence_source")
    has_crowding_data = any(
        _cell(row, column)
        for column in (
            "crowding_status",
            "crowding_signal_name",
            "crowding_evidence_source",
            "crowding_updated_at",
            "crowding_notes",
        )
    )
    if not has_crowding_data:
        return None

    if status not in VALID_CROWDING_STATUSES:
        _append_row_issue(
            issues,
            path,
            row_number,
            row,
            "crowding_status",
            "invalid_crowding_status",
            "crowding_status 必须是 normal、elevated、extreme 或 unknown。",
        )
        return None
    if not evidence_source:
        _append_row_issue(
            issues,
            path,
            row_number,
            row,
            "crowding_evidence_source",
            "missing_crowding_evidence",
            "创建 CrowdingSignal 时必须提供 crowding_evidence_source。",
        )
        return None

    updated_at = _parse_date(path, row_number, row, "crowding_updated_at", issues)
    if updated_at is None:
        if not _cell(row, "crowding_updated_at"):
            _append_row_issue(
                issues,
                path,
                row_number,
                row,
                "crowding_updated_at",
                "missing_crowding_updated_at",
                "创建 CrowdingSignal 时必须提供 crowding_updated_at。",
            )
        return None

    name = _cell(row, "crowding_signal_name") or "Crowding signal"
    return CrowdingSignal(
        signal_id=_metric_id_from_name(name),
        name=name,
        status=cast(CrowdingStatus, status),
        evidence_source=evidence_source,
        updated_at=updated_at,
        notes=_cell(row, "crowding_notes"),
    )


def _parse_date(
    path: Path,
    row_number: int,
    row: dict[str, str | None],
    column: str,
    issues: list[ValuationImportIssue],
) -> date | None:
    raw_value = _cell(row, column)
    if not raw_value:
        return None
    try:
        return date.fromisoformat(raw_value)
    except ValueError:
        _append_row_issue(
            issues,
            path,
            row_number,
            row,
            column,
            "invalid_date",
            f"{column} 必须使用 YYYY-MM-DD 日期格式。",
        )
        return None


def _parse_optional_float(
    path: Path,
    row_number: int,
    row: dict[str, str | None],
    column: str,
    issues: list[ValuationImportIssue],
) -> float | None:
    if not _cell(row, column):
        return None
    return _parse_float(path, row_number, row, column, issues)


def _parse_float(
    path: Path,
    row_number: int,
    row: dict[str, str | None],
    column: str,
    issues: list[ValuationImportIssue],
) -> float | None:
    raw_value = _cell(row, column)
    try:
        return float(raw_value)
    except ValueError:
        _append_row_issue(
            issues,
            path,
            row_number,
            row,
            column,
            "invalid_numeric",
            f"{column} 必须是可解析的数值。",
        )
        return None


def _append_row_issue(
    issues: list[ValuationImportIssue],
    path: Path,
    row_number: int,
    row: dict[str, str | None],
    column: str,
    code: str,
    message: str,
) -> None:
    issues.append(
        ValuationImportIssue(
            severity=ValuationIssueSeverity.ERROR,
            code=code,
            message=message,
            row_number=row_number,
            snapshot_id=_cell(row, "snapshot_id") or None,
            ticker=_cell(row, "ticker").upper() or None,
            column=column,
            path=path,
        )
    )


def _cell(row: dict[str, str | None], column: str) -> str:
    return (row.get(column) or "").strip()


def _metric_id_from_name(name: str) -> str:
    value = re.sub(r"[^A-Za-z0-9_.-]+", "_", name.strip().lower()).strip("_")
    return value or "crowding_signal"


def _compact_validation_error(exc: ValidationError) -> str:
    first_error = exc.errors()[0] if exc.errors() else None
    if not first_error:
        return "valuation snapshot schema validation failed"
    location = ".".join(str(part) for part in first_error.get("loc", ()))
    message = str(first_error.get("msg", "schema validation failed"))
    return f"{location}: {message}" if location else message


def _escape_markdown_table(value: str) -> str:
    return value.replace("|", "\\|").replace("\n", " ")
