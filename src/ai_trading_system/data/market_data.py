from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, timedelta
from hashlib import sha256
from importlib import import_module
from io import BytesIO, StringIO
from pathlib import Path
from time import sleep
from typing import Any, Protocol, cast

import pandas as pd

from ai_trading_system.external_request_cache import (
    cached_requests_get,
    default_external_request_cache_dir,
    lookup_external_request_cache,
    sanitize_diagnostic_text,
    write_external_request_cache_response,
)

MARKETSTACK_EOD_URL = "https://api.marketstack.com/v2/eod"
MARKETSTACK_DEFAULT_SYMBOL_ALIASES: dict[str, str | None] = {
    "^VIX": None,
}
FMP_EOD_NON_SPLIT_ADJUSTED_URL = (
    "https://financialmodelingprep.com/stable/historical-price-eod/non-split-adjusted"
)
FMP_EOD_DIVIDEND_ADJUSTED_URL = (
    "https://financialmodelingprep.com/stable/historical-price-eod/dividend-adjusted"
)
FMP_DEFAULT_SYMBOL_ALIASES: dict[str, str | None] = {
    "^VIX": None,
}
CBOE_VIX_DAILY_PRICES_URL = "https://cdn.cboe.com/api/global/us_indices/daily_prices/VIX_History.csv"
CBOE_VIX_TICKER = "^VIX"


@dataclass(frozen=True)
class PriceRequest:
    tickers: list[str]
    start: date
    end: date
    interval: str = "1d"


@dataclass(frozen=True)
class RateRequest:
    series_ids: list[str]
    start: date
    end: date


class PriceDataProvider(Protocol):
    def download_prices(self, request: PriceRequest) -> pd.DataFrame:
        """Download adjusted OHLCV data."""


class RateDataProvider(Protocol):
    def download_rates(self, request: RateRequest) -> pd.DataFrame:
        """Download macro rate data."""


@dataclass(frozen=True)
class ProviderRequestDiagnostic:
    provider: str
    api_family: str
    endpoint: str
    stage: str
    method: str
    request_parameters: dict[str, object]
    cache_status: str
    cache_key: str | None = None
    cache_metadata_path: Path | None = None
    http_status: int | None = None
    error_code: str | None = None
    response_body_sha256: str | None = None
    response_body_size_bytes: int | None = None
    row_count_before_failure: int | None = None
    attempt_count: int | None = None
    max_attempts: int | None = None
    timeout_seconds: float | None = None
    exception_type: str | None = None
    exception_message: str | None = None


class ProviderDownloadError(RuntimeError):
    def __init__(self, message: str, diagnostic: ProviderRequestDiagnostic) -> None:
        super().__init__(message)
        self.diagnostic = diagnostic


@dataclass(frozen=True)
class CsvDataCache:
    root: Path

    def write_prices(self, prices: pd.DataFrame, filename: str = "prices_daily.csv") -> Path:
        return self._write_csv(prices, filename)

    def write_rates(self, rates: pd.DataFrame, filename: str = "rates_daily.csv") -> Path:
        return self._write_csv(rates, filename)

    def _write_csv(self, data: pd.DataFrame, filename: str) -> Path:
        self.root.mkdir(parents=True, exist_ok=True)
        output_path = self.root / filename
        data.to_csv(output_path, index=False)
        return output_path


@dataclass(frozen=True)
class YFinancePriceProvider:
    request_cache_dir: Path | str | None = None

    def download_prices(self, request: PriceRequest) -> pd.DataFrame:
        request_cache_dir = default_external_request_cache_dir(
            explicit_cache_dir=self.request_cache_dir,
        )
        cache_params = {
            "tickers": list(request.tickers),
            "start": request.start.isoformat(),
            "end": (request.end + timedelta(days=1)).isoformat(),
            "interval": request.interval,
            "group_by": "ticker",
            "auto_adjust": False,
        }
        cache_lookup = lookup_external_request_cache(
            provider="Yahoo Finance via yfinance",
            api_family="download",
            method="YFINANCE_DOWNLOAD",
            url="yfinance.download",
            params=cache_params,
            cache_dir=None if request_cache_dir is None else Path(request_cache_dir),
        )
        if cache_lookup.response is not None:
            raw = pd.read_pickle(BytesIO(cache_lookup.response.content))
            if not isinstance(raw, pd.DataFrame):
                raise TypeError("cached yfinance.download payload was not a pandas DataFrame")
            return normalize_yfinance_prices(raw, request.tickers)

        yfinance = cast(Any, import_module("yfinance"))

        raw = yfinance.download(
            tickers=request.tickers,
            start=request.start.isoformat(),
            end=(request.end + timedelta(days=1)).isoformat(),
            interval=request.interval,
            group_by="ticker",
            auto_adjust=False,
            progress=False,
            threads=True,
        )
        if not isinstance(raw, pd.DataFrame):
            raise TypeError("yfinance.download did not return a pandas DataFrame")
        if request_cache_dir is not None:
            buffer = BytesIO()
            raw.to_pickle(buffer)
            write_external_request_cache_response(
                provider="Yahoo Finance via yfinance",
                api_family="download",
                method="YFINANCE_DOWNLOAD",
                url="yfinance.download",
                params=cache_params,
                status_code=200,
                response_headers={},
                content=buffer.getvalue(),
                cache_dir=Path(request_cache_dir),
            )
        return normalize_yfinance_prices(raw, request.tickers)


@dataclass(frozen=True)
class MarketstackPriceProvider:
    api_key: str
    base_url: str = MARKETSTACK_EOD_URL
    page_limit: int = 1000
    symbol_aliases: dict[str, str | None] = field(
        default_factory=lambda: dict(MARKETSTACK_DEFAULT_SYMBOL_ALIASES)
    )
    requests_module: Any | None = None
    request_cache_dir: Path | str | None = None

    def __post_init__(self) -> None:
        if not self.api_key:
            raise ValueError("Marketstack API key must not be empty")
        if self.page_limit <= 0:
            raise ValueError("Marketstack page_limit must be positive")

    def download_prices(self, request: PriceRequest) -> pd.DataFrame:
        requests = self.requests_module or cast(Any, import_module("requests"))
        request_cache_dir = default_external_request_cache_dir(
            requests_module=self.requests_module,
            explicit_cache_dir=self.request_cache_dir,
        )
        provider_symbols, provider_to_ticker = self._provider_symbols(request.tickers)
        if not provider_symbols:
            raise ValueError("Marketstack request has no supported tickers")

        records: list[dict[str, Any]] = []
        offset = 0
        while True:
            params = {
                "access_key": self.api_key,
                "symbols": ",".join(provider_symbols),
                "date_from": request.start.isoformat(),
                "date_to": request.end.isoformat(),
                "limit": str(self.page_limit),
                "offset": str(offset),
            }
            cache_lookup = lookup_external_request_cache(
                provider="Marketstack",
                api_family="eod_daily_prices",
                method="GET",
                url=self.base_url,
                params=params,
                cache_dir=None if request_cache_dir is None else Path(request_cache_dir),
            )
            try:
                response = cached_requests_get(
                    provider="Marketstack",
                    api_family="eod_daily_prices",
                    url=self.base_url,
                    params=params,
                    timeout=30,
                    requests_module=requests,
                    cache_dir=request_cache_dir,
                )
            except Exception as exc:
                diagnostic = _marketstack_diagnostic(
                    base_url=self.base_url,
                    params=params,
                    cache_key=cache_lookup.cache_key,
                    cache_metadata_path=(
                        None if request_cache_dir is None else cache_lookup.metadata_path
                    ),
                    cache_status="DISABLED" if request_cache_dir is None else "MISS_NO_RESPONSE",
                    stage="http_request",
                    row_count_before_failure=len(records),
                    exception=exc,
                    api_key=self.api_key,
                )
                raise ProviderDownloadError(
                    "Marketstack request failed before receiving a cacheable response",
                    diagnostic,
                ) from exc

            try:
                payload = response.json()
            except ValueError as exc:
                diagnostic = _marketstack_diagnostic(
                    base_url=self.base_url,
                    params=params,
                    cache_key=response.cache_key,
                    cache_metadata_path=response.cache_metadata_path,
                    cache_status="HIT" if response.from_cache else "MISS_WRITTEN",
                    stage="response_json",
                    row_count_before_failure=len(records),
                    response=response,
                    exception=exc,
                    api_key=self.api_key,
                )
                raise ProviderDownloadError(
                    "Marketstack response was not valid JSON",
                    diagnostic,
                ) from exc
            if not response.ok:
                error_code = _marketstack_error_code(payload)
                diagnostic = _marketstack_diagnostic(
                    base_url=self.base_url,
                    params=params,
                    cache_key=response.cache_key,
                    cache_metadata_path=response.cache_metadata_path,
                    cache_status="HIT" if response.from_cache else "MISS_WRITTEN",
                    stage="http_status",
                    row_count_before_failure=len(records),
                    response=response,
                    error_code=error_code,
                )
                raise ProviderDownloadError(
                    "Marketstack request failed: "
                    f"http_status={response.status_code}, "
                    f"error_code={error_code}",
                    diagnostic,
                )
            if isinstance(payload, dict) and payload.get("error"):
                error_code = _marketstack_error_code(payload)
                diagnostic = _marketstack_diagnostic(
                    base_url=self.base_url,
                    params=params,
                    cache_key=response.cache_key,
                    cache_metadata_path=response.cache_metadata_path,
                    cache_status="HIT" if response.from_cache else "MISS_WRITTEN",
                    stage="provider_error",
                    row_count_before_failure=len(records),
                    response=response,
                    error_code=error_code,
                )
                raise ProviderDownloadError(
                    "Marketstack response returned an error: "
                    f"error_code={error_code}",
                    diagnostic,
                )

            page_records = payload.get("data", []) if isinstance(payload, dict) else []
            if not isinstance(page_records, list):
                diagnostic = _marketstack_diagnostic(
                    base_url=self.base_url,
                    params=params,
                    cache_key=response.cache_key,
                    cache_metadata_path=response.cache_metadata_path,
                    cache_status="HIT" if response.from_cache else "MISS_WRITTEN",
                    stage="schema",
                    row_count_before_failure=len(records),
                    response=response,
                )
                raise ProviderDownloadError(
                    "Marketstack response data is not a list",
                    diagnostic,
                )
            records.extend(record for record in page_records if isinstance(record, dict))

            pagination = payload.get("pagination", {}) if isinstance(payload, dict) else {}
            count = int(pagination.get("count") or len(page_records))
            total = int(pagination.get("total") or len(records))
            if count <= 0 or len(records) >= total:
                break
            offset += count

        try:
            return normalize_marketstack_prices(records, provider_to_ticker)
        except ValueError as exc:
            diagnostic = ProviderRequestDiagnostic(
                provider="Marketstack",
                api_family="eod_daily_prices",
                endpoint=self.base_url,
                stage="normalize",
                method="GET",
                request_parameters={
                    "symbols": ",".join(provider_symbols),
                    "date_from": request.start.isoformat(),
                    "date_to": request.end.isoformat(),
                    "limit": str(self.page_limit),
                    "last_offset": str(offset),
                },
                cache_status="UNKNOWN",
                row_count_before_failure=len(records),
                exception_type=type(exc).__name__,
                exception_message=sanitize_diagnostic_text(
                    str(exc),
                    extra_secrets=(self.api_key,),
                ),
            )
            raise ProviderDownloadError(
                "Marketstack response normalization failed",
                diagnostic,
            ) from exc

    def _provider_symbols(self, tickers: list[str]) -> tuple[list[str], dict[str, str]]:
        provider_symbols: list[str] = []
        provider_to_ticker: dict[str, str] = {}
        for ticker in tickers:
            provider_symbol = self.symbol_aliases.get(ticker, ticker)
            if provider_symbol is None:
                continue
            provider_symbols.append(provider_symbol)
            provider_to_ticker[provider_symbol] = ticker
        return provider_symbols, provider_to_ticker


@dataclass(frozen=True)
class FmpPriceProvider:
    api_key: str
    base_url: str = FMP_EOD_NON_SPLIT_ADJUSTED_URL
    adjusted_url: str = FMP_EOD_DIVIDEND_ADJUSTED_URL
    symbol_aliases: dict[str, str | None] = field(
        default_factory=lambda: dict(FMP_DEFAULT_SYMBOL_ALIASES)
    )
    requests_module: Any | None = None
    request_cache_dir: Path | str | None = None

    def __post_init__(self) -> None:
        if not self.api_key.strip():
            raise ValueError("FMP API key must not be empty")

    def download_prices(self, request: PriceRequest) -> pd.DataFrame:
        requests = self.requests_module or cast(Any, import_module("requests"))
        request_cache_dir = default_external_request_cache_dir(
            requests_module=self.requests_module,
            explicit_cache_dir=self.request_cache_dir,
        )
        provider_symbols, provider_to_ticker = self._provider_symbols(request.tickers)
        if not provider_symbols:
            raise ValueError("FMP price request has no supported tickers")

        records: list[dict[str, Any]] = []
        for provider_symbol in provider_symbols:
            params = self._request_params(provider_symbol, request)
            page_records = _fetch_fmp_price_records(
                requests=requests,
                url=self.base_url,
                params=params,
                response_label="FMP price",
                request_cache_dir=request_cache_dir,
            )
            adjusted_records = _fetch_fmp_price_records(
                requests=requests,
                url=self.adjusted_url,
                params=params,
                response_label="FMP adjusted price",
                request_cache_dir=request_cache_dir,
            )
            if not page_records:
                raise ValueError(f"FMP price response was empty for symbol {provider_symbol}")
            adjusted_close_by_date = {
                str(record.get("date")): record.get("adjClose") for record in adjusted_records
            }
            missing_adjusted_dates = [
                str(record.get("date"))
                for record in page_records
                if adjusted_close_by_date.get(str(record.get("date"))) is None
            ]
            if missing_adjusted_dates:
                sample = ", ".join(missing_adjusted_dates[:3])
                raise ValueError(
                    "FMP adjusted price response missing adjClose for "
                    f"{provider_symbol}: {sample}"
                )
            records.extend(
                {
                    **_normalize_fmp_raw_price_record(record),
                    "adjClose": adjusted_close_by_date[str(record.get("date"))],
                    "_provider_symbol": provider_symbol,
                }
                for record in page_records
            )

        return normalize_fmp_prices(records, provider_to_ticker)

    def endpoint_summary(self) -> str:
        return f"{self.base_url}; {self.adjusted_url}"

    def provider_symbol_for(self, ticker: str) -> str | None:
        return self.symbol_aliases.get(ticker, ticker)

    def _request_params(self, provider_symbol: str, request: PriceRequest) -> dict[str, object]:
        return {
            "symbol": provider_symbol,
            "from": request.start.isoformat(),
            "to": request.end.isoformat(),
            "apikey": self.api_key,
        }

    def _provider_symbols(self, tickers: list[str]) -> tuple[list[str], dict[str, str]]:
        provider_symbols: list[str] = []
        provider_to_ticker: dict[str, str] = {}
        for ticker in tickers:
            provider_symbol = self.provider_symbol_for(ticker)
            if provider_symbol is None:
                continue
            provider_symbols.append(provider_symbol)
            provider_to_ticker[provider_symbol] = ticker
        return provider_symbols, provider_to_ticker


@dataclass(frozen=True)
class CboeVixPriceProvider:
    base_url: str = CBOE_VIX_DAILY_PRICES_URL
    ticker: str = CBOE_VIX_TICKER
    requests_module: Any | None = None
    request_cache_dir: Path | str | None = None

    def download_prices(self, request: PriceRequest) -> pd.DataFrame:
        if self.ticker not in request.tickers:
            raise ValueError(f"Cboe VIX request must include {self.ticker}")

        requests = self.requests_module or cast(Any, import_module("requests"))
        request_cache_dir = default_external_request_cache_dir(
            requests_module=self.requests_module,
            explicit_cache_dir=self.request_cache_dir,
        )
        response = cached_requests_get(
            provider="Cboe Global Markets",
            api_family="vix_daily_prices",
            url=self.base_url,
            timeout=30,
            requests_module=requests,
            cache_dir=request_cache_dir,
        )
        if not response.ok:
            raise ValueError(
                "Cboe VIX request failed: "
                f"http_status={response.status_code}"
            )

        raw = pd.read_csv(StringIO(str(response.text)))
        return normalize_cboe_vix_prices(
            raw,
            ticker=self.ticker,
            start=request.start,
            end=request.end,
        )


@dataclass(frozen=True)
class FredRateProvider:
    base_url = "https://fred.stlouisfed.org/graph/fredgraph.csv"
    requests_module: Any | None = None
    request_cache_dir: Path | str | None = None
    timeout_seconds: float = 60
    max_attempts: int = 2
    retry_backoff_seconds: float = 3

    def __post_init__(self) -> None:
        if self.timeout_seconds <= 0:
            raise ValueError("FRED timeout_seconds must be positive")
        if self.max_attempts <= 0:
            raise ValueError("FRED max_attempts must be positive")
        if self.retry_backoff_seconds < 0:
            raise ValueError("FRED retry_backoff_seconds must not be negative")

    def download_rates(self, request: RateRequest) -> pd.DataFrame:
        requests = self.requests_module or cast(Any, import_module("requests"))
        request_cache_dir = default_external_request_cache_dir(
            requests_module=self.requests_module,
            explicit_cache_dir=self.request_cache_dir,
        )
        frames: list[pd.DataFrame] = []

        for series_id in request.series_ids:
            params = {
                "id": series_id,
                "cosd": request.start.isoformat(),
                "coed": request.end.isoformat(),
            }
            cache_lookup = lookup_external_request_cache(
                provider="Federal Reserve Economic Data",
                api_family="fredgraph_csv",
                method="GET",
                url=self.base_url,
                params=params,
                cache_dir=None if request_cache_dir is None else Path(request_cache_dir),
            )
            rows_before_failure = sum(len(frame) for frame in frames)
            response = None
            last_exception: Exception | None = None
            for attempt in range(1, self.max_attempts + 1):
                try:
                    response = cached_requests_get(
                        provider="Federal Reserve Economic Data",
                        api_family="fredgraph_csv",
                        url=self.base_url,
                        params=params,
                        timeout=self.timeout_seconds,
                        requests_module=requests,
                        cache_dir=request_cache_dir,
                    )
                    break
                except Exception as exc:
                    last_exception = exc
                    if attempt < self.max_attempts and self.retry_backoff_seconds > 0:
                        sleep(self.retry_backoff_seconds)
            if response is None:
                diagnostic = _fred_diagnostic(
                    base_url=self.base_url,
                    params=params,
                    cache_key=cache_lookup.cache_key,
                    cache_metadata_path=(
                        None if request_cache_dir is None else cache_lookup.metadata_path
                    ),
                    cache_status="DISABLED" if request_cache_dir is None else "MISS_NO_RESPONSE",
                    stage="http_request",
                    row_count_before_failure=rows_before_failure,
                    exception=last_exception,
                    attempt_count=self.max_attempts,
                    max_attempts=self.max_attempts,
                    timeout_seconds=self.timeout_seconds,
                )
                raise ProviderDownloadError(
                    "FRED request failed before receiving a cacheable response",
                    diagnostic,
                ) from last_exception
            if not response.ok:
                diagnostic = _fred_diagnostic(
                    base_url=self.base_url,
                    params=params,
                    cache_key=response.cache_key,
                    cache_metadata_path=response.cache_metadata_path,
                    cache_status="HIT" if response.from_cache else "MISS_WRITTEN",
                    stage="http_status",
                    row_count_before_failure=rows_before_failure,
                    response=response,
                    error_code=str(response.status_code),
                )
                raise ProviderDownloadError(
                    f"FRED request failed: http_status={response.status_code}",
                    diagnostic,
                )
            try:
                frame = pd.read_csv(StringIO(str(response.text)))
            except Exception as exc:
                diagnostic = _fred_diagnostic(
                    base_url=self.base_url,
                    params=params,
                    cache_key=response.cache_key,
                    cache_metadata_path=response.cache_metadata_path,
                    cache_status="HIT" if response.from_cache else "MISS_WRITTEN",
                    stage="csv_parse",
                    row_count_before_failure=rows_before_failure,
                    response=response,
                    exception=exc,
                )
                raise ProviderDownloadError(
                    f"FRED response for {series_id} could not be parsed as CSV",
                    diagnostic,
                ) from exc
            if "observation_date" not in frame.columns or series_id not in frame.columns:
                diagnostic = _fred_diagnostic(
                    base_url=self.base_url,
                    params=params,
                    cache_key=response.cache_key,
                    cache_metadata_path=response.cache_metadata_path,
                    cache_status="HIT" if response.from_cache else "MISS_WRITTEN",
                    stage="schema",
                    row_count_before_failure=rows_before_failure,
                    response=response,
                )
                raise ProviderDownloadError(
                    f"FRED response for {series_id} has unexpected columns",
                    diagnostic,
                )

            series_frame = frame.rename(columns={"observation_date": "date", series_id: "value"})
            series_frame["series"] = series_id
            series_frame["value"] = pd.to_numeric(series_frame["value"], errors="coerce")
            frames.append(series_frame[["date", "series", "value"]])

        if not frames:
            raise ValueError("rate series list must not be empty")

        rates = pd.concat(frames, ignore_index=True)
        rates["date"] = pd.to_datetime(rates["date"]).dt.strftime("%Y-%m-%d")
        return rates.dropna(subset=["value"]).sort_values(["series", "date"]).reset_index(drop=True)


def normalize_yfinance_prices(raw: pd.DataFrame, tickers: list[str]) -> pd.DataFrame:
    if raw.empty:
        raise ValueError("price data is empty")
    if not tickers:
        raise ValueError("tickers must not be empty")

    frames: list[pd.DataFrame] = []
    if isinstance(raw.columns, pd.MultiIndex):
        level_zero = set(str(value) for value in raw.columns.get_level_values(0))
        level_one = set(str(value) for value in raw.columns.get_level_values(1))

        for ticker in tickers:
            if ticker in level_zero:
                ticker_frame = cast(pd.DataFrame, raw[ticker])
            elif ticker in level_one:
                ticker_frame = cast(pd.DataFrame, raw.xs(ticker, level=1, axis=1))
            else:
                continue
            frames.append(_normalize_single_ticker_prices(ticker_frame, ticker))
    else:
        if len(tickers) != 1:
            raise ValueError("single-index price data requires exactly one ticker")
        frames.append(_normalize_single_ticker_prices(raw, tickers[0]))

    if not frames:
        raise ValueError("no requested tickers were found in price data")

    prices = pd.concat(frames, ignore_index=True)
    return prices.sort_values(["ticker", "date"]).reset_index(drop=True)


def normalize_marketstack_prices(
    records: list[dict[str, Any]],
    provider_to_ticker: dict[str, str],
) -> pd.DataFrame:
    if not records:
        raise ValueError("Marketstack price data is empty")
    if not provider_to_ticker:
        raise ValueError("Marketstack provider symbol mapping must not be empty")

    raw = pd.DataFrame(records)
    required_columns = {"date", "symbol", "open", "high", "low", "close"}
    missing = sorted(required_columns - set(raw.columns))
    if missing:
        raise ValueError(f"Marketstack response missing columns: {', '.join(missing)}")

    frame = raw.copy()
    frame["ticker"] = frame["symbol"].map(provider_to_ticker)
    frame = frame.loc[frame["ticker"].notna()].copy()
    if frame.empty:
        raise ValueError("Marketstack response did not include requested tickers")

    frame["date"] = pd.to_datetime(frame["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    if "adj_close" not in frame.columns:
        frame["adj_close"] = frame["close"]
    if "volume" not in frame.columns:
        frame["volume"] = frame["adj_volume"] if "adj_volume" in frame.columns else pd.NA

    columns = ["date", "ticker", "open", "high", "low", "close", "adj_close", "volume"]
    return (
        frame[columns]
        .dropna(subset=["date", "close"])
        .sort_values(["ticker", "date"])
        .reset_index(drop=True)
    )


def normalize_fmp_prices(
    records: list[dict[str, Any]],
    provider_to_ticker: dict[str, str],
) -> pd.DataFrame:
    if not records:
        raise ValueError("FMP price data is empty")
    if not provider_to_ticker:
        raise ValueError("FMP provider symbol mapping must not be empty")

    raw = pd.DataFrame(records)
    required_columns = {"date", "_provider_symbol", "open", "high", "low", "close", "volume"}
    missing = sorted(required_columns - set(raw.columns))
    if missing:
        raise ValueError(f"FMP price response missing columns: {', '.join(missing)}")
    if "adjClose" not in raw.columns:
        raise ValueError("FMP price response missing required adjClose column")

    frame = raw.copy()
    frame["ticker"] = frame["_provider_symbol"].map(provider_to_ticker)
    frame = frame.loc[frame["ticker"].notna()].copy()
    if frame.empty:
        raise ValueError("FMP price response did not include requested tickers")

    frame["date"] = pd.to_datetime(frame["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    frame["adj_close"] = frame["adjClose"]

    columns = ["date", "ticker", "open", "high", "low", "close", "adj_close", "volume"]
    return (
        frame[columns]
        .dropna(subset=["date", "close", "adj_close"])
        .sort_values(["ticker", "date"])
        .reset_index(drop=True)
    )


def normalize_cboe_vix_prices(
    raw: pd.DataFrame,
    *,
    ticker: str,
    start: date,
    end: date,
) -> pd.DataFrame:
    if raw.empty:
        raise ValueError("Cboe VIX price data is empty")
    if start > end:
        raise ValueError("Cboe VIX start date must be earlier than or equal to end date")

    frame = raw.copy()
    frame.columns = [_normalize_column_name(str(column)) for column in frame.columns]
    required_columns = {"date", "open", "high", "low", "close"}
    missing = sorted(required_columns - set(frame.columns))
    if missing:
        raise ValueError(f"Cboe VIX response missing columns: {', '.join(missing)}")

    frame["date"] = pd.to_datetime(frame["date"], errors="coerce")
    for column in ["open", "high", "low", "close"]:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")

    start_timestamp = pd.Timestamp(start)
    end_timestamp = pd.Timestamp(end)
    frame = frame.loc[
        (frame["date"] >= start_timestamp) & (frame["date"] <= end_timestamp)
    ].copy()
    if frame.empty:
        raise ValueError(
            "Cboe VIX response had no rows for "
            f"{start.isoformat()} to {end.isoformat()}"
        )
    if frame[["date", "open", "high", "low", "close"]].isna().any().any():
        raise ValueError("Cboe VIX response contains invalid date or OHLC values")

    frame["date"] = frame["date"].dt.strftime("%Y-%m-%d")
    frame["ticker"] = ticker
    frame["adj_close"] = frame["close"]
    frame["volume"] = pd.NA

    columns = ["date", "ticker", "open", "high", "low", "close", "adj_close", "volume"]
    return frame[columns].sort_values(["ticker", "date"]).reset_index(drop=True)


def normalize_fred_rates(raw: pd.DataFrame) -> pd.DataFrame:
    if raw.empty:
        raise ValueError("rate data is empty")

    reset = raw.reset_index()
    date_column = str(reset.columns[0])
    rates = reset.melt(id_vars=[date_column], var_name="series", value_name="value")
    rates = rates.rename(columns={date_column: "date"})
    rates["date"] = pd.to_datetime(rates["date"]).dt.strftime("%Y-%m-%d")
    rates = rates.dropna(subset=["value"])
    return rates.sort_values(["series", "date"]).reset_index(drop=True)


def _normalize_single_ticker_prices(data: pd.DataFrame, ticker: str) -> pd.DataFrame:
    frame = data.copy()
    frame.columns = [_normalize_column_name(str(column)) for column in frame.columns]

    if "close" not in frame.columns:
        raise ValueError(f"{ticker} price data must include close")
    if "adj_close" not in frame.columns:
        frame["adj_close"] = frame["close"]

    reset = frame.reset_index()
    date_column = str(reset.columns[0])
    reset = reset.rename(columns={date_column: "date"})
    reset["date"] = pd.to_datetime(reset["date"]).dt.strftime("%Y-%m-%d")
    reset["ticker"] = ticker

    for column in ["open", "high", "low", "volume"]:
        if column not in reset.columns:
            reset[column] = pd.NA

    columns = ["date", "ticker", "open", "high", "low", "close", "adj_close", "volume"]
    return reset[columns].dropna(subset=["close"])


def _normalize_column_name(column: str) -> str:
    return column.strip().lower().replace(" ", "_")


def _marketstack_error_code(payload: Any) -> str:
    if not isinstance(payload, dict):
        return "unknown"
    error = payload.get("error")
    if isinstance(error, dict):
        code = error.get("code")
        return "unknown" if code is None else str(code)
    return "unknown"


def _marketstack_diagnostic(
    *,
    base_url: str,
    params: dict[str, object],
    cache_key: str | None,
    cache_metadata_path: Path | None,
    cache_status: str,
    stage: str,
    row_count_before_failure: int,
    response: Any | None = None,
    error_code: str | None = None,
    exception: Exception | None = None,
    api_key: str = "",
) -> ProviderRequestDiagnostic:
    content = getattr(response, "content", None)
    content_bytes = content if isinstance(content, (bytes, bytearray)) else None
    return ProviderRequestDiagnostic(
        provider="Marketstack",
        api_family="eod_daily_prices",
        endpoint=base_url,
        stage=stage,
        method="GET",
        request_parameters=_marketstack_safe_request_parameters(params),
        cache_status=cache_status,
        cache_key=cache_key,
        cache_metadata_path=cache_metadata_path,
        http_status=getattr(response, "status_code", None),
        error_code=error_code,
        response_body_sha256=(
            sha256(bytes(content_bytes)).hexdigest() if content_bytes is not None else None
        ),
        response_body_size_bytes=(len(content_bytes) if content_bytes is not None else None),
        row_count_before_failure=row_count_before_failure,
        exception_type=type(exception).__name__ if exception is not None else None,
        exception_message=(
            sanitize_diagnostic_text(str(exception), extra_secrets=(api_key,))
            if exception is not None
            else None
        ),
    )


def _marketstack_safe_request_parameters(params: dict[str, object]) -> dict[str, object]:
    return {
        key: ("***" if key == "access_key" else value)
        for key, value in sorted(params.items(), key=lambda item: item[0])
    }


def _fred_diagnostic(
    *,
    base_url: str,
    params: dict[str, object],
    cache_key: str | None,
    cache_metadata_path: Path | None,
    cache_status: str,
    stage: str,
    row_count_before_failure: int,
    response: Any | None = None,
    error_code: str | None = None,
    exception: Exception | None = None,
    attempt_count: int | None = None,
    max_attempts: int | None = None,
    timeout_seconds: float | None = None,
) -> ProviderRequestDiagnostic:
    content = getattr(response, "content", None)
    content_bytes = content if isinstance(content, (bytes, bytearray)) else None
    return ProviderRequestDiagnostic(
        provider="Federal Reserve Economic Data",
        api_family="fredgraph_csv",
        endpoint=base_url,
        stage=stage,
        method="GET",
        request_parameters=dict(sorted(params.items(), key=lambda item: item[0])),
        cache_status=cache_status,
        cache_key=cache_key,
        cache_metadata_path=cache_metadata_path,
        http_status=getattr(response, "status_code", None),
        error_code=error_code,
        response_body_sha256=(
            sha256(bytes(content_bytes)).hexdigest() if content_bytes is not None else None
        ),
        response_body_size_bytes=(len(content_bytes) if content_bytes is not None else None),
        row_count_before_failure=row_count_before_failure,
        attempt_count=attempt_count,
        max_attempts=max_attempts,
        timeout_seconds=timeout_seconds,
        exception_type=type(exception).__name__ if exception is not None else None,
        exception_message=(
            sanitize_diagnostic_text(str(exception)) if exception is not None else None
        ),
    )


def _fmp_price_records(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        if not all(isinstance(item, dict) for item in payload):
            raise ValueError("FMP price response contained non-object rows")
        return cast(list[dict[str, Any]], payload)
    if isinstance(payload, dict):
        if "Error Message" in payload or "error" in payload:
            raise ValueError(
                "FMP price response returned an error: "
                f"error_code={_fmp_error_code(payload)}"
            )
        historical = payload.get("historical")
        if isinstance(historical, list):
            if not all(isinstance(item, dict) for item in historical):
                raise ValueError("FMP historical price response contained non-object rows")
            return cast(list[dict[str, Any]], historical)
    raise ValueError("FMP price response was not a supported JSON shape")


def _normalize_fmp_raw_price_record(record: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(record)
    if "open" not in normalized and "adjOpen" in normalized:
        normalized["open"] = normalized["adjOpen"]
    if "high" not in normalized and "adjHigh" in normalized:
        normalized["high"] = normalized["adjHigh"]
    if "low" not in normalized and "adjLow" in normalized:
        normalized["low"] = normalized["adjLow"]
    if "close" not in normalized and "adjClose" in normalized:
        normalized["close"] = normalized["adjClose"]
    return normalized


def _fmp_error_code(payload: Any) -> str:
    if not isinstance(payload, dict):
        return "unknown"
    error = payload.get("Error Message") or payload.get("error")
    if error is None:
        return "unknown"
    if isinstance(error, dict):
        return str(error.get("code") or "unknown")
    return str(error).splitlines()[0][:120]


def _fetch_fmp_price_records(
    *,
    requests: Any,
    url: str,
    params: dict[str, object],
    response_label: str,
    request_cache_dir: Path | str | None = None,
) -> list[dict[str, Any]]:
    response = cached_requests_get(
        provider="Financial Modeling Prep",
        api_family="eod_daily_prices",
        url=url,
        params=params,
        timeout=30,
        requests_module=requests,
        cache_dir=request_cache_dir,
    )
    try:
        payload = response.json()
    except ValueError as exc:
        raise ValueError(f"{response_label} response was not valid JSON") from exc
    if not response.ok:
        raise ValueError(
            f"{response_label} request failed: "
            f"http_status={response.status_code}, "
            f"error_code={_fmp_error_code(payload)}"
        )
    return _fmp_price_records(payload)
