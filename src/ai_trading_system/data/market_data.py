from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, timedelta
from importlib import import_module
from io import StringIO
from pathlib import Path
from typing import Any, Protocol, cast

import pandas as pd

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
    "GOOG": "GOOGL",
    "^VIX": None,
}


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


class YFinancePriceProvider:
    def download_prices(self, request: PriceRequest) -> pd.DataFrame:
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
        return normalize_yfinance_prices(raw, request.tickers)


@dataclass(frozen=True)
class MarketstackPriceProvider:
    api_key: str
    base_url: str = MARKETSTACK_EOD_URL
    page_limit: int = 1000
    symbol_aliases: dict[str, str | None] = field(
        default_factory=lambda: dict(MARKETSTACK_DEFAULT_SYMBOL_ALIASES)
    )

    def __post_init__(self) -> None:
        if not self.api_key:
            raise ValueError("Marketstack API key must not be empty")
        if self.page_limit <= 0:
            raise ValueError("Marketstack page_limit must be positive")

    def download_prices(self, request: PriceRequest) -> pd.DataFrame:
        requests = cast(Any, import_module("requests"))
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
            response = requests.get(self.base_url, params=params, timeout=30)
            try:
                payload = response.json()
            except ValueError:
                payload = {}
            if not response.ok:
                raise ValueError(
                    "Marketstack request failed: "
                    f"http_status={response.status_code}, "
                    f"error_code={_marketstack_error_code(payload)}"
                )
            if isinstance(payload, dict) and payload.get("error"):
                raise ValueError(
                    "Marketstack response returned an error: "
                    f"error_code={_marketstack_error_code(payload)}"
                )

            page_records = payload.get("data", []) if isinstance(payload, dict) else []
            if not isinstance(page_records, list):
                raise ValueError("Marketstack response data is not a list")
            records.extend(record for record in page_records if isinstance(record, dict))

            pagination = payload.get("pagination", {}) if isinstance(payload, dict) else {}
            count = int(pagination.get("count") or len(page_records))
            total = int(pagination.get("total") or len(records))
            if count <= 0 or len(records) >= total:
                break
            offset += count

        return normalize_marketstack_prices(records, provider_to_ticker)

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

    def __post_init__(self) -> None:
        if not self.api_key.strip():
            raise ValueError("FMP API key must not be empty")

    def download_prices(self, request: PriceRequest) -> pd.DataFrame:
        requests = self.requests_module or cast(Any, import_module("requests"))
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
            )
            adjusted_records = _fetch_fmp_price_records(
                requests=requests,
                url=self.adjusted_url,
                params=params,
                response_label="FMP adjusted price",
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


class FredRateProvider:
    base_url = "https://fred.stlouisfed.org/graph/fredgraph.csv"

    def download_rates(self, request: RateRequest) -> pd.DataFrame:
        requests = cast(Any, import_module("requests"))
        frames: list[pd.DataFrame] = []

        for series_id in request.series_ids:
            response = requests.get(
                self.base_url,
                params={
                    "id": series_id,
                    "cosd": request.start.isoformat(),
                    "coed": request.end.isoformat(),
                },
                timeout=30,
            )
            response.raise_for_status()
            frame = pd.read_csv(StringIO(str(response.text)))
            if "observation_date" not in frame.columns or series_id not in frame.columns:
                raise ValueError(f"FRED response for {series_id} has unexpected columns")

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
) -> list[dict[str, Any]]:
    response = requests.get(url, params=params, timeout=30)
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
