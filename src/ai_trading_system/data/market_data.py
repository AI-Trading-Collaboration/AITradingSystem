from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from importlib import import_module
from io import StringIO
from pathlib import Path
from typing import Any, Protocol, cast

import pandas as pd


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
