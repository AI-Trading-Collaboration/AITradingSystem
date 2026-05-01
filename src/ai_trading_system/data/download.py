from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path

from ai_trading_system.config import (
    UniverseConfig,
    configured_price_tickers,
    configured_rate_series,
)
from ai_trading_system.data.market_data import (
    CsvDataCache,
    FredRateProvider,
    PriceDataProvider,
    PriceRequest,
    RateDataProvider,
    RateRequest,
    YFinancePriceProvider,
)


@dataclass(frozen=True)
class DataDownloadSummary:
    prices_path: Path
    rates_path: Path
    price_rows: int
    rate_rows: int
    price_tickers: tuple[str, ...]
    rate_series: tuple[str, ...]


def download_daily_data(
    config: UniverseConfig,
    start: date,
    end: date,
    output_dir: Path,
    include_full_ai_chain: bool = False,
    price_provider: PriceDataProvider | None = None,
    rate_provider: RateDataProvider | None = None,
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

    prices = price_provider.download_prices(
        PriceRequest(tickers=price_tickers, start=start, end=end, interval="1d")
    )
    rates = rate_provider.download_rates(RateRequest(series_ids=rate_series, start=start, end=end))

    prices_path = cache.write_prices(prices)
    rates_path = cache.write_rates(rates)

    return DataDownloadSummary(
        prices_path=prices_path,
        rates_path=rates_path,
        price_rows=len(prices),
        rate_rows=len(rates),
        price_tickers=tuple(price_tickers),
        rate_series=tuple(rate_series),
    )
