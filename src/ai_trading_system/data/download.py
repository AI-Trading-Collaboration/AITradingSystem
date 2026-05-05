from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path

import pandas as pd

from ai_trading_system.config import (
    UniverseConfig,
    configured_price_tickers,
    configured_rate_series,
)
from ai_trading_system.data.market_data import (
    CsvDataCache,
    FmpPriceProvider,
    FredRateProvider,
    MarketstackPriceProvider,
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
    manifest_path: Path
    price_rows: int
    rate_rows: int
    price_tickers: tuple[str, ...]
    rate_series: tuple[str, ...]
    secondary_prices_path: Path | None = None
    secondary_price_rows: int = 0


def download_daily_data(
    config: UniverseConfig,
    start: date,
    end: date,
    output_dir: Path,
    include_full_ai_chain: bool = False,
    price_provider: PriceDataProvider | None = None,
    secondary_price_provider: PriceDataProvider | None = None,
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

    price_request = PriceRequest(tickers=price_tickers, start=start, end=end, interval="1d")
    rate_request = RateRequest(series_ids=rate_series, start=start, end=end)

    prices = price_provider.download_prices(price_request)
    rates = rate_provider.download_rates(rate_request)

    prices_path = cache.write_prices(prices)
    rates_path = cache.write_rates(rates)
    secondary_prices_path: Path | None = None
    secondary_price_rows = 0
    manifest_records = [
        _manifest_record_for_prices(price_provider, price_request, prices_path, len(prices)),
        _manifest_record_for_rates(rate_provider, rate_request, rates_path, len(rates)),
    ]

    if secondary_price_provider is not None:
        secondary_prices = secondary_price_provider.download_prices(price_request)
        secondary_prices_path = cache.write_prices(
            secondary_prices,
            filename="prices_marketstack_daily.csv",
        )
        secondary_price_rows = len(secondary_prices)
        manifest_records.append(
            _manifest_record_for_prices(
                secondary_price_provider,
                price_request,
                secondary_prices_path,
                secondary_price_rows,
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
    )


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
) -> dict[str, object]:
    source_id, provider_name, endpoint = _rate_provider_metadata(provider)
    return _manifest_record(
        source_id=source_id,
        provider=provider_name,
        endpoint=endpoint,
        request_parameters={
            "series_ids": request.series_ids,
            "start": request.start.isoformat(),
            "end": request.end.isoformat(),
        },
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
