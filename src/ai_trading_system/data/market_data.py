from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Protocol


@dataclass(frozen=True)
class PriceRequest:
    tickers: list[str]
    start: date
    end: date
    interval: str = "1d"


class MarketDataProvider(Protocol):
    def download_prices(self, request: PriceRequest, output_path: Path) -> Path:
        """Download adjusted OHLCV data and return the local file path."""
