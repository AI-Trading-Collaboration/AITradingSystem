"""Optional second price source (Marketstack) next to the primary price cache.

DATA-022 (owner_decision:DATA-022:2026-10-12:fmp_only_secondary_optional): prices come from
FMP only. A Marketstack cache is written only by an explicit
``aits download-data --with-marketstack``; when it sits next to the price cache, data quality
reconciles against it as before, and its absence never blocks.
"""

from __future__ import annotations

from pathlib import Path
from typing import Final

SECONDARY_PRICES_FILENAME: Final = "prices_marketstack_daily.csv"


def secondary_prices_path(prices_path: Path) -> Path:
    return prices_path.parent / SECONDARY_PRICES_FILENAME


def optional_secondary_prices_path(prices_path: Path) -> Path | None:
    """Return the Marketstack cache next to ``prices_path`` when it exists, else None."""

    candidate = secondary_prices_path(prices_path)
    return candidate if candidate.is_file() else None
