from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import UTC, date, datetime, time
from pathlib import Path
from typing import Literal, Protocol

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.trading_engine.schemas.market import MarketSnapshot
from ai_trading_system.trading_engine.schemas.order_intent_candidate import (
    OrderIntentCandidate,
)

MarketSnapshotSource = Literal[
    "historical_ohlc",
    "candidate_metadata",
    "synthetic_limit_price",
]


@dataclass(frozen=True)
class MarketSnapshotResolution:
    snapshot: MarketSnapshot
    source: MarketSnapshotSource


class MarketSnapshotProvider(Protocol):
    def resolve(
        self,
        candidate: OrderIntentCandidate,
        *,
        as_of: date,
    ) -> MarketSnapshotResolution:
        """Resolve a daily market snapshot for a paper-trading candidate."""


class HistoricalPriceMarketSnapshotProvider:
    def __init__(
        self,
        prices_path: Path | str | None = None,
    ) -> None:
        self.prices_path = (
            Path(prices_path)
            if prices_path is not None
            else (PROJECT_ROOT / "data" / "raw" / "prices_daily.csv")
        )

    def resolve(
        self,
        candidate: OrderIntentCandidate,
        *,
        as_of: date,
    ) -> MarketSnapshotResolution:
        historical = self._historical_snapshot(candidate, as_of=as_of)
        if historical is not None:
            return MarketSnapshotResolution(
                snapshot=historical,
                source="historical_ohlc",
            )
        metadata = candidate_metadata_market_snapshot(candidate, as_of=as_of)
        if metadata is not None:
            return MarketSnapshotResolution(
                snapshot=metadata,
                source="candidate_metadata",
            )
        return MarketSnapshotResolution(
            snapshot=synthetic_limit_price_market_snapshot(candidate, as_of=as_of),
            source="synthetic_limit_price",
        )

    def _historical_snapshot(
        self,
        candidate: OrderIntentCandidate,
        *,
        as_of: date,
    ) -> MarketSnapshot | None:
        if candidate.symbol is None or not self.prices_path.exists():
            return None
        target_symbol = candidate.symbol.upper()
        try:
            with self.prices_path.open("r", encoding="utf-8", newline="") as file:
                for row in csv.DictReader(file):
                    symbol = str(row.get("ticker") or row.get("symbol") or "").upper()
                    if symbol != target_symbol or str(row.get("date") or "") != as_of.isoformat():
                        continue
                    open_price = _positive_float(row.get("open"))
                    high = _positive_float(row.get("high"))
                    low = _positive_float(row.get("low"))
                    close = _positive_float(row.get("close"))
                    if None in {open_price, high, low, close}:
                        continue
                    return MarketSnapshot(
                        symbol=target_symbol,
                        timestamp=datetime.combine(as_of, time(20, 0), tzinfo=UTC),
                        open=open_price,
                        high=high,
                        low=low,
                        last=close,
                    )
        except OSError:
            return None
        return None


def candidate_metadata_market_snapshot(
    candidate: OrderIntentCandidate,
    *,
    as_of: date,
) -> MarketSnapshot | None:
    if candidate.symbol is None or candidate.limit_price is None:
        return None
    raw_snapshot = candidate.metadata.get("market_snapshot")
    if not isinstance(raw_snapshot, dict):
        return None
    return MarketSnapshot(
        symbol=str(raw_snapshot.get("symbol") or candidate.symbol),
        timestamp=_parse_datetime(raw_snapshot.get("timestamp"))
        or datetime.combine(as_of, time(20, 0), tzinfo=UTC),
        open=_float_or_default(raw_snapshot.get("open"), candidate.limit_price),
        high=_float_or_default(raw_snapshot.get("high"), candidate.limit_price),
        low=_float_or_default(raw_snapshot.get("low"), candidate.limit_price),
        last=_float_or_default(raw_snapshot.get("last"), candidate.limit_price),
    )


def synthetic_limit_price_market_snapshot(
    candidate: OrderIntentCandidate,
    *,
    as_of: date,
) -> MarketSnapshot:
    if candidate.symbol is None or candidate.limit_price is None:
        raise ValueError("candidate cannot create market snapshot without symbol and limit")
    return MarketSnapshot(
        symbol=candidate.symbol,
        timestamp=datetime.combine(as_of, time(20, 0), tzinfo=UTC),
        open=candidate.limit_price,
        high=candidate.limit_price,
        low=candidate.limit_price,
        last=candidate.limit_price,
    )


def _parse_datetime(value: object) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed


def _float_or_default(value: object, default: float) -> float:
    try:
        if isinstance(value, (int, float, str)):
            parsed = float(value)
            if parsed > 0:
                return parsed
    except ValueError:
        pass
    return default


def _positive_float(value: object) -> float | None:
    try:
        if isinstance(value, (int, float, str)):
            parsed = float(value)
            if parsed > 0:
                return parsed
    except ValueError:
        return None
    return None
