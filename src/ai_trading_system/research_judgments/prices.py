"""Strict CSV schema validation and content hashes for settlement price inputs."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable
from pathlib import Path
from typing import cast

import pandas as pd

from ai_trading_system.research_judgments.schema import Issue


def read_closes(path: Path) -> tuple[pd.DataFrame | None, list[Issue]]:
    try:
        rows = pd.read_csv(path, usecols=["date", "ticker", "adj_close"], dtype=str)
        if (
            rows.empty
            or not rows["date"].str.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}", na=False).all()
            or rows["ticker"].isna().any()
            or rows["ticker"].str.strip().eq("").any()
            or rows.duplicated(["date", "ticker"]).any()
        ):
            raise ValueError("invalid price schema")
        rows["date"] = pd.to_datetime(rows["date"], format="%Y-%m-%d", errors="raise")
        rows["adj_close"] = pd.to_numeric(rows["adj_close"], errors="coerce")
        closes = rows.pivot(index="date", columns="ticker", values="adj_close")
        return closes.sort_index().sort_index(axis=1), []
    except OSError:
        return None, [Issue("prices_missing")]
    except (ValueError, pd.errors.ParserError):
        return None, [Issue("prices_schema")]


def digest(closes: pd.DataFrame, tickers: Iterable[str]) -> str:
    rows = [
        [cast(pd.Timestamp, stamp).date().isoformat(), ticker, float(value)]
        for ticker in sorted(set(tickers).intersection(closes.columns))
        for stamp, value in closes[ticker].sort_index().dropna().items()
    ]
    return hashlib.sha256(json.dumps(rows, separators=(",", ":")).encode("utf-8")).hexdigest()
