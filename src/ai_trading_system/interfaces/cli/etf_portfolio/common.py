from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd
import typer

from ai_trading_system.etf_portfolio.data import (
    latest_price_date,
    read_price_frame,
    standardize_price_frame,
)
from ai_trading_system.etf_portfolio.models import load_etf_config_bundle


def parse_date(value: str | None) -> date:
    if value is None:
        raise typer.BadParameter("日期不能为空")
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise typer.BadParameter("日期必须使用 YYYY-MM-DD 或 latest") from exc


def resolve_date(
    value: str | None,
    *,
    prices_path: Path | None = None,
    prices: pd.DataFrame | None = None,
) -> date:
    if value is not None and value != "latest":
        return parse_date(value)
    if prices is None:
        if prices_path is None:
            raise typer.BadParameter("latest 需要 prices 或 prices_path")
        config = load_etf_config_bundle()
        raw = read_price_frame(prices_path)
        prices, _ = standardize_price_frame(raw, assets=config.assets, source_name=str(prices_path))
    return latest_price_date(prices)


def satellite_symbols(config: Any) -> set[str]:
    if config.p1 is None:
        return set()
    return set(config.p1.satellite_stocks)


__all__ = ["parse_date", "resolve_date", "satellite_symbols"]
