from __future__ import annotations

from datetime import date
from pathlib import Path

import typer

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.trading_engine.market_data_freshness import (
    latest_market_data_freshness_path,
)
from ai_trading_system.trading_engine.market_data_refresh import (
    latest_market_data_refresh_path,
)


def _parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise typer.BadParameter("日期必须使用 YYYY-MM-DD 格式。") from exc


def _resolve_market_data_freshness_path(*, latest: bool, as_of: str | None) -> Path:
    root = PROJECT_ROOT / "artifacts" / "data_freshness"
    if latest or as_of is None:
        latest_path = latest_market_data_freshness_path(root)
        if latest_path is None:
            raise typer.BadParameter(f"未找到 market data freshness artifact：{root}")
        return latest_path
    return root / _parse_date(as_of).isoformat() / "market_data_freshness_summary.json"


def _resolve_market_data_refresh_path(*, latest: bool, as_of: str | None) -> Path:
    root = PROJECT_ROOT / "artifacts" / "data_refresh"
    if latest or as_of is None:
        latest_path = latest_market_data_refresh_path(root)
        if latest_path is None:
            raise typer.BadParameter(f"未找到 market data refresh artifact：{root}")
        return latest_path
    return root / _parse_date(as_of).isoformat() / "market_data_refresh_summary.json"
