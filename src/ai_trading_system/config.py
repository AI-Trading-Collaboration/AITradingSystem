from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, Field

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CONFIG_PATH = PROJECT_ROOT / "config" / "universe.yaml"
DEFAULT_PORTFOLIO_CONFIG_PATH = PROJECT_ROOT / "config" / "portfolio.yaml"
DEFAULT_DATA_QUALITY_CONFIG_PATH = PROJECT_ROOT / "config" / "data_quality.yaml"


class MarketUniverse(BaseModel):
    decision_frequency: str = "daily"
    benchmarks: list[str] = Field(default_factory=list)
    defensive: list[str] = Field(default_factory=list)


class MacroUniverse(BaseModel):
    volatility: list[str] = Field(default_factory=list)
    rates: list[str] = Field(default_factory=list)
    currency: list[str] = Field(default_factory=list)


class UniverseConfig(BaseModel):
    market: MarketUniverse
    macro: MacroUniverse
    ai_chain: dict[str, list[str]]
    scoring_weights: dict[str, float]


class DecisionConfig(BaseModel):
    frequency: str
    market_timezone: str
    report_timezone: str
    timing: str


class PortfolioBudgetConfig(BaseModel):
    total_risk_asset_min: float
    total_risk_asset_max: float


class PositionLimitsConfig(BaseModel):
    max_single_stock_in_ai_bucket: float
    max_total_ai_exposure: float


class PortfolioConfig(BaseModel):
    decision: DecisionConfig
    portfolio: PortfolioBudgetConfig
    position_limits: PositionLimitsConfig


class PriceQualityConfig(BaseModel):
    max_stale_calendar_days: int = Field(gt=0)
    suspicious_daily_return_abs: float = Field(gt=0)
    extreme_daily_return_abs: float = Field(gt=0)
    suspicious_adjustment_ratio_change_abs: float = Field(gt=0)


class RateQualityConfig(BaseModel):
    max_stale_calendar_days: int = Field(gt=0)
    min_plausible_value: float
    max_plausible_value: float
    suspicious_daily_change_abs: float = Field(gt=0)
    extreme_daily_change_abs: float = Field(gt=0)


class DataQualityConfig(BaseModel):
    prices: PriceQualityConfig
    rates: RateQualityConfig


def load_universe(path: Path | str = DEFAULT_CONFIG_PATH) -> UniverseConfig:
    config_path = Path(path)
    with config_path.open("r", encoding="utf-8") as file:
        raw: dict[str, Any] = yaml.safe_load(file)
    return UniverseConfig.model_validate(raw)


def load_portfolio(path: Path | str = DEFAULT_PORTFOLIO_CONFIG_PATH) -> PortfolioConfig:
    config_path = Path(path)
    with config_path.open("r", encoding="utf-8") as file:
        raw: dict[str, Any] = yaml.safe_load(file)
    return PortfolioConfig.model_validate(raw)


def load_data_quality(
    path: Path | str = DEFAULT_DATA_QUALITY_CONFIG_PATH,
) -> DataQualityConfig:
    config_path = Path(path)
    with config_path.open("r", encoding="utf-8") as file:
        raw: dict[str, Any] = yaml.safe_load(file)
    return DataQualityConfig.model_validate(raw)


def configured_price_tickers(
    config: UniverseConfig,
    include_full_ai_chain: bool = False,
) -> list[str]:
    tickers: list[str] = []
    tickers.extend(config.market.benchmarks)
    tickers.extend(config.market.defensive)
    tickers.extend(config.macro.volatility)
    tickers.extend(config.macro.currency)
    tickers.extend(config.ai_chain.get("core_watchlist", []))

    if include_full_ai_chain:
        for group_name, group_tickers in config.ai_chain.items():
            if group_name != "core_watchlist":
                tickers.extend(group_tickers)

    return dedupe_preserving_order(tickers)


def configured_rate_series(config: UniverseConfig) -> list[str]:
    return dedupe_preserving_order(config.macro.rates)


def dedupe_preserving_order(items: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    unique_items: list[str] = []
    for item in items:
        if item not in seen:
            unique_items.append(item)
            seen.add(item)
    return unique_items
