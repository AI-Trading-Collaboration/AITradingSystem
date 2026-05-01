from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, Field

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CONFIG_PATH = PROJECT_ROOT / "config" / "universe.yaml"
DEFAULT_PORTFOLIO_CONFIG_PATH = PROJECT_ROOT / "config" / "portfolio.yaml"


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
