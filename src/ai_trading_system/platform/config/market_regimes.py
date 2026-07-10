from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Self

from pydantic import BaseModel, Field, model_validator

from ai_trading_system.platform.config.resolver import ResolvedConfig, resolve_yaml_config

PROJECT_ROOT = Path(__file__).resolve().parents[4]
DEFAULT_MARKET_REGIMES_CONFIG_PATH = PROJECT_ROOT / "config" / "market_regimes.yaml"


class MarketRegimeConfig(BaseModel):
    regime_id: str = Field(min_length=1)
    name: str = Field(min_length=1)
    start_date: date
    anchor_date: date
    anchor_event: str = Field(min_length=1)
    description: str = Field(min_length=1)
    primary: bool = False


class MarketRegimesConfig(BaseModel):
    default_backtest_regime: str = Field(min_length=1)
    regimes: list[MarketRegimeConfig] = Field(min_length=1)

    @model_validator(mode="after")
    def validate_regimes(self) -> Self:
        ids = [regime.regime_id for regime in self.regimes]
        duplicate_ids = sorted({regime_id for regime_id in ids if ids.count(regime_id) > 1})
        if duplicate_ids:
            raise ValueError(f"market regime ids must be unique: {', '.join(duplicate_ids)}")
        if self.default_backtest_regime not in set(ids):
            raise ValueError("default_backtest_regime must match one configured market regime id")
        return self


def resolve_market_regimes(
    path: Path | str = DEFAULT_MARKET_REGIMES_CONFIG_PATH,
) -> ResolvedConfig[MarketRegimesConfig]:
    return resolve_yaml_config(
        path,
        MarketRegimesConfig,
        policy_id="market_regimes",
    )


def load_market_regimes(
    path: Path | str = DEFAULT_MARKET_REGIMES_CONFIG_PATH,
) -> MarketRegimesConfig:
    return resolve_market_regimes(path).value


def market_regime_by_id(
    config: MarketRegimesConfig,
    regime_id: str,
) -> MarketRegimeConfig:
    for regime in config.regimes:
        if regime.regime_id == regime_id:
            return regime
    configured_ids = ", ".join(regime.regime_id for regime in config.regimes)
    raise ValueError(f"unknown market regime '{regime_id}', available: {configured_ids}")
