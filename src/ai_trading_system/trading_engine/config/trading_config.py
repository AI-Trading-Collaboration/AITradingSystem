from __future__ import annotations

from pathlib import Path
from typing import Literal, Self

from pydantic import BaseModel, Field, model_validator

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.trading_engine.schemas.order_intent import AssetType, OrderSide, OrderType
from ai_trading_system.yaml_loader import safe_load_yaml_path

DEFAULT_TRADING_ENGINE_CONFIG_PATH = PROJECT_ROOT / "config" / "trading_engine.yaml"


class TradingSettings(BaseModel):
    mode: Literal["paper", "real"] = "paper"
    real_trading_enabled: bool = False
    base_currency: Literal["USD"] = "USD"


class RiskPolicyMetadata(BaseModel):
    policy_id: str = Field(min_length=1)
    version: str = Field(min_length=1)
    status: str = Field(min_length=1)
    owner: str = Field(min_length=1)
    rationale: str = Field(min_length=1)
    validation: str = Field(min_length=1)
    review_condition: str = Field(min_length=1)


class RiskLimits(BaseModel):
    allowed_asset_types: list[AssetType] = Field(min_length=1)
    allowed_order_types: list[OrderType] = Field(min_length=1)
    allowed_sides: list[OrderSide] = Field(min_length=1)
    allow_short: bool = False
    allow_margin: bool = False
    min_confidence: float = Field(ge=0, le=1)
    max_order_notional_pct: float = Field(gt=0, le=1)
    max_position_pct_per_symbol: float = Field(gt=0, le=1)
    max_total_exposure_pct: float = Field(gt=0, le=1)
    max_daily_trades_per_symbol: int = Field(ge=0)
    kill_switch_enabled: bool = False

    @model_validator(mode="after")
    def validate_exposure_limits(self) -> Self:
        if self.max_position_pct_per_symbol > self.max_total_exposure_pct:
            raise ValueError("max_position_pct_per_symbol must not exceed max_total_exposure_pct")
        return self


class ExecutionSettings(BaseModel):
    default_initial_cash_usd: float = Field(gt=0)
    commission_bps: float = Field(default=0.0, ge=0)
    slippage_bps: float = Field(default=0.0, ge=0)


class TradingEngineConfig(BaseModel):
    trading: TradingSettings
    risk_policy: RiskPolicyMetadata
    risk_limits: RiskLimits
    execution: ExecutionSettings

    def assert_paper_only(self) -> None:
        if self.trading.mode != "paper":
            raise RuntimeError("Real trading mode is not implemented in this phase")
        if self.trading.real_trading_enabled:
            raise RuntimeError("real_trading_enabled must remain false in this phase")


def load_trading_engine_config(
    path: Path | str = DEFAULT_TRADING_ENGINE_CONFIG_PATH,
) -> TradingEngineConfig:
    config_path = Path(path)
    raw = safe_load_yaml_path(config_path)
    return TradingEngineConfig.model_validate(raw)
