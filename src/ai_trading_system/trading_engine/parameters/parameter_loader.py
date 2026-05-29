from __future__ import annotations

from pathlib import Path

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.trading_engine.parameters.parameter_schema import (
    ProductionParameters,
    PromotionRulesConfig,
    ShadowBacktestConfig,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

DEFAULT_PRODUCTION_PARAMETERS_PATH = (
    PROJECT_ROOT / "config" / "parameters" / "production" / "current.yaml"
)
DEFAULT_SHADOW_BACKTEST_CONFIG_PATH = (
    PROJECT_ROOT / "config" / "parameters" / "shadow" / "shadow_backtest.yaml"
)
DEFAULT_PROMOTION_RULES_PATH = (
    PROJECT_ROOT / "config" / "parameters" / "promotion" / "promotion_rules.yaml"
)


def load_production_parameters(
    path: Path | str = DEFAULT_PRODUCTION_PARAMETERS_PATH,
) -> ProductionParameters:
    payload = _load_mapping(Path(path))
    return ProductionParameters.model_validate(payload)


def load_shadow_backtest_config(
    path: Path | str = DEFAULT_SHADOW_BACKTEST_CONFIG_PATH,
) -> ShadowBacktestConfig:
    payload = _load_mapping(Path(path))
    return ShadowBacktestConfig.model_validate(payload)


def load_promotion_rules(path: Path | str = DEFAULT_PROMOTION_RULES_PATH) -> PromotionRulesConfig:
    payload = _load_mapping(Path(path))
    return PromotionRulesConfig.model_validate(payload)


def resolve_project_path(path_text: str, *, project_root: Path = PROJECT_ROOT) -> Path:
    path = Path(path_text)
    return path if path.is_absolute() else project_root / path


def _load_mapping(path: Path) -> dict[str, object]:
    raw = safe_load_yaml_path(path)
    if not isinstance(raw, dict):
        raise ValueError(f"YAML config must be a mapping: {path}")
    return raw
