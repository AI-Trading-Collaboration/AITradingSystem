from __future__ import annotations

import json
from collections.abc import Iterable, Mapping
from hashlib import sha256
from pathlib import Path
from typing import Any, Literal, Self

from pydantic import BaseModel, Field, model_validator

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.etf_portfolio.models import PolicyMetadata
from ai_trading_system.yaml_loader import safe_load_yaml_path

DEFAULT_AI_CONFIRMATION_UNIVERSE_CONFIG_PATH = (
    PROJECT_ROOT / "config" / "etf_portfolio" / "ai_confirmation_universe.yaml"
)

AI_CONFIRMATION_SAFETY = {
    "observe_only": True,
    "candidate_only": True,
    "production_effect": "none",
    "broker_action": "none",
    "manual_review_required": True,
}


class AIConfirmationSafetyConfig(BaseModel):
    observe_only: bool
    candidate_only: bool
    production_effect: str = Field(min_length=1)
    broker_action: str = Field(min_length=1)
    manual_review_required: bool

    @model_validator(mode="after")
    def validate_safety_boundary(self) -> Self:
        if self.model_dump(mode="json") != AI_CONFIRMATION_SAFETY:
            raise ValueError(
                "AI confirmation safety must keep observe_only=true, "
                "candidate_only=true, production_effect=none, broker_action=none, "
                "manual_review_required=true"
            )
        return self


class AIConfirmationSymbolConfig(BaseModel):
    ticker: str = Field(min_length=1)
    name: str = Field(min_length=1)
    group: str = Field(min_length=1)
    role: str = Field(min_length=1)
    enabled: bool
    weight_cap: float = Field(ge=0, le=1)
    benchmark: str = Field(min_length=1)
    data_required: bool
    optional: bool

    @model_validator(mode="after")
    def normalize_and_validate_symbol(self) -> Self:
        self.ticker = self.ticker.strip().upper()
        self.group = self.group.strip()
        self.benchmark = self.benchmark.strip().upper()
        if self.optional and self.data_required:
            raise ValueError(
                f"AI confirmation symbol cannot be both optional and data_required: "
                f"{self.ticker}"
            )
        return self


class AIConfirmationGroupConfig(BaseModel):
    group_id: str = Field(min_length=1)
    description: str = Field(min_length=1)
    symbols: list[AIConfirmationSymbolConfig] = Field(min_length=1)
    default_weighting_method: Literal["equal_weight", "weight_cap"]
    benchmark: str = Field(min_length=1)
    enabled: bool
    required_data_level: Literal["strict", "warning", "optional"]

    @model_validator(mode="after")
    def normalize_group(self) -> Self:
        self.group_id = self.group_id.strip()
        self.benchmark = self.benchmark.strip().upper()
        for symbol in self.symbols:
            if symbol.group != self.group_id:
                raise ValueError(
                    f"AI confirmation symbol {symbol.ticker} group mismatch: "
                    f"{symbol.group} != {self.group_id}"
                )
        return self


class AIConfirmationUniverseConfig(BaseModel):
    policy_metadata: PolicyMetadata
    safety: AIConfirmationSafetyConfig
    allowed_benchmarks: list[str] = Field(min_length=1)
    ai_confirmation_universe: dict[str, AIConfirmationGroupConfig] = Field(min_length=1)

    @model_validator(mode="after")
    def validate_universe(self) -> Self:
        self.allowed_benchmarks = sorted(
            {benchmark.strip().upper() for benchmark in self.allowed_benchmarks}
        )
        if any(not benchmark for benchmark in self.allowed_benchmarks):
            raise ValueError("AI confirmation allowed benchmarks must be non-empty")
        allowed = set(self.allowed_benchmarks)
        for key, group in self.ai_confirmation_universe.items():
            if group.group_id != key:
                raise ValueError(
                    f"AI confirmation group mapping key must match group_id: "
                    f"{key} != {group.group_id}"
                )
            if group.benchmark not in allowed:
                raise ValueError(
                    f"AI confirmation group {group.group_id} references invalid benchmark: "
                    f"{group.benchmark}"
                )
            for symbol in group.symbols:
                if symbol.benchmark not in allowed:
                    raise ValueError(
                        f"AI confirmation symbol {symbol.ticker} references invalid "
                        f"benchmark: {symbol.benchmark}"
                    )
        return self

    @property
    def config_hash(self) -> str:
        return _config_hash(self.model_dump(mode="json"))


def load_ai_confirmation_universe_config(
    path: Path | str = DEFAULT_AI_CONFIRMATION_UNIVERSE_CONFIG_PATH,
) -> AIConfirmationUniverseConfig:
    raw = safe_load_yaml_path(Path(path))
    if not isinstance(raw, dict):
        raise ValueError(f"AI confirmation universe config must be a YAML mapping: {path}")
    return AIConfirmationUniverseConfig.model_validate(raw)


def enabled_symbols_for_group(
    config: AIConfirmationUniverseConfig,
    group_id: str,
) -> list[AIConfirmationSymbolConfig]:
    group = config.ai_confirmation_universe.get(group_id)
    if group is None:
        raise KeyError(f"unknown AI confirmation group: {group_id}")
    if not group.enabled:
        return []
    selected = [symbol for symbol in group.symbols if symbol.enabled]
    return _dedupe_symbols(selected)


def all_enabled_tickers(config: AIConfirmationUniverseConfig) -> tuple[str, ...]:
    tickers: set[str] = set()
    for group_id in sorted(config.ai_confirmation_universe):
        tickers.update(symbol.ticker for symbol in enabled_symbols_for_group(config, group_id))
    return tuple(sorted(tickers))


def validate_ai_confirmation_data_availability(
    config: AIConfirmationUniverseConfig,
    available_symbols: Iterable[str],
) -> dict[str, Any]:
    available = {symbol.strip().upper() for symbol in available_symbols}
    group_reports: list[dict[str, Any]] = []
    errors: list[str] = []
    warnings: list[str] = []
    for group_id in sorted(config.ai_confirmation_universe):
        group = config.ai_confirmation_universe[group_id]
        enabled_symbols = enabled_symbols_for_group(config, group_id)
        present = [symbol.ticker for symbol in enabled_symbols if symbol.ticker in available]
        missing_required = [
            symbol.ticker
            for symbol in enabled_symbols
            if symbol.data_required and symbol.ticker not in available
        ]
        missing_optional = [
            symbol.ticker
            for symbol in enabled_symbols
            if symbol.optional and symbol.ticker not in available
        ]
        if missing_required and group.required_data_level == "strict":
            errors.extend(
                f"{group_id}:missing_required:{symbol}" for symbol in missing_required
            )
        elif missing_required:
            warnings.extend(
                f"{group_id}:missing_required:{symbol}" for symbol in missing_required
            )
        if missing_optional:
            warnings.extend(f"{group_id}:missing_optional:{symbol}" for symbol in missing_optional)
        symbol_count = len(enabled_symbols)
        valid_symbol_count = len(present)
        coverage = 1.0 if symbol_count == 0 else valid_symbol_count / symbol_count
        group_reports.append(
            {
                "group_id": group_id,
                "symbol_count": symbol_count,
                "valid_symbol_count": valid_symbol_count,
                "data_coverage_ratio": coverage,
                "missing_required": sorted(missing_required),
                "missing_optional": sorted(missing_optional),
                "required_data_level": group.required_data_level,
                "enabled": group.enabled,
            }
        )
    status = "FAIL" if errors else "PASS_WITH_WARNINGS" if warnings else "PASS"
    return {
        "status": status,
        "group_reports": group_reports,
        "errors": sorted(errors),
        "warnings": sorted(warnings),
        **AI_CONFIRMATION_SAFETY,
    }


def _dedupe_symbols(
    symbols: Iterable[AIConfirmationSymbolConfig],
) -> list[AIConfirmationSymbolConfig]:
    by_ticker: dict[str, AIConfirmationSymbolConfig] = {}
    for symbol in sorted(symbols, key=lambda item: (item.ticker, item.role, item.name)):
        existing = by_ticker.get(symbol.ticker)
        if existing is None:
            by_ticker[symbol.ticker] = symbol
            continue
        if symbol.data_required and not existing.data_required:
            by_ticker[symbol.ticker] = symbol
    return [by_ticker[ticker] for ticker in sorted(by_ticker)]


def _config_hash(payload: Mapping[str, Any]) -> str:
    normalized = json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str)
    return sha256(normalized.encode("utf-8")).hexdigest()
