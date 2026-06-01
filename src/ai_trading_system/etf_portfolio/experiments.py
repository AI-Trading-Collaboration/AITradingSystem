from __future__ import annotations

import json
from collections.abc import Mapping
from hashlib import sha256
from pathlib import Path
from typing import Any, Self

from pydantic import BaseModel, Field, model_validator

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.etf_portfolio.models import PolicyMetadata
from ai_trading_system.yaml_loader import safe_load_yaml_path

DEFAULT_ETF_EXPERIMENTS_CONFIG_PATH = (
    PROJECT_ROOT / "config" / "etf_portfolio" / "experiments.yaml"
)
DEFAULT_ETF_EXPERIMENT_PACKS_CONFIG_PATH = (
    PROJECT_ROOT / "config" / "etf_portfolio" / "experiment_packs.yaml"
)

# Schema keys only. These names define the controlled TRADING-064 override surface,
# not investment thresholds.
ALLOWED_EXPERIMENT_OVERRIDE_KEYS = frozenset(
    {
        "base_weights",
        "regime_multipliers",
        "semiconductor_sleeve_max_weight",
        "min_rebalance_delta",
        "relative_strength_weight",
    }
)


class ETFExperimentBaseConfigRef(BaseModel):
    assets_config: str = Field(min_length=1)
    strategy_config: str = Field(min_length=1)
    risk_config: str = Field(min_length=1)
    backtest_config: str = Field(min_length=1)
    asset_symbols: list[str] = Field(min_length=1)

    @model_validator(mode="after")
    def validate_unique_asset_symbols(self) -> Self:
        normalized = [symbol.strip().upper() for symbol in self.asset_symbols]
        if any(not symbol for symbol in normalized):
            raise ValueError("ETF experiment base config asset symbols must be non-empty")
        if len(normalized) != len(set(normalized)):
            raise ValueError("ETF experiment base config asset symbols must be unique")
        self.asset_symbols = normalized
        return self


class ETFExperimentConfig(BaseModel):
    experiment_id: str = Field(min_length=1)
    experiment_version: str = Field(min_length=1)
    description: str = Field(min_length=1)
    family: str = Field(min_length=1)
    base_config_ref: str = Field(min_length=1)
    overrides: dict[str, Any] = Field(default_factory=dict)
    benchmark_set: str = Field(min_length=1)
    expected_risk_profile: str = Field(min_length=1)
    observe_only: bool
    production_effect: str = Field(min_length=1)
    broker_action: str = Field(min_length=1)
    manual_review_required: bool

    @model_validator(mode="after")
    def validate_safety_and_override_keys(self) -> Self:
        if not self.observe_only:
            raise ValueError("ETF experiments must keep observe_only=true")
        if self.production_effect != "none":
            raise ValueError("ETF experiments must keep production_effect=none")
        if self.broker_action != "none":
            raise ValueError("ETF experiments must keep broker_action=none")
        if not self.manual_review_required:
            raise ValueError("ETF experiments must require manual review")
        unknown = sorted(set(self.overrides) - ALLOWED_EXPERIMENT_OVERRIDE_KEYS)
        if unknown:
            raise ValueError(
                "ETF experiment override contains unsupported keys: "
                f"{', '.join(unknown)}"
            )
        _validate_override_scalar(self.overrides, "semiconductor_sleeve_max_weight")
        _validate_override_scalar(self.overrides, "min_rebalance_delta")
        _validate_override_scalar(self.overrides, "relative_strength_weight")
        _validate_regime_multipliers(self.overrides)
        return self


class ETFExperimentRegistry(BaseModel):
    policy_metadata: PolicyMetadata
    base_configs: dict[str, ETFExperimentBaseConfigRef] = Field(min_length=1)
    experiments: dict[str, ETFExperimentConfig] = Field(min_length=1)

    @model_validator(mode="after")
    def validate_registry_integrity(self) -> Self:
        experiment_ids = [experiment.experiment_id for experiment in self.experiments.values()]
        if len(experiment_ids) != len(set(experiment_ids)):
            raise ValueError("ETF experiment IDs must be unique")
        for key, experiment in self.experiments.items():
            if experiment.experiment_id != key:
                raise ValueError(
                    "ETF experiment mapping key must match experiment_id: "
                    f"{key} != {experiment.experiment_id}"
                )
            base_config = self.base_configs.get(experiment.base_config_ref)
            if base_config is None:
                raise ValueError(
                    f"ETF experiment references unknown base_config_ref: "
                    f"{experiment.base_config_ref}"
                )
            _validate_base_weights(
                experiment_id=experiment.experiment_id,
                overrides=experiment.overrides,
                asset_symbols=base_config.asset_symbols,
            )
        return self

    @property
    def config_hash(self) -> str:
        return _config_hash(self.model_dump(mode="json"))


class ETFExperimentPolicyRef(BaseModel):
    description: str = Field(min_length=1)
    policy_status: str = Field(min_length=1)


class ETFExperimentPackConfig(BaseModel):
    pack_id: str = Field(min_length=1)
    description: str = Field(min_length=1)
    created_for_task: str = Field(min_length=1)
    experiment_ids: list[str] = Field(min_length=1)
    benchmark_set: str = Field(min_length=1)
    ranking_policy: str = Field(min_length=1)
    promotion_policy: str = Field(min_length=1)
    observe_only: bool
    production_effect: str = Field(min_length=1)
    broker_action: str = Field(min_length=1)
    manual_review_required: bool

    @model_validator(mode="after")
    def validate_pack_safety_and_scope(self) -> Self:
        if not self.observe_only:
            raise ValueError("ETF experiment packs must keep observe_only=true")
        if self.production_effect != "none":
            raise ValueError("ETF experiment packs must keep production_effect=none")
        if self.broker_action != "none":
            raise ValueError("ETF experiment packs must keep broker_action=none")
        if not self.manual_review_required:
            raise ValueError("ETF experiment packs must require manual review")
        if len(self.experiment_ids) != len(set(self.experiment_ids)):
            raise ValueError("ETF experiment packs must not contain duplicate experiments")
        return self


class ETFExperimentPackRegistry(BaseModel):
    policy_metadata: PolicyMetadata
    ranking_policies: dict[str, ETFExperimentPolicyRef] = Field(min_length=1)
    promotion_policies: dict[str, ETFExperimentPolicyRef] = Field(min_length=1)
    experiment_packs: dict[str, ETFExperimentPackConfig] = Field(min_length=1)

    @model_validator(mode="after")
    def validate_pack_registry(self) -> Self:
        for key, pack in self.experiment_packs.items():
            if pack.pack_id != key:
                raise ValueError(
                    f"ETF experiment pack mapping key must match pack_id: {key} != {pack.pack_id}"
                )
            if pack.ranking_policy not in self.ranking_policies:
                raise ValueError(
                    f"ETF experiment pack references unknown ranking_policy: "
                    f"{pack.ranking_policy}"
                )
            if pack.promotion_policy not in self.promotion_policies:
                raise ValueError(
                    f"ETF experiment pack references unknown promotion_policy: "
                    f"{pack.promotion_policy}"
                )
        return self

    @property
    def config_hash(self) -> str:
        return _config_hash(self.model_dump(mode="json"))


def load_experiment_registry(
    path: Path = DEFAULT_ETF_EXPERIMENTS_CONFIG_PATH,
) -> ETFExperimentRegistry:
    raw = safe_load_yaml_path(path)
    if not isinstance(raw, dict):
        raise ValueError(f"ETF experiment registry must be a YAML mapping: {path}")
    return ETFExperimentRegistry.model_validate(raw)


def load_experiment_pack_registry(
    path: Path = DEFAULT_ETF_EXPERIMENT_PACKS_CONFIG_PATH,
    *,
    experiment_registry: ETFExperimentRegistry | None = None,
) -> ETFExperimentPackRegistry:
    raw = safe_load_yaml_path(path)
    if not isinstance(raw, dict):
        raise ValueError(f"ETF experiment pack registry must be a YAML mapping: {path}")
    pack_registry = ETFExperimentPackRegistry.model_validate(raw)
    validate_experiment_pack_registry(
        pack_registry,
        experiment_registry=experiment_registry or load_experiment_registry(),
    )
    return pack_registry


def validate_experiment_pack_registry(
    pack_registry: ETFExperimentPackRegistry,
    *,
    experiment_registry: ETFExperimentRegistry,
) -> None:
    for pack in pack_registry.experiment_packs.values():
        for experiment_id in pack.experiment_ids:
            experiment = experiment_registry.experiments.get(experiment_id)
            if experiment is None:
                raise ValueError(
                    f"ETF experiment pack {pack.pack_id} references unknown experiment_id: "
                    f"{experiment_id}"
                )
            if experiment.benchmark_set != pack.benchmark_set:
                raise ValueError(
                    f"ETF experiment pack {pack.pack_id} benchmark_set mismatch for "
                    f"{experiment_id}"
                )
            _raise_if_experiment_is_unsafe(pack.pack_id, experiment)


def _raise_if_experiment_is_unsafe(
    pack_id: str,
    experiment: ETFExperimentConfig,
) -> None:
    if (
        not experiment.observe_only
        or experiment.production_effect != "none"
        or experiment.broker_action != "none"
        or not experiment.manual_review_required
    ):
        raise ValueError(
            f"ETF experiment pack {pack_id} includes unsafe experiment: "
            f"{experiment.experiment_id}"
        )


def _validate_base_weights(
    *,
    experiment_id: str,
    overrides: Mapping[str, Any],
    asset_symbols: list[str],
) -> None:
    value = overrides.get("base_weights")
    if value is None:
        return
    if not isinstance(value, Mapping) or not value:
        raise ValueError(f"{experiment_id} base_weights override must be a non-empty mapping")
    normalized_weights = {str(symbol).strip().upper(): weight for symbol, weight in value.items()}
    expected_symbols = set(asset_symbols)
    actual_symbols = set(normalized_weights)
    if actual_symbols != expected_symbols:
        missing = sorted(expected_symbols - actual_symbols)
        extra = sorted(actual_symbols - expected_symbols)
        raise ValueError(
            f"{experiment_id} base_weights must match base config assets; "
            f"missing={missing}; extra={extra}"
        )
    total = 0.0
    for symbol, raw_weight in normalized_weights.items():
        weight = _scale_float(raw_weight, f"{experiment_id} base_weights.{symbol}")
        total += weight
    if abs(total - 1.0) > 1e-6:
        raise ValueError(f"{experiment_id} base_weights must sum to 1.0")


def _validate_regime_multipliers(overrides: Mapping[str, Any]) -> None:
    value = overrides.get("regime_multipliers")
    if value is None:
        return
    if not isinstance(value, Mapping):
        raise ValueError("regime_multipliers override must be a mapping")
    expected = {"risk_on", "neutral", "risk_off"}
    actual = {str(key) for key in value}
    if actual != expected:
        raise ValueError("regime_multipliers must define risk_on, neutral, and risk_off")
    for key, raw_value in value.items():
        _scale_float(raw_value, f"regime_multipliers.{key}")


def _validate_override_scalar(overrides: Mapping[str, Any], key: str) -> None:
    if key not in overrides:
        return
    _scale_float(overrides[key], key)


def _scale_float(value: Any, field_name: str) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field_name} must be numeric") from exc
    if parsed < 0.0 or parsed > 1.0:
        raise ValueError(f"{field_name} must be within 0.0 and 1.0")
    return parsed


def _config_hash(payload: Mapping[str, Any]) -> str:
    normalized = json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str)
    return sha256(normalized.encode("utf-8")).hexdigest()
