from __future__ import annotations

import json
from collections.abc import Iterable, Mapping
from datetime import date
from hashlib import sha256
from pathlib import Path
from typing import Any, Literal, Self

import numpy as np
import pandas as pd
from pydantic import BaseModel, Field, model_validator

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.etf_portfolio.models import PolicyMetadata
from ai_trading_system.yaml_loader import safe_load_yaml_path

DEFAULT_AI_CONFIRMATION_UNIVERSE_CONFIG_PATH = (
    PROJECT_ROOT / "config" / "etf_portfolio" / "ai_confirmation_universe.yaml"
)
DEFAULT_AI_CONFIRMATION_POLICY_CONFIG_PATH = (
    PROJECT_ROOT / "config" / "etf_portfolio" / "ai_confirmation_policy.yaml"
)
DEFAULT_AI_CONFIRMATION_REPORT_DIR = (
    PROJECT_ROOT / "reports" / "etf_portfolio" / "ai_confirmation"
)
DEFAULT_AI_CONFIRMATION_FEATURE_DIR = DEFAULT_AI_CONFIRMATION_REPORT_DIR / "features"
DEFAULT_AI_CONFIRMATION_STANDALONE_REPORT_DIR = DEFAULT_AI_CONFIRMATION_REPORT_DIR / "reports"

AI_CONFIRMATION_BREADTH_FEATURE_VERSION = "ai_confirmation_breadth_v0_1"
AI_CONFIRMATION_REPORT_SCHEMA_VERSION = "ai_confirmation_report_v1"
AI_CONFIRMATION_EVENT_GROUP_ID = "event_risk_symbols_or_calendar_refs"

# TRADING-066B explicitly requires these price-derived confirmation horizons.
BREADTH_MA_WINDOWS = (20, 50, 100, 200)
BREADTH_RETURN_WINDOWS = (20, 60)

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


class AIScoreBandConfig(BaseModel):
    min_score: float = Field(ge=0, le=100)


class MegaCapAIScorePolicy(BaseModel):
    group_id: str = Field(min_length=1)
    component_weights: dict[str, float] = Field(min_length=1)
    relative_strength_full_scale_return: float = Field(gt=0)
    drawdown_full_penalty: float = Field(lt=0)
    coverage_warning_min: float = Field(ge=0, le=1)
    driver_positive_min: float = Field(ge=0, le=100)
    driver_negative_max: float = Field(ge=0, le=100)

    @model_validator(mode="after")
    def validate_weights(self) -> Self:
        total = sum(self.component_weights.values())
        if abs(total - 1.0) > 1e-6:
            raise ValueError(f"mega-cap AI component weights must sum to 1.0: {total}")
        if any(weight < 0 for weight in self.component_weights.values()):
            raise ValueError("mega-cap AI component weights must be non-negative")
        return self


class RelativeStrengthPairPolicy(BaseModel):
    numerator: str = Field(min_length=1)
    denominator: str = Field(min_length=1)
    component: str = Field(min_length=1)

    @model_validator(mode="after")
    def normalize_pair(self) -> Self:
        self.numerator = self.numerator.strip().upper()
        self.denominator = self.denominator.strip().upper()
        self.component = self.component.strip()
        if self.numerator == self.denominator:
            raise ValueError("relative strength pair numerator and denominator must differ")
        return self


class AISemiconductorRelativeStrengthPolicy(BaseModel):
    required_pairs: list[RelativeStrengthPairPolicy] = Field(min_length=1)
    optional_pairs: list[RelativeStrengthPairPolicy] = Field(default_factory=list)
    component_weights: dict[str, float] = Field(min_length=1)
    relative_return_full_scale: float = Field(gt=0)
    relative_drawdown_full_penalty: float = Field(lt=0)
    driver_positive_min: float = Field(ge=0, le=100)
    driver_negative_max: float = Field(ge=0, le=100)

    @model_validator(mode="after")
    def validate_weights_and_components(self) -> Self:
        total = sum(self.component_weights.values())
        if abs(total - 1.0) > 1e-6:
            raise ValueError(
                f"AI semiconductor relative strength weights must sum to 1.0: {total}"
            )
        if any(weight < 0 for weight in self.component_weights.values()):
            raise ValueError("AI semiconductor relative strength weights must be non-negative")
        pair_components = {pair.component for pair in [*self.required_pairs, *self.optional_pairs]}
        missing = sorted(
            pair_components - set(self.component_weights) - {"relative_drawdown_penalty"}
        )
        if missing:
            raise ValueError(f"relative strength pair components missing weights: {missing}")
        return self


class EventRiskBandConfig(BaseModel):
    max_score: float = Field(ge=0, le=100)


class EventRiskOverlayPolicy(BaseModel):
    severity_scores: dict[str, float] = Field(min_length=1)
    risk_bands: dict[str, EventRiskBandConfig] = Field(min_length=1)
    multiple_event_increment: float = Field(ge=0)
    maximum_event_risk_score: float = Field(ge=0, le=100)

    @model_validator(mode="after")
    def validate_event_policy(self) -> Self:
        required = {"low", "medium", "high", "critical"}
        missing = required - set(self.severity_scores)
        if missing:
            raise ValueError(f"event risk severity scores missing: {sorted(missing)}")
        if missing_bands := (required - set(self.risk_bands)):
            raise ValueError(f"event risk bands missing: {sorted(missing_bands)}")
        return self


class AIConfirmationCompositePolicy(BaseModel):
    component_weights: dict[str, float] = Field(min_length=1)
    insufficient_data_coverage_min: float = Field(ge=0, le=1)
    supports_overweight_min: float = Field(ge=0, le=100)
    supports_neutral_min: float = Field(ge=0, le=100)
    event_risk_high_min: float = Field(ge=0, le=100)

    @model_validator(mode="after")
    def validate_composite_weights(self) -> Self:
        total = sum(self.component_weights.values())
        if abs(total - 1.0) > 1e-6:
            raise ValueError(f"AI confirmation composite weights must sum to 1.0: {total}")
        if any(weight < 0 for weight in self.component_weights.values()):
            raise ValueError("AI confirmation composite weights must be non-negative")
        return self


class AIConfirmationPolicyConfig(BaseModel):
    policy_metadata: PolicyMetadata
    safety: AIConfirmationSafetyConfig
    score_bands: dict[str, AIScoreBandConfig] = Field(min_length=1)
    mega_cap_ai_score: MegaCapAIScorePolicy
    ai_semiconductor_relative_strength_score: AISemiconductorRelativeStrengthPolicy
    event_risk_overlay: EventRiskOverlayPolicy
    ai_confirmation_composite_score: AIConfirmationCompositePolicy

    @model_validator(mode="after")
    def validate_score_bands(self) -> Self:
        if "negative" not in self.score_bands:
            raise ValueError("AI confirmation score bands must include negative")
        if self.score_bands["negative"].min_score != 0:
            raise ValueError("negative score band must start at 0")
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


def load_ai_confirmation_policy_config(
    path: Path | str = DEFAULT_AI_CONFIRMATION_POLICY_CONFIG_PATH,
) -> AIConfirmationPolicyConfig:
    raw = safe_load_yaml_path(Path(path))
    if not isinstance(raw, dict):
        raise ValueError(f"AI confirmation policy config must be a YAML mapping: {path}")
    return AIConfirmationPolicyConfig.model_validate(raw)


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


def ai_confirmation_price_group_ids(config: AIConfirmationUniverseConfig) -> tuple[str, ...]:
    return tuple(
        group_id
        for group_id in sorted(config.ai_confirmation_universe)
        if group_id != AI_CONFIRMATION_EVENT_GROUP_ID
        and config.ai_confirmation_universe[group_id].enabled
    )


def all_enabled_price_tickers(config: AIConfirmationUniverseConfig) -> tuple[str, ...]:
    tickers: set[str] = set()
    for group_id in ai_confirmation_price_group_ids(config):
        tickers.update(symbol.ticker for symbol in enabled_symbols_for_group(config, group_id))
    return tuple(sorted(tickers))


def validate_ai_confirmation_data_availability(
    config: AIConfirmationUniverseConfig,
    available_symbols: Iterable[str],
    *,
    group_ids: Iterable[str] | None = None,
) -> dict[str, Any]:
    available = {symbol.strip().upper() for symbol in available_symbols}
    selected_group_ids = tuple(group_ids) if group_ids is not None else tuple(
        sorted(config.ai_confirmation_universe)
    )
    group_reports: list[dict[str, Any]] = []
    errors: list[str] = []
    warnings: list[str] = []
    for group_id in sorted(selected_group_ids):
        if group_id not in config.ai_confirmation_universe:
            raise KeyError(f"unknown AI confirmation group: {group_id}")
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


def build_ai_confirmation_breadth_features(
    prices: pd.DataFrame,
    *,
    config: AIConfirmationUniverseConfig,
    run_date: date,
    group_ids: Iterable[str] | None = None,
) -> list[dict[str, Any]]:
    frame = _prepare_ai_price_history(prices, run_date)
    selected_group_ids = (
        tuple(group_ids) if group_ids is not None else ai_confirmation_price_group_ids(config)
    )
    available_symbols = set(frame["symbol"].dropna().astype(str))
    availability = validate_ai_confirmation_data_availability(
        config,
        available_symbols,
        group_ids=selected_group_ids,
    )
    availability_by_group = {
        str(item["group_id"]): item for item in availability["group_reports"]
    }
    records: list[dict[str, Any]] = []
    for group_id in sorted(selected_group_ids):
        group = config.ai_confirmation_universe.get(group_id)
        if group is None:
            raise KeyError(f"unknown AI confirmation group: {group_id}")
        symbols = enabled_symbols_for_group(config, group_id)
        symbol_tickers = [symbol.ticker for symbol in symbols]
        symbol_stats = [
            _symbol_breadth_stats(frame, ticker)
            for ticker in symbol_tickers
            if ticker in available_symbols
        ]
        warnings = _group_availability_warnings(availability_by_group[group_id])
        warnings.extend(_insufficient_history_warnings(group_id, symbol_stats))
        feature_values = _group_breadth_values(frame, symbol_tickers, symbol_stats)
        records.append(
            {
                "date": run_date.isoformat(),
                "group_id": group_id,
                "feature_version": AI_CONFIRMATION_BREADTH_FEATURE_VERSION,
                "feature_values": feature_values,
                "symbol_count": len(symbol_tickers),
                "valid_symbol_count": len(symbol_stats),
                "data_coverage_ratio": _safe_ratio(len(symbol_stats), len(symbol_tickers)),
                "warnings": sorted(set(warnings)),
                **AI_CONFIRMATION_SAFETY,
            }
        )
    return records


def ai_confirmation_breadth_records_to_frame(records: list[dict[str, Any]]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for record in records:
        feature_values = dict(record.get("feature_values") or {})
        warnings = list(record.get("warnings") or [])
        row = {
            key: value
            for key, value in record.items()
            if key not in {"feature_values", "warnings"}
        }
        row.update(feature_values)
        row["feature_values_json"] = json.dumps(
            feature_values,
            ensure_ascii=False,
            sort_keys=True,
        )
        row["warnings_json"] = json.dumps(warnings, ensure_ascii=False, sort_keys=True)
        rows.append(row)
    return pd.DataFrame(rows).sort_values(["date", "group_id"]).reset_index(drop=True)


def write_ai_confirmation_breadth_features(
    records: list[dict[str, Any]],
    *,
    output_dir: Path,
    run_date: date,
    data_quality_status: str,
    data_quality_report: str | None = None,
) -> dict[str, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    stem = f"ai_confirmation_features_{run_date.isoformat()}"
    payload = {
        "report_type": "ai_confirmation_breadth_features",
        "date": run_date.isoformat(),
        "feature_version": AI_CONFIRMATION_BREADTH_FEATURE_VERSION,
        "data_quality_status": data_quality_status,
        "data_quality_report": data_quality_report,
        "records": records,
        **AI_CONFIRMATION_SAFETY,
    }
    json_path = output_dir / f"{stem}.json"
    csv_path = output_dir / f"{stem}.csv"
    json_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    ai_confirmation_breadth_records_to_frame(records).to_csv(csv_path, index=False)
    return {"json": json_path, "csv": csv_path}


def build_mega_cap_ai_confirmation_score(
    prices: pd.DataFrame,
    *,
    breadth_records: list[dict[str, Any]],
    universe_config: AIConfirmationUniverseConfig,
    policy_config: AIConfirmationPolicyConfig,
    run_date: date,
) -> dict[str, Any]:
    policy = policy_config.mega_cap_ai_score
    group_id = policy.group_id
    if group_id not in universe_config.ai_confirmation_universe:
        raise KeyError(f"unknown AI confirmation group: {group_id}")
    breadth_record = _breadth_record_for_group(breadth_records, group_id)
    feature_values = dict(breadth_record.get("feature_values") or {})
    frame = _prepare_ai_price_history(prices, run_date)
    tickers = [symbol.ticker for symbol in enabled_symbols_for_group(universe_config, group_id)]
    warnings = list(breadth_record.get("warnings") or [])
    data_coverage_ratio = _safe_float(breadth_record.get("data_coverage_ratio")) or 0.0

    component_scores = {
        "mega_cap_trend_score": _average_score(
            feature_values.get("percent_above_50d_ma"),
            feature_values.get("percent_above_200d_ma"),
        ),
        "mega_cap_momentum_score": _average_score(
            feature_values.get("percent_positive_20d_return"),
            feature_values.get("percent_positive_60d_return"),
        ),
        "mega_cap_breadth_score": _average_score(
            feature_values.get("percent_above_20d_ma"),
            feature_values.get("percent_above_50d_ma"),
            feature_values.get("percent_above_100d_ma"),
            feature_values.get("percent_above_200d_ma"),
        ),
        "mega_cap_relative_strength_vs_QQQ": _relative_strength_component(
            frame,
            tickers,
            "QQQ",
            policy,
        ),
        "mega_cap_relative_strength_vs_SPY": _relative_strength_component(
            frame,
            tickers,
            "SPY",
            policy,
        ),
        "mega_cap_drawdown_penalty": _drawdown_component(
            feature_values.get("group_drawdown_from_60d_high"),
            policy,
        ),
        "data_coverage_penalty": _clamp_score(data_coverage_ratio * 100.0),
    }
    for component, score in component_scores.items():
        if score is None:
            warnings.append(f"{group_id}:component_unavailable:{component}")
            component_scores[component] = 50.0
    if data_coverage_ratio < policy.coverage_warning_min:
        warnings.append(f"{group_id}:low_data_coverage:{data_coverage_ratio:.2f}")
    weighted_score = sum(
        component_scores[component] * policy.component_weights[component]
        for component in policy.component_weights
    )
    score_value = round(_clamp_score(weighted_score), 2)
    return {
        "date": run_date.isoformat(),
        "score_name": "MegaCapAIScore",
        "score_value": score_value,
        "score_band": score_band(score_value, policy_config),
        "component_scores": {
            component: round(float(score), 2) for component, score in component_scores.items()
        },
        "top_positive_drivers": _driver_list(
            component_scores,
            positive=True,
            threshold=policy.driver_positive_min,
        ),
        "top_negative_drivers": _driver_list(
            component_scores,
            positive=False,
            threshold=policy.driver_negative_max,
        ),
        "data_coverage_ratio": data_coverage_ratio,
        "warnings": sorted(set(warnings)),
        "policy_version": policy_config.policy_metadata.version,
        "policy_config_hash": policy_config.config_hash,
        **AI_CONFIRMATION_SAFETY,
    }


def build_ai_semiconductor_relative_strength_score(
    prices: pd.DataFrame,
    *,
    policy_config: AIConfirmationPolicyConfig,
    run_date: date,
) -> dict[str, Any]:
    policy = policy_config.ai_semiconductor_relative_strength_score
    frame = _prepare_ai_price_history(prices, run_date)
    pair_features: list[dict[str, Any]] = []
    warnings: list[str] = []
    for pair in policy.required_pairs:
        features = _relative_strength_pair_features(frame, pair, policy)
        if features is None:
            warnings.append(
                f"required_pair_missing:{pair.numerator}/{pair.denominator}"
            )
            continue
        features["optional"] = False
        pair_features.append(features)
    for pair in policy.optional_pairs:
        features = _relative_strength_pair_features(frame, pair, policy)
        if features is None:
            warnings.append(f"optional_pair_missing:{pair.numerator}/{pair.denominator}")
            continue
        features["optional"] = True
        pair_features.append(features)

    component_scores: dict[str, float] = {}
    for component in policy.component_weights:
        if component == "relative_drawdown_penalty":
            component_scores[component] = _relative_drawdown_component(pair_features, policy)
            continue
        scores = [
            float(pair["relative_momentum_score"])
            for pair in pair_features
            if pair["component"] == component
            and _is_finite_number(pair.get("relative_momentum_score"))
        ]
        if scores:
            component_scores[component] = _clamp_score(float(np.mean(scores)))
        else:
            component_scores[component] = 50.0
            warnings.append(f"component_unavailable:{component}")

    weighted_score = sum(
        component_scores[component] * policy.component_weights[component]
        for component in policy.component_weights
    )
    score_value = round(_clamp_score(weighted_score), 2)
    return {
        "date": run_date.isoformat(),
        "score_name": "AISemiconductorRelativeStrengthScore",
        "score_value": score_value,
        "score_band": score_band(score_value, policy_config),
        "component_scores": {
            component: round(float(score), 2) for component, score in component_scores.items()
        },
        "pair_features": pair_features,
        "top_positive_drivers": _driver_list(
            component_scores,
            positive=True,
            threshold=policy.driver_positive_min,
        ),
        "top_negative_drivers": _driver_list(
            component_scores,
            positive=False,
            threshold=policy.driver_negative_max,
        ),
        "warnings": sorted(set(warnings)),
        "policy_version": policy_config.policy_metadata.version,
        "policy_config_hash": policy_config.config_hash,
        **AI_CONFIRMATION_SAFETY,
    }


def build_ai_event_risk_overlay(
    events: Iterable[Mapping[str, Any]],
    *,
    universe_config: AIConfirmationUniverseConfig,
    policy_config: AIConfirmationPolicyConfig,
    run_date: date,
) -> dict[str, Any]:
    policy = policy_config.event_risk_overlay
    normalized_events = [_normalize_event(event) for event in events]
    active_events: list[dict[str, Any]] = []
    upcoming_events: list[dict[str, Any]] = []
    recent_events: list[dict[str, Any]] = []
    for event in normalized_events:
        event_date = event["event_date"]
        lookahead_start = event_date - pd.Timedelta(days=event["lookahead_window_days"])
        lookback_end = event_date + pd.Timedelta(days=event["lookback_window_days"])
        run_timestamp = pd.Timestamp(run_date)
        if lookahead_start <= run_timestamp <= lookback_end:
            active_events.append(event)
        if lookahead_start <= run_timestamp < event_date:
            upcoming_events.append(event)
        if event_date < run_timestamp <= lookback_end:
            recent_events.append(event)
    score = _event_risk_score(active_events, policy)
    affected_groups = _affected_event_groups(active_events, universe_config)
    reason_codes = [
        f"{event['event_type']}:{event['severity']}" for event in active_events
    ] or ["no_active_ai_event_risk"]
    return {
        "date": run_date.isoformat(),
        "event_risk_score": score,
        "active_events": [_event_output(event) for event in active_events],
        "upcoming_events": [_event_output(event) for event in upcoming_events],
        "recent_events": [_event_output(event) for event in recent_events],
        "affected_groups": affected_groups,
        "risk_band": event_risk_band(score, policy_config),
        "reason_codes": reason_codes,
        **AI_CONFIRMATION_SAFETY,
    }


def build_ai_confirmation_composite_score(
    *,
    breadth_records: list[dict[str, Any]],
    mega_cap_score: Mapping[str, Any],
    relative_strength_score: Mapping[str, Any],
    event_risk_overlay: Mapping[str, Any],
    policy_config: AIConfirmationPolicyConfig,
    run_date: date,
) -> dict[str, Any]:
    policy = policy_config.ai_confirmation_composite_score
    semiconductor_breadth = _semiconductor_breadth_score(breadth_records)
    data_coverage = _composite_data_coverage(breadth_records, mega_cap_score)
    event_risk_score = _safe_float(event_risk_overlay.get("event_risk_score")) or 0.0
    component_scores = {
        "semiconductor_breadth": semiconductor_breadth,
        "mega_cap_ai": float(mega_cap_score["score_value"]),
        "ai_relative_strength": float(relative_strength_score["score_value"]),
        "event_risk_adjustment": _clamp_score(100.0 - event_risk_score),
        "data_coverage": _clamp_score(data_coverage * 100.0),
    }
    weighted_score = sum(
        component_scores[component] * policy.component_weights[component]
        for component in policy.component_weights
    )
    score_value = round(_clamp_score(weighted_score), 2)
    reason_codes = _composite_reason_codes(
        component_scores,
        data_coverage,
        event_risk_score,
        policy,
    )
    return {
        "date": run_date.isoformat(),
        "score_name": "AIConfirmationScore",
        "AIConfirmationScore": score_value,
        "score_value": score_value,
        "component_scores": {
            key: round(float(value), 2) for key, value in component_scores.items()
        },
        "score_band": score_band(score_value, policy_config),
        "action_hint": _composite_action_hint(score_value, data_coverage, event_risk_score, policy),
        "reason_codes": reason_codes,
        "data_coverage_ratio": data_coverage,
        "policy_version": policy_config.policy_metadata.version,
        "policy_config_hash": policy_config.config_hash,
        **AI_CONFIRMATION_SAFETY,
    }


def build_ai_confirmation_report(
    *,
    prices: pd.DataFrame,
    events: Iterable[Mapping[str, Any]],
    universe_config: AIConfirmationUniverseConfig,
    policy_config: AIConfirmationPolicyConfig,
    run_date: date,
    data_quality_status: str,
    data_quality_report: str | None = None,
    market_regime: str | None = None,
    requested_date_range: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    breadth_records = build_ai_confirmation_breadth_features(
        prices,
        config=universe_config,
        run_date=run_date,
    )
    mega_cap_score = build_mega_cap_ai_confirmation_score(
        prices,
        breadth_records=breadth_records,
        universe_config=universe_config,
        policy_config=policy_config,
        run_date=run_date,
    )
    relative_strength_score = build_ai_semiconductor_relative_strength_score(
        prices,
        policy_config=policy_config,
        run_date=run_date,
    )
    event_risk_overlay = build_ai_event_risk_overlay(
        events,
        universe_config=universe_config,
        policy_config=policy_config,
        run_date=run_date,
    )
    composite_score = build_ai_confirmation_composite_score(
        breadth_records=breadth_records,
        mega_cap_score=mega_cap_score,
        relative_strength_score=relative_strength_score,
        event_risk_overlay=event_risk_overlay,
        policy_config=policy_config,
        run_date=run_date,
    )
    return {
        "schema_version": AI_CONFIRMATION_REPORT_SCHEMA_VERSION,
        "report_type": "ai_confirmation_report",
        "date": run_date.isoformat(),
        "market_regime": market_regime or "",
        "requested_date_range": dict(
            requested_date_range or {"start": "", "end": run_date.isoformat()}
        ),
        "safety_banner": _ai_confirmation_safety_banner(),
        "data_quality": {
            "status": data_quality_status,
            "report_path": data_quality_report or "",
        },
        "policy": {
            "universe_version": universe_config.policy_metadata.version,
            "universe_config_hash": universe_config.config_hash,
            "score_policy_version": policy_config.policy_metadata.version,
            "score_policy_hash": policy_config.config_hash,
        },
        "AIConfirmationScore": composite_score,
        "component_scores": {
            "semiconductor_breadth": composite_score["component_scores"]["semiconductor_breadth"],
            "mega_cap_ai": mega_cap_score["score_value"],
            "ai_relative_strength": relative_strength_score["score_value"],
            "event_risk_adjustment": composite_score["component_scores"]["event_risk_adjustment"],
            "data_coverage": composite_score["component_scores"]["data_coverage"],
        },
        "semiconductor_breadth": _breadth_record_or_empty(
            breadth_records,
            "semiconductor_hardware",
        ),
        "mega_cap_ai_confirmation": mega_cap_score,
        "ai_semiconductor_relative_strength": relative_strength_score,
        "event_risk_overlay": event_risk_overlay,
        "data_coverage": _report_data_coverage(
            breadth_records,
            mega_cap_score,
            composite_score,
        ),
        "top_positive_drivers": _report_drivers(
            mega_cap_score,
            relative_strength_score,
            positive=True,
        ),
        "top_negative_drivers": _report_drivers(
            mega_cap_score,
            relative_strength_score,
            positive=False,
        ),
        "candidate_only_usage_note": (
            "AI confirmation is observe-only and candidate-only; it must not mutate "
            "official ETF target weights or trigger broker action."
        ),
        "recommended_shadow_experiment_usage": (
            "Use AIConfirmationScore only as a bounded shadow overlay input after "
            "manual review; compare candidate/shadow/hypothetical weights against "
            "the ETF baseline."
        ),
        **AI_CONFIRMATION_SAFETY,
    }


def write_ai_confirmation_report(
    payload: Mapping[str, Any],
    *,
    json_path: Path,
    markdown_path: Path,
) -> tuple[Path, Path]:
    json_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    markdown_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_path.write_text(render_ai_confirmation_report_markdown(payload), encoding="utf-8")
    return json_path, markdown_path


def load_ai_confirmation_events(path: Path | None) -> list[dict[str, Any]]:
    if path is None or not path.exists():
        return []
    if path.suffix.lower() == ".json":
        payload = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(payload, Mapping):
            payload = payload.get("events", [])
        if not isinstance(payload, list):
            raise ValueError(f"AI confirmation event JSON must be a list or events mapping: {path}")
        return [dict(item) for item in payload if isinstance(item, Mapping)]
    frame = pd.read_csv(path)
    return frame.to_dict(orient="records")


def render_ai_confirmation_report_markdown(payload: Mapping[str, Any]) -> str:
    score = _mapping(payload.get("AIConfirmationScore"))
    components = _mapping(payload.get("component_scores"))
    data_coverage = _mapping(payload.get("data_coverage"))
    event_risk = _mapping(payload.get("event_risk_overlay"))
    mega_cap = _mapping(payload.get("mega_cap_ai_confirmation"))
    relative_strength = _mapping(payload.get("ai_semiconductor_relative_strength"))
    breadth = _mapping(payload.get("semiconductor_breadth"))
    feature_values = _mapping(breadth.get("feature_values"))
    lines = [
        "# AI Confirmation Report",
        "",
        f"- Date: {payload.get('date')}",
        f"- Market Regime: {payload.get('market_regime') or 'unspecified'}",
        f"- Safety: {payload.get('safety_banner')}",
        f"- Candidate-only usage: {payload.get('candidate_only_usage_note')}",
        f"- Data Quality: {_mapping(payload.get('data_quality')).get('status')}",
        "",
        "## AIConfirmationScore Summary",
        "",
        f"- AIConfirmationScore: {_fmt_number(score.get('score_value'))}",
        f"- Score Band: {score.get('score_band')}",
        f"- Action Hint: {score.get('action_hint')}",
        f"- Manual Review Required: {payload.get('manual_review_required')}",
        "",
        "## Component Scores",
        "",
        "| Component | Score |",
        "|---|---:|",
    ]
    for component, value in components.items():
        lines.append(f"| {component} | {_fmt_number(value)} |")
    lines.extend(
        [
            "",
            "## Semiconductor Breadth",
            "",
            f"- Symbol Count: {breadth.get('symbol_count', 0)}",
            f"- Valid Symbol Count: {breadth.get('valid_symbol_count', 0)}",
            f"- Data Coverage: {_fmt_pct(breadth.get('data_coverage_ratio'))}",
            f"- Above 50D MA: {_fmt_pct(feature_values.get('percent_above_50d_ma'))}",
            f"- Above 200D MA: {_fmt_pct(feature_values.get('percent_above_200d_ma'))}",
            "",
            "## Mega-Cap AI Confirmation",
            "",
            f"- Score: {_fmt_number(mega_cap.get('score_value'))}",
            f"- Band: {mega_cap.get('score_band')}",
            f"- Data Coverage: {_fmt_pct(mega_cap.get('data_coverage_ratio'))}",
            "",
            "## AI / Semiconductor Relative Strength",
            "",
            f"- Score: {_fmt_number(relative_strength.get('score_value'))}",
            f"- Band: {relative_strength.get('score_band')}",
            f"- Pair Count: {len(_records(relative_strength.get('pair_features')))}",
            "",
            "## Event Risk Overlay",
            "",
            f"- Event Risk Score: {_fmt_number(event_risk.get('event_risk_score'))}",
            f"- Risk Band: {event_risk.get('risk_band')}",
            f"- Active Events: {len(_records(event_risk.get('active_events')))}",
            "",
            "## Data Coverage",
            "",
            f"- Composite Coverage: {_fmt_pct(data_coverage.get('composite_data_coverage_ratio'))}",
            f"- Breadth Groups: {data_coverage.get('breadth_group_count')}",
            "",
            "## Drivers",
            "",
            f"- Top Positive Drivers: {_join_list(payload.get('top_positive_drivers'))}",
            f"- Top Negative Drivers: {_join_list(payload.get('top_negative_drivers'))}",
            "",
            "## Candidate-Only Usage",
            "",
            str(payload.get("recommended_shadow_experiment_usage")),
        ]
    )
    return "\n".join(lines) + "\n"


def event_risk_band(score: float, policy_config: AIConfirmationPolicyConfig) -> str:
    for band, config in sorted(
        policy_config.event_risk_overlay.risk_bands.items(),
        key=lambda item: item[1].max_score,
    ):
        if score <= config.max_score:
            return band
    return "critical"


def score_band(score: float, policy_config: AIConfirmationPolicyConfig) -> str:
    for band, config in sorted(
        policy_config.score_bands.items(),
        key=lambda item: item[1].min_score,
        reverse=True,
    ):
        if score >= config.min_score:
            return band
    return "negative"


def _breadth_record_or_empty(
    breadth_records: list[dict[str, Any]],
    group_id: str,
) -> dict[str, Any]:
    try:
        return _breadth_record_for_group(breadth_records, group_id)
    except KeyError:
        return {
            "group_id": group_id,
            "feature_values": {},
            "symbol_count": 0,
            "valid_symbol_count": 0,
            "data_coverage_ratio": 0.0,
            "warnings": [f"{group_id}:missing_breadth_record"],
            **AI_CONFIRMATION_SAFETY,
        }


def _report_data_coverage(
    breadth_records: list[dict[str, Any]],
    mega_cap_score: Mapping[str, Any],
    composite_score: Mapping[str, Any],
) -> dict[str, Any]:
    group_coverages = {
        str(record.get("group_id")): float(record["data_coverage_ratio"])
        for record in breadth_records
        if _is_finite_number(record.get("data_coverage_ratio"))
    }
    return {
        "composite_data_coverage_ratio": composite_score.get("data_coverage_ratio"),
        "mega_cap_data_coverage_ratio": mega_cap_score.get("data_coverage_ratio"),
        "breadth_group_count": len(breadth_records),
        "group_data_coverage": group_coverages,
    }


def _report_drivers(
    mega_cap_score: Mapping[str, Any],
    relative_strength_score: Mapping[str, Any],
    *,
    positive: bool,
) -> list[str]:
    key = "top_positive_drivers" if positive else "top_negative_drivers"
    drivers = [
        f"MegaCapAIScore:{driver}"
        for driver in _string_list(mega_cap_score.get(key))
    ]
    drivers.extend(
        f"AISemiconductorRelativeStrengthScore:{driver}"
        for driver in _string_list(relative_strength_score.get(key))
    )
    return drivers or ["none"]


def _ai_confirmation_safety_banner() -> str:
    return (
        "observe_only=true; candidate_only=true; production_effect=none; "
        "broker_action=none; manual_review_required=true"
    )


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


def _prepare_ai_price_history(prices: pd.DataFrame, run_date: date) -> pd.DataFrame:
    required = {"date", "symbol", "adj_close"}
    missing = sorted(required - set(prices.columns))
    if missing:
        raise ValueError(f"AI confirmation prices missing columns: {', '.join(missing)}")
    frame = prices.copy()
    frame["symbol"] = frame["symbol"].astype(str).str.strip().str.upper()
    frame["_date"] = pd.to_datetime(frame["date"], errors="coerce")
    frame["_adj_close"] = pd.to_numeric(frame["adj_close"], errors="coerce")
    frame = frame.loc[
        frame["_date"].notna()
        & (frame["_date"] <= pd.Timestamp(run_date))
        & frame["_adj_close"].notna()
        & (frame["_adj_close"] > 0)
    ].copy()
    return frame.sort_values(["symbol", "_date"]).reset_index(drop=True)


def _symbol_breadth_stats(frame: pd.DataFrame, ticker: str) -> dict[str, Any]:
    history = frame.loc[frame["symbol"] == ticker].sort_values("_date").reset_index(drop=True)
    closes = history["_adj_close"].astype(float)
    current = float(closes.iloc[-1])
    stats: dict[str, Any] = {
        "ticker": ticker,
        "latest_date": pd.Timestamp(history["_date"].iloc[-1]).date().isoformat(),
        "adj_close": current,
    }
    for window in BREADTH_MA_WINDOWS:
        if len(closes) < window:
            stats[f"above_{window}d_ma"] = None
            continue
        moving_average = float(closes.rolling(window=window, min_periods=window).mean().iloc[-1])
        stats[f"above_{window}d_ma"] = bool(current > moving_average)
    for window in BREADTH_RETURN_WINDOWS:
        if len(closes) <= window:
            stats[f"return_{window}d"] = None
            continue
        stats[f"return_{window}d"] = float(current / float(closes.iloc[-window - 1]) - 1.0)
    stats["return_1d"] = (
        float(current / float(closes.iloc[-2]) - 1.0) if len(closes) > 1 else None
    )
    return stats


def _group_breadth_values(
    frame: pd.DataFrame,
    symbol_tickers: list[str],
    symbol_stats: list[dict[str, Any]],
) -> dict[str, Any]:
    values: dict[str, Any] = {}
    for window in BREADTH_MA_WINDOWS:
        values[f"percent_above_{window}d_ma"] = _share_true(
            stat.get(f"above_{window}d_ma") for stat in symbol_stats
        )
    for window in BREADTH_RETURN_WINDOWS:
        returns = [
            float(stat[f"return_{window}d"])
            for stat in symbol_stats
            if _is_finite_number(stat.get(f"return_{window}d"))
        ]
        values[f"percent_positive_{window}d_return"] = _share_positive(returns)
        values[f"median_{window}d_return"] = _safe_float(np.median(returns)) if returns else None
        values[f"equal_weight_group_return_{window}d"] = (
            _safe_float(np.mean(returns)) if returns else None
        )
    values["group_drawdown_from_60d_high"] = _group_drawdown_from_high(
        frame,
        symbol_tickers,
        window=60,
    )
    values["group_realized_vol_20d"] = _group_realized_vol(frame, symbol_tickers, window=20)
    values["advancing_declining_ratio"] = _advancing_declining_ratio(symbol_stats)
    return values


def _group_drawdown_from_high(
    frame: pd.DataFrame,
    symbol_tickers: list[str],
    *,
    window: int,
) -> float | None:
    group_index = _equal_weight_group_index(frame, symbol_tickers)
    if len(group_index) < window:
        return None
    recent = group_index.tail(window)
    high = recent.max()
    if not _is_finite_number(high) or high <= 0:
        return None
    return _safe_float(recent.iloc[-1] / high - 1.0)


def _group_realized_vol(
    frame: pd.DataFrame,
    symbol_tickers: list[str],
    *,
    window: int,
) -> float | None:
    group_index = _equal_weight_group_index(frame, symbol_tickers)
    if len(group_index) <= window:
        return None
    returns = group_index.pct_change().dropna().tail(window)
    if returns.empty:
        return None
    return _safe_float(returns.std() * np.sqrt(252))


def _equal_weight_group_index(frame: pd.DataFrame, symbol_tickers: list[str]) -> pd.Series:
    if not symbol_tickers:
        return pd.Series(dtype=float)
    selected = frame.loc[frame["symbol"].isin(symbol_tickers)].copy()
    if selected.empty:
        return pd.Series(dtype=float)
    pivot = selected.pivot_table(
        index="_date",
        columns="symbol",
        values="_adj_close",
        aggfunc="last",
    ).sort_index()
    pivot = pivot.ffill()
    returns = pivot.pct_change()
    group_returns = returns.mean(axis=1, skipna=True).fillna(0.0)
    return (1.0 + group_returns).cumprod()


def _group_availability_warnings(group_report: Mapping[str, Any]) -> list[str]:
    group_id = str(group_report["group_id"])
    warnings: list[str] = []
    for ticker in group_report.get("missing_required", []):
        warnings.append(f"{group_id}:missing_required:{ticker}")
    for ticker in group_report.get("missing_optional", []):
        warnings.append(f"{group_id}:missing_optional:{ticker}")
    return warnings


def _insufficient_history_warnings(
    group_id: str,
    symbol_stats: list[dict[str, Any]],
) -> list[str]:
    warnings: list[str] = []
    for window in BREADTH_MA_WINDOWS:
        if symbol_stats and all(stat.get(f"above_{window}d_ma") is None for stat in symbol_stats):
            warnings.append(f"{group_id}:insufficient_history:ma_{window}d")
    for window in BREADTH_RETURN_WINDOWS:
        if symbol_stats and all(stat.get(f"return_{window}d") is None for stat in symbol_stats):
            warnings.append(f"{group_id}:insufficient_history:return_{window}d")
    return warnings


def _advancing_declining_ratio(symbol_stats: list[dict[str, Any]]) -> float | None:
    returns = [
        float(stat["return_1d"])
        for stat in symbol_stats
        if _is_finite_number(stat.get("return_1d"))
    ]
    if not returns:
        return None
    advancing = sum(1 for value in returns if value > 0)
    declining = sum(1 for value in returns if value < 0)
    return _safe_float(advancing / max(declining, 1))


def _share_true(values: Iterable[Any]) -> float | None:
    booleans = [value for value in values if isinstance(value, bool)]
    if not booleans:
        return None
    return _safe_ratio(sum(1 for value in booleans if value), len(booleans))


def _share_positive(values: Iterable[float]) -> float | None:
    numbers = [float(value) for value in values if _is_finite_number(value)]
    if not numbers:
        return None
    return _safe_ratio(sum(1 for value in numbers if value > 0), len(numbers))


def _safe_ratio(numerator: int | float, denominator: int | float) -> float | None:
    if denominator == 0:
        return None
    return _safe_float(float(numerator) / float(denominator))


def _safe_float(value: Any) -> float | None:
    if not _is_finite_number(value):
        return None
    return float(value)


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _records(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [dict(item) for item in value if isinstance(item, Mapping)]


def _string_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list | tuple | set):
        return [str(item) for item in value]
    return [str(value)]


def _fmt_number(value: Any) -> str:
    parsed = _safe_float(value)
    if parsed is None:
        return "-"
    return f"{parsed:.2f}"


def _fmt_pct(value: Any) -> str:
    parsed = _safe_float(value)
    if parsed is None:
        return "-"
    return f"{parsed:.1%}"


def _join_list(value: Any) -> str:
    values = _string_list(value)
    return ", ".join(values) if values else "none"


def _is_finite_number(value: Any) -> bool:
    if value is None:
        return False
    try:
        return bool(np.isfinite(float(value)))
    except (TypeError, ValueError):
        return False


def _breadth_record_for_group(
    breadth_records: list[dict[str, Any]],
    group_id: str,
) -> dict[str, Any]:
    for record in breadth_records:
        if record.get("group_id") == group_id:
            return record
    raise KeyError(f"missing breadth record for AI confirmation group: {group_id}")


def _average_score(*fractions: Any) -> float | None:
    values = [float(value) * 100.0 for value in fractions if _is_finite_number(value)]
    if not values:
        return None
    return _clamp_score(float(np.mean(values)))


def _relative_strength_component(
    frame: pd.DataFrame,
    tickers: list[str],
    benchmark: str,
    policy: MegaCapAIScorePolicy,
) -> float | None:
    group_return = _group_return(frame, tickers, window=60)
    benchmark_return = _symbol_return(frame, benchmark, window=60)
    if group_return is None or benchmark_return is None:
        return None
    relative_return = group_return - benchmark_return
    normalized = 50.0 + (relative_return / policy.relative_strength_full_scale_return) * 50.0
    return _clamp_score(normalized)


def _drawdown_component(value: Any, policy: MegaCapAIScorePolicy) -> float | None:
    if not _is_finite_number(value):
        return None
    drawdown = float(value)
    if drawdown >= 0:
        return 100.0
    capped_penalty = min(abs(drawdown), abs(policy.drawdown_full_penalty))
    normalized = 100.0 * (1.0 - capped_penalty / abs(policy.drawdown_full_penalty))
    return _clamp_score(normalized)


def _group_return(frame: pd.DataFrame, tickers: list[str], *, window: int) -> float | None:
    returns: list[float] = []
    for ticker in tickers:
        value = _symbol_return(frame, ticker, window=window)
        if value is not None:
            returns.append(value)
    if not returns:
        return None
    return _safe_float(np.mean(returns))


def _symbol_return(frame: pd.DataFrame, ticker: str, *, window: int) -> float | None:
    history = frame.loc[frame["symbol"] == ticker].sort_values("_date")
    if len(history) <= window:
        return None
    closes = history["_adj_close"].astype(float).reset_index(drop=True)
    return _safe_float(closes.iloc[-1] / closes.iloc[-window - 1] - 1.0)


def _driver_list(
    component_scores: Mapping[str, float],
    *,
    positive: bool,
    threshold: float,
) -> list[str]:
    if positive:
        selected = [
            (component, score)
            for component, score in component_scores.items()
            if score >= threshold
        ]
        selected.sort(key=lambda item: item[1], reverse=True)
    else:
        selected = [
            (component, score)
            for component, score in component_scores.items()
            if score <= threshold
        ]
        selected.sort(key=lambda item: item[1])
    return [f"{component}={score:.2f}" for component, score in selected[:3]]


def _clamp_score(value: float) -> float:
    return min(100.0, max(0.0, float(value)))


def _relative_strength_pair_features(
    frame: pd.DataFrame,
    pair: RelativeStrengthPairPolicy,
    policy: AISemiconductorRelativeStrengthPolicy,
) -> dict[str, Any] | None:
    ratio = _relative_price_ratio(frame, pair.numerator, pair.denominator)
    if ratio is None or len(ratio) <= 120:
        return None
    features: dict[str, Any] = {
        "pair": f"{pair.numerator}/{pair.denominator}",
        "numerator": pair.numerator,
        "denominator": pair.denominator,
        "component": pair.component,
    }
    returns_for_score: list[float] = []
    for window in (20, 60, 120):
        relative_return = _series_return(ratio, window=window)
        features[f"relative_return_{window}d"] = relative_return
        if relative_return is not None:
            returns_for_score.append(relative_return)
    for window in (50, 200):
        moving_average = ratio.rolling(window=window, min_periods=window).mean().iloc[-1]
        features[f"relative_price_above_{window}d_ma"] = (
            bool(ratio.iloc[-1] > moving_average)
            if _is_finite_number(moving_average)
            else None
        )
    relative_drawdown = _series_drawdown(ratio, window=60)
    features["relative_drawdown"] = relative_drawdown
    if returns_for_score:
        average_relative_return = float(np.mean(returns_for_score))
        normalized = 50.0 + (
            average_relative_return / policy.relative_return_full_scale
        ) * 50.0
        features["relative_momentum_score"] = _clamp_score(normalized)
    else:
        features["relative_momentum_score"] = None
    return features


def _relative_price_ratio(
    frame: pd.DataFrame,
    numerator: str,
    denominator: str,
) -> pd.Series | None:
    selected = frame.loc[frame["symbol"].isin({numerator, denominator})]
    if selected.empty:
        return None
    pivot = selected.pivot_table(
        index="_date",
        columns="symbol",
        values="_adj_close",
        aggfunc="last",
    ).sort_index()
    if numerator not in pivot.columns or denominator not in pivot.columns:
        return None
    ratio = (pivot[numerator] / pivot[denominator]).replace([np.inf, -np.inf], np.nan)
    return ratio.dropna()


def _series_return(series: pd.Series, *, window: int) -> float | None:
    if len(series) <= window:
        return None
    return _safe_float(series.iloc[-1] / series.iloc[-window - 1] - 1.0)


def _series_drawdown(series: pd.Series, *, window: int) -> float | None:
    if len(series) < window:
        return None
    recent = series.tail(window)
    high = recent.max()
    if not _is_finite_number(high) or high <= 0:
        return None
    return _safe_float(recent.iloc[-1] / high - 1.0)


def _relative_drawdown_component(
    pair_features: list[dict[str, Any]],
    policy: AISemiconductorRelativeStrengthPolicy,
) -> float:
    drawdowns = [
        float(pair["relative_drawdown"])
        for pair in pair_features
        if _is_finite_number(pair.get("relative_drawdown"))
    ]
    if not drawdowns:
        return 50.0
    penalties = []
    for drawdown in drawdowns:
        if drawdown >= 0:
            penalties.append(100.0)
            continue
        capped = min(abs(drawdown), abs(policy.relative_drawdown_full_penalty))
        penalties.append(100.0 * (1.0 - capped / abs(policy.relative_drawdown_full_penalty)))
    return _clamp_score(float(np.mean(penalties)))


def _normalize_event(event: Mapping[str, Any]) -> dict[str, Any]:
    event_date = pd.Timestamp(str(event["event_date"]))
    related_symbols = event.get("related_symbols") or []
    return {
        "event_id": str(event["event_id"]),
        "event_date": event_date,
        "event_type": str(event["event_type"]),
        "related_symbols": [str(symbol).strip().upper() for symbol in related_symbols],
        "severity": str(event.get("severity", "low")).strip().lower(),
        "lookback_window_days": int(event.get("lookback_window_days", 0)),
        "lookahead_window_days": int(event.get("lookahead_window_days", 0)),
        "source": str(event.get("source", "unknown")),
        "confidence": str(event.get("confidence", "unknown")),
        "optional": bool(event.get("optional", False)),
    }


def _event_risk_score(
    active_events: list[dict[str, Any]],
    policy: EventRiskOverlayPolicy,
) -> float:
    if not active_events:
        return 0.0
    base = max(
        policy.severity_scores.get(event["severity"], policy.severity_scores["low"])
        for event in active_events
    )
    increment = max(0, len(active_events) - 1) * policy.multiple_event_increment
    return round(min(policy.maximum_event_risk_score, base + increment), 2)


def _affected_event_groups(
    active_events: list[dict[str, Any]],
    universe_config: AIConfirmationUniverseConfig,
) -> list[str]:
    if not active_events:
        return []
    affected: set[str] = set()
    macro_types = {"FOMC", "CPI", "PCE", "major_macro_event", "export_control_window"}
    for event in active_events:
        related_symbols = set(event["related_symbols"])
        if event["event_type"] in macro_types or not related_symbols:
            affected.update(
                group_id
                for group_id, group in universe_config.ai_confirmation_universe.items()
                if group.enabled and group_id != AI_CONFIRMATION_EVENT_GROUP_ID
            )
            continue
        for group_id, group in universe_config.ai_confirmation_universe.items():
            if not group.enabled or group_id == AI_CONFIRMATION_EVENT_GROUP_ID:
                continue
            group_symbols = {symbol.ticker for symbol in enabled_symbols_for_group(
                universe_config, group_id
            )}
            if related_symbols & group_symbols:
                affected.add(group_id)
    return sorted(affected)


def _event_output(event: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "event_id": event["event_id"],
        "event_date": pd.Timestamp(event["event_date"]).date().isoformat(),
        "event_type": event["event_type"],
        "related_symbols": event["related_symbols"],
        "severity": event["severity"],
        "lookback_window_days": event["lookback_window_days"],
        "lookahead_window_days": event["lookahead_window_days"],
        "source": event["source"],
        "confidence": event["confidence"],
        "optional": event["optional"],
    }


def _semiconductor_breadth_score(breadth_records: list[dict[str, Any]]) -> float:
    try:
        record = _breadth_record_for_group(breadth_records, "semiconductor_hardware")
    except KeyError:
        return 50.0
    values = dict(record.get("feature_values") or {})
    fractions = [
        values.get("percent_above_20d_ma"),
        values.get("percent_above_50d_ma"),
        values.get("percent_above_100d_ma"),
        values.get("percent_above_200d_ma"),
        values.get("percent_positive_20d_return"),
        values.get("percent_positive_60d_return"),
    ]
    return _average_score(*fractions) or 50.0


def _composite_data_coverage(
    breadth_records: list[dict[str, Any]],
    mega_cap_score: Mapping[str, Any],
) -> float:
    coverages = [
        float(record["data_coverage_ratio"])
        for record in breadth_records
        if _is_finite_number(record.get("data_coverage_ratio"))
    ]
    if _is_finite_number(mega_cap_score.get("data_coverage_ratio")):
        coverages.append(float(mega_cap_score["data_coverage_ratio"]))
    if not coverages:
        return 0.0
    return _safe_float(np.mean(coverages)) or 0.0


def _composite_action_hint(
    score_value: float,
    data_coverage: float,
    event_risk_score: float,
    policy: AIConfirmationCompositePolicy,
) -> str:
    if data_coverage < policy.insufficient_data_coverage_min:
        return "insufficient_data"
    if event_risk_score >= policy.event_risk_high_min:
        return "warns_against_ai_overweight"
    if score_value >= policy.supports_overweight_min:
        return "supports_ai_overweight_candidate"
    if score_value >= policy.supports_neutral_min:
        return "supports_neutral_ai_exposure"
    return "warns_against_ai_overweight"


def _composite_reason_codes(
    component_scores: Mapping[str, float],
    data_coverage: float,
    event_risk_score: float,
    policy: AIConfirmationCompositePolicy,
) -> list[str]:
    reasons = [
        f"semiconductor_breadth={component_scores['semiconductor_breadth']:.2f}",
        f"mega_cap_ai={component_scores['mega_cap_ai']:.2f}",
        f"ai_relative_strength={component_scores['ai_relative_strength']:.2f}",
        f"event_risk_score={event_risk_score:.2f}",
        f"data_coverage={data_coverage:.2f}",
    ]
    if data_coverage < policy.insufficient_data_coverage_min:
        reasons.append("insufficient_data_coverage")
    if event_risk_score >= policy.event_risk_high_min:
        reasons.append("high_event_risk")
    return reasons
