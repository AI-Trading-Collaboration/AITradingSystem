from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime
from hashlib import sha256
from pathlib import Path
from typing import Any, Literal, Self

import numpy as np
import pandas as pd
from pydantic import BaseModel, Field, model_validator

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.etf_portfolio.features import build_feature_store
from ai_trading_system.etf_portfolio.models import (
    ETFAssetsConfig,
    ETFStrategyConfig,
    PolicyMetadata,
    load_etf_config_bundle,
)
from ai_trading_system.etf_portfolio.signals import (
    momentum_score_for_row,
    relative_strength_score_for_row,
    trend_score_for_row,
)
from ai_trading_system.reports.report_index import (
    DEFAULT_REPORT_REGISTRY_PATH,
    load_report_registry,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

DEFAULT_TREND_CALIBRATION_POLICY_CONFIG_PATH = (
    PROJECT_ROOT / "config" / "etf_portfolio" / "trend_calibration.yaml"
)
DEFAULT_TREND_CALIBRATION_REPORT_ROOT = (
    PROJECT_ROOT / "reports" / "etf_portfolio" / "trend_calibration"
)
DEFAULT_TREND_CALIBRATION_DATASET_DIR = DEFAULT_TREND_CALIBRATION_REPORT_ROOT / "datasets"
DEFAULT_TREND_CALIBRATION_REPORT_DIR = DEFAULT_TREND_CALIBRATION_REPORT_ROOT / "reports"
DEFAULT_TREND_CALIBRATION_REGISTRY_DIR = DEFAULT_TREND_CALIBRATION_REPORT_ROOT / "registry"
DEFAULT_TREND_CALIBRATION_VALIDATION_DIR = DEFAULT_TREND_CALIBRATION_REPORT_ROOT / "validation"

TREND_CALIBRATION_POLICY_SCHEMA_VERSION = "etf_trend_calibration_policy_v1"
TREND_SIGNAL_DATASET_SCHEMA_VERSION = "etf_trend_signal_dataset_v1"
TREND_SCORE_RUN_SCHEMA_VERSION = "etf_trend_score_run_v1"
TREND_WEIGHT_SEARCH_SCHEMA_VERSION = "etf_trend_signal_weight_search_v1"
TREND_CALIBRATION_REPORT_SCHEMA_VERSION = "etf_trend_calibration_report_v1"
TREND_CONFIG_REGISTRY_SCHEMA_VERSION = "etf_trend_signal_config_registry_v1"
TREND_CALIBRATION_VALIDATION_SCHEMA_VERSION = "etf_trend_calibration_validation_v1"

TREND_CALIBRATION_REPORT_REGISTRY_ID = "etf_trend_calibration_report"
TREND_CALIBRATION_VALIDATION_REGISTRY_ID = "etf_trend_calibration_validation"

TREND_CALIBRATION_SAFETY = {
    "observe_only": True,
    "candidate_only": True,
    "production_effect": "none",
    "broker_action": "none",
    "manual_review_required": True,
    "evaluation_only": True,
}
FORBIDDEN_OUTPUT_KEYS = {
    "target_weights",
    "production_weights",
    "production_weight_update",
    "baseline_config_mutation",
    "broker_order",
    "automatic_candidate_promotion",
    "auto_promotion",
}
DEFAULT_MARKET_REGIME = "ai_after_chatgpt"
DEFAULT_REGIME_START = date(2022, 12, 1)
PRICE_TARGETS = ("QQQ", "SPY", "SMH", "SOXX")


class TrendCalibrationError(ValueError):
    """Raised when trend calibration inputs or outputs are unsafe."""


class TrendCalibrationSafety(BaseModel):
    observe_only: Literal[True]
    candidate_only: Literal[True]
    production_effect: Literal["none"]
    broker_action: Literal["none"]
    manual_review_required: Literal[True]
    evaluation_only: Literal[True]


class TrendCalibrationMarketRegime(BaseModel):
    regime_id: Literal["ai_after_chatgpt"]
    anchor_event: str = Field(min_length=1)
    anchor_date: date
    default_evaluation_start: date

    @model_validator(mode="after")
    def validate_ai_regime_start(self) -> Self:
        if self.default_evaluation_start < DEFAULT_REGIME_START:
            raise ValueError("default trend calibration start cannot predate 2022-12-01")
        return self


class TrendSignalWeightBounds(BaseModel):
    min_weight: float = Field(ge=0, le=1)
    max_weight: float = Field(ge=0, le=1)

    @model_validator(mode="after")
    def validate_order(self) -> Self:
        if self.min_weight > self.max_weight:
            raise ValueError("signal min_weight cannot exceed max_weight")
        return self


class TrendSignalInputConfig(BaseModel):
    signal_id: str = Field(min_length=1)
    source_module: str = Field(min_length=1)
    required: bool
    direction: Literal["positive", "negative"]
    normalization_method: str = Field(min_length=1)
    weight_bounds: TrendSignalWeightBounds
    data_quality_requirement: str = Field(min_length=1)


class TrendScoreBandConfig(BaseModel):
    band_id: str = Field(min_length=1)
    min_score: float = Field(ge=0, le=100)
    max_score: float = Field(ge=0, le=100)

    @model_validator(mode="after")
    def validate_band(self) -> Self:
        if self.min_score >= self.max_score:
            raise ValueError("score band min_score must be below max_score")
        return self


class TrendForwardAttributionPolicy(BaseModel):
    forward_windows: list[int] = Field(min_length=1)
    primary_window: int = Field(gt=0)
    bucket_count_min: int = Field(gt=0)
    minimum_sample_count: int = Field(gt=0)

    @model_validator(mode="after")
    def validate_primary_window(self) -> Self:
        self.forward_windows = sorted({int(window) for window in self.forward_windows})
        if self.primary_window not in self.forward_windows:
            raise ValueError("primary_window must be included in forward_windows")
        return self


class TrendSearchRankingPolicy(BaseModel):
    forward_lift_full_score: float = Field(gt=0)
    drawdown_improvement_full_score: float = Field(gt=0)
    coverage_full_score_multiplier: float = Field(gt=0)
    high_redundancy_penalty: float = Field(ge=0, le=1)
    return_lift_weight: float = Field(ge=0, le=1)
    drawdown_control_weight: float = Field(ge=0, le=1)
    coverage_weight: float = Field(ge=0, le=1)
    regime_stability_weight: float = Field(ge=0, le=1)

    @model_validator(mode="after")
    def validate_weight_sum(self) -> Self:
        total = (
            self.return_lift_weight
            + self.drawdown_control_weight
            + self.coverage_weight
            + self.regime_stability_weight
        )
        if abs(total - 1.0) > 1e-6:
            raise ValueError("trend search ranking weights must sum to 1.0")
        return self


class TrendSignalWeightSetConfig(BaseModel):
    weight_set_id: str = Field(min_length=1)
    status: str = Field(min_length=1)
    rationale: str = Field(min_length=1)
    weights: dict[str, float] = Field(min_length=1)

    @model_validator(mode="after")
    def validate_weights(self) -> Self:
        if any(weight < 0 or weight > 1 for weight in self.weights.values()):
            raise ValueError(f"trend weight set has out-of-range weights: {self.weight_set_id}")
        total = sum(float(value) for value in self.weights.values())
        if abs(total - 1.0) > 1e-6:
            raise ValueError(f"trend weight set weights must sum to 1.0: {self.weight_set_id}")
        return self


class TrendSearchPolicy(BaseModel):
    search_id: str = Field(min_length=1)
    top_n: int = Field(gt=0)
    ranking_policy: TrendSearchRankingPolicy
    preset_weight_sets: list[TrendSignalWeightSetConfig] = Field(min_length=1)


class TrendRedundancyPolicy(BaseModel):
    medium_correlation: float = Field(ge=0, le=1)
    high_correlation: float = Field(ge=0, le=1)
    rank_correlation_warning: float = Field(ge=0, le=1)

    @model_validator(mode="after")
    def validate_order(self) -> Self:
        if self.medium_correlation > self.high_correlation:
            raise ValueError("medium correlation threshold cannot exceed high threshold")
        return self


class TrendRegimeStabilityPolicy(BaseModel):
    minimum_regime_sample_count: int = Field(gt=0)
    required_regimes: list[str] = Field(min_length=1)


class TrendCalibrationPolicyConfig(BaseModel):
    schema_version: Literal["etf_trend_calibration_policy_v1"]
    policy_metadata: PolicyMetadata
    market_regime: TrendCalibrationMarketRegime
    safety: TrendCalibrationSafety
    signal_registry: list[TrendSignalInputConfig] = Field(min_length=1)
    score_bands: list[TrendScoreBandConfig] = Field(min_length=1)
    forward_attribution: TrendForwardAttributionPolicy
    search: TrendSearchPolicy
    redundancy: TrendRedundancyPolicy
    regime_stability: TrendRegimeStabilityPolicy

    @model_validator(mode="after")
    def validate_policy(self) -> Self:
        if self.safety.model_dump(mode="json") != TREND_CALIBRATION_SAFETY:
            raise ValueError("trend calibration safety fields are unsafe")
        signal_ids = [signal.signal_id for signal in self.signal_registry]
        if len(signal_ids) != len(set(signal_ids)):
            raise ValueError("trend signal ids must be unique")
        required = {
            "price_trend",
            "momentum",
            "relative_strength",
            "volatility_risk",
            "drawdown_risk",
            "AIConfirmationScore",
            "SemiconductorBreadthScore",
            "MegaCapAIScore",
            "EventRiskScore",
        }
        missing = sorted(required - set(signal_ids))
        if missing:
            raise ValueError("trend signal registry missing required ids: " + ", ".join(missing))
        for weight_set in self.search.preset_weight_sets:
            extra = sorted(set(weight_set.weights) - set(signal_ids))
            missing_weights = sorted(set(signal_ids) - set(weight_set.weights))
            if extra or missing_weights:
                raise ValueError(
                    f"trend weight set {weight_set.weight_set_id} mismatch: "
                    f"extra={extra}; missing={missing_weights}"
                )
            for signal in self.signal_registry:
                weight = float(weight_set.weights[signal.signal_id])
                bounds = signal.weight_bounds
                if weight < bounds.min_weight or weight > bounds.max_weight:
                    raise ValueError(
                        f"trend weight set {weight_set.weight_set_id} puts "
                        f"{signal.signal_id} outside configured bounds"
                    )
        return self

    @property
    def signal_map(self) -> dict[str, TrendSignalInputConfig]:
        return {signal.signal_id: signal for signal in self.signal_registry}

    @property
    def config_hash(self) -> str:
        return _stable_hash(self.model_dump(mode="json"))


def load_trend_calibration_policy_config(
    path: Path | str = DEFAULT_TREND_CALIBRATION_POLICY_CONFIG_PATH,
) -> TrendCalibrationPolicyConfig:
    raw = safe_load_yaml_path(Path(path))
    if not isinstance(raw, Mapping):
        raise TrendCalibrationError("trend calibration policy must be a mapping")
    try:
        return TrendCalibrationPolicyConfig.model_validate(raw)
    except Exception as exc:
        raise TrendCalibrationError(str(exc)) from exc


def build_trend_signal_dataset(
    *,
    features: pd.DataFrame,
    prices: pd.DataFrame,
    strategy: ETFStrategyConfig,
    policy: TrendCalibrationPolicyConfig | None = None,
    start: date | None = None,
    end: date | None = None,
    data_quality_status: str = "UNKNOWN",
    data_quality_report: str = "",
    price_source_path: str = "",
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    resolved_policy = policy or load_trend_calibration_policy_config()
    generated = _coerce_datetime(generated_at or datetime.now(UTC))
    run_start = start or resolved_policy.market_regime.default_evaluation_start
    if run_start < resolved_policy.market_regime.default_evaluation_start:
        raise TrendCalibrationError("trend calibration primary start cannot predate AI regime")
    feature_frame = _prepare_feature_frame(features, start=run_start, end=end)
    price_pivot = _price_pivot(prices, end=end)
    records: list[dict[str, Any]] = []
    windows = tuple(resolved_policy.forward_attribution.forward_windows)

    for signal_date, group in feature_frame.groupby("_date", sort=True):
        score_date = pd.Timestamp(signal_date).date()
        values = _date_signal_values(group, strategy)
        forward = _forward_evaluation_fields(
            price_pivot,
            score_date=score_date,
            windows=windows,
        )
        record = {
            "date": score_date.isoformat(),
            "asset_context": "market",
            "market_regime": resolved_policy.market_regime.regime_id,
            "regime_label": _regime_label(values),
            "raw_signal_values": values,
            "normalized_signal_values": {
                signal.signal_id: _clamp_score(values.get(signal.signal_id, 50.0))
                for signal in resolved_policy.signal_registry
            },
            "signal_availability": _signal_availability(values, resolved_policy),
            "forward_return_windows": forward["forward_return_windows"],
            "forward_drawdown_windows": forward["forward_drawdown_windows"],
            "forward_volatility_windows": forward["forward_volatility_windows"],
            "sample_available": forward["sample_available"],
            "evaluation_only": True,
            "data_quality_status": data_quality_status,
            "safety": dict(TREND_CALIBRATION_SAFETY),
            **TREND_CALIBRATION_SAFETY,
        }
        records.append(record)

    payload = {
        "schema_version": TREND_SIGNAL_DATASET_SCHEMA_VERSION,
        "report_type": "etf_trend_signal_dataset",
        "dataset_id": _stable_id(
            "trend-signal-dataset",
            run_start.isoformat(),
            "" if end is None else end.isoformat(),
            len(records),
            resolved_policy.config_hash,
        ),
        "generated_at": generated.isoformat(),
        "market_regime": resolved_policy.market_regime.regime_id,
        "requested_date_range": {
            "start": run_start.isoformat(),
            "end": "" if end is None else end.isoformat(),
        },
        "record_count": len(records),
        "available_forward_sample_count": len(
            [record for record in records if record["sample_available"] is True]
        ),
        "signal_registry": [
            signal.model_dump(mode="json") for signal in resolved_policy.signal_registry
        ],
        "forward_windows": [f"{window}D" for window in windows],
        "data_quality": {
            "status": data_quality_status,
            "report_path": data_quality_report,
        },
        "source_links": {
            "price_source_path": price_source_path,
            "policy_config": str(DEFAULT_TREND_CALIBRATION_POLICY_CONFIG_PATH),
        },
        "records": records,
        "evaluation_only": True,
        "commands_executed": False,
        "production_state_mutated": False,
        "safety": dict(TREND_CALIBRATION_SAFETY),
        **TREND_CALIBRATION_SAFETY,
    }
    _assert_safe_output(payload)
    return payload


def compute_trend_scores(
    dataset: Mapping[str, Any],
    *,
    weight_set: Mapping[str, Any],
    policy: TrendCalibrationPolicyConfig | None = None,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    resolved_policy = policy or load_trend_calibration_policy_config()
    generated = _coerce_datetime(generated_at or datetime.now(UTC))
    weight_record = _weight_set_record(weight_set)
    weights = _mapping(weight_record.get("weights"))
    rows: list[dict[str, Any]] = []
    for record in _records(dataset.get("records")):
        normalized = _mapping(record.get("normalized_signal_values"))
        contributions: dict[str, float] = {}
        composite = 0.0
        for signal in resolved_policy.signal_registry:
            raw_value = _clamp_score(normalized.get(signal.signal_id, 50.0))
            directional_value = 100.0 - raw_value if signal.direction == "negative" else raw_value
            contribution = float(weights.get(signal.signal_id, 0.0)) * directional_value
            contributions[signal.signal_id] = round(contribution, 6)
            composite += contribution
        composite = _clamp_score(composite)
        event_risk = _clamp_score(normalized.get("EventRiskScore", 50.0))
        row = {
            "date": _text(record.get("date")),
            "weight_set_id": _text(weight_record.get("weight_set_id")),
            "CompositeTrendScore": round(composite, 6),
            "TrendScoreBand": _score_band(composite, resolved_policy),
            "RiskRegimeScore": round(
                _mean(
                    [
                        _clamp_score(normalized.get("volatility_risk", 50.0)),
                        _clamp_score(normalized.get("drawdown_risk", 50.0)),
                    ]
                ),
                6,
            ),
            "GrowthLeadershipScore": round(
                _clamp_score(normalized.get("relative_strength", 50.0)),
                6,
            ),
            "SemiconductorLeadershipScore": round(
                _clamp_score(normalized.get("SemiconductorBreadthScore", 50.0)),
                6,
            ),
            "AIConfirmationAdjustedTrendScore": round(
                _clamp_score(
                    composite
                    + (float(weights.get("AIConfirmationScore", 0.0)) * 0.5)
                    * (_clamp_score(normalized.get("AIConfirmationScore", 50.0)) - 50.0)
                ),
                6,
            ),
            "RiskOffProbabilityScore": round(
                _clamp_score(100.0 - _mean([composite, normalized.get("volatility_risk", 50.0)])),
                6,
            ),
            "EventRiskAdjustedTrendScore": round(
                _clamp_score(
                    composite - (event_risk - 50.0) * float(weights.get("EventRiskScore", 0.0))
                ),
                6,
            ),
            "signal_contributions": contributions,
            "regime_label": _text(record.get("regime_label"), "unknown"),
            "evaluation_only": True,
            "safety": dict(TREND_CALIBRATION_SAFETY),
            **TREND_CALIBRATION_SAFETY,
        }
        rows.append(row)
    payload = {
        "schema_version": TREND_SCORE_RUN_SCHEMA_VERSION,
        "report_type": "etf_trend_score_run",
        "score_run_id": _stable_id(
            "trend-score-run",
            _text(dataset.get("dataset_id")),
            _text(weight_record.get("weight_set_id")),
        ),
        "generated_at": generated.isoformat(),
        "dataset_id": _text(dataset.get("dataset_id")),
        "weight_set": weight_record,
        "record_count": len(rows),
        "scores": rows,
        "evaluation_only": True,
        "commands_executed": False,
        "production_state_mutated": False,
        "safety": dict(TREND_CALIBRATION_SAFETY),
        **TREND_CALIBRATION_SAFETY,
    }
    _assert_safe_output(payload)
    return payload


def build_trend_score_bucket_forward_attribution(
    dataset: Mapping[str, Any],
    score_run: Mapping[str, Any],
    *,
    policy: TrendCalibrationPolicyConfig | None = None,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    resolved_policy = policy or load_trend_calibration_policy_config()
    generated = _coerce_datetime(generated_at or datetime.now(UTC))
    records_by_date = {
        _text(record.get("date")): record for record in _records(dataset.get("records"))
    }
    primary = resolved_policy.forward_attribution.primary_window
    grouped: dict[str, list[dict[str, Any]]] = {}
    for score in _records(score_run.get("scores")):
        source = records_by_date.get(_text(score.get("date")), {})
        if source.get("sample_available") is not True:
            continue
        bucket = _text(score.get("TrendScoreBand"), "unknown")
        row = {
            "date": _text(score.get("date")),
            "CompositeTrendScore": _float(score.get("CompositeTrendScore")),
            "regime_label": _text(score.get("regime_label"), "unknown"),
            "QQQ_forward_return": _float_or_none(
                _mapping(source.get("forward_return_windows")).get(f"QQQ_{primary}D")
            ),
            "SPY_forward_return": _float_or_none(
                _mapping(source.get("forward_return_windows")).get(f"SPY_{primary}D")
            ),
            "SMH_forward_return": _float_or_none(
                _mapping(source.get("forward_return_windows")).get(f"SMH_{primary}D")
            ),
            "SOXX_forward_return": _float_or_none(
                _mapping(source.get("forward_return_windows")).get(f"SOXX_{primary}D")
            ),
            "QQQ_SPY_relative_return": _float_or_none(
                _mapping(source.get("forward_return_windows")).get(f"QQQ_SPY_relative_{primary}D")
            ),
            "SMH_QQQ_relative_return": _float_or_none(
                _mapping(source.get("forward_return_windows")).get(f"SMH_QQQ_relative_{primary}D")
            ),
            "QQQ_forward_drawdown": _float_or_none(
                _mapping(source.get("forward_drawdown_windows")).get(f"QQQ_{primary}D")
            ),
            "QQQ_forward_volatility": _float_or_none(
                _mapping(source.get("forward_volatility_windows")).get(f"QQQ_{primary}D")
            ),
        }
        grouped.setdefault(bucket, []).append(row)
    bucket_rows = []
    for band in resolved_policy.score_bands:
        rows = grouped.get(band.band_id, [])
        bucket_rows.append(
            {
                "bucket": band.band_id,
                "sample_count": len(rows),
                "mean_score": _mean([row.get("CompositeTrendScore") for row in rows]),
                "QQQ_forward_return_mean": _mean([row.get("QQQ_forward_return") for row in rows]),
                "SMH_forward_return_mean": _mean([row.get("SMH_forward_return") for row in rows]),
                "QQQ_SPY_relative_return_mean": _mean(
                    [row.get("QQQ_SPY_relative_return") for row in rows]
                ),
                "SMH_QQQ_relative_return_mean": _mean(
                    [row.get("SMH_QQQ_relative_return") for row in rows]
                ),
                "QQQ_forward_drawdown_mean": _mean(
                    [row.get("QQQ_forward_drawdown") for row in rows]
                ),
                "QQQ_forward_volatility_mean": _mean(
                    [row.get("QQQ_forward_volatility") for row in rows]
                ),
            }
        )
    payload = {
        "schema_version": "etf_trend_score_bucket_forward_attribution_v1",
        "report_type": "etf_trend_score_bucket_forward_attribution",
        "generated_at": generated.isoformat(),
        "score_run_id": _text(score_run.get("score_run_id")),
        "weight_set_id": _text(_mapping(score_run.get("weight_set")).get("weight_set_id")),
        "primary_forward_window": f"{primary}D",
        "bucket_rows": bucket_rows,
        "usable_bucket_count": len(
            [
                row
                for row in bucket_rows
                if _int(row.get("sample_count"))
                >= resolved_policy.forward_attribution.bucket_count_min
            ]
        ),
        "evaluation_only": True,
        "safety": dict(TREND_CALIBRATION_SAFETY),
        **TREND_CALIBRATION_SAFETY,
    }
    _assert_safe_output(payload)
    return payload


def build_trend_signal_redundancy_diagnostics(
    dataset: Mapping[str, Any],
    *,
    policy: TrendCalibrationPolicyConfig | None = None,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    resolved_policy = policy or load_trend_calibration_policy_config()
    generated = _coerce_datetime(generated_at or datetime.now(UTC))
    rows = [
        {
            "date": _text(record.get("date")),
            **{
                signal.signal_id: _float(
                    _mapping(record.get("normalized_signal_values")).get(signal.signal_id),
                    default=np.nan,
                )
                for signal in resolved_policy.signal_registry
            },
        }
        for record in _records(dataset.get("records"))
    ]
    frame = pd.DataFrame(rows)
    signal_ids = [signal.signal_id for signal in resolved_policy.signal_registry]
    if frame.empty:
        corr = pd.DataFrame(index=signal_ids, columns=signal_ids, dtype=float)
        rank_corr = corr.copy()
    else:
        corr = frame[signal_ids].corr(method="pearson")
        rank_corr = frame[signal_ids].corr(method="spearman")
    pairs: list[dict[str, Any]] = []
    for left_index, left in enumerate(signal_ids):
        for right in signal_ids[left_index + 1 :]:
            correlation = _float_or_none(corr.loc[left, right]) if left in corr.index else None
            rank_correlation = (
                _float_or_none(rank_corr.loc[left, right]) if left in rank_corr.index else None
            )
            severity = "none"
            if (
                correlation is not None
                and abs(correlation) >= resolved_policy.redundancy.high_correlation
            ):
                severity = "high_redundancy_warning"
            elif (
                correlation is not None
                and abs(correlation) >= resolved_policy.redundancy.medium_correlation
            ) or (
                rank_correlation is not None
                and abs(rank_correlation) >= resolved_policy.redundancy.rank_correlation_warning
            ):
                severity = "medium_redundancy_warning"
            pairs.append(
                {
                    "signal_a": left,
                    "signal_b": right,
                    "correlation": None if correlation is None else round(correlation, 6),
                    "rank_correlation": (
                        None if rank_correlation is None else round(rank_correlation, 6)
                    ),
                    "severity": severity,
                    "evaluation_only": True,
                }
            )
    payload = {
        "schema_version": "etf_trend_signal_redundancy_diagnostics_v1",
        "report_type": "etf_trend_signal_redundancy_diagnostics",
        "generated_at": generated.isoformat(),
        "dataset_id": _text(dataset.get("dataset_id")),
        "pair_count": len(pairs),
        "high_redundancy_count": len(
            [row for row in pairs if row["severity"] == "high_redundancy_warning"]
        ),
        "medium_redundancy_count": len(
            [row for row in pairs if row["severity"] == "medium_redundancy_warning"]
        ),
        "pairs": pairs,
        "evaluation_only": True,
        "safety": dict(TREND_CALIBRATION_SAFETY),
        **TREND_CALIBRATION_SAFETY,
    }
    _assert_safe_output(payload)
    return payload


def build_trend_signal_regime_stability_review(
    dataset: Mapping[str, Any],
    score_run: Mapping[str, Any],
    *,
    policy: TrendCalibrationPolicyConfig | None = None,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    resolved_policy = policy or load_trend_calibration_policy_config()
    generated = _coerce_datetime(generated_at or datetime.now(UTC))
    records_by_date = {
        _text(record.get("date")): record for record in _records(dataset.get("records"))
    }
    primary = resolved_policy.forward_attribution.primary_window
    grouped: dict[str, list[dict[str, Any]]] = {}
    for score in _records(score_run.get("scores")):
        source = records_by_date.get(_text(score.get("date")), {})
        regime = _text(source.get("regime_label"), _text(score.get("regime_label"), "unknown"))
        grouped.setdefault(regime, []).append(
            {
                "CompositeTrendScore": _float(score.get("CompositeTrendScore")),
                "QQQ_forward_return": _float_or_none(
                    _mapping(source.get("forward_return_windows")).get(f"QQQ_{primary}D")
                ),
                "QQQ_forward_drawdown": _float_or_none(
                    _mapping(source.get("forward_drawdown_windows")).get(f"QQQ_{primary}D")
                ),
            }
        )
    rows: list[dict[str, Any]] = []
    for regime in sorted(set(resolved_policy.regime_stability.required_regimes) | set(grouped)):
        values = grouped.get(regime, [])
        sample_count = len(values)
        rows.append(
            {
                "regime_label": regime,
                "sample_count": sample_count,
                "coverage_status": (
                    "usable"
                    if sample_count >= resolved_policy.regime_stability.minimum_regime_sample_count
                    else "limited"
                ),
                "mean_score": _mean([row.get("CompositeTrendScore") for row in values]),
                "QQQ_forward_return_mean": _mean([row.get("QQQ_forward_return") for row in values]),
                "QQQ_forward_drawdown_mean": _mean(
                    [row.get("QQQ_forward_drawdown") for row in values]
                ),
                "evaluation_only": True,
            }
        )
    payload = {
        "schema_version": "etf_trend_signal_regime_stability_v1",
        "report_type": "etf_trend_signal_regime_stability",
        "generated_at": generated.isoformat(),
        "score_run_id": _text(score_run.get("score_run_id")),
        "weight_set_id": _text(_mapping(score_run.get("weight_set")).get("weight_set_id")),
        "regime_rows": rows,
        "limited_regime_count": len([row for row in rows if row["coverage_status"] == "limited"]),
        "evaluation_only": True,
        "safety": dict(TREND_CALIBRATION_SAFETY),
        **TREND_CALIBRATION_SAFETY,
    }
    _assert_safe_output(payload)
    return payload


def run_trend_signal_weight_search(
    dataset: Mapping[str, Any],
    *,
    policy: TrendCalibrationPolicyConfig | None = None,
    top: int | None = None,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    resolved_policy = policy or load_trend_calibration_policy_config()
    generated = _coerce_datetime(generated_at or datetime.now(UTC))
    redundancy = build_trend_signal_redundancy_diagnostics(
        dataset,
        policy=resolved_policy,
        generated_at=generated,
    )
    candidate_rows: list[dict[str, Any]] = []
    for weight_set in resolved_policy.search.preset_weight_sets:
        score_run = compute_trend_scores(
            dataset,
            weight_set=weight_set.model_dump(mode="json"),
            policy=resolved_policy,
            generated_at=generated,
        )
        attribution = build_trend_score_bucket_forward_attribution(
            dataset,
            score_run,
            policy=resolved_policy,
            generated_at=generated,
        )
        regime = build_trend_signal_regime_stability_review(
            dataset,
            score_run,
            policy=resolved_policy,
            generated_at=generated,
        )
        quality = _trend_config_quality_summary(
            attribution=attribution,
            regime_stability=regime,
            redundancy=redundancy,
            policy=resolved_policy,
        )
        candidate_rows.append(
            {
                "rank": 0,
                "trend_signal_config_id": weight_set.weight_set_id,
                "weight_set": weight_set.model_dump(mode="json"),
                "quality_summary": quality,
                "score_summary": _score_summary(score_run),
                "bucket_attribution": attribution,
                "regime_stability": regime,
                "redundancy_summary": _redundancy_summary(redundancy),
                "status": _candidate_status(quality, resolved_policy),
                "evaluation_only": True,
                "safety": dict(TREND_CALIBRATION_SAFETY),
                **TREND_CALIBRATION_SAFETY,
            }
        )
    candidate_rows.sort(
        key=lambda row: (
            -_float(_mapping(row.get("quality_summary")).get("trend_signal_quality_score")),
            _text(row.get("trend_signal_config_id")),
        )
    )
    for index, row in enumerate(candidate_rows, start=1):
        row["rank"] = index
    selected_top = top or resolved_policy.search.top_n
    payload = {
        "schema_version": TREND_WEIGHT_SEARCH_SCHEMA_VERSION,
        "report_type": "etf_trend_signal_weight_search",
        "search_id": resolved_policy.search.search_id,
        "generated_at": generated.isoformat(),
        "dataset_id": _text(dataset.get("dataset_id")),
        "candidate_count": len(candidate_rows),
        "top_n": selected_top,
        "ranked_configs": candidate_rows,
        "top_configs": candidate_rows[:selected_top],
        "redundancy_diagnostics": redundancy,
        "evaluation_only": True,
        "commands_executed": False,
        "production_state_mutated": False,
        "safety": dict(TREND_CALIBRATION_SAFETY),
        **TREND_CALIBRATION_SAFETY,
    }
    _assert_safe_output(payload)
    return payload


def build_trend_signal_config_registry(
    search: Mapping[str, Any],
    *,
    policy: TrendCalibrationPolicyConfig | None = None,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    resolved_policy = policy or load_trend_calibration_policy_config()
    generated = _coerce_datetime(generated_at or datetime.now(UTC))
    configs = []
    for row in _records(search.get("top_configs")):
        configs.append(
            {
                "trend_signal_config_id": _text(row.get("trend_signal_config_id")),
                "weights": _mapping(_mapping(row.get("weight_set")).get("weights")),
                "score_summary": _mapping(row.get("score_summary")),
                "attribution_summary": _mapping(row.get("quality_summary")),
                "redundancy_summary": _mapping(row.get("redundancy_summary")),
                "regime_stability": {
                    "limited_regime_count": _int(
                        _mapping(row.get("regime_stability")).get("limited_regime_count")
                    )
                },
                "status": _text(row.get("status"), "candidate_review"),
                "production_effect": "none",
            }
        )
    payload = {
        "schema_version": TREND_CONFIG_REGISTRY_SCHEMA_VERSION,
        "report_type": "etf_trend_signal_config_registry",
        "generated_at": generated.isoformat(),
        "policy_version": resolved_policy.policy_metadata.version,
        "config_count": len(configs),
        "configs": configs,
        "evaluation_only": True,
        "commands_executed": False,
        "production_state_mutated": False,
        "safety": dict(TREND_CALIBRATION_SAFETY),
        **TREND_CALIBRATION_SAFETY,
    }
    _assert_safe_output(payload)
    return payload


def build_trend_calibration_report(
    *,
    dataset: Mapping[str, Any],
    policy: TrendCalibrationPolicyConfig | None = None,
    top: int | None = None,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    resolved_policy = policy or load_trend_calibration_policy_config()
    generated = _coerce_datetime(generated_at or datetime.now(UTC))
    search = run_trend_signal_weight_search(
        dataset,
        policy=resolved_policy,
        top=top,
        generated_at=generated,
    )
    registry = build_trend_signal_config_registry(
        search,
        policy=resolved_policy,
        generated_at=generated,
    )
    top_configs = _records(search.get("top_configs"))
    top_config = top_configs[0] if top_configs else {}
    quality = _mapping(top_config.get("quality_summary"))
    redundancy = _mapping(top_config.get("redundancy_summary"))
    report_status = (
        "PASS"
        if _records(search.get("top_configs"))
        and _int(dataset.get("available_forward_sample_count"))
        >= resolved_policy.forward_attribution.minimum_sample_count
        else "LIMITED"
    )
    payload = {
        "schema_version": TREND_CALIBRATION_REPORT_SCHEMA_VERSION,
        "report_type": "etf_trend_calibration_report",
        "trend_calibration_report_id": _stable_id(
            "trend-calibration-report",
            _text(dataset.get("dataset_id")),
            generated.isoformat(),
        ),
        "generated_at": generated.isoformat(),
        "status": report_status,
        "market_regime": resolved_policy.market_regime.regime_id,
        "requested_date_range": _mapping(dataset.get("requested_date_range")),
        "policy_version": resolved_policy.policy_metadata.version,
        "policy_config_hash": resolved_policy.config_hash,
        "dataset_coverage": {
            "dataset_id": _text(dataset.get("dataset_id")),
            "record_count": _int(dataset.get("record_count")),
            "available_forward_sample_count": _int(dataset.get("available_forward_sample_count")),
            "data_quality_status": _text(_mapping(dataset.get("data_quality")).get("status")),
        },
        "top_trend_signal_configs": _records(search.get("top_configs")),
        "trend_signal_config_registry": registry,
        "score_bucket_attribution": _mapping(top_config.get("bucket_attribution")),
        "redundancy_diagnostics": _mapping(search.get("redundancy_diagnostics")),
        "regime_stability": _mapping(top_config.get("regime_stability")),
        "recommended_configs_for_allocation_testing": [
            {
                "trend_signal_config_id": _text(row.get("trend_signal_config_id")),
                "status": _text(row.get("status")),
                "trend_signal_quality_score": _float(
                    _mapping(row.get("quality_summary")).get("trend_signal_quality_score")
                ),
                "allocation_usage": "candidate_only_input_for_TRADING_084",
            }
            for row in _records(search.get("top_configs"))
            if _text(row.get("status")) in {"candidate_review", "review_recommended"}
        ],
        "summary": {
            "top_config": _text(top_config.get("trend_signal_config_id"), "MISSING"),
            "top_quality_score": _float(quality.get("trend_signal_quality_score")),
            "evidence_status": report_status,
            "redundancy_risk": _text(redundancy.get("risk_level"), "unknown"),
            "regime_stability": _text(quality.get("regime_stability_status"), "unknown"),
            "data_quality_status": _text(_mapping(dataset.get("data_quality")).get("status")),
        },
        "source_links": _mapping(dataset.get("source_links")),
        "evaluation_only": True,
        "commands_executed": False,
        "production_state_mutated": False,
        "safety": dict(TREND_CALIBRATION_SAFETY),
        **TREND_CALIBRATION_SAFETY,
    }
    _assert_safe_output(payload)
    return payload


def build_trend_calibration_validation_report(
    *,
    config_path: Path = DEFAULT_TREND_CALIBRATION_POLICY_CONFIG_PATH,
    report_registry_path: Path = DEFAULT_REPORT_REGISTRY_PATH,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = _coerce_datetime(generated_at or datetime.now(UTC))
    checks: list[dict[str, Any]] = []
    try:
        policy = load_trend_calibration_policy_config(config_path)
        _append_check(checks, "policy_config_valid", True, "trend calibration policy loads")
    except Exception as exc:
        policy = None
        _append_check(checks, "policy_config_valid", False, str(exc))
    if policy is not None:
        try:
            prices, assets, strategy = _sample_prices_assets_strategy()
            features = build_feature_store(prices, assets=assets, strategy=strategy)
            dataset = build_trend_signal_dataset(
                features=features,
                prices=prices,
                strategy=strategy,
                policy=policy,
                start=date(2023, 1, 3),
                end=date(2024, 3, 29),
                data_quality_status="PASS",
                data_quality_report="validation_probe",
                generated_at=generated,
            )
            search = run_trend_signal_weight_search(dataset, policy=policy, generated_at=generated)
            report = build_trend_calibration_report(
                dataset=dataset,
                policy=policy,
                generated_at=generated,
            )
            registry = build_trend_signal_config_registry(
                search,
                policy=policy,
                generated_at=generated,
            )
            ranked_configs = _records(search.get("ranked_configs"))
            first_ranked = ranked_configs[0] if ranked_configs else {}
            probe = {
                "signal_registry_available": bool(policy.signal_registry),
                "dataset_builder_available": _int(dataset.get("record_count")) > 0,
                "weight_config_schema_available": bool(policy.search.preset_weight_sets),
                "score_engine_available": _int(
                    _mapping(first_ranked.get("score_summary")).get("score_count")
                )
                > 0,
                "search_runner_available": bool(_records(search.get("ranked_configs"))),
                "bucket_attribution_available": (
                    bool(
                        _records(
                            _mapping(first_ranked.get("bucket_attribution")).get("bucket_rows")
                        )
                    )
                    if first_ranked
                    else False
                ),
                "redundancy_diagnostics_available": bool(
                    _records(_mapping(search.get("redundancy_diagnostics")).get("pairs"))
                ),
                "regime_stability_available": bool(
                    _records(_mapping(report.get("regime_stability")).get("regime_rows"))
                ),
                "config_registry_available": _int(registry.get("config_count")) > 0,
                "report_available": _text(report.get("report_type"))
                == "etf_trend_calibration_report",
            }
            for check_id, passed in probe.items():
                _append_check(checks, check_id, passed, _validation_summary(check_id, passed))
            _append_check(
                checks,
                "evaluation_only_forward_fields",
                all(
                    record.get("evaluation_only") is True
                    and bool(_mapping(record.get("forward_return_windows")))
                    for record in _records(dataset.get("records"))
                ),
                "forward fields are present and evaluation-only",
            )
        except Exception as exc:
            _append_check(checks, "workflow_probe", False, str(exc))
    _append_check(
        checks,
        "report_registry_integration_available",
        _registry_has_trend_calibration(report_registry_path),
        "report registry includes trend calibration report and validation",
    )
    safety_payload = {
        "safety": dict(TREND_CALIBRATION_SAFETY),
        "commands_executed": False,
        "production_state_mutated": False,
        **TREND_CALIBRATION_SAFETY,
    }
    try:
        _assert_safe_output(safety_payload)
        safety_ok = True
    except TrendCalibrationError:
        safety_ok = False
    _append_check(
        checks,
        "safety_boundary_preserved",
        safety_ok,
        "trend calibration workflow preserves observe-only candidate-only safety",
    )
    failed = [check for check in checks if check["status"] == "FAIL"]
    payload = {
        "schema_version": TREND_CALIBRATION_VALIDATION_SCHEMA_VERSION,
        "report_type": "etf_trend_calibration_validation",
        "validation_id": _stable_id("trend-calibration-validation", generated.date().isoformat()),
        "generated_at": generated.isoformat(),
        "status": "FAIL" if failed else "PASS",
        "failed_check_count": len(failed),
        "checks": checks,
        "policy_config_path": str(config_path),
        "report_registry_path": str(report_registry_path),
        "production_weight_update_blocked": True,
        "broker_order_blocked": True,
        "target_weight_output_blocked": True,
        "automatic_candidate_promotion_blocked": True,
        "evaluation_only": True,
        "commands_executed": False,
        "production_state_mutated": False,
        "safety": dict(TREND_CALIBRATION_SAFETY),
        **TREND_CALIBRATION_SAFETY,
    }
    _assert_safe_output(payload)
    return payload


def write_trend_signal_dataset(
    payload: Mapping[str, Any],
    *,
    output_dir: Path = DEFAULT_TREND_CALIBRATION_DATASET_DIR,
) -> dict[str, Path]:
    _assert_safe_output(payload)
    dataset_id = _artifact_stem(_text(payload.get("dataset_id"), "trend-signal-dataset"))
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / f"{dataset_id}.json"
    csv_path = output_dir / f"{dataset_id}.csv"
    markdown_path = output_dir / f"{dataset_id}.md"
    _write_json(payload, json_path)
    _dataset_frame(payload).to_csv(csv_path, index=False)
    _write_text(render_trend_signal_dataset_markdown(payload), markdown_path)
    return {"json": json_path, "csv": csv_path, "markdown": markdown_path}


def write_trend_calibration_report(
    payload: Mapping[str, Any],
    *,
    output_dir: Path = DEFAULT_TREND_CALIBRATION_REPORT_DIR,
) -> dict[str, Path]:
    _assert_safe_output(payload)
    report_id = _artifact_stem(_text(payload.get("trend_calibration_report_id")))
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / f"{report_id}.json"
    markdown_path = output_dir / f"{report_id}.md"
    _write_json(payload, json_path)
    _write_text(render_trend_calibration_report_markdown(payload), markdown_path)
    return {"json": json_path, "markdown": markdown_path}


def write_trend_signal_config_registry(
    payload: Mapping[str, Any],
    *,
    output_dir: Path = DEFAULT_TREND_CALIBRATION_REGISTRY_DIR,
) -> dict[str, Path]:
    _assert_safe_output(payload)
    registry_id = _artifact_stem(_text(payload.get("report_type"), "trend-config-registry"))
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / f"{registry_id}.json"
    markdown_path = output_dir / f"{registry_id}.md"
    _write_json(payload, json_path)
    _write_text(render_trend_signal_config_registry_markdown(payload), markdown_path)
    return {"json": json_path, "markdown": markdown_path}


def write_trend_calibration_validation_report(
    payload: Mapping[str, Any],
    *,
    output_dir: Path = DEFAULT_TREND_CALIBRATION_VALIDATION_DIR,
) -> dict[str, Path]:
    _assert_safe_output(payload)
    validation_id = _artifact_stem(_text(payload.get("validation_id")))
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / f"{validation_id}.json"
    markdown_path = output_dir / f"{validation_id}.md"
    _write_json(payload, json_path)
    _write_text(render_trend_calibration_validation_markdown(payload), markdown_path)
    return {"json": json_path, "markdown": markdown_path}


def render_trend_signal_dataset_markdown(payload: Mapping[str, Any]) -> str:
    data_quality = _mapping(payload.get("data_quality"))
    return "\n".join(
        [
            "# ETF Trend Signal Dataset",
            "",
            "## Safety",
            "",
            "- observe_only=true; candidate_only=true; production_effect=none; "
            "broker_action=none; manual_review_required=true",
            "- Forward fields are evaluation_only=true.",
            "",
            "## Summary",
            "",
            f"- Dataset ID: `{_text(payload.get('dataset_id'))}`",
            f"- Market Regime: `{_text(payload.get('market_regime'))}`",
            f"- Requested Date Range: `{_mapping(payload.get('requested_date_range'))}`",
            f"- Record Count: `{_int(payload.get('record_count'))}`",
            f"- Available Forward Samples: `{_int(payload.get('available_forward_sample_count'))}`",
            f"- Data Quality Status: `{_text(data_quality.get('status'), 'UNKNOWN')}`",
            "",
        ]
    )


def render_trend_calibration_report_markdown(payload: Mapping[str, Any]) -> str:
    summary = _mapping(payload.get("summary"))
    coverage = _mapping(payload.get("dataset_coverage"))
    lines = [
        "# ETF Trend Signal Calibration Report",
        "",
        "## Safety",
        "",
        "- observe_only=true; candidate_only=true; production_effect=none; "
        "broker_action=none; manual_review_required=true",
        "- This report does not output ETF target weights.",
        "",
        "## Summary",
        "",
        f"- Status: `{_text(payload.get('status'))}`",
        f"- Market Regime: `{_text(payload.get('market_regime'))}`",
        f"- Top Config: `{_text(summary.get('top_config'), 'MISSING')}`",
        f"- Top Quality Score: `{_float(summary.get('top_quality_score')):.3f}`",
        f"- Evidence Status: `{_text(summary.get('evidence_status'))}`",
        f"- Redundancy Risk: `{_text(summary.get('redundancy_risk'))}`",
        f"- Regime Stability: `{_text(summary.get('regime_stability'))}`",
        f"- Data Quality Status: `{_text(coverage.get('data_quality_status'), 'UNKNOWN')}`",
        "",
        "## Top Trend Signal Configs",
        "",
        "| Rank | Config | Quality | Status |",
        "|---:|---|---:|---|",
    ]
    for row in _records(payload.get("top_trend_signal_configs")):
        quality = _mapping(row.get("quality_summary"))
        lines.append(
            "| {rank} | `{config}` | {score:.3f} | `{status}` |".format(
                rank=_int(row.get("rank")),
                config=_text(row.get("trend_signal_config_id")),
                score=_float(quality.get("trend_signal_quality_score")),
                status=_text(row.get("status")),
            )
        )
    lines.extend(["", "## Source Links", ""])
    for key, value in _mapping(payload.get("source_links")).items():
        lines.append(f"- `{key}`: `{_text(value)}`")
    lines.append("")
    return "\n".join(lines)


def render_trend_signal_config_registry_markdown(payload: Mapping[str, Any]) -> str:
    lines = [
        "# ETF Trend Signal Config Registry",
        "",
        "## Safety",
        "",
        "- candidate_only=true; production_effect=none; broker_action=none; evaluation_only=true",
        "",
        "| Config | Status | Quality |",
        "|---|---|---:|",
    ]
    for row in _records(payload.get("configs")):
        quality = _mapping(row.get("attribution_summary"))
        lines.append(
            f"| `{_text(row.get('trend_signal_config_id'))}` | "
            f"`{_text(row.get('status'))}` | "
            f"{_float(quality.get('trend_signal_quality_score')):.3f} |"
        )
    lines.append("")
    return "\n".join(lines)


def render_trend_calibration_validation_markdown(payload: Mapping[str, Any]) -> str:
    lines = [
        "# ETF Trend Calibration Validation",
        "",
        f"- Status: `{_text(payload.get('status'))}`",
        f"- Failed Check Count: `{_int(payload.get('failed_check_count'))}`",
        "- Safety: observe_only=true; candidate_only=true; production_effect=none; "
        "broker_action=none; manual_review_required=true",
        "",
        "| Check | Status | Summary |",
        "|---|---|---|",
    ]
    for check in _records(payload.get("checks")):
        lines.append(
            f"| `{_text(check.get('check_id'))}` | `{_text(check.get('status'))}` | "
            f"{_escape_md(check.get('summary'))} |"
        )
    lines.append("")
    return "\n".join(lines)


def latest_trend_calibration_report_path(
    report_dir: Path = DEFAULT_TREND_CALIBRATION_REPORT_DIR,
) -> Path | None:
    return _latest_file(report_dir, "trend-calibration-report_*.json")


def _prepare_feature_frame(
    features: pd.DataFrame,
    *,
    start: date,
    end: date | None,
) -> pd.DataFrame:
    required = {"date", "symbol"}
    missing = sorted(required - set(features.columns))
    if missing:
        raise TrendCalibrationError("trend feature frame missing columns: " + ", ".join(missing))
    frame = features.copy()
    frame["_date"] = pd.to_datetime(frame["date"], errors="coerce")
    frame = frame.loc[frame["_date"].notna()].copy()
    frame = frame.loc[frame["_date"] >= pd.Timestamp(start)].copy()
    if end is not None:
        frame = frame.loc[frame["_date"] <= pd.Timestamp(end)].copy()
    if frame.empty:
        raise TrendCalibrationError("trend signal dataset has no feature rows in requested range")
    return frame.sort_values(["_date", "symbol"]).reset_index(drop=True)


def _price_pivot(prices: pd.DataFrame, *, end: date | None) -> pd.DataFrame:
    frame = prices.copy()
    if "symbol" not in frame.columns and "ticker" in frame.columns:
        frame["symbol"] = frame["ticker"]
    if "adj_close" not in frame.columns and "close" in frame.columns:
        frame["adj_close"] = frame["close"]
    required = {"date", "symbol", "adj_close"}
    missing = sorted(required - set(frame.columns))
    if missing:
        raise TrendCalibrationError(
            "trend calibration prices missing columns: " + ", ".join(missing)
        )
    frame["_date"] = pd.to_datetime(frame["date"], errors="coerce")
    frame["_adj_close"] = pd.to_numeric(frame["adj_close"], errors="coerce")
    frame = frame.loc[frame["_date"].notna() & frame["_adj_close"].notna()].copy()
    if end is not None:
        frame = frame.loc[frame["_date"] <= pd.Timestamp(end) + pd.Timedelta(days=90)].copy()
    if frame.empty:
        raise TrendCalibrationError("trend calibration prices are empty")
    return (
        frame.pivot_table(index="_date", columns="symbol", values="_adj_close", aggfunc="last")
        .sort_index()
        .ffill()
    )


def _date_signal_values(group: pd.DataFrame, strategy: ETFStrategyConfig) -> dict[str, float]:
    by_symbol: dict[str, dict[str, float]] = {}
    for _, row in group.iterrows():
        symbol = _text(row.get("symbol"))
        if not symbol or symbol == "CASH":
            continue
        trend, _ = trend_score_for_row(row)
        momentum, _ = momentum_score_for_row(row, strategy)
        relative, _ = relative_strength_score_for_row(row, strategy)
        by_symbol[symbol] = {
            "trend": _clamp_score(trend),
            "momentum": _clamp_score(momentum),
            "relative_strength": _clamp_score(relative),
            "volatility_risk": _volatility_risk_score(row, strategy),
            "drawdown_risk": _drawdown_risk_score(row, strategy),
        }
    values = {
        "price_trend": _mean_symbol(by_symbol, "trend", ("SPY", "QQQ", "SMH", "SOXX")),
        "momentum": _mean_symbol(by_symbol, "momentum", ("SPY", "QQQ", "SMH", "SOXX")),
        "relative_strength": _mean_symbol(
            by_symbol,
            "relative_strength",
            ("QQQ", "SMH", "SOXX"),
        ),
        "volatility_risk": _mean_symbol(
            by_symbol,
            "volatility_risk",
            ("SPY", "QQQ", "SMH", "SOXX"),
        ),
        "drawdown_risk": _mean_symbol(by_symbol, "drawdown_risk", ("SPY", "QQQ", "SMH", "SOXX")),
        "AIConfirmationScore": 50.0,
        "SemiconductorBreadthScore": _mean(
            [
                by_symbol.get("SMH", {}).get("trend"),
                by_symbol.get("SOXX", {}).get("trend"),
                by_symbol.get("SMH", {}).get("momentum"),
                by_symbol.get("SOXX", {}).get("momentum"),
            ]
        ),
        "MegaCapAIScore": 50.0,
        "EventRiskScore": 50.0,
    }
    return {key: round(_clamp_score(value), 6) for key, value in values.items()}


def _volatility_risk_score(row: Mapping[str, Any], strategy: ETFStrategyConfig) -> float:
    vol = _float_or_none(row.get("realized_vol_20d"))
    if vol is None:
        return 50.0
    low = float(strategy.score_mapping.vol_low)
    high = float(strategy.score_mapping.vol_high)
    if vol <= low:
        return 100.0
    if vol >= high:
        return 0.0
    return _clamp_score(100.0 * (1.0 - (vol - low) / (high - low)))


def _drawdown_risk_score(row: Mapping[str, Any], strategy: ETFStrategyConfig) -> float:
    drawdown = _float_or_none(row.get("drawdown_63d"))
    if drawdown is None:
        return 50.0
    low = float(strategy.score_mapping.drawdown_low)
    high = float(strategy.score_mapping.drawdown_high)
    if drawdown >= low:
        return 100.0
    if drawdown <= high:
        return 0.0
    return _clamp_score(100.0 * (drawdown - high) / (low - high))


def _forward_evaluation_fields(
    price_pivot: pd.DataFrame,
    *,
    score_date: date,
    windows: Sequence[int],
) -> dict[str, Any]:
    dates = list(price_pivot.index)
    score_ts = pd.Timestamp(score_date)
    if score_ts not in price_pivot.index:
        candidates = [dt for dt in dates if dt <= score_ts]
        if not candidates:
            return _empty_forward_fields(windows)
        score_ts = candidates[-1]
    start_index = dates.index(score_ts)
    returns: dict[str, float | None] = {}
    drawdowns: dict[str, float | None] = {}
    vols: dict[str, float | None] = {}
    sample_available = False
    for window in windows:
        end_index = start_index + int(window)
        if end_index >= len(dates):
            for target in PRICE_TARGETS:
                returns[f"{target}_{window}D"] = None
                drawdowns[f"{target}_{window}D"] = None
                vols[f"{target}_{window}D"] = None
            returns[f"QQQ_SPY_relative_{window}D"] = None
            returns[f"SMH_QQQ_relative_{window}D"] = None
            continue
        end_ts = dates[end_index]
        for target in PRICE_TARGETS:
            returns[f"{target}_{window}D"] = _window_return(price_pivot, target, score_ts, end_ts)
            drawdowns[f"{target}_{window}D"] = _window_drawdown(
                price_pivot,
                target,
                score_ts,
                end_ts,
            )
            vols[f"{target}_{window}D"] = _window_volatility(price_pivot, target, score_ts, end_ts)
        qqq = returns.get(f"QQQ_{window}D")
        spy = returns.get(f"SPY_{window}D")
        smh = returns.get(f"SMH_{window}D")
        returns[f"QQQ_SPY_relative_{window}D"] = None if qqq is None or spy is None else qqq - spy
        returns[f"SMH_QQQ_relative_{window}D"] = None if smh is None or qqq is None else smh - qqq
        sample_available = sample_available or returns.get(f"QQQ_{window}D") is not None
    return {
        "forward_return_windows": returns,
        "forward_drawdown_windows": drawdowns,
        "forward_volatility_windows": vols,
        "sample_available": sample_available,
    }


def _empty_forward_fields(windows: Sequence[int]) -> dict[str, Any]:
    returns: dict[str, None] = {}
    drawdowns: dict[str, None] = {}
    vols: dict[str, None] = {}
    for window in windows:
        for target in PRICE_TARGETS:
            returns[f"{target}_{window}D"] = None
            drawdowns[f"{target}_{window}D"] = None
            vols[f"{target}_{window}D"] = None
        returns[f"QQQ_SPY_relative_{window}D"] = None
        returns[f"SMH_QQQ_relative_{window}D"] = None
    return {
        "forward_return_windows": returns,
        "forward_drawdown_windows": drawdowns,
        "forward_volatility_windows": vols,
        "sample_available": False,
    }


def _window_return(
    frame: pd.DataFrame,
    symbol: str,
    start: pd.Timestamp,
    end: pd.Timestamp,
) -> float | None:
    if symbol not in frame.columns:
        return None
    start_value = _float_or_none(frame.loc[start, symbol])
    end_value = _float_or_none(frame.loc[end, symbol])
    if start_value is None or end_value is None or start_value <= 0:
        return None
    return round(end_value / start_value - 1.0, 10)


def _window_drawdown(
    frame: pd.DataFrame,
    symbol: str,
    start: pd.Timestamp,
    end: pd.Timestamp,
) -> float | None:
    if symbol not in frame.columns:
        return None
    series = frame.loc[start:end, symbol].dropna()
    if series.empty:
        return None
    start_value = _float_or_none(series.iloc[0])
    if start_value is None or start_value <= 0:
        return None
    cumulative = series / start_value - 1.0
    return round(float(cumulative.min()), 10)


def _window_volatility(
    frame: pd.DataFrame,
    symbol: str,
    start: pd.Timestamp,
    end: pd.Timestamp,
) -> float | None:
    if symbol not in frame.columns:
        return None
    returns = frame.loc[start:end, symbol].pct_change().dropna()
    if returns.empty:
        return None
    return round(float(returns.std() * np.sqrt(252)), 10)


def _signal_availability(
    values: Mapping[str, Any],
    policy: TrendCalibrationPolicyConfig,
) -> dict[str, dict[str, Any]]:
    output = {}
    for signal in policy.signal_registry:
        value = _float_or_none(values.get(signal.signal_id))
        output[signal.signal_id] = {
            "available": value is not None,
            "required": signal.required,
            "data_quality_requirement": signal.data_quality_requirement,
            "normalization_method": signal.normalization_method,
            "used_neutral_default": value is None and not signal.required,
        }
    return output


def _regime_label(values: Mapping[str, Any]) -> str:
    trend = _float(values.get("price_trend"), 50.0)
    risk = _mean([values.get("volatility_risk"), values.get("drawdown_risk")])
    growth = _float(values.get("relative_strength"), 50.0)
    semis = _float(values.get("SemiconductorBreadthScore"), 50.0)
    if risk < 40 or trend < 40:
        return "risk_off"
    if growth < 45 and trend < 55:
        return "growth_underperformance"
    if semis >= 65 and semis >= growth:
        return "semiconductor_leadership"
    if trend >= 70 and risk >= 55:
        return "risk_on"
    return "neutral"


def _score_band(score: float, policy: TrendCalibrationPolicyConfig) -> str:
    for band in policy.score_bands:
        if score >= band.min_score and (score < band.max_score or band.max_score == 100):
            return band.band_id
    return policy.score_bands[-1].band_id


def _weight_set_record(weight_set: Mapping[str, Any]) -> dict[str, Any]:
    record = dict(weight_set)
    if "weights" not in record:
        record = {
            "weight_set_id": _text(weight_set.get("weight_set_id")),
            "weights": dict(weight_set),
        }
    record["weights"] = {
        key: float(value) for key, value in _mapping(record.get("weights")).items()
    }
    return record


def _trend_config_quality_summary(
    *,
    attribution: Mapping[str, Any],
    regime_stability: Mapping[str, Any],
    redundancy: Mapping[str, Any],
    policy: TrendCalibrationPolicyConfig,
) -> dict[str, Any]:
    bucket_rows = _records(attribution.get("bucket_rows"))
    by_bucket = {_text(row.get("bucket")): row for row in bucket_rows}
    high = _first_existing_mapping(by_bucket, ("strong_risk_on", "risk_on"))
    low = _first_existing_mapping(by_bucket, ("risk_off", "weak"))
    high_return = _float(high.get("QQQ_forward_return_mean"))
    low_return = _float(low.get("QQQ_forward_return_mean"))
    high_drawdown = _float(high.get("QQQ_forward_drawdown_mean"))
    low_drawdown = _float(low.get("QQQ_forward_drawdown_mean"))
    lift = high_return - low_return
    drawdown_improvement = high_drawdown - low_drawdown
    ranking = policy.search.ranking_policy
    return_lift_score = _ratio_score(lift, ranking.forward_lift_full_score)
    drawdown_score = _ratio_score(drawdown_improvement, ranking.drawdown_improvement_full_score)
    usable_count = sum(_int(row.get("sample_count")) for row in bucket_rows)
    coverage_target = (
        policy.forward_attribution.minimum_sample_count * ranking.coverage_full_score_multiplier
    )
    coverage_score = _ratio_score(float(usable_count), coverage_target)
    limited_regimes = _int(regime_stability.get("limited_regime_count"))
    required_regime_count = max(1, len(policy.regime_stability.required_regimes))
    regime_score = max(0.0, 1.0 - limited_regimes / required_regime_count)
    high_redundancy = _int(redundancy.get("high_redundancy_count"))
    redundancy_penalty = min(1.0, high_redundancy * ranking.high_redundancy_penalty)
    quality = (
        ranking.return_lift_weight * return_lift_score
        + ranking.drawdown_control_weight * drawdown_score
        + ranking.coverage_weight * coverage_score
        + ranking.regime_stability_weight * regime_score
        - redundancy_penalty
    )
    quality = max(0.0, min(1.0, quality))
    return {
        "trend_signal_quality_score": round(quality, 6),
        "return_lift": round(lift, 10),
        "drawdown_improvement": round(drawdown_improvement, 10),
        "usable_sample_count": usable_count,
        "usable_bucket_count": _int(attribution.get("usable_bucket_count")),
        "regime_stability_status": "limited" if limited_regimes else "usable",
        "high_redundancy_count": high_redundancy,
        "ranking_policy": ranking.model_dump(mode="json"),
        "evaluation_only": True,
    }


def _candidate_status(
    quality: Mapping[str, Any],
    policy: TrendCalibrationPolicyConfig,
) -> str:
    if _int(quality.get("usable_sample_count")) < policy.forward_attribution.minimum_sample_count:
        return "needs_more_data"
    if _int(quality.get("high_redundancy_count")):
        return "review_with_redundancy_warning"
    if _float(quality.get("trend_signal_quality_score")) >= 0.60:
        return "review_recommended"
    return "candidate_review"


def _score_summary(score_run: Mapping[str, Any]) -> dict[str, Any]:
    rows = _records(score_run.get("scores"))
    return {
        "score_count": len(rows),
        "mean_composite_score": _mean([row.get("CompositeTrendScore") for row in rows]),
        "min_composite_score": _min_value([row.get("CompositeTrendScore") for row in rows]),
        "max_composite_score": _max_value([row.get("CompositeTrendScore") for row in rows]),
        "band_counts": _counts([_text(row.get("TrendScoreBand")) for row in rows]),
    }


def _redundancy_summary(redundancy: Mapping[str, Any]) -> dict[str, Any]:
    high = _int(redundancy.get("high_redundancy_count"))
    medium = _int(redundancy.get("medium_redundancy_count"))
    return {
        "risk_level": "high" if high else ("medium" if medium else "low"),
        "high_redundancy_count": high,
        "medium_redundancy_count": medium,
    }


def _dataset_frame(payload: Mapping[str, Any]) -> pd.DataFrame:
    rows = []
    for record in _records(payload.get("records")):
        row = {
            "date": _text(record.get("date")),
            "asset_context": _text(record.get("asset_context")),
            "regime_label": _text(record.get("regime_label")),
            "sample_available": record.get("sample_available") is True,
            "evaluation_only": record.get("evaluation_only") is True,
            "data_quality_status": _text(record.get("data_quality_status")),
        }
        for key, value in _mapping(record.get("normalized_signal_values")).items():
            row[key] = value
        for key, value in _mapping(record.get("forward_return_windows")).items():
            row[f"forward_return_{key}"] = value
        rows.append(row)
    return pd.DataFrame(rows)


def _append_check(
    checks: list[dict[str, Any]],
    check_id: str,
    passed: bool,
    summary: str,
) -> None:
    checks.append(
        {
            "check_id": check_id,
            "status": "PASS" if passed else "FAIL",
            "summary": summary,
            "production_effect": "none",
        }
    )


def _validation_summary(check_id: str, passed: bool) -> str:
    messages = {
        "signal_registry_available": "trend signal registry is available",
        "dataset_builder_available": "trend signal dataset builder is available",
        "weight_config_schema_available": "trend weight config schema is available",
        "score_engine_available": "trend score engine is available",
        "search_runner_available": "bounded trend search runner is available",
        "bucket_attribution_available": "bucket forward attribution is available",
        "redundancy_diagnostics_available": "redundancy diagnostics are available",
        "regime_stability_available": "regime stability review is available",
        "config_registry_available": "trend config registry is available",
        "report_available": "trend calibration report is available",
    }
    return f"{messages.get(check_id, check_id)}: {'available' if passed else 'failed'}"


def _sample_prices_assets_strategy() -> tuple[pd.DataFrame, ETFAssetsConfig, ETFStrategyConfig]:
    config = load_etf_config_bundle()
    trading_dates = pd.bdate_range("2022-01-03", "2024-05-31")
    rows: list[dict[str, Any]] = []
    for symbol, start_price, drift in (
        ("SPY", 450.0, 0.00025),
        ("QQQ", 360.0, 0.00035),
        ("SMH", 260.0, 0.00045),
        ("SOXX", 520.0, 0.00040),
    ):
        for index, dt in enumerate(trading_dates):
            seasonal = np.sin(index / 18.0) * 0.01
            price = start_price * (1.0 + drift + seasonal / 10.0) ** index
            rows.append(
                {
                    "date": dt.date().isoformat(),
                    "symbol": symbol,
                    "open": price * 0.99,
                    "high": price * 1.01,
                    "low": price * 0.98,
                    "close": price,
                    "adj_close": price,
                    "volume": 1000000 + index,
                    "source": "trend_calibration_validation_probe",
                    "created_at": "2026-06-05T00:00:00+00:00",
                }
            )
    return pd.DataFrame(rows), config.assets, config.strategy


def _registry_has_trend_calibration(path: Path) -> bool:
    try:
        registry = load_report_registry(path)
    except Exception:
        return False
    reports = {_text(item.get("report_id")): item for item in _records(registry.get("reports"))}
    return all(
        reports.get(report_id, {}).get("include_in_reader_brief") is True
        for report_id in (
            TREND_CALIBRATION_REPORT_REGISTRY_ID,
            TREND_CALIBRATION_VALIDATION_REGISTRY_ID,
        )
    )


def _mean_symbol(
    by_symbol: Mapping[str, Mapping[str, Any]],
    field: str,
    symbols: Sequence[str],
) -> float:
    values = [_float_or_none(_mapping(by_symbol.get(symbol)).get(field)) for symbol in symbols]
    return _mean(values)


def _mean(values: Sequence[Any]) -> float:
    parsed = [_float_or_none(value) for value in values]
    clean = [float(value) for value in parsed if value is not None and np.isfinite(value)]
    if not clean:
        return 50.0
    return float(np.mean(clean))


def _min_value(values: Sequence[Any]) -> float | None:
    clean = [value for value in (_float_or_none(item) for item in values) if value is not None]
    return None if not clean else min(clean)


def _max_value(values: Sequence[Any]) -> float | None:
    clean = [value for value in (_float_or_none(item) for item in values) if value is not None]
    return None if not clean else max(clean)


def _ratio_score(value: float, full_score_value: float) -> float:
    if value <= 0:
        return 0.0
    return max(0.0, min(1.0, value / full_score_value))


def _first_existing_mapping(
    mappings: Mapping[str, Mapping[str, Any]],
    keys: Sequence[str],
) -> dict[str, Any]:
    for key in keys:
        if key in mappings:
            return dict(mappings[key])
    return {}


def _counts(values: Sequence[str]) -> dict[str, int]:
    output: dict[str, int] = {}
    for value in values:
        if value:
            output[value] = output.get(value, 0) + 1
    return dict(sorted(output.items()))


def _records(value: object) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [dict(item) for item in value if isinstance(item, Mapping)]


def _mapping(value: object) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _text(value: object, default: str = "") -> str:
    if value is None or value == "":
        return default
    text = str(value).strip()
    return text or default


def _int(value: object, default: int = 0) -> int:
    parsed = _float_or_none(value)
    return default if parsed is None else int(parsed)


def _float(value: object, default: float = 0.0) -> float:
    parsed = _float_or_none(value)
    return default if parsed is None else float(parsed)


def _float_or_none(value: object) -> float | None:
    if value in (None, ""):
        return None
    try:
        parsed = float(str(value))
    except (TypeError, ValueError):
        return None
    if not np.isfinite(parsed):
        return None
    return parsed


def _clamp_score(value: object) -> float:
    return min(100.0, max(0.0, _float(value, 50.0)))


def _coerce_datetime(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _stable_id(prefix: str, *parts: object) -> str:
    digest = sha256("|".join(str(part) for part in parts).encode("utf-8")).hexdigest()[:12]
    return f"{prefix}:{digest}"


def _stable_hash(payload: Mapping[str, Any]) -> str:
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
    return sha256(encoded).hexdigest()


def _artifact_stem(value: object) -> str:
    text = _text(value, "artifact").replace(":", "_").replace("/", "_").replace("\\", "_")
    return "".join(char if char.isalnum() or char in {"_", "-", "."} else "_" for char in text)


def _latest_file(directory: Path, pattern: str) -> Path | None:
    if not directory.exists():
        return None
    candidates = [path for path in directory.glob(pattern) if path.is_file()]
    if not candidates:
        return None
    return sorted(candidates, key=lambda path: (path.stat().st_mtime, path.name))[-1]


def _write_json(payload: Mapping[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(_json_safe(payload), ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _write_text(text: str, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _json_safe(value: object) -> object:
    if isinstance(value, Mapping):
        return {str(key): _json_safe(child) for key, child in value.items()}
    if isinstance(value, list):
        return [_json_safe(item) for item in value]
    if isinstance(value, tuple):
        return [_json_safe(item) for item in value]
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
        return None if not np.isfinite(float(value)) else float(value)
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    return value


def _escape_md(value: object) -> str:
    return _text(value).replace("|", "\\|").replace("\n", " ")


def _assert_safe_output(payload: Mapping[str, Any]) -> None:
    if _contains_forbidden_output(payload):
        raise TrendCalibrationError("trend calibration output contains forbidden action keys")
    safety = _mapping(payload.get("safety"))
    if safety and safety != TREND_CALIBRATION_SAFETY:
        raise TrendCalibrationError("trend calibration output safety fields are unsafe")
    for key, expected in TREND_CALIBRATION_SAFETY.items():
        if key in payload and payload.get(key) != expected:
            raise TrendCalibrationError(f"trend calibration output unsafe field: {key}")


def _contains_forbidden_output(value: object) -> bool:
    if isinstance(value, Mapping):
        for key, child in value.items():
            if str(key) in FORBIDDEN_OUTPUT_KEYS:
                return True
            if _contains_forbidden_output(child):
                return True
    elif isinstance(value, list):
        return any(_contains_forbidden_output(item) for item in value)
    return False
