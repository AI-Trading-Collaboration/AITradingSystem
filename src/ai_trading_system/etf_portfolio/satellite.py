from __future__ import annotations

import json
from collections.abc import Iterable, Mapping
from datetime import UTC, date, datetime, timedelta
from hashlib import sha256
from pathlib import Path
from typing import Any, Literal, Self

import numpy as np
import pandas as pd
from pydantic import BaseModel, Field, model_validator

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.etf_portfolio.models import PolicyMetadata
from ai_trading_system.yaml_loader import safe_load_yaml_path

DEFAULT_SATELLITE_UNIVERSE_CONFIG_PATH = (
    PROJECT_ROOT / "config" / "etf_portfolio" / "satellite_universe.yaml"
)
DEFAULT_SATELLITE_POLICY_CONFIG_PATH = (
    PROJECT_ROOT / "config" / "etf_portfolio" / "satellite_policy.yaml"
)
DEFAULT_SATELLITE_REPORT_DIR = PROJECT_ROOT / "reports" / "etf_portfolio" / "satellite"
DEFAULT_SATELLITE_FEATURE_DIR = DEFAULT_SATELLITE_REPORT_DIR / "features"
DEFAULT_SATELLITE_STANDALONE_REPORT_DIR = DEFAULT_SATELLITE_REPORT_DIR / "reports"
DEFAULT_SATELLITE_EXPERIMENT_DIR = DEFAULT_SATELLITE_REPORT_DIR / "experiments"
DEFAULT_SATELLITE_VALIDATION_DIR = DEFAULT_SATELLITE_REPORT_DIR / "validation"

SATELLITE_FEATURE_SCHEMA_VERSION = "satellite_relative_strength_features_v1"
SATELLITE_REPORT_SCHEMA_VERSION = "satellite_replacement_report_v1"
SATELLITE_EXPERIMENT_SCHEMA_VERSION = "satellite_shadow_experiment_v1"
SATELLITE_VALIDATION_SCHEMA_VERSION = "satellite_policy_validation_v1"

SATELLITE_SAFETY = {
    "observe_only": True,
    "candidate_only": True,
    "production_effect": "none",
    "broker_action": "none",
    "manual_review_required": True,
}

FEATURE_RETURN_WINDOWS = (20, 60, 120)
STOCK_MA_WINDOWS = (20, 50, 100, 200)
BENCHMARK_MA_WINDOWS = (50, 200)
RELATIVE_MA_WINDOWS = (50, 200)
DRAW_DOWN_WINDOW = 60
VOLATILITY_WINDOW = 20


class SatelliteSafetyConfig(BaseModel):
    observe_only: bool
    candidate_only: bool
    production_effect: str = Field(min_length=1)
    broker_action: str = Field(min_length=1)
    manual_review_required: bool

    @model_validator(mode="after")
    def validate_safety_boundary(self) -> Self:
        if self.model_dump(mode="json") != SATELLITE_SAFETY:
            raise ValueError(
                "satellite replacement safety must keep observe_only=true, "
                "candidate_only=true, production_effect=none, broker_action=none, "
                "manual_review_required=true"
            )
        return self


class SatelliteStockConfig(BaseModel):
    ticker: str = Field(min_length=1)
    name: str = Field(min_length=1)
    group: str = Field(min_length=1)
    role: str = Field(min_length=1)
    enabled: bool
    optional: bool
    benchmark_etf: str = Field(min_length=1)
    sleeve: str = Field(min_length=1)
    max_single_name_weight: float = Field(gt=0, le=1)
    min_data_coverage: float = Field(ge=0, le=1)
    event_risk_group: str = Field(min_length=1)

    @model_validator(mode="after")
    def normalize_symbol_fields(self) -> Self:
        self.ticker = self.ticker.strip().upper()
        self.benchmark_etf = self.benchmark_etf.strip().upper()
        self.group = self.group.strip()
        self.role = self.role.strip()
        self.sleeve = self.sleeve.strip()
        self.event_risk_group = self.event_risk_group.strip()
        return self


class SatelliteUniverseConfig(BaseModel):
    policy_metadata: PolicyMetadata
    safety: SatelliteSafetyConfig
    allowed_benchmarks: list[str] = Field(min_length=1)
    satellite_universe: list[SatelliteStockConfig] = Field(min_length=1)

    @model_validator(mode="after")
    def validate_universe(self) -> Self:
        self.allowed_benchmarks = sorted(
            {benchmark.strip().upper() for benchmark in self.allowed_benchmarks}
        )
        if any(not benchmark for benchmark in self.allowed_benchmarks):
            raise ValueError("satellite allowed benchmarks must be non-empty")
        allowed = set(self.allowed_benchmarks)
        for stock in self.satellite_universe:
            if stock.benchmark_etf not in allowed:
                raise ValueError(
                    f"satellite stock {stock.ticker} references invalid benchmark ETF: "
                    f"{stock.benchmark_etf}"
                )
        return self

    @property
    def config_hash(self) -> str:
        return _config_hash(self.model_dump(mode="json"))


class SatelliteScoreBandConfig(BaseModel):
    min_score: float = Field(ge=0, le=100)


class SatelliteScorePolicy(BaseModel):
    policy_id: str = Field(min_length=1)
    component_weights: dict[str, float] = Field(min_length=1)
    relative_return_full_scale: float = Field(gt=0)
    momentum_return_full_scale: float = Field(gt=0)
    drawdown_full_penalty: float = Field(lt=0)
    relative_volatility_warning: float = Field(gt=0)
    relative_volatility_block: float = Field(gt=0)
    driver_positive_min: float = Field(ge=0, le=100)
    driver_negative_max: float = Field(ge=0, le=100)

    @model_validator(mode="after")
    def validate_weights_and_thresholds(self) -> Self:
        total = sum(float(value) for value in self.component_weights.values())
        if abs(total - 1.0) > 1e-6:
            raise ValueError(f"satellite score component weights must sum to 1.0: {total}")
        if any(float(value) < 0 for value in self.component_weights.values()):
            raise ValueError("satellite score component weights must be non-negative")
        if self.relative_volatility_warning >= self.relative_volatility_block:
            raise ValueError("relative_volatility_warning must be below block threshold")
        return self


class SatelliteEligibilityPolicy(BaseModel):
    policy_id: str = Field(min_length=1)
    eligible_score_min: float = Field(ge=0, le=100)
    watch_score_min: float = Field(ge=0, le=100)
    relative_return_60d_min: float
    trend_score_min: float = Field(ge=0, le=100)
    max_drawdown: float = Field(le=0)
    max_relative_volatility: float = Field(gt=0)
    event_risk_high_min: float = Field(ge=0, le=100)
    ai_confirmation_neutral_min: float = Field(ge=0, le=100)
    min_data_coverage: float = Field(ge=0, le=1)
    fallback_to_etf_on_gate_fail: bool

    @model_validator(mode="after")
    def validate_gate_order(self) -> Self:
        if self.watch_score_min > self.eligible_score_min:
            raise ValueError("watch score threshold must not exceed eligible threshold")
        if not self.fallback_to_etf_on_gate_fail:
            raise ValueError("satellite policy must keep ETF fallback enabled")
        return self


class SatelliteRiskConstraints(BaseModel):
    policy_id: str = Field(min_length=1)
    max_total_satellite_weight: float = Field(ge=0, le=1)
    max_single_stock_weight: float = Field(gt=0, le=1)
    max_stocks_per_sleeve: int = Field(gt=0)
    max_sector_replacement_weight: float = Field(ge=0, le=1)
    min_etf_residual_weight: float = Field(ge=0, le=1)
    max_relative_volatility: float = Field(gt=0)
    max_drawdown: float = Field(le=0)
    event_risk_block: str = Field(min_length=1)
    earnings_window_block_or_discount: Literal["block", "discount"]
    liquidity_warning_if_available: bool


class SatellitePolicyConfig(BaseModel):
    policy_metadata: PolicyMetadata
    safety: SatelliteSafetyConfig
    score_bands: dict[str, SatelliteScoreBandConfig] = Field(min_length=1)
    score_policy: SatelliteScorePolicy
    eligibility_policy: SatelliteEligibilityPolicy
    risk_constraints: SatelliteRiskConstraints

    @model_validator(mode="after")
    def validate_policy(self) -> Self:
        if "fallback" not in self.score_bands:
            raise ValueError("satellite score bands must include fallback")
        if self.score_bands["fallback"].min_score != 0:
            raise ValueError("satellite fallback score band must start at 0")
        return self

    @property
    def config_hash(self) -> str:
        return _config_hash(self.model_dump(mode="json"))


def load_satellite_universe_config(
    path: Path | str = DEFAULT_SATELLITE_UNIVERSE_CONFIG_PATH,
) -> SatelliteUniverseConfig:
    raw = safe_load_yaml_path(Path(path))
    if not isinstance(raw, dict):
        raise ValueError(f"satellite universe config must be a YAML mapping: {path}")
    return SatelliteUniverseConfig.model_validate(raw)


def load_satellite_policy_config(
    path: Path | str = DEFAULT_SATELLITE_POLICY_CONFIG_PATH,
) -> SatellitePolicyConfig:
    raw = safe_load_yaml_path(Path(path))
    if not isinstance(raw, dict):
        raise ValueError(f"satellite policy config must be a YAML mapping: {path}")
    return SatellitePolicyConfig.model_validate(raw)


def enabled_satellite_stocks(config: SatelliteUniverseConfig) -> list[SatelliteStockConfig]:
    return [stock for stock in _dedupe_stocks(config.satellite_universe) if stock.enabled]


def all_enabled_satellite_tickers(config: SatelliteUniverseConfig) -> tuple[str, ...]:
    return tuple(sorted(stock.ticker for stock in enabled_satellite_stocks(config)))


def satellite_price_symbols(config: SatelliteUniverseConfig) -> tuple[str, ...]:
    symbols = {stock.benchmark_etf for stock in enabled_satellite_stocks(config)}
    symbols.update(stock.ticker for stock in enabled_satellite_stocks(config))
    return tuple(sorted(symbols))


def stock_benchmark_mapping(
    config: SatelliteUniverseConfig,
    ticker: str,
    *,
    include_disabled: bool = False,
) -> dict[str, Any]:
    normalized = ticker.strip().upper()
    for stock in _dedupe_stocks(config.satellite_universe):
        if stock.ticker != normalized:
            continue
        if not include_disabled and not stock.enabled:
            raise KeyError(f"satellite ticker is disabled: {normalized}")
        return {
            "stock_ticker": stock.ticker,
            "benchmark_etf": stock.benchmark_etf,
            "sleeve": stock.sleeve,
            "role": stock.role,
            "group": stock.group,
            "replacement_source": config.policy_metadata.version,
            "optional": stock.optional,
            "enabled": stock.enabled,
        }
    raise KeyError(f"unknown satellite ticker: {normalized}")


def satellite_benchmark_mappings(config: SatelliteUniverseConfig) -> list[dict[str, Any]]:
    return [
        stock_benchmark_mapping(config, stock.ticker)
        for stock in sorted(enabled_satellite_stocks(config), key=lambda item: item.ticker)
    ]


def validate_satellite_data_availability(
    config: SatelliteUniverseConfig,
    available_symbols: Iterable[str],
) -> dict[str, Any]:
    available = {symbol.strip().upper() for symbol in available_symbols}
    reports: list[dict[str, Any]] = []
    errors: list[str] = []
    warnings: list[str] = []
    for stock in sorted(enabled_satellite_stocks(config), key=lambda item: item.ticker):
        has_stock = stock.ticker in available
        has_benchmark = stock.benchmark_etf in available
        missing: list[str] = []
        if not has_stock:
            missing.append(stock.ticker)
        if not has_benchmark:
            missing.append(stock.benchmark_etf)
        if missing:
            prefix = "missing_optional" if stock.optional else "missing_required"
            messages = [f"{stock.ticker}:{prefix}:{symbol}" for symbol in missing]
            if stock.optional:
                warnings.extend(messages)
            else:
                errors.extend(messages)
        reports.append(
            {
                "ticker": stock.ticker,
                "benchmark_etf": stock.benchmark_etf,
                "optional": stock.optional,
                "stock_available": has_stock,
                "benchmark_available": has_benchmark,
                "data_coverage_ratio": (float(has_stock) + float(has_benchmark)) / 2.0,
            }
        )
    status = "FAIL" if errors else "PASS_WITH_WARNINGS" if warnings else "PASS"
    return {
        "status": status,
        "symbol_reports": reports,
        "errors": sorted(errors),
        "warnings": sorted(warnings),
        **SATELLITE_SAFETY,
    }


def build_satellite_relative_strength_features(
    prices: pd.DataFrame,
    *,
    universe_config: SatelliteUniverseConfig,
    run_date: date,
) -> list[dict[str, Any]]:
    frame = _prepare_price_history(prices, run_date)
    rows: list[dict[str, Any]] = []
    for stock in sorted(enabled_satellite_stocks(universe_config), key=lambda item: item.ticker):
        rows.append(_satellite_feature_row(frame, stock, run_date))
    return rows


def write_satellite_features(
    records: list[dict[str, Any]],
    *,
    output_dir: Path,
    run_date: date,
    data_quality_status: str,
    data_quality_report: str | None = None,
) -> dict[str, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    stem = f"satellite_features_{run_date.isoformat()}"
    payload = {
        "schema_version": SATELLITE_FEATURE_SCHEMA_VERSION,
        "report_type": "satellite_relative_strength_features",
        "date": run_date.isoformat(),
        "data_quality_status": data_quality_status,
        "data_quality_report": data_quality_report or "",
        "records": records,
        **SATELLITE_SAFETY,
    }
    json_path = output_dir / f"{stem}.json"
    csv_path = output_dir / f"{stem}.csv"
    json_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    satellite_feature_records_to_frame(records).to_csv(csv_path, index=False)
    return {"json": json_path, "csv": csv_path}


def satellite_feature_records_to_frame(records: list[dict[str, Any]]) -> pd.DataFrame:
    return pd.DataFrame(records)


def build_satellite_candidate_scores(
    feature_records: list[dict[str, Any]],
    *,
    universe_config: SatelliteUniverseConfig,
    policy_config: SatellitePolicyConfig,
    run_date: date,
    ai_confirmation_payload: Mapping[str, Any] | None = None,
) -> list[dict[str, Any]]:
    stocks = {stock.ticker: stock for stock in enabled_satellite_stocks(universe_config)}
    ai_context = _ai_confirmation_context(ai_confirmation_payload)
    scores: list[dict[str, Any]] = []
    for feature in sorted(feature_records, key=lambda item: str(item.get("ticker"))):
        ticker = str(feature.get("ticker", "")).upper()
        stock = stocks.get(ticker)
        if stock is None:
            continue
        scores.append(
            _satellite_score_row(
                feature,
                stock=stock,
                policy_config=policy_config,
                run_date=run_date,
                ai_context=ai_context,
            )
        )
    return scores


def build_replacement_eligibility_gate(
    score_records: list[dict[str, Any]],
    *,
    universe_config: SatelliteUniverseConfig,
    policy_config: SatellitePolicyConfig,
    base_weights: Mapping[str, float] | None = None,
) -> list[dict[str, Any]]:
    stocks = {stock.ticker: stock for stock in enabled_satellite_stocks(universe_config)}
    base = _normalize_weight_mapping(base_weights or {})
    rows: list[dict[str, Any]] = []
    for score in sorted(score_records, key=lambda item: str(item.get("ticker"))):
        ticker = str(score.get("ticker", "")).upper()
        stock = stocks.get(ticker)
        if stock is None:
            continue
        rows.append(
            _eligibility_row(
                score,
                stock=stock,
                policy_config=policy_config,
                base_weights=base,
            )
        )
    return rows


def build_etf_replacement_plan(
    *,
    run_date: date,
    base_weights: Mapping[str, float],
    eligibility_records: list[dict[str, Any]],
    universe_config: SatelliteUniverseConfig,
    policy_config: SatellitePolicyConfig,
    plan_id: str | None = None,
) -> dict[str, Any]:
    base = _normalize_weight_mapping(base_weights)
    candidate = dict(base)
    constraints = policy_config.risk_constraints
    stocks = {stock.ticker: stock for stock in enabled_satellite_stocks(universe_config)}
    eligible = [row for row in eligibility_records if row.get("status") == "eligible"]
    eligible.sort(key=lambda item: (-float(item.get("score_value", 0.0)), str(item.get("ticker"))))
    sleeve_counts: dict[str, int] = {}
    sleeve_replaced: dict[str, float] = {}
    etf_replaced: dict[str, float] = {}
    total_replaced = 0.0
    satellite_allocations: list[dict[str, Any]] = []
    fallback_positions: list[dict[str, Any]] = []
    constraints_applied: list[str] = []
    reason_codes: list[str] = []

    for row in eligible:
        ticker = str(row.get("ticker", "")).upper()
        stock = stocks.get(ticker)
        if stock is None:
            continue
        if sleeve_counts.get(stock.sleeve, 0) >= constraints.max_stocks_per_sleeve:
            fallback_positions.append(_fallback_position(row, "MAX_STOCKS_PER_SLEEVE"))
            constraints_applied.append("max_stocks_per_sleeve")
            continue
        requested = min(stock.max_single_name_weight, constraints.max_single_stock_weight)
        if requested < stock.max_single_name_weight:
            constraints_applied.append("max_single_stock_weight")
        benchmark_weight = base.get(stock.benchmark_etf, 0.0)
        max_benchmark_replace = max(
            0.0,
            benchmark_weight * (1.0 - constraints.min_etf_residual_weight)
            - etf_replaced.get(stock.benchmark_etf, 0.0),
        )
        max_sleeve_replace = max(
            0.0,
            constraints.max_sector_replacement_weight - sleeve_replaced.get(stock.sleeve, 0.0),
        )
        max_total_replace = max(0.0, constraints.max_total_satellite_weight - total_replaced)
        allocation = min(requested, max_benchmark_replace, max_sleeve_replace, max_total_replace)
        if allocation <= 1e-12:
            fallback_positions.append(_fallback_position(row, "REPLACEMENT_CAPACITY_EXHAUSTED"))
            constraints_applied.append("replacement_capacity_exhausted")
            continue
        if allocation < requested - 1e-12:
            constraints_applied.append("replacement_weight_clipped")
        candidate[stock.benchmark_etf] = candidate.get(stock.benchmark_etf, 0.0) - allocation
        candidate[ticker] = candidate.get(ticker, 0.0) + allocation
        total_replaced += allocation
        sleeve_counts[stock.sleeve] = sleeve_counts.get(stock.sleeve, 0) + 1
        sleeve_replaced[stock.sleeve] = sleeve_replaced.get(stock.sleeve, 0.0) + allocation
        etf_replaced[stock.benchmark_etf] = etf_replaced.get(stock.benchmark_etf, 0.0) + allocation
        satellite_allocations.append(
            {
                "ticker": ticker,
                "benchmark_etf": stock.benchmark_etf,
                "sleeve": stock.sleeve,
                "allocation": round(allocation, 10),
                "requested_weight": round(requested, 10),
                "score_value": row.get("score_value"),
                "reason_codes": row.get("reason_codes", []),
            }
        )
        reason_codes.append(f"{ticker}_REPLACES_{stock.benchmark_etf}")

    for row in eligibility_records:
        if row.get("status") != "eligible":
            fallback_positions.append(_fallback_position(row, str(row.get("status"))))
    if not satellite_allocations:
        reason_codes.append("NO_ELIGIBLE_SATELLITE_REPLACEMENT")
    _normalize_rounding(candidate)
    return {
        "schema_version": "satellite_replacement_plan_v1",
        "replacement_plan_id": plan_id or f"satellite-replacement-{run_date.isoformat()}",
        "date": run_date.isoformat(),
        "base_weights": _round_weights(base),
        "candidate_weights": _round_weights(candidate),
        "shadow_weights": _round_weights(candidate),
        "hypothetical_weights": _round_weights(candidate),
        "replacement_plan": {
            "satellite_allocations": satellite_allocations,
            "fallback_positions": fallback_positions,
        },
        "replaced_etf": _round_weights(etf_replaced),
        "satellite_allocations": satellite_allocations,
        "total_replaced_weight": round(total_replaced, 10),
        "fallback_positions": fallback_positions,
        "constraints_applied": sorted(set(constraints_applied)),
        "reason_codes": sorted(set(reason_codes)),
        "fallback_to_etf": bool(fallback_positions),
        **SATELLITE_SAFETY,
    }


def build_satellite_shadow_portfolio_experiment(
    *,
    run_date: date,
    replacement_plan: Mapping[str, Any],
    universe_config: SatelliteUniverseConfig,
    base_candidate_id: str = "etf_baseline",
) -> dict[str, Any]:
    allocations = _records(replacement_plan.get("satellite_allocations"))
    fallback_positions = _records(replacement_plan.get("fallback_positions"))
    mapping = satellite_benchmark_mappings(universe_config)
    status = "CANDIDATE_WEIGHTS_GENERATED" if allocations else "NO_ELIGIBLE_SATELLITES"
    return {
        "schema_version": SATELLITE_EXPERIMENT_SCHEMA_VERSION,
        "report_type": "satellite_shadow_portfolio_experiment",
        "experiment_id": f"satellite-shadow-{run_date.isoformat()}-{base_candidate_id}",
        "date": run_date.isoformat(),
        "status": status,
        "base_candidate_id": base_candidate_id,
        "replacement_plan_id": replacement_plan.get("replacement_plan_id"),
        "comparisons": [
            "ETF baseline",
            "ETF candidate from TRADING-064",
            "ETF + AI confirmation overlay from TRADING-066",
            "ETF + satellite replacement candidate",
        ],
        "before_weights": dict(_mapping(replacement_plan.get("base_weights"))),
        "after_candidate_weights": dict(_mapping(replacement_plan.get("candidate_weights"))),
        "candidate_weights": dict(_mapping(replacement_plan.get("candidate_weights"))),
        "shadow_weights": dict(_mapping(replacement_plan.get("shadow_weights"))),
        "hypothetical_weights": dict(_mapping(replacement_plan.get("hypothetical_weights"))),
        "eligible_satellites": [item.get("ticker") for item in allocations],
        "fallback_to_etf_symbols": [item.get("ticker") for item in fallback_positions],
        "constraints_applied": list(replacement_plan.get("constraints_applied", [])),
        "expected_benchmark_mapping": mapping,
        "decision_input_usage": (
            "decision-time replacement plan only; future returns are evaluation-only"
        ),
        **SATELLITE_SAFETY,
    }


def write_satellite_shadow_experiment(
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
    markdown_path.write_text(render_satellite_shadow_experiment_markdown(payload), encoding="utf-8")
    return json_path, markdown_path


def build_satellite_replacement_report(
    *,
    prices: pd.DataFrame,
    universe_config: SatelliteUniverseConfig,
    policy_config: SatellitePolicyConfig,
    run_date: date,
    data_quality_status: str,
    data_quality_report: str | None = None,
    base_weights: Mapping[str, float] | None = None,
    ai_confirmation_payload: Mapping[str, Any] | None = None,
    market_regime: str | None = None,
    requested_date_range: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    default_base_weights = {
        "SPY": 0.30,
        "QQQ": 0.40,
        "SMH": 0.15,
        "SOXX": 0.0,
        "CASH": 0.15,
    }
    base = _normalize_weight_mapping(base_weights or default_base_weights)
    features = build_satellite_relative_strength_features(
        prices,
        universe_config=universe_config,
        run_date=run_date,
    )
    scores = build_satellite_candidate_scores(
        features,
        universe_config=universe_config,
        policy_config=policy_config,
        run_date=run_date,
        ai_confirmation_payload=ai_confirmation_payload,
    )
    eligibility = build_replacement_eligibility_gate(
        scores,
        universe_config=universe_config,
        policy_config=policy_config,
        base_weights=base,
    )
    replacement_plan = build_etf_replacement_plan(
        run_date=run_date,
        base_weights=base,
        eligibility_records=eligibility,
        universe_config=universe_config,
        policy_config=policy_config,
    )
    experiment = build_satellite_shadow_portfolio_experiment(
        run_date=run_date,
        replacement_plan=replacement_plan,
        universe_config=universe_config,
        base_candidate_id="satellite_replacement_v1",
    )
    eligible = [row for row in eligibility if row.get("status") == "eligible"]
    fallback = [row for row in eligibility if row.get("fallback_to_etf") is True]
    return {
        "schema_version": SATELLITE_REPORT_SCHEMA_VERSION,
        "report_type": "satellite_replacement_report",
        "date": run_date.isoformat(),
        "market_regime": market_regime or "",
        "requested_date_range": dict(
            requested_date_range or {"start": "", "end": run_date.isoformat()}
        ),
        "safety_banner": _satellite_safety_banner(),
        "data_quality": {
            "status": data_quality_status,
            "report_path": data_quality_report or "",
        },
        "policy": {
            "universe_version": universe_config.policy_metadata.version,
            "universe_config_hash": universe_config.config_hash,
            "policy_version": policy_config.policy_metadata.version,
            "policy_hash": policy_config.config_hash,
        },
        "satellite_universe_summary": {
            "enabled_count": len(enabled_satellite_stocks(universe_config)),
            "optional_count": sum(
                1 for stock in enabled_satellite_stocks(universe_config) if stock.optional
            ),
            "benchmarks": sorted(
                {stock.benchmark_etf for stock in enabled_satellite_stocks(universe_config)}
            ),
        },
        "stock_vs_etf_features": features,
        "satellite_candidate_scores": scores,
        "replacement_eligibility": eligibility,
        "eligible_stocks": [row.get("ticker") for row in eligible],
        "watchlist": [row.get("ticker") for row in eligibility if row.get("status") == "watch"],
        "fallback_to_etf_stocks": [row.get("ticker") for row in fallback],
        "replacement_plan": replacement_plan,
        "shadow_experiment": experiment,
        "risk_constraints": policy_config.risk_constraints.model_dump(mode="json"),
        "ai_confirmation_context": _ai_confirmation_context(ai_confirmation_payload),
        "top_positive_drivers": _report_driver_summary(scores, "top_positive_drivers"),
        "top_negative_drivers": _report_driver_summary(scores, "top_negative_drivers"),
        "manual_review_note": (
            "Satellite replacement is candidate-only and requires manual review; "
            "official ETF target weights remain unchanged."
        ),
        **SATELLITE_SAFETY,
    }


def write_satellite_replacement_report(
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
    markdown_path.write_text(
        render_satellite_replacement_report_markdown(payload),
        encoding="utf-8",
    )
    return json_path, markdown_path


def latest_satellite_report_path(
    report_dir: Path = DEFAULT_SATELLITE_STANDALONE_REPORT_DIR,
    *,
    as_of: date | None = None,
) -> Path | None:
    if not report_dir.exists():
        return None
    candidates = sorted(report_dir.glob("satellite_replacement_report_*.json"))
    if as_of is not None:
        candidates = [path for path in candidates if path.stem.endswith(as_of.isoformat())]
    if not candidates:
        return None
    return max(candidates, key=lambda path: path.stat().st_mtime)


def build_satellite_policy_validation_report(
    *,
    universe_config: SatelliteUniverseConfig,
    policy_config: SatellitePolicyConfig,
    report_registry: Mapping[str, Any],
    reader_brief_available: bool,
    generated_at: str | None = None,
) -> dict[str, Any]:
    checks: list[dict[str, Any]] = []
    _append_validation_check(
        checks,
        "satellite_universe_config_valid",
        "PASS",
        "Satellite universe config is valid.",
        {"universe_version": universe_config.policy_metadata.version},
    )
    _append_validation_check(
        checks,
        "satellite_policy_config_valid",
        "PASS",
        "Satellite policy config is valid and threshold governance is explicit.",
        {"policy_version": policy_config.policy_metadata.version},
    )
    mappings_status, mappings_blockers = _mapping_validation(universe_config)
    _append_validation_check(
        checks,
        "stock_to_etf_mapping_valid",
        mappings_status,
        "Enabled satellite tickers map deterministically to benchmark ETF sleeves.",
        {"mapping_count": len(satellite_benchmark_mappings(universe_config))},
        mappings_blockers,
    )
    report_payload, toy_error = _toy_satellite_validation_payload(universe_config, policy_config)
    _append_validation_check(
        checks,
        "relative_strength_features_available",
        "PASS" if report_payload.get("stock_vs_etf_features") else "FAIL",
        "Stock-vs-ETF relative strength features can be built from deterministic fixture.",
        {"feature_count": len(_records(report_payload.get("stock_vs_etf_features")))},
        (
            []
            if report_payload.get("stock_vs_etf_features")
            else [toy_error or "FEATURE_BUILD_FAILED"]
        ),
    )
    _append_validation_check(
        checks,
        "candidate_score_available",
        "PASS" if report_payload.get("satellite_candidate_scores") else "FAIL",
        "SatelliteCandidateScore is available.",
        {"score_count": len(_records(report_payload.get("satellite_candidate_scores")))},
        [] if report_payload.get("satellite_candidate_scores") else ["CANDIDATE_SCORE_MISSING"],
    )
    _append_validation_check(
        checks,
        "replacement_eligibility_gate_available",
        "PASS" if report_payload.get("replacement_eligibility") else "FAIL",
        "Replacement eligibility gate is available and emits fallback statuses.",
        {
            "status_set": sorted(
                {
                    str(row.get("status"))
                    for row in _records(report_payload.get("replacement_eligibility"))
                }
            )
        },
        [] if report_payload.get("replacement_eligibility") else ["ELIGIBILITY_GATE_MISSING"],
    )
    plan = _mapping(report_payload.get("replacement_plan"))
    _append_validation_check(
        checks,
        "replacement_plan_generator_available",
        "PASS" if plan.get("candidate_weights") else "FAIL",
        "ETF replacement plan generator emits candidate/shadow/hypothetical weights.",
        {"replacement_plan_id": plan.get("replacement_plan_id")},
        _plan_validation_blockers(plan),
    )
    experiment = _mapping(report_payload.get("shadow_experiment"))
    _append_validation_check(
        checks,
        "satellite_shadow_experiment_available",
        "PASS" if experiment.get("after_candidate_weights") else "FAIL",
        "Satellite shadow portfolio experiment is available.",
        {"experiment_id": experiment.get("experiment_id")},
        _experiment_validation_blockers(experiment),
    )
    _append_validation_check(
        checks,
        "satellite_risk_constraints_available",
        "PASS" if policy_config.risk_constraints.max_total_satellite_weight > 0 else "FAIL",
        "Satellite risk constraints are configured.",
        policy_config.risk_constraints.model_dump(mode="json"),
    )
    _append_validation_check(
        checks,
        "satellite_report_available",
        "PASS" if report_payload else "FAIL",
        "Standalone satellite replacement report is available.",
        {"schema_version": report_payload.get("schema_version")},
        [] if report_payload else ["SATELLITE_REPORT_MISSING"],
    )
    _append_validation_check(
        checks,
        "reader_brief_section_available",
        "PASS" if reader_brief_available else "FAIL",
        "Reader Brief satellite section is wired.",
        {},
        [] if reader_brief_available else ["READER_BRIEF_SATELLITE_SECTION_UNAVAILABLE"],
    )
    registry_status, registry_blockers = _registry_validation(report_registry)
    _append_validation_check(
        checks,
        "report_registry_integration",
        registry_status,
        "Satellite artifacts are registered for report index discovery.",
        {"required_report_ids": sorted(_required_report_ids())},
        registry_blockers,
    )
    _append_validation_check(
        checks,
        "etf_fallback_behavior_present",
        "PASS" if _fallback_behavior_present(report_payload) else "FAIL",
        "ETF-first fallback behavior is present for failed or insufficient stock candidates.",
        {},
        [] if _fallback_behavior_present(report_payload) else ["ETF_FALLBACK_BEHAVIOR_MISSING"],
    )
    _append_validation_check(
        checks,
        "production_weights_not_mutated",
        "PASS" if "target_weights" not in json.dumps(report_payload, default=str) else "FAIL",
        "Satellite policy does not emit official target_weights fields.",
        {"production_weights_mutated": False},
        (
            []
            if "target_weights" not in json.dumps(report_payload, default=str)
            else ["PRODUCTION_WEIGHTS_MUTATED"]
        ),
    )
    _append_validation_check(
        checks,
        "safety_fields_locked",
        "PASS" if _satellite_payloads_safe(report_payload, plan, experiment) else "FAIL",
        "Safety fields remain observe-only/candidate-only/no broker action.",
        {"expected": SATELLITE_SAFETY},
        (
            []
            if _satellite_payloads_safe(report_payload, plan, experiment)
            else ["UNSAFE_SATELLITE_SAFETY_FIELDS"]
        ),
    )
    blockers = [blocker for check in checks for blocker in check["blockers"]]
    status = "PASS" if not blockers else "FAIL"
    return {
        "schema_version": SATELLITE_VALIDATION_SCHEMA_VERSION,
        "report_type": "satellite_policy_validation",
        "task": "TRADING-067K",
        "status": status,
        "generated_at": generated_at or datetime.now(UTC).isoformat(),
        "checks": checks,
        "blockers": blockers,
        "safe_for_shadow_replacement": status == "PASS",
        "production_weights_mutated": False,
        **SATELLITE_SAFETY,
    }


def write_satellite_policy_validation_report(
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
    markdown_path.write_text(render_satellite_policy_validation_markdown(payload), encoding="utf-8")
    return json_path, markdown_path


def render_satellite_replacement_report_markdown(payload: Mapping[str, Any]) -> str:
    plan = _mapping(payload.get("replacement_plan"))
    ai_context = _mapping(payload.get("ai_confirmation_context"))
    universe_summary = _mapping(payload.get("satellite_universe_summary"))
    lines = [
        "# Satellite Replacement Report",
        "",
        f"- Date: {payload.get('date')}",
        f"- Market Regime: {payload.get('market_regime') or 'unspecified'}",
        f"- Safety: {payload.get('safety_banner')}",
        f"- Data Quality: {_mapping(payload.get('data_quality')).get('status')}",
        "- Policy: ETF first; stock replacement only if eligibility gate passes.",
        "",
        "## Satellite Universe Summary",
        "",
        f"- Enabled Stocks: {universe_summary.get('enabled_count')}",
        f"- Optional Stocks: {universe_summary.get('optional_count')}",
        f"- Benchmarks: {_join_list(universe_summary.get('benchmarks'))}",
        "",
        "## Stock vs ETF Score Table",
        "",
        "| Ticker | Benchmark | Score | Band | Status | Fallback | Blockers |",
        "|---|---|---:|---|---|---|---|",
    ]
    eligibility = {
        row.get("ticker"): row for row in _records(payload.get("replacement_eligibility"))
    }
    for score in _records(payload.get("satellite_candidate_scores")):
        gate = eligibility.get(score.get("ticker"), {})
        lines.append(
            "| "
            f"{score.get('ticker')} | "
            f"{score.get('benchmark_etf')} | "
            f"{_fmt_number(score.get('score_value'))} | "
            f"{score.get('score_band')} | "
            f"{gate.get('status', 'missing')} | "
            f"{str(gate.get('fallback_to_etf', True)).lower()} | "
            f"{_join_list(gate.get('blockers'))} |"
        )
    lines.extend(
        [
            "",
            "## Replacement Plan Summary",
            "",
            f"- Plan ID: {plan.get('replacement_plan_id')}",
            f"- Total Replaced Weight: {_fmt_pct(plan.get('total_replaced_weight'))}",
            f"- Eligible Stocks: {_join_list(payload.get('eligible_stocks'))}",
            f"- Watchlist: {_join_list(payload.get('watchlist'))}",
            f"- Fallback To ETF: {_join_list(payload.get('fallback_to_etf_stocks'))}",
            f"- Constraints Applied: {_join_list(plan.get('constraints_applied'))}",
            "",
            "## Candidate Weights",
            "",
            "| Symbol | Base | Candidate |",
            "|---|---:|---:|",
        ]
    )
    base = _mapping(plan.get("base_weights"))
    candidate = _mapping(plan.get("candidate_weights"))
    for symbol in sorted(set(base) | set(candidate)):
        lines.append(
            f"| {symbol} | {_fmt_pct(base.get(symbol))} | {_fmt_pct(candidate.get(symbol))} |"
        )
    lines.extend(
        [
            "",
            "## AI Confirmation Context",
            "",
            f"- AIConfirmationScore: {_fmt_number(ai_context.get('score_value'))}",
            f"- Score Band: {ai_context.get('score_band')}",
            f"- Action Hint: {ai_context.get('action_hint')}",
            f"- Event Risk Score: {_fmt_number(ai_context.get('event_risk_score'))}",
            "",
            "## Drivers",
            "",
            f"- Top Positive Drivers: {_join_list(payload.get('top_positive_drivers'))}",
            f"- Top Negative Drivers: {_join_list(payload.get('top_negative_drivers'))}",
            "",
            "## Manual Review",
            "",
            str(payload.get("manual_review_note")),
        ]
    )
    return "\n".join(lines) + "\n"


def render_satellite_shadow_experiment_markdown(payload: Mapping[str, Any]) -> str:
    before = _mapping(payload.get("before_weights"))
    after = _mapping(payload.get("after_candidate_weights"))
    lines = [
        "# Satellite Shadow Portfolio Experiment",
        "",
        f"- Experiment ID: {payload.get('experiment_id')}",
        f"- Status: {payload.get('status')}",
        f"- Replacement Plan: {payload.get('replacement_plan_id')}",
        "- Safety: observe_only=true, candidate_only=true, production_effect=none, "
        "broker_action=none, manual_review_required=true",
        "",
        "## Candidate Weights",
        "",
        "| Symbol | Before | After Candidate |",
        "|---|---:|---:|",
    ]
    for symbol in sorted(set(before) | set(after)):
        lines.append(
            f"| {symbol} | {_fmt_pct(before.get(symbol))} | " f"{_fmt_pct(after.get(symbol))} |"
        )
    lines.extend(
        [
            "",
            "## Experiment Context",
            "",
            f"- Eligible Satellites: {_join_list(payload.get('eligible_satellites'))}",
            f"- Fallback Symbols: {_join_list(payload.get('fallback_to_etf_symbols'))}",
            f"- Constraints Applied: {_join_list(payload.get('constraints_applied'))}",
            "- Future returns are evaluation-only and are not used by the replacement gate.",
        ]
    )
    return "\n".join(lines) + "\n"


def render_satellite_policy_validation_markdown(payload: Mapping[str, Any]) -> str:
    lines = [
        "# Satellite Policy Validation Gate",
        "",
        f"- Task: {payload.get('task')}",
        f"- Status: {payload.get('status')}",
        "- Safety: observe_only=true, candidate_only=true, production_effect=none, "
        "broker_action=none, manual_review_required=true",
        f"- Production Weights Mutated: {payload.get('production_weights_mutated')}",
        "",
        "## Checks",
        "",
        "| Check | Status | Summary | Blockers |",
        "|---|---|---|---|",
    ]
    for check in _records(payload.get("checks")):
        lines.append(
            "| "
            f"{check.get('check_id')} | "
            f"{check.get('status')} | "
            f"{check.get('summary')} | "
            f"{_join_list(check.get('blockers'))} |"
        )
    return "\n".join(lines) + "\n"


def satellite_score_band(score: float, policy_config: SatellitePolicyConfig) -> str:
    for band, config in sorted(
        policy_config.score_bands.items(),
        key=lambda item: item[1].min_score,
        reverse=True,
    ):
        if score >= config.min_score:
            return band
    return "fallback"


def _satellite_feature_row(
    frame: pd.DataFrame,
    stock: SatelliteStockConfig,
    run_date: date,
) -> dict[str, Any]:
    stock_history = _symbol_history(frame, stock.ticker)
    benchmark_history = _symbol_history(frame, stock.benchmark_etf)
    feature_date = _common_feature_date(stock_history, benchmark_history, run_date)
    if feature_date is None:
        return _missing_feature_row(stock, run_date, "PRICE_HISTORY_MISSING")
    stock_history = stock_history.loc[stock_history["_date"] <= pd.Timestamp(feature_date)]
    benchmark_history = benchmark_history.loc[
        benchmark_history["_date"] <= pd.Timestamp(feature_date)
    ]
    coverage = min(
        1.0,
        len(stock_history) / float(max(STOCK_MA_WINDOWS) + 1),
        len(benchmark_history) / float(max(STOCK_MA_WINDOWS) + 1),
    )
    row: dict[str, Any] = {
        "date": run_date.isoformat(),
        "feature_date": feature_date.isoformat(),
        "score_date": run_date.isoformat(),
        "earliest_execution_date": (run_date + timedelta(days=1)).isoformat(),
        "ticker": stock.ticker,
        "benchmark_etf": stock.benchmark_etf,
        "sleeve": stock.sleeve,
        "role": stock.role,
        "optional": stock.optional,
        "data_coverage_ratio": round(coverage, 10),
        "fallback_to_etf": coverage < stock.min_data_coverage,
        "reason_codes": [],
        **SATELLITE_SAFETY,
    }
    for window in FEATURE_RETURN_WINDOWS:
        stock_return = _series_return(stock_history["_adj_close"], window=window)
        benchmark_return = _series_return(benchmark_history["_adj_close"], window=window)
        row[f"stock_return_{window}d"] = stock_return
        row[f"benchmark_return_{window}d"] = benchmark_return
        row[f"relative_return_{window}d"] = _relative_return(stock_return, benchmark_return)
    for window in STOCK_MA_WINDOWS:
        row[f"stock_above_{window}d_ma"] = _above_moving_average(
            stock_history["_adj_close"],
            window=window,
        )
    for window in BENCHMARK_MA_WINDOWS:
        row[f"benchmark_above_{window}d_ma"] = _above_moving_average(
            benchmark_history["_adj_close"],
            window=window,
        )
    ratio = _relative_ratio(stock_history, benchmark_history)
    for window in RELATIVE_MA_WINDOWS:
        row[f"relative_price_above_{window}d_ma"] = _above_moving_average(ratio, window=window)
    stock_drawdown = _series_drawdown(stock_history["_adj_close"], window=DRAW_DOWN_WINDOW)
    benchmark_drawdown = _series_drawdown(
        benchmark_history["_adj_close"],
        window=DRAW_DOWN_WINDOW,
    )
    row["stock_drawdown_from_60d_high"] = stock_drawdown
    row["benchmark_drawdown_from_60d_high"] = benchmark_drawdown
    row["relative_drawdown"] = (
        None
        if stock_drawdown is None or benchmark_drawdown is None
        else stock_drawdown - benchmark_drawdown
    )
    stock_vol = _realized_volatility(stock_history["_adj_close"], window=VOLATILITY_WINDOW)
    benchmark_vol = _realized_volatility(
        benchmark_history["_adj_close"],
        window=VOLATILITY_WINDOW,
    )
    row["stock_realized_vol_20d"] = stock_vol
    row["benchmark_realized_vol_20d"] = benchmark_vol
    row["relative_volatility"] = (
        None if stock_vol is None or benchmark_vol in (None, 0.0) else stock_vol / benchmark_vol
    )
    missing_core = [
        key
        for key in ("relative_return_60d", "stock_drawdown_from_60d_high", "relative_volatility")
        if row.get(key) is None
    ]
    if missing_core:
        row["reason_codes"] = [f"FEATURE_MISSING:{key}" for key in missing_core]
        row["feature_status"] = "insufficient_data"
        row["fallback_to_etf"] = True
    elif coverage < stock.min_data_coverage:
        row["reason_codes"] = ["DATA_COVERAGE_BELOW_STOCK_MINIMUM"]
        row["feature_status"] = "insufficient_data"
    else:
        row["reason_codes"] = ["FEATURES_AVAILABLE"]
        row["feature_status"] = "available"
    return row


def _satellite_score_row(
    feature: Mapping[str, Any],
    *,
    stock: SatelliteStockConfig,
    policy_config: SatellitePolicyConfig,
    run_date: date,
    ai_context: Mapping[str, Any],
) -> dict[str, Any]:
    policy = policy_config.score_policy
    relative_returns = [
        _float_or_none(feature.get(f"relative_return_{window}d"))
        for window in FEATURE_RETURN_WINDOWS
    ]
    stock_returns = [
        _float_or_none(feature.get(f"stock_return_{window}d")) for window in FEATURE_RETURN_WINDOWS
    ]
    component_scores = {
        "relative_strength_score": _return_component(
            relative_returns,
            full_scale=policy.relative_return_full_scale,
        ),
        "trend_score": _trend_component(feature),
        "momentum_score": _return_component(
            stock_returns,
            full_scale=policy.momentum_return_full_scale,
        ),
        "drawdown_risk_score": _drawdown_component(
            feature.get("stock_drawdown_from_60d_high"),
            full_penalty=policy.drawdown_full_penalty,
        ),
        "volatility_score": _volatility_component(
            feature.get("relative_volatility"),
            warning=policy.relative_volatility_warning,
            block=policy.relative_volatility_block,
        ),
        "ai_confirmation_support_score": _clamp_score(
            _float_or_none(ai_context.get("score_value")) or 50.0
        ),
        "event_risk_adjusted_score": _clamp_score(
            100.0 - (_float_or_none(ai_context.get("event_risk_score")) or 0.0)
        ),
        "data_coverage_score": _clamp_score(
            (_float_or_none(feature.get("data_coverage_ratio")) or 0.0) * 100.0
        ),
    }
    score_value = round(
        _clamp_score(
            sum(
                component_scores[component] * float(policy.component_weights[component])
                for component in policy.component_weights
            )
        ),
        2,
    )
    component_output = {
        component: round(float(value), 2) for component, value in component_scores.items()
    }
    return {
        "date": run_date.isoformat(),
        "ticker": stock.ticker,
        "benchmark_etf": stock.benchmark_etf,
        "sleeve": stock.sleeve,
        "role": stock.role,
        "score_name": "SatelliteCandidateScore",
        "score_value": score_value,
        "score_band": satellite_score_band(score_value, policy_config),
        "component_scores": component_output,
        "top_positive_drivers": _driver_list(
            component_output,
            positive=True,
            threshold=policy.driver_positive_min,
        ),
        "top_negative_drivers": _driver_list(
            component_output,
            positive=False,
            threshold=policy.driver_negative_max,
        ),
        "data_coverage_ratio": feature.get("data_coverage_ratio"),
        "relative_return_60d": feature.get("relative_return_60d"),
        "stock_drawdown_from_60d_high": feature.get("stock_drawdown_from_60d_high"),
        "relative_volatility": feature.get("relative_volatility"),
        "stock_above_50d_ma": feature.get("stock_above_50d_ma"),
        "stock_above_200d_ma": feature.get("stock_above_200d_ma"),
        "relative_price_above_50d_ma": feature.get("relative_price_above_50d_ma"),
        "ai_confirmation_score": ai_context.get("score_value"),
        "ai_confirmation_band": ai_context.get("score_band"),
        "ai_confirmation_action_hint": ai_context.get("action_hint"),
        "event_risk_score": ai_context.get("event_risk_score"),
        "policy_version": policy_config.policy_metadata.version,
        "policy_config_hash": policy_config.config_hash,
        **SATELLITE_SAFETY,
    }


def _eligibility_row(
    score: Mapping[str, Any],
    *,
    stock: SatelliteStockConfig,
    policy_config: SatellitePolicyConfig,
    base_weights: Mapping[str, float],
) -> dict[str, Any]:
    policy = policy_config.eligibility_policy
    constraints = policy_config.risk_constraints
    blockers: list[str] = []
    score_value = _float_or_none(score.get("score_value")) or 0.0
    relative_return_60d = _float_or_none(score.get("relative_return_60d"))
    drawdown = _float_or_none(score.get("stock_drawdown_from_60d_high"))
    relative_volatility = _float_or_none(score.get("relative_volatility"))
    data_coverage = _float_or_none(score.get("data_coverage_ratio")) or 0.0
    component_scores = _mapping(score.get("component_scores"))
    trend_score = _float_or_none(component_scores.get("trend_score")) or 0.0
    ai_score = _float_or_none(score.get("ai_confirmation_score")) or 50.0
    event_risk = _float_or_none(score.get("event_risk_score")) or 0.0
    if data_coverage < max(policy.min_data_coverage, stock.min_data_coverage):
        blockers.append("INSUFFICIENT_DATA")
    if relative_return_60d is None or relative_return_60d <= policy.relative_return_60d_min:
        blockers.append("LOW_RELATIVE_STRENGTH")
    if trend_score < policy.trend_score_min:
        blockers.append("NEGATIVE_TREND")
    if drawdown is None or drawdown < policy.max_drawdown:
        blockers.append("HIGH_DRAWDOWN")
    if relative_volatility is None or relative_volatility > policy.max_relative_volatility:
        blockers.append("HIGH_RELATIVE_VOLATILITY")
    if event_risk >= policy.event_risk_high_min:
        blockers.append("HIGH_EVENT_RISK")
    if ai_score < policy.ai_confirmation_neutral_min:
        blockers.append("AI_CONFIRMATION_WEAK")
    if stock.max_single_name_weight > constraints.max_single_stock_weight + 1e-12:
        blockers.append("SINGLE_NAME_CAP_EXCEEDED")
    if base_weights and base_weights.get(stock.benchmark_etf, 0.0) <= 0:
        blockers.append("SLEEVE_CAP_EXCEEDED")
    for key, expected in SATELLITE_SAFETY.items():
        if score.get(key) != expected:
            blockers.append(
                "UNSAFE_PRODUCTION_EFFECT"
                if key == "production_effect"
                else "BROKER_ACTION_NOT_NONE" if key == "broker_action" else f"UNSAFE_{key.upper()}"
            )

    blocker_set = sorted(set(blockers))
    if "INSUFFICIENT_DATA" in blocker_set:
        status = "insufficient_data"
    elif any(
        blocker in blocker_set
        for blocker in (
            "SINGLE_NAME_CAP_EXCEEDED",
            "SLEEVE_CAP_EXCEEDED",
            "UNSAFE_PRODUCTION_EFFECT",
            "BROKER_ACTION_NOT_NONE",
        )
    ):
        status = "blocked"
    elif not blocker_set and score_value >= policy.eligible_score_min:
        status = "eligible"
    elif score_value >= policy.watch_score_min:
        status = "watch"
    else:
        status = "fallback_to_etf"
    fallback_to_etf = status != "eligible"
    return {
        "date": score.get("date"),
        "ticker": stock.ticker,
        "benchmark_etf": stock.benchmark_etf,
        "sleeve": stock.sleeve,
        "role": stock.role,
        "status": status,
        "score_value": score_value,
        "score_band": score.get("score_band"),
        "blockers": blocker_set,
        "reason_codes": blocker_set or ["SATELLITE_REPLACEMENT_ELIGIBLE"],
        "fallback_to_etf": fallback_to_etf,
        "manual_review_required": True,
        **SATELLITE_SAFETY,
    }


def _missing_feature_row(
    stock: SatelliteStockConfig,
    run_date: date,
    reason: str,
) -> dict[str, Any]:
    return {
        "date": run_date.isoformat(),
        "feature_date": "",
        "score_date": run_date.isoformat(),
        "earliest_execution_date": (run_date + timedelta(days=1)).isoformat(),
        "ticker": stock.ticker,
        "benchmark_etf": stock.benchmark_etf,
        "sleeve": stock.sleeve,
        "role": stock.role,
        "optional": stock.optional,
        "data_coverage_ratio": 0.0,
        "feature_status": "insufficient_data",
        "fallback_to_etf": True,
        "reason_codes": [reason],
        **{f"stock_return_{window}d": None for window in FEATURE_RETURN_WINDOWS},
        **{f"benchmark_return_{window}d": None for window in FEATURE_RETURN_WINDOWS},
        **{f"relative_return_{window}d": None for window in FEATURE_RETURN_WINDOWS},
        **{f"stock_above_{window}d_ma": None for window in STOCK_MA_WINDOWS},
        **{f"benchmark_above_{window}d_ma": None for window in BENCHMARK_MA_WINDOWS},
        **{f"relative_price_above_{window}d_ma": None for window in RELATIVE_MA_WINDOWS},
        "stock_drawdown_from_60d_high": None,
        "benchmark_drawdown_from_60d_high": None,
        "relative_drawdown": None,
        "stock_realized_vol_20d": None,
        "benchmark_realized_vol_20d": None,
        "relative_volatility": None,
        **SATELLITE_SAFETY,
    }


def _prepare_price_history(prices: pd.DataFrame, run_date: date) -> pd.DataFrame:
    required = {"date", "symbol", "adj_close"}
    missing = sorted(required - set(prices.columns))
    if missing:
        raise ValueError(f"satellite prices missing columns: {', '.join(missing)}")
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


def _symbol_history(frame: pd.DataFrame, symbol: str) -> pd.DataFrame:
    return frame.loc[frame["symbol"] == symbol].sort_values("_date").reset_index(drop=True)


def _common_feature_date(
    stock_history: pd.DataFrame,
    benchmark_history: pd.DataFrame,
    run_date: date,
) -> date | None:
    if stock_history.empty or benchmark_history.empty:
        return None
    stock_dates = set(pd.to_datetime(stock_history["_date"]).dt.date)
    benchmark_dates = set(pd.to_datetime(benchmark_history["_date"]).dt.date)
    common = sorted(item for item in stock_dates & benchmark_dates if item <= run_date)
    return common[-1] if common else None


def _relative_ratio(stock_history: pd.DataFrame, benchmark_history: pd.DataFrame) -> pd.Series:
    left = stock_history[["_date", "_adj_close"]].rename(columns={"_adj_close": "stock"})
    right = benchmark_history[["_date", "_adj_close"]].rename(columns={"_adj_close": "benchmark"})
    merged = left.merge(right, on="_date", how="inner").sort_values("_date")
    ratio = merged["stock"] / merged["benchmark"]
    return ratio.replace([np.inf, -np.inf], np.nan).dropna().reset_index(drop=True)


def _series_return(series: pd.Series, *, window: int) -> float | None:
    values = pd.to_numeric(series, errors="coerce").dropna().reset_index(drop=True)
    if len(values) <= window:
        return None
    return _safe_float(values.iloc[-1] / values.iloc[-window - 1] - 1.0)


def _relative_return(left: float | None, right: float | None) -> float | None:
    if left is None or right is None:
        return None
    return left - right


def _above_moving_average(series: pd.Series, *, window: int) -> bool | None:
    values = pd.to_numeric(series, errors="coerce").dropna().reset_index(drop=True)
    if len(values) < window:
        return None
    moving_average = values.rolling(window=window, min_periods=window).mean().iloc[-1]
    if not _is_finite_number(moving_average):
        return None
    return bool(values.iloc[-1] > moving_average)


def _series_drawdown(series: pd.Series, *, window: int) -> float | None:
    values = pd.to_numeric(series, errors="coerce").dropna().reset_index(drop=True)
    if len(values) < window:
        return None
    recent = values.tail(window)
    high = recent.max()
    if not _is_finite_number(high) or float(high) <= 0:
        return None
    return _safe_float(recent.iloc[-1] / high - 1.0)


def _realized_volatility(series: pd.Series, *, window: int) -> float | None:
    values = pd.to_numeric(series, errors="coerce").dropna().reset_index(drop=True)
    if len(values) <= window:
        return None
    returns = values.pct_change().dropna().tail(window)
    if returns.empty:
        return None
    return _safe_float(returns.std() * np.sqrt(252))


def _return_component(values: Iterable[float | None], *, full_scale: float) -> float:
    parsed = [float(value) for value in values if _is_finite_number(value)]
    if not parsed:
        return 50.0
    average = float(np.mean(parsed))
    return _clamp_score(50.0 + (average / full_scale) * 50.0)


def _trend_component(feature: Mapping[str, Any]) -> float:
    values = [
        feature.get("stock_above_50d_ma"),
        feature.get("stock_above_200d_ma"),
        feature.get("relative_price_above_50d_ma"),
        feature.get("relative_price_above_200d_ma"),
    ]
    scores = [100.0 if value is True else 0.0 for value in values if isinstance(value, bool)]
    if not scores:
        return 50.0
    return _clamp_score(float(np.mean(scores)))


def _drawdown_component(value: Any, *, full_penalty: float) -> float:
    drawdown = _float_or_none(value)
    if drawdown is None:
        return 50.0
    if drawdown >= 0:
        return 100.0
    capped = min(abs(drawdown), abs(full_penalty))
    return _clamp_score(100.0 * (1.0 - capped / abs(full_penalty)))


def _volatility_component(value: Any, *, warning: float, block: float) -> float:
    relative_vol = _float_or_none(value)
    if relative_vol is None:
        return 50.0
    if relative_vol <= warning:
        return 100.0
    if relative_vol >= block:
        return 0.0
    return _clamp_score(100.0 * (1.0 - (relative_vol - warning) / (block - warning)))


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
            if float(score) >= threshold
        ]
        selected.sort(key=lambda item: item[1], reverse=True)
    else:
        selected = [
            (component, score)
            for component, score in component_scores.items()
            if float(score) <= threshold
        ]
        selected.sort(key=lambda item: item[1])
    return [f"{component}={score:.2f}" for component, score in selected[:3]] or ["none"]


def _ai_confirmation_context(payload: Mapping[str, Any] | None) -> dict[str, Any]:
    if not payload:
        return {
            "score_value": 50.0,
            "score_band": "neutral",
            "action_hint": "neutral_missing_ai_confirmation_report",
            "event_risk_score": 0.0,
            "source": "neutral_default_missing_ai_confirmation",
        }
    score = _mapping(payload.get("AIConfirmationScore"))
    if not score and "score_value" in payload:
        score = payload
    event_risk = _mapping(payload.get("event_risk_overlay"))
    return {
        "score_value": _float_or_none(score.get("score_value")) or 50.0,
        "score_band": str(score.get("score_band") or "neutral"),
        "action_hint": str(score.get("action_hint") or "neutral"),
        "event_risk_score": _float_or_none(event_risk.get("event_risk_score"))
        or _event_risk_from_score(score),
        "source": str(payload.get("report_type") or "ai_confirmation_payload"),
    }


def _event_risk_from_score(score: Mapping[str, Any]) -> float:
    components = _mapping(score.get("component_scores"))
    adjustment = _float_or_none(components.get("event_risk_adjustment"))
    if adjustment is None:
        return 0.0
    return _clamp_score(100.0 - adjustment)


def _fallback_position(row: Mapping[str, Any], reason: str) -> dict[str, Any]:
    return {
        "ticker": row.get("ticker"),
        "benchmark_etf": row.get("benchmark_etf"),
        "sleeve": row.get("sleeve"),
        "fallback_to_etf": True,
        "reason": reason,
        "blockers": list(row.get("blockers", [])),
    }


def _normalize_weight_mapping(weights: Mapping[str, float]) -> dict[str, float]:
    normalized = {str(symbol).strip().upper(): float(weight) for symbol, weight in weights.items()}
    if not normalized:
        return {}
    if any(weight < -1e-12 for weight in normalized.values()):
        raise ValueError("satellite base weights must be non-negative")
    total = sum(normalized.values())
    if total <= 0:
        raise ValueError("satellite base weights must have positive total weight")
    if abs(total - 1.0) > 1e-6:
        raise ValueError(f"satellite base weights must sum to 1.0: {total:.8f}")
    return normalized


def _normalize_rounding(weights: dict[str, float]) -> None:
    for symbol, weight in list(weights.items()):
        if abs(weight) <= 1e-12:
            weights[symbol] = 0.0
    total = sum(weights.values())
    if abs(total - 1.0) <= 1e-8:
        return
    cash_symbol = "CASH" if "CASH" in weights else sorted(weights)[0]
    weights[cash_symbol] = weights.get(cash_symbol, 0.0) + (1.0 - total)


def _round_weights(weights: Mapping[str, float]) -> dict[str, float]:
    return {
        symbol: round(float(weight), 10)
        for symbol, weight in sorted(weights.items())
        if abs(float(weight)) > 1e-12 or symbol in {"SPY", "QQQ", "SMH", "SOXX", "CASH"}
    }


def _report_driver_summary(scores: list[dict[str, Any]], key: str) -> list[str]:
    rows: list[str] = []
    for score in sorted(scores, key=lambda item: -float(item.get("score_value", 0.0)))[:5]:
        rows.append(f"{score.get('ticker')}:{_join_list(score.get(key))}")
    return rows or ["none"]


def _mapping_validation(config: SatelliteUniverseConfig) -> tuple[str, list[str]]:
    blockers: list[str] = []
    for expected_ticker, expected_benchmark in {
        "NVDA": "SMH",
        "AMD": "SMH",
        "TSM": "SMH",
        "MSFT": "QQQ",
    }.items():
        try:
            mapping = stock_benchmark_mapping(config, expected_ticker)
        except KeyError as exc:
            blockers.append(f"MAPPING_MISSING:{expected_ticker}:{exc}")
            continue
        if mapping["benchmark_etf"] != expected_benchmark:
            blockers.append(
                f"MAPPING_BENCHMARK_MISMATCH:{expected_ticker}:{mapping['benchmark_etf']}"
            )
    return ("PASS" if not blockers else "FAIL", blockers)


def _plan_validation_blockers(plan: Mapping[str, Any]) -> list[str]:
    blockers: list[str] = []
    for key in ("candidate_weights", "shadow_weights", "hypothetical_weights", "replacement_plan"):
        if not plan.get(key):
            blockers.append(f"PLAN_{key.upper()}_MISSING")
    if "target_weights" in plan:
        blockers.append("PLAN_MUTATES_TARGET_WEIGHTS")
    for safety_key, expected in SATELLITE_SAFETY.items():
        if plan.get(safety_key) != expected:
            blockers.append(f"PLAN_UNSAFE_{safety_key.upper()}")
    return blockers


def _experiment_validation_blockers(experiment: Mapping[str, Any]) -> list[str]:
    blockers: list[str] = []
    if "target_weights" in experiment:
        blockers.append("EXPERIMENT_MUTATES_TARGET_WEIGHTS")
    if not experiment.get("candidate_weights"):
        blockers.append("EXPERIMENT_CANDIDATE_WEIGHTS_MISSING")
    for safety_key, expected in SATELLITE_SAFETY.items():
        if experiment.get(safety_key) != expected:
            blockers.append(f"EXPERIMENT_UNSAFE_{safety_key.upper()}")
    return blockers


def _registry_validation(report_registry: Mapping[str, Any]) -> tuple[str, list[str]]:
    registry_ids = {
        str(item.get("report_id"))
        for item in report_registry.get("reports", [])
        if isinstance(item, Mapping)
    }
    missing = sorted(_required_report_ids() - registry_ids)
    blockers = [f"REPORT_REGISTRY_MISSING:{report_id}" for report_id in missing]
    return ("PASS" if not blockers else "FAIL", blockers)


def _required_report_ids() -> set[str]:
    return {
        "etf_satellite_features",
        "etf_satellite_replacement_report",
        "etf_satellite_shadow_experiment",
        "etf_satellite_validation",
    }


def _fallback_behavior_present(report_payload: Mapping[str, Any]) -> bool:
    eligibility = _records(report_payload.get("replacement_eligibility"))
    return any(row.get("fallback_to_etf") is True for row in eligibility) or bool(
        _records(_mapping(report_payload.get("replacement_plan")).get("fallback_positions"))
    )


def _satellite_payloads_safe(*payloads: Mapping[str, Any]) -> bool:
    material = [payload for payload in payloads if payload]
    if not material:
        return False
    return all(
        all(payload.get(key) == expected for key, expected in SATELLITE_SAFETY.items())
        for payload in material
    )


def _append_validation_check(
    checks: list[dict[str, Any]],
    check_id: str,
    status: str,
    summary: str,
    details: Mapping[str, Any] | None = None,
    blockers: list[str] | None = None,
) -> None:
    checks.append(
        {
            "check_id": check_id,
            "status": status,
            "summary": summary,
            "details": dict(details or {}),
            "blockers": list(blockers or []),
        }
    )


def _toy_satellite_validation_payload(
    universe_config: SatelliteUniverseConfig,
    policy_config: SatellitePolicyConfig,
) -> tuple[dict[str, Any], str]:
    run_date = date(2026, 6, 1)
    symbols = sorted(set(satellite_price_symbols(universe_config)) | {"SPY", "CASH"})
    prices = _toy_satellite_prices(symbols, run_date=run_date)
    ai_payload = {
        "report_type": "ai_confirmation_report",
        "AIConfirmationScore": {
            "score_value": 66.0,
            "score_band": "confirm",
            "action_hint": "supports_neutral_ai_exposure",
        },
        "event_risk_overlay": {"event_risk_score": 15.0, "risk_band": "low"},
    }
    try:
        return (
            build_satellite_replacement_report(
                prices=prices,
                universe_config=universe_config,
                policy_config=policy_config,
                run_date=run_date,
                data_quality_status="STRUCTURAL_VALIDATION_FIXTURE",
                data_quality_report="",
                base_weights={"SPY": 0.25, "QQQ": 0.45, "SMH": 0.20, "SOXX": 0.0, "CASH": 0.10},
                ai_confirmation_payload=ai_payload,
                market_regime="ai_after_chatgpt",
                requested_date_range={"start": "2025-09-01", "end": run_date.isoformat()},
            ),
            "",
        )
    except Exception as exc:  # pragma: no cover - validation tests monkeypatch failures.
        return {}, f"{type(exc).__name__}:{exc}"


def _toy_satellite_prices(symbols: list[str], *, run_date: date) -> pd.DataFrame:
    start_date = date(2025, 9, 1)
    days = (run_date - start_date).days + 1
    rows: list[dict[str, Any]] = []
    for day_index in range(days):
        current_date = (pd.Timestamp(start_date) + pd.Timedelta(days=day_index)).date()
        for symbol in symbols:
            base = 100.0 + (sum(ord(char) for char in symbol) % 17)
            if symbol in {"SMH", "QQQ", "SPY", "SOXX"}:
                slope = 0.18 + (sum(ord(char) for char in symbol) % 5) / 100.0
            elif symbol in {"NVDA", "AVGO", "MSFT"}:
                slope = 0.36
            elif symbol in {"AMD", "TSM"}:
                slope = 0.16
            else:
                slope = 0.20 + (sum(ord(char) for char in symbol) % 7) / 100.0
            price = base + slope * day_index
            rows.append(
                {
                    "date": current_date.isoformat(),
                    "symbol": symbol,
                    "adj_close": price,
                    "close": price,
                    "open": price,
                    "high": price * 1.01,
                    "low": price * 0.99,
                    "volume": 1_000_000 + day_index,
                }
            )
    return pd.DataFrame(rows)


def _dedupe_stocks(stocks: Iterable[SatelliteStockConfig]) -> list[SatelliteStockConfig]:
    selected: dict[str, SatelliteStockConfig] = {}
    for stock in stocks:
        current = selected.get(stock.ticker)
        if current is None:
            selected[stock.ticker] = stock
            continue
        if current.optional and not stock.optional:
            selected[stock.ticker] = stock
    return [selected[ticker] for ticker in sorted(selected)]


def _satellite_safety_banner() -> str:
    return (
        "observe_only=true; candidate_only=true; production_effect=none; "
        "broker_action=none; manual_review_required=true"
    )


def _config_hash(payload: Mapping[str, Any]) -> str:
    normalized = json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str)
    return sha256(normalized.encode("utf-8")).hexdigest()


def _records(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [dict(item) for item in value if isinstance(item, Mapping)]


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _join_list(value: Any) -> str:
    if value is None:
        return "none"
    if isinstance(value, list | tuple | set):
        values = [str(item) for item in value if str(item)]
    else:
        values = [str(value)]
    return ", ".join(values) if values else "none"


def _fmt_number(value: Any) -> str:
    parsed = _float_or_none(value)
    if parsed is None:
        return "-"
    return f"{parsed:.2f}"


def _fmt_pct(value: Any) -> str:
    parsed = _float_or_none(value)
    if parsed is None:
        return "-"
    return f"{parsed:.1%}"


def _float_or_none(value: Any) -> float | None:
    if not _is_finite_number(value):
        return None
    return float(value)


def _safe_float(value: Any) -> float | None:
    return _float_or_none(value)


def _is_finite_number(value: Any) -> bool:
    if value is None:
        return False
    try:
        return bool(np.isfinite(float(value)))
    except (TypeError, ValueError):
        return False


def _clamp_score(value: float) -> float:
    return min(100.0, max(0.0, float(value)))
