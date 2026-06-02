from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime
from hashlib import sha256
from math import prod, sqrt
from pathlib import Path
from statistics import mean, pstdev
from typing import Any, Self

import pandas as pd
from pydantic import BaseModel, Field, model_validator

from ai_trading_system.backtest.engine import BacktestMetrics, summarize_long_only_backtest
from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.etf_portfolio.backtest import calculate_portfolio_accounting_step
from ai_trading_system.etf_portfolio.models import (
    ETFConfigBundle,
    ETFQualityReport,
    PolicyMetadata,
    load_etf_config_bundle,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

DEFAULT_ETF_WEIGHT_SEARCH_CONFIG_PATH = (
    PROJECT_ROOT / "config" / "etf_portfolio" / "weight_search.yaml"
)
DEFAULT_ETF_WEIGHT_CALIBRATION_REPORT_DIR = (
    PROJECT_ROOT / "reports" / "etf_portfolio" / "weight_calibration"
)
DEFAULT_ETF_WEIGHT_CALIBRATION_DATA_DIR = (
    PROJECT_ROOT / "data" / "etf_portfolio" / "weight_calibration"
)
DEFAULT_CANDIDATE_WEIGHT_REGISTRY_PATH = (
    DEFAULT_ETF_WEIGHT_CALIBRATION_DATA_DIR / "candidate_weight_registry.json"
)
DEFAULT_WEIGHT_FORWARD_ENROLLMENT_PATH = (
    DEFAULT_ETF_WEIGHT_CALIBRATION_DATA_DIR / "forward_enrollments.json"
)
DEFAULT_WEIGHT_FORWARD_EVIDENCE_DIR = (
    DEFAULT_ETF_WEIGHT_CALIBRATION_REPORT_DIR / "evidence"
)
DEFAULT_WEIGHT_OVERFIT_DIAGNOSTICS_DIR = (
    DEFAULT_ETF_WEIGHT_CALIBRATION_REPORT_DIR / "overfit_diagnostics"
)

WEIGHT_SEARCH_SCHEMA_VERSION = "etf_weight_search_v1"
WEIGHT_SEARCH_RUN_SCHEMA_VERSION = "etf_weight_search_run_v1"
CANDIDATE_WEIGHT_REGISTRY_SCHEMA_VERSION = "etf_candidate_weight_registry_v1"
WEIGHT_FORWARD_ENROLLMENT_SCHEMA_VERSION = "etf_weight_forward_enrollment_v1"
WEIGHT_BACKTEST_FORWARD_EVIDENCE_SCHEMA_VERSION = (
    "etf_weight_backtest_forward_evidence_v1"
)
WEIGHT_OVERFIT_DIAGNOSTICS_SCHEMA_VERSION = "etf_weight_overfit_diagnostics_v1"

WEIGHT_CALIBRATION_SAFETY = {
    "observe_only": True,
    "candidate_only": True,
    "production_effect": "none",
    "broker_action": "none",
    "manual_review_required": True,
}

ALLOWED_CANDIDATE_WEIGHT_STATUSES = frozenset(
    {"candidate", "shadow_ready", "blocked", "rejected", "needs_more_data"}
)
FORWARD_ENROLLABLE_CANDIDATE_STATUSES = frozenset({"candidate", "shadow_ready"})
ALLOWED_WEIGHT_FORWARD_ENROLLMENT_STATUSES = frozenset(
    {"active", "needs_more_forward_data", "paused", "archived"}
)
WEIGHT_FORWARD_EVIDENCE_STATUSES = frozenset(
    {
        "consistent",
        "forward_better_than_backtest",
        "forward_worse_than_backtest",
        "needs_more_forward_data",
        "mixed",
        "blocked",
    }
)

# Pilot comparison policy for TRADING-071F evidence triage only. These thresholds do
# not approve baseline changes; TRADING-071G/H/K add later risk/proposal gates.
WEIGHT_FORWARD_EVIDENCE_POLICY = {
    "policy_id": "etf_weight_backtest_forward_evidence_v1",
    "owner": "TRADING-071F",
    "status": "pilot_baseline",
    "rationale": (
        "Compare historical expectations with real forward evidence without treating "
        "either side as production approval."
    ),
    "min_forward_days": 20,
    "return_gap_tolerance": 0.02,
    "drawdown_gap_tolerance": 0.02,
    "turnover_gap_tolerance": 0.25,
    "stability_gap_tolerance": 0.10,
}

# Pilot diagnostics policy for TRADING-071G. Scores are explainability/risk triage
# inputs only and cannot promote or apply ETF weights.
WEIGHT_OVERFIT_DIAGNOSTICS_POLICY = {
    "policy_id": "etf_weight_overfit_diagnostics_v1",
    "owner": "TRADING-071G",
    "status": "pilot_baseline",
    "rationale": (
        "Flag historically attractive ETF weight candidates that may be unstable, "
        "over-concentrated, benchmark-dependent, or diverging forward."
    ),
    "component_weights": {
        "performance_concentration": 0.15,
        "single_period_dependency": 0.15,
        "regime_fragility": 0.15,
        "turnover_instability": 0.10,
        "constraint_hit_instability": 0.10,
        "weight_extremeness": 0.10,
        "benchmark_dependency": 0.10,
        "forward_backtest_divergence": 0.15,
    },
    "risk_band_thresholds": {
        "medium": 0.25,
        "high": 0.50,
        "critical": 0.75,
    },
    "turnover_reference": 1.0,
    "constraint_hit_reference": 0.25,
    "performance_concentration_reference": 0.70,
    "weight_extremeness_reference": 0.70,
}


class WeightCalibrationError(ValueError):
    """Raised when ETF weight calibration config violates governance requirements."""


@dataclass(frozen=True)
class ETFWeightSearchRun:
    run_id: str
    payload: dict[str, Any]


class ETFWeightCalibrationSafety(BaseModel):
    observe_only: bool
    candidate_only: bool
    production_effect: str = Field(min_length=1)
    broker_action: str = Field(min_length=1)
    manual_review_required: bool

    @model_validator(mode="after")
    def validate_safety_boundary(self) -> Self:
        for field, expected in WEIGHT_CALIBRATION_SAFETY.items():
            if getattr(self, field) != expected:
                raise ValueError(f"ETF weight calibration safety {field} must be {expected!r}")
        return self


class ETFWeightConstraint(BaseModel):
    min: float = Field(ge=0, le=1)
    max: float = Field(ge=0, le=1)

    @model_validator(mode="after")
    def validate_order(self) -> Self:
        if self.min > self.max:
            raise ValueError("ETF weight constraint min must be <= max")
        return self


class ETFWeightSleeveConstraints(BaseModel):
    equity_total_max: float = Field(ge=0, le=1)
    semiconductor_total_max: float = Field(ge=0, le=1)
    cash_min_when_risk_off: float = Field(ge=0, le=1)


class ETFWeightObjectivePolicy(BaseModel):
    description: str = Field(min_length=1)
    policy_status: str = Field(min_length=1)
    owner: str = Field(min_length=1)
    rationale: str = Field(min_length=1)
    intended_effect: str = Field(min_length=1)
    validation_evidence: str = Field(min_length=1)
    review_condition: str = Field(min_length=1)
    component_weights: dict[str, float] = Field(min_length=1)
    component_scales: dict[str, float] = Field(default_factory=dict)
    hard_blocker_thresholds: dict[str, float] = Field(default_factory=dict)
    hard_blockers: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_policy_numbers(self) -> Self:
        total = sum(float(value) for value in self.component_weights.values())
        if abs(total - 1.0) > 1e-6:
            raise ValueError("ETF weight objective component_weights must sum to 1.0")
        for key, value in self.component_scales.items():
            if float(value) <= 0:
                raise ValueError(f"ETF weight objective component scale must be positive: {key}")
        for key, value in self.hard_blocker_thresholds.items():
            if float(value) < 0:
                raise ValueError(
                    f"ETF weight objective hard blocker threshold must be non-negative: {key}"
                )
        normalized_blockers = [str(item).strip() for item in self.hard_blockers]
        if any(not item for item in normalized_blockers):
            raise ValueError("ETF weight objective hard_blockers must not contain blanks")
        if len(normalized_blockers) != len(set(normalized_blockers)):
            raise ValueError("ETF weight objective hard_blockers must be unique")
        self.hard_blockers = normalized_blockers
        return self


class ETFWeightBenchmarkSet(BaseModel):
    description: str = Field(min_length=1)
    benchmark_ids: list[str] = Field(min_length=1)

    @model_validator(mode="after")
    def validate_unique_benchmarks(self) -> Self:
        normalized = [str(item).strip() for item in self.benchmark_ids]
        if any(not item for item in normalized):
            raise ValueError("ETF weight benchmark set ids must not be blank")
        if len(normalized) != len(set(normalized)):
            raise ValueError("ETF weight benchmark set ids must be unique")
        self.benchmark_ids = normalized
        return self


class ETFWalkForwardWindow(BaseModel):
    window_id: str = Field(min_length=1)
    start_date: date
    end_date: date
    description: str = ""

    @model_validator(mode="after")
    def validate_date_order(self) -> Self:
        if self.start_date > self.end_date:
            raise ValueError("ETF walk-forward window start_date must be <= end_date")
        return self


class ETFRegimeSplitConfig(BaseModel):
    description: str = Field(min_length=1)
    start_date: date | None = None
    end_date: date | None = None
    regime_label: str | None = None
    signal: str | None = None

    @model_validator(mode="after")
    def validate_date_order(self) -> Self:
        if self.start_date is not None and self.end_date is not None:
            if self.start_date > self.end_date:
                raise ValueError("ETF regime split start_date must be <= end_date")
        if not self.regime_label and not self.signal:
            raise ValueError("ETF regime split requires regime_label or signal")
        return self


class ETFWeightSearchDefinition(BaseModel):
    search_id: str = Field(min_length=1)
    description: str = Field(min_length=1)
    universe: list[str] = Field(min_length=1)
    weight_constraints: dict[str, ETFWeightConstraint] = Field(min_length=1)
    sleeve_constraints: ETFWeightSleeveConstraints
    grid_step: float = Field(gt=0, le=0.25)
    max_candidate_count: int = Field(gt=0, le=10_000)
    objective_policy: str = Field(min_length=1)
    benchmark_set: str = Field(min_length=1)
    backtest_start_date: date
    backtest_end_date: date | None = None
    walk_forward_windows: list[ETFWalkForwardWindow] = Field(default_factory=list)
    regime_splits: dict[str, ETFRegimeSplitConfig] = Field(default_factory=dict)
    safety: ETFWeightCalibrationSafety

    @model_validator(mode="after")
    def validate_search_definition(self) -> Self:
        self.universe = _normalized_unique_symbols(self.universe, "universe")
        if "CASH" not in self.universe:
            raise ValueError("ETF weight search universe must include CASH")
        constraint_symbols = set(self.weight_constraints)
        universe_symbols = set(self.universe)
        missing = sorted(universe_symbols - constraint_symbols)
        extra = sorted(constraint_symbols - universe_symbols)
        if missing or extra:
            raise ValueError(
                "ETF weight search constraints must match universe; "
                f"missing={missing}; extra={extra}"
            )
        if self.backtest_end_date is not None and self.backtest_start_date > self.backtest_end_date:
            raise ValueError("ETF weight search backtest_start_date must be <= backtest_end_date")
        _raise_if_invalid_grid_step(self.grid_step)
        return self


class ETFWeightSearchRegistry(BaseModel):
    schema_version: str = Field(min_length=1)
    policy_metadata: PolicyMetadata
    objective_policies: dict[str, ETFWeightObjectivePolicy] = Field(min_length=1)
    benchmark_sets: dict[str, ETFWeightBenchmarkSet] = Field(min_length=1)
    weight_searches: dict[str, ETFWeightSearchDefinition] = Field(min_length=1)

    @model_validator(mode="after")
    def validate_registry_references(self) -> Self:
        if self.schema_version != WEIGHT_SEARCH_SCHEMA_VERSION:
            raise ValueError(
                f"ETF weight search schema_version must be {WEIGHT_SEARCH_SCHEMA_VERSION}"
            )
        for key, search in self.weight_searches.items():
            if search.search_id != key:
                raise ValueError(
                    "ETF weight search mapping key must match search_id: "
                    f"{key} != {search.search_id}"
                )
            if search.objective_policy not in self.objective_policies:
                raise ValueError(
                    f"ETF weight search references unknown objective_policy: "
                    f"{search.objective_policy}"
                )
            if search.benchmark_set not in self.benchmark_sets:
                raise ValueError(
                    f"ETF weight search references unknown benchmark_set: {search.benchmark_set}"
                )
        return self

    @property
    def config_hash(self) -> str:
        return _config_hash(self.model_dump(mode="json"))


def load_weight_search_registry(
    path: Path = DEFAULT_ETF_WEIGHT_SEARCH_CONFIG_PATH,
    *,
    etf_config: ETFConfigBundle | None = None,
) -> ETFWeightSearchRegistry:
    raw = safe_load_yaml_path(path)
    if not isinstance(raw, dict):
        raise WeightCalibrationError(f"ETF weight search config must be a YAML mapping: {path}")
    try:
        registry = ETFWeightSearchRegistry.model_validate(raw)
        validate_weight_search_registry(
            registry,
            etf_config=etf_config or load_etf_config_bundle(),
        )
    except ValueError as exc:
        raise WeightCalibrationError(str(exc)) from exc
    return registry


def load_weight_search_definition(
    search_id: str,
    path: Path = DEFAULT_ETF_WEIGHT_SEARCH_CONFIG_PATH,
    *,
    etf_config: ETFConfigBundle | None = None,
) -> ETFWeightSearchDefinition:
    registry = load_weight_search_registry(path, etf_config=etf_config)
    try:
        return registry.weight_searches[search_id]
    except KeyError as exc:
        raise WeightCalibrationError(f"unknown ETF weight search: {search_id}") from exc


def validate_weight_search_registry(
    registry: ETFWeightSearchRegistry,
    *,
    etf_config: ETFConfigBundle,
) -> None:
    available_symbols = set(etf_config.assets.assets)
    configured_benchmark_ids = set(etf_config.backtest.backtest.benchmarks)
    issues: list[str] = []
    for search in registry.weight_searches.values():
        missing_symbols = sorted(set(search.universe) - available_symbols)
        if missing_symbols:
            issues.append(
                f"{search.search_id} universe references unknown symbols: "
                f"{', '.join(missing_symbols)}"
            )
        benchmark_set = registry.benchmark_sets[search.benchmark_set]
        missing_benchmarks = sorted(set(benchmark_set.benchmark_ids) - configured_benchmark_ids)
        if missing_benchmarks:
            issues.append(
                f"{search.search_id} benchmark_set {search.benchmark_set} references "
                f"unknown ETF benchmark ids: {', '.join(missing_benchmarks)}"
            )
    if issues:
        raise WeightCalibrationError("; ".join(issues))


def generate_weight_candidates(
    search: ETFWeightSearchDefinition,
    *,
    etf_config: ETFConfigBundle,
    max_candidates: int | None = None,
) -> tuple[list[dict[str, float]], dict[str, Any]]:
    candidate_limit = int(max_candidates or search.max_candidate_count)
    if candidate_limit <= 0:
        raise WeightCalibrationError("ETF weight search max_candidates must be positive")
    if candidate_limit > search.max_candidate_count:
        raise WeightCalibrationError(
            "ETF weight search max_candidates cannot exceed config max_candidate_count"
        )
    units = int(round(1.0 / search.grid_step))
    constraints = {
        symbol: (
            int(round(search.weight_constraints[symbol].min * units)),
            int(round(search.weight_constraints[symbol].max * units)),
        )
        for symbol in search.universe
    }
    non_cash_symbols = [symbol for symbol in search.universe if symbol != "CASH"]
    semiconductor_symbols = {
        symbol
        for symbol in non_cash_symbols
        if etf_config.assets.assets[symbol].risk_group == "semiconductor"
    }
    valid_candidates: list[dict[str, float]] = []

    def visit(index: int, current: dict[str, int]) -> None:
        if index == len(non_cash_symbols):
            cash_units = units - sum(current.values())
            cash_min, cash_max = constraints["CASH"]
            if cash_units < cash_min or cash_units > cash_max:
                return
            candidate_units = {**current, "CASH": cash_units}
            candidate = {
                symbol: round(candidate_units[symbol] / units, 10)
                for symbol in search.universe
            }
            if not _candidate_satisfies_sleeves(
                candidate,
                search=search,
                semiconductor_symbols=semiconductor_symbols,
            ):
                return
            valid_candidates.append(candidate)
            return
        symbol = non_cash_symbols[index]
        min_units, max_units = constraints[symbol]
        for value in range(min_units, max_units + 1):
            current[symbol] = value
            if sum(current.values()) <= units:
                visit(index + 1, current)
        current.pop(symbol, None)

    visit(0, {})
    baseline_weights = _default_weights(etf_config, search.universe)
    valid_candidates.sort(key=lambda weights: _candidate_selection_key(weights, baseline_weights))
    selected = valid_candidates[:candidate_limit]
    return selected, {
        "selection_method": "deterministic_bounded_grid_baseline_proximity_v1",
        "grid_step": search.grid_step,
        "total_valid_candidate_count": len(valid_candidates),
        "evaluated_candidate_count": len(selected),
        "max_candidate_count": search.max_candidate_count,
        "effective_candidate_limit": candidate_limit,
        "truncated_by_candidate_limit": len(valid_candidates) > len(selected),
    }


def run_historical_weight_search(
    prices: pd.DataFrame,
    *,
    etf_config: ETFConfigBundle,
    quality_report: ETFQualityReport,
    registry: ETFWeightSearchRegistry,
    search_id: str,
    start: date | None = None,
    end: date | None = None,
    max_candidates: int | None = None,
    generated_at: datetime | None = None,
) -> ETFWeightSearchRun:
    if not quality_report.passed:
        raise WeightCalibrationError(
            f"ETF weight calibration requires passed data quality gate: {quality_report.status}"
        )
    try:
        search = registry.weight_searches[search_id]
    except KeyError as exc:
        raise WeightCalibrationError(f"unknown ETF weight search: {search_id}") from exc
    generated = generated_at or datetime.now(UTC)
    run_start = start or search.backtest_start_date
    run_end = end or search.backtest_end_date or _latest_price_date(prices)
    if run_start >= run_end:
        raise WeightCalibrationError("ETF weight search start date must be before end date")
    candidates, generation = generate_weight_candidates(
        search,
        etf_config=etf_config,
        max_candidates=max_candidates,
    )
    baseline_weights = _default_weights(etf_config, search.universe)
    baseline = _run_static_weight_backtest(
        prices,
        weights=baseline_weights,
        config=etf_config,
        start=run_start,
        end=run_end,
    )
    benchmark_results = _run_benchmark_set_backtests(
        prices,
        config=etf_config,
        registry=registry,
        search=search,
        start=run_start,
        end=run_end,
    )
    objective = registry.objective_policies[search.objective_policy]
    baseline_daily = baseline["daily"]
    ranked_rows = []
    candidate_weight_sets = []
    metrics_rows = []
    robustness_payloads = []
    for index, weights in enumerate(candidates, start=1):
        candidate_id = f"weight_set_{index:04d}"
        backtest = _run_static_weight_backtest(
            prices,
            weights=weights,
            config=etf_config,
            start=run_start,
            end=run_end,
        )
        robustness = build_weight_robustness_evaluation(
            candidate_id=candidate_id,
            candidate_daily=backtest["daily"],
            baseline_daily=baseline_daily,
            weights=weights,
            search=search,
            etf_config=etf_config,
            objective=objective,
        )
        metric_row = _candidate_metric_row(
            candidate_id=candidate_id,
            weights=weights,
            candidate_metrics=backtest["metrics"],
            baseline_metrics=baseline["metrics"],
            benchmark_results=benchmark_results,
            objective=objective,
            search=search,
            semiconductor_symbols=_semiconductor_symbols(etf_config, search.universe),
            baseline_weights=baseline_weights,
            robustness_score=float(robustness["summary"]["stability_score"]),
            robustness_summary=robustness["summary"],
        )
        robustness_payloads.append(robustness)
        candidate_weight_sets.append(
            {
                "candidate_id": candidate_id,
                "weights": weights,
                "robustness_summary": robustness["summary"],
                "safety": dict(WEIGHT_CALIBRATION_SAFETY),
                **WEIGHT_CALIBRATION_SAFETY,
            }
        )
        metrics_rows.append(metric_row)
        ranked_rows.append(
            {
                "candidate_id": candidate_id,
                "candidate_score": metric_row["candidate_score"],
                "candidate_status": metric_row["candidate_status"],
                "hard_blockers": metric_row["hard_blockers"],
                "ranking_reason": metric_row["ranking_reason"],
                "weights": weights,
                "observe_only": True,
                "candidate_only": True,
                "production_effect": "none",
                "broker_action": "none",
                "manual_review_required": True,
            }
        )
    ranking = sorted(
        ranked_rows,
        key=lambda row: (
            0 if row["candidate_status"] == "ranked" else 1,
            -float(row["candidate_score"]),
            str(row["candidate_id"]),
        ),
    )
    for rank, row in enumerate(ranking, start=1):
        row["rank"] = rank
    rank_by_candidate = {row["candidate_id"]: row["rank"] for row in ranking}
    for row in candidate_weight_sets:
        row["rank"] = rank_by_candidate[row["candidate_id"]]
    for row in metrics_rows:
        row["rank"] = rank_by_candidate[row["candidate_id"]]
    blocked_candidates = [
        row for row in ranking if row["candidate_status"] in {"blocked", "rejected"}
    ]
    run_id = f"etf-weight-search-{generated.strftime('%Y%m%dT%H%M%SZ')}"
    payload = {
        "schema_version": WEIGHT_SEARCH_RUN_SCHEMA_VERSION,
        "report_type": "etf_weight_search_run",
        "search_run_id": run_id,
        "search_id": search.search_id,
        "search_config_hash": registry.config_hash,
        "generated_at": generated.isoformat(),
        "market_regime": etf_config.backtest.backtest.regime,
        "requested_date_range": {
            "start": run_start.isoformat(),
            "end": run_end.isoformat(),
        },
        "data_quality_status": quality_report.status,
        "baseline_weight_set": {
            "weight_set_id": "current_default_weights",
            "weights": baseline_weights,
            "metrics": _metrics_payload(baseline["metrics"]),
        },
        "candidate_generation": generation,
        "objective_policy": {
            "policy_id": search.objective_policy,
            "policy_status": objective.policy_status,
            "component_weights": dict(objective.component_weights),
            "component_scales": dict(objective.component_scales),
            "hard_blocker_thresholds": dict(objective.hard_blocker_thresholds),
            "hard_blockers": list(objective.hard_blockers),
        },
        "benchmark_set": {
            "benchmark_set_id": search.benchmark_set,
            "benchmark_ids": list(registry.benchmark_sets[search.benchmark_set].benchmark_ids),
            "benchmark_metrics": benchmark_results,
        },
        "candidate_weight_sets": candidate_weight_sets,
        "metrics": metrics_rows,
        "ranking": ranking,
        "blocked_candidates": blocked_candidates,
        "robustness_evaluation": {
            "schema_version": "etf_weight_robustness_v1",
            "evaluation_modes": [
                "full_period",
                "walk_forward_windows",
                "risk_on_periods",
                "neutral_periods",
                "risk_off_periods",
                "high_volatility_periods",
                "semiconductor_leadership_periods",
                "growth_underperformance_periods",
            ],
            "candidate_evaluations": robustness_payloads,
            "summary": _robustness_run_summary(robustness_payloads),
        },
        "production_weights_mutated": False,
        "applied_weight_set": None,
        "safety": dict(WEIGHT_CALIBRATION_SAFETY),
        **WEIGHT_CALIBRATION_SAFETY,
    }
    validate_weight_search_run_payload(payload)
    return ETFWeightSearchRun(run_id=run_id, payload=payload)


def validate_weight_search_run_payload(payload: Mapping[str, Any]) -> None:
    issues = []
    if payload.get("schema_version") != WEIGHT_SEARCH_RUN_SCHEMA_VERSION:
        issues.append("schema_version")
    for field, expected in WEIGHT_CALIBRATION_SAFETY.items():
        if payload.get(field) != expected:
            issues.append(field)
    if payload.get("production_weights_mutated") is not False:
        issues.append("production_weights_mutated")
    if payload.get("applied_weight_set") is not None:
        issues.append("applied_weight_set")
    generation = _mapping(payload.get("candidate_generation"))
    if int(generation.get("evaluated_candidate_count", 0)) <= 0:
        issues.append("candidate_generation.evaluated_candidate_count")
    for candidate in _records(payload.get("candidate_weight_sets")):
        weights = _mapping(candidate.get("weights"))
        if abs(sum(float(value) for value in weights.values()) - 1.0) > 1e-6:
            issues.append(f"{candidate.get('candidate_id')}.weights_sum")
        for field, expected in WEIGHT_CALIBRATION_SAFETY.items():
            if candidate.get(field) != expected:
                issues.append(f"{candidate.get('candidate_id')}.{field}")
    if not _records(payload.get("ranking")):
        issues.append("ranking")
    if issues:
        raise WeightCalibrationError(
            "ETF weight search run payload validation failed: " + ", ".join(issues)
        )


def write_weight_search_run(
    run: ETFWeightSearchRun,
    *,
    report_root: Path = DEFAULT_ETF_WEIGHT_CALIBRATION_REPORT_DIR,
    data_root: Path = DEFAULT_ETF_WEIGHT_CALIBRATION_DATA_DIR,
) -> dict[str, Path]:
    payload = run.payload
    validate_weight_search_run_payload(payload)
    report_dir = report_root / run.run_id
    data_dir = data_root / run.run_id
    report_dir.mkdir(parents=True, exist_ok=True)
    data_dir.mkdir(parents=True, exist_ok=True)
    summary_json = report_dir / "summary.json"
    summary_md = report_dir / "summary.md"
    metrics_csv = report_dir / "metrics.csv"
    ranking_json = report_dir / "ranking.json"
    robustness_json = report_dir / "robustness.json"
    candidates_json = data_dir / "candidate_weight_sets.json"
    candidates_csv = data_dir / "candidate_weight_sets.csv"
    summary_json.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    summary_md.write_text(render_weight_search_run_markdown(payload), encoding="utf-8")
    metrics_frame = pd.DataFrame(payload["metrics"])
    metrics_frame.to_csv(metrics_csv, index=False)
    ranking_json.write_text(
        json.dumps(payload["ranking"], ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    robustness_json.write_text(
        json.dumps(
            payload["robustness_evaluation"],
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    candidates_json.write_text(
        json.dumps(
            payload["candidate_weight_sets"],
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    pd.DataFrame(
        [
            {
                "candidate_id": candidate["candidate_id"],
                "rank": candidate["rank"],
                **{
                    f"weight_{symbol}": weight
                    for symbol, weight in dict(candidate["weights"]).items()
                },
                "observe_only": True,
                "candidate_only": True,
                "production_effect": "none",
                "broker_action": "none",
                "manual_review_required": True,
            }
            for candidate in payload["candidate_weight_sets"]
        ]
    ).to_csv(candidates_csv, index=False)
    return {
        "report_dir": report_dir,
        "data_dir": data_dir,
        "summary_json": summary_json,
        "summary_md": summary_md,
        "metrics_csv": metrics_csv,
        "ranking_json": ranking_json,
        "robustness_json": robustness_json,
        "candidates_json": candidates_json,
        "candidates_csv": candidates_csv,
    }


def find_latest_weight_search_run_dir(output_root: Path) -> Path:
    if not output_root.exists():
        raise WeightCalibrationError(f"ETF weight search output dir does not exist: {output_root}")
    candidates = [
        item for item in output_root.iterdir() if item.is_dir() and (item / "summary.json").exists()
    ]
    if not candidates:
        raise WeightCalibrationError(f"no ETF weight search runs found under {output_root}")
    return max(candidates, key=lambda item: (item / "summary.json").stat().st_mtime)


def read_weight_search_run_payload(run_dir: Path) -> dict[str, Any]:
    path = run_dir / "summary.json"
    if not path.exists():
        raise WeightCalibrationError(f"ETF weight search summary not found: {path}")
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise WeightCalibrationError(f"ETF weight search summary must be a JSON object: {path}")
    validate_weight_search_run_payload(payload)
    return payload


def load_candidate_weight_registry(
    path: Path = DEFAULT_CANDIDATE_WEIGHT_REGISTRY_PATH,
) -> dict[str, Any]:
    if not path.exists():
        return _empty_candidate_weight_registry()
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise WeightCalibrationError(f"candidate weight registry must be a JSON object: {path}")
    validate_candidate_weight_registry(payload)
    return payload


def write_candidate_weight_registry(
    registry: Mapping[str, Any],
    path: Path = DEFAULT_CANDIDATE_WEIGHT_REGISTRY_PATH,
) -> Path:
    validate_candidate_weight_registry(registry)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(registry, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return path


def register_candidate_weight_sets(
    search_payload: Mapping[str, Any],
    *,
    registry_path: Path = DEFAULT_CANDIDATE_WEIGHT_REGISTRY_PATH,
    top: int | None = None,
    weight_set_ids: list[str] | None = None,
    created_at: datetime | None = None,
) -> dict[str, Any]:
    validate_weight_search_run_payload(search_payload)
    registry = load_candidate_weight_registry(registry_path)
    selected = _selected_ranked_candidates(
        search_payload,
        top=top,
        weight_set_ids=weight_set_ids,
    )
    existing = {
        str(record.get("weight_set_id")): record
        for record in _records(registry.get("weight_sets"))
    }
    generated = created_at or datetime.now(UTC)
    for ranked in selected:
        record = _candidate_weight_record_from_search(
            search_payload,
            ranked,
            created_at=generated,
        )
        existing[record["weight_set_id"]] = record
    weight_sets = sorted(existing.values(), key=lambda item: str(item.get("weight_set_id")))
    registry = {
        "schema_version": CANDIDATE_WEIGHT_REGISTRY_SCHEMA_VERSION,
        "registry_type": "etf_candidate_weight_sets",
        "updated_at": generated.isoformat(),
        "candidate_count": len(weight_sets),
        "weight_sets": weight_sets,
        "safety": dict(WEIGHT_CALIBRATION_SAFETY),
        **WEIGHT_CALIBRATION_SAFETY,
    }
    validate_candidate_weight_registry(registry)
    write_candidate_weight_registry(registry, registry_path)
    return registry


def validate_candidate_weight_registry(registry: Mapping[str, Any]) -> None:
    issues = []
    if registry.get("schema_version") != CANDIDATE_WEIGHT_REGISTRY_SCHEMA_VERSION:
        issues.append("schema_version")
    for field, expected in WEIGHT_CALIBRATION_SAFETY.items():
        if registry.get(field) != expected:
            issues.append(field)
    seen = set()
    for record in _records(registry.get("weight_sets")):
        weight_set_id = str(record.get("weight_set_id") or "")
        if not weight_set_id:
            issues.append("weight_set_id")
        if weight_set_id in seen:
            issues.append(f"duplicate_weight_set_id:{weight_set_id}")
        seen.add(weight_set_id)
        try:
            validate_candidate_weight_record(record)
        except WeightCalibrationError as exc:
            issues.append(f"{weight_set_id}:{exc}")
    if issues:
        raise WeightCalibrationError(
            "ETF candidate weight registry validation failed: " + ", ".join(issues)
        )


def validate_candidate_weight_record(record: Mapping[str, Any]) -> None:
    required = (
        "weight_set_id",
        "source_search_run_id",
        "rank",
        "status",
        "weights",
        "metrics_summary",
        "robustness_summary",
        "blockers",
        "selection_reason",
        "config_hash",
        "created_at",
        "safety",
    )
    issues = [field for field in required if field not in record]
    status = str(record.get("status") or "")
    if status not in ALLOWED_CANDIDATE_WEIGHT_STATUSES:
        issues.append("status")
    for field, expected in WEIGHT_CALIBRATION_SAFETY.items():
        if record.get(field) != expected:
            issues.append(field)
    safety = _mapping(record.get("safety"))
    for field, expected in WEIGHT_CALIBRATION_SAFETY.items():
        if safety.get(field) != expected:
            issues.append(f"safety.{field}")
    weights = _mapping(record.get("weights"))
    if abs(sum(float(value) for value in weights.values()) - 1.0) > 1e-6:
        issues.append("weights_sum")
    blockers = [str(item) for item in record.get("blockers") or []]
    if status == "shadow_ready" and blockers:
        issues.append("blocked_candidate_cannot_be_shadow_ready")
    if issues:
        raise WeightCalibrationError("; ".join(issues))


def update_candidate_weight_status(
    registry: Mapping[str, Any],
    *,
    weight_set_id: str,
    status: str,
    updated_at: datetime | None = None,
) -> dict[str, Any]:
    if status not in ALLOWED_CANDIDATE_WEIGHT_STATUSES:
        raise WeightCalibrationError(f"unsupported candidate weight status: {status}")
    payload = json.loads(json.dumps(registry, default=str))
    records = _records(payload.get("weight_sets"))
    matched = False
    for record in records:
        if str(record.get("weight_set_id")) != weight_set_id:
            continue
        record["status"] = status
        record["updated_at"] = (updated_at or datetime.now(UTC)).isoformat()
        validate_candidate_weight_record(record)
        matched = True
    if not matched:
        raise WeightCalibrationError(f"unknown candidate weight_set_id: {weight_set_id}")
    payload["weight_sets"] = records
    payload["updated_at"] = (updated_at or datetime.now(UTC)).isoformat()
    validate_candidate_weight_registry(payload)
    return payload


def load_weight_forward_enrollments(
    path: Path = DEFAULT_WEIGHT_FORWARD_ENROLLMENT_PATH,
) -> dict[str, Any]:
    if not path.exists():
        return _empty_weight_forward_enrollment_registry()
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise WeightCalibrationError(
            f"ETF weight forward enrollment registry must be a JSON object: {path}"
        )
    validate_weight_forward_enrollment_registry(payload)
    return payload


def write_weight_forward_enrollments(
    registry: Mapping[str, Any],
    path: Path = DEFAULT_WEIGHT_FORWARD_ENROLLMENT_PATH,
) -> Path:
    validate_weight_forward_enrollment_registry(registry)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(registry, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return path


def enroll_candidate_weights_forward(
    candidate_registry: Mapping[str, Any],
    *,
    enrollment_path: Path = DEFAULT_WEIGHT_FORWARD_ENROLLMENT_PATH,
    top: int | None = None,
    weight_set_ids: list[str] | None = None,
    enrolled_at: datetime | None = None,
) -> dict[str, Any]:
    validate_candidate_weight_registry(candidate_registry)
    selected = _selected_candidate_weight_records(
        candidate_registry,
        top=top,
        weight_set_ids=weight_set_ids,
    )
    if not selected:
        raise WeightCalibrationError("no ETF candidate weight sets selected for enrollment")
    for record in selected:
        _raise_if_candidate_weight_forward_enrollable(record)
    existing_registry = load_weight_forward_enrollments(enrollment_path)
    existing = {
        str(record.get("weight_set_id")): record
        for record in _records(existing_registry.get("enrollments"))
    }
    generated = enrolled_at or datetime.now(UTC)
    changed = False
    selected_ids = [str(record.get("weight_set_id")) for record in selected]
    for candidate in selected:
        weight_set_id = str(candidate.get("weight_set_id"))
        if weight_set_id in existing:
            continue
        enrollment = _weight_forward_enrollment_record(
            candidate,
            enrollment_path=enrollment_path,
            enrolled_at=generated,
        )
        existing[weight_set_id] = enrollment
        changed = True
    enrollments = sorted(
        existing.values(),
        key=lambda item: (int(item.get("rank") or 999_999), str(item.get("weight_set_id"))),
    )
    registry = {
        "schema_version": WEIGHT_FORWARD_ENROLLMENT_SCHEMA_VERSION,
        "registry_type": "etf_weight_forward_enrollments",
        "updated_at": generated.isoformat() if changed else existing_registry.get("updated_at"),
        "enrollment_count": len(enrollments),
        "enrollments": enrollments,
        "latest_selection": {
            "selected_at": generated.isoformat(),
            "weight_set_ids": selected_ids,
        },
        "shared_shadow_registry_mutated": False,
        "production_weights_mutated": False,
        "applied_weight_set": None,
        "safety": dict(WEIGHT_CALIBRATION_SAFETY),
        **WEIGHT_CALIBRATION_SAFETY,
    }
    validate_weight_forward_enrollment_registry(registry)
    write_weight_forward_enrollments(registry, enrollment_path)
    return registry


def validate_weight_forward_enrollment_registry(registry: Mapping[str, Any]) -> None:
    issues = []
    if registry.get("schema_version") != WEIGHT_FORWARD_ENROLLMENT_SCHEMA_VERSION:
        issues.append("schema_version")
    if registry.get("registry_type") != "etf_weight_forward_enrollments":
        issues.append("registry_type")
    for field, expected in WEIGHT_CALIBRATION_SAFETY.items():
        if registry.get(field) != expected:
            issues.append(field)
    safety = _mapping(registry.get("safety"))
    for field, expected in WEIGHT_CALIBRATION_SAFETY.items():
        if safety.get(field) != expected:
            issues.append(f"safety.{field}")
    if registry.get("shared_shadow_registry_mutated") is not False:
        issues.append("shared_shadow_registry_mutated")
    if registry.get("production_weights_mutated") is not False:
        issues.append("production_weights_mutated")
    if registry.get("applied_weight_set") is not None:
        issues.append("applied_weight_set")
    seen_weight_sets: set[str] = set()
    seen_shadow_ids: set[str] = set()
    enrollments = _records(registry.get("enrollments"))
    for record in enrollments:
        weight_set_id = str(record.get("weight_set_id") or "")
        shadow_id = str(record.get("shadow_id") or "")
        if weight_set_id in seen_weight_sets:
            issues.append(f"duplicate_weight_set_id:{weight_set_id}")
        if shadow_id in seen_shadow_ids:
            issues.append(f"duplicate_shadow_id:{shadow_id}")
        seen_weight_sets.add(weight_set_id)
        seen_shadow_ids.add(shadow_id)
        try:
            validate_weight_forward_enrollment_record(record)
        except WeightCalibrationError as exc:
            issues.append(f"{weight_set_id}:{exc}")
    if int(registry.get("enrollment_count") or 0) != len(enrollments):
        issues.append("enrollment_count")
    if issues:
        raise WeightCalibrationError(
            "ETF weight forward enrollment validation failed: "
            + ", ".join(str(issue) for issue in issues)
        )


def validate_weight_forward_enrollment_record(record: Mapping[str, Any]) -> None:
    required = (
        "shadow_id",
        "weight_set_id",
        "source_search_run_id",
        "source_candidate_id",
        "rank",
        "status",
        "weights",
        "metrics_summary",
        "robustness_summary",
        "blockers",
        "selection_reason",
        "config_hash",
        "enrolled_at",
        "enrollment_date",
        "forward_tracking_link",
        "tracking_state",
        "shadow_record",
        "shared_shadow_registry_mutated",
        "production_weights_mutated",
        "applied_weight_set",
        "production_promotion_allowed",
        "safety",
    )
    issues = [field for field in required if field not in record]
    status = str(record.get("status") or "")
    if status not in ALLOWED_WEIGHT_FORWARD_ENROLLMENT_STATUSES:
        issues.append("status")
    for field, expected in WEIGHT_CALIBRATION_SAFETY.items():
        if record.get(field) != expected:
            issues.append(field)
    safety = _mapping(record.get("safety"))
    for field, expected in WEIGHT_CALIBRATION_SAFETY.items():
        if safety.get(field) != expected:
            issues.append(f"safety.{field}")
    if record.get("production_weights_mutated") is not False:
        issues.append("production_weights_mutated")
    if record.get("applied_weight_set") is not None:
        issues.append("applied_weight_set")
    if record.get("production_promotion_allowed") is not False:
        issues.append("production_promotion_allowed")
    weights = _mapping(record.get("weights"))
    if abs(sum(float(value) for value in weights.values()) - 1.0) > 1e-6:
        issues.append("weights_sum")
    blockers = [str(item) for item in record.get("blockers") or []]
    if blockers:
        issues.append("blocked_candidate_cannot_enroll_forward")
    shadow = _mapping(record.get("shadow_record"))
    if shadow.get("weight_set_id") != record.get("weight_set_id"):
        issues.append("shadow_record.weight_set_id")
    if shadow.get("shadow_id") != record.get("shadow_id"):
        issues.append("shadow_record.shadow_id")
    if shadow.get("status") != "active":
        issues.append("shadow_record.status")
    for field, expected in WEIGHT_CALIBRATION_SAFETY.items():
        if shadow.get(field) != expected:
            issues.append(f"shadow_record.{field}")
    tracking = _mapping(record.get("tracking_state"))
    if tracking.get("tracking_status") != "active":
        issues.append("tracking_state.tracking_status")
    if not str(record.get("forward_tracking_link") or ""):
        issues.append("forward_tracking_link")
    if issues:
        raise WeightCalibrationError("; ".join(issues))


def build_backtest_forward_evidence_aggregation(
    *,
    as_of: date,
    candidate_registry: Mapping[str, Any],
    forward_enrollments: Mapping[str, Any],
    search_payload: Mapping[str, Any] | None = None,
    forward_dashboard: Mapping[str, Any] | None = None,
    weekly_review: Mapping[str, Any] | None = None,
    decision_journal: Mapping[str, Any] | None = None,
    parameter_review: Mapping[str, Any] | None = None,
    generated_at: datetime | None = None,
    source_paths: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    validate_candidate_weight_registry(candidate_registry)
    validate_weight_forward_enrollment_registry(forward_enrollments)
    if search_payload:
        validate_weight_search_run_payload(search_payload)
    generated = generated_at or datetime.now(UTC)
    records = [
        _backtest_forward_evidence_record(
            enrollment,
            as_of=as_of,
            candidate_registry=candidate_registry,
            search_payload=search_payload or {},
            forward_dashboard=forward_dashboard or {},
            weekly_review=weekly_review or {},
            decision_journal=decision_journal or {},
            parameter_review=parameter_review or {},
            generated_at=generated,
            source_paths=source_paths or {},
        )
        for enrollment in _records(forward_enrollments.get("enrollments"))
    ]
    status_counts = _status_counts(records)
    if not records:
        status = "needs_more_forward_data"
        reason = "NO_FORWARD_ENROLLMENTS"
    elif status_counts.get("blocked"):
        status = "blocked"
        reason = "BLOCKED_CANDIDATE_EVIDENCE"
    elif status_counts.get("forward_worse_than_backtest"):
        status = "forward_worse_than_backtest"
        reason = "FORWARD_UNDERPERFORMANCE_PRESENT"
    elif status_counts.get("mixed"):
        status = "mixed"
        reason = "MIXED_FORWARD_EVIDENCE"
    elif status_counts.get("needs_more_forward_data") == len(records):
        status = "needs_more_forward_data"
        reason = "INSUFFICIENT_FORWARD_EVIDENCE"
    elif status_counts.get("forward_better_than_backtest"):
        status = "forward_better_than_backtest"
        reason = "FORWARD_OUTPERFORMANCE_PRESENT"
    else:
        status = "consistent"
        reason = "FORWARD_EVIDENCE_WITHIN_TOLERANCE"
    payload = {
        "schema_version": WEIGHT_BACKTEST_FORWARD_EVIDENCE_SCHEMA_VERSION,
        "report_type": "etf_weight_backtest_forward_evidence",
        "as_of": as_of.isoformat(),
        "generated_at": generated.isoformat(),
        "status": status,
        "reason": reason,
        "policy": dict(WEIGHT_FORWARD_EVIDENCE_POLICY),
        "candidate_count": len(_records(candidate_registry.get("weight_sets"))),
        "enrollment_count": len(_records(forward_enrollments.get("enrollments"))),
        "evidence_record_count": len(records),
        "status_counts": status_counts,
        "source_paths": dict(source_paths or {}),
        "evidence_records": records,
        "production_weights_mutated": False,
        "applied_weight_set": None,
        "safety": dict(WEIGHT_CALIBRATION_SAFETY),
        **WEIGHT_CALIBRATION_SAFETY,
    }
    validate_backtest_forward_evidence_payload(payload)
    return payload


def validate_backtest_forward_evidence_payload(payload: Mapping[str, Any]) -> None:
    issues = []
    if payload.get("schema_version") != WEIGHT_BACKTEST_FORWARD_EVIDENCE_SCHEMA_VERSION:
        issues.append("schema_version")
    if payload.get("report_type") != "etf_weight_backtest_forward_evidence":
        issues.append("report_type")
    status = str(payload.get("status") or "")
    if status not in WEIGHT_FORWARD_EVIDENCE_STATUSES:
        issues.append("status")
    for field, expected in WEIGHT_CALIBRATION_SAFETY.items():
        if payload.get(field) != expected:
            issues.append(field)
    safety = _mapping(payload.get("safety"))
    for field, expected in WEIGHT_CALIBRATION_SAFETY.items():
        if safety.get(field) != expected:
            issues.append(f"safety.{field}")
    if payload.get("production_weights_mutated") is not False:
        issues.append("production_weights_mutated")
    if payload.get("applied_weight_set") is not None:
        issues.append("applied_weight_set")
    records = _records(payload.get("evidence_records"))
    if int(payload.get("evidence_record_count") or 0) != len(records):
        issues.append("evidence_record_count")
    for record in records:
        try:
            validate_backtest_forward_evidence_record(record)
        except WeightCalibrationError as exc:
            issues.append(f"{record.get('weight_set_id')}:{exc}")
    if issues:
        raise WeightCalibrationError(
            "ETF weight backtest-forward evidence validation failed: "
            + ", ".join(str(issue) for issue in issues)
        )


def validate_backtest_forward_evidence_record(record: Mapping[str, Any]) -> None:
    required = (
        "evidence_id",
        "weight_set_id",
        "shadow_id",
        "source_search_run_id",
        "source_candidate_id",
        "rank",
        "forward_days",
        "backtest_expected_return",
        "forward_realized_return",
        "expectation_gap",
        "backtest_expected_drawdown",
        "forward_realized_drawdown",
        "drawdown_gap",
        "backtest_expected_turnover",
        "forward_realized_turnover",
        "turnover_gap",
        "backtest_expected_stability",
        "forward_realized_stability",
        "stability_gap",
        "evidence_status",
        "status_reasons",
        "source_links",
        "safety",
    )
    issues = [field for field in required if field not in record]
    status = str(record.get("evidence_status") or "")
    if status not in WEIGHT_FORWARD_EVIDENCE_STATUSES:
        issues.append("evidence_status")
    for field, expected in WEIGHT_CALIBRATION_SAFETY.items():
        if record.get(field) != expected:
            issues.append(field)
    safety = _mapping(record.get("safety"))
    for field, expected in WEIGHT_CALIBRATION_SAFETY.items():
        if safety.get(field) != expected:
            issues.append(f"safety.{field}")
    if record.get("production_weights_mutated") is not False:
        issues.append("production_weights_mutated")
    if record.get("applied_weight_set") is not None:
        issues.append("applied_weight_set")
    if not isinstance(record.get("status_reasons"), list):
        issues.append("status_reasons")
    if not isinstance(record.get("source_links"), Mapping):
        issues.append("source_links")
    if status not in {"needs_more_forward_data", "blocked"}:
        for field in (
            "forward_realized_return",
            "expectation_gap",
            "forward_realized_drawdown",
            "drawdown_gap",
        ):
            if record.get(field) is None:
                issues.append(field)
    if issues:
        raise WeightCalibrationError("; ".join(issues))


def write_backtest_forward_evidence_aggregation(
    payload: Mapping[str, Any],
    *,
    output_dir: Path = DEFAULT_WEIGHT_FORWARD_EVIDENCE_DIR,
) -> dict[str, Path]:
    validate_backtest_forward_evidence_payload(payload)
    as_of = str(payload.get("as_of"))
    json_path = output_dir / f"backtest_forward_evidence_{as_of}.json"
    markdown_path = output_dir / f"backtest_forward_evidence_{as_of}.md"
    json_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    markdown_path.write_text(
        render_backtest_forward_evidence_markdown(payload),
        encoding="utf-8",
    )
    return {"json": json_path, "markdown": markdown_path}


def render_backtest_forward_evidence_markdown(payload: Mapping[str, Any]) -> str:
    lines = [
        "# ETF Weight Backtest vs Forward Evidence",
        "",
        "## Safety Banner",
        "",
        "- observe_only = true",
        "- candidate_only = true",
        "- production_effect = none",
        "- broker_action = none",
        "- manual_review_required = true",
        "- 本报告只比较 historical expectation 与 forward evidence，不应用权重。",
        "",
        "## Summary",
        "",
        f"- Status: {payload.get('status')}",
        f"- Reason: {payload.get('reason')}",
        f"- As Of: {payload.get('as_of')}",
        f"- Evidence Records: {payload.get('evidence_record_count')}",
        f"- Status Counts: {payload.get('status_counts')}",
        "",
        "## Candidate Evidence",
        "",
        (
            "| Weight Set | Forward Days | Expected Return | Forward Return | "
            "Gap | Expected MDD | Forward MDD | Evidence Status |"
        ),
        "|---|---:|---:|---:|---:|---:|---:|---|",
    ]
    for record in _records(payload.get("evidence_records")):
        lines.append(
            f"| {record.get('weight_set_id')} | {record.get('forward_days')} | "
            f"{_fmt_pct(record.get('backtest_expected_return'))} | "
            f"{_fmt_pct(record.get('forward_realized_return'))} | "
            f"{_fmt_pct(record.get('expectation_gap'))} | "
            f"{_fmt_pct(record.get('backtest_expected_drawdown'))} | "
            f"{_fmt_pct(record.get('forward_realized_drawdown'))} | "
            f"{record.get('evidence_status')} |"
        )
    if not _records(payload.get("evidence_records")):
        lines.append("| none | 0 | n/a | n/a | n/a | n/a | n/a | needs_more_forward_data |")
    lines.extend(["", "## Source Links", ""])
    source_paths = _mapping(payload.get("source_paths"))
    if source_paths:
        for key, value in sorted(source_paths.items()):
            lines.append(f"- {key}: `{value}`")
    else:
        lines.append("- no explicit source paths supplied")
    return "\n".join(lines) + "\n"


def build_weight_overfit_diagnostics(
    *,
    candidate_registry: Mapping[str, Any],
    search_payload: Mapping[str, Any] | None = None,
    evidence_payload: Mapping[str, Any] | None = None,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    validate_candidate_weight_registry(candidate_registry)
    if search_payload:
        validate_weight_search_run_payload(search_payload)
    if evidence_payload:
        validate_backtest_forward_evidence_payload(evidence_payload)
    generated = generated_at or datetime.now(UTC)
    candidate_diagnostics = [
        _weight_overfit_candidate_diagnostics(
            candidate,
            search_payload=search_payload or {},
            evidence_payload=evidence_payload or {},
        )
        for candidate in _records(candidate_registry.get("weight_sets"))
    ]
    risk_counts = _risk_band_counts(candidate_diagnostics)
    payload = {
        "schema_version": WEIGHT_OVERFIT_DIAGNOSTICS_SCHEMA_VERSION,
        "report_type": "etf_weight_overfit_diagnostics",
        "status": "available" if candidate_diagnostics else "needs_more_data",
        "generated_at": generated.isoformat(),
        "policy": dict(WEIGHT_OVERFIT_DIAGNOSTICS_POLICY),
        "candidate_count": len(candidate_diagnostics),
        "risk_counts": risk_counts,
        "highest_risk_candidate": _highest_risk_candidate(candidate_diagnostics),
        "candidate_diagnostics": candidate_diagnostics,
        "production_weights_mutated": False,
        "applied_weight_set": None,
        "safety": dict(WEIGHT_CALIBRATION_SAFETY),
        **WEIGHT_CALIBRATION_SAFETY,
    }
    validate_weight_overfit_diagnostics_payload(payload)
    return payload


def validate_weight_overfit_diagnostics_payload(payload: Mapping[str, Any]) -> None:
    issues = []
    if payload.get("schema_version") != WEIGHT_OVERFIT_DIAGNOSTICS_SCHEMA_VERSION:
        issues.append("schema_version")
    if payload.get("report_type") != "etf_weight_overfit_diagnostics":
        issues.append("report_type")
    for field, expected in WEIGHT_CALIBRATION_SAFETY.items():
        if payload.get(field) != expected:
            issues.append(field)
    safety = _mapping(payload.get("safety"))
    for field, expected in WEIGHT_CALIBRATION_SAFETY.items():
        if safety.get(field) != expected:
            issues.append(f"safety.{field}")
    if payload.get("production_weights_mutated") is not False:
        issues.append("production_weights_mutated")
    if payload.get("applied_weight_set") is not None:
        issues.append("applied_weight_set")
    records = _records(payload.get("candidate_diagnostics"))
    if int(payload.get("candidate_count") or 0) != len(records):
        issues.append("candidate_count")
    for record in records:
        try:
            validate_weight_overfit_candidate_diagnostics(record)
        except WeightCalibrationError as exc:
            issues.append(f"{record.get('weight_set_id')}:{exc}")
    if issues:
        raise WeightCalibrationError(
            "ETF weight overfit diagnostics validation failed: "
            + ", ".join(str(issue) for issue in issues)
        )


def validate_weight_overfit_candidate_diagnostics(record: Mapping[str, Any]) -> None:
    required = (
        "weight_set_id",
        "source_search_run_id",
        "source_candidate_id",
        "rank",
        "overfit_risk_score",
        "overfit_risk_band",
        "component_diagnostics",
        "reason_codes",
        "safety",
    )
    issues = [field for field in required if field not in record]
    band = str(record.get("overfit_risk_band") or "")
    if band not in {"low", "medium", "high", "critical"}:
        issues.append("overfit_risk_band")
    score = _float_or_none(record.get("overfit_risk_score"))
    if score is None or score < 0.0 or score > 1.0:
        issues.append("overfit_risk_score")
    components = _mapping(record.get("component_diagnostics"))
    expected = set(WEIGHT_OVERFIT_DIAGNOSTICS_POLICY["component_weights"])
    if set(components) != expected:
        issues.append("component_diagnostics")
    for field, expected_value in WEIGHT_CALIBRATION_SAFETY.items():
        if record.get(field) != expected_value:
            issues.append(field)
    safety = _mapping(record.get("safety"))
    for field, expected_value in WEIGHT_CALIBRATION_SAFETY.items():
        if safety.get(field) != expected_value:
            issues.append(f"safety.{field}")
    if issues:
        raise WeightCalibrationError("; ".join(issues))


def write_weight_overfit_diagnostics(
    payload: Mapping[str, Any],
    *,
    output_dir: Path = DEFAULT_WEIGHT_OVERFIT_DIAGNOSTICS_DIR,
) -> dict[str, Path]:
    validate_weight_overfit_diagnostics_payload(payload)
    timestamp = str(payload.get("generated_at", "unknown")).replace(":", "").replace("+", "")
    stem = f"overfit_diagnostics_{timestamp[:15]}"
    json_path = output_dir / f"{stem}.json"
    markdown_path = output_dir / f"{stem}.md"
    json_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    markdown_path.write_text(render_weight_overfit_diagnostics_markdown(payload), encoding="utf-8")
    return {"json": json_path, "markdown": markdown_path}


def render_weight_overfit_diagnostics_markdown(payload: Mapping[str, Any]) -> str:
    lines = [
        "# ETF Weight Overfit Diagnostics",
        "",
        "## Safety Banner",
        "",
        "- observe_only = true",
        "- candidate_only = true",
        "- production_effect = none",
        "- broker_action = none",
        "- manual_review_required = true",
        "- 本报告只标记 overfit/stability risk，不应用权重。",
        "",
        "## Summary",
        "",
        f"- Status: {payload.get('status')}",
        f"- Candidate Count: {payload.get('candidate_count')}",
        f"- Risk Counts: {payload.get('risk_counts')}",
        f"- Highest Risk Candidate: {payload.get('highest_risk_candidate')}",
        "",
        "## Candidate Diagnostics",
        "",
        "| Weight Set | Rank | Risk Score | Risk Band | Reasons |",
        "|---|---:|---:|---|---|",
    ]
    for record in _records(payload.get("candidate_diagnostics")):
        lines.append(
            f"| {record.get('weight_set_id')} | {record.get('rank')} | "
            f"{_fmt_number(record.get('overfit_risk_score'))} | "
            f"{record.get('overfit_risk_band')} | "
            f"{', '.join(str(item) for item in record.get('reason_codes') or [])} |"
        )
    if not _records(payload.get("candidate_diagnostics")):
        lines.append("| none | 0 | n/a | needs_more_data | no candidate registry records |")
    return "\n".join(lines) + "\n"


def weight_overfit_risk_band(score: float) -> str:
    thresholds = _mapping(WEIGHT_OVERFIT_DIAGNOSTICS_POLICY["risk_band_thresholds"])
    if score >= float(thresholds["critical"]):
        return "critical"
    if score >= float(thresholds["high"]):
        return "high"
    if score >= float(thresholds["medium"]):
        return "medium"
    return "low"


def render_weight_search_run_markdown(payload: Mapping[str, Any]) -> str:
    generation = _mapping(payload.get("candidate_generation"))
    baseline = _mapping(payload.get("baseline_weight_set"))
    baseline_metrics = _mapping(baseline.get("metrics"))
    lines = [
        f"# ETF Weight Calibration Search - {payload.get('search_run_id')}",
        "",
        "## Safety Banner",
        "",
        "- observe_only = true",
        "- candidate_only = true",
        "- production_effect = none",
        "- broker_action = none",
        "- manual_review_required = true",
        "- 本报告只生成 candidate initial weights，不修改 production baseline weights。",
        "",
        "## Search Configuration",
        "",
        f"- Search ID: {payload.get('search_id')}",
        f"- Search Config Hash: `{payload.get('search_config_hash')}`",
        f"- Market Regime: {payload.get('market_regime')}",
        (
            "- Requested Date Range: "
            f"{_mapping(payload.get('requested_date_range')).get('start')} to "
            f"{_mapping(payload.get('requested_date_range')).get('end')}"
        ),
        f"- Data Quality Status: {payload.get('data_quality_status')}",
        (
            "- Candidate Grid: "
            f"{generation.get('evaluated_candidate_count')} evaluated / "
            f"{generation.get('total_valid_candidate_count')} valid; "
            f"method={generation.get('selection_method')}"
        ),
        "",
        "## Baseline",
        "",
        f"- Baseline Weight Set: {baseline.get('weight_set_id')}",
        (
            "- Baseline Metrics: "
            f"total_return={_fmt_pct(baseline_metrics.get('total_return'))}; "
            f"max_drawdown={_fmt_pct(baseline_metrics.get('max_drawdown'))}; "
            f"Sharpe={_fmt_number(baseline_metrics.get('sharpe'))}"
        ),
        "",
        "## Top Historical Candidates",
        "",
        (
            "| Rank | Candidate | Score | Status | Total Return | Excess vs Baseline | "
            "Max Drawdown | Turnover vs Baseline | Blockers |"
        ),
        "|---:|---|---:|---|---:|---:|---:|---:|---|",
    ]
    metrics_by_id = {row["candidate_id"]: row for row in _records(payload.get("metrics"))}
    for row in _records(payload.get("ranking"))[:10]:
        metrics = metrics_by_id.get(str(row.get("candidate_id")), {})
        lines.append(
            "| "
            f"{row.get('rank')} | "
            f"{row.get('candidate_id')} | "
            f"{_fmt_number(row.get('candidate_score'))} | "
            f"{row.get('candidate_status')} | "
            f"{_fmt_pct(metrics.get('total_return'))} | "
            f"{_fmt_pct(metrics.get('excess_return_vs_baseline'))} | "
            f"{_fmt_pct(metrics.get('max_drawdown'))} | "
            f"{_fmt_number(metrics.get('turnover_vs_baseline'))} | "
            f"{', '.join(row.get('hard_blockers') or []) or 'none'} |"
        )
    lines.extend(
        [
            "",
            "## Runtime Outputs",
            "",
            "- `summary.json` / `summary.md`: historical search run payload.",
            "- `metrics.csv`: candidate scoring and risk metrics.",
            "- `ranking.json`: deterministic candidate ranking.",
            "- `robustness.json`: walk-forward and regime-slice robustness diagnostics.",
            "- `candidate_weight_sets.json/csv`: candidate-only weight sets.",
        ]
    )
    return "\n".join(lines) + "\n"


def weight_search_config_to_json(registry: ETFWeightSearchRegistry) -> str:
    return (
        json.dumps(registry.model_dump(mode="json"), ensure_ascii=False, indent=2, sort_keys=True)
        + "\n"
    )


def build_weight_robustness_evaluation(
    *,
    candidate_id: str,
    candidate_daily: pd.DataFrame,
    baseline_daily: pd.DataFrame,
    weights: Mapping[str, float],
    search: ETFWeightSearchDefinition,
    etf_config: ETFConfigBundle,
    objective: ETFWeightObjectivePolicy,
) -> dict[str, Any]:
    semiconductor_symbols = _semiconductor_symbols(etf_config, search.universe)
    slice_definitions = _robustness_slice_definitions(
        candidate_daily,
        search=search,
    )
    slice_metrics = [
        _metrics_for_slice(
            candidate_daily,
            baseline_daily=baseline_daily,
            weights=weights,
            semiconductor_symbols=semiconductor_symbols,
            slice_definition=slice_definition,
        )
        for slice_definition in slice_definitions
    ]
    summary = summarize_weight_robustness(
        slice_metrics,
        objective=objective,
    )
    return {
        "candidate_id": candidate_id,
        "schema_version": "etf_weight_candidate_robustness_v1",
        "slice_method": "price_derived_regime_proxy_v1",
        "slice_metrics": slice_metrics,
        "summary": summary,
        "safety": dict(WEIGHT_CALIBRATION_SAFETY),
        **WEIGHT_CALIBRATION_SAFETY,
    }


def summarize_weight_robustness(
    slice_metrics: list[dict[str, Any]],
    *,
    objective: ETFWeightObjectivePolicy,
) -> dict[str, Any]:
    valid = [
        item
        for item in slice_metrics
        if item.get("status") == "AVAILABLE" and item.get("slice_type") != "full_period"
    ]
    if not valid:
        return {
            "status": "INSUFFICIENT_DATA",
            "evaluated_slice_count": 0,
            "positive_excess_slice_count": 0,
            "weak_slice_count": 0,
            "insufficient_slice_count": len(slice_metrics),
            "stability_score": 0.0,
            "weakest_slice_id": None,
            "reason_codes": ["NO_AVAILABLE_ROBUSTNESS_SLICES"],
        }
    thresholds = objective.hard_blocker_thresholds
    drawdown_tolerance = float(thresholds.get("slice_drawdown_worsening_tolerance", 0.02))
    dispersion_scale = float(thresholds.get("robustness_dispersion_scale", 0.10))
    positive_excess = [
        item for item in valid if float(item.get("excess_return_vs_baseline") or 0.0) >= 0.0
    ]
    weak_slices = [
        item
        for item in valid
        if float(item.get("excess_return_vs_baseline") or 0.0) < 0.0
        or float(item.get("drawdown_reduction_vs_baseline") or 0.0) < -drawdown_tolerance
    ]
    excess_values = [float(item.get("excess_return_vs_baseline") or 0.0) for item in valid]
    dispersion = pstdev(excess_values) if len(excess_values) > 1 else 0.0
    positive_ratio = len(positive_excess) / len(valid)
    drawdown_ok_ratio = 1.0 - sum(
        1
        for item in valid
        if float(item.get("drawdown_reduction_vs_baseline") or 0.0) < -drawdown_tolerance
    ) / len(valid)
    dispersion_score = _penalty_score(dispersion, dispersion_scale)
    stability_score = (
        0.50 * positive_ratio
        + 0.30 * drawdown_ok_ratio
        + 0.20 * dispersion_score
    )
    weakest = min(
        valid,
        key=lambda item: (
            float(item.get("excess_return_vs_baseline") or 0.0),
            float(item.get("drawdown_reduction_vs_baseline") or 0.0),
            str(item.get("slice_id")),
        ),
    )
    reason_codes = []
    if weak_slices:
        reason_codes.append("WEAK_ROBUSTNESS_SLICES_PRESENT")
    if dispersion > dispersion_scale:
        reason_codes.append("HIGH_EXCESS_RETURN_DISPERSION")
    if not reason_codes:
        reason_codes.append("ROBUSTNESS_SLICES_ACCEPTABLE")
    return {
        "status": "AVAILABLE",
        "evaluated_slice_count": len(valid),
        "positive_excess_slice_count": len(positive_excess),
        "weak_slice_count": len(weak_slices),
        "insufficient_slice_count": sum(
            1 for item in slice_metrics if item.get("status") != "AVAILABLE"
        ),
        "stability_score": round(max(0.0, min(1.0, stability_score)), 6),
        "positive_excess_ratio": round(positive_ratio, 6),
        "drawdown_ok_ratio": round(drawdown_ok_ratio, 6),
        "excess_return_dispersion": dispersion,
        "weakest_slice_id": weakest.get("slice_id"),
        "reason_codes": reason_codes,
    }


def _normalized_unique_symbols(values: list[str], field_name: str) -> list[str]:
    normalized = [str(item).strip().upper() for item in values]
    if any(not item for item in normalized):
        raise ValueError(f"ETF weight search {field_name} must not contain blank symbols")
    if len(normalized) != len(set(normalized)):
        raise ValueError(f"ETF weight search {field_name} must be unique")
    return normalized


def _raise_if_invalid_grid_step(grid_step: float) -> None:
    inverse = 1.0 / float(grid_step)
    if abs(inverse - round(inverse)) > 1e-8:
        raise ValueError("ETF weight search grid_step must divide 1.0 exactly")


def _config_hash(payload: dict[str, Any]) -> str:
    normalized = json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str)
    return sha256(normalized.encode("utf-8")).hexdigest()


def _run_static_weight_backtest(
    prices: pd.DataFrame,
    *,
    weights: Mapping[str, float],
    config: ETFConfigBundle,
    start: date,
    end: date,
) -> dict[str, Any]:
    close_pivot = _price_pivot(prices, config.backtest.backtest.price_field)
    trading_dates = [
        item.date()
        for item in close_pivot.index
        if start <= item.date() <= end
    ]
    signal_lag_days = int(config.backtest.backtest.signal_lag_days)
    if len(trading_dates) < signal_lag_days + 2:
        raise WeightCalibrationError("ETF weight search has too few trading dates")
    rows: list[dict[str, Any]] = []
    returns: list[float] = []
    turnovers: list[float] = []
    exposures: list[float] = []
    previous_weights: dict[str, float] | None = None
    equity = 1.0
    clean_weights = {str(symbol): float(value) for symbol, value in weights.items()}
    for index, signal_date in enumerate(trading_dates):
        execution_index = index + signal_lag_days
        return_index = execution_index + 1
        if return_index >= len(trading_dates):
            break
        execution_date = trading_dates[execution_index]
        return_date = trading_dates[return_index]
        accounting = calculate_portfolio_accounting_step(
            close_pivot,
            signal_date=signal_date,
            execution_date=execution_date,
            return_date=return_date,
            target_weights=clean_weights,
            previous_weights=previous_weights,
            asset_symbols=tuple(config.assets.assets),
            total_cost_bps=_total_cost_bps(config),
            starting_equity=equity,
        )
        equity = accounting.ending_equity
        returns.append(accounting.strategy_return)
        turnovers.append(accounting.turnover)
        exposures.append(1.0 - clean_weights.get("CASH", 0.0))
        rows.append(
            {
                "signal_date": signal_date.isoformat(),
                "execution_date": execution_date.isoformat(),
                "return_date": return_date.isoformat(),
                "strategy_return": accounting.strategy_return,
                "gross_return": accounting.gross_return,
                "turnover": accounting.turnover,
                "transaction_cost": accounting.transaction_cost,
                "portfolio_equity": equity,
                "target_weights_json": json.dumps(
                    clean_weights,
                    ensure_ascii=False,
                    sort_keys=True,
                ),
                "asset_returns_json": json.dumps(
                    accounting.period_returns,
                    ensure_ascii=False,
                    sort_keys=True,
                ),
                "asset_contributions_json": json.dumps(
                    accounting.asset_contributions,
                    ensure_ascii=False,
                    sort_keys=True,
                ),
            }
        )
        previous_weights = clean_weights
    metrics = summarize_long_only_backtest(returns, exposures, turnovers)
    return {
        "daily": pd.DataFrame(rows),
        "metrics": metrics,
        "first_signal_date": rows[0]["signal_date"],
        "last_signal_date": rows[-1]["signal_date"],
        "row_count": len(rows),
    }


def _run_benchmark_set_backtests(
    prices: pd.DataFrame,
    *,
    config: ETFConfigBundle,
    registry: ETFWeightSearchRegistry,
    search: ETFWeightSearchDefinition,
    start: date,
    end: date,
) -> dict[str, dict[str, Any]]:
    benchmark_set = registry.benchmark_sets[search.benchmark_set]
    benchmark_results: dict[str, dict[str, Any]] = {}
    for benchmark_id in benchmark_set.benchmark_ids:
        benchmark = config.backtest.backtest.benchmarks.get(benchmark_id)
        if benchmark is None:
            benchmark_results[benchmark_id] = {
                "status": "MISSING",
                "metric_null_reasons": {"benchmark": "benchmark_id_missing"},
            }
            continue
        try:
            weights = _benchmark_static_weights(benchmark)
            result = _run_static_weight_backtest(
                prices,
                weights=weights,
                config=config,
                start=start,
                end=end,
            )
        except WeightCalibrationError as exc:
            benchmark_results[benchmark_id] = {
                "status": "MISSING",
                "benchmark_name": benchmark.name,
                "metric_null_reasons": {"benchmark": str(exc)},
            }
            continue
        benchmark_results[benchmark_id] = {
            "status": "AVAILABLE",
            "benchmark_name": benchmark.name,
            "benchmark_type": benchmark.benchmark_type,
            **_metrics_payload(result["metrics"]),
        }
    return benchmark_results


def _candidate_metric_row(
    *,
    candidate_id: str,
    weights: Mapping[str, float],
    candidate_metrics: BacktestMetrics,
    baseline_metrics: BacktestMetrics,
    benchmark_results: Mapping[str, Mapping[str, Any]],
    objective: ETFWeightObjectivePolicy,
    search: ETFWeightSearchDefinition,
    semiconductor_symbols: set[str],
    baseline_weights: Mapping[str, float],
    robustness_score: float,
    robustness_summary: Mapping[str, Any],
) -> dict[str, Any]:
    benchmark_comparison = _benchmark_comparison(candidate_metrics, benchmark_results)
    turnover_vs_baseline = sum(
        abs(float(weights.get(symbol, 0.0)) - float(baseline_weights.get(symbol, 0.0)))
        for symbol in set(weights) | set(baseline_weights)
    )
    excess_return = candidate_metrics.total_return - baseline_metrics.total_return
    drawdown_reduction = abs(baseline_metrics.max_drawdown) - abs(
        candidate_metrics.max_drawdown
    )
    component_scores = _historical_component_scores(
        candidate_metrics=candidate_metrics,
        excess_return=excess_return,
        drawdown_reduction=drawdown_reduction,
        turnover_vs_baseline=turnover_vs_baseline,
        weights=weights,
        objective=objective,
        robustness_score=robustness_score,
    )
    hard_blockers = _candidate_hard_blockers(
        weights=weights,
        candidate_metrics=candidate_metrics,
        benchmark_comparison=benchmark_comparison,
        turnover_vs_baseline=turnover_vs_baseline,
        objective=objective,
        search=search,
        semiconductor_symbols=semiconductor_symbols,
    )
    candidate_score = (
        0.0
        if hard_blockers
        else sum(
            float(objective.component_weights[name]) * float(component_scores[name])
            for name in objective.component_weights
        )
    )
    status = "ranked"
    if hard_blockers:
        status = "blocked" if _blocking_safety_issue(hard_blockers) else "rejected"
    return {
        "candidate_id": candidate_id,
        "candidate_status": status,
        "candidate_score": round(candidate_score, 6),
        "total_return": candidate_metrics.total_return,
        "CAGR": candidate_metrics.cagr,
        "max_drawdown": candidate_metrics.max_drawdown,
        "Sharpe": candidate_metrics.sharpe,
        "Sortino": candidate_metrics.sortino,
        "Calmar": candidate_metrics.calmar,
        "turnover": candidate_metrics.turnover,
        "turnover_vs_baseline": turnover_vs_baseline,
        "excess_return_vs_baseline": excess_return,
        "drawdown_reduction_vs_baseline": drawdown_reduction,
        "benchmark_comparison": benchmark_comparison,
        "component_scores": component_scores,
        "robustness_summary": dict(robustness_summary),
        "regime_robustness_score": robustness_score,
        "hard_blockers": hard_blockers,
        "ranking_reason": _ranking_reasons(component_scores, hard_blockers),
        "weights": dict(weights),
        "observe_only": True,
        "candidate_only": True,
        "production_effect": "none",
        "broker_action": "none",
        "manual_review_required": True,
    }


def _empty_candidate_weight_registry() -> dict[str, Any]:
    return {
        "schema_version": CANDIDATE_WEIGHT_REGISTRY_SCHEMA_VERSION,
        "registry_type": "etf_candidate_weight_sets",
        "updated_at": None,
        "candidate_count": 0,
        "weight_sets": [],
        "safety": dict(WEIGHT_CALIBRATION_SAFETY),
        **WEIGHT_CALIBRATION_SAFETY,
    }


def _empty_weight_forward_enrollment_registry() -> dict[str, Any]:
    return {
        "schema_version": WEIGHT_FORWARD_ENROLLMENT_SCHEMA_VERSION,
        "registry_type": "etf_weight_forward_enrollments",
        "updated_at": None,
        "enrollment_count": 0,
        "enrollments": [],
        "latest_selection": {"selected_at": None, "weight_set_ids": []},
        "shared_shadow_registry_mutated": False,
        "production_weights_mutated": False,
        "applied_weight_set": None,
        "safety": dict(WEIGHT_CALIBRATION_SAFETY),
        **WEIGHT_CALIBRATION_SAFETY,
    }


def _selected_candidate_weight_records(
    registry: Mapping[str, Any],
    *,
    top: int | None,
    weight_set_ids: list[str] | None,
) -> list[dict[str, Any]]:
    records = sorted(
        _records(registry.get("weight_sets")),
        key=lambda item: (int(item.get("rank") or 999_999), str(item.get("weight_set_id"))),
    )
    requested = {str(item) for item in weight_set_ids or []}
    if requested:
        selected = [
            record
            for record in records
            if str(record.get("weight_set_id")) in requested
            or str(record.get("source_candidate_id")) in requested
        ]
        matched = {
            str(record.get("weight_set_id")) for record in selected
        } | {str(record.get("source_candidate_id")) for record in selected}
        missing = sorted(requested - matched)
        if missing:
            raise WeightCalibrationError(
                "unknown candidate weight set id(s): " + ", ".join(missing)
            )
        return selected
    limit = top or 1
    if limit <= 0:
        raise WeightCalibrationError("top must be positive")
    return records[:limit]


def _raise_if_candidate_weight_forward_enrollable(record: Mapping[str, Any]) -> None:
    validate_candidate_weight_record(record)
    weight_set_id = str(record.get("weight_set_id"))
    status = str(record.get("status"))
    if status not in FORWARD_ENROLLABLE_CANDIDATE_STATUSES:
        raise WeightCalibrationError(
            "candidate weight set cannot enroll forward unless status is "
            f"candidate/shadow_ready: {weight_set_id}"
        )
    if record.get("blockers"):
        raise WeightCalibrationError(
            f"blocked candidate weight set cannot enroll forward: {weight_set_id}"
        )


def _weight_forward_enrollment_record(
    candidate: Mapping[str, Any],
    *,
    enrollment_path: Path,
    enrolled_at: datetime,
) -> dict[str, Any]:
    timestamp = enrolled_at.isoformat()
    enrollment_date = enrolled_at.date().isoformat()
    weight_set_id = str(candidate.get("weight_set_id"))
    shadow_id = _weight_forward_shadow_id(weight_set_id)
    tracking_link = f"{enrollment_path}#{shadow_id}"
    tracking_state = {
        "tracking_status": "active",
        "tracking_started_at": timestamp,
        "tracking_start_date": enrollment_date,
        "cadence": "daily",
        "evidence_status": "needs_more_forward_data",
        "forward_tracking_link": tracking_link,
        "weekly_review_task": "TRADING-068",
        "decision_journal_task": "TRADING-069",
        "parameter_review_task": "TRADING-070",
        "calibration_task": "TRADING-071E",
    }
    shadow_record = {
        "record_type": "etf_weight_calibration_shadow_candidate",
        "shadow_id": shadow_id,
        "candidate_id": str(candidate.get("source_candidate_id") or weight_set_id),
        "weight_set_id": weight_set_id,
        "source_search_run_id": candidate.get("source_search_run_id"),
        "source_candidate_id": candidate.get("source_candidate_id"),
        "rank": candidate.get("rank"),
        "weights": dict(_mapping(candidate.get("weights"))),
        "status": "active",
        "enrolled_at": timestamp,
        "enrollment_date": enrollment_date,
        "evaluation_schedule": {
            "cadence": "daily",
            "start_date": enrollment_date,
            "weekly_review_task": "TRADING-068",
        },
        "tracking_state": dict(tracking_state),
        "observe_only": True,
        "candidate_only": True,
        "production_effect": "none",
        "broker_action": "none",
        "manual_review_required": True,
        "production_promotion_allowed": False,
        "production_weights_mutated": False,
        "applied_weight_set": None,
    }
    record = {
        "shadow_id": shadow_id,
        "weight_set_id": weight_set_id,
        "source_search_run_id": candidate.get("source_search_run_id"),
        "source_candidate_id": candidate.get("source_candidate_id"),
        "rank": int(candidate.get("rank") or 0),
        "status": "active",
        "weights": dict(_mapping(candidate.get("weights"))),
        "metrics_summary": dict(_mapping(candidate.get("metrics_summary"))),
        "robustness_summary": dict(_mapping(candidate.get("robustness_summary"))),
        "blockers": [str(item) for item in candidate.get("blockers") or []],
        "selection_reason": candidate.get("selection_reason"),
        "config_hash": candidate.get("config_hash"),
        "enrolled_at": timestamp,
        "enrollment_date": enrollment_date,
        "forward_tracking_link": tracking_link,
        "tracking_state": tracking_state,
        "shadow_record": shadow_record,
        "shared_shadow_registry_mutated": False,
        "production_weights_mutated": False,
        "applied_weight_set": None,
        "production_promotion_allowed": False,
        "safety": dict(WEIGHT_CALIBRATION_SAFETY),
        **WEIGHT_CALIBRATION_SAFETY,
    }
    validate_weight_forward_enrollment_record(record)
    return record


def _weight_forward_shadow_id(weight_set_id: str) -> str:
    return f"etf_weight_shadow_{sha256(weight_set_id.encode('utf-8')).hexdigest()[:16]}"


def _backtest_forward_evidence_record(
    enrollment: Mapping[str, Any],
    *,
    as_of: date,
    candidate_registry: Mapping[str, Any],
    search_payload: Mapping[str, Any],
    forward_dashboard: Mapping[str, Any],
    weekly_review: Mapping[str, Any],
    decision_journal: Mapping[str, Any],
    parameter_review: Mapping[str, Any],
    generated_at: datetime,
    source_paths: Mapping[str, str],
) -> dict[str, Any]:
    weight_set_id = str(enrollment.get("weight_set_id"))
    candidate = _candidate_weight_record_for_id(candidate_registry, weight_set_id)
    historical = _historical_expectation_for_candidate(
        candidate,
        search_payload=search_payload,
    )
    forward_row = _forward_row_for_enrollment(enrollment, forward_dashboard)
    forward = _forward_realized_metrics(forward_row)
    gaps = _evidence_gaps(historical, forward)
    source_links = _evidence_source_links(
        enrollment,
        forward_row=forward_row,
        weekly_review=weekly_review,
        decision_journal=decision_journal,
        parameter_review=parameter_review,
        source_paths=source_paths,
    )
    status, reasons = _forward_evidence_status(
        candidate=candidate,
        enrollment=enrollment,
        forward_row=forward_row,
        gaps=gaps,
    )
    record = {
        "evidence_id": _stable_evidence_id(weight_set_id, as_of),
        "weight_set_id": weight_set_id,
        "shadow_id": enrollment.get("shadow_id"),
        "source_search_run_id": enrollment.get("source_search_run_id"),
        "source_candidate_id": enrollment.get("source_candidate_id"),
        "rank": int(enrollment.get("rank") or 0),
        "generated_at": generated_at.isoformat(),
        "as_of": as_of.isoformat(),
        "candidate_status": candidate.get("status"),
        "enrollment_status": enrollment.get("status"),
        "forward_days": int(forward.get("forward_days") or 0),
        "backtest_expected_return": historical["return"],
        "forward_realized_return": forward["return"],
        "expectation_gap": gaps["return_gap"],
        "backtest_expected_drawdown": historical["drawdown"],
        "forward_realized_drawdown": forward["drawdown"],
        "drawdown_gap": gaps["drawdown_gap"],
        "backtest_expected_turnover": historical["turnover"],
        "forward_realized_turnover": forward["turnover"],
        "turnover_gap": gaps["turnover_gap"],
        "backtest_expected_stability": historical["stability"],
        "forward_realized_stability": forward["stability"],
        "stability_gap": gaps["stability_gap"],
        "evidence_status": status,
        "status_reasons": reasons,
        "metric_null_reasons": forward.get("metric_null_reasons", {}),
        "source_links": source_links,
        "production_weights_mutated": False,
        "applied_weight_set": None,
        "safety": dict(WEIGHT_CALIBRATION_SAFETY),
        **WEIGHT_CALIBRATION_SAFETY,
    }
    validate_backtest_forward_evidence_record(record)
    return record


def _candidate_weight_record_for_id(
    registry: Mapping[str, Any],
    weight_set_id: str,
) -> dict[str, Any]:
    for record in _records(registry.get("weight_sets")):
        if str(record.get("weight_set_id")) == weight_set_id:
            return record
    raise WeightCalibrationError(f"candidate weight_set_id not found: {weight_set_id}")


def _historical_expectation_for_candidate(
    candidate: Mapping[str, Any],
    *,
    search_payload: Mapping[str, Any],
) -> dict[str, float | None]:
    metrics = dict(_mapping(candidate.get("metrics_summary")))
    source_candidate_id = str(candidate.get("source_candidate_id") or "")
    if search_payload:
        search_metrics = _optional_candidate_metrics(search_payload, source_candidate_id)
        if search_metrics:
            metrics.update(search_metrics)
    robustness = _mapping(candidate.get("robustness_summary"))
    if search_payload:
        candidate_payload = _optional_candidate_weight_set(search_payload, source_candidate_id)
        robustness = _mapping(candidate_payload.get("robustness_summary")) or robustness
    return {
        "return": _float_or_none(metrics.get("total_return")),
        "drawdown": _float_or_none(metrics.get("max_drawdown")),
        "turnover": _float_or_none(metrics.get("turnover_vs_baseline")),
        "stability": _float_or_none(robustness.get("stability_score")),
    }


def _forward_realized_metrics(row: Mapping[str, Any] | None) -> dict[str, Any]:
    if not row:
        return {
            "forward_days": 0,
            "return": None,
            "drawdown": None,
            "turnover": None,
            "stability": None,
            "metric_null_reasons": {"forward_row": "FORWARD_EVIDENCE_NOT_FOUND"},
        }
    days = int(_float_or_none(row.get("days_since_enrollment")) or 0)
    metric_null_reasons = dict(_mapping(row.get("metric_null_reasons")))
    metrics = {
        "forward_days": days,
        "return": _float_or_none(row.get("return_since_enrollment")),
        "drawdown": _float_or_none(row.get("max_drawdown_since_enrollment")),
        "turnover": _float_or_none(row.get("turnover_since_enrollment")),
        "stability": _float_or_none(row.get("weight_stability_score")),
        "metric_null_reasons": metric_null_reasons,
    }
    for field, value in metrics.items():
        if field in {"forward_days", "metric_null_reasons"}:
            continue
        if value is None:
            metric_null_reasons.setdefault(field, f"{field} unavailable in forward evidence")
    return metrics


def _evidence_gaps(
    historical: Mapping[str, float | None],
    forward: Mapping[str, Any],
) -> dict[str, float | None]:
    historical_drawdown = _float_or_none(historical.get("drawdown"))
    forward_drawdown = _float_or_none(forward.get("drawdown"))
    drawdown_gap = None
    if historical_drawdown is not None and forward_drawdown is not None:
        drawdown_gap = abs(forward_drawdown) - abs(historical_drawdown)
    return {
        "return_gap": _subtract_optional(forward.get("return"), historical.get("return")),
        "drawdown_gap": drawdown_gap,
        "turnover_gap": _subtract_optional(forward.get("turnover"), historical.get("turnover")),
        "stability_gap": _subtract_optional(forward.get("stability"), historical.get("stability")),
    }


def _forward_evidence_status(
    *,
    candidate: Mapping[str, Any],
    enrollment: Mapping[str, Any],
    forward_row: Mapping[str, Any] | None,
    gaps: Mapping[str, float | None],
) -> tuple[str, list[str]]:
    if str(candidate.get("status")) in {"blocked", "rejected"} or candidate.get("blockers"):
        return "blocked", ["candidate_weight_set_blocked"]
    if str(enrollment.get("status")) not in {"active", "needs_more_forward_data"}:
        return "blocked", [f"enrollment_status_not_active:{enrollment.get('status')}"]
    if not forward_row:
        return "needs_more_forward_data", ["forward_evidence_not_found"]
    minimum_days = int(WEIGHT_FORWARD_EVIDENCE_POLICY["min_forward_days"])
    days = int(_float_or_none(forward_row.get("days_since_enrollment")) or 0)
    if days < minimum_days:
        return "needs_more_forward_data", [f"forward_days_below_minimum:{days}<{minimum_days}"]
    if gaps.get("return_gap") is None or gaps.get("drawdown_gap") is None:
        return "needs_more_forward_data", ["required_forward_gap_metric_missing"]
    return_gap = float(gaps["return_gap"])
    drawdown_gap = float(gaps["drawdown_gap"])
    turnover_gap = _float_or_none(gaps.get("turnover_gap")) or 0.0
    stability_gap = _float_or_none(gaps.get("stability_gap")) or 0.0
    return_tolerance = float(WEIGHT_FORWARD_EVIDENCE_POLICY["return_gap_tolerance"])
    drawdown_tolerance = float(WEIGHT_FORWARD_EVIDENCE_POLICY["drawdown_gap_tolerance"])
    turnover_tolerance = float(WEIGHT_FORWARD_EVIDENCE_POLICY["turnover_gap_tolerance"])
    stability_tolerance = float(WEIGHT_FORWARD_EVIDENCE_POLICY["stability_gap_tolerance"])
    better = return_gap >= return_tolerance
    worse = return_gap <= -return_tolerance
    risk_worse = (
        drawdown_gap > drawdown_tolerance
        or turnover_gap > turnover_tolerance
        or stability_gap < -stability_tolerance
    )
    risk_better = (
        drawdown_gap < -drawdown_tolerance
        or turnover_gap < -turnover_tolerance
        or stability_gap > stability_tolerance
    )
    reasons = [
        f"return_gap={return_gap:.4f}",
        f"drawdown_gap={drawdown_gap:.4f}",
        f"turnover_gap={turnover_gap:.4f}",
        f"stability_gap={stability_gap:.4f}",
    ]
    if better and risk_worse:
        return "mixed", [*reasons, "return_better_but_risk_worse"]
    if worse or risk_worse:
        return "forward_worse_than_backtest", reasons
    if better and not risk_worse:
        return "forward_better_than_backtest", reasons
    if risk_better:
        return "forward_better_than_backtest", reasons
    return "consistent", reasons


def _forward_row_for_enrollment(
    enrollment: Mapping[str, Any],
    forward_dashboard: Mapping[str, Any],
) -> dict[str, Any] | None:
    identifiers = {
        str(enrollment.get("weight_set_id")),
        str(enrollment.get("shadow_id")),
        str(enrollment.get("source_candidate_id")),
        str(_mapping(enrollment.get("shadow_record")).get("candidate_id")),
    }
    identifiers = {item for item in identifiers if item and item != "None"}
    for row in _records(forward_dashboard.get("candidate_summary_table")):
        row_ids = {
            str(row.get("weight_set_id")),
            str(row.get("shadow_id")),
            str(row.get("candidate_id")),
            str(row.get("source_candidate_id")),
        }
        if identifiers & {item for item in row_ids if item and item != "None"}:
            return row
    return None


def _evidence_source_links(
    enrollment: Mapping[str, Any],
    *,
    forward_row: Mapping[str, Any] | None,
    weekly_review: Mapping[str, Any],
    decision_journal: Mapping[str, Any],
    parameter_review: Mapping[str, Any],
    source_paths: Mapping[str, str],
) -> dict[str, Any]:
    weight_set_id = str(enrollment.get("weight_set_id"))
    source_candidate_id = str(enrollment.get("source_candidate_id"))
    return {
        "historical_search": source_paths.get("historical_search", ""),
        "candidate_registry": source_paths.get("candidate_registry", ""),
        "forward_enrollment": source_paths.get("forward_enrollment", ""),
        "forward_dashboard": source_paths.get("forward_dashboard", ""),
        "weekly_review": source_paths.get("weekly_review", ""),
        "decision_journal": source_paths.get("decision_journal", ""),
        "parameter_review": source_paths.get("parameter_review", ""),
        "forward_row_found": forward_row is not None,
        "weekly_review_links": _records_matching_candidate(
            weekly_review,
            candidate_ids={weight_set_id, source_candidate_id, str(enrollment.get("shadow_id"))},
        ),
        "decision_journal_links": _records_matching_candidate(
            decision_journal,
            candidate_ids={weight_set_id, source_candidate_id},
        ),
        "parameter_review_links": _records_matching_candidate(
            parameter_review,
            candidate_ids={weight_set_id, source_candidate_id},
        ),
    }


def _records_matching_candidate(
    payload: Mapping[str, Any],
    *,
    candidate_ids: set[str],
) -> list[dict[str, Any]]:
    ids = {item for item in candidate_ids if item and item != "None"}
    if not payload or not ids:
        return []
    matches: list[dict[str, Any]] = []
    for key in (
        "candidate_summary_table",
        "active_candidates",
        "manual_review_actions",
        "entries",
        "evidence_records",
    ):
        for row in _records(payload.get(key)):
            row_text = json.dumps(row, ensure_ascii=False, sort_keys=True)
            if any(candidate_id in row_text for candidate_id in ids):
                matches.append(row)
    sections = _mapping(payload.get("sections"))
    for section in sections.values():
        if isinstance(section, Mapping):
            matches.extend(_records_matching_candidate(section, candidate_ids=ids))
    return matches


def _status_counts(records: list[Mapping[str, Any]]) -> dict[str, int]:
    counts = {status: 0 for status in sorted(WEIGHT_FORWARD_EVIDENCE_STATUSES)}
    for record in records:
        status = str(record.get("evidence_status") or "needs_more_forward_data")
        if status not in counts:
            counts[status] = 0
        counts[status] += 1
    return counts


def _optional_candidate_metrics(
    search_payload: Mapping[str, Any],
    candidate_id: str,
) -> dict[str, Any]:
    for row in _records(search_payload.get("metrics")):
        if str(row.get("candidate_id")) == candidate_id:
            return row
    return {}


def _optional_candidate_weight_set(
    search_payload: Mapping[str, Any],
    candidate_id: str,
) -> dict[str, Any]:
    for row in _records(search_payload.get("candidate_weight_sets")):
        if str(row.get("candidate_id")) == candidate_id:
            return row
    return {}


def _stable_evidence_id(weight_set_id: str, as_of: date) -> str:
    basis = f"{weight_set_id}|{as_of.isoformat()}"
    return "etf-weight-evidence-" + sha256(basis.encode("utf-8")).hexdigest()[:12]


def _weight_overfit_candidate_diagnostics(
    candidate: Mapping[str, Any],
    *,
    search_payload: Mapping[str, Any],
    evidence_payload: Mapping[str, Any],
) -> dict[str, Any]:
    component_diagnostics = {
        "performance_concentration": _performance_concentration_diagnostic(
            candidate,
            search_payload,
        ),
        "single_period_dependency": _single_period_dependency_diagnostic(
            candidate,
            search_payload,
        ),
        "regime_fragility": _regime_fragility_diagnostic(candidate),
        "turnover_instability": _turnover_instability_diagnostic(candidate),
        "constraint_hit_instability": _constraint_hit_instability_diagnostic(
            candidate,
            search_payload,
        ),
        "weight_extremeness": _weight_extremeness_diagnostic(candidate),
        "benchmark_dependency": _benchmark_dependency_diagnostic(candidate),
        "forward_backtest_divergence": _forward_backtest_divergence_diagnostic(
            candidate,
            evidence_payload,
        ),
    }
    weights = _mapping(WEIGHT_OVERFIT_DIAGNOSTICS_POLICY["component_weights"])
    risk_score = sum(
        float(weights[key]) * float(component_diagnostics[key]["risk_score"])
        for key in weights
    )
    risk_score = round(max(0.0, min(1.0, risk_score)), 6)
    reason_codes = [
        reason
        for diagnostic in component_diagnostics.values()
        for reason in diagnostic.get("reason_codes", [])
    ]
    return {
        "weight_set_id": candidate.get("weight_set_id"),
        "source_search_run_id": candidate.get("source_search_run_id"),
        "source_candidate_id": candidate.get("source_candidate_id"),
        "rank": int(candidate.get("rank") or 0),
        "overfit_risk_score": risk_score,
        "overfit_risk_band": weight_overfit_risk_band(risk_score),
        "component_diagnostics": component_diagnostics,
        "reason_codes": sorted(set(reason_codes)) or ["NO_OVERFIT_RISK_FLAGS"],
        "safety": dict(WEIGHT_CALIBRATION_SAFETY),
        **WEIGHT_CALIBRATION_SAFETY,
    }


def _performance_concentration_diagnostic(
    candidate: Mapping[str, Any],
    search_payload: Mapping[str, Any],
) -> dict[str, Any]:
    returns = _available_slice_values(candidate, search_payload, "excess_return_vs_baseline")
    positives = [max(0.0, value) for value in returns]
    total_positive = sum(positives)
    concentration = 0.0 if total_positive <= 0 else max(positives) / total_positive
    reference = float(WEIGHT_OVERFIT_DIAGNOSTICS_POLICY["performance_concentration_reference"])
    score = _positive_scaled(concentration - 0.30, reference - 0.30)
    return _component_result(
        "performance_concentration",
        score,
        {"positive_slice_concentration": concentration, "available_slice_count": len(returns)},
        ["PERFORMANCE_CONCENTRATED_IN_FEW_SLICES"] if score >= 0.5 else [],
    )


def _single_period_dependency_diagnostic(
    candidate: Mapping[str, Any],
    search_payload: Mapping[str, Any],
) -> dict[str, Any]:
    values = _available_slice_values(candidate, search_payload, "return")
    if len(values) < 2:
        score = 0.0
        metrics = {"available_slice_count": len(values), "dispersion": 0.0}
    else:
        average_abs = sum(abs(value) for value in values) / len(values)
        dispersion = 0.0 if average_abs == 0 else (max(values) - min(values)) / average_abs
        score = max(0.0, min(1.0, dispersion / 4.0))
        metrics = {"available_slice_count": len(values), "dispersion": dispersion}
    return _component_result(
        "single_period_dependency",
        score,
        metrics,
        ["SINGLE_PERIOD_DEPENDENCY"] if score >= 0.5 else [],
    )


def _regime_fragility_diagnostic(candidate: Mapping[str, Any]) -> dict[str, Any]:
    robustness = _mapping(candidate.get("robustness_summary"))
    stability = _float_or_none(robustness.get("stability_score"))
    weak_count = int(_float_or_none(robustness.get("weak_slice_count")) or 0)
    score = max(0.0, min(1.0, 1.0 - (stability if stability is not None else 0.0)))
    if weak_count:
        score = max(score, min(1.0, weak_count / 4.0))
    return _component_result(
        "regime_fragility",
        score,
        {"stability_score": stability, "weak_slice_count": weak_count},
        ["REGIME_FRAGILITY"] if score >= 0.5 else [],
    )


def _turnover_instability_diagnostic(candidate: Mapping[str, Any]) -> dict[str, Any]:
    metrics = _mapping(candidate.get("metrics_summary"))
    turnover = _float_or_none(metrics.get("turnover_vs_baseline")) or 0.0
    reference = float(WEIGHT_OVERFIT_DIAGNOSTICS_POLICY["turnover_reference"])
    score = max(0.0, min(1.0, turnover / reference))
    return _component_result(
        "turnover_instability",
        score,
        {"turnover_vs_baseline": turnover},
        ["HIGH_TURNOVER_INSTABILITY"] if score >= 0.5 else [],
    )


def _constraint_hit_instability_diagnostic(
    candidate: Mapping[str, Any],
    search_payload: Mapping[str, Any],
) -> dict[str, Any]:
    rates = _available_slice_values(candidate, search_payload, "constraint_hit_rate")
    max_rate = max(rates) if rates else 0.0
    reference = float(WEIGHT_OVERFIT_DIAGNOSTICS_POLICY["constraint_hit_reference"])
    score = max(0.0, min(1.0, max_rate / reference)) if reference > 0 else 0.0
    return _component_result(
        "constraint_hit_instability",
        score,
        {"max_constraint_hit_rate": max_rate, "available_slice_count": len(rates)},
        ["CONSTRAINT_HIT_INSTABILITY"] if score >= 0.5 else [],
    )


def _weight_extremeness_diagnostic(candidate: Mapping[str, Any]) -> dict[str, Any]:
    weights = {key: float(value) for key, value in _mapping(candidate.get("weights")).items()}
    max_weight = max(weights.values()) if weights else 0.0
    herfindahl = sum(value * value for value in weights.values())
    reference = float(WEIGHT_OVERFIT_DIAGNOSTICS_POLICY["weight_extremeness_reference"])
    max_score = max(0.0, min(1.0, max_weight / reference))
    herfindahl_score = max(0.0, min(1.0, (herfindahl - 0.20) / 0.80))
    score = max(max_score, herfindahl_score)
    return _component_result(
        "weight_extremeness",
        score,
        {"max_weight": max_weight, "herfindahl": herfindahl},
        ["EXTREME_WEIGHT_CONCENTRATION"] if score >= 0.75 else [],
    )


def _benchmark_dependency_diagnostic(candidate: Mapping[str, Any]) -> dict[str, Any]:
    metrics = _mapping(candidate.get("metrics_summary"))
    benchmark = _mapping(metrics.get("benchmark_comparison"))
    if not benchmark:
        return _component_result(
            "benchmark_dependency",
            1.0,
            {"available_benchmark_count": 0, "non_positive_excess_count": 0},
            ["NO_BENCHMARK_COMPARISON"],
        )
    excess_values = [
        _float_or_none(_mapping(row).get("excess_return"))
        for row in benchmark.values()
        if isinstance(row, Mapping)
    ]
    excess_values = [value for value in excess_values if value is not None]
    if not excess_values:
        score = 1.0
    else:
        score = sum(1 for value in excess_values if value <= 0) / len(excess_values)
    return _component_result(
        "benchmark_dependency",
        score,
        {
            "available_benchmark_count": len(excess_values),
            "non_positive_excess_count": sum(1 for value in excess_values if value <= 0),
        },
        ["BENCHMARK_DEPENDENCY"] if score >= 0.5 else [],
    )


def _forward_backtest_divergence_diagnostic(
    candidate: Mapping[str, Any],
    evidence_payload: Mapping[str, Any],
) -> dict[str, Any]:
    record = _evidence_record_for_weight_set(
        evidence_payload,
        str(candidate.get("weight_set_id")),
    )
    if not record:
        return _component_result(
            "forward_backtest_divergence",
            0.25,
            {"evidence_status": "missing"},
            ["FORWARD_EVIDENCE_MISSING"],
        )
    status = str(record.get("evidence_status"))
    gap = abs(_float_or_none(record.get("expectation_gap")) or 0.0)
    drawdown_gap = max(0.0, _float_or_none(record.get("drawdown_gap")) or 0.0)
    base_score = {
        "consistent": 0.0,
        "forward_better_than_backtest": 0.10,
        "needs_more_forward_data": 0.25,
        "mixed": 0.60,
        "forward_worse_than_backtest": 0.85,
        "blocked": 1.0,
    }.get(status, 0.25)
    score = max(base_score, min(1.0, gap / 0.10), min(1.0, drawdown_gap / 0.10))
    return _component_result(
        "forward_backtest_divergence",
        score,
        {
            "evidence_status": status,
            "expectation_gap_abs": gap,
            "drawdown_gap": drawdown_gap,
        },
        ["FORWARD_BACKTEST_DIVERGENCE"] if score >= 0.5 else [],
    )


def _available_slice_values(
    candidate: Mapping[str, Any],
    search_payload: Mapping[str, Any],
    field: str,
) -> list[float]:
    source_candidate_id = str(candidate.get("source_candidate_id"))
    robustness = _mapping(search_payload.get("robustness_evaluation"))
    for payload in _records(robustness.get("candidate_evaluations")):
        if str(payload.get("candidate_id")) != source_candidate_id:
            continue
        values = [
            _float_or_none(slice_metric.get(field))
            for slice_metric in _records(payload.get("slice_metrics"))
            if slice_metric.get("status") == "AVAILABLE"
        ]
        return [value for value in values if value is not None]
    return []


def _component_result(
    diagnostic_id: str,
    score: float,
    metrics: Mapping[str, Any],
    reason_codes: list[str],
) -> dict[str, Any]:
    normalized = round(max(0.0, min(1.0, float(score))), 6)
    return {
        "diagnostic_id": diagnostic_id,
        "risk_score": normalized,
        "risk_band": weight_overfit_risk_band(normalized),
        "metrics": dict(metrics),
        "reason_codes": reason_codes,
    }


def _risk_band_counts(records: list[Mapping[str, Any]]) -> dict[str, int]:
    counts = {"low": 0, "medium": 0, "high": 0, "critical": 0}
    for record in records:
        band = str(record.get("overfit_risk_band"))
        if band in counts:
            counts[band] += 1
    return counts


def _highest_risk_candidate(records: list[Mapping[str, Any]]) -> dict[str, Any] | None:
    if not records:
        return None
    selected = max(
        records,
        key=lambda record: (
            float(record.get("overfit_risk_score") or 0.0),
            str(record.get("weight_set_id")),
        ),
    )
    return {
        "weight_set_id": selected.get("weight_set_id"),
        "overfit_risk_score": selected.get("overfit_risk_score"),
        "overfit_risk_band": selected.get("overfit_risk_band"),
    }


def _evidence_record_for_weight_set(
    evidence_payload: Mapping[str, Any],
    weight_set_id: str,
) -> dict[str, Any]:
    for record in _records(evidence_payload.get("evidence_records")):
        if str(record.get("weight_set_id")) == weight_set_id:
            return record
    return {}


def _selected_ranked_candidates(
    search_payload: Mapping[str, Any],
    *,
    top: int | None,
    weight_set_ids: list[str] | None,
) -> list[dict[str, Any]]:
    ranking = _records(search_payload.get("ranking"))
    requested = {str(item) for item in weight_set_ids or []}
    if requested:
        selected = [
            row
            for row in ranking
            if str(row.get("candidate_id")) in requested
            or _weight_set_id(search_payload, row) in requested
        ]
        matched = {
            str(row.get("candidate_id")) for row in selected
        } | {_weight_set_id(search_payload, row) for row in selected}
        missing = sorted(requested - matched)
        if missing:
            raise WeightCalibrationError(
                "unknown candidate weight set id(s): " + ", ".join(missing)
            )
        return selected
    limit = top or 1
    if limit <= 0:
        raise WeightCalibrationError("top must be positive")
    return ranking[:limit]


def _candidate_weight_record_from_search(
    search_payload: Mapping[str, Any],
    ranked: Mapping[str, Any],
    *,
    created_at: datetime,
) -> dict[str, Any]:
    candidate_id = str(ranked.get("candidate_id"))
    candidate = _candidate_weight_set(search_payload, candidate_id)
    metrics = _candidate_metrics(search_payload, candidate_id)
    blockers = [str(item) for item in ranked.get("hard_blockers") or []]
    candidate_status = str(ranked.get("candidate_status"))
    if blockers:
        status = "blocked" if candidate_status == "blocked" else "rejected"
    elif _mapping(metrics.get("robustness_summary")).get("status") == "INSUFFICIENT_DATA":
        status = "needs_more_data"
    else:
        status = "candidate"
    record = {
        "weight_set_id": _weight_set_id(search_payload, ranked),
        "source_search_run_id": search_payload.get("search_run_id"),
        "source_candidate_id": candidate_id,
        "rank": int(ranked.get("rank")),
        "status": status,
        "weights": dict(candidate.get("weights") or ranked.get("weights") or {}),
        "metrics_summary": {
            "candidate_score": ranked.get("candidate_score"),
            "candidate_status": ranked.get("candidate_status"),
            "total_return": metrics.get("total_return"),
            "excess_return_vs_baseline": metrics.get("excess_return_vs_baseline"),
            "max_drawdown": metrics.get("max_drawdown"),
            "turnover_vs_baseline": metrics.get("turnover_vs_baseline"),
            "component_scores": metrics.get("component_scores"),
        },
        "robustness_summary": dict(
            _mapping(
                candidate.get("robustness_summary")
                or metrics.get("robustness_summary")
            )
        ),
        "blockers": blockers,
        "selection_reason": (
            "top_historical_weight_search_rank"
            if not blockers
            else "historical_search_candidate_blocked"
        ),
        "config_hash": search_payload.get("search_config_hash"),
        "created_at": created_at.isoformat(),
        "source_report_path": "",
        "safety": dict(WEIGHT_CALIBRATION_SAFETY),
        **WEIGHT_CALIBRATION_SAFETY,
    }
    validate_candidate_weight_record(record)
    return record


def _candidate_weight_set(
    search_payload: Mapping[str, Any],
    candidate_id: str,
) -> dict[str, Any]:
    for candidate in _records(search_payload.get("candidate_weight_sets")):
        if str(candidate.get("candidate_id")) == candidate_id:
            return candidate
    raise WeightCalibrationError(
        f"candidate weight set missing from search payload: {candidate_id}"
    )


def _candidate_metrics(
    search_payload: Mapping[str, Any],
    candidate_id: str,
) -> dict[str, Any]:
    for row in _records(search_payload.get("metrics")):
        if str(row.get("candidate_id")) == candidate_id:
            return row
    raise WeightCalibrationError(f"candidate metrics missing from search payload: {candidate_id}")


def _weight_set_id(search_payload: Mapping[str, Any], ranked: Mapping[str, Any]) -> str:
    return f"{search_payload.get('search_run_id')}:{ranked.get('candidate_id')}"


def _historical_component_scores(
    *,
    candidate_metrics: BacktestMetrics,
    excess_return: float,
    drawdown_reduction: float,
    turnover_vs_baseline: float,
    weights: Mapping[str, float],
    objective: ETFWeightObjectivePolicy,
    robustness_score: float,
) -> dict[str, float]:
    scales = objective.component_scales
    sharpe = candidate_metrics.sharpe or 0.0
    nonzero_weights = sum(1 for value in weights.values() if float(value) > 1e-9)
    simplicity = 1.0 - max(0.0, (nonzero_weights - 1) / max(len(weights) - 1, 1))
    return {
        "excess_return_vs_baseline_score": _positive_scaled(
            excess_return,
            scales.get("excess_return_vs_baseline", 0.10),
        ),
        "drawdown_reduction_score": _positive_scaled(
            drawdown_reduction,
            scales.get("drawdown_reduction", 0.10),
        ),
        "risk_adjusted_return_score": _positive_scaled(
            sharpe,
            scales.get("sharpe", 2.0),
        ),
        "turnover_penalty_score": _penalty_score(
            turnover_vs_baseline,
            scales.get("turnover", 1.0),
        ),
        "regime_robustness_score": max(0.0, min(1.0, robustness_score)),
        "simplicity_score": max(0.0, min(1.0, simplicity)),
    }


def _robustness_slice_definitions(
    daily: pd.DataFrame,
    *,
    search: ETFWeightSearchDefinition,
) -> list[dict[str, Any]]:
    frame = _daily_with_slice_features(daily)
    definitions: list[dict[str, Any]] = [
        {
            "slice_id": "full_period",
            "slice_type": "full_period",
            "description": "Full requested historical period.",
            "mask": pd.Series([True] * len(frame), index=frame.index),
        }
    ]
    for window in search.walk_forward_windows:
        signal_dates = pd.to_datetime(frame["signal_date"], errors="coerce")
        definitions.append(
            {
                "slice_id": window.window_id,
                "slice_type": "walk_forward_window",
                "description": window.description,
                "start_date": window.start_date.isoformat(),
                "end_date": window.end_date.isoformat(),
                "mask": (
                    (signal_dates >= pd.Timestamp(window.start_date))
                    & (signal_dates <= pd.Timestamp(window.end_date))
                ),
            }
        )
    regime_masks = {
        "risk_on_periods": frame["_risk_proxy"] == "risk_on",
        "neutral_periods": frame["_risk_proxy"] == "neutral",
        "risk_off_periods": frame["_risk_proxy"] == "risk_off",
        "high_volatility_periods": frame["_high_volatility"],
        "semiconductor_leadership_periods": frame["_semiconductor_leadership"],
        "growth_underperformance_periods": frame["_growth_underperformance"],
    }
    for split_id, split in search.regime_splits.items():
        definitions.append(
            {
                "slice_id": split_id,
                "slice_type": "regime_slice",
                "description": split.description,
                "regime_label": split.regime_label,
                "signal": split.signal,
                "mask": regime_masks.get(
                    split_id,
                    pd.Series([False] * len(frame), index=frame.index),
                ),
            }
        )
    return definitions


def _metrics_for_slice(
    daily: pd.DataFrame,
    *,
    baseline_daily: pd.DataFrame,
    weights: Mapping[str, float],
    semiconductor_symbols: set[str],
    slice_definition: Mapping[str, Any],
) -> dict[str, Any]:
    mask = slice_definition.get("mask")
    if not isinstance(mask, pd.Series):
        selected = daily.iloc[0:0].copy()
    else:
        selected = daily.loc[mask.reindex(daily.index, fill_value=False)].copy()
    common = {
        "slice_id": slice_definition.get("slice_id"),
        "slice_type": slice_definition.get("slice_type"),
        "description": slice_definition.get("description", ""),
        "row_count": int(len(selected)),
    }
    if len(selected) < 2:
        return {
            **common,
            "status": "INSUFFICIENT_DATA",
            "return": None,
            "excess_return_vs_baseline": None,
            "max_drawdown": None,
            "volatility": None,
            "Sharpe": None,
            "Sortino": None,
            "Calmar": None,
            "turnover": None,
            "cash_exposure": float(weights.get("CASH", 0.0)),
            "semiconductor_exposure": sum(
                float(weights.get(symbol, 0.0)) for symbol in semiconductor_symbols
            ),
            "constraint_hit_rate": None,
            "metric_null_reasons": {
                "slice": "insufficient rows for robustness slice metrics"
            },
        }
    returns = [
        float(value)
        for value in pd.to_numeric(selected["strategy_return"], errors="coerce")
        if pd.notna(value)
    ]
    turnovers = [
        float(value)
        for value in pd.to_numeric(selected["turnover"], errors="coerce")
        if pd.notna(value)
    ]
    if len(turnovers) != len(returns):
        turnovers = [0.0] * len(returns)
    exposure = 1.0 - float(weights.get("CASH", 0.0))
    metrics = summarize_long_only_backtest(
        returns,
        [exposure] * len(returns),
        turnovers,
    )
    baseline_returns = _matching_baseline_returns(selected, baseline_daily)
    baseline_return = _compound_returns(baseline_returns) if baseline_returns else None
    candidate_return = metrics.total_return
    drawdown_reduction = None
    if baseline_returns:
        baseline_metrics = summarize_long_only_backtest(
            baseline_returns,
            [exposure] * len(baseline_returns),
            [0.0] * len(baseline_returns),
        )
        drawdown_reduction = abs(baseline_metrics.max_drawdown) - abs(metrics.max_drawdown)
    volatility = pstdev(returns) * sqrt(252) if len(returns) > 1 else None
    return {
        **common,
        "status": "AVAILABLE",
        "return": candidate_return,
        "baseline_return": baseline_return,
        "excess_return_vs_baseline": (
            None if baseline_return is None else candidate_return - baseline_return
        ),
        "max_drawdown": metrics.max_drawdown,
        "drawdown_reduction_vs_baseline": drawdown_reduction,
        "volatility": volatility,
        "Sharpe": metrics.sharpe,
        "Sortino": metrics.sortino,
        "Calmar": metrics.calmar,
        "turnover": metrics.turnover,
        "cash_exposure": float(weights.get("CASH", 0.0)),
        "semiconductor_exposure": sum(
            float(weights.get(symbol, 0.0)) for symbol in semiconductor_symbols
        ),
        "constraint_hit_rate": 0.0,
        "metric_null_reasons": {},
    }


def _candidate_hard_blockers(
    *,
    weights: Mapping[str, float],
    candidate_metrics: BacktestMetrics,
    benchmark_comparison: Mapping[str, Any],
    turnover_vs_baseline: float,
    objective: ETFWeightObjectivePolicy,
    search: ETFWeightSearchDefinition,
    semiconductor_symbols: set[str],
) -> list[str]:
    thresholds = objective.hard_blocker_thresholds
    blockers = []
    if not benchmark_comparison:
        blockers.append("NO_BENCHMARK_COMPARISON")
    max_drawdown_abs = float(thresholds.get("max_drawdown_abs", 0.35))
    if abs(candidate_metrics.max_drawdown) > max_drawdown_abs:
        blockers.append("MAX_DRAWDOWN_TOO_HIGH")
    max_turnover = float(thresholds.get("max_turnover_vs_baseline", 0.80))
    if turnover_vs_baseline > max_turnover:
        blockers.append("TURNOVER_TOO_HIGH")
    semiconductor_total = sum(float(weights.get(symbol, 0.0)) for symbol in semiconductor_symbols)
    if semiconductor_total > search.sleeve_constraints.semiconductor_total_max + 1e-8:
        blockers.append("SEMICONDUCTOR_CAP_VIOLATED")
    cash_weight = float(weights.get("CASH", 0.0))
    if cash_weight < search.weight_constraints["CASH"].min - 1e-8:
        blockers.append("CASH_CONSTRAINT_VIOLATED")
    if search.safety.production_effect != "none":
        blockers.append("UNSAFE_PRODUCTION_EFFECT")
    if search.safety.broker_action != "none":
        blockers.append("BROKER_ACTION_NOT_NONE")
    return [item for item in blockers if item in objective.hard_blockers or item.startswith("NO_")]


def _daily_with_slice_features(daily: pd.DataFrame) -> pd.DataFrame:
    frame = daily.copy()
    asset_returns = [_json_float_mapping(value) for value in frame["asset_returns_json"]]
    spy = [item.get("SPY", 0.0) for item in asset_returns]
    qqq = [item.get("QQQ", 0.0) for item in asset_returns]
    smh = [item.get("SMH", 0.0) for item in asset_returns]
    soxx = [item.get("SOXX", 0.0) for item in asset_returns]
    broad = [(left + right) / 2.0 for left, right in zip(spy, qqq, strict=True)]
    semiconductor = [
        (left + right) / 2.0 for left, right in zip(smh, soxx, strict=True)
    ]
    abs_broad = [abs(value) for value in broad]
    vol_cutoff = _median(abs_broad)
    risk_proxy = []
    for spy_return, qqq_return in zip(spy, qqq, strict=True):
        if spy_return > 0 and qqq_return > 0:
            risk_proxy.append("risk_on")
        elif spy_return < 0 and qqq_return < 0:
            risk_proxy.append("risk_off")
        else:
            risk_proxy.append("neutral")
    frame["_risk_proxy"] = risk_proxy
    frame["_high_volatility"] = [value >= vol_cutoff and value > 0 for value in abs_broad]
    frame["_semiconductor_leadership"] = [
        semi_return > broad_return
        for semi_return, broad_return in zip(semiconductor, broad, strict=True)
    ]
    frame["_growth_underperformance"] = [
        qqq_return < spy_return for qqq_return, spy_return in zip(qqq, spy, strict=True)
    ]
    return frame


def _matching_baseline_returns(
    selected: pd.DataFrame,
    baseline_daily: pd.DataFrame,
) -> list[float]:
    if selected.empty or baseline_daily.empty:
        return []
    baseline_by_signal = {
        str(row["signal_date"]): float(row["strategy_return"])
        for _, row in baseline_daily.iterrows()
    }
    returns = []
    for _, row in selected.iterrows():
        value = baseline_by_signal.get(str(row["signal_date"]))
        if value is not None:
            returns.append(value)
    return returns


def _compound_returns(returns: list[float]) -> float:
    if not returns:
        return 0.0
    return prod(1.0 + value for value in returns) - 1.0


def _robustness_run_summary(robustness_payloads: list[dict[str, Any]]) -> dict[str, Any]:
    summaries = [_mapping(payload.get("summary")) for payload in robustness_payloads]
    available = [item for item in summaries if item.get("status") == "AVAILABLE"]
    if not available:
        return {
            "status": "INSUFFICIENT_DATA",
            "candidate_count": len(summaries),
            "average_stability_score": 0.0,
            "weak_candidate_count": 0,
            "weakest_candidate_id": None,
        }
    scores = [float(item.get("stability_score") or 0.0) for item in available]
    weakest_payload = min(
        robustness_payloads,
        key=lambda payload: (
            float(_mapping(payload.get("summary")).get("stability_score") or 0.0),
            str(payload.get("candidate_id")),
        ),
    )
    return {
        "status": "AVAILABLE",
        "candidate_count": len(summaries),
        "average_stability_score": mean(scores),
        "weak_candidate_count": sum(1 for score in scores if score < 0.5),
        "weakest_candidate_id": weakest_payload.get("candidate_id"),
    }


def _benchmark_comparison(
    candidate_metrics: BacktestMetrics,
    benchmark_results: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    comparison = {}
    for benchmark_id, benchmark in benchmark_results.items():
        if benchmark.get("status") != "AVAILABLE":
            continue
        benchmark_return = _float_or_none(benchmark.get("total_return"))
        benchmark_drawdown = _float_or_none(benchmark.get("max_drawdown"))
        if benchmark_return is None or benchmark_drawdown is None:
            continue
        comparison[benchmark_id] = {
            "benchmark_name": benchmark.get("benchmark_name"),
            "excess_return": candidate_metrics.total_return - benchmark_return,
            "drawdown_reduction": abs(benchmark_drawdown) - abs(
                candidate_metrics.max_drawdown
            ),
        }
    return comparison


def _benchmark_static_weights(benchmark: Any) -> dict[str, float]:
    benchmark_type = str(benchmark.benchmark_type)
    if benchmark_type == "buy_and_hold":
        return {str(benchmark.symbol): 1.0}
    if benchmark_type == "static_portfolio":
        return {str(symbol): float(weight) for symbol, weight in benchmark.weights.items()}
    raise WeightCalibrationError(
        f"benchmark_type {benchmark_type} is not available for TRADING-071B static search"
    )


def _candidate_satisfies_sleeves(
    weights: Mapping[str, float],
    *,
    search: ETFWeightSearchDefinition,
    semiconductor_symbols: set[str],
) -> bool:
    non_cash_total = sum(float(value) for symbol, value in weights.items() if symbol != "CASH")
    if non_cash_total > search.sleeve_constraints.equity_total_max + 1e-8:
        return False
    semiconductor_total = sum(float(weights.get(symbol, 0.0)) for symbol in semiconductor_symbols)
    return semiconductor_total <= search.sleeve_constraints.semiconductor_total_max + 1e-8


def _candidate_selection_key(
    weights: Mapping[str, float],
    baseline_weights: Mapping[str, float],
) -> tuple[float, float, float, tuple[tuple[str, float], ...]]:
    baseline_distance = sum(
        abs(float(weights.get(symbol, 0.0)) - float(baseline_weights.get(symbol, 0.0)))
        for symbol in set(weights) | set(baseline_weights)
    )
    concentration = max(float(value) for value in weights.values())
    cash_distance = abs(float(weights.get("CASH", 0.0)) - float(baseline_weights.get("CASH", 0.0)))
    return (
        round(baseline_distance, 10),
        round(concentration, 10),
        round(cash_distance, 10),
        tuple(sorted((str(symbol), float(weight)) for symbol, weight in weights.items())),
    )


def _default_weights(config: ETFConfigBundle, universe: list[str]) -> dict[str, float]:
    weights = {
        symbol: float(config.assets.assets[symbol].default_weight)
        for symbol in universe
    }
    total = sum(weights.values())
    if abs(total - 1.0) > 1e-6:
        raise WeightCalibrationError("ETF default weights must sum to 1.0 for search universe")
    return weights


def _semiconductor_symbols(config: ETFConfigBundle, universe: list[str]) -> set[str]:
    return {
        symbol
        for symbol in universe
        if symbol in config.assets.assets
        and config.assets.assets[symbol].risk_group == "semiconductor"
    }


def _metrics_payload(metrics: BacktestMetrics) -> dict[str, float | None]:
    return {
        "total_return": metrics.total_return,
        "CAGR": metrics.cagr,
        "max_drawdown": metrics.max_drawdown,
        "sharpe": metrics.sharpe,
        "sortino": metrics.sortino,
        "calmar": metrics.calmar,
        "time_in_market": metrics.time_in_market,
        "turnover": metrics.turnover,
    }


def _ranking_reasons(
    component_scores: Mapping[str, float],
    hard_blockers: list[str],
) -> list[str]:
    if hard_blockers:
        return [f"HARD_BLOCKER:{blocker}" for blocker in hard_blockers]
    ordered = sorted(component_scores.items(), key=lambda item: (-float(item[1]), item[0]))
    return [f"{key}={value:.3f}" for key, value in ordered[:3]]


def _blocking_safety_issue(blockers: list[str]) -> bool:
    return any(blocker.startswith(("UNSAFE_", "BROKER_", "NO_")) for blocker in blockers)


def _positive_scaled(value: float, scale: float) -> float:
    if scale <= 0:
        return 0.0
    return max(0.0, min(1.0, float(value) / float(scale)))


def _penalty_score(value: float, scale: float) -> float:
    if scale <= 0:
        return 0.0
    return max(0.0, min(1.0, 1.0 - float(value) / float(scale)))


def _price_pivot(prices: pd.DataFrame, price_field: str) -> pd.DataFrame:
    frame = prices.copy()
    frame["_date"] = pd.to_datetime(frame["date"], errors="coerce")
    frame["_price"] = pd.to_numeric(frame[price_field], errors="coerce")
    pivot = frame.pivot(index="_date", columns="symbol", values="_price").sort_index()
    return pivot.dropna(how="all")


def _latest_price_date(prices: pd.DataFrame) -> date:
    parsed = pd.to_datetime(prices["date"], errors="coerce").dropna()
    if parsed.empty:
        raise WeightCalibrationError("ETF weight search price data has no valid dates")
    return parsed.max().date()


def _total_cost_bps(config: ETFConfigBundle) -> float:
    costs = config.risk.transaction_costs
    return costs.commission_bps + costs.slippage_bps


def _float_or_none(value: object) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if parsed != parsed:
        return None
    return parsed


def _subtract_optional(left: object, right: object) -> float | None:
    left_value = _float_or_none(left)
    right_value = _float_or_none(right)
    if left_value is None or right_value is None:
        return None
    return left_value - right_value


def _json_float_mapping(value: object) -> dict[str, float]:
    try:
        parsed = json.loads(str(value))
    except (TypeError, json.JSONDecodeError):
        return {}
    if not isinstance(parsed, Mapping):
        return {}
    result = {}
    for key, item in parsed.items():
        parsed_value = _float_or_none(item)
        if parsed_value is not None:
            result[str(key)] = parsed_value
    return result


def _median(values: list[float]) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    middle = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[middle]
    return (ordered[middle - 1] + ordered[middle]) / 2.0


def _mapping(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _records(value: object) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [dict(item) for item in value if isinstance(item, Mapping)]


def _fmt_pct(value: object) -> str:
    parsed = _float_or_none(value)
    return "n/a" if parsed is None else f"{parsed:.2%}"


def _fmt_number(value: object) -> str:
    parsed = _float_or_none(value)
    return "n/a" if parsed is None else f"{parsed:.3f}"
