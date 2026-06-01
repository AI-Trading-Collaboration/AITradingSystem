from __future__ import annotations

import json
import subprocess
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime
from hashlib import sha256
from pathlib import Path
from typing import Any, Self

import pandas as pd
from pydantic import BaseModel, Field, model_validator

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.etf_portfolio.backtest import run_portfolio_backtest
from ai_trading_system.etf_portfolio.models import (
    DEFAULT_ETF_REPORT_DIR,
    ETFConfigBundle,
    ETFQualityReport,
    PolicyMetadata,
    dataframe_checksum,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

DEFAULT_ETF_EXPERIMENTS_CONFIG_PATH = (
    PROJECT_ROOT / "config" / "etf_portfolio" / "experiments.yaml"
)
DEFAULT_ETF_EXPERIMENT_PACKS_CONFIG_PATH = (
    PROJECT_ROOT / "config" / "etf_portfolio" / "experiment_packs.yaml"
)
DEFAULT_ETF_EXPERIMENT_RUN_DIR = DEFAULT_ETF_REPORT_DIR / "experiments"
DEFAULT_ETF_SHADOW_CANDIDATE_REGISTRY_PATH = (
    PROJECT_ROOT / "data" / "simulation" / "etf_shadow_candidates.json"
)
EXPERIMENT_METRIC_SCHEMA_VERSION = "etf_experiment_metrics_v1"
SHADOW_CANDIDATE_REGISTRY_SCHEMA_VERSION = "etf_shadow_candidate_registry_v1"
EXPERIMENT_COMPARISON_SCHEMA_KEYS = (
    "schema_version",
    "report_type",
    "run_metadata",
    "experiment_list",
    "baseline_comparison",
    "benchmark_comparison",
    "metrics_table",
    "risk_metrics_table",
    "turnover_stability_table",
    "constraint_hit_summary",
    "warning_summary",
    "top_candidates_by_ranking_policy",
    "ranking_policy_status",
    "observe_only",
    "production_effect",
    "broker_action",
    "manual_review_required",
)
EXPERIMENT_CANDIDATE_SELECTION_SCHEMA_VERSION = "etf_experiment_candidate_selection_v1"
EXPERIMENT_CANDIDATE_SELECTION_SCHEMA_KEYS = (
    "schema_version",
    "report_type",
    "run_metadata",
    "ranking_policy_status",
    "promotion_policy_status",
    "promotion_policy_id",
    "selection_thresholds",
    "selection_summary",
    "candidates",
    "observe_only",
    "production_effect",
    "broker_action",
    "manual_review_required",
    "production_promotion_allowed",
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
    component_weights: dict[str, float] = Field(default_factory=dict)
    component_scales: dict[str, float] = Field(default_factory=dict)
    thresholds: dict[str, float] = Field(default_factory=dict)
    hard_rejection_rules: list[str] = Field(default_factory=list)
    blocked_hard_rejection_rules: list[str] = Field(default_factory=list)
    rejected_hard_rejection_rules: list[str] = Field(default_factory=list)
    shadow_observation_allowed: bool = False
    production_promotion_allowed: bool = False

    @model_validator(mode="after")
    def validate_policy_numbers(self) -> Self:
        if self.component_weights:
            total = sum(float(value) for value in self.component_weights.values())
            if abs(total - 1.0) > 1e-6:
                raise ValueError("ETF experiment ranking component weights must sum to 1.0")
        for key, value in self.component_scales.items():
            if float(value) <= 0.0:
                raise ValueError(f"ETF experiment component scale must be positive: {key}")
        for key, value in self.thresholds.items():
            if float(value) < 0.0:
                raise ValueError(f"ETF experiment threshold must be non-negative: {key}")
        if self.production_promotion_allowed:
            raise ValueError("ETF experiment policies must keep production_promotion_allowed=false")
        _validate_unique_policy_rules("hard_rejection_rules", self.hard_rejection_rules)
        _validate_unique_policy_rules(
            "blocked_hard_rejection_rules",
            self.blocked_hard_rejection_rules,
        )
        _validate_unique_policy_rules(
            "rejected_hard_rejection_rules",
            self.rejected_hard_rejection_rules,
        )
        overlap = set(self.blocked_hard_rejection_rules) & set(
            self.rejected_hard_rejection_rules
        )
        if overlap:
            raise ValueError(
                "ETF experiment promotion policy cannot classify the same hard "
                f"rejection rule as both blocked and rejected: {', '.join(sorted(overlap))}"
            )
        return self


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


@dataclass(frozen=True)
class ETFExperimentBatchRun:
    run_id: str
    run_dir: Path
    manifest: dict[str, Any]
    experiment_results: dict[str, Any]
    benchmark_results: dict[str, Any]
    metrics_summary: dict[str, Any]
    diagnostics_summary: dict[str, Any]


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


def build_experiment_config_bundle(
    base_config: ETFConfigBundle,
    experiment: ETFExperimentConfig,
) -> ETFConfigBundle:
    _raise_if_experiment_is_unsafe("standalone", experiment)
    config = base_config.model_copy(deep=True)
    overrides = experiment.overrides
    _apply_base_weights(config, overrides)
    _apply_regime_multipliers(config, overrides)
    _apply_semiconductor_cap(config, overrides)
    _apply_min_rebalance_delta(config, overrides)
    _apply_relative_strength_weight(config, overrides)
    payload = {
        "base_config_hash": base_config.config_hash,
        "experiment": experiment.model_dump(mode="json"),
    }
    return ETFConfigBundle(
        assets=config.assets,
        strategy=config.strategy,
        risk=config.risk,
        backtest=config.backtest,
        p1=config.p1,
        p2=config.p2,
        config_hash=_config_hash(payload),
    )


def run_experiment_batch(
    prices: pd.DataFrame,
    *,
    base_config: ETFConfigBundle,
    quality_report: ETFQualityReport,
    experiment_registry: ETFExperimentRegistry,
    pack_registry: ETFExperimentPackRegistry | None = None,
    pack_id: str | None = None,
    experiment_id: str | None = None,
    start: date,
    end: date,
    output_root: Path = DEFAULT_ETF_EXPERIMENT_RUN_DIR,
    generated_at: datetime | None = None,
) -> ETFExperimentBatchRun:
    selected_ids = _selected_experiment_ids(
        experiment_registry=experiment_registry,
        pack_registry=pack_registry,
        pack_id=pack_id,
        experiment_id=experiment_id,
    )
    generated = generated_at or datetime.now(UTC)
    run_id = _batch_run_id(pack_id=pack_id, experiment_id=experiment_id, generated_at=generated)
    run_dir = output_root / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    config_hash = _config_hash(
        {
            "experiment_registry": experiment_registry.config_hash,
            "pack_registry": None if pack_registry is None else pack_registry.config_hash,
            "selected_experiment_ids": selected_ids,
            "start": start.isoformat(),
            "end": end.isoformat(),
        }
    )
    manifest = {
        "run_id": run_id,
        "pack_id": pack_id,
        "experiment_ids": selected_ids,
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
        "created_at": generated.isoformat(),
        "git_commit": _git_commit(),
        "config_hash": config_hash,
        "data_hash": dataframe_checksum(prices.to_dict("records")),
        "data_quality_status": quality_report.status,
        "metric_schema_version": EXPERIMENT_METRIC_SCHEMA_VERSION,
        "observe_only": True,
        "production_effect": "none",
        "broker_action": "none",
        "manual_review_required": True,
    }
    experiment_results: dict[str, Any] = {
        "schema_version": 1,
        "run_id": run_id,
        "results": [],
    }
    benchmark_results: dict[str, Any] = {
        "schema_version": 1,
        "run_id": run_id,
        "benchmarks": {},
    }
    metrics_summary: dict[str, Any] = {
        "schema_version": 1,
        "run_id": run_id,
        "metric_schema_version": EXPERIMENT_METRIC_SCHEMA_VERSION,
        "metrics": [],
    }
    diagnostics_summary: dict[str, Any] = {
        "schema_version": 1,
        "run_id": run_id,
        "status": "PASS",
        "failed_experiment_ids": [],
        "observe_only": True,
        "production_effect": "none",
        "broker_action": "none",
        "manual_review_required": True,
    }
    for selected_id in selected_ids:
        experiment = experiment_registry.experiments[selected_id]
        try:
            experiment_config = build_experiment_config_bundle(base_config, experiment)
            backtest_run = run_portfolio_backtest(
                prices,
                config=experiment_config,
                quality_report=quality_report,
                start=start,
                end=end,
            )
        except Exception as exc:  # noqa: BLE001 - persisted diagnostics must capture failures.
            diagnostics_summary["status"] = "PARTIAL_FAIL"
            diagnostics_summary["failed_experiment_ids"].append(selected_id)
            experiment_results["results"].append(
                {
                    "experiment_id": selected_id,
                    "status": "FAILED",
                    "error_type": type(exc).__name__,
                    "error_message": str(exc),
                    "observe_only": True,
                    "production_effect": "none",
                    "broker_action": "none",
                    "manual_review_required": True,
                }
            )
            continue
        summary = dict(backtest_run.summary)
        experiment_results["results"].append(
            {
                "experiment_id": selected_id,
                "status": "PASS",
                "backtest_run_id": backtest_run.run_id,
                "experiment_version": experiment.experiment_version,
                "family": experiment.family,
                "config_hash": experiment_config.config_hash,
                "model_version": experiment_config.strategy.model.version,
                "first_signal_date": summary.get("first_signal_date"),
                "last_signal_date": summary.get("last_signal_date"),
                "row_count": summary.get("row_count"),
                "observe_only": True,
                "production_effect": "none",
                "broker_action": "none",
                "manual_review_required": True,
            }
        )
        benchmark_results["benchmarks"][selected_id] = summary.get("benchmark_metrics", {})
        metrics_summary["metrics"].append(
            {
                "experiment_id": selected_id,
                "strategy_metrics": summary.get("strategy_metrics", {}),
                "standardized_metrics": summary.get("standardized_metrics", {}),
                "allocation_stability_diagnostics": summary.get(
                    "allocation_stability_diagnostics",
                    {},
                ),
            }
        )
    _write_batch_run_documents(
        run_dir=run_dir,
        manifest=manifest,
        experiment_results=experiment_results,
        benchmark_results=benchmark_results,
        metrics_summary=metrics_summary,
        diagnostics_summary=diagnostics_summary,
    )
    return ETFExperimentBatchRun(
        run_id=run_id,
        run_dir=run_dir,
        manifest=manifest,
        experiment_results=experiment_results,
        benchmark_results=benchmark_results,
        metrics_summary=metrics_summary,
        diagnostics_summary=diagnostics_summary,
    )


def find_latest_experiment_run_dir(
    output_root: Path = DEFAULT_ETF_EXPERIMENT_RUN_DIR,
) -> Path:
    if not output_root.exists():
        raise FileNotFoundError(f"ETF experiment output root does not exist: {output_root}")
    candidates = [
        item
        for item in output_root.iterdir()
        if item.is_dir() and (item / "run_manifest.json").exists()
    ]
    if not candidates:
        raise FileNotFoundError(f"no ETF experiment runs found under: {output_root}")
    return max(candidates, key=lambda item: (item / "run_manifest.json").stat().st_mtime)


def build_experiment_comparison_report(run_dir: Path) -> dict[str, Any]:
    documents = _load_batch_run_documents(run_dir)
    manifest = documents["run_manifest"]
    experiment_results = documents["experiment_results"]
    benchmark_results = documents["benchmark_results"]
    metrics_summary = documents["metrics_summary"]
    diagnostics_summary = documents["diagnostics_summary"]
    metrics_by_id = {
        str(item.get("experiment_id")): item
        for item in metrics_summary.get("metrics", [])
        if isinstance(item, Mapping)
    }
    results = [
        item
        for item in experiment_results.get("results", [])
        if isinstance(item, Mapping)
    ]
    baseline = _baseline_context(metrics_summary)
    metrics_rows = [
        _comparison_metric_row(
            result,
            metrics_by_id.get(str(result.get("experiment_id")), {}),
            benchmark_results.get("benchmarks", {}),
            baseline,
        )
        for result in results
    ]
    warning_summary = _comparison_warnings(
        metrics_rows,
        diagnostics_summary=diagnostics_summary,
        baseline=baseline,
    )
    payload = {
        "schema_version": 1,
        "report_type": "etf_experiment_comparison",
        "run_metadata": manifest,
        "experiment_list": [
            {
                "experiment_id": result.get("experiment_id"),
                "status": result.get("status"),
                "family": result.get("family"),
                "experiment_version": result.get("experiment_version"),
            }
            for result in results
        ],
        "baseline_comparison": baseline,
        "benchmark_comparison": _benchmark_context(benchmark_results),
        "metrics_table": metrics_rows,
        "risk_metrics_table": _select_table(
            metrics_rows,
            [
                "experiment_id",
                "max_drawdown",
                "Sharpe",
                "Sortino",
                "Calmar",
                "drawdown_reduction_vs_baseline",
                "drawdown_reduction_vs_QQQ",
                "candidate_status",
            ],
        ),
        "turnover_stability_table": _select_table(
            metrics_rows,
            [
                "experiment_id",
                "turnover",
                "average_equity_exposure",
                "average_cash_weight",
                "constraint_hit_rate",
                "regime_transition_count",
                "candidate_status",
            ],
        ),
        "constraint_hit_summary": _constraint_hit_summary(metrics_rows),
        "warning_summary": warning_summary,
        "top_candidates_by_ranking_policy": [],
        "ranking_policy_status": "PENDING_TRADING_064E_RISK_ADJUSTED_V1",
        "observe_only": True,
        "production_effect": "none",
        "broker_action": "none",
        "manual_review_required": True,
    }
    return {key: payload[key] for key in EXPERIMENT_COMPARISON_SCHEMA_KEYS}


def write_experiment_comparison_report(
    payload: Mapping[str, Any],
    *,
    json_path: Path,
    markdown_path: Path,
) -> tuple[Path, Path]:
    json_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    markdown_path.write_text(render_experiment_comparison_report(payload), encoding="utf-8")
    return json_path, markdown_path


def write_candidate_selection_report(
    payload: Mapping[str, Any],
    *,
    json_path: Path,
    markdown_path: Path,
) -> tuple[Path, Path]:
    json_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    markdown_path.write_text(render_candidate_selection_report(payload), encoding="utf-8")
    return json_path, markdown_path


def load_shadow_candidate_registry(
    path: Path = DEFAULT_ETF_SHADOW_CANDIDATE_REGISTRY_PATH,
) -> dict[str, Any]:
    if not path.exists():
        return _empty_shadow_candidate_registry()
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"ETF shadow candidate registry must be a JSON object: {path}")
    candidates = payload.get("candidates")
    if not isinstance(candidates, list):
        raise ValueError(f"ETF shadow candidate registry missing candidates list: {path}")
    return dict(payload)


def enroll_shadow_candidates(
    selection_report: Mapping[str, Any],
    *,
    registry_path: Path = DEFAULT_ETF_SHADOW_CANDIDATE_REGISTRY_PATH,
    candidate_ids: list[str] | None = None,
    top: int | None = None,
    enrolled_at: datetime | None = None,
) -> dict[str, Any]:
    selected = _selected_shadow_candidates(selection_report, candidate_ids=candidate_ids)
    if top is not None:
        if top <= 0:
            raise ValueError("ETF shadow enrollment --top must be positive")
        selected = selected[:top]
    if not selected:
        raise ValueError("no eligible ETF experiment candidates selected for shadow enrollment")
    timestamp = (enrolled_at or datetime.now(tz=UTC)).isoformat()
    registry = load_shadow_candidate_registry(registry_path)
    existing = {
        str(candidate.get("shadow_id")): dict(candidate)
        for candidate in registry.get("candidates", [])
        if isinstance(candidate, Mapping) and candidate.get("shadow_id")
    }
    changed = False
    for candidate in selected:
        record = _shadow_candidate_record(
            selection_report=selection_report,
            candidate=candidate,
            enrolled_at=timestamp,
        )
        shadow_id = str(record["shadow_id"])
        if shadow_id in existing:
            continue
        existing[shadow_id] = record
        changed = True
    candidates = sorted(existing.values(), key=lambda item: str(item["shadow_id"]))
    payload = {
        "schema_version": SHADOW_CANDIDATE_REGISTRY_SCHEMA_VERSION,
        "registry_type": "etf_shadow_candidates",
        "updated_at": timestamp if changed else registry.get("updated_at"),
        "candidate_count": len(candidates),
        "candidates": candidates,
        "observe_only": True,
        "production_effect": "none",
        "broker_action": "none",
        "manual_review_required": True,
        "production_promotion_allowed": False,
    }
    registry_path.parent.mkdir(parents=True, exist_ok=True)
    registry_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return payload


def apply_ranking_policy_to_comparison_report(
    payload: Mapping[str, Any],
    *,
    ranking_policy: ETFExperimentPolicyRef,
    ranking_policy_id: str,
) -> dict[str, Any]:
    updated = dict(payload)
    ranked = rank_experiment_candidates(payload, ranking_policy=ranking_policy)
    updated["top_candidates_by_ranking_policy"] = ranked
    updated["ranking_policy_status"] = f"APPLIED:{ranking_policy_id}"
    return {key: updated[key] for key in EXPERIMENT_COMPARISON_SCHEMA_KEYS}


def build_candidate_selection_report(
    comparison_report: Mapping[str, Any],
    *,
    promotion_policy: ETFExperimentPolicyRef,
    promotion_policy_id: str,
) -> dict[str, Any]:
    min_candidate_score = _required_policy_threshold(
        promotion_policy,
        "min_candidate_score",
    )
    ranked_candidates = [
        item
        for item in comparison_report.get("top_candidates_by_ranking_policy", [])
        if isinstance(item, Mapping)
    ]
    candidates = [
        _candidate_selection_row(
            candidate,
            rank=rank,
            promotion_policy=promotion_policy,
            min_candidate_score=min_candidate_score,
        )
        for rank, candidate in enumerate(ranked_candidates, start=1)
    ]
    summary = _candidate_selection_summary(candidates)
    payload = {
        "schema_version": EXPERIMENT_CANDIDATE_SELECTION_SCHEMA_VERSION,
        "report_type": "etf_experiment_candidate_selection",
        "run_metadata": dict(_mapping(comparison_report.get("run_metadata"))),
        "ranking_policy_status": comparison_report.get("ranking_policy_status"),
        "promotion_policy_status": promotion_policy.policy_status,
        "promotion_policy_id": promotion_policy_id,
        "selection_thresholds": {
            "min_candidate_score": min_candidate_score,
            "blocked_hard_rejection_rules": list(
                promotion_policy.blocked_hard_rejection_rules
            ),
            "rejected_hard_rejection_rules": list(
                promotion_policy.rejected_hard_rejection_rules
            ),
        },
        "selection_summary": summary,
        "candidates": candidates,
        "observe_only": True,
        "production_effect": "none",
        "broker_action": "none",
        "manual_review_required": True,
        "production_promotion_allowed": False,
    }
    return {key: payload[key] for key in EXPERIMENT_CANDIDATE_SELECTION_SCHEMA_KEYS}


def rank_experiment_candidates(
    comparison_report: Mapping[str, Any],
    *,
    ranking_policy: ETFExperimentPolicyRef,
) -> list[dict[str, Any]]:
    weights = ranking_policy.component_weights
    scales = ranking_policy.component_scales
    thresholds = ranking_policy.thresholds
    metadata = _mapping(comparison_report.get("run_metadata"))
    source_run_id = str(metadata.get("run_id") or "")
    ranked: list[dict[str, Any]] = []
    for row in comparison_report.get("metrics_table", []):
        if not isinstance(row, Mapping):
            continue
        experiment_id = row.get("experiment_id")
        component_scores = {
            "benchmark_excess_return_score": _positive_score(
                row.get("excess_return_vs_QQQ"),
                scales["excess_return_reference"],
            ),
            "drawdown_reduction_score": _positive_score(
                row.get("drawdown_reduction_vs_QQQ"),
                scales["drawdown_reduction_reference"],
            ),
            "risk_adjusted_return_score": _risk_adjusted_score(
                row,
                scales["risk_adjusted_return_reference"],
            ),
            "turnover_penalty_score": _inverse_score(
                row.get("turnover"),
                scales["turnover_reference"],
            ),
            "stability_score": _inverse_score(
                row.get("constraint_hit_rate"),
                scales["constraint_hit_rate_reference"],
            ),
        }
        hard_rejection_flags = _hard_rejection_flags(row, thresholds)
        candidate_score = sum(
            component_scores[key] * float(weights[key]) for key in component_scores
        )
        if hard_rejection_flags:
            candidate_score = 0.0
        ranked.append(
            {
                "candidate_id": _shadow_candidate_id(source_run_id, experiment_id),
                "experiment_id": experiment_id,
                "source_run_id": source_run_id,
                "model_version": row.get("model_version"),
                "config_hash": row.get("config_hash"),
                "start_date": row.get("first_signal_date") or metadata.get("start_date"),
                "candidate_score": round(candidate_score, 10),
                **component_scores,
                "hard_rejection_flags": hard_rejection_flags,
                "candidate_status": "rejected" if hard_rejection_flags else "ranked",
                "ranking_reason": _ranking_reason(component_scores, hard_rejection_flags),
                "observe_only": True,
                "production_effect": "none",
                "broker_action": "none",
                "manual_review_required": True,
            }
        )
    return sorted(
        ranked,
        key=lambda item: (-float(item["candidate_score"]), str(item["experiment_id"])),
    )


def render_experiment_comparison_report(payload: Mapping[str, Any]) -> str:
    metadata = _mapping(payload.get("run_metadata"))
    rows = [row for row in payload.get("metrics_table", []) if isinstance(row, Mapping)]
    warnings = payload.get("warning_summary")
    warning_lines = ["- none"]
    if isinstance(warnings, list) and warnings:
        warning_lines = [f"- {item}" for item in warnings]
    lines = [
        "# ETF Experiment Comparison Report",
        "",
        f"- Run ID: {metadata.get('run_id')}",
        f"- Pack ID: {metadata.get('pack_id')}",
        f"- Date Range: {metadata.get('start_date')} to {metadata.get('end_date')}",
        f"- Data Quality: {metadata.get('data_quality_status')}",
        f"- Metric Schema: {metadata.get('metric_schema_version')}",
        "- Safety: observe_only=true, production_effect=none, broker_action=none",
        f"- Ranking Policy Status: {payload.get('ranking_policy_status')}",
        "",
        "## Baseline Context",
        "",
        f"- Status: {_mapping(payload.get('baseline_comparison')).get('status')}",
        "",
        "## Metrics Table",
        "",
        (
            "| Experiment | Status | Total Return | CAGR | Max Drawdown | Sharpe | "
            "Turnover | Excess vs QQQ | Constraint Hit Rate |"
        ),
        "|---|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for row in rows:
        lines.append(
            "| "
            f"{row.get('experiment_id')} | "
            f"{row.get('candidate_status')} | "
            f"{_fmt_pct(row.get('total_return'))} | "
            f"{_fmt_pct(row.get('CAGR'))} | "
            f"{_fmt_pct(row.get('max_drawdown'))} | "
            f"{_fmt_number(row.get('Sharpe'))} | "
            f"{_fmt_number(row.get('turnover'))} | "
            f"{_fmt_pct(row.get('excess_return_vs_QQQ'))} | "
            f"{_fmt_pct(row.get('constraint_hit_rate'))} |"
        )
    lines.extend(
        [
            "",
            "## Warnings",
            "",
            *warning_lines,
            "",
            "## Ranking",
            "",
            *_ranking_lines(payload),
        ]
    )
    return "\n".join(lines) + "\n"


def render_candidate_selection_report(payload: Mapping[str, Any]) -> str:
    metadata = _mapping(payload.get("run_metadata"))
    summary = _mapping(payload.get("selection_summary"))
    rows = [row for row in payload.get("candidates", []) if isinstance(row, Mapping)]
    lines = [
        "# ETF Experiment Candidate Selection Gate",
        "",
        f"- Run ID: {metadata.get('run_id')}",
        f"- Pack ID: {metadata.get('pack_id')}",
        f"- Date Range: {metadata.get('start_date')} to {metadata.get('end_date')}",
        f"- Ranking Policy Status: {payload.get('ranking_policy_status')}",
        f"- Promotion Policy: {payload.get('promotion_policy_id')}",
        f"- Promotion Policy Status: {payload.get('promotion_policy_status')}",
        f"- Gate Status: {summary.get('status')}",
        "- Safety: observe_only=true, production_effect=none, broker_action=none",
        "- Production Promotion Allowed: false",
        "",
        "## Candidate Gate",
        "",
        (
            "| Rank | Experiment | Score | Selection Status | Shadow Allowed | "
            "Production Promotion | Blockers |"
        ),
        "|---:|---|---:|---|---|---|---|",
    ]
    for row in rows:
        blockers = row.get("blockers")
        blocker_text = (
            ", ".join(str(blocker) for blocker in blockers)
            if isinstance(blockers, list) and blockers
            else "none"
        )
        lines.append(
            "| "
            f"{row.get('rank')} | "
            f"{row.get('experiment_id')} | "
            f"{_fmt_number(row.get('candidate_score'))} | "
            f"{row.get('selection_status')} | "
            f"{str(row.get('shadow_observation_allowed')).lower()} | "
            f"{str(row.get('production_promotion_allowed')).lower()} | "
            f"{blocker_text} |"
        )
    if not rows:
        lines.extend(
            [
                "| - | no ranked candidates | - | blocked | false | false | "
                "RANKING_POLICY_NOT_APPLIED |"
            ]
        )
    lines.extend(
        [
            "",
            "## Summary",
            "",
            f"- Eligible for shadow: {summary.get('eligible_for_shadow_count')}",
            f"- Needs more data: {summary.get('needs_more_data_count')}",
            f"- Rejected: {summary.get('rejected_count')}",
            f"- Blocked: {summary.get('blocked_count')}",
        ]
    )
    return "\n".join(lines) + "\n"


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


def _selected_experiment_ids(
    *,
    experiment_registry: ETFExperimentRegistry,
    pack_registry: ETFExperimentPackRegistry | None,
    pack_id: str | None,
    experiment_id: str | None,
) -> list[str]:
    if pack_id and experiment_id:
        raise ValueError("select either pack_id or experiment_id, not both")
    if experiment_id:
        experiment = experiment_registry.experiments.get(experiment_id)
        if experiment is None:
            raise ValueError(f"unknown ETF experiment_id: {experiment_id}")
        _raise_if_experiment_is_unsafe("standalone", experiment)
        return [experiment_id]
    if not pack_id:
        raise ValueError("pack_id or experiment_id is required")
    if pack_registry is None:
        raise ValueError("pack_registry is required when selecting a pack")
    validate_experiment_pack_registry(
        pack_registry,
        experiment_registry=experiment_registry,
    )
    pack = pack_registry.experiment_packs.get(pack_id)
    if pack is None:
        raise ValueError(f"unknown ETF experiment pack: {pack_id}")
    return list(pack.experiment_ids)


def _write_batch_run_documents(
    *,
    run_dir: Path,
    manifest: Mapping[str, Any],
    experiment_results: Mapping[str, Any],
    benchmark_results: Mapping[str, Any],
    metrics_summary: Mapping[str, Any],
    diagnostics_summary: Mapping[str, Any],
) -> None:
    documents = {
        "run_manifest.json": manifest,
        "experiment_results.json": experiment_results,
        "benchmark_results.json": benchmark_results,
        "metrics_summary.json": metrics_summary,
        "diagnostics_summary.json": diagnostics_summary,
    }
    for name, payload in documents.items():
        (run_dir / name).write_text(
            json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )


def _batch_run_id(
    *,
    pack_id: str | None,
    experiment_id: str | None,
    generated_at: datetime,
) -> str:
    selected = pack_id or experiment_id or "unknown"
    timestamp = generated_at.strftime("%Y%m%dT%H%M%SZ")
    return f"etf-experiments-{selected}-{timestamp}"


def _load_batch_run_documents(run_dir: Path) -> dict[str, Any]:
    names = {
        "run_manifest": "run_manifest.json",
        "experiment_results": "experiment_results.json",
        "benchmark_results": "benchmark_results.json",
        "metrics_summary": "metrics_summary.json",
        "diagnostics_summary": "diagnostics_summary.json",
    }
    documents: dict[str, Any] = {}
    for key, name in names.items():
        path = run_dir / name
        if not path.exists():
            raise FileNotFoundError(f"ETF experiment run missing {name}: {run_dir}")
        payload = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            raise ValueError(f"ETF experiment run document must be an object: {path}")
        documents[key] = payload
    return documents


def _baseline_context(metrics_summary: Mapping[str, Any]) -> dict[str, Any]:
    baseline = metrics_summary.get("baseline_metrics")
    if isinstance(baseline, Mapping):
        return {
            "status": "AVAILABLE",
            "metrics": dict(baseline),
            "null_reason": None,
        }
    return {
        "status": "MISSING_BASELINE_METRICS",
        "metrics": {},
        "null_reason": "Batch run output does not include baseline metrics yet.",
    }


def _comparison_metric_row(
    result: Mapping[str, Any],
    metrics_item: Mapping[str, Any],
    benchmarks_by_experiment: Mapping[str, Any],
    baseline: Mapping[str, Any],
) -> dict[str, Any]:
    experiment_id = str(result.get("experiment_id"))
    standardized = _mapping(metrics_item.get("standardized_metrics"))
    stability = _mapping(metrics_item.get("allocation_stability_diagnostics"))
    benchmark_metrics = _mapping(benchmarks_by_experiment.get(experiment_id))
    qqq_metrics = _mapping(benchmark_metrics.get("B002"))
    null_reasons: dict[str, str] = {}
    total_return = _metric(standardized, "total_return", null_reasons, experiment_id)
    cagr = _metric(standardized, "CAGR", null_reasons, experiment_id)
    max_drawdown = _metric(standardized, "max_drawdown", null_reasons, experiment_id)
    qqq_total_return = _optional_float(qqq_metrics.get("total_return"))
    qqq_max_drawdown = _optional_float(qqq_metrics.get("max_drawdown"))
    baseline_metrics = _mapping(baseline.get("metrics"))
    baseline_total_return = _optional_float(baseline_metrics.get("total_return"))
    baseline_max_drawdown = _optional_float(baseline_metrics.get("max_drawdown"))
    if baseline.get("status") != "AVAILABLE":
        null_reasons["baseline_comparison"] = str(baseline.get("null_reason"))
    row = {
        "experiment_id": experiment_id,
        "total_return": total_return,
        "CAGR": cagr,
        "max_drawdown": max_drawdown,
        "Sharpe": _metric(standardized, "Sharpe", null_reasons, experiment_id),
        "Sortino": _metric(standardized, "Sortino", null_reasons, experiment_id),
        "Calmar": _metric(standardized, "Calmar", null_reasons, experiment_id),
        "turnover": _metric(standardized, "turnover", null_reasons, experiment_id),
        "average_equity_exposure": _metric(
            standardized,
            "average_equity_exposure",
            null_reasons,
            experiment_id,
        ),
        "average_cash_weight": _metric(
            standardized,
            "average_cash_weight",
            null_reasons,
            experiment_id,
        ),
        "excess_return_vs_baseline": _subtract_or_none(total_return, baseline_total_return),
        "drawdown_reduction_vs_baseline": _drawdown_reduction_or_none(
            max_drawdown,
            baseline_max_drawdown,
        ),
        "excess_return_vs_QQQ": _subtract_or_none(total_return, qqq_total_return),
        "drawdown_reduction_vs_QQQ": _drawdown_reduction_or_none(
            max_drawdown,
            qqq_max_drawdown,
        ),
        "constraint_hit_rate": _optional_float(stability.get("constraint_hit_rate")),
        "regime_transition_count": stability.get("regime_transition_count"),
        "candidate_status": _candidate_status(result),
        "metric_null_reasons": null_reasons,
        "model_version": result.get("model_version"),
        "config_hash": result.get("config_hash"),
        "first_signal_date": result.get("first_signal_date"),
        "last_signal_date": result.get("last_signal_date"),
    }
    if row["excess_return_vs_QQQ"] is None:
        null_reasons["excess_return_vs_QQQ"] = "QQQ benchmark metrics missing"
    if row["drawdown_reduction_vs_QQQ"] is None:
        null_reasons["drawdown_reduction_vs_QQQ"] = "QQQ benchmark drawdown missing"
    return row


def _benchmark_context(benchmark_results: Mapping[str, Any]) -> dict[str, Any]:
    benchmarks = benchmark_results.get("benchmarks")
    if not isinstance(benchmarks, Mapping) or not benchmarks:
        return {"status": "MISSING_BENCHMARKS", "benchmark_ids": []}
    ids: set[str] = set()
    for value in benchmarks.values():
        if isinstance(value, Mapping):
            ids.update(str(key) for key in value)
    return {"status": "AVAILABLE", "benchmark_ids": sorted(ids)}


def _select_table(rows: list[dict[str, Any]], keys: list[str]) -> list[dict[str, Any]]:
    return [{key: row.get(key) for key in keys} for row in rows]


def _constraint_hit_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    rates = [
        value
        for value in (_optional_float(row.get("constraint_hit_rate")) for row in rows)
        if value is not None
    ]
    return {
        "experiment_count": len(rows),
        "max_constraint_hit_rate": max(rates) if rates else None,
        "experiments_with_constraint_hits": [
            row["experiment_id"]
            for row in rows
            if (_optional_float(row.get("constraint_hit_rate")) or 0.0) > 0.0
        ],
    }


def _comparison_warnings(
    rows: list[dict[str, Any]],
    *,
    diagnostics_summary: Mapping[str, Any],
    baseline: Mapping[str, Any],
) -> list[str]:
    warnings: list[str] = []
    if baseline.get("status") != "AVAILABLE":
        warnings.append("BASELINE_METRICS_MISSING")
    if diagnostics_summary.get("status") != "PASS":
        warnings.append(f"RUN_STATUS_{diagnostics_summary.get('status')}")
    for row in rows:
        if row.get("candidate_status") == "failed":
            warnings.append(f"FAILED_EXPERIMENT:{row.get('experiment_id')}")
        if row.get("metric_null_reasons"):
            warnings.append(f"MISSING_METRICS:{row.get('experiment_id')}")
    return sorted(set(warnings))


def _candidate_status(result: Mapping[str, Any]) -> str:
    if result.get("status") == "FAILED":
        return "failed"
    return "needs_ranking_policy"


def _hard_rejection_flags(
    row: Mapping[str, Any],
    thresholds: Mapping[str, float],
) -> list[str]:
    flags: list[str] = []
    if row.get("candidate_status") == "failed":
        flags.append("CREDIBILITY_GATE_FAILED")
    if row.get("excess_return_vs_QQQ") is None or row.get(
        "excess_return_vs_baseline"
    ) is None:
        flags.append("NO_BENCHMARK_COMPARISON")
    turnover = _optional_float(row.get("turnover"))
    max_turnover = float(thresholds["max_turnover"])
    if turnover is None or turnover > max_turnover:
        flags.append("TURNOVER_TOO_HIGH")
    drawdown_reduction_vs_baseline = _optional_float(
        row.get("drawdown_reduction_vs_baseline")
    )
    max_worsening = float(thresholds["max_drawdown_worsening_vs_baseline"])
    if (
        drawdown_reduction_vs_baseline is None
        or drawdown_reduction_vs_baseline < -max_worsening
    ):
        flags.append("DRAWDOWN_TOO_HIGH")
    constraint_hit_rate = _optional_float(row.get("constraint_hit_rate")) or 0.0
    if constraint_hit_rate > 1.0:
        flags.append("CONSTRAINT_VIOLATION")
    if row.get("production_effect") not in (None, "none"):
        flags.append("UNSAFE_PRODUCTION_EFFECT")
    if row.get("manual_review_required") is False:
        flags.append("MISSING_MANUAL_REVIEW")
    return sorted(set(flags))


def _positive_score(value: Any, reference: float) -> float:
    parsed = _optional_float(value)
    if parsed is None:
        return 0.0
    return max(0.0, min(1.0, parsed / reference))


def _inverse_score(value: Any, reference: float) -> float:
    parsed = _optional_float(value)
    if parsed is None:
        return 0.0
    return max(0.0, min(1.0, 1.0 - parsed / reference))


def _risk_adjusted_score(row: Mapping[str, Any], reference: float) -> float:
    values = [
        value
        for value in (
            _optional_float(row.get("Sharpe")),
            _optional_float(row.get("Sortino")),
            _optional_float(row.get("Calmar")),
        )
        if value is not None
    ]
    if not values:
        return 0.0
    return max(0.0, min(1.0, (sum(values) / len(values)) / reference))


def _ranking_reason(
    component_scores: Mapping[str, float],
    hard_rejection_flags: list[str],
) -> list[str]:
    if hard_rejection_flags:
        return [f"hard_rejected:{flag}" for flag in hard_rejection_flags]
    strongest = max(component_scores, key=lambda key: component_scores[key])
    weakest = min(component_scores, key=lambda key: component_scores[key])
    return [f"strongest_component:{strongest}", f"weakest_component:{weakest}"]


def _ranking_lines(payload: Mapping[str, Any]) -> list[str]:
    ranked = [
        item
        for item in payload.get("top_candidates_by_ranking_policy", [])
        if isinstance(item, Mapping)
    ]
    if not ranked:
        return [
            (
                "- Ranking is intentionally pending TRADING-064E. This report does not "
                "rank candidates by return only."
            )
        ]
    lines = [
        "| Rank | Experiment | Candidate Score | Status | Hard Rejections |",
        "|---:|---|---:|---|---|",
    ]
    for index, item in enumerate(ranked, start=1):
        flags = item.get("hard_rejection_flags")
        flag_text = ", ".join(str(flag) for flag in flags) if isinstance(flags, list) else ""
        lines.append(
            "| "
            f"{index} | "
            f"{item.get('experiment_id')} | "
            f"{_fmt_number(item.get('candidate_score'))} | "
            f"{item.get('candidate_status')} | "
            f"{flag_text or 'none'} |"
        )
    return lines


def _candidate_selection_row(
    candidate: Mapping[str, Any],
    *,
    rank: int,
    promotion_policy: ETFExperimentPolicyRef,
    min_candidate_score: float,
) -> dict[str, Any]:
    score = _optional_float(candidate.get("candidate_score")) or 0.0
    hard_flags = _string_list(candidate.get("hard_rejection_flags"))
    blocked_rules = set(promotion_policy.blocked_hard_rejection_rules)
    rejected_rules = set(promotion_policy.rejected_hard_rejection_rules)
    blockers = sorted(flag for flag in hard_flags if flag in blocked_rules)
    rejected_flags = sorted(flag for flag in hard_flags if flag in rejected_rules)
    unclassified_flags = sorted(
        flag for flag in hard_flags if flag not in blocked_rules and flag not in rejected_rules
    )
    blockers.extend(f"UNCLASSIFIED_HARD_REJECTION:{flag}" for flag in unclassified_flags)
    if candidate.get("observe_only") is False:
        blockers.append("OBSERVE_ONLY_DISABLED")
    if candidate.get("production_effect") not in (None, "none"):
        blockers.append("UNSAFE_PRODUCTION_EFFECT")
    if candidate.get("broker_action") not in (None, "none"):
        blockers.append("BROKER_ACTION_NOT_NONE")
    if candidate.get("manual_review_required") is False:
        blockers.append("MISSING_MANUAL_REVIEW")
    if candidate.get("candidate_status") == "rejected" and not hard_flags:
        blockers.append("RANKING_REJECTED_WITHOUT_FLAGS")
    if blockers:
        selection_status = "blocked"
        shadow_allowed = False
        reasons = [f"blocked:{blocker}" for blocker in sorted(set(blockers))]
    elif rejected_flags:
        selection_status = "rejected"
        shadow_allowed = False
        reasons = [f"hard_rejected:{flag}" for flag in rejected_flags]
    elif score < min_candidate_score:
        selection_status = "needs_more_data"
        shadow_allowed = False
        reasons = [f"candidate_score_below_min:{score:.4f}<{min_candidate_score:.4f}"]
    elif not promotion_policy.shadow_observation_allowed:
        selection_status = "blocked"
        shadow_allowed = False
        reasons = ["blocked:SHADOW_OBSERVATION_POLICY_DISABLED"]
        blockers.append("SHADOW_OBSERVATION_POLICY_DISABLED")
    else:
        selection_status = "eligible_for_shadow"
        shadow_allowed = True
        reasons = [
            "candidate_score_meets_min",
            "manual_review_required_before_shadow",
        ]
    return {
        "rank": rank,
        "candidate_id": candidate.get("candidate_id"),
        "experiment_id": candidate.get("experiment_id"),
        "source_run_id": candidate.get("source_run_id"),
        "model_version": candidate.get("model_version"),
        "config_hash": candidate.get("config_hash"),
        "start_date": candidate.get("start_date"),
        "candidate_score": round(score, 10),
        "ranking_candidate_status": candidate.get("candidate_status"),
        "hard_rejection_flags": hard_flags,
        "selection_status": selection_status,
        "selection_reasons": reasons,
        "blockers": sorted(set(blockers)),
        "shadow_observation_allowed": shadow_allowed,
        "production_promotion_allowed": False,
        "observe_only": True,
        "production_effect": "none",
        "broker_action": "none",
        "manual_review_required": True,
    }


def _candidate_selection_summary(candidates: list[dict[str, Any]]) -> dict[str, Any]:
    counts = {
        "eligible_for_shadow_count": 0,
        "needs_more_data_count": 0,
        "rejected_count": 0,
        "blocked_count": 0,
    }
    for candidate in candidates:
        status = candidate.get("selection_status")
        if status == "eligible_for_shadow":
            counts["eligible_for_shadow_count"] += 1
        elif status == "needs_more_data":
            counts["needs_more_data_count"] += 1
        elif status == "rejected":
            counts["rejected_count"] += 1
        elif status == "blocked":
            counts["blocked_count"] += 1
    if not candidates:
        status = "BLOCKED_NO_RANKED_CANDIDATES"
        blockers = ["RANKING_POLICY_NOT_APPLIED"]
    elif counts["eligible_for_shadow_count"] > 0:
        status = "PASS"
        blockers = []
    elif counts["blocked_count"] > 0:
        status = "BLOCKED"
        blockers = sorted(
            {
                str(blocker)
                for candidate in candidates
                for blocker in candidate.get("blockers", [])
            }
        )
    else:
        status = "NO_ELIGIBLE_CANDIDATE"
        blockers = []
    return {
        "status": status,
        "candidate_count": len(candidates),
        **counts,
        "blockers": blockers,
        "production_promotion_allowed": False,
    }


def _selected_shadow_candidates(
    selection_report: Mapping[str, Any],
    *,
    candidate_ids: list[str] | None,
) -> list[Mapping[str, Any]]:
    requested = {str(candidate_id) for candidate_id in candidate_ids or []}
    unmatched = set(requested)
    selected: list[Mapping[str, Any]] = []
    for candidate in selection_report.get("candidates", []):
        if not isinstance(candidate, Mapping):
            continue
        candidate_id = str(candidate.get("candidate_id") or "")
        experiment_id = str(candidate.get("experiment_id") or "")
        if requested and candidate_id not in requested and experiment_id not in requested:
            continue
        unmatched.discard(candidate_id)
        unmatched.discard(experiment_id)
        if candidate.get("selection_status") != "eligible_for_shadow":
            if requested:
                raise ValueError(
                    "ETF shadow enrollment candidate is not eligible_for_shadow: "
                    f"{candidate_id or experiment_id}"
                )
            continue
        _raise_if_shadow_candidate_unsafe(candidate)
        selected.append(candidate)
    if unmatched:
        raise ValueError(
            "ETF shadow enrollment requested unknown candidate(s): "
            f"{', '.join(sorted(unmatched))}"
        )
    return selected


def _raise_if_shadow_candidate_unsafe(candidate: Mapping[str, Any]) -> None:
    candidate_id = candidate.get("candidate_id") or candidate.get("experiment_id")
    if candidate.get("shadow_observation_allowed") is not True:
        raise ValueError(f"ETF shadow candidate is not shadow eligible: {candidate_id}")
    if candidate.get("observe_only") is not True:
        raise ValueError(f"ETF shadow candidate must keep observe_only=true: {candidate_id}")
    if candidate.get("production_effect") != "none":
        raise ValueError(
            f"ETF shadow candidate must keep production_effect=none: {candidate_id}"
        )
    if candidate.get("broker_action") != "none":
        raise ValueError(f"ETF shadow candidate must keep broker_action=none: {candidate_id}")
    if candidate.get("manual_review_required") is not True:
        raise ValueError(
            f"ETF shadow candidate must keep manual_review_required=true: {candidate_id}"
        )
    if candidate.get("production_promotion_allowed") is not False:
        raise ValueError(
            "ETF shadow candidate must keep production_promotion_allowed=false: "
            f"{candidate_id}"
        )


def _shadow_candidate_record(
    *,
    selection_report: Mapping[str, Any],
    candidate: Mapping[str, Any],
    enrolled_at: str,
) -> dict[str, Any]:
    metadata = _mapping(selection_report.get("run_metadata"))
    experiment_id = _required_candidate_text(candidate, "experiment_id")
    source_run_id = str(candidate.get("source_run_id") or metadata.get("run_id") or "")
    if not source_run_id:
        raise ValueError(f"ETF shadow candidate missing source_run_id: {experiment_id}")
    candidate_id = str(
        candidate.get("candidate_id") or _shadow_candidate_id(source_run_id, experiment_id)
    )
    start_date = str(candidate.get("start_date") or metadata.get("start_date") or "")
    if not start_date:
        raise ValueError(f"ETF shadow candidate missing start_date: {candidate_id}")
    return {
        "shadow_id": _shadow_id(candidate_id),
        "candidate_id": candidate_id,
        "experiment_id": experiment_id,
        "source_run_id": source_run_id,
        "enrolled_at": enrolled_at,
        "model_version": _required_candidate_text(candidate, "model_version"),
        "config_hash": _required_candidate_text(candidate, "config_hash"),
        "start_date": start_date,
        "status": "active_shadow_observation",
        "candidate_score": candidate.get("candidate_score"),
        "selection_status": candidate.get("selection_status"),
        "observe_only": True,
        "production_effect": "none",
        "broker_action": "none",
        "manual_review_required": True,
        "production_promotion_allowed": False,
        "evaluation_schedule": {
            "cadence": "daily",
            "start_date": start_date,
            "weekly_review_task": "TRADING-064H",
        },
    }


def _empty_shadow_candidate_registry() -> dict[str, Any]:
    return {
        "schema_version": SHADOW_CANDIDATE_REGISTRY_SCHEMA_VERSION,
        "registry_type": "etf_shadow_candidates",
        "updated_at": None,
        "candidate_count": 0,
        "candidates": [],
        "observe_only": True,
        "production_effect": "none",
        "broker_action": "none",
        "manual_review_required": True,
        "production_promotion_allowed": False,
    }


def _shadow_candidate_id(source_run_id: Any, experiment_id: Any) -> str:
    run = str(source_run_id or "unknown_run")
    experiment = str(experiment_id or "unknown_experiment")
    return f"{run}:{experiment}"


def _shadow_id(candidate_id: str) -> str:
    return f"etf_shadow_{sha256(candidate_id.encode('utf-8')).hexdigest()[:16]}"


def _required_candidate_text(candidate: Mapping[str, Any], key: str) -> str:
    value = candidate.get(key)
    if value is None or str(value) == "":
        raise ValueError(
            f"ETF shadow candidate missing required field {key}: "
            f"{candidate.get('candidate_id') or candidate.get('experiment_id')}"
        )
    return str(value)


def _required_policy_threshold(
    policy: ETFExperimentPolicyRef,
    threshold_name: str,
) -> float:
    if threshold_name not in policy.thresholds:
        raise ValueError(
            f"ETF experiment promotion policy missing required threshold: {threshold_name}"
        )
    return float(policy.thresholds[threshold_name])


def _metric(
    payload: Mapping[str, Any],
    key: str,
    null_reasons: dict[str, str],
    experiment_id: str,
) -> float | None:
    value = _optional_float(payload.get(key))
    if value is None:
        null_reasons[key] = f"{experiment_id} missing metric {key}"
    return value


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _string_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(item) for item in value]


def _subtract_or_none(left: float | None, right: float | None) -> float | None:
    if left is None or right is None:
        return None
    return left - right


def _drawdown_reduction_or_none(
    candidate_drawdown: float | None,
    reference_drawdown: float | None,
) -> float | None:
    if candidate_drawdown is None or reference_drawdown is None:
        return None
    return abs(reference_drawdown) - abs(candidate_drawdown)


def _optional_float(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if pd.isna(parsed):
        return None
    return parsed


def _fmt_pct(value: Any) -> str:
    parsed = _optional_float(value)
    if parsed is None:
        return "n/a"
    return f"{parsed:.2%}"


def _fmt_number(value: Any) -> str:
    parsed = _optional_float(value)
    if parsed is None:
        return "n/a"
    return f"{parsed:.2f}"


def _git_commit() -> str:
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=PROJECT_ROOT,
            check=True,
            capture_output=True,
            text=True,
            timeout=5,
        )
    except (OSError, subprocess.SubprocessError):
        return "unknown"
    return result.stdout.strip() or "unknown"


def _apply_base_weights(config: ETFConfigBundle, overrides: Mapping[str, Any]) -> None:
    value = overrides.get("base_weights")
    if value is None:
        return
    if not isinstance(value, Mapping):
        raise ValueError("base_weights override must be a mapping")
    for symbol, weight in value.items():
        normalized_symbol = str(symbol).strip().upper()
        if normalized_symbol not in config.assets.assets:
            raise ValueError(f"unknown ETF base weight symbol: {normalized_symbol}")
        config.assets.assets[normalized_symbol].default_weight = float(weight)


def _apply_regime_multipliers(config: ETFConfigBundle, overrides: Mapping[str, Any]) -> None:
    value = overrides.get("regime_multipliers")
    if value is None:
        return
    if not isinstance(value, Mapping):
        raise ValueError("regime_multipliers override must be a mapping")
    mapping = {
        "risk_on": "Risk-On",
        "neutral": "Neutral",
        "risk_off": "Risk-Off",
    }
    for key, regime_name in mapping.items():
        multiplier = float(value[key])
        constraints = config.risk.regime_constraints[regime_name]
        constraints.equity_cap = multiplier
        constraints.cash_min = 1.0 - multiplier


def _apply_semiconductor_cap(config: ETFConfigBundle, overrides: Mapping[str, Any]) -> None:
    if "semiconductor_sleeve_max_weight" not in overrides:
        return
    cap = float(overrides["semiconductor_sleeve_max_weight"])
    if "semiconductor" in config.assets.risk_groups:
        config.assets.risk_groups["semiconductor"].max_weight = cap
    for constraints in config.risk.regime_constraints.values():
        constraints.semiconductor_cap = min(constraints.semiconductor_cap, cap)


def _apply_min_rebalance_delta(config: ETFConfigBundle, overrides: Mapping[str, Any]) -> None:
    if "min_rebalance_delta" in overrides:
        config.strategy.model.min_rebalance_delta = float(overrides["min_rebalance_delta"])


def _apply_relative_strength_weight(config: ETFConfigBundle, overrides: Mapping[str, Any]) -> None:
    if "relative_strength_weight" not in overrides:
        return
    target = float(overrides["relative_strength_weight"])
    existing = {
        key: float(value.weight)
        for key, value in config.strategy.scores.items()
        if key != "relative_strength"
    }
    existing_total = sum(existing.values())
    if existing_total <= 0:
        raise ValueError("non-relative strength score weights must be positive")
    remaining = 1.0 - target
    for key, weight in existing.items():
        config.strategy.scores[key].weight = remaining * weight / existing_total
    config.strategy.scores["relative_strength"].weight = target


def _validate_unique_policy_rules(field_name: str, rules: list[str]) -> None:
    normalized = [str(rule).strip() for rule in rules]
    if any(not rule for rule in normalized):
        raise ValueError(f"ETF experiment policy {field_name} must not contain blank rules")
    if len(normalized) != len(set(normalized)):
        raise ValueError(f"ETF experiment policy {field_name} must not contain duplicates")


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
