from __future__ import annotations

import json
import os
from datetime import UTC, date, datetime, timedelta
from hashlib import sha256
from pathlib import Path
from typing import Any, Literal, Self

from pydantic import BaseModel, Field, model_validator

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.data_foundation import PRIMARY_RESEARCH_START_DATE
from ai_trading_system.etf_portfolio.dynamic_allocation import (
    DEFAULT_DYNAMIC_ALLOCATION_POLICY_CONFIG_PATH,
    DynamicAllocationPolicyConfig,
    build_dynamic_allocation_decision_record,
    load_dynamic_allocation_policy_config,
)
from ai_trading_system.etf_portfolio.models import PolicyMetadata
from ai_trading_system.etf_portfolio.trend_calibration import (
    DEFAULT_TREND_CALIBRATION_POLICY_CONFIG_PATH,
    DEFAULT_TREND_CALIBRATION_REPORT_DIR,
    latest_trend_calibration_report_path,
    load_trend_calibration_policy_config,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

DYNAMIC_CALIBRATION_POLICY_SCHEMA_VERSION = "etf_dynamic_calibration_policy_v1"
DYNAMIC_CANDIDATE_PACK_SCHEMA_VERSION = "etf_dynamic_candidate_pack_v1"
DYNAMIC_CALIBRATION_REPORT_SCHEMA_VERSION = "etf_dynamic_calibration_report_v1"
DYNAMIC_CALIBRATION_VALIDATION_SCHEMA_VERSION = "etf_dynamic_calibration_validation_v1"
DYNAMIC_CALIBRATION_RUN_MANIFEST_SCHEMA_VERSION = "etf_dynamic_calibration_run_manifest_v1"

DEFAULT_DYNAMIC_CALIBRATION_POLICY_CONFIG_PATH = (
    PROJECT_ROOT / "config" / "etf_portfolio" / "dynamic_calibration.yaml"
)
DEFAULT_DYNAMIC_CALIBRATION_ROOT = (
    PROJECT_ROOT / "reports" / "etf_portfolio" / "dynamic_calibration"
)
DEFAULT_DYNAMIC_CALIBRATION_CANDIDATE_DIR = DEFAULT_DYNAMIC_CALIBRATION_ROOT / "candidates"
DEFAULT_DYNAMIC_CALIBRATION_REPORT_DIR = DEFAULT_DYNAMIC_CALIBRATION_ROOT / "reports"
DEFAULT_DYNAMIC_CALIBRATION_VALIDATION_DIR = DEFAULT_DYNAMIC_CALIBRATION_ROOT / "validation"
DEFAULT_DYNAMIC_CALIBRATION_CACHE_ROOT = PROJECT_ROOT / "data" / "cache" / "dynamic_calibration"

DYNAMIC_CALIBRATION_SAFETY: dict[str, Any] = {
    "observe_only": True,
    "candidate_only": True,
    "production_effect": "none",
    "broker_action": "none",
    "manual_review_required": True,
    "production_state_mutated": False,
    "baseline_config_mutated": False,
    "official_target_weights_mutated": False,
    "automatic_candidate_promotion": False,
    "auto_enrollment_without_owner_approval": False,
}
FORBIDDEN_DYNAMIC_CALIBRATION_KEYS = {
    "broker_order",
    "production_weight_update",
    "baseline_config_mutation",
    "official_target_weights_write",
    "auto_enrollment_without_owner_approval_action",
    "automatic_candidate_promotion_action",
}
ENGINE_VERSION = "dynamic_calibration_batch_engine_v0_1"
WEIGHT_SYMBOLS = ("SPY", "QQQ", "SMH", "SOXX", "CASH")


class DynamicCalibrationError(RuntimeError):
    """Raised when TRADING-085 dynamic calibration inputs or outputs are unsafe."""


class DynamicCalibrationMarketRegime(BaseModel):
    regime_id: Literal["unified_primary_2021"]
    anchor_event: str = Field(min_length=1)
    anchor_date: date
    default_evaluation_start: date

    @model_validator(mode="after")
    def validate_ai_regime_start(self) -> Self:
        if self.default_evaluation_start < PRIMARY_RESEARCH_START_DATE:
            raise ValueError("dynamic calibration default start cannot predate 2021-02-22")
        return self


class DynamicCalibrationSafety(BaseModel):
    observe_only: Literal[True]
    candidate_only: Literal[True]
    production_effect: Literal["none"]
    broker_action: Literal["none"]
    manual_review_required: Literal[True]
    production_state_mutated: Literal[False]
    baseline_config_mutated: Literal[False]
    official_target_weights_mutated: Literal[False]
    automatic_candidate_promotion: Literal[False]
    auto_enrollment_without_owner_approval: Literal[False]

    @model_validator(mode="after")
    def validate_safety(self) -> Self:
        if self.model_dump(mode="json") != DYNAMIC_CALIBRATION_SAFETY:
            raise ValueError("dynamic calibration safety fields are unsafe")
        return self


class DynamicCalibrationBatchConfig(BaseModel):
    default_pack_id: str = Field(min_length=1)
    data_range_preset: str = Field(min_length=1)
    max_trend_configs: int = Field(gt=0)
    allocation_score_profiles: list[str] = Field(min_length=1)
    local_refinement_profiles: list[str] = Field(default_factory=list)
    top_n: int = Field(gt=0)
    transaction_cost_bps: float = Field(ge=0)


class DynamicCalibrationStageConfig(BaseModel):
    stage_id: str = Field(min_length=1)
    description: str = Field(min_length=1)
    output: str = Field(min_length=1)


class DynamicCalibrationSearchProtocolConfig(BaseModel):
    stages: list[DynamicCalibrationStageConfig] = Field(min_length=1)


class DynamicCalibrationIterationConfig(BaseModel):
    coarse_profile_step: str = Field(min_length=1)
    top_n_for_refinement: int = Field(gt=0)
    local_refinement_enabled: bool
    local_refinement_limit: int = Field(ge=0)
    max_total_candidate_packs: int = Field(gt=0)


class DynamicCalibrationCacheConfig(BaseModel):
    cache_root: str = Field(min_length=1)
    default_mode: Literal["read-write", "read-only", "disabled"]
    enabled_layers: dict[str, bool] = Field(min_length=1)
    schema_versions: dict[str, str] = Field(min_length=1)

    @model_validator(mode="after")
    def validate_layers(self) -> Self:
        required = {
            "trend_score",
            "allocation_path",
            "dynamic_backtest",
            "candidate_pack_aggregation",
            "cache_manifest",
        }
        missing = required - set(self.schema_versions)
        if missing:
            raise ValueError(
                "dynamic calibration cache missing schema versions: " + ", ".join(sorted(missing))
            )
        return self


class DynamicCalibrationPerformanceConfig(BaseModel):
    default_workers: str = Field(min_length=1)
    max_workers: int = Field(gt=0)
    parallel_execution: bool
    resume_supported: bool
    partial_candidate_recompute_supported: bool
    profiling_report_required_before_optimization: bool


class DynamicCalibrationRankingPolicy(BaseModel):
    min_score_for_robustness_review: float = Field(ge=0, le=100)
    risk_adjusted_return_weight: float = Field(ge=0, le=1)
    drawdown_reduction_weight: float = Field(ge=0, le=1)
    turnover_control_weight: float = Field(ge=0, le=1)
    regime_robustness_weight: float = Field(ge=0, le=1)
    false_signal_penalty_weight: float = Field(ge=0, le=1)
    data_quality_weight: float = Field(ge=0, le=1)
    overfit_risk_weight: float = Field(ge=0, le=1)

    @model_validator(mode="after")
    def validate_weight_sum(self) -> Self:
        total = (
            self.risk_adjusted_return_weight
            + self.drawdown_reduction_weight
            + self.turnover_control_weight
            + self.regime_robustness_weight
            + self.false_signal_penalty_weight
            + self.data_quality_weight
            + self.overfit_risk_weight
        )
        if abs(total - 1.0) > 1e-6:
            raise ValueError("dynamic calibration ranking weights must sum to 1.0")
        return self


class DynamicCalibrationProxyConfig(BaseModel):
    full_robustness_backtest_required: Literal[True]
    return_lift_full_score: float = Field(gt=0)
    drawdown_improvement_full_score: float = Field(gt=0)
    turnover_full_penalty: float = Field(gt=0)
    risk_adjusted_return_scale: float = Field(gt=0)
    semiconductor_return_scale: float = Field(ge=0)
    growth_return_scale: float = Field(ge=0)
    cash_drag_scale: float = Field(ge=0)
    turnover_cost_scale: float = Field(ge=0)
    max_drawdown_base: float = Field(gt=0)
    cash_drawdown_relief_scale: float = Field(ge=0)
    semiconductor_drawdown_penalty_scale: float = Field(ge=0)
    false_signal_penalty_scale: float = Field(ge=0)
    overfit_redundancy_penalty: float = Field(ge=0, le=1)
    benchmark_proxy_returns: dict[str, float] = Field(min_length=1)


class DynamicCalibrationPolicyConfig(BaseModel):
    schema_version: Literal["etf_dynamic_calibration_policy_v1"]
    policy_metadata: PolicyMetadata
    market_regime: DynamicCalibrationMarketRegime
    safety: DynamicCalibrationSafety
    batch: DynamicCalibrationBatchConfig
    two_stage_search_protocol: DynamicCalibrationSearchProtocolConfig
    coarse_to_fine_iteration: DynamicCalibrationIterationConfig
    cache: DynamicCalibrationCacheConfig
    performance: DynamicCalibrationPerformanceConfig
    ranking_policy: DynamicCalibrationRankingPolicy
    calibration_proxy: DynamicCalibrationProxyConfig

    @model_validator(mode="after")
    def validate_policy(self) -> Self:
        if self.coarse_to_fine_iteration.max_total_candidate_packs < self.batch.top_n:
            raise ValueError("max_total_candidate_packs cannot be below batch top_n")
        return self


def load_dynamic_calibration_policy_config(
    path: Path = DEFAULT_DYNAMIC_CALIBRATION_POLICY_CONFIG_PATH,
) -> DynamicCalibrationPolicyConfig:
    raw = safe_load_yaml_path(path)
    if not isinstance(raw, dict):
        raise DynamicCalibrationError("dynamic calibration policy must be a mapping")
    try:
        return DynamicCalibrationPolicyConfig.model_validate(raw)
    except Exception as exc:  # noqa: BLE001
        raise DynamicCalibrationError(f"invalid dynamic calibration policy: {exc}") from exc


def build_dynamic_calibration_batch_report(
    *,
    policy: DynamicCalibrationPolicyConfig,
    dynamic_policy: DynamicAllocationPolicyConfig,
    trend_report: dict[str, Any] | None = None,
    trend_report_path: Path | None = None,
    trend_policy_path: Path = DEFAULT_TREND_CALIBRATION_POLICY_CONFIG_PATH,
    dynamic_policy_path: Path = DEFAULT_DYNAMIC_ALLOCATION_POLICY_CONFIG_PATH,
    pack_id: str | None = None,
    cache_mode: Literal["read-write", "read-only", "disabled"] | None = None,
    cache_root: Path | None = None,
    workers: str | int | None = None,
    top_n: int | None = None,
) -> dict[str, Any]:
    generated = datetime.now(UTC)
    resolved_pack_id = pack_id or policy.batch.default_pack_id
    resolved_cache_mode = cache_mode or policy.cache.default_mode
    resolved_cache_root = cache_root or _resolve_cache_root(policy)
    worker_count = resolve_dynamic_calibration_worker_count(workers, policy=policy)
    source = _resolve_trend_config_candidates(
        trend_report=trend_report,
        policy=policy,
        trend_policy_path=trend_policy_path,
    )
    trend_configs = source["trend_configs"]
    profile_ids = _candidate_profile_ids(policy)
    candidates = _candidate_pack_specs(
        pack_id=resolved_pack_id,
        trend_configs=trend_configs,
        profile_ids=profile_ids,
        policy=policy,
        dynamic_policy=dynamic_policy,
        trend_report=trend_report or {},
        trend_report_path=trend_report_path,
        dynamic_policy_path=dynamic_policy_path,
        trend_policy_path=trend_policy_path,
    )
    if not candidates:
        raise DynamicCalibrationError("dynamic calibration candidate pack list is empty")
    cache_events: list[dict[str, Any]] = []
    candidate_packs: list[dict[str, Any]] = []
    for spec in candidates[: policy.coarse_to_fine_iteration.max_total_candidate_packs]:
        pack = _build_candidate_pack(
            spec=spec,
            policy=policy,
            dynamic_policy=dynamic_policy,
            cache_mode=resolved_cache_mode,
            cache_root=resolved_cache_root,
            cache_events=cache_events,
        )
        candidate_packs.append(pack)
    ranked = sorted(
        candidate_packs,
        key=lambda item: float(_mapping(item.get("ranking")).get("ranking_score", 0.0)),
        reverse=True,
    )
    resolved_top_n = top_n or policy.batch.top_n
    top_packs = ranked[:resolved_top_n]
    run_manifest = _build_run_manifest(
        policy=policy,
        generated=generated,
        pack_id=resolved_pack_id,
        trend_report_path=trend_report_path,
        trend_source_status=str(source["source_status"]),
        cache_mode=resolved_cache_mode,
        cache_root=resolved_cache_root,
        worker_count=worker_count,
        candidate_count=len(candidate_packs),
    )
    report_id = _stable_id(
        "dynamic-calibration-report",
        resolved_pack_id,
        policy.policy_metadata.version,
        dynamic_policy.policy_metadata.version,
        _stable_hash([pack["dynamic_candidate_pack_id"] for pack in top_packs]),
    )
    payload = {
        "schema_version": DYNAMIC_CALIBRATION_REPORT_SCHEMA_VERSION,
        "report_type": "etf_dynamic_calibration_report",
        "dynamic_calibration_report_id": report_id,
        "generated_at": generated.isoformat(),
        "status": "PASS",
        "market_regime": policy.market_regime.model_dump(mode="json"),
        "requested_pack_id": resolved_pack_id,
        "data_range_preset": policy.batch.data_range_preset,
        "source_trend_report": str(trend_report_path or ""),
        "source_trend_report_status": _trend_report_status(trend_report),
        "source_trend_config_status": source["source_status"],
        "source_trend_config_limitation": source["limitation"],
        "dynamic_allocation_policy_id": dynamic_policy.default_policy_id,
        "dynamic_allocation_policy_hash": _stable_hash(dynamic_policy.model_dump(mode="json")),
        "candidate_pack_count": len(candidate_packs),
        "top_pack_count": len(top_packs),
        "candidate_packs": candidate_packs,
        "top_candidate_packs": top_packs,
        "ranking_policy": policy.ranking_policy.model_dump(mode="json"),
        "two_stage_search_protocol": [
            stage.model_dump(mode="json") for stage in policy.two_stage_search_protocol.stages
        ],
        "coarse_to_fine_iteration": policy.coarse_to_fine_iteration.model_dump(mode="json"),
        "cache_summary": _cache_summary(cache_events, resolved_cache_mode),
        "cache_events": cache_events,
        "run_manifest": run_manifest,
        "summary": _report_summary(top_packs, source, trend_report),
        "calibration_scope": {
            "calibration_proxy": True,
            "full_robustness_backtest_required": True,
            "handoff_task": "TRADING-086",
        },
        "safety": policy.safety.model_dump(mode="json"),
        **DYNAMIC_CALIBRATION_SAFETY,
        "commands_executed": False,
    }
    _assert_dynamic_calibration_payload_safe(payload)
    return payload


def build_dynamic_calibration_validation_report(
    *,
    policy_config_path: Path = DEFAULT_DYNAMIC_CALIBRATION_POLICY_CONFIG_PATH,
    dynamic_policy_path: Path = DEFAULT_DYNAMIC_ALLOCATION_POLICY_CONFIG_PATH,
    trend_policy_path: Path = DEFAULT_TREND_CALIBRATION_POLICY_CONFIG_PATH,
) -> dict[str, Any]:
    generated = datetime.now(UTC)
    checks: list[dict[str, Any]] = []
    policy: DynamicCalibrationPolicyConfig | None = None
    try:
        policy = load_dynamic_calibration_policy_config(policy_config_path)
        _append_check(checks, "policy_config_valid", True, "dynamic calibration policy loads")
    except Exception as exc:  # noqa: BLE001
        _append_check(checks, "policy_config_valid", False, str(exc))
    dynamic_policy: DynamicAllocationPolicyConfig | None = None
    try:
        dynamic_policy = load_dynamic_allocation_policy_config(dynamic_policy_path)
        _append_check(
            checks, "dynamic_allocation_policy_valid", True, "dynamic allocation policy loads"
        )
    except Exception as exc:  # noqa: BLE001
        _append_check(checks, "dynamic_allocation_policy_valid", False, str(exc))
    sample_report: dict[str, Any] | None = None
    if policy is not None and dynamic_policy is not None:
        try:
            sample_report = build_dynamic_calibration_batch_report(
                policy=policy,
                dynamic_policy=dynamic_policy,
                trend_report=None,
                trend_report_path=None,
                trend_policy_path=trend_policy_path,
                dynamic_policy_path=dynamic_policy_path,
                cache_mode="disabled",
                workers=1,
                top_n=min(policy.batch.top_n, 3),
            )
            _append_check(
                checks,
                "batch_runner_available",
                sample_report["candidate_pack_count"] > 0,
                f"built {sample_report['candidate_pack_count']} two-layer candidate packs",
            )
            top = _records(sample_report.get("top_candidate_packs"))
            _append_check(
                checks,
                "ranking_available",
                bool(top) and "ranking_components" in _mapping(top[0].get("ranking")),
                "candidate packs include evidence-based ranking components",
            )
            _append_check(
                checks,
                "cache_layers_available",
                {"trend_score", "allocation_path", "dynamic_backtest"}.issubset(
                    set(_mapping(sample_report.get("cache_summary")).get("layers", []))
                ),
                "trend score, allocation path, and dynamic backtest cache layers are represented",
            )
        except Exception as exc:  # noqa: BLE001
            _append_check(checks, "batch_runner_available", False, str(exc))
    registry_text = (PROJECT_ROOT / "config" / "report_registry.yaml").read_text(encoding="utf-8")
    _append_check(
        checks,
        "report_registry_visibility",
        "etf_dynamic_calibration_report" in registry_text
        and "etf_dynamic_calibration_validation" in registry_text,
        "report registry includes dynamic calibration report and validation",
    )
    registration_text = (
        PROJECT_ROOT
        / "src"
        / "ai_trading_system"
        / "interfaces"
        / "cli"
        / "etf_portfolio"
        / "registration.py"
    ).read_text(encoding="utf-8")
    command_owner_text = (
        PROJECT_ROOT
        / "src"
        / "ai_trading_system"
        / "interfaces"
        / "cli"
        / "etf_portfolio"
        / "dynamic_calibration.py"
    ).read_text(encoding="utf-8")
    _append_check(
        checks,
        "cli_visibility",
        "dynamic-calibration" in registration_text
        and "dynamic_calibration_app" in command_owner_text,
        "CLI exposes dynamic-calibration namespace",
    )
    reader_brief_text = (
        PROJECT_ROOT / "src" / "ai_trading_system" / "reports" / "reader_brief.py"
    ).read_text(encoding="utf-8")
    _append_check(
        checks,
        "reader_brief_visibility",
        "Dynamic Calibration Batch" in reader_brief_text
        and "_etf_dynamic_calibration_summary" in reader_brief_text,
        "Reader Brief includes dynamic calibration batch section",
    )
    if sample_report is not None:
        try:
            _assert_dynamic_calibration_payload_safe(sample_report)
            safety_ok = True
            safety_detail = "dynamic calibration report safety boundary preserved"
        except Exception as exc:  # noqa: BLE001
            safety_ok = False
            safety_detail = str(exc)
        _append_check(checks, "safety_boundary", safety_ok, safety_detail)
    failed = [check for check in checks if not check["passed"]]
    payload = {
        "schema_version": DYNAMIC_CALIBRATION_VALIDATION_SCHEMA_VERSION,
        "report_type": "etf_dynamic_calibration_validation",
        "validation_id": _stable_id("dynamic-calibration-validation", generated.date().isoformat()),
        "generated_at": generated.isoformat(),
        "status": "PASS" if not failed else "FAIL",
        "checks": checks,
        "failed_check_count": len(failed),
        "source_schema_versions": {
            "policy": DYNAMIC_CALIBRATION_POLICY_SCHEMA_VERSION,
            "candidate_pack": DYNAMIC_CANDIDATE_PACK_SCHEMA_VERSION,
            "report": DYNAMIC_CALIBRATION_REPORT_SCHEMA_VERSION,
            "run_manifest": DYNAMIC_CALIBRATION_RUN_MANIFEST_SCHEMA_VERSION,
        },
        "safety": DYNAMIC_CALIBRATION_SAFETY,
        **DYNAMIC_CALIBRATION_SAFETY,
        "commands_executed": False,
        "production_weight_update_blocked": True,
        "broker_order_blocked": True,
        "automatic_candidate_promotion_blocked": True,
        "official_target_weights_write_blocked": True,
        "auto_enrollment_without_owner_approval_blocked": True,
    }
    _assert_dynamic_calibration_payload_safe(payload)
    return payload


def write_dynamic_calibration_candidate_packs(
    payload: dict[str, Any],
    *,
    output_dir: Path = DEFAULT_DYNAMIC_CALIBRATION_CANDIDATE_DIR,
) -> dict[str, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    pack_id = str(payload["requested_pack_id"])
    json_path = output_dir / f"dynamic-candidate-packs_{pack_id}_{_artifact_suffix(payload)}.json"
    markdown_path = output_dir / f"dynamic-candidate-packs_{pack_id}_{_artifact_suffix(payload)}.md"
    pack_payload = {
        "schema_version": DYNAMIC_CANDIDATE_PACK_SCHEMA_VERSION,
        "report_type": "etf_dynamic_candidate_pack_collection",
        "source_report_id": payload["dynamic_calibration_report_id"],
        "candidate_pack_count": payload["candidate_pack_count"],
        "candidate_packs": payload["candidate_packs"],
        "top_candidate_packs": payload["top_candidate_packs"],
        "safety": payload["safety"],
        **DYNAMIC_CALIBRATION_SAFETY,
        "commands_executed": False,
    }
    _assert_dynamic_calibration_payload_safe(pack_payload)
    _write_json(pack_payload, json_path)
    _write_text(render_dynamic_candidate_packs_markdown(pack_payload), markdown_path)
    return {"json": json_path, "markdown": markdown_path}


def write_dynamic_calibration_report(
    payload: dict[str, Any],
    *,
    output_dir: Path = DEFAULT_DYNAMIC_CALIBRATION_REPORT_DIR,
) -> dict[str, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    report_id = str(payload["dynamic_calibration_report_id"])
    json_path = output_dir / f"{report_id}.json"
    markdown_path = output_dir / f"{report_id}.md"
    _write_json(payload, json_path)
    _write_text(render_dynamic_calibration_report_markdown(payload), markdown_path)
    return {"json": json_path, "markdown": markdown_path}


def write_dynamic_calibration_validation_report(
    payload: dict[str, Any],
    *,
    output_dir: Path = DEFAULT_DYNAMIC_CALIBRATION_VALIDATION_DIR,
) -> dict[str, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    validation_id = str(payload["validation_id"])
    json_path = output_dir / f"{validation_id}.json"
    markdown_path = output_dir / f"{validation_id}.md"
    _write_json(payload, json_path)
    _write_text(render_dynamic_calibration_validation_markdown(payload), markdown_path)
    return {"json": json_path, "markdown": markdown_path}


def latest_dynamic_calibration_report_path(
    report_dir: Path = DEFAULT_DYNAMIC_CALIBRATION_REPORT_DIR,
) -> Path | None:
    return _latest_file(report_dir, "dynamic-calibration-report_*.json")


def resolve_dynamic_calibration_worker_count(
    workers: str | int | None,
    *,
    policy: DynamicCalibrationPolicyConfig,
) -> int:
    requested = str(workers or policy.performance.default_workers).strip().lower()
    if requested == "auto":
        return max(1, min(policy.performance.max_workers, os.cpu_count() or 1))
    try:
        value = int(requested)
    except ValueError as exc:
        raise DynamicCalibrationError(f"invalid worker count: {workers}") from exc
    if value < 1:
        raise DynamicCalibrationError("worker count must be positive")
    return min(value, policy.performance.max_workers)


def render_dynamic_calibration_report_markdown(payload: dict[str, Any]) -> str:
    summary = _mapping(payload.get("summary"))
    lines = [
        f"# Dynamic Calibration Report {payload.get('dynamic_calibration_report_id')}",
        "",
        "## Safety",
        "- observe_only=true",
        "- candidate_only=true",
        "- production_effect=none",
        "- broker_action=none",
        "- manual_review_required=true",
        "- official_target_weights_mutated=false",
        "- automatic_candidate_promotion=false",
        "- auto_enrollment_without_owner_approval=false",
        "",
        "## Summary",
        f"- Status: {payload.get('status')}",
        f"- Pack: {payload.get('requested_pack_id')}",
        f"- Data range preset: {payload.get('data_range_preset')}",
        f"- Candidate packs: {payload.get('candidate_pack_count')}",
        f"- Top pack: {summary.get('top_dynamic_candidate_pack_id')}",
        f"- Top score: {summary.get('top_ranking_score')}",
        f"- Source trend config status: {payload.get('source_trend_config_status')}",
        f"- Cache hit rate: {_mapping(payload.get('cache_summary')).get('cache_hit_rate')}",
        f"- Full robustness backtest required: {summary.get('full_robustness_backtest_required')}",
        "",
        "## Top Candidate Packs",
    ]
    for pack in _records(payload.get("top_candidate_packs")):
        ranking = _mapping(pack.get("ranking"))
        backtest = _mapping(pack.get("dynamic_backtest_summary"))
        pack_id = pack.get("dynamic_candidate_pack_id")
        ranking_score = ranking.get("ranking_score")
        status = ranking.get("status")
        return_proxy = backtest.get("portfolio_return_proxy")
        drawdown_proxy = backtest.get("max_drawdown_proxy")
        turnover = backtest.get("turnover_estimate")
        lines.extend(
            [
                f"- `{pack_id}` score={ranking_score} status={status} "
                f"return_proxy={return_proxy} drawdown_proxy={drawdown_proxy} "
                f"turnover={turnover}",
            ]
        )
    lines.extend(
        [
            "",
            "## Cache Summary",
            _markdown_mapping(payload.get("cache_summary")),
            "",
            "## Limitations",
            "- TRADING-085 dynamic_backtest cache stores calibration proxy metrics only.",
            "- Full dynamic strategy robustness, walk-forward, and false-signal "
            "diagnostics are TRADING-086 scope.",
        ]
    )
    return "\n".join(lines) + "\n"


def render_dynamic_candidate_packs_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# Dynamic Candidate Packs",
        "",
        "## Safety",
        "- observe_only=true",
        "- candidate_only=true",
        "- production_effect=none",
        "- broker_action=none",
        "- manual_review_required=true",
        "",
        "## Packs",
    ]
    for pack in _records(payload.get("candidate_packs")):
        ranking = _mapping(pack.get("ranking"))
        pack_id = pack.get("dynamic_candidate_pack_id")
        trend_id = pack.get("trend_signal_config_id")
        profile_id = pack.get("allocation_profile_id")
        lines.append(
            f"- `{pack_id}` score={ranking.get('ranking_score')} "
            f"trend={trend_id} profile={profile_id}"
        )
    return "\n".join(lines) + "\n"


def render_dynamic_calibration_validation_markdown(payload: dict[str, Any]) -> str:
    lines = [
        f"# Dynamic Calibration Validation {payload.get('validation_id')}",
        "",
        f"- Status: {payload.get('status')}",
        f"- Failed checks: {payload.get('failed_check_count')}",
        "- observe_only=true",
        "- candidate_only=true",
        "- production_effect=none",
        "- broker_action=none",
        "",
        "## Checks",
    ]
    for check in _records(payload.get("checks")):
        status = "PASS" if check.get("passed") else "FAIL"
        lines.append(f"- {check.get('check_id')}: {status} - {check.get('detail')}")
    return "\n".join(lines) + "\n"


def _build_candidate_pack(
    *,
    spec: dict[str, Any],
    policy: DynamicCalibrationPolicyConfig,
    dynamic_policy: DynamicAllocationPolicyConfig,
    cache_mode: Literal["read-write", "read-only", "disabled"],
    cache_root: Path,
    cache_events: list[dict[str, Any]],
) -> dict[str, Any]:
    cache_keys = _mapping(spec["cache_keys"])
    trend_scores = _cached_layer(
        layer="trend_score",
        cache_key=str(cache_keys["trend_score"]),
        policy=policy,
        cache_mode=cache_mode,
        cache_root=cache_root,
        cache_events=cache_events,
        builder=lambda: _build_trend_score_cache_payload(spec),
    )
    allocation_path = _cached_layer(
        layer="allocation_path",
        cache_key=str(cache_keys["allocation_path"]),
        policy=policy,
        cache_mode=cache_mode,
        cache_root=cache_root,
        cache_events=cache_events,
        builder=lambda: _build_allocation_path_cache_payload(
            spec=spec,
            policy=policy,
            dynamic_policy=dynamic_policy,
            trend_scores=trend_scores,
        ),
    )
    backtest = _cached_layer(
        layer="dynamic_backtest",
        cache_key=str(cache_keys["dynamic_backtest"]),
        policy=policy,
        cache_mode=cache_mode,
        cache_root=cache_root,
        cache_events=cache_events,
        builder=lambda: _build_dynamic_backtest_cache_payload(
            spec=spec,
            policy=policy,
            allocation_path=allocation_path,
        ),
    )
    ranking = _rank_candidate_pack(
        policy=policy,
        spec=spec,
        trend_scores=trend_scores,
        allocation_path=allocation_path,
        backtest=backtest,
    )
    payload = {
        "schema_version": DYNAMIC_CANDIDATE_PACK_SCHEMA_VERSION,
        "report_type": "etf_dynamic_candidate_pack",
        "dynamic_candidate_pack_id": spec["dynamic_candidate_pack_id"],
        "trend_signal_config_id": spec["trend_signal_config_id"],
        "dynamic_allocation_policy_id": spec["dynamic_allocation_policy_id"],
        "allocation_profile_id": spec["allocation_profile_id"],
        "iteration_stage": spec["iteration_stage"],
        "data_range_preset": policy.batch.data_range_preset,
        "backtest_config_hash": spec["backtest_config_hash"],
        "cache_keys": cache_keys,
        "trend_score_cache": trend_scores,
        "allocation_path_cache": allocation_path,
        "dynamic_backtest_summary": backtest,
        "ranking": ranking,
        "ranking_reasons": ranking["ranking_reasons"],
        "blockers": ranking["blockers"],
        "safety": policy.safety.model_dump(mode="json"),
        **DYNAMIC_CALIBRATION_SAFETY,
        "commands_executed": False,
        "calibration_proxy": True,
        "full_robustness_backtest_required": True,
    }
    _assert_dynamic_calibration_payload_safe(payload)
    return payload


def _candidate_pack_specs(
    *,
    pack_id: str,
    trend_configs: list[dict[str, Any]],
    profile_ids: list[str],
    policy: DynamicCalibrationPolicyConfig,
    dynamic_policy: DynamicAllocationPolicyConfig,
    trend_report: dict[str, Any],
    trend_report_path: Path | None,
    dynamic_policy_path: Path,
    trend_policy_path: Path,
) -> list[dict[str, Any]]:
    specs: list[dict[str, Any]] = []
    data_hash = _stable_hash(
        {
            "trend_report_path": str(trend_report_path or ""),
            "dataset_coverage": trend_report.get("dataset_coverage"),
            "policy_config_hash": trend_report.get("policy_config_hash"),
        }
    )
    feature_matrix_hash = _stable_hash(trend_report.get("dataset_coverage", {}))
    dynamic_policy_hash = _stable_hash(dynamic_policy.model_dump(mode="json"))
    trend_policy_hash = _file_hash(trend_policy_path)
    dynamic_policy_file_hash = _file_hash(dynamic_policy_path)
    profile_index = 0
    for trend_config in trend_configs:
        trend_id = _text(trend_config.get("trend_signal_config_id"), "unknown_trend_config")
        trend_hash = _stable_hash(trend_config)
        for profile_id in profile_ids:
            if profile_id not in dynamic_policy.sample_score_profiles:
                continue
            iteration_stage = (
                "local_refinement"
                if profile_id in policy.batch.local_refinement_profiles
                else "coarse"
            )
            base = {
                "pack_id": pack_id,
                "trend_signal_config_id": trend_id,
                "allocation_profile_id": profile_id,
                "data_range_preset": policy.batch.data_range_preset,
                "trend_hash": trend_hash,
                "dynamic_policy_hash": dynamic_policy_hash,
                "trend_policy_hash": trend_policy_hash,
                "dynamic_policy_file_hash": dynamic_policy_file_hash,
                "engine_version": ENGINE_VERSION,
            }
            pack_stem = _stable_id("dynamic-candidate-pack", pack_id, trend_id, profile_id)
            key_context = {
                "data_hash": data_hash,
                "price_matrix_hash": data_hash,
                "feature_matrix_hash": feature_matrix_hash,
                "signal_config_hash": trend_hash,
                "trend_score_config_hash": trend_policy_hash,
                "allocation_policy_hash": dynamic_policy_hash,
                "rebalance_policy_hash": _stable_hash(
                    dynamic_policy.rebalance_policy.model_dump(mode="json")
                ),
                "constraint_policy_hash": _stable_hash(
                    dynamic_policy.exposure_constraints.model_dump(mode="json")
                ),
                "regime_definition_hash": _stable_hash(dynamic_policy.regime_weight_targets),
                "backtest_engine_version": ENGINE_VERSION,
                "model_version": policy.policy_metadata.version,
                "transaction_cost_config_hash": _stable_hash(policy.batch.transaction_cost_bps),
            }
            specs.append(
                {
                    "dynamic_candidate_pack_id": pack_stem,
                    "dynamic_allocation_policy_id": dynamic_policy.default_policy_id,
                    "trend_signal_config_id": trend_id,
                    "allocation_profile_id": profile_id,
                    "iteration_stage": iteration_stage,
                    "trend_config": trend_config,
                    "cache_key_context": key_context,
                    "cache_keys": {
                        "trend_score": _stable_id(
                            "trend-score-cache", trend_id, _stable_hash(key_context)
                        ),
                        "allocation_path": _stable_id(
                            "allocation-path-cache", pack_stem, _stable_hash(key_context)
                        ),
                        "dynamic_backtest": _stable_id(
                            "dynamic-backtest-cache", pack_stem, _stable_hash(key_context)
                        ),
                        "candidate_pack_aggregation": _stable_id(
                            "candidate-pack-cache",
                            pack_id,
                            _stable_hash(key_context),
                        ),
                    },
                    "backtest_config_hash": _stable_hash({**base, **key_context}),
                    "source_trend_report": str(trend_report_path or ""),
                    "profile_index": profile_index,
                }
            )
            profile_index += 1
    return specs


def _candidate_profile_ids(policy: DynamicCalibrationPolicyConfig) -> list[str]:
    profiles = list(policy.batch.allocation_score_profiles)
    if policy.coarse_to_fine_iteration.local_refinement_enabled:
        profiles.extend(
            policy.batch.local_refinement_profiles[
                : policy.coarse_to_fine_iteration.local_refinement_limit
            ]
        )
    deduped: list[str] = []
    for profile in profiles:
        if profile not in deduped:
            deduped.append(profile)
    return deduped


def _build_trend_score_cache_payload(spec: dict[str, Any]) -> dict[str, Any]:
    trend_config = _mapping(spec.get("trend_config"))
    score_summary = _mapping(trend_config.get("score_summary"))
    band_counts = _mapping(score_summary.get("band_counts"))
    attribution = _mapping(trend_config.get("attribution_summary"))
    mean_score = _float(score_summary.get("mean_composite_score"), default=50.0)
    min_score = _float(
        score_summary.get("min_composite_score"), default=max(0.0, mean_score - 20.0)
    )
    max_score = _float(
        score_summary.get("max_composite_score"), default=min(100.0, mean_score + 20.0)
    )
    trend_score_series = [
        {
            "sample_index": index,
            "regime_label": regime,
            "CompositeTrendScore": _clamp(
                mean_score + ((index % 3) - 1) * max(1.0, (max_score - min_score) / 6.0),
                0.0,
                100.0,
            ),
            "score_band_count": int(_float(count, default=0.0)),
            "evaluation_only": True,
        }
        for index, (regime, count) in enumerate(sorted(band_counts.items()))
    ]
    if not trend_score_series:
        trend_score_series = [
            {
                "sample_index": 0,
                "regime_label": "neutral",
                "CompositeTrendScore": mean_score,
                "score_band_count": 0,
                "evaluation_only": True,
            }
        ]
    return {
        "schema_version": "etf_dynamic_calibration_trend_score_cache_v1",
        "trend_signal_config_id": spec["trend_signal_config_id"],
        "trend_score_series": trend_score_series,
        "score_bands": dict(band_counts),
        "regime_labels": [row["regime_label"] for row in trend_score_series],
        "score_summary": dict(score_summary),
        "attribution_summary": dict(attribution),
        "source_status": _text(trend_config.get("status"), "unknown"),
        "evaluation_only": True,
    }


def _build_allocation_path_cache_payload(
    *,
    spec: dict[str, Any],
    policy: DynamicCalibrationPolicyConfig,
    dynamic_policy: DynamicAllocationPolicyConfig,
    trend_scores: dict[str, Any],
) -> dict[str, Any]:
    start_date = policy.market_regime.default_evaluation_start
    profile_ids = [spec["allocation_profile_id"]]
    if spec["iteration_stage"] == "local_refinement":
        profile_ids = ["neutral", spec["allocation_profile_id"]]
    elif spec["allocation_profile_id"] != "neutral":
        profile_ids = ["neutral", spec["allocation_profile_id"]]
    previous_weights = dict(dynamic_policy.base_weights)
    previous_scores = dynamic_policy.sample_score_profiles.get("neutral")
    decisions: list[dict[str, Any]] = []
    for idx, profile_id in enumerate(profile_ids):
        scores = dynamic_policy.sample_score_profiles[profile_id]
        decision = build_dynamic_allocation_decision_record(
            policy=dynamic_policy,
            decision_date=start_date
            + timedelta(days=idx * dynamic_policy.rebalance_policy.minimum_holding_days),
            input_scores=dict(scores),
            previous_weights=previous_weights,
            previous_scores=previous_scores,
            days_since_last_rebalance=dynamic_policy.rebalance_policy.minimum_holding_days + idx,
            confirmed_regime_days=dynamic_policy.rebalance_policy.regime_confirmation_days + idx,
            source_trend_report=str(spec.get("source_trend_report") or ""),
            data_quality_status=_data_quality_status_from_trend_scores(trend_scores),
        )
        decisions.append(decision)
        previous_weights = dict(decision["candidate_target_weights"])
        previous_scores = dict(scores)
    daily_target_weights = [
        {
            "date": decision["date"],
            "candidate_target_weights": decision["candidate_target_weights"],
            "rebalance_decision": _mapping(decision.get("rebalance_decision")).get("decision"),
            "constraint_diagnostics": decision.get("constraint_diagnostics", []),
            "reason_codes": decision.get("reason_codes", []),
        }
        for decision in decisions
    ]
    turnover = sum(
        _float(_mapping(item.get("rebalance_decision")).get("turnover_estimate"))
        for item in decisions
    )
    return {
        "schema_version": "etf_dynamic_calibration_allocation_path_cache_v1",
        "dynamic_candidate_pack_id": spec["dynamic_candidate_pack_id"],
        "trend_signal_config_id": spec["trend_signal_config_id"],
        "allocation_profile_id": spec["allocation_profile_id"],
        "decision_count": len(decisions),
        "daily_target_weights": daily_target_weights,
        "rebalance_decisions": [
            _mapping(decision.get("rebalance_decision")).get("decision") for decision in decisions
        ],
        "constraint_diagnostics": [
            diagnostic
            for decision in decisions
            for diagnostic in _records(decision.get("constraint_diagnostics"))
        ],
        "reason_codes": sorted(
            {
                str(reason)
                for decision in decisions
                for reason in _records(decision.get("reason_codes"))
            }
        ),
        "turnover_estimate": round(turnover, 10),
        "latest_candidate_target_weights": decisions[-1]["candidate_target_weights"],
        "allocation_decision_path": decisions,
        "evaluation_only": True,
    }


def _build_dynamic_backtest_cache_payload(
    *,
    spec: dict[str, Any],
    policy: DynamicCalibrationPolicyConfig,
    allocation_path: dict[str, Any],
) -> dict[str, Any]:
    trend_config = _mapping(spec.get("trend_config"))
    attribution = _mapping(trend_config.get("attribution_summary"))
    weights = _mapping(allocation_path.get("latest_candidate_target_weights"))
    turnover = _float(allocation_path.get("turnover_estimate"))
    return_lift = _float(attribution.get("return_lift"))
    drawdown_improvement = _float(attribution.get("drawdown_improvement"))
    semi_weight = _float(weights.get("SMH")) + _float(weights.get("SOXX"))
    growth_weight = _float(weights.get("QQQ"))
    cash_weight = _float(weights.get("CASH"))
    proxy = policy.calibration_proxy
    portfolio_return = (
        return_lift
        + semi_weight * proxy.semiconductor_return_scale
        + growth_weight * proxy.growth_return_scale
        - cash_weight * proxy.cash_drag_scale
        - turnover * proxy.turnover_cost_scale
        - _redundancy_penalty(trend_config, proxy)
    )
    max_drawdown = max(
        0.01,
        proxy.max_drawdown_base
        - drawdown_improvement
        - cash_weight * proxy.cash_drawdown_relief_scale
        + semi_weight * proxy.semiconductor_drawdown_penalty_scale,
    )
    volatility = max(0.01, 0.12 + semi_weight * 0.10 + growth_weight * 0.05 - cash_weight * 0.04)
    sharpe = portfolio_return / volatility
    static_base = _float(proxy.benchmark_proxy_returns.get("static_base_candidate"))
    benchmark_metrics = {
        name: {
            "proxy_return": value,
            "excess_return_proxy": round(portfolio_return - value, 10),
            "evaluation_only": True,
        }
        for name, value in proxy.benchmark_proxy_returns.items()
    }
    return {
        "schema_version": "etf_dynamic_calibration_dynamic_backtest_cache_v1",
        "dynamic_candidate_pack_id": spec["dynamic_candidate_pack_id"],
        "metric_scope": "calibration_proxy",
        "full_robustness_backtest_required": True,
        "portfolio_return_proxy": round(portfolio_return, 10),
        "excess_vs_static_base_proxy": round(portfolio_return - static_base, 10),
        "max_drawdown_proxy": round(max_drawdown, 10),
        "volatility_proxy": round(volatility, 10),
        "sharpe_proxy": round(sharpe, 10),
        "turnover_estimate": round(turnover, 10),
        "benchmark_metrics": benchmark_metrics,
        "regime_metrics": {
            "regime_stability_status": _text(attribution.get("regime_stability_status"), "unknown"),
            "semiconductor_weight": round(semi_weight, 10),
            "growth_weight": round(growth_weight, 10),
            "cash_weight": round(cash_weight, 10),
        },
        "comparison_to_static_base": {
            "static_base_return_proxy": static_base,
            "dynamic_excess_return_proxy": round(portfolio_return - static_base, 10),
            "evaluation_only": True,
        },
        "evaluation_only": True,
    }


def _rank_candidate_pack(
    *,
    policy: DynamicCalibrationPolicyConfig,
    spec: dict[str, Any],
    trend_scores: dict[str, Any],
    allocation_path: dict[str, Any],
    backtest: dict[str, Any],
) -> dict[str, Any]:
    attribution = _mapping(trend_scores.get("attribution_summary"))
    proxy = policy.calibration_proxy
    ranking_policy = policy.ranking_policy
    return_score = _clamp(
        _float(backtest.get("portfolio_return_proxy")) * proxy.risk_adjusted_return_scale,
        0.0,
        100.0,
    )
    drawdown_score = _clamp(
        100.0
        - (_float(backtest.get("max_drawdown_proxy")) / max(proxy.max_drawdown_base, 1e-8)) * 100.0,
        0.0,
        100.0,
    )
    turnover_score = _clamp(
        100.0 - (_float(backtest.get("turnover_estimate")) / proxy.turnover_full_penalty) * 100.0,
        0.0,
        100.0,
    )
    regime_score = 80.0 if _text(attribution.get("regime_stability_status")) == "usable" else 45.0
    reason_codes = _records(allocation_path.get("reason_codes"))
    false_signal_score = _clamp(
        100.0
        - len([code for code in reason_codes if "RISK" in str(code)])
        * proxy.false_signal_penalty_scale
        * 100.0,
        0.0,
        100.0,
    )
    data_quality_score = (
        85.0 if _data_quality_status_from_trend_scores(trend_scores).startswith("PASS") else 50.0
    )
    redundancy_summary = _mapping(_mapping(spec.get("trend_config")).get("redundancy_summary"))
    overfit_score = 60.0 if redundancy_summary.get("risk_level") == "high" else 85.0
    components = {
        "risk_adjusted_return": round(return_score, 6),
        "drawdown_reduction": round(drawdown_score, 6),
        "turnover_control": round(turnover_score, 6),
        "regime_robustness": round(regime_score, 6),
        "false_signal_penalty": round(false_signal_score, 6),
        "data_quality": round(data_quality_score, 6),
        "overfit_risk": round(overfit_score, 6),
    }
    ranking_score = (
        components["risk_adjusted_return"] * ranking_policy.risk_adjusted_return_weight
        + components["drawdown_reduction"] * ranking_policy.drawdown_reduction_weight
        + components["turnover_control"] * ranking_policy.turnover_control_weight
        + components["regime_robustness"] * ranking_policy.regime_robustness_weight
        + components["false_signal_penalty"] * ranking_policy.false_signal_penalty_weight
        + components["data_quality"] * ranking_policy.data_quality_weight
        + components["overfit_risk"] * ranking_policy.overfit_risk_weight
    )
    blockers: list[str] = []
    warnings: list[str] = []
    if redundancy_summary.get("risk_level") == "high":
        warnings.append("high_trend_signal_redundancy_requires_TRADING_086_review")
    if _data_quality_status_from_trend_scores(trend_scores) not in {
        "PASS",
        "PASS_WITH_WARNINGS",
        "VALIDATION_SAMPLE",
    }:
        blockers.append("data_quality_not_pass")
    status = (
        "candidate_for_robustness_review"
        if ranking_score >= ranking_policy.min_score_for_robustness_review and not blockers
        else "review_required"
    )
    return {
        "ranking_score": round(ranking_score, 6),
        "ranking_components": components,
        "ranking_weights": ranking_policy.model_dump(mode="json"),
        "status": status,
        "blockers": blockers,
        "warnings": warnings,
        "ranking_reasons": _ranking_reasons(components, warnings),
        "return_only_ranking_blocked": True,
        "full_robustness_backtest_required": True,
    }


def _resolve_trend_config_candidates(
    *,
    trend_report: dict[str, Any] | None,
    policy: DynamicCalibrationPolicyConfig,
    trend_policy_path: Path,
) -> dict[str, Any]:
    registry = _mapping((trend_report or {}).get("trend_signal_config_registry"))
    configs = _records(registry.get("configs"))
    if configs:
        return {
            "source_status": "latest_trend_registry_configs",
            "limitation": "",
            "trend_configs": configs[: policy.batch.max_trend_configs],
        }
    trend_policy = load_trend_calibration_policy_config(trend_policy_path)
    fallback_configs = [
        {
            "trend_signal_config_id": item.weight_set_id,
            "status": item.status,
            "weights": item.weights,
            "score_summary": {
                "mean_composite_score": 60.0,
                "min_composite_score": 35.0,
                "max_composite_score": 85.0,
                "band_counts": {"neutral": 1, "risk_on": 1, "weak": 1},
            },
            "attribution_summary": {
                "return_lift": 0.0,
                "drawdown_improvement": 0.0,
                "regime_stability_status": "validation_sample",
            },
            "redundancy_summary": {"risk_level": "unknown"},
            "regime_stability": {"limited_regime_count": 0},
        }
        for item in trend_policy.search.preset_weight_sets
    ]
    return {
        "source_status": "trend_policy_preset_configs",
        "limitation": (
            "Latest trend report had no registry configs; using reviewed policy "
            "preset configs for validation/sample batch only."
        ),
        "trend_configs": fallback_configs[: policy.batch.max_trend_configs],
    }


def _cached_layer(
    *,
    layer: str,
    cache_key: str,
    policy: DynamicCalibrationPolicyConfig,
    cache_mode: Literal["read-write", "read-only", "disabled"],
    cache_root: Path,
    cache_events: list[dict[str, Any]],
    builder,
) -> dict[str, Any]:
    enabled = bool(policy.cache.enabled_layers.get(layer, False))
    if cache_mode == "disabled" or not enabled:
        payload = builder()
        cache_events.append(_cache_event(layer, cache_key, "disabled"))
        return payload
    cache_path = _cache_path(cache_root, layer, cache_key)
    if cache_path.exists():
        try:
            cached = json.loads(cache_path.read_text(encoding="utf-8"))
            payload = _mapping(cached.get("payload"))
            cache_events.append(_cache_event(layer, cache_key, "hit", cache_path))
            return dict(payload)
        except Exception:  # noqa: BLE001
            cache_events.append(_cache_event(layer, cache_key, "corrupt", cache_path))
    payload = builder()
    if cache_mode == "read-write":
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        wrapped = {
            "schema_version": policy.cache.schema_versions.get("cache_manifest"),
            "cache_layer": layer,
            "cache_key": cache_key,
            "written_at": datetime.now(UTC).isoformat(),
            "engine_version": ENGINE_VERSION,
            "payload_hash": _stable_hash(payload),
            "payload": payload,
            "safety": policy.safety.model_dump(mode="json"),
            **DYNAMIC_CALIBRATION_SAFETY,
        }
        _assert_dynamic_calibration_payload_safe(wrapped)
        _write_json(wrapped, cache_path)
        cache_events.append(_cache_event(layer, cache_key, "miss_written", cache_path))
    else:
        cache_events.append(_cache_event(layer, cache_key, "miss"))
    return payload


def _build_run_manifest(
    *,
    policy: DynamicCalibrationPolicyConfig,
    generated: datetime,
    pack_id: str,
    trend_report_path: Path | None,
    trend_source_status: str,
    cache_mode: str,
    cache_root: Path,
    worker_count: int,
    candidate_count: int,
) -> dict[str, Any]:
    return {
        "schema_version": DYNAMIC_CALIBRATION_RUN_MANIFEST_SCHEMA_VERSION,
        "run_id": _stable_id("dynamic-calibration-run", pack_id, generated.isoformat()),
        "generated_at": generated.isoformat(),
        "pack_id": pack_id,
        "data_range_preset": policy.batch.data_range_preset,
        "trend_report_path": str(trend_report_path or ""),
        "trend_source_status": trend_source_status,
        "cache_mode": cache_mode,
        "cache_root": str(cache_root),
        "worker_count": worker_count,
        "parallel_execution": policy.performance.parallel_execution,
        "resume_supported": policy.performance.resume_supported,
        "partial_candidate_recompute_supported": (
            policy.performance.partial_candidate_recompute_supported
        ),
        "candidate_count": candidate_count,
        "commands_executed": False,
        **DYNAMIC_CALIBRATION_SAFETY,
    }


def _report_summary(
    top_packs: list[dict[str, Any]],
    source: dict[str, Any],
    trend_report: dict[str, Any] | None,
) -> dict[str, Any]:
    top = top_packs[0] if top_packs else {}
    ranking = _mapping(top.get("ranking"))
    backtest = _mapping(top.get("dynamic_backtest_summary"))
    return {
        "status": "PASS" if top_packs else "NO_CANDIDATES",
        "top_dynamic_candidate_pack_id": top.get("dynamic_candidate_pack_id", ""),
        "top_trend_signal_config_id": top.get("trend_signal_config_id", ""),
        "top_allocation_profile_id": top.get("allocation_profile_id", ""),
        "top_ranking_score": ranking.get("ranking_score", 0.0),
        "top_candidate_status": ranking.get("status", "missing"),
        "top_return_proxy": backtest.get("portfolio_return_proxy", 0.0),
        "top_drawdown_proxy": backtest.get("max_drawdown_proxy", 0.0),
        "top_turnover_estimate": backtest.get("turnover_estimate", 0.0),
        "data_quality_status": _trend_report_data_quality(trend_report),
        "source_trend_config_status": source["source_status"],
        "full_robustness_backtest_required": True,
        "handoff_task": "TRADING-086",
    }


def _ranking_reasons(components: dict[str, float], warnings: list[str]) -> list[str]:
    reasons = [
        f"risk_adjusted_return_component={components['risk_adjusted_return']}",
        f"drawdown_reduction_component={components['drawdown_reduction']}",
        f"turnover_control_component={components['turnover_control']}",
        f"regime_robustness_component={components['regime_robustness']}",
    ]
    reasons.extend(warnings)
    return reasons


def _cache_summary(events: list[dict[str, Any]], cache_mode: str) -> dict[str, Any]:
    hit = sum(1 for event in events if event["cache_status"] == "hit")
    miss = sum(1 for event in events if event["cache_status"] in {"miss", "miss_written"})
    writes = sum(1 for event in events if event["cache_status"] == "miss_written")
    layers = sorted({str(event["cache_layer"]) for event in events})
    total = len(events)
    return {
        "cache_mode": cache_mode,
        "cache_event_count": total,
        "cache_hit_count": hit,
        "cache_miss_count": miss,
        "cache_write_count": writes,
        "cache_hit_rate": round(hit / total, 6) if total else 0.0,
        "layers": layers,
    }


def _cache_event(
    layer: str, cache_key: str, status: str, path: Path | None = None
) -> dict[str, Any]:
    return {
        "cache_layer": layer,
        "cache_key": cache_key,
        "cache_status": status,
        "cache_path": str(path or ""),
    }


def _cache_path(cache_root: Path, layer: str, cache_key: str) -> Path:
    safe_key = "".join(ch if ch.isalnum() or ch in {"-", "_"} else "_" for ch in cache_key)
    return cache_root / layer / f"{safe_key}.json"


def _resolve_cache_root(policy: DynamicCalibrationPolicyConfig) -> Path:
    configured = Path(policy.cache.cache_root)
    if configured.is_absolute():
        return configured
    return PROJECT_ROOT / configured


def _trend_report_status(trend_report: dict[str, Any] | None) -> str:
    if not trend_report:
        return "MISSING"
    return _text(
        trend_report.get("status"),
        _mapping(trend_report.get("summary")).get("evidence_status", "UNKNOWN"),
    )


def _trend_report_data_quality(trend_report: dict[str, Any] | None) -> str:
    if not trend_report:
        return "VALIDATION_SAMPLE"
    return _text(
        _mapping(trend_report.get("summary")).get(
            "data_quality_status",
            _mapping(trend_report.get("dataset_coverage")).get("data_quality_status", "UNKNOWN"),
        ),
        "UNKNOWN",
    )


def _data_quality_status_from_trend_scores(trend_scores: dict[str, Any]) -> str:
    return _text(trend_scores.get("data_quality_status"), "PASS_WITH_WARNINGS")


def _redundancy_penalty(
    trend_config: dict[str, Any], proxy: DynamicCalibrationProxyConfig
) -> float:
    redundancy = _mapping(trend_config.get("redundancy_summary"))
    return proxy.overfit_redundancy_penalty if redundancy.get("risk_level") == "high" else 0.0


def _artifact_suffix(payload: dict[str, Any]) -> str:
    return _stable_hash(payload)[:12]


def _append_check(checks: list[dict[str, Any]], check_id: str, passed: bool, detail: str) -> None:
    checks.append({"check_id": check_id, "passed": bool(passed), "detail": detail})


def _latest_file(directory: Path, pattern: str) -> Path | None:
    if not directory.exists():
        return None
    candidates = [path for path in directory.glob(pattern) if path.is_file()]
    if not candidates:
        return None
    return max(candidates, key=lambda path: (path.stat().st_mtime, path.name))


def _write_json(payload: dict[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )


def _write_text(text: str, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def _records(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []


def _text(value: Any, default: str = "") -> str:
    return str(value) if value not in (None, "") else default


def _float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def _stable_hash(value: Any) -> str:
    return sha256(
        json.dumps(value, ensure_ascii=False, sort_keys=True, default=str).encode("utf-8")
    ).hexdigest()


def _file_hash(path: Path) -> str:
    if not path.exists():
        return "missing"
    return sha256(path.read_bytes()).hexdigest()


def _stable_id(prefix: str, *parts: Any) -> str:
    return f"{prefix}_{_stable_hash([prefix, *parts])[:12]}"


def _markdown_mapping(value: Any) -> str:
    mapping = _mapping(value)
    if not mapping:
        return "- none"
    return "\n".join(f"- {key}: {item}" for key, item in sorted(mapping.items()))


def _assert_dynamic_calibration_payload_safe(payload: dict[str, Any]) -> None:
    forbidden = sorted(set(_forbidden_keys_in_payload(payload)))
    if forbidden:
        raise DynamicCalibrationError(
            "dynamic calibration payload contains forbidden keys: " + ", ".join(sorted(forbidden))
        )
    for key, expected in DYNAMIC_CALIBRATION_SAFETY.items():
        if payload.get(key) != expected:
            raise DynamicCalibrationError(f"dynamic calibration payload has unsafe {key}")
    nested = _mapping(payload.get("safety"))
    if nested and nested != DYNAMIC_CALIBRATION_SAFETY:
        raise DynamicCalibrationError("dynamic calibration nested safety fields are unsafe")


def _forbidden_keys_in_payload(value: Any) -> list[str]:
    found: list[str] = []
    if isinstance(value, dict):
        for key, item in value.items():
            if str(key) in FORBIDDEN_DYNAMIC_CALIBRATION_KEYS:
                found.append(str(key))
            found.extend(_forbidden_keys_in_payload(item))
    elif isinstance(value, list):
        for item in value:
            found.extend(_forbidden_keys_in_payload(item))
    return found


def load_latest_trend_report(path: Path | None = None) -> tuple[Path | None, dict[str, Any]]:
    resolved = path or latest_trend_calibration_report_path(DEFAULT_TREND_CALIBRATION_REPORT_DIR)
    if resolved is None or not resolved.exists():
        return None, {}
    return resolved, json.loads(resolved.read_text(encoding="utf-8"))
