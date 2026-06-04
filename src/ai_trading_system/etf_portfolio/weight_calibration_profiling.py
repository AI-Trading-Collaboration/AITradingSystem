from __future__ import annotations

import cProfile
import csv
import json
import pstats
from collections import defaultdict
from collections.abc import Callable, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from time import perf_counter
from typing import Any, Literal, Self, TypeVar

from pydantic import BaseModel, Field, model_validator

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.etf_portfolio.models import PolicyMetadata
from ai_trading_system.yaml_loader import safe_load_yaml_path

DEFAULT_WEIGHT_CALIBRATION_PROFILING_POLICY_CONFIG_PATH = (
    PROJECT_ROOT / "config" / "etf_portfolio" / "profiling_policy.yaml"
)
DEFAULT_WEIGHT_CALIBRATION_PROFILING_REPORT_DIR = (
    PROJECT_ROOT / "reports" / "etf_portfolio" / "weight_calibration" / "profiling"
)
DEFAULT_WEIGHT_CALIBRATION_PROFILING_VALIDATION_DIR = (
    PROJECT_ROOT / "reports" / "etf_portfolio" / "weight_calibration" / "validation"
)

WEIGHT_CALIBRATION_PROFILING_POLICY_SCHEMA_VERSION = (
    "etf_weight_calibration_profiling_policy_v1"
)
WEIGHT_CALIBRATION_PROFILING_REPORT_SCHEMA_VERSION = (
    "etf_weight_calibration_profiling_report_v1"
)
WEIGHT_CALIBRATION_CANDIDATE_HOTSPOT_SCHEMA_VERSION = (
    "etf_weight_calibration_candidate_hotspots_v1"
)
WEIGHT_CALIBRATION_PROFILING_VALIDATION_SCHEMA_VERSION = (
    "etf_weight_calibration_profiling_validation_v1"
)

WEIGHT_CALIBRATION_PROFILING_SAFETY = {
    "observe_only": True,
    "candidate_only": True,
    "production_effect": "none",
    "broker_action": "none",
    "manual_review_required": True,
}

ProfilingMode = Literal["off", "summary", "detailed", "cprofile"]
T = TypeVar("T")


class WeightCalibrationProfilingError(ValueError):
    """Raised when weight-calibration profiling policy or reports are invalid."""


class WeightCalibrationProfilingSafety(BaseModel):
    observe_only: Literal[True]
    candidate_only: Literal[True]
    production_effect: Literal["none"]
    broker_action: Literal["none"]
    manual_review_required: Literal[True]


class WeightCalibrationProfilingModeSettings(BaseModel):
    enabled: bool
    step_timing: bool = False
    cache_timing: bool = False
    worker_timing: bool = False
    candidate_timing: bool = False
    cprofile: bool = False

    @model_validator(mode="after")
    def validate_disabled_mode(self) -> Self:
        if not self.enabled and any(
            (
                self.step_timing,
                self.cache_timing,
                self.worker_timing,
                self.candidate_timing,
                self.cprofile,
            )
        ):
            raise ValueError("disabled profiling mode cannot enable timing features")
        return self


class WeightCalibrationProfilingThresholds(BaseModel):
    slow_step_seconds: float = Field(gt=0)
    slow_candidate_seconds: float = Field(gt=0)
    cold_run_profile_trigger_seconds: float = Field(gt=0)
    slow_cache_entry_seconds: float = Field(gt=0)
    worker_imbalance_ratio: float = Field(gt=1)
    regime_mask_precompute_reuse_floor: int = Field(ge=1)


class WeightCalibrationProfilingSettings(BaseModel):
    default_mode: ProfilingMode
    modes: dict[ProfilingMode, WeightCalibrationProfilingModeSettings] = Field(
        min_length=4,
    )
    thresholds: WeightCalibrationProfilingThresholds
    top_n: int = Field(ge=1, le=1000)
    safety: WeightCalibrationProfilingSafety

    @model_validator(mode="after")
    def validate_modes(self) -> Self:
        required_modes = {"off", "summary", "detailed", "cprofile"}
        missing = required_modes - set(self.modes)
        if missing:
            raise ValueError("profiling modes missing: " + ", ".join(sorted(missing)))
        if self.default_mode not in self.modes:
            raise ValueError("default_mode must be present in modes")
        if self.modes["off"].enabled:
            raise ValueError("off profiling mode must be disabled")
        if not self.modes["cprofile"].cprofile:
            raise ValueError("cprofile mode must enable cprofile")
        return self


class WeightCalibrationProfilingPolicyConfig(BaseModel):
    schema_version: Literal["etf_weight_calibration_profiling_policy_v1"]
    policy_metadata: PolicyMetadata
    weight_calibration_profiling: WeightCalibrationProfilingSettings


@dataclass(frozen=True)
class StepTimingRecord:
    step_id: str
    start_time: str
    end_time: str
    duration_seconds: float
    status: str
    warning_if_slow: bool
    parent_step_id: str | None = None

    def as_payload(self) -> dict[str, Any]:
        return {
            "step_id": self.step_id,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "duration_seconds": round(self.duration_seconds, 6),
            "status": self.status,
            "warning_if_slow": self.warning_if_slow,
            "parent_step_id": self.parent_step_id,
            "safety": dict(WEIGHT_CALIBRATION_PROFILING_SAFETY),
            **WEIGHT_CALIBRATION_PROFILING_SAFETY,
        }


class WeightCalibrationRuntimeProfiler:
    def __init__(
        self,
        *,
        mode: str | None = None,
        policy: WeightCalibrationProfilingPolicyConfig | None = None,
        started_at: datetime | None = None,
    ) -> None:
        self.policy = policy or load_weight_calibration_profiling_policy_config()
        self.mode = normalize_weight_calibration_profile_mode(mode, policy=self.policy)
        self.started_at = started_at or datetime.now(UTC)
        self._stack: list[str] = []
        self._records: list[StepTimingRecord] = []

    @property
    def settings(self) -> WeightCalibrationProfilingModeSettings:
        return self.policy.weight_calibration_profiling.modes[self.mode]

    @property
    def enabled(self) -> bool:
        return self.settings.enabled

    @contextmanager
    def profile_step(self, step_id: str) -> Any:
        if not self.enabled or not self.settings.step_timing:
            yield
            return
        parent = self._stack[-1] if self._stack else None
        self._stack.append(step_id)
        started_at = datetime.now(UTC)
        started_perf = perf_counter()
        status = "completed"
        try:
            yield
        except Exception:
            status = "failed"
            raise
        finally:
            duration = perf_counter() - started_perf
            ended_at = datetime.now(UTC)
            slow = (
                duration
                >= self.policy.weight_calibration_profiling.thresholds.slow_step_seconds
            )
            self._records.append(
                StepTimingRecord(
                    step_id=step_id,
                    start_time=started_at.isoformat(),
                    end_time=ended_at.isoformat(),
                    duration_seconds=duration,
                    status=status,
                    warning_if_slow=slow,
                    parent_step_id=parent,
                )
            )
            self._stack.pop()

    def record_step(
        self,
        step_id: str,
        *,
        duration_seconds: float,
        status: str = "completed",
        parent_step_id: str | None = None,
        started_at: datetime | None = None,
        ended_at: datetime | None = None,
    ) -> None:
        if not self.enabled or not self.settings.step_timing:
            return
        start = started_at or datetime.now(UTC)
        end = ended_at or start
        slow = (
            float(duration_seconds)
            >= self.policy.weight_calibration_profiling.thresholds.slow_step_seconds
        )
        self._records.append(
            StepTimingRecord(
                step_id=step_id,
                start_time=start.isoformat(),
                end_time=end.isoformat(),
                duration_seconds=float(duration_seconds),
                status=status,
                warning_if_slow=slow,
                parent_step_id=parent_step_id,
            )
        )

    def records(self) -> list[dict[str, Any]]:
        return [record.as_payload() for record in self._records]

    def summary(self, *, top_n: int | None = None) -> dict[str, Any]:
        return build_step_timing_summary(
            self.records(),
            top_n=top_n or self.policy.weight_calibration_profiling.top_n,
        )


def load_weight_calibration_profiling_policy_config(
    path: Path | str = DEFAULT_WEIGHT_CALIBRATION_PROFILING_POLICY_CONFIG_PATH,
) -> WeightCalibrationProfilingPolicyConfig:
    raw = safe_load_yaml_path(Path(path))
    if not isinstance(raw, dict):
        raise WeightCalibrationProfilingError(
            "weight calibration profiling policy must be a mapping"
        )
    try:
        return WeightCalibrationProfilingPolicyConfig.model_validate(raw)
    except ValueError as exc:
        raise WeightCalibrationProfilingError(str(exc)) from exc


def normalize_weight_calibration_profile_mode(
    value: str | None,
    *,
    policy: WeightCalibrationProfilingPolicyConfig | None = None,
) -> ProfilingMode:
    settings = policy.weight_calibration_profiling if policy is not None else None
    raw_value = value if value is not None else (settings.default_mode if settings else "summary")
    token = str(raw_value).strip().lower().replace("-", "_")
    aliases = {
        "none": "off",
        "disabled": "off",
        "false": "off",
        "0": "off",
        "off": "off",
        "summary": "summary",
        "detailed": "detailed",
        "detail": "detailed",
        "c_profile": "cprofile",
        "cprofile": "cprofile",
        "profile": "cprofile",
    }
    try:
        normalized = aliases[token]
    except KeyError as exc:
        raise WeightCalibrationProfilingError(f"invalid profiling mode: {value}") from exc
    if settings is not None and normalized not in settings.modes:
        raise WeightCalibrationProfilingError(
            f"profiling mode is not allowed by policy: {normalized}"
        )
    return normalized  # type: ignore[return-value]


def profiling_mode_settings(
    policy: WeightCalibrationProfilingPolicyConfig,
    mode: str | None,
) -> WeightCalibrationProfilingModeSettings:
    return policy.weight_calibration_profiling.modes[
        normalize_weight_calibration_profile_mode(mode, policy=policy)
    ]


def build_step_timing_summary(
    step_records: Sequence[Mapping[str, Any]],
    *,
    top_n: int = 20,
) -> dict[str, Any]:
    rows = [dict(record) for record in step_records]
    sorted_rows = sorted(
        rows,
        key=lambda row: (
            -_float(row.get("duration_seconds")),
            _text(row.get("step_id")),
            _text(row.get("start_time")),
        ),
    )
    return {
        "step_timings": rows,
        "slowest_steps": sorted_rows[:top_n],
        "total_profiled_seconds": round(
            sum(_float(record.get("duration_seconds")) for record in rows),
            6,
        ),
        "unprofiled_overhead_seconds": None,
        "safety": dict(WEIGHT_CALIBRATION_PROFILING_SAFETY),
        **WEIGHT_CALIBRATION_PROFILING_SAFETY,
    }


def build_weight_calibration_candidate_hotspot_table(
    diagnostics_payload: Mapping[str, Any],
    *,
    top_n: int = 20,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    timing_rows = _profiling_records(diagnostics_payload, "candidate_timings")
    if not timing_rows:
        timing_rows = _records(diagnostics_payload.get("candidate_timings"))
    enriched = [_candidate_hotspot_row(row) for row in timing_rows]
    ranked = sorted(
        enriched,
        key=lambda row: (
            -_float(row.get("total_candidate_seconds")),
            _text(row.get("search_id")),
            _text(row.get("preset_id")),
            _text(row.get("candidate_id")),
        ),
    )
    for rank, row in enumerate(ranked, start=1):
        row["rank"] = row.get("rank") or rank
        row["hotspot_rank"] = rank
    payload = {
        "schema_version": WEIGHT_CALIBRATION_CANDIDATE_HOTSPOT_SCHEMA_VERSION,
        "report_type": "etf_weight_calibration_candidate_hotspots",
        "generated_at": generated.isoformat(),
        "run_id": _run_id(diagnostics_payload),
        "top_n": int(top_n),
        "candidate_count": len(ranked),
        "hotspots": ranked[:top_n],
        "cache_hit_candidates": [
            row for row in ranked if _text(row.get("cache_status")).startswith("hit")
        ][:top_n],
        "failed_candidates": [
            row for row in ranked if _text(row.get("status")).lower() == "failed"
        ],
        "safety": dict(WEIGHT_CALIBRATION_PROFILING_SAFETY),
        **WEIGHT_CALIBRATION_PROFILING_SAFETY,
    }
    validate_weight_calibration_candidate_hotspot_table(payload)
    return payload


def validate_weight_calibration_candidate_hotspot_table(payload: Mapping[str, Any]) -> None:
    issues = []
    if payload.get("schema_version") != WEIGHT_CALIBRATION_CANDIDATE_HOTSPOT_SCHEMA_VERSION:
        issues.append("schema_version")
    if payload.get("report_type") != "etf_weight_calibration_candidate_hotspots":
        issues.append("report_type")
    _extend_safety_issues(issues, payload)
    for row in _records(payload.get("hotspots")):
        if not row.get("candidate_id"):
            issues.append("hotspots.candidate_id")
        if _float(row.get("total_candidate_seconds")) < 0:
            issues.append("hotspots.total_candidate_seconds")
    if issues:
        raise WeightCalibrationProfilingError(
            "weight calibration candidate hotspot table validation failed: "
            + ", ".join(issues)
        )


def write_weight_calibration_candidate_hotspot_table(
    payload: Mapping[str, Any],
    *,
    output_dir: Path | str,
) -> dict[str, Path]:
    validate_weight_calibration_candidate_hotspot_table(payload)
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    json_path = output_path / "candidate_hotspots.json"
    csv_path = output_path / "candidate_hotspots.csv"
    markdown_path = output_path / "candidate_hotspots.md"
    _write_json(json_path, payload)
    _write_csv(csv_path, _records(payload.get("hotspots")))
    markdown_path.write_text(
        render_weight_calibration_candidate_hotspot_markdown(payload),
        encoding="utf-8",
    )
    return {"json": json_path, "csv": csv_path, "markdown": markdown_path}


def render_weight_calibration_candidate_hotspot_markdown(
    payload: Mapping[str, Any],
) -> str:
    lines = [
        "# ETF Weight Calibration Candidate Hotspots",
        "",
        "## Safety Banner",
        "",
        "- observe_only = true",
        "- candidate_only = true",
        "- production_effect = none",
        "- broker_action = none",
        "- manual_review_required = true",
        "",
        "## Slowest Candidates",
        "",
        "| Rank | Candidate | Search | Preset | Seconds | Cache | Worker | Readiness | Risk |",
        "|---:|---|---|---|---:|---|---|---|---|",
    ]
    for row in _records(payload.get("hotspots")):
        lines.append(
            "| "
            f"{row.get('hotspot_rank')} | {row.get('candidate_id')} | "
            f"{row.get('search_id')} | {row.get('preset_id')} | "
            f"{_format_seconds(row.get('total_candidate_seconds'))} | "
            f"{row.get('cache_status')} | {row.get('worker_id')} | "
            f"{row.get('readiness_status')} | {row.get('overfit_risk')} |"
        )
    return "\n".join(lines) + "\n"


def build_cache_timing_breakdown(
    cache_events: Sequence[Mapping[str, Any]],
    *,
    slow_entry_seconds: float = 1.0,
) -> dict[str, Any]:
    layers = (
        "price_returns_matrix",
        "candidate_backtest",
        "regime_robustness",
        "diagnostics_aggregation",
    )
    rows = []
    for layer in layers:
        layer_events = [event for event in cache_events if _text(event.get("cache_layer")) == layer]
        hit_count = _count_status(layer_events, "hit")
        miss_count = _count_status(layer_events, "miss")
        write_count = _count_status(layer_events, "write")
        total_reads = hit_count + miss_count
        rows.append(
            {
                "cache_layer": layer,
                "hit_count": hit_count,
                "miss_count": miss_count,
                "write_count": write_count,
                "read_seconds": round(
                    sum(
                        _event_seconds(event)
                        for event in layer_events
                        if _text(event.get("cache_status")) in {"hit", "miss"}
                    ),
                    6,
                ),
                "write_seconds": round(
                    sum(
                        _event_seconds(event)
                        for event in layer_events
                        if _text(event.get("cache_status")) == "write"
                    ),
                    6,
                ),
                "validation_seconds": round(
                    sum(_float(event.get("validation_seconds")) for event in layer_events),
                    6,
                ),
                "serialization_seconds": round(
                    sum(_float(event.get("serialization_seconds")) for event in layer_events),
                    6,
                ),
                "hit_rate": round(hit_count / total_reads, 6) if total_reads else None,
                "slow_cache_entries": [
                    dict(event)
                    for event in layer_events
                    if _event_seconds(event) >= slow_entry_seconds
                ],
            }
        )
    return {
        "cache_layers": rows,
        "cache_disabled": not bool(cache_events),
        "safety": dict(WEIGHT_CALIBRATION_PROFILING_SAFETY),
        **WEIGHT_CALIBRATION_PROFILING_SAFETY,
    }


def build_worker_timing_breakdown(
    candidate_timings: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    grouped: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for row in candidate_timings:
        grouped[_text(row.get("worker_id"), "main")].append(row)
    worker_rows = []
    for worker_id, rows in sorted(grouped.items()):
        durations = [_float(row.get("total_candidate_seconds")) for row in rows]
        completed = [row for row in rows if _text(row.get("status"), "completed") != "failed"]
        failed = [row for row in rows if _text(row.get("status"), "completed") == "failed"]
        worker_rows.append(
            {
                "worker_id": worker_id,
                "assigned_candidate_count": len(rows),
                "completed_candidate_count": len(completed),
                "failed_candidate_count": len(failed),
                "runtime_seconds": round(sum(durations), 6),
                "mean_candidate_seconds": round(
                    sum(durations) / len(durations),
                    6,
                )
                if durations
                else 0.0,
                "max_candidate_seconds": round(max(durations), 6) if durations else 0.0,
                "cache_hit_count": sum(
                    1 for row in rows if "hit" in _text(row.get("cache_status"))
                ),
                "cache_miss_count": sum(
                    1 for row in rows if "miss" in _text(row.get("cache_status"))
                ),
                "idle_or_wait_seconds": None,
            }
        )
    return {
        "workers": worker_rows,
        "worker_count": len(worker_rows),
        "candidate_count": sum(len(rows) for rows in grouped.values()),
        "safety": dict(WEIGHT_CALIBRATION_PROFILING_SAFETY),
        **WEIGHT_CALIBRATION_PROFILING_SAFETY,
    }


def build_vectorization_audit_report(
    diagnostics_payload: Mapping[str, Any],
) -> dict[str, Any]:
    observed = _observed_step_seconds(diagnostics_payload)
    areas = [
        _audit_area(
            "portfolio_returns_calculation",
            "per-candidate static weight accounting loop over trading days",
            observed.get("candidate_backtest"),
            True,
            "use_numpy_dot",
            "high",
        ),
        _audit_area(
            "drawdown_calculation",
            "drawdown is computed from return/equity series after candidate backtest",
            observed.get("candidate_metric_timing"),
            True,
            "precompute_cumulative_returns",
            "medium",
        ),
        _audit_area(
            "volatility_and_sharpe_metrics",
            "summary metrics operate on numeric return arrays",
            observed.get("candidate_metric_timing"),
            False,
            "already_vectorized",
            "low",
        ),
        _audit_area(
            "candidate_loop_structure",
            "candidate grid is evaluated one candidate at a time",
            observed.get("run_searches"),
            True,
            "batch_candidates",
            "high",
        ),
        _audit_area(
            "regime_slice_metrics",
            "regime slices are rebuilt per candidate robustness evaluation",
            observed.get("regime_robustness"),
            True,
            "precompute_regime_masks",
            "medium",
        ),
        _audit_area(
            "groupby_or_aggregation_usage",
            "diagnostics aggregation clusters top candidates after searches complete",
            observed.get("aggregate_diagnostics"),
            False,
            "no_action",
            "low",
        ),
        _audit_area(
            "serialization_or_io_loop",
            "cache JSON read/write and manifest validation are timed separately",
            observed.get("cache_write"),
            True,
            "reduce_serialization_overhead",
            "medium",
        ),
    ]
    return {
        "report_type": "etf_weight_calibration_vectorization_audit",
        "areas": areas,
        "safety": dict(WEIGHT_CALIBRATION_PROFILING_SAFETY),
        **WEIGHT_CALIBRATION_PROFILING_SAFETY,
    }


def build_regime_mask_precomputation_assessment(
    diagnostics_payload: Mapping[str, Any],
    *,
    policy: WeightCalibrationProfilingPolicyConfig | None = None,
) -> dict[str, Any]:
    floor = (
        policy.weight_calibration_profiling.thresholds.regime_mask_precompute_reuse_floor
        if policy is not None
        else 2
    )
    rows = _profiling_records(diagnostics_payload, "regime_mask_timings")
    if not rows:
        rows = _regime_rows_from_diagnostics(diagnostics_payload)
    assessments = []
    for row in rows:
        reuse_count = max(0, int(_float(row.get("mask_reuse_count"))))
        candidate_count = max(0, int(_float(row.get("candidate_count"))))
        mask_build_seconds = _float(row.get("mask_build_seconds"))
        potential_saved = max(0.0, mask_build_seconds * max(0, reuse_count - 1))
        assessments.append(
            {
                "regime_id": _text(row.get("regime_id"), "missing_regime"),
                "date_range": dict(_mapping(row.get("date_range"))),
                "mask_build_seconds": round(mask_build_seconds, 6),
                "mask_reuse_count": reuse_count,
                "candidate_count": candidate_count,
                "potential_saved_seconds": round(potential_saved, 6),
                "recommend_precompute": reuse_count >= floor and potential_saved > 0.0,
            }
        )
    return {
        "report_type": "etf_weight_calibration_regime_mask_assessment",
        "regime_count": len(assessments),
        "regimes": sorted(assessments, key=lambda item: _text(item.get("regime_id"))),
        "safety": dict(WEIGHT_CALIBRATION_PROFILING_SAFETY),
        **WEIGHT_CALIBRATION_PROFILING_SAFETY,
    }


def run_with_optional_cprofile(
    func: Callable[..., T],
    *args: Any,
    enabled: bool,
    **kwargs: Any,
) -> tuple[T, cProfile.Profile | None]:
    if not enabled:
        return func(*args, **kwargs), None
    profiler = cProfile.Profile()
    result = profiler.runcall(func, *args, **kwargs)
    return result, profiler


def write_cprofile_artifacts(
    profiler: cProfile.Profile,
    *,
    output_dir: Path | str,
    top_n: int,
) -> dict[str, Path]:
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    stats_path = output_path / "cprofile.stats"
    json_path = output_path / "cprofile_top_functions.json"
    markdown_path = output_path / "cprofile_top_functions.md"
    profiler.dump_stats(stats_path)
    rows = _cprofile_top_rows(profiler, top_n=top_n)
    payload = {
        "report_type": "etf_weight_calibration_cprofile_top_functions",
        "top_n": top_n,
        "functions": rows,
        "safety": dict(WEIGHT_CALIBRATION_PROFILING_SAFETY),
        **WEIGHT_CALIBRATION_PROFILING_SAFETY,
    }
    _write_json(json_path, payload)
    markdown_path.write_text(_render_cprofile_markdown(payload), encoding="utf-8")
    return {"stats": stats_path, "json": json_path, "markdown": markdown_path}


def build_weight_calibration_profiling_report(
    diagnostics_payload: Mapping[str, Any],
    *,
    policy: WeightCalibrationProfilingPolicyConfig,
    profile_mode: str | None,
    profile_top_n: int | None = None,
    cprofile_artifacts: Mapping[str, Path] | None = None,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    mode = normalize_weight_calibration_profile_mode(profile_mode, policy=policy)
    top_n = profile_top_n or policy.weight_calibration_profiling.top_n
    profiling = _mapping(diagnostics_payload.get("profiling"))
    step_summary = build_step_timing_summary(
        _records(profiling.get("step_timings")),
        top_n=top_n,
    )
    cache_events = _records(_mapping(diagnostics_payload.get("cache_summary")).get("cache_events"))
    if not cache_events:
        cache_events = _records(profiling.get("cache_events"))
    if not cache_events:
        cache_events = _records(_mapping(profiling.get("cache_timing")).get("cache_events"))
    candidate_hotspots = build_weight_calibration_candidate_hotspot_table(
        diagnostics_payload,
        top_n=top_n,
        generated_at=generated,
    )
    worker_timing = _mapping(profiling.get("worker_timing")) or build_worker_timing_breakdown(
        _records(_mapping(candidate_hotspots).get("hotspots"))
        or _profiling_records(diagnostics_payload, "candidate_timings")
    )
    cache_timing = build_cache_timing_breakdown(
        cache_events,
        slow_entry_seconds=policy.weight_calibration_profiling.thresholds.slow_cache_entry_seconds,
    )
    vectorization_audit = build_vectorization_audit_report(diagnostics_payload)
    regime_assessment = build_regime_mask_precomputation_assessment(
        diagnostics_payload,
        policy=policy,
    )
    recommendations = build_optimization_recommendations(
        step_summary=step_summary,
        cache_timing=cache_timing,
        worker_timing=worker_timing,
        vectorization_audit=vectorization_audit,
        regime_assessment=regime_assessment,
    )
    payload = {
        "schema_version": WEIGHT_CALIBRATION_PROFILING_REPORT_SCHEMA_VERSION,
        "report_type": "etf_weight_calibration_profiling_report",
        "run_id": _run_id(diagnostics_payload),
        "generated_at": generated.isoformat(),
        "profile_mode": mode,
        "policy_version": policy.policy_metadata.version,
        "market_regime": diagnostics_payload.get("market_regime"),
        "data_quality_status": diagnostics_payload.get("data_quality_status"),
        "total_runtime_seconds": _float(
            _mapping(diagnostics_payload.get("performance_report")).get(
                "total_runtime_seconds"
            )
        )
        or _float(profiling.get("total_runtime_seconds")),
        "step_timing": step_summary,
        "candidate_hotspots": candidate_hotspots,
        "cache_timing_breakdown": cache_timing,
        "worker_timing_breakdown": worker_timing,
        "cprofile_summary": {
            key: str(value) for key, value in dict(cprofile_artifacts or {}).items()
        },
        "vectorization_audit": vectorization_audit,
        "regime_mask_precomputation_assessment": regime_assessment,
        "optimization_recommendations": recommendations,
        "next_step_recommendation": _next_step_recommendation(recommendations),
        "observe_only": True,
        "candidate_only": True,
        "production_effect": "none",
        "broker_action": "none",
        "manual_review_required": True,
        "safety": dict(WEIGHT_CALIBRATION_PROFILING_SAFETY),
    }
    validate_weight_calibration_profiling_report(payload)
    return payload


def validate_weight_calibration_profiling_report(payload: Mapping[str, Any]) -> None:
    issues = []
    if payload.get("schema_version") != WEIGHT_CALIBRATION_PROFILING_REPORT_SCHEMA_VERSION:
        issues.append("schema_version")
    if payload.get("report_type") != "etf_weight_calibration_profiling_report":
        issues.append("report_type")
    for section in (
        "step_timing",
        "candidate_hotspots",
        "cache_timing_breakdown",
        "worker_timing_breakdown",
        "vectorization_audit",
        "regime_mask_precomputation_assessment",
        "optimization_recommendations",
    ):
        if section not in payload:
            issues.append(section)
    _extend_safety_issues(issues, payload)
    forbidden_keys = {
        "production_weight_update",
        "broker_order",
        "auto_candidate_promotion",
        "baseline_config_mutation",
        "native_kernel_rewrite",
    }
    if _payload_contains_forbidden_key(payload, forbidden_keys):
        issues.append("forbidden_output_key")
    if issues:
        raise WeightCalibrationProfilingError(
            "weight calibration profiling report validation failed: " + ", ".join(issues)
        )


def write_weight_calibration_profiling_report(
    payload: Mapping[str, Any],
    *,
    output_dir: Path | str,
) -> dict[str, Path]:
    validate_weight_calibration_profiling_report(payload)
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    json_path = output_path / "profiling_report.json"
    markdown_path = output_path / "profiling_report.md"
    _write_json(json_path, payload)
    markdown_path.write_text(
        render_weight_calibration_profiling_report_markdown(payload),
        encoding="utf-8",
    )
    return {"json": json_path, "markdown": markdown_path}


def render_weight_calibration_profiling_report_markdown(payload: Mapping[str, Any]) -> str:
    lines = [
        "# ETF Weight Calibration Profiling Report",
        "",
        "## Safety Banner",
        "",
        "- observe_only = true",
        "- candidate_only = true",
        "- production_effect = none",
        "- broker_action = none",
        "- manual_review_required = true",
        "",
        "## Summary",
        "",
        f"- Run ID: {payload.get('run_id')}",
        f"- Profile Mode: {payload.get('profile_mode')}",
        f"- Market Regime: {payload.get('market_regime')}",
        f"- Data Quality Status: {payload.get('data_quality_status')}",
        f"- Total Runtime Seconds: {payload.get('total_runtime_seconds')}",
        f"- Next Step: {payload.get('next_step_recommendation')}",
        "",
        "## Step-Level Timing",
        "",
        "| Step | Seconds | Status | Slow |",
        "|---|---:|---|---|",
    ]
    for row in _records(_mapping(payload.get("step_timing")).get("slowest_steps"))[:10]:
        lines.append(
            f"| {row.get('step_id')} | {_format_seconds(row.get('duration_seconds'))} | "
            f"{row.get('status')} | {row.get('warning_if_slow')} |"
        )
    lines.extend(["", "## Candidate Hotspots", ""])
    for row in _records(_mapping(payload.get("candidate_hotspots")).get("hotspots"))[:10]:
        lines.append(
            "- "
            f"{row.get('candidate_id')} / {row.get('search_id')} / {row.get('preset_id')}: "
            f"{_format_seconds(row.get('total_candidate_seconds'))}s, "
            f"cache={row.get('cache_status')}, worker={row.get('worker_id')}"
        )
    lines.extend(["", "## Optimization Recommendations", ""])
    for row in _records(payload.get("optimization_recommendations")):
        lines.append(
            f"- {row.get('category')}: {row.get('recommended_action')} "
            f"(priority={row.get('priority')}, native_extension_needed=false)"
        )
    return "\n".join(lines) + "\n"


def build_optimization_recommendations(
    *,
    step_summary: Mapping[str, Any],
    cache_timing: Mapping[str, Any],
    worker_timing: Mapping[str, Any],
    vectorization_audit: Mapping[str, Any],
    regime_assessment: Mapping[str, Any],
) -> list[dict[str, Any]]:
    recommendations = []
    slowest = _records(step_summary.get("slowest_steps"))
    if slowest:
        step = slowest[0]
        recommendations.append(
            _recommendation(
                "profile_slowest_step",
                f"slowest_step={step.get('step_id')} seconds={step.get('duration_seconds')}",
                "inspect_step_hotspot_before_rewrite",
                "high",
            )
        )
    for area in _records(vectorization_audit.get("areas")):
        if area.get("optimization_candidate") is True:
            recommendations.append(
                _recommendation(
                    _text(area.get("area")),
                    _text(area.get("current_implementation_hint")),
                    _text(area.get("recommended_action")),
                    _text(area.get("priority"), "medium"),
                )
            )
    if any(
        _records(row.get("slow_cache_entries"))
        for row in _records(cache_timing.get("cache_layers"))
    ):
        recommendations.append(
            _recommendation(
                "reduce_serialization_overhead",
                "slow cache read/write entry detected",
                "measure_json_payload_size_before_format_change",
                "medium",
            )
        )
    worker_rows = _records(worker_timing.get("workers"))
    if worker_rows:
        max_runtime = max(_float(row.get("runtime_seconds")) for row in worker_rows)
        min_runtime = min(_float(row.get("runtime_seconds")) for row in worker_rows)
        if min_runtime > 0 and max_runtime / min_runtime > 1.5:
            recommendations.append(
                _recommendation(
                    "tune_worker_count",
                    f"worker_runtime_ratio={round(max_runtime / min_runtime, 6)}",
                    "review_candidate_batching_or_worker_count",
                    "medium",
                )
            )
    if any(
        row.get("recommend_precompute") is True
        for row in _records(regime_assessment.get("regimes"))
    ):
        recommendations.append(
            _recommendation(
                "precompute_regime_masks",
                "regime mask reuse count exceeds policy floor",
                "precompute_regime_masks",
                "medium",
            )
        )
    recommendations.append(
        _recommendation(
            "native_extension_not_recommended",
            "profiling stage has not proven a native extension maintenance payoff",
            "stay_with_python_numpy_precompute_first",
            "low",
        )
    )
    return recommendations


def build_weight_calibration_profiling_validation_report(
    *,
    policy_config_path: Path | str = DEFAULT_WEIGHT_CALIBRATION_PROFILING_POLICY_CONFIG_PATH,
    report_registry_path: Path | str = PROJECT_ROOT / "config" / "report_registry.yaml",
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    checks: list[dict[str, Any]] = []
    policy: WeightCalibrationProfilingPolicyConfig | None = None
    try:
        policy = load_weight_calibration_profiling_policy_config(policy_config_path)
        _append_check(checks, "profiling_policy_valid", True, "Profiling policy loads.")
    except WeightCalibrationProfilingError as exc:
        _append_check(checks, "profiling_policy_valid", False, str(exc))
    if policy is not None:
        modes = set(policy.weight_calibration_profiling.modes)
        _append_check(
            checks,
            "profiling_modes_available",
            {"off", "summary", "detailed", "cprofile"}.issubset(modes),
            "Required profiling modes are present.",
            {"modes": sorted(modes)},
        )
        _append_check(
            checks,
            "profiling_safety_boundary",
            _policy_safety_is_safe(policy),
            "Profiling policy safety boundary is observe-only and no-broker.",
        )
    sample_policy = policy or _sample_policy()
    profiler = WeightCalibrationRuntimeProfiler(mode="summary", policy=sample_policy)
    profiler.record_step("sample", duration_seconds=0.001)
    _append_check(
        checks,
        "step_profiler_available",
        bool(profiler.records()),
        "Step profiler records deterministic timing payloads.",
    )
    sample_payload = _sample_diagnostics_payload()
    _append_check(
        checks,
        "candidate_hotspot_table_available",
        bool(build_weight_calibration_candidate_hotspot_table(sample_payload)["hotspots"]),
        "Candidate hotspot table can be generated.",
    )
    _append_check(
        checks,
        "cprofile_artifact_mode_available",
        sample_policy.weight_calibration_profiling.modes["cprofile"].cprofile,
        "cProfile mode is available but optional.",
    )
    _append_check(
        checks,
        "cache_timing_breakdown_available",
        bool(build_cache_timing_breakdown(_sample_cache_events())["cache_layers"]),
        "Cache timing breakdown can be generated.",
    )
    _append_check(
        checks,
        "worker_timing_breakdown_available",
        bool(build_worker_timing_breakdown(_sample_candidate_timings())["workers"]),
        "Worker timing breakdown can be generated.",
    )
    _append_check(
        checks,
        "vectorization_audit_available",
        bool(build_vectorization_audit_report(sample_payload)["areas"]),
        "Vectorization audit can be generated.",
    )
    _append_check(
        checks,
        "regime_mask_assessment_available",
        bool(build_regime_mask_precomputation_assessment(sample_payload)["regimes"]),
        "Regime mask precomputation assessment can be generated.",
    )
    try:
        report = build_weight_calibration_profiling_report(
            sample_payload,
            policy=sample_policy,
            profile_mode="detailed",
        )
        report_available = report["production_effect"] == "none"
    except WeightCalibrationProfilingError:
        report_available = False
    _append_check(
        checks,
        "profiling_report_generator_available",
        report_available,
        "Profiling report generator is available and safe.",
    )
    _append_check(
        checks,
        "reader_brief_integration_available",
        _report_registry_has_profiling(report_registry_path),
        "Report registry exposes profiling report for Reader Brief.",
        {"report_registry_path": str(report_registry_path)},
    )
    failed = [check for check in checks if check["status"] != "PASS"]
    payload = {
        "schema_version": WEIGHT_CALIBRATION_PROFILING_VALIDATION_SCHEMA_VERSION,
        "report_type": "etf_weight_calibration_profiling_validation",
        "generated_at": generated.isoformat(),
        "status": "PASS" if not failed else "FAIL",
        "failed_check_count": len(failed),
        "checks": checks,
        "policy_config_path": str(policy_config_path),
        "report_registry_path": str(report_registry_path),
        "safety": dict(WEIGHT_CALIBRATION_PROFILING_SAFETY),
        **WEIGHT_CALIBRATION_PROFILING_SAFETY,
    }
    validate_weight_calibration_profiling_validation_report(payload)
    return payload


def validate_weight_calibration_profiling_validation_report(
    payload: Mapping[str, Any],
) -> None:
    issues = []
    if payload.get("schema_version") != WEIGHT_CALIBRATION_PROFILING_VALIDATION_SCHEMA_VERSION:
        issues.append("schema_version")
    if payload.get("report_type") != "etf_weight_calibration_profiling_validation":
        issues.append("report_type")
    _extend_safety_issues(issues, payload)
    failed = [check for check in _records(payload.get("checks")) if check.get("status") != "PASS"]
    if int(payload.get("failed_check_count") or 0) != len(failed):
        issues.append("failed_check_count")
    if payload.get("status") == "PASS" and failed:
        issues.append("status")
    if issues:
        raise WeightCalibrationProfilingError(
            "weight calibration profiling validation report failed: " + ", ".join(issues)
        )


def write_weight_calibration_profiling_validation_report(
    payload: Mapping[str, Any],
    *,
    output_dir: Path | str = DEFAULT_WEIGHT_CALIBRATION_PROFILING_VALIDATION_DIR,
) -> dict[str, Path]:
    validate_weight_calibration_profiling_validation_report(payload)
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    timestamp = _artifact_timestamp(_text(payload.get("generated_at")))
    json_path = output_path / f"weight_calibration_profiling_validation_{timestamp}.json"
    markdown_path = output_path / f"weight_calibration_profiling_validation_{timestamp}.md"
    _write_json(json_path, payload)
    markdown_path.write_text(
        render_weight_calibration_profiling_validation_markdown(payload),
        encoding="utf-8",
    )
    return {"json": json_path, "markdown": markdown_path}


def render_weight_calibration_profiling_validation_markdown(
    payload: Mapping[str, Any],
) -> str:
    lines = [
        "# ETF Weight Calibration Profiling Validation",
        "",
        "## Safety Banner",
        "",
        "- observe_only = true",
        "- candidate_only = true",
        "- production_effect = none",
        "- broker_action = none",
        "- manual_review_required = true",
        "",
        "## Summary",
        "",
        f"- Status: {payload.get('status')}",
        f"- Failed Check Count: {payload.get('failed_check_count')}",
        "",
        "## Checks",
        "",
        "| Check | Status | Message |",
        "|---|---|---|",
    ]
    for check in _records(payload.get("checks")):
        lines.append(
            f"| {check.get('check_id')} | {check.get('status')} | {check.get('message')} |"
        )
    return "\n".join(lines) + "\n"


def _candidate_hotspot_row(row: Mapping[str, Any]) -> dict[str, Any]:
    weights = _mapping(row.get("weights"))
    weights_hash = _text(row.get("weights_hash")) or _hash_payload(weights)
    cache_status = _text(
        row.get("cache_status"),
        _combined_cache_status(
            _text(row.get("candidate_backtest_cache_status")),
            _text(row.get("regime_robustness_cache_status")),
        ),
    )
    return {
        "candidate_id": _text(row.get("candidate_id"), _text(row.get("source_candidate_id"))),
        "search_id": _text(row.get("search_id")),
        "preset_id": _text(row.get("preset_id")),
        "weights_hash": weights_hash,
        "backtest_seconds": round(_float(row.get("backtest_seconds")), 6),
        "regime_seconds": round(_float(row.get("regime_seconds")), 6),
        "overfit_seconds": round(_float(row.get("overfit_seconds")), 6),
        "cache_read_seconds": round(_float(row.get("cache_read_seconds")), 6),
        "cache_write_seconds": round(_float(row.get("cache_write_seconds")), 6),
        "serialization_seconds": round(_float(row.get("serialization_seconds")), 6),
        "total_candidate_seconds": round(
            _float(row.get("total_candidate_seconds"))
            or (
                _float(row.get("backtest_seconds"))
                + _float(row.get("regime_seconds"))
                + _float(row.get("overfit_seconds"))
                + _float(row.get("cache_read_seconds"))
                + _float(row.get("cache_write_seconds"))
            ),
            6,
        ),
        "cache_status": cache_status,
        "worker_id": _text(row.get("worker_id"), "main"),
        "rank": row.get("rank"),
        "readiness_status": _text(
            row.get("readiness_status"),
            _text(row.get("forward_readiness_status")),
        ),
        "overfit_risk": _text(row.get("overfit_risk")),
        "status": _text(row.get("status"), "completed"),
        "safety": dict(WEIGHT_CALIBRATION_PROFILING_SAFETY),
        **WEIGHT_CALIBRATION_PROFILING_SAFETY,
    }


def _audit_area(
    area: str,
    hint: str,
    observed_runtime: float | None,
    optimization_candidate: bool,
    recommended_action: str,
    priority: str,
) -> dict[str, Any]:
    return {
        "area": area,
        "current_implementation_hint": hint,
        "observed_runtime_if_available": (
            None if observed_runtime is None else round(float(observed_runtime), 6)
        ),
        "optimization_candidate": optimization_candidate,
        "recommended_action": recommended_action,
        "priority": priority,
        "native_extension_needed": False,
    }


def _recommendation(
    category: str,
    evidence: str,
    recommended_action: str,
    priority: str,
) -> dict[str, Any]:
    return {
        "category": category,
        "evidence": evidence,
        "recommended_action": recommended_action,
        "priority": priority,
        "native_extension_needed": False,
    }


def _next_step_recommendation(recommendations: Sequence[Mapping[str, Any]]) -> str:
    high = [row for row in recommendations if _text(row.get("priority")) == "high"]
    if high:
        return _text(high[0].get("recommended_action"), "review_profile_before_optimization")
    return "profile_cold_run_before_numerical_optimization"


def _profiling_records(payload: Mapping[str, Any], key: str) -> list[dict[str, Any]]:
    return _records(_mapping(payload.get("profiling")).get(key))


def _observed_step_seconds(payload: Mapping[str, Any]) -> dict[str, float]:
    observed = {
        _text(row.get("step_id")): _float(row.get("duration_seconds"))
        for row in _profiling_records(payload, "step_timings")
    }
    performance = _mapping(payload.get("performance_report"))
    observed.update(
        {
            _text(key): _float(value)
            for key, value in _mapping(performance.get("step_runtime_seconds")).items()
        }
    )
    return observed


def _regime_rows_from_diagnostics(payload: Mapping[str, Any]) -> list[dict[str, Any]]:
    candidate_count = int(_float(payload.get("candidate_observation_count")))
    rows = []
    regime_ids = set()
    for result in _records(payload.get("preset_results")):
        failures = _mapping(result.get("regime_failure_summary"))
        for regime_id in failures:
            regime_ids.add(_text(regime_id))
    if not regime_ids:
        regime_ids.add("price_derived_regime_proxy_v1")
    for regime_id in sorted(regime_ids):
        rows.append(
            {
                "regime_id": regime_id,
                "date_range": {},
                "mask_build_seconds": 0.0,
                "mask_reuse_count": candidate_count,
                "candidate_count": candidate_count,
            }
        )
    return rows


def _cprofile_top_rows(
    profiler: cProfile.Profile,
    *,
    top_n: int,
) -> list[dict[str, Any]]:
    stats = pstats.Stats(profiler).strip_dirs().sort_stats("cumulative")
    rows = []
    for filename, line_number, function_name in (stats.fcn_list or [])[:top_n]:
        call_count, primitive_call_count, total_time, cumulative_time, _ = stats.stats[
            (filename, line_number, function_name)
        ]
        rows.append(
            {
                "function_name": function_name,
                "file_path": filename,
                "line_number": line_number,
                "call_count": call_count,
                "primitive_call_count": primitive_call_count,
                "total_time": round(total_time, 6),
                "cumulative_time": round(cumulative_time, 6),
                "per_call_time": round(total_time / call_count, 6) if call_count else None,
            }
        )
    return rows


def _render_cprofile_markdown(payload: Mapping[str, Any]) -> str:
    lines = [
        "# ETF Weight Calibration cProfile Top Functions",
        "",
        "| Function | File | Line | Calls | Total | Cumulative | Per Call |",
        "|---|---|---:|---:|---:|---:|---:|",
    ]
    for row in _records(payload.get("functions")):
        lines.append(
            f"| {row.get('function_name')} | {row.get('file_path')} | "
            f"{row.get('line_number')} | {row.get('call_count')} | "
            f"{row.get('total_time')} | {row.get('cumulative_time')} | "
            f"{row.get('per_call_time')} |"
        )
    return "\n".join(lines) + "\n"


def _sample_policy() -> WeightCalibrationProfilingPolicyConfig:
    return load_weight_calibration_profiling_policy_config()


def _sample_diagnostics_payload() -> dict[str, Any]:
    return {
        "report_type": "etf_weight_search_diagnostics",
        "run_manifest": {"run_id": "profiling-validation-sample"},
        "market_regime": "ai_after_chatgpt",
        "data_quality_status": "PASS",
        "candidate_observation_count": 2,
        "profiling": {
            "total_runtime_seconds": 3.0,
            "step_timings": [
                {
                    "step_id": "candidate_backtest",
                    "duration_seconds": 2.0,
                    "status": "completed",
                },
                {
                    "step_id": "regime_robustness",
                    "duration_seconds": 1.0,
                    "status": "completed",
                },
            ],
            "candidate_timings": _sample_candidate_timings(),
            "regime_mask_timings": [
                {
                    "regime_id": "risk_on",
                    "date_range": {"start": "2022-12-01", "end": "2026-06-03"},
                    "mask_build_seconds": 0.2,
                    "mask_reuse_count": 2,
                    "candidate_count": 2,
                }
            ],
        },
        "cache_summary": {"cache_events": _sample_cache_events()},
        "safety": dict(WEIGHT_CALIBRATION_PROFILING_SAFETY),
        **WEIGHT_CALIBRATION_PROFILING_SAFETY,
    }


def _sample_candidate_timings() -> list[dict[str, Any]]:
    return [
        {
            "candidate_id": "weight_set_0001",
            "search_id": "etf_initial_weight_search_v1",
            "preset_id": "ai_cycle_recent",
            "weights_hash": "abc",
            "backtest_seconds": 1.2,
            "regime_seconds": 0.4,
            "overfit_seconds": 0.1,
            "total_candidate_seconds": 1.7,
            "cache_status": "miss_written",
            "worker_id": "worker-1",
            "rank": 1,
            "readiness_status": "shadow_ready",
            "overfit_risk": "low",
            "status": "completed",
        },
        {
            "candidate_id": "weight_set_0002",
            "search_id": "etf_initial_weight_search_v1",
            "preset_id": "ai_cycle_recent",
            "weights_hash": "def",
            "backtest_seconds": 0.0,
            "regime_seconds": 0.0,
            "overfit_seconds": 0.1,
            "total_candidate_seconds": 0.1,
            "cache_status": "hit",
            "worker_id": "main",
            "rank": 2,
            "readiness_status": "needs_manual_review",
            "overfit_risk": "medium",
            "status": "completed",
        },
    ]


def _sample_cache_events() -> list[dict[str, Any]]:
    return [
        {
            "cache_layer": "candidate_backtest",
            "cache_status": "miss",
            "duration_seconds": 0.01,
        },
        {
            "cache_layer": "candidate_backtest",
            "cache_status": "write",
            "duration_seconds": 0.02,
            "serialization_seconds": 0.02,
        },
        {
            "cache_layer": "regime_robustness",
            "cache_status": "hit",
            "duration_seconds": 0.01,
        },
    ]


def _report_registry_has_profiling(path: Path | str) -> bool:
    try:
        raw = safe_load_yaml_path(Path(path))
    except OSError:
        return False
    if not isinstance(raw, Mapping):
        return False
    for report in _records(raw.get("reports")):
        if _text(report.get("report_id")) != "etf_weight_calibration_profiling_report":
            continue
        return (
            report.get("include_in_reader_brief") is True
            and _text(report.get("production_effect"), "none") == "none"
        )
    return False


def _policy_safety_is_safe(policy: WeightCalibrationProfilingPolicyConfig) -> bool:
    safety = policy.weight_calibration_profiling.safety.model_dump(mode="json")
    return all(
        safety.get(field) == expected
        for field, expected in WEIGHT_CALIBRATION_PROFILING_SAFETY.items()
    )


def _append_check(
    checks: list[dict[str, Any]],
    check_id: str,
    passed: bool,
    message: str,
    details: Mapping[str, Any] | None = None,
) -> None:
    checks.append(
        {
            "check_id": check_id,
            "status": "PASS" if passed else "FAIL",
            "message": message,
            "details": dict(details or {}),
        }
    )


def _extend_safety_issues(issues: list[str], payload: Mapping[str, Any]) -> None:
    for field, expected in WEIGHT_CALIBRATION_PROFILING_SAFETY.items():
        if payload.get(field) != expected:
            issues.append(field)
        if _mapping(payload.get("safety")).get(field) != expected:
            issues.append(f"safety.{field}")


def _payload_contains_forbidden_key(value: Any, forbidden_keys: set[str]) -> bool:
    if isinstance(value, Mapping):
        for key, item in value.items():
            if str(key) in forbidden_keys:
                return True
            if _payload_contains_forbidden_key(item, forbidden_keys):
                return True
    elif isinstance(value, list):
        return any(_payload_contains_forbidden_key(item, forbidden_keys) for item in value)
    return False


def _run_id(payload: Mapping[str, Any]) -> str:
    return _text(
        payload.get("run_id"),
        _text(_mapping(payload.get("run_manifest")).get("run_id"), "unknown_run"),
    )


def _combined_cache_status(backtest_status: str, regime_status: str) -> str:
    statuses = [status for status in (backtest_status, regime_status) if status]
    if not statuses:
        return "not_reported"
    if all(status == "hit" for status in statuses):
        return "hit"
    if any("miss" in status for status in statuses):
        return "miss"
    return "+".join(statuses)


def _count_status(events: Sequence[Mapping[str, Any]], status: str) -> int:
    return sum(1 for event in events if _text(event.get("cache_status")) == status)


def _event_seconds(event: Mapping[str, Any]) -> float:
    return _float(event.get("duration_seconds")) or _float(event.get("read_seconds")) or _float(
        event.get("write_seconds")
    )


def _records(value: Any) -> list[dict[str, Any]]:
    if isinstance(value, list):
        return [dict(item) for item in value if isinstance(item, Mapping)]
    return []


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _text(value: Any, default: str = "") -> str:
    if value is None:
        return default
    text = str(value)
    return text if text else default


def _float(value: Any) -> float:
    try:
        if value is None or value == "":
            return 0.0
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _format_seconds(value: Any) -> str:
    return f"{_float(value):.6f}"


def _hash_payload(value: Any) -> str:
    return sha256(json.dumps(value, sort_keys=True, default=str).encode("utf-8")).hexdigest()


def _artifact_timestamp(value: str) -> str:
    return (
        value.replace("-", "")
        .replace(":", "")
        .replace("+", "")
        .replace(".", "")
        .replace("T", "T")[:15]
    )


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _write_csv(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = sorted({str(key) for row in rows for key in row})
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(
                {
                    key: json.dumps(value, ensure_ascii=False, sort_keys=True)
                    if isinstance(value, (dict, list))
                    else value
                    for key, value in row.items()
                }
            )
