from __future__ import annotations

import json
import os
from collections.abc import Callable, Mapping, Sequence
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import UTC, date, datetime
from hashlib import sha256
from pathlib import Path
from time import perf_counter
from typing import Any, Literal, Self

import pandas as pd
from pydantic import BaseModel, Field, model_validator

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.etf_portfolio.models import PolicyMetadata
from ai_trading_system.yaml_loader import safe_load_yaml_path

DEFAULT_WEIGHT_CALIBRATION_CACHE_POLICY_CONFIG_PATH = (
    PROJECT_ROOT / "config" / "etf_portfolio" / "cache_policy.yaml"
)
DEFAULT_WEIGHT_CALIBRATION_CACHE_ROOT = PROJECT_ROOT / "data" / "cache" / "weight_calibration"
DEFAULT_WEIGHT_CALIBRATION_PERFORMANCE_REPORT_DIR = (
    PROJECT_ROOT / "reports" / "etf_portfolio" / "weight_calibration" / "performance"
)
DEFAULT_WEIGHT_CALIBRATION_CACHE_VALIDATION_DIR = (
    PROJECT_ROOT / "reports" / "etf_portfolio" / "weight_calibration" / "validation"
)

WEIGHT_CALIBRATION_CACHE_POLICY_SCHEMA_VERSION = "etf_weight_calibration_cache_policy_v1"
WEIGHT_CALIBRATION_CACHE_MANIFEST_SCHEMA_VERSION = "etf_weight_calibration_cache_manifest_v1"
WEIGHT_CALIBRATION_PRICE_RETURNS_CACHE_SCHEMA_VERSION = (
    "etf_weight_calibration_price_returns_matrix_cache_v1"
)
WEIGHT_CALIBRATION_CANDIDATE_BACKTEST_CACHE_SCHEMA_VERSION = (
    "etf_weight_calibration_candidate_backtest_cache_v1"
)
WEIGHT_CALIBRATION_REGIME_ROBUSTNESS_CACHE_SCHEMA_VERSION = (
    "etf_weight_calibration_regime_robustness_cache_v1"
)
WEIGHT_CALIBRATION_DIAGNOSTICS_AGGREGATION_CACHE_SCHEMA_VERSION = (
    "etf_weight_calibration_diagnostics_aggregation_cache_v1"
)
WEIGHT_CALIBRATION_RUN_MANIFEST_SCHEMA_VERSION = (
    "etf_weight_calibration_diagnostics_run_manifest_v1"
)
WEIGHT_CALIBRATION_PERFORMANCE_REPORT_SCHEMA_VERSION = (
    "etf_weight_calibration_performance_report_v1"
)
WEIGHT_CALIBRATION_CACHE_VALIDATION_SCHEMA_VERSION = (
    "etf_weight_calibration_cache_parallel_validation_v1"
)

WEIGHT_CALIBRATION_CACHE_SAFETY = {
    "observe_only": True,
    "candidate_only": True,
    "production_effect": "none",
    "broker_action": "none",
    "manual_review_required": True,
}

CacheLayer = Literal[
    "price_returns_matrix",
    "candidate_universe",
    "candidate_backtest",
    "regime_robustness",
    "diagnostics_aggregation",
]
CacheMode = Literal["read_write", "read_only", "disabled"]
RunManifestStatus = Literal["running", "completed", "failed", "interrupted", "partial", "resumed"]

WEIGHT_CALIBRATION_CACHE_LAYERS: tuple[CacheLayer, ...] = (
    "price_returns_matrix",
    "candidate_universe",
    "candidate_backtest",
    "regime_robustness",
    "diagnostics_aggregation",
)
WEIGHT_CALIBRATION_COMMON_CACHE_KEY_INPUTS = (
    "source_config_hash",
    "data_hash",
    "engine_version",
)
WEIGHT_CALIBRATION_LAYER_CACHE_KEY_INPUTS: dict[str, tuple[str, ...]] = {
    "price_returns_matrix": (
        "asset_universe",
        "start_date",
        "end_date",
        "data_source",
        "adjusted_price_mode",
        "trading_calendar_hash",
    ),
    "candidate_universe": (
        "search_id",
        "weight_constraints_hash",
        "sleeve_constraints_hash",
        "grid_step",
        "asset_universe_hash",
    ),
    "candidate_backtest": (
        "candidate_weights_hash",
        "date_range",
        "returns_matrix_hash",
        "backtest_engine_version",
        "transaction_cost_config_hash",
        "rebalance_policy_hash",
        "benchmark_set_hash",
    ),
    "regime_robustness": (
        "candidate_weights_hash",
        "date_range",
        "regime_definition_hash",
        "returns_matrix_hash",
        "metrics_schema_version",
    ),
    "diagnostics_aggregation": (
        "diagnostics_config_hash",
        "input_search_run_ids",
        "input_candidate_result_hashes",
        "aggregation_engine_version",
    ),
}


class WeightCalibrationCacheError(ValueError):
    """Raised when weight-calibration cache policy, keys, or manifests are invalid."""


class WeightCalibrationCacheSafety(BaseModel):
    observe_only: Literal[True]
    candidate_only: Literal[True]
    production_effect: Literal["none"]
    broker_action: Literal["none"]
    manual_review_required: Literal[True]


class WeightCalibrationEnabledLayers(BaseModel):
    price_returns_matrix: bool
    candidate_universe: bool
    candidate_backtest: bool
    regime_robustness: bool
    diagnostics_aggregation: bool


class WeightCalibrationCacheModes(BaseModel):
    allowed: list[CacheMode] = Field(min_length=1)

    @model_validator(mode="after")
    def validate_unique_modes(self) -> Self:
        if len(self.allowed) != len(set(self.allowed)):
            raise ValueError("cache modes must be unique")
        return self


class WeightCalibrationCacheTTLPolicy(BaseModel):
    price_returns_matrix_days: int = Field(ge=0)
    candidate_universe_days: int = Field(ge=0)
    candidate_backtest_days: int = Field(ge=0)
    regime_robustness_days: int = Field(ge=0)
    diagnostics_aggregation_days: int = Field(ge=0)


class WeightCalibrationParallelPolicy(BaseModel):
    default_workers: int | Literal["auto"]
    max_workers: int = Field(ge=1, le=64)

    @model_validator(mode="after")
    def validate_worker_bounds(self) -> Self:
        if isinstance(self.default_workers, int):
            if self.default_workers < 1:
                raise ValueError("default_workers must be positive")
            if self.default_workers > self.max_workers:
                raise ValueError("default_workers must be <= max_workers")
        return self


class WeightCalibrationLockingPolicy(BaseModel):
    enabled: bool
    lock_timeout_seconds: int = Field(ge=1)


class WeightCalibrationPruningPolicy(BaseModel):
    enabled: bool
    older_than_days: int = Field(ge=0)


class WeightCalibrationCacheSettings(BaseModel):
    enabled: bool
    default_mode: CacheMode
    cache_root: str = Field(min_length=1)
    enabled_layers: WeightCalibrationEnabledLayers
    cache_modes: WeightCalibrationCacheModes
    ttl_policy: WeightCalibrationCacheTTLPolicy
    schema_versions: dict[str, str] = Field(min_length=1)
    parallel: WeightCalibrationParallelPolicy
    locking: WeightCalibrationLockingPolicy
    pruning: WeightCalibrationPruningPolicy
    safety: WeightCalibrationCacheSafety

    @model_validator(mode="after")
    def validate_cache_settings(self) -> Self:
        if self.default_mode not in self.cache_modes.allowed:
            raise ValueError("default_mode must be listed in cache_modes.allowed")
        missing_versions = {
            "cache_manifest",
            "price_returns_matrix",
            "candidate_universe",
            "candidate_backtest",
            "regime_robustness",
            "diagnostics_aggregation",
            "run_manifest",
            "performance_report",
        } - set(self.schema_versions)
        if missing_versions:
            raise ValueError(
                "cache schema_versions missing: " + ", ".join(sorted(missing_versions))
            )
        if (
            self.schema_versions["cache_manifest"]
            != WEIGHT_CALIBRATION_CACHE_MANIFEST_SCHEMA_VERSION
        ):
            raise ValueError("cache_manifest schema version is not supported")
        return self


class WeightCalibrationCachePolicyConfig(BaseModel):
    schema_version: Literal["etf_weight_calibration_cache_policy_v1"]
    policy_metadata: PolicyMetadata
    weight_calibration_cache: WeightCalibrationCacheSettings


class WeightCalibrationCacheManifest(BaseModel):
    cache_key: str = Field(min_length=1)
    cache_layer: CacheLayer
    created_at: datetime
    last_accessed_at: datetime
    schema_version: str = Field(min_length=1)
    source_config_hash: str = Field(min_length=1)
    data_hash: str = Field(min_length=1)
    model_version: str = Field(min_length=1)
    engine_version: str = Field(min_length=1)
    input_summary: dict[str, Any] = Field(default_factory=dict)
    output_summary: dict[str, Any] = Field(default_factory=dict)
    artifact_paths: dict[str, str] = Field(default_factory=dict)
    safety: WeightCalibrationCacheSafety

    @model_validator(mode="after")
    def validate_manifest_schema(self) -> Self:
        if self.schema_version != WEIGHT_CALIBRATION_CACHE_MANIFEST_SCHEMA_VERSION:
            raise ValueError("cache manifest schema_version mismatch")
        if self.last_accessed_at < self.created_at:
            raise ValueError("last_accessed_at must be >= created_at")
        return self


class WeightCalibrationDiagnosticsRunManifest(BaseModel):
    schema_version: Literal["etf_weight_calibration_diagnostics_run_manifest_v1"]
    run_id: str = Field(min_length=1)
    started_at: datetime
    completed_at: datetime | None = None
    status: RunManifestStatus
    config_hash: str = Field(min_length=1)
    data_hash: str = Field(min_length=1)
    planned_steps: list[str] = Field(default_factory=list)
    completed_steps: list[str] = Field(default_factory=list)
    failed_steps: list[str] = Field(default_factory=list)
    candidate_statuses: dict[str, str] = Field(default_factory=dict)
    cache_summary: dict[str, Any] = Field(default_factory=dict)
    output_artifacts: dict[str, str] = Field(default_factory=dict)
    safety: WeightCalibrationCacheSafety

    @model_validator(mode="after")
    def validate_status_transition(self) -> Self:
        if self.completed_at is not None and self.completed_at < self.started_at:
            raise ValueError("completed_at must be >= started_at")
        unknown_completed = set(self.completed_steps) - set(self.planned_steps)
        unknown_failed = set(self.failed_steps) - set(self.planned_steps)
        if unknown_completed or unknown_failed:
            raise ValueError("completed_steps and failed_steps must be planned")
        return self


def load_weight_calibration_cache_policy_config(
    path: Path | str = DEFAULT_WEIGHT_CALIBRATION_CACHE_POLICY_CONFIG_PATH,
) -> WeightCalibrationCachePolicyConfig:
    raw = safe_load_yaml_path(Path(path))
    if not isinstance(raw, dict):
        raise WeightCalibrationCacheError("weight calibration cache policy must be a mapping")
    try:
        return WeightCalibrationCachePolicyConfig.model_validate(raw)
    except ValueError as exc:
        raise WeightCalibrationCacheError(str(exc)) from exc


def normalize_weight_calibration_cache_mode(value: str | None) -> CacheMode:
    token = str(value or "disabled").strip().lower().replace("-", "_")
    aliases = {
        "readwrite": "read_write",
        "read_write": "read_write",
        "rw": "read_write",
        "read_only": "read_only",
        "readonly": "read_only",
        "ro": "read_only",
        "no_cache": "disabled",
        "none": "disabled",
        "off": "disabled",
        "disabled": "disabled",
    }
    try:
        return aliases[token]  # type: ignore[return-value]
    except KeyError as exc:
        raise WeightCalibrationCacheError(
            f"invalid weight calibration cache mode: {value}"
        ) from exc


def resolve_weight_calibration_worker_count(
    workers: str | int | None,
    *,
    policy: WeightCalibrationCachePolicyConfig | None = None,
    available_cpu_count: int | None = None,
) -> int:
    parallel = policy.weight_calibration_cache.parallel if policy else None
    max_workers = parallel.max_workers if parallel else 8
    selected: str | int | None = workers
    if selected is None and parallel is not None:
        selected = parallel.default_workers
    if selected is None:
        selected = 1
    if isinstance(selected, str):
        token = selected.strip().lower()
        if token == "auto":
            cpu_count = available_cpu_count if available_cpu_count is not None else os.cpu_count()
            return max(1, min(max_workers, int(cpu_count or 1)))
        if not token.isdigit():
            raise WeightCalibrationCacheError(f"invalid workers value: {selected}")
        selected_count = int(token)
    else:
        selected_count = int(selected)
    if selected_count < 1:
        raise WeightCalibrationCacheError("workers must be positive")
    if selected_count > max_workers:
        raise WeightCalibrationCacheError(f"workers must be <= max_workers ({max_workers})")
    return selected_count


def weight_calibration_input_hash(value: Any) -> str:
    return sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def weight_calibration_dataframe_hash(frame: pd.DataFrame) -> str:
    ordered = frame.copy()
    ordered = ordered.reindex(sorted(ordered.columns), axis=1)
    if not ordered.empty:
        ordered = ordered.sort_values(list(ordered.columns)).reset_index(drop=True)
    return weight_calibration_input_hash(
        {
            "columns": list(ordered.columns),
            "rows": [_jsonable(row) for row in ordered.to_dict("records")],
        }
    )


def build_weight_calibration_cache_key(
    cache_layer: str,
    inputs: Mapping[str, Any],
    *,
    schema_version: str | None = None,
) -> str:
    layer = _normalize_cache_layer(cache_layer)
    required = (
        *WEIGHT_CALIBRATION_COMMON_CACHE_KEY_INPUTS,
        *WEIGHT_CALIBRATION_LAYER_CACHE_KEY_INPUTS[layer],
    )
    missing = [field for field in required if field not in inputs or _blank(inputs[field])]
    if missing:
        raise WeightCalibrationCacheError(
            f"cache key inputs missing for {layer}: " + ", ".join(missing)
        )
    payload = {
        "cache_layer": layer,
        "schema_version": schema_version or "v1",
        "inputs": {key: _jsonable(inputs[key]) for key in sorted(inputs)},
    }
    digest = sha256(_canonical_json(payload).encode("utf-8")).hexdigest()
    return f"{layer.replace('_', '-')}-{digest}"


def build_weight_calibration_cache_manifest(
    *,
    cache_key: str,
    cache_layer: str,
    source_config_hash: str,
    data_hash: str,
    model_version: str,
    engine_version: str,
    input_summary: Mapping[str, Any] | None = None,
    output_summary: Mapping[str, Any] | None = None,
    artifact_paths: Mapping[str, str] | None = None,
    created_at: datetime | None = None,
    last_accessed_at: datetime | None = None,
) -> dict[str, Any]:
    created = created_at or datetime.now(UTC)
    accessed = last_accessed_at or created
    manifest = WeightCalibrationCacheManifest(
        cache_key=cache_key,
        cache_layer=_normalize_cache_layer(cache_layer),
        created_at=created,
        last_accessed_at=accessed,
        schema_version=WEIGHT_CALIBRATION_CACHE_MANIFEST_SCHEMA_VERSION,
        source_config_hash=source_config_hash,
        data_hash=data_hash,
        model_version=model_version,
        engine_version=engine_version,
        input_summary=dict(input_summary or {}),
        output_summary=dict(output_summary or {}),
        artifact_paths=dict(artifact_paths or {}),
        safety=WeightCalibrationCacheSafety.model_validate(WEIGHT_CALIBRATION_CACHE_SAFETY),
    )
    payload = manifest.model_dump(mode="json")
    payload.update(WEIGHT_CALIBRATION_CACHE_SAFETY)
    return payload


def validate_weight_calibration_cache_manifest(
    payload: Mapping[str, Any],
    *,
    expected_cache_key: str | None = None,
    expected_cache_layer: str | None = None,
    expected_source_config_hash: str | None = None,
    expected_data_hash: str | None = None,
    expected_engine_version: str | None = None,
) -> WeightCalibrationCacheManifest:
    try:
        manifest = WeightCalibrationCacheManifest.model_validate(payload)
    except ValueError as exc:
        raise WeightCalibrationCacheError(str(exc)) from exc
    issues = []
    if expected_cache_key is not None and manifest.cache_key != expected_cache_key:
        issues.append("cache_key")
    if expected_cache_layer is not None:
        expected_layer = _normalize_cache_layer(expected_cache_layer)
        if manifest.cache_layer != expected_layer:
            issues.append("cache_layer")
    if (
        expected_source_config_hash is not None
        and manifest.source_config_hash != expected_source_config_hash
    ):
        issues.append("source_config_hash")
    if expected_data_hash is not None and manifest.data_hash != expected_data_hash:
        issues.append("data_hash")
    if expected_engine_version is not None and manifest.engine_version != expected_engine_version:
        issues.append("engine_version")
    if issues:
        raise WeightCalibrationCacheError(
            "weight calibration cache manifest validation failed: " + ", ".join(issues)
        )
    return manifest


def write_weight_calibration_json_cache_entry(
    *,
    cache_root: Path | str,
    cache_layer: str,
    cache_key: str,
    payload: Mapping[str, Any],
    manifest: Mapping[str, Any],
) -> dict[str, Path]:
    layer = _normalize_cache_layer(cache_layer)
    validate_weight_calibration_cache_manifest(
        manifest,
        expected_cache_key=cache_key,
        expected_cache_layer=layer,
    )
    entry_dir = Path(cache_root) / layer / cache_key
    entry_dir.mkdir(parents=True, exist_ok=True)
    payload_path = entry_dir / "payload.json"
    manifest_path = entry_dir / "manifest.json"
    _atomic_write_json(payload_path, payload)
    _atomic_write_json(manifest_path, manifest)
    return {"entry_dir": entry_dir, "payload": payload_path, "manifest": manifest_path}


def load_weight_calibration_json_cache_entry(
    *,
    cache_root: Path | str,
    cache_layer: str,
    cache_key: str,
    expected_source_config_hash: str | None = None,
    expected_data_hash: str | None = None,
    expected_engine_version: str | None = None,
    fail_silently: bool = True,
) -> dict[str, Any] | None:
    layer = _normalize_cache_layer(cache_layer)
    entry_dir = Path(cache_root) / layer / cache_key
    manifest_path = entry_dir / "manifest.json"
    payload_path = entry_dir / "payload.json"
    try:
        manifest_payload = json.loads(manifest_path.read_text(encoding="utf-8"))
        validate_weight_calibration_cache_manifest(
            manifest_payload,
            expected_cache_key=cache_key,
            expected_cache_layer=layer,
            expected_source_config_hash=expected_source_config_hash,
            expected_data_hash=expected_data_hash,
            expected_engine_version=expected_engine_version,
        )
        payload = json.loads(payload_path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            raise WeightCalibrationCacheError("cache payload must be a JSON object")
        return {"payload": payload, "manifest": manifest_payload}
    except (OSError, json.JSONDecodeError, WeightCalibrationCacheError, ValueError):
        if fail_silently:
            return None
        raise


def build_price_returns_matrix_cache_payload(
    prices: pd.DataFrame,
    *,
    asset_universe: Sequence[str],
    start: date,
    end: date,
    data_source: str,
    adjusted_price_mode: str = "adj_close",
    source_config_hash: str = "not_applicable",
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    lookup = build_price_returns_matrix_cache_lookup(
        prices,
        asset_universe=asset_universe,
        start=start,
        end=end,
        data_source=data_source,
        adjusted_price_mode=adjusted_price_mode,
        source_config_hash=source_config_hash,
    )
    if start >= end:
        raise WeightCalibrationCacheError("price returns cache start must be before end")
    if not asset_universe:
        raise WeightCalibrationCacheError("asset_universe must not be empty")
    frame = prices.copy()
    if "date" not in frame.columns or "symbol" not in frame.columns:
        raise WeightCalibrationCacheError("prices must include date and symbol columns")
    price_column = _price_column_for_mode(frame, adjusted_price_mode)
    frame["_date"] = pd.to_datetime(frame["date"], errors="coerce")
    frame = frame.loc[
        (frame["_date"].dt.date >= start)
        & (frame["_date"].dt.date <= end)
        & frame["symbol"].isin(asset_universe)
    ].copy()
    if frame.empty:
        raise WeightCalibrationCacheError("no price rows available for requested cache window")
    price_matrix = (
        frame.pivot_table(index="_date", columns="symbol", values=price_column, aggfunc="last")
        .sort_index()
        .reindex(columns=list(asset_universe))
    )
    returns_matrix = price_matrix.pct_change(fill_method=None).fillna(0.0)
    generated = generated_at or datetime.now(UTC)
    payload = {
        "schema_version": WEIGHT_CALIBRATION_PRICE_RETURNS_CACHE_SCHEMA_VERSION,
        "cache_key": lookup["cache_key"],
        "cache_layer": "price_returns_matrix",
        "generated_at": generated.isoformat(),
        "asset_universe": list(asset_universe),
        "date_range": {"start": start.isoformat(), "end": end.isoformat()},
        "data_source": data_source,
        "adjusted_price_mode": adjusted_price_mode,
        "trading_calendar": lookup["trading_calendar"],
        "trading_calendar_hash": lookup["trading_calendar_hash"],
        "price_matrix": _matrix_rows(price_matrix),
        "returns_matrix": _matrix_rows(returns_matrix),
        "data_hash": lookup["data_hash"],
        "source_config_hash": source_config_hash,
        "safety": dict(WEIGHT_CALIBRATION_CACHE_SAFETY),
        **WEIGHT_CALIBRATION_CACHE_SAFETY,
    }
    return payload


def build_price_returns_matrix_cache_lookup(
    prices: pd.DataFrame,
    *,
    asset_universe: Sequence[str],
    start: date,
    end: date,
    data_source: str,
    adjusted_price_mode: str = "adj_close",
    source_config_hash: str = "not_applicable",
) -> dict[str, Any]:
    if start >= end:
        raise WeightCalibrationCacheError("price returns cache start must be before end")
    if not asset_universe:
        raise WeightCalibrationCacheError("asset_universe must not be empty")
    frame = prices.copy()
    if "date" not in frame.columns or "symbol" not in frame.columns:
        raise WeightCalibrationCacheError("prices must include date and symbol columns")
    _price_column_for_mode(frame, adjusted_price_mode)
    frame["_date"] = pd.to_datetime(frame["date"], errors="coerce")
    frame = frame.loc[
        (frame["_date"].dt.date >= start)
        & (frame["_date"].dt.date <= end)
        & frame["symbol"].isin(asset_universe)
    ].copy()
    if frame.empty:
        raise WeightCalibrationCacheError("no price rows available for requested cache window")
    trading_calendar = [
        pd.Timestamp(item).date().isoformat()
        for item in sorted(frame["_date"].dropna().unique())
    ]
    data_hash = weight_calibration_dataframe_hash(frame.drop(columns=["_date"]))
    trading_calendar_hash = weight_calibration_input_hash(trading_calendar)
    cache_key = build_weight_calibration_cache_key(
        "price_returns_matrix",
        {
            "source_config_hash": source_config_hash,
            "data_hash": data_hash,
            "engine_version": WEIGHT_CALIBRATION_PRICE_RETURNS_CACHE_SCHEMA_VERSION,
            "asset_universe": list(asset_universe),
            "start_date": start.isoformat(),
            "end_date": end.isoformat(),
            "data_source": data_source,
            "adjusted_price_mode": adjusted_price_mode,
            "trading_calendar_hash": trading_calendar_hash,
        },
        schema_version=WEIGHT_CALIBRATION_PRICE_RETURNS_CACHE_SCHEMA_VERSION,
    )
    return {
        "cache_key": cache_key,
        "data_hash": data_hash,
        "source_config_hash": source_config_hash,
        "trading_calendar": trading_calendar,
        "trading_calendar_hash": trading_calendar_hash,
    }


def write_price_returns_matrix_cache_entry(
    payload: Mapping[str, Any],
    *,
    cache_root: Path | str = DEFAULT_WEIGHT_CALIBRATION_CACHE_ROOT,
) -> dict[str, Path]:
    cache_key = str(payload.get("cache_key") or "")
    data_hash = str(payload.get("data_hash") or "")
    source_config_hash = str(payload.get("source_config_hash") or "")
    manifest = build_weight_calibration_cache_manifest(
        cache_key=cache_key,
        cache_layer="price_returns_matrix",
        source_config_hash=source_config_hash,
        data_hash=data_hash,
        model_version=WEIGHT_CALIBRATION_PRICE_RETURNS_CACHE_SCHEMA_VERSION,
        engine_version=WEIGHT_CALIBRATION_PRICE_RETURNS_CACHE_SCHEMA_VERSION,
        input_summary={
            "asset_universe": payload.get("asset_universe"),
            "date_range": payload.get("date_range"),
            "adjusted_price_mode": payload.get("adjusted_price_mode"),
        },
        output_summary={
            "trading_day_count": len(payload.get("trading_calendar") or []),
            "asset_count": len(payload.get("asset_universe") or []),
        },
    )
    return write_weight_calibration_json_cache_entry(
        cache_root=cache_root,
        cache_layer="price_returns_matrix",
        cache_key=cache_key,
        payload=payload,
        manifest=manifest,
    )


def run_weight_calibration_parallel_tasks(
    tasks: Sequence[Mapping[str, Any]],
    task_fn: Callable[[dict[str, Any]], Mapping[str, Any]],
    *,
    workers: int,
) -> dict[str, Any]:
    started = perf_counter()
    indexed_tasks = [(index, dict(task)) for index, task in enumerate(tasks)]
    results: dict[int, dict[str, Any]] = {}
    exceptions: list[dict[str, Any]] = []
    if workers == 1:
        for index, task in indexed_tasks:
            try:
                results[index] = dict(task_fn(task))
            except Exception as exc:  # noqa: BLE001 - worker exceptions are report data.
                exceptions.append(_worker_exception(index, task, exc))
    else:
        with ProcessPoolExecutor(max_workers=workers) as executor:
            future_by_index = {
                executor.submit(task_fn, task): (index, task) for index, task in indexed_tasks
            }
            for future in as_completed(future_by_index):
                index, task = future_by_index[future]
                try:
                    results[index] = dict(future.result())
                except Exception as exc:  # noqa: BLE001 - worker exceptions are report data.
                    exceptions.append(_worker_exception(index, task, exc))
    ordered_results = [
        results[index]
        for index in range(len(indexed_tasks))
        if index in results
    ]
    return {
        "status": "PASS" if not exceptions else "PARTIAL",
        "worker_count": workers,
        "task_count": len(indexed_tasks),
        "result_count": len(ordered_results),
        "exception_count": len(exceptions),
        "results": ordered_results,
        "exceptions": sorted(exceptions, key=lambda item: int(item["task_index"])),
        "runtime_seconds": round(perf_counter() - started, 6),
        "safety": dict(WEIGHT_CALIBRATION_CACHE_SAFETY),
        **WEIGHT_CALIBRATION_CACHE_SAFETY,
    }


def build_weight_calibration_diagnostics_run_manifest(
    *,
    run_id: str,
    status: RunManifestStatus,
    config_hash: str,
    data_hash: str,
    planned_steps: Sequence[str],
    completed_steps: Sequence[str] = (),
    failed_steps: Sequence[str] = (),
    candidate_statuses: Mapping[str, str] | None = None,
    cache_summary: Mapping[str, Any] | None = None,
    output_artifacts: Mapping[str, str] | None = None,
    started_at: datetime | None = None,
    completed_at: datetime | None = None,
) -> dict[str, Any]:
    manifest = WeightCalibrationDiagnosticsRunManifest(
        schema_version=WEIGHT_CALIBRATION_RUN_MANIFEST_SCHEMA_VERSION,
        run_id=run_id,
        started_at=started_at or datetime.now(UTC),
        completed_at=completed_at,
        status=status,
        config_hash=config_hash,
        data_hash=data_hash,
        planned_steps=list(planned_steps),
        completed_steps=list(completed_steps),
        failed_steps=list(failed_steps),
        candidate_statuses=dict(candidate_statuses or {}),
        cache_summary=dict(cache_summary or {}),
        output_artifacts=dict(output_artifacts or {}),
        safety=WeightCalibrationCacheSafety.model_validate(WEIGHT_CALIBRATION_CACHE_SAFETY),
    )
    payload = manifest.model_dump(mode="json")
    payload.update(WEIGHT_CALIBRATION_CACHE_SAFETY)
    return payload


def validate_weight_calibration_diagnostics_run_manifest(
    payload: Mapping[str, Any],
) -> WeightCalibrationDiagnosticsRunManifest:
    try:
        return WeightCalibrationDiagnosticsRunManifest.model_validate(payload)
    except ValueError as exc:
        raise WeightCalibrationCacheError(str(exc)) from exc


def write_weight_calibration_diagnostics_run_manifest(
    payload: Mapping[str, Any],
    *,
    cache_root: Path | str = DEFAULT_WEIGHT_CALIBRATION_CACHE_ROOT,
) -> Path:
    manifest = validate_weight_calibration_diagnostics_run_manifest(payload)
    path = Path(cache_root) / "runs" / manifest.run_id / "run_manifest.json"
    _atomic_write_json(path, payload)
    return path


def build_weight_calibration_performance_report(
    *,
    run_id: str,
    total_runtime_seconds: float,
    step_runtime_seconds: Mapping[str, float],
    worker_count: int,
    cache_mode: str,
    cache_events: Sequence[Mapping[str, Any]] = (),
    resume_status: str = "not_resumed",
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    hit_count = sum(1 for event in cache_events if str(event.get("cache_status")) == "hit")
    miss_count = sum(1 for event in cache_events if str(event.get("cache_status")) == "miss")
    write_count = sum(1 for event in cache_events if str(event.get("cache_status")) == "write")
    total_reads = hit_count + miss_count
    slowest_step = None
    if step_runtime_seconds:
        slowest_step = max(step_runtime_seconds.items(), key=lambda item: float(item[1]))[0]
    payload = {
        "schema_version": WEIGHT_CALIBRATION_PERFORMANCE_REPORT_SCHEMA_VERSION,
        "report_type": "etf_weight_calibration_performance_report",
        "run_id": run_id,
        "generated_at": (generated_at or datetime.now(UTC)).isoformat(),
        "total_runtime_seconds": round(float(total_runtime_seconds), 6),
        "step_runtime_seconds": {
            str(key): round(float(value), 6) for key, value in step_runtime_seconds.items()
        },
        "worker_count": worker_count,
        "cache_mode": normalize_weight_calibration_cache_mode(cache_mode),
        "cache_hit_count": hit_count,
        "cache_miss_count": miss_count,
        "cache_hit_rate": round(hit_count / total_reads, 6) if total_reads else None,
        "cache_write_count": write_count,
        "cache_events": [dict(event) for event in cache_events],
        "slowest_step": slowest_step,
        "slowest_candidate": None,
        "slowest_preset": None,
        "parallel_speedup_estimate_if_available": None,
        "resume_status": resume_status,
        "safety": dict(WEIGHT_CALIBRATION_CACHE_SAFETY),
        **WEIGHT_CALIBRATION_CACHE_SAFETY,
    }
    validate_weight_calibration_performance_report(payload)
    return payload


def validate_weight_calibration_performance_report(payload: Mapping[str, Any]) -> None:
    issues = []
    if payload.get("schema_version") != WEIGHT_CALIBRATION_PERFORMANCE_REPORT_SCHEMA_VERSION:
        issues.append("schema_version")
    if payload.get("report_type") != "etf_weight_calibration_performance_report":
        issues.append("report_type")
    for field, expected in WEIGHT_CALIBRATION_CACHE_SAFETY.items():
        if payload.get(field) != expected:
            issues.append(field)
        if _mapping(payload.get("safety")).get(field) != expected:
            issues.append(f"safety.{field}")
    if int(payload.get("worker_count") or 0) < 1:
        issues.append("worker_count")
    if issues:
        raise WeightCalibrationCacheError(
            "weight calibration performance report validation failed: " + ", ".join(issues)
        )


def write_weight_calibration_performance_report(
    payload: Mapping[str, Any],
    *,
    output_dir: Path | str = DEFAULT_WEIGHT_CALIBRATION_PERFORMANCE_REPORT_DIR,
) -> dict[str, Path]:
    validate_weight_calibration_performance_report(payload)
    timestamp = _artifact_timestamp(str(payload.get("generated_at") or datetime.now(UTC)))
    output_path = Path(output_dir)
    json_path = output_path / f"weight_calibration_performance_{timestamp}.json"
    markdown_path = output_path / f"weight_calibration_performance_{timestamp}.md"
    _atomic_write_json(json_path, payload)
    markdown_path.write_text(
        render_weight_calibration_performance_report_markdown(payload),
        encoding="utf-8",
    )
    return {"json": json_path, "markdown": markdown_path}


def render_weight_calibration_performance_report_markdown(payload: Mapping[str, Any]) -> str:
    lines = [
        "# ETF Weight Calibration Performance Report",
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
        f"- Cache Mode: {payload.get('cache_mode')}",
        f"- Worker Count: {payload.get('worker_count')}",
        f"- Total Runtime Seconds: {payload.get('total_runtime_seconds')}",
        f"- Cache Hit Count: {payload.get('cache_hit_count')}",
        f"- Cache Miss Count: {payload.get('cache_miss_count')}",
        f"- Cache Hit Rate: {payload.get('cache_hit_rate')}",
        f"- Cache Write Count: {payload.get('cache_write_count')}",
        f"- Slowest Step: {payload.get('slowest_step')}",
        f"- Resume Status: {payload.get('resume_status')}",
        "",
        "## Step Runtime",
        "",
        "| Step | Seconds |",
        "|---|---:|",
    ]
    for step, seconds in _mapping(payload.get("step_runtime_seconds")).items():
        lines.append(f"| {step} | {seconds} |")
    lines.extend(["", "## Cache Events", "", "| Layer | Status | Key |", "|---|---|---|"])
    for event in _records(payload.get("cache_events")):
        lines.append(
            f"| {event.get('cache_layer')} | {event.get('cache_status')} | "
            f"{event.get('cache_key')} |"
        )
    return "\n".join(lines) + "\n"


def build_weight_calibration_cache_parallel_validation_report(
    *,
    policy_config_path: Path | str = DEFAULT_WEIGHT_CALIBRATION_CACHE_POLICY_CONFIG_PATH,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    checks: list[dict[str, Any]] = []
    policy: WeightCalibrationCachePolicyConfig | None = None
    try:
        policy = load_weight_calibration_cache_policy_config(policy_config_path)
        _append_check(checks, "cache_policy_valid", True, "Cache policy loaded and validated.")
    except WeightCalibrationCacheError as exc:
        _append_check(checks, "cache_policy_valid", False, str(exc))
    if policy is not None:
        _append_check(
            checks,
            "cache_root_configured",
            bool(policy.weight_calibration_cache.cache_root),
            "Cache root is configured.",
            {"cache_root": policy.weight_calibration_cache.cache_root},
        )
        _append_check(
            checks,
            "safety_fields_valid",
            policy.weight_calibration_cache.safety.model_dump(mode="json")
            == WEIGHT_CALIBRATION_CACHE_SAFETY,
            "Cache policy safety boundary is observe-only and candidate-only.",
        )
    _append_check(
        checks,
        "cache_key_builder_available",
        _validation_cache_key_builder_available(),
        "Cache key builder requires common and layer-specific inputs.",
    )
    _append_check(
        checks,
        "manifest_schema_valid",
        _validation_manifest_schema_available(),
        "Cache manifest validates schema, safety, config hash, data hash, and engine version.",
    )
    _append_check(
        checks,
        "price_returns_cache_available",
        _validation_price_returns_cache_available(),
        "Price / returns matrix cache payload can be generated.",
    )
    for layer in (
        "candidate_universe",
        "candidate_backtest",
        "regime_robustness",
        "diagnostics_aggregation",
    ):
        _append_check(
            checks,
            f"{layer}_cache_key_available",
            _validation_layer_key_available(layer),
            f"{layer} cache key includes required invalidation inputs.",
        )
    _append_check(
        checks,
        "parallel_runner_available",
        _validation_parallel_runner_available(),
        "Parallel runner preserves deterministic result ordering and captures safety fields.",
    )
    _append_check(
        checks,
        "resume_manifest_available",
        _validation_resume_manifest_available(),
        "Diagnostics run manifest schema is available.",
    )
    _append_check(
        checks,
        "performance_report_available",
        _validation_performance_report_available(),
        "Performance report schema and cache hit-rate calculation are available.",
    )
    failed = [check for check in checks if check["status"] != "PASS"]
    payload = {
        "schema_version": WEIGHT_CALIBRATION_CACHE_VALIDATION_SCHEMA_VERSION,
        "report_type": "etf_weight_calibration_cache_parallel_validation",
        "status": "PASS" if not failed else "FAIL",
        "generated_at": generated.isoformat(),
        "policy_config_path": str(policy_config_path),
        "check_count": len(checks),
        "failed_check_count": len(failed),
        "checks": checks,
        "safety": dict(WEIGHT_CALIBRATION_CACHE_SAFETY),
        **WEIGHT_CALIBRATION_CACHE_SAFETY,
    }
    validate_weight_calibration_cache_parallel_validation_report(payload)
    return payload


def validate_weight_calibration_cache_parallel_validation_report(
    payload: Mapping[str, Any],
) -> None:
    issues = []
    if payload.get("schema_version") != WEIGHT_CALIBRATION_CACHE_VALIDATION_SCHEMA_VERSION:
        issues.append("schema_version")
    if payload.get("report_type") != "etf_weight_calibration_cache_parallel_validation":
        issues.append("report_type")
    for field, expected in WEIGHT_CALIBRATION_CACHE_SAFETY.items():
        if payload.get(field) != expected:
            issues.append(field)
        if _mapping(payload.get("safety")).get(field) != expected:
            issues.append(f"safety.{field}")
    if int(payload.get("failed_check_count") or 0) != sum(
        1 for check in _records(payload.get("checks")) if check.get("status") != "PASS"
    ):
        issues.append("failed_check_count")
    if issues:
        raise WeightCalibrationCacheError(
            "weight calibration cache validation report failed: " + ", ".join(issues)
        )


def write_weight_calibration_cache_parallel_validation_report(
    payload: Mapping[str, Any],
    *,
    output_dir: Path | str = DEFAULT_WEIGHT_CALIBRATION_CACHE_VALIDATION_DIR,
) -> dict[str, Path]:
    validate_weight_calibration_cache_parallel_validation_report(payload)
    timestamp = _artifact_timestamp(str(payload.get("generated_at") or datetime.now(UTC)))
    output_path = Path(output_dir)
    json_path = output_path / f"weight_calibration_cache_parallel_validation_{timestamp}.json"
    markdown_path = output_path / f"weight_calibration_cache_parallel_validation_{timestamp}.md"
    _atomic_write_json(json_path, payload)
    markdown_path.write_text(
        render_weight_calibration_cache_parallel_validation_markdown(payload),
        encoding="utf-8",
    )
    return {"json": json_path, "markdown": markdown_path}


def render_weight_calibration_cache_parallel_validation_markdown(
    payload: Mapping[str, Any],
) -> str:
    lines = [
        "# ETF Weight Calibration Cache / Parallel Validation",
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
        f"- Policy Config: {payload.get('policy_config_path')}",
        "",
        "## Checks",
        "",
        "| Check | Status | Message |",
        "|---|---|---|",
    ]
    for check in _records(payload.get("checks")):
        lines.append(
            f"| {check.get('check_id')} | {check.get('status')} | "
            f"{_md_escape(check.get('message'))} |"
        )
    return "\n".join(lines) + "\n"


def _normalize_cache_layer(cache_layer: str) -> CacheLayer:
    layer = str(cache_layer).strip().lower().replace("-", "_")
    if layer not in WEIGHT_CALIBRATION_CACHE_LAYERS:
        raise WeightCalibrationCacheError(f"unknown weight calibration cache layer: {cache_layer}")
    return layer  # type: ignore[return-value]


def _canonical_json(value: Any) -> str:
    return json.dumps(_jsonable(value), ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _jsonable(value: Any) -> Any:
    if isinstance(value, BaseModel):
        return value.model_dump(mode="json")
    if isinstance(value, Mapping):
        return {str(key): _jsonable(value[key]) for key in sorted(value, key=str)}
    if isinstance(value, (list, tuple)):
        return [_jsonable(item) for item in value]
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    if value is pd.NA:
        return None
    if isinstance(value, float) and value != value:
        return None
    return value


def _blank(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return not value.strip()
    if isinstance(value, Sequence) and not isinstance(value, (bytes, bytearray, str)):
        return len(value) == 0
    return False


def _atomic_write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_name(f"{path.name}.{os.getpid()}.tmp")
    temp_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    temp_path.replace(path)


def _price_column_for_mode(frame: pd.DataFrame, adjusted_price_mode: str) -> str:
    mode = adjusted_price_mode.strip().lower()
    if mode in {"adj_close", "adjusted", "adjusted_close"}:
        if "adj_close" not in frame.columns:
            raise WeightCalibrationCacheError("adj_close column required for adjusted price mode")
        return "adj_close"
    if mode == "close":
        if "close" not in frame.columns:
            raise WeightCalibrationCacheError("close column required for close price mode")
        return "close"
    if mode == "adj_close_or_close":
        return "adj_close" if "adj_close" in frame.columns else "close"
    raise WeightCalibrationCacheError(f"unsupported adjusted_price_mode: {adjusted_price_mode}")


def _matrix_rows(matrix: pd.DataFrame) -> list[dict[str, Any]]:
    rows = []
    for current_date, row in matrix.iterrows():
        record = {"date": current_date.date().isoformat()}
        for symbol, value in row.items():
            record[str(symbol)] = None if pd.isna(value) else float(value)
        rows.append(record)
    return rows


def _worker_exception(index: int, task: Mapping[str, Any], exc: Exception) -> dict[str, Any]:
    return {
        "task_index": index,
        "task_id": str(task.get("task_id") or task.get("candidate_id") or index),
        "exception_type": type(exc).__name__,
        "message": str(exc),
    }


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
            "safety": dict(WEIGHT_CALIBRATION_CACHE_SAFETY),
            **WEIGHT_CALIBRATION_CACHE_SAFETY,
        }
    )


def _validation_cache_key_builder_available() -> bool:
    valid_inputs = _sample_cache_key_inputs("candidate_backtest")
    first = build_weight_calibration_cache_key("candidate_backtest", valid_inputs)
    second = build_weight_calibration_cache_key(
        "candidate_backtest",
        dict(reversed(valid_inputs.items())),
    )
    try:
        build_weight_calibration_cache_key(
            "candidate_backtest",
            {"source_config_hash": "cfg", "engine_version": "engine"},
        )
    except WeightCalibrationCacheError:
        return first == second
    return False


def _validation_manifest_schema_available() -> bool:
    key = build_weight_calibration_cache_key(
        "diagnostics_aggregation",
        _sample_cache_key_inputs("diagnostics_aggregation"),
    )
    manifest = build_weight_calibration_cache_manifest(
        cache_key=key,
        cache_layer="diagnostics_aggregation",
        source_config_hash="cfg",
        data_hash="data",
        model_version="model",
        engine_version="engine",
        input_summary={"sample": True},
    )
    validate_weight_calibration_cache_manifest(
        manifest,
        expected_cache_key=key,
        expected_cache_layer="diagnostics_aggregation",
        expected_source_config_hash="cfg",
        expected_data_hash="data",
        expected_engine_version="engine",
    )
    unsafe = dict(manifest)
    unsafe["safety"] = {**dict(unsafe["safety"]), "production_effect": "apply_weights"}
    try:
        validate_weight_calibration_cache_manifest(unsafe)
    except WeightCalibrationCacheError:
        return True
    return False


def _validation_price_returns_cache_available() -> bool:
    prices = pd.DataFrame(
        [
            {"date": "2026-06-01", "symbol": "SPY", "adj_close": 100.0},
            {"date": "2026-06-02", "symbol": "SPY", "adj_close": 101.0},
            {"date": "2026-06-01", "symbol": "QQQ", "adj_close": 200.0},
            {"date": "2026-06-02", "symbol": "QQQ", "adj_close": 202.0},
        ]
    )
    payload = build_price_returns_matrix_cache_payload(
        prices,
        asset_universe=["SPY", "QQQ"],
        start=date(2026, 6, 1),
        end=date(2026, 6, 2),
        data_source="validation_fixture",
        source_config_hash="cfg",
    )
    return bool(payload["price_matrix"]) and bool(payload["returns_matrix"])


def _validation_layer_key_available(layer: str) -> bool:
    key = build_weight_calibration_cache_key(layer, _sample_cache_key_inputs(layer))
    changed = dict(_sample_cache_key_inputs(layer))
    changed["data_hash"] = "changed-data"
    changed_key = build_weight_calibration_cache_key(layer, changed)
    return key != changed_key


def _validation_parallel_runner_available() -> bool:
    result = run_weight_calibration_parallel_tasks(
        [{"task_id": "b", "value": 2}, {"task_id": "a", "value": 1}],
        _validation_parallel_worker,
        workers=1,
    )
    return (
        result["status"] == "PASS"
        and [item["task_id"] for item in result["results"]] == ["b", "a"]
        and result["production_effect"] == "none"
    )


def _validation_resume_manifest_available() -> bool:
    payload = build_weight_calibration_diagnostics_run_manifest(
        run_id="validation-run",
        status="completed",
        config_hash="cfg",
        data_hash="data",
        planned_steps=["diagnostics"],
        completed_steps=["diagnostics"],
        completed_at=datetime(2026, 6, 4, tzinfo=UTC),
        started_at=datetime(2026, 6, 4, tzinfo=UTC),
    )
    validate_weight_calibration_diagnostics_run_manifest(payload)
    return payload["production_effect"] == "none"


def _validation_performance_report_available() -> bool:
    payload = build_weight_calibration_performance_report(
        run_id="validation-run",
        total_runtime_seconds=1.0,
        step_runtime_seconds={"diagnostics": 1.0},
        worker_count=1,
        cache_mode="read-write",
        cache_events=[
            {"cache_layer": "diagnostics_aggregation", "cache_status": "hit"},
            {"cache_layer": "candidate_backtest", "cache_status": "miss"},
        ],
    )
    return payload["cache_hit_rate"] == 0.5 and payload["production_effect"] == "none"


def _validation_parallel_worker(task: dict[str, Any]) -> dict[str, Any]:
    return {"task_id": str(task["task_id"]), "value": int(task["value"]) * 2}


def _sample_cache_key_inputs(layer: str) -> dict[str, Any]:
    base = {
        "source_config_hash": "cfg",
        "data_hash": "data",
        "engine_version": "engine",
    }
    layer_inputs = {
        "price_returns_matrix": {
            "asset_universe": ["SPY", "QQQ"],
            "start_date": "2022-12-01",
            "end_date": "2026-06-02",
            "data_source": "prices",
            "adjusted_price_mode": "adj_close",
            "trading_calendar_hash": "calendar",
        },
        "candidate_universe": {
            "search_id": "search",
            "weight_constraints_hash": "weights",
            "sleeve_constraints_hash": "sleeves",
            "grid_step": 0.05,
            "asset_universe_hash": "universe",
        },
        "candidate_backtest": {
            "candidate_weights_hash": "candidate",
            "date_range": {"start": "2022-12-01", "end": "2026-06-02"},
            "returns_matrix_hash": "returns",
            "backtest_engine_version": "backtest",
            "transaction_cost_config_hash": "cost",
            "rebalance_policy_hash": "rebalance",
            "benchmark_set_hash": "benchmarks",
        },
        "regime_robustness": {
            "candidate_weights_hash": "candidate",
            "date_range": {"start": "2022-12-01", "end": "2026-06-02"},
            "regime_definition_hash": "regimes",
            "returns_matrix_hash": "returns",
            "metrics_schema_version": "metrics",
        },
        "diagnostics_aggregation": {
            "diagnostics_config_hash": "diagnostics",
            "input_search_run_ids": ["run-a"],
            "input_candidate_result_hashes": ["candidate-a"],
            "aggregation_engine_version": "aggregation",
        },
    }
    selected_layer = _normalize_cache_layer(layer)
    return {**base, **layer_inputs[selected_layer]}


def _records(value: Any) -> list[Mapping[str, Any]]:
    if isinstance(value, list):
        return [item for item in value if isinstance(item, Mapping)]
    return []


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _artifact_timestamp(value: str) -> str:
    return (
        value.replace("+00:00", "Z")
        .replace(":", "")
        .replace("-", "")
        .replace(".", "")
        .replace("T", "T")[:15]
    )


def _md_escape(value: Any) -> str:
    return str(value).replace("|", "\\|").replace("\n", " ")
