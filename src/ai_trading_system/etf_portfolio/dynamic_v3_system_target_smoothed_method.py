from __future__ import annotations

import math
from collections.abc import Callable, Mapping, Sequence
from datetime import datetime
from pathlib import Path
from typing import Any

import yaml

from ai_trading_system.etf_portfolio import dynamic_v3_system_target as legacy
from ai_trading_system.etf_portfolio import dynamic_v3_system_target_hardening as hardening
from ai_trading_system.etf_portfolio import dynamic_v3_system_target_history as history
from ai_trading_system.etf_portfolio import dynamic_v3_system_target_portfolio as target_core
from ai_trading_system.etf_portfolio import dynamic_v3_system_target_risk_capped as risk_capped
from ai_trading_system.etf_portfolio.dynamic_v3_confirmation_operations import (
    _file_bytes_match,
    _json_bytes,
    _jsonl_bytes,
)
from ai_trading_system.etf_portfolio.dynamic_v3_historical_replay import (
    _check,
    _mapping,
    _read_json,
    _read_jsonl,
    _records,
    _stable_id,
    _text,
    _unique_dir,
    _update_latest_pointer,
)
from ai_trading_system.etf_portfolio.dynamic_v3_pressure_validation import (
    _write_views_atomic,
)
from ai_trading_system.platform.artifacts.validation_session import (
    cached_artifact_validation,
)

SMOOTHED_CONFIG_SNAPSHOT_SCHEMA = "smoothed_config_input_snapshot.v2"
SMOOTHED_TARGET_SNAPSHOT_SCHEMA = "smoothed_target_input_snapshot.v2"
SMOOTHED_BACKFILL_SNAPSHOT_SCHEMA = "smoothed_backfill_input_snapshot.v2"
SMOOTHED_COMPARISON_SNAPSHOT_SCHEMA = "smoothed_comparison_input_snapshot.v2"
SMOOTHED_REVIEW_SNAPSHOT_SCHEMA = "smoothed_review_input_snapshot.v2"

DEFAULT_SMOOTHED_LIMITED_CONFIG_PATH = legacy.DEFAULT_SMOOTHED_LIMITED_CONFIG_PATH
DEFAULT_MODEL_TARGET_CONFIG_PATH = legacy.DEFAULT_MODEL_TARGET_CONFIG_PATH
DEFAULT_PAPER_SHADOW_BACKFILL_CONFIG_PATH = history.DEFAULT_PAPER_SHADOW_BACKFILL_CONFIG_PATH
DEFAULT_SMOOTHED_CONFIG_DIR = legacy.DEFAULT_SMOOTHED_CONFIG_DIR
DEFAULT_SMOOTHED_LIMITED_DIR = legacy.DEFAULT_SMOOTHED_LIMITED_DIR
DEFAULT_SMOOTHED_BACKFILL_DIR = legacy.DEFAULT_SMOOTHED_BACKFILL_DIR
DEFAULT_SMOOTHED_COMPARISON_DIR = legacy.DEFAULT_SMOOTHED_COMPARISON_DIR
DEFAULT_SMOOTHED_REVIEW_DIR = legacy.DEFAULT_SMOOTHED_REVIEW_DIR
DEFAULT_MODEL_TARGET_DIR = target_core.DEFAULT_MODEL_TARGET_DIR
DEFAULT_PAPER_SHADOW_BACKFILL_DIR = history.DEFAULT_PAPER_SHADOW_BACKFILL_DIR
DEFAULT_RISK_CAPPED_BACKFILL_DIR = risk_capped.DEFAULT_RISK_CAPPED_BACKFILL_DIR
DEFAULT_RATES_CACHE_PATH = history.DEFAULT_RATES_CACHE_PATH
SYSTEM_TARGET_SAFETY = history.SYSTEM_TARGET_SAFETY
SMOOTHED_VARIANT_TO_METHOD = legacy.SMOOTHED_VARIANT_TO_METHOD
SMOOTHED_METHOD_TO_VARIANT = legacy.SMOOTHED_METHOD_TO_VARIANT


class DynamicV3SmoothedMethodError(ValueError):
    """Raised when smoothed-method evidence is invalid or not reproducible."""


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise DynamicV3SmoothedMethodError(message)


def _generated_at(value: datetime | None) -> datetime:
    try:
        return target_core._generated_at(value)
    except Exception as exc:  # noqa: BLE001
        raise DynamicV3SmoothedMethodError(str(exc)) from exc


def _finite(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)


def _validation_payload(
    report_type: str,
    artifact_id: str,
    checks: Sequence[Mapping[str, Any]],
    *,
    artifact_id_key: str | None = None,
    extra: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    return target_core._validation_payload(
        report_type,
        artifact_id,
        checks,
        artifact_id_key=artifact_id_key,
        extra=extra,
    )


def _write(root: Path, views: Mapping[str, bytes], pointer: str, manifest_name: str) -> None:
    _write_views_atomic(root, views)
    _update_latest_pointer(pointer, root.name, root / manifest_name)


def _view_errors(root: Path, views: Mapping[str, bytes]) -> list[str]:
    return [name for name, payload in views.items() if not _file_bytes_match(root / name, payload)]


def _bundle_json(binding: Mapping[str, Any], name: str) -> dict[str, Any]:
    return hardening._bundle_json(binding, name)


def _bundle_jsonl(binding: Mapping[str, Any], name: str) -> list[dict[str, Any]]:
    return hardening._bundle_jsonl(binding, name)


def _config_path(binding: Mapping[str, Any]) -> Path:
    return target_core._binding_path(binding)


def _policy_metadata(payload: Mapping[str, Any]) -> dict[str, Any]:
    try:
        return target_core._policy_metadata(payload, name="smoothed-method policy")
    except Exception as exc:  # noqa: BLE001
        raise DynamicV3SmoothedMethodError(str(exc)) from exc


def _evaluation_policy(payload: Mapping[str, Any]) -> dict[str, Any]:
    policy = _mapping(payload.get("evaluation_policy"))
    numeric_fields = {
        "acceptable_return_delta_floor",
        "drawdown_improvement_floor",
        "turnover_improvement_ceiling",
        "jump_improvement_ceiling",
        "lag_cost_high_threshold",
        "lag_cost_medium_threshold",
        "minimum_path_observations",
        "minimum_regime_observations",
        "minimum_rolling_windows",
    }
    required = {*numeric_fields, "candidate_priority"}
    _require(set(policy) == required, "smoothed evaluation_policy fields must be exact")
    for field in sorted(numeric_fields):
        _require(_finite(policy.get(field)), f"evaluation_policy.{field} must be finite")
    _require(
        -1.0 <= float(policy["acceptable_return_delta_floor"]) <= 0.0,
        "acceptable_return_delta_floor must be between -1 and 0",
    )
    _require(
        0.0 <= float(policy["drawdown_improvement_floor"]) <= 0.1,
        "drawdown_improvement_floor must be between 0 and 0.1",
    )
    _require(
        -1.0 <= float(policy["turnover_improvement_ceiling"]) <= 0.0,
        "turnover_improvement_ceiling must be between -1 and 0",
    )
    _require(
        float(policy["jump_improvement_ceiling"]) <= 0.0,
        "jump_improvement_ceiling must be non-positive",
    )
    _require(
        float(policy["lag_cost_high_threshold"])
        <= float(policy["lag_cost_medium_threshold"])
        <= 0.0,
        "lag thresholds must be ordered and non-positive",
    )
    for field in (
        "minimum_path_observations",
        "minimum_regime_observations",
        "minimum_rolling_windows",
    ):
        value = float(policy[field])
        _require(value.is_integer() and value >= 1.0, f"{field} must be a positive integer")
    priority = list(policy.get("candidate_priority") or [])
    _require(
        priority == list(SMOOTHED_METHOD_TO_VARIANT),
        "candidate_priority must list every smoothed method exactly once",
    )
    return {
        **{field: float(policy[field]) for field in sorted(numeric_fields)},
        "minimum_path_observations": int(policy["minimum_path_observations"]),
        "minimum_regime_observations": int(policy["minimum_regime_observations"]),
        "minimum_rolling_windows": int(policy["minimum_rolling_windows"]),
        "candidate_priority": priority,
    }


def load_smoothed_limited_config(
    path: Path = DEFAULT_SMOOTHED_LIMITED_CONFIG_PATH,
) -> dict[str, Any]:
    payload = legacy._load_yaml_mapping(path)
    legacy._assert_smoothed_limited_config_safe(payload)
    _policy_metadata(payload)
    _evaluation_policy(payload)
    return payload


def validate_smoothed_limited_config(
    path: Path = DEFAULT_SMOOTHED_LIMITED_CONFIG_PATH,
    *,
    model_config_path: Path = DEFAULT_MODEL_TARGET_CONFIG_PATH,
) -> dict[str, Any]:
    checks: list[dict[str, Any]] = []
    payload: dict[str, Any] = {}
    model_config: dict[str, Any] = {}
    try:
        payload = load_smoothed_limited_config(path)
        checks.append(_check("config_loads", True, ""))
    except Exception as exc:  # noqa: BLE001
        checks.append(_check("config_loads", False, str(exc)))
    try:
        model_config = target_core.load_model_target_config(model_config_path)
        checks.append(_check("model_config_loads", True, ""))
    except Exception as exc:  # noqa: BLE001
        checks.append(_check("model_config_loads", False, str(exc)))
    if payload and model_config:
        method = _mapping(payload.get("method"))
        variants = _mapping(payload.get("variants"))
        constraints = _mapping(payload.get("constraints"))
        model_constraints = legacy._constraints(model_config)
        enabled = legacy._enabled_smoothed_variants(payload)
        variant_rows = [_mapping(variants.get(variant)) for variant in enabled]
        finite_variant_fields = all(
            _finite(row.get(field))
            for row in variant_rows
            for field in (
                "smoothing_window_days",
                "alpha",
                "min_signal_persistence_days",
                "max_daily_total_weight_change",
                "max_single_symbol_daily_change",
            )
        )
        model_max_single = legacy._float(model_constraints.get("max_single_symbol_weight"), 1.0)
        model_max_semi = legacy._float(model_constraints.get("max_semiconductor_weight"), 1.0)
        model_max_risk = legacy._float(model_constraints.get("max_total_risk_asset_weight"), 1.0)
        model_min_cash = legacy._float(model_constraints.get("min_cash_weight"), 0.0)
        checks.extend(
            [
                _check("schema_version", payload.get("schema_version") == 1, ""),
                _check(
                    "method_name",
                    method.get("name") == "smoothed_limited_adjustment",
                    _text(method.get("name")),
                ),
                _check(
                    "base_method_limited_adjustment",
                    method.get("base_method") == "limited_adjustment",
                    _text(method.get("base_method")),
                ),
                _check(
                    "research_target_only",
                    method.get("mode") == "research_target_only"
                    and method.get("not_official_target_weights") is True
                    and method.get("paper_shadow_only") is True,
                    "",
                ),
                _check(
                    "required_variants_enabled",
                    enabled == list(SMOOTHED_VARIANT_TO_METHOD),
                    ",".join(enabled),
                ),
                _check("variant_fields_finite", finite_variant_fields, ""),
                _check(
                    "variant_smoothing_windows_positive",
                    finite_variant_fields
                    and all(
                        float(row["smoothing_window_days"]).is_integer()
                        and float(row["smoothing_window_days"]) >= 1.0
                        for row in variant_rows
                    ),
                    "",
                ),
                _check(
                    "variant_persistence_positive_integer",
                    finite_variant_fields
                    and all(
                        float(row["min_signal_persistence_days"]).is_integer()
                        and float(row["min_signal_persistence_days"]) >= 1.0
                        for row in variant_rows
                    ),
                    "",
                ),
                _check(
                    "variant_alpha_within_bounds",
                    finite_variant_fields
                    and all(0.0 < float(row["alpha"]) <= 1.0 for row in variant_rows),
                    "",
                ),
                _check(
                    "variant_change_caps_valid",
                    finite_variant_fields
                    and all(
                        0.0 < float(row["max_single_symbol_daily_change"])
                        <= float(row["max_daily_total_weight_change"])
                        <= 1.0
                        for row in variant_rows
                    ),
                    "",
                ),
                _check(
                    "constraints_not_wider_than_model_target",
                    legacy._float(constraints.get("max_single_symbol_weight"), 2.0)
                    <= model_max_single
                    and legacy._float(constraints.get("max_semiconductor_weight"), 2.0)
                    <= model_max_semi
                    and legacy._float(constraints.get("max_total_risk_asset_weight"), 2.0)
                    <= model_max_risk
                    and legacy._float(constraints.get("min_cash_weight"), -1.0)
                    >= model_min_cash,
                    "",
                ),
                _check(
                    "constraints_within_bounds",
                    legacy._smoothed_constraints_within_bounds(constraints),
                    "",
                ),
                _check("policy_metadata_reviewed", bool(_policy_metadata(payload)), ""),
                _check("evaluation_policy_governed", bool(_evaluation_policy(payload)), ""),
                _check(
                    "safety_locked",
                    legacy._safety_config_locked(_mapping(payload.get("safety"))),
                    "",
                ),
            ]
        )
    return _validation_payload(
        "etf_dynamic_v3_smoothed_limited_config_validation",
        "smoothed_limited_config",
        checks,
        extra={"config_path": str(path), "model_config_path": str(model_config_path)},
    )


def _config_snapshot(
    *, config_path: Path, model_config_path: Path, generated: datetime
) -> dict[str, Any]:
    validation = validate_smoothed_limited_config(
        config_path, model_config_path=model_config_path
    )
    _require(validation.get("status") == "PASS", "smoothed config validation failed")
    return {
        "schema_version": SMOOTHED_CONFIG_SNAPSHOT_SCHEMA,
        "generated_at": generated.isoformat(),
        "config_binding": target_core._config_binding(config_path, kind="smoothed_method_policy"),
        "model_config_binding": target_core._config_binding(
            model_config_path, kind="model_target_policy"
        ),
        "validation": validation,
        "production_effect": "none",
    }


def _config_views(
    snapshot: Mapping[str, Any], *, config_validation_id: str, output_dir: Path
) -> tuple[dict[str, bytes], dict[str, Any]]:
    config = _mapping(_mapping(snapshot.get("config_binding")).get("payload"))
    validation = _mapping(snapshot.get("validation"))
    normalized = legacy._normalized_smoothed_config(config)
    manifest = {
        "schema_version": 1,
        "report_type": "etf_dynamic_v3_smoothed_limited_config_manifest",
        "config_validation_id": config_validation_id,
        "generated_at": snapshot.get("generated_at"),
        "status": validation.get("status"),
        "config_path": str(_config_path(_mapping(snapshot.get("config_binding")))),
        "model_config_path": str(_config_path(_mapping(snapshot.get("model_config_binding")))),
        "base_method": _mapping(config.get("method")).get("base_method"),
        "target_method": _mapping(config.get("method")).get("name"),
        "enabled_variants": legacy._enabled_smoothed_variants(config),
        "policy_id": _mapping(config.get("policy_metadata")).get("policy_id"),
        "smoothed_config_input_snapshot_path": str(
            output_dir / "smoothed_config_input_snapshot.json"
        ),
        "smoothed_limited_config_manifest_path": str(
            output_dir / "smoothed_limited_config_manifest.json"
        ),
        "normalized_smoothed_limited_config_path": str(
            output_dir / "normalized_smoothed_limited_config.yaml"
        ),
        "smoothed_limited_config_report_path": str(
            output_dir / "smoothed_limited_config_report.md"
        ),
        **SYSTEM_TARGET_SAFETY,
    }
    report = legacy.render_smoothed_limited_config_report(manifest, normalized, validation)
    views = {
        "smoothed_config_input_snapshot.json": _json_bytes(dict(snapshot)),
        "smoothed_limited_config_manifest.json": _json_bytes(manifest),
        "normalized_smoothed_limited_config.yaml": yaml.safe_dump(
            normalized, sort_keys=True, allow_unicode=True
        ).encode("utf-8"),
        "smoothed_limited_config_report.md": report.encode("utf-8"),
    }
    return views, {"manifest": manifest, "normalized_config": normalized, "validation": validation}


def _validate_config_snapshot(snapshot: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    try:
        _require(
            snapshot.get("schema_version") == SMOOTHED_CONFIG_SNAPSHOT_SCHEMA,
            "smoothed config snapshot schema invalid",
        )
        config_binding = _mapping(snapshot.get("config_binding"))
        model_binding = _mapping(snapshot.get("model_config_binding"))
        errors.extend(target_core._validate_config_binding(config_binding))
        errors.extend(target_core._validate_config_binding(model_binding))
        validation = validate_smoothed_limited_config(
            _config_path(config_binding), model_config_path=_config_path(model_binding)
        )
        if validation != _mapping(snapshot.get("validation")):
            errors.append("smoothed config validation drift")
    except Exception as exc:  # noqa: BLE001
        errors.append(str(exc))
    return errors


def build_smoothed_limited_config_report(
    *,
    config_path: Path = DEFAULT_SMOOTHED_LIMITED_CONFIG_PATH,
    model_config_path: Path = DEFAULT_MODEL_TARGET_CONFIG_PATH,
    output_dir: Path = DEFAULT_SMOOTHED_CONFIG_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = _generated_at(generated_at)
    snapshot = _config_snapshot(
        config_path=config_path, model_config_path=model_config_path, generated=generated
    )
    config_validation_id = _stable_id("smoothed-limited-config", snapshot)
    root = _unique_dir(output_dir / config_validation_id)
    views, payload = _config_views(snapshot, config_validation_id=root.name, output_dir=root)
    _write(
        root,
        views,
        "latest_smoothed_limited_config",
        "smoothed_limited_config_manifest.json",
    )
    return {"config_validation_id": root.name, "config_dir": root, **payload}


def smoothed_limited_config_report_payload(
    *,
    config_validation_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_SMOOTHED_CONFIG_DIR,
) -> dict[str, Any]:
    root = hardening._artifact_dir_from_latest(
        output_dir=output_dir,
        artifact_id=config_validation_id if not latest else None,
        pointer_name="latest_smoothed_limited_config",
    )
    return {
        **_read_json(root / "smoothed_limited_config_manifest.json"),
        "config_dir": str(root),
        "normalized_config": legacy._load_yaml_mapping(
            root / "normalized_smoothed_limited_config.yaml"
        ),
        "input_snapshot": _read_json(root / "smoothed_config_input_snapshot.json"),
    }


def validate_smoothed_limited_config_report_artifact(
    *,
    config_validation_id: str,
    output_dir: Path = DEFAULT_SMOOTHED_CONFIG_DIR,
) -> dict[str, Any]:
    root = output_dir / config_validation_id
    snapshot = legacy._read_optional_json(root / "smoothed_config_input_snapshot.json") or {}
    errors = _validate_config_snapshot(snapshot)
    mismatches: list[str] = []
    try:
        views, _ = _config_views(
            snapshot, config_validation_id=config_validation_id, output_dir=root
        )
        mismatches = _view_errors(root, views)
    except Exception as exc:  # noqa: BLE001
        errors.append(str(exc))
    return _validation_payload(
        "etf_dynamic_v3_smoothed_limited_config_report_validation",
        config_validation_id,
        [
            _check("snapshot_and_live_inputs", not errors, "; ".join(errors)),
            _check("content_derived_views", not mismatches, ",".join(mismatches)),
        ],
        artifact_id_key="config_validation_id",
    )


def _model_target_binding(target_id: str, root: Path) -> dict[str, Any]:
    validation = target_core.validate_model_target_artifact(target_id=target_id, output_dir=root)
    return target_core._artifact_binding(
        kind="model_target",
        artifact_dir=root / target_id,
        artifact_id=target_id,
        validation=validation,
        json_views=(
            "model_target_input_snapshot.json",
            "model_target_manifest.json",
            "model_target_weights.json",
            "target_constraint_checks.json",
        ),
        jsonl_views=("method_target_weights.jsonl",),
    )


def _target_snapshot(
    *,
    target_id: str,
    model_target_dir: Path,
    config_path: Path,
    regime_context: str,
    generated: datetime,
) -> dict[str, Any]:
    source = _model_target_binding(target_id, model_target_dir)
    manifest = _bundle_json(source, "model_target_manifest.json")
    source_generated = target_core._datetime(
        manifest.get("generated_at"), field="model target generated_at"
    )
    source_as_of = target_core._date(manifest.get("as_of"), field="model target as_of")
    _require(source_generated <= generated, "model target generated after smoothed cutoff")
    _require(source_as_of <= generated.date(), "model target as_of after smoothed cutoff")
    model_config_path = legacy._resolve_project_path(
        manifest.get("config_path"), DEFAULT_MODEL_TARGET_CONFIG_PATH
    )
    config_validation = validate_smoothed_limited_config(
        config_path, model_config_path=model_config_path
    )
    _require(config_validation.get("status") == "PASS", "smoothed config validation failed")
    config = load_smoothed_limited_config(config_path)
    allowed_contexts = {"normal", *_mapping(config.get("regime_context"))}
    _require(regime_context in allowed_contexts, "smoothed regime_context invalid")
    return {
        "schema_version": SMOOTHED_TARGET_SNAPSHOT_SCHEMA,
        "generated_at": generated.isoformat(),
        "target_id": target_id,
        "regime_context": regime_context,
        "model_target_source": source,
        "config_binding": target_core._config_binding(
            config_path, kind="smoothed_method_policy"
        ),
        "model_config_binding": target_core._config_binding(
            model_config_path, kind="model_target_policy"
        ),
        "config_validation": config_validation,
        "source_cutoff": {
            "generated_at": generated.isoformat(),
            "as_of": generated.date().isoformat(),
        },
        "production_effect": "none",
    }


def _target_views(
    snapshot: Mapping[str, Any], *, smoothed_id: str, output_dir: Path
) -> tuple[dict[str, bytes], dict[str, Any]]:
    source = _mapping(snapshot.get("model_target_source"))
    source_manifest = _bundle_json(source, "model_target_manifest.json")
    model_target_weights = _bundle_json(source, "model_target_weights.json")
    method_weights = _mapping(model_target_weights.get("method_weights"))
    base_weights = target_core._weights(
        method_weights.get("limited_adjustment"), field="limited_adjustment"
    )
    previous_default = target_core._weights(
        method_weights.get("static_baseline"), field="static_baseline"
    )
    config = _mapping(_mapping(snapshot.get("config_binding")).get("payload"))
    model_config = _mapping(_mapping(snapshot.get("model_config_binding")).get("payload"))
    as_of = target_core._date(source_manifest.get("as_of"), field="model target as_of")
    target_rows: list[dict[str, Any]] = []
    smoothing_events: list[dict[str, Any]] = []
    lag_events: list[dict[str, Any]] = []
    for variant in legacy._enabled_smoothed_variants(config):
        target_method = SMOOTHED_VARIANT_TO_METHOD[variant]
        raw_previous = method_weights.get(target_method) or previous_default
        previous = target_core._weights(raw_previous, field=f"{target_method}.previous")
        result = legacy._apply_smoothed_limited_adjustment(
            as_of=as_of,
            base_weights=base_weights,
            previous_smoothed_weights=previous,
            smoothed_config=config,
            model_config=model_config,
            variant_id=variant,
            regime_context=_text(snapshot.get("regime_context")),
        )
        row = {
            "as_of": as_of.isoformat(),
            "base_method": "limited_adjustment",
            "target_method": target_method,
            "base_weights": base_weights,
            "previous_smoothed_weights": previous,
            "smoothed_weights": result["smoothed_weights"],
            "smoothing_window_days": result["effective_policy"]["smoothing_window_days"],
            "alpha": result["effective_policy"]["alpha"],
            "regime_context": result["regime_context"],
            "research_target_only": True,
            "not_official_target_weights": True,
            "broker_action_allowed": False,
            **SYSTEM_TARGET_SAFETY,
        }
        target_rows.append(row)
        smoothing_events.extend(_records(result.get("smoothing_events")))
        lag_events.extend(_records(result.get("lag_events")))
    _require(
        {row.get("target_method") for row in target_rows} == set(SMOOTHED_METHOD_TO_VARIANT),
        "smoothed target methods incomplete",
    )
    for row in target_rows:
        weights = _mapping(row.get("smoothed_weights"))
        _require(
            weights
            and all(
                _finite(value) and float(value) >= 0.0 for value in weights.values()
            ),
            "smoothed weights must be finite and non-negative",
        )
        _require(
            abs(sum(float(value) for value in weights.values()) - 1.0) <= 1e-8,
            "smoothed weights must sum to one",
        )
    summary = {
        "schema_version": 1,
        "report_type": "etf_dynamic_v3_smoothed_weight_jump_reduction_summary",
        "status": "PASS",
        **legacy._smoothed_weight_jump_reduction_summary(target_rows),
        **SYSTEM_TARGET_SAFETY,
    }
    manifest = {
        "schema_version": 1,
        "report_type": "etf_dynamic_v3_smoothed_limited_manifest",
        "smoothed_id": smoothed_id,
        "target_id": snapshot.get("target_id"),
        "generated_at": snapshot.get("generated_at"),
        "as_of": as_of.isoformat(),
        "status": "PASS",
        "base_method": "limited_adjustment",
        "target_methods": [row["target_method"] for row in target_rows],
        "config_path": str(_config_path(_mapping(snapshot.get("config_binding")))),
        "policy_id": _mapping(config.get("policy_metadata")).get("policy_id"),
        "smoothed_target_input_snapshot_path": str(
            output_dir / "smoothed_target_input_snapshot.json"
        ),
        "smoothed_limited_manifest_path": str(output_dir / "smoothed_limited_manifest.json"),
        "smoothed_target_weights_path": str(output_dir / "smoothed_target_weights.jsonl"),
        "smoothing_events_path": str(output_dir / "smoothing_events.jsonl"),
        "lag_events_path": str(output_dir / "lag_events.jsonl"),
        "weight_jump_reduction_summary_path": str(
            output_dir / "weight_jump_reduction_summary.json"
        ),
        "smoothed_limited_report_path": str(output_dir / "smoothed_limited_report.md"),
        **SYSTEM_TARGET_SAFETY,
    }
    report = legacy.render_smoothed_limited_report(manifest, target_rows, summary, lag_events)
    views = {
        "smoothed_target_input_snapshot.json": _json_bytes(dict(snapshot)),
        "smoothed_limited_manifest.json": _json_bytes(manifest),
        "smoothed_target_weights.jsonl": _jsonl_bytes(target_rows),
        "smoothing_events.jsonl": _jsonl_bytes(smoothing_events),
        "lag_events.jsonl": _jsonl_bytes(lag_events),
        "weight_jump_reduction_summary.json": _json_bytes(summary),
        "smoothed_limited_report.md": report.encode("utf-8"),
    }
    return views, {
        "manifest": manifest,
        "smoothed_target_weights": target_rows,
        "smoothing_events": smoothing_events,
        "lag_events": lag_events,
        "weight_jump_reduction_summary": summary,
    }


def _validate_target_snapshot(snapshot: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    try:
        _require(
            snapshot.get("schema_version") == SMOOTHED_TARGET_SNAPSHOT_SCHEMA,
            "smoothed target snapshot schema invalid",
        )
        source = _mapping(snapshot.get("model_target_source"))
        errors.extend(target_core._validate_artifact_binding(source))
        config_binding = _mapping(snapshot.get("config_binding"))
        model_binding = _mapping(snapshot.get("model_config_binding"))
        errors.extend(target_core._validate_config_binding(config_binding))
        errors.extend(target_core._validate_config_binding(model_binding))
        validation = validate_smoothed_limited_config(
            _config_path(config_binding), model_config_path=_config_path(model_binding)
        )
        if validation != _mapping(snapshot.get("config_validation")):
            errors.append("smoothed config validation drift")
        generated = target_core._datetime(
            snapshot.get("generated_at"), field="smoothed generated_at"
        )
        manifest = _bundle_json(source, "model_target_manifest.json")
        source_generated = target_core._datetime(
            manifest.get("generated_at"), field="model target generated_at"
        )
        source_as_of = target_core._date(manifest.get("as_of"), field="model target as_of")
        _require(source_generated <= generated, "model target chronology invalid")
        _require(source_as_of <= generated.date(), "model target cutoff invalid")
        allowed_contexts = {
            "normal",
            *_mapping(_mapping(config_binding.get("payload")).get("regime_context")),
        }
        _require(snapshot.get("regime_context") in allowed_contexts, "regime context invalid")
    except Exception as exc:  # noqa: BLE001
        errors.append(str(exc))
    return errors


def generate_smoothed_limited_target(
    *,
    target_id: str,
    config_path: Path = DEFAULT_SMOOTHED_LIMITED_CONFIG_PATH,
    model_target_dir: Path = DEFAULT_MODEL_TARGET_DIR,
    output_dir: Path = DEFAULT_SMOOTHED_LIMITED_DIR,
    regime_context: str = "normal",
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = _generated_at(generated_at)
    snapshot = _target_snapshot(
        target_id=target_id,
        model_target_dir=model_target_dir,
        config_path=config_path,
        regime_context=regime_context,
        generated=generated,
    )
    smoothed_id = _stable_id("smoothed-limited", snapshot)
    root = _unique_dir(output_dir / smoothed_id)
    views, payload = _target_views(snapshot, smoothed_id=root.name, output_dir=root)
    _write(root, views, "latest_smoothed_limited", "smoothed_limited_manifest.json")
    return {"smoothed_id": root.name, "smoothed_dir": root, **payload}


def smoothed_limited_report_payload(
    *,
    smoothed_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_SMOOTHED_LIMITED_DIR,
) -> dict[str, Any]:
    root = hardening._artifact_dir_from_latest(
        output_dir=output_dir,
        artifact_id=smoothed_id if not latest else None,
        pointer_name="latest_smoothed_limited",
    )
    return {
        **_read_json(root / "smoothed_limited_manifest.json"),
        "smoothed_target_weights": _read_jsonl(root / "smoothed_target_weights.jsonl"),
        "smoothing_events": _read_jsonl(root / "smoothing_events.jsonl"),
        "lag_events": _read_jsonl(root / "lag_events.jsonl"),
        "weight_jump_reduction_summary": _read_json(
            root / "weight_jump_reduction_summary.json"
        ),
        "smoothed_dir": str(root),
        "input_snapshot": _read_json(root / "smoothed_target_input_snapshot.json"),
    }


def validate_smoothed_limited_artifact(
    *, smoothed_id: str, output_dir: Path = DEFAULT_SMOOTHED_LIMITED_DIR
) -> dict[str, Any]:
    root = output_dir / smoothed_id
    snapshot = legacy._read_optional_json(root / "smoothed_target_input_snapshot.json") or {}
    errors = _validate_target_snapshot(snapshot)
    mismatches: list[str] = []
    try:
        views, _ = _target_views(snapshot, smoothed_id=smoothed_id, output_dir=root)
        mismatches = _view_errors(root, views)
    except Exception as exc:  # noqa: BLE001
        errors.append(str(exc))
    return _validation_payload(
        "etf_dynamic_v3_smoothed_limited_validation",
        smoothed_id,
        [
            _check("snapshot_and_live_inputs", not errors, "; ".join(errors)),
            _check("content_derived_views", not mismatches, ",".join(mismatches)),
            _check(
                "research_safety",
                _mapping(
                    legacy._read_optional_json(root / "smoothed_limited_manifest.json") or {}
                ).get("production_effect")
                == "none",
                "production_effect=none",
            ),
        ],
        artifact_id_key="smoothed_id",
    )


def _backfill_views(
    snapshot: Mapping[str, Any], *, smoothed_backfill_id: str, output_dir: Path
) -> tuple[dict[str, bytes], dict[str, Any]]:
    source = _mapping(snapshot.get("source_paper_shadow_backfill"))
    source_manifest = _bundle_json(source, "paper_shadow_backfill_manifest.json")
    all_states = _bundle_jsonl(source, "backfill_method_states.jsonl")
    all_ledger = _bundle_jsonl(source, "backfill_trade_ledger.jsonl")
    methods = set(SMOOTHED_METHOD_TO_VARIANT)
    states = [row for row in all_states if row.get("target_method") in methods]
    ledger = [row for row in all_ledger if row.get("target_method") in methods]
    _require(states and ledger, "smoothed backfill states and ledger are required")
    _require(
        {row.get("target_method") for row in states} == methods,
        "smoothed backfill methods incomplete",
    )
    state_keys = [(row.get("target_method"), row.get("date")) for row in states]
    _require(len(state_keys) == len(set(state_keys)), "duplicate smoothed state method/date")
    ledger_keys = [(row.get("target_method"), row.get("date")) for row in ledger]
    _require(len(ledger_keys) == len(set(ledger_keys)), "duplicate smoothed ledger method/date")
    smoothing_events = [event for row in ledger for event in _records(row.get("smoothing_events"))]
    lag_events = [event for row in ledger for event in _records(row.get("lag_events"))]
    summary = {
        "schema_version": 1,
        "report_type": "etf_dynamic_v3_smoothed_backfill_summary",
        "status": "PASS",
        **legacy._smoothed_backfill_summary(
            source_manifest, states, ledger, smoothing_events, lag_events
        ),
        **SYSTEM_TARGET_SAFETY,
    }
    manifest = {
        "schema_version": 1,
        "report_type": "etf_dynamic_v3_smoothed_backfill_manifest",
        "smoothed_backfill_id": smoothed_backfill_id,
        "source_paper_shadow_backfill_id": source.get("artifact_id"),
        "generated_at": snapshot.get("generated_at"),
        "status": "PASS",
        "methods": list(SMOOTHED_METHOD_TO_VARIANT),
        "market_regime": source_manifest.get("market_regime"),
        "date_start": source_manifest.get("date_start"),
        "date_end": source_manifest.get("date_end"),
        "smoothed_backfill_input_snapshot_path": str(
            output_dir / "smoothed_backfill_input_snapshot.json"
        ),
        "smoothed_backfill_manifest_path": str(
            output_dir / "smoothed_backfill_manifest.json"
        ),
        "smoothed_method_states_path": str(output_dir / "smoothed_method_states.jsonl"),
        "smoothed_trade_ledger_path": str(output_dir / "smoothed_trade_ledger.jsonl"),
        "smoothed_backfill_summary_path": str(
            output_dir / "smoothed_backfill_summary.json"
        ),
        "smoothed_backfill_report_path": str(output_dir / "smoothed_backfill_report.md"),
        **SYSTEM_TARGET_SAFETY,
    }
    report = legacy.render_smoothed_backfill_report(manifest, summary)
    views = {
        "smoothed_backfill_input_snapshot.json": _json_bytes(dict(snapshot)),
        "smoothed_backfill_manifest.json": _json_bytes(manifest),
        "smoothed_method_states.jsonl": _jsonl_bytes(states),
        "smoothed_trade_ledger.jsonl": _jsonl_bytes(ledger),
        "smoothed_backfill_summary.json": _json_bytes(summary),
        "smoothed_backfill_report.md": report.encode("utf-8"),
    }
    return views, {
        "manifest": manifest,
        "smoothed_method_states": states,
        "smoothed_trade_ledger": ledger,
        "smoothed_backfill_summary": summary,
        "smoothing_events": smoothing_events,
        "lag_events": lag_events,
    }


def _validate_backfill_snapshot(snapshot: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    try:
        _require(
            snapshot.get("schema_version") == SMOOTHED_BACKFILL_SNAPSHOT_SCHEMA,
            "smoothed backfill snapshot schema invalid",
        )
        source = _mapping(snapshot.get("source_paper_shadow_backfill"))
        errors.extend(history._validate_history_binding(source))
        generated = target_core._datetime(
            snapshot.get("generated_at"), field="smoothed backfill generated_at"
        )
        source_manifest = _bundle_json(source, "paper_shadow_backfill_manifest.json")
        source_generated = target_core._datetime(
            source_manifest.get("generated_at"), field="paper backfill generated_at"
        )
        _require(source_generated <= generated, "paper backfill chronology invalid")
        _require(
            source_manifest.get("data_quality_status") in {"PASS", "PASS_WITH_WARNINGS"},
            "paper backfill data quality invalid",
        )
        _require(
            target_core._date(source_manifest.get("date_end"), field="backfill date_end")
            <= generated.date(),
            "paper backfill date_end after smoothed cutoff",
        )
    except Exception as exc:  # noqa: BLE001
        errors.append(str(exc))
    return errors


def run_smoothed_backfill(
    *,
    config_path: Path = DEFAULT_PAPER_SHADOW_BACKFILL_CONFIG_PATH,
    output_dir: Path = DEFAULT_SMOOTHED_BACKFILL_DIR,
    paper_shadow_backfill_dir: Path = DEFAULT_PAPER_SHADOW_BACKFILL_DIR,
    price_cache_path: Path | None = None,
    rates_cache_path: Path = DEFAULT_RATES_CACHE_PATH,
    risk_capped_backfill_dir: Path | None = None,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = _generated_at(generated_at)
    source_backfill = history.run_paper_shadow_backfill(
        config_path=config_path,
        output_dir=paper_shadow_backfill_dir,
        price_cache_path=price_cache_path,
        rates_cache_path=rates_cache_path,
        generated_at=generated,
    )
    source = history._backfill_binding(source_backfill["backfill_id"], paper_shadow_backfill_dir)
    snapshot = {
        "schema_version": SMOOTHED_BACKFILL_SNAPSHOT_SCHEMA,
        "generated_at": generated.isoformat(),
        "source_paper_shadow_backfill": source,
        "orchestration": "explicit wrapper generated and validated canonical paper backfill",
        "production_effect": "none",
    }
    errors = _validate_backfill_snapshot(snapshot)
    _require(not errors, "; ".join(errors))
    backfill_id = _stable_id("smoothed-backfill", snapshot)
    root = _unique_dir(output_dir / backfill_id)
    views, payload = _backfill_views(
        snapshot, smoothed_backfill_id=root.name, output_dir=root
    )
    _write(root, views, "latest_smoothed_backfill", "smoothed_backfill_manifest.json")
    risk_output_dir = risk_capped_backfill_dir or output_dir.parent / "risk_capped_backfill"
    risk_snapshot = {
        "schema_version": risk_capped.RISK_CAPPED_BACKFILL_SNAPSHOT_SCHEMA,
        "generated_at": generated.isoformat(),
        "source_paper_shadow_backfill": source,
        "orchestration": "smoothed wrapper projected the same validated canonical paper backfill",
        "production_effect": "none",
    }
    risk_errors = risk_capped._validate_backfill_snapshot(risk_snapshot)
    _require(not risk_errors, "; ".join(risk_errors))
    risk_backfill_id = _stable_id("risk-capped-backfill", risk_snapshot)
    risk_root = _unique_dir(risk_output_dir / risk_backfill_id)
    risk_views, risk_payload = risk_capped._backfill_views(
        risk_snapshot,
        risk_capped_backfill_id=risk_root.name,
        output_dir=risk_root,
    )
    risk_capped._write(
        risk_root,
        risk_views,
        "latest_risk_capped_backfill",
        "risk_capped_backfill_manifest.json",
    )
    return {
        "smoothed_backfill_id": root.name,
        "smoothed_backfill_dir": root,
        "source_paper_shadow_backfill": source_backfill,
        "source_risk_capped_backfill": {
            "risk_capped_backfill_id": risk_root.name,
            "risk_capped_backfill_dir": risk_root,
            **risk_payload,
        },
        **payload,
    }


def smoothed_backfill_report_payload(
    *,
    backfill_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_SMOOTHED_BACKFILL_DIR,
) -> dict[str, Any]:
    root = hardening._artifact_dir_from_latest(
        output_dir=output_dir,
        artifact_id=backfill_id if not latest else None,
        pointer_name="latest_smoothed_backfill",
    )
    return {
        **_read_json(root / "smoothed_backfill_manifest.json"),
        "smoothed_method_states": _read_jsonl(root / "smoothed_method_states.jsonl"),
        "smoothed_trade_ledger": _read_jsonl(root / "smoothed_trade_ledger.jsonl"),
        "smoothed_backfill_summary": _read_json(root / "smoothed_backfill_summary.json"),
        "smoothed_backfill_dir": str(root),
        "input_snapshot": _read_json(root / "smoothed_backfill_input_snapshot.json"),
    }


def validate_smoothed_backfill_artifact(
    *, backfill_id: str, output_dir: Path = DEFAULT_SMOOTHED_BACKFILL_DIR
) -> dict[str, Any]:
    root = output_dir / backfill_id
    snapshot = legacy._read_optional_json(root / "smoothed_backfill_input_snapshot.json") or {}
    errors = _validate_backfill_snapshot(snapshot)
    mismatches: list[str] = []
    try:
        views, _ = _backfill_views(
            snapshot, smoothed_backfill_id=backfill_id, output_dir=root
        )
        mismatches = _view_errors(root, views)
    except Exception as exc:  # noqa: BLE001
        errors.append(str(exc))
    return _validation_payload(
        "etf_dynamic_v3_smoothed_backfill_validation",
        backfill_id,
        [
            _check("snapshot_and_live_inputs", not errors, "; ".join(errors)),
            _check("content_derived_views", not mismatches, ",".join(mismatches)),
            _check(
                "research_safety",
                _mapping(
                    legacy._read_optional_json(root / "smoothed_backfill_manifest.json") or {}
                ).get("production_effect")
                == "none",
                "production_effect=none",
            ),
        ],
        artifact_id_key="backfill_id",
    )


def _validate_custom_binding(
    binding: Mapping[str, Any],
    *,
    kind: str,
    validator: Callable[..., dict[str, Any]],
    validator_key: str,
) -> list[str]:
    errors = hardening._validate_operations_source_bundle(_mapping(binding.get("bundle")))
    try:
        _require(binding.get("kind") == kind, f"{kind} binding kind invalid")
        artifact_id = _text(binding.get("artifact_id"))
        source_dir = Path(_text(_mapping(binding.get("bundle")).get("source_dir")))
        actual = cached_artifact_validation(
            validator=validator,
            validator_key=validator_key,
            artifact_id=artifact_id,
            root=source_dir.parent,
        )
        if actual != _mapping(binding.get("validation")):
            errors.append(f"{kind} source validation drift")
    except Exception as exc:  # noqa: BLE001
        errors.append(str(exc))
    return errors


def _smoothed_backfill_binding(backfill_id: str, root: Path) -> dict[str, Any]:
    return hardening._source_binding(
        kind="smoothed_backfill",
        artifact_id=backfill_id,
        artifact_root=root,
        validator=validate_smoothed_backfill_artifact,
        validator_key="backfill_id",
        json_views=(
            "smoothed_backfill_manifest.json",
            "smoothed_backfill_summary.json",
        ),
        jsonl_views=("smoothed_method_states.jsonl", "smoothed_trade_ledger.jsonl"),
        text_views=("smoothed_backfill_report.md",),
    )


def _validate_smoothed_backfill_binding(binding: Mapping[str, Any]) -> list[str]:
    return _validate_custom_binding(
        binding,
        kind="smoothed_backfill",
        validator=validate_smoothed_backfill_artifact,
        validator_key="backfill_id",
    )


def _method_rows(
    rows: Sequence[Mapping[str, Any]], method: str
) -> list[dict[str, Any]]:
    selected = [dict(row) for row in rows if row.get("target_method") == method]
    keys = [_text(row.get("date")) for row in selected]
    _require(all(keys), f"{method} contains missing dates")
    _require(len(keys) == len(set(keys)), f"{method} contains duplicate dates")
    return sorted(selected, key=lambda row: _text(row.get("date")))


def _paired_method_rows(
    left: Sequence[Mapping[str, Any]], right: Sequence[Mapping[str, Any]]
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    left_by_date = {_text(row.get("date")): dict(row) for row in left}
    right_by_date = {_text(row.get("date")): dict(row) for row in right}
    dates = sorted(set(left_by_date) & set(right_by_date))
    return [left_by_date[day] for day in dates], [right_by_date[day] for day in dates]


def _comparison_conclusion(values: Mapping[str, Any], policy: Mapping[str, Any]) -> str:
    if values.get("evidence_status") != "PASS":
        return "INSUFFICIENT_DATA"
    return_delta = float(values["total_return_delta"])
    drawdown_delta = float(values["max_drawdown_delta"])
    turnover_delta = float(values["turnover_delta"])
    jump_delta = float(values["large_jump_count_delta"])
    if (
        return_delta >= float(policy["acceptable_return_delta_floor"])
        and drawdown_delta >= float(policy["drawdown_improvement_floor"])
        and turnover_delta <= float(policy["turnover_improvement_ceiling"])
        and jump_delta <= float(policy["jump_improvement_ceiling"])
    ):
        return "smoothed_better"
    if (
        return_delta < float(policy["acceptable_return_delta_floor"])
        and drawdown_delta < float(policy["drawdown_improvement_floor"])
    ):
        return "limited_better"
    return "mixed"


def _comparison_metrics(
    smoothed_states: Sequence[Mapping[str, Any]],
    baseline_states: Sequence[Mapping[str, Any]],
    risk_states: Sequence[Mapping[str, Any]],
    policy: Mapping[str, Any],
) -> dict[str, Any]:
    references = {
        "limited_adjustment": _method_rows(baseline_states, "limited_adjustment"),
        "risk_capped_limited_adjustment": _method_rows(
            risk_states, "risk_capped_limited_adjustment"
        ),
        "static_baseline": _method_rows(baseline_states, "static_baseline"),
        "no_trade_baseline": _method_rows(baseline_states, "no_trade_baseline"),
        "consensus_target": _method_rows(baseline_states, "consensus_target"),
        "defensive_limited_adjustment": _method_rows(
            baseline_states, "defensive_limited_adjustment"
        ),
    }
    minimum = int(policy["minimum_path_observations"])
    comparisons: list[dict[str, Any]] = []
    for method in SMOOTHED_METHOD_TO_VARIANT:
        method_rows = _method_rows(smoothed_states, method)
        for reference, reference_rows in references.items():
            left, right = _paired_method_rows(method_rows, reference_rows)
            sample_count = len(left)
            if sample_count < minimum:
                values: dict[str, Any] = {
                    "method_a": method,
                    "method_b": reference,
                    "sample_count": sample_count,
                    "evidence_status": "INSUFFICIENT_DATA",
                    "total_return_delta": None,
                    "annualized_return_delta": None,
                    "max_drawdown_delta": None,
                    "realized_volatility_delta": None,
                    "turnover_delta": None,
                    "large_jump_count_delta": None,
                    "rolling_consistency_delta": None,
                }
            else:
                method_metrics = legacy._state_path_metrics(left, min_observations=minimum)
                reference_metrics = legacy._state_path_metrics(right, min_observations=minimum)
                values = {
                    "method_a": method,
                    "method_b": reference,
                    "sample_count": sample_count,
                    "evidence_status": "PASS",
                    "total_return_delta": round(
                        float(method_metrics["total_return"])
                        - float(reference_metrics["total_return"]),
                        10,
                    ),
                    "annualized_return_delta": round(
                        float(method_metrics["annualized_return"])
                        - float(reference_metrics["annualized_return"]),
                        10,
                    ),
                    "max_drawdown_delta": round(
                        float(method_metrics["max_drawdown"])
                        - float(reference_metrics["max_drawdown"]),
                        10,
                    ),
                    "realized_volatility_delta": round(
                        float(method_metrics["realized_volatility"])
                        - float(reference_metrics["realized_volatility"]),
                        10,
                    ),
                    "turnover_delta": round(
                        float(method_metrics["turnover"])
                        - float(reference_metrics["turnover"]),
                        10,
                    ),
                    "large_jump_count_delta": legacy._large_jump_count(left)
                    - legacy._large_jump_count(right),
                    "rolling_consistency_delta": None,
                }
                _require(
                    all(
                        _finite(value)
                        for key, value in values.items()
                        if key.endswith("_delta") and key != "rolling_consistency_delta"
                    ),
                    "smoothed comparison metrics must be finite",
                )
            values["conclusion"] = _comparison_conclusion(values, policy)
            comparisons.append(values)
    return {
        "comparisons": comparisons,
        "evaluation_policy": dict(policy),
        **SYSTEM_TARGET_SAFETY,
    }


def _sanitize_regime(
    payload: dict[str, Any], policy: Mapping[str, Any]
) -> dict[str, Any]:
    minimum = int(policy["minimum_regime_observations"])
    for raw in payload.get("regimes", []):
        _require(isinstance(raw, dict), "regime row must be a mapping")
        row = raw
        count = int(row.get("sample_count") or 0)
        if count < minimum:
            for prefix in ("smooth_3d", "smooth_5d"):
                row[f"{prefix}_return_delta_vs_limited"] = None
                row[f"{prefix}_drawdown_delta_vs_limited"] = None
                row[f"{prefix}_turnover_delta_vs_limited"] = None
                row[f"{prefix}_conclusion"] = "INSUFFICIENT_DATA"
            if row.get("regime") == "strong_recovery":
                row["smooth_3d_lag_cost"] = None
                row["smooth_5d_lag_cost"] = None
                row["lag_status"] = "INSUFFICIENT_DATA"
            continue
        for prefix in ("smooth_3d", "smooth_5d"):
            values = {
                "evidence_status": "PASS",
                "total_return_delta": row[f"{prefix}_return_delta_vs_limited"],
                "max_drawdown_delta": row[f"{prefix}_drawdown_delta_vs_limited"],
                "turnover_delta": row[f"{prefix}_turnover_delta_vs_limited"],
                "large_jump_count_delta": 0.0,
            }
            row[f"{prefix}_conclusion"] = _comparison_conclusion(values, policy)
    return payload


def _lag_status(value: float | None, policy: Mapping[str, Any]) -> str:
    if value is None:
        return "INSUFFICIENT_DATA"
    if value <= float(policy["lag_cost_high_threshold"]):
        return "HIGH"
    if value <= float(policy["lag_cost_medium_threshold"]):
        return "MEDIUM"
    return "LOW"


def _comparison_views(
    snapshot: Mapping[str, Any], *, comparison_id: str, output_dir: Path
) -> tuple[dict[str, bytes], dict[str, Any]]:
    smoothed_source = _mapping(snapshot.get("smoothed_backfill_source"))
    baseline_source = _mapping(snapshot.get("baseline_backfill_source"))
    risk_source = _mapping(snapshot.get("risk_capped_backfill_source"))
    smoothed_states = _bundle_jsonl(smoothed_source, "smoothed_method_states.jsonl")
    smoothed_ledger = _bundle_jsonl(smoothed_source, "smoothed_trade_ledger.jsonl")
    baseline_states = _bundle_jsonl(baseline_source, "backfill_method_states.jsonl")
    risk_states = _bundle_jsonl(risk_source, "risk_capped_method_states.jsonl")
    baseline_manifest = _bundle_json(baseline_source, "paper_shadow_backfill_manifest.json")
    baseline = {
        **baseline_manifest,
        "backfill_method_states": baseline_states,
        "backfill_trade_ledger": _bundle_jsonl(
            baseline_source, "backfill_trade_ledger.jsonl"
        ),
    }
    combined_states = [
        *[
            row
            for row in baseline_states
            if row.get("target_method")
            not in {*SMOOTHED_METHOD_TO_VARIANT, "risk_capped_limited_adjustment"}
        ],
        *risk_states,
        *smoothed_states,
    ]
    policy = _evaluation_policy(
        _mapping(_mapping(snapshot.get("policy_binding")).get("payload"))
    )
    metrics = _comparison_metrics(smoothed_states, baseline_states, risk_states, policy)
    regime = _sanitize_regime(
        legacy._smoothed_regime_comparison(combined_states, baseline), policy
    )
    rolling = legacy._smoothed_rolling_comparison(combined_states, baseline)
    for raw in rolling.get("methods", []):
        _require(isinstance(raw, dict), "rolling row must be a mapping")
        window_count = int(raw.get("rolling_windows_total") or 0)
        if window_count < int(
            policy["minimum_rolling_windows"]
        ):
            raw["top_3_frequency_delta_vs_limited"] = None
            raw["bottom_3_frequency_delta_vs_limited"] = None
            raw["rolling_consistency_delta"] = "INSUFFICIENT_DATA"
        elif raw.get("rolling_consistency_delta") == "INSUFFICIENT_DATA":
            # The legacy helper used all-zero rank frequencies as a proxy for
            # missing data. With a non-empty reviewed window inventory, zero
            # deltas instead mean no observed rank change.
            raw["rolling_consistency_delta"] = "MIXED"
    stability = legacy._smoothed_stability_comparison(smoothed_states, baseline_states)
    lag_cost = legacy._smoothed_lag_cost_analysis(combined_states, smoothed_ledger, baseline)
    strong = next(
        (
            row
            for row in _records(regime.get("regimes"))
            if row.get("regime") == "strong_recovery"
        ),
        {},
    )
    strong_count = int(strong.get("sample_count") or 0)
    for raw in lag_cost.get("methods", []):
        _require(isinstance(raw, dict), "lag row must be a mapping")
        raw["sample_count"] = strong_count
        if strong_count < int(policy["minimum_regime_observations"]):
            raw["strong_recovery_lag_cost"] = None
            raw["fast_regime_change_lag_cost"] = None
            raw["missed_upside_count"] = None
            raw["lag_cost_status"] = "INSUFFICIENT_DATA"
        else:
            raw["lag_cost_status"] = _lag_status(
                float(raw["strong_recovery_lag_cost"]), policy
            )
    smoothed_manifest = _bundle_json(smoothed_source, "smoothed_backfill_manifest.json")
    risk_manifest = _bundle_json(risk_source, "risk_capped_backfill_manifest.json")
    manifest = {
        "schema_version": 1,
        "report_type": "etf_dynamic_v3_smoothed_comparison_manifest",
        "comparison_id": comparison_id,
        "smoothed_backfill_id": smoothed_source.get("artifact_id"),
        "baseline_backfill_id": baseline_source.get("artifact_id"),
        "risk_capped_backfill_id": risk_source.get("artifact_id"),
        "generated_at": snapshot.get("generated_at"),
        "status": "PASS",
        "date_start": baseline_manifest.get("date_start"),
        "date_end": baseline_manifest.get("date_end"),
        "same_backfill_lineage": (
            smoothed_manifest.get("source_paper_shadow_backfill_id")
            == baseline_source.get("artifact_id")
            == risk_manifest.get("source_paper_shadow_backfill_id")
        ),
        "policy_id": _mapping(
            _mapping(_mapping(snapshot.get("policy_binding")).get("payload")).get(
                "policy_metadata"
            )
        ).get("policy_id"),
        "smoothed_comparison_input_snapshot_path": str(
            output_dir / "smoothed_comparison_input_snapshot.json"
        ),
        "smoothed_comparison_manifest_path": str(
            output_dir / "smoothed_comparison_manifest.json"
        ),
        "smoothed_vs_limited_metrics_path": str(
            output_dir / "smoothed_vs_limited_metrics.json"
        ),
        "smoothed_regime_comparison_path": str(
            output_dir / "smoothed_regime_comparison.json"
        ),
        "smoothed_rolling_comparison_path": str(
            output_dir / "smoothed_rolling_comparison.json"
        ),
        "smoothed_stability_comparison_path": str(
            output_dir / "smoothed_stability_comparison.json"
        ),
        "smoothing_lag_cost_analysis_path": str(
            output_dir / "smoothing_lag_cost_analysis.json"
        ),
        "smoothed_comparison_report_path": str(
            output_dir / "smoothed_comparison_report.md"
        ),
        **SYSTEM_TARGET_SAFETY,
    }
    report = legacy.render_smoothed_comparison_report(
        manifest, metrics, regime, rolling, stability, lag_cost
    )
    views = {
        "smoothed_comparison_input_snapshot.json": _json_bytes(dict(snapshot)),
        "smoothed_comparison_manifest.json": _json_bytes(manifest),
        "smoothed_vs_limited_metrics.json": _json_bytes(metrics),
        "smoothed_regime_comparison.json": _json_bytes(regime),
        "smoothed_rolling_comparison.json": _json_bytes(rolling),
        "smoothed_stability_comparison.json": _json_bytes(stability),
        "smoothing_lag_cost_analysis.json": _json_bytes(lag_cost),
        "smoothed_comparison_report.md": report.encode("utf-8"),
    }
    return views, {
        "manifest": manifest,
        "smoothed_vs_limited_metrics": metrics,
        "smoothed_regime_comparison": regime,
        "smoothed_rolling_comparison": rolling,
        "smoothed_stability_comparison": stability,
        "smoothing_lag_cost_analysis": lag_cost,
    }


def _validate_comparison_snapshot(snapshot: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    try:
        _require(
            snapshot.get("schema_version") == SMOOTHED_COMPARISON_SNAPSHOT_SCHEMA,
            "smoothed comparison snapshot schema invalid",
        )
        smoothed = _mapping(snapshot.get("smoothed_backfill_source"))
        baseline = _mapping(snapshot.get("baseline_backfill_source"))
        risk = _mapping(snapshot.get("risk_capped_backfill_source"))
        errors.extend(_validate_smoothed_backfill_binding(smoothed))
        errors.extend(history._validate_history_binding(baseline))
        errors.extend(risk_capped._validate_risk_backfill_binding(risk))
        policy_binding = _mapping(snapshot.get("policy_binding"))
        errors.extend(target_core._validate_config_binding(policy_binding))
        _policy_metadata(_mapping(policy_binding.get("payload")))
        _evaluation_policy(_mapping(policy_binding.get("payload")))
        smoothed_manifest = _bundle_json(smoothed, "smoothed_backfill_manifest.json")
        baseline_manifest = _bundle_json(baseline, "paper_shadow_backfill_manifest.json")
        risk_manifest = _bundle_json(risk, "risk_capped_backfill_manifest.json")
        _require(
            smoothed_manifest.get("source_paper_shadow_backfill_id")
            == baseline.get("artifact_id")
            == risk_manifest.get("source_paper_shadow_backfill_id"),
            "smoothed/baseline/risk backfill lineage mismatch",
        )
        ranges = {
            (manifest.get("date_start"), manifest.get("date_end"))
            for manifest in (smoothed_manifest, baseline_manifest, risk_manifest)
        }
        _require(len(ranges) == 1, "smoothed comparison date ranges mismatch")
        generated = target_core._datetime(
            snapshot.get("generated_at"), field="smoothed comparison generated_at"
        )
        for manifest in (smoothed_manifest, baseline_manifest, risk_manifest):
            source_generated = target_core._datetime(
                manifest.get("generated_at"), field="comparison source generated_at"
            )
            _require(source_generated <= generated, "comparison source chronology invalid")
    except Exception as exc:  # noqa: BLE001
        errors.append(str(exc))
    return errors


def run_smoothed_comparison(
    *,
    smoothed_backfill_id: str,
    baseline_backfill_id: str,
    risk_capped_backfill_id: str,
    smoothed_backfill_dir: Path = DEFAULT_SMOOTHED_BACKFILL_DIR,
    baseline_backfill_dir: Path = DEFAULT_PAPER_SHADOW_BACKFILL_DIR,
    risk_capped_backfill_dir: Path = DEFAULT_RISK_CAPPED_BACKFILL_DIR,
    output_dir: Path = DEFAULT_SMOOTHED_COMPARISON_DIR,
    config_path: Path = DEFAULT_SMOOTHED_LIMITED_CONFIG_PATH,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = _generated_at(generated_at)
    snapshot = {
        "schema_version": SMOOTHED_COMPARISON_SNAPSHOT_SCHEMA,
        "generated_at": generated.isoformat(),
        "smoothed_backfill_source": _smoothed_backfill_binding(
            smoothed_backfill_id, smoothed_backfill_dir
        ),
        "baseline_backfill_source": history._backfill_binding(
            baseline_backfill_id, baseline_backfill_dir
        ),
        "risk_capped_backfill_source": risk_capped._risk_backfill_binding(
            risk_capped_backfill_id, risk_capped_backfill_dir
        ),
        "policy_binding": target_core._config_binding(
            config_path, kind="smoothed_method_policy"
        ),
        "production_effect": "none",
    }
    errors = _validate_comparison_snapshot(snapshot)
    _require(not errors, "; ".join(errors))
    comparison_id = _stable_id("smoothed-comparison", snapshot)
    root = _unique_dir(output_dir / comparison_id)
    views, payload = _comparison_views(snapshot, comparison_id=root.name, output_dir=root)
    _write(root, views, "latest_smoothed_comparison", "smoothed_comparison_manifest.json")
    return {"comparison_id": root.name, "comparison_dir": root, **payload}


def smoothed_comparison_report_payload(
    *,
    comparison_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_SMOOTHED_COMPARISON_DIR,
) -> dict[str, Any]:
    root = hardening._artifact_dir_from_latest(
        output_dir=output_dir,
        artifact_id=comparison_id if not latest else None,
        pointer_name="latest_smoothed_comparison",
    )
    return {
        **_read_json(root / "smoothed_comparison_manifest.json"),
        "smoothed_vs_limited_metrics": _read_json(root / "smoothed_vs_limited_metrics.json"),
        "smoothed_regime_comparison": _read_json(root / "smoothed_regime_comparison.json"),
        "smoothed_rolling_comparison": _read_json(root / "smoothed_rolling_comparison.json"),
        "smoothed_stability_comparison": _read_json(root / "smoothed_stability_comparison.json"),
        "smoothing_lag_cost_analysis": _read_json(root / "smoothing_lag_cost_analysis.json"),
        "comparison_dir": str(root),
        "input_snapshot": _read_json(root / "smoothed_comparison_input_snapshot.json"),
    }


def validate_smoothed_comparison_artifact(
    *, comparison_id: str, output_dir: Path = DEFAULT_SMOOTHED_COMPARISON_DIR
) -> dict[str, Any]:
    root = output_dir / comparison_id
    snapshot = legacy._read_optional_json(root / "smoothed_comparison_input_snapshot.json") or {}
    errors = _validate_comparison_snapshot(snapshot)
    mismatches: list[str] = []
    try:
        views, _ = _comparison_views(snapshot, comparison_id=comparison_id, output_dir=root)
        mismatches = _view_errors(root, views)
    except Exception as exc:  # noqa: BLE001
        errors.append(str(exc))
    manifest = _mapping(
        legacy._read_optional_json(root / "smoothed_comparison_manifest.json") or {}
    )
    return _validation_payload(
        "etf_dynamic_v3_smoothed_comparison_validation",
        comparison_id,
        [
            _check("snapshot_and_live_inputs", not errors, "; ".join(errors)),
            _check("content_derived_views", not mismatches, ",".join(mismatches)),
            _check("same_backfill_lineage", manifest.get("same_backfill_lineage") is True, ""),
        ],
        artifact_id_key="comparison_id",
    )


def _comparison_binding(comparison_id: str, root: Path) -> dict[str, Any]:
    return hardening._source_binding(
        kind="smoothed_comparison",
        artifact_id=comparison_id,
        artifact_root=root,
        validator=validate_smoothed_comparison_artifact,
        validator_key="comparison_id",
        json_views=(
            "smoothed_comparison_manifest.json",
            "smoothed_vs_limited_metrics.json",
            "smoothed_regime_comparison.json",
            "smoothed_rolling_comparison.json",
            "smoothed_stability_comparison.json",
            "smoothing_lag_cost_analysis.json",
        ),
        text_views=("smoothed_comparison_report.md",),
    )


def _candidate_evidence(
    comparison: Mapping[str, Any], method: str, policy: Mapping[str, Any]
) -> dict[str, Any]:
    comparisons = _records(
        _mapping(comparison.get("smoothed_vs_limited_metrics")).get("comparisons")
    )
    primary = next(
        (
            row
            for row in comparisons
            if row.get("method_a") == method and row.get("method_b") == "limited_adjustment"
        ),
        {},
    )
    rolling = next(
        (
            row
            for row in _records(
                _mapping(comparison.get("smoothed_rolling_comparison")).get("methods")
            )
            if row.get("method") == method
        ),
        {},
    )
    stability = next(
        (
            row
            for row in _records(
                _mapping(comparison.get("smoothed_stability_comparison")).get("methods")
            )
            if row.get("method") == method
        ),
        {},
    )
    lag = next(
        (
            row
            for row in _records(
                _mapping(comparison.get("smoothing_lag_cost_analysis")).get("methods")
            )
            if row.get("method") == method
        ),
        {},
    )
    evidence_complete = (
        primary.get("evidence_status") == "PASS"
        and all(
            _finite(primary.get(field))
            for field in (
                "total_return_delta",
                "max_drawdown_delta",
                "turnover_delta",
                "large_jump_count_delta",
            )
        )
        and rolling.get("rolling_consistency_delta")
        not in {None, "", "INSUFFICIENT_DATA"}
        and stability.get("stability_conclusion") not in {None, "", "INSUFFICIENT_DATA"}
        and lag.get("lag_cost_status") not in {None, "", "INSUFFICIENT_DATA"}
    )
    statuses: dict[str, Any]
    if not evidence_complete:
        statuses = {
            "return_preservation": "INSUFFICIENT_DATA",
            "drawdown": "INSUFFICIENT_DATA",
            "turnover": "INSUFFICIENT_DATA",
            "weight_jumps": "INSUFFICIENT_DATA",
            "rolling_consistency": rolling.get(
                "rolling_consistency_delta", "INSUFFICIENT_DATA"
            ),
            "stability": stability.get("stability_conclusion", "INSUFFICIENT_DATA"),
            "lag_risk": lag.get("lag_cost_status", "INSUFFICIENT_DATA"),
        }
        score = None
        promotion_eligible = False
    else:
        return_delta = float(primary["total_return_delta"])
        drawdown_delta = float(primary["max_drawdown_delta"])
        turnover_delta = float(primary["turnover_delta"])
        jump_delta = float(primary["large_jump_count_delta"])
        statuses = {
            "return_preservation": (
                "GOOD"
                if return_delta >= 0.0
                else "ACCEPTABLE"
                if return_delta >= float(policy["acceptable_return_delta_floor"])
                else "POOR"
            ),
            "drawdown": (
                "IMPROVED"
                if drawdown_delta >= float(policy["drawdown_improvement_floor"])
                else "WORSE"
            ),
            "turnover": (
                "IMPROVED"
                if turnover_delta <= float(policy["turnover_improvement_ceiling"])
                else "WORSE"
            ),
            "weight_jumps": (
                "IMPROVED"
                if jump_delta <= float(policy["jump_improvement_ceiling"])
                else "WORSE"
            ),
            "rolling_consistency": rolling.get("rolling_consistency_delta"),
            "stability": stability.get("stability_conclusion"),
            "lag_risk": lag.get("lag_cost_status"),
        }
        criteria = (
            statuses["return_preservation"] in {"GOOD", "ACCEPTABLE"},
            statuses["drawdown"] == "IMPROVED",
            statuses["turnover"] == "IMPROVED",
            statuses["weight_jumps"] == "IMPROVED",
            statuses["rolling_consistency"] in {"IMPROVED", "MIXED"},
            statuses["stability"] in {"IMPROVED", "MIXED"},
            statuses["lag_risk"] in {"LOW", "MEDIUM"},
        )
        score = sum(criteria)
        promotion_eligible = all(criteria)
    return {
        "method": method,
        "evidence_complete": evidence_complete,
        "promotion_eligible": promotion_eligible,
        "criteria_pass_count": score,
        "statuses": statuses,
        "comparison": dict(primary),
        "rolling": dict(rolling),
        "stability": dict(stability),
        "lag": dict(lag),
        **SYSTEM_TARGET_SAFETY,
    }


def _review_decision(
    comparison: Mapping[str, Any], backfill: Mapping[str, Any], policy: Mapping[str, Any]
) -> dict[str, Any]:
    candidates = [
        _candidate_evidence(comparison, method, policy)
        for method in policy["candidate_priority"]
    ]
    priority = {method: index for index, method in enumerate(policy["candidate_priority"])}

    def rank_key(row: Mapping[str, Any]) -> tuple[float, float, int]:
        comparison_row = _mapping(row.get("comparison"))
        return (
            float(row.get("criteria_pass_count") or 0),
            float(comparison_row.get("total_return_delta") or -1.0),
            -priority[_text(row.get("method"))],
        )

    complete = [row for row in candidates if row.get("evidence_complete") is True]
    promotable = [row for row in complete if row.get("promotion_eligible") is True]
    ranked_complete = sorted(complete, key=rank_key, reverse=True)
    ranked_promotable = sorted(promotable, key=rank_key, reverse=True)
    recommended_method: str | None = None
    secondary_method: str | None = None
    if ranked_promotable:
        decision = "PROMOTE_TO_RECOMMENDED_RESEARCH"
        recommended_method = _text(ranked_promotable[0].get("method"))
        if len(ranked_promotable) > 1:
            secondary_method = _text(ranked_promotable[1].get("method"))
    elif not complete:
        decision = "DEFER"
    elif all(
        _mapping(row.get("statuses")).get("return_preservation") == "POOR"
        or _mapping(row.get("statuses")).get("lag_risk") == "HIGH"
        for row in complete
    ):
        decision = "REJECT"
    else:
        decision = "CONTINUE_OBSERVATION"
    summary = _mapping(backfill.get("smoothed_backfill_summary"))
    confidence = (
        "LOW"
        if summary.get("data_quality") == "PASS_WITH_WARNINGS"
        or decision != "PROMOTE_TO_RECOMMENDED_RESEARCH"
        else "MEDIUM"
    )
    top_statuses = (
        _mapping(ranked_complete[0].get("statuses")) if ranked_complete else {}
    )
    return {
        "candidate_methods": list(policy["candidate_priority"]),
        "base_method": "limited_adjustment",
        "recommended_method": recommended_method,
        "secondary_method": secondary_method,
        "observation_candidates": [row.get("method") for row in ranked_complete],
        "decision": decision,
        "decision_confidence": confidence,
        "improvements_vs_limited": dict(top_statuses),
        "lag_risk": top_statuses.get("lag_risk", "INSUFFICIENT_DATA"),
        "candidate_evidence": candidates,
        "evaluation_policy": dict(policy),
        "research_target_only": True,
        "not_official_target_weights": True,
        "broker_action_allowed": False,
        "production_effect": "none",
        "requires_forward_confirmation": True,
        "next_action": (
            "owner_review_then_forward_confirmation"
            if recommended_method
            else "continue_observation_without_method_promotion"
        ),
        **SYSTEM_TARGET_SAFETY,
    }


def _review_views(
    snapshot: Mapping[str, Any], *, review_id: str, output_dir: Path
) -> tuple[dict[str, bytes], dict[str, Any]]:
    comparison_source = _mapping(snapshot.get("comparison_source"))
    backfill_source = _mapping(snapshot.get("smoothed_backfill_source"))
    comparison = {
        **_bundle_json(comparison_source, "smoothed_comparison_manifest.json"),
        "smoothed_vs_limited_metrics": _bundle_json(
            comparison_source, "smoothed_vs_limited_metrics.json"
        ),
        "smoothed_regime_comparison": _bundle_json(
            comparison_source, "smoothed_regime_comparison.json"
        ),
        "smoothed_rolling_comparison": _bundle_json(
            comparison_source, "smoothed_rolling_comparison.json"
        ),
        "smoothed_stability_comparison": _bundle_json(
            comparison_source, "smoothed_stability_comparison.json"
        ),
        "smoothing_lag_cost_analysis": _bundle_json(
            comparison_source, "smoothing_lag_cost_analysis.json"
        ),
    }
    backfill = {
        **_bundle_json(backfill_source, "smoothed_backfill_manifest.json"),
        "smoothed_backfill_summary": _bundle_json(
            backfill_source, "smoothed_backfill_summary.json"
        ),
    }
    config = _mapping(_mapping(snapshot.get("policy_binding")).get("payload"))
    policy = _evaluation_policy(config)
    decision = _review_decision(comparison, backfill, policy)
    decision["review_id"] = review_id
    manifest = {
        "schema_version": 1,
        "report_type": "etf_dynamic_v3_smoothed_review_manifest",
        "review_id": review_id,
        "comparison_id": comparison_source.get("artifact_id"),
        "smoothed_backfill_id": backfill_source.get("artifact_id"),
        "generated_at": snapshot.get("generated_at"),
        "status": "PASS",
        "same_backfill_lineage": True,
        "policy_id": _mapping(config.get("policy_metadata")).get("policy_id"),
        "smoothed_review_input_snapshot_path": str(
            output_dir / "smoothed_review_input_snapshot.json"
        ),
        "smoothed_review_manifest_path": str(output_dir / "smoothed_review_manifest.json"),
        "smoothed_decision_path": str(output_dir / "smoothed_decision.json"),
        "owner_smoothed_checklist_path": str(output_dir / "owner_smoothed_checklist.md"),
        "smoothed_review_report_path": str(output_dir / "smoothed_review_report.md"),
        "reader_brief_section_path": str(output_dir / "reader_brief_section.md"),
        **SYSTEM_TARGET_SAFETY,
    }
    checklist = legacy.render_smoothed_owner_checklist(decision)
    report = legacy.render_smoothed_review_report(manifest, decision, comparison, backfill)
    reader = legacy.render_smoothed_review_reader_brief(decision)
    views = {
        "smoothed_review_input_snapshot.json": _json_bytes(dict(snapshot)),
        "smoothed_review_manifest.json": _json_bytes(manifest),
        "smoothed_decision.json": _json_bytes(decision),
        "owner_smoothed_checklist.md": checklist.encode("utf-8"),
        "smoothed_review_report.md": report.encode("utf-8"),
        "reader_brief_section.md": reader.encode("utf-8"),
    }
    return views, {
        "manifest": manifest,
        "smoothed_decision": decision,
        "reader_brief_section": reader,
    }


def _validate_review_snapshot(snapshot: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    try:
        _require(
            snapshot.get("schema_version") == SMOOTHED_REVIEW_SNAPSHOT_SCHEMA,
            "smoothed review snapshot schema invalid",
        )
        comparison = _mapping(snapshot.get("comparison_source"))
        backfill = _mapping(snapshot.get("smoothed_backfill_source"))
        errors.extend(
            _validate_custom_binding(
                comparison,
                kind="smoothed_comparison",
                validator=validate_smoothed_comparison_artifact,
                validator_key="comparison_id",
            )
        )
        errors.extend(_validate_smoothed_backfill_binding(backfill))
        policy_binding = _mapping(snapshot.get("policy_binding"))
        errors.extend(target_core._validate_config_binding(policy_binding))
        _policy_metadata(_mapping(policy_binding.get("payload")))
        _evaluation_policy(_mapping(policy_binding.get("payload")))
        comparison_manifest = _bundle_json(
            comparison, "smoothed_comparison_manifest.json"
        )
        backfill_manifest = _bundle_json(backfill, "smoothed_backfill_manifest.json")
        _require(
            comparison_manifest.get("smoothed_backfill_id") == backfill.get("artifact_id"),
            "comparison/review smoothed backfill lineage mismatch",
        )
        _require(
            comparison_manifest.get("baseline_backfill_id")
            == backfill_manifest.get("source_paper_shadow_backfill_id"),
            "comparison/review baseline lineage mismatch",
        )
        generated = target_core._datetime(
            snapshot.get("generated_at"), field="smoothed review generated_at"
        )
        for manifest in (comparison_manifest, backfill_manifest):
            source_generated = target_core._datetime(
                manifest.get("generated_at"), field="review source generated_at"
            )
            _require(source_generated <= generated, "review source chronology invalid")
    except Exception as exc:  # noqa: BLE001
        errors.append(str(exc))
    return errors


def build_smoothed_review_pack(
    *,
    comparison_id: str,
    smoothed_backfill_id: str,
    comparison_dir: Path = DEFAULT_SMOOTHED_COMPARISON_DIR,
    smoothed_backfill_dir: Path = DEFAULT_SMOOTHED_BACKFILL_DIR,
    output_dir: Path = DEFAULT_SMOOTHED_REVIEW_DIR,
    config_path: Path = DEFAULT_SMOOTHED_LIMITED_CONFIG_PATH,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = _generated_at(generated_at)
    snapshot = {
        "schema_version": SMOOTHED_REVIEW_SNAPSHOT_SCHEMA,
        "generated_at": generated.isoformat(),
        "comparison_source": _comparison_binding(comparison_id, comparison_dir),
        "smoothed_backfill_source": _smoothed_backfill_binding(
            smoothed_backfill_id, smoothed_backfill_dir
        ),
        "policy_binding": target_core._config_binding(
            config_path, kind="smoothed_method_policy"
        ),
        "production_effect": "none",
    }
    errors = _validate_review_snapshot(snapshot)
    _require(not errors, "; ".join(errors))
    review_id = _stable_id("smoothed-review", snapshot)
    root = _unique_dir(output_dir / review_id)
    views, payload = _review_views(snapshot, review_id=root.name, output_dir=root)
    _write(root, views, "latest_smoothed_review", "smoothed_review_manifest.json")
    return {"review_id": root.name, "review_dir": root, **payload}


def smoothed_review_report_payload(
    *,
    review_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_SMOOTHED_REVIEW_DIR,
) -> dict[str, Any]:
    root = hardening._artifact_dir_from_latest(
        output_dir=output_dir,
        artifact_id=review_id if not latest else None,
        pointer_name="latest_smoothed_review",
    )
    return {
        **_read_json(root / "smoothed_review_manifest.json"),
        "smoothed_decision": _read_json(root / "smoothed_decision.json"),
        "reader_brief_section": (root / "reader_brief_section.md").read_text(
            encoding="utf-8"
        ),
        "review_dir": str(root),
        "input_snapshot": _read_json(root / "smoothed_review_input_snapshot.json"),
    }


def validate_smoothed_review_artifact(
    *, review_id: str, output_dir: Path = DEFAULT_SMOOTHED_REVIEW_DIR
) -> dict[str, Any]:
    root = output_dir / review_id
    snapshot = legacy._read_optional_json(root / "smoothed_review_input_snapshot.json") or {}
    errors = _validate_review_snapshot(snapshot)
    mismatches: list[str] = []
    try:
        views, _ = _review_views(snapshot, review_id=review_id, output_dir=root)
        mismatches = _view_errors(root, views)
    except Exception as exc:  # noqa: BLE001
        errors.append(str(exc))
    decision = _mapping(legacy._read_optional_json(root / "smoothed_decision.json") or {})
    return _validation_payload(
        "etf_dynamic_v3_smoothed_review_validation",
        review_id,
        [
            _check("snapshot_and_live_inputs", not errors, "; ".join(errors)),
            _check("content_derived_views", not mismatches, ",".join(mismatches)),
            _check("research_safety", decision.get("production_effect") == "none", ""),
            _check(
                "method_selection_evidence_driven",
                decision.get("recommended_method") is None
                or any(
                    row.get("method") == decision.get("recommended_method")
                    and row.get("promotion_eligible") is True
                    for row in _records(decision.get("candidate_evidence"))
                ),
                "",
            ),
        ],
        artifact_id_key="review_id",
    )
