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

RISK_CAPPED_CONFIG_SNAPSHOT_SCHEMA = "risk_capped_config_input_snapshot.v2"
RISK_CAPPED_TARGET_SNAPSHOT_SCHEMA = "risk_capped_target_input_snapshot.v2"
RISK_CAPPED_BACKFILL_SNAPSHOT_SCHEMA = "risk_capped_backfill_input_snapshot.v2"
RISK_CAPPED_COMPARISON_SNAPSHOT_SCHEMA = "risk_capped_comparison_input_snapshot.v2"
RISK_CAPPED_REVIEW_SNAPSHOT_SCHEMA = "risk_capped_review_input_snapshot.v2"

DEFAULT_RISK_CAPPED_LIMITED_CONFIG_PATH = legacy.DEFAULT_RISK_CAPPED_LIMITED_CONFIG_PATH
DEFAULT_MODEL_TARGET_CONFIG_PATH = legacy.DEFAULT_MODEL_TARGET_CONFIG_PATH
DEFAULT_PAPER_SHADOW_BACKFILL_CONFIG_PATH = history.DEFAULT_PAPER_SHADOW_BACKFILL_CONFIG_PATH
DEFAULT_RISK_CAPPED_CONFIG_DIR = legacy.DEFAULT_RISK_CAPPED_CONFIG_DIR
DEFAULT_RISK_CAPPED_LIMITED_DIR = legacy.DEFAULT_RISK_CAPPED_LIMITED_DIR
DEFAULT_RISK_CAPPED_BACKFILL_DIR = legacy.DEFAULT_RISK_CAPPED_BACKFILL_DIR
DEFAULT_RISK_CAPPED_COMPARISON_DIR = legacy.DEFAULT_RISK_CAPPED_COMPARISON_DIR
DEFAULT_RISK_CAPPED_REVIEW_DIR = legacy.DEFAULT_RISK_CAPPED_REVIEW_DIR
DEFAULT_MODEL_TARGET_DIR = target_core.DEFAULT_MODEL_TARGET_DIR
DEFAULT_PAPER_SHADOW_BACKFILL_DIR = history.DEFAULT_PAPER_SHADOW_BACKFILL_DIR
DEFAULT_RATES_CACHE_PATH = history.DEFAULT_RATES_CACHE_PATH
SYSTEM_TARGET_SAFETY = history.SYSTEM_TARGET_SAFETY


class DynamicV3RiskCappedError(ValueError):
    """Raised when risk-capped evidence is not reproducible or lineage-safe."""


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise DynamicV3RiskCappedError(message)


def _generated_at(value: datetime | None) -> datetime:
    try:
        return target_core._generated_at(value)
    except Exception as exc:  # noqa: BLE001
        raise DynamicV3RiskCappedError(str(exc)) from exc


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
        return target_core._policy_metadata(payload, name="risk-capped policy")
    except Exception as exc:  # noqa: BLE001
        raise DynamicV3RiskCappedError(str(exc)) from exc


def _evaluation_policy(payload: Mapping[str, Any]) -> dict[str, float]:
    policy = _mapping(payload.get("evaluation_policy"))
    required = {
        "acceptable_return_delta_floor",
        "exposure_change_tolerance",
        "drawdown_improvement_floor",
    }
    _require(set(policy) == required, "risk-capped evaluation_policy fields must be exact")
    for field in sorted(required):
        _require(_finite(policy.get(field)), f"evaluation_policy.{field} must be finite")
    _require(
        -1.0 <= float(policy["acceptable_return_delta_floor"]) <= 0.0,
        "acceptable_return_delta_floor must be between -1 and 0",
    )
    _require(
        0.0 <= float(policy["exposure_change_tolerance"]) <= 0.1,
        "exposure_change_tolerance must be between 0 and 0.1",
    )
    _require(
        0.0 <= float(policy["drawdown_improvement_floor"]) <= 0.1,
        "drawdown_improvement_floor must be between 0 and 0.1",
    )
    return {field: float(policy[field]) for field in sorted(required)}


def load_risk_capped_limited_config(
    path: Path = DEFAULT_RISK_CAPPED_LIMITED_CONFIG_PATH,
) -> dict[str, Any]:
    payload = legacy._load_yaml_mapping(path)
    legacy._assert_risk_capped_limited_config_safe(payload)
    _policy_metadata(payload)
    _evaluation_policy(payload)
    return payload


def validate_risk_capped_limited_config(
    path: Path = DEFAULT_RISK_CAPPED_LIMITED_CONFIG_PATH,
    *,
    model_config_path: Path = DEFAULT_MODEL_TARGET_CONFIG_PATH,
) -> dict[str, Any]:
    checks: list[dict[str, Any]] = []
    payload: dict[str, Any] = {}
    model_config: dict[str, Any] = {}
    try:
        payload = load_risk_capped_limited_config(path)
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
        caps = _mapping(payload.get("caps"))
        delta_caps = _mapping(payload.get("delta_caps"))
        safety = _mapping(payload.get("safety"))
        model_constraints = legacy._constraints(model_config)
        universe = legacy._risk_capped_symbol_universe(model_config, payload)
        reallocation = _mapping(payload.get("reallocation"))
        destinations = legacy._texts(reallocation.get("excess_weight_destination"))
        max_semi = legacy._float(caps.get("max_semiconductor_weight"))
        model_max_semi = legacy._float(
            model_constraints.get("max_semiconductor_weight"), 1.0
        )
        checks.extend(
            [
                _check("schema_version", payload.get("schema_version") == 1, ""),
                _check(
                    "method_name",
                    method.get("name") == "risk_capped_limited_adjustment",
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
                    "max_semiconductor_not_wider_than_model_target",
                    max_semi <= model_max_semi,
                    f"risk_capped={max_semi};model_target={model_max_semi}",
                ),
                _check(
                    "min_cash_non_negative",
                    legacy._float(caps.get("min_cash_weight")) >= 0.0,
                    _text(caps.get("min_cash_weight")),
                ),
                _check(
                    "caps_within_bounds",
                    legacy._risk_capped_caps_within_bounds(caps, delta_caps),
                    "",
                ),
                _check(
                    "allocation_possible",
                    legacy._risk_capped_allocation_possible(caps, universe),
                    ",".join(sorted(universe)),
                ),
                _check(
                    "contextual_caps_not_relaxed",
                    legacy._risk_capped_contextual_caps_not_relaxed(payload),
                    "",
                ),
                _check(
                    "reallocation_destinations_in_universe",
                    bool(destinations) and all(item.upper() in universe for item in destinations),
                    ",".join(destinations),
                ),
                _check(
                    "fallback_destination_in_universe",
                    _text(reallocation.get("fallback_destination"), "CASH").upper() in universe,
                    _text(reallocation.get("fallback_destination")),
                ),
                _check("policy_metadata_reviewed", bool(_policy_metadata(payload)), ""),
                _check("evaluation_policy_governed", bool(_evaluation_policy(payload)), ""),
                _check("safety_locked", legacy._safety_config_locked(safety), ""),
            ]
        )
    return _validation_payload(
        "etf_dynamic_v3_risk_capped_limited_config_validation",
        "risk_capped_limited_config",
        checks,
        extra={"config_path": str(path), "model_config_path": str(model_config_path)},
    )


def _config_snapshot(
    *, config_path: Path, model_config_path: Path, generated: datetime
) -> dict[str, Any]:
    validation = validate_risk_capped_limited_config(
        config_path, model_config_path=model_config_path
    )
    _require(validation.get("status") == "PASS", "risk-capped config validation failed")
    return {
        "schema_version": RISK_CAPPED_CONFIG_SNAPSHOT_SCHEMA,
        "generated_at": generated.isoformat(),
        "config_binding": target_core._config_binding(config_path, kind="risk_capped_policy"),
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
    normalized = legacy._normalized_risk_capped_config(config)
    manifest = {
        "schema_version": 1,
        "report_type": "etf_dynamic_v3_risk_capped_limited_config_manifest",
        "config_validation_id": config_validation_id,
        "generated_at": snapshot.get("generated_at"),
        "status": validation.get("status"),
        "config_path": str(_config_path(_mapping(snapshot.get("config_binding")))),
        "model_config_path": str(
            _config_path(_mapping(snapshot.get("model_config_binding")))
        ),
        "base_method": _mapping(config.get("method")).get("base_method"),
        "target_method": _mapping(config.get("method")).get("name"),
        "policy_id": _mapping(config.get("policy_metadata")).get("policy_id"),
        "risk_capped_config_input_snapshot_path": str(
            output_dir / "risk_capped_config_input_snapshot.json"
        ),
        "risk_capped_config_manifest_path": str(output_dir / "risk_capped_config_manifest.json"),
        "normalized_risk_capped_config_path": str(
            output_dir / "normalized_risk_capped_config.yaml"
        ),
        "risk_capped_config_report_path": str(output_dir / "risk_capped_config_report.md"),
        **SYSTEM_TARGET_SAFETY,
    }
    report = legacy.render_risk_capped_limited_config_report(
        manifest, normalized, validation
    )
    views = {
        "risk_capped_config_input_snapshot.json": _json_bytes(dict(snapshot)),
        "risk_capped_config_manifest.json": _json_bytes(manifest),
        "normalized_risk_capped_config.yaml": yaml.safe_dump(
            normalized, sort_keys=True, allow_unicode=True
        ).encode("utf-8"),
        "risk_capped_config_report.md": report.encode("utf-8"),
    }
    return views, {"manifest": manifest, "normalized_config": normalized, "validation": validation}


def _validate_config_snapshot(snapshot: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    try:
        _require(
            snapshot.get("schema_version") == RISK_CAPPED_CONFIG_SNAPSHOT_SCHEMA,
            "risk-capped config snapshot schema invalid",
        )
        config_binding = _mapping(snapshot.get("config_binding"))
        model_binding = _mapping(snapshot.get("model_config_binding"))
        errors.extend(target_core._validate_config_binding(config_binding))
        errors.extend(target_core._validate_config_binding(model_binding))
        validation = validate_risk_capped_limited_config(
            _config_path(config_binding), model_config_path=_config_path(model_binding)
        )
        if validation != _mapping(snapshot.get("validation")):
            errors.append("risk-capped config validation drift")
    except Exception as exc:  # noqa: BLE001
        errors.append(str(exc))
    return errors


def build_risk_capped_limited_config_report(
    *,
    config_path: Path = DEFAULT_RISK_CAPPED_LIMITED_CONFIG_PATH,
    model_config_path: Path = DEFAULT_MODEL_TARGET_CONFIG_PATH,
    output_dir: Path = DEFAULT_RISK_CAPPED_CONFIG_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = _generated_at(generated_at)
    snapshot = _config_snapshot(
        config_path=config_path, model_config_path=model_config_path, generated=generated
    )
    config_validation_id = _stable_id("risk-capped-config", snapshot)
    root = _unique_dir(output_dir / config_validation_id)
    views, payload = _config_views(
        snapshot, config_validation_id=root.name, output_dir=root
    )
    _write(
        root,
        views,
        "latest_risk_capped_limited_config",
        "risk_capped_config_manifest.json",
    )
    return {"config_validation_id": root.name, "config_dir": root, **payload}


def risk_capped_limited_config_report_payload(
    *,
    config_validation_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_RISK_CAPPED_CONFIG_DIR,
) -> dict[str, Any]:
    root = hardening._artifact_dir_from_latest(
        output_dir=output_dir,
        artifact_id=config_validation_id if not latest else None,
        pointer_name="latest_risk_capped_limited_config",
    )
    return {
        **_read_json(root / "risk_capped_config_manifest.json"),
        "config_dir": str(root),
        "normalized_config": legacy._load_yaml_mapping(
            root / "normalized_risk_capped_config.yaml"
        ),
        "input_snapshot": _read_json(root / "risk_capped_config_input_snapshot.json"),
    }


def validate_risk_capped_limited_config_report_artifact(
    *,
    config_validation_id: str,
    output_dir: Path = DEFAULT_RISK_CAPPED_CONFIG_DIR,
) -> dict[str, Any]:
    root = output_dir / config_validation_id
    snapshot = legacy._read_optional_json(root / "risk_capped_config_input_snapshot.json") or {}
    errors = _validate_config_snapshot(snapshot)
    mismatches: list[str] = []
    try:
        views, _ = _config_views(
            snapshot,
            config_validation_id=config_validation_id,
            output_dir=root,
        )
        mismatches = _view_errors(root, views)
    except Exception as exc:  # noqa: BLE001
        errors.append(str(exc))
    checks = [
        _check("snapshot_and_live_inputs", not errors, "; ".join(errors)),
        _check("content_derived_views", not mismatches, ",".join(mismatches)),
    ]
    return _validation_payload(
        "etf_dynamic_v3_risk_capped_limited_config_report_validation",
        config_validation_id,
        checks,
        artifact_id_key="config_validation_id",
    )


def _model_target_binding(target_id: str, root: Path) -> dict[str, Any]:
    validation = target_core.validate_model_target_artifact(
        target_id=target_id, output_dir=root
    )
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
    target_binding = _model_target_binding(target_id, model_target_dir)
    manifest = _bundle_json(target_binding, "model_target_manifest.json")
    source_generated = target_core._datetime(
        manifest.get("generated_at"), field="model target generated_at"
    )
    source_as_of = target_core._date(manifest.get("as_of"), field="model target as_of")
    _require(source_generated <= generated, "model target generated after risk-capped cutoff")
    _require(source_as_of <= generated.date(), "model target as_of after risk-capped cutoff")
    model_config_path = legacy._resolve_project_path(
        manifest.get("config_path"), DEFAULT_MODEL_TARGET_CONFIG_PATH
    )
    config_validation = validate_risk_capped_limited_config(
        config_path, model_config_path=model_config_path
    )
    _require(config_validation.get("status") == "PASS", "risk-capped config validation failed")
    config = load_risk_capped_limited_config(config_path)
    allowed_contexts = {"normal", *_mapping(config.get("contextual_caps"))}
    _require(regime_context in allowed_contexts, "risk-capped regime_context invalid")
    return {
        "schema_version": RISK_CAPPED_TARGET_SNAPSHOT_SCHEMA,
        "generated_at": generated.isoformat(),
        "target_id": target_id,
        "regime_context": regime_context,
        "model_target_source": target_binding,
        "config_binding": target_core._config_binding(
            config_path, kind="risk_capped_policy"
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
    snapshot: Mapping[str, Any], *, risk_capped_id: str, output_dir: Path
) -> tuple[dict[str, bytes], dict[str, Any]]:
    target_binding = _mapping(snapshot.get("model_target_source"))
    target_manifest = _bundle_json(target_binding, "model_target_manifest.json")
    model_target_weights = _bundle_json(target_binding, "model_target_weights.json")
    method_weights = _mapping(model_target_weights.get("method_weights"))
    base_weights = target_core._weights(
        method_weights.get("limited_adjustment"), field="limited_adjustment"
    )
    previous_weights = target_core._weights(
        method_weights.get("static_baseline"), field="static_baseline"
    )
    config = _mapping(_mapping(snapshot.get("config_binding")).get("payload"))
    model_config = _mapping(_mapping(snapshot.get("model_config_binding")).get("payload"))
    as_of = target_core._date(target_manifest.get("as_of"), field="model target as_of")
    cap_result = legacy._apply_risk_capped_limited_adjustment(
        as_of=as_of,
        base_weights=base_weights,
        previous_weights=previous_weights,
        risk_config=config,
        model_config=model_config,
        regime_context=_text(snapshot.get("regime_context")),
    )
    target_row = {
        "as_of": as_of.isoformat(),
        "base_method": "limited_adjustment",
        "target_method": "risk_capped_limited_adjustment",
        "base_weights": base_weights,
        "previous_weights": previous_weights,
        "capped_weights": cap_result["capped_weights"],
        "active_caps": cap_result["active_caps"],
        "regime_context": cap_result["regime_context"],
        "research_target_only": True,
        "not_official_target_weights": True,
        "broker_action_allowed": False,
        **SYSTEM_TARGET_SAFETY,
    }
    manifest = {
        "schema_version": 1,
        "report_type": "etf_dynamic_v3_risk_capped_limited_manifest",
        "risk_capped_id": risk_capped_id,
        "target_id": snapshot.get("target_id"),
        "generated_at": snapshot.get("generated_at"),
        "as_of": as_of.isoformat(),
        "status": cap_result["cap_reason_summary"]["cap_status"],
        "base_method": "limited_adjustment",
        "target_method": "risk_capped_limited_adjustment",
        "config_path": str(_config_path(_mapping(snapshot.get("config_binding")))),
        "policy_id": _mapping(config.get("policy_metadata")).get("policy_id"),
        "risk_capped_target_input_snapshot_path": str(
            output_dir / "risk_capped_target_input_snapshot.json"
        ),
        "risk_capped_limited_manifest_path": str(
            output_dir / "risk_capped_limited_manifest.json"
        ),
        "risk_capped_target_weights_path": str(
            output_dir / "risk_capped_target_weights.jsonl"
        ),
        "cap_events_path": str(output_dir / "cap_events.jsonl"),
        "reallocation_events_path": str(output_dir / "reallocation_events.jsonl"),
        "cap_reason_summary_path": str(output_dir / "cap_reason_summary.json"),
        "risk_capped_limited_report_path": str(
            output_dir / "risk_capped_limited_report.md"
        ),
        **SYSTEM_TARGET_SAFETY,
    }
    cap_events = _records(cap_result.get("cap_events"))
    reallocation_events = _records(cap_result.get("reallocation_events"))
    summary = _mapping(cap_result.get("cap_reason_summary"))
    report = legacy.render_risk_capped_limited_report(manifest, target_row, summary)
    views = {
        "risk_capped_target_input_snapshot.json": _json_bytes(dict(snapshot)),
        "risk_capped_limited_manifest.json": _json_bytes(manifest),
        "risk_capped_target_weights.jsonl": _jsonl_bytes([target_row]),
        "cap_events.jsonl": _jsonl_bytes(cap_events),
        "reallocation_events.jsonl": _jsonl_bytes(reallocation_events),
        "cap_reason_summary.json": _json_bytes(summary),
        "risk_capped_limited_report.md": report.encode("utf-8"),
    }
    return views, {
        "manifest": manifest,
        "risk_capped_target_weights": [target_row],
        "cap_events": cap_events,
        "reallocation_events": reallocation_events,
        "cap_reason_summary": summary,
    }


def _validate_target_snapshot(snapshot: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    try:
        _require(
            snapshot.get("schema_version") == RISK_CAPPED_TARGET_SNAPSHOT_SCHEMA,
            "risk-capped target snapshot schema invalid",
        )
        source = _mapping(snapshot.get("model_target_source"))
        errors.extend(target_core._validate_artifact_binding(source))
        config_binding = _mapping(snapshot.get("config_binding"))
        model_binding = _mapping(snapshot.get("model_config_binding"))
        errors.extend(target_core._validate_config_binding(config_binding))
        errors.extend(target_core._validate_config_binding(model_binding))
        validation = validate_risk_capped_limited_config(
            _config_path(config_binding), model_config_path=_config_path(model_binding)
        )
        if validation != _mapping(snapshot.get("config_validation")):
            errors.append("risk-capped config validation drift")
        generated = target_core._datetime(
            snapshot.get("generated_at"), field="risk-capped generated_at"
        )
        manifest = _bundle_json(source, "model_target_manifest.json")
        source_generated = target_core._datetime(
            manifest.get("generated_at"), field="model target generated_at"
        )
        source_as_of = target_core._date(manifest.get("as_of"), field="model target as_of")
        _require(source_generated <= generated, "model target chronology invalid")
        _require(source_as_of <= generated.date(), "model target cutoff invalid")
        config = _mapping(config_binding.get("payload"))
        allowed_contexts = {"normal", *_mapping(config.get("contextual_caps"))}
        _require(snapshot.get("regime_context") in allowed_contexts, "regime context invalid")
    except Exception as exc:  # noqa: BLE001
        errors.append(str(exc))
    return errors


def generate_risk_capped_limited_target(
    *,
    target_id: str,
    config_path: Path = DEFAULT_RISK_CAPPED_LIMITED_CONFIG_PATH,
    model_target_dir: Path = DEFAULT_MODEL_TARGET_DIR,
    output_dir: Path = DEFAULT_RISK_CAPPED_LIMITED_DIR,
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
    risk_capped_id = _stable_id("risk-capped-limited", snapshot)
    root = _unique_dir(output_dir / risk_capped_id)
    views, payload = _target_views(snapshot, risk_capped_id=root.name, output_dir=root)
    _write(root, views, "latest_risk_capped_limited", "risk_capped_limited_manifest.json")
    return {"risk_capped_id": root.name, "risk_capped_dir": root, **payload}


def risk_capped_limited_report_payload(
    *,
    risk_capped_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_RISK_CAPPED_LIMITED_DIR,
) -> dict[str, Any]:
    root = hardening._artifact_dir_from_latest(
        output_dir=output_dir,
        artifact_id=risk_capped_id if not latest else None,
        pointer_name="latest_risk_capped_limited",
    )
    return {
        **_read_json(root / "risk_capped_limited_manifest.json"),
        "risk_capped_target_weights": _read_jsonl(root / "risk_capped_target_weights.jsonl"),
        "cap_events": _read_jsonl(root / "cap_events.jsonl"),
        "reallocation_events": _read_jsonl(root / "reallocation_events.jsonl"),
        "cap_reason_summary": _read_json(root / "cap_reason_summary.json"),
        "risk_capped_dir": str(root),
        "input_snapshot": _read_json(root / "risk_capped_target_input_snapshot.json"),
    }


def validate_risk_capped_limited_artifact(
    *, risk_capped_id: str, output_dir: Path = DEFAULT_RISK_CAPPED_LIMITED_DIR
) -> dict[str, Any]:
    root = output_dir / risk_capped_id
    snapshot = legacy._read_optional_json(root / "risk_capped_target_input_snapshot.json") or {}
    errors = _validate_target_snapshot(snapshot)
    mismatches: list[str] = []
    try:
        views, _ = _target_views(snapshot, risk_capped_id=risk_capped_id, output_dir=root)
        mismatches = _view_errors(root, views)
    except Exception as exc:  # noqa: BLE001
        errors.append(str(exc))
    checks = [
        _check("snapshot_and_live_inputs", not errors, "; ".join(errors)),
        _check("content_derived_views", not mismatches, ",".join(mismatches)),
        _check(
            "research_safety",
            _mapping(
                legacy._read_optional_json(root / "risk_capped_limited_manifest.json") or {}
            ).get("production_effect")
            == "none",
            "production_effect=none",
        ),
    ]
    return _validation_payload(
        "etf_dynamic_v3_risk_capped_limited_validation",
        risk_capped_id,
        checks,
        artifact_id_key="risk_capped_id",
    )


def _backfill_views(
    snapshot: Mapping[str, Any], *, risk_capped_backfill_id: str, output_dir: Path
) -> tuple[dict[str, bytes], dict[str, Any]]:
    source = _mapping(snapshot.get("source_paper_shadow_backfill"))
    source_manifest = _bundle_json(source, "paper_shadow_backfill_manifest.json")
    all_states = _bundle_jsonl(source, "backfill_method_states.jsonl")
    all_ledger = _bundle_jsonl(source, "backfill_trade_ledger.jsonl")
    states = [
        row
        for row in all_states
        if row.get("target_method") == "risk_capped_limited_adjustment"
    ]
    ledger = [
        row
        for row in all_ledger
        if row.get("target_method") == "risk_capped_limited_adjustment"
    ]
    _require(bool(states), "risk-capped states missing")
    _require(bool(ledger), "risk-capped ledger missing")
    cap_events = [event for row in ledger for event in _records(row.get("cap_events"))]
    reallocations = [
        event for row in ledger for event in _records(row.get("reallocation_events"))
    ]
    summary = {
        "schema_version": 1,
        "report_type": "etf_dynamic_v3_risk_capped_backfill_summary",
        "status": "PASS",
        **legacy._risk_capped_backfill_summary(
            source_manifest, states, ledger, cap_events, reallocations
        ),
        **SYSTEM_TARGET_SAFETY,
    }
    manifest = {
        "schema_version": 1,
        "report_type": "etf_dynamic_v3_risk_capped_backfill_manifest",
        "risk_capped_backfill_id": risk_capped_backfill_id,
        "source_paper_shadow_backfill_id": source.get("artifact_id"),
        "generated_at": snapshot.get("generated_at"),
        "status": "PASS",
        "method": "risk_capped_limited_adjustment",
        "market_regime": source_manifest.get("market_regime"),
        "date_start": summary.get("date_start"),
        "date_end": summary.get("date_end"),
        "risk_capped_backfill_input_snapshot_path": str(
            output_dir / "risk_capped_backfill_input_snapshot.json"
        ),
        "risk_capped_backfill_manifest_path": str(
            output_dir / "risk_capped_backfill_manifest.json"
        ),
        "risk_capped_method_states_path": str(output_dir / "risk_capped_method_states.jsonl"),
        "risk_capped_trade_ledger_path": str(output_dir / "risk_capped_trade_ledger.jsonl"),
        "risk_capped_backfill_summary_path": str(
            output_dir / "risk_capped_backfill_summary.json"
        ),
        "risk_capped_backfill_report_path": str(
            output_dir / "risk_capped_backfill_report.md"
        ),
        **SYSTEM_TARGET_SAFETY,
    }
    report = legacy.render_risk_capped_backfill_report(manifest, summary)
    views = {
        "risk_capped_backfill_input_snapshot.json": _json_bytes(dict(snapshot)),
        "risk_capped_backfill_manifest.json": _json_bytes(manifest),
        "risk_capped_method_states.jsonl": _jsonl_bytes(states),
        "risk_capped_trade_ledger.jsonl": _jsonl_bytes(ledger),
        "risk_capped_backfill_summary.json": _json_bytes(summary),
        "risk_capped_backfill_report.md": report.encode("utf-8"),
    }
    return views, {
        "manifest": manifest,
        "risk_capped_method_states": states,
        "risk_capped_trade_ledger": ledger,
        "risk_capped_backfill_summary": summary,
        "cap_events": cap_events,
        "reallocation_events": reallocations,
    }


def _validate_backfill_snapshot(snapshot: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    try:
        _require(
            snapshot.get("schema_version") == RISK_CAPPED_BACKFILL_SNAPSHOT_SCHEMA,
            "risk-capped backfill snapshot schema invalid",
        )
        source = _mapping(snapshot.get("source_paper_shadow_backfill"))
        errors.extend(history._validate_history_binding(source))
        generated = target_core._datetime(
            snapshot.get("generated_at"), field="risk-capped backfill generated_at"
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
    except Exception as exc:  # noqa: BLE001
        errors.append(str(exc))
    return errors


def run_risk_capped_backfill(
    *,
    config_path: Path = DEFAULT_PAPER_SHADOW_BACKFILL_CONFIG_PATH,
    output_dir: Path = DEFAULT_RISK_CAPPED_BACKFILL_DIR,
    paper_shadow_backfill_dir: Path = DEFAULT_PAPER_SHADOW_BACKFILL_DIR,
    price_cache_path: Path | None = None,
    rates_cache_path: Path = DEFAULT_RATES_CACHE_PATH,
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
        "schema_version": RISK_CAPPED_BACKFILL_SNAPSHOT_SCHEMA,
        "generated_at": generated.isoformat(),
        "source_paper_shadow_backfill": source,
        "orchestration": "explicit wrapper generated and validated canonical paper backfill",
        "production_effect": "none",
    }
    risk_capped_backfill_id = _stable_id("risk-capped-backfill", snapshot)
    root = _unique_dir(output_dir / risk_capped_backfill_id)
    views, payload = _backfill_views(
        snapshot, risk_capped_backfill_id=root.name, output_dir=root
    )
    _write(
        root,
        views,
        "latest_risk_capped_backfill",
        "risk_capped_backfill_manifest.json",
    )
    return {
        "risk_capped_backfill_id": root.name,
        "risk_capped_backfill_dir": root,
        "source_paper_shadow_backfill": source_backfill,
        **payload,
    }


def risk_capped_backfill_report_payload(
    *,
    backfill_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_RISK_CAPPED_BACKFILL_DIR,
) -> dict[str, Any]:
    root = hardening._artifact_dir_from_latest(
        output_dir=output_dir,
        artifact_id=backfill_id if not latest else None,
        pointer_name="latest_risk_capped_backfill",
    )
    return {
        **_read_json(root / "risk_capped_backfill_manifest.json"),
        "risk_capped_method_states": _read_jsonl(root / "risk_capped_method_states.jsonl"),
        "risk_capped_trade_ledger": _read_jsonl(root / "risk_capped_trade_ledger.jsonl"),
        "risk_capped_backfill_summary": _read_json(root / "risk_capped_backfill_summary.json"),
        "risk_capped_backfill_dir": str(root),
        "input_snapshot": _read_json(root / "risk_capped_backfill_input_snapshot.json"),
    }


def validate_risk_capped_backfill_artifact(
    *, backfill_id: str, output_dir: Path = DEFAULT_RISK_CAPPED_BACKFILL_DIR
) -> dict[str, Any]:
    root = output_dir / backfill_id
    snapshot = legacy._read_optional_json(root / "risk_capped_backfill_input_snapshot.json") or {}
    errors = _validate_backfill_snapshot(snapshot)
    mismatches: list[str] = []
    try:
        views, _ = _backfill_views(
            snapshot, risk_capped_backfill_id=backfill_id, output_dir=root
        )
        mismatches = _view_errors(root, views)
    except Exception as exc:  # noqa: BLE001
        errors.append(str(exc))
    checks = [
        _check("snapshot_and_live_inputs", not errors, "; ".join(errors)),
        _check("content_derived_views", not mismatches, ",".join(mismatches)),
        _check(
            "research_safety",
            _mapping(
                legacy._read_optional_json(root / "risk_capped_backfill_manifest.json") or {}
            ).get("production_effect")
            == "none",
            "production_effect=none",
        ),
    ]
    return _validation_payload(
        "etf_dynamic_v3_risk_capped_backfill_validation",
        backfill_id,
        checks,
        artifact_id_key="backfill_id",
    )


def _risk_backfill_binding(backfill_id: str, root: Path) -> dict[str, Any]:
    return hardening._source_binding(
        kind="risk_capped_backfill",
        artifact_id=backfill_id,
        artifact_root=root,
        validator=validate_risk_capped_backfill_artifact,
        validator_key="backfill_id",
        json_views=(
            "risk_capped_backfill_manifest.json",
            "risk_capped_backfill_summary.json",
        ),
        jsonl_views=("risk_capped_method_states.jsonl", "risk_capped_trade_ledger.jsonl"),
        text_views=("risk_capped_backfill_report.md",),
    )


def _validate_risk_backfill_binding(binding: Mapping[str, Any]) -> list[str]:
    return _validate_custom_binding(
        binding,
        kind="risk_capped_backfill",
        validator=validate_risk_capped_backfill_artifact,
        validator_key="backfill_id",
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
        actual = validator(**{validator_key: artifact_id, "output_dir": source_dir.parent})
        if actual != _mapping(binding.get("validation")):
            errors.append(f"{kind} source validation drift")
    except Exception as exc:  # noqa: BLE001
        errors.append(str(exc))
    return errors


def _comparison_conclusion(values: Mapping[str, Any], policy: Mapping[str, Any]) -> str:
    drawdown_improved = float(values["max_drawdown_delta"]) > float(
        policy["drawdown_improvement_floor"]
    )
    semi_reduced = float(values["avg_semiconductor_weight_delta"]) < -float(
        policy["exposure_change_tolerance"]
    )
    return_preserved = float(values["total_return_delta"]) >= float(
        policy["acceptable_return_delta_floor"]
    )
    if drawdown_improved and semi_reduced and return_preserved:
        return "risk_capped_better"
    if not drawdown_improved and not semi_reduced:
        return "limited_better"
    return "mixed"


def _comparison_views(
    snapshot: Mapping[str, Any], *, comparison_id: str, output_dir: Path
) -> tuple[dict[str, bytes], dict[str, Any]]:
    risk_source = _mapping(snapshot.get("risk_capped_backfill_source"))
    baseline_source = _mapping(snapshot.get("baseline_backfill_source"))
    risk_states = _bundle_jsonl(risk_source, "risk_capped_method_states.jsonl")
    baseline_states = _bundle_jsonl(baseline_source, "backfill_method_states.jsonl")
    baseline = {
        **_bundle_json(baseline_source, "paper_shadow_backfill_manifest.json"),
        "backfill_method_states": baseline_states,
        "backfill_trade_ledger": _bundle_jsonl(
            baseline_source, "backfill_trade_ledger.jsonl"
        ),
    }
    # The canonical baseline backfill already tracks the risk-capped method.
    # The dedicated wrapper is its auditable projection, so comparison must
    # replace—not append to—the baseline copy or regime/rolling samples would
    # double-count the same observations.
    combined_states = [
        *[
            row
            for row in baseline_states
            if row.get("target_method") != "risk_capped_limited_adjustment"
        ],
        *risk_states,
    ]
    policy = _evaluation_policy(
        _mapping(_mapping(snapshot.get("policy_binding")).get("payload"))
    )
    metrics = legacy._risk_capped_vs_limited_metrics(risk_states, baseline_states)
    values = _mapping(metrics.get("metrics"))
    _require(all(_finite(value) for value in values.values()), "comparison metrics must be finite")
    metrics["conclusion"] = _comparison_conclusion(values, policy)
    metrics["policy"] = policy
    regime = legacy._risk_capped_regime_comparison(combined_states, baseline)
    for raw_row in regime.get("regimes", []):
        _require(isinstance(raw_row, dict), "regime comparison row must be a mapping")
        row = raw_row
        if int(row.get("sample_count") or 0) == 0:
            row["conclusion"] = "insufficient_data"
            row["return_delta_vs_limited"] = None
            row["drawdown_delta_vs_limited"] = None
            row["win_rate_vs_limited"] = None
        elif (
            float(row["drawdown_delta_vs_limited"])
            > float(policy["drawdown_improvement_floor"])
            and float(row["return_delta_vs_limited"])
            >= float(policy["acceptable_return_delta_floor"])
        ):
            row["conclusion"] = "risk_capped_better"
        elif float(row["drawdown_delta_vs_limited"]) < 0.0 and float(
            row["return_delta_vs_limited"]
        ) < 0.0:
            row["conclusion"] = "limited_better"
        else:
            row["conclusion"] = "mixed"
    rolling = legacy._risk_capped_rolling_comparison(combined_states, baseline)
    stability = legacy._risk_capped_stability_comparison(risk_states, baseline_states)
    risk_manifest = _bundle_json(risk_source, "risk_capped_backfill_manifest.json")
    baseline_manifest = _bundle_json(
        baseline_source, "paper_shadow_backfill_manifest.json"
    )
    manifest = {
        "schema_version": 1,
        "report_type": "etf_dynamic_v3_risk_capped_comparison_manifest",
        "comparison_id": comparison_id,
        "risk_capped_backfill_id": risk_source.get("artifact_id"),
        "baseline_backfill_id": baseline_source.get("artifact_id"),
        "generated_at": snapshot.get("generated_at"),
        "status": "PASS",
        "date_start": baseline_manifest.get("date_start"),
        "date_end": baseline_manifest.get("date_end"),
        "same_backfill_lineage": (
            risk_manifest.get("source_paper_shadow_backfill_id")
            == baseline_source.get("artifact_id")
        ),
        "policy_id": _mapping(
            _mapping(_mapping(snapshot.get("policy_binding")).get("payload")).get(
                "policy_metadata"
            )
        ).get("policy_id"),
        "risk_capped_comparison_input_snapshot_path": str(
            output_dir / "risk_capped_comparison_input_snapshot.json"
        ),
        "risk_capped_comparison_manifest_path": str(
            output_dir / "risk_capped_comparison_manifest.json"
        ),
        "risk_capped_vs_limited_metrics_path": str(
            output_dir / "risk_capped_vs_limited_metrics.json"
        ),
        "risk_capped_regime_comparison_path": str(
            output_dir / "risk_capped_regime_comparison.json"
        ),
        "risk_capped_rolling_comparison_path": str(
            output_dir / "risk_capped_rolling_comparison.json"
        ),
        "risk_capped_stability_comparison_path": str(
            output_dir / "risk_capped_stability_comparison.json"
        ),
        "risk_capped_comparison_report_path": str(
            output_dir / "risk_capped_comparison_report.md"
        ),
        **SYSTEM_TARGET_SAFETY,
    }
    report = legacy.render_risk_capped_comparison_report(
        manifest, metrics, regime, rolling, stability
    )
    views = {
        "risk_capped_comparison_input_snapshot.json": _json_bytes(dict(snapshot)),
        "risk_capped_comparison_manifest.json": _json_bytes(manifest),
        "risk_capped_vs_limited_metrics.json": _json_bytes(metrics),
        "risk_capped_regime_comparison.json": _json_bytes(regime),
        "risk_capped_rolling_comparison.json": _json_bytes(rolling),
        "risk_capped_stability_comparison.json": _json_bytes(stability),
        "risk_capped_comparison_report.md": report.encode("utf-8"),
    }
    return views, {
        "manifest": manifest,
        "risk_capped_vs_limited_metrics": metrics,
        "risk_capped_regime_comparison": regime,
        "risk_capped_rolling_comparison": rolling,
        "risk_capped_stability_comparison": stability,
    }


def _validate_comparison_snapshot(snapshot: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    try:
        _require(
            snapshot.get("schema_version") == RISK_CAPPED_COMPARISON_SNAPSHOT_SCHEMA,
            "risk-capped comparison snapshot schema invalid",
        )
        risk = _mapping(snapshot.get("risk_capped_backfill_source"))
        baseline = _mapping(snapshot.get("baseline_backfill_source"))
        errors.extend(_validate_risk_backfill_binding(risk))
        errors.extend(history._validate_history_binding(baseline))
        policy_binding = _mapping(snapshot.get("policy_binding"))
        errors.extend(target_core._validate_config_binding(policy_binding))
        _policy_metadata(_mapping(policy_binding.get("payload")))
        _evaluation_policy(_mapping(policy_binding.get("payload")))
        risk_manifest = _bundle_json(risk, "risk_capped_backfill_manifest.json")
        baseline_manifest = _bundle_json(baseline, "paper_shadow_backfill_manifest.json")
        _require(
            risk_manifest.get("source_paper_shadow_backfill_id") == baseline.get("artifact_id"),
            "risk/baseline backfill lineage mismatch",
        )
        _require(
            risk_manifest.get("date_start") == baseline_manifest.get("date_start")
            and risk_manifest.get("date_end") == baseline_manifest.get("date_end"),
            "risk/baseline date range mismatch",
        )
        generated = target_core._datetime(
            snapshot.get("generated_at"), field="risk comparison generated_at"
        )
        for manifest in (risk_manifest, baseline_manifest):
            source_generated = target_core._datetime(
                manifest.get("generated_at"), field="comparison source generated_at"
            )
            _require(source_generated <= generated, "comparison source chronology invalid")
    except Exception as exc:  # noqa: BLE001
        errors.append(str(exc))
    return errors


def run_risk_capped_comparison(
    *,
    risk_capped_backfill_id: str,
    baseline_backfill_id: str,
    risk_capped_backfill_dir: Path = DEFAULT_RISK_CAPPED_BACKFILL_DIR,
    baseline_backfill_dir: Path = DEFAULT_PAPER_SHADOW_BACKFILL_DIR,
    output_dir: Path = DEFAULT_RISK_CAPPED_COMPARISON_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = _generated_at(generated_at)
    snapshot = {
        "schema_version": RISK_CAPPED_COMPARISON_SNAPSHOT_SCHEMA,
        "generated_at": generated.isoformat(),
        "risk_capped_backfill_source": _risk_backfill_binding(
            risk_capped_backfill_id, risk_capped_backfill_dir
        ),
        "baseline_backfill_source": history._backfill_binding(
            baseline_backfill_id, baseline_backfill_dir
        ),
        "policy_binding": target_core._config_binding(
            DEFAULT_RISK_CAPPED_LIMITED_CONFIG_PATH, kind="risk_capped_policy"
        ),
        "production_effect": "none",
    }
    errors = _validate_comparison_snapshot(snapshot)
    _require(not errors, "; ".join(errors))
    comparison_id = _stable_id("risk-capped-comparison", snapshot)
    root = _unique_dir(output_dir / comparison_id)
    views, payload = _comparison_views(snapshot, comparison_id=root.name, output_dir=root)
    _write(
        root,
        views,
        "latest_risk_capped_comparison",
        "risk_capped_comparison_manifest.json",
    )
    return {"comparison_id": root.name, "comparison_dir": root, **payload}


def risk_capped_comparison_report_payload(
    *,
    comparison_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_RISK_CAPPED_COMPARISON_DIR,
) -> dict[str, Any]:
    root = hardening._artifact_dir_from_latest(
        output_dir=output_dir,
        artifact_id=comparison_id if not latest else None,
        pointer_name="latest_risk_capped_comparison",
    )
    return {
        **_read_json(root / "risk_capped_comparison_manifest.json"),
        "risk_capped_vs_limited_metrics": _read_json(
            root / "risk_capped_vs_limited_metrics.json"
        ),
        "risk_capped_regime_comparison": _read_json(
            root / "risk_capped_regime_comparison.json"
        ),
        "risk_capped_rolling_comparison": _read_json(
            root / "risk_capped_rolling_comparison.json"
        ),
        "risk_capped_stability_comparison": _read_json(
            root / "risk_capped_stability_comparison.json"
        ),
        "comparison_dir": str(root),
        "input_snapshot": _read_json(root / "risk_capped_comparison_input_snapshot.json"),
    }


def validate_risk_capped_comparison_artifact(
    *, comparison_id: str, output_dir: Path = DEFAULT_RISK_CAPPED_COMPARISON_DIR
) -> dict[str, Any]:
    root = output_dir / comparison_id
    snapshot = legacy._read_optional_json(root / "risk_capped_comparison_input_snapshot.json") or {}
    errors = _validate_comparison_snapshot(snapshot)
    mismatches: list[str] = []
    try:
        views, _ = _comparison_views(snapshot, comparison_id=comparison_id, output_dir=root)
        mismatches = _view_errors(root, views)
    except Exception as exc:  # noqa: BLE001
        errors.append(str(exc))
    checks = [
        _check("snapshot_and_live_inputs", not errors, "; ".join(errors)),
        _check("content_derived_views", not mismatches, ",".join(mismatches)),
        _check(
            "same_backfill_lineage",
            _mapping(
                legacy._read_optional_json(root / "risk_capped_comparison_manifest.json") or {}
            ).get("same_backfill_lineage")
            is True,
            "",
        ),
    ]
    return _validation_payload(
        "etf_dynamic_v3_risk_capped_comparison_validation",
        comparison_id,
        checks,
        artifact_id_key="comparison_id",
    )


def _comparison_binding(comparison_id: str, root: Path) -> dict[str, Any]:
    return hardening._source_binding(
        kind="risk_capped_comparison",
        artifact_id=comparison_id,
        artifact_root=root,
        validator=validate_risk_capped_comparison_artifact,
        validator_key="comparison_id",
        json_views=(
            "risk_capped_comparison_input_snapshot.json",
            "risk_capped_comparison_manifest.json",
            "risk_capped_vs_limited_metrics.json",
            "risk_capped_regime_comparison.json",
            "risk_capped_rolling_comparison.json",
            "risk_capped_stability_comparison.json",
        ),
        text_views=("risk_capped_comparison_report.md",),
    )


def _review_decision(
    comparison: Mapping[str, Any], backfill: Mapping[str, Any], policy: Mapping[str, Any]
) -> dict[str, Any]:
    metrics = _mapping(_mapping(comparison.get("risk_capped_vs_limited_metrics")).get("metrics"))
    rolling = _mapping(comparison.get("risk_capped_rolling_comparison"))
    summary = _mapping(backfill.get("risk_capped_backfill_summary"))
    drawdown_delta = float(metrics["max_drawdown_delta"])
    exposure_delta = float(metrics["avg_semiconductor_weight_delta"])
    return_delta = float(metrics["total_return_delta"])
    exposure_tolerance = float(policy["exposure_change_tolerance"])
    return_floor = float(policy["acceptable_return_delta_floor"])
    improvements = {
        "max_drawdown": (
            "IMPROVED"
            if drawdown_delta > float(policy["drawdown_improvement_floor"])
            else "WORSE"
        ),
        "rolling_consistency": rolling.get("stability_delta", "INSUFFICIENT_DATA"),
        "semiconductor_exposure": (
            "REDUCED"
            if exposure_delta < -exposure_tolerance
            else "INCREASED"
            if exposure_delta > exposure_tolerance
            else "UNCHANGED"
        ),
        "return_preservation": (
            "GOOD"
            if return_delta >= 0.0
            else "ACCEPTABLE"
            if return_delta >= return_floor
            else "POOR"
        ),
    }
    if (
        improvements["max_drawdown"] == "IMPROVED"
        and improvements["semiconductor_exposure"] == "REDUCED"
        and improvements["return_preservation"] in {"GOOD", "ACCEPTABLE"}
        and improvements["rolling_consistency"] in {"IMPROVED", "MIXED"}
    ):
        decision = "PROMOTE_TO_RECOMMENDED_RESEARCH"
    elif improvements["return_preservation"] == "POOR":
        decision = "REJECT"
    else:
        decision = "CONTINUE_OBSERVATION"
    confidence = "LOW" if summary.get("data_quality") == "PASS_WITH_WARNINGS" else "MEDIUM"
    return {
        "candidate_method": "risk_capped_limited_adjustment",
        "base_method": "limited_adjustment",
        "decision": decision,
        "decision_confidence": confidence,
        "reason": [
            f"max_drawdown={improvements['max_drawdown']}",
            f"rolling_consistency={improvements['rolling_consistency']}",
            f"semiconductor_exposure={improvements['semiconductor_exposure']}",
            f"return_preservation={improvements['return_preservation']}",
            f"data_quality={summary.get('data_quality')}",
            "research_only_no_broker_no_production",
        ],
        "improvements_vs_limited": improvements,
        "evaluation_policy": dict(policy),
        "research_target_only": True,
        "not_official_target_weights": True,
        "broker_action_allowed": False,
        "production_effect": "none",
        "requires_forward_confirmation": True,
        "next_action": "owner_review_then_forward_confirmation",
        **SYSTEM_TARGET_SAFETY,
    }


def _review_views(
    snapshot: Mapping[str, Any], *, review_id: str, output_dir: Path
) -> tuple[dict[str, bytes], dict[str, Any]]:
    comparison_source = _mapping(snapshot.get("comparison_source"))
    backfill_source = _mapping(snapshot.get("risk_capped_backfill_source"))
    comparison = {
        **_bundle_json(comparison_source, "risk_capped_comparison_manifest.json"),
        "risk_capped_vs_limited_metrics": _bundle_json(
            comparison_source, "risk_capped_vs_limited_metrics.json"
        ),
        "risk_capped_regime_comparison": _bundle_json(
            comparison_source, "risk_capped_regime_comparison.json"
        ),
        "risk_capped_rolling_comparison": _bundle_json(
            comparison_source, "risk_capped_rolling_comparison.json"
        ),
        "risk_capped_stability_comparison": _bundle_json(
            comparison_source, "risk_capped_stability_comparison.json"
        ),
    }
    backfill = {
        **_bundle_json(backfill_source, "risk_capped_backfill_manifest.json"),
        "risk_capped_backfill_summary": _bundle_json(
            backfill_source, "risk_capped_backfill_summary.json"
        ),
    }
    config = _mapping(_mapping(snapshot.get("policy_binding")).get("payload"))
    policy = _evaluation_policy(config)
    decision = _review_decision(comparison, backfill, policy)
    decision["review_id"] = review_id
    manifest = {
        "schema_version": 1,
        "report_type": "etf_dynamic_v3_risk_capped_review_manifest",
        "review_id": review_id,
        "comparison_id": comparison_source.get("artifact_id"),
        "risk_capped_backfill_id": backfill_source.get("artifact_id"),
        "generated_at": snapshot.get("generated_at"),
        "status": "PASS",
        "same_backfill_lineage": True,
        "policy_id": _mapping(config.get("policy_metadata")).get("policy_id"),
        "risk_capped_review_input_snapshot_path": str(
            output_dir / "risk_capped_review_input_snapshot.json"
        ),
        "risk_capped_review_manifest_path": str(
            output_dir / "risk_capped_review_manifest.json"
        ),
        "risk_capped_decision_path": str(output_dir / "risk_capped_decision.json"),
        "owner_risk_capped_checklist_path": str(
            output_dir / "owner_risk_capped_checklist.md"
        ),
        "risk_capped_review_report_path": str(
            output_dir / "risk_capped_review_report.md"
        ),
        "reader_brief_section_path": str(output_dir / "reader_brief_section.md"),
        **SYSTEM_TARGET_SAFETY,
    }
    checklist = legacy.render_risk_capped_owner_checklist(decision)
    report = legacy.render_risk_capped_review_report(
        manifest, decision, comparison, backfill
    )
    reader = legacy.render_risk_capped_review_reader_brief(decision)
    views = {
        "risk_capped_review_input_snapshot.json": _json_bytes(dict(snapshot)),
        "risk_capped_review_manifest.json": _json_bytes(manifest),
        "risk_capped_decision.json": _json_bytes(decision),
        "owner_risk_capped_checklist.md": checklist.encode("utf-8"),
        "risk_capped_review_report.md": report.encode("utf-8"),
        "reader_brief_section.md": reader.encode("utf-8"),
    }
    return views, {
        "manifest": manifest,
        "risk_capped_decision": decision,
        "reader_brief_section": reader,
    }


def _validate_review_snapshot(snapshot: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    try:
        _require(
            snapshot.get("schema_version") == RISK_CAPPED_REVIEW_SNAPSHOT_SCHEMA,
            "risk-capped review snapshot schema invalid",
        )
        comparison = _mapping(snapshot.get("comparison_source"))
        backfill = _mapping(snapshot.get("risk_capped_backfill_source"))
        errors.extend(
            _validate_custom_binding(
                comparison,
                kind="risk_capped_comparison",
                validator=validate_risk_capped_comparison_artifact,
                validator_key="comparison_id",
            )
        )
        errors.extend(_validate_risk_backfill_binding(backfill))
        policy_binding = _mapping(snapshot.get("policy_binding"))
        errors.extend(target_core._validate_config_binding(policy_binding))
        _policy_metadata(_mapping(policy_binding.get("payload")))
        _evaluation_policy(_mapping(policy_binding.get("payload")))
        comparison_manifest = _bundle_json(
            comparison, "risk_capped_comparison_manifest.json"
        )
        backfill_manifest = _bundle_json(backfill, "risk_capped_backfill_manifest.json")
        _require(
            comparison_manifest.get("risk_capped_backfill_id") == backfill.get("artifact_id"),
            "comparison/review backfill lineage mismatch",
        )
        _require(
            comparison_manifest.get("baseline_backfill_id")
            == backfill_manifest.get("source_paper_shadow_backfill_id"),
            "comparison/review baseline lineage mismatch",
        )
        generated = target_core._datetime(
            snapshot.get("generated_at"), field="risk review generated_at"
        )
        for manifest in (comparison_manifest, backfill_manifest):
            source_generated = target_core._datetime(
                manifest.get("generated_at"), field="review source generated_at"
            )
            _require(source_generated <= generated, "review source chronology invalid")
    except Exception as exc:  # noqa: BLE001
        errors.append(str(exc))
    return errors


def build_risk_capped_review_pack(
    *,
    comparison_id: str,
    risk_capped_backfill_id: str,
    comparison_dir: Path = DEFAULT_RISK_CAPPED_COMPARISON_DIR,
    risk_capped_backfill_dir: Path = DEFAULT_RISK_CAPPED_BACKFILL_DIR,
    output_dir: Path = DEFAULT_RISK_CAPPED_REVIEW_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = _generated_at(generated_at)
    snapshot = {
        "schema_version": RISK_CAPPED_REVIEW_SNAPSHOT_SCHEMA,
        "generated_at": generated.isoformat(),
        "comparison_source": _comparison_binding(comparison_id, comparison_dir),
        "risk_capped_backfill_source": _risk_backfill_binding(
            risk_capped_backfill_id, risk_capped_backfill_dir
        ),
        "policy_binding": target_core._config_binding(
            DEFAULT_RISK_CAPPED_LIMITED_CONFIG_PATH, kind="risk_capped_policy"
        ),
        "production_effect": "none",
    }
    errors = _validate_review_snapshot(snapshot)
    _require(not errors, "; ".join(errors))
    review_id = _stable_id("risk-capped-review", snapshot)
    root = _unique_dir(output_dir / review_id)
    views, payload = _review_views(snapshot, review_id=root.name, output_dir=root)
    _write(root, views, "latest_risk_capped_review", "risk_capped_review_manifest.json")
    return {"review_id": root.name, "review_dir": root, **payload}


def risk_capped_review_report_payload(
    *,
    review_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_RISK_CAPPED_REVIEW_DIR,
) -> dict[str, Any]:
    root = hardening._artifact_dir_from_latest(
        output_dir=output_dir,
        artifact_id=review_id if not latest else None,
        pointer_name="latest_risk_capped_review",
    )
    return {
        **_read_json(root / "risk_capped_review_manifest.json"),
        "risk_capped_decision": _read_json(root / "risk_capped_decision.json"),
        "reader_brief_section": (root / "reader_brief_section.md").read_text(
            encoding="utf-8"
        ),
        "review_dir": str(root),
        "input_snapshot": _read_json(root / "risk_capped_review_input_snapshot.json"),
    }


def validate_risk_capped_review_artifact(
    *, review_id: str, output_dir: Path = DEFAULT_RISK_CAPPED_REVIEW_DIR
) -> dict[str, Any]:
    root = output_dir / review_id
    snapshot = legacy._read_optional_json(root / "risk_capped_review_input_snapshot.json") or {}
    errors = _validate_review_snapshot(snapshot)
    mismatches: list[str] = []
    try:
        views, _ = _review_views(snapshot, review_id=review_id, output_dir=root)
        mismatches = _view_errors(root, views)
    except Exception as exc:  # noqa: BLE001
        errors.append(str(exc))
    checks = [
        _check("snapshot_and_live_inputs", not errors, "; ".join(errors)),
        _check("content_derived_views", not mismatches, ",".join(mismatches)),
        _check(
            "research_safety",
            _mapping(legacy._read_optional_json(root / "risk_capped_decision.json") or {}).get(
                "production_effect"
            )
            == "none",
            "production_effect=none",
        ),
    ]
    return _validation_payload(
        "etf_dynamic_v3_risk_capped_review_validation",
        review_id,
        checks,
        artifact_id_key="review_id",
    )
