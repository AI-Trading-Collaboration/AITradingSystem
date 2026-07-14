from __future__ import annotations

import math
import statistics
from collections.abc import Callable, Mapping, Sequence
from datetime import date, datetime
from pathlib import Path
from typing import Any

import pandas as pd
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

HYPOTHESIS_BACKLOG_SNAPSHOT_SCHEMA = "experiment_hypothesis_backlog_input_snapshot.v2"
VARIANT_TRANSFORM_SNAPSHOT_SCHEMA = "experiment_variant_transform_input_snapshot.v2"
EXPERIMENT_MATRIX_SNAPSHOT_SCHEMA = "experiment_matrix_input_snapshot.v2"
BATCH_EXPERIMENT_SNAPSHOT_SCHEMA = "batch_experiment_input_snapshot.v2"
EXPERIMENT_TRIAGE_SNAPSHOT_SCHEMA = "experiment_triage_input_snapshot.v2"
TOP_VARIANT_INTERPRETATION_SNAPSHOT_SCHEMA = "top_variant_interpretation_input_snapshot.v2"
METHOD_PROMOTION_PLAN_SNAPSHOT_SCHEMA = "method_promotion_plan_input_snapshot.v2"

DEFAULT_WEIGHT_OPTIMIZATION_HYPOTHESIS_CONFIG_PATH = (
    legacy.DEFAULT_WEIGHT_OPTIMIZATION_HYPOTHESIS_CONFIG_PATH
)
DEFAULT_WEIGHT_VARIANT_TRANSFORM_CONFIG_PATH = legacy.DEFAULT_WEIGHT_VARIANT_TRANSFORM_CONFIG_PATH
DEFAULT_WEIGHT_EXPERIMENT_MATRIX_CONFIG_PATH = legacy.DEFAULT_WEIGHT_EXPERIMENT_MATRIX_CONFIG_PATH
DEFAULT_HYPOTHESIS_BACKLOG_DIR = legacy.DEFAULT_HYPOTHESIS_BACKLOG_DIR
DEFAULT_VARIANT_TRANSFORM_SPEC_DIR = legacy.DEFAULT_VARIANT_TRANSFORM_SPEC_DIR
DEFAULT_EXPERIMENT_MATRIX_DIR = legacy.DEFAULT_EXPERIMENT_MATRIX_DIR
DEFAULT_BATCH_EXPERIMENT_DIR = legacy.DEFAULT_BATCH_EXPERIMENT_DIR
DEFAULT_EXPERIMENT_TRIAGE_DIR = legacy.DEFAULT_EXPERIMENT_TRIAGE_DIR
DEFAULT_TOP_VARIANT_INTERPRETATION_DIR = legacy.DEFAULT_TOP_VARIANT_INTERPRETATION_DIR
DEFAULT_METHOD_PROMOTION_PLAN_DIR = legacy.DEFAULT_METHOD_PROMOTION_PLAN_DIR
DEFAULT_PAPER_SHADOW_BACKFILL_DIR = history.DEFAULT_PAPER_SHADOW_BACKFILL_DIR
DEFAULT_RATES_CACHE_PATH = history.DEFAULT_RATES_CACHE_PATH
DEFAULT_FAILURE_MODES = legacy.DEFAULT_FAILURE_MODES
DEFAULT_HYPOTHESIS_FAMILIES = legacy.DEFAULT_HYPOTHESIS_FAMILIES
DEFAULT_TRANSFORM_TYPES = legacy.DEFAULT_TRANSFORM_TYPES
DEFAULT_TRIAGE_DECISIONS = legacy.DEFAULT_TRIAGE_DECISIONS
EXPERIMENT_FACTORY_SAFETY = legacy.EXPERIMENT_FACTORY_SAFETY

_TRIAGE_COMPONENTS = (
    "return",
    "drawdown",
    "rolling_consistency",
    "regime",
    "turnover",
    "simplicity",
)
_HARD_REJECT_RULES = (
    "max_drawdown_materially_worse",
    "rolling_consistency_worse",
    "pressure_regime_performance_worse",
    "turnover_explodes",
    "data_quality_FAIL",
    "insufficient_required_metrics",
)

# Numerical equality tolerance only; it does not define an investment-facing
# threshold. A transform must change at least one rebalance target beyond this
# floating-point tolerance before its screening metrics are interpretable.
_WEIGHT_COMPARISON_TOLERANCE = 1e-12


class DynamicV3ExperimentFactoryError(ValueError):
    """Raised when experiment evidence is invalid, stale, or not reproducible."""


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise DynamicV3ExperimentFactoryError(message)


def _generated_at(value: datetime | None) -> datetime:
    try:
        return target_core._generated_at(value)
    except Exception as exc:  # noqa: BLE001
        raise DynamicV3ExperimentFactoryError(str(exc)) from exc


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


def _artifact_root(
    *, output_dir: Path, artifact_id: str | None, latest: bool, pointer_name: str
) -> Path:
    return hardening._artifact_dir_from_latest(
        output_dir=output_dir,
        artifact_id=artifact_id if not latest else None,
        pointer_name=pointer_name,
    )


def _bundle_json(binding: Mapping[str, Any], name: str) -> dict[str, Any]:
    return hardening._bundle_json(binding, name)


def _bundle_jsonl(binding: Mapping[str, Any], name: str) -> list[dict[str, Any]]:
    return hardening._bundle_jsonl(binding, name)


def _binding_path(binding: Mapping[str, Any]) -> Path:
    return target_core._binding_path(binding)


def _policy_metadata(payload: Mapping[str, Any], *, name: str) -> dict[str, Any]:
    try:
        return target_core._policy_metadata(payload, name=name)
    except Exception as exc:  # noqa: BLE001
        raise DynamicV3ExperimentFactoryError(str(exc)) from exc


def _source_binding(
    *,
    kind: str,
    artifact_id: str,
    artifact_dir: Path,
    validation: Mapping[str, Any],
    json_views: Sequence[str] = (),
    jsonl_views: Sequence[str] = (),
    text_views: Sequence[str] = (),
) -> dict[str, Any]:
    return history._history_binding(
        kind=kind,
        artifact_dir=artifact_dir,
        artifact_id=artifact_id,
        validation=validation,
        json_views=json_views,
        jsonl_views=jsonl_views,
        text_views=text_views,
    )


def _source_dir(binding: Mapping[str, Any]) -> Path:
    return Path(_text(_mapping(binding.get("bundle")).get("source_dir")))


def _source_generated(binding: Mapping[str, Any], manifest_name: str, *, field: str) -> datetime:
    return target_core._datetime(
        _bundle_json(binding, manifest_name).get("generated_at"), field=field
    )


def _source_not_after(
    binding: Mapping[str, Any], manifest_name: str, generated: datetime, *, field: str
) -> None:
    _require(
        _source_generated(binding, manifest_name, field=field) <= generated, f"{field} after cutoff"
    )


def _factory_validator(kind: str) -> tuple[Callable[..., dict[str, Any]], str]:
    validators: dict[str, tuple[Callable[..., dict[str, Any]], str]] = {
        "hypothesis_backlog": (validate_hypothesis_backlog_artifact, "backlog_id"),
        "variant_transform_spec": (validate_variant_transform_spec_artifact, "spec_id"),
        "experiment_matrix": (validate_experiment_matrix_artifact, "matrix_id"),
        "batch_experiment": (validate_batch_experiment_artifact, "batch_id"),
        "experiment_triage": (validate_experiment_triage_artifact, "triage_id"),
        "top_variant_interpretation": (
            validate_top_variant_interpretation_artifact,
            "interpretation_id",
        ),
        "method_promotion_plan": (
            validate_method_promotion_plan_artifact,
            "promotion_plan_id",
        ),
        "paper_shadow_backfill": (
            history.validate_paper_shadow_backfill_artifact,
            "backfill_id",
        ),
    }
    _require(kind in validators, f"unknown experiment source kind: {kind}")
    return validators[kind]


def _validate_source_binding(binding: Mapping[str, Any]) -> list[str]:
    errors = history._validate_operations_source_bundle(_mapping(binding.get("bundle")))
    try:
        kind = _text(binding.get("kind"))
        artifact_id = _text(binding.get("artifact_id"))
        validator, key = _factory_validator(kind)
        actual = validator(**{key: artifact_id, "output_dir": _source_dir(binding).parent})
        if actual != _mapping(binding.get("validation")):
            errors.append(f"{kind} source validation drift")
    except Exception as exc:  # noqa: BLE001
        errors.append(str(exc))
    return errors


def _hypothesis_binding(backlog_id: str, output_dir: Path) -> dict[str, Any]:
    validation = validate_hypothesis_backlog_artifact(backlog_id=backlog_id, output_dir=output_dir)
    return _source_binding(
        kind="hypothesis_backlog",
        artifact_id=backlog_id,
        artifact_dir=output_dir / backlog_id,
        validation=validation,
        json_views=(
            "hypothesis_backlog_manifest.json",
            "failure_mode_taxonomy.json",
            "hypothesis_priority_summary.json",
        ),
        jsonl_views=("hypotheses.jsonl",),
    )


def _transform_binding(spec_id: str, output_dir: Path) -> dict[str, Any]:
    validation = validate_variant_transform_spec_artifact(spec_id=spec_id, output_dir=output_dir)
    return _source_binding(
        kind="variant_transform_spec",
        artifact_id=spec_id,
        artifact_dir=output_dir / spec_id,
        validation=validation,
        json_views=("variant_transform_spec_manifest.json", "transform_type_catalog.json"),
        text_views=("normalized_transform_spec.yaml",),
    )


def _matrix_binding(matrix_id: str, output_dir: Path) -> dict[str, Any]:
    validation = validate_experiment_matrix_artifact(matrix_id=matrix_id, output_dir=output_dir)
    return _source_binding(
        kind="experiment_matrix",
        artifact_id=matrix_id,
        artifact_dir=output_dir / matrix_id,
        validation=validation,
        json_views=("experiment_matrix_manifest.json", "transform_specs.json"),
        jsonl_views=("variant_specs.jsonl",),
    )


def _backfill_binding(backfill_id: str, output_dir: Path) -> dict[str, Any]:
    validation = history.validate_paper_shadow_backfill_artifact(
        backfill_id=backfill_id, output_dir=output_dir
    )
    return _source_binding(
        kind="paper_shadow_backfill",
        artifact_id=backfill_id,
        artifact_dir=output_dir / backfill_id,
        validation=validation,
        json_views=(
            "paper_shadow_backfill_manifest.json",
            "backfill_rebalance_calendar.json",
            "backfill_data_quality.json",
        ),
        jsonl_views=("backfill_method_states.jsonl", "backfill_trade_ledger.jsonl"),
    )


def _batch_binding(batch_id: str, output_dir: Path) -> dict[str, Any]:
    validation = validate_batch_experiment_artifact(batch_id=batch_id, output_dir=output_dir)
    return _source_binding(
        kind="batch_experiment",
        artifact_id=batch_id,
        artifact_dir=output_dir / batch_id,
        validation=validation,
        json_views=("batch_experiment_manifest.json",),
        jsonl_views=(
            "variant_weight_paths.jsonl",
            "variant_performance_metrics.jsonl",
            "variant_regime_metrics.jsonl",
            "variant_stability_metrics.jsonl",
        ),
    )


def _triage_binding(triage_id: str, output_dir: Path) -> dict[str, Any]:
    validation = validate_experiment_triage_artifact(triage_id=triage_id, output_dir=output_dir)
    return _source_binding(
        kind="experiment_triage",
        artifact_id=triage_id,
        artifact_dir=output_dir / triage_id,
        validation=validation,
        json_views=("triage_manifest.json", "triage_summary.json"),
        jsonl_views=(
            "variant_scorecard.jsonl",
            "promotion_candidates.jsonl",
            "rejected_variants.jsonl",
        ),
    )


def _interpretation_binding(interpretation_id: str, output_dir: Path) -> dict[str, Any]:
    validation = validate_top_variant_interpretation_artifact(
        interpretation_id=interpretation_id, output_dir=output_dir
    )
    return _source_binding(
        kind="top_variant_interpretation",
        artifact_id=interpretation_id,
        artifact_dir=output_dir / interpretation_id,
        validation=validation,
        json_views=(
            "top_variant_interpretation_manifest.json",
            "variant_failure_mode_coverage.json",
        ),
        jsonl_views=("top_variant_explanations.jsonl",),
    )


def _experiment_safety(payload: Mapping[str, Any]) -> None:
    _require(
        legacy._experiment_safety_config_locked(_mapping(payload.get("safety"))),
        "experiment safety fields invalid",
    )


def _exact_unique_texts(value: Any, *, field: str) -> list[str]:
    values = legacy._texts(value)
    _require(bool(values) and len(values) == len(set(values)), f"{field} missing/duplicate")
    return values


def load_weight_optimization_hypothesis_config(
    path: Path = DEFAULT_WEIGHT_OPTIMIZATION_HYPOTHESIS_CONFIG_PATH,
) -> dict[str, Any]:
    payload = legacy._load_yaml_mapping(path)
    _experiment_safety(payload)
    _policy_metadata(payload, name="weight optimization hypothesis")
    return payload


def validate_weight_optimization_hypothesis_config(
    path: Path = DEFAULT_WEIGHT_OPTIMIZATION_HYPOTHESIS_CONFIG_PATH,
) -> dict[str, Any]:
    checks: list[dict[str, Any]] = []
    try:
        payload = load_weight_optimization_hypothesis_config(path)
        failure_modes = _records(payload.get("failure_modes"))
        hypotheses = _records(payload.get("hypotheses"))
        mode_ids = [_text(row.get("id")) for row in failure_modes]
        families = _exact_unique_texts(payload.get("hypothesis_families"), field="families")
        hypothesis_ids = [_text(row.get("hypothesis_id")) for row in hypotheses]
        _require(payload.get("schema_version") == 1, "hypothesis schema_version invalid")
        _require(bool(failure_modes), "failure modes missing")
        _require(all(mode_ids) and len(mode_ids) == len(set(mode_ids)), "failure modes invalid")
        _require(set(DEFAULT_FAILURE_MODES).issubset(mode_ids), "required failure modes missing")
        _require(set(DEFAULT_HYPOTHESIS_FAMILIES).issubset(families), "families incomplete")
        _require(bool(hypotheses), "hypotheses missing")
        _require(
            all(hypothesis_ids) and len(hypothesis_ids) == len(set(hypothesis_ids)),
            "hypothesis ids missing/duplicate",
        )
        for row in hypotheses:
            _require(
                row.get("base_method") == "limited_adjustment", "hypothesis base method invalid"
            )
            _require(_text(row.get("family")) in families, "unknown hypothesis family")
            modes = _exact_unique_texts(row.get("target_failure_modes"), field="target modes")
            _require(set(modes).issubset(mode_ids), "unknown hypothesis failure mode")
            _require(bool(_text(row.get("description"))), "hypothesis description missing")
            _require(bool(legacy._texts(row.get("expected_benefit"))), "expected benefit missing")
            _require(bool(legacy._texts(row.get("expected_cost"))), "expected cost missing")
            _require(
                _text(row.get("complexity")) in {"LOW", "MEDIUM", "HIGH"}, "complexity invalid"
            )
            _require(_text(row.get("priority")) in {"LOW", "MEDIUM", "HIGH"}, "priority invalid")
            _require(_text(row.get("status")) in {"proposed", "ready_for_matrix"}, "status invalid")
        checks.extend(
            [
                _check("config_and_policy_valid", True, "reviewed source-exact"),
                _check(
                    "taxonomy_and_hypotheses_complete",
                    True,
                    f"{len(failure_modes)}/{len(hypotheses)}",
                ),
                _check("safety_locked", True, "experiment only"),
            ]
        )
    except Exception as exc:  # noqa: BLE001
        checks.append(_check("hypothesis_config_valid", False, str(exc)))
    return _validation_payload(
        "etf_dynamic_v3_weight_optimization_hypothesis_config_validation",
        "weight_optimization_hypothesis_config",
        checks,
        extra={"config_path": str(path)},
    )


def _config_snapshot(
    *, schema: str, kind: str, config_path: Path, validation: Mapping[str, Any], generated: datetime
) -> dict[str, Any]:
    _require(validation.get("status") == "PASS", f"{kind} config validation failed")
    return {
        "schema_version": schema,
        "generated_at": generated.isoformat(),
        "config_binding": target_core._config_binding(config_path, kind=kind),
        "validation": dict(validation),
        "production_effect": "none",
    }


def _validate_single_config_snapshot(
    snapshot: Mapping[str, Any],
    *,
    schema: str,
    validator: Callable[[Path], dict[str, Any]],
) -> list[str]:
    errors: list[str] = []
    try:
        _require(snapshot.get("schema_version") == schema, "config snapshot schema invalid")
        binding = _mapping(snapshot.get("config_binding"))
        errors.extend(target_core._validate_config_binding(binding))
        actual = validator(_binding_path(binding))
        if actual != _mapping(snapshot.get("validation")):
            errors.append("config validation drift")
    except Exception as exc:  # noqa: BLE001
        errors.append(str(exc))
    return errors


def _hypothesis_views(
    snapshot: Mapping[str, Any], *, backlog_id: str, output_dir: Path
) -> tuple[dict[str, bytes], dict[str, Any]]:
    config = _mapping(_mapping(snapshot.get("config_binding")).get("payload"))
    failure_modes = legacy._normalized_failure_modes(config)
    hypotheses = legacy._normalized_hypotheses(config)
    priority = legacy._hypothesis_priority_summary(failure_modes, hypotheses)
    manifest = {
        "schema_version": 1,
        "report_type": "etf_dynamic_v3_hypothesis_backlog_manifest",
        "backlog_id": backlog_id,
        "generated_at": snapshot.get("generated_at"),
        "status": "PASS",
        "config_path": str(_binding_path(_mapping(snapshot.get("config_binding")))),
        "failure_modes_count": len(_records(failure_modes.get("failure_modes"))),
        "hypotheses_count": len(hypotheses),
        "high_priority_hypotheses": priority["high_priority_hypotheses"],
        "hypothesis_backlog_input_snapshot_path": str(
            output_dir / "hypothesis_backlog_input_snapshot.json"
        ),
        "hypothesis_backlog_manifest_path": str(output_dir / "hypothesis_backlog_manifest.json"),
        "failure_mode_taxonomy_path": str(output_dir / "failure_mode_taxonomy.json"),
        "hypotheses_path": str(output_dir / "hypotheses.jsonl"),
        "hypothesis_priority_summary_path": str(output_dir / "hypothesis_priority_summary.json"),
        "hypothesis_backlog_report_path": str(output_dir / "hypothesis_backlog_report.md"),
        **EXPERIMENT_FACTORY_SAFETY,
    }
    report = legacy.render_hypothesis_backlog_report(manifest, failure_modes, hypotheses, priority)
    views = {
        "hypothesis_backlog_input_snapshot.json": _json_bytes(dict(snapshot)),
        "hypothesis_backlog_manifest.json": _json_bytes(manifest),
        "failure_mode_taxonomy.json": _json_bytes(failure_modes),
        "hypotheses.jsonl": _jsonl_bytes(hypotheses),
        "hypothesis_priority_summary.json": _json_bytes(priority),
        "hypothesis_backlog_report.md": report.encode("utf-8"),
    }
    return views, {
        "manifest": manifest,
        "failure_mode_taxonomy": failure_modes,
        "hypotheses": hypotheses,
        "hypothesis_priority_summary": priority,
        "validation": _mapping(snapshot.get("validation")),
    }


def build_hypothesis_backlog(
    *,
    config_path: Path = DEFAULT_WEIGHT_OPTIMIZATION_HYPOTHESIS_CONFIG_PATH,
    output_dir: Path = DEFAULT_HYPOTHESIS_BACKLOG_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = _generated_at(generated_at)
    validation = validate_weight_optimization_hypothesis_config(config_path)
    snapshot = _config_snapshot(
        schema=HYPOTHESIS_BACKLOG_SNAPSHOT_SCHEMA,
        kind="weight_optimization_hypothesis_policy",
        config_path=config_path,
        validation=validation,
        generated=generated,
    )
    backlog_id = _stable_id("hypothesis-backlog", snapshot)
    root = _unique_dir(output_dir / backlog_id)
    views, payload = _hypothesis_views(snapshot, backlog_id=root.name, output_dir=root)
    _write(root, views, "latest_hypothesis_backlog", "hypothesis_backlog_manifest.json")
    return {"backlog_id": root.name, "backlog_dir": root, **payload}


def hypothesis_backlog_report_payload(
    *,
    backlog_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_HYPOTHESIS_BACKLOG_DIR,
) -> dict[str, Any]:
    root = _artifact_root(
        output_dir=output_dir,
        artifact_id=backlog_id,
        latest=latest,
        pointer_name="latest_hypothesis_backlog",
    )
    return {
        **_read_json(root / "hypothesis_backlog_manifest.json"),
        "failure_mode_taxonomy": _read_json(root / "failure_mode_taxonomy.json"),
        "hypotheses": _read_jsonl(root / "hypotheses.jsonl"),
        "hypothesis_priority_summary": _read_json(root / "hypothesis_priority_summary.json"),
        "input_snapshot": _read_json(root / "hypothesis_backlog_input_snapshot.json"),
        "backlog_dir": str(root),
    }


def validate_hypothesis_backlog_artifact(
    *, backlog_id: str, output_dir: Path = DEFAULT_HYPOTHESIS_BACKLOG_DIR
) -> dict[str, Any]:
    root = output_dir / backlog_id
    snapshot = legacy._read_optional_json(root / "hypothesis_backlog_input_snapshot.json") or {}
    errors = _validate_single_config_snapshot(
        snapshot,
        schema=HYPOTHESIS_BACKLOG_SNAPSHOT_SCHEMA,
        validator=validate_weight_optimization_hypothesis_config,
    )
    mismatches: list[str] = []
    try:
        views, _ = _hypothesis_views(snapshot, backlog_id=backlog_id, output_dir=root)
        mismatches = _view_errors(root, views)
    except Exception as exc:  # noqa: BLE001
        errors.append(str(exc))
    return _validation_payload(
        "etf_dynamic_v3_hypothesis_backlog_validation",
        backlog_id,
        [
            _check("snapshot_and_live_config", not errors, "; ".join(errors)),
            _check("content_derived_views", not mismatches, ",".join(mismatches)),
        ],
    )


def load_weight_variant_transform_spec(
    path: Path = DEFAULT_WEIGHT_VARIANT_TRANSFORM_CONFIG_PATH,
) -> dict[str, Any]:
    payload = legacy._load_yaml_mapping(path)
    _experiment_safety(payload)
    _policy_metadata(payload, name="weight variant transform")
    return payload


def validate_weight_variant_transform_spec_config(
    path: Path = DEFAULT_WEIGHT_VARIANT_TRANSFORM_CONFIG_PATH,
) -> dict[str, Any]:
    checks: list[dict[str, Any]] = []
    try:
        payload = load_weight_variant_transform_spec(path)
        _require(payload.get("schema_version") == 1, "transform schema_version invalid")
        types = _mapping(payload.get("transform_types"))
        _require(set(types) == set(DEFAULT_TRANSFORM_TYPES), "transform vocabulary must be exact")
        for name, raw in types.items():
            spec = _mapping(raw)
            _require(bool(_text(spec.get("description"))), f"{name} description missing")
            _exact_unique_texts(spec.get("required_fields"), field=f"{name}.required_fields")
            _exact_unique_texts(spec.get("allowed_modes"), field=f"{name}.allowed_modes")
        checks.extend(
            [
                _check("config_and_policy_valid", True, "reviewed exact allow-list"),
                _check("transform_vocabulary_complete", True, str(len(types))),
                _check("safety_locked", True, "experiment only"),
            ]
        )
    except Exception as exc:  # noqa: BLE001
        checks.append(_check("transform_config_valid", False, str(exc)))
    return _validation_payload(
        "etf_dynamic_v3_variant_transform_spec_config_validation",
        "weight_variant_transform_spec_config",
        checks,
        extra={"config_path": str(path)},
    )


def _transform_views(
    snapshot: Mapping[str, Any], *, spec_id: str, output_dir: Path
) -> tuple[dict[str, bytes], dict[str, Any]]:
    config = _mapping(_mapping(snapshot.get("config_binding")).get("payload"))
    normalized = legacy._normalized_variant_transform_spec(config)
    catalog = legacy._transform_type_catalog(normalized)
    manifest = {
        "schema_version": 1,
        "report_type": "etf_dynamic_v3_variant_transform_spec_manifest",
        "spec_id": spec_id,
        "generated_at": snapshot.get("generated_at"),
        "status": "PASS",
        "config_path": str(_binding_path(_mapping(snapshot.get("config_binding")))),
        "transform_type_count": len(_records(catalog.get("transform_types"))),
        "variant_transform_input_snapshot_path": str(
            output_dir / "variant_transform_input_snapshot.json"
        ),
        "variant_transform_spec_manifest_path": str(
            output_dir / "variant_transform_spec_manifest.json"
        ),
        "normalized_transform_spec_path": str(output_dir / "normalized_transform_spec.yaml"),
        "transform_type_catalog_path": str(output_dir / "transform_type_catalog.json"),
        "variant_transform_spec_report_path": str(output_dir / "variant_transform_spec_report.md"),
        **EXPERIMENT_FACTORY_SAFETY,
    }
    report = legacy.render_variant_transform_spec_report(manifest, catalog)
    views = {
        "variant_transform_input_snapshot.json": _json_bytes(dict(snapshot)),
        "variant_transform_spec_manifest.json": _json_bytes(manifest),
        "normalized_transform_spec.yaml": yaml.safe_dump(
            normalized, sort_keys=True, allow_unicode=True
        ).encode("utf-8"),
        "transform_type_catalog.json": _json_bytes(catalog),
        "variant_transform_spec_report.md": report.encode("utf-8"),
    }
    return views, {
        "manifest": manifest,
        "normalized_transform_spec": normalized,
        "transform_type_catalog": catalog,
        "validation": _mapping(snapshot.get("validation")),
    }


def build_variant_transform_spec_report(
    *,
    config_path: Path = DEFAULT_WEIGHT_VARIANT_TRANSFORM_CONFIG_PATH,
    output_dir: Path = DEFAULT_VARIANT_TRANSFORM_SPEC_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = _generated_at(generated_at)
    validation = validate_weight_variant_transform_spec_config(config_path)
    snapshot = _config_snapshot(
        schema=VARIANT_TRANSFORM_SNAPSHOT_SCHEMA,
        kind="weight_variant_transform_policy",
        config_path=config_path,
        validation=validation,
        generated=generated,
    )
    spec_id = _stable_id("variant-transform-spec", snapshot)
    root = _unique_dir(output_dir / spec_id)
    views, payload = _transform_views(snapshot, spec_id=root.name, output_dir=root)
    _write(root, views, "latest_variant_transform_spec", "variant_transform_spec_manifest.json")
    return {"spec_id": root.name, "spec_dir": root, **payload}


def variant_transform_spec_report_payload(
    *,
    spec_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_VARIANT_TRANSFORM_SPEC_DIR,
) -> dict[str, Any]:
    root = _artifact_root(
        output_dir=output_dir,
        artifact_id=spec_id,
        latest=latest,
        pointer_name="latest_variant_transform_spec",
    )
    return {
        **_read_json(root / "variant_transform_spec_manifest.json"),
        "normalized_transform_spec": legacy._load_yaml_mapping(
            root / "normalized_transform_spec.yaml"
        ),
        "transform_type_catalog": _read_json(root / "transform_type_catalog.json"),
        "input_snapshot": _read_json(root / "variant_transform_input_snapshot.json"),
        "spec_dir": str(root),
    }


def validate_variant_transform_spec_artifact(
    *, spec_id: str, output_dir: Path = DEFAULT_VARIANT_TRANSFORM_SPEC_DIR
) -> dict[str, Any]:
    root = output_dir / spec_id
    snapshot = legacy._read_optional_json(root / "variant_transform_input_snapshot.json") or {}
    errors = _validate_single_config_snapshot(
        snapshot,
        schema=VARIANT_TRANSFORM_SNAPSHOT_SCHEMA,
        validator=validate_weight_variant_transform_spec_config,
    )
    mismatches: list[str] = []
    try:
        views, _ = _transform_views(snapshot, spec_id=spec_id, output_dir=root)
        mismatches = _view_errors(root, views)
    except Exception as exc:  # noqa: BLE001
        errors.append(str(exc))
    return _validation_payload(
        "etf_dynamic_v3_variant_transform_spec_validation",
        spec_id,
        [
            _check("snapshot_and_live_config", not errors, "; ".join(errors)),
            _check("content_derived_views", not mismatches, ",".join(mismatches)),
        ],
    )


def _triage_policy(payload: Mapping[str, Any]) -> dict[str, Any]:
    policy = _mapping(payload.get("triage_policy"))
    for field in (
        "policy_version",
        "owner",
        "status",
        "rationale",
        "intended_effect",
        "validation_evidence",
        "review_condition",
    ):
        _require(bool(_text(policy.get(field))), f"triage_policy.{field} required")
    _require(
        _text(policy.get("status")) in {"pilot_baseline", "approved", "active"},
        "triage policy status invalid",
    )
    weights = _mapping(policy.get("score_weights"))
    _require(set(weights) == set(_TRIAGE_COMPONENTS), "triage score weights must be exact")
    _require(
        all(_finite(value) and float(value) >= 0.0 for value in weights.values()),
        "score weights invalid",
    )
    _require(
        abs(sum(float(value) for value in weights.values()) - 1.0) <= 1e-9,
        "score weights must sum to one",
    )
    promote = policy.get("promote_score")
    keep = policy.get("keep_testing_score")
    _require(_finite(promote) and _finite(keep), "triage thresholds must be finite")
    _require(0.0 <= float(keep) <= float(promote) <= 1.0, "triage thresholds invalid")
    rules = _exact_unique_texts(policy.get("hard_reject_rules"), field="hard reject rules")
    _require(set(rules) == set(_HARD_REJECT_RULES), "hard reject rules must be exact")
    bounds = _mapping(policy.get("score_bounds"))
    _require(set(bounds) == {"return", "drawdown", "turnover"}, "score bounds must be exact")
    for field, raw in bounds.items():
        values = list(raw) if isinstance(raw, Sequence) and not isinstance(raw, str | bytes) else []
        _require(
            len(values) == 2 and all(_finite(value) for value in values), f"{field} bounds invalid"
        )
        _require(float(values[0]) < float(values[1]), f"{field} bounds unordered")
    labels = _mapping(policy.get("label_scores"))
    expected_labels = {
        "rolling_consistency": {"IMPROVED", "MIXED", "INSUFFICIENT_DATA", "WORSE"},
        "regime": {"IMPROVED", "MIXED", "WORSE"},
        "simplicity": {"one_transform", "two_transforms", "multi_transform"},
    }
    _require(set(labels) == set(expected_labels), "label score groups invalid")
    for group, keys in expected_labels.items():
        values = _mapping(labels.get(group))
        _require(set(values) == keys, f"{group} label scores incomplete")
        _require(
            all(_finite(value) and 0.0 <= float(value) <= 1.0 for value in values.values()),
            f"{group} label scores invalid",
        )
    thresholds = _mapping(policy.get("hard_reject_thresholds"))
    _require(
        set(thresholds)
        == {"max_drawdown_delta_floor", "turnover_delta_ceiling", "pressure_regimes"},
        "hard reject thresholds incomplete",
    )
    _require(
        _finite(thresholds.get("max_drawdown_delta_floor")), "drawdown reject threshold invalid"
    )
    _require(_finite(thresholds.get("turnover_delta_ceiling")), "turnover reject threshold invalid")
    _exact_unique_texts(thresholds.get("pressure_regimes"), field="pressure regimes")
    minimum = policy.get("minimum_valid_regime_count")
    cap = policy.get("top_candidate_cap")
    _require(
        isinstance(minimum, int) and not isinstance(minimum, bool) and minimum > 0,
        "minimum regime count invalid",
    )
    _require(
        isinstance(cap, int) and not isinstance(cap, bool) and 0 < cap <= 10,
        "top candidate cap invalid",
    )
    _require(policy.get("missing_metric_decision") == "DEFER", "missing metric behavior must DEFER")
    return dict(policy)


def _candidate_selection_policy(payload: Mapping[str, Any]) -> dict[str, Any]:
    policy = _mapping(payload.get("candidate_selection_policy"))
    preference = _exact_unique_texts(policy.get("method_preference"), field="method preference")
    representatives = _exact_unique_texts(
        policy.get("cluster_representatives"), field="cluster representatives"
    )
    available = _exact_unique_texts(policy.get("all_available_methods"), field="available methods")
    top_n = policy.get("top_n")
    _require(
        isinstance(top_n, int) and not isinstance(top_n, bool) and 0 < top_n <= len(preference),
        "top_n invalid",
    )
    _require(set(representatives).issubset(available), "representatives outside available methods")
    _require(set(preference).issubset(available), "preference outside available methods")
    return dict(policy)


def _transform_instance_errors(
    transform: Mapping[str, Any], catalog: Mapping[str, Any]
) -> list[str]:
    errors: list[str] = []
    transform_type = _text(transform.get("type"))
    spec = _mapping(catalog.get(transform_type))
    if not transform_type or not spec:
        return [f"unknown transform type: {transform_type or 'MISSING'}"]
    for field in legacy._texts(spec.get("required_fields")):
        if field not in transform or transform.get(field) in (None, ""):
            errors.append(f"{transform_type}.{field} missing")
    mode_field = next(
        (field for field in ("action", "selection_rule", "method", "mode") if field in transform),
        None,
    )
    if mode_field and _text(transform.get(mode_field)) not in legacy._texts(
        spec.get("allowed_modes")
    ):
        errors.append(f"{transform_type}.{mode_field} invalid")
    for field in (
        "max_weight",
        "min_cash_weight",
        "min_total_abs_delta",
        "max_turnover",
        "multiplier",
    ):
        if field in transform and (
            not _finite(transform.get(field)) or not 0.0 <= float(transform[field]) <= 1.0
        ):
            errors.append(f"{transform_type}.{field} invalid")
    for field in ("cooldown_days", "window_days", "persistence_days"):
        if field in transform and (
            not isinstance(transform.get(field), int)
            or isinstance(transform.get(field), bool)
            or int(transform[field]) <= 0
        ):
            errors.append(f"{transform_type}.{field} invalid")
    return errors


def load_weight_experiment_matrix_config(
    path: Path = DEFAULT_WEIGHT_EXPERIMENT_MATRIX_CONFIG_PATH,
) -> dict[str, Any]:
    payload = legacy._load_yaml_mapping(path)
    _experiment_safety(payload)
    _policy_metadata(payload, name="weight experiment matrix")
    _candidate_selection_policy(payload)
    _triage_policy(payload)
    return payload


def _matrix_source_paths(payload: Mapping[str, Any]) -> tuple[Path, Path]:
    source = _mapping(payload.get("source"))
    return (
        legacy._resolve_project_path(
            source.get("hypothesis_config"),
            DEFAULT_WEIGHT_OPTIMIZATION_HYPOTHESIS_CONFIG_PATH,
        ),
        legacy._resolve_project_path(
            source.get("transform_spec_config"), DEFAULT_WEIGHT_VARIANT_TRANSFORM_CONFIG_PATH
        ),
    )


def validate_weight_experiment_matrix_config(
    path: Path = DEFAULT_WEIGHT_EXPERIMENT_MATRIX_CONFIG_PATH,
) -> dict[str, Any]:
    checks: list[dict[str, Any]] = []
    try:
        payload = load_weight_experiment_matrix_config(path)
        _require(payload.get("schema_version") == 1, "matrix schema_version invalid")
        hypothesis_path, transform_path = _matrix_source_paths(payload)
        hypothesis_validation = validate_weight_optimization_hypothesis_config(hypothesis_path)
        transform_validation = validate_weight_variant_transform_spec_config(transform_path)
        _require(
            hypothesis_validation.get("status") == "PASS", "hypothesis config validation failed"
        )
        _require(transform_validation.get("status") == "PASS", "transform config validation failed")
        hypothesis = load_weight_optimization_hypothesis_config(hypothesis_path)
        transform = load_weight_variant_transform_spec(transform_path)
        hypothesis_rows = _records(hypothesis.get("hypotheses"))
        hypothesis_lookup = {_text(row.get("hypothesis_id")): row for row in hypothesis_rows}
        catalog = _mapping(transform.get("transform_types"))
        group = _mapping(payload.get("experiment_group"))
        _require(bool(_text(group.get("id"))), "experiment group id missing")
        _require(group.get("base_method") == "limited_adjustment", "matrix base method invalid")
        _require(bool(_text(group.get("source_backfill_id"))), "source backfill id missing")
        _require(group.get("market_regime") == "ai_after_chatgpt", "market regime invalid")
        _require(str(group.get("requested_start_date")) == "2022-12-01", "requested start invalid")
        variants = _records(payload.get("variants"))
        ids = [_text(row.get("variant_id")) for row in variants]
        _require(bool(variants) and all(ids) and len(ids) == len(set(ids)), "variant ids invalid")
        used: set[str] = set()
        for row in variants:
            hypothesis_id = _text(row.get("hypothesis_id"))
            _require(hypothesis_id in hypothesis_lookup, "variant hypothesis unknown")
            hypothesis_row = _mapping(hypothesis_lookup[hypothesis_id])
            _require(
                _text(row.get("family")) == _text(hypothesis_row.get("family")),
                "variant family mismatch",
            )
            transforms = _records(row.get("transforms"))
            _require(bool(transforms), "variant transforms missing")
            for transform_row in transforms:
                errors = _transform_instance_errors(transform_row, catalog)
                _require(not errors, "; ".join(errors))
                used.add(_text(transform_row.get("type")))
        _require(
            {
                "regime_gate",
                "weight_smoothing",
                "rebalance_threshold",
                "consensus_aggregation",
            }.issubset(used),
            "required transform families missing",
        )
        checks.extend(
            [
                _check(
                    "matrix_and_reviewed_policy_valid",
                    True,
                    _text(_mapping(payload.get("policy_metadata")).get("policy_id")),
                ),
                _check("source_configs_valid", True, "hypothesis+transform PASS"),
                _check("variants_and_transforms_source_exact", True, str(len(variants))),
                _check("safety_locked", True, "experiment only"),
            ]
        )
    except Exception as exc:  # noqa: BLE001
        checks.append(_check("matrix_config_valid", False, str(exc)))
    return _validation_payload(
        "etf_dynamic_v3_experiment_matrix_config_validation",
        "weight_experiment_matrix_config",
        checks,
        extra={"config_path": str(path)},
    )


def _matrix_snapshot(*, config_path: Path, generated: datetime) -> dict[str, Any]:
    validation = validate_weight_experiment_matrix_config(config_path)
    _require(validation.get("status") == "PASS", "matrix config validation failed")
    config = load_weight_experiment_matrix_config(config_path)
    hypothesis_path, transform_path = _matrix_source_paths(config)
    hypothesis_validation = validate_weight_optimization_hypothesis_config(hypothesis_path)
    transform_validation = validate_weight_variant_transform_spec_config(transform_path)
    return {
        "schema_version": EXPERIMENT_MATRIX_SNAPSHOT_SCHEMA,
        "generated_at": generated.isoformat(),
        "matrix_config_binding": target_core._config_binding(
            config_path, kind="weight_experiment_matrix_policy"
        ),
        "hypothesis_config_binding": target_core._config_binding(
            hypothesis_path, kind="weight_optimization_hypothesis_policy"
        ),
        "transform_config_binding": target_core._config_binding(
            transform_path, kind="weight_variant_transform_policy"
        ),
        "matrix_validation": validation,
        "hypothesis_validation": hypothesis_validation,
        "transform_validation": transform_validation,
        "production_effect": "none",
    }


def _validate_matrix_snapshot(snapshot: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    try:
        _require(
            snapshot.get("schema_version") == EXPERIMENT_MATRIX_SNAPSHOT_SCHEMA,
            "matrix snapshot schema invalid",
        )
        matrix_binding = _mapping(snapshot.get("matrix_config_binding"))
        hypothesis_binding = _mapping(snapshot.get("hypothesis_config_binding"))
        transform_binding = _mapping(snapshot.get("transform_config_binding"))
        for binding in (matrix_binding, hypothesis_binding, transform_binding):
            errors.extend(target_core._validate_config_binding(binding))
        actual_matrix = validate_weight_experiment_matrix_config(_binding_path(matrix_binding))
        actual_hypothesis = validate_weight_optimization_hypothesis_config(
            _binding_path(hypothesis_binding)
        )
        actual_transform = validate_weight_variant_transform_spec_config(
            _binding_path(transform_binding)
        )
        if actual_matrix != _mapping(snapshot.get("matrix_validation")):
            errors.append("matrix config validation drift")
        if actual_hypothesis != _mapping(snapshot.get("hypothesis_validation")):
            errors.append("hypothesis config validation drift")
        if actual_transform != _mapping(snapshot.get("transform_validation")):
            errors.append("transform config validation drift")
        config = _mapping(matrix_binding.get("payload"))
        expected_hypothesis, expected_transform = _matrix_source_paths(config)
        _require(
            expected_hypothesis.resolve() == _binding_path(hypothesis_binding).resolve(),
            "hypothesis source binding mismatch",
        )
        _require(
            expected_transform.resolve() == _binding_path(transform_binding).resolve(),
            "transform source binding mismatch",
        )
    except Exception as exc:  # noqa: BLE001
        errors.append(str(exc))
    return errors


def _matrix_views(
    snapshot: Mapping[str, Any], *, matrix_id: str, output_dir: Path
) -> tuple[dict[str, bytes], dict[str, Any]]:
    config = _mapping(_mapping(snapshot.get("matrix_config_binding")).get("payload"))
    hypothesis_config = _mapping(_mapping(snapshot.get("hypothesis_config_binding")).get("payload"))
    transform_config = _mapping(_mapping(snapshot.get("transform_config_binding")).get("payload"))
    hypothesis_lookup = {
        _text(row.get("hypothesis_id")): row
        for row in legacy._normalized_hypotheses(hypothesis_config)
    }
    variant_specs = legacy._normalized_variant_specs(config, hypothesis_lookup, transform_config)
    for row in variant_specs:
        for transform in _records(row.get("transforms")):
            _require(
                not legacy._texts(transform.get("missing_required_fields")),
                "normalized transform missing fields",
            )
    transform_specs = legacy._matrix_transform_specs(transform_config, variant_specs)
    summary = legacy._experiment_matrix_summary(variant_specs)
    group = _mapping(config.get("experiment_group"))
    manifest = {
        "schema_version": 1,
        "report_type": "etf_dynamic_v3_experiment_matrix_manifest",
        "matrix_id": matrix_id,
        "experiment_group_id": group.get("id"),
        "base_method": group.get("base_method"),
        "source_backfill_id": group.get("source_backfill_id"),
        "generated_at": snapshot.get("generated_at"),
        "status": "PASS",
        "config_path": str(_binding_path(_mapping(snapshot.get("matrix_config_binding")))),
        "policy_id": _mapping(config.get("policy_metadata")).get("policy_id"),
        "triage_policy_version": _mapping(config.get("triage_policy")).get("policy_version"),
        "variant_count": len(variant_specs),
        "families_covered": summary["families_covered"],
        "failure_modes_covered": summary["failure_modes_covered"],
        "experiment_matrix_input_snapshot_path": str(
            output_dir / "experiment_matrix_input_snapshot.json"
        ),
        "experiment_matrix_manifest_path": str(output_dir / "experiment_matrix_manifest.json"),
        "variant_specs_path": str(output_dir / "variant_specs.jsonl"),
        "transform_specs_path": str(output_dir / "transform_specs.json"),
        "experiment_matrix_report_path": str(output_dir / "experiment_matrix_report.md"),
        **EXPERIMENT_FACTORY_SAFETY,
    }
    report = legacy.render_experiment_matrix_report(
        manifest, variant_specs, transform_specs, summary
    )
    views = {
        "experiment_matrix_input_snapshot.json": _json_bytes(dict(snapshot)),
        "experiment_matrix_manifest.json": _json_bytes(manifest),
        "variant_specs.jsonl": _jsonl_bytes(variant_specs),
        "transform_specs.json": _json_bytes(transform_specs),
        "experiment_matrix_report.md": report.encode("utf-8"),
    }
    return views, {
        "manifest": manifest,
        "variant_specs": variant_specs,
        "transform_specs": transform_specs,
        "summary": summary,
        "validation": _mapping(snapshot.get("matrix_validation")),
    }


def build_experiment_matrix(
    *,
    config_path: Path = DEFAULT_WEIGHT_EXPERIMENT_MATRIX_CONFIG_PATH,
    output_dir: Path = DEFAULT_EXPERIMENT_MATRIX_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = _generated_at(generated_at)
    snapshot = _matrix_snapshot(config_path=config_path, generated=generated)
    matrix_id = _stable_id("experiment-matrix", snapshot)
    root = _unique_dir(output_dir / matrix_id)
    views, payload = _matrix_views(snapshot, matrix_id=root.name, output_dir=root)
    _write(root, views, "latest_experiment_matrix", "experiment_matrix_manifest.json")
    return {"matrix_id": root.name, "matrix_dir": root, **payload}


def experiment_matrix_report_payload(
    *,
    matrix_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_EXPERIMENT_MATRIX_DIR,
) -> dict[str, Any]:
    root = _artifact_root(
        output_dir=output_dir,
        artifact_id=matrix_id,
        latest=latest,
        pointer_name="latest_experiment_matrix",
    )
    return {
        **_read_json(root / "experiment_matrix_manifest.json"),
        "variant_specs": _read_jsonl(root / "variant_specs.jsonl"),
        "transform_specs": _read_json(root / "transform_specs.json"),
        "input_snapshot": _read_json(root / "experiment_matrix_input_snapshot.json"),
        "matrix_dir": str(root),
    }


def validate_experiment_matrix_artifact(
    *, matrix_id: str, output_dir: Path = DEFAULT_EXPERIMENT_MATRIX_DIR
) -> dict[str, Any]:
    root = output_dir / matrix_id
    snapshot = legacy._read_optional_json(root / "experiment_matrix_input_snapshot.json") or {}
    errors = _validate_matrix_snapshot(snapshot)
    mismatches: list[str] = []
    try:
        views, _ = _matrix_views(snapshot, matrix_id=matrix_id, output_dir=root)
        mismatches = _view_errors(root, views)
    except Exception as exc:  # noqa: BLE001
        errors.append(str(exc))
    return _validation_payload(
        "etf_dynamic_v3_experiment_matrix_validation",
        matrix_id,
        [
            _check("snapshot_and_live_configs", not errors, "; ".join(errors)),
            _check("content_derived_views", not mismatches, ",".join(mismatches)),
        ],
    )


def _read_source_json(binding: Mapping[str, Any], name: str) -> dict[str, Any]:
    return _read_json(_source_dir(binding) / name)


def _matrix_policy_payload(binding: Mapping[str, Any]) -> dict[str, Any]:
    snapshot = _read_source_json(binding, "experiment_matrix_input_snapshot.json")
    return _mapping(_mapping(snapshot.get("matrix_config_binding")).get("payload"))


def _batch_snapshot(
    *,
    matrix_id: str,
    matrix_dir: Path,
    baseline_backfill_dir: Path,
    price_cache_path: Path | None,
    rates_cache_path: Path,
    generated: datetime,
) -> dict[str, Any]:
    matrix_binding = _matrix_binding(matrix_id, matrix_dir)
    matrix_manifest = _bundle_json(matrix_binding, "experiment_matrix_manifest.json")
    _source_not_after(
        matrix_binding,
        "experiment_matrix_manifest.json",
        generated,
        field="experiment matrix generated_at",
    )
    backfill_id = _text(matrix_manifest.get("source_backfill_id"))
    _require(bool(backfill_id), "matrix source backfill id missing")
    backfill_binding = _backfill_binding(backfill_id, baseline_backfill_dir)
    backfill_manifest = _bundle_json(backfill_binding, "paper_shadow_backfill_manifest.json")
    _source_not_after(
        backfill_binding,
        "paper_shadow_backfill_manifest.json",
        generated,
        field="paper backfill generated_at",
    )
    _require(
        backfill_manifest.get("backfill_id") == backfill_id,
        "paper backfill identity mismatch",
    )
    _require(
        backfill_manifest.get("data_quality_status") in {"PASS", "PASS_WITH_WARNINGS"},
        "paper backfill data quality invalid",
    )
    backfill_snapshot = _read_source_json(
        backfill_binding, "paper_shadow_backfill_input_snapshot.json"
    )
    caches = _records(backfill_snapshot.get("cache_bindings"))
    recorded_prices = next(
        (Path(_text(row.get("path"))) for row in caches if row.get("kind") == "prices"),
        None,
    )
    recorded_rates = next(
        (Path(_text(row.get("path"))) for row in caches if row.get("kind") == "rates"),
        None,
    )
    _require(
        recorded_prices is not None and recorded_rates is not None, "backfill cache lineage missing"
    )
    if price_cache_path is not None:
        _require(
            price_cache_path.resolve() == recorded_prices.resolve(),
            "batch price cache must equal backfill price cache",
        )
    _require(
        rates_cache_path.resolve() == recorded_rates.resolve(),
        "batch rates cache must equal backfill rates cache",
    )
    return {
        "schema_version": BATCH_EXPERIMENT_SNAPSHOT_SCHEMA,
        "generated_at": generated.isoformat(),
        "source_experiment_matrix": matrix_binding,
        "source_paper_shadow_backfill": backfill_binding,
        "matrix_id": matrix_id,
        "source_backfill_id": backfill_id,
        "price_cache_path": str(recorded_prices),
        "rates_cache_path": str(recorded_rates),
        "data_quality": _mapping(backfill_snapshot.get("data_quality")),
        "source_price_rows_are_common_finite_and_duplicate_free": True,
        "first_or_missing_return_may_be_filled_zero": False,
        "production_effect": "none",
    }


def _validate_batch_snapshot(snapshot: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    try:
        _require(
            snapshot.get("schema_version") == BATCH_EXPERIMENT_SNAPSHOT_SCHEMA,
            "batch snapshot schema invalid",
        )
        matrix_binding = _mapping(snapshot.get("source_experiment_matrix"))
        backfill_binding = _mapping(snapshot.get("source_paper_shadow_backfill"))
        errors.extend(_validate_source_binding(matrix_binding))
        errors.extend(_validate_source_binding(backfill_binding))
        generated = target_core._datetime(snapshot.get("generated_at"), field="batch generated_at")
        _source_not_after(
            matrix_binding,
            "experiment_matrix_manifest.json",
            generated,
            field="matrix generated_at",
        )
        _source_not_after(
            backfill_binding,
            "paper_shadow_backfill_manifest.json",
            generated,
            field="backfill generated_at",
        )
        matrix_manifest = _bundle_json(matrix_binding, "experiment_matrix_manifest.json")
        backfill_manifest = _bundle_json(backfill_binding, "paper_shadow_backfill_manifest.json")
        _require(
            matrix_binding.get("artifact_id") == snapshot.get("matrix_id"),
            "matrix binding id mismatch",
        )
        _require(
            backfill_binding.get("artifact_id") == snapshot.get("source_backfill_id"),
            "backfill binding id mismatch",
        )
        _require(
            matrix_manifest.get("source_backfill_id") == backfill_binding.get("artifact_id"),
            "matrix/backfill lineage mismatch",
        )
        _require(
            backfill_manifest.get("data_quality_status") in {"PASS", "PASS_WITH_WARNINGS"},
            "backfill DQ invalid",
        )
        backfill_snapshot = _read_source_json(
            backfill_binding, "paper_shadow_backfill_input_snapshot.json"
        )
        _require(
            _mapping(backfill_snapshot.get("data_quality"))
            == _mapping(snapshot.get("data_quality")),
            "batch data-quality commitment drift",
        )
    except Exception as exc:  # noqa: BLE001
        errors.append(str(exc))
    return errors


def _weight_rows_valid(rows: Sequence[Mapping[str, Any]], *, label: str) -> None:
    _require(bool(rows), f"{label} rows missing")
    seen: set[tuple[str, str]] = set()
    for row in rows:
        key = (_text(row.get("date")), _text(row.get("target_method")))
        _require(all(key) and key not in seen, f"{label} date/method missing or duplicate")
        seen.add(key)
        weights = _mapping(row.get("weights"))
        _require(bool(weights), f"{label} weights missing")
        _require(
            all(_finite(value) and float(value) >= 0.0 for value in weights.values()),
            f"{label} weights non-finite/negative",
        )
        _require(
            abs(sum(float(value) for value in weights.values()) - 1.0) <= 1e-7,
            f"{label} weights do not sum to one",
        )


def _price_returns_from_backfill(
    backfill_binding: Mapping[str, Any], baseline_states: Sequence[Mapping[str, Any]]
) -> tuple[pd.DataFrame, dict[str, Any], date, date]:
    snapshot = _read_source_json(backfill_binding, "paper_shadow_backfill_input_snapshot.json")
    symbols = legacy._symbols_from_state_paths(baseline_states)
    price_rows = _records(snapshot.get("price_rows"))
    pivot, incomplete = history._common_price_pivot(price_rows, symbols=symbols)
    _require(not incomplete, "backfill snapshot contains incomplete price dates")
    _require(len(pivot.index) >= 2, "batch requires at least two common price dates")
    returns = pivot / pivot.shift(1) - 1.0
    returns = returns.iloc[1:]
    _require(not returns.empty, "batch return matrix empty")
    _require(
        all(math.isfinite(float(value)) for value in returns.to_numpy().ravel()),
        "batch return matrix contains missing/non-finite values",
    )
    config = _mapping(_mapping(snapshot.get("config_binding")).get("payload"))
    return returns, config, returns.index[0].date(), returns.index[-1].date()


def _aggregate_candidate_weights(
    weights_list: Sequence[Mapping[str, Any]], *, method: str
) -> dict[str, float]:
    clean = [legacy._normalize_weights(weights) for weights in weights_list if weights]
    _require(bool(clean), "candidate aggregation has no selected weight sources")
    symbols = sorted({symbol for weights in clean for symbol in weights})
    values_by_symbol = {
        symbol: [float(weights.get(symbol, 0.0)) for weights in clean] for symbol in symbols
    }
    if method == "median":
        result = {symbol: statistics.median(values) for symbol, values in values_by_symbol.items()}
    elif method == "trimmed_mean":
        _require(len(clean) >= 3, "trimmed mean requires at least three selected methods")
        result = {}
        for symbol, values in values_by_symbol.items():
            ordered = sorted(values)
            result[symbol] = sum(ordered[1:-1]) / len(ordered[1:-1])
    else:
        _require(method == "weighted_mean", f"unsupported aggregation method: {method}")
        result = {symbol: sum(values) / len(values) for symbol, values in values_by_symbol.items()}
    return legacy._normalize_weights(result)


def _variant_source_target(
    *,
    variant: Mapping[str, Any],
    date_text: str,
    rows_by_date_method: Mapping[str, Mapping[str, Mapping[str, Any]]],
    fallback: Mapping[str, Any],
    selection_policy: Mapping[str, Any],
) -> dict[str, float]:
    transforms = _records(variant.get("transforms"))
    aggregation = next(
        (
            transform
            for transform in transforms
            if _text(transform.get("type")) == "consensus_aggregation"
        ),
        None,
    )
    if aggregation is None:
        return legacy._normalize_weights(fallback)
    subset = next(
        (
            transform
            for transform in transforms
            if _text(transform.get("type")) == "candidate_subset"
        ),
        None,
    )
    rule = _text(_mapping(subset).get("selection_rule"), "all_available_methods")
    if rule == "top_5_by_score":
        names = legacy._texts(selection_policy.get("method_preference"))[
            : int(selection_policy.get("top_n", 5))
        ]
    elif rule == "cluster_representatives":
        names = legacy._texts(selection_policy.get("cluster_representatives"))
    elif rule == "all_available_methods":
        names = legacy._texts(selection_policy.get("all_available_methods"))
    else:
        raise DynamicV3ExperimentFactoryError(f"unknown candidate selection rule: {rule}")
    method_rows = rows_by_date_method.get(date_text, {})
    selected = [_mapping(method_rows[name].get("weights")) for name in names if name in method_rows]
    _require(bool(selected), f"candidate selection {rule} has no source rows on {date_text}")
    return _aggregate_candidate_weights(selected, method=_text(aggregation.get("method")))


def _run_variant_weight_path(
    *,
    variant: Mapping[str, Any],
    baseline_states: Sequence[Mapping[str, Any]],
    returns: pd.DataFrame,
    labels: Mapping[str, str],
    config: Mapping[str, Any],
    selection_policy: Mapping[str, Any],
) -> list[dict[str, Any]]:
    variant_id = _text(variant.get("variant_id"))
    ordered_states = sorted(
        baseline_states, key=lambda row: (_text(row.get("date")), _text(row.get("target_method")))
    )
    base_by_date = {
        _text(row.get("date")): row
        for row in ordered_states
        if row.get("target_method") == "limited_adjustment"
    }
    rows_by_date_method: dict[str, dict[str, Mapping[str, Any]]] = {}
    for row in ordered_states:
        rows_by_date_method.setdefault(_text(row.get("date")), {})[
            _text(row.get("target_method"))
        ] = row
    initial_row = next(iter(base_by_date.values()), {})
    current_weights = legacy._normalize_weights(
        _mapping(initial_row.get("weights")) or legacy._backfill_initial_weights(config)
    )
    portfolio_value = 1.0
    peak_value = 1.0
    transform_state: dict[str, Any] = {
        "target_history": [],
        "last_target": current_weights,
        "cooldown_until": {},
        "persistence_count": 0,
    }
    result: list[dict[str, Any]] = []
    for timestamp, return_row in returns.iterrows():
        date_text = timestamp.date().isoformat()
        base_row = _mapping(base_by_date.get(date_text))
        _require(bool(base_row), f"limited baseline row missing on common date {date_text}")
        before_return = legacy._normalize_weights(current_weights)
        daily_return = legacy._portfolio_return(before_return, return_row)
        _require(_finite(daily_return), f"variant return non-finite on {date_text}")
        portfolio_value *= 1.0 + float(daily_return)
        drifted = legacy._drift_weights(before_return, return_row, float(daily_return))
        after_weights = dict(drifted)
        turnover = 0.0
        rebalance_event = False
        transform_delta = 0.0
        if base_row.get("rebalance_event") is True:
            base_target = legacy._normalize_weights(_mapping(base_row.get("weights")))
            target = _variant_source_target(
                variant=variant,
                date_text=date_text,
                rows_by_date_method=rows_by_date_method,
                fallback=_mapping(base_row.get("weights")),
                selection_policy=selection_policy,
            )
            after_weights = legacy._apply_experiment_transforms(
                as_of=timestamp.date(),
                target=target,
                previous=drifted,
                variant=variant,
                regime=_text(labels.get(date_text), "sideways_choppy"),
                transform_state=transform_state,
            )
            transform_delta = max(
                abs(float(after_weights.get(symbol, 0.0)) - float(base_target.get(symbol, 0.0)))
                for symbol in set(after_weights) | set(base_target)
            )
            turnover = legacy._turnover(drifted, after_weights)
            rebalance_event = turnover > 0.0
        peak_value = max(peak_value, portfolio_value)
        drawdown = portfolio_value / peak_value - 1.0
        current_weights = after_weights
        result.append(
            {
                "date": date_text,
                "variant_id": variant_id,
                "base_method": variant.get("base_method"),
                "target_method": variant_id,
                "regime": _text(labels.get(date_text), "sideways_choppy"),
                "weights": after_weights,
                "portfolio_value": round(portfolio_value, 10),
                "daily_return": round(float(daily_return), 10),
                "drawdown": round(drawdown, 10),
                "turnover": round(turnover, 10),
                "rebalance_event": rebalance_event,
                "transform_effective": transform_delta > _WEIGHT_COMPARISON_TOLERANCE,
                "max_transform_weight_delta": round(transform_delta, 10),
                "target_failure_modes": legacy._texts(variant.get("target_failure_modes")),
                **EXPERIMENT_FACTORY_SAFETY,
            }
        )
    return result


def _nullable_delta(left: Any, right: Any) -> float | None:
    if not _finite(left) or not _finite(right):
        return None
    return round(float(left) - float(right), 10)


def _path_metrics(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    metrics = legacy._state_path_metrics(rows, min_observations=2)
    if metrics.get("status") == "INSUFFICIENT_DATA":
        return metrics
    for field in (
        "total_return",
        "annualized_return",
        "max_drawdown",
        "realized_volatility",
        "turnover",
    ):
        _require(_finite(metrics.get(field)), f"path metric {field} missing/non-finite")
    return metrics


def _performance_metrics(
    variant_states: Sequence[Mapping[str, Any]], baseline_states: Sequence[Mapping[str, Any]]
) -> list[dict[str, Any]]:
    dates = {_text(row.get("date")) for row in variant_states}
    baselines = {
        method: [
            row
            for row in baseline_states
            if row.get("target_method") == method and row.get("date") in dates
        ]
        for method in ("limited_adjustment", "static_baseline", "no_trade_baseline")
    }
    baseline_metrics = {method: _path_metrics(rows) for method, rows in baselines.items()}
    limited = baseline_metrics["limited_adjustment"]
    rows: list[dict[str, Any]] = []
    for variant_id in sorted({_text(row.get("variant_id")) for row in variant_states}):
        states = [row for row in variant_states if row.get("variant_id") == variant_id]
        metrics = _path_metrics(states)
        status = legacy._performance_status(metrics, limited)
        rows.append(
            {
                "variant_id": variant_id,
                "base_method": "limited_adjustment",
                "total_return": metrics.get("total_return"),
                "annualized_return": metrics.get("annualized_return"),
                "max_drawdown": metrics.get("max_drawdown"),
                "realized_volatility": metrics.get("realized_volatility"),
                "turnover": metrics.get("turnover"),
                "relative_to_limited_adjustment": _nullable_delta(
                    metrics.get("total_return"), limited.get("total_return")
                ),
                "relative_to_static_baseline": _nullable_delta(
                    metrics.get("total_return"),
                    baseline_metrics["static_baseline"].get("total_return"),
                ),
                "relative_to_no_trade_baseline": _nullable_delta(
                    metrics.get("total_return"),
                    baseline_metrics["no_trade_baseline"].get("total_return"),
                ),
                "drawdown_delta_vs_limited": _nullable_delta(
                    metrics.get("max_drawdown"), limited.get("max_drawdown")
                ),
                "turnover_delta_vs_limited": _nullable_delta(
                    metrics.get("turnover"), limited.get("turnover")
                ),
                "transform_effective_rebalance_count": sum(
                    row.get("transform_effective") is True for row in states
                ),
                "max_transform_weight_delta": max(
                    (float(row.get("max_transform_weight_delta", 0.0)) for row in states),
                    default=0.0,
                ),
                "performance_status": status,
                **EXPERIMENT_FACTORY_SAFETY,
            }
        )
    return rows


def _regime_metrics(
    variant_states: Sequence[Mapping[str, Any]],
    baseline_states: Sequence[Mapping[str, Any]],
    labels: Mapping[str, str],
    config: Mapping[str, Any],
) -> list[dict[str, Any]]:
    min_sample = legacy._config_int(config, ("regime_policy", "min_sample_count"), 5)
    rows: list[dict[str, Any]] = []
    for variant_id in sorted({_text(row.get("variant_id")) for row in variant_states}):
        for regime in legacy._configured_regimes():
            dates = {date_text for date_text, label in labels.items() if label == regime}
            selected = [
                row
                for row in variant_states
                if row.get("variant_id") == variant_id and row.get("date") in dates
            ]
            limited = [
                row
                for row in baseline_states
                if row.get("target_method") == "limited_adjustment" and row.get("date") in dates
            ]
            static = [
                row
                for row in baseline_states
                if row.get("target_method") == "static_baseline" and row.get("date") in dates
            ]
            metrics = legacy._sample_return_metrics(selected, min_sample=min_sample)
            limited_metrics = legacy._sample_return_metrics(limited, min_sample=min_sample)
            static_metrics = legacy._sample_return_metrics(static, min_sample=min_sample)
            sufficient = all(
                item.get("status") != "INSUFFICIENT_DATA"
                for item in (metrics, limited_metrics, static_metrics)
            )
            return_delta = (
                _nullable_delta(metrics.get("total_return"), limited_metrics.get("total_return"))
                if sufficient
                else None
            )
            drawdown_delta = (
                _nullable_delta(metrics.get("max_drawdown"), limited_metrics.get("max_drawdown"))
                if sufficient
                else None
            )
            turnover_delta = (
                _nullable_delta(metrics.get("turnover"), limited_metrics.get("turnover"))
                if sufficient
                else None
            )
            rows.append(
                {
                    "variant_id": variant_id,
                    "regime": regime,
                    "sample_count": len(selected),
                    "relative_to_limited_adjustment": return_delta,
                    "relative_to_static_baseline": _nullable_delta(
                        metrics.get("total_return"), static_metrics.get("total_return")
                    )
                    if sufficient
                    else None,
                    "drawdown_delta_vs_limited": drawdown_delta,
                    "turnover_delta_vs_limited": turnover_delta,
                    "regime_status": (
                        legacy._regime_status(metrics, float(return_delta), float(drawdown_delta))
                        if sufficient and return_delta is not None and drawdown_delta is not None
                        else "INSUFFICIENT_DATA"
                    ),
                    **EXPERIMENT_FACTORY_SAFETY,
                }
            )
    return rows


def _stability_metrics(
    variant_states: Sequence[Mapping[str, Any]],
    baseline_states: Sequence[Mapping[str, Any]],
    config: Mapping[str, Any],
) -> list[dict[str, Any]]:
    variant_metrics, _, _ = legacy._stability_diagnostics(variant_states, config)
    limited_states = [
        row for row in baseline_states if row.get("target_method") == "limited_adjustment"
    ]
    limited_metrics, _, _ = legacy._stability_diagnostics(limited_states, config)
    limited_row = legacy._find_method(limited_metrics, "limited_adjustment")
    rows: list[dict[str, Any]] = []
    for row in variant_metrics:
        variant_id = _text(row.get("target_method"))
        states = [item for item in variant_states if item.get("variant_id") == variant_id]
        status = _text(row.get("stability_status"), "INSUFFICIENT_DATA")
        rows.append(
            {
                "variant_id": variant_id,
                "avg_rebalance_turnover": row.get("avg_rebalance_turnover")
                if status != "INSUFFICIENT_DATA"
                else None,
                "max_rebalance_turnover": row.get("max_rebalance_turnover")
                if status != "INSUFFICIENT_DATA"
                else None,
                "large_jump_count": row.get("large_jump_count")
                if status != "INSUFFICIENT_DATA"
                else None,
                "weight_flip_count": legacy._weight_flip_count(states) if states else None,
                "rolling_consistency_delta": legacy._variant_rolling_consistency_delta(
                    states, limited_states
                ),
                "stability_status": legacy._variant_stability_status(row, limited_row),
                **EXPERIMENT_FACTORY_SAFETY,
            }
        )
    return rows


def _batch_views(
    snapshot: Mapping[str, Any], *, batch_id: str, output_dir: Path
) -> tuple[dict[str, bytes], dict[str, Any]]:
    matrix_binding = _mapping(snapshot.get("source_experiment_matrix"))
    backfill_binding = _mapping(snapshot.get("source_paper_shadow_backfill"))
    backfill_manifest = _bundle_json(backfill_binding, "paper_shadow_backfill_manifest.json")
    variant_specs = _bundle_jsonl(matrix_binding, "variant_specs.jsonl")
    baseline_states = _bundle_jsonl(backfill_binding, "backfill_method_states.jsonl")
    _weight_rows_valid(baseline_states, label="paper backfill")
    returns, config, actual_start, actual_end = _price_returns_from_backfill(
        backfill_binding, baseline_states
    )
    eval_dates = {index.date().isoformat() for index in returns.index}
    baseline_eval = [row for row in baseline_states if row.get("date") in eval_dates]
    _weight_rows_valid(baseline_eval, label="paper backfill evaluation")
    labels = {
        index.date().isoformat(): legacy._risk_capped_regime_context_for_return(row, config)
        for index, row in returns.iterrows()
    }
    selection_policy = _candidate_selection_policy(_matrix_policy_payload(matrix_binding))
    variant_states: list[dict[str, Any]] = []
    for variant in variant_specs:
        variant_states.extend(
            _run_variant_weight_path(
                variant=variant,
                baseline_states=baseline_eval,
                returns=returns,
                labels=labels,
                config=config,
                selection_policy=selection_policy,
            )
        )
    performance = _performance_metrics(variant_states, baseline_eval)
    regime = _regime_metrics(variant_states, baseline_eval, labels, config)
    stability = _stability_metrics(variant_states, baseline_eval, config)
    variants = {_text(row.get("variant_id")) for row in variant_specs}
    _require(
        variants == {_text(row.get("variant_id")) for row in performance},
        "performance variant coverage mismatch",
    )
    _require(
        variants == {_text(row.get("variant_id")) for row in stability},
        "stability variant coverage mismatch",
    )
    _require(
        variants.issubset({_text(row.get("variant_id")) for row in regime}),
        "regime variant coverage mismatch",
    )
    data_quality = _mapping(snapshot.get("data_quality"))
    manifest = {
        "schema_version": 1,
        "report_type": "etf_dynamic_v3_batch_experiment_manifest",
        "batch_id": batch_id,
        "matrix_id": matrix_binding.get("artifact_id"),
        "source_backfill_id": backfill_binding.get("artifact_id"),
        "generated_at": snapshot.get("generated_at"),
        "status": "PASS",
        "market_regime": backfill_manifest.get("market_regime", "ai_after_chatgpt"),
        "requested_start_date": backfill_manifest.get("requested_start_date"),
        "requested_end_date": backfill_manifest.get("requested_end_date"),
        "date_start": actual_start.isoformat(),
        "date_end": actual_end.isoformat(),
        "data_quality_status": data_quality.get("status"),
        "data_quality_as_of": data_quality.get("as_of"),
        "data_quality_checked_at": data_quality.get("checked_at"),
        "variants_total": len(variant_specs),
        "variants_completed": len(performance),
        "batch_experiment_input_snapshot_path": str(
            output_dir / "batch_experiment_input_snapshot.json"
        ),
        "variant_weight_paths_path": str(output_dir / "variant_weight_paths.jsonl"),
        "variant_performance_metrics_path": str(output_dir / "variant_performance_metrics.jsonl"),
        "variant_regime_metrics_path": str(output_dir / "variant_regime_metrics.jsonl"),
        "variant_stability_metrics_path": str(output_dir / "variant_stability_metrics.jsonl"),
        "batch_experiment_report_path": str(output_dir / "batch_experiment_report.md"),
        **EXPERIMENT_FACTORY_SAFETY,
    }
    report = legacy.render_batch_experiment_report(manifest, performance, regime, stability)
    views = {
        "batch_experiment_input_snapshot.json": _json_bytes(dict(snapshot)),
        "batch_experiment_manifest.json": _json_bytes(manifest),
        "variant_weight_paths.jsonl": _jsonl_bytes(variant_states),
        "variant_performance_metrics.jsonl": _jsonl_bytes(performance),
        "variant_regime_metrics.jsonl": _jsonl_bytes(regime),
        "variant_stability_metrics.jsonl": _jsonl_bytes(stability),
        "batch_experiment_report.md": report.encode("utf-8"),
    }
    return views, {
        "manifest": manifest,
        "variant_weight_paths": variant_states,
        "variant_performance_metrics": performance,
        "variant_regime_metrics": regime,
        "variant_stability_metrics": stability,
    }


def run_batch_experiment(
    *,
    matrix_id: str,
    matrix_dir: Path = DEFAULT_EXPERIMENT_MATRIX_DIR,
    baseline_backfill_dir: Path = DEFAULT_PAPER_SHADOW_BACKFILL_DIR,
    output_dir: Path = DEFAULT_BATCH_EXPERIMENT_DIR,
    price_cache_path: Path | None = None,
    rates_cache_path: Path = DEFAULT_RATES_CACHE_PATH,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = _generated_at(generated_at)
    snapshot = _batch_snapshot(
        matrix_id=matrix_id,
        matrix_dir=matrix_dir,
        baseline_backfill_dir=baseline_backfill_dir,
        price_cache_path=price_cache_path,
        rates_cache_path=rates_cache_path,
        generated=generated,
    )
    batch_id = _stable_id("batch-experiment", snapshot)
    root = _unique_dir(output_dir / batch_id)
    views, payload = _batch_views(snapshot, batch_id=root.name, output_dir=root)
    _write(root, views, "latest_batch_experiment", "batch_experiment_manifest.json")
    return {"batch_id": root.name, "batch_dir": root, **payload}


def batch_experiment_report_payload(
    *,
    batch_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_BATCH_EXPERIMENT_DIR,
) -> dict[str, Any]:
    root = _artifact_root(
        output_dir=output_dir,
        artifact_id=batch_id,
        latest=latest,
        pointer_name="latest_batch_experiment",
    )
    return {
        **_read_json(root / "batch_experiment_manifest.json"),
        "variant_weight_paths": _read_jsonl(root / "variant_weight_paths.jsonl"),
        "variant_performance_metrics": _read_jsonl(root / "variant_performance_metrics.jsonl"),
        "variant_regime_metrics": _read_jsonl(root / "variant_regime_metrics.jsonl"),
        "variant_stability_metrics": _read_jsonl(root / "variant_stability_metrics.jsonl"),
        "input_snapshot": _read_json(root / "batch_experiment_input_snapshot.json"),
        "batch_dir": str(root),
    }


def validate_batch_experiment_artifact(
    *, batch_id: str, output_dir: Path = DEFAULT_BATCH_EXPERIMENT_DIR
) -> dict[str, Any]:
    root = output_dir / batch_id
    snapshot = legacy._read_optional_json(root / "batch_experiment_input_snapshot.json") or {}
    errors = _validate_batch_snapshot(snapshot)
    mismatches: list[str] = []
    try:
        views, _ = _batch_views(snapshot, batch_id=batch_id, output_dir=root)
        mismatches = _view_errors(root, views)
    except Exception as exc:  # noqa: BLE001
        errors.append(str(exc))
    return _validation_payload(
        "etf_dynamic_v3_batch_experiment_validation",
        batch_id,
        [
            _check("snapshot_and_live_sources", not errors, "; ".join(errors)),
            _check("content_derived_views", not mismatches, ",".join(mismatches)),
        ],
    )


def _triage_snapshot(
    *, batch_id: str, batch_dir: Path, matrix_dir: Path, generated: datetime
) -> dict[str, Any]:
    batch_binding = _batch_binding(batch_id, batch_dir)
    batch_manifest = _bundle_json(batch_binding, "batch_experiment_manifest.json")
    _source_not_after(
        batch_binding,
        "batch_experiment_manifest.json",
        generated,
        field="batch generated_at",
    )
    matrix_id = _text(batch_manifest.get("matrix_id"))
    matrix_binding = _matrix_binding(matrix_id, matrix_dir)
    _source_not_after(
        matrix_binding,
        "experiment_matrix_manifest.json",
        generated,
        field="matrix generated_at",
    )
    _require(
        _bundle_json(matrix_binding, "experiment_matrix_manifest.json").get("matrix_id")
        == matrix_id,
        "triage matrix identity mismatch",
    )
    policy = _triage_policy(_matrix_policy_payload(matrix_binding))
    return {
        "schema_version": EXPERIMENT_TRIAGE_SNAPSHOT_SCHEMA,
        "generated_at": generated.isoformat(),
        "source_batch_experiment": batch_binding,
        "source_experiment_matrix": matrix_binding,
        "batch_id": batch_id,
        "matrix_id": matrix_id,
        "triage_policy": policy,
        "production_effect": "none",
    }


def _validate_triage_snapshot(snapshot: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    try:
        _require(
            snapshot.get("schema_version") == EXPERIMENT_TRIAGE_SNAPSHOT_SCHEMA,
            "triage snapshot schema invalid",
        )
        batch_binding = _mapping(snapshot.get("source_batch_experiment"))
        matrix_binding = _mapping(snapshot.get("source_experiment_matrix"))
        errors.extend(_validate_source_binding(batch_binding))
        errors.extend(_validate_source_binding(matrix_binding))
        generated = target_core._datetime(snapshot.get("generated_at"), field="triage generated_at")
        _source_not_after(
            batch_binding, "batch_experiment_manifest.json", generated, field="batch generated_at"
        )
        _source_not_after(
            matrix_binding,
            "experiment_matrix_manifest.json",
            generated,
            field="matrix generated_at",
        )
        batch_manifest = _bundle_json(batch_binding, "batch_experiment_manifest.json")
        _require(
            batch_binding.get("artifact_id") == snapshot.get("batch_id"), "triage batch id mismatch"
        )
        _require(
            matrix_binding.get("artifact_id") == snapshot.get("matrix_id"),
            "triage matrix id mismatch",
        )
        _require(
            batch_manifest.get("matrix_id") == matrix_binding.get("artifact_id"),
            "batch/matrix lineage mismatch",
        )
        policy = _triage_policy(_matrix_policy_payload(matrix_binding))
        _require(policy == _mapping(snapshot.get("triage_policy")), "triage policy drift")
    except Exception as exc:  # noqa: BLE001
        errors.append(str(exc))
    return errors


def _bounded_score(value: Any, bounds: Sequence[Any]) -> float | None:
    if not _finite(value) or len(bounds) != 2:
        return None
    lower, upper = float(bounds[0]), float(bounds[1])
    return max(0.0, min(1.0, (float(value) - lower) / (upper - lower)))


def _regime_score(
    rows: Sequence[Mapping[str, Any]], policy: Mapping[str, Any]
) -> tuple[float | None, list[str]]:
    valid = [row for row in rows if row.get("regime_status") != "INSUFFICIENT_DATA"]
    if len(valid) < int(policy.get("minimum_valid_regime_count", 1)):
        return None, []
    scores = _mapping(_mapping(policy.get("label_scores")).get("regime"))
    values = [scores.get(_text(row.get("regime_status"))) for row in valid]
    if not all(_finite(value) for value in values):
        return None, []
    improved = [_text(row.get("regime")) for row in valid if row.get("regime_status") == "IMPROVED"]
    return sum(float(value) for value in values) / len(values), improved


def _scorecard(
    batch_binding: Mapping[str, Any],
    matrix_binding: Mapping[str, Any],
    policy: Mapping[str, Any],
) -> list[dict[str, Any]]:
    batch_manifest = _bundle_json(batch_binding, "batch_experiment_manifest.json")
    performance = {
        _text(row.get("variant_id")): row
        for row in _bundle_jsonl(batch_binding, "variant_performance_metrics.jsonl")
    }
    stability = {
        _text(row.get("variant_id")): row
        for row in _bundle_jsonl(batch_binding, "variant_stability_metrics.jsonl")
    }
    regimes = _bundle_jsonl(batch_binding, "variant_regime_metrics.jsonl")
    variants = _bundle_jsonl(matrix_binding, "variant_specs.jsonl")
    weights = _mapping(policy.get("score_weights"))
    bounds = _mapping(policy.get("score_bounds"))
    labels = _mapping(policy.get("label_scores"))
    thresholds = _mapping(policy.get("hard_reject_thresholds"))
    rules = set(legacy._texts(policy.get("hard_reject_rules")))
    rows: list[dict[str, Any]] = []
    for spec in variants:
        variant_id = _text(spec.get("variant_id"))
        perf = _mapping(performance.get(variant_id))
        stable = _mapping(stability.get(variant_id))
        regime_rows = [row for row in regimes if row.get("variant_id") == variant_id]
        regime_component, improved_regimes = _regime_score(regime_rows, policy)
        count = len(_records(spec.get("transforms")))
        simplicity_key = (
            "one_transform" if count <= 1 else "two_transforms" if count == 2 else "multi_transform"
        )
        components: dict[str, float | None] = {
            "return_score": _bounded_score(
                perf.get("relative_to_limited_adjustment"), list(_mapping(bounds).get("return", []))
            ),
            "drawdown_score": _bounded_score(
                perf.get("drawdown_delta_vs_limited"), list(_mapping(bounds).get("drawdown", []))
            ),
            "rolling_consistency_score": (
                float(
                    _mapping(labels.get("rolling_consistency")).get(
                        _text(stable.get("rolling_consistency_delta"))
                    )
                )
                if _finite(
                    _mapping(labels.get("rolling_consistency")).get(
                        _text(stable.get("rolling_consistency_delta"))
                    )
                )
                else None
            ),
            "regime_score": regime_component,
            "turnover_score": (
                _bounded_score(
                    -float(perf["turnover_delta_vs_limited"]),
                    list(_mapping(bounds).get("turnover", [])),
                )
                if _finite(perf.get("turnover_delta_vs_limited"))
                else None
            ),
            "simplicity_score": float(_mapping(labels.get("simplicity")).get(simplicity_key)),
        }
        missing = [key for key, value in components.items() if value is None]
        if int(perf.get("transform_effective_rebalance_count") or 0) <= 0:
            missing.append("transform_effect")
        overall = None
        if not missing:
            overall = sum(
                float(components[f"{name}_score"]) * float(weights[name])
                for name in _TRIAGE_COMPONENTS
            )
        flags: list[str] = []
        if missing and "insufficient_required_metrics" in rules:
            flags.append("insufficient_required_metrics")
        if batch_manifest.get("data_quality_status") == "FAIL" and "data_quality_FAIL" in rules:
            flags.append("data_quality_FAIL")
        if (
            _finite(perf.get("drawdown_delta_vs_limited"))
            and float(perf["drawdown_delta_vs_limited"])
            < float(thresholds.get("max_drawdown_delta_floor"))
            and "max_drawdown_materially_worse" in rules
        ):
            flags.append("max_drawdown_materially_worse")
        if (
            stable.get("rolling_consistency_delta") == "WORSE"
            and "rolling_consistency_worse" in rules
        ):
            flags.append("rolling_consistency_worse")
        pressure = set(legacy._texts(thresholds.get("pressure_regimes")))
        if (
            any(
                row.get("regime") in pressure and row.get("regime_status") == "WORSE"
                for row in regime_rows
            )
            and "pressure_regime_performance_worse" in rules
        ):
            flags.append("pressure_regime_performance_worse")
        if (
            _finite(perf.get("turnover_delta_vs_limited"))
            and float(perf["turnover_delta_vs_limited"])
            > float(thresholds.get("turnover_delta_ceiling"))
            and "turnover_explodes" in rules
        ):
            flags.append("turnover_explodes")
        non_missing_flags = [flag for flag in flags if flag != "insufficient_required_metrics"]
        if "insufficient_required_metrics" in flags:
            decision = _text(policy.get("missing_metric_decision"), "DEFER")
        elif non_missing_flags:
            decision = "REJECT"
        elif (
            overall is not None
            and overall >= float(policy.get("promote_score"))
            and perf.get("performance_status") != "FAIL"
        ):
            decision = "PROMOTE_TO_FORMAL_RESEARCH_CANDIDATE"
        elif overall is not None and (
            overall >= float(policy.get("keep_testing_score"))
            or stable.get("rolling_consistency_delta") == "IMPROVED"
        ):
            decision = "KEEP_FOR_MORE_TESTING"
        else:
            decision = "REJECT"
        observed = {
            "relative_to_limited_adjustment": perf.get("relative_to_limited_adjustment"),
            "drawdown_delta_vs_limited": perf.get("drawdown_delta_vs_limited"),
            "turnover_delta_vs_limited": perf.get("turnover_delta_vs_limited"),
            "rolling_consistency_delta": stable.get("rolling_consistency_delta"),
            "improved_regimes": improved_regimes,
            "valid_regime_count": sum(
                row.get("regime_status") != "INSUFFICIENT_DATA" for row in regime_rows
            ),
            "transform_effective_rebalance_count": perf.get("transform_effective_rebalance_count"),
            "max_transform_weight_delta": perf.get("max_transform_weight_delta"),
        }
        rows.append(
            {
                "variant_id": variant_id,
                "overall_score": round(overall, 6) if overall is not None else None,
                "score_components": {
                    key: round(value, 6) if value is not None else None
                    for key, value in components.items()
                },
                "missing_score_components": missing,
                "observed_metrics": observed,
                "hard_reject_flags": flags,
                "triage_decision": decision,
                "reason": legacy._triage_reason(decision, flags, perf, stable),
                **EXPERIMENT_FACTORY_SAFETY,
            }
        )
    return sorted(
        rows,
        key=lambda row: (
            row.get("overall_score") is not None,
            float(row.get("overall_score") or -1.0),
            _text(row.get("variant_id")),
        ),
        reverse=True,
    )


def _promotion_candidates(
    scorecard: Sequence[Mapping[str, Any]],
    variants: Sequence[Mapping[str, Any]],
    policy: Mapping[str, Any],
) -> list[dict[str, Any]]:
    specs = {_text(row.get("variant_id")): row for row in variants}
    rows: list[dict[str, Any]] = []
    for row in scorecard:
        if row.get("triage_decision") != "PROMOTE_TO_FORMAL_RESEARCH_CANDIDATE":
            continue
        variant_id = _text(row.get("variant_id"))
        spec = _mapping(specs.get(variant_id))
        rows.append(
            {
                "variant_id": variant_id,
                "proposed_method_name": legacy._proposed_method_name(variant_id),
                "source_hypothesis_id": spec.get("hypothesis_id"),
                "promotion_reason": (
                    "Reviewed screening policy passed with complete observed metrics and no "
                    "hard-reject flags; formal implementation remains owner-controlled "
                    "research-only work."
                ),
                "implementation_complexity": _text(spec.get("complexity"), "MEDIUM"),
                "requires_formal_method": True,
                **EXPERIMENT_FACTORY_SAFETY,
            }
        )
    return rows[: int(policy.get("top_candidate_cap"))]


def _triage_summary(
    scorecard: Sequence[Mapping[str, Any]], candidates: Sequence[Mapping[str, Any]]
) -> dict[str, Any]:
    counts = {decision: 0 for decision in DEFAULT_TRIAGE_DECISIONS}
    for row in scorecard:
        counts[_text(row.get("triage_decision"))] = (
            counts.get(_text(row.get("triage_decision")), 0) + 1
        )
    top = _text(scorecard[0].get("variant_id")) if scorecard else "INSUFFICIENT_DATA"
    action = (
        "promote_top_variant"
        if candidates
        else "run_more_experiments"
        if counts.get("KEEP_FOR_MORE_TESTING", 0)
        else "continue_diagnosis"
        if counts.get("REJECT", 0) == len(scorecard)
        else "defer"
    )
    return {
        "schema_version": 1,
        "variants_total": len(scorecard),
        "promote_count": counts.get("PROMOTE_TO_FORMAL_RESEARCH_CANDIDATE", 0),
        "keep_testing_count": counts.get("KEEP_FOR_MORE_TESTING", 0),
        "reject_count": counts.get("REJECT", 0),
        "defer_count": counts.get("DEFER", 0),
        "top_variant": top,
        "recommended_next_action": action,
        **EXPERIMENT_FACTORY_SAFETY,
    }


def _triage_views(
    snapshot: Mapping[str, Any], *, triage_id: str, output_dir: Path
) -> tuple[dict[str, bytes], dict[str, Any]]:
    batch_binding = _mapping(snapshot.get("source_batch_experiment"))
    matrix_binding = _mapping(snapshot.get("source_experiment_matrix"))
    policy = _mapping(snapshot.get("triage_policy"))
    batch_manifest = _bundle_json(batch_binding, "batch_experiment_manifest.json")
    variants = _bundle_jsonl(matrix_binding, "variant_specs.jsonl")
    scorecard = _scorecard(batch_binding, matrix_binding, policy)
    candidates = _promotion_candidates(scorecard, variants, policy)
    rejected = [row for row in scorecard if row.get("triage_decision") == "REJECT"]
    summary = _triage_summary(scorecard, candidates)
    manifest = {
        "schema_version": 1,
        "report_type": "etf_dynamic_v3_experiment_triage_manifest",
        "triage_id": triage_id,
        "batch_id": batch_binding.get("artifact_id"),
        "matrix_id": matrix_binding.get("artifact_id"),
        "generated_at": snapshot.get("generated_at"),
        "status": "PASS",
        "triage_policy_version": policy.get("policy_version"),
        "market_regime": batch_manifest.get("market_regime", "ai_after_chatgpt"),
        "date_start": batch_manifest.get("date_start"),
        "date_end": batch_manifest.get("date_end"),
        "data_quality_status": batch_manifest.get("data_quality_status"),
        "experiment_triage_input_snapshot_path": str(
            output_dir / "experiment_triage_input_snapshot.json"
        ),
        "triage_manifest_path": str(output_dir / "triage_manifest.json"),
        "variant_scorecard_path": str(output_dir / "variant_scorecard.jsonl"),
        "promotion_candidates_path": str(output_dir / "promotion_candidates.jsonl"),
        "rejected_variants_path": str(output_dir / "rejected_variants.jsonl"),
        "triage_summary_path": str(output_dir / "triage_summary.json"),
        "triage_report_path": str(output_dir / "triage_report.md"),
        **EXPERIMENT_FACTORY_SAFETY,
    }
    report = legacy.render_experiment_triage_report(manifest, summary, scorecard)
    views = {
        "experiment_triage_input_snapshot.json": _json_bytes(dict(snapshot)),
        "triage_manifest.json": _json_bytes(manifest),
        "variant_scorecard.jsonl": _jsonl_bytes(scorecard),
        "promotion_candidates.jsonl": _jsonl_bytes(candidates),
        "rejected_variants.jsonl": _jsonl_bytes(rejected),
        "triage_summary.json": _json_bytes(summary),
        "triage_report.md": report.encode("utf-8"),
    }
    return views, {
        "manifest": manifest,
        "variant_scorecard": scorecard,
        "promotion_candidates": candidates,
        "rejected_variants": rejected,
        "triage_summary": summary,
    }


def run_experiment_triage(
    *,
    batch_id: str,
    batch_dir: Path = DEFAULT_BATCH_EXPERIMENT_DIR,
    matrix_dir: Path = DEFAULT_EXPERIMENT_MATRIX_DIR,
    output_dir: Path = DEFAULT_EXPERIMENT_TRIAGE_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = _generated_at(generated_at)
    snapshot = _triage_snapshot(
        batch_id=batch_id, batch_dir=batch_dir, matrix_dir=matrix_dir, generated=generated
    )
    triage_id = _stable_id("experiment-triage", snapshot)
    root = _unique_dir(output_dir / triage_id)
    views, payload = _triage_views(snapshot, triage_id=root.name, output_dir=root)
    _write(root, views, "latest_experiment_triage", "triage_manifest.json")
    return {"triage_id": root.name, "triage_dir": root, **payload}


def experiment_triage_report_payload(
    *,
    triage_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_EXPERIMENT_TRIAGE_DIR,
) -> dict[str, Any]:
    root = _artifact_root(
        output_dir=output_dir,
        artifact_id=triage_id,
        latest=latest,
        pointer_name="latest_experiment_triage",
    )
    return {
        **_read_json(root / "triage_manifest.json"),
        "variant_scorecard": _read_jsonl(root / "variant_scorecard.jsonl"),
        "promotion_candidates": _read_jsonl(root / "promotion_candidates.jsonl"),
        "rejected_variants": _read_jsonl(root / "rejected_variants.jsonl"),
        "triage_summary": _read_json(root / "triage_summary.json"),
        "input_snapshot": _read_json(root / "experiment_triage_input_snapshot.json"),
        "triage_dir": str(root),
    }


def validate_experiment_triage_artifact(
    *, triage_id: str, output_dir: Path = DEFAULT_EXPERIMENT_TRIAGE_DIR
) -> dict[str, Any]:
    root = output_dir / triage_id
    snapshot = legacy._read_optional_json(root / "experiment_triage_input_snapshot.json") or {}
    errors = _validate_triage_snapshot(snapshot)
    mismatches: list[str] = []
    try:
        views, _ = _triage_views(snapshot, triage_id=triage_id, output_dir=root)
        mismatches = _view_errors(root, views)
    except Exception as exc:  # noqa: BLE001
        errors.append(str(exc))
    return _validation_payload(
        "etf_dynamic_v3_experiment_triage_validation",
        triage_id,
        [
            _check("snapshot_and_live_sources", not errors, "; ".join(errors)),
            _check("content_derived_views", not mismatches, ",".join(mismatches)),
        ],
    )


def _interpretation_snapshot(
    *, triage_id: str, triage_dir: Path, matrix_dir: Path, generated: datetime
) -> dict[str, Any]:
    triage_binding = _triage_binding(triage_id, triage_dir)
    triage_manifest = _bundle_json(triage_binding, "triage_manifest.json")
    _source_not_after(
        triage_binding,
        "triage_manifest.json",
        generated,
        field="triage generated_at",
    )
    matrix_id = _text(triage_manifest.get("matrix_id"))
    matrix_binding = _matrix_binding(matrix_id, matrix_dir)
    _source_not_after(
        matrix_binding,
        "experiment_matrix_manifest.json",
        generated,
        field="matrix generated_at",
    )
    _require(
        _bundle_json(matrix_binding, "experiment_matrix_manifest.json").get("matrix_id")
        == matrix_id,
        "interpretation matrix identity mismatch",
    )
    return {
        "schema_version": TOP_VARIANT_INTERPRETATION_SNAPSHOT_SCHEMA,
        "generated_at": generated.isoformat(),
        "source_experiment_triage": triage_binding,
        "source_experiment_matrix": matrix_binding,
        "triage_id": triage_id,
        "matrix_id": matrix_id,
        "expected_and_observed_evidence_are_separate": True,
        "production_effect": "none",
    }


def _validate_interpretation_snapshot(snapshot: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    try:
        _require(
            snapshot.get("schema_version") == TOP_VARIANT_INTERPRETATION_SNAPSHOT_SCHEMA,
            "interpretation snapshot schema invalid",
        )
        triage_binding = _mapping(snapshot.get("source_experiment_triage"))
        matrix_binding = _mapping(snapshot.get("source_experiment_matrix"))
        errors.extend(_validate_source_binding(triage_binding))
        errors.extend(_validate_source_binding(matrix_binding))
        generated = target_core._datetime(
            snapshot.get("generated_at"), field="interpretation generated_at"
        )
        _source_not_after(
            triage_binding,
            "triage_manifest.json",
            generated,
            field="triage generated_at",
        )
        _source_not_after(
            matrix_binding,
            "experiment_matrix_manifest.json",
            generated,
            field="matrix generated_at",
        )
        triage_manifest = _bundle_json(triage_binding, "triage_manifest.json")
        _require(
            triage_binding.get("artifact_id") == snapshot.get("triage_id"),
            "interpretation triage id mismatch",
        )
        _require(
            matrix_binding.get("artifact_id") == snapshot.get("matrix_id"),
            "interpretation matrix id mismatch",
        )
        _require(
            triage_manifest.get("matrix_id") == matrix_binding.get("artifact_id"),
            "triage/matrix lineage mismatch",
        )
        _require(
            snapshot.get("expected_and_observed_evidence_are_separate") is True,
            "interpretation evidence boundary missing",
        )
    except Exception as exc:  # noqa: BLE001
        errors.append(str(exc))
    return errors


def _observed_screening_evidence(row: Mapping[str, Any]) -> list[str]:
    observed = _mapping(row.get("observed_metrics"))
    evidence: list[str] = []
    if (
        _finite(observed.get("relative_to_limited_adjustment"))
        and float(observed["relative_to_limited_adjustment"]) > 0.0
    ):
        evidence.append(f"observed_return_delta={observed['relative_to_limited_adjustment']}")
    if (
        _finite(observed.get("drawdown_delta_vs_limited"))
        and float(observed["drawdown_delta_vs_limited"]) > 0.0
    ):
        evidence.append(f"observed_drawdown_delta={observed['drawdown_delta_vs_limited']}")
    if (
        _finite(observed.get("turnover_delta_vs_limited"))
        and float(observed["turnover_delta_vs_limited"]) < 0.0
    ):
        evidence.append(f"observed_turnover_delta={observed['turnover_delta_vs_limited']}")
    if observed.get("rolling_consistency_delta") == "IMPROVED":
        evidence.append("observed_rolling_consistency=IMPROVED")
    improved_regimes = legacy._texts(observed.get("improved_regimes"))
    if improved_regimes:
        evidence.append("observed_improved_regimes=" + ",".join(improved_regimes))
    if int(observed.get("transform_effective_rebalance_count") or 0) > 0:
        evidence.append(
            "observed_effective_rebalances=" + str(observed["transform_effective_rebalance_count"])
        )
    if not evidence:
        evidence.append("no_positive_observed_component_beyond_policy_composite_score")
    return evidence


def _observed_screening_costs(row: Mapping[str, Any]) -> list[str]:
    observed = _mapping(row.get("observed_metrics"))
    costs: list[str] = []
    for field, label, costly_when_positive in (
        ("relative_to_limited_adjustment", "return_delta", False),
        ("drawdown_delta_vs_limited", "drawdown_delta", False),
        ("turnover_delta_vs_limited", "turnover_delta", True),
    ):
        value = observed.get(field)
        if _finite(value):
            costly = float(value) > 0.0 if costly_when_positive else float(value) < 0.0
            if costly:
                costs.append(f"observed_{label}={value}")
    if observed.get("rolling_consistency_delta") == "WORSE":
        costs.append("observed_rolling_consistency=WORSE")
    if not costs:
        costs.append("no_observed_cost_in_screening_metrics")
    return costs


def _top_variant_explanations(
    triage_binding: Mapping[str, Any], matrix_binding: Mapping[str, Any]
) -> list[dict[str, Any]]:
    specs = {
        _text(row.get("variant_id")): row
        for row in _bundle_jsonl(matrix_binding, "variant_specs.jsonl")
    }
    scorecard = _bundle_jsonl(triage_binding, "variant_scorecard.jsonl")
    selected = [
        row
        for row in scorecard
        if row.get("triage_decision")
        in {"PROMOTE_TO_FORMAL_RESEARCH_CANDIDATE", "KEEP_FOR_MORE_TESTING"}
    ][:5]
    rows: list[dict[str, Any]] = []
    for score in selected:
        variant_id = _text(score.get("variant_id"))
        spec = _mapping(specs.get(variant_id))
        transforms = _records(spec.get("transforms"))
        _require(bool(spec), f"interpretation variant spec missing: {variant_id}")
        observed = _observed_screening_evidence(score)
        observed_costs = _observed_screening_costs(score)
        rows.append(
            {
                "variant_id": variant_id,
                "triage_decision": score.get("triage_decision"),
                "what_it_changes": [
                    legacy._describe_transform(transform) for transform in transforms
                ],
                "expected_benefit_hypothesis": legacy._texts(spec.get("expected_benefit")),
                "observed_screening_evidence": observed,
                "why_it_helped": observed,
                "expected_cost_hypothesis": legacy._texts(spec.get("expected_cost")),
                "expected_costs": legacy._texts(spec.get("expected_cost")),
                "observed_screening_costs": observed_costs,
                "what_it_costs": observed_costs,
                "best_regimes": legacy._texts(
                    _mapping(score.get("observed_metrics")).get("improved_regimes")
                )
                or ["no_regime_improvement_observed"],
                "weak_regimes": legacy._weak_regimes_for_variant(spec),
                "implementation_risk": legacy._implementation_risk(spec),
                "recommended_promotion": score.get("triage_decision")
                == "PROMOTE_TO_FORMAL_RESEARCH_CANDIDATE",
                "not_a_formal_method_or_investment_conclusion": True,
                **EXPERIMENT_FACTORY_SAFETY,
            }
        )
    return rows


def _failure_mode_coverage(
    variants: Sequence[Mapping[str, Any]], explanations: Sequence[Mapping[str, Any]]
) -> dict[str, Any]:
    selected = {_text(row.get("variant_id")): row for row in explanations}
    rows: list[dict[str, Any]] = []
    for mode in DEFAULT_FAILURE_MODES:
        covered = [
            _text(row.get("variant_id"))
            for row in variants
            if mode in legacy._texts(row.get("target_failure_modes"))
        ]
        selected_ids = [variant_id for variant_id in covered if variant_id in selected]
        observed = [
            variant_id
            for variant_id in selected_ids
            if _mapping(selected[variant_id]).get("observed_screening_evidence")
            != ["no_positive_observed_component_beyond_policy_composite_score"]
        ]
        status = (
            "OBSERVED_SUPPORT" if observed else "HYPOTHESIS_COVERAGE_ONLY" if covered else "MISSING"
        )
        rows.append(
            {
                "failure_mode": mode,
                "covered_by_variants": covered,
                "selected_variants": selected_ids,
                "observed_support_variants": observed,
                "coverage_status": status,
                **EXPERIMENT_FACTORY_SAFETY,
            }
        )
    return {"schema_version": 1, "failure_modes": rows, **EXPERIMENT_FACTORY_SAFETY}


def _recommended_variant(explanations: Sequence[Mapping[str, Any]]) -> str:
    promoted = next(
        (
            _text(row.get("variant_id"))
            for row in explanations
            if row.get("recommended_promotion") is True
        ),
        "",
    )
    if promoted:
        return promoted
    return _text(explanations[0].get("variant_id")) if explanations else "INSUFFICIENT_DATA"


def _interpretation_views(
    snapshot: Mapping[str, Any], *, interpretation_id: str, output_dir: Path
) -> tuple[dict[str, bytes], dict[str, Any]]:
    triage_binding = _mapping(snapshot.get("source_experiment_triage"))
    matrix_binding = _mapping(snapshot.get("source_experiment_matrix"))
    explanations = _top_variant_explanations(triage_binding, matrix_binding)
    variants = _bundle_jsonl(matrix_binding, "variant_specs.jsonl")
    coverage = _failure_mode_coverage(variants, explanations)
    recommended = _recommended_variant(explanations)
    manifest = {
        "schema_version": 1,
        "report_type": "etf_dynamic_v3_top_variant_interpretation_manifest",
        "interpretation_id": interpretation_id,
        "triage_id": triage_binding.get("artifact_id"),
        "matrix_id": matrix_binding.get("artifact_id"),
        "generated_at": snapshot.get("generated_at"),
        "status": "PASS" if explanations else "DEFER",
        "top_variant_count": len(explanations),
        "recommended_variant": recommended,
        "evidence_semantics": "expected hypotheses are separate from observed screening evidence",
        "top_variant_interpretation_input_snapshot_path": str(
            output_dir / "top_variant_interpretation_input_snapshot.json"
        ),
        "top_variant_interpretation_manifest_path": str(
            output_dir / "top_variant_interpretation_manifest.json"
        ),
        "top_variant_explanations_path": str(output_dir / "top_variant_explanations.jsonl"),
        "variant_failure_mode_coverage_path": str(
            output_dir / "variant_failure_mode_coverage.json"
        ),
        "top_variant_interpretation_report_path": str(
            output_dir / "top_variant_interpretation_report.md"
        ),
        "reader_brief_section_path": str(output_dir / "reader_brief_section.md"),
        **EXPERIMENT_FACTORY_SAFETY,
    }
    report = legacy.render_top_variant_interpretation_report(manifest, explanations, coverage)
    reader = legacy.render_top_variant_interpretation_reader_brief(manifest, explanations)
    views = {
        "top_variant_interpretation_input_snapshot.json": _json_bytes(dict(snapshot)),
        "top_variant_interpretation_manifest.json": _json_bytes(manifest),
        "top_variant_explanations.jsonl": _jsonl_bytes(explanations),
        "variant_failure_mode_coverage.json": _json_bytes(coverage),
        "top_variant_interpretation_report.md": report.encode("utf-8"),
        "reader_brief_section.md": reader.encode("utf-8"),
    }
    return views, {
        "manifest": manifest,
        "top_variant_explanations": explanations,
        "variant_failure_mode_coverage": coverage,
        "reader_brief_section": reader,
    }


def run_top_variant_interpretation(
    *,
    triage_id: str,
    triage_dir: Path = DEFAULT_EXPERIMENT_TRIAGE_DIR,
    matrix_dir: Path = DEFAULT_EXPERIMENT_MATRIX_DIR,
    output_dir: Path = DEFAULT_TOP_VARIANT_INTERPRETATION_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = _generated_at(generated_at)
    snapshot = _interpretation_snapshot(
        triage_id=triage_id, triage_dir=triage_dir, matrix_dir=matrix_dir, generated=generated
    )
    interpretation_id = _stable_id("top-variant-interpretation", snapshot)
    root = _unique_dir(output_dir / interpretation_id)
    views, payload = _interpretation_views(snapshot, interpretation_id=root.name, output_dir=root)
    _write(
        root,
        views,
        "latest_top_variant_interpretation",
        "top_variant_interpretation_manifest.json",
    )
    return {"interpretation_id": root.name, "interpretation_dir": root, **payload}


def top_variant_interpretation_report_payload(
    *,
    interpretation_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_TOP_VARIANT_INTERPRETATION_DIR,
) -> dict[str, Any]:
    root = _artifact_root(
        output_dir=output_dir,
        artifact_id=interpretation_id,
        latest=latest,
        pointer_name="latest_top_variant_interpretation",
    )
    return {
        **_read_json(root / "top_variant_interpretation_manifest.json"),
        "top_variant_explanations": _read_jsonl(root / "top_variant_explanations.jsonl"),
        "variant_failure_mode_coverage": _read_json(root / "variant_failure_mode_coverage.json"),
        "reader_brief_section": (root / "reader_brief_section.md").read_text(encoding="utf-8"),
        "input_snapshot": _read_json(root / "top_variant_interpretation_input_snapshot.json"),
        "interpretation_dir": str(root),
    }


def validate_top_variant_interpretation_artifact(
    *, interpretation_id: str, output_dir: Path = DEFAULT_TOP_VARIANT_INTERPRETATION_DIR
) -> dict[str, Any]:
    root = output_dir / interpretation_id
    snapshot = (
        legacy._read_optional_json(root / "top_variant_interpretation_input_snapshot.json") or {}
    )
    errors = _validate_interpretation_snapshot(snapshot)
    mismatches: list[str] = []
    try:
        views, _ = _interpretation_views(
            snapshot, interpretation_id=interpretation_id, output_dir=root
        )
        mismatches = _view_errors(root, views)
    except Exception as exc:  # noqa: BLE001
        errors.append(str(exc))
    return _validation_payload(
        "etf_dynamic_v3_top_variant_interpretation_validation",
        interpretation_id,
        [
            _check("snapshot_and_live_sources", not errors, "; ".join(errors)),
            _check("content_derived_views", not mismatches, ",".join(mismatches)),
        ],
    )


def _promotion_snapshot(
    *,
    triage_id: str,
    interpretation_id: str,
    triage_dir: Path,
    interpretation_dir: Path,
    generated: datetime,
) -> dict[str, Any]:
    triage_binding = _triage_binding(triage_id, triage_dir)
    interpretation_binding = _interpretation_binding(interpretation_id, interpretation_dir)
    _source_not_after(
        triage_binding,
        "triage_manifest.json",
        generated,
        field="triage generated_at",
    )
    _source_not_after(
        interpretation_binding,
        "top_variant_interpretation_manifest.json",
        generated,
        field="interpretation generated_at",
    )
    interpretation_manifest = _bundle_json(
        interpretation_binding, "top_variant_interpretation_manifest.json"
    )
    _require(
        interpretation_manifest.get("triage_id") == triage_id,
        "promotion triage/interpretation lineage mismatch",
    )
    return {
        "schema_version": METHOD_PROMOTION_PLAN_SNAPSHOT_SCHEMA,
        "generated_at": generated.isoformat(),
        "source_experiment_triage": triage_binding,
        "source_top_variant_interpretation": interpretation_binding,
        "triage_id": triage_id,
        "interpretation_id": interpretation_id,
        "formal_method_implementation_or_auto_apply_allowed": False,
        "production_effect": "none",
    }


def _validate_promotion_snapshot(snapshot: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    try:
        _require(
            snapshot.get("schema_version") == METHOD_PROMOTION_PLAN_SNAPSHOT_SCHEMA,
            "promotion snapshot schema invalid",
        )
        triage_binding = _mapping(snapshot.get("source_experiment_triage"))
        interpretation_binding = _mapping(snapshot.get("source_top_variant_interpretation"))
        errors.extend(_validate_source_binding(triage_binding))
        errors.extend(_validate_source_binding(interpretation_binding))
        generated = target_core._datetime(
            snapshot.get("generated_at"), field="promotion generated_at"
        )
        _source_not_after(
            triage_binding,
            "triage_manifest.json",
            generated,
            field="triage generated_at",
        )
        _source_not_after(
            interpretation_binding,
            "top_variant_interpretation_manifest.json",
            generated,
            field="interpretation generated_at",
        )
        interpretation_manifest = _bundle_json(
            interpretation_binding, "top_variant_interpretation_manifest.json"
        )
        _require(
            triage_binding.get("artifact_id") == snapshot.get("triage_id"),
            "promotion triage id mismatch",
        )
        _require(
            interpretation_binding.get("artifact_id") == snapshot.get("interpretation_id"),
            "promotion interpretation id mismatch",
        )
        _require(
            interpretation_manifest.get("triage_id") == triage_binding.get("artifact_id"),
            "promotion exact lineage mismatch",
        )
        _require(
            snapshot.get("formal_method_implementation_or_auto_apply_allowed") is False,
            "promotion safety boundary invalid",
        )
    except Exception as exc:  # noqa: BLE001
        errors.append(str(exc))
    return errors


def _method_specs(
    triage_binding: Mapping[str, Any], interpretation_binding: Mapping[str, Any]
) -> dict[str, Any]:
    explanations = {
        _text(row.get("variant_id")): row
        for row in _bundle_jsonl(interpretation_binding, "top_variant_explanations.jsonl")
    }
    methods: list[dict[str, Any]] = []
    for candidate in _bundle_jsonl(triage_binding, "promotion_candidates.jsonl"):
        variant_id = _text(candidate.get("variant_id"))
        explanation = _mapping(explanations.get(variant_id))
        _require(bool(explanation), f"promotion candidate interpretation missing: {variant_id}")
        _require(
            explanation.get("recommended_promotion") is True,
            "promotion candidate not recommended by interpretation",
        )
        methods.append(
            {
                "proposed_method_name": candidate.get("proposed_method_name"),
                "source_variant_id": variant_id,
                "base_method": "limited_adjustment",
                "implementation_scope": "research_only",
                "expected_benefit_hypothesis": legacy._texts(
                    explanation.get("expected_benefit_hypothesis")
                ),
                "observed_screening_evidence": legacy._texts(
                    explanation.get("observed_screening_evidence")
                ),
                "observed_screening_costs": legacy._texts(
                    explanation.get("observed_screening_costs")
                ),
                "expected_benefit": legacy._texts(explanation.get("expected_benefit_hypothesis")),
                "expected_cost_hypothesis": legacy._texts(
                    explanation.get("expected_cost_hypothesis")
                ),
                "expected_cost": legacy._texts(explanation.get("expected_cost_hypothesis")),
                "required_validation_after_implementation": [
                    "paper shadow backfill",
                    "rolling eval",
                    "regime review",
                    "stability diagnostics",
                    "forward confirmation",
                ],
                "owner_approval_required": True,
                "auto_apply": False,
                "broker_action_allowed": False,
                "production_effect": "none",
                **EXPERIMENT_FACTORY_SAFETY,
            }
        )
    return {
        "schema_version": 1,
        "methods": methods,
        "next_action": (
            "owner_review_then_formal_research_method_task"
            if methods
            else "run_more_experiments_before_promotion"
        ),
        **EXPERIMENT_FACTORY_SAFETY,
    }


def _promotion_views(
    snapshot: Mapping[str, Any], *, promotion_plan_id: str, output_dir: Path
) -> tuple[dict[str, bytes], dict[str, Any]]:
    triage_binding = _mapping(snapshot.get("source_experiment_triage"))
    interpretation_binding = _mapping(snapshot.get("source_top_variant_interpretation"))
    method_specs = _method_specs(triage_binding, interpretation_binding)
    methods = _records(method_specs.get("methods"))
    manifest = {
        "schema_version": 1,
        "report_type": "etf_dynamic_v3_method_promotion_plan_manifest",
        "promotion_plan_id": promotion_plan_id,
        "triage_id": triage_binding.get("artifact_id"),
        "interpretation_id": interpretation_binding.get("artifact_id"),
        "generated_at": snapshot.get("generated_at"),
        "status": "PASS" if methods else "DEFER",
        "proposed_method_names": [row.get("proposed_method_name") for row in methods],
        "implementation_scope": "research_only",
        "method_promotion_plan_input_snapshot_path": str(
            output_dir / "method_promotion_plan_input_snapshot.json"
        ),
        "method_promotion_manifest_path": str(output_dir / "method_promotion_manifest.json"),
        "promoted_method_specs_path": str(output_dir / "promoted_method_specs.json"),
        "formal_implementation_plan_path": str(output_dir / "formal_implementation_plan.md"),
        "owner_review_checklist_path": str(output_dir / "owner_review_checklist.md"),
        "method_promotion_plan_report_path": str(output_dir / "method_promotion_plan_report.md"),
        "reader_brief_section_path": str(output_dir / "reader_brief_section.md"),
        **EXPERIMENT_FACTORY_SAFETY,
    }
    plan = legacy.render_formal_implementation_plan(manifest, method_specs)
    checklist = legacy.render_method_promotion_owner_checklist(method_specs)
    report = legacy.render_method_promotion_plan_report(manifest, method_specs)
    reader = legacy.render_method_promotion_reader_brief(manifest, method_specs)
    views = {
        "method_promotion_plan_input_snapshot.json": _json_bytes(dict(snapshot)),
        "method_promotion_manifest.json": _json_bytes(manifest),
        "promoted_method_specs.json": _json_bytes(method_specs),
        "formal_implementation_plan.md": plan.encode("utf-8"),
        "owner_review_checklist.md": checklist.encode("utf-8"),
        "method_promotion_plan_report.md": report.encode("utf-8"),
        "reader_brief_section.md": reader.encode("utf-8"),
    }
    return views, {
        "manifest": manifest,
        "promoted_method_specs": method_specs,
        "formal_implementation_plan": plan,
        "owner_review_checklist": checklist,
        "reader_brief_section": reader,
    }


def run_method_promotion_plan(
    *,
    triage_id: str,
    interpretation_id: str,
    triage_dir: Path = DEFAULT_EXPERIMENT_TRIAGE_DIR,
    interpretation_dir: Path = DEFAULT_TOP_VARIANT_INTERPRETATION_DIR,
    output_dir: Path = DEFAULT_METHOD_PROMOTION_PLAN_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = _generated_at(generated_at)
    snapshot = _promotion_snapshot(
        triage_id=triage_id,
        interpretation_id=interpretation_id,
        triage_dir=triage_dir,
        interpretation_dir=interpretation_dir,
        generated=generated,
    )
    promotion_plan_id = _stable_id("method-promotion-plan", snapshot)
    root = _unique_dir(output_dir / promotion_plan_id)
    views, payload = _promotion_views(snapshot, promotion_plan_id=root.name, output_dir=root)
    _write(root, views, "latest_method_promotion_plan", "method_promotion_manifest.json")
    return {"promotion_plan_id": root.name, "promotion_plan_dir": root, **payload}


def method_promotion_plan_report_payload(
    *,
    promotion_plan_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_METHOD_PROMOTION_PLAN_DIR,
) -> dict[str, Any]:
    root = _artifact_root(
        output_dir=output_dir,
        artifact_id=promotion_plan_id,
        latest=latest,
        pointer_name="latest_method_promotion_plan",
    )
    return {
        **_read_json(root / "method_promotion_manifest.json"),
        "promoted_method_specs": _read_json(root / "promoted_method_specs.json"),
        "formal_implementation_plan": (root / "formal_implementation_plan.md").read_text(
            encoding="utf-8"
        ),
        "owner_review_checklist": (root / "owner_review_checklist.md").read_text(encoding="utf-8"),
        "reader_brief_section": (root / "reader_brief_section.md").read_text(encoding="utf-8"),
        "input_snapshot": _read_json(root / "method_promotion_plan_input_snapshot.json"),
        "promotion_plan_dir": str(root),
    }


def validate_method_promotion_plan_artifact(
    *, promotion_plan_id: str, output_dir: Path = DEFAULT_METHOD_PROMOTION_PLAN_DIR
) -> dict[str, Any]:
    root = output_dir / promotion_plan_id
    snapshot = legacy._read_optional_json(root / "method_promotion_plan_input_snapshot.json") or {}
    errors = _validate_promotion_snapshot(snapshot)
    mismatches: list[str] = []
    try:
        views, payload = _promotion_views(
            snapshot, promotion_plan_id=promotion_plan_id, output_dir=root
        )
        methods = _records(_mapping(payload.get("promoted_method_specs")).get("methods"))
        names = [_text(row.get("proposed_method_name")) for row in methods]
        _require(all(names) and len(names) == len(set(names)), "promotion method names invalid")
        _require(
            all(
                row.get("implementation_scope") == "research_only"
                and row.get("owner_approval_required") is True
                and row.get("auto_apply") is False
                and row.get("broker_action_allowed") is False
                and row.get("production_effect") == "none"
                for row in methods
            ),
            "promotion method safety invalid",
        )
        mismatches = _view_errors(root, views)
    except Exception as exc:  # noqa: BLE001
        errors.append(str(exc))
    return _validation_payload(
        "etf_dynamic_v3_method_promotion_plan_validation",
        promotion_plan_id,
        [
            _check("snapshot_and_live_sources", not errors, "; ".join(errors)),
            _check("content_derived_views", not mismatches, ",".join(mismatches)),
        ],
    )
