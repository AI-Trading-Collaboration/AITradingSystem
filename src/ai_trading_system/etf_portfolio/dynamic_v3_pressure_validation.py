from __future__ import annotations

import math
from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from datetime import date, datetime
from pathlib import Path
from typing import Any

import yaml

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.etf_portfolio.dynamic_v3_backtest_simulation import (
    DEFAULT_BACKTEST_SIM_OUTCOME_DIR,
)
from ai_trading_system.etf_portfolio.dynamic_v3_confirmation_operations import (
    DEFAULT_CONFIRMATION_CYCLE_WEEKLY_DIR,
    DEFAULT_PRESSURE_REGIME_TAG_DIR,
    PRESSURE_TAGS,
    PRESSURE_VALIDATION_TAGS,
    _file_bytes_match,
    _json_bytes,
    _jsonl_bytes,
    _operations_datetime,
    _operations_generated_at,
    _operations_source_bundle,
    _report_input_snapshot,
    _semantic_artifact_id,
    _source_not_after_cutoff,
    _validate_operations_source_bundle,
)
from ai_trading_system.etf_portfolio.dynamic_v3_historical_replay import (
    DEFAULT_BACKFILLED_OUTCOME_DIR,
    _artifact_dir_from_latest,
    _check,
    _date_from_any,
    _float,
    _int,
    _mapping,
    _read_json,
    _read_jsonl,
    _read_optional_json,
    _records,
    _stable_id,
    _text,
    _unique_dir,
    _update_latest_pointer,
    _validation_payload,
)
from ai_trading_system.etf_portfolio.dynamic_v3_paper_tracking import (
    DEFAULT_ADVISORY_OUTCOME_DIR,
)
from ai_trading_system.etf_portfolio.dynamic_v3_parameter_research import (
    DEFAULT_DYNAMIC_V3_RESEARCH_ROOT,
    DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    SCHEMA_VERSION,
)
from ai_trading_system.platform.artifacts.writer import write_bytes_atomic

DEFAULT_PRESSURE_TAG_DIAGNOSIS_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "pressure_tag_diagnosis"
DEFAULT_PRESSURE_OUTCOME_BACKFILL_DIR = (
    DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "pressure_outcome_backfill"
)
DEFAULT_DEFENSIVE_PRESSURE_COMPARE_DIR = (
    DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "defensive_pressure_compare"
)
DEFAULT_DEFENSIVE_RULE_REVIEW_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "defensive_rule_review"
DEFAULT_WEEKLY_OPS_DECISION_UPDATE_DIR = (
    DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "weekly_ops_decision_update"
)
DEFAULT_SIM_DEFENSIVE_VALIDATION_POLICY_PATH = (
    PROJECT_ROOT
    / "config"
    / "etf_portfolio"
    / "dynamic_v3_rescue"
    / "sim_defensive_validation_v1.yaml"
)
DEFAULT_FORWARD_PRESSURE_CAPTURE_POLICY_PATH = (
    PROJECT_ROOT
    / "config"
    / "etf_portfolio"
    / "dynamic_v3_rescue"
    / "forward_pressure_capture_v1.yaml"
)

PRESSURE_TAG_DIAGNOSIS_SNAPSHOT_SCHEMA_VERSION = "pressure_tag_diagnosis_input_snapshot.v2"
PRESSURE_OUTCOME_BACKFILL_SNAPSHOT_SCHEMA_VERSION = "pressure_outcome_backfill_input_snapshot.v2"
DEFENSIVE_PRESSURE_COMPARE_SNAPSHOT_SCHEMA_VERSION = "defensive_pressure_compare_input_snapshot.v2"
DEFENSIVE_RULE_REVIEW_SNAPSHOT_SCHEMA_VERSION = "defensive_rule_review_input_snapshot.v2"
WEEKLY_OPS_DECISION_UPDATE_SNAPSHOT_SCHEMA_VERSION = "weekly_ops_decision_update_input_snapshot.v2"

SOURCE_MODES = ("FORWARD_OUTCOME", "HISTORICAL_REPLAY", "BACKTEST_SIMULATION")
EVIDENCE_QUALITY_BY_SOURCE = {
    "FORWARD_OUTCOME": "FORWARD",
    "HISTORICAL_REPLAY": "PIT_WARNING",
    "BACKTEST_SIMULATION": "SIMULATION_NOT_PIT",
}
COMPARISON_VARIANTS = (
    "no_trade",
    "defensive_limited_adjustment",
    "limited_adjustment",
    "consensus_target",
)
PRESSURE_REGIMES = ("tech_drawdown", "risk_off", "semiconductor_pullback")

# Diagnostic-only bands: they do not mutate tagging policy or define trading thresholds.
NEAR_MISS_ABS_DISTANCE = 0.005
NEAR_MISS_VOL_DISTANCE = 0.02


class DynamicV3PressureValidationError(ValueError):
    """Raised when pressure-regime validation artifacts fail closed."""


def _pressure_generated_at(value: datetime | None) -> datetime:
    try:
        return _operations_generated_at(value)
    except Exception as exc:  # noqa: BLE001
        raise DynamicV3PressureValidationError(str(exc)) from exc


def _pressure_datetime(value: Any, *, field: str) -> datetime:
    try:
        return _operations_datetime(value, field=field)
    except Exception as exc:  # noqa: BLE001
        raise DynamicV3PressureValidationError(str(exc)) from exc


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise DynamicV3PressureValidationError(message)


def _finite_number(value: Any) -> bool:
    return (
        isinstance(value, int | float)
        and not isinstance(value, bool)
        and math.isfinite(float(value))
    )


def _policy_snapshot(path: Path) -> dict[str, Any]:
    policy = _read_yaml(path)
    metadata = _mapping(policy.get("policy_metadata"))
    _require(bool(_text(metadata.get("owner"))), f"policy owner missing: {path}")
    _require(bool(_text(metadata.get("version"))), f"policy version missing: {path}")
    _require(bool(_text(metadata.get("status"))), f"policy status missing: {path}")
    _require(bool(_text(metadata.get("rationale"))), f"policy rationale missing: {path}")
    _require(
        bool(_text(metadata.get("review_condition"))), f"policy review condition missing: {path}"
    )
    return {
        "path": str(path),
        "payload": policy,
        "bundle": _operations_source_bundle(
            source_dir=path.parent,
            canonical_files=(path.name,),
        ),
    }


def _validate_policy_live(snapshot: Mapping[str, Any]) -> list[str]:
    errors = _validate_operations_source_bundle(_mapping(snapshot.get("bundle")))
    path = Path(_text(snapshot.get("path")))
    try:
        if _read_yaml(path) != _mapping(snapshot.get("payload")):
            errors.append(f"policy payload drift: {path}")
    except Exception as exc:  # noqa: BLE001
        errors.append(f"policy invalid: {path}: {exc}")
    return errors


def _manifest_generated(
    manifest: Mapping[str, Any], *, generated: datetime, source_name: str
) -> datetime:
    try:
        return _source_not_after_cutoff(
            manifest,
            generated=generated,
            source_name=source_name,
        )
    except Exception as exc:  # noqa: BLE001
        raise DynamicV3PressureValidationError(str(exc)) from exc


def _validate_pressure_tag_source(source_dir: Path, *, generated: datetime) -> dict[str, Any]:
    manifest = _read_json(source_dir / "pressure_regime_manifest.json")
    tag_id = _text(manifest.get("tag_id"))
    _require(tag_id == source_dir.name, "pressure tag identity mismatch")
    _require(
        manifest.get("status") in {"PASS", "PASS_WITH_WARNINGS"}, "pressure tag status invalid"
    )
    source_generated = _manifest_generated(
        manifest, generated=generated, source_name="pressure regime tag"
    )
    _require(
        manifest.get("production_effect") == "none"
        and manifest.get("broker_action_allowed") is False,
        "pressure tag safety invalid",
    )
    windows = _read_jsonl(source_dir / "regime_window_tags.jsonl")
    outcomes = _read_jsonl(source_dir / "outcome_regime_tags.jsonl")
    summary = _read_json(source_dir / "pressure_regime_summary.json")
    window_ids: set[tuple[str, int]] = set()
    for row in windows:
        identity = (
            _text(row.get("window_id") or row.get("date") or row.get("end_date")),
            _int(row.get("window_days")),
        )
        _require(identity[0] != "" and identity[1] > 0, "pressure window identity invalid")
        _require(identity not in window_ids, "duplicate pressure window identity")
        window_ids.add(identity)
        _require(
            set(_records_to_texts(row.get("regime_tags"))) <= set(PRESSURE_TAGS),
            "pressure window tag invalid",
        )
        metrics = _mapping(row.get("metrics"))
        for field in ("qqq_drawdown", "smh_drawdown", "realized_volatility"):
            _require(_finite_number(metrics.get(field)), f"pressure metric invalid: {field}")
    outcome_ids: set[tuple[str, str, int]] = set()
    for row in outcomes:
        identity = (
            _text(row.get("outcome_id")),
            _text(row.get("daily_advisory_id")),
            _int(row.get("window_days")),
        )
        _require(all(identity[:2]) and identity[2] > 0, "pressure outcome identity invalid")
        _require(identity not in outcome_ids, "duplicate pressure outcome identity")
        outcome_ids.add(identity)
        _require(
            set(_records_to_texts(row.get("regime_tags"))) <= set(PRESSURE_TAGS),
            "pressure outcome tag invalid",
        )
    observed_pressure_windows = sum(
        bool(set(_records_to_texts(row.get("regime_tags"))) & PRESSURE_VALIDATION_TAGS)
        for row in windows
    )
    _require(
        _int(summary.get("pressure_window_count")) == observed_pressure_windows,
        "pressure summary window count drift",
    )
    return {
        "status": "PASS",
        "source_kind": "pressure_tag",
        "artifact_id": tag_id,
        "generated_at": source_generated.isoformat(),
        "window_count": len(windows),
        "outcome_count": len(outcomes),
    }


def _validate_sim_outcome_source(source_dir: Path, *, generated: datetime) -> dict[str, Any]:
    manifest = _read_json(source_dir / "sim_outcome_manifest.json")
    artifact_id = _text(manifest.get("sim_outcome_id"))
    _require(artifact_id == source_dir.name, "simulation outcome identity mismatch")
    _require(manifest.get("status") == "PASS", "simulation outcome status invalid")
    _require(manifest.get("outcome_mode") == "BACKTEST_SIMULATION", "simulation mode invalid")
    _require(
        manifest.get("pit_safety_status") == "SIMULATION_NOT_PIT"
        and manifest.get("production_effect") == "none"
        and manifest.get("broker_action_allowed") is False,
        "simulation safety invalid",
    )
    source_generated = _manifest_generated(
        manifest, generated=generated, source_name="backtest simulation outcome"
    )
    rows = _read_jsonl(source_dir / "simulated_outcome_windows.jsonl")
    identities: set[tuple[str, str, int, str, str]] = set()
    for row in rows:
        variant = _normalize_variant_name(_text(row.get("variant")))
        identity = (
            _text(row.get("sim_event_id")),
            _text(row.get("as_of") or row.get("start_date")),
            _int(row.get("window_days")),
            _text(row.get("regime_label")),
            variant,
        )
        _require(
            all(identity[:2]) and identity[2] > 0 and identity[3] != "",
            "simulation row identity invalid",
        )
        _require(variant in COMPARISON_VARIANTS, "simulation variant invalid")
        _require(identity not in identities, "duplicate simulation outcome row")
        identities.add(identity)
        status = _text(row.get("outcome_status"))
        _require(
            status in {"AVAILABLE", "PENDING", "INSUFFICIENT_DATA"},
            "simulation outcome status invalid",
        )
        if status == "AVAILABLE":
            for field in ("return", "max_drawdown", "turnover"):
                _require(_finite_number(row.get(field)), f"simulation metric invalid: {field}")
            for field in ("risk_asset_exposure", "risk_exposure", "equity_exposure"):
                if field in row:
                    _require(_finite_number(row.get(field)), f"simulation metric invalid: {field}")
    return {
        "status": "PASS",
        "source_kind": "simulation_outcome",
        "artifact_id": artifact_id,
        "generated_at": source_generated.isoformat(),
        "row_count": len(rows),
    }


def _validate_historical_outcome_source(source_dir: Path, *, generated: datetime) -> dict[str, Any]:
    manifest = _read_json(source_dir / "backfill_manifest.json")
    artifact_id = _text(manifest.get("backfill_id"))
    _require(artifact_id == source_dir.name, "historical backfill identity mismatch")
    _require(
        manifest.get("status") in {"PASS", "PASS_WITH_WARNINGS"},
        "historical backfill status invalid",
    )
    source_generated = _manifest_generated(
        manifest, generated=generated, source_name="historical replay outcome"
    )
    _require(
        manifest.get("production_effect") == "none"
        and manifest.get("broker_action_allowed") is False,
        "historical backfill safety invalid",
    )
    rows = _read_jsonl(source_dir / "replay_outcome_windows.jsonl")
    identities: set[tuple[str, str, int, str]] = set()
    for row in rows:
        variant = _normalize_variant_name(_text(row.get("variant")))
        identity = (
            _text(row.get("replay_event_id") or row.get("daily_advisory_id")),
            _text(row.get("as_of") or row.get("start_date")),
            _int(row.get("window_days")),
            variant,
        )
        _require(all(identity[:2]) and identity[2] > 0, "historical outcome identity invalid")
        _require(variant in COMPARISON_VARIANTS, "historical variant invalid")
        _require(identity not in identities, "duplicate historical outcome row")
        identities.add(identity)
        status = _text(row.get("outcome_status"))
        _require(
            status in {"AVAILABLE", "PENDING", "INSUFFICIENT_DATA"},
            "historical outcome status invalid",
        )
        if status == "AVAILABLE":
            for field in ("return", "max_drawdown", "turnover"):
                _require(_finite_number(row.get(field)), f"historical metric invalid: {field}")
    return {
        "status": "PASS",
        "source_kind": "historical_outcome",
        "artifact_id": artifact_id,
        "generated_at": source_generated.isoformat(),
        "row_count": len(rows),
    }


def _validate_advisory_outcome_source(source_dir: Path, *, generated: datetime) -> dict[str, Any]:
    manifest = _read_json(source_dir / "advisory_outcome_manifest.json")
    artifact_id = _text(manifest.get("outcome_id"))
    _require(artifact_id == source_dir.name, "advisory outcome identity mismatch")
    _require(
        manifest.get("status") in {"PASS", "PASS_WITH_WARNINGS"}, "advisory outcome status invalid"
    )
    source_generated = _manifest_generated(
        manifest, generated=generated, source_name="advisory outcome"
    )
    _require(
        manifest.get("production_effect") == "none"
        and manifest.get("broker_action_allowed") is False,
        "advisory outcome safety invalid",
    )
    rows = _read_jsonl(source_dir / "outcome_windows.jsonl")
    identities: set[tuple[str, int]] = set()
    for row in rows:
        identity = (
            _text(row.get("daily_advisory_id") or manifest.get("daily_advisory_id")),
            _int(row.get("window_days")),
        )
        _require(identity[0] != "" and identity[1] > 0, "advisory outcome row identity invalid")
        _require(identity not in identities, "duplicate advisory outcome row")
        identities.add(identity)
        status = _text(row.get("outcome_status"))
        _require(
            status in {"AVAILABLE", "PENDING", "INSUFFICIENT_DATA"},
            "advisory outcome status invalid",
        )
        if status == "AVAILABLE":
            for field in (
                "no_trade_return",
                "paper_portfolio_return",
                "target_weight_return",
                "max_drawdown",
            ):
                _require(
                    _finite_number(row.get(field)), f"advisory outcome metric invalid: {field}"
                )
    return {
        "status": "PASS",
        "source_kind": "advisory_outcome",
        "artifact_id": artifact_id,
        "generated_at": source_generated.isoformat(),
        "row_count": len(rows),
    }


def _validate_weekly_source(source_dir: Path, *, generated: datetime) -> dict[str, Any]:
    manifest = _read_json(source_dir / "weekly_cycle_manifest.json")
    summary = _read_json(source_dir / "weekly_cycle_summary.json")
    artifact_id = _text(manifest.get("weekly_cycle_id"))
    _require(artifact_id == source_dir.name, "weekly cycle identity mismatch")
    _require(
        manifest.get("status") in {"PASS", "PASS_WITH_WARNINGS"}, "weekly cycle status invalid"
    )
    _require(summary.get("weekly_cycle_id") == artifact_id, "weekly summary identity mismatch")
    source_generated = _manifest_generated(
        manifest, generated=generated, source_name="weekly cycle"
    )
    _require(
        manifest.get("production_effect") == "none"
        and manifest.get("broker_action_allowed") is False,
        "weekly cycle safety invalid",
    )
    return {
        "status": "PASS",
        "source_kind": "weekly_cycle",
        "artifact_id": artifact_id,
        "generated_at": source_generated.isoformat(),
    }


def _external_source_validation(
    source_kind: str, source_dir: Path, *, generated: datetime
) -> dict[str, Any]:
    if source_kind == "pressure_tag":
        return _validate_pressure_tag_source(source_dir, generated=generated)
    if source_kind == "simulation_outcome":
        return _validate_sim_outcome_source(source_dir, generated=generated)
    if source_kind == "historical_outcome":
        return _validate_historical_outcome_source(source_dir, generated=generated)
    if source_kind == "advisory_outcome":
        return _validate_advisory_outcome_source(source_dir, generated=generated)
    if source_kind == "weekly_cycle":
        return _validate_weekly_source(source_dir, generated=generated)
    if source_kind == "pressure_backfill":
        manifest = _read_json(source_dir / "pressure_backfill_manifest.json")
        artifact_id = _text(manifest.get("pressure_backfill_id"))
        validation = validate_pressure_outcome_backfill_artifact(
            backfill_id=artifact_id, output_dir=source_dir.parent
        )
        _require(validation.get("status") == "PASS", "pressure backfill validation failed")
        source_generated = _manifest_generated(
            manifest, generated=generated, source_name="pressure outcome backfill"
        )
        return {
            "status": "PASS",
            "source_kind": source_kind,
            "artifact_id": artifact_id,
            "generated_at": source_generated.isoformat(),
        }
    if source_kind == "defensive_compare":
        manifest = _read_json(source_dir / "defensive_pressure_compare_manifest.json")
        artifact_id = _text(manifest.get("comparison_id"))
        validation = validate_defensive_pressure_compare_artifact(
            comparison_id=artifact_id, output_dir=source_dir.parent
        )
        _require(validation.get("status") == "PASS", "defensive compare validation failed")
        source_generated = _manifest_generated(
            manifest, generated=generated, source_name="defensive pressure compare"
        )
        return {
            "status": "PASS",
            "source_kind": source_kind,
            "artifact_id": artifact_id,
            "generated_at": source_generated.isoformat(),
        }
    if source_kind == "defensive_review":
        manifest = _read_json(source_dir / "defensive_rule_review_manifest.json")
        artifact_id = _text(manifest.get("review_id"))
        validation = validate_defensive_rule_review_artifact(
            review_id=artifact_id, output_dir=source_dir.parent
        )
        _require(validation.get("status") == "PASS", "defensive review validation failed")
        source_generated = _manifest_generated(
            manifest, generated=generated, source_name="defensive rule review"
        )
        return {
            "status": "PASS",
            "source_kind": source_kind,
            "artifact_id": artifact_id,
            "generated_at": source_generated.isoformat(),
        }
    raise DynamicV3PressureValidationError(f"unknown source kind: {source_kind}")


def _source_binding(
    *,
    source_kind: str,
    source_dir: Path,
    generated: datetime,
    canonical_files: Sequence[str] | None = None,
    json_views: Sequence[str] = (),
    jsonl_views: Sequence[str] = (),
    text_views: Sequence[str] = (),
) -> dict[str, Any]:
    validation = _external_source_validation(source_kind, source_dir, generated=generated)
    return {
        "source_kind": source_kind,
        "artifact_id": validation["artifact_id"],
        "generated_at": validation["generated_at"],
        "validation": validation,
        "bundle": _operations_source_bundle(
            source_dir=source_dir,
            canonical_files=canonical_files,
            json_views=json_views,
            jsonl_views=jsonl_views,
            text_views=text_views,
        ),
    }


def _semantic_optional_binding(
    *,
    source_kind: str,
    output_dir: Path,
    manifest_name: str,
    id_key: str,
    generated: datetime,
    canonical_files: Sequence[str],
    json_views: Sequence[str] = (),
    jsonl_views: Sequence[str] = (),
) -> dict[str, Any] | None:
    if not output_dir.exists() or not any(output_dir.glob(f"*/{manifest_name}")):
        return None
    try:
        artifact_id = _semantic_artifact_id(
            output_dir=output_dir,
            artifact_id=None,
            manifest_name=manifest_name,
            id_key=id_key,
            generated=generated,
            source_name=source_kind,
            required=False,
        )
    except Exception as exc:  # noqa: BLE001
        raise DynamicV3PressureValidationError(str(exc)) from exc
    if not artifact_id:
        return None
    return _source_binding(
        source_kind=source_kind,
        source_dir=output_dir / artifact_id,
        generated=generated,
        canonical_files=canonical_files,
        json_views=json_views,
        jsonl_views=jsonl_views,
    )


def _validate_binding_live(binding: Mapping[str, Any], *, generated: datetime) -> list[str]:
    errors = _validate_operations_source_bundle(_mapping(binding.get("bundle")))
    try:
        source_dir = Path(_text(_mapping(binding.get("bundle")).get("source_dir")))
        validation = _external_source_validation(
            _text(binding.get("source_kind")), source_dir, generated=generated
        )
        if validation != _mapping(binding.get("validation")):
            errors.append(f"source validation drift: {source_dir}")
        if validation.get("artifact_id") != binding.get("artifact_id"):
            errors.append(f"source identity drift: {source_dir}")
    except Exception as exc:  # noqa: BLE001
        errors.append(str(exc))
    return errors


def _write_views_atomic(output_dir: Path, views: Mapping[str, bytes]) -> None:
    output_dir.mkdir(parents=True, exist_ok=False)
    for name, payload in views.items():
        write_bytes_atomic(output_dir / name, payload)


def _views_match(output_dir: Path, views: Mapping[str, bytes]) -> list[str]:
    return [
        name for name, payload in views.items() if not _file_bytes_match(output_dir / name, payload)
    ]


def _snapshot_validation_checks(
    *, snapshot: Mapping[str, Any], expected_schema: str, generated: datetime
) -> tuple[list[dict[str, Any]], list[str]]:
    errors: list[str] = []
    if snapshot.get("schema_version") != expected_schema:
        errors.append("snapshot schema invalid")
    for binding in _records(snapshot.get("source_bindings")):
        errors.extend(_validate_binding_live(binding, generated=generated))
    for policy in _records(snapshot.get("policy_bindings")):
        errors.extend(_validate_policy_live(policy))
    checks = [
        _check(
            "snapshot_schema", snapshot.get("schema_version") == expected_schema, expected_schema
        ),
        _check("live_sources_and_policies", not errors, "; ".join(errors)),
    ]
    return checks, errors


def _binding_by_kind(snapshot: Mapping[str, Any], source_kind: str) -> Mapping[str, Any] | None:
    matches = [
        row
        for row in _records(snapshot.get("source_bindings"))
        if row.get("source_kind") == source_kind
    ]
    _require(len(matches) <= 1, f"duplicate snapshot source kind: {source_kind}")
    return matches[0] if matches else None


def _bundle_json(binding: Mapping[str, Any], name: str) -> Mapping[str, Any]:
    return _mapping(_mapping(_mapping(binding.get("bundle")).get("json")).get(name))


def _bundle_jsonl(binding: Mapping[str, Any], name: str) -> list[Mapping[str, Any]]:
    return _records(_mapping(_mapping(binding.get("bundle")).get("jsonl")).get(name))


def _diagnosis_mapping_from_snapshot(
    snapshot: Mapping[str, Any],
    pressure_summary: Mapping[str, Any],
    outcome_tags: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    historical = _binding_by_kind(snapshot, "historical_outcome")
    simulation = _binding_by_kind(snapshot, "simulation_outcome")
    historical_rows = (
        _bundle_jsonl(historical, "replay_outcome_windows.jsonl") if historical else []
    )
    simulation_rows = (
        _bundle_jsonl(simulation, "simulated_outcome_windows.jsonl") if simulation else []
    )
    forward_scanned = len(outcome_tags)
    with_tags = sum(1 for row in outcome_tags if _records_to_texts(row.get("regime_tags")))
    missing_tags = forward_scanned - with_tags
    simulation_pressure = sum(
        1
        for row in simulation_rows
        if row.get("outcome_status") == "AVAILABLE"
        and _text(row.get("regime_label")) in PRESSURE_VALIDATION_TAGS
    )
    historical_available = sum(
        1 for row in historical_rows if row.get("outcome_status") == "AVAILABLE"
    )
    mapping_failures: list[dict[str, Any]] = []
    if (
        _int(pressure_summary.get("pressure_window_count")) > 0
        and _int(pressure_summary.get("pressure_tagged_outcomes")) == 0
    ):
        mapping_failures.append(
            {"reason": "outcome_window_not_mapped_to_regime_window", "count": forward_scanned}
        )
    if missing_tags:
        mapping_failures.append({"reason": "outcomes_missing_regime_tags", "count": missing_tags})
    if simulation_pressure:
        mapping_failures.append(
            {
                "reason": "pressure_tagger_does_not_scan_backtest_simulation",
                "count": simulation_pressure,
            }
        )
    if historical_available:
        mapping_failures.append(
            {
                "reason": "pressure_tagger_does_not_scan_historical_replay_backfill",
                "count": historical_available,
            }
        )
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_outcome_mapping_diagnostics",
        "forward_outcomes_scanned": forward_scanned,
        "historical_replay_outcomes_scanned": len(historical_rows),
        "backtest_simulation_outcomes_scanned": len(simulation_rows),
        "historical_replay_outcomes_available": historical_available,
        "backtest_simulation_outcomes_available": sum(
            row.get("outcome_status") == "AVAILABLE" for row in simulation_rows
        ),
        "backtest_simulation_pressure_outcomes_available": simulation_pressure,
        "outcomes_with_regime_tags": with_tags,
        "outcomes_missing_regime_tags": missing_tags,
        "pressure_relevant_outcomes": _int(
            pressure_summary.get("defensive_validation_relevant_outcomes")
        ),
        "mapping_failures": mapping_failures,
        "production_effect": "none",
        "broker_action_allowed": False,
    }


def _diagnosis_views(
    snapshot: Mapping[str, Any], *, diagnosis_id: str, diagnosis_dir: Path
) -> tuple[dict[str, bytes], dict[str, Any]]:
    pressure = _binding_by_kind(snapshot, "pressure_tag")
    _require(pressure is not None, "diagnosis pressure source missing")
    policy = _records(snapshot.get("policy_bindings"))
    _require(len(policy) == 1, "diagnosis policy binding invalid")
    config = _mapping(policy[0].get("payload"))
    thresholds = _mapping(config.get("thresholds"))
    window_tags = _bundle_jsonl(pressure, "regime_window_tags.jsonl")
    outcome_tags = _bundle_jsonl(pressure, "outcome_regime_tags.jsonl")
    pressure_summary = _bundle_json(pressure, "pressure_regime_summary.json")
    distribution, near_misses = _threshold_distribution_and_near_misses(
        window_tags=window_tags,
        thresholds=thresholds,
        config_path=Path(_text(policy[0].get("path"))),
    )
    mapping = _diagnosis_mapping_from_snapshot(snapshot, pressure_summary, outcome_tags)
    summary = _pressure_tag_diagnosis_summary(
        pressure_summary=pressure_summary,
        distribution=distribution,
        mapping_diagnostics=mapping,
        near_misses=near_misses,
    )
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_pressure_tag_diagnosis_manifest",
        "diagnosis_id": diagnosis_id,
        "tag_id": pressure.get("artifact_id"),
        "generated_at": snapshot.get("generated_at"),
        "status": "PASS",
        "source_pressure_tag_manifest_path": str(
            Path(_text(_mapping(pressure.get("bundle")).get("source_dir")))
            / "pressure_regime_manifest.json"
        ),
        "config_path": policy[0].get("path"),
        "pressure_tag_diagnosis_input_snapshot_path": str(
            diagnosis_dir / "pressure_tag_diagnosis_input_snapshot.json"
        ),
        "pressure_tag_diagnosis_manifest_path": str(
            diagnosis_dir / "pressure_tag_diagnosis_manifest.json"
        ),
        "threshold_hit_distribution_path": str(diagnosis_dir / "threshold_hit_distribution.json"),
        "near_miss_windows_path": str(diagnosis_dir / "near_miss_windows.jsonl"),
        "outcome_mapping_diagnostics_path": str(diagnosis_dir / "outcome_mapping_diagnostics.json"),
        "pressure_tag_diagnosis_report_path": str(
            diagnosis_dir / "pressure_tag_diagnosis_report.md"
        ),
        "diagnosis_summary": summary,
        "market_regime": "ai_after_chatgpt",
        **_artifact_safety(),
    }
    views = {
        "pressure_tag_diagnosis_input_snapshot.json": _json_bytes(snapshot),
        "pressure_tag_diagnosis_manifest.json": _json_bytes(manifest),
        "threshold_hit_distribution.json": _json_bytes(distribution),
        "near_miss_windows.jsonl": _jsonl_bytes(near_misses),
        "outcome_mapping_diagnostics.json": _json_bytes(mapping),
        "pressure_tag_diagnosis_report.md": render_pressure_tag_diagnosis_report(
            manifest, distribution, mapping, summary
        ).encode("utf-8"),
    }
    return views, {
        "manifest": manifest,
        "threshold_hit_distribution": distribution,
        "near_miss_windows": near_misses,
        "outcome_mapping_diagnostics": mapping,
        "diagnosis_summary": summary,
    }


def _backfill_inventory_from_snapshot(snapshot: Mapping[str, Any]) -> list[dict[str, Any]]:
    start = date.fromisoformat(_text(snapshot.get("start")))
    end = date.fromisoformat(_text(snapshot.get("end")))
    pressure = _binding_by_kind(snapshot, "pressure_tag")
    _require(pressure is not None, "backfill pressure source missing")
    pressure_windows = _bundle_jsonl(pressure, "regime_window_tags.jsonl")
    outcome_tags = _bundle_jsonl(pressure, "outcome_regime_tags.jsonl")
    advisory_by_id = {
        _text(row.get("artifact_id")): row
        for row in _records(snapshot.get("source_bindings"))
        if row.get("source_kind") == "advisory_outcome"
    }
    inventory: list[dict[str, Any]] = []
    for tag_row in outcome_tags:
        as_of = _date_from_any(tag_row.get("as_of"))
        tags = _records_to_texts(tag_row.get("regime_tags"))
        if (
            as_of is None
            or not (start <= as_of <= end)
            or not (set(tags) & PRESSURE_VALIDATION_TAGS)
        ):
            continue
        outcome_id = _text(tag_row.get("outcome_id"))
        source = advisory_by_id.get(outcome_id)
        _require(source is not None, f"pressure-tagged advisory outcome missing: {outcome_id}")
        manifest = _bundle_json(source, "advisory_outcome_manifest.json")
        source_rows = _bundle_jsonl(source, "outcome_windows.jsonl")
        matches = [
            row
            for row in source_rows
            if _text(row.get("daily_advisory_id") or manifest.get("daily_advisory_id"))
            == _text(tag_row.get("daily_advisory_id"))
            and _int(row.get("window_days")) == _int(tag_row.get("window_days"))
        ]
        _require(len(matches) == 1, "advisory outcome window missing or ambiguous")
        source_window = matches[0]
        if source_window.get("outcome_status") != "AVAILABLE":
            continue
        inventory.append(
            _pressure_inventory_row(
                source_mode="FORWARD_OUTCOME",
                source_artifact_id=outcome_id,
                source_event_id=_text(tag_row.get("daily_advisory_id")),
                as_of=_text(tag_row.get("as_of")),
                window_days=_int(tag_row.get("window_days")),
                regime_tags=tags,
                variant_results=_forward_variant_results(source_window),
                outcome_status="AVAILABLE",
            )
        )
    pressure_by_end = _pressure_tags_by_end(pressure_windows)
    historical = _binding_by_kind(snapshot, "historical_outcome")
    if historical is not None:
        manifest = _bundle_json(historical, "backfill_manifest.json")
        grouped: dict[tuple[str, str, int], list[Mapping[str, Any]]] = defaultdict(list)
        for row in _bundle_jsonl(historical, "replay_outcome_windows.jsonl"):
            as_of = _date_from_any(row.get("as_of") or row.get("start_date"))
            if (
                row.get("outcome_status") != "AVAILABLE"
                or as_of is None
                or not (start <= as_of <= end)
            ):
                continue
            grouped[
                (
                    _text(row.get("replay_event_id") or row.get("daily_advisory_id")),
                    _text(row.get("as_of") or row.get("start_date")),
                    _int(row.get("window_days")),
                )
            ].append(row)
        for (event_id, as_of_text, window_days), rows in sorted(grouped.items()):
            tags = _records_to_texts(rows[0].get("regime_tags"))
            if not tags:
                tags = pressure_by_end.get((_text(rows[0].get("end_date")), window_days), [])
            if set(tags) & PRESSURE_VALIDATION_TAGS:
                inventory.append(
                    _pressure_inventory_row(
                        source_mode="HISTORICAL_REPLAY",
                        source_artifact_id=_text(manifest.get("backfill_id")),
                        source_event_id=event_id,
                        as_of=as_of_text,
                        window_days=window_days,
                        regime_tags=tags,
                        variant_results=_variant_results_from_rows(rows),
                        outcome_status="AVAILABLE",
                    )
                )
    simulation = _binding_by_kind(snapshot, "simulation_outcome")
    if simulation is not None:
        manifest = _bundle_json(simulation, "sim_outcome_manifest.json")
        grouped_sim: dict[tuple[str, str, int, str], list[Mapping[str, Any]]] = defaultdict(list)
        for row in _bundle_jsonl(simulation, "simulated_outcome_windows.jsonl"):
            as_of = _date_from_any(row.get("as_of") or row.get("start_date"))
            regime = _text(row.get("regime_label"))
            if (
                row.get("outcome_status") != "AVAILABLE"
                or as_of is None
                or not (start <= as_of <= end)
                or regime not in PRESSURE_VALIDATION_TAGS
            ):
                continue
            grouped_sim[
                (
                    _text(row.get("sim_event_id")),
                    _text(row.get("as_of") or row.get("start_date")),
                    _int(row.get("window_days")),
                    regime,
                )
            ].append(row)
        for (event_id, as_of_text, window_days, regime), rows in sorted(grouped_sim.items()):
            inventory.append(
                _pressure_inventory_row(
                    source_mode="BACKTEST_SIMULATION",
                    source_artifact_id=_text(manifest.get("sim_outcome_id")),
                    source_event_id=event_id,
                    as_of=as_of_text,
                    window_days=window_days,
                    regime_tags=[regime],
                    variant_results=_variant_results_from_rows(rows),
                    outcome_status="AVAILABLE",
                )
            )
    identities = [_text(row.get("pressure_outcome_id")) for row in inventory]
    _require(len(identities) == len(set(identities)), "duplicate pressure outcome identity")
    return sorted(inventory, key=lambda row: _text(row.get("pressure_outcome_id")))


def _backfill_views(
    snapshot: Mapping[str, Any], *, backfill_id: str, backfill_dir: Path
) -> tuple[dict[str, bytes], dict[str, Any]]:
    inventory = _backfill_inventory_from_snapshot(snapshot)
    summary = _pressure_source_summary(inventory)
    pressure = _binding_by_kind(snapshot, "pressure_tag")
    _require(pressure is not None, "backfill pressure source missing")
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_pressure_backfill_manifest",
        "pressure_backfill_id": backfill_id,
        "generated_at": snapshot.get("generated_at"),
        "start": snapshot.get("start"),
        "end": snapshot.get("end"),
        "status": "PASS" if inventory else "PASS_WITH_WARNINGS",
        "source_pressure_tag_id": pressure.get("artifact_id"),
        "pressure_outcome_backfill_input_snapshot_path": str(
            backfill_dir / "pressure_outcome_backfill_input_snapshot.json"
        ),
        "pressure_backfill_manifest_path": str(backfill_dir / "pressure_backfill_manifest.json"),
        "pressure_outcome_inventory_path": str(backfill_dir / "pressure_outcome_inventory.jsonl"),
        "pressure_source_summary_path": str(backfill_dir / "pressure_source_summary.json"),
        "pressure_backfill_report_path": str(backfill_dir / "pressure_backfill_report.md"),
        "market_regime": "ai_after_chatgpt",
        "actual_requested_date_range": {
            "start": snapshot.get("start"),
            "end": snapshot.get("end"),
        },
        **_artifact_safety(),
    }
    views = {
        "pressure_outcome_backfill_input_snapshot.json": _json_bytes(snapshot),
        "pressure_backfill_manifest.json": _json_bytes(manifest),
        "pressure_outcome_inventory.jsonl": _jsonl_bytes(inventory),
        "pressure_source_summary.json": _json_bytes(summary),
        "pressure_backfill_report.md": render_pressure_backfill_report(manifest, summary).encode(
            "utf-8"
        ),
    }
    return views, {
        "manifest": manifest,
        "pressure_outcome_inventory": inventory,
        "pressure_source_summary": summary,
    }


def _comparison_policy_values(snapshot: Mapping[str, Any]) -> dict[str, Any]:
    policies = _records(snapshot.get("policy_bindings"))
    by_name = {Path(_text(row.get("path"))).name: row for row in policies}
    defensive = _mapping(
        _mapping(by_name.get(DEFAULT_SIM_DEFENSIVE_VALIDATION_POLICY_PATH.name, {})).get("payload")
    )
    forward = _mapping(
        _mapping(by_name.get(DEFAULT_FORWARD_PRESSURE_CAPTURE_POLICY_PATH.name, {})).get("payload")
    )
    regimes = tuple(_records_to_texts(defensive.get("pressure_regimes")))
    windows = tuple(_int(item) for item in defensive.get("tracked_window_days", []))
    minimum_events = _int(defensive.get("minimum_distinct_events_per_pressure_regime"))
    minimum_return = defensive.get("minimum_relative_return")
    minimum_drawdown = defensive.get("minimum_drawdown_delta_vs_no_trade")
    minimum_win_rate = defensive.get("minimum_win_rate_vs_no_trade")
    forward_floor = _int(
        _mapping(forward.get("validation")).get("required_forward_pressure_samples")
    )
    _require(
        regimes and set(regimes) <= PRESSURE_VALIDATION_TAGS, "defensive policy regimes invalid"
    )
    _require(windows and all(item > 0 for item in windows), "defensive policy windows invalid")
    _require(minimum_events > 0 and forward_floor > 0, "defensive policy sample floor invalid")
    for name, value in (
        ("minimum_relative_return", minimum_return),
        ("minimum_drawdown_delta_vs_no_trade", minimum_drawdown),
        ("minimum_win_rate_vs_no_trade", minimum_win_rate),
    ):
        _require(_finite_number(value), f"defensive policy threshold invalid: {name}")
    _require(0.0 <= float(minimum_win_rate) <= 1.0, "defensive win-rate policy invalid")
    return {
        "pressure_regimes": regimes,
        "tracked_window_days": windows,
        "minimum_distinct_events_per_pressure_regime": minimum_events,
        "required_forward_pressure_samples": forward_floor,
        "minimum_relative_return": float(minimum_return),
        "minimum_drawdown_delta_vs_no_trade": float(minimum_drawdown),
        "minimum_win_rate_vs_no_trade": float(minimum_win_rate),
        "policy_versions": {
            name: _text(
                _mapping(_mapping(row.get("payload")).get("policy_metadata")).get("version")
            )
            for name, row in by_name.items()
        },
    }


def _compare_views(
    snapshot: Mapping[str, Any], *, comparison_id: str, comparison_dir: Path
) -> tuple[dict[str, bytes], dict[str, Any]]:
    backfill = _binding_by_kind(snapshot, "pressure_backfill")
    _require(backfill is not None, "comparison backfill source missing")
    inventory = _bundle_jsonl(backfill, "pressure_outcome_inventory.jsonl")
    policy = _comparison_policy_values(snapshot)
    metrics = _pressure_variant_metrics(inventory, policy=policy)
    pairwise = _defensive_pairwise_comparison(inventory, policy=policy)
    summary = _defensive_pressure_summary(metrics, pairwise, policy=policy)
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_defensive_pressure_compare_manifest",
        "comparison_id": comparison_id,
        "pressure_backfill_id": backfill.get("artifact_id"),
        "generated_at": snapshot.get("generated_at"),
        "status": "PASS",
        "source_pressure_backfill_id": backfill.get("artifact_id"),
        "defensive_pressure_compare_input_snapshot_path": str(
            comparison_dir / "defensive_pressure_compare_input_snapshot.json"
        ),
        "defensive_pressure_compare_manifest_path": str(
            comparison_dir / "defensive_pressure_compare_manifest.json"
        ),
        "pressure_variant_metrics_path": str(comparison_dir / "pressure_variant_metrics.jsonl"),
        "defensive_pairwise_comparison_path": str(
            comparison_dir / "defensive_pairwise_comparison.json"
        ),
        "defensive_pressure_summary_path": str(comparison_dir / "defensive_pressure_summary.json"),
        "defensive_pressure_compare_report_path": str(
            comparison_dir / "defensive_pressure_compare_report.md"
        ),
        "reviewed_policy_versions": policy["policy_versions"],
        "market_regime": "ai_after_chatgpt",
        **_artifact_safety(),
    }
    views = {
        "defensive_pressure_compare_input_snapshot.json": _json_bytes(snapshot),
        "defensive_pressure_compare_manifest.json": _json_bytes(manifest),
        "pressure_variant_metrics.jsonl": _jsonl_bytes(metrics),
        "defensive_pairwise_comparison.json": _json_bytes(pairwise),
        "defensive_pressure_summary.json": _json_bytes(summary),
        "defensive_pressure_compare_report.md": render_defensive_pressure_compare_report(
            manifest, summary, metrics, pairwise
        ).encode("utf-8"),
    }
    return views, {
        "manifest": manifest,
        "pressure_variant_metrics": metrics,
        "defensive_pairwise_comparison": pairwise,
        "defensive_pressure_summary": summary,
    }


def _rule_review_views(
    snapshot: Mapping[str, Any], *, review_id: str, review_dir: Path
) -> tuple[dict[str, bytes], dict[str, Any]]:
    comparison = _binding_by_kind(snapshot, "defensive_compare")
    _require(comparison is not None, "rule review comparison source missing")
    summary = _bundle_json(comparison, "defensive_pressure_summary.json")
    policy = _comparison_policy_values(snapshot)
    matrix = _defensive_rule_decision_matrix(summary, policy=policy)
    checklist = render_defensive_rule_owner_checklist(matrix, summary)
    reader_brief = render_defensive_rule_reader_brief(matrix)
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_defensive_rule_review_manifest",
        "review_id": review_id,
        "comparison_id": comparison.get("artifact_id"),
        "generated_at": snapshot.get("generated_at"),
        "status": "PASS",
        "source_comparison_id": comparison.get("artifact_id"),
        "defensive_rule_review_input_snapshot_path": str(
            review_dir / "defensive_rule_review_input_snapshot.json"
        ),
        "defensive_rule_review_manifest_path": str(
            review_dir / "defensive_rule_review_manifest.json"
        ),
        "defensive_rule_decision_matrix_path": str(
            review_dir / "defensive_rule_decision_matrix.json"
        ),
        "defensive_rule_owner_checklist_path": str(
            review_dir / "defensive_rule_owner_checklist.md"
        ),
        "defensive_rule_review_report_path": str(review_dir / "defensive_rule_review_report.md"),
        "reader_brief_section_path": str(review_dir / "reader_brief_section.md"),
        "reviewed_policy_versions": policy["policy_versions"],
        "rule_approval_allowed": False,
        "auto_apply": False,
        "owner_approval_required": True,
        "market_regime": "ai_after_chatgpt",
        **_artifact_safety(),
    }
    views = {
        "defensive_rule_review_input_snapshot.json": _json_bytes(snapshot),
        "defensive_rule_review_manifest.json": _json_bytes(manifest),
        "defensive_rule_decision_matrix.json": _json_bytes(matrix),
        "defensive_rule_owner_checklist.md": checklist.encode("utf-8"),
        "defensive_rule_review_report.md": render_defensive_rule_review_report(
            manifest, matrix, summary
        ).encode("utf-8"),
        "reader_brief_section.md": reader_brief.encode("utf-8"),
    }
    return views, {
        "manifest": manifest,
        "defensive_rule_decision_matrix": matrix,
        "defensive_rule_owner_checklist": checklist,
        "reader_brief_section": reader_brief,
    }


def _weekly_update_views(
    snapshot: Mapping[str, Any], *, decision_update_id: str, update_dir: Path
) -> tuple[dict[str, bytes], dict[str, Any]]:
    weekly = _binding_by_kind(snapshot, "weekly_cycle")
    backfill = _binding_by_kind(snapshot, "pressure_backfill")
    comparison = _binding_by_kind(snapshot, "defensive_compare")
    review = _binding_by_kind(snapshot, "defensive_review")
    _require(all((weekly, backfill, comparison, review)), "weekly update source missing")
    assert (
        weekly is not None
        and backfill is not None
        and comparison is not None
        and review is not None
    )
    weekly_summary = _bundle_json(weekly, "weekly_cycle_summary.json")
    backfill_manifest = _bundle_json(backfill, "pressure_backfill_manifest.json")
    backfill_summary = _bundle_json(backfill, "pressure_source_summary.json")
    backfill_snapshot = _bundle_json(backfill, "pressure_outcome_backfill_input_snapshot.json")
    comparison_manifest = _bundle_json(comparison, "defensive_pressure_compare_manifest.json")
    review_manifest = _bundle_json(review, "defensive_rule_review_manifest.json")
    review_matrix = _bundle_json(review, "defensive_rule_decision_matrix.json")
    _require(
        comparison_manifest.get("pressure_backfill_id")
        == backfill.get("artifact_id")
        == backfill_manifest.get("pressure_backfill_id"),
        "weekly update Backfill→Compare lineage mismatch",
    )
    _require(
        review_manifest.get("comparison_id") == comparison.get("artifact_id"),
        "weekly update Compare→Review lineage mismatch",
    )
    backfill_time = _pressure_datetime(backfill.get("generated_at"), field="backfill generated_at")
    comparison_time = _pressure_datetime(
        comparison.get("generated_at"), field="comparison generated_at"
    )
    review_time = _pressure_datetime(review.get("generated_at"), field="review generated_at")
    _require(
        backfill_time <= comparison_time <= review_time, "weekly update source chronology invalid"
    )
    pressure_source = _binding_by_kind(backfill_snapshot, "pressure_tag")
    _require(pressure_source is not None, "backfill frozen pressure source missing")
    frozen_pressure_summary = _bundle_json(pressure_source, "pressure_regime_summary.json")
    before_count = _int(frozen_pressure_summary.get("defensive_validation_relevant_outcomes"))
    after_count = _int(backfill_summary.get("defensive_validation_relevant_count"))
    policy = _comparison_policy_values(snapshot)
    matrix = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_updated_weekly_decision_matrix",
        "weekly_cycle_id": weekly.get("artifact_id"),
        "pressure_backfill_id": backfill.get("artifact_id"),
        "comparison_id": comparison.get("artifact_id"),
        "defensive_review_id": review.get("artifact_id"),
        "defensive_validation_relevant_outcomes_before": before_count,
        "defensive_validation_relevant_outcomes_after": after_count,
        "defensive_rule_status": _text(review_matrix.get("recommended_status"), "RESEARCH_ONLY"),
        "rule_approval_allowed": False,
        "weekly_recommendation": "continue_tracking",
        "owner_action_required": _text(review_matrix.get("recommended_status"))
        in {"OWNER_REVIEW_REQUIRED", "RENAME_RECOMMENDED", "DISABLE_RECOMMENDED"},
        "reviewed_policy_versions": policy["policy_versions"],
        "policy_change_allowed": False,
        "broker_action_allowed": False,
        "production_effect": "none",
    }
    actions = _weekly_ops_next_actions(matrix, backfill_summary, review_matrix, policy=policy)
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_weekly_ops_decision_update_manifest",
        "decision_update_id": decision_update_id,
        "weekly_cycle_id": weekly.get("artifact_id"),
        "pressure_backfill_id": backfill.get("artifact_id"),
        "comparison_id": comparison.get("artifact_id"),
        "defensive_review_id": review.get("artifact_id"),
        "generated_at": snapshot.get("generated_at"),
        "status": "PASS",
        "weekly_ops_decision_update_input_snapshot_path": str(
            update_dir / "weekly_ops_decision_update_input_snapshot.json"
        ),
        "weekly_ops_decision_update_manifest_path": str(
            update_dir / "weekly_ops_decision_update_manifest.json"
        ),
        "updated_weekly_decision_matrix_path": str(
            update_dir / "updated_weekly_decision_matrix.json"
        ),
        "weekly_ops_next_actions_path": str(update_dir / "weekly_ops_next_actions.json"),
        "weekly_ops_decision_update_report_path": str(
            update_dir / "weekly_ops_decision_update_report.md"
        ),
        "reader_brief_section_path": str(update_dir / "reader_brief_section.md"),
        "source_weekly_summary": weekly_summary,
        "reviewed_policy_versions": policy["policy_versions"],
        "market_regime": "ai_after_chatgpt",
        **_artifact_safety(),
    }
    reader_brief = render_weekly_ops_decision_reader_brief(matrix, actions)
    views = {
        "weekly_ops_decision_update_input_snapshot.json": _json_bytes(snapshot),
        "weekly_ops_decision_update_manifest.json": _json_bytes(manifest),
        "updated_weekly_decision_matrix.json": _json_bytes(matrix),
        "weekly_ops_next_actions.json": _json_bytes(actions),
        "weekly_ops_decision_update_report.md": render_weekly_ops_decision_update_report(
            manifest, matrix, actions
        ).encode("utf-8"),
        "reader_brief_section.md": reader_brief.encode("utf-8"),
    }
    return views, {
        "manifest": manifest,
        "updated_weekly_decision_matrix": matrix,
        "weekly_ops_next_actions": actions,
        "reader_brief_section": reader_brief,
    }


def run_pressure_tag_diagnosis(
    *,
    tag_id: str,
    output_dir: Path = DEFAULT_PRESSURE_TAG_DIAGNOSIS_DIR,
    pressure_tag_dir: Path = DEFAULT_PRESSURE_REGIME_TAG_DIR,
    backfilled_outcome_dir: Path = DEFAULT_BACKFILLED_OUTCOME_DIR,
    backtest_sim_outcome_dir: Path = DEFAULT_BACKTEST_SIM_OUTCOME_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = _pressure_generated_at(generated_at)
    pressure_source = _source_binding(
        source_kind="pressure_tag",
        source_dir=pressure_tag_dir / tag_id,
        generated=generated,
        json_views=("pressure_regime_manifest.json", "pressure_regime_summary.json"),
        jsonl_views=("regime_window_tags.jsonl", "outcome_regime_tags.jsonl"),
    )
    pressure_manifest = _bundle_json(pressure_source, "pressure_regime_manifest.json")
    config_path = _resolve_project_path(Path(_text(pressure_manifest.get("config_path"))))
    policy = _policy_snapshot(config_path)
    historical = _semantic_optional_binding(
        source_kind="historical_outcome",
        output_dir=backfilled_outcome_dir,
        manifest_name="backfill_manifest.json",
        id_key="backfill_id",
        generated=generated,
        canonical_files=("backfill_manifest.json", "replay_outcome_windows.jsonl"),
        json_views=("backfill_manifest.json",),
        jsonl_views=("replay_outcome_windows.jsonl",),
    )
    simulation = _semantic_optional_binding(
        source_kind="simulation_outcome",
        output_dir=backtest_sim_outcome_dir,
        manifest_name="sim_outcome_manifest.json",
        id_key="sim_outcome_id",
        generated=generated,
        canonical_files=("sim_outcome_manifest.json", "simulated_outcome_windows.jsonl"),
        json_views=("sim_outcome_manifest.json",),
        jsonl_views=("simulated_outcome_windows.jsonl",),
    )
    diagnosis_id = _stable_id("pressure-tag-diagnosis", tag_id, generated.isoformat())
    diagnosis_dir = _unique_dir(output_dir / diagnosis_id)
    snapshot = {
        "schema_version": PRESSURE_TAG_DIAGNOSIS_SNAPSHOT_SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_pressure_tag_diagnosis_input_snapshot",
        "diagnosis_id": diagnosis_dir.name,
        "generated_at": generated.isoformat(),
        "source_bindings": [
            pressure_source,
            *([historical] if historical is not None else []),
            *([simulation] if simulation is not None else []),
        ],
        "policy_bindings": [policy],
        "semantic_selection": {
            "pressure_tag_id": tag_id,
            "historical_backfill_id": historical.get("artifact_id") if historical else None,
            "simulation_outcome_id": simulation.get("artifact_id") if simulation else None,
            "selection_rule": "generated_at_then_artifact_id_at_or_before_cutoff",
        },
        "production_effect": "none",
        "broker_action_allowed": False,
    }
    views, payload = _diagnosis_views(
        snapshot, diagnosis_id=diagnosis_dir.name, diagnosis_dir=diagnosis_dir
    )
    _write_views_atomic(diagnosis_dir, views)
    _update_latest_pointer(
        "latest_pressure_tag_diagnosis",
        diagnosis_dir.name,
        diagnosis_dir / "pressure_tag_diagnosis_manifest.json",
    )
    return {
        "diagnosis_id": diagnosis_dir.name,
        "diagnosis_dir": diagnosis_dir,
        "input_snapshot": snapshot,
        **payload,
    }


def pressure_tag_diagnosis_report_payload(
    *,
    diagnosis_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_PRESSURE_TAG_DIAGNOSIS_DIR,
) -> dict[str, Any]:
    diagnosis_dir = _artifact_dir_from_latest(
        output_dir=output_dir,
        artifact_id=diagnosis_id if not latest else None,
        pointer_name="latest_pressure_tag_diagnosis",
    )
    return {
        **_read_json(diagnosis_dir / "pressure_tag_diagnosis_manifest.json"),
        **_report_input_snapshot(diagnosis_dir / "pressure_tag_diagnosis_input_snapshot.json"),
        "threshold_hit_distribution": _read_json(diagnosis_dir / "threshold_hit_distribution.json"),
        "near_miss_windows": _read_jsonl(diagnosis_dir / "near_miss_windows.jsonl"),
        "outcome_mapping_diagnostics": _read_json(
            diagnosis_dir / "outcome_mapping_diagnostics.json"
        ),
        "diagnosis_dir": str(diagnosis_dir),
    }


def validate_pressure_tag_diagnosis_artifact(
    *, diagnosis_id: str, output_dir: Path = DEFAULT_PRESSURE_TAG_DIAGNOSIS_DIR
) -> dict[str, Any]:
    diagnosis_dir = output_dir / diagnosis_id
    manifest = _read_optional_json(diagnosis_dir / "pressure_tag_diagnosis_manifest.json") or {}
    snapshot = (
        _read_optional_json(diagnosis_dir / "pressure_tag_diagnosis_input_snapshot.json") or {}
    )
    checks = [
        _check(
            "snapshot_exists",
            (diagnosis_dir / "pressure_tag_diagnosis_input_snapshot.json").is_file(),
            "versioned input snapshot required",
        ),
        _check("diagnosis_id_matches", manifest.get("diagnosis_id") == diagnosis_id, ""),
        _check(
            "safety_no_auto_change",
            manifest.get("production_effect") == "none"
            and manifest.get("policy_change_allowed") is False
            and manifest.get("broker_action_allowed") is False,
            "diagnosis does not mutate policy",
        ),
    ]
    if snapshot:
        try:
            generated = _pressure_datetime(
                snapshot.get("generated_at"), field="diagnosis snapshot generated_at"
            )
            snapshot_checks, _ = _snapshot_validation_checks(
                snapshot=snapshot,
                expected_schema=PRESSURE_TAG_DIAGNOSIS_SNAPSHOT_SCHEMA_VERSION,
                generated=generated,
            )
            checks.extend(snapshot_checks)
            expected, _ = _diagnosis_views(
                snapshot, diagnosis_id=diagnosis_id, diagnosis_dir=diagnosis_dir
            )
            mismatches = _views_match(diagnosis_dir, expected)
            checks.append(
                _check(
                    "content_derived_views",
                    not mismatches,
                    f"mismatched files: {mismatches}",
                )
            )
        except Exception as exc:  # noqa: BLE001
            checks.append(_check("content_derived_views", False, str(exc)))
    else:
        checks.append(_check("content_derived_views", False, "snapshot missing"))
    return _validation_payload(
        report_type="etf_dynamic_v3_pressure_tag_diagnosis_validation",
        artifact_id_key="diagnosis_id",
        artifact_id=diagnosis_id,
        checks=checks,
    )


def run_pressure_outcome_backfill(
    *,
    start: date,
    end: date,
    output_dir: Path = DEFAULT_PRESSURE_OUTCOME_BACKFILL_DIR,
    pressure_tag_dir: Path = DEFAULT_PRESSURE_REGIME_TAG_DIR,
    advisory_outcome_dir: Path = DEFAULT_ADVISORY_OUTCOME_DIR,
    backfilled_outcome_dir: Path = DEFAULT_BACKFILLED_OUTCOME_DIR,
    backtest_sim_outcome_dir: Path = DEFAULT_BACKTEST_SIM_OUTCOME_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    _require(start <= end, "pressure backfill start must be on or before end")
    generated = _pressure_generated_at(generated_at)
    try:
        tag_id = _semantic_artifact_id(
            output_dir=pressure_tag_dir,
            artifact_id=None,
            manifest_name="pressure_regime_manifest.json",
            id_key="tag_id",
            generated=generated,
            source_name="pressure regime tag",
            required=True,
        )
    except Exception as exc:  # noqa: BLE001
        raise DynamicV3PressureValidationError(str(exc)) from exc
    pressure = _source_binding(
        source_kind="pressure_tag",
        source_dir=pressure_tag_dir / tag_id,
        generated=generated,
        json_views=("pressure_regime_manifest.json", "pressure_regime_summary.json"),
        jsonl_views=("regime_window_tags.jsonl", "outcome_regime_tags.jsonl"),
    )
    relevant_outcome_ids = sorted(
        {
            _text(row.get("outcome_id"))
            for row in _bundle_jsonl(pressure, "outcome_regime_tags.jsonl")
            if set(_records_to_texts(row.get("regime_tags"))) & PRESSURE_VALIDATION_TAGS
        }
    )
    advisory_bindings = [
        _source_binding(
            source_kind="advisory_outcome",
            source_dir=advisory_outcome_dir / outcome_id,
            generated=generated,
            json_views=("advisory_outcome_manifest.json",),
            jsonl_views=("outcome_windows.jsonl",),
        )
        for outcome_id in relevant_outcome_ids
    ]
    historical = _semantic_optional_binding(
        source_kind="historical_outcome",
        output_dir=backfilled_outcome_dir,
        manifest_name="backfill_manifest.json",
        id_key="backfill_id",
        generated=generated,
        canonical_files=("backfill_manifest.json", "replay_outcome_windows.jsonl"),
        json_views=("backfill_manifest.json",),
        jsonl_views=("replay_outcome_windows.jsonl",),
    )
    simulation = _semantic_optional_binding(
        source_kind="simulation_outcome",
        output_dir=backtest_sim_outcome_dir,
        manifest_name="sim_outcome_manifest.json",
        id_key="sim_outcome_id",
        generated=generated,
        canonical_files=("sim_outcome_manifest.json", "simulated_outcome_windows.jsonl"),
        json_views=("sim_outcome_manifest.json",),
        jsonl_views=("simulated_outcome_windows.jsonl",),
    )
    pressure_backfill_id = _stable_id(
        "pressure-outcome-backfill",
        start.isoformat(),
        end.isoformat(),
        generated.isoformat(),
    )
    artifact_dir = _unique_dir(output_dir / pressure_backfill_id)
    snapshot = {
        "schema_version": PRESSURE_OUTCOME_BACKFILL_SNAPSHOT_SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_pressure_outcome_backfill_input_snapshot",
        "pressure_backfill_id": artifact_dir.name,
        "generated_at": generated.isoformat(),
        "start": start.isoformat(),
        "end": end.isoformat(),
        "source_bindings": [
            pressure,
            *advisory_bindings,
            *([historical] if historical is not None else []),
            *([simulation] if simulation is not None else []),
        ],
        "policy_bindings": [],
        "semantic_selection": {
            "pressure_tag_id": tag_id,
            "advisory_outcome_ids": relevant_outcome_ids,
            "historical_backfill_id": historical.get("artifact_id") if historical else None,
            "simulation_outcome_id": simulation.get("artifact_id") if simulation else None,
            "selection_rule": "generated_at_then_artifact_id_at_or_before_cutoff",
        },
        "evidence_boundary": {
            "available_only": True,
            "simulation_not_pit": True,
            "historical_replay_has_pit_warning": True,
            "production_support_source_modes": ["FORWARD_OUTCOME"],
        },
        "production_effect": "none",
        "broker_action_allowed": False,
    }
    views, payload = _backfill_views(
        snapshot, backfill_id=artifact_dir.name, backfill_dir=artifact_dir
    )
    _write_views_atomic(artifact_dir, views)
    _update_latest_pointer(
        "latest_pressure_outcome_backfill",
        artifact_dir.name,
        artifact_dir / "pressure_backfill_manifest.json",
    )
    return {
        "pressure_backfill_id": artifact_dir.name,
        "pressure_backfill_dir": artifact_dir,
        "input_snapshot": snapshot,
        **payload,
    }


def pressure_outcome_backfill_report_payload(
    *,
    backfill_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_PRESSURE_OUTCOME_BACKFILL_DIR,
) -> dict[str, Any]:
    backfill_dir = _artifact_dir_from_latest(
        output_dir=output_dir,
        artifact_id=backfill_id if not latest else None,
        pointer_name="latest_pressure_outcome_backfill",
    )
    return {
        **_read_json(backfill_dir / "pressure_backfill_manifest.json"),
        **_report_input_snapshot(backfill_dir / "pressure_outcome_backfill_input_snapshot.json"),
        "pressure_outcome_inventory": _read_jsonl(
            backfill_dir / "pressure_outcome_inventory.jsonl"
        ),
        "pressure_source_summary": _read_json(backfill_dir / "pressure_source_summary.json"),
        "pressure_backfill_dir": str(backfill_dir),
    }


def validate_pressure_outcome_backfill_artifact(
    *, backfill_id: str, output_dir: Path = DEFAULT_PRESSURE_OUTCOME_BACKFILL_DIR
) -> dict[str, Any]:
    artifact_dir = output_dir / backfill_id
    manifest = _read_optional_json(artifact_dir / "pressure_backfill_manifest.json") or {}
    snapshot = (
        _read_optional_json(artifact_dir / "pressure_outcome_backfill_input_snapshot.json") or {}
    )
    checks = [
        _check(
            "snapshot_exists",
            (artifact_dir / "pressure_outcome_backfill_input_snapshot.json").is_file(),
            "versioned input snapshot required",
        ),
        _check(
            "backfill_id_matches",
            manifest.get("pressure_backfill_id") == backfill_id,
            "",
        ),
        _check(
            "safety_no_approval",
            manifest.get("production_effect") == "none"
            and manifest.get("policy_change_allowed") is False
            and manifest.get("broker_action_allowed") is False,
            "no approval from backfill",
        ),
    ]
    if snapshot:
        try:
            generated = _pressure_datetime(
                snapshot.get("generated_at"), field="backfill snapshot generated_at"
            )
            snapshot_checks, _ = _snapshot_validation_checks(
                snapshot=snapshot,
                expected_schema=PRESSURE_OUTCOME_BACKFILL_SNAPSHOT_SCHEMA_VERSION,
                generated=generated,
            )
            checks.extend(snapshot_checks)
            expected, payload = _backfill_views(
                snapshot, backfill_id=backfill_id, backfill_dir=artifact_dir
            )
            inventory = payload["pressure_outcome_inventory"]
            checks.extend(
                [
                    _check(
                        "available_only",
                        all(row.get("outcome_status") == "AVAILABLE" for row in inventory),
                        "PENDING/INSUFFICIENT rows excluded",
                    ),
                    _check(
                        "simulation_research_only",
                        all(
                            row.get("evidence_quality") == "SIMULATION_NOT_PIT"
                            and row.get("can_support_production") is False
                            for row in inventory
                            if row.get("source_mode") == "BACKTEST_SIMULATION"
                        ),
                        "simulation evidence is research-only",
                    ),
                ]
            )
            mismatches = _views_match(artifact_dir, expected)
            checks.append(
                _check(
                    "content_derived_views",
                    not mismatches,
                    f"mismatched files: {mismatches}",
                )
            )
        except Exception as exc:  # noqa: BLE001
            checks.append(_check("content_derived_views", False, str(exc)))
    else:
        checks.append(_check("content_derived_views", False, "snapshot missing"))
    return _validation_payload(
        report_type="etf_dynamic_v3_pressure_outcome_backfill_validation",
        artifact_id_key="pressure_backfill_id",
        artifact_id=backfill_id,
        checks=checks,
    )


def run_defensive_pressure_compare(
    *,
    pressure_backfill_id: str,
    backfill_dir: Path = DEFAULT_PRESSURE_OUTCOME_BACKFILL_DIR,
    output_dir: Path = DEFAULT_DEFENSIVE_PRESSURE_COMPARE_DIR,
    sim_policy_path: Path = DEFAULT_SIM_DEFENSIVE_VALIDATION_POLICY_PATH,
    forward_policy_path: Path = DEFAULT_FORWARD_PRESSURE_CAPTURE_POLICY_PATH,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = _pressure_generated_at(generated_at)
    backfill = _source_binding(
        source_kind="pressure_backfill",
        source_dir=backfill_dir / pressure_backfill_id,
        generated=generated,
        canonical_files=(
            "pressure_outcome_backfill_input_snapshot.json",
            "pressure_backfill_manifest.json",
            "pressure_outcome_inventory.jsonl",
            "pressure_source_summary.json",
            "pressure_backfill_report.md",
        ),
        json_views=(
            "pressure_outcome_backfill_input_snapshot.json",
            "pressure_backfill_manifest.json",
            "pressure_source_summary.json",
        ),
        jsonl_views=("pressure_outcome_inventory.jsonl",),
        text_views=("pressure_backfill_report.md",),
    )
    comparison_id = _stable_id(
        "defensive-pressure-compare",
        pressure_backfill_id,
        generated.isoformat(),
    )
    artifact_dir = _unique_dir(output_dir / comparison_id)
    snapshot = {
        "schema_version": DEFENSIVE_PRESSURE_COMPARE_SNAPSHOT_SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_defensive_pressure_compare_input_snapshot",
        "comparison_id": artifact_dir.name,
        "generated_at": generated.isoformat(),
        "source_bindings": [backfill],
        "policy_bindings": [
            _policy_snapshot(sim_policy_path),
            _policy_snapshot(forward_policy_path),
        ],
        "lineage": {"pressure_backfill_id": pressure_backfill_id},
        "evidence_boundary": {
            "paired_same_event_window_required": True,
            "available_finite_only": True,
            "missing_metrics_are_null": True,
            "simulation_and_historical_research_only": True,
        },
        "production_effect": "none",
        "broker_action_allowed": False,
    }
    views, payload = _compare_views(
        snapshot, comparison_id=artifact_dir.name, comparison_dir=artifact_dir
    )
    _write_views_atomic(artifact_dir, views)
    _update_latest_pointer(
        "latest_defensive_pressure_compare",
        artifact_dir.name,
        artifact_dir / "defensive_pressure_compare_manifest.json",
    )
    return {
        "comparison_id": artifact_dir.name,
        "comparison_dir": artifact_dir,
        "input_snapshot": snapshot,
        **payload,
    }


def defensive_pressure_compare_report_payload(
    *,
    comparison_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_DEFENSIVE_PRESSURE_COMPARE_DIR,
) -> dict[str, Any]:
    comparison_dir = _artifact_dir_from_latest(
        output_dir=output_dir,
        artifact_id=comparison_id if not latest else None,
        pointer_name="latest_defensive_pressure_compare",
    )
    return {
        **_read_json(comparison_dir / "defensive_pressure_compare_manifest.json"),
        **_report_input_snapshot(comparison_dir / "defensive_pressure_compare_input_snapshot.json"),
        "pressure_variant_metrics": _read_jsonl(comparison_dir / "pressure_variant_metrics.jsonl"),
        "defensive_pairwise_comparison": _read_json(
            comparison_dir / "defensive_pairwise_comparison.json"
        ),
        "defensive_pressure_summary": _read_json(
            comparison_dir / "defensive_pressure_summary.json"
        ),
        "comparison_dir": str(comparison_dir),
    }


def validate_defensive_pressure_compare_artifact(
    *, comparison_id: str, output_dir: Path = DEFAULT_DEFENSIVE_PRESSURE_COMPARE_DIR
) -> dict[str, Any]:
    artifact_dir = output_dir / comparison_id
    manifest = _read_optional_json(artifact_dir / "defensive_pressure_compare_manifest.json") or {}
    snapshot = (
        _read_optional_json(artifact_dir / "defensive_pressure_compare_input_snapshot.json") or {}
    )
    checks = [
        _check(
            "snapshot_exists",
            (artifact_dir / "defensive_pressure_compare_input_snapshot.json").is_file(),
            "versioned input snapshot required",
        ),
        _check("comparison_id_matches", manifest.get("comparison_id") == comparison_id, ""),
        _check(
            "safety_no_broker",
            manifest.get("broker_action_allowed") is False
            and manifest.get("production_effect") == "none"
            and manifest.get("policy_change_allowed") is False,
            "no broker/no production",
        ),
    ]
    if snapshot:
        try:
            generated = _pressure_datetime(
                snapshot.get("generated_at"), field="comparison snapshot generated_at"
            )
            snapshot_checks, _ = _snapshot_validation_checks(
                snapshot=snapshot,
                expected_schema=DEFENSIVE_PRESSURE_COMPARE_SNAPSHOT_SCHEMA_VERSION,
                generated=generated,
            )
            checks.extend(snapshot_checks)
            expected, payload = _compare_views(
                snapshot, comparison_id=comparison_id, comparison_dir=artifact_dir
            )
            metrics = payload["pressure_variant_metrics"]
            summary = payload["defensive_pressure_summary"]
            checks.extend(
                [
                    _check(
                        "missing_metrics_are_null",
                        all(
                            row.get("sample_count", 0) > 0
                            or (
                                row.get("avg_return") is None
                                and row.get("avg_relative_to_no_trade") is None
                                and row.get("win_rate_vs_no_trade") is None
                                and row.get("avg_max_drawdown") is None
                                and row.get("drawdown_delta_vs_no_trade") is None
                            )
                            for row in metrics
                        ),
                        "empty cohorts retain null metrics",
                    ),
                    _check(
                        "rule_approval_guarded",
                        summary.get("can_support_rule_approval") is False
                        or _mapping(summary.get("source_mode_breakdown")).get("FORWARD_OUTCOME")
                        == "PROVEN_DEFENSIVE",
                        "support requires sufficient forward evidence",
                    ),
                ]
            )
            mismatches = _views_match(artifact_dir, expected)
            checks.append(
                _check(
                    "content_derived_views",
                    not mismatches,
                    f"mismatched files: {mismatches}",
                )
            )
        except Exception as exc:  # noqa: BLE001
            checks.append(_check("content_derived_views", False, str(exc)))
    else:
        checks.append(_check("content_derived_views", False, "snapshot missing"))
    return _validation_payload(
        report_type="etf_dynamic_v3_defensive_pressure_compare_validation",
        artifact_id_key="comparison_id",
        artifact_id=comparison_id,
        checks=checks,
    )


def run_defensive_rule_review(
    *,
    comparison_id: str,
    comparison_dir: Path = DEFAULT_DEFENSIVE_PRESSURE_COMPARE_DIR,
    output_dir: Path = DEFAULT_DEFENSIVE_RULE_REVIEW_DIR,
    sim_policy_path: Path = DEFAULT_SIM_DEFENSIVE_VALIDATION_POLICY_PATH,
    forward_policy_path: Path = DEFAULT_FORWARD_PRESSURE_CAPTURE_POLICY_PATH,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = _pressure_generated_at(generated_at)
    comparison = _source_binding(
        source_kind="defensive_compare",
        source_dir=comparison_dir / comparison_id,
        generated=generated,
        canonical_files=(
            "defensive_pressure_compare_input_snapshot.json",
            "defensive_pressure_compare_manifest.json",
            "pressure_variant_metrics.jsonl",
            "defensive_pairwise_comparison.json",
            "defensive_pressure_summary.json",
            "defensive_pressure_compare_report.md",
        ),
        json_views=(
            "defensive_pressure_compare_input_snapshot.json",
            "defensive_pressure_compare_manifest.json",
            "defensive_pairwise_comparison.json",
            "defensive_pressure_summary.json",
        ),
        jsonl_views=("pressure_variant_metrics.jsonl",),
        text_views=("defensive_pressure_compare_report.md",),
    )
    review_id = _stable_id("defensive-rule-review", comparison_id, generated.isoformat())
    artifact_dir = _unique_dir(output_dir / review_id)
    snapshot = {
        "schema_version": DEFENSIVE_RULE_REVIEW_SNAPSHOT_SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_defensive_rule_review_input_snapshot",
        "review_id": artifact_dir.name,
        "generated_at": generated.isoformat(),
        "source_bindings": [comparison],
        "policy_bindings": [
            _policy_snapshot(sim_policy_path),
            _policy_snapshot(forward_policy_path),
        ],
        "lineage": {"comparison_id": comparison_id},
        "decision_boundary": {
            "rule_approval_allowed": False,
            "auto_apply": False,
            "owner_review_only": True,
        },
        "production_effect": "none",
        "broker_action_allowed": False,
    }
    views, payload = _rule_review_views(
        snapshot, review_id=artifact_dir.name, review_dir=artifact_dir
    )
    _write_views_atomic(artifact_dir, views)
    _update_latest_pointer(
        "latest_defensive_rule_review",
        artifact_dir.name,
        artifact_dir / "defensive_rule_review_manifest.json",
    )
    return {
        "review_id": artifact_dir.name,
        "review_dir": artifact_dir,
        "input_snapshot": snapshot,
        **payload,
    }


def defensive_rule_review_report_payload(
    *,
    review_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_DEFENSIVE_RULE_REVIEW_DIR,
) -> dict[str, Any]:
    review_dir = _artifact_dir_from_latest(
        output_dir=output_dir,
        artifact_id=review_id if not latest else None,
        pointer_name="latest_defensive_rule_review",
    )
    return {
        **_read_json(review_dir / "defensive_rule_review_manifest.json"),
        **_report_input_snapshot(review_dir / "defensive_rule_review_input_snapshot.json"),
        "defensive_rule_decision_matrix": _read_json(
            review_dir / "defensive_rule_decision_matrix.json"
        ),
        "defensive_rule_owner_checklist": _read_text(
            review_dir / "defensive_rule_owner_checklist.md"
        ),
        "reader_brief_section": _read_text(review_dir / "reader_brief_section.md"),
        "review_dir": str(review_dir),
    }


def validate_defensive_rule_review_artifact(
    *, review_id: str, output_dir: Path = DEFAULT_DEFENSIVE_RULE_REVIEW_DIR
) -> dict[str, Any]:
    artifact_dir = output_dir / review_id
    manifest = _read_optional_json(artifact_dir / "defensive_rule_review_manifest.json") or {}
    snapshot = _read_optional_json(artifact_dir / "defensive_rule_review_input_snapshot.json") or {}
    checks = [
        _check(
            "snapshot_exists",
            (artifact_dir / "defensive_rule_review_input_snapshot.json").is_file(),
            "versioned input snapshot required",
        ),
        _check("review_id_matches", manifest.get("review_id") == review_id, ""),
        _check(
            "safety_no_policy_change",
            manifest.get("policy_change_allowed") is False
            and manifest.get("rule_approval_allowed") is False
            and manifest.get("auto_apply") is False
            and manifest.get("production_effect") == "none",
            "manual owner review only",
        ),
    ]
    if snapshot:
        try:
            generated = _pressure_datetime(
                snapshot.get("generated_at"), field="rule review snapshot generated_at"
            )
            snapshot_checks, _ = _snapshot_validation_checks(
                snapshot=snapshot,
                expected_schema=DEFENSIVE_RULE_REVIEW_SNAPSHOT_SCHEMA_VERSION,
                generated=generated,
            )
            checks.extend(snapshot_checks)
            expected, payload = _rule_review_views(
                snapshot, review_id=review_id, review_dir=artifact_dir
            )
            matrix = payload["defensive_rule_decision_matrix"]
            checks.extend(
                [
                    _check("rule_approval_false", matrix.get("rule_approval_allowed") is False, ""),
                    _check("auto_apply_false", matrix.get("auto_apply") is False, ""),
                ]
            )
            mismatches = _views_match(artifact_dir, expected)
            checks.append(
                _check(
                    "content_derived_views",
                    not mismatches,
                    f"mismatched files: {mismatches}",
                )
            )
        except Exception as exc:  # noqa: BLE001
            checks.append(_check("content_derived_views", False, str(exc)))
    else:
        checks.append(_check("content_derived_views", False, "snapshot missing"))
    return _validation_payload(
        report_type="etf_dynamic_v3_defensive_rule_review_validation",
        artifact_id_key="review_id",
        artifact_id=review_id,
        checks=checks,
    )


def run_weekly_ops_decision_update(
    *,
    weekly_cycle_id: str,
    pressure_backfill_id: str,
    defensive_review_id: str,
    weekly_cycle_dir: Path = DEFAULT_CONFIRMATION_CYCLE_WEEKLY_DIR,
    backfill_dir: Path = DEFAULT_PRESSURE_OUTCOME_BACKFILL_DIR,
    defensive_review_dir: Path = DEFAULT_DEFENSIVE_RULE_REVIEW_DIR,
    pressure_tag_dir: Path = DEFAULT_PRESSURE_REGIME_TAG_DIR,
    output_dir: Path = DEFAULT_WEEKLY_OPS_DECISION_UPDATE_DIR,
    sim_policy_path: Path = DEFAULT_SIM_DEFENSIVE_VALIDATION_POLICY_PATH,
    forward_policy_path: Path = DEFAULT_FORWARD_PRESSURE_CAPTURE_POLICY_PATH,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    del pressure_tag_dir
    generated = _pressure_generated_at(generated_at)
    weekly = _source_binding(
        source_kind="weekly_cycle",
        source_dir=weekly_cycle_dir / weekly_cycle_id,
        generated=generated,
        json_views=("weekly_cycle_manifest.json", "weekly_cycle_summary.json"),
    )
    backfill = _source_binding(
        source_kind="pressure_backfill",
        source_dir=backfill_dir / pressure_backfill_id,
        generated=generated,
        canonical_files=(
            "pressure_outcome_backfill_input_snapshot.json",
            "pressure_backfill_manifest.json",
            "pressure_outcome_inventory.jsonl",
            "pressure_source_summary.json",
            "pressure_backfill_report.md",
        ),
        json_views=(
            "pressure_outcome_backfill_input_snapshot.json",
            "pressure_backfill_manifest.json",
            "pressure_source_summary.json",
        ),
        jsonl_views=("pressure_outcome_inventory.jsonl",),
        text_views=("pressure_backfill_report.md",),
    )
    review = _source_binding(
        source_kind="defensive_review",
        source_dir=defensive_review_dir / defensive_review_id,
        generated=generated,
        canonical_files=(
            "defensive_rule_review_input_snapshot.json",
            "defensive_rule_review_manifest.json",
            "defensive_rule_decision_matrix.json",
            "defensive_rule_owner_checklist.md",
            "defensive_rule_review_report.md",
            "reader_brief_section.md",
        ),
        json_views=(
            "defensive_rule_review_input_snapshot.json",
            "defensive_rule_review_manifest.json",
            "defensive_rule_decision_matrix.json",
        ),
        text_views=(
            "defensive_rule_owner_checklist.md",
            "defensive_rule_review_report.md",
            "reader_brief_section.md",
        ),
    )
    review_snapshot = _bundle_json(review, "defensive_rule_review_input_snapshot.json")
    frozen_comparison = _binding_by_kind(review_snapshot, "defensive_compare")
    _require(frozen_comparison is not None, "review comparison binding missing")
    comparison_source_dir = Path(_text(_mapping(frozen_comparison.get("bundle")).get("source_dir")))
    comparison = _source_binding(
        source_kind="defensive_compare",
        source_dir=comparison_source_dir,
        generated=generated,
        canonical_files=(
            "defensive_pressure_compare_input_snapshot.json",
            "defensive_pressure_compare_manifest.json",
            "pressure_variant_metrics.jsonl",
            "defensive_pairwise_comparison.json",
            "defensive_pressure_summary.json",
            "defensive_pressure_compare_report.md",
        ),
        json_views=(
            "defensive_pressure_compare_input_snapshot.json",
            "defensive_pressure_compare_manifest.json",
            "defensive_pairwise_comparison.json",
            "defensive_pressure_summary.json",
        ),
        jsonl_views=("pressure_variant_metrics.jsonl",),
        text_views=("defensive_pressure_compare_report.md",),
    )
    _require(
        comparison.get("artifact_id") == frozen_comparison.get("artifact_id"),
        "review live comparison identity drift",
    )
    decision_update_id = _stable_id(
        "weekly-ops-decision-update",
        weekly_cycle_id,
        pressure_backfill_id,
        defensive_review_id,
        generated.isoformat(),
    )
    artifact_dir = _unique_dir(output_dir / decision_update_id)
    snapshot = {
        "schema_version": WEEKLY_OPS_DECISION_UPDATE_SNAPSHOT_SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_weekly_ops_decision_update_input_snapshot",
        "decision_update_id": artifact_dir.name,
        "generated_at": generated.isoformat(),
        "source_bindings": [weekly, backfill, comparison, review],
        "policy_bindings": [
            _policy_snapshot(sim_policy_path),
            _policy_snapshot(forward_policy_path),
        ],
        "lineage": {
            "weekly_cycle_id": weekly_cycle_id,
            "pressure_backfill_id": pressure_backfill_id,
            "comparison_id": comparison.get("artifact_id"),
            "defensive_review_id": defensive_review_id,
        },
        "decision_boundary": {
            "weekly_recommendation": "continue_tracking",
            "rule_approval_allowed": False,
            "policy_change_allowed": False,
            "broker_action_allowed": False,
        },
        "production_effect": "none",
        "broker_action_allowed": False,
    }
    views, payload = _weekly_update_views(
        snapshot, decision_update_id=artifact_dir.name, update_dir=artifact_dir
    )
    _write_views_atomic(artifact_dir, views)
    _update_latest_pointer(
        "latest_weekly_ops_decision_update",
        artifact_dir.name,
        artifact_dir / "weekly_ops_decision_update_manifest.json",
    )
    return {
        "decision_update_id": artifact_dir.name,
        "decision_update_dir": artifact_dir,
        "input_snapshot": snapshot,
        **payload,
    }


def weekly_ops_decision_update_report_payload(
    *,
    decision_update_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_WEEKLY_OPS_DECISION_UPDATE_DIR,
) -> dict[str, Any]:
    update_dir = _artifact_dir_from_latest(
        output_dir=output_dir,
        artifact_id=decision_update_id if not latest else None,
        pointer_name="latest_weekly_ops_decision_update",
    )
    return {
        **_read_json(update_dir / "weekly_ops_decision_update_manifest.json"),
        **_report_input_snapshot(update_dir / "weekly_ops_decision_update_input_snapshot.json"),
        "updated_weekly_decision_matrix": _read_json(
            update_dir / "updated_weekly_decision_matrix.json"
        ),
        "weekly_ops_next_actions": _read_json(update_dir / "weekly_ops_next_actions.json"),
        "reader_brief_section": _read_text(update_dir / "reader_brief_section.md"),
        "decision_update_dir": str(update_dir),
    }


def validate_weekly_ops_decision_update_artifact(
    *,
    decision_update_id: str,
    output_dir: Path = DEFAULT_WEEKLY_OPS_DECISION_UPDATE_DIR,
) -> dict[str, Any]:
    artifact_dir = output_dir / decision_update_id
    manifest = _read_optional_json(artifact_dir / "weekly_ops_decision_update_manifest.json") or {}
    snapshot = (
        _read_optional_json(artifact_dir / "weekly_ops_decision_update_input_snapshot.json") or {}
    )
    checks = [
        _check(
            "snapshot_exists",
            (artifact_dir / "weekly_ops_decision_update_input_snapshot.json").is_file(),
            "versioned input snapshot required",
        ),
        _check(
            "decision_update_id_matches",
            manifest.get("decision_update_id") == decision_update_id,
            "",
        ),
        _check(
            "safety_no_production",
            manifest.get("production_effect") == "none"
            and manifest.get("broker_action_allowed") is False
            and manifest.get("policy_change_allowed") is False,
            "no production/no broker",
        ),
    ]
    if snapshot:
        try:
            generated = _pressure_datetime(
                snapshot.get("generated_at"), field="weekly update snapshot generated_at"
            )
            snapshot_checks, _ = _snapshot_validation_checks(
                snapshot=snapshot,
                expected_schema=WEEKLY_OPS_DECISION_UPDATE_SNAPSHOT_SCHEMA_VERSION,
                generated=generated,
            )
            checks.extend(snapshot_checks)
            expected, payload = _weekly_update_views(
                snapshot,
                decision_update_id=decision_update_id,
                update_dir=artifact_dir,
            )
            matrix = payload["updated_weekly_decision_matrix"]
            checks.extend(
                [
                    _check(
                        "policy_change_disallowed", matrix.get("policy_change_allowed") is False, ""
                    ),
                    _check(
                        "broker_action_disallowed", matrix.get("broker_action_allowed") is False, ""
                    ),
                    _check(
                        "rule_approval_disallowed", matrix.get("rule_approval_allowed") is False, ""
                    ),
                    _check(
                        "next_actions_present",
                        bool(_records(payload["weekly_ops_next_actions"].get("next_actions"))),
                        "weekly next actions",
                    ),
                ]
            )
            mismatches = _views_match(artifact_dir, expected)
            checks.append(
                _check(
                    "content_derived_views",
                    not mismatches,
                    f"mismatched files: {mismatches}",
                )
            )
        except Exception as exc:  # noqa: BLE001
            checks.append(_check("content_derived_views", False, str(exc)))
    else:
        checks.append(_check("content_derived_views", False, "snapshot missing"))
    return _validation_payload(
        report_type="etf_dynamic_v3_weekly_ops_decision_update_validation",
        artifact_id_key="decision_update_id",
        artifact_id=decision_update_id,
        checks=checks,
    )


def render_pressure_tag_diagnosis_report(
    manifest: Mapping[str, Any],
    distribution: Mapping[str, Any],
    mapping: Mapping[str, Any],
    summary: Mapping[str, Any],
) -> str:
    hit_counts = _mapping(distribution.get("hit_counts"))
    near_counts = _mapping(distribution.get("near_miss_counts"))
    return "\n".join(
        [
            "# Dynamic Rescue Pressure Tag Diagnosis",
            "",
            f"- diagnosis_id: `{manifest.get('diagnosis_id')}`",
            f"- source_tag_id: `{manifest.get('tag_id')}`",
            f"- primary_reason: `{summary.get('primary_reason')}`",
            f"- pressure_relevant_outcomes: {mapping.get('pressure_relevant_outcomes')}",
            f"- forward_outcomes_scanned: {mapping.get('forward_outcomes_scanned')}",
            "- backtest_simulation_outcomes_scanned: "
            f"{mapping.get('backtest_simulation_outcomes_scanned')}",
            "- backtest_simulation_outcomes_available: "
            f"{mapping.get('backtest_simulation_outcomes_available')}",
            f"- tech_drawdown_hits: {hit_counts.get('tech_drawdown')}",
            f"- risk_off_hits: {hit_counts.get('risk_off')}",
            f"- semiconductor_pullback_hits: {hit_counts.get('semiconductor_pullback')}",
            f"- near_miss_windows: {sum(_int(v) for v in near_counts.values())}",
            "- threshold_adjustment_recommended: "
            f"`{summary.get('threshold_adjustment_recommended')}`",
            "- tagging_logic_adjustment_recommended: "
            f"`{summary.get('tagging_logic_adjustment_recommended')}`",
            "- conclusion: forward outcome windows did not overlap tagged pressure windows; "
            "simulation pressure evidence must be backfilled as research-only, "
            "not forward evidence.",
            "- broker_action_allowed: `false`",
            "- production_effect: `none`",
            "",
        ]
    )


def render_pressure_backfill_report(manifest: Mapping[str, Any], summary: Mapping[str, Any]) -> str:
    by_source = _mapping(summary.get("by_source_mode"))
    by_regime = _mapping(summary.get("by_regime"))
    return "\n".join(
        [
            "# Dynamic Rescue Pressure Outcome Backfill",
            "",
            f"- pressure_backfill_id: `{manifest.get('pressure_backfill_id')}`",
            f"- date_range: `{manifest.get('start')}` to `{manifest.get('end')}`",
            f"- total_pressure_outcomes: {summary.get('total_pressure_outcomes')}",
            f"- FORWARD_OUTCOME: {by_source.get('FORWARD_OUTCOME')}",
            f"- HISTORICAL_REPLAY: {by_source.get('HISTORICAL_REPLAY')}",
            f"- BACKTEST_SIMULATION: {by_source.get('BACKTEST_SIMULATION')}",
            f"- tech_drawdown: {by_regime.get('tech_drawdown')}",
            f"- risk_off: {by_regime.get('risk_off')}",
            f"- semiconductor_pullback: {by_regime.get('semiconductor_pullback')}",
            "- defensive_validation_relevant_count: "
            f"{summary.get('defensive_validation_relevant_count')}",
            "- research_evidence: `HISTORICAL_REPLAY`, `BACKTEST_SIMULATION`",
            "- forward_confirmation_evidence: `FORWARD_OUTCOME` only",
            "- defensive_rule_approval_allowed: `false`",
            "- production_effect: `none`",
            "",
        ]
    )


def render_defensive_pressure_compare_report(
    manifest: Mapping[str, Any],
    summary: Mapping[str, Any],
    metrics: Sequence[Mapping[str, Any]],
    pairwise: Mapping[str, Any],
) -> str:
    source_modes = _mapping(summary.get("source_mode_breakdown"))
    defensive_rows = [
        row
        for row in metrics
        if row.get("variant") == "defensive_limited_adjustment"
        and row.get("regime") in PRESSURE_REGIMES
    ]
    lines = [
        "# Dynamic Rescue Defensive Pressure Compare",
        "",
        f"- comparison_id: `{manifest.get('comparison_id')}`",
        f"- pressure_backfill_id: `{manifest.get('pressure_backfill_id')}`",
        f"- defensive_status: `{summary.get('defensive_status')}`",
        f"- can_support_rule_approval: `{summary.get('can_support_rule_approval')}`",
        f"- FORWARD_OUTCOME: `{source_modes.get('FORWARD_OUTCOME')}`",
        f"- HISTORICAL_REPLAY: `{source_modes.get('HISTORICAL_REPLAY')}`",
        f"- BACKTEST_SIMULATION: `{source_modes.get('BACKTEST_SIMULATION')}`",
        "",
        "## Defensive Limited Adjustment",
    ]
    for row in defensive_rows:
        lines.append(
            "- "
            f"{row.get('source_mode')} / {row.get('regime')}: "
            f"sample_count={row.get('sample_count')}, "
            f"avg_relative_to_no_trade={row.get('avg_relative_to_no_trade')}, "
            f"drawdown_delta_vs_no_trade={row.get('drawdown_delta_vs_no_trade')}, "
            f"status=`{row.get('status')}`"
        )
    lines.extend(
        [
            "",
            "## Pairwise",
            f"- comparison_count: {len(_records(pairwise.get('comparisons')))}",
            "- conclusion: simulation pressure samples can inform research, but forward "
            "pressure evidence is insufficient for approval.",
            "- policy_change_allowed: `false`",
            "- production_effect: `none`",
            "",
        ]
    )
    return "\n".join(lines)


def render_defensive_rule_review_report(
    manifest: Mapping[str, Any],
    matrix: Mapping[str, Any],
    defensive_summary: Mapping[str, Any],
) -> str:
    return "\n".join(
        [
            "# Dynamic Rescue Defensive Rule Review",
            "",
            f"- review_id: `{manifest.get('review_id')}`",
            f"- rule_name: `{matrix.get('rule_name')}`",
            f"- current_status: `{matrix.get('current_status')}`",
            f"- recommended_status: `{matrix.get('recommended_status')}`",
            f"- rule_approval_allowed: `{matrix.get('rule_approval_allowed')}`",
            f"- auto_apply: `{matrix.get('auto_apply')}`",
            f"- owner_approval_required: `{matrix.get('owner_approval_required')}`",
            f"- reason: {matrix.get('reason')}",
            f"- source_mode_breakdown: `{defensive_summary.get('source_mode_breakdown')}`",
            "- conclusion: defensive_limited_adjustment remains research-only until "
            "forward pressure samples prove drawdown improvement.",
            "- broker_action_allowed: `false`",
            "- production_effect: `none`",
            "",
        ]
    )


def render_defensive_rule_owner_checklist(
    matrix: Mapping[str, Any], defensive_summary: Mapping[str, Any]
) -> str:
    return "\n".join(
        [
            "# Defensive Rule Owner Checklist",
            "",
            "- 是否继续使用 `defensive_limited_adjustment` 这个名称？",
            "- 是否将其改名为 `active_limited_adjustment` 或 `risk_aware_limited_adjustment`？",
            "- 是否继续作为 observe-only variant？",
            "- 是否需要 forward pressure samples 后再评估？",
            "- 是否禁止其进入默认执行规则？",
            "- 是否继续 no broker / no production？",
            "",
            "## 当前判断",
            "",
            f"- recommended_status: `{matrix.get('recommended_status')}`",
            f"- rule_approval_allowed: `{matrix.get('rule_approval_allowed')}`",
            f"- defensive_status: `{defensive_summary.get('defensive_status')}`",
            "- local drawdown_delta convention: positive means less drawdown than no_trade.",
            "",
        ]
    )


def render_defensive_rule_reader_brief(matrix: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "## Dynamic Rescue Defensive Rule Status",
            "",
            f"- rule_name: `{matrix.get('rule_name')}`",
            f"- recommended_status: `{matrix.get('recommended_status')}`",
            f"- rule_approval_allowed: `{matrix.get('rule_approval_allowed')}`",
            f"- auto_apply: `{matrix.get('auto_apply')}`",
            "- production_effect: `none`",
            "",
        ]
    )


def render_weekly_ops_decision_update_report(
    manifest: Mapping[str, Any],
    matrix: Mapping[str, Any],
    actions: Mapping[str, Any],
) -> str:
    return "\n".join(
        [
            "# Dynamic Rescue Weekly Operations Decision Update",
            "",
            f"- decision_update_id: `{manifest.get('decision_update_id')}`",
            f"- weekly_cycle_id: `{matrix.get('weekly_cycle_id')}`",
            f"- pressure_backfill_id: `{matrix.get('pressure_backfill_id')}`",
            f"- defensive_review_id: `{matrix.get('defensive_review_id')}`",
            "- defensive_validation_relevant_outcomes_before: "
            f"{matrix.get('defensive_validation_relevant_outcomes_before')}",
            "- defensive_validation_relevant_outcomes_after: "
            f"{matrix.get('defensive_validation_relevant_outcomes_after')}",
            f"- defensive_rule_status: `{matrix.get('defensive_rule_status')}`",
            f"- weekly_recommendation: `{matrix.get('weekly_recommendation')}`",
            f"- owner_action_required: `{matrix.get('owner_action_required')}`",
            f"- policy_change_allowed: `{matrix.get('policy_change_allowed')}`",
            f"- broker_action_allowed: `{matrix.get('broker_action_allowed')}`",
            "",
            "## Next Actions",
            *[
                f"- {row.get('action')}: `{row.get('priority')}` - {row.get('reason')}"
                for row in _records(actions.get("next_actions"))
            ],
            "",
        ]
    )


def render_weekly_ops_decision_reader_brief(
    matrix: Mapping[str, Any], actions: Mapping[str, Any]
) -> str:
    return "\n".join(
        [
            "## Dynamic Rescue Weekly Operations Decision",
            "",
            "- pressure_sample_status: "
            f"`{matrix.get('defensive_validation_relevant_outcomes_after')}` "
            "relevant pressure outcomes",
            f"- defensive_rule_status: `{matrix.get('defensive_rule_status')}`",
            f"- weekly_recommendation: `{matrix.get('weekly_recommendation')}`",
            f"- policy_change_allowed: `{matrix.get('policy_change_allowed')}`",
            f"- broker_action_allowed: `{matrix.get('broker_action_allowed')}`",
            "- next_actions: "
            + ", ".join(_text(row.get("action")) for row in _records(actions.get("next_actions"))),
            "",
        ]
    )


def _threshold_distribution_and_near_misses(
    *,
    window_tags: Sequence[Mapping[str, Any]],
    thresholds: Mapping[str, Any],
    config_path: Path,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    hit_counts = Counter(
        tag for row in window_tags for tag in _records_to_texts(row.get("regime_tags"))
    )
    vol_values = sorted(
        _float(_mapping(row.get("metrics")).get("realized_volatility")) for row in window_tags
    )
    vol_threshold = _percentile(
        vol_values,
        _float(thresholds.get("risk_off_volatility_percentile")),
    )
    near_misses: list[dict[str, Any]] = []
    for row in window_tags:
        metrics = _mapping(row.get("metrics"))
        tags = set(_records_to_texts(row.get("regime_tags")))
        _append_near_miss(
            near_misses,
            row=row,
            metric_name="qqq_drawdown",
            candidate_tag="tech_drawdown",
            actual=_float(metrics.get("qqq_drawdown")),
            threshold=_float(thresholds.get("tech_drawdown_pct")),
            already_hit="tech_drawdown" in tags,
            distance_band=NEAR_MISS_ABS_DISTANCE,
        )
        _append_near_miss(
            near_misses,
            row=row,
            metric_name="smh_drawdown",
            candidate_tag="semiconductor_pullback",
            actual=_float(metrics.get("smh_drawdown")),
            threshold=_float(thresholds.get("semiconductor_pullback_pct")),
            already_hit="semiconductor_pullback" in tags,
            distance_band=NEAR_MISS_ABS_DISTANCE,
        )
        qqq_drawdown = _float(metrics.get("qqq_drawdown"))
        vol = _float(metrics.get("realized_volatility"))
        if (
            "risk_off" not in tags
            and qqq_drawdown <= _float(thresholds.get("tech_drawdown_pct"))
            and 0 <= vol_threshold - vol <= NEAR_MISS_VOL_DISTANCE
        ):
            near_misses.append(
                _near_miss_row(
                    row=row,
                    candidate_tag="risk_off",
                    actual_metric=round(vol, 6),
                    threshold=round(vol_threshold, 6),
                    distance=round(vol_threshold - vol, 6),
                )
            )
    near_counts = Counter(_text(row.get("candidate_tag")) for row in near_misses)
    return (
        {
            "schema_version": SCHEMA_VERSION,
            "report_type": "etf_dynamic_v3_threshold_hit_distribution",
            "config_path": str(config_path),
            "thresholds": {
                "tech_drawdown_pct": _float(thresholds.get("tech_drawdown_pct")),
                "semiconductor_pullback_pct": _float(thresholds.get("semiconductor_pullback_pct")),
                "risk_off_volatility_percentile": _float(
                    thresholds.get("risk_off_volatility_percentile")
                ),
                "risk_off_realized_volatility_threshold": round(vol_threshold, 6),
            },
            "hit_counts": {tag: hit_counts.get(tag, 0) for tag in PRESSURE_TAGS},
            "near_miss_counts": {tag: near_counts.get(tag, 0) for tag in PRESSURE_REGIMES},
            "production_effect": "none",
            "broker_action_allowed": False,
        },
        near_misses,
    )


def _append_near_miss(
    rows: list[dict[str, Any]],
    *,
    row: Mapping[str, Any],
    metric_name: str,
    candidate_tag: str,
    actual: float,
    threshold: float,
    already_hit: bool,
    distance_band: float,
) -> None:
    del metric_name
    distance = actual - threshold
    if already_hit or not (0 < distance <= distance_band):
        return
    rows.append(
        _near_miss_row(
            row=row,
            candidate_tag=candidate_tag,
            actual_metric=round(actual, 6),
            threshold=round(threshold, 6),
            distance=round(distance, 6),
        )
    )


def _near_miss_row(
    *,
    row: Mapping[str, Any],
    candidate_tag: str,
    actual_metric: float,
    threshold: float,
    distance: float,
) -> dict[str, Any]:
    return {
        "window_id": _text(row.get("window_id")),
        "start_date": _text(row.get("start_date")),
        "end_date": _text(row.get("end_date")),
        "candidate_tag": candidate_tag,
        "actual_metric": actual_metric,
        "threshold": threshold,
        "distance_to_threshold": distance,
        "near_miss": True,
        "suggested_action": "review_threshold_not_auto_change",
        "production_effect": "none",
        "broker_action_allowed": False,
    }


def _outcome_mapping_diagnostics(
    *,
    outcome_tags: Sequence[Mapping[str, Any]],
    pressure_summary: Mapping[str, Any],
    backfilled_outcome_dir: Path,
    backtest_sim_outcome_dir: Path,
) -> dict[str, Any]:
    forward_scanned = len(outcome_tags)
    with_tags = sum(1 for row in outcome_tags if _records_to_texts(row.get("regime_tags")))
    missing_tags = forward_scanned - with_tags
    backfilled_available = _latest_jsonl_count(
        backfilled_outcome_dir,
        "latest_backfilled_outcome",
        "replay_outcome_windows.jsonl",
    )
    sim_available, sim_pressure_available = _latest_backtest_sim_counts(backtest_sim_outcome_dir)
    mapping_failures = []
    if (
        _int(pressure_summary.get("pressure_window_count")) > 0
        and _int(pressure_summary.get("pressure_tagged_outcomes")) == 0
    ):
        mapping_failures.append(
            {
                "reason": "outcome_window_not_mapped_to_regime_window",
                "count": forward_scanned,
            }
        )
    if missing_tags:
        mapping_failures.append({"reason": "outcomes_missing_regime_tags", "count": missing_tags})
    if sim_pressure_available:
        mapping_failures.append(
            {
                "reason": "pressure_tagger_does_not_scan_backtest_simulation",
                "count": sim_pressure_available,
            }
        )
    if backfilled_available:
        mapping_failures.append(
            {
                "reason": "pressure_tagger_does_not_scan_historical_replay_backfill",
                "count": backfilled_available,
            }
        )
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_outcome_mapping_diagnostics",
        "forward_outcomes_scanned": forward_scanned,
        "historical_replay_outcomes_scanned": 0,
        "backtest_simulation_outcomes_scanned": 0,
        "historical_replay_outcomes_available": backfilled_available,
        "backtest_simulation_outcomes_available": sim_available,
        "backtest_simulation_pressure_outcomes_available": sim_pressure_available,
        "outcomes_with_regime_tags": with_tags,
        "outcomes_missing_regime_tags": missing_tags,
        "pressure_relevant_outcomes": _int(
            pressure_summary.get("defensive_validation_relevant_outcomes")
        ),
        "mapping_failures": mapping_failures,
        "production_effect": "none",
        "broker_action_allowed": False,
    }


def _pressure_tag_diagnosis_summary(
    *,
    pressure_summary: Mapping[str, Any],
    distribution: Mapping[str, Any],
    mapping_diagnostics: Mapping[str, Any],
    near_misses: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    pressure_window_count = _int(pressure_summary.get("pressure_window_count"))
    relevant = _int(mapping_diagnostics.get("pressure_relevant_outcomes"))
    sim_pressure = _int(mapping_diagnostics.get("backtest_simulation_pressure_outcomes_available"))
    if pressure_window_count <= 0:
        primary = "thresholds_or_price_proxy_produced_no_pressure_windows"
    elif relevant <= 0 and sim_pressure > 0:
        primary = "forward_outcome_mapping_gap_and_simulation_not_scanned"
    elif relevant <= 0:
        primary = "forward_outcome_window_not_mapped_to_pressure_window"
    else:
        primary = "pressure_outcome_mapping_available"
    hit_counts = _mapping(distribution.get("hit_counts"))
    threshold_maybe_strict = sum(
        _int(hit_counts.get(tag)) for tag in PRESSURE_REGIMES
    ) == 0 and bool(near_misses)
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_pressure_tag_diagnosis_summary",
        "primary_reason": primary,
        "threshold_too_strict": threshold_maybe_strict,
        "outcome_mapping_problem": relevant <= 0 and pressure_window_count > 0,
        "simulation_not_scanned": sim_pressure > 0,
        "near_miss_window_count": len(near_misses),
        "threshold_adjustment_recommended": (
            "review_only_not_auto_change" if threshold_maybe_strict else "not_primary"
        ),
        "tagging_logic_adjustment_recommended": (
            "include_research_only_simulation_inventory"
            if sim_pressure > 0
            else "review_forward_window_mapping"
        ),
        "production_effect": "none",
        "broker_action_allowed": False,
    }


def _forward_pressure_inventory(
    *,
    outcome_regime_tags: Sequence[Mapping[str, Any]],
    advisory_outcome_dir: Path,
    start: date,
    end: date,
) -> list[dict[str, Any]]:
    outcome_rows_by_key = _forward_outcome_rows_by_key(advisory_outcome_dir)
    rows = []
    for tag_row in outcome_regime_tags:
        as_of = _date_from_any(tag_row.get("as_of"))
        if as_of is None or not (start <= as_of <= end):
            continue
        tags = _records_to_texts(tag_row.get("regime_tags"))
        if not (set(tags) & PRESSURE_VALIDATION_TAGS):
            continue
        key = (
            _text(tag_row.get("outcome_id")),
            _text(tag_row.get("daily_advisory_id")),
            _int(tag_row.get("window_days")),
        )
        source_window = outcome_rows_by_key.get(key, {})
        rows.append(
            _pressure_inventory_row(
                source_mode="FORWARD_OUTCOME",
                source_artifact_id=_text(tag_row.get("outcome_id")),
                source_event_id=_text(tag_row.get("daily_advisory_id")),
                as_of=_text(tag_row.get("as_of")),
                window_days=_int(tag_row.get("window_days")),
                regime_tags=tags,
                variant_results=_forward_variant_results(source_window),
                outcome_status=_text(source_window.get("outcome_status"), "UNKNOWN"),
            )
        )
    return rows


def _historical_replay_pressure_inventory(
    *,
    pressure_window_tags: Sequence[Mapping[str, Any]],
    backfilled_outcome_dir: Path,
    start: date,
    end: date,
) -> list[dict[str, Any]]:
    source_dir = _optional_latest_dir(backfilled_outcome_dir, "latest_backfilled_outcome")
    if source_dir is None:
        return []
    manifest = _read_optional_json(source_dir / "backfill_manifest.json") or {}
    pressure_by_end = _pressure_tags_by_end(pressure_window_tags)
    grouped: dict[tuple[str, str, int], list[Mapping[str, Any]]] = defaultdict(list)
    for row in _read_jsonl(source_dir / "replay_outcome_windows.jsonl"):
        as_of = _date_from_any(row.get("as_of") or row.get("start_date"))
        if as_of is None or not (start <= as_of <= end):
            continue
        grouped[
            (
                _text(row.get("replay_event_id") or row.get("daily_advisory_id")),
                _text(row.get("as_of") or row.get("start_date")),
                _int(row.get("window_days")),
            )
        ].append(row)
    inventory = []
    for (event_id, as_of_text, window_days), rows in sorted(grouped.items()):
        first = rows[0]
        tags = _records_to_texts(first.get("regime_tags"))
        if not tags:
            tags = pressure_by_end.get((_text(first.get("end_date")), window_days), [])
        if not (set(tags) & PRESSURE_VALIDATION_TAGS):
            continue
        inventory.append(
            _pressure_inventory_row(
                source_mode="HISTORICAL_REPLAY",
                source_artifact_id=_text(manifest.get("backfill_id"), source_dir.name),
                source_event_id=event_id,
                as_of=as_of_text,
                window_days=window_days,
                regime_tags=tags,
                variant_results=_variant_results_from_rows(rows),
                outcome_status=_text(first.get("outcome_status"), "UNKNOWN"),
            )
        )
    return inventory


def _backtest_simulation_pressure_inventory(
    *, backtest_sim_outcome_dir: Path, start: date, end: date
) -> list[dict[str, Any]]:
    source_dir = _optional_latest_dir(backtest_sim_outcome_dir, "latest_backtest_sim_outcome")
    if source_dir is None:
        return []
    manifest = _read_json(source_dir / "sim_outcome_manifest.json")
    grouped: dict[tuple[str, str, int, str], list[Mapping[str, Any]]] = defaultdict(list)
    for row in _read_jsonl(source_dir / "simulated_outcome_windows.jsonl"):
        as_of = _date_from_any(row.get("as_of") or row.get("start_date"))
        if as_of is None or not (start <= as_of <= end):
            continue
        regime = _text(row.get("regime_label"), "unknown")
        if regime not in PRESSURE_VALIDATION_TAGS:
            continue
        grouped[
            (
                _text(row.get("sim_event_id")),
                _text(row.get("as_of") or row.get("start_date")),
                _int(row.get("window_days")),
                regime,
            )
        ].append(row)
    inventory = []
    for (event_id, as_of_text, window_days, regime), rows in sorted(grouped.items()):
        first = rows[0]
        inventory.append(
            _pressure_inventory_row(
                source_mode="BACKTEST_SIMULATION",
                source_artifact_id=_text(manifest.get("sim_outcome_id"), source_dir.name),
                source_event_id=event_id,
                as_of=as_of_text,
                window_days=window_days,
                regime_tags=[regime],
                variant_results=_variant_results_from_rows(rows),
                outcome_status=_text(first.get("outcome_status"), "UNKNOWN"),
            )
        )
    return inventory


def _pressure_inventory_row(
    *,
    source_mode: str,
    source_artifact_id: str,
    source_event_id: str,
    as_of: str,
    window_days: int,
    regime_tags: Sequence[str],
    variant_results: Mapping[str, Any],
    outcome_status: str,
) -> dict[str, Any]:
    pressure = bool(set(regime_tags) & PRESSURE_VALIDATION_TAGS)
    paired_variants = (
        "defensive_limited_adjustment" in variant_results and "no_trade" in variant_results
    )
    defensive_relevant = pressure and outcome_status == "AVAILABLE" and paired_variants
    can_support_production = source_mode == "FORWARD_OUTCOME" and defensive_relevant
    return {
        "schema_version": SCHEMA_VERSION,
        "pressure_outcome_id": _stable_id(
            "pressure-outcome",
            source_mode,
            source_artifact_id,
            source_event_id,
            as_of,
            window_days,
            ",".join(sorted(regime_tags)),
        ),
        "source_mode": source_mode,
        "source_artifact_id": source_artifact_id,
        "source_event_id": source_event_id,
        "as_of": as_of,
        "window_days": window_days,
        "regime_tags": list(regime_tags),
        "pressure_regime": pressure,
        "defensive_validation_relevant": defensive_relevant,
        "outcome_status": outcome_status,
        "variant_results": dict(variant_results),
        "evidence_quality": EVIDENCE_QUALITY_BY_SOURCE[source_mode],
        "can_support_production": can_support_production,
        "production_effect": "none",
        "broker_action_allowed": False,
    }


def _pressure_source_summary(inventory: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    by_source = Counter(_text(row.get("source_mode")) for row in inventory)
    distinct_events_by_source = {
        mode: len(
            {
                _text(row.get("source_event_id"))
                for row in inventory
                if row.get("source_mode") == mode and _text(row.get("source_event_id"))
            }
        )
        for mode in SOURCE_MODES
    }
    by_regime: Counter[str] = Counter()
    for row in inventory:
        for tag in _records_to_texts(row.get("regime_tags")):
            by_regime[tag] += 1
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_pressure_source_summary",
        "total_pressure_outcomes": len(inventory),
        "by_source_mode": {mode: by_source.get(mode, 0) for mode in SOURCE_MODES},
        "distinct_event_count_by_source_mode": distinct_events_by_source,
        "by_regime": {tag: by_regime.get(tag, 0) for tag in PRESSURE_TAGS},
        "defensive_validation_relevant_count": sum(
            1 for row in inventory if row.get("defensive_validation_relevant") is True
        ),
        "can_support_production_count": sum(
            1 for row in inventory if row.get("can_support_production") is True
        ),
        "production_effect": "none",
        "broker_action_allowed": False,
    }


def _pressure_variant_metrics(
    inventory: Sequence[Mapping[str, Any]], *, policy: Mapping[str, Any]
) -> list[dict[str, Any]]:
    result = []
    for source_mode in SOURCE_MODES:
        for regime in policy["pressure_regimes"]:
            regime_rows = [
                row
                for row in inventory
                if row.get("source_mode") == source_mode
                and regime in _records_to_texts(row.get("regime_tags"))
                and row.get("defensive_validation_relevant") is True
                and _int(row.get("window_days")) in policy["tracked_window_days"]
            ]
            for variant in COMPARISON_VARIANTS:
                variant_samples = [
                    _variant_sample_metrics(row, variant)
                    for row in regime_rows
                    if variant in _mapping(row.get("variant_results"))
                ]
                result.append(
                    _pressure_variant_metric_row(
                        source_mode=source_mode,
                        regime=regime,
                        variant=variant,
                        samples=variant_samples,
                        minimum_events=_int(
                            policy.get("minimum_distinct_events_per_pressure_regime")
                        ),
                    )
                )
    return result


def _pressure_variant_metric_row(
    *,
    source_mode: str,
    regime: str,
    variant: str,
    samples: Sequence[Mapping[str, Any]],
    minimum_events: int,
) -> dict[str, Any]:
    sample_count = len(samples)
    distinct_event_count = len(
        {_text(row.get("source_event_id")) for row in samples if row.get("source_event_id")}
    )
    status = (
        "INSUFFICIENT_DATA"
        if sample_count <= 0
        else "PASS"
        if distinct_event_count >= minimum_events
        else "PASS_WITH_WARNINGS"
    )
    return {
        "schema_version": SCHEMA_VERSION,
        "source_mode": source_mode,
        "regime": regime,
        "variant": variant,
        "sample_count": sample_count,
        "distinct_event_count": distinct_event_count,
        "minimum_distinct_events_required": minimum_events,
        "avg_return": _rounded_optional_avg([row.get("return") for row in samples]),
        "avg_relative_to_no_trade": _rounded_optional_avg(
            [row.get("relative_to_no_trade") for row in samples]
        ),
        "win_rate_vs_no_trade": (
            round(
                sum(1 for row in samples if _float(row.get("relative_to_no_trade")) > 0)
                / sample_count,
                6,
            )
            if sample_count
            else None
        ),
        "avg_max_drawdown": _rounded_optional_avg([row.get("max_drawdown") for row in samples]),
        "drawdown_delta_vs_no_trade": _rounded_optional_avg(
            [row.get("drawdown_delta_vs_no_trade") for row in samples]
        ),
        "avg_turnover": _rounded_optional_avg([row.get("turnover") for row in samples]),
        "status": status,
        "production_effect": "none",
        "broker_action_allowed": False,
    }


def _defensive_pairwise_comparison(
    inventory: Sequence[Mapping[str, Any]], *, policy: Mapping[str, Any]
) -> dict[str, Any]:
    comparisons = []
    for source_mode in SOURCE_MODES:
        for regime in policy["pressure_regimes"]:
            rows = [
                row
                for row in inventory
                if row.get("source_mode") == source_mode
                and regime in _records_to_texts(row.get("regime_tags"))
                and row.get("defensive_validation_relevant") is True
                and _int(row.get("window_days")) in policy["tracked_window_days"]
            ]
            samples = [
                _variant_sample_metrics(row, "defensive_limited_adjustment")
                for row in rows
                if "defensive_limited_adjustment" in _mapping(row.get("variant_results"))
                and "no_trade" in _mapping(row.get("variant_results"))
            ]
            return_delta = _rounded_optional_avg(
                [row.get("relative_to_no_trade") for row in samples]
            )
            drawdown_delta = _rounded_optional_avg(
                [row.get("drawdown_delta_vs_no_trade") for row in samples]
            )
            sample_count = len(samples)
            distinct_event_count = len(
                {_text(row.get("source_event_id")) for row in samples if row.get("source_event_id")}
            )
            win_rate = (
                round(
                    sum(1 for row in samples if _float(row.get("relative_to_no_trade")) > 0)
                    / sample_count,
                    6,
                )
                if sample_count
                else None
            )
            sufficient = distinct_event_count >= _int(
                policy.get("minimum_distinct_events_per_pressure_regime")
            )
            comparisons.append(
                {
                    "schema_version": SCHEMA_VERSION,
                    "source_mode": source_mode,
                    "regime": regime,
                    "variant_a": "defensive_limited_adjustment",
                    "variant_b": "no_trade",
                    "sample_count": sample_count,
                    "distinct_event_count": distinct_event_count,
                    "minimum_distinct_events_required": policy[
                        "minimum_distinct_events_per_pressure_regime"
                    ],
                    "return_delta": return_delta,
                    "drawdown_delta": drawdown_delta,
                    "win_rate": win_rate,
                    "evidence_status": "SUFFICIENT" if sufficient else "INSUFFICIENT_DATA",
                    "conclusion": _pairwise_conclusion(
                        sample_count,
                        return_delta,
                        drawdown_delta,
                        win_rate,
                        sufficient=sufficient,
                        policy=policy,
                    ),
                }
            )
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_defensive_pairwise_comparison",
        "comparisons": comparisons,
        "production_effect": "none",
        "broker_action_allowed": False,
    }


def _defensive_pressure_summary(
    metrics: Sequence[Mapping[str, Any]],
    pairwise: Mapping[str, Any],
    *,
    policy: Mapping[str, Any],
) -> dict[str, Any]:
    source_breakdown = {
        mode: _source_mode_defensive_status(
            mode,
            _records(pairwise.get("comparisons")),
            required_regimes=policy["pressure_regimes"],
        )
        for mode in SOURCE_MODES
    }
    forward_status = source_breakdown["FORWARD_OUTCOME"]
    sim_status = source_breakdown["BACKTEST_SIMULATION"]
    if forward_status == "PROVEN_DEFENSIVE":
        overall = "PROVEN_DEFENSIVE"
    elif forward_status == "FAILS_DEFENSIVE_EXPECTATION":
        overall = "FAILS_DEFENSIVE_EXPECTATION"
    elif sim_status in {"PROVEN_DEFENSIVE", "PARTIALLY_DEFENSIVE"}:
        overall = "INSUFFICIENT_FORWARD_DATA"
    elif sim_status == "FAILS_DEFENSIVE_EXPECTATION":
        overall = "FAILS_DEFENSIVE_EXPECTATION"
    else:
        overall = "NOT_PROVEN_DEFENSIVE"
    can_approve = forward_status == "PROVEN_DEFENSIVE"
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_defensive_pressure_summary",
        "defensive_status": overall,
        "source_mode_breakdown": source_breakdown,
        "primary_conclusion": (
            "defensive_limited_adjustment remains research-only and requires "
            "forward pressure samples."
            if not can_approve
            else "forward pressure samples support owner review for defensive behavior."
        ),
        "can_support_rule_approval": can_approve,
        "reviewed_policy": dict(policy),
        "metrics_row_count": len(metrics),
        "production_effect": "none",
        "broker_action_allowed": False,
    }


def _defensive_rule_decision_matrix(
    defensive_summary: Mapping[str, Any], *, policy: Mapping[str, Any]
) -> dict[str, Any]:
    status = _text(defensive_summary.get("defensive_status"), "NOT_PROVEN_DEFENSIVE")
    if status == "PROVEN_DEFENSIVE":
        recommended = "OWNER_REVIEW_REQUIRED"
    elif status == "FAILS_DEFENSIVE_EXPECTATION":
        recommended = "RENAME_RECOMMENDED"
    elif status == "INSUFFICIENT_FORWARD_DATA":
        recommended = "RESEARCH_ONLY"
    else:
        recommended = "RESEARCH_ONLY"
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_defensive_rule_decision_matrix",
        "rule_name": "defensive_limited_adjustment",
        "current_status": status,
        "recommended_status": recommended,
        "rule_approval_allowed": False,
        "auto_apply": False,
        "owner_approval_required": True,
        "reason": (
            "Simulation evidence is not enough to approve defensive behavior; "
            "forward pressure samples remain insufficient."
        ),
        "required_next_evidence": [
            "at least "
            f"{policy['required_forward_pressure_samples']} forward pressure regime events",
            "drawdown_delta_vs_no_trade >= "
            f"{policy['minimum_drawdown_delta_vs_no_trade']} under the local convention "
            "that positive means lower drawdown",
            f"win_rate_vs_no_trade >= {policy['minimum_win_rate_vs_no_trade']}",
        ],
        "reviewed_policy_versions": policy["policy_versions"],
        "policy_change_allowed": False,
        "broker_action_allowed": False,
        "production_effect": "none",
    }


def _weekly_ops_next_actions(
    matrix: Mapping[str, Any],
    backfill_summary: Mapping[str, Any],
    review_matrix: Mapping[str, Any],
    *,
    policy: Mapping[str, Any],
) -> dict[str, Any]:
    forward_count = _int(
        _mapping(backfill_summary.get("distinct_event_count_by_source_mode")).get(
            "FORWARD_OUTCOME"
        )
    )
    forward_floor = _int(policy.get("required_forward_pressure_samples"))
    actions = [
        {
            "action": "continue_pressure_sample_collection",
            "priority": "HIGH",
            "reason": "Forward pressure regime outcomes remain insufficient."
            if forward_count < forward_floor
            else "Forward pressure outcomes are available but still require owner review.",
        },
        {
            "action": "review_defensive_label",
            "priority": "MEDIUM",
            "reason": "Simulation evidence does not prove production-ready defensive behavior.",
        },
        {
            "action": "do_not_approve_defensive_rule",
            "priority": "HIGH",
            "reason": "Rule approval remains blocked without sufficient forward pressure evidence.",
        },
    ]
    if _text(review_matrix.get("recommended_status")) == "RENAME_RECOMMENDED":
        actions.append(
            {
                "action": "owner_review_defensive_name",
                "priority": "MEDIUM",
                "reason": (
                    "Pressure comparison suggests the current defensive label may be misleading."
                ),
            }
        )
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_weekly_ops_next_actions",
        "next_actions": actions,
        "weekly_recommendation": _text(matrix.get("weekly_recommendation"), "continue_tracking"),
        "required_forward_pressure_samples": forward_floor,
        "reviewed_policy_versions": policy["policy_versions"],
        "policy_change_allowed": False,
        "broker_action_allowed": False,
        "production_effect": "none",
    }


def _variant_sample_metrics(row: Mapping[str, Any], variant: str) -> dict[str, Any]:
    variant_result = _mapping(_mapping(row.get("variant_results")).get(variant))
    no_trade = _mapping(_mapping(row.get("variant_results")).get("no_trade"))
    return_value = _float(variant_result.get("return"))
    no_trade_return = _float(no_trade.get("return"))
    max_drawdown = _float(variant_result.get("max_drawdown"))
    no_trade_drawdown = _float(no_trade.get("max_drawdown"))
    return {
        "source_event_id": _text(row.get("source_event_id")),
        "as_of": _text(row.get("as_of")),
        "window_days": _int(row.get("window_days")),
        "return": return_value,
        "relative_to_no_trade": return_value - no_trade_return,
        "max_drawdown": max_drawdown,
        "drawdown_delta_vs_no_trade": max_drawdown - no_trade_drawdown,
        "turnover": _float(variant_result.get("turnover")),
    }


def _variant_results_from_rows(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    result = {}
    for row in rows:
        variant = _normalize_variant_name(_text(row.get("variant")))
        if not variant:
            continue
        metrics = {
            "return": _float(row.get("return")),
            "max_drawdown": _float(row.get("max_drawdown")),
            "turnover": _float(row.get("turnover")),
            "relative_to_no_trade": _float(row.get("relative_to_no_trade")),
        }
        for field in ("risk_asset_exposure", "risk_exposure", "equity_exposure"):
            if field in row:
                metrics[field] = _float(row.get(field))
        result[variant] = metrics
    return result


def _forward_variant_results(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "no_trade": {
            "return": _float(row.get("no_trade_return")),
            "max_drawdown": _float(row.get("max_drawdown")),
            "turnover": 0.0,
        },
        "limited_adjustment": {
            "return": _float(row.get("paper_portfolio_return")),
            "max_drawdown": _float(row.get("max_drawdown")),
            "turnover": 0.0,
        },
        "consensus_target": {
            "return": _float(row.get("target_weight_return")),
            "max_drawdown": _float(row.get("max_drawdown")),
            "turnover": 0.0,
        },
    }


def _forward_outcome_rows_by_key(
    advisory_outcome_dir: Path,
) -> dict[tuple[str, str, int], dict[str, Any]]:
    by_key = {}
    for manifest_path in sorted(advisory_outcome_dir.glob("*/advisory_outcome_manifest.json")):
        manifest = _read_optional_json(manifest_path) or {}
        outcome_id = _text(manifest.get("outcome_id"), manifest_path.parent.name)
        for row in _read_jsonl(manifest_path.parent / "outcome_windows.jsonl"):
            by_key[
                (
                    outcome_id,
                    _text(row.get("daily_advisory_id") or manifest.get("daily_advisory_id")),
                    _int(row.get("window_days")),
                )
            ] = row
    return by_key


def _pressure_tags_by_end(
    pressure_window_tags: Sequence[Mapping[str, Any]],
) -> dict[tuple[str, int], list[str]]:
    result = {}
    for row in pressure_window_tags:
        tags = [
            tag
            for tag in _records_to_texts(row.get("regime_tags"))
            if tag in PRESSURE_VALIDATION_TAGS
        ]
        if tags:
            result[(_text(row.get("end_date")), _int(row.get("window_days")))] = tags
    return result


def _source_mode_defensive_status(
    source_mode: str,
    comparisons: Sequence[Mapping[str, Any]],
    *,
    required_regimes: Sequence[str],
) -> str:
    rows = [
        row
        for row in comparisons
        if row.get("source_mode") == source_mode and row.get("regime") in PRESSURE_REGIMES
    ]
    sufficient_rows = [row for row in rows if row.get("evidence_status") == "SUFFICIENT"]
    sufficient_regimes = {_text(row.get("regime")) for row in sufficient_rows}
    if sufficient_regimes != set(required_regimes):
        return "INSUFFICIENT_DATA"
    better = [row for row in sufficient_rows if row.get("conclusion") == "variant_a_better"]
    worse = [row for row in sufficient_rows if row.get("conclusion") == "variant_b_better"]
    if len(better) == len(sufficient_rows):
        return "PROVEN_DEFENSIVE"
    if len(worse) == len(sufficient_rows):
        return "FAILS_DEFENSIVE_EXPECTATION"
    return "PARTIALLY_DEFENSIVE"


def _pairwise_conclusion(
    sample_count: int,
    return_delta: float | None,
    drawdown_delta: float | None,
    win_rate: float | None,
    *,
    sufficient: bool,
    policy: Mapping[str, Any],
) -> str:
    if sample_count <= 0 or not sufficient:
        return "insufficient_data"
    assert return_delta is not None and drawdown_delta is not None and win_rate is not None
    if (
        return_delta >= float(policy["minimum_relative_return"])
        and drawdown_delta >= float(policy["minimum_drawdown_delta_vs_no_trade"])
        and win_rate >= float(policy["minimum_win_rate_vs_no_trade"])
    ):
        return "variant_a_better"
    if return_delta < float(policy["minimum_relative_return"]) and drawdown_delta < float(
        policy["minimum_drawdown_delta_vs_no_trade"]
    ):
        return "variant_b_better"
    return "mixed"


def _latest_pressure_payload(pressure_tag_dir: Path) -> dict[str, Any]:
    pressure_dir = _artifact_dir_from_latest(
        output_dir=pressure_tag_dir,
        artifact_id=None,
        pointer_name="latest_pressure_regime_tag",
    )
    return {
        **_read_json(pressure_dir / "pressure_regime_manifest.json"),
        "regime_window_tags": _read_jsonl(pressure_dir / "regime_window_tags.jsonl"),
        "outcome_regime_tags": _read_jsonl(pressure_dir / "outcome_regime_tags.jsonl"),
        "pressure_regime_summary": _read_json(pressure_dir / "pressure_regime_summary.json"),
    }


def _latest_defensive_relevant_outcomes(pressure_tag_dir: Path) -> int:
    try:
        payload = _latest_pressure_payload(pressure_tag_dir)
    except Exception:  # noqa: BLE001
        return 0
    return _int(
        _mapping(payload.get("pressure_regime_summary")).get(
            "defensive_validation_relevant_outcomes"
        )
    )


def _optional_latest_dir(output_dir: Path, pointer_name: str) -> Path | None:
    try:
        return _artifact_dir_from_latest(
            output_dir=output_dir,
            artifact_id=None,
            pointer_name=pointer_name,
        )
    except Exception:  # noqa: BLE001
        return None


def _latest_jsonl_count(output_dir: Path, pointer_name: str, file_name: str) -> int:
    source_dir = _optional_latest_dir(output_dir, pointer_name)
    if source_dir is None:
        return 0
    return len(_read_jsonl(source_dir / file_name))


def _latest_backtest_sim_counts(output_dir: Path) -> tuple[int, int]:
    source_dir = _optional_latest_dir(output_dir, "latest_backtest_sim_outcome")
    if source_dir is None:
        return 0, 0
    rows = _read_jsonl(source_dir / "simulated_outcome_windows.jsonl")
    pressure = sum(1 for row in rows if _text(row.get("regime_label")) in PRESSURE_VALIDATION_TAGS)
    return len(rows), pressure


def _normalize_variant_name(value: str) -> str:
    if value == "candidate_consensus_target":
        return "consensus_target"
    return value


def _read_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    loaded = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(loaded, Mapping):
        return {}
    return dict(loaded)


def _resolve_project_path(path: Path) -> Path:
    if path.is_absolute():
        return path
    return PROJECT_ROOT / path


def _records_to_texts(value: Any) -> list[str]:
    if isinstance(value, str | bytes) or not isinstance(value, Sequence):
        return []
    return [_text(item) for item in value if _text(item)]


def _avg(values: Sequence[float]) -> float:
    clean = [value for value in values if value == value]
    return sum(clean) / len(clean) if clean else 0.0


def _rounded_optional_avg(values: Sequence[Any]) -> float | None:
    clean = [float(value) for value in values if _finite_number(value)]
    return round(sum(clean) / len(clean), 6) if clean else None


def _percentile(values: Sequence[float], percentile: float) -> float:
    if not values:
        return 0.0
    clipped = min(max(percentile, 0.0), 1.0)
    index = min(len(values) - 1, int(round((len(values) - 1) * clipped)))
    return values[index]


def _read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8") if path.exists() else ""


def _artifact_safety() -> dict[str, Any]:
    return {
        "broker_action_allowed": False,
        "broker_action_taken": False,
        "policy_change_allowed": False,
        "auto_apply": False,
        "auto_policy_apply": False,
        "production_effect": "none",
        "production_candidate_generated": False,
        "manual_review_required": True,
        "owner_approval_required": True,
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }
