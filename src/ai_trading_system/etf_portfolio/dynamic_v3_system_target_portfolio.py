from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from datetime import date, datetime
from pathlib import Path
from typing import Any

import pandas as pd
import yaml

from ai_trading_system.config import DEFAULT_DATA_QUALITY_CONFIG_PATH
from ai_trading_system.etf_portfolio import dynamic_v3_system_target as legacy
from ai_trading_system.etf_portfolio.dynamic_v3_confirmation_operations import (
    _file_bytes_match,
    _json_bytes,
    _jsonl_bytes,
    _operations_datetime,
    _operations_file_commitment,
    _operations_generated_at,
    _operations_source_bundle,
    _report_input_snapshot,
    _validate_operations_source_bundle,
)
from ai_trading_system.etf_portfolio.dynamic_v3_historical_replay import (
    _artifact_dir_from_latest,
    _check,
    _mapping,
    _read_json,
    _read_jsonl,
    _read_optional_json,
    _records,
    _stable_id,
    _text,
    _unique_dir,
    _update_latest_pointer,
)
from ai_trading_system.etf_portfolio.dynamic_v3_historical_replay import (
    _validation_payload as _historical_validation_payload,
)
from ai_trading_system.etf_portfolio.dynamic_v3_pressure_validation import (
    _write_views_atomic,
)

MODEL_TARGET_SNAPSHOT_SCHEMA = "model_target_input_snapshot.v2"
PAPER_SHADOW_SNAPSHOT_SCHEMA = "paper_shadow_account_input_snapshot.v2"
MODEL_REBALANCE_SNAPSHOT_SCHEMA = "model_rebalance_input_snapshot.v2"
PAPER_PERFORMANCE_SNAPSHOT_SCHEMA = "paper_shadow_performance_input_snapshot.v2"
SYSTEM_TARGET_REVIEW_SNAPSHOT_SCHEMA = "system_target_review_input_snapshot.v2"

DEFAULT_MODEL_TARGET_CONFIG_PATH = legacy.DEFAULT_MODEL_TARGET_CONFIG_PATH
DEFAULT_PAPER_SHADOW_CONFIG_PATH = legacy.DEFAULT_PAPER_SHADOW_CONFIG_PATH
DEFAULT_MODEL_TARGET_DIR = legacy.DEFAULT_MODEL_TARGET_DIR
DEFAULT_PAPER_SHADOW_DIR = legacy.DEFAULT_PAPER_SHADOW_DIR
DEFAULT_MODEL_REBALANCE_DIR = legacy.DEFAULT_MODEL_REBALANCE_DIR
DEFAULT_PAPER_SHADOW_PERFORMANCE_DIR = legacy.DEFAULT_PAPER_SHADOW_PERFORMANCE_DIR
DEFAULT_SYSTEM_TARGET_REVIEW_DIR = legacy.DEFAULT_SYSTEM_TARGET_REVIEW_DIR
DEFAULT_PRICE_CACHE_PATH = legacy.DEFAULT_PRICE_CACHE_PATH
DEFAULT_RATES_CACHE_PATH = legacy.DEFAULT_RATES_CACHE_PATH
DEFAULT_POSITION_ADVISORY_DAILY_DIR = legacy.DEFAULT_POSITION_ADVISORY_DAILY_DIR
DEFAULT_SHADOW_MONITOR_RUN_DIR = legacy.DEFAULT_SHADOW_MONITOR_RUN_DIR
DEFAULT_SHADOW_SHORTLIST_DIR = legacy.DEFAULT_SHADOW_SHORTLIST_DIR
DEFAULT_CONSENSUS_DRIFT_DIR = legacy.DEFAULT_CONSENSUS_DRIFT_DIR
TARGET_METHODS = legacy.TARGET_METHODS
SYSTEM_TARGET_SAFETY = legacy.SYSTEM_TARGET_SAFETY


class DynamicV3SystemTargetPortfolioError(ValueError):
    """Raised when system-target or paper-shadow evidence is not reproducible."""


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise DynamicV3SystemTargetPortfolioError(message)


def _generated_at(value: datetime | None) -> datetime:
    try:
        return _operations_generated_at(value)
    except Exception as exc:  # noqa: BLE001
        raise DynamicV3SystemTargetPortfolioError(str(exc)) from exc


def _datetime(value: Any, *, field: str) -> datetime:
    try:
        return _operations_datetime(value, field=field)
    except Exception as exc:  # noqa: BLE001
        raise DynamicV3SystemTargetPortfolioError(str(exc)) from exc


def _date(value: Any, *, field: str) -> date:
    try:
        return date.fromisoformat(_text(value))
    except ValueError as exc:
        raise DynamicV3SystemTargetPortfolioError(f"{field} must be ISO date") from exc


def _finite(value: Any) -> bool:
    return (
        isinstance(value, int | float)
        and not isinstance(value, bool)
        and math.isfinite(float(value))
    )


def _optional_float(value: Any) -> float | None:
    return round(float(value), 10) if _finite(value) else None


def _text_bytes(value: str) -> bytes:
    return (value.rstrip() + "\n").encode("utf-8")


def _read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _json_safe(value: Any) -> Any:
    if isinstance(value, datetime | date):
        return value.isoformat()
    if isinstance(value, Mapping):
        return {_text(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, Sequence) and not isinstance(value, str | bytes):
        return [_json_safe(item) for item in value]
    return value


def _yaml(path: Path) -> dict[str, Any]:
    payload = yaml.safe_load(_read_text(path))
    _require(isinstance(payload, Mapping), f"YAML mapping required: {path}")
    return _mapping(_json_safe(payload))


def _validation_payload(
    report_type: str,
    artifact_id: str,
    checks: Sequence[Mapping[str, Any]],
    *,
    artifact_id_key: str | None = None,
    extra: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    if artifact_id_key is None:
        artifact_id_key = {
            "etf_dynamic_v3_model_target_config_validation": "config_id",
            "etf_dynamic_v3_model_target_validation": "target_id",
            "etf_dynamic_v3_paper_shadow_validation": "paper_shadow_id",
            "etf_dynamic_v3_model_rebalance_validation": "rebalance_id",
            "etf_dynamic_v3_paper_shadow_performance_validation": "performance_id",
            "etf_dynamic_v3_system_target_review_validation": "review_id",
        }.get(report_type, "artifact_id")
    payload = _historical_validation_payload(
        report_type=report_type,
        artifact_id_key=artifact_id_key,
        artifact_id=artifact_id,
        checks=checks,
    )
    payload.update(dict(extra or {}))
    return payload


def _config_binding(path: Path, *, kind: str) -> dict[str, Any]:
    payload = _yaml(path)
    return {
        "kind": kind,
        "bundle": _operations_source_bundle(
            source_dir=path.parent,
            canonical_files=[path.name],
            text_views=[path.name],
        ),
        "payload": payload,
    }


def _validate_config_binding(binding: Mapping[str, Any]) -> list[str]:
    errors = _validate_operations_source_bundle(_mapping(binding.get("bundle")))
    try:
        bundle = _mapping(binding.get("bundle"))
        source_dir = Path(_text(bundle.get("source_dir")))
        names = list(_mapping(bundle.get("files")))
        _require(len(names) == 1, "config bundle must contain exactly one file")
        if _yaml(source_dir / names[0]) != _mapping(binding.get("payload")):
            errors.append(f"config parsed payload drift: {source_dir / names[0]}")
    except Exception as exc:  # noqa: BLE001
        errors.append(str(exc))
    return errors


def _policy_metadata(payload: Mapping[str, Any], *, name: str) -> dict[str, Any]:
    metadata = _mapping(payload.get("policy_metadata"))
    for field in ("policy_id", "owner", "version", "status", "rationale", "review_condition"):
        _require(bool(_text(metadata.get(field))), f"{name} policy_metadata.{field} required")
    _require(
        _text(metadata.get("status")) in {"pilot_baseline", "approved", "active"},
        f"{name} policy status invalid",
    )
    return metadata


def _weights(value: Any, *, field: str) -> dict[str, float]:
    payload = _mapping(value)
    _require(bool(payload), f"{field} weights required")
    normalized: dict[str, float] = {}
    for symbol, raw in payload.items():
        _require(_finite(raw) and float(raw) >= 0.0, f"{field}.{symbol} must be finite/nonnegative")
        normalized[_text(symbol).upper()] = float(raw)
    _require(abs(sum(normalized.values()) - 1.0) <= 1e-8, f"{field} weights must sum to one")
    return legacy._normalize_weights(normalized)


def _validate_model_config_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    _require(payload.get("schema_version") == 1, "model target schema_version must be 1")
    model = _mapping(payload.get("model_target"))
    _require(model.get("mode") == "research_target_only", "model target mode invalid")
    _require(model.get("not_official_target_weights") is True, "official target guard missing")
    _require(model.get("paper_shadow_only") is True, "paper shadow guard missing")
    metadata = _policy_metadata(payload, name="model target")
    enabled = [_text(item) for item in _mapping(payload.get("target_methods")).get("enabled", [])]
    _require(
        bool(enabled) and len(enabled) == len(set(enabled)), "target methods missing/duplicate"
    )
    _require(not (set(enabled) - set(TARGET_METHODS)), "unknown target method")
    _require(
        {
            "static_baseline",
            "no_trade_baseline",
            "consensus_target",
            "limited_adjustment",
            "defensive_limited_adjustment",
        }.issubset(enabled),
        "required target methods missing",
    )
    baseline = _weights(_mapping(payload.get("baseline")).get("static_weights"), field="baseline")
    constraints = _mapping(payload.get("constraints"))
    for field in (
        "max_single_symbol_weight",
        "max_semiconductor_weight",
        "min_cash_weight",
        "max_total_risk_asset_weight",
    ):
        value = constraints.get(field)
        _require(_finite(value) and 0.0 <= float(value) <= 1.0, f"constraint {field} invalid")
    _require(bool(constraints.get("semiconductor_symbols")), "semiconductor symbols required")
    _require(bool(constraints.get("defensive_symbols")), "defensive symbols required")
    defensive = _mapping(_mapping(payload.get("method_policy")).get("defensive_limited_adjustment"))
    for field in ("semiconductor_reduction", "growth_reduction", "max_cash_weight"):
        value = defensive.get(field)
        _require(_finite(value) and float(value) >= 0.0, f"defensive policy {field} invalid")
    review = _mapping(_mapping(payload.get("method_policy")).get("review_policy"))
    preferred = [_text(item) for item in review.get("preferred_method_order", [])]
    _require(bool(preferred) and len(preferred) == len(set(preferred)), "review preference invalid")
    safety = _mapping(payload.get("safety"))
    _require(legacy._safety_config_locked(safety), "model target safety fields invalid")
    return {
        "policy_id": metadata["policy_id"],
        "policy_version": _text(metadata["version"]),
        "enabled_methods": enabled,
        "baseline": baseline,
        "constraints": constraints,
        "review_preference": preferred,
    }


def load_model_target_config(path: Path = DEFAULT_MODEL_TARGET_CONFIG_PATH) -> dict[str, Any]:
    payload = _yaml(path)
    _validate_model_config_payload(payload)
    return payload


def validate_model_target_config(path: Path = DEFAULT_MODEL_TARGET_CONFIG_PATH) -> dict[str, Any]:
    checks: list[dict[str, Any]] = []
    try:
        payload = load_model_target_config(path)
        summary = _validate_model_config_payload(payload)
        checks.extend(
            [
                _check(
                    "reviewed_policy", True, f"{summary['policy_id']}@{summary['policy_version']}"
                ),
                _check("methods_unique_and_governed", True, ",".join(summary["enabled_methods"])),
                _check("weights_and_constraints_valid", True, "source-exact"),
                _check("safety_locked", True, "research/paper-shadow only"),
            ]
        )
    except Exception as exc:  # noqa: BLE001
        checks.append(_check("config_valid", False, str(exc)))
    return _validation_payload(
        "etf_dynamic_v3_model_target_config_validation",
        "model_target_config",
        checks,
        extra={"config_path": str(path)},
    )


def _daily_payload(source_dir: Path) -> dict[str, Any]:
    manifest = _read_json(source_dir / "daily_advisory_manifest.json")
    actions = _read_json(source_dir / "daily_advisory_actions.json")
    candidates = _read_jsonl(source_dir / "daily_candidate_targets.jsonl")
    frame = pd.read_csv(source_dir / "daily_consensus_weights.csv")
    _require(
        {"symbol", "mean_target_weight"}.issubset(frame.columns), "consensus CSV schema invalid"
    )
    consensus = {
        _text(row["symbol"]).upper(): float(row["mean_target_weight"])
        for _, row in frame.iterrows()
    }
    source_id = _text(manifest.get("daily_advisory_id"))
    _require(source_id == source_dir.name, "daily advisory identity mismatch")
    _require(actions.get("daily_advisory_id") == source_id, "daily advisory actions mismatch")
    _require(
        manifest.get("status") in {"PASS", "PASS_WITH_WARNINGS"}, "daily advisory status invalid"
    )
    source_generated = _datetime(manifest.get("generated_at"), field="daily advisory generated_at")
    source_as_of = _date(manifest.get("as_of"), field="daily advisory as_of")
    candidate_ids = [_text(row.get("candidate_id")) for row in candidates]
    _require(bool(candidate_ids) and all(candidate_ids), "candidate targets required")
    _require(len(candidate_ids) == len(set(candidate_ids)), "candidate ids duplicate")
    normalized_candidates = []
    for row in candidates:
        normalized = dict(row)
        normalized["target_weights"] = _weights(
            row.get("target_weights"), field=f"candidate {row.get('candidate_id')}"
        )
        _require(_finite(row.get("shortlist_rank")), "candidate shortlist_rank invalid")
        _require(_finite(row.get("shortlist_score")), "candidate shortlist_score invalid")
        normalized_candidates.append(normalized)
    consensus = _weights(consensus, field="daily consensus")
    return {
        "source_id": source_id,
        "generated_at": source_generated.isoformat(),
        "as_of": source_as_of.isoformat(),
        "candidates": normalized_candidates,
        "consensus_weights": consensus,
        "actions": actions,
        "manifest": manifest,
    }


def _select_daily_source(
    root: Path, *, target_date: date, generated: datetime
) -> tuple[Path, dict[str, Any]]:
    candidates: list[tuple[date, datetime, Path, dict[str, Any]]] = []
    if root.exists():
        for child in sorted(path for path in root.iterdir() if path.is_dir()):
            if not (child / "daily_advisory_manifest.json").is_file():
                continue
            manifest = _read_json(child / "daily_advisory_manifest.json")
            try:
                source_date = _date(manifest.get("as_of"), field="daily advisory as_of")
                source_generated = _datetime(
                    manifest.get("generated_at"), field="daily advisory generated_at"
                )
            except DynamicV3SystemTargetPortfolioError:
                continue
            if source_date <= target_date and source_generated <= generated:
                candidates.append((source_date, source_generated, child, manifest))
    _require(bool(candidates), "no daily advisory source at or before cutoff")
    latest_date = max(item[0] for item in candidates)
    same_date = [item for item in candidates if item[0] == latest_date]
    _require(len(same_date) == 1, f"ambiguous daily advisory as_of: {latest_date}")
    selected = same_date[0][2]
    payload = _daily_payload(selected)
    _require(_date(payload["as_of"], field="daily as_of") <= target_date, "daily source future")
    _require(
        _datetime(payload["generated_at"], field="daily generated_at") <= generated,
        "daily generated after cutoff",
    )
    return selected, payload


def _daily_binding(source_dir: Path, payload: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "kind": "daily_advisory",
        "artifact_id": payload["source_id"],
        "validation": {
            "status": "PASS",
            "artifact_id": payload["source_id"],
            "generated_at": payload["generated_at"],
            "as_of": payload["as_of"],
            "candidate_count": len(_records(payload.get("candidates"))),
        },
        "bundle": _operations_source_bundle(
            source_dir=source_dir,
            json_views=["daily_advisory_manifest.json", "daily_advisory_actions.json"],
            jsonl_views=["daily_candidate_targets.jsonl"],
            text_views=["daily_consensus_weights.csv"],
        ),
        "calculation": {
            "candidates": _records(payload.get("candidates")),
            "consensus_weights": _mapping(payload.get("consensus_weights")),
        },
    }


def _validate_daily_binding(binding: Mapping[str, Any]) -> list[str]:
    errors = _validate_operations_source_bundle(_mapping(binding.get("bundle")))
    try:
        source_dir = Path(_text(_mapping(binding.get("bundle")).get("source_dir")))
        payload = _daily_payload(source_dir)
        expected = _mapping(binding.get("validation"))
        actual = {
            "status": "PASS",
            "artifact_id": payload["source_id"],
            "generated_at": payload["generated_at"],
            "as_of": payload["as_of"],
            "candidate_count": len(_records(payload.get("candidates"))),
        }
        if actual != expected:
            errors.append("daily advisory validation drift")
        if _records(payload.get("candidates")) != _records(
            _mapping(binding.get("calculation")).get("candidates")
        ):
            errors.append("daily advisory candidate view drift")
        if _mapping(payload.get("consensus_weights")) != _mapping(
            _mapping(binding.get("calculation")).get("consensus_weights")
        ):
            errors.append("daily advisory consensus view drift")
    except Exception as exc:  # noqa: BLE001
        errors.append(str(exc))
    return errors


def _linked_model_bindings(config: Mapping[str, Any]) -> list[dict[str, Any]]:
    source = _mapping(config.get("source"))
    advisory = Path(_text(source.get("position_advisory_config")))
    if not advisory.is_absolute():
        advisory = legacy.PROJECT_ROOT / advisory
    _require(advisory.is_file(), "position advisory policy missing")
    advisory_binding = _config_binding(advisory, kind="position_advisory_policy")
    limits = _mapping(_mapping(advisory_binding.get("payload")).get("advisory_limits"))
    for field in ("max_single_day_total_adjustment", "max_single_symbol_adjustment"):
        _require(
            _finite(limits.get(field)) and float(limits[field]) > 0.0,
            f"advisory limit {field} invalid",
        )
    bindings = [advisory_binding]
    enabled = set(_mapping(config.get("target_methods")).get("enabled", []))
    if "risk_capped_limited_adjustment" in enabled:
        raw = (
            source.get("risk_capped_limited_config")
            or legacy.DEFAULT_RISK_CAPPED_LIMITED_CONFIG_PATH
        )
        path = Path(str(raw))
        if not path.is_absolute():
            path = legacy.PROJECT_ROOT / path
        bindings.append(_config_binding(path, kind="risk_capped_policy"))
    if enabled & {"smooth_weights_3d_limited_adjustment", "smooth_weights_5d_limited_adjustment"}:
        raw = source.get("smoothed_limited_config") or legacy.DEFAULT_SMOOTHED_LIMITED_CONFIG_PATH
        path = Path(str(raw))
        if not path.is_absolute():
            path = legacy.PROJECT_ROOT / path
        bindings.append(_config_binding(path, kind="smoothed_policy"))
    return bindings


def _binding_payload(bindings: Sequence[Mapping[str, Any]], kind: str) -> dict[str, Any]:
    for binding in bindings:
        if binding.get("kind") == kind:
            return _mapping(binding.get("payload"))
    return {}


def _model_target_views(
    snapshot: Mapping[str, Any], *, target_id: str, output_dir: Path
) -> tuple[dict[str, bytes], dict[str, Any]]:
    config = _mapping(_mapping(snapshot.get("config_binding")).get("payload"))
    config_summary = _validate_model_config_payload(config)
    source = _mapping(snapshot.get("daily_source"))
    calculation = _mapping(source.get("calculation"))
    candidates = _records(calculation.get("candidates"))
    consensus = _weights(calculation.get("consensus_weights"), field="snapshot consensus")
    baseline = _mapping(config_summary.get("baseline"))
    linked = _records(snapshot.get("linked_policy_bindings"))
    limits = _mapping(_binding_payload(linked, "position_advisory_policy").get("advisory_limits"))
    limited = legacy._limited_adjustment(
        baseline=baseline,
        target=consensus,
        max_total_adjustment=float(limits["max_single_day_total_adjustment"]),
        max_symbol_adjustment=float(limits["max_single_symbol_adjustment"]),
    )
    defensive = legacy._defensive_adjustment(
        limited,
        _mapping(_mapping(config.get("method_policy")).get("defensive_limited_adjustment")),
    )
    target_date = _date(snapshot.get("as_of"), field="model target as_of")
    risk = legacy._risk_capped_limited_weights_for_model_target(
        base_weights=limited,
        previous_weights=baseline,
        risk_config=_binding_payload(linked, "risk_capped_policy"),
        model_config=config,
        as_of=target_date,
        regime_context="normal",
    )
    smooth_config = _binding_payload(linked, "smoothed_policy")
    smooth_3d = legacy._smoothed_limited_weights_for_model_target(
        base_weights=limited,
        previous_weights=baseline,
        smoothed_config=smooth_config,
        model_config=config,
        as_of=target_date,
        variant_id="smooth_weights_3d",
        regime_context="normal",
    )
    smooth_5d = legacy._smoothed_limited_weights_for_model_target(
        base_weights=limited,
        previous_weights=baseline,
        smoothed_config=smooth_config,
        model_config=config,
        as_of=target_date,
        variant_id="smooth_weights_5d",
        regime_context="normal",
    )
    top_candidate = _weights(legacy._first_candidate_weights(candidates), field="top candidate")
    equal_weight = _weights(
        legacy._average_candidate_weights(candidates), field="candidate average"
    )
    method_weights = {
        "static_baseline": baseline,
        "no_trade_baseline": baseline,
        "consensus_target": consensus,
        "limited_adjustment": limited,
        "smooth_weights_3d_limited_adjustment": smooth_3d,
        "smooth_weights_5d_limited_adjustment": smooth_5d,
        "risk_capped_limited_adjustment": risk,
        "defensive_limited_adjustment": defensive,
        "equal_weight_shadow_candidates": equal_weight,
        "selected_top_candidate": top_candidate,
    }
    enabled = list(config_summary["enabled_methods"])
    rows = [
        {
            "target_id": target_id,
            "as_of": target_date.isoformat(),
            "target_method": method,
            "weights": method_weights[method],
            "source_candidates": [_text(row.get("candidate_id")) for row in candidates],
            "source_daily_advisory_id": source["artifact_id"],
            **SYSTEM_TARGET_SAFETY,
        }
        for method in enabled
    ]
    checks_payload = legacy._constraint_checks(
        target_id=target_id,
        rows=rows,
        constraints=_mapping(config_summary.get("constraints")),
    )
    observation_method = next(
        (method for method in config_summary["review_preference"] if method in enabled), enabled[0]
    )
    manifest = {
        "schema_version": 2,
        "report_type": "etf_dynamic_v3_model_target_manifest",
        "target_id": target_id,
        "as_of": target_date.isoformat(),
        "generated_at": snapshot["generated_at"],
        "status": checks_payload["overall_status"],
        "config_path": _text(
            _mapping(_mapping(snapshot.get("config_binding")).get("bundle")).get("source_dir")
        )
        + "/"
        + next(
            iter(
                _mapping(
                    _mapping(_mapping(snapshot.get("config_binding")).get("bundle")).get("files")
                )
            )
        ),
        "generated_methods": enabled,
        "recommended_research_method": observation_method,
        "selection_basis": "REVIEWED_OBSERVATION_PRIORITY",
        "performance_winner_claimed": False,
        "source_daily_advisory_id": source["artifact_id"],
        "source_summary": {
            "source_daily_advisory_id": source["artifact_id"],
            "candidate_count": len(candidates),
            "consensus_status": _mapping(_mapping(source.get("validation"))).get("status"),
        },
        "warnings": [],
        "method_policy": _mapping(config.get("method_policy")),
        "input_snapshot_path": str(output_dir / "model_target_input_snapshot.json"),
        "model_target_manifest_path": str(output_dir / "model_target_manifest.json"),
        "model_target_weights_path": str(output_dir / "model_target_weights.json"),
        "method_target_weights_path": str(output_dir / "method_target_weights.jsonl"),
        "target_constraint_checks_path": str(output_dir / "target_constraint_checks.json"),
        "model_target_report_path": str(output_dir / "model_target_report.md"),
        **SYSTEM_TARGET_SAFETY,
    }
    selected_weights = method_weights[observation_method]
    weights_payload = {
        "schema_version": 2,
        "target_id": target_id,
        "as_of": target_date.isoformat(),
        "recommended_research_method": observation_method,
        "selection_basis": "REVIEWED_OBSERVATION_PRIORITY",
        "weights": selected_weights,
        "method_count": len(rows),
        "method_weights": {row["target_method"]: row["weights"] for row in rows},
        **SYSTEM_TARGET_SAFETY,
    }
    views = {
        "model_target_input_snapshot.json": _json_bytes(snapshot),
        "model_target_manifest.json": _json_bytes(manifest),
        "model_target_weights.json": _json_bytes(weights_payload),
        "method_target_weights.jsonl": _jsonl_bytes(rows),
        "target_constraint_checks.json": _json_bytes(checks_payload),
        "model_target_report.md": _text_bytes(
            legacy.render_model_target_report(manifest, rows, checks_payload)
        ),
    }
    return views, {
        "manifest": manifest,
        "model_target_weights": weights_payload,
        "method_target_weights": rows,
        "target_constraint_checks": checks_payload,
    }


def generate_model_target(
    *,
    config_path: Path = DEFAULT_MODEL_TARGET_CONFIG_PATH,
    as_of: date | None = None,
    output_dir: Path = DEFAULT_MODEL_TARGET_DIR,
    position_advisory_daily_dir: Path = DEFAULT_POSITION_ADVISORY_DAILY_DIR,
    shadow_monitor_dir: Path = DEFAULT_SHADOW_MONITOR_RUN_DIR,
    shadow_shortlist_dir: Path = DEFAULT_SHADOW_SHORTLIST_DIR,
    consensus_drift_dir: Path = DEFAULT_CONSENSUS_DRIFT_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = _generated_at(generated_at)
    target_date = as_of or generated.date()
    _require(target_date <= generated.date(), "model target as_of cannot be future")
    config_binding = _config_binding(config_path, kind="model_target_policy")
    config = _mapping(config_binding.get("payload"))
    _validate_model_config_payload(config)
    linked = _linked_model_bindings(config)
    source_dir, source_payload = _select_daily_source(
        position_advisory_daily_dir, target_date=target_date, generated=generated
    )
    daily_binding = _daily_binding(source_dir, source_payload)
    snapshot = {
        "schema_version": MODEL_TARGET_SNAPSHOT_SCHEMA,
        "generated_at": generated.isoformat(),
        "as_of": target_date.isoformat(),
        "config_binding": config_binding,
        "linked_policy_bindings": linked,
        "daily_source": daily_binding,
        "source_selection": {
            "role": "daily_advisory",
            "root": str(position_advisory_daily_dir),
            "artifact_id": source_payload["source_id"],
            "cutoff_as_of": target_date.isoformat(),
            "cutoff_generated_at": generated.isoformat(),
            "selection_rule": "unique latest semantic as_of at or before cutoff",
        },
        "supplemental_roots_not_used_for_calculation": {
            "shadow_monitor_dir": str(shadow_monitor_dir),
            "shadow_shortlist_dir": str(shadow_shortlist_dir),
            "consensus_drift_dir": str(consensus_drift_dir),
        },
        "fallback_weight_generation_allowed": False,
        "production_effect": "none",
    }
    target_id = _stable_id(
        "model-target",
        target_date.isoformat(),
        generated.isoformat(),
        source_payload["source_id"],
        _operations_file_commitment(config_path)["sha256"],
    )
    target_dir = _unique_dir(output_dir / target_id)
    views, payload = _model_target_views(snapshot, target_id=target_dir.name, output_dir=target_dir)
    _write_views_atomic(target_dir, views)
    _update_latest_pointer(
        "latest_model_target", target_dir.name, target_dir / "model_target_manifest.json"
    )
    return {"target_id": target_dir.name, "target_dir": target_dir, **payload}


def model_target_report_payload(
    *,
    target_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_MODEL_TARGET_DIR,
) -> dict[str, Any]:
    root = _artifact_dir_from_latest(
        output_dir=output_dir,
        artifact_id=target_id if not latest else None,
        pointer_name="latest_model_target",
    )
    return {
        **_read_json(root / "model_target_manifest.json"),
        "model_target_weights": _read_json(root / "model_target_weights.json"),
        "method_target_weights": _read_jsonl(root / "method_target_weights.jsonl"),
        "target_constraint_checks": _read_json(root / "target_constraint_checks.json"),
        "target_dir": str(root),
        **_report_input_snapshot(root / "model_target_input_snapshot.json"),
    }


def _validate_model_snapshot(snapshot: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    if snapshot.get("schema_version") != MODEL_TARGET_SNAPSHOT_SCHEMA:
        errors.append("model target snapshot schema invalid")
    errors.extend(_validate_config_binding(_mapping(snapshot.get("config_binding"))))
    for binding in _records(snapshot.get("linked_policy_bindings")):
        errors.extend(_validate_config_binding(binding))
    errors.extend(_validate_daily_binding(_mapping(snapshot.get("daily_source"))))
    try:
        selection = _mapping(snapshot.get("source_selection"))
        selected, payload = _select_daily_source(
            Path(_text(selection.get("root"))),
            target_date=_date(selection.get("cutoff_as_of"), field="source cutoff as_of"),
            generated=_datetime(
                selection.get("cutoff_generated_at"), field="source cutoff generated_at"
            ),
        )
        if selected.name != selection.get("artifact_id") or payload["source_id"] != selection.get(
            "artifact_id"
        ):
            errors.append("daily advisory semantic selection drift")
    except Exception as exc:  # noqa: BLE001
        errors.append(str(exc))
    return errors


def validate_model_target_artifact(
    *, target_id: str, output_dir: Path = DEFAULT_MODEL_TARGET_DIR
) -> dict[str, Any]:
    root = output_dir / target_id
    snapshot = _read_optional_json(root / "model_target_input_snapshot.json") or {}
    errors = _validate_model_snapshot(snapshot)
    mismatches: list[str] = []
    try:
        views, _ = _model_target_views(snapshot, target_id=target_id, output_dir=root)
        mismatches = [
            name for name, payload in views.items() if not _file_bytes_match(root / name, payload)
        ]
    except Exception as exc:  # noqa: BLE001
        errors.append(str(exc))
    checks = [
        _check("snapshot_and_live_inputs", not errors, "; ".join(errors)),
        _check("content_derived_views", not mismatches, ",".join(mismatches)),
        _check(
            "research_safety",
            _mapping(_read_optional_json(root / "model_target_manifest.json") or {}).get(
                "production_effect"
            )
            == "none",
            "production_effect=none",
        ),
    ]
    return _validation_payload("etf_dynamic_v3_model_target_validation", target_id, checks)


def _binding_path(binding: Mapping[str, Any]) -> Path:
    bundle = _mapping(binding.get("bundle"))
    names = list(_mapping(bundle.get("files")))
    _require(len(names) == 1, "single-file binding expected")
    return Path(_text(bundle.get("source_dir"))) / names[0]


def _artifact_binding(
    *,
    kind: str,
    artifact_dir: Path,
    artifact_id: str,
    validation: Mapping[str, Any],
    json_views: Sequence[str] = (),
    jsonl_views: Sequence[str] = (),
) -> dict[str, Any]:
    _require(validation.get("status") == "PASS", f"{kind} validation failed")
    return {
        "kind": kind,
        "artifact_id": artifact_id,
        "validation": dict(validation),
        "bundle": _operations_source_bundle(
            source_dir=artifact_dir,
            json_views=json_views,
            jsonl_views=jsonl_views,
        ),
    }


def _validate_artifact_binding(binding: Mapping[str, Any]) -> list[str]:
    errors = _validate_operations_source_bundle(_mapping(binding.get("bundle")))
    try:
        kind = _text(binding.get("kind"))
        artifact_id = _text(binding.get("artifact_id"))
        source_dir = Path(_text(_mapping(binding.get("bundle")).get("source_dir")))
        validators = {
            "model_target": lambda: validate_model_target_artifact(
                target_id=artifact_id, output_dir=source_dir.parent
            ),
            "paper_shadow": lambda: validate_paper_shadow_artifact(
                paper_shadow_id=artifact_id, output_dir=source_dir.parent
            ),
            "model_rebalance": lambda: validate_model_rebalance_artifact(
                rebalance_id=artifact_id, output_dir=source_dir.parent
            ),
            "paper_shadow_performance": lambda: validate_paper_shadow_performance_artifact(
                performance_id=artifact_id, output_dir=source_dir.parent
            ),
        }
        _require(kind in validators, f"unknown source binding kind: {kind}")
        actual = validators[kind]()
        if actual != _mapping(binding.get("validation")):
            errors.append(f"{kind} source validation drift")
    except Exception as exc:  # noqa: BLE001
        errors.append(str(exc))
    return errors


def _select_model_target(root: Path, *, generated: datetime) -> tuple[Path, dict[str, Any]]:
    candidates: list[tuple[date, datetime, Path, dict[str, Any]]] = []
    if root.exists():
        for child in sorted(path for path in root.iterdir() if path.is_dir()):
            manifest_path = child / "model_target_manifest.json"
            if not manifest_path.is_file():
                continue
            manifest = _read_json(manifest_path)
            try:
                as_of = _date(manifest.get("as_of"), field="model target as_of")
                source_generated = _datetime(
                    manifest.get("generated_at"), field="model target generated_at"
                )
            except DynamicV3SystemTargetPortfolioError:
                continue
            if as_of <= generated.date() and source_generated <= generated:
                candidates.append((as_of, source_generated, child, manifest))
    _require(bool(candidates), "no model target at or before cutoff")
    latest_as_of = max(item[0] for item in candidates)
    same_date = [item for item in candidates if item[0] == latest_as_of]
    _require(len(same_date) == 1, f"ambiguous model target as_of: {latest_as_of}")
    selected = same_date[0]
    validation = validate_model_target_artifact(target_id=selected[2].name, output_dir=root)
    _require(validation.get("status") == "PASS", "selected model target validation failed")
    return selected[2], selected[3]


def _validate_paper_config_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    _require(payload.get("schema_version") == 1, "paper shadow schema_version must be 1")
    account = _mapping(payload.get("paper_shadow_account"))
    _require(account.get("mode") == "paper_shadow_only", "paper shadow mode invalid")
    _require(_text(account.get("base_currency")) == "USD", "paper shadow base currency invalid")
    initial_equity = account.get("initial_equity")
    _require(_finite(initial_equity) and float(initial_equity) > 0.0, "initial equity invalid")
    start_date = _date(account.get("start_date"), field="paper shadow start_date")
    _require(start_date >= legacy.AI_AFTER_CHATGPT_START, "paper shadow start before AI regime")
    metadata = _policy_metadata(payload, name="paper shadow")
    methods = [_text(item) for item in _mapping(payload.get("tracking")).get("target_methods", [])]
    _require(
        bool(methods) and len(methods) == len(set(methods)), "tracked methods missing/duplicate"
    )
    _require(not (set(methods) - set(TARGET_METHODS)), "unknown tracked method")
    initial_method = _text(account.get("initial_method"))
    _require(initial_method in methods, "paper initial method must be tracked")
    baseline = _weights(
        _mapping(payload.get("baseline")).get("static_weights"), field="paper baseline"
    )
    costs = _mapping(payload.get("costs"))
    for field in ("transaction_cost_bps", "slippage_bps"):
        value = costs.get(field)
        _require(_finite(value) and float(value) >= 0.0, f"paper cost {field} invalid")
    performance = _mapping(payload.get("performance_policy"))
    for field in ("minimum_common_observations", "minimum_regime_observations"):
        value = performance.get(field)
        _require(
            isinstance(value, int) and not isinstance(value, bool) and value > 0, f"{field} invalid"
        )
    threshold = performance.get("pressure_daily_return_threshold")
    _require(_finite(threshold) and float(threshold) < 0.0, "pressure threshold invalid")
    _require(legacy._safety_config_locked(_mapping(payload.get("safety"))), "paper safety invalid")
    return {
        "policy_id": metadata["policy_id"],
        "policy_version": _text(metadata["version"]),
        "start_date": start_date.isoformat(),
        "initial_equity": float(initial_equity),
        "tracked_methods": methods,
        "initial_method": initial_method,
        "baseline": baseline,
        "costs": costs,
        "performance_policy": performance,
    }


def load_paper_shadow_config(path: Path = DEFAULT_PAPER_SHADOW_CONFIG_PATH) -> dict[str, Any]:
    payload = _yaml(path)
    _validate_paper_config_payload(payload)
    return payload


def _paper_shadow_views(
    snapshot: Mapping[str, Any], *, paper_shadow_id: str, output_dir: Path
) -> tuple[dict[str, bytes], dict[str, Any]]:
    config_binding = _mapping(snapshot.get("config_binding"))
    config = _mapping(config_binding.get("payload"))
    summary = _validate_paper_config_payload(config)
    target_binding = _mapping(snapshot.get("model_target_source"))
    target_bundle = _mapping(target_binding.get("bundle"))
    target_rows = _records(_mapping(target_bundle.get("jsonl")).get("method_target_weights.jsonl"))
    available_methods = {_text(row.get("target_method")) for row in target_rows}
    _require(
        set(summary["tracked_methods"]).issubset(available_methods),
        "tracked method absent from model target",
    )
    method_rows = [
        {
            "paper_shadow_id": paper_shadow_id,
            "target_method": method,
            "as_of": summary["start_date"],
            "portfolio_value": summary["initial_equity"],
            "weights": summary["baseline"],
            "cash_weight": legacy._float(summary["baseline"].get("CASH")),
            "turnover_to_date": 0.0,
            "last_rebalance_date": None,
            "broker_action_taken": False,
            "production_effect": "none",
            **SYSTEM_TARGET_SAFETY,
        }
        for method in summary["tracked_methods"]
    ]
    state = {
        "schema_version": 2,
        "report_type": "etf_dynamic_v3_paper_shadow_state",
        "paper_shadow_id": paper_shadow_id,
        "state_status": "INITIALIZED",
        "as_of": summary["start_date"],
        "base_currency": "USD",
        "initial_equity": summary["initial_equity"],
        "tracked_methods": summary["tracked_methods"],
        "method_states": method_rows,
        "source_model_target_id": target_binding["artifact_id"],
        "source_model_target_used_for_initial_weights": False,
        "initial_weights_basis": "REVIEWED_STATIC_BASELINE_AT_AI_REGIME_START",
        "config_path": str(_binding_path(config_binding)),
        **SYSTEM_TARGET_SAFETY,
    }
    manifest = {
        "schema_version": 2,
        "report_type": "etf_dynamic_v3_paper_shadow_manifest",
        "paper_shadow_id": paper_shadow_id,
        "generated_at": snapshot["generated_at"],
        "status": "PASS",
        "config_path": str(_binding_path(config_binding)),
        "tracked_methods": summary["tracked_methods"],
        "source_model_target_id": target_binding["artifact_id"],
        "input_snapshot_path": str(output_dir / "paper_shadow_account_input_snapshot.json"),
        "paper_shadow_manifest_path": str(output_dir / "paper_shadow_manifest.json"),
        "paper_shadow_state_path": str(output_dir / "paper_shadow_state.json"),
        "paper_shadow_method_states_path": str(output_dir / "paper_shadow_method_states.jsonl"),
        "paper_shadow_report_path": str(output_dir / "paper_shadow_report.md"),
        **SYSTEM_TARGET_SAFETY,
    }
    views = {
        "paper_shadow_account_input_snapshot.json": _json_bytes(snapshot),
        "paper_shadow_manifest.json": _json_bytes(manifest),
        "paper_shadow_state.json": _json_bytes(state),
        "paper_shadow_method_states.jsonl": _jsonl_bytes(method_rows),
        "paper_shadow_report.md": _text_bytes(legacy.render_paper_shadow_report(manifest, state)),
    }
    return views, {"manifest": manifest, "state": state, "method_states": method_rows}


def init_paper_shadow_account(
    *,
    config_path: Path = DEFAULT_PAPER_SHADOW_CONFIG_PATH,
    output_dir: Path = DEFAULT_PAPER_SHADOW_DIR,
    model_target_dir: Path = DEFAULT_MODEL_TARGET_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = _generated_at(generated_at)
    config_binding = _config_binding(config_path, kind="paper_shadow_policy")
    config = _mapping(config_binding.get("payload"))
    _validate_paper_config_payload(config)
    target_dir, target_manifest = _select_model_target(model_target_dir, generated=generated)
    target_validation = validate_model_target_artifact(
        target_id=target_dir.name, output_dir=model_target_dir
    )
    target_binding = _artifact_binding(
        kind="model_target",
        artifact_dir=target_dir,
        artifact_id=target_dir.name,
        validation=target_validation,
        json_views=["model_target_manifest.json", "model_target_weights.json"],
        jsonl_views=["method_target_weights.jsonl"],
    )
    snapshot = {
        "schema_version": PAPER_SHADOW_SNAPSHOT_SCHEMA,
        "generated_at": generated.isoformat(),
        "config_binding": config_binding,
        "model_target_source": target_binding,
        "source_selection": {
            "role": "model_target",
            "root": str(model_target_dir),
            "artifact_id": target_dir.name,
            "cutoff_generated_at": generated.isoformat(),
            "selected_as_of": target_manifest["as_of"],
            "selection_rule": "unique latest semantic as_of at or before cutoff",
        },
        "initial_weights_use_future_target": False,
        "production_effect": "none",
    }
    paper_shadow_id = _stable_id(
        "paper-shadow",
        generated.isoformat(),
        target_dir.name,
        _operations_file_commitment(config_path)["sha256"],
    )
    root = _unique_dir(output_dir / paper_shadow_id)
    views, payload = _paper_shadow_views(snapshot, paper_shadow_id=root.name, output_dir=root)
    _write_views_atomic(root, views)
    _update_latest_pointer("latest_paper_shadow", root.name, root / "paper_shadow_manifest.json")
    return {"paper_shadow_id": root.name, "paper_shadow_dir": root, **payload}


def paper_shadow_state_payload(
    *,
    paper_shadow_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_PAPER_SHADOW_DIR,
) -> dict[str, Any]:
    root = _artifact_dir_from_latest(
        output_dir=output_dir,
        artifact_id=paper_shadow_id if not latest else None,
        pointer_name="latest_paper_shadow",
    )
    return {
        **_read_json(root / "paper_shadow_state.json"),
        "paper_shadow_dir": str(root),
        **_report_input_snapshot(root / "paper_shadow_account_input_snapshot.json"),
    }


def paper_shadow_report_payload(
    *,
    paper_shadow_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_PAPER_SHADOW_DIR,
) -> dict[str, Any]:
    root = _artifact_dir_from_latest(
        output_dir=output_dir,
        artifact_id=paper_shadow_id if not latest else None,
        pointer_name="latest_paper_shadow",
    )
    return {
        **_read_json(root / "paper_shadow_manifest.json"),
        "paper_shadow_state": _read_json(root / "paper_shadow_state.json"),
        "paper_shadow_method_states": _read_jsonl(root / "paper_shadow_method_states.jsonl"),
        "paper_shadow_dir": str(root),
        **_report_input_snapshot(root / "paper_shadow_account_input_snapshot.json"),
    }


def _validate_paper_snapshot(snapshot: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    if snapshot.get("schema_version") != PAPER_SHADOW_SNAPSHOT_SCHEMA:
        errors.append("paper shadow snapshot schema invalid")
    errors.extend(_validate_config_binding(_mapping(snapshot.get("config_binding"))))
    errors.extend(_validate_artifact_binding(_mapping(snapshot.get("model_target_source"))))
    try:
        selection = _mapping(snapshot.get("source_selection"))
        selected, _ = _select_model_target(
            Path(_text(selection.get("root"))),
            generated=_datetime(
                selection.get("cutoff_generated_at"), field="model target selection cutoff"
            ),
        )
        if selected.name != selection.get("artifact_id"):
            errors.append("model target semantic selection drift")
    except Exception as exc:  # noqa: BLE001
        errors.append(str(exc))
    return errors


def validate_paper_shadow_artifact(
    *, paper_shadow_id: str, output_dir: Path = DEFAULT_PAPER_SHADOW_DIR
) -> dict[str, Any]:
    root = output_dir / paper_shadow_id
    snapshot = _read_optional_json(root / "paper_shadow_account_input_snapshot.json") or {}
    errors = _validate_paper_snapshot(snapshot)
    mismatches: list[str] = []
    try:
        views, _ = _paper_shadow_views(snapshot, paper_shadow_id=paper_shadow_id, output_dir=root)
        mismatches = [
            name for name, payload in views.items() if not _file_bytes_match(root / name, payload)
        ]
    except Exception as exc:  # noqa: BLE001
        errors.append(str(exc))
    checks = [
        _check("snapshot_and_live_inputs", not errors, "; ".join(errors)),
        _check("content_derived_views", not mismatches, ",".join(mismatches)),
        _check(
            "paper_shadow_safety",
            (_read_optional_json(root / "paper_shadow_manifest.json") or {}).get(
                "production_effect"
            )
            == "none",
            "production_effect=none",
        ),
    ]
    return _validation_payload("etf_dynamic_v3_paper_shadow_validation", paper_shadow_id, checks)


def _existing_rebalance_for_pair(root: Path, *, paper_shadow_id: str, target_id: str) -> str | None:
    if not root.exists():
        return None
    matches: list[str] = []
    for child in sorted(path for path in root.iterdir() if path.is_dir()):
        manifest = _read_optional_json(child / "model_rebalance_manifest.json") or {}
        if (
            manifest.get("paper_shadow_id") == paper_shadow_id
            and manifest.get("target_id") == target_id
        ):
            matches.append(child.name)
    _require(len(matches) <= 1, "duplicate existing Paper+Target rebalance artifacts")
    return matches[0] if matches else None


def _model_rebalance_views(
    snapshot: Mapping[str, Any], *, rebalance_id: str, output_dir: Path
) -> tuple[dict[str, bytes], dict[str, Any]]:
    paper_binding = _mapping(snapshot.get("paper_shadow_source"))
    target_binding = _mapping(snapshot.get("model_target_source"))
    paper_bundle = _mapping(paper_binding.get("bundle"))
    target_bundle = _mapping(target_binding.get("bundle"))
    state = dict(_mapping(_mapping(paper_bundle.get("json")).get("paper_shadow_state.json")))
    target_manifest = _mapping(
        _mapping(target_bundle.get("json")).get("model_target_manifest.json")
    )
    target_rows = _records(_mapping(target_bundle.get("jsonl")).get("method_target_weights.jsonl"))
    constraint_payload = _mapping(
        _mapping(target_bundle.get("json")).get("target_constraint_checks.json")
    )
    method_targets: dict[str, dict[str, Any]] = {}
    for row in target_rows:
        method = _text(row.get("target_method"))
        _require(bool(method) and method not in method_targets, "target method duplicate")
        method_targets[method] = row
    constraint_rows: dict[str, dict[str, Any]] = {}
    for row in _records(constraint_payload.get("checks")):
        method = _text(row.get("target_method"))
        _require(bool(method) and method not in constraint_rows, "constraint method duplicate")
        constraint_rows[method] = row
    current_rows: dict[str, dict[str, Any]] = {}
    for row in _records(state.get("method_states")):
        method = _text(row.get("target_method"))
        _require(bool(method) and method not in current_rows, "paper state method duplicate")
        current_rows[method] = row
    tracked = [_text(item) for item in state.get("tracked_methods", [])]
    _require(bool(tracked) and len(tracked) == len(set(tracked)), "tracked methods invalid")
    target_date = _date(target_manifest.get("as_of"), field="target as_of")
    paper_date = _date(state.get("as_of"), field="paper state as_of")
    _require(target_date >= paper_date, "model target would roll paper state backward")
    events: list[dict[str, Any]] = []
    history: list[dict[str, Any]] = []
    next_rows: list[dict[str, Any]] = []
    for method in tracked:
        _require(method in current_rows, f"paper method state missing: {method}")
        before = _weights(current_rows[method].get("weights"), field=f"paper {method}")
        target_row = method_targets.get(method)
        constraint = constraint_rows.get(method)
        _require(constraint is not None, f"target constraint missing: {method}")
        if target_row is None:
            status = "INSUFFICIENT_DATA"
            target_weights: dict[str, float] = {}
            after = before
            turnover = 0.0
        elif constraint.get("status") == "FAIL":
            status = "SKIPPED"
            target_weights = _weights(target_row.get("weights"), field=f"target {method}")
            after = before
            turnover = 0.0
        else:
            status = "APPLIED_TO_PAPER"
            target_weights = _weights(target_row.get("weights"), field=f"target {method}")
            after = target_weights
            turnover = legacy._turnover(before, target_weights)
        row = dict(current_rows[method])
        row.update(
            {
                "paper_shadow_id": paper_binding["artifact_id"],
                "target_method": method,
                "as_of": target_date.isoformat(),
                "weights": after,
                "cash_weight": legacy._float(after.get("CASH")),
                "turnover_to_date": round(
                    legacy._float(row.get("turnover_to_date")) + turnover, 10
                ),
                "last_rebalance_date": (
                    target_date.isoformat()
                    if status == "APPLIED_TO_PAPER"
                    else row.get("last_rebalance_date")
                ),
                "source_rebalance_id": rebalance_id,
                **SYSTEM_TARGET_SAFETY,
            }
        )
        next_rows.append(row)
        events.append(
            {
                "rebalance_id": rebalance_id,
                "paper_shadow_id": paper_binding["artifact_id"],
                "target_id": target_binding["artifact_id"],
                "date": target_date.isoformat(),
                "target_method": method,
                "before_weights": before,
                "target_weights": target_weights,
                "after_weights": after,
                "deltas": legacy._weight_deltas(before, target_weights or before),
                "turnover": round(turnover, 10),
                "rebalance_status": status,
                **SYSTEM_TARGET_SAFETY,
            }
        )
        history.append(
            {
                "date": target_date.isoformat(),
                "target_method": method,
                "weights": after,
                "portfolio_value": legacy._float(row.get("portfolio_value")),
                "daily_return": None,
                "drawdown": None,
                "turnover": round(turnover, 10),
                "observation_status": "TARGET_TRANSITION_ONLY",
                **SYSTEM_TARGET_SAFETY,
            }
        )
    post_state = dict(state)
    post_state.update(
        {
            "schema_version": 2,
            "state_status": "REBALANCED_TO_MODEL_TARGET",
            "as_of": target_date.isoformat(),
            "method_states": next_rows,
            "source_model_target_id": target_binding["artifact_id"],
            "source_rebalance_id": rebalance_id,
            "materialized_in_rebalance_artifact": True,
            **SYSTEM_TARGET_SAFETY,
        }
    )
    summary = {
        "schema_version": 2,
        "rebalance_id": rebalance_id,
        "paper_shadow_id": paper_binding["artifact_id"],
        "target_id": target_binding["artifact_id"],
        "state_effective_as_of": target_date.isoformat(),
        "total_turnover": round(sum(float(event["turnover"]) for event in events), 10),
        "applied_methods": [
            event["target_method"]
            for event in events
            if event["rebalance_status"] == "APPLIED_TO_PAPER"
        ],
        "skipped_methods": [
            event["target_method"] for event in events if event["rebalance_status"] == "SKIPPED"
        ],
        "insufficient_data_methods": [
            event["target_method"]
            for event in events
            if event["rebalance_status"] == "INSUFFICIENT_DATA"
        ],
        **SYSTEM_TARGET_SAFETY,
    }
    status = "PASS" if not summary["insufficient_data_methods"] else "PASS_WITH_WARNINGS"
    manifest = {
        "schema_version": 2,
        "report_type": "etf_dynamic_v3_model_rebalance_manifest",
        "rebalance_id": rebalance_id,
        "paper_shadow_id": paper_binding["artifact_id"],
        "target_id": target_binding["artifact_id"],
        "generated_at": snapshot["generated_at"],
        "state_effective_as_of": target_date.isoformat(),
        "status": status,
        "paper_state_mutation_mode": "APPEND_ONLY_REBALANCE_POST_STATE",
        "input_snapshot_path": str(output_dir / "model_rebalance_input_snapshot.json"),
        "paper_shadow_state_after_path": str(output_dir / "paper_shadow_state_after.json"),
        "model_rebalance_manifest_path": str(output_dir / "model_rebalance_manifest.json"),
        "rebalance_events_path": str(output_dir / "rebalance_events.jsonl"),
        "method_weight_history_path": str(output_dir / "method_weight_history.jsonl"),
        "rebalance_turnover_summary_path": str(output_dir / "rebalance_turnover_summary.json"),
        "model_rebalance_report_path": str(output_dir / "model_rebalance_report.md"),
        **SYSTEM_TARGET_SAFETY,
    }
    views = {
        "model_rebalance_input_snapshot.json": _json_bytes(snapshot),
        "model_rebalance_manifest.json": _json_bytes(manifest),
        "paper_shadow_state_after.json": _json_bytes(post_state),
        "rebalance_events.jsonl": _jsonl_bytes(events),
        "method_weight_history.jsonl": _jsonl_bytes(history),
        "rebalance_turnover_summary.json": _json_bytes(summary),
        "model_rebalance_report.md": _text_bytes(
            legacy.render_model_rebalance_report(manifest, summary, events)
        ),
    }
    return views, {
        "manifest": manifest,
        "paper_shadow_state_after": post_state,
        "rebalance_events": events,
        "method_weight_history": history,
        "rebalance_turnover_summary": summary,
    }


def simulate_model_rebalance(
    *,
    paper_shadow_id: str,
    target_id: str,
    paper_shadow_dir: Path = DEFAULT_PAPER_SHADOW_DIR,
    model_target_dir: Path = DEFAULT_MODEL_TARGET_DIR,
    output_dir: Path = DEFAULT_MODEL_REBALANCE_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = _generated_at(generated_at)
    _require(
        _existing_rebalance_for_pair(
            output_dir, paper_shadow_id=paper_shadow_id, target_id=target_id
        )
        is None,
        "Paper+Target rebalance already exists",
    )
    paper_root = paper_shadow_dir / paper_shadow_id
    target_root = model_target_dir / target_id
    paper_validation = validate_paper_shadow_artifact(
        paper_shadow_id=paper_shadow_id, output_dir=paper_shadow_dir
    )
    target_validation = validate_model_target_artifact(
        target_id=target_id, output_dir=model_target_dir
    )
    paper_binding = _artifact_binding(
        kind="paper_shadow",
        artifact_dir=paper_root,
        artifact_id=paper_shadow_id,
        validation=paper_validation,
        json_views=["paper_shadow_manifest.json", "paper_shadow_state.json"],
        jsonl_views=["paper_shadow_method_states.jsonl"],
    )
    target_binding = _artifact_binding(
        kind="model_target",
        artifact_dir=target_root,
        artifact_id=target_id,
        validation=target_validation,
        json_views=[
            "model_target_manifest.json",
            "model_target_weights.json",
            "target_constraint_checks.json",
        ],
        jsonl_views=["method_target_weights.jsonl"],
    )
    target_generated = _datetime(
        _mapping(_mapping(target_binding["bundle"])["json"])["model_target_manifest.json"][
            "generated_at"
        ],
        field="model target generated_at",
    )
    paper_generated = _datetime(
        _mapping(_mapping(paper_binding["bundle"])["json"])["paper_shadow_manifest.json"][
            "generated_at"
        ],
        field="paper shadow generated_at",
    )
    _require(max(target_generated, paper_generated) <= generated, "rebalance source after cutoff")
    snapshot = {
        "schema_version": MODEL_REBALANCE_SNAPSHOT_SCHEMA,
        "generated_at": generated.isoformat(),
        "paper_shadow_source": paper_binding,
        "model_target_source": target_binding,
        "duplicate_pair_checked": True,
        "paper_state_transition": "FORWARD_ONLY_APPEND_ONLY_POST_STATE",
        "production_effect": "none",
    }
    rebalance_id = _stable_id("model-rebalance", paper_shadow_id, target_id, generated.isoformat())
    root = _unique_dir(output_dir / rebalance_id)
    views, payload = _model_rebalance_views(snapshot, rebalance_id=root.name, output_dir=root)
    _write_views_atomic(root, views)
    _update_latest_pointer(
        "latest_model_rebalance", root.name, root / "model_rebalance_manifest.json"
    )
    return {"rebalance_id": root.name, "rebalance_dir": root, **payload}


def model_rebalance_report_payload(
    *,
    rebalance_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_MODEL_REBALANCE_DIR,
) -> dict[str, Any]:
    root = _artifact_dir_from_latest(
        output_dir=output_dir,
        artifact_id=rebalance_id if not latest else None,
        pointer_name="latest_model_rebalance",
    )
    return {
        **_read_json(root / "model_rebalance_manifest.json"),
        "paper_shadow_state_after": _read_json(root / "paper_shadow_state_after.json"),
        "rebalance_events": _read_jsonl(root / "rebalance_events.jsonl"),
        "method_weight_history": _read_jsonl(root / "method_weight_history.jsonl"),
        "rebalance_turnover_summary": _read_json(root / "rebalance_turnover_summary.json"),
        "rebalance_dir": str(root),
        **_report_input_snapshot(root / "model_rebalance_input_snapshot.json"),
    }


def _validate_rebalance_snapshot(snapshot: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    if snapshot.get("schema_version") != MODEL_REBALANCE_SNAPSHOT_SCHEMA:
        errors.append("model rebalance snapshot schema invalid")
    errors.extend(_validate_artifact_binding(_mapping(snapshot.get("paper_shadow_source"))))
    errors.extend(_validate_artifact_binding(_mapping(snapshot.get("model_target_source"))))
    return errors


def validate_model_rebalance_artifact(
    *, rebalance_id: str, output_dir: Path = DEFAULT_MODEL_REBALANCE_DIR
) -> dict[str, Any]:
    root = output_dir / rebalance_id
    snapshot = _read_optional_json(root / "model_rebalance_input_snapshot.json") or {}
    errors = _validate_rebalance_snapshot(snapshot)
    mismatches: list[str] = []
    try:
        views, _ = _model_rebalance_views(snapshot, rebalance_id=rebalance_id, output_dir=root)
        mismatches = [
            name for name, payload in views.items() if not _file_bytes_match(root / name, payload)
        ]
    except Exception as exc:  # noqa: BLE001
        errors.append(str(exc))
    checks = [
        _check("snapshot_and_live_inputs", not errors, "; ".join(errors)),
        _check("content_derived_views", not mismatches, ",".join(mismatches)),
        _check(
            "append_only_post_state",
            (_read_optional_json(root / "model_rebalance_manifest.json") or {}).get(
                "paper_state_mutation_mode"
            )
            == "APPEND_ONLY_REBALANCE_POST_STATE",
            "no source state overwrite",
        ),
    ]
    return _validation_payload("etf_dynamic_v3_model_rebalance_validation", rebalance_id, checks)


def _select_rebalance_for_paper(
    root: Path,
    *,
    paper_shadow_id: str,
    evaluation_as_of: date,
    generated: datetime,
) -> tuple[Path, dict[str, Any]] | None:
    candidates: list[tuple[date, datetime, Path, dict[str, Any]]] = []
    if root.exists():
        for child in sorted(path for path in root.iterdir() if path.is_dir()):
            manifest = _read_optional_json(child / "model_rebalance_manifest.json") or {}
            if manifest.get("paper_shadow_id") != paper_shadow_id:
                continue
            try:
                effective = _date(
                    manifest.get("state_effective_as_of"), field="rebalance effective as_of"
                )
                source_generated = _datetime(
                    manifest.get("generated_at"), field="rebalance generated_at"
                )
            except DynamicV3SystemTargetPortfolioError:
                continue
            if effective <= evaluation_as_of and source_generated <= generated:
                candidates.append((effective, source_generated, child, manifest))
    if not candidates:
        return None
    latest_date = max(item[0] for item in candidates)
    same_date = [item for item in candidates if item[0] == latest_date]
    _require(len(same_date) == 1, f"ambiguous rebalance effective date: {latest_date}")
    selected = same_date[0]
    validation = validate_model_rebalance_artifact(rebalance_id=selected[2].name, output_dir=root)
    _require(validation.get("status") == "PASS", "selected rebalance validation failed")
    return selected[2], selected[3]


def _cache_binding(path: Path, *, kind: str, required: bool = True) -> dict[str, Any]:
    if not path.is_file():
        _require(not required, f"required cache input missing: {path}")
        return {"kind": kind, "required": False, "path": str(path), "commitment": None}
    return {
        "kind": kind,
        "required": required,
        "path": str(path),
        "commitment": _operations_file_commitment(path),
    }


def _validate_cache_binding(binding: Mapping[str, Any]) -> list[str]:
    expected = binding.get("commitment")
    if expected is None:
        return (
            []
            if not binding.get("required")
            else [f"required cache binding absent: {binding.get('path')}"]
        )
    try:
        actual = _operations_file_commitment(Path(_text(binding.get("path"))))
    except Exception as exc:  # noqa: BLE001
        return [str(exc)]
    return [] if actual == expected else [f"cache commitment drift: {binding.get('path')}"]


def _market_price_rows(
    path: Path, *, symbols: Sequence[str], start: date, end: date
) -> list[dict[str, Any]]:
    frame = pd.read_csv(path)
    _require({"date", "ticker", "adj_close"}.issubset(frame.columns), "price cache schema invalid")
    frame = frame.loc[frame["ticker"].astype(str).isin(symbols)].copy()
    frame["date"] = pd.to_datetime(frame["date"], errors="coerce")
    frame["adj_close"] = pd.to_numeric(frame["adj_close"], errors="coerce")
    frame = frame.loc[frame["date"].notna() & frame["adj_close"].notna()]
    frame = frame.loc[(frame["date"].dt.date >= start) & (frame["date"].dt.date <= end)]
    _require(not frame.duplicated(["date", "ticker"]).any(), "price cache duplicate date/ticker")
    rows = [
        {
            "date": row.date.date().isoformat(),
            "ticker": _text(row.ticker),
            "adj_close": float(row.adj_close),
        }
        for row in frame.sort_values(["date", "ticker"]).itertuples(index=False)
    ]
    _require(
        all(_finite(row["adj_close"]) and row["adj_close"] > 0.0 for row in rows),
        "price values invalid",
    )
    return rows


def _returns_from_rows(
    rows: Sequence[Mapping[str, Any]], *, symbols: Sequence[str]
) -> pd.DataFrame:
    frame = pd.DataFrame(rows)
    if frame.empty:
        return pd.DataFrame()
    frame["date"] = pd.to_datetime(frame["date"], errors="raise")
    pivot = frame.pivot(index="date", columns="ticker", values="adj_close").sort_index()
    missing_symbols = set(symbols) - set(pivot.columns)
    if missing_symbols:
        return pd.DataFrame()
    common = pivot[list(symbols)].dropna(how="any")
    return common.pct_change(fill_method=None).dropna(how="any")


def _performance_row(
    *,
    method: str,
    weights: Mapping[str, Any],
    returns: pd.DataFrame,
    turnover: float,
    total_cost_bps: float,
    minimum_observations: int,
) -> dict[str, Any]:
    normalized = _weights(weights, field=f"performance {method}")
    if len(returns) < minimum_observations:
        return {
            "target_method": method,
            "total_return": None,
            "annualized_return": None,
            "max_drawdown": None,
            "realized_volatility": None,
            "turnover": round(turnover, 10),
            "estimated_transition_cost": round(turnover * total_cost_bps / 10000.0, 10),
            "relative_to_static_baseline": None,
            "relative_to_no_trade": None,
            "risk_adjusted_return_to_volatility": None,
            "observation_count": len(returns),
            "performance_status": "INSUFFICIENT_DATA",
        }
    series = pd.Series(0.0, index=returns.index)
    for symbol, weight in normalized.items():
        if symbol != "CASH":
            _require(symbol in returns.columns, f"price return missing for {symbol}")
            series = series + returns[symbol] * float(weight)
    cost = turnover * total_cost_bps / 10000.0
    equity = (1.0 - cost) * (1.0 + series).cumprod()
    total_return = float(equity.iloc[-1] - 1.0)
    periods = len(series)
    annualized = (
        float((1.0 + total_return) ** (252.0 / periods) - 1.0) if total_return > -1.0 else -1.0
    )
    volatility = float(series.std(ddof=0) * math.sqrt(252.0)) if periods > 1 else 0.0
    drawdown = equity / equity.cummax() - 1.0
    max_drawdown = float(drawdown.min())
    risk_adjusted = annualized / volatility if volatility > 0.0 else None
    return {
        "target_method": method,
        "total_return": round(total_return, 10),
        "annualized_return": round(annualized, 10),
        "max_drawdown": round(max_drawdown, 10),
        "realized_volatility": round(volatility, 10),
        "turnover": round(turnover, 10),
        "estimated_transition_cost": round(cost, 10),
        "relative_to_static_baseline": None,
        "relative_to_no_trade": None,
        "risk_adjusted_return_to_volatility": _optional_float(risk_adjusted),
        "observation_count": periods,
        "performance_status": "PASS",
    }


def _metric_by_method(rows: Sequence[Mapping[str, Any]], method: str, field: str) -> float | None:
    for row in rows:
        if row.get("target_method") == method:
            return _optional_float(row.get(field))
    return None


def _best_available(rows: Sequence[Mapping[str, Any]], field: str, *, high: bool) -> str:
    available = [
        row for row in rows if _finite(row.get(field)) and row.get("performance_status") == "PASS"
    ]
    if not available:
        return "INSUFFICIENT_DATA"
    selected = (max if high else min)(available, key=lambda row: float(row[field]))
    return _text(selected.get("target_method"))


def _performance_comparisons(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    comparisons: list[dict[str, Any]] = []
    for index, left in enumerate(rows):
        for right in rows[index + 1 :]:
            if not all(
                _finite(row.get(field))
                for row in (left, right)
                for field in ("total_return", "max_drawdown", "turnover")
            ):
                return_delta = drawdown_delta = turnover_delta = None
                conclusion = "insufficient_data"
            else:
                return_delta = round(float(left["total_return"]) - float(right["total_return"]), 10)
                drawdown_delta = round(
                    float(left["max_drawdown"]) - float(right["max_drawdown"]), 10
                )
                turnover_delta = round(float(left["turnover"]) - float(right["turnover"]), 10)
                if return_delta > 0.0 and drawdown_delta >= 0.0:
                    conclusion = "method_a_better"
                elif return_delta < 0.0 and drawdown_delta <= 0.0:
                    conclusion = "method_b_better"
                else:
                    conclusion = "mixed"
            comparisons.append(
                {
                    "method_a": left.get("target_method"),
                    "method_b": right.get("target_method"),
                    "return_delta": return_delta,
                    "drawdown_delta": drawdown_delta,
                    "turnover_delta": turnover_delta,
                    "conclusion": conclusion,
                }
            )
    return {"schema_version": 2, "comparisons": comparisons, **SYSTEM_TARGET_SAFETY}


def _regime_performance(
    *,
    state_rows: Sequence[Mapping[str, Any]],
    returns: pd.DataFrame,
    turnover_by_method: Mapping[str, float],
    threshold: float,
    minimum_observations: int,
) -> dict[str, Any]:
    if returns.empty:
        return {"schema_version": 2, "regimes": [], **SYSTEM_TARGET_SAFETY}
    labels = pd.Series("normal", index=returns.index)
    if "QQQ" in returns.columns:
        labels.loc[returns["QQQ"] <= threshold] = "tech_drawdown"
    if "SMH" in returns.columns:
        labels.loc[returns["SMH"] <= threshold] = "semiconductor_pressure"
    regimes: list[dict[str, Any]] = []
    for regime in ("tech_drawdown", "semiconductor_pressure", "normal"):
        selected = returns.loc[labels == regime]
        methods = [
            _performance_row(
                method=_text(row.get("target_method")),
                weights=_mapping(row.get("weights")),
                returns=selected,
                turnover=0.0,
                total_cost_bps=0.0,
                minimum_observations=minimum_observations,
            )
            for row in state_rows
        ]
        no_trade = _metric_by_method(methods, "no_trade_baseline", "total_return")
        for item in methods:
            value = _optional_float(item.get("total_return"))
            item["relative_to_no_trade"] = (
                round(value - no_trade, 10) if value is not None and no_trade is not None else None
            )
        regimes.append({"regime": regime, "observation_count": len(selected), "methods": methods})
    return {"schema_version": 2, "regimes": regimes, **SYSTEM_TARGET_SAFETY}


def _dq_report(snapshot: Mapping[str, Any]) -> str:
    dq = _mapping(snapshot.get("data_quality"))
    return "\n".join(
        [
            "# Paper Shadow Performance Data Quality",
            "",
            f"- status: {dq.get('status')}",
            f"- checked_at: {dq.get('checked_at')}",
            f"- as_of: {dq.get('as_of')}",
            f"- error_count: {dq.get('error_count')}",
            f"- warning_count: {dq.get('warning_count')}",
            "- gate_enforced: true",
            "- production_effect: none",
            "",
        ]
    )


def _paper_performance_views(
    snapshot: Mapping[str, Any], *, performance_id: str, output_dir: Path
) -> tuple[dict[str, bytes], dict[str, Any]]:
    paper_binding = _mapping(snapshot.get("paper_shadow_source"))
    paper_bundle = _mapping(paper_binding.get("bundle"))
    state_binding = _mapping(snapshot.get("rebalance_source"))
    if state_binding:
        state = _mapping(
            _mapping(_mapping(state_binding.get("bundle")).get("json")).get(
                "paper_shadow_state_after.json"
            )
        )
        events = _records(
            _mapping(_mapping(state_binding.get("bundle")).get("jsonl")).get(
                "rebalance_events.jsonl"
            )
        )
        source_target_id = _text(state.get("source_model_target_id"))
        source_rebalance_id = _text(state_binding.get("artifact_id"))
    else:
        state = _mapping(_mapping(paper_bundle.get("json")).get("paper_shadow_state.json"))
        events = []
        source_target_id = _text(state.get("source_model_target_id"))
        source_rebalance_id = ""
    config = _mapping(_mapping(snapshot.get("config_binding")).get("payload"))
    config_summary = _validate_paper_config_payload(config)
    policy = _mapping(config_summary.get("performance_policy"))
    state_rows = _records(state.get("method_states"))
    methods = [_text(row.get("target_method")) for row in state_rows]
    _require(
        bool(methods) and len(methods) == len(set(methods)), "performance state methods invalid"
    )
    symbols = sorted(
        {
            symbol
            for row in state_rows
            for symbol, weight in _mapping(row.get("weights")).items()
            if symbol != "CASH" and _finite(weight) and float(weight) > 0.0
        }
    )
    returns = _returns_from_rows(_records(snapshot.get("price_rows")), symbols=symbols)
    turnover_by_method = {
        _text(event.get("target_method")): float(event.get("turnover"))
        for event in events
        if _finite(event.get("turnover"))
    }
    total_cost_bps = float(config_summary["costs"]["transaction_cost_bps"]) + float(
        config_summary["costs"]["slippage_bps"]
    )
    rows = [
        _performance_row(
            method=_text(row.get("target_method")),
            weights=_mapping(row.get("weights")),
            returns=returns,
            turnover=turnover_by_method.get(_text(row.get("target_method")), 0.0),
            total_cost_bps=total_cost_bps,
            minimum_observations=int(policy["minimum_common_observations"]),
        )
        for row in state_rows
    ]
    static_return = _metric_by_method(rows, "static_baseline", "total_return")
    no_trade_return = _metric_by_method(rows, "no_trade_baseline", "total_return")
    for row in rows:
        total_return = _optional_float(row.get("total_return"))
        row["relative_to_static_baseline"] = (
            round(total_return - static_return, 10)
            if total_return is not None and static_return is not None
            else None
        )
        row["relative_to_no_trade"] = (
            round(total_return - no_trade_return, 10)
            if total_return is not None and no_trade_return is not None
            else None
        )
    summary = {
        "schema_version": 2,
        "methods": rows,
        "best_return_method": _best_available(rows, "total_return", high=True),
        "best_drawdown_method": _best_available(rows, "max_drawdown", high=True),
        "best_risk_adjusted_method": _best_available(
            rows, "risk_adjusted_return_to_volatility", high=True
        ),
        "data_quality_status": _mapping(snapshot.get("data_quality")).get("status"),
        "data_quality_checked_at": _mapping(snapshot.get("data_quality")).get("checked_at"),
        "performance_start_date": snapshot["performance_start_date"],
        "evaluation_as_of": snapshot["evaluation_as_of"],
        "return_observation_count": len(returns),
        "source_model_target_id": source_target_id,
        "source_rebalance_id": source_rebalance_id,
        "common_date_policy": "ALL_POSITIVE_WEIGHT_SYMBOLS_FINITE",
        "missing_metrics_remain_null": True,
        **SYSTEM_TARGET_SAFETY,
    }
    pairwise = _performance_comparisons(rows)
    regime = _regime_performance(
        state_rows=state_rows,
        returns=returns,
        turnover_by_method=turnover_by_method,
        threshold=float(policy["pressure_daily_return_threshold"]),
        minimum_observations=int(policy["minimum_regime_observations"]),
    )
    status = (
        "PASS" if all(row["performance_status"] == "PASS" for row in rows) else "PASS_WITH_WARNINGS"
    )
    manifest = {
        "schema_version": 2,
        "report_type": "etf_dynamic_v3_paper_shadow_performance_manifest",
        "performance_id": performance_id,
        "paper_shadow_id": paper_binding["artifact_id"],
        "source_model_target_id": source_target_id,
        "source_rebalance_id": source_rebalance_id,
        "generated_at": snapshot["generated_at"],
        "status": status,
        "performance_start_date": snapshot["performance_start_date"],
        "evaluation_as_of": snapshot["evaluation_as_of"],
        "input_snapshot_path": str(output_dir / "paper_shadow_performance_input_snapshot.json"),
        "data_quality_report_path": str(output_dir / "validate_data_quality_report.md"),
        "paper_shadow_performance_manifest_path": str(
            output_dir / "paper_shadow_performance_manifest.json"
        ),
        "method_performance_summary_path": str(output_dir / "method_performance_summary.json"),
        "method_pairwise_comparison_path": str(output_dir / "method_pairwise_comparison.json"),
        "regime_performance_breakdown_path": str(output_dir / "regime_performance_breakdown.json"),
        "paper_shadow_performance_report_path": str(
            output_dir / "paper_shadow_performance_report.md"
        ),
        "reader_brief_section_path": str(output_dir / "reader_brief_section.md"),
        "data_quality_status": _mapping(snapshot.get("data_quality")).get("status"),
        "data_quality_report_visible": True,
        **SYSTEM_TARGET_SAFETY,
    }
    views = {
        "paper_shadow_performance_input_snapshot.json": _json_bytes(snapshot),
        "paper_shadow_performance_manifest.json": _json_bytes(manifest),
        "method_performance_summary.json": _json_bytes(summary),
        "method_pairwise_comparison.json": _json_bytes(pairwise),
        "regime_performance_breakdown.json": _json_bytes(regime),
        "validate_data_quality_report.md": _text_bytes(_dq_report(snapshot)),
        "paper_shadow_performance_report.md": _text_bytes(
            legacy.render_paper_shadow_performance_report(manifest, summary, pairwise, regime)
        ),
        "reader_brief_section.md": _text_bytes(legacy.render_performance_reader_brief(summary)),
    }
    return views, {
        "manifest": manifest,
        "method_performance_summary": summary,
        "method_pairwise_comparison": pairwise,
        "regime_performance_breakdown": regime,
    }


def run_paper_shadow_performance(
    *,
    paper_shadow_id: str,
    paper_shadow_dir: Path = DEFAULT_PAPER_SHADOW_DIR,
    output_dir: Path = DEFAULT_PAPER_SHADOW_PERFORMANCE_DIR,
    price_cache_path: Path = DEFAULT_PRICE_CACHE_PATH,
    rates_cache_path: Path = DEFAULT_RATES_CACHE_PATH,
    model_rebalance_dir: Path = DEFAULT_MODEL_REBALANCE_DIR,
    as_of: date | None = None,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = _generated_at(generated_at)
    evaluation_as_of = as_of or generated.date()
    _require(evaluation_as_of <= generated.date(), "performance as_of cannot be future")
    paper_root = paper_shadow_dir / paper_shadow_id
    paper_validation = validate_paper_shadow_artifact(
        paper_shadow_id=paper_shadow_id, output_dir=paper_shadow_dir
    )
    paper_binding = _artifact_binding(
        kind="paper_shadow",
        artifact_dir=paper_root,
        artifact_id=paper_shadow_id,
        validation=paper_validation,
        json_views=["paper_shadow_manifest.json", "paper_shadow_state.json"],
        jsonl_views=["paper_shadow_method_states.jsonl"],
    )
    selected_rebalance = _select_rebalance_for_paper(
        model_rebalance_dir,
        paper_shadow_id=paper_shadow_id,
        evaluation_as_of=evaluation_as_of,
        generated=generated,
    )
    rebalance_binding: dict[str, Any] = {}
    if selected_rebalance is not None:
        rebalance_root, _ = selected_rebalance
        rebalance_validation = validate_model_rebalance_artifact(
            rebalance_id=rebalance_root.name, output_dir=model_rebalance_dir
        )
        rebalance_binding = _artifact_binding(
            kind="model_rebalance",
            artifact_dir=rebalance_root,
            artifact_id=rebalance_root.name,
            validation=rebalance_validation,
            json_views=[
                "model_rebalance_manifest.json",
                "paper_shadow_state_after.json",
                "rebalance_turnover_summary.json",
            ],
            jsonl_views=["rebalance_events.jsonl"],
        )
        state = _mapping(
            _mapping(_mapping(rebalance_binding["bundle"])["json"])["paper_shadow_state_after.json"]
        )
    else:
        state = _mapping(
            _mapping(_mapping(paper_binding["bundle"])["json"])["paper_shadow_state.json"]
        )
    performance_start = _date(state.get("as_of"), field="performance state as_of")
    _require(evaluation_as_of >= performance_start, "performance as_of predates state")
    paper_manifest = _mapping(
        _mapping(_mapping(paper_binding["bundle"])["json"])["paper_shadow_manifest.json"]
    )
    config_path = Path(_text(paper_manifest.get("config_path")))
    config_binding = _config_binding(config_path, kind="paper_shadow_policy")
    config = _mapping(config_binding.get("payload"))
    _validate_paper_config_payload(config)
    state_rows = _records(state.get("method_states"))
    symbols = sorted(
        {
            symbol
            for row in state_rows
            for symbol, weight in _mapping(row.get("weights")).items()
            if symbol != "CASH" and _finite(weight) and float(weight) > 0.0
        }
    )
    _require(bool(symbols), "paper state has no priced symbols")
    quality = legacy._run_data_quality_gate(
        price_cache_path=price_cache_path,
        rates_cache_path=rates_cache_path,
        expected_symbols=symbols,
        as_of=evaluation_as_of,
    )
    _require(quality.passed, f"data quality gate failed: {quality.status}")
    cache_bindings = [
        _cache_binding(price_cache_path, kind="prices", required=True),
        _cache_binding(rates_cache_path, kind="rates", required=True),
        _cache_binding(DEFAULT_DATA_QUALITY_CONFIG_PATH, kind="data_quality_policy", required=True),
        _cache_binding(
            price_cache_path.parent / "download_manifest.csv",
            kind="download_manifest",
            required=False,
        ),
        _cache_binding(
            price_cache_path.parent / "prices_marketstack_daily.csv",
            kind="secondary_prices",
            required=False,
        ),
    ]
    price_rows = _market_price_rows(
        price_cache_path,
        symbols=symbols,
        start=performance_start,
        end=evaluation_as_of,
    )
    snapshot = {
        "schema_version": PAPER_PERFORMANCE_SNAPSHOT_SCHEMA,
        "generated_at": generated.isoformat(),
        "performance_start_date": performance_start.isoformat(),
        "evaluation_as_of": evaluation_as_of.isoformat(),
        "paper_shadow_source": paper_binding,
        "rebalance_source": rebalance_binding,
        "rebalance_selection": {
            "root": str(model_rebalance_dir),
            "paper_shadow_id": paper_shadow_id,
            "artifact_id": _text(rebalance_binding.get("artifact_id")),
            "evaluation_as_of": evaluation_as_of.isoformat(),
            "cutoff_generated_at": generated.isoformat(),
            "selection_rule": "zero-or-one unique latest effective state",
        },
        "config_binding": config_binding,
        "cache_bindings": cache_bindings,
        "expected_symbols": symbols,
        "price_rows": price_rows,
        "data_quality": {
            "status": quality.status,
            "checked_at": quality.checked_at.isoformat(),
            "as_of": quality.as_of.isoformat(),
            "error_count": quality.error_count,
            "warning_count": quality.warning_count,
            "gate_enforced": True,
        },
        "missing_metrics_remain_null": True,
        "production_effect": "none",
    }
    performance_id = _stable_id(
        "paper-shadow-performance",
        paper_shadow_id,
        _text(rebalance_binding.get("artifact_id")),
        evaluation_as_of.isoformat(),
        generated.isoformat(),
    )
    root = _unique_dir(output_dir / performance_id)
    views, payload = _paper_performance_views(snapshot, performance_id=root.name, output_dir=root)
    _write_views_atomic(root, views)
    _update_latest_pointer(
        "latest_paper_shadow_performance",
        root.name,
        root / "paper_shadow_performance_manifest.json",
    )
    return {"performance_id": root.name, "performance_dir": root, **payload}


def paper_shadow_performance_report_payload(
    *,
    performance_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_PAPER_SHADOW_PERFORMANCE_DIR,
) -> dict[str, Any]:
    root = _artifact_dir_from_latest(
        output_dir=output_dir,
        artifact_id=performance_id if not latest else None,
        pointer_name="latest_paper_shadow_performance",
    )
    return {
        **_read_json(root / "paper_shadow_performance_manifest.json"),
        "method_performance_summary": _read_json(root / "method_performance_summary.json"),
        "method_pairwise_comparison": _read_json(root / "method_pairwise_comparison.json"),
        "regime_performance_breakdown": _read_json(root / "regime_performance_breakdown.json"),
        "performance_dir": str(root),
        **_report_input_snapshot(root / "paper_shadow_performance_input_snapshot.json"),
    }


def _validate_performance_snapshot(snapshot: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    if snapshot.get("schema_version") != PAPER_PERFORMANCE_SNAPSHOT_SCHEMA:
        errors.append("paper performance snapshot schema invalid")
    errors.extend(_validate_artifact_binding(_mapping(snapshot.get("paper_shadow_source"))))
    rebalance = _mapping(snapshot.get("rebalance_source"))
    if rebalance:
        errors.extend(_validate_artifact_binding(rebalance))
    errors.extend(_validate_config_binding(_mapping(snapshot.get("config_binding"))))
    for binding in _records(snapshot.get("cache_bindings")):
        errors.extend(_validate_cache_binding(binding))
    try:
        selection = _mapping(snapshot.get("rebalance_selection"))
        selected = _select_rebalance_for_paper(
            Path(_text(selection.get("root"))),
            paper_shadow_id=_text(selection.get("paper_shadow_id")),
            evaluation_as_of=_date(
                selection.get("evaluation_as_of"), field="rebalance selection as_of"
            ),
            generated=_datetime(
                selection.get("cutoff_generated_at"), field="rebalance selection cutoff"
            ),
        )
        actual_id = selected[0].name if selected is not None else ""
        if actual_id != _text(selection.get("artifact_id")):
            errors.append("rebalance semantic selection drift")
    except Exception as exc:  # noqa: BLE001
        errors.append(str(exc))
    try:
        cache_by_kind = {
            _text(binding.get("kind")): Path(_text(binding.get("path")))
            for binding in _records(snapshot.get("cache_bindings"))
        }
        quality = legacy._run_data_quality_gate(
            price_cache_path=cache_by_kind["prices"],
            rates_cache_path=cache_by_kind["rates"],
            expected_symbols=[_text(item) for item in snapshot.get("expected_symbols", [])],
            as_of=_date(snapshot.get("evaluation_as_of"), field="performance evaluation as_of"),
        )
        expected_dq = _mapping(snapshot.get("data_quality"))
        if (
            quality.status != expected_dq.get("status")
            or quality.error_count != expected_dq.get("error_count")
            or quality.warning_count != expected_dq.get("warning_count")
        ):
            errors.append("data quality result drift")
        actual_rows = _market_price_rows(
            cache_by_kind["prices"],
            symbols=[_text(item) for item in snapshot.get("expected_symbols", [])],
            start=_date(snapshot.get("performance_start_date"), field="performance start"),
            end=_date(snapshot.get("evaluation_as_of"), field="performance evaluation as_of"),
        )
        if actual_rows != _records(snapshot.get("price_rows")):
            errors.append("price calculation rows drift")
    except Exception as exc:  # noqa: BLE001
        errors.append(str(exc))
    return errors


def validate_paper_shadow_performance_artifact(
    *,
    performance_id: str,
    output_dir: Path = DEFAULT_PAPER_SHADOW_PERFORMANCE_DIR,
) -> dict[str, Any]:
    root = output_dir / performance_id
    snapshot = _read_optional_json(root / "paper_shadow_performance_input_snapshot.json") or {}
    errors = _validate_performance_snapshot(snapshot)
    mismatches: list[str] = []
    try:
        views, _ = _paper_performance_views(
            snapshot, performance_id=performance_id, output_dir=root
        )
        mismatches = [
            name for name, payload in views.items() if not _file_bytes_match(root / name, payload)
        ]
    except Exception as exc:  # noqa: BLE001
        errors.append(str(exc))
    checks = [
        _check("snapshot_live_sources_and_dq", not errors, "; ".join(errors)),
        _check("content_derived_views", not mismatches, ",".join(mismatches)),
        _check(
            "missing_metrics_null",
            (_read_optional_json(root / "method_performance_summary.json") or {}).get(
                "missing_metrics_remain_null"
            )
            is True,
            "null-preserving",
        ),
    ]
    return _validation_payload(
        "etf_dynamic_v3_paper_shadow_performance_validation", performance_id, checks
    )


def _system_target_review_views(
    snapshot: Mapping[str, Any], *, review_id: str, output_dir: Path
) -> tuple[dict[str, bytes], dict[str, Any]]:
    target_binding = _mapping(snapshot.get("model_target_source"))
    paper_binding = _mapping(snapshot.get("paper_shadow_source"))
    performance_binding = _mapping(snapshot.get("performance_source"))
    target_json = _mapping(_mapping(target_binding.get("bundle")).get("json"))
    paper_json = _mapping(_mapping(paper_binding.get("bundle")).get("json"))
    performance_json = _mapping(_mapping(performance_binding.get("bundle")).get("json"))
    target_manifest = _mapping(target_json.get("model_target_manifest.json"))
    paper_manifest = _mapping(paper_json.get("paper_shadow_manifest.json"))
    performance_manifest = _mapping(performance_json.get("paper_shadow_performance_manifest.json"))
    summary = _mapping(performance_json.get("method_performance_summary.json"))
    target_id = _text(target_binding.get("artifact_id"))
    paper_id = _text(paper_binding.get("artifact_id"))
    performance_id = _text(performance_binding.get("artifact_id"))
    _require(
        paper_manifest.get("source_model_target_id") == target_id, "Target→Paper lineage mismatch"
    )
    _require(
        performance_manifest.get("paper_shadow_id") == paper_id,
        "Paper→Performance lineage mismatch",
    )
    _require(
        performance_manifest.get("source_model_target_id") == target_id,
        "Target→Performance lineage mismatch",
    )
    target_generated = _datetime(target_manifest.get("generated_at"), field="target generated_at")
    paper_generated = _datetime(paper_manifest.get("generated_at"), field="paper generated_at")
    performance_generated = _datetime(
        performance_manifest.get("generated_at"), field="performance generated_at"
    )
    review_generated = _datetime(snapshot.get("generated_at"), field="review generated_at")
    _require(
        target_generated <= paper_generated <= performance_generated <= review_generated,
        "Target→Paper→Performance chronology invalid",
    )
    method_rows = _records(summary.get("methods"))
    available = {
        _text(row.get("target_method"))
        for row in method_rows
        if row.get("performance_status") == "PASS" and _finite(row.get("total_return"))
    }
    review_policy = _mapping(_mapping(target_manifest.get("method_policy")).get("review_policy"))
    preferred = [_text(item) for item in review_policy.get("preferred_method_order", [])]
    _require(bool(preferred), "review preference missing")
    observation_method = next((method for method in preferred if method in available), "")
    decision_status = "CONTINUE_OBSERVATION" if observation_method else "INSUFFICIENT_DATA"
    alternatives = [
        method for method in preferred if method in available and method != observation_method
    ][:3]
    decision = {
        "review_id": review_id,
        "recommended_research_method": observation_method,
        "observation_priority_method": observation_method,
        "selection_basis": "REVIEWED_OBSERVATION_PRIORITY",
        "performance_winner_claimed": False,
        "best_return_method": summary.get("best_return_method"),
        "best_drawdown_method": summary.get("best_drawdown_method"),
        "best_risk_adjusted_method": summary.get("best_risk_adjusted_method"),
        "alternative_methods": alternatives,
        "not_recommended_methods": [
            row.get("target_method")
            for row in method_rows
            if row.get("performance_status") == "INSUFFICIENT_DATA"
        ],
        "decision_status": decision_status,
        "reason": (
            f"{observation_method} is the first available method in reviewed observation priority; "
            "performance winners remain separately disclosed and no official target is approved."
            if observation_method
            else "No method has sufficient finite common-date evidence for observation priority."
        ),
        **SYSTEM_TARGET_SAFETY,
    }
    status = "PASS" if observation_method else "PASS_WITH_WARNINGS"
    manifest = {
        "schema_version": 2,
        "report_type": "etf_dynamic_v3_system_target_review_manifest",
        "review_id": review_id,
        "system_target_review_id": review_id,
        "target_id": target_id,
        "paper_shadow_id": paper_id,
        "performance_id": performance_id,
        "generated_at": snapshot["generated_at"],
        "status": status,
        "recommended_research_method": observation_method,
        "selection_basis": "REVIEWED_OBSERVATION_PRIORITY",
        "decision_status": decision_status,
        "input_snapshot_path": str(output_dir / "system_target_review_input_snapshot.json"),
        "system_target_review_manifest_path": str(
            output_dir / "system_target_review_manifest.json"
        ),
        "system_target_decision_path": str(output_dir / "system_target_decision.json"),
        "owner_research_review_checklist_path": str(
            output_dir / "owner_research_review_checklist.md"
        ),
        "system_target_review_report_path": str(output_dir / "system_target_review_report.md"),
        "reader_brief_section_path": str(output_dir / "reader_brief_section.md"),
        **SYSTEM_TARGET_SAFETY,
    }
    target_payload = {
        **target_manifest,
        "model_target_weights": _mapping(target_json.get("model_target_weights.json")),
    }
    paper_payload = {
        **paper_manifest,
        "paper_shadow_state": _mapping(paper_json.get("paper_shadow_state.json")),
    }
    performance_payload = {**performance_manifest, "method_performance_summary": summary}
    views = {
        "system_target_review_input_snapshot.json": _json_bytes(snapshot),
        "system_target_review_manifest.json": _json_bytes(manifest),
        "system_target_decision.json": _json_bytes(decision),
        "owner_research_review_checklist.md": _text_bytes(
            legacy.render_owner_research_review_checklist(decision)
        ),
        "system_target_review_report.md": _text_bytes(
            legacy.render_system_target_review_report(
                manifest, decision, target_payload, paper_payload, performance_payload
            )
        ),
        "reader_brief_section.md": _text_bytes(
            legacy.render_system_target_reader_brief(decision, summary)
        ),
    }
    return views, {"manifest": manifest, "system_target_decision": decision}


def build_system_target_review_pack(
    *,
    target_id: str,
    paper_shadow_id: str,
    performance_id: str,
    model_target_dir: Path = DEFAULT_MODEL_TARGET_DIR,
    paper_shadow_dir: Path = DEFAULT_PAPER_SHADOW_DIR,
    performance_dir: Path = DEFAULT_PAPER_SHADOW_PERFORMANCE_DIR,
    output_dir: Path = DEFAULT_SYSTEM_TARGET_REVIEW_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = _generated_at(generated_at)
    target_root = model_target_dir / target_id
    paper_root = paper_shadow_dir / paper_shadow_id
    performance_root = performance_dir / performance_id
    target_binding = _artifact_binding(
        kind="model_target",
        artifact_dir=target_root,
        artifact_id=target_id,
        validation=validate_model_target_artifact(target_id=target_id, output_dir=model_target_dir),
        json_views=["model_target_manifest.json", "model_target_weights.json"],
        jsonl_views=["method_target_weights.jsonl"],
    )
    paper_binding = _artifact_binding(
        kind="paper_shadow",
        artifact_dir=paper_root,
        artifact_id=paper_shadow_id,
        validation=validate_paper_shadow_artifact(
            paper_shadow_id=paper_shadow_id, output_dir=paper_shadow_dir
        ),
        json_views=["paper_shadow_manifest.json", "paper_shadow_state.json"],
        jsonl_views=["paper_shadow_method_states.jsonl"],
    )
    performance_binding = _artifact_binding(
        kind="paper_shadow_performance",
        artifact_dir=performance_root,
        artifact_id=performance_id,
        validation=validate_paper_shadow_performance_artifact(
            performance_id=performance_id, output_dir=performance_dir
        ),
        json_views=[
            "paper_shadow_performance_manifest.json",
            "method_performance_summary.json",
            "method_pairwise_comparison.json",
            "regime_performance_breakdown.json",
        ],
    )
    snapshot = {
        "schema_version": SYSTEM_TARGET_REVIEW_SNAPSHOT_SCHEMA,
        "generated_at": generated.isoformat(),
        "model_target_source": target_binding,
        "paper_shadow_source": paper_binding,
        "performance_source": performance_binding,
        "lineage_requirement": "EXACT_TARGET_TO_PAPER_TO_PERFORMANCE",
        "selection_semantics": "REVIEWED_OBSERVATION_PRIORITY_NOT_PERFORMANCE_WINNER",
        "automatic_approval_allowed": False,
        "production_effect": "none",
    }
    review_id = _stable_id(
        "system-target-review",
        target_id,
        paper_shadow_id,
        performance_id,
        generated.isoformat(),
    )
    root = _unique_dir(output_dir / review_id)
    views, payload = _system_target_review_views(snapshot, review_id=root.name, output_dir=root)
    _write_views_atomic(root, views)
    _update_latest_pointer(
        "latest_system_target_review", root.name, root / "system_target_review_manifest.json"
    )
    return {
        "review_id": root.name,
        "system_target_review_id": root.name,
        "review_dir": root,
        **payload,
    }


def system_target_review_report_payload(
    *,
    review_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_SYSTEM_TARGET_REVIEW_DIR,
) -> dict[str, Any]:
    root = _artifact_dir_from_latest(
        output_dir=output_dir,
        artifact_id=review_id if not latest else None,
        pointer_name="latest_system_target_review",
    )
    return {
        **_read_json(root / "system_target_review_manifest.json"),
        "system_target_decision": _read_json(root / "system_target_decision.json"),
        "review_dir": str(root),
        **_report_input_snapshot(root / "system_target_review_input_snapshot.json"),
    }


def _validate_review_snapshot(snapshot: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    if snapshot.get("schema_version") != SYSTEM_TARGET_REVIEW_SNAPSHOT_SCHEMA:
        errors.append("system target review snapshot schema invalid")
    for key in ("model_target_source", "paper_shadow_source", "performance_source"):
        errors.extend(_validate_artifact_binding(_mapping(snapshot.get(key))))
    return errors


def validate_system_target_review_artifact(
    *, review_id: str, output_dir: Path = DEFAULT_SYSTEM_TARGET_REVIEW_DIR
) -> dict[str, Any]:
    root = output_dir / review_id
    snapshot = _read_optional_json(root / "system_target_review_input_snapshot.json") or {}
    errors = _validate_review_snapshot(snapshot)
    mismatches: list[str] = []
    try:
        views, _ = _system_target_review_views(snapshot, review_id=review_id, output_dir=root)
        mismatches = [
            name for name, payload in views.items() if not _file_bytes_match(root / name, payload)
        ]
    except Exception as exc:  # noqa: BLE001
        errors.append(str(exc))
    decision = _read_optional_json(root / "system_target_decision.json") or {}
    checks = [
        _check("snapshot_live_sources_and_exact_lineage", not errors, "; ".join(errors)),
        _check("content_derived_views", not mismatches, ",".join(mismatches)),
        _check(
            "observation_priority_not_winner",
            decision.get("selection_basis") == "REVIEWED_OBSERVATION_PRIORITY"
            and decision.get("performance_winner_claimed") is False,
            "manual observation only",
        ),
        _check(
            "research_safety",
            decision.get("production_effect") == "none"
            and decision.get("broker_action_allowed") is False,
            "production_effect=none",
        ),
    ]
    return _validation_payload("etf_dynamic_v3_system_target_review_validation", review_id, checks)
