from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import Any

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.yaml_loader import safe_load_yaml_path

DEFAULT_DYNAMIC_STRATEGY_PIT_INPUT_REGISTRY_PATH = (
    PROJECT_ROOT / "config" / "research" / "dynamic_strategy_pit_input_registry.yaml"
)

REGISTRY_SCHEMA_VERSION = "dynamic_strategy_pit_input_registry.v1"
INPUT_TYPES: tuple[str, ...] = (
    "MARKET_DATA",
    "FEATURE",
    "SIGNAL",
    "EXECUTION_SEMANTIC",
    "REGIME_LABEL",
    "GATE_INPUT",
    "REPORTING_INPUT",
)
PIT_STATUSES: tuple[str, ...] = (
    "TRUE_PIT",
    "APPROXIMATE_PIT",
    "NOT_PIT_SAFE",
    "UNKNOWN",
    "NOT_APPLICABLE",
)
PIT_CONFIDENCE_VALUES: tuple[str, ...] = ("HIGH", "MEDIUM", "LOW", "UNKNOWN")
SEVERITIES: tuple[str, ...] = ("BLOCKING", "MATERIAL", "MINOR", "INFO")

REQUIRED_ENTRY_FIELDS: tuple[str, ...] = (
    "input_id",
    "input_type",
    "owner_module",
    "source_artifact_or_config",
    "used_by",
    "pit_status",
    "pit_confidence",
    "risk_flags",
    "severity",
    "candidate_search_blocker",
    "observation_blocker",
    "paper_shadow_blocker",
    "production_blocker",
    "remediation_owner",
    "recommended_action",
)


def load_pit_input_registry(
    path: Path = DEFAULT_DYNAMIC_STRATEGY_PIT_INPUT_REGISTRY_PATH,
) -> dict[str, Any]:
    raw = safe_load_yaml_path(path)
    if not isinstance(raw, Mapping):
        return _invalid_registry(path, "registry root must be a mapping")
    payload = dict(raw)
    entries = [_normalize_entry(entry) for entry in _as_list(payload.get("entries"))]
    registry = {
        "schema_version": _text(payload.get("schema_version")),
        "scope": _text(payload.get("scope")),
        "owner": _text(payload.get("owner")),
        "status": _text(payload.get("status")),
        "rationale": _text(payload.get("rationale")),
        "intended_effect": _text(payload.get("intended_effect")),
        "validation_evidence": _text(payload.get("validation_evidence")),
        "review_condition": _text(payload.get("review_condition")),
        "entries": entries,
        "path": str(path),
    }
    registry["validation_errors"] = validate_pit_input_registry(registry)
    registry["entry_count"] = len(entries)
    registry["validation_status"] = (
        "PASS" if not registry["validation_errors"] else "FAIL"
    )
    return registry


def validate_pit_input_registry(registry: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    if registry.get("schema_version") != REGISTRY_SCHEMA_VERSION:
        errors.append(
            "registry schema_version must be "
            f"{REGISTRY_SCHEMA_VERSION}, observed {registry.get('schema_version')}"
        )
    if registry.get("scope") != "dynamic_strategy":
        errors.append("registry scope must be dynamic_strategy")
    entries = _as_list(registry.get("entries"))
    if not entries:
        errors.append("registry must contain at least one entry")
    seen: set[str] = set()
    for index, raw_entry in enumerate(entries):
        entry = _as_mapping(raw_entry)
        input_id = _text(entry.get("input_id"))
        if not input_id:
            errors.append(f"entry[{index}] input_id is required")
        elif input_id in seen:
            errors.append(f"duplicate input_id: {input_id}")
        seen.add(input_id)
        for field in REQUIRED_ENTRY_FIELDS:
            if field not in entry:
                errors.append(f"{input_id or f'entry[{index}]'} missing field: {field}")
        if entry.get("input_type") not in INPUT_TYPES:
            errors.append(f"{input_id}: invalid input_type {entry.get('input_type')}")
        if entry.get("pit_status") not in PIT_STATUSES:
            errors.append(f"{input_id}: invalid pit_status {entry.get('pit_status')}")
        if entry.get("pit_confidence") not in PIT_CONFIDENCE_VALUES:
            errors.append(
                f"{input_id}: invalid pit_confidence {entry.get('pit_confidence')}"
            )
        if entry.get("severity") not in SEVERITIES:
            errors.append(f"{input_id}: invalid severity {entry.get('severity')}")
        if not isinstance(entry.get("used_by"), list) or not entry.get("used_by"):
            errors.append(f"{input_id}: used_by must be a non-empty list")
        if not isinstance(entry.get("risk_flags"), list):
            errors.append(f"{input_id}: risk_flags must be a list")
        for field in (
            "candidate_search_blocker",
            "observation_blocker",
            "paper_shadow_blocker",
            "production_blocker",
        ):
            if not isinstance(entry.get(field), bool):
                errors.append(f"{input_id}: {field} must be boolean")
    return errors


def registry_entries_by_id(registry: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    return {
        _text(entry.get("input_id")): dict(entry)
        for entry in _as_list(registry.get("entries"))
        if isinstance(entry, Mapping) and _text(entry.get("input_id"))
    }


def _invalid_registry(path: Path, reason: str) -> dict[str, Any]:
    return {
        "schema_version": "",
        "scope": "",
        "owner": "",
        "status": "",
        "entries": [],
        "path": str(path),
        "entry_count": 0,
        "validation_status": "FAIL",
        "validation_errors": [reason],
    }


def _normalize_entry(value: Any) -> dict[str, Any]:
    entry = dict(_as_mapping(value))
    for key in (
        "input_id",
        "input_type",
        "owner_module",
        "source_artifact_or_config",
        "pit_status",
        "pit_confidence",
        "severity",
        "remediation_owner",
        "recommended_action",
    ):
        entry[key] = _text(entry.get(key))
    for key in (
        "as_of_field",
        "generated_at_field",
        "valid_from_field",
        "valid_until_field",
    ):
        entry[key] = None if entry.get(key) is None else _text(entry.get(key))
    entry["used_by"] = [_text(item) for item in _as_list(entry.get("used_by"))]
    entry["risk_flags"] = [_text(item) for item in _as_list(entry.get("risk_flags"))]
    for key in (
        "candidate_search_blocker",
        "observation_blocker",
        "paper_shadow_blocker",
        "production_blocker",
    ):
        entry[key] = bool(entry.get(key))
    return entry


def _as_mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _as_list(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []


def _text(value: Any) -> str:
    return str(value) if value not in (None, "") else ""
