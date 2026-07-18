from __future__ import annotations

import json
import re
from collections.abc import Mapping
from pathlib import PurePosixPath

SCHEMA_VERSION = "validation_trigger_provenance.v1"
TRIGGER_REASON_ENV = "AITS_VALIDATION_TRIGGER_REASON"
TASK_ID_ENV = "AITS_VALIDATION_TASK_ID"
BOUNDARY_ID_ENV = "AITS_VALIDATION_BOUNDARY_ID"
PARENT_RUN_ENV = "AITS_VALIDATION_PARENT_RUN"
PROFILE_JSON_ENV = "AITS_VALIDATION_PROVENANCE_JSON"

FULL_TRIGGER_REASONS = (
    "natural_integration_boundary",
    "phase_exit_or_handoff",
    "broad_shared_contract_change",
    "formal_performance_profile",
    "failure_fix_rerun",
    "scheduled_ci",
)
OPTIONAL_TRIGGER_REASONS = (
    "ci_change_validation",
    "focused_validation",
    "manual_validation",
    "serial_reproduction",
)
TRIGGER_REASONS = (*FULL_TRIGGER_REASONS, *OPTIONAL_TRIGGER_REASONS)

IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:/@+-]{0,255}$")
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
PROVENANCE_KEYS = {
    "schema_version",
    "status",
    "required_for_tier",
    "trigger_reason",
    "task_id",
    "boundary_id",
    "parent_run",
    "envelope_source",
    "field_sources",
    "cli_over_environment_precedence",
    "validation_errors",
}
PARENT_RUN_KEYS = {
    "run_id",
    "summary_path",
    "summary_sha256",
    "runtime_profile_sha256",
    "report_type",
    "resolved_tier",
    "status",
    "failure_basis",
    "production_effect",
}


def _reject_duplicate_keys(pairs: list[tuple[str, object]]) -> dict[str, object]:
    payload: dict[str, object] = {}
    for key, value in pairs:
        if key in payload:
            raise ValueError(f"duplicate JSON key: {key}")
        payload[key] = value
    return payload


def _reject_non_finite_constant(value: str) -> object:
    raise ValueError(f"non-finite JSON constant: {value}")


def load_json(raw_value: str) -> dict[str, object]:
    payload = json.loads(
        raw_value,
        object_pairs_hook=_reject_duplicate_keys,
        parse_constant=_reject_non_finite_constant,
    )
    if not isinstance(payload, dict):
        raise ValueError("validation provenance root must be a mapping")
    return payload


def validate_full_provenance(payload: object) -> list[str]:
    """Validate the exact v1 envelope used by formal Full runtime profiles."""

    if not isinstance(payload, Mapping):
        return ["validation provenance must be a mapping"]
    errors: list[str] = []
    keys = set(payload)
    if keys != PROVENANCE_KEYS:
        errors.append(
            "validation provenance keys mismatch: "
            f"missing={sorted(PROVENANCE_KEYS - keys)} extra={sorted(keys - PROVENANCE_KEYS)}"
        )
    if payload.get("schema_version") != SCHEMA_VERSION:
        errors.append("validation provenance schema_version is invalid")
    if payload.get("status") != "PASS":
        errors.append("validation provenance status must be PASS")
    if payload.get("required_for_tier") is not True:
        errors.append("validation provenance required_for_tier must be true")
    trigger_reason = payload.get("trigger_reason")
    if not isinstance(trigger_reason, str) or trigger_reason not in FULL_TRIGGER_REASONS:
        errors.append("validation provenance trigger_reason is not Full-eligible")
    for field_name in ("task_id", "boundary_id"):
        value = payload.get(field_name)
        if not isinstance(value, str) or IDENTIFIER_RE.fullmatch(value) is None:
            errors.append(f"validation provenance {field_name} is invalid")
    input_source = payload.get("envelope_source")
    if not isinstance(input_source, str) or input_source not in ("cli", "environment"):
        errors.append("validation provenance envelope_source is invalid")
    field_sources = payload.get("field_sources")
    if not isinstance(field_sources, Mapping) or set(field_sources) != {
        "trigger_reason",
        "task_id",
        "boundary_id",
        "parent_run",
    }:
        errors.append("validation provenance field_sources is invalid")
    elif any(
        not isinstance(value, str) or value not in (input_source, "unset")
        for value in field_sources.values()
    ):
        errors.append("validation provenance field_sources mix multiple envelopes")
    elif any(
        field_sources[field_name]
        != ("unset" if payload.get(field_name) is None else input_source)
        for field_name in field_sources
    ):
        errors.append("validation provenance field_sources do not match declared values")
    if payload.get("cli_over_environment_precedence") != "whole_envelope":
        errors.append("validation provenance envelope precedence is invalid")
    if payload.get("validation_errors") != []:
        errors.append("validation provenance contains unresolved validation errors")

    parent_run = payload.get("parent_run")
    if trigger_reason == "failure_fix_rerun":
        if not isinstance(parent_run, Mapping):
            errors.append("failure_fix_rerun parent_run must be a validated summary binding")
        else:
            parent_keys = set(parent_run)
            if parent_keys != PARENT_RUN_KEYS:
                errors.append(
                    "parent_run keys mismatch: "
                    f"missing={sorted(PARENT_RUN_KEYS - parent_keys)} "
                    f"extra={sorted(parent_keys - PARENT_RUN_KEYS)}"
                )
            if (
                not isinstance(parent_run.get("run_id"), str)
                or IDENTIFIER_RE.fullmatch(str(parent_run.get("run_id"))) is None
            ):
                errors.append("parent_run run_id is invalid")
            summary_path = parent_run.get("summary_path")
            if not isinstance(summary_path, str):
                errors.append("parent_run summary_path is invalid")
            else:
                normalized_path = summary_path.replace("\\", "/")
                parts = PurePosixPath(normalized_path).parts
                if (
                    normalized_path != summary_path
                    or not normalized_path.startswith("outputs/validation_runtime/")
                    or not normalized_path.endswith("/test_runtime_summary.json")
                    or PurePosixPath(normalized_path).is_absolute()
                    or "." in parts
                    or ".." in parts
                    or len(parts) < 4
                    or parts[-2] != parent_run.get("run_id")
                ):
                    errors.append("parent_run summary_path is invalid")
            if (
                not isinstance(parent_run.get("summary_sha256"), str)
                or SHA256_RE.fullmatch(str(parent_run.get("summary_sha256"))) is None
            ):
                errors.append("parent_run summary_sha256 is invalid")
            if (
                not isinstance(parent_run.get("runtime_profile_sha256"), str)
                or SHA256_RE.fullmatch(str(parent_run.get("runtime_profile_sha256"))) is None
            ):
                errors.append("parent_run runtime_profile_sha256 is invalid")
            if parent_run.get("report_type") != "test_runtime_summary":
                errors.append("parent_run report_type is invalid")
            if parent_run.get("resolved_tier") != "full":
                errors.append("parent_run resolved_tier must be full")
            parent_status = parent_run.get("status")
            if not isinstance(parent_status, str) or parent_status not in ("PASS", "FAIL"):
                errors.append("parent_run status is invalid")
            failure_basis = parent_run.get("failure_basis")
            if not isinstance(failure_basis, str) or failure_basis not in (
                "PYTEST_FAIL",
                "RUNTIME_PROFILE_FAIL",
            ):
                errors.append("parent_run failure_basis is invalid")
            elif failure_basis == "PYTEST_FAIL" and parent_status != "FAIL":
                errors.append("parent_run PYTEST_FAIL requires status=FAIL")
            elif failure_basis == "RUNTIME_PROFILE_FAIL" and parent_status != "PASS":
                errors.append(f"parent_run {failure_basis} requires status=PASS")
            if parent_run.get("production_effect") != "none":
                errors.append("parent_run production_effect must be none")
    elif parent_run is not None:
        errors.append("parent_run is only allowed for failure_fix_rerun")
    return errors
