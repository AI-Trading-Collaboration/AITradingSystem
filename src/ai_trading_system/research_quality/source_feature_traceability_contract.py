from __future__ import annotations

from collections.abc import Mapping
from typing import Any

SCHEMA_NAME = "source_feature_traceability_contract"
SCHEMA_VERSION = "source_feature_traceability_contract.v1"

AS_OF_HANDLING_VALUES: tuple[str, ...] = (
    "EXPLICIT_AS_OF",
    "DERIVED_FROM_SOURCE_CUTOFF",
    "APPROXIMATE",
    "UNKNOWN",
)
GENERATED_AT_HANDLING_VALUES: tuple[str, ...] = (
    "EXPLICIT_GENERATED_AT",
    "DERIVED_FROM_PIPELINE_RUN",
    "APPROXIMATE",
    "UNKNOWN",
)
PIT_STATUS_VALUES: tuple[str, ...] = (
    "TRUE_PIT",
    "APPROXIMATE_PIT",
    "NOT_PIT_SAFE",
    "UNKNOWN",
    "NOT_APPLICABLE",
)
PIT_CONFIDENCE_VALUES: tuple[str, ...] = ("HIGH", "MEDIUM", "LOW", "UNKNOWN")
RISK_FLAG_VALUES: tuple[str, ...] = (
    "LOOKAHEAD_RISK",
    "REVISION_RISK",
    "BACKFILL_RISK",
    "STALE_DATA_RISK",
    "MISSING_DATA_RISK",
    "REGIME_CONFIRMATION_RISK",
    "VALID_UNTIL_UNGROUNDED",
    "THRESHOLD_UNCALIBRATED",
)
SEVERITY_VALUES: tuple[str, ...] = ("BLOCKING", "MATERIAL", "MINOR", "INFO")
REQUIRED_FIELDS: tuple[str, ...] = (
    "feature_id",
    "feature_family",
    "source_config",
    "source_data",
    "as_of_handling",
    "generated_at_handling",
    "forward_window_used",
    "pit_status",
    "pit_confidence",
    "severity",
)
OPTIONAL_FIELDS: tuple[str, ...] = ("lookback_window", "risk_flags", "explicit_reason")
INVARIANTS: tuple[str, ...] = (
    "every_signal_feature_has_feature_id",
    "forward_window_used=false_for_TRUE_PIT_features",
    "pit_status_UNKNOWN_requires_LOW_or_UNKNOWN_confidence",
    "severity_BLOCKING_requires_risk_flag_or_explicit_reason",
    "NOT_PIT_SAFE_cannot_have_HIGH_confidence",
)
VALIDATION_ERROR_CODES: tuple[str, ...] = (
    "REQUIRED_FIELD_MISSING",
    "INVALID_ENUM_VALUE",
    "INVALID_PIT_CONFIDENCE_COMBINATION",
    "FORWARD_WINDOW_CONFLICTS_WITH_TRUE_PIT",
    "BLOCKING_SEVERITY_REQUIRES_REASON",
)


def build_source_feature_traceability_contract_schema() -> dict[str, Any]:
    return {
        "schema_name": SCHEMA_NAME,
        "schema_version": SCHEMA_VERSION,
        "fields": {
            "feature_id": {"type": "string", "required": True},
            "feature_family": {"type": "string", "required": True},
            "source_config": {"type": "string", "required": True},
            "source_data": {"type": "string_or_list", "required": True},
            "as_of_handling": {
                "type": "enum",
                "values": list(AS_OF_HANDLING_VALUES),
                "required": True,
            },
            "generated_at_handling": {
                "type": "enum",
                "values": list(GENERATED_AT_HANDLING_VALUES),
                "required": True,
            },
            "lookback_window": {"type": "integer_or_null", "required": False},
            "forward_window_used": {"type": "boolean", "required": True},
            "pit_status": {
                "type": "enum",
                "values": list(PIT_STATUS_VALUES),
                "required": True,
            },
            "pit_confidence": {
                "type": "enum",
                "values": list(PIT_CONFIDENCE_VALUES),
                "required": True,
            },
            "risk_flags": {
                "type": "list",
                "values": list(RISK_FLAG_VALUES),
                "required": False,
            },
            "severity": {
                "type": "enum",
                "values": list(SEVERITY_VALUES),
                "required": True,
            },
        },
        "required_fields": list(REQUIRED_FIELDS),
        "optional_fields": list(OPTIONAL_FIELDS),
        "invariants": list(INVARIANTS),
        "validation_error_codes": list(VALIDATION_ERROR_CODES),
        "production_effect": "none",
        "broker_action": "none",
    }


def validate_source_feature_traceability_contract(
    payload: Mapping[str, Any],
) -> dict[str, Any]:
    errors: list[dict[str, Any]] = []
    warnings: list[dict[str, Any]] = []
    for field in REQUIRED_FIELDS:
        if _missing(payload, field):
            errors.append(_error(field, "REQUIRED_FIELD_MISSING", "required field missing"))

    _check_enum(errors, payload, "as_of_handling", AS_OF_HANDLING_VALUES)
    _check_enum(errors, payload, "generated_at_handling", GENERATED_AT_HANDLING_VALUES)
    _check_enum(errors, payload, "pit_status", PIT_STATUS_VALUES)
    _check_enum(errors, payload, "pit_confidence", PIT_CONFIDENCE_VALUES)
    _check_enum(errors, payload, "severity", SEVERITY_VALUES)

    pit_status = payload.get("pit_status")
    pit_confidence = payload.get("pit_confidence")
    if pit_status == "TRUE_PIT" and payload.get("forward_window_used") is True:
        errors.append(
            _error(
                "forward_window_used",
                "FORWARD_WINDOW_CONFLICTS_WITH_TRUE_PIT",
                "TRUE_PIT features cannot use forward windows",
            )
        )
    if pit_status == "UNKNOWN" and pit_confidence not in {"LOW", "UNKNOWN"}:
        errors.append(
            _error(
                "pit_confidence",
                "INVALID_PIT_CONFIDENCE_COMBINATION",
                "UNKNOWN pit_status requires LOW or UNKNOWN pit_confidence",
            )
        )
    if pit_status == "NOT_PIT_SAFE" and pit_confidence == "HIGH":
        errors.append(
            _error(
                "pit_confidence",
                "INVALID_PIT_CONFIDENCE_COMBINATION",
                "NOT_PIT_SAFE cannot have HIGH pit_confidence",
            )
        )
    if payload.get("severity") == "BLOCKING" and not (
        _as_list(payload.get("risk_flags"))
        or payload.get("explicit_reason")
        or payload.get("blocker_reason")
    ):
        errors.append(
            _error(
                "severity",
                "BLOCKING_SEVERITY_REQUIRES_REASON",
                "BLOCKING severity requires a risk flag or explicit reason",
            )
        )

    return {
        "valid": not errors,
        "schema_name": SCHEMA_NAME,
        "error_count": len(errors),
        "warning_count": len(warnings),
        "errors": errors,
        "warnings": warnings,
    }


def valid_source_feature_traceability_contract_example() -> dict[str, Any]:
    return {
        "feature_id": "feature_a",
        "feature_family": "growth_tilt",
        "source_config": "config/example.yaml",
        "source_data": ["source_a"],
        "as_of_handling": "EXPLICIT_AS_OF",
        "generated_at_handling": "EXPLICIT_GENERATED_AT",
        "lookback_window": 63,
        "forward_window_used": False,
        "pit_status": "APPROXIMATE_PIT",
        "pit_confidence": "MEDIUM",
        "risk_flags": ["REVISION_RISK"],
        "severity": "MATERIAL",
    }


def _check_enum(
    errors: list[dict[str, Any]],
    payload: Mapping[str, Any],
    field: str,
    allowed_values: tuple[str, ...],
) -> None:
    value = payload.get(field)
    if value is None or value == "":
        return
    if value not in allowed_values:
        errors.append(
            _error(field, "INVALID_ENUM_VALUE", f"{field} must be one of {allowed_values}")
        )


def _error(field: str, code: str, message: str) -> dict[str, Any]:
    return {"field": field, "code": code, "message": message, "severity": "ERROR"}


def _missing(payload: Mapping[str, Any], field: str) -> bool:
    value = payload.get(field)
    return value is None or value == ""


def _as_list(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []
