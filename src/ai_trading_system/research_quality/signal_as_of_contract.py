from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, date, datetime, time
from numbers import Number
from typing import Any

SCHEMA_NAME = "signal_as_of_contract"
SCHEMA_VERSION = "signal_as_of_contract.v1"

REQUIRED_FIELDS: tuple[str, ...] = (
    "signal_id",
    "signal_version",
    "as_of_date",
    "generated_at",
    "source_data_cutoff",
    "source_feature_ids",
    "signal_horizon_days",
    "signal_value",
)
OPTIONAL_FIELDS: tuple[str, ...] = (
    "source_artifact_ids",
    "signal_strength",
    "confidence",
    "uncertainty_reason",
)
INVARIANTS: tuple[str, ...] = (
    "generated_at >= source_data_cutoff",
    "as_of_date <= generated_at_or_research_as_of",
    "source_feature_ids_non_empty",
    "signal_horizon_days > 0",
    "no_forward_window_dependency_unless_explicitly_marked_not_pit_safe",
)
VALIDATION_ERROR_CODES: tuple[str, ...] = (
    "REQUIRED_FIELD_MISSING",
    "INVALID_DATE_ORDER",
    "INVALID_HORIZON",
    "FORWARD_WINDOW_DEPENDENCY_NOT_MARKED_NOT_PIT_SAFE",
)


def build_signal_as_of_contract_schema() -> dict[str, Any]:
    return {
        "schema_name": SCHEMA_NAME,
        "schema_version": SCHEMA_VERSION,
        "fields": {
            "signal_id": {"type": "string", "required": True},
            "signal_version": {"type": "string", "required": True},
            "as_of_date": {"type": "date", "required": True},
            "generated_at": {"type": "datetime", "required": True},
            "source_data_cutoff": {"type": "date_or_datetime", "required": True},
            "source_feature_ids": {"type": "list[string]", "required": True},
            "source_artifact_ids": {"type": "list[string]", "required": False},
            "signal_horizon_days": {"type": "integer", "required": True},
            "signal_value": {"type": "number_or_object", "required": True},
            "signal_strength": {"type": "number", "required": False},
            "confidence": {"type": "number_or_enum", "required": False},
            "uncertainty_reason": {"type": "string_or_list", "required": False},
        },
        "required_fields": list(REQUIRED_FIELDS),
        "optional_fields": list(OPTIONAL_FIELDS),
        "invariants": list(INVARIANTS),
        "validation_error_codes": list(VALIDATION_ERROR_CODES),
        "production_effect": "none",
        "broker_action": "none",
    }


def validate_signal_as_of_contract(
    payload: Mapping[str, Any],
    *,
    research_as_of: date | datetime | str | None = None,
) -> dict[str, Any]:
    errors: list[dict[str, Any]] = []
    warnings: list[dict[str, Any]] = []
    for field in REQUIRED_FIELDS:
        if _missing(payload, field):
            errors.append(_error(field, "REQUIRED_FIELD_MISSING", "required field missing"))

    generated_at = _parse_temporal(payload.get("generated_at"))
    source_cutoff = _parse_temporal(payload.get("source_data_cutoff"))
    as_of_date = _parse_temporal(payload.get("as_of_date"))
    if generated_at and source_cutoff and generated_at < source_cutoff:
        errors.append(
            _error(
                "generated_at",
                "INVALID_DATE_ORDER",
                "generated_at must be greater than or equal to source_data_cutoff",
            )
        )
    as_of_limit = _parse_temporal(research_as_of) or generated_at
    if as_of_date and as_of_limit and as_of_date > as_of_limit:
        errors.append(
            _error(
                "as_of_date",
                "INVALID_DATE_ORDER",
                "as_of_date must be less than or equal to generated_at or research_as_of",
            )
        )
    if payload.get("source_feature_ids") == []:
        errors.append(
            _error(
                "source_feature_ids",
                "REQUIRED_FIELD_MISSING",
                "source_feature_ids must be non-empty",
            )
        )
    if not _positive_integer(payload.get("signal_horizon_days")):
        errors.append(
            _error(
                "signal_horizon_days",
                "INVALID_HORIZON",
                "signal_horizon_days must be greater than 0",
            )
        )
    if (
        payload.get("forward_window_dependency") is True
        and payload.get("pit_status") != "NOT_PIT_SAFE"
    ):
        errors.append(
            _error(
                "forward_window_dependency",
                "FORWARD_WINDOW_DEPENDENCY_NOT_MARKED_NOT_PIT_SAFE",
                "forward window dependency must be explicitly marked NOT_PIT_SAFE",
            )
        )

    return _validation_result(errors, warnings)


def valid_signal_as_of_contract_example() -> dict[str, Any]:
    return {
        "signal_id": "example_signal",
        "signal_version": "v1",
        "as_of_date": "2026-07-07",
        "generated_at": "2026-07-07T21:00:00",
        "source_data_cutoff": "2026-07-07",
        "source_feature_ids": ["feature_a"],
        "source_artifact_ids": ["artifact_a"],
        "signal_horizon_days": 5,
        "signal_value": {"score": 0.25},
        "signal_strength": 0.25,
        "confidence": "MEDIUM",
        "uncertainty_reason": [],
    }


def _validation_result(
    errors: list[dict[str, Any]],
    warnings: list[dict[str, Any]],
) -> dict[str, Any]:
    return {
        "valid": not errors,
        "schema_name": SCHEMA_NAME,
        "error_count": len(errors),
        "warning_count": len(warnings),
        "errors": errors,
        "warnings": warnings,
    }


def _error(field: str, code: str, message: str) -> dict[str, Any]:
    return {"field": field, "code": code, "message": message, "severity": "ERROR"}


def _missing(payload: Mapping[str, Any], field: str) -> bool:
    value = payload.get(field)
    return value is None or value == ""


def _positive_integer(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value > 0


def _parse_temporal(value: Any) -> datetime | None:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return _naive_utc(value)
    if isinstance(value, date):
        return datetime.combine(value, time.min)
    if isinstance(value, Number):
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        try:
            parsed = datetime.combine(date.fromisoformat(text), time.min)
        except ValueError:
            return None
    return _naive_utc(parsed)


def _naive_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value
    return value.astimezone(UTC).replace(tzinfo=None)
