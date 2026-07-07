from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, date, datetime, time
from numbers import Number
from typing import Any

SCHEMA_NAME = "signal_validity_contract"
SCHEMA_VERSION = "signal_validity_contract.v1"

CARRY_FORWARD_RULE_VALUES: tuple[str, ...] = (
    "FORBIDDEN",
    "ALLOWED_WITH_EXPLICIT_RULE",
    "ALLOWED_WITH_OWNER_APPROVAL",
    "UNKNOWN",
)
NEAR_EXPIRY_RULE_VALUES: tuple[str, ...] = (
    "BLOCK",
    "DECAY",
    "REQUIRE_REFRESH",
    "ALLOW_WITH_CAVEAT",
    "UNKNOWN",
)
REQUIRED_FIELDS: tuple[str, ...] = (
    "signal_id",
    "signal_version",
    "valid_from",
    "valid_until",
    "stale_after",
    "horizon_days",
    "expiry_rule",
    "carry_forward_rule",
    "near_expiry_rule",
    "signal_to_execution_lag_rule",
)
INVARIANTS: tuple[str, ...] = (
    "valid_until > valid_from",
    "stale_after <= valid_until",
    "horizon_days > 0",
    "expired_signal_cannot_trigger_new_trade",
    "carry_forward_requires_explicit_rule",
    "missing_valid_until_blocks_candidate_search_for_dependent_strategy",
    "signal_to_execution_lag_rule_must_be_present",
)
VALIDATION_ERROR_CODES: tuple[str, ...] = (
    "REQUIRED_FIELD_MISSING",
    "INVALID_ENUM_VALUE",
    "INVALID_DATE_ORDER",
    "INVALID_HORIZON",
    "CARRY_FORWARD_RULE_UNKNOWN",
    "VALID_UNTIL_MISSING_OR_INVALID",
)


def build_signal_validity_contract_schema() -> dict[str, Any]:
    return {
        "schema_name": SCHEMA_NAME,
        "schema_version": SCHEMA_VERSION,
        "fields": {
            "signal_id": {"type": "string", "required": True},
            "signal_version": {"type": "string", "required": True},
            "valid_from": {"type": "date_or_datetime", "required": True},
            "valid_until": {"type": "date_or_datetime", "required": True},
            "stale_after": {"type": "date_or_datetime", "required": True},
            "horizon_days": {"type": "integer", "required": True},
            "expiry_rule": {"type": "enum_or_string", "required": True},
            "carry_forward_rule": {
                "type": "enum",
                "values": list(CARRY_FORWARD_RULE_VALUES),
                "required": True,
            },
            "near_expiry_rule": {
                "type": "enum",
                "values": list(NEAR_EXPIRY_RULE_VALUES),
                "required": True,
            },
            "signal_to_execution_lag_rule": {
                "type": "enum_or_string",
                "required": True,
            },
        },
        "required_fields": list(REQUIRED_FIELDS),
        "invariants": list(INVARIANTS),
        "validation_error_codes": list(VALIDATION_ERROR_CODES),
        "production_effect": "none",
        "broker_action": "none",
    }


def validate_signal_validity_contract(payload: Mapping[str, Any]) -> dict[str, Any]:
    errors: list[dict[str, Any]] = []
    warnings: list[dict[str, Any]] = []
    for field in REQUIRED_FIELDS:
        if _missing(payload, field):
            code = (
                "VALID_UNTIL_MISSING_OR_INVALID"
                if field == "valid_until"
                else "REQUIRED_FIELD_MISSING"
            )
            errors.append(_error(field, code, "required field missing"))

    if payload.get("carry_forward_rule") not in (None, "") and (
        payload.get("carry_forward_rule") not in CARRY_FORWARD_RULE_VALUES
    ):
        errors.append(
            _error(
                "carry_forward_rule",
                "INVALID_ENUM_VALUE",
                "carry_forward_rule has invalid value",
            )
        )
    if payload.get("near_expiry_rule") not in (None, "") and (
        payload.get("near_expiry_rule") not in NEAR_EXPIRY_RULE_VALUES
    ):
        errors.append(
            _error(
                "near_expiry_rule",
                "INVALID_ENUM_VALUE",
                "near_expiry_rule has invalid value",
            )
        )

    valid_from = _parse_temporal(payload.get("valid_from"))
    valid_until = _parse_temporal(payload.get("valid_until"))
    stale_after = _parse_temporal(payload.get("stale_after"))
    if payload.get("valid_until") and valid_until is None:
        errors.append(
            _error(
                "valid_until",
                "VALID_UNTIL_MISSING_OR_INVALID",
                "valid_until must be a date or datetime",
            )
        )
    if valid_from and valid_until and valid_until <= valid_from:
        errors.append(
            _error(
                "valid_until",
                "INVALID_DATE_ORDER",
                "valid_until must be greater than valid_from",
            )
        )
    if stale_after and valid_until and stale_after > valid_until:
        errors.append(
            _error(
                "stale_after",
                "INVALID_DATE_ORDER",
                "stale_after must be less than or equal to valid_until",
            )
        )
    if not _positive_integer(payload.get("horizon_days")):
        errors.append(
            _error("horizon_days", "INVALID_HORIZON", "horizon_days must be greater than 0")
        )
    if payload.get("carry_forward_rule") == "UNKNOWN":
        errors.append(
            _error(
                "carry_forward_rule",
                "CARRY_FORWARD_RULE_UNKNOWN",
                "carry_forward_rule must be explicit",
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


def valid_signal_validity_contract_example() -> dict[str, Any]:
    return {
        "signal_id": "example_signal",
        "signal_version": "v1",
        "valid_from": "2026-07-07T21:00:00",
        "valid_until": "2026-07-14T21:00:00",
        "stale_after": "2026-07-10T21:00:00",
        "horizon_days": 5,
        "expiry_rule": "valid_until_close",
        "carry_forward_rule": "FORBIDDEN",
        "near_expiry_rule": "BLOCK",
        "signal_to_execution_lag_rule": "next_executable_session_only",
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
