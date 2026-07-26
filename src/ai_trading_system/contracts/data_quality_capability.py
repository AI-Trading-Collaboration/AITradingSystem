from __future__ import annotations

import hashlib
import json
import re
from datetime import date, datetime
from typing import Any, Self

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

_SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
_TEXT_PATTERN = re.compile(r"^[^\x00-\x1f\x7f]+$")


class DataQualityCapabilityContractError(ValueError):
    def __init__(self, code: str, message: str) -> None:
        self.code = code
        self.message = message
        super().__init__(f"{code}: {message}")


class _StrictContractModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


def _required_text(value: str, field: str) -> str:
    if not value or value != value.strip() or not _TEXT_PATTERN.fullmatch(value):
        raise ValueError(f"{field} must be non-empty normalized text")
    return value


def _sorted_unique(
    values: tuple[str, ...],
    field: str,
    *,
    allow_empty: bool = False,
) -> tuple[str, ...]:
    normalized = tuple(sorted(_required_text(str(item), field) for item in values))
    if (not normalized and not allow_empty) or len(normalized) != len(set(normalized)):
        raise ValueError(f"{field} must contain unique values")
    return normalized


class ConsumerDataCapabilityPolicy(_StrictContractModel):
    schema_version: str
    policy_id: str
    policy_version: str
    status: str
    owner: str
    owner_decision_id: str
    capability_id: str
    capability_version: str
    consumer_id: str
    consumer_version: str
    requested_start: date
    required_price_tickers: tuple[str, ...]
    required_rate_series: tuple[str, ...]
    required_price_fields: tuple[str, ...]
    accepted_scoped_statuses: tuple[str, ...]
    allowed_global_error_codes: tuple[str, ...]
    global_error_attribution_rule: str
    full_canonical_validation_required: bool
    same_validation_code_path: str
    full_status_disclosure_required: bool
    cross_consumer_reuse_allowed: bool
    daily_operation_authorized: bool
    production_effect: str
    broker_action: str
    review_condition: str

    @field_validator(
        "schema_version",
        "policy_id",
        "policy_version",
        "status",
        "owner",
        "owner_decision_id",
        "capability_id",
        "capability_version",
        "consumer_id",
        "consumer_version",
        "global_error_attribution_rule",
        "same_validation_code_path",
        "production_effect",
        "broker_action",
        "review_condition",
    )
    @classmethod
    def _validate_text(cls, value: str, info: Any) -> str:
        return _required_text(value, str(info.field_name))

    @field_validator(
        "required_price_tickers",
        "required_price_fields",
        "accepted_scoped_statuses",
        "allowed_global_error_codes",
    )
    @classmethod
    def _validate_unique_tuple(cls, value: tuple[str, ...], info: Any) -> tuple[str, ...]:
        return _sorted_unique(value, str(info.field_name))

    @field_validator("required_rate_series")
    @classmethod
    def _validate_optional_rate_scope(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        return _sorted_unique(value, "required_rate_series", allow_empty=True)

    @model_validator(mode="after")
    def _validate_governance(self) -> Self:
        if self.schema_version != "data_quality_consumer_capability_policy.v1":
            raise ValueError("unsupported capability policy schema")
        if self.accepted_scoped_statuses != ("PASS",):
            raise ValueError("v1 capability policy accepts exact PASS only")
        if not self.full_canonical_validation_required:
            raise ValueError("full canonical validation must remain required")
        if self.same_validation_code_path != "validate_data_cache":
            raise ValueError("capability must use validate_data_cache")
        if not self.full_status_disclosure_required:
            raise ValueError("full status disclosure must remain required")
        if self.cross_consumer_reuse_allowed or self.daily_operation_authorized:
            raise ValueError("v1 capability cannot authorize reuse or daily operation")
        if self.production_effect != "none" or self.broker_action != "none":
            raise ValueError("capability policy cannot create production or broker effects")
        if self.global_error_attribution_rule != "ALL_AFFECTED_INSTRUMENTS_OUTSIDE_REQUIRED_SCOPE":
            raise ValueError("unsupported global error attribution rule")
        return self


class CapabilityFileBinding(_StrictContractModel):
    role: str
    path: str
    sha256: str
    size_bytes: int = Field(ge=0)
    row_count: int = Field(ge=0)

    @field_validator("role", "path")
    @classmethod
    def _validate_text(cls, value: str, info: Any) -> str:
        return _required_text(value, str(info.field_name))

    @field_validator("sha256")
    @classmethod
    def _validate_sha256(cls, value: str) -> str:
        if not _SHA256_PATTERN.fullmatch(value):
            raise ValueError("sha256 must be lowercase SHA-256")
        return value


class CapabilityIssueBinding(_StrictContractModel):
    severity: str
    code: str
    rows: int | None = Field(default=None, ge=0)
    sample: str | None = None
    source: str | None = None
    affected_instruments: tuple[str, ...] = ()
    isolated_from_capability: bool

    @field_validator("severity", "code")
    @classmethod
    def _validate_text(cls, value: str, info: Any) -> str:
        return _required_text(value, str(info.field_name))

    @field_validator("affected_instruments")
    @classmethod
    def _validate_instruments(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if not value:
            return ()
        return _sorted_unique(value, "affected_instruments")


class CapabilityQualityBinding(_StrictContractModel):
    status: str
    error_count: int = Field(ge=0)
    warning_count: int = Field(ge=0)
    report: CapabilityFileBinding
    issues: tuple[CapabilityIssueBinding, ...]

    @field_validator("status")
    @classmethod
    def _validate_status(cls, value: str) -> str:
        if value not in {"PASS", "PASS_WITH_WARNINGS", "FAIL"}:
            raise ValueError("unsupported DQ status")
        return value

    @model_validator(mode="after")
    def _validate_counts(self) -> Self:
        error_count = sum(item.severity == "ERROR" for item in self.issues)
        warning_count = sum(item.severity == "WARNING" for item in self.issues)
        if self.error_count != error_count or self.warning_count != warning_count:
            raise ValueError("DQ issue counts do not match issue bindings")
        expected = "FAIL" if error_count else ("PASS_WITH_WARNINGS" if warning_count else "PASS")
        if self.status != expected:
            raise ValueError("DQ status does not match issue bindings")
        return self


class ConsumerDataCapabilityReceipt(_StrictContractModel):
    schema_version: str
    policy_id: str
    policy_version: str
    policy_path: str
    policy_sha256: str
    data_quality_policy_path: str
    data_quality_policy_sha256: str
    capability_id: str
    capability_version: str
    consumer_id: str
    consumer_version: str
    requested_start: date
    as_of: date
    generated_at: datetime
    required_price_tickers: tuple[str, ...]
    required_rate_series: tuple[str, ...]
    required_price_fields: tuple[str, ...]
    full_expected_price_tickers: tuple[str, ...]
    full_expected_rate_series: tuple[str, ...]
    full_require_secondary_prices: bool
    canonical_inputs: tuple[CapabilityFileBinding, ...]
    materialized_inputs: tuple[CapabilityFileBinding, ...]
    full_quality: CapabilityQualityBinding
    scoped_quality: CapabilityQualityBinding
    requested_window_authority_id: str | None
    capability_passed: bool
    global_cache_pass_claimed: bool
    isolated_global_error_codes: tuple[str, ...]
    unisolated_global_error_codes: tuple[str, ...]
    cross_consumer_reuse_allowed: bool
    daily_operation_authorized: bool
    production_effect: str
    broker_action: str

    @field_validator(
        "schema_version",
        "policy_id",
        "policy_version",
        "policy_path",
        "data_quality_policy_path",
        "capability_id",
        "capability_version",
        "consumer_id",
        "consumer_version",
        "production_effect",
        "broker_action",
    )
    @classmethod
    def _validate_text(cls, value: str, info: Any) -> str:
        return _required_text(value, str(info.field_name))

    @field_validator("policy_sha256", "data_quality_policy_sha256")
    @classmethod
    def _validate_sha256(cls, value: str) -> str:
        if not _SHA256_PATTERN.fullmatch(value):
            raise ValueError("policy checksum must be lowercase SHA-256")
        return value

    @field_validator(
        "required_price_tickers",
        "required_price_fields",
        "full_expected_price_tickers",
        "full_expected_rate_series",
    )
    @classmethod
    def _validate_unique_tuple(cls, value: tuple[str, ...], info: Any) -> tuple[str, ...]:
        return _sorted_unique(value, str(info.field_name))

    @field_validator("required_rate_series")
    @classmethod
    def _validate_optional_rate_scope(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        return _sorted_unique(value, "required_rate_series", allow_empty=True)

    @field_validator("isolated_global_error_codes", "unisolated_global_error_codes")
    @classmethod
    def _validate_code_tuple(cls, value: tuple[str, ...], info: Any) -> tuple[str, ...]:
        if not value:
            return ()
        return _sorted_unique(value, str(info.field_name))

    @model_validator(mode="after")
    def _validate_receipt_semantics(self) -> Self:
        if self.schema_version != "data_quality_consumer_capability_receipt.v1":
            raise ValueError("unsupported capability receipt schema")
        if self.generated_at.tzinfo is None or self.generated_at.utcoffset() is None:
            raise ValueError("generated_at must be timezone-aware")
        if self.requested_start > self.as_of:
            raise ValueError("requested window is inverted")
        if len({item.role for item in self.canonical_inputs}) != len(self.canonical_inputs):
            raise ValueError("duplicate canonical input role")
        canonical_roles = {item.role for item in self.canonical_inputs}
        if (
            self.full_require_secondary_prices
            and "canonical_secondary_prices" not in canonical_roles
        ):
            raise ValueError("required secondary prices are missing from canonical inputs")
        if len({item.role for item in self.materialized_inputs}) != len(self.materialized_inputs):
            raise ValueError("duplicate materialized input role")
        if set(self.isolated_global_error_codes) & set(self.unisolated_global_error_codes):
            raise ValueError("global error cannot be both isolated and unisolated")
        expected_global_claim = self.full_quality.status == "PASS"
        if self.global_cache_pass_claimed is not expected_global_claim:
            raise ValueError("global cache claim conflicts with full DQ status")
        expected_pass = (
            self.scoped_quality.status == "PASS" and not self.unisolated_global_error_codes
        )
        if self.capability_passed is not expected_pass:
            raise ValueError("capability pass conflicts with scoped/global issue state")
        if self.capability_passed and self.requested_window_authority_id is None:
            raise ValueError("passed capability requires requested-window authority")
        if self.requested_window_authority_id is not None:
            _required_text(self.requested_window_authority_id, "requested_window_authority_id")
        if self.cross_consumer_reuse_allowed or self.daily_operation_authorized:
            raise ValueError("v1 receipt cannot authorize reuse or daily operation")
        if self.production_effect != "none" or self.broker_action != "none":
            raise ValueError("receipt cannot create production or broker effects")
        return self

    def semantic_payload(self) -> dict[str, Any]:
        return self.model_dump(mode="json")

    @property
    def receipt_id(self) -> str:
        material = json.dumps(
            self.semantic_payload(),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
        return f"dq_capability_{hashlib.sha256(material).hexdigest()}"

    def to_dict(self) -> dict[str, Any]:
        return {"receipt_id": self.receipt_id, **self.semantic_payload()}

    @property
    def canonical_bytes(self) -> bytes:
        return (
            json.dumps(
                self.to_dict(),
                ensure_ascii=False,
                sort_keys=True,
                indent=2,
                allow_nan=False,
            )
            + "\n"
        ).encode("utf-8")

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> ConsumerDataCapabilityReceipt:
        copied = dict(payload)
        supplied_id = copied.pop("receipt_id", None)
        try:
            receipt = cls.model_validate(copied)
        except ValueError as exc:
            raise DataQualityCapabilityContractError(
                "DQ_CAPABILITY_RECEIPT_FIELDS_INVALID", str(exc)
            ) from exc
        if supplied_id != receipt.receipt_id:
            raise DataQualityCapabilityContractError(
                "DQ_CAPABILITY_RECEIPT_ID_MISMATCH",
                f"supplied={supplied_id!r} actual={receipt.receipt_id}",
            )
        return receipt

    @classmethod
    def from_json_bytes(cls, content: bytes) -> ConsumerDataCapabilityReceipt:
        try:
            payload = json.loads(content.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise DataQualityCapabilityContractError(
                "DQ_CAPABILITY_RECEIPT_JSON_INVALID", str(exc)
            ) from exc
        if not isinstance(payload, dict):
            raise DataQualityCapabilityContractError(
                "DQ_CAPABILITY_RECEIPT_JSON_INVALID", "receipt root must be an object"
            )
        receipt = cls.from_dict(payload)
        if content != receipt.canonical_bytes:
            raise DataQualityCapabilityContractError(
                "DQ_CAPABILITY_RECEIPT_BYTES_NONCANONICAL",
                receipt.receipt_id,
            )
        return receipt


__all__ = [
    "CapabilityFileBinding",
    "CapabilityIssueBinding",
    "CapabilityQualityBinding",
    "ConsumerDataCapabilityPolicy",
    "ConsumerDataCapabilityReceipt",
    "DataQualityCapabilityContractError",
]
