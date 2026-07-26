from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import PurePosixPath
from typing import Any, ClassVar, Self

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

_SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
_TEXT_PATTERN = re.compile(r"^[^\x00-\x1f\x7f]+$")
_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]*$")
_RECEIPT_ID_PATTERN = re.compile(r"^dq_capability_[0-9a-f]{64}$")
_DEPENDENCY_ID_PATTERN = re.compile(r"^dq_dependency_[0-9a-f]{64}$")
_VERIFIED_CAPABILITY_PREFLIGHT_SEAL = object()


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


class ConsumerDataCapabilityDependency(_StrictContractModel):
    schema_version: str
    consumer_id: str
    consumer_version: str
    capability_id: str
    capability_version: str
    capability_policy_path: str
    capability_policy_sha256: str
    data_quality_policy_path: str
    data_quality_policy_sha256: str
    accepted_receipt_schema_version: str
    strict_pass_required: bool
    cross_consumer_reuse_allowed: bool
    daily_operation_authorized: bool
    production_effect: str
    broker_action: str

    @field_validator(
        "schema_version",
        "consumer_id",
        "consumer_version",
        "capability_id",
        "capability_version",
        "accepted_receipt_schema_version",
        "production_effect",
        "broker_action",
    )
    @classmethod
    def _validate_text(cls, value: str, info: Any) -> str:
        checked = _required_text(value, str(info.field_name))
        if str(info.field_name) in {
            "consumer_id",
            "consumer_version",
            "capability_id",
            "capability_version",
        } and not _IDENTIFIER_PATTERN.fullmatch(checked):
            raise ValueError(f"{info.field_name} must be a portable identifier")
        return checked

    @field_validator("capability_policy_path", "data_quality_policy_path")
    @classmethod
    def _validate_policy_path(cls, value: str, info: Any) -> str:
        return _repo_relative_posix_path(value, str(info.field_name))

    @field_validator("capability_policy_sha256", "data_quality_policy_sha256")
    @classmethod
    def _validate_policy_sha256(cls, value: str) -> str:
        if not _SHA256_PATTERN.fullmatch(value):
            raise ValueError("policy checksum must be lowercase SHA-256")
        return value

    @model_validator(mode="after")
    def _validate_governance(self) -> Self:
        if self.schema_version != "data_quality_consumer_dependency.v1":
            raise ValueError("unsupported consumer dependency schema")
        if self.accepted_receipt_schema_version != ("data_quality_consumer_capability_receipt.v1"):
            raise ValueError("unsupported accepted receipt schema")
        if not self.strict_pass_required:
            raise ValueError("consumer dependency must require strict PASS")
        if self.cross_consumer_reuse_allowed or self.daily_operation_authorized:
            raise ValueError(
                "generic research dependency cannot authorize reuse or daily operation"
            )
        if self.production_effect != "none" or self.broker_action != "none":
            raise ValueError("consumer dependency cannot create production or broker effects")
        return self

    def semantic_payload(self) -> dict[str, Any]:
        return self.model_dump(mode="json")

    @property
    def dependency_id(self) -> str:
        material = json.dumps(
            self.semantic_payload(),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
        return f"dq_dependency_{hashlib.sha256(material).hexdigest()}"

    def to_dict(self) -> dict[str, Any]:
        return {"dependency_id": self.dependency_id, **self.semantic_payload()}

    @property
    def canonical_bytes(self) -> bytes:
        return _canonical_json_bytes(self.to_dict())


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


class ConsumerDataCapabilityDiscoveryPointer(_StrictContractModel):
    schema_version: str
    dependency_id: str
    consumer_id: str
    consumer_version: str
    capability_id: str
    capability_version: str
    as_of: date
    published_at: datetime
    receipt_id: str
    receipt_path: str
    receipt_sha256: str
    receipt_size_bytes: int = Field(gt=0)

    @field_validator(
        "schema_version",
        "consumer_id",
        "consumer_version",
        "capability_id",
        "capability_version",
    )
    @classmethod
    def _validate_text(cls, value: str, info: Any) -> str:
        checked = _required_text(value, str(info.field_name))
        if str(info.field_name) != "schema_version" and not _IDENTIFIER_PATTERN.fullmatch(checked):
            raise ValueError(f"{info.field_name} must be a portable identifier")
        return checked

    @field_validator("dependency_id")
    @classmethod
    def _validate_dependency_id(cls, value: str) -> str:
        if not _DEPENDENCY_ID_PATTERN.fullmatch(value):
            raise ValueError("dependency_id is invalid")
        return value

    @field_validator("receipt_id")
    @classmethod
    def _validate_receipt_id(cls, value: str) -> str:
        if not _RECEIPT_ID_PATTERN.fullmatch(value):
            raise ValueError("receipt_id is invalid")
        return value

    @field_validator("receipt_path")
    @classmethod
    def _validate_receipt_path(cls, value: str) -> str:
        checked = _repo_relative_posix_path(value, "receipt_path")
        expected = f"outputs/data_quality/capabilities/receipts/{PurePosixPath(checked).name}"
        if checked != expected:
            raise ValueError("receipt_path is not the canonical retained-receipt path")
        return checked

    @field_validator("receipt_sha256")
    @classmethod
    def _validate_receipt_sha256(cls, value: str) -> str:
        if not _SHA256_PATTERN.fullmatch(value):
            raise ValueError("receipt_sha256 must be lowercase SHA-256")
        return value

    @model_validator(mode="after")
    def _validate_pointer(self) -> Self:
        if self.schema_version != "data_quality_consumer_capability_discovery_pointer.v1":
            raise ValueError("unsupported capability discovery pointer schema")
        if self.published_at.tzinfo is None or self.published_at.utcoffset() is None:
            raise ValueError("published_at must be timezone-aware")
        if self.published_at.utcoffset() != timedelta(0):
            raise ValueError("published_at must be UTC")
        if PurePosixPath(self.receipt_path).name != f"{self.receipt_id}.json":
            raise ValueError("receipt_path filename does not match receipt_id")
        return self

    def to_dict(self) -> dict[str, Any]:
        return self.model_dump(mode="json")

    @property
    def canonical_bytes(self) -> bytes:
        return _canonical_json_bytes(self.to_dict())

    @classmethod
    def from_json_bytes(cls, content: bytes) -> ConsumerDataCapabilityDiscoveryPointer:
        payload = _strict_json_mapping(content, "DQ_CAPABILITY_DISCOVERY_JSON_INVALID")
        try:
            pointer = cls.model_validate(payload)
        except ValueError as exc:
            raise DataQualityCapabilityContractError(
                "DQ_CAPABILITY_DISCOVERY_FIELDS_INVALID", str(exc)
            ) from exc
        if content != pointer.canonical_bytes:
            raise DataQualityCapabilityContractError(
                "DQ_CAPABILITY_DISCOVERY_BYTES_NONCANONICAL",
                pointer.receipt_id,
            )
        return pointer


@dataclass(frozen=True, init=False)
class VerifiedConsumerDataCapabilityPreflight:
    schema_version: ClassVar[str] = "verified_consumer_data_capability_preflight.v1"

    dependency: ConsumerDataCapabilityDependency
    receipt: ConsumerDataCapabilityReceipt
    pointer_path: str
    pointer_sha256: str
    receipt_path: str
    receipt_sha256: str
    receipt_size_bytes: int
    verified_at: datetime

    def __init__(
        self,
        *,
        dependency: ConsumerDataCapabilityDependency,
        receipt: ConsumerDataCapabilityReceipt,
        pointer_path: str,
        pointer_sha256: str,
        receipt_path: str,
        receipt_sha256: str,
        receipt_size_bytes: int,
        verified_at: datetime,
        _verification_seal: object,
    ) -> None:
        if _verification_seal is not _VERIFIED_CAPABILITY_PREFLIGHT_SEAL:
            raise DataQualityCapabilityContractError(
                "DQ_CAPABILITY_NOT_VERIFIED",
                "preflight must be created by the canonical capability verifier",
            )
        if not isinstance(dependency, ConsumerDataCapabilityDependency) or not isinstance(
            receipt, ConsumerDataCapabilityReceipt
        ):
            raise DataQualityCapabilityContractError(
                "DQ_CAPABILITY_NOT_VERIFIED", "typed dependency and receipt are required"
            )
        _assert_dependency_receipt_match(dependency, receipt)
        if (
            not receipt.capability_passed
            or receipt.scoped_quality.status != "PASS"
            or receipt.unisolated_global_error_codes
        ):
            raise DataQualityCapabilityContractError(
                "DQ_CAPABILITY_STRICT_PASS_REQUIRED", receipt.receipt_id
            )
        checked_pointer_path = _repo_relative_posix_path(pointer_path, "pointer_path")
        checked_receipt_path = _repo_relative_posix_path(receipt_path, "receipt_path")
        expected_receipt_path = (
            f"outputs/data_quality/capabilities/receipts/{receipt.receipt_id}.json"
        )
        if checked_receipt_path != expected_receipt_path:
            raise DataQualityCapabilityContractError(
                "DQ_CAPABILITY_RECEIPT_PATH_MISMATCH", checked_receipt_path
            )
        for value, field in (
            (pointer_sha256, "pointer_sha256"),
            (receipt_sha256, "receipt_sha256"),
        ):
            if not _SHA256_PATTERN.fullmatch(value):
                raise DataQualityCapabilityContractError(
                    "DQ_CAPABILITY_FILE_BINDING_MISMATCH", field
                )
        if receipt_sha256 != hashlib.sha256(receipt.canonical_bytes).hexdigest():
            raise DataQualityCapabilityContractError(
                "DQ_CAPABILITY_FILE_BINDING_MISMATCH", "retained receipt checksum differs"
            )
        if receipt_size_bytes != len(receipt.canonical_bytes):
            raise DataQualityCapabilityContractError(
                "DQ_CAPABILITY_FILE_BINDING_MISMATCH", "retained receipt size differs"
            )
        if verified_at.tzinfo is None or verified_at.utcoffset() is None:
            raise DataQualityCapabilityContractError(
                "DQ_CAPABILITY_CHRONOLOGY_INVALID", "verified_at must be timezone-aware"
            )
        if verified_at < receipt.generated_at:
            raise DataQualityCapabilityContractError(
                "DQ_CAPABILITY_CHRONOLOGY_INVALID",
                "verification cannot precede receipt generation",
            )
        object.__setattr__(self, "dependency", dependency)
        object.__setattr__(self, "receipt", receipt)
        object.__setattr__(self, "pointer_path", checked_pointer_path)
        object.__setattr__(self, "pointer_sha256", pointer_sha256)
        object.__setattr__(self, "receipt_path", checked_receipt_path)
        object.__setattr__(self, "receipt_sha256", receipt_sha256)
        object.__setattr__(self, "receipt_size_bytes", receipt_size_bytes)
        object.__setattr__(self, "verified_at", verified_at)

    @property
    def consumer_id(self) -> str:
        return self.receipt.consumer_id

    @property
    def consumer_version(self) -> str:
        return self.receipt.consumer_version

    @property
    def capability_id(self) -> str:
        return self.receipt.capability_id

    @property
    def as_of(self) -> date:
        return self.receipt.as_of


def _build_verified_consumer_data_capability_preflight(
    *,
    dependency: ConsumerDataCapabilityDependency,
    receipt: ConsumerDataCapabilityReceipt,
    pointer_path: str,
    pointer_sha256: str,
    receipt_path: str,
    receipt_sha256: str,
    receipt_size_bytes: int,
    verified_at: datetime,
) -> VerifiedConsumerDataCapabilityPreflight:
    """Verifier-only factory; receipt parsing alone is not verification."""

    return VerifiedConsumerDataCapabilityPreflight(
        dependency=dependency,
        receipt=receipt,
        pointer_path=pointer_path,
        pointer_sha256=pointer_sha256,
        receipt_path=receipt_path,
        receipt_sha256=receipt_sha256,
        receipt_size_bytes=receipt_size_bytes,
        verified_at=verified_at,
        _verification_seal=_VERIFIED_CAPABILITY_PREFLIGHT_SEAL,
    )


def _assert_dependency_receipt_match(
    dependency: ConsumerDataCapabilityDependency,
    receipt: ConsumerDataCapabilityReceipt,
) -> None:
    if (
        receipt.schema_version != dependency.accepted_receipt_schema_version
        or receipt.consumer_id != dependency.consumer_id
        or receipt.consumer_version != dependency.consumer_version
        or receipt.capability_id != dependency.capability_id
        or receipt.capability_version != dependency.capability_version
        or receipt.policy_sha256 != dependency.capability_policy_sha256
        or receipt.data_quality_policy_sha256 != dependency.data_quality_policy_sha256
        or receipt.cross_consumer_reuse_allowed != dependency.cross_consumer_reuse_allowed
        or receipt.daily_operation_authorized != dependency.daily_operation_authorized
        or receipt.production_effect != dependency.production_effect
        or receipt.broker_action != dependency.broker_action
    ):
        raise DataQualityCapabilityContractError(
            "DQ_CAPABILITY_DEPENDENCY_MISMATCH", receipt.receipt_id
        )


def _repo_relative_posix_path(value: str, field: str) -> str:
    checked = _required_text(value, field)
    parsed = PurePosixPath(checked)
    if (
        "\\" in checked
        or checked.startswith("/")
        or re.match(r"^[A-Za-z]:", checked)
        or any(part in {"", ".", ".."} for part in parsed.parts)
        or str(parsed) != checked
    ):
        raise ValueError(f"{field} must be a normalized repo-relative POSIX path")
    return checked


def _canonical_json_bytes(payload: dict[str, Any]) -> bytes:
    return (
        json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            indent=2,
            allow_nan=False,
        )
        + "\n"
    ).encode("utf-8")


def _strict_json_mapping(content: bytes, code: str) -> dict[str, Any]:
    try:
        decoded = content.decode("utf-8")
        payload = json.loads(
            decoded,
            object_pairs_hook=_strict_json_object_pairs,
            parse_constant=_reject_json_constant,
        )
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise DataQualityCapabilityContractError(code, str(exc)) from exc
    if not isinstance(payload, dict):
        raise DataQualityCapabilityContractError(code, "JSON root must be an object")
    return payload


def _strict_json_object_pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    for key, value in pairs:
        if key in payload:
            raise DataQualityCapabilityContractError(
                "DQ_CAPABILITY_DISCOVERY_JSON_INVALID", f"duplicate JSON key: {key}"
            )
        payload[key] = value
    return payload


def _reject_json_constant(value: str) -> None:
    raise DataQualityCapabilityContractError(
        "DQ_CAPABILITY_DISCOVERY_JSON_INVALID", f"non-finite JSON constant: {value}"
    )


__all__ = [
    "CapabilityFileBinding",
    "CapabilityIssueBinding",
    "CapabilityQualityBinding",
    "ConsumerDataCapabilityDependency",
    "ConsumerDataCapabilityDiscoveryPointer",
    "ConsumerDataCapabilityPolicy",
    "ConsumerDataCapabilityReceipt",
    "DataQualityCapabilityContractError",
    "VerifiedConsumerDataCapabilityPreflight",
]
