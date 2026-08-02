from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime
from typing import Any, Literal, Self

from pydantic import BaseModel, ConfigDict, field_validator, model_validator

CapabilityStatus = Literal["CONFIRMED", "UNKNOWN", "CONTRADICTED"]
EvidenceSourceKind = Literal[
    "OFFICIAL_PUBLIC_DOCS",
    "PLATFORM_UI",
    "PLATFORM_ARTIFACT",
    "LICENSE_REVIEW",
    "MANUAL_ATTESTATION",
    "REPOSITORY_REQUIREMENT",
]
ExportClassification = Literal[
    "QC_ONLY_NOT_EXPORTED",
    "EXPORT_ALLOWED_DERIVED",
    "UNKNOWN_REQUIRES_LICENSE_REVIEW",
    "EXPORT_PROHIBITED",
]
AdmissionDecision = Literal[
    "CAPABILITY_CONFIRMED_FOR_BOUNDED_PILOT",
    "CAPABILITY_OR_LICENSE_BLOCKED",
]

_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]*$")
_SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
_TEXT_PATTERN = re.compile(r"^[^\x00-\x1f\x7f]+$")
_RECEIPT_ID_PATTERN = re.compile(r"^qc_qqq_options_admission_[0-9a-f]{64}$")


class QCCapabilityAdmissionContractError(ValueError):
    def __init__(self, code: str, message: str) -> None:
        self.code = code
        self.message = message
        super().__init__(f"{code}: {message}")


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


def _required_text(value: str, field: str) -> str:
    if not value or value != value.strip() or not _TEXT_PATTERN.fullmatch(value):
        raise ValueError(f"{field} must be non-empty normalized text")
    return value


def _identifier(value: str, field: str) -> str:
    checked = _required_text(value, field)
    if not _IDENTIFIER_PATTERN.fullmatch(checked):
        raise ValueError(f"{field} must be a portable identifier")
    return checked


def _sorted_unique_text(values: tuple[str, ...], field: str) -> tuple[str, ...]:
    normalized = tuple(sorted(_identifier(str(item), field) for item in values))
    if not normalized or len(normalized) != len(set(normalized)):
        raise ValueError(f"{field} must contain unique values")
    return normalized


def _aware_datetime(value: datetime, field: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field} must be timezone-aware")
    return value


class QCCapabilitySafetyBoundary(_StrictModel):
    research_only: bool
    manual_review_required: bool
    promotion_allowed: bool
    paper_shadow_allowed: bool
    production_allowed: bool
    raw_options_data_download_allowed: bool
    strategy_execution_allowed: bool
    bounded_cloud_pilot_authorized: bool
    production_effect: str
    broker_action: str

    @field_validator("production_effect", "broker_action")
    @classmethod
    def _validate_text(cls, value: str, info: Any) -> str:
        return _required_text(value, str(info.field_name))

    @model_validator(mode="after")
    def _validate_boundary(self) -> Self:
        if not self.research_only or not self.manual_review_required:
            raise ValueError("capability admission must remain research-only and manually reviewed")
        if any(
            (
                self.promotion_allowed,
                self.paper_shadow_allowed,
                self.production_allowed,
                self.raw_options_data_download_allowed,
                self.strategy_execution_allowed,
                self.bounded_cloud_pilot_authorized,
            )
        ):
            raise ValueError("offline admission cannot authorize execution, export, or promotion")
        if self.production_effect != "none" or self.broker_action != "none":
            raise ValueError("capability admission cannot create production or broker effects")
        return self


class QCFieldExportRule(_StrictModel):
    field_id: str
    required_classification: ExportClassification
    allowed_source_kinds: tuple[EvidenceSourceKind, ...]
    rationale: str

    @field_validator("field_id")
    @classmethod
    def _validate_field_id(cls, value: str) -> str:
        return _identifier(value, "field_id")

    @field_validator("rationale")
    @classmethod
    def _validate_rationale(cls, value: str) -> str:
        return _required_text(value, "rationale")

    @field_validator("allowed_source_kinds")
    @classmethod
    def _validate_source_kinds(
        cls, value: tuple[EvidenceSourceKind, ...]
    ) -> tuple[EvidenceSourceKind, ...]:
        ordered = tuple(sorted(value))
        if not ordered or len(ordered) != len(set(ordered)):
            raise ValueError("allowed_source_kinds must contain unique values")
        return ordered

    @model_validator(mode="after")
    def _validate_admissible_rule(self) -> Self:
        if self.required_classification in {
            "UNKNOWN_REQUIRES_LICENSE_REVIEW",
            "EXPORT_PROHIBITED",
        }:
            raise ValueError("policy rules must define an admissible terminal classification")
        return self


class QCCapabilityItemRule(_StrictModel):
    item_id: str
    allowed_source_kinds: tuple[EvidenceSourceKind, ...]
    rationale: str

    @field_validator("item_id")
    @classmethod
    def _validate_item_id(cls, value: str) -> str:
        return _identifier(value, "item_id")

    @field_validator("allowed_source_kinds")
    @classmethod
    def _validate_source_kinds(
        cls, value: tuple[EvidenceSourceKind, ...]
    ) -> tuple[EvidenceSourceKind, ...]:
        ordered = tuple(sorted(value))
        if not ordered or len(ordered) != len(set(ordered)):
            raise ValueError("allowed_source_kinds must contain unique values")
        return ordered

    @field_validator("rationale")
    @classmethod
    def _validate_rationale(cls, value: str) -> str:
        return _required_text(value, "rationale")


class QCCapabilityAdmissionPolicy(_StrictModel):
    schema_version: str
    policy_id: str
    policy_version: str
    status: str
    owner: str
    required_owner_authorization_id: str
    owner_authorization_required: bool
    platform: str
    item_rules: tuple[QCCapabilityItemRule, ...]
    field_export_rules: tuple[QCFieldExportRule, ...]
    allowed_evidence_source_kinds: tuple[EvidenceSourceKind, ...]
    confirmed_decision: Literal["CAPABILITY_CONFIRMED_FOR_BOUNDED_PILOT"]
    blocked_decision: Literal["CAPABILITY_OR_LICENSE_BLOCKED"]
    safety: QCCapabilitySafetyBoundary
    review_condition: str

    @field_validator(
        "schema_version",
        "policy_id",
        "policy_version",
        "status",
        "owner",
        "required_owner_authorization_id",
        "platform",
        "confirmed_decision",
        "blocked_decision",
        "review_condition",
    )
    @classmethod
    def _validate_text(cls, value: str, info: Any) -> str:
        return _required_text(value, str(info.field_name))

    @field_validator("item_rules")
    @classmethod
    def _validate_item_rules(
        cls, value: tuple[QCCapabilityItemRule, ...]
    ) -> tuple[QCCapabilityItemRule, ...]:
        ordered = tuple(sorted(value, key=lambda item: item.item_id))
        if not ordered or len({item.item_id for item in ordered}) != len(ordered):
            raise ValueError("item_rules must contain unique item ids")
        return ordered

    @field_validator("field_export_rules")
    @classmethod
    def _validate_field_rules(
        cls, value: tuple[QCFieldExportRule, ...]
    ) -> tuple[QCFieldExportRule, ...]:
        ordered = tuple(sorted(value, key=lambda item: item.field_id))
        if not ordered or len({item.field_id for item in ordered}) != len(ordered):
            raise ValueError("field_export_rules must contain unique field ids")
        return ordered

    @field_validator("allowed_evidence_source_kinds")
    @classmethod
    def _validate_source_kinds(
        cls, value: tuple[EvidenceSourceKind, ...]
    ) -> tuple[EvidenceSourceKind, ...]:
        ordered = tuple(sorted(value))
        if not ordered or len(ordered) != len(set(ordered)):
            raise ValueError("allowed_evidence_source_kinds must contain unique values")
        return ordered

    @model_validator(mode="after")
    def _validate_policy(self) -> Self:
        if self.schema_version != "qc_qqq_options_capability_admission_policy.v1":
            raise ValueError("unsupported capability admission policy schema")
        if self.status != "PREAUTHORIZATION_BASELINE":
            raise ValueError("v1 policy must remain a preauthorization baseline")
        if self.platform != "QuantConnect":
            raise ValueError("v1 policy is scoped to QuantConnect")
        if not self.owner_authorization_required:
            raise ValueError("external capability probe must require Owner authorization")
        if self.confirmed_decision != "CAPABILITY_CONFIRMED_FOR_BOUNDED_PILOT":
            raise ValueError("unsupported confirmed admission decision")
        if self.blocked_decision != "CAPABILITY_OR_LICENSE_BLOCKED":
            raise ValueError("unsupported blocked admission decision")
        return self


class QCCapabilityEvidenceItem(_StrictModel):
    item_id: str
    status: CapabilityStatus
    source_kind: EvidenceSourceKind
    source_locator: str
    recorded_at: datetime
    recorded_by: str
    summary: str
    exit_condition: str | None = None

    @field_validator("item_id")
    @classmethod
    def _validate_item_id(cls, value: str) -> str:
        return _identifier(value, "item_id")

    @field_validator("source_locator", "recorded_by", "summary")
    @classmethod
    def _validate_text(cls, value: str, info: Any) -> str:
        return _required_text(value, str(info.field_name))

    @field_validator("recorded_at")
    @classmethod
    def _validate_recorded_at(cls, value: datetime) -> datetime:
        return _aware_datetime(value, "recorded_at")

    @field_validator("exit_condition")
    @classmethod
    def _validate_exit_condition(cls, value: str | None) -> str | None:
        return None if value is None else _required_text(value, "exit_condition")

    @model_validator(mode="after")
    def _validate_disposition(self) -> Self:
        if self.status == "CONFIRMED" and self.exit_condition is not None:
            raise ValueError("confirmed evidence must not retain an exit condition")
        if self.status != "CONFIRMED" and self.exit_condition is None:
            raise ValueError("unconfirmed evidence requires an exit condition")
        return self


class QCFieldExportEvidence(_StrictModel):
    field_id: str
    status: CapabilityStatus
    export_classification: ExportClassification
    source_kind: EvidenceSourceKind
    source_locator: str
    recorded_at: datetime
    recorded_by: str
    summary: str
    exit_condition: str | None = None
    raw_rows_embedded: bool

    @field_validator("field_id")
    @classmethod
    def _validate_field_id(cls, value: str) -> str:
        return _identifier(value, "field_id")

    @field_validator("source_locator", "recorded_by", "summary")
    @classmethod
    def _validate_text(cls, value: str, info: Any) -> str:
        return _required_text(value, str(info.field_name))

    @field_validator("recorded_at")
    @classmethod
    def _validate_recorded_at(cls, value: datetime) -> datetime:
        return _aware_datetime(value, "recorded_at")

    @field_validator("exit_condition")
    @classmethod
    def _validate_exit_condition(cls, value: str | None) -> str | None:
        return None if value is None else _required_text(value, "exit_condition")

    @model_validator(mode="after")
    def _validate_disposition(self) -> Self:
        if self.raw_rows_embedded:
            raise ValueError("field export evidence cannot embed raw rows")
        if self.status == "CONFIRMED" and self.exit_condition is not None:
            raise ValueError("confirmed field evidence must not retain an exit condition")
        if self.status != "CONFIRMED" and self.exit_condition is None:
            raise ValueError("unconfirmed field evidence requires an exit condition")
        return self


class QCCapabilityEvidence(_StrictModel):
    schema_version: str
    probe_id: str
    platform: str
    captured_at: datetime
    external_action_authorized: bool
    owner_authorization_id: str | None
    items: tuple[QCCapabilityEvidenceItem, ...]
    field_exports: tuple[QCFieldExportEvidence, ...]
    raw_options_data_included: bool
    investment_metrics_included: bool
    account_or_broker_identifiers_included: bool
    safety: QCCapabilitySafetyBoundary

    @field_validator("schema_version", "probe_id", "platform")
    @classmethod
    def _validate_text(cls, value: str, info: Any) -> str:
        field = str(info.field_name)
        return _identifier(value, field) if field == "probe_id" else _required_text(value, field)

    @field_validator("captured_at")
    @classmethod
    def _validate_captured_at(cls, value: datetime) -> datetime:
        return _aware_datetime(value, "captured_at")

    @field_validator("owner_authorization_id")
    @classmethod
    def _validate_authorization_id(cls, value: str | None) -> str | None:
        return None if value is None else _required_text(value, "owner_authorization_id")

    @field_validator("items")
    @classmethod
    def _validate_items(
        cls, value: tuple[QCCapabilityEvidenceItem, ...]
    ) -> tuple[QCCapabilityEvidenceItem, ...]:
        ordered = tuple(sorted(value, key=lambda item: item.item_id))
        if not ordered or len({item.item_id for item in ordered}) != len(ordered):
            raise ValueError("items must contain unique item ids")
        return ordered

    @field_validator("field_exports")
    @classmethod
    def _validate_field_exports(
        cls, value: tuple[QCFieldExportEvidence, ...]
    ) -> tuple[QCFieldExportEvidence, ...]:
        ordered = tuple(sorted(value, key=lambda item: item.field_id))
        if not ordered or len({item.field_id for item in ordered}) != len(ordered):
            raise ValueError("field_exports must contain unique field ids")
        return ordered

    @model_validator(mode="after")
    def _validate_evidence(self) -> Self:
        if self.schema_version != "qc_qqq_options_capability_evidence.v1":
            raise ValueError("unsupported capability evidence schema")
        if self.platform != "QuantConnect":
            raise ValueError("v1 evidence is scoped to QuantConnect")
        if self.external_action_authorized != (self.owner_authorization_id is not None):
            raise ValueError("external action authorization flag and id must agree")
        if any(
            (
                self.raw_options_data_included,
                self.investment_metrics_included,
                self.account_or_broker_identifiers_included,
            )
        ):
            raise ValueError("capability evidence contains prohibited content")
        if any(item.recorded_at > self.captured_at for item in self.items):
            raise ValueError("item evidence cannot be recorded after capture")
        if any(item.recorded_at > self.captured_at for item in self.field_exports):
            raise ValueError("field evidence cannot be recorded after capture")
        return self

    def semantic_payload(self) -> dict[str, Any]:
        return self.model_dump(mode="json")

    @property
    def canonical_bytes(self) -> bytes:
        return _canonical_json_bytes(self.semantic_payload())

    @classmethod
    def from_json_bytes(cls, content: bytes) -> Self:
        try:
            evidence = cls.model_validate_json(content)
        except ValueError as exc:
            raise QCCapabilityAdmissionContractError(
                "QC_CAPABILITY_EVIDENCE_INVALID", str(exc)
            ) from exc
        if content != evidence.canonical_bytes:
            raise QCCapabilityAdmissionContractError(
                "QC_CAPABILITY_EVIDENCE_NOT_CANONICAL",
                "evidence bytes do not match canonical JSON encoding",
            )
        return evidence


class QCCapabilityAdmissionReceipt(_StrictModel):
    schema_version: str
    policy_id: str
    policy_version: str
    policy_sha256: str
    evidence_probe_id: str
    evidence_sha256: str
    evaluated_at: datetime
    decision: AdmissionDecision
    blocking_reason_codes: tuple[str, ...]
    confirmed_item_count: int
    required_item_count: int
    confirmed_field_count: int
    required_field_count: int
    bounded_pilot_preparation_allowed: bool
    safety: QCCapabilitySafetyBoundary

    @field_validator(
        "schema_version",
        "policy_id",
        "policy_version",
        "evidence_probe_id",
    )
    @classmethod
    def _validate_text(cls, value: str, info: Any) -> str:
        return _required_text(value, str(info.field_name))

    @field_validator("policy_sha256", "evidence_sha256")
    @classmethod
    def _validate_sha256(cls, value: str) -> str:
        if not _SHA256_PATTERN.fullmatch(value):
            raise ValueError("receipt hashes must be lowercase SHA-256")
        return value

    @field_validator("evaluated_at")
    @classmethod
    def _validate_evaluated_at(cls, value: datetime) -> datetime:
        return _aware_datetime(value, "evaluated_at")

    @field_validator("blocking_reason_codes")
    @classmethod
    def _validate_reason_codes(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        ordered = tuple(sorted(_required_text(item, "blocking_reason_codes") for item in value))
        if len(ordered) != len(set(ordered)):
            raise ValueError("blocking_reason_codes must contain unique values")
        return ordered

    @model_validator(mode="after")
    def _validate_decision(self) -> Self:
        if self.schema_version != "qc_qqq_options_capability_admission_receipt.v1":
            raise ValueError("unsupported capability admission receipt schema")
        if (
            min(
                self.confirmed_item_count,
                self.required_item_count,
                self.confirmed_field_count,
                self.required_field_count,
            )
            < 0
        ):
            raise ValueError("receipt counts cannot be negative")
        admitted = not self.blocking_reason_codes
        expected_decision = (
            "CAPABILITY_CONFIRMED_FOR_BOUNDED_PILOT"
            if admitted
            else "CAPABILITY_OR_LICENSE_BLOCKED"
        )
        if self.decision != expected_decision:
            raise ValueError("decision does not match blocking reasons")
        if self.bounded_pilot_preparation_allowed != admitted:
            raise ValueError("bounded pilot preparation flag does not match decision")
        if admitted and (
            self.confirmed_item_count != self.required_item_count
            or self.confirmed_field_count != self.required_field_count
        ):
            raise ValueError("admitted receipt must confirm every required item and field")
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
        return f"qc_qqq_options_admission_{hashlib.sha256(material).hexdigest()}"

    def to_dict(self) -> dict[str, Any]:
        return {"receipt_id": self.receipt_id, **self.semantic_payload()}

    @property
    def canonical_bytes(self) -> bytes:
        return _canonical_json_bytes(self.to_dict())

    @classmethod
    def from_json_bytes(cls, content: bytes) -> Self:
        try:
            payload = json.loads(content)
            if not isinstance(payload, dict):
                raise TypeError("receipt root must be an object")
            receipt_id = payload.pop("receipt_id", None)
            receipt = cls.model_validate(payload)
        except (TypeError, ValueError) as exc:
            raise QCCapabilityAdmissionContractError(
                "QC_CAPABILITY_RECEIPT_INVALID", str(exc)
            ) from exc
        if not isinstance(receipt_id, str) or not _RECEIPT_ID_PATTERN.fullmatch(receipt_id):
            raise QCCapabilityAdmissionContractError(
                "QC_CAPABILITY_RECEIPT_ID_INVALID", "receipt id is malformed"
            )
        if receipt_id != receipt.receipt_id:
            raise QCCapabilityAdmissionContractError(
                "QC_CAPABILITY_RECEIPT_ID_MISMATCH", "receipt id does not match content"
            )
        if content != receipt.canonical_bytes:
            raise QCCapabilityAdmissionContractError(
                "QC_CAPABILITY_RECEIPT_NOT_CANONICAL",
                "receipt bytes do not match canonical JSON encoding",
            )
        return receipt


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
