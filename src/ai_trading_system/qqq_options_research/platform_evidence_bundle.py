from __future__ import annotations

import csv
import hashlib
import io
import json
import os
import re
import stat
import zipfile
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Literal, Self

from pydantic import BaseModel, ConfigDict, ValidationInfo, field_validator, model_validator

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.platform.artifacts import sha256_path
from ai_trading_system.qqq_options_capability_admission import (
    DEFAULT_QC_QQQ_OPTIONS_CAPABILITY_EVIDENCE_TEMPLATE_PATH,
    DEFAULT_QC_QQQ_OPTIONS_CAPABILITY_POLICY_PATH,
    verify_qc_qqq_options_capability_admission_receipt,
)
from ai_trading_system.qqq_options_research.contracts import (
    QQQ_OPTIONS_CONTRACT_SHA256,
    EvidenceArtifact,
    PlatformEvidenceManifestRecord,
    QQQOptionsSafetyBoundary,
)
from ai_trading_system.qqq_options_research.qc_project_adapter import (
    DEFAULT_QC_QQQ_OPTIONS_PROJECT_ADAPTER_POLICY_PATH,
    load_qc_qqq_options_project_adapter_policy,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

DEFAULT_QC_QQQ_OPTIONS_PLATFORM_EVIDENCE_BUNDLE_POLICY_PATH = Path(
    "config/research/qc_qqq_options_platform_evidence_manual_bundle_v1.yaml"
)

_SHARED_POLICY_PATH = Path("config/research/qqq_options_shared_contract_v1.yaml")
_DQ_PIT_POLICY_PATH = Path("config/research/qqq_options_dq_pit_identity_v1.yaml")
_UNSEALED_SHA256 = "0" * 64
_SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
_GIT_SHA_PATTERN = re.compile(r"^(?:[0-9a-f]{40}|[0-9a-f]{64})$")
_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]*$")
_OWNER_TOKEN_PREFIX = "owner_decision:TRADING-2492:"
_NOT_GRANTED_TOKEN = "NOT_GRANTED_FOR_EXTERNAL_PLATFORM_ACTIONS"
_EXPECTED_DIRECTORIES = ("artifacts", "attestations")
_EXPECTED_INVENTORY = (
    "artifact_index.json",
    "artifacts/logs.txt",
    "artifacts/orders.csv",
    "artifacts/platform_ui.png",
    "artifacts/project_files.zip",
    "artifacts/report.pdf",
    "artifacts/results.json",
    "artifacts/trades.csv",
    "attestations/collector.json",
    "attestations/independent_reviewer.json",
    "bundle_metadata.json",
    "platform_evidence_manifest.json",
)
_EXPECTED_ARTIFACTS = (
    ("logs", "artifacts/logs.txt", "Logs", "ALGORITHM_TIMEZONE", "text/plain", True),
    ("orders_csv", "artifacts/orders.csv", "Orders CSV", "UTC", "text/csv", True),
    (
        "platform_ui",
        "artifacts/platform_ui.png",
        "Platform UI Screenshot",
        "UTC",
        "image/png",
        False,
    ),
    (
        "project_files",
        "artifacts/project_files.zip",
        "Project Files",
        "NOT_APPLICABLE",
        "application/zip",
        False,
    ),
    (
        "report_pdf",
        "artifacts/report.pdf",
        "Report PDF",
        "MIXED_DECLARED_BY_ARTIFACT",
        "application/pdf",
        False,
    ),
    (
        "results_json",
        "artifacts/results.json",
        "Results JSON",
        "UTC",
        "application/json",
        True,
    ),
    ("trades_csv", "artifacts/trades.csv", "Trades CSV", "UTC", "text/csv", True),
)
_EXPECTED_TEXT_SECURITY_MARKERS = (
    "account_id",
    "api_key",
    "authorization_bearer",
    "brokerage_account",
    "client_secret",
    "open_interest_rows",
    "option_chain_rows",
    "option_quote_rows",
    "private_key",
    "refresh_token",
)
_EXPECTED_ENGINE_FIELDS = (
    "adapter_descriptor_sha256",
    "algorithm_language",
    "backtest_id",
    "evaluated_end",
    "evaluated_start",
    "lean_engine_identity",
    "project_id",
    "repository_code_sha",
    "requested_end",
    "requested_start",
    "resource_runtime_telemetry",
)


class QCPlatformEvidenceBundleContractError(ValueError):
    def __init__(self, code: str, message: str) -> None:
        self.code = code
        self.message = message
        super().__init__(f"{code}: {message}")


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


def _required_text(value: str, field: str) -> str:
    if not value or value != value.strip() or any(ord(char) < 32 for char in value):
        raise ValueError(f"{field} must be non-empty normalized text")
    return value


def _identifier(value: str, field: str) -> str:
    checked = _required_text(value, field)
    if not _IDENTIFIER_PATTERN.fullmatch(checked):
        raise ValueError(f"{field} must be a stable identifier")
    return checked


def _sha256(value: str, field: str) -> str:
    if not _SHA256_PATTERN.fullmatch(value):
        raise ValueError(f"{field} must be lowercase SHA-256")
    return value


def _git_sha(value: str, field: str) -> str:
    if not _GIT_SHA_PATTERN.fullmatch(value):
        raise ValueError(f"{field} must be a lowercase Git object id")
    return value


def _utc(value: datetime, field: str) -> datetime:
    offset = value.utcoffset()
    if value.tzinfo is None or offset is None:
        raise ValueError(f"{field} must be timezone-aware")
    if offset.total_seconds() != 0:
        raise ValueError(f"{field} must use UTC")
    return value


def _canonical_json_bytes(payload: dict[str, Any]) -> bytes:
    return (
        json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            indent=2,
            allow_nan=False,
        ).encode("utf-8")
        + b"\n"
    )


def _content_sha256(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


class _SealedModel(_StrictModel):
    content_sha256: str

    @field_validator("content_sha256")
    @classmethod
    def _validate_content_hash(cls, value: str) -> str:
        return _sha256(value, "content_sha256")

    @model_validator(mode="after")
    def _validate_seal(self, info: ValidationInfo) -> Self:
        allow_unsealed = bool(info.context and info.context.get("allow_unsealed") is True)
        if allow_unsealed and self.content_sha256 == _UNSEALED_SHA256:
            return self
        if self.content_sha256 != self.compute_content_sha256():
            raise ValueError("content SHA-256 does not match canonical semantics")
        return self

    def semantic_payload_without_hash(self) -> dict[str, Any]:
        return self.model_dump(mode="json", exclude={"content_sha256"})

    def compute_content_sha256(self) -> str:
        return _content_sha256(_canonical_json_bytes(self.semantic_payload_without_hash()))

    @classmethod
    def seal(cls, **payload: Any) -> Self:
        if "content_sha256" in payload:
            raise QCPlatformEvidenceBundleContractError(
                "QC_PLATFORM_EVIDENCE_HASH_CALLER_SUPPLIED",
                "seal computes content_sha256 and rejects caller-supplied values",
            )
        provisional = cls.model_validate(
            {**payload, "content_sha256": _UNSEALED_SHA256},
            context={"allow_unsealed": True},
        )
        return cls.model_validate(
            {**payload, "content_sha256": provisional.compute_content_sha256()}
        )

    @property
    def canonical_bytes(self) -> bytes:
        return _canonical_json_bytes(self.model_dump(mode="json"))

    @property
    def canonical_sha256(self) -> str:
        return _content_sha256(self.canonical_bytes)

    @classmethod
    def from_json_bytes(cls, content: bytes) -> Self:
        try:
            record = cls.model_validate_json(content)
        except ValueError as exc:
            raise QCPlatformEvidenceBundleContractError(
                "QC_PLATFORM_EVIDENCE_RECORD_INVALID", str(exc)
            ) from exc
        if content != record.canonical_bytes:
            raise QCPlatformEvidenceBundleContractError(
                "QC_PLATFORM_EVIDENCE_RECORD_NOT_CANONICAL",
                "record bytes do not match canonical JSON encoding",
            )
        return record


class QCManualArtifactRule(_StrictModel):
    artifact_id: str
    locator: str
    platform_artifact: str
    timestamp_semantics: Literal[
        "UTC", "ALGORITHM_TIMEZONE", "NOT_APPLICABLE", "MIXED_DECLARED_BY_ARTIFACT"
    ]
    media_type: Literal[
        "text/plain",
        "text/csv",
        "image/png",
        "application/zip",
        "application/pdf",
        "application/json",
    ]
    maximum_bytes: int
    text_security_scan_required: bool
    export_classification: Literal["EXPORT_ALLOWED_DERIVED"]

    @field_validator("artifact_id")
    @classmethod
    def _validate_id(cls, value: str) -> str:
        return _identifier(value, "artifact_id")

    @field_validator("locator", "platform_artifact")
    @classmethod
    def _validate_text(cls, value: str, info: ValidationInfo) -> str:
        return _required_text(value, str(info.field_name))

    @field_validator("maximum_bytes")
    @classmethod
    def _validate_maximum(cls, value: int) -> int:
        if value != 10 * 1024 * 1024:
            raise ValueError("artifact maximum must remain the reviewed 10 MiB boundary")
        return value


class QCApprovedNonPrimaryWindow(_StrictModel):
    role: Literal["SENSITIVITY", "PROXY", "STRESS"]
    authority_id: str
    dq_caveat: str

    @field_validator("authority_id")
    @classmethod
    def _validate_authority(cls, value: str) -> str:
        return _identifier(value, "authority_id")

    @field_validator("dq_caveat")
    @classmethod
    def _validate_caveat(cls, value: str) -> str:
        return _required_text(value, "dq_caveat")


class QCPlatformEvidenceBundleSafety(_StrictModel):
    research_only: Literal[True]
    external_platform_action_allowed: Literal[False]
    cloud_run_authorized: Literal[False]
    api_allowed: Literal[False]
    cli_allowed: Literal[False]
    remote_http_allowed: Literal[False]
    raw_options_data_export_allowed: Literal[False]
    account_or_broker_identifiers_allowed: Literal[False]
    secrets_allowed: Literal[False]
    paper_shadow_allowed: Literal[False]
    production_allowed: Literal[False]
    promotion_allowed: Literal[False]
    strategy_execution_allowed: Literal[False]
    production_effect: Literal["none"]
    broker_action: Literal["none"]


class QCPlatformEvidenceBundlePolicy(_StrictModel):
    schema_version: Literal["qc_qqq_options_platform_evidence_manual_bundle_policy.v1"]
    policy_id: Literal["qc_qqq_options_platform_evidence_manual_bundle_v1"]
    policy_version: Literal["1.0.0"]
    status: Literal["REVIEWED_OFFLINE_CONTRACT_BASELINE", "OWNER_REVIEWED_ACTIVE"]
    owner: str
    owner_instruction: str
    rationale: str
    intended_effect: str
    validation_plan: str
    review_condition: str
    expiry_condition: str
    platform: Literal["QuantConnect"]
    collection_authorized: bool
    owner_authorization_token: str
    required_collection_authority_task_id: Literal[
        "TRADING-2492_QC_QQQ_OPTIONS_BOUNDED_PLATFORM_ACTION_AUTHORIZATION_V1"
    ]
    required_capability_decision: Literal["CAPABILITY_CONFIRMED_FOR_BOUNDED_PILOT"]
    capability_policy_sha256: str
    shared_contract_sha256: str
    shared_policy_sha256: str
    dq_pit_policy_sha256: str
    adapter_policy_sha256: str
    primary_research_start: date
    approved_non_primary_roles: tuple[QCApprovedNonPrimaryWindow, ...]
    legacy_non_default_start: date
    legacy_non_default_start_is_default: Literal[False]
    expected_directories: tuple[str, ...]
    expected_inventory: tuple[str, ...]
    artifact_rules: tuple[QCManualArtifactRule, ...]
    text_security_markers: tuple[str, ...]
    decision: Literal["QC_MANUAL_EVIDENCE_BUNDLE_V1_READY"]
    safety: QCPlatformEvidenceBundleSafety

    @field_validator(
        "owner",
        "owner_instruction",
        "rationale",
        "intended_effect",
        "validation_plan",
        "review_condition",
        "expiry_condition",
        "owner_authorization_token",
    )
    @classmethod
    def _validate_text(cls, value: str, info: ValidationInfo) -> str:
        return _required_text(value, str(info.field_name))

    @field_validator(
        "capability_policy_sha256",
        "shared_contract_sha256",
        "shared_policy_sha256",
        "dq_pit_policy_sha256",
        "adapter_policy_sha256",
    )
    @classmethod
    def _validate_hashes(cls, value: str, info: ValidationInfo) -> str:
        return _sha256(value, str(info.field_name))

    @model_validator(mode="after")
    def _validate_frozen_contract(self) -> Self:
        if self.primary_research_start != date(2021, 2, 22):
            raise ValueError("primary research start must remain 2021-02-22")
        if self.legacy_non_default_start != date(2022, 12, 1):
            raise ValueError("legacy non-default marker drifted")
        if self.expected_directories != _EXPECTED_DIRECTORIES:
            raise ValueError("manual evidence directory inventory drifted")
        if self.expected_inventory != _EXPECTED_INVENTORY:
            raise ValueError("manual evidence file inventory drifted")
        artifact_semantics = tuple(
            (
                item.artifact_id,
                item.locator,
                item.platform_artifact,
                item.timestamp_semantics,
                item.media_type,
                item.text_security_scan_required,
            )
            for item in self.artifact_rules
        )
        if artifact_semantics != _EXPECTED_ARTIFACTS:
            raise ValueError("manual evidence artifact mapping drifted")
        if self.text_security_markers != _EXPECTED_TEXT_SECURITY_MARKERS:
            raise ValueError("manual evidence security marker taxonomy drifted")
        roles = tuple(item.role for item in self.approved_non_primary_roles)
        if roles != tuple(sorted(roles)) or len(roles) != len(set(roles)):
            raise ValueError("approved non-primary roles must be sorted and unique")
        if self.status == "REVIEWED_OFFLINE_CONTRACT_BASELINE":
            if self.collection_authorized or self.owner_authorization_token != _NOT_GRANTED_TOKEN:
                raise ValueError("offline baseline cannot authorize platform collection")
            if self.approved_non_primary_roles:
                raise ValueError("offline baseline has no approved non-primary research window")
        elif (
            not self.collection_authorized
            or not self.owner_authorization_token.startswith(_OWNER_TOKEN_PREFIX)
        ):
            raise ValueError("active policy requires the reviewed TRADING-2492 owner token")
        return self


@dataclass(frozen=True)
class QCPlatformEvidenceBundlePolicyLoadResult:
    policy: QCPlatformEvidenceBundlePolicy
    policy_sha256: str
    policy_path: Path


class QCEngineIdentityField(_StrictModel):
    field_name: str
    value: str

    @field_validator("field_name")
    @classmethod
    def _validate_field_name(cls, value: str) -> str:
        return _identifier(value, "field_name")

    @field_validator("value")
    @classmethod
    def _validate_value(cls, value: str) -> str:
        return _required_text(value, "value")


class QCManualEvidenceArtifactIndex(_SealedModel):
    schema_version: Literal["qc_qqq_options_manual_evidence_artifact_index.v1"]
    bundle_id: str
    artifacts: tuple[EvidenceArtifact, ...]

    @field_validator("bundle_id")
    @classmethod
    def _validate_bundle_id(cls, value: str) -> str:
        return _identifier(value, "bundle_id")

    @model_validator(mode="after")
    def _validate_artifacts(self) -> Self:
        ids = tuple(item.artifact_id for item in self.artifacts)
        if ids != tuple(sorted(ids)) or len(ids) != len(set(ids)):
            raise ValueError("artifact index ids must be sorted and unique")
        return self


class QCManualEvidenceBundleMetadata(_SealedModel):
    schema_version: Literal["qc_qqq_options_manual_evidence_bundle_metadata.v1"]
    bundle_id: str
    run_id: str
    collected_at_utc: datetime
    bundle_closed_at_utc: datetime
    collected_by: str
    producer_version: str
    repository_code_sha: str
    policy_id: Literal["qc_qqq_options_platform_evidence_manual_bundle_v1"]
    policy_version: Literal["1.0.0"]
    policy_sha256: str
    capability_receipt_id: str
    capability_receipt_sha256: str
    capability_policy_sha256: str
    capability_evidence_sha256: str
    capability_decision: Literal["CAPABILITY_CONFIRMED_FOR_BOUNDED_PILOT"]
    shared_contract_sha256: str
    shared_policy_sha256: str
    dq_pit_policy_sha256: str
    adapter_policy_sha256: str
    adapter_descriptor_sha256: str
    artifact_index_sha256: str
    project_id: str
    backtest_id: str
    lean_engine_identity: str
    algorithm_language: Literal["Python"]
    resource_runtime_telemetry: str
    requested_start: date
    requested_end: date
    evaluated_start: date
    evaluated_end: date
    research_window_role: Literal["PRIMARY", "SENSITIVITY", "PROXY", "STRESS"]
    reviewed_non_primary_authority_id: str | None
    dq_caveat: str | None
    tier_status: Literal["CONFIRMED", "UNKNOWN", "CONTRADICTED"]
    engine_identity_status: Literal["CONFIRMED", "UNKNOWN", "CONTRADICTED"]
    license_status: Literal["CONFIRMED", "UNKNOWN", "CONTRADICTED"]
    license_review_authority_id: str | None
    engine_identity_fields: tuple[QCEngineIdentityField, ...]
    lineage_id: str
    limitations: tuple[str, ...]
    data_quality_gate_required: Literal[False]
    option_event_dq_status: Literal["NOT_EVALUATED"]
    option_event_pit_status: Literal["NOT_EVALUATED"]
    raw_option_rows_included: Literal[False]
    account_or_broker_identifiers_included: Literal[False]
    secrets_included: Literal[False]

    @field_validator(
        "bundle_id",
        "run_id",
        "collected_by",
        "producer_version",
        "capability_receipt_id",
        "project_id",
        "backtest_id",
        "lineage_id",
    )
    @classmethod
    def _validate_ids(cls, value: str, info: ValidationInfo) -> str:
        return _identifier(value, str(info.field_name))

    @field_validator("repository_code_sha")
    @classmethod
    def _validate_repository_sha(cls, value: str) -> str:
        return _git_sha(value, "repository_code_sha")

    @field_validator(
        "policy_sha256",
        "capability_receipt_sha256",
        "capability_policy_sha256",
        "capability_evidence_sha256",
        "shared_contract_sha256",
        "shared_policy_sha256",
        "dq_pit_policy_sha256",
        "adapter_policy_sha256",
        "adapter_descriptor_sha256",
        "artifact_index_sha256",
    )
    @classmethod
    def _validate_hashes(cls, value: str, info: ValidationInfo) -> str:
        return _sha256(value, str(info.field_name))

    @field_validator("collected_at_utc", "bundle_closed_at_utc")
    @classmethod
    def _validate_times(cls, value: datetime, info: ValidationInfo) -> datetime:
        return _utc(value, str(info.field_name))

    @field_validator("lean_engine_identity", "resource_runtime_telemetry")
    @classmethod
    def _validate_text(cls, value: str, info: ValidationInfo) -> str:
        return _required_text(value, str(info.field_name))

    @field_validator("reviewed_non_primary_authority_id", "license_review_authority_id")
    @classmethod
    def _validate_optional_ids(cls, value: str | None, info: ValidationInfo) -> str | None:
        return None if value is None else _identifier(value, str(info.field_name))

    @field_validator("dq_caveat")
    @classmethod
    def _validate_optional_text(cls, value: str | None) -> str | None:
        return None if value is None else _required_text(value, "dq_caveat")

    @field_validator("limitations")
    @classmethod
    def _validate_limitations(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        checked = tuple(_required_text(item, "limitations") for item in value)
        if checked != tuple(sorted(checked)) or len(checked) != len(set(checked)):
            raise ValueError("limitations must be sorted and unique")
        return checked

    @model_validator(mode="after")
    def _validate_metadata(self) -> Self:
        if self.collected_at_utc > self.bundle_closed_at_utc:
            raise ValueError("bundle cannot close before collection")
        if not (
            self.requested_start <= self.evaluated_start <= self.evaluated_end <= self.requested_end
        ):
            raise ValueError("requested/evaluated ranges are inconsistent")
        field_names = tuple(item.field_name for item in self.engine_identity_fields)
        if field_names != _EXPECTED_ENGINE_FIELDS:
            raise ValueError("engine identity fields must be complete, sorted, and exact")
        values = {item.field_name: item.value for item in self.engine_identity_fields}
        expected_values = {
            "adapter_descriptor_sha256": self.adapter_descriptor_sha256,
            "algorithm_language": self.algorithm_language,
            "backtest_id": self.backtest_id,
            "evaluated_end": self.evaluated_end.isoformat(),
            "evaluated_start": self.evaluated_start.isoformat(),
            "lean_engine_identity": self.lean_engine_identity,
            "project_id": self.project_id,
            "repository_code_sha": self.repository_code_sha,
            "requested_end": self.requested_end.isoformat(),
            "requested_start": self.requested_start.isoformat(),
            "resource_runtime_telemetry": self.resource_runtime_telemetry,
        }
        if values != expected_values:
            raise ValueError("engine identity fields do not match metadata facts")
        if self.license_status == "CONFIRMED" and self.license_review_authority_id is None:
            raise ValueError("confirmed license status requires reviewed authority")
        if self.license_status != "CONFIRMED" and self.license_review_authority_id is not None:
            raise ValueError("unconfirmed license status cannot claim reviewed authority")
        if self.research_window_role == "PRIMARY":
            if self.reviewed_non_primary_authority_id is not None or self.dq_caveat is not None:
                raise ValueError("primary window cannot claim a non-primary authority or caveat")
        elif self.reviewed_non_primary_authority_id is None or self.dq_caveat is None:
            raise ValueError("non-primary windows require reviewed authority and DQ caveat")
        return self


class QCManualEvidenceAttestation(_SealedModel):
    schema_version: Literal["qc_qqq_options_manual_evidence_attestation.v1"]
    attestation_id: str
    bundle_id: str
    role: Literal["COLLECTOR", "INDEPENDENT_REVIEWER"]
    attested_by: str
    attested_at_utc: datetime
    bundle_metadata_sha256: str
    artifact_index_sha256: str
    platform_evidence_manifest_sha256: str
    capability_receipt_sha256: str
    collector_attestation_sha256: str | None
    inventory_reviewed: Literal[True]
    checksums_reviewed: Literal[True]
    platform_tier_reviewed: Literal[True]
    engine_identity_reviewed: Literal[True]
    license_reviewed: Literal[True]
    no_raw_option_rows_confirmed: Literal[True]
    no_secrets_confirmed: Literal[True]
    no_account_identifiers_confirmed: Literal[True]
    no_broker_identifiers_confirmed: Literal[True]

    @field_validator("attestation_id", "bundle_id", "attested_by")
    @classmethod
    def _validate_ids(cls, value: str, info: ValidationInfo) -> str:
        return _identifier(value, str(info.field_name))

    @field_validator("attested_at_utc")
    @classmethod
    def _validate_time(cls, value: datetime) -> datetime:
        return _utc(value, "attested_at_utc")

    @field_validator(
        "bundle_metadata_sha256",
        "artifact_index_sha256",
        "platform_evidence_manifest_sha256",
        "capability_receipt_sha256",
        "collector_attestation_sha256",
    )
    @classmethod
    def _validate_hashes(cls, value: str | None, info: ValidationInfo) -> str | None:
        return None if value is None else _sha256(value, str(info.field_name))

    @model_validator(mode="after")
    def _validate_role(self) -> Self:
        if self.role == "COLLECTOR" and self.collector_attestation_sha256 is not None:
            raise ValueError("collector attestation cannot reference itself")
        if self.role == "INDEPENDENT_REVIEWER" and self.collector_attestation_sha256 is None:
            raise ValueError("reviewer attestation must bind the collector attestation")
        return self


class QCPlatformEvidenceBundleDescriptor(_SealedModel):
    schema_version: Literal["qc_qqq_options_platform_evidence_bundle_descriptor.v1"]
    policy_id: Literal["qc_qqq_options_platform_evidence_manual_bundle_v1"]
    policy_version: Literal["1.0.0"]
    policy_sha256: str
    platform: Literal["QuantConnect"]
    collection_authorized: bool
    owner_authorization_token_status: Literal["NOT_GRANTED", "GRANTED"]
    required_collection_authority_task_id: Literal[
        "TRADING-2492_QC_QQQ_OPTIONS_BOUNDED_PLATFORM_ACTION_AUTHORIZATION_V1"
    ]
    required_capability_decision: Literal["CAPABILITY_CONFIRMED_FOR_BOUNDED_PILOT"]
    primary_research_start: date
    expected_inventory: tuple[str, ...]
    decision: Literal["QC_MANUAL_EVIDENCE_BUNDLE_V1_READY"]
    default_disposition: Literal["MANUAL_COLLECTION_INCOMPLETE"]
    safety: QCPlatformEvidenceBundleSafety

    @field_validator("policy_sha256")
    @classmethod
    def _validate_policy_hash(cls, value: str) -> str:
        return _sha256(value, "policy_sha256")


class QCPlatformEvidenceBundleValidationRecord(_SealedModel):
    schema_version: Literal["qc_qqq_options_platform_evidence_bundle_validation.v1"]
    bundle_id: str
    run_id: str
    policy_sha256: str
    capability_receipt_sha256: str
    bundle_metadata_sha256: str
    artifact_index_sha256: str
    platform_evidence_manifest_sha256: str
    collector_attestation_sha256: str
    independent_reviewer_attestation_sha256: str
    validated_at_utc: datetime
    disposition: Literal["MANUAL_COLLECTION_READY_FOR_LOCAL_RECONCILIATION"]
    reason_codes: tuple[()]
    data_quality_gate_required: Literal[False]
    option_event_dq_status: Literal["NOT_EVALUATED"]
    option_event_pit_status: Literal["NOT_EVALUATED"]
    safety: QCPlatformEvidenceBundleSafety

    @field_validator("bundle_id", "run_id")
    @classmethod
    def _validate_ids(cls, value: str, info: ValidationInfo) -> str:
        return _identifier(value, str(info.field_name))

    @field_validator(
        "policy_sha256",
        "capability_receipt_sha256",
        "bundle_metadata_sha256",
        "artifact_index_sha256",
        "platform_evidence_manifest_sha256",
        "collector_attestation_sha256",
        "independent_reviewer_attestation_sha256",
    )
    @classmethod
    def _validate_hashes(cls, value: str, info: ValidationInfo) -> str:
        return _sha256(value, str(info.field_name))

    @field_validator("validated_at_utc")
    @classmethod
    def _validate_time(cls, value: datetime) -> datetime:
        return _utc(value, "validated_at_utc")


@dataclass(frozen=True)
class LoadedQCPlatformEvidenceBundle:
    package_root: Path
    policy: QCPlatformEvidenceBundlePolicy
    policy_sha256: str
    metadata: QCManualEvidenceBundleMetadata
    artifact_index: QCManualEvidenceArtifactIndex
    platform_manifest: PlatformEvidenceManifestRecord
    collector_attestation: QCManualEvidenceAttestation
    reviewer_attestation: QCManualEvidenceAttestation
    validation: QCPlatformEvidenceBundleValidationRecord
    file_sha256s: dict[str, str]
    file_byte_counts: dict[str, int]


def load_qc_qqq_options_platform_evidence_bundle_policy(
    path: Path = DEFAULT_QC_QQQ_OPTIONS_PLATFORM_EVIDENCE_BUNDLE_POLICY_PATH,
    *,
    project_root: Path = PROJECT_ROOT,
) -> QCPlatformEvidenceBundlePolicyLoadResult:
    resolved = _resolve(path, project_root=project_root)
    try:
        payload = safe_load_yaml_path(resolved)
        if not isinstance(payload, dict):
            raise TypeError("policy root must be a mapping")
        policy = QCPlatformEvidenceBundlePolicy.model_validate(payload)
        policy_sha256 = sha256_path(resolved)
        _validate_inherited_policy_authority(policy, project_root=project_root)
    except QCPlatformEvidenceBundleContractError:
        raise
    except (OSError, TypeError, ValueError) as exc:
        raise QCPlatformEvidenceBundleContractError(
            "QC_PLATFORM_EVIDENCE_POLICY_INVALID", f"{resolved}: {exc}"
        ) from exc
    return QCPlatformEvidenceBundlePolicyLoadResult(
        policy=policy,
        policy_sha256=policy_sha256,
        policy_path=resolved,
    )


def build_qc_qqq_options_platform_evidence_bundle_descriptor(
    *,
    policy_path: Path = DEFAULT_QC_QQQ_OPTIONS_PLATFORM_EVIDENCE_BUNDLE_POLICY_PATH,
    project_root: Path = PROJECT_ROOT,
) -> QCPlatformEvidenceBundleDescriptor:
    loaded = load_qc_qqq_options_platform_evidence_bundle_policy(
        policy_path, project_root=project_root
    )
    policy = loaded.policy
    return QCPlatformEvidenceBundleDescriptor.seal(
        schema_version="qc_qqq_options_platform_evidence_bundle_descriptor.v1",
        policy_id=policy.policy_id,
        policy_version=policy.policy_version,
        policy_sha256=loaded.policy_sha256,
        platform=policy.platform,
        collection_authorized=policy.collection_authorized,
        owner_authorization_token_status=(
            "GRANTED" if policy.collection_authorized else "NOT_GRANTED"
        ),
        required_collection_authority_task_id=policy.required_collection_authority_task_id,
        required_capability_decision=policy.required_capability_decision,
        primary_research_start=policy.primary_research_start,
        expected_inventory=policy.expected_inventory,
        decision=policy.decision,
        default_disposition="MANUAL_COLLECTION_INCOMPLETE",
        safety=policy.safety,
    )


def build_qc_qqq_options_platform_evidence_manifest(
    *,
    metadata: QCManualEvidenceBundleMetadata,
    artifact_index: QCManualEvidenceArtifactIndex,
    policy: QCPlatformEvidenceBundlePolicy,
    policy_sha256: str,
    capability_receipt_sha256: str,
    bundle_metadata_sha256: str,
    artifact_index_sha256: str,
) -> PlatformEvidenceManifestRecord:
    """Build the shared manifest from verified local package facts, never caller status."""
    return _build_expected_manifest(
        metadata=metadata,
        artifact_index=artifact_index,
        policy=policy,
        policy_sha256=_sha256(policy_sha256, "policy_sha256"),
        receipt_sha256=_sha256(
            capability_receipt_sha256, "capability_receipt_sha256"
        ),
        metadata_sha256=_sha256(bundle_metadata_sha256, "bundle_metadata_sha256"),
        artifact_index_sha256=_sha256(
            artifact_index_sha256, "artifact_index_sha256"
        ),
    )


def load_qc_qqq_options_manual_evidence_bundle(
    package_root: Path,
    *,
    capability_receipt_path: Path,
    policy_path: Path = DEFAULT_QC_QQQ_OPTIONS_PLATFORM_EVIDENCE_BUNDLE_POLICY_PATH,
    capability_policy_path: Path = DEFAULT_QC_QQQ_OPTIONS_CAPABILITY_POLICY_PATH,
    capability_evidence_path: Path = DEFAULT_QC_QQQ_OPTIONS_CAPABILITY_EVIDENCE_TEMPLATE_PATH,
    project_root: Path = PROJECT_ROOT,
) -> LoadedQCPlatformEvidenceBundle:
    loaded_policy = load_qc_qqq_options_platform_evidence_bundle_policy(
        policy_path, project_root=project_root
    )
    policy = loaded_policy.policy
    if not policy.collection_authorized:
        _incomplete("tracked policy does not authorize external platform collection")

    try:
        receipt = verify_qc_qqq_options_capability_admission_receipt(
            capability_receipt_path,
            policy_path=capability_policy_path,
            evidence_path=capability_evidence_path,
            project_root=project_root,
        )
    except (OSError, ValueError) as exc:
        raise QCPlatformEvidenceBundleContractError(
            "MANUAL_COLLECTION_INVALID",
            f"capability receipt could not be reconstructed from canonical facts: {exc}",
        ) from exc
    if receipt.decision != policy.required_capability_decision:
        _incomplete("verified capability receipt does not admit bounded pilot preparation")

    root = _resolve(package_root, project_root=project_root)
    actual_files, actual_directories = _audit_inventory(root)
    missing = tuple(sorted(set(policy.expected_inventory) - set(actual_files)))
    extra = tuple(sorted(set(actual_files) - set(policy.expected_inventory)))
    missing_directories = tuple(
        sorted(set(policy.expected_directories) - set(actual_directories))
    )
    extra_directories = tuple(
        sorted(set(actual_directories) - set(policy.expected_directories))
    )
    if extra or extra_directories:
        _invalid(f"unexpected package entries: files={extra}, directories={extra_directories}")
    if missing or missing_directories:
        _incomplete(
            f"mandatory package entries missing: files={missing}, directories={missing_directories}"
        )

    contents = {relative: (root / relative).read_bytes() for relative in actual_files}
    file_sha256s = {relative: _content_sha256(content) for relative, content in contents.items()}
    file_byte_counts = {relative: len(content) for relative, content in contents.items()}

    artifact_index = QCManualEvidenceArtifactIndex.from_json_bytes(
        contents["artifact_index.json"]
    )
    metadata = QCManualEvidenceBundleMetadata.from_json_bytes(
        contents["bundle_metadata.json"]
    )
    try:
        manifest = PlatformEvidenceManifestRecord.from_json_bytes(
            contents["platform_evidence_manifest.json"]
        )
    except ValueError as exc:
        raise QCPlatformEvidenceBundleContractError(
            "MANUAL_COLLECTION_INVALID", f"platform evidence manifest invalid: {exc}"
        ) from exc
    collector = QCManualEvidenceAttestation.from_json_bytes(
        contents["attestations/collector.json"]
    )
    reviewer = QCManualEvidenceAttestation.from_json_bytes(
        contents["attestations/independent_reviewer.json"]
    )

    receipt_file = _resolve(capability_receipt_path, project_root=project_root)
    receipt_sha256 = sha256_path(receipt_file)
    _validate_artifacts(
        root=root,
        contents=contents,
        policy=policy,
        artifact_index=artifact_index,
    )
    _validate_metadata(
        metadata=metadata,
        artifact_index=artifact_index,
        receipt=receipt,
        receipt_sha256=receipt_sha256,
        policy=policy,
        policy_sha256=loaded_policy.policy_sha256,
        file_sha256s=file_sha256s,
    )
    expected_manifest = _build_expected_manifest(
        metadata=metadata,
        artifact_index=artifact_index,
        policy=policy,
        policy_sha256=loaded_policy.policy_sha256,
        receipt_sha256=receipt_sha256,
        metadata_sha256=file_sha256s["bundle_metadata.json"],
        artifact_index_sha256=file_sha256s["artifact_index.json"],
    )
    if manifest != expected_manifest:
        _invalid("caller manifest does not equal the manifest rebuilt from package facts")
    _validate_attestations(
        metadata=metadata,
        collector=collector,
        reviewer=reviewer,
        receipt_sha256=receipt_sha256,
        file_sha256s=file_sha256s,
    )

    validation = QCPlatformEvidenceBundleValidationRecord.seal(
        schema_version="qc_qqq_options_platform_evidence_bundle_validation.v1",
        bundle_id=metadata.bundle_id,
        run_id=metadata.run_id,
        policy_sha256=loaded_policy.policy_sha256,
        capability_receipt_sha256=receipt_sha256,
        bundle_metadata_sha256=file_sha256s["bundle_metadata.json"],
        artifact_index_sha256=file_sha256s["artifact_index.json"],
        platform_evidence_manifest_sha256=file_sha256s[
            "platform_evidence_manifest.json"
        ],
        collector_attestation_sha256=file_sha256s["attestations/collector.json"],
        independent_reviewer_attestation_sha256=file_sha256s[
            "attestations/independent_reviewer.json"
        ],
        validated_at_utc=metadata.bundle_closed_at_utc,
        disposition="MANUAL_COLLECTION_READY_FOR_LOCAL_RECONCILIATION",
        reason_codes=(),
        data_quality_gate_required=False,
        option_event_dq_status="NOT_EVALUATED",
        option_event_pit_status="NOT_EVALUATED",
        safety=policy.safety,
    )
    return LoadedQCPlatformEvidenceBundle(
        package_root=root,
        policy=policy,
        policy_sha256=loaded_policy.policy_sha256,
        metadata=metadata,
        artifact_index=artifact_index,
        platform_manifest=manifest,
        collector_attestation=collector,
        reviewer_attestation=reviewer,
        validation=validation,
        file_sha256s=file_sha256s,
        file_byte_counts=file_byte_counts,
    )


def _validate_inherited_policy_authority(
    policy: QCPlatformEvidenceBundlePolicy, *, project_root: Path
) -> None:
    if policy.shared_contract_sha256 != QQQ_OPTIONS_CONTRACT_SHA256:
        _invalid("shared contract schema authority mismatch")
    inherited_paths = (
        (policy.capability_policy_sha256, DEFAULT_QC_QQQ_OPTIONS_CAPABILITY_POLICY_PATH),
        (policy.shared_policy_sha256, _SHARED_POLICY_PATH),
        (policy.dq_pit_policy_sha256, _DQ_PIT_POLICY_PATH),
        (policy.adapter_policy_sha256, DEFAULT_QC_QQQ_OPTIONS_PROJECT_ADAPTER_POLICY_PATH),
    )
    for expected, relative in inherited_paths:
        if sha256_path(_resolve(relative, project_root=project_root)) != expected:
            _invalid(f"inherited policy bytes drifted: {relative.as_posix()}")
    adapter = load_qc_qqq_options_project_adapter_policy(
        DEFAULT_QC_QQQ_OPTIONS_PROJECT_ADAPTER_POLICY_PATH,
        project_root=project_root,
    ).policy
    if (
        adapter.shared_contract_sha256 != policy.shared_contract_sha256
        or adapter.shared_policy_sha256 != policy.shared_policy_sha256
        or adapter.dq_pit_policy_sha256 != policy.dq_pit_policy_sha256
    ):
        _invalid("adapter lineage does not match inherited shared authority")
    adapter_mapping = {
        item.mapping_id: (
            item.platform_artifact,
            item.timestamp_semantics,
            item.export_classification,
        )
        for item in adapter.result_mappings
    }
    policy_mapping = {
        item.artifact_id: (
            item.platform_artifact,
            item.timestamp_semantics,
            item.export_classification,
        )
        for item in policy.artifact_rules
        if item.artifact_id != "platform_ui"
    }
    if policy_mapping != adapter_mapping:
        _invalid("manual evidence mapping does not inherit the 2484 adapter mappings")


def _audit_inventory(root: Path) -> tuple[tuple[str, ...], tuple[str, ...]]:
    if _is_link_or_reparse(root) or not root.is_dir():
        _invalid("package root must be an existing non-link directory")
    files: list[str] = []
    directories: list[str] = []
    for candidate in root.rglob("*"):
        if _is_link_or_reparse(candidate):
            _invalid(f"symlink or reparse-point entry prohibited: {candidate}")
        try:
            candidate.resolve(strict=True).relative_to(root.resolve(strict=True))
        except (OSError, ValueError) as exc:
            raise QCPlatformEvidenceBundleContractError(
                "MANUAL_COLLECTION_INVALID", f"entry escapes package root: {candidate}"
            ) from exc
        relative = candidate.relative_to(root).as_posix()
        if candidate.is_file():
            files.append(relative)
        elif candidate.is_dir():
            directories.append(relative)
        else:
            _invalid(f"unsupported filesystem entry: {candidate}")
    if len({item.casefold() for item in files}) != len(files):
        _invalid("case-folding file alias detected")
    return tuple(sorted(files)), tuple(sorted(directories))


def _is_link_or_reparse(path: Path) -> bool:
    if path.is_symlink():
        return True
    try:
        attributes = getattr(os.lstat(path), "st_file_attributes", 0)
    except OSError:
        return False
    return bool(attributes & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400))


def _validate_artifacts(
    *,
    root: Path,
    contents: dict[str, bytes],
    policy: QCPlatformEvidenceBundlePolicy,
    artifact_index: QCManualEvidenceArtifactIndex,
) -> None:
    rules = {item.artifact_id: item for item in policy.artifact_rules}
    indexed = {item.artifact_id: item for item in artifact_index.artifacts}
    if artifact_index.bundle_id == "":
        _invalid("artifact index bundle id missing")
    if set(indexed) != set(rules):
        missing = tuple(sorted(set(rules) - set(indexed)))
        extra = tuple(sorted(set(indexed) - set(rules)))
        if missing and not extra:
            _incomplete(f"mandatory artifacts missing from index: {missing}")
        _invalid(f"artifact index mismatch: missing={missing}, extra={extra}")
    marker_bytes = tuple(marker.encode("utf-8") for marker in policy.text_security_markers)
    for artifact_id in sorted(rules):
        rule = rules[artifact_id]
        artifact = indexed[artifact_id]
        if (
            artifact.locator != rule.locator
            or artifact.export_classification != rule.export_classification
        ):
            _invalid(f"artifact rule binding mismatch: {artifact_id}")
        content = contents[rule.locator]
        if not content:
            _incomplete(f"mandatory artifact is empty: {artifact_id}")
        if len(content) > rule.maximum_bytes:
            _invalid(f"artifact exceeds reviewed size boundary: {artifact_id}")
        if artifact.byte_count != len(content) or artifact.sha256 != _content_sha256(content):
            _invalid(f"artifact checksum or byte count mismatch: {artifact_id}")
        _validate_artifact_shape(rule, content)
        if rule.text_security_scan_required:
            lowered = content.lower()
            matched = [marker.decode("utf-8") for marker in marker_bytes if marker in lowered]
            if matched:
                _invalid(f"prohibited marker in {artifact_id}: {tuple(matched)}")
    if any((root / item.locator).is_symlink() for item in rules.values()):
        _invalid("artifact symlink prohibited")


def _validate_artifact_shape(rule: QCManualArtifactRule, content: bytes) -> None:
    if rule.media_type in {"text/plain", "text/csv", "application/json"}:
        try:
            text = content.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise QCPlatformEvidenceBundleContractError(
                "MANUAL_COLLECTION_INVALID", f"{rule.artifact_id} is not UTF-8"
            ) from exc
        if "\x00" in text:
            _invalid(f"NUL byte in text artifact: {rule.artifact_id}")
        if rule.media_type == "application/json":
            try:
                payload = json.loads(text)
            except ValueError as exc:
                raise QCPlatformEvidenceBundleContractError(
                    "MANUAL_COLLECTION_INVALID", "results_json is not valid JSON"
                ) from exc
            if not isinstance(payload, dict):
                _invalid("results_json root must be an object")
        elif rule.media_type == "text/csv":
            rows = list(csv.reader(io.StringIO(text)))
            if not rows or not rows[0] or any(not cell.strip() for cell in rows[0]):
                _invalid(f"CSV artifact lacks a normalized header: {rule.artifact_id}")
    elif rule.media_type == "image/png" and not content.startswith(b"\x89PNG\r\n\x1a\n"):
        _invalid("platform UI artifact is not PNG")
    elif rule.media_type == "application/pdf" and not content.startswith(b"%PDF-"):
        _invalid("report artifact is not PDF")
    elif rule.media_type == "application/zip":
        try:
            with zipfile.ZipFile(io.BytesIO(content)) as archive:
                names = archive.namelist()
                if not names or archive.testzip() is not None:
                    _invalid("project file archive is empty or corrupt")
                for info in archive.infolist():
                    path = Path(info.filename.replace("\\", "/"))
                    if path.is_absolute() or ".." in path.parts:
                        _invalid("project file archive contains path traversal")
                    if (info.external_attr >> 16) & 0o170000 == 0o120000:
                        _invalid("project file archive contains a symlink")
        except zipfile.BadZipFile as exc:
            raise QCPlatformEvidenceBundleContractError(
                "MANUAL_COLLECTION_INVALID", "project file artifact is not a valid ZIP"
            ) from exc


def _validate_metadata(
    *,
    metadata: QCManualEvidenceBundleMetadata,
    artifact_index: QCManualEvidenceArtifactIndex,
    receipt: Any,
    receipt_sha256: str,
    policy: QCPlatformEvidenceBundlePolicy,
    policy_sha256: str,
    file_sha256s: dict[str, str],
) -> None:
    if metadata.bundle_id != artifact_index.bundle_id:
        _invalid("bundle metadata and artifact index bundle ids differ")
    expected_bindings = (
        (metadata.policy_id, policy.policy_id, "policy id"),
        (metadata.policy_version, policy.policy_version, "policy version"),
        (metadata.policy_sha256, policy_sha256, "policy hash"),
        (metadata.capability_receipt_id, receipt.receipt_id, "capability receipt id"),
        (metadata.capability_receipt_sha256, receipt_sha256, "capability receipt hash"),
        (metadata.capability_policy_sha256, receipt.policy_sha256, "capability policy hash"),
        (metadata.capability_evidence_sha256, receipt.evidence_sha256, "capability evidence hash"),
        (metadata.capability_decision, receipt.decision, "capability decision"),
        (metadata.shared_contract_sha256, policy.shared_contract_sha256, "shared contract hash"),
        (metadata.shared_policy_sha256, policy.shared_policy_sha256, "shared policy hash"),
        (metadata.dq_pit_policy_sha256, policy.dq_pit_policy_sha256, "DQ/PIT policy hash"),
        (metadata.adapter_policy_sha256, policy.adapter_policy_sha256, "adapter policy hash"),
        (
            metadata.artifact_index_sha256,
            file_sha256s["artifact_index.json"],
            "artifact index file hash",
        ),
    )
    for actual, expected, field in expected_bindings:
        if actual != expected:
            _invalid(f"metadata {field} mismatch")
    statuses = (
        metadata.tier_status,
        metadata.engine_identity_status,
        metadata.license_status,
    )
    if "CONTRADICTED" in statuses:
        _invalid("tier, engine identity, or license evidence is contradicted")
    if "UNKNOWN" in statuses:
        _incomplete("tier, engine identity, and license evidence must all be confirmed")
    if metadata.research_window_role == "PRIMARY":
        if (
            metadata.requested_start != policy.primary_research_start
            or metadata.evaluated_start != policy.primary_research_start
        ):
            _invalid("PRIMARY package must request and evaluate from 2021-02-22")
    else:
        approved = {
            item.role: (item.authority_id, item.dq_caveat)
            for item in policy.approved_non_primary_roles
        }
        observed = (
            metadata.reviewed_non_primary_authority_id,
            metadata.dq_caveat,
        )
        if approved.get(metadata.research_window_role) != observed:
            _invalid("non-primary research window lacks exact reviewed policy authority")


def _build_expected_manifest(
    *,
    metadata: QCManualEvidenceBundleMetadata,
    artifact_index: QCManualEvidenceArtifactIndex,
    policy: QCPlatformEvidenceBundlePolicy,
    policy_sha256: str,
    receipt_sha256: str,
    metadata_sha256: str,
    artifact_index_sha256: str,
) -> PlatformEvidenceManifestRecord:
    source_pairs = tuple(
        sorted(
            (
                ("qc.capability_receipt", receipt_sha256),
                ("qc.manual_artifact_index", artifact_index_sha256),
                ("qc.manual_bundle_metadata", metadata_sha256),
                ("qc.manual_evidence_policy", policy_sha256),
            )
        )
    )
    return PlatformEvidenceManifestRecord.seal(
        schema_name="platform_evidence_manifest",
        schema_version="1.0.0",
        run_id=metadata.run_id,
        record_id=metadata.bundle_id,
        created_at_utc=metadata.bundle_closed_at_utc,
        producer_version=metadata.producer_version,
        repository_code_sha=metadata.repository_code_sha,
        policy_id=policy.policy_id,
        policy_version=policy.policy_version,
        policy_sha256=policy_sha256,
        contract_schema_sha256=policy.shared_contract_sha256,
        source_ids=tuple(item[0] for item in source_pairs),
        source_checksums=tuple(item[1] for item in source_pairs),
        requested_start=metadata.requested_start,
        requested_end=metadata.requested_end,
        evaluated_start=metadata.evaluated_start,
        evaluated_end=metadata.evaluated_end,
        storage_timezone="UTC",
        exchange_timezone="America/New_York",
        dq_status="NOT_EVALUATED",
        pit_status="NOT_EVALUATED",
        export_classification="EXPORT_ALLOWED_DERIVED",
        lineage_id=metadata.lineage_id,
        safety=QQQOptionsSafetyBoundary(
            research_only=True,
            promotion_allowed=False,
            paper_shadow_allowed=False,
            production_allowed=False,
            raw_options_data_export_allowed=False,
            strategy_execution_allowed=False,
            bounded_cloud_pilot_authorized=False,
            production_effect="none",
            broker_action="none",
        ),
        bundle_id=metadata.bundle_id,
        platform="QuantConnect",
        backtest_id=metadata.backtest_id,
        tier_status=metadata.tier_status,
        engine_identity_status=metadata.engine_identity_status,
        collected_at_utc=metadata.collected_at_utc,
        collected_by=metadata.collected_by,
        artifacts=artifact_index.artifacts,
        limitations=metadata.limitations,
        raw_option_rows_included=False,
        account_or_broker_identifiers_included=False,
    )


def _validate_attestations(
    *,
    metadata: QCManualEvidenceBundleMetadata,
    collector: QCManualEvidenceAttestation,
    reviewer: QCManualEvidenceAttestation,
    receipt_sha256: str,
    file_sha256s: dict[str, str],
) -> None:
    if collector.role != "COLLECTOR" or reviewer.role != "INDEPENDENT_REVIEWER":
        _invalid("attestation files have incorrect roles")
    if collector.attested_by == reviewer.attested_by:
        _invalid("collector and independent reviewer must be different people")
    if collector.attested_at_utc > reviewer.attested_at_utc:
        _invalid("independent review cannot precede collector attestation")
    if metadata.bundle_closed_at_utc > collector.attested_at_utc:
        _invalid("attestation cannot precede bundle close")
    expected_common = {
        "bundle_id": metadata.bundle_id,
        "bundle_metadata_sha256": file_sha256s["bundle_metadata.json"],
        "artifact_index_sha256": file_sha256s["artifact_index.json"],
        "platform_evidence_manifest_sha256": file_sha256s[
            "platform_evidence_manifest.json"
        ],
        "capability_receipt_sha256": receipt_sha256,
    }
    for attestation in (collector, reviewer):
        for field, expected in expected_common.items():
            if getattr(attestation, field) != expected:
                _invalid(f"{attestation.role} attestation {field} mismatch")
    if reviewer.collector_attestation_sha256 != file_sha256s[
        "attestations/collector.json"
    ]:
        _invalid("reviewer does not bind the exact collector attestation bytes")


def _resolve(path: Path, *, project_root: Path) -> Path:
    return path if path.is_absolute() else project_root / path


def _incomplete(message: str) -> None:
    raise QCPlatformEvidenceBundleContractError("MANUAL_COLLECTION_INCOMPLETE", message)


def _invalid(message: str) -> None:
    raise QCPlatformEvidenceBundleContractError("MANUAL_COLLECTION_INVALID", message)


__all__ = [
    "DEFAULT_QC_QQQ_OPTIONS_PLATFORM_EVIDENCE_BUNDLE_POLICY_PATH",
    "LoadedQCPlatformEvidenceBundle",
    "QCApprovedNonPrimaryWindow",
    "QCEngineIdentityField",
    "QCManualArtifactRule",
    "QCManualEvidenceArtifactIndex",
    "QCManualEvidenceAttestation",
    "QCManualEvidenceBundleMetadata",
    "QCPlatformEvidenceBundleContractError",
    "QCPlatformEvidenceBundleDescriptor",
    "QCPlatformEvidenceBundlePolicy",
    "QCPlatformEvidenceBundlePolicyLoadResult",
    "QCPlatformEvidenceBundleSafety",
    "QCPlatformEvidenceBundleValidationRecord",
    "build_qc_qqq_options_platform_evidence_bundle_descriptor",
    "build_qc_qqq_options_platform_evidence_manifest",
    "load_qc_qqq_options_manual_evidence_bundle",
    "load_qc_qqq_options_platform_evidence_bundle_policy",
]
