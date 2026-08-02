from __future__ import annotations

import hashlib
import json
import re
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
    DailySignalRecord,
    RunManifestRecord,
)
from ai_trading_system.qqq_options_research.signal_package import (
    DEFAULT_QQQ_OPTIONS_SIGNAL_EXPORT_POLICY_PATH,
    QQQOptionsSignalIndex,
    QQQOptionsSignalPackage,
    QQQOptionsSignalPackageReceipt,
    SignalArtifactDigest,
    load_qqq_options_signal_export_policy,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

DEFAULT_QC_QQQ_OPTIONS_PROJECT_ADAPTER_POLICY_PATH = Path(
    "config/research/qc_qqq_options_project_adapter_contract_v1.yaml"
)

_SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
_GIT_OBJECT_SHA_PATTERN = re.compile(r"^(?:[0-9a-f]{40}|[0-9a-f]{64})$")
_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]*$")
_DAILY_SIGNAL_PATH_PATTERN = re.compile(r"^daily_signals/(\d{4}-\d{2}-\d{2})\.json$")
_UNSEALED_SHA256 = "0" * 64
_EXPECTED_RESULT_MAPPING_IDS = (
    "logs",
    "orders_csv",
    "project_files",
    "report_pdf",
    "results_json",
    "trades_csv",
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
_EXPECTED_OFFICIAL_SOURCES = (
    "https://www.quantconnect.com/docs/v2/cloud-platform/projects/files",
    "https://www.quantconnect.com/docs/v2/cloud-platform/object-store",
    "https://www.quantconnect.com/docs/v2/writing-algorithms/universes/equity-options",
    "https://www.quantconnect.com/docs/v2/writing-algorithms/securities/asset-classes/equity-options/requesting-data/individual-contracts",
    "https://www.quantconnect.com/docs/v2/cloud-platform/backtesting/results",
)


class QCProjectAdapterContractError(ValueError):
    def __init__(self, code: str, message: str) -> None:
        self.code = code
        self.message = message
        super().__init__(f"{code}: {message}")


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


def _required_text(value: str, field: str) -> str:
    if not value or value.strip() != value:
        raise ValueError(f"{field} must be non-empty trimmed text")
    return value


def _identifier(value: str, field: str) -> str:
    value = _required_text(value, field)
    if not _IDENTIFIER_PATTERN.fullmatch(value):
        raise ValueError(f"{field} must be a stable identifier")
    return value


def _sha256(value: str, field: str) -> str:
    if not _SHA256_PATTERN.fullmatch(value):
        raise ValueError(f"{field} must be lowercase SHA-256")
    return value


def _git_object_sha(value: str, field: str) -> str:
    if not _GIT_OBJECT_SHA_PATTERN.fullmatch(value):
        raise ValueError(f"{field} must be a lowercase Git object id")
    return value


def _utc(value: datetime, field: str) -> datetime:
    offset = value.utcoffset()
    if value.tzinfo is None or offset is None:
        raise ValueError(f"{field} must be timezone-aware")
    if offset.total_seconds() != 0:
        raise ValueError(f"{field} must be UTC")
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


class QCSubscriptionContract(_StrictModel):
    underlying: Literal["QQQ"]
    market: Literal["USA"]
    underlying_security_type: Literal["EQUITY"]
    underlying_resolution: Literal["MINUTE"]
    underlying_data_normalization: Literal["RAW"]
    option_security_type: Literal["OPTION"]
    option_resolution: Literal["MINUTE"]
    signal_resolution: Literal["DAILY"]
    execution_resolution: Literal["MINUTE"]
    storage_timezone: Literal["UTC"]
    exchange_timezone: Literal["America/New_York"]


class QCProjectFileBoundary(_StrictModel):
    maximum_file_bytes: int
    descriptor_input_mode: Literal["CONTENT_BOUND_DESCRIPTOR_ONLY"]
    complete_signal_input_admission: Literal[
        "UNKNOWN_REQUIRES_PLATFORM_EVIDENCE"
    ]
    offline_descriptor_generation_allowed: Literal[True]
    object_store_allowed: Literal[False]
    api_allowed: Literal[False]
    cli_allowed: Literal[False]
    remote_http_allowed: Literal[False]
    secrets_allowed: Literal[False]
    raw_option_rows_allowed: Literal[False]

    @field_validator("maximum_file_bytes")
    @classmethod
    def _validate_maximum_file_bytes(cls, value: int) -> int:
        if value != 32768:
            raise ValueError("Free project-file maximum must remain exactly 32768 bytes")
        return value


class QCEngineIdentityRequirement(_StrictModel):
    status: Literal["REQUIRED_NOT_OBSERVED"]
    required_fields: tuple[str, ...]

    @field_validator("required_fields")
    @classmethod
    def _validate_required_fields(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if value != _EXPECTED_ENGINE_FIELDS:
            raise ValueError("engine identity fields drifted from the reviewed contract")
        return value


class QCResultMappingRule(_StrictModel):
    mapping_id: str
    platform_artifact: str
    timestamp_semantics: Literal[
        "UTC",
        "ALGORITHM_TIMEZONE",
        "NOT_APPLICABLE",
        "MIXED_DECLARED_BY_ARTIFACT",
    ]
    export_classification: Literal["EXPORT_ALLOWED_DERIVED"]
    collection_authority_task_id: Literal[
        "TRADING-2489_QC_QQQ_OPTIONS_PLATFORM_EVIDENCE_MANUAL_BUNDLE_V1"
    ]

    @field_validator("mapping_id")
    @classmethod
    def _validate_mapping_id(cls, value: str) -> str:
        return _identifier(value, "mapping_id")

    @field_validator("platform_artifact")
    @classmethod
    def _validate_platform_artifact(cls, value: str) -> str:
        return _required_text(value, "platform_artifact")


class QCProjectAdapterSafety(_StrictModel):
    research_only: Literal[True]
    external_platform_action_allowed: Literal[False]
    project_creation_allowed: Literal[False]
    cloud_run_authorized: Literal[False]
    paper_shadow_allowed: Literal[False]
    production_allowed: Literal[False]
    promotion_allowed: Literal[False]
    raw_options_data_export_allowed: Literal[False]
    strategy_execution_allowed: Literal[False]
    production_effect: Literal["none"]
    broker_action: Literal["none"]


class QCProjectAdapterPolicy(_StrictModel):
    schema_version: Literal["qc_qqq_options_project_adapter_policy.v1"]
    policy_id: Literal["qc_qqq_options_project_adapter_contract_v1"]
    policy_version: Literal["1.0.0"]
    status: Literal["REVIEWED_OFFLINE_CONTRACT_BASELINE"]
    owner: str
    owner_instruction: str
    rationale: str
    intended_effect: str
    validation_plan: str
    review_condition: str
    expiry_condition: str
    platform: Literal["QuantConnect"]
    algorithm_language: Literal["Python"]
    signal_export_policy_sha256: str
    shared_contract_sha256: str
    shared_policy_sha256: str
    dq_pit_policy_sha256: str
    primary_research_start: date
    approved_non_primary_authority_count: int
    legacy_non_default_start: date
    legacy_non_default_start_is_default: Literal[False]
    subscription: QCSubscriptionContract
    project_file_boundary: QCProjectFileBoundary
    engine_identity: QCEngineIdentityRequirement
    result_mappings: tuple[QCResultMappingRule, ...]
    raw_option_export_classifications: tuple[
        Literal["EXPORT_PROHIBITED", "QC_ONLY_NOT_EXPORTED"], ...
    ]
    decision: Literal["QC_ADAPTER_CONTRACT_READY_NO_CLOUD_RUN"]
    safety: QCProjectAdapterSafety
    official_sources: tuple[str, ...]
    official_sources_reviewed_on: date

    @field_validator(
        "owner",
        "owner_instruction",
        "rationale",
        "intended_effect",
        "validation_plan",
        "review_condition",
        "expiry_condition",
    )
    @classmethod
    def _validate_text(cls, value: str, info: ValidationInfo) -> str:
        return _required_text(value, str(info.field_name))

    @field_validator(
        "signal_export_policy_sha256",
        "shared_contract_sha256",
        "shared_policy_sha256",
        "dq_pit_policy_sha256",
    )
    @classmethod
    def _validate_hashes(cls, value: str, info: ValidationInfo) -> str:
        return _sha256(value, str(info.field_name))

    @model_validator(mode="after")
    def _validate_contract_freeze(self) -> Self:
        if self.primary_research_start != date(2021, 2, 22):
            raise ValueError("primary research start must remain 2021-02-22")
        if self.approved_non_primary_authority_count != 0:
            raise ValueError("no non-primary research-window authority is currently approved")
        if self.legacy_non_default_start != date(2022, 12, 1):
            raise ValueError("legacy non-default marker drifted")
        mapping_ids = tuple(item.mapping_id for item in self.result_mappings)
        if mapping_ids != _EXPECTED_RESULT_MAPPING_IDS:
            raise ValueError("result mappings must be complete, sorted, and exact")
        if self.raw_option_export_classifications != (
            "EXPORT_PROHIBITED",
            "QC_ONLY_NOT_EXPORTED",
        ):
            raise ValueError("raw option export boundary drifted")
        if self.official_sources != _EXPECTED_OFFICIAL_SOURCES:
            raise ValueError("official source set drifted")
        if self.official_sources_reviewed_on != date(2026, 8, 2):
            raise ValueError("official source review date drifted")
        return self


@dataclass(frozen=True)
class QCProjectAdapterPolicyLoadResult:
    policy: QCProjectAdapterPolicy
    policy_sha256: str
    policy_path: Path


@dataclass(frozen=True)
class LoadedQQQOptionsSignalPackage:
    package_root: Path
    package: QQQOptionsSignalPackage
    file_sha256s: dict[str, str]
    file_byte_counts: dict[str, int]


class QCProjectAdapterDescriptor(_StrictModel):
    schema_version: Literal["qc_qqq_options_project_adapter_descriptor.v1"]
    run_id: str
    created_at_utc: datetime
    repository_code_sha: str
    adapter_policy_id: Literal["qc_qqq_options_project_adapter_contract_v1"]
    adapter_policy_version: Literal["1.0.0"]
    adapter_policy_sha256: str
    signal_export_policy_sha256: str
    shared_contract_sha256: str
    shared_policy_sha256: str
    dq_pit_policy_sha256: str
    signal_package_receipt_sha256: str
    signal_package_receipt_content_sha256: str
    signal_index_sha256: str
    signal_index_content_sha256: str
    run_manifest_sha256: str
    daily_signal_count: int
    requested_start: date
    requested_end: date
    evaluated_start: date
    evaluated_end: date
    capability_receipt_id: str
    capability_receipt_sha256: str
    capability_policy_sha256: str
    capability_evidence_sha256: str
    capability_decision: Literal[
        "CAPABILITY_CONFIRMED_FOR_BOUNDED_PILOT",
        "CAPABILITY_OR_LICENSE_BLOCKED",
    ]
    capability_blocking_reason_codes: tuple[str, ...]
    capability_bounded_pilot_preparation_allowed: bool
    option_event_dq_status: Literal["NOT_EVALUATED"]
    option_event_pit_status: Literal["NOT_EVALUATED"]
    input_admission_status: Literal["UNKNOWN_REQUIRES_PLATFORM_EVIDENCE"]
    subscription: QCSubscriptionContract
    project_file_boundary: QCProjectFileBoundary
    engine_identity: QCEngineIdentityRequirement
    result_mappings: tuple[QCResultMappingRule, ...]
    cloud_run_authorized: Literal[False]
    decision: Literal["QC_ADAPTER_CONTRACT_READY_NO_CLOUD_RUN"]
    safety: QCProjectAdapterSafety
    content_sha256: str

    @field_validator("run_id", "capability_receipt_id")
    @classmethod
    def _validate_ids(cls, value: str, info: ValidationInfo) -> str:
        return _identifier(value, str(info.field_name))

    @field_validator("created_at_utc")
    @classmethod
    def _validate_created_at(cls, value: datetime) -> datetime:
        return _utc(value, "created_at_utc")

    @field_validator("repository_code_sha")
    @classmethod
    def _validate_code_sha(cls, value: str) -> str:
        return _git_object_sha(value, "repository_code_sha")

    @field_validator(
        "adapter_policy_sha256",
        "signal_export_policy_sha256",
        "shared_contract_sha256",
        "shared_policy_sha256",
        "dq_pit_policy_sha256",
        "signal_package_receipt_sha256",
        "signal_package_receipt_content_sha256",
        "signal_index_sha256",
        "signal_index_content_sha256",
        "run_manifest_sha256",
        "capability_receipt_sha256",
        "capability_policy_sha256",
        "capability_evidence_sha256",
        "content_sha256",
    )
    @classmethod
    def _validate_hashes(cls, value: str, info: ValidationInfo) -> str:
        return _sha256(value, str(info.field_name))

    @field_validator("daily_signal_count")
    @classmethod
    def _validate_signal_count(cls, value: int) -> int:
        if value <= 0:
            raise ValueError("daily_signal_count must be positive")
        return value

    @field_validator("capability_blocking_reason_codes")
    @classmethod
    def _validate_reason_codes(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        ordered = tuple(sorted(_required_text(item, "blocking reason") for item in value))
        if value != ordered or len(value) != len(set(value)):
            raise ValueError("capability blocking reasons must be sorted and unique")
        return value

    @model_validator(mode="after")
    def _validate_descriptor(self, info: ValidationInfo) -> Self:
        if not (
            self.requested_start <= self.evaluated_start <= self.evaluated_end
            <= self.requested_end
        ):
            raise ValueError("adapter descriptor ranges are inconsistent")
        blocked = self.capability_decision == "CAPABILITY_OR_LICENSE_BLOCKED"
        if blocked != bool(self.capability_blocking_reason_codes):
            raise ValueError("capability decision does not match blocking reasons")
        if self.capability_bounded_pilot_preparation_allowed == blocked:
            raise ValueError("capability preparation flag does not match decision")
        if tuple(item.mapping_id for item in self.result_mappings) != (
            _EXPECTED_RESULT_MAPPING_IDS
        ):
            raise ValueError("descriptor result mappings drifted")
        allow_unsealed = bool(info.context and info.context.get("allow_unsealed") is True)
        if allow_unsealed and self.content_sha256 == _UNSEALED_SHA256:
            return self
        if self.content_sha256 != self.compute_content_sha256():
            raise ValueError("descriptor content hash does not match canonical semantics")
        return self

    def semantic_payload_without_hash(self) -> dict[str, Any]:
        return self.model_dump(mode="json", exclude={"content_sha256"})

    def compute_content_sha256(self) -> str:
        return _content_sha256(_canonical_json_bytes(self.semantic_payload_without_hash()))

    @classmethod
    def seal(cls, **payload: Any) -> Self:
        if "content_sha256" in payload:
            raise QCProjectAdapterContractError(
                "QC_PROJECT_ADAPTER_HASH_CALLER_SUPPLIED",
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
            descriptor = cls.model_validate_json(content)
        except ValueError as exc:
            raise QCProjectAdapterContractError(
                "QC_PROJECT_ADAPTER_DESCRIPTOR_INVALID", str(exc)
            ) from exc
        if content != descriptor.canonical_bytes:
            raise QCProjectAdapterContractError(
                "QC_PROJECT_ADAPTER_DESCRIPTOR_NOT_CANONICAL",
                "descriptor bytes do not match canonical JSON encoding",
            )
        return descriptor


def load_qc_qqq_options_project_adapter_policy(
    path: Path = DEFAULT_QC_QQQ_OPTIONS_PROJECT_ADAPTER_POLICY_PATH,
    *,
    project_root: Path = PROJECT_ROOT,
) -> QCProjectAdapterPolicyLoadResult:
    resolved = _resolve(path, project_root=project_root)
    try:
        payload = safe_load_yaml_path(resolved)
        if not isinstance(payload, dict):
            raise TypeError("policy root must be a mapping")
        policy = QCProjectAdapterPolicy.model_validate(payload)
    except (OSError, TypeError, ValueError) as exc:
        raise QCProjectAdapterContractError(
            "QC_PROJECT_ADAPTER_POLICY_INVALID", f"{resolved}: {exc}"
        ) from exc
    return QCProjectAdapterPolicyLoadResult(
        policy=policy,
        policy_sha256=sha256_path(resolved),
        policy_path=resolved,
    )


def load_qqq_options_signal_package_for_qc(
    package_root: Path,
    *,
    adapter_policy_path: Path = DEFAULT_QC_QQQ_OPTIONS_PROJECT_ADAPTER_POLICY_PATH,
    signal_policy_path: Path = DEFAULT_QQQ_OPTIONS_SIGNAL_EXPORT_POLICY_PATH,
    project_root: Path = PROJECT_ROOT,
) -> LoadedQQQOptionsSignalPackage:
    adapter_loaded = load_qc_qqq_options_project_adapter_policy(
        adapter_policy_path, project_root=project_root
    )
    signal_loaded = load_qqq_options_signal_export_policy(
        signal_policy_path, project_root=project_root
    )
    root = _resolve(package_root, project_root=project_root)
    if root.is_symlink() or not root.is_dir():
        raise QCProjectAdapterContractError(
            "QC_PROJECT_ADAPTER_PACKAGE_ROOT_INVALID",
            "signal package root must be a non-symlink directory",
        )

    actual_paths: list[str] = []
    for candidate in root.rglob("*"):
        if candidate.is_symlink():
            raise QCProjectAdapterContractError(
                "QC_PROJECT_ADAPTER_PACKAGE_SYMLINK_PROHIBITED",
                str(candidate),
            )
        if candidate.is_file():
            actual_paths.append(candidate.relative_to(root).as_posix())
        elif not candidate.is_dir():
            raise QCProjectAdapterContractError(
                "QC_PROJECT_ADAPTER_PACKAGE_ENTRY_INVALID",
                str(candidate),
            )
    actual_inventory = tuple(sorted(actual_paths))
    if "package_receipt.json" not in actual_inventory:
        raise QCProjectAdapterContractError(
            "QC_PROJECT_ADAPTER_PACKAGE_RECEIPT_MISSING",
            "package_receipt.json is required",
        )

    receipt_bytes = _read_package_file(root, "package_receipt.json")
    receipt = _parse_package_receipt(receipt_bytes)
    daily_paths = tuple(item.relative_path for item in receipt.daily_signal_artifacts)
    for relative_path in daily_paths:
        match = _DAILY_SIGNAL_PATH_PATTERN.fullmatch(relative_path)
        if match is None:
            raise QCProjectAdapterContractError(
                "QC_PROJECT_ADAPTER_DAILY_SIGNAL_PATH_INVALID", relative_path
            )
        try:
            date.fromisoformat(match.group(1))
        except ValueError as exc:
            raise QCProjectAdapterContractError(
                "QC_PROJECT_ADAPTER_DAILY_SIGNAL_PATH_INVALID", relative_path
            ) from exc
    expected_inventory = tuple(
        sorted(
            (
                *daily_paths,
                "package_receipt.json",
                "run_manifest.json",
                "signal_index.json",
            )
        )
    )
    if actual_inventory != expected_inventory:
        missing = sorted(set(expected_inventory) - set(actual_inventory))
        extra = sorted(set(actual_inventory) - set(expected_inventory))
        raise QCProjectAdapterContractError(
            "QC_PROJECT_ADAPTER_PACKAGE_INVENTORY_MISMATCH",
            f"missing={missing}; extra={extra}",
        )

    file_bytes = {path: _read_package_file(root, path) for path in actual_inventory}
    file_sha256s = {path: _content_sha256(content) for path, content in file_bytes.items()}
    file_byte_counts = {path: len(content) for path, content in file_bytes.items()}
    _verify_artifact(
        receipt.run_manifest_artifact,
        file_bytes["run_manifest.json"],
    )
    _verify_artifact(
        receipt.signal_index_artifact,
        file_bytes["signal_index.json"],
    )
    for artifact in receipt.daily_signal_artifacts:
        _verify_artifact(artifact, file_bytes[artifact.relative_path])

    index = _parse_signal_index(file_bytes["signal_index.json"])
    if index.artifacts != receipt.daily_signal_artifacts:
        raise QCProjectAdapterContractError(
            "QC_PROJECT_ADAPTER_INDEX_RECEIPT_MISMATCH",
            "signal index artifacts do not match package receipt",
        )
    run_manifest = _parse_run_manifest(file_bytes["run_manifest.json"])
    daily_signals = tuple(
        _parse_daily_signal(file_bytes[artifact.relative_path])
        for artifact in receipt.daily_signal_artifacts
    )

    policy = adapter_loaded.policy
    if signal_loaded.policy_sha256 != policy.signal_export_policy_sha256:
        raise QCProjectAdapterContractError(
            "QC_PROJECT_ADAPTER_SIGNAL_POLICY_HASH_MISMATCH",
            "current signal policy bytes do not match adapter authority",
        )
    if receipt.policy_sha256 != signal_loaded.policy_sha256:
        raise QCProjectAdapterContractError(
            "QC_PROJECT_ADAPTER_PACKAGE_POLICY_MISMATCH",
            "package receipt is not bound to the current signal policy",
        )
    predecessor_hashes = (
        (receipt.shared_contract_sha256, policy.shared_contract_sha256),
        (receipt.shared_policy_sha256, policy.shared_policy_sha256),
        (receipt.dq_pit_policy_sha256, policy.dq_pit_policy_sha256),
    )
    if any(observed != expected for observed, expected in predecessor_hashes):
        raise QCProjectAdapterContractError(
            "QC_PROJECT_ADAPTER_PREDECESSOR_HASH_MISMATCH",
            "signal package predecessor hashes drifted",
        )
    _verify_package_semantics(
        receipt=receipt,
        index=index,
        run_manifest=run_manifest,
        daily_signals=daily_signals,
        policy=policy,
    )
    package = QQQOptionsSignalPackage(
        policy_sha256=signal_loaded.policy_sha256,
        daily_signals=daily_signals,
        signal_index=index,
        run_manifest=run_manifest,
        receipt=receipt,
    )
    if package.files != file_bytes:
        raise QCProjectAdapterContractError(
            "QC_PROJECT_ADAPTER_PACKAGE_CANONICAL_REPLAY_MISMATCH",
            "reconstructed signal package bytes do not match disk",
        )
    return LoadedQQQOptionsSignalPackage(
        package_root=root,
        package=package,
        file_sha256s=file_sha256s,
        file_byte_counts=file_byte_counts,
    )


def build_qc_qqq_options_project_adapter_descriptor(
    *,
    signal_package_root: Path,
    capability_receipt_path: Path,
    adapter_policy_path: Path = DEFAULT_QC_QQQ_OPTIONS_PROJECT_ADAPTER_POLICY_PATH,
    signal_policy_path: Path = DEFAULT_QQQ_OPTIONS_SIGNAL_EXPORT_POLICY_PATH,
    capability_policy_path: Path = DEFAULT_QC_QQQ_OPTIONS_CAPABILITY_POLICY_PATH,
    capability_evidence_path: Path = (
        DEFAULT_QC_QQQ_OPTIONS_CAPABILITY_EVIDENCE_TEMPLATE_PATH
    ),
    project_root: Path = PROJECT_ROOT,
) -> QCProjectAdapterDescriptor:
    adapter_loaded = load_qc_qqq_options_project_adapter_policy(
        adapter_policy_path, project_root=project_root
    )
    loaded_package = load_qqq_options_signal_package_for_qc(
        signal_package_root,
        adapter_policy_path=adapter_policy_path,
        signal_policy_path=signal_policy_path,
        project_root=project_root,
    )
    capability_receipt = verify_qc_qqq_options_capability_admission_receipt(
        capability_receipt_path,
        policy_path=capability_policy_path,
        evidence_path=capability_evidence_path,
        project_root=project_root,
    )
    receipt_path = _resolve(capability_receipt_path, project_root=project_root)
    package = loaded_package.package
    signal_receipt = package.receipt
    manifest = package.run_manifest
    policy = adapter_loaded.policy
    descriptor = QCProjectAdapterDescriptor.seal(
        schema_version="qc_qqq_options_project_adapter_descriptor.v1",
        run_id=signal_receipt.run_id,
        created_at_utc=signal_receipt.created_at_utc,
        repository_code_sha=signal_receipt.repository_code_sha,
        adapter_policy_id=policy.policy_id,
        adapter_policy_version=policy.policy_version,
        adapter_policy_sha256=adapter_loaded.policy_sha256,
        signal_export_policy_sha256=signal_receipt.policy_sha256,
        shared_contract_sha256=signal_receipt.shared_contract_sha256,
        shared_policy_sha256=signal_receipt.shared_policy_sha256,
        dq_pit_policy_sha256=signal_receipt.dq_pit_policy_sha256,
        signal_package_receipt_sha256=loaded_package.file_sha256s[
            "package_receipt.json"
        ],
        signal_package_receipt_content_sha256=signal_receipt.content_sha256,
        signal_index_sha256=loaded_package.file_sha256s["signal_index.json"],
        signal_index_content_sha256=package.signal_index.content_sha256,
        run_manifest_sha256=loaded_package.file_sha256s["run_manifest.json"],
        daily_signal_count=len(package.daily_signals),
        requested_start=manifest.requested_start,
        requested_end=manifest.requested_end,
        evaluated_start=manifest.evaluated_start,
        evaluated_end=manifest.evaluated_end,
        capability_receipt_id=capability_receipt.receipt_id,
        capability_receipt_sha256=sha256_path(receipt_path),
        capability_policy_sha256=capability_receipt.policy_sha256,
        capability_evidence_sha256=capability_receipt.evidence_sha256,
        capability_decision=capability_receipt.decision,
        capability_blocking_reason_codes=capability_receipt.blocking_reason_codes,
        capability_bounded_pilot_preparation_allowed=(
            capability_receipt.bounded_pilot_preparation_allowed
        ),
        option_event_dq_status=signal_receipt.option_event_dq_status,
        option_event_pit_status=signal_receipt.option_event_pit_status,
        input_admission_status=policy.project_file_boundary.complete_signal_input_admission,
        subscription=policy.subscription,
        project_file_boundary=policy.project_file_boundary,
        engine_identity=policy.engine_identity,
        result_mappings=policy.result_mappings,
        cloud_run_authorized=False,
        decision=policy.decision,
        safety=policy.safety,
    )
    if len(descriptor.canonical_bytes) > policy.project_file_boundary.maximum_file_bytes:
        raise QCProjectAdapterContractError(
            "QC_PROJECT_ADAPTER_DESCRIPTOR_TOO_LARGE",
            f"descriptor bytes={len(descriptor.canonical_bytes)} exceed Free project-file maximum",
        )
    return descriptor


def _verify_package_semantics(
    *,
    receipt: QQQOptionsSignalPackageReceipt,
    index: QQQOptionsSignalIndex,
    run_manifest: RunManifestRecord,
    daily_signals: tuple[DailySignalRecord, ...],
    policy: QCProjectAdapterPolicy,
) -> None:
    if receipt.run_id != index.run_id or receipt.run_id != run_manifest.run_id:
        raise QCProjectAdapterContractError(
            "QC_PROJECT_ADAPTER_RUN_ID_MISMATCH", "package run ids do not match"
        )
    manifest_receipt_fields = (
        run_manifest.created_at_utc == receipt.created_at_utc,
        run_manifest.producer_version == receipt.producer_version,
        run_manifest.repository_code_sha == receipt.repository_code_sha,
        run_manifest.policy_id == receipt.policy_id,
        run_manifest.policy_version == receipt.policy_version,
        run_manifest.policy_sha256 == receipt.policy_sha256,
        run_manifest.contract_schema_sha256 == receipt.shared_contract_sha256,
        run_manifest.storage_timezone == "UTC",
        run_manifest.exchange_timezone == "America/New_York",
        run_manifest.dq_status == "PASS",
        run_manifest.pit_status == "PASS",
        run_manifest.export_classification == receipt.export_classification,
        run_manifest.safety == receipt.safety,
    )
    if not all(manifest_receipt_fields):
        raise QCProjectAdapterContractError(
            "QC_PROJECT_ADAPTER_MANIFEST_RECEIPT_SEMANTIC_MISMATCH",
            "manifest envelope is not bound to the package receipt",
        )
    if receipt.research_window_role != "PRIMARY" or (
        receipt.research_window_authority is not None
    ):
        raise QCProjectAdapterContractError(
            "QC_PROJECT_ADAPTER_NON_PRIMARY_AUTHORITY_NOT_APPROVED",
            "2484 currently admits only the project primary research window",
        )
    if receipt.option_event_dq_status != "NOT_EVALUATED" or (
        receipt.option_event_pit_status != "NOT_EVALUATED"
    ):
        raise QCProjectAdapterContractError(
            "QC_PROJECT_ADAPTER_OPTION_EVENT_STATUS_INVALID",
            "adapter cannot promote option-event DQ/PIT",
        )
    if receipt.local_cached_data_gate.status != "PASS":
        raise QCProjectAdapterContractError(
            "QC_PROJECT_ADAPTER_LOCAL_DQ_NOT_PASS", "local cached-data DQ must pass"
        )
    if run_manifest.underlying != "QQQ" or run_manifest.account_type != "CASH":
        raise QCProjectAdapterContractError(
            "QC_PROJECT_ADAPTER_MANIFEST_SCOPE_MISMATCH",
            "adapter requires QQQ in a CASH research account",
        )
    if run_manifest.signal_resolution != "DAILY" or (
        run_manifest.execution_resolution != "MINUTE"
    ):
        raise QCProjectAdapterContractError(
            "QC_PROJECT_ADAPTER_MANIFEST_RESOLUTION_MISMATCH",
            "adapter requires DAILY signals and MINUTE execution data",
        )
    if run_manifest.engine_identity_status != "UNKNOWN" or (
        run_manifest.engine_identity is not None
    ):
        raise QCProjectAdapterContractError(
            "QC_PROJECT_ADAPTER_ENGINE_IDENTITY_PRETENDED",
            "2483 manifest must not pretend a QuantConnect engine identity",
        )
    if run_manifest.evidence_admission_decision != "CAPABILITY_OR_LICENSE_BLOCKED":
        raise QCProjectAdapterContractError(
            "QC_PROJECT_ADAPTER_MANIFEST_ADMISSION_MISMATCH",
            "2483 manifest must preserve its blocked external admission state",
        )
    if run_manifest.signal_artifact_sha256 != receipt.signal_index_artifact.sha256:
        raise QCProjectAdapterContractError(
            "QC_PROJECT_ADAPTER_MANIFEST_INDEX_MISMATCH",
            "manifest signal artifact does not match receipt index",
        )
    if run_manifest.requested_start != policy.primary_research_start or (
        run_manifest.evaluated_start != policy.primary_research_start
    ):
        raise QCProjectAdapterContractError(
            "QC_PROJECT_ADAPTER_PRIMARY_START_MISMATCH",
            "primary requested/evaluated start must be 2021-02-22",
        )
    if len(daily_signals) != len(index.artifacts):
        raise QCProjectAdapterContractError(
            "QC_PROJECT_ADAPTER_SIGNAL_COUNT_MISMATCH",
            "daily signal count does not match index",
        )
    observed_sessions: list[date] = []
    for record, artifact in zip(daily_signals, index.artifacts, strict=True):
        session = record.signal_session
        observed_sessions.append(session)
        if artifact.relative_path != f"daily_signals/{session.isoformat()}.json":
            raise QCProjectAdapterContractError(
                "QC_PROJECT_ADAPTER_SIGNAL_PATH_SESSION_MISMATCH",
                artifact.relative_path,
            )
        shared_fields = (
            record.run_id == receipt.run_id,
            record.repository_code_sha == receipt.repository_code_sha,
            record.policy_sha256 == receipt.policy_sha256,
            record.contract_schema_sha256 == receipt.shared_contract_sha256,
            record.policy_id == run_manifest.policy_id,
            record.policy_version == run_manifest.policy_version,
            record.requested_start == run_manifest.requested_start,
            record.requested_end == run_manifest.requested_end,
            record.evaluated_start == run_manifest.evaluated_start,
            record.evaluated_end == run_manifest.evaluated_end,
            record.created_at_utc == run_manifest.created_at_utc,
            record.producer_version == run_manifest.producer_version,
            record.lineage_id == run_manifest.lineage_id,
            record.dq_status == "PASS",
            record.pit_status == "PASS",
            record.export_classification == "EXPORT_ALLOWED_DERIVED",
            record.storage_timezone == "UTC",
            record.exchange_timezone == "America/New_York",
            record.safety == run_manifest.safety,
        )
        if not all(shared_fields):
            raise QCProjectAdapterContractError(
                "QC_PROJECT_ADAPTER_SIGNAL_SEMANTIC_MISMATCH",
                artifact.relative_path,
            )
    if tuple(observed_sessions) != tuple(sorted(observed_sessions)) or len(
        observed_sessions
    ) != len(set(observed_sessions)):
        raise QCProjectAdapterContractError(
            "QC_PROJECT_ADAPTER_SIGNAL_SESSION_ORDER_INVALID",
            "daily signal sessions must be sorted and unique",
        )
    if (
        run_manifest.evaluated_start is None
        or run_manifest.evaluated_end is None
        or observed_sessions[0] != run_manifest.evaluated_start
        or observed_sessions[-1] != run_manifest.evaluated_end
    ):
        raise QCProjectAdapterContractError(
            "QC_PROJECT_ADAPTER_SIGNAL_WINDOW_COVERAGE_MISMATCH",
            "daily signal boundaries do not match the evaluated window",
        )


def _verify_artifact(artifact: SignalArtifactDigest, content: bytes) -> None:
    if _content_sha256(content) != artifact.sha256 or len(content) != artifact.byte_count:
        raise QCProjectAdapterContractError(
            "QC_PROJECT_ADAPTER_ARTIFACT_BINDING_MISMATCH",
            artifact.relative_path,
        )


def _parse_package_receipt(content: bytes) -> QQQOptionsSignalPackageReceipt:
    try:
        return QQQOptionsSignalPackageReceipt.from_json_bytes(content)
    except ValueError as exc:
        raise QCProjectAdapterContractError(
            "QC_PROJECT_ADAPTER_PACKAGE_RECEIPT_INVALID", str(exc)
        ) from exc


def _parse_signal_index(content: bytes) -> QQQOptionsSignalIndex:
    try:
        return QQQOptionsSignalIndex.from_json_bytes(content)
    except ValueError as exc:
        raise QCProjectAdapterContractError(
            "QC_PROJECT_ADAPTER_SIGNAL_INDEX_INVALID", str(exc)
        ) from exc


def _parse_run_manifest(content: bytes) -> RunManifestRecord:
    try:
        return RunManifestRecord.from_json_bytes(content)
    except ValueError as exc:
        raise QCProjectAdapterContractError(
            "QC_PROJECT_ADAPTER_RUN_MANIFEST_INVALID", str(exc)
        ) from exc


def _parse_daily_signal(content: bytes) -> DailySignalRecord:
    try:
        return DailySignalRecord.from_json_bytes(content)
    except ValueError as exc:
        raise QCProjectAdapterContractError(
            "QC_PROJECT_ADAPTER_DAILY_SIGNAL_INVALID", str(exc)
        ) from exc


def _read_package_file(root: Path, relative_path: str) -> bytes:
    resolved = root / Path(relative_path)
    try:
        return resolved.read_bytes()
    except OSError as exc:
        raise QCProjectAdapterContractError(
            "QC_PROJECT_ADAPTER_PACKAGE_READ_FAILED", f"{relative_path}: {exc}"
        ) from exc


def _resolve(path: Path, *, project_root: Path) -> Path:
    return path if path.is_absolute() else project_root / path


__all__ = [
    "DEFAULT_QC_QQQ_OPTIONS_PROJECT_ADAPTER_POLICY_PATH",
    "LoadedQQQOptionsSignalPackage",
    "QCEngineIdentityRequirement",
    "QCProjectAdapterContractError",
    "QCProjectAdapterDescriptor",
    "QCProjectAdapterPolicy",
    "QCProjectAdapterPolicyLoadResult",
    "QCProjectAdapterSafety",
    "QCProjectFileBoundary",
    "QCResultMappingRule",
    "QCSubscriptionContract",
    "build_qc_qqq_options_project_adapter_descriptor",
    "load_qc_qqq_options_project_adapter_policy",
    "load_qqq_options_signal_package_for_qc",
]
