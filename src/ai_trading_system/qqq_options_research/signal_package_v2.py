from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import tempfile
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path, PurePosixPath
from typing import Any, Literal, Self

from pydantic import BaseModel, ConfigDict, ValidationInfo, field_validator, model_validator

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.data.quality_execution import (
    DataQualityExecutionError,
    verify_data_quality_execution_receipt,
)
from ai_trading_system.platform.artifacts import write_bytes_atomic
from ai_trading_system.qqq_options_research.contracts import (
    QQQ_OPTIONS_CONTRACT_SHA256,
    DailySignalRecord,
    QQQOptionsContractError,
    QQQOptionsSafetyBoundary,
    RunManifestRecord,
    SignalDirection,
)
from ai_trading_system.qqq_options_research.dq_pit_identity import (
    LocalCachedDataGateDeclaration,
    load_qqq_options_dq_pit_identity_policy,
)
from ai_trading_system.qqq_options_research.policy import (
    load_qqq_options_shared_contract_policy,
)
from ai_trading_system.trading_calendar import (
    US_EQUITY_MARKET_TIMEZONE,
    is_us_equity_trading_day,
    us_equity_market_session,
)
from ai_trading_system.us_equity_special_closure_policy import (
    US_EQUITY_DECISION_CALENDAR_ID,
    load_us_equity_special_closure_policy_by_identity,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

DEFAULT_QQQ_OPTIONS_SIGNAL_EXPORT_POLICY_PATH = Path(
    "config/research/qqq_options_signal_export_v2.yaml"
)

_SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
_GIT_OBJECT_SHA_PATTERN = re.compile(r"^(?:[0-9a-f]{40}|[0-9a-f]{64})$")
_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]*$")
_PATH_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]*$")
_UNSEALED_SHA256 = "0" * 64
_PACKAGE_LAYOUT = (
    "daily_signals/<YYYY-MM-DD>.json",
    "package_receipt.json",
    "run_manifest.json",
    "signal_index.json",
)
_APPROVED_SIGNAL_VALUES = ("FLAT", "LONG_CALL", "LONG_PUT")
_NON_PRIMARY_WINDOW_ROLES = ("PROXY", "SENSITIVITY", "STRESS")

ResearchWindowRole = Literal["PRIMARY", "PROXY", "SENSITIVITY", "STRESS"]


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


def _required_text(value: str, field: str) -> str:
    if not value or value != value.strip() or any(ord(char) < 32 for char in value):
        raise ValueError(f"{field} must be non-empty normalized text")
    return value


def _identifier(value: str, field: str) -> str:
    checked = _required_text(value, field)
    if not _IDENTIFIER_PATTERN.fullmatch(checked):
        raise ValueError(f"{field} must be a portable identifier")
    return checked


def _path_identifier(value: str, field: str) -> str:
    checked = _required_text(value, field)
    if not _PATH_IDENTIFIER_PATTERN.fullmatch(checked):
        raise ValueError(f"{field} must be a filesystem-safe identifier")
    return checked


def _sha256(value: str, field: str) -> str:
    if not _SHA256_PATTERN.fullmatch(value):
        raise ValueError(f"{field} must be lowercase SHA-256")
    return value


def _git_object_sha(value: str, field: str) -> str:
    if not _GIT_OBJECT_SHA_PATTERN.fullmatch(value):
        raise ValueError(f"{field} must be a lowercase Git object SHA")
    return value


def _utc(value: datetime, field: str) -> datetime:
    offset = value.utcoffset()
    if value.tzinfo is None or offset is None:
        raise ValueError(f"{field} must be timezone-aware")
    if offset.total_seconds() != 0:
        raise ValueError(f"{field} must use UTC offset")
    return value.astimezone(UTC)


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


def _content_sha256(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


class SignalSourceArtifact(_StrictModel):
    artifact_id: str
    locator: str
    sha256: str
    byte_count: int
    export_classification: Literal["EXPORT_ALLOWED_DERIVED"]

    @field_validator("artifact_id")
    @classmethod
    def _validate_artifact_id(cls, value: str) -> str:
        return _identifier(value, "artifact_id")

    @field_validator("locator")
    @classmethod
    def _validate_locator(cls, value: str) -> str:
        return _required_text(value, "locator")

    @field_validator("sha256")
    @classmethod
    def _validate_hash(cls, value: str) -> str:
        return _sha256(value, "sha256")

    @field_validator("byte_count")
    @classmethod
    def _validate_byte_count(cls, value: int) -> int:
        if value <= 0:
            raise ValueError("source artifact must contain bytes")
        return value


class NormalizedDailySignalInput(_StrictModel):
    signal_session: date
    source_data_cutoff_utc: datetime
    generated_at_utc: datetime
    signal: SignalDirection

    @field_validator("source_data_cutoff_utc", "generated_at_utc")
    @classmethod
    def _validate_times(cls, value: datetime, info: ValidationInfo) -> datetime:
        return _utc(value, str(info.field_name))

    @model_validator(mode="after")
    def _validate_order(self) -> Self:
        if self.generated_at_utc < self.source_data_cutoff_utc:
            raise ValueError("signal generation cannot precede the source data cutoff")
        return self


def canonical_normalized_signal_source_bytes(
    signals: tuple[NormalizedDailySignalInput, ...],
) -> bytes:
    ordered = tuple(sorted(signals, key=lambda item: item.signal_session))
    return _canonical_json_bytes(
        {
            "schema_version": "qqq_options_normalized_signal_input.v1",
            "signals": [item.model_dump(mode="json") for item in ordered],
        }
    )


class ReviewedResearchWindowAuthority(_StrictModel):
    authority_id: str
    role: Literal["PROXY", "SENSITIVITY", "STRESS"]
    status: Literal["REVIEWED_ACTIVE"]
    owner: str
    owner_decision: str
    rationale: str
    dq_caveat: str
    review_condition: str
    requested_start: date
    evaluated_start: date

    @field_validator("authority_id")
    @classmethod
    def _validate_authority_id(cls, value: str) -> str:
        return _identifier(value, "authority_id")

    @field_validator("owner", "rationale", "dq_caveat", "review_condition")
    @classmethod
    def _validate_text(cls, value: str, info: ValidationInfo) -> str:
        return _required_text(value, str(info.field_name))

    @field_validator("owner_decision")
    @classmethod
    def _validate_owner_decision(cls, value: str) -> str:
        checked = _required_text(value, "owner_decision")
        if not checked.startswith("owner_decision:"):
            raise ValueError("non-primary window authority requires an owner_decision token")
        return checked


class SignalArtifactDigest(_StrictModel):
    relative_path: str
    sha256: str
    byte_count: int

    @field_validator("relative_path")
    @classmethod
    def _validate_relative_path(cls, value: str) -> str:
        checked = _required_text(value, "relative_path")
        pure = PurePosixPath(checked)
        if pure.is_absolute() or ".." in pure.parts or checked != pure.as_posix():
            raise ValueError("relative_path must be normalized and traversal-free")
        return checked

    @field_validator("sha256")
    @classmethod
    def _validate_hash(cls, value: str) -> str:
        return _sha256(value, "sha256")

    @field_validator("byte_count")
    @classmethod
    def _validate_byte_count(cls, value: int) -> int:
        if value <= 0:
            raise ValueError("package artifacts must contain bytes")
        return value


class QQQOptionsSignalIndex(_StrictModel):
    schema_version: Literal["qqq_options_signal_index.v1"]
    run_id: str
    artifacts: tuple[SignalArtifactDigest, ...]
    content_sha256: str

    @field_validator("run_id")
    @classmethod
    def _validate_run_id(cls, value: str) -> str:
        return _path_identifier(value, "run_id")

    @field_validator("content_sha256")
    @classmethod
    def _validate_hash(cls, value: str) -> str:
        return _sha256(value, "content_sha256")

    @model_validator(mode="after")
    def _validate_index(self, info: ValidationInfo) -> Self:
        paths = tuple(item.relative_path for item in self.artifacts)
        if not paths or paths != tuple(sorted(paths)) or len(paths) != len(set(paths)):
            raise ValueError("signal artifacts must be non-empty, sorted, and unique")
        allow_unsealed = bool(info.context and info.context.get("allow_unsealed") is True)
        if allow_unsealed and self.content_sha256 == _UNSEALED_SHA256:
            return self
        if self.content_sha256 != self.compute_content_sha256():
            raise ValueError("signal index content hash does not match canonical semantics")
        return self

    def semantic_payload_without_hash(self) -> dict[str, Any]:
        return self.model_dump(mode="json", exclude={"content_sha256"})

    def compute_content_sha256(self) -> str:
        return _content_sha256(_canonical_json_bytes(self.semantic_payload_without_hash()))

    @classmethod
    def seal(cls, *, run_id: str, artifacts: tuple[SignalArtifactDigest, ...]) -> Self:
        payload = {
            "schema_version": "qqq_options_signal_index.v1",
            "run_id": run_id,
            "artifacts": artifacts,
        }
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

    @classmethod
    def from_json_bytes(cls, content: bytes) -> Self:
        try:
            index = cls.model_validate_json(content)
        except ValueError as exc:
            raise QQQOptionsContractError("QQQ_OPTIONS_SIGNAL_INDEX_INVALID", str(exc)) from exc
        if content != index.canonical_bytes:
            raise QQQOptionsContractError(
                "QQQ_OPTIONS_SIGNAL_INDEX_NOT_CANONICAL",
                "signal index bytes do not match canonical JSON encoding",
            )
        return index


class QQQOptionsSignalPackageReceipt(_StrictModel):
    schema_version: Literal["qqq_options_signal_package_receipt.v1"]
    run_id: str
    created_at_utc: datetime
    producer_version: str
    repository_code_sha: str
    policy_id: Literal["qqq_options_signal_export_v1", "qqq_options_signal_export_v2"]
    policy_version: Literal["1.0.0", "2.0.0"]
    policy_sha256: str
    shared_contract_sha256: str
    shared_policy_sha256: str
    dq_pit_policy_sha256: str
    calendar_id: Literal["XNYS"]
    calendar_policy_id: Literal["us_equity_special_closure_registry"]
    calendar_policy_version: str
    calendar_policy_sha256: str
    research_window_role: ResearchWindowRole
    research_window_authority: ReviewedResearchWindowAuthority | None
    source_artifact: SignalSourceArtifact
    local_dq_execution_receipt: SignalSourceArtifact
    local_cached_data_gate: LocalCachedDataGateDeclaration
    run_manifest_artifact: SignalArtifactDigest
    signal_index_artifact: SignalArtifactDigest
    daily_signal_artifacts: tuple[SignalArtifactDigest, ...]
    option_event_dq_status: Literal["NOT_EVALUATED"]
    option_event_pit_status: Literal["NOT_EVALUATED"]
    export_classification: Literal["EXPORT_ALLOWED_DERIVED"]
    safety: QQQOptionsSafetyBoundary
    content_sha256: str

    @field_validator("run_id")
    @classmethod
    def _validate_run_id(cls, value: str) -> str:
        return _path_identifier(value, "run_id")

    @field_validator("created_at_utc")
    @classmethod
    def _validate_created_at(cls, value: datetime) -> datetime:
        return _utc(value, "created_at_utc")

    @field_validator("producer_version", "calendar_policy_version")
    @classmethod
    def _validate_text(cls, value: str, info: ValidationInfo) -> str:
        return _required_text(value, str(info.field_name))

    @field_validator("repository_code_sha")
    @classmethod
    def _validate_code_sha(cls, value: str) -> str:
        return _git_object_sha(value, "repository_code_sha")

    @field_validator(
        "policy_sha256",
        "shared_contract_sha256",
        "shared_policy_sha256",
        "dq_pit_policy_sha256",
        "calendar_policy_sha256",
        "content_sha256",
    )
    @classmethod
    def _validate_hashes(cls, value: str, info: ValidationInfo) -> str:
        return _sha256(value, str(info.field_name))

    @model_validator(mode="after")
    def _validate_receipt(self, info: ValidationInfo) -> Self:
        if (self.policy_id, self.policy_version) not in {
            ("qqq_options_signal_export_v1", "1.0.0"),
            ("qqq_options_signal_export_v2", "2.0.0"),
        }:
            raise ValueError("signal package policy id/version pair drifted")
        paths = tuple(item.relative_path for item in self.daily_signal_artifacts)
        if not paths or paths != tuple(sorted(paths)) or len(paths) != len(set(paths)):
            raise ValueError("daily signal receipt artifacts must be sorted and unique")
        if self.run_manifest_artifact.relative_path != "run_manifest.json":
            raise ValueError("receipt run manifest path drifted")
        if self.signal_index_artifact.relative_path != "signal_index.json":
            raise ValueError("receipt signal index path drifted")
        if self.local_cached_data_gate.status != "PASS":
            raise ValueError("signal package requires local cached-data DQ PASS")
        if self.local_cached_data_gate.scope != "CACHED_MARKET_MACRO":
            raise ValueError("signal package local DQ scope must be CACHED_MARKET_MACRO")
        if self.research_window_role == "PRIMARY":
            if self.research_window_authority is not None:
                raise ValueError("primary window cannot carry non-primary authority")
        elif (
            self.research_window_authority is None
            or self.research_window_authority.role != self.research_window_role
        ):
            raise ValueError("non-primary window requires matching reviewed authority")
        allow_unsealed = bool(info.context and info.context.get("allow_unsealed") is True)
        if allow_unsealed and self.content_sha256 == _UNSEALED_SHA256:
            return self
        if self.content_sha256 != self.compute_content_sha256():
            raise ValueError("package receipt content hash does not match canonical semantics")
        return self

    def semantic_payload_without_hash(self) -> dict[str, Any]:
        return self.model_dump(mode="json", exclude={"content_sha256"})

    def compute_content_sha256(self) -> str:
        return _content_sha256(_canonical_json_bytes(self.semantic_payload_without_hash()))

    @classmethod
    def seal(cls, **payload: Any) -> Self:
        if "content_sha256" in payload:
            raise QQQOptionsContractError(
                "QQQ_OPTIONS_SIGNAL_RECEIPT_HASH_CALLER_SUPPLIED",
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

    @classmethod
    def from_json_bytes(cls, content: bytes) -> Self:
        try:
            receipt = cls.model_validate_json(content)
        except ValueError as exc:
            raise QQQOptionsContractError("QQQ_OPTIONS_SIGNAL_RECEIPT_INVALID", str(exc)) from exc
        if content != receipt.canonical_bytes:
            raise QQQOptionsContractError(
                "QQQ_OPTIONS_SIGNAL_RECEIPT_NOT_CANONICAL",
                "package receipt bytes do not match canonical JSON encoding",
            )
        return receipt


class QQQOptionsSignalExportPolicy(_StrictModel):
    schema_version: Literal["qqq_options_signal_export_policy.v1"]
    policy_id: Literal["qqq_options_signal_export_v1", "qqq_options_signal_export_v2"]
    policy_version: Literal["1.0.0", "2.0.0"]
    status: Literal["REVIEWED_OFFLINE_BASELINE"]
    owner: str
    owner_instruction: str
    rationale: str
    intended_effect: str
    validation_plan: str
    review_condition: str
    expiry_condition: str
    shared_contract_sha256: str
    shared_policy_sha256: str
    dq_pit_policy_sha256: str
    calendar_id: Literal["XNYS"]
    calendar_policy_id: Literal["us_equity_special_closure_registry"]
    calendar_policy_version: Literal["1.0.0", "1.1.0"]
    calendar_policy_sha256: str
    input_mode: Literal["PRE_NORMALIZED_ONLY"]
    approved_signal_values: tuple[SignalDirection, ...]
    etf_signal_mapping_status: Literal["UNKNOWN_REQUIRES_OWNER_REVIEW"]
    etf_signal_mapping_allowed: Literal[False]
    signal_lag_sessions: Literal[1]
    effective_session_rule: Literal["NEXT_VALID_US_EQUITY_SESSION"]
    coverage_rule: Literal["EXACTLY_ONE_SIGNAL_PER_EVALUATED_SESSION"]
    source_cutoff_rule: Literal["AT_OR_AFTER_REVIEWED_SESSION_CLOSE"]
    generation_rule: Literal["SAME_EXCHANGE_SESSION_AS_SOURCE_CUTOFF"]
    primary_research_start: date
    primary_window_role: Literal["PRIMARY"]
    non_primary_window_roles: tuple[Literal["PROXY", "SENSITIVITY", "STRESS"], ...]
    approved_non_primary_window_authorities: tuple[ReviewedResearchWindowAuthority, ...]
    non_primary_window_requires_reviewed_authority: Literal[True]
    non_primary_window_requires_dq_caveat: Literal[True]
    legacy_non_default_start: date
    legacy_non_default_start_is_default: Literal[False]
    package_layout: tuple[str, ...]
    canonical_json: Literal["UTF8_SORTED_KEYS_INDENT2_LF_NO_NAN"]
    artifact_export_classification: Literal["EXPORT_ALLOWED_DERIVED"]
    option_event_dq_status: Literal["NOT_EVALUATED"]
    option_event_pit_status: Literal["NOT_EVALUATED"]
    safety: QQQOptionsSafetyBoundary

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
        "shared_contract_sha256",
        "shared_policy_sha256",
        "dq_pit_policy_sha256",
        "calendar_policy_sha256",
    )
    @classmethod
    def _validate_hashes(cls, value: str, info: ValidationInfo) -> str:
        return _sha256(value, str(info.field_name))

    @model_validator(mode="after")
    def _validate_freeze(self) -> Self:
        if (self.policy_id, self.policy_version) not in {
            ("qqq_options_signal_export_v1", "1.0.0"),
            ("qqq_options_signal_export_v2", "2.0.0"),
        }:
            raise ValueError("signal export policy id/version pair drifted")
        if self.shared_contract_sha256 != QQQ_OPTIONS_CONTRACT_SHA256:
            raise ValueError("2483 policy must bind the reviewed 2481 contract hash")
        if self.approved_signal_values != _APPROVED_SIGNAL_VALUES:
            raise ValueError("approved signals differ from the 2481 public enum")
        if self.package_layout != _PACKAGE_LAYOUT:
            raise ValueError("signal package layout differs from the reviewed 2483 freeze")
        if self.non_primary_window_roles != _NON_PRIMARY_WINDOW_ROLES:
            raise ValueError("non-primary research roles differ from the reviewed freeze")
        if self.primary_research_start != date(2021, 2, 22):
            raise ValueError("primary research start must remain 2021-02-22")
        if self.legacy_non_default_start != date(2022, 12, 1):
            raise ValueError("legacy non-default start must remain 2022-12-01")
        authority_ids = tuple(
            authority.authority_id for authority in self.approved_non_primary_window_authorities
        )
        if authority_ids != tuple(sorted(authority_ids)) or len(authority_ids) != len(
            set(authority_ids)
        ):
            raise ValueError("non-primary authorities must be sorted and unique")
        return self


@dataclass(frozen=True)
class QQQOptionsSignalExportPolicyLoadResult:
    policy: QQQOptionsSignalExportPolicy
    policy_path: Path
    policy_sha256: str


@dataclass(frozen=True)
class QQQOptionsSignalPackage:
    policy_sha256: str
    daily_signals: tuple[DailySignalRecord, ...]
    signal_index: QQQOptionsSignalIndex
    run_manifest: RunManifestRecord
    receipt: QQQOptionsSignalPackageReceipt

    @property
    def files(self) -> dict[str, bytes]:
        files = {
            f"daily_signals/{record.signal_session.isoformat()}.json": record.canonical_bytes
            for record in self.daily_signals
        }
        files["signal_index.json"] = self.signal_index.canonical_bytes
        files["run_manifest.json"] = self.run_manifest.canonical_bytes
        files["package_receipt.json"] = self.receipt.canonical_bytes
        return dict(sorted(files.items()))


def load_qqq_options_signal_export_policy(
    path: Path = DEFAULT_QQQ_OPTIONS_SIGNAL_EXPORT_POLICY_PATH,
    *,
    project_root: Path = PROJECT_ROOT,
) -> QQQOptionsSignalExportPolicyLoadResult:
    resolved = path if path.is_absolute() else project_root / path
    try:
        content = resolved.read_bytes()
        payload = safe_load_yaml_path(resolved)
        if not isinstance(payload, dict):
            raise TypeError("policy root must be a mapping")
        policy = QQQOptionsSignalExportPolicy.model_validate(payload, strict=False)
        shared = load_qqq_options_shared_contract_policy(project_root=project_root)
        dq = load_qqq_options_dq_pit_identity_policy(project_root=project_root)
        calendar = load_us_equity_special_closure_policy_by_identity(
            policy_version=policy.calendar_policy_version,
            policy_sha256=policy.calendar_policy_sha256,
            project_root=project_root,
        )
        if policy.shared_policy_sha256 != shared.policy_sha256:
            raise ValueError("2483 policy must bind the exact 2481 policy bytes")
        if policy.dq_pit_policy_sha256 != dq.policy_sha256:
            raise ValueError("2483 policy must bind the exact 2482 policy bytes")
        if (
            policy.calendar_id != US_EQUITY_DECISION_CALENDAR_ID
            or policy.calendar_policy_id != calendar.policy_id
            or policy.calendar_policy_version != calendar.policy_version
            or policy.calendar_policy_sha256 != calendar.sha256
        ):
            raise ValueError("2483 policy must bind the reviewed exchange calendar identity")
    except (OSError, TypeError, ValueError) as exc:
        raise QQQOptionsContractError(
            "QQQ_OPTIONS_SIGNAL_EXPORT_POLICY_INVALID", f"{resolved}: {exc}"
        ) from exc
    return QQQOptionsSignalExportPolicyLoadResult(
        policy=policy,
        policy_path=resolved,
        policy_sha256=_content_sha256(content),
    )


def next_us_equity_trading_session(value: date) -> date:
    candidate = value + timedelta(days=1)
    while not is_us_equity_trading_day(candidate):
        candidate += timedelta(days=1)
    return candidate


def _evaluated_sessions(start: date, end: date) -> tuple[date, ...]:
    if start > end:
        raise QQQOptionsContractError(
            "QQQ_OPTIONS_SIGNAL_EVALUATED_RANGE_REVERSED",
            "evaluated range start must not follow end",
        )
    if not is_us_equity_trading_day(start) or not is_us_equity_trading_day(end):
        raise QQQOptionsContractError(
            "QQQ_OPTIONS_SIGNAL_EVALUATED_BOUNDARY_NOT_SESSION",
            "evaluated range boundaries must be valid exchange sessions",
        )
    sessions: list[date] = []
    candidate = start
    while candidate <= end:
        if is_us_equity_trading_day(candidate):
            sessions.append(candidate)
        candidate += timedelta(days=1)
    return tuple(sessions)


def resolve_normalized_signal_effective_session(
    item: NormalizedDailySignalInput,
) -> date:
    if not is_us_equity_trading_day(item.signal_session):
        raise QQQOptionsContractError(
            "QQQ_OPTIONS_SIGNAL_NON_SESSION",
            f"signal session is not a reviewed exchange session: {item.signal_session}",
        )
    session = us_equity_market_session(item.signal_session)
    if session.close_time is None:
        raise QQQOptionsContractError(
            "QQQ_OPTIONS_SIGNAL_SESSION_CLOSE_UNKNOWN",
            f"reviewed close time is unavailable for {item.signal_session}",
        )
    cutoff_local = item.source_data_cutoff_utc.astimezone(US_EQUITY_MARKET_TIMEZONE)
    generated_local = item.generated_at_utc.astimezone(US_EQUITY_MARKET_TIMEZONE)
    close_local = datetime.combine(
        item.signal_session,
        session.close_time,
        tzinfo=US_EQUITY_MARKET_TIMEZONE,
    )
    if cutoff_local.date() != item.signal_session or cutoff_local < close_local:
        raise QQQOptionsContractError(
            "QQQ_OPTIONS_SIGNAL_SOURCE_CUTOFF_INVALID",
            "source cutoff must occur at or after the reviewed close in signal_session",
        )
    if generated_local.date() != item.signal_session:
        raise QQQOptionsContractError(
            "QQQ_OPTIONS_SIGNAL_GENERATION_SESSION_INVALID",
            "signal generation must remain in the source exchange session",
        )
    return next_us_equity_trading_session(item.signal_session)


def _artifact(relative_path: str, content: bytes) -> SignalArtifactDigest:
    return SignalArtifactDigest(
        relative_path=relative_path,
        sha256=_content_sha256(content),
        byte_count=len(content),
    )


def _resolve_research_window_authority(
    *,
    policy: QQQOptionsSignalExportPolicy,
    research_window_role: ResearchWindowRole,
    research_window_authority_id: str | None,
    requested_start: date,
    evaluated_start: date,
) -> ReviewedResearchWindowAuthority | None:
    if research_window_role == policy.primary_window_role:
        if research_window_authority_id is not None:
            raise QQQOptionsContractError(
                "QQQ_OPTIONS_SIGNAL_PRIMARY_WINDOW_AUTHORITY_INVALID",
                "primary window cannot carry a non-primary authority id",
            )
        if (
            requested_start != policy.primary_research_start
            or evaluated_start != policy.primary_research_start
        ):
            raise QQQOptionsContractError(
                "QQQ_OPTIONS_SIGNAL_PRIMARY_WINDOW_START_MISMATCH",
                "primary requested/evaluated start must be 2021-02-22",
            )
        return None
    if research_window_role not in policy.non_primary_window_roles:
        raise QQQOptionsContractError(
            "QQQ_OPTIONS_SIGNAL_RESEARCH_WINDOW_ROLE_INVALID",
            f"unsupported research window role: {research_window_role}",
        )
    if research_window_authority_id is None:
        raise QQQOptionsContractError(
            "QQQ_OPTIONS_SIGNAL_NON_PRIMARY_AUTHORITY_REQUIRED",
            "non-primary window requires a reviewed authority id",
        )
    authority = next(
        (
            candidate
            for candidate in policy.approved_non_primary_window_authorities
            if candidate.authority_id == research_window_authority_id
        ),
        None,
    )
    if authority is None:
        raise QQQOptionsContractError(
            "QQQ_OPTIONS_SIGNAL_NON_PRIMARY_AUTHORITY_UNKNOWN",
            "non-primary window authority is not registered in the exact policy",
        )
    if (
        authority.role != research_window_role
        or authority.requested_start != requested_start
        or authority.evaluated_start != evaluated_start
    ):
        raise QQQOptionsContractError(
            "QQQ_OPTIONS_SIGNAL_NON_PRIMARY_AUTHORITY_MISMATCH",
            "non-primary authority role or exact start boundary differs",
        )
    return authority


def build_qqq_options_signal_package(
    *,
    run_id: str,
    signals: tuple[NormalizedDailySignalInput, ...],
    source_artifact: SignalSourceArtifact,
    source_artifact_bytes: bytes,
    data_quality_receipt_path: Path,
    expected_data_quality_as_of: date,
    expected_data_quality_policy_path: Path,
    expected_data_quality_input_roles: tuple[str, ...],
    requested_start: date,
    requested_end: date,
    evaluated_start: date,
    evaluated_end: date,
    research_window_role: ResearchWindowRole = "PRIMARY",
    research_window_authority_id: str | None = None,
    initial_cash_usd: Decimal,
    created_at_utc: datetime,
    producer_version: str,
    repository_code_sha: str,
    lineage_id: str,
    policy_path: Path = DEFAULT_QQQ_OPTIONS_SIGNAL_EXPORT_POLICY_PATH,
    project_root: Path = PROJECT_ROOT,
) -> QQQOptionsSignalPackage:
    run_id = _path_identifier(run_id, "run_id")
    created_at_utc = _utc(created_at_utc, "created_at_utc")
    _git_object_sha(repository_code_sha, "repository_code_sha")
    _identifier(lineage_id, "lineage_id")
    _required_text(producer_version, "producer_version")
    if source_artifact_bytes != canonical_normalized_signal_source_bytes(signals):
        raise QQQOptionsContractError(
            "QQQ_OPTIONS_SIGNAL_SOURCE_SEMANTIC_MISMATCH",
            "normalized source bytes must be the canonical typed signal input",
        )
    if _content_sha256(source_artifact_bytes) != source_artifact.sha256:
        raise QQQOptionsContractError(
            "QQQ_OPTIONS_SIGNAL_SOURCE_CHECKSUM_MISMATCH",
            "normalized signal source bytes do not match the declared SHA-256",
        )
    if len(source_artifact_bytes) != source_artifact.byte_count:
        raise QQQOptionsContractError(
            "QQQ_OPTIONS_SIGNAL_SOURCE_SIZE_MISMATCH",
            "normalized signal source bytes do not match the declared byte count",
        )
    loaded = load_qqq_options_signal_export_policy(policy_path, project_root=project_root)
    policy = loaded.policy
    research_window_authority = _resolve_research_window_authority(
        policy=policy,
        research_window_role=research_window_role,
        research_window_authority_id=research_window_authority_id,
        requested_start=requested_start,
        evaluated_start=evaluated_start,
    )
    if not isinstance(expected_data_quality_as_of, date) or isinstance(
        expected_data_quality_as_of, datetime
    ):
        raise QQQOptionsContractError(
            "QQQ_OPTIONS_SIGNAL_LOCAL_DQ_AS_OF_INVALID",
            "expected data-quality as-of must be a date",
        )
    try:
        data_quality_preflight = verify_data_quality_execution_receipt(
            data_quality_receipt_path,
            expected_as_of=expected_data_quality_as_of,
            expected_policy_path=expected_data_quality_policy_path,
            expected_input_roles=expected_data_quality_input_roles,
            project_root=project_root,
        )
    except DataQualityExecutionError as exc:
        raise QQQOptionsContractError(
            "QQQ_OPTIONS_SIGNAL_LOCAL_DQ_INVALID",
            f"{exc.code}: {exc.message}",
        ) from exc
    dq_receipt = data_quality_preflight.receipt
    if dq_receipt.checked_at > created_at_utc:
        raise QQQOptionsContractError(
            "QQQ_OPTIONS_SIGNAL_LOCAL_DQ_FUTURE_EVIDENCE",
            "canonical local DQ execution cannot finish after package creation",
        )
    if (
        dq_receipt.requested_window.start != requested_start
        or dq_receipt.requested_window.end != requested_end
        or dq_receipt.evaluated_window.start != evaluated_start
        or dq_receipt.evaluated_window.end != evaluated_end
    ):
        raise QQQOptionsContractError(
            "QQQ_OPTIONS_SIGNAL_LOCAL_DQ_WINDOW_MISMATCH",
            "canonical local DQ requested/evaluated windows differ from the package",
        )
    local_cached_data_gate = LocalCachedDataGateDeclaration(
        status="PASS",
        scope="CACHED_MARKET_MACRO",
        as_of_utc=dq_receipt.checked_at,
        report_locator=dq_receipt.report.path,
        report_sha256=dq_receipt.report.sha256,
    )

    calendar = load_us_equity_special_closure_policy_by_identity(
        policy_version=policy.calendar_policy_version,
        policy_sha256=policy.calendar_policy_sha256,
        project_root=project_root,
    )
    expected_sessions = _evaluated_sessions(evaluated_start, evaluated_end)
    by_session: dict[date, NormalizedDailySignalInput] = {}
    for item in signals:
        if item.signal_session in by_session:
            raise QQQOptionsContractError(
                "QQQ_OPTIONS_SIGNAL_DUPLICATE_SESSION",
                f"duplicate signal session: {item.signal_session}",
            )
        by_session[item.signal_session] = item
    actual_sessions = tuple(sorted(by_session))
    if actual_sessions != expected_sessions:
        missing = sorted(set(expected_sessions) - set(actual_sessions))
        extra = sorted(set(actual_sessions) - set(expected_sessions))
        raise QQQOptionsContractError(
            "QQQ_OPTIONS_SIGNAL_COVERAGE_MISMATCH",
            f"missing={missing}; extra={extra}",
        )

    source_pairs = {
        "qqq.options.dq_policy": policy.dq_pit_policy_sha256,
        "qqq.options.exchange_calendar": calendar.sha256,
        "qqq.options.local_dq_report": local_cached_data_gate.report_sha256,
        "qqq.options.local_dq_execution_receipt": data_quality_preflight.receipt_sha256,
        "qqq.options.shared_policy": policy.shared_policy_sha256,
        "qqq.options.signal_export_policy": loaded.policy_sha256,
        f"qqq.signal.source:{source_artifact.artifact_id}": source_artifact.sha256,
    }
    sorted_source_pairs = tuple(sorted(source_pairs.items()))
    daily_signals: list[DailySignalRecord] = []
    for session_date in expected_sessions:
        item = by_session[session_date]
        effective_session = resolve_normalized_signal_effective_session(item)
        daily_signals.append(
            DailySignalRecord.seal(
                schema_name="daily_signal",
                schema_version="1.0.0",
                run_id=run_id,
                record_id=f"signal-{session_date.isoformat()}",
                created_at_utc=created_at_utc,
                producer_version=producer_version,
                repository_code_sha=repository_code_sha,
                policy_id=policy.policy_id,
                policy_version=policy.policy_version,
                policy_sha256=loaded.policy_sha256,
                contract_schema_sha256=policy.shared_contract_sha256,
                source_ids=tuple(key for key, _ in sorted_source_pairs),
                source_checksums=tuple(value for _, value in sorted_source_pairs),
                requested_start=requested_start,
                requested_end=requested_end,
                evaluated_start=evaluated_start,
                evaluated_end=evaluated_end,
                storage_timezone="UTC",
                exchange_timezone="America/New_York",
                dq_status="PASS",
                pit_status="PASS",
                export_classification=policy.artifact_export_classification,
                lineage_id=lineage_id,
                safety=policy.safety,
                signal_session=session_date,
                signal_as_of_utc=item.source_data_cutoff_utc,
                generated_at_utc=item.generated_at_utc,
                earliest_effective_session=effective_session,
                signal=item.signal,
                signal_source_sha256=source_artifact.sha256,
            )
        )

    daily_artifacts = tuple(
        _artifact(
            f"daily_signals/{record.signal_session.isoformat()}.json",
            record.canonical_bytes,
        )
        for record in daily_signals
    )
    signal_index = QQQOptionsSignalIndex.seal(
        run_id=run_id,
        artifacts=daily_artifacts,
    )
    signal_index_artifact = _artifact("signal_index.json", signal_index.canonical_bytes)
    manifest_source_pairs = tuple(
        sorted((*sorted_source_pairs, ("qqq.signal.index", signal_index_artifact.sha256)))
    )
    run_manifest = RunManifestRecord.seal(
        schema_name="run_manifest",
        schema_version="1.0.0",
        run_id=run_id,
        record_id=f"manifest-{run_id}",
        created_at_utc=created_at_utc,
        producer_version=producer_version,
        repository_code_sha=repository_code_sha,
        policy_id=policy.policy_id,
        policy_version=policy.policy_version,
        policy_sha256=loaded.policy_sha256,
        contract_schema_sha256=policy.shared_contract_sha256,
        source_ids=tuple(key for key, _ in manifest_source_pairs),
        source_checksums=tuple(value for _, value in manifest_source_pairs),
        requested_start=requested_start,
        requested_end=requested_end,
        evaluated_start=evaluated_start,
        evaluated_end=evaluated_end,
        storage_timezone="UTC",
        exchange_timezone="America/New_York",
        dq_status="PASS",
        pit_status="PASS",
        export_classification=policy.artifact_export_classification,
        lineage_id=lineage_id,
        safety=policy.safety,
        underlying="QQQ",
        initial_cash_usd=initial_cash_usd,
        account_currency="USD",
        account_type="CASH",
        signal_resolution="DAILY",
        execution_resolution="MINUTE",
        signal_artifact_sha256=signal_index_artifact.sha256,
        engine_identity_status="UNKNOWN",
        engine_identity=None,
        evidence_admission_decision="CAPABILITY_OR_LICENSE_BLOCKED",
    )
    manifest_artifact = _artifact("run_manifest.json", run_manifest.canonical_bytes)
    receipt = QQQOptionsSignalPackageReceipt.seal(
        schema_version="qqq_options_signal_package_receipt.v1",
        run_id=run_id,
        created_at_utc=created_at_utc,
        producer_version=producer_version,
        repository_code_sha=repository_code_sha,
        policy_id=policy.policy_id,
        policy_version=policy.policy_version,
        policy_sha256=loaded.policy_sha256,
        shared_contract_sha256=policy.shared_contract_sha256,
        shared_policy_sha256=policy.shared_policy_sha256,
        dq_pit_policy_sha256=policy.dq_pit_policy_sha256,
        calendar_id=policy.calendar_id,
        calendar_policy_id=policy.calendar_policy_id,
        calendar_policy_version=policy.calendar_policy_version,
        calendar_policy_sha256=policy.calendar_policy_sha256,
        research_window_role=research_window_role,
        research_window_authority=research_window_authority,
        source_artifact=source_artifact,
        local_dq_execution_receipt=SignalSourceArtifact(
            artifact_id="canonical-data-quality-execution-receipt",
            locator=data_quality_preflight.receipt_path,
            sha256=data_quality_preflight.receipt_sha256,
            byte_count=data_quality_preflight.receipt_size_bytes,
            export_classification="EXPORT_ALLOWED_DERIVED",
        ),
        local_cached_data_gate=local_cached_data_gate,
        run_manifest_artifact=manifest_artifact,
        signal_index_artifact=signal_index_artifact,
        daily_signal_artifacts=daily_artifacts,
        option_event_dq_status=policy.option_event_dq_status,
        option_event_pit_status=policy.option_event_pit_status,
        export_classification=policy.artifact_export_classification,
        safety=policy.safety,
    )
    return QQQOptionsSignalPackage(
        policy_sha256=loaded.policy_sha256,
        daily_signals=tuple(daily_signals),
        signal_index=signal_index,
        run_manifest=run_manifest,
        receipt=receipt,
    )


def _validate_existing_package(target: Path, expected_files: dict[str, bytes]) -> None:
    if target.is_symlink() or not target.is_dir():
        raise QQQOptionsContractError(
            "QQQ_OPTIONS_SIGNAL_PACKAGE_TARGET_INVALID",
            "existing package target must be a real directory",
        )
    paths = tuple(target.rglob("*"))
    if any(path.is_symlink() for path in paths):
        raise QQQOptionsContractError(
            "QQQ_OPTIONS_SIGNAL_PACKAGE_SYMLINK_PROHIBITED",
            "package directories and files cannot be symlinks",
        )
    directories = {path.relative_to(target).as_posix() for path in paths if path.is_dir()}
    actual_files = {path.relative_to(target).as_posix() for path in paths if path.is_file()}
    if directories != {"daily_signals"} or actual_files != set(expected_files):
        raise QQQOptionsContractError(
            "QQQ_OPTIONS_SIGNAL_PACKAGE_INVENTORY_MISMATCH",
            "existing package files differ from the canonical inventory",
        )
    for relative_path, expected in expected_files.items():
        if (target / PurePosixPath(relative_path)).read_bytes() != expected:
            raise QQQOptionsContractError(
                "QQQ_OPTIONS_SIGNAL_PACKAGE_BYTES_MISMATCH",
                f"existing package artifact differs: {relative_path}",
            )


def write_qqq_options_signal_package(
    package: QQQOptionsSignalPackage,
    *,
    output_root: Path,
) -> Path:
    output_root.mkdir(parents=True, exist_ok=True)
    if output_root.is_symlink() or not output_root.is_dir():
        raise QQQOptionsContractError(
            "QQQ_OPTIONS_SIGNAL_OUTPUT_ROOT_INVALID",
            "output root must be a real directory",
        )
    target = output_root / _path_identifier(package.run_manifest.run_id, "run_id")
    expected_files = package.files
    if target.exists() or target.is_symlink():
        _validate_existing_package(target, expected_files)
        return target

    temporary = Path(
        tempfile.mkdtemp(prefix=f".{package.run_manifest.run_id}.tmp-", dir=output_root)
    )
    try:
        for relative_path, content in expected_files.items():
            destination = temporary / PurePosixPath(relative_path)
            destination.parent.mkdir(parents=True, exist_ok=True)
            write_bytes_atomic(destination, content)
        try:
            os.replace(temporary, target)
        except FileExistsError:
            _validate_existing_package(target, expected_files)
        return target
    finally:
        if temporary.exists():
            shutil.rmtree(temporary)


__all__ = [
    "DEFAULT_QQQ_OPTIONS_SIGNAL_EXPORT_POLICY_PATH",
    "NormalizedDailySignalInput",
    "QQQOptionsSignalExportPolicy",
    "QQQOptionsSignalExportPolicyLoadResult",
    "QQQOptionsSignalIndex",
    "QQQOptionsSignalPackage",
    "QQQOptionsSignalPackageReceipt",
    "ResearchWindowRole",
    "ReviewedResearchWindowAuthority",
    "SignalArtifactDigest",
    "SignalSourceArtifact",
    "build_qqq_options_signal_package",
    "canonical_normalized_signal_source_bytes",
    "load_qqq_options_signal_export_policy",
    "next_us_equity_trading_session",
    "resolve_normalized_signal_effective_session",
    "write_qqq_options_signal_package",
]
