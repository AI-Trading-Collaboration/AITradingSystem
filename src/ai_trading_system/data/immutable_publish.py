from __future__ import annotations

import errno
import hashlib
import importlib
import json
import math
import os
import re
import stat
import sys
import time
from collections.abc import Iterator, Mapping
from contextlib import ExitStack, contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, replace
from dataclasses import field as dataclass_field
from datetime import date, datetime
from pathlib import Path, PurePosixPath
from typing import Any, NoReturn, cast
from uuid import uuid4

from ai_trading_system.contracts import (
    ArtifactEnvelope,
    ArtifactLifecycle,
    ArtifactPointer,
    ArtifactVisibility,
    CanonicalStatus,
    DataQualityEvidence,
)
from ai_trading_system.core import ProductionEffect
from ai_trading_system.platform.artifacts import (
    ArtifactWriteError,
    ArtifactWriteResult,
    canonical_json_bytes,
    sha256_bytes,
)

SOURCE_EVENT_SCHEMA_VERSION = "data_source_event_manifest.v1"
SNAPSHOT_MANIFEST_SCHEMA_VERSION = "data_snapshot_manifest.v1"
CURRENT_POINTER_SCHEMA_VERSION = "data_current_pointer.v1"
DATA_QUALITY_REPORT_SCHEMA_VERSION = "data_snapshot_quality_report.v1"
DQ_EXECUTION_PROVENANCE_LIMITATION = "dq_execution_provenance_verified=false"
CONSUMER_CUTOVER_LIMITATION = "consumer_cutover_allowed=false"
SAME_PRINCIPAL_POST_ACK_LIMITATION = "same_principal_post_ack_mutation_protection=false"
FILESYSTEM_SECURITY_PROFILE = "acl_isolated_writer.v1"
SAME_PRINCIPAL_ADVERSARIAL_MUTATION_RESISTANCE = "NOT_GUARANTEED"
FILESYSTEM_SECURITY_PROFILE_LIMITATION = (
    f"filesystem_security_profile={FILESYSTEM_SECURITY_PROFILE}"
)
TRUSTED_WRITER_PRINCIPAL_LIMITATION = "trusted_writer_principal_required=true"
SAME_PRINCIPAL_ADVERSARIAL_MUTATION_LIMITATION = (
    "same_principal_adversarial_mutation_resistance="
    f"{SAME_PRINCIPAL_ADVERSARIAL_MUTATION_RESISTANCE}"
)
STORE_ACL_VERIFIED_LIMITATION = "store_acl_verified=false"
CRASH_DURABILITY_VERIFIED_LIMITATION = "crash_durability_verified=false"
_GOVERNED_LIMITATION_VALUES = (
    ("dq_execution_provenance_verified", "false"),
    ("filesystem_security_profile", FILESYSTEM_SECURITY_PROFILE),
    ("trusted_writer_principal_required", "true"),
    (
        "same_principal_adversarial_mutation_resistance",
        SAME_PRINCIPAL_ADVERSARIAL_MUTATION_RESISTANCE,
    ),
    ("store_acl_verified", "false"),
    ("consumer_cutover_allowed", "false"),
    ("crash_durability_verified", "false"),
    ("same_principal_post_ack_mutation_protection", "false"),
)

# Filesystem coordination bounds; these do not affect investment interpretation.
DEFAULT_LOCK_TIMEOUT_SECONDS = 10.0
DEFAULT_LOCK_POLL_SECONDS = 0.02

_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
_TYPE_RE = re.compile(r"^[a-z0-9][a-z0-9._+-]{0,31}$")
_SHA_RE = re.compile(r"^[0-9a-f]{64}$")
_LOCK_ERRNOS = {
    item
    for item in (
        getattr(errno, "EACCES", None),
        getattr(errno, "EAGAIN", None),
        getattr(errno, "EBUSY", None),
        getattr(errno, "EDEADLK", None),
    )
    if item is not None
}
_SOURCE_FIELDS = {
    "source_event_id",
    "schema_version",
    "dataset_id",
    "run_id",
    "source_id",
    "provider_name",
    "endpoint",
    "request_parameters",
    "request_parameters_sha256",
    "downloaded_at",
    "row_count",
    "source_role",
    "response_headers_sanitized",
    "snapshot",
    "production_effect",
}
_MANIFEST_FIELDS = {
    "manifest_id",
    "schema_version",
    "dataset_id",
    "snapshot_id",
    "source_event_id",
    "run_id",
    "quality_binding",
    "artifact_envelope",
    "dq_execution_provenance_verified",
    "filesystem_security_profile",
    "trusted_writer_principal_required",
    "same_principal_adversarial_mutation_resistance",
    "store_acl_verified",
    "consumer_cutover_allowed",
    "crash_durability_verified",
    "same_principal_post_ack_mutation_protection",
    "production_effect",
}
_QUALITY_FIELDS = {
    "data_quality_evidence_id",
    "report",
    "evaluated_snapshot",
    "coverage_start",
    "coverage_end",
}
_DQ_REPORT_FIELDS = {
    "schema_version",
    "contract_id",
    "policy_id",
    "policy_version",
    "status",
    "passed",
    "checked_at",
    "as_of",
    "coverage_start",
    "coverage_end",
    "checked_input_count",
    "error_count",
    "warning_count",
    "blocking_issues",
    "evaluated_snapshot",
    "production_effect",
}
_POINTER_FIELDS = {
    "pointer_id",
    "schema_version",
    "dataset_id",
    "snapshot_id",
    "manifest_id",
    "source_event_id",
    "run_id",
    "generation",
    "published_at",
    "manifest",
    "snapshot",
    "source_event",
    "data_quality_evidence_id",
    "previous_pointer_id",
    "previous_pointer_sha256",
    "production_effect",
}


class DataPublicationError(RuntimeError):
    def __init__(
        self,
        code: str,
        message: str,
        *,
        path: Path | None = None,
        commit_state: str = "NOT_REPLACED",
    ) -> None:
        if commit_state not in {"NOT_REPLACED", "ROLLED_BACK", "INDETERMINATE"}:
            raise ValueError("invalid commit_state")
        self.code = code
        self.message = message
        self.path = path
        self.commit_state = commit_state
        super().__init__(f"{code}{'' if path is None else f' [{path}]'}: {message}")


class DataPublicationIntegrityError(DataPublicationError):
    pass


class DataPublicationConflictError(DataPublicationError):
    pass


class DataPublicationLockTimeout(DataPublicationError):
    pass


@dataclass(frozen=True)
class SourceEventProvenance:
    source_id: str
    provider_name: str
    endpoint: str
    request_parameters: Mapping[str, object]
    downloaded_at: datetime
    row_count: int
    source_role: str
    response_headers_sanitized: bool = False


@dataclass(frozen=True)
class SnapshotPublishRequest:
    dataset_id: str
    run_id: str
    producer: str
    owner: str
    as_of: date
    generated_at: datetime
    coverage_start: date
    coverage_end: date
    payload_artifact_type: str
    payload_schema_version: str
    data_quality_report_schema_version: str
    source_event: SourceEventProvenance
    data_quality: DataQualityEvidence
    retention_until: date | None = None


@dataclass(frozen=True)
class CurrentPointerPrecondition:
    """When supplied, None requires no current pointer; a digest requires that version."""

    expected_sha256: str | None

    def __post_init__(self) -> None:
        if self.expected_sha256 is not None and not _SHA_RE.fullmatch(self.expected_sha256):
            raise ValueError("expected_sha256 must be a lowercase SHA-256 digest or None")


@dataclass(frozen=True)
class ValidatedCurrentSnapshot:
    dataset_id: str
    snapshot_id: str
    manifest_id: str
    source_event_id: str
    run_id: str
    generation: int
    pointer_id: str
    pointer_sha256: str
    pointer_path: Path
    payload_path: Path
    source_event_path: Path
    manifest_path: Path
    envelope: ArtifactEnvelope


@dataclass(frozen=True)
class SnapshotPublishResult:
    snapshot: ValidatedCurrentSnapshot
    current_pointer_changed: bool
    dq_execution_provenance_verified: bool = False
    filesystem_security_profile: str = FILESYSTEM_SECURITY_PROFILE
    trusted_writer_principal_required: bool = True
    same_principal_adversarial_mutation_resistance: str = (
        SAME_PRINCIPAL_ADVERSARIAL_MUTATION_RESISTANCE
    )
    store_acl_verified: bool = False
    consumer_cutover_allowed: bool = False
    crash_durability_verified: bool = False
    same_principal_post_ack_mutation_protection: bool = False
    post_commit_cleanup_status: str = "PASS"
    post_commit_cleanup_warnings: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if (
            self.dq_execution_provenance_verified
            or self.filesystem_security_profile != FILESYSTEM_SECURITY_PROFILE
            or self.trusted_writer_principal_required is not True
            or self.same_principal_adversarial_mutation_resistance
            != SAME_PRINCIPAL_ADVERSARIAL_MUTATION_RESISTANCE
            or self.store_acl_verified
            or self.consumer_cutover_allowed
            or self.crash_durability_verified
            or self.same_principal_post_ack_mutation_protection
        ):
            raise ValueError(
                "D0A cannot verify DQ execution provenance, filesystem ACLs, crash "
                "durability, same-principal resistance, or allow consumer cutover"
            )
        if self.post_commit_cleanup_status not in {"PASS", "PASS_WITH_WARNINGS"}:
            raise ValueError("invalid post_commit_cleanup_status")
        if bool(self.post_commit_cleanup_warnings) != (
            self.post_commit_cleanup_status == "PASS_WITH_WARNINGS"
        ):
            raise ValueError("cleanup status/warnings mismatch")


@dataclass
class _LockLease:
    committed: bool = False
    cleanup_warnings: list[str] = dataclass_field(default_factory=list)

    def mark_committed(self) -> None:
        self.committed = True


@dataclass(frozen=True)
class _CurrentCommitState:
    previous_bytes: bytes | None


@dataclass(frozen=True)
class _FileIdentity:
    device: int
    inode: int


@dataclass(frozen=True)
class _BoundDirectory:
    root: Path
    path: Path
    identity: _FileIdentity
    descriptor: int | None = None


_ACTIVE_ROOT_BINDING: ContextVar[_BoundDirectory | None] = ContextVar(
    "immutable_publish_active_root_binding",
    default=None,
)


@dataclass(frozen=True)
class _QualityReportPublication:
    pointer: ArtifactPointer
    raw: bytes
    payload: dict[str, object]
    evidence: DataQualityEvidence


@dataclass(frozen=True)
class _Publication:
    snapshot: ArtifactPointer
    source: ArtifactPointer
    manifest: ArtifactPointer
    quality_report: ArtifactPointer
    source_payload: dict[str, object]
    manifest_payload: dict[str, object]
    pointer_payload: dict[str, object]
    quality_report_bytes: bytes


def publish_immutable_snapshot(
    *,
    store_root: Path,
    evidence_root: Path,
    request: SnapshotPublishRequest,
    payload: bytes,
    current_precondition: CurrentPointerPrecondition | None = None,
    lock_timeout_seconds: float = DEFAULT_LOCK_TIMEOUT_SECONDS,
    lock_poll_seconds: float = DEFAULT_LOCK_POLL_SECONDS,
) -> SnapshotPublishResult:
    """Publish immutable evidence first; atomically replace current as the final step."""

    _validate_request(request)
    if not isinstance(payload, bytes):
        raise TypeError("payload must be bytes")
    for value, name in (
        (lock_timeout_seconds, "lock_timeout_seconds"),
        (lock_poll_seconds, "lock_poll_seconds"),
    ):
        if (
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            or not math.isfinite(float(value))
            or value <= 0
        ):
            raise ValueError(f"{name} must be positive and finite")

    root = _directory(store_root, create=True, code="STORE_ROOT_UNAVAILABLE")
    evidence = _directory(evidence_root, create=False, code="EVIDENCE_ROOT_UNAVAILABLE")
    lock_path = _internal_path(
        root,
        f"locks/{request.dataset_id}.lock",
        "dataset lock",
        create_parents=True,
    )
    with (
        _root_authority(root),
        _file_lock(
            lock_path,
            root=root,
            timeout_seconds=float(lock_timeout_seconds),
            poll_seconds=float(lock_poll_seconds),
        ) as lease,
    ):
        current_path = _internal_path(
            root,
            f"current/{request.dataset_id}.json",
            "current pointer",
            create_parents=True,
        )
        old_bytes = (
            _read_json(current_path, "CURRENT_POINTER_INVALID", root)[0]
            if os.path.lexists(current_path)
            else None
        )
        old = (
            validate_current_snapshot(
                store_root=root,
                evidence_root=evidence,
                dataset_id=request.dataset_id,
            )
            if old_bytes is not None
            else None
        )
        actual_sha = None if old_bytes is None else sha256_bytes(old_bytes)
        if current_precondition is not None and current_precondition.expected_sha256 != actual_sha:
            raise DataPublicationConflictError(
                "CURRENT_POINTER_CAS_MISMATCH",
                f"expected={current_precondition.expected_sha256!r} actual={actual_sha!r}",
            )

        _validate_publish_order(request, old)
        built = _build(request, payload, evidence, old)
        if old is not None and _text(built.pointer_payload, "manifest_id") == old.manifest_id:
            return SnapshotPublishResult(old, current_pointer_changed=False)
        if (
            old is not None
            and request.as_of == old.envelope.as_of
            and request.generated_at == old.envelope.generated_at
        ):
            raise DataPublicationConflictError(
                "CURRENT_VERSION_COLLISION",
                "same as_of/generated_at identifies a different immutable publication",
                path=current_path,
            )

        stage_relative = (
            PurePosixPath("staging")
            / request.dataset_id
            / f"{sha256_bytes(request.run_id.encode())[:20]}_{uuid4().hex}"
        ).as_posix()
        stage = _internal_path(
            root,
            stage_relative,
            "publication stage",
            create_parents=True,
        )
        stage_identity: _FileIdentity | None = None
        try:
            stage_identity = _mkdir_checked(root, stage, "publication stage")
            stage_payload = _internal_path(root, f"{stage_relative}/payload.bin", "staged payload")
            stage_source = _internal_path(
                root, f"{stage_relative}/source_event.json", "staged source event"
            )
            stage_manifest = _internal_path(
                root, f"{stage_relative}/manifest.json", "staged manifest"
            )
            stage_quality = _internal_path(
                root, f"{stage_relative}/quality_report.json", "staged DQ report"
            )
            stage_pointer_path = _internal_path(
                root, f"{stage_relative}/pointer.json", "staged pointer"
            )
            writes = (
                (
                    _write_checked_atomic(root, stage_payload, payload, "staged payload"),
                    built.snapshot,
                ),
                (
                    _write_checked_atomic(
                        root,
                        stage_source,
                        canonical_json_bytes(built.source_payload),
                        "staged source event",
                    ),
                    built.source,
                ),
                (
                    _write_checked_atomic(
                        root,
                        stage_manifest,
                        canonical_json_bytes(built.manifest_payload),
                        "staged manifest",
                    ),
                    built.manifest,
                ),
                (
                    _write_checked_atomic(
                        root,
                        stage_quality,
                        built.quality_report_bytes,
                        "staged DQ report",
                    ),
                    built.quality_report,
                ),
            )
            for result, pointer in writes:
                if (result.sha256, result.size_bytes) != (pointer.sha256, pointer.size_bytes):
                    _fail("STAGED_ARTIFACT_MISMATCH", pointer.path)

            pointer_bytes = canonical_json_bytes(built.pointer_payload)
            staged_pointer = _write_checked_atomic(
                root,
                stage_pointer_path,
                pointer_bytes,
                "staged pointer",
            )
            for staged, pointer in writes:
                _install(
                    staged.path,
                    _store_path(root, pointer, create_parents=True),
                    pointer,
                )
            envelope, paths = _validate_references(root, built.pointer_payload)

            pointer_id = _text(built.pointer_payload, "pointer_id")
            history = _history_path(
                root,
                request.dataset_id,
                pointer_id,
                create_parents=True,
            )
            _install_raw(
                staged_pointer.path,
                history,
                staged_pointer.sha256,
                staged_pointer.size_bytes,
            )
            _validate_history(root, built.pointer_payload)

            # Every candidate byte and the complete pointer chain is valid before this
            # single mutable replace. A successful replace is the commit point.
            validated = _validated_snapshot(
                root=root,
                pointer=built.pointer_payload,
                pointer_raw=pointer_bytes,
                pointer_path=current_path,
                envelope=envelope,
                paths=paths,
            )
            _, commit_warnings = _commit_current_atomic(
                root,
                current_path,
                pointer_bytes,
                previous_bytes=old_bytes,
            )
            lease.cleanup_warnings.extend(commit_warnings)
            lease.mark_committed()
        except ArtifactWriteError:
            raise
        finally:
            if stage_identity is not None:
                cleanup_warning = _cleanup_stage(root, stage, stage_identity)
                if cleanup_warning is not None and lease.committed:
                    lease.cleanup_warnings.append(cleanup_warning)
        published_snapshot = validated
    warnings = tuple(lease.cleanup_warnings)
    return SnapshotPublishResult(
        published_snapshot,
        current_pointer_changed=True,
        post_commit_cleanup_status="PASS_WITH_WARNINGS" if warnings else "PASS",
        post_commit_cleanup_warnings=warnings,
    )


def validate_current_snapshot(
    *,
    store_root: Path,
    evidence_root: Path,
    dataset_id: str,
) -> ValidatedCurrentSnapshot:
    """Return current only when pointer, history, manifests, DQ and bytes all verify."""

    _identifier(dataset_id, "dataset_id")
    root = _directory(store_root, create=False, code="STORE_ROOT_UNAVAILABLE")
    # The argument remains for API compatibility. Published validation is deliberately
    # self-contained and never reopens the caller's mutable evidence directory.
    del evidence_root
    with _root_authority(root):
        pointer_path = _internal_path(root, f"current/{dataset_id}.json", "current pointer")
        raw, pointer = _read_json(pointer_path, "CURRENT_POINTER_INVALID", root)
        _validate_pointer(pointer, dataset_id)
        _validate_history(root, pointer, current_raw=raw)
        envelope, paths = _validate_references(root, pointer)
        return _validated_snapshot(
            root=root,
            pointer=pointer,
            pointer_raw=raw,
            pointer_path=pointer_path,
            envelope=envelope,
            paths=paths,
        )


def _validate_publish_order(
    request: SnapshotPublishRequest,
    previous: ValidatedCurrentSnapshot | None,
) -> None:
    if previous is None:
        return
    if request.as_of < previous.envelope.as_of:
        raise DataPublicationConflictError(
            "CURRENT_AS_OF_REGRESSION",
            f"candidate={request.as_of} current={previous.envelope.as_of}",
        )
    if request.generated_at < previous.envelope.generated_at:
        raise DataPublicationConflictError(
            "CURRENT_GENERATED_AT_REGRESSION",
            (
                f"candidate={request.generated_at.isoformat()} "
                f"current={previous.envelope.generated_at.isoformat()}"
            ),
        )


def _validated_snapshot(
    *,
    root: Path,
    pointer: Mapping[str, object],
    pointer_raw: bytes,
    pointer_path: Path,
    envelope: ArtifactEnvelope,
    paths: tuple[Path, Path, Path],
) -> ValidatedCurrentSnapshot:
    try:
        for path in (pointer_path, *paths):
            path.relative_to(root)
    except ValueError as exc:
        raise DataPublicationIntegrityError(
            "ARTIFACT_PATH_ESCAPES_ROOT",
            "validated publication path escapes store",
        ) from exc
    return ValidatedCurrentSnapshot(
        dataset_id=_text(pointer, "dataset_id"),
        snapshot_id=_text(pointer, "snapshot_id"),
        manifest_id=_text(pointer, "manifest_id"),
        source_event_id=_text(pointer, "source_event_id"),
        run_id=_text(pointer, "run_id"),
        generation=_integer(pointer, "generation", 1),
        pointer_id=_text(pointer, "pointer_id"),
        pointer_sha256=sha256_bytes(pointer_raw),
        pointer_path=pointer_path,
        payload_path=paths[0],
        source_event_path=paths[1],
        manifest_path=paths[2],
        envelope=envelope,
    )


def _build(
    request: SnapshotPublishRequest,
    content: bytes,
    evidence_root: Path,
    previous: ValidatedCurrentSnapshot | None,
) -> _Publication:
    content_sha = sha256_bytes(content)
    snapshot_id = f"data_snapshot_{content_sha[:32]}"
    snapshot = ArtifactPointer(
        path=(
            PurePosixPath("snapshots")
            / request.dataset_id
            / content_sha
            / f"payload.{request.payload_artifact_type}"
        ).as_posix(),
        artifact_type=request.payload_artifact_type,
        sha256=content_sha,
        size_bytes=len(content),
        schema_version=request.payload_schema_version,
    )
    quality_publication = _quality_report_publication(request, evidence_root, snapshot)
    report = quality_publication.pointer
    published_quality = quality_publication.evidence
    parameters = _json_mapping(request.source_event.request_parameters)
    source_body: dict[str, object] = {
        "schema_version": SOURCE_EVENT_SCHEMA_VERSION,
        "dataset_id": request.dataset_id,
        "run_id": request.run_id,
        "source_id": request.source_event.source_id,
        "provider_name": request.source_event.provider_name,
        "endpoint": request.source_event.endpoint,
        "request_parameters": parameters,
        "request_parameters_sha256": _digest(parameters),
        "downloaded_at": request.source_event.downloaded_at.isoformat(),
        "row_count": request.source_event.row_count,
        "source_role": request.source_event.source_role,
        "response_headers_sanitized": True,
        "snapshot": snapshot.to_dict(),
        "production_effect": ProductionEffect.NONE.value,
    }
    source_id = f"source_event_{_digest(source_body)[:32]}"
    source_payload = {"source_event_id": source_id, **source_body}
    source_bytes = canonical_json_bytes(source_payload)
    source = ArtifactPointer(
        path=(PurePosixPath("source_events") / request.dataset_id / f"{source_id}.json").as_posix(),
        artifact_type="json",
        sha256=sha256_bytes(source_bytes),
        size_bytes=len(source_bytes),
        schema_version=SOURCE_EVENT_SCHEMA_VERSION,
    )
    envelope = ArtifactEnvelope(
        artifact_id=snapshot_id,
        producer=request.producer,
        run_id=request.run_id,
        generated_at=request.generated_at,
        as_of=request.as_of,
        status=CanonicalStatus.PASS,
        production_effect=ProductionEffect.NONE,
        payload=snapshot,
        owner=request.owner,
        lifecycle=ArtifactLifecycle.CURRENT,
        visibility=ArtifactVisibility.AUDIT,
        retention_until=request.retention_until,
        data_quality_required=True,
        data_quality=published_quality,
        input_artifacts=(source,),
        limitations=(
            "D0A capability only; no consumer cutover",
            DQ_EXECUTION_PROVENANCE_LIMITATION,
            FILESYSTEM_SECURITY_PROFILE_LIMITATION,
            TRUSTED_WRITER_PRINCIPAL_LIMITATION,
            SAME_PRINCIPAL_ADVERSARIAL_MUTATION_LIMITATION,
            STORE_ACL_VERIFIED_LIMITATION,
            CONSUMER_CUTOVER_LIMITATION,
            CRASH_DURABILITY_VERIFIED_LIMITATION,
            SAME_PRINCIPAL_POST_ACK_LIMITATION,
        ),
    )
    manifest_body: dict[str, object] = {
        "schema_version": SNAPSHOT_MANIFEST_SCHEMA_VERSION,
        "dataset_id": request.dataset_id,
        "snapshot_id": snapshot_id,
        "source_event_id": source_id,
        "run_id": request.run_id,
        "quality_binding": {
            "data_quality_evidence_id": published_quality.evidence_id,
            "report": report.to_dict(),
            "evaluated_snapshot": snapshot.to_dict(),
            "coverage_start": request.coverage_start.isoformat(),
            "coverage_end": request.coverage_end.isoformat(),
        },
        "artifact_envelope": envelope.to_dict(),
        "dq_execution_provenance_verified": False,
        "filesystem_security_profile": FILESYSTEM_SECURITY_PROFILE,
        "trusted_writer_principal_required": True,
        "same_principal_adversarial_mutation_resistance": (
            SAME_PRINCIPAL_ADVERSARIAL_MUTATION_RESISTANCE
        ),
        "store_acl_verified": False,
        "consumer_cutover_allowed": False,
        "crash_durability_verified": False,
        "same_principal_post_ack_mutation_protection": False,
        "production_effect": ProductionEffect.NONE.value,
    }
    manifest_id = f"data_manifest_{_digest(manifest_body)[:32]}"
    manifest_payload = {"manifest_id": manifest_id, **manifest_body}
    manifest_bytes = canonical_json_bytes(manifest_payload)
    manifest = ArtifactPointer(
        path=(PurePosixPath("manifests") / request.dataset_id / f"{manifest_id}.json").as_posix(),
        artifact_type="json",
        sha256=sha256_bytes(manifest_bytes),
        size_bytes=len(manifest_bytes),
        schema_version=SNAPSHOT_MANIFEST_SCHEMA_VERSION,
    )
    pointer_body: dict[str, object] = {
        "schema_version": CURRENT_POINTER_SCHEMA_VERSION,
        "dataset_id": request.dataset_id,
        "snapshot_id": snapshot_id,
        "manifest_id": manifest_id,
        "source_event_id": source_id,
        "run_id": request.run_id,
        "generation": 1 if previous is None else previous.generation + 1,
        "published_at": request.generated_at.isoformat(),
        "manifest": manifest.to_dict(),
        "snapshot": snapshot.to_dict(),
        "source_event": source.to_dict(),
        "data_quality_evidence_id": published_quality.evidence_id,
        "previous_pointer_id": None if previous is None else previous.pointer_id,
        "previous_pointer_sha256": None if previous is None else previous.pointer_sha256,
        "production_effect": ProductionEffect.NONE.value,
    }
    pointer_payload = {"pointer_id": f"data_pointer_{_digest(pointer_body)[:32]}", **pointer_body}
    _validate_source(source_payload, request.dataset_id, request.run_id, snapshot)
    _validate_manifest(
        manifest_payload,
        request.dataset_id,
        snapshot_id,
        source_id,
        request.run_id,
        snapshot,
        source,
        None,
        quality_report_payload=quality_publication.payload,
    )
    _validate_pointer(pointer_payload, request.dataset_id)
    return _Publication(
        snapshot,
        source,
        manifest,
        report,
        source_payload,
        manifest_payload,
        pointer_payload,
        quality_publication.raw,
    )


def _validate_references(
    root: Path,
    pointer: Mapping[str, object],
) -> tuple[ArtifactEnvelope, tuple[Path, Path, Path]]:
    dataset = _text(pointer, "dataset_id")
    snapshot_id = _text(pointer, "snapshot_id")
    manifest_id = _text(pointer, "manifest_id")
    source_id = _text(pointer, "source_event_id")
    run_id = _text(pointer, "run_id")
    snapshot = _pointer(pointer, "snapshot")
    source = _pointer(pointer, "source_event")
    manifest = _pointer(pointer, "manifest")
    expected_paths = (
        (
            snapshot,
            (
                PurePosixPath("snapshots")
                / dataset
                / snapshot.sha256
                / f"payload.{snapshot.artifact_type}"
            ).as_posix(),
        ),
        (source, (PurePosixPath("source_events") / dataset / f"{source_id}.json").as_posix()),
        (manifest, (PurePosixPath("manifests") / dataset / f"{manifest_id}.json").as_posix()),
    )
    for item, expected in expected_paths:
        if item.path != expected:
            _fail("ARTIFACT_POINTER_PATH_MISMATCH", f"expected={expected} actual={item.path}")
    paths = tuple(_store_path(root, item) for item, _ in expected_paths)
    _verify(root, paths[0], snapshot, "SNAPSHOT_PAYLOAD_INVALID")
    source_payload = _verified_json(root, paths[1], source, "SOURCE_EVENT_MANIFEST_INVALID")
    _validate_source(source_payload, dataset, run_id, snapshot)
    manifest_payload = _verified_json(root, paths[2], manifest, "SNAPSHOT_MANIFEST_INVALID")
    envelope = _validate_manifest(
        manifest_payload,
        dataset,
        snapshot_id,
        source_id,
        run_id,
        snapshot,
        source,
        root,
    )
    data_quality = envelope.data_quality
    if data_quality is None:
        _fail("POINTER_DQ_BINDING_MISMATCH", "envelope has no data quality evidence")
    if pointer.get("data_quality_evidence_id") != data_quality.evidence_id:
        _fail("POINTER_DQ_BINDING_MISMATCH", "pointer/envelope evidence ids differ")
    return envelope, (paths[0], paths[1], paths[2])


def _validate_source(
    payload: Mapping[str, object],
    dataset: str,
    run_id: str,
    snapshot: ArtifactPointer,
) -> None:
    _shape(payload, _SOURCE_FIELDS, SOURCE_EVENT_SCHEMA_VERSION)
    if (payload.get("dataset_id"), payload.get("run_id")) != (dataset, run_id):
        _fail("SOURCE_EVENT_IDENTITY_MISMATCH", "dataset/run mismatch")
    for field in ("source_id", "provider_name", "endpoint", "source_role"):
        _text(payload, field)
    _aware(payload.get("downloaded_at"), "downloaded_at")
    _integer(payload, "row_count", 0)
    if payload.get("response_headers_sanitized") is not True:
        _fail("UNSANITIZED_RESPONSE_HEADERS", "sanitation attestation required")
    parameters = _json_mapping(_mapping(payload.get("request_parameters"), "request_parameters"))
    if payload.get("request_parameters_sha256") != _digest(parameters):
        _fail("REQUEST_PARAMETERS_CHECKSUM_MISMATCH", "parameters changed")
    if _pointer(payload, "snapshot") != snapshot:
        _fail("SOURCE_EVENT_SNAPSHOT_MISMATCH", "snapshot binding differs")
    _effect(payload)
    _semantic_id(payload, "source_event_id", "source_event_")


def _validate_manifest(
    payload: Mapping[str, object],
    dataset: str,
    snapshot_id: str,
    source_id: str,
    run_id: str,
    snapshot: ArtifactPointer,
    source: ArtifactPointer,
    store_root: Path | None,
    *,
    quality_report_payload: Mapping[str, object] | None = None,
) -> ArtifactEnvelope:
    _shape(payload, _MANIFEST_FIELDS, SNAPSHOT_MANIFEST_SCHEMA_VERSION)
    if (
        payload.get("dataset_id"),
        payload.get("snapshot_id"),
        payload.get("source_event_id"),
        payload.get("run_id"),
    ) != (dataset, snapshot_id, source_id, run_id):
        _fail("SNAPSHOT_MANIFEST_IDENTITY_MISMATCH", "identity fields differ")
    _effect(payload)
    if (
        payload.get("dq_execution_provenance_verified") is not False
        or payload.get("filesystem_security_profile") != FILESYSTEM_SECURITY_PROFILE
        or payload.get("trusted_writer_principal_required") is not True
        or payload.get("same_principal_adversarial_mutation_resistance")
        != SAME_PRINCIPAL_ADVERSARIAL_MUTATION_RESISTANCE
        or payload.get("store_acl_verified") is not False
        or payload.get("consumer_cutover_allowed") is not False
        or payload.get("crash_durability_verified") is not False
        or payload.get("same_principal_post_ack_mutation_protection") is not False
    ):
        _fail("D0A_GOVERNANCE_BOUNDARY_INVALID", "security boundary fields are invalid")
    try:
        raw_envelope = _mapping(payload.get("artifact_envelope"), "artifact_envelope")
        _validate_governed_limitations(raw_envelope.get("limitations"))
        envelope = ArtifactEnvelope.from_dict(raw_envelope)
    except ValueError as exc:
        raise DataPublicationIntegrityError("ARTIFACT_ENVELOPE_INVALID", str(exc)) from exc
    if envelope.to_dict() != dict(raw_envelope):
        _fail("ARTIFACT_ENVELOPE_NONCANONICAL", "unknown/normalized fields")
    if (
        envelope.artifact_id,
        envelope.run_id,
        envelope.status,
        envelope.production_effect,
        envelope.payload,
        envelope.input_artifacts,
        envelope.data_quality_required,
    ) != (
        snapshot_id,
        run_id,
        CanonicalStatus.PASS,
        ProductionEffect.NONE,
        snapshot,
        (source,),
        True,
    ):
        _fail("ARTIFACT_ENVELOPE_BINDING_MISMATCH", "snapshot/source/DQ binding differs")
    data_quality = envelope.data_quality
    if data_quality is None:
        _fail("ARTIFACT_ENVELOPE_BINDING_MISMATCH", "data quality evidence is missing")
    data_quality.assert_ready()
    quality = _mapping(payload.get("quality_binding"), "quality_binding")
    _fields(quality, _QUALITY_FIELDS, "QUALITY_BINDING_SCHEMA_FIELDS_INVALID")
    if quality.get("data_quality_evidence_id") != data_quality.evidence_id:
        _fail("QUALITY_EVIDENCE_ID_MISMATCH", "evidence id differs")
    if _pointer(quality, "evaluated_snapshot") != snapshot:
        _fail("QUALITY_SNAPSHOT_BINDING_MISMATCH", "evaluated snapshot differs")
    start = _date(quality.get("coverage_start"), "coverage_start")
    end = _date(quality.get("coverage_end"), "coverage_end")
    if start > end or end > envelope.as_of:
        _fail("QUALITY_COVERAGE_INVALID", "coverage is outside snapshot as-of")
    report = _pointer(quality, "report")
    if (report.path, report.sha256) != (
        data_quality.report_path,
        data_quality.report_sha256,
    ):
        _fail("QUALITY_REPORT_BINDING_MISMATCH", "report differs from evidence")
    expected_report_path = (
        PurePosixPath("quality_reports") / dataset / report.sha256 / "report.json"
    ).as_posix()
    if (
        report.path != expected_report_path
        or report.artifact_type != "json"
        or report.schema_version != DATA_QUALITY_REPORT_SCHEMA_VERSION
    ):
        _fail("QUALITY_REPORT_POINTER_INVALID", report.path)
    report_payload: Mapping[str, object]
    if quality_report_payload is None:
        if store_root is None:
            _fail("QUALITY_REPORT_VALIDATION_CONTEXT_MISSING", report.path)
        report_payload = _verified_json(
            store_root,
            _store_path(store_root, report),
            report,
            "DQ_REPORT_INVALID",
        )
    else:
        report_payload = quality_report_payload
    _validate_quality_report(
        report_payload,
        evidence=data_quality,
        snapshot=snapshot,
        as_of=envelope.as_of,
        coverage_start=start,
        coverage_end=end,
    )
    _semantic_id(payload, "manifest_id", "data_manifest_")
    return envelope


def _validate_governed_limitations(value: object) -> None:
    if not isinstance(value, list):
        _fail("D0A_GOVERNANCE_BOUNDARY_INVALID", "envelope limitations must be a list")
    expected_by_key = dict(_GOVERNED_LIMITATION_VALUES)
    seen: dict[str, int] = {}
    for item in value:
        if not isinstance(item, str):
            _fail(
                "D0A_GOVERNANCE_BOUNDARY_INVALID",
                "envelope limitations must contain strings",
            )
        raw_key, separator, raw_value = item.partition("=")
        normalized_key = raw_key.strip().lower()
        governed_key = next(
            (
                key
                for key in expected_by_key
                if normalized_key == key.lower() or item.strip().lower().startswith(key.lower())
            ),
            None,
        )
        if governed_key is None:
            continue
        expected_value = expected_by_key[governed_key]
        if separator != "=" or raw_key != governed_key or raw_value != expected_value:
            _fail(
                "D0A_GOVERNANCE_BOUNDARY_INVALID",
                f"governed limitation is malformed or contradictory: {item!r}",
            )
        seen[governed_key] = seen.get(governed_key, 0) + 1
        if seen[governed_key] != 1:
            _fail(
                "D0A_GOVERNANCE_BOUNDARY_INVALID",
                f"governed limitation is duplicated: {governed_key}",
            )
    missing = tuple(key for key in expected_by_key if seen.get(key) != 1)
    if missing:
        _fail(
            "D0A_GOVERNANCE_BOUNDARY_INVALID",
            f"governed limitations are missing: {','.join(missing)}",
        )


def _validate_pointer(payload: Mapping[str, object], dataset: str) -> None:
    _shape(payload, _POINTER_FIELDS, CURRENT_POINTER_SCHEMA_VERSION)
    if payload.get("dataset_id") != dataset:
        _fail("CURRENT_POINTER_DATASET_MISMATCH", str(payload.get("dataset_id")))
    for field in (
        "snapshot_id",
        "manifest_id",
        "source_event_id",
        "run_id",
        "data_quality_evidence_id",
    ):
        _text(payload, field)
    generation = _integer(payload, "generation", 1)
    _aware(payload.get("published_at"), "published_at")
    for field in ("manifest", "snapshot", "source_event"):
        _pointer(payload, field)
    predecessor = (payload.get("previous_pointer_id"), payload.get("previous_pointer_sha256"))
    if generation == 1 and predecessor != (None, None):
        _fail("CURRENT_POINTER_PREDECESSOR_INVALID", "generation 1 has predecessor")
    if generation > 1 and (
        not isinstance(predecessor[0], str)
        or not predecessor[0]
        or not isinstance(predecessor[1], str)
        or not _SHA_RE.fullmatch(predecessor[1])
    ):
        _fail("CURRENT_POINTER_PREDECESSOR_REQUIRED", "generation >1 needs id/hash")
    _effect(payload)
    _semantic_id(payload, "pointer_id", "data_pointer_")


def _validate_history(
    root: Path,
    pointer: Mapping[str, object],
    *,
    current_raw: bytes | None = None,
) -> None:
    dataset = _text(pointer, "dataset_id")
    expected_id = _text(pointer, "pointer_id")
    expected_generation = _integer(pointer, "generation", 1)
    expected_sha: str | None = None
    seen: set[str] = set()
    first = True
    while True:
        if expected_id in seen:
            _fail("POINTER_HISTORY_CYCLE", expected_id)
        seen.add(expected_id)
        code = "POINTER_HISTORY_INVALID" if first else "PREVIOUS_POINTER_INVALID"
        raw, stored = _read_json(_history_path(root, dataset, expected_id), code, root)
        _validate_pointer(stored, dataset)
        if (
            stored.get("pointer_id") != expected_id
            or _integer(stored, "generation", 1) != expected_generation
        ):
            _fail("PREVIOUS_POINTER_CHAIN_MISMATCH", "id/generation discontinuity")
        if expected_sha is not None and sha256_bytes(raw) != expected_sha:
            _fail("PREVIOUS_POINTER_CHECKSUM_MISMATCH", "predecessor changed")
        if first:
            if stored != dict(pointer) or (current_raw is not None and raw != current_raw):
                _fail("POINTER_HISTORY_CURRENT_MISMATCH", "history/current bytes differ")
            first = False
        if expected_generation == 1:
            return
        expected_id = _text(stored, "previous_pointer_id")
        expected_sha = _sha(stored.get("previous_pointer_sha256"), "previous_pointer_sha256")
        expected_generation -= 1


def _validate_request(request: SnapshotPublishRequest) -> None:
    _identifier(request.dataset_id, "dataset_id")
    for value, field in (
        (request.run_id, "run_id"),
        (request.producer, "producer"),
        (request.owner, "owner"),
        (request.payload_schema_version, "payload_schema_version"),
        (request.data_quality_report_schema_version, "data_quality_report_schema_version"),
        (request.source_event.source_id, "source_id"),
        (request.source_event.provider_name, "provider_name"),
        (request.source_event.endpoint, "endpoint"),
        (request.source_event.source_role, "source_role"),
    ):
        if not isinstance(value, str) or not value.strip():
            _fail("REQUIRED_FIELD_INVALID", field)
    if not _TYPE_RE.fullmatch(request.payload_artifact_type):
        _fail("INVALID_PAYLOAD_ARTIFACT_TYPE", request.payload_artifact_type)
    if request.data_quality_report_schema_version != DATA_QUALITY_REPORT_SCHEMA_VERSION:
        _fail(
            "DQ_REPORT_SCHEMA_UNSUPPORTED",
            request.data_quality_report_schema_version,
        )
    for timestamp, field in (
        (request.generated_at, "generated_at"),
        (request.source_event.downloaded_at, "downloaded_at"),
        (request.data_quality.checked_at, "DQ checked_at"),
    ):
        if timestamp.tzinfo is None or timestamp.utcoffset() is None:
            _fail("TIMEZONE_REQUIRED", field)
    if request.generated_at.date() < request.as_of:
        _fail("GENERATED_BEFORE_AS_OF", "generated_at precedes as_of")
    if request.source_event.downloaded_at > request.generated_at:
        _fail("SOURCE_EVENT_AFTER_PUBLICATION", "downloaded_at follows generated_at")
    if request.data_quality.checked_at > request.generated_at:
        _fail("DQ_CHECK_AFTER_PUBLICATION", "checked_at follows generated_at")
    if request.data_quality.as_of != request.as_of:
        _fail("DQ_AS_OF_MISMATCH", "DQ and snapshot as_of differ")
    if request.coverage_start > request.coverage_end or request.coverage_end > request.as_of:
        _fail("INVALID_COVERAGE_WINDOW", "coverage outside snapshot as-of")
    if (
        not isinstance(request.source_event.row_count, int)
        or isinstance(request.source_event.row_count, bool)
        or request.source_event.row_count < 0
    ):
        _fail("INVALID_SOURCE_ROW_COUNT", "row_count must be non-negative integer")
    if request.source_event.response_headers_sanitized is not True:
        _fail("UNSANITIZED_RESPONSE_HEADERS", "sanitation attestation required")
    _json_mapping(request.source_event.request_parameters)
    try:
        request.data_quality.assert_ready()
    except ValueError as exc:
        raise DataPublicationIntegrityError("DATA_QUALITY_NOT_READY", str(exc)) from exc
    if request.data_quality.checked_input_count != 1:
        _fail("DQ_INPUT_BINDING_REQUIRED", "checked_input_count must equal candidate count 1")
    report_path = request.data_quality.report_path
    if report_path is None:
        _fail("DQ_REPORT_REFERENCE_REQUIRED", "path/checksum required")
    _portable_parts(report_path, "DQ evidence")


def _quality_report_publication(
    request: SnapshotPublishRequest,
    root: Path,
    snapshot: ArtifactPointer,
) -> _QualityReportPublication:
    evidence = request.data_quality
    if evidence.report_path is None or evidence.report_sha256 is None:
        _fail("DQ_REPORT_REFERENCE_REQUIRED", "path/checksum required")
    path = _evidence_path(root, evidence.report_path)
    raw, payload = _read_json(path, "DQ_REPORT_INVALID", root)
    if sha256_bytes(raw) != evidence.report_sha256:
        _fail("DQ_REPORT_INVALID", "checksum mismatch")
    _validate_quality_report(
        payload,
        evidence=evidence,
        snapshot=snapshot,
        as_of=request.as_of,
        coverage_start=request.coverage_start,
        coverage_end=request.coverage_end,
    )
    pointer = ArtifactPointer(
        path=(
            PurePosixPath("quality_reports")
            / request.dataset_id
            / evidence.report_sha256
            / "report.json"
        ).as_posix(),
        artifact_type="json",
        sha256=evidence.report_sha256,
        size_bytes=len(raw),
        schema_version=DATA_QUALITY_REPORT_SCHEMA_VERSION,
    )
    published_evidence = replace(evidence, report_path=pointer.path)
    return _QualityReportPublication(pointer, raw, payload, published_evidence)


def _validate_quality_report(
    payload: Mapping[str, object],
    *,
    evidence: DataQualityEvidence,
    snapshot: ArtifactPointer,
    as_of: date,
    coverage_start: date,
    coverage_end: date,
) -> None:
    _shape(payload, _DQ_REPORT_FIELDS, DATA_QUALITY_REPORT_SCHEMA_VERSION)
    _effect(payload)
    expected_text = {
        "contract_id": evidence.contract_id,
        "policy_id": evidence.policy_id,
        "policy_version": evidence.policy_version,
        "status": evidence.status,
    }
    for text_field, expected_text_value in expected_text.items():
        if _text(payload, text_field) != expected_text_value:
            _fail("DQ_REPORT_EVIDENCE_MISMATCH", text_field)
    if payload.get("passed") is not True or not evidence.passed:
        _fail("DQ_REPORT_STATUS_INVALID", str(payload.get("status")))
    if _aware(payload.get("checked_at"), "checked_at") != evidence.checked_at:
        _fail("DQ_REPORT_EVIDENCE_MISMATCH", "checked_at")
    if _date(payload.get("as_of"), "as_of") != as_of or evidence.as_of != as_of:
        _fail("DQ_REPORT_EVIDENCE_MISMATCH", "as_of")
    if (
        _date(payload.get("coverage_start"), "coverage_start") != coverage_start
        or _date(payload.get("coverage_end"), "coverage_end") != coverage_end
    ):
        _fail("DQ_REPORT_COVERAGE_MISMATCH", "coverage window differs")
    for count_field, expected_count in (
        ("checked_input_count", evidence.checked_input_count),
        ("error_count", evidence.error_count),
        ("warning_count", evidence.warning_count),
    ):
        if _integer(payload, count_field, 0) != expected_count:
            _fail("DQ_REPORT_EVIDENCE_MISMATCH", count_field)
    if evidence.checked_input_count != 1:
        _fail("DQ_INPUT_BINDING_REQUIRED", "report must evaluate one candidate snapshot")
    blocking = payload.get("blocking_issues")
    if not isinstance(blocking, list) or blocking != list(evidence.blocking_issues):
        _fail("DQ_REPORT_EVIDENCE_MISMATCH", "blocking_issues")
    if _pointer(payload, "evaluated_snapshot") != snapshot:
        _fail("DQ_REPORT_SNAPSHOT_MISMATCH", snapshot.path)


def _shape(payload: Mapping[str, object], fields: set[str], schema: str) -> None:
    _fields(payload, fields, "SCHEMA_FIELDS_INVALID")
    if payload.get("schema_version") != schema:
        _fail("SCHEMA_VERSION_UNSUPPORTED", str(payload.get("schema_version")))


def _fields(payload: Mapping[str, object], fields: set[str], code: str) -> None:
    if set(payload) != fields:
        _fail(code, f"missing={sorted(fields-set(payload))} unknown={sorted(set(payload)-fields)}")


def _effect(payload: Mapping[str, object]) -> None:
    if payload.get("production_effect") != ProductionEffect.NONE.value:
        _fail("PRODUCTION_EFFECT_INVALID", str(payload.get("production_effect")))


def _semantic_id(payload: Mapping[str, object], field: str, prefix: str) -> None:
    semantic = dict(payload)
    supplied = semantic.pop(field, None)
    expected = f"{prefix}{_digest(semantic)[:32]}"
    if supplied != expected:
        _fail(f"{field.upper()}_MISMATCH", f"supplied={supplied} actual={expected}")


def _pointer(payload: Mapping[str, object], field: str) -> ArtifactPointer:
    raw = _mapping(payload.get(field), field)
    try:
        pointer = ArtifactPointer.from_dict(raw)
    except ValueError as exc:
        raise DataPublicationIntegrityError("ARTIFACT_POINTER_INVALID", f"{field}: {exc}") from exc
    if pointer.to_dict() != dict(raw):
        _fail("ARTIFACT_POINTER_NONCANONICAL", field)
    return pointer


def _verified_json(
    root: Path,
    path: Path,
    pointer: ArtifactPointer,
    code: str,
) -> dict[str, object]:
    _verify(root, path, pointer, code)
    return _read_json(path, code, root)[1]


def _read_json(path: Path, code: str, root: Path) -> tuple[bytes, dict[str, object]]:
    try:
        raw = _read_checked_bytes(path, code, root)
        payload = json.loads(raw.decode())
    except DataPublicationError:
        raise
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise DataPublicationIntegrityError(code, str(exc), path=path) from exc
    if not isinstance(payload, dict) or raw != canonical_json_bytes(payload):
        raise DataPublicationIntegrityError(code, "non-object or noncanonical JSON", path=path)
    return raw, payload


def _install(staged: Path, target: Path, pointer: ArtifactPointer) -> None:
    _install_raw(staged, target, pointer.sha256, pointer.size_bytes)


def _install_raw(staged: Path, target: Path, digest: str, size: int) -> None:
    staged_root = staged.parents[3]
    target_root = _common_store_root(target)
    with (
        _bound_directory(staged_root, staged.parent, "staged immutable parent") as staged_binding,
        _bound_directory(target_root, target.parent, "immutable target parent") as target_binding,
    ):
        _verify_raw_bound(
            staged_binding,
            staged,
            digest,
            size,
            "STAGED_IMMUTABLE_OBJECT_INVALID",
        )
        staged_metadata = _bound_path_metadata(
            staged_binding,
            staged,
            "staged immutable object",
        )
        staged_identity = _file_identity(staged_metadata)
        source_descriptor = _checked_open_regular(
            staged,
            os.O_RDONLY,
            "staged immutable object",
            binding=staged_binding,
        )
        created = False
        try:
            try:
                if os.name == "nt":
                    os.link(staged, target, follow_symlinks=False)
                else:
                    assert staged_binding.descriptor is not None
                    assert target_binding.descriptor is not None
                    os.link(
                        _bound_leaf(staged_binding, staged, "staged immutable object"),
                        _bound_leaf(target_binding, target, "immutable target"),
                        src_dir_fd=staged_binding.descriptor,
                        dst_dir_fd=target_binding.descriptor,
                        follow_symlinks=False,
                    )
                created = True
            except FileExistsError:
                _verify_raw_bound(
                    target_binding,
                    target,
                    digest,
                    size,
                    "IMMUTABLE_OBJECT_CONFLICT",
                    conflict=True,
                )
                os.close(source_descriptor)
                source_descriptor = -1
                _unlink_bound_checked(
                    staged_binding,
                    staged,
                    expected_identity=staged_identity,
                    expected_nlink=1,
                )
                return
            except OSError as exc:
                raise DataPublicationError(
                    "IMMUTABLE_INSTALL_FAILED",
                    "atomic no-overwrite hard link failed",
                    path=target,
                ) from exc
            staged_link = _bound_path_metadata(
                staged_binding,
                staged,
                "staged immutable link",
                expected_nlink=2,
            )
            target_link = _bound_path_metadata(
                target_binding,
                target,
                "installed immutable link",
                expected_nlink=2,
            )
            if (
                _file_identity(staged_link) != staged_identity
                or _file_identity(target_link) != staged_identity
            ):
                _fail("IMMUTABLE_INSTALL_IDENTITY_MISMATCH", str(target))
            os.close(source_descriptor)
            source_descriptor = -1
            _unlink_bound_checked(
                staged_binding,
                staged,
                expected_identity=staged_identity,
                expected_nlink=2,
            )
            _verify_raw_bound(
                target_binding,
                target,
                digest,
                size,
                "IMMUTABLE_OBJECT_POST_INSTALL_INVALID",
            )
        except Exception:
            if created:
                _unlink_bound_created_target(
                    target_binding,
                    target,
                    staged_identity,
                )
            raise
        finally:
            if source_descriptor >= 0:
                try:
                    os.close(source_descriptor)
                except OSError:
                    pass


def _verify(root: Path, path: Path, pointer: ArtifactPointer, code: str) -> None:
    _verify_raw(path, pointer.sha256, pointer.size_bytes, code, root=root)


def _verify_raw(
    path: Path,
    digest: str,
    size: int | None,
    code: str,
    *,
    conflict: bool = False,
    root: Path,
) -> None:
    error = DataPublicationConflictError if conflict else DataPublicationIntegrityError
    try:
        actual_size, actual = _hash_checked_file(path, code, root)
        if size is not None and actual_size != size:
            raise error(code, "size mismatch", path=path)
    except DataPublicationError:
        raise
    except OSError as exc:
        raise error(code, str(exc), path=path) from exc
    if actual != digest:
        raise error(code, f"sha256 expected={digest} actual={actual}", path=path)


def _verify_raw_bound(
    binding: _BoundDirectory,
    path: Path,
    digest: str,
    size: int | None,
    code: str,
    *,
    conflict: bool = False,
) -> None:
    error = DataPublicationConflictError if conflict else DataPublicationIntegrityError
    try:
        actual_size, actual = _hash_bound_file(binding, path, code)
        if size is not None and actual_size != size:
            raise error(code, "size mismatch", path=path)
    except DataPublicationError:
        raise
    except OSError as exc:
        raise error(code, str(exc), path=path) from exc
    if actual != digest:
        raise error(code, f"sha256 expected={digest} actual={actual}", path=path)


def _file_identity(metadata: os.stat_result) -> _FileIdentity:
    return _FileIdentity(metadata.st_dev, metadata.st_ino)


def _validate_directory_metadata(metadata: os.stat_result, path: Path, field: str) -> None:
    if _metadata_is_reparse(metadata):
        raise DataPublicationIntegrityError(
            "ARTIFACT_PATH_REPARSE_POINT",
            field,
            path=path,
        )
    if not stat.S_ISDIR(metadata.st_mode):
        raise DataPublicationIntegrityError(
            "ARTIFACT_PATH_PARENT_INVALID",
            field,
            path=path,
        )


def _windows_open_descriptor(
    path: Path,
    flags: int,
    *,
    directory: bool,
    delete_access: bool = False,
    exclusive_share: bool = False,
) -> int:
    import ctypes
    import msvcrt

    kernel32: Any = ctypes.WinDLL("kernel32", use_last_error=True)
    create_file: Any = kernel32.CreateFileW
    create_file.argtypes = [
        ctypes.c_wchar_p,
        ctypes.c_uint32,
        ctypes.c_uint32,
        ctypes.c_void_p,
        ctypes.c_uint32,
        ctypes.c_uint32,
        ctypes.c_void_p,
    ]
    create_file.restype = ctypes.c_void_p
    generic_read = 0x80000000
    generic_write = 0x40000000
    delete = 0x00010000
    file_share_read = 0x00000001
    file_share_write = 0x00000002
    create_new = 1
    open_existing = 3
    open_always = 4
    file_flag_open_reparse_point = 0x00200000
    file_flag_backup_semantics = 0x02000000
    writable = bool(flags & (os.O_WRONLY | os.O_RDWR))
    desired_access = generic_read | (generic_write if writable else 0)
    if delete_access:
        desired_access |= delete
    if flags & os.O_CREAT:
        creation = create_new if flags & os.O_EXCL else open_always
    else:
        creation = open_existing
    attributes = file_flag_open_reparse_point
    if directory:
        attributes |= file_flag_backup_semantics
    raw_handle = create_file(
        str(path),
        desired_access,
        0 if exclusive_share else file_share_read | file_share_write,
        None,
        creation,
        attributes,
        None,
    )
    invalid_handle = ctypes.c_void_p(-1).value
    if raw_handle in {None, invalid_handle}:
        error = ctypes.get_last_error()
        raise OSError(error, ctypes.FormatError(error), str(path))
    crt_flags = flags & (os.O_RDONLY | os.O_WRONLY | os.O_RDWR)
    crt_flags |= getattr(os, "O_BINARY", 0)
    try:
        return msvcrt.open_osfhandle(raw_handle, crt_flags)
    except Exception:
        kernel32.CloseHandle(ctypes.c_void_p(raw_handle))
        raise


def _open_directory_descriptor(path: Path) -> int:
    if os.name == "nt":
        return _windows_open_descriptor(path, os.O_RDONLY, directory=True)
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
    flags |= getattr(os, "O_CLOEXEC", 0)
    return os.open(path, flags)


@contextmanager
def _open_bound_directory(
    root: Path,
    parent: Path,
    field: str,
    *,
    create: bool,
    base: _BoundDirectory | None,
) -> Iterator[_BoundDirectory]:
    try:
        relative = parent.relative_to(root)
    except ValueError as exc:
        raise DataPublicationIntegrityError(
            "ARTIFACT_PATH_ESCAPES_ROOT",
            field,
            path=parent,
        ) from exc
    descriptors: list[int] = []
    yielded = False
    try:
        if base is None:
            current_path = root
            current_descriptor = _open_directory_descriptor(root)
            descriptors.append(current_descriptor)
        else:
            if base.root != root or base.path != root or base.descriptor is None:
                _fail("ARTIFACT_ROOT_AUTHORITY_INVALID", str(root))
            current_path = root
            current_descriptor = os.dup(base.descriptor)
            descriptors.append(current_descriptor)
        root_metadata = os.fstat(current_descriptor)
        _validate_directory_metadata(root_metadata, current_path, field)
        if os.name == "nt":
            path_metadata = current_path.lstat()
            if not os.path.samestat(root_metadata, path_metadata):
                _fail("ARTIFACT_COMPONENT_REPLACED", str(current_path))
        for component in relative.parts:
            candidate = current_path / component
            if os.name == "nt":
                if create:
                    try:
                        candidate.mkdir()
                    except FileExistsError:
                        pass
                child_descriptor = _open_directory_descriptor(candidate)
            else:
                if create:
                    try:
                        os.mkdir(component, mode=0o700, dir_fd=current_descriptor)
                    except FileExistsError:
                        pass
                path_metadata = os.stat(
                    component,
                    dir_fd=current_descriptor,
                    follow_symlinks=False,
                )
                _validate_directory_metadata(path_metadata, candidate, field)
                directory_flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
                directory_flags |= getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_CLOEXEC", 0)
                child_descriptor = os.open(
                    component,
                    directory_flags,
                    dir_fd=current_descriptor,
                )
            descriptors.append(child_descriptor)
            child_metadata = os.fstat(child_descriptor)
            _validate_directory_metadata(child_metadata, candidate, field)
            if os.name == "nt":
                path_metadata = candidate.lstat()
                if not os.path.samestat(child_metadata, path_metadata):
                    _fail("ARTIFACT_COMPONENT_REPLACED", str(candidate))
            elif not os.path.samestat(child_metadata, path_metadata):
                _fail("ARTIFACT_COMPONENT_REPLACED", str(candidate))
            current_path = candidate
            current_descriptor = child_descriptor
        yielded = True
        yield _BoundDirectory(
            root=root,
            path=parent,
            identity=_file_identity(os.fstat(current_descriptor)),
            descriptor=current_descriptor,
        )
    except OSError as exc:
        if yielded:
            raise
        raise DataPublicationIntegrityError(
            "ARTIFACT_BOUND_DIRECTORY_FAILED",
            field,
            path=parent,
        ) from exc
    finally:
        for descriptor in reversed(descriptors):
            try:
                os.close(descriptor)
            except OSError:
                pass


@contextmanager
def _root_authority(root: Path) -> Iterator[_BoundDirectory]:
    active = _ACTIVE_ROOT_BINDING.get()
    if active is not None:
        if active.root != root or active.path != root:
            _fail("ARTIFACT_ROOT_AUTHORITY_CONFLICT", str(root))
        yield active
        return
    with _open_bound_directory(
        root,
        root,
        "store root authority",
        create=False,
        base=None,
    ) as binding:
        token = _ACTIVE_ROOT_BINDING.set(binding)
        try:
            yield binding
        finally:
            _ACTIVE_ROOT_BINDING.reset(token)


@contextmanager
def _bound_directory(
    root: Path,
    parent: Path,
    field: str,
    *,
    create: bool = False,
) -> Iterator[_BoundDirectory]:
    active = _ACTIVE_ROOT_BINDING.get()
    if active is None or active.root != root:
        with _open_bound_directory(
            root,
            parent,
            field,
            create=create,
            base=None,
        ) as binding:
            yield binding
        return
    with _open_bound_directory(
        root,
        parent,
        field,
        create=create,
        base=active,
    ) as binding:
        yield binding


def _bound_leaf(binding: _BoundDirectory, path: Path, field: str) -> str:
    if path.parent != binding.path or not path.name:
        _fail("ARTIFACT_BOUND_PATH_INVALID", f"{field}: {path}")
    return path.name


def _bound_path_metadata(
    binding: _BoundDirectory,
    path: Path,
    field: str,
    *,
    expected_nlink: int = 1,
) -> os.stat_result:
    name = _bound_leaf(binding, path, field)
    try:
        if os.name == "nt":
            metadata = path.lstat()
        else:
            assert binding.descriptor is not None
            metadata = os.stat(name, dir_fd=binding.descriptor, follow_symlinks=False)
    except OSError as exc:
        raise DataPublicationIntegrityError(
            "ARTIFACT_PATH_INSPECTION_FAILED",
            field,
            path=path,
        ) from exc
    _validate_regular_metadata(metadata, path, field, expected_nlink=expected_nlink)
    return metadata


def _bound_regular_metadata_any_links(
    binding: _BoundDirectory,
    path: Path,
    field: str,
) -> os.stat_result:
    name = _bound_leaf(binding, path, field)
    try:
        if os.name == "nt":
            metadata = path.lstat()
        else:
            assert binding.descriptor is not None
            metadata = os.stat(name, dir_fd=binding.descriptor, follow_symlinks=False)
    except OSError as exc:
        raise DataPublicationIntegrityError(
            "ARTIFACT_PATH_INSPECTION_FAILED",
            field,
            path=path,
        ) from exc
    if _metadata_is_reparse(metadata):
        raise DataPublicationIntegrityError(
            "ARTIFACT_PATH_REPARSE_POINT",
            field,
            path=path,
        )
    if not stat.S_ISREG(metadata.st_mode):
        raise DataPublicationIntegrityError(
            "ARTIFACT_NOT_REGULAR_FILE",
            field,
            path=path,
        )
    return metadata


def _bound_path_exists(binding: _BoundDirectory, path: Path, field: str) -> bool:
    name = _bound_leaf(binding, path, field)
    try:
        if os.name == "nt":
            path.lstat()
        else:
            assert binding.descriptor is not None
            os.stat(name, dir_fd=binding.descriptor, follow_symlinks=False)
    except FileNotFoundError:
        return False
    except OSError as exc:
        raise DataPublicationIntegrityError(
            "ARTIFACT_PATH_INSPECTION_FAILED",
            field,
            path=path,
        ) from exc
    return True


def _metadata_is_reparse(metadata: os.stat_result) -> bool:
    reparse_flag = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400)
    return stat.S_ISLNK(metadata.st_mode) or bool(
        getattr(metadata, "st_file_attributes", 0) & reparse_flag
    )


def _validate_regular_metadata(
    metadata: os.stat_result,
    path: Path,
    field: str,
    *,
    expected_nlink: int = 1,
) -> None:
    if _metadata_is_reparse(metadata):
        raise DataPublicationIntegrityError(
            "ARTIFACT_PATH_REPARSE_POINT",
            field,
            path=path,
        )
    if not stat.S_ISREG(metadata.st_mode):
        raise DataPublicationIntegrityError(
            "ARTIFACT_NOT_REGULAR_FILE",
            field,
            path=path,
        )
    if metadata.st_nlink != expected_nlink:
        raise DataPublicationIntegrityError(
            "ARTIFACT_MULTIPLE_LINKS",
            f"{field}: expected nlink={expected_nlink} actual={metadata.st_nlink}",
            path=path,
        )


def _checked_open_regular(
    path: Path,
    flags: int,
    field: str,
    *,
    binding: _BoundDirectory,
    mode: int = 0o600,
    delete_access: bool = False,
    exclusive_share: bool = False,
) -> int:
    before = (
        _bound_path_metadata(binding, path, field)
        if _bound_path_exists(binding, path, field)
        else None
    )
    safe_flags = flags | getattr(os, "O_BINARY", 0) | getattr(os, "O_NOINHERIT", 0)
    safe_flags |= getattr(os, "O_NOFOLLOW", 0)
    try:
        if os.name == "nt":
            descriptor = _windows_open_descriptor(
                path,
                safe_flags,
                directory=False,
                delete_access=delete_access,
                exclusive_share=exclusive_share,
            )
        else:
            assert binding.descriptor is not None
            descriptor = os.open(
                _bound_leaf(binding, path, field),
                safe_flags,
                mode,
                dir_fd=binding.descriptor,
            )
    except OSError as exc:
        raise DataPublicationIntegrityError(
            "ARTIFACT_OPEN_FAILED",
            field,
            path=path,
        ) from exc
    try:
        opened = os.fstat(descriptor)
        _validate_regular_metadata(opened, path, field)
        after = _bound_path_metadata(binding, path, field)
        if not os.path.samestat(opened, after) or (
            before is not None and not os.path.samestat(before, opened)
        ):
            _fail("ARTIFACT_COMPONENT_REPLACED", f"{field}: {path}")
    except Exception:
        os.close(descriptor)
        raise
    return descriptor


def _assert_descriptor_path(
    descriptor: int,
    path: Path,
    field: str,
    binding: _BoundDirectory,
) -> os.stat_result:
    opened = os.fstat(descriptor)
    _validate_regular_metadata(opened, path, field)
    current = _bound_path_metadata(binding, path, field)
    if not os.path.samestat(opened, current):
        _fail("ARTIFACT_COMPONENT_REPLACED", f"{field}: {path}")
    return opened


def _file_version(metadata: os.stat_result) -> tuple[int, int, int]:
    return (metadata.st_size, metadata.st_mtime_ns, metadata.st_ctime_ns)


def _read_checked_bytes(path: Path, field: str, root: Path) -> bytes:
    with _bound_directory(root, path.parent, field) as binding:
        return _read_bound_bytes(binding, path, field)


def _read_bound_bytes(binding: _BoundDirectory, path: Path, field: str) -> bytes:
    descriptor = _checked_open_regular(path, os.O_RDONLY, field, binding=binding)
    try:
        before = os.fstat(descriptor)
        chunks: list[bytes] = []
        while chunk := os.read(descriptor, 1024 * 1024):
            chunks.append(chunk)
        after = _assert_descriptor_path(descriptor, path, field, binding)
        if _file_version(before) != _file_version(after):
            _fail("ARTIFACT_CHANGED_DURING_READ", f"{field}: {path}")
        return b"".join(chunks)
    finally:
        os.close(descriptor)


def _hash_checked_file(path: Path, field: str, root: Path) -> tuple[int, str]:
    with _bound_directory(root, path.parent, field) as binding:
        return _hash_bound_file(binding, path, field)


def _hash_bound_file(
    binding: _BoundDirectory,
    path: Path,
    field: str,
) -> tuple[int, str]:
    descriptor = _checked_open_regular(path, os.O_RDONLY, field, binding=binding)
    try:
        before = os.fstat(descriptor)
        digest = hashlib.sha256()
        while chunk := os.read(descriptor, 1024 * 1024):
            digest.update(chunk)
        after = _assert_descriptor_path(descriptor, path, field, binding)
        if _file_version(before) != _file_version(after):
            _fail("ARTIFACT_CHANGED_DURING_READ", f"{field}: {path}")
        return after.st_size, digest.hexdigest()
    finally:
        os.close(descriptor)


def _common_store_root(path: Path) -> Path:
    for parent in path.parents:
        if parent.name in {
            "snapshots",
            "source_events",
            "manifests",
            "quality_reports",
            "pointer_history",
        }:
            return parent.parent
    _fail("ARTIFACT_STORE_ROOT_UNRESOLVED", str(path))


def _unlink_bound_checked(
    binding: _BoundDirectory,
    path: Path,
    *,
    expected_identity: _FileIdentity,
    expected_nlink: int,
) -> None:
    metadata = _bound_path_metadata(
        binding,
        path,
        "unlink candidate",
        expected_nlink=expected_nlink,
    )
    if _file_identity(metadata) != expected_identity:
        _fail("ARTIFACT_COMPONENT_REPLACED", str(path))
    try:
        if os.name == "nt":
            path.unlink()
        else:
            assert binding.descriptor is not None
            os.unlink(
                _bound_leaf(binding, path, "unlink candidate"),
                dir_fd=binding.descriptor,
            )
    except OSError as exc:
        raise DataPublicationError("ARTIFACT_UNLINK_FAILED", str(exc), path=path) from exc
    if _bound_path_exists(binding, path, "unlink candidate"):
        _fail("ARTIFACT_UNLINK_FAILED", str(path))


def _unlink_bound_created_target(
    binding: _BoundDirectory,
    target: Path,
    expected_identity: _FileIdentity,
) -> None:
    try:
        try:
            metadata = _bound_path_metadata(
                binding,
                target,
                "installed immutable cleanup",
                expected_nlink=1,
            )
        except DataPublicationIntegrityError as exc:
            if exc.code != "ARTIFACT_MULTIPLE_LINKS":
                raise
            metadata = _bound_path_metadata(
                binding,
                target,
                "installed immutable cleanup",
                expected_nlink=2,
            )
        if metadata.st_nlink not in {1, 2} or _file_identity(metadata) != expected_identity:
            return
        if os.name == "nt":
            target.unlink()
        else:
            assert binding.descriptor is not None
            os.unlink(
                _bound_leaf(binding, target, "installed immutable cleanup"),
                dir_fd=binding.descriptor,
            )
    except (OSError, DataPublicationError):
        pass


def _write_checked_atomic(
    root: Path,
    path: Path,
    content: bytes,
    field: str,
) -> ArtifactWriteResult:
    with _bound_directory(root, path.parent, field) as binding:
        if _bound_path_exists(binding, path, field):
            _bound_path_metadata(binding, path, field)
        result = _write_bytes_atomic_bound(binding, path, content)
        _verify_raw_bound(
            binding,
            path,
            result.sha256,
            result.size_bytes,
            "ATOMIC_ARTIFACT_POST_WRITE_INVALID",
        )
        return result


def _commit_current_atomic(
    root: Path,
    path: Path,
    content: bytes,
    *,
    previous_bytes: bytes | None,
) -> tuple[ArtifactWriteResult, tuple[str, ...]]:
    """Replace current as the final commit operation, with no fallible post-check."""

    with _bound_directory(root, path.parent, "current pointer") as binding:
        current_exists = _bound_path_exists(binding, path, "current pointer")
        if current_exists:
            _bound_path_metadata(binding, path, "current pointer")
        if previous_bytes is None and current_exists:
            raise DataPublicationConflictError(
                "CURRENT_CHANGED_BEFORE_COMMIT",
                "expected no current pointer",
                path=path,
            )
        if previous_bytes is not None:
            if not current_exists:
                raise DataPublicationConflictError(
                    "CURRENT_CHANGED_BEFORE_COMMIT",
                    "expected previous current pointer",
                    path=path,
                )
            if _read_bound_bytes(binding, path, "current pointer pre-commit") != previous_bytes:
                raise DataPublicationConflictError(
                    "CURRENT_CHANGED_BEFORE_COMMIT",
                    "previous current bytes changed",
                    path=path,
                )
        # All references and pointer history are valid before this call. The bound
        # writer fsyncs, replaces the exact source fd, then attests identity/nlink.
        # A failed attestation restores previous_bytes (or removes generation 1)
        # before raising; only a successful return is a commit acknowledgement.
        current_state = _CurrentCommitState(previous_bytes)
        commit_warnings: list[str] = []
        result = _write_bytes_atomic_bound(
            binding,
            path,
            content,
            current_state=current_state,
            committed_warnings=commit_warnings,
        )
        return result, tuple(commit_warnings)


def _write_bytes_atomic_bound(
    binding: _BoundDirectory,
    path: Path,
    content: bytes,
    *,
    current_state: _CurrentCommitState | None = None,
    committed_warnings: list[str] | None = None,
) -> ArtifactWriteResult:
    assert binding.descriptor is not None
    name = _bound_leaf(binding, path, "bound atomic write")
    temporary_name = f".{name}.{uuid4().hex}.tmp"
    temporary_path = path.parent / temporary_name
    descriptor: int | None = None
    replaced = False
    attested = False
    try:
        flags = os.O_RDWR | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
        flags |= getattr(os, "O_CLOEXEC", 0)
        descriptor = _checked_open_regular(
            temporary_path,
            flags,
            "bound atomic temporary",
            binding=binding,
            delete_access=(os.name == "nt"),
            exclusive_share=(os.name == "nt"),
        )
        view = memoryview(content)
        written = 0
        while written < len(view):
            count = os.write(descriptor, view[written:])
            if count <= 0:
                raise OSError("short write")
            written += count
        os.fsync(descriptor)
        pre_replace = os.fstat(descriptor)
        source_identity = _file_identity(pre_replace)
        pre_payload_version = (pre_replace.st_size, pre_replace.st_mtime_ns)
        if pre_replace.st_size != len(content):
            raise OSError("atomic temporary size mismatch")
        _validate_regular_metadata(
            pre_replace,
            temporary_path,
            "bound atomic temporary pre-replace",
        )
        _assert_descriptor_path(
            descriptor,
            temporary_path,
            "bound atomic temporary pre-replace",
            binding,
        )
        expected_digest = sha256_bytes(content)
        if _hash_open_descriptor(descriptor) != expected_digest:
            raise DataPublicationIntegrityError(
                "ATOMIC_COMMIT_ATTESTATION_FAILED",
                "temporary digest mismatch before replace",
                path=temporary_path,
            )
        _replace_bound_temporary(
            binding,
            temporary_name,
            path,
            descriptor,
            current_precondition=current_state,
        )
        replaced = True
        try:
            post_replace = _attest_atomic_descriptor(
                binding,
                path,
                descriptor,
                "ATOMIC_COMMIT_ATTESTATION_FAILED",
            )
            if post_replace.st_size != len(content):
                _fail("ATOMIC_COMMIT_ATTESTATION_FAILED", "post-replace size mismatch")
            if (post_replace.st_size, post_replace.st_mtime_ns) != pre_payload_version:
                _fail("ATOMIC_COMMIT_ATTESTATION_FAILED", "post-replace version mismatch")
            if _hash_open_descriptor(descriptor) != expected_digest:
                _fail("ATOMIC_COMMIT_ATTESTATION_FAILED", "post-replace digest mismatch")
            _attest_atomic_descriptor(
                binding,
                path,
                descriptor,
                "ATOMIC_COMMIT_ATTESTATION_FAILED",
            )
            attested = True
        except Exception as attestation_error:
            try:
                _rollback_invalid_atomic_candidate(
                    binding,
                    path,
                    descriptor,
                    source_identity,
                    current_state=current_state,
                )
            except Exception as rollback_error:
                descriptor = None
                raise DataPublicationIntegrityError(
                    "ATOMIC_COMMIT_ROLLBACK_INDETERMINATE",
                    f"attestation={attestation_error}; rollback={rollback_error}",
                    path=path,
                    commit_state="INDETERMINATE",
                ) from rollback_error
            descriptor = None
            replaced = False
            raise DataPublicationIntegrityError(
                "ATOMIC_COMMIT_ROLLED_BACK",
                str(attestation_error),
                path=path,
                commit_state="ROLLED_BACK",
            ) from attestation_error
    except Exception as exc:
        if descriptor is not None:
            try:
                os.close(descriptor)
            except OSError:
                pass
            descriptor = None
        if not replaced:
            try:
                if os.name == "nt":
                    temporary_path.unlink()
                else:
                    os.unlink(temporary_name, dir_fd=binding.descriptor)
            except OSError:
                pass
        if isinstance(exc, (ArtifactWriteError, DataPublicationError)):
            raise
        raise ArtifactWriteError("ATOMIC_ARTIFACT_WRITE_FAILED", path, str(exc)) from exc
    finally:
        if descriptor is not None:
            try:
                os.close(descriptor)
            except OSError as exc:
                if attested and current_state is not None:
                    if committed_warnings is None:
                        raise AssertionError("current commit warnings sink is required") from exc
                    warning = f"CURRENT_DESCRIPTOR_CLOSE_FAILED: {exc}"
                    try:
                        os.close(descriptor)
                    except OSError as retry_error:
                        warning = f"{warning}; retry={retry_error}"
                    committed_warnings.append(warning)
                elif attested:
                    raise ArtifactWriteError(
                        "ATOMIC_DESCRIPTOR_CLOSE_FAILED",
                        path,
                        str(exc),
                    ) from exc
    return ArtifactWriteResult(
        path=path,
        artifact_type=path.suffix.lower().lstrip(".") or "file",
        sha256=sha256_bytes(content),
        size_bytes=len(content),
    )


def _rollback_invalid_atomic_candidate(
    binding: _BoundDirectory,
    path: Path,
    source_descriptor: int,
    failed_identity: _FileIdentity,
    *,
    current_state: _CurrentCommitState | None,
) -> None:
    if current_state is not None and current_state.previous_bytes is not None:
        try:
            os.close(source_descriptor)
        except OSError as exc:
            raise DataPublicationIntegrityError(
                "CURRENT_ROLLBACK_SOURCE_CLOSE_FAILED",
                str(exc),
                path=path,
            ) from exc
        restored = _write_bytes_atomic_bound(
            binding,
            path,
            current_state.previous_bytes,
        )
        _verify_raw_bound(
            binding,
            path,
            sha256_bytes(current_state.previous_bytes),
            len(current_state.previous_bytes),
            "CURRENT_ROLLBACK_VALIDATION_FAILED",
        )
        if restored.sha256 != sha256_bytes(current_state.previous_bytes):
            _fail("CURRENT_ROLLBACK_VALIDATION_FAILED", "restored digest mismatch")
        return

    rollback_error: Exception | None = None
    try:
        if os.name == "nt":
            _windows_mark_delete_pending(source_descriptor)
        else:
            metadata = _bound_regular_metadata_any_links(
                binding,
                path,
                "invalid atomic candidate rollback",
            )
            if _file_identity(metadata) != failed_identity:
                _fail("ATOMIC_ROLLBACK_IDENTITY_MISMATCH", str(path))
            assert binding.descriptor is not None
            os.unlink(
                _bound_leaf(binding, path, "invalid atomic candidate rollback"),
                dir_fd=binding.descriptor,
            )
    except Exception as exc:
        rollback_error = exc
    try:
        os.close(source_descriptor)
    except OSError as exc:
        rollback_error = rollback_error or exc
    if rollback_error is not None:
        raise DataPublicationIntegrityError(
            "ATOMIC_ROLLBACK_DELETE_FAILED",
            str(rollback_error),
            path=path,
        ) from rollback_error
    if _bound_path_exists(binding, path, "invalid atomic candidate rollback"):
        _fail("ATOMIC_ROLLBACK_FAILED", str(path))


def _windows_mark_delete_pending(source_descriptor: int) -> None:
    import ctypes
    import msvcrt
    from ctypes import wintypes

    class _FileDispositionInformation(ctypes.Structure):
        _fields_ = [("delete_file", wintypes.BOOLEAN)]

    disposition = _FileDispositionInformation(1)
    kernel32: Any = ctypes.WinDLL("kernel32", use_last_error=True)
    set_information: Any = kernel32.SetFileInformationByHandle
    set_information.argtypes = [
        wintypes.HANDLE,
        ctypes.c_int,
        ctypes.c_void_p,
        wintypes.DWORD,
    ]
    set_information.restype = wintypes.BOOL
    file_disposition_info = 4
    if not set_information(
        msvcrt.get_osfhandle(source_descriptor),
        file_disposition_info,
        ctypes.byref(disposition),
        ctypes.sizeof(disposition),
    ):
        error = ctypes.get_last_error()
        raise OSError(error, ctypes.FormatError(error))


def _replace_bound_temporary(
    binding: _BoundDirectory,
    temporary_name: str,
    target: Path,
    source_descriptor: int,
    *,
    current_precondition: _CurrentCommitState | None = None,
) -> None:
    temporary_path = binding.path / temporary_name
    _attest_atomic_descriptor(
        binding,
        temporary_path,
        source_descriptor,
        "ATOMIC_COMMIT_ATTESTATION_FAILED",
    )
    if current_precondition is not None:
        _assert_current_pre_replace(
            binding,
            target,
            current_precondition.previous_bytes,
        )
    if os.name == "nt":
        _windows_replace_file_handle(source_descriptor, binding, target)
        return
    assert binding.descriptor is not None
    os.replace(
        temporary_name,
        _bound_leaf(binding, target, "bound atomic replace"),
        src_dir_fd=binding.descriptor,
        dst_dir_fd=binding.descriptor,
    )


def _assert_current_pre_replace(
    binding: _BoundDirectory,
    path: Path,
    expected_previous_bytes: bytes | None,
) -> None:
    """Rebind the exact current state immediately before the commit rename."""

    try:
        exists = _bound_path_exists(binding, path, "current pointer final precondition")
        if expected_previous_bytes is None:
            if not exists:
                return
            raise DataPublicationConflictError(
                "CURRENT_CHANGED_BEFORE_COMMIT",
                "expected no current pointer at final pre-replace check",
                path=path,
            )
        if not exists:
            raise DataPublicationConflictError(
                "CURRENT_CHANGED_BEFORE_COMMIT",
                "expected previous current pointer at final pre-replace check",
                path=path,
            )
        actual = _read_bound_bytes(binding, path, "current pointer final precondition")
        if actual != expected_previous_bytes:
            raise DataPublicationConflictError(
                "CURRENT_CHANGED_BEFORE_COMMIT",
                "previous current bytes changed before commit rename",
                path=path,
            )
    except DataPublicationConflictError:
        raise
    except (OSError, DataPublicationError) as exc:
        raise DataPublicationConflictError(
            "CURRENT_CHANGED_BEFORE_COMMIT",
            str(exc),
            path=path,
        ) from exc


def _attest_atomic_descriptor(
    binding: _BoundDirectory,
    path: Path,
    descriptor: int,
    code: str,
) -> os.stat_result:
    try:
        opened = os.fstat(descriptor)
        named = _bound_regular_metadata_any_links(binding, path, code)
    except Exception as exc:
        raise DataPublicationIntegrityError(code, str(exc), path=path) from exc
    identity_matches = os.path.samestat(opened, named)
    if (
        not stat.S_ISREG(opened.st_mode)
        or _metadata_is_reparse(opened)
        or not identity_matches
        or opened.st_nlink != 1
        or named.st_nlink != 1
    ):
        raise DataPublicationIntegrityError(
            code,
            (
                f"identity_matches={identity_matches} "
                f"fd_nlink={opened.st_nlink} path_nlink={named.st_nlink}"
            ),
            path=path,
        )
    return opened


def _hash_open_descriptor(descriptor: int) -> str:
    before = os.fstat(descriptor)
    _validate_hash_descriptor_metadata(before, "before")
    os.lseek(descriptor, 0, os.SEEK_SET)
    digest = hashlib.sha256()
    while chunk := os.read(descriptor, 1024 * 1024):
        digest.update(chunk)
    after = os.fstat(descriptor)
    _validate_hash_descriptor_metadata(after, "after")
    if (
        not os.path.samestat(before, after)
        or _file_version(before) != _file_version(after)
        or before.st_nlink != after.st_nlink
    ):
        raise DataPublicationIntegrityError(
            "ATOMIC_DESCRIPTOR_CHANGED_DURING_HASH",
            (
                f"before_version={_file_version(before)} "
                f"after_version={_file_version(after)} "
                f"before_nlink={before.st_nlink} after_nlink={after.st_nlink}"
            ),
        )
    return digest.hexdigest()


def _validate_hash_descriptor_metadata(metadata: os.stat_result, phase: str) -> None:
    if (
        not stat.S_ISREG(metadata.st_mode)
        or _metadata_is_reparse(metadata)
        or metadata.st_nlink != 1
    ):
        raise DataPublicationIntegrityError(
            "ATOMIC_DESCRIPTOR_SECURITY_INVALID",
            (
                f"phase={phase} regular={stat.S_ISREG(metadata.st_mode)} "
                f"reparse={_metadata_is_reparse(metadata)} nlink={metadata.st_nlink}"
            ),
        )


def _windows_replace_file_handle(
    source_descriptor: int,
    binding: _BoundDirectory,
    target: Path,
) -> None:
    import ctypes
    import msvcrt
    from ctypes import wintypes

    _bound_leaf(binding, target, "bound Windows atomic replace")

    class _FileRenameInformation(ctypes.Structure):
        _fields_ = [
            ("replace_if_exists", wintypes.BOOLEAN),
            ("root_directory", wintypes.HANDLE),
            ("file_name_length", wintypes.DWORD),
            ("file_name", wintypes.WCHAR * 1),
        ]

    target_bytes = str(target).encode("utf-16-le")
    buffer_size = _FileRenameInformation.file_name.offset + len(target_bytes) + 2
    buffer = ctypes.create_string_buffer(buffer_size)
    information = ctypes.cast(
        buffer,
        ctypes.POINTER(_FileRenameInformation),
    ).contents
    information.replace_if_exists = 1
    information.root_directory = None
    information.file_name_length = len(target_bytes)
    ctypes.memmove(
        ctypes.addressof(buffer) + _FileRenameInformation.file_name.offset,
        target_bytes,
        len(target_bytes),
    )
    kernel32: Any = ctypes.WinDLL("kernel32", use_last_error=True)
    set_information: Any = kernel32.SetFileInformationByHandle
    set_information.argtypes = [
        wintypes.HANDLE,
        ctypes.c_int,
        ctypes.c_void_p,
        wintypes.DWORD,
    ]
    set_information.restype = wintypes.BOOL
    file_rename_info = 3
    if not set_information(
        msvcrt.get_osfhandle(source_descriptor),
        file_rename_info,
        buffer,
        buffer_size,
    ):
        error = ctypes.get_last_error()
        raise OSError(error, ctypes.FormatError(error), str(target))


def _mkdir_checked(root: Path, path: Path, field: str) -> _FileIdentity:
    with _bound_directory(root, path.parent, field) as parent_binding:
        name = _bound_leaf(parent_binding, path, field)
        try:
            if os.name == "nt":
                path.mkdir(exist_ok=False)
            else:
                assert parent_binding.descriptor is not None
                os.mkdir(name, mode=0o700, dir_fd=parent_binding.descriptor)
        except OSError as exc:
            raise DataPublicationError("ARTIFACT_PATH_CREATE_FAILED", field, path=path) from exc
        with _bound_directory(root, path, field) as created:
            return created.identity


def _cleanup_stage(
    root: Path,
    stage: Path,
    expected_identity: _FileIdentity,
) -> str | None:
    """Conservatively remove only the publication's known files and directory."""

    expected_names = {
        "payload.bin",
        "source_event.json",
        "manifest.json",
        "quality_report.json",
        "pointer.json",
    }
    try:
        with _bound_directory(root, stage, "publication stage cleanup") as stage_binding:
            if stage_binding.identity != expected_identity:
                _fail("ARTIFACT_COMPONENT_REPLACED", str(stage))
            assert stage_binding.descriptor is not None
            names = (
                tuple(os.listdir(stage))
                if os.name == "nt"
                else tuple(os.listdir(stage_binding.descriptor))
            )
            for name in names:
                child = stage / name
                is_known = name in expected_names or any(
                    name.startswith(f".{expected_name}.") and name.endswith(".tmp")
                    for expected_name in expected_names
                )
                if not is_known:
                    return f"PUBLICATION_STAGE_CLEANUP_SKIPPED: unexpected entry {name}"
                child_metadata = _bound_path_metadata(
                    stage_binding,
                    child,
                    "publication stage cleanup",
                )
                _unlink_bound_checked(
                    stage_binding,
                    child,
                    expected_identity=_file_identity(child_metadata),
                    expected_nlink=1,
                )
            remaining = (
                tuple(os.listdir(stage))
                if os.name == "nt"
                else tuple(os.listdir(stage_binding.descriptor))
            )
            if remaining:
                return "PUBLICATION_STAGE_CLEANUP_SKIPPED: stage is not empty"
        with _bound_directory(root, stage.parent, "publication stage cleanup") as parent_binding:
            name = _bound_leaf(parent_binding, stage, "publication stage cleanup")
            if os.name == "nt":
                metadata = stage.lstat()
            else:
                assert parent_binding.descriptor is not None
                metadata = os.stat(
                    name,
                    dir_fd=parent_binding.descriptor,
                    follow_symlinks=False,
                )
            _validate_directory_metadata(metadata, stage, "publication stage cleanup")
            if _file_identity(metadata) != expected_identity:
                _fail("ARTIFACT_COMPONENT_REPLACED", str(stage))
            if os.name == "nt":
                stage.rmdir()
            else:
                os.rmdir(name, dir_fd=parent_binding.descriptor)
    except (OSError, DataPublicationError) as exc:
        return f"PUBLICATION_STAGE_CLEANUP_FAILED: {exc}"
    return None


def _store_path(
    root: Path,
    pointer: ArtifactPointer,
    *,
    create_parents: bool = False,
) -> Path:
    return _internal_path(
        root,
        pointer.path,
        "store pointer",
        create_parents=create_parents,
    )


def _evidence_path(root: Path, value: str) -> Path:
    return _internal_path(root, value, "DQ evidence")


def _portable_parts(value: str, field: str) -> tuple[str, ...]:
    if not isinstance(value, str) or not value or "\\" in value or ":" in value:
        _fail("ARTIFACT_PATH_INVALID", f"{field}: {value!r}")
    pure = PurePosixPath(value)
    if pure.is_absolute() or not pure.parts or ".." in pure.parts or pure.as_posix() != value:
        _fail("ARTIFACT_PATH_INVALID", f"{field}: {value!r}")
    return pure.parts


def _internal_path(
    root: Path,
    value: str,
    field: str,
    *,
    create_parents: bool = False,
) -> Path:
    parts = _portable_parts(value, field)
    parent = root.joinpath(*parts[:-1])
    if create_parents:
        with _bound_directory(root, parent, field, create=True):
            pass
    return root.joinpath(*parts)


def _history_path(
    root: Path,
    dataset: str,
    pointer_id: str,
    *,
    create_parents: bool = False,
) -> Path:
    _identifier(dataset, "dataset_id")
    _identifier(pointer_id, "pointer_id")
    return _internal_path(
        root,
        f"pointer_history/{dataset}/{pointer_id}.json",
        "pointer history",
        create_parents=create_parents,
    )


def _directory(path: Path, *, create: bool, code: str) -> Path:
    try:
        if create:
            path.mkdir(parents=True, exist_ok=True)
        resolved = path.resolve(strict=True)
    except OSError as exc:
        raise DataPublicationError(code, str(exc), path=path) from exc
    if not resolved.is_dir():
        raise DataPublicationError(code, "not a directory", path=resolved)
    return resolved


def _json_mapping(value: Mapping[str, object]) -> dict[str, object]:
    if not isinstance(value, Mapping) or any(not isinstance(key, str) or not key for key in value):
        _fail("REQUEST_PARAMETERS_INVALID", "must be object with non-empty string keys")
    try:
        encoded = json.dumps(
            dict(value),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
        result = json.loads(encoded)
    except (TypeError, ValueError) as exc:
        raise DataPublicationIntegrityError("REQUEST_PARAMETERS_INVALID", str(exc)) from exc
    return cast(dict[str, object], result)


def _digest(value: object) -> str:
    try:
        raw = json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode()
    except (TypeError, ValueError) as exc:
        raise DataPublicationIntegrityError("NONCANONICAL_JSON_VALUE", str(exc)) from exc
    return sha256_bytes(raw)


def _mapping(value: object, field: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        _fail("INVALID_MANIFEST_MAPPING", field)
    return cast(Mapping[str, object], value)


def _text(payload: Mapping[str, object], field: str) -> str:
    value = payload.get(field)
    if not isinstance(value, str) or not value.strip():
        _fail("REQUIRED_FIELD_INVALID", field)
    return value


def _integer(payload: Mapping[str, object], field: str, minimum: int) -> int:
    value = payload.get(field)
    if not isinstance(value, int) or isinstance(value, bool) or value < minimum:
        _fail("REQUIRED_INTEGER_INVALID", field)
    return value


def _sha(value: object, field: str) -> str:
    if not isinstance(value, str) or not _SHA_RE.fullmatch(value):
        _fail("SHA256_FIELD_INVALID", field)
    return value


def _aware(value: object, field: str) -> datetime:
    try:
        result = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError as exc:
        raise DataPublicationIntegrityError("DATETIME_FIELD_INVALID", field) from exc
    if result.tzinfo is None or result.utcoffset() is None:
        _fail("TIMEZONE_REQUIRED", field)
    return result


def _date(value: object, field: str) -> date:
    try:
        return date.fromisoformat(str(value))
    except ValueError as exc:
        raise DataPublicationIntegrityError("DATE_FIELD_INVALID", field) from exc


def _identifier(value: str, field: str) -> None:
    if not isinstance(value, str) or not _ID_RE.fullmatch(value):
        _fail("IDENTIFIER_INVALID", f"{field}={value!r}")


def _fail(code: str, message: str) -> NoReturn:
    raise DataPublicationIntegrityError(code, message)


@contextmanager
def _file_lock(
    path: Path,
    *,
    root: Path,
    timeout_seconds: float,
    poll_seconds: float,
) -> Iterator[_LockLease]:
    stack = ExitStack()
    try:
        binding = stack.enter_context(_bound_directory(root, path.parent, "dataset lock parent"))
        descriptor = _checked_open_regular(
            path,
            os.O_RDWR | os.O_CREAT,
            "dataset lock",
            binding=binding,
        )
        handle = os.fdopen(descriptor, "r+b", buffering=0)
    except (OSError, DataPublicationError) as exc:
        stack.close()
        raise DataPublicationError("DATASET_LOCK_FAILED", str(exc), path=path) from exc
    acquired = False
    lease = _LockLease()
    try:
        deadline = time.monotonic() + timeout_seconds
        while not acquired:
            try:
                _try_lock(handle)
                acquired = True
            except OSError as exc:
                if exc.errno not in _LOCK_ERRNOS and not isinstance(exc, PermissionError):
                    raise DataPublicationError("DATASET_LOCK_FAILED", str(exc), path=path) from exc
                if time.monotonic() >= deadline:
                    raise DataPublicationLockTimeout(
                        "DATASET_LOCK_TIMEOUT", "unable to acquire dataset lock", path=path
                    ) from exc
                time.sleep(poll_seconds)
        _assert_descriptor_path(handle.fileno(), path, "dataset lock", binding)
        # Initialize only after acquiring the OS lock.  On Windows, publishing a
        # first byte before locking lets a waiter lock byte 0 while the creator
        # is still flushing it (the OPS-066 first-create race).
        handle.seek(0, os.SEEK_END)
        if handle.tell() == 0:
            _assert_descriptor_path(handle.fileno(), path, "dataset lock", binding)
            handle.write(b"\0")
            handle.flush()
            os.fsync(handle.fileno())
            _assert_descriptor_path(handle.fileno(), path, "dataset lock", binding)
        yield lease
    finally:
        active_error = sys.exc_info()[0] is not None
        cleanup_errors: list[str] = []
        if acquired:
            try:
                _unlock(handle)
            except OSError as exc:
                cleanup_errors.append(f"DATASET_UNLOCK_FAILED: {exc}")
        try:
            handle.close()
        except OSError as exc:
            cleanup_errors.append(f"DATASET_LOCK_CLOSE_FAILED: {exc}")
        stack.close()
        if cleanup_errors:
            if lease.committed:
                lease.cleanup_warnings.extend(cleanup_errors)
            elif not active_error:
                raise DataPublicationError(
                    "DATASET_LOCK_CLEANUP_FAILED",
                    "; ".join(cleanup_errors),
                    path=path,
                )


def _try_lock(handle: Any) -> None:
    handle.seek(0)
    if os.name == "nt":
        import msvcrt

        msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
    else:
        fcntl: Any = importlib.import_module("fcntl")
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)


def _unlock(handle: Any) -> None:
    handle.seek(0)
    if os.name == "nt":
        import msvcrt

        msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
    else:
        fcntl: Any = importlib.import_module("fcntl")
        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
