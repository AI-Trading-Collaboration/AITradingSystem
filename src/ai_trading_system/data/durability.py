from __future__ import annotations

import ctypes
import json
import os
import platform
import re
import stat
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path, PurePosixPath
from typing import Any, NoReturn

import yaml

from ai_trading_system.data.immutable_publish import (
    COMMIT_CHECKPOINTS,
    PUBLICATION_DURABILITY_PROTOCOL_VERSION,
    DataPublicationError,
    exclusive_store_maintenance,
    read_contained_artifact_bytes,
    validate_current_snapshot,
    write_contained_artifact_bytes,
)
from ai_trading_system.data.immutable_publish import (
    delete_contained_artifact_bytes as delete_contained_artifact_bytes,
)
from ai_trading_system.platform.artifacts import canonical_json_bytes, sha256_bytes

DURABILITY_POLICY_SCHEMA_VERSION = "data_foundation_durability_policy.v1"
FILESYSTEM_PROFILE_SCHEMA_VERSION = "data_filesystem_durability_profile.v1"
CRASH_REHEARSAL_SCHEMA_VERSION = "data_publication_crash_rehearsal.v1"
GC_PLAN_SCHEMA_VERSION = "data_store_gc_plan.v1"
GC_RECEIPT_SCHEMA_VERSION = "data_store_gc_receipt.v1"
BACKUP_MANIFEST_SCHEMA_VERSION = "data_checksum_backup_manifest.v1"
RESTORE_RECEIPT_SCHEMA_VERSION = "data_checksum_restore_receipt.v1"
DURABILITY_ATTESTATION_SCHEMA_VERSION = "data_publication_durability_attestation.v1"

_SHA_RE = re.compile(r"^[0-9a-f]{64}$")
_MANAGED_STORE_ROOTS = (
    "manifests",
    "pointer_history",
    "quality_reports",
    "snapshots",
    "source_events",
    "staging",
)
_NETWORK_FILESYSTEMS = {
    "9p",
    "afs",
    "ceph",
    "cifs",
    "fuse.sshfs",
    "glusterfs",
    "nfs",
    "nfs4",
    "smb",
    "smb2",
}
_WINDOWS_DRIVE_FIXED = 3


class DataDurabilityError(RuntimeError):
    def __init__(self, code: str, message: str, *, path: Path | None = None) -> None:
        self.code = code
        self.message = message
        self.path = path
        location = "" if path is None else f" [{path}]"
        super().__init__(f"{code}{location}: {message}")


class DataGcApplyError(DataDurabilityError):
    def __init__(
        self,
        code: str,
        message: str,
        *,
        receipt: Mapping[str, object],
    ) -> None:
        self.receipt = dict(receipt)
        super().__init__(code, message)


@dataclass(frozen=True)
class DurabilityPolicy:
    policy_id: str
    policy_version: str
    orphan_grace_seconds: int
    managed_store_roots: tuple[str, ...]
    backup_categories: tuple[str, ...]
    required_crash_checkpoints: tuple[str, ...]
    production_effect: str
    consumer_cutover_allowed: bool
    store_acl_verified: bool


@dataclass(frozen=True)
class FilesystemDurabilityProfile:
    schema_version: str
    system: str
    filesystem: str
    storage_scope: str
    protocol_version: str
    namespace_durability_method: str
    supported: bool
    limitations: tuple[str, ...]

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "system": self.system,
            "filesystem": self.filesystem,
            "storage_scope": self.storage_scope,
            "protocol_version": self.protocol_version,
            "namespace_durability_method": self.namespace_durability_method,
            "supported": self.supported,
            "limitations": list(self.limitations),
        }


@dataclass(frozen=True)
class StoreObject:
    relative_path: str
    sha256: str
    size_bytes: int
    modified_at_ns: int

    def to_dict(self) -> dict[str, object]:
        return {
            "relative_path": self.relative_path,
            "sha256": self.sha256,
            "size_bytes": self.size_bytes,
            "modified_at_ns": self.modified_at_ns,
        }


@dataclass(frozen=True)
class StoreGcPlan:
    plan_id: str
    generated_at: datetime
    policy_id: str
    policy_version: str
    store_identity: str
    store_state_sha256: str
    external_references: tuple[str, ...]
    protected_objects: tuple[tuple[str, str], ...]
    deletion_candidates: tuple[StoreObject, ...]
    production_effect: str = "none"

    def to_dict(self) -> dict[str, object]:
        return {
            "plan_id": self.plan_id,
            "schema_version": GC_PLAN_SCHEMA_VERSION,
            "generated_at": _utc_text(self.generated_at),
            "policy_id": self.policy_id,
            "policy_version": self.policy_version,
            "store_identity": self.store_identity,
            "store_state_sha256": self.store_state_sha256,
            "external_references": list(self.external_references),
            "protected_objects": [
                {"relative_path": path, "reason": reason} for path, reason in self.protected_objects
            ],
            "deletion_candidates": [item.to_dict() for item in self.deletion_candidates],
            "production_effect": self.production_effect,
        }


@dataclass(frozen=True)
class BackupSource:
    source_identity: str
    category: str
    root: Path
    relative_path: str
    restore_path: str


def load_durability_policy(path: Path) -> DurabilityPolicy:
    try:
        raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, yaml.YAMLError) as exc:
        raise DataDurabilityError("DURABILITY_POLICY_INVALID", str(exc), path=path) from exc
    payload = _mapping(raw, "policy")
    _exact_fields(
        payload,
        {
            "schema_version",
            "policy_id",
            "policy_version",
            "orphan_grace_seconds",
            "managed_store_roots",
            "backup_categories",
            "required_crash_checkpoints",
            "claim_boundary",
        },
        "DURABILITY_POLICY_INVALID",
    )
    if payload.get("schema_version") != DURABILITY_POLICY_SCHEMA_VERSION:
        _fail("DURABILITY_POLICY_INVALID", "unsupported schema_version")
    grace = _integer(payload.get("orphan_grace_seconds"), "orphan_grace_seconds")
    if grace < 1:
        _fail("DURABILITY_POLICY_INVALID", "orphan_grace_seconds must be positive")
    managed = _strings(payload.get("managed_store_roots"), "managed_store_roots")
    categories = _strings(payload.get("backup_categories"), "backup_categories")
    checkpoints = tuple(
        _text(item, "required_crash_checkpoints[]")
        for item in _sequence(
            payload.get("required_crash_checkpoints"),
            "required_crash_checkpoints",
        )
    )
    if managed != tuple(sorted(_MANAGED_STORE_ROOTS)):
        _fail("DURABILITY_POLICY_INVALID", "managed_store_roots drift")
    if set(categories) != {"critical_config", "forward_only", "manual_input"}:
        _fail("DURABILITY_POLICY_INVALID", "backup_categories drift")
    if checkpoints != COMMIT_CHECKPOINTS:
        _fail("DURABILITY_POLICY_INVALID", "crash checkpoint drift")
    boundary = _mapping(payload.get("claim_boundary"), "claim_boundary")
    _exact_fields(
        boundary,
        {
            "production_effect",
            "consumer_cutover_allowed",
            "store_acl_verified",
        },
        "DURABILITY_POLICY_INVALID",
    )
    if (
        boundary.get("production_effect") != "none"
        or boundary.get("consumer_cutover_allowed") is not False
        or boundary.get("store_acl_verified") is not False
    ):
        _fail("DURABILITY_POLICY_INVALID", "claim boundary widened")
    return DurabilityPolicy(
        policy_id=_text(payload.get("policy_id"), "policy_id"),
        policy_version=_text(payload.get("policy_version"), "policy_version"),
        orphan_grace_seconds=grace,
        managed_store_roots=managed,
        backup_categories=categories,
        required_crash_checkpoints=checkpoints,
        production_effect="none",
        consumer_cutover_allowed=False,
        store_acl_verified=False,
    )


def probe_filesystem_durability(store_root: Path) -> FilesystemDurabilityProfile:
    root = _resolved_directory(store_root, "DURABILITY_STORE_ROOT_INVALID")
    system = platform.system()
    if os.name == "nt":
        filesystem, drive_type = _windows_filesystem(root)
        supported = filesystem.upper() == "NTFS" and drive_type == _WINDOWS_DRIVE_FIXED
        limitations = (
            "local_ntfs_only",
            "hardware_controller_write_cache_requires_deployment_review",
            "acl_and_same_principal_mutation_not_verified",
        )
        return FilesystemDurabilityProfile(
            schema_version=FILESYSTEM_PROFILE_SCHEMA_VERSION,
            system=system,
            filesystem=filesystem,
            storage_scope="LOCAL_FIXED_VOLUME" if drive_type == 3 else f"DRIVE_TYPE_{drive_type}",
            protocol_version=PUBLICATION_DURABILITY_PROTOCOL_VERSION,
            namespace_durability_method=(
                "FILE_FLAG_WRITE_THROUGH_HANDLE_RENAME_AND_POST_RENAME_FLUSH"
            ),
            supported=supported,
            limitations=limitations,
        )
    filesystem, mount_scope = _posix_filesystem(root)
    supported = filesystem not in _NETWORK_FILESYSTEMS and filesystem != "UNKNOWN"
    return FilesystemDurabilityProfile(
        schema_version=FILESYSTEM_PROFILE_SCHEMA_VERSION,
        system=system,
        filesystem=filesystem,
        storage_scope=mount_scope,
        protocol_version=PUBLICATION_DURABILITY_PROTOCOL_VERSION,
        namespace_durability_method="FILE_FSYNC_ATOMIC_RENAME_PARENT_DIRECTORY_FSYNC",
        supported=supported,
        limitations=(
            "local_filesystem_mount_required",
            "hardware_controller_write_cache_requires_deployment_review",
            "acl_and_same_principal_mutation_not_verified",
        ),
    )


def build_crash_rehearsal_receipt(
    *,
    generated_at: datetime,
    profile: FilesystemDurabilityProfile,
    cases: Sequence[Mapping[str, object]],
) -> dict[str, object]:
    if not profile.supported:
        _fail("DURABILITY_PROFILE_UNSUPPORTED", profile.filesystem)
    normalized: list[dict[str, object]] = []
    seen: set[str] = set()
    for raw in cases:
        case = _mapping(raw, "crash case")
        _exact_fields(
            case,
            {
                "case_id",
                "checkpoint",
                "exit_mode",
                "recovered_generation",
                "expected_generations",
                "validator_status",
                "lock_reacquired",
                "torn_state_observed",
            },
            "CRASH_REHEARSAL_CASE_INVALID",
        )
        case_id = _text(case.get("case_id"), "case_id")
        if case_id in seen:
            _fail("CRASH_REHEARSAL_CASE_INVALID", f"duplicate {case_id}")
        seen.add(case_id)
        checkpoint = _text(case.get("checkpoint"), "checkpoint")
        if checkpoint not in COMMIT_CHECKPOINTS:
            _fail("CRASH_REHEARSAL_CASE_INVALID", checkpoint)
        expected = tuple(
            _integer(item, "expected_generations[]")
            for item in _sequence(case.get("expected_generations"), "expected_generations")
        )
        recovered = _integer(case.get("recovered_generation"), "recovered_generation")
        if (
            case.get("exit_mode") != "FORCED_PROCESS_EXIT"
            or case.get("validator_status") != "PASS"
            or case.get("lock_reacquired") is not True
            or case.get("torn_state_observed") is not False
            or recovered not in expected
        ):
            _fail("CRASH_REHEARSAL_CASE_FAILED", case_id)
        if checkpoint in {
            "NAMESPACE_DURABLE_BEFORE_ATTEST",
            "ATTESTED_BEFORE_ACK",
        } and expected != (2,):
            _fail("CRASH_REHEARSAL_CASE_INVALID", f"{case_id}: new generation required")
        normalized.append(
            {
                "case_id": case_id,
                "checkpoint": checkpoint,
                "exit_mode": "FORCED_PROCESS_EXIT",
                "recovered_generation": recovered,
                "expected_generations": list(expected),
                "validator_status": "PASS",
                "lock_reacquired": True,
                "torn_state_observed": False,
            }
        )
    normalized.sort(key=lambda item: str(item["case_id"]))
    covered = {str(item["checkpoint"]) for item in normalized}
    if covered != set(COMMIT_CHECKPOINTS):
        _fail("CRASH_REHEARSAL_INCOMPLETE", ",".join(sorted(covered)))
    body: dict[str, object] = {
        "schema_version": CRASH_REHEARSAL_SCHEMA_VERSION,
        "generated_at": _utc_text(generated_at),
        "status": "PASS",
        "profile": profile.to_dict(),
        "checkpoint_coverage": list(COMMIT_CHECKPOINTS),
        "cases": normalized,
        "process_crash_rehearsal_verified": True,
        "old_or_new_never_torn_verified": True,
        "lock_recovery_verified": True,
        "production_effect": "none",
    }
    return {"rehearsal_id": _semantic_id("crash_rehearsal_", body), **body}


def plan_reference_safe_gc(
    *,
    store_root: Path,
    policy: DurabilityPolicy,
    generated_at: datetime,
    external_references: Sequence[str] = (),
) -> StoreGcPlan:
    root = _resolved_directory(store_root, "DURABILITY_STORE_ROOT_INVALID")
    instant = _aware_utc(generated_at)
    external = tuple(
        sorted({_portable_path(item, "external reference") for item in external_references})
    )
    inventory = _store_inventory(root, policy.managed_store_roots)
    by_path = {item.relative_path: item for item in inventory}
    protected: dict[str, str] = {item: "EXTERNAL_RUN_OR_LINEAGE_REFERENCE" for item in external}
    for external_reference in external:
        if external_reference not in by_path:
            _fail("GC_EXTERNAL_REFERENCE_MISSING", external_reference)
    history = _history_payloads(root, inventory)
    reachable_history = _reachable_pointer_history(root, inventory, history, protected)
    cutoff_ns = int(instant.timestamp() * 1_000_000_000) - (
        policy.orphan_grace_seconds * 1_000_000_000
    )
    for history_path, payload in history.items():
        if history_path in reachable_history:
            continue
        references = _pointer_references(root, payload)
        published_at = _parse_datetime(payload.get("published_at"), "published_at")
        retention_until = _pointer_retention_until(root, payload)
        if published_at.timestamp() * 1_000_000_000 > cutoff_ns:
            reason = "ORPHAN_GRACE_PERIOD_ACTIVE"
        elif retention_until is not None and retention_until >= instant.date():
            reason = "RETENTION_UNTIL_ACTIVE"
        else:
            continue
        protected[history_path] = reason
        for relative_path in references:
            protected.setdefault(relative_path, reason)
    candidates = tuple(
        item
        for item in inventory
        if item.relative_path not in protected and item.modified_at_ns <= cutoff_ns
    )
    for store_object in inventory:
        if store_object.relative_path not in protected and store_object not in candidates:
            protected[store_object.relative_path] = "ORPHAN_GRACE_PERIOD_ACTIVE"
    state_payload = {
        "store_identity": str(root),
        "objects": [item.to_dict() for item in inventory],
        "current_pointers": _current_pointer_state(root),
        "external_references": list(external),
    }
    state_sha = sha256_bytes(canonical_json_bytes(state_payload))
    body: dict[str, object] = {
        "schema_version": GC_PLAN_SCHEMA_VERSION,
        "generated_at": _utc_text(instant),
        "policy_id": policy.policy_id,
        "policy_version": policy.policy_version,
        "store_identity": str(root),
        "store_state_sha256": state_sha,
        "external_references": list(external),
        "protected_objects": [
            {"relative_path": path, "reason": reason} for path, reason in sorted(protected.items())
        ],
        "deletion_candidates": [item.to_dict() for item in candidates],
        "production_effect": "none",
    }
    return StoreGcPlan(
        plan_id=_semantic_id("store_gc_plan_", body),
        generated_at=instant,
        policy_id=policy.policy_id,
        policy_version=policy.policy_version,
        store_identity=str(root),
        store_state_sha256=state_sha,
        external_references=external,
        protected_objects=tuple(sorted(protected.items())),
        deletion_candidates=candidates,
    )


def apply_reference_safe_gc(
    *,
    store_root: Path,
    policy: DurabilityPolicy,
    plan: StoreGcPlan,
) -> dict[str, object]:
    profile = probe_filesystem_durability(store_root)
    if not profile.supported:
        _fail("DURABILITY_PROFILE_UNSUPPORTED", profile.filesystem)
    deleted: list[dict[str, object]] = []
    failed: dict[str, object] | None = None
    with exclusive_store_maintenance(store_root=store_root) as session:
        refreshed = plan_reference_safe_gc(
            store_root=session.root,
            policy=policy,
            generated_at=plan.generated_at,
            external_references=plan.external_references,
        )
        if refreshed.to_dict() != plan.to_dict():
            _fail("GC_PLAN_STALE", plan.plan_id)
        for item in plan.deletion_candidates:
            try:
                result = delete_contained_artifact_bytes(
                    root=session.root,
                    relative_path=item.relative_path,
                    expected_sha256=item.sha256,
                    expected_size_bytes=item.size_bytes,
                )
            except Exception as exc:
                failed = {
                    "relative_path": item.relative_path,
                    "expected_sha256": item.sha256,
                    "expected_size_bytes": item.size_bytes,
                    "error_type": type(exc).__name__,
                    "error_code": getattr(exc, "code", None),
                    "message": str(exc),
                }
                break
            deleted.append(
                {
                    "relative_path": item.relative_path,
                    "sha256": result.sha256,
                    "size_bytes": result.size_bytes,
                    "device": result.device,
                    "inode": result.inode,
                    "reason": "UNREFERENCED_RETENTION_EXPIRED",
                    "post_delete_absent": True,
                }
            )
            session.mark_committed()
    cleanup = [item.to_dict() for item in session.cleanup_observations]
    if failed is not None:
        failure_status = "FAIL_PARTIAL" if deleted else "FAIL_NO_DELETE"
        failure_body = _gc_receipt_body(
            status=failure_status,
            plan=plan,
            profile=profile,
            deleted=deleted,
            protected_count=len(plan.protected_objects),
            cleanup=cleanup,
            failed_object=failed,
        )
        failure_receipt = {
            "gc_receipt_id": _semantic_id("store_gc_receipt_", failure_body),
            **failure_body,
        }
        raise DataGcApplyError(
            "GC_APPLY_PARTIAL" if deleted else "GC_APPLY_FAILED",
            str(failed["relative_path"]),
            receipt=failure_receipt,
        )
    body = _gc_receipt_body(
        status="PASS",
        plan=plan,
        profile=profile,
        deleted=deleted,
        protected_count=len(plan.protected_objects),
        cleanup=cleanup,
        failed_object=None,
    )
    return {"gc_receipt_id": _semantic_id("store_gc_receipt_", body), **body}


def _gc_receipt_body(
    *,
    status: str,
    plan: StoreGcPlan,
    profile: FilesystemDurabilityProfile,
    deleted: Sequence[Mapping[str, object]],
    protected_count: int,
    cleanup: Sequence[Mapping[str, object]],
    failed_object: Mapping[str, object] | None,
) -> dict[str, object]:
    return {
        "schema_version": GC_RECEIPT_SCHEMA_VERSION,
        "status": status,
        "plan_id": plan.plan_id,
        "plan_sha256": sha256_bytes(canonical_json_bytes(plan.to_dict())),
        "profile": profile.to_dict(),
        "deleted_objects": [dict(item) for item in deleted],
        "deleted_count": len(deleted),
        "protected_count": protected_count,
        "failed_object": None if failed_object is None else dict(failed_object),
        "cleanup_observations": [dict(item) for item in cleanup],
        "production_effect": "none",
    }


def create_checksum_backup(
    *,
    backup_root: Path,
    sources: Sequence[BackupSource],
    policy: DurabilityPolicy,
    captured_at: datetime,
) -> dict[str, object]:
    root = _resolved_directory(backup_root, "BACKUP_ROOT_INVALID")
    instant = _aware_utc(captured_at)
    normalized: list[dict[str, object]] = []
    seen: set[str] = set()
    for source in sources:
        if source.category not in policy.backup_categories:
            _fail("BACKUP_CATEGORY_INVALID", source.category)
        source_identity = _text(source.source_identity, "source_identity")
        source_relative = _portable_path(source.relative_path, "source relative_path")
        restore_path = _portable_path(source.restore_path, "restore_path")
        if restore_path in seen:
            _fail("BACKUP_RESTORE_PATH_DUPLICATE", restore_path)
        seen.add(restore_path)
        content = read_contained_artifact_bytes(
            root=source.root,
            relative_path=source_relative,
        )
        digest = sha256_bytes(content)
        object_path = f"objects/{digest[:2]}/{digest}.bin"
        write_contained_artifact_bytes(
            root=root,
            relative_path=object_path,
            content=content,
            immutable=True,
        )
        normalized.append(
            {
                "source_identity": source_identity,
                "category": source.category,
                "source_relative_path": source_relative,
                "restore_path": restore_path,
                "object_path": object_path,
                "sha256": digest,
                "size_bytes": len(content),
            }
        )
    normalized.sort(key=lambda item: str(item["restore_path"]))
    body: dict[str, object] = {
        "schema_version": BACKUP_MANIFEST_SCHEMA_VERSION,
        "captured_at": _utc_text(instant),
        "policy_id": policy.policy_id,
        "policy_version": policy.policy_version,
        "status": "PASS",
        "objects": normalized,
        "object_count": len(normalized),
        "production_effect": "none",
    }
    manifest = {"backup_id": _semantic_id("checksum_backup_", body), **body}
    manifest_bytes = canonical_json_bytes(manifest)
    manifest_path = f"manifests/{manifest['backup_id']}.json"
    write_contained_artifact_bytes(
        root=root,
        relative_path=manifest_path,
        content=manifest_bytes,
        immutable=True,
    )
    return {
        **manifest,
        "manifest_path": manifest_path,
        "manifest_sha256": sha256_bytes(manifest_bytes),
    }


def restore_checksum_backup(
    *,
    backup_root: Path,
    manifest_path: str,
    destination_root: Path,
    semantic_validators: Sequence[Callable[[Path], None]] = (),
) -> dict[str, object]:
    backup = _resolved_directory(backup_root, "BACKUP_ROOT_INVALID")
    destination = _resolved_directory(destination_root, "RESTORE_DESTINATION_INVALID")
    if any(destination.iterdir()):
        _fail("RESTORE_DESTINATION_NOT_EMPTY", str(destination))
    manifest_relative = _portable_path(manifest_path, "manifest_path")
    raw = read_contained_artifact_bytes(root=backup, relative_path=manifest_relative)
    manifest = _canonical_mapping(raw, "BACKUP_MANIFEST_INVALID")
    _validate_backup_manifest(manifest)
    restored: list[dict[str, object]] = []
    for raw_object in _sequence(manifest.get("objects"), "objects"):
        item = _mapping(raw_object, "backup object")
        content = read_contained_artifact_bytes(
            root=backup,
            relative_path=_portable_path(item.get("object_path"), "object_path"),
        )
        expected_sha = _digest(item.get("sha256"), "sha256")
        expected_size = _integer(item.get("size_bytes"), "size_bytes")
        if sha256_bytes(content) != expected_sha or len(content) != expected_size:
            _fail("BACKUP_OBJECT_BINDING_MISMATCH", str(item.get("restore_path")))
        restore_path = _portable_path(item.get("restore_path"), "restore_path")
        write_contained_artifact_bytes(
            root=destination,
            relative_path=restore_path,
            content=content,
            immutable=True,
        )
        verified = read_contained_artifact_bytes(
            root=destination,
            relative_path=restore_path,
        )
        if verified != content:
            _fail("RESTORE_CHECKSUM_MISMATCH", restore_path)
        restored.append(
            {
                "restore_path": restore_path,
                "sha256": expected_sha,
                "size_bytes": expected_size,
                "checksum_verified": True,
            }
        )
    for validator in semantic_validators:
        validator(destination)
    manifest_sha = sha256_bytes(raw)
    body: dict[str, object] = {
        "schema_version": RESTORE_RECEIPT_SCHEMA_VERSION,
        "status": "PASS",
        "backup_id": manifest.get("backup_id"),
        "backup_manifest_path": manifest_relative,
        "backup_manifest_sha256": manifest_sha,
        "destination_identity": str(destination),
        "restored_objects": restored,
        "restored_count": len(restored),
        "checksum_verified": True,
        "semantic_validators_passed": len(semantic_validators),
        "live_source_overwrite_allowed": False,
        "production_effect": "none",
    }
    return {"restore_receipt_id": _semantic_id("checksum_restore_", body), **body}


def build_durability_attestation(
    *,
    store_root: Path,
    evidence_root: Path,
    dataset_id: str,
    generated_at: datetime,
    policy: DurabilityPolicy,
    profile: FilesystemDurabilityProfile,
    crash_receipt: Mapping[str, object],
    gc_receipt: Mapping[str, object],
    restore_receipt: Mapping[str, object],
) -> dict[str, object]:
    if not profile.supported:
        _fail("DURABILITY_PROFILE_UNSUPPORTED", profile.filesystem)
    _require_pass_receipt(
        crash_receipt,
        CRASH_REHEARSAL_SCHEMA_VERSION,
        "rehearsal_id",
    )
    _require_pass_receipt(gc_receipt, GC_RECEIPT_SCHEMA_VERSION, "gc_receipt_id")
    _require_pass_receipt(
        restore_receipt,
        RESTORE_RECEIPT_SCHEMA_VERSION,
        "restore_receipt_id",
    )
    snapshot = validate_current_snapshot(
        store_root=store_root,
        evidence_root=evidence_root,
        dataset_id=dataset_id,
    )
    try:
        manifest_relative_path = snapshot.manifest_path.relative_to(
            store_root.resolve(strict=True)
        ).as_posix()
        manifest_payload = json.loads(
            read_contained_artifact_bytes(
                root=store_root,
                relative_path=manifest_relative_path,
            ).decode("utf-8")
        )
    except (
        DataPublicationError,
        OSError,
        UnicodeDecodeError,
        ValueError,
        json.JSONDecodeError,
    ) as exc:
        raise DataDurabilityError(
            "D0A_MANIFEST_UNREADABLE",
            str(exc),
            path=snapshot.manifest_path,
        ) from exc
    if (
        not isinstance(manifest_payload, dict)
        or manifest_payload.get("crash_durability_verified") is not False
        or manifest_payload.get("store_acl_verified") is not False
        or manifest_payload.get("consumer_cutover_allowed") is not False
    ):
        _fail("D0A_GOVERNANCE_BOUNDARY_DRIFT", dataset_id)
    evidence = {
        "crash_rehearsal": _receipt_binding(crash_receipt, "rehearsal_id"),
        "gc_receipt": _receipt_binding(gc_receipt, "gc_receipt_id"),
        "restore_receipt": _receipt_binding(restore_receipt, "restore_receipt_id"),
    }
    body: dict[str, object] = {
        "schema_version": DURABILITY_ATTESTATION_SCHEMA_VERSION,
        "generated_at": _utc_text(generated_at),
        "status": "PASS",
        "policy_id": policy.policy_id,
        "policy_version": policy.policy_version,
        "profile": profile.to_dict(),
        "publication_binding": {
            "dataset_id": snapshot.dataset_id,
            "generation": snapshot.generation,
            "pointer_id": snapshot.pointer_id,
            "pointer_sha256": snapshot.pointer_sha256,
            "manifest_id": snapshot.manifest_id,
            "snapshot_id": snapshot.snapshot_id,
        },
        "evidence": evidence,
        "d0a_manifest_crash_durability_verified": False,
        "scoped_crash_durability_verified": True,
        "store_acl_verified": False,
        "consumer_cutover_allowed": False,
        "production_effect": "none",
    }
    attestation = {
        "attestation_id": _semantic_id("durability_attestation_", body),
        **body,
    }
    validate_durability_attestation(attestation)
    return attestation


def validate_durability_attestation(value: Mapping[str, object]) -> None:
    payload = dict(value)
    _exact_fields(
        payload,
        {
            "attestation_id",
            "schema_version",
            "generated_at",
            "status",
            "policy_id",
            "policy_version",
            "profile",
            "publication_binding",
            "evidence",
            "d0a_manifest_crash_durability_verified",
            "scoped_crash_durability_verified",
            "store_acl_verified",
            "consumer_cutover_allowed",
            "production_effect",
        },
        "DURABILITY_ATTESTATION_INVALID",
    )
    if (
        payload.get("schema_version") != DURABILITY_ATTESTATION_SCHEMA_VERSION
        or payload.get("status") != "PASS"
        or payload.get("d0a_manifest_crash_durability_verified") is not False
        or payload.get("scoped_crash_durability_verified") is not True
        or payload.get("store_acl_verified") is not False
        or payload.get("consumer_cutover_allowed") is not False
        or payload.get("production_effect") != "none"
    ):
        _fail("DURABILITY_ATTESTATION_INVALID", "claim boundary invalid")
    supplied = payload.pop("attestation_id")
    expected = _semantic_id("durability_attestation_", payload)
    if supplied != expected:
        _fail("DURABILITY_ATTESTATION_TAMPERED", str(supplied))


def _store_inventory(root: Path, managed_roots: Sequence[str]) -> tuple[StoreObject, ...]:
    items: list[StoreObject] = []
    for managed in managed_roots:
        base = root / managed
        if not os.path.lexists(base):
            continue
        metadata = base.lstat()
        _validate_no_link(metadata, base)
        if not stat.S_ISDIR(metadata.st_mode):
            _fail("GC_MANAGED_ROOT_INVALID", managed)
        for current_root, directory_names, file_names in os.walk(base):
            current = Path(current_root)
            directory_names.sort()
            file_names.sort()
            for name in directory_names:
                path = current / name
                metadata = path.lstat()
                _validate_no_link(metadata, path)
                if not stat.S_ISDIR(metadata.st_mode):
                    _fail("GC_ENTRY_INVALID", str(path))
            for name in file_names:
                path = current / name
                metadata = path.lstat()
                _validate_no_link(metadata, path)
                if not stat.S_ISREG(metadata.st_mode) or metadata.st_nlink != 1:
                    _fail("GC_ENTRY_INVALID", str(path))
                relative = path.relative_to(root).as_posix()
                content = read_contained_artifact_bytes(root=root, relative_path=relative)
                items.append(
                    StoreObject(
                        relative_path=relative,
                        sha256=sha256_bytes(content),
                        size_bytes=len(content),
                        modified_at_ns=metadata.st_mtime_ns,
                    )
                )
    items.sort(key=lambda item: item.relative_path)
    return tuple(items)


def _current_pointer_state(root: Path) -> list[dict[str, object]]:
    current_root = root / "current"
    if not os.path.lexists(current_root):
        return []
    metadata = current_root.lstat()
    _validate_no_link(metadata, current_root)
    if not stat.S_ISDIR(metadata.st_mode):
        _fail("CURRENT_POINTER_ROOT_INVALID", str(current_root))
    state: list[dict[str, object]] = []
    for path in sorted(current_root.iterdir(), key=lambda item: item.name):
        metadata = path.lstat()
        _validate_no_link(metadata, path)
        if path.suffix != ".json" or not stat.S_ISREG(metadata.st_mode) or metadata.st_nlink != 1:
            _fail("CURRENT_POINTER_ENTRY_INVALID", str(path))
        relative = path.relative_to(root).as_posix()
        content = read_contained_artifact_bytes(root=root, relative_path=relative)
        state.append(
            {
                "relative_path": relative,
                "sha256": sha256_bytes(content),
                "size_bytes": len(content),
            }
        )
    return state


def _history_payloads(
    root: Path,
    inventory: Sequence[StoreObject],
) -> dict[str, dict[str, object]]:
    payloads: dict[str, dict[str, object]] = {}
    for item in inventory:
        if not item.relative_path.startswith("pointer_history/"):
            continue
        payloads[item.relative_path] = _canonical_mapping(
            read_contained_artifact_bytes(root=root, relative_path=item.relative_path),
            "POINTER_HISTORY_INVALID",
        )
    return payloads


def _reachable_pointer_history(
    root: Path,
    inventory: Sequence[StoreObject],
    history: Mapping[str, Mapping[str, object]],
    protected: dict[str, str],
) -> set[str]:
    inventory_paths = {item.relative_path for item in inventory}
    current_root = root / "current"
    if not current_root.exists():
        return set()
    reachable: set[str] = set()
    for path in sorted(current_root.glob("*.json")):
        metadata = path.lstat()
        _validate_no_link(metadata, path)
        dataset_id = path.stem
        validate_current_snapshot(
            store_root=root,
            evidence_root=root,
            dataset_id=dataset_id,
        )
        current = _canonical_mapping(
            read_contained_artifact_bytes(
                root=root,
                relative_path=path.relative_to(root).as_posix(),
            ),
            "CURRENT_POINTER_INVALID",
        )
        pointer: Mapping[str, object] = current
        expected_sha: str | None = None
        seen: set[str] = set()
        while True:
            pointer_id = _text(pointer.get("pointer_id"), "pointer_id")
            if pointer_id in seen:
                _fail("GC_POINTER_CYCLE", pointer_id)
            seen.add(pointer_id)
            history_path = f"pointer_history/{dataset_id}/{pointer_id}.json"
            history_payload = history.get(history_path)
            if history_payload is None:
                _fail("GC_REACHABLE_HISTORY_MISSING", history_path)
            raw = canonical_json_bytes(history_payload)
            if expected_sha is not None and sha256_bytes(raw) != expected_sha:
                _fail("GC_POINTER_HISTORY_DIGEST_MISMATCH", history_path)
            reachable.add(history_path)
            protected[history_path] = "REACHABLE_CURRENT_HISTORY"
            for relative in _pointer_references(root, history_payload):
                if relative not in inventory_paths:
                    _fail("GC_REACHABLE_OBJECT_MISSING", relative)
                protected[relative] = "REACHABLE_CURRENT_CHAIN"
            previous_id = history_payload.get("previous_pointer_id")
            previous_sha = history_payload.get("previous_pointer_sha256")
            if previous_id is None and previous_sha is None:
                break
            pointer = history.get(
                f"pointer_history/{dataset_id}/{_text(previous_id, 'previous_pointer_id')}.json"
            ) or _fail("GC_REACHABLE_HISTORY_MISSING", str(previous_id))
            expected_sha = _digest(previous_sha, "previous_pointer_sha256")
    return reachable


def _pointer_references(root: Path, pointer: Mapping[str, object]) -> set[str]:
    references: set[str] = set()
    for field in ("manifest", "snapshot", "source_event"):
        artifact = _mapping(pointer.get(field), field)
        references.add(_portable_path(artifact.get("path"), f"{field}.path"))
    manifest_path = _portable_path(
        _mapping(pointer.get("manifest"), "manifest").get("path"),
        "manifest.path",
    )
    manifest = _canonical_mapping(
        read_contained_artifact_bytes(root=root, relative_path=manifest_path),
        "GC_MANIFEST_INVALID",
    )
    quality = _mapping(manifest.get("quality_binding"), "quality_binding")
    report = _mapping(quality.get("report"), "quality_binding.report")
    references.add(_portable_path(report.get("path"), "quality report path"))
    return references


def _pointer_retention_until(
    root: Path,
    pointer: Mapping[str, object],
) -> date | None:
    manifest_path = _portable_path(
        _mapping(pointer.get("manifest"), "manifest").get("path"),
        "manifest.path",
    )
    manifest = _canonical_mapping(
        read_contained_artifact_bytes(root=root, relative_path=manifest_path),
        "GC_MANIFEST_INVALID",
    )
    envelope = _mapping(manifest.get("artifact_envelope"), "artifact_envelope")
    value = envelope.get("retention_until")
    if value is None:
        return None
    try:
        return date.fromisoformat(str(value))
    except ValueError as exc:
        raise DataDurabilityError("GC_RETENTION_INVALID", str(value)) from exc


def _validate_backup_manifest(manifest: Mapping[str, object]) -> None:
    payload = dict(manifest)
    _exact_fields(
        payload,
        {
            "backup_id",
            "schema_version",
            "captured_at",
            "policy_id",
            "policy_version",
            "status",
            "objects",
            "object_count",
            "production_effect",
        },
        "BACKUP_MANIFEST_INVALID",
    )
    if (
        payload.get("schema_version") != BACKUP_MANIFEST_SCHEMA_VERSION
        or payload.get("status") != "PASS"
        or payload.get("production_effect") != "none"
    ):
        _fail("BACKUP_MANIFEST_INVALID", "status/schema/boundary")
    objects = _sequence(payload.get("objects"), "objects")
    if _integer(payload.get("object_count"), "object_count") != len(objects):
        _fail("BACKUP_MANIFEST_INVALID", "object_count")
    restore_paths: list[str] = []
    for raw in objects:
        item = _mapping(raw, "backup object")
        _exact_fields(
            item,
            {
                "source_identity",
                "category",
                "source_relative_path",
                "restore_path",
                "object_path",
                "sha256",
                "size_bytes",
            },
            "BACKUP_MANIFEST_INVALID",
        )
        restore_paths.append(_portable_path(item.get("restore_path"), "restore_path"))
        _portable_path(item.get("object_path"), "object_path")
        _digest(item.get("sha256"), "sha256")
        _integer(item.get("size_bytes"), "size_bytes")
    if restore_paths != sorted(set(restore_paths)):
        _fail("BACKUP_MANIFEST_INVALID", "restore path order/duplicate")
    supplied = payload.pop("backup_id")
    if supplied != _semantic_id("checksum_backup_", payload):
        _fail("BACKUP_MANIFEST_TAMPERED", str(supplied))


def _require_pass_receipt(
    receipt: Mapping[str, object],
    schema_version: str,
    id_field: str,
) -> None:
    if (
        receipt.get("schema_version") != schema_version
        or receipt.get("status") != "PASS"
        or receipt.get("production_effect") != "none"
    ):
        _fail("DURABILITY_EVIDENCE_NOT_PASS", schema_version)
    supplied = _text(receipt.get(id_field), id_field)
    body = dict(receipt)
    body.pop(id_field)
    prefixes = {
        "rehearsal_id": "crash_rehearsal_",
        "gc_receipt_id": "store_gc_receipt_",
        "restore_receipt_id": "checksum_restore_",
    }
    if supplied != _semantic_id(prefixes[id_field], body):
        _fail("DURABILITY_EVIDENCE_TAMPERED", supplied)


def _receipt_binding(
    receipt: Mapping[str, object],
    id_field: str,
) -> dict[str, object]:
    return {
        "evidence_id": receipt[id_field],
        "schema_version": receipt["schema_version"],
        "sha256": sha256_bytes(canonical_json_bytes(dict(receipt))),
    }


def _windows_filesystem(root: Path) -> tuple[str, int]:
    kernel32: Any = ctypes.WinDLL("kernel32", use_last_error=True)
    volume_path = ctypes.create_unicode_buffer(1024)
    if not kernel32.GetVolumePathNameW(str(root), volume_path, len(volume_path)):
        error = ctypes.get_last_error()
        raise DataDurabilityError(
            "DURABILITY_FILESYSTEM_PROBE_FAILED",
            ctypes.FormatError(error),
            path=root,
        )
    filesystem = ctypes.create_unicode_buffer(256)
    if not kernel32.GetVolumeInformationW(
        volume_path.value,
        None,
        0,
        None,
        None,
        None,
        filesystem,
        len(filesystem),
    ):
        error = ctypes.get_last_error()
        raise DataDurabilityError(
            "DURABILITY_FILESYSTEM_PROBE_FAILED",
            ctypes.FormatError(error),
            path=root,
        )
    return filesystem.value, int(kernel32.GetDriveTypeW(volume_path.value))


def _posix_filesystem(root: Path) -> tuple[str, str]:
    mountinfo = Path("/proc/self/mountinfo")
    if not mountinfo.is_file():
        return "UNKNOWN", "UNPROBED"
    try:
        rows = mountinfo.read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeDecodeError):
        return "UNKNOWN", "UNPROBED"
    candidates: list[tuple[int, str, str]] = []
    root_text = str(root)
    for row in rows:
        before, separator, after = row.partition(" - ")
        if not separator:
            continue
        fields = before.split()
        after_fields = after.split()
        if len(fields) < 5 or not after_fields:
            continue
        mount_point = fields[4].replace("\\040", " ")
        try:
            common = os.path.commonpath((root_text, mount_point))
        except ValueError:
            continue
        if common == mount_point:
            candidates.append((len(mount_point), after_fields[0], mount_point))
    if not candidates:
        return "UNKNOWN", "UNPROBED"
    _, filesystem, mount_point = max(candidates)
    return filesystem, f"LOCAL_MOUNT:{mount_point}"


def _validate_no_link(metadata: os.stat_result, path: Path) -> None:
    reparse = bool(getattr(metadata, "st_file_attributes", 0) & 0x400)
    if stat.S_ISLNK(metadata.st_mode) or reparse:
        _fail("DURABILITY_PATH_LINK_FORBIDDEN", str(path))


def _canonical_mapping(raw: bytes, code: str) -> dict[str, object]:
    try:
        value = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise DataDurabilityError(code, str(exc)) from exc
    if not isinstance(value, dict) or canonical_json_bytes(value) != raw:
        _fail(code, "noncanonical JSON object")
    return value


def _semantic_id(prefix: str, body: Mapping[str, object]) -> str:
    return f"{prefix}{sha256_bytes(canonical_json_bytes(dict(body)))[:32]}"


def _resolved_directory(path: Path, code: str) -> Path:
    try:
        resolved = path.resolve(strict=True)
    except OSError as exc:
        raise DataDurabilityError(code, str(exc), path=path) from exc
    if not resolved.is_dir():
        _fail(code, str(resolved))
    return resolved


def _aware_utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        _fail("DURABILITY_TIME_INVALID", str(value))
    return value.astimezone(UTC)


def _utc_text(value: datetime) -> str:
    return _aware_utc(value).isoformat().replace("+00:00", "Z")


def _parse_datetime(value: object, field: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError as exc:
        raise DataDurabilityError("DURABILITY_TIME_INVALID", field) from exc
    return _aware_utc(parsed)


def _portable_path(value: object, field: str) -> str:
    if not isinstance(value, str) or not value or "\\" in value or ":" in value:
        _fail("DURABILITY_PATH_INVALID", f"{field}: {value!r}")
    path = PurePosixPath(value)
    if path.is_absolute() or any(part in {"", ".", ".."} for part in path.parts):
        _fail("DURABILITY_PATH_INVALID", f"{field}: {value!r}")
    return path.as_posix()


def _mapping(value: object, field: str) -> dict[str, object]:
    if not isinstance(value, Mapping) or any(not isinstance(key, str) for key in value):
        _fail("DURABILITY_FIELD_INVALID", field)
    return dict(value)


def _sequence(value: object, field: str) -> list[object]:
    if not isinstance(value, list):
        _fail("DURABILITY_FIELD_INVALID", field)
    return value


def _strings(value: object, field: str) -> tuple[str, ...]:
    sequence = _sequence(value, field)
    result = tuple(_text(item, f"{field}[]") for item in sequence)
    if result != tuple(sorted(set(result))):
        _fail("DURABILITY_FIELD_INVALID", f"{field}: order/duplicate")
    return result


def _text(value: object, field: str) -> str:
    if not isinstance(value, str) or not value:
        _fail("DURABILITY_FIELD_INVALID", field)
    return value


def _integer(value: object, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        _fail("DURABILITY_FIELD_INVALID", field)
    return value


def _digest(value: object, field: str) -> str:
    text = _text(value, field)
    if not _SHA_RE.fullmatch(text):
        _fail("DURABILITY_FIELD_INVALID", field)
    return text


def _exact_fields(
    payload: Mapping[str, object],
    expected: set[str],
    code: str,
) -> None:
    if set(payload) != expected:
        raise DataDurabilityError(
            code,
            (
                f"missing={sorted(expected - set(payload))} "
                f"unknown={sorted(set(payload) - expected)}"
            ),
        )


def _fail(code: str, message: str) -> NoReturn:
    raise DataDurabilityError(code, message)


__all__ = [
    "BACKUP_MANIFEST_SCHEMA_VERSION",
    "CRASH_REHEARSAL_SCHEMA_VERSION",
    "DURABILITY_ATTESTATION_SCHEMA_VERSION",
    "DURABILITY_POLICY_SCHEMA_VERSION",
    "FILESYSTEM_PROFILE_SCHEMA_VERSION",
    "GC_PLAN_SCHEMA_VERSION",
    "GC_RECEIPT_SCHEMA_VERSION",
    "RESTORE_RECEIPT_SCHEMA_VERSION",
    "BackupSource",
    "DataDurabilityError",
    "DurabilityPolicy",
    "FilesystemDurabilityProfile",
    "StoreGcPlan",
    "apply_reference_safe_gc",
    "build_crash_rehearsal_receipt",
    "build_durability_attestation",
    "create_checksum_backup",
    "load_durability_policy",
    "plan_reference_safe_gc",
    "probe_filesystem_durability",
    "restore_checksum_backup",
    "validate_durability_attestation",
]
