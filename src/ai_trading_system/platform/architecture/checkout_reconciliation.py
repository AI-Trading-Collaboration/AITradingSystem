from __future__ import annotations

import hashlib
import json
import os
import subprocess
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path, PurePosixPath
from typing import Any

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.platform.architecture.checkout_guard import (
    CheckoutGuardError,
    KnownUnrelatedExclusion,
    load_checkout_guard_policy,
    resolve_checkout_identity,
)
from ai_trading_system.platform.artifacts import write_json_atomic
from ai_trading_system.yaml_loader import safe_load_yaml_path

CHECKOUT_RECONCILIATION_POLICY_SCHEMA_VERSION = (
    "arch_005_s4e_checkout_reconciliation_policy.v1"
)
CHECKOUT_HANDOFF_SCHEMA_VERSION = "checkout_handoff.v1"
CHECKOUT_RECONCILIATION_REPORT_SCHEMA_VERSION = "checkout_reconciliation_report.v1"
DEFAULT_CHECKOUT_RECONCILIATION_POLICY_PATH = (
    PROJECT_ROOT
    / "config"
    / "architecture"
    / "arch_005_s4e_checkout_reconciliation.yaml"
)


class CheckoutHandoffMode(StrEnum):
    PREPARED_COPY = "PREPARED_COPY"
    RECOVERY_AUDIT = "RECOVERY_AUDIT"


class CheckoutPathRole(StrEnum):
    OWNED = "owned"
    GENERATED = "generated"
    RETAINED = "retained"
    KNOWN_UNRELATED = "known_unrelated"


class ReconciliationClassification(StrEnum):
    EXACT_TARGET = "EXACT_TARGET"
    SUPERSEDED_IN_TARGET_HISTORY = "SUPERSEDED_IN_TARGET_HISTORY"
    SOURCE_RESTORED_OR_STAT_ONLY = "SOURCE_RESTORED_OR_STAT_ONLY"
    GENERATED_INVALIDATED = "GENERATED_INVALIDATED"
    RETAIN_UNIQUE = "RETAIN_UNIQUE"
    KNOWN_UNRELATED_NOT_READ = "KNOWN_UNRELATED_NOT_READ"
    MIXED_SPLIT_REQUIRED = "MIXED_SPLIT_REQUIRED"
    TARGET_LINEAGE_MISSING = "TARGET_LINEAGE_MISSING"
    UNATTRIBUTED_DIRTY = "UNATTRIBUTED_DIRTY"


@dataclass(frozen=True)
class CheckoutReconciliationPolicy:
    policy_id: str
    version: str
    status: str
    owner: str
    approval_ref: str
    protected_branches: tuple[str, ...]
    domain_mutation_allowed: bool
    shared_mutation_actors: tuple[str, ...]
    path_roles: tuple[CheckoutPathRole, ...]
    cleanup_eligible_classifications: tuple[ReconciliationClassification, ...]
    blocking_classifications: tuple[ReconciliationClassification, ...]
    known_unrelated_exclusions: tuple[KnownUnrelatedExclusion, ...]

    @property
    def policy_version(self) -> str:
        return f"{self.policy_id}@{self.version}"


@dataclass(frozen=True)
class CheckoutPathSnapshot:
    path: str
    exists: bool
    status_code: str | None
    raw_sha256: str | None
    normalized_blob_id: str | None
    size_bytes: int | None

    def to_dict(self) -> dict[str, object]:
        return {
            "path": self.path,
            "exists": self.exists,
            "status_code": self.status_code,
            "raw_sha256": self.raw_sha256,
            "normalized_blob_id": self.normalized_blob_id,
            "size_bytes": self.size_bytes,
        }


def load_checkout_reconciliation_policy(
    path: Path = DEFAULT_CHECKOUT_RECONCILIATION_POLICY_PATH,
) -> CheckoutReconciliationPolicy:
    payload = _mapping(safe_load_yaml_path(path), "policy")
    if payload.get("schema_version") != CHECKOUT_RECONCILIATION_POLICY_SCHEMA_VERSION:
        raise CheckoutGuardError(
            "CHECKOUT_RECONCILIATION_POLICY_SCHEMA",
            str(payload.get("schema_version")),
        )
    if payload.get("status") != "OWNER_APPROVED_S0_S1":
        raise CheckoutGuardError(
            "CHECKOUT_RECONCILIATION_POLICY_STATUS",
            str(payload.get("status")),
        )
    guard_ref = _mapping(payload.get("checkout_guard_policy"), "checkout_guard_policy")
    guard_path = _portable_path(guard_ref.get("path"), "checkout_guard_policy.path")
    guard_policy = load_checkout_guard_policy(PROJECT_ROOT / guard_path)
    if (
        guard_ref.get("policy_id") != guard_policy.policy_id
        or guard_ref.get("version") != guard_policy.version
    ):
        raise CheckoutGuardError(
            "CHECKOUT_RECONCILIATION_POLICY_GUARD_REF",
            f"{guard_ref.get('policy_id')}@{guard_ref.get('version')}",
        )
    path_roles = tuple(
        CheckoutPathRole(value)
        for value in _strings(payload.get("path_roles"), "path_roles")
    )
    if path_roles != tuple(CheckoutPathRole):
        raise CheckoutGuardError(
            "CHECKOUT_RECONCILIATION_POLICY_PATH_ROLES",
            ",".join(role.value for role in path_roles),
        )
    cleanup = tuple(
        ReconciliationClassification(value)
        for value in _strings(
            payload.get("cleanup_eligible_classifications"),
            "cleanup_eligible_classifications",
        )
    )
    if cleanup != (
        ReconciliationClassification.EXACT_TARGET,
        ReconciliationClassification.SUPERSEDED_IN_TARGET_HISTORY,
    ):
        raise CheckoutGuardError(
            "CHECKOUT_RECONCILIATION_POLICY_CLEANUP",
            ",".join(value.value for value in cleanup),
        )
    blocking = tuple(
        ReconciliationClassification(value)
        for value in _strings(
            payload.get("blocking_classifications"),
            "blocking_classifications",
        )
    )
    expected_blocking = (
        ReconciliationClassification.MIXED_SPLIT_REQUIRED,
        ReconciliationClassification.TARGET_LINEAGE_MISSING,
        ReconciliationClassification.UNATTRIBUTED_DIRTY,
    )
    if blocking != expected_blocking:
        raise CheckoutGuardError(
            "CHECKOUT_RECONCILIATION_POLICY_BLOCKING",
            ",".join(value.value for value in blocking),
        )
    raw_exclusions = payload.get("known_unrelated_exclusions")
    if not isinstance(raw_exclusions, list):
        raise CheckoutGuardError(
            "CHECKOUT_RECONCILIATION_POLICY_EXCLUSIONS",
            "known_unrelated_exclusions must be a list",
        )
    exclusions = tuple(
        KnownUnrelatedExclusion(
            path=_portable_path(
                _mapping(row, "known_unrelated_exclusion").get("path"),
                "known_unrelated.path",
            ),
            rationale=_required_text(
                _mapping(row, "known_unrelated_exclusion").get("rationale"),
                "known_unrelated.rationale",
            ),
            owner_ref=_required_text(
                _mapping(row, "known_unrelated_exclusion").get("owner_ref"),
                "known_unrelated.owner_ref",
            ),
        )
        for row in raw_exclusions
    )
    safety = _mapping(payload.get("safety"), "safety")
    false_fields = (
        "automatic_cleanup_allowed",
        "automatic_restore_allowed",
        "automatic_delete_allowed",
        "automatic_commit_allowed",
        "automatic_merge_allowed",
        "automatic_push_allowed",
        "automatic_task_mutation",
        "task_source_cutover",
        "strategy_logic_change",
        "strategy_threshold_change",
    )
    if any(safety.get(field) is not False for field in false_fields):
        raise CheckoutGuardError(
            "CHECKOUT_RECONCILIATION_POLICY_UNSAFE_PERMISSION",
            "automatic mutation flags must remain false",
        )
    if safety.get("production_effect") != "none" or safety.get("broker_action") != "none":
        raise CheckoutGuardError(
            "CHECKOUT_RECONCILIATION_POLICY_UNSAFE_EFFECT",
            f"{safety.get('production_effect')}/{safety.get('broker_action')}",
        )
    return CheckoutReconciliationPolicy(
        policy_id=_identifier(payload.get("policy_id"), "policy_id"),
        version=_identifier(payload.get("version"), "version"),
        status=str(payload.get("status")),
        owner=_required_text(payload.get("owner"), "owner"),
        approval_ref=_required_text(payload.get("approval_ref"), "approval_ref"),
        protected_branches=guard_policy.protected_branches,
        domain_mutation_allowed=(
            guard_policy.protected_branch_domain_mutation_allowed
        ),
        shared_mutation_actors=guard_policy.protected_branch_shared_mutation_actors,
        path_roles=path_roles,
        cleanup_eligible_classifications=cleanup,
        blocking_classifications=blocking,
        known_unrelated_exclusions=exclusions,
    )


def build_checkout_handoff(
    *,
    source_root: Path,
    target_root: Path,
    task_id: str,
    target_ref: str,
    owned_paths: Sequence[str],
    generated_paths: Sequence[str] = (),
    retained_paths: Sequence[str] = (),
    mode: CheckoutHandoffMode = CheckoutHandoffMode.PREPARED_COPY,
    policy_path: Path = DEFAULT_CHECKOUT_RECONCILIATION_POLICY_PATH,
    created_at: datetime | None = None,
) -> dict[str, object]:
    policy = load_checkout_reconciliation_policy(policy_path)
    source_identity = resolve_checkout_identity(source_root)
    target_identity = resolve_checkout_identity(target_root)
    if source_identity.workspace_id == target_identity.workspace_id:
        raise CheckoutGuardError(
            "CHECKOUT_HANDOFF_SAME_WORKSPACE",
            source_identity.workspace_id,
        )
    if not _same_resolved_path(
        source_identity.git_common_dir,
        target_identity.git_common_dir,
    ):
        raise CheckoutGuardError(
            "CHECKOUT_HANDOFF_REPOSITORY_MISMATCH",
            f"{source_identity.git_common_dir}!={target_identity.git_common_dir}",
        )
    if mode is CheckoutHandoffMode.PREPARED_COPY:
        if source_identity.head_commit != target_identity.head_commit:
            raise CheckoutGuardError(
                "CHECKOUT_HANDOFF_BASE_DRIFT",
                f"{source_identity.head_commit}!={target_identity.head_commit}",
            )
        base_commit = source_identity.head_commit
    else:
        base_commit = source_identity.head_commit
        target_commit = _resolve_commit(target_root, target_ref)
        if not _is_first_parent_ancestor(target_root, base_commit, target_commit):
            raise CheckoutGuardError(
                "CHECKOUT_HANDOFF_TARGET_NOT_DESCENDANT",
                f"{base_commit}!<={target_commit}",
            )
    role_paths = _checked_role_paths(
        owned_paths=owned_paths,
        generated_paths=generated_paths,
        retained_paths=retained_paths,
        known_unrelated_paths=tuple(
            exclusion.path for exclusion in policy.known_unrelated_exclusions
        ),
    )
    source_status = _git_status_map(
        source_root,
        exclusions=tuple(
            exclusion.path for exclusion in policy.known_unrelated_exclusions
        ),
    )
    target_status = _git_status_map(
        target_root,
        exclusions=tuple(
            exclusion.path for exclusion in policy.known_unrelated_exclusions
        ),
    )
    entries: list[dict[str, object]] = []
    for role, paths in role_paths.items():
        for relative_path in paths:
            if role is CheckoutPathRole.KNOWN_UNRELATED:
                exclusion = next(
                    row
                    for row in policy.known_unrelated_exclusions
                    if row.path == relative_path
                )
                entries.append(
                    {
                        "path": relative_path,
                        "role": role.value,
                        "source_snapshot": None,
                        "target_snapshot": None,
                        "copy_equal": None,
                        "rationale": exclusion.rationale,
                        "owner_ref": exclusion.owner_ref,
                        "bytes_observed": False,
                    }
                )
                continue
            source_snapshot = _snapshot_path(
                source_root,
                relative_path,
                status_code=source_status.get(relative_path),
            )
            target_snapshot = _snapshot_path(
                target_root,
                relative_path,
                status_code=target_status.get(relative_path),
            )
            copy_equal = _snapshots_equal(source_snapshot, target_snapshot)
            if (
                mode is CheckoutHandoffMode.PREPARED_COPY
                and role is CheckoutPathRole.OWNED
                and not copy_equal
            ):
                raise CheckoutGuardError(
                    "CHECKOUT_HANDOFF_COPY_MISMATCH",
                    relative_path,
                )
            entries.append(
                {
                    "path": relative_path,
                    "role": role.value,
                    "source_snapshot": source_snapshot.to_dict(),
                    "target_snapshot": target_snapshot.to_dict(),
                    "copy_equal": copy_equal,
                    "rationale": None,
                    "owner_ref": None,
                    "bytes_observed": True,
                }
            )
    instant = _aware_utc(created_at or datetime.now(tz=UTC))
    payload: dict[str, object] = {
        "schema_version": CHECKOUT_HANDOFF_SCHEMA_VERSION,
        "status": "PASS",
        "decision": "HANDOFF_PREPARED",
        "mode": mode.value,
        "policy_version": policy.policy_version,
        "task_id": _identifier(task_id, "task_id"),
        "created_at": instant.isoformat(),
        "base_commit": base_commit,
        "target_ref": _required_text(target_ref, "target_ref"),
        "source_identity": source_identity.to_dict(),
        "target_identity": target_identity.to_dict(),
        "source_dirty_paths": sorted(source_status),
        "target_dirty_paths": sorted(target_status),
        "entries": entries,
        "automatic_cleanup_allowed": False,
        "task_source_cutover": False,
        "production_effect": "none",
        "broker_action": "none",
    }
    payload["handoff_checksum"] = _payload_checksum(payload, "handoff_checksum")
    validate_checkout_handoff(payload, policy_path=policy_path)
    return payload


def write_checkout_handoff(path: Path, payload: Mapping[str, object]) -> None:
    validate_checkout_handoff(payload)
    write_json_atomic(path, dict(payload))


def validate_checkout_handoff(
    payload: Mapping[str, object],
    *,
    policy_path: Path = DEFAULT_CHECKOUT_RECONCILIATION_POLICY_PATH,
) -> None:
    policy = load_checkout_reconciliation_policy(policy_path)
    if payload.get("schema_version") != CHECKOUT_HANDOFF_SCHEMA_VERSION:
        raise CheckoutGuardError(
            "CHECKOUT_HANDOFF_SCHEMA",
            str(payload.get("schema_version")),
        )
    if payload.get("status") != "PASS" or payload.get("decision") != "HANDOFF_PREPARED":
        raise CheckoutGuardError(
            "CHECKOUT_HANDOFF_STATUS",
            f"{payload.get('status')}/{payload.get('decision')}",
        )
    if payload.get("policy_version") != policy.policy_version:
        raise CheckoutGuardError(
            "CHECKOUT_HANDOFF_POLICY_VERSION",
            str(payload.get("policy_version")),
        )
    try:
        mode = CheckoutHandoffMode(str(payload.get("mode")))
    except ValueError as exc:
        raise CheckoutGuardError(
            "CHECKOUT_HANDOFF_MODE",
            str(payload.get("mode")),
        ) from exc
    _exact_commit(payload.get("base_commit"), "base_commit")
    _required_text(payload.get("target_ref"), "target_ref")
    _validate_identity(payload.get("source_identity"), "source_identity")
    _validate_identity(payload.get("target_identity"), "target_identity")
    entries = payload.get("entries")
    if not isinstance(entries, list) or not entries:
        raise CheckoutGuardError("CHECKOUT_HANDOFF_ENTRIES", "entries must be non-empty")
    seen: set[str] = set()
    for raw_entry in entries:
        entry = _mapping(raw_entry, "entry")
        path = _portable_path(entry.get("path"), "entry.path")
        if path.casefold() in seen:
            raise CheckoutGuardError("CHECKOUT_HANDOFF_PATH_DUPLICATE", path)
        seen.add(path.casefold())
        try:
            role = CheckoutPathRole(str(entry.get("role")))
        except ValueError as exc:
            raise CheckoutGuardError(
                "CHECKOUT_HANDOFF_ROLE",
                str(entry.get("role")),
            ) from exc
        if role is CheckoutPathRole.KNOWN_UNRELATED:
            if (
                entry.get("source_snapshot") is not None
                or entry.get("target_snapshot") is not None
                or entry.get("copy_equal") is not None
                or entry.get("bytes_observed") is not False
            ):
                raise CheckoutGuardError(
                    "CHECKOUT_HANDOFF_EXCLUSION_BYTES",
                    path,
                )
            _required_text(entry.get("rationale"), "entry.rationale")
            _required_text(entry.get("owner_ref"), "entry.owner_ref")
            continue
        source = _validate_snapshot(entry.get("source_snapshot"), "source_snapshot")
        target = _validate_snapshot(entry.get("target_snapshot"), "target_snapshot")
        copy_equal = entry.get("copy_equal")
        if not isinstance(copy_equal, bool):
            raise CheckoutGuardError("CHECKOUT_HANDOFF_COPY_EQUAL", path)
        if copy_equal != _snapshots_equal(source, target):
            raise CheckoutGuardError("CHECKOUT_HANDOFF_COPY_EVIDENCE", path)
        if (
            mode is CheckoutHandoffMode.PREPARED_COPY
            and role is CheckoutPathRole.OWNED
            and not copy_equal
        ):
            raise CheckoutGuardError("CHECKOUT_HANDOFF_COPY_MISMATCH", path)
        if entry.get("bytes_observed") is not True:
            raise CheckoutGuardError("CHECKOUT_HANDOFF_BYTES_OBSERVED", path)
    if any(
        payload.get(field) is not False
        for field in ("automatic_cleanup_allowed", "task_source_cutover")
    ):
        raise CheckoutGuardError(
            "CHECKOUT_HANDOFF_UNSAFE_PERMISSION",
            "automatic cleanup and task cutover must remain false",
        )
    if payload.get("production_effect") != "none" or payload.get("broker_action") != "none":
        raise CheckoutGuardError(
            "CHECKOUT_HANDOFF_UNSAFE_EFFECT",
            f"{payload.get('production_effect')}/{payload.get('broker_action')}",
        )
    checksum = payload.get("handoff_checksum")
    if checksum != _payload_checksum(payload, "handoff_checksum"):
        raise CheckoutGuardError("CHECKOUT_HANDOFF_CHECKSUM", str(checksum))


def build_checkout_reconciliation_report(
    *,
    handoff: Mapping[str, object],
    source_root: Path | None = None,
    target_root: Path | None = None,
    target_ref: str | None = None,
    policy_path: Path = DEFAULT_CHECKOUT_RECONCILIATION_POLICY_PATH,
    created_at: datetime | None = None,
) -> dict[str, object]:
    validate_checkout_handoff(handoff, policy_path=policy_path)
    policy = load_checkout_reconciliation_policy(policy_path)
    source_identity_payload = _mapping(handoff.get("source_identity"), "source_identity")
    target_identity_payload = _mapping(handoff.get("target_identity"), "target_identity")
    actual_source_root = (
        source_root
        if source_root is not None
        else Path(_required_text(source_identity_payload.get("checkout_root"), "checkout_root"))
    )
    actual_target_root = (
        target_root
        if target_root is not None
        else Path(_required_text(target_identity_payload.get("checkout_root"), "checkout_root"))
    )
    current_source_identity = resolve_checkout_identity(actual_source_root)
    current_target_identity = resolve_checkout_identity(actual_target_root)
    if current_source_identity.workspace_id != source_identity_payload.get("workspace_id"):
        raise CheckoutGuardError(
            "CHECKOUT_RECONCILIATION_SOURCE_IDENTITY",
            current_source_identity.workspace_id,
        )
    if not _same_resolved_path(
        current_source_identity.git_common_dir,
        current_target_identity.git_common_dir,
    ):
        raise CheckoutGuardError(
            "CHECKOUT_RECONCILIATION_REPOSITORY_MISMATCH",
            f"{current_source_identity.git_common_dir}!={current_target_identity.git_common_dir}",
        )
    base_commit = _exact_commit(handoff.get("base_commit"), "base_commit")
    if current_source_identity.head_commit != base_commit:
        raise CheckoutGuardError(
            "CHECKOUT_RECONCILIATION_SOURCE_HEAD_DRIFT",
            f"{current_source_identity.head_commit}!={base_commit}",
        )
    resolved_target_ref = target_ref or _required_text(handoff.get("target_ref"), "target_ref")
    target_commit = _resolve_commit(actual_target_root, resolved_target_ref)
    if not _is_first_parent_ancestor(
        actual_target_root,
        base_commit,
        target_commit,
    ):
        raise CheckoutGuardError(
            "CHECKOUT_RECONCILIATION_TARGET_NOT_DESCENDANT",
            f"{base_commit}!<={target_commit}",
        )
    excluded_paths = tuple(
        exclusion.path for exclusion in policy.known_unrelated_exclusions
    )
    current_status = _git_status_map(actual_source_root, exclusions=excluded_paths)
    entries = handoff.get("entries")
    assert isinstance(entries, list)
    declared_paths = {
        _required_text(_mapping(entry, "entry").get("path"), "entry.path").casefold()
        for entry in entries
        if _mapping(entry, "entry").get("role")
        != CheckoutPathRole.KNOWN_UNRELATED.value
    }
    results: list[dict[str, object]] = []
    for raw_entry in entries:
        entry = _mapping(raw_entry, "entry")
        path = _portable_path(entry.get("path"), "entry.path")
        role = CheckoutPathRole(str(entry.get("role")))
        if role is CheckoutPathRole.KNOWN_UNRELATED:
            results.append(
                _result_row(
                    path=path,
                    role=role,
                    classification=(
                        ReconciliationClassification.KNOWN_UNRELATED_NOT_READ
                    ),
                    reason_codes=("KNOWN_UNRELATED_EXACT_PATH_EXCLUDED",),
                    cleanup_eligible=False,
                    prepared_snapshot=None,
                    current_snapshot=None,
                    target_blob_id=None,
                    matching_history_commit=None,
                    bytes_observed=False,
                )
            )
            continue
        prepared = _validate_snapshot(entry.get("source_snapshot"), "source_snapshot")
        current = _snapshot_path(
            actual_source_root,
            path,
            status_code=current_status.get(path),
        )
        target_blob = _blob_at_commit(actual_target_root, target_commit, path)
        matching_commit: str | None = None
        if role is CheckoutPathRole.GENERATED:
            classification = (
                ReconciliationClassification.GENERATED_INVALIDATED
                if path in current_status
                else ReconciliationClassification.SOURCE_RESTORED_OR_STAT_ONLY
            )
            reasons = (
                ("GENERATED_VIEW_REBUILD_REQUIRED",)
                if classification
                is ReconciliationClassification.GENERATED_INVALIDATED
                else ("SOURCE_PATH_CLEAN",)
            )
        elif role is CheckoutPathRole.RETAINED:
            classification = (
                ReconciliationClassification.RETAIN_UNIQUE
                if path in current_status
                else ReconciliationClassification.SOURCE_RESTORED_OR_STAT_ONLY
            )
            reasons = (
                ("EXPLICIT_RETAINED_PATH",)
                if classification is ReconciliationClassification.RETAIN_UNIQUE
                else ("SOURCE_PATH_CLEAN",)
            )
        elif path not in current_status:
            classification = ReconciliationClassification.SOURCE_RESTORED_OR_STAT_ONLY
            reasons = ("SOURCE_PATH_CLEAN",)
        elif not _snapshots_equal(prepared, current):
            classification = ReconciliationClassification.MIXED_SPLIT_REQUIRED
            reasons = ("SOURCE_CHANGED_AFTER_HANDOFF",)
        elif current.normalized_blob_id == target_blob:
            classification = ReconciliationClassification.EXACT_TARGET
            reasons = ("SOURCE_BYTES_EQUAL_TARGET_COMMIT",)
        else:
            matching_commit = _find_blob_in_first_parent_history(
                actual_target_root,
                base_commit=base_commit,
                target_commit=target_commit,
                path=path,
                blob_id=current.normalized_blob_id,
            )
            if matching_commit is None:
                classification = ReconciliationClassification.TARGET_LINEAGE_MISSING
                reasons = ("SOURCE_BYTES_ABSENT_FROM_TARGET_HISTORY",)
            else:
                classification = (
                    ReconciliationClassification.SUPERSEDED_IN_TARGET_HISTORY
                )
                reasons = ("SOURCE_BYTES_PRESENT_IN_REVIEWED_TARGET_HISTORY",)
        results.append(
            _result_row(
                path=path,
                role=role,
                classification=classification,
                reason_codes=reasons,
                cleanup_eligible=(
                    classification in policy.cleanup_eligible_classifications
                ),
                prepared_snapshot=prepared,
                current_snapshot=current,
                target_blob_id=target_blob,
                matching_history_commit=matching_commit,
                bytes_observed=True,
            )
        )
    unattributed = tuple(
        sorted(
            path
            for path in current_status
            if path.casefold() not in declared_paths
        )
    )
    for path in unattributed:
        results.append(
            _result_row(
                path=path,
                role=None,
                classification=ReconciliationClassification.UNATTRIBUTED_DIRTY,
                reason_codes=("DIRTY_PATH_NOT_DECLARED_IN_HANDOFF",),
                cleanup_eligible=False,
                prepared_snapshot=None,
                current_snapshot=None,
                target_blob_id=None,
                matching_history_commit=None,
                bytes_observed=False,
            )
        )
    blocking_values = {value.value for value in policy.blocking_classifications}
    blockers = tuple(
        row for row in results if row["classification"] in blocking_values
    )
    cleanup_allowlist = tuple(
        sorted(row["path"] for row in results if row["cleanup_eligible"])
    )
    generated_to_rebuild = tuple(
        sorted(
            row["path"]
            for row in results
            if row["classification"]
            == ReconciliationClassification.GENERATED_INVALIDATED.value
        )
    )
    retained = tuple(
        sorted(
            row["path"]
            for row in results
            if row["classification"]
            == ReconciliationClassification.RETAIN_UNIQUE.value
        )
    )
    if blockers:
        status = "BLOCKED"
        decision = "BLOCKED"
    elif not current_status:
        status = "PASS"
        decision = "PASS_CLEAN"
    else:
        status = "PASS"
        decision = "READY_FOR_COORDINATOR_RECONCILIATION"
    instant = _aware_utc(created_at or datetime.now(tz=UTC))
    payload: dict[str, object] = {
        "schema_version": CHECKOUT_RECONCILIATION_REPORT_SCHEMA_VERSION,
        "status": status,
        "decision": decision,
        "policy_version": policy.policy_version,
        "created_at": instant.isoformat(),
        "task_id": handoff.get("task_id"),
        "handoff_checksum": handoff.get("handoff_checksum"),
        "source_identity": current_source_identity.to_dict(),
        "target_identity": current_target_identity.to_dict(),
        "base_commit": base_commit,
        "target_ref": resolved_target_ref,
        "target_commit": target_commit,
        "target_first_parent_ancestry": True,
        "results": results,
        "cleanup_allowlist": list(cleanup_allowlist),
        "generated_to_rebuild": list(generated_to_rebuild),
        "retained_paths": list(retained),
        "blocking_paths": sorted(row["path"] for row in blockers),
        "unattributed_dirty_paths": list(unattributed),
        "automatic_cleanup_allowed": False,
        "automatic_restore_allowed": False,
        "task_source_cutover": False,
        "production_effect": "none",
        "broker_action": "none",
    }
    payload["report_checksum"] = _payload_checksum(payload, "report_checksum")
    validate_checkout_reconciliation_report(payload, policy_path=policy_path)
    return payload


def write_checkout_reconciliation_report(
    path: Path,
    payload: Mapping[str, object],
) -> None:
    validate_checkout_reconciliation_report(payload)
    write_json_atomic(path, dict(payload))


def validate_checkout_reconciliation_report(
    payload: Mapping[str, object],
    *,
    policy_path: Path = DEFAULT_CHECKOUT_RECONCILIATION_POLICY_PATH,
) -> None:
    policy = load_checkout_reconciliation_policy(policy_path)
    if payload.get("schema_version") != CHECKOUT_RECONCILIATION_REPORT_SCHEMA_VERSION:
        raise CheckoutGuardError(
            "CHECKOUT_RECONCILIATION_REPORT_SCHEMA",
            str(payload.get("schema_version")),
        )
    if payload.get("policy_version") != policy.policy_version:
        raise CheckoutGuardError(
            "CHECKOUT_RECONCILIATION_REPORT_POLICY",
            str(payload.get("policy_version")),
        )
    status = payload.get("status")
    decision = payload.get("decision")
    if (status, decision) not in {
        ("PASS", "PASS_CLEAN"),
        ("PASS", "READY_FOR_COORDINATOR_RECONCILIATION"),
        ("BLOCKED", "BLOCKED"),
    }:
        raise CheckoutGuardError(
            "CHECKOUT_RECONCILIATION_REPORT_STATUS",
            f"{status}/{decision}",
        )
    _exact_commit(payload.get("base_commit"), "base_commit")
    _exact_commit(payload.get("target_commit"), "target_commit")
    if payload.get("target_first_parent_ancestry") is not True:
        raise CheckoutGuardError(
            "CHECKOUT_RECONCILIATION_REPORT_ANCESTRY",
            str(payload.get("target_first_parent_ancestry")),
        )
    results = payload.get("results")
    if not isinstance(results, list) or not results:
        raise CheckoutGuardError(
            "CHECKOUT_RECONCILIATION_REPORT_RESULTS",
            "results must be non-empty",
        )
    seen: set[str] = set()
    computed_cleanup: list[str] = []
    computed_blockers: list[str] = []
    computed_unattributed: list[str] = []
    for raw_result in results:
        row = _mapping(raw_result, "result")
        path = _portable_path(row.get("path"), "result.path")
        if path.casefold() in seen:
            raise CheckoutGuardError(
                "CHECKOUT_RECONCILIATION_REPORT_DUPLICATE",
                path,
            )
        seen.add(path.casefold())
        try:
            classification = ReconciliationClassification(
                str(row.get("classification"))
            )
        except ValueError as exc:
            raise CheckoutGuardError(
                "CHECKOUT_RECONCILIATION_REPORT_CLASSIFICATION",
                str(row.get("classification")),
            ) from exc
        cleanup_eligible = row.get("cleanup_eligible")
        if not isinstance(cleanup_eligible, bool):
            raise CheckoutGuardError(
                "CHECKOUT_RECONCILIATION_REPORT_CLEANUP_FLAG",
                path,
            )
        expected_cleanup = classification in policy.cleanup_eligible_classifications
        if cleanup_eligible != expected_cleanup:
            raise CheckoutGuardError(
                "CHECKOUT_RECONCILIATION_REPORT_CLEANUP_CLASS",
                path,
            )
        if cleanup_eligible:
            computed_cleanup.append(path)
        if classification in policy.blocking_classifications:
            computed_blockers.append(path)
        if classification is ReconciliationClassification.UNATTRIBUTED_DIRTY:
            computed_unattributed.append(path)
        bytes_observed = row.get("bytes_observed")
        if classification in {
            ReconciliationClassification.KNOWN_UNRELATED_NOT_READ,
            ReconciliationClassification.UNATTRIBUTED_DIRTY,
        }:
            if bytes_observed is not False:
                raise CheckoutGuardError(
                    "CHECKOUT_RECONCILIATION_REPORT_UNSCOPED_BYTES",
                    path,
                )
        elif bytes_observed is not True:
            raise CheckoutGuardError(
                "CHECKOUT_RECONCILIATION_REPORT_SCOPED_BYTES",
                path,
            )
    if payload.get("cleanup_allowlist") != sorted(computed_cleanup):
        raise CheckoutGuardError(
            "CHECKOUT_RECONCILIATION_REPORT_ALLOWLIST",
            "cleanup allowlist mismatch",
        )
    if payload.get("blocking_paths") != sorted(computed_blockers):
        raise CheckoutGuardError(
            "CHECKOUT_RECONCILIATION_REPORT_BLOCKERS",
            "blocking path mismatch",
        )
    if payload.get("unattributed_dirty_paths") != sorted(computed_unattributed):
        raise CheckoutGuardError(
            "CHECKOUT_RECONCILIATION_REPORT_UNATTRIBUTED",
            "unattributed path mismatch",
        )
    if decision == "BLOCKED" and not computed_blockers:
        raise CheckoutGuardError(
            "CHECKOUT_RECONCILIATION_REPORT_BLOCKED_EMPTY",
            "BLOCKED requires a blocking path",
        )
    if decision != "BLOCKED" and computed_blockers:
        raise CheckoutGuardError(
            "CHECKOUT_RECONCILIATION_REPORT_BLOCKER_STATUS",
            decision,
        )
    if any(
        payload.get(field) is not False
        for field in (
            "automatic_cleanup_allowed",
            "automatic_restore_allowed",
            "task_source_cutover",
        )
    ):
        raise CheckoutGuardError(
            "CHECKOUT_RECONCILIATION_REPORT_UNSAFE_PERMISSION",
            "automatic mutation flags must remain false",
        )
    if payload.get("production_effect") != "none" or payload.get("broker_action") != "none":
        raise CheckoutGuardError(
            "CHECKOUT_RECONCILIATION_REPORT_UNSAFE_EFFECT",
            f"{payload.get('production_effect')}/{payload.get('broker_action')}",
        )
    checksum = payload.get("report_checksum")
    if checksum != _payload_checksum(payload, "report_checksum"):
        raise CheckoutGuardError(
            "CHECKOUT_RECONCILIATION_REPORT_CHECKSUM",
            str(checksum),
        )


def _checked_role_paths(
    *,
    owned_paths: Sequence[str],
    generated_paths: Sequence[str],
    retained_paths: Sequence[str],
    known_unrelated_paths: Sequence[str],
) -> dict[CheckoutPathRole, tuple[str, ...]]:
    rows = {
        CheckoutPathRole.OWNED: _paths(owned_paths, "owned_paths"),
        CheckoutPathRole.GENERATED: _paths(generated_paths, "generated_paths"),
        CheckoutPathRole.RETAINED: _paths(retained_paths, "retained_paths"),
        CheckoutPathRole.KNOWN_UNRELATED: _paths(
            known_unrelated_paths,
            "known_unrelated_paths",
        ),
    }
    if not rows[CheckoutPathRole.OWNED]:
        raise CheckoutGuardError(
            "CHECKOUT_HANDOFF_OWNED_PATHS_EMPTY",
            "at least one owned path is required",
        )
    owners: dict[str, CheckoutPathRole] = {}
    for role, paths in rows.items():
        for path in paths:
            key = path.casefold()
            if key in owners:
                raise CheckoutGuardError(
                    "CHECKOUT_HANDOFF_ROLE_OVERLAP",
                    f"{path}:{owners[key].value}/{role.value}",
                )
            owners[key] = role
    return rows


def _snapshot_path(
    root: Path,
    relative_path: str,
    *,
    status_code: str | None,
) -> CheckoutPathSnapshot:
    path = root / Path(*PurePosixPath(relative_path).parts)
    if not path.exists():
        return CheckoutPathSnapshot(
            path=relative_path,
            exists=False,
            status_code=status_code,
            raw_sha256=None,
            normalized_blob_id=None,
            size_bytes=None,
        )
    if not path.is_file():
        raise CheckoutGuardError("CHECKOUT_HANDOFF_PATH_NOT_FILE", relative_path)
    raw = path.read_bytes()
    normalized_blob = _git_output(
        root,
        ("hash-object", f"--path={relative_path}", "--", relative_path),
        required=True,
    )
    return CheckoutPathSnapshot(
        path=relative_path,
        exists=True,
        status_code=status_code,
        raw_sha256=hashlib.sha256(raw).hexdigest(),
        normalized_blob_id=normalized_blob,
        size_bytes=len(raw),
    )


def _git_status_map(
    root: Path,
    *,
    exclusions: Sequence[str],
) -> dict[str, str]:
    args = [
        "status",
        "--porcelain=v1",
        "-z",
        "--untracked-files=all",
        "--ignore-submodules=none",
        "--",
        ".",
        *(f":(exclude,literal){path}" for path in exclusions),
    ]
    try:
        result = subprocess.run(
            ["git", "-c", "core.quotepath=false", *args],
            cwd=root,
            check=False,
            capture_output=True,
        )
    except OSError as exc:
        raise CheckoutGuardError("CHECKOUT_HANDOFF_GIT_STATUS_EXECUTION", str(exc)) from exc
    if result.returncode != 0:
        raise CheckoutGuardError(
            "CHECKOUT_HANDOFF_GIT_STATUS_FAILED",
            result.stderr.decode("utf-8", errors="replace").strip(),
        )
    tokens = result.stdout.split(b"\0")
    rows: dict[str, str] = {}
    index = 0
    while index < len(tokens):
        token = tokens[index]
        index += 1
        if not token:
            continue
        text = token.decode("utf-8", errors="strict")
        if len(text) < 4:
            raise CheckoutGuardError("CHECKOUT_HANDOFF_GIT_STATUS_INVALID", text)
        status_code = text[:2]
        path = _portable_path(text[3:], "git_status.path")
        rows[path] = status_code
        if "R" in status_code or "C" in status_code:
            if index >= len(tokens) or not tokens[index]:
                raise CheckoutGuardError(
                    "CHECKOUT_HANDOFF_GIT_STATUS_RENAME_INVALID",
                    text,
                )
            original = _portable_path(
                tokens[index].decode("utf-8", errors="strict"),
                "git_status.original_path",
            )
            rows[original] = status_code
            index += 1
    return dict(sorted(rows.items()))


def _find_blob_in_first_parent_history(
    root: Path,
    *,
    base_commit: str,
    target_commit: str,
    path: str,
    blob_id: str | None,
) -> str | None:
    commits_text = _git_output(
        root,
        (
            "rev-list",
            "--first-parent",
            "--reverse",
            f"{base_commit}..{target_commit}",
        ),
        required=False,
    )
    commits = () if commits_text is None else tuple(commits_text.splitlines())
    for commit in commits:
        if _blob_at_commit(root, commit, path) == blob_id:
            return commit
    return None


def _blob_at_commit(root: Path, commit: str, path: str) -> str | None:
    return _git_output(root, ("rev-parse", f"{commit}:{path}"), required=False)


def _resolve_commit(root: Path, ref: str) -> str:
    return _exact_commit(
        _git_output(root, ("rev-parse", "--verify", f"{ref}^{{commit}}"), required=True),
        "target_commit",
    )


def _is_first_parent_ancestor(
    root: Path,
    ancestor: str,
    descendant: str,
) -> bool:
    commits = _git_output(
        root,
        ("rev-list", "--first-parent", descendant),
        required=True,
    )
    assert commits is not None
    return ancestor in commits.splitlines()


def _same_resolved_path(first: str, second: str) -> bool:
    return os.path.normcase(str(Path(first).resolve())) == os.path.normcase(
        str(Path(second).resolve())
    )


def _result_row(
    *,
    path: str,
    role: CheckoutPathRole | None,
    classification: ReconciliationClassification,
    reason_codes: Sequence[str],
    cleanup_eligible: bool,
    prepared_snapshot: CheckoutPathSnapshot | None,
    current_snapshot: CheckoutPathSnapshot | None,
    target_blob_id: str | None,
    matching_history_commit: str | None,
    bytes_observed: bool,
) -> dict[str, object]:
    return {
        "path": path,
        "role": None if role is None else role.value,
        "classification": classification.value,
        "reason_codes": list(reason_codes),
        "cleanup_eligible": cleanup_eligible,
        "prepared_snapshot": (
            None if prepared_snapshot is None else prepared_snapshot.to_dict()
        ),
        "current_snapshot": (
            None if current_snapshot is None else current_snapshot.to_dict()
        ),
        "target_blob_id": target_blob_id,
        "matching_history_commit": matching_history_commit,
        "bytes_observed": bytes_observed,
    }


def _validate_snapshot(value: object, field: str) -> CheckoutPathSnapshot:
    row = _mapping(value, field)
    path = _portable_path(row.get("path"), f"{field}.path")
    exists = row.get("exists")
    if not isinstance(exists, bool):
        raise CheckoutGuardError("CHECKOUT_HANDOFF_SNAPSHOT_EXISTS", path)
    status_code = row.get("status_code")
    if status_code is not None and (
        not isinstance(status_code, str) or len(status_code) != 2
    ):
        raise CheckoutGuardError("CHECKOUT_HANDOFF_SNAPSHOT_STATUS", path)
    raw_sha256 = row.get("raw_sha256")
    normalized_blob_id = row.get("normalized_blob_id")
    size_bytes = row.get("size_bytes")
    if exists:
        _hex_digest(raw_sha256, 64, f"{field}.raw_sha256")
        _hex_digest(normalized_blob_id, 40, f"{field}.normalized_blob_id")
        if isinstance(size_bytes, bool) or not isinstance(size_bytes, int) or size_bytes < 0:
            raise CheckoutGuardError("CHECKOUT_HANDOFF_SNAPSHOT_SIZE", path)
    elif any(value is not None for value in (raw_sha256, normalized_blob_id, size_bytes)):
        raise CheckoutGuardError("CHECKOUT_HANDOFF_SNAPSHOT_MISSING_BYTES", path)
    return CheckoutPathSnapshot(
        path=path,
        exists=exists,
        status_code=status_code,
        raw_sha256=raw_sha256,
        normalized_blob_id=normalized_blob_id,
        size_bytes=size_bytes,
    )


def _validate_identity(value: object, field: str) -> None:
    row = _mapping(value, field)
    _required_text(row.get("workspace_id"), f"{field}.workspace_id")
    _required_text(row.get("checkout_root"), f"{field}.checkout_root")
    _required_text(row.get("git_common_dir"), f"{field}.git_common_dir")
    _exact_commit(row.get("head_commit"), f"{field}.head_commit")
    branch_name = row.get("branch_name")
    if branch_name is not None:
        _required_text(branch_name, f"{field}.branch_name")


def _snapshots_equal(
    first: CheckoutPathSnapshot,
    second: CheckoutPathSnapshot,
) -> bool:
    return (
        first.exists == second.exists
        and first.raw_sha256 == second.raw_sha256
        and first.normalized_blob_id == second.normalized_blob_id
        and first.size_bytes == second.size_bytes
    )


def _payload_checksum(payload: Mapping[str, object], checksum_field: str) -> str:
    canonical = {
        key: value for key, value in payload.items() if key != checksum_field
    }
    raw = json.dumps(
        canonical,
        sort_keys=True,
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def _git_output(
    root: Path,
    args: Sequence[str],
    *,
    required: bool,
) -> str | None:
    try:
        result = subprocess.run(
            ["git", *args],
            cwd=root,
            check=False,
            capture_output=True,
            text=True,
            encoding="utf-8",
        )
    except OSError as exc:
        raise CheckoutGuardError("CHECKOUT_HANDOFF_GIT_EXECUTION", str(exc)) from exc
    if result.returncode != 0:
        if not required:
            return None
        raise CheckoutGuardError(
            "CHECKOUT_HANDOFF_GIT_FAILED",
            result.stderr.strip() or " ".join(args),
        )
    value = result.stdout.strip()
    if not value and required:
        raise CheckoutGuardError("CHECKOUT_HANDOFF_GIT_EMPTY", " ".join(args))
    return value or None


def _paths(values: Sequence[str], field: str) -> tuple[str, ...]:
    rows = tuple(_portable_path(value, field) for value in values)
    if len(rows) != len({row.casefold() for row in rows}):
        raise CheckoutGuardError("CHECKOUT_HANDOFF_PATH_DUPLICATE", field)
    return tuple(sorted(rows, key=str.casefold))


def _portable_path(value: object, field: str) -> str:
    text = _required_text(value, field)
    if "\\" in text or text.endswith("/"):
        raise CheckoutGuardError(
            "CHECKOUT_HANDOFF_PATH_NON_CANONICAL",
            f"{field} must use canonical POSIX syntax",
        )
    path = PurePosixPath(text)
    if (
        path.is_absolute()
        or ":" in path.parts[0]
        or any(part in {"", ".", ".."} for part in path.parts)
        or str(path) != text
    ):
        raise CheckoutGuardError(
            "CHECKOUT_HANDOFF_PATH_UNSAFE",
            f"{field} must be repository relative",
        )
    return text


def _mapping(value: object, field: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise CheckoutGuardError("CHECKOUT_HANDOFF_MAPPING_REQUIRED", field)
    return value


def _strings(value: object, field: str) -> tuple[str, ...]:
    if not isinstance(value, list):
        raise CheckoutGuardError("CHECKOUT_HANDOFF_LIST_REQUIRED", field)
    rows = tuple(_required_text(item, field) for item in value)
    if len(rows) != len(set(rows)):
        raise CheckoutGuardError("CHECKOUT_HANDOFF_LIST_DUPLICATE", field)
    return rows


def _identifier(value: object, field: str) -> str:
    text = _required_text(value, field)
    if len(text) > 128 or not all(
        character.isalnum() or character in "._:-" for character in text
    ):
        raise CheckoutGuardError(
            "CHECKOUT_HANDOFF_IDENTIFIER_INVALID",
            f"{field}={text}",
        )
    return text


def _required_text(value: object, field: str) -> str:
    if not isinstance(value, str) or not value.strip() or value != value.strip():
        raise CheckoutGuardError("CHECKOUT_HANDOFF_TEXT_REQUIRED", field)
    return value


def _exact_commit(value: object, field: str) -> str:
    return _hex_digest(value, 40, field)


def _hex_digest(value: object, length: int, field: str) -> str:
    if (
        not isinstance(value, str)
        or len(value) != length
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise CheckoutGuardError(
            "CHECKOUT_HANDOFF_HEX_REQUIRED",
            field,
        )
    return value


def _aware_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise CheckoutGuardError(
            "CHECKOUT_HANDOFF_TIMEZONE_REQUIRED",
            value.isoformat(),
        )
    return value.astimezone(UTC)
