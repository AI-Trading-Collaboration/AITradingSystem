from __future__ import annotations

import csv
import ctypes
import hashlib
import io
import json
import os
import platform
import re
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, NoReturn

import yaml

from ai_trading_system.platform.artifacts import canonical_json_bytes, write_bytes_atomic

ACL_POLICY_SCHEMA_VERSION = "data_foundation_store_acl_policy.v1"
ACL_ATTESTATION_SCHEMA_VERSION = "data_store_acl_attestation.v1"
ACL_PROBE_SCHEMA_VERSION = "data_store_acl_probe_receipt.v1"
ACL_CLEANUP_RECEIPT_SCHEMA_VERSION = "data_store_acl_cleanup_receipt.v1"
ACL_REHEARSAL_BUNDLE_SCHEMA_VERSION = "data_foundation_acl_rehearsal_bundle.v1"
ACL_PLATFORM_PROFILE = "WINDOWS_NTFS_RESTRICTED_TOKEN.v1"
ACL_VALIDATOR_VERSION = "data_store_acl_validator.v1"

# These are Windows file-security protocol constants, not investment heuristics.
_FILE_ALL_ACCESS = 0x001F01FF
_FILE_READ_EXECUTE = 0x001200A9
_OBJECT_AND_CONTAINER_INHERIT = 0x03
_INHERITED_ACE = 0x10
_ACCESS_ALLOWED_ACE_TYPE = 0x00
_SET_ACCESS = 2
_TRUSTEE_IS_SID = 0
_TRUSTEE_IS_UNKNOWN = 0
_SE_FILE_OBJECT = 1
_DACL_SECURITY_INFORMATION = 0x00000004
_OWNER_SECURITY_INFORMATION = 0x00000001
_GROUP_SECURITY_INFORMATION = 0x00000002
_PROTECTED_DACL_SECURITY_INFORMATION = 0x80000000
_SE_DACL_PROTECTED = 0x1000
_TOKEN_DUPLICATE = 0x0002
_TOKEN_IMPERSONATE = 0x0004
_TOKEN_QUERY = 0x0008
_DISABLE_MAX_PRIVILEGE = 0x0001
_ERROR_ACCESS_DENIED = 5
_SDDL_REVISION_1 = 1
_FILE_ATTRIBUTE_REPARSE_POINT = 0x0400

_SID_RE = re.compile(r"^S-\d(?:-\d+)+$")
_POLICY_FIELDS = {
    "schema_version",
    "policy_id",
    "policy_version",
    "status",
    "owner",
    "rationale",
    "review_condition",
    "scope",
    "claim_boundary",
    "windows_ntfs",
    "posix_local",
}
_BOUNDARY_FIELDS = {
    "production_effect",
    "broker_action",
    "historical_manifest_store_acl_verified",
    "generic_consumer_cutover_allowed",
}
_WINDOWS_FIELDS = {
    "status",
    "writer_selector",
    "reader_sid",
    "recovery_sids",
    "forbidden_broad_write_sids",
    "writer_rights",
    "reader_rights",
    "recovery_rights",
    "inheritance",
    "dacl_protected",
}
_POSIX_FIELDS = {"status", "required_evidence"}
_ATTESTATION_FIELDS = {
    "attestation_id",
    "schema_version",
    "status",
    "generated_at",
    "policy_id",
    "policy_version",
    "policy_sha256",
    "scope",
    "platform_profile",
    "platform",
    "filesystem",
    "validator",
    "store_identity",
    "resolved_store_root",
    "principals",
    "acl_snapshot",
    "probe_receipt",
    "claim_boundary",
    "limitations",
    "production_effect",
    "broker_action",
}
_PROBE_FIELDS = {
    "probe_id",
    "schema_version",
    "writer_create_replace_read_delete",
    "reader_read",
    "reader_write_denied",
    "reader_delete_denied",
    "reader_acl_change_denied",
    "unapproved_read_denied",
    "unapproved_write_denied",
    "child_inheritance_verified",
    "production_effect",
    "broker_action",
}
_CLEANUP_FIELDS = {
    "cleanup_receipt_id",
    "schema_version",
    "store_identity",
    "resolved_store_root",
    "root_existed_before",
    "root_exists_after",
    "cleanup_method",
    "attestation_id",
    "attestation_sha256",
    "production_effect",
    "broker_action",
}
_BUNDLE_FIELDS = {
    "bundle_id",
    "schema_version",
    "status",
    "generated_at",
    "policy_id",
    "policy_version",
    "policy_sha256",
    "attestation",
    "cleanup_receipt",
    "claim_boundary",
    "production_effect",
    "broker_action",
}
_POINTER_FIELDS = {
    "path",
    "artifact_id",
    "schema_version",
    "sha256",
    "size_bytes",
}
_PRINCIPAL_FIELDS = {
    "writer_sid",
    "reader_sid",
    "recovery_sids",
    "forbidden_broad_write_sids",
}
_ACL_SNAPSHOT_FIELDS = {
    "owner_sid",
    "dacl_protected",
    "sddl",
    "security_descriptor_sha256",
    "entries",
}
_ACL_ENTRY_FIELDS = {"sid", "access_mask", "ace_flags", "inherited"}


class DataAccessControlError(RuntimeError):
    def __init__(self, code: str, message: str, *, path: Path | None = None) -> None:
        self.code = code
        self.message = message
        self.path = path
        location = "" if path is None else f" [{path}]"
        super().__init__(f"{code}{location}: {message}")


@dataclass(frozen=True)
class StoreAclPolicy:
    policy_id: str
    policy_version: str
    status: str
    owner: str
    rationale: str
    review_condition: str
    scope: str
    reader_sid: str
    recovery_sids: tuple[str, ...]
    forbidden_broad_write_sids: tuple[str, ...]
    policy_sha256: str
    production_effect: str = "none"
    broker_action: str = "none"
    historical_manifest_store_acl_verified: bool = False
    generic_consumer_cutover_allowed: bool = False


@dataclass(frozen=True)
class AclEntry:
    sid: str
    access_mask: int
    ace_flags: int
    inherited: bool

    def to_dict(self) -> dict[str, object]:
        return {
            "sid": self.sid,
            "access_mask": self.access_mask,
            "ace_flags": self.ace_flags,
            "inherited": self.inherited,
        }


@dataclass(frozen=True)
class StoreAclSnapshot:
    owner_sid: str
    dacl_protected: bool
    sddl: str
    security_descriptor_sha256: str
    entries: tuple[AclEntry, ...]

    def to_dict(self) -> dict[str, object]:
        return {
            "owner_sid": self.owner_sid,
            "dacl_protected": self.dacl_protected,
            "sddl": self.sddl,
            "security_descriptor_sha256": self.security_descriptor_sha256,
            "entries": [entry.to_dict() for entry in self.entries],
        }


class _TrusteeW(ctypes.Structure):
    _fields_ = [
        ("pMultipleTrustee", ctypes.c_void_p),
        ("MultipleTrusteeOperation", ctypes.c_int),
        ("TrusteeForm", ctypes.c_int),
        ("TrusteeType", ctypes.c_int),
        ("ptstrName", ctypes.c_void_p),
    ]


class _ExplicitAccessW(ctypes.Structure):
    _fields_ = [
        ("grfAccessPermissions", ctypes.c_uint32),
        ("grfAccessMode", ctypes.c_int),
        ("grfInheritance", ctypes.c_uint32),
        ("Trustee", _TrusteeW),
    ]


class _AclHeader(ctypes.Structure):
    _fields_ = [
        ("AclRevision", ctypes.c_ubyte),
        ("Sbz1", ctypes.c_ubyte),
        ("AclSize", ctypes.c_ushort),
        ("AceCount", ctypes.c_ushort),
        ("Sbz2", ctypes.c_ushort),
    ]


class _AceHeader(ctypes.Structure):
    _fields_ = [
        ("AceType", ctypes.c_ubyte),
        ("AceFlags", ctypes.c_ubyte),
        ("AceSize", ctypes.c_ushort),
    ]


class _AccessAllowedAce(ctypes.Structure):
    _fields_ = [
        ("Header", _AceHeader),
        ("Mask", ctypes.c_uint32),
        ("SidStart", ctypes.c_uint32),
    ]


class _SecurityDescriptorControl(ctypes.c_ushort):
    pass


class _SidAndAttributes(ctypes.Structure):
    _fields_ = [("Sid", ctypes.c_void_p), ("Attributes", ctypes.c_uint32)]


def load_acl_policy(path: Path) -> StoreAclPolicy:
    try:
        raw_bytes = path.read_bytes()
        raw = yaml.safe_load(raw_bytes.decode("utf-8"))
    except (OSError, UnicodeDecodeError, yaml.YAMLError) as exc:
        raise DataAccessControlError("ACL_POLICY_INVALID", str(exc), path=path) from exc
    root = _mapping(raw, "root")
    if set(root) != {"policy"}:
        _fail("ACL_POLICY_INVALID", "root must contain only policy", path=path)
    payload = _mapping(root.get("policy"), "policy")
    _exact_fields(payload, _POLICY_FIELDS, "ACL_POLICY_INVALID", path=path)
    if payload.get("schema_version") != ACL_POLICY_SCHEMA_VERSION:
        _fail("ACL_POLICY_INVALID", "unsupported schema_version", path=path)
    if payload.get("status") != "PILOT_BASELINE":
        _fail("ACL_POLICY_INVALID", "status must be PILOT_BASELINE", path=path)
    if payload.get("scope") != "ISOLATED_REHEARSAL_ONLY":
        _fail("ACL_POLICY_INVALID", "scope widened", path=path)
    boundary = _mapping(payload.get("claim_boundary"), "claim_boundary")
    _exact_fields(boundary, _BOUNDARY_FIELDS, "ACL_POLICY_INVALID", path=path)
    if boundary != {
        "production_effect": "none",
        "broker_action": "none",
        "historical_manifest_store_acl_verified": False,
        "generic_consumer_cutover_allowed": False,
    }:
        _fail("ACL_POLICY_INVALID", "claim boundary widened", path=path)
    windows = _mapping(payload.get("windows_ntfs"), "windows_ntfs")
    _exact_fields(windows, _WINDOWS_FIELDS, "ACL_POLICY_INVALID", path=path)
    if (
        windows.get("status") != "SUPPORTED_FOR_ISOLATED_REHEARSAL"
        or windows.get("writer_selector") != "CURRENT_PROCESS_USER"
        or windows.get("writer_rights") != "FULL_CONTROL"
        or windows.get("reader_rights") != "READ_EXECUTE"
        or windows.get("recovery_rights") != "FULL_CONTROL"
        or windows.get("inheritance") != "OBJECT_AND_CONTAINER"
        or windows.get("dacl_protected") is not True
    ):
        _fail("ACL_POLICY_INVALID", "Windows ACL invariant drift", path=path)
    reader_sid = _sid(windows.get("reader_sid"), "reader_sid")
    recovery_sids = _sids(windows.get("recovery_sids"), "recovery_sids")
    forbidden_sids = _sids(
        windows.get("forbidden_broad_write_sids"),
        "forbidden_broad_write_sids",
    )
    if reader_sid != "S-1-5-32-545":
        _fail("ACL_POLICY_INVALID", "reader must be BUILTIN Users", path=path)
    if recovery_sids != ("S-1-5-18", "S-1-5-32-544"):
        _fail("ACL_POLICY_INVALID", "recovery SID drift", path=path)
    if forbidden_sids != ("S-1-1-0", "S-1-5-11"):
        _fail("ACL_POLICY_INVALID", "forbidden broad SID drift", path=path)
    if set((reader_sid, *recovery_sids)) & set(forbidden_sids):
        _fail("ACL_POLICY_INVALID", "principal roles overlap", path=path)
    posix = _mapping(payload.get("posix_local"), "posix_local")
    _exact_fields(posix, _POSIX_FIELDS, "ACL_POLICY_INVALID", path=path)
    if posix.get("status") != "BLOCKED_PENDING_DISTINCT_IDENTITY_REHEARSAL":
        _fail("ACL_POLICY_INVALID", "POSIX status widened", path=path)
    if _texts(posix.get("required_evidence"), "required_evidence") != (
        "exact_writer_uid",
        "exact_reader_gid",
        "independent_reader_token",
        "negative_write_delete_chmod_probe",
    ):
        _fail("ACL_POLICY_INVALID", "POSIX evidence contract drift", path=path)
    return StoreAclPolicy(
        policy_id=_text(payload.get("policy_id"), "policy_id"),
        policy_version=_text(payload.get("policy_version"), "policy_version"),
        status="PILOT_BASELINE",
        owner=_text(payload.get("owner"), "owner"),
        rationale=_text(payload.get("rationale"), "rationale"),
        review_condition=_text(payload.get("review_condition"), "review_condition"),
        scope="ISOLATED_REHEARSAL_ONLY",
        reader_sid=reader_sid,
        recovery_sids=recovery_sids,
        forbidden_broad_write_sids=forbidden_sids,
        policy_sha256=hashlib.sha256(raw_bytes).hexdigest(),
    )


def apply_isolated_store_acl(
    store_root: Path,
    *,
    allowed_parent: Path,
    policy: StoreAclPolicy,
) -> StoreAclSnapshot:
    root = _isolated_empty_root(store_root, allowed_parent)
    _require_windows_ntfs(root)
    writer_sid = current_windows_user_sid()
    expected = _expected_entries(policy, writer_sid)
    _set_windows_dacl(root, expected)
    snapshot = inspect_store_acl(root)
    validate_acl_snapshot(snapshot, policy=policy, writer_sid=writer_sid, child=False)
    return snapshot


def inspect_store_acl(store_root: Path) -> StoreAclSnapshot:
    root = _resolved_plain_directory(store_root)
    if os.name != "nt":
        _fail("ACL_PLATFORM_UNSUPPORTED", "only Windows NTFS rehearsal is implemented", path=root)
    advapi32, kernel32 = _windows_libraries()
    owner = ctypes.c_void_p()
    dacl = ctypes.c_void_p()
    descriptor = ctypes.c_void_p()
    get_info = advapi32.GetNamedSecurityInfoW
    get_info.argtypes = [
        ctypes.c_wchar_p,
        ctypes.c_uint32,
        ctypes.c_uint32,
        ctypes.POINTER(ctypes.c_void_p),
        ctypes.c_void_p,
        ctypes.POINTER(ctypes.c_void_p),
        ctypes.c_void_p,
        ctypes.POINTER(ctypes.c_void_p),
    ]
    get_info.restype = ctypes.c_uint32
    result = int(
        get_info(
            str(root),
            _SE_FILE_OBJECT,
            _OWNER_SECURITY_INFORMATION
            | _GROUP_SECURITY_INFORMATION
            | _DACL_SECURITY_INFORMATION,
            ctypes.byref(owner),
            None,
            ctypes.byref(dacl),
            None,
            ctypes.byref(descriptor),
        )
    )
    if result != 0 or not descriptor.value or not dacl.value or not owner.value:
        _fail("ACL_INSPECTION_FAILED", f"GetNamedSecurityInfoW={result}", path=root)
    try:
        owner_sid = _sid_to_text(owner)
        entries = _read_acl_entries(dacl)
        control = _SecurityDescriptorControl()
        revision = ctypes.c_uint32()
        get_control = advapi32.GetSecurityDescriptorControl
        get_control.argtypes = [
            ctypes.c_void_p,
            ctypes.POINTER(_SecurityDescriptorControl),
            ctypes.POINTER(ctypes.c_uint32),
        ]
        get_control.restype = ctypes.c_int
        if not get_control(descriptor, ctypes.byref(control), ctypes.byref(revision)):
            _fail("ACL_INSPECTION_FAILED", "GetSecurityDescriptorControl failed", path=root)
        sddl = _security_descriptor_to_sddl(descriptor)
        return StoreAclSnapshot(
            owner_sid=owner_sid,
            dacl_protected=bool(control.value & _SE_DACL_PROTECTED),
            sddl=sddl,
            security_descriptor_sha256=hashlib.sha256(sddl.encode("utf-8")).hexdigest(),
            entries=entries,
        )
    finally:
        kernel32.LocalFree(descriptor)


def validate_acl_snapshot(
    snapshot: StoreAclSnapshot,
    *,
    policy: StoreAclPolicy,
    writer_sid: str,
    child: bool,
) -> None:
    if not snapshot.dacl_protected and not child:
        _fail("ACL_POLICY_MISMATCH", "root DACL is not protected")
    expected_masks = {
        writer_sid: _FILE_ALL_ACCESS,
        policy.reader_sid: _FILE_READ_EXECUTE,
        **{sid: _FILE_ALL_ACCESS for sid in policy.recovery_sids},
    }
    observed: dict[str, AclEntry] = {}
    for entry in snapshot.entries:
        if entry.sid in observed:
            _fail("ACL_POLICY_MISMATCH", f"duplicate ACE for {entry.sid}")
        observed[entry.sid] = entry
    for sid in policy.forbidden_broad_write_sids:
        broad_entry = observed.get(sid)
        if broad_entry is not None and _mask_can_mutate(broad_entry.access_mask):
            _fail("ACL_BROAD_WRITE_DETECTED", f"broad principal can mutate sid={sid}")
    if set(observed) != set(expected_masks):
        _fail(
            "ACL_POLICY_MISMATCH",
            f"principal set differs expected={sorted(expected_masks)} actual={sorted(observed)}",
        )
    for sid, expected_mask in expected_masks.items():
        entry = observed[sid]
        if entry.access_mask != expected_mask:
            _fail(
                "ACL_POLICY_MISMATCH",
                f"mask differs sid={sid} expected={expected_mask} actual={entry.access_mask}",
            )
        if child:
            if not entry.inherited:
                _fail("ACL_POLICY_MISMATCH", f"child ACE is not inherited sid={sid}")
        elif (
            entry.ace_flags != _OBJECT_AND_CONTAINER_INHERIT
            or entry.inherited
        ):
            _fail("ACL_POLICY_MISMATCH", f"root inheritance flags differ sid={sid}")
def run_acl_enforcement_probe(
    store_root: Path,
    *,
    policy: StoreAclPolicy,
) -> dict[str, object]:
    root = _resolved_plain_directory(store_root)
    writer_sid = current_windows_user_sid()
    validate_acl_snapshot(
        inspect_store_acl(root),
        policy=policy,
        writer_sid=writer_sid,
        child=False,
    )
    child = root / "probe_child"
    child.mkdir()
    source = child / "writer_source.bin"
    write_bytes_atomic(source, b"acl-writer-source")
    if source.read_bytes() != b"acl-writer-source":
        _fail("ACL_WRITER_PROBE_FAILED", "writer read mismatch", path=source)
    write_bytes_atomic(source, b"acl-writer-replacement")
    if source.read_bytes() != b"acl-writer-replacement":
        _fail("ACL_WRITER_PROBE_FAILED", "writer replace mismatch", path=source)
    child_snapshot = inspect_store_acl(child)
    validate_acl_snapshot(
        child_snapshot,
        policy=policy,
        writer_sid=writer_sid,
        child=True,
    )
    reader_create = child / "reader_write_must_fail.bin"
    unapproved_create = child / "unapproved_write_must_fail.bin"
    try:
        with _restricted_windows_token(policy.reader_sid):
            reader_read = source.read_bytes() == b"acl-writer-replacement"
            reader_write_denied = _write_is_denied(reader_create)
            reader_delete_denied = _delete_is_denied(source)
            reader_acl_change_denied = _acl_change_is_denied(root)
        with _restricted_windows_token("S-1-1-0"):
            unapproved_read_denied = _read_is_denied(source)
            unapproved_write_denied = _write_is_denied(unapproved_create)
    finally:
        for candidate in (reader_create, unapproved_create, source):
            if candidate.exists():
                candidate.unlink()
        child.rmdir()
    receipt_body: dict[str, object] = {
        "schema_version": ACL_PROBE_SCHEMA_VERSION,
        "writer_create_replace_read_delete": True,
        "reader_read": reader_read,
        "reader_write_denied": reader_write_denied,
        "reader_delete_denied": reader_delete_denied,
        "reader_acl_change_denied": reader_acl_change_denied,
        "unapproved_read_denied": unapproved_read_denied,
        "unapproved_write_denied": unapproved_write_denied,
        "child_inheritance_verified": True,
        "production_effect": "none",
        "broker_action": "none",
    }
    if not all(
        value is True
        for key, value in receipt_body.items()
        if key
        not in {
            "schema_version",
            "production_effect",
            "broker_action",
        }
    ):
        _fail("ACL_ENFORCEMENT_PROBE_FAILED", "one or more native permission probes failed")
    probe_id = f"acl_probe_{_digest(receipt_body)[:32]}"
    return {"probe_id": probe_id, **receipt_body}


def build_acl_attestation(
    store_root: Path,
    *,
    policy: StoreAclPolicy,
    generated_at: datetime,
) -> dict[str, object]:
    if generated_at.tzinfo is None or generated_at.utcoffset() is None:
        raise ValueError("generated_at must be timezone-aware")
    root = _resolved_plain_directory(store_root)
    profile = _require_windows_ntfs(root)
    writer_sid = current_windows_user_sid()
    snapshot = inspect_store_acl(root)
    validate_acl_snapshot(snapshot, policy=policy, writer_sid=writer_sid, child=False)
    probe = run_acl_enforcement_probe(root, policy=policy)
    stat_result = root.stat()
    identity_body = {
        "resolved_store_root": root.as_posix(),
        "device": int(stat_result.st_dev),
        "inode": int(stat_result.st_ino),
        "security_descriptor_sha256": snapshot.security_descriptor_sha256,
    }
    store_identity = f"acl_store_{_digest(identity_body)[:32]}"
    body: dict[str, object] = {
        "schema_version": ACL_ATTESTATION_SCHEMA_VERSION,
        "status": "PASS",
        "generated_at": generated_at.astimezone(UTC).isoformat(),
        "policy_id": policy.policy_id,
        "policy_version": policy.policy_version,
        "policy_sha256": policy.policy_sha256,
        "scope": policy.scope,
        "platform_profile": ACL_PLATFORM_PROFILE,
        "platform": platform.system(),
        "filesystem": profile,
        "validator": _validator_binding(),
        "store_identity": store_identity,
        "resolved_store_root": root.as_posix(),
        "principals": {
            "writer_sid": writer_sid,
            "reader_sid": policy.reader_sid,
            "recovery_sids": list(policy.recovery_sids),
            "forbidden_broad_write_sids": list(policy.forbidden_broad_write_sids),
        },
        "acl_snapshot": snapshot.to_dict(),
        "probe_receipt": probe,
        "claim_boundary": {
            "historical_manifest_store_acl_verified": False,
            "generic_consumer_cutover_allowed": False,
        },
        "limitations": [
            "exact_isolated_store_only",
            "restricted_tokens_validate_kernel_access_intersection_not_distinct_account_logon",
            "same_principal_adversarial_mutation_not_prevented",
            "administrator_system_offline_and_network_store_out_of_scope",
        ],
        "production_effect": "none",
        "broker_action": "none",
    }
    attestation_id = f"store_acl_attestation_{_digest(body)[:32]}"
    return {"attestation_id": attestation_id, **body}


def validate_acl_attestation(
    payload: Mapping[str, object],
    *,
    store_root: Path,
    policy: StoreAclPolicy,
) -> None:
    _validate_acl_attestation_payload(payload, policy=policy)
    root = _resolved_plain_directory(store_root)
    if payload.get("resolved_store_root") != root.as_posix():
        _fail("ACL_ATTESTATION_INVALID", "store root mismatch")
    generated_at = _aware_datetime(payload.get("generated_at"), "generated_at")
    expected = build_acl_attestation(root, policy=policy, generated_at=generated_at)
    if dict(payload) != expected:
        _fail("ACL_ATTESTATION_INVALID", "live ACL, principals, probe, or identity drift")


def validate_acl_rehearsal_bundle(
    bundle_path: Path,
    *,
    policy_path: Path,
) -> dict[str, object]:
    policy = load_acl_policy(policy_path)
    bundle_root = bundle_path.resolve(strict=True).parent
    bundle = _canonical_json_file(bundle_path, "ACL_BUNDLE_INVALID")
    _exact_fields(bundle, _BUNDLE_FIELDS, "ACL_BUNDLE_INVALID", path=bundle_path)
    bundle_body = dict(bundle)
    bundle_id = _text(bundle_body.pop("bundle_id"), "bundle_id")
    if bundle_id != f"data_foundation_acl_bundle_{_digest(bundle_body)[:32]}":
        _fail("ACL_BUNDLE_INVALID", "bundle id mismatch", path=bundle_path)
    if (
        bundle.get("schema_version") != ACL_REHEARSAL_BUNDLE_SCHEMA_VERSION
        or bundle.get("status") != "PASS"
        or bundle.get("policy_id") != policy.policy_id
        or bundle.get("policy_version") != policy.policy_version
        or bundle.get("policy_sha256") != policy.policy_sha256
        or bundle.get("production_effect") != "none"
        or bundle.get("broker_action") != "none"
        or bundle.get("claim_boundary")
        != {
            "historical_manifest_store_acl_verified": False,
            "generic_consumer_cutover_allowed": False,
        }
    ):
        _fail("ACL_BUNDLE_INVALID", "bundle identity or claim boundary mismatch", path=bundle_path)
    _aware_datetime(bundle.get("generated_at"), "generated_at")
    attestation_pointer = _mapping(bundle.get("attestation"), "attestation")
    cleanup_pointer = _mapping(bundle.get("cleanup_receipt"), "cleanup_receipt")
    attestation = _read_pointer_payload(
        bundle_root,
        attestation_pointer,
        expected_schema=ACL_ATTESTATION_SCHEMA_VERSION,
        code="ACL_BUNDLE_INVALID",
    )
    cleanup = _read_pointer_payload(
        bundle_root,
        cleanup_pointer,
        expected_schema=ACL_CLEANUP_RECEIPT_SCHEMA_VERSION,
        code="ACL_BUNDLE_INVALID",
    )
    _validate_acl_attestation_payload(attestation, policy=policy)
    _validate_cleanup_receipt(
        cleanup,
        attestation=attestation,
        attestation_sha256=_text(attestation_pointer.get("sha256"), "attestation.sha256"),
    )
    if attestation_pointer.get("artifact_id") != attestation.get("attestation_id"):
        _fail("ACL_BUNDLE_INVALID", "attestation pointer id mismatch", path=bundle_path)
    if cleanup_pointer.get("artifact_id") != cleanup.get("cleanup_receipt_id"):
        _fail("ACL_BUNDLE_INVALID", "cleanup pointer id mismatch", path=bundle_path)
    resolved_store_root = Path(_text(cleanup.get("resolved_store_root"), "resolved_store_root"))
    if resolved_store_root.exists():
        _fail(
            "ACL_BUNDLE_INVALID",
            "cleaned rehearsal root has reappeared",
            path=resolved_store_root,
        )
    return dict(bundle)


def _validate_acl_attestation_payload(
    payload: Mapping[str, object],
    *,
    policy: StoreAclPolicy,
) -> None:
    if set(payload) != _ATTESTATION_FIELDS:
        _fail("ACL_ATTESTATION_INVALID", "attestation field set differs")
    body = dict(payload)
    attestation_id = _text(body.pop("attestation_id"), "attestation_id")
    if attestation_id != f"store_acl_attestation_{_digest(body)[:32]}":
        _fail("ACL_ATTESTATION_INVALID", "attestation id mismatch")
    if (
        payload.get("schema_version") != ACL_ATTESTATION_SCHEMA_VERSION
        or payload.get("status") != "PASS"
        or payload.get("policy_id") != policy.policy_id
        or payload.get("policy_version") != policy.policy_version
        or payload.get("policy_sha256") != policy.policy_sha256
        or payload.get("scope") != policy.scope
        or payload.get("platform_profile") != ACL_PLATFORM_PROFILE
        or payload.get("validator") != _validator_binding()
        or payload.get("production_effect") != "none"
        or payload.get("broker_action") != "none"
    ):
        _fail("ACL_ATTESTATION_INVALID", "identity or claim boundary mismatch")
    _aware_datetime(payload.get("generated_at"), "generated_at")
    claim_boundary = _mapping(payload.get("claim_boundary"), "claim_boundary")
    if claim_boundary != {
        "historical_manifest_store_acl_verified": False,
        "generic_consumer_cutover_allowed": False,
    }:
        _fail("ACL_ATTESTATION_INVALID", "claim boundary widened")
    principals = _mapping(payload.get("principals"), "principals")
    _exact_fields(principals, _PRINCIPAL_FIELDS, "ACL_ATTESTATION_INVALID")
    if (
        principals.get("reader_sid") != policy.reader_sid
        or tuple(_texts(principals.get("recovery_sids"), "recovery_sids"))
        != policy.recovery_sids
        or tuple(
            _texts(
                principals.get("forbidden_broad_write_sids"),
                "forbidden_broad_write_sids",
            )
        )
        != policy.forbidden_broad_write_sids
        or not _SID_RE.fullmatch(_text(principals.get("writer_sid"), "writer_sid"))
    ):
        _fail("ACL_ATTESTATION_INVALID", "principal binding mismatch")
    probe = _mapping(payload.get("probe_receipt"), "probe_receipt")
    _exact_fields(probe, _PROBE_FIELDS, "ACL_ATTESTATION_INVALID")
    probe_body = dict(probe)
    probe_id = _text(probe_body.pop("probe_id"), "probe_id")
    if probe_id != f"acl_probe_{_digest(probe_body)[:32]}":
        _fail("ACL_ATTESTATION_INVALID", "probe id mismatch")
    if (
        probe.get("schema_version") != ACL_PROBE_SCHEMA_VERSION
        or probe.get("production_effect") != "none"
        or probe.get("broker_action") != "none"
        or not all(
            value is True
            for key, value in probe.items()
            if key
            not in {
                "probe_id",
                "schema_version",
                "production_effect",
                "broker_action",
            }
        )
    ):
        _fail("ACL_ATTESTATION_INVALID", "probe receipt is not strict PASS")
    snapshot = _mapping(payload.get("acl_snapshot"), "acl_snapshot")
    _exact_fields(snapshot, _ACL_SNAPSHOT_FIELDS, "ACL_ATTESTATION_INVALID")
    sddl = _text(snapshot.get("sddl"), "sddl")
    descriptor_sha = _text(
        snapshot.get("security_descriptor_sha256"),
        "security_descriptor_sha256",
    )
    if (
        not re.fullmatch(r"[0-9a-f]{64}", descriptor_sha)
        or hashlib.sha256(sddl.encode("utf-8")).hexdigest() != descriptor_sha
    ):
        _fail("ACL_ATTESTATION_INVALID", "security descriptor digest invalid")
    raw_entries = snapshot.get("entries")
    if not isinstance(raw_entries, Sequence) or isinstance(raw_entries, (str, bytes)):
        _fail("ACL_ATTESTATION_INVALID", "ACL entries must be a sequence")
    entries: list[AclEntry] = []
    for raw_entry in raw_entries:
        entry = _mapping(raw_entry, "acl entry")
        _exact_fields(entry, _ACL_ENTRY_FIELDS, "ACL_ATTESTATION_INVALID")
        mask = entry.get("access_mask")
        flags = entry.get("ace_flags")
        inherited = entry.get("inherited")
        if (
            not isinstance(mask, int)
            or isinstance(mask, bool)
            or not isinstance(flags, int)
            or isinstance(flags, bool)
            or not isinstance(inherited, bool)
        ):
            _fail("ACL_ATTESTATION_INVALID", "ACL entry types invalid")
        entries.append(
            AclEntry(
                sid=_sid(entry.get("sid"), "sid"),
                access_mask=mask,
                ace_flags=flags,
                inherited=inherited,
            )
        )
    writer_sid = _sid(principals.get("writer_sid"), "writer_sid")
    snapshot_model = StoreAclSnapshot(
        owner_sid=_sid(snapshot.get("owner_sid"), "owner_sid"),
        dacl_protected=snapshot.get("dacl_protected") is True,
        sddl=sddl,
        security_descriptor_sha256=descriptor_sha,
        entries=tuple(entries),
    )
    if snapshot_model.owner_sid != writer_sid:
        _fail("ACL_ATTESTATION_INVALID", "store owner/writer mismatch")
    validate_acl_snapshot(
        snapshot_model,
        policy=policy,
        writer_sid=writer_sid,
        child=False,
    )
    if payload.get("platform") != "Windows" or payload.get("filesystem") != "NTFS":
        _fail("ACL_ATTESTATION_INVALID", "platform/filesystem scope mismatch")
    if payload.get("limitations") != [
        "exact_isolated_store_only",
        "restricted_tokens_validate_kernel_access_intersection_not_distinct_account_logon",
        "same_principal_adversarial_mutation_not_prevented",
        "administrator_system_offline_and_network_store_out_of_scope",
    ]:
        _fail("ACL_ATTESTATION_INVALID", "limitation set differs")


def _validate_cleanup_receipt(
    payload: Mapping[str, object],
    *,
    attestation: Mapping[str, object],
    attestation_sha256: str,
) -> None:
    _exact_fields(payload, _CLEANUP_FIELDS, "ACL_CLEANUP_RECEIPT_INVALID")
    body = dict(payload)
    receipt_id = _text(body.pop("cleanup_receipt_id"), "cleanup_receipt_id")
    if receipt_id != f"acl_cleanup_{_digest(body)[:32]}":
        _fail("ACL_CLEANUP_RECEIPT_INVALID", "cleanup receipt id mismatch")
    if (
        payload.get("schema_version") != ACL_CLEANUP_RECEIPT_SCHEMA_VERSION
        or payload.get("store_identity") != attestation.get("store_identity")
        or payload.get("resolved_store_root") != attestation.get("resolved_store_root")
        or payload.get("root_existed_before") is not True
        or payload.get("root_exists_after") is not False
        or payload.get("cleanup_method") != "trusted_writer_shutil_rmtree"
        or payload.get("attestation_id") != attestation.get("attestation_id")
        or payload.get("attestation_sha256") != attestation_sha256
        or payload.get("production_effect") != "none"
        or payload.get("broker_action") != "none"
    ):
        _fail("ACL_CLEANUP_RECEIPT_INVALID", "cleanup receipt binding mismatch")


def current_windows_user_sid() -> str:
    if os.name != "nt":
        _fail("ACL_PLATFORM_UNSUPPORTED", "Windows SID requested on non-Windows platform")
    import subprocess

    try:
        completed = subprocess.run(
            ["whoami.exe", "/user", "/fo", "csv", "/nh"],
            check=True,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="strict",
        )
        row = next(csv.reader(io.StringIO(completed.stdout)))
    except (OSError, subprocess.SubprocessError, StopIteration, csv.Error) as exc:
        raise DataAccessControlError("ACL_PRINCIPAL_RESOLUTION_FAILED", str(exc)) from exc
    matches = [value.strip() for value in row if _SID_RE.fullmatch(value.strip())]
    if len(matches) != 1:
        _fail("ACL_PRINCIPAL_RESOLUTION_FAILED", "whoami returned ambiguous SID")
    return matches[0]


def _set_windows_dacl(root: Path, entries: tuple[AclEntry, ...]) -> None:
    advapi32, kernel32 = _windows_libraries()
    sid_pointers: list[ctypes.c_void_p] = []
    new_acl = ctypes.c_void_p()
    try:
        for entry in entries:
            sid_pointers.append(_text_to_sid(entry.sid))
        explicit_array = (_ExplicitAccessW * len(entries))()
        for index, (entry, sid_pointer) in enumerate(zip(entries, sid_pointers, strict=True)):
            explicit_array[index].grfAccessPermissions = entry.access_mask
            explicit_array[index].grfAccessMode = _SET_ACCESS
            explicit_array[index].grfInheritance = entry.ace_flags
            explicit_array[index].Trustee = _TrusteeW(
                None,
                0,
                _TRUSTEE_IS_SID,
                _TRUSTEE_IS_UNKNOWN,
                sid_pointer,
            )
        set_entries = advapi32.SetEntriesInAclW
        set_entries.argtypes = [
            ctypes.c_uint32,
            ctypes.POINTER(_ExplicitAccessW),
            ctypes.c_void_p,
            ctypes.POINTER(ctypes.c_void_p),
        ]
        set_entries.restype = ctypes.c_uint32
        result = int(set_entries(len(entries), explicit_array, None, ctypes.byref(new_acl)))
        if result != 0 or not new_acl.value:
            _fail("ACL_APPLY_FAILED", f"SetEntriesInAclW={result}", path=root)
        set_info = advapi32.SetNamedSecurityInfoW
        set_info.argtypes = [
            ctypes.c_wchar_p,
            ctypes.c_uint32,
            ctypes.c_uint32,
            ctypes.c_void_p,
            ctypes.c_void_p,
            ctypes.c_void_p,
            ctypes.c_void_p,
        ]
        set_info.restype = ctypes.c_uint32
        result = int(
            set_info(
                str(root),
                _SE_FILE_OBJECT,
                _DACL_SECURITY_INFORMATION | _PROTECTED_DACL_SECURITY_INFORMATION,
                None,
                None,
                new_acl,
                None,
            )
        )
        if result != 0:
            _fail("ACL_APPLY_FAILED", f"SetNamedSecurityInfoW={result}", path=root)
    finally:
        if new_acl.value:
            kernel32.LocalFree(new_acl)
        for pointer in sid_pointers:
            if pointer.value:
                kernel32.LocalFree(pointer)


def _read_acl_entries(dacl: ctypes.c_void_p) -> tuple[AclEntry, ...]:
    advapi32, _ = _windows_libraries()
    header = ctypes.cast(dacl, ctypes.POINTER(_AclHeader)).contents
    get_ace = advapi32.GetAce
    get_ace.argtypes = [
        ctypes.c_void_p,
        ctypes.c_uint32,
        ctypes.POINTER(ctypes.c_void_p),
    ]
    get_ace.restype = ctypes.c_int
    entries: list[AclEntry] = []
    for index in range(header.AceCount):
        ace_pointer = ctypes.c_void_p()
        if not get_ace(dacl, index, ctypes.byref(ace_pointer)) or not ace_pointer.value:
            _fail("ACL_INSPECTION_FAILED", f"GetAce failed index={index}")
        ace = ctypes.cast(ace_pointer, ctypes.POINTER(_AccessAllowedAce)).contents
        if ace.Header.AceType != _ACCESS_ALLOWED_ACE_TYPE:
            _fail("ACL_POLICY_MISMATCH", f"non-allow ACE type={ace.Header.AceType}")
        sid_address = ace_pointer.value + _AccessAllowedAce.SidStart.offset
        entries.append(
            AclEntry(
                sid=_sid_to_text(ctypes.c_void_p(sid_address)),
                access_mask=int(ace.Mask),
                ace_flags=int(ace.Header.AceFlags & ~_INHERITED_ACE),
                inherited=bool(ace.Header.AceFlags & _INHERITED_ACE),
            )
        )
    return tuple(entries)


@contextmanager
def _restricted_windows_token(restricting_sid: str) -> Iterator[None]:
    advapi32, kernel32 = _windows_libraries()
    process_token = ctypes.c_void_p()
    restricted_token = ctypes.c_void_p()
    sid_pointer = _text_to_sid(restricting_sid)
    open_token = advapi32.OpenProcessToken
    open_token.argtypes = [
        ctypes.c_void_p,
        ctypes.c_uint32,
        ctypes.POINTER(ctypes.c_void_p),
    ]
    open_token.restype = ctypes.c_int
    create_token = advapi32.CreateRestrictedToken
    create_token.argtypes = [
        ctypes.c_void_p,
        ctypes.c_uint32,
        ctypes.c_uint32,
        ctypes.c_void_p,
        ctypes.c_uint32,
        ctypes.c_void_p,
        ctypes.c_uint32,
        ctypes.POINTER(_SidAndAttributes),
        ctypes.POINTER(ctypes.c_void_p),
    ]
    create_token.restype = ctypes.c_int
    impersonate = advapi32.ImpersonateLoggedOnUser
    impersonate.argtypes = [ctypes.c_void_p]
    impersonate.restype = ctypes.c_int
    revert = advapi32.RevertToSelf
    revert.argtypes = []
    revert.restype = ctypes.c_int
    restriction = _SidAndAttributes(sid_pointer, 0)
    impersonated = False
    try:
        if not open_token(
            kernel32.GetCurrentProcess(),
            _TOKEN_DUPLICATE | _TOKEN_IMPERSONATE | _TOKEN_QUERY,
            ctypes.byref(process_token),
        ):
            _fail("ACL_RESTRICTED_TOKEN_UNAVAILABLE", "OpenProcessToken failed")
        if not create_token(
            process_token,
            _DISABLE_MAX_PRIVILEGE,
            0,
            None,
            0,
            None,
            1,
            ctypes.byref(restriction),
            ctypes.byref(restricted_token),
        ):
            _fail("ACL_RESTRICTED_TOKEN_UNAVAILABLE", "CreateRestrictedToken failed")
        if not impersonate(restricted_token):
            _fail("ACL_RESTRICTED_TOKEN_UNAVAILABLE", "ImpersonateLoggedOnUser failed")
        impersonated = True
        yield
    finally:
        if impersonated:
            revert()
        if restricted_token.value:
            kernel32.CloseHandle(restricted_token)
        if process_token.value:
            kernel32.CloseHandle(process_token)
        if sid_pointer.value:
            kernel32.LocalFree(sid_pointer)


def _acl_change_is_denied(path: Path) -> bool:
    advapi32, kernel32 = _windows_libraries()
    dacl = ctypes.c_void_p()
    descriptor = ctypes.c_void_p()
    get_info = advapi32.GetNamedSecurityInfoW
    result = int(
        get_info(
            str(path),
            _SE_FILE_OBJECT,
            _DACL_SECURITY_INFORMATION,
            None,
            None,
            ctypes.byref(dacl),
            None,
            ctypes.byref(descriptor),
        )
    )
    if result != 0 or not descriptor.value or not dacl.value:
        _fail("ACL_INSPECTION_FAILED", f"GetNamedSecurityInfoW={result}", path=path)
    try:
        set_info = advapi32.SetNamedSecurityInfoW
        result = int(
            set_info(
                str(path),
                _SE_FILE_OBJECT,
                _DACL_SECURITY_INFORMATION | _PROTECTED_DACL_SECURITY_INFORMATION,
                None,
                None,
                dacl,
                None,
            )
        )
        return result == _ERROR_ACCESS_DENIED
    finally:
        kernel32.LocalFree(descriptor)


def _write_is_denied(path: Path) -> bool:
    # This is a native access-denial probe, not an artifact writer: if the OS
    # unexpectedly permits creation, the caller deletes the probe and fails closed.
    try:
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except PermissionError:
        return True
    except OSError as exc:
        return getattr(exc, "winerror", None) == _ERROR_ACCESS_DENIED
    os.close(descriptor)
    return False


def _read_is_denied(path: Path) -> bool:
    try:
        path.read_bytes()
    except PermissionError:
        return True
    except OSError as exc:
        return getattr(exc, "winerror", None) == _ERROR_ACCESS_DENIED
    return False


def _delete_is_denied(path: Path) -> bool:
    try:
        path.unlink()
    except PermissionError:
        return True
    except OSError as exc:
        return getattr(exc, "winerror", None) == _ERROR_ACCESS_DENIED
    return False


def _expected_entries(policy: StoreAclPolicy, writer_sid: str) -> tuple[AclEntry, ...]:
    raw = (
        (writer_sid, _FILE_ALL_ACCESS),
        (policy.recovery_sids[0], _FILE_ALL_ACCESS),
        (policy.recovery_sids[1], _FILE_ALL_ACCESS),
        (policy.reader_sid, _FILE_READ_EXECUTE),
    )
    if len({sid for sid, _ in raw}) != len(raw):
        _fail("ACL_POLICY_INVALID", "resolved principal roles overlap")
    return tuple(
        AclEntry(
            sid=sid,
            access_mask=mask,
            ace_flags=_OBJECT_AND_CONTAINER_INHERIT,
            inherited=False,
        )
        for sid, mask in raw
    )


def _mask_can_mutate(mask: int) -> bool:
    mutation_bits = 0x00000002 | 0x00000004 | 0x00010000 | 0x00040000 | 0x00080000
    return bool(mask & mutation_bits)


def _isolated_empty_root(store_root: Path, allowed_parent: Path) -> Path:
    root = _resolved_plain_directory(store_root)
    parent = _resolved_plain_directory(allowed_parent)
    if root == parent or not root.is_relative_to(parent):
        _fail("ACL_SCOPE_VIOLATION", "store root is outside allowed parent", path=root)
    if any(root.iterdir()):
        _fail("ACL_SCOPE_VIOLATION", "ACL apply requires an empty isolated root", path=root)
    return root


def _resolved_plain_directory(path: Path) -> Path:
    try:
        if path.is_symlink():
            _fail("ACL_PATH_INVALID", "symlink is not allowed", path=path)
        root = path.resolve(strict=True)
        stat_result = root.stat()
    except OSError as exc:
        raise DataAccessControlError("ACL_PATH_INVALID", str(exc), path=path) from exc
    if not root.is_dir():
        _fail("ACL_PATH_INVALID", "path is not a directory", path=root)
    attributes = getattr(stat_result, "st_file_attributes", 0)
    if attributes & _FILE_ATTRIBUTE_REPARSE_POINT:
        _fail("ACL_PATH_INVALID", "reparse point is not allowed", path=root)
    return root


def _require_windows_ntfs(root: Path) -> str:
    if os.name != "nt":
        _fail(
            "ACL_PLATFORM_UNSUPPORTED",
            "POSIX requires a separately provisioned distinct-identity rehearsal",
            path=root,
        )
    from ai_trading_system.data.durability import probe_filesystem_durability

    profile = probe_filesystem_durability(root)
    if not profile.supported or profile.filesystem.upper() != "NTFS":
        _fail(
            "ACL_PLATFORM_UNSUPPORTED",
            f"requires supported local NTFS, got {profile.filesystem}/{profile.storage_scope}",
            path=root,
        )
    return profile.filesystem.upper()


def _windows_libraries() -> tuple[Any, Any]:
    if os.name != "nt":
        _fail("ACL_PLATFORM_UNSUPPORTED", "Windows API requested on non-Windows platform")
    loader = ctypes.WinDLL
    advapi32 = loader("advapi32", use_last_error=True)
    kernel32 = loader("kernel32", use_last_error=True)
    kernel32.LocalFree.argtypes = [ctypes.c_void_p]
    kernel32.LocalFree.restype = ctypes.c_void_p
    kernel32.CloseHandle.argtypes = [ctypes.c_void_p]
    kernel32.CloseHandle.restype = ctypes.c_int
    kernel32.GetCurrentProcess.argtypes = []
    kernel32.GetCurrentProcess.restype = ctypes.c_void_p
    return advapi32, kernel32


def _text_to_sid(value: str) -> ctypes.c_void_p:
    if not _SID_RE.fullmatch(value):
        _fail("ACL_PRINCIPAL_INVALID", f"invalid SID {value!r}")
    advapi32, _ = _windows_libraries()
    pointer = ctypes.c_void_p()
    convert = advapi32.ConvertStringSidToSidW
    convert.argtypes = [ctypes.c_wchar_p, ctypes.POINTER(ctypes.c_void_p)]
    convert.restype = ctypes.c_int
    if not convert(value, ctypes.byref(pointer)) or not pointer.value:
        _fail("ACL_PRINCIPAL_RESOLUTION_FAILED", f"cannot resolve SID {value}")
    return pointer


def _sid_to_text(pointer: ctypes.c_void_p) -> str:
    advapi32, kernel32 = _windows_libraries()
    text_pointer = ctypes.c_void_p()
    convert = advapi32.ConvertSidToStringSidW
    convert.argtypes = [ctypes.c_void_p, ctypes.POINTER(ctypes.c_void_p)]
    convert.restype = ctypes.c_int
    if not convert(pointer, ctypes.byref(text_pointer)) or not text_pointer.value:
        _fail("ACL_INSPECTION_FAILED", "ConvertSidToStringSidW failed")
    try:
        value = ctypes.wstring_at(text_pointer.value)
    finally:
        kernel32.LocalFree(text_pointer)
    if not _SID_RE.fullmatch(value):
        _fail("ACL_INSPECTION_FAILED", f"invalid SID returned {value!r}")
    return value


def _security_descriptor_to_sddl(descriptor: ctypes.c_void_p) -> str:
    advapi32, kernel32 = _windows_libraries()
    text_pointer = ctypes.c_void_p()
    length = ctypes.c_uint32()
    convert = advapi32.ConvertSecurityDescriptorToStringSecurityDescriptorW
    convert.argtypes = [
        ctypes.c_void_p,
        ctypes.c_uint32,
        ctypes.c_uint32,
        ctypes.POINTER(ctypes.c_void_p),
        ctypes.POINTER(ctypes.c_uint32),
    ]
    convert.restype = ctypes.c_int
    if not convert(
        descriptor,
        _SDDL_REVISION_1,
        _OWNER_SECURITY_INFORMATION
        | _GROUP_SECURITY_INFORMATION
        | _DACL_SECURITY_INFORMATION,
        ctypes.byref(text_pointer),
        ctypes.byref(length),
    ):
        _fail("ACL_INSPECTION_FAILED", "security descriptor serialization failed")
    if text_pointer.value is None:
        _fail("ACL_INSPECTION_FAILED", "security descriptor serialization returned null")
    try:
        return ctypes.wstring_at(text_pointer.value)
    finally:
        kernel32.LocalFree(text_pointer)


def _validator_binding() -> dict[str, str]:
    module_path = Path(__file__).resolve(strict=True)
    return {
        "validator_id": ACL_VALIDATOR_VERSION,
        "module": "ai_trading_system.data.access_control",
        "module_sha256": hashlib.sha256(module_path.read_bytes()).hexdigest(),
    }


def _mapping(value: object, field: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping) or not all(isinstance(key, str) for key in value):
        _fail("ACL_SCHEMA_INVALID", f"{field} must be a string-keyed mapping")
    return value


def _canonical_json_file(path: Path, code: str) -> Mapping[str, object]:
    try:
        raw = path.read_bytes()
        parsed = json.loads(raw)
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise DataAccessControlError(code, str(exc), path=path) from exc
    payload = _mapping(parsed, path.name)
    if canonical_json_bytes(payload) != raw:
        _fail(code, "JSON bytes are not canonical", path=path)
    return payload


def _read_pointer_payload(
    root: Path,
    pointer: Mapping[str, object],
    *,
    expected_schema: str,
    code: str,
) -> Mapping[str, object]:
    _exact_fields(pointer, _POINTER_FIELDS, code)
    relative_path = _text(pointer.get("path"), "path")
    if (
        "\\" in relative_path
        or relative_path.startswith("/")
        or any(part in {"", ".", ".."} for part in relative_path.split("/"))
    ):
        _fail(code, "pointer path is not a contained portable path")
    expected_sha = _text(pointer.get("sha256"), "sha256")
    expected_size = pointer.get("size_bytes")
    if (
        not re.fullmatch(r"[0-9a-f]{64}", expected_sha)
        or not isinstance(expected_size, int)
        or isinstance(expected_size, bool)
        or expected_size < 1
        or pointer.get("schema_version") != expected_schema
    ):
        _fail(code, "pointer metadata invalid")
    from ai_trading_system.data.immutable_publish import read_contained_artifact_bytes

    raw = read_contained_artifact_bytes(root=root, relative_path=relative_path)
    if len(raw) != expected_size or hashlib.sha256(raw).hexdigest() != expected_sha:
        _fail(code, "pointer checksum or size mismatch")
    try:
        parsed = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise DataAccessControlError(code, str(exc)) from exc
    payload = _mapping(parsed, relative_path)
    if canonical_json_bytes(payload) != raw:
        _fail(code, "pointed JSON bytes are not canonical")
    return payload


def _aware_datetime(value: object, field: str) -> datetime:
    text = _text(value, field)
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        raise DataAccessControlError("ACL_SCHEMA_INVALID", str(exc)) from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        _fail("ACL_SCHEMA_INVALID", f"{field} must be timezone-aware")
    return parsed


def _exact_fields(
    payload: Mapping[str, object],
    expected: set[str],
    code: str,
    *,
    path: Path | None = None,
) -> None:
    if set(payload) != expected:
        _fail(
            code,
            f"field set differs missing={sorted(expected - set(payload))} "
            f"extra={sorted(set(payload) - expected)}",
            path=path,
        )


def _text(value: object, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        _fail("ACL_SCHEMA_INVALID", f"{field} must be non-empty text")
    return value


def _sid(value: object, field: str) -> str:
    text = _text(value, field)
    if not _SID_RE.fullmatch(text):
        _fail("ACL_POLICY_INVALID", f"{field} must be a SID")
    return text


def _texts(value: object, field: str) -> tuple[str, ...]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        _fail("ACL_SCHEMA_INVALID", f"{field} must be a sequence")
    result = tuple(_text(item, f"{field}[]") for item in value)
    if not result or len(result) != len(set(result)):
        _fail("ACL_SCHEMA_INVALID", f"{field} must be non-empty and unique")
    return result


def _sids(value: object, field: str) -> tuple[str, ...]:
    return tuple(_sid(item, f"{field}[]") for item in _texts(value, field))


def _digest(payload: Mapping[str, object]) -> str:
    return hashlib.sha256(canonical_json_bytes(payload)).hexdigest()


def _fail(code: str, message: str, *, path: Path | None = None) -> NoReturn:
    raise DataAccessControlError(code, message, path=path)
