from __future__ import annotations

import copy
import json
import os
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path

import pytest
import yaml

import ai_trading_system.data.access_control as acl_module
from ai_trading_system.data.access_control import (
    ACL_ATTESTATION_SCHEMA_VERSION,
    ACL_CLEANUP_RECEIPT_SCHEMA_VERSION,
    ACL_PLATFORM_PROFILE,
    ACL_POLICY_SCHEMA_VERSION,
    ACL_PROBE_SCHEMA_VERSION,
    ACL_REHEARSAL_BUNDLE_SCHEMA_VERSION,
    AclEntry,
    DataAccessControlError,
    apply_isolated_store_acl,
    build_acl_attestation,
    current_windows_user_sid,
    inspect_store_acl,
    load_acl_policy,
    run_acl_enforcement_probe,
    validate_acl_attestation,
    validate_acl_rehearsal_bundle,
    validate_acl_snapshot,
)
from ai_trading_system.data.immutable_publish import (
    CONSUMER_CUTOVER_LIMITATION,
    STORE_ACL_VERIFIED_LIMITATION,
)
from scripts.data_foundation_acl_rehearsal import run_acl_rehearsal

ROOT = Path(__file__).resolve().parents[1]
POLICY_PATH = ROOT / "config" / "data" / "data_foundation_acl.yaml"
GENERATED_AT = datetime(2026, 7, 29, 3, 30, tzinfo=UTC)
WINDOWS_ONLY = pytest.mark.skipif(os.name != "nt", reason="native Windows ACL rehearsal")


def _policy_payload() -> dict[str, object]:
    payload = yaml.safe_load(POLICY_PATH.read_text(encoding="utf-8"))
    assert isinstance(payload, dict)
    return payload


def _write_policy(tmp_path: Path, payload: dict[str, object]) -> Path:
    path = tmp_path / "policy.yaml"
    path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    return path


def _error_code(exc: pytest.ExceptionInfo[DataAccessControlError]) -> str:
    return exc.value.code


def test_policy_contract_is_reviewed_and_keeps_global_boundaries_false() -> None:
    policy = load_acl_policy(POLICY_PATH)

    assert policy.policy_id == "data_foundation_store_acl_isolation"
    assert policy.policy_version == "data_foundation_store_acl_isolation.v1"
    assert policy.status == "PILOT_BASELINE"
    assert policy.scope == "ISOLATED_REHEARSAL_ONLY"
    assert policy.production_effect == "none"
    assert policy.broker_action == "none"
    assert policy.historical_manifest_store_acl_verified is False
    assert policy.generic_consumer_cutover_allowed is False
    assert policy.reader_sid == "S-1-5-32-545"
    assert policy.recovery_sids == ("S-1-5-18", "S-1-5-32-544")
    assert policy.forbidden_broad_write_sids == ("S-1-1-0", "S-1-5-11")
    assert len(policy.policy_sha256) == 64


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        (
            lambda policy: policy["claim_boundary"].__setitem__(  # type: ignore[union-attr]
                "generic_consumer_cutover_allowed",
                True,
            ),
            "claim boundary widened",
        ),
        (
            lambda policy: policy["windows_ntfs"].__setitem__(  # type: ignore[union-attr]
                "reader_rights",
                "MODIFY",
            ),
            "Windows ACL invariant drift",
        ),
        (
            lambda policy: policy["windows_ntfs"].__setitem__(  # type: ignore[union-attr]
                "reader_sid",
                "S-1-1-0",
            ),
            "reader must be BUILTIN Users",
        ),
        (
            lambda policy: policy.__setitem__("unknown", True),
            "field set differs",
        ),
    ],
)
def test_policy_drift_fails_closed(
    tmp_path: Path,
    mutation: object,
    message: str,
) -> None:
    payload = _policy_payload()
    policy = payload["policy"]
    assert isinstance(policy, dict)
    assert callable(mutation)
    mutation(policy)

    with pytest.raises(DataAccessControlError, match=message) as exc:
        load_acl_policy(_write_policy(tmp_path, payload))

    assert _error_code(exc) == "ACL_POLICY_INVALID"


def test_policy_checksum_binds_exact_bytes(tmp_path: Path) -> None:
    original = load_acl_policy(POLICY_PATH)
    copied = tmp_path / "policy.yaml"
    copied.write_bytes(POLICY_PATH.read_bytes() + b"\n")

    copy_policy = load_acl_policy(copied)

    assert copy_policy.policy_id == original.policy_id
    assert copy_policy.policy_sha256 != original.policy_sha256


def test_snapshot_validator_rejects_broad_write_and_role_drift() -> None:
    policy = load_acl_policy(POLICY_PATH)
    writer_sid = "S-1-5-21-1-2-3-1001"
    base_entries = (
        AclEntry(writer_sid, acl_module._FILE_ALL_ACCESS, 3, False),
        AclEntry("S-1-5-18", acl_module._FILE_ALL_ACCESS, 3, False),
        AclEntry("S-1-5-32-544", acl_module._FILE_ALL_ACCESS, 3, False),
        AclEntry("S-1-5-32-545", acl_module._FILE_READ_EXECUTE, 3, False),
    )
    snapshot = acl_module.StoreAclSnapshot(
        owner_sid=writer_sid,
        dacl_protected=True,
        sddl="test",
        security_descriptor_sha256="0" * 64,
        entries=base_entries,
    )
    validate_acl_snapshot(snapshot, policy=policy, writer_sid=writer_sid, child=False)

    broad = replace(
        snapshot,
        entries=(
            *base_entries,
            AclEntry("S-1-1-0", acl_module._FILE_ALL_ACCESS, 3, False),
        ),
    )
    with pytest.raises(DataAccessControlError, match="broad principal") as exc:
        validate_acl_snapshot(broad, policy=policy, writer_sid=writer_sid, child=False)
    assert _error_code(exc) == "ACL_BROAD_WRITE_DETECTED"

    unprotected = replace(snapshot, dacl_protected=False)
    with pytest.raises(DataAccessControlError, match="not protected") as exc:
        validate_acl_snapshot(unprotected, policy=policy, writer_sid=writer_sid, child=False)
    assert _error_code(exc) == "ACL_POLICY_MISMATCH"


def test_apply_refuses_nonempty_or_out_of_scope_root(tmp_path: Path) -> None:
    policy = load_acl_policy(POLICY_PATH)
    inside = tmp_path / "inside"
    inside.mkdir()
    (inside / "existing.txt").write_text("not empty", encoding="utf-8")
    with pytest.raises(DataAccessControlError, match="empty isolated root") as exc:
        apply_isolated_store_acl(inside, allowed_parent=tmp_path, policy=policy)
    assert _error_code(exc) == "ACL_SCOPE_VIOLATION"

    outside_parent = tmp_path / "outside-parent"
    outside_parent.mkdir()
    outside = tmp_path / "outside"
    outside.mkdir()
    with pytest.raises(DataAccessControlError, match="outside allowed parent") as exc:
        apply_isolated_store_acl(outside, allowed_parent=outside_parent, policy=policy)
    assert _error_code(exc) == "ACL_SCOPE_VIOLATION"


@WINDOWS_ONLY
def test_windows_native_acl_apply_inspect_and_enforcement_probe(tmp_path: Path) -> None:
    policy = load_acl_policy(POLICY_PATH)
    store_root = tmp_path / "store"
    store_root.mkdir()

    applied = apply_isolated_store_acl(
        store_root,
        allowed_parent=tmp_path,
        policy=policy,
    )
    inspected = inspect_store_acl(store_root)
    receipt = run_acl_enforcement_probe(store_root, policy=policy)

    assert inspected == applied
    assert inspected.owner_sid == current_windows_user_sid()
    assert inspected.dacl_protected is True
    assert len(inspected.entries) == 4
    assert receipt["schema_version"] == ACL_PROBE_SCHEMA_VERSION
    assert receipt["probe_id"].startswith("acl_probe_")
    assert all(
        value is True
        for key, value in receipt.items()
        if key
        not in {
            "probe_id",
            "schema_version",
            "production_effect",
            "broker_action",
        }
    )
    assert list(store_root.iterdir()) == []


@WINDOWS_ONLY
def test_attestation_is_deterministic_live_validated_and_scoped(tmp_path: Path) -> None:
    policy = load_acl_policy(POLICY_PATH)
    store_root = tmp_path / "store"
    store_root.mkdir()
    apply_isolated_store_acl(store_root, allowed_parent=tmp_path, policy=policy)

    first = build_acl_attestation(store_root, policy=policy, generated_at=GENERATED_AT)
    second = build_acl_attestation(store_root, policy=policy, generated_at=GENERATED_AT)
    validate_acl_attestation(first, store_root=store_root, policy=policy)

    assert first == second
    assert first["schema_version"] == ACL_ATTESTATION_SCHEMA_VERSION
    assert first["platform_profile"] == ACL_PLATFORM_PROFILE
    assert first["status"] == "PASS"
    assert first["claim_boundary"] == {
        "historical_manifest_store_acl_verified": False,
        "generic_consumer_cutover_allowed": False,
    }
    assert first["production_effect"] == "none"
    assert first["broker_action"] == "none"
    assert STORE_ACL_VERIFIED_LIMITATION == "store_acl_verified=false"
    assert CONSUMER_CUTOVER_LIMITATION == "consumer_cutover_allowed=false"
    json.dumps(first, sort_keys=True)


@WINDOWS_ONLY
def test_attestation_tamper_and_policy_byte_drift_fail_closed(tmp_path: Path) -> None:
    policy = load_acl_policy(POLICY_PATH)
    store_root = tmp_path / "store"
    store_root.mkdir()
    apply_isolated_store_acl(store_root, allowed_parent=tmp_path, policy=policy)
    attestation = build_acl_attestation(
        store_root,
        policy=policy,
        generated_at=GENERATED_AT,
    )

    tampered = copy.deepcopy(attestation)
    tampered["status"] = "BLOCKED"
    with pytest.raises(DataAccessControlError, match="attestation id mismatch") as exc:
        validate_acl_attestation(tampered, store_root=store_root, policy=policy)
    assert _error_code(exc) == "ACL_ATTESTATION_INVALID"

    copied = tmp_path / "policy-copy.yaml"
    copied.write_bytes(POLICY_PATH.read_bytes() + b"\n")
    drifted_policy = load_acl_policy(copied)
    with pytest.raises(DataAccessControlError, match="identity or claim boundary") as exc:
        validate_acl_attestation(
            attestation,
            store_root=store_root,
            policy=drifted_policy,
        )
    assert _error_code(exc) == "ACL_ATTESTATION_INVALID"


@WINDOWS_ONLY
def test_live_acl_drift_invalidates_attestation(tmp_path: Path) -> None:
    policy = load_acl_policy(POLICY_PATH)
    store_root = tmp_path / "store"
    store_root.mkdir()
    apply_isolated_store_acl(store_root, allowed_parent=tmp_path, policy=policy)
    attestation = build_acl_attestation(
        store_root,
        policy=policy,
        generated_at=GENERATED_AT,
    )
    writer_sid = current_windows_user_sid()
    drifted_entries = (
        AclEntry(writer_sid, acl_module._FILE_ALL_ACCESS, 3, False),
        AclEntry("S-1-5-18", acl_module._FILE_ALL_ACCESS, 3, False),
        AclEntry("S-1-5-32-544", acl_module._FILE_ALL_ACCESS, 3, False),
        AclEntry("S-1-5-32-545", acl_module._FILE_ALL_ACCESS, 3, False),
    )
    acl_module._set_windows_dacl(store_root, drifted_entries)

    with pytest.raises(DataAccessControlError, match="mask differs") as exc:
        validate_acl_attestation(attestation, store_root=store_root, policy=policy)
    assert _error_code(exc) == "ACL_POLICY_MISMATCH"

    acl_module._set_windows_dacl(
        store_root,
        acl_module._expected_entries(policy, writer_sid),
    )


def test_attestation_field_contract_is_exact() -> None:
    assert acl_module._ATTESTATION_FIELDS == {
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
    assert acl_module._PROBE_FIELDS == {
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
    assert ACL_POLICY_SCHEMA_VERSION == "data_foundation_store_acl_policy.v1"


@WINDOWS_ONLY
def test_rehearsal_bundle_is_live_validated_cleaned_and_offline_verifiable(
    tmp_path: Path,
) -> None:
    output_dir = tmp_path / "bundle"

    bundle = run_acl_rehearsal(
        policy_path=POLICY_PATH,
        output_dir=output_dir,
        allowed_output_parent=tmp_path,
        generated_at=GENERATED_AT,
    )
    validated = validate_acl_rehearsal_bundle(
        output_dir / "rehearsal_bundle.json",
        policy_path=POLICY_PATH,
    )

    assert validated == bundle
    assert bundle["schema_version"] == ACL_REHEARSAL_BUNDLE_SCHEMA_VERSION
    assert bundle["status"] == "PASS"
    assert not (output_dir / "live_rehearsal_store").exists()
    assert sorted(path.name for path in output_dir.iterdir()) == [
        "acl_attestation.json",
        "cleanup_receipt.json",
        "rehearsal_bundle.json",
    ]
    cleanup = json.loads((output_dir / "cleanup_receipt.json").read_bytes())
    assert cleanup["schema_version"] == ACL_CLEANUP_RECEIPT_SCHEMA_VERSION
    assert cleanup["root_existed_before"] is True
    assert cleanup["root_exists_after"] is False
    assert cleanup["production_effect"] == "none"


@WINDOWS_ONLY
def test_rehearsal_bundle_tamper_fails_closed(tmp_path: Path) -> None:
    output_dir = tmp_path / "bundle"
    run_acl_rehearsal(
        policy_path=POLICY_PATH,
        output_dir=output_dir,
        allowed_output_parent=tmp_path,
        generated_at=GENERATED_AT,
    )
    attestation_path = output_dir / "acl_attestation.json"
    attestation = json.loads(attestation_path.read_bytes())
    attestation["status"] = "BLOCKED"
    attestation_path.write_text(
        json.dumps(attestation, sort_keys=True, separators=(",", ":")),
        encoding="utf-8",
    )

    with pytest.raises(DataAccessControlError, match="checksum or size mismatch") as exc:
        validate_acl_rehearsal_bundle(
            output_dir / "rehearsal_bundle.json",
            policy_path=POLICY_PATH,
        )
    assert _error_code(exc) == "ACL_BUNDLE_INVALID"


def test_rehearsal_output_must_be_below_explicit_parent(tmp_path: Path) -> None:
    allowed = tmp_path / "allowed"
    allowed.mkdir()
    outside = tmp_path / "outside"

    with pytest.raises(ValueError, match="child of allowed_output_parent"):
        run_acl_rehearsal(
            policy_path=POLICY_PATH,
            output_dir=outside,
            allowed_output_parent=allowed,
            generated_at=GENERATED_AT,
        )
    assert not outside.exists()
