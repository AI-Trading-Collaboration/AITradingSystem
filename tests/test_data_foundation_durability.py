from __future__ import annotations

import json
import os
from collections.abc import Callable
from copy import deepcopy
from datetime import UTC, date, datetime, timedelta
from pathlib import Path, PurePosixPath
from typing import Any

import pytest

from ai_trading_system.contracts import ArtifactPointer, DataQualityEvidence
from ai_trading_system.data import durability as durability_module
from ai_trading_system.data.durability import (
    CRASH_REHEARSAL_SCHEMA_VERSION,
    DURABILITY_ATTESTATION_SCHEMA_VERSION,
    BackupSource,
    DataDurabilityError,
    DataGcApplyError,
    apply_reference_safe_gc,
    build_crash_rehearsal_receipt,
    build_durability_attestation,
    create_checksum_backup,
    load_durability_policy,
    plan_reference_safe_gc,
    probe_filesystem_durability,
    restore_checksum_backup,
    validate_durability_attestation,
)
from ai_trading_system.data.immutable_publish import (
    COMMIT_CHECKPOINTS,
    DATA_QUALITY_REPORT_SCHEMA_VERSION,
    ContainedArtifactDeletionResult,
    CurrentPointerPrecondition,
    DataPublicationIntegrityError,
    SnapshotPublishRequest,
    SnapshotPublishResult,
    SourceEventProvenance,
    delete_contained_artifact_bytes,
    publish_immutable_snapshot,
    validate_current_snapshot,
    write_contained_artifact_bytes,
)
from ai_trading_system.platform.artifacts import (
    canonical_json_bytes,
    sha256_bytes,
    sha256_path,
)

AS_OF = date(2026, 7, 22)
COVERAGE_START = date(2021, 2, 22)
GENERATED_AT = datetime(2026, 7, 29, 2, 0, tzinfo=UTC)
DATASET_ID = "validated_prices"
PAYLOAD_TYPE = "csv"
PAYLOAD_SCHEMA = "validated_prices.v1"
POLICY_PATH = (
    Path(__file__).resolve().parents[1] / "config" / "data" / "data_foundation_durability.yaml"
)


def test_policy_and_filesystem_profile_keep_claim_boundary_closed(tmp_path: Path) -> None:
    policy = load_durability_policy(POLICY_PATH)
    store = tmp_path / "store"
    store.mkdir()
    profile = probe_filesystem_durability(store)

    assert policy.required_crash_checkpoints == COMMIT_CHECKPOINTS
    assert policy.production_effect == "none"
    assert policy.store_acl_verified is False
    assert policy.consumer_cutover_allowed is False
    assert profile.protocol_version == "data_publication_durable_commit.v1"
    assert "acl_and_same_principal_mutation_not_verified" in profile.limitations


def test_commit_checkpoints_are_ordered_and_d0a_flags_remain_false(
    tmp_path: Path,
) -> None:
    store, evidence, first = _published_store(tmp_path, generations=1)
    observed: list[str] = []
    second = _publish_generation(
        store=store,
        evidence=evidence,
        generation=2,
        expected_pointer_sha256=first.snapshot.pointer_sha256,
        checkpoint_observer=observed.append,
    )

    assert tuple(observed) == COMMIT_CHECKPOINTS
    assert second.crash_durability_verified is False
    assert second.store_acl_verified is False
    assert second.consumer_cutover_allowed is False
    manifest = json.loads(second.snapshot.manifest_path.read_text(encoding="utf-8"))
    assert manifest["crash_durability_verified"] is False
    assert manifest["store_acl_verified"] is False
    assert manifest["consumer_cutover_allowed"] is False


def test_observer_failure_after_replace_is_reported_as_indeterminate(
    tmp_path: Path,
) -> None:
    store, evidence, first = _published_store(tmp_path, generations=1)

    def fail_after_replace(checkpoint: str) -> None:
        if checkpoint == "REPLACED_BEFORE_NAMESPACE_DURABLE":
            raise RuntimeError("injected process boundary")

    with pytest.raises(DataPublicationIntegrityError) as caught:
        _publish_generation(
            store=store,
            evidence=evidence,
            generation=2,
            expected_pointer_sha256=first.snapshot.pointer_sha256,
            checkpoint_observer=fail_after_replace,
        )

    assert caught.value.code == "COMMIT_CHECKPOINT_OBSERVER_FAILED"
    assert caught.value.commit_state == "INDETERMINATE"
    recovered = validate_current_snapshot(
        store_root=store,
        evidence_root=evidence,
        dataset_id=DATASET_ID,
    )
    assert recovered.generation == 2


def test_crash_receipt_requires_every_checkpoint_and_rejects_tamper(
    tmp_path: Path,
) -> None:
    store = tmp_path / "store"
    store.mkdir()
    profile = probe_filesystem_durability(store)
    if not profile.supported:
        pytest.skip(f"unsupported local durability profile: {profile.filesystem}")

    receipt = build_crash_rehearsal_receipt(
        generated_at=GENERATED_AT,
        profile=profile,
        cases=_passing_crash_cases(),
    )

    assert receipt["schema_version"] == CRASH_REHEARSAL_SCHEMA_VERSION
    assert receipt["status"] == "PASS"
    assert receipt["checkpoint_coverage"] == list(COMMIT_CHECKPOINTS)
    with pytest.raises(DataDurabilityError, match="CRASH_REHEARSAL_INCOMPLETE"):
        build_crash_rehearsal_receipt(
            generated_at=GENERATED_AT,
            profile=profile,
            cases=_passing_crash_cases()[:-1],
        )


def test_gc_deletes_only_expired_unreferenced_objects_and_rejects_stale_plan(
    tmp_path: Path,
) -> None:
    policy = load_durability_policy(POLICY_PATH)
    store, _, _ = _published_store(tmp_path, generations=2)
    orphan_relative = "staging/expired-orphan.bin"
    orphan = write_contained_artifact_bytes(
        root=store,
        relative_path=orphan_relative,
        content=b"unreferenced",
        immutable=False,
    )
    expired = GENERATED_AT - timedelta(seconds=policy.orphan_grace_seconds + 60)
    os.utime(orphan.path, (expired.timestamp(), expired.timestamp()))

    plan = plan_reference_safe_gc(
        store_root=store,
        policy=policy,
        generated_at=GENERATED_AT,
    )

    assert [item.relative_path for item in plan.deletion_candidates] == [orphan_relative]
    assert any(reason == "REACHABLE_CURRENT_CHAIN" for _, reason in plan.protected_objects)
    receipt = apply_reference_safe_gc(
        store_root=store,
        policy=policy,
        plan=plan,
    )
    assert receipt["status"] == "PASS"
    assert receipt["deleted_count"] == 1
    assert not orphan.path.exists()

    stale = plan_reference_safe_gc(
        store_root=store,
        policy=policy,
        generated_at=GENERATED_AT,
    )
    write_contained_artifact_bytes(
        root=store,
        relative_path="staging/new-after-plan.bin",
        content=b"new state",
        immutable=False,
    )
    with pytest.raises(DataDurabilityError, match="GC_PLAN_STALE"):
        apply_reference_safe_gc(store_root=store, policy=policy, plan=stale)


def test_contained_delete_requires_exact_integer_size(tmp_path: Path) -> None:
    root = tmp_path / "root"
    root.mkdir()
    artifact = write_contained_artifact_bytes(
        root=root,
        relative_path="objects/item.bin",
        content=b"item",
        immutable=False,
    )

    with pytest.raises(ValueError, match="non-negative integer"):
        delete_contained_artifact_bytes(
            root=root,
            relative_path="objects/item.bin",
            expected_sha256=artifact.sha256,
            expected_size_bytes=4.0,  # type: ignore[arg-type]
        )


def test_gc_external_reference_is_protected_and_hardlink_fails_closed(
    tmp_path: Path,
) -> None:
    policy = load_durability_policy(POLICY_PATH)
    store = tmp_path / "store"
    store.mkdir()
    referenced = write_contained_artifact_bytes(
        root=store,
        relative_path="staging/externally-referenced.bin",
        content=b"referenced",
        immutable=False,
    )
    expired = GENERATED_AT - timedelta(seconds=policy.orphan_grace_seconds + 60)
    os.utime(referenced.path, (expired.timestamp(), expired.timestamp()))
    plan = plan_reference_safe_gc(
        store_root=store,
        policy=policy,
        generated_at=GENERATED_AT,
        external_references=("staging/externally-referenced.bin",),
    )

    assert plan.deletion_candidates == ()
    assert (
        "staging/externally-referenced.bin",
        "EXTERNAL_RUN_OR_LINEAGE_REFERENCE",
    ) in plan.protected_objects

    linked = write_contained_artifact_bytes(
        root=store,
        relative_path="staging/hardlinked.bin",
        content=b"linked",
        immutable=False,
    )
    hardlink = store / "staging" / "hardlinked-copy.bin"
    try:
        os.link(linked.path, hardlink)
    except OSError as exc:
        pytest.skip(f"hardlink creation unavailable: {exc}")
    with pytest.raises(DataDurabilityError, match="GC_ENTRY_INVALID"):
        plan_reference_safe_gc(
            store_root=store,
            policy=policy,
            generated_at=GENERATED_AT,
        )


def test_gc_partial_failure_emits_machine_readable_failure_receipt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    policy = load_durability_policy(POLICY_PATH)
    store = tmp_path / "store"
    store.mkdir()
    expired = GENERATED_AT - timedelta(seconds=policy.orphan_grace_seconds + 60)
    for relative_path in ("staging/a.bin", "staging/b.bin"):
        artifact = write_contained_artifact_bytes(
            root=store,
            relative_path=relative_path,
            content=relative_path.encode(),
            immutable=False,
        )
        os.utime(artifact.path, (expired.timestamp(), expired.timestamp()))
    plan = plan_reference_safe_gc(
        store_root=store,
        policy=policy,
        generated_at=GENERATED_AT,
    )
    real_delete = durability_module.delete_contained_artifact_bytes

    def fail_second_delete(**kwargs: Any) -> ContainedArtifactDeletionResult:
        if kwargs["relative_path"] == "staging/b.bin":
            raise OSError("injected second-delete failure")
        return real_delete(**kwargs)

    monkeypatch.setattr(
        durability_module,
        "delete_contained_artifact_bytes",
        fail_second_delete,
    )
    with pytest.raises(DataGcApplyError, match="GC_APPLY_PARTIAL") as caught:
        apply_reference_safe_gc(store_root=store, policy=policy, plan=plan)

    receipt = caught.value.receipt
    assert receipt["status"] == "FAIL_PARTIAL"
    assert receipt["deleted_count"] == 1
    failed_object = receipt["failed_object"]
    assert isinstance(failed_object, dict)
    assert failed_object["relative_path"] == "staging/b.bin"
    assert not (store / "staging" / "a.bin").exists()
    assert (store / "staging" / "b.bin").exists()


def test_checksum_backup_restores_reviewed_categories_and_detects_corruption(
    tmp_path: Path,
) -> None:
    policy = load_durability_policy(POLICY_PATH)
    source = tmp_path / "source"
    backup = tmp_path / "backup"
    destination = tmp_path / "restore"
    corrupt_destination = tmp_path / "corrupt-restore"
    for path in (source, backup, destination, corrupt_destination):
        path.mkdir()
    fixtures = {
        "config/policy.yaml": b"version: 1\n",
        "forward/run.json": canonical_json_bytes({"run": "forward-only"}),
        "manual/override.csv": b"key,value\nreviewed,true\n",
    }
    for relative_path, content in fixtures.items():
        write_contained_artifact_bytes(
            root=source,
            relative_path=relative_path,
            content=content,
            immutable=False,
        )
    sources = (
        BackupSource(
            source_identity="policy",
            category="critical_config",
            root=source,
            relative_path="config/policy.yaml",
            restore_path="config/policy.yaml",
        ),
        BackupSource(
            source_identity="forward-run",
            category="forward_only",
            root=source,
            relative_path="forward/run.json",
            restore_path="forward/run.json",
        ),
        BackupSource(
            source_identity="manual-override",
            category="manual_input",
            root=source,
            relative_path="manual/override.csv",
            restore_path="manual/override.csv",
        ),
    )
    manifest = create_checksum_backup(
        backup_root=backup,
        sources=sources,
        policy=policy,
        captured_at=GENERATED_AT,
    )

    def validate_policy(restored: Path) -> None:
        assert (restored / "config" / "policy.yaml").read_bytes() == fixtures["config/policy.yaml"]

    receipt = restore_checksum_backup(
        backup_root=backup,
        manifest_path=str(manifest["manifest_path"]),
        destination_root=destination,
        semantic_validators=(validate_policy,),
    )
    assert receipt["status"] == "PASS"
    assert receipt["restored_count"] == 3
    assert receipt["semantic_validators_passed"] == 1
    for relative_path, content in fixtures.items():
        assert (destination / relative_path).read_bytes() == content

    objects = manifest["objects"]
    assert isinstance(objects, list)
    first_object = objects[0]
    assert isinstance(first_object, dict)
    object_path = backup / str(first_object["object_path"])
    object_path.write_bytes(b"corrupted")
    with pytest.raises(DataDurabilityError, match="BACKUP_OBJECT_BINDING_MISMATCH"):
        restore_checksum_backup(
            backup_root=backup,
            manifest_path=str(manifest["manifest_path"]),
            destination_root=corrupt_destination,
        )


def test_final_attestation_is_separate_from_d0a_and_tamper_evident(
    tmp_path: Path,
) -> None:
    policy = load_durability_policy(POLICY_PATH)
    store, evidence, _ = _published_store(tmp_path, generations=1)
    profile = probe_filesystem_durability(store)
    if not profile.supported:
        pytest.skip(f"unsupported local durability profile: {profile.filesystem}")
    crash = build_crash_rehearsal_receipt(
        generated_at=GENERATED_AT,
        profile=profile,
        cases=_passing_crash_cases(),
    )
    gc_plan = plan_reference_safe_gc(
        store_root=store,
        policy=policy,
        generated_at=GENERATED_AT,
    )
    gc_receipt = apply_reference_safe_gc(
        store_root=store,
        policy=policy,
        plan=gc_plan,
    )
    backup_root = tmp_path / "backup"
    source_root = tmp_path / "backup-source"
    restore_root = tmp_path / "restore"
    for path in (backup_root, source_root, restore_root):
        path.mkdir()
    write_contained_artifact_bytes(
        root=source_root,
        relative_path="policy.yaml",
        content=b"version: 1\n",
        immutable=False,
    )
    backup = create_checksum_backup(
        backup_root=backup_root,
        sources=(
            BackupSource(
                source_identity="policy",
                category="critical_config",
                root=source_root,
                relative_path="policy.yaml",
                restore_path="policy.yaml",
            ),
        ),
        policy=policy,
        captured_at=GENERATED_AT,
    )
    restore = restore_checksum_backup(
        backup_root=backup_root,
        manifest_path=str(backup["manifest_path"]),
        destination_root=restore_root,
    )

    attestation = build_durability_attestation(
        store_root=store,
        evidence_root=evidence,
        dataset_id=DATASET_ID,
        generated_at=GENERATED_AT,
        policy=policy,
        profile=profile,
        crash_receipt=crash,
        gc_receipt=gc_receipt,
        restore_receipt=restore,
    )

    assert attestation["schema_version"] == DURABILITY_ATTESTATION_SCHEMA_VERSION
    assert attestation["d0a_manifest_crash_durability_verified"] is False
    assert attestation["scoped_crash_durability_verified"] is True
    assert attestation["store_acl_verified"] is False
    assert attestation["consumer_cutover_allowed"] is False
    validate_durability_attestation(attestation)
    tampered = deepcopy(attestation)
    tampered["store_acl_verified"] = True
    with pytest.raises(DataDurabilityError, match="DURABILITY_ATTESTATION_INVALID"):
        validate_durability_attestation(tampered)


def _published_store(
    root: Path,
    *,
    generations: int,
) -> tuple[Path, Path, SnapshotPublishResult]:
    store = root / "store"
    evidence = root / "evidence"
    evidence.mkdir()
    previous_sha: str | None = None
    result: SnapshotPublishResult | None = None
    for generation in range(1, generations + 1):
        result = _publish_generation(
            store=store,
            evidence=evidence,
            generation=generation,
            expected_pointer_sha256=previous_sha,
        )
        previous_sha = result.snapshot.pointer_sha256
    assert result is not None
    return store, evidence, result


def _publish_generation(
    *,
    store: Path,
    evidence: Path,
    generation: int,
    expected_pointer_sha256: str | None,
    checkpoint_observer: Callable[[str], None] | None = None,
) -> SnapshotPublishResult:
    generated_at = GENERATED_AT + timedelta(minutes=generation)
    payload = ("date,ticker,close\n" f"{AS_OF.isoformat()},QQQ,{550 + generation}.00\n").encode()
    report_relative = _write_quality_report(
        evidence=evidence,
        payload=payload,
        name=f"run-{generation}",
        generated_at=generated_at,
    )
    request = _request(
        evidence=evidence,
        report_relative=report_relative,
        run_id=f"run-{generation}",
        generated_at=generated_at,
    )
    return publish_immutable_snapshot(
        store_root=store,
        evidence_root=evidence,
        request=request,
        payload=payload,
        current_precondition=CurrentPointerPrecondition(expected_sha256=expected_pointer_sha256),
        commit_checkpoint_observer=checkpoint_observer,
    )


def _write_quality_report(
    *,
    evidence: Path,
    payload: bytes,
    name: str,
    generated_at: datetime,
) -> Path:
    pointer = _snapshot_pointer(payload)
    report = {
        "schema_version": DATA_QUALITY_REPORT_SCHEMA_VERSION,
        "contract_id": "validated_prices_dq",
        "policy_id": "data_quality",
        "policy_version": "data_quality.v1",
        "status": "PASS",
        "passed": True,
        "checked_at": (generated_at - timedelta(minutes=5)).isoformat(),
        "as_of": AS_OF.isoformat(),
        "coverage_start": COVERAGE_START.isoformat(),
        "coverage_end": AS_OF.isoformat(),
        "checked_input_count": 1,
        "error_count": 0,
        "warning_count": 0,
        "blocking_issues": [],
        "evaluated_snapshot": pointer.to_dict(),
        "production_effect": "none",
    }
    relative = Path("dq") / f"{name}.json"
    path = evidence / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(canonical_json_bytes(report))
    return relative


def _snapshot_pointer(payload: bytes) -> ArtifactPointer:
    digest = sha256_bytes(payload)
    return ArtifactPointer(
        path=(
            PurePosixPath("snapshots") / DATASET_ID / digest / f"payload.{PAYLOAD_TYPE}"
        ).as_posix(),
        artifact_type=PAYLOAD_TYPE,
        sha256=digest,
        size_bytes=len(payload),
        schema_version=PAYLOAD_SCHEMA,
    )


def _request(
    *,
    evidence: Path,
    report_relative: Path,
    run_id: str,
    generated_at: datetime,
) -> SnapshotPublishRequest:
    report_path = evidence / report_relative
    quality = DataQualityEvidence(
        contract_id="validated_prices_dq",
        policy_id="data_quality",
        policy_version="data_quality.v1",
        status="PASS",
        passed=True,
        checked_at=generated_at - timedelta(minutes=5),
        as_of=AS_OF,
        report_path=report_relative.as_posix(),
        report_sha256=sha256_path(report_path),
        checked_input_count=1,
    )
    return SnapshotPublishRequest(
        dataset_id=DATASET_ID,
        run_id=run_id,
        producer="tests.data_foundation_durability",
        owner="data_platform",
        as_of=AS_OF,
        generated_at=generated_at,
        coverage_start=COVERAGE_START,
        coverage_end=AS_OF,
        payload_artifact_type=PAYLOAD_TYPE,
        payload_schema_version=PAYLOAD_SCHEMA,
        data_quality_report_schema_version=DATA_QUALITY_REPORT_SCHEMA_VERSION,
        source_event=SourceEventProvenance(
            source_id="prices_primary",
            provider_name="test-provider",
            endpoint="https://example.invalid/prices",
            request_parameters={
                "start": COVERAGE_START.isoformat(),
                "end": AS_OF.isoformat(),
            },
            downloaded_at=generated_at - timedelta(minutes=10),
            row_count=1,
            source_role="primary",
            response_headers_sanitized=True,
        ),
        data_quality=quality,
    )


def _passing_crash_cases() -> list[dict[str, object]]:
    cases: list[dict[str, object]] = []
    for index, checkpoint in enumerate(COMMIT_CHECKPOINTS, start=1):
        if checkpoint == "FILE_DURABLE_BEFORE_REPLACE":
            expected = [1]
            recovered = 1
        elif checkpoint == "REPLACED_BEFORE_NAMESPACE_DURABLE":
            expected = [1, 2]
            recovered = 2
        else:
            expected = [2]
            recovered = 2
        cases.append(
            {
                "case_id": f"case-{index}",
                "checkpoint": checkpoint,
                "exit_mode": "FORCED_PROCESS_EXIT",
                "recovered_generation": recovered,
                "expected_generations": expected,
                "validator_status": "PASS",
                "lock_reacquired": True,
                "torn_state_observed": False,
            }
        )
    return cases
