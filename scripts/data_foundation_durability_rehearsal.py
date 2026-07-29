from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
from collections.abc import Callable, Sequence
from datetime import UTC, date, datetime, timedelta
from pathlib import Path, PurePosixPath

from ai_trading_system.contracts import ArtifactPointer, DataQualityEvidence
from ai_trading_system.data.durability import (
    BackupSource,
    DurabilityPolicy,
    StoreGcPlan,
    apply_reference_safe_gc,
    build_crash_rehearsal_receipt,
    build_durability_attestation,
    create_checksum_backup,
    load_durability_policy,
    plan_reference_safe_gc,
    probe_filesystem_durability,
    restore_checksum_backup,
)
from ai_trading_system.data.immutable_publish import (
    COMMIT_CHECKPOINTS,
    DATA_QUALITY_REPORT_SCHEMA_VERSION,
    CurrentPointerPrecondition,
    SnapshotPublishRequest,
    SnapshotPublishResult,
    SourceEventProvenance,
    exclusive_store_maintenance,
    publish_immutable_snapshot,
    validate_current_snapshot,
    write_contained_artifact_bytes,
)
from ai_trading_system.platform.artifacts import (
    canonical_json_bytes,
    sha256_bytes,
    sha256_path,
)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_POLICY_PATH = PROJECT_ROOT / "config" / "data" / "data_foundation_durability.yaml"
DATASET_ID = "validated_prices"
PAYLOAD_TYPE = "csv"
PAYLOAD_SCHEMA = "validated_prices.v1"
AS_OF = date(2026, 7, 22)
COVERAGE_START = date(2021, 2, 22)
FORCED_EXIT_CODE = 91


def main(argv: Sequence[str] | None = None) -> int:
    arguments = list(sys.argv[1:] if argv is None else argv)
    if arguments and arguments[0] == "_child":
        return _child_main(arguments[1:])
    parser = argparse.ArgumentParser(
        description=(
            "运行 DATA-GOV-001 D0C 子进程 crash、reference-safe GC、"
            "checksum backup/restore 验收演练。"
        )
    )
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--policy", type=Path, default=DEFAULT_POLICY_PATH)
    parser.add_argument(
        "--generated-at",
        help="UTC ISO-8601；默认使用当前 UTC 时间。",
    )
    args = parser.parse_args(arguments)
    generated_at = (
        _parse_utc(args.generated_at)
        if args.generated_at
        else datetime.now(tz=UTC).replace(microsecond=0)
    )
    output = args.output_dir.resolve()
    _prepare_empty_output(output)
    policy = load_durability_policy(args.policy.resolve())

    workspace = output / "workspace"
    workspace.mkdir()
    acceptance_store = output / "acceptance_store"
    acceptance_evidence = output / "acceptance_evidence"
    backup_store = output / "backup_store"
    acceptance_evidence.mkdir()
    backup_store.mkdir()

    try:
        profile = probe_filesystem_durability(output)
        if not profile.supported:
            raise RuntimeError(
                "DURABILITY_PROFILE_UNSUPPORTED: "
                f"{profile.system}/{profile.filesystem}/{profile.storage_scope}"
            )
        crash_receipt = _run_crash_matrix(
            workspace=workspace,
            generated_at=generated_at,
        )
        publication = _publish_generation(
            store=acceptance_store,
            evidence=acceptance_evidence,
            generation=1,
            generated_at=generated_at + timedelta(minutes=10),
            expected_pointer_sha256=None,
        )
        publication = _publish_generation(
            store=acceptance_store,
            evidence=acceptance_evidence,
            generation=2,
            generated_at=generated_at + timedelta(minutes=11),
            expected_pointer_sha256=publication.snapshot.pointer_sha256,
        )
        gc_plan, gc_receipt = _run_gc_rehearsal(
            store=acceptance_store,
            evidence=acceptance_evidence,
            policy=policy,
            generated_at=generated_at + timedelta(minutes=20),
        )
        backup, restore_receipt = _run_backup_restore_rehearsal(
            workspace=workspace,
            backup_store=backup_store,
            policy=policy,
            generated_at=generated_at + timedelta(minutes=30),
        )
        attestation = build_durability_attestation(
            store_root=acceptance_store,
            evidence_root=acceptance_evidence,
            dataset_id=DATASET_ID,
            generated_at=generated_at + timedelta(minutes=40),
            policy=policy,
            profile=profile,
            crash_receipt=crash_receipt,
            gc_receipt=gc_receipt,
            restore_receipt=restore_receipt,
        )
        artifacts = {
            "filesystem_profile.json": profile.to_dict(),
            "crash_rehearsal.json": crash_receipt,
            "gc_plan.json": gc_plan.to_dict(),
            "gc_receipt.json": gc_receipt,
            "backup_binding.json": backup,
            "restore_receipt.json": restore_receipt,
            "durability_attestation.json": attestation,
        }
        artifact_bindings = _write_evidence_artifacts(output, artifacts)
    finally:
        if workspace.exists():
            shutil.rmtree(workspace)

    bundle_body: dict[str, object] = {
        "schema_version": "data_foundation_d0c_rehearsal_bundle.v1",
        "generated_at": generated_at.isoformat(),
        "status": "PASS",
        "policy_id": policy.policy_id,
        "policy_version": policy.policy_version,
        "dataset_id": DATASET_ID,
        "publication_generation": publication.snapshot.generation,
        "publication_pointer_id": publication.snapshot.pointer_id,
        "durability_attestation_id": attestation["attestation_id"],
        "artifact_bindings": artifact_bindings,
        "retained_acceptance_store": "acceptance_store",
        "retained_acceptance_evidence": "acceptance_evidence",
        "retained_backup_store": "backup_store",
        "temporary_workspace_removed": True,
        "fixture_scope": "ISOLATED_REHEARSAL_ONLY",
        "store_acl_verified": False,
        "consumer_cutover_allowed": False,
        "production_effect": "none",
    }
    bundle = {
        "bundle_id": _semantic_id("data_foundation_d0c_bundle_", bundle_body),
        **bundle_body,
    }
    bundle_binding = _write_json_artifact(output, "rehearsal_bundle.json", bundle)
    summary = {
        "status": "PASS",
        "message": "D0C crash durability / GC / backup-restore 演练通过",
        "bundle_id": bundle["bundle_id"],
        "attestation_id": attestation["attestation_id"],
        "bundle_path": str(output / "rehearsal_bundle.json"),
        "bundle_sha256": bundle_binding["sha256"],
        "production_effect": "none",
        "store_acl_verified": False,
        "consumer_cutover_allowed": False,
    }
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


def _child_main(argv: Sequence[str]) -> int:
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--store", type=Path, required=True)
    parser.add_argument("--evidence", type=Path, required=True)
    parser.add_argument("--checkpoint", choices=COMMIT_CHECKPOINTS, required=True)
    parser.add_argument("--previous-sha256", required=True)
    parser.add_argument("--generated-at", required=True)
    args = parser.parse_args(argv)

    def force_exit(checkpoint: str) -> None:
        if checkpoint == args.checkpoint:
            os._exit(FORCED_EXIT_CODE)

    _publish_generation(
        store=args.store,
        evidence=args.evidence,
        generation=2,
        generated_at=_parse_utc(args.generated_at),
        expected_pointer_sha256=args.previous_sha256,
        checkpoint_observer=force_exit,
    )
    return 3


def _run_crash_matrix(
    *,
    workspace: Path,
    generated_at: datetime,
) -> dict[str, object]:
    base = workspace / "crash_base"
    base.mkdir()
    base_store = base / "store"
    base_evidence = base / "evidence"
    base_evidence.mkdir()
    first = _publish_generation(
        store=base_store,
        evidence=base_evidence,
        generation=1,
        generated_at=generated_at,
        expected_pointer_sha256=None,
    )
    cases: list[dict[str, object]] = []
    for index, checkpoint in enumerate(COMMIT_CHECKPOINTS, start=1):
        case_root = workspace / "crash_cases" / f"{index:02d}_{checkpoint.lower()}"
        case_root.parent.mkdir(exist_ok=True)
        shutil.copytree(base, case_root)
        case_store = case_root / "store"
        case_evidence = case_root / "evidence"
        completed = subprocess.run(
            [
                sys.executable,
                str(Path(__file__).resolve()),
                "_child",
                "--store",
                str(case_store),
                "--evidence",
                str(case_evidence),
                "--checkpoint",
                checkpoint,
                "--previous-sha256",
                first.snapshot.pointer_sha256,
                "--generated-at",
                (generated_at + timedelta(minutes=index)).isoformat(),
            ],
            cwd=PROJECT_ROOT,
            capture_output=True,
            text=True,
            check=False,
        )
        if completed.returncode != FORCED_EXIT_CODE:
            raise RuntimeError(
                f"CRASH_CASE_DID_NOT_EXIT: {checkpoint}; "
                f"returncode={completed.returncode}; stderr={completed.stderr}"
            )
        recovered = validate_current_snapshot(
            store_root=case_store,
            evidence_root=case_evidence,
            dataset_id=DATASET_ID,
        )
        with exclusive_store_maintenance(store_root=case_store):
            lock_reacquired = True
        if checkpoint == "FILE_DURABLE_BEFORE_REPLACE":
            expected_generations = [1]
        elif checkpoint == "REPLACED_BEFORE_NAMESPACE_DURABLE":
            expected_generations = [1, 2]
        else:
            expected_generations = [2]
        cases.append(
            {
                "case_id": f"process-crash-{index:02d}",
                "checkpoint": checkpoint,
                "exit_mode": "FORCED_PROCESS_EXIT",
                "recovered_generation": recovered.generation,
                "expected_generations": expected_generations,
                "validator_status": "PASS",
                "lock_reacquired": lock_reacquired,
                "torn_state_observed": False,
            }
        )
    profile = probe_filesystem_durability(base_store)
    return build_crash_rehearsal_receipt(
        generated_at=generated_at,
        profile=profile,
        cases=cases,
    )


def _run_gc_rehearsal(
    *,
    store: Path,
    evidence: Path,
    policy: DurabilityPolicy,
    generated_at: datetime,
) -> tuple[StoreGcPlan, dict[str, object]]:
    orphan = write_contained_artifact_bytes(
        root=store,
        relative_path="staging/rehearsal-expired-orphan.bin",
        content=b"expired orphan",
        immutable=False,
    )
    expired = generated_at - timedelta(seconds=policy.orphan_grace_seconds + 60)
    os.utime(orphan.path, (expired.timestamp(), expired.timestamp()))
    plan = plan_reference_safe_gc(
        store_root=store,
        policy=policy,
        generated_at=generated_at,
    )
    if [item.relative_path for item in plan.deletion_candidates] != [
        "staging/rehearsal-expired-orphan.bin"
    ]:
        raise RuntimeError("GC_REHEARSAL_PLAN_UNEXPECTED")
    receipt = apply_reference_safe_gc(store_root=store, policy=policy, plan=plan)
    if orphan.path.exists():
        raise RuntimeError("GC_REHEARSAL_DELETE_FAILED")
    validate_current_snapshot(
        store_root=store,
        evidence_root=evidence,
        dataset_id=DATASET_ID,
    )
    return plan, receipt


def _run_backup_restore_rehearsal(
    *,
    workspace: Path,
    backup_store: Path,
    policy: DurabilityPolicy,
    generated_at: datetime,
) -> tuple[dict[str, object], dict[str, object]]:
    source = workspace / "backup_source"
    restored = workspace / "restore_destination"
    source.mkdir()
    restored.mkdir()
    fixtures = {
        "config/policy.yaml": b"version: 1\n",
        "forward/run.json": canonical_json_bytes({"run": "forward-only"}),
        "manual/override.csv": b"key,value\nreviewed,true\n",
    }
    categories = {
        "config/policy.yaml": "critical_config",
        "forward/run.json": "forward_only",
        "manual/override.csv": "manual_input",
    }
    sources: list[BackupSource] = []
    for relative_path, content in fixtures.items():
        write_contained_artifact_bytes(
            root=source,
            relative_path=relative_path,
            content=content,
            immutable=False,
        )
        sources.append(
            BackupSource(
                source_identity=f"rehearsal:{relative_path}",
                category=categories[relative_path],
                root=source,
                relative_path=relative_path,
                restore_path=relative_path,
            )
        )
    backup = create_checksum_backup(
        backup_root=backup_store,
        sources=tuple(sources),
        policy=policy,
        captured_at=generated_at,
    )

    def validate_fixture_restore(root: Path) -> None:
        for relative_path, expected in fixtures.items():
            actual = (root / relative_path).read_bytes()
            if actual != expected:
                raise RuntimeError(f"RESTORE_SEMANTIC_MISMATCH: {relative_path}")

    restore = restore_checksum_backup(
        backup_root=backup_store,
        manifest_path=str(backup["manifest_path"]),
        destination_root=restored,
        semantic_validators=(validate_fixture_restore,),
    )
    return backup, restore


def _publish_generation(
    *,
    store: Path,
    evidence: Path,
    generation: int,
    generated_at: datetime,
    expected_pointer_sha256: str | None,
    checkpoint_observer: Callable[[str], None] | None = None,
) -> SnapshotPublishResult:
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
        producer="scripts.data_foundation_durability_rehearsal",
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
            provider_name="rehearsal-fixture",
            endpoint="https://example.invalid/rehearsal",
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


def _write_evidence_artifacts(
    root: Path,
    artifacts: dict[str, dict[str, object]],
) -> list[dict[str, object]]:
    return [_write_json_artifact(root, name, artifacts[name]) for name in sorted(artifacts)]


def _write_json_artifact(
    root: Path,
    relative_path: str,
    payload: dict[str, object],
) -> dict[str, object]:
    raw = canonical_json_bytes(payload)
    result = write_contained_artifact_bytes(
        root=root,
        relative_path=relative_path,
        content=raw,
        immutable=True,
    )
    return {
        "path": relative_path,
        "sha256": result.sha256,
        "size_bytes": result.size_bytes,
    }


def _semantic_id(prefix: str, payload: dict[str, object]) -> str:
    return f"{prefix}{sha256_bytes(canonical_json_bytes(payload))[:32]}"


def _prepare_empty_output(path: Path) -> None:
    if path.exists():
        if not path.is_dir() or any(path.iterdir()):
            raise RuntimeError(f"OUTPUT_DIR_NOT_EMPTY: {path}")
        return
    path.mkdir(parents=True)


def _parse_utc(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("generated-at must be timezone-aware")
    return parsed.astimezone(UTC)


if __name__ == "__main__":
    raise SystemExit(main())
