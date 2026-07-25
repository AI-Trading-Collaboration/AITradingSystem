from __future__ import annotations

import hashlib
import json
import subprocess
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from ai_trading_system.platform.architecture.checkout_guard import (
    CheckoutGuardError,
    CheckoutLeaseGuard,
    CheckoutOperationClass,
)
from ai_trading_system.platform.architecture.checkout_telemetry import (
    CHECKOUT_FALSE_BLOCK_REVIEW_SCHEMA_VERSION,
    build_checkout_telemetry_rollup,
    build_checkout_telemetry_snapshot,
    load_checkout_telemetry_policy,
    validate_checkout_telemetry_rollup,
    validate_checkout_telemetry_snapshot,
    validate_false_block_review,
    write_checkout_telemetry_rollup,
    write_checkout_telemetry_snapshot,
)

NOW = datetime(2026, 7, 26, 3, 0, tzinfo=UTC)
TELEMETRY_ROOT = Path(
    "outputs/architecture/arch_005_s4d_checkout_guard/telemetry"
)


@pytest.fixture
def telemetry_checkout(tmp_path: Path) -> tuple[Path, Path]:
    (tmp_path / "src").mkdir()
    (tmp_path / "src/a.py").write_text("A = 1\n", encoding="utf-8")
    unrelated = tmp_path / "docs/research/growth_tilt_owner_diagnosis_pack.md"
    unrelated.parent.mkdir(parents=True)
    unrelated.write_text("owner bytes must remain unread\n", encoding="utf-8")
    _git(tmp_path, "init", "-b", "fixture")
    _git(tmp_path, "config", "user.email", "telemetry@example.com")
    _git(tmp_path, "config", "user.name", "Telemetry Test")
    _git(tmp_path, "add", ".")
    _git(tmp_path, "commit", "-m", "fixture")
    runtime_root = tmp_path / "outputs/architecture/checkout-guard-test"
    guard = CheckoutLeaseGuard(project_root=tmp_path, runtime_root=runtime_root)

    _, first_handle = guard.acquire(
        intent_id="writer-a",
        task_id="TASK-A",
        thread_id="thread-a",
        actor="architecture-control-plane",
        operation_class=CheckoutOperationClass.DOMAIN_MUTATION,
        owned_paths=("src/a.py",),
        now=NOW,
    )
    assert first_handle is not None
    blocked, blocked_handle = guard.acquire(
        intent_id="writer-b",
        task_id="TASK-B",
        thread_id="thread-b",
        actor="architecture-control-plane",
        operation_class=CheckoutOperationClass.DOMAIN_MUTATION,
        owned_paths=("src/a.py",),
        now=NOW + timedelta(seconds=1),
    )
    assert blocked.status == "BLOCKED"
    assert blocked_handle is None
    first_handle.heartbeat(at=NOW + timedelta(seconds=10))
    first_handle.release(outcome="completed", at=NOW + timedelta(seconds=20))

    (tmp_path / "src/a.py").write_text("A = 2\n", encoding="utf-8")
    daily, daily_handle = guard.acquire(
        intent_id="daily-dirty",
        task_id="OPS-DAILY-UNIFIED-TRIGGER",
        thread_id="daily",
        actor="operations-automation",
        operation_class=CheckoutOperationClass.DAILY_OPERATION,
        now=NOW + timedelta(seconds=30),
    )
    assert daily.status == "BLOCKED"
    assert daily_handle is None
    return tmp_path, runtime_root


def test_policy_freezes_read_only_s2_and_keeps_s5_denied() -> None:
    policy = load_checkout_telemetry_policy()

    assert policy.status == "OWNER_APPROVED_READ_ONLY"
    assert policy.policy_version == "arch_005_s4d_checkout_guard@1.2.0"
    assert policy.minimum_observation_batches == 2
    assert policy.accepted_batch_kinds == (
        "supervised_automation",
        "s4c_integration",
        "manual_control_plane",
    )


def test_snapshot_projects_wait_hold_conflict_and_unattributed_metrics(
    telemetry_checkout: tuple[Path, Path],
) -> None:
    root, runtime_root = telemetry_checkout
    snapshot = _snapshot(root, runtime_root, "batch-one")

    assert snapshot["status"] == "PASS"
    assert snapshot["task_governance_status_mutated"] is False
    assert snapshot["automatic_task_mutation"] is False
    assert snapshot["s5_cutover_authorized"] is False
    assert snapshot["task_source_cutover"] is False
    assert snapshot["production_effect"] == "none"
    metrics = snapshot["metrics"]
    assert isinstance(metrics, dict)
    assert metrics["intent_count"] == 3
    assert metrics["lease_count"] == 2
    assert metrics["conflict_count"] == 1
    assert metrics["unattributed_path_count"] == 1
    assert metrics["heartbeat_count"] == 1
    assert metrics["wait_duration_seconds"]["count"] == 2
    assert metrics["lease_held_duration_seconds"]["total"] == 20.0
    assert all(
        source["path"]
        != "docs/research/growth_tilt_owner_diagnosis_pack.md"
        for source in snapshot["sources"]
    )

    output = root / TELEMETRY_ROOT / "batch-one.json"
    write_checkout_telemetry_snapshot(
        output,
        snapshot,
        project_root=root,
    )
    validate_checkout_telemetry_snapshot(snapshot, project_root=root)
    write_checkout_telemetry_snapshot(
        output,
        snapshot,
        project_root=root,
    )


def test_snapshot_rejects_raw_source_tamper_and_output_escape(
    telemetry_checkout: tuple[Path, Path],
) -> None:
    root, runtime_root = telemetry_checkout
    snapshot = _snapshot(root, runtime_root, "batch-tamper")

    with pytest.raises(
        CheckoutGuardError,
        match="CHECKOUT_TELEMETRY_OUTPUT_OUTSIDE",
    ):
        write_checkout_telemetry_snapshot(
            root / "outside.json",
            snapshot,
            project_root=root,
        )

    first_source = snapshot["sources"][0]
    source_path = root / first_source["path"]
    source_path.write_bytes(source_path.read_bytes() + b"\n")
    with pytest.raises(
        CheckoutGuardError,
        match="CHECKOUT_TELEMETRY_SOURCE_HASH",
    ):
        validate_checkout_telemetry_snapshot(snapshot, project_root=root)


def test_release_scope_drift_is_projected_as_unattributed_observation(
    telemetry_checkout: tuple[Path, Path],
) -> None:
    root, runtime_root = telemetry_checkout
    guard = CheckoutLeaseGuard(project_root=root, runtime_root=runtime_root)
    decision, handle = guard.acquire(
        intent_id="release-scope-drift",
        task_id="TASK-C",
        thread_id="thread-c",
        actor="architecture-control-plane",
        operation_class=CheckoutOperationClass.DOMAIN_MUTATION,
        owned_paths=("src/a.py",),
        now=NOW + timedelta(seconds=35),
    )
    assert decision.status == "PASS"
    assert handle is not None
    (root / "src/late.py").write_text("LATE = 1\n", encoding="utf-8")
    with pytest.raises(
        CheckoutGuardError,
        match="CHECKOUT_RELEASE_DIRTY_UNATTRIBUTED",
    ):
        handle.release(outcome="completed", at=NOW + timedelta(seconds=36))

    snapshot = build_checkout_telemetry_snapshot(
        project_root=root,
        batch_id="batch-release-drift",
        batch_kind="manual_control_plane",
        runtime_root=runtime_root,
        generated_at=NOW + timedelta(seconds=40),
    )
    observations = snapshot["block_observations"]
    assert any(
        row["reason_code"]
        == "CHECKOUT_RELEASE_DIRTY_UNATTRIBUTED:src/late.py"
        for row in observations
    )
    assert snapshot["metrics"]["unattributed_path_count"] == 2
    assert snapshot["metrics"]["active_lease_count"] == 0


def test_false_block_review_binds_exact_observation(
    telemetry_checkout: tuple[Path, Path],
) -> None:
    root, runtime_root = telemetry_checkout
    baseline = _snapshot(root, runtime_root, "batch-reviewed")
    observation = baseline["block_observations"][0]
    review = {
        "schema_version": CHECKOUT_FALSE_BLOCK_REVIEW_SCHEMA_VERSION,
        "review_id": "review-batch-reviewed",
        "status": "PASS",
        "batch_id": "batch-reviewed",
        "reviewer": "architecture_control_plane_owner",
        "reviewed_at": (NOW + timedelta(seconds=41)).isoformat(),
        "records": [
            {
                "observation_id": observation["observation_id"],
                "classification": "EXPECTED_BLOCK",
                "rationale": "同一路径存在有效ACTIVE writer，阻塞符合策略。",
                "source_ref": observation["reason_code"],
            }
        ],
        "automatic_task_mutation": False,
        "s5_cutover_authorized": False,
        "production_effect": "none",
        "broker_action": "none",
    }
    review["review_checksum"] = _checksum(review, "review_checksum")
    validate_false_block_review(review, expected_batch_id="batch-reviewed")
    review_path = root / TELEMETRY_ROOT / "reviews/batch-reviewed.json"
    review_path.parent.mkdir(parents=True, exist_ok=True)
    review_path.write_text(
        json.dumps(review, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )

    reviewed = build_checkout_telemetry_snapshot(
        project_root=root,
        batch_id="batch-reviewed",
        batch_kind="manual_control_plane",
        runtime_root=runtime_root,
        false_block_review_path=review_path,
        generated_at=NOW + timedelta(seconds=45),
    )
    metrics = reviewed["metrics"]
    assert metrics["false_block_review_count"] == 1
    assert metrics["expected_block_count"] == 1
    assert metrics["confirmed_false_block_count"] == 0
    assert metrics["unreviewed_block_count"] == 1

    review["records"][0]["observation_id"] = "unknown-observation"
    review["review_checksum"] = _checksum(review, "review_checksum")
    review_path.write_text(
        json.dumps(review, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    with pytest.raises(
        CheckoutGuardError,
        match="CHECKOUT_FALSE_BLOCK_REVIEW_UNKNOWN_OBSERVATION",
    ):
        build_checkout_telemetry_snapshot(
            project_root=root,
            batch_id="batch-reviewed",
            batch_kind="manual_control_plane",
            runtime_root=runtime_root,
            false_block_review_path=review_path,
            generated_at=NOW + timedelta(seconds=45),
        )


def test_two_batch_rollup_is_evaluation_ready_but_never_authorizes_s5(
    telemetry_checkout: tuple[Path, Path],
) -> None:
    root, runtime_root = telemetry_checkout
    snapshot_paths: list[Path] = []
    for index, batch_id in enumerate(("batch-one", "batch-two"), start=1):
        snapshot = build_checkout_telemetry_snapshot(
            project_root=root,
            batch_id=batch_id,
            batch_kind="s4c_integration",
            runtime_root=runtime_root,
            generated_at=NOW + timedelta(minutes=index),
        )
        path = root / TELEMETRY_ROOT / f"{batch_id}.json"
        write_checkout_telemetry_snapshot(path, snapshot, project_root=root)
        snapshot_paths.append(path)

    one_batch = build_checkout_telemetry_rollup(
        project_root=root,
        snapshot_paths=snapshot_paths[:1],
        generated_at=NOW + timedelta(minutes=3),
    )
    assert one_batch["s5_evaluation_evidence_ready"] is False

    rollup = build_checkout_telemetry_rollup(
        project_root=root,
        snapshot_paths=snapshot_paths,
        generated_at=NOW + timedelta(minutes=4),
    )
    assert rollup["metrics"]["observed_batch_count"] == 2
    assert rollup["s5_evaluation_evidence_ready"] is True
    assert rollup["s5_owner_decision_required"] is True
    assert rollup["s5_cutover_authorized"] is False
    assert rollup["task_source_cutover"] is False
    validate_checkout_telemetry_rollup(rollup, project_root=root)
    output = root / TELEMETRY_ROOT / "rollup.json"
    write_checkout_telemetry_rollup(output, rollup, project_root=root)


def test_cli_build_and_validate_round_trip(
    telemetry_checkout: tuple[Path, Path],
) -> None:
    root, runtime_root = telemetry_checkout
    output = root / TELEMETRY_ROOT / "cli-batch.json"
    script = Path(__file__).parents[1] / "scripts/architecture_arch005_checkout_guard.py"
    result = subprocess.run(
        [
            sys.executable,
            str(script),
            "telemetry-build",
            "--project-root",
            str(root),
            "--runtime-root",
            str(runtime_root),
            "--batch-id",
            "cli-batch",
            "--batch-kind",
            "manual_control_plane",
            "--at",
            (NOW + timedelta(minutes=1)).isoformat(),
            "--output",
            str(output),
        ],
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    assert result.returncode == 0, result.stderr
    assert '"s5_cutover_authorized": false' in result.stdout
    validate = subprocess.run(
        [
            sys.executable,
            str(script),
            "telemetry-validate",
            "--project-root",
            str(root),
            "--artifact",
            str(output),
        ],
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    assert validate.returncode == 0, validate.stderr
    assert '"status": "PASS"' in validate.stdout


def _snapshot(root: Path, runtime_root: Path, batch_id: str) -> dict[str, object]:
    return build_checkout_telemetry_snapshot(
        project_root=root,
        batch_id=batch_id,
        batch_kind="manual_control_plane",
        runtime_root=runtime_root,
        generated_at=NOW + timedelta(seconds=40),
    )


def _checksum(payload: dict[str, object], field: str) -> str:
    body = {key: value for key, value in payload.items() if key != field}
    raw = json.dumps(
        body,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def _git(root: Path, *args: str) -> None:
    subprocess.run(
        ["git", *args],
        cwd=root,
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
