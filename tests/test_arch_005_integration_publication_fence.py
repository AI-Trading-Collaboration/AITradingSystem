from __future__ import annotations

import json
import subprocess
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Barrier

import pytest

from ai_trading_system.platform.architecture.integration_publication_fence import (
    DEFAULT_POLICY_PATH,
    IntegrationPublicationFence,
    PublicationFenceError,
    load_publication_fence_policy,
)

ROOT = Path(__file__).resolve().parents[1]
CHECKOUT_POLICY = ROOT / "config/architecture/arch_005_s4d_checkout_guard.yaml"
PARALLEL_POLICY = ROOT / "config/architecture/arch_005_parallel_control_policy.yaml"
TASK_ID = "DEVX-009_PARALLEL_INTEGRATION_PUBLICATION_FENCE_AND_GENERATED_STATE_REBUILD_V1"


@pytest.fixture
def publication_checkout(tmp_path: Path) -> Path:
    repository = tmp_path / "repository"
    repository.mkdir()
    (repository / "src").mkdir()
    (repository / "src/a.py").write_text("VALUE = 1\n", encoding="utf-8")
    (repository / "docs").mkdir()
    (repository / "docs/task_register.md").write_text("task v1\n", encoding="utf-8")
    (repository / "inputs").mkdir()
    (repository / "inputs/generated.json").write_text('{"version": 1}\n', encoding="utf-8")
    (repository / ".gitignore").write_text("outputs/\n", encoding="utf-8")
    _git(repository, "init", "-b", "main")
    _git(repository, "config", "user.email", "publication-fence@example.com")
    _git(repository, "config", "user.name", "Publication Fence Test")
    _git(repository, "add", ".")
    _git(repository, "commit", "-m", "fixture")
    remote = tmp_path / "origin.git"
    subprocess.run(
        ["git", "init", "--bare", str(remote)],
        check=True,
        capture_output=True,
        text=True,
    )
    _git(repository, "remote", "add", "origin", str(remote))
    _git(repository, "push", "-u", "origin", "main")
    _git(repository, "switch", "-c", "codex/publication-test")
    return repository


def test_policy_reuses_s4d_lease_authority_and_freezes_no_unsafe_actions() -> None:
    policy = load_publication_fence_policy(DEFAULT_POLICY_PATH)

    assert policy.status == "OWNER_APPROVED_ENFORCED"
    assert policy.exclusive_validation_resource == "outputs/validation_runtime"
    assert policy.phase_order[0] == "ACQUIRED"
    assert policy.phase_order[-1] == "RELEASED"
    assert policy.heavyweight_tier == "full"


def test_stale_main_is_rejected_before_lease_or_shared_write(
    publication_checkout: Path,
) -> None:
    fence = _fence(publication_checkout)
    head = _git(publication_checkout, "rev-parse", "HEAD")

    with pytest.raises(PublicationFenceError) as error:
        _acquire(
            fence,
            publication_checkout,
            transaction_id="stale-main",
            expected_main="0" * 40,
        )

    assert error.value.code == "PUBLICATION_EXPECTED_MAIN_STALE"
    assert head == _git(publication_checkout, "rev-parse", "HEAD")
    assert fence.guard.replay().active_leases == ()


def test_concurrent_coordinators_allow_exactly_one_publication_transaction(
    publication_checkout: Path,
) -> None:
    barrier = Barrier(2)

    def acquire(transaction_id: str) -> tuple[str, str | None]:
        fence = _fence(publication_checkout)
        barrier.wait()
        try:
            binding = _acquire(
                fence,
                publication_checkout,
                transaction_id=transaction_id,
            )
        except PublicationFenceError as exc:
            return exc.code, None
        return "PASS", str(binding["transaction_path"])

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = tuple(executor.map(acquire, ("coordinator-a", "coordinator-b")))

    assert sorted(row[0] for row in results) == ["PASS", "PUBLICATION_LEASE_CONFLICT"]
    transaction_path = next(row[1] for row in results if row[1] is not None)
    fence = _fence(publication_checkout)
    fence.release(transaction_path, actor="integration-coordinator", outcome="failed")
    assert fence.guard.replay().active_leases == ()


def test_plan_tamper_fails_closed_before_task_source_mutation(
    publication_checkout: Path,
) -> None:
    plan = publication_checkout / "inputs/integration_plan.json"
    plan.write_text('{"plan_id":"plan-v1","decision":"READY"}\n', encoding="utf-8")
    _git(publication_checkout, "add", "inputs/integration_plan.json")
    _git(publication_checkout, "commit", "-m", "add plan")
    fence = _fence(publication_checkout)
    binding = _acquire(
        fence,
        publication_checkout,
        transaction_id="plan-tamper",
        integration_plan=plan,
        extra_shared=("inputs/integration_plan.json",),
    )
    transaction = Path(str(binding["transaction_path"]))

    plan.write_text('{"plan_id":"plan-v2","decision":"READY"}\n', encoding="utf-8")
    with pytest.raises(PublicationFenceError) as error:
        fence.validate(transaction, exact_phase="ACQUIRED")

    assert error.value.code == "PUBLICATION_PLAN_TAMPERED"
    fence.release(transaction, actor="integration-coordinator", outcome="failed")


def test_full_transaction_replays_candidate_publish_and_closeout_receipt(
    publication_checkout: Path,
) -> None:
    fence = _fence(publication_checkout)
    binding = _acquire(
        fence,
        publication_checkout,
        transaction_id="complete-publication",
    )
    transaction = Path(str(binding["transaction_path"]))

    fence.checkpoint(
        transaction,
        phase="TASK_SOURCE_PRE_WRITE",
        actor="integration-coordinator",
    )
    (publication_checkout / "docs/task_register.md").write_text(
        "task v2\n",
        encoding="utf-8",
    )
    fence.checkpoint(
        transaction,
        phase="GENERATED_REBUILD_PRE",
        actor="integration-coordinator",
        generator_ids=("canonical-task-source",),
        evidence_paths=(Path("docs/task_register.md"),),
    )
    (publication_checkout / "inputs/generated.json").write_text(
        '{"version": 2}\n',
        encoding="utf-8",
    )
    fence.checkpoint(
        transaction,
        phase="GENERATED_REBUILD_POST",
        actor="integration-coordinator",
        generator_ids=("canonical-task-source",),
        evidence_paths=(Path("docs/task_register.md"), Path("inputs/generated.json")),
    )
    fence.checkpoint(
        transaction,
        phase="CANDIDATE_COMMIT_PRE",
        actor="integration-coordinator",
    )
    _git(publication_checkout, "add", "docs/task_register.md", "inputs/generated.json")
    _git(publication_checkout, "commit", "-m", "candidate")
    candidate = _git(publication_checkout, "rev-parse", "HEAD")
    formal = fence.checkpoint(
        transaction,
        phase="FORMAL_VALIDATION_PRE",
        actor="integration-coordinator",
    )
    assert formal["candidate_sha"] == candidate
    dispatched = fence.checkpoint(
        transaction,
        phase="FULL_DISPATCHED",
        actor="integration-coordinator",
        full_run_id="full-fixture-v1",
    )
    assert dispatched["phase"] == "FULL_DISPATCHED"
    validated = fence.validate(
        transaction,
        exact_phase="FULL_DISPATCHED",
        task_id=TASK_ID,
        validation_tier="full",
        require_candidate=True,
    )
    assert validated["lease_id"] == binding["lease_id"]

    summary = publication_checkout / "outputs/validation_runtime/full/test_runtime_summary.json"
    summary.parent.mkdir(parents=True)
    summary.write_text('{"status":"PASS"}\n', encoding="utf-8")
    fence.checkpoint(
        transaction,
        phase="FORMAL_VALIDATION_RESULT",
        actor="integration-coordinator",
        evidence_paths=(summary,),
        validation_status="PASS",
    )
    fence.checkpoint(
        transaction,
        phase="LOCAL_MAIN_FF_PRE",
        actor="integration-coordinator",
    )
    _git(publication_checkout, "switch", "main")
    _git(publication_checkout, "merge", "--ff-only", candidate)
    _git(publication_checkout, "fetch", "origin", "main")
    fence.checkpoint(
        transaction,
        phase="REMOTE_PUSH_PRE",
        actor="integration-coordinator",
    )
    _git(publication_checkout, "push", "origin", "main")
    fence.checkpoint(
        transaction,
        phase="CLEANUP_PRE",
        actor="integration-coordinator",
    )
    receipt = fence.release(
        transaction,
        actor="integration-coordinator",
        outcome="completed",
        evidence_paths=(summary,),
    )

    assert receipt["status"] == "PASS"
    assert receipt["candidate_sha"] == candidate
    assert receipt["lease_state"] == "RELEASED"
    assert fence.replay(transaction).phase == "RELEASED"
    assert (
        fence.release(
            transaction,
            actor="integration-coordinator",
            outcome="completed",
            evidence_paths=(summary,),
        )
        == receipt
    )


def test_append_only_event_tamper_is_detected(
    publication_checkout: Path,
) -> None:
    fence = _fence(publication_checkout)
    binding = _acquire(
        fence,
        publication_checkout,
        transaction_id="event-tamper",
    )
    transaction = publication_checkout / str(binding["transaction_path"])
    event = next((transaction.parent / "events").glob("*.json"))
    payload = json.loads(event.read_text(encoding="utf-8"))
    payload["payload"]["observed_main"] = "f" * 40
    event.write_text(json.dumps(payload), encoding="utf-8")

    replay = fence.replay(transaction)
    assert replay.status == "FAIL"
    assert any("PUBLICATION_EVENT_HASH_MISMATCH" in row for row in replay.issues)


def _fence(repository: Path) -> IntegrationPublicationFence:
    return IntegrationPublicationFence(
        project_root=repository,
        policy_path=DEFAULT_POLICY_PATH,
        checkout_guard_policy_path=CHECKOUT_POLICY,
        parallel_control_policy_path=PARALLEL_POLICY,
    )


def _acquire(
    fence: IntegrationPublicationFence,
    repository: Path,
    *,
    transaction_id: str,
    expected_main: str | None = None,
    integration_plan: Path | None = None,
    extra_shared: tuple[str, ...] = (),
) -> dict[str, object]:
    head = _git(repository, "rev-parse", "HEAD")
    main = _git(repository, "rev-parse", "main")
    return fence.acquire(
        transaction_id=transaction_id,
        task_id=TASK_ID,
        change_id=f"{transaction_id}-change",
        thread_id=f"{transaction_id}-thread",
        actor="integration-coordinator",
        frozen_base_sha=main,
        lane_head_sha=head,
        expected_main_sha=expected_main or main,
        owned_paths=("src/a.py",),
        shared_paths=(
            "docs/task_register.md",
            "inputs/generated.json",
            *extra_shared,
        ),
        generator_ids=("canonical-task-source",),
        integration_plan_path=integration_plan,
    )


def _git(repository: Path, *args: str) -> str:
    completed = subprocess.run(
        ["git", *args],
        cwd=repository,
        check=True,
        capture_output=True,
        text=True,
    )
    return completed.stdout.strip()
