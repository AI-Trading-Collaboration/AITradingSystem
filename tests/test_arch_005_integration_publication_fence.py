from __future__ import annotations

import hashlib
import inspect
import json
import os
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Barrier
from typing import Any

import pytest
from test_devx015_workflow_integration import (
    canonical_merge_repository as canonical_merge_repository,
)
from test_devx015_workflow_integration import (
    small_repository as small_repository,
)

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
    for policy in (DEFAULT_POLICY_PATH, CHECKOUT_POLICY, PARALLEL_POLICY):
        target = repository / "config/architecture" / policy.name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(policy.read_bytes())
    _git(repository, "init", "-b", "main")
    _git(repository, "config", "user.email", "publication-fence@example.com")
    _git(repository, "config", "user.name", "Publication Fence Test")
    # Synthetic text has a repository-local EOL contract even when the Git
    # system/global configuration is isolated; do not inherit host defaults.
    _git(repository, "config", "core.autocrlf", "true")
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


def test_publication_checkout_inspection_does_not_block_execution_arbiter(
    publication_checkout: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Git inspection must not exclude an already admitted worker's transition."""
    from datetime import UTC, datetime

    from ai_trading_system.platform.architecture import checkout_guard

    fence = _fence(publication_checkout)
    original = checkout_guard.collect_checkout_dirty_paths
    observed = []

    def inspect(*args: Any, **kwargs: Any) -> Any:
        def worker_transition() -> None:
            with fence.guard.store.atomic(
                actor="integration-coordinator", now=datetime.now(UTC),
                operation="execution_worker_admit",
            ):
                observed.append("ADMITTED")

        with ThreadPoolExecutor(max_workers=1) as executor:
            executor.submit(worker_transition).result(timeout=10)
        return original(*args, **kwargs)

    monkeypatch.setattr(checkout_guard, "collect_checkout_dirty_paths", inspect)
    binding = _acquire(fence, publication_checkout, transaction_id="inspection-lock-scope")
    try:
        assert observed
    finally:
        monkeypatch.setattr(checkout_guard, "collect_checkout_dirty_paths", original)
        fence.release(
            binding["transaction_path"], actor="integration-coordinator", outcome="failed",
        )


@pytest.mark.parametrize("changed_ref", ["HEAD", "main"])
def test_publication_acquire_rechecks_identity_after_unlocked_inspection(
    publication_checkout: Path, monkeypatch: pytest.MonkeyPatch, changed_ref: str,
) -> None:
    fence = _fence(publication_checkout)
    original = fence.guard.acquire
    old_head = _git(publication_checkout, "rev-parse", "HEAD")
    tree = _git(publication_checkout, "rev-parse", "HEAD^{tree}")
    replacement = _git(
        publication_checkout, "commit-tree", tree, "-p", old_head, "-m", "identity race",
    )

    def changed(*args: Any, **kwargs: Any) -> Any:
        decision, handle = original(*args, **kwargs)
        assert handle is not None
        ref = "HEAD" if changed_ref == "HEAD" else "refs/heads/main"
        _git(publication_checkout, "update-ref", ref, replacement, old_head)
        return decision, handle

    monkeypatch.setattr(fence.guard, "acquire", changed)
    with pytest.raises(PublicationFenceError) as error:
        _acquire(fence, publication_checkout, transaction_id="inspection-identity-race")
    assert error.value.code == "PUBLICATION_ACQUIRE_IDENTITY_CHANGED"
    assert fence.guard.replay().active_leases == ()
    assert not (fence.runtime_root / "transactions/inspection-identity-race").exists()


def test_publication_writer_gate_precedes_unlocked_git_inspection(
    publication_checkout: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from ai_trading_system.platform.architecture import integration_publication_fence as module
    from ai_trading_system.platform.architecture.parallel_control import ParallelControlError

    fence = _fence(publication_checkout)

    def reject(operation: str) -> None:
        raise ParallelControlError("WORKFLOW_CONTROL_DRAIN_REQUIRED", operation)

    def forbidden(*args: Any, **kwargs: Any) -> str:
        pytest.fail("unadmitted writer reached Git inspection")

    monkeypatch.setattr(fence.guard.store, "_assert_writer", reject)
    monkeypatch.setattr(module, "_git", forbidden)
    with pytest.raises(PublicationFenceError) as error:
        _acquire(fence, publication_checkout, transaction_id="writer-before-inspection")
    assert error.value.code == "WORKFLOW_CONTROL_DRAIN_REQUIRED"
    assert not fence.guard.store.events_root.exists()


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


@pytest.mark.parametrize("installed", [False, True])
def test_profile_child_command_uses_fixed_installed_runtime(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, installed: bool,
) -> None:
    from types import SimpleNamespace

    from ai_trading_system.platform.architecture import integration_publication_fence as module

    runtime = tmp_path / "runtime"
    project = runtime / "project"
    script = project / "scripts" / "run_validation_tier.py"
    script.parent.mkdir(parents=True)
    script.write_text("# unit command witness\n", encoding="utf-8")
    origin = (project
              / "src/ai_trading_system/platform/architecture/integration_publication_fence.py")
    executable = (runtime if installed else tmp_path / "development") / "python.exe"
    monkeypatch.setattr(module, "__file__", str(origin))
    monkeypatch.setattr(module, "sys", SimpleNamespace(executable=str(executable)))
    candidate = tmp_path / "candidate"
    command = module._full_profile_inspector_command(candidate, candidate / "txn.json", "unit")
    assert command[:2] == [str(executable), "-I"]
    assert str(script) in command
    assert str(candidate / "scripts/run_validation_tier.py") not in command
    assert ("--protected-inspector" in command) is installed
    assert ("-S" in command) is installed
    assert ("-B" in command) is installed


@pytest.mark.parametrize("installed", [False, True])
def test_profile_child_timeout_refuses_publication_with_bounded_mode_budget(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, installed: bool,
) -> None:
    from types import SimpleNamespace

    from ai_trading_system.platform.architecture import integration_publication_fence as module

    events = [{"phase": "FORMAL_VALIDATION_RESULT", "payload": {"validation_status": "PASS"}}]
    replay = SimpleNamespace(
        transaction={"actor": "unit", "task_id": "unit"},
        phase="FORMAL_VALIDATION_RESULT", events=events,
    )
    witness = SimpleNamespace(
        project_root=tmp_path, replay=lambda *args: replay,
        _active_lease=lambda *args, **kwargs: None, validate=lambda *args, **kwargs: None,
        _require_clean_candidate=lambda: None, _transaction_path=lambda path: path,
    )
    command = [sys.executable, "fixed-inspector"]
    if installed:
        command.append("--protected-inspector")
    monkeypatch.setattr(module, "_full_profile_inspector_command", lambda *args: command)
    # Caller loaded-source custody is covered separately; this unit checks only the
    # inspector budget, independent of code other tests left loaded in this worker.
    from ai_trading_system.platform.architecture import workflow_execution

    monkeypatch.setattr(workflow_execution, "acceptance_runtime_identity", lambda: {})
    calls = []

    def timeout_run(argv, **kwargs):
        calls.append(argv)
        assert kwargs["timeout"] == (360 if installed else 180)
        assert kwargs["cwd"] == (Path(sys.executable).absolute().parent if installed else tmp_path)
        raise subprocess.TimeoutExpired(argv, kwargs["timeout"])

    monkeypatch.setattr(module.subprocess, "run", timeout_run)
    with pytest.raises(PublicationFenceError, match="PUBLICATION_FULL_CLOSURE_INVALID"):
        IntegrationPublicationFence._prepare_current_full_profile(
            witness, tmp_path / "unit.json", actor="unit",
        )
    assert calls == [command]
    assert events == [{"phase": "FORMAL_VALIDATION_RESULT",
                       "payload": {"validation_status": "PASS"}}]


def test_profile_caller_loaded_source_custody_refuses_before_inspector(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The isolated (-I) inspector cannot see caller startup code; the caller must."""
    from types import SimpleNamespace

    from ai_trading_system.platform.architecture import integration_publication_fence as module
    from ai_trading_system.platform.architecture import workflow_execution

    events = [{"phase": "FORMAL_VALIDATION_RESULT", "payload": {"validation_status": "PASS"}}]
    replay = SimpleNamespace(
        transaction={"actor": "unit", "task_id": "unit"},
        phase="FORMAL_VALIDATION_RESULT", events=events,
    )
    witness = SimpleNamespace(
        project_root=tmp_path, replay=lambda *args: replay,
        _active_lease=lambda *args, **kwargs: None, validate=lambda *args, **kwargs: None,
        _require_clean_candidate=lambda: None, _transaction_path=lambda path: path,
    )

    def changed_origin():
        raise workflow_execution.ExecutionContainmentError(
            "ACCEPTANCE_LOADED_CODE_ORIGIN", "synthetic caller drift",
        )

    monkeypatch.setattr(workflow_execution, "acceptance_runtime_identity", changed_origin)
    calls = []
    monkeypatch.setattr(module.subprocess, "run", lambda argv, **kwargs: calls.append(argv))
    with pytest.raises(PublicationFenceError, match="ACCEPTANCE_LOADED_CODE_ORIGIN"):
        IntegrationPublicationFence._prepare_current_full_profile(
            witness, tmp_path / "unit.json", actor="unit",
        )
    assert calls == []


def _v03_cli(
    repository: Path, label: str, script: str, args: list[str], *, timeout: float = 60,
) -> dict[str, Any]:
    environment = dict(os.environ)
    code_root = ROOT
    if (repository / "scripts" / script).is_file() and (
        repository / "src/ai_trading_system/platform/architecture/integration_publication_fence.py"
    ).is_file():
        # A real coordinator runs from the checkout it publishes. The isolated
        # profile inspector (-I) runs the caller's own implementation, which must
        # then be the candidate; lightweight checkouts without it keep ROOT code.
        code_root = repository
    environment.update(PYTHONPATH=str(code_root / "src"), PYTHONDONTWRITEBYTECODE="1",
                       GIT_CONFIG_NOSYSTEM="1", GIT_CONFIG_GLOBAL=os.devnull,
                       GIT_OPTIONAL_LOCKS="0")
    bootstrap = repository.parent / "native-host-bootstrap"
    if (bootstrap / "sitecustomize.py").is_file():
        environment["PYTHONPATH"] = str(bootstrap) + os.pathsep + environment["PYTHONPATH"]
    command = [sys.executable, str(code_root / "scripts" / script), *args]
    result = subprocess.run(
        command, cwd=repository, env=environment, capture_output=True, text=True, timeout=timeout,
    )
    record = {"argv": command, "exit_code": result.returncode,
              "stdout": result.stdout, "stderr": result.stderr}
    (repository.parent / (label + ".json")).write_text(json.dumps(record), encoding="utf-8")
    return record


def _v03_fence_cli(
    repository: Path, label: str, args: list[str], *, timeout: float = 60,
) -> dict[str, Any]:
    return _v03_cli(repository, label, "architecture_arch005_publication_fence.py", [
        "--repository", str(repository), "--policy", str(DEFAULT_POLICY_PATH), *args,
    ], timeout=timeout)


def _v03_unchanged_state(fence: IntegrationPublicationFence, transaction: Path) -> dict[str, Any]:
    repository = fence.project_root
    paths = [*transaction.parent.rglob("*.json"), *fence.guard.runtime_root.rglob("*.json")]
    return {"refs": _git(repository, "show-ref"),
            "index": (repository / ".git/index").read_bytes(),
            "governance": {str(path): path.read_bytes() for path in paths}}


@pytest.mark.parametrize("command", [
    "local-publication-inspect", "local-publication-recover-index",
])
def test_local_publication_inspection_cli_requires_original_ready_phase(
    publication_checkout, command,
):
    root = publication_checkout
    fence = _fence(root)
    binding = _acquire(fence, root, transaction_id="local-inspection-not-ready")
    transaction = root / str(binding["transaction_path"])
    before = _v03_unchanged_state(fence, transaction)
    try:
        result = _v03_fence_cli(root, "local-inspection-refusal", [
            command, "--transaction", str(transaction),
        ])
        assert result["exit_code"] == 2, result
        assert "PUBLICATION_PHASE" in result["stdout"]
        assert _v03_unchanged_state(fence, transaction) == before
    finally:
        fence.release(transaction, actor="integration-coordinator", outcome="failed")


@pytest.mark.parametrize("outcome", ["commit", "abort", "stale-old"])
def test_git_prepared_main_update_retains_owned_lock_identity(publication_checkout, outcome):
    root = publication_checkout
    old = _git(root, "rev-parse", "main")
    source = root / "src/a.py"
    source.write_text("VALUE = 2\n", encoding="utf-8")
    _git(root, "add", "--", "src/a.py")
    _git(root, "commit", "-m", "candidate for prepared ref protocol")
    candidate = _git(root, "rev-parse", "HEAD")
    competing = _git(root, "commit-tree", _git(root, "rev-parse", old + "^{tree}"),
                     "-p", old, "-m", "independent competing main")
    index = (root / ".git/index").read_bytes()
    worktree = source.read_bytes()
    ref = root / ".git/refs/heads/main"
    lock = root / ".git/refs/heads/main.lock"
    argv = ["git", "update-ref", "--stdin"]
    commands = f"start\nupdate refs/heads/main {candidate} {old}\nprepare\n"
    evidence = {"argv": argv, "old": old, "candidate": candidate, "competing": competing,
                "outcome": outcome}
    if outcome == "stale-old":
        _git(root, "update-ref", "refs/heads/main", competing, old)
        result = subprocess.run(argv, cwd=root, input=(commands + "commit\n").encode("ascii"),
                                capture_output=True, timeout=20)
        evidence.update(exit_code=result.returncode, stdout=result.stdout.decode("utf-8"),
                        stderr=result.stderr.decode("utf-8"))
        assert result.returncode != 0
        assert f"is at {competing} but expected {old}".encode("ascii") in result.stderr
        assert _git(root, "rev-parse", "main") == competing
        assert not lock.exists()
    else:
        process = subprocess.Popen(argv, cwd=root, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE)
        reader = ThreadPoolExecutor(max_workers=1)
        try:
            assert process.stdin is not None and process.stdout is not None
            process.stdin.write(commands.encode("ascii"))
            process.stdin.flush()
            acknowledgements = [reader.submit(process.stdout.readline).result(timeout=15)
                                for _ in range(2)]
            assert acknowledgements == [b"start: ok\n", b"prepare: ok\n"]
            assert process.poll() is None
            prepared = lock.stat()
            prepared_identity = (prepared.st_dev, prepared.st_ino)
            evidence.update(acknowledgements=[item.decode("ascii") for item in acknowledgements],
                            process_id=process.pid,
                            prepared_identity=list(prepared_identity),
                            prepared_bytes_hex=lock.read_bytes().hex())
            assert ref.read_text().strip() == old
            rival = subprocess.run(["git", "update-ref", "refs/heads/main", competing, old],
                                   cwd=root, capture_output=True, text=True, timeout=20)
            evidence["rival"] = {"exit_code": rival.returncode, "stdout": rival.stdout,
                                 "stderr": rival.stderr}
            assert rival.returncode != 0 and "main.lock" in rival.stderr
            assert ref.read_text().strip() == old
            assert (lock.stat().st_dev, lock.stat().st_ino) == prepared_identity
            stdout, stderr = process.communicate((outcome + "\n").encode("ascii"), timeout=20)
            evidence.update(exit_code=process.returncode, stdout=stdout.decode("utf-8"),
                            stderr=stderr.decode("utf-8"))
            assert process.returncode == 0, evidence
            assert stdout == (outcome + ": ok\n").encode("ascii")
            assert not lock.exists()
            assert _git(root, "rev-parse", "main") == (candidate if outcome == "commit" else old)
            if outcome == "commit":
                assert (ref.stat().st_dev, ref.stat().st_ino) == prepared_identity
                assert ref.read_text().strip() == candidate
        finally:
            if process.poll() is None:
                process.kill()
                process.wait(timeout=20)
            reader.shutdown(wait=True, cancel_futures=True)
            for stream in (process.stdin, process.stdout, process.stderr):
                if stream is not None:
                    stream.close()
    assert _git(root, "rev-parse", "HEAD") == candidate
    assert (root / ".git/index").read_bytes() == index
    assert source.read_bytes() == worktree
    (root.parent / "git-prepared-ref-evidence.json").write_text(
        json.dumps(evidence), encoding="utf-8"
    )


@pytest.mark.parametrize("custody_mode,reader", [
    ("none", "none"), ("root", "none"), ("common", "none"), ("refs", "none"),
    ("none", "bounded"), ("common", "bounded"), ("all", "bounded"), ("all", "ordinary"),
])
def test_git_orig_head_commit_under_native_custody(publication_checkout, custody_mode, reader):
    """Minimal real ORIG_HEAD commit, not a new Full or publication acceptance claim."""
    import shlex
    from contextlib import ExitStack

    from ai_trading_system.platform.architecture.workflow_contract import hold_bound_directory

    root = publication_checkout
    common = root / ".git"
    main = _git(root, "rev-parse", "main")
    hooks = root / "outputs/orig-head-repro-hooks"
    hooks.mkdir(parents=True)
    if reader != "none":
        probe = hooks / "probe.py"
        probe.write_text(
            "import json,sys\nfrom pathlib import Path\n"
            "from ai_trading_system.platform.architecture.workflow_contract "
            "import bounded_regular_bytes\n"
            "updates=sys.stdin.buffer.read()\n"
            "if sys.argv[1]=='prepared':\n"
            f" path=Path({(common / 'ORIG_HEAD.lock').as_posix()!r})\n"
            " stat=path.stat()\n"
            + (" raw=bounded_regular_bytes(path,expected_identity=(stat.st_dev,stat.st_ino))\n"
               if reader == "bounded" else " raw=path.read_bytes()\n")
            + f" assert raw=={(main + chr(10)).encode()!r}\n"
            "print(json.dumps({'stage':sys.argv[1],'updates_hex':updates.hex()}))\n",
            encoding="utf-8",
        )
        (hooks / "reference-transaction").write_text(
            "#!/bin/sh\nexec " + shlex.quote(Path(sys.executable).as_posix()) + " -B "
            + shlex.quote(probe.as_posix()) + ' "$1"\n', encoding="utf-8", newline="\n",
        )
    environment = {key: value for key, value in os.environ.items()
                   if not key.upper().startswith("GIT_")}
    environment.update(GIT_CONFIG_NOSYSTEM="1", GIT_CONFIG_GLOBAL=os.devnull,
                       GIT_OPTIONAL_LOCKS="0", PYTHONDONTWRITEBYTECODE="1",
                       PYTHONPATH=str(ROOT / "src"))
    targets = {"none": [], "root": [root], "common": [common],
               "refs": [common / "refs/heads"],
               "all": [root, common, common / "refs", common / "refs/heads"]}[custody_mode]
    with ExitStack() as stack:
        for path in targets:
            current, parent = path.stat(), path.parent.stat()
            stack.enter_context(hold_bound_directory(
                path.parent, path.name, expected_identity=(current.st_dev, current.st_ino),
                expected_root_identity=(parent.st_dev, parent.st_ino),
                expected_parent_identities={},
                allow_child_updates=True,
            ))
        argv = ["git", "-c", "core.hooksPath=" + hooks.as_posix(),
                "-c", "maintenance.auto=false", "update-ref", "ORIG_HEAD", main]
        result = subprocess.run(argv, cwd=root, env=environment, capture_output=True, text=True,
                                timeout=60, creationflags=subprocess.CREATE_NO_WINDOW)
        for path in targets:
            with pytest.raises(OSError) as denied:
                path.rename(path.with_name(path.name + "-unexpected-move"))
            assert denied.value.winerror in {5, 32}
    observation = {"custody": custody_mode, "reader": reader, "argv": argv,
                   "exit_code": result.returncode, "stdout": result.stdout, "stderr": result.stderr,
                   "orig_exists": (common / "ORIG_HEAD").exists(),
                   "lock_exists": (common / "ORIG_HEAD.lock").exists()}
    (root.parent / "orig-head-native-commit.json").write_text(
        json.dumps(observation, sort_keys=True), encoding="utf-8",
    )
    assert result.returncode == 0, observation
    assert (common / "ORIG_HEAD").read_bytes() == (main + "\n").encode()
    assert not (common / "ORIG_HEAD.lock").exists()


@pytest.mark.parametrize("scene", ["unchanged", "replaced", "advanced", "published"])
def test_local_recovery_dispatch_selects_actual_index_state(monkeypatch, tmp_path, scene):
    """Cheap dispatcher check only: synthetic authority is explicitly not Full evidence."""
    from types import SimpleNamespace

    from ai_trading_system.platform.architecture import workflow_integration as integration
    from ai_trading_system.platform.architecture.workflow_coordination import PublicationLifecycle

    lifecycle = PublicationLifecycle.__new__(PublicationLifecycle)
    lifecycle.fence = SimpleNamespace(
        project_root=tmp_path, _transaction_path=lambda value: value,
        replay=lambda value: SimpleNamespace(status="PASS", transaction={"actor": "owner",
                                                                        "lease_id": "shape-only"}),
    )
    original_index = {"path": "shape-only-index", "identity": [1, 2]}
    current_index = ({**original_index, "identity": [1, 3]}
                     if scene == "replaced" else original_index)
    request = {"candidate_sha": "c" * 40, "expected_main_sha": "a" * 40}
    calls = []
    monkeypatch.setattr(lifecycle, "recover", lambda lease, **kwargs: {"status": "REPLAY_ONLY"})
    monkeypatch.setattr(lifecycle, "_head", lambda lease, actor: SimpleNamespace(
        execution={"request": request, "checkout_effect": {}},
    ))
    monkeypatch.setattr(lifecycle, "_restore_failed_publication_heads",
                        lambda lease, **kwargs: calls.append("restore"))
    monkeypatch.setattr(lifecycle, "_require_original_publication", lambda checked, actor: {
        "intent": {"topology": {"candidate_checkout": {"index": original_index}}},
    })
    for name, label in (("adopt_unchanged_failed_attempt", "unchanged"),
                        ("adopt_index_replaced_failed_attempt", "replaced"),
                        ("adopt_main_advanced_failed_attempt", "advanced"),
                        ("adopt_published_attempt", "published")):
        monkeypatch.setattr(
            lifecycle, name, lambda lease, label=label, **kwargs: {"selected": label},
        )
    main = "b" * 40 if scene == "advanced" else "c" * 40 if scene == "published" else "a" * 40
    monkeypatch.setattr(integration, "_git", lambda *args: (main + "\n").encode())
    monkeypatch.setattr(integration, "inspect_local_publication_topology", lambda *args, **kwargs: {
        "candidate_checkout": {"index": current_index},
    })
    # An advanced main restores the retained candidate first, then routes to the
    # independent CANDIDATE_RETAINED_MAIN_ADVANCED adopter (never a new dispatch).
    assert lifecycle.recover_local_publication(tmp_path / "shape-only.json", actor="owner") == {
        "selected": scene,
    }
    assert calls == ([] if scene == "published" else ["restore"])


@pytest.mark.parametrize("target_name", ["common", "refs", "heads"])
@pytest.mark.parametrize("completion", ["abort", "commit"])
def test_git_inherits_directory_custody_after_parent_handles_close(
    publication_checkout, target_name, completion,
):
    from ai_trading_system.platform.architecture.workflow_contract import hold_bound_directory

    root = publication_checkout
    common = root / ".git"
    refs, heads = common / "refs", common / "refs/heads"

    def identity(path):
        info = path.stat()
        return info.st_dev, info.st_ino

    before_refs = _git(root, "show-ref")
    before_index = (common / "index").read_bytes()
    main = _git(root, "rev-parse", "main")
    candidate = _git(root, "commit-tree", main + "^{tree}", "-p", main,
                     "-m", "directory custody candidate")
    target = {"common": common, "refs": refs, "heads": heads}[target_name]
    moved = target.with_name(target.name + "-moved")
    evidence = {"target": str(target), "main": main, "candidate": candidate, "attempts": []}

    def refused_rename(label):
        program = (
            "import json,os,sys\n"
            "try: os.rename(sys.argv[1],sys.argv[2])\n"
            "except OSError as error: print(json.dumps({'winerror':error.winerror}))\n"
            "else: print(json.dumps({'unexpected_move':True}))\n"
        )
        observed = subprocess.run([sys.executable, "-c", program, str(target), str(moved)],
                                  cwd=root.parent, capture_output=True, text=True, timeout=20,
                                  creationflags=subprocess.CREATE_NO_WINDOW)
        assert observed.returncode == 0, observed.stderr
        fact = json.loads(observed.stdout)
        evidence["attempts"].append({"phase": label, **fact})
        assert fact.get("winerror") in {5, 32}, fact
        assert target.is_dir() and not moved.exists()

    process = None
    reader = ThreadPoolExecutor(max_workers=1)
    try:
        with hold_bound_directory(
            common, "refs/heads", expected_identity=identity(heads),
            expected_root_identity=identity(common),
            expected_parent_identities={"refs": identity(refs)},
            allow_child_updates=completion == "commit",
        ) as custody:
            with custody.subprocess_inheritance() as startup:
                process = subprocess.Popen(
                    ["git", "update-ref", "--stdin"], cwd=root, stdin=subprocess.PIPE,
                    stdout=subprocess.PIPE, stderr=subprocess.PIPE, startupinfo=startup,
                    close_fds=True, creationflags=subprocess.CREATE_NO_WINDOW,
                )
            assert process.stdin is not None and process.stdout is not None
            commands = f"start\nupdate refs/heads/main {candidate} {main}\nprepare\n".encode()
            process.stdin.write(commands)
            process.stdin.flush()
            acks = [reader.submit(process.stdout.readline).result(timeout=15) for _ in range(2)]
            assert acks == [b"start: ok\n", b"prepare: ok\n"]
            evidence["git_pid"] = process.pid
            refused_rename("parent-and-child")
        # The parent's verified and inheritable copies are all closed. The
        # original live Git child alone must keep the directory chain pinned.
        assert process.poll() is None
        refused_rename("only-inherited-git-handles")
        stdout, stderr = process.communicate((completion + "\n").encode(), timeout=20)
        assert process.returncode == 0 and stdout == (completion + ": ok\n").encode() and not stderr
        target.rename(moved)
        moved.rename(target)
        evidence["after_git_exit_rename"] = "SUCCEEDED_AND_RESTORED"
        if completion == "abort":
            assert _git(root, "show-ref") == before_refs
        else:
            assert _git(root, "rev-parse", "main") == candidate
        assert (common / "index").read_bytes() == before_index
        assert not (heads / "main.lock").exists()
    finally:
        if process is not None:
            if process.poll() is None:
                process.kill()
                process.wait(timeout=20)
            for stream in (process.stdin, process.stdout, process.stderr):
                if stream is not None:
                    stream.close()
        reader.shutdown(wait=True, cancel_futures=True)
        (root.parent / "git-directory-custody-evidence.json").write_text(json.dumps(evidence))


@pytest.mark.parametrize("fault", ["root", "parent", "leaf", "body-exception"])
def test_directory_custody_rejection_and_exception_release_all_handles(publication_checkout, fault):
    from ai_trading_system.platform.architecture.workflow_contract import (
        WorkflowContractError,
        hold_bound_directory,
    )

    common = publication_checkout / ".git"
    refs, heads = common / "refs", common / "refs/heads"

    def identity(path):
        info = path.stat()
        return info.st_dev, info.st_ino

    expected = {"root": identity(common), "parent": identity(refs), "leaf": identity(heads)}
    if fault != "body-exception":
        expected[fault] = (expected[fault][0], expected[fault][1] + 1)
    error = RuntimeError if fault == "body-exception" else WorkflowContractError
    match = {"root": "OUTPUT_ROOT_IDENTITY_CHANGED", "parent": "OUTPUT_PARENT_IDENTITY_CHANGED",
             "leaf": "OUTPUT_FILE_IDENTITY", "body-exception": "owned body failure"}[fault]
    with pytest.raises(error, match=match):
        with hold_bound_directory(
            common, "refs/heads", expected_identity=expected["leaf"],
            expected_root_identity=expected["root"],
            expected_parent_identities={"refs": expected["parent"]},
        ):
            assert fault == "body-exception"
            raise RuntimeError("owned body failure")
    for target in (heads, refs, common):
        moved = target.with_name(target.name + "-released")
        target.rename(moved)
        moved.rename(target)


@pytest.mark.parametrize("operation", [
    "reserve", "worker", "checkout-plan", "hook-capsule", "materialize-hooks", "hold-hook-inputs",
    "ready-hooks", "main-advanced-window", "main-advanced-original",
])
def test_publication_lifecycle_requires_original_ready_event(publication_checkout, operation):
    from ai_trading_system.platform.architecture.workflow_coordination import PublicationLifecycle

    root = publication_checkout
    fence = _fence(root)
    binding = _acquire(fence, root, transaction_id="publication-lifecycle-not-ready")
    transaction = root / str(binding["transaction_path"])
    physical = fence.guard.replay().active_leases[0]
    lifecycle = PublicationLifecycle(fence)
    assert lifecycle.store is fence.guard.store
    # Correctly shaped fields are not Full evidence or a publish capability.
    request = {
        "schema_version": "workflow_execution_request.v5", "request_id": "not-ready-publication",
        "execution_kind": "CONTROLLED_LOCAL_PUBLICATION", "lease_id": physical.lease_id,
        "manifest_sha256": physical.change_manifest_sha256,
        "candidate_sha": _git(root, "rev-parse", "HEAD"),
        "argv": [sys.executable, "-c", "raise SystemExit(99)"], "cwd": root.as_posix(),
        "environment_sha256": "a" * 64, "stdout_path": (root / "outputs/pub.log").as_posix(),
        "result_path": (root / "outputs/pub-result.json").as_posix(),
        "job_name": "Local\\AITS-DEVX015-not-ready-publication", "host_id": "not-ready-host",
        "writer_epoch": "not-ready-epoch", "subject_task_id": physical.task_id,
        "task_authority_sha256": "b" * 64, "publication_transaction_path": transaction.as_posix(),
        "publication_transaction_sha256": binding["transaction_sha256"],
        "local_publication_event_id": "c" * 64, "local_publication_intent_sha256": "d" * 64,
        "full_execution_sha256": "e" * 64, "expected_main_sha": _git(root, "rev-parse", "main"),
        "publication_action": "PUBLISH", "publication_attempt": 1,
        "previous_publication_sha256": None,
    }
    before = _v03_unchanged_state(fence, transaction)
    try:
        with pytest.raises(PublicationFenceError, match="PUBLICATION_PHASE"):
            if operation == "reserve":
                lifecycle.reserve(request, actor="integration-coordinator")
            elif operation == "checkout-plan":
                lifecycle.record_checkout_plan(request, actor="integration-coordinator")
            elif operation == "hook-capsule":
                lifecycle.record_hook_capsule_definition(request, actor="integration-coordinator")
            elif operation == "materialize-hooks":
                lifecycle.materialize_hook_capsule(request, actor="integration-coordinator")
            elif operation == "hold-hook-inputs":
                with lifecycle.hold_hook_capsule_inputs(request, actor="integration-coordinator"):
                    raise AssertionError("unadmitted worker acquired publication inputs")
            elif operation == "ready-hooks":
                with lifecycle.prepare_hook_capsule_ready(request, actor="integration-coordinator"):
                    raise AssertionError("unadmitted worker recorded readiness")
            elif operation == "main-advanced-window":
                fence.validate_publication_main_advanced_recovery_window(transaction)
            elif operation == "main-advanced-original":
                lifecycle._require_original_publication(
                    request, "integration-coordinator", recovery_window=True,
                    main_advanced_recovery=True,
                )
            else:
                lifecycle.require_publication_worker(request, actor="integration-coordinator")
        assert _v03_unchanged_state(fence, transaction) == before
        assert fence.guard.replay().active_leases == (physical,)
        assert not Path(request["stdout_path"]).exists()
        assert not Path(request["result_path"]).exists()
    finally:
        fence.release(transaction, actor="integration-coordinator", outcome="failed")


def test_x04_original_waiter_retries_after_holder_release(publication_checkout):
    root = publication_checkout
    fence = _fence(root)
    head = _git(root, "rev-parse", "HEAD")
    main = _git(root, "rev-parse", "main")

    def acquire_args(name):
        return ["acquire", "--transaction-id", name, "--task-id", TASK_ID,
                "--change-id", name + "-change", "--thread-id", name + "-thread",
                "--frozen-base", main, "--lane-head", head, "--expected-main", main,
                "--owned-path", "src/a.py", "--shared-path", "docs/task_register.md",
                "--shared-path", "inputs/generated.json", "--generator-id", "canonical-task-source"]

    holder = "x04-original-holder"
    waiter = "x04-original-waiter"
    holder_path = fence.runtime_root / "transactions" / holder / "transaction.json"
    waiter_path = fence.runtime_root / "transactions" / waiter / "transaction.json"
    initial_refs = _git(root, "show-ref")
    initial_index = (root / ".git/index").read_bytes()
    first = _v03_fence_cli(root, "x04-holder-acquire", acquire_args(holder))
    assert first["exit_code"] == 0, first
    try:
        blocked = _v03_fence_cli(root, "x04-waiter-blocked", acquire_args(waiter))
        assert blocked["exit_code"] == 2, blocked
        assert "PUBLICATION_LEASE_CONFLICT" in blocked["stdout"]
        waiting = [row for row in fence.guard.replay().lease_heads
                   if row.change_id == "checkout:publication-" + waiter]
        assert len(waiting) == 1 and waiting[0].state == "BLOCKED"
        assert not waiter_path.exists()
        original_blocked = waiting[0]
        blocked_root = fence.guard.store.events_root / original_blocked.lease_id
        blocked_paths = list(blocked_root.glob("*.json"))
        blocked_bytes = {str(path): path.read_bytes() for path in blocked_paths}
        before_retry = fence.guard.replay().lease_heads
        again = _v03_fence_cli(root, "x04-still-blocked", acquire_args(waiter))
        assert again["exit_code"] == 2 and "PUBLICATION_LEASE_CONFLICT" in again["stdout"]
        assert fence.guard.replay().lease_heads == before_retry
        released = _v03_fence_cli(root, "x04-holder-release", [
            "release", "--transaction", str(holder_path), "--outcome", "failed",
        ])
        assert released["exit_code"] == 0, released
        assert not fence.guard.replay().active_leases
        # Same complete request via a fresh original CLI process, after verified
        # release; no generation edits, substitute IDs or alternate store.
        retried = _v03_fence_cli(root, "x04-waiter-after-release", acquire_args(waiter))
        assert retried["exit_code"] == 0, retried
        admitted = json.loads(retried["stdout"])
        active = fence.guard.replay().active_leases
        assert len(active) == 1 and active[0].lease_id == admitted["lease_id"]
        assert active[0].previous_lease_id == original_blocked.lease_id
        assert active[0].generation == original_blocked.generation + 1
        assert {str(path): path.read_bytes() for path in blocked_paths} == blocked_bytes
        repeated = _v03_fence_cli(root, "x04-active-replay", acquire_args(waiter))
        assert repeated["exit_code"] == 0
        assert json.loads(repeated["stdout"])["lease_id"] == active[0].lease_id
        assert _git(root, "show-ref") == initial_refs
        assert (root / ".git/index").read_bytes() == initial_index
    finally:
        for transaction in (holder_path, waiter_path):
            if (transaction.exists()
                    and fence.replay(transaction).phase not in {"FAILED", "RELEASED"}):
                fence.release(transaction, actor="integration-coordinator", outcome="failed")


@pytest.mark.parametrize("fault", ["lane", "main"])
def test_x04_invalid_public_request_does_not_block_later_waiter(publication_checkout, fault):
    root = publication_checkout
    fence = _fence(root)
    head = _git(root, "rev-parse", "HEAD")
    main = _git(root, "rev-parse", "main")
    names = ["invalid-public-holder", "invalid-public-first", "valid-public-later"]
    paths = {name: fence.runtime_root / "transactions" / name / "transaction.json"
             for name in names}

    def args(name):
        return ["acquire", "--transaction-id", name, "--task-id", TASK_ID,
                "--change-id", name + "-change", "--thread-id", name + "-thread",
                "--frozen-base", main, "--lane-head", head, "--expected-main", main,
                "--owned-path", "src/a.py", "--shared-path", "docs/task_register.md",
                "--shared-path", "inputs/generated.json", "--generator-id", "canonical-task-source"]

    try:
        acquired = _v03_fence_cli(root, "invalid-public-holder", args(names[0]))
        assert acquired["exit_code"] == 0, acquired
        for name in names[1:]:
            blocked = _v03_fence_cli(root, name + "-blocked", args(name))
            assert blocked["exit_code"] == 2, blocked
            assert "PUBLICATION_LEASE_CONFLICT" in blocked["stdout"]
            assert not paths[name].exists()
        released = _v03_fence_cli(root, "invalid-public-holder-release", [
            "release", "--transaction", str(paths[names[0]]), "--outcome", "failed",
        ])
        assert released["exit_code"] == 0, released
        assert not fence.guard.replay().active_leases
        before = _v03_unchanged_state(fence, paths[names[0]])
        original_heads = fence.guard.replay().lease_heads
        malformed = args(names[1])
        flag = "--lane-head" if fault == "lane" else "--expected-main"
        malformed[malformed.index(flag) + 1] = "0" * 40
        denied = _v03_fence_cli(root, "invalid-public-refused", malformed)
        assert denied["exit_code"] == 2, denied
        expected = ("PUBLICATION_LANE_HEAD_DRIFT" if fault == "lane"
                    else "PUBLICATION_EXPECTED_MAIN_STALE")
        assert expected in denied["stdout"]
        after = _v03_unchanged_state(fence, paths[names[0]])
        assert after["refs"] == before["refs"] and after["index"] == before["index"]
        assert set(after["governance"]) == set(before["governance"])
        changed = {path for path in before["governance"]
                   if before["governance"][path] != after["governance"][path]}
        # Original CLI construction inspects the store under its OS arbiter.
        # Its released diagnostic owner is not a lease event or admission.
        owner_path = str(fence.guard.runtime_root / "leases/arbiter.owner.json")
        assert changed <= {owner_path}, changed
        for label, snapshot in (("before", before), ("after", after)):
            owner_raw = snapshot["governance"][owner_path]
            owner = json.loads(owner_raw)
            assert owner["schema_version"] == "execution_lease_os_arbiter_owner.v2"
            assert owner["state"] == "RELEASED"
            (root.parent / (label + "-arbiter-owner.json")).write_bytes(owner_raw)
        assert fence.guard.replay().lease_heads == original_heads
        admitted = _v03_fence_cli(root, "valid-public-later-admitted", args(names[2]))
        assert admitted["exit_code"] == 0, admitted
        active = fence.guard.replay().active_leases
        assert len(active) == 1 and active[0].change_id == "checkout:publication-" + names[2]
        assert active[0].previous_lease_id in {row.lease_id for row in original_heads
                                              if row.change_id == active[0].change_id}
        released = _v03_fence_cli(root, "valid-public-later-release", [
            "release", "--transaction", str(paths[names[2]]), "--outcome", "failed",
        ])
        assert released["exit_code"] == 0, released
        assert not fence.guard.replay().active_leases
        assert _git(root, "show-ref") == before["refs"]
        assert (root / ".git/index").read_bytes() == before["index"]
    finally:
        for transaction in paths.values():
            if (transaction.exists()
                    and fence.replay(transaction).phase not in {"FAILED", "RELEASED"}):
                fence.release(transaction, actor="integration-coordinator", outcome="failed")


@pytest.mark.parametrize("ordering", ["cancel-first", "race"])
def test_x04_public_cancel_preserves_later_waiter_and_serializes_retry(
    publication_checkout, ordering,
):
    root = publication_checkout
    fence = _fence(root)
    head, main = _git(root, "rev-parse", "HEAD"), _git(root, "rev-parse", "main")
    refs, index = _git(root, "show-ref"), (root / ".git/index").read_bytes()
    names = ("cancel-holder", "cancel-first", "cancel-later")
    paths = {name: fence.runtime_root / "transactions" / name / "transaction.json"
             for name in names}

    def acquire(name, label):
        return _v03_fence_cli(root, label, [
            "acquire", "--transaction-id", name, "--task-id", TASK_ID,
            "--change-id", name + "-change", "--thread-id", name + "-thread",
            "--frozen-base", main, "--lane-head", head, "--expected-main", main,
            "--owned-path", "src/a.py", "--shared-path", "docs/task_register.md",
            "--shared-path", "inputs/generated.json", "--generator-id", "canonical-task-source",
        ])

    def cancel(lease_id, label, actor="integration-coordinator"):
        return _v03_cli(root, label, "architecture_arch005_checkout_guard.py", [
            "cancel-request", "--repository", str(root), "--lease-id", lease_id,
            "--actor", actor,
        ])

    def release(name):
        result = _v03_fence_cli(root, name + "-release", [
            "release", "--transaction", str(paths[name]), "--outcome", "failed",
        ])
        assert result["exit_code"] == 0, result

    def lease(name):
        return next(row for row in fence.guard.replay().lease_heads
                    if row.change_id == "checkout:publication-" + name and row.generation == 1)

    try:
        assert acquire(names[0], "cancel-holder-acquire")["exit_code"] == 0
        for name in names[1:]:
            blocked = acquire(name, name + "-blocked")
            assert blocked["exit_code"] == 2 and "PUBLICATION_LEASE_CONFLICT" in blocked["stdout"]
            assert lease(name).state == "BLOCKED" and not paths[name].exists()
        first = lease(names[1])
        preserved = {p: p.read_bytes() for p in
                     (fence.guard.store.events_root / first.lease_id).glob("*.json")}
        original = tuple(fence.guard.replay().head_event_ids)
        wrong = cancel(first.lease_id, "cancel-wrong-actor", "wrong-actor")
        assert wrong["exit_code"] == 1 and "LEASE_ACTOR_MISMATCH" in wrong["stdout"]
        active_denied = cancel(lease(names[0]).lease_id, "cancel-active-holder")
        assert active_denied["exit_code"] == 1
        assert "LEASE_CANCEL_REQUIRES_BLOCKED" in active_denied["stdout"]
        assert tuple(fence.guard.replay().head_event_ids) == original
        if ordering == "cancel-first":
            cancelled = cancel(first.lease_id, "cancel-first-success")
            assert cancelled["exit_code"] == 0, cancelled
            terminal = tuple(fence.guard.replay().head_event_ids)
            replay = cancel(first.lease_id, "cancel-first-replay")
            assert replay["exit_code"] == 0 and replay["stdout"] == cancelled["stdout"]
            assert tuple(fence.guard.replay().head_event_ids) == terminal
            still_blocked = acquire(names[2], "later-holder-still-active")
            assert still_blocked["exit_code"] == 2
            release(names[0])
            retried = acquire(names[1], "cancelled-request-cannot-retry")
        else:
            release(names[0])
            barrier = Barrier(2)

            def competing_cancel():
                barrier.wait(timeout=10)
                return cancel(first.lease_id, "cancel-race")

            def competing_retry():
                barrier.wait(timeout=10)
                return acquire(names[1], "retry-race")

            with ThreadPoolExecutor(max_workers=2) as pool:
                c = pool.submit(competing_cancel)
                r = pool.submit(competing_retry)
                cancelled, retried = c.result(timeout=60), r.result(timeout=60)
            # The original arbiter rejects contention rather than queueing.
            # Resolve only an observed BUSY after both original processes exit,
            # using the same complete request; never infer a terminal decision.
            if cancelled["exit_code"] != 0 and "LEASE_ARBITER_BUSY" in cancelled["stdout"]:
                cancelled = cancel(first.lease_id, "cancel-after-contention")
            if retried["exit_code"] != 0 and "LEASE_ARBITER_BUSY" in retried["stdout"]:
                retried = acquire(names[1], "retry-after-contention")
        if cancelled["exit_code"] == 0:
            assert retried["exit_code"] == 2 and "LEASE_ALREADY_TERMINAL" in retried["stdout"]
            retired = lease(names[1])
            assert retired.state == "RELEASED" and retired.execution is None
            assert retired.acquired_at is None and not paths[names[1]].exists()
        else:
            assert ordering == "race" and retried["exit_code"] == 0, (cancelled, retried)
            assert "LEASE_CANCEL_SUPERSEDED" in cancelled["stdout"]
            admitted = fence.guard.replay().active_leases
            assert len(admitted) == 1 and admitted[0].previous_lease_id == first.lease_id
            release(names[1])
        assert not fence.guard.replay().active_leases
        later = acquire(names[2], "cancel-later-eligible")
        assert later["exit_code"] == 0, later
        active = fence.guard.replay().active_leases
        assert len(active) == 1 and active[0].previous_lease_id == lease(names[2]).lease_id
        before_stale_cancel = tuple(fence.guard.replay().head_event_ids)
        stale = cancel(lease(names[2]).lease_id, "cancel-superseded")
        assert stale["exit_code"] == 1 and "LEASE_CANCEL_SUPERSEDED" in stale["stdout"]
        assert tuple(fence.guard.replay().head_event_ids) == before_stale_cancel
        release(names[2])
        assert not fence.guard.replay().active_leases
        assert {p: p.read_bytes() for p in preserved} == preserved
        assert _git(root, "show-ref") == refs and (root / ".git/index").read_bytes() == index
    finally:
        for path in paths.values():
            if path.exists() and fence.replay(path).phase not in {"FAILED", "RELEASED"}:
                fence.release(path, actor="integration-coordinator", outcome="failed")


def test_x04_finite_competition_all_original_requests_progress(publication_checkout):
    root = publication_checkout
    fence = _fence(root)
    head = _git(root, "rev-parse", "HEAD")
    main = _git(root, "rev-parse", "main")
    names = ["finite-holder", "finite-first", "finite-second", "finite-third"]
    paths = {name: fence.runtime_root / "transactions" / name / "transaction.json"
             for name in names}
    refs = _git(root, "show-ref")
    index = (root / ".git/index").read_bytes()

    def acquire(name, label):
        return _v03_fence_cli(root, label, [
            "acquire", "--transaction-id", name, "--task-id", TASK_ID,
            "--change-id", name + "-change", "--thread-id", name + "-thread",
            "--frozen-base", main, "--lane-head", head, "--expected-main", main,
            "--owned-path", "src/a.py", "--shared-path", "docs/task_register.md",
            "--shared-path", "inputs/generated.json", "--generator-id", "canonical-task-source",
        ])

    original = {}
    retained = {}
    completed = []
    try:
        first = acquire(names[0], "finite-holder-acquire")
        assert first["exit_code"] == 0, first
        for name in names[1:]:
            denied = acquire(name, name + "-initial")
            assert denied["exit_code"] == 2, denied
            assert "PUBLICATION_LEASE_CONFLICT" in denied["stdout"]
            rows = [row for row in fence.guard.replay().lease_heads
                    if row.change_id == "checkout:publication-" + name]
            assert len(rows) == 1 and rows[0].state == "BLOCKED"
            original[name] = rows[0]
            retained.update({path: path.read_bytes() for path in
                             (fence.guard.store.events_root / rows[0].lease_id).glob("*.json")})
            assert not paths[name].exists()
        for position, holder in enumerate(names):
            active = fence.guard.replay().active_leases
            assert len(active) == 1
            assert active[0].change_id == "checkout:publication-" + holder
            if holder in original:
                assert active[0].previous_lease_id == original[holder].lease_id
                assert active[0].generation == original[holder].generation + 1
            # Every remaining original waiter actually retries under the current
            # holder; finite contention must not manufacture retry generations.
            for waiting in names[position + 1:]:
                before = fence.guard.replay().lease_heads
                denied = acquire(waiting, f"{holder}-{waiting}-blocked")
                assert denied["exit_code"] == 2, denied
                assert "PUBLICATION_LEASE_CONFLICT" in denied["stdout"]
                assert fence.guard.replay().lease_heads == before
            released = _v03_fence_cli(root, holder + "-release", [
                "release", "--transaction", str(paths[holder]), "--outcome", "failed",
            ])
            assert released["exit_code"] == 0, released
            assert fence.replay(paths[holder]).phase == "FAILED"
            assert not fence.guard.replay().active_leases
            completed.append(holder)
            if position + 1 < len(names):
                following = names[position + 1]
                admitted = acquire(following, following + "-eligible")
                assert admitted["exit_code"] == 0, admitted
        assert completed == names
        assert {path: path.read_bytes() for path in retained} == retained
        assert _git(root, "show-ref") == refs
        assert (root / ".git/index").read_bytes() == index
    finally:
        for transaction in paths.values():
            if (transaction.exists()
                    and fence.replay(transaction).phase not in {"FAILED", "RELEASED"}):
                fence.release(transaction, actor="integration-coordinator", outcome="failed")


@pytest.mark.parametrize("canonical_merge_repository", ["full-profile-publish"], indirect=True)
@pytest.mark.parametrize("stable_recovery", ["unchanged", "index-replaced"])
def test_publication_lifecycle_binds_original_actual_full(
    canonical_merge_repository, stable_recovery,
):
    from test_devx015_workflow_coordination import _run_actual_profile_full

    from ai_trading_system.platform.architecture.parallel_control import ParallelControlError
    from ai_trading_system.platform.architecture.workflow_contract import canonical_digest
    from ai_trading_system.platform.architecture.workflow_coordination import (
        PublicationLifecycle,
        full_execution_projection,
    )

    root, _scope = canonical_merge_repository
    fence = IntegrationPublicationFence(project_root=root)
    transaction = fence.runtime_root / "transactions/merge-authority/transaction.json"
    binding, directory, _driver, _environment = _run_actual_profile_full(root)
    fence.checkpoint(transaction, phase="LOCAL_MAIN_FF_PRE", actor="integration-coordinator")
    replay = fence.replay(transaction)
    original_event = replay.events[-1]
    physical = next(row for row in fence.guard.replay().active_leases
                    if row.lease_id == binding["lease_id"])
    original_full = full_execution_projection(physical.execution)
    assert physical.task_id != binding["task_id"]  # Lease authority != user's development task.
    request = {key: value for key, value in original_full["request"].items()
               if key != "validation_identity_sha256"}
    request.update(
        schema_version="workflow_execution_request.v5",
        request_id="actual-full-publication-binding",
        execution_kind="CONTROLLED_LOCAL_PUBLICATION",
        publication_transaction_path=transaction.as_posix(),
        publication_transaction_sha256=binding["transaction_sha256"],
        local_publication_event_id=original_event["event_id"],
        local_publication_intent_sha256=canonical_digest(
            original_event["payload"]["local_publication_intent"],
        ),
        full_execution_sha256=canonical_digest(original_full),
        expected_main_sha=binding["expected_main_sha"], publication_action="PUBLISH",
        publication_attempt=1, previous_publication_sha256=None,
        argv=[sys.executable, "-c", "raise SystemExit(99)"],
        stdout_path=(directory / "publication-binding.stdout").as_posix(),
        result_path=(directory / "publication-binding-result.json").as_posix(),
        job_name="Local\\AITS-DEVX015-actual-full-publication-binding",
    )
    lifecycle = PublicationLifecycle(fence)
    assert lifecycle.store is fence.guard.store
    witness = lifecycle._require_original_publication(request, "integration-coordinator")
    assert witness["event_id"] == original_event["event_id"]
    assert witness["full"] == original_full
    before = _v03_unchanged_state(fence, transaction)
    for field in ("publication_transaction_sha256", "local_publication_event_id",
                  "local_publication_intent_sha256", "full_execution_sha256", "manifest_sha256",
                  "task_authority_sha256", "writer_epoch", "expected_main_sha", "subject_task_id"):
        wrong = dict(request)
        wrong[field] = ("0" * 40 if field == "expected_main_sha"
                        else "stale-epoch" if field == "writer_epoch"
                        else binding["task_id"] if field == "subject_task_id" else "0" * 64)
        with pytest.raises(ParallelControlError, match="PUBLICATION_(ORIGINAL|FULL)_BINDING"):
            lifecycle.reserve(wrong, actor="integration-coordinator")
        assert _v03_unchanged_state(fence, transaction) == before
        assert fence.guard.replay().active_leases == (physical,)
    with pytest.raises(ParallelControlError, match="PUBLICATION_MISSING"):
        lifecycle.require_publication_worker(request, actor="integration-coordinator")
    assert fence.guard.replay().active_leases == (physical,)
    assert not Path(request["stdout_path"]).exists()
    assert not Path(request["result_path"]).exists()
    assert full_execution_projection(physical.execution) == original_full
    _exercise_failed_publication_job(
        fence, lifecycle, request, directory, original_full, stable_recovery=stable_recovery,
    )


def _publication_stage_trace_source() -> str:
    """Synchronous test diagnostics only; no timer or background stack traversal."""
    return (
        "import time\n"
        "_publication_stage_started=time.monotonic()\n"
        "def publication_stage(name):\n"
        " print('PUBLICATION_STAGE',name,"
        "round(time.monotonic()-_publication_stage_started,3),flush=True)\n"
    )


def test_publication_stage_trace_is_synchronous(capsys) -> None:
    scope = {}
    exec(_publication_stage_trace_source(), scope)
    assert capsys.readouterr().out == ""
    scope["publication_stage"]("first")
    scope["publication_stage"]("second")
    rows = [line.split() for line in capsys.readouterr().out.splitlines()]
    assert [row[:2] for row in rows] == [
        ["PUBLICATION_STAGE", "first"], ["PUBLICATION_STAGE", "second"],
    ]
    assert 0 <= float(rows[0][2]) <= float(rows[1][2])


def _publication_main_prepare_started(stdout_path: Path) -> bool:
    """Diagnostic phase boundary only; never a witness or effect authority."""
    if not stdout_path.exists():
        return False
    return any(
        line.split()[:2] == ["PUBLICATION_STAGE", "main_prepare_begin"]
        for line in stdout_path.read_text(encoding="utf-8").splitlines()
    )


def test_publication_phase_boundary_is_not_a_completion_witness(tmp_path: Path) -> None:
    stdout = tmp_path / "worker.stdout"
    assert not _publication_main_prepare_started(stdout)
    stdout.write_text("main_prepare_begin\nPUBLICATION_STAGE ready_held 232.5\n")
    assert not _publication_main_prepare_started(stdout)
    stdout.write_text("PUBLICATION_STAGE main_prepare_begin 290.766\n")
    assert _publication_main_prepare_started(stdout)
    assert not (tmp_path / "publication-worker-witness.json").exists()


def _publication_input_write_probe_source() -> str:
    """Independent Win32 OPEN_EXISTING probe; never truncate or modify input bytes."""
    return """
import ctypes,json,sys
from ctypes import wintypes as w
from pathlib import Path
api=ctypes.WinDLL('kernel32',use_last_error=True)
api.CreateFileW.argtypes=[w.LPCWSTR,w.DWORD,w.DWORD,ctypes.c_void_p,w.DWORD,w.DWORD,w.HANDLE]
api.CreateFileW.restype=w.HANDLE
api.CloseHandle.argtypes=[w.HANDLE]
api.CloseHandle.restype=w.BOOL
mode,output=sys.argv[1:3]
assert mode in ('writable','denied')
rows=[]
for path in sys.argv[3:]:
    ctypes.set_last_error(0)
    handle=api.CreateFileW(path,0x40000000,7,None,3,0,None)
    error=ctypes.get_last_error()
    if handle==ctypes.c_void_p(-1).value:
        assert mode=='denied' and error==32,(path,mode,error)
    else:
        assert api.CloseHandle(handle)
        assert mode=='writable',('protected input writable',path)
        error=0
    rows.append({'path':path,'winerror':error})
Path(output).write_text(json.dumps(rows))
"""


def test_publication_input_write_probe_uses_native_error(tmp_path: Path) -> None:
    from ai_trading_system.platform.architecture.workflow_contract import hold_bound_read_file

    path = tmp_path / "owned.py"
    raw = b"VALUE=1\n"
    path.write_bytes(raw)
    root_info, info = tmp_path.stat(), path.stat()

    def probe(mode: str, label: str) -> None:
        output = tmp_path / (label + ".json")
        result = subprocess.run(
            [sys.executable, "-c", _publication_input_write_probe_source(), mode,
             str(output), str(path)], capture_output=True, text=True, timeout=15, check=False,
        )
        assert result.returncode == 0, (result.stdout, result.stderr)
        assert json.loads(output.read_text()) == [
            {"path": str(path), "winerror": 32 if mode == "denied" else 0},
        ]

    probe("writable", "before")
    with hold_bound_read_file(
        tmp_path, path.name, expected=raw, expected_identity=(info.st_dev, info.st_ino),
        expected_root_identity=(root_info.st_dev, root_info.st_ino), expected_parent_identities={},
    ):
        probe("denied", "held")
    probe("writable", "released")
    assert path.read_bytes() == raw


def _exercise_failed_publication_job(
    fence, lifecycle, request, directory, original_full, *, stable_recovery,
):
    from datetime import UTC, datetime

    from test_devx015_workflow_execution import NativeOracle, _until

    from ai_trading_system.platform.architecture.parallel_control import ParallelControlError
    from ai_trading_system.platform.architecture.workflow_coordination import (
        execution_is_terminal,
        full_execution_projection,
    )
    from ai_trading_system.platform.architecture.workflow_execution import (
        WindowsJobProcess,
        execution_environment_sha256,
    )

    actor = "integration-coordinator"
    root = fence.project_root
    transaction = Path(request["publication_transaction_path"])
    request_path = directory / "publication-attempt-request.json"
    witness_path = directory / "publication-worker-witness.json"
    release_path = directory / "publication-worker.release"
    program = (
        "import json,sys,time\nfrom pathlib import Path\n"
        + _publication_stage_trace_source()
        + "publication_stage('worker_started')\n"
        "from ai_trading_system.platform.architecture.integration_publication_fence "
        "import IntegrationPublicationFence,PublicationFenceError\n"
        "from ai_trading_system.platform.architecture.workflow_coordination "
        "import PublicationLifecycle,_result_binding\n"
        "from ai_trading_system.platform.architecture.parallel_control "
        "import ParallelControlError\n"
        f"request=json.loads(Path({str(request_path)!r}).read_text())\n"
        "fence=IntegrationPublicationFence(project_root=Path.cwd())\n"
        "lifecycle=PublicationLifecycle(fence)\n"
        f"witness=lifecycle.require_publication_worker(request,actor={actor!r})\n"
        "publication_stage('worker_admitted')\n"
        f"plan=lifecycle.record_checkout_plan(request,actor={actor!r})\n"
        "assert plan['status']=='RECORDED' and plan['publication_allowed'] is False\n"
        f"replay=lifecycle.record_checkout_plan(request,actor={actor!r})\n"
        "assert replay['status']=='REPLAY_ONLY' and replay['binding']==plan['binding']\n"
        "witness['checkout_plan']=plan['binding']\n"
        f"capsule=lifecycle.record_hook_capsule_definition(request,actor={actor!r})\n"
        "assert capsule['status']=='RECORDED' and capsule['publication_allowed'] is False\n"
        f"replay=lifecycle.record_hook_capsule_definition(request,actor={actor!r})\n"
        "assert replay['status']=='REPLAY_ONLY' and replay['binding']==capsule['binding']\n"
        "witness['hook_capsule']=capsule['binding']\n"
        "assert not Path(capsule['binding']['definition']['directory']).exists()\n"
        "hookdir=Path(capsule['binding']['definition']['directory'])\n"
        "hookdir.mkdir(); (hookdir/'unknown-canary').write_bytes(b'preserve unknown object')\n"
        "try:\n"
        f" lifecycle.materialize_hook_capsule(request,actor={actor!r})\n"
        " raise AssertionError('unknown hook directory admitted')\n"
        "except OSError:\n"
        " assert (hookdir/'unknown-canary').read_bytes()==b'preserve unknown object'\n"
        " assert lifecycle._head(request['lease_id'],'integration-coordinator').execution["
        "'hook_capsule']['objects']==[]\n"
        "hookdir.rename(hookdir.with_name(hookdir.name+'-preserved-collision'))\n"
        f"materialized=lifecycle.materialize_hook_capsule(request,actor={actor!r})\n"
        "assert materialized['status']=='CREATED_NOT_READY'\n"
        "assert materialized['resume_allowed'] is False\n"
        "witness['hook_capsule']=materialized['binding']\n"
        "witness['unknown_hook_directory_preserved']=True\n"
        "try:\n"
        f" lifecycle.materialize_hook_capsule(request,actor={actor!r})\n"
        " raise AssertionError('repeated hook creation admitted')\n"
        "except ParallelControlError as exc:\n"
        " assert exc.code=='LEASE_EXECUTION_PUBLICATION_HOOK_CAPSULE_ALREADY_CREATED',str(exc)\n"
        "publication_stage('hooks_created')\n"
        "from ai_trading_system.platform.architecture.workflow_execution import InheritedJobChild\n"
        "from ai_trading_system.platform.architecture.workflow_contract import "
        "WorkflowContractError,canonical_digest\n"
        "before_inputs=fence.guard.replay().active_leases[0].execution\n"
        "publication_stage('ready_begin')\n"
        f"with lifecycle.prepare_hook_capsule_ready(request,actor={actor!r}) as (ready,files):\n"
        " publication_stage('ready_held')\n"
        " inputs=ready['inputs']\n"
        " assert inputs['resume_allowed'] is False and inputs['publication_allowed'] is False\n"
        " profile=inputs['profile_inspection']\n"
        " assert len(files)==inputs['runtime_file_count']+len(profile['captures'])+4\n"
        " assert profile['execution_sha256']==canonical_digest(before_inputs)\n"
        " after_inputs=json.loads(json.dumps(before_inputs))\n"
        " after_inputs['publication_attempts'][-1]['hook_capsule']['ready']=ready\n"
        " assert fence.guard.replay().active_leases[0].execution==after_inputs\n"
        " assert profile['execution_sha256']!=canonical_digest(after_inputs)\n"
        " targets=[str(Path('src/a.py').absolute()),str(hookdir/'reference-transaction'),"
        "inputs['profile_inspection']['captures'][0]['path']]\n"
        f" probe={_publication_input_write_probe_source()!r}\n"
        " probe_argv=[sys.executable,'-c',probe,'denied',\n"
        "  'outputs/publication-input-write-probe.json',*targets]\n"
        " child=InheritedJobChild.create(argv=probe_argv,\n"
        "  cwd=Path.cwd(),environment=dict(__import__('os').environ),\n"
        "  stdout_path=Path('outputs/input-child.stdout').absolute(),\n"
        "  job_name=request['job_name'],file_custodies=files)\n"
        " try:\n"
        "  child_binding=child.pre_resume_binding()\n"
        "  assert child_binding['read_file_custodies']==inputs['read_file_custodies']\n"
        "  Path('outputs/publication-live-inputs.json').write_text(json.dumps(inputs))\n"
        "  child_binding_path=Path('outputs/publication-input-child-binding.json')\n"
        "  child_binding_path.write_text(json.dumps(child_binding))\n"
        "  child.resume(); assert child.wait_exit(timeout=30)==0\n"
        "  publication_stage('input_child_exited')\n"
        " finally: child.close()\n"
        "witness['input_custody']={'file_count':len(files),'runtime_count':inputs['runtime_file_count'],\n"
        " 'profile_count':len(profile['captures']),'child':child_binding['process']}\n"
        "for item in files:\n"
        " try: item.binding()\n"
        " except WorkflowContractError as exc:\n"
        "  assert exc.code=='WORKFLOW_READ_FILE_CUSTODY_OWNER'\n"
        " else: raise AssertionError('publication input custody remained live after exit')\n"
        "publication_stage('inputs_released')\n"
        "assert fence.guard.replay().active_leases[0].execution==after_inputs\n"
        "witness['hook_capsule']=after_inputs['publication_attempts'][-1]['hook_capsule']\n"
        "try:\n"
        f" with lifecycle.prepare_hook_capsule_ready(request,actor={actor!r}):\n"
        "  raise AssertionError('ready record recreated')\n"
        "except ParallelControlError as exc:\n"
        " assert exc.code=='LEASE_EXECUTION_PUBLICATION_HOOK_INPUT_STATE',str(exc)\n"
        " assert fence.guard.replay().active_leases[0].execution==after_inputs\n"
        "publication_stage('duplicate_ready_rejected')\n"
        "source=Path('src/a.py'); original_source=source.read_bytes()\n"
        "original_execution=fence.guard.replay().active_leases[0].execution\n"
        "source.write_bytes(original_source+b'\\n# changed after original Full and LOCAL\\n')\n"
        "try:\n"
        f" with lifecycle.prepare_main_reference(request,actor={actor!r}):\n"
        "  raise AssertionError('dirty post-Full source admitted')\n"
        "except PublicationFenceError as exc:\n"
        " assert exc.code=='PUBLICATION_CANDIDATE_DIRTY',str(exc)\n"
        " witness['post_full_source_rejection']=exc.code\n"
        " assert fence.guard.replay().active_leases[0].execution==original_execution\n"
        "finally: source.write_bytes(original_source)\n"
        "publication_stage('source_drift_rejected')\n"
        "publication_stage('main_prepare_begin')\n"
        f"with lifecycle.prepare_main_reference(request,actor={actor!r}) as preparation:\n"
        " publication_stage('main_prepared')\n"
        " witness['main_preparation']=preparation\n"
        f" Path({str(witness_path)!r}).write_text(json.dumps(witness))\n"
        f" while not Path({str(release_path)!r}).exists(): time.sleep(.02)\n"
        "publication_stage('main_prepare_aborted')\n"
        "result={**_result_binding(request),'status':'FAIL','reason':'AFTER_PREPARE_ABORT'}\n"
        "Path(request['result_path']).write_text(json.dumps(result))\n"
        "raise SystemExit(7)\n"
    )
    environment = dict(os.environ)
    environment["PYTHONPATH"] = str(fence.project_root / "src")
    request.update(argv=[sys.executable, "-c", program],
                   environment_sha256=execution_environment_sha256(environment))
    request_path.write_text(json.dumps(request), encoding="utf-8")
    reserved = lifecycle.reserve(request, actor=actor)
    assert reserved["status"] == "RESERVED"
    assert lifecycle.reserve(request, actor=actor)["dispatch_allowed"] is False
    lease_id = request["lease_id"]
    with pytest.raises(ParallelControlError, match="PUBLICATION_STABLE_STATE"):
        lifecycle.adopt_unchanged_failed_attempt(lease_id, actor=actor)
    with WindowsJobProcess.create(
        argv=request["argv"], cwd=fence.project_root, environment=environment,
        stdout_path=Path(request["stdout_path"]), job_name=request["job_name"],
    ) as process:
        lifecycle.bind(lease_id, process, actor=actor)
        lifecycle.resume(lease_id, process, actor=actor)
        # v157 reached main preparation at 290.766s. One shared 420s deadline
        # left only 129s for another original profile (81.468s observed), three
        # topology observations and all original worker/lease/C rechecks. Bound
        # each distinct phase instead; never turn progress stdout into PASS.
        _until(lambda: witness_path.exists() or process.poll() is not None
               or _publication_main_prepare_started(Path(request["stdout_path"])),
               description="actual publication worker input custody and negative checks",
               timeout=420)
        _until(lambda: witness_path.exists() or process.poll() is not None,
               description="actual publication worker original Git preparation witness",
               timeout=420)
        assert witness_path.exists(), Path(request["stdout_path"]).read_text()
        witness = json.loads(witness_path.read_text())
        inputs = witness["input_custody"]
        assert inputs["file_count"] == inputs["runtime_count"] + inputs["profile_count"] + 4
        assert inputs["runtime_count"] > 2 and inputs["profile_count"] > 0
        probes = json.loads((root / "outputs/publication-input-write-probe.json").read_text())
        assert len(probes) == 3 and all(row["winerror"] == 32 for row in probes)
        assert witness["post_full_source_rejection"] == "PUBLICATION_CANDIDATE_DIRTY"
        preparation = witness["main_preparation"]
        actual = lifecycle._head(lease_id, actor).execution
        assert actual["checkout_plan"] == witness["checkout_plan"]
        assert actual["checkout_plan"]["worker_process"] == witness["worker_process"]
        assert actual["checkout_plan"]["plan"]["request_sha256"] == actual["request_sha256"]
        assert actual["hook_capsule"] == witness["hook_capsule"]
        assert actual["hook_capsule"]["worker_process"] == witness["worker_process"]
        assert actual["hook_capsule"]["request_sha256"] == actual["request_sha256"]
        assert len(actual["hook_capsule"]["objects"]) == 3
        ready = actual["hook_capsule"]["ready"]
        assert ready["schema_version"] == "workflow_publication_hook_ready.v1"
        assert ready["inputs"]["worker_process"] == witness["worker_process"]
        assert ready["inputs"]["resume_allowed"] is False
        assert witness["unknown_hook_directory_preserved"] is True
        for row, created in zip(actual["hook_capsule"]["definition"]["files"],
                                actual["hook_capsule"]["objects"][1:], strict=True):
            path = root / row["path"]
            info = path.stat()
            assert [info.st_dev, info.st_ino] == created["file_identity"]
            assert path.read_bytes() == bytes.fromhex(row["bytes_hex"])
        assert actual["main_preparation"] == preparation
        assert preparation["worker_process"] == witness["worker_process"]
        assert preparation["acknowledgements"] == ["start: ok\n", "prepare: ok\n"]
        prepared_lock = Path(preparation["prepared_ref"]["path"])
        assert list((prepared_lock.stat().st_dev, prepared_lock.stat().st_ino)) == (
            preparation["prepared_ref"]["identity"]
        )
        assert prepared_lock.read_bytes() == (request["candidate_sha"] + "\n").encode("ascii")
        assert _git(fence.project_root, "rev-parse", "main") == request["expected_main_sha"]
        ref_directory = Path(preparation["topology"]["ref_directories"]["refs/heads"]["path"])
        moved_directory = ref_directory.with_name("heads-custody-probe")
        assert not moved_directory.exists()
        rename_probe = subprocess.run(
            [sys.executable, "-c",
             "import os,sys\n"
             "try: os.rename(sys.argv[1],sys.argv[2])\n"
             "except OSError as exc:\n"
             " print(exc.winerror);sys.exit(0 if exc.winerror in (5,32) else 9)\n"
             "os.rename(sys.argv[2],sys.argv[1]);sys.exit(8)\n",
             str(ref_directory), str(moved_directory)],
            capture_output=True, text=True, timeout=15, check=False,
        )
        assert rename_probe.returncode == 0, (rename_probe.stdout, rename_probe.stderr)
        (directory / "publication-directory-rename-probe.json").write_text(json.dumps({
            "path": str(ref_directory), "winerror": int(rename_probe.stdout),
            "git_process": preparation["git_process"], "state": "PREPARED",
        }))
        from dataclasses import replace

        from ai_trading_system.platform.architecture.workflow_coordination import (
            validate_execution_transition,
        )

        physical = fence.guard.replay().active_leases[0]
        tampered = json.loads(json.dumps(physical.execution))
        tampered["publication_attempts"][-1]["main_preparation"]["observed_at"] = (
            datetime.now(UTC).isoformat()
        )
        with pytest.raises(ParallelControlError, match="PUBLICATION_PREPARATION_CHANGED"):
            validate_execution_transition(physical, replace(physical, execution=tampered))
        tampered_ready = json.loads(json.dumps(physical.execution))
        tampered_ready["publication_attempts"][-1]["hook_capsule"]["ready"]["recorded_at"] = (
            datetime.now(UTC).isoformat()
        )
        with pytest.raises(ParallelControlError, match="PUBLICATION_HOOK_CAPSULE_CHANGED"):
            validate_execution_transition(physical, replace(physical, execution=tampered_ready))
        oracle = NativeOracle()
        with (oracle.process(witness["worker_process"]["pid"]) as native,
              oracle.process(preparation["git_process"]["pid"]) as native_git):
            assert oracle.creation_time(native) == witness["worker_process"]["creation_time"]
            oracle.assert_in_job(native, request["job_name"])
            assert oracle.creation_time(native_git) == preparation["git_process"]["creation_time"]
            oracle.assert_in_job(native_git, request["job_name"])
            release_path.write_text("release")
            assert process.wait(timeout=30) == 7
            assert oracle.exited(native, 10)
            assert oracle.exited(native_git, 10)
        ref_directory.rename(moved_directory)
        moved_directory.rename(ref_directory)
        assert list((ref_directory.stat().st_dev, ref_directory.stat().st_ino)) == (
            preparation["topology"]["ref_directories"]["refs/heads"]["identity"]
        )
        assert not prepared_lock.exists()
        assert _git(fence.project_root, "rev-parse", "main") == request["expected_main_sha"]
        lifecycle.confirm_exit(lease_id, process, actor=actor)
    result_path = Path(request["result_path"])
    original_result = result_path.read_bytes()
    false_pass = json.loads(original_result)
    false_pass["status"] = "PASS"
    result_path.write_text(json.dumps(false_pass))
    (directory / "rejected-worker-pass.json").write_bytes(result_path.read_bytes())
    with pytest.raises(ParallelControlError, match="PUBLICATION_STABLE_VERIFICATION_REQUIRED"):
        lifecycle.record_result(lease_id, actor=actor, result_path=result_path)
    result_path.write_bytes(original_result)
    lifecycle.record_result(lease_id, actor=actor, result_path=result_path)
    before = fence.guard.replay().active_leases[0]
    assert full_execution_projection(before.execution) == original_full
    assert not execution_is_terminal(before.execution)
    with pytest.raises(ParallelControlError, match="LEASE_EXECUTION_NOT_TERMINAL"):
        fence.guard.store.release(lease_id, actor=actor, now=datetime.now(UTC), evidence_refs=())
    # A different writer's new lock is not the vanished original Git object.
    # Ref/index equality cannot authorize releasing around this unknown residue.
    unknown = b"independent lock canary: do not delete"
    with prepared_lock.open("xb") as stream:
        stream.write(unknown)
    unknown_identity = (prepared_lock.stat().st_dev, prepared_lock.stat().st_ino)
    with pytest.raises(ParallelControlError, match="PUBLICATION_PREPARATION_LOCK_REMAINS"):
        lifecycle.adopt_unchanged_failed_attempt(lease_id, actor=actor)
    assert fence.guard.replay().active_leases[0].execution == before.execution
    assert prepared_lock.read_bytes() == unknown
    assert (prepared_lock.stat().st_dev, prepared_lock.stat().st_ino) == unknown_identity
    preserved_lock = directory / "rejected-independent-main.lock"
    prepared_lock.rename(preserved_lock)  # Preserve the test-owned canary, do not delete it.
    assert preserved_lock.read_bytes() == unknown
    if stable_recovery == "index-replaced":
        index = root / ".git/index"
        original_index = index.read_bytes()
        replacement = root / ".git/test-replacement-index"
        replacement.write_bytes(original_index)  # Only this fixture's owned index.
        replacement.replace(index)
        installed_identity = (index.stat().st_dev, index.stat().st_ino)
        for label, expected in (("adopt", "STABLE_FAILED_ATTEMPT"), ("replay", "REPLAY_ONLY")):
            # The original read-only profile has its own 180s bound; allow command overhead.
            result = _v03_fence_cli(root, "index-recovery-" + label, [
                "local-publication-recover-index", "--transaction", str(transaction),
                "--actor", actor,
            ], timeout=240)
            assert result["exit_code"] == 0, result
            payload = json.loads(result["stdout"])
            assert payload["status"] == expected
            assert payload["stable_state"] == "CANDIDATE_STABLE_INDEX_REPLACED"
            assert payload["dispatch_allowed"] is False
            assert payload["publication_allowed"] is False
            assert index.read_bytes() == original_index
            assert (index.stat().st_dev, index.stat().st_ino) == installed_identity
    else:
        adopted = lifecycle.adopt_unchanged_failed_attempt(lease_id, actor=actor)
        assert adopted["status"] == "STABLE_FAILED_ATTEMPT"
        repeated = lifecycle.adopt_unchanged_failed_attempt(lease_id, actor=actor)
        assert repeated["status"] == "REPLAY_ONLY"
    after = fence.guard.replay().active_leases[0]
    resolution = after.execution["publication_stable_observation"]["preparation_resolution"]
    assert resolution["lock_state"] == "ABSENT"
    assert resolution["git_process"]["state"] in {"EXITED", "REUSED"}
    assert after.execution["publication_attempts"] == before.execution["publication_attempts"]
    assert full_execution_projection(after.execution) == original_full
    assert execution_is_terminal(after.execution)
    if stable_recovery == "index-replaced":
        observation = after.execution["publication_stable_observation"]
        assert observation["profile_inspection"]["captures"]
        assert observation["original_topology"]["candidate_checkout"]["index"]["identity"] != (
            observation["topology"]["candidate_checkout"]["index"]["identity"]
        )
    # The fixture now releases the original fence normally. A failed worker
    # never becomes publication PASS; only independently unchanged state closes it.


@pytest.mark.parametrize(
    "canonical_merge_repository", ["full-profile-publish", "native-linked-full-profile-publish"],
    indirect=True,
)
def test_original_publication_cli_ff_only_and_independent_recovery(canonical_merge_repository):
    """One new original Full -> public worker/hooks -> real ff-only -> fresh adopter.

    The retained fixture deliberately does not publish to any remote. A FAILED
    closeout below means this fixture's remote stage was not attempted; it does
    not rewrite the recorded local result, original Full, or a failed worker.
    """
    from test_devx015_workflow_coordination import _run_actual_profile_full

    from ai_trading_system.platform.architecture.workflow_coordination import (
        execution_is_terminal,
        full_execution_projection,
    )

    root, _scope = canonical_merge_repository
    fence = IntegrationPublicationFence(project_root=root)
    transaction = fence.runtime_root / "transactions/merge-authority/transaction.json"
    binding, directory, _driver, _environment = _run_actual_profile_full(root)
    fence.checkpoint(transaction, phase="LOCAL_MAIN_FF_PRE", actor="integration-coordinator")
    def physical():
        return next(row for row in fence.guard.replay().lease_heads
                    if row.lease_id == binding["lease_id"])
    original_full = full_execution_projection(physical().execution)
    arguments = ["local-publish", "--transaction", str(transaction)]
    observation_error = None
    if (root.parent / "native-full-contender").exists():
        with ThreadPoolExecutor(max_workers=1) as pool:
            pending = pool.submit(_v03_fence_cli, root, "original-publication-cli", arguments,
                                  timeout=4000)
            try:
                _observe_native_publication_contender(root, physical, pending, binding)
            except Exception as error:
                observation_error = error
            finally:
                published = pending.result(timeout=4000)
    else:
        published = _v03_fence_cli(root, "original-publication-cli", arguments, timeout=4000)
    # Recovery is a fresh public process using the same transaction/history.
    # On an implementation fault this captures the actual recovery outcome too;
    # it never turns the failed success-path assertion into a PASS.
    recovered = _v03_fence_cli(root, "original-publication-recovery-cli", [
        "local-publication-recover", "--transaction", str(transaction),
    ], timeout=600)
    value = physical().execution
    assert full_execution_projection(value) == original_full
    try:
        if observation_error is not None:
            raise observation_error
        assert published["exit_code"] == 0, published
        assert json.loads(published["stdout"])["status"] == "LOCAL_PUBLISHED", published
        assert recovered["exit_code"] == 0, recovered
        assert json.loads(recovered["stdout"])["status"] == "REPLAY_ONLY", recovered
        assert _git(root, "rev-parse", "HEAD") == binding["candidate_sha"]
        assert _git(root, "rev-parse", "refs/heads/main") == binding["candidate_sha"]
        assert _git(root, "branch", "--show-current") == "main"
        assert len(value["publication_attempts"]) == 1
        attempt = value["publication_attempts"][0]
        assert attempt["git_merge"]["exit"]["returncode"] == 0
        assert attempt["result"]["status"] != "PASS"
        assert value["publication_stable_observation"]["stable_state"] == "LOCAL_PUBLISHED"
        assert value["publication_stable_observation"]["completion_basis"] == (
            "ORIGINAL_GIT_EXIT_ZERO"
        )
        assert {row["kind"] for row in attempt["git_merge"]["hooks"]} == {
            "reference-transaction", "post-merge",
        }
        for row in attempt["git_merge"]["hooks"]:
            assert row["process_chain"][-1] == attempt["git_launch"]["process"]
        (directory / "publication-vertical-evidence.json").write_text(json.dumps({
            "candidate_sha": binding["candidate_sha"], "lease_id": binding["lease_id"],
            "original_full_unchanged": True, "publication": published, "recovery": recovered,
            "attempt_count": len(value["publication_attempts"]),
            "stable_state": value["publication_stable_observation"]["stable_state"],
        }, sort_keys=True), encoding="utf-8")
    finally:
        if execution_is_terminal(value):
            fence.release(transaction, actor="integration-coordinator", outcome="failed")


def _observe_native_publication_contender(root, physical, pending, binding):
    import time

    from ai_trading_system.platform.architecture.workflow_execution import observe_process

    started = time.monotonic()
    while True:
        attempts = physical().execution.get("publication_attempts", [])
        running = [row for row in attempts if row["state"] == "RUNNING"]
        if running:
            break
        assert not pending.done(), "PUBLICATION_ENDED_BEFORE_LIVE_OBSERVATION"
        assert time.monotonic() - started < 600, "PUBLICATION_EXECUTOR_NOT_OBSERVED"
        time.sleep(0.25)
    assert len(running) == 1
    identity = running[0]["process"]
    assert observe_process(**identity)["state"] == "RUNNING"
    peer = root.parent / "native-full-contender"
    head = _git(peer, "rev-parse", "HEAD")
    result = _v03_fence_cli(peer, "native-publication-contender", [
        "acquire", "--transaction-id", "publication-loser", "--task-id", "L01-publication",
        "--change-id", "publication-loser", "--thread-id", "publication-loser",
        "--frozen-base", head, "--lane-head", head,
        "--expected-main", binding["expected_main_sha"],
    ])
    after = observe_process(**identity)
    (root.parent / "native-live-publication-contention.json").write_text(json.dumps({
        "executor": identity, "executor_after": after, "result": result,
        "elapsed_seconds": time.monotonic() - started,
    }), encoding="utf-8")
    assert after["state"] == "RUNNING"
    assert result["exit_code"] == 2, result
    assert json.loads(result["stdout"])["reason_code"] == "PUBLICATION_LEASE_CONFLICT"
    files = {p.relative_to(peer).as_posix() for p in (peer / "outputs").rglob("*") if p.is_file()}
    assert files == {
        "outputs/architecture/arch_005_s4d_checkout_guard/intents/publication-publication-loser.json",
    }, files


def _publication_interruption_attempt(execution):
    """Project the sole original publication, never the enclosing Full worker."""
    attempts = execution["publication_attempts"]
    assert len(attempts) == 1, "interruption requires one original publication attempt"
    attempt = attempts[0]
    assert attempt["state"] == "RUNNING"
    assert attempt["request"]["execution_kind"] == "CONTROLLED_LOCAL_PUBLICATION"
    merge = attempt["git_merge"]
    assert merge["exit"] is None, "missed original live-Git interruption window"
    assert [(row["reference_kind"], row["stage"]) for row in merge["hooks"]] == [
        ("ORIG_HEAD", "prepared"), ("ORIG_HEAD", "committed"),
        ("FAST_FORWARD", "prepared"), ("FAST_FORWARD", "committed"),
    ], "missed exact committed-FF window"
    return attempt


@pytest.mark.parametrize("fault", [
    "none", "missing-attempts", "empty", "duplicate", "wrong-kind", "not-running",
    "missing-merge", "exited", "early", "late",
])
def test_publication_interruption_attempt_projection(fault):
    """Cheap test-driver contract, not native Job/Full/publication acceptance."""
    attempt = {
        "state": "RUNNING", "request": {"execution_kind": "CONTROLLED_LOCAL_PUBLICATION"},
        "process": {"pid": 2, "creation_time": 22},
        "git_merge": {"exit": None, "hooks": [
            {"reference_kind": reference, "stage": stage} for reference, stage in [
                ("ORIG_HEAD", "prepared"), ("ORIG_HEAD", "committed"),
                ("FAST_FORWARD", "prepared"), ("FAST_FORWARD", "committed"),
            ]
        ]},
    }
    # Deliberate top-level decoys: selecting these repeats the v178 mistake.
    execution = {"state": "RESULT_RECORDED", "process": {"pid": 1, "creation_time": 11},
                 "git_merge": {"exit": 0}, "publication_attempts": [attempt]}
    assert _publication_interruption_attempt(execution) is attempt
    if fault == "none":
        return
    if fault == "missing-attempts":
        del execution["publication_attempts"]
    elif fault == "empty":
        execution["publication_attempts"] = []
    elif fault == "duplicate":
        execution["publication_attempts"] = [attempt, attempt]
    elif fault == "wrong-kind":
        attempt["request"]["execution_kind"] = "FULL"
    elif fault == "not-running":
        attempt["state"] = "RESULT_RECORDED"
    elif fault == "missing-merge":
        del attempt["git_merge"]
    elif fault == "exited":
        attempt["git_merge"]["exit"] = {"returncode": 0}
    elif fault == "early":
        attempt["git_merge"]["hooks"].pop()
    elif fault == "late":
        attempt["git_merge"]["hooks"].append({"reference_kind": None, "stage": "0"})
    with pytest.raises((AssertionError, KeyError)):
        _publication_interruption_attempt(execution)


def _interrupt_original_publication_job(pending, physical, root, directory, binding):
    import time

    from ai_trading_system.platform.architecture.workflow_execution import (
        observe_job,
        observe_process,
    )

    deadline = time.monotonic() + 3600
    while not pending.done() and time.monotonic() < deadline:
        logs = list(directory.glob("publication-merge-*.stdout"))
        # Stdout only wakes this observer; durable history selects the process.
        if len(logs) == 1 and logs[0].read_text(encoding="utf-8").count(
            '"stage": "committed"'
        ) >= 2:
            before = _publication_interruption_attempt(physical().execution)
            assert _git(root, "rev-parse", "refs/heads/main") == binding["candidate_sha"]
            assert observe_process(**before["git_launch"]["process"])["state"] == "RUNNING"
            interrupted = {
                "lease_id": binding["lease_id"], "candidate_sha": binding["candidate_sha"],
                "job_name": before["request"]["job_name"], "worker": before["process"],
                "git_process": before["git_launch"]["process"],
                "hooks": before["git_merge"]["hooks"],
                "observation": observe_job(
                    before["request"]["job_name"], terminate=True, timeout=10,
                    expected_process=before["process"],
                ),
            }
            (directory / "publication-interruption.json").write_text(
                json.dumps(interrupted, sort_keys=True), encoding="utf-8",
            )
            assert interrupted["observation"]["state"] == "EMPTY"
            return interrupted
        time.sleep(0.25)
    return None


@pytest.mark.parametrize("canonical_merge_repository", ["full-profile-publish"], indirect=True)
def test_original_publication_cli_interrupted_after_main_commit(canonical_merge_repository):
    """Kill only this fixture's original Job after its durable FF hook receipt.

    The public publisher remains alive to record native exit/missing result.
    No authority, hook, Full, ref or receipt is replaced for fault injection.
    Missing the requested interruption window is a failure, never a normal PASS.
    """
    from test_devx015_workflow_coordination import _run_actual_profile_full

    from ai_trading_system.platform.architecture.workflow_coordination import (
        execution_is_terminal,
        full_execution_projection,
    )
    root, _scope = canonical_merge_repository
    fence = IntegrationPublicationFence(project_root=root)
    transaction = fence.runtime_root / "transactions/merge-authority/transaction.json"
    binding, directory, _driver, _environment = _run_actual_profile_full(root)
    fence.checkpoint(transaction, phase="LOCAL_MAIN_FF_PRE", actor="integration-coordinator")

    def physical():
        return next(row for row in fence.guard.replay().lease_heads
                    if row.lease_id == binding["lease_id"])

    original_full = full_execution_projection(physical().execution)
    interrupted = None
    injection_error = None
    with ThreadPoolExecutor(max_workers=1) as publisher:
        pending = publisher.submit(_v03_fence_cli, root, "interrupted-publication-cli", [
            "local-publish", "--transaction", str(transaction),
        ], timeout=4000)
        try:
            interrupted = _interrupt_original_publication_job(
                pending, physical, root, directory, binding,
            )
        except Exception as error:
            # Preserve the failure, but still await the owned original process
            # and run the original independent recovery before failing the test.
            injection_error = {"type": type(error).__name__, "message": str(error)}
            (directory / "publication-interruption-error.json").write_text(
                json.dumps(injection_error, sort_keys=True), encoding="utf-8",
            )
        published = pending.result(timeout=4000)

    # Even an unsuccessful interruption retains its real public recovery result.
    before_recovery = physical().execution
    recovered = _v03_fence_cli(root, "interrupted-publication-recovery-cli", [
        "local-publication-recover", "--transaction", str(transaction),
    ], timeout=600)
    value = physical().execution
    try:
        assert injection_error is None, injection_error
        assert interrupted is not None, "original publisher never reached interruption boundary"
        assert not execution_is_terminal(before_recovery)
        assert full_execution_projection(before_recovery) == original_full
        assert full_execution_projection(value) == original_full
        assert published["exit_code"] == 2, published
        assert json.loads(published["stdout"])["status"] == "RECOVERY_REQUIRED", published
        assert recovered["exit_code"] == 0, recovered
        assert json.loads(recovered["stdout"])["status"] == "LOCAL_PUBLISHED", recovered
        assert len(value["publication_attempts"]) == 1
        attempt = value["publication_attempts"][0]
        assert attempt["git_merge"]["exit"] is None
        assert attempt["git_merge"]["hooks"] == interrupted["hooks"]
        assert attempt["result"]["status"] == "INSUFFICIENT"
        assert attempt["result"]["artifact"] is None
        observation = value["publication_stable_observation"]
        assert observation["stable_state"] == "LOCAL_PUBLISHED"
        assert observation["completion_basis"] == "RECOVERED_STABLE_C"
        assert _git(root, "rev-parse", "HEAD") == binding["candidate_sha"]
        assert _git(root, "rev-parse", "refs/heads/main") == binding["candidate_sha"]
        assert _git(root, "branch", "--show-current") == "main"
        repeated = _v03_fence_cli(root, "interrupted-publication-replay-cli", [
            "local-publication-recover", "--transaction", str(transaction),
        ], timeout=600)
        assert repeated["exit_code"] == 0, repeated
        assert json.loads(repeated["stdout"])["status"] == "REPLAY_ONLY", repeated
        assert physical().execution == value
    finally:
        if execution_is_terminal(value):
            fence.release(transaction, actor="integration-coordinator", outcome="failed")


@pytest.mark.parametrize("canonical_merge_repository", ["full-profile-publish"], indirect=True)
def test_original_publication_cli_recovers_independent_main_advance(canonical_merge_repository):
    """Original actual Full/public handoff -> one real competing CAS -> fresh failed recovery."""
    import time

    from test_devx015_workflow_coordination import _run_actual_profile_full

    from ai_trading_system.platform.architecture.workflow_contract import canonical_digest
    from ai_trading_system.platform.architecture.workflow_coordination import (
        execution_is_terminal,
        full_execution_projection,
    )
    from ai_trading_system.platform.architecture.workflow_execution import observe_process

    root, _scope = canonical_merge_repository
    fence = IntegrationPublicationFence(project_root=root)
    transaction = fence.runtime_root / "transactions/merge-authority/transaction.json"
    main = str(fence.replay(transaction).transaction["expected_main_sha"])
    newer = _git(root, "commit-tree", main + "^{tree}", "-p", main,
                 "-m", "independent original main successor")
    peer = root.parent / "original-main-peer"
    _git(root, "worktree", "add", str(peer), "main")
    private = peer / "private-owner-canary.txt"
    private.write_bytes(b"original peer private bytes\0\n")
    peer_gitdir = Path(_git(peer, "rev-parse", "--absolute-git-dir"))
    retained_paths = [private, peer_gitdir / "index"]
    retained = {path.as_posix(): (path.read_bytes(), path.stat().st_ino)
                for path in retained_paths}
    binding, directory, _driver, _environment = _run_actual_profile_full(root)
    assert newer not in {main, binding["candidate_sha"]}
    fence.checkpoint(transaction, phase="LOCAL_MAIN_FF_PRE", actor="integration-coordinator")
    intent = fence.replay(transaction).events[-1]["payload"]["local_publication_intent"]
    assert intent["topology"]["main_checkout"]["root"]["path"] == peer.as_posix()

    def physical():
        return next(row for row in fence.guard.replay().lease_heads
                    if row.lease_id == binding["lease_id"])

    original_full = full_execution_projection(physical().execution)
    injected = False
    injection_error = None
    with ThreadPoolExecutor(max_workers=1) as publisher:
        pending = publisher.submit(_v03_fence_cli, root, "main-race-publication-cli", [
            "local-publish", "--transaction", str(transaction), "--authorize-peer-head-handoff",
        ], timeout=4000)
        try:
            deadline = time.monotonic() + 3600
            while not pending.done() and time.monotonic() < deadline:
                logs = [path for path in directory.glob("publication-*.stdout")
                        if not path.name.startswith("publication-merge-")]
                if len(logs) == 1 and "PUBLICATION_STAGE heads_switched" in logs[0].read_text(
                    encoding="utf-8",
                ):
                    attempts = physical().execution["publication_attempts"]
                    assert len(attempts) == 1
                    attempt = attempts[0]
                    assert attempt["state"] == "RUNNING"
                    assert attempt["checkout_effect"]["state"] == "HEADS_SWITCHED"
                    assert attempt["checkout_effect"]["peer_handoff_authorized"] is True
                    assert all(row["reference_kind"] == "ORIG_HEAD"
                               for row in attempt.get("git_merge", {}).get("hooks", []))
                    assert observe_process(**attempt["process"])["state"] == "RUNNING"
                    assert observe_process(**attempt["git_launch"]["process"])["state"] == "RUNNING"
                    assert _git(peer, "rev-parse", "HEAD") == main
                    assert _git(peer, "branch", "--show-current") == ""
                    argv = ["git", "-C", str(peer), "update-ref", "-m", "independent main CAS",
                            "refs/heads/main", newer, main]
                    result = subprocess.run(argv, capture_output=True, text=True, timeout=30)
                    receipt = {
                        "argv": argv, "exit_code": result.returncode,
                        "stdout": result.stdout, "stderr": result.stderr,
                        "main_before": main, "main_after": _git(root, "rev-parse", "main"),
                        "newer": newer, "candidate": binding["candidate_sha"],
                        "request_sha256": attempt["request_sha256"],
                        "checkout_effect_sha256": canonical_digest(attempt["checkout_effect"]),
                        "worker": attempt["process"],
                        "git_process": attempt["git_launch"]["process"],
                        "hooks_before": attempt.get("git_merge", {}).get("hooks", []),
                    }
                    (directory / "independent-main-cas.json").write_text(
                        json.dumps(receipt, sort_keys=True), encoding="utf-8",
                    )
                    assert result.returncode == 0, receipt
                    assert receipt["main_after"] == newer
                    injected = True
                    break
                time.sleep(0.25)
        except Exception as error:
            injection_error = {"type": type(error).__name__, "message": str(error)}
            (directory / "main-race-injection-error.json").write_text(
                json.dumps(injection_error, sort_keys=True), encoding="utf-8",
            )
        published = pending.result(timeout=4000)
    before = physical().execution
    recovered = _v03_fence_cli(root, "main-race-recovery-cli", [
        "local-publication-recover", "--transaction", str(transaction),
    ], timeout=600)
    value = physical().execution
    try:
        assert injection_error is None, injection_error
        assert injected, "original handoff/FAST-preparation race window was not reached"
        assert published["exit_code"] == 2, published
        assert json.loads(published["stdout"])["status"] == "RECOVERY_REQUIRED", published
        assert not execution_is_terminal(before)
        assert full_execution_projection(before) == original_full
        assert full_execution_projection(value) == original_full
        assert recovered["exit_code"] == 0, recovered
        payload = json.loads(recovered["stdout"])
        assert payload["status"] == "RECOVERED_FAILED", recovered
        assert payload["stable_state"] == "CANDIDATE_RETAINED_MAIN_ADVANCED"
        assert payload["dispatch_allowed"] is False and payload["publication_allowed"] is False
        assert len(value["publication_attempts"]) == 1
        attempt = value["publication_attempts"][0]
        assert attempt["result"]["status"] != "PASS"
        assert all(row["reference_kind"] == "ORIG_HEAD"
                   for row in attempt.get("git_merge", {}).get("hooks", []))
        assert attempt["head_recovery"]["schema_version"] == "workflow_publication_head_recovery.v2"
        assert attempt["head_recovery"]["state"] == "RESTORED"
        assert value["publication_stable_observation"]["schema_version"] == (
            "workflow_publication_stable_observation.v4"
        )
        assert _git(root, "rev-parse", "main") == newer
        assert _git(root, "rev-parse", "HEAD") == binding["candidate_sha"]
        original_branch = intent["topology"]["candidate_branch"].removeprefix("refs/heads/")
        assert _git(root, "branch", "--show-current") == original_branch
        assert _git(peer, "rev-parse", "HEAD") == main
        assert _git(peer, "branch", "--show-current") == ""
        assert {path.as_posix(): (path.read_bytes(), path.stat().st_ino)
                for path in retained_paths} == retained
        repeated = _v03_fence_cli(root, "main-race-replay-cli", [
            "local-publication-recover", "--transaction", str(transaction),
        ], timeout=600)
        assert repeated["exit_code"] == 0, repeated
        assert json.loads(repeated["stdout"])["status"] == "REPLAY_ONLY", repeated
        assert physical().execution == value
        assert execution_is_terminal(value)
    finally:
        if execution_is_terminal(value):
            fence.release(transaction, actor="integration-coordinator", outcome="failed")


@pytest.mark.parametrize("canonical_merge_repository", ["full-profile-publish"], indirect=True)
def test_local_publication_inspection_cli_after_actual_full(canonical_merge_repository):
    from test_devx015_workflow_coordination import _run_actual_profile_full

    root, _scope = canonical_merge_repository
    fence = IntegrationPublicationFence(project_root=root)
    transaction = fence.runtime_root / "transactions/merge-authority/transaction.json"
    binding, directory, _driver, _environment = _run_actual_profile_full(root)
    candidate = str(binding["candidate_sha"])
    assert fence.replay(transaction).phase == "FORMAL_VALIDATION_RESULT"
    fence.checkpoint(transaction, phase="LOCAL_MAIN_FF_PRE", actor="integration-coordinator")
    before = _v03_unchanged_state(fence, transaction)
    original_event = fence.replay(transaction).events[-1]
    intent = original_event["payload"]["local_publication_intent"]
    assert intent["schema_version"] == "integration_publication_local_intent.v1"
    assert intent["lease_id"] == binding["lease_id"]
    assert intent["transaction_sha256"] == binding["transaction_sha256"]
    summary = (directory / "test_runtime_summary.json").read_bytes()
    result = _v03_fence_cli(root, "local-inspection-actual-full", [
        "local-publication-inspect", "--transaction", str(transaction),
    ])
    assert result["exit_code"] == 0, result
    observed = json.loads(result["stdout"])
    assert observed["status"] == "OBSERVED"
    assert observed["transaction_sha256"] == binding["transaction_sha256"]
    assert observed["head_event_id"] == fence.replay(transaction).events[-1]["event_id"]
    topology = observed["topology"]
    assert topology["candidate_sha"] == candidate == _git(root, "rev-parse", "HEAD")
    assert topology["candidate_checkout"]["observed_head"] == candidate
    assert topology["expected_main_sha"] == _git(root, "rev-parse", "main")
    assert topology["candidate_index_matches_tree"] is True
    assert intent["topology"] == topology
    for record in (observed, topology):
        assert record["publication_allowed"] is False
        assert record["dispatch_allowed"] is False
        assert record["mutation_performed"] is False
    assert _v03_unchanged_state(fence, transaction) == before

    # A fresh internally consistent capture must not silently replace the
    # original checkpoint identity. Change config before any index operation
    # so the rejection is attributable to this single real metadata fault.
    _git(root, "config", "devx015.publicationCaptureProbe", "changed")
    changed = _v03_unchanged_state(fence, transaction)
    assert changed["index"] == before["index"]
    assert changed["refs"] == before["refs"]
    changed_config = (root / ".git/config").read_bytes()
    refused = _v03_fence_cli(root, "local-inspection-original-intent", [
        "local-publication-inspect", "--transaction", str(transaction),
    ])
    assert refused["exit_code"] == 2, refused
    assert "PUBLICATION_LOCAL_INTENT_CHANGED" in refused["stdout"]
    assert _v03_unchanged_state(fence, transaction) == changed
    assert (root / ".git/config").read_bytes() == changed_config
    assert fence.replay(transaction).events[-1] == original_event

    # A real Git index flag change after the valid Full must not gain admission
    # merely because the candidate OID and tracked worktree bytes still match.
    path = "src/a.py"
    _git(root, "update-index", "--assume-unchanged", "--", path)
    fault_state = _v03_unchanged_state(fence, transaction)
    try:
        refused = _v03_fence_cli(root, "local-inspection-hidden-index", [
            "local-publication-inspect", "--transaction", str(transaction),
        ])
        assert refused["exit_code"] == 2, refused
        assert "LOCAL_PUBLICATION_INDEX_HIDDEN" in refused["stdout"]
        assert _v03_unchanged_state(fence, transaction) == fault_state
    finally:
        _git(root, "update-index", "--no-assume-unchanged", "--", path)
    assert (directory / "test_runtime_summary.json").read_bytes() == summary
    assert _git(root, "rev-parse", "HEAD") == candidate
    assert fence.replay(transaction).phase == "LOCAL_MAIN_FF_PRE"
    # The original fixture releases its real terminal Full through the original
    # failed-publication path. No fake Full, main update, push or registry key.


def _publication_business_snapshot(fence, transaction):
    state = _v03_unchanged_state(fence, transaction)
    # Recovery acquires/releases the real short arbiter even when _head rejects
    # a missing publication. Only its documented diagnostic sidecar may change.
    owner_path = fence.guard.store.root / "arbiter.owner.json"
    owner = json.loads(state["governance"].pop(str(owner_path)))
    assert owner["schema_version"] == "execution_lease_os_arbiter_owner.v2"
    assert owner["state"] == "RELEASED" and owner["actor"] == "integration-coordinator"
    anchor = fence.guard.store.root / "arbiter.lock"
    state["arbiter_anchor"] = (anchor.stat().st_ino, anchor.read_bytes())
    return state


@pytest.mark.parametrize(
    "command", ["local-publication-recover", "local-publication-adopt-published"],
)
def test_public_missing_publication_preserves_business_state(publication_checkout, command):
    root = publication_checkout
    fence = _fence(root)
    binding = _acquire(fence, root, transaction_id="missing-publication-diagnostic")
    transaction = root / str(binding["transaction_path"])
    before = _publication_business_snapshot(fence, transaction)
    original_heads = tuple(fence.guard.replay().head_event_ids)
    try:
        result = _v03_fence_cli(root, "missing-publication-" + command, [
            command, "--transaction", str(transaction),
        ])
        assert result["exit_code"] == 2, result
        assert "PUBLICATION_MISSING" in result["stdout"], result
        assert _publication_business_snapshot(fence, transaction) == before
        assert tuple(fence.guard.replay().head_event_ids) == original_heads
    finally:
        fence.release(transaction, actor="integration-coordinator", outcome="failed")


@pytest.mark.parametrize("canonical_merge_repository", ["full-profile-publish"], indirect=True)
def test_p01_public_entries_reject_ref_only_update_before_checkout(canonical_merge_repository):
    """An external real CAS is not an original publisher's coherent completion.

    Preserve the deliberately inconsistent peer as evidence; never repair its
    private files or forge a publisher attempt to make recovery adopt the CAS.
    Original publisher crash-to-stable recovery has separate native coverage.
    """
    from test_devx015_workflow_coordination import _run_actual_profile_full

    from ai_trading_system.platform.architecture.workflow_coordination import (
        full_execution_projection,
    )

    root, _scope = canonical_merge_repository
    fence = IntegrationPublicationFence(project_root=root)
    transaction = fence.runtime_root / "transactions/merge-authority/transaction.json"
    main = str(fence.replay(transaction).transaction["expected_main_sha"])
    peer = root.parent / "ref-only-main-peer"
    _git(root, "worktree", "add", str(peer), "main")
    peer_gitdir = Path(_git(peer, "rev-parse", "--absolute-git-dir"))
    private = peer / "private-owner-canary.txt"
    private.write_bytes(b"preserve original peer private bytes\0\r\n")
    retained = {path: (path.read_bytes(), path.stat().st_ino)
                for path in (peer_gitdir / "HEAD", peer_gitdir / "index", private)}
    old_entries = _git(peer, "ls-files", "--stage")
    binding, directory, _driver, _environment = _run_actual_profile_full(root)
    candidate = str(binding["candidate_sha"])
    assert candidate != main
    assert _git(root, "rev-parse", candidate + "^{tree}") != _git(
        root, "rev-parse", main + "^{tree}",
    )
    fence.checkpoint(transaction, phase="LOCAL_MAIN_FF_PRE", actor="integration-coordinator")
    original_event = fence.replay(transaction).events[-1]
    assert original_event["payload"]["local_publication_intent"]["topology"][
        "main_checkout"
    ]["root"]["path"] == peer.as_posix()
    summary = (directory / "test_runtime_summary.json").read_bytes()

    def execution():
        return next(row.execution for row in fence.guard.replay().lease_heads
                    if row.lease_id == binding["lease_id"])

    original_execution = execution()
    original_full = full_execution_projection(original_execution)
    assert not original_execution.get("publication_attempts")
    positive = _v03_fence_cli(root, "ref-only-original-inspection", [
        "local-publication-inspect", "--transaction", str(transaction),
    ], timeout=180)
    assert positive["exit_code"] == 0, positive
    assert json.loads(positive["stdout"])["status"] == "OBSERVED"
    _git(root, "update-ref", "-m", "external ref-only fault before checkout",
         "refs/heads/main", candidate, main)
    # The peer's symbolic HEAD now resolves to C, while its original index and
    # working bytes still describe M. There is no publisher execution receipt.
    assert _git(peer, "rev-parse", "HEAD") == candidate
    assert _git(peer, "branch", "--show-current") == "main"
    assert _git(peer, "ls-files", "--stage") == old_entries
    assert {path: (path.read_bytes(), path.stat().st_ino) for path in retained} == retained
    damaged = _publication_business_snapshot(fence, transaction)
    (directory / "ref-only-checkout-evidence.json").write_text(json.dumps({
        "main_before": main, "main_after": candidate, "peer": peer.as_posix(),
        "peer_index_sha256": hashlib.sha256(retained[peer_gitdir / "index"][0]).hexdigest(),
        "peer_index_entries": old_entries, "original_intent": original_event,
        "fault_origin": "EXTERNAL_GIT_CAS_NOT_ORIGINAL_PUBLISHER",
    }), encoding="utf-8")
    for command, reason in (
        ("local-publication-inspect", "PUBLICATION_EXPECTED_MAIN_STALE"),
        ("local-publish", "PUBLICATION_EXPECTED_MAIN_STALE"),
        ("local-publication-recover", "PUBLICATION_MISSING"),
        ("local-publication-adopt-published", "PUBLICATION_MISSING"),
    ):
        args = [command, "--transaction", str(transaction)]
        if command == "local-publish":
            args.append("--authorize-peer-head-handoff")
        refused = _v03_fence_cli(root, "ref-only-refused-" + command, args, timeout=180)
        assert refused["exit_code"] == 2, refused
        assert reason in refused["stdout"], refused
        assert _publication_business_snapshot(fence, transaction) == damaged
        assert execution() == original_execution
        assert full_execution_projection(execution()) == original_full
        assert fence.replay(transaction).events[-1] == original_event
        assert (directory / "test_runtime_summary.json").read_bytes() == summary
        assert {path: (path.read_bytes(), path.stat().st_ino) for path in retained} == retained
        assert _git(peer, "ls-files", "--stage") == old_entries
    fence.release(transaction, actor="integration-coordinator", outcome="failed")
    assert fence.replay(transaction).phase == "FAILED"
    assert not fence.guard.replay().active_leases
    assert _git(root, "rev-parse", "main") == candidate
    assert {path: (path.read_bytes(), path.stat().st_ino) for path in retained} == retained


@pytest.mark.parametrize("canonical_merge_repository", ["full-profile-publish"], indirect=True)
def test_x02_original_publication_rejects_ref_aba_after_actual_full(canonical_merge_repository):
    from test_devx015_workflow_coordination import _run_actual_profile_full

    from ai_trading_system.platform.architecture.workflow_coordination import (
        full_execution_projection,
    )

    root, _scope = canonical_merge_repository
    fence = IntegrationPublicationFence(project_root=root)
    transaction = fence.runtime_root / "transactions/merge-authority/transaction.json"
    binding, directory, _driver, _environment = _run_actual_profile_full(root)
    fence.checkpoint(transaction, phase="LOCAL_MAIN_FF_PRE", actor="integration-coordinator")
    candidate, main = str(binding["candidate_sha"]), str(binding["expected_main_sha"])
    original_event = fence.replay(transaction).events[-1]
    summary = (directory / "test_runtime_summary.json").read_bytes()
    original_state = _v03_unchanged_state(fence, transaction)

    def execution():
        return next(row.execution for row in fence.guard.replay().lease_heads
                    if row.lease_id == binding["lease_id"])

    original_execution = execution()
    original_full = full_execution_projection(original_execution)
    positive = _v03_fence_cli(root, "aba-original-inspection", [
        "local-publication-inspect", "--transaction", str(transaction),
    ], timeout=180)
    assert positive["exit_code"] == 0, positive
    assert json.loads(positive["stdout"])["status"] == "OBSERVED"
    assert _v03_unchanged_state(fence, transaction) == original_state
    common = Path(_git(root, "rev-parse", "--path-format=absolute", "--git-common-dir"))
    log = common / "logs/refs/heads/main"
    before_log = log.read_bytes()
    successor = _git(root, "commit-tree", main + "^{tree}", "-p", main, "-m", "ABA successor")
    assert successor not in {main, candidate}
    _git(root, "update-ref", "-m", "ABA outbound", "refs/heads/main", successor, main)
    _git(root, "update-ref", "-m", "ABA return", "refs/heads/main", main, successor)
    after_log = log.read_bytes()
    assert after_log.startswith(before_log)
    transitions = after_log[len(before_log):].splitlines()
    assert len(transitions) == 2
    assert transitions[0].startswith(f"{main} {successor} ".encode())
    assert transitions[1].startswith(f"{successor} {main} ".encode())
    assert _git(root, "rev-parse", "refs/heads/main") == main
    assert _git(root, "rev-parse", "HEAD") == candidate
    assert _v03_unchanged_state(fence, transaction) == original_state
    (directory / "ref-aba-evidence.json").write_text(json.dumps({
        "main": main, "successor": successor, "candidate": candidate,
        "before_reflog_hex": before_log.hex(), "after_reflog_hex": after_log.hex(),
        "original_intent": original_event["payload"]["local_publication_intent"],
    }), encoding="utf-8")
    for command in ("local-publication-inspect", "local-publish"):
        refused = _v03_fence_cli(root, "aba-refused-" + command, [
            command, "--transaction", str(transaction),
        ], timeout=180)
        assert refused["exit_code"] == 2, refused
        assert "PUBLICATION_LOCAL_INTENT_CHANGED" in refused["stdout"], refused
        assert not refused["stderr"]
        assert _v03_unchanged_state(fence, transaction) == original_state
        assert execution() == original_execution
        assert full_execution_projection(execution()) == original_full
        assert fence.replay(transaction).events[-1] == original_event
        assert log.read_bytes() == after_log
        assert (directory / "test_runtime_summary.json").read_bytes() == summary
    fence.release(transaction, actor="integration-coordinator", outcome="failed")
    assert not fence.guard.replay().active_leases
    assert log.read_bytes() == after_log
    assert _git(root, "rev-parse", "refs/heads/main") == main
    assert _git(root, "rev-parse", "HEAD") == candidate
    assert (root / ".git/index").read_bytes() == original_state["index"]


@pytest.mark.parametrize("case", ["rehashed_delta", "consistent_new_claim"])
def test_v03_rehashed_plan_cannot_replace_frozen_authority(
    publication_checkout: Path, case: str,
) -> None:
    from test_arch_005_integration_revalidation import _manifest

    from ai_trading_system.platform.architecture.integration_revalidation import (
        build_integration_revalidation_plan,
    )

    root = publication_checkout
    base = _git(root, "rev-parse", "main")
    (root / "src/a.py").write_text("VALUE = 2\n", encoding="utf-8")
    _git(root, "add", "src/a.py")
    _git(root, "commit", "-m", "V03 actual declared lane change")
    lane = _git(root, "rev-parse", "HEAD")
    directory = root / "outputs/v03-plan"
    directory.mkdir(parents=True)
    manifest = _manifest(base, owned_paths=["src/a.py"])
    manifest["task_id"] = TASK_ID
    manifest_path = directory / "manifest.json"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    plan_path = directory / "plan.json"
    policy_path = ROOT / "config/architecture/arch_005_integration_revalidation.yaml"
    shared_args = ["--repository", str(root), "--manifest", str(manifest_path),
                   "--policy", str(policy_path)]
    planned = _v03_cli(root, "original-plan", "architecture_arch005_integration_revalidation.py", [
        "plan", *shared_args, "--frozen-base", base, "--lane-head", lane,
        "--latest-main", base, "--output", str(plan_path),
    ])
    assert planned["exit_code"] == 0, planned
    validation_args = ["validate", *shared_args, "--plan", str(plan_path)]
    original = _v03_cli(root, "original-plan-validation",
                        "architecture_arch005_integration_revalidation.py", validation_args)
    assert original["exit_code"] == 0, original
    original_raw = plan_path.read_bytes()
    fence = _fence(root)
    binding = _acquire(fence, root, transaction_id="v03-plan", integration_plan=plan_path,
                       extra_shared=("outputs/v03-plan/plan.json",))
    transaction = root / str(binding["transaction_path"])
    validate = ["validate", "--transaction", str(transaction), "--exact-phase", "ACQUIRED"]
    admitted = _v03_fence_cli(root, "original-frozen-admission", validate)
    assert admitted["exit_code"] == 0, admitted
    before = _v03_unchanged_state(fence, transaction)
    plan = json.loads(original_raw)
    if case == "rehashed_delta":
        assert plan["task_delta"]
        plan["task_delta"] = []
        body = {key: value for key, value in plan.items()
                if key not in {"plan_id", "plan_sha256"}}
        digest = hashlib.sha256(json.dumps(
            body, sort_keys=True, ensure_ascii=False, separators=(",", ":"),
        ).encode()).hexdigest()
        plan.update(plan_sha256=digest, plan_id="integration-revalidation-" + digest[:20])
    else:
        plan = build_integration_revalidation_plan(
            repository=root, frozen_base=base, lane_head=lane, latest_main=base,
            manifest=manifest, policy_path=policy_path,
            mainline_contract_claims=[{"contract_id": "new-v03-declaration",
                                      "version": "1.0.0", "access": "READ"}],
        )
    (root.parent / "original-frozen-plan.json").write_bytes(original_raw)
    changed_raw = json.dumps(plan).encode()
    assert changed_raw != original_raw
    try:
        plan_path.write_bytes(changed_raw)
        checked = _v03_cli(root, "changed-plan-validation",
                          "architecture_arch005_integration_revalidation.py", validation_args)
        if case == "rehashed_delta":
            assert checked["exit_code"] == 2, checked
            assert "PLAN_REBUILD_MISMATCH" in checked["stdout"] + checked["stderr"]
            assert "PLAN_CHECKSUM" not in checked["stdout"] + checked["stderr"]
        else:
            assert checked["exit_code"] == 0, checked
        refused = _v03_fence_cli(root, "changed-frozen-admission", validate)
        assert refused["exit_code"] == 2, refused
        assert "PUBLICATION_PLAN_TAMPERED" in refused["stdout"]
        assert plan_path.read_bytes() == changed_raw
        assert _v03_unchanged_state(fence, transaction) == before
        (root.parent / "changed-plan.json").write_bytes(changed_raw)
    finally:
        plan_path.write_bytes(original_raw)
        fence.release(transaction, actor="integration-coordinator", outcome="failed")


@pytest.mark.parametrize("case", ["wrong_task", "changed_candidate"])
def test_v03_public_candidate_binding_rejects_wrong_subject(
    publication_checkout: Path, case: str,
) -> None:
    root = publication_checkout
    fence = _fence(root)
    binding = _acquire(fence, root, transaction_id="v03-subject")
    transaction = root / str(binding["transaction_path"])
    for phase in ("TASK_SOURCE_PRE_WRITE", "GENERATED_REBUILD_PRE", "GENERATED_REBUILD_POST",
                  "CANDIDATE_COMMIT_PRE", "FORMAL_VALIDATION_PRE"):
        fence.checkpoint(transaction, phase=phase, actor="integration-coordinator",
                         generator_ids=("canonical-task-source",) if phase.startswith("GENERATED_")
                         else ())
    command = ["validate", "--transaction", str(transaction), "--require-candidate", "--task-id"]
    valid = _v03_fence_cli(root, "valid-subject", [*command, TASK_ID])
    assert valid["exit_code"] == 0, valid
    original_candidate = _git(root, "rev-parse", "HEAD")
    if case == "changed_candidate":
        (root / "src/a.py").write_text("VALUE = 3\n", encoding="utf-8")
        _git(root, "add", "src/a.py")
        _git(root, "commit", "-m", "V03 independently changed candidate")
        assert _git(root, "rev-parse", "HEAD") != original_candidate
    before = _v03_unchanged_state(fence, transaction)
    try:
        task = TASK_ID + "-OTHER" if case == "wrong_task" else TASK_ID
        refused = _v03_fence_cli(root, "invalid-subject", [*command, task])
        assert refused["exit_code"] == 2, refused
        reason = (
            "PUBLICATION_TASK_MISMATCH" if case == "wrong_task"
            else "PUBLICATION_CANDIDATE_DRIFT"
        )
        assert reason in refused["stdout"]
        assert _v03_unchanged_state(fence, transaction) == before
        assert fence.replay(transaction).phase == "FORMAL_VALIDATION_PRE"
    finally:
        fence.release(transaction, actor="integration-coordinator", outcome="failed")


@pytest.mark.parametrize("case", ["receipt", "event"])
def test_v03_foreign_valid_terminal_object_cannot_replace_original(
    publication_checkout: Path, case: str,
) -> None:
    root = publication_checkout
    fence = _fence(root)
    transactions = []
    for name in ("v03-donor", "v03-target"):
        binding = _acquire(fence, root, transaction_id=name)
        transaction = root / str(binding["transaction_path"])
        fence.release(transaction, actor="integration-coordinator", outcome="failed")
        positive = _v03_fence_cli(root, name + "-valid-terminal", [
            "release", "--transaction", str(transaction), "--outcome", "failed",
        ])
        assert positive["exit_code"] == 0, positive
        transactions.append(transaction)
    donor, target = transactions
    if case == "receipt":
        source = donor.parent / "closeout_receipt.json"
        destination = target.parent / "closeout_receipt.json"
    else:
        source = sorted((donor.parent / "events").glob("*.json"))[0]
        destination = sorted((target.parent / "events").glob("*.json"))[0]
    original, foreign = destination.read_bytes(), source.read_bytes()
    assert original != foreign
    (root.parent / "original-target-object.json").write_bytes(original)
    (root.parent / "foreign-valid-object.json").write_bytes(foreign)
    destination.write_bytes(foreign)
    before = _v03_unchanged_state(fence, target)
    refused = _v03_fence_cli(root, "foreign-object-rejected", [
        "release", "--transaction", str(target), "--outcome", "failed",
    ])
    assert refused["exit_code"] == 2, refused
    reason = ("PUBLICATION_TERMINAL_RECEIPT_MISMATCH" if case == "receipt"
              else "PUBLICATION_EVENT_TRANSACTION_BINDING")
    assert reason in refused["stdout"]
    assert "PUBLICATION_EVENT_HASH_MISMATCH" not in refused["stdout"]
    assert destination.read_bytes() == foreign
    after = _v03_unchanged_state(fence, target)
    assert after["refs"] == before["refs"]
    assert after["index"] == before["index"]
    assert set(after["governance"]) == set(before["governance"])
    changed = {path for path in before["governance"]
               if before["governance"][path] != after["governance"][path]}
    # release holds the original OS arbiter even when terminal replay refuses.
    # Only its diagnostic owner sidecar may change; ledger/events/receipts may not.
    owner_path = str(fence.guard.runtime_root / "leases/arbiter.owner.json")
    assert changed == {owner_path}, changed
    for label, snapshot in (("before", before), ("after", after)):
        owner_raw = snapshot["governance"][owner_path]
        owner = json.loads(owner_raw)
        assert owner["schema_version"] == "execution_lease_os_arbiter_owner.v2"
        assert owner["state"] == "RELEASED"
        assert owner["actor"] == "integration-coordinator"
        assert owner["production_effect"] == "none"
        (root.parent / (label + "-arbiter-owner.json")).write_bytes(owner_raw)
        evidence = {"refs": snapshot["refs"],
                    "index_sha256": hashlib.sha256(snapshot["index"]).hexdigest(),
                    "governance_sha256": {path: hashlib.sha256(raw).hexdigest()
                                          for path, raw in snapshot["governance"].items()}}
        (root.parent / (label + "-state.json")).write_text(
            json.dumps(evidence, sort_keys=True), encoding="utf-8",
        )
    assert fence.guard.replay().active_leases == ()


def test_bound_full_parent_is_optional_until_explicitly_consumed(
    publication_checkout: Path,
) -> None:
    parent = publication_checkout / "outputs/validation_runtime/parent/test_runtime_summary.json"
    parent.parent.mkdir(parents=True)
    parent.write_text('{"status":"FAIL"}\n', encoding="utf-8")
    fence = _fence(publication_checkout)
    binding = _acquire(
        fence,
        publication_checkout,
        transaction_id="parent-binding",
        full_parent=parent,
    )
    transaction = Path(str(binding["transaction_path"]))

    assert fence.validate(transaction, exact_phase="ACQUIRED")["status"] == "PASS"
    assert (
        fence.validate(
            transaction,
            exact_phase="ACQUIRED",
            parent_path=parent,
        )["status"]
        == "PASS"
    )

    parent.write_text('{"status":"PASS"}\n', encoding="utf-8")
    with pytest.raises(PublicationFenceError) as error:
        fence.validate(transaction, exact_phase="ACQUIRED", parent_path=parent)

    assert error.value.code == "PUBLICATION_FULL_PARENT_MISMATCH"
    fence.release(transaction, actor="integration-coordinator", outcome="failed")


@pytest.mark.parametrize("canonical_merge_repository", ["full-profile-publish"], indirect=True)
@pytest.mark.parametrize(
    "remote_state",
    [
        "normal", "advanced", "m10", "unreachable", "endpoint-changed", "ack-lost",
        "probe-unlocked", "probe-state-race", "before-push-divergence",
    ],
)
def test_full_transaction_replays_candidate_publish_and_closeout_receipt(
    canonical_merge_repository,
    remote_state: str,
) -> None:
    publication_checkout, _scope = canonical_merge_repository
    _run_actual_publication_fixture(publication_checkout, remote_state)


def _run_actual_publication_fixture(publication_checkout: Path, remote_state: str) -> None:
    remote = publication_checkout.parent / "origin.git"
    _git(publication_checkout, "init", "--bare", str(remote))
    # Preserve the real repository identity gate; isolate only the push
    # transport. Fetches below explicitly address this actual local bare remote.
    _git(publication_checkout, "remote", "set-url", "--push", "origin", str(remote))
    _git(publication_checkout, "push", "-u", "origin", "main")
    trace = publication_checkout.parent / "actual-git-push-trace.jsonl"
    before = _actual_push_invocations(trace)
    with pytest.MonkeyPatch.context() as environment:
        environment.setenv("GIT_TRACE2_EVENT", trace.as_posix())
        _assert_full_transaction_replays_candidate_publish_and_closeout_receipt(
            publication_checkout, remote_state,
        )
    pushes = _actual_push_invocations(trace) - before
    # Includes the independent peer's push for the advanced/divergent variants.
    # A redundant same-candidate push is still a second real invocation.
    assert len(pushes) == (2 if remote_state in {"advanced", "m10"} else 1)
    commands = _actual_push_commands(trace)
    for identity in pushes:
        assert not any(
            argument == "-f" or argument.startswith(("--force", "+"))
            for argument in commands[identity][1:]
        ), "publication performed a force push"


def _actual_push_invocations(trace: Path) -> set[str]:
    return set(_actual_push_commands(trace))


def _actual_push_commands(trace: Path) -> dict[str, list[str]]:
    if not trace.exists():
        return {}
    events = [json.loads(line) for line in trace.read_text(encoding="utf-8").splitlines()]
    pushes = {
        row["sid"] for row in events
        if row.get("event") == "cmd_name" and row.get("name") == "push"
    }
    starts = {row["sid"]: row["argv"] for row in events if row.get("event") == "start"}
    assert pushes <= starts.keys(), "Git observer lost invocation identity/argv"
    return {identity: starts[identity] for identity in pushes}


def test_actual_git_trace_detects_duplicate_push_with_unchanged_remote(
    publication_checkout: Path,
) -> None:
    trace = publication_checkout.parent / "duplicate-push-control.jsonl"
    remote = Path(_git(publication_checkout, "remote", "get-url", "origin"))
    before = _git(remote, "rev-parse", "main")
    with pytest.MonkeyPatch.context() as environment:
        environment.setenv("GIT_TRACE2_EVENT", trace.as_posix())
        _git(publication_checkout, "push", "origin", "HEAD:refs/heads/main")
        first = _actual_push_invocations(trace)
        assert len(first) == 1
        _git(publication_checkout, "push", "origin", "HEAD:refs/heads/main")
        assert len(_actual_push_invocations(trace) - first) == 1
        assert len(_actual_push_invocations(trace)) == 2
    assert _git(remote, "rev-parse", "main") == before


def _m10_remote_probe_bootstrap(directory: Path) -> str:
    from ai_trading_system.platform.architecture import integration_publication_fence as module

    original = inspect.getsource(module._observe_push_remote)
    target = '["git", "ls-remote", "--exit-code", "--refs", endpoint, "refs/heads/main"]'
    assert original.count(target) == 1, "INVALID_M10_MUTATION_TARGET"
    changed = original.replace(target, (
        '["git", "for-each-ref", "--format=%(objectname)%09refs/heads/main", '
        '"refs/remotes/origin/main"]'
    ), 1)
    (directory / "m10-method-before.py").write_text(original, encoding="utf-8")
    method_path = directory / "m10-method-after.py"
    method_path.write_text(changed, encoding="utf-8")
    compile(changed, str(method_path), "exec")  # Close mutation shape before actual Full.
    witness = directory / "m10-loaded-method.json"
    bootstrap = (
        "from ai_trading_system.platform.architecture "
        "import integration_publication_fence as _m10\n"
        "from pathlib import Path as _M10Path\nimport hashlib as _m10hash,json as _m10json\n"
        f"_m10body=_M10Path({str(method_path)!r}).read_text(encoding='utf-8')\n"
        "_m10namespace={}\n"
        f"exec(compile(_m10body,{str(method_path)!r},'exec'),_m10.__dict__,_m10namespace)\n"
        "_m10._observe_push_remote=_m10namespace['_observe_push_remote']\n"
        f"_M10Path({str(witness)!r}).write_text(_m10json.dumps({{"
        "'module_path':_m10.__file__,"
        "'module_sha256':_m10hash.sha256(_M10Path(_m10.__file__).read_bytes()).hexdigest(),"
        f"'before_method_sha256':{hashlib.sha256(original.encode()).hexdigest()!r},"
        "'after_method_sha256':_m10hash.sha256(_m10body.encode()).hexdigest()}),encoding='utf-8')\n"
        "import runpy,sys\nsys.argv=sys.argv[1:]\n"
        "runpy.run_path(sys.argv[0],run_name='__main__')\n"
    )
    (directory / "m10-driver.py").write_text(bootstrap, encoding="utf-8")
    return bootstrap


def _assert_observed_actual_remote_tip(observed: dict[str, Any], actual_tip: str) -> None:
    assert observed["tip_sha"] == actual_tip, "REMOTE_TIP_MUST_MATCH_ACTUAL_ENDPOINT"


def _assert_full_transaction_replays_candidate_publish_and_closeout_receipt(
    publication_checkout: Path,
    remote_state: str,
) -> None:
    from test_devx015_workflow_coordination import _run_actual_profile_full

    from ai_trading_system.platform.architecture.workflow_coordination import (
        full_execution_projection,
    )

    m10_bootstrap = (
        _m10_remote_probe_bootstrap(publication_checkout.parent) if remote_state == "m10" else None
    )
    if remote_state == "m10":
        remote_state = "advanced"
    fence = IntegrationPublicationFence(project_root=publication_checkout)
    transaction = fence.runtime_root / "transactions/merge-authority/transaction.json"
    binding, directory, _driver, _environment = _run_actual_profile_full(publication_checkout)
    candidate = str(binding["candidate_sha"])
    summary = directory / "test_runtime_summary.json"
    assert fence.replay(transaction).phase == "FORMAL_VALIDATION_RESULT"
    fence.checkpoint(
        transaction,
        phase="LOCAL_MAIN_FF_PRE",
        actor="integration-coordinator",
    )
    _git(publication_checkout, "switch", "main")
    _git(publication_checkout, "merge", "--ff-only", candidate)
    _git(publication_checkout, "fetch",
         _git(publication_checkout, "remote", "get-url", "--push", "origin"),
         "refs/heads/main:refs/remotes/origin/main")
    if remote_state == "before-push-divergence":
        remote = Path(_git(publication_checkout, "remote", "get-url", "--push", "origin"))
        cached = _git(publication_checkout, "rev-parse", "origin/main")
        peer = publication_checkout.parent / "divergent-writer"
        _git(publication_checkout, "clone", "--branch", "main", str(remote), str(peer))
        _git(peer, "config", "user.name", "Divergent Writer Fixture")
        _git(peer, "config", "user.email", "divergence@example.invalid")
        (peer / "src/a.py").write_text("VALUE = 'independent remote'\n", encoding="utf-8")
        _git(peer, "add", "src/a.py")
        _git(peer, "commit", "-m", "independent remote branch")
        _git(peer, "push", "origin", "main")
        divergent = _git(peer, "rev-parse", "HEAD")
        # Fetch the exact object without refreshing origin/main. Rejection must
        # prove divergence, not merely inability to resolve an unknown object.
        _git(publication_checkout, "fetch", str(remote), divergent)
        assert _git(publication_checkout, "cat-file", "-t", divergent) == "commit"
        assert _git(publication_checkout, "rev-parse", "origin/main") == cached != divergent
        relation = subprocess.run(
            ["git", "merge-base", "--is-ancestor", divergent, candidate],
            cwd=publication_checkout, capture_output=True,
        )
        assert relation.returncode == 1
        original_index = (publication_checkout / ".git/index").read_bytes()
        before = fence.replay(transaction).events
        with pytest.raises(PublicationFenceError, match="PUBLICATION_ANCESTRY_INVALID"):
            fence.checkpoint(transaction, phase="REMOTE_PUSH_PRE", actor="integration-coordinator")
        assert fence.replay(transaction).events == before
        assert _git(remote, "rev-parse", "main") == divergent
        assert _git(publication_checkout, "rev-parse", "HEAD") == candidate
        assert (publication_checkout / ".git/index").read_bytes() == original_index
        raw_result = summary.read_bytes()
        fence.release(transaction, actor="integration-coordinator", outcome="failed")
        assert summary.read_bytes() == raw_result
        assert not fence.guard.replay().active_leases
        return
    fence.checkpoint(
        transaction,
        phase="REMOTE_PUSH_PRE",
        actor="integration-coordinator",
    )
    if remote_state == "ack-lost":
        cached_main = _git(publication_checkout, "rev-parse", "origin/main")
        assert cached_main != candidate
        # A legitimate non-default fetch mapping leaves origin/main stale;
        # do not hand-edit a tracking ref to manufacture the observation.
        _git(
            publication_checkout, "config", "remote.origin.fetch",
            "+refs/heads/main:refs/remotes/origin/nondefault-main",
        )
        launcher = subprocess.run(
            [
                sys.executable, "-c",
                "import os,subprocess; "
                "subprocess.run(['git','push','origin','main'],check=True,capture_output=True); "
                "os._exit(47)",
            ], cwd=publication_checkout, capture_output=True, timeout=60,
        )
        assert launcher.returncode == 47 and launcher.stdout == b""
        assert _git(publication_checkout, "rev-parse", "origin/main") == cached_main
        assert _git(publication_checkout, "rev-parse", "origin/nondefault-main") == candidate
    else:
        _git(publication_checkout, "push", "origin", "main")
        assert _git(publication_checkout, "rev-parse", "origin/main") == candidate
    remote = Path(_git(publication_checkout, "remote", "get-url", "--push", "origin"))

    probe_count = 0

    def inspect_remote(bootstrap: str | None = None) -> tuple[int, dict[str, Any]]:
        nonlocal probe_count
        before = fence.replay(transaction).events
        command = [
            sys.executable, str(ROOT / "scripts/architecture_arch005_publication_fence.py"),
            "--repository", str(publication_checkout), "--policy", str(DEFAULT_POLICY_PATH),
            "remote-observe", "--transaction", str(publication_checkout / transaction),
        ]
        if bootstrap is not None:
            command = [sys.executable, "-c", bootstrap, *command[1:]]
        result = subprocess.run(
            command, cwd=ROOT, capture_output=True, text=True, timeout=60,
        )
        if m10_bootstrap is not None:
            (publication_checkout.parent / f"m10-observation-{probe_count}.json").write_text(
                json.dumps({"argv": command, "returncode": result.returncode,
                            "stdout": result.stdout, "stderr": result.stderr}), encoding="utf-8"
            )
            probe_count += 1
        assert not result.stderr, result.stderr
        assert fence.replay(transaction).events == before
        return result.returncode, json.loads(result.stdout)

    code, observation = inspect_remote()
    assert code == 0 and observation["status"] == "OBSERVED"
    assert observation["tip_sha"] == _git(remote, "rev-parse", "main") == candidate
    assert observation["endpoint_sha256"] == hashlib.sha256(
        _git(publication_checkout, "remote", "get-url", "--push", "origin").encode()
    ).hexdigest()
    assert observation["push_allowed"] is False and observation["mutation_performed"] is False
    if remote_state == "advanced":
        peer = publication_checkout.parent / "remote-writer"
        _git(publication_checkout, "clone", "--branch", "main", str(remote), str(peer))
        _git(peer, "config", "user.name", "Remote Writer Fixture")
        _git(peer, "config", "user.email", "remote-writer@example.invalid")
        _git(peer, "commit", "--allow-empty", "-m", "remote advances after candidate push")
        _git(peer, "push", "origin", "main")
        newer = _git(peer, "rev-parse", "HEAD")
        assert _git(remote, "rev-parse", "main") == newer != candidate
        assert _git(publication_checkout, "rev-parse", "origin/main") == candidate
        code, observed = inspect_remote()
        assert code == 0
        _assert_observed_actual_remote_tip(observed, newer)
        assert observed["candidate_is_remote_tip"] is False and observed["push_allowed"] is False
        if m10_bootstrap is not None:
            before_mutant = (
                fence.replay(transaction).events, fence.guard.replay().head_event_ids,
                _git(publication_checkout, "show-ref"),
                (publication_checkout / ".git/index").read_bytes(), summary.read_bytes(),
            )
            bad_code, bad_observation = inspect_remote(m10_bootstrap)
            assert bad_code == 0 and bad_observation["status"] == "OBSERVED"
            assert bad_observation["tip_sha"] == candidate != newer
            assert bad_observation["candidate_is_remote_tip"] is True
            assert bad_observation["push_allowed"] is False
            with pytest.raises(
                AssertionError, match=r"^REMOTE_TIP_MUST_MATCH_ACTUAL_ENDPOINT(?:\n|$)"
            ):
                _assert_observed_actual_remote_tip(bad_observation, newer)
            assert before_mutant == (
                fence.replay(transaction).events, fence.guard.replay().head_event_ids,
                _git(publication_checkout, "show-ref"),
                (publication_checkout / ".git/index").read_bytes(), summary.read_bytes(),
            )
            assert _git(remote, "rev-parse", "main") == newer
            (publication_checkout.parent / "m10-counterfactual.json").write_text(json.dumps({
                "schema_version": "devx015_m10_counterfactual.v1", "mutant_id": "M10",
                "target_assertion_killed": True,
                "target_assertion": "REMOTE_TIP_MUST_MATCH_ACTUAL_ENDPOINT",
                "candidate": candidate, "actual_remote": newer, "local_tracking": candidate,
                "full_summary_sha256": hashlib.sha256(summary.read_bytes()).hexdigest(),
                "scope": "ORIGINAL_ACTUAL_FULL_PUBLIC_REMOTE_OBSERVER",
                "formal_project_acceptance": False,
            }), encoding="utf-8")
        with pytest.raises(PublicationFenceError, match="PUBLICATION_REMOTE_SHA_MISMATCH"):
            fence.checkpoint(transaction, phase="CLEANUP_PRE", actor="integration-coordinator")
        assert fence.replay(transaction).phase == "REMOTE_PUSH_PRE"
        assert _git(remote, "rev-parse", "main") == newer
        fence.release(transaction, actor="integration-coordinator", outcome="failed")
        assert not fence.guard.replay().active_leases
        return
    if remote_state == "unreachable":
        offline = remote.with_name("offline-origin.git")
        # Move only this test's disposable bare repository; preserve and restore it.
        assert remote.resolve().parent == publication_checkout.resolve().parent
        assert offline.resolve().parent == remote.resolve().parent and not offline.exists()
        remote.rename(offline)
        try:
            code, observed = inspect_remote()
            assert code == 2 and observed["status"] == "REMOTE_UNKNOWN"
            assert observed["push_allowed"] is False
            assert observed["allowed_actions"] == ["remote-observe"]
            with pytest.raises(PublicationFenceError, match="PUBLICATION_REMOTE_UNKNOWN"):
                fence.checkpoint(
                    transaction, phase="CLEANUP_PRE", actor="integration-coordinator"
                )
            assert fence.replay(transaction).phase == "REMOTE_PUSH_PRE"
            assert _git(publication_checkout, "rev-parse", "origin/main") == candidate
        finally:
            offline.rename(remote)
        code, observed = inspect_remote()
        assert code == 0 and observed["candidate_is_remote_tip"] is True
    if remote_state == "endpoint-changed":
        _git(publication_checkout, "remote", "set-url", "--push", "origin", str(remote) + ".other")
        try:
            code, observed = inspect_remote()
            assert code == 2 and observed["reason_code"] == "PUBLICATION_REMOTE_ENDPOINT_CHANGED"
            with pytest.raises(PublicationFenceError, match="PUBLICATION_REMOTE_ENDPOINT_CHANGED"):
                fence.checkpoint(transaction, phase="CLEANUP_PRE", actor="integration-coordinator")
            assert _git(remote, "rev-parse", "main") == candidate
        finally:
            _git(publication_checkout, "config", "--unset-all", "remote.origin.pushurl")
    def ack_recovery_cli(label: str, arguments: list[str]) -> dict[str, Any]:
        # Fresh original candidate CLI, with the candidate's own imported code.
        # This is recovery after a real successful push whose launcher died.
        environment = dict(os.environ, PYTHONPATH=str(publication_checkout / "src"),
                           PYTHONDONTWRITEBYTECODE="1")
        command = [sys.executable,
                   str(publication_checkout / "scripts/architecture_arch005_publication_fence.py"),
                   "--repository", str(publication_checkout), *arguments]
        completed = subprocess.run(
            command, cwd=publication_checkout, env=environment, capture_output=True,
            text=True, encoding="utf-8", timeout=300,
        )
        (publication_checkout.parent / (label + ".json")).write_text(json.dumps({
            "argv": command, "returncode": completed.returncode,
            "stdout": completed.stdout, "stderr": completed.stderr,
        }), encoding="utf-8")
        assert completed.returncode == 0, completed.stdout + completed.stderr
        return json.loads(completed.stdout)

    if remote_state == "ack-lost":
        retained_refs = _git(publication_checkout, "show-ref")
        retained_index = (publication_checkout / ".git/index").read_bytes()
        retained_summary = summary.read_bytes()
        retained_lease = fence.guard.replay().active_leases[0]
        recovered = ack_recovery_cli("ack-lost-cleanup", [
            "checkpoint", "--transaction", str(transaction), "--phase", "CLEANUP_PRE",
        ])
        assert recovered["phase"] == "CLEANUP_PRE"
    elif remote_state.startswith("probe-"):
        from datetime import UTC, datetime

        from test_devx015_workflow_execution import NativeOracle

        environment = dict(
            os.environ, PYTHONPATH=str(publication_checkout / "src"), PYTHONDONTWRITEBYTECODE="1"
        )
        process = subprocess.Popen(
            [
                sys.executable,
                str(publication_checkout / "scripts/architecture_arch005_publication_fence.py"),
                "checkpoint", "--transaction", str(transaction), "--phase", "CLEANUP_PRE",
            ], cwd=publication_checkout, env=environment,
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        )
        try:
            assert process.stdout is not None and process.stdin is not None
            witness = json.loads(process.stdout.readline())["probe_barrier"]
            native = NativeOracle()
            with native.process(witness["pid"]) as handle:
                assert native.creation_time(handle) == witness["creation_time"]
                assert not native.exited(handle)
                # A different PID must be able to use the real same store while
                # the remote observation is paused, not merely after timeout.
                with fence.guard.store.atomic(
                    actor="integration-coordinator", now=datetime.now(UTC), operation="compound"
                ):
                    assert fence.replay(transaction).phase == "REMOTE_PUSH_PRE"
                if remote_state == "probe-state-race":
                    fence.release(transaction, actor="integration-coordinator", outcome="failed")
                process.stdin.write("continue\n")
                process.stdin.flush()
                output, error = process.communicate(timeout=60)
                assert native.exited(handle, timeout=5)
            result = json.loads(output)
            assert not error, error
            if remote_state == "probe-state-race":
                assert process.returncode == 2
                assert result["reason_code"] == "PUBLICATION_REMOTE_OBSERVATION_STALE"
                assert fence.replay(transaction).phase == "FAILED"
                assert not fence.guard.replay().active_leases
                return
            assert process.returncode == 0, result
            assert result["phase"] == "CLEANUP_PRE"
        finally:
            if process.poll() is None:
                process.kill()
                process.communicate(timeout=30)
    else:
        fence.checkpoint(
            transaction,
            phase="CLEANUP_PRE",
            actor="integration-coordinator",
        )
    if remote_state == "ack-lost":
        release_arguments = ["release", "--transaction", str(transaction),
                             "--outcome", "completed", "--evidence", str(summary)]
        receipt = ack_recovery_cli("ack-lost-release", release_arguments)
        terminal = fence.replay(transaction)
        assert ack_recovery_cli("ack-lost-release-replay", release_arguments) == receipt
        assert fence.replay(transaction) == terminal
        assert _git(publication_checkout, "show-ref") == retained_refs
        assert (publication_checkout / ".git/index").read_bytes() == retained_index
        assert summary.read_bytes() == retained_summary
        final_lease = next(row for row in fence.guard.replay().lease_heads
                           if row.lease_id == retained_lease.lease_id)
        assert full_execution_projection(final_lease.execution) == full_execution_projection(
            retained_lease.execution,
        )
        assert not fence.guard.replay().active_leases
    else:
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
    cleanup = next(
        event for event in fence.replay(transaction).events if event["phase"] == "CLEANUP_PRE"
    )
    assert receipt.get("remote_confirmation") == {
        "candidate_sha": candidate,
        "source_event_id": cleanup["event_id"],
        "point_in_time": True,
        "observation": cleanup["payload"]["remote_observation"],
    }
    assert receipt["remote_confirmation"]["observation"]["tip_sha"] == candidate
    assert (
        fence.release(
            transaction,
            actor="integration-coordinator",
            outcome="completed",
            evidence_paths=(summary,),
        )
        == receipt
    )


@pytest.mark.parametrize("damage", ["none", "rehashed-transaction"])
def test_x02_original_checkpoint_rejects_transaction_change_after_admission(
    publication_checkout, damage,
):
    _exercise_original_checkpoint_admission_race(publication_checkout, damage)


@pytest.mark.parametrize("damage", ["none", "main-advance"])
@pytest.mark.parametrize("ref_layout", [
    "loose", "packed", "loose-no-log", "packed-no-log", "symbolic",
])
def test_x02_original_checkpoint_rejects_main_change_after_admission(
    publication_checkout, damage, ref_layout,
):
    if ref_layout.startswith("packed"):
        _git(publication_checkout, "pack-refs", "--all", "--prune")
        assert not (publication_checkout / ".git/refs/heads/main").exists()
    if ref_layout.endswith("no-log"):
        _git(publication_checkout, "config", "core.logAllRefUpdates", "false")
        (publication_checkout / ".git/logs/refs/heads/main").unlink()
    if ref_layout == "symbolic":
        main = _git(publication_checkout, "rev-parse", "refs/heads/main")
        _git(publication_checkout, "update-ref", "refs/heads/main-target", main)
        _git(publication_checkout, "symbolic-ref", "refs/heads/main", "refs/heads/main-target")
    _exercise_original_checkpoint_admission_race(publication_checkout, damage)


def _exercise_original_checkpoint_admission_race(root, damage):
    from test_arch_005_task_checkpoint import _terminate_fixture_producer
    from test_devx015_workflow_execution import NativeOracle, _until

    fence = _fence(root)
    binding = _acquire(fence, root, transaction_id="transaction-race")
    transaction = root / binding["transaction_path"]
    original = transaction.read_bytes()
    event_root = transaction.parent / "events"
    before_events = {p.name: p.read_bytes() for p in event_root.glob("*")}
    before_heads = tuple(fence.guard.replay().head_event_ids)
    refs, index = _git(root, "show-ref"), (root / ".git/index").read_bytes()
    main_before = _git(root, "rev-parse", "refs/heads/main")
    head_before = _git(root, "rev-parse", "HEAD")
    successor = (
        _git(root, "commit-tree", "HEAD^{tree}", "-p", main_before, "-m", "independent main")
        if damage == "main-advance" else None
    )
    ready, release = root.parent / "checkpoint-ready.json", root.parent / "checkpoint-release"
    (root.parent / "original-transaction.json").write_bytes(original)
    (root.parent / "original-fence.py").write_bytes(
        (ROOT / "src/ai_trading_system/platform/architecture/integration_publication_fence.py")
        .read_bytes()
    )
    (root.parent / "original-harness.py").write_bytes(Path(__file__).read_bytes())
    script = ROOT / "scripts/architecture_arch005_publication_fence.py"
    program = """
import json,runpy,sys,time
from pathlib import Path
from ai_trading_system.platform.architecture.integration_publication_fence import (
    IntegrationPublicationFence)
from ai_trading_system.platform.architecture.workflow_execution import current_process_identity
ready,release=Path(sys.argv[1]),Path(sys.argv[2])
original=IntegrationPublicationFence._checkpoint_payload
def observe(self,*args,**kwargs):
    payload=original(self,*args,**kwargs)
    ready.write_text(json.dumps(current_process_identity()),encoding='utf-8')
    end=time.monotonic()+30
    while not release.exists():
        if time.monotonic()>end:raise RuntimeError('fixture observation deadline')
        time.sleep(.01)
    return payload
IntegrationPublicationFence._checkpoint_payload=observe
sys.argv=sys.argv[3:]
runpy.run_path(sys.argv[0],run_name='__main__')
"""
    environment = dict(os.environ, PYTHONPATH=str(ROOT / "src"), PYTHONDONTWRITEBYTECODE="1",
                       GIT_CONFIG_NOSYSTEM="1", GIT_CONFIG_GLOBAL=os.devnull)
    identity = None
    changed = False
    denied = None
    try:
        with (root.parent / "checkpoint-cli.stdout").open("wb") as output:
            with (root.parent / "checkpoint-cli.stderr").open("wb") as error:
                process = subprocess.Popen(
                    [sys.executable, "-c", program, str(ready), str(release), str(script),
                     "--repository", str(root), "checkpoint", "--transaction", str(transaction),
                     "--phase", "TASK_SOURCE_PRE_WRITE"],
                    cwd=root, env=environment, stdout=output, stderr=error,
                    creationflags=subprocess.CREATE_NO_WINDOW,
                )
                try:
                    _until(
                        lambda: ready.exists() or process.poll() is not None,
                        description="original checkpoint after real payload admission", timeout=30,
                    )
                    assert ready.exists(), (root.parent / "checkpoint-cli.stderr").read_bytes()
                    identity = json.loads(ready.read_bytes())
                    native = NativeOracle()
                    with native.process(identity["pid"]) as observed:
                        assert native.creation_time(observed) == identity["creation_time"]
                        assert not native.exited(observed)
                        if damage == "main-advance":
                            advance = subprocess.run(
                                ["git", "update-ref", "refs/heads/main", successor, main_before],
                                cwd=root, env=environment, capture_output=True, text=True,
                                timeout=15, check=False,
                            )
                            (root.parent / "independent-main-update.json").write_text(
                                json.dumps({"exit_code": advance.returncode,
                                            "stdout": advance.stdout, "stderr": advance.stderr}),
                                encoding="utf-8",
                            )
                            changed = advance.returncode == 0
                            assert changed or advance.returncode == 128, advance.stderr
                            if not changed:
                                protected = root / ".git/refs/heads/main"
                                if not protected.exists():
                                    protected = root / ".git/packed-refs"
                                native_output = root.parent / "main-write-denied.json"
                                probe = subprocess.run(
                                    [sys.executable, "-c", _publication_input_write_probe_source(),
                                     "denied", str(native_output), str(protected)],
                                    capture_output=True, text=True, timeout=15, check=False,
                                )
                                assert probe.returncode == 0, (probe.stdout, probe.stderr)
                                assert json.loads(native_output.read_text())[0]["winerror"] == 32
                        elif damage != "none":
                            from ai_trading_system.platform.artifacts import canonical_json_bytes

                            body = json.loads(original)
                            body.pop("transaction_sha256")
                            body["thread_id"] += "-external-change"
                            body["transaction_sha256"] = hashlib.sha256(
                                json.dumps(body, sort_keys=True, ensure_ascii=False,
                                           separators=(",", ":")).encode("utf-8")
                            ).hexdigest()
                            mutant = canonical_json_bytes(body)
                            (root.parent / "attempted-transaction.json").write_bytes(mutant)
                            try:
                                transaction.write_bytes(mutant)
                                changed = True
                            except PermissionError as exc:
                                assert exc.errno == 13 or exc.winerror in {5, 32}, exc
                                # Python's CRT open can discard the native error.
                                # Require an independent CreateFileW observation.
                                native_output = root.parent / "transaction-write-denied.json"
                                probe = subprocess.run(
                                    [sys.executable, "-c", _publication_input_write_probe_source(),
                                     "denied", str(native_output), str(transaction)],
                                    capture_output=True, text=True, timeout=15, check=False,
                                )
                                assert probe.returncode == 0, (probe.stdout, probe.stderr)
                                denied = json.loads(native_output.read_text())[0]["winerror"]
                                assert denied == 32
                        release.write_text("continue", encoding="utf-8")
                        process.wait(timeout=30)
                        assert native.exited(observed, 10)
                finally:
                    if identity is not None:
                        _terminate_fixture_producer(identity)
                    if process.poll() is None:
                        process.kill()
                        process.wait(timeout=30)
        stdout = (root.parent / "checkpoint-cli.stdout").read_text(encoding="utf-8")
        stderr = (root.parent / "checkpoint-cli.stderr").read_text(encoding="utf-8")
        observation = {"mutation": damage, "mutation_succeeded": changed, "winerror": denied,
                       "returncode": process.returncode, "stdout": stdout, "stderr": stderr}
        (root.parent / "transaction-race-observation.json").write_text(
            json.dumps(observation), encoding="utf-8",
        )
        if changed:
            assert process.returncode == 2, observation
            assert json.loads(stdout)["status"] == "BLOCKED" and not stderr
            assert tuple(fence.guard.replay().head_event_ids) == before_heads
            assert {p.name: p.read_bytes() for p in event_root.glob("*")} == before_events
        else:
            assert process.returncode == 0 and json.loads(stdout)["status"] == "PASS", observation
            assert transaction.read_bytes() == original
            replay = fence.replay(transaction)
            assert replay.status == "PASS" and replay.phase == "TASK_SOURCE_PRE_WRITE"
        if damage == "main-advance" and changed:
            assert _git(root, "rev-parse", "refs/heads/main") == successor
            assert _git(root, "rev-parse", "HEAD") == head_before
        else:
            assert _git(root, "show-ref") == refs
        assert (root / ".git/index").read_bytes() == index
        if damage == "main-advance" and not changed:
            # After the original process exits, the exact refused Git command
            # must progress. This rules out an unrelated permission failure or
            # a leaked native hold masquerading as a successful race guard.
            progress = subprocess.run(
                advance.args, cwd=root, env=environment, capture_output=True, text=True,
                timeout=15, check=False,
            )
            assert progress.returncode == 0, (progress.stdout, progress.stderr)
            assert _git(root, "rev-parse", "refs/heads/main") == successor
            assert _git(root, "rev-parse", "HEAD") == head_before
            (root.parent / "main-progress-after-checkpoint.json").write_text(
                json.dumps({"from": main_before, "to": successor, "original_command_pass": True,
                            "same_argv_and_environment": True, "exit_code": progress.returncode}),
                encoding="utf-8",
            )
    finally:
        if changed and damage == "rehashed-transaction":
            transaction.write_bytes(original)
        replay = fence.replay(transaction)
        if replay.status == "PASS":
            fence.release(transaction, actor="integration-coordinator", outcome="failed")
        else:
            # Preserve corrupt baseline event evidence; release only its exact
            # fixture lease through the original guard, never edit the store.
            fence.guard.release(
                binding["lease_id"], actor="integration-coordinator", outcome="failed",
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


@pytest.mark.parametrize(
    "changed_field",
    (
        "actor",
        "thread_id",
        "frozen_base_sha",
        "owned_paths",
        "shared_paths",
        "generator_ids",
        "required_validation_tiers",
        "integration_plan_path",
        "full_parent_path",
    ),
)
def test_r01_existing_request_rejects_changed_complete_payload(
    publication_checkout: Path,
    changed_field: str,
) -> None:
    """Valid red at the existing acquire API, not at a not-yet-added API."""
    fence = _fence(publication_checkout)
    transaction_id = "complete-request-identity"
    binding = _acquire(fence, publication_checkout, transaction_id=transaction_id)
    transaction = publication_checkout / str(binding["transaction_path"])
    original_transaction = transaction.read_bytes()
    original_events = {
        path.name: path.read_bytes() for path in (transaction.parent / "events").glob("*.json")
    }
    head = _git(publication_checkout, "rev-parse", "HEAD")
    main = _git(publication_checkout, "rev-parse", "main")
    request: dict[str, Any] = {
        "transaction_id": transaction_id,
        "task_id": TASK_ID,
        "change_id": f"{transaction_id}-change",
        "thread_id": f"{transaction_id}-thread",
        "actor": "integration-coordinator",
        "frozen_base_sha": main,
        "lane_head_sha": head,
        "expected_main_sha": main,
        "owned_paths": ("src/a.py",),
        "shared_paths": ("docs/task_register.md", "inputs/generated.json"),
        "generator_ids": ("canonical-task-source",),
    }
    plan = publication_checkout / "outputs/request-plan.json"
    plan.parent.mkdir(parents=True, exist_ok=True)
    plan.write_text('{"plan_id":"different-plan"}\n', encoding="utf-8")
    parent = publication_checkout / "outputs/request-parent.json"
    parent.write_text('{"status":"FAIL","fixture_only":true}\n', encoding="utf-8")
    alternatives: dict[str, Any] = {
        "actor": "different-actor",
        "thread_id": "different-thread",
        "frozen_base_sha": "a" * 40,
        "owned_paths": ("src/b.py",),
        "shared_paths": ("docs/task_register.md", "inputs/generated.json", "src/b.py"),
        "generator_ids": ("canonical-task-source", "architecture-manifests"),
        "required_validation_tiers": (*fence.policy.required_formal_tiers, "fast-unit"),
        "integration_plan_path": plan,
        "full_parent_path": parent,
    }
    request[changed_field] = alternatives[changed_field]
    try:
        with pytest.raises(PublicationFenceError) as error:
            fence.acquire(**request)
        assert error.value.code == "PUBLICATION_TRANSACTION_IDENTITY_CONFLICT"
        assert transaction.read_bytes() == original_transaction
        assert {
            path.name: path.read_bytes() for path in (transaction.parent / "events").glob("*.json")
        } == original_events
        assert len(fence.guard.replay().active_leases) == 1
    finally:
        fence.release(transaction, actor="integration-coordinator", outcome="failed")


def test_r02_terminal_receipt_crash_is_recovered_in_fresh_process(
    publication_checkout: Path,
) -> None:
    """Kill a real producer after its terminal event; recover using another PID."""
    fence = _fence(publication_checkout)
    binding = _acquire(fence, publication_checkout, transaction_id="terminal-receipt-crash")
    transaction = publication_checkout / str(binding["transaction_path"])
    child = """
import json, os, sys
from pathlib import Path
import ai_trading_system.platform.architecture.integration_publication_fence as module
root, policy, checkout, parallel, transaction, action = sys.argv[1:]
fence = module.IntegrationPublicationFence(
    project_root=Path(root), policy_path=Path(policy),
    checkout_guard_policy_path=Path(checkout), parallel_control_policy_path=Path(parallel))
if action == 'crash':
    original_write = module.write_json_atomic
    def crash_before_receipt(path, payload):
        if Path(path).name == 'closeout_receipt.json':
            (Path(root).parent / 'terminal-crash-boundary.json').write_text(json.dumps({
                'pid': os.getpid(), 'target': str(path), 'pending_receipt': payload,
                'exit_code': 17}), encoding='utf-8')
            os._exit(17)
        return original_write(path, payload)
    module.write_json_atomic = crash_before_receipt
receipt = fence.release(Path(transaction), actor='integration-coordinator', outcome='failed')
print(json.dumps(receipt, sort_keys=True))
"""
    command = [
        sys.executable,
        "-c",
        child,
        str(publication_checkout),
        str(DEFAULT_POLICY_PATH),
        str(CHECKOUT_POLICY),
        str(PARALLEL_POLICY),
        str(transaction),
    ]
    crashed = subprocess.run(
        [*command, "crash"], capture_output=True, text=True, timeout=60, check=False
    )
    assert crashed.returncode == 17, crashed.stderr
    assert not (transaction.parent / "closeout_receipt.json").exists()
    terminal_files = sorted((transaction.parent / "events").glob("*.json"))
    original_events = {path.name: path.read_bytes() for path in terminal_files}
    terminal = json.loads(terminal_files[-1].read_text(encoding="utf-8"))
    assert terminal["phase"] == "FAILED"
    assert fence.guard.replay().active_leases == ()
    boundary = json.loads(
        (publication_checkout.parent / "terminal-crash-boundary.json").read_bytes()
    )
    assert boundary["pid"] != os.getpid()
    assert boundary["target"] == str(transaction.parent / "closeout_receipt.json")
    before = _v03_unchanged_state(fence, transaction)
    recovery_command = ["release", "--transaction", str(transaction), "--outcome", "failed"]
    recovered = _v03_fence_cli(publication_checkout, "original-cli-recovery", recovery_command)
    assert recovered["exit_code"] == 0, recovered
    receipt = json.loads(recovered["stdout"])
    assert receipt["status"] == "FAIL"
    assert receipt["final_phase"] == "FAILED"
    assert receipt["head_event_id"] == terminal["event_id"]
    assert receipt["completed_at"] == terminal["occurred_at"]
    assert receipt == json.loads(
        (transaction.parent / "closeout_receipt.json").read_text(encoding="utf-8")
    )
    assert receipt == boundary["pending_receipt"]
    after = _v03_unchanged_state(fence, transaction)
    owner_path = str(fence.guard.runtime_root / "leases/arbiter.owner.json")
    receipt_path = str(transaction.parent / "closeout_receipt.json")
    assert set(after["governance"]) - set(before["governance"]) == {receipt_path}
    assert set(before["governance"]) - set(after["governance"]) == set()
    assert after["refs"] == before["refs"] and after["index"] == before["index"]
    assert all(after["governance"][path] == raw
               for path, raw in before["governance"].items() if path != owner_path)
    duplicate = _v03_fence_cli(
        publication_checkout, "original-cli-recovery-replay", recovery_command,
    )
    assert duplicate["exit_code"] == 0, duplicate
    assert json.loads(duplicate["stdout"]) == receipt
    replayed = _v03_unchanged_state(fence, transaction)
    assert set(replayed["governance"]) == set(after["governance"])
    assert all(replayed["governance"][path] == raw
               for path, raw in after["governance"].items() if path != owner_path)
    assert replayed["refs"] == before["refs"] and replayed["index"] == before["index"]
    for snapshot in (after, replayed):
        assert json.loads(snapshot["governance"][owner_path])["state"] == "RELEASED"
    assert fence.guard.replay().active_leases == ()
    assert {
        path.name: path.read_bytes() for path in (transaction.parent / "events").glob("*.json")
    } == original_events


def test_r02_terminal_replay_rejects_modified_receipt(publication_checkout: Path) -> None:
    fence = _fence(publication_checkout)
    binding = _acquire(fence, publication_checkout, transaction_id="receipt-tamper")
    transaction = publication_checkout / str(binding["transaction_path"])
    fence.release(transaction, actor="integration-coordinator", outcome="failed")
    receipt_path = transaction.parent / "closeout_receipt.json"
    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    receipt["status"] = "PASS"
    receipt_path.write_text(json.dumps(receipt), encoding="utf-8")
    tampered = receipt_path.read_bytes()
    with pytest.raises(PublicationFenceError) as error:
        _fence(publication_checkout).release(
            transaction, actor="integration-coordinator", outcome="failed"
        )
    assert error.value.code == "PUBLICATION_TERMINAL_RECEIPT_MISMATCH"
    assert receipt_path.read_bytes() == tampered


def test_r02_terminal_replay_rejects_wrong_actor(publication_checkout: Path) -> None:
    fence = _fence(publication_checkout)
    binding = _acquire(fence, publication_checkout, transaction_id="receipt-actor")
    transaction = publication_checkout / str(binding["transaction_path"])
    fence.release(transaction, actor="integration-coordinator", outcome="failed")
    with pytest.raises(PublicationFenceError) as error:
        fence.release(transaction, actor="another-actor", outcome="failed")
    assert error.value.code == "PUBLICATION_ACTOR_MISMATCH"


def test_x02_two_process_release_has_one_terminal_transition(publication_checkout: Path) -> None:
    """Hold A after lease release, then enter B before A's publication append."""
    fence = _fence(publication_checkout)
    binding = _acquire(fence, publication_checkout, transaction_id="concurrent-terminal")
    transaction = publication_checkout / str(binding["transaction_path"])
    child = """
import json, sys
from pathlib import Path
from ai_trading_system.platform.architecture.integration_publication_fence import (
    IntegrationPublicationFence, PublicationFenceError)
root, policy, checkout, parallel, transaction, action = sys.argv[1:]
fence = IntegrationPublicationFence(project_root=Path(root), policy_path=Path(policy),
    checkout_guard_policy_path=Path(checkout), parallel_control_policy_path=Path(parallel))
if action == 'producer':
    original = fence.guard.release
    def pause(*args, **kwargs):
        result = original(*args, **kwargs)
        print('LEASE_RELEASED_BARRIER', flush=True)
        assert sys.stdin.readline().strip() == 'continue'
        return result
    fence.guard.release = pause
try:
    result = fence.release(Path(transaction), actor='integration-coordinator', outcome='failed')
except PublicationFenceError as exc:
    result = {'reason_code': exc.code}
print(json.dumps(result, sort_keys=True), flush=True)
"""
    command = [
        sys.executable,
        "-c",
        child,
        str(publication_checkout),
        str(DEFAULT_POLICY_PATH),
        str(CHECKOUT_POLICY),
        str(PARALLEL_POLICY),
        str(transaction),
    ]
    producer = subprocess.Popen(
        [*command, "producer"],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        assert producer.stdout is not None and producer.stdin is not None
        assert producer.stdout.readline().strip() == "LEASE_RELEASED_BARRIER"
        before = _v03_unchanged_state(fence, transaction)
        release_command = ["release", "--transaction", str(transaction), "--outcome", "failed"]
        observer = _v03_fence_cli(publication_checkout, "x02-contending-release", release_command)
        assert observer["exit_code"] == 2, observer
        observed = json.loads(observer["stdout"])
        assert observed["reason_code"] == "PUBLICATION_BUSY"
        assert producer.poll() is None
        assert _v03_unchanged_state(fence, transaction) == before
        producer.stdin.write("continue\n")
        producer.stdin.flush()
        output, error = producer.communicate(timeout=60)
        assert producer.returncode == 0, error
        produced = json.loads(output)
        # A busy observer is not an execution grant. Retry only after A actually exits.
        assert produced.get("final_phase") == "FAILED", produced
        terminal_before = {
            str(path): path.read_bytes() for path in transaction.parent.rglob("*.json")
        }
        replayed = _v03_fence_cli(
            publication_checkout, "x02-terminal-release-replay", release_command,
        )
        assert replayed["exit_code"] == 0, replayed
        assert json.loads(replayed["stdout"]) == produced
        assert {str(path): path.read_bytes() for path in transaction.parent.rglob("*.json")} == (
            terminal_before
        )
        after = _v03_unchanged_state(fence, transaction)
        assert after["refs"] == before["refs"] and after["index"] == before["index"]
        assert fence.guard.replay().active_leases == ()
        events = [
            json.loads(p.read_bytes()) for p in (transaction.parent / "events").glob("*.json")
        ]
        assert [e["phase"] for e in sorted(events, key=lambda e: e["sequence"])] == [
            "ACQUIRED",
            "FAILED",
        ]
        assert len({e["sequence"] for e in events}) == len(events)
    finally:
        if producer.poll() is None:
            producer.kill()
            producer.communicate(timeout=60)


@pytest.mark.parametrize("advance_at", ["before-dispatch", "before-result"])
@pytest.mark.parametrize("technical_status", ["PASS", "FAIL"])
def test_fixed_candidate_result_survives_main_advance_without_publication_permission(
    publication_checkout: Path, advance_at: str, technical_status: str
) -> None:
    """Fence transition oracle only; actual runner coverage is separately required."""
    root = publication_checkout
    fence = _fence(root)
    binding = _acquire(fence, root, transaction_id="fixed-validation")
    transaction = Path(str(binding["transaction_path"]))
    for phase in (
        "TASK_SOURCE_PRE_WRITE", "GENERATED_REBUILD_PRE", "GENERATED_REBUILD_POST",
        "CANDIDATE_COMMIT_PRE", "FORMAL_VALIDATION_PRE",
    ):
        fence.checkpoint(
            transaction, phase=phase, actor="integration-coordinator",
            generator_ids=("canonical-task-source",) if phase.startswith("GENERATED_") else (),
        )
    candidate = _git(root, "rev-parse", "HEAD")
    original_index = (root / ".git/index").read_bytes()
    peer = root.parent / "main-writer"
    _git(root, "worktree", "add", str(peer), "main")

    def advance_main() -> str:
        (peer / "src/a.py").write_text("VALUE = 2\n", encoding="utf-8")
        _git(peer, "add", "src/a.py")
        _git(peer, "commit", "-m", "independent main advance")
        return _git(peer, "rev-parse", "HEAD")

    if advance_at == "before-dispatch":
        newer_main = advance_main()
    fence.validate(
        transaction, exact_phase="FORMAL_VALIDATION_PRE", task_id=TASK_ID,
        validation_tier="full", require_candidate=True,
    )
    fence.checkpoint(
        transaction, phase="FULL_DISPATCHED", actor="integration-coordinator",
        full_run_id="fixed-candidate-run",
    )
    if advance_at == "before-result":
        newer_main = advance_main()
    evidence = root / "outputs/validation_runtime/fixed/test_runtime_summary.json"
    evidence.parent.mkdir(parents=True, exist_ok=True)
    raw = json.dumps({"candidate": candidate, "status": technical_status}).encode()
    evidence.write_bytes(raw)
    _record_contained_full_fixture_result(fence, transaction, evidence, status=technical_status)
    recorded = fence.checkpoint(
        transaction, phase="FORMAL_VALIDATION_RESULT", actor="integration-coordinator",
        evidence_paths=(evidence,), validation_status=technical_status,
    )
    assert recorded["candidate_sha"] == candidate
    replay = fence.replay(transaction)
    assert replay.status == "PASS" and replay.phase == "FORMAL_VALIDATION_RESULT"
    assert replay.events[-1]["payload"]["validation_status"] == technical_status
    assert replay.events[-1]["payload"]["observed_main"] == newer_main != candidate
    assert evidence.read_bytes() == raw
    with pytest.raises(PublicationFenceError, match="PUBLICATION_EXPECTED_MAIN_STALE"):
        fence.validate(transaction, require_candidate=True)
    with pytest.raises(PublicationFenceError, match="PUBLICATION_EXPECTED_MAIN_STALE"):
        fence.checkpoint(transaction, phase="LOCAL_MAIN_FF_PRE", actor="integration-coordinator")
    assert _git(root, "rev-parse", "HEAD") == candidate
    assert _git(root, "rev-parse", "main") == newer_main
    assert (root / ".git/index").read_bytes() == original_index
    assert (root / "src/a.py").read_text(encoding="utf-8") == "VALUE = 1\n"
    fence.release(transaction, actor="integration-coordinator", outcome="failed")
    assert not fence.guard.replay().active_leases
    assert evidence.read_bytes() == raw


def test_checkpoint_rejects_caller_remote_observation_before_mutation(
    publication_checkout: Path,
) -> None:
    fence = _fence(publication_checkout)
    binding = _acquire(fence, publication_checkout, transaction_id="forged-observation")
    transaction = Path(str(binding["transaction_path"]))
    before = fence.replay(transaction).events
    lease_heads = fence.guard.replay().head_event_ids
    with pytest.raises(PublicationFenceError, match="PUBLICATION_CALLER_OBSERVATION_FORBIDDEN"):
        fence.checkpoint(
            transaction, phase="REMOTE_PUSH_PRE", actor="integration-coordinator",
            _remote_preparation=("fake", "fake", "REMOTE_PUSH_PRE", b'{"status":"OBSERVED"}'),
        )
    assert fence.replay(transaction).events == before
    assert fence.guard.replay().head_event_ids == lease_heads
    fence.release(transaction, actor="integration-coordinator", outcome="failed")
    assert not fence.guard.replay().active_leases


@pytest.mark.parametrize("canonical_merge_repository", ["full-profile-publish"], indirect=True)
def test_terminal_confirmation_cannot_be_fabricated_from_missing_observation(
    canonical_merge_repository,
) -> None:
    """Projection parser boundary; never substitute this for public replay validation."""
    from dataclasses import replace

    publication_checkout, _scope = canonical_merge_repository
    _run_actual_publication_fixture(publication_checkout, "normal")
    fence = IntegrationPublicationFence(project_root=publication_checkout)
    replay = fence.replay(
        fence.runtime_root / "transactions/merge-authority/transaction.json"
    )
    events = json.loads(json.dumps(replay.events))
    cleanup = next(event for event in events if event["phase"] == "CLEANUP_PRE")
    cleanup["payload"]["remote_observation"] = None
    events[-1]["payload"]["remote_confirmation"]["observation"] = None
    with pytest.raises(PublicationFenceError, match="PUBLICATION_TERMINAL_REPLAY_INVALID"):
        fence._terminal_receipt(replace(replay, events=tuple(events)))


def _record_contained_full_fixture_result(
    fence: IntegrationPublicationFence, transaction: Path, summary: Path, *,
    status: str = "PASS", commit_result: bool = True, reserved_at=None,
) -> None:
    """Actual Job/custody, synthetic validation; not formal readiness acceptance."""
    from test_devx015_workflow_execution import NativeOracle

    from ai_trading_system.platform.architecture.workflow_contract import canonical_digest
    from ai_trading_system.platform.architecture.workflow_execution import (
        WindowsJobProcess,
        execution_environment_sha256,
    )
    from ai_trading_system.platform.artifacts import canonical_json_bytes

    replay = fence.replay(transaction)
    claim = replay.events[-1]["payload"]["dispatch_request"]
    lease = fence.guard.store.replay().active_leases[0]
    actor = replay.transaction["actor"]
    request_id = canonical_digest({
        "transaction": replay.transaction["transaction_sha256"],
        "full_run_id": claim["full_run_id"],
    })
    environment = dict(os.environ)
    directory = summary.parent.resolve()
    directory.mkdir(parents=True, exist_ok=True)
    assert summary.name == "test_runtime_summary.json"
    exit_code = 0 if status == "PASS" else 7
    request = {
        "schema_version": "workflow_execution_request.v1",
        "request_id": request_id, "lease_id": lease.lease_id,
        "manifest_sha256": lease.change_manifest_sha256, "subject_task_id": lease.task_id,
        "candidate_sha": replay.candidate_sha,
        "validation_identity_sha256": canonical_digest({"scope": "synthetic-fence-transition"}),
        "task_authority_sha256": canonical_digest({"scope": "synthetic-task-fixture"}),
        "argv": [sys.executable, "-c", f"print('actual contained fixture');exit({exit_code})"],
        "cwd": fence.project_root.as_posix(),
        "environment_sha256": execution_environment_sha256(environment),
        "stdout_path": (directory / "execution.stdout.log").as_posix(),
        "result_path": (directory / "execution_result.json").as_posix(),
        "job_name": "Local\\AITS-DEVX015-full-" + request_id,
        "host_id": "synthetic-fence-host", "writer_epoch": "synthetic-fence-epoch",
    }
    lifecycle = fence.guard.store.execution_lifecycle()
    reserved = lifecycle.reserve(request, actor=actor, now=reserved_at)
    assert reserved["dispatch_allowed"] is True
    oracle = NativeOracle()
    with WindowsJobProcess.create(
        argv=request["argv"], cwd=fence.project_root, environment=environment,
        stdout_path=Path(request["stdout_path"]), job_name=request["job_name"],
    ) as process:
        lifecycle.bind(lease.lease_id, process, actor=actor)
        identity = process.identity()
        with oracle.process(identity["pid"]) as native:
            assert oracle.creation_time(native) == identity["creation_time"]
            oracle.assert_in_job(native, request["job_name"])
            lifecycle.resume(lease.lease_id, process, actor=actor)
            assert process.wait(timeout=20) == exit_code
            assert oracle.exited(native, 10)
        lifecycle.confirm_exit(lease.lease_id, process, actor=actor)
    record = {
        "schema_version": "full_execution_result.v1",
        "candidate_sha": replay.candidate_sha,
        "validation_identity_sha256": request["validation_identity_sha256"],
        "request_id": request_id, "status": status,
        "summary": {"path": summary.resolve().as_posix(),
                    "sha256": hashlib.sha256(summary.read_bytes()).hexdigest()},
    }
    if commit_result:
        lifecycle.commit_full_result(lease.lease_id, actor=actor, record=record)
    result_path = Path(request["result_path"])
    result_path.write_bytes(canonical_json_bytes(record))
    lifecycle.record_result(lease.lease_id, actor=actor, result_path=result_path)


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
    full_parent: Path | None = None,
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
        full_parent_path=full_parent,
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
