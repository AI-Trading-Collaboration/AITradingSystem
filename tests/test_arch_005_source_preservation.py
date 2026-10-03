"""Synthetic Git source preservation; never research or publication authority.

The ordinary unit fixture substitutes only the *implementation identity* binding
while the new implementation is uncommitted. Source Git, terminal publication,
canonical task history, S4D leases and snapshots remain real. The separately named
committed-implementation E2E must execute after source commit and in final Full.
"""

from __future__ import annotations

import copy
import hashlib
import importlib.util
import json
import os
import shutil
import subprocess
import sys
import traceback
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from threading import Barrier, Event
from types import SimpleNamespace
from typing import Any

import pytest

from ai_trading_system.platform.architecture import source_preservation as preservation
from ai_trading_system.platform.architecture.checkout_guard import (
    CheckoutGuardError,
    CheckoutLeaseGuard,
)
from ai_trading_system.platform.architecture.integration_publication_fence import (
    IntegrationPublicationFence,
    PublicationFenceError,
)
from ai_trading_system.platform.architecture.integration_revalidation import (
    IntegrationRevalidationPolicy,
    build_integration_revalidation_plan,
)
from ai_trading_system.platform.architecture.source_preservation import (
    DEFAULT_POLICY_PATH,
    SourcePreservation,
    SourcePreservationError,
)
from ai_trading_system.platform.architecture.task_registry_canonical import (
    validate_canonical_fragment,
)

# Test hang bounds, not production policy. DEVX-018 load calibration (provisional, owner review
# pending; exit condition in docs/requirements/DEVX-018_Validation_Runtime_Throughput_V1.md): steps that
# hash the runtime identity (~11s idle, 116-194s loaded), run an inner Full or call a publication CLI
# scale with formal-Full load, so their guards sit above the 900s production profile inspector bound.
LOADED_HOST_CLI_TIMEOUT_SECONDS = 1800

ROOT = Path(__file__).resolve().parents[1]
EXCLUDED = "docs/research/growth_tilt_owner_diagnosis_pack.md"
EXCLUSION = f":(exclude,literal){EXCLUDED}"
SOURCE_TASK = "SYNTHETIC_SOURCE_PRESERVATION_TASK"
RECOVERY_TASK = "DEVX-014_DIRTY_SOURCE_PRESERVATION_RECOVERY_V1"
OWNER_REF = "owner_instruction:DEVX-014:2026-09-06:source-only-recovery"
ACTOR = "integration-coordinator"
BRANCH = "codex/synthetic-source"
RUNTIME = "outputs/architecture/arch_005_source_preservation"
POLICIES = (
    "config/architecture/arch_005_integration_publication_fence.yaml",
    "config/architecture/arch_005_s4d_checkout_guard.yaml",
    "config/architecture/arch_005_parallel_control_policy.yaml",
)
TASK_DIGEST = hashlib.sha256(SOURCE_TASK.encode()).hexdigest()
TASK_PATH = f"registry/development_tasks/{TASK_DIGEST[:2]}/{TASK_DIGEST}.yaml"
SOURCE_PATHS = (TASK_PATH, "src/a.py", "src/raw.bin")


def _sha(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def _json(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":")).encode()


def _git(repo: Path, *args: str, content: bytes | None = None) -> bytes:
    # This helper operates only on pytest's synthetic repository. Explicit
    # allowlists are used for every content inventory/add/diff operation.
    environment = {key: value for key, value in os.environ.items() if not key.startswith("GIT_")}
    environment.update(
        {
            "GIT_CONFIG_NOSYSTEM": "1",
            "GIT_CONFIG_GLOBAL": os.devnull,
            "GIT_TERMINAL_PROMPT": "0",
            "GIT_OPTIONAL_LOCKS": "0",
        }
    )
    return subprocess.run(
        [
            "git",
            "--no-replace-objects",
            "--no-lazy-fetch",
            "-c",
            "core.autocrlf=false",
            "-c",
            "commit.gpgSign=false",
            "-c",
            f"core.hooksPath={repo / '.git/no-hooks'}",
            *args,
        ],
        cwd=repo,
        env=environment,
        input=content,
        capture_output=True,
        check=True,
        timeout=60,
    ).stdout


def _ref(repo: Path, name: str) -> str:
    return _git(repo, "rev-parse", name).decode().strip()


def _write(repo: Path, relative: str, content: bytes) -> None:
    path = repo / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)


def _commit(repo: Path, paths: tuple[str, ...], message: str) -> str:
    _git(repo, "add", "--", *(f":(literal){path}" for path in paths))
    _git(repo, "commit", "-m", message)
    return _ref(repo, "HEAD")


def _event_id(event: dict[str, Any]) -> str:
    return "task-event-" + _sha(_json({k: v for k, v in event.items() if k != "event_id"}))[:32]


def _seal_fragment(fragment: dict[str, Any]) -> dict[str, Any]:
    fragment["last_event_id"] = fragment["events"][-1]["event_id"]
    fragment["fragment_checksum"] = _sha(
        _json({k: v for k, v in fragment.items() if k != "fragment_checksum"})
    )
    validate_canonical_fragment(fragment)
    return fragment


def _fragment(base: str) -> dict[str, Any]:
    cells = [
        SOURCE_TASK,
        "Synthetic governance test",
        "P0",
        "IN_PROGRESS",
        ACTOR,
        "source only",
        "No publication authority",
        "synthetic fixture",
    ]
    event: dict[str, Any] = {
        "schema_version": "task_event.v1",
        "task_id": SOURCE_TASK,
        "event_type": "TASK_REGISTERED",
        "occurred_at": "2026-09-05T00:00:00+00:00",
        "actor": ACTOR,
        "change_id": "synthetic-register",
        "lane_id": None,
        "base_commit": base,
        "previous_state_event_id": None,
        "from_status": None,
        "to_status": "IN_PROGRESS",
        "payload": {"legacy_projection": cells},
        "evidence_refs": [],
    }
    event["event_id"] = _event_id(event)
    return _seal_fragment(
        {
            "schema_version": "arch_005_task_registry_fragment.v1",
            "source_of_truth": "ARCH_005_TASK_REGISTRY",
            "stable_task_identity": {"task_id": SOURCE_TASK, "task_id_sha256": TASK_DIGEST},
            "task_record": {"schema_version": "task_record.v1", "task_id": SOURCE_TASK},
            "events": [event],
            "projection": {
                "legacy_first_eight_cells": cells,
                "terminal": False,
                "canonical_row_sha256": _sha(("|" + "|".join(cells) + "|").encode()),
            },
        }
    )


def _append_fragment(original: dict[str, Any], head: str) -> dict[str, Any]:
    fragment = copy.deepcopy(original)
    event = copy.deepcopy(fragment["events"][-1])
    event.update(
        {
            "event_type": "TASK_UPDATED",
            "occurred_at": "2026-09-06T00:00:00+00:00",
            "change_id": "synthetic-pending-update",
            "base_commit": head,
            "previous_state_event_id": fragment["last_event_id"],
            "from_status": "IN_PROGRESS",
        }
    )
    event["event_id"] = _event_id(event)
    fragment["events"].append(event)
    return _seal_fragment(fragment)


def _fence(repo: Path) -> IntegrationPublicationFence:
    return IntegrationPublicationFence(
        project_root=repo,
        policy_path=repo / POLICIES[0],
        checkout_guard_policy_path=repo / POLICIES[1],
        parallel_control_policy_path=repo / POLICIES[2],
    )


@dataclass(frozen=True)
class SourceCase:
    root: Path
    frozen: str
    head: str
    main: str
    transaction: Path
    original_fragment: dict[str, Any]
    request: dict[str, Any]

    @property
    def destination(self) -> Path:
        return self.root / RUNTIME / self.request["preservation_id"]


def _files(repo: Path) -> list[dict[str, Any]]:
    return [
        {
            "path": path,
            "sha256": _sha((repo / path).read_bytes()),
            "size_bytes": len((repo / path).read_bytes()),
            "git_mode": "100644",
        }
        for path in sorted(SOURCE_PATHS)
    ]


def _source_case(
    tmp_path: Path, *, linked_worktree: bool = False, mixed_scope: bool = False,
    extra_source_files: dict[str, bytes] | None = None,
) -> SourceCase:
    repo = tmp_path / "synthetic-source"
    repo.mkdir()
    _git(repo, "init", "-b", "main")
    _git(repo, "config", "user.email", "source-preservation@example.invalid")
    _git(repo, "config", "user.name", "Synthetic Source Preservation")
    for relative in POLICIES:
        _write(repo, relative, (ROOT / relative).read_bytes())
    _write(repo, ".gitignore", b"outputs/\n")
    _write(repo, "src/a.py", b"VALUE = 1\n")
    _write(repo, "src/raw.bin", b"base\x00binary\n")
    for name, content in (extra_source_files or {}).items():
        _write(repo, name, content)
    seed = _commit(
        repo, (*POLICIES, ".gitignore", "src/a.py", "src/raw.bin", *(extra_source_files or {})),
        "synthetic seed",
    )
    original_fragment = _fragment(seed)
    _write(repo, TASK_PATH, _json(original_fragment) + b"\n")
    frozen = _commit(repo, (TASK_PATH,), "synthetic canonical task")
    if linked_worktree:
        linked = tmp_path / "synthetic-linked-source"
        _git(repo, "worktree", "add", "-b", BRANCH, str(linked), frozen)
        repo = linked
    else:
        _git(repo, "switch", "-c", BRANCH)
    _write(repo, "src/a.py", b"VALUE = 2\n")
    head = _commit(repo, ("src/a.py",), "synthetic source lane")
    _git(repo, "update-ref", "refs/remotes/origin/main", frozen)
    fence = _fence(repo)
    binding = fence.acquire(
        transaction_id="synthetic-old-source",
        task_id=SOURCE_TASK,
        change_id="synthetic-old",
        thread_id="synthetic-source-thread",
        actor=ACTOR,
        frozen_base_sha=frozen,
        lane_head_sha=head,
        expected_main_sha=frozen,
        owned_paths=("src",) if mixed_scope else ("src/a.py", "src/raw.bin"),
        shared_paths=("registry/development_tasks",) if mixed_scope else (TASK_PATH,),
        generator_ids=("canonical-task-source",),
    )
    transaction = Path(str(binding["transaction_path"]))
    if not transaction.is_absolute():
        transaction = repo / transaction
    fence.checkpoint(transaction, phase="TASK_SOURCE_PRE_WRITE", actor=ACTOR)
    # Another mainline actor has advanced the ref; this uses only synthetic Git
    # metadata and never checks out or touches the source lane's tracked files.
    main = (
        _git(
            repo,
            "commit-tree",
            f"{frozen}^{{tree}}",
            "-p",
            frozen,
            content=b"synthetic independent main advance\n",
        )
        .decode()
        .strip()
    )
    _git(repo, "update-ref", "refs/heads/main", main, frozen)
    _git(repo, "update-ref", "refs/remotes/origin/main", main, frozen)
    fence.release(transaction, actor=ACTOR, outcome="failed")
    _write(repo, "src/a.py", b"VALUE = 3\r\n# raw CRLF is intentional\r\n")
    _write(repo, "src/raw.bin", b"changed\x00binary\xff\r\n")
    _write(repo, TASK_PATH, _json(_append_fragment(original_fragment, head)) + b"\n")
    request = {
        "schema_version": "source_preservation_request.v1",
        "preservation_id": "synthetic-preserve-v1",
        "recovery_task_id": RECOVERY_TASK,
        "source_task_id": SOURCE_TASK,
        "owner_instruction_ref": OWNER_REF,
        "actor": ACTOR,
        "thread_id": "synthetic-recovery-thread",
        "source_root": repo.as_posix(),
        "source_common_git_dir": Path(
            _git(repo, "rev-parse", "--path-format=absolute", "--git-common-dir").decode().strip()
        ).as_posix(),
        "source_branch": BRANCH,
        "frozen_base_sha": frozen,
        "source_head_sha": head,
        "observed_main_sha": main,
        "observed_origin_main_sha": main,
        "terminal_transaction": {
            "path": transaction.relative_to(repo).as_posix(),
            "sha256": _sha(transaction.read_bytes()),
            "closeout_sha256": _sha((transaction.parent / "closeout_receipt.json").read_bytes()),
        },
        "files": _files(repo),
    }
    return SourceCase(repo, frozen, head, main, transaction, original_fragment, request)


@pytest.fixture
def controlled_git_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    # Legacy S4D commands inherit ambient state. This is an explicit fixture
    # boundary, not authority; every repository here is a pytest-owned synthetic.
    for key in tuple(os.environ):
        if key.upper().startswith("GIT_"):
            monkeypatch.delenv(key)
    monkeypatch.setenv("GIT_CONFIG_NOSYSTEM", "1")
    monkeypatch.setenv("GIT_CONFIG_GLOBAL", os.devnull)
    monkeypatch.setenv("GIT_OPTIONAL_LOCKS", "0")


@pytest.fixture
def source_case(tmp_path: Path, controlled_git_environment: None) -> SourceCase:
    return _source_case(tmp_path)


def _unchanged_state(case: SourceCase) -> dict[str, Any]:
    return {
        "head": _ref(case.root, "HEAD"),
        "branch": _git(case.root, "branch", "--show-current"),
        "main": _ref(case.root, "main"),
        "origin": _ref(case.root, "origin/main"),
        "index": Path(
            _git(case.root, "rev-parse", "--path-format=absolute", "--git-path", "index")
            .decode()
            .strip()
        ).read_bytes(),
        "files": {path: (case.root / path).read_bytes() for path in SOURCE_PATHS},
        "old_transaction": {
            path.relative_to(case.transaction.parent).as_posix(): path.read_bytes()
            for path in case.transaction.parent.rglob("*")
            if path.is_file()
        },
    }


@pytest.fixture
def unit_engine(monkeypatch: pytest.MonkeyPatch) -> SourcePreservation:
    engine = SourcePreservation(project_root=ROOT, policy_path=DEFAULT_POLICY_PATH)
    # Only the pre-commit implementation identity boundary is substituted. This
    # label cannot be mistaken for evidence that the real implementation is in Git.
    monkeypatch.setattr(
        engine,
        "_implementation_binding",
        lambda: {
            "profile": "SYNTHETIC_IMPLEMENTATION_BINDING_NOT_PRODUCTION",
            "project_root": ROOT.as_posix(),
            "head_sha": "1" * 40,
            "files": [],
        },
    )
    return engine


@pytest.fixture
def mixed_engine(tmp_path, monkeypatch):
    # A real on-disk V2 policy in a disposable trusted-root model. The default
    # policy stays V1. As in unit_engine, only implementation identity is
    # modeled; Git, source authority, leases, raw capture and replay remain real.
    trusted = tmp_path / "mixed-trusted"
    policy_path = trusted / preservation.V2_POLICY_PATH
    policy_path.parent.mkdir(parents=True)
    policy = preservation.safe_load_yaml_text(
        (ROOT / preservation.V2_POLICY_PATH).read_text(encoding="utf-8"),
    )
    policy_path.write_bytes(_json(policy))
    _git(trusted, "init", "-b", "main")
    _git(trusted, "config", "user.email", "mixed-source@example.invalid")
    _git(trusted, "config", "user.name", "Mixed Source Fixture")
    _commit(trusted, (preservation.V2_POLICY_PATH,), "synthetic mixed policy")
    engine = SourcePreservation(project_root=trusted, policy_path=policy_path)
    monkeypatch.setattr(engine, "_implementation_binding", lambda: {
        "profile": "SYNTHETIC_IMPLEMENTATION_BINDING_NOT_PRODUCTION",
        "project_root": trusted.as_posix(), "head_sha": "1" * 40, "files": [],
    })
    return engine


def _mixed_source_case(tmp_path, engine, *, new_task_history=False, large=False):
    modified = [f"src/existing/modify-{index:02}.bin" for index in range(42)] if large else []
    deleted = [f"src/existing/delete-{index:02}.bin" for index in range(16)] if large else []
    case = _source_case(
        tmp_path, mixed_scope=True,
        extra_source_files={name: b"old\x00" + name.encode() for name in (*modified, *deleted)},
    )
    (case.root / "src/raw.bin").unlink()
    for name in modified:
        _write(case.root, name, b"modified\x00\r\n" + name.encode())
    for name in deleted:
        (case.root / name).unlink()
    added = ({f"src/new/add-{index:02}.bin": b"new\x00\r\n" + str(index).encode()
              for index in range(31)} if large else
             {"src/new/nested.bin": b"new\x00raw\r\n", "src/new/empty.bin": b""})
    for name, content in added.items():
        _write(case.root, name, content)
    request = copy.deepcopy(case.request)
    request["schema_version"] = "source_preservation_request.v2"
    request["recovery_task_id"] = engine.policy["recovery_task_id"]
    request["owner_instruction_ref"] = engine.policy["owner_instruction_ref"]
    request["files"] = []
    paths = [*SOURCE_PATHS, *modified, *deleted, *added]
    if new_task_history:
        new_task = "SYNTHETIC_NEW_MIXED_SOURCE_TASK"
        digest = _sha(new_task.encode())
        relative = f"registry/development_tasks/{digest[:2]}/{digest}.yaml"
        with pytest.MonkeyPatch.context() as temporary:
            temporary.setattr(sys.modules[__name__], "SOURCE_TASK", new_task)
            temporary.setattr(sys.modules[__name__], "TASK_DIGEST", digest)
            fragment = _fragment(case.head)
        _write(case.root, relative, _json(fragment) + b"\n")
        paths.append(relative)
    for name in sorted(paths, key=str.casefold):
        mode, oid = engine._tree_entry(case.root, case.head, name, allow_absent=True)
        exists = (case.root / name).exists()
        content = (case.root / name).read_bytes() if exists else None
        request["files"].append({
            "path": name, "base_git_mode": mode, "base_git_oid": oid,
            "change": "DELETE" if not exists else ("ADD" if mode == "000000" else "MODIFY"),
            "git_mode": ("100644" if mode == "000000" else mode) if exists else "000000",
            "sha256": _sha(content) if content is not None else None,
            "size_bytes": len(content) if content is not None else 0,
        })
    return case, request


def test_mixed_preservation_real_92_path_shape_keeps_original_source_and_exact_delta(
    tmp_path, controlled_git_environment, mixed_engine, monkeypatch,
):
    import time

    case, request = _mixed_source_case(tmp_path, mixed_engine, large=True)
    assert len(request["files"]) == 92
    assert {kind: sum(row["change"] == kind for row in request["files"])
            for kind in ("MODIFY", "ADD", "DELETE")} == {"MODIFY": 44, "ADD": 31, "DELETE": 17}
    before = _mixed_working_state(case, request)
    started = time.monotonic()
    actual_git = mixed_engine._git
    timings = {}
    observed = 0
    timing_path = tmp_path / "mixed-source-92-timing.json"

    def timing(status):
        timing_path.write_text(json.dumps({
            "status": status, "path_count": 92, "git_call_count": observed,
            "elapsed_seconds": round(time.monotonic() - started, 3), "git_commands": timings,
        }, indent=2), encoding="utf-8")

    def measured_git(root, *args, **kwargs):
        nonlocal observed
        called = time.monotonic()
        try:
            return actual_git(root, *args, **kwargs)
        finally:
            row = timings.setdefault(args[0], {"count": 0, "seconds": 0.0})
            row["count"] += 1
            row["seconds"] += time.monotonic() - called
            observed += 1
            if observed % 100 == 0:
                timing("RUNNING")

    monkeypatch.setattr(mixed_engine, "_git", measured_git)
    try:
        result = mixed_engine.preserve(request)
        validation = mixed_engine.validate(Path(result["receipt_path"]))
    finally:
        timing("FINISHED_ATTEMPT")
    assert validation["status"] == "PASS" and len(result["snapshot"]["files"]) == 92
    assert _mixed_working_state(case, request) == before
    changed = _git(
        case.root, "diff-tree", "--no-commit-id", "--no-renames", "--name-only", "-r", "-z",
        case.head, result["snapshot"]["commit"], "--", "src", "registry/development_tasks",
    )
    assert {name.decode() for name in changed.split(b"\0") if name} == {
        row["path"] for row in request["files"]
    }
    assert _fence(case.root).guard.replay().active_leases == ()


def test_batched_tree_metadata_matches_git_and_rechecks_each_commit(
    tmp_path, controlled_git_environment, unit_engine,
):
    root = tmp_path / "metadata"
    root.mkdir()
    _git(root, "init", "-q")
    _git(root, "config", "user.name", "Synthetic")
    _git(root, "config", "user.email", "synthetic@example.invalid")
    paths = [f"src/member {index:02}.bin" for index in range(33)]
    (root / "src").mkdir()
    for index, path in enumerate(paths):
        (root / path).write_bytes(bytes([index]))
    _git(root, "add", "--", *paths)
    _git(root, "commit", "-qm", "baseline")
    baseline = _ref(root, "HEAD")
    before = unit_engine._tree_entries(root, baseline, paths)
    for path, (mode, oid) in before.items():
        assert mode == "100644"
        assert _git(root, "cat-file", "blob", oid) == (root / path).read_bytes()
    missing = "src/absent.bin"
    assert unit_engine._tree_entries(root, baseline, [missing], allow_absent=True) == {
        missing: ("000000", "0" * 40),
    }
    for invalid in (missing, "src"):
        with pytest.raises(SourcePreservationError, match="SOURCE_PRESERVATION_UNSUPPORTED_CHANGE"):
            unit_engine._tree_entries(root, baseline, [invalid])
    (root / paths[0]).write_bytes(b"changed")
    _git(root, "add", "--", paths[0])
    _git(root, "commit", "-qm", "next")
    after = unit_engine._tree_entries(root, _ref(root, "HEAD"), paths)
    assert after[paths[0]] != before[paths[0]]
    assert unit_engine._tree_entries(root, baseline, paths) == before


def _mixed_working_state(case, request):
    return {
        "head": _ref(case.root, "HEAD"),
        "refs": _git(case.root, "for-each-ref", "refs/heads", "refs/remotes"),
        "index": (case.root / ".git/index").read_bytes(),
        "files": {row["path"]: ((case.root / row["path"]).read_bytes()
                                 if (case.root / row["path"]).exists() else None)
                  for row in request["files"]},
    }


def test_mixed_preservation_captures_raw_add_modify_delete_and_independently_replays(
    tmp_path, controlled_git_environment, mixed_engine,
):
    case, request = _mixed_source_case(tmp_path, mixed_engine, new_task_history=True)
    before = _mixed_working_state(case, request)
    v1 = SourcePreservation(project_root=ROOT)
    with pytest.raises(SourcePreservationError, match="SOURCE_PRESERVATION_REQUEST"):
        v1.preserve(request)
    result = mixed_engine.preserve(request)
    assert result["schema_version"] == "source_preservation_receipt.v2"
    validation = mixed_engine.validate(Path(result["receipt_path"]))
    assert validation["status"] == "PASS"
    assert validation["schema_version"] == "source_preservation_validation.v2"
    assert _mixed_working_state(case, request) == before
    snapshot = result["snapshot"]["commit"]
    assert not _git(case.root, "ls-tree", snapshot, "--", "src/raw.bin")
    for name in ("src/new/nested.bin", "src/new/empty.bin", "src/a.py"):
        assert _git(case.root, "cat-file", "blob", f"{snapshot}:{name}") == before["files"][name]
    assert result["safety"] == preservation._SAFETY
    assert _fence(case.root).guard.replay().active_leases == ()
    # The retained v2 receipt is not silently interpreted using the v1 policy.
    with pytest.raises(SourcePreservationError, match="SOURCE_PRESERVATION_RECEIPT"):
        v1.validate(Path(result["receipt_path"]))
    run = Path(result["receipt_path"]).parent
    paths = [run / "request.json", run / "receipt.json", *sorted((run / "events").glob("*.json"))]
    original = {path: path.read_bytes() for path in paths}
    for fault in ("deleted-content", "baseline-object"):
        receipt = json.loads(original[run / "receipt.json"])
        requested = json.loads(original[run / "request.json"])
        events = [json.loads(original[path]) for path in paths[2:]]
        assert all(event["schema_version"] == "source_preservation_event.v2" for event in events)
        if fault == "deleted-content":
            row = next(row for row in receipt["snapshot"]["files"] if row["change"] == "DELETE")
            row["blob_content_sha256"] = _sha(b"")
        else:
            for rows in (requested["files"], receipt["source_state_before"]["files"],
                         receipt["source_state_after"]["files"], receipt["snapshot"]["files"]):
                next(row for row in rows if row["path"] == "src/a.py")["base_git_oid"] = "f" * 40
            encoded = preservation._json_bytes(requested)
            (run / "request.json").write_bytes(encoded)
            receipt["request_sha256"] = _sha(encoded)
        events[1]["payload"]["source_state"] = receipt["source_state_before"]
        events[2]["payload"]["snapshot"] = receipt["snapshot"]
        events[4]["payload"]["source_state"] = receipt["source_state_after"]
        previous = None
        for path, event in zip(paths[2:], events, strict=True):
            event["previous_event_id"] = previous
            body = {key: value for key, value in event.items() if key != "event_id"}
            event["event_id"] = _sha(preservation._json_bytes(body))
            previous = event["event_id"]
            path.write_bytes(preservation._json_bytes(event))
        receipt["head_event_id"] = previous
        body = {key: value for key, value in receipt.items() if key != "receipt_sha256"}
        receipt["receipt_sha256"] = _sha(preservation._json_bytes(body))
        (run / "receipt.json").write_bytes(preservation._json_bytes(receipt))
        try:
            with pytest.raises(SourcePreservationError) as error:
                mixed_engine.validate(run / "receipt.json")
            assert error.value.code == "SOURCE_PRESERVATION_RECEIPT", fault
        finally:
            for path, content in original.items():
                path.write_bytes(content)
    assert _mixed_working_state(case, request) == before


@pytest.mark.parametrize("fault", ["baseline", "deleted-returned", "added-disappeared"])
def test_mixed_source_state_drift_is_rejected_before_snapshot(
    tmp_path, controlled_git_environment, mixed_engine, fault,
):
    case, request = _mixed_source_case(tmp_path, mixed_engine)
    if fault == "baseline":
        row = next(row for row in request["files"] if row["path"] == "src/a.py")
        row["base_git_oid"] = "f" * 40
    elif fault == "deleted-returned":
        _write(case.root, "src/raw.bin", b"unexpected return")
    else:
        (case.root / "src/new/nested.bin").unlink()
    with pytest.raises(SourcePreservationError) as error:
        mixed_engine.inspect_migration_source(request)
    assert error.value.code in {"SOURCE_PRESERVATION_DRIFT", "SOURCE_PRESERVATION_DIRTY_SCOPE"}
    assert not case.destination.exists()
    assert not _git(case.root, "for-each-ref", "refs/aits/source-preservation/")


@pytest.mark.parametrize(
    "fault", ["schema", "deletion-bytes", "added-baseline", "canonical-delete",
              "canonical-case", "added-executable", "unknown-change", "file-budget"],
)
def test_mixed_request_requires_version_and_explicit_change_semantics(
    tmp_path, controlled_git_environment, mixed_engine, fault,
):
    case, request = _mixed_source_case(tmp_path, mixed_engine)
    deleted = next(row for row in request["files"] if row["change"] == "DELETE")
    if fault == "schema":
        request["schema_version"] = "source_preservation_request.v1"
    elif fault == "deletion-bytes":
        deleted["sha256"] = _sha(b"")
    elif fault == "added-baseline":
        next(row for row in request["files"] if row["change"] == "ADD")["base_git_oid"] = "f" * 40
    elif fault == "added-executable":
        next(row for row in request["files"] if row["change"] == "ADD")["git_mode"] = "100755"
    elif fault == "unknown-change":
        deleted["change"] = []
    elif fault == "file-budget":
        request["files"] = [copy.deepcopy(deleted) for _ in range(93)]
    else:
        task = next(row for row in request["files"] if row["path"] == TASK_PATH)
        task.update(change="DELETE", sha256=None, size_bytes=0, git_mode="000000")
        if fault == "canonical-case":
            task["path"] = task["path"].upper()
    with pytest.raises(SourcePreservationError) as error:
        mixed_engine.preserve(request)
    assert error.value.code in {"SOURCE_PRESERVATION_REQUEST", "SOURCE_PRESERVATION_HISTORY"}
    assert not case.destination.exists()


@pytest.mark.parametrize("fault", ["v1-at-v2", "v2-at-v1", "arbitrary-path"])
def test_policy_version_requires_its_exact_reviewed_locator(tmp_path, fault):
    v1 = SourcePreservation(ROOT)
    v2 = SourcePreservation(ROOT, Path(preservation.V2_POLICY_PATH))
    assert (v1.protocol_version, v1.policy["max_files"]) == ("v1", 64)
    assert (v2.protocol_version, v2.policy["max_files"]) == ("v2", 92)
    source = preservation.V2_POLICY_PATH if fault == "v2-at-v1" else preservation.POLICY_PATH
    target = {
        "v1-at-v2": preservation.V2_POLICY_PATH,
        "v2-at-v1": preservation.POLICY_PATH,
        "arbitrary-path": "config/architecture/arbitrary.yaml",
    }[fault]
    path = tmp_path / target
    path.parent.mkdir(parents=True)
    path.write_bytes((ROOT / source).read_bytes())
    with pytest.raises(SourcePreservationError, match="SOURCE_PRESERVATION_POLICY"):
        SourcePreservation(tmp_path, path)


def test_preservation_policy_invalid_schema_is_typed_before_execution(tmp_path):
    path = tmp_path / preservation.POLICY_PATH
    path.parent.mkdir(parents=True)
    policy = preservation.safe_load_yaml_text(DEFAULT_POLICY_PATH.read_text(encoding="utf-8"))
    policy["schema_version"] = []
    path.write_bytes(_json(policy))
    with pytest.raises(SourcePreservationError, match="SOURCE_PRESERVATION_POLICY"):
        SourcePreservation(project_root=tmp_path, policy_path=path)


def _receipt_path(case: SourceCase, result: dict[str, Any]) -> Path:
    path = Path(result["receipt_path"])
    return path if path.is_absolute() else case.root / path


def _tree_oracle_repository(tmp_path: Path) -> tuple[Path, str, SourcePreservation]:
    root = tmp_path / "tree-oracle"
    root.mkdir()
    _git(root, "init", "-b", "main")
    _git(root, "config", "user.email", "tree-oracle@example.invalid")
    _git(root, "config", "user.name", "Tree Oracle")
    for name in ("src/a.py", "src/raw.bin", "untouched.bin"):
        _write(root, name, (name + "\r\n").encode() + b"\x00")
    head = _commit(root, ("src/a.py", "src/raw.bin", "untouched.bin"), "tree baseline")
    return root, head, SourcePreservation(project_root=ROOT)


@pytest.mark.parametrize("variant", ["mixed", "remove-directory", "remove-all", "empty-add"])
def test_mixed_tree_oracle_matches_real_git_without_object_or_index_writes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, variant: str,
) -> None:
    root, head, engine = _tree_oracle_repository(tmp_path)
    tree = _git(root, "rev-parse", head + "^{tree}").decode().strip()
    # Git's private index is only the independent expected-result producer.
    # The implementation under test must not invoke it or create any objects.
    index = tmp_path / "oracle.index"
    engine._git(root, "read-tree", head, index=index)
    deleted = ["src/raw.bin"]
    additions = {"src/a.py": b"changed\r\n\x00", "a.c": b"file", "a/x.bin": b"nested"}
    if variant in {"remove-directory", "remove-all"}:
        deleted = ["src/a.py", "src/raw.bin"]
        if variant == "remove-all":
            deleted.append("untouched.bin")
        additions = {}
    elif variant == "empty-add":
        deleted, additions = [], {"new/empty.bin": b""}
    changes = {name: ("000000", "0" * 40) for name in deleted}
    for name in deleted:
        engine._git(root, "update-index", "--force-remove", "--", name, index=index)
    for name, content in additions.items():
        oid = engine._git(
            root, "hash-object", "-w", "--stdin", "--no-filters", content=content,
        ).decode().strip()
        engine._git(root, "update-index", "--add", "--cacheinfo", "100644", oid, name, index=index)
        changes[name] = ("100644", oid)
    expected = engine._git(root, "write-tree", index=index).decode().strip()
    before = (_git(root, "count-objects", "-v"), (root / ".git/index").read_bytes(),
              _git(root, "show-ref"), index.read_bytes())
    actual_git = engine._git
    reads = []

    def metadata_only(repo, *args, **kwargs):
        assert args[:2] == ("cat-file", "tree") and not kwargs
        reads.append(args[2])
        return actual_git(repo, *args)

    monkeypatch.setattr(engine, "_git", metadata_only)
    assert engine._expected_tree(root, tree, changes, allow_path_changes=True) == expected
    assert reads and len(reads) == len(set(reads))
    assert before == (_git(root, "count-objects", "-v"), (root / ".git/index").read_bytes(),
                      _git(root, "show-ref"), index.read_bytes())


@pytest.mark.parametrize(
    "fault", ["v1-add", "v1-delete", "absent-delete", "delete-blob", "mode", "ancestor", "overlap"],
)
def test_mixed_tree_oracle_rejects_unsupported_changes_and_keeps_v1_closed(tmp_path, fault):
    root, head, engine = _tree_oracle_repository(tmp_path)
    tree = _git(root, "rev-parse", head + "^{tree}").decode().strip()
    oid = _git(root, "rev-parse", head + ":src/a.py").decode().strip()
    changes = {
        "v1-add": {"new.bin": ("100644", oid)},
        "v1-delete": {"src/a.py": ("000000", "0" * 40)},
        "absent-delete": {"absent.bin": ("000000", "0" * 40)},
        "delete-blob": {"src/a.py": ("000000", oid)},
        "mode": {"src/a.py": ("100755", oid)},
        "ancestor": {"untouched.bin/child": ("100644", oid)},
        "overlap": {"new.bin": ("100644", oid), "new.bin/child": ("100644", oid)},
    }[fault]
    with pytest.raises(SourcePreservationError) as error:
        engine._expected_tree(root, tree, changes, allow_path_changes=not fault.startswith("v1-"))
    assert error.value.code == "SOURCE_PRESERVATION_RECEIPT"


def _assert_snapshot(case: SourceCase, engine: SourcePreservation) -> tuple[Path, dict[str, Any]]:
    before = _unchanged_state(case)
    result = engine.preserve(case.request)
    receipt_path = _receipt_path(case, result)
    validation = engine.validate(receipt_path)
    assert validation["status"] == "PASS"
    assert validation["schema_version"] == "source_preservation_validation.v1"
    assert _unchanged_state(case) == before
    snapshot = validation["snapshot_commit"]
    ref = f"refs/aits/source-preservation/{case.request['preservation_id']}"
    assert validation["ref"] == ref
    assert _ref(case.root, ref) == snapshot
    assert _git(case.root, "rev-list", "--parents", "-n", "1", snapshot).decode().split() == [
        snapshot,
        case.head,
    ]
    changed = (
        _git(case.root, "diff", "--name-only", "-z", case.head, snapshot, "--", ".", EXCLUSION)
        .decode()
        .strip("\0")
        .split("\0")
    )
    assert set(changed) == set(SOURCE_PATHS)
    for row in case.request["files"]:
        content = _git(case.root, "cat-file", "blob", f"{snapshot}:{row['path']}")
        assert content == before["files"][row["path"]]
        assert _sha(content) == row["sha256"]
    fragment = json.loads(_git(case.root, "cat-file", "blob", f"{snapshot}:{TASK_PATH}"))
    validate_canonical_fragment(fragment)
    assert (
        fragment["events"][: len(case.original_fragment["events"])]
        == case.original_fragment["events"]
    )
    assert result["implementation"] == engine._implementation_binding()
    assert (
        result["safety"]
        == validation["safety"]
        == {
            "profile": "RAW_BYTES_SOURCE_ONLY_UNVALIDATED",
            "task_source_write_allowed": False,
            "generator_allowed": False,
            "formal_validation_allowed": False,
            "full_allowed": False,
            "main_ff_allowed": False,
            "push_allowed": False,
            "research_allowed": False,
            "data_action_allowed": False,
            "trading_allowed": False,
            "production_effect": "none",
            "broker_action": "none",
        }
    )
    return receipt_path, validation


def test_raw_snapshot_preserves_crlf_binary_canonical_prefix_index_and_source_refs(
    source_case: SourceCase,
    unit_engine: SourcePreservation,
) -> None:
    _assert_snapshot(source_case, unit_engine)


def test_linked_worktree_preserves_shared_git_identity_and_its_real_private_index(
    tmp_path: Path,
    controlled_git_environment: None,
    unit_engine: SourcePreservation,
) -> None:
    # The complete synthetic repository and its linked worktree are owned by
    # pytest tmp_path; neither is a real project worktree or a persistent lane.
    case = _source_case(tmp_path, linked_worktree=True)
    assert (case.root / ".git").is_file()
    assert Path(case.request["source_common_git_dir"]) != case.root / ".git"
    _assert_snapshot(case, unit_engine)


def test_successful_replay_is_idempotent_and_does_not_make_another_commit_or_lease(
    source_case: SourceCase,
    unit_engine: SourcePreservation,
) -> None:
    path, original = _assert_snapshot(source_case, unit_engine)
    before = {
        p.relative_to(source_case.destination).as_posix(): p.read_bytes()
        for p in source_case.destination.rglob("*")
        if p.is_file()
    }
    lease_events = _fence(source_case.root).guard.replay().event_count
    replayed = unit_engine.preserve(copy.deepcopy(source_case.request))
    assert _receipt_path(source_case, replayed) == path
    assert unit_engine.validate(path) == original
    assert {
        p.relative_to(source_case.destination).as_posix(): p.read_bytes()
        for p in source_case.destination.rglob("*")
        if p.is_file()
    } == before
    assert _fence(source_case.root).guard.replay().event_count == lease_events


def test_receipt_validation_does_not_require_future_source_bytes_to_remain_frozen(
    source_case: SourceCase,
    unit_engine: SourcePreservation,
) -> None:
    path, original = _assert_snapshot(source_case, unit_engine)
    (source_case.root / "src/a.py").write_bytes(b"later uncommitted work\r\n")
    assert unit_engine.validate(path) == original


@pytest.mark.parametrize(
    "tamper", ["schema", "extra", "missing", "empty_files", "duplicate", "boolean_size"]
)
def test_request_shape_is_strict_before_any_source_snapshot(
    source_case: SourceCase,
    unit_engine: SourcePreservation,
    tamper: str,
) -> None:
    request = copy.deepcopy(source_case.request)
    if tamper == "schema":
        request["schema_version"] = "unreviewed_request.v1"
    elif tamper == "extra":
        request["allow_dirty"] = True
    elif tamper == "missing":
        del request["owner_instruction_ref"]
    elif tamper == "empty_files":
        request["files"] = []
    elif tamper == "duplicate":
        request["files"].append(copy.deepcopy(request["files"][0]))
    else:
        request["files"][0]["size_bytes"] = True
    before = _unchanged_state(source_case)
    with pytest.raises(SourcePreservationError) as error:
        unit_engine.preserve(request)
    assert error.value.code == "SOURCE_PRESERVATION_REQUEST"
    assert _unchanged_state(source_case) == before
    assert not (source_case.destination / "receipt.json").exists()


@pytest.mark.parametrize(
    "field",
    [
        "source_branch",
        "source_head_sha",
        "source_common_git_dir",
        "observed_main_sha",
        "observed_origin_main_sha",
    ],
)
def test_wrong_source_identity_is_rejected_without_rebinding_it(
    source_case: SourceCase,
    unit_engine: SourcePreservation,
    field: str,
) -> None:
    request = copy.deepcopy(source_case.request)
    replacements = {
        "source_branch": "codex/wrong-source",
        "source_head_sha": source_case.frozen,
        "source_common_git_dir": (source_case.root.parent / "wrong.git").as_posix(),
        "observed_main_sha": source_case.frozen,
        "observed_origin_main_sha": source_case.frozen,
    }
    request[field] = replacements[field]
    before = _unchanged_state(source_case)
    with pytest.raises(SourcePreservationError) as error:
        unit_engine.preserve(request)
    assert error.value.code == (
        "SOURCE_PRESERVATION_IDENTITY"
        if field.startswith("observed_")
        else "SOURCE_PRESERVATION_TERMINAL"
    )
    assert _unchanged_state(source_case) == before


@pytest.mark.parametrize("tamper", ["source_task", "transaction_sha", "closeout_sha", "event"])
def test_terminal_source_transaction_and_event_chain_are_not_replaceable(
    source_case: SourceCase,
    unit_engine: SourcePreservation,
    tamper: str,
) -> None:
    request = copy.deepcopy(source_case.request)
    if tamper == "source_task":
        request["source_task_id"] = "WRONG_SYNTHETIC_TASK"
    elif tamper == "transaction_sha":
        request["terminal_transaction"]["sha256"] = "0" * 64
    elif tamper == "closeout_sha":
        request["terminal_transaction"]["closeout_sha256"] = "0" * 64
    else:
        event = sorted((source_case.transaction.parent / "events").glob("*.json"))[0]
        value = json.loads(event.read_bytes())
        value["payload"]["observed_head"] = "0" * 40
        event.write_bytes(_json(value))
    with pytest.raises(SourcePreservationError) as error:
        unit_engine.preserve(request)
    assert error.value.code == "SOURCE_PRESERVATION_TERMINAL"
    assert not (source_case.destination / "receipt.json").exists()


@pytest.mark.parametrize("tamper", ["staged", "untracked", "delete", "rename", "index_mode"])
def test_v1_rejects_non_unstaged_regular_modification_states(
    source_case: SourceCase,
    unit_engine: SourcePreservation,
    tamper: str,
) -> None:
    repo = source_case.root
    if tamper == "staged":
        _git(repo, "add", "--", ":(literal)src/a.py")
    elif tamper == "untracked":
        _write(repo, "src/untracked.py", b"must not be silently captured\n")
    elif tamper == "delete":
        (repo / "src/a.py").unlink()
    elif tamper == "rename":
        (repo / "src/a.py").rename(repo / "src/renamed.py")
    else:
        _git(repo, "update-index", "--chmod=+x", "--", "src/a.py")
    index_before = (repo / ".git/index").read_bytes()
    with pytest.raises(SourcePreservationError) as error:
        unit_engine.preserve(source_case.request)
    assert error.value.code == "SOURCE_PRESERVATION_UNSUPPORTED_CHANGE"
    assert (repo / ".git/index").read_bytes() == index_before
    assert _ref(repo, "HEAD") == source_case.head
    assert _ref(repo, "main") == _ref(repo, "origin/main") == source_case.main


def test_canonical_history_rewrite_is_rejected_even_when_all_hashes_are_self_consistent(
    source_case: SourceCase,
    unit_engine: SourcePreservation,
) -> None:
    fragment = json.loads((source_case.root / TASK_PATH).read_bytes())
    fragment["events"][0]["base_commit"] = source_case.head
    fragment["events"][0]["event_id"] = _event_id(fragment["events"][0])
    fragment["events"][1]["previous_state_event_id"] = fragment["events"][0]["event_id"]
    fragment["events"][1]["event_id"] = _event_id(fragment["events"][1])
    _write(source_case.root, TASK_PATH, _json(_seal_fragment(fragment)) + b"\n")
    request = copy.deepcopy(source_case.request)
    request["files"] = _files(source_case.root)
    with pytest.raises(SourcePreservationError) as error:
        unit_engine.preserve(request)
    assert error.value.code == "SOURCE_PRESERVATION_HISTORY"
    assert _ref(source_case.root, "HEAD") == source_case.head


def test_unsupported_clean_filter_is_rejected_before_any_transform_can_run(
    source_case: SourceCase,
    unit_engine: SourcePreservation,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    attributes = source_case.root / ".git/info/attributes"
    attributes.write_bytes(b"src/a.py filter=synthetic-forbidden-filter\n")
    _git(
        source_case.root,
        "config",
        "filter.synthetic-forbidden-filter.clean",
        "synthetic-filter-must-never-run",
    )
    before = _unchanged_state(source_case)
    actual_run = subprocess.run
    commands: list[list[str]] = []

    def audited_run(command: list[str], *args: Any, **kwargs: Any) -> Any:
        commands.append([str(part) for part in command])
        assert "status" not in command, "a Git status could dispatch the configured clean filter"
        return actual_run(command, *args, **kwargs)

    monkeypatch.setattr(subprocess, "run", audited_run)
    with pytest.raises(SourcePreservationError) as error:
        unit_engine.preserve(source_case.request)
    assert error.value.code == "SOURCE_PRESERVATION_FILTER"
    assert any("config" in command or "check-attr" in command for command in commands)
    monkeypatch.setattr(subprocess, "run", actual_run)
    assert _unchanged_state(source_case) == before


def test_existing_preservation_ref_is_never_overwritten(
    source_case: SourceCase,
    unit_engine: SourcePreservation,
) -> None:
    ref = f"refs/aits/source-preservation/{source_case.request['preservation_id']}"
    _git(source_case.root, "update-ref", ref, source_case.head)
    with pytest.raises(SourcePreservationError) as error:
        unit_engine.preserve(source_case.request)
    assert error.value.code == "SOURCE_PRESERVATION_REF_EXISTS"
    assert _ref(source_case.root, ref) == source_case.head


def test_same_id_with_different_request_cannot_reuse_successful_receipt(
    source_case: SourceCase,
    unit_engine: SourcePreservation,
) -> None:
    path, _ = _assert_snapshot(source_case, unit_engine)
    prior = path.read_bytes()
    request = copy.deepcopy(source_case.request)
    request["thread_id"] = "different-request-identity"
    with pytest.raises(SourcePreservationError) as error:
        unit_engine.preserve(request)
    assert error.value.code == "SOURCE_PRESERVATION_REF_EXISTS"
    assert path.read_bytes() == prior


@pytest.mark.parametrize("field", ["recovery_task_id", "owner_instruction_ref"])
def test_owner_recovery_binding_is_exact(
    source_case: SourceCase,
    unit_engine: SourcePreservation,
    field: str,
) -> None:
    request = copy.deepcopy(source_case.request)
    request[field] = "not-the-reviewed-owner-binding"
    with pytest.raises(SourcePreservationError, match="SOURCE_PRESERVATION_REQUEST"):
        unit_engine.preserve(request)
    assert not source_case.destination.exists()


@pytest.mark.parametrize("tamper", ["missing", "extra"])
def test_declared_files_must_equal_the_whole_attributed_dirty_set(
    source_case: SourceCase,
    unit_engine: SourcePreservation,
    tamper: str,
) -> None:
    request = copy.deepcopy(source_case.request)
    if tamper == "missing":
        request["files"].pop()
    else:
        request["files"].append(
            {"path": "src/not-owned.py", "sha256": "0" * 64, "size_bytes": 0, "git_mode": "100644"}
        )
        request["files"].sort(key=lambda row: row["path"])
    with pytest.raises(SourcePreservationError, match="SOURCE_PRESERVATION_DIRTY_SCOPE"):
        unit_engine.preserve(request)


@pytest.mark.parametrize(
    "path", [EXCLUDED, "../escaped.py", "src/../escaped.py", "src\\a.py", ".git/index"]
)
def test_exclusions_and_path_escapes_never_become_capture_members(
    source_case: SourceCase,
    unit_engine: SourcePreservation,
    path: str,
) -> None:
    request = copy.deepcopy(source_case.request)
    request["files"] = [{"path": path, "sha256": "0" * 64, "size_bytes": 0, "git_mode": "100644"}]
    with pytest.raises(SourcePreservationError, match="SOURCE_PRESERVATION_PATH"):
        unit_engine.preserve(request)
    assert not source_case.destination.exists()


@pytest.mark.parametrize("field", ["sha256", "size_bytes"])
def test_source_bytes_are_verified_against_request_not_silently_recaptured(
    source_case: SourceCase,
    unit_engine: SourcePreservation,
    field: str,
) -> None:
    request = copy.deepcopy(source_case.request)
    request["files"][0][field] = "0" * 64 if field == "sha256" else 1
    with pytest.raises(SourcePreservationError, match="SOURCE_PRESERVATION_DRIFT"):
        unit_engine.preserve(request)


def test_excluded_file_is_poisoned_and_every_wide_git_call_excludes_it(
    source_case: SourceCase,
    unit_engine: SourcePreservation,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    poison = source_case.root / EXCLUDED
    # This is a wholly synthetic path, never the actual excluded owner document.
    _write(source_case.root, EXCLUDED, b"synthetic content must never be opened\n")
    actual_open, actual_run = Path.open, subprocess.run
    commands: list[list[str]] = []

    def poisoned_open(path: Path, *args: Any, **kwargs: Any) -> Any:
        assert path.resolve() != poison.resolve(), "excluded synthetic content was opened"
        return actual_open(path, *args, **kwargs)

    def audited_run(command: list[str], *args: Any, **kwargs: Any) -> Any:
        tokens = [str(part) for part in command]
        if tokens and tokens[0] == "git":
            commands.append(tokens)
            if any(verb in tokens for verb in ("status", "diff", "diff-tree")):
                assert any(
                    EXCLUDED in token and "literal" in token and "exclude" in token
                    for token in tokens
                )
            if "cat-file" in tokens:
                assert not any(EXCLUDED in token for token in tokens)
        return actual_run(command, *args, **kwargs)

    monkeypatch.setattr(Path, "open", poisoned_open)
    monkeypatch.setattr(subprocess, "run", audited_run)
    _assert_snapshot(source_case, unit_engine)
    assert any("status" in command for command in commands)
    assert any("diff-tree" in command for command in commands)


def test_source_only_receipt_does_not_relax_terminal_fence_ancestry_or_clean_planner(
    source_case: SourceCase,
    unit_engine: SourcePreservation,
) -> None:
    receipt, _ = _assert_snapshot(source_case, unit_engine)
    fence = _fence(source_case.root)
    with pytest.raises(PublicationFenceError, match="PUBLICATION_TRANSACTION_TERMINAL"):
        fence.validate(source_case.transaction)
    with pytest.raises(PublicationFenceError, match="PUBLICATION_ANCESTRY_INVALID"):
        fence.acquire(
            transaction_id="synthetic-ordinary-retry",
            task_id=SOURCE_TASK,
            change_id="synthetic-retry",
            thread_id="synthetic-retry",
            actor=ACTOR,
            frozen_base_sha=source_case.frozen,
            lane_head_sha=source_case.head,
            expected_main_sha=source_case.main,
            owned_paths=("src/a.py", "src/raw.bin"),
            shared_paths=(TASK_PATH,),
            generator_ids=("canonical-task-source",),
        )
    with pytest.raises(PublicationFenceError):
        fence.validate(receipt)
    manifest = {
        "schema_version": "change_manifest.v1",
        "change_id": "synthetic-drift",
        "task_id": SOURCE_TASK,
        "lane_role": "COORDINATOR",
        "base_commit": source_case.frozen,
        "owner": ACTOR,
        "production_effect": "none",
        "owned_paths": ["src/a.py", "src/raw.bin"],
        "shared_paths": [TASK_PATH],
        "module_ids": ["synthetic-source"],
        "contract_claims": [],
        "required_validation_tiers": ["focused"],
    }
    plan = build_integration_revalidation_plan(
        repository=source_case.root,
        frozen_base=source_case.frozen,
        lane_head=source_case.head,
        latest_main=source_case.main,
        manifest=manifest,
        policy=IntegrationRevalidationPolicy((EXCLUDED,), (), (), ("full",)),
    )
    assert plan["decision"] == "BLOCKED"
    assert any(row["code"] == "REPOSITORY_DIRTY" for row in plan["blockers"])


@pytest.mark.parametrize("tamper", ["receipt", "event", "request", "ref", "archived_terminal"])
def test_independent_validation_rejects_tampered_retained_evidence(
    source_case: SourceCase,
    unit_engine: SourcePreservation,
    tamper: str,
) -> None:
    path, validation = _assert_snapshot(source_case, unit_engine)
    expected_code = "SOURCE_PRESERVATION_RECEIPT"
    if tamper == "receipt":
        value = json.loads(path.read_bytes())
        value["safety"]["full_allowed"] = True
        path.write_bytes(_json(value))
    elif tamper == "event":
        event = path.parent / "events/0002_CAPTURED.json"
        value = json.loads(event.read_bytes())
        value["payload"]["source_state"]["head"] = "0" * 40
        event.write_bytes(_json(value))
    elif tamper == "request":
        request = path.parent / "request.json"
        value = json.loads(request.read_bytes())
        value["thread_id"] = "tampered-recovery-thread"
        request.write_bytes(_json(value))
    elif tamper == "ref":
        _git(source_case.root, "update-ref", validation["ref"], source_case.head)
    else:
        archive = path.parent / "terminal/closeout_receipt.json"
        archive.write_bytes(archive.read_bytes() + b" ")
        expected_code = "SOURCE_PRESERVATION_TERMINAL"
    with pytest.raises(SourcePreservationError) as error:
        unit_engine.validate(path)
    assert error.value.code == expected_code


@pytest.mark.parametrize(
    "tamper", ["source_identity", "released_unrelated_lease", "full_authority"]
)
def test_self_consistent_checksums_cannot_replace_bound_source_or_lease_authority(
    source_case: SourceCase,
    unit_engine: SourcePreservation,
    tamper: str,
) -> None:
    path, _ = _assert_snapshot(source_case, unit_engine)
    receipt = json.loads(path.read_bytes())
    event_paths = sorted((path.parent / "events").glob("*.json"))
    events = [json.loads(event.read_bytes()) for event in event_paths]
    if tamper == "source_identity":
        for field in ("source_state_before", "source_state_after"):
            receipt[field]["head"] = source_case.frozen
        events[1]["payload"]["source_state"]["head"] = source_case.frozen
        events[4]["payload"]["source_state"]["head"] = source_case.frozen
    elif tamper == "released_unrelated_lease":
        # A genuine RELEASED lease is insufficient: this is the earlier failed
        # publication lease, not the exact source-only acquisition intent.
        lease_id = json.loads(source_case.transaction.read_bytes())["lease_id"]
        receipt["lease_id"] = lease_id
        events[0]["payload"]["lease_id"] = lease_id
        events[5]["payload"]["lease_id"] = lease_id
    else:
        receipt["safety"]["full_allowed"] = True
    previous = None
    for event_path, event in zip(event_paths, events, strict=True):
        event["previous_event_id"] = previous
        body = {key: value for key, value in event.items() if key != "event_id"}
        event["event_id"] = _sha(_json(body) + b"\n")
        previous = event["event_id"]
        event_path.write_bytes(_json(event) + b"\n")
    receipt["head_event_id"] = previous
    body = {key: value for key, value in receipt.items() if key != "receipt_sha256"}
    receipt["receipt_sha256"] = _sha(_json(body) + b"\n")
    path.write_bytes(_json(receipt) + b"\n")
    with pytest.raises(SourcePreservationError) as error:
        unit_engine.validate(path)
    assert error.value.code == (
        "SOURCE_PRESERVATION_LEASE"
        if tamper == "released_unrelated_lease"
        else "SOURCE_PRESERVATION_RECEIPT"
    )


@pytest.mark.parametrize("key", ["GIT_INDEX_FILE", "GIT_OBJECT_DIRECTORY", "GIT_REPLACE_REF_BASE"])
def test_ambient_git_redirection_is_rejected_before_legacy_guard_dispatch(
    source_case: SourceCase,
    unit_engine: SourcePreservation,
    monkeypatch: pytest.MonkeyPatch,
    key: str,
) -> None:
    monkeypatch.setenv(key, str(source_case.root / ".git/synthetic-override"))
    with pytest.raises(SourcePreservationError, match="SOURCE_PRESERVATION_ENVIRONMENT"):
        unit_engine.preserve(source_case.request)
    assert not source_case.destination.exists()


@pytest.mark.parametrize("phase", ["CAPTURED", "REF_CREATED"])
def test_partial_failure_retains_objects_and_ref_without_false_success_or_retry(
    source_case: SourceCase,
    unit_engine: SourcePreservation,
    monkeypatch: pytest.MonkeyPatch,
    phase: str,
) -> None:
    actual_event = unit_engine._event
    before = _unchanged_state(source_case)

    def fail_after_phase(
        run: Path, events: list[dict[str, Any]], reached: str, payload: dict[str, Any]
    ) -> None:
        actual_event(run, events, reached, payload)
        if reached == phase:
            raise OSError("synthetic failure after durable phase")

    monkeypatch.setattr(unit_engine, "_event", fail_after_phase)
    with pytest.raises(SourcePreservationError, match="SOURCE_PRESERVATION_PARTIAL"):
        unit_engine.preserve(source_case.request)
    failure = source_case.destination / "failure.json"
    assert failure.is_file()
    prior = failure.read_bytes()
    assert json.loads(prior)["completed_phases"][-1] == phase
    assert not (source_case.destination / "receipt.json").exists()
    assert _unchanged_state(source_case) == before
    ref = f"refs/aits/source-preservation/{source_case.request['preservation_id']}"
    if phase == "REF_CREATED":
        snapshot = _ref(source_case.root, ref)
        assert _git(
            source_case.root, "rev-list", "--parents", "-n", "1", snapshot
        ).decode().split() == [snapshot, source_case.head]
    with pytest.raises(SourcePreservationError, match="SOURCE_PRESERVATION_PARTIAL"):
        unit_engine.preserve(source_case.request)
    assert failure.read_bytes() == prior


def test_failure_after_receipt_write_can_never_be_promoted_by_later_replay(
    source_case: SourceCase,
    unit_engine: SourcePreservation,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    actual_validate = unit_engine.validate

    def fail_postwrite(path: Path) -> dict[str, Any]:
        assert path.is_file()
        raise SourcePreservationError(
            "SOURCE_PRESERVATION_RECEIPT", "synthetic postwrite verification failure"
        )

    monkeypatch.setattr(unit_engine, "validate", fail_postwrite)
    with pytest.raises(SourcePreservationError, match="SOURCE_PRESERVATION_RECEIPT"):
        unit_engine.preserve(source_case.request)
    path = source_case.destination / "receipt.json"
    assert path.is_file()
    assert (source_case.destination / "failure.json").is_file()
    original = path.read_bytes()
    monkeypatch.setattr(unit_engine, "validate", actual_validate)
    with pytest.raises(SourcePreservationError, match="SOURCE_PRESERVATION_PARTIAL"):
        unit_engine.validate(path)
    with pytest.raises(SourcePreservationError, match="SOURCE_PRESERVATION_PARTIAL"):
        unit_engine.preserve(source_case.request)
    assert path.read_bytes() == original


def test_concurrent_same_id_loser_cannot_write_failure_into_winners_run(
    source_case: SourceCase,
    unit_engine: SourcePreservation,
    monkeypatch: pytest.MonkeyPatch,
    request: pytest.FixtureRequest,
) -> None:
    actual_acquire = CheckoutLeaseGuard.acquire
    actual_event = unit_engine._event
    entered = Barrier(2, timeout=45)
    winner_created, loser_finished = Event(), Event()
    decisions: list[str] = []
    guard_error_codes: list[str] = []

    def synchronized_acquire(guard: CheckoutLeaseGuard, *args: Any, **kwargs: Any) -> Any:
        try:
            entered.wait()
            decision, handle = actual_acquire(guard, *args, **kwargs)
        except CheckoutGuardError as exc:
            guard_error_codes.append(exc.code)
            guard_error_codes.append(f"CheckoutGuardError.message:{exc.message}")
            guard_error_codes.append("".join(traceback.format_exception(exc)))
            # The OS arbiter can reject a concurrent live-handle acquisition
            # through this typed error rather than a BLOCKED decision. Preserve
            # the actual denial; delay only its delivery until the winner owns
            # its run directory, exercising the loser-pollution boundary.
            assert exc.code == "LEASE_ARBITER_BUSY"
            decisions.append("BLOCKED")
            assert winner_created.wait(45)
            raise
        except BaseException as exc:
            guard_error_codes.append(f"{type(exc).__name__}:{getattr(exc, 'code', '')}:{exc}")
            raise
        decisions.append(decision.status)
        if decision.status != "PASS":
            assert winner_created.wait(45)
        return decision, handle

    def hold_winner(
        run: Path, events: list[dict[str, Any]], phase: str, payload: dict[str, Any]
    ) -> None:
        actual_event(run, events, phase, payload)
        if phase == "ACQUIRED":
            winner_created.set()
            assert loser_finished.wait(45)

    def dispatch(index: int) -> dict[str, Any] | str:
        request = copy.deepcopy(source_case.request)
        request["thread_id"] = f"synthetic-contender-{index}"
        try:
            return unit_engine.preserve(request)
        except SourcePreservationError as exc:
            guard_error_codes.append(f"dispatch:{exc.code}:{exc.message}")
            # Preserve the original synthetic exception context even when the
            # production boundary deliberately emits only a typed safe message.
            guard_error_codes.append("".join(traceback.format_exception(exc)))
            loser_finished.set()
            return exc.code

    monkeypatch.setattr(CheckoutLeaseGuard, "acquire", synchronized_acquire)
    monkeypatch.setattr(unit_engine, "_event", hold_winner)
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(dispatch, (1, 2)))
    # JUnit properties retain complete synthetic causality without pytest's
    # shortened list repr. This records diagnostics, not execution authority.
    request.node.user_properties.extend(
        [
            ("diagnostic_profile", "SYNTHETIC_GIT_ONLY"),
            ("guard_decision_statuses", json.dumps(decisions)),
            ("guard_exception_chains", json.dumps(guard_error_codes, ensure_ascii=False)),
        ]
    )
    assert sum(isinstance(result, dict) for result in results) == 1
    assert [result for result in results if isinstance(result, str)] == [
        "SOURCE_PRESERVATION_LEASE"
    ], guard_error_codes
    assert sorted(decisions) == ["BLOCKED", "PASS"]
    assert not (source_case.destination / "failure.json").exists()
    result = next(result for result in results if isinstance(result, dict))
    assert unit_engine.validate(_receipt_path(source_case, result))["status"] == "PASS"


def test_pre_ref_capture_drift_is_not_rolled_back_or_silently_rebound(
    source_case: SourceCase,
    unit_engine: SourcePreservation,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    actual_event = unit_engine._event
    later = b"concurrent edit after capture\n"

    def drift_after_objects(
        run: Path, events: list[dict[str, Any]], phase: str, payload: dict[str, Any]
    ) -> None:
        actual_event(run, events, phase, payload)
        if phase == "OBJECTS_WRITTEN":
            (source_case.root / "src/a.py").write_bytes(later)

    monkeypatch.setattr(unit_engine, "_event", drift_after_objects)
    with pytest.raises(SourcePreservationError, match="SOURCE_PRESERVATION_DRIFT"):
        unit_engine.preserve(source_case.request)
    assert (source_case.root / "src/a.py").read_bytes() == later
    assert (source_case.destination / "failure.json").is_file()
    assert not (source_case.destination / "receipt.json").exists()
    assert _ref(source_case.root, "HEAD") == source_case.head


def _implementation_fixture(tmp_path: Path, policy_path: str = preservation.POLICY_PATH) -> Path:
    trusted = tmp_path / "synthetic-trusted-implementation"
    trusted.mkdir()
    _git(trusted, "init", "-b", "main")
    _git(trusted, "config", "user.name", "Synthetic Identity Checker")
    _git(trusted, "config", "user.email", "identity@example.invalid")
    paths = _implementation_paths(policy_path)
    for relative in paths:
        _write(trusted, relative, (ROOT / relative).read_bytes().replace(b"\r\n", b"\n"))
    _commit(
        trusted,
        paths,
        "synthetic identity source",
    )
    return trusted


def _implementation_paths(policy_path: str = preservation.POLICY_PATH) -> tuple[str, ...]:
    paths = {preservation.CLI_PATH, policy_path}
    # Only the explicit finite reviewed list, not runtime or filesystem discovery.
    for name in preservation._IMPLEMENTATION_MODULES:
        stem = ROOT / "src" / Path(*name.split("."))
        path = stem.with_suffix(".py")
        if not path.is_file():
            path = stem / "__init__.py"
        assert path.is_file()
        paths.add(path.relative_to(ROOT).as_posix())
    return tuple(sorted(paths))


@pytest.mark.parametrize("tamper", [None, "module", "cli", "policy", "loaded_root", "helper_root"])
@pytest.mark.parametrize("policy_path", [preservation.POLICY_PATH, preservation.V2_POLICY_PATH])
def test_implementation_identity_checker_requires_exact_head_and_loaded_root(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    tamper: str | None,
    policy_path: str,
) -> None:
    trusted = _implementation_fixture(tmp_path, policy_path)
    engine = SourcePreservation(trusted, Path(policy_path))
    # This tests the checker with a synthetic __file__ locator. It is not evidence
    # of actual loaded-code identity; the no-mock E2E below provides that proof.
    for name in preservation._IMPLEMENTATION_MODULES:
        module = sys.modules[name]
        relative = Path(module.__file__).resolve().relative_to(ROOT)
        monkeypatch.setattr(module, "__file__", str(trusted / relative))
    paths = {
        "module": preservation.MODULE_PATH,
        "cli": preservation.CLI_PATH,
        "policy": policy_path,
    }
    if tamper in paths:
        target = trusted / paths[tamper]
        target.write_bytes(target.read_bytes() + b"\n# synthetic uncommitted tamper\n")
    elif tamper == "loaded_root":
        monkeypatch.setattr(preservation, "__file__", str(tmp_path / "wrong-loaded-module.py"))
    elif tamper == "helper_root":
        monkeypatch.setattr(
            sys.modules["ai_trading_system.platform.architecture.checkout_guard"],
            "__file__",
            str(tmp_path / "wrong-loaded-guard.py"),
        )
    if tamper is not None:
        with pytest.raises(SourcePreservationError, match="SOURCE_PRESERVATION_IDENTITY"):
            engine._implementation_binding()
    else:
        binding = engine._implementation_binding()
        assert binding["commit"] == _ref(trusted, "HEAD")
        assert binding["basis"] == "COMMITTED_SOURCE_GIT_EOL_LF"
        assert {row["path"] for row in binding["files"]} == set(_implementation_paths(policy_path))
        for row in binding["files"]:
            assert row["sha256"] == row["git_blob_content_sha256"]


def _require_committed_preservation_implementation(engine: SourcePreservation) -> None:
    # Source-stage only prerequisite: this precise test MUST execute PASS after
    # source commit and in final Full. No env option, copied execution root, dirty
    # implementation fallback or mocked identity is accepted in this case.
    head = engine._git(ROOT, "rev-parse", "--verify", "HEAD").decode().strip()
    for relative in (
        preservation.MODULE_PATH, preservation.CLI_PATH,
        engine.policy_path.relative_to(ROOT).as_posix(),
    ):
        entry = engine._git(ROOT, "ls-tree", "-z", head, "--", f":(literal){relative}")
        if not entry:
            pytest.skip(
                "SOURCE_COMMIT_PREREQUISITE: actual implementation E2E must run "
                "after source commit and in final Full"
            )
        committed = engine._git(ROOT, "cat-file", "blob", f"{head}:{relative}")
        if (ROOT / relative).read_bytes().replace(b"\r\n", b"\n") != committed:
            pytest.skip(
                "SOURCE_COMMIT_PREREQUISITE: actual implementation E2E must run "
                "after source commit and in final Full"
            )
    binding = engine._implementation_binding()
    assert binding["project_root"] == ROOT.as_posix()
    assert binding["commit"] == head


def test_committed_implementation_identity_e2e(source_case: SourceCase) -> None:
    engine = SourcePreservation(ROOT, DEFAULT_POLICY_PATH)
    _require_committed_preservation_implementation(engine)
    _assert_snapshot(source_case, engine)


def test_committed_v2_cli_preserves_92_paths_and_independent_process_validates(
    tmp_path, controlled_git_environment,
) -> None:
    # Mandatory after the actual source commit and in final Full. No copied
    # implementation, patched identity or substituted policy is used here.
    engine = SourcePreservation(ROOT, Path(preservation.V2_POLICY_PATH))
    _require_committed_preservation_implementation(engine)
    case, request = _mixed_source_case(tmp_path, engine, large=True)
    before = _mixed_working_state(case, request)
    request_path = ROOT / "outputs/validation_runtime" / (
        f"source-preservation-v2-e2e-{_sha(str(tmp_path).encode())[:16]}.json"
    )
    request_path.parent.mkdir(parents=True, exist_ok=True)
    with request_path.open("xb") as stream:
        stream.write(_json(request))
    command = [sys.executable, str(ROOT / preservation.CLI_PATH)]
    selected = ["--policy", str(ROOT / preservation.V2_POLICY_PATH)]
    captured = subprocess.run(
        [*command, *selected, "preserve", "--request", str(request_path)],
        cwd=ROOT, capture_output=True, text=True, encoding="utf-8", timeout=LOADED_HOST_CLI_TIMEOUT_SECONDS, check=False,
    )
    assert captured.returncode == 0, captured.stdout + captured.stderr
    receipt = json.loads(captured.stdout)
    assert receipt["schema_version"] == "source_preservation_receipt.v2"
    assert receipt["status"] == "PASS" and len(receipt["snapshot"]["files"]) == 92
    checked = subprocess.run(
        [*command, *selected, "validate", "--receipt", receipt["receipt_path"]],
        cwd=ROOT, capture_output=True, text=True, encoding="utf-8", timeout=LOADED_HOST_CLI_TIMEOUT_SECONDS, check=False,
    )
    assert checked.returncode == 0, checked.stdout + checked.stderr
    assert json.loads(checked.stdout)["status"] == "PASS"
    assert receipt["implementation"]["commit"] == _ref(ROOT, "HEAD")
    assert preservation.V2_POLICY_PATH in {
        row["path"] for row in receipt["implementation"]["files"]
    }
    assert _mixed_working_state(case, request) == before
    assert _fence(case.root).guard.replay().active_leases == ()


def test_migration_source_inspection_is_read_only_and_preserves_exact_evidence(
    source_case: SourceCase,
    unit_engine: SourcePreservation,
) -> None:
    before = _unchanged_state(source_case)
    guard = _fence(source_case.root).guard
    lease_before = guard.replay().to_dict()
    result = unit_engine.inspect_migration_source(source_case.request)
    assert result["source_root"] == source_case.root.as_posix()
    assert result["store_root"] == guard.store.root.as_posix()
    assert result["source_task_id"] == SOURCE_TASK
    assert result["source_lease_replay"] == lease_before
    assert result["terminal_sha256"] == _sha(source_case.transaction.read_bytes())
    assert _unchanged_state(source_case) == before
    assert guard.replay().to_dict() == lease_before
    assert not source_case.destination.exists()


def test_migration_source_inspection_rejects_active_source_lease_without_mutation(
    source_case: SourceCase,
    unit_engine: SourcePreservation,
) -> None:
    guard = _fence(source_case.root).guard
    decision, handle = guard.acquire(
        intent_id="synthetic-migration-active-source",
        task_id=SOURCE_TASK,
        thread_id="synthetic-active-owner",
        actor=ACTOR,
        operation_class=preservation.CheckoutOperationClass.SHARED_MUTATION,
        base_commit=source_case.head,
        owned_paths=SOURCE_PATHS,
        shared_paths=(),
    )
    assert decision.status == "PASS" and handle is not None
    before = _unchanged_state(source_case)
    lease_before = guard.replay().to_dict()
    try:
        with pytest.raises(SourcePreservationError, match="SOURCE_PRESERVATION_LEASE"):
            unit_engine.inspect_migration_source(source_case.request)
        assert _unchanged_state(source_case) == before
        assert guard.replay().to_dict() == lease_before
        assert not source_case.destination.exists()
    finally:
        handle.release(outcome="completed")


def _migration_cli() -> Any:
    path = ROOT / "scripts/architecture_arch005_lease_arbiter.py"
    spec = importlib.util.spec_from_file_location("synthetic_migration_cli", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_migration_cli_rejects_excluded_locator_before_read(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cli = _migration_cli()
    poison = ROOT / EXCLUDED
    actual_read = Path.read_bytes

    def guarded_read(path: Path) -> bytes:
        if path == poison:
            pytest.fail("excluded locator must be rejected before reading")
        return actual_read(path)

    monkeypatch.setattr(Path, "read_bytes", guarded_read)
    with pytest.raises(ValueError):
        cli._runtime_json(poison)


def test_migration_cli_environment_denial_precedes_any_fence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from argparse import Namespace

    cli = _migration_cli()
    monkeypatch.setattr(cli, "_code_admission", lambda _: {})

    class DeniedEnvironment:
        def __init__(self, root: Path) -> None:
            assert root == ROOT

        def _environment(self, root: Path) -> None:
            raise ValueError("synthetic ambient Git rejection")

    def forbidden_fence(**kwargs: Any) -> None:
        pytest.fail("fence must not run before execution environment admission")

    monkeypatch.setattr(cli, "SourcePreservation", DeniedEnvironment)
    monkeypatch.setattr(cli, "IntegrationPublicationFence", forbidden_fence)
    with pytest.raises(ValueError, match="ambient Git rejection"):
        cli._admit(
            Namespace(
                owner_instruction_ref=cli.OWNER_REF,
                migration_id="synthetic-env-denied",
                reviewed_code=Path("unused.json"),
                source_request=None,
            )
        )


@pytest.mark.parametrize(
    ("phase", "branch", "head", "accepted"),
    [
        ("TASK_SOURCE_PRE_WRITE", BRANCH, "a" * 40, True),
        ("TASK_SOURCE_PRE_WRITE", "main", "a" * 40, False),
        ("TASK_SOURCE_PRE_WRITE", BRANCH, "b" * 40, False),
        ("CLEANUP_PRE", "main", "b" * 40, True),
        ("CLEANUP_PRE", BRANCH, "b" * 40, False),
        ("CLEANUP_PRE", "foreign-branch", "b" * 40, False),
        ("REMOTE_PUSH_PRE", "main", "b" * 40, False),
    ],
)
def test_migration_cli_branch_identity_matches_the_existing_publication_phase(
    tmp_path: Path, phase: str, branch: str, head: str, accepted: bool
) -> None:
    # Pure branch admission only. Published candidate equality and phase-chain
    # integrity remain the real fence's independent checks, never mocked here.
    cli = _migration_cli()
    common = tmp_path / "common"
    identity = {
        "checkout_root": ROOT.as_posix(),
        "git_common_dir": common.as_posix(),
        "branch_name": BRANCH,
        "head_commit": "a" * 40,
    }
    arguments = dict(phase=phase, current_head=head, current_branch=branch, current_common=common)
    if accepted:
        cli._check_workspace_identity(identity, **arguments)
    else:
        with pytest.raises(ValueError):
            cli._check_workspace_identity(identity, **arguments)


@pytest.mark.parametrize("field", ["checkout_root", "git_common_dir"])
def test_migration_cli_never_changes_root_or_common_during_main_handoff(
    tmp_path: Path, field: str
) -> None:
    cli = _migration_cli()
    common = tmp_path / "common"
    identity = {
        "checkout_root": ROOT.as_posix(),
        "git_common_dir": common.as_posix(),
        "branch_name": BRANCH,
        "head_commit": "a" * 40,
    }
    identity[field] = (tmp_path / "foreign").as_posix()
    with pytest.raises(ValueError):
        cli._check_workspace_identity(
            identity,
            phase="CLEANUP_PRE",
            current_head="b" * 40,
            current_branch="main",
            current_common=common,
        )


def _configuration_files(root: Path) -> tuple[Path, Path]:
    common = Path(
        _git(root, "rev-parse", "--path-format=absolute", "--git-common-dir").decode().strip()
    )
    git_dir = Path(_git(root, "rev-parse", "--absolute-git-dir").decode().strip())
    return common / "config", git_dir / "config.worktree"


def _configuration_digests(paths: tuple[Path, ...]) -> dict[str, tuple[int, str] | None]:
    # Retain only digests and sizes; do not print or archive configuration values.
    return {
        path.as_posix(): (path.stat().st_size, _sha(path.read_bytes())) if path.exists() else None
        for path in paths
    }


@pytest.fixture
def configuration_case(
    tmp_path: Path, controlled_git_environment: None
) -> tuple[SourcePreservation, Path, dict[str, tuple[Path, Path]]]:
    trusted = tmp_path / "configuration-trusted"
    source = tmp_path / "configuration-source"
    for root in (trusted, source):
        root.mkdir()
        _git(root, "init", "-b", "main")
        _git(root, "config", "user.name", "SYNTHETIC_CONFIG_VALUE_NOT_RECEIPT_DATA")
    _write(trusted, preservation.POLICY_PATH, (ROOT / preservation.POLICY_PATH).read_bytes())
    # This fixture tests real configuration admission only, not loaded code or
    # source preservation. It does not substitute any implementation identity.
    return (
        SourcePreservation(trusted, Path(preservation.POLICY_PATH)),
        source,
        {"trusted": _configuration_files(trusted), "source": _configuration_files(source)},
    )


def test_shared_git_admission_preserves_original_snapshot_and_drift_rejection(
    configuration_case: tuple[SourcePreservation, Path, dict[str, tuple[Path, Path]]],
) -> None:
    engine, source, _paths = configuration_case
    executable = shutil.which("git")
    assert executable is not None
    shared = preservation.GitConfigurationAdmission(
        engine.project_root, git_executable=Path(executable), git_environment=dict(os.environ),
    )
    original = engine._environment(source)
    assert shared.capture(source) == original
    shared.recheck(source, original)
    _git(source, "config", "filter.synthetic.clean", "SYNTHETIC_NEVER_EXECUTE")
    with pytest.raises(SourcePreservationError) as drift:
        shared.recheck(source, original)
    assert drift.value.code == "SOURCE_PRESERVATION_DRIFT"
    with pytest.raises(SourcePreservationError) as denied:
        shared.capture(source)
    assert denied.value.code == "SOURCE_PRESERVATION_FILTER"


def test_shared_git_admission_ignores_hostile_path(
    configuration_case: tuple[SourcePreservation, Path, dict[str, tuple[Path, Path]]],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine, source, _paths = configuration_case
    executable = shutil.which("git")
    assert executable is not None
    shared = preservation.GitConfigurationAdmission(
        engine.project_root, git_executable=Path(executable), git_environment=dict(os.environ),
    )
    expected = shared.capture(source)
    hostile = tmp_path / "hostile-path"
    hostile.mkdir()
    (hostile / "git.exe").write_bytes(b"must never execute")
    (hostile / "git").write_bytes(b"must never execute")
    monkeypatch.setenv("PATH", str(hostile))
    assert shared.capture(source) == expected
    shared.recheck(source, expected)


@pytest.mark.parametrize("program", ["git", "missing-git.exe"])
def test_shared_git_admission_rejects_unbound_program_before_spawn(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, program: str,
) -> None:
    def forbidden(*args: Any, **kwargs: Any) -> None:
        pytest.fail("invalid executable must fail before subprocess creation")

    monkeypatch.setattr(subprocess, "run", forbidden)
    candidate = Path(program) if program == "git" else tmp_path / program
    with pytest.raises(SourcePreservationError) as error:
        preservation.GitConfigurationAdmission(
            tmp_path, git_executable=candidate, git_environment=dict(os.environ),
        )
    assert error.value.code == "SOURCE_PRESERVATION_ENVIRONMENT"


def test_shared_git_environment_is_copied_and_matches_actual_dispatch(
    configuration_case: tuple[SourcePreservation, Path, dict[str, tuple[Path, Path]]],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine, source, _paths = configuration_case
    executable = shutil.which("git")
    assert executable is not None
    explicit = {**os.environ, "DEVX015_ENV_WITNESS": "bound"}
    shared = preservation.GitConfigurationAdmission(
        engine.project_root, git_executable=Path(executable), git_environment=explicit,
    )
    expected = shared.capture(source)
    explicit["DEVX015_ENV_WITNESS"] = "caller-mutated"
    explicit["GIT_DIR"] = str(source / "caller-routing")
    monkeypatch.setenv("DEVX015_ENV_WITNESS", "parent-mutated")
    monkeypatch.setenv("GIT_DIR", str(source / "parent-routing"))
    original = subprocess.run
    observations = []

    def observed_run(*args, **kwargs):
        environment = kwargs["env"]
        assert environment["DEVX015_ENV_WITNESS"] == "bound"
        assert "GIT_DIR" not in environment
        observations.append(environment)
        return original(*args, **kwargs)

    monkeypatch.setattr(subprocess, "run", observed_run)
    assert shared.capture(source) == expected
    shared.recheck(source, expected)
    assert observations
    count = len(observations)
    with pytest.raises(SourcePreservationError) as rejected:
        engine._environment(source)
    assert rejected.value.code == "SOURCE_PRESERVATION_ENVIRONMENT"
    assert len(observations) == count


@pytest.mark.skipif(os.name != "nt", reason="Windows native file custody proof")
@pytest.mark.parametrize("insert_configuration", [False, True, "runtime"])
def test_held_git_admission_parses_only_after_native_custody(
    configuration_case: tuple[SourcePreservation, Path, dict[str, tuple[Path, Path]]],
    monkeypatch: pytest.MonkeyPatch, insert_configuration: bool | str,
) -> None:
    import ctypes as c
    from contextlib import contextmanager
    from ctypes import wintypes as w

    from ai_trading_system.platform.architecture import workflow_coordination as coordination
    from ai_trading_system.platform.architecture.workflow_execution import (
        ExecutionContainmentError,
        capture_acceptance_implementation,
        capture_matching_inspector_sources,
    )

    engine, source, paths = configuration_case
    source_file = source / "src" / "probe.py"
    source_file.parent.mkdir()
    source_file.write_text("raise RuntimeError('CANDIDATE_MUST_NOT_EXECUTE')\n", encoding="utf-8")
    _git(source, "add", "--", "src/probe.py")
    _git(source, "-c", "user.name=Synthetic", "-c", "user.email=synthetic@localhost",
         "commit", "-qm", "synthetic source")
    candidate = _ref(source, "HEAD")
    copies = {}
    extra_files = {
        "extra_source": "shadow.py", "cache": "__pycache__/probe.cpython-313.pyc",
        "native": "probe.pyd", "path_config": "import.pth", "archive": "import.zip",
    }
    for variant in ("matching", "stale", "unheld", "junction", *extra_files):
        copy_root = source.parent / ("inspector-" + variant)
        copy_file = copy_root / "src" / "probe.py"
        copy_file.parent.mkdir(parents=True)
        copy_file.write_bytes(source_file.read_bytes() if variant != "stale" else b"# stale\n")
        if variant in extra_files:
            extra = copy_file.parent / extra_files[variant]
            extra.parent.mkdir(exist_ok=True)
            extra.write_bytes(b"unapproved code carrier")
        copies[variant] = (copy_root, copy_file)
    outside_inspector = source.parent / "outside-inspector"
    outside_inspector.mkdir()
    (outside_inspector / "witness.py").write_text("raise RuntimeError('MUST_NOT_EXECUTE')\n")
    subprocess.run(
        ["cmd.exe", "/d", "/c", "mklink", "/J",
         str(copies["junction"][0] / "src" / "redirect"), str(outside_inspector)],
        check=True, capture_output=True,
    )
    missing_copy = source.parent / "inspector-missing"
    missing_copy.mkdir()
    empty_runtime_directory = copies["matching"][0] / "empty"
    empty_runtime_directory.mkdir()
    executable = shutil.which("git")
    assert executable is not None
    program = Path(executable)
    shared = preservation.GitConfigurationAdmission(
        engine.project_root, git_executable=program, git_environment=dict(os.environ),
    )
    expected = shared.capture(source)
    active = False
    calls = []

    class ReadOnlyProbe(coordination._WindowsEnrollmentAdministrator):
        # Only elevation/ACL admission is modeled; file/ancestor handles and
        # Git parsing are real. This is not administrative deployment evidence.
        def __init__(self):
            self.c, self.w = c, w
            self.kernel = c.WinDLL("kernel32", use_last_error=True)
            self._bind(self.kernel, "CloseHandle", [w.HANDLE], w.BOOL)

        def assert_protected(self, path, **kwargs):
            pass

        @contextmanager
        def hold_protected_files(self, *args, **kwargs):
            nonlocal active
            with super().hold_protected_files(*args, **kwargs):
                active = True
                try:
                    if insert_configuration is True:
                        paths["source"][1].write_text("[test]\nvalue = inserted\n")
                    elif insert_configuration == "runtime":
                        (copies["matching"][0] / "injected.py").write_bytes(b"# unreported input")
                    yield
                finally:
                    active = False

    original = shared._git

    def checked_git(*args, **kwargs):
        assert active, "Git must not parse configuration outside the protected lifetime"
        calls.append(args)
        return original(*args, **kwargs)

    monkeypatch.setattr(coordination, "_WindowsEnrollmentAdministrator", ReadOnlyProbe)
    monkeypatch.setattr(shared, "_git", checked_git)
    info = program.stat()
    runtime_files = {program: _sha(program.read_bytes())}
    runtime_files.update({file: _sha(file.read_bytes()) for variant, (_root, file) in copies.items()
                          if variant != "unheld"})
    context = shared.held(
        source, runtime_files=runtime_files,
        runtime_identities={program: (info.st_dev, info.st_ino, info.st_nlink)},
        runtime_roots=(copies["matching"][0],),
    )
    with pytest.raises(SourcePreservationError, match="HELD_CONTEXT_INACTIVE"):
        shared.active_context()
    with pytest.raises(SourcePreservationError, match="HELD_CONTEXT_FACTORY_REQUIRED"):
        preservation.HeldGitConfiguration()
    if insert_configuration:
        with pytest.raises(SourcePreservationError) as error:
            with context:
                pytest.fail("new configuration must be rejected before caller entry")
        assert error.value.code == (
            "SOURCE_PRESERVATION_DRIFT" if insert_configuration is True
            else "SOURCE_PRESERVATION_RUNTIME_UNDECLARED_FILE"
        )
        assert calls == []
    else:
        with context as observed:
            assert observed == expected
            live = shared.active_context()
            live.assert_current(source, {program: _sha(program.read_bytes())})
            assert live.assert_runtime_root(copies["matching"][0]) == copies["matching"][0]
            with pytest.raises(SourcePreservationError, match="RUNTIME_ROOT_NOT_HELD"):
                live.assert_runtime_root(copies["stale"][0])
            with pytest.raises(OSError):
                empty_runtime_directory.rename(empty_runtime_directory.with_name("renamed"))
            from ai_trading_system.atlas import page_effectiveness as page
            from ai_trading_system.platform.architecture import (
                compatibility_authority as compatibility,
            )
            from ai_trading_system.platform.architecture import (
                validation_readiness as readiness,
            )

            transported = []
            original_result = shared._git_result

            def checked_result(root, *arguments, **kwargs):
                assert active
                transported.append((root, arguments))
                return original_result(root, *arguments, **kwargs)

            monkeypatch.setattr(shared, "_git_result", checked_result)
            with monkeypatch.context() as scoped:
                scoped.setattr(page, "_ordinary_commit_time", lambda *args: "OLD_CACHE")
                with live.inspection(source):
                    from ai_trading_system.platform.architecture import (
                        workflow_execution as execution,
                    )

                    auto_capture = capture_acceptance_implementation(source, candidate)
                    assert [row["path"] for row in auto_capture] == ["src/probe.py"]
                    # Bind the synthetic authority path to invalid JSON so the
                    # original mapping gate rejects after all real object reads.
                    scoped.setattr(execution, "_ACCEPTANCE_MANIFEST", "src/probe.py")
                    before_binding = len(transported)
                    with pytest.raises(
                        ExecutionContainmentError, match="ACCEPTANCE_MAPPING_INVALID",
                    ):
                        execution.bind_mandatory_acceptance(source, candidate)
                    assert [args[0] for _root, args in transported[before_binding:]] == [
                        "rev-parse", "ls-tree", "cat-file",
                    ]
                    assert page.repository_head(source) == candidate
                    from ai_trading_system.platform.architecture import (
                        integration_publication_fence as fence,
                    )
                    from scripts import run_validation_tier as runner

                    before_full_git = len(transported)
                    assert fence._git(source, "rev-parse", "HEAD") == candidate
                    fence._require_ancestor(source, candidate, candidate)
                    assert int(runner._inspection_git_bytes(
                        source, "cat-file", "-s", f"{candidate}:src/probe.py",
                    ).stdout) == len(source_file.read_bytes())
                    assert runner._inspection_git_bytes(
                        source, "cat-file", "blob", f"{candidate}:src/probe.py",
                    ).stdout == source_file.read_bytes()
                    assert len(transported) == before_full_git + 4
                    assert runner._git_commit(source) == candidate
                    assert runner._inspection_git_bytes(
                        source, "show", f"{candidate}:src/probe.py",
                    ).stdout == source_file.read_bytes()
                    assert len(transported) == before_full_git + 6
                    from ai_trading_system.platform.architecture import task_registry_canonical

                    before_canonical = len(transported)
                    assert task_registry_canonical._canonical_git_read(
                        source, "rev-parse", "--verify", candidate + "^{commit}",
                    ).strip() == candidate.encode("ascii")
                    assert task_registry_canonical._canonical_git_blob(
                        source, candidate, "src/probe.py",
                    ) == source_file.read_bytes()
                    assert len(transported) == before_canonical + 3
                    with pytest.raises(ValueError, match="CANONICAL_GIT_BLOB_MISSING"):
                        task_registry_canonical._canonical_git_blob(
                            source, candidate, "src/missing.py",
                        )
                    from ai_trading_system.platform.architecture import checkout_guard as checkout

                    before_checkout = len(transported)
                    identity = checkout.resolve_checkout_identity(source)
                    assert identity.head_commit == candidate
                    assert checkout._registered_worktrees(source)
                    audit_index = (source / ".git" / "index").read_bytes()
                    assert isinstance(checkout.collect_checkout_dirty_paths(
                        source, exclusions=("unrelated.txt",),
                    ), tuple)
                    checkout._run_git_diff_check(
                        source, exclusions=("unrelated.txt",), cached=False,
                    )
                    checkout._run_git_diff_check(
                        source, exclusions=("unrelated.txt",), cached=True,
                    )
                    assert (source / ".git" / "index").read_bytes() == audit_index
                    assert len(transported) > before_checkout
                    with pytest.raises(fence.PublicationFenceError, match="ANCESTRY_INVALID"):
                        fence._require_ancestor(source, "0" * 40, candidate)
                    assert page._commit_time(str(source), candidate) != "OLD_CACHE"
                    assert compatibility._git_bytes(source, candidate, "src/probe.py") == (
                        source_file.read_bytes()
                    )
                    assert compatibility._git_text(source, candidate, "src/missing.py") == ""
                    assert compatibility._git_lines(
                        source, ["grep", "-l", "-F", "CANDIDATE_MUST_NOT_EXECUTE", candidate,
                                 "--", "src", "scripts", "tests"], allow_no_match=True,
                    ) == [f"{candidate}:src/probe.py"]
                    assert readiness._git(source, "cat-file", "-e",
                                          f"{candidate}:src/probe.py").returncode == 0
                    assert readiness._git(
                        source, "diff", "--quiet", "--no-ext-diff", "--no-textconv",
                        candidate, "--", ":(literal)src",
                    ).returncode == 0
                    index_path = source / ".git" / "index"
                    index_before = index_path.read_bytes()
                    source_before = source_file.read_bytes()
                    source_file.write_bytes(source_before + b"# changed\n")
                    try:
                        assert readiness._git(
                            source, "diff", "--quiet", "--no-ext-diff", "--no-textconv",
                            candidate, "--", ":(literal)src",
                        ).returncode == 1
                        assert index_path.read_bytes() == index_before
                    finally:
                        source_file.write_bytes(source_before)
                    assert readiness._git(
                        source, "ls-files", "--others", "--exclude-standard", "-z",
                        "--", ":(literal)src",
                    ).stdout == ""
                    before_rejected = len(transported)
                    for rejected_args in (
                        ("reset", "--hard"), ("-c", "core.fsmonitor=evil", "status"),
                        ("show", "HEAD:src/probe.py"),
                        ("show", candidate + ":../outside"),
                        ("diff", "--numstat", "-z", "--ext-diff", candidate, "--", "src"),
                        ("show", "-s", "--format=%cI", "--output=outside"),
                        ("ls-tree", "HEAD", "--", "src/probe.py"),
                        ("cat-file", "blob", "HEAD:src/probe.py"),
                        ("cat-file", "-s", candidate + ":../outside"),
                        ("merge-base", "--is-ancestor", "HEAD", candidate),
                        ("remote", "set-url", "origin", "outside"),
                        ("rev-parse", "--git-path", "outside"),
                        ("rev-parse", "--verify", "--output=outside"),
                        ("worktree", "add", "outside"),
                        ("rev-parse", "--verify", "HEAD^{commit}"),
                        ("rev-parse", "--verify", candidate + "^{tree}"),
                        ("ls-tree", "-z", candidate, "--", "../outside"),
                        ("diff", "--check", "--", ".", ":(exclude)unrelated.txt"),
                    ):
                        with pytest.raises(
                            SourcePreservationError, match="HELD_CONTEXT_GIT_COMMAND",
                        ):
                            preservation.inspection_git_result(source, *rejected_args)
                    assert len(transported) == before_rejected
                    with pytest.raises(SourcePreservationError, match="HELD_CONTEXT_NESTED"):
                        with live.inspection(source):
                            pytest.fail("nested transport accepted")
                assert page._commit_time(str(source), candidate) == "OLD_CACHE"
                assert preservation.inspection_git_result(source, "rev-parse", "HEAD") is None
                # Synthetic checker bodies isolate transport propagation, not
                # actual Full dependency acceptance. Code-root/version gates
                # retain their independent original regression coverage.
                scoped.setattr(readiness, "_inspection_code_root", lambda: source)
                scoped.setattr(
                    readiness, "_inspection_code_identity", readiness._candidate_identity,
                )
                visited = []

                def checker(root, sha):
                    visited.append(sha)
                    assert page._commit_time(str(root), sha) != "OLD_CACHE"
                    return readiness._candidate_identity(root, sha)

                scoped.setattr(readiness, "_checkers", lambda: {
                    name: checker for name in readiness.CHECKER_IDS
                })
                inspected = readiness.check_full_readiness(source, candidate, git_context=live)
                assert inspected["status"] == "PASS"
                assert len(visited) == len(readiness.CHECKER_IDS)
                assert all(root == source for root, _args in transported)
                before = len(transported)
                refused = readiness.check_full_readiness(source, candidate, git_context={})
                assert refused["status"] == "BLOCKED"
                assert len(transported) == before
                # A real live custody context does not admit this ordinary
                # development interpreter as an installed isolated inspector.
                scoped.setattr(readiness, "_inspection_code_root", lambda: copies["matching"][0])
                before_visits = len(visited)
                refused_runtime = readiness.check_full_readiness(
                    source, candidate, git_context=live,
                )
                assert refused_runtime["status"] == "BLOCKED"
                assert "ACCEPTANCE_INSPECTOR_STARTUP" in str(refused_runtime)
                assert len(visited) == before_visits
                # Only test readiness composition here; this seam is not proof
                # of a deployed protected runtime or administrator admission.
                from ai_trading_system.platform.architecture import workflow_execution as execution

                def admitted_runtime(root, sha, *, git_context):
                    assert root == source and sha == candidate and git_context is live
                    return {"inspector_root": copies["matching"][0].as_posix()}

                scoped.setattr(execution, "bind_protected_inspector_runtime", admitted_runtime)
                composed = readiness.check_full_readiness(source, candidate, git_context=live)
                assert composed["status"] == "PASS"
                assert len(visited) == before_visits + len(readiness.CHECKER_IDS)
                with live.inspection(source):
                    nested_readiness = readiness.check_full_readiness(
                        source, candidate, git_context=live,
                    )
                    assert nested_readiness["status"] == "PASS"
                before_visits = len(visited) - len(readiness.CHECKER_IDS)
                # Explicit launcher composition seams: no real worker token,
                # administrator enrollment or Full execution is represented.
                class UnitToken:
                    def validate_launcher(self):
                        return {"unit": True}

                token = UnitToken()

                class UnitExchange:
                    def validate_worker(self, worker):
                        assert worker is token

                    @property
                    def profile_directory(self):
                        return source

                exchange = UnitExchange()
                scoped.setattr(execution, "WindowsWorkerToken", UnitToken)
                scoped.setattr(coordination, "WindowsWorkerExchange", UnitExchange)
                launch_environment = {"UNIT_ONLY": "1"}

                def receive_main(args, *, protected):
                    assert preservation.current_inspection_context(source) is live
                    assert protected.root == source
                    assert protected.worker_token is token
                    assert protected.worker_exchange is exchange
                    assert protected.worker_environment == launch_environment
                    assert protected.worker_environment is not launch_environment
                    return 27

                scoped.setattr(runner, "_main", receive_main)
                launch_arguments = ["full", "--write-runtime-artifact", "--task-id", "unit",
                                    "--publication-transaction", "unit.json"]
                assert runner.run_protected_full(
                    launch_arguments, candidate_root=source, git_context=live,
                    worker_token=token, worker_environment=launch_environment,
                    worker_exchange=exchange,
                ) == 27
                assert preservation.current_inspection_context(source) is None

                def failed_main(*args, **kwargs):
                    raise RuntimeError("unit dispatch failure")

                scoped.setattr(runner, "_main", failed_main)
                with pytest.raises(RuntimeError, match="unit dispatch failure"):
                    runner.run_protected_full(
                        launch_arguments, candidate_root=source, git_context=live,
                        worker_token=token, worker_environment=launch_environment,
                        worker_exchange=exchange,
                    )
                assert preservation.current_inspection_context(source) is None
                scoped.setattr(readiness, "_inspection_code_root", lambda: copies["stale"][0])
                wrong_origin = readiness.check_full_readiness(source, candidate, git_context=live)
                assert wrong_origin["status"] == "BLOCKED"
                assert "READINESS_INSPECTOR_ORIGIN" in str(wrong_origin)
                assert len(visited) == before_visits + len(readiness.CHECKER_IDS)
            captured = capture_acceptance_implementation(source, candidate, git_context=live)
            assert [row["path"] for row in captured] == ["src/probe.py"]
            assert captured[0]["sha256"] == _sha(source_file.read_bytes())
            matched = capture_matching_inspector_sources(
                copies["matching"][0], source, candidate, git_context=live,
            )
            assert matched == [{"path": copies["matching"][1].as_posix(),
                                "sha256": _sha(source_file.read_bytes()),
                                "size_bytes": source_file.stat().st_size}]
            for variant, reason in (("stale", "ACCEPTANCE_INSPECTOR_NOT_CANDIDATE"),
                                    ("unheld", "ACCEPTANCE_INSPECTOR_NOT_HELD"),
                                    ("junction", "ACCEPTANCE_INSPECTOR_REPARSE"),
                                    ("extra_source", "ACCEPTANCE_INSPECTOR_SOURCE_EXTRA"),
                                    *((name, "ACCEPTANCE_INSPECTOR_EXECUTABLE_EXTRA")
                                      for name in extra_files if name != "extra_source")):
                with pytest.raises(ExecutionContainmentError, match=reason):
                    capture_matching_inspector_sources(
                        copies[variant][0], source, candidate, git_context=live,
                    )
            with pytest.raises(
                ExecutionContainmentError, match="ACCEPTANCE_INSPECTOR_SOURCE_MISSING",
            ):
                capture_matching_inspector_sources(
                    missing_copy, source, candidate, git_context=live,
                )
            assert live.candidate_source_tree(source / ".." / source.name, candidate)
            assert calls[-1][0] == source
            before_calls = len(calls)
            with pytest.raises(ExecutionContainmentError, match="HELD_CONTEXT_CANDIDATE"):
                capture_acceptance_implementation(source, "HEAD", git_context=live)
            with pytest.raises(ExecutionContainmentError, match="ACCEPTANCE_GIT_CONTEXT_REQUIRED"):
                capture_acceptance_implementation(source, candidate, git_context={})
            assert len(calls) == before_calls
            with pytest.raises(SourcePreservationError, match="HELD_CONTEXT_FILES"):
                live.assert_current(source, {program: "0" * 64})
            with pytest.raises(SourcePreservationError, match="HELD_CONTEXT_SCOPE"):
                live.assert_current(source.parent, {})
            with ThreadPoolExecutor(max_workers=1) as executor:
                foreign = executor.submit(live.assert_current, source, {})
                with pytest.raises(SourcePreservationError, match="HELD_CONTEXT_INACTIVE"):
                    foreign.result(timeout=10)
            with pytest.raises(SourcePreservationError, match="HELD_CONTEXT_NESTED"):
                with shared.held(source, runtime_files={}, runtime_identities={}):
                    pytest.fail("nested context must not replace the active custody")
            with pytest.raises(OSError):
                paths["source"][0].write_bytes(b"must not replace config")
            observed.clear()  # Caller changes cannot weaken the internal exit recheck.
        assert calls and not active
        before_transport = len(transported)
        expired = readiness.check_full_readiness(source, candidate, git_context=live)
        assert expired["status"] == "BLOCKED"
        assert len(transported) == before_transport
        # A stale context selected by an inherited caller must not silently
        # become an ordinary HEAD/profile lookup after custody has ended.
        selected = preservation._INSPECTION_GIT.set(live)
        try:
            with pytest.raises(SourcePreservationError, match="HELD_CONTEXT_INACTIVE"):
                runner._git_commit(source)
            with pytest.raises(SourcePreservationError, match="HELD_CONTEXT_INACTIVE"):
                runner._inspection_git_bytes(source, "show", f"{candidate}:src/probe.py")
        finally:
            preservation._INSPECTION_GIT.reset(selected)
        assert len(transported) == before_transport
        with pytest.raises(SourcePreservationError, match="HELD_CONTEXT_INACTIVE"):
            live.assert_current(source, {})
        with pytest.raises(SourcePreservationError, match="HELD_CONTEXT_INACTIVE"):
            live.assert_runtime_root(copies["matching"][0])
        before_calls = len(calls)
        with pytest.raises(ExecutionContainmentError, match="HELD_CONTEXT_INACTIVE"):
            capture_acceptance_implementation(source, candidate, git_context=live)
        assert len(calls) == before_calls
        with pytest.raises(SourcePreservationError, match="HELD_CONTEXT_INACTIVE"):
            shared.active_context()


@pytest.mark.skipif(os.name != "nt", reason="Windows administrator admission proof")
def test_held_git_admission_requires_real_administrator_before_metadata(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    import ctypes

    from ai_trading_system.platform.architecture import workflow_coordination as coordination

    if ctypes.windll.shell32.IsUserAnAdmin():
        pytest.skip("this negative case requires an ordinary unelevated process")
    executable = shutil.which("git")
    assert executable is not None
    shared = preservation.GitConfigurationAdmission(
        tmp_path, git_executable=Path(executable), git_environment=dict(os.environ),
    )

    def forbidden(*args, **kwargs):
        pytest.fail("administrator rejection must precede Git metadata access")

    monkeypatch.setattr(preservation, "_git_configuration_layout", forbidden)
    with pytest.raises(coordination.ParallelControlError, match="ADMINISTRATOR_REQUIRED"):
        with shared.held(tmp_path, runtime_files={}, runtime_identities={}):
            pytest.fail("ordinary process cannot acquire protected admission")


def test_installed_inspector_rejects_development_startup_before_inventory(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    def forbidden(*args, **kwargs):
        pytest.fail("ordinary startup reached installation inventory")

    monkeypatch.setattr(preservation, "_runtime_namespace", forbidden)
    with pytest.raises(SourcePreservationError, match="INSTALLED_INSPECTOR_STARTUP"):
        with preservation.hold_installed_inspector(tmp_path):
            pytest.fail("development interpreter admitted")


@pytest.mark.skipif(os.name != "nt", reason="Windows administrator admission proof")
def test_installed_inspector_requires_real_admin_before_inventory(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    import ctypes
    from types import SimpleNamespace

    from ai_trading_system.platform.architecture import workflow_coordination as coordination

    if ctypes.windll.shell32.IsUserAnAdmin():
        pytest.skip("negative case requires an ordinary unelevated process")
    # Only startup locations are modeled; the OS administrator check is real.
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(preservation, "__file__", str(tmp_path / "installed.py"))
    monkeypatch.setattr(preservation, "sys", SimpleNamespace(
        executable=str(tmp_path / "python.exe"), flags=SimpleNamespace(isolated=1, no_site=1),
        dont_write_bytecode=True, modules={}, prefix=str(tmp_path), base_prefix=str(tmp_path),
    ))

    def forbidden(*args, **kwargs):
        pytest.fail("non-administrator reached installation inventory")

    monkeypatch.setattr(preservation, "_runtime_namespace", forbidden)
    with pytest.raises(coordination.ParallelControlError, match="ADMINISTRATOR_REQUIRED"):
        with preservation.hold_installed_inspector(tmp_path / "candidate"):
            pytest.fail("ordinary process admitted")


@pytest.mark.skipif(os.name != "nt", reason="native bounded file reads")
@pytest.mark.parametrize("fault", [None, "hash", "size", "extra", "duplicate", "self", "escape"])
def test_installed_inspector_composes_complete_inventory_and_fixed_git_environment(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, fault: str | None,
) -> None:
    import ctypes as c
    from contextlib import contextmanager
    from ctypes import wintypes as w
    from types import SimpleNamespace

    from ai_trading_system.platform.architecture import workflow_coordination as coordination

    runtime = tmp_path / "installed"
    git = runtime / "git" / "cmd" / "git.exe"
    git.parent.mkdir(parents=True)
    git.write_bytes(b"unit git witness - never executed")
    (runtime / "python.exe").write_bytes(b"unit interpreter witness - never executed")
    rows = [{"path": path.relative_to(runtime).as_posix(), "sha256": _sha(path.read_bytes()),
             "size_bytes": path.stat().st_size} for path in (git, runtime / "python.exe")]
    if fault == "hash":
        rows[0]["sha256"] = "0" * 64
    elif fault == "size":
        rows[0]["size_bytes"] += 1
    elif fault == "extra":
        (runtime / "undeclared.py").write_text("# must refuse", encoding="utf-8")
    elif fault == "duplicate":
        rows.append(dict(rows[0]))
    elif fault == "self":
        rows[0]["path"] = "runtime-manifest.json"
    elif fault == "escape":
        rows[0]["path"] = "../outside"
    manifest = runtime / "runtime-manifest.json"
    manifest.write_text(json.dumps({"schema_version": "devx015_installed_runtime.v1",
                                    "files": rows}), encoding="utf-8")
    monkeypatch.chdir(runtime)
    monkeypatch.setattr(preservation, "__file__", str(runtime / "source_preservation.py"))
    monkeypatch.setattr(preservation, "sys", SimpleNamespace(
        executable=str(runtime / "python.exe"), flags=SimpleNamespace(isolated=1, no_site=1),
        dont_write_bytecode=True, modules={}, prefix=str(runtime), base_prefix=str(runtime),
    ))
    monkeypatch.setenv("GIT_EXEC_PATH", "untrusted-helper-path")
    monkeypatch.setenv("PATH", "untrusted-search-path")
    protected = set()

    class UnitAdministrator(coordination._WindowsEnrollmentAdministrator):
        def __init__(self):
            self.c, self.w = c, w
            self.kernel = c.WinDLL("kernel32", use_last_error=True)
            self._bind(self.kernel, "CloseHandle", [w.HANDLE], w.BOOL)

        def assert_protected(self, path):
            protected.add(path)

    marker = object()

    class UnitAdmission:
        def __init__(self, root, *, git_executable, git_environment):
            assert git_executable == git
            assert "GIT_EXEC_PATH" not in git_environment
            assert git_environment["PATH"].split(os.pathsep) == [
                str(runtime / "git" / part) for part in ("cmd", "mingw64/bin", "usr/bin")
            ]

        @contextmanager
        def held(self, root, *, runtime_files, runtime_identities, runtime_roots):
            assert set(runtime_files) == {git, runtime / "python.exe", manifest}
            assert manifest in protected
            assert runtime_roots == (runtime,)
            assert all(row[2] == 1 for row in runtime_identities.values())
            with UnitAdministrator().hold_protected_files(
                runtime_files, protected_directories=runtime_roots,
                expected_file_identities=runtime_identities,
            ):
                assert set(runtime_files) <= protected
                yield

        def active_context(self):
            return marker

    # Explicit composition seams, not native administrator admission evidence.
    monkeypatch.setattr(coordination, "_WindowsEnrollmentAdministrator", UnitAdministrator)
    monkeypatch.setattr(preservation, "GitConfigurationAdmission", UnitAdmission)
    if fault is None:
        with preservation.hold_installed_inspector(tmp_path / "candidate") as context:
            assert context is marker
            with pytest.raises(OSError):
                with git.open("ab"):
                    pass
    else:
        with pytest.raises((ValueError, coordination.ParallelControlError)):
            with preservation.hold_installed_inspector(tmp_path / "candidate"):
                pytest.fail("invalid installation admitted")


def _assert_configuration_capture(
    snapshot: dict[str, Any],
    engine: SourcePreservation,
    source: Path,
    paths: dict[str, tuple[Path, Path]],
) -> None:
    assert snapshot["schema_version"] == "source_preservation_git_configuration.v1"
    for role, root in (("trusted", engine.project_root), ("source", source)):
        record = snapshot[role]
        assert record["checkout_root"] == root.as_posix()
        assert record["git_common_dir"] == paths[role][0].parent.as_posix()
        assert record["git_dir"] == paths[role][1].parent.as_posix()
        assert record["entry_count"] > 0
        assert len(record["entries_sha256"]) == 64
        files = record["files"]
        assert [item["role"] for item in files] == ["COMMON_CONFIG", "WORKTREE_CONFIG"]
        for item, path in zip(files, paths[role], strict=True):
            assert set(item) == {"role", "path", "present", "size_bytes", "sha256"}
            assert item["path"] == path.as_posix()
            assert item["present"] is path.exists()
            if path.exists():
                assert item["size_bytes"] == path.stat().st_size
                assert item["sha256"] == _sha(path.read_bytes())
            else:
                assert item["size_bytes"] is item["sha256"] is None
    assert len(snapshot["snapshot_sha256"]) == 64
    assert "SYNTHETIC_CONFIG_VALUE_NOT_RECEIPT_DATA" not in _json(snapshot).decode()


@pytest.mark.parametrize(
    ("extension", "mode"),
    [(None, "ABSENT"), ("false", "FALSE"), ("0", "FALSE"), ("true", "TRUE"), ("yes", "TRUE")],
)
def test_worktree_configuration_uses_git_boolean_semantics_and_records_missing_file(
    configuration_case: tuple[SourcePreservation, Path, dict[str, tuple[Path, Path]]],
    extension: str | None,
    mode: str,
) -> None:
    engine, source, paths = configuration_case
    if extension is not None:
        for root in (engine.project_root, source):
            _git(root, "config", "extensions.worktreeConfig", extension)
    all_paths = (*paths["trusted"], *paths["source"])
    before = _configuration_digests(all_paths)
    snapshot = engine._environment(source)
    _assert_configuration_capture(snapshot, engine, source, paths)
    assert snapshot["trusted"]["worktree_config_mode"] == mode
    assert snapshot["source"]["worktree_config_mode"] == mode
    assert _configuration_digests(all_paths) == before
    engine._recheck_environment(source, snapshot)
    assert _configuration_digests(all_paths) == before


def test_distinct_linked_worktrees_capture_their_actual_private_configuration_files(
    tmp_path: Path,
    controlled_git_environment: None,
) -> None:
    trusted = _implementation_fixture(tmp_path)
    source = tmp_path / "configuration-linked-source"
    _git(trusted, "worktree", "add", "-b", "codex/configuration-linked", str(source), "HEAD")
    _git(trusted, "config", "extensions.worktreeConfig", "true")
    paths = {"trusted": _configuration_files(trusted), "source": _configuration_files(source)}
    paths["trusted"][1].write_bytes(b"[diff]\n\talgorithm = histogram\n")
    paths["source"][1].write_bytes(b"[diff]\n\talgorithm = patience\n")
    assert paths["trusted"][0] == paths["source"][0]
    assert paths["trusted"][1] != paths["source"][1]
    before = _configuration_digests((*paths["trusted"], *paths["source"]))
    engine = SourcePreservation(trusted, Path(preservation.POLICY_PATH))
    snapshot = engine._environment(source)
    _assert_configuration_capture(snapshot, engine, source, paths)
    assert snapshot["trusted"]["entries_sha256"] != snapshot["source"]["entries_sha256"]
    engine._recheck_environment(source, snapshot)
    assert _configuration_digests((*paths["trusted"], *paths["source"])) == before


@pytest.mark.parametrize("side", ["trusted", "source"])
@pytest.mark.parametrize(
    ("key", "value"),
    [
        ("core.fsmonitor", "SYNTHETIC_NEVER_EXECUTE"),
        ("core.sshCommand", "SYNTHETIC_NEVER_EXECUTE"),
        ("filter.synthetic.clean", "SYNTHETIC_NEVER_EXECUTE"),
        ("remote.synthetic.promisor", "true"),
    ],
)
def test_dangerous_worktree_only_configuration_is_rejected_in_each_actual_checkout(
    configuration_case: tuple[SourcePreservation, Path, dict[str, tuple[Path, Path]]],
    side: str,
    key: str,
    value: str,
) -> None:
    engine, source, paths = configuration_case
    root = engine.project_root if side == "trusted" else source
    _git(root, "config", "extensions.worktreeConfig", "true")
    _git(root, "config", "--worktree", key, value)
    before = _configuration_digests((*paths["trusted"], *paths["source"]))
    with pytest.raises(SourcePreservationError) as error:
        engine._environment(source)
    assert error.value.code == "SOURCE_PRESERVATION_FILTER"
    assert value not in str(error.value)
    assert _configuration_digests((*paths["trusted"], *paths["source"])) == before


@pytest.mark.parametrize("side", ["trusted", "source"])
def test_dangerous_common_value_cannot_be_hidden_by_worktree_or_command_override(
    configuration_case: tuple[SourcePreservation, Path, dict[str, tuple[Path, Path]]],
    side: str,
) -> None:
    engine, source, paths = configuration_case
    root = engine.project_root if side == "trusted" else source
    _git(root, "config", "extensions.worktreeConfig", "true")
    _git(root, "config", "core.fsmonitor", "SYNTHETIC_NEVER_EXECUTE")
    _git(root, "config", "--worktree", "core.fsmonitor", "false")
    # The implementation additionally supplies -c core.fsmonitor=false. Neither
    # this command value nor the worktree's false may erase the common entry.
    before = _configuration_digests((*paths["trusted"], *paths["source"]))
    with pytest.raises(SourcePreservationError) as error:
        engine._environment(source)
    assert error.value.code == "SOURCE_PRESERVATION_FILTER"
    assert "SYNTHETIC_NEVER_EXECUTE" not in str(error.value)
    assert _configuration_digests((*paths["trusted"], *paths["source"])) == before


@pytest.mark.parametrize("side", ["trusted", "source"])
@pytest.mark.parametrize("configuration_scope", ["common", "worktree"])
def test_configuration_include_is_rejected_without_opening_its_poison_target(
    configuration_case: tuple[SourcePreservation, Path, dict[str, tuple[Path, Path]]],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    side: str,
    configuration_scope: str,
) -> None:
    engine, source, paths = configuration_case
    root = engine.project_root if side == "trusted" else source
    _git(root, "config", "extensions.worktreeConfig", "true")
    poison = tmp_path / "synthetic-include-must-not-be-read"
    poison.write_bytes(b"[deliberately invalid Git configuration\n")
    scope = "--local" if configuration_scope == "common" else "--worktree"
    _git(root, "config", scope, "include.path", poison.as_posix())
    before = _configuration_digests((*paths["trusted"], *paths["source"]))
    actual_open = Path.open
    actual_run = subprocess.run
    config_calls: list[bool] = []

    def poison_read(path: Path, *args: Any, **kwargs: Any) -> Any:
        assert path != poison, "configuration include target was opened"
        return actual_open(path, *args, **kwargs)

    def audited_run(command: list[str], *args: Any, **kwargs: Any) -> Any:
        if command and command[0] == "git" and "config" in command:
            config_index = command.index("config")
            assert "--no-pager" in command[1:config_index]
            assert "--no-includes" in command[config_index + 1 :]
            config_calls.append(True)
        # Keep the real Git parser: this is an argv assertion, not a successful
        # subprocess substitute or a file-level configuration prescan.
        return actual_run(command, *args, **kwargs)

    monkeypatch.setattr(Path, "open", poison_read)
    monkeypatch.setattr(subprocess, "run", audited_run)
    with pytest.raises(SourcePreservationError) as error:
        engine._environment(source)
    # Git following the malformed include would return a Git/parse error, not
    # this explicit pre-inclusion policy rejection. No included file is copied.
    assert error.value.code == "SOURCE_PRESERVATION_FILTER"
    assert config_calls
    assert _configuration_digests((*paths["trusted"], *paths["source"])) == before


def test_invalid_worktree_configuration_boolean_is_typed_and_never_repaired(
    configuration_case: tuple[SourcePreservation, Path, dict[str, tuple[Path, Path]]],
) -> None:
    engine, source, paths = configuration_case
    _git(source, "config", "extensions.worktreeConfig", "SYNTHETIC_INVALID_BOOLEAN")
    before = _configuration_digests((*paths["trusted"], *paths["source"]))
    with pytest.raises(SourcePreservationError) as error:
        engine._environment(source)
    assert error.value.code == "SOURCE_PRESERVATION_ENVIRONMENT"
    assert "SYNTHETIC_INVALID_BOOLEAN" not in str(error.value)
    assert _configuration_digests((*paths["trusted"], *paths["source"])) == before


@pytest.mark.parametrize("mutation", ["bytes", "missing_to_present", "extension"])
def test_configuration_recheck_rejects_changed_or_new_file_without_recapture(
    configuration_case: tuple[SourcePreservation, Path, dict[str, tuple[Path, Path]]],
    mutation: str,
) -> None:
    engine, source, paths = configuration_case
    _git(source, "config", "extensions.worktreeConfig", "true")
    snapshot = engine._environment(source)
    if mutation == "bytes":
        common = paths["source"][0]
        common.write_bytes(common.read_bytes() + b"\n# synthetic safe-byte drift\n")
    elif mutation == "missing_to_present":
        paths["source"][1].write_bytes(b"[diff]\n\talgorithm = patience\n")
    else:
        _git(source, "config", "extensions.worktreeConfig", "false")
    changed = _configuration_digests((*paths["trusted"], *paths["source"]))
    with pytest.raises(SourcePreservationError) as error:
        engine._recheck_environment(source, snapshot)
    assert error.value.code == "SOURCE_PRESERVATION_DRIFT"
    assert _configuration_digests((*paths["trusted"], *paths["source"])) == changed


def test_configuration_reparse_attribute_is_rejected_before_file_bytes_are_read(
    configuration_case: tuple[SourcePreservation, Path, dict[str, tuple[Path, Path]]],
    monkeypatch: pytest.MonkeyPatch,
    request: pytest.FixtureRequest,
) -> None:
    engine, source, paths = configuration_case
    target = paths["source"][0]
    actual_lstat = Path.lstat
    actual_open = Path.open
    request.node.user_properties.append(
        ("reparse_probe", "SYNTHETIC_LSTAT_ATTRIBUTE_NOT_OS_LINK_CREATION")
    )

    def marked_reparse(path: Path) -> Any:
        info = actual_lstat(path)
        if path == target:
            return SimpleNamespace(st_mode=info.st_mode, st_file_attributes=0x400)
        return info

    def forbidden_read(path: Path, *args: Any, **kwargs: Any) -> Any:
        assert path != target, "reparse configuration bytes must not be opened"
        return actual_open(path, *args, **kwargs)

    monkeypatch.setattr(Path, "lstat", marked_reparse)
    monkeypatch.setattr(Path, "open", forbidden_read)
    with pytest.raises(SourcePreservationError) as error:
        engine._environment(source)
    assert error.value.code == "SOURCE_PRESERVATION_PATH"


def test_enabled_worktree_config_preserves_real_snapshot_source_index_refs_and_configs(
    source_case: SourceCase,
    unit_engine: SourcePreservation,
) -> None:
    paths = _configuration_files(source_case.root)
    _git(source_case.root, "config", "extensions.worktreeConfig", "true")
    paths[1].write_bytes(b"[diff]\n\talgorithm = patience\n")
    before = _configuration_digests(paths)
    receipt_path, _ = _assert_snapshot(source_case, unit_engine)
    receipt = json.loads(receipt_path.read_bytes())
    assert receipt["source_state_before"]["git_configuration"]["source"][
        "worktree_config_mode"
    ] == ("TRUE")
    assert _configuration_digests(paths) == before
    receipt_sha = _sha(receipt_path.read_bytes())
    # A later legitimate user configuration change is not retroactive source
    # drift. Independent validation binds the retained execution capture only.
    paths[1].write_bytes(paths[1].read_bytes() + b"\n# later synthetic configuration change\n")
    later = _configuration_digests(paths)
    assert unit_engine.validate(receipt_path)["status"] == "PASS"
    assert _sha(receipt_path.read_bytes()) == receipt_sha
    assert _configuration_digests(paths) == later


@pytest.mark.parametrize("boundary", ["guard_construction", "first_mutation"])
@pytest.mark.parametrize("mutation", ["bytes", "missing_to_present"])
def test_configuration_drift_stops_before_legacy_guard_or_first_lease_mutation(
    source_case: SourceCase,
    unit_engine: SourcePreservation,
    monkeypatch: pytest.MonkeyPatch,
    boundary: str,
    mutation: str,
) -> None:
    common, worktree = _configuration_files(source_case.root)
    _git(source_case.root, "config", "extensions.worktreeConfig", "true")
    before = _unchanged_state(source_case)
    actual_binding = unit_engine._implementation_binding
    actual_state = unit_engine._state
    changed = False
    changed_digest: dict[str, tuple[int, str] | None] = {}

    def change_once() -> None:
        nonlocal changed
        if not changed:
            if mutation == "bytes":
                common.write_bytes(common.read_bytes() + b"\n# synthetic boundary drift\n")
            else:
                worktree.write_bytes(b"[diff]\n\talgorithm = patience\n")
            changed_digest.update(_configuration_digests((common, worktree)))
            changed = True

    def binding_then_drift() -> dict[str, Any]:
        result = actual_binding()
        change_once()
        return result

    def state_then_drift(*args: Any, **kwargs: Any) -> Any:
        result = actual_state(*args, **kwargs)
        change_once()
        return result

    def forbidden(*args: Any, **kwargs: Any) -> Any:
        pytest.fail(f"configuration drift crossed {boundary} boundary")

    if boundary == "guard_construction":
        monkeypatch.setattr(unit_engine, "_implementation_binding", binding_then_drift)
        monkeypatch.setattr(preservation, "IntegrationPublicationFence", forbidden)
    else:
        monkeypatch.setattr(unit_engine, "_state", state_then_drift)
        monkeypatch.setattr(CheckoutLeaseGuard, "acquire", forbidden)
    with pytest.raises(SourcePreservationError) as error:
        unit_engine.preserve(source_case.request)
    assert error.value.code == "SOURCE_PRESERVATION_DRIFT"
    assert changed
    assert not source_case.destination.exists()
    assert _unchanged_state(source_case) == before
    assert _configuration_digests((common, worktree)) == changed_digest


def test_config_drift_after_acquire_retains_active_lease_without_unsafe_failure_cleanup(
    source_case: SourceCase,
    unit_engine: SourcePreservation,
    monkeypatch: pytest.MonkeyPatch,
    request: pytest.FixtureRequest,
) -> None:
    config = _configuration_files(source_case.root)[0]
    original = config.read_bytes()
    before = _unchanged_state(source_case)
    acquire = CheckoutLeaseGuard.acquire
    actual_run = subprocess.run
    captured: list[tuple[Any, Any]] = []
    after_drift = False
    release_calls: list[str] = []

    def forbidden_release(*args: Any, **kwargs: Any) -> None:
        release_calls.append("release")
        pytest.fail("failure cleanup must not enter a legacy guard after config drift")

    def acquire_then_drift(guard: CheckoutLeaseGuard, *args: Any, **kwargs: Any) -> Any:
        nonlocal after_drift
        decision, handle = acquire(guard, *args, **kwargs)
        assert decision.status == "PASS" and handle is not None
        captured.append((handle, handle.release))
        monkeypatch.setattr(handle, "release", forbidden_release)
        config.write_bytes(original + b"\n# synthetic post-acquire configuration drift\n")
        after_drift = True
        return decision, handle

    def no_subprocess_after_drift(*args: Any, **kwargs: Any) -> Any:
        assert not after_drift, "changed configuration reached a subprocess during failure cleanup"
        return actual_run(*args, **kwargs)

    monkeypatch.setattr(CheckoutLeaseGuard, "acquire", acquire_then_drift)
    monkeypatch.setattr(subprocess, "run", no_subprocess_after_drift)
    try:
        with pytest.raises(SourcePreservationError) as error:
            unit_engine.preserve(source_case.request)
        assert error.value.code == "SOURCE_PRESERVATION_DRIFT"
        assert len(captured) == 1
        handle = captured[0][0]
        assert handle.released is False
        replay = handle.guard.replay()
        assert replay.status == "PASS"
        assert [lease.lease_id for lease in replay.active_leases] == [handle.lease_id]
        assert release_calls == []
        assert not source_case.destination.exists()
        request.node.user_properties.append(
            ("unsafe_cleanup_observation", "ACTIVE_LEASE_RETAINED_BEFORE_SYNTHETIC_TEST_CLEANUP")
        )
    finally:
        # Restore only this fixture's deliberately changed config, then release
        # its actual synthetic lease. Production does neither as an automatic fix.
        config.write_bytes(original)
        after_drift = False
        for handle, release in captured:
            if not handle.released:
                release(outcome="failed")
    assert _unchanged_state(source_case) == before
    assert _sha(config.read_bytes()) == _sha(original)


@pytest.mark.parametrize("source_selected", [False, True])
def test_migration_cli_rechecks_the_root_bound_by_its_existing_admission(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    source_selected: bool,
) -> None:
    # Control-flow only: this verifies root selection, not fence or configuration
    # validity. The real configuration and admission checks are tested separately.
    cli = _migration_cli()
    source = tmp_path / "synthetic-admitted-source"
    snapshot = {"profile": "SYNTHETIC_CONTROL_FLOW_ONLY"}
    observed: list[tuple[Path, Any]] = []

    class ObservedEnvironment:
        def __init__(self, root: Path) -> None:
            assert root == cli.PROJECT_ROOT

        def _recheck_environment(self, root: Path, expected: Any) -> None:
            observed.append((root, expected))

    monkeypatch.setattr(cli, "SourcePreservation", ObservedEnvironment)
    cli._recheck_admission_environment(
        {
            "source_admission": {"source_root": source.as_posix()} if source_selected else None,
            "git_configuration": snapshot,
        }
    )
    assert observed == [(source if source_selected else cli.PROJECT_ROOT, snapshot)]
    assert observed[0][1] is snapshot


@pytest.mark.parametrize("failed_recheck", [1, 2], ids=["before_first_mkdir", "before_helper"])
def test_migration_cli_configuration_drift_stops_before_its_next_mutation_boundary(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    failed_recheck: int,
) -> None:
    # A fixed CLI ordering probe only. The synthetic admission cannot authorize
    # any real migration; the real migration helper is forbidden in both cases.
    cli = _migration_cli()
    root = tmp_path / "synthetic-cli-root"
    runtime = root / "outputs/validation_runtime"
    runtime.mkdir(parents=True)
    monkeypatch.setattr(cli, "PROJECT_ROOT", root)
    admission = {"profile": "SYNTHETIC_CONTROL_FLOW_ONLY"}
    monkeypatch.setattr(cli, "_admit", lambda _: (tmp_path / "unused-store", admission))
    checks: list[int] = []

    def recheck(value: dict[str, Any]) -> None:
        assert value is admission
        checks.append(len(checks) + 1)
        if len(checks) == failed_recheck:
            raise SourcePreservationError("SOURCE_PRESERVATION_DRIFT", "synthetic config drift")

    def forbidden_helper(*args: Any, **kwargs: Any) -> None:
        pytest.fail("migration helper must not run after a configuration recheck fails")

    monkeypatch.setattr(cli, "_recheck_admission_environment", recheck)
    monkeypatch.setattr(cli.lease_arbiter, "migrate_legacy_arbiter", forbidden_helper)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "synthetic-lease-arbiter-cli",
            "--publication-transaction",
            "unused-transaction.json",
            "--migration-id",
            "synthetic-config-drift",
            "--owner-instruction-ref",
            cli.OWNER_REF,
            "--expected-owner-sha256",
            "1" * 64,
            "--quiescence-receipt",
            "unused-quiescence.json",
            "--quiescence-receipt-sha256",
            "2" * 64,
            "--reviewed-code",
            "unused-reviewed-code.json",
        ],
    )
    assert cli.main() == 2
    result = json.loads(capsys.readouterr().out)
    assert result["reason_code"] == "SOURCE_PRESERVATION_DRIFT"
    assert checks == list(range(1, failed_recheck + 1))
    parent = runtime / "lease-arbiter-migration-admission"
    if failed_recheck == 1:
        assert not parent.exists()
    else:
        run = parent / "synthetic-config-drift"
        assert sorted(path.name for path in run.iterdir()) == ["admission.json", "failure.json"]
        assert not (run / "result.json").exists()


@pytest.mark.parametrize("side", ["trusted", "source"])
@pytest.mark.parametrize("configuration_scope", ["common", "worktree"])
@pytest.mark.parametrize("key", ["fsmonitor", "sshCommand", "gitProxy"])
def test_valueless_executable_configuration_is_rejected_before_any_guard(
    configuration_case: tuple[SourcePreservation, Path, dict[str, tuple[Path, Path]]],
    monkeypatch: pytest.MonkeyPatch,
    side: str,
    configuration_scope: str,
    key: str,
) -> None:
    engine, source, paths = configuration_case
    root = engine.project_root if side == "trusted" else source
    _git(root, "config", "extensions.worktreeConfig", "true")
    target = paths[side][0 if configuration_scope == "common" else 1]
    existing = target.read_bytes() if target.exists() else b""
    # Raw fixture bytes deliberately omit "=". Do not use a setter that could
    # normalize this implicit value into an explicitly empty configuration value.
    target.write_bytes(existing + f"\n[core]\n\t{key}\n".encode("ascii"))
    before = _configuration_digests((*paths["trusted"], *paths["source"]))

    # Shape-valid PRE_ENVIRONMENT_REJECTION_ONLY request. The roots are real
    # synthetic Git checkouts, but these commits, terminal evidence and source
    # member are intentionally not a complete source-identity fixture. This case
    # must fail through the unchanged _request -> _environment entry sequence;
    # it must never bind implementation identity, construct a guard or preserve.
    request: dict[str, Any] = {
        "schema_version": "source_preservation_request.v1",
        "preservation_id": "synthetic-config-rejection-only",
        "recovery_task_id": RECOVERY_TASK,
        "source_task_id": "SYNTHETIC_CONFIGURATION_REJECTION_ONLY",
        "owner_instruction_ref": OWNER_REF,
        "actor": ACTOR,
        "thread_id": "synthetic-configuration-rejection-thread",
        "source_root": source.as_posix(),
        "source_common_git_dir": paths["source"][0].parent.as_posix(),
        "source_branch": BRANCH,
        "frozen_base_sha": "1" * 40,
        "source_head_sha": "1" * 40,
        "observed_main_sha": "1" * 40,
        "observed_origin_main_sha": None,
        "terminal_transaction": {
            "path": (
                "outputs/architecture/arch_005_integration_publication_fence/"
                "transactions/synthetic-configuration-only/transaction.json"
            ),
            "sha256": "2" * 64,
            "closeout_sha256": "3" * 64,
        },
        "files": [
            {
                "path": "src/synthetic_configuration_only.py",
                "sha256": _sha(b""),
                "size_bytes": 0,
                "git_mode": "100644",
            }
        ],
    }
    request_sha = _sha(_json(request))
    reached: list[str] = []

    def forbidden_binding() -> dict[str, Any]:
        reached.append("implementation_binding")
        pytest.fail("implicit executable configuration crossed environment admission")

    def forbidden_fence(*args: Any, **kwargs: Any) -> Any:
        reached.append("guard")
        pytest.fail("implicit executable configuration reached a legacy guard")

    monkeypatch.setattr(engine, "_implementation_binding", forbidden_binding)
    monkeypatch.setattr(preservation, "IntegrationPublicationFence", forbidden_fence)
    with pytest.raises(SourcePreservationError) as error:
        engine.preserve(request)
    assert error.value.code == "SOURCE_PRESERVATION_FILTER"
    assert reached == []
    assert _sha(_json(request)) == request_sha
    assert _configuration_digests((*paths["trusted"], *paths["source"])) == before
    assert not (source / RUNTIME / request["preservation_id"]).exists()


@pytest.mark.parametrize("value", ["", "false", "0"], ids=["explicit_empty", "false", "zero"])
def test_explicit_non_executable_fsmonitor_values_remain_admitted(
    configuration_case: tuple[SourcePreservation, Path, dict[str, tuple[Path, Path]]],
    value: str,
) -> None:
    engine, source, paths = configuration_case
    _git(source, "config", "extensions.worktreeConfig", "true")
    target = paths["source"][1]
    # The separator is present even when the value is empty. These are exactly
    # the existing accepted forms; this test does not add "no" or "off" support.
    target.write_bytes(f"[core]\n\tfsmonitor = {value}\n".encode("ascii"))
    before = _configuration_digests((*paths["trusted"], *paths["source"]))
    snapshot = engine._environment(source)
    _assert_configuration_capture(snapshot, engine, source, paths)
    assert snapshot["source"]["worktree_config_mode"] == "TRUE"
    engine._recheck_environment(source, snapshot)
    assert _configuration_digests((*paths["trusted"], *paths["source"])) == before
