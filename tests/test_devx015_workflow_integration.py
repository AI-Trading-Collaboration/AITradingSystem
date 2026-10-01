"""Real Git/source-authority acceptance for new controlled merge APIs."""

from __future__ import annotations

import ast
import copy
import hashlib
import inspect
import json
import os
import shutil
import struct
import subprocess
import sys
import uuid
from pathlib import Path

import pytest

from ai_trading_system.platform.architecture import task_registry_canonical as canonical
from ai_trading_system.platform.architecture import workflow_contract as contract
from ai_trading_system.platform.architecture import workflow_integration as integration
from ai_trading_system.platform.architecture.integration_publication_fence import (
    IntegrationPublicationFence,
    PublicationFenceError,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

ROOT = Path(__file__).resolve().parents[1]
TASK = "DEVX-015-MERGE-FIXTURE"


@pytest.mark.skipif(os.name != "nt", reason="Windows native ACL access check")
def test_bound_result_write_does_not_require_delete_permission(tmp_path: Path) -> None:
    import ctypes
    from ctypes import wintypes as w

    from ai_trading_system.platform.architecture import workflow_execution as execution

    root = tmp_path / "exchange"
    root.mkdir()
    output = root / "result.json"
    output.write_bytes(b"")
    root_info, leaf_info = root.stat(), output.stat()
    api = execution._api()
    sid = execution._process_primary_token(api, api.GetCurrentProcess())["sid"]
    security = ctypes.WinDLL("advapi32", use_last_error=True)
    security.GetFileSecurityW.argtypes = [w.LPCWSTR, w.DWORD, ctypes.c_void_p,
                                         w.DWORD, ctypes.POINTER(w.DWORD)]
    security.GetFileSecurityW.restype = w.BOOL
    security.SetFileSecurityW.argtypes = [w.LPCWSTR, w.DWORD, ctypes.c_void_p]
    security.SetFileSecurityW.restype = w.BOOL
    originals = []
    for path in (root, output):
        size = w.DWORD()
        security.GetFileSecurityW(str(path), 4, None, 0, ctypes.byref(size))
        assert size.value > 0
        descriptor = ctypes.create_string_buffer(size.value)
        assert security.GetFileSecurityW(str(path), 4, descriptor, size.value,
                                         ctypes.byref(size))
        originals.append((path, descriptor))
    try:
        # DE is exact DELETE; icacls basic D also denies SYNCHRONIZE.
        for path, right in ((root, "DC"), (output, "DE")):
            subprocess.run(["icacls", str(path), "/deny", f"*{sid}:({right})"],
                           check=True, capture_output=True)
        with output.open("r+b") as readable_writer:
            assert readable_writer.read() == b""
        contract.apply_bound_file(
            root, output.name, b"", b"worker result",
            expected_identity=(leaf_info.st_dev, leaf_info.st_ino),
            expected_root_identity=(root_info.st_dev, root_info.st_ino),
        )
        assert output.read_bytes() == b"worker result"
        with pytest.raises(OSError):
            contract.apply_bound_file(
                root, output.name, b"worker result", None,
                expected_identity=(leaf_info.st_dev, leaf_info.st_ino),
            )
        with pytest.raises(PermissionError):
            output.unlink()
        assert output.read_bytes() == b"worker result"
    finally:
        for path, descriptor in originals:
            assert security.SetFileSecurityW(str(path), 4, descriptor)
SCOPE_PATH = "config/architecture/merge_fixture_scope.json"


def test_source_generation_large_roundtrip_keeps_ordinary_budget_and_hash_binding(tmp_path):
    value = {
        "schema_version": "rendered_source_candidate_delta.v1",
        "padding": "x" * (17 * 1024 * 1024),
    }
    content = integration._encode_source_generation(value)
    path = tmp_path / "generation.json"
    path.write_bytes(content)
    digest = hashlib.sha256(content).hexdigest()
    assert 16 * 1024 * 1024 < len(content) < 64 * 1024 * 1024
    with pytest.raises(contract.WorkflowContractError, match="ARTIFACT_BUDGET"):
        contract.bounded_regular_bytes(path)
    with pytest.raises(contract.WorkflowContractError, match="ARTIFACT_BUDGET"):
        contract.read_bound_json(tmp_path, {"path": path.name, "sha256": digest})
    assert integration._source_generation_bytes(tmp_path) == content
    assert integration._read_source_generation(tmp_path, digest) == value
    with pytest.raises(contract.WorkflowContractError, match="REFERENCE_DRIFT"):
        integration._read_source_generation(tmp_path, "0" * 64)
    assert path.read_bytes() == content


@pytest.mark.parametrize("offset", [-1, 0, 1])
def test_source_generation_producer_exact_budget_before_effects(tmp_path, offset):
    budget = 64 * 1024 * 1024
    assert integration._SOURCE_GENERATION_BUDGET == budget
    overhead = len(json.dumps({"padding": ""}, separators=(",", ":")).encode() + b"\n")
    value = {"padding": "x" * (budget + offset - overhead)}
    if offset > 0:
        with pytest.raises(contract.WorkflowContractError, match="SOURCE_GENERATION_BUDGET"):
            integration._encode_source_generation(value)
    else:
        content = integration._encode_source_generation(value)
        assert len(content) == budget + offset
        assert json.loads(content) == value
    assert list(tmp_path.iterdir()) == []


def test_source_generation_reader_rejects_aggregate_overflow(tmp_path):
    path = tmp_path / "generation.json"
    content = b"x" * (64 * 1024 * 1024 + 1)
    path.write_bytes(content)
    digest = hashlib.sha256(content).hexdigest()
    with pytest.raises(contract.WorkflowContractError, match="ARTIFACT_BUDGET"):
        integration._read_source_generation(tmp_path, digest)
    assert path.stat().st_size == len(content)
    assert hashlib.sha256(path.read_bytes()).hexdigest() == digest


@pytest.mark.parametrize("excluded_kind", ["file", "directory"])
def test_candidate_source_scan_omits_declared_exclusion_before_open_or_descent(
    small_repository, monkeypatch, excluded_kind
):
    root = small_repository
    allowed = root / "docs/allowed.md"
    allowed.parent.mkdir()
    allowed.write_bytes(b"reviewed source\n")
    _git(root, "add", "docs/allowed.md")
    _git(root, "commit", "-m", "synthetic known source")
    main = _git(root, "rev-parse", "HEAD")
    relative = "docs/research/growth_tilt_owner_diagnosis_pack.md"
    policy = safe_load_yaml_path(root / "config/architecture/arch_005_s4d_checkout_guard.yaml")
    assert relative in {row["path"] for row in policy["known_unrelated_exclusions"]}
    excluded = root / relative
    excluded.parent.mkdir()
    if excluded_kind == "directory":
        excluded.mkdir()
        canary = excluded / "never-read.md"
    else:
        canary = excluded
    canary.write_bytes(b"synthetic private canary\0\r\n")
    index_before = (root / ".git/index").read_bytes()
    refs_before = _git(root, "for-each-ref", "--format=%(refname) %(objectname)")
    calls = []

    def guarded(original):
        def call(path, *args, **kwargs):
            if not isinstance(path, int):
                absolute = Path(path).absolute()
                if absolute == excluded or excluded in absolute.parents:
                    calls.append(str(path))
                    raise AssertionError("excluded source opened or traversed")
            return original(path, *args, **kwargs)

        return call

    with monkeypatch.context() as hooks:
        hooks.setattr(Path, "open", guarded(Path.open))
        hooks.setattr(os, "open", guarded(os.open))
        hooks.setattr(os, "scandir", guarded(os.scandir))
        names = integration._candidate_scan_admission(
            root, main, {"candidate_sources": [], "resolutions": []}
        )
    assert "docs/allowed.md" in names
    assert not any(name == relative or name.startswith(relative + "/") for name in names)
    assert calls == []
    assert canary.read_bytes() == b"synthetic private canary\0\r\n"
    assert (root / ".git/index").read_bytes() == index_before
    assert _git(root, "rev-parse", "HEAD") == main
    assert _git(root, "for-each-ref", "--format=%(refname) %(objectname)") == refs_before


@pytest.mark.parametrize("action", ["INSTALL", "RECOVER"])
def test_installation_stable_verifier_rejects_same_bytes_replacement(tmp_path, monkeypatch, action):
    # A narrow verifier seam, not public Job/lease acceptance: freeze context,
    # but exercise actual files and native held-handle identity independently.
    path = tmp_path / "original.bin"
    content = b"frozen target"
    path.write_bytes(content)
    original = path.stat()
    root_info = tmp_path.stat()
    root_identity = (root_info.st_dev, root_info.st_ino)
    row = {
        "path": path.name,
        "before_hex": content.hex(),
        "after_hex": content.hex(),
        "identity": [original.st_dev, original.st_ino],
    }
    plan = {
        "schema_version": "controlled_source_installation_plan.v1",
        "main": "a" * 40,
        "candidate": "b" * 40,
    }
    monkeypatch.setattr(
        integration, "_installation_context", lambda *args: (plan, [(tmp_path, root_identity, row)])
    )
    path.rename(tmp_path / "preserved.bin")
    path.write_bytes(content)
    assert path.stat().st_ino != original.st_ino
    with pytest.raises(contract.WorkflowContractError, match="HANDLE_EXPECTED_IDENTITY_CHANGED"):
        integration._verify_source_installation(
            tmp_path, {"installation_action": action}, {}, "fixture"
        )
    assert path.read_bytes() == (tmp_path / "preserved.bin").read_bytes() == content


def test_installation_identity_resolves_original_attempt_history_and_rejects_tamper(tmp_path):
    root_info = tmp_path.stat()
    root_identity = (root_info.st_dev, root_info.st_ino)
    row = {
        "path": "created.bin",
        "before_hex": None,
        "after_hex": b"target".hex(),
        "identity": None,
    }
    record = {
        "root": tmp_path.as_posix(),
        "path": "created.bin",
        "root_identity": list(root_identity),
        "plan_sha256": "a" * 64,
        "target_sha256": hashlib.sha256(b"target").hexdigest(),
        "file_identity": [root_info.st_dev, 123],
    }
    source = {
        "installation_attempts": [
            {
                "request": {"installation_action": "INSTALL", "installation_plan_sha256": "a" * 64},
                "created_objects": [record],
            }
        ]
    }
    assert integration._installation_object_identity(source, tmp_path, root_identity, row) == (
        root_info.st_dev,
        123,
    )
    assert (
        integration._installation_object_identity(
            source, tmp_path, root_identity, row, initial=True
        )
        is None
    )
    for field, value in (
        ("plan_sha256", "b" * 64),
        ("target_sha256", "b" * 64),
        ("root_identity", [0, 0]),
    ):
        changed = copy.deepcopy(source)
        changed["installation_attempts"][0]["created_objects"][0][field] = value
        with pytest.raises(contract.WorkflowContractError, match="INSTALLATION_CREATION_BINDING"):
            integration._installation_object_identity(changed, tmp_path, root_identity, row)


def test_installation_directory_plan_exact_scope_and_native_recovery(tmp_path):
    gitdir = tmp_path / ".git"
    (gitdir / "refs/heads").mkdir(parents=True)

    def binding(path):
        return {
            "path": path.as_posix(),
            "device": path.stat().st_dev,
            "file_id": path.stat().st_ino,
        }

    plan = {
        "schema_version": "controlled_source_installation_plan.v2",
        "root_identity": binding(tmp_path),
        "gitdir_identity": binding(gitdir),
        "common_identity": binding(gitdir),
        "branch": "refs/heads/task",
        "files": [{"path": "new/deeper/target.bin"}],
    }
    names = integration._installation_directory_names(plan)
    assert set(names) == {
        ("root_identity", "new"),
        ("root_identity", "new/deeper"),
        ("common_identity", "refs"),
        ("common_identity", "refs/heads"),
    }
    plan["directories"] = []
    for key, name in names:
        path = Path(plan[key]["path"]) / name
        info = path.stat() if path.exists() else None
        plan["directories"].append(
            {
                "root_key": key,
                "path": name,
                "identity": None if info is None else [info.st_dev, info.st_ino],
            }
        )
    integration._installation_directories(plan, {}, initial=True, restored=True)
    for changed in (
        plan["directories"][:-1],
        [
            *plan["directories"],
            {"root_key": "root_identity", "path": "unrelated", "identity": None},
        ],
    ):
        with pytest.raises(contract.WorkflowContractError, match="INSTALLATION_DIRECTORY_SET"):
            integration._installation_directory_rows({**plan, "directories": changed})
    with pytest.raises(contract.WorkflowContractError, match="INSTALLATION_DIRECTORY_LEGACY"):
        integration._installation_directory_rows(
            {**plan, "schema_version": "controlled_source_installation_plan.v1"}
        )
    source = {
        "installation_attempts": [
            {"request": {"installation_plan_sha256": "a" * 64}, "created_objects": []}
        ]
    }
    for row in plan["directories"]:
        if row["identity"] is not None:
            continue
        path = tmp_path / row["path"]
        path.mkdir()
        info = path.stat()
        source["installation_attempts"][0]["created_objects"].append(
            {
                "kind": "directory",
                "root": tmp_path.as_posix(),
                "path": row["path"],
                "root_identity": [tmp_path.stat().st_dev, tmp_path.stat().st_ino],
                "file_identity": [info.st_dev, info.st_ino],
                "plan_sha256": "a" * 64,
            }
        )
    integration._installation_directories(plan, source)
    canary = tmp_path / "new/deeper/unknown.bin"
    canary.write_bytes(b"unowned")
    with pytest.raises(OSError):
        integration._installation_directories(plan, source, restored=True, remove=True)
    assert canary.read_bytes() == b"unowned"
    # Test owns this canary; move it out, never ask the SUT to erase unknown data.
    canary.rename(tmp_path / "preserved.bin")
    integration._installation_directories(plan, source, restored=True, remove=True)
    integration._installation_directories(plan, source, restored=True)
    assert not (tmp_path / "new").exists()
    assert (gitdir / "refs/heads").is_dir()
    assert (tmp_path / "preserved.bin").read_bytes() == b"unowned"


def _git(root: Path, *args: str, content: bytes | None = None, index: Path | None = None) -> str:
    env = {key: value for key, value in os.environ.items() if not key.startswith("GIT_")}
    env.update(GIT_CONFIG_GLOBAL=os.devnull, GIT_CONFIG_NOSYSTEM="1", GIT_OPTIONAL_LOCKS="0")
    if index is not None:
        env["GIT_INDEX_FILE"] = str(index)
    result = subprocess.run(
        ["git", "-C", str(root), *args], input=content, capture_output=True, env=env, timeout=30
    )
    assert result.returncode == 0, result.stderr.decode(errors="replace")
    return result.stdout.decode().strip()


def _json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, sort_keys=True), encoding="utf-8")


def _tree_commit(root: Path, parent: str, changes: dict[str, tuple[str, bytes] | None]) -> str:
    index = root / ".git" / ("test-index-" + uuid.uuid4().hex)
    _git(root, "read-tree", parent, index=index)
    for path, value in changes.items():
        if value is None:
            _git(root, "update-index", "--force-remove", "--", path, index=index)
        else:
            mode, content = value
            if mode == "160000":
                oid = content.decode("ascii")
                assert _git(root, "cat-file", "-t", oid) == "commit"
            else:
                oid = _git(root, "hash-object", "-w", "--stdin", content=content)
            _git(root, "update-index", "--add", "--cacheinfo", mode, oid, path, index=index)
    tree = _git(root, "write-tree", index=index)
    return _git(root, "commit-tree", tree, "-p", parent, "-m", "synthetic source history")


def _expected(root: Path, commit: str, path: str) -> dict:
    # Independent Git plumbing oracle, not implementation inventory helpers.
    value = _git(root, "ls-tree", commit, "--", path)
    if not value:
        return {"exists": False, "mode": None, "type": None, "oid": None}
    metadata, actual_path = value.split("\t")
    mode, kind, oid = metadata.split()
    assert actual_path == path
    return {"exists": True, "mode": mode, "type": kind, "oid": oid}


def _scope(root: Path, base: str, lane: str, main: str, paths: list[str]) -> dict:
    return {
        "schema_version": "workflow_merge_scope.v1",
        "task_id": TASK,
        "decision_id": "owner_decision:DEVX-015:merge-fixture",
        "repository_common": (root / ".git").resolve().as_posix(),
        "frozen_base": base,
        "lane_head": lane,
        "latest_main": main,
        "source_commits": _git(root, "rev-list", "--reverse", base + ".." + lane).splitlines(),
        "source_paths": sorted(paths),
        "keep_current_paths": ["keep.txt"] if "keep.txt" in paths else [],
        "generated_paths": {"generated.txt": "fixture-generator"}
        if "generated.txt" in paths
        else {},
        "generator_order": ["fixture-generator"],
        "contract_claims": [
            {
                "contract_id": "EXACT_OBJECT_CUSTODY",
                "version": "v1",
                "paths": sorted(paths),
                "required_acceptance": [
                    "exact mode, presence and object identity",
                    "complete source history",
                ],
            }
        ],
    }


@pytest.fixture
def small_repository(tmp_path: Path) -> Path:
    root = tmp_path / "git-fixture"
    root.mkdir()
    _git(root, "init", "-b", "main")
    _git(root, "config", "user.name", "Merge fixture")
    _git(root, "config", "user.email", "fixture@example.invalid")
    _git(root, "config", "core.autocrlf", "false")
    # Mirror the real repository-level setting: fixture Git ignores global config,
    # and copied long requirement paths under default pytest tmp exceed MAX_PATH.
    _git(root, "config", "core.longpaths", "true")
    _git(
        root,
        "remote",
        "add",
        "origin",
        "https://github.com/AI-Trading-Collaboration/AITradingSystem.git",
    )
    policy = root / "config/architecture/arch_005_s4d_checkout_guard.yaml"
    policy.parent.mkdir(parents=True)
    shutil.copyfile(ROOT / policy.relative_to(root), policy)
    (root / ".gitignore").write_bytes(b"outputs/\n")
    _git(root, "add", ".")
    _git(root, "commit", "-m", "fixture genesis")
    return root


@pytest.mark.parametrize(
    "case", ["equivalent", "unchanged", "missing-index", "bad-identity", "bad-size",
             "bad-original-digest", "bad-observed-digest", "changed-main", "wrong-tree"],
)
def test_failed_publication_index_replacement_topology_contract(small_repository, case):
    """Real index replacement plus malformed-record refusals; not Full/adoption evidence."""
    from ai_trading_system.platform.architecture.parallel_control import ParallelControlError
    from ai_trading_system.platform.architecture.workflow_coordination import (
        _require_index_replaced_topology,
    )

    root = small_repository
    for name in ("scripts/architecture_arch005_checkout_guard.py",
                 "docs/requirements/DEVX-002_Governed_Development_Workflow_Skill.md"):
        destination = root / name
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(ROOT / name, destination)
    _git(root, "add", "scripts", "docs")
    _git(root, "commit", "-m", "index recovery original sentinels")
    main = _git(root, "rev-parse", "HEAD")
    _git(root, "switch", "-c", "codex/index-recovery-contract")
    (root / "value.txt").write_bytes(b"candidate\n")
    _git(root, "add", "value.txt")
    _git(root, "commit", "-m", "index recovery candidate")
    candidate = _git(root, "rev-parse", "HEAD")
    original = integration.inspect_local_publication_topology(
        root, candidate=candidate, expected_main=main,
    )
    index = root / ".git/index"
    original_bytes = index.read_bytes()
    if case != "unchanged":
        # This fixture owns this index; never repair a retained production index this way.
        replacement = root / ".git/test-replacement-index"
        replacement.write_bytes(original_bytes)
        replacement.replace(index)
    observed = integration.inspect_local_publication_topology(
        root, candidate=candidate, expected_main=main,
    )
    physical_before = index.stat()
    if case == "missing-index":
        observed["candidate_checkout"].pop("index")
    elif case == "bad-identity":
        observed["candidate_checkout"]["index"]["identity"] = [True, 1]
    elif case == "bad-size":
        observed["candidate_checkout"]["index"]["size"] = -1
    elif case == "changed-main":
        observed["expected_main_sha"] = "f" * 40
    elif case == "wrong-tree":
        observed["candidate_index_matches_tree"] = False
    observed["topology_sha256"] = contract.canonical_digest(
        {key: value for key, value in observed.items() if key != "topology_sha256"}
    )
    if case == "bad-original-digest":
        original["topology_sha256"] = "0" * 64
    if case == "bad-observed-digest":
        observed["topology_sha256"] = "0" * 64
    if case == "equivalent":
        _require_index_replaced_topology(original, observed)
        assert original["candidate_checkout"]["index"]["identity"] != (
            observed["candidate_checkout"]["index"]["identity"]
        )
    else:
        with pytest.raises(ParallelControlError, match="PUBLICATION_INDEX_"):
            _require_index_replaced_topology(original, observed)
    assert index.read_bytes() == original_bytes
    assert index.stat().st_ino == physical_before.st_ino
    assert _git(root, "rev-parse", "HEAD") == candidate
    assert _git(root, "rev-parse", "main") == main


@pytest.mark.parametrize("case", [
    "plain", "guarded", "main-advanced", "deny", "auto-lock-deny", "auto-lock-observer",
])
def test_git_ff_only_reference_transaction_with_candidate_index(small_repository, case):
    """Installed Git behavior only; no application publisher/Full/Job capability."""
    root = small_repository
    value = root / "value.txt"
    value.write_bytes(b"original main\n")
    _git(root, "add", "value.txt")
    _git(root, "commit", "-m", "reference probe original main")
    main = _git(root, "rev-parse", "HEAD")
    branch = "codex/ff-reference-candidate"
    _git(root, "switch", "-c", branch)
    value.write_bytes(b"intermediate\n")
    _git(root, "add", "value.txt")
    _git(root, "commit", "-m", "reference probe intermediate")
    intermediate = _git(root, "rev-parse", "HEAD")
    value.write_bytes(b"candidate\n")
    _git(root, "add", "value.txt")
    _git(root, "commit", "-m", "reference probe candidate")
    candidate = _git(root, "rev-parse", "HEAD")
    start = intermediate if case == "main-advanced" else main
    if case == "main-advanced":
        _git(root, "update-ref", "refs/heads/main", intermediate, main)
    # Deliberate owned-fixture intermediate state: no checkout/index rewrite.
    _git(root, "symbolic-ref", "HEAD", "refs/heads/main")
    hooks = root / "outputs/reference-probe-hooks"
    hooks.mkdir(parents=True)
    trace = hooks / "trace.jsonl"
    hook_plan = {"topology": {"candidate_checkout": {"common": {"path": str(root / ".git")}}}}
    program = (
        "import json,os,sys\nfrom pathlib import Path\n"
        "rows=[] if sys.argv[1]=='post-merge' else "
        "[line.split() for line in sys.stdin.read().splitlines()]\n"
        f"lock=Path({str(root / '.git/packed-refs.lock')!r})\n"
        "info=lock.lstat() if lock.exists() else None\n"
        "observed=None if info is None else "
        "{'identity':[info.st_dev,info.st_ino],'size':info.st_size,'hex':lock.read_bytes().hex()}\n"
        "cleanup=None\n"
        f"if {case == 'auto-lock-observer'!r} and rows and rows[0][2]=='AUTO_MERGE':\n"
        f" sys.path.insert(0,{str(ROOT / 'src')!r})\n"
        " from ai_trading_system.platform.architecture.workflow_integration "
        "import observe_publication_auto_merge_lock\n"
        f" old=[json.loads(line) for line in Path({str(trace)!r}).read_text().splitlines()]\n"
        " history=[{'kind':'post-merge' if x['state']=='post-merge' else 'reference-transaction',"
        "'stage':'0' if x['state']=='post-merge' else x['state'],"
        "'reference_kind':('FAST_FORWARD' if any(r[2]=='refs/heads/main' for r in x['rows']) "
        "else x['rows'][0][2] if x['rows'] else None),'cleanup_lock':x.get('cleanup_lock')} "
        "for x in old]\n"
        f" plan={hook_plan!r}\n"
        " cleanup=observe_publication_auto_merge_lock("
        "plan,{'hooks':history,'exit':None},sys.argv[1])\n"
        f"with Path({str(trace)!r}).open('a',encoding='utf-8') as stream:\n"
        " stream.write(json.dumps({'state':sys.argv[1],'rows':rows,'packed_lock':observed,"
        "'cleanup_lock':cleanup,'pid':os.getpid(),'parent_pid':os.getppid()})+'\\n')\n"
        f"if {case == 'auto-lock-deny'!r} and observed is not None: raise SystemExit(96)\n"
        "if sys.argv[1]=='prepared':\n"
        " for row in rows:\n"
        "  if len(row)!=3: raise SystemExit(93)\n"
        "  if row[2]=='refs/heads/main':\n"
        f"   if row[:2]!={[main, candidate]!r}: raise SystemExit(94)\n"
        f"   if {case == 'deny'!r}: raise SystemExit(95)\n"
    )
    hook_program = hooks / "guard.py"
    hook_program.write_text(program, encoding="utf-8")
    (hooks / "reference-transaction").write_bytes((
        '#!/bin/sh\nexec "' + Path(sys.executable).as_posix() + '" -B "'
        + hook_program.as_posix() + '" "$@"\n'
    ).encode())
    (hooks / "post-merge").write_bytes((
        '#!/bin/sh\nexec "' + Path(sys.executable).as_posix() + '" -B "'
        + hook_program.as_posix() + '" post-merge\n'
    ).encode())

    def state():
        index = root / ".git/index"
        stat = index.stat()
        orig_head = root / ".git/ORIG_HEAD"
        return {
            "refs": _git(root, "show-ref"), "head": (root / ".git/HEAD").read_text(),
            "resolved_head": _git(root, "rev-parse", "HEAD"),
            "main": _git(root, "rev-parse", "main"),
            "index_sha256": hashlib.sha256(index.read_bytes()).hexdigest(),
            "index_identity": [stat.st_dev, stat.st_ino],
            "index_tree": _git(root, "ls-files", "--stage"),
            "worktree_value_hex": value.read_bytes().hex(),
            "orig_head": orig_head.read_text() if orig_head.exists() else None,
            "packed_lock_exists": (root / ".git/packed-refs.lock").exists(),
        }

    before = state()
    environment = {key: item for key, item in os.environ.items()
                   if not key.upper().startswith("GIT_")}
    environment.update(GIT_CONFIG_NOSYSTEM="1", GIT_CONFIG_GLOBAL=os.devnull,
                       GIT_OPTIONAL_LOCKS="0", PYTHONDONTWRITEBYTECODE="1")
    command = ["git", "-c", "maintenance.auto=false"]
    if case != "plain":
        command += ["-c", "core.hooksPath=" + hooks.as_posix()]
    command += ["merge", "--ff-only", "--no-edit", branch]
    result = subprocess.run(command, cwd=root, env=environment, capture_output=True,
                            text=True, timeout=30, check=False)
    after = state()
    events = [json.loads(line) for line in trace.read_text().splitlines()] if trace.exists() else []
    (root.parent / "ff-reference-observation.json").write_text(json.dumps({
        "case": case, "git_version": _git(root, "--version"), "argv": command,
        "expected_main": main, "candidate": candidate, "before": before, "after": after,
        "exit_code": result.returncode, "stdout": result.stdout, "stderr": result.stderr,
        "hook_events": events,
    }, indent=2), encoding="utf-8")
    assert before["resolved_head"] == start
    assert before["index_tree"] == after["index_tree"]
    assert before["worktree_value_hex"] == after["worktree_value_hex"]
    assert after["head"] == "ref: refs/heads/main\n"
    assert not before["packed_lock_exists"] and not after["packed_lock_exists"]
    prepared_main = [row for event in events if event["state"] == "prepared"
                     for row in event["rows"] if row[2] == "refs/heads/main"]
    if case in {"plain", "guarded", "auto-lock-observer"}:
        assert result.returncode == 0, (result.stdout, result.stderr)
        assert after["main"] == after["resolved_head"] == candidate
        assert before["main"] == main != candidate  # Actual fast-forward, never cosmetic.
        if case in {"guarded", "auto-lock-observer"}:
            assert prepared_main == [[main, candidate, "refs/heads/main"]]
            locked = [event for event in events if event["packed_lock"] is not None]
            assert locked, events
            assert all(event["rows"] and all(row[2] == "AUTO_MERGE" for row in event["rows"])
                       for event in locked), events
            assert any(event["state"] == "prepared" for event in locked), events
            assert any(event["state"] == "committed" and any(
                row[2] == "AUTO_MERGE" for row in event["rows"]
            ) for event in events), events
    elif case == "auto-lock-deny":
        assert result.returncode != 0
        assert "ref updates aborted by hook" in result.stderr
        assert after["main"] == after["resolved_head"] == candidate
        assert any(event["state"] == "post-merge" for event in events)
        assert any(event["packed_lock"] is not None for event in events)
        assert not any(event["state"] == "committed" and any(
            row[2] == "AUTO_MERGE" for row in event["rows"]
        ) for event in events)
    else:
        assert result.returncode != 0
        assert after["main"] == after["resolved_head"] == start
        assert prepared_main == [[start, candidate, "refs/heads/main"]]
        assert before["refs"] == after["refs"]


@pytest.mark.parametrize("case", [
    "aborted", "prepared", "committed-absent", "aborted-absent", "missing-prepared",
    "nonempty", "replaced", "exit-recorded", "before-fast-forward", "before-post-merge",
    "after-committed", "terminal-residue",
])
def test_auto_merge_cleanup_lock_observer_boundaries(tmp_path, case):
    """Actual files, synthetic phase history only; not native hook admission evidence."""
    common = tmp_path / "git-common"
    common.mkdir()
    lock = common / "packed-refs.lock"
    plan = {"topology": {"candidate_checkout": {"common": {"path": common.as_posix()}}}}
    hooks = [
        {"kind": "reference-transaction", "reference_kind": "FAST_FORWARD", "stage": "committed"},
        {"kind": "post-merge", "reference_kind": None, "stage": "0"},
    ]
    merge = {"hooks": hooks, "exit": None}
    stage = ("committed" if case in {"committed-absent", "terminal-residue"}
             else "aborted" if case in {"aborted", "aborted-absent"} else "prepared")
    if case not in {"committed-absent", "aborted-absent", "missing-prepared"}:
        lock.write_bytes(b"foreign bytes" if case == "nonempty" else b"")
    if case == "replaced":
        old = integration._local_publication_metadata(lock, contents=True)
        hooks.append({"kind": "reference-transaction", "reference_kind": "AUTO_MERGE",
                      "stage": "aborted", "cleanup_lock": old})
        lock.rename(common / "original-retained.lock")
        lock.write_bytes(b"")
    elif case == "exit-recorded":
        merge["exit"] = {"returncode": 0}
    elif case == "before-fast-forward":
        hooks.pop(0)
    elif case == "before-post-merge":
        hooks.pop()
    elif case == "after-committed":
        hooks.append({"kind": "reference-transaction", "reference_kind": "AUTO_MERGE",
                      "stage": "committed"})
    before = (lock.read_bytes(), lock.stat().st_ino) if lock.exists() else None
    if case in {"aborted", "prepared", "committed-absent", "aborted-absent"}:
        observed = integration.observe_publication_auto_merge_lock(plan, merge, stage)
        assert (observed is None) == (before is None)
        if observed is not None:
            assert observed["identity"][1] == before[1]
            assert observed["bytes_hex"] == ""
    else:
        with pytest.raises(contract.WorkflowContractError, match="AUTO_MERGE_LOCK_"):
            integration.observe_publication_auto_merge_lock(plan, merge, stage)
    assert ((lock.read_bytes(), lock.stat().st_ino) if lock.exists() else None) == before


@pytest.mark.parametrize(
    "case", ["plain", "linked", "packed", "wrong-main", "wrong-candidate", "wrong-index",
             "hidden-index", "capture-swap", "candidate-before-capture"],
)
def test_local_publication_topology_captures_actual_checkout_without_publishing(
    small_repository, monkeypatch, case,
):
    root = small_repository
    for name in ("scripts/architecture_arch005_checkout_guard.py",
                 "docs/requirements/DEVX-002_Governed_Development_Workflow_Skill.md"):
        destination = root / name
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(ROOT / name, destination)
    (root / "value.txt").write_bytes(b"main\n")
    _git(root, "add", "scripts", "docs", "value.txt")
    _git(root, "commit", "-m", "P01 original main and real sentinels")
    main = _git(root, "rev-parse", "HEAD")
    _git(root, "switch", "-c", "codex/publication-candidate")
    (root / "value.txt").write_bytes(b"candidate\n")
    _git(root, "add", "value.txt")
    _git(root, "commit", "-m", "P01 exact candidate")
    candidate = _git(root, "rev-parse", "HEAD")
    newer_candidate = _git(
        root, "commit-tree", candidate + "^{tree}", "-p", candidate,
        "-m", "P01 independent commit with same tree",
    )
    peer = None
    if case in {"linked", "packed", "capture-swap"}:
        peer = root.parent / "main-peer"
        _git(root, "worktree", "add", str(peer), "main")
        (peer / "private-untracked.txt").write_bytes(b"unrelated owner bytes\0\r\n")
    if case == "packed":
        _git(root, "pack-refs", "--all", "--prune")
    if case == "wrong-index":
        blob = _git(root, "rev-parse", main + ":value.txt")
        _git(root, "update-index", "--cacheinfo", "100644," + blob + ",value.txt")
    if case == "hidden-index":
        _git(root, "update-index", "--assume-unchanged", "value.txt")
    before_refs = _git(root, "show-ref")
    before_index = (root / ".git/index").read_bytes()
    before_head = (root / ".git/HEAD").read_bytes()
    metadata_reads = []
    git_reads = []
    original_metadata = integration._local_publication_metadata
    original_git = integration._git
    swapped = False

    def observe_metadata(path, **kwargs):
        nonlocal swapped
        metadata_reads.append(str(path))
        result = original_metadata(path, **kwargs)
        if case == "capture-swap" and path.name == "packed-refs" and not swapped:
            assert peer is not None
            _git(peer, "update-ref", "--no-deref", "HEAD", main, main)
            swapped = True
        return result

    def observe_git(directory, *args, **kwargs):
        nonlocal swapped
        git_reads.append((str(directory), args))
        if (case == "candidate-before-capture" and not swapped
                and args == ("worktree", "list", "--porcelain", "-z")):
            _git(root, "update-ref", "refs/heads/codex/publication-candidate",
                 newer_candidate, candidate)
            swapped = True
        return original_git(directory, *args, **kwargs)

    failures = {
        "wrong-main": "LOCAL_PUBLICATION_MAIN_CHANGED",
        "wrong-candidate": "LOCAL_PUBLICATION_CANDIDATE_CHANGED",
        "wrong-index": "LOCAL_PUBLICATION_INDEX_CANDIDATE_MISMATCH",
        "hidden-index": "LOCAL_PUBLICATION_INDEX_HIDDEN",
        "capture-swap": "LOCAL_PUBLICATION_CAPTURE_CHANGED",
        "candidate-before-capture": "LOCAL_PUBLICATION_CANDIDATE_CHANGED",
    }
    with monkeypatch.context() as hooks:
        hooks.setattr(integration, "_local_publication_metadata", observe_metadata)
        hooks.setattr(integration, "_git", observe_git)
        arguments = {"candidate": main if case == "wrong-candidate" else candidate,
                     "expected_main": candidate if case == "wrong-main" else main}
        if case in failures:
            with pytest.raises(contract.WorkflowContractError, match=failures[case]):
                unexpected = integration.inspect_local_publication_topology(root, **arguments)
                (root.parent / "unexpected-admission.json").write_text(
                    json.dumps(unexpected, sort_keys=True), encoding="utf-8",
                )
        else:
            plan = integration.inspect_local_publication_topology(root, **arguments)
            assert plan["candidate_sha"] == candidate and plan["expected_main_sha"] == main
            assert plan["candidate_index_matches_tree"] is True
            assert plan["publication_allowed"] is False
            assert plan["dispatch_allowed"] is False and plan["mutation_performed"] is False
            if peer is None:
                assert plan["main_checkout"] is None
            else:
                assert Path(plan["main_checkout"]["root"]["path"]) == peer
                assert plan["main_checkout"]["observed_head"] == main
            if case == "packed":
                assert plan["main_ref"]["identity"] is None
                assert plan["packed_refs"]["identity"] is not None
            (root.parent / "local-publication-topology.json").write_text(
                json.dumps(plan, sort_keys=True), encoding="utf-8",
            )
    expected_refs = before_refs
    if case == "candidate-before-capture":
        assert swapped
        expected_refs = before_refs.replace(
            candidate + " refs/heads/codex/publication-candidate",
            newer_candidate + " refs/heads/codex/publication-candidate",
        )
        assert _git(root, "rev-parse", "refs/heads/main") == main
    assert _git(root, "show-ref") == expected_refs
    assert (root / ".git/index").read_bytes() == before_index
    assert (root / ".git/HEAD").read_bytes() == before_head
    assert (root / "value.txt").read_bytes() == b"candidate\n"
    if peer is not None:
        assert all("private-untracked.txt" not in path for path in metadata_reads)
        assert all(args[0] == "rev-parse" for directory, args in git_reads
                   if Path(directory) == peer)
        assert (peer / "private-untracked.txt").read_bytes() == b"unrelated owner bytes\0\r\n"
        assert (peer / "value.txt").read_bytes() == b"main\n"
    if case == "capture-swap":
        assert swapped and _git(peer, "rev-parse", "HEAD") == main
    assert not list((root / ".git").rglob("*.lock"))


@pytest.mark.parametrize(
    "case", ["plain", "linked", "orig-head", "active-merge", "unknown-lock",
             "tamper", "capture-drift"],
)
def test_publication_checkout_plan_preserves_actual_git_state(
    small_repository, monkeypatch, case,
):
    """Real metadata capture; synthetic request deliberately provides no Full authority."""
    root = small_repository
    for name in ("scripts/architecture_arch005_checkout_guard.py",
                 "docs/requirements/DEVX-002_Governed_Development_Workflow_Skill.md"):
        destination = root / name
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(ROOT / name, destination)
    _git(root, "add", "scripts", "docs")
    _git(root, "commit", "-m", "checkout plan sentinels")
    main = _git(root, "rev-parse", "HEAD")
    _git(root, "switch", "-c", "codex/checkout-plan")
    (root / "value.txt").write_bytes(b"candidate\n")
    _git(root, "add", "value.txt")
    _git(root, "commit", "-m", "checkout plan candidate")
    candidate = _git(root, "rev-parse", "HEAD")
    peer = root.parent / "main-peer" if case == "linked" else None
    if peer is not None:
        _git(root, "worktree", "add", str(peer), "main")
        (peer / "private.txt").write_bytes(b"other owner\n")
    gitdir = root / ".git"
    if case == "orig-head":
        (gitdir / "ORIG_HEAD").write_bytes((main + "\n").encode())
    canary = None
    if case in {"active-merge", "unknown-lock"}:
        canary = gitdir / ("AUTO_MERGE" if case == "active-merge" else "ORIG_HEAD.lock")
        canary.write_bytes(b"unknown owner - preserve exactly\n")
    topology = integration.inspect_local_publication_topology(
        root, candidate=candidate, expected_main=main,
    )
    request = {
        "candidate_sha": candidate, "expected_main_sha": main, "cwd": root.as_posix(),
        "publication_transaction_sha256": "1" * 64, "lease_id": "shape-only-no-lease",
    }
    intent = {
        "schema_version": "integration_publication_local_intent.v1",
        "transaction_sha256": request["publication_transaction_sha256"],
        "lease_id": request["lease_id"], "candidate_sha": candidate,
        "expected_main_sha": main, "topology": topology,
        "dispatch_allowed": False, "publication_allowed": False,
    }
    request["local_publication_intent_sha256"] = contract.canonical_digest(intent)
    watched = [gitdir / "HEAD", gitdir / "index", gitdir / "logs/HEAD",
               gitdir / "logs/refs/heads/main", gitdir / "ORIG_HEAD"]
    if peer is not None:
        watched += [Path(topology["main_checkout"]["gitdir"]["path"]) / "HEAD",
                    peer / "private.txt"]
    if canary is not None:
        watched.append(canary)
    def snapshot():
        return {str(path): (path.read_bytes(), path.stat().st_ino) if path.exists() else None
                for path in watched}
    before = snapshot()
    original_metadata = integration._local_publication_metadata
    injected = False
    def capture(path, **kwargs):
        nonlocal injected
        assert canary is None or path != canary, "unknown state content must not be read"
        result = original_metadata(path, **kwargs)
        if case == "capture-drift" and path == gitdir / "ORIG_HEAD" and not injected:
            injected = True
            path.write_bytes((main + "\n").encode())
        return result
    with monkeypatch.context() as hooks:
        hooks.setattr(integration, "_local_publication_metadata", capture)
        if case in {"active-merge", "unknown-lock", "capture-drift"}:
            error = "CAPTURE_CHANGED" if case == "capture-drift" else "STATE_EXISTS"
            with pytest.raises(contract.WorkflowContractError, match="CHECKOUT_PLAN_" + error):
                integration.prepare_local_publication_checkout_plan(root, request)
        else:
            plan = integration.prepare_local_publication_checkout_plan(root, request)
            integration.validate_local_publication_checkout_plan(plan, request)
            assert plan["peer_handoff_required"] is (peer is not None)
            assert all(plan[key] is False for key in (
                "dispatch_allowed", "publication_allowed", "resume_allowed",
            ))
            assert plan["topology"] == topology
            assert plan["orig_head_after_hex"] == (main + "\n").encode().hex()
            assert len(plan["head_transitions"]) == (2 if peer is not None else 1)
            (root.parent / "checkout-plan.json").write_text(
                json.dumps(plan, sort_keys=True), encoding="utf-8",
            )
            if case == "tamper":
                for field, replacement in (("resume_allowed", True), ("absent_paths", []),
                                           ("head_transitions", []), ("request_sha256", "0" * 64)):
                    changed = copy.deepcopy(plan)
                    changed[field] = replacement
                    changed["plan_sha256"] = contract.canonical_digest({
                        key: value for key, value in changed.items() if key != "plan_sha256"
                    })
                    with pytest.raises(contract.WorkflowContractError, match="CHECKOUT_PLAN_"):
                        integration.validate_local_publication_checkout_plan(changed, request)
    after = snapshot()
    if case == "capture-drift":
        assert injected
        assert after.pop(str(gitdir / "ORIG_HEAD"))[0] == (main + "\n").encode()
        assert before.pop(str(gitdir / "ORIG_HEAD")) is None
    assert before == after
    assert _git(root, "rev-parse", "refs/heads/main") == main
    assert _git(root, "rev-parse", "HEAD") == candidate


@pytest.mark.parametrize("case", [
    "plain", "linked", "packed", "not-switched", "main-advanced", "branch-advanced",
    "index-drift", "head-replaced", "peer-index", "config-drift", "reflog-drift",
    "orig-head", "unknown-lock", "extra-worktree", "capture-drift",
])
def test_switched_head_window_requires_only_original_head_changes(
    small_repository, monkeypatch, case,
):
    """Real Git and native HEAD writes; this read-only seam grants no Full/lease authority."""
    root = small_repository
    for name in ("scripts/architecture_arch005_checkout_guard.py",
                 "docs/requirements/DEVX-002_Governed_Development_Workflow_Skill.md"):
        path = root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(ROOT / name, path)
    (root / "value.txt").write_bytes(b"original main\n")
    _git(root, "add", "scripts", "docs", "value.txt")
    _git(root, "commit", "-m", "original switched window main")
    main = _git(root, "rev-parse", "HEAD")
    _git(root, "switch", "-c", "codex/switched-window")
    (root / "value.txt").write_bytes(b"candidate\n")
    _git(root, "add", "value.txt")
    _git(root, "commit", "-m", "original switched window candidate")
    candidate = _git(root, "rev-parse", "HEAD")
    peer = root.parent / "main-peer" if case in {"linked", "peer-index"} else None
    if peer is not None:
        _git(root, "worktree", "add", str(peer), "main")
        (peer / "private-untracked.txt").write_bytes(b"other owner bytes\0\n")
    if case == "packed":
        _git(root, "pack-refs", "--all", "--prune")
    topology = integration.inspect_local_publication_topology(
        root, candidate=candidate, expected_main=main,
    )
    request = {
        "cwd": root.as_posix(), "candidate_sha": candidate, "expected_main_sha": main,
        "publication_transaction_sha256": "a" * 64, "lease_id": "read-only-not-authority",
    }
    request["local_publication_intent_sha256"] = contract.canonical_digest({
        "schema_version": "integration_publication_local_intent.v1",
        "transaction_sha256": request["publication_transaction_sha256"],
        "lease_id": request["lease_id"], "candidate_sha": candidate,
        "expected_main_sha": main, "topology": topology,
        "dispatch_allowed": False, "publication_allowed": False,
    })
    plan = integration.prepare_local_publication_checkout_plan(root, request)
    if case != "not-switched":
        for transition in plan["head_transitions"]:
            checkout = (topology["main_checkout"] if transition["role"] == "peer_head"
                        else topology["candidate_checkout"])
            before = transition["before"]
            contract.apply_bound_file(
                Path(checkout["gitdir"]["path"]), "HEAD", bytes.fromhex(before["bytes_hex"]),
                bytes.fromhex(transition["after_hex"]), expected_identity=tuple(before["identity"]),
                expected_root_identity=tuple(checkout["gitdir"]["identity"]),
                expected_parent_identities={},
            )
    gitdir = Path(topology["candidate_checkout"]["gitdir"]["path"])
    if case == "main-advanced":
        _git(root, "update-ref", "refs/heads/main", candidate, main)
    elif case == "branch-advanced":
        _git(root, "update-ref", topology["candidate_branch"], main, candidate)
    elif case == "index-drift":
        _git(root, "read-tree", main)
    elif case == "head-replaced":
        head = gitdir / "HEAD"
        head.rename(gitdir / "HEAD.retained")
        head.write_bytes(b"ref: refs/heads/main\n")
    elif case == "peer-index":
        _git(peer, "read-tree", candidate)
    elif case == "config-drift":
        _git(root, "config", "user.name", "unexpected owner")
    elif case == "reflog-drift":
        with (gitdir / "logs/HEAD").open("ab") as stream:
            stream.write(b"unexpected row\n")
    elif case == "orig-head":
        (gitdir / "ORIG_HEAD").write_bytes((main + "\n").encode())
    elif case == "unknown-lock":
        (gitdir / "HEAD.lock").write_bytes(b"unknown owner preserved\n")
    elif case == "extra-worktree":
        _git(root, "worktree", "add", "--detach", str(root.parent / "unexpected-peer"), main)
    observed_paths = [gitdir / name for name in (
        "HEAD", "index", "ORIG_HEAD", "HEAD.lock", "logs/HEAD",
    )]
    if peer is not None:
        observed_paths += [Path(topology["main_checkout"]["gitdir"]["path"]) / "HEAD",
                           Path(topology["main_checkout"]["gitdir"]["path"]) / "index",
                           peer / "private-untracked.txt"]
    def snapshot():
        return {str(path): (path.read_bytes(), path.stat().st_ino) if path.exists() else None
                for path in observed_paths}
    before = snapshot()
    metadata_reads = []
    original_metadata = integration._local_publication_metadata
    def metadata(path, **kwargs):
        metadata_reads.append(str(path))
        return original_metadata(path, **kwargs)
    original_checkout = integration._local_publication_checkout
    injected = False
    def checkout(path):
        nonlocal injected
        observed = original_checkout(path)
        if case == "capture-drift" and path == root and not injected:
            injected = True
            (gitdir / "HEAD").write_bytes((candidate + "\n").encode())
        return observed
    with monkeypatch.context() as hooks:
        hooks.setattr(integration, "_local_publication_metadata", metadata)
        hooks.setattr(integration, "_local_publication_checkout", checkout)
        if case in {"plain", "linked", "packed"}:
            result = integration.inspect_publication_switched_heads(root, request, plan)
            assert result["main_sha"] == main and result["candidate_index_matches_tree"] is True
            assert all(result[key] is False for key in (
                "dispatch_allowed", "publication_allowed", "mutation_performed",
            ))
        else:
            with pytest.raises(contract.WorkflowContractError):
                integration.inspect_publication_switched_heads(root, request, plan)
    after = snapshot()
    if case == "capture-drift":
        assert injected
        assert after.pop(str(gitdir / "HEAD"))[0] == (candidate + "\n").encode()
        before.pop(str(gitdir / "HEAD"))
    assert before == after
    assert all("private-untracked.txt" not in path for path in metadata_reads)
    assert str(gitdir / "HEAD.lock") not in metadata_reads
    assert (root / "value.txt").read_bytes() == b"candidate\n"


def _prepare_main_advanced_scene(root: Path, *, candidate_log: bool = False):
    """Construct native fixture state only; never original Full/store authority."""
    for name in ("scripts/architecture_arch005_checkout_guard.py",
                 "docs/requirements/DEVX-002_Governed_Development_Workflow_Skill.md"):
        target = root / name
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(ROOT / name, target)
    _git(root, "add", "scripts", "docs")
    _git(root, "commit", "-m", "main advance original main")
    main = _git(root, "rev-parse", "HEAD")
    _git(root, "switch", "-c", "codex/main-advance-scene")
    (root / "candidate.txt").write_bytes(b"candidate\n")
    _git(root, "add", "candidate.txt")
    _git(root, "commit", "-m", "main advance original candidate")
    candidate = _git(root, "rev-parse", "HEAD")
    newer = _git(root, "commit-tree", main + "^{tree}", "-p", main,
                 content=b"independent main successor\n")
    peer = root.parent / "original-main-peer"
    _git(root, "worktree", "add", str(peer), "main")
    private = peer / "private.txt"
    private.write_bytes(b"private original owner\0\n")
    topology = integration.inspect_local_publication_topology(
        root, candidate=candidate, expected_main=main,
    )
    request = {"cwd": root.as_posix(), "candidate_sha": candidate, "expected_main_sha": main,
               "publication_transaction_sha256": "a" * 64, "lease_id": "scene-not-authority",
               "local_publication_event_id": "shape-only-event"}
    request["local_publication_intent_sha256"] = contract.canonical_digest({
        "schema_version": "integration_publication_local_intent.v1",
        "transaction_sha256": request["publication_transaction_sha256"],
        "lease_id": request["lease_id"], "candidate_sha": candidate, "expected_main_sha": main,
        "topology": topology, "dispatch_allowed": False, "publication_allowed": False,
    })
    plan = integration.prepare_local_publication_checkout_plan(root, request)
    for transition in plan["head_transitions"]:
        checkout = (topology["main_checkout"] if transition["role"] == "peer_head"
                    else topology["candidate_checkout"])
        contract.apply_bound_file(
            Path(checkout["gitdir"]["path"]), "HEAD",
            bytes.fromhex(transition["before"]["bytes_hex"]),
            bytes.fromhex(transition["after_hex"]),
            expected_identity=tuple(transition["before"]["identity"]),
            expected_root_identity=tuple(checkout["gitdir"]["identity"]),
            expected_parent_identities={},
        )
    _git(root if candidate_log else peer,
         "update-ref", "-m", "independent successor", "refs/heads/main", newer, main)
    return request, plan, newer, peer


@pytest.mark.parametrize("case", [
    "plain", "candidate-log", "candidate-restored", "peer-reattached", "fast-prepared",
    "unchanged-main", "published-main", "unrelated-main", "wrong-index", "extra-reflog",
    "unknown-lock", "capture-ref-replaced", "candidate-head-replaced",
])
def test_publication_main_advanced_read_only_scene(small_repository, monkeypatch, case):
    """Real Git M->N and native namespace checks, not original Full/fence authority."""
    root = small_repository
    request, plan, newer, peer = _prepare_main_advanced_scene(
        root, candidate_log=case == "candidate-log",
    )
    topology = plan["topology"]
    main, candidate = request["expected_main_sha"], request["candidate_sha"]
    private = peer / "private.txt"
    frozen = copy.deepcopy((request, plan))
    merge = None
    def observe():
        return integration.inspect_publication_main_advanced_recovery_window(
            root, request, plan, merge, None,
        )
    baseline = observe()
    assert baseline["main_sha"] == newer
    # The old M-only observer must not be silently relaxed by the new observer.
    with pytest.raises(contract.WorkflowContractError, match="PUBLICATION_EFFECT_REFS"):
        integration.inspect_publication_recovery_window(root, request, plan, None, None)
    gitdir = Path(topology["candidate_checkout"]["gitdir"]["path"])
    peer_gitdir = Path(topology["main_checkout"]["gitdir"]["path"])
    common = Path(topology["candidate_checkout"]["common"]["path"])
    main_ref = common / "refs/heads/main"
    expected_main = newer
    if case in {"candidate-restored", "peer-reattached"}:
        role = "candidate_head" if case == "candidate-restored" else "peer_head"
        transition = next(row for row in plan["head_transitions"] if row["role"] == role)
        checkout = topology["candidate_checkout" if role == "candidate_head" else "main_checkout"]
        contract.apply_bound_file(
            Path(checkout["gitdir"]["path"]), "HEAD", bytes.fromhex(transition["after_hex"]),
            bytes.fromhex(transition["before"]["bytes_hex"]),
            expected_identity=tuple(transition["before"]["identity"]),
            expected_root_identity=tuple(checkout["gitdir"]["identity"]),
            expected_parent_identities={},
        )
    elif case == "fast-prepared":
        merge = {"hooks": [{"reference_kind": "FAST_FORWARD", "stage": "prepared",
                            "prepared_file": None}], "exit": None}
    elif case in {"unchanged-main", "published-main", "unrelated-main"}:
        expected_main = main if case == "unchanged-main" else candidate
        if case == "unrelated-main":
            expected_main = _git(root, "commit-tree", main + "^{tree}", content=b"unrelated\n")
        _git(peer, "update-ref", "refs/heads/main", expected_main, newer)
    elif case == "wrong-index":
        _git(root, "read-tree", main)
    elif case == "extra-reflog":
        with (common / "logs/refs/heads/main").open("ab") as stream:
            stream.write(b"unknown extra transition\n")
    elif case == "unknown-lock":
        (gitdir / "index.lock").write_bytes(b"foreign owner\n")
    elif case == "candidate-head-replaced":
        (gitdir / "HEAD").rename(gitdir / "original-head-preserved")
        (gitdir / "HEAD").write_bytes(b"ref: refs/heads/main\n")
    paths = [gitdir / "HEAD", gitdir / "index", gitdir / "logs/HEAD", main_ref,
             common / "logs/refs/heads/main", peer_gitdir / "HEAD", peer_gitdir / "index",
             private, gitdir / "index.lock"]
    def snapshot():
        return {path.as_posix(): (path.read_bytes(), path.stat().st_ino) if path.exists() else None
                for path in paths}
    before = snapshot()
    original_metadata = integration._local_publication_metadata
    injected = False
    def metadata(path, **kwargs):
        nonlocal injected
        value = original_metadata(path, **kwargs)
        if case == "capture-ref-replaced" and path == main_ref and not injected:
            injected = True
            main_ref.rename(main_ref.with_name("original-main-preserved"))
            main_ref.write_bytes((newer + "\n").encode())
        return value
    with monkeypatch.context() as hooks:
        hooks.setattr(integration, "_local_publication_metadata", metadata)
        if case in {"plain", "candidate-log", "candidate-restored"}:
            result = observe()
            assert result["schema_version"] == (
                "workflow_publication_main_advanced_recovery_window_observation.v1"
            )
            assert result["candidate_index_matches_tree"] is True
            assert result["checkouts"]["peer_head"]["observed_head"] == main
            assert result["main_ref"] == baseline["main_ref"]
            assert all(result[key] is False for key in (
                "dispatch_allowed", "publication_allowed", "mutation_performed",
            ))
            if case == "candidate-restored":
                assert result["checkouts"]["candidate_head"]["observed_head"] == candidate
        else:
            with pytest.raises(contract.WorkflowContractError):
                observe()
    after = snapshot()
    if case == "capture-ref-replaced":
        assert injected
        original_ref = before.pop(main_ref.as_posix())
        replaced_ref = after.pop(main_ref.as_posix())
        assert original_ref[0] == replaced_ref[0] and original_ref[1] != replaced_ref[1]
    assert after == before
    assert _git(root, "rev-parse", "refs/heads/main") == expected_main
    assert (request, plan) == frozen
    assert private.read_bytes() == b"private original owner\0\n"


@pytest.mark.parametrize("case", [
    "restored", "orig-created", "known-lock", "held-N", "intent-only", "legacy-schema",
    "live-git", "live-job", "fast-prepared", "bad-request", "peer-attached-before",
    "bad-reflog-identity", "candidate-not-restored", "peer-attached-after",
    "main-changed-after", "ref-identity-changed-after", "reflog-changed-after",
    "early-completion", "intent-rewrite", "backwards-transition", "reentry-ref-replaced",
    "held-N-and-reflogs", "held-N-and-reflogs-candidate",
])
def test_publication_main_advanced_durable_recovery_contract(small_repository, case):
    """Native inverse-write scenes plus pure append-only contract, not fence admission."""
    from contextlib import ExitStack

    from ai_trading_system.platform.architecture.parallel_control import ParallelControlError
    from ai_trading_system.platform.architecture.workflow_coordination import (
        _validate_publication_head_recovery,
        _validate_publication_head_recovery_transition,
        execution_is_terminal,
    )

    root = small_repository
    request, plan, newer, peer = _prepare_main_advanced_scene(
        root, candidate_log=case == "held-N-and-reflogs-candidate",
    )
    frozen = copy.deepcopy((request, plan))
    topology = plan["topology"]
    checkout = topology["candidate_checkout"]
    gitdir, common = Path(checkout["gitdir"]["path"]), Path(checkout["common"]["path"])
    peer_gitdir = Path(topology["main_checkout"]["gitdir"]["path"])
    preserved_paths = [root / "candidate.txt", gitdir / "index", peer / "private.txt",
                       peer_gitdir / "HEAD", peer_gitdir / "index", common / "refs/heads/main",
                       common / "logs/refs/heads/main", gitdir / "logs/HEAD"]
    def snapshot():
        return {path.as_posix(): (path.read_bytes(), path.stat().st_ino)
                for path in preserved_paths}
    preserved = snapshot()
    merge = None
    if case in {"orig-created", "known-lock"}:
        lock = gitdir / "ORIG_HEAD.lock"
        lock.write_bytes((request["expected_main_sha"] + "\n").encode())
        prepared = integration._local_publication_metadata(lock, contents=True)
        merge = {"hooks": [{"reference_kind": "ORIG_HEAD", "stage": "prepared",
                            "prepared_file": prepared}], "exit": None}
        if case == "orig-created":
            lock.replace(gitdir / "ORIG_HEAD")
    def observe(recovery=None):
        return integration.inspect_publication_main_advanced_recovery_window(
            root, request, plan, merge, recovery,
        )
    before = observe()
    restored_orig = integration.validate_publication_main_advanced_recovery_observation(
        before, request, plan, merge,
    )
    attempt = {"request": request, "request_sha256": contract.canonical_digest(request),
               "state": "RESULT_RECORDED", "result": {"status": "INSUFFICIENT"},
               "exit": {"observed_at": "2026-09-15T00:00:00+00:00"},
               "checkout_effect": {"shape_only": True}, "git_launch": {"shape_only": True},
               "checkout_plan": {"plan": plan}}
    if merge is not None:
        attempt["git_merge"] = merge
    prior = copy.deepcopy(attempt)
    record = {"schema_version": "workflow_publication_head_recovery.v2",
              "request_sha256": attempt["request_sha256"],
              "checkout_effect_sha256": contract.canonical_digest(attempt["checkout_effect"]),
              "git_merge_sha256": contract.canonical_digest(merge) if merge is not None else None,
              "state": "INTENT", "observer": {"pid": 1, "creation_time": 1},
              "job_state": "EMPTY", "git_process": {"state": "EXITED"},
              "started_at": "2026-09-15T00:00:01+00:00", "completed_at": None,
              "scene_before": before, "scene_after": None, "restored_orig_head": restored_orig}
    attempt["head_recovery"] = record
    _validate_publication_head_recovery(attempt)
    _validate_publication_head_recovery_transition(prior, attempt)
    assert observe(record) == before
    intent = copy.deepcopy(attempt)
    if case == "legacy-schema":
        record["schema_version"] = "workflow_publication_head_recovery.v1"
    elif case == "live-git":
        record["git_process"] = {"state": "RUNNING"}
    elif case == "live-job":
        record["job_state"] = "RUNNING"
    elif case == "fast-prepared":
        attempt["git_merge"] = {"hooks": [{"reference_kind": "FAST_FORWARD",
                                           "stage": "prepared", "prepared_file": None}]}
        record["git_merge_sha256"] = contract.canonical_digest(attempt["git_merge"])
    elif case == "bad-request":
        record["request_sha256"] = "0" * 64
    elif case == "peer-attached-before":
        record["scene_before"]["checkouts"]["peer_head"] = copy.deepcopy(topology["main_checkout"])
    elif case == "bad-reflog-identity":
        record["scene_before"]["auxiliary"]["reflogs"]["main"]["identity"] = [0, 0]
    if case in {"legacy-schema", "live-git", "live-job", "fast-prepared", "bad-request",
                "peer-attached-before", "bad-reflog-identity"}:
        with pytest.raises((contract.WorkflowContractError, ParallelControlError)):
            _validate_publication_head_recovery(attempt)
        assert snapshot() == preserved
        return
    if case == "intent-only":
        assert not execution_is_terminal({"schema_version": "lease_execution.v5",
                                          "state": "RESULT_RECORDED",
                                          "publication_attempts": [attempt]})
        assert snapshot() == preserved
        return
    if case == "reentry-ref-replaced":
        path = common / "refs/heads/main"
        path.rename(path.with_name("original-main-preserved"))
        path.write_bytes((newer + "\n").encode())
        assert observe()["main_sha"] == newer
        with pytest.raises(contract.WorkflowContractError,
                           match="PUBLICATION_RECOVERY_MAIN_ADVANCE_BINDING"):
            observe(record)
        changed = snapshot()
        old = preserved.pop(path.as_posix())
        new = changed.pop(path.as_posix())
        assert old[0] == new[0] and old[1] != new[1]
        assert changed == preserved
        return
    main_ref = before["main_ref"]
    with ExitStack() as custody:
        if case in {"held-N-and-reflogs", "held-N-and-reflogs-candidate"}:
            assert custody.enter_context(integration.hold_publication_main_advanced_recovery_scene(
                root, request, plan, merge, record,
            )) == before
        else:
            custody.enter_context(contract.hold_bound_read_file(
                common, "refs/heads/main", expected=bytes.fromhex(main_ref["bytes_hex"]),
                expected_identity=tuple(main_ref["identity"]),
                expected_root_identity=tuple(checkout["common"]["identity"]),
                expected_parent_identities={key: tuple(value["identity"])
                                            for key, value in topology["ref_directories"].items()},
            ))
        if case in {"held-N", "held-N-and-reflogs", "held-N-and-reflogs-candidate"}:
            next_main = _git(root, "commit-tree", newer + "^{tree}", "-p", newer,
                             content=b"racing during recovery\n")
            result = subprocess.run(
                ["git", "-C", str(root if case == "held-N-and-reflogs-candidate" else peer),
                 "update-ref", "refs/heads/main", next_main, newer],
                capture_output=True, timeout=30,
            )
            assert result.returncode != 0
            assert _git(root, "rev-parse", "refs/heads/main") == newer
            if case == "held-N":
                # Preserve the v182 counterexample: Git may append its reflog
                # before discovering that the held ref cannot be replaced.
                with pytest.raises(contract.WorkflowContractError,
                                   match="PUBLICATION_RECOVERY_MAIN_ADVANCE_REFLOG"):
                    observe(record)
                changed = snapshot()
                log_path = (common / "logs/refs/heads/main").as_posix()
                old_log, new_log = preserved.pop(log_path), changed.pop(log_path)
                assert new_log[0].startswith(old_log[0]) and new_log[1] == old_log[1]
                suffix = new_log[0][len(old_log[0]):]
                assert suffix.count(b"\n") == 1
                assert suffix.startswith((newer + " " + next_main + " ").encode())
                assert changed == preserved
                assert (gitdir / "HEAD").read_bytes() == b"ref: refs/heads/main\n"
                assert record["state"] == "INTENT" and record["scene_after"] is None
                return
            assert observe(record) == before
        transition = next(row for row in plan["head_transitions"]
                          if row["role"] == "candidate_head")
        contract.apply_bound_file(
            gitdir, "HEAD", bytes.fromhex(transition["after_hex"]),
            bytes.fromhex(transition["before"]["bytes_hex"]),
            expected_identity=tuple(transition["before"]["identity"]),
            expected_root_identity=tuple(checkout["gitdir"]["identity"]),
            expected_parent_identities={},
        )
        if case == "orig-created":
            current = observe(record)["auxiliary"]["orig_head"]
            contract.apply_bound_file(
                gitdir, "ORIG_HEAD", bytes.fromhex(current["bytes_hex"]), None,
                expected_identity=tuple(current["identity"]),
                expected_root_identity=tuple(checkout["gitdir"]["identity"]),
                expected_parent_identities={},
            )
        for lock in observe(record)["owned_locks"].values():
            contract.apply_bound_file(
                gitdir, "ORIG_HEAD.lock", bytes.fromhex(lock["bytes_hex"]), None,
                expected_identity=tuple(lock["identity"]),
                expected_root_identity=tuple(checkout["gitdir"]["identity"]),
                expected_parent_identities={},
            )
        after = observe(record)
    record.update(state="RESTORED", completed_at="2026-09-15T00:00:02+00:00", scene_after=after)
    _validate_publication_head_recovery(attempt)
    _validate_publication_head_recovery_transition(intent, attempt)
    assert observe(record) == after
    assert snapshot() == preserved
    assert _git(root, "rev-parse", "HEAD") == request["candidate_sha"]
    assert _git(peer, "rev-parse", "HEAD") == request["expected_main_sha"]
    assert (request, plan) == frozen
    assert not execution_is_terminal({"schema_version": "lease_execution.v5",
                                      "state": "RESULT_RECORDED",
                                      "publication_attempts": [attempt]})
    if case == "restored":
        _assert_main_advanced_stable_contract(root, attempt)
    if case == "candidate-not-restored":
        after["checkouts"]["candidate_head"] = copy.deepcopy(before["checkouts"]["candidate_head"])
    elif case == "peer-attached-after":
        after["checkouts"]["peer_head"] = copy.deepcopy(topology["main_checkout"])
    elif case == "main-changed-after":
        after["main_sha"] = "b" * 40
        raw = (after["main_sha"] + "\n").encode()
        after["main_ref"].update(bytes_hex=raw.hex(), sha256=hashlib.sha256(raw).hexdigest())
    elif case == "ref-identity-changed-after":
        after["main_ref"]["identity"] = [0, 0]
    elif case == "reflog-changed-after":
        after["auxiliary"]["reflogs"]["main"]["sha256"] = "b" * 64
    elif case == "early-completion":
        record["completed_at"] = "2026-09-15T00:00:00+00:00"
    elif case == "intent-rewrite":
        record["observer"]["pid"] += 1
        with pytest.raises(ParallelControlError, match="PUBLICATION_HEAD_RECOVERY_TRANSITION"):
            _validate_publication_head_recovery_transition(intent, attempt)
        return
    elif case == "backwards-transition":
        with pytest.raises(ParallelControlError, match="PUBLICATION_HEAD_RECOVERY_TRANSITION"):
            _validate_publication_head_recovery_transition(attempt, intent)
        return
    if case in {"candidate-not-restored", "peer-attached-after", "main-changed-after",
                "ref-identity-changed-after", "reflog-changed-after", "early-completion"}:
        with pytest.raises((contract.WorkflowContractError, ParallelControlError)):
            _validate_publication_head_recovery(attempt)


def _assert_main_advanced_stable_contract(root: Path, original_attempt) -> None:
    """Synthetic stable/profile envelope only; no original Full/store/lease admission."""
    from ai_trading_system.platform.architecture.parallel_control import ParallelControlError
    from ai_trading_system.platform.architecture.workflow_coordination import (
        _validate_unchanged_publication_observation,
        execution_is_terminal,
    )

    attempt = copy.deepcopy(original_attempt)
    execution = {"schema_version": "lease_execution.v5", "state": "RESULT_RECORDED",
                 "publication_attempts": [attempt]}
    request = attempt["request"]
    profile = {
        "schema_version": "full_publication_profile_inspection.v1", "status": "PASS",
        "scope": "PROFILE_MANDATORY_AND_READINESS_IDENTITY",
        "candidate_sha": request["candidate_sha"],
        "transaction_sha256": request["publication_transaction_sha256"],
        "head_event_id": request["local_publication_event_id"],
        "execution_sha256": contract.canonical_digest(execution),
        "dispatch_performed": False, "publication_performed": False,
        "captures": [{"path": (root / "outputs/shape-only.json").as_posix(),
                      "size_bytes": 2, "sha256": "e" * 64}],
    }
    observation = {
        "schema_version": "workflow_publication_stable_observation.v4",
        "stable_state": "CANDIDATE_RETAINED_MAIN_ADVANCED",
        "request_sha256": attempt["request_sha256"],
        "execution_sha256": contract.canonical_digest(attempt),
        "head_recovery_sha256": contract.canonical_digest(attempt["head_recovery"]),
        "recovery_observation": copy.deepcopy(attempt["head_recovery"]["scene_after"]),
        "profile_inspection": profile, "git_process": {"state": "EXITED"}, "job_state": "EMPTY",
        "observer": {"pid": 1, "creation_time": 1},
        "observed_at": "2026-09-15T00:00:03+00:00",
        "dispatch_allowed": False, "publication_allowed": False,
    }
    execution["publication_stable_observation"] = observation
    _validate_unchanged_publication_observation(execution)
    assert execution_is_terminal(execution)  # Pure predicate, not store append/release authority.
    for fault in ("state", "dispatch", "publication", "request", "execution", "recovery-digest",
                  "scene", "job", "git", "time", "profile-execution", "profile-candidate",
                  "profile-event", "profile-capture", "unfinished-recovery"):
        changed = copy.deepcopy(execution)
        record = changed["publication_stable_observation"]
        if fault == "state":
            record["stable_state"] = "LOCAL_PUBLISHED"
        elif fault in {"dispatch", "publication"}:
            record[fault + "_allowed"] = True
        elif fault in {"request", "execution"}:
            record[fault + "_sha256"] = "0" * 64
        elif fault == "recovery-digest":
            record["head_recovery_sha256"] = "0" * 64
        elif fault == "scene":
            record["recovery_observation"]["main_ref"]["identity"] = [0, 0]
        elif fault == "job":
            record["job_state"] = "RUNNING"
        elif fault == "git":
            record["git_process"] = {"state": "RUNNING"}
        elif fault == "time":
            record["observed_at"] = "2026-09-15T00:00:01+00:00"
        elif fault.startswith("profile-"):
            field = {"profile-execution": "execution_sha256", "profile-candidate": "candidate_sha",
                     "profile-event": "head_event_id", "profile-capture": "captures"}[fault]
            record["profile_inspection"][field] = [] if fault == "profile-capture" else "incorrect"
        else:
            current = changed["publication_attempts"][0]
            current["head_recovery"].update(state="INTENT", completed_at=None, scene_after=None)
            record["execution_sha256"] = contract.canonical_digest(current)
            record["head_recovery_sha256"] = contract.canonical_digest(current["head_recovery"])
        with pytest.raises((ParallelControlError, contract.WorkflowContractError)):
            _validate_unchanged_publication_observation(changed)
        assert not execution_is_terminal(changed)
    assert original_attempt == attempt


@pytest.mark.parametrize("case", [
    "none-switched", "peer-only", "both-switched", "orig-replaced", "orig-created",
    "known-lock", "foreign-lock", "unknown-lock", "main-advanced", "head-replaced",
    "bad-scene", "restore-replay",
])
def test_publication_failed_head_recovery_scene_and_durable_contract(small_repository, case):
    """Real native files/Git scene and recovery receipt contract, not Full/store authority."""
    from ai_trading_system.platform.architecture.workflow_coordination import (
        _validate_publication_head_recovery,
        _validate_publication_head_recovery_transition,
    )

    root = small_repository
    for name in ("scripts/architecture_arch005_checkout_guard.py",
                 "docs/requirements/DEVX-002_Governed_Development_Workflow_Skill.md"):
        target = root / name
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(ROOT / name, target)
    _git(root, "add", "scripts", "docs")
    _git(root, "commit", "-m", "recovery original main")
    main = _git(root, "rev-parse", "HEAD")
    _git(root, "switch", "-c", "codex/recovery-scene")
    (root / "candidate.txt").write_bytes(b"candidate\n")
    _git(root, "add", "candidate.txt")
    _git(root, "commit", "-m", "recovery candidate")
    candidate = _git(root, "rev-parse", "HEAD")
    peer = root.parent / "original-main-peer"
    _git(root, "worktree", "add", str(peer), "main")
    (peer / "private.txt").write_bytes(b"private owner content\n")
    gitdir = root / ".git"
    if case == "orig-replaced":
        (gitdir / "ORIG_HEAD").write_bytes((candidate + "\n").encode())
    topology = integration.inspect_local_publication_topology(
        root, candidate=candidate, expected_main=main,
    )
    request = {"cwd": root.as_posix(), "candidate_sha": candidate, "expected_main_sha": main,
               "publication_transaction_sha256": "a" * 64, "lease_id": "scene-not-authority"}
    request["local_publication_intent_sha256"] = contract.canonical_digest({
        "schema_version": "integration_publication_local_intent.v1",
        "transaction_sha256": request["publication_transaction_sha256"],
        "lease_id": request["lease_id"], "candidate_sha": candidate, "expected_main_sha": main,
        "topology": topology, "dispatch_allowed": False, "publication_allowed": False,
    })
    plan = integration.prepare_local_publication_checkout_plan(root, request)
    count = 0 if case == "none-switched" else 1 if case == "peer-only" else 2
    for transition in plan["head_transitions"][:count]:
        checkout = topology["main_checkout"] if transition["role"] == "peer_head" else topology[
            "candidate_checkout"
        ]
        contract.apply_bound_file(
            Path(checkout["gitdir"]["path"]), "HEAD",
            bytes.fromhex(transition["before"]["bytes_hex"]),
            bytes.fromhex(transition["after_hex"]),
            expected_identity=tuple(transition["before"]["identity"]),
            expected_root_identity=tuple(checkout["gitdir"]["identity"]),
            expected_parent_identities={},
        )
    merge = None
    if case in {"orig-replaced", "orig-created", "known-lock", "foreign-lock"}:
        lock = gitdir / "ORIG_HEAD.lock"
        lock.write_bytes((main + "\n").encode())
        prepared = integration._local_publication_metadata(lock, contents=True)
        merge = {"hooks": [{"reference_kind": "ORIG_HEAD", "stage": "prepared",
                            "prepared_file": prepared}], "exit": None}
        if case in {"orig-replaced", "orig-created"}:
            lock.replace(gitdir / "ORIG_HEAD")
        elif case == "foreign-lock":
            lock.rename(lock.with_name("original-lock-preserved"))
            lock.write_bytes(b"unknown owner bytes\n")
    if case == "unknown-lock":
        (gitdir / "index.lock").write_bytes(b"foreign content\n")
    elif case == "main-advanced":
        _git(root, "update-ref", "refs/heads/main", candidate, main)
    elif case == "head-replaced":
        (gitdir / "HEAD").rename(gitdir / "original-HEAD-preserved")
        (gitdir / "HEAD").write_bytes(b"ref: refs/heads/main\n")
    def inspect_scene(recovery=None):
        return integration.inspect_publication_recovery_window(root, request, plan, merge, recovery)
    if case in {"foreign-lock", "unknown-lock", "main-advanced", "head-replaced"}:
        with pytest.raises(contract.WorkflowContractError):
            inspect_scene()
        assert (peer / "private.txt").read_bytes() == b"private owner content\n"
        return
    scene = inspect_scene()
    restored = integration.validate_publication_recovery_observation(scene, request, plan, merge)
    attempt = {"request": request, "request_sha256": contract.canonical_digest(request),
               "state": "RESULT_RECORDED", "result": {"status": "INSUFFICIENT"},
               "exit": {"observed_at": "2026-09-14T00:00:00+00:00"},
               "checkout_effect": {"shape_only": True}, "git_launch": {"shape_only": True},
               "checkout_plan": {"plan": plan}}
    if merge is not None:
        attempt["git_merge"] = merge
    prior = copy.deepcopy(attempt)
    record = {"schema_version": "workflow_publication_head_recovery.v1",
              "request_sha256": attempt["request_sha256"],
              "checkout_effect_sha256": contract.canonical_digest(attempt["checkout_effect"]),
              "git_merge_sha256": contract.canonical_digest(merge) if merge is not None else None,
              "state": "INTENT", "observer": {"pid": 1, "creation_time": 1},
              "job_state": "ABSENT", "git_process": {"state": "EXITED"},
              "started_at": "2026-09-14T00:00:01+00:00", "completed_at": None,
              "scene_before": scene, "scene_after": None, "restored_orig_head": restored}
    attempt["head_recovery"] = record
    if case == "bad-scene":
        record["scene_before"]["checkouts"]["candidate_head"]["head"]["identity"] = [8, 8]
        with pytest.raises(contract.WorkflowContractError, match="PUBLICATION_RECOVERY_CHECKOUT"):
            _validate_publication_head_recovery(attempt)
        return
    _validate_publication_head_recovery(attempt)
    _validate_publication_head_recovery_transition(prior, attempt)
    intent = copy.deepcopy(attempt)
    for transition in reversed(plan["head_transitions"][:count]):
        role = transition["role"]
        checkout = topology["main_checkout"] if role == "peer_head" else topology[
            "candidate_checkout"
        ]
        current = inspect_scene(record)["checkouts"][role]["head"]
        contract.apply_bound_file(
            Path(checkout["gitdir"]["path"]), "HEAD", bytes.fromhex(current["bytes_hex"]),
            bytes.fromhex(transition["before"]["bytes_hex"]),
            expected_identity=tuple(current["identity"]),
            expected_root_identity=tuple(checkout["gitdir"]["identity"]),
            expected_parent_identities={},
        )
    current_orig = inspect_scene(record)["auxiliary"]["orig_head"]
    if current_orig != restored:
        contract.apply_bound_file(
            gitdir, "ORIG_HEAD", bytes.fromhex(current_orig["bytes_hex"]),
            bytes.fromhex(restored["bytes_hex"]) if restored["identity"] is not None else None,
            expected_identity=tuple(current_orig["identity"]),
            expected_root_identity=tuple(topology["candidate_checkout"]["gitdir"]["identity"]),
            expected_parent_identities={},
        )
    for lock in inspect_scene(record)["owned_locks"].values():
        contract.apply_bound_file(
            gitdir, "ORIG_HEAD.lock", bytes.fromhex(lock["bytes_hex"]), None,
            expected_identity=tuple(lock["identity"]),
            expected_root_identity=tuple(topology["candidate_checkout"]["gitdir"]["identity"]),
            expected_parent_identities={},
        )
    record.update(state="RESTORED", completed_at="2026-09-14T00:00:02+00:00",
                  scene_after=inspect_scene(record))
    _validate_publication_head_recovery(attempt)
    _validate_publication_head_recovery_transition(intent, attempt)
    _validate_publication_head_recovery_transition(attempt, copy.deepcopy(attempt))
    assert record["scene_after"]["worktree_inventory_sha256"] == topology[
        "worktree_inventory_sha256"
    ]
    if case == "orig-replaced":
        assert restored["identity"] != plan["orig_head"]["identity"]
        assert restored["bytes_hex"] == plan["orig_head"]["bytes_hex"]
    assert _git(root, "rev-parse", "HEAD") == candidate
    assert _git(root, "rev-parse", "refs/heads/main") == main
    assert (peer / "private.txt").read_bytes() == b"private owner content\n"


@pytest.mark.parametrize("case", ["success", "advanced", "race"])
def test_publication_prepared_gate_controls_actual_ff_only(small_repository, case):
    """Actual Git/parser composition, not original Full/Job publisher authority."""
    root = small_repository
    for name in ("scripts/architecture_arch005_checkout_guard.py",
                 "docs/requirements/DEVX-002_Governed_Development_Workflow_Skill.md"):
        destination = root / name
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(ROOT / name, destination)
    value = root / "value.txt"
    value.write_bytes(b"original main\n")
    _git(root, "add", "scripts", "docs", "value.txt")
    _git(root, "commit", "-m", "prepared production gate original main")
    main = _git(root, "rev-parse", "HEAD")
    branch = "codex/prepared-gate-candidate"
    _git(root, "switch", "-c", branch)
    value.write_bytes(b"intermediate\n")
    _git(root, "add", "value.txt")
    _git(root, "commit", "-m", "prepared gate concurrent writer target")
    intermediate = _git(root, "rev-parse", "HEAD")
    value.write_bytes(b"candidate\n")
    _git(root, "add", "value.txt")
    _git(root, "commit", "-m", "prepared gate exact candidate")
    candidate = _git(root, "rev-parse", "HEAD")
    topology = integration.inspect_local_publication_topology(
        root, candidate=candidate, expected_main=main,
    )
    request = {
        "candidate_sha": candidate, "expected_main_sha": main, "cwd": root.as_posix(),
        "publication_transaction_sha256": "1" * 64, "lease_id": "parser-test-not-authority",
    }
    request["local_publication_intent_sha256"] = contract.canonical_digest({
        "schema_version": "integration_publication_local_intent.v1",
        "transaction_sha256": request["publication_transaction_sha256"],
        "lease_id": request["lease_id"], "candidate_sha": candidate,
        "expected_main_sha": main, "topology": topology,
        "dispatch_allowed": False, "publication_allowed": False,
    })
    plan = integration.prepare_local_publication_checkout_plan(root, request)
    hooks = root / "outputs/prepared-gate-hooks"
    hooks.mkdir(parents=True)
    payload = hooks / "payload.json"
    payload.write_text(json.dumps({"plan": plan, "request": request}), encoding="utf-8")
    trace = hooks / "trace.jsonl"
    program = (
        "import json,os,subprocess,sys\nfrom pathlib import Path\n"
        "from ai_trading_system.platform.architecture.workflow_integration "
        "import classify_publication_prepared_reference_update\n"
        "from ai_trading_system.platform.architecture.workflow_contract "
        "import WorkflowContractError\n"
        f"payload=json.loads(Path({str(payload)!r}).read_text())\n"
        "raw=sys.stdin.buffer.read(); stage=sys.argv[1]\n"
        "event={'stage':stage,'raw_hex':raw.hex()}; code=0\n"
        "if stage=='prepared':\n"
        f" if {case == 'race'!r} and raw.endswith(b' ORIG_HEAD\\n'):\n"
        "  env={key:value for key,value in os.environ.items() "
        "if not key.upper().startswith('GIT_')}\n"
        "  env.update(GIT_CONFIG_NOSYSTEM='1',GIT_CONFIG_GLOBAL=os.devnull)\n"
        f"  result=subprocess.run(['git','update-ref','refs/heads/main',{intermediate!r},{main!r}],"
        "env=env,capture_output=True,timeout=15)\n"
        "  assert result.returncode==0,result.stderr\n"
        "  event['race_writer_exit']=result.returncode\n"
        " try:\n"
        "  event['kind']=classify_publication_prepared_reference_update("
        "payload['plan'],payload['request'],raw)\n"
        " except WorkflowContractError as exc:\n"
        "  event['error']=exc.code; code=97\n"
        f"with Path({str(trace)!r}).open('a',encoding='utf-8') as stream:\n"
        " stream.write(json.dumps(event)+'\\n')\n"
        "raise SystemExit(code)\n"
    )
    hook_program = hooks / "gate.py"
    hook_program.write_text(program, encoding="utf-8")
    (hooks / "reference-transaction").write_bytes((
        '#!/bin/sh\nexec "' + Path(sys.executable).as_posix() + '" -B "'
        + hook_program.as_posix() + '" "$@"\n'
    ).encode())
    if case == "advanced":
        _git(root, "update-ref", "refs/heads/main", intermediate, main)
    # Owned fixture handoff only; real peer ownership is not authorized here.
    _git(root, "symbolic-ref", "HEAD", "refs/heads/main")
    index = root / ".git/index"
    before_index = index.read_bytes()
    before_identity = [index.stat().st_dev, index.stat().st_ino]
    before_refs = _git(root, "show-ref")
    environment = {key: item for key, item in os.environ.items()
                   if not key.upper().startswith("GIT_")}
    environment.update(GIT_CONFIG_NOSYSTEM="1", GIT_CONFIG_GLOBAL=os.devnull,
                       GIT_OPTIONAL_LOCKS="0", PYTHONDONTWRITEBYTECODE="1",
                       PYTHONPATH=str(ROOT / "src"))
    command = ["git", "-c", "core.hooksPath=" + hooks.as_posix(),
               "merge", "--ff-only", "--no-edit", branch]
    result = subprocess.run(command, cwd=root, env=environment, capture_output=True,
                            text=True, timeout=45, check=False)
    events = [json.loads(line) for line in trace.read_text().splitlines()]
    actual_main = _git(root, "rev-parse", "main")
    orig_head = root / ".git/ORIG_HEAD"
    observation = {
        "case": case, "argv": command, "returncode": result.returncode,
        "stdout": result.stdout, "stderr": result.stderr, "events": events,
        "original_main": main, "intermediate": intermediate, "candidate": candidate,
        "actual_main": actual_main, "before_refs": before_refs,
        "after_refs": _git(root, "show-ref"), "before_index_identity": before_identity,
        "after_index_identity": [index.stat().st_dev, index.stat().st_ino],
        "before_index_sha256": hashlib.sha256(before_index).hexdigest(),
        "after_index_sha256": hashlib.sha256(index.read_bytes()).hexdigest(),
        "orig_head": orig_head.read_text() if orig_head.exists() else None,
    }
    (root.parent / "prepared-gate-observation.json").write_text(
        json.dumps(observation, indent=2), encoding="utf-8",
    )
    admitted = [event.get("kind") for event in events if event["stage"] == "prepared"]
    if case == "success":
        assert result.returncode == 0, observation
        assert actual_main == candidate != main
        assert admitted.count("FAST_FORWARD") == 1
        assert set(admitted) == {"ORIG_HEAD", "FAST_FORWARD", "AUTO_MERGE"}
    else:
        assert result.returncode != 0, observation
        assert actual_main == intermediate != candidate
        assert "FAST_FORWARD" not in admitted
        if case == "race":
            assert sum("race_writer_exit" in event for event in events) == 1
        else:
            assert any("error" in event for event in events)
    assert _git(root, "rev-parse", branch) == candidate
    assert index.read_bytes() == before_index
    assert value.read_bytes() == b"candidate\n"
    assert (root / ".git/HEAD").read_bytes() == b"ref: refs/heads/main\n"


def _synthetic_publication_checkout_plan(root):
    """Parser-only shape; no real lease, Full, Git capture, or publication capability."""
    gitdir = root / ".git"
    def absent(path):
        return {"path": path.as_posix(), "identity": None, "sha256": None, "size": None}
    topology = {
        "candidate_sha": "c" * 40, "expected_main_sha": "a" * 40,
        "candidate_branch": "refs/heads/codex/shape-only",
        "candidate_checkout": {"root": {"path": root.as_posix(), "identity": [1, 2]},
                               "gitdir": {"path": gitdir.as_posix()},
                               "common": {"path": gitdir.as_posix()},
                               "head": absent(gitdir / "HEAD")},
        "main_checkout": None,
    }
    topology["topology_sha256"] = contract.canonical_digest(topology)
    request = {
        "schema_version": "workflow_execution_request.v5", "cwd": root.as_posix(),
        "candidate_sha": "c" * 40, "expected_main_sha": "a" * 40,
        "publication_transaction_sha256": "b" * 64, "lease_id": "shape-only-not-real",
    }
    intent = {
        "schema_version": "integration_publication_local_intent.v1",
        "transaction_sha256": request["publication_transaction_sha256"],
        "lease_id": request["lease_id"], "candidate_sha": request["candidate_sha"],
        "expected_main_sha": request["expected_main_sha"], "topology": topology,
        "dispatch_allowed": False, "publication_allowed": False,
    }
    request["local_publication_intent_sha256"] = contract.canonical_digest(intent)
    names = ["AUTO_MERGE", "MERGE_HEAD", "MERGE_MSG", "MERGE_MODE", "MERGE_RR",
             "MERGE_AUTOSTASH", "SQUASH_MSG", "HEAD.lock", "index.lock", "ORIG_HEAD.lock",
             "AUTO_MERGE.lock", "refs/heads/main.lock", "packed-refs.lock"]
    plan = {
        "schema_version": "workflow_publication_checkout_plan.v1",
        "request_sha256": contract.canonical_digest(request), "topology": topology,
        "head_transitions": [{"role": "candidate_head",
                              "before": topology["candidate_checkout"]["head"],
                              "after_hex": b"ref: refs/heads/main\n".hex()}],
        "orig_head": absent(gitdir / "ORIG_HEAD"),
        "orig_head_after_hex": (request["expected_main_sha"] + "\n").encode().hex(),
        "reflogs": {"candidate": absent(gitdir / "logs/HEAD"),
                    "main": absent(gitdir / "logs/refs/heads/main")},
        "absent_paths": sorted((gitdir / name).as_posix() for name in names),
        "merge_argv_tail": ["merge", "--ff-only", "--no-edit", topology["candidate_branch"]],
        "peer_handoff_required": False, "dispatch_allowed": False,
        "publication_allowed": False, "resume_allowed": False,
    }
    plan["plan_sha256"] = contract.canonical_digest(plan)
    return request, plan


def _synthetic_publication_hook_definition(root):
    """Definition/parser inputs only: deliberately not a Full/Job capability."""
    request, plan = _synthetic_publication_checkout_plan(root)
    request.update(
        execution_kind="CONTROLLED_LOCAL_PUBLICATION", argv=[sys.executable, "-c", "pass"],
        stdout_path=(root / "outputs/original-attempt/stdout.log").as_posix(),
        publication_transaction_path=(root / "outputs/original-transaction.json").as_posix(),
    )
    plan["request_sha256"] = contract.canonical_digest(request)
    plan["plan_sha256"] = contract.canonical_digest(
        {key: value for key, value in plan.items() if key != "plan_sha256"}
    )
    policy = root / "config/original-policy.json"
    definition = integration.define_local_publication_hook_capsule(
        request, plan, actor="integration-coordinator", policy_path=policy,
    )
    return request, plan, policy, definition


@pytest.mark.parametrize("fault", [
    "none", "schema", "request", "plan", "root", "actor", "policy", "python", "entrypoint",
    "directory", "extra-hook", "missing-hook", "reorder", "script", "file-path", "argv",
    "size", "file-sha", "dispatch", "publication", "resume", "false-as-zero", "extra-field",
])
def test_publication_hook_definition_rederived_not_merely_rehashed(tmp_path, fault):
    request, plan, policy, definition = _synthetic_publication_hook_definition(tmp_path)
    original = copy.deepcopy(definition)
    if fault in {"schema", "request", "plan", "root", "actor", "policy", "python", "entrypoint",
                 "directory"}:
        field = {"schema": "schema_version", "request": "request_sha256",
                 "plan": "checkout_plan_sha256", "policy": "policy_path",
                 "python": "python_path", "entrypoint": "entrypoint_path"}.get(fault, fault)
        definition[field] += "-replacement"
    elif fault == "extra-hook":
        definition["files"].append(copy.deepcopy(definition["files"][0]))
    elif fault == "missing-hook":
        definition["files"].pop()
    elif fault == "reorder":
        definition["files"].reverse()
    elif fault == "script":
        replacement = b"#!/bin/sh\nexit 0\n"
        definition["files"][0].update(bytes_hex=replacement.hex(), size_bytes=len(replacement),
                                      sha256=hashlib.sha256(replacement).hexdigest())
    elif fault in {"file-path", "argv", "size", "file-sha"}:
        field = {"file-path": "path", "argv": "argv", "size": "size_bytes",
                 "file-sha": "sha256"}[fault]
        definition["files"][0][field] = {"path": "outside/reference-transaction",
            "argv": [sys.executable, "-c", "pass"], "size_bytes": 0, "sha256": "0" * 64}[field]
    elif fault in {"dispatch", "publication", "resume"}:
        definition[f"{fault}_allowed"] = True
    elif fault == "false-as-zero":
        definition["resume_allowed"] = 0
    elif fault == "extra-field":
        definition["caller_script"] = "arbitrary"
    definition["definition_sha256"] = contract.canonical_digest(
        {key: value for key, value in definition.items() if key != "definition_sha256"}
    )
    if fault == "none":
        integration.validate_local_publication_hook_capsule_definition(
            definition, request, plan, actor="integration-coordinator", policy_path=policy,
        )
        assert definition == original
        assert definition["directory"] == (
            "outputs/original-attempt/publication-" + contract.canonical_digest(request) + ".hooks"
        )
        assert [file["kind"] for file in definition["files"]] == [
            "reference-transaction", "post-merge",
        ]
        for file in definition["files"]:
            raw = bytes.fromhex(file["bytes_hex"])
            assert len(raw) == file["size_bytes"]
            assert hashlib.sha256(raw).hexdigest() == file["sha256"]
    else:
        with pytest.raises(contract.WorkflowContractError,
                           match="PUBLICATION_HOOK_DEFINITION_CHANGED"):
            integration.validate_local_publication_hook_capsule_definition(
                definition, request, plan, actor="integration-coordinator", policy_path=policy,
            )
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize("fault", [
    "actor", "kind", "python-relative", "stdout-outside", "transaction-outside",
    "stdout-traversal", "transaction-traversal", "newline", "policy-relative",
])
def test_publication_hook_definition_rejects_unbound_inputs(tmp_path, fault):
    request, plan, policy, _definition = _synthetic_publication_hook_definition(tmp_path)
    actor = "integration-coordinator"
    if fault == "actor":
        actor = "--replacement"
    elif fault == "kind":
        request["execution_kind"] = "EXECUTE_ARBITRARY_SCRIPT"
    elif fault == "python-relative":
        request["argv"][0] = "python.exe"
    elif fault == "stdout-outside":
        request["stdout_path"] = (tmp_path.parent / "outside/stdout.log").as_posix()
    elif fault == "transaction-outside":
        request["publication_transaction_path"] = (tmp_path.parent / "transaction.json").as_posix()
    elif fault == "stdout-traversal":
        request["stdout_path"] = (tmp_path / "outputs/../stdout.log").as_posix()
    elif fault == "transaction-traversal":
        request["publication_transaction_path"] = (
            tmp_path / "outputs/../transaction.json"
        ).as_posix()
    elif fault == "newline":
        request["argv"][0] += "\n"
    elif fault == "policy-relative":
        policy = Path("policy.json")
    plan["request_sha256"] = contract.canonical_digest(request)
    plan["plan_sha256"] = contract.canonical_digest(
        {key: value for key, value in plan.items() if key != "plan_sha256"}
    )
    with pytest.raises(contract.WorkflowContractError, match="PUBLICATION_HOOK_DEFINITION"):
        integration.define_local_publication_hook_capsule(request, plan, actor=actor,
                                                        policy_path=policy)
    assert list(tmp_path.iterdir()) == []


@pytest.mark.skipif(os.name != "nt", reason="Required Git-for-Windows shell platform")
@pytest.mark.parametrize("kind,stages", [
    ("reference-transaction", ["prepared"]), ("reference-transaction", ["committed"]),
    ("reference-transaction", ["aborted"]), ("post-merge", ["0"]), ("post-merge", ["1"]),
    ("reference-transaction", []), ("post-merge", ["0", "extra"]),
])
def test_publication_hook_actual_shell_preserves_fixed_argv_and_stdin(tmp_path, kind, stages):
    # This spy replaces only the fixture CLI to observe the actual shell boundary.
    # It is not a publisher, original Full, or production hook admission oracle.
    root = tmp_path / "quote ' dollar $ semicolon ; unicode 日本"
    root.mkdir()
    request, plan, policy, definition = _synthetic_publication_hook_definition(root)
    entrypoint = Path(definition["entrypoint_path"])
    entrypoint.parent.mkdir()
    entrypoint.write_text(
        "import json,os,sys\n"
        "print(json.dumps({'argv':sys.argv[1:],'cwd':os.getcwd(),"
        "'stdin_hex':sys.stdin.buffer.read().hex()}))\nraise SystemExit(23)\n",
        encoding="utf-8",
    )
    file = next(file for file in definition["files"] if file["kind"] == kind)
    hook = root / file["path"]
    hook.parent.mkdir(parents=True)
    hook.write_bytes(bytes.fromhex(file["bytes_hex"]))
    shell = Path("C:/Program Files/Git/bin/sh.exe")
    assert shell.is_file(), "Required installed Git-for-Windows shell unavailable"
    stdin = b"a" * 40 + b" " + b"c" * 40 + b" refs/heads/main\n\x00\xff"
    completed = subprocess.run([str(shell), str(hook), *stages], cwd=root, input=stdin,
                               capture_output=True, timeout=30, check=False)
    observation = {"shell": str(shell), "argv": [str(hook), *stages],
                   "returncode": completed.returncode, "stdout_hex": completed.stdout.hex(),
                   "stderr_hex": completed.stderr.hex(), "definition": definition}
    (tmp_path / "actual-shell-observation.json").write_text(
        json.dumps(observation), encoding="utf-8",
    )
    if len(stages) != 1:
        assert completed.returncode == 97
        assert completed.stdout == b""
    else:
        assert completed.returncode == 23, completed.stderr.decode(errors="replace")
        actual = json.loads(completed.stdout)
        assert actual["argv"] == [*file["argv"][3:], *stages]
        assert Path(actual["cwd"]) == root
        assert actual["stdin_hex"] == stdin.hex()
    integration.validate_local_publication_hook_capsule_definition(
        definition, request, plan, actor="integration-coordinator", policy_path=policy,
    )


@pytest.mark.parametrize("case", [
    "empty", "directory", "first-file", "complete", "extra", "order", "changed-parent",
    "root", "identity", "worker", "digest", "path", "target", "time", "missing-parent",
])
def test_publication_hook_created_object_prefix_shape(tmp_path, case):
    from ai_trading_system.platform.architecture.parallel_control import ParallelControlError
    from ai_trading_system.platform.architecture.workflow_coordination import (
        _validate_hook_created_objects,
    )
    from ai_trading_system.platform.architecture.workflow_execution import current_process_identity

    request, plan, _policy, definition = _synthetic_publication_hook_definition(tmp_path)
    worker = current_process_identity()
    capsule = {"request_sha256": contract.canonical_digest(request), "definition": definition,
               "worker_process": worker, "observed_at": "2026-09-14T00:00:00+00:00", "objects": []}
    execution = {"checkout_plan": {"plan": plan}, "hook_capsule": capsule}
    paths = [("directory", definition["directory"], None), *[
        ("file", row["path"], row["sha256"]) for row in definition["files"]
    ]]
    for index, (kind, path, target_sha) in enumerate(paths):
        parents = {"outputs": [1, 3], "outputs/original-attempt": [1, 4]}
        if index:
            parents[definition["directory"]] = [1, 10]
        capsule["objects"].append({
            "schema_version": "workflow_publication_hook_created_object.v1",
            "request_sha256": capsule["request_sha256"],
            "definition_sha256": definition["definition_sha256"], "kind": kind, "path": path,
            "root_identity": [1, 2], "parent_identities": parents, "file_identity": [1, 10 + index],
            "worker_process": worker, "observed_at": f"2026-09-14T00:00:0{index + 1}+00:00",
            "target_sha256": target_sha,
        })
    objects = capsule["objects"]
    if case in {"empty", "directory", "first-file"}:
        del objects[{"empty": 0, "directory": 1, "first-file": 2}[case]:]
    elif case == "extra":
        objects.append(copy.deepcopy(objects[-1]))
    elif case == "order":
        objects.reverse()
    elif case == "changed-parent":
        objects[1]["parent_identities"][definition["directory"]] = [1, 99]
    elif case == "root":
        objects[0]["root_identity"] = [1, 99]
    elif case == "identity":
        objects[0]["file_identity"] = [True, 10]
    elif case == "worker":
        objects[0]["worker_process"] = {**worker, "pid": worker["pid"] + 1}
    elif case == "digest":
        objects[0]["definition_sha256"] = "0" * 64
    elif case == "path":
        objects[0]["path"] = "outside"
    elif case == "target":
        objects[1]["target_sha256"] = "0" * 64
    elif case == "time":
        objects[2]["observed_at"] = "2026-09-13T00:00:00+00:00"
    elif case == "missing-parent":
        objects[1]["parent_identities"].pop("outputs")
    if case in {"empty", "directory", "first-file", "complete"}:
        _validate_hook_created_objects(execution)
    else:
        with pytest.raises(ParallelControlError, match="PUBLICATION_HOOK_CREATION"):
            _validate_hook_created_objects(execution)
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize("case", [
    "append", "overwrite-prefix", "append-two", "erase", "after-exit", "with-other-change",
    "after-ready", "after-preparation",
])
def test_publication_hook_created_object_append_transition(case):
    from ai_trading_system.platform.architecture.parallel_control import ParallelControlError
    from ai_trading_system.platform.architecture.workflow_coordination import (
        _validate_hook_capsule_transition,
    )

    old = {"state": "RUNNING", "hook_capsule": {"definition": "shape_only",
           "objects": [{"original": True}], "ready": None}}
    value = copy.deepcopy(old)
    value["hook_capsule"]["objects"].append({"new": True})
    if case == "overwrite-prefix":
        value["hook_capsule"]["objects"][0] = {"changed": True}
    elif case == "append-two":
        value["hook_capsule"]["objects"].append({"third": True})
    elif case == "erase":
        value["hook_capsule"]["objects"] = []
    elif case == "after-exit":
        old["state"] = value["state"] = "EXIT_CONFIRMED"
    elif case == "with-other-change":
        value["unrelated"] = "changed"
    elif case == "after-ready":
        old["hook_capsule"]["ready"] = value["hook_capsule"]["ready"] = {"shape_only": True}
    elif case == "after-preparation":
        old["main_preparation"] = value["main_preparation"] = {}
    if case == "append":
        _validate_hook_capsule_transition(old, value)
    else:
        with pytest.raises(ParallelControlError, match="PUBLICATION_HOOK_CREATION_TRANSITION"):
            _validate_hook_capsule_transition(old, value)


@pytest.mark.skipif(os.name != "nt", reason="Required original Windows native creation platform")
@pytest.mark.parametrize("case", [
    "file", "directory", "wrong-path", "wrong-kind", "closed-fd", "body-exception",
    "existing-file", "existing-directory", "hardlink", "nonempty",
])
def test_publication_hook_actual_created_descriptor(tmp_path, case):
    from ai_trading_system.platform.architecture.parallel_control import ParallelControlError
    from ai_trading_system.platform.architecture.workflow_coordination import (
        _publication_created_object_identity,
    )

    target = tmp_path / "fixed-object"
    root_info = tmp_path.stat()
    root_identity = (root_info.st_dev, root_info.st_ino)
    seen = []
    kind = "directory" if case in {"directory", "existing-directory"} else "file"
    if case == "existing-file":
        target.write_bytes(b"unknown-file-preserved")
    elif case == "existing-directory":
        target.mkdir()
        (target / "canary").write_bytes(b"unknown-directory-preserved")
    elif case in {"hardlink", "nonempty"}:
        target.write_bytes(b"" if case == "hardlink" else b"nonempty-preserved")
        if case == "hardlink":
            os.link(target, tmp_path / "other-hardlink")
        with target.open("rb") as stream, pytest.raises(ParallelControlError,
                                                       match="PUBLICATION_HOOK_CREATION_HANDLE"):
            _publication_created_object_identity(stream.fileno(), target, "file")
        return

    def observe(descriptor):
        info = os.fstat(descriptor)
        selected = descriptor
        if case == "closed-fd":
            selected = os.dup(descriptor)
            os.close(selected)
        identity = _publication_created_object_identity(
            selected,
            target if case != "wrong-path" else tmp_path / "other",
            kind if case != "wrong-kind" else "directory",
        )
        assert identity == [info.st_dev, info.st_ino]
        if kind == "file":
            assert info.st_size == 0
        seen.append(identity)
        if case == "body-exception":
            raise RuntimeError("callback did not persist")

    def create():
        if kind == "directory":
            contract.create_bound_recoverable_directory(
                tmp_path, target.name, record_created=observe,
                expected_root_identity=root_identity, expected_parent_identities={},
            )
        else:
            contract.create_bound_recoverable_file(
                tmp_path, target.name, b"fixed-bytes", record_created=observe,
                expected_root_identity=root_identity, expected_parent_identities={},
            )

    if case in {"file", "directory"}:
        create()
        info = target.stat()
        assert seen == [[info.st_dev, info.st_ino]]
        if kind == "file":
            assert target.read_bytes() == b"fixed-bytes"
    else:
        with pytest.raises((ParallelControlError, OSError, RuntimeError)):
            create()
        if case == "existing-file":
            assert target.read_bytes() == b"unknown-file-preserved" and seen == []
        elif case == "existing-directory":
            assert (target / "canary").read_bytes() == b"unknown-directory-preserved" and seen == []
        else:
            assert not target.exists()


def _synthetic_publication_git_launch(root):
    """Shape only; there is no original lease, Full or live native capability."""
    _before, outer = _synthetic_publication_ready(root)
    execution = outer["publication_attempts"][-1]
    capsule, plan = execution["hook_capsule"], execution["checkout_plan"]["plan"]
    request = execution["request"]
    install = root / "installed-git"
    identities = {}
    def identity(name):
        return identities.setdefault(name, [2, 1 + len(identities)])
    files = []
    for relative, count in (("cmd/git.exe", 2), ("mingw64/bin/git.exe", 4), ("usr/bin/sh.exe", 1)):
        parts = Path(relative).parts
        row = {
            "schema_version": "workflow_read_file_custody.v1" if count == 1
            else "workflow_read_file_custody.v2", "root": install.as_posix(),
            "relative": relative, "root_identity": identity(""),
            "parent_identities": {"/".join(parts[:index]): identity("/".join(parts[:index]))
                                  for index in range(1, len(parts))},
            "identity": identity(relative), "size_bytes": 3, "sha256": "f" * 64,
        }
        if count != 1:
            row["link_count"] = count
        files.append(row)
    worker = capsule["worker_process"]
    launch = {
        "argv": [(install / "cmd/git.exe").as_posix(), "-c", "core.hooksPath=" + (
            root / capsule["definition"]["directory"]
        ).as_posix(), "-c", "maintenance.auto=false", *plan["merge_argv_tail"]],
        "cwd": request["cwd"], "environment_sha256": "e" * 64,
        "stdout_path": (
            Path(request["stdout_path"]).parent
            / ("publication-merge-" + execution["request_sha256"] + ".stdout")
        ).as_posix(),
    }
    record = {
        "schema_version": "workflow_publication_git_launch.v1",
        "request_sha256": execution["request_sha256"], "checkout_plan_sha256": plan["plan_sha256"],
        "ready_sha256": contract.canonical_digest(capsule["ready"]),
        "process": {"pid": worker["pid"] + 1, "creation_time": worker["creation_time"] + 1},
        "worker_process": copy.deepcopy(worker), "launch_binding": launch,
        "git_file_custodies": files, "observed_at": "2026-09-14T00:00:06+00:00",
    }
    record["pre_resume_sha256"] = contract.canonical_digest({
        "schema_version": "workflow_inherited_child_pre_resume.v2",
        "owner_resume_state": "NOT_RESUMED", "process": record["process"],
        "worker_process": worker, "job_name": request["job_name"], "launch_binding": launch,
        "read_file_custodies": [*capsule["ready"]["inputs"]["read_file_custodies"], *files],
        "dispatch_allowed": False, "publication_allowed": False,
    })
    execution["git_launch"] = record
    return execution


@pytest.mark.parametrize("fault", [
    "none", "extra", "schema", "request", "plan", "ready", "worker", "self-child",
    "old-child", "early-time", "naive-time", "wrong-cwd", "wrong-env", "wrong-stdout",
    "wrong-argv", "wrong-git", "relative-git", "extra-file", "file-schema", "file-root",
    "file-relative", "file-count", "file-size", "file-hash", "file-parent", "file-identity",
    "namespace", "pre-resume", "main-preparation", "before-running", "ready-absent",
])
def test_publication_git_launch_binding_shape(tmp_path, fault):
    from ai_trading_system.platform.architecture.parallel_control import ParallelControlError
    from ai_trading_system.platform.architecture.workflow_coordination import (
        _validate_publication_git_launch,
    )

    execution = _synthetic_publication_git_launch(tmp_path)
    record = execution["git_launch"]
    launch, files = record["launch_binding"], record["git_file_custodies"]
    mapped = {"schema": "schema_version", "request": "request_sha256",
              "plan": "checkout_plan_sha256", "ready": "ready_sha256",
              "pre-resume": "pre_resume_sha256"}
    if fault in mapped:
        record[mapped[fault]] = "bad"
    elif fault == "extra":
        record["resume_allowed"] = True
    elif fault == "worker":
        record["worker_process"]["creation_time"] += 1
    elif fault == "self-child":
        record["process"] = copy.deepcopy(record["worker_process"])
    elif fault == "old-child":
        record["process"]["creation_time"] = record["worker_process"]["creation_time"] - 1
    elif fault in {"early-time", "naive-time"}:
        record["observed_at"] = ("2026-09-14T00:00:04+00:00" if fault == "early-time"
                                 else "2026-09-14T00:00:06")
    elif fault in {"wrong-cwd", "wrong-env", "wrong-stdout"}:
        launch[{"wrong-cwd": "cwd", "wrong-env": "environment_sha256",
                "wrong-stdout": "stdout_path"}[fault]] = "bad"
    elif fault == "wrong-argv":
        launch["argv"][4] = "--force"
    elif fault == "wrong-git":
        launch["argv"][0] = str(tmp_path / "installed-git/cmd/other.exe")
    elif fault == "relative-git":
        launch["argv"][0] = "cmd/git.exe"
    elif fault == "extra-file":
        files.append(copy.deepcopy(files[0]))
    elif fault in {"file-schema", "file-root", "file-relative", "file-count", "file-size",
                   "file-hash", "file-parent", "file-identity"}:
        field, replacement = {
            "file-schema": ("schema_version", "workflow_read_file_custody.v1"),
            "file-root": ("root", "wrong"), "file-relative": ("relative", "bin/git.exe"),
            "file-count": ("link_count", True), "file-size": ("size_bytes", 0),
            "file-hash": ("sha256", "bad"), "file-parent": ("parent_identities", {}),
            "file-identity": ("identity", [True, 2]),
        }[fault]
        files[0][field] = replacement
    elif fault == "namespace":
        files[1]["root_identity"] = [2, 999]
    elif fault == "main-preparation":
        execution["main_preparation"] = {}
    elif fault == "before-running":
        execution["state"] = "RESERVED"
    elif fault == "ready-absent":
        execution["hook_capsule"]["ready"] = None
    if fault == "none":
        _validate_publication_git_launch(execution)
    else:
        with pytest.raises(ParallelControlError):
            _validate_publication_git_launch(execution)
    assert list(tmp_path.iterdir()) == []


def _synthetic_publication_checkout_effect(root, *, switched=False):
    execution = _synthetic_publication_git_launch(root)
    plan = execution["checkout_plan"]["plan"]
    for transition in plan["head_transitions"]:
        transition["before"]["identity"] = [3, 9]
    record = {
        "schema_version": "workflow_publication_checkout_effect.v1",
        "request_sha256": execution["request_sha256"],
        "git_launch_sha256": contract.canonical_digest(execution["git_launch"]),
        "checkout_plan_sha256": plan["plan_sha256"],
        "worker_process": copy.deepcopy(execution["git_launch"]["worker_process"]),
        "state": "HEAD_HANDOFF_INTENT", "peer_handoff_authorized": False,
        "started_at": "2026-09-14T00:00:07+00:00", "completed_at": None,
        "head_observations": [],
    }
    if switched:
        record.update(state="HEADS_SWITCHED", completed_at="2026-09-14T00:00:08+00:00")
        for transition in plan["head_transitions"]:
            raw = bytes.fromhex(transition["after_hex"])
            record["head_observations"].append({
                "role": transition["role"], "path": transition["before"]["path"],
                "identity": transition["before"]["identity"], "bytes_hex": raw.hex(),
                "size": len(raw), "sha256": hashlib.sha256(raw).hexdigest(),
            })
    execution["checkout_effect"] = record
    return execution


@pytest.mark.parametrize("fault", [
    "valid", "legacy", "path", "nonempty", "replacement", "committed-lock", "other-reference",
    "missing-post", "missing-prepared", "no-identity", "bad-hash", "rewrite-history", "append-two",
])
def test_publication_auto_merge_cleanup_receipt_contract(tmp_path, fault):
    """Synthetic record contract, not an original Full or native admission claim."""
    from ai_trading_system.platform.architecture.parallel_control import ParallelControlError
    from ai_trading_system.platform.architecture.workflow_coordination import (
        _validate_publication_git_merge,
        _validate_publication_git_merge_transition,
    )

    execution = _synthetic_publication_checkout_effect(tmp_path, switched=True)
    launch = execution["git_launch"]
    plan = execution["checkout_plan"]["plan"]
    # The effect-only helper substitutes a HEAD identity without rederiving
    # its plan. Restore its original absent parser fixture before composing
    # the independent merge-record contract (still no runtime authority).
    for transition in plan["head_transitions"]:
        transition["before"]["identity"] = None
    integration.validate_local_publication_checkout_plan(plan, execution["request"])
    checkout = plan["topology"]["candidate_checkout"]
    main, candidate = (execution["request"][key] for key in ("expected_main_sha", "candidate_sha"))
    def metadata(path, raw, identity):
        return {"path": path.as_posix(), "identity": identity, "size": len(raw),
                "bytes_hex": raw.hex(), "sha256": hashlib.sha256(raw).hexdigest()}
    cleanup = metadata(Path(checkout["common"]["path"]) / "packed-refs.lock", b"", [7, 11])
    hooks = []
    for index, (reference, stage) in enumerate([
        ("ORIG_HEAD", "prepared"), ("ORIG_HEAD", "committed"),
        ("FAST_FORWARD", "prepared"), ("FAST_FORWARD", "committed"),
        (None, "0"), ("AUTO_MERGE", "aborted"), ("AUTO_MERGE", "prepared"),
        ("AUTO_MERGE", "committed"),
    ]):
        raw = (f"{'0' * 40} {main} ORIG_HEAD\n" if reference == "ORIG_HEAD" else
               f"{main} {candidate} HEAD\n{main} {candidate} refs/heads/main\n"
               if reference == "FAST_FORWARD" else
               f"{'0' * 40} {'0' * 40} AUTO_MERGE\n" if reference == "AUTO_MERGE" else "")
        prepared = None
        if stage == "prepared" and reference in {"ORIG_HEAD", "FAST_FORWARD"}:
            path = (Path(checkout["gitdir"]["path"]) / "ORIG_HEAD.lock"
                    if reference == "ORIG_HEAD" else
                    Path(checkout["common"]["path"]) / "refs/heads/main.lock")
            prepared = metadata(path, ((main if reference == "ORIG_HEAD" else candidate)
                                       + "\n").encode(), [7, index + 20])
        row = {"kind": "post-merge" if reference is None else "reference-transaction",
               "stage": stage, "reference_kind": reference, "updates_hex": raw.encode().hex(),
               "process_chain": [{"pid": launch["process"]["pid"] + 10 + index,
                                  "creation_time": launch["process"]["creation_time"] + 10 + index},
                                 copy.deepcopy(launch["process"])],
               "observed_at": f"2026-09-14T00:00:{10 + index:02d}+00:00",
               "prepared_file": prepared}
        if reference == "AUTO_MERGE":
            row["cleanup_lock"] = copy.deepcopy(cleanup) if stage != "committed" else None
        hooks.append(row)
    execution["git_merge"] = {
        "schema_version": "workflow_publication_git_merge.v1",
        "request_sha256": execution["request_sha256"],
        "git_launch_sha256": contract.canonical_digest(launch),
        "checkout_effect_sha256": contract.canonical_digest(execution["checkout_effect"]),
        "worker_process": copy.deepcopy(launch["worker_process"]),
        "resume_intent_at": "2026-09-14T00:00:09+00:00", "hooks": hooks, "exit": None,
    }
    _validate_publication_git_merge(execution)  # Prove construction before fault injection.
    old = copy.deepcopy(execution)
    old["git_merge"]["hooks"] = old["git_merge"]["hooks"][:-1]
    _validate_publication_git_merge_transition(old, execution)
    if fault == "legacy":
        for row in hooks:
            row.pop("cleanup_lock", None)
    elif fault == "path":
        hooks[6]["cleanup_lock"]["path"] = str(tmp_path / "foreign.lock")
    elif fault == "nonempty":
        hooks[6]["cleanup_lock"] = metadata(Path(cleanup["path"]), b"foreign", [7, 11])
    elif fault == "replacement":
        hooks[6]["cleanup_lock"]["identity"] = [7, 99]
    elif fault == "committed-lock":
        hooks[7]["cleanup_lock"] = cleanup
    elif fault == "other-reference":
        hooks[0]["cleanup_lock"] = cleanup
    elif fault == "missing-post":
        hooks.pop(4)
    elif fault == "missing-prepared":
        hooks[6]["cleanup_lock"] = None
    elif fault == "no-identity":
        hooks[6]["cleanup_lock"]["identity"] = None
    elif fault == "bad-hash":
        hooks[6]["cleanup_lock"]["sha256"] = "f" * 64
    elif fault == "rewrite-history":
        hooks[5]["cleanup_lock"]["identity"] = [7, 99]
    elif fault == "append-two":
        old["git_merge"]["hooks"].pop()
    if fault in {"valid", "legacy"}:
        _validate_publication_git_merge(execution)
    elif fault in {"rewrite-history", "append-two"}:
        with pytest.raises(ParallelControlError, match="PUBLICATION_GIT_MERGE_TRANSITION"):
            _validate_publication_git_merge_transition(old, execution)
    else:
        with pytest.raises((ParallelControlError, contract.WorkflowContractError)):
            _validate_publication_git_merge(execution)


@pytest.mark.parametrize("fault", [
    "intent", "switched", "extra", "schema", "request", "plan", "launch", "worker",
    "peer", "peer-zero", "early-time", "naive-time", "premature-heads", "missing-heads",
    "wrong-head", "wrong-head-identity", "missing-completed", "early-completed", "no-launch",
])
def test_publication_checkout_effect_binding_shape(tmp_path, fault):
    from ai_trading_system.platform.architecture.parallel_control import ParallelControlError
    from ai_trading_system.platform.architecture.workflow_coordination import (
        _validate_publication_checkout_effect,
    )

    execution = _synthetic_publication_checkout_effect(tmp_path, switched=fault != "intent")
    record = execution["checkout_effect"]
    mapped = {"schema": "schema_version", "request": "request_sha256",
              "plan": "checkout_plan_sha256", "launch": "git_launch_sha256"}
    if fault in mapped:
        record[mapped[fault]] = "bad"
    elif fault == "extra":
        record["publication_allowed"] = True
    elif fault == "worker":
        record["worker_process"]["creation_time"] += 1
    elif fault in {"peer", "peer-zero"}:
        record["peer_handoff_authorized"] = True if fault == "peer" else 0
    elif fault in {"early-time", "naive-time"}:
        record["started_at"] = ("2026-09-14T00:00:05+00:00" if fault == "early-time"
                                else "2026-09-14T00:00:07")
    elif fault == "premature-heads":
        record["state"] = "HEAD_HANDOFF_INTENT"
    elif fault == "missing-heads":
        record["head_observations"] = []
    elif fault == "wrong-head":
        record["head_observations"][0]["bytes_hex"] = b"wrong head\n".hex()
    elif fault == "wrong-head-identity":
        record["head_observations"][0]["identity"] = [3, 10]
    elif fault == "missing-completed":
        record["completed_at"] = None
    elif fault == "early-completed":
        record["completed_at"] = "2026-09-14T00:00:06+00:00"
    elif fault == "no-launch":
        del execution["git_launch"]
    if fault in {"intent", "switched"}:
        _validate_publication_checkout_effect(execution)
    else:
        with pytest.raises(ParallelControlError):
            _validate_publication_checkout_effect(execution)
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize("case", [
    "first", "switched", "unchanged", "skip-intent", "drop", "rewrite-intent", "repeat-switch",
    "other-field", "no-launch", "from-reserved", "change-state", "rewrite-completed",
])
def test_publication_checkout_effect_transition_contract(tmp_path, case):
    from ai_trading_system.platform.architecture.parallel_control import ParallelControlError
    from ai_trading_system.platform.architecture.workflow_coordination import (
        _validate_publication_checkout_effect_transition,
    )

    intent = _synthetic_publication_checkout_effect(tmp_path)
    switched = _synthetic_publication_checkout_effect(tmp_path, switched=True)
    old = {key: item for key, item in intent.items() if key != "checkout_effect"}
    value = copy.deepcopy(intent)
    if case in {"switched", "rewrite-intent"}:
        old, value = intent, switched
        if case == "rewrite-intent":
            value["checkout_effect"]["started_at"] = "2026-09-14T00:00:07.1+00:00"
    elif case == "unchanged":
        old, value = intent, copy.deepcopy(intent)
    elif case == "skip-intent":
        value = switched
    elif case == "drop":
        old, value = intent, old
    elif case in {"repeat-switch", "rewrite-completed"}:
        old, value = switched, copy.deepcopy(switched)
        value["checkout_effect"]["completed_at"] = "2026-09-14T00:00:09+00:00"
    elif case == "other-field":
        value["unrelated"] = True
    elif case == "no-launch":
        del old["git_launch"]
        del value["git_launch"]
    elif case == "from-reserved":
        old["state"] = value["state"] = "RESERVED"
    elif case == "change-state":
        value["state"] = "EXIT_CONFIRMED"
    if case in {"first", "switched", "unchanged"}:
        _validate_publication_checkout_effect_transition(old, value)
    else:
        with pytest.raises(ParallelControlError, match="PUBLICATION_CHECKOUT_EFFECT_TRANSITION"):
            _validate_publication_checkout_effect_transition(old, value)
    assert list(tmp_path.iterdir()) == []


def _synthetic_publication_ready(root):
    """In-memory shape only: cannot pass the real Full/lease/worker admission."""
    from ai_trading_system.platform.architecture.workflow_execution import current_process_identity

    request, plan, policy, _definition = _synthetic_publication_hook_definition(root)
    request["local_publication_event_id"] = "d" * 64
    request["job_name"] = "Local\\AITS-DEVX015-shape-only-not-a-native-job"
    request["argv"][0] = str(root / "installed/python.exe")
    plan["request_sha256"] = contract.canonical_digest(request)
    plan["plan_sha256"] = contract.canonical_digest(
        {key: value for key, value in plan.items() if key != "plan_sha256"},
    )
    definition = integration.define_local_publication_hook_capsule(
        request, plan, actor="integration-coordinator", policy_path=policy,
    )
    worker = current_process_identity()
    native = {root: [1, 2], root / "outputs": [1, 3],
              root / "outputs/original-attempt": [1, 4],
              root / definition["directory"]: [1, 10]}
    def identity(path):
        return native.setdefault(path, [1, 100 + len(native)])
    def binding(relative, size, digest):
        parts = Path(relative).parts
        return {"schema_version": "workflow_read_file_custody.v1", "root": root.as_posix(),
                "relative": relative, "root_identity": [1, 2],
                "parent_identities": {
                    "/".join(parts[:n]): identity(root.joinpath(*parts[:n]))
                    for n in range(1, len(parts))
                }, "identity": identity(root / relative), "size_bytes": size, "sha256": digest}
    capsule = {"schema_version": "workflow_publication_hook_capsule.v1",
               "request_sha256": contract.canonical_digest(request), "definition": definition,
               "worker_process": worker, "observed_at": "2026-09-14T00:00:00+00:00",
               "objects": [], "ready": None}
    for index, (kind, path, digest) in enumerate([
        ("directory", definition["directory"], None),
        *[("file", row["path"], row["sha256"]) for row in definition["files"]],
    ]):
        file = binding(path, 0, "0" * 64)
        capsule["objects"].append({
            "schema_version": "workflow_publication_hook_created_object.v1",
            "request_sha256": capsule["request_sha256"],
            "definition_sha256": definition["definition_sha256"], "kind": kind, "path": path,
            "root_identity": file["root_identity"], "parent_identities": file["parent_identities"],
            "file_identity": file["identity"], "worker_process": worker,
            "observed_at": f"2026-09-14T00:00:0{index + 1}+00:00", "target_sha256": digest,
        })
    files = [binding("installed/python.exe", 1, "1" * 64),
             binding("installed/python311.dll", 2, "2" * 64),
             binding("installed/code.py", 3, "3" * 64)]
    distribution_hash = hashlib.sha256((json.dumps(
        [str(root / "installed/code.py"), 3, "3" * 64], separators=(",", ":"),
    ) + "\n").encode()).hexdigest()
    runtime = {
        "schema_version": "acceptance_runtime_identity.v1",
        "executable": str(root / "installed/python.exe"), "executable_sha256": "1" * 64,
        "engine": str(root / "installed/python311.dll"), "engine_sha256": "2" * 64,
        "prefix": str(root / "installed"), "base_prefix": str(root / "installed"),
        "python_version": "3.11.9 shape-only", "implementation": "cpython", "platform": "win32",
        "distribution_inventory_sha256": "4" * 64, "distribution_count": 1,
        "distribution_code": {"sha256": distribution_hash, "file_count": 1, "size_bytes": 3},
        "environment_sha256": "5" * 64,
    }
    files.extend(binding(row["path"], row["size_bytes"], row["sha256"])
                 for row in definition["files"])
    files.extend([binding("evidence.json", 2, "6" * 64),
                  binding(Path(definition["entrypoint_path"]).relative_to(root).as_posix(),
                          1, "9" * 64),
                  binding(Path(request["publication_transaction_path"]).relative_to(root).as_posix(),
                          2, "7" * 64),
                  binding(policy.relative_to(root).as_posix(), 2, "8" * 64)])
    execution = {"request": request, "request_sha256": capsule["request_sha256"],
                 "state": "RUNNING", "hook_capsule": capsule,
                 "checkout_plan": {"plan": plan, "worker_process": worker,
                                   "observed_at": "2026-09-13T23:59:59+00:00"}}
    before = {"original_full": "shape-only-not-authority",
              "publication_attempts": [copy.deepcopy(execution)]}
    profile = {
        "schema_version": "full_publication_profile_inspection.v1", "status": "PASS",
        "scope": "PROFILE_MANDATORY_AND_READINESS_IDENTITY",
        "candidate_sha": request["candidate_sha"],
        "transaction_sha256": request["publication_transaction_sha256"],
        "head_event_id": request["local_publication_event_id"],
        "execution_sha256": contract.canonical_digest(before),
        "captures": [{"path": (root / "evidence.json").as_posix(),
                      "size_bytes": 2, "sha256": "6" * 64},
                     {"path": definition["entrypoint_path"], "size_bytes": 1, "sha256": "9" * 64}],
        "dispatch_performed": False, "publication_performed": False,
    }
    inputs = {
        "schema_version": "workflow_publication_live_inputs.v1",
        "request_sha256": capsule["request_sha256"],
        "definition_sha256": definition["definition_sha256"], "worker_process": worker,
        "observed_at": "2026-09-14T00:00:04+00:00",
        "profile_inspection": profile, "runtime_identity": runtime, "runtime_file_count": 3,
        "read_file_custodies": files,
        "dispatch_allowed": False, "publication_allowed": False, "resume_allowed": False,
    }
    capsule["ready"] = {"schema_version": "workflow_publication_hook_ready.v1",
                        "request_sha256": capsule["request_sha256"], "inputs": inputs,
                        "recorded_at": "2026-09-14T00:00:05+00:00"}
    return before, {**before, "publication_attempts": [execution]}


@pytest.mark.parametrize("fault", [
    "none", "record-schema", "request", "definition", "worker", "early-time", "naive-time",
    "incomplete-objects", "grant-resume", "false-as-zero", "profile-event", "profile-dispatch-zero",
    "profile-duplicate", "runtime-count", "runtime-size", "runtime-hash", "runtime-executable",
    "runtime-file-sha", "file-identity", "file-parent", "file-root", "file-budget",
    "hook-leaf", "capture-sha", "control-path", "extra-file", "namespace", "missing-entrypoint",
    "runtime-root-relative", "runtime-root-traversal",
])
def test_publication_ready_binding_shape(tmp_path, fault):
    from ai_trading_system.platform.architecture.parallel_control import ParallelControlError
    from ai_trading_system.platform.architecture.workflow_coordination import _validate_hook_capsule

    _before, after = _synthetic_publication_ready(tmp_path)
    execution = after["publication_attempts"][-1]
    capsule = execution["hook_capsule"]
    ready = capsule["ready"]
    inputs = ready["inputs"]
    files = inputs["read_file_custodies"]
    profile = inputs["profile_inspection"]
    if fault == "record-schema":
        ready["schema_version"] = "unbound"
    elif fault == "request":
        ready["request_sha256"] = "0" * 64
    elif fault == "definition":
        inputs["definition_sha256"] = "0" * 64
    elif fault == "worker":
        inputs["worker_process"] = {**inputs["worker_process"], "pid": 1}
    elif fault == "early-time":
        ready["recorded_at"] = "2026-09-14T00:00:03+00:00"
    elif fault == "naive-time":
        inputs["observed_at"] = "2026-09-14T00:00:04"
    elif fault == "incomplete-objects":
        capsule["objects"].pop()
    elif fault in {"grant-resume", "false-as-zero"}:
        inputs["resume_allowed"] = True if fault == "grant-resume" else 0
    elif fault == "profile-event":
        profile["head_event_id"] = "0" * 64
    elif fault == "profile-dispatch-zero":
        profile["dispatch_performed"] = 0
    elif fault == "profile-duplicate":
        profile["captures"].append(copy.deepcopy(profile["captures"][0]))
    elif fault == "runtime-count":
        inputs["runtime_file_count"] = True
    elif fault in {"runtime-size", "runtime-hash"}:
        inputs["runtime_identity"]["distribution_code"][
            "size_bytes" if fault == "runtime-size" else "sha256"
        ] = 4 if fault == "runtime-size" else "0" * 64
    elif fault == "runtime-executable":
        inputs["runtime_identity"]["executable"] += "-replaced"
    elif fault == "runtime-file-sha":
        files[2]["sha256"] = "0" * 64
    elif fault == "file-identity":
        files[2]["identity"] = [True, 7]
    elif fault == "file-parent":
        files[2]["parent_identities"] = {}
    elif fault == "file-root":
        files[5]["root_identity"] = [1, 99]
    elif fault == "runtime-root-relative":
        files[2]["root"] = "relative-runtime"
    elif fault == "runtime-root-traversal":
        files[2]["root"] += "/.."
    elif fault == "file-budget":
        files[5]["size_bytes"] = 16 * 1024**2 + 1
    elif fault == "hook-leaf":
        files[3]["identity"] = [1, 99]
    elif fault == "capture-sha":
        files[5]["sha256"] = "0" * 64
    elif fault == "control-path":
        files[-1]["relative"] = "config/other-policy.json"
    elif fault == "extra-file":
        files.append(copy.deepcopy(files[-1]))
    elif fault == "namespace":
        files[2]["parent_identities"]["installed"] = [1, 999]
    elif fault == "missing-entrypoint":
        files[6]["relative"] = "scripts/not-entrypoint.py"
        profile["captures"][1]["path"] = (tmp_path / files[6]["relative"]).as_posix()
    if fault == "none":
        _validate_hook_capsule(execution, actor="integration-coordinator")
    else:
        with pytest.raises((ParallelControlError, contract.WorkflowContractError)):
            _validate_hook_capsule(execution, actor="integration-coordinator")
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize("case", [
    "append", "unchanged", "terminal-preserve", "wrong-prior", "self-hash", "other-change",
    "after-preparation", "without-objects", "rewrite", "delete",
])
def test_publication_ready_original_outer_transition(tmp_path, case):
    from ai_trading_system.platform.architecture.parallel_control import ParallelControlError
    from ai_trading_system.platform.architecture.workflow_coordination import (
        _validate_hook_capsule_transition,
        _validate_publication_ready_outer_transition,
    )

    before, after = _synthetic_publication_ready(tmp_path)
    ready = after["publication_attempts"][-1]["hook_capsule"]["ready"]
    if case == "wrong-prior":
        ready["inputs"]["profile_inspection"]["execution_sha256"] = "0" * 64
    elif case == "self-hash":
        ready["inputs"]["profile_inspection"]["execution_sha256"] = contract.canonical_digest(after)
    elif case == "other-change":
        after["original_full"] = "replaced"
    elif case == "after-preparation":
        before["publication_attempts"][-1]["main_preparation"] = {}
        after["publication_attempts"][-1]["main_preparation"] = {}
        ready["inputs"]["profile_inspection"]["execution_sha256"] = (
            contract.canonical_digest(before)
        )
    elif case == "without-objects":
        before["publication_attempts"][-1]["hook_capsule"]["objects"] = []
        after["publication_attempts"][-1]["hook_capsule"]["objects"] = []
        ready["inputs"]["profile_inspection"]["execution_sha256"] = (
            contract.canonical_digest(before)
        )
    elif case in {"unchanged", "terminal-preserve", "rewrite", "delete"}:
        before = copy.deepcopy(after)
        if case == "terminal-preserve":
            after["publication_attempts"][-1]["state"] = "EXIT_CONFIRMED"
        elif case == "rewrite":
            ready["recorded_at"] = "2026-09-14T00:00:06+00:00"
        elif case == "delete":
            after["publication_attempts"][-1]["hook_capsule"]["ready"] = None
    def validate():
        _validate_publication_ready_outer_transition(before, after)
        _validate_hook_capsule_transition(
            before["publication_attempts"][-1], after["publication_attempts"][-1],
        )
    if case in {"append", "unchanged", "terminal-preserve"}:
        validate()
    else:
        with pytest.raises(ParallelControlError, match="PUBLICATION_HOOK_"):
            validate()
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize("fault", [
    "none", "schema", "request", "worker", "time", "early-time", "missing-plan", "definition",
    "actor", "objects", "objects-shape", "ready", "state",
])
def test_publication_hook_capsule_initial_binding_shape(tmp_path, fault):
    from ai_trading_system.platform.architecture.parallel_control import ParallelControlError
    from ai_trading_system.platform.architecture.workflow_coordination import _validate_hook_capsule
    from ai_trading_system.platform.architecture.workflow_execution import current_process_identity

    request, plan, _policy, definition = _synthetic_publication_hook_definition(tmp_path)
    worker = current_process_identity()
    record = {"schema_version": "workflow_publication_hook_capsule.v1",
              "request_sha256": contract.canonical_digest(request), "definition": definition,
              "worker_process": worker, "observed_at": "2026-09-14T00:00:01+00:00",
              "objects": [], "ready": None}
    execution = {"request": request, "request_sha256": contract.canonical_digest(request),
                 "state": "RUNNING", "hook_capsule": record,
                 "checkout_plan": {"plan": plan, "worker_process": worker,
                                   "observed_at": "2026-09-14T00:00:00+00:00"}}
    actor = "integration-coordinator"
    if fault == "schema":
        record["schema_version"] = "replacement"
    elif fault == "request":
        record["request_sha256"] = "0" * 64
    elif fault == "worker":
        record["worker_process"] = {**worker, "pid": worker["pid"] + 1}
    elif fault == "time":
        record["observed_at"] = "2026-09-14T00:00:01"
    elif fault == "early-time":
        record["observed_at"] = "2026-09-13T00:00:00+00:00"
    elif fault == "missing-plan":
        del execution["checkout_plan"]
    elif fault == "definition":
        definition["files"][0]["sha256"] = "0" * 64
    elif fault == "actor":
        actor = "other-actor"
    elif fault == "objects":
        record["objects"] = [{"claimed_created": True}]
    elif fault == "objects-shape":
        record["objects"] = {}
    elif fault == "ready":
        record["ready"] = {"claimed_ready": True}
    elif fault == "state":
        execution["state"] = "RESERVED"
    if fault == "none":
        _validate_hook_capsule(execution, actor=actor)
    else:
        with pytest.raises((ParallelControlError, contract.WorkflowContractError)):
            _validate_hook_capsule(execution, actor=actor)
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize("case", [
    "append", "unchanged", "terminal-preserve", "delete", "overwrite", "replace-definition",
    "without-plan", "after-preparation", "reserved", "with-other-change", "with-exit",
])
def test_publication_hook_capsule_initial_transition_contract(case):
    from ai_trading_system.platform.architecture.parallel_control import ParallelControlError
    from ai_trading_system.platform.architecture.workflow_coordination import (
        _validate_hook_capsule_transition,
    )

    # Isolated transition predicate only; these records cannot enter a real lease.
    old = {"state": "RUNNING", "checkout_plan": {"shape_only": True}}
    value = {**old, "hook_capsule": {"definition": "shape_only", "objects": [], "ready": None}}
    if case in {"unchanged", "terminal-preserve", "delete", "overwrite", "replace-definition"}:
        old = copy.deepcopy(value)
        if case == "terminal-preserve":
            value["state"] = "EXIT_CONFIRMED"
        elif case == "delete":
            del value["hook_capsule"]
        elif case == "overwrite":
            value["hook_capsule"] = None
        elif case == "replace-definition":
            value["hook_capsule"]["definition"] = "replacement"
    elif case == "without-plan":
        del old["checkout_plan"]
        del value["checkout_plan"]
    elif case == "after-preparation":
        old["main_preparation"] = value["main_preparation"] = {}
    elif case == "reserved":
        old["state"] = value["state"] = "RESERVED"
    elif case == "with-other-change":
        value["unrelated"] = "changed"
    elif case == "with-exit":
        value["state"] = "EXIT_CONFIRMED"
    if case in {"append", "unchanged", "terminal-preserve"}:
        _validate_hook_capsule_transition(old, value)
    else:
        with pytest.raises(ParallelControlError, match="PUBLICATION_HOOK_CAPSULE"):
            _validate_hook_capsule_transition(old, value)


@pytest.mark.parametrize("fault", ["none", "schema", "request", "worker", "time", "plan"])
def test_publication_checkout_plan_binding_shape(tmp_path, fault):
    from ai_trading_system.platform.architecture.parallel_control import ParallelControlError
    from ai_trading_system.platform.architecture.workflow_coordination import (
        _validate_checkout_plan,
    )
    from ai_trading_system.platform.architecture.workflow_execution import current_process_identity

    request, plan = _synthetic_publication_checkout_plan(tmp_path)
    binding = {
        "schema_version": "workflow_publication_checkout_plan_binding.v1",
        "request_sha256": contract.canonical_digest(request), "plan": plan,
        "worker_process": current_process_identity(), "observed_at": "2026-09-14T00:00:00+00:00",
    }
    execution = {"request": request, "request_sha256": contract.canonical_digest(request),
                 "checkout_plan": binding, "state": "RUNNING"}
    if fault == "schema":
        binding["schema_version"] = "other.v1"
    elif fault == "request":
        binding["request_sha256"] = "0" * 64
    elif fault == "worker":
        binding["worker_process"] = {"pid": 0, "creation_time": 0}
    elif fault == "time":
        binding["observed_at"] = "2026-09-14T00:00:00"
    elif fault == "plan":
        plan["resume_allowed"] = True
    if fault == "none":
        _validate_checkout_plan(execution)
    else:
        with pytest.raises((ParallelControlError, contract.WorkflowContractError)):
            _validate_checkout_plan(execution)
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize("case", [
    "none", "append", "preserve", "drop", "rewrite", "from-reserved", "after-exit",
    "change-state", "change-other", "after-prepare",
])
def test_publication_checkout_plan_immutable_transition_contract(tmp_path, case):
    """Exercise the production transition guard without inventing Full or lease events."""
    from ai_trading_system.platform.architecture.parallel_control import ParallelControlError
    from ai_trading_system.platform.architecture.workflow_coordination import (
        _validate_checkout_plan_transition,
    )

    _request, plan = _synthetic_publication_checkout_plan(tmp_path)
    old = {"state": "RUNNING", "unchanged": "other execution fields"}
    value = {**old, "checkout_plan": plan}
    if case == "none":
        value = dict(old)
    elif case in {"preserve", "drop", "rewrite"}:
        old = copy.deepcopy(value)
        if case == "drop":
            del value["checkout_plan"]
        elif case == "rewrite":
            value["checkout_plan"] = {**plan, "replaced": True}
    elif case in {"from-reserved", "after-exit"}:
        old["state"] = value["state"] = (
            "RESERVED" if case == "from-reserved" else "EXIT_CONFIRMED"
        )
    elif case == "change-state":
        value["state"] = "EXIT_CONFIRMED"
    elif case == "change-other":
        value["unchanged"] = "changed"
    elif case == "after-prepare":
        old["main_preparation"] = value["main_preparation"] = {"existing": True}
    if case in {"none", "append", "preserve"}:
        _validate_checkout_plan_transition(old, value)
    else:
        with pytest.raises(ParallelControlError, match="PUBLICATION_CHECKOUT_PLAN_"):
            _validate_checkout_plan_transition(old, value)
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize("case", [
    "orig-head", "fast-forward", "reversed", "auto-merge", "wrong-main", "wrong-candidate",
    "head-only", "main-only", "unknown-ref", "duplicate", "mixed", "crlf", "no-newline",
    "empty", "too-large", "non-ascii", "orig-old", "orig-new", "auto-nonzero",
])
def test_publication_prepared_reference_update_exact_contract(tmp_path, case):
    request, plan = _synthetic_publication_checkout_plan(tmp_path)
    main, candidate, zero = "a" * 40, "c" * 40, "0" * 40
    head = f"{main} {candidate} HEAD\n".encode()
    branch = f"{main} {candidate} refs/heads/main\n".encode()
    orig = f"{zero} {main} ORIG_HEAD\n".encode()
    auto = f"{zero} {zero} AUTO_MERGE\n".encode()
    cases = {
        "orig-head": orig, "fast-forward": head + branch, "reversed": branch + head,
        "auto-merge": auto, "wrong-main": (head + branch).replace(main.encode(), b"b" * 40),
        "wrong-candidate": (head + branch).replace(candidate.encode(), b"d" * 40),
        "head-only": head, "main-only": branch,
        "unknown-ref": head + branch.replace(b"refs/heads/main", b"refs/heads/other"),
        "duplicate": head + head, "mixed": orig + branch,
        "crlf": (head + branch).replace(b"\n", b"\r\n"), "no-newline": (head + branch)[:-1],
        "empty": b"", "too-large": b"x" * 257, "non-ascii": b"\xff\n",
        "orig-old": orig.replace(zero.encode(), main.encode()),
        "orig-new": orig.replace(main.encode(), candidate.encode()),
        "auto-nonzero": auto.replace(zero.encode(), main.encode(), 1),
    }
    admitted = {"orig-head": "ORIG_HEAD", "fast-forward": "FAST_FORWARD",
                "reversed": "FAST_FORWARD", "auto-merge": "AUTO_MERGE"}
    if case in admitted:
        assert integration.classify_publication_prepared_reference_update(
            plan, request, cases[case],
        ) == admitted[case]
    else:
        with pytest.raises(contract.WorkflowContractError, match="PUBLICATION_REFERENCE_UPDATE_"):
            integration.classify_publication_prepared_reference_update(plan, request, cases[case])
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize("optional_locks", [None, "0", "1"], ids=["unset", "disabled", "enabled"])
@pytest.mark.parametrize("cached", [False, True], ids=["working-tree", "cached"])
def test_governed_diff_audit_preserves_exact_installed_index(
    small_repository: Path,
    monkeypatch: pytest.MonkeyPatch,
    optional_locks: str | None,
    cached: bool,
) -> None:
    from ai_trading_system.platform.architecture.checkout_guard import _run_git_diff_check

    root = small_repository
    candidate = _git(root, "rev-parse", "HEAD")
    index = root / ".git/index"
    installed = integration._source_index_bytes(root, candidate)
    index.write_bytes(installed)
    before_identity = (index.stat().st_dev, index.stat().st_ino)
    if optional_locks is None:
        monkeypatch.delenv("GIT_OPTIONAL_LOCKS", raising=False)
    else:
        monkeypatch.setenv("GIT_OPTIONAL_LOCKS", optional_locks)
    _run_git_diff_check(root, exclusions=(), cached=cached)
    assert index.read_bytes() == installed, "read-only diff audit refreshed the installed index"
    assert (index.stat().st_dev, index.stat().st_ino) == before_identity


@pytest.mark.parametrize("optional_locks", [None, "0", "1"], ids=["unset", "disabled", "enabled"])
@pytest.mark.parametrize("dirty", [False, True], ids=["clean", "dirty"])
def test_readiness_git_probe_preserves_exact_installed_index(
    small_repository: Path, monkeypatch: pytest.MonkeyPatch, optional_locks: str | None,
    dirty: bool,
) -> None:
    from ai_trading_system.platform.architecture import validation_readiness as readiness

    root = small_repository
    candidate = _git(root, "rev-parse", "HEAD")
    index = root / ".git/index"
    installed = integration._source_index_bytes(root, candidate)
    index.write_bytes(installed)
    before_identity = (index.stat().st_dev, index.stat().st_ino)
    if dirty:
        source = root / ".gitignore"
        source.write_bytes(source.read_bytes() + b"\n# actual source drift\n")
    if optional_locks is None:
        monkeypatch.delenv("GIT_OPTIONAL_LOCKS", raising=False)
    else:
        monkeypatch.setenv("GIT_OPTIONAL_LOCKS", optional_locks)
    result = readiness._git(root, "diff", "--quiet", "--no-ext-diff", "--no-textconv",
                            candidate, "--", ".")
    assert result.returncode == (1 if dirty else 0), result.stderr
    assert index.read_bytes() == installed, "readiness probe refreshed the installed index"
    assert (index.stat().st_dev, index.stat().st_ino) == before_identity


@pytest.mark.parametrize("linked", [False, True], ids=["checkout", "linked-worktree"])
@pytest.mark.parametrize("branch", ["refs/heads/Main", "refs/heads/MAIN", "refs/heads/topic"])
def test_installation_lock_namespace_rejects_main_alias_before_writes(
    small_repository: Path,
    linked: bool,
    branch: str,
) -> None:
    root = small_repository
    common = root / ".git"
    gitdir = common / "worktrees/synthetic" if linked else common
    plan = {
        "branch": branch,
        "gitdir_identity": {"path": gitdir.as_posix(), "device": 1, "file_id": 2},
        "common_identity": {"path": common.as_posix(), "device": 1, "file_id": 3},
    }
    before = {
        p.relative_to(common).as_posix(): p.read_bytes() for p in common.rglob("*") if p.is_file()
    }
    if branch.casefold() == "refs/heads/main":
        with pytest.raises(
            contract.WorkflowContractError, match="INSTALLATION_SOURCE_BRANCH_REQUIRED"
        ):
            integration._installation_locks(plan)
    else:
        result = integration._installation_locks(plan)
        assert len(result) == 5
        assert len({(base / name).as_posix().casefold() for base, _identity, name in result}) == 5
    assert {
        p.relative_to(common).as_posix(): p.read_bytes() for p in common.rglob("*") if p.is_file()
    } == before


@pytest.mark.parametrize("linked", [False, True], ids=["checkout", "linked-worktree"])
def test_source_installation_index_exact_git_tree_without_source_reads(
    small_repository: Path,
    monkeypatch: pytest.MonkeyPatch,
    linked: bool,
) -> None:
    root = small_repository
    parent = _git(root, "rev-parse", "HEAD")
    candidate = _tree_commit(
        root,
        parent,
        {
            "binary.bin": ("100644", b"\0\xff\x80\r\n"),
            "empty.bin": ("100644", b""),
            "executable": ("100755", b"executable\n"),
            "dir/file": ("100644", b"nested"),
            "dir-name": ("100644", b"sort before directory entry"),
            "dir.ext": ("100644", b"sort before directory entry too"),
            "日本語.txt": ("100644", "本文".encode()),
            "link": ("120000", b"binary.bin"),
            "submodule": ("160000", parent.encode()),
        },
    )
    selected = root
    if linked:
        selected = root.parent / "linked-index-fixture"
        _git(root, "worktree", "add", "--detach", str(selected), parent)
    index = Path(_git(selected, "rev-parse", "--path-format=absolute", "--git-path", "index"))
    before_index = index.read_bytes()
    before_head = _git(selected, "rev-parse", "HEAD")
    before_refs = _git(root, "for-each-ref", "--format=%(refname) %(objectname)")
    physical = {
        p.relative_to(selected).as_posix(): p.read_bytes()
        for p in selected.rglob("*")
        if p.is_file() and ".git" not in p.relative_to(selected).parts
    }
    tree_rows = _git(root, "-c", "core.quotepath=false", "ls-tree", "-r", candidate).splitlines()
    expected = {}
    for row in tree_rows:
        metadata, name = row.split("\t")
        mode, _kind, oid = metadata.split()
        expected[name] = (mode, oid)
    original_run = subprocess.run
    calls = []

    def observe(args, *positional, **kwargs):
        if isinstance(args, (list, tuple)) and str(args[0]).lower().endswith("git"):
            calls.append(tuple(str(value) for value in args))
            assert "show" not in args
            if "cat-file" in args:
                assert not {"blob", "-p", "--batch", "--batch-command"}.intersection(args)
        return original_run(args, *positional, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(subprocess, "run", observe)
        content = integration._source_index_bytes(selected, candidate)
    assert calls, "the exact Git tree must be observed, not inferred from worktree"
    assert content[:4] == b"DIRC"
    version, count = struct.unpack("!II", content[4:12])
    assert version == 2 and count == len(expected)
    assert hashlib.sha1(content[:-20]).digest() == content[-20:]
    cursor = 12
    names = []
    for _ in range(count):
        start = cursor
        fields = struct.unpack("!10I", content[cursor : cursor + 40])
        assert all(value == 0 for i, value in enumerate(fields) if i != 6)
        oid = content[cursor + 40 : cursor + 60].hex()
        flags = struct.unpack("!H", content[cursor + 60 : cursor + 62])[0]
        end = content.index(b"\0", cursor + 62)
        name = content[cursor + 62 : end].decode("utf-8")
        assert flags == min(len(name.encode("utf-8")), 0xFFF)
        assert (format(fields[6], "06o"), oid) == expected[name]
        names.append(name)
        cursor = start + ((end + 1 - start + 7) // 8) * 8
    assert cursor == len(content) - 20
    assert names == sorted(expected, key=lambda name: name.encode("utf-8"))
    private = root.parent / ("returned-index-" + uuid.uuid4().hex)
    private.write_bytes(content)
    actual = _git(selected, "-c", "core.quotepath=false", "ls-files", "--stage", index=private)
    assert actual.splitlines() == [
        f"{expected[name][0]} {expected[name][1]} 0\t{name}" for name in names
    ]
    assert all(
        row.startswith("H ") for row in _git(selected, "ls-files", "-v", index=private).splitlines()
    )
    assert _git(selected, "write-tree", index=private) == _git(
        root,
        "rev-parse",
        candidate + "^{tree}",
    )
    assert index.read_bytes() == before_index
    assert _git(selected, "rev-parse", "HEAD") == before_head
    assert _git(root, "for-each-ref", "--format=%(refname) %(objectname)") == before_refs
    assert {
        p.relative_to(selected).as_posix(): p.read_bytes()
        for p in selected.rglob("*")
        if p.is_file() and ".git" not in p.relative_to(selected).parts
    } == physical
    if linked:
        _git(root, "worktree", "remove", str(selected))


def test_source_installation_index_keeps_missing_blob_metadata(small_repository: Path) -> None:
    root = small_repository
    parent = _git(root, "rev-parse", "HEAD")
    candidate = _tree_commit(
        root,
        parent,
        {"unavailable.bin": ("100644", b"unique missing blob\0")},
    )
    metadata = _expected(root, candidate, "unavailable.bin")
    oid = metadata["oid"]
    object_path = root / ".git/objects" / oid[:2] / oid[2:]
    original = object_path.read_bytes()
    retained = root.parent / "retained-missing-blob-object"
    before_index = (root / ".git/index").read_bytes()
    before_refs = _git(root, "for-each-ref", "--format=%(refname) %(objectname)")
    object_path.rename(retained)
    try:
        content = integration._source_index_bytes(root, candidate)
        private = root.parent / "missing-blob-private-index"
        private.write_bytes(content)
        assert f"100644 {oid} 0\tunavailable.bin" in _git(
            root,
            "ls-files",
            "--stage",
            index=private,
        )
        assert _git(root, "write-tree", "--missing-ok", index=private) == _git(
            root,
            "rev-parse",
            candidate + "^{tree}",
        )
        assert not object_path.exists() and not (root / "unavailable.bin").exists()
        assert (root / ".git/index").read_bytes() == before_index
        assert _git(root, "for-each-ref", "--format=%(refname) %(objectname)") == before_refs
    finally:
        retained.rename(object_path)
        assert object_path.read_bytes() == original


@pytest.mark.parametrize(
    ("names", "code"),
    [
        (["NUL"], "WORKFLOW_MERGE_SOURCE_INDEX_ENTRY_NAME"),
        (["tail."], "WORKFLOW_MERGE_SOURCE_INDEX_ENTRY_NAME"),
        ([".git"], "WORKFLOW_MERGE_SOURCE_INDEX_ENTRY_NAME"),
        (["bad:name"], "WORKFLOW_MERGE_SOURCE_INDEX_ENTRY_NAME"),
        (["line\nname"], "WORKFLOW_MERGE_SOURCE_INDEX_ENTRY_NAME"),
        (["Case", "case"], "WORKFLOW_MERGE_SOURCE_INDEX_PATH_COLLISION"),
    ],
)
def test_source_installation_index_rejects_real_unsafe_tree_names(
    small_repository: Path,
    names: list[str],
    code: str,
) -> None:
    root = small_repository
    parent = _git(root, "rev-parse", "HEAD")
    oid = _git(root, "hash-object", "-w", "--stdin", content=b"unsafe tree payload")
    raw = b"".join(b"100644 " + name.encode() + b"\0" + bytes.fromhex(oid) for name in names)
    tree = _git(root, "hash-object", "-w", "-t", "tree", "--literally", "--stdin", content=raw)
    candidate = _git(root, "commit-tree", tree, "-p", parent, "-m", "unsafe names fixture")
    before = ((root / ".git/index").read_bytes(), _git(root, "show-ref"))
    with pytest.raises(contract.WorkflowContractError) as rejected:
        integration._source_index_bytes(root, candidate)
    assert rejected.value.code == code
    assert ((root / ".git/index").read_bytes(), _git(root, "show-ref")) == before


@pytest.mark.parametrize("candidate_kind", ["symbolic", "tree"])
def test_source_installation_index_requires_exact_commit(
    small_repository: Path,
    candidate_kind,
) -> None:
    root = small_repository
    candidate = "HEAD" if candidate_kind == "symbolic" else _git(root, "rev-parse", "HEAD^{tree}")
    before = ((root / ".git/index").read_bytes(), _git(root, "show-ref"))
    with pytest.raises(contract.WorkflowContractError) as rejected:
        integration._source_index_bytes(root, candidate)
    assert rejected.value.code == "WORKFLOW_MERGE_SOURCE_INDEX_CANDIDATE"
    assert ((root / ".git/index").read_bytes(), _git(root, "show-ref")) == before


def test_source_installation_index_rejects_real_sha256_repository(tmp_path: Path) -> None:
    root = tmp_path / "sha256-index-fixture"
    root.mkdir()
    _git(root, "init", "--object-format=sha256", "-b", "main")
    _git(root, "config", "user.name", "SHA256 fixture")
    _git(root, "config", "user.email", "fixture@example.invalid")
    (root / "file").write_bytes(b"nonempty\n")
    _git(root, "add", "file")
    _git(root, "commit", "-m", "sha256 fixture")
    before = ((root / ".git/index").read_bytes(), _git(root, "show-ref"))
    with pytest.raises(contract.WorkflowContractError) as rejected:
        integration._source_index_bytes(root, _git(root, "rev-parse", "HEAD"))
    assert rejected.value.code == "WORKFLOW_MERGE_SOURCE_INDEX_OBJECT_FORMAT"
    assert ((root / ".git/index").read_bytes(), _git(root, "show-ref")) == before


def _crossfile_program(left, right, key):
    # Separate real logical regions so git merge-file can independently prove
    # nonoverlapping textual edits; semantic truth is asserted by subprocesses.
    return (
        f"LEFT = {left!r}\n"
        + "\n" * 10
        + f"RIGHT = {right!r}\n"
        + "\n" * 10
        + f"def consume(policy):\n    return [LEFT, RIGHT, policy[{key!r}]]\n"
    ).encode()


def _contract_conflict_sources(kind):
    policies = (b'{"old":1,"new":2}\n', b'{"old":1,"new":2,"lane":9}\n', b'{"old":3}\n')
    if kind == "text":
        programs = tuple(
            f"def consume(policy):\n    return {value!r}\n".encode()
            for value in ("base", "lane", "main")
        )
    elif kind == "clean":

        def program(policy, key):
            return (
                f"POLICY = {policy!r}\n"
                + "\n" * 10
                + f"def consume(policy):\n    return POLICY[{key!r}]\n"
            ).encode()

        programs = (
            program({"old": 1, "new": 2}, "old"),
            program({"old": 1, "new": 2}, "new"),
            program({"old": 3}, "old"),
        )
    else:
        assert kind == "crosspath"
        programs = tuple(
            f"def consume(policy):\n    return policy[{key!r}]\n".encode()
            for key in ("old", "new", "old")
        )
    return programs, policies


def _matrix(
    root: Path,
    parent: str,
    *,
    absorbed_generated: bool = False,
    semantic_rules: bool = False,
    operation_conflicts: bool = False,
    cross_file: bool = False,
    contract_conflict: str | None = None,
) -> tuple[str, str, str, list[str]]:
    initial = {
        name: ("100644", b"base\n")
        for name in (
            "mode.txt",
            "delete.txt",
            "rename-old.txt",
            "absorbed.txt",
            "keep.txt",
            "generated.txt",
        )
    }
    if semantic_rules:
        initial["mode.txt"] = (
            "100644",
            b"def decide(tag):\n    return {'shared':'BASE','legacy':'RETIRED'}.get(tag,'DENY')\n",
        )
    if cross_file:
        initial["mode.txt"] = ("100644", _crossfile_program("BASE_LEFT", "BASE_RIGHT", "old"))
        initial["keep.txt"] = ("100644", b'{"old":"BASE_DATA"}\n')
    conflict_changes = [{}, {}, {}]
    if contract_conflict:
        programs, policies = _contract_conflict_sources(contract_conflict)
        conflict_changes = [
            {"mode.txt": ("100644", program), "keep.txt": ("100644", policy)}
            for program, policy in zip(programs, policies, strict=True)
        ]
        initial.update(conflict_changes[0])
    base = _tree_commit(root, parent, initial)
    if contract_conflict:
        return (
            base,
            _tree_commit(root, base, conflict_changes[1]),
            _tree_commit(root, base, conflict_changes[2]),
            ["keep.txt", "mode.txt"],
        )
    lane = _tree_commit(
        root,
        base,
        {
            "mode.txt": (
                "100755",
                b"def decide(tag):\n"
                b"    return {'shared':'VALID_LANE','legacy':'RETIRED'}.get(tag,'DENY')\n"
                if semantic_rules
                else b"same new blob\n",
            ),
            "delete.txt": None,
            "rename-old.txt": None,
            "rename-new.txt": ("100644", b"base\n"),
            "add.bin": ("100644", b"\0\xff\x80\r\n"),
            "link-object": ("120000", b"absorbed.txt"),
            "gitlink-object": ("160000", base.encode("ascii")),
            "absorbed.txt": ("100644", b"already merged\n"),
            "keep.txt": ("100644", b"lane authority\n"),
            "generated.txt": ("100644", b"stale generated\n"),
            **(
                {
                    "mode.txt": ("100755", _crossfile_program("LANE_LEFT", "BASE_RIGHT", "new")),
                    "keep.txt": ("100644", b'{"new":"LANE_DATA"}\n'),
                }
                if cross_file
                else {}
            ),
        },
    )
    main = _tree_commit(
        root,
        base,
        {
            "mode.txt": (
                "100644",
                b"def decide(tag):\n"
                b"    return {'shared':'BASE','main':'NEW_MAIN'}.get(tag,'DENY')\n"
                if semantic_rules
                else b"same new blob\n",
            ),
            "delete.txt": None,
            "absorbed.txt": ("100644", b"already merged\n"),
            "keep.txt": ("100644", b"current authority\n"),
            **({"generated.txt": ("100644", b"stale generated\n")} if absorbed_generated else {}),
            **(
                {
                    "rename-old.txt": ("100755", b"main retained modification\n"),
                    "delete.txt": ("100644", b"main changed before reviewed deletion\n"),
                }
                if operation_conflicts
                else {}
            ),
            **(
                {
                    "mode.txt": ("100644", _crossfile_program("BASE_LEFT", "MAIN_RIGHT", "old")),
                    "keep.txt": ("100644", b'{"old":"MAIN_OLD","new":"MAIN_DATA"}\n'),
                }
                if cross_file
                else {}
            ),
        },
    )
    paths = sorted(set(initial) | {"rename-new.txt", "add.bin", "link-object", "gitlink-object"})
    return base, lane, main, paths


def test_inventory_preserves_objects_modes_delete_and_rename_endpoints(
    small_repository: Path,
) -> None:
    root = small_repository
    base, lane, main, paths = _matrix(root, _git(root, "rev-parse", "HEAD"))
    scope = _scope(root, base, lane, main, paths)
    observed = integration.inventory_merge_sources(
        root, {"scope": scope, "repository": {"common": scope["repository_common"]}}
    )
    rows = {row["path"]: row for row in observed["source_entries"]}
    assert set(rows) == set(paths)
    for path, row in rows.items():
        for name, commit in (("base", base), ("lane", lane), ("main", main)):
            assert row[name] == _expected(root, commit, path)
    assert rows["mode.txt"]["lane"]["oid"] == rows["mode.txt"]["main"]["oid"]
    assert rows["mode.txt"]["lane"]["mode"] != rows["mode.txt"]["main"]["mode"]
    assert (
        rows["delete.txt"]["lane"]
        == rows["delete.txt"]["main"]
        == _expected(root, lane, "delete.txt")
    )
    assert rows["rename-old.txt"]["operation"] == "D"
    assert rows["rename-new.txt"]["operation"] == "A"
    assert rows["rename-old.txt"]["base"]["oid"] == rows["rename-new.txt"]["lane"]["oid"]
    assert observed["source_history"][0]["commit"] == lane
    assert observed["source_history"][0]["parents"] == [base]
    assert {row["path"] for row in observed["source_history"][0]["operations"]} == set(paths)


@pytest.mark.parametrize(
    "mutation", ["unowned-netzero", "missing-source-commit", "wrong-path", "missing-claims"]
)
def test_inventory_rejects_incomplete_history_scope_and_claims(
    small_repository: Path, mutation: str
) -> None:
    root = small_repository
    base = _git(root, "rev-parse", "HEAD")
    first = _tree_commit(root, base, {"unowned.txt": ("100644", b"intermediate effect")})
    lane = _tree_commit(root, first, {"unowned.txt": None, "owned.txt": ("100644", b"final")})
    scope = _scope(root, base, lane, base, ["owned.txt", "unowned.txt"])
    expected = "UNATTRIBUTED_HISTORY"
    if mutation == "unowned-netzero":
        scope["source_paths"] = ["owned.txt"]
        assert _git(root, "diff", "--name-only", base, lane) == "owned.txt"
    elif mutation == "missing-source-commit":
        scope["source_commits"] = [lane]
        expected = "SOURCE_HISTORY_IDENTITY"
    elif mutation == "wrong-path":
        scope["source_paths"] = ["Owned.txt", "unowned.txt"]
    else:
        scope["contract_claims"] = []
        expected = "CONTRACT_CLAIMS_MISSING"
    with pytest.raises(contract.WorkflowContractError, match=expected):
        integration.inventory_merge_sources(
            root, {"scope": scope, "repository": {"common": scope["repository_common"]}}
        )


def _seed_compatibility_inputs(root: Path) -> None:
    """Five-phase official-builder inputs, installed before immutable Git bases."""
    from test_devx_006c_compatibility_authority import _write_fixture_authority
    from test_devx_006d_report_catalog_flow_authority import _write_fixture

    from ai_trading_system.platform.architecture import compatibility_authority as compatibility

    fixture = _write_fixture_authority(root)
    _write_fixture(root)
    policy = fixture["policy"]
    legacy = {
        "schema_version": "fixture.v1",
        "legacy_section": {
            "generated_fragment_authority": {
                "index_path": "inputs/architecture/arch_005_task_shadow_v2_index.yaml"
            }
        },
    }
    content = canonical._yaml_bytes(legacy)
    fixture["legacy_path"].write_bytes(content)
    policy["exact_start_base"] = _git(root, "rev-parse", "HEAD")
    policy["legacy_prefix"].update(
        {
            "byte_count": len(content),
            "file_sha256": hashlib.sha256(content).hexdigest(),
            "lf_sha256": hashlib.sha256(content).hexdigest(),
            "git_blob": compatibility._git_blob_id(content),
            "mapping_replay_sha256": hashlib.sha256(
                compatibility._canonical_mapping_bytes(legacy, sort_keys=False)
            ).hexdigest(),
        }
    )
    (root / compatibility.DEFAULT_POLICY_PATH).write_bytes(canonical._yaml_bytes(policy))
    paths = """
config/architecture/arch_004_g2_5_readiness.yaml
config/architecture/arch_005_supervised_automation_policy.yaml
docs/artifact_catalog.md
docs/system_flow.md
docs/requirements/ARCH-005_Parallel_Development_Control_Plane.md
docs/requirements/ARCH-005S5_Canonical_Task_Source_Cutover.md
docs/requirements/DEVX-006_Fragmented_Generated_Authority_and_Stable_Task_Shadow_v2.md
docs/requirements/DEVX-006C_Compatibility_Authority_Fragmentation.md
inputs/architecture/arch_004e_architecture_fitness.yaml
inputs/architecture/arch_004e_module_manifest.yaml
inputs/architecture/arch_004e_test_manifest.yaml
inputs/architecture/arch_004g_deprecation_inventory.yaml
inputs/architecture/arch_005_s5_rollback_rehearsal/rollback_rehearsal.yaml
inputs/architecture/arch_005_s5_rollback_rehearsal/task_register.md
inputs/architecture/arch_005_s5_rollback_rehearsal/task_register_completed.md
scripts/architecture_compatibility_authority.py
scripts/architecture_arch005_control_plane.py
scripts/architecture_arch005_registry.py
src/ai_trading_system/external_request_cache_revalidation_coordination.py
src/ai_trading_system/platform/architecture/__init__.py
src/ai_trading_system/platform/architecture/bootstrap_handoff.py
src/ai_trading_system/platform/architecture/compatibility_authority.py
src/ai_trading_system/platform/architecture/parallel_control_dispatch.py
src/ai_trading_system/platform/architecture/parallel_control_kernel.py
src/ai_trading_system/platform/architecture/parallel_control_scheduler.py
src/ai_trading_system/platform/architecture/supervised_automation.py
src/ai_trading_system/platform/architecture/task_registry_canonical.py
src/ai_trading_system/cli_commands/feedback.py
src/ai_trading_system/cli_commands/reports.py
src/ai_trading_system/reports/research_roadmap_dashboard.py
src/ai_trading_system/reports/research_safety_boundary.py
src/ai_trading_system/reports/task_register_consistency.py
tests/test_arch_004_refactor_policy.py
tests/test_arch_004g_deprecation.py
tests/atlas/test_historical_source_adapters.py
tests/test_devx_006c_compatibility_authority.py
tests/test_etf_dynamic_v3_parameter_research.py
tests/test_external_request_cache_revalidation_coordination.py
tests/test_trading2452_architecture_contract.py
tests/test_arch_005_s5_task_source_cutover.py
tests/test_arch_005_s2_kernel.py
tests/test_arch_005_s4_dispatch.py
tests/test_arch_005_s4a_supervised_automation.py
tests/test_arch_005_task_registry_shadow.py
""".split()
    # These three declared dependency lists are fixture material, not the test
    # oracle. Keep source files inert; use real renderers for structural inputs.
    for function, variable in (
        (compatibility._devx_006d_section, "d_source_paths"),
        (compatibility._trading_2542c_section, "source_paths"),
        (compatibility._devx_009_section, "source_paths"),
    ):
        definition = ast.parse(inspect.getsource(function)).body[0]
        declarations = [
            statement.value
            for statement in definition.body
            if isinstance(statement, ast.Assign)
            and any(
                isinstance(name, ast.Name) and name.id == variable for name in statement.targets
            )
            and isinstance(statement.value, ast.List)
        ]
        assert len(declarations) == 1
        paths.extend(
            element.value
            for element in declarations[0].elts
            if isinstance(element, ast.Constant) and isinstance(element.value, str)
        )
    identities = [
        "6d/6d8d0da0de81f10e89f99dc55533b0f53ac3787dcc59a693cb4ed8ca19f67fc1.yaml",
        "98/989bc4bfe58706d37f7b749b47ba03259688afcb2cac1cdf1fafb35b290130af.yaml",
    ]
    for version, directory in (
        ("", "development_tasks_shadow/active"),
        ("_v2", "development_tasks_shadow_v2"),
    ):
        fragments = [f"registry/{directory}/{identity}" for identity in identities]
        paths.extend(fragments)
        index = {
            "status": "PASS",
            "fragment_count": 2,
            "index_checksum": "a" * 64,
            "fragments": [{"path": path} for path in fragments],
        }
        (root / f"inputs/architecture/arch_005_task_shadow{version}_index.yaml").write_bytes(
            canonical._yaml_bytes(index)
        )
    (root / "inputs/architecture/arch_005_task_registry_baseline.yaml").write_bytes(
        canonical._yaml_bytes({"inventory": {"active_task_count": 2, "completed_task_count": 0}})
    )
    for relative in paths:
        target = root / relative
        if not target.exists():
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(b"# Synthetic compatibility source; no runtime behavior.\n")


@pytest.fixture
def canonical_merge_repository(small_repository: Path, monkeypatch: pytest.MonkeyPatch, request):
    """Build source objects first, then real canonical registry/CLI publication authority."""
    from test_governed_development_skill import _admission_registry

    root = small_repository
    fixture_mode = getattr(request, "param", None)
    linked_native = fixture_mode == "native-linked-full-profile-publish"
    independent_native = fixture_mode == "native-independent-full-profile-publish"
    native_host = (
        fixture_mode == "native-full-profile-publish" or linked_native or independent_native
    )
    if native_host:
        fixture_mode = "full-profile-publish"
    bound_parent = fixture_mode == "full-readiness-profile-parent"
    if bound_parent:
        fixture_mode = "full-readiness-profile"
    whole_profile = fixture_mode in {
        "full-profile", "full-profile-publish", "full-readiness-profile",
    }
    atlas_records = []
    creation_fault = (
        fixture_mode.removeprefix("source-job-created-")
        if isinstance(fixture_mode, str) and fixture_mode.startswith("source-job-created-")
        else None
    )
    source_job = fixture_mode in {
        "source-job", "full-readiness", "full-readiness-profile",
    } or whole_profile or creation_fault is not None
    source_candidate = (
        fixture_mode
        in {"source-candidate", "source-semantics", "source-operations", "source-cross-file"}
        or source_job
    )
    for name in tuple(os.environ):
        if name.startswith("GIT_"):
            monkeypatch.delenv(name)
    monkeypatch.setenv("GIT_CONFIG_NOSYSTEM", "1")
    monkeypatch.setenv("GIT_CONFIG_GLOBAL", os.devnull)
    monkeypatch.setenv("PYTHONPATH", str(ROOT / "src"))
    for relative in (
        "AGENTS.md",
        "scripts/architecture_arch005_checkout_guard.py",
        "scripts/architecture_arch005_task_source.py",
        "docs/requirements/DEVX-002_Governed_Development_Workflow_Skill.md",
        "config/architecture/arch_005_s5_task_source_cutover.yaml",
        "config/architecture/arch_005_parallel_control_policy.yaml",
        "config/architecture/arch_005_integration_publication_fence.yaml",
    ):
        target = root / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(ROOT / relative, target)
    tasks = {TASK: "IN_PROGRESS", TASK + "-OTHER": "IN_PROGRESS"}
    if (
        getattr(request, "param", None) in {"candidate", "all-absorbed"}
        or (isinstance(fixture_mode, str) and fixture_mode.startswith("conflict-"))
        or source_candidate
    ):
        shutil.copyfile(
            ROOT / "scripts/architecture_arch005_workflow.py",
            root / "scripts/architecture_arch005_workflow.py",
        )
    if getattr(request, "param", None) == "compatibility" or source_candidate:
        tasks.update(
            {
                (
                    "DEVX-009_PARALLEL_INTEGRATION_PUBLICATION_FENCE_AND_GENERATED_STATE_REBUILD_V1"
                ): "IN_PROGRESS",
                (
                    "TRADING-2542C_GROWTH_ACTION_VALUE_INDEPENDENT_REVIEW_"
                    "REMEDIATION_AND_FREEZE_READINESS_V1"
                ): "IN_PROGRESS",
            }
        )
    _admission_registry(root, tasks, "a" * 40)
    if getattr(request, "param", None) == "report":
        from test_devx_006d_report_catalog_flow_authority import _write_fixture

        _write_fixture(root)
    if getattr(request, "param", None) == "compatibility" or source_candidate:
        _seed_compatibility_inputs(root)
    if source_job:
        from test_devx015_workflow_execution import _install_source_job_runtime

        from ai_trading_system.platform.architecture import report_catalog_flow_authority as report

        _install_source_job_runtime(root)
        source_boundary = request.node.callspec.params.get("source_boundary")
        if source_boundary is not None:
            from test_devx015_workflow_execution import _install_source_boundary_probe

            _install_source_boundary_probe(root, source_boundary)
        if fixture_mode == "full-readiness" or whole_profile:
            from test_devx015_workflow_coordination import (
                _seed_readiness_atlas_inputs,
                _seed_readiness_retained_evidence,
            )

            for name in ("run_validation_tier.py", "validation_readiness.py"):
                shutil.copyfile(ROOT / "scripts" / name, root / "scripts" / name)
            _seed_readiness_retained_evidence(root)
            atlas_records = _seed_readiness_atlas_inputs(root)
        if creation_fault is not None:
            from test_devx015_workflow_acceptance import install_creation_crash_fixture

            install_creation_crash_fixture(root, creation_fault)
        monkeypatch.setenv("PYTHONPATH", str(root / "src"))
        monkeypatch.setenv("PYTHONDONTWRITEBYTECODE", "1")
        report.build_repository_authority(root, write=True)
    if fixture_mode in {
        "full-recovery", "full-mandatory", "full-profile", "full-profile-publish",
        "full-readiness-profile",
    }:
        from test_devx015_workflow_execution import _install_source_job_runtime

        _install_source_job_runtime(root)
        for name in ("run_validation_tier.py", "pytest_runtime_profile.py"):
            shutil.copyfile(ROOT / "scripts" / name, root / "scripts" / name)
        monkeypatch.setenv("PYTHONPATH", str(root / "src"))
        monkeypatch.setenv("PYTHONDONTWRITEBYTECODE", "1")
        if fixture_mode in {
            "full-mandatory", "full-profile", "full-profile-publish", "full-readiness-profile",
        }:
            manifest_path = "config/architecture/devx_015_workflow_acceptance.v1.json"
            manifest = json.loads((ROOT / manifest_path).read_text(encoding="utf-8"))
            manifest["mapping_state"] = "COMPLETE_REVIEWED"
            manifest["variant_node_mapping"] = [
                {"case_id": case["id"], "variant": variant,
                 "node_ids": ["tests/test_full_job.py::test_required"]}
                for case in manifest["cases"] for variant in case["variants"]
            ]
            _json(root / manifest_path, manifest)
            test_path = root / "tests/test_full_job.py"
            test_path.parent.mkdir(exist_ok=True)
            test_path.write_text(
                "import ctypes,json,os\nfrom ctypes import wintypes as w\n"
                "from pathlib import Path\n"
                "def test_required():\n"
                "    api=ctypes.WinDLL('kernel32',use_last_error=True)\n"
                "    api.OpenJobObjectW.argtypes=[w.DWORD,w.BOOL,w.LPCWSTR]\n"
                "    api.OpenJobObjectW.restype=w.HANDLE\n"
                "    api.GetCurrentProcess.restype=w.HANDLE\n"
                "    api.IsProcessInJob.argtypes=[w.HANDLE,w.HANDLE,ctypes.POINTER(w.BOOL)]\n"
                "    api.CloseHandle.argtypes=[w.HANDLE]\n"
                "    job=api.OpenJobObjectW(4,False,os.environ['DEVX015_EXPECTED_JOB'])\n"
                "    assert job\n"
                "    try:\n"
                "        member=w.BOOL()\n"
                "        assert api.IsProcessInJob(\n"
                "            api.GetCurrentProcess(),job,ctypes.byref(member))\n"
                "        assert member.value\n"
                "    finally:\n"
                "        assert api.CloseHandle(job)\n"
                "    worker=os.environ['PYTEST_XDIST_WORKER']\n"
                "    assert worker in {'gw0','gw1'}\n"
                "    Path('outputs/validation_runtime/mandatory/job-witness.json').write_text(\n"
                "        json.dumps({'pid':os.getpid(),'worker':worker,'in_job':True}))\n"
                "    assert os.environ['DEVX015_FAIL'] == '0', 'actual mandatory target failure'\n",
                encoding="utf-8",
            )
            if fixture_mode in {"full-profile", "full-profile-publish", "full-readiness-profile"}:
                # The reviewed complete duration profile requires 16 workers.
                # loadfile assigns whole files, so provide 16 actual test files.
                template = test_path.read_text(encoding="utf-8").replace(
                    "{'gw0','gw1'}", "{f'gw{index}' for index in range(16)}",
                )
                test_path.write_text(template, encoding="utf-8", newline="\n")
                extra_nodes = []
                for index in range(1, 16):
                    extra_path = root / f"tests/test_full_job_{index:02d}.py"
                    extra_path.write_text(
                        template.replace("job-witness.json", f"job-witness-{index:02d}.json"),
                        encoding="utf-8", newline="\n",
                    )
                    extra_nodes.append(extra_path.relative_to(root).as_posix() + "::test_required")
                for row in manifest["variant_node_mapping"]:
                    row["node_ids"].extend(extra_nodes)
                _json(root / manifest_path, manifest)
                if whole_profile:
                    # These files are engineering inputs in a disposable fixture,
                    # not the original project's research tests or 106 oracles.
                    # Run every generated-manifest test file as a real Job probe;
                    # no collector exclusions, synthetic PASS records or skipped files.
                    probes = []
                    for probe in sorted((root / "tests").rglob("test_*.py")):
                        relative = probe.relative_to(root).as_posix()
                        if fixture_mode != "full-readiness-profile" and probe.name.startswith(
                            "test_full_job"
                        ):
                            continue  # Preserve the existing named witness assertions.
                        witness = "probe-" + hashlib.sha256(relative.encode()).hexdigest() + ".json"
                        original = probe.read_bytes()
                        probe.write_text(template.replace("job-witness.json", witness),
                                         encoding="utf-8", newline="\n")
                        probes.append({
                            "path": relative,
                            "input_sha256": hashlib.sha256(original).hexdigest(),
                            "executed_sha256": hashlib.sha256(probe.read_bytes()).hexdigest(),
                        })
                    _json(root.parent / "executed-engineering-probes.json", {
                        "scope": "ISOLATED_FULL_MECHANISM_PROBES_NOT_RESEARCH_OR_V3_CASE_ORACLES",
                        "tests": probes,
                    })
                # Keep fatal-error diagnostics, but do not add a repeating native
                # stack walker to every worker. v79's dump faults inside that
                # fixture-only watchdog; it is not an acceptance requirement.
                (root / "conftest.py").write_text(
                    "import faulthandler\n"
                    "faulthandler.enable()\n",
                    encoding="utf-8", newline="\n",
                )
            if fixture_mode == "full-profile-publish":
                (root / "src/a.py").write_text("VALUE = 1\n", encoding="utf-8", newline="\n")
                if request.node.callspec.params.get("case") == "expired":
                    # Only this disposable fixture uses a finite real expiry.
                    # Freeze before source history/acquire/C; production policy
                    # and all production clocks remain unchanged.
                    lease_policy = root / "config/architecture/arch_005_s4d_checkout_guard.yaml"
                    expiry_policy = safe_load_yaml_path(lease_policy)
                    expiry_policy["lease"]["ttl_seconds"] = 180
                    expiry_policy["lease"]["heartbeat_interval_seconds"] = 30
                    _json(lease_policy, expiry_policy)
                remote_state = request.node.callspec.params.get("remote_state", "normal")
                if remote_state.startswith("probe-"):
                    module = root / (
                        "src/ai_trading_system/platform/architecture/integration_publication_fence.py"
                    )
                    source = module.read_text(encoding="utf-8")
                    marker = "    endpoint = _push_endpoint(root)\n"
                    assert source.count(marker) == 1
                    hook = (
                        "    import sys\n"
                        "    if ('checkpoint' in sys.argv and '--phase' in sys.argv "
                        "and sys.argv[sys.argv.index('--phase') + 1] == 'CLEANUP_PRE'):\n"
                        "        from ai_trading_system.platform.architecture.workflow_execution "
                        "import current_process_identity\n"
                        "        print(json.dumps({'probe_barrier': current_process_identity()}),"
                        " flush=True)\n"
                        "        assert sys.stdin.readline().strip() == 'continue'\n"
                    )
                    # Original real remote probe stays intact; freeze only a
                    # finite observation barrier before the canonical candidate.
                    module.write_text(source.replace(marker, hook + marker),
                                      encoding="utf-8", newline="\n")
    if whole_profile:
        # Probe source bytes and conftest are final before the consumer inventory.
        from ai_trading_system.platform.architecture import report_catalog_flow_authority as report

        report.build_repository_authority(root, write=True)
    policy = canonical.load_cutover_policy(root)
    records = canonical._load_generated_mapping(root / canonical.CANONICAL_INDEX_PATH)["fragments"]
    if atlas_records:
        # Frozen test samples join the synthetic engineering tasks before the
        # fixture index exists. Full canonical validation below checks the newly
        # built chain and every unchanged sample event; no original task is written.
        atlas_tasks = {row["task_id"] for row in atlas_records}
        records = [row for row in records if row["task_id"] not in atlas_tasks] + atlas_records
        records = [{**row, "order": order} for order, row in enumerate(records, 1)]
        fragments = tuple(canonical._load_generated_mapping(root / row["path"]) for row in records)
    else:
        fragments = canonical._load_canonical_fragments(root, records)
    templates = []
    for partition in ("active", "completed"):
        relative = policy["generated_views"][partition + "_template_path"]
        target = root / relative
        target.write_bytes(b"Synthetic preserved template\n")
        templates.append(
            {
                "partition": partition,
                "path": relative,
                "sha256": hashlib.sha256(target.read_bytes()).hexdigest(),
                "byte_count": target.stat().st_size,
                "removed_task_row_count": 0,
            }
        )
    manifest = {
        "schema_version": canonical.CUTOVER_MANIFEST_SCHEMA,
        "status": "PASS",
        "task_id": policy["task_id"],
        "source_of_truth_after": canonical.CANONICAL_SOURCE,
    }
    manifest["manifest_checksum"] = canonical._payload_checksum(manifest, "manifest_checksum")
    manifest_path = root / policy["canonical"]["manifest_path"]
    manifest_path.write_bytes(canonical._yaml_bytes(manifest))
    inventory_path = root / policy["canonical"]["consumer_inventory_path"]
    inventory_path.write_bytes(canonical._yaml_bytes(canonical.build_consumer_inventory(root)))
    index = canonical._build_index(
        root=root,
        policy=policy,
        fragment_records=records,
        fragments=fragments,
        templates=templates,
        governance_cycles=(
            [{"cycle_id": "fixture-import"}, {"cycle_id": "fixture-self-host"}]
            if getattr(request, "param", None) == "compatibility" or source_candidate
            else []
        ),
        manifest_sha256=hashlib.sha256(manifest_path.read_bytes()).hexdigest(),
        consumer_inventory_sha256=hashlib.sha256(inventory_path.read_bytes()).hexdigest(),
    )
    canonical._write_index_and_views(root, policy, index, fragments)
    architecture_outputs = set()
    if source_candidate:
        from test_devx015_workflow_execution import _seed_source_generator_architecture

        _additions, architecture_outputs = _seed_source_generator_architecture(
            root, capture_runtime_baseline=source_job
        )
    if getattr(request, "param", None) == "compatibility" or source_candidate:
        from ai_trading_system.platform.architecture import compatibility_authority as compatibility

        seeded = compatibility.build_repository_authority(root, write=True)
        if fixture_mode == "full-readiness" or whole_profile:
            assert seeded["fragment_count"] > 5  # Frozen Atlas requirements enable later sections.
        else:
            assert seeded["fragment_count"] == 5
        compatibility.validate_repository_authority(root)
    _json(root / SCOPE_PATH, {"placeholder": "before Git source history"})
    _git(root, "add", ".")
    _git(root, "commit", "-m", "strict canonical registry fixture")
    base, lane, main, paths = _matrix(
        root,
        _git(root, "rev-parse", "HEAD"),
        absorbed_generated=getattr(request, "param", None) == "absorbed-generated",
        semantic_rules=fixture_mode == "source-semantics",
        operation_conflicts=fixture_mode == "source-operations",
        cross_file=fixture_mode == "source-cross-file",
        contract_conflict=fixture_mode.removeprefix("conflict-")
        if isinstance(fixture_mode, str) and fixture_mode.startswith("conflict-")
        else None,
    )
    if fixture_mode == "all-absorbed":
        # Distinct B->L and B->M histories with identical source trees. This is
        # frozen before the real canonical event and publication lease exist.
        main = _git(
            root,
            "commit-tree",
            _git(root, "rev-parse", lane + "^{tree}"),
            "-p",
            base,
            "-m",
            "independent main absorbed all source changes",
        )
        assert main != lane
    # Source B/L/M are immutable before the fence. Integration checkout starts at M.
    _git(root, "switch", "-c", "workflow-merge", main)
    _git(root, "update-ref", "refs/heads/main", main)
    scope = _scope(root, base, lane, main, paths)
    if getattr(request, "param", None) == "candidate" or source_candidate:
        scope["generator_order"] = ["canonical-task-source"]
        scope["generated_paths"] = {
            name: "canonical-task-source"
            for name in (
                canonical.CANONICAL_INDEX_PATH,
                policy["canonical"]["consumer_inventory_path"],
                policy["generated_views"]["active_path"],
                policy["generated_views"]["completed_path"],
                *(row["path"] for row in records),
            )
        }
    if source_candidate:
        from ai_trading_system.platform.architecture import report_catalog_flow_authority as report

        scope["generator_order"] = [
            "canonical-task-source",
            "architecture-manifests",
            "report-flow-authority",
            "compatibility-authority",
        ]
        scope["generated_paths"].update(
            {name: "architecture-manifests" for name in architecture_outputs}
        )
        report_policy = report.load_policy(root)
        report_inventory = report.validate_repository_authority(root)
        scope["generated_paths"].update(
            {
                name: "report-flow-authority"
                for name in (
                    report.DEFAULT_POLICY_PATH.as_posix(),
                    report_policy["index_path"],
                    report_policy["consumer_inventory_path"],
                    *report_inventory["fragment_paths"],
                )
            }
        )
        scope["generated_paths"].update(
            {
                name: "compatibility-authority"
                for name in (
                    seeded["index_path"],
                    seeded["consumer_inventory_path"],
                    *(row["fragment_path"] for row in seeded["index"]["entries"]),
                )
            }
        )
    _json(root / SCOPE_PATH, scope)
    authority = {
        "schema_version": "workflow_task_authority.v1",
        "task_id": TASK,
        "decision_id": scope["decision_id"],
        "status": "ACTIVE",
        "review_ref": None,
        "scope_ref": {
            "path": SCOPE_PATH,
            "sha256": hashlib.sha256((root / SCOPE_PATH).read_bytes()).hexdigest(),
        },
    }
    canonical.validate_canonical_registry(project_root=root)
    if native_host:
        from test_devx015_workflow_coordination import native_full_host_registration

        native_context = native_full_host_registration(
            root, monkeypatch, linked_contender=linked_native,
            independent_contender=independent_native,
        )
        native_context.__enter__()
        request.addfinalizer(lambda: native_context.__exit__(None, None, None))
    if whole_profile:
        # The profile inspector deliberately runs the coordinator's own
        # implementation (-I), never the candidate script. A real coordinator runs
        # from the candidate checkout, so bind the fixture's byte-identical copy as
        # that implementation for in-process and CLI callers alike.
        import ai_trading_system.platform.architecture.integration_publication_fence as fence_module

        fixture_fence = root / "src/ai_trading_system/platform/architecture/" / Path(
            fence_module.__file__
        ).name
        # Not byte-compared with the loaded module: probe variants deliberately
        # instrument the candidate's copy, which is the implementation under test.
        assert fixture_fence.is_file()
        monkeypatch.setattr(fence_module, "__file__", str(fixture_fence))
        monkeypatch.setenv(
            "PYTHONPATH", str(root / "src") + os.pathsep + os.environ.get("PYTHONPATH", "")
        )
        # The real Full launcher freezes its child environment and imports the
        # candidate src, writing .pyc there. The project ignores __pycache__/;
        # mirror that locally (not a tracked candidate change) so readiness and
        # release see the same ignore semantics as the real repository.
        exclude = root / ".git/info/exclude"
        exclude.parent.mkdir(parents=True, exist_ok=True)
        with exclude.open("a", encoding="utf-8") as handle:
            handle.write("__pycache__/\n")
    fence = IntegrationPublicationFence(project_root=root)
    parent_summary = None
    if bound_parent:
        # Stand-in for the failed Full this transaction re-runs. Qualifying a real
        # parent summary is the upstream CLI boundary; this fixture only freezes
        # its exact bytes so publication-stage consumption can be rehearsed.
        parent_summary = root / "outputs/validation_runtime/parent-run/test_runtime_summary.json"
        parent_summary.parent.mkdir(parents=True)
        parent_summary.write_text(
            '{"status":"FAIL","fixture":"publication-stage parent binding"}\n',
            encoding="utf-8",
        )
    binding = fence.acquire(
        transaction_id="merge-authority",
        task_id=TASK,
        change_id="merge-fixture",
        thread_id="fixture",
        actor="integration-coordinator",
        frozen_base_sha=base,
        lane_head_sha=main,
        expected_main_sha=main,
        owned_paths=tuple(
            paths
            + [
                SCOPE_PATH,
                "extra-source.txt",
                "unreviewed-source.txt",
                "scripts/unreviewed_canary.py",
                "scripts/architecture_arch005_task_source.py",
                "config/architecture/devex_ownership_policy.yaml",
                "config/architecture/arch_004c_dependency_policy.yaml",
                "config/architecture/arch_004g_deprecation_policy.yaml",
                "src/ai_trading_system/contracts/synthetic.py",
                "config/architecture/devx_006d_report_catalog_flow_authority.yaml",
                *(["src/a.py"] if fixture_mode == "full-profile-publish" else []),
                *(["src/sitecustomize.py"] if native_host else []),
                *(
                    ["tests/test_binding.py",
                     "config/architecture/devx_015_workflow_acceptance.v1.json", ".gitignore"]
                    if fixture_mode == "full-recovery" else []
                ),
            ]
        ),
        shared_paths=(
            "registry/development_tasks",
            "inputs/architecture",
            "docs/task_register.md",
            "docs/task_register_completed.md",
            "outputs/architecture/workflow_integration/reviews",
            "config/report_registry.yaml",
            "docs/artifact_catalog.md",
            "docs/system_flow.md",
            "registry/report_catalog_flow_authority",
            "registry/architecture_compatibility_authority",
            *(
                ("outputs/architecture/workflow_integration/source_candidates",)
                if source_job
                else ()
            ),
        ),
        generator_ids=(
            tuple(scope["generator_order"]) if source_candidate else ("canonical-task-source",)
        ),
        full_parent_path=parent_summary,
    )
    transaction = root / binding["transaction_path"]
    try:
        fence.checkpoint(
            transaction, phase="TASK_SOURCE_PRE_WRITE", actor="integration-coordinator"
        )
        authority_path = root / "outputs/merge-authority.json"
        _json(authority_path, authority)
        command = [
            sys.executable,
            str(root / "scripts/architecture_arch005_task_source.py"),
            "update",
            "--task-id",
            TASK,
            "--actor",
            "integration-coordinator",
            "--change-id",
            "bind-merge-authority",
            "--occurred-at",
            "2026-09-11T10:00:00+00:00",
            "--base-commit",
            main,
            "--publication-transaction",
            str(transaction),
            "--workflow-authority",
            str(authority_path),
        ]
        result = subprocess.run(
            command, cwd=root, capture_output=True, text=True, encoding="utf-8", timeout=30
        )
        assert result.returncode == 0, result.stdout + result.stderr
        assert contract.load_current_task_authority(root, TASK)["scope"] == scope
        yield root, scope
    finally:
        # Revalidate an already completed publication as completed; a fixture
        # must not request a contradictory failed replay or rewrite its receipt.
        outcome = "completed" if fence.replay(transaction).phase == "RELEASED" else "failed"
        try:
            fence.release(transaction, actor="integration-coordinator", outcome=outcome)
        except PublicationFenceError as exc:
            if (
                getattr(request.node, "callspec", None) is None
                or request.node.callspec.params.get("case") != "tampered"
                or exc.code != "PUBLICATION_REPLAY_INVALID"
                or fence.replay(transaction).status == "PASS"
            ):
                raise
            # Retain the deliberately damaged original transaction. Release
            # only its known, actually terminal execution via the same guard;
            # do not manufacture a successful publication receipt.
            fence.guard.release(
                str(binding["lease_id"]), actor="integration-coordinator", outcome="failed",
            )
            assert not fence.guard.replay().active_leases


@pytest.mark.parametrize("canonical_merge_repository", ["source-job"], indirect=True)
def test_source_final_handoff_rejects_actual_later_phase_failure(canonical_merge_repository):
    # Keep the second independent full-source fixture in this module so normal
    # --dist loadfile can execute both real process chains concurrently.
    from test_devx015_workflow_execution import (
        test_source_candidate_cli_uses_real_job_and_private_two_parent_commit,
    )

    test_source_candidate_cli_uses_real_job_and_private_two_parent_commit(
        canonical_merge_repository,
        handoff_boundary="later-phase",
    )


@pytest.mark.parametrize(
    "canonical_merge_repository",
    ["source-job-created-file-before-record", "source-job-created-file-after-record"],
    indirect=True,
)
def test_public_recovery_restores_file_creation_record_crashes(canonical_merge_repository, request):
    from test_devx015_workflow_execution import (
        test_source_candidate_cli_uses_real_job_and_private_two_parent_commit,
    )

    mode = request.node.callspec.params["canonical_merge_repository"]
    test_source_candidate_cli_uses_real_job_and_private_two_parent_commit(
        canonical_merge_repository,
        installation_boundary=mode.removeprefix("source-job-"),
    )


def _all_absorbed_observation(root: Path) -> dict:
    return {
        "refs": _git(root, "show-ref"),
        "head": _git(root, "rev-parse", "HEAD"),
        "index": (root / ".git/index").read_bytes(),
        "config": (root / ".git/config").read_bytes(),
        "files": {
            path.relative_to(root).as_posix(): path.read_bytes()
            for path in root.rglob("*")
            if ".git" not in path.relative_to(root).parts and path.is_file()
        },
    }


@pytest.mark.parametrize("canonical_merge_repository", ["all-absorbed"], indirect=True)
def test_public_all_absorbed_has_zero_residual_and_no_source_effects(
    canonical_merge_repository, bootstrap: str | None = None,
):
    root, scope = canonical_merge_repository
    before = _all_absorbed_observation(root)
    script = root / "scripts/architecture_arch005_workflow.py"
    command = [sys.executable, str(script), "merge-plan", "--task-id", TASK]
    if bootstrap is not None:
        command = [sys.executable, "-c", bootstrap, *command[1:]]
    result = subprocess.run(command, cwd=root, capture_output=True, text=True, timeout=60)
    _json(root.parent / "all-absorbed-cli-result.json", {
        "returncode": result.returncode, "stdout": result.stdout, "stderr": result.stderr,
    })
    assert result.returncode == 0, "ABSORBED_SOURCE_MUST_NOT_BE_REJECTED\n" + result.stderr
    plan = json.loads(result.stdout)
    assert plan["status"] == "NO_RESIDUAL_SOURCE"
    rows = {row["path"]: row for row in plan["dispositions"]}
    assert set(rows) == set(scope["source_paths"])
    assert rows and all(row["disposition"] == "ALREADY_ABSORBED" for row in rows.values())
    for path, row in rows.items():
        assert row["lane"] == _expected(root, scope["lane_head"], path)
        assert row["main"] == _expected(root, scope["latest_main"], path)
        assert row["lane"] == row["main"]
    assert rows["mode.txt"]["main"]["mode"] == "100755"
    assert rows["link-object"]["main"]["mode"] == "120000"
    assert rows["gitlink-object"]["main"]["type"] == "commit"
    assert rows["delete.txt"]["main"]["exists"] is False
    assert rows["rename-old.txt"]["main"]["exists"] is False
    assert plan["contract_claims"] == scope["contract_claims"]
    # External fixture-owned input: the read-only CLI never persists a plan or
    # creates a review, candidate, duplicate application or contract wave.
    external_plan = root.parent / "absorbed-plan.json"
    _json(external_plan, plan)
    checked = subprocess.run(
        [
            sys.executable,
            str(script),
            "merge-validate",
            "--task-id",
            TASK,
            "--plan",
            str(external_plan),
        ],
        cwd=root,
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert checked.returncode == 0, checked.stdout + checked.stderr
    assert json.loads(checked.stdout)["status"] == "NO_RESIDUAL_SOURCE"
    assert _all_absorbed_observation(root) == before


@pytest.mark.parametrize("canonical_merge_repository", ["all-absorbed"], indirect=True)
@pytest.mark.parametrize("mutation", [False, True], ids=["original", "M01"])
def test_m01_actual_absorbed_source_mutant_hits_original_cli_assertion(
    canonical_merge_repository, mutation: bool,
) -> None:
    root, scope = canonical_merge_repository
    original = inspect.getsource(integration._plan_from_authority)
    target = '            disposition = "ALREADY_ABSORBED"'
    assert original.count(target) == 1, "INVALID_MUTATION_TARGET"
    changed = original.replace(
        target, '            _fail("MUTANT_OVERLAP_ALWAYS_REJECT", path)', 1
    ) if mutation else original
    (root.parent / "m01-method-before.py").write_text(original, encoding="utf-8")
    method_path = root.parent / "m01-method-after.py"
    method_path.write_text(changed, encoding="utf-8")
    script = root / "scripts/architecture_arch005_workflow.py"
    original_cli = script.read_text(encoding="utf-8")
    (root.parent / "m01-original-cli.py").write_text(original_cli, encoding="utf-8")
    witness = root.parent / "m01-loaded-method.json"
    hook = (
        "from ai_trading_system.platform.architecture import workflow_integration as _m01\n"
        "from pathlib import Path as _M01Path\nimport hashlib as _m01hash, json as _m01json\n"
        f"_m01body=_M01Path({str(method_path)!r}).read_text(encoding='utf-8')\n"
        f"if {mutation!r}:\n"
        "    _m01namespace={}\n"
        f"    exec(compile(_m01body,{str(method_path)!r},'exec'),_m01.__dict__,_m01namespace)\n"
        "    _m01._plan_from_authority=_m01namespace['_plan_from_authority']\n"
        f"_M01Path({str(witness)!r}).write_text(_m01json.dumps({{"
        f"'mutation':{mutation!r},'module_path':_m01.__file__,"
        "'module_sha256':_m01hash.sha256(_M01Path(_m01.__file__).read_bytes()).hexdigest(),"
        "'method_sha256':_m01hash.sha256(_m01body.encode()).hexdigest()}),encoding='utf-8')\n"
    )
    hook += (
        "import runpy, sys\n"
        "sys.argv=sys.argv[1:]\n"
        "runpy.run_path(sys.argv[0],run_name='__main__')\n"
    )
    (root.parent / "m01-driver.py").write_text(hook, encoding="utf-8")
    # Independently prove the real same-object precondition before the public CLI.
    assert scope["source_paths"]
    for path in scope["source_paths"]:
        assert _expected(root, scope["lane_head"], path) == _expected(
            root, scope["latest_main"], path
        )
    before = _all_absorbed_observation(root)
    if mutation:
        with pytest.raises(
            AssertionError, match=r"^ABSORBED_SOURCE_MUST_NOT_BE_REJECTED(?:\n|$)"
        ):
            test_public_all_absorbed_has_zero_residual_and_no_source_effects(
                canonical_merge_repository, bootstrap=hook
            )
        result = json.loads((root.parent / "all-absorbed-cli-result.json").read_text())
        assert result["returncode"] != 0
        assert "WORKFLOW_MERGE_MUTANT_OVERLAP_ALWAYS_REJECT" in result["stderr"]
    else:
        test_public_all_absorbed_has_zero_residual_and_no_source_effects(
            canonical_merge_repository, bootstrap=hook
        )
    assert _all_absorbed_observation(root) == before
    assert script.read_text(encoding="utf-8") == original_cli
    loaded = json.loads(witness.read_text())
    assert loaded["mutation"] == mutation
    assert loaded["method_sha256"] == hashlib.sha256(changed.encode()).hexdigest()
    _json(root.parent / "m01-counterfactual.json", {
        "schema_version": "devx015_m01_counterfactual.v1", "mutant_id": "M01",
        "mutation": mutation, "target_assertion_killed": mutation,
        "target_assertion": "ABSORBED_SOURCE_MUST_NOT_BE_REJECTED",
        "scope": "ORIGINAL_ALL_ABSORBED_PUBLIC_CLI", "formal_acceptance": False,
        "before_method_sha256": hashlib.sha256(original.encode()).hexdigest(),
        "after_method_sha256": hashlib.sha256(changed.encode()).hexdigest(),
    })


@pytest.mark.parametrize("canonical_merge_repository", ["candidate"], indirect=True)
@pytest.mark.parametrize("mutation", ["unreviewed", "claims", "acceptance"])
def test_public_merge_validation_rejects_unreviewed_or_rehashed_claims_without_effects(
    canonical_merge_repository, mutation
):
    root, scope = canonical_merge_repository
    script = root / "scripts/architecture_arch005_workflow.py"
    fence = IntegrationPublicationFence(project_root=root)

    def observe():
        return (
            _git(root, "show-ref"),
            (root / ".git/index").read_bytes(),
            (root / ".git/config").read_bytes(),
            fence.guard.store.replay().head_event_ids,
            {
                path.relative_to(root).as_posix(): path.read_bytes()
                for path in root.rglob("*")
                if ".git" not in path.relative_to(root).parts and path.is_file()
            },
        )

    before = observe()
    planned = subprocess.run(
        [sys.executable, str(script), "merge-plan", "--task-id", TASK],
        cwd=root,
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert planned.returncode == 0, planned.stdout + planned.stderr
    plan = json.loads(planned.stdout)
    assert plan["status"] == "COORDINATOR_REVIEW_REQUIRED"
    assert plan["contract_claims"] == scope["contract_claims"]
    if mutation == "claims":
        plan["contract_claims"] = []
    elif mutation == "acceptance":
        plan["contract_claims"][0]["required_acceptance"] = []
    if mutation != "unreviewed":
        plan.pop("plan_sha256")
        # Independent canonical JSON/SHA oracle; do not ask the SUT to seal
        # the forged plan or replace current canonical authority.
        plan["plan_sha256"] = hashlib.sha256(
            json.dumps(
                plan, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False
            ).encode()
        ).hexdigest()
    supplied = root.parent / "externally-supplied-plan.json"
    _json(supplied, plan)
    rejected = subprocess.run(
        [sys.executable, str(script), "merge-validate", "--task-id", TASK, "--plan", str(supplied)],
        cwd=root,
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert rejected.returncode != 0
    expected = "REVIEW_NOT_FROZEN" if mutation == "unreviewed" else "PLAN_IDENTITY_OR_CLAIMS"
    assert "WORKFLOW_MERGE_" + expected in rejected.stderr, rejected.stdout + rejected.stderr
    assert observe() == before


def test_public_plan_classifies_exact_objects_and_retains_required_claims(
    canonical_merge_repository,
) -> None:
    root, scope = canonical_merge_repository
    plan = integration.build_controlled_merge_plan(root, TASK)
    rows = {row["path"]: row for row in plan["dispositions"]}
    assert rows["mode.txt"]["disposition"] == "CONTRACT_SEMANTICS_UNRESOLVED"
    assert rows["absorbed.txt"]["disposition"] == "ALREADY_ABSORBED"
    assert rows["delete.txt"]["disposition"] == "ALREADY_ABSORBED"
    assert rows["keep.txt"]["disposition"] == "KEEP_CURRENT_AUTHORITY"
    assert rows["generated.txt"]["disposition"] == "REGENERATE"
    assert plan["contract_claims"] == scope["contract_claims"]
    assert set(rows) == set(scope["source_paths"])
    assert all(row["contract_ids"] == ["EXACT_OBJECT_CUSTODY"] for row in rows.values())
    assert (
        integration.validate_controlled_merge_plan(root, TASK, plan, require_review=False)["status"]
        == "COORDINATOR_REVIEW_REQUIRED"
    )
    with pytest.raises(contract.WorkflowContractError, match="REVIEW_NOT_FROZEN"):
        integration.validate_controlled_merge_plan(root, TASK, plan)


@pytest.mark.parametrize("mutation", ["claims", "acceptance", "source-path", "mode", "history"])
def test_rehashed_plan_cannot_drop_current_authority_claims_or_objects(
    canonical_merge_repository, mutation: str
) -> None:
    root, _scope_value = canonical_merge_repository
    plan = copy.deepcopy(integration.build_controlled_merge_plan(root, TASK))
    if mutation == "claims":
        plan["contract_claims"] = []
    elif mutation == "acceptance":
        plan["contract_claims"][0]["required_acceptance"] = []
    elif mutation == "source-path":
        plan["inventory"]["source_entries"].pop()
    elif mutation == "mode":
        row = next(row for row in plan["dispositions"] if row["path"] == "mode.txt")
        row["lane"]["mode"] = row["main"]["mode"]
        row["disposition"] = "ALREADY_ABSORBED"
    else:
        plan["inventory"]["source_history"] = []
    plan.pop("plan_sha256")
    plan["plan_sha256"] = contract.canonical_digest(plan)
    with pytest.raises(contract.WorkflowContractError, match="PLAN_IDENTITY_OR_CLAIMS"):
        integration.validate_controlled_merge_plan(root, TASK, plan, require_review=False)


@pytest.mark.parametrize(
    "mutation", ["none", "bytes", "delete", "mode", "extra-source", "new-source"]
)
def test_frozen_review_rechecks_real_working_objects(canonical_merge_repository, mutation):
    root, _scope_value = canonical_merge_repository
    (root / "extra-source.txt").write_bytes(b"reviewed extra source\n")
    plan = integration.build_controlled_merge_plan(root, TASK)
    proposal = {
        "resolutions": [
            {
                "path": row["path"],
                "disposition": "MERGE_REVIEWED"
                if row["disposition"] == "CONTRACT_SEMANTICS_UNRESOLVED"
                else row["disposition"],
                "result": None
                if row["disposition"] == "REGENERATE"
                else integration.working_object(root, row["path"]),
                "rationale": "Synthetic coordinator retains each observed object explicitly.",
            }
            for row in plan["dispositions"]
        ],
        "contract_resolutions": [
            {
                "claim": claim,
                "semantic_decision": "PRESERVE_VALID_RULES",
                "rationale": "Synthetic exact-object review, not production semantic approval.",
            }
            for claim in plan["contract_claims"]
        ],
        "rename_pairs": [],
    }
    transaction = root / (
        "outputs/architecture/arch_005_integration_publication_fence/transactions/"
        "merge-authority/transaction.json"
    )
    result = integration.freeze_controlled_merge_review(
        root,
        TASK,
        proposal=proposal,
        transaction_path=transaction,
        actor="integration-coordinator",
        change_id="freeze-object-review",
    )
    assert result["status"] == "READY_FOR_CONTROLLED_MERGE"
    reference = result["review_ref"]
    original_review = (root / reference["path"]).read_bytes()
    if mutation == "bytes":
        (root / "mode.txt").write_bytes(b"changed after review\r\n")
    elif mutation == "delete":
        (root / "mode.txt").unlink()
    elif mutation == "mode":
        _git(root, "update-index", "--chmod=+x", "--", "mode.txt")
    elif mutation == "extra-source":
        (root / "extra-source.txt").write_bytes(b"not reviewed\n")
    elif mutation == "new-source":
        (root / "unreviewed-source.txt").write_bytes(b"added after review\n")
    before = (_git(root, "rev-parse", "HEAD"), _git(root, "write-tree"))
    if mutation == "none":
        assert integration.validate_controlled_merge_plan(root, TASK, plan)["status"] == (
            "READY_FOR_CONTROLLED_MERGE"
        )
    else:
        expected_error = (
            "REVIEW_SOURCE_SET_CHANGED"
            if mutation == "new-source"
            else "REVIEW_WORKING_RESULT_CHANGED"
        )
        with pytest.raises(contract.WorkflowContractError, match=expected_error):
            integration.validate_controlled_merge_plan(root, TASK, plan)
    assert (root / reference["path"]).read_bytes() == original_review
    assert (_git(root, "rev-parse", "HEAD"), _git(root, "write-tree")) == before


@pytest.mark.parametrize("canonical_merge_repository", ["absorbed-generated"], indirect=True)
def test_absorbed_generated_disposition_does_not_freeze_old_output(canonical_merge_repository):
    root, _scope_value = canonical_merge_repository
    plan = integration.build_controlled_merge_plan(root, TASK)
    generated = next(row for row in plan["dispositions"] if row["path"] == "generated.txt")
    assert generated["disposition"] == "ALREADY_ABSORBED"
    assert generated["generator_id"] == "fixture-generator"
    proposal = {
        "resolutions": [
            {
                "path": row["path"],
                "disposition": "MERGE_REVIEWED"
                if row["disposition"] == "CONTRACT_SEMANTICS_UNRESOLVED"
                else row["disposition"],
                "result": None
                if row["generator_id"]
                else integration.working_object(root, row["path"]),
                "rationale": "Absorbed-source fact; generated bytes need separate closure.",
            }
            for row in plan["dispositions"]
        ],
        "contract_resolutions": [
            {
                "claim": claim,
                "semantic_decision": "PRESERVE_VALID_RULES",
                "rationale": "Synthetic source review; this does not authorize generated output.",
            }
            for claim in plan["contract_claims"]
        ],
        "rename_pairs": [],
    }
    transaction = root / (
        "outputs/architecture/arch_005_integration_publication_fence/transactions/"
        "merge-authority/transaction.json"
    )
    integration.freeze_controlled_merge_review(
        root,
        TASK,
        proposal=proposal,
        transaction_path=transaction,
        actor="integration-coordinator",
        change_id="freeze-absorbed-generated-review",
    )
    (root / "generated.txt").write_bytes(b"new output, not yet admitted by an output closure\n")
    result = integration.validate_controlled_merge_plan(root, TASK, plan)
    assert result["status"] == "READY_FOR_CONTROLLED_MERGE"
    assert (
        next(row for row in result["review"]["resolutions"] if row["path"] == "generated.txt")[
            "result"
        ]
        is None
    )
    bad = copy.deepcopy(proposal)
    next(row for row in bad["resolutions"] if row["path"] == "generated.txt")["result"] = generated[
        "main"
    ]
    with pytest.raises(contract.WorkflowContractError, match="OLD_GENERATED_BYTES"):
        integration.freeze_controlled_merge_review(
            root,
            TASK,
            proposal=bad,
            transaction_path=transaction,
            actor="integration-coordinator",
            change_id="reject-old-generated-freeze",
        )


def _freeze_candidate_fixture(root: Path, *, rename_pairs=()) -> Path:
    plan = integration.build_controlled_merge_plan(root, TASK)
    proposal = {
        "resolutions": [
            {
                "path": row["path"],
                "disposition": "MERGE_REVIEWED"
                if row["disposition"] == "CONTRACT_SEMANTICS_UNRESOLVED"
                else row["disposition"],
                "result": None
                if row["generator_id"]
                else integration.working_object(root, row["path"]),
                "rationale": "Synthetic bounded source review; generated bytes separate.",
            }
            for row in plan["dispositions"]
        ],
        "contract_resolutions": [
            {
                "claim": claim,
                "semantic_decision": "PRESERVE_VALID_RULES",
                "rationale": "Independent exact object assertions follow in the test.",
            }
            for claim in plan["contract_claims"]
        ],
        "rename_pairs": [list(pair) for pair in rename_pairs],
    }
    transaction = root / (
        "outputs/architecture/arch_005_integration_publication_fence/transactions/"
        "merge-authority/transaction.json"
    )
    integration.freeze_controlled_merge_review(
        root,
        TASK,
        proposal=proposal,
        transaction_path=transaction,
        actor="integration-coordinator",
        change_id="freeze-candidate-closure",
    )
    return transaction


@pytest.mark.parametrize("canonical_merge_repository", ["source-semantics"], indirect=True)
def test_reviewed_candidate_executes_main_and_valid_lane_rules_without_reviving_retired(
    canonical_merge_repository,
):
    root, scope = canonical_merge_repository

    def consume(raw):
        probe = raw.decode() + (
            "\nimport json\n"
            "print(json.dumps([decide(x) for x in ['shared','main','legacy','unknown']]))\n"
        )
        executed = subprocess.run(
            [sys.executable, "-c", probe],
            cwd=root.parent,
            capture_output=True,
            text=True,
            timeout=30,
        )
        assert executed.returncode == 0, executed.stderr
        return json.loads(executed.stdout)

    lane_object = _expected(root, scope["lane_head"], "mode.txt")
    main_object = _expected(root, scope["latest_main"], "mode.txt")
    lane_raw = _git(root, "cat-file", "blob", lane_object["oid"]).encode() + b"\n"
    main_raw = _git(root, "cat-file", "blob", main_object["oid"]).encode() + b"\n"
    assert consume(lane_raw) == ["VALID_LANE", "DENY", "RETIRED", "DENY"]
    assert consume(main_raw) == ["BASE", "NEW_MAIN", "DENY", "DENY"]
    # Explicit reviewed resolution, not an automatic semantic merger. The
    # independently frozen consumer vector must distinguish both old trees.
    reviewed = (
        b"def decide(tag):\n    return {'shared':'VALID_LANE','main':'NEW_MAIN'}.get(tag,'DENY')\n"
    )
    (root / "mode.txt").write_bytes(reviewed)
    _git(root, "update-index", "--chmod=+x", "--", "mode.txt")
    transaction = _freeze_candidate_fixture(root)
    prepared = integration.prepare_source_generation(
        root, TASK, transaction_path=transaction, actor="integration-coordinator"
    )
    fence = IntegrationPublicationFence(project_root=root)
    fence.checkpoint(
        transaction,
        phase="GENERATED_REBUILD_PRE",
        actor="integration-coordinator",
        generator_ids=tuple(scope["generator_order"]),
    )
    before = _source_prepare_snapshot(root)
    delta, captured = integration.render_source_candidate_delta(
        root, prepared, transaction_path=transaction, actor="integration-coordinator"
    )
    row = next(row for row in delta["operations"] if row["path"] == "mode.txt")
    assert row["before"] == main_object
    assert row["after"] == {
        "exists": True,
        "mode": "100755",
        "type": "blob",
        "oid": hashlib.sha1(b"blob " + str(len(reviewed)).encode() + b"\0" + reviewed).hexdigest(),
    }
    assert captured["mode.txt"] == reviewed
    assert consume(captured["mode.txt"]) == ["VALID_LANE", "NEW_MAIN", "DENY", "DENY"]
    assert _source_prepare_snapshot(root) == before


@pytest.mark.parametrize("canonical_merge_repository", ["source-operations"], indirect=True)
def test_reviewed_rename_modify_and_modify_delete_preserve_exact_delta(canonical_merge_repository):
    root, scope = canonical_merge_repository
    old, new, deleted = "rename-old.txt", "rename-new.txt", "delete.txt"
    assert _expected(root, scope["frozen_base"], old) == _expected(root, scope["lane_head"], new)
    assert not _expected(root, scope["lane_head"], old)["exists"]
    assert not _expected(root, scope["lane_head"], deleted)["exists"]
    for name in (old, deleted):
        assert _expected(root, scope["latest_main"], name) != _expected(
            root, scope["frozen_base"], name
        )
    retained = b"main retained modification\n"
    assert (root / old).read_bytes() == retained
    (root / new).write_bytes(retained)
    _git(root, "add", "--", new)
    _git(root, "update-index", "--chmod=+x", "--", new)
    (root / old).unlink()
    (root / deleted).unlink()
    transaction = _freeze_candidate_fixture(root, rename_pairs=((old, new),))
    prepared = integration.prepare_source_generation(
        root, TASK, transaction_path=transaction, actor="integration-coordinator"
    )
    fence = IntegrationPublicationFence(project_root=root)
    fence.checkpoint(
        transaction,
        phase="GENERATED_REBUILD_PRE",
        actor="integration-coordinator",
        generator_ids=tuple(scope["generator_order"]),
    )
    before = _source_prepare_snapshot(root)
    delta, captured = integration.render_source_candidate_delta(
        root, prepared, transaction_path=transaction, actor="integration-coordinator"
    )
    rows = {row["path"]: row for row in delta["operations"]}
    assert len(rows) == len(delta["operations"])
    absent = {"exists": False, "mode": None, "type": None, "oid": None}
    for name in (old, deleted):
        assert rows[name]["before"] == _expected(root, scope["latest_main"], name)
        assert rows[name]["after"] == absent
        assert rows[name]["operation"] == "D"
        assert name not in captured
    assert rows[new]["operation"] == "A"
    assert rows[new]["before"] == absent
    assert rows[new]["after"] == _expected(root, scope["latest_main"], old)
    assert rows[new]["after"]["mode"] == "100755"
    assert captured[new] == retained
    assert _source_prepare_snapshot(root) == before


@pytest.mark.parametrize("canonical_merge_repository", ["source-cross-file"], indirect=True)
def test_disjoint_reviewed_hunks_preserve_actual_cross_file_consumer(canonical_merge_repository):
    root, scope = canonical_merge_repository

    def raw_at(commit, path):
        oid = _expected(root, commit, path)["oid"]
        return subprocess.check_output(
            ["git", "-C", str(root), "cat-file", "blob", oid], timeout=30
        )

    def consume(program, policy):
        script = program.decode() + (
            "\nimport json, sys\nprint(json.dumps(consume(json.load(sys.stdin))))\n"
        )
        result = subprocess.run(
            [sys.executable, "-c", script],
            cwd=root.parent,
            input=policy.decode(),
            capture_output=True,
            text=True,
            timeout=30,
        )
        assert result.returncode == 0, result.stderr
        return json.loads(result.stdout)

    base = raw_at(scope["frozen_base"], "mode.txt")
    lane = raw_at(scope["lane_head"], "mode.txt")
    main = raw_at(scope["latest_main"], "mode.txt")
    main_policy = raw_at(scope["latest_main"], "keep.txt")
    assert consume(lane, raw_at(scope["lane_head"], "keep.txt")) == [
        "LANE_LEFT",
        "BASE_RIGHT",
        "LANE_DATA",
    ]
    assert consume(main, main_policy) == ["BASE_LEFT", "MAIN_RIGHT", "MAIN_OLD"]
    merge_paths = [root.parent / name for name in ("main-program", "base-program", "lane-program")]
    for path, raw in zip(merge_paths, (main, base, lane), strict=True):
        path.write_bytes(raw)
    merged = subprocess.run(
        ["git", "merge-file", "-p", *map(str, merge_paths)],
        cwd=root.parent,
        capture_output=True,
        timeout=30,
    )
    assert merged.returncode == 0, merged.stderr
    expected_program = _crossfile_program("LANE_LEFT", "MAIN_RIGHT", "new")
    assert merged.stdout == expected_program
    (root / "mode.txt").write_bytes(merged.stdout)
    _git(root, "update-index", "--chmod=+x", "--", "mode.txt")
    transaction = _freeze_candidate_fixture(root)
    prepared = integration.prepare_source_generation(
        root, TASK, transaction_path=transaction, actor="integration-coordinator"
    )
    fence = IntegrationPublicationFence(project_root=root)
    fence.checkpoint(
        transaction,
        phase="GENERATED_REBUILD_PRE",
        actor="integration-coordinator",
        generator_ids=tuple(scope["generator_order"]),
    )
    before = _source_prepare_snapshot(root)
    delta, captured = integration.render_source_candidate_delta(
        root, prepared, transaction_path=transaction, actor="integration-coordinator"
    )
    rows = {row["path"]: row for row in delta["operations"]}
    assert captured["mode.txt"] == expected_program
    assert rows["mode.txt"]["after"]["mode"] == "100755"
    assert (
        rows["mode.txt"]["after"]["oid"]
        == hashlib.sha1(
            b"blob " + str(len(expected_program)).encode() + b"\0" + expected_program
        ).hexdigest()
    )
    assert "keep.txt" not in rows, "M policy is retained, not overwritten from L"
    assert (root / "keep.txt").read_bytes() == main_policy
    assert consume(captured["mode.txt"], main_policy) == ["LANE_LEFT", "MAIN_RIGHT", "MAIN_DATA"]
    assert _source_prepare_snapshot(root) == before


@pytest.mark.parametrize(
    "canonical_merge_repository",
    ["conflict-text", "conflict-clean", "conflict-crosspath"],
    indirect=True,
)
def test_public_unresolved_actual_conflict_cannot_gain_review_permission(
    canonical_merge_repository, request, bootstrap: str | None = None,
):
    root, scope = canonical_merge_repository
    kind = request.node.callspec.params["canonical_merge_repository"].removeprefix("conflict-")
    programs, policies = _contract_conflict_sources(kind)
    for commit, program in zip(
        (scope["frozen_base"], scope["lane_head"], scope["latest_main"]), programs, strict=True
    ):
        oid = _expected(root, commit, "mode.txt")["oid"]
        assert (
            subprocess.check_output(["git", "-C", str(root), "cat-file", "blob", oid], timeout=30)
            == program
        )
    inputs = [root.parent / label for label in ("merge-main", "merge-base", "merge-lane")]
    for path, raw in zip(inputs, (programs[2], programs[0], programs[1]), strict=True):
        path.write_bytes(raw)
    merged = subprocess.run(
        ["git", "merge-file", "-p", *map(str, inputs)], capture_output=True, timeout=30
    )

    def run(program, policy):
        return subprocess.run(
            [
                sys.executable,
                "-c",
                program.decode()
                + "\nimport json,sys\nprint(json.dumps(consume(json.load(sys.stdin))))\n",
            ],
            cwd=root.parent,
            input=policy.decode(),
            capture_output=True,
            text=True,
            timeout=30,
        )

    if kind == "text":
        assert merged.returncode == 1
        assert b"<<<<<<<" in merged.stdout and b">>>>>>>" in merged.stdout
    else:
        assert merged.returncode == 0, merged.stderr
        for program, policy, expected in (
            (programs[1], policies[1], 2),
            (programs[2], policies[2], 3),
        ):
            independent = run(program, policy)
            assert independent.returncode == 0, independent.stderr
            assert json.loads(independent.stdout) == expected
        broken = run(merged.stdout, policies[2])
        assert broken.returncode != 0
        assert "KeyError: 'new'" in broken.stderr
    fence = IntegrationPublicationFence(project_root=root)
    before = (
        _source_prepare_snapshot(root),
        _git(root, "show-ref"),
        (root / ".git/config").read_bytes(),
        fence.guard.store.replay().head_event_ids,
    )
    script = root / "scripts/architecture_arch005_workflow.py"
    planned = subprocess.run(
        [sys.executable, str(script), "merge-plan", "--task-id", TASK],
        cwd=root,
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert planned.returncode == 0, planned.stdout + planned.stderr
    plan = json.loads(planned.stdout)
    assert plan["status"] == "COORDINATOR_REVIEW_REQUIRED"
    rows = {row["path"]: row for row in plan["dispositions"]}
    assert rows["mode.txt"]["disposition"] == "CONTRACT_SEMANTICS_UNRESOLVED"
    assert rows["mode.txt"]["contract_ids"] == rows["keep.txt"]["contract_ids"]
    assert rows["mode.txt"]["contract_ids"] == ["EXACT_OBJECT_CUSTODY"]
    assert plan["contract_claims"] == scope["contract_claims"]
    supplied = root.parent / "unresolved-plan.json"
    _json(supplied, plan)
    command = [
        sys.executable, str(script), "merge-validate", "--task-id", TASK, "--plan", str(supplied)
    ]
    if bootstrap is not None:
        command = [sys.executable, "-c", bootstrap, *command[1:]]
    rejected = subprocess.run(
        command,
        cwd=root,
        capture_output=True,
        text=True,
        timeout=60,
    )
    _json(root.parent / "unresolved-cli-result.json", {
        "returncode": rejected.returncode, "stdout": rejected.stdout, "stderr": rejected.stderr,
    })
    assert rejected.returncode != 0, "UNRESOLVED_CONFLICT_MUST_NOT_GAIN_PERMISSION"
    assert "WORKFLOW_MERGE_REVIEW_NOT_FROZEN" in rejected.stderr
    assert (
        _source_prepare_snapshot(root),
        _git(root, "show-ref"),
        (root / ".git/config").read_bytes(),
        fence.guard.store.replay().head_event_ids,
    ) == before


@pytest.mark.parametrize(
    "canonical_merge_repository", ["conflict-text", "conflict-clean", "conflict-crosspath"],
    indirect=True,
)
def test_m02_actual_conflict_mutant_hits_original_public_refusal_assertion(
    canonical_merge_repository, request,
) -> None:
    root, _scope = canonical_merge_repository
    original = inspect.getsource(integration.validate_controlled_merge_plan)
    target = '        _fail("REVIEW_NOT_FROZEN")'
    assert original.count(target) == 1, "INVALID_MUTATION_TARGET"
    changed = original.replace(
        target, '        return {"status": "READY_FOR_CONTROLLED_MERGE", "plan": expected}', 1
    )
    (root.parent / "m02-method-before.py").write_text(original, encoding="utf-8")
    method_path = root.parent / "m02-method-after.py"
    method_path.write_text(changed, encoding="utf-8")
    witness = root.parent / "m02-loaded-method.json"
    bootstrap = (
        "from ai_trading_system.platform.architecture import workflow_integration as _m02\n"
        "from pathlib import Path as _M02Path\nimport hashlib as _m02hash,json as _m02json\n"
        f"_m02body=_M02Path({str(method_path)!r}).read_text(encoding='utf-8')\n"
        "_m02namespace={}\n"
        f"exec(compile(_m02body,{str(method_path)!r},'exec'),_m02.__dict__,_m02namespace)\n"
        "_m02.validate_controlled_merge_plan=_m02namespace['validate_controlled_merge_plan']\n"
        f"_M02Path({str(witness)!r}).write_text(_m02json.dumps({{"
        "'module_path':_m02.__file__,"
        "'module_sha256':_m02hash.sha256(_M02Path(_m02.__file__).read_bytes()).hexdigest(),"
        "'method_sha256':_m02hash.sha256(_m02body.encode()).hexdigest()}),encoding='utf-8')\n"
        "import runpy,sys\nsys.argv=sys.argv[1:]\n"
        "runpy.run_path(sys.argv[0],run_name='__main__')\n"
    )
    (root.parent / "m02-driver.py").write_text(bootstrap, encoding="utf-8")
    fence = IntegrationPublicationFence(project_root=root)

    def observed():
        return (
            _source_prepare_snapshot(root), _git(root, "show-ref"),
            (root / ".git/config").read_bytes(), fence.guard.store.replay().head_event_ids,
        )

    before = observed()
    test_public_unresolved_actual_conflict_cannot_gain_review_permission(
        canonical_merge_repository, request
    )
    original_result = (root.parent / "unresolved-cli-result.json").read_bytes()
    (root.parent / "m02-original-cli-result.json").write_bytes(original_result)
    assert observed() == before
    with pytest.raises(
        AssertionError, match=r"^UNRESOLVED_CONFLICT_MUST_NOT_GAIN_PERMISSION(?:\n|$)"
    ):
        test_public_unresolved_actual_conflict_cannot_gain_review_permission(
            canonical_merge_repository, request, bootstrap=bootstrap
        )
    result = json.loads((root.parent / "unresolved-cli-result.json").read_text())
    assert result["returncode"] == 0 and result["stderr"] == ""
    assert json.loads(result["stdout"])["status"] == "READY_FOR_CONTROLLED_MERGE"
    assert observed() == before
    loaded = json.loads(witness.read_text())
    assert loaded["method_sha256"] == hashlib.sha256(changed.encode()).hexdigest()
    _json(root.parent / "m02-counterfactual.json", {
        "schema_version": "devx015_m02_counterfactual.v1", "mutant_id": "M02",
        "conflict": request.node.callspec.params["canonical_merge_repository"],
        "target_assertion_killed": True,
        "target_assertion": "UNRESOLVED_CONFLICT_MUST_NOT_GAIN_PERMISSION",
        "scope": "ORIGINAL_CONFLICT_PUBLIC_CLI", "formal_acceptance": False,
        "before_method_sha256": hashlib.sha256(original.encode()).hexdigest(),
        "after_method_sha256": hashlib.sha256(changed.encode()).hexdigest(),
    })


def _source_prepare_snapshot(root: Path):
    """Independent fixture byte inventory; the preparation API must remain read-only."""
    excluded = {
        row["path"]
        for row in safe_load_yaml_path(
            root / "config/architecture/arch_005_s4d_checkout_guard.yaml"
        )["known_unrelated_exclusions"]
    }
    files = {
        path.relative_to(root).as_posix(): path.read_bytes()
        for path in root.rglob("*")
        if ".git" not in path.relative_to(root).parts
        and not any(
            path.relative_to(root).as_posix() == name
            or path.relative_to(root).as_posix().startswith(name + "/")
            for name in excluded
        )
        and path.is_file()
    }
    return (
        files,
        _git(root, "rev-parse", "HEAD", "refs/heads/main"),
        (root / ".git/index").read_bytes(),
    )


@pytest.mark.parametrize("canonical_merge_repository", ["candidate"], indirect=True)
def test_prepare_source_generation_binds_complete_main_and_reviewed_raw_objects(
    canonical_merge_repository,
) -> None:
    root, scope = canonical_merge_repository
    raw = b"reviewed raw source\0value\r\n"
    added = b"reviewed new source\r\n"
    (root / "mode.txt").write_bytes(raw)
    (root / "extra-source.txt").write_bytes(added)
    (root / "rename-old.txt").unlink()
    _git(root, "update-index", "--chmod=+x", "--", "mode.txt")
    transaction = _freeze_candidate_fixture(root)
    main_objects = {}
    excluded = {
        row["path"]
        for row in safe_load_yaml_path(
            root / "config/architecture/arch_005_s4d_checkout_guard.yaml"
        )["known_unrelated_exclusions"]
    }
    for row in _git(root, "ls-tree", "-r", "--full-tree", scope["latest_main"]).splitlines():
        metadata, name = row.split("\t", 1)
        if any(name == value or name.startswith(value + "/") for value in excluded):
            continue
        mode, kind, oid = metadata.split()
        main_objects[name] = {"exists": True, "mode": mode, "type": kind, "oid": oid}
    authority = contract.load_current_task_authority(root, TASK)
    before = _source_prepare_snapshot(root)
    result = integration.prepare_source_generation(
        root, TASK, transaction_path=transaction, actor="integration-coordinator"
    )
    assert result["schema_version"] == "controlled_source_generation_inputs.v1"
    assert result["status"] == "PREPARED_SOURCE_GENERATION_INPUTS"
    assert result["materialization_allowed"] is False
    assert result["review_ref"] == authority["authority"]["review_ref"]
    assert result["authority_sha256"] == authority["authority_sha256"]
    inputs = result["expected_inputs"]
    assert main_objects.keys() <= inputs.keys()
    assert main_objects.keys() <= result["main_inputs"].keys()
    assert {name: result["main_inputs"][name] for name in main_objects} == main_objects
    assert inputs["mode.txt"] == {
        "exists": True,
        "mode": "100755",
        "type": "blob",
        "oid": _git(root, "hash-object", "--no-filters", "--stdin", content=raw),
    }
    assert inputs["extra-source.txt"] == {
        "exists": True,
        "mode": "100644",
        "type": "blob",
        "oid": _git(root, "hash-object", "--no-filters", "--stdin", content=added),
    }
    assert inputs["rename-old.txt"] == {
        "exists": False,
        "mode": None,
        "type": None,
        "oid": None,
    }
    for name, state in main_objects.items():
        if name in before[0] and state["mode"] in {"100644", "100755"}:
            content = before[0][name]
            raw_oid = hashlib.sha1(
                b"blob " + str(len(content)).encode() + b"\0" + content
            ).hexdigest()
            if raw_oid == state["oid"] and name != "mode.txt":
                assert inputs[name] == state, name
    assert _source_prepare_snapshot(root) == before


@pytest.mark.parametrize("canonical_merge_repository", ["candidate"], indirect=True)
def test_prepare_source_generation_rejects_unknown_generated_name_before_open(
    canonical_merge_repository, monkeypatch: pytest.MonkeyPatch
) -> None:
    import ctypes

    root, _scope = canonical_merge_repository
    transaction = _freeze_candidate_fixture(root)
    canary = root / "inputs/architecture/unreviewed.json"
    canary.write_bytes(b"unreviewed generated namespace input: never read")
    before = _source_prepare_snapshot(root)
    opened = []

    def guarded(original):
        def call(path, *args, **kwargs):
            if not isinstance(path, int) and Path(path).absolute() == canary.absolute():
                opened.append(str(path))
                raise AssertionError("unknown generated file reached actual open")
            return original(path, *args, **kwargs)

        return call

    class NativeOpen:
        def __init__(self, native):
            object.__setattr__(self, "native", native)

        def __setattr__(self, name, value):
            setattr(self.native, name, value)

        def __call__(self, *args):
            return guarded(self.native)(*args)

    class ObservedKernel:
        def __init__(self, native):
            self.native = native
            self.CreateFileW = NativeOpen(native.CreateFileW)

        def __getattr__(self, name):
            return getattr(self.native, name)

    real_dll = ctypes.WinDLL
    with monkeypatch.context() as hooks:
        hooks.setattr(Path, "open", guarded(Path.open))
        hooks.setattr(os, "open", guarded(os.open))
        hooks.setattr(ctypes, "WinDLL", lambda *a, **k: ObservedKernel(real_dll(*a, **k)))
        with pytest.raises(contract.WorkflowContractError, match="CANDIDATE_DELTA_UNCOVERED"):
            integration.prepare_source_generation(
                root, TASK, transaction_path=transaction, actor="integration-coordinator"
            )
    assert opened == []
    assert _source_prepare_snapshot(root) == before


@pytest.mark.parametrize("canonical_merge_repository", ["candidate"], indirect=True)
def test_prepare_source_generation_rejects_changed_reviewed_source(
    canonical_merge_repository,
) -> None:
    root, _scope = canonical_merge_repository
    (root / "mode.txt").write_bytes(b"source bytes explicitly included in review\n")
    transaction = _freeze_candidate_fixture(root)
    (root / "mode.txt").write_bytes(b"changed after immutable source review\n")
    before = _source_prepare_snapshot(root)
    with pytest.raises(contract.WorkflowContractError, match="REVIEW_WORKING_RESULT_CHANGED"):
        integration.prepare_source_generation(
            root, TASK, transaction_path=transaction, actor="integration-coordinator"
        )
    assert _source_prepare_snapshot(root) == before


@pytest.mark.parametrize("canonical_merge_repository", ["candidate"], indirect=True)
@pytest.mark.parametrize("flag", ["--assume-unchanged", "--skip-worktree"])
def test_prepare_source_generation_rejects_hidden_index_inputs(
    canonical_merge_repository, flag: str
) -> None:
    root, _scope = canonical_merge_repository
    transaction = _freeze_candidate_fixture(root)
    _git(root, "update-index", flag, "--", "mode.txt")
    before = _source_prepare_snapshot(root)
    try:
        with pytest.raises(contract.WorkflowContractError, match="GENERATOR_INDEX_VISIBILITY"):
            integration.prepare_source_generation(
                root, TASK, transaction_path=transaction, actor="integration-coordinator"
            )
        assert _source_prepare_snapshot(root) == before
    finally:
        _git(root, "update-index", flag.replace("--", "--no-", 1), "--", "mode.txt")


@pytest.mark.parametrize("canonical_merge_repository", ["candidate"], indirect=True)
@pytest.mark.parametrize("damage", ["actor", "no-review"])
def test_prepare_source_generation_requires_actual_actor_and_frozen_review(
    canonical_merge_repository, damage: str
) -> None:
    root, _scope = canonical_merge_repository
    if damage == "actor":
        transaction = _freeze_candidate_fixture(root)
    else:
        transaction = root / (
            "outputs/architecture/arch_005_integration_publication_fence/transactions/"
            "merge-authority/transaction.json"
        )
    before = _source_prepare_snapshot(root)
    code = "CANDIDATE_ACTOR" if damage == "actor" else "REVIEW_NOT_FROZEN"
    with pytest.raises(contract.WorkflowContractError, match=code):
        integration.prepare_source_generation(
            root,
            TASK,
            transaction_path=transaction,
            actor="other-actor" if damage == "actor" else "integration-coordinator",
        )
    assert _source_prepare_snapshot(root) == before


@pytest.mark.parametrize("canonical_merge_repository", ["candidate"], indirect=True)
@pytest.mark.parametrize(
    "mutation", ["none", "unknown-input", "unknown-script", "actor", "source", "output-mode"]
)
def test_candidate_delta_closes_review_and_real_canonical_outputs(
    canonical_merge_repository, mutation: str, monkeypatch: pytest.MonkeyPatch
):
    root, scope = canonical_merge_repository
    raw = b"reviewed binary\0bytes\r\n"
    (root / "mode.txt").write_bytes(raw)
    _git(root, "update-index", "--chmod=+x", "--", "mode.txt")
    transaction = _freeze_candidate_fixture(root)
    actor = "integration-coordinator"
    canary = root / (
        "scripts/unreviewed_canary.py"
        if mutation == "unknown-script"
        else "inputs/architecture/unreviewed.json"
    )
    opened = []
    if mutation in {"unknown-input", "unknown-script"}:
        canary.write_bytes(b"invalid JSON; never open this unreviewed generated-root file")
        original_open = Path.open
        original_os_open = os.open

        def guarded_open(path, *args, **kwargs):
            if Path(path).absolute() == canary.absolute():
                opened.append(str(path))
                raise AssertionError("unreviewed path was opened")
            return original_open(path, *args, **kwargs)

        def guarded_os_open(path, *args, **kwargs):
            if Path(path).absolute() == canary.absolute():
                opened.append(str(path))
                raise AssertionError("unreviewed path was opened via os.open")
            return original_os_open(path, *args, **kwargs)

        monkeypatch.setattr(Path, "open", guarded_open)
        monkeypatch.setattr(os, "open", guarded_os_open)
    elif mutation == "actor":
        actor = "lane-worker"
    elif mutation == "source":
        (root / "mode.txt").write_bytes(b"unreviewed replacement\n")
    elif mutation == "output-mode":
        _git(root, "update-index", "--chmod=+x", "--", canonical.CANONICAL_INDEX_PATH)
    before = (_git(root, "rev-parse", "HEAD"), (root / ".git/index").read_bytes())
    if mutation != "none":
        code = {
            "unknown-input": "CANDIDATE_DELTA_UNCOVERED",
            "unknown-script": "CANDIDATE_DELTA_UNCOVERED",
            "actor": "CANDIDATE_ACTOR",
            "source": "REVIEW_WORKING_RESULT_CHANGED",
            "output-mode": "CANDIDATE_OUTPUT_MODE",
        }[mutation]
        with pytest.raises(contract.WorkflowContractError, match=code):
            integration.inspect_controlled_candidate_delta(
                root, TASK, transaction_path=transaction, actor=actor
            )
    else:
        result = subprocess.run(
            [
                sys.executable,
                str(root / "scripts/architecture_arch005_workflow.py"),
                "candidate-input-inspect",
                "--task-id",
                TASK,
                "--publication-transaction",
                str(transaction),
                "--actor",
                actor,
            ],
            cwd=root,
            capture_output=True,
            text=True,
            encoding="utf-8",
            timeout=60,
        )
        assert result.returncode == 0, result.stdout + result.stderr
        observed = json.loads(result.stdout)
        assert observed["status"] == "VALIDATED_CANDIDATE_DELTA"
        assert (_git(root, "rev-parse", "HEAD"), (root / ".git/index").read_bytes()) == before, (
            "candidate-input-inspect changed original HEAD/index before the test's dirty audit"
        )
        assert observed["generator_order"] == ["canonical-task-source"]
        assert observed["materialization_allowed"] is False
        assert observed["generator_execution_proven"] is False
        by_path = {row["path"]: row for row in observed["operations"]}
        source = by_path["mode.txt"]
        assert source["before"] == _expected(root, scope["latest_main"], "mode.txt")
        assert source["after"] == {
            "exists": True,
            "type": "blob",
            "mode": "100755",
            "oid": hashlib.sha1(b"blob " + str(len(raw)).encode() + b"\0" + raw).hexdigest(),
        }
        assert source["origin"] == "REVIEWED_SOURCE"
        assert source["generator_id"] is None
        assert by_path[canonical.CANONICAL_INDEX_PATH]["generator_id"] == "canonical-task-source"
        assert by_path[SCOPE_PATH]["origin"] == "REVIEWED_SOURCE"
        # Every actual dirty name appears as a reviewed/generated delta, unless
        # its raw object is already identical to M (not an invented operation).
        policy = safe_load_yaml_path(root / "config/architecture/arch_005_s4d_checkout_guard.yaml")
        pathspec = [
            ".",
            *(":(exclude,literal)" + row["path"] for row in policy["known_unrelated_exclusions"]),
        ]
        dirty = set(
            _git(
                root,
                "-c",
                "diff.autoRefreshIndex=false",
                "diff",
                "--name-only",
                "HEAD",
                "--",
                *pathspec,
            ).splitlines()
        )
        dirty.update(
            _git(root, "ls-files", "--others", "--exclude-standard", "--", *pathspec).splitlines()
        )
        for name in dirty:
            if name not in by_path:
                content = (root / name).read_bytes()
                oid = hashlib.sha1(
                    b"blob " + str(len(content)).encode() + b"\0" + content
                ).hexdigest()
                assert oid == _expected(root, scope["latest_main"], name)["oid"]
    assert opened == []
    assert (_git(root, "rev-parse", "HEAD"), (root / ".git/index").read_bytes()) == before


@pytest.mark.parametrize("canonical_merge_repository", ["candidate"], indirect=True)
def test_candidate_delta_raw_capture_and_mid_capture_change(
    canonical_merge_repository, monkeypatch
):
    root, _scope_value = canonical_merge_repository
    raw = b"raw captured content\0\xff\r\n"
    (root / "mode.txt").write_bytes(raw)
    transaction = _freeze_candidate_fixture(root)
    result, captured = integration._capture_controlled_candidate_delta(
        root, TASK, transaction_path=transaction, actor="integration-coordinator"
    )
    assert captured["mode.txt"] == raw
    assert set(captured) == {row["path"] for row in result["operations"] if row["operation"] != "D"}
    before = (_git(root, "rev-parse", "HEAD"), (root / ".git/index").read_bytes())
    real_inspect = integration.inspect_canonical_merge_outputs
    calls = []

    def drift_after_real_revalidation(*args, **kwargs):
        result = real_inspect(*args, **kwargs)
        calls.append(result["status"])
        if len(calls) == 2:
            (root / "mode.txt").write_bytes(b"changed after raw capture\0\n")
        return result

    monkeypatch.setattr(
        integration, "inspect_canonical_merge_outputs", drift_after_real_revalidation
    )
    with pytest.raises(contract.WorkflowContractError, match="CANDIDATE_OBJECT_CHANGED"):
        integration._capture_controlled_candidate_delta(
            root, TASK, transaction_path=transaction, actor="integration-coordinator"
        )
    assert len(calls) == 2
    assert (_git(root, "rev-parse", "HEAD"), (root / ".git/index").read_bytes()) == before


@pytest.mark.parametrize("canonical_merge_repository", ["candidate"], indirect=True)
def test_candidate_structure_bootstrap_does_not_replace_freshness(canonical_merge_repository):
    root, _scope_value = canonical_merge_repository
    transaction = _freeze_candidate_fixture(root)
    source = root / "scripts/architecture_arch005_task_source.py"
    source.write_bytes(source.read_bytes() + b"\n# task_register.md deliberate inventory drift\n")
    structure = contract.load_current_task_structure(root, TASK)
    assert structure["consumer_inventory_checked"] is False
    assert structure["authority"]["review_ref"] is not None
    with pytest.raises(canonical.CanonicalTaskRegistryError, match="CONSUMER_INVENTORY_STALE"):
        contract.load_current_task_authority(root, TASK)
    with pytest.raises(contract.WorkflowContractError, match="CANDIDATE_DELTA_UNCOVERED"):
        integration.inspect_controlled_candidate_delta(
            root, TASK, transaction_path=transaction, actor="integration-coordinator"
        )


def _rewrite_registered_history_with_current_hashes(root: Path) -> None:
    # Rebuild all local hashes with the real renderer: internal consistency
    # must not authorize rewriting the external main's immutable history.
    registry = canonical.validate_canonical_registry(project_root=root)
    fragments = copy.deepcopy(registry.fragments)
    changed = next(row for row in fragments if row["task_record"]["task_id"] == TASK)
    assert changed["events"][0]["event_type"] == "TASK_REGISTERED"
    changed["events"][0]["payload"]["forged_history_annotation"] = True
    previous_event = None
    for event in changed["events"]:
        event["previous_state_event_id"] = previous_event
        event["event_id"] = canonical._canonical_event_id(event)
        previous_event = event["event_id"]
    changed["last_event_id"] = previous_event
    changed["fragment_checksum"] = canonical._payload_checksum(changed, "fragment_checksum")
    records = canonical._write_canonical_fragments(
        root,
        fragments,
        order_by_task={row["task_id"]: row["order"] for row in registry.index["fragments"]},
    )
    policy = canonical.load_cutover_policy(root)
    index = canonical._build_index(
        root=root,
        policy=policy,
        fragment_records=records,
        fragments=fragments,
        templates=registry.index["templates"],
        governance_cycles=registry.index["governance_cycles"],
        manifest_sha256=registry.index["cutover_manifest_sha256"],
        consumer_inventory_sha256=registry.index["consumer_inventory_sha256"],
    )
    canonical._write_index_and_views(root, policy, index, fragments)
    canonical.validate_canonical_registry(project_root=root)


@pytest.mark.parametrize(
    "mutation", ["none", "unknown-file", "unrelated-task", "rewritten-history"]
)
def test_canonical_output_inventory_uses_main_and_exact_paths(canonical_merge_repository, mutation):
    root, scope = canonical_merge_repository
    if mutation == "unknown-file":
        (root / "registry/development_tasks/unregistered-canary.yaml").write_bytes(
            b"invalid YAML: [do not parse\n"
        )
    elif mutation == "unrelated-task":
        canonical.update_task(
            project_root=root,
            task_id=TASK + "-OTHER",
            actor="integration-coordinator",
            change_id="unrelated-valid-append",
            occurred_at="2026-09-11T10:01:00+00:00",
            base_commit=scope["latest_main"],
            notes="Legitimate internal chain, unrelated to merge.",
        )
    elif mutation == "rewritten-history":
        _rewrite_registered_history_with_current_hashes(root)
    if mutation == "none":
        result = integration.inspect_canonical_merge_outputs(root, TASK)
        assert result["status"] == "VALIDATED_CANONICAL_INPUTS"
        assert result["materialization_allowed"] is False
        assert result["main"] == scope["latest_main"]
        registry = canonical.validate_canonical_registry(project_root=root)
        assert {row["path"] for row in registry.index["fragments"]} <= set(result["output_paths"])
        assert "docs/task_register.md" in result["output_paths"]
        assert "inputs/architecture/arch_005_s5_cutover_manifest.yaml" not in result["output_paths"]
    else:
        code = {
            "unknown-file": "OUTPUT_SET_CHANGED",
            "unrelated-task": "UNRELATED_TASK_CHANGED",
            "rewritten-history": "EVENT_HISTORY_CHANGED",
        }[mutation]
        with pytest.raises(contract.WorkflowContractError, match=code):
            integration.inspect_canonical_merge_outputs(root, TASK)


@pytest.mark.parametrize("mutation", ["none", "stale-fragment", "unknown-file"])
def test_report_output_inventory_uses_official_builder(
    canonical_merge_repository, monkeypatch, mutation
):
    from test_devx_006d_report_catalog_flow_authority import _write_fixture

    from ai_trading_system.platform.architecture import report_catalog_flow_authority as report

    root, scope = canonical_merge_repository
    fixture = _write_fixture(root)
    fragments = fixture["result"]["fragment_paths"]
    canary = root / "registry/report_catalog_flow_authority/fragments/unreviewed.json"
    if mutation == "stale-fragment":
        (root / fragments[0]).write_bytes(b"invalid stale fragment")
    elif mutation == "unknown-file":
        canary.write_bytes(b"invalid JSON: do not read")
    attempts = []
    original_open = os.open
    original_path_open = Path.open

    def guarded_open(path, *args, **kwargs):
        if str(path) == str(canary):
            attempts.append(str(path))
            raise AssertionError("unknown fragment was opened")
        return original_open(path, *args, **kwargs)

    def guarded_path_open(path, *args, **kwargs):
        if path == canary:
            attempts.append(str(path))
            raise AssertionError("unknown fragment was opened")
        return original_path_open(path, *args, **kwargs)

    monkeypatch.setattr(os, "open", guarded_open)
    monkeypatch.setattr(Path, "open", guarded_path_open)
    if mutation == "none":
        result = integration.inspect_report_merge_outputs(root, TASK)
        assert result["status"] == "VALIDATED_REPORT_INPUTS"
        assert result["main"] == scope["latest_main"]
        assert result["materialization_allowed"] is False
        assert result["retained_main_paths"] == []
        assert set(result["output_paths"]) == set(fragments) | {
            fixture["policy"]["index_path"],
            fixture["policy"]["consumer_inventory_path"],
            report.DEFAULT_POLICY_PATH.as_posix(),
        }
        assert set(fixture["sources"]) <= set(result["dependency_paths"])
    elif mutation == "stale-fragment":
        with pytest.raises(report.ReportCatalogFlowAuthorityError, match="RCF_FRAGMENT_STALE"):
            integration.inspect_report_merge_outputs(root, TASK)
    else:
        with pytest.raises(contract.WorkflowContractError, match="REPORT_OUTPUT_SET_CHANGED"):
            integration.inspect_report_merge_outputs(root, TASK)
    assert attempts == []


@pytest.mark.parametrize("canonical_merge_repository", ["report"], indirect=True)
@pytest.mark.parametrize("tamper", [False, True, "policy-contract"])
def test_report_output_inventory_retains_only_unchanged_main_fragments(
    canonical_merge_repository, tamper
):
    from test_devx_006d_report_catalog_flow_authority import _seal

    from ai_trading_system.platform.architecture import report_catalog_flow_authority as report

    root, _scope_value = canonical_merge_repository
    before = report.validate_repository_authority(root)
    policy = report.load_policy(root)
    if tamper == "policy-contract":
        policy["owner_decision"] = "unreviewed-but-well-formed-owner-decision"
    content = b"# Changed Flow\n\nnew graph -> output\n"
    (root / "docs/system_flow.md").write_bytes(content)
    target = next(row for row in policy["targets"] if row["target_id"] == "system_flow")
    target.update(_seal(content))
    target["entry_count"] = len(report._split_markdown("system_flow", content))
    (root / report.DEFAULT_POLICY_PATH).write_bytes(canonical._yaml_bytes(policy))
    after = report.build_repository_authority(root, write=True)
    retained = set(before["fragment_paths"]) - set(after["fragment_paths"])
    assert retained
    if tamper == "policy-contract":
        report.validate_repository_authority(root)
        with pytest.raises(contract.WorkflowContractError, match="REPORT_POLICY_CONTRACT_CHANGED"):
            integration.inspect_report_merge_outputs(root, TASK)
    elif tamper:
        (root / sorted(retained)[0]).write_bytes(b"changed historical bytes")
        with pytest.raises(
            contract.WorkflowContractError, match="REPORT_HISTORICAL_OUTPUT_CHANGED"
        ):
            integration.inspect_report_merge_outputs(root, TASK)
    else:
        result = integration.inspect_report_merge_outputs(root, TASK)
        assert set(result["retained_main_paths"]) == retained
        assert not retained & set(result["output_paths"])


@pytest.mark.parametrize("canonical_merge_repository", ["compatibility"], indirect=True)
@pytest.mark.parametrize(
    "mutation", ["none", "nonlatest-fragment", "unknown-file", "obsolete-main"]
)
def test_compatibility_output_inventory_uses_complete_official_chain(
    canonical_merge_repository, monkeypatch, mutation
):
    from ai_trading_system.platform.architecture import compatibility_authority as compatibility

    root, scope = canonical_merge_repository
    result = compatibility.build_repository_authority(root, write=True)
    entries = result["index"]["entries"]
    assert [row["section_id"] for row in entries] == [
        "phase_devx_006c_compatibility_authority_fragmentation",
        "phase_devx_006d_report_catalog_flow_lossless_fragmentation",
        "phase_arch_005_s5_canonical_task_source_cutover",
        "phase_trading_2542c_growth_action_value_independent_review_remediation_and_freeze_readiness_v1",
        "phase_devx_009_parallel_integration_publication_fence_and_generated_state_rebuild_v1",
    ]
    prior = json.loads(_git(root, "show", scope["latest_main"] + ":" + result["index_path"]))
    obsolete = {row["fragment_path"] for row in prior["entries"]} - {
        row["fragment_path"] for row in entries
    }
    assert obsolete
    if mutation == "nonlatest-fragment":
        assert entries[0]["fragment_path"] != result["fragment_path"]
        (root / entries[0]["fragment_path"]).write_bytes(b"stale nonlatest fragment")
    elif mutation == "obsolete-main":
        target = root / sorted(obsolete)[0]
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(b"unreviewed replacement of obsolete main bytes")
    canary = root / "registry/architecture_compatibility_authority/fragments/unreviewed.json"
    if mutation == "unknown-file":
        canary.write_bytes(b"invalid JSON: never open or delete")
    original_open = Path.open
    original_os_open = os.open
    attempts = []

    def guarded_open(path, *args, **kwargs):
        if path == canary:
            attempts.append(str(path))
            raise AssertionError("unknown compatibility fragment was opened")
        return original_open(path, *args, **kwargs)

    def guarded_os_open(path, *args, **kwargs):
        if str(path) == str(canary):
            attempts.append(str(path))
            raise AssertionError("unknown compatibility fragment was opened")
        return original_os_open(path, *args, **kwargs)

    monkeypatch.setattr(Path, "open", guarded_open)
    monkeypatch.setattr(os, "open", guarded_os_open)
    if mutation == "none":
        inventory = integration.inspect_compatibility_merge_outputs(root, TASK)
        assert inventory["status"] == "VALIDATED_COMPATIBILITY_INPUTS"
        assert inventory["materialization_allowed"] is False
        assert inventory["deletion_allowed"] is False
        assert set(inventory["output_paths"]) == {row["fragment_path"] for row in entries} | {
            result["index_path"],
            result["consumer_inventory_path"],
        }
        assert set(inventory["obsolete_main_paths"]) == obsolete
    else:
        code = {
            "nonlatest-fragment": "COMPATIBILITY_FRAGMENT_STALE",
            "unknown-file": "COMPATIBILITY_OUTPUT_SET_CHANGED",
            "obsolete-main": "COMPATIBILITY_HISTORICAL_OUTPUT_CHANGED",
        }[mutation]
        with pytest.raises(contract.WorkflowContractError, match=code):
            integration.inspect_compatibility_merge_outputs(root, TASK)
    assert attempts == []
    if mutation == "unknown-file":
        assert canary.exists()


@pytest.mark.parametrize("mutation", ["none", "module", "fitness", "deprecation"])
def test_architecture_output_inventory_recomputes_official_outputs(
    canonical_merge_repository, mutation
):
    from ai_trading_system.platform.architecture import (
        build_aggregate_shadow_index,
        build_architecture_fitness,
        build_module_manifest,
        build_test_manifest,
        load_deprecation_policy,
        scan_deprecation_inventory,
        write_generated_architecture_artifact,
    )

    root, _scope_value = canonical_merge_repository
    for relative in (
        "config/architecture/devex_ownership_policy.yaml",
        "config/architecture/arch_004c_dependency_policy.yaml",
        "config/architecture/arch_004g_deprecation_policy.yaml",
        "inputs/architecture/arch_004c_direct_writer_baseline.yaml",
    ):
        target = root / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(ROOT / relative, target)
    source = root / "src/ai_trading_system/contracts/synthetic.py"
    source.parent.mkdir(parents=True)
    source.write_bytes(b"VALUE = 1\n")
    deprecation_path = root / "config/architecture/arch_004g_deprecation_policy.yaml"
    deprecation_policy = safe_load_yaml_path(deprecation_path)
    synthetic_target = copy.deepcopy(deprecation_policy["targets"][1])
    synthetic_target["surface_id"] = "synthetic_module"
    synthetic_target["path"] = "src/ai_trading_system/contracts/synthetic.py"
    deprecation_policy["targets"] = [synthetic_target]
    write_generated_architecture_artifact(deprecation_path, deprecation_policy)
    policy = root / "config/architecture/devex_ownership_policy.yaml"
    outputs = {
        "module": root / "inputs/architecture/arch_004e_module_manifest.yaml",
        "test": root / "inputs/architecture/arch_004e_test_manifest.yaml",
        "aggregate": root / "inputs/architecture/arch_004e_aggregate_shadow_index.yaml",
        "fitness": root / "inputs/architecture/arch_004e_architecture_fitness.yaml",
        "deprecation": root / "inputs/architecture/arch_004g_deprecation_inventory.yaml",
    }
    for name, builder in (
        ("module", build_module_manifest),
        ("test", build_test_manifest),
        ("aggregate", build_aggregate_shadow_index),
    ):
        write_generated_architecture_artifact(
            outputs[name], builder(project_root=root, policy_path=policy)
        )
    fitness = build_architecture_fitness(
        project_root=root,
        policy_path=policy,
        module_manifest_path=outputs["module"],
        test_manifest_path=outputs["test"],
        aggregate_index_path=outputs["aggregate"],
        dependency_policy_path=root / "config/architecture/arch_004c_dependency_policy.yaml",
        direct_writer_baseline_path=root
        / "inputs/architecture/arch_004c_direct_writer_baseline.yaml",
    )
    assert fitness["status"] == "PASS"
    write_generated_architecture_artifact(outputs["fitness"], fitness)
    inventory = scan_deprecation_inventory(
        load_deprecation_policy(root / "config/architecture/arch_004g_deprecation_policy.yaml"),
        project_root=root,
        architecture_fitness_path=outputs["fitness"],
    ).to_dict()
    write_generated_architecture_artifact(outputs["deprecation"], inventory)
    if mutation != "none":
        value = safe_load_yaml_path(outputs[mutation])
        value["unreviewed_stale_output"] = True
        write_generated_architecture_artifact(outputs[mutation], value)
    before = {name: path.read_bytes() for name, path in outputs.items()}
    if mutation == "none":
        result = integration.inspect_architecture_merge_outputs(root, TASK)
        assert result["status"] == "VALIDATED_ARCHITECTURE_INPUTS"
        assert result["output_paths"] == sorted(
            path.relative_to(root).as_posix() for path in outputs.values()
        )
        assert result["materialization_allowed"] is False
    else:
        with pytest.raises(contract.WorkflowContractError, match="ARCHITECTURE_.*STALE"):
            integration.inspect_architecture_merge_outputs(root, TASK)
    assert before == {name: path.read_bytes() for name, path in outputs.items()}


def test_working_object_uses_raw_bytes_and_index_mode_without_writing_object(
    small_repository: Path,
) -> None:
    root = small_repository
    path = root / "raw.bin"
    path.write_bytes(b"original")
    _git(root, "add", "--", "raw.bin")
    _git(root, "update-index", "--chmod=+x", "--", "raw.bin")
    content = b"new raw\r\n\0\xff"
    path.write_bytes(content)
    expected_oid = _git(root, "hash-object", "--no-filters", "--stdin", content=content)
    expected = {"exists": True, "mode": "100755", "type": "blob", "oid": expected_oid}
    assert integration.working_object(root, "raw.bin") == expected
    result = subprocess.run(
        ["git", "-C", str(root), "cat-file", "-e", expected_oid], capture_output=True
    )
    assert result.returncode != 0, "working-object observation unexpectedly wrote an object"
    assert integration.working_object(root, "absent.bin") == {
        "exists": False,
        "mode": None,
        "type": None,
        "oid": None,
    }


class _TreePlumbing:
    """Real Git transport for construction tests, not a source admission fixture."""

    @staticmethod
    def _git(root: Path, *args: str, content: bytes | None = None) -> bytes:
        return _git(root, *args, content=content).encode("utf-8")


@pytest.mark.parametrize("object_format", ["sha256", "sha1-compat"])
def test_source_tree_without_index_rejects_other_object_formats(
    tmp_path: Path, object_format: str
) -> None:
    root = tmp_path / "tree-object-format-fixture"
    root.mkdir()
    _git(root, "init", "--object-format=" + object_format.split("-")[0])
    if object_format == "sha1-compat":
        _git(root, "config", "core.repositoryformatversion", "1")
        _git(root, "config", "extensions.compatObjectFormat", "sha256")
        assert "extensions.compatobjectformat\nsha256" in _git(
            root, "config", "--no-includes", "--null", "--list"
        )
    before = {p.relative_to(root).as_posix() for p in (root / ".git/objects").rglob("*")}
    # Invalid main is intentional: format must be refused before even ls-tree,
    # not after object creation or a later independent snapshot check.
    with pytest.raises(contract.WorkflowContractError, match="SOURCE_TREE_OBJECT_FORMAT"):
        integration._source_tree_without_index(root, _TreePlumbing(), "unread-main", [])
    assert {p.relative_to(root).as_posix() for p in (root / ".git/objects").rglob("*")} == before


def _tree_operations(root: Path, changes: dict[str, tuple[str, bytes] | None]) -> list[dict]:
    operations = []
    for path, value in changes.items():
        after = {"exists": False, "mode": None, "type": None, "oid": None}
        if value is not None:
            mode, raw = value
            after = {
                "exists": True,
                "mode": mode,
                "type": "commit" if mode == "160000" else "blob",
                "oid": raw.decode("ascii")
                if mode == "160000"
                else _git(root, "hash-object", "-w", "--stdin", content=raw),
            }
        operations.append({"path": path, "after": after})
    return operations


def test_source_tree_without_index_matches_independent_git_tree(small_repository: Path) -> None:
    root = small_repository
    genesis = _git(root, "rev-parse", "HEAD")
    main = _tree_commit(
        root,
        genesis,
        {
            "a.c": ("100644", b"before\n"),
            "a/deep/old": ("100644", b"remove\n"),
            "becomes-directory": ("100644", b"old\n"),
            "becomes-file/old": ("100644", b"old\n"),
            "untouched": ("100755", b"retained\r\n\0\xff"),
        },
    )
    # Addition-first order deliberately differs from the necessary deletion-first
    # application; the oracle builds the same set using ordinary Git index tools.
    changes = {
        "becomes-file": ("100644", b"file\n"),
        "becomes-directory/new": ("100644", b"directory\n"),
        "a/new": ("100755", b"raw\r\n\0\xff"),
        "a.c": ("100755", b"changed\n"),
        "link": ("120000", b"a.c"),
        "submodule": ("160000", genesis.encode("ascii")),
        "日本語": ("100644", b""),
        "a/deep/old": None,
        "becomes-directory": None,
        "becomes-file/old": None,
    }
    oracle_changes = dict(sorted(changes.items(), key=lambda row: row[1] is not None))
    expected = _tree_commit(root, main, oracle_changes)
    operations = _tree_operations(root, changes)
    index_before = (root / ".git/index").read_bytes()
    refs_before = _git(root, "show-ref")
    files_before = {p.relative_to(root).as_posix() for p in root.rglob("*") if p.is_file()}
    actual = integration._source_tree_without_index(root, _TreePlumbing(), main, operations)
    assert actual == _git(root, "rev-parse", expected + "^{tree}")
    assert _git(root, "ls-tree", "-r", "-t", "-z", actual) == _git(
        root, "ls-tree", "-r", "-t", "-z", expected
    )
    assert (root / ".git/index").read_bytes() == index_before
    assert _git(root, "show-ref") == refs_before
    files_after = {p.relative_to(root).as_posix() for p in root.rglob("*") if p.is_file()}
    assert all(path.startswith(".git/objects/") for path in files_after - files_before)


def test_source_tree_without_index_preserves_empty_tree_and_unavailable_blobs(
    small_repository: Path,
) -> None:
    root = small_repository
    blob = _git(root, "hash-object", "-w", "--stdin", content=b"retained target\n")
    empty = _git(root, "mktree", content=b"")
    tree = _git(
        root,
        "mktree",
        content=(
            f"100644 blob {blob}\t.gitattributes\n"
            f"100644 blob {blob}\t.gitmodules\n"
            f"040000 tree {empty}\tempty\n"
            f"120000 blob {blob}\tlink\n"
        ).encode("ascii"),
    )
    main = _git(root, "commit-tree", tree, "-m", "retained metadata fixture")
    changes = {"added": ("100644", b"new\r\n\0\xff")}
    operations = _tree_operations(root, changes)
    expected = _git(
        root,
        "mktree",
        content=(
            f"100644 blob {blob}\t.gitattributes\n"
            f"100644 blob {blob}\t.gitmodules\n"
            f"100644 blob {operations[0]['after']['oid']}\tadded\n"
            f"040000 tree {empty}\tempty\n"
            f"120000 blob {blob}\tlink\n"
        ).encode("ascii"),
    )
    stored = root / ".git/objects" / blob[:2] / blob[2:]
    preserved = root / "retained-blob-object"
    stored.rename(preserved)
    try:
        unavailable = subprocess.run(
            ["git", "-C", str(root), "cat-file", "-e", blob], capture_output=True
        )
        assert unavailable.returncode != 0
        actual = integration._source_tree_without_index(root, _TreePlumbing(), main, operations)
        assert actual == expected
        assert _git(root, "ls-tree", "-r", "-t", "-z", actual) == _git(
            root, "ls-tree", "-r", "-t", "-z", expected
        )
        assert _git(root, "ls-tree", actual, "--", "empty") == f"040000 tree {empty}\tempty"
        assert not stored.exists(), "construction unexpectedly restored/read a retained blob"
    finally:
        preserved.rename(stored)


@pytest.mark.parametrize(
    "mode,kind,oid", [("040000", "blob", "1" * 40), ("100644", "blob", "0" * 40)]
)
def test_source_tree_without_index_rejects_invalid_entries_before_tree_write(
    small_repository: Path, mode: str, kind: str, oid: str
) -> None:
    root = small_repository
    main = _git(root, "rev-parse", "HEAD")
    before = {p.relative_to(root).as_posix() for p in (root / ".git/objects").rglob("*")}
    with pytest.raises(contract.WorkflowContractError, match="SOURCE_TREE_ENTRY_OBJECT"):
        integration._source_tree_without_index(
            root,
            _TreePlumbing(),
            main,
            [
                {
                    "path": "invalid",
                    "after": {"exists": True, "mode": mode, "type": kind, "oid": oid},
                }
            ],
        )
    assert {p.relative_to(root).as_posix() for p in (root / ".git/objects").rglob("*")} == before
