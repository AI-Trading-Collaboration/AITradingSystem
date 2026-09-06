"""Synthetic loader tests, not canonical DQ, filesystem, or verified-seal proof.

Each fresh child executes the real bootstrap committed inside its own short-lived
pytest toy Git repository.  The package initialization is real fixture code, but
the context, contained reader, and worker below are deliberately non-authoritative
stubs.  No market inputs, actual DQ, capability, or production seal are created.
The coordinator separately owns the real committed 55-module end-to-end tests.
"""

from __future__ import annotations

import hashlib
import importlib.util
import json
import os
import py_compile
import shutil
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
BOOTSTRAP_PATH = "scripts/run_named_data_quality.py"
MANIFEST_PATH = "config/data_governance/toy_named_sources.json"
POLICY_PATH = "config/toy_policy.yaml"
LEAF_PATH = "src/ai_trading_system/data/toy_leaf.py"
WORKER_PATH = "src/ai_trading_system/data/named_quality_execution.py"
CONTEXT_PATH = "src/ai_trading_system/contracts/named_execution_context.py"

# These stubs intentionally have no authority.  They only expose the bootstrap's
# calls so tests do not confuse successful loading with actual context/seal proof.
TOY_CONTEXT = """\
_active = None

class GitCompiledModuleBinding:
    @classmethod
    def from_dict(cls, value):
        return dict(value)

class NamedExecutionIdentity:
    def __init__(self, **values):
        self.__dict__.update(values)

def _initialize_named_execution_context(identity):
    global _active
    assert _active is None
    _active = identity
    return identity

def _freeze_named_execution_context(context, *, loaded_modules):
    assert context is _active
    assert tuple(loaded_modules) == context.modules

def require_named_execution_context():
    assert _active is not None
    return _active

def close_named_execution_context(context):
    global _active
    assert context is _active
    _active = None
"""

TOY_READER = """\
# NOT a production contained reader: this tests loader orchestration only.
def read_contained_artifact_bytes(*, root, relative_path):
    return (root / relative_path).read_bytes()
"""

TOY_WORKER = """\
from pathlib import Path
import subprocess
import sys
import ai_trading_system
from ai_trading_system.data import toy_leaf

def bootstrap_worker(
    request, *, operation, receipt_path, receipt_sha256,
    run_dispatch_path, run_dispatch_sha256, bootstrap
):
    case = request.get("toy_case", "normal")
    root = Path(bootstrap.root)
    if case == "dispatch_without_precheck":
        bootstrap.note_canonical_dq_dispatch()
    if case == "source_before_dq":
        (root / "src/ai_trading_system/data/toy_leaf.py").write_bytes(b'VALUE = "B"\\n')
    if case == "policy_before_dq":
        (root / "config/toy_policy.yaml").write_bytes(b'policy: drifted\\n')
    if operation == "run" and case != "no_dispatch":
        bootstrap.assert_execution_unchanged(stage="PRE_DQ")
        bootstrap.note_canonical_dq_dispatch()
    if case in {"dispatch_twice", "dispatch_during_verify"}:
        bootstrap.note_canonical_dq_dispatch()
    if case == "source_before_terminal":
        (root / "src/ai_trading_system/data/toy_leaf.py").write_bytes(b'VALUE = "B"\\n')
    if case == "policy_before_terminal":
        (root / "config/toy_policy.yaml").write_bytes(b'policy: drifted\\n')
    if case == "head_before_terminal":
        subprocess.run([
            str(bootstrap.git.executable), "-c", "safe.directory=" + root.as_posix(),
            "-C", str(root), "update-ref", "HEAD", request["toy_previous_commit"],
        ], env=bootstrap.git.environment, check=True, capture_output=True)
    return {
        "schema_version": "toy_loader_observation.v1",
        "status": "TOY_LOADER_ONLY",
        "canonical_dq_call_count": bootstrap.canonical_dq_call_count,
        "actual_canonical_dq_calls": 0,
        "real_verified_seal_created": False,
        "package_initializers": ai_trading_system.PACKAGE_INITIALIZERS,
        "value": toy_leaf.value(),
        "leaf_file": toy_leaf.__file__,
        "leaf_origin": toy_leaf.__spec__.origin,
        "leaf_code_filename": toy_leaf.value.__code__.co_filename,
        "leaf_cached": toy_leaf.__cached__,
        "loaded_modules": sorted(bootstrap.loader.loaded),
        "source_bindings": list(bootstrap.context.modules),
        "git_environment": {
            key: value for key, value in bootstrap.git.environment.items()
            if key.startswith("GIT_")
        },
        "isolated": bool(sys.flags.isolated),
        "dont_write_bytecode": bool(sys.flags.dont_write_bytecode),
    }
"""


def _sha(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def _json_bytes(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")


def _clean_git_environment() -> dict[str, str]:
    result = {key: value for key, value in os.environ.items() if not key.upper().startswith("GIT_")}
    result.update(
        GIT_CONFIG_NOSYSTEM="1",
        GIT_CONFIG_GLOBAL=os.devnull,
        GIT_CONFIG_SYSTEM=os.devnull,
        GIT_TERMINAL_PROMPT="0",
    )
    return result


@dataclass
class ToyRepository:
    root: Path
    git_executable: str
    manifest: dict[str, Any]
    candidate: str = ""
    previous_commit: str = ""

    def write(self, relative: str, content: bytes) -> Path:
        path = self.root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)
        return path

    def git(self, *arguments: str, input_bytes: bytes | None = None) -> bytes:
        result = subprocess.run(
            [
                self.git_executable,
                "-c",
                f"safe.directory={self.root.as_posix()}",
                "-C",
                str(self.root),
                *arguments,
            ],
            input=input_bytes,
            env=_clean_git_environment(),
            capture_output=True,
            check=False,
        )
        assert result.returncode == 0, result.stderr.decode("utf-8", errors="replace")
        return result.stdout

    def commit(self, *, stage: bool = True) -> None:
        self.previous_commit = self.candidate
        if stage:
            self.git("add", "--all", "--", ".")
        self.git("commit", "--allow-empty", "-m", "synthetic bootstrap fixture")
        self.candidate = self.git("rev-parse", "HEAD").decode("ascii").strip()

    def commit_manifest(self) -> None:
        self.write(MANIFEST_PATH, _json_bytes(self.manifest))
        self.commit()

    def request(self, **updates: Any) -> dict[str, Any]:
        result: dict[str, Any] = {
            "roots": {"execution_root": self.root.as_posix()},
            "candidate_commit": self.candidate,
            "source_manifest_path": MANIFEST_PATH,
            "source_manifest_sha256": _sha((self.root / MANIFEST_PATH).read_bytes()),
            "policy_path": POLICY_PATH,
            "toy_previous_commit": self.previous_commit,
        }
        result.update(updates)
        return result

    def run(
        self,
        *,
        request: dict[str, Any] | None = None,
        request_bytes: bytes | None = None,
        request_sha256: str | None = None,
        operation: str = "run",
        environment: dict[str, str] | None = None,
        preimport: bool = False,
        isolated: bool = True,
        script_path: Path | None = None,
        extra_arguments: tuple[str, ...] = (),
    ) -> tuple[subprocess.CompletedProcess[bytes], dict[str, Any]]:
        content = (
            request_bytes if request_bytes is not None else _json_bytes(request or self.request())
        )
        # Request output is outside the toy execution root; it is not a policy,
        # market cache, report receipt, or proof of real execution permission.
        request_path = self.root.parent / f"{self.root.name}-loader-request.json"
        request_path.write_bytes(content)
        arguments = [sys.executable, *(["-I"] if isolated else []), "-B"]
        bootstrap = script_path or self.root / BOOTSTRAP_PATH
        if preimport:
            arguments.extend(
                [
                    "-c",
                    "import runpy,sys,types;"
                    "sys.modules['ai_trading_system']=types.ModuleType('ai_trading_system');"
                    "sys.argv=sys.argv[1:];runpy.run_path(sys.argv[0],run_name='__main__')",
                ]
            )
        arguments.extend(
            [
                str(bootstrap),
                "--request",
                str(request_path),
                "--request-sha256",
                request_sha256 if request_sha256 is not None else _sha(content),
                "--operation",
                operation,
                "--source-lease-id",
                "lease-00000000000000000000",
            ]
        )
        if operation == "verify":
            arguments.extend(["--receipt-path", "toy/receipt.json", "--receipt-sha256", "a" * 64])
            arguments.extend(
                ["--run-dispatch-path", "toy/dispatch.json", "--run-dispatch-sha256", "b" * 64]
            )
        arguments.extend(extra_arguments)
        result = subprocess.run(
            arguments,
            env=environment,
            capture_output=True,
            check=False,
            timeout=45,
        )
        lines = result.stdout.decode("utf-8").splitlines()
        assert lines, result.stderr.decode("utf-8", errors="replace")
        payload = json.loads(lines[-1])
        return result, payload


@pytest.fixture
def toy_repository(tmp_path: Path) -> ToyRepository:
    git_executable = shutil.which("git")
    assert git_executable is not None, "Reviewed named execution requires local Git; do not skip."
    root = tmp_path / "named-loader-toy"
    root.mkdir()
    fixture = ToyRepository(root=root, git_executable=git_executable, manifest={})
    fixture.git("init", "--initial-branch=main")
    fixture.git("config", "user.name", "Named loader synthetic fixture")
    fixture.git("config", "user.email", "named-loader-fixture@example.invalid")
    fixture.git("config", "core.autocrlf", "false")
    fixture.git("config", "commit.gpgsign", "false")
    fixture.write(BOOTSTRAP_PATH, (REPOSITORY_ROOT / BOOTSTRAP_PATH).read_bytes())
    fixture.write(POLICY_PATH, b"policy: synthetic-loader-only\n")
    sources = {
        "ai_trading_system": ("PACKAGE_INITIALIZERS = ['root']\n", True),
        "ai_trading_system.contracts": (
            "import ai_trading_system\n"
            "ai_trading_system.PACKAGE_INITIALIZERS.append('contracts')\n",
            True,
        ),
        "ai_trading_system.contracts.named_execution_context": (TOY_CONTEXT, False),
        "ai_trading_system.data": (
            "import ai_trading_system\nai_trading_system.PACKAGE_INITIALIZERS.append('data')\n",
            True,
        ),
        "ai_trading_system.data.immutable_publish": (TOY_READER, False),
        "ai_trading_system.data.named_quality_execution": (TOY_WORKER, False),
        "ai_trading_system.data.toy_leaf": (
            'VALUE = "GIT_A"\ndef value():\n    return VALUE\n',
            False,
        ),
    }
    rows = []
    for name, (source, is_package) in sorted(sources.items()):
        relative = "src/" + name.replace(".", "/") + ("/__init__.py" if is_package else ".py")
        fixture.write(relative, source.encode("utf-8"))
        rows.append({"module_name": name, "source_path": relative, "is_package": is_package})
    fixture.manifest = {
        "schema_version": "named_data_quality_execution_sources.v1",
        "source_kind": "GIT_COMMIT_BYTES_COMPILED",
        "bootstrap_path": BOOTSTRAP_PATH,
        "worker_entrypoint": "ai_trading_system.data.named_quality_execution:bootstrap_worker",
        "modules": rows,
        "policy_dependencies": [POLICY_PATH],
    }
    fixture.commit_manifest()
    fixture.commit()
    return fixture


def _assert_blocked(
    observed: tuple[subprocess.CompletedProcess[bytes], dict[str, Any]],
    code: str,
    *,
    dispatches: int = 0,
) -> dict[str, Any]:
    process, payload = observed
    assert process.returncode == 2, (payload, process.stderr.decode("utf-8", errors="replace"))
    assert payload["status"] == "BLOCKED"
    assert payload["reason_code"] == code, payload
    assert payload["canonical_dq_call_count"] == dispatches
    assert payload["dispatch_allowed"] is False
    assert payload["production_effect"] == payload["broker_action"] == "none"
    return payload


def _assert_toy_observation(
    observed: tuple[subprocess.CompletedProcess[bytes], dict[str, Any]],
    *,
    dispatches: int = 1,
) -> dict[str, Any]:
    process, payload = observed
    assert process.returncode == 0, (payload, process.stderr.decode("utf-8", errors="replace"))
    assert payload["status"] == "TOY_LOADER_ONLY"
    assert payload["actual_canonical_dq_calls"] == 0
    assert payload["real_verified_seal_created"] is False
    assert payload["canonical_dq_call_count"] == dispatches
    return payload


def test_fresh_child_executes_committed_bytes_and_original_package_initialization(
    toy_repository: ToyRepository,
) -> None:
    result = _assert_toy_observation(toy_repository.run())
    assert result["package_initializers"] == ["root", "contracts", "data"]
    assert result["value"] == "GIT_A"
    leaf_path = str(toy_repository.root / LEAF_PATH)
    assert result["leaf_file"] == result["leaf_origin"] == result["leaf_code_filename"] == leaf_path
    assert result["leaf_cached"] is None
    assert result["isolated"] is result["dont_write_bytecode"] is True
    assert result["loaded_modules"] == sorted(
        row["module_name"] for row in toy_repository.manifest["modules"]
    )
    binding = next(row for row in result["source_bindings"] if row["source_path"] == LEAF_PATH)
    assert binding["sha256"] == _sha((toy_repository.root / LEAF_PATH).read_bytes())
    assert (
        binding["git_blob_id"]
        == toy_repository.git("rev-parse", f"HEAD:{LEAF_PATH}").decode("ascii").strip()
    )
    assert not list(toy_repository.root.rglob("*.pyc"))


def test_exact_linked_worktree_with_git_file_is_supported(toy_repository: ToyRepository) -> None:
    linked = toy_repository.root.parent / "linked-loader-toy"
    toy_repository.git("worktree", "add", "--detach", str(linked), toy_repository.candidate)
    assert (linked / ".git").is_file()
    linked_fixture = ToyRepository(
        root=linked,
        git_executable=toy_repository.git_executable,
        manifest=toy_repository.manifest,
        candidate=toy_repository.candidate,
        previous_commit=toy_repository.previous_commit,
    )
    result = _assert_toy_observation(linked_fixture.run())
    assert result["leaf_file"] == str(linked / LEAF_PATH)


def test_preimported_project_is_rejected_before_worker(toy_repository: ToyRepository) -> None:
    _assert_blocked(toy_repository.run(preimport=True), "NAMED_BOOTSTRAP_PROJECT_PREIMPORTED")


def test_nonisolated_entry_is_rejected(toy_repository: ToyRepository) -> None:
    _assert_blocked(toy_repository.run(isolated=False), "NAMED_BOOTSTRAP_ISOLATED_CHILD_REQUIRED")


def test_missing_git_executable_does_not_fall_back_to_worktree_imports(
    toy_repository: ToyRepository,
) -> None:
    environment = os.environ.copy()
    environment["PATH"] = ""
    _assert_blocked(toy_repository.run(environment=environment), "NAMED_BOOTSTRAP_GIT_REQUIRED")


def test_unchecked_hash_pyc_cannot_replace_committed_source(toy_repository: ToyRepository) -> None:
    evil_source = toy_repository.write(
        "toy_pyc_payload.py", b'def value():\n    return "PYC_ATTACK"\n'
    )
    pyc_path = Path(importlib.util.cache_from_source(str(toy_repository.root / LEAF_PATH)))
    pyc_path.parent.mkdir(parents=True)
    py_compile.compile(
        str(evil_source),
        cfile=str(pyc_path),
        dfile=str(toy_repository.root / LEAF_PATH),
        doraise=True,
        invalidation_mode=py_compile.PycInvalidationMode.UNCHECKED_HASH,
    )
    stale_bytes = pyc_path.read_bytes()
    assert int.from_bytes(stale_bytes[4:8], "little") == 1
    result = _assert_toy_observation(toy_repository.run())
    assert result["value"] == "GIT_A"
    assert pyc_path.read_bytes() == stale_bytes


def test_unknown_project_import_does_not_fall_back_to_disk(toy_repository: ToyRepository) -> None:
    toy_repository.write("src/ai_trading_system/unreviewed.py", b"VALUE = 'must-not-load'\n")
    worker = toy_repository.root / WORKER_PATH
    worker.write_bytes(worker.read_bytes() + b"\nimport ai_trading_system.unreviewed\n")
    toy_repository.commit()
    _assert_blocked(toy_repository.run(), "NAMED_BOOTSTRAP_UNREVIEWED_IMPORT")


def test_ambient_git_target_and_configuration_are_not_inherited(
    toy_repository: ToyRepository,
) -> None:
    environment = os.environ.copy()
    environment.update(
        GIT_DIR=str(toy_repository.root.parent / "wrong-git-dir"),
        GIT_WORK_TREE=str(toy_repository.root.parent),
        GIT_INDEX_FILE=str(toy_repository.root.parent / "wrong-index"),
        GIT_OBJECT_DIRECTORY=str(toy_repository.root.parent / "wrong-objects"),
        GIT_CONFIG_COUNT="1",
        GIT_CONFIG_KEY_0="core.repositoryformatversion",
        GIT_CONFIG_VALUE_0="999",
        GIT_CONFIG_GLOBAL=str(toy_repository.root.parent / "absent-global-config"),
        GIT_NO_REPLACE_OBJECTS="0",
        GIT_NO_LAZY_FETCH="0",
    )
    result = _assert_toy_observation(toy_repository.run(environment=environment))
    inherited = result["git_environment"]
    assert not set(inherited) & {
        "GIT_DIR",
        "GIT_WORK_TREE",
        "GIT_INDEX_FILE",
        "GIT_OBJECT_DIRECTORY",
        "GIT_CONFIG_COUNT",
        "GIT_CONFIG_KEY_0",
        "GIT_CONFIG_VALUE_0",
    }
    assert inherited["GIT_NO_REPLACE_OBJECTS"] == inherited["GIT_NO_LAZY_FETCH"] == "1"
    assert inherited["GIT_CONFIG_GLOBAL"] == inherited["GIT_CONFIG_SYSTEM"] == os.devnull
    assert inherited["GIT_TERMINAL_PROMPT"] == inherited["GIT_OPTIONAL_LOCKS"] == "0"


def test_git_replace_blob_cannot_change_compiled_bytes(toy_repository: ToyRepository) -> None:
    original = toy_repository.git("rev-parse", f"HEAD:{LEAF_PATH}").decode("ascii").strip()
    attack = b'def value():\n    return "GIT_REPLACE_ATTACK"\n'
    replacement = (
        toy_repository.git("hash-object", "-w", "--stdin", input_bytes=attack)
        .decode("ascii")
        .strip()
    )
    toy_repository.git("replace", original, replacement)
    assert toy_repository.git("cat-file", "blob", original) == attack
    result = _assert_toy_observation(toy_repository.run())
    assert result["value"] == "GIT_A"


@pytest.mark.parametrize("promisor", [False, True])
def test_missing_local_blob_fails_closed_without_remote_materialization(
    toy_repository: ToyRepository, promisor: bool
) -> None:
    blob = toy_repository.git("rev-parse", f"HEAD:{LEAF_PATH}").decode("ascii").strip()
    # All objects in this newly initialized, unpacked toy repository are local.
    # This deletes exactly one fixture blob, never a project/shared Git object.
    object_path = toy_repository.root / ".git" / "objects" / blob[:2] / blob[2:]
    assert object_path.is_file()
    object_path.chmod(0o600)
    object_path.unlink()
    if promisor:
        toy_repository.git("config", "extensions.partialClone", "origin")
        toy_repository.git("config", "remote.origin.promisor", "true")
        # A nonexistent local fixture path: no network URL or provider is used.
        toy_repository.git(
            "config", "remote.origin.url", str(toy_repository.root.parent / "absent-local-remote")
        )
    _assert_blocked(toy_repository.run(), "NAMED_BOOTSTRAP_LOCAL_GIT_FAILED")
    assert not object_path.exists()
    assert not (toy_repository.root / ".git" / "FETCH_HEAD").exists()


@pytest.mark.parametrize("mode", ["120000", "160000"])
def test_git_symlink_or_gitlink_entry_is_rejected_without_following_it(
    toy_repository: ToyRepository, mode: str
) -> None:
    identity = toy_repository.candidate
    if mode == "120000":
        identity = (
            toy_repository.git("hash-object", "-w", "--stdin", input_bytes=b"../outside-target")
            .decode("ascii")
            .strip()
        )
    toy_repository.git("update-index", "--cacheinfo", f"{mode},{identity},{LEAF_PATH}")
    toy_repository.commit(stage=False)
    assert (toy_repository.root / LEAF_PATH).is_file()
    _assert_blocked(toy_repository.run(), "NAMED_BOOTSTRAP_GIT_ENTRY_INVALID")


def test_declared_but_uncommitted_source_does_not_fall_back_to_disk(
    toy_repository: ToyRepository,
) -> None:
    toy_repository.git("rm", "--cached", "--", LEAF_PATH)
    toy_repository.commit(stage=False)
    assert (toy_repository.root / LEAF_PATH).is_file()
    _assert_blocked(toy_repository.run(), "NAMED_BOOTSTRAP_GIT_OBJECT_MISSING")


@pytest.mark.parametrize("relative", [LEAF_PATH, POLICY_PATH, BOOTSTRAP_PATH])
def test_preimport_disk_drift_is_blocked(toy_repository: ToyRepository, relative: str) -> None:
    path = toy_repository.root / relative
    path.write_bytes(path.read_bytes() + b"\n# disk drift\n")
    _assert_blocked(toy_repository.run(), "NAMED_BOOTSTRAP_DISK_SOURCE_DRIFT")


@pytest.mark.parametrize(
    ("case", "dispatches", "stage"),
    [
        ("source_before_dq", 0, "PRE_DQ"),
        ("policy_before_dq", 0, "PRE_DQ"),
        ("source_before_terminal", 1, "TERMINAL"),
        ("policy_before_terminal", 1, "TERMINAL"),
    ],
)
def test_loaded_a_then_disk_b_is_blocked_at_the_required_boundary(
    toy_repository: ToyRepository, case: str, dispatches: int, stage: str
) -> None:
    payload = _assert_blocked(
        toy_repository.run(request=toy_repository.request(toy_case=case)),
        "NAMED_BOOTSTRAP_DISK_SOURCE_DRIFT",
        dispatches=dispatches,
    )
    assert stage in payload["detail"]


def test_candidate_mismatch_is_blocked_before_import(toy_repository: ToyRepository) -> None:
    _assert_blocked(
        toy_repository.run(
            request=toy_repository.request(candidate_commit=toy_repository.previous_commit)
        ),
        "NAMED_BOOTSTRAP_CANDIDATE_MISMATCH",
    )


def test_candidate_moved_after_dispatch_is_blocked(toy_repository: ToyRepository) -> None:
    _assert_blocked(
        toy_repository.run(request=toy_repository.request(toy_case="head_before_terminal")),
        "NAMED_BOOTSTRAP_CANDIDATE_MISMATCH",
        dispatches=1,
    )


def test_bootstrap_cannot_be_executed_from_a_copied_root(toy_repository: ToyRepository) -> None:
    copied = toy_repository.root.parent / "copied_bootstrap.py"
    copied.write_bytes((toy_repository.root / BOOTSTRAP_PATH).read_bytes())
    _assert_blocked(toy_repository.run(script_path=copied), "NAMED_BOOTSTRAP_ENTRY_ROOT_MISMATCH")


def test_hardlinked_source_is_rejected(toy_repository: ToyRepository) -> None:
    os.link(toy_repository.root / LEAF_PATH, toy_repository.root / "hardlink-to-leaf.py")
    _assert_blocked(toy_repository.run(), "NAMED_BOOTSTRAP_SOURCE_NOT_REGULAR")


@pytest.mark.parametrize("root_value", ["relative-root", "{root}/../named-loader-toy"])
def test_noncanonical_execution_root_is_rejected(
    toy_repository: ToyRepository, root_value: str
) -> None:
    value = root_value.format(root=toy_repository.root.as_posix())
    _assert_blocked(
        toy_repository.run(request=toy_repository.request(roots={"execution_root": value})),
        "NAMED_BOOTSTRAP_ROOT_INVALID",
    )


@pytest.mark.parametrize(
    ("claimed_sha", "code"),
    [
        ("0" * 64, "NAMED_BOOTSTRAP_REQUEST_SHA_MISMATCH"),
        ("not-a-sha", "NAMED_BOOTSTRAP_ID_INVALID"),
    ],
)
def test_request_exact_bytes_hash_is_required(
    toy_repository: ToyRepository, claimed_sha: str, code: str
) -> None:
    _assert_blocked(toy_repository.run(request_sha256=claimed_sha), code)


def test_duplicate_request_json_key_is_rejected(toy_repository: ToyRepository) -> None:
    _assert_blocked(
        toy_repository.run(request_bytes=b'{"candidate_commit":"a","candidate_commit":"b"}'),
        "NAMED_BOOTSTRAP_JSON_DUPLICATE",
    )


@pytest.mark.parametrize("content", [b'{"value":NaN}', b"[]"])
def test_nonstandard_or_nonobject_request_json_is_rejected(
    toy_repository: ToyRepository, content: bytes
) -> None:
    _assert_blocked(toy_repository.run(request_bytes=content), "NAMED_BOOTSTRAP_JSON_INVALID")


def test_source_manifest_exact_sha_is_required(toy_repository: ToyRepository) -> None:
    _assert_blocked(
        toy_repository.run(request=toy_repository.request(source_manifest_sha256="0" * 64)),
        "NAMED_BOOTSTRAP_MANIFEST_SHA_MISMATCH",
    )


@pytest.mark.parametrize(
    ("field", "value", "code"),
    [
        ("extra", True, "NAMED_BOOTSTRAP_MANIFEST_SCHEMA"),
        ("schema_version", "legacy.v0", "NAMED_BOOTSTRAP_MANIFEST_SCHEMA"),
        ("source_kind", "WORKTREE_FILE_BYTES", "NAMED_BOOTSTRAP_MANIFEST_SCOPE"),
        ("worker_entrypoint", "arbitrary.module:run", "NAMED_BOOTSTRAP_MANIFEST_SCOPE"),
        ("bootstrap_path", "scripts/unreviewed.py", "NAMED_BOOTSTRAP_MANIFEST_SCOPE"),
    ],
)
def test_manifest_fields_are_strict_and_fixed_scope(
    toy_repository: ToyRepository, field: str, value: object, code: str
) -> None:
    toy_repository.manifest[field] = value
    toy_repository.commit_manifest()
    _assert_blocked(toy_repository.run(), code)


@pytest.mark.parametrize(
    ("case", "code"),
    [
        ("empty", "NAMED_BOOTSTRAP_MODULE_SET_INVALID"),
        ("duplicate", "NAMED_BOOTSTRAP_MODULE_SET_INVALID"),
        ("extra-field", "NAMED_BOOTSTRAP_MODULE_SET_INVALID"),
        ("wrong-module-root", "NAMED_BOOTSTRAP_MODULE_SET_INVALID"),
        ("nonbool-package", "NAMED_BOOTSTRAP_MODULE_SET_INVALID"),
        ("wrong-source", "NAMED_BOOTSTRAP_MODULE_SET_INVALID"),
        ("traversal", "NAMED_BOOTSTRAP_PATH_INVALID"),
        ("missing-parent", "NAMED_BOOTSTRAP_PACKAGE_MISSING"),
        ("missing-worker", "NAMED_BOOTSTRAP_MODULE_SET_INVALID"),
    ],
)
def test_manifest_module_closure_rows_fail_closed(
    toy_repository: ToyRepository, case: str, code: str
) -> None:
    rows = toy_repository.manifest["modules"]
    if case == "empty":
        rows.clear()
    elif case == "duplicate":
        rows.append(dict(rows[-1]))
    elif case == "extra-field":
        rows[-1]["approved"] = True
    elif case == "wrong-module-root":
        rows[-1]["module_name"] = "unreviewed.module"
    elif case == "nonbool-package":
        rows[-1]["is_package"] = 0
    elif case == "wrong-source":
        rows[-1]["source_path"] = WORKER_PATH
    elif case == "traversal":
        rows[-1]["source_path"] = "src/../outside.py"
    elif case == "missing-parent":
        rows[:] = [row for row in rows if row["module_name"] != "ai_trading_system.contracts"]
    elif case == "missing-worker":
        rows[:] = [row for row in rows if row["source_path"] != WORKER_PATH]
    else:
        raise AssertionError(case)
    toy_repository.commit_manifest()
    _assert_blocked(toy_repository.run(), code)


@pytest.mark.parametrize(
    "dependencies", [[], [POLICY_PATH, POLICY_PATH], ["outputs/toy_policy.yaml"]]
)
def test_policy_dependencies_are_explicit_unique_and_narrow(
    toy_repository: ToyRepository, dependencies: list[str]
) -> None:
    toy_repository.manifest["policy_dependencies"] = dependencies
    toy_repository.commit_manifest()
    _assert_blocked(toy_repository.run(), "NAMED_BOOTSTRAP_DEPENDENCIES_INVALID")


def test_request_policy_cannot_escape_reviewed_dependency_set(
    toy_repository: ToyRepository,
) -> None:
    _assert_blocked(
        toy_repository.run(request=toy_repository.request(policy_path="config/not-declared.yaml")),
        "NAMED_BOOTSTRAP_POLICY_NOT_DECLARED",
    )


def test_duplicate_manifest_json_key_is_rejected(toy_repository: ToyRepository) -> None:
    content = _json_bytes(toy_repository.manifest)
    toy_repository.write(MANIFEST_PATH, b'{"schema_version":"duplicate",' + content[1:])
    toy_repository.commit()
    _assert_blocked(toy_repository.run(), "NAMED_BOOTSTRAP_JSON_DUPLICATE")


def test_verify_child_does_not_record_a_dq_dispatch(toy_repository: ToyRepository) -> None:
    _assert_toy_observation(toy_repository.run(operation="verify"), dispatches=0)


def test_run_cannot_return_success_without_its_single_dispatch(
    toy_repository: ToyRepository,
) -> None:
    _assert_blocked(
        toy_repository.run(request=toy_repository.request(toy_case="no_dispatch")),
        "NAMED_BOOTSTRAP_DQ_CALL_COUNT_MISMATCH",
    )


@pytest.mark.parametrize(
    ("case", "operation", "code", "dispatches"),
    [
        ("dispatch_without_precheck", "run", "NAMED_BOOTSTRAP_DQ_DISPATCH_FORBIDDEN", 0),
        ("dispatch_twice", "run", "NAMED_BOOTSTRAP_DQ_DISPATCH_REPEATED", 1),
        ("dispatch_during_verify", "verify", "NAMED_BOOTSTRAP_DQ_DISPATCH_FORBIDDEN", 0),
    ],
)
def test_dispatch_count_cannot_bypass_precheck_or_operation_boundary(
    toy_repository: ToyRepository, case: str, operation: str, code: str, dispatches: int
) -> None:
    _assert_blocked(
        toy_repository.run(request=toy_repository.request(toy_case=case), operation=operation),
        code,
        dispatches=dispatches,
    )


def test_run_rejects_receipt_sha_without_receipt_path(toy_repository: ToyRepository) -> None:
    _assert_blocked(
        toy_repository.run(extra_arguments=("--receipt-sha256", "a" * 64)),
        "NAMED_BOOTSTRAP_RECEIPT_ARGUMENT_INVALID",
    )
