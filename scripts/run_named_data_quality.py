"""Fixed stdlib-first entry for the named canonical DQ child.

Run with the reviewed interpreter's ``-I -B`` flags.  Project modules are compiled
from one local Git commit, never from project pyc.  This is not a general sandbox
or an independent grant to run research.  See TRADING-2564 S2b sections 5--7.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib
import importlib.abc
import importlib.util
import json
import os
import re
import shutil
import stat
import subprocess
import sys
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from types import CodeType, ModuleType
from typing import Any, NoReturn

SOURCE_SCHEMA = "named_data_quality_execution_sources.v1"
BOOTSTRAP_PATH = "scripts/run_named_data_quality.py"
WORKER_MODULE = "ai_trading_system.data.named_quality_execution"
CONTEXT_MODULE = "ai_trading_system.contracts.named_execution_context"
# A protocol invariant: one runner invocation cannot silently dispatch DQ twice.
CANONICAL_DQ_DISPATCH_MAXIMUM = 1


class NamedBootstrapError(ValueError):
    def __init__(self, code: str, message: str) -> None:
        self.code = code
        super().__init__(f"{code}: {message}")


def _fail(code: str, message: str) -> NoReturn:
    raise NamedBootstrapError(code, message)


def _sha(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def _text(value: object, label: str) -> str:
    if not isinstance(value, str) or not value or value != value.strip():
        _fail("NAMED_BOOTSTRAP_FIELD_INVALID", label)
    assert isinstance(value, str)
    return value


def _digest(value: object, label: str, *, git: bool = False) -> str:
    result = _text(value, label)
    pattern = r"[0-9a-f]{40}" if git else r"[0-9a-f]{64}"
    if re.fullmatch(pattern, result) is None:
        _fail("NAMED_BOOTSTRAP_ID_INVALID", label)
    return result


def _relative(value: object) -> str:
    result = _text(value, "relative_path")
    parsed = PurePosixPath(result)
    if (
        parsed.is_absolute()
        or parsed.as_posix() != result
        or "\\" in result
        or ":" in result
        or any(part in {"", ".", ".."} for part in result.split("/"))
    ):
        _fail("NAMED_BOOTSTRAP_PATH_INVALID", result)
    return result


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            _fail("NAMED_BOOTSTRAP_JSON_DUPLICATE", key)
        result[key] = value
    return result


def _json_object(content: bytes) -> dict[str, Any]:
    def reject_constant(value: str) -> None:
        _fail("NAMED_BOOTSTRAP_JSON_INVALID", value)

    result = json.loads(
        content.decode("utf-8"),
        object_pairs_hook=_unique_object,
        parse_constant=reject_constant,
    )
    if not isinstance(result, dict):
        _fail("NAMED_BOOTSTRAP_JSON_INVALID", "object required")
    return result


def _check_path_component(path: Path, *, directory: bool) -> os.stat_result:
    metadata = path.lstat()
    reparse = getattr(metadata, "st_file_attributes", 0) & getattr(
        stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400
    )
    if stat.S_ISLNK(metadata.st_mode) or reparse:
        _fail("NAMED_BOOTSTRAP_LINK_FORBIDDEN", str(path))
    if directory:
        if not stat.S_ISDIR(metadata.st_mode):
            _fail("NAMED_BOOTSTRAP_ROOT_INVALID", str(path))
    elif not stat.S_ISREG(metadata.st_mode) or metadata.st_nlink != 1:
        _fail("NAMED_BOOTSTRAP_SOURCE_NOT_REGULAR", str(path))
    return metadata


def _absolute_directory(value: object) -> Path:
    raw = _text(value, "execution_root")
    path = Path(raw)
    if not path.is_absolute() or ".." in path.parts:
        _fail("NAMED_BOOTSTRAP_ROOT_INVALID", raw)
    for item in reversed((path, *path.parents)):
        _check_path_component(item, directory=True)
    return path


def _initial_file_bytes(root: Path, relative: str) -> bytes:
    """Narrow pre-import check, not a replacement for the contained reader.

    The coordinator's checkout/reparse audit and cooperative lease remain
    prerequisites.  Once project code is loaded, all repeated reads use the
    existing descriptor-bound contained reader.
    """

    path = root.joinpath(*PurePosixPath(_relative(relative)).parts)
    for parent in reversed(path.parent.relative_to(root).parents):
        _check_path_component(root / parent, directory=True)
    _check_path_component(path.parent, directory=True)
    before = _check_path_component(path, directory=False)
    with path.open("rb") as handle:
        opened = os.fstat(handle.fileno())
        content = handle.read()
        after_open = os.fstat(handle.fileno())
    after = _check_path_component(path, directory=False)

    def identity(item: os.stat_result) -> tuple[int, int, int, int, int]:
        return (item.st_dev, item.st_ino, item.st_size, item.st_mtime_ns, item.st_nlink)

    if not (identity(before) == identity(opened) == identity(after_open) == identity(after)):
        _fail("NAMED_BOOTSTRAP_SOURCE_CHANGED", relative)
    return content


@dataclass(frozen=True)
class GitArtifact:
    relative_path: str
    git_blob_id: str
    content: bytes

    @property
    def sha256(self) -> str:
        return _sha(self.content)

    def to_dict(self) -> dict[str, object]:
        return {
            "relative_path": self.relative_path,
            "git_blob_id": self.git_blob_id,
            "sha256": self.sha256,
            "size_bytes": len(self.content),
        }


class LocalGitBytes:
    def __init__(self, root: Path, candidate: str) -> None:
        self.root = root
        self.candidate = _digest(candidate, "candidate_commit", git=True)
        executable = shutil.which("git")
        if executable is None:
            _fail("NAMED_BOOTSTRAP_GIT_REQUIRED", "local Git executable missing")
        self.executable = Path(executable).absolute()
        self.environment = {
            key: value for key, value in os.environ.items() if not key.upper().startswith("GIT_")
        }
        self.environment.update(
            {
                "GIT_CONFIG_NOSYSTEM": "1",
                "GIT_CONFIG_SYSTEM": os.devnull,
                "GIT_CONFIG_GLOBAL": os.devnull,
                "GIT_TERMINAL_PROMPT": "0",
                "GIT_NO_REPLACE_OBJECTS": "1",
                "GIT_NO_LAZY_FETCH": "1",
                "GIT_OPTIONAL_LOCKS": "0",
            }
        )
        self.version = self.run("--version").decode("utf-8").strip()
        self.assert_head()

    def run(self, *arguments: str) -> bytes:
        # Git 2.45+ supports the explicit no-lazy-fetch switch.  Unsupported Git
        # fails closed instead of retrying without the network prohibition.
        command = [
            str(self.executable),
            "--no-replace-objects",
            "--no-lazy-fetch",
            "-c",
            f"safe.directory={self.root.as_posix()}",
            "-c",
            "core.fsmonitor=false",
            "-c",
            "protocol.allow=never",
            "-C",
            str(self.root),
            *arguments,
        ]
        result = subprocess.run(
            command,
            shell=False,
            env=self.environment,
            stdin=subprocess.DEVNULL,
            capture_output=True,
            check=False,
        )
        if result.returncode != 0:
            _fail("NAMED_BOOTSTRAP_LOCAL_GIT_FAILED", "/".join(arguments[:2]))
        return result.stdout

    def assert_head(self) -> None:
        actual_root = Path(self.run("rev-parse", "--show-toplevel").decode("utf-8").strip())
        if os.path.normcase(str(actual_root)) != os.path.normcase(str(self.root)):
            _fail("NAMED_BOOTSTRAP_GIT_ROOT_MISMATCH", str(actual_root))
        head = self.run("rev-parse", "--verify", "HEAD").decode("ascii").strip()
        candidate = (
            self.run("rev-parse", "--verify", f"{self.candidate}^{{commit}}")
            .decode("ascii")
            .strip()
        )
        if head != self.candidate or candidate != self.candidate:
            _fail("NAMED_BOOTSTRAP_CANDIDATE_MISMATCH", head)

    def artifact(self, relative: str) -> GitArtifact:
        path = _relative(relative)
        entry = self.run("ls-tree", "-z", self.candidate, "--", f":(literal){path}")
        rows = entry.split(b"\0")
        if len(rows) != 2 or rows[-1] != b"" or b"\t" not in rows[0]:
            _fail("NAMED_BOOTSTRAP_GIT_OBJECT_MISSING", path)
        metadata, actual_path = rows[0].split(b"\t", 1)
        mode, kind, blob_id = metadata.decode("ascii").split(" ")
        if (
            mode not in {"100644", "100755"}
            or kind != "blob"
            or actual_path.decode("utf-8") != path
        ):
            _fail("NAMED_BOOTSTRAP_GIT_ENTRY_INVALID", path)
        blob = _digest(blob_id, "git_blob_id", git=True)
        content = self.run("cat-file", "blob", blob)
        if _initial_file_bytes(self.root, path) != content:
            _fail("NAMED_BOOTSTRAP_DISK_SOURCE_DRIFT", path)
        return GitArtifact(path, blob, content)


@dataclass(frozen=True)
class CompiledProjectModule:
    name: str
    artifact: GitArtifact
    is_package: bool
    code: CodeType

    def to_dict(self) -> dict[str, object]:
        return {
            "module_name": self.name,
            "source_path": self.artifact.relative_path,
            "git_blob_id": self.artifact.git_blob_id,
            "sha256": self.artifact.sha256,
            "size_bytes": len(self.artifact.content),
            "is_package": self.is_package,
        }


class CapturedProjectLoader(importlib.abc.MetaPathFinder, importlib.abc.Loader):
    def __init__(self, root: Path, modules: Mapping[str, CompiledProjectModule]) -> None:
        self.root = root
        self.modules = dict(modules)
        self.loaded: set[str] = set()

    def find_spec(self, fullname: str, path: Any = None, target: Any = None) -> Any:
        if fullname != "ai_trading_system" and not fullname.startswith("ai_trading_system."):
            return None
        item = self.modules.get(fullname)
        if item is None:
            _fail("NAMED_BOOTSTRAP_UNREVIEWED_IMPORT", fullname)
        assert item is not None
        physical = self.root / item.artifact.relative_path
        return importlib.util.spec_from_loader(
            fullname, self, origin=str(physical), is_package=item.is_package
        )

    def create_module(self, spec: Any) -> None:
        return None

    def exec_module(self, module: ModuleType) -> None:
        item = self.modules[module.__name__]
        if item.name in self.loaded:
            _fail("NAMED_BOOTSTRAP_RELOAD_FORBIDDEN", item.name)
        physical = self.root / item.artifact.relative_path
        module.__file__ = str(physical)
        module.__dict__["__cached__"] = None
        if item.is_package:
            module.__path__ = [str(physical.parent)]
        exec(item.code, module.__dict__)
        self.loaded.add(item.name)


class NamedBootstrapSession:
    """One child; only plain observations leave it, never its context/seal."""

    def __init__(self, request: Mapping[str, Any], *, operation: str, source_lease_id: str) -> None:
        preloaded = sorted(
            name
            for name in sys.modules
            if name == "ai_trading_system" or name.startswith("ai_trading_system.")
        )
        if preloaded:
            _fail("NAMED_BOOTSTRAP_PROJECT_PREIMPORTED", preloaded[0])
        if operation not in {
            "run",
            "verify",
            "activate",
            "capture",
            "composer-activate",
            "composer-readiness",
            "composer-capture",
        }:
            _fail("NAMED_BOOTSTRAP_OPERATION_INVALID", operation)
        # Correlation from the trusted coordinator, not an independent lease
        # proof. The parent binds a live guard/fence proof and this child's
        # terminal outcome; no new lock/lease store is created by the bootstrap.
        self.source_lease_id = _text(source_lease_id, "source_lease_id")
        if re.fullmatch(r"lease-[0-9a-f]{20}", self.source_lease_id) is None:
            _fail("NAMED_BOOTSTRAP_LEASE_ID_INVALID", "coordinator lease id required")
        roots = request.get("roots")
        if not isinstance(roots, dict):
            _fail("NAMED_BOOTSTRAP_FIELD_INVALID", "roots")
        self.root = _absolute_directory(roots.get("execution_root"))
        self.started_at = datetime.now(UTC).isoformat()
        self.operation = operation
        self.canonical_dq_call_count = 0
        self.pre_dq_checked_at: str | None = None
        self.terminal_checked_at: str | None = None
        self.context: Any = None
        self.context_module: Any = None
        self.git = LocalGitBytes(
            self.root, _digest(request.get("candidate_commit"), "candidate", git=True)
        )
        self.manifest = self.git.artifact(_relative(request.get("source_manifest_path")))
        if self.manifest.sha256 != _digest(
            request.get("source_manifest_sha256"), "source manifest"
        ):
            _fail("NAMED_BOOTSTRAP_MANIFEST_SHA_MISMATCH", self.manifest.relative_path)
        # New parent operations require the exact reviewed S3b profile before
        # any project module is compiled/imported. Legacy run/verify are not
        # aliases for capture and keep their original worker behavior.
        if operation in {"activate", "capture"} and (
            self.manifest.relative_path
            != "config/data_governance/named_prospective_five_candidate_sources_v1.json"
            or self.manifest.sha256
            != "9a11ed94e1c318aee3c44a7d01ba0f31556bd4de355eef1f75893245fe8bc1ce"
        ):
            _fail("NAMED_BOOTSTRAP_PROSPECTIVE_PROFILE_REQUIRED", operation)
        if operation in {"composer-activate", "composer-readiness", "composer-capture"} and (
            self.manifest.relative_path
            != "config/data_governance/named_composer_prospective_sources_v1.json"
            or self.manifest.sha256
            != "2613012b0774aaf78b448ccf63ee469099dd796cea28de8060b0ad9cd3bc3d2a"
        ):
            _fail("NAMED_BOOTSTRAP_COMPOSER_PROFILE_REQUIRED", operation)
        self.bootstrap = self.git.artifact(BOOTSTRAP_PATH)
        if Path(__file__).absolute() != self.root / BOOTSTRAP_PATH:
            _fail("NAMED_BOOTSTRAP_ENTRY_ROOT_MISMATCH", str(Path(__file__).absolute()))
        manifest = _json_object(self.manifest.content)
        expected_keys = {
            "schema_version",
            "source_kind",
            "bootstrap_path",
            "worker_entrypoint",
            "modules",
            "policy_dependencies",
        }
        if set(manifest) != expected_keys or manifest["schema_version"] != SOURCE_SCHEMA:
            _fail("NAMED_BOOTSTRAP_MANIFEST_SCHEMA", self.manifest.relative_path)
        if (
            manifest["source_kind"] != "GIT_COMMIT_BYTES_COMPILED"
            or manifest["bootstrap_path"] != BOOTSTRAP_PATH
            or manifest["worker_entrypoint"] != f"{WORKER_MODULE}:bootstrap_worker"
        ):
            _fail("NAMED_BOOTSTRAP_MANIFEST_SCOPE", self.manifest.relative_path)
        self.modules = self._compile_modules(manifest["modules"])
        self.dependencies = self._capture_dependencies(manifest["policy_dependencies"])
        if _relative(request.get("policy_path")) not in self.dependencies:
            _fail("NAMED_BOOTSTRAP_POLICY_NOT_DECLARED", str(request.get("policy_path")))
        self.loader = CapturedProjectLoader(self.root, self.modules)
        self.imports_completed_at: str | None = None

    def _compile_modules(self, rows: object) -> dict[str, CompiledProjectModule]:
        if not isinstance(rows, list) or not rows:
            _fail("NAMED_BOOTSTRAP_MODULE_SET_INVALID", "nonempty array required")
        result: dict[str, CompiledProjectModule] = {}
        paths: set[str] = set()
        for row in rows:
            if not isinstance(row, dict) or set(row) != {
                "module_name",
                "source_path",
                "is_package",
            }:
                _fail("NAMED_BOOTSTRAP_MODULE_SET_INVALID", "strict module row required")
            name = _text(row["module_name"], "module_name")
            if re.fullmatch(r"ai_trading_system(?:\.[A-Za-z_][A-Za-z0-9_]*)*", name) is None:
                _fail("NAMED_BOOTSTRAP_MODULE_SET_INVALID", name)
            is_package = row["is_package"]
            if type(is_package) is not bool:
                _fail("NAMED_BOOTSTRAP_MODULE_SET_INVALID", name)
            path = _relative(row["source_path"])
            expected = "src/" + name.replace(".", "/") + ("/__init__.py" if is_package else ".py")
            if path != expected or name in result or path in paths:
                _fail("NAMED_BOOTSTRAP_MODULE_SET_INVALID", name)
            artifact = self.git.artifact(path)
            code = compile(artifact.content, str(self.root / path), "exec", dont_inherit=True)
            result[name] = CompiledProjectModule(name, artifact, is_package, code)
            paths.add(path)
        for name in result:
            parts = name.split(".")
            for depth in range(1, len(parts)):
                parent = result.get(".".join(parts[:depth]))
                if parent is None or not parent.is_package:
                    _fail("NAMED_BOOTSTRAP_PACKAGE_MISSING", name)
        if CONTEXT_MODULE not in result or WORKER_MODULE not in result:
            _fail("NAMED_BOOTSTRAP_MODULE_SET_INVALID", "fixed context/worker required")
        return result

    def _capture_dependencies(self, rows: object) -> dict[str, GitArtifact]:
        if not isinstance(rows, list) or not rows:
            _fail("NAMED_BOOTSTRAP_DEPENDENCIES_INVALID", "explicit array required")
        result: dict[str, GitArtifact] = {}
        for raw in rows:
            path = _relative(raw)
            if path in result or not path.startswith(("config/", "inputs/data_quality/")):
                _fail("NAMED_BOOTSTRAP_DEPENDENCIES_INVALID", path)
            result[path] = self.git.artifact(path)
        return result

    def load(self) -> None:
        sys.meta_path.insert(0, self.loader)
        try:
            for name in sorted(self.modules):
                importlib.import_module(name)
            if self.loader.loaded != set(self.modules):
                _fail("NAMED_BOOTSTRAP_LOADED_SET_MISMATCH", "incomplete original initialization")
            context = importlib.import_module(CONTEXT_MODULE)
            bindings = tuple(
                context.GitCompiledModuleBinding.from_dict(self.modules[name].to_dict())
                for name in sorted(self.modules)
            )
            identity = context.NamedExecutionIdentity(
                execution_root=self.root.as_posix(),
                candidate_commit=self.git.candidate,
                source_manifest_path=self.manifest.relative_path,
                source_manifest_sha256=self.manifest.sha256,
                bootstrap_path=BOOTSTRAP_PATH,
                bootstrap_sha256=self.bootstrap.sha256,
                python_executable=Path(sys.executable).absolute().as_posix(),
                python_version=sys.version,
                git_executable=self.git.executable.as_posix(),
                git_version=self.git.version,
                modules=bindings,
            )
            self.context_module = context
            self.context = context._initialize_named_execution_context(identity)
            context._freeze_named_execution_context(self.context, loaded_modules=bindings)
            self.imports_completed_at = datetime.now(UTC).isoformat()
            self.assert_execution_unchanged(stage="POST_IMPORT")
        except BaseException:
            self.close()
            raise

    def assert_execution_unchanged(self, *, stage: str) -> str:
        if self.context is None:
            _fail("NAMED_BOOTSTRAP_CONTEXT_REQUIRED", stage)
        self.context_module.require_named_execution_context()
        self.git.assert_head()
        reader = importlib.import_module("ai_trading_system.data.immutable_publish")
        artifacts = [
            self.manifest,
            self.bootstrap,
            *self.dependencies.values(),
            *(item.artifact for item in self.modules.values()),
        ]
        for item in artifacts:
            observed = reader.read_contained_artifact_bytes(
                root=self.root, relative_path=item.relative_path
            )
            if observed != item.content:
                _fail("NAMED_BOOTSTRAP_DISK_SOURCE_DRIFT", f"{stage}:{item.relative_path}")
        if self.loader.loaded != set(self.modules):
            _fail("NAMED_BOOTSTRAP_LOADED_SET_MISMATCH", stage)
        checked = datetime.now(UTC).isoformat()
        if stage == "PRE_DQ":
            self.pre_dq_checked_at = checked
        if stage == "TERMINAL":
            self.terminal_checked_at = checked
        return checked

    def note_canonical_dq_dispatch(self) -> None:
        if self.operation != "run" or self.pre_dq_checked_at is None:
            _fail("NAMED_BOOTSTRAP_DQ_DISPATCH_FORBIDDEN", self.operation)
        if self.canonical_dq_call_count >= CANONICAL_DQ_DISPATCH_MAXIMUM:
            _fail("NAMED_BOOTSTRAP_DQ_DISPATCH_REPEATED", self.operation)
        self.canonical_dq_call_count += 1

    def close(self) -> None:
        if self.context is not None:
            self.context_module.close_named_execution_context(self.context)
            self.context = None
        if hasattr(self, "loader") and self.loader in sys.meta_path:
            sys.meta_path.remove(self.loader)


def main() -> int:
    parser = argparse.ArgumentParser(description="指定不可变快照的受控 canonical DQ 子进程")
    parser.add_argument("--request", required=True, type=Path)
    parser.add_argument("--request-sha256", required=True)
    parser.add_argument(
        "--operation",
        choices=(
            "run",
            "verify",
            "activate",
            "capture",
            "composer-activate",
            "composer-readiness",
            "composer-capture",
        ),
        required=True,
    )
    parser.add_argument("--source-lease-id", required=True)
    parser.add_argument("--receipt-path")
    parser.add_argument("--receipt-sha256")
    parser.add_argument("--run-dispatch-path")
    parser.add_argument("--run-dispatch-sha256")
    args = parser.parse_args()
    session: NamedBootstrapSession | None = None
    result: dict[str, object] | None = None
    try:
        if not sys.flags.isolated:
            _fail("NAMED_BOOTSTRAP_ISOLATED_CHILD_REQUIRED", "使用 python -I -B 启动此固定入口")
        request_path = args.request
        if not request_path.is_absolute():
            _fail("NAMED_BOOTSTRAP_REQUEST_PATH_INVALID", "absolute request path required")
        request_root = _absolute_directory(request_path.parent.as_posix())
        content = _initial_file_bytes(request_root, request_path.name)
        if _sha(content) != _digest(args.request_sha256, "request_sha256"):
            _fail("NAMED_BOOTSTRAP_REQUEST_SHA_MISMATCH", str(request_path))
        verification_arguments = (
            args.receipt_path,
            args.receipt_sha256,
            args.run_dispatch_path,
            args.run_dispatch_sha256,
        )
        if (
            args.operation
            in {
                "run",
                "activate",
                "capture",
                "composer-activate",
                "composer-readiness",
                "composer-capture",
            }
            and any(value is not None for value in verification_arguments)
        ) or (
            args.operation == "verify" and any(value is None for value in verification_arguments)
        ):
            _fail("NAMED_BOOTSTRAP_RECEIPT_ARGUMENT_INVALID", args.operation)
        if args.receipt_path is not None:
            _relative(args.receipt_path)
            _digest(args.receipt_sha256, "receipt_sha256")
        if args.run_dispatch_path is not None:
            _relative(args.run_dispatch_path)
            _digest(args.run_dispatch_sha256, "run_dispatch_sha256")
        request = _json_object(content)
        session = NamedBootstrapSession(
            request, operation=args.operation, source_lease_id=args.source_lease_id
        )
        session.load()
        worker = importlib.import_module(WORKER_MODULE)
        result = worker.bootstrap_worker(
            request,
            operation=args.operation,
            receipt_path=args.receipt_path,
            receipt_sha256=args.receipt_sha256,
            run_dispatch_path=args.run_dispatch_path,
            run_dispatch_sha256=args.run_dispatch_sha256,
            bootstrap=session,
        )
        expected_calls = 1 if args.operation == "run" else 0
        if session.canonical_dq_call_count != expected_calls:
            _fail("NAMED_BOOTSTRAP_DQ_CALL_COUNT_MISMATCH", args.operation)
        session.assert_execution_unchanged(stage="TERMINAL")
        result = {
            **result,
            "child_started_at": session.started_at,
            "child_terminal_checked_at": session.terminal_checked_at,
        }
        print(json.dumps(result, ensure_ascii=False, sort_keys=True, allow_nan=False))
        return 0
    except (ValueError, OSError, ImportError, RuntimeError, SyntaxError, TypeError) as exc:
        parent_calls = 0 if session is None else session.canonical_dq_call_count
        capture_operation = args.operation in {
            "activate",
            "capture",
            "composer-activate",
            "composer-readiness",
            "composer-capture",
        }
        observed_calls = (
            (
                result.get("canonical_dq_call_count")
                if result is not None
                else getattr(exc, "prospective_child_canonical_dq_call_count", None)
            )
            if capture_operation
            else parent_calls
        )
        if observed_calls is not None and (type(observed_calls) is not int or observed_calls < 0):
            observed_calls = None
        capture_metadata = (
            {
                "parent_canonical_dq_call_count": parent_calls,
                "counter_observation_state": "UNKNOWN" if observed_calls is None else "KNOWN",
                "dq_parent_receipt": (
                    result.get("dq_parent_receipt")
                    if result is not None
                    else getattr(exc, "prospective_dq_parent_receipt", None)
                ),
                "capture_admitted": False,
                "activation_admitted": False,
                "real_observation_admitted": False,
                "clock_failure_diagnostic": getattr(
                    exc, "prospective_clock_failure_diagnostic", None
                ),
                "recorder_return_observations": getattr(
                    exc, "prospective_recorder_return_observations", None
                ),
            }
            if capture_operation
            else {}
        )
        print(
            json.dumps(
                {
                    "schema_version": "named_data_quality_bootstrap_result.v1",
                    "status": "BLOCKED",
                    "reason_code": getattr(exc, "code", "NAMED_BOOTSTRAP_FAILED"),
                    "detail": str(exc),
                    "canonical_dq_call_count": observed_calls,
                    **capture_metadata,
                    "dispatch_allowed": False,
                    "production_effect": "none",
                    "broker_action": "none",
                },
                ensure_ascii=False,
                sort_keys=True,
            )
        )
        return 2
    finally:
        if session is not None:
            session.close()


if __name__ == "__main__":
    raise SystemExit(main())
