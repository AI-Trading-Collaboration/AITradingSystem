"""DEVX-014: preserve a terminated lane without publishing or changing its checkout.

The custom Git ref is a raw-byte evidence carrier, not an integration candidate.
Normal publication, task-source and validation gates intentionally do not accept
this receipt as permission. See DEVX-014_Dirty_Source_Preservation_Recovery_V1.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
import subprocess
import sys
import threading
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from contextvars import ContextVar
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from typing import Any, NoReturn
from uuid import uuid4

from ai_trading_system.platform.architecture.checkout_guard import (
    CheckoutGuardError,
    CheckoutIdentity,
    CheckoutLeaseHandle,
    CheckoutOperationClass,
    CheckoutOperationIntent,
)
from ai_trading_system.platform.architecture.integration_publication_fence import (
    IntegrationPublicationFence,
    PublicationFenceError,
)
from ai_trading_system.platform.architecture.task_registry_canonical import (
    validate_canonical_fragment,
)
from ai_trading_system.platform.artifacts.json_contract import load_strict_json_text
from ai_trading_system.yaml_loader import safe_load_yaml_text

MODULE_PATH = "src/ai_trading_system/platform/architecture/source_preservation.py"
CLI_PATH = "scripts/architecture_arch005_source_preservation.py"
POLICY_PATH = "config/architecture/arch_005_source_preservation.yaml"
V2_POLICY_PATH = "config/architecture/arch_005_source_preservation_v2.yaml"
DEFAULT_POLICY_PATH = Path(__file__).resolve().parents[4] / POLICY_PATH
FENCE_POLICY_PATH = "config/architecture/arch_005_integration_publication_fence.yaml"
_OLD_POLICIES = {
    "fence": FENCE_POLICY_PATH,
    "checkout": "config/architecture/arch_005_s4d_checkout_guard.yaml",
    "parallel": "config/architecture/arch_005_parallel_control_policy.yaml",
}
PROFILE = "RAW_BYTES_SOURCE_ONLY_UNVALIDATED"
_RUNTIME = "outputs/architecture/arch_005_source_preservation"
_REF_PREFIX = "refs/aits/source-preservation/"
# Engineering bound for local Git configuration/locator evidence, not market data.
_MAX_GIT_CONFIGURATION_BYTES = 1024 * 1024
_GIT_CONFIGURATION_SCHEMA = "source_preservation_git_configuration.v1"
# Reviewed finite implementation dependencies: the invoked guard/fence/lease and
# canonical-fragment validators, their JSON/YAML/artifact/config value helpers,
# and normal parent package initializers. Unrelated Full collection imports are
# deliberately not source-preservation execution authority.
_IMPLEMENTATION_MODULES = (
    "ai_trading_system",
    "ai_trading_system.config",
    "ai_trading_system.yaml_loader",
    "ai_trading_system.platform",
    "ai_trading_system.platform.architecture",
    "ai_trading_system.platform.architecture.source_preservation",
    "ai_trading_system.platform.architecture.checkout_guard",
    "ai_trading_system.platform.architecture.integration_publication_fence",
    "ai_trading_system.platform.architecture.parallel_control",
    "ai_trading_system.platform.architecture.parallel_control_kernel",
    "ai_trading_system.platform.architecture.lease_arbiter",
    "ai_trading_system.platform.architecture.task_registry_canonical",
    "ai_trading_system.platform.architecture.task_registry_shadow",
    "ai_trading_system.platform.architecture.devex",
    "ai_trading_system.platform.architecture.dependency_gate",
    "ai_trading_system.platform.artifacts",
    "ai_trading_system.platform.artifacts.writer",
    "ai_trading_system.platform.artifacts.json_contract",
    "ai_trading_system.platform.config",
    "ai_trading_system.platform.config.market_regimes",
    "ai_trading_system.platform.config.resolver",
    "ai_trading_system.contracts",
    "ai_trading_system.contracts.artifact_envelope",
    "ai_trading_system.contracts.data_quality",
    "ai_trading_system.contracts.research_context",
    "ai_trading_system.contracts.status",
    "ai_trading_system.core",
    "ai_trading_system.core.production_effect",
)
_PHASES = ("ACQUIRED", "CAPTURED", "OBJECTS_WRITTEN", "REF_CREATED", "VERIFIED", "RELEASED")
_REQUEST_KEYS = {
    "schema_version",
    "preservation_id",
    "recovery_task_id",
    "source_task_id",
    "owner_instruction_ref",
    "actor",
    "thread_id",
    "source_root",
    "source_common_git_dir",
    "source_branch",
    "frozen_base_sha",
    "source_head_sha",
    "observed_main_sha",
    "observed_origin_main_sha",
    "terminal_transaction",
    "files",
}
_POLICY_KEYS = {
    "schema_version",
    "policy_id",
    "version",
    "status",
    "recovery_task_id",
    "owner_instruction_ref",
    "runtime_root",
    "ref_prefix",
    "max_files",
    "max_total_bytes",
}
_SAFETY: dict[str, Any] = {
    "profile": PROFILE,
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


class SourcePreservationError(ValueError):
    def __init__(self, code: str, message: str) -> None:
        self.code = code
        self.message = message
        super().__init__(f"{code}: {message}")


def _fail(code: str, message: str) -> NoReturn:
    raise SourcePreservationError(f"SOURCE_PRESERVATION_{code}", message)


def _json_bytes(value: object) -> bytes:
    return (
        json.dumps(
            value, sort_keys=True, ensure_ascii=False, separators=(",", ":"), allow_nan=False
        )
        + "\n"
    ).encode("utf-8")


def _sha(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _object(value: object, keys: set[str] | None = None) -> dict[str, Any]:
    if not isinstance(value, Mapping) or any(not isinstance(key, str) for key in value):
        _fail("REQUEST", "object required")
    result = dict(value)
    if keys is not None and set(result) != keys:
        _fail("REQUEST", "unexpected or missing fields")
    return result


def _text(value: object) -> str:
    if not isinstance(value, str) or not value or value != value.strip() or "\x00" in value:
        _fail("REQUEST", "nonempty canonical text required")
    return value


def _digest(value: object, length: int = 64) -> str:
    result = _text(value)
    if re.fullmatch(r"[0-9a-f]{" + str(length) + r"}", result) is None:
        _fail("REQUEST", "invalid exact digest")
    return result


def _relative(value: object) -> str:
    result = _text(value)
    pure = PurePosixPath(result)
    if (
        pure.is_absolute()
        or "\\" in result
        or ":" in result
        or any(part in {"", ".", "..", ".git"} for part in result.split("/"))
        or pure.as_posix() != result
    ):
        _fail("PATH", "noncanonical repository-relative path")
    return result


def _under(path: str, scope: str) -> bool:
    return path.casefold() == scope.casefold() or path.casefold().startswith(scope.casefold() + "/")


def _regular(path: Path, *, expected_size: int | None = None) -> bytes:
    # Reject Windows junctions as well as symlinks, including every existing ancestor.
    for item in (path, *path.parents):
        try:
            info = item.lstat()
        except FileNotFoundError:
            _fail("PATH", "required path missing")
        if stat.S_ISLNK(info.st_mode) or getattr(info, "st_file_attributes", 0) & 0x400:
            _fail("PATH", "symlink or reparse point")
    before = path.stat()
    if not stat.S_ISREG(before.st_mode):
        _fail("PATH", "regular file required")
    if expected_size is not None and before.st_size != expected_size:
        _fail("DRIFT", "source size differs before capture")
    with path.open("rb") as stream:
        opened = os.fstat(stream.fileno())
        content = stream.read() if expected_size is None else stream.read(expected_size + 1)
        after_open = os.fstat(stream.fileno())
    after = path.stat()

    def fields(item: os.stat_result) -> tuple[int, int, int, int]:
        return item.st_dev, item.st_ino, item.st_size, item.st_mtime_ns

    if not (fields(before) == fields(opened) == fields(after_open) == fields(after)):
        _fail("DRIFT", "file changed during capture")
    return content


def _member(root: Path, relative: str) -> Path:
    path = root / _relative(relative)
    if not path.resolve(strict=False).is_relative_to(root.resolve()):
        _fail("PATH", "path escaped root")
    return path


def _load_json(path: Path) -> dict[str, Any]:
    try:
        return _object(load_strict_json_text(_regular(path).decode("utf-8")))
    except (ValueError, UnicodeError) as exc:
        if isinstance(exc, SourcePreservationError):
            raise
        _fail("RECEIPT", "invalid strict JSON")


def _write_once(path: Path, content: bytes) -> None:
    for parent in path.parents:
        if not parent.exists():
            continue
        info = parent.lstat()
        if stat.S_ISLNK(info.st_mode) or getattr(info, "st_file_attributes", 0) & 0x400:
            _fail("PATH", "output parent is a reparse point")
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with path.open("xb") as stream:
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
    except FileExistsError:
        _fail("PARTIAL", "write-once evidence already exists")


def _configuration_path(path: Path) -> Path:
    """Check existing components before resolving or reading Git metadata."""
    for item in (path, *path.parents):
        try:
            info = item.lstat()
        except FileNotFoundError:
            continue
        if stat.S_ISLNK(info.st_mode) or getattr(info, "st_file_attributes", 0) & 0x400:
            _fail("PATH", "Git configuration path is a symlink/reparse point")
    return path.resolve(strict=False)


def _configuration_file(path: Path, role: str) -> dict[str, Any]:
    checked = _configuration_path(path)
    try:
        info = checked.stat()
    except FileNotFoundError:
        return {
            "role": role,
            "path": checked.as_posix(),
            "present": False,
            "size_bytes": None,
            "sha256": None,
        }
    if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
        _fail("PATH", "single-link regular Git configuration required")
    if info.st_size > _MAX_GIT_CONFIGURATION_BYTES:
        _fail("ENVIRONMENT", "Git configuration exceeds engineering byte budget")
    content = _regular(checked, expected_size=info.st_size)
    if len(content) != info.st_size:
        _fail("DRIFT", "Git configuration changed during bounded capture")
    return {
        "role": role,
        "path": checked.as_posix(),
        "present": True,
        "size_bytes": len(content),
        "sha256": _sha(content),
    }


def _git_configuration_layout(root: Path) -> tuple[Path, Path]:
    """Resolve only .git/commondir locators, before Git can load config bytes."""
    marker = _configuration_path(root / ".git")
    if marker.is_dir():
        git_dir = marker
    else:
        record = _configuration_file(marker, "GIT_DIR_POINTER")
        if not record["present"]:
            _fail("IDENTITY", "Git directory marker missing")
        raw = _regular(marker, expected_size=record["size_bytes"])
        if _sha(raw) != record["sha256"]:
            _fail("DRIFT", "Git directory pointer changed")
        text = raw.decode("utf-8").strip()
        if not text.startswith("gitdir: ") or "\n" in text or "\r" in text:
            _fail("IDENTITY", "unsupported Git directory pointer")
        git_dir = _configuration_path(root / _text(text.removeprefix("gitdir: ")))
    if not git_dir.is_dir():
        _fail("IDENTITY", "Git directory does not exist")
    common_marker = git_dir / "commondir"
    common_record = _configuration_file(common_marker, "COMMON_DIR_POINTER")
    common = git_dir
    if common_record["present"]:
        raw = _regular(common_marker, expected_size=common_record["size_bytes"])
        if _sha(raw) != common_record["sha256"]:
            _fail("DRIFT", "Git common directory pointer changed")
        text = raw.decode("utf-8").strip()
        if "\n" in text or "\r" in text:
            _fail("IDENTITY", "unsupported Git common directory pointer")
        common = _configuration_path(git_dir / _text(text))
    if not common.is_dir():
        _fail("IDENTITY", "Git common directory does not exist")
    return git_dir, common


def _configuration_evidence(
    value: object, *, trusted_root: Path, source_root: Path, source_common: Path
) -> None:
    """Validate retained summaries, never read historical credential-bearing files."""
    outer = _object(value, {"schema_version", "trusted", "source", "snapshot_sha256"})
    body = {key: item for key, item in outer.items() if key != "snapshot_sha256"}
    if outer["schema_version"] != _GIT_CONFIGURATION_SCHEMA or outer["snapshot_sha256"] != _sha(
        _json_bytes(body)
    ):
        _fail("RECEIPT", "captured Git configuration summary checksum/schema mismatch")
    for role, root in (("trusted", trusted_root), ("source", source_root)):
        row = _object(
            outer[role],
            {
                "checkout_root",
                "git_common_dir",
                "git_dir",
                "files",
                "worktree_config_mode",
                "entry_count",
                "entries_sha256",
            },
        )
        paths = {}
        for field in ("checkout_root", "git_common_dir", "git_dir"):
            raw = _text(row[field])
            path = Path(raw)
            if not path.is_absolute() or path.as_posix() != raw or ".." in path.parts:
                _fail("RECEIPT", "invalid captured configuration locator")
            paths[field] = path
        if (
            paths["checkout_root"] != root
            or not paths["git_dir"].is_relative_to(paths["git_common_dir"])
            or role == "source"
            and paths["git_common_dir"] != source_common
            or row["worktree_config_mode"] not in {"ABSENT", "FALSE", "TRUE"}
            or type(row["entry_count"]) is not int
            or row["entry_count"] < 0
        ):
            _fail("RECEIPT", "captured configuration identity contradicts execution scope")
        _digest(row["entries_sha256"])
        if not isinstance(row["files"], list) or len(row["files"]) != 2:
            _fail("RECEIPT", "exact common/worktree configuration summaries required")
        for item, expected_role, expected_path in zip(
            row["files"],
            ("COMMON_CONFIG", "WORKTREE_CONFIG"),
            (paths["git_common_dir"] / "config", paths["git_dir"] / "config.worktree"),
            strict=True,
        ):
            record = _object(item, {"role", "path", "present", "size_bytes", "sha256"})
            if (
                record["role"] != expected_role
                or record["path"] != expected_path.as_posix()
                or type(record["present"]) is not bool
            ):
                _fail("RECEIPT", "captured configuration file locator mismatch")
            if record["present"]:
                if (
                    type(record["size_bytes"]) is not int
                    or not 0 <= record["size_bytes"] <= _MAX_GIT_CONFIGURATION_BYTES
                ):
                    _fail("RECEIPT", "invalid captured configuration byte count")
                _digest(record["sha256"])
            elif record["size_bytes"] is not None or record["sha256"] is not None:
                _fail("RECEIPT", "absent configuration cannot claim content")


_RuntimeNamespace = tuple[
    dict[Path, tuple[int, int]], dict[Path, tuple[int, int, int, int]],
]


def _runtime_namespace(root: Path, declared: Mapping[Path, str] | None) -> _RuntimeNamespace:
    """Bounded metadata inventory, never imports or reads undeclared file content."""
    if not root.is_absolute() or ".." in root.parts or _configuration_path(root) != root:
        _fail("RUNTIME_ROOT", "exact absolute runtime root required")
    directories: dict[Path, tuple[int, int]] = {}
    files: dict[Path, tuple[int, int, int, int]] = {}
    pending = [root]
    while pending:
        path = pending.pop()
        info = path.lstat()
        if stat.S_ISLNK(info.st_mode) or getattr(info, "st_file_attributes", 0) & 0x400:
            _fail("RUNTIME_REPARSE", "runtime namespace contains a reparse object")
        if len(directories) + len(files) >= 50000:
            _fail("RUNTIME_BUDGET", "runtime namespace exceeds 50000 entries")
        if stat.S_ISDIR(info.st_mode):
            directories[path] = (info.st_dev, info.st_ino)
            with os.scandir(path) as entries:
                for entry in entries:
                    if len(directories) + len(files) + len(pending) >= 50000:
                        _fail("RUNTIME_BUDGET", "runtime namespace exceeds 50000 entries")
                    pending.append(Path(entry.path))
        elif stat.S_ISREG(info.st_mode):
            if declared is not None and path not in declared:
                _fail("RUNTIME_UNDECLARED_FILE", "runtime file absent from held declaration")
            files[path] = (info.st_dev, info.st_ino, info.st_nlink, info.st_size)
        else:
            _fail("RUNTIME_FILE_TYPE", "runtime object is not a directory or regular file")
    if root not in directories or (declared is not None and set(files) != {
        path for path in declared if path.is_relative_to(root)
    }):
        _fail("RUNTIME_INVENTORY", "runtime root or declared file inventory differs")
    return directories, files


class HeldGitConfiguration:
    """Live proof of held declared files, not general execution authority."""

    _admission: GitConfigurationAdmission
    _owner: tuple[int, int]
    _roots: frozenset[Path]
    _files: dict[Path, str]
    _runtime_namespaces: dict[Path, _RuntimeNamespace]
    _active: bool

    def __init__(self) -> None:
        _fail("HELD_CONTEXT_FACTORY_REQUIRED", "use successful protected admission")

    def assert_current(self, repository: Path, required_files: Mapping[Path, str]) -> Path:
        if (not self._active or self._admission._held_context is not self
                or self._owner != (os.getpid(), threading.get_ident())):
            _fail("HELD_CONTEXT_INACTIVE", "held context is expired or belongs to another owner")
        canonical_root = repository.resolve(strict=True)
        if canonical_root not in self._roots:
            _fail("HELD_CONTEXT_SCOPE", "repository is outside protected admission")
        if any(not isinstance(digest, str) or re.fullmatch(r"[0-9a-f]{64}", digest) is None
               or self._files.get(path) != digest for path, digest in required_files.items()):
            _fail("HELD_CONTEXT_FILES", "required input is not held with the declared bytes")
        return canonical_root

    def candidate_source_tree(self, repository: Path, candidate_sha: str) -> bytes:
        """Only the fixed source inventory command, never arbitrary Git argv."""
        repository = self.assert_current(repository, {})
        if (not isinstance(candidate_sha, str)
                or re.fullmatch(r"[0-9a-f]{40}", candidate_sha) is None):
            _fail("HELD_CONTEXT_CANDIDATE", "an exact candidate commit is required")
        return self._admission._git(
            repository, "ls-tree", "-r", "-z", candidate_sha, "--", "src", "scripts",
        )

    def assert_runtime_root(self, root: Path) -> Path:
        """Prove a complete namespace in this live hold, not permission to execute it."""
        self.assert_current(self._admission.project_root, {})
        expected = self._runtime_namespaces.get(root)
        if expected is None:
            _fail("RUNTIME_ROOT_NOT_HELD", "runtime root was not admitted as a complete tree")
        if _runtime_namespace(root, self._files) != expected:
            _fail("RUNTIME_DRIFT", "runtime namespace identity changed while held")
        return root


    @contextmanager
    def inspection(self, repository: Path) -> Iterator[None]:
        """Route synchronous inspector reads; this does not authorize code execution."""
        self.assert_current(repository, {})
        if _INSPECTION_GIT.get() is not None:
            _fail("HELD_CONTEXT_NESTED", "an inspection transport is already selected")
        token = _INSPECTION_GIT.set(self)
        try:
            yield
            self.assert_current(repository, {})
        finally:
            _INSPECTION_GIT.reset(token)


_INSPECTION_GIT: ContextVar[HeldGitConfiguration | None] = ContextVar(
    "devx015_inspection_git", default=None,
)


def current_inspection_context(repository: Path) -> HeldGitConfiguration | None:
    """Observe the selected live scope without creating or nesting authority."""
    context = _INSPECTION_GIT.get()
    if context is not None:
        context.assert_current(repository, {})
    return context


def inspection_git_result(
    repository: Path, *arguments: str,
) -> subprocess.CompletedProcess[bytes] | None:
    """Return None only for ordinary callers; selected invalid contexts never fall back."""
    context = _INSPECTION_GIT.get()
    if context is None:
        return None
    root = context.assert_current(repository, {})
    _validate_inspection_git_arguments(arguments)
    return context._admission._git_result(root, *arguments, allowed=(0, 1, 128))


def _validate_inspection_git_arguments(arguments: tuple[str, ...]) -> None:
    """Finite read-only command grammar used by readiness and its dependency adapters."""
    def commit(value: str) -> bool:
        return re.fullmatch(r"[0-9a-f]{40}", value) is not None

    def path(value: str) -> bool:
        value = value.removeprefix(":(literal)")
        try:
            return _relative(value) == value and not value.startswith("-")
        except SourcePreservationError:
            return False

    def object_name(value: str) -> bool:
        sha, sep, relative = value.partition(":")
        return bool(sep) and commit(sha) and path(relative)

    valid = arguments in {
        ("rev-parse", "HEAD"), ("rev-parse", "main"),
        ("rev-parse", "refs/heads/main"), ("rev-parse", "refs/remotes/origin/main"),
        ("rev-parse", "--path-format=absolute", "--git-common-dir"),
        ("branch", "--show-current"),
        ("remote", "get-url", "--push", "--all", "origin"),
        ("rev-parse", "--show-toplevel"),
        ("symbolic-ref", "--quiet", "--short", "HEAD"),
        ("rev-parse", "--abbrev-ref", "--symbolic-full-name", "@{upstream}"),
        ("worktree", "list", "--porcelain", "-z"),
    }
    if arguments[:2] == ("rev-parse", "--verify"):
        valid = len(arguments) == 3 and (
            (arguments[2].endswith("^{commit}") and commit(arguments[2][:-9]))
            or (path(arguments[2])
                 and re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._/-]*", arguments[2]) is not None
                 and ".." not in arguments[2])
        )
    elif arguments[:2] == ("merge-base", "--is-ancestor"):
        valid = len(arguments) == 4 and all(map(commit, arguments[2:]))
    elif arguments[:2] in {("cat-file", "-e"), ("cat-file", "-s")}:
        valid = len(arguments) == 3 and object_name(arguments[2])
    elif arguments[:2] == ("cat-file", "blob"):
        valid = len(arguments) == 3 and (commit(arguments[2]) or object_name(arguments[2]))
    elif arguments[:1] == ("ls-tree",):
        valid = ((len(arguments) == 4 and commit(arguments[1]) and arguments[2] == "--"
                  and path(arguments[3]))
                 or (len(arguments) == 5 and arguments[1] == "-z" and commit(arguments[2])
                     and arguments[3] == "--" and path(arguments[4]))
                 or (len(arguments) == 7 and arguments[1:3] == ("-r", "-z")
                     and commit(arguments[3]) and arguments[4:] == ("--", "src", "scripts")))
    elif arguments[:1] == ("show",):
        valid = ((len(arguments) == 2 and object_name(arguments[1]))
                 or (len(arguments) == 4 and arguments[1:3] == ("-s", "--format=%cI")
                     and commit(arguments[3])))
    elif arguments[:4] == ("ls-files", "--others", "--exclude-standard", "-z"):
        valid = len(arguments) > 5 and arguments[4] == "--" and all(map(path, arguments[5:]))
    elif arguments[:3] == ("grep", "-l", "-F"):
        valid = (len(arguments) == 9 and "\0" not in arguments[3] and commit(arguments[4])
                 and arguments[5:] == ("--", "src", "scripts", "tests"))
    elif arguments[:3] == ("diff", "--numstat", "-z"):
        rest = arguments[3:]
        if rest[:1] == ("--cached",):
            rest = rest[1:]
        valid = (len(rest) >= 5 and rest[:2] == ("--no-ext-diff", "--no-textconv")
                 and commit(rest[2]) and rest[3] == "--" and all(map(path, rest[4:])))
    elif arguments[:1] in {("status",), ("diff",)}:
        prefix: tuple[str, ...] = ("status", "--porcelain=v1", "-z", "--untracked-files=all",
                  "--ignore-submodules=none", "--", ".")
        if arguments[:1] == ("diff",):
            prefix = (("diff", "--cached", "--check", "--", ".")
                      if arguments[1:2] == ("--cached",)
                      else ("diff", "--check", "--", "."))
        valid = arguments[:len(prefix)] == prefix and all(
            value.startswith(":(exclude,literal)") and path(value[len(":(exclude,literal)"):])
            for value in arguments[len(prefix):]
        )
    if not valid:
        _fail("HELD_CONTEXT_GIT_COMMAND", "unsupported inspector Git operation")


def _installed_runtime_manifest(
    runtime: Path, raw: bytes,
) -> tuple[dict[Path, str], dict[Path, int]]:
    from ai_trading_system.platform.artifacts.json_contract import load_strict_json_text

    payload = _object(load_strict_json_text(raw.decode("utf-8")), {"schema_version", "files"})
    rows = payload["files"]
    if (payload["schema_version"] != "devx015_installed_runtime.v1"
            or not isinstance(rows, list) or not 0 < len(rows) < 50000):
        _fail("RUNTIME_MANIFEST", "invalid installed runtime manifest")
    files: dict[Path, str] = {}
    sizes: dict[Path, int] = {}
    names: set[str] = {"runtime-manifest.json"}
    for row in rows:
        record = _object(row, {"path", "sha256", "size_bytes"})
        relative = _relative(record["path"])
        if relative.casefold() in names:
            _fail("RUNTIME_MANIFEST", "duplicate or self-referential runtime path")
        names.add(relative.casefold())
        size = record["size_bytes"]
        if type(size) is not int or not 0 <= size <= 64 * 1024 * 1024:
            _fail("RUNTIME_BUDGET", "invalid installed file size")
        files[runtime / relative] = _digest(record["sha256"])
        sizes[runtime / relative] = size
    manifest = runtime / "runtime-manifest.json"
    files[manifest], sizes[manifest] = _sha(raw), len(raw)
    return files, sizes


@contextmanager
def hold_installed_inspector(candidate_root: Path) -> Iterator[HeldGitConfiguration]:
    """Reconstruct read-only native custody in an already protected installation.

    No installation, ACL repair, account change or execution permission is granted.
    Git ships inside the fixed runtime so its dependencies share the held namespace.
    """
    from ai_trading_system.platform.architecture.workflow_contract import bounded_regular_bytes
    from ai_trading_system.platform.architecture.workflow_coordination import (
        _WindowsEnrollmentAdministrator,
    )

    runtime = Path(sys.executable).absolute().parent
    if (not sys.flags.isolated or not sys.flags.no_site or not sys.dont_write_bytecode
            or "site" in sys.modules or Path.cwd() != runtime
            or Path(sys.prefix) != runtime or Path(sys.base_prefix) != runtime
            or not Path(__file__).resolve().is_relative_to(runtime)):
        _fail("INSTALLED_INSPECTOR_STARTUP", "fixed isolated installation required")
    administrator = _WindowsEnrollmentAdministrator()
    administrator.assert_protected(runtime)
    with administrator.pin_directories((runtime,)):
        manifest = runtime / "runtime-manifest.json"
        administrator.assert_protected(manifest)
        raw = bounded_regular_bytes(manifest, expected_link_count=1)
        # Parse only while the protected manifest itself stays natively held.
        with administrator.hold_protected_files({manifest: _sha(raw)}):
            files, sizes = _installed_runtime_manifest(runtime, raw)
            namespace = _runtime_namespace(runtime, files)
            _, inventory = namespace
            # Engineering I/O envelope includes Python, bundled Git and manifest.
            if sum(sizes.values()) > 2 * 1024**3:
                _fail("RUNTIME_BUDGET", "installed runtime exceeds 2 GiB")
            git = runtime / "git" / "cmd" / "git.exe"
            if git not in inventory:
                _fail("INSTALLED_INSPECTOR_GIT", "bundled Git executable missing")
            identities: dict[Path, tuple[int, int, int]] = {}
            for path, (device, file_id, links, size) in inventory.items():
                if links != 1:
                    _fail("RUNTIME_LINKS", "installed runtime must have single-link files")
                if size != sizes[path]:
                    _fail("RUNTIME_MANIFEST", "installed file size differs from manifest")
                identities[path] = (device, file_id, links)
            environment = {key: value for key, value in os.environ.items()
                           if not key.upper().startswith("GIT_") and key.upper() != "PATH"}
            environment.update(GIT_OPTIONAL_LOCKS="0", GIT_CONFIG_NOSYSTEM="1",
                               GIT_CONFIG_GLOBAL=os.devnull, PATH=os.pathsep.join(
                                   str(runtime / "git" / part)
                                   for part in ("cmd", "mingw64/bin", "usr/bin")
                               ))
            admission = GitConfigurationAdmission(
                candidate_root, git_executable=git, git_environment=environment,
            )
            with admission.held(candidate_root, runtime_files=files,
                                runtime_identities=identities, runtime_roots=(runtime,)):
                yield admission.active_context()


class GitConfigurationAdmission:
    """Shared configuration checks, not an OS-protected execution capability.

    Preserve the original source-preservation evidence and error contracts.
    Callers must separately establish executable and filesystem protection.
    """

    def __init__(
        self, project_root: Path, *, git_executable: Path, git_environment: Mapping[str, str],
    ) -> None:
        self.project_root = project_root.resolve(strict=True)
        if not git_executable.is_absolute() or not git_executable.is_file():
            _fail("ENVIRONMENT", "explicit existing absolute Git executable required")
        self._git_executable = str(_configuration_path(git_executable))
        environment = dict(git_environment)
        if any(not isinstance(key, str) or not key or "=" in key or "\0" in key
               or not isinstance(value, str) or "\0" in value
               for key, value in environment.items()):
            _fail("ENVIRONMENT", "invalid explicit Git environment")
        if len({key.casefold() for key in environment}) != len(environment):
            _fail("ENVIRONMENT", "duplicate case-insensitive Git environment key")
        self._git_environment: dict[str, str] | None = environment
        self._held_context: HeldGitConfiguration | None = None

    def _environment_source(self) -> dict[str, str]:
        return dict(os.environ if self._git_environment is None else self._git_environment)

    def capture(self, source_root: Path) -> dict[str, Any]:
        return self._environment(source_root)

    def recheck(self, source_root: Path, expected: Mapping[str, Any]) -> None:
        self._recheck_environment(source_root, expected)

    def active_context(self) -> HeldGitConfiguration:
        context = self._held_context
        if context is None:
            _fail("HELD_CONTEXT_INACTIVE", "no protected admission is active")
        context.assert_current(self.project_root, {})
        return context

    @contextmanager
    def held(
        self, source_root: Path, *, runtime_files: Mapping[Path, str],
        runtime_identities: Mapping[Path, tuple[int, int, int]],
        runtime_roots: Sequence[Path] = (),
    ) -> Iterator[dict[str, Any]]:
        """Check configuration while declared runtime and metadata stay pinned.

        Explicit runtime_roots require complete file and directory coverage.
        This does not establish dependencies outside those roots or replace
        the caller's execution, repository, or publication authority.
        """
        from ai_trading_system.platform.architecture.workflow_coordination import (
            _WindowsEnrollmentAdministrator,
        )

        if self._held_context is not None:
            _fail("HELD_CONTEXT_NESTED", "protected admission cannot be nested")
        administrator = _WindowsEnrollmentAdministrator()
        executable = Path(self._git_executable)
        if not executable.is_absolute() or executable not in runtime_files:
            _fail("ENVIRONMENT", "fixed Git executable must be declared in runtime files")
        roots = {self.project_root, source_root.resolve(strict=True)}
        if len(set(runtime_roots)) != len(runtime_roots) or any(
            runtime.is_relative_to(repository) or repository.is_relative_to(runtime)
            for runtime in runtime_roots for repository in roots
        ):
            _fail("RUNTIME_ROOT", "runtime roots must be distinct from candidate repositories")
        namespaces = {root: _runtime_namespace(root, runtime_files) for root in runtime_roots}
        runtime_directories = {path for namespace in namespaces.values() for path in namespace[0]}

        def recheck_runtime() -> None:
            if any(_runtime_namespace(root, runtime_files) != expected
                   for root, expected in namespaces.items()):
                _fail("RUNTIME_DRIFT", "runtime namespace changed during admission")

        def physical() -> tuple[set[Path], dict[Path, dict[str, Any]]]:
            directories: set[Path] = set()
            records: dict[Path, dict[str, Any]] = {}
            for root in roots:
                git_dir, common = _git_configuration_layout(root)
                directories.update((root, git_dir, common))
                entries = [(git_dir / "commondir", "COMMON_DIR_POINTER"),
                           (common / "config", "COMMON_CONFIG"),
                           (git_dir / "config.worktree", "WORKTREE_CONFIG")]
                if not (root / ".git").is_dir():
                    entries.append((root / ".git", "GIT_DIR_POINTER"))
                for path, role in entries:
                    records[path] = _configuration_file(path, role)
            return directories, records

        directories, records = physical()
        files = dict(runtime_files)
        for path, record in records.items():
            if record["present"]:
                digest = record["sha256"]
                if path in files and files[path] != digest:
                    _fail("DRIFT", "configuration and runtime declarations conflict")
                files[path] = digest
        with administrator.hold_protected_files(
            files, protected_directories=tuple(directories | runtime_directories),
            expected_file_identities=runtime_identities,
        ):
            # No Git parser/process runs before all physical inputs are held.
            recheck_runtime()
            if physical() != (directories, records):
                _fail("DRIFT", "Git configuration changed before protected admission")
            snapshot = self.capture(source_root)
            context = object.__new__(HeldGitConfiguration)
            context._admission = self
            context._owner = (os.getpid(), threading.get_ident())
            context._roots = frozenset(roots)
            context._files = dict(files)
            context._runtime_namespaces = namespaces
            context._active = True
            self._held_context = context
            try:
                yield json.loads(json.dumps(snapshot))
            finally:
                context._active = False
                self._held_context = None
                recheck_runtime()
                self.recheck(source_root, snapshot)
                if physical() != (directories, records):
                    _fail("DRIFT", "Git configuration changed during protected admission")

    def _environment(self, root: Path) -> dict[str, Any]:
        """Legacy guard inherits these values; never mutate process-global env.

        V1 callers must dispatch a fresh child with OPTIONAL_LOCKS=0 and disabled
        system/global configuration. Only exact safe.directory config entries are
        admitted. Missing settings fail before any legacy status/guard helper.
        """
        required = {
            "GIT_OPTIONAL_LOCKS": "0",
            "GIT_CONFIG_NOSYSTEM": "1",
            "GIT_CONFIG_GLOBAL": os.devnull,
        }
        environment = self._environment_source()
        if any(environment.get(key) != value for key, value in required.items()):
            _fail("ENVIRONMENT", "fresh child requires fixed read-only Git environment")
        allowed = set(required)
        count = environment.get("GIT_CONFIG_COUNT")
        if count is not None:
            if re.fullmatch(r"[0-9]+", count) is None or int(count) > 2:
                _fail("ENVIRONMENT", "only exact safe.directory entries are allowed")
            allowed.add("GIT_CONFIG_COUNT")
            roots = {self.project_root, root}
            for number in range(int(count)):
                key = f"GIT_CONFIG_KEY_{number}"
                value = f"GIT_CONFIG_VALUE_{number}"
                if (
                    environment.get(key) != "safe.directory"
                    or not environment.get(value)
                    or Path(environment[value]).resolve() not in roots
                ):
                    _fail("ENVIRONMENT", "foreign Git configuration entry")
                allowed.update((key, value))
        if any(key.upper().startswith("GIT_") and key not in allowed for key in environment):
            _fail("ENVIRONMENT", "ambient Git override is unsupported")
        # Same common Git directory does not imply the same worktree config.
        # Capture each actual checkout independently; no secret values survive.
        trusted = self._configuration_snapshot(self.project_root)
        source = self._configuration_snapshot(root)
        body = {"schema_version": _GIT_CONFIGURATION_SCHEMA, "trusted": trusted, "source": source}
        snapshot = {**body, "snapshot_sha256": _sha(_json_bytes(body))}
        # Catch changes to the first checkout while inspecting the second one.
        for checkout in (trusted, source):
            self._recheck_configuration_files(checkout)
        return snapshot

    def _configuration_snapshot(self, root: Path) -> dict[str, Any]:
        root = _configuration_path(root)
        git_dir, common = _git_configuration_layout(root)
        files = [
            _configuration_file(common / "config", "COMMON_CONFIG"),
            _configuration_file(git_dir / "config.worktree", "WORKTREE_CONFIG"),
        ]
        # Keep every raw entry, including values shadowed by command-line safety
        # flags. A last-value mapping could conceal a dangerous file setting.
        try:
            raw = self._git(root, "config", "--no-includes", "--null", "--list")
        except SourcePreservationError:
            _fail("ENVIRONMENT", "unable to parse effective Git configuration")
        count = 0
        extension_entries = 0
        for item in raw.split(b"\0"):
            if not item:
                continue
            count += 1
            key_bytes, value_separator, value_bytes = item.partition(b"\n")
            try:
                key_name = key_bytes.decode("utf-8").lower()
                value_text = value_bytes.decode("utf-8").lower()
            except UnicodeError:
                _fail("ENVIRONMENT", "unsupported Git configuration encoding")
            if key_name == "extensions.worktreeconfig":
                extension_entries += 1
            if (
                key_name.startswith("filter.")
                or key_name in {"core.fsmonitor", "core.sshcommand", "core.gitproxy"}
                # A valueless key is not an explicit empty disabled value:
                # notably, valueless fsmonitor is Git's implicit Boolean true.
                and (not value_separator or value_text not in {"false", "0", ""})
                or key_name == "extensions.partialclone"
                or key_name.endswith(".promisor")
                and value_text not in {"false", "0"}
                or key_name.startswith("include")
            ):
                _fail("FILTER", "executable/indirect/partial-clone Git configuration unsupported")
        # Native Git boolean parsing preserves valueless=true, empty=false,
        # case-insensitive names and integer semantics; malformed values fail.
        try:
            booleans = self._git(
                root,
                "config",
                "--no-includes",
                "--null",
                "--type=bool",
                "--get-all",
                "extensions.worktreeconfig",
                allowed=(0, 1),
            ).split(b"\0")
        except SourcePreservationError:
            _fail("ENVIRONMENT", "invalid Git worktreeConfig Boolean")
        values = [value for value in booleans if value]
        if len(values) != extension_entries or any(
            value not in {b"true", b"false"} for value in values
        ):
            _fail("ENVIRONMENT", "inconsistent Git worktreeConfig Boolean entries")
        try:
            common_booleans = self._git(
                root,
                "config",
                "--no-includes",
                "--local",
                "--null",
                "--type=bool",
                "--get-all",
                "extensions.worktreeconfig",
                allowed=(0, 1),
            ).split(b"\0")
        except SourcePreservationError:
            _fail("ENVIRONMENT", "invalid common Git worktreeConfig Boolean")
        common_values = [value for value in common_booleans if value]
        if any(value not in {b"true", b"false"} for value in common_values):
            _fail("ENVIRONMENT", "invalid common Git worktreeConfig Boolean")
        if self._git(root, "for-each-ref", "--format=%(refname)", "refs/replace/"):
            _fail("ENVIRONMENT", "replacement refs are unsupported by the legacy guard boundary")
        observed_dir = Path(
            self._git(root, "rev-parse", "--absolute-git-dir").decode().strip()
        ).resolve()
        observed_common = Path(
            self._git(root, "rev-parse", "--path-format=absolute", "--git-common-dir")
            .decode()
            .strip()
        ).resolve()
        observed_root = Path(
            self._git(root, "rev-parse", "--show-toplevel").decode().strip()
        ).resolve()
        if (observed_dir, observed_common, observed_root) != (git_dir, common, root):
            _fail("DRIFT", "effective Git configuration redirects checkout identity")
        snapshot = {
            "checkout_root": root.as_posix(),
            "git_common_dir": common.as_posix(),
            "git_dir": git_dir.as_posix(),
            "files": files,
            # The common extension controls whether config.worktree is loaded.
            "worktree_config_mode": (
                "ABSENT" if not common_values else common_values[-1].decode().upper()
            ),
            "entry_count": count,
            "entries_sha256": _sha(raw),
        }
        self._recheck_configuration_files(snapshot)
        return snapshot

    def _recheck_configuration_files(self, expected: Mapping[str, Any]) -> None:
        root = Path(expected["checkout_root"])
        git_dir, common = _git_configuration_layout(root)
        files = [
            _configuration_file(common / "config", "COMMON_CONFIG"),
            _configuration_file(git_dir / "config.worktree", "WORKTREE_CONFIG"),
        ]
        if (
            git_dir.as_posix() != expected["git_dir"]
            or common.as_posix() != expected["git_common_dir"]
            or files != expected["files"]
        ):
            _fail("DRIFT", "Git configuration locator/presence/bytes changed")

    def _recheck_environment(self, root: Path, expected: Mapping[str, Any]) -> None:
        # Compare physical captures first, before invoking Git against changed
        # config. This catches absent-to-present and newly dangerous values alike.
        for role in ("trusted", "source"):
            self._recheck_configuration_files(expected[role])
        if self._environment(root) != expected:
            _fail("DRIFT", "execution Git configuration identity changed")

    def _git(
        self,
        root: Path,
        *args: str,
        content: bytes | None = None,
        index: Path | None = None,
        allowed: tuple[int, ...] = (0,),
        timestamp: str | None = None,
    ) -> bytes:
        return self._git_result(
            root, *args, content=content, index=index, allowed=allowed, timestamp=timestamp,
        ).stdout

    def _git_result(
        self,
        root: Path,
        *args: str,
        content: bytes | None = None,
        index: Path | None = None,
        allowed: tuple[int, ...] = (0,),
        timestamp: str | None = None,
    ) -> subprocess.CompletedProcess[bytes]:
        environment = {
            key: value for key, value in self._environment_source().items()
            if not key.upper().startswith("GIT_")
        }
        environment.update(
            {
                "GIT_CONFIG_NOSYSTEM": "1",
                "GIT_CONFIG_GLOBAL": os.devnull,
                "GIT_NO_REPLACE_OBJECTS": "1",
                "GIT_NO_LAZY_FETCH": "1",
                "GIT_TERMINAL_PROMPT": "0",
                "GIT_OPTIONAL_LOCKS": "0",
            }
        )
        if index is not None:
            environment["GIT_INDEX_FILE"] = str(index)
        if timestamp is not None:
            for role in ("AUTHOR", "COMMITTER"):
                environment[f"GIT_{role}_NAME"] = "Source preservation coordinator"
                environment[f"GIT_{role}_EMAIL"] = "source-preservation@localhost"
                environment[f"GIT_{role}_DATE"] = timestamp
        command = [
            self._git_executable,
            # Git config's automatic pager lookup can read includes separately
            # from --no-includes. Fix this before any repository config command.
            "--no-pager",
            "--no-replace-objects",
            "--no-optional-locks",
            "--no-lazy-fetch",
            "-c",
            f"safe.directory={root.as_posix()}",
            "-c",
            "core.fsmonitor=false",
            "-c",
            "diff.autoRefreshIndex=false",
            "-c",
            f"core.hooksPath={os.devnull}",
            "-c",
            "commit.gpgsign=false",
            "-c",
            "tag.gpgsign=false",
            "-c",
            "protocol.allow=never",
            "-c",
            "submodule.recurse=false",
            *args,
        ]
        try:
            result = subprocess.run(
                command,
                cwd=root,
                env=environment,
                input=content,
                capture_output=True,
                check=False,
                timeout=60,
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            _fail("GIT", type(exc).__name__)
        if result.returncode not in allowed:
            # Do not print arbitrary repository config, hook output, or input bytes.
            _fail("GIT", f"{args[0]} returned {result.returncode}")
        return result


class SourcePreservation(GitConfigurationAdmission):
    def __init__(self, project_root: Path, policy_path: Path = DEFAULT_POLICY_PATH) -> None:
        # Preserve the original source-preservation command contract. Standalone
        # trusted-launcher admission instead requires explicit executable input.
        self._git_executable = "git"
        self._git_environment = None
        self._held_context = None
        self.project_root = project_root.resolve(strict=True)
        self.policy_path = (
            policy_path if policy_path.is_absolute() else self.project_root / policy_path
        ).resolve(strict=True)
        policy_versions = {POLICY_PATH: "v1", V2_POLICY_PATH: "v2"}
        selected = next(
            (path for path in policy_versions if self.policy_path == self.project_root / path),
            None,
        )
        if selected is None:
            _fail("POLICY", "the exact reviewed policy locator is required")
        try:
            policy = _object(
                safe_load_yaml_text(_regular(self.policy_path).decode("utf-8")), _POLICY_KEYS
            )
        except (ValueError, UnicodeError) as exc:
            _fail("POLICY", type(exc).__name__)
        if (
            policy["schema_version"] != f"source_preservation_policy.{policy_versions[selected]}"
            or policy["status"] != "OWNER_APPROVED_ENFORCED"
            or policy["runtime_root"] != _RUNTIME
            or policy["ref_prefix"] != _REF_PREFIX
        ):
            _fail("POLICY", "unsupported policy contract")
        for field in ("policy_id", "version", "recovery_task_id", "owner_instruction_ref"):
            _text(policy[field])
        for field in ("max_files", "max_total_bytes"):
            if type(policy[field]) is not int or policy[field] <= 0:
                _fail("POLICY", "positive integer resource limit required")
        self.policy = policy
        self.protocol_version = policy["schema_version"].rsplit(".", 1)[1]
        # The fresh trusted coordinator process and installed interpreter/packages
        # are assumptions, not an in-memory attestation mechanism. Bind all project
        # modules used through the normal package initializers, not just this
        # facade: foreign-root or dirty helper implementations must fail closed.
        self._loaded_modules = _IMPLEMENTATION_MODULES

    def _implementation_binding(self) -> dict[str, Any]:
        if Path(__file__).resolve() != self.project_root / MODULE_PATH:
            _fail("IDENTITY", "loaded implementation root mismatch")
        head = self._git(self.project_root, "rev-parse", "--verify", "HEAD").decode().strip()
        records = []
        paths = {MODULE_PATH, CLI_PATH, self.policy_path.relative_to(self.project_root).as_posix()}
        for name in self._loaded_modules:
            module = sys.modules.get(name)
            origin = getattr(module, "__file__", None)
            if not isinstance(origin, str):
                _fail("IDENTITY", "project module has no physical source origin")
            path = Path(origin).resolve()
            expected = self.project_root / "src" / Path(*name.split("."))
            if path not in {expected.with_suffix(".py"), expected / "__init__.py"}:
                _fail("IDENTITY", "loaded project helper belongs to another checkout")
            paths.add(path.relative_to(self.project_root).as_posix())
        for relative in sorted(paths):
            content = _regular(_member(self.project_root, relative))
            committed = self._git(self.project_root, "cat-file", "blob", f"{head}:{relative}")
            if content.replace(b"\r\n", b"\n") != committed:
                _fail("IDENTITY", "trusted implementation is not exact committed source")
            records.append(
                {
                    "path": relative,
                    "sha256": _sha(content),
                    "git_blob_content_sha256": _sha(committed),
                }
            )
        return {
            "project_root": self.project_root.as_posix(),
            "commit": head,
            "files": records,
            "basis": "COMMITTED_SOURCE_GIT_EOL_LF",
        }

    def _request(self, request: Mapping[str, Any]) -> dict[str, Any]:
        result = _object(request, _REQUEST_KEYS)
        if result["schema_version"] != f"source_preservation_request.{self.protocol_version}":
            _fail("REQUEST", "unsupported request schema")
        identifier = _text(result["preservation_id"])
        if re.fullmatch(r"[a-z0-9][a-z0-9-]{0,95}", identifier) is None:
            _fail("REQUEST", "invalid preservation id")
        for field in ("recovery_task_id", "owner_instruction_ref"):
            if result[field] != self.policy[field]:
                _fail("REQUEST", f"{field} does not match reviewed policy")
        for field in ("source_task_id", "actor", "thread_id", "source_branch"):
            _text(result[field])
        if not result["source_branch"].startswith("codex/"):
            _fail("IDENTITY", "a non-protected codex source branch is required")
        for field in ("source_root", "source_common_git_dir"):
            if not Path(_text(result[field])).is_absolute():
                _fail("REQUEST", "absolute root required")
        for field in ("frozen_base_sha", "source_head_sha", "observed_main_sha"):
            _digest(result[field], 40)
        if result["observed_origin_main_sha"] is not None:
            _digest(result["observed_origin_main_sha"], 40)
        terminal = _object(result["terminal_transaction"], {"path", "sha256", "closeout_sha256"})
        _relative(terminal["path"])
        _digest(terminal["sha256"])
        _digest(terminal["closeout_sha256"])
        files = result["files"]
        if not isinstance(files, list) or not files or len(files) > self.policy["max_files"]:
            _fail("REQUEST", "invalid file count")
        paths = []
        total = 0
        mixed = self.protocol_version == "v2"
        for raw in files:
            keys = {"path", "sha256", "size_bytes", "git_mode"}
            if mixed:
                keys |= {"change", "base_git_mode", "base_git_oid"}
            row = _object(raw, keys)
            paths.append(_relative(row["path"]))
            change = row.get("change", "MODIFY")
            if mixed:
                if not isinstance(change, str) or change not in {"ADD", "MODIFY", "DELETE"}:
                    _fail("REQUEST", "unsupported source change")
                if change == "ADD":
                    if (row["base_git_mode"], row["base_git_oid"]) != ("000000", "0" * 40):
                        _fail("REQUEST", "addition must bind an absent baseline")
                    if row["git_mode"] != "100644":
                        _fail("REQUEST", "new source must use the regular non-executable mode")
                else:
                    if row["base_git_mode"] not in ("100644", "100755"):
                        _fail("REQUEST", "existing source requires a regular baseline")
                    if _digest(row["base_git_oid"], 40) == "0" * 40:
                        _fail("REQUEST", "existing source requires a nonzero baseline")
                if change == "MODIFY" and row["base_git_mode"] != row["git_mode"]:
                    _fail("REQUEST", "source mode changes are outside this contract")
                if change == "DELETE" and row["path"].casefold().startswith(
                    "registry/development_tasks/",
                ):
                    _fail("HISTORY", "canonical task history cannot be deleted")
            if change == "DELETE":
                if (
                    row["sha256"] is not None
                    or row["size_bytes"] != 0
                    or row["git_mode"] != "000000"
                ):
                    _fail("REQUEST", "deletion cannot claim after content")
            else:
                _digest(row["sha256"])
            if type(row["size_bytes"]) is not int or row["size_bytes"] < 0:
                _fail("REQUEST", "invalid byte size")
            total += row["size_bytes"]
            if change != "DELETE" and row["git_mode"] not in ("100644", "100755"):
                _fail("UNSUPPORTED_CHANGE", "unsupported Git mode")
        if paths != sorted(paths, key=str.casefold) or len(
            {path.casefold() for path in paths}
        ) != len(paths):
            _fail("REQUEST", "file paths must be unique and sorted")
        if mixed and any(
            path.casefold().startswith(other.casefold() + "/")
            for path in paths for other in paths if path != other
        ):
            _fail("REQUEST", "source paths cannot overlap a descendant")
        if total > self.policy["max_total_bytes"]:
            _fail("REQUEST", "aggregate capture budget exceeded; splitting is not automatic")
        return result

    def _terminal(
        self,
        request: dict[str, Any],
        root: Path,
        archived: Path | None = None,
        *,
        git_configuration: Mapping[str, Any] | None = None,
    ) -> tuple[IntegrationPublicationFence, dict[str, bytes]]:
        configuration = self._environment(root) if git_configuration is None else git_configuration
        self._recheck_environment(root, configuration)
        original_transaction = _member(root, request["terminal_transaction"]["path"])
        # Establish the original committed policies before a legacy constructor
        # can follow configuration-defined paths or apply exclusions. These are
        # the source checkout's policies, not copies of the recovery policy.
        captured: dict[str, bytes] = {}
        policy_paths: dict[str, Path] = {}
        for name, relative in _OLD_POLICIES.items():
            path = (
                _member(root, relative) if archived is None else archived / f"policies/{name}.yaml"
            )
            content = _regular(path)
            committed = self._git(
                root, "cat-file", "blob", f"{request['source_head_sha']}:{relative}"
            )
            if content.replace(b"\r\n", b"\n") != committed:
                _fail("TERMINAL", "original policy differs from exact source HEAD")
            captured[f"policies/{name}.yaml"] = content
            policy_paths[name] = path
        policy_value = _object(safe_load_yaml_text(captured["policies/fence.yaml"].decode("utf-8")))
        authority = _object(policy_value.get("authority"))
        if (
            authority.get("checkout_guard_policy") != _OLD_POLICIES["checkout"]
            or authority.get("parallel_control_policy") != _OLD_POLICIES["parallel"]
        ):
            _fail("TERMINAL", "original policy has unsupported helper locators")
        self._recheck_environment(root, configuration)
        if archived is None:
            fence = IntegrationPublicationFence(
                project_root=root, policy_path=policy_paths["fence"]
            )
            transaction_path = original_transaction
        else:
            fence = IntegrationPublicationFence(
                project_root=root,
                policy_path=policy_paths["fence"],
                checkout_guard_policy_path=policy_paths["checkout"],
                parallel_control_policy_path=policy_paths["parallel"],
            )
            transaction_path = archived / "transaction.json"
        for name, path in policy_paths.items():
            if _regular(path) != captured[f"policies/{name}.yaml"]:
                _fail("DRIFT", "original policy changed during legacy initialization")
        exclusions = tuple(row.path for row in fence.guard.policy.known_unrelated_exclusions)
        relative_transaction = request["terminal_transaction"]["path"]
        parts = PurePosixPath(relative_transaction).parts
        prefix = PurePosixPath(fence.policy.transaction_root).parts
        if (
            parts[: len(prefix)] != prefix
            or len(parts) != len(prefix) + 3
            or parts[-3] != "transactions"
            or parts[-1] != "transaction.json"
            or re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", parts[-2]) is None
            or any(_under(relative_transaction, path) for path in exclusions)
        ):
            _fail("PATH", "exact original publication transaction locator required")
        for row in request["files"]:
            if any(_under(row["path"], path) for path in exclusions):
                _fail("PATH", "excluded source path is never capturable")
        raw_transaction = _regular(transaction_path)
        raw_closeout = _regular(transaction_path.parent / "closeout_receipt.json")
        expected = request["terminal_transaction"]
        if (
            _sha(raw_transaction) != expected["sha256"]
            or _sha(raw_closeout) != expected["closeout_sha256"]
        ):
            _fail("TERMINAL", "terminal evidence digest mismatch")
        _load_json(transaction_path)
        closeout = _load_json(transaction_path.parent / "closeout_receipt.json")
        for path in sorted((transaction_path.parent / "events").glob("*.json")):
            _load_json(path)
            captured[f"events/{path.name}"] = _regular(path)
        replay = fence.replay(transaction_path)
        if replay.status != "PASS" or replay.phase != "FAILED":
            _fail("TERMINAL", "complete failed transaction replay required")
        transaction = replay.transaction
        expected_fields = {
            "task_id": request["source_task_id"],
            "actor": request["actor"],
            "lane_head_sha": request["source_head_sha"],
            "frozen_base_sha": request["frozen_base_sha"],
        }
        if any(transaction.get(key) != value for key, value in expected_fields.items()):
            _fail("TERMINAL", "transaction source identity mismatch")
        identity = _object(transaction.get("workspace_identity"))
        if (
            Path(str(identity.get("checkout_root"))).resolve() != root
            or Path(str(identity.get("git_common_dir"))).resolve()
            != Path(request["source_common_git_dir"]).resolve()
            or identity.get("branch_name") != request["source_branch"]
            or identity.get("head_commit") != request["source_head_sha"]
        ):
            _fail("TERMINAL", "transaction workspace mismatch")
        expected_closeout = {
            "schema_version": "integration_publication_closeout_receipt.v1",
            "status": "FAIL",
            "outcome": "FAILED",
            "final_phase": "FAILED",
            "transaction_id": transaction["transaction_id"],
            "transaction_sha256": transaction["transaction_sha256"],
            "lease_id": transaction["lease_id"],
            "lease_state": "RELEASED",
            "candidate_sha": replay.candidate_sha,
            "head_event_id": replay.events[-1]["event_id"],
            "production_effect": "none",
            "broker_action": "none",
        }
        if set(closeout) != set(expected_closeout) | {"evidence", "completed_at"} or any(
            closeout.get(key) != value for key, value in expected_closeout.items()
        ):
            _fail("TERMINAL", "closeout receipt does not bind complete terminal replay")
        final_payload = _object(replay.events[-1].get("payload"))
        if closeout.get("evidence") != final_payload.get("evidence") or closeout.get(
            "completed_at"
        ) != replay.events[-1].get("occurred_at"):
            _fail("TERMINAL", "closeout/event evidence or chronology mismatch")
        intent_id = "publication-" + transaction["transaction_id"]
        intent_locator = fence.guard.runtime_root / "intents" / f"{intent_id}.json"
        if Path(transaction["checkout_intent_path"]).resolve() != intent_locator.resolve():
            _fail("TERMINAL", "original checkout intent locator mismatch")
        intent_bytes = _regular(
            intent_locator if archived is None else archived / "checkout_intent.json"
        )
        intent = _object(load_strict_json_text(intent_bytes.decode("utf-8")))
        self._lease(
            fence,
            request,
            transaction["lease_id"],
            intent,
            task_id=request["source_task_id"],
            thread_id=transaction["thread_id"],
            owned=transaction["owned_paths"],
            shared=transaction["shared_paths"],
            expected_intent_id=intent_id,
            outcome="FAILED",
        )
        if intent["workspace_identity"] != identity:
            _fail("TERMINAL", "original intent/transaction workspace mismatch")
        declared = [*transaction["owned_paths"], *transaction["shared_paths"]]
        if any(
            not any(_under(row["path"], scope) for scope in declared) for row in request["files"]
        ):
            _fail("DIRTY_SCOPE", "source is outside terminated transaction scope")
        captured["transaction.json"] = raw_transaction
        captured["closeout_receipt.json"] = raw_closeout
        captured["checkout_intent.json"] = intent_bytes
        if archived is not None:
            for name in ("fence", "checkout", "parallel"):
                captured[f"policies/{name}.yaml"] = _regular(archived / f"policies/{name}.yaml")
        return fence, captured

    def _lease(
        self,
        fence: IntegrationPublicationFence,
        request: dict[str, Any],
        lease_id: str,
        raw_intent: dict[str, Any],
        *,
        task_id: str,
        thread_id: str,
        owned: list[str],
        shared: list[str],
        expected_intent_id: str,
        outcome: str,
    ) -> None:
        expected = {
            "schema_version": "checkout_operation_intent.v1",
            "intent_id": expected_intent_id,
            "task_id": task_id,
            "thread_id": thread_id,
            "actor": request["actor"],
            "operation_class": CheckoutOperationClass.SHARED_MUTATION.value,
            "base_commit": request["source_head_sha"],
            "owned_paths": sorted(owned),
            "shared_paths": sorted(shared),
            "task_source_cutover": False,
            "production_effect": "none",
            "broker_action": "none",
            "known_unrelated_exclusions": [
                row.to_dict() for row in fence.guard.policy.known_unrelated_exclusions
            ],
        }
        if set(raw_intent) != set(expected) | {
            "workspace_identity",
            "observed_dirty_paths",
            "created_at",
        } or any(raw_intent.get(key) != value for key, value in expected.items()):
            _fail("LEASE", "checkout intent is not exact task/actor/base/scope")
        identity = _object(
            raw_intent["workspace_identity"],
            {
                "workspace_id",
                "checkout_root",
                "git_common_dir",
                "head_commit",
                "branch_name",
                "upstream_ref",
                "upstream_commit",
            },
        )
        if (
            Path(identity["checkout_root"]).resolve() != Path(request["source_root"]).resolve()
            or Path(identity["git_common_dir"]).resolve()
            != Path(request["source_common_git_dir"]).resolve()
            or identity["head_commit"] != request["source_head_sha"]
            or identity["branch_name"] != request["source_branch"]
        ):
            _fail("LEASE", "checkout intent workspace identity mismatch")
        intent = CheckoutOperationIntent(
            intent_id=expected_intent_id,
            task_id=task_id,
            thread_id=thread_id,
            actor=request["actor"],
            operation_class=CheckoutOperationClass.SHARED_MUTATION,
            base_commit=request["source_head_sha"],
            owned_paths=tuple(sorted(owned)),
            shared_paths=tuple(sorted(shared)),
            workspace_identity=CheckoutIdentity(**identity),
            observed_dirty_paths=tuple(raw_intent["observed_dirty_paths"]),
            known_unrelated_exclusions=fence.guard.policy.known_unrelated_exclusions,
            created_at=datetime.fromisoformat(raw_intent["created_at"]),
        )
        task, _ = fence.guard._lease_task(intent)
        replay = fence.guard.replay()
        lease = {row.lease_id: row for row in replay.lease_heads}.get(lease_id)
        if (
            replay.status != "PASS"
            or lease is None
            or lease.state != "RELEASED"
            or lease.change_id != f"checkout:{expected_intent_id}"
            or lease.task_id != fence.guard.policy.authority_task_id
            or lease.actor != request["actor"]
            or lease.base_commit != request["source_head_sha"]
            or lease.change_manifest_sha256 != task.manifest.sha256
            or lease.lane_id != "checkout-shared-coordinator"
        ):
            _fail("LEASE", "complete released lease does not bind exact intent")
        head = dict(replay.head_event_ids)[lease_id]
        event = _load_json(fence.guard.store.events_root / lease_id / f"{head}.json")
        if (
            event.get("reason_codes") != [f"CHECKOUT_OPERATION_{outcome}"]
            or event.get("actor") != request["actor"]
            or event.get("to_state") != "RELEASED"
        ):
            _fail("LEASE", "lease terminal reason is not successful governed release")

    def _state(
        self, request: dict[str, Any], fence: IntegrationPublicationFence
    ) -> tuple[dict[str, Any], dict[str, bytes]]:
        root = Path(request["source_root"]).resolve(strict=True)
        configuration = self._environment(root)
        head = self._git(root, "rev-parse", "--verify", "HEAD").decode().strip()
        branch = self._git(root, "symbolic-ref", "--short", "HEAD").decode().strip()
        common = Path(
            self._git(root, "rev-parse", "--path-format=absolute", "--git-common-dir")
            .decode()
            .strip()
        ).resolve()
        top = Path(self._git(root, "rev-parse", "--show-toplevel").decode().strip()).resolve()
        main = self._git(root, "rev-parse", "--verify", "refs/heads/main").decode().strip()
        origin = (
            self._git(
                root, "rev-parse", "--verify", "--quiet", "refs/remotes/origin/main", allowed=(0, 1)
            )
            .decode()
            .strip()
            or None
        )
        if (
            top != root
            or head != request["source_head_sha"]
            or branch != request["source_branch"]
            or common != Path(request["source_common_git_dir"]).resolve()
            or main != request["observed_main_sha"]
            or origin != request["observed_origin_main_sha"]
        ):
            _fail("IDENTITY", "source checkout/ref identity mismatch")
        for descendant in (head, main):
            try:
                self._git(
                    root, "merge-base", "--is-ancestor", request["frozen_base_sha"], descendant
                )
            except SourcePreservationError:
                _fail("ANCESTRY", "frozen base is not a common ancestor")
        exclusions = [row.path for row in fence.guard.policy.known_unrelated_exclusions]
        exclusions += [_RUNTIME, fence.guard.policy.runtime_root]
        for row in request["files"]:
            attributes = self._git(
                root, "check-attr", "-z", "filter", "working-tree-encoding", "--", row["path"]
            ).split(b"\0")
            if any(value not in {b"unspecified", b"unset"} for value in attributes[2::3] if value):
                _fail("FILTER", "filter/encoding transform unsupported")
        pathspec = ["--", ".", *(f":(top,literal,exclude){path}" for path in exclusions)]
        staged_entries = self._git(root, "ls-files", "--stage", "-z", *pathspec)
        if any(entry.startswith(b"160000 ") for entry in staged_entries.split(b"\0")):
            _fail("UNSUPPORTED_CHANGE", "submodule execution is outside V1 preservation")
        status = self._git(
            root,
            "status",
            "--porcelain=v1",
            "-z",
            "--untracked-files=all",
            "--ignore-submodules=none",
            *pathspec,
        )
        dirty = []
        observed_status = {}
        mixed = self.protocol_version == "v2"
        for token in status.split(b"\0"):
            if not token:
                continue
            allowed_status = {b" M ", b" D ", b"?? "} if mixed else {b" M "}
            if token[:3] not in allowed_status:
                _fail(
                    "UNSUPPORTED_CHANGE",
                    "only unstaged regular modification/addition/deletion is supported"
                    if mixed else "only tracked unstaged regular modifications are supported",
                )
            dirty.append(token[3:].decode("utf-8"))
            observed_status[dirty[-1]] = token[:2].decode("ascii")
        expected_paths = [row["path"] for row in request["files"]]
        if sorted(dirty, key=str.casefold) != expected_paths:
            _fail("DIRTY_SCOPE", "exact non-excluded dirty set mismatch")
        captures = {}
        source_entries = self._tree_entries(root, head, expected_paths, allow_absent=mixed)
        for row in request["files"]:
            path = row["path"]
            mode, oid = source_entries[path]
            change = row.get("change", "MODIFY")
            if mixed and (
                (mode, oid) != (row["base_git_mode"], row["base_git_oid"])
                or observed_status[path] != {"ADD": "??", "MODIFY": " M", "DELETE": " D"}[change]
            ):
                _fail("DRIFT", "source baseline or change kind differs")
            if change == "MODIFY" and mode != row["git_mode"]:
                _fail("UNSUPPORTED_CHANGE", "Git file mode differs")
            if change == "DELETE":
                target = _member(root, path)
                try:
                    target.lstat()
                except FileNotFoundError:
                    continue
                _fail("DRIFT", "deleted source exists during capture")
            attributes = self._git(
                root, "check-attr", "-z", "filter", "working-tree-encoding", "--", path
            ).split(b"\0")
            if any(value not in {b"unspecified", b"unset"} for value in attributes[2::3] if value):
                _fail("FILTER", "filter/encoding transform unsupported")
            raw = _regular(_member(root, path), expected_size=row["size_bytes"])
            if len(raw) != row["size_bytes"] or _sha(raw) != row["sha256"]:
                _fail("DRIFT", "source bytes differ from request")
            captures[path] = raw
            self._history(root, head, path, raw, new_file=change == "ADD")
        index_path = Path(
            self._git(root, "rev-parse", "--path-format=absolute", "--git-path", "index")
            .decode()
            .strip()
        )
        index = _regular(index_path)
        refs = self._git(
            root,
            "for-each-ref",
            "--format=%(refname) %(objectname)",
            "refs/heads/",
            "refs/remotes/",
        )
        return {
            "head": head,
            "branch": branch,
            "main": main,
            "origin_main": origin,
            "common_git_dir": common.as_posix(),
            "dirty_paths": expected_paths,
            "index_sha256": _sha(index),
            "index_size_bytes": len(index),
            "branch_remote_refs_sha256": _sha(refs),
            "files": request["files"],
            "git_configuration": configuration,
        }, captures

    def _history(
        self, root: Path, head: str, path: str, content: bytes, *, new_file: bool = False,
    ) -> None:
        if not path.casefold().startswith("registry/development_tasks/"):
            return
        try:
            after = _object(safe_load_yaml_text(content.decode("utf-8")))
            validate_canonical_fragment(after)
            if new_file:
                identity = after["stable_task_identity"]
                digest = _sha(identity["task_id"].encode("utf-8"))
                if (
                    self.protocol_version != "v2"
                    or path != f"registry/development_tasks/{digest[:2]}/{digest}.yaml"
                    or after["events"][0]["event_type"] != "TASK_REGISTERED"
                ):
                    _fail("HISTORY", "new task history identity or initial event differs")
                return
            before = _object(
                safe_load_yaml_text(
                    self._git(root, "cat-file", "blob", f"{head}:{path}").decode("utf-8")
                )
            )
            validate_canonical_fragment(before)
            if (
                after["events"][: len(before["events"])] != before["events"]
                or before["stable_task_identity"] != after["stable_task_identity"]
            ):
                _fail("HISTORY", "previous task events were rewritten")
        except (ValueError, UnicodeError, KeyError) as exc:
            _fail("HISTORY", type(exc).__name__)

    def _tree_entry(
        self, root: Path, commit: str, path: str, *, allow_absent: bool = False,
    ) -> tuple[str, str]:
        return self._tree_entries(root, commit, [path], allow_absent=allow_absent)[path]

    def _tree_entries(
        self, root: Path, commit: str, paths: list[str], *, allow_absent: bool = False,
    ) -> dict[str, tuple[str, str]]:
        """Read declared metadata in bounded batches, without cross-check caching."""
        result: dict[str, tuple[str, str]] = {}
        for start in range(0, len(paths), 16):
            batch = paths[start:start + 16]
            expected = set(batch)
            raw = self._git(root, "ls-tree", "-z", commit, "--", *batch)
            for entry in raw.split(b"\0"):
                if not entry:
                    continue
                metadata, found = entry.split(b"\t", 1)
                path = found.decode("utf-8")
                mode, kind, oid = metadata.decode().split()
                if (
                    path not in expected or path in result
                    or kind != "blob" or mode not in {"100644", "100755"}
                ):
                    _fail("UNSUPPORTED_CHANGE", "unique declared regular Git blob required")
                result[path] = mode, _digest(oid, 40)
            for path in batch:
                if path not in result:
                    if not allow_absent:
                        _fail("UNSUPPORTED_CHANGE", "existing unique tracked file required")
                    result[path] = "000000", "0" * 40
        return result

    def inspect_migration_source(self, request: Mapping[str, Any]) -> dict[str, Any]:
        """Read-only admission for an explicitly approved legacy arbiter migration.

        This reuses the same published-code, terminal, checkout and exact dirty
        source checks; it never acquires a lease, captures a snapshot or writes a
        receipt. The migration caller still needs its own live coordinator fence
        and explicit quiescence evidence. See DEVX-014 S1a.
        """
        checked = self._request(request)
        root = Path(checked["source_root"]).resolve(strict=True)
        configuration = self._environment(root)
        implementation = self._implementation_binding()
        fence, terminal = self._terminal(checked, root, git_configuration=configuration)
        before, _ = self._state(checked, fence)
        self._recheck_environment(root, configuration)
        if before["git_configuration"] != configuration:
            _fail("DRIFT", "Git configuration changed during source inspection")
        leases = fence.guard.replay()
        if leases.status != "PASS" or leases.active_leases:
            _fail("LEASE", "migration source must have no active logical lease")
        return {
            "source_root": root.as_posix(),
            "store_root": fence.guard.store.root.as_posix(),
            "source_task_id": checked["source_task_id"],
            "request_sha256": _sha(_json_bytes(checked)),
            "terminal_sha256": _sha(terminal["transaction.json"]),
            "source_state": before,
            "source_lease_replay": leases.to_dict(),
            "implementation": implementation,
            "production_effect": "none",
            "broker_action": "none",
        }

    def preserve(self, request: Mapping[str, Any]) -> dict[str, Any]:
        checked = self._request(request)
        root = Path(checked["source_root"]).resolve(strict=True)
        configuration = self._environment(root)
        implementation = self._implementation_binding()
        run = _member(root, f"{_RUNTIME}/{checked['preservation_id']}")
        receipt_path = run / "receipt.json"
        if run.exists():
            if (run / "failure.json").exists() or not receipt_path.is_file():
                _fail("PARTIAL", "incomplete preservation already exists")
            existing = _load_json(receipt_path)
            if existing.get("request_sha256") != _sha(_json_bytes(checked)):
                _fail("REF_EXISTS", "same id has different request bytes")
            self.validate(receipt_path)
            return existing
        handle: CheckoutLeaseHandle | None = None
        events: list[dict[str, Any]] = []
        owned_run = False
        try:
            fence, terminal = self._terminal(checked, root, git_configuration=configuration)
            before, captures = self._state(checked, fence)
            if before["git_configuration"] != configuration:
                _fail("DRIFT", "Git configuration changed before source capture")
            ref = _REF_PREFIX + checked["preservation_id"]
            if self._git(root, "rev-parse", "--verify", "--quiet", ref, allowed=(0, 1)):
                _fail("REF_EXISTS", "create-only source ref already exists")
            self._recheck_environment(root, configuration)
            decision, handle = fence.guard.acquire(
                intent_id="source-preservation-" + uuid4().hex,
                task_id=checked["recovery_task_id"],
                thread_id=checked["thread_id"],
                actor=checked["actor"],
                operation_class=CheckoutOperationClass.SHARED_MUTATION,
                shared_paths=tuple([row["path"] for row in checked["files"]] + [_RUNTIME]),
                base_commit=checked["source_head_sha"],
            )
            if decision.status != "PASS" or handle is None:
                _fail("LEASE", "shared source preservation lease unavailable")
            self._recheck_environment(root, configuration)
            run.mkdir(parents=True, exist_ok=False)
            owned_run = True
            self._event(run, events, "ACQUIRED", {"lease_id": handle.lease_id})
            _write_once(run / "request.json", _json_bytes(checked))
            intent_bytes = _regular(decision.intent_path)
            _write_once(run / "checkout_intent.json", intent_bytes)
            index_path = Path(
                self._git(root, "rev-parse", "--path-format=absolute", "--git-path", "index")
                .decode()
                .strip()
            )
            original_index = _regular(index_path)
            original_refs = self._git(
                root,
                "for-each-ref",
                "--format=%(refname) %(objectname)",
                "refs/heads/",
                "refs/remotes/",
            )
            if (
                _sha(original_index) != before["index_sha256"]
                or _sha(original_refs) != before["branch_remote_refs_sha256"]
            ):
                _fail("DRIFT", "source metadata changed before evidence capture")
            _write_once(run / "source.index", original_index)
            _write_once(run / "source_refs.txt", original_refs)
            for relative, content in terminal.items():
                _write_once(_member(run / "terminal", relative), content)
            after_acquire, captures_again = self._state(checked, fence)
            if before != after_acquire or captures != captures_again:
                _fail("DRIFT", "source changed during lease acquisition")
            self._event(run, events, "CAPTURED", {"source_state": before})
            self._recheck_environment(root, configuration)
            self._active(handle)
            index = run / "private.index"
            self._recheck_environment(root, configuration)
            self._git(root, "read-tree", checked["source_head_sha"], index=index)
            blobs = []
            for row in checked["files"]:
                if row.get("change") == "DELETE":
                    self._git(
                        root, "update-index", "--force-remove", "--", row["path"], index=index,
                    )
                    blobs.append({**row, "blob_oid": "0" * 40, "blob_content_sha256": None})
                    continue
                content = captures[row["path"]]
                oid = (
                    self._git(root, "hash-object", "-w", "--stdin", "--no-filters", content=content)
                    .decode()
                    .strip()
                )
                _digest(oid, 40)
                self._git(
                    root,
                    "update-index",
                    *(["--add"] if row.get("change") == "ADD" else []),
                    "--cacheinfo",
                    row["git_mode"],
                    oid,
                    row["path"],
                    index=index,
                )
                blobs.append({**row, "blob_oid": oid, "blob_content_sha256": _sha(content)})
            tree = self._git(root, "write-tree", index=index).decode().strip()
            timestamp = datetime.now(UTC).isoformat()
            commit = (
                self._git(
                    root,
                    "commit-tree",
                    tree,
                    "-p",
                    checked["source_head_sha"],
                    content=(
                        f"Source-only preservation {checked['preservation_id']}\n\n"
                        f"request_sha256={_sha(_json_bytes(checked))}\n"
                    ).encode(),
                    timestamp=timestamp,
                )
                .decode()
                .strip()
            )
            snapshot = {
                "commit": _digest(commit, 40),
                "parent": checked["source_head_sha"],
                "tree": _digest(tree, 40),
                "ref": ref,
                "files": blobs,
            }
            self._verify_snapshot(root, checked, snapshot, fence)
            self._event(run, events, "OBJECTS_WRITTEN", {"snapshot": snapshot})
            self._unchanged(checked, fence, before, implementation)
            if self._terminal(checked, root, git_configuration=configuration)[1] != terminal:
                _fail("DRIFT", "original terminal evidence changed before ref creation")
            self._recheck_environment(root, configuration)
            self._active(handle)
            self._recheck_environment(root, configuration)
            self._git(root, "update-ref", ref, commit, "0" * 40)
            self._event(run, events, "REF_CREATED", {"ref": ref, "commit": commit})
            self._verify_snapshot(root, checked, snapshot, fence, require_ref=True)
            self._unchanged(checked, fence, before, implementation)
            self._event(run, events, "VERIFIED", {"source_state": before})
            lease_id = handle.lease_id
            self._recheck_environment(root, configuration)
            handle.release(outcome="completed", evidence_refs=(str(run / "events"),))
            handle = None
            self._unchanged(checked, fence, before, implementation)
            self._event(run, events, "RELEASED", {"lease_id": lease_id, "lease_state": "RELEASED"})
            body = {
                "schema_version": f"source_preservation_receipt.{self.protocol_version}",
                "status": "PASS",
                "preservation_id": checked["preservation_id"],
                "request_sha256": _sha(_json_bytes(checked)),
                "receipt_path": receipt_path.as_posix(),
                "implementation": implementation,
                "policy_sha256": _sha(_regular(self.policy_path)),
                "lease_id": lease_id,
                "checkout_intent_sha256": _sha(intent_bytes),
                "source_state_before": before,
                "source_state_after": before,
                "snapshot": snapshot,
                "terminal_evidence": [
                    {"path": relative, "sha256": _sha(content), "size_bytes": len(content)}
                    for relative, content in sorted(terminal.items())
                ],
                "head_event_id": events[-1]["event_id"],
                "safety": dict(_SAFETY),
            }
            receipt = {**body, "receipt_sha256": _sha(_json_bytes(body))}
            _write_once(receipt_path, _json_bytes(receipt))
            self.validate(receipt_path)
            return receipt
        except BaseException as exc:
            failed_lease_id = handle.lease_id if handle is not None else None
            disposition = "NO_HELD_LEASE"
            release_error_code: str | None = None
            if handle is not None:
                try:
                    # Do not reach even cleanup's legacy Git helpers through a
                    # changed context. Preserve the original error and journal.
                    self._recheck_environment(root, configuration)
                except Exception as cleanup_error:
                    disposition = "DEFERRED_CONFIGURATION_RECHECK_FAILED"
                    release_error_code = getattr(
                        cleanup_error, "code", type(cleanup_error).__name__
                    )
                else:
                    try:
                        handle.release(outcome="failed")
                    except Exception as cleanup_error:
                        # A rejecting release may have appended terminal events;
                        # do not invent a resulting logical lease state here.
                        disposition = "RELEASE_REJECTED_STATE_NOT_ASSERTED"
                        release_error_code = getattr(
                            cleanup_error, "code", type(cleanup_error).__name__
                        )
                    else:
                        disposition = "RELEASED"
            if owned_run and not (run / "failure.json").exists():
                failure = {
                    "schema_version": f"source_preservation_failure.{self.protocol_version}",
                    "status": "FAIL",
                    "request_sha256": _sha(_json_bytes(checked)),
                    "completed_phases": [event["phase"] for event in events],
                    "error_code": getattr(exc, "code", type(exc).__name__),
                    "lease_id": failed_lease_id,
                    "release_disposition": disposition,
                    "release_error_code": release_error_code,
                    "safety": dict(_SAFETY),
                }
                try:
                    _write_once(run / "failure.json", _json_bytes(failure))
                except (OSError, SourcePreservationError):
                    pass  # Original cause survives evidence-write failure.
            if isinstance(exc, SourcePreservationError):
                raise
            if isinstance(exc, (CheckoutGuardError, PublicationFenceError)):
                _fail("LEASE" if isinstance(exc, CheckoutGuardError) else "TERMINAL", exc.code)
            if isinstance(exc, Exception):
                _fail("PARTIAL", type(exc).__name__)
            raise

    def _active(self, handle: CheckoutLeaseHandle) -> None:
        handle.heartbeat()
        replay = handle.guard.replay()
        lease = {row.lease_id: row for row in replay.active_leases}.get(handle.lease_id)
        if (
            replay.status != "PASS"
            or lease is None
            or lease.actor != handle.actor
            or lease.change_id != "checkout:" + handle.decision.intent.intent_id
            or lease.expires_at is None
            or datetime.fromisoformat(lease.expires_at) <= datetime.now(UTC)
        ):
            _fail("LEASE", "source preservation lease is not active")

    def _unchanged(
        self,
        request: dict[str, Any],
        fence: IntegrationPublicationFence,
        before: dict[str, Any],
        implementation: dict[str, Any],
    ) -> None:
        self._recheck_environment(Path(request["source_root"]), before["git_configuration"])
        if (
            self._state(request, fence)[0] != before
            or self._implementation_binding() != implementation
        ):
            _fail("DRIFT", "source/index/refs or trusted implementation changed")

    def _event(
        self, run: Path, events: list[dict[str, Any]], phase: str, payload: dict[str, Any]
    ) -> None:
        if len(events) >= len(_PHASES) or phase != _PHASES[len(events)]:
            _fail("RECEIPT", "invalid preservation phase")
        body = {
            "schema_version": f"source_preservation_event.{self.protocol_version}",
            "sequence": len(events) + 1,
            "phase": phase,
            "previous_event_id": events[-1]["event_id"] if events else None,
            "occurred_at": datetime.now(UTC).isoformat(),
            "payload": payload,
        }
        event = {**body, "event_id": _sha(_json_bytes(body))}
        _write_once(run / "events" / f"{len(events) + 1:04d}_{phase}.json", _json_bytes(event))
        events.append(event)

    def _verify_snapshot(
        self,
        root: Path,
        request: dict[str, Any],
        snapshot: dict[str, Any],
        fence: IntegrationPublicationFence,
        *,
        require_ref: bool = False,
    ) -> None:
        _object(snapshot, {"commit", "parent", "tree", "ref", "files"})
        commit = _digest(snapshot["commit"], 40)
        parents = self._git(root, "rev-list", "--parents", "-n", "1", commit).decode().split()
        tree = self._git(root, "rev-parse", f"{commit}^{{tree}}").decode().strip()
        if (
            parents != [commit, request["source_head_sha"]]
            or snapshot["parent"] != request["source_head_sha"]
            or snapshot["tree"] != tree
            or snapshot["ref"] != _REF_PREFIX + request["preservation_id"]
        ):
            _fail("RECEIPT", "snapshot parent/tree/ref mismatch")
        exclusions = [row.path for row in fence.guard.policy.known_unrelated_exclusions]
        changed = self._git(
            root,
            "diff-tree",
            "--no-commit-id",
            "--no-ext-diff",
            "--no-textconv",
            "--no-renames",
            "--name-only",
            "-r",
            "-z",
            snapshot["parent"],
            commit,
            "--",
            ".",
            *(f":(top,literal,exclude){path}" for path in exclusions),
        )
        paths = sorted(
            (path.decode("utf-8") for path in changed.split(b"\0") if path), key=str.casefold
        )
        if paths != [row["path"] for row in request["files"]]:
            _fail("RECEIPT", "snapshot delta is not exact source scope")
        if len(snapshot["files"]) != len(request["files"]):
            _fail("RECEIPT", "snapshot member count mismatch")
        mixed = self.protocol_version == "v2"
        declared_paths = [row["path"] for row in request["files"]]
        baseline_entries = self._tree_entries(
            root, request["source_head_sha"], declared_paths, allow_absent=True,
        ) if mixed else {}
        snapshot_entries = self._tree_entries(root, commit, declared_paths, allow_absent=mixed)
        for expected, actual in zip(request["files"], snapshot["files"], strict=True):
            row = _object(
                actual,
                set(expected) | {"blob_oid", "blob_content_sha256"},
            )
            if any(row[key] != value for key, value in expected.items()):
                _fail("RECEIPT", "snapshot file binding mismatch")
            change = row.get("change", "MODIFY")
            if mixed and baseline_entries[row["path"]] != (
                row["base_git_mode"], row["base_git_oid"],
            ):
                _fail("RECEIPT", "snapshot baseline binding differs")
            mode, oid = snapshot_entries[row["path"]]
            if change == "DELETE":
                if (
                    (mode, oid) != ("000000", "0" * 40)
                    or row["blob_oid"] != "0" * 40
                    or row["blob_content_sha256"] is not None
                ):
                    _fail("RECEIPT", "deleted snapshot member is present or claims content")
                continue
            size = int(self._git(root, "cat-file", "-s", oid).decode().strip())
            if size != row["size_bytes"]:
                _fail("RECEIPT", "snapshot blob size differs before capture")
            content = self._git(root, "cat-file", "blob", oid)
            if (
                mode != row["git_mode"]
                or oid != row["blob_oid"]
                or len(content) != row["size_bytes"]
                or _sha(content) != row["sha256"]
                or row["blob_content_sha256"] != row["sha256"]
            ):
                _fail("RECEIPT", "raw snapshot bytes mismatch")
            self._history(
                root, request["source_head_sha"], row["path"], content, new_file=change == "ADD",
            )
        replacements = {
            row["path"]: (row["git_mode"], row["blob_oid"]) for row in snapshot["files"]
        }
        parent_tree = (
            self._git(root, "rev-parse", f"{request['source_head_sha']}^{{tree}}").decode().strip()
        )
        if self._expected_tree(
            root, parent_tree, replacements, allow_path_changes=self.protocol_version == "v2",
        ) != tree:
            _fail("RECEIPT", "snapshot includes an undeclared tree change")
        if (
            require_ref
            and self._git(root, "rev-parse", "--verify", snapshot["ref"]).decode().strip() != commit
        ):
            _fail("RECEIPT", "preservation ref drift")

    def _expected_tree(
        self, root: Path, tree: str | None, replacements: dict[str, tuple[str, str]],
        *, allow_path_changes: bool = False,
    ) -> str:
        """Recompute only changed tree nodes, never read/hash undeclared blobs.

        Git V1 is SHA-1; object headers and untouched entry OIDs are metadata.
        Validation does not write Git objects or a temporary index. V1 callers
        retain replacement-only semantics. Explicit path-change verification is
        a tree oracle, not permission to capture a wider source request.
        """
        if tree is None and not allow_path_changes:
            _fail("RECEIPT", "replacement path absent in original parent tree")
        raw = b"" if tree is None else self._git(root, "cat-file", "tree", tree)
        entries: dict[str, tuple[bytes, bytes]] = {}
        cursor = 0
        while cursor < len(raw):
            nul = raw.index(b"\0", cursor)
            mode, name_bytes = raw[cursor:nul].split(b" ", 1)
            oid_bytes = raw[nul + 1 : nul + 21]
            if len(oid_bytes) != 20:
                _fail("RECEIPT", "unsupported Git tree object format")
            name = name_bytes.decode("utf-8")
            if name in entries:
                _fail("RECEIPT", "duplicate original tree entry")
            entries[name] = mode, oid_bytes
            cursor = nul + 21
        groups: dict[str, dict[str, tuple[str, str]]] = {}
        for path, row in replacements.items():
            _relative(path)
            name, separator, tail = path.partition("/")
            groups.setdefault(name, {})[tail if separator else ""] = row
        empty_tree = hashlib.sha1(b"tree 0\0").hexdigest()
        for name, changes in groups.items():
            original = entries.get(name)
            if original is None and not allow_path_changes:
                _fail("RECEIPT", "replacement path absent in original parent tree")
            if name in replacements:
                if len(changes) != 1:
                    _fail("RECEIPT", "replacement path overlaps a descendant")
                expected_mode, replacement_oid = replacements[name]
                if expected_mode == "000000" and allow_path_changes:
                    if original is None or original[0] not in {b"100644", b"100755"}:
                        _fail("RECEIPT", "deletion requires an existing regular blob")
                    if replacement_oid != "0" * 40:
                        _fail("RECEIPT", "deletion cannot claim a blob")
                    del entries[name]
                    continue
                if expected_mode not in {"100644", "100755"}:
                    _fail("RECEIPT", "replacement requires a regular blob")
                if original is not None and original[0].decode() != expected_mode:
                    _fail("RECEIPT", "snapshot changes an existing file mode")
                oid = bytes.fromhex(_digest(replacement_oid, 40))
                if oid == b"\0" * 20:
                    _fail("RECEIPT", "regular blob cannot have a zero identity")
                entries[name] = expected_mode.encode("ascii"), oid
            else:
                if original is not None and original[0] != b"40000":
                    _fail("RECEIPT", "replacement ancestor is not a tree")
                nested_tree = self._expected_tree(
                    root, None if original is None else original[1].hex(), changes,
                    allow_path_changes=allow_path_changes,
                )
                if allow_path_changes and nested_tree == empty_tree:
                    entries.pop(name, None)
                else:
                    entries[name] = b"40000", bytes.fromhex(nested_tree)
        result = bytearray()
        # Git sorts directories as name + slash, regular entries as name + NUL.
        # Ordinary string sorting gets adjacent names such as a.c and a/x wrong.
        for name, (mode, oid) in sorted(
            entries.items(),
            key=lambda item: item[0].encode("utf-8")
            + (b"/" if item[1][0] == b"40000" else b"\0"),
        ):
            result.extend(mode + b" " + name.encode("utf-8") + b"\0" + oid)
        return hashlib.sha1(b"tree " + str(len(result)).encode() + b"\0" + result).hexdigest()

    def validate(self, receipt_path: Path) -> dict[str, Any]:
        try:
            return self._validate(receipt_path)
        except SourcePreservationError:
            raise
        except (OSError, ValueError, TypeError, KeyError, AttributeError) as exc:
            _fail("RECEIPT", f"invalid preservation evidence: {type(exc).__name__}")

    def _validate(self, receipt_path: Path) -> dict[str, Any]:
        # Reject foreign/poison locators before opening any bytes.
        parts = receipt_path.parts
        if (
            not receipt_path.is_absolute()
            or len(parts) < 6
            or tuple(parts[-5:-2]) != PurePosixPath(_RUNTIME).parts
            or parts[-1] != "receipt.json"
            or re.fullmatch(r"[a-z0-9][a-z0-9-]{0,95}", parts[-2]) is None
        ):
            _fail("PATH", "exact source runtime receipt locator required")
        run = receipt_path.resolve().parent
        if (run / "failure.json").exists():
            _fail("PARTIAL", "failed preservation cannot be replayed as success")
        receipt = _load_json(receipt_path)
        if set(receipt) != {
            "schema_version",
            "status",
            "preservation_id",
            "request_sha256",
            "receipt_path",
            "implementation",
            "policy_sha256",
            "lease_id",
            "checkout_intent_sha256",
            "source_state_before",
            "source_state_after",
            "snapshot",
            "terminal_evidence",
            "head_event_id",
            "safety",
            "receipt_sha256",
        }:
            _fail("RECEIPT", "unexpected or missing receipt fields")
        body = dict(receipt)
        checksum = body.pop("receipt_sha256", None)
        if (
            checksum != _sha(_json_bytes(body))
            or receipt.get("schema_version")
            != f"source_preservation_receipt.{self.protocol_version}"
        ):
            _fail("RECEIPT", "receipt checksum/schema mismatch")
        if receipt.get("status") != "PASS" or receipt.get("safety") != _SAFETY:
            _fail("RECEIPT", "only successful source-only evidence is valid")
        request = self._request(_load_json(run / "request.json"))
        root = Path(request["source_root"]).resolve(strict=True)
        configuration = self._environment(root)
        expected_path = _member(root, f"{_RUNTIME}/{request['preservation_id']}/receipt.json")
        if (
            receipt_path.resolve() != expected_path
            or receipt.get("receipt_path") != expected_path.as_posix()
        ):
            _fail("RECEIPT", "receipt locator mismatch")
        if (
            receipt.get("request_sha256") != _sha(_json_bytes(request))
            or receipt.get("preservation_id") != request["preservation_id"]
        ):
            _fail("RECEIPT", "request binding mismatch")
        if receipt.get("policy_sha256") != _sha(_regular(self.policy_path)):
            _fail("POLICY", "reviewed policy changed")
        if receipt.get("implementation") != self._implementation_binding():
            _fail("IDENTITY", "validator must use the same trusted implementation identity")
        fence, terminal = self._terminal(
            request, root, archived=run / "terminal", git_configuration=configuration
        )
        expected_terminal = [
            {"path": relative, "sha256": _sha(content), "size_bytes": len(content)}
            for relative, content in sorted(terminal.items())
        ]
        if receipt["terminal_evidence"] != expected_terminal:
            _fail("TERMINAL", "archived terminal exact member/hash binding changed")
        prior = None
        event_paths = sorted((run / "events").glob("*.json"))
        if len(event_paths) != len(_PHASES):
            _fail("RECEIPT", "complete event sequence required")
        events = []
        prior_time: datetime | None = None
        for sequence, (path, phase) in enumerate(zip(event_paths, _PHASES, strict=True), start=1):
            event = _load_json(path)
            if (
                set(event)
                != {
                    "schema_version",
                    "sequence",
                    "phase",
                    "previous_event_id",
                    "occurred_at",
                    "payload",
                    "event_id",
                }
                or event["schema_version"]
                != f"source_preservation_event.{self.protocol_version}"
                or path.name != f"{sequence:04d}_{phase}.json"
            ):
                _fail("RECEIPT", "invalid exact event schema/locator")
            try:
                event_time = datetime.fromisoformat(event["occurred_at"])
                if event_time.tzinfo is None or prior_time is not None and event_time < prior_time:
                    _fail("RECEIPT", "event chronology mismatch")
            except (ValueError, TypeError):
                _fail("RECEIPT", "invalid event timestamp")
            prior_time = event_time
            event_body = dict(event)
            event_id = event_body.pop("event_id", None)
            if (
                event_id != _sha(_json_bytes(event_body))
                or event.get("phase") != phase
                or event.get("sequence") != sequence
                or event.get("previous_event_id") != prior
            ):
                _fail("RECEIPT", "event chain mismatch")
            prior = event_id
            events.append(event)
        if (
            prior != receipt.get("head_event_id")
            or events[1]["payload"].get("source_state") != receipt.get("source_state_before")
            or events[2]["payload"].get("snapshot") != receipt.get("snapshot")
            or events[4]["payload"].get("source_state") != receipt.get("source_state_after")
            or receipt.get("source_state_before") != receipt.get("source_state_after")
            or events[0]["payload"].get("lease_id") != receipt.get("lease_id")
            or events[5]["payload"].get("lease_id") != receipt.get("lease_id")
        ):
            _fail("RECEIPT", "event/receipt fact mismatch")
        expected_payloads = [
            {"lease_id": receipt["lease_id"]},
            {"source_state": receipt["source_state_before"]},
            {"snapshot": receipt["snapshot"]},
            {"ref": receipt["snapshot"]["ref"], "commit": receipt["snapshot"]["commit"]},
            {"source_state": receipt["source_state_after"]},
            {"lease_id": receipt["lease_id"], "lease_state": "RELEASED"},
        ]
        if [event["payload"] for event in events] != expected_payloads:
            _fail("RECEIPT", "phase-specific payload mismatch")
        state = receipt["source_state_before"]
        expected_state = {
            "head": request["source_head_sha"],
            "branch": request["source_branch"],
            "main": request["observed_main_sha"],
            "origin_main": request["observed_origin_main_sha"],
            "common_git_dir": Path(request["source_common_git_dir"]).resolve().as_posix(),
            "dirty_paths": [row["path"] for row in request["files"]],
            "files": request["files"],
        }
        if (
            not isinstance(state, dict)
            or set(state)
            != set(expected_state)
            | {"index_sha256", "index_size_bytes", "branch_remote_refs_sha256", "git_configuration"}
            or any(state.get(key) != value for key, value in expected_state.items())
        ):
            _fail("RECEIPT", "captured state does not bind exact request")
        try:
            _configuration_evidence(
                state["git_configuration"],
                trusted_root=self.project_root,
                source_root=root,
                source_common=Path(request["source_common_git_dir"]).resolve(),
            )
        except SourcePreservationError:
            _fail("RECEIPT", "invalid retained Git configuration summary")
        original_index = _regular(run / "source.index")
        original_refs = _regular(run / "source_refs.txt")
        if (
            _sha(original_index) != state["index_sha256"]
            or len(original_index) != state["index_size_bytes"]
            or _sha(original_refs) != state["branch_remote_refs_sha256"]
        ):
            _fail("RECEIPT", "captured original index/ref evidence mismatch")
        refs = dict(line.split(" ", 1) for line in original_refs.decode("utf-8").splitlines())
        if (
            refs.get("refs/heads/" + request["source_branch"]) != request["source_head_sha"]
            or refs.get("refs/heads/main") != request["observed_main_sha"]
            or refs.get("refs/remotes/origin/main") != request["observed_origin_main_sha"]
        ):
            _fail("RECEIPT", "captured ref metadata contradicts request")
        intent_bytes = _regular(run / "checkout_intent.json")
        if _sha(intent_bytes) != receipt["checkout_intent_sha256"]:
            _fail("LEASE", "captured preservation intent changed")
        intent = _object(load_strict_json_text(intent_bytes.decode("utf-8")))
        intent_id = _text(intent.get("intent_id"))
        if re.fullmatch(r"source-preservation-[0-9a-f]{32}", intent_id) is None:
            _fail("LEASE", "invalid source-preservation attempt intent")
        original_intent_path = fence.guard.runtime_root / "intents" / f"{intent_id}.json"
        if _regular(original_intent_path) != intent_bytes:
            _fail("LEASE", "captured intent differs from original lease evidence")
        self._lease(
            fence,
            request,
            receipt["lease_id"],
            intent,
            task_id=request["recovery_task_id"],
            thread_id=request["thread_id"],
            owned=[],
            shared=[row["path"] for row in request["files"]] + [_RUNTIME],
            expected_intent_id=intent_id,
            outcome="COMPLETED",
        )
        self._verify_snapshot(root, request, receipt["snapshot"], fence, require_ref=True)
        self._recheck_environment(root, configuration)
        return {
            "schema_version": f"source_preservation_validation.{self.protocol_version}",
            "status": "PASS",
            "receipt_path": expected_path.as_posix(),
            "snapshot_commit": receipt["snapshot"]["commit"],
            "ref": receipt["snapshot"]["ref"],
            "safety": dict(_SAFETY),
        }
