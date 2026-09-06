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
from collections.abc import Mapping
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


class SourcePreservation:
    def __init__(self, project_root: Path, policy_path: Path = DEFAULT_POLICY_PATH) -> None:
        self.project_root = project_root.resolve(strict=True)
        self.policy_path = (
            policy_path if policy_path.is_absolute() else self.project_root / policy_path
        ).resolve(strict=True)
        if self.policy_path != self.project_root / POLICY_PATH:
            _fail("POLICY", "V1 requires the exact reviewed policy locator")
        try:
            policy = _object(
                safe_load_yaml_text(_regular(self.policy_path).decode("utf-8")), _POLICY_KEYS
            )
        except (ValueError, UnicodeError) as exc:
            _fail("POLICY", type(exc).__name__)
        if (
            policy["schema_version"] != "source_preservation_policy.v1"
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
        # The fresh trusted coordinator process and installed interpreter/packages
        # are assumptions, not an in-memory attestation mechanism. Bind all project
        # modules used through the normal package initializers, not just this
        # facade: foreign-root or dirty helper implementations must fail closed.
        self._loaded_modules = _IMPLEMENTATION_MODULES

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
        if any(os.environ.get(key) != value for key, value in required.items()):
            _fail("ENVIRONMENT", "fresh child requires fixed read-only Git environment")
        allowed = set(required)
        count = os.environ.get("GIT_CONFIG_COUNT")
        if count is not None:
            if re.fullmatch(r"[0-9]+", count) is None or int(count) > 2:
                _fail("ENVIRONMENT", "only exact safe.directory entries are allowed")
            allowed.add("GIT_CONFIG_COUNT")
            roots = {self.project_root, root}
            for number in range(int(count)):
                key = f"GIT_CONFIG_KEY_{number}"
                value = f"GIT_CONFIG_VALUE_{number}"
                if (
                    os.environ.get(key) != "safe.directory"
                    or not os.environ.get(value)
                    or Path(os.environ[value]).resolve() not in roots
                ):
                    _fail("ENVIRONMENT", "foreign Git configuration entry")
                allowed.update((key, value))
        if any(key.upper().startswith("GIT_") and key not in allowed for key in os.environ):
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
        environment = {
            key: value for key, value in os.environ.items() if not key.upper().startswith("GIT_")
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
            "git",
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
        return result.stdout

    def _implementation_binding(self) -> dict[str, Any]:
        if Path(__file__).resolve() != self.project_root / MODULE_PATH:
            _fail("IDENTITY", "loaded implementation root mismatch")
        head = self._git(self.project_root, "rev-parse", "--verify", "HEAD").decode().strip()
        records = []
        paths = {MODULE_PATH, CLI_PATH, POLICY_PATH}
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
        if result["schema_version"] != "source_preservation_request.v1":
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
        for raw in files:
            row = _object(raw, {"path", "sha256", "size_bytes", "git_mode"})
            paths.append(_relative(row["path"]))
            _digest(row["sha256"])
            if type(row["size_bytes"]) is not int or row["size_bytes"] < 0:
                _fail("REQUEST", "invalid byte size")
            total += row["size_bytes"]
            if row["git_mode"] not in {"100644", "100755"}:
                _fail("UNSUPPORTED_CHANGE", "unsupported Git mode")
        if paths != sorted(paths, key=str.casefold) or len(
            {path.casefold() for path in paths}
        ) != len(paths):
            _fail("REQUEST", "file paths must be unique and sorted")
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
        for token in status.split(b"\0"):
            if not token:
                continue
            if token[:3] != b" M ":
                _fail(
                    "UNSUPPORTED_CHANGE",
                    "only tracked unstaged regular modifications are supported",
                )
            dirty.append(token[3:].decode("utf-8"))
        expected_paths = [row["path"] for row in request["files"]]
        if sorted(dirty, key=str.casefold) != expected_paths:
            _fail("DIRTY_SCOPE", "exact non-excluded dirty set mismatch")
        captures = {}
        for row in request["files"]:
            path = row["path"]
            mode, _ = self._tree_entry(root, head, path)
            if mode != row["git_mode"]:
                _fail("UNSUPPORTED_CHANGE", "Git file mode differs")
            attributes = self._git(
                root, "check-attr", "-z", "filter", "working-tree-encoding", "--", path
            ).split(b"\0")
            if any(value not in {b"unspecified", b"unset"} for value in attributes[2::3] if value):
                _fail("FILTER", "filter/encoding transform unsupported")
            raw = _regular(_member(root, path), expected_size=row["size_bytes"])
            if len(raw) != row["size_bytes"] or _sha(raw) != row["sha256"]:
                _fail("DRIFT", "source bytes differ from request")
            captures[path] = raw
            self._history(root, head, path, raw)
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

    def _history(self, root: Path, head: str, path: str, content: bytes) -> None:
        if not path.startswith("registry/development_tasks/"):
            return
        try:
            before = _object(
                safe_load_yaml_text(
                    self._git(root, "cat-file", "blob", f"{head}:{path}").decode("utf-8")
                )
            )
            after = _object(safe_load_yaml_text(content.decode("utf-8")))
            validate_canonical_fragment(before)
            validate_canonical_fragment(after)
            if (
                after["events"][: len(before["events"])] != before["events"]
                or before["stable_task_identity"] != after["stable_task_identity"]
            ):
                _fail("HISTORY", "previous task events were rewritten")
        except (ValueError, UnicodeError, KeyError) as exc:
            _fail("HISTORY", type(exc).__name__)

    def _tree_entry(self, root: Path, commit: str, path: str) -> tuple[str, str]:
        raw = self._git(root, "ls-tree", "-z", commit, "--", path)
        entries = [entry for entry in raw.split(b"\0") if entry]
        if len(entries) != 1:
            _fail("UNSUPPORTED_CHANGE", "existing unique tracked file required")
        metadata, found = entries[0].split(b"\t", 1)
        mode, kind, oid = metadata.decode().split()
        if found.decode("utf-8") != path or kind != "blob" or mode not in {"100644", "100755"}:
            _fail("UNSUPPORTED_CHANGE", "regular Git blob required")
        return mode, _digest(oid, 40)

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
                "schema_version": "source_preservation_receipt.v1",
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
                    "schema_version": "source_preservation_failure.v1",
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
            "schema_version": "source_preservation_event.v1",
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
        for expected, actual in zip(request["files"], snapshot["files"], strict=True):
            row = _object(
                actual,
                {"path", "sha256", "size_bytes", "git_mode", "blob_oid", "blob_content_sha256"},
            )
            if any(row[key] != value for key, value in expected.items()):
                _fail("RECEIPT", "snapshot file binding mismatch")
            mode, oid = self._tree_entry(root, commit, row["path"])
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
            self._history(root, request["source_head_sha"], row["path"], content)
        replacements = {
            row["path"]: (row["git_mode"], row["blob_oid"]) for row in snapshot["files"]
        }
        parent_tree = (
            self._git(root, "rev-parse", f"{request['source_head_sha']}^{{tree}}").decode().strip()
        )
        if self._expected_tree(root, parent_tree, replacements) != tree:
            _fail("RECEIPT", "snapshot includes an undeclared tree change")
        if (
            require_ref
            and self._git(root, "rev-parse", "--verify", snapshot["ref"]).decode().strip() != commit
        ):
            _fail("RECEIPT", "preservation ref drift")

    def _expected_tree(
        self, root: Path, tree: str, replacements: dict[str, tuple[str, str]]
    ) -> str:
        """Recompute only changed tree nodes, never read/hash undeclared blobs.

        Git V1 is SHA-1; object headers and untouched entry OIDs are metadata.
        Validation does not write Git objects or a temporary index.
        """
        raw = self._git(root, "cat-file", "tree", tree)
        result = bytearray()
        seen: set[str] = set()
        cursor = 0
        while cursor < len(raw):
            nul = raw.index(b"\0", cursor)
            mode, name_bytes = raw[cursor:nul].split(b" ", 1)
            oid_bytes = raw[nul + 1 : nul + 21]
            if len(oid_bytes) != 20:
                _fail("RECEIPT", "unsupported Git tree object format")
            name = name_bytes.decode("utf-8")
            nested = {
                path[len(name) + 1 :]: row
                for path, row in replacements.items()
                if path.startswith(name + "/")
            }
            if name in replacements:
                expected_mode, replacement_oid = replacements[name]
                if mode.decode() != expected_mode:
                    _fail("RECEIPT", "snapshot changes an existing file mode")
                oid_bytes = bytes.fromhex(_digest(replacement_oid, 40))
                seen.add(name)
            elif nested:
                if mode != b"40000":
                    _fail("RECEIPT", "replacement ancestor is not a tree")
                oid_bytes = bytes.fromhex(self._expected_tree(root, oid_bytes.hex(), nested))
                seen.update(name + "/" + path for path in nested)
            result.extend(mode + b" " + name_bytes + b"\0" + oid_bytes)
            cursor = nul + 21
        if seen != set(replacements):
            _fail("RECEIPT", "replacement path absent in original parent tree")
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
            or receipt.get("schema_version") != "source_preservation_receipt.v1"
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
                or event["schema_version"] != "source_preservation_event.v1"
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
            "schema_version": "source_preservation_validation.v1",
            "status": "PASS",
            "receipt_path": expected_path.as_posix(),
            "snapshot_commit": receipt["snapshot"]["commit"],
            "ref": receipt["snapshot"]["ref"],
            "safety": dict(_SAFETY),
        }
