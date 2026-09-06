"""Process-local named DQ context; declarations alone never prove loaded code.

Only the reviewed fresh-child bootstrap initializes the production context.  Its
Git-byte loader, not this pure contract module, supplies the provenance proof.
See TRADING-2564_S2b_Named_DQ_Execution_Contract_V1.md sections 5 and 6.
"""

from __future__ import annotations

import hashlib
import os
import re
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import PurePosixPath, PureWindowsPath
from typing import Never, SupportsIndex

from ai_trading_system.contracts.data_quality_execution import (
    _int_value,
    _repo_relative_path,
    _require_exact_keys,
    _sha256,
    _text_value,
    canonical_json_value,
)


class NamedExecutionContextError(ValueError):
    def __init__(self, code: str, message: str) -> None:
        self.code = code
        super().__init__(f"{code}: {message}")


def _absolute_root(value: str, field: str) -> None:
    _text_value(value, field)
    parsed = PureWindowsPath(value) if PureWindowsPath(value).drive else PurePosixPath(value)
    if (
        not parsed.is_absolute()
        or parsed.as_posix() != value
        or any(part in {".", ".."} for part in value.split("/"))
        or "\\" in value
    ):
        raise NamedExecutionContextError("NAMED_ROOT_INVALID", field)


def _git_id(value: str, field: str) -> None:
    if not isinstance(value, str) or re.fullmatch(r"(?:[0-9a-f]{40}|[0-9a-f]{64})", value) is None:
        raise NamedExecutionContextError("NAMED_GIT_ID_INVALID", field)


@dataclass(frozen=True)
class GitCompiledModuleBinding:
    module_name: str
    source_path: str
    git_blob_id: str
    sha256: str
    size_bytes: int
    is_package: bool

    def __post_init__(self) -> None:
        if (
            not isinstance(self.module_name, str)
            or re.fullmatch(r"ai_trading_system(?:\.[A-Za-z_][A-Za-z0-9_]*)*", self.module_name)
            is None
        ):
            raise NamedExecutionContextError("NAMED_MODULE_INVALID", str(self.module_name))
        if type(self.is_package) is not bool:
            raise NamedExecutionContextError("NAMED_MODULE_INVALID", "is_package must be bool")
        expected = "src/" + self.module_name.replace(".", "/")
        expected += "/__init__.py" if self.is_package else ".py"
        if _repo_relative_path(self.source_path, "source_path") != expected:
            raise NamedExecutionContextError("NAMED_MODULE_PATH_MISMATCH", self.source_path)
        _git_id(self.git_blob_id, "git_blob_id")
        _sha256(self.sha256, "module.sha256")
        _int_value(self.size_bytes, "module.size_bytes")

    def to_dict(self) -> dict[str, object]:
        return {
            "module_name": self.module_name,
            "source_path": self.source_path,
            "git_blob_id": self.git_blob_id,
            "sha256": self.sha256,
            "size_bytes": self.size_bytes,
            "is_package": self.is_package,
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> GitCompiledModuleBinding:
        _require_exact_keys(payload, frozenset(cls.__dataclass_fields__), "compiled module")
        package = payload["is_package"]
        if type(package) is not bool:
            raise NamedExecutionContextError("NAMED_MODULE_INVALID", "is_package must be bool")
        return cls(
            module_name=_text_value(payload["module_name"], "module_name"),
            source_path=_text_value(payload["source_path"], "source_path"),
            git_blob_id=_text_value(payload["git_blob_id"], "git_blob_id"),
            sha256=_text_value(payload["sha256"], "sha256"),
            size_bytes=_int_value(payload["size_bytes"], "size_bytes"),
            is_package=package,
        )


@dataclass(frozen=True)
class NamedExecutionIdentity:
    execution_root: str
    candidate_commit: str
    source_manifest_path: str
    source_manifest_sha256: str
    bootstrap_path: str
    bootstrap_sha256: str
    python_executable: str
    python_version: str
    git_executable: str
    git_version: str
    modules: tuple[GitCompiledModuleBinding, ...]
    source_kind: str = "GIT_COMMIT_BYTES_COMPILED"

    def __post_init__(self) -> None:
        _absolute_root(self.execution_root, "execution_root")
        _absolute_root(self.python_executable, "python_executable")
        _absolute_root(self.git_executable, "git_executable")
        _git_id(self.candidate_commit, "candidate_commit")
        for path in (self.source_manifest_path, self.bootstrap_path):
            _repo_relative_path(path, "execution source path")
        for checksum in (self.source_manifest_sha256, self.bootstrap_sha256):
            _sha256(checksum, "execution source sha256")
        _text_value(self.python_version, "python_version")
        _text_value(self.git_version, "git_version")
        if self.source_kind != "GIT_COMMIT_BYTES_COMPILED":
            raise NamedExecutionContextError("NAMED_SOURCE_KIND_INVALID", self.source_kind)
        if (
            type(self.modules) is not tuple
            or not self.modules
            or any(not isinstance(item, GitCompiledModuleBinding) for item in self.modules)
        ):
            raise NamedExecutionContextError(
                "NAMED_MODULE_SET_INVALID", "typed nonempty tuple required"
            )
        modules = tuple(sorted(self.modules, key=lambda item: item.module_name))
        names = {item.module_name for item in modules}
        packages = {item.module_name for item in modules if item.is_package}
        if len(names) != len(modules) or len({item.source_path for item in modules}) != len(
            modules
        ):
            raise NamedExecutionContextError("NAMED_MODULE_SET_INVALID", "duplicate module/path")
        if "ai_trading_system" not in packages:
            raise NamedExecutionContextError(
                "NAMED_MODULE_SET_INVALID", "real root package required"
            )
        for name in names:
            parts = name.split(".")
            if any(".".join(parts[:index]) not in packages for index in range(1, len(parts))):
                raise NamedExecutionContextError("NAMED_MODULE_SET_INVALID", "missing package init")
        object.__setattr__(self, "modules", modules)

    def to_dict(self) -> dict[str, object]:
        return {
            "execution_root": self.execution_root,
            "candidate_commit": self.candidate_commit,
            "source_manifest_path": self.source_manifest_path,
            "source_manifest_sha256": self.source_manifest_sha256,
            "bootstrap_path": self.bootstrap_path,
            "bootstrap_sha256": self.bootstrap_sha256,
            "python_executable": self.python_executable,
            "python_version": self.python_version,
            "git_executable": self.git_executable,
            "git_version": self.git_version,
            "modules": [item.to_dict() for item in self.modules],
            "source_kind": self.source_kind,
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> NamedExecutionIdentity:
        _require_exact_keys(payload, frozenset(cls.__dataclass_fields__), "execution identity")
        raw = payload["modules"]
        if not isinstance(raw, list) or any(not isinstance(item, dict) for item in raw):
            raise NamedExecutionContextError("NAMED_MODULE_SET_INVALID", "modules must be array")
        return cls(
            execution_root=_text_value(payload["execution_root"], "execution_root"),
            candidate_commit=_text_value(payload["candidate_commit"], "candidate_commit"),
            source_manifest_path=_text_value(
                payload["source_manifest_path"], "source_manifest_path"
            ),
            source_manifest_sha256=_text_value(
                payload["source_manifest_sha256"], "source_manifest_sha256"
            ),
            bootstrap_path=_text_value(payload["bootstrap_path"], "bootstrap_path"),
            bootstrap_sha256=_text_value(payload["bootstrap_sha256"], "bootstrap_sha256"),
            python_executable=_text_value(payload["python_executable"], "python_executable"),
            python_version=_text_value(payload["python_version"], "python_version"),
            git_executable=_text_value(payload["git_executable"], "git_executable"),
            git_version=_text_value(payload["git_version"], "git_version"),
            modules=tuple(GitCompiledModuleBinding.from_dict(item) for item in raw),
            source_kind=_text_value(payload["source_kind"], "source_kind"),
        )

    @property
    def stable_identity_sha256(self) -> str:
        return hashlib.sha256(canonical_json_value(self.to_dict()).encode("utf-8")).hexdigest()


_CONTEXT_SEAL = object()
_active_context: NamedExecutionContext | None = None


@dataclass(frozen=True, init=False, eq=False)
class NamedExecutionContext:
    identity: NamedExecutionIdentity
    process_id: int
    provenance_kind: str
    _frozen: bool

    def __init__(self, identity: NamedExecutionIdentity, *, _seal: object, test_only: bool) -> None:
        if _seal is not _CONTEXT_SEAL:
            raise NamedExecutionContextError("NAMED_CONTEXT_SEAL_REQUIRED", "bootstrap only")
        object.__setattr__(self, "identity", identity)
        object.__setattr__(self, "process_id", os.getpid())
        object.__setattr__(
            self,
            "provenance_kind",
            "SYNTHETIC_CONTRACT_TEST_ONLY" if test_only else "GIT_COMMIT_BYTES_COMPILED",
        )
        object.__setattr__(self, "_frozen", False)

    @property
    def stable_identity_sha256(self) -> str:
        return self.identity.stable_identity_sha256

    def __reduce_ex__(self, protocol: SupportsIndex) -> Never:
        raise TypeError("named execution contexts cannot be serialized")

    def __reduce__(self) -> Never:
        raise TypeError("named execution contexts cannot be serialized")


def _initialize_context(
    identity: NamedExecutionIdentity, *, test_only: bool
) -> NamedExecutionContext:
    global _active_context
    if _active_context is not None:
        raise NamedExecutionContextError("NAMED_CONTEXT_ALREADY_ACTIVE", "nested context forbidden")
    if not isinstance(identity, NamedExecutionIdentity):
        raise NamedExecutionContextError(
            "NAMED_CONTEXT_IDENTITY_INVALID", "typed identity required"
        )
    result = NamedExecutionContext(identity, _seal=_CONTEXT_SEAL, test_only=test_only)
    _active_context = result
    return result


def _initialize_named_execution_context(identity: NamedExecutionIdentity) -> NamedExecutionContext:
    """Restricted bootstrap entry; calling this does not independently verify Git bytes."""
    return _initialize_context(identity, test_only=False)


def _initialize_test_named_execution_context(
    identity: NamedExecutionIdentity,
) -> NamedExecutionContext:
    """Contract-only fixture entry.  Production accessors always reject its context."""
    return _initialize_context(identity, test_only=True)


def _check_current(handle: NamedExecutionContext) -> None:
    if handle is not _active_context or handle.process_id != os.getpid():
        raise NamedExecutionContextError(
            "NAMED_CONTEXT_PROCESS_MISMATCH", "inactive/foreign context"
        )


def _freeze_named_execution_context(
    handle: NamedExecutionContext, *, loaded_modules: tuple[GitCompiledModuleBinding, ...]
) -> None:
    _check_current(handle)
    object.__setattr__(handle, "_frozen", False)
    if (
        type(loaded_modules) is not tuple
        or any(not isinstance(item, GitCompiledModuleBinding) for item in loaded_modules)
        or tuple(sorted(loaded_modules, key=lambda item: item.module_name))
        != handle.identity.modules
    ):
        raise NamedExecutionContextError(
            "NAMED_LOADED_SET_MISMATCH", "exact compiled module set required"
        )
    object.__setattr__(handle, "_frozen", True)


def require_named_execution_context() -> NamedExecutionContext:
    handle = _active_context
    if handle is None:
        raise NamedExecutionContextError("NAMED_CONTEXT_REQUIRED", "fresh bootstrap required")
    _check_current(handle)
    if not handle._frozen or handle.provenance_kind != "GIT_COMMIT_BYTES_COMPILED":
        raise NamedExecutionContextError("NAMED_CONTEXT_NOT_PROVEN", "unfrozen/test-only context")
    return handle


def close_named_execution_context(handle: NamedExecutionContext) -> None:
    global _active_context
    _check_current(handle)
    _active_context = None
