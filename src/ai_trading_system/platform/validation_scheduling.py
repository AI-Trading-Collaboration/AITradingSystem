"""DEVX-018 governed pytest scheduling manifest.

The manifest decides which test files are distributed per node instead of per
file, which functions run a real inner Full / source job / actual runner chain
(``real_full_chain``), and how many of those may be in flight at once. It is a
reviewed policy input: parsing is strict and fails closed, and runtime profiles
bind its exact bytes by SHA-256. See
``docs/requirements/DEVX-018_Validation_Runtime_Throughput_V1.md``.
"""

from __future__ import annotations

import hashlib
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path, PurePosixPath

from ai_trading_system.yaml_loader import StrictYamlError, load_strict_yaml_text

SCHEDULING_MANIFEST_SCHEMA_VERSION = "devx_018_validation_scheduling.v1"
SCHEDULING_MANIFEST_RELATIVE_PATH = "config/architecture/devx_018_validation_scheduling.yaml"
SPLIT_SCOPE_EVIDENCE_SCHEMA_VERSION = "devx_018_split_scope_evidence.v1"
REAL_FULL_CHAIN_MARKER = "real_full_chain"
# An exclusive group runs as one sequential work unit under this scope prefix.
EXCLUSIVE_GROUP_SCOPE_PREFIX = "exclusive-group::"
ALLOWED_MANIFEST_STATUSES = frozenset({"ACTIVE_PILOT", "ACTIVE"})
_REQUIRED_TEXT_FIELDS = (
    "policy_id",
    "owner",
    "requirement",
    "rationale",
    "intended_effect",
    "validation_evidence",
    "review_condition",
)
_ALLOWED_TOP_LEVEL_FIELDS = frozenset(
    {
        "schema_version",
        "version",
        "status",
        "heavy_concurrency_cap",
        "heavy_start_interval_seconds",
        "split_scope_files",
        "real_full_chain",
        "exclusive_groups",
        "production_effect",
        *_REQUIRED_TEXT_FIELDS,
    }
)
_TEST_FILE_RE = re.compile(r"^tests/(?:[A-Za-z0-9_]+/)*test_[A-Za-z0-9_]+\.py$")
_FUNCTION_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_GROUP_RE = re.compile(r"^[a-z][a-z0-9_]{2,63}$")


class SchedulingManifestError(ValueError):
    """The reviewed scheduling manifest is malformed; callers must fail closed."""


@dataclass(frozen=True)
class SchedulingManifest:
    relative_path: str
    sha256: str
    policy_id: str
    version: int
    status: str
    heavy_concurrency_cap: int
    split_scope_files: tuple[str, ...]
    real_full_chain_functions: tuple[str, ...]
    # Units sharing one host-global resource (a fixed Job name, the whole HKCU
    # test-root view) run as one sequential unit; a function is in at most one group.
    exclusive_groups: tuple[tuple[str, tuple[str, ...]], ...] = ()
    # Minimum spacing between two workers starting a heavy unit; 0 disables the ramp.
    # The manifest SHA-256 already binds this value in runtime profile evidence.
    heavy_start_interval_seconds: int = 0

    def exclusive_group_of(self, nodeid: str) -> str | None:
        key = nodeid_function_key(nodeid)
        for name, functions in self.exclusive_groups:
            if key in functions:
                return name
        return None

    def is_heavy_scope(self, scope: str) -> bool:
        """A group unit is heavy when any member is; a node unit when it is real_full_chain."""
        if scope.startswith(EXCLUSIVE_GROUP_SCOPE_PREFIX):
            members = dict(self.exclusive_groups).get(
                scope[len(EXCLUSIVE_GROUP_SCOPE_PREFIX) :], ()
            )
            return any(key in self.real_full_chain_functions for key in members)
        return "::" in scope and self.is_real_full_chain(scope)

    def is_split_file(self, file_path: str) -> bool:
        return file_path in self.split_scope_files

    def is_real_full_chain(self, nodeid: str) -> bool:
        return nodeid_function_key(nodeid) in self.real_full_chain_functions

    def split_scope_evidence(self) -> dict[str, object]:
        """Profile evidence; the formal inspector recomputes it from committed bytes."""
        return {
            "schema_version": SPLIT_SCOPE_EVIDENCE_SCHEMA_VERSION,
            "manifest_path": self.relative_path,
            "manifest_sha256": self.sha256,
            "policy_id": self.policy_id,
            "version": self.version,
            "status": self.status,
            "heavy_concurrency_cap": self.heavy_concurrency_cap,
            "split_scope_files": list(self.split_scope_files),
            "real_full_chain_marker": REAL_FULL_CHAIN_MARKER,
            "real_full_chain_function_count": len(self.real_full_chain_functions),
        }


def nodeid_file(nodeid: str) -> str:
    return nodeid.replace("\\", "/").split("::", 1)[0]


def nodeid_function_key(nodeid: str) -> str:
    """``file::function`` (or ``file::Class::method``) without parametrization."""
    return nodeid.replace("\\", "/").split("[", 1)[0]


def _string_list(value: object, field: str) -> list[str]:
    if not isinstance(value, list) or not value:
        raise SchedulingManifestError(f"{field} must be a non-empty list")
    if any(not isinstance(item, str) for item in value):
        raise SchedulingManifestError(f"{field} entries must be strings")
    if len(set(value)) != len(value):
        raise SchedulingManifestError(f"{field} contains duplicates")
    if list(value) != sorted(value):
        raise SchedulingManifestError(f"{field} must be sorted")
    return list(value)


def parse_scheduling_manifest(
    raw: bytes, *, relative_path: str = SCHEDULING_MANIFEST_RELATIVE_PATH
) -> SchedulingManifest:
    if not isinstance(raw, bytes) or not raw:
        raise SchedulingManifestError("scheduling manifest bytes are required")
    try:
        payload = load_strict_yaml_text(raw.decode("utf-8"), label="scheduling_manifest")
    except (UnicodeDecodeError, StrictYamlError) as exc:
        raise SchedulingManifestError(f"scheduling manifest is not strict YAML: {exc}") from exc
    if not isinstance(payload, Mapping):
        raise SchedulingManifestError("scheduling manifest must be a mapping")
    unknown = sorted(set(payload) - _ALLOWED_TOP_LEVEL_FIELDS)
    if unknown:
        raise SchedulingManifestError(f"scheduling manifest has unknown fields: {unknown}")
    if payload.get("schema_version") != SCHEDULING_MANIFEST_SCHEMA_VERSION:
        raise SchedulingManifestError("scheduling manifest schema_version is unsupported")
    for field in _REQUIRED_TEXT_FIELDS:
        value = payload.get(field)
        if not isinstance(value, str) or not value.strip():
            raise SchedulingManifestError(f"scheduling manifest {field} is required")
    if payload.get("production_effect") != "none":
        raise SchedulingManifestError("scheduling manifest production_effect must be none")
    version = payload.get("version")
    if type(version) is not int or version < 1:
        raise SchedulingManifestError("scheduling manifest version must be a positive integer")
    status = payload.get("status")
    if status not in ALLOWED_MANIFEST_STATUSES:
        raise SchedulingManifestError("scheduling manifest status is not active")
    cap = payload.get("heavy_concurrency_cap")
    if type(cap) is not int or cap < 1:
        raise SchedulingManifestError("heavy_concurrency_cap must be a positive integer")
    interval = payload.get("heavy_start_interval_seconds", 0)
    if type(interval) is not int or interval < 0:
        raise SchedulingManifestError("heavy_start_interval_seconds must be a non-negative integer")
    split_files = _string_list(payload.get("split_scope_files"), "split_scope_files")
    for path in split_files:
        if _TEST_FILE_RE.fullmatch(path) is None or PurePosixPath(path).as_posix() != path:
            raise SchedulingManifestError(f"split_scope_files entry is not a test file: {path}")
    chain = payload.get("real_full_chain")
    if not isinstance(chain, Mapping) or set(chain) != {"marker", "functions"}:
        raise SchedulingManifestError("real_full_chain must contain exactly marker/functions")
    if chain.get("marker") != REAL_FULL_CHAIN_MARKER:
        raise SchedulingManifestError("real_full_chain marker is unsupported")
    functions = _string_list(chain.get("functions"), "real_full_chain.functions")

    def require_split_function(key: str, field: str) -> None:
        file_path, separator, function = key.partition("::")
        if (
            not separator
            or file_path not in split_files
            or any(_FUNCTION_RE.fullmatch(part) is None for part in function.split("::"))
        ):
            # Heavy and exclusive accounting is per node, so entries must live in split files.
            raise SchedulingManifestError(f"{field} entry is invalid: {key}")

    for key in functions:
        require_split_function(key, "real_full_chain")
    raw_groups = payload.get("exclusive_groups", {})
    if not isinstance(raw_groups, Mapping):
        raise SchedulingManifestError("exclusive_groups must be a mapping")
    groups: list[tuple[str, tuple[str, ...]]] = []
    group_of: dict[str, str] = {}
    for name in sorted(raw_groups):
        if not isinstance(name, str) or _GROUP_RE.fullmatch(name) is None:
            raise SchedulingManifestError(f"exclusive group name is invalid: {name!r}")
        members = _string_list(raw_groups[name], f"exclusive_groups.{name}")
        for key in members:
            require_split_function(key, f"exclusive_groups.{name}")
            if key in group_of:
                # A group is one sequential unit, so a function cannot join two of them.
                raise SchedulingManifestError(
                    f"exclusive group member is in two groups: {key} ({group_of[key]}, {name})"
                )
            group_of[key] = name
        groups.append((name, tuple(members)))
    return SchedulingManifest(
        relative_path=relative_path,
        sha256=hashlib.sha256(raw).hexdigest(),
        policy_id=str(payload["policy_id"]),
        version=version,
        status=str(status),
        heavy_concurrency_cap=cap,
        split_scope_files=tuple(split_files),
        real_full_chain_functions=tuple(functions),
        exclusive_groups=tuple(groups),
        heavy_start_interval_seconds=interval,
    )


def load_scheduling_manifest(repository_root: Path) -> SchedulingManifest | None:
    """Return None only when the manifest file is absent (e.g. minimal fixture repos)."""
    path = repository_root / SCHEDULING_MANIFEST_RELATIVE_PATH
    try:
        raw = path.read_bytes()
    except FileNotFoundError:
        return None
    except OSError as exc:
        raise SchedulingManifestError(f"scheduling manifest could not be read: {exc}") from exc
    return parse_scheduling_manifest(raw)


def missing_real_full_chain_functions(
    manifest: SchedulingManifest, nodeids: Sequence[str]
) -> list[str]:
    """Listed functions absent from a collection that did collect their file."""
    collected_files = {nodeid_file(nodeid) for nodeid in nodeids}
    collected_functions = {nodeid_function_key(nodeid) for nodeid in nodeids}
    listed = sorted(
        {
            *manifest.real_full_chain_functions,
            *(key for _, members in manifest.exclusive_groups for key in members),
        }
    )
    return [
        key
        for key in listed
        if key.split("::", 1)[0] in collected_files and key not in collected_functions
    ]


def split_scope_evidence_error(evidence: object, manifest: SchedulingManifest | None) -> str | None:
    """Compare profile evidence with the manifest parsed from trusted bytes."""
    if manifest is None:
        return None if evidence is None else "split scope evidence without a manifest"
    if not isinstance(evidence, Mapping):
        return "split scope evidence is missing although the manifest is present"
    if dict(evidence) != manifest.split_scope_evidence():
        return "split scope evidence differs from the scheduling manifest"
    return None


__all__ = [
    "EXCLUSIVE_GROUP_SCOPE_PREFIX",
    "REAL_FULL_CHAIN_MARKER",
    "SCHEDULING_MANIFEST_RELATIVE_PATH",
    "SCHEDULING_MANIFEST_SCHEMA_VERSION",
    "SPLIT_SCOPE_EVIDENCE_SCHEMA_VERSION",
    "SchedulingManifest",
    "SchedulingManifestError",
    "load_scheduling_manifest",
    "missing_real_full_chain_functions",
    "nodeid_file",
    "nodeid_function_key",
    "parse_scheduling_manifest",
    "split_scope_evidence_error",
]
