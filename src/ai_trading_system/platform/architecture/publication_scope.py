"""DEVX-016 S3: the publication scope policy and the derivation of one candidate's scope.

A publication transaction declares owned paths, shared paths, generators and required tiers.
Until S3 they were copied by hand from the previous transaction. The reviewed policy declares
what is the same for every ordinary publication (the shared paths, the prefixes a candidate may
change, the paths no ordinary publication may touch); the owned set of one candidate is DERIVED
from its commit-range diff. A path outside the allowed prefixes, or inside the forbidden set,
fails before any transaction is acquired. Generators and required tiers stay in the reviewed
fence policy (single source of truth), so this module takes them as inputs instead of declaring
a second copy.

Nothing here reads the worktree: the diff is a commit-range tree diff, which also keeps the
registered known-unrelated exclusion out of every inspection.
"""

from __future__ import annotations

import hashlib
import subprocess
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ai_trading_system.yaml_loader import safe_load_yaml_path

POLICY_SCHEMA_VERSION = "devx_016_publication_scope_policy.v1"
DEFAULT_POLICY_PATH = Path("config/architecture/devx_016_publication_scope.v1.yaml")
# The policy is unusable until the owner has reviewed it once (DEVX-016 doc 10.10, decision of
# 2026-10-07); a PROPOSED policy is refused by the loader, so no run can start from an
# unreviewed list.
APPROVED_STATUS = "OWNER_APPROVED_ENFORCED"
PROPOSED_STATUS = "PROPOSED_PENDING_OWNER_REVIEW"
# acquire adds these resources itself; declaring them again fails it with
# PUBLICATION_PATH_DUPLICATE, so the policy refuses to list them.
FENCE_AUTO_RESOURCES = frozenset(
    {
        "outputs/validation_runtime",
        "outputs/architecture/arch_005_integration_publication_fence/publication.resource",
    }
)


class PublicationScopeError(RuntimeError):
    def __init__(self, code: str, message: str) -> None:
        self.code = code
        self.message = message
        super().__init__(f"{code}: {message}")


@dataclass(frozen=True)
class PublicationScopePolicy:
    policy_id: str
    version: str
    status: str
    owner: str
    approval_ref: str
    rationale: str
    review_condition: str
    shared_paths: tuple[str, ...]
    allowed_owned_prefixes: tuple[str, ...]
    forbidden_paths: tuple[str, ...]
    resource_paths: tuple[str, ...]
    max_hours_since_full_end: float
    min_free_disk_gb: float
    max_generator_rounds: int
    sha256: str


@dataclass(frozen=True)
class DerivedPublicationScope:
    owned_paths: tuple[str, ...]
    shared_paths: tuple[str, ...]
    generator_ids: tuple[str, ...]
    required_validation_tiers: tuple[str, ...]
    diff_paths: tuple[str, ...]
    shared_diff_paths: tuple[str, ...]
    policy_id: str
    policy_version: str
    policy_sha256: str
    resource_paths: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": "devx_016_derived_publication_scope.v1",
            "policy_id": self.policy_id,
            "policy_version": self.policy_version,
            "policy_sha256": self.policy_sha256,
            "diff_paths": list(self.diff_paths),
            "owned_paths": list(self.owned_paths),
            "shared_paths": list(self.shared_paths),
            "shared_diff_paths": list(self.shared_diff_paths),
            "resource_paths": list(self.resource_paths),
            "generator_ids": list(self.generator_ids),
            "required_validation_tiers": list(self.required_validation_tiers),
            "production_effect": "none",
            "broker_action": "none",
        }


def load_publication_scope_policy(
    path: Path, *, allow_proposed: bool = False
) -> PublicationScopePolicy:
    raw = path.read_bytes()
    payload = safe_load_yaml_path(path)
    if not isinstance(payload, dict):
        raise PublicationScopeError("PUBLICATION_SCOPE_POLICY_INVALID", "policy is not a mapping")
    if payload.get("schema_version") != POLICY_SCHEMA_VERSION:
        raise PublicationScopeError(
            "PUBLICATION_SCOPE_POLICY_SCHEMA", str(payload.get("schema_version"))
        )
    status = payload.get("status")
    allowed_statuses = {APPROVED_STATUS, PROPOSED_STATUS} if allow_proposed else {APPROVED_STATUS}
    if status not in allowed_statuses:
        raise PublicationScopeError("PUBLICATION_SCOPE_POLICY_STATUS", str(payload.get("status")))
    shared = _paths(payload.get("shared_paths"), "shared_paths", prefix=False)
    allowed = _paths(payload.get("allowed_owned_prefixes"), "allowed_owned_prefixes", prefix=True)
    forbidden = _paths(payload.get("forbidden_paths"), "forbidden_paths", prefix=None)
    resources = _paths(payload.get("resource_paths"), "resource_paths", prefix=False)
    for resource in resources:
        if not resource.startswith("outputs/") or resource in FENCE_AUTO_RESOURCES:
            raise PublicationScopeError("PUBLICATION_SCOPE_RESOURCE_INVALID", resource)
    _require_unique_casefolded(resources, "resource_paths")
    _require_unique_casefolded(shared, "shared_paths")
    _require_unique_casefolded(allowed, "allowed_owned_prefixes")
    _require_unique_casefolded(forbidden, "forbidden_paths")
    for entry in shared:
        if _is_forbidden(entry, forbidden):
            raise PublicationScopeError("PUBLICATION_SCOPE_POLICY_OVERLAP", f"shared:{entry}")
    limits = payload.get("limits")
    if not isinstance(limits, dict):
        raise PublicationScopeError("PUBLICATION_SCOPE_POLICY_FIELD", "limits")
    hours = _bounded(limits.get("max_hours_since_full_end"), "max_hours_since_full_end", 0.1, 24.0)
    disk = _bounded(limits.get("min_free_disk_gb"), "min_free_disk_gb", 1.0, 10_000.0)
    rounds = _bounded(limits.get("max_generator_rounds"), "max_generator_rounds", 1.0, 10.0)
    if rounds != int(rounds):
        raise PublicationScopeError("PUBLICATION_SCOPE_POLICY_FIELD", "max_generator_rounds")
    return PublicationScopePolicy(
        policy_id=_text(payload.get("policy_id"), "policy_id"),
        version=_text(payload.get("version"), "version"),
        status=str(status),
        owner=_text(payload.get("owner"), "owner"),
        approval_ref=_text(payload.get("approval_ref"), "approval_ref"),
        rationale=_text(payload.get("rationale"), "rationale"),
        review_condition=_text(payload.get("review_condition"), "review_condition"),
        shared_paths=shared,
        allowed_owned_prefixes=allowed,
        forbidden_paths=forbidden,
        resource_paths=resources,
        max_hours_since_full_end=hours,
        min_free_disk_gb=disk,
        max_generator_rounds=int(rounds),
        sha256=hashlib.sha256(raw).hexdigest(),
    )


def derive_publication_scope(
    policy: PublicationScopePolicy,
    *,
    diff_paths: Iterable[str],
    generator_ids: Sequence[str],
    required_validation_tiers: Sequence[str],
    extra_forbidden: Sequence[str] = (),
) -> DerivedPublicationScope:
    """Classify every changed path as shared, owned or refused; fail before any acquire."""
    forbidden = tuple(policy.forbidden_paths) + tuple(
        _portable(entry, prefix=None) for entry in extra_forbidden
    )
    changed = sorted({_portable(path, prefix=False) for path in diff_paths}, key=_sort_key)
    if not changed:
        raise PublicationScopeError("PUBLICATION_SCOPE_DIFF_EMPTY", "candidate changes nothing")
    _require_unique_casefolded(tuple(changed), "diff_paths")
    owned: list[str] = []
    shared_changed: list[str] = []
    for path in changed:
        if _is_forbidden(path, forbidden):
            raise PublicationScopeError("PUBLICATION_SCOPE_PATH_FORBIDDEN", path)
        if _is_shared(path, policy.shared_paths):
            shared_changed.append(path)
        elif any(
            path.casefold().startswith(prefix.casefold())
            for prefix in policy.allowed_owned_prefixes
        ):
            owned.append(path)
        else:
            raise PublicationScopeError("PUBLICATION_SCOPE_PATH_NOT_ALLOWED", path)
    return DerivedPublicationScope(
        owned_paths=tuple(owned),
        shared_paths=policy.shared_paths,
        generator_ids=tuple(generator_ids),
        required_validation_tiers=tuple(required_validation_tiers),
        diff_paths=tuple(changed),
        shared_diff_paths=tuple(shared_changed),
        policy_id=policy.policy_id,
        policy_version=policy.version,
        policy_sha256=policy.sha256,
        resource_paths=policy.resource_paths,
    )


def changed_paths_between(repository_root: Path, base: str, head: str) -> tuple[str, ...]:
    """Paths changed by the commit range (tree diff; additions, modifications and deletions)."""
    completed = subprocess.run(
        [
            "git",
            "diff-tree",
            "-r",
            "--no-renames",
            "--no-commit-id",
            "--name-only",
            "-z",
            base,
            head,
        ],
        cwd=repository_root,
        capture_output=True,
        check=False,
    )
    if completed.returncode != 0:
        raise PublicationScopeError(
            "PUBLICATION_SCOPE_DIFF_UNAVAILABLE",
            completed.stderr.decode("utf-8", "replace").strip(),
        )
    names = [name for name in completed.stdout.decode("utf-8").split("\0") if name]
    return tuple(sorted(set(names), key=_sort_key))


def _sort_key(path: str) -> tuple[str, str]:
    return (path.casefold(), path)


def _bounded(value: object, field: str, low: float, high: float) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise PublicationScopeError("PUBLICATION_SCOPE_POLICY_FIELD", field)
    if not low <= float(value) <= high:
        raise PublicationScopeError("PUBLICATION_SCOPE_POLICY_RANGE", f"{field}={value}")
    return float(value)


def _text(value: object, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise PublicationScopeError("PUBLICATION_SCOPE_POLICY_FIELD", field)
    return value.strip()


def _paths(value: object, field: str, *, prefix: bool | None) -> tuple[str, ...]:
    if not isinstance(value, list) or not value:
        raise PublicationScopeError("PUBLICATION_SCOPE_POLICY_FIELD", field)
    return tuple(_portable(entry, prefix=prefix) for entry in value)


def _portable(value: object, *, prefix: bool | None) -> str:
    """A repo-relative POSIX path. prefix=True: a directory prefix ending in '/'; False: a file or
    directory name without trailing '/'; None: either form."""
    if not isinstance(value, str) or not value:
        raise PublicationScopeError("PUBLICATION_SCOPE_PATH_INVALID", repr(value))
    text = value
    if (
        "\\" in text
        or text.startswith("/")
        or ":" in text
        or "//" in text
        or any(ord(char) < 32 for char in text)
        or any(part in {"", ".", ".."} for part in text.rstrip("/").split("/"))
    ):
        raise PublicationScopeError("PUBLICATION_SCOPE_PATH_INVALID", text)
    trailing = text.endswith("/")
    if prefix is True and not trailing:
        raise PublicationScopeError("PUBLICATION_SCOPE_PREFIX_INVALID", text)
    if prefix is False and trailing:
        raise PublicationScopeError("PUBLICATION_SCOPE_PATH_INVALID", text)
    return text


def _require_unique_casefolded(paths: Sequence[str], field: str) -> None:
    seen: dict[str, str] = {}
    for path in paths:
        key = path.casefold()
        if key in seen:
            raise PublicationScopeError("PUBLICATION_SCOPE_PATH_DUPLICATE", f"{field}:{path}")
        seen[key] = path


def _is_forbidden(path: str, forbidden: Sequence[str]) -> bool:
    lowered = path.casefold()
    for entry in forbidden:
        target = entry.casefold()
        if target.endswith("/"):
            if lowered.startswith(target) or lowered == target.rstrip("/"):
                return True
        elif lowered == target or lowered.startswith(target + "/"):
            return True
    return False


def _is_shared(path: str, shared: Sequence[str]) -> bool:
    lowered = path.casefold()
    return any(
        lowered == entry.casefold() or lowered.startswith(entry.casefold() + "/")
        for entry in shared
    )


def policy_summary(policy: PublicationScopePolicy) -> Mapping[str, Any]:
    return {
        "policy_id": policy.policy_id,
        "version": policy.version,
        "status": policy.status,
        "owner": policy.owner,
        "approval_ref": policy.approval_ref,
        "sha256": policy.sha256,
        "shared_path_count": len(policy.shared_paths),
        "allowed_owned_prefix_count": len(policy.allowed_owned_prefixes),
        "forbidden_path_count": len(policy.forbidden_paths),
    }
