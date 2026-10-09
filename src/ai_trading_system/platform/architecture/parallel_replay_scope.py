"""DEVX-023 P6 and candidate B: the reviewed scope of the opt-in lease replay switches.

Two switches shorten the replay of the lease store and both default to off: the chain-level parallel
replay (``AITS_LEASE_PARALLEL_REPLAY``, ``lease_parallel_replay``) and the replay seal
(``AITS_LEASE_SEAL``, ``lease_replay_seal``). This module only answers "which of the publication
chain's processes get which switch" from a reviewed configuration, so that the answer is data with
an owner, a status and an exit condition instead of an environment variable somebody exported. It
imports nothing heavy (no lease kernel), so the validation driver and the publication command can
both use it.

Fail safe: a missing, unreadable, malformed or not-enabled configuration means that no process gets
a switch and every replay stays serial, which is today's behaviour. The value of a switch is only
ever taken from the configuration; an inherited value is removed by the callers
(``without_switch``) before the per-role values are added. The optional ``lease_seal`` section has
the same roles and the same invariants as ``roles``; its absence means no process gets the seal.
"""

from __future__ import annotations

import hashlib
from collections.abc import Mapping, MutableMapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ai_trading_system.yaml_loader import load_strict_yaml_text

SCOPE_CONFIG_PATH = Path("config/architecture/devx_023_parallel_replay_scope.v1.yaml")
SCHEMA_VERSION = "devx_023_parallel_replay_scope.v1"

# Same name as lease_parallel_replay.PARALLEL_REPLAY_ENV; a test keeps the two equal so this module
# does not have to import the lease kernel.
PARALLEL_REPLAY_ENV = "AITS_LEASE_PARALLEL_REPLAY"
# Same name as lease_replay_seal.SEAL_ENV (a test keeps them equal) and the value that turns it on.
SEAL_ENV = "AITS_LEASE_SEAL"
SEAL_ON_VALUE = "1"
# The parallel replay clamps larger values to this (lease_parallel_replay.MAX_WORKERS); a test keeps
# the two equal. A configuration above it is invalid rather than silently clamped.
MAX_CONFIGURED_WORKERS = 8
MIN_CONFIGURED_WORKERS = 2

ROLE_S3_COMMAND = "s3_command"
ROLE_VALIDATION_STAGE_1 = "validation_stage_1"
ROLE_LOCAL_PUBLISH_WORKER = "local_publish_worker"
ROLE_VALIDATION_PRE_FULL_TIERS = "validation_pre_full_tiers"
ROLE_FORMAL_FULL = "formal_full"
ROLE_NAMED_DQ_CHILDREN = "named_dq_restricted_children"
ROLES = (
    ROLE_S3_COMMAND,
    ROLE_VALIDATION_STAGE_1,
    ROLE_LOCAL_PUBLISH_WORKER,
    ROLE_VALIDATION_PRE_FULL_TIERS,
    ROLE_FORMAL_FULL,
    ROLE_NAMED_DQ_CHILDREN,
)
# Invariants, not tunables: the formal Full contract is unchanged by owner decision (DEVX-022 option
# C, 2026-10-05) and the restricted named-DQ children must only run reviewed committed code
# (DEVX-023 12.4). A configuration that enables one of them is invalid and enables nothing.
NEVER_ENABLED = frozenset({ROLE_FORMAL_FULL, ROLE_NAMED_DQ_CHILDREN})
# Statuses under which the scope is in force. PROPOSED_PENDING_OWNER_REVIEW, DISABLED or anything
# else enables nothing. PILOT_BASELINE is AGENTS.md heuristic-governance option 3 (a documented
# temporary baseline with an exit condition).
ENABLED_STATUSES = frozenset({"PILOT_BASELINE", "OWNER_APPROVED"})
KNOWN_STATUSES = ENABLED_STATUSES | {"PROPOSED_PENDING_OWNER_REVIEW", "DISABLED"}


@dataclass(frozen=True)
class ParallelReplayScope:
    status: str
    version: str
    workers: int
    enabled_roles: frozenset[str]
    config_sha256: str | None
    problem: str | None
    # Roles that get the replay seal (candidate B); empty when the policy has no lease_seal section.
    seal_roles: frozenset[str] = frozenset()

    @property
    def active(self) -> bool:
        return (
            self.problem is None
            and self.status in ENABLED_STATUSES
            and bool(self.enabled_roles or self.seal_roles)
        )

    def workers_for(self, role: str) -> int:
        """The worker count for a role, 0 when the role does not get the switch."""
        return self.workers if self.active and role in self.enabled_roles else 0

    def seal_for(self, role: str) -> bool:
        """Whether the role gets the replay seal switch."""
        return self.active and role in self.seal_roles

    def environment_for(self, role: str) -> dict[str, str]:
        environment: dict[str, str] = {}
        workers = self.workers_for(role)
        if workers:
            environment[PARALLEL_REPLAY_ENV] = str(workers)
        if self.seal_for(role):
            environment[SEAL_ENV] = SEAL_ON_VALUE
        return environment

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "version": self.version,
            "workers": self.workers,
            "enabled_roles": sorted(self.enabled_roles),
            "seal_roles": sorted(self.seal_roles),
            "config_sha256": self.config_sha256,
            "active": self.active,
            "problem": self.problem,
        }


def inactive_scope(problem: str | None = None, *, status: str = "ABSENT") -> ParallelReplayScope:
    return ParallelReplayScope(
        status=status,
        version="",
        workers=0,
        enabled_roles=frozenset(),
        config_sha256=None,
        problem=problem,
    )


SWITCH_ENVIRONMENT_NAMES = (PARALLEL_REPLAY_ENV, SEAL_ENV)


def without_switch(environment: Mapping[str, str]) -> dict[str, str]:
    """A copy of the environment without inherited switches (only the policy may set them)."""
    return {key: value for key, value in environment.items() if key not in SWITCH_ENVIRONMENT_NAMES}


def apply_switch(scope: ParallelReplayScope, role: str, target: MutableMapping[str, str]) -> None:
    """Make ``target`` (``os.environ`` in a script) carry exactly the role's switches, or none."""
    for name in SWITCH_ENVIRONMENT_NAMES:
        target.pop(name, None)
    target.update(scope.environment_for(role))


def load_scope(repository_root: Path, relative: Path = SCOPE_CONFIG_PATH) -> ParallelReplayScope:
    path = repository_root / relative
    try:
        raw = path.read_bytes()
    except OSError as error:
        return inactive_scope(f"SCOPE_CONFIG_UNREADABLE: {type(error).__name__}")
    digest = hashlib.sha256(raw).hexdigest()
    try:
        # Strict: a duplicate key or a non-finite number makes the policy invalid, not "last wins".
        document = load_strict_yaml_text(raw.decode("utf-8"), label=str(relative))
    except ValueError:  # UnicodeDecodeError and StrictYamlError are both ValueErrors
        return _invalid("SCOPE_CONFIG_NOT_YAML", digest)
    return parse_scope(document, digest)


def _invalid(problem: str, digest: str, status: str = "INVALID") -> ParallelReplayScope:
    return ParallelReplayScope(
        status=status,
        version="",
        workers=0,
        enabled_roles=frozenset(),
        config_sha256=digest,
        problem=problem,
    )


def parse_scope(document: object, digest: str) -> ParallelReplayScope:
    if not isinstance(document, Mapping) or document.get("schema_version") != SCHEMA_VERSION:
        return _invalid("SCOPE_CONFIG_SCHEMA", digest)
    status = document.get("status")
    if not isinstance(status, str) or status not in KNOWN_STATUSES:
        return _invalid("SCOPE_CONFIG_STATUS", digest)
    version = document.get("version")
    if not isinstance(version, str) or not version:
        return _invalid("SCOPE_CONFIG_VERSION", digest, status)
    workers = document.get("workers")
    if type(workers) is not int or not MIN_CONFIGURED_WORKERS <= workers <= MAX_CONFIGURED_WORKERS:
        return _invalid("SCOPE_CONFIG_WORKERS", digest, status)
    enabled, problem = _enabled_roles(document.get("roles"), prefix="")
    if problem is not None:
        return _invalid(problem, digest, status)
    seal_roles: frozenset[str] = frozenset()
    if "lease_seal" in document:
        section = document["lease_seal"]
        if not isinstance(section, Mapping):
            return _invalid("SCOPE_CONFIG_SEAL_SECTION", digest, status)
        seal_roles, problem = _enabled_roles(section.get("roles"), prefix="SEAL_")
        if problem is not None:
            return _invalid(problem, digest, status)
    return ParallelReplayScope(
        status=status,
        version=version,
        workers=workers,
        enabled_roles=enabled,
        config_sha256=digest,
        problem=None,
        seal_roles=seal_roles,
    )


def _enabled_roles(roles: object, *, prefix: str) -> tuple[frozenset[str], str | None]:
    """The enabled roles of one roles mapping, or the problem code (``prefix`` names the switch)."""
    if not isinstance(roles, Mapping) or set(roles) != set(ROLES):
        return frozenset(), f"SCOPE_CONFIG_{prefix}ROLES"
    enabled: set[str] = set()
    for role in ROLES:
        entry = roles[role]
        value = entry.get("value") if isinstance(entry, Mapping) else None
        if value not in ("enabled", "disabled"):
            return frozenset(), f"SCOPE_CONFIG_{prefix}ROLE_VALUE"
        if value == "enabled":
            if role in NEVER_ENABLED:
                return frozenset(), f"SCOPE_CONFIG_{prefix}NEVER_ENABLED_ROLE"
            enabled.add(role)
    return frozenset(enabled), None
