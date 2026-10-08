"""DEVX-023 P6: the reviewed parallel-replay scope decides which processes get the switch.

The policy file is data with an owner, a status and an exit condition. These tests pin what makes it
safe: anything unreadable, malformed or not enabled enables nothing (every replay stays serial), the
formal Full and the restricted named-DQ children can never be enabled, and a switch exported by the
launching shell never reaches a process the policy leaves out.
"""

from __future__ import annotations

import copy
import hashlib
from pathlib import Path
from typing import Any

import pytest
import yaml

from ai_trading_system.platform.architecture import lease_parallel_replay
from ai_trading_system.platform.architecture.parallel_replay_scope import (
    NEVER_ENABLED,
    PARALLEL_REPLAY_ENV,
    ROLE_FORMAL_FULL,
    ROLE_LOCAL_PUBLISH_WORKER,
    ROLE_NAMED_DQ_CHILDREN,
    ROLE_S3_COMMAND,
    ROLE_VALIDATION_PRE_FULL_TIERS,
    ROLE_VALIDATION_STAGE_1,
    ROLES,
    SCOPE_CONFIG_PATH,
    ParallelReplayScope,
    apply_switch,
    inactive_scope,
    load_scope,
    parse_scope,
    without_switch,
)

ROOT = Path(__file__).resolve().parents[1]
REAL = ROOT / SCOPE_CONFIG_PATH
ENABLED_ROLES = frozenset({ROLE_S3_COMMAND, ROLE_VALIDATION_STAGE_1, ROLE_LOCAL_PUBLISH_WORKER})


def _document() -> dict[str, Any]:
    loaded = yaml.safe_load(REAL.read_text(encoding="utf-8"))
    assert isinstance(loaded, dict)
    return loaded


def _root_with(tmp_path: Path, document: object) -> Path:
    target = tmp_path / SCOPE_CONFIG_PATH
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(yaml.safe_dump(document, sort_keys=False), encoding="utf-8", newline="\n")
    return tmp_path


def test_the_constants_stay_equal_to_the_parallel_replay_module() -> None:
    # this module does not import the lease kernel, so the two copies are pinned by this test
    assert PARALLEL_REPLAY_ENV == lease_parallel_replay.PARALLEL_REPLAY_ENV
    from ai_trading_system.platform.architecture.parallel_replay_scope import (
        MAX_CONFIGURED_WORKERS,
    )

    assert MAX_CONFIGURED_WORKERS == lease_parallel_replay.MAX_WORKERS


def test_the_real_policy_is_valid_in_force_and_matches_the_reviewed_design() -> None:
    scope = load_scope(ROOT)
    assert scope.problem is None and scope.active
    assert scope.status == "PILOT_BASELINE" and scope.version == "1.0.0"
    assert scope.workers == 4
    assert scope.enabled_roles == ENABLED_ROLES
    assert scope.config_sha256 == hashlib.sha256(REAL.read_bytes()).hexdigest()
    document = _document()
    assert set(document["roles"]) == set(ROLES)
    for role in NEVER_ENABLED:
        assert document["roles"][role]["value"] == "disabled"
        assert document["roles"][role]["never_enabled"] is True
    # the pre-Full tiers run 16 xdist workers; the policy keeps them serial until measured
    assert document["roles"][ROLE_VALIDATION_PRE_FULL_TIERS]["value"] == "disabled"
    assert {ROLE_FORMAL_FULL, ROLE_NAMED_DQ_CHILDREN} == NEVER_ENABLED


def test_only_enabled_roles_get_the_switch_and_its_value_comes_from_the_policy() -> None:
    scope = load_scope(ROOT)
    for role in ENABLED_ROLES:
        assert scope.workers_for(role) == 4
        assert scope.environment_for(role) == {PARALLEL_REPLAY_ENV: "4"}
    for role in set(ROLES) - ENABLED_ROLES:
        assert scope.workers_for(role) == 0 and scope.environment_for(role) == {}
    assert scope.environment_for("a_role_nobody_declared") == {}
    assert inactive_scope().environment_for(ROLE_S3_COMMAND) == {}


@pytest.mark.parametrize(
    ("mutate", "problem"),
    [
        (lambda d: d.update(schema_version="other.v1"), "SCOPE_CONFIG_SCHEMA"),
        (lambda d: d.update(status="MAYBE"), "SCOPE_CONFIG_STATUS"),
        (lambda d: d.pop("status"), "SCOPE_CONFIG_STATUS"),
        (lambda d: d.update(version=""), "SCOPE_CONFIG_VERSION"),
        (lambda d: d.pop("version"), "SCOPE_CONFIG_VERSION"),
        (lambda d: d.update(workers=1), "SCOPE_CONFIG_WORKERS"),
        (lambda d: d.update(workers=9), "SCOPE_CONFIG_WORKERS"),
        (lambda d: d.update(workers="4"), "SCOPE_CONFIG_WORKERS"),
        (lambda d: d.update(workers=True), "SCOPE_CONFIG_WORKERS"),
        (lambda d: d["roles"].pop(ROLE_S3_COMMAND), "SCOPE_CONFIG_ROLES"),
        (lambda d: d["roles"].update(surprise={"value": "enabled"}), "SCOPE_CONFIG_ROLES"),
        (lambda d: d["roles"][ROLE_S3_COMMAND].update(value="maybe"), "SCOPE_CONFIG_ROLE_VALUE"),
        (lambda d: d["roles"].update({ROLE_S3_COMMAND: "enabled"}), "SCOPE_CONFIG_ROLE_VALUE"),
        (
            lambda d: d["roles"][ROLE_FORMAL_FULL].update(value="enabled"),
            "SCOPE_CONFIG_NEVER_ENABLED_ROLE",
        ),
        (
            lambda d: d["roles"][ROLE_NAMED_DQ_CHILDREN].update(value="enabled"),
            "SCOPE_CONFIG_NEVER_ENABLED_ROLE",
        ),
    ],
)
def test_an_invalid_policy_enables_nothing(tmp_path: Path, mutate: Any, problem: str) -> None:
    document = copy.deepcopy(_document())
    mutate(document)
    scope = load_scope(_root_with(tmp_path, document))
    assert scope.problem == problem
    assert not scope.active and scope.enabled_roles == frozenset()
    for role in ROLES:
        assert scope.environment_for(role) == {}


def test_a_missing_or_unparsable_policy_enables_nothing(tmp_path: Path) -> None:
    missing = load_scope(tmp_path)
    assert missing.status == "ABSENT" and not missing.active
    assert missing.problem is not None and missing.problem.startswith("SCOPE_CONFIG_UNREADABLE")
    for body in (b"::: not yaml :::\n\t- [", b"\xff\xfe\x00bad", b"status: a\nstatus: b\n"):
        target = tmp_path / SCOPE_CONFIG_PATH
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(body)
        scope = load_scope(tmp_path)
        assert not scope.active and scope.problem in {
            "SCOPE_CONFIG_NOT_YAML",
            "SCOPE_CONFIG_SCHEMA",
        }
        assert scope.config_sha256 == hashlib.sha256(body).hexdigest()


def test_a_duplicate_key_is_not_silently_resolved(tmp_path: Path) -> None:
    text = REAL.read_text(encoding="utf-8").replace("workers: 4\n", "workers: 4\nworkers: 8\n", 1)
    target = tmp_path / SCOPE_CONFIG_PATH
    target.parent.mkdir(parents=True)
    target.write_text(text, encoding="utf-8", newline="\n")
    scope = load_scope(tmp_path)
    assert not scope.active and scope.problem == "SCOPE_CONFIG_NOT_YAML"


@pytest.mark.parametrize(
    ("status", "in_force"),
    [
        ("PILOT_BASELINE", True),
        ("OWNER_APPROVED", True),
        ("PROPOSED_PENDING_OWNER_REVIEW", False),
        ("DISABLED", False),
    ],
)
def test_only_pilot_baseline_and_owner_approved_put_the_policy_in_force(
    tmp_path: Path, status: str, in_force: bool
) -> None:
    document = copy.deepcopy(_document())
    document["status"] = status
    scope = load_scope(_root_with(tmp_path, document))
    assert scope.problem is None and scope.status == status
    assert scope.active is in_force
    assert bool(scope.environment_for(ROLE_S3_COMMAND)) is in_force


def test_an_inherited_switch_is_removed_and_only_the_policy_may_set_it() -> None:
    shell = {"PATH": "x", PARALLEL_REPLAY_ENV: "8"}
    assert without_switch(shell) == {"PATH": "x"}
    assert shell[PARALLEL_REPLAY_ENV] == "8"  # the caller's mapping is not modified
    scope = load_scope(ROOT)

    target = dict(shell)
    apply_switch(scope, ROLE_S3_COMMAND, target)
    assert target == {"PATH": "x", PARALLEL_REPLAY_ENV: "4"}  # the shell's 8 never wins

    target = dict(shell)
    apply_switch(scope, ROLE_FORMAL_FULL, target)  # a role the policy leaves out: no switch at all
    assert target == {"PATH": "x"}

    target = dict(shell)
    apply_switch(inactive_scope("SCOPE_CONFIG_SCHEMA"), ROLE_S3_COMMAND, target)
    assert target == {"PATH": "x"}


def test_the_snapshot_for_the_run_record_is_plain_data() -> None:
    snapshot = load_scope(ROOT).to_dict()
    assert snapshot["active"] is True and snapshot["workers"] == 4
    assert snapshot["enabled_roles"] == sorted(ENABLED_ROLES)
    assert len(snapshot["config_sha256"]) == 64
    assert inactive_scope().to_dict()["active"] is False
    parsed = parse_scope(_document(), "d" * 64)
    assert isinstance(parsed, ParallelReplayScope) and parsed.config_sha256 == "d" * 64
