"""DEVX-023 candidate B: the replay seal is enabled in the publication chain only, by policy.

The seal itself is covered by test_arch_005_lease_replay_seal.py. These tests pin what candidate B
adds: the ``lease_seal`` section of the reviewed scope (same roles and invariants as the
parallel replay, nothing enabled when anything is wrong), the two switches that reach each role,
step A06 that rebuilds and verifies the seal before anything else replays the store with it, and
the bounded re-observation of the host (S3 follow-up g) that keeps the desktop app's transient
git.exe polling from stopping a chain.
"""

from __future__ import annotations

import copy
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
from publication_run_support import T0, World, make_run_config

from ai_trading_system.platform.architecture import lease_replay_seal
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
    SCHEMA_VERSION,
    SEAL_ENV,
    SEAL_ON_VALUE,
    ParallelReplayScope,
    apply_switch,
    inactive_scope,
    parse_scope,
    without_switch,
)
from ai_trading_system.platform.architecture.publication_commands import (
    LEASE_SEAL_SCRIPT,
    CommandResult,
)
from ai_trading_system.platform.architecture.publication_journal import PublicationJournal
from ai_trading_system.platform.architecture.publication_orchestrator import (
    OUTCOME_COMPLETE,
    OUTCOME_STOPPED_FAILED,
    PublicationRunEngine,
    StepContext,
)
from ai_trading_system.platform.architecture.publication_services import (
    PROCESS_SETTLE_ATTEMPTS,
    PROCESS_SETTLE_INTERVAL_SECONDS,
    HostFactsCollector,
)
from ai_trading_system.platform.architecture.publication_steps import build_precondition_steps

CHAIN_ROLES = frozenset({ROLE_S3_COMMAND, ROLE_VALIDATION_STAGE_1, ROLE_LOCAL_PUBLISH_WORKER})
NOW = datetime(2026, 10, 9, 12, 0, tzinfo=UTC)
_ABSENT = object()


def _roles(*enabled: str) -> dict[str, dict[str, Any]]:
    return {role: {"value": "enabled" if role in enabled else "disabled"} for role in ROLES}


def _document(
    *,
    seal: object = _ABSENT,
    parallel: tuple[str, ...] = (ROLE_S3_COMMAND,),
    status: str = "PILOT_BASELINE",
) -> dict[str, Any]:
    document: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "status": status,
        "version": "1.1.0",
        "workers": 4,
        "roles": _roles(*parallel),
    }
    if seal is not _ABSENT:
        document["lease_seal"] = seal
    return document


def _seal_section(*enabled: str) -> dict[str, Any]:
    return {"roles": _roles(*enabled)}


# ---------------------------------------------------------------------------------- the policy


def test_the_switch_name_and_value_are_the_ones_the_seal_module_reads() -> None:
    # the scope module does not import the lease kernel, so the copies are pinned by this test
    assert SEAL_ENV == lease_replay_seal.SEAL_ENV
    assert lease_replay_seal.seal_enabled({SEAL_ENV: SEAL_ON_VALUE})
    assert SEAL_ENV != PARALLEL_REPLAY_ENV


def test_a_policy_without_a_lease_seal_section_gives_no_seal() -> None:
    scope = parse_scope(_document(), "a" * 64)
    assert scope.problem is None and scope.active and scope.seal_roles == frozenset()
    assert scope.environment_for(ROLE_S3_COMMAND) == {PARALLEL_REPLAY_ENV: "4"}
    assert not scope.seal_for(ROLE_S3_COMMAND)


def test_the_seal_section_names_the_roles_that_get_the_seal() -> None:
    scope = parse_scope(_document(seal=_seal_section(*CHAIN_ROLES)), "b" * 64)
    assert scope.problem is None and scope.seal_roles == CHAIN_ROLES
    # the parallel replay and the seal are separate switches with separate roles
    assert scope.environment_for(ROLE_S3_COMMAND) == {
        PARALLEL_REPLAY_ENV: "4",
        SEAL_ENV: SEAL_ON_VALUE,
    }
    assert scope.environment_for(ROLE_VALIDATION_STAGE_1) == {SEAL_ENV: SEAL_ON_VALUE}
    for role in set(ROLES) - CHAIN_ROLES:
        assert scope.environment_for(role) == {} and not scope.seal_for(role)
    assert scope.to_dict()["seal_roles"] == sorted(CHAIN_ROLES)


def test_a_seal_only_policy_is_in_force_and_gives_no_parallel_workers() -> None:
    scope = parse_scope(_document(seal=_seal_section(ROLE_S3_COMMAND), parallel=()), "c" * 64)
    assert scope.problem is None and scope.active
    assert scope.workers_for(ROLE_S3_COMMAND) == 0
    assert scope.environment_for(ROLE_S3_COMMAND) == {SEAL_ENV: SEAL_ON_VALUE}


@pytest.mark.parametrize(
    ("mutate", "problem"),
    [
        (lambda d: d.update(lease_seal="enabled"), "SCOPE_CONFIG_SEAL_SECTION"),
        (lambda d: d.update(lease_seal=None), "SCOPE_CONFIG_SEAL_SECTION"),
        (lambda d: d["lease_seal"].pop("roles"), "SCOPE_CONFIG_SEAL_ROLES"),
        (lambda d: d["lease_seal"]["roles"].pop(ROLE_S3_COMMAND), "SCOPE_CONFIG_SEAL_ROLES"),
        (
            lambda d: d["lease_seal"]["roles"].update(surprise={"value": "enabled"}),
            "SCOPE_CONFIG_SEAL_ROLES",
        ),
        (
            lambda d: d["lease_seal"]["roles"][ROLE_S3_COMMAND].update(value="maybe"),
            "SCOPE_CONFIG_SEAL_ROLE_VALUE",
        ),
        (
            lambda d: d["lease_seal"]["roles"].update({ROLE_S3_COMMAND: "enabled"}),
            "SCOPE_CONFIG_SEAL_ROLE_VALUE",
        ),
        (
            lambda d: d["lease_seal"]["roles"][ROLE_FORMAL_FULL].update(value="enabled"),
            "SCOPE_CONFIG_SEAL_NEVER_ENABLED_ROLE",
        ),
        (
            lambda d: d["lease_seal"]["roles"][ROLE_NAMED_DQ_CHILDREN].update(value="enabled"),
            "SCOPE_CONFIG_SEAL_NEVER_ENABLED_ROLE",
        ),
    ],
)
def test_a_wrong_seal_section_enables_nothing_not_even_the_parallel_replay(
    mutate: Any, problem: str
) -> None:
    document = copy.deepcopy(_document(seal=_seal_section(*CHAIN_ROLES)))
    mutate(document)
    scope = parse_scope(document, "d" * 64)
    assert scope.problem == problem
    assert not scope.active and scope.enabled_roles == frozenset()
    assert scope.seal_roles == frozenset()
    for role in ROLES:
        assert scope.environment_for(role) == {}


def test_the_formal_full_and_the_restricted_children_never_get_the_seal() -> None:
    assert NEVER_ENABLED == {ROLE_FORMAL_FULL, ROLE_NAMED_DQ_CHILDREN}
    scope = parse_scope(_document(seal=_seal_section(*CHAIN_ROLES)), "e" * 64)
    for role in NEVER_ENABLED | {ROLE_VALIDATION_PRE_FULL_TIERS}:
        assert not scope.seal_for(role) and scope.environment_for(role) == {}


@pytest.mark.parametrize("status", ["PROPOSED_PENDING_OWNER_REVIEW", "DISABLED"])
def test_a_policy_that_is_not_in_force_gives_no_seal(status: str) -> None:
    scope = parse_scope(_document(seal=_seal_section(*CHAIN_ROLES), status=status), "f" * 64)
    assert scope.problem is None and not scope.active
    for role in ROLES:
        assert scope.environment_for(role) == {} and not scope.seal_for(role)


def test_both_inherited_switches_are_removed_and_only_the_policy_sets_them() -> None:
    shell = {"PATH": "x", PARALLEL_REPLAY_ENV: "8", SEAL_ENV: "yes"}
    assert without_switch(shell) == {"PATH": "x"}
    assert shell[SEAL_ENV] == "yes"  # the caller's mapping is not modified
    scope = parse_scope(_document(seal=_seal_section(ROLE_S3_COMMAND)), "1" * 64)
    target = dict(shell)
    apply_switch(scope, ROLE_S3_COMMAND, target)
    assert target == {"PATH": "x", PARALLEL_REPLAY_ENV: "4", SEAL_ENV: SEAL_ON_VALUE}
    target = dict(shell)
    apply_switch(scope, ROLE_VALIDATION_STAGE_1, target)  # a role the policy leaves out
    assert target == {"PATH": "x"}
    target = dict(shell)
    apply_switch(inactive_scope("SCOPE_CONFIG_SCHEMA"), ROLE_S3_COMMAND, target)
    assert target == {"PATH": "x"}


# ---------------------------------------------------------------------------------- step A06


class _Host:
    def __init__(self, world: World) -> None:
        self.executable = str(world.repo / ".venv" / "Scripts" / "python.exe")
        self.version = (3, 11)
        self.processes: tuple[str, ...] = ()
        self.free_gb = 500.0


def _preconditions(
    tmp_path: Path, world: World, *, seal_rebuild: bool
) -> tuple[PublicationRunEngine, PublicationJournal]:
    host = _Host(world)
    steps = build_precondition_steps(
        make_run_config(world),
        world,
        live_processes=lambda: host.processes,
        interpreter=lambda: (host.executable, host.version),
        free_disk_gb=lambda: host.free_gb,
        min_free_disk_gb=100.0,
        seal_rebuild=seal_rebuild,
    )
    journal = PublicationJournal(tmp_path / "run" / "journal.jsonl")
    engine = PublicationRunEngine(
        steps=steps,
        journal=journal,
        context=StepContext(run_id="r1"),
        monotonic=lambda: 0.0,
        now=lambda: T0,
    )
    return engine, journal


def _failed(journal: PublicationJournal, step: str) -> dict[str, Any]:
    detail = journal.replay().last_detail(step, status="FAILED")
    assert detail is not None
    return dict(detail)


def test_a06_is_not_applicable_when_the_scope_does_not_give_the_seal_to_the_command(
    tmp_path: Path,
) -> None:
    world = World(tmp_path)
    engine, journal = _preconditions(tmp_path, world, seal_rebuild=False)
    summary = engine.run()
    assert summary.outcome == OUTCOME_COMPLETE, summary.to_dict()
    detail = journal.replay().last_detail("A06.seal_rebuild")
    assert detail is not None and detail["seal"] == "NOT_APPLICABLE"
    assert world.named(LEASE_SEAL_SCRIPT) == []  # nothing was built, nothing was verified


def test_a06_builds_the_seal_for_this_candidate_and_proves_it_equal_to_the_serial_replay(
    tmp_path: Path,
) -> None:
    world = World(tmp_path)
    engine, journal = _preconditions(tmp_path, world, seal_rebuild=True)
    summary = engine.run()
    assert summary.outcome == OUTCOME_COMPLETE, summary.to_dict()
    calls = world.named(LEASE_SEAL_SCRIPT)
    assert [call[3] for call in calls] == ["build", "verify"]  # build first, then the proof
    assert calls[0][calls[0].index("--actor") + 1] == "integration-coordinator"
    detail = journal.replay().last_detail("A06.seal_rebuild")
    assert detail is not None and detail["seal"] == "BUILT_AND_VERIFIED"
    assert detail["sealed_chains"] == 901 and detail["sealed_events"] == 6371
    assert detail["seal_sha256"] == "5" * 64 and detail["kernel_fingerprint"] == "6" * 64
    assert detail["verify_sealed_seconds"] == 3.05 and detail["verify_full_seconds"] == 33.08


def test_a06_runs_after_the_dependency_gate_and_before_any_transaction(tmp_path: Path) -> None:
    world = World(tmp_path)
    engine, journal = _preconditions(tmp_path, world, seal_rebuild=True)
    engine.run()
    order = [row.step_id for row in engine.run().rows]
    assert order[-2:] == ["A05.dependency_gate", "A06.seal_rebuild"]
    assert journal.replay().last_detail("B10.prep_acquire") is None  # stage B has not started


def test_a_failed_seal_build_stops_the_run_before_verify(tmp_path: Path) -> None:
    for exit_code, body_status in ((2, "PASS"), (0, "BLOCKED")):
        world = World(tmp_path / f"w{exit_code}")
        world.seal_build_exit = exit_code
        world.seal_build_body = {**world.seal_build_body, "status": body_status}
        engine, journal = _preconditions(tmp_path / f"j{exit_code}", world, seal_rebuild=True)
        summary = engine.run()
        assert summary.outcome == OUTCOME_STOPPED_FAILED
        assert summary.stopped_step == "A06.seal_rebuild"
        assert _failed(journal, "A06.seal_rebuild")["code"] == "PUBLICATION_RUN_SEAL_BUILD"
        assert [call[3] for call in world.named(LEASE_SEAL_SCRIPT)] == ["build"]


def test_a_seal_that_differs_from_the_serial_replay_stops_the_run(tmp_path: Path) -> None:
    world = World(tmp_path)
    world.seal_verify_body = {
        **world.seal_verify_body,
        "ok": False,
        "reason": "differs",
        "differing_fields": ["lease_heads", "active_leases"],
    }
    engine, journal = _preconditions(tmp_path, world, seal_rebuild=True)
    summary = engine.run()
    assert summary.outcome == OUTCOME_STOPPED_FAILED and summary.stopped_step == "A06.seal_rebuild"
    failed = _failed(journal, "A06.seal_rebuild")
    assert failed["code"] == "PUBLICATION_RUN_SEAL_VERIFY"
    assert failed["differing_fields"] == ["lease_heads", "active_leases"]
    assert journal.replay().last_detail("B10.prep_acquire") is None


def test_a_verify_that_fails_to_run_or_prints_garbage_stops_the_run(tmp_path: Path) -> None:
    world = World(tmp_path / "exit")
    world.seal_verify_exit = 2
    engine, journal = _preconditions(tmp_path / "exit", world, seal_rebuild=True)
    assert engine.run().outcome == OUTCOME_STOPPED_FAILED
    assert _failed(journal, "A06.seal_rebuild")["code"] == "PUBLICATION_RUN_SEAL_VERIFY"
    garbage = World(tmp_path / "garbage")
    garbage.seal_verify_body = {"status": "PASS", "command": "verify"}  # no "ok" at all
    engine, journal = _preconditions(tmp_path / "garbage", garbage, seal_rebuild=True)
    assert engine.run().outcome == OUTCOME_STOPPED_FAILED
    assert _failed(journal, "A06.seal_rebuild")["code"] == "PUBLICATION_RUN_SEAL_VERIFY"


# ------------------------------------------------------------------------ S3 follow-up (g)


def _result(stdout: str) -> CommandResult:
    return CommandResult(("powershell.exe",), 0, stdout, "", 0.1, "log")


BURST = "300|1|git.exe|git.exe -c core.hooksPath=NUL -c safe.directory=* status"


def _collector(
    tmp_path: Path, listings: list[str], sleeps: list[float]
) -> tuple[HostFactsCollector, list[int]]:
    observations: list[int] = []

    def run_powershell(script: str) -> CommandResult:
        index = min(len(observations), len(listings) - 1)  # the last listing repeats
        observations.append(index)
        return _result(listings[index])

    collector = HostFactsCollector(
        repository_root=tmp_path,
        run_powershell=run_powershell,
        git=lambda args: "m" * 40,
        lease_replay=lambda: ("PASS", ("lease-1",)),
        now=lambda: NOW,
        own_pids=(100, 101),
        sleep=sleeps.append,
    )
    return collector, observations


def test_the_settle_window_is_long_enough_for_a_polling_burst_and_short_enough_to_fail_fast() -> (
    None
):
    window = (PROCESS_SETTLE_ATTEMPTS - 1) * PROCESS_SETTLE_INTERVAL_SECONDS
    assert PROCESS_SETTLE_ATTEMPTS >= 3 and 15.0 <= window <= 60.0


def test_a_clean_host_is_clean_at_once_without_waiting(tmp_path: Path) -> None:
    sleeps: list[float] = []
    collector, observations = _collector(tmp_path, [""], sleeps)
    assert collector.settled_live_processes() == ()
    assert len(observations) == 1 and sleeps == []


def test_a_transient_burst_of_the_desktop_apps_git_polling_does_not_stop_the_chain(
    tmp_path: Path,
) -> None:
    sleeps: list[float] = []
    collector, observations = _collector(tmp_path, [BURST, BURST, ""], sleeps)
    assert collector.settled_live_processes() == ()
    assert len(observations) == 3 and sleeps == [PROCESS_SETTLE_INTERVAL_SECONDS] * 2


def test_a_process_that_stays_still_fails_after_the_bounded_window(tmp_path: Path) -> None:
    sleeps: list[float] = []
    collector, observations = _collector(tmp_path, [BURST], sleeps)
    assert collector.settled_live_processes() == (BURST,)
    assert len(observations) == PROCESS_SETTLE_ATTEMPTS
    assert sleeps == [PROCESS_SETTLE_INTERVAL_SECONDS] * (PROCESS_SETTLE_ATTEMPTS - 1)


def test_the_pre_publication_checklist_uses_the_settled_observation(tmp_path: Path) -> None:
    (tmp_path / ".git").mkdir()
    transient, _ = _collector(tmp_path, [BURST, ""], [])
    facts = transient.pre_publish_facts(
        expected_main="m" * 40, expected_lease="lease-1", full_ended_at=NOW
    )
    assert facts.live_processes == ()
    persistent, _ = _collector(tmp_path, [BURST], [])
    facts = persistent.pre_publish_facts(
        expected_main="m" * 40, expected_lease="lease-1", full_ended_at=NOW
    )
    assert facts.live_processes == (BURST,)


def test_the_scope_type_still_constructs_without_seal_roles() -> None:
    # the P6 tests build scopes with the original six fields; the new field defaults to none
    scope = ParallelReplayScope(
        status="PILOT_BASELINE",
        version="1.0.0",
        workers=4,
        enabled_roles=frozenset({ROLE_S3_COMMAND}),
        config_sha256="a" * 64,
        problem=None,
    )
    assert scope.seal_roles == frozenset() and scope.environment_for(ROLE_S3_COMMAND) == {
        PARALLEL_REPLAY_ENV: "4"
    }
