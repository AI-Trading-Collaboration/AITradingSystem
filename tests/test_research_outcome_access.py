"""Synthetic gateway tests using a real capture hold, immutable files and the hold-store lock.

No provider, market cache, research adapter, DQ run, order or fill is executed.
Callback provenance remains TRUSTED_SYNTHETIC_CALLBACK_NOT_ATTESTED.
"""

from __future__ import annotations

import hashlib
import multiprocessing
import os
import shutil
import subprocess
from collections.abc import Iterator
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from test_research_experiment_envelope import make_admission, make_authority, make_envelope

import ai_trading_system.research_outcome_access as gateway
from ai_trading_system.contracts.prospective_event_time_evidence import (
    canonical_json_bytes,
    strict_json_loads,
)
from ai_trading_system.contracts.research_experiment_envelope import (
    ExperimentContractError,
    ExperimentEnvelope,
    LocalExperimentAuthority,
    OutcomeExposure,
    OutcomeInterval,
)
from ai_trading_system.data import capture_hold
from ai_trading_system.data.capture_hold import (
    HOLD_ROOT_RELATIVE,
    CaptureHold,
    acquire_capture_hold,
    release_capture_hold,
    restore_capture_hold,
)
from ai_trading_system.data.immutable_publish import DataPublicationIntegrityError

ROOT = Path(__file__).resolve().parents[1]
RESULT = b"synthetic secret outcome: 73"
# The gateway's ledger and checkpoint chain both live below this held directory.
HELD_PATHS = ("outputs/research",)


def _git(root: Path, *arguments: str) -> str:
    return subprocess.run(
        ["git", *arguments], cwd=root, check=True, capture_output=True, text=True
    ).stdout.strip()


def _hash(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


@dataclass
class _Fixture:
    root: Path
    commit: str
    hold: CaptureHold

    def configure(
        self, envelopes: tuple[ExperimentEnvelope, ...] | None = None, **changes: Any
    ) -> tuple[gateway.ResearchOutcomeAccessGateway, ExperimentEnvelope, LocalExperimentAuthority]:
        envelopes = envelopes or (make_envelope(),)
        envelope = envelopes[0]
        authority = replace(
            make_authority(envelope),
            canonical_execution_root=self.root.as_posix(),
            canonical_git_common_dir=(self.root / ".git").as_posix(),
            ledger_relative_path=changes.pop("ledger_relative_path", gateway.CANONICAL_LEDGER_PATH),
            approved_envelope_sha256s=tuple(item.canonical_sha256() for item in envelopes),
            approved_policy_sha256s=tuple(
                sorted(
                    {policy.canonical_sha256() for item in envelopes for policy in item.policies}
                )
            ),
            **changes,
        )
        authority = replace(authority, genesis_sha256=_hash(gateway.genesis_bytes(authority)))
        self.install(authority)
        service = gateway.ResearchOutcomeAccessGateway(
            execution_root=self.root, candidate_commit=self.commit
        )
        return service, envelope, authority

    def install(self, authority: LocalExperimentAuthority) -> None:
        target = self.root / gateway.AUTHORITY_PATH
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(authority.canonical_bytes())

    def freeze(
        self,
        service: gateway.ResearchOutcomeAccessGateway,
        envelope: ExperimentEnvelope,
        authority: LocalExperimentAuthority,
    ) -> str:
        evidence = gateway.local_freeze_time_evidence(
            envelope=envelope, recorded_at=datetime(2026, 1, 1, tzinfo=UTC)
        )
        admission = replace(
            make_admission(envelope, authority), time_evidence_sha256=_hash(evidence)
        )
        return service.freeze(
            envelope=envelope, admission=admission, time_evidence_bytes=evidence, hold=self.hold
        )

    def ready(
        self, *, look_mode: str = "FIRST_ONLY"
    ) -> tuple[gateway.ResearchOutcomeAccessGateway, ExperimentEnvelope, LocalExperimentAuthority]:
        service, envelope, authority = self.configure((make_envelope(look_mode=look_mode),))
        service.bootstrap(hold=self.hold)
        self.freeze(service, envelope, authority)
        service.begin_attempt(
            envelope_sha256=envelope.canonical_sha256(), attempt_id="attempt-1", hold=self.hold
        )
        return service, envelope, authority


def _acquire(root: Path, commit: str, paths: tuple[str, ...] = HELD_PATHS) -> CaptureHold:
    return acquire_capture_hold(
        execution_root=root,
        candidate_commit=commit,
        required_paths=paths,
        actor="integration-coordinator",
        ttl_seconds=3600,
    )


@pytest.fixture
def held(tmp_path: Path) -> Iterator[_Fixture]:
    root = tmp_path / "synthetic-outcome-access"
    root.mkdir()
    root = root.resolve()
    (root / ".gitignore").write_text("outputs/\n", encoding="utf-8")
    (root / "src").mkdir()
    (root / "src" / "placeholder.py").write_text("VALUE = 1\n", encoding="utf-8")
    _git(root, "init", "-b", "synthetic-outcome-access")
    _git(root, "config", "user.email", "outcome@example.invalid")
    _git(root, "config", "user.name", "Synthetic Outcome Access")
    _git(root, "config", "core.autocrlf", "false")
    _git(root, "add", ".gitignore", "src")
    _git(root, "commit", "-m", "synthetic outcome fixture")
    commit = _git(root, "rev-parse", "HEAD")
    handle = _acquire(root, commit)
    try:
        yield _Fixture(root, commit, handle)
    finally:
        if not handle.released:
            release_capture_hold(handle)


def _view(
    service: gateway.ResearchOutcomeAccessGateway, held: _Fixture, **changes: Any
) -> gateway.ReleasedSyntheticOutcome:
    arguments = {
        "attempt_id": "attempt-1",
        "access_id": "access-1",
        "loader": lambda: RESULT,
        "hold": held.hold,
    }
    arguments.update(changes)
    return service.view(**arguments)


def test_pending_is_durable_before_loader_and_metadata_never_contains_result(
    held: _Fixture,
) -> None:
    service, _, _ = held.ready()
    calls = []

    def load() -> bytes:
        pending = service.replay_metadata()
        assert pending["pending_access_count"] == 1
        assert pending["accesses"][0]["visibility"] == "POSSIBLY_EXPOSED"
        calls.append("called")
        return RESULT

    result = _view(service, held, loader=load)
    assert calls == ["called"] and result.content == RESULT
    assert result.receipt["result_sha256"] == _hash(RESULT)
    assert result.receipt["result_size_bytes"] == len(RESULT)
    assert result.receipt["callback_attestation"] == gateway.CALLBACK_ATTESTATION
    assert result.receipt["real_outcome_access_authorized"] is False
    assert result.receipt["oos_status"] == "NOT_ESTABLISHED"
    assert result.receipt["envelope_sha256"] == make_envelope().canonical_sha256()
    metadata = service.replay_metadata()
    assert (metadata["trial_count"], metadata["attempt_count"], metadata["access_count"]) == (
        1,
        1,
        1,
    )
    assert metadata["completed_access_count"] == 1
    assert metadata["real_outcome_access_authorized"] is False
    assert metadata["oos_status"] == "NOT_ESTABLISHED"
    assert metadata["checkpoint_head_sha256"] == result.receipt["checkpoint_head_sha256"]
    for relative in (*service.required_paths(),):
        assert all(
            RESULT not in path.read_bytes() for path in (held.root / relative).glob("*.json")
        )
    assert not gateway.CHECKPOINT_PATH.startswith(HOLD_ROOT_RELATIVE)
    assert not gateway.CANONICAL_LEDGER_PATH.startswith(HOLD_ROOT_RELATIVE)


def test_read_only_replay_after_hold_release_never_mutates_or_reloads(held: _Fixture) -> None:
    service, _, _ = held.ready()
    _view(service, held)
    release_capture_hold(held.hold)
    tree = held.root / "outputs"
    before = {path: path.read_bytes() for path in tree.rglob("*.json")}
    assert service.replay_metadata()["access_count"] == 1
    assert before == {path: path.read_bytes() for path in tree.rglob("*.json")}


def test_explicit_bootstrap_required_and_cannot_be_repeated(held: _Fixture) -> None:
    service, envelope, authority = held.configure()
    with pytest.raises(gateway.OutcomeAccessError, match="BOOTSTRAP_REQUIRED"):
        held.freeze(service, envelope, authority)
    assert not (held.root / service.required_paths()[0]).exists()
    assert service.bootstrap(hold=held.hold) == authority.genesis_sha256
    with pytest.raises(gateway.OutcomeAccessError, match="ALREADY_EXISTS"):
        service.bootstrap(hold=held.hold)


def test_wrong_genesis_never_bootstraps(held: _Fixture) -> None:
    service, _, authority = held.configure()
    held.install(replace(authority, genesis_sha256="0" * 64))
    with pytest.raises(gateway.OutcomeAccessError, match="GENESIS_HASH_MISMATCH"):
        service.bootstrap(hold=held.hold)
    assert not (held.root / authority.ledger_relative_path).exists()


@pytest.mark.parametrize(
    "damage",
    [
        "tail",
        "whole_ledger",
        "checkpoint_tail",
        "checkpoint_pair_tail",
        "whole_checkpoints",
        "event_predecessor",
        "checkpoint_predecessor",
        "extra_file",
        "event_hole",
    ],
)
def test_history_damage_fails_closed_before_loader(held: _Fixture, damage: str) -> None:
    service, _, authority = held.ready()
    ledger = held.root / authority.ledger_relative_path
    checkpoints = held.root / service.checkpoint_path
    if damage == "tail":
        sorted(ledger.glob("*.json"))[-1].unlink()
    elif damage == "whole_ledger":
        shutil.rmtree(ledger)
    elif damage == "checkpoint_tail":
        sorted(checkpoints.glob("*.json"))[-1].unlink()
    elif damage == "checkpoint_pair_tail":
        for target in sorted(checkpoints.glob("*.json"))[-2:]:
            target.unlink()
    elif damage == "whole_checkpoints":
        shutil.rmtree(checkpoints)
    elif damage == "extra_file":
        (ledger / "partial.tmp").write_bytes(b"partial")
    elif damage == "event_hole":
        (ledger / "000000000001.json").unlink()
    else:
        target = sorted((ledger if damage == "event_predecessor" else checkpoints).glob("*.json"))[
            -1
        ]
        row = strict_json_loads(target.read_bytes())
        row[
            (
                "previous_event_sha256"
                if damage == "event_predecessor"
                else "previous_checkpoint_sha256"
            )
        ] = "0" * 64
        target.write_bytes(canonical_json_bytes(row))
    calls = []
    with pytest.raises((ValueError, OSError, DataPublicationIntegrityError)):
        _view(service, held, loader=lambda: calls.append(1))
    assert calls == []
    with pytest.raises((ValueError, OSError, DataPublicationIntegrityError)):
        service.bootstrap(hold=held.hold)


@pytest.mark.parametrize("phase", ["PREPARED", "EVENT", "COMPLETED"])
def test_pending_write_failure_does_not_call_loader_or_repair_history(
    held: _Fixture, monkeypatch: pytest.MonkeyPatch, phase: str
) -> None:
    service, _, _ = held.ready()
    publish = service._publish

    def damaged(relative: str, row: dict[str, Any]) -> None:
        matches = row.get("phase") == phase or (
            phase == "EVENT" and row.get("kind") == "VIEW_PENDING"
        )
        if matches:
            raise OSError("synthetic durable write failure")
        publish(relative, row)

    monkeypatch.setattr(service, "_publish", damaged)
    calls = []
    with pytest.raises(OSError):
        _view(service, held, loader=lambda: calls.append(1))
    assert calls == []
    monkeypatch.setattr(service, "_publish", publish)
    if phase != "PREPARED":
        with pytest.raises(gateway.OutcomeAccessError, match="CHECKPOINT_INCOMPLETE"):
            _view(service, held, loader=lambda: calls.append(1))
    else:
        assert service.replay_metadata()["access_count"] == 0
    assert calls == []


def test_terminal_write_failure_preserves_possible_exposure(
    held: _Fixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    service, _, _ = held.ready()
    publish = service._publish

    def damaged(relative: str, row: dict[str, Any]) -> None:
        if row.get("kind") == "VIEW_COMPLETED":
            raise OSError("synthetic terminal write failure")
        publish(relative, row)

    monkeypatch.setattr(service, "_publish", damaged)
    calls = []
    with pytest.raises(OSError):
        _view(service, held, loader=lambda: (calls.append(1), RESULT)[1])
    assert calls == [1]
    monkeypatch.setattr(service, "_publish", publish)
    with pytest.raises(gateway.OutcomeAccessError, match="CHECKPOINT_INCOMPLETE"):
        _view(service, held, access_id="access-2", loader=lambda: calls.append(2))
    assert calls == [1]


@pytest.mark.parametrize("failure", ["exception", "wrong_type"])
def test_failed_loader_never_restores_unseen_or_leaks_exception_text(
    held: _Fixture, failure: str, capsys: pytest.CaptureFixture[str]
) -> None:
    service, envelope, _ = held.ready(look_mode="REPEAT_DECLARED")

    def load() -> Any:
        print("synthetic partial stdout")
        if failure == "exception":
            raise ValueError(RESULT.decode())
        return RESULT.decode()

    with pytest.raises(gateway.OutcomeAccessError) as caught:
        _view(service, held, loader=load)
    assert RESULT.decode() not in str(caught.value)
    assert caught.value.__context__ is None
    assert "partial stdout" in capsys.readouterr().out
    metadata = service.replay_metadata()
    assert metadata["failed_access_count"] == metadata["possibly_exposed_count"] == 1
    assert RESULT.decode() not in str(metadata)
    with pytest.raises(gateway.OutcomeAccessError, match="ALREADY_POSSIBLY_EXPOSED"):
        _view(service, held, access_id="access-2")
    with pytest.raises(gateway.OutcomeAccessError, match="ALREADY_POSSIBLY_EXPOSED"):
        service.begin_attempt(
            envelope_sha256=envelope.canonical_sha256(), attempt_id="attempt-2", hold=held.hold
        )


def test_repeat_requires_declared_policy_new_access_and_same_result_binding(
    held: _Fixture,
) -> None:
    service, _, _ = held.ready(look_mode="REPEAT_DECLARED")
    _view(service, held)
    with pytest.raises(gateway.OutcomeAccessError, match="ACCESS_DUPLICATE"):
        _view(service, held)
    _view(service, held, access_id="access-2")
    assert service.replay_metadata()["access_count"] == 2
    with pytest.raises(gateway.OutcomeAccessError, match="REPEATED_RESULT_MISMATCH"):
        _view(service, held, access_id="access-3", loader=lambda: b"changed outcome")
    metadata = service.replay_metadata()
    assert metadata["access_count"] == 3 and metadata["failed_access_count"] == 1
    with pytest.raises(gateway.OutcomeAccessError, match="ALREADY_POSSIBLY_EXPOSED"):
        _view(service, held, access_id="access-4")


def test_first_only_rejects_second_access_without_loader(held: _Fixture) -> None:
    service, _, _ = held.ready()
    _view(service, held)
    calls = []
    with pytest.raises(gateway.OutcomeAccessError, match="ALREADY_POSSIBLY_EXPOSED"):
        _view(service, held, access_id="access-2", loader=lambda: calls.append(1))
    assert calls == [] and service.replay_metadata()["access_count"] == 1


@pytest.mark.parametrize(
    "history", ["UNKNOWN", "KNOWN", "PARTIAL", "POSSIBLY_EXPOSED", "UNKNOWN_EXPOSURE"]
)
def test_prior_inventory_blocks_first_access(held: _Fixture, history: str) -> None:
    envelope = make_envelope()
    changes: dict[str, Any] = (
        {"history_status": "UNKNOWN"}
        if history == "UNKNOWN"
        else {
            "prior_exposures": (
                OutcomeExposure(
                    OutcomeInterval(date(2026, 1, 1), date(2026, 1, 1)),
                    "UNKNOWN" if history == "UNKNOWN_EXPOSURE" else history,
                    "synthetic-prior",
                ),
            ),
        }
    )
    service, envelope, authority = held.configure((envelope,), **changes)
    service.bootstrap(hold=held.hold)
    held.freeze(service, envelope, authority)
    with pytest.raises(gateway.OutcomeAccessError, match="PRIOR_OR_UNKNOWN_HISTORY"):
        service.begin_attempt(
            envelope_sha256=envelope.canonical_sha256(), attempt_id="attempt-1", hold=held.hold
        )


@pytest.mark.parametrize("role", ["DISCOVERY_KNOWN", "TRAIN", "SEEN_VALIDATION"])
def test_known_data_role_cannot_be_called_unseen(held: _Fixture, role: str) -> None:
    service, envelope, authority = held.configure((replace(make_envelope(), data_role=role),))
    service.bootstrap(hold=held.hold)
    held.freeze(service, envelope, authority)
    with pytest.raises(gateway.OutcomeAccessError, match="ROLE_NOT_UNTOUCHED"):
        service.begin_attempt(
            envelope_sha256=envelope.canonical_sha256(), attempt_id="attempt-1", hold=held.hold
        )


@pytest.mark.parametrize(
    "revision",
    [
        "candidate_id",
        "family_id",
        "candidate_parameters_sha256",
        "implementation_sha256",
        "input_information_set_sha256",
    ],
)
def test_renaming_and_revising_do_not_reset_domain_history(held: _Fixture, revision: str) -> None:
    first = make_envelope()
    revised = replace(
        first,
        envelope_id="revised-envelope",
        **{revision: "9" * 64 if revision.endswith("sha256") else "renamed"},
    )
    service, _, authority = held.configure((first, revised))
    service.bootstrap(hold=held.hold)
    for envelope in (first, revised):
        held.freeze(service, envelope, authority)
    service.begin_attempt(
        envelope_sha256=first.canonical_sha256(), attempt_id="attempt-1", hold=held.hold
    )
    _view(service, held)
    with pytest.raises(gateway.OutcomeAccessError, match="ALREADY_POSSIBLY_EXPOSED"):
        service.begin_attempt(
            envelope_sha256=revised.canonical_sha256(), attempt_id="attempt-2", hold=held.hold
        )
    assert service.replay_metadata()["trial_count"] == 2


@pytest.mark.parametrize("same_identity", ["domain", "information_set"])
@pytest.mark.parametrize("exposed", [False, True])
def test_alias_ledger_inherits_global_checkpoint_exposure(
    held: _Fixture, same_identity: str, exposed: bool
) -> None:
    first_service, first, old_authority = held.ready()
    if exposed:
        _view(first_service, held)
    alias = replace(
        first,
        envelope_id="alias-envelope",
        domain_id="alias-domain",
        family_id="alias-family",
        input_information_set_sha256=(
            first.input_information_set_sha256 if same_identity == "information_set" else "8" * 64
        ),
    )
    service, alias, authority = held.configure(
        (alias,),
        domain_definition_sha256=(
            old_authority.domain_definition_sha256 if same_identity == "domain" else "9" * 64
        ),
    )
    held.freeze(service, alias, authority)
    if exposed:
        with pytest.raises(gateway.OutcomeAccessError, match="ALREADY_POSSIBLY_EXPOSED"):
            service.begin_attempt(
                envelope_sha256=alias.canonical_sha256(),
                attempt_id="attempt-alias",
                hold=held.hold,
            )
    else:
        service.begin_attempt(
            envelope_sha256=alias.canonical_sha256(), attempt_id="attempt-alias", hold=held.hold
        )
        assert service.replay_metadata()["attempt_count"] == 2


def test_nonoverlapping_interval_can_start_new_trial(held: _Fixture) -> None:
    first = make_envelope()
    second = replace(
        first,
        envelope_id="next-interval",
        requested_interval=OutcomeInterval(date(2026, 2, 1), date(2026, 2, 28)),
        evaluated_interval=OutcomeInterval(date(2026, 2, 2), date(2026, 2, 27)),
    )
    service, _, authority = held.configure((first, second))
    service.bootstrap(hold=held.hold)
    for envelope in (first, second):
        held.freeze(service, envelope, authority)
    service.begin_attempt(
        envelope_sha256=first.canonical_sha256(), attempt_id="attempt-1", hold=held.hold
    )
    _view(service, held)
    service.begin_attempt(
        envelope_sha256=second.canonical_sha256(), attempt_id="attempt-2", hold=held.hold
    )
    _view(service, held, attempt_id="attempt-2", access_id="access-2")
    assert service.replay_metadata()["access_count"] == 2


@pytest.mark.parametrize("scope", ["REAL_RESEARCH", "PRODUCTION", None, True])
def test_real_scope_rejected_before_loader(held: _Fixture, scope: Any) -> None:
    service, _, _ = held.ready()
    calls = []
    with pytest.raises(gateway.OutcomeAccessError, match="REAL_SCOPE_NOT_ADMITTED"):
        _view(service, held, scope=scope, loader=lambda: calls.append(1))
    assert calls == [] and service.replay_metadata()["access_count"] == 0


def test_changed_checkout_commit_and_released_hold_rejected(held: _Fixture) -> None:
    service, _, _ = held.ready()
    calls = []
    _git(held.root, "commit", "--allow-empty", "-m", "changed candidate")
    with pytest.raises(ValueError):
        _view(service, held, loader=lambda: calls.append(1))
    release_capture_hold(held.hold)
    with pytest.raises(ValueError):
        _view(service, held, loader=lambda: calls.append(1))
    assert calls == []


@pytest.mark.parametrize("field", ["canonical_execution_root", "canonical_git_common_dir"])
def test_authority_cannot_redirect_canonical_root(held: _Fixture, field: str) -> None:
    service, _, authority = held.ready()
    held.install(replace(authority, **{field: (held.root.parent / "another-root").as_posix()}))
    with pytest.raises(gateway.OutcomeAccessError, match="CANONICAL_ROOT_MISMATCH"):
        _view(service, held)


@pytest.mark.parametrize(
    "change",
    ["unapproved_envelope", "unapproved_policy", "review_reference", "time_hash", "time_order"],
)
def test_freeze_requires_exact_review_and_time_binding(held: _Fixture, change: str) -> None:
    service, envelope, authority = held.configure()
    if change == "unapproved_policy":
        authority = replace(authority, approved_policy_sha256s=("0" * 64,))
        held.install(authority)
    service.bootstrap(hold=held.hold)
    if change == "unapproved_envelope":
        envelope = replace(envelope, candidate_id="new-unapproved-candidate")
    evidence = gateway.local_freeze_time_evidence(
        envelope=envelope, recorded_at=datetime(2026, 1, 1, tzinfo=UTC)
    )
    admission = replace(make_admission(envelope, authority), time_evidence_sha256=_hash(evidence))
    if change == "review_reference":
        admission = replace(admission, owner_review_reference="unreviewed-self-assertion")
    elif change == "time_hash":
        admission = replace(admission, time_evidence_sha256="0" * 64)
    elif change == "time_order":
        admission = replace(admission, reviewed_at=datetime(2026, 1, 2, tzinfo=UTC))
    with pytest.raises((gateway.OutcomeAccessError, ExperimentContractError)):
        service.freeze(
            envelope=envelope, admission=admission, time_evidence_bytes=evidence, hold=held.hold
        )
    assert service.replay_metadata()["trial_count"] == 0


def test_freeze_idempotency_and_new_authority_cannot_change_frozen_policy(held: _Fixture) -> None:
    service, envelope, authority = held.ready()
    first = held.freeze(service, envelope, authority)
    assert held.freeze(service, envelope, authority) == first
    assert service.replay_metadata()["trial_count"] == 1
    held.install(replace(authority, version="v2"))
    with pytest.raises(gateway.OutcomeAccessError, match="FROZEN_AUTHORITY_CHANGED"):
        _view(service, held)


def _child_view(
    root_text: str,
    commit: str,
    hold_id: str,
    access_id: str,
    connection: Any,
) -> None:
    """Spawned fresh interpreter; real existing-hold restore, no inherited lock."""
    try:
        service = gateway.ResearchOutcomeAccessGateway(
            execution_root=Path(root_text), candidate_commit=commit
        )
        hold = restore_capture_hold(
            execution_root=Path(root_text),
            hold_id=hold_id,
            candidate_commit=commit,
            required_paths=service.required_paths(),
        )

        def load() -> bytes:
            connection.send("STARTED")
            if connection.recv() != "FINISH":
                raise RuntimeError("synthetic process handshake mismatch")
            return RESULT

        service.view(attempt_id="attempt-1", access_id=access_id, loader=load, hold=hold)
        connection.send("COMPLETED")
    except BaseException as exc:
        connection.send(getattr(exc, "code", type(exc).__name__))
    finally:
        connection.close()


@pytest.mark.parametrize("kill_first", [False, True])
def test_multiprocess_same_live_hold_serializes_and_crash_keeps_pending(
    held: _Fixture, kill_first: bool
) -> None:
    service, _, _ = held.ready()
    context = multiprocessing.get_context("spawn")
    parent_connection, child_connection = context.Pipe()
    child = context.Process(
        target=_child_view,
        args=(
            str(held.root),
            held.commit,
            held.hold.hold_id,
            "access-1",
            child_connection,
        ),
    )
    child.start()
    child_connection.close()
    try:
        assert parent_connection.poll(30)
        assert parent_connection.recv() == "STARTED"
        assert service.replay_metadata()["pending_access_count"] == 1
        calls = []
        with pytest.raises(ValueError) as caught:
            _view(service, held, access_id="access-2", loader=lambda: calls.append(1))
        assert "ALREADY_POSSIBLY_EXPOSED" in str(caught.value) and calls == []
        if kill_first:
            child.terminate()
            child.join(30)
            assert not child.is_alive()
            with pytest.raises(gateway.OutcomeAccessError, match="ALREADY_POSSIBLY_EXPOSED"):
                _view(service, held, access_id="access-3", loader=lambda: calls.append(1))
            assert service.replay_metadata()["pending_access_count"] == 1 and calls == []
        else:
            parent_connection.send("FINISH")
            child.join(30)
            assert not child.is_alive() and parent_connection.poll(10)
            assert parent_connection.recv() == "COMPLETED"
            assert service.replay_metadata()["completed_access_count"] == 1
    finally:
        if child.is_alive():
            child.terminate()
            child.join(30)
        parent_connection.close()


def test_backward_local_clock_cannot_append_or_call_loader(
    held: _Fixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    service, _, _ = held.ready()
    before = datetime.now(UTC) - timedelta(hours=1)
    monkeypatch.setattr(capture_hold, "_now", lambda: before)
    calls = []
    with pytest.raises(ValueError):
        _view(service, held, loader=lambda: calls.append(1))
    assert calls == []


@pytest.mark.parametrize("control", [SystemExit, KeyboardInterrupt, GeneratorExit])
def test_process_control_propagates_unchanged_with_durable_pending(
    held: _Fixture, control: type[BaseException]
) -> None:
    service, _, _ = held.ready()
    original = control("synthetic cancellation")

    def load() -> bytes:
        raise original

    with pytest.raises(control) as caught:
        _view(service, held, loader=load)
    assert caught.value is original
    assert service.replay_metadata()["pending_access_count"] == 1
    with pytest.raises(gateway.OutcomeAccessError, match="ALREADY_POSSIBLY_EXPOSED"):
        _view(service, held, access_id="access-2")


def test_callback_can_take_the_hold_store_lock_while_access_is_pending(held: _Fixture) -> None:
    service, _, _ = held.ready()

    def load() -> bytes:
        # Acquisition serializes on the same store lock the gateway uses for replay/append.
        release_capture_hold(_acquire(held.root, held.commit, ("outputs/scratch",)))
        return RESULT

    assert _view(service, held, loader=load).content == RESULT


def test_callback_can_complete_nonoverlapping_access_before_its_fresh_terminal_replay(
    held: _Fixture,
) -> None:
    first = make_envelope()
    second = replace(
        first,
        envelope_id="interleaved-envelope",
        requested_interval=OutcomeInterval(date(2026, 2, 1), date(2026, 2, 28)),
        evaluated_interval=OutcomeInterval(date(2026, 2, 2), date(2026, 2, 27)),
    )
    service, _, authority = held.configure((first, second))
    service.bootstrap(hold=held.hold)
    for envelope in (first, second):
        held.freeze(service, envelope, authority)
    service.begin_attempt(
        envelope_sha256=first.canonical_sha256(), attempt_id="attempt-1", hold=held.hold
    )

    def load() -> bytes:
        service.begin_attempt(
            envelope_sha256=second.canonical_sha256(), attempt_id="attempt-2", hold=held.hold
        )
        assert _view(service, held, attempt_id="attempt-2", access_id="access-2").content == RESULT
        return RESULT

    assert _view(service, held, loader=load).content == RESULT
    metadata = service.replay_metadata()
    assert metadata["completed_access_count"] == metadata["access_count"] == 2
    assert metadata["pending_access_count"] == 0


@pytest.mark.parametrize("origin", ["access", "inventory"])
def test_alias_then_input_revision_inherits_transitive_history(held: _Fixture, origin: str) -> None:
    first = make_envelope()
    changes = (
        {}
        if origin == "access"
        else {
            "prior_exposures": (
                OutcomeExposure(first.requested_interval, "KNOWN", "original-inventory"),
            ),
        }
    )
    service, first, authority = held.configure((first,), **changes)
    service.bootstrap(hold=held.hold)
    held.freeze(service, first, authority)
    if origin == "access":
        service.begin_attempt(
            envelope_sha256=first.canonical_sha256(), attempt_id="attempt-1", hold=held.hold
        )
        _view(service, held)
    bridge = replace(first, envelope_id="bridge-envelope", domain_id="bridge-domain")
    service, bridge, authority = held.configure((bridge,), domain_definition_sha256="9" * 64)
    held.freeze(service, bridge, authority)
    reason = "ALREADY_POSSIBLY_EXPOSED" if origin == "access" else "PRIOR_OR_UNKNOWN_HISTORY"
    with pytest.raises(gateway.OutcomeAccessError, match=reason):
        service.begin_attempt(
            envelope_sha256=bridge.canonical_sha256(),
            attempt_id="bridge-attempt",
            hold=held.hold,
        )
    revision = replace(
        bridge, envelope_id="revision-envelope", input_information_set_sha256="8" * 64
    )
    service, revision, authority = held.configure((revision,), domain_definition_sha256="9" * 64)
    held.freeze(service, revision, authority)
    with pytest.raises(gateway.OutcomeAccessError, match=reason):
        service.begin_attempt(
            envelope_sha256=revision.canonical_sha256(),
            attempt_id="revision-attempt",
            hold=held.hold,
        )
    assert service.replay_metadata()["trial_count"] == 3


def test_later_reviewed_identity_bridge_blocks_preexisting_attempt_before_loader(
    held: _Fixture,
) -> None:
    service, first, _ = held.ready()
    _view(service, held)
    revision = replace(
        first,
        envelope_id="revision-before-bridge",
        domain_id="revised-domain",
        input_information_set_sha256="8" * 64,
    )
    service, revision, revision_authority = held.configure(
        (revision,), domain_definition_sha256="9" * 64
    )
    held.freeze(service, revision, revision_authority)
    service.begin_attempt(
        envelope_sha256=revision.canonical_sha256(),
        attempt_id="revision-attempt",
        hold=held.hold,
    )
    bridge = replace(first, envelope_id="later-bridge", domain_id="revised-domain")
    service, bridge, authority = held.configure((bridge,), domain_definition_sha256="9" * 64)
    held.freeze(service, bridge, authority)
    held.install(revision_authority)
    calls = []
    with pytest.raises(gateway.OutcomeAccessError, match="ALREADY_POSSIBLY_EXPOSED"):
        _view(
            service,
            held,
            attempt_id="revision-attempt",
            access_id="revision-access",
            loader=lambda: calls.append(1),
        )
    assert calls == [] and service.replay_metadata()["access_count"] == 1


def test_root_case_equivalence_is_consistent_before_and_after_bootstrap(held: _Fixture) -> None:
    service, envelope, authority = held.configure()
    changed = replace(
        authority,
        canonical_execution_root=authority.canonical_execution_root.upper(),
        canonical_git_common_dir=authority.canonical_git_common_dir.upper(),
    )
    changed = replace(changed, genesis_sha256=_hash(gateway.genesis_bytes(changed)))
    held.install(changed)
    if os.name != "nt":
        with pytest.raises(gateway.OutcomeAccessError, match="CANONICAL_ROOT_MISMATCH"):
            service.bootstrap(hold=held.hold)
        assert not (held.root / gateway.CANONICAL_LEDGER_PATH).exists()
    else:
        service.bootstrap(hold=held.hold)
        held.freeze(service, envelope, changed)
        assert service.replay_metadata()["trial_count"] == 1


def test_retained_domain_inventory_inherits_outside_old_envelope_interval(held: _Fixture) -> None:
    old = replace(
        make_envelope(),
        requested_interval=OutcomeInterval(date(2026, 2, 1), date(2026, 2, 28)),
        evaluated_interval=OutcomeInterval(date(2026, 2, 2), date(2026, 2, 27)),
    )
    service, old, authority = held.configure(
        (old,),
        prior_exposures=(
            OutcomeExposure(make_envelope().requested_interval, "KNOWN", "old-domain-inventory"),
        ),
    )
    service.bootstrap(hold=held.hold)
    held.freeze(service, old, authority)
    alias = replace(make_envelope(), domain_id="new-domain-alias")
    service, alias, new_authority = held.configure((alias,))
    held.freeze(service, alias, new_authority)
    with pytest.raises(gateway.OutcomeAccessError, match="PRIOR_OR_UNKNOWN_HISTORY"):
        service.begin_attempt(
            envelope_sha256=alias.canonical_sha256(), attempt_id="alias-attempt", hold=held.hold
        )


def test_independent_domain_and_information_set_are_distinct_reviewed_scope(
    held: _Fixture,
) -> None:
    service, old, _ = held.ready()
    _view(service, held)
    alias = replace(
        old,
        envelope_id="independent-envelope",
        domain_id="independent-domain",
        input_information_set_sha256="8" * 64,
    )
    service, alias, authority = held.configure((alias,), domain_definition_sha256="9" * 64)
    held.freeze(service, alias, authority)
    service.begin_attempt(
        envelope_sha256=alias.canonical_sha256(),
        attempt_id="independent-attempt",
        hold=held.hold,
    )
    _view(service, held, attempt_id="independent-attempt", access_id="independent-access")
    assert service.replay_metadata()["trial_count"] == 2


def test_output_path_cannot_select_a_fresh_ledger(held: _Fixture) -> None:
    _, _, authority = held.ready()
    held.install(replace(authority, ledger_relative_path="outputs/new-empty-history"))
    with pytest.raises(gateway.OutcomeAccessError, match="CANONICAL_LEDGER_MISMATCH"):
        gateway.ResearchOutcomeAccessGateway(execution_root=held.root, candidate_commit=held.commit)


@pytest.mark.parametrize("drift", ["record", "root", "expired"])
def test_live_hold_identity_is_revalidated_on_every_use(
    held: _Fixture, drift: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    service, _, _ = held.ready()
    calls = []
    with monkeypatch.context() as patch:
        if drift == "record":
            patch.setattr(held.hold, "record_bytes", held.hold.record_bytes + b" ")
        elif drift == "root":
            patch.setattr(held.hold, "root", held.root / "outputs")
        else:
            patch.setattr(capture_hold, "_now", lambda: datetime.now(UTC) + timedelta(days=30))
        with pytest.raises(ValueError):
            _view(service, held, loader=lambda: calls.append(1))
    assert calls == [] and service.replay_metadata()["access_count"] == 0


@pytest.mark.parametrize("missing", ["ledger", "checkpoint"])
def test_real_hold_must_claim_both_durable_resources(held: _Fixture, missing: str) -> None:
    service, _, _ = held.ready()
    release_capture_hold(held.hold)
    ledger, checkpoint = service.required_paths()
    narrow = _acquire(held.root, held.commit, (checkpoint if missing == "ledger" else ledger,))
    calls = []
    try:
        with pytest.raises(ValueError, match="CAPTURE_HOLD_SCOPE_INVALID"):
            _view(service, held, hold=narrow, loader=lambda: calls.append(1))
        assert calls == []
    finally:
        release_capture_hold(narrow)


def _rewrite_test_history(
    service: gateway.ResearchOutcomeAccessGateway, held: _Fixture, events: list[Any]
) -> None:
    """Deliberate synthetic corruption, preserving hashes to test semantic replay."""
    ledger = held.root / gateway.CANONICAL_LEDGER_PATH
    for index, event in enumerate(events):
        event["previous_event_sha256"] = (
            _hash(canonical_json_bytes(events[index - 1])) if index else None
        )
        (ledger / f"{index:012d}.json").write_bytes(canonical_json_bytes(event))
    folder = held.root / service.checkpoint_path
    checkpoints = [strict_json_loads(path.read_bytes()) for path in sorted(folder.glob("*.json"))]
    for index, row in enumerate(checkpoints):
        row["previous_checkpoint_sha256"] = (
            _hash(canonical_json_bytes(checkpoints[index - 1])) if index else None
        )
        if row["phase"] == "PREPARED":
            row["event_sha256"] = _hash(canonical_json_bytes(events[row["event_sequence"]]))
        else:
            row["prepared_sha256"] = _hash(canonical_json_bytes(checkpoints[index - 1]))
        (folder / f"{index:012d}.json").write_bytes(canonical_json_bytes(row))


def test_replay_rejects_backdated_event_even_with_consistent_hashes(held: _Fixture) -> None:
    service, _, _ = held.ready()
    ledger = held.root / gateway.CANONICAL_LEDGER_PATH
    events = [strict_json_loads(path.read_bytes()) for path in sorted(ledger.glob("*.json"))]
    events[-1]["payload"]["occurred_at"] = "2026-01-01T00:00:00+00:00"
    _rewrite_test_history(service, held, events)
    with pytest.raises(gateway.OutcomeAccessError, match="EVENT_TIME_INVALID"):
        service.replay_metadata()


def test_retained_authority_must_reconstruct_exact_genesis_bytes(held: _Fixture) -> None:
    service, _, _ = held.ready()
    ledger = held.root / gateway.CANONICAL_LEDGER_PATH
    events = [strict_json_loads(path.read_bytes()) for path in sorted(ledger.glob("*.json"))]
    payload = events[1]["payload"]
    authority = replace(
        LocalExperimentAuthority.from_dict(payload["authority"]),
        canonical_execution_root=(held.root.parent / "different-root").as_posix(),
    )
    payload["authority"] = authority.to_dict()
    payload["admission"]["authority_sha256"] = authority.canonical_sha256()
    _rewrite_test_history(service, held, events)
    with pytest.raises(gateway.OutcomeAccessError, match="FREEZE_GENESIS_MISMATCH"):
        service.replay_metadata()


@pytest.mark.skipif(
    os.name == "nt",
    reason="POSIX symlink fixture; Windows containment is exercised by immutable publisher suite",
)
def test_symlink_ledger_rejected_without_following(held: _Fixture) -> None:
    service, _, authority = held.configure()
    outside = held.root.parent / "outside"
    outside.mkdir()
    ledger = held.root / authority.ledger_relative_path
    ledger.parent.mkdir(parents=True, exist_ok=True)
    ledger.symlink_to(outside, target_is_directory=True)
    with pytest.raises(gateway.OutcomeAccessError, match="PATH_UNSAFE"):
        service.bootstrap(hold=held.hold)
    assert list(outside.iterdir()) == []
