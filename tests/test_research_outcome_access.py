"""Synthetic gateway tests using real S4D leases, immutable files and OS arbiter.

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
from ai_trading_system.data.immutable_publish import DataPublicationIntegrityError
from ai_trading_system.data.named_quality_dispatch import restore_named_capture_lease
from ai_trading_system.platform.architecture.checkout_guard import (
    CheckoutLeaseGuard,
    CheckoutLeaseHandle,
    CheckoutOperationClass,
)

ROOT = Path(__file__).resolve().parents[1]
RESULT = b"synthetic secret outcome: 73"


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
    lease: CheckoutLeaseHandle

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
            envelope=envelope, admission=admission, time_evidence_bytes=evidence, lease=self.lease
        )

    def ready(
        self, *, look_mode: str = "FIRST_ONLY"
    ) -> tuple[gateway.ResearchOutcomeAccessGateway, ExperimentEnvelope, LocalExperimentAuthority]:
        service, envelope, authority = self.configure((make_envelope(look_mode=look_mode),))
        service.bootstrap(lease=self.lease)
        self.freeze(service, envelope, authority)
        service.begin_attempt(
            envelope_sha256=envelope.canonical_sha256(), attempt_id="attempt-1", lease=self.lease
        )
        return service, envelope, authority


@pytest.fixture
def leased(tmp_path: Path) -> Iterator[_Fixture]:
    root = tmp_path / "synthetic-outcome-access"
    root.mkdir()
    (root / ".gitignore").write_text("outputs/\n", encoding="utf-8")
    for relative in (
        "config/architecture/arch_005_s4d_checkout_guard.yaml",
        "config/architecture/arch_005_parallel_control_policy.yaml",
    ):
        target = root / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes((ROOT / relative).read_bytes())
    _git(root, "init", "-b", "synthetic-outcome-access")
    _git(root, "config", "user.email", "outcome@example.invalid")
    _git(root, "config", "user.name", "Synthetic Outcome Access")
    _git(root, "config", "core.autocrlf", "false")
    _git(root, "add", ".gitignore", "config")
    _git(root, "commit", "-m", "synthetic outcome fixture")
    commit = _git(root, "rev-parse", "HEAD")
    guard = CheckoutLeaseGuard(
        project_root=root,
        policy_path=root / "config/architecture/arch_005_s4d_checkout_guard.yaml",
        parallel_policy_path=root / "config/architecture/arch_005_parallel_control_policy.yaml",
    )
    decision, handle = guard.acquire(
        intent_id="synthetic-outcome-access",
        task_id="TRADING-2564-SYNTHETIC",
        thread_id="synthetic-outcome-access",
        actor="integration-coordinator",
        operation_class=CheckoutOperationClass.SHARED_MUTATION,
        shared_paths=("outputs", "config/research"),
        base_commit=commit,
    )
    assert decision.status == "PASS" and handle is not None
    try:
        yield _Fixture(root, commit, handle)
    finally:
        if not handle.released:
            handle.release(outcome="synthetic_test_complete")


def _view(
    service: gateway.ResearchOutcomeAccessGateway, leased: _Fixture, **changes: Any
) -> gateway.ReleasedSyntheticOutcome:
    arguments = {
        "attempt_id": "attempt-1",
        "access_id": "access-1",
        "loader": lambda: RESULT,
        "lease": leased.lease,
    }
    arguments.update(changes)
    return service.view(**arguments)


def test_pending_is_durable_before_loader_and_metadata_never_contains_result(
    leased: _Fixture,
) -> None:
    service, _, _ = leased.ready()
    calls = []

    def load() -> bytes:
        pending = service.replay_metadata()
        assert pending["pending_access_count"] == 1
        assert pending["accesses"][0]["visibility"] == "POSSIBLY_EXPOSED"
        calls.append("called")
        return RESULT

    result = _view(service, leased, loader=load)
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
            RESULT not in path.read_bytes() for path in (leased.root / relative).glob("*.json")
        )
    assert gateway.CHECKPOINT_DIRECTORY not in str(leased.lease.guard.store.events_root)
    assert leased.lease.guard.replay().status == "PASS"


def test_read_only_replay_after_lease_release_never_mutates_or_reloads(leased: _Fixture) -> None:
    service, _, _ = leased.ready()
    _view(service, leased)
    leased.lease.release(outcome="synthetic_metadata_replay")
    before = {path: path.read_bytes() for path in leased.lease.guard.store.root.rglob("*.json")}
    assert service.replay_metadata()["access_count"] == 1
    assert before == {
        path: path.read_bytes() for path in leased.lease.guard.store.root.rglob("*.json")
    }


def test_explicit_bootstrap_required_and_cannot_be_repeated(leased: _Fixture) -> None:
    service, envelope, authority = leased.configure()
    with pytest.raises(gateway.OutcomeAccessError, match="BOOTSTRAP_REQUIRED"):
        leased.freeze(service, envelope, authority)
    assert not (leased.root / service.required_paths()[0]).exists()
    assert service.bootstrap(lease=leased.lease) == authority.genesis_sha256
    with pytest.raises(gateway.OutcomeAccessError, match="ALREADY_EXISTS"):
        service.bootstrap(lease=leased.lease)


def test_wrong_genesis_never_bootstraps(leased: _Fixture) -> None:
    service, _, authority = leased.configure()
    leased.install(replace(authority, genesis_sha256="0" * 64))
    with pytest.raises(gateway.OutcomeAccessError, match="GENESIS_HASH_MISMATCH"):
        service.bootstrap(lease=leased.lease)
    assert not (leased.root / authority.ledger_relative_path).exists()


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
def test_history_damage_fails_closed_before_loader(leased: _Fixture, damage: str) -> None:
    service, _, authority = leased.ready()
    ledger = leased.root / authority.ledger_relative_path
    checkpoints = leased.root / service.checkpoint_path
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
        ] = ("0" * 64)
        target.write_bytes(canonical_json_bytes(row))
    calls = []
    with pytest.raises((ValueError, OSError, DataPublicationIntegrityError)):
        _view(service, leased, loader=lambda: calls.append(1))
    assert calls == []
    with pytest.raises((ValueError, OSError, DataPublicationIntegrityError)):
        service.bootstrap(lease=leased.lease)


@pytest.mark.parametrize("phase", ["PREPARED", "EVENT", "COMPLETED"])
def test_pending_write_failure_does_not_call_loader_or_repair_history(
    leased: _Fixture, monkeypatch: pytest.MonkeyPatch, phase: str
) -> None:
    service, _, _ = leased.ready()
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
        _view(service, leased, loader=lambda: calls.append(1))
    assert calls == []
    monkeypatch.setattr(service, "_publish", publish)
    if phase != "PREPARED":
        with pytest.raises(gateway.OutcomeAccessError, match="CHECKPOINT_INCOMPLETE"):
            _view(service, leased, loader=lambda: calls.append(1))
    else:
        assert service.replay_metadata()["access_count"] == 0
    assert calls == []


def test_terminal_write_failure_preserves_possible_exposure(
    leased: _Fixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    service, _, _ = leased.ready()
    publish = service._publish

    def damaged(relative: str, row: dict[str, Any]) -> None:
        if row.get("kind") == "VIEW_COMPLETED":
            raise OSError("synthetic terminal write failure")
        publish(relative, row)

    monkeypatch.setattr(service, "_publish", damaged)
    calls = []
    with pytest.raises(OSError):
        _view(service, leased, loader=lambda: (calls.append(1), RESULT)[1])
    assert calls == [1]
    monkeypatch.setattr(service, "_publish", publish)
    with pytest.raises(gateway.OutcomeAccessError, match="CHECKPOINT_INCOMPLETE"):
        _view(service, leased, access_id="access-2", loader=lambda: calls.append(2))
    assert calls == [1]


@pytest.mark.parametrize("failure", ["exception", "wrong_type"])
def test_failed_loader_never_restores_unseen_or_leaks_exception_text(
    leased: _Fixture, failure: str, capsys: pytest.CaptureFixture[str]
) -> None:
    service, envelope, _ = leased.ready(look_mode="REPEAT_DECLARED")

    def load() -> Any:
        print("synthetic partial stdout")
        if failure == "exception":
            raise ValueError(RESULT.decode())
        return RESULT.decode()

    with pytest.raises(gateway.OutcomeAccessError) as caught:
        _view(service, leased, loader=load)
    assert RESULT.decode() not in str(caught.value)
    assert caught.value.__context__ is None
    assert "partial stdout" in capsys.readouterr().out
    metadata = service.replay_metadata()
    assert metadata["failed_access_count"] == metadata["possibly_exposed_count"] == 1
    assert RESULT.decode() not in str(metadata)
    with pytest.raises(gateway.OutcomeAccessError, match="ALREADY_POSSIBLY_EXPOSED"):
        _view(service, leased, access_id="access-2")
    with pytest.raises(gateway.OutcomeAccessError, match="ALREADY_POSSIBLY_EXPOSED"):
        service.begin_attempt(
            envelope_sha256=envelope.canonical_sha256(), attempt_id="attempt-2", lease=leased.lease
        )


def test_repeat_requires_declared_policy_new_access_and_same_result_binding(
    leased: _Fixture,
) -> None:
    service, _, _ = leased.ready(look_mode="REPEAT_DECLARED")
    _view(service, leased)
    with pytest.raises(gateway.OutcomeAccessError, match="ACCESS_DUPLICATE"):
        _view(service, leased)
    _view(service, leased, access_id="access-2")
    assert service.replay_metadata()["access_count"] == 2
    with pytest.raises(gateway.OutcomeAccessError, match="REPEATED_RESULT_MISMATCH"):
        _view(service, leased, access_id="access-3", loader=lambda: b"changed outcome")
    metadata = service.replay_metadata()
    assert metadata["access_count"] == 3 and metadata["failed_access_count"] == 1
    with pytest.raises(gateway.OutcomeAccessError, match="ALREADY_POSSIBLY_EXPOSED"):
        _view(service, leased, access_id="access-4")


def test_first_only_rejects_second_access_without_loader(leased: _Fixture) -> None:
    service, _, _ = leased.ready()
    _view(service, leased)
    calls = []
    with pytest.raises(gateway.OutcomeAccessError, match="ALREADY_POSSIBLY_EXPOSED"):
        _view(service, leased, access_id="access-2", loader=lambda: calls.append(1))
    assert calls == [] and service.replay_metadata()["access_count"] == 1


@pytest.mark.parametrize(
    "history", ["UNKNOWN", "KNOWN", "PARTIAL", "POSSIBLY_EXPOSED", "UNKNOWN_EXPOSURE"]
)
def test_prior_inventory_blocks_first_access(leased: _Fixture, history: str) -> None:
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
    service, envelope, authority = leased.configure((envelope,), **changes)
    service.bootstrap(lease=leased.lease)
    leased.freeze(service, envelope, authority)
    with pytest.raises(gateway.OutcomeAccessError, match="PRIOR_OR_UNKNOWN_HISTORY"):
        service.begin_attempt(
            envelope_sha256=envelope.canonical_sha256(), attempt_id="attempt-1", lease=leased.lease
        )


@pytest.mark.parametrize("role", ["DISCOVERY_KNOWN", "TRAIN", "SEEN_VALIDATION"])
def test_known_data_role_cannot_be_called_unseen(leased: _Fixture, role: str) -> None:
    service, envelope, authority = leased.configure((replace(make_envelope(), data_role=role),))
    service.bootstrap(lease=leased.lease)
    leased.freeze(service, envelope, authority)
    with pytest.raises(gateway.OutcomeAccessError, match="ROLE_NOT_UNTOUCHED"):
        service.begin_attempt(
            envelope_sha256=envelope.canonical_sha256(), attempt_id="attempt-1", lease=leased.lease
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
def test_renaming_and_revising_do_not_reset_domain_history(leased: _Fixture, revision: str) -> None:
    first = make_envelope()
    revised = replace(
        first,
        envelope_id="revised-envelope",
        **{revision: "9" * 64 if revision.endswith("sha256") else "renamed"},
    )
    service, _, authority = leased.configure((first, revised))
    service.bootstrap(lease=leased.lease)
    for envelope in (first, revised):
        leased.freeze(service, envelope, authority)
    service.begin_attempt(
        envelope_sha256=first.canonical_sha256(), attempt_id="attempt-1", lease=leased.lease
    )
    _view(service, leased)
    with pytest.raises(gateway.OutcomeAccessError, match="ALREADY_POSSIBLY_EXPOSED"):
        service.begin_attempt(
            envelope_sha256=revised.canonical_sha256(), attempt_id="attempt-2", lease=leased.lease
        )
    assert service.replay_metadata()["trial_count"] == 2


@pytest.mark.parametrize("same_identity", ["domain", "information_set"])
@pytest.mark.parametrize("exposed", [False, True])
def test_alias_ledger_inherits_global_checkpoint_exposure(
    leased: _Fixture, same_identity: str, exposed: bool
) -> None:
    first_service, first, old_authority = leased.ready()
    if exposed:
        _view(first_service, leased)
    alias = replace(
        first,
        envelope_id="alias-envelope",
        domain_id="alias-domain",
        family_id="alias-family",
        input_information_set_sha256=(
            first.input_information_set_sha256 if same_identity == "information_set" else "8" * 64
        ),
    )
    service, alias, authority = leased.configure(
        (alias,),
        domain_definition_sha256=(
            old_authority.domain_definition_sha256 if same_identity == "domain" else "9" * 64
        ),
    )
    leased.freeze(service, alias, authority)
    if exposed:
        with pytest.raises(gateway.OutcomeAccessError, match="ALREADY_POSSIBLY_EXPOSED"):
            service.begin_attempt(
                envelope_sha256=alias.canonical_sha256(),
                attempt_id="attempt-alias",
                lease=leased.lease,
            )
    else:
        service.begin_attempt(
            envelope_sha256=alias.canonical_sha256(), attempt_id="attempt-alias", lease=leased.lease
        )
        assert service.replay_metadata()["attempt_count"] == 2


def test_nonoverlapping_interval_can_start_new_trial(leased: _Fixture) -> None:
    first = make_envelope()
    second = replace(
        first,
        envelope_id="next-interval",
        requested_interval=OutcomeInterval(date(2026, 2, 1), date(2026, 2, 28)),
        evaluated_interval=OutcomeInterval(date(2026, 2, 2), date(2026, 2, 27)),
    )
    service, _, authority = leased.configure((first, second))
    service.bootstrap(lease=leased.lease)
    for envelope in (first, second):
        leased.freeze(service, envelope, authority)
    service.begin_attempt(
        envelope_sha256=first.canonical_sha256(), attempt_id="attempt-1", lease=leased.lease
    )
    _view(service, leased)
    service.begin_attempt(
        envelope_sha256=second.canonical_sha256(), attempt_id="attempt-2", lease=leased.lease
    )
    _view(service, leased, attempt_id="attempt-2", access_id="access-2")
    assert service.replay_metadata()["access_count"] == 2


@pytest.mark.parametrize("scope", ["REAL_RESEARCH", "PRODUCTION", None, True])
def test_real_scope_rejected_before_loader(leased: _Fixture, scope: Any) -> None:
    service, _, _ = leased.ready()
    calls = []
    with pytest.raises(gateway.OutcomeAccessError, match="REAL_SCOPE_NOT_ADMITTED"):
        _view(service, leased, scope=scope, loader=lambda: calls.append(1))
    assert calls == [] and service.replay_metadata()["access_count"] == 0


def test_changed_checkout_commit_and_released_lease_rejected(leased: _Fixture) -> None:
    service, _, _ = leased.ready()
    calls = []
    _git(leased.root, "commit", "--allow-empty", "-m", "changed candidate")
    with pytest.raises(ValueError):
        _view(service, leased, loader=lambda: calls.append(1))
    leased.lease.release(outcome="synthetic_lease_changed")
    with pytest.raises(ValueError):
        _view(service, leased, loader=lambda: calls.append(1))
    assert calls == []


@pytest.mark.parametrize("field", ["canonical_execution_root", "canonical_git_common_dir"])
def test_authority_cannot_redirect_canonical_root(leased: _Fixture, field: str) -> None:
    service, _, authority = leased.ready()
    leased.install(replace(authority, **{field: (leased.root.parent / "another-root").as_posix()}))
    with pytest.raises(gateway.OutcomeAccessError, match="CANONICAL_ROOT_MISMATCH"):
        _view(service, leased)


@pytest.mark.parametrize(
    "change",
    ["unapproved_envelope", "unapproved_policy", "review_reference", "time_hash", "time_order"],
)
def test_freeze_requires_exact_review_and_time_binding(leased: _Fixture, change: str) -> None:
    service, envelope, authority = leased.configure()
    if change == "unapproved_policy":
        authority = replace(authority, approved_policy_sha256s=("0" * 64,))
        leased.install(authority)
    service.bootstrap(lease=leased.lease)
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
            envelope=envelope, admission=admission, time_evidence_bytes=evidence, lease=leased.lease
        )
    assert service.replay_metadata()["trial_count"] == 0


def test_freeze_idempotency_and_new_authority_cannot_change_frozen_policy(leased: _Fixture) -> None:
    service, envelope, authority = leased.ready()
    first = leased.freeze(service, envelope, authority)
    assert leased.freeze(service, envelope, authority) == first
    assert service.replay_metadata()["trial_count"] == 1
    leased.install(replace(authority, version="v2"))
    with pytest.raises(gateway.OutcomeAccessError, match="FROZEN_AUTHORITY_CHANGED"):
        _view(service, leased)


def _child_view(
    root_text: str,
    commit: str,
    lease_id: str,
    access_id: str,
    connection: Any,
) -> None:
    """Spawned fresh interpreter; real existing-lease restore, no inherited lock."""
    try:
        service = gateway.ResearchOutcomeAccessGateway(
            execution_root=Path(root_text), candidate_commit=commit
        )
        lease = restore_named_capture_lease(
            execution_root=Path(root_text),
            source_lease_id=lease_id,
            candidate_commit=commit,
            required_paths=service.required_paths(),
        )

        def load() -> bytes:
            connection.send("STARTED")
            if connection.recv() != "FINISH":
                raise RuntimeError("synthetic process handshake mismatch")
            return RESULT

        service.view(attempt_id="attempt-1", access_id=access_id, loader=load, lease=lease)
        connection.send("COMPLETED")
    except BaseException as exc:
        connection.send(getattr(exc, "code", type(exc).__name__))
    finally:
        connection.close()


@pytest.mark.parametrize("kill_first", [False, True])
def test_multiprocess_same_active_lease_serializes_and_crash_keeps_pending(
    leased: _Fixture, kill_first: bool
) -> None:
    service, _, _ = leased.ready()
    context = multiprocessing.get_context("spawn")
    parent_connection, child_connection = context.Pipe()
    child = context.Process(
        target=_child_view,
        args=(
            str(leased.root),
            leased.commit,
            leased.lease.lease_id,
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
            _view(service, leased, access_id="access-2", loader=lambda: calls.append(1))
        assert "ALREADY_POSSIBLY_EXPOSED" in str(caught.value) and calls == []
        if kill_first:
            child.terminate()
            child.join(30)
            assert not child.is_alive()
            with pytest.raises(gateway.OutcomeAccessError, match="ALREADY_POSSIBLY_EXPOSED"):
                _view(service, leased, access_id="access-3", loader=lambda: calls.append(1))
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
    leased: _Fixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    service, _, _ = leased.ready()
    import ai_trading_system.data.named_quality_dispatch as dispatch

    before = datetime.now(UTC) - timedelta(hours=1)
    monkeypatch.setattr(dispatch, "_now", lambda: before)
    calls = []
    with pytest.raises(ValueError):
        _view(service, leased, loader=lambda: calls.append(1))
    assert calls == []


@pytest.mark.parametrize("control", [SystemExit, KeyboardInterrupt, GeneratorExit])
def test_process_control_propagates_unchanged_with_durable_pending(
    leased: _Fixture, control: type[BaseException]
) -> None:
    service, _, _ = leased.ready()
    original = control("synthetic cancellation")

    def load() -> bytes:
        raise original

    with pytest.raises(control) as caught:
        _view(service, leased, loader=load)
    assert caught.value is original
    assert service.replay_metadata()["pending_access_count"] == 1
    with pytest.raises(gateway.OutcomeAccessError, match="ALREADY_POSSIBLY_EXPOSED"):
        _view(service, leased, access_id="access-2")


def test_callback_can_heartbeat_using_same_global_arbiter(leased: _Fixture) -> None:
    service, _, _ = leased.ready()

    def load() -> bytes:
        leased.lease.heartbeat()
        return RESULT

    assert _view(service, leased, loader=load).content == RESULT


def test_callback_can_complete_nonoverlapping_access_before_its_fresh_terminal_replay(
    leased: _Fixture,
) -> None:
    first = make_envelope()
    second = replace(
        first,
        envelope_id="interleaved-envelope",
        requested_interval=OutcomeInterval(date(2026, 2, 1), date(2026, 2, 28)),
        evaluated_interval=OutcomeInterval(date(2026, 2, 2), date(2026, 2, 27)),
    )
    service, _, authority = leased.configure((first, second))
    service.bootstrap(lease=leased.lease)
    for envelope in (first, second):
        leased.freeze(service, envelope, authority)
    service.begin_attempt(
        envelope_sha256=first.canonical_sha256(), attempt_id="attempt-1", lease=leased.lease
    )

    def load() -> bytes:
        service.begin_attempt(
            envelope_sha256=second.canonical_sha256(), attempt_id="attempt-2", lease=leased.lease
        )
        assert (
            _view(service, leased, attempt_id="attempt-2", access_id="access-2").content == RESULT
        )
        return RESULT

    assert _view(service, leased, loader=load).content == RESULT
    metadata = service.replay_metadata()
    assert metadata["completed_access_count"] == metadata["access_count"] == 2
    assert metadata["pending_access_count"] == 0


@pytest.mark.parametrize("origin", ["access", "inventory"])
def test_alias_then_input_revision_inherits_transitive_history(
    leased: _Fixture, origin: str
) -> None:
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
    service, first, authority = leased.configure((first,), **changes)
    service.bootstrap(lease=leased.lease)
    leased.freeze(service, first, authority)
    if origin == "access":
        service.begin_attempt(
            envelope_sha256=first.canonical_sha256(), attempt_id="attempt-1", lease=leased.lease
        )
        _view(service, leased)
    bridge = replace(first, envelope_id="bridge-envelope", domain_id="bridge-domain")
    service, bridge, authority = leased.configure((bridge,), domain_definition_sha256="9" * 64)
    leased.freeze(service, bridge, authority)
    reason = "ALREADY_POSSIBLY_EXPOSED" if origin == "access" else "PRIOR_OR_UNKNOWN_HISTORY"
    with pytest.raises(gateway.OutcomeAccessError, match=reason):
        service.begin_attempt(
            envelope_sha256=bridge.canonical_sha256(),
            attempt_id="bridge-attempt",
            lease=leased.lease,
        )
    revision = replace(
        bridge, envelope_id="revision-envelope", input_information_set_sha256="8" * 64
    )
    service, revision, authority = leased.configure((revision,), domain_definition_sha256="9" * 64)
    leased.freeze(service, revision, authority)
    with pytest.raises(gateway.OutcomeAccessError, match=reason):
        service.begin_attempt(
            envelope_sha256=revision.canonical_sha256(),
            attempt_id="revision-attempt",
            lease=leased.lease,
        )
    assert service.replay_metadata()["trial_count"] == 3


def test_later_reviewed_identity_bridge_blocks_preexisting_attempt_before_loader(
    leased: _Fixture,
) -> None:
    service, first, _ = leased.ready()
    _view(service, leased)
    revision = replace(
        first,
        envelope_id="revision-before-bridge",
        domain_id="revised-domain",
        input_information_set_sha256="8" * 64,
    )
    service, revision, revision_authority = leased.configure(
        (revision,), domain_definition_sha256="9" * 64
    )
    leased.freeze(service, revision, revision_authority)
    service.begin_attempt(
        envelope_sha256=revision.canonical_sha256(),
        attempt_id="revision-attempt",
        lease=leased.lease,
    )
    bridge = replace(first, envelope_id="later-bridge", domain_id="revised-domain")
    service, bridge, authority = leased.configure((bridge,), domain_definition_sha256="9" * 64)
    leased.freeze(service, bridge, authority)
    leased.install(revision_authority)
    calls = []
    with pytest.raises(gateway.OutcomeAccessError, match="ALREADY_POSSIBLY_EXPOSED"):
        _view(
            service,
            leased,
            attempt_id="revision-attempt",
            access_id="revision-access",
            loader=lambda: calls.append(1),
        )
    assert calls == [] and service.replay_metadata()["access_count"] == 1


def test_root_case_equivalence_is_consistent_before_and_after_bootstrap(leased: _Fixture) -> None:
    service, envelope, authority = leased.configure()
    changed = replace(
        authority,
        canonical_execution_root=authority.canonical_execution_root.upper(),
        canonical_git_common_dir=authority.canonical_git_common_dir.upper(),
    )
    changed = replace(changed, genesis_sha256=_hash(gateway.genesis_bytes(changed)))
    leased.install(changed)
    if os.name != "nt":
        with pytest.raises(gateway.OutcomeAccessError, match="CANONICAL_ROOT_MISMATCH"):
            service.bootstrap(lease=leased.lease)
        assert not (leased.root / gateway.CANONICAL_LEDGER_PATH).exists()
    else:
        service.bootstrap(lease=leased.lease)
        leased.freeze(service, envelope, changed)
        assert service.replay_metadata()["trial_count"] == 1


def test_retained_domain_inventory_inherits_outside_old_envelope_interval(leased: _Fixture) -> None:
    old = replace(
        make_envelope(),
        requested_interval=OutcomeInterval(date(2026, 2, 1), date(2026, 2, 28)),
        evaluated_interval=OutcomeInterval(date(2026, 2, 2), date(2026, 2, 27)),
    )
    service, old, authority = leased.configure(
        (old,),
        prior_exposures=(
            OutcomeExposure(make_envelope().requested_interval, "KNOWN", "old-domain-inventory"),
        ),
    )
    service.bootstrap(lease=leased.lease)
    leased.freeze(service, old, authority)
    alias = replace(make_envelope(), domain_id="new-domain-alias")
    service, alias, new_authority = leased.configure((alias,))
    leased.freeze(service, alias, new_authority)
    with pytest.raises(gateway.OutcomeAccessError, match="PRIOR_OR_UNKNOWN_HISTORY"):
        service.begin_attempt(
            envelope_sha256=alias.canonical_sha256(), attempt_id="alias-attempt", lease=leased.lease
        )


def test_independent_domain_and_information_set_are_distinct_reviewed_scope(
    leased: _Fixture,
) -> None:
    service, old, _ = leased.ready()
    _view(service, leased)
    alias = replace(
        old,
        envelope_id="independent-envelope",
        domain_id="independent-domain",
        input_information_set_sha256="8" * 64,
    )
    service, alias, authority = leased.configure((alias,), domain_definition_sha256="9" * 64)
    leased.freeze(service, alias, authority)
    service.begin_attempt(
        envelope_sha256=alias.canonical_sha256(),
        attempt_id="independent-attempt",
        lease=leased.lease,
    )
    _view(service, leased, attempt_id="independent-attempt", access_id="independent-access")
    assert service.replay_metadata()["trial_count"] == 2


def test_output_path_cannot_select_a_fresh_ledger(leased: _Fixture) -> None:
    _, _, authority = leased.ready()
    leased.install(replace(authority, ledger_relative_path="outputs/new-empty-history"))
    with pytest.raises(gateway.OutcomeAccessError, match="CANONICAL_LEDGER_MISMATCH"):
        gateway.ResearchOutcomeAccessGateway(
            execution_root=leased.root, candidate_commit=leased.commit
        )


@pytest.mark.parametrize("drift", ["actor", "cached_runtime", "expired"])
def test_active_lease_identity_and_cached_authority_are_revalidated(
    leased: _Fixture, drift: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    service, _, _ = leased.ready()
    import ai_trading_system.data.named_quality_dispatch as dispatch

    calls = []
    with monkeypatch.context() as patch:
        if drift == "actor":
            patch.setattr(leased.lease, "actor", "another-actor")
        elif drift == "cached_runtime":
            patch.setattr(service.guard, "runtime_root", leased.root / "outputs/another-store")
        else:
            patch.setattr(dispatch, "_now", lambda: datetime.now(UTC) + timedelta(days=30))
        with pytest.raises(ValueError):
            _view(service, leased, loader=lambda: calls.append(1))
    assert calls == [] and service.replay_metadata()["access_count"] == 0


@pytest.mark.parametrize("missing", ["ledger", "checkpoint"])
def test_real_active_lease_must_claim_both_durable_resources(
    leased: _Fixture, missing: str
) -> None:
    service, _, _ = leased.ready()
    leased.lease.release(outcome="synthetic_narrow_scope_fixture")
    ledger, checkpoint = service.required_paths()
    decision, narrow = leased.lease.guard.acquire(
        intent_id="synthetic-narrow-scope",
        task_id="TRADING-2564-SYNTHETIC",
        thread_id="synthetic-narrow-scope",
        actor="integration-coordinator",
        operation_class=CheckoutOperationClass.SHARED_MUTATION,
        shared_paths=("config/research", checkpoint if missing == "ledger" else ledger),
        base_commit=leased.commit,
    )
    assert decision.status == "PASS" and narrow is not None
    calls = []
    try:
        with pytest.raises(ValueError, match="LEASE_SCOPE_INVALID"):
            _view(service, leased, lease=narrow, loader=lambda: calls.append(1))
        assert calls == []
    finally:
        narrow.release(outcome="synthetic_scope_checked")


def _rewrite_test_history(
    service: gateway.ResearchOutcomeAccessGateway, leased: _Fixture, events: list[Any]
) -> None:
    """Deliberate synthetic corruption, preserving hashes to test semantic replay."""
    ledger = leased.root / gateway.CANONICAL_LEDGER_PATH
    for index, event in enumerate(events):
        event["previous_event_sha256"] = (
            _hash(canonical_json_bytes(events[index - 1])) if index else None
        )
        (ledger / f"{index:012d}.json").write_bytes(canonical_json_bytes(event))
    folder = leased.root / service.checkpoint_path
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


def test_replay_rejects_backdated_event_even_with_consistent_hashes(leased: _Fixture) -> None:
    service, _, _ = leased.ready()
    ledger = leased.root / gateway.CANONICAL_LEDGER_PATH
    events = [strict_json_loads(path.read_bytes()) for path in sorted(ledger.glob("*.json"))]
    events[-1]["payload"]["occurred_at"] = "2026-01-01T00:00:00+00:00"
    _rewrite_test_history(service, leased, events)
    with pytest.raises(gateway.OutcomeAccessError, match="EVENT_TIME_INVALID"):
        service.replay_metadata()


def test_retained_authority_must_reconstruct_exact_genesis_bytes(leased: _Fixture) -> None:
    service, _, _ = leased.ready()
    ledger = leased.root / gateway.CANONICAL_LEDGER_PATH
    events = [strict_json_loads(path.read_bytes()) for path in sorted(ledger.glob("*.json"))]
    payload = events[1]["payload"]
    authority = replace(
        LocalExperimentAuthority.from_dict(payload["authority"]),
        canonical_execution_root=(leased.root.parent / "different-root").as_posix(),
    )
    payload["authority"] = authority.to_dict()
    payload["admission"]["authority_sha256"] = authority.canonical_sha256()
    _rewrite_test_history(service, leased, events)
    with pytest.raises(gateway.OutcomeAccessError, match="FREEZE_GENESIS_MISMATCH"):
        service.replay_metadata()


@pytest.mark.skipif(
    os.name == "nt",
    reason="POSIX symlink fixture; Windows containment is exercised by immutable publisher suite",
)
def test_symlink_ledger_rejected_without_following(leased: _Fixture) -> None:
    service, _, authority = leased.configure()
    outside = leased.root.parent / "outside"
    outside.mkdir()
    ledger = leased.root / authority.ledger_relative_path
    ledger.parent.mkdir(parents=True, exist_ok=True)
    ledger.symlink_to(outside, target_is_directory=True)
    with pytest.raises(gateway.OutcomeAccessError, match="PATH_UNSAFE"):
        service.bootstrap(lease=leased.lease)
    assert list(outside.iterdir()) == []
