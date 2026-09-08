"""Bounded S4 synthetic outcome gateway; no real research consumer is admitted.

The existing S4D OS arbiter serializes replay/check/append. An independent
PREPARED/event/COMPLETED checkpoint chain detects a missing or truncated business
ledger. Pending is durable possible exposure, even after a killed process. This
is local controlled evidence, not an external clock, an OS sandbox or proof of
universal non-exposure. A trusted administrator replacing *all* authority and
history is outside this guarantee. See the TRADING-2564 S4 requirement.
"""

from __future__ import annotations

import hashlib
import os
import re
import stat
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, NoReturn, cast

from ai_trading_system.contracts.prospective_event_time_evidence import (
    canonical_json_bytes,
    parse_utc_datetime,
    strict_json_loads,
    utc_datetime_text,
)
from ai_trading_system.contracts.research_experiment_envelope import (
    LOCAL_TIME_EVIDENCE,
    SYNTHETIC_SCOPE,
    ExperimentEnvelope,
    FreezeAdmission,
    LocalExperimentAuthority,
)
from ai_trading_system.data.immutable_publish import (
    read_contained_artifact_bytes,
    write_contained_artifact_bytes,
)
from ai_trading_system.data.named_quality_dispatch import (
    recheck_named_capture_lease,
    verify_retained_named_capture_proof,
)
from ai_trading_system.platform.architecture.checkout_guard import (
    CheckoutLeaseGuard,
    CheckoutLeaseHandle,
    resolve_checkout_identity,
)
from ai_trading_system.platform.architecture.lease_arbiter import hold_lease_arbiter

# Reviewed protocol identities, not research thresholds or configurable output roots.
AUTHORITY_PATH = "config/research/research_experiment_authority.json"
CHECKPOINT_DIRECTORY = "research_outcome_access_checkpoints"
CANONICAL_LEDGER_PATH = "outputs/research/experiment_outcome_ledger_v1"
CALLBACK_ATTESTATION = "TRUSTED_SYNTHETIC_CALLBACK_NOT_ATTESTED"
_EVENT_SCHEMA = "research_outcome_access_event.v1"
_CHECKPOINT_SCHEMA = "research_outcome_access_checkpoint.v1"
_TIME_SCHEMA = "research_local_freeze_time_evidence.v1"
_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.@-]*")
_HASH = re.compile(r"[0-9a-f]{64}")


class OutcomeAccessError(ValueError):
    """Only fixed diagnostic codes; never retain callback output or exception text."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


def _fail(code: str) -> NoReturn:
    raise OutcomeAccessError(code)


def _sha(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def _now() -> datetime:
    return datetime.now(UTC)


def _object(value: object, keys: set[str] | None = None) -> dict[str, Any]:
    if type(value) is not dict or any(type(key) is not str for key in value):
        _fail("OUTCOME_METADATA_INVALID")
    result = cast(dict[str, Any], value)
    if keys is not None and set(result) != keys:
        _fail("OUTCOME_METADATA_INVALID")
    return result


def _identifier(value: object) -> str:
    if type(value) is not str or _ID.fullmatch(value) is None:
        _fail("OUTCOME_IDENTIFIER_INVALID")
    return value


def _same_path(left: Path, right: Path) -> bool:
    return os.path.normcase(str(left)) == os.path.normcase(str(right))


def _event(kind: str, payload: dict[str, Any], previous: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "schema_version": _EVENT_SCHEMA,
        "sequence": len(previous),
        "previous_event_sha256": _sha(canonical_json_bytes(previous[-1])) if previous else None,
        "kind": kind,
        "payload": payload,
    }


def genesis_bytes(authority: LocalExperimentAuthority) -> bytes:
    """Deterministic reviewed bootstrap bytes, without a circular authority hash."""
    if type(authority) is not LocalExperimentAuthority:
        _fail("OUTCOME_AUTHORITY_INVALID")
    return canonical_json_bytes(
        _event(
            "GENESIS",
            {
                "canonical_execution_root": authority.canonical_execution_root,
                "canonical_git_common_dir": authority.canonical_git_common_dir,
                "ledger_relative_path": authority.ledger_relative_path,
                "scope": SYNTHETIC_SCOPE,
            },
            [],
        )
    )


def local_freeze_time_evidence(*, envelope: ExperimentEnvelope, recorded_at: datetime) -> bytes:
    """Build local timestamp metadata; this grants no review or external time quality."""
    return canonical_json_bytes(
        {
            "schema_version": _TIME_SCHEMA,
            "envelope_sha256": envelope.canonical_sha256(),
            "recorded_at": utc_datetime_text(recorded_at),
            "time_evidence_level": LOCAL_TIME_EVIDENCE,
            "scope": SYNTHETIC_SCOPE,
        }
    )


@dataclass(frozen=True)
class ReleasedSyntheticOutcome:
    """Transient result bytes; the gateway persists only their hash and size."""

    content: bytes
    receipt: dict[str, Any]


@dataclass
class _Replay:
    checkpoints: list[dict[str, Any]]
    ledgers: dict[str, list[dict[str, Any]]]
    freezes: dict[str, dict[str, Any]]
    attempts: dict[str, dict[str, Any]]
    accesses: dict[str, dict[str, Any]]


class ResearchOutcomeAccessGateway:
    def __init__(self, *, execution_root: Path, candidate_commit: str) -> None:
        if (
            not execution_root.is_absolute()
            or execution_root.resolve(strict=True) != execution_root
            or type(candidate_commit) is not str
            or re.fullmatch(r"[0-9a-f]{40}", candidate_commit) is None
        ):
            _fail("OUTCOME_EXECUTION_IDENTITY_INVALID")
        self.root = execution_root
        self.candidate_commit = candidate_commit
        self.guard = CheckoutLeaseGuard(
            project_root=execution_root,
            policy_path=execution_root / "config/architecture/arch_005_s4d_checkout_guard.yaml",
            parallel_policy_path=execution_root
            / "config/architecture/arch_005_parallel_control_policy.yaml",
        )
        self.checkpoint_path = (
            (self.guard.store.root / CHECKPOINT_DIRECTORY).relative_to(self.root).as_posix()
        )
        self._authority()

    def _authority(self) -> LocalExperimentAuthority:
        authority = LocalExperimentAuthority.from_json_bytes(self._read(AUTHORITY_PATH))
        identity = resolve_checkout_identity(self.root)
        if (
            not _same_path(Path(authority.canonical_execution_root), self.root)
            or not _same_path(
                Path(authority.canonical_git_common_dir), Path(identity.git_common_dir)
            )
            or authority.scope != SYNTHETIC_SCOPE
        ):
            _fail("OUTCOME_CANONICAL_ROOT_MISMATCH")
        if authority.ledger_relative_path != CANONICAL_LEDGER_PATH:
            _fail("OUTCOME_CANONICAL_LEDGER_MISMATCH")
        ledger = self.root / authority.ledger_relative_path
        protected = (self.root / AUTHORITY_PATH, self.guard.runtime_root)
        if any(
            ledger == path or ledger in path.parents or path in ledger.parents for path in protected
        ):
            _fail("OUTCOME_LEDGER_ROOT_INVALID")
        return authority

    def required_paths(self) -> tuple[str, ...]:
        return self._required(self._authority())

    def _required(self, authority: LocalExperimentAuthority) -> tuple[str, ...]:
        return authority.ledger_relative_path, self.checkpoint_path

    def _read(self, relative: str) -> bytes:
        return read_contained_artifact_bytes(root=self.root, relative_path=relative)

    def _publish(self, relative: str, row: dict[str, Any]) -> None:
        write_contained_artifact_bytes(
            root=self.root,
            relative_path=relative,
            content=canonical_json_bytes(row),
            immutable=True,
        )

    def _inventory(self, relative: str) -> tuple[str, ...]:
        """Reject links, partial files and unknown children without following them."""
        path = self.root
        for part in relative.split("/"):
            path = path / part
            try:
                metadata = path.lstat()
            except FileNotFoundError:
                return ()
            if (
                not stat.S_ISDIR(metadata.st_mode)
                or stat.S_ISLNK(metadata.st_mode)
                or getattr(metadata, "st_file_attributes", 0)
                & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0)
            ):
                _fail("OUTCOME_LEDGER_PATH_UNSAFE")
        names = tuple(sorted(item.name for item in path.iterdir()))
        if any(re.fullmatch(r"[0-9]{12}\.json", name) is None for name in names):
            _fail("OUTCOME_LEDGER_INVENTORY_INVALID")
        if names != tuple(f"{index:012d}.json" for index in range(len(names))):
            _fail("OUTCOME_LEDGER_SEQUENCE_INVALID")
        return names

    @contextmanager
    def _locked(
        self, lease: CheckoutLeaseHandle
    ) -> Iterator[tuple[LocalExperimentAuthority, dict[str, Any]]]:
        authority = self._authority()
        # Validate the canonical lease BEFORE writing even arbiter diagnostic metadata.
        self._proof(lease, authority)
        with hold_lease_arbiter(
            self.guard.store.root,
            actor=lease.actor,
            now=_now(),
            arbiter_ttl_seconds=self.guard.lease_policy.arbiter_ttl_seconds,
        ):
            if self._authority() != authority:
                _fail("OUTCOME_AUTHORITY_CHANGED")
            yield authority, self._proof(lease, authority)

    def _proof(
        self, lease: CheckoutLeaseHandle, authority: LocalExperimentAuthority
    ) -> dict[str, Any]:
        if type(lease) is not CheckoutLeaseHandle or lease.guard.project_root != self.root:
            _fail("OUTCOME_LEASE_ROOT_MISMATCH")
        if (
            self.guard.runtime_root != lease.guard.runtime_root
            or self.guard.policy != lease.guard.policy
            or self.guard.lease_policy != lease.guard.lease_policy
        ):
            _fail("OUTCOME_CACHED_LEASE_AUTHORITY_CHANGED")
        return recheck_named_capture_lease(
            lease, candidate_commit=self.candidate_commit, required_paths=self._required(authority)
        )

    def _replay(self) -> _Replay:
        checkpoints: list[dict[str, Any]] = []
        ledgers: dict[str, list[dict[str, Any]]] = {}
        initial = self._inventory(self.checkpoint_path)
        if len(initial) % 2:
            _fail("OUTCOME_CHECKPOINT_INCOMPLETE")
        for index, name in enumerate(initial):
            content = self._read(f"{self.checkpoint_path}/{name}")
            row = _object(strict_json_loads(content))
            if (
                canonical_json_bytes(row) != content
                or row.get("schema_version") != _CHECKPOINT_SCHEMA
                or type(row.get("sequence")) is not int
                or row["sequence"] != index
                or row.get("previous_checkpoint_sha256")
                != (_sha(canonical_json_bytes(checkpoints[-1])) if checkpoints else None)
            ):
                _fail("OUTCOME_CHECKPOINT_CHAIN_INVALID")
            if index % 2 == 0:
                _object(
                    row,
                    {
                        "schema_version",
                        "sequence",
                        "previous_checkpoint_sha256",
                        "phase",
                        "ledger_relative_path",
                        "event_sequence",
                        "event_sha256",
                        "lease_proof",
                    },
                )
                if row["phase"] != "PREPARED":
                    _fail("OUTCOME_CHECKPOINT_PHASE_INVALID")
                ledger = row["ledger_relative_path"]
                if ledger != CANONICAL_LEDGER_PATH:
                    _fail("OUTCOME_CANONICAL_LEDGER_MISMATCH")
                proof = _object(row["lease_proof"])
                verify_retained_named_capture_proof(
                    proof,
                    execution_root=self.root,
                    candidate_commit=proof["candidate_commit"],
                    required_paths=(ledger, self.checkpoint_path),
                    source_lease_id=proof["active_lease"]["lease_id"],
                    checked_at=parse_utc_datetime(proof["checked_at"]),
                )
                events = ledgers.setdefault(ledger, [])
                if type(row["event_sequence"]) is not int or row["event_sequence"] != len(events):
                    _fail("OUTCOME_EVENT_SEQUENCE_INVALID")
                event_content = self._read(f"{ledger}/{len(events):012d}.json")
                event = _object(
                    strict_json_loads(event_content),
                    {
                        "schema_version",
                        "sequence",
                        "previous_event_sha256",
                        "kind",
                        "payload",
                    },
                )
                if (
                    event_content != canonical_json_bytes(event)
                    or _sha(event_content) != row["event_sha256"]
                    or event.get("schema_version") != _EVENT_SCHEMA
                    or type(event["sequence"]) is not int
                    or event["sequence"] != len(events)
                    or event["previous_event_sha256"]
                    != (_sha(canonical_json_bytes(events[-1])) if events else None)
                ):
                    _fail("OUTCOME_EVENT_CHAIN_INVALID")
                events.append(event)
            else:
                _object(
                    row,
                    {
                        "schema_version",
                        "sequence",
                        "previous_checkpoint_sha256",
                        "phase",
                        "prepared_sha256",
                    },
                )
                if row["phase"] != "COMPLETED" or row["prepared_sha256"] != _sha(
                    canonical_json_bytes(checkpoints[-1])
                ):
                    _fail("OUTCOME_CHECKPOINT_PHASE_INVALID")
            checkpoints.append(row)
        if len(self._inventory(CANONICAL_LEDGER_PATH)) != len(
            ledgers.get(CANONICAL_LEDGER_PATH, [])
        ):
            _fail("OUTCOME_LEDGER_CHECKPOINT_MISMATCH")
        if initial != self._inventory(self.checkpoint_path):
            _fail("OUTCOME_CONCURRENT_REPLAY")
        replay = _Replay(checkpoints, ledgers, {}, {}, {})
        self._derive(replay)
        return replay

    def _derive(self, replay: _Replay) -> None:
        """Derive global exposure across aliases; no caller-supplied counters are used."""
        last_checked: datetime | None = None
        last_occurred: datetime | None = None
        for checkpoint in replay.checkpoints[::2]:
            ledger = checkpoint["ledger_relative_path"]
            event = replay.ledgers[ledger][checkpoint["event_sequence"]]
            kind, payload = event["kind"], _object(event["payload"])
            checked = parse_utc_datetime(checkpoint["lease_proof"]["checked_at"])
            if last_checked is not None and checked < last_checked:
                _fail("OUTCOME_EVENT_TIME_INVALID")
            last_checked = checked
            if kind == "GENESIS":
                _object(
                    payload,
                    {
                        "canonical_execution_root",
                        "canonical_git_common_dir",
                        "ledger_relative_path",
                        "scope",
                    },
                )
                if (
                    event["sequence"] != 0
                    or payload["ledger_relative_path"] != ledger
                    or not _same_path(Path(payload["canonical_execution_root"]), self.root)
                    or not _same_path(
                        Path(payload["canonical_git_common_dir"]),
                        Path(
                            checkpoint["lease_proof"]["lease_intent"]["workspace_identity"][
                                "git_common_dir"
                            ]
                        ),
                    )
                    or payload["scope"] != SYNTHETIC_SCOPE
                ):
                    _fail("OUTCOME_GENESIS_INVALID")
                last_occurred = checked
                continue
            if not replay.ledgers[ledger] or replay.ledgers[ledger][0]["kind"] != "GENESIS":
                _fail("OUTCOME_GENESIS_MISSING")
            if kind == "FREEZE":
                _object(
                    payload, {"envelope", "authority", "admission", "time_evidence", "occurred_at"}
                )
                authority = LocalExperimentAuthority.from_dict(payload["authority"])
                envelope = ExperimentEnvelope.from_dict(payload["envelope"])
                admission = FreezeAdmission.from_dict(payload["admission"])
                admission.validate_bindings(envelope, authority)
                self._validate_time(
                    envelope,
                    admission,
                    canonical_json_bytes(payload["time_evidence"]),
                    parse_utc_datetime(payload["occurred_at"]),
                )
                if (
                    authority.ledger_relative_path != ledger
                    or canonical_json_bytes(replay.ledgers[ledger][0]) != genesis_bytes(authority)
                    or _sha(canonical_json_bytes(replay.ledgers[ledger][0]))
                    != authority.genesis_sha256
                ):
                    _fail("OUTCOME_FREEZE_GENESIS_MISMATCH")
                key = envelope.canonical_sha256()
                if key in replay.freezes:
                    _fail("OUTCOME_FREEZE_DUPLICATE")
                replay.freezes[key] = {
                    **payload,
                    "ledger": ledger,
                    "freeze_sha256": _sha(canonical_json_bytes(event)),
                }
            elif kind == "ATTEMPT":
                _object(payload, {"attempt_id", "envelope_sha256", "occurred_at"})
                identifier = _identifier(payload["attempt_id"])
                frozen = replay.freezes.get(payload["envelope_sha256"])
                if identifier in replay.attempts or frozen is None or frozen["ledger"] != ledger:
                    _fail("OUTCOME_ATTEMPT_INVALID")
                replay.attempts[identifier] = payload
            elif kind == "VIEW_PENDING":
                _object(
                    payload,
                    {
                        "attempt_id",
                        "access_id",
                        "occurred_at",
                        "visibility",
                        "callback_attestation",
                    },
                )
                identifier = _identifier(payload["access_id"])
                attempt = replay.attempts.get(payload["attempt_id"])
                if (
                    identifier in replay.accesses
                    or attempt is None
                    or replay.freezes[attempt["envelope_sha256"]]["ledger"] != ledger
                    or payload["visibility"] != "POSSIBLY_EXPOSED"
                    or payload["callback_attestation"] != CALLBACK_ATTESTATION
                ):
                    _fail("OUTCOME_ACCESS_INVALID")
                replay.accesses[identifier] = {**payload, "state": "PENDING"}
            elif kind in {"VIEW_COMPLETED", "VIEW_FAILED"}:
                expected = {"access_id", "occurred_at", "callback_attestation"}
                expected |= (
                    {"result_sha256", "result_size_bytes"}
                    if kind == "VIEW_COMPLETED"
                    else {"failure_code"}
                )
                _object(payload, expected)
                access = replay.accesses.get(payload["access_id"])
                if (
                    access is None
                    or access["state"] != "PENDING"
                    or payload["callback_attestation"] != CALLBACK_ATTESTATION
                ):
                    _fail("OUTCOME_TERMINAL_INVALID")
                frozen = replay.freezes[replay.attempts[access["attempt_id"]]["envelope_sha256"]]
                if frozen["ledger"] != ledger:
                    _fail("OUTCOME_TERMINAL_INVALID")
                if kind == "VIEW_COMPLETED" and (
                    type(payload["result_sha256"]) is not str
                    or _HASH.fullmatch(payload["result_sha256"]) is None
                    or type(payload["result_size_bytes"]) is not int
                    or payload["result_size_bytes"] < 0
                ):
                    _fail("OUTCOME_RESULT_BINDING_INVALID")
                if kind == "VIEW_FAILED" and payload["failure_code"] not in {
                    "LOADER_FAILED",
                    "LOADER_RESULT_TYPE_INVALID",
                    "REPEATED_RESULT_MISMATCH",
                }:
                    _fail("OUTCOME_FAILURE_CODE_INVALID")
                access.update(payload)
                access["state"] = "COMPLETED" if kind == "VIEW_COMPLETED" else "FAILED"
            else:
                _fail("OUTCOME_EVENT_KIND_INVALID")
            occurred = parse_utc_datetime(payload["occurred_at"])
            if occurred > checked or (last_occurred is not None and occurred < last_occurred):
                _fail("OUTCOME_EVENT_TIME_INVALID")
            last_occurred = occurred

    def _require_genesis(self, state: _Replay, authority: LocalExperimentAuthority) -> None:
        events = state.ledgers.get(authority.ledger_relative_path)
        if (
            not events
            or canonical_json_bytes(events[0]) != genesis_bytes(authority)
            or _sha(canonical_json_bytes(events[0])) != authority.genesis_sha256
        ):
            _fail("OUTCOME_BOOTSTRAP_REQUIRED")

    def _append(
        self,
        state: _Replay,
        authority: LocalExperimentAuthority,
        proof: dict[str, Any],
        kind: str,
        payload: dict[str, Any],
    ) -> _Replay:
        current = self._replay()
        if current.checkpoints != state.checkpoints:
            _fail("OUTCOME_HISTORY_CHANGED")
        if state.checkpoints and parse_utc_datetime(proof["checked_at"]) < parse_utc_datetime(
            state.checkpoints[-2]["lease_proof"]["checked_at"]
        ):
            _fail("OUTCOME_LOCAL_CLOCK_REGRESSION")
        ledger = authority.ledger_relative_path
        event = _event(kind, payload, state.ledgers.get(ledger, []))
        checkpoints = state.checkpoints
        prepared = {
            "schema_version": _CHECKPOINT_SCHEMA,
            "sequence": len(checkpoints),
            "previous_checkpoint_sha256": (
                _sha(canonical_json_bytes(checkpoints[-1])) if checkpoints else None
            ),
            "phase": "PREPARED",
            "ledger_relative_path": ledger,
            "event_sequence": event["sequence"],
            "event_sha256": _sha(canonical_json_bytes(event)),
            "lease_proof": proof,
        }
        self._publish(f"{self.checkpoint_path}/{len(checkpoints):012d}.json", prepared)
        self._publish(f"{ledger}/{event['sequence']:012d}.json", event)
        completed = {
            "schema_version": _CHECKPOINT_SCHEMA,
            "sequence": len(checkpoints) + 1,
            "previous_checkpoint_sha256": _sha(canonical_json_bytes(prepared)),
            "phase": "COMPLETED",
            "prepared_sha256": _sha(canonical_json_bytes(prepared)),
        }
        self._publish(f"{self.checkpoint_path}/{len(checkpoints) + 1:012d}.json", completed)
        return self._replay()

    def bootstrap(self, *, lease: CheckoutLeaseHandle) -> str:
        """Explicit one-time admission of the authority's exact pre-reviewed genesis."""
        with self._locked(lease) as (authority, proof):
            state = self._replay()
            if authority.ledger_relative_path in state.ledgers or self._inventory(
                authority.ledger_relative_path
            ):
                _fail("OUTCOME_BOOTSTRAP_ALREADY_EXISTS")
            content = genesis_bytes(authority)
            if _sha(content) != authority.genesis_sha256:
                _fail("OUTCOME_GENESIS_HASH_MISMATCH")
            self._append(
                state, authority, proof, "GENESIS", _object(strict_json_loads(content))["payload"]
            )
            return authority.genesis_sha256

    def _validate_time(
        self,
        envelope: ExperimentEnvelope,
        admission: FreezeAdmission,
        evidence: bytes,
        now: datetime,
    ) -> None:
        row = _object(
            strict_json_loads(evidence),
            {
                "schema_version",
                "envelope_sha256",
                "recorded_at",
                "time_evidence_level",
                "scope",
            },
        )
        if (
            canonical_json_bytes(row) != evidence
            or _sha(evidence) != admission.time_evidence_sha256
            or row["schema_version"] != _TIME_SCHEMA
            or row["scope"] != SYNTHETIC_SCOPE
            or row["time_evidence_level"] != LOCAL_TIME_EVIDENCE
            or row["envelope_sha256"] != envelope.canonical_sha256()
            or not admission.reviewed_at <= parse_utc_datetime(row["recorded_at"]) <= now
        ):
            _fail("OUTCOME_FREEZE_TIME_INVALID")

    def freeze(
        self,
        *,
        envelope: ExperimentEnvelope,
        admission: FreezeAdmission,
        time_evidence_bytes: bytes,
        lease: CheckoutLeaseHandle,
    ) -> str:
        with self._locked(lease) as (authority, proof):
            admission.validate_bindings(envelope, authority)
            observed = parse_utc_datetime(proof["checked_at"])
            self._validate_time(envelope, admission, time_evidence_bytes, observed)
            state = self._replay()
            self._require_genesis(state, authority)
            key = envelope.canonical_sha256()
            if key in state.freezes:
                if state.freezes[key]["admission"] != admission.to_dict():
                    _fail("OUTCOME_FREEZE_CONFLICT")
                return cast(str, state.freezes[key]["freeze_sha256"])
            state = self._append(
                state,
                authority,
                proof,
                "FREEZE",
                {
                    "envelope": envelope.to_dict(),
                    "authority": authority.to_dict(),
                    "admission": admission.to_dict(),
                    "time_evidence": strict_json_loads(time_evidence_bytes),
                    "occurred_at": utc_datetime_text(observed),
                },
            )
            return cast(str, state.freezes[key]["freeze_sha256"])

    def _frozen(
        self, state: _Replay, key: str, authority: LocalExperimentAuthority
    ) -> ExperimentEnvelope:
        self._require_genesis(state, authority)
        frozen = state.freezes.get(key)
        if frozen is None or frozen["ledger"] != authority.ledger_relative_path:
            _fail("OUTCOME_FREEZE_REQUIRED")
        envelope = ExperimentEnvelope.from_dict(frozen["envelope"])
        authority.validate_envelope(envelope)
        if frozen["authority"] != authority.to_dict():
            _fail("OUTCOME_FROZEN_AUTHORITY_CHANGED")
        return envelope

    @staticmethod
    def _information_nodes(
        envelope: ExperimentEnvelope, authority: LocalExperimentAuthority
    ) -> frozenset[tuple[str, str]]:
        # A domain digest and an input digest are different protocol namespaces.
        return frozenset(
            {
                ("domain", authority.domain_definition_sha256),
                ("input", envelope.input_information_set_sha256),
            }
        )

    def _frozen_nodes(self, frozen: dict[str, Any]) -> frozenset[tuple[str, str]]:
        return self._information_nodes(
            ExperimentEnvelope.from_dict(frozen["envelope"]),
            LocalExperimentAuthority.from_dict(frozen["authority"]),
        )

    def _information_component(
        self, state: _Replay, envelope: ExperimentEnvelope, authority: LocalExperimentAuthority
    ) -> set[tuple[str, str]]:
        """Reviewed FREEZE mappings remain transitive through aliases and revisions.

        Even a variant never viewed, or refused after freeze, contributes its
        reviewed identity edge. Hash equality alone cannot infer unseen semantic
        domains; disconnected mappings remain owner-reviewed trust inputs.
        """
        component = set(self._information_nodes(envelope, authority))
        edges = [self._frozen_nodes(frozen) for frozen in state.freezes.values()]
        while True:
            previous = len(component)
            for edge in edges:
                if component.intersection(edge):
                    component.update(edge)
            if len(component) == previous:
                return component

    def _related(
        self,
        left: dict[str, Any],
        envelope: ExperimentEnvelope,
        component: set[tuple[str, str]],
    ) -> bool:
        return bool(
            component.intersection(self._frozen_nodes(left))
        ) and ExperimentEnvelope.from_dict(left["envelope"]).requested_interval.overlaps(
            envelope.requested_interval
        )

    def _check_access(
        self,
        state: _Replay,
        envelope: ExperimentEnvelope,
        authority: LocalExperimentAuthority,
        *,
        repeat_attempt: str | None = None,
    ) -> dict[str, Any] | None:
        if envelope.data_role != "PROSPECTIVE_UNTOUCHED":
            _fail("OUTCOME_ROLE_NOT_UNTOUCHED")
        if authority.prior_history_blocks_first_access(envelope.requested_interval):
            _fail("OUTCOME_PRIOR_OR_UNKNOWN_HISTORY")
        component = self._information_component(state, envelope, authority)
        # Retained authorities carry inventories across renamed domains and later revisions.
        for frozen in state.freezes.values():
            if component.intersection(
                self._frozen_nodes(frozen)
            ) and LocalExperimentAuthority.from_dict(
                frozen["authority"]
            ).prior_history_blocks_first_access(
                envelope.requested_interval
            ):
                _fail("OUTCOME_PRIOR_OR_UNKNOWN_HISTORY")
        repeated: list[dict[str, Any]] = []
        for access in state.accesses.values():
            previous = state.freezes[state.attempts[access["attempt_id"]]["envelope_sha256"]]
            if not self._related(previous, envelope, component):
                continue
            if (
                repeat_attempt == access["attempt_id"]
                and envelope.look_mode == "REPEAT_DECLARED"
                and access["state"] == "COMPLETED"
            ):
                repeated.append(access)
            else:
                _fail("OUTCOME_ALREADY_POSSIBLY_EXPOSED")
        return repeated[0] if repeated else None

    def begin_attempt(
        self, *, envelope_sha256: str, attempt_id: str, lease: CheckoutLeaseHandle
    ) -> str:
        _identifier(attempt_id)
        with self._locked(lease) as (authority, proof):
            state = self._replay()
            envelope = self._frozen(state, envelope_sha256, authority)
            if attempt_id in state.attempts:
                _fail("OUTCOME_ATTEMPT_DUPLICATE")
            self._check_access(state, envelope, authority)
            self._append(
                state,
                authority,
                proof,
                "ATTEMPT",
                {
                    "attempt_id": attempt_id,
                    "envelope_sha256": envelope_sha256,
                    "occurred_at": proof["checked_at"],
                },
            )
            return attempt_id

    def view(
        self,
        *,
        attempt_id: str,
        access_id: str,
        loader: Callable[[], bytes],
        lease: CheckoutLeaseHandle,
        scope: str = SYNTHETIC_SCOPE,
    ) -> ReleasedSyntheticOutcome:
        """Reserve before callback; every repeat needs a new immutable access record.

        Callbacks are trusted synthetic dependencies. Their declared envelope source
        hashes are not executable attestation and this API cannot sandbox Python.
        """
        if type(scope) is not str or scope != SYNTHETIC_SCOPE:
            _fail("OUTCOME_REAL_SCOPE_NOT_ADMITTED")
        _identifier(attempt_id)
        _identifier(access_id)
        if not callable(loader):
            _fail("OUTCOME_LOADER_INVALID")
        failed: str | None = None
        content: bytes | None = None
        with self._locked(lease) as (authority, proof):
            state = self._replay()
            attempt = state.attempts.get(attempt_id)
            if attempt is None:
                _fail("OUTCOME_ATTEMPT_REQUIRED")
            envelope = self._frozen(state, attempt["envelope_sha256"], authority)
            if access_id in state.accesses:
                _fail("OUTCOME_ACCESS_DUPLICATE")
            previous = self._check_access(state, envelope, authority, repeat_attempt=attempt_id)
            state = self._append(
                state,
                authority,
                proof,
                "VIEW_PENDING",
                {
                    "attempt_id": attempt_id,
                    "access_id": access_id,
                    "occurred_at": proof["checked_at"],
                    "visibility": "POSSIBLY_EXPOSED",
                    "callback_attestation": CALLBACK_ATTESTATION,
                },
            )
            self._proof(lease, authority)
            pending = dict(state.accesses[access_id])
        # A slow or interrupted callback never owns the global S4D arbiter. Process
        # controls (SystemExit/KeyboardInterrupt/GeneratorExit) propagate unchanged,
        # leaving durable pending. Ordinary failure messages are never persisted.
        try:
            loaded = loader()
            if type(loaded) is not bytes:
                failed = "LOADER_RESULT_TYPE_INVALID"
            else:
                content = loaded
        except Exception:
            failed = "LOADER_FAILED"
        with self._locked(lease) as (terminal_authority, proof):
            if terminal_authority != authority:
                _fail("OUTCOME_AUTHORITY_CHANGED")
            state = self._replay()
            self._frozen(state, attempt["envelope_sha256"], authority)
            if (
                state.accesses.get(access_id) != pending
                or state.attempts.get(attempt_id) != attempt
            ):
                _fail("OUTCOME_PENDING_CHANGED")
            if (
                content is not None
                and previous is not None
                and (
                    _sha(content) != previous["result_sha256"]
                    or len(content) != previous["result_size_bytes"]
                )
            ):
                failed = "REPEATED_RESULT_MISMATCH"
            payload = {
                "access_id": access_id,
                "occurred_at": proof["checked_at"],
                "callback_attestation": CALLBACK_ATTESTATION,
            }
            if failed is not None:
                payload["failure_code"] = failed
                kind = "VIEW_FAILED"
            else:
                assert content is not None
                payload.update(result_sha256=_sha(content), result_size_bytes=len(content))
                kind = "VIEW_COMPLETED"
            state = self._append(state, authority, proof, kind, payload)
            receipt = {
                **state.accesses[access_id],
                "envelope_sha256": envelope.canonical_sha256(),
                "authority_sha256": authority.canonical_sha256(),
                "checkpoint_head_sha256": _sha(canonical_json_bytes(state.checkpoints[-1])),
                "real_outcome_access_authorized": False,
                "oos_status": "NOT_ESTABLISHED",
            }
        if failed is not None:
            raise OutcomeAccessError(failed)
        assert content is not None
        return ReleasedSyntheticOutcome(content, receipt)

    def replay_metadata(self) -> dict[str, Any]:
        """Read only: no callback, result bytes, arbiter mutation or active lease needed.

        A concurrent or incomplete append fails closed; callers may replay metadata
        later. This never retries outcome access or repairs retained history.
        """
        authority = self._authority()
        state = self._replay()
        self._require_genesis(state, authority)
        return {
            "schema_version": "research_outcome_access_replay.v1",
            "scope": SYNTHETIC_SCOPE,
            "time_evidence_level": LOCAL_TIME_EVIDENCE,
            "callback_attestation": CALLBACK_ATTESTATION,
            "real_outcome_access_authorized": False,
            "oos_status": "NOT_ESTABLISHED",
            "trial_count": len(state.freezes),
            "attempt_count": len(state.attempts),
            "access_count": len(state.accesses),
            "possibly_exposed_count": len(state.accesses),
            "completed_access_count": sum(
                row["state"] == "COMPLETED" for row in state.accesses.values()
            ),
            "failed_access_count": sum(row["state"] == "FAILED" for row in state.accesses.values()),
            "pending_access_count": sum(
                row["state"] == "PENDING" for row in state.accesses.values()
            ),
            "accesses": [dict(row) for row in state.accesses.values()],
            "checkpoint_head_sha256": _sha(canonical_json_bytes(state.checkpoints[-1])),
            "production_effect": "none",
            "broker_action": "none",
        }
