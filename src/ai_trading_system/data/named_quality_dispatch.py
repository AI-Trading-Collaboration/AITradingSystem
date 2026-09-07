"""Production parent correlation for one canonical DQ child, never research authority.

TRADING-2564 S3b: an existing capture bootstrap and S4D lease are prerequisites.
Attempts are immutable and cannot be resumed or dispatched twice. This module
does not acquire leases, generate previews, or adopt temporal evidence.
"""

from __future__ import annotations

import hashlib
import os
import re
import subprocess
import sys
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath, PureWindowsPath
from typing import Any, NoReturn, cast

from ai_trading_system.contracts.named_data_quality_execution import (
    NamedArtifactBinding,
    NamedDQExecutionReceipt,
    NamedDQExecutionRequest,
    NamedDQSuccessfulDispatchBinding,
)
from ai_trading_system.contracts.named_execution_context import require_named_execution_context
from ai_trading_system.contracts.prospective_event_time_evidence import (
    EventBinding,
    canonical_json_bytes,
    parse_utc_datetime,
    strict_json_loads,
)
from ai_trading_system.data.immutable_publish import (
    _bound_directory,
    _root_authority,
    exclusive_store_maintenance,
    read_contained_artifact_bytes,
    write_contained_artifact_bytes,
)
from ai_trading_system.data.named_quality_execution import NamedBootstrapAuthority
from ai_trading_system.platform.architecture.checkout_guard import (
    CheckoutGuardDecision,
    CheckoutIdentity,
    CheckoutLeaseGuard,
    CheckoutLeaseHandle,
    CheckoutOperationClass,
    CheckoutOperationIntent,
    KnownUnrelatedExclusion,
)
from ai_trading_system.platform.architecture.parallel_control_kernel import (
    manifest_resource_claims,
    parse_lease_event,
)

# Process resource limit, not an investment threshold. No timeout retry is allowed.
CHILD_TIMEOUT_SECONDS = 120
BOOTSTRAP_PATH = "scripts/run_named_data_quality.py"


@dataclass(frozen=True)
class NamedQualityTerminalObservation:
    """Original call-stack observation when terminal evidence publication fails.

    The counter remains an observation of the actual child output. A missing
    parent binding means no successfully published parent artifact is attested;
    this DTO cannot itself supply a retained DQ proof or authorize another run.
    """

    canonical_dq_call_count: int | None
    counter_observation_state: str
    returncode: int | None
    terminal_state: str
    parent_receipt: NamedArtifactBinding | None
    status: str = "BLOCKED"

    def __post_init__(self) -> None:
        count = self.canonical_dq_call_count
        if (
            self.status != "BLOCKED"
            or (count is not None and (type(count) is not int or count < 0))
            or self.counter_observation_state != ("UNKNOWN" if count is None else "KNOWN")
            or (self.returncode is not None and type(self.returncode) is not int)
            or self.terminal_state
            not in {
                "NOT_STARTED",
                "EXITED",
                "TIMED_OUT_AND_CHILD_REAPED",
                "PARENT_ERROR_AND_CHILD_REAPED",
            }
            or (
                self.parent_receipt is not None
                and type(self.parent_receipt) is not NamedArtifactBinding
            )
        ):
            raise ValueError("invalid original child terminal observation")


class NamedQualityDispatchError(ValueError):
    def __init__(
        self,
        code: str,
        message: str,
        *,
        terminal_observation: NamedQualityTerminalObservation | None = None,
    ) -> None:
        self.code = code
        self.terminal_observation = terminal_observation
        super().__init__(f"{code}: {message}")


def _fail(code: str, message: str) -> NoReturn:
    raise NamedQualityDispatchError(code, message)


def _sha(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def _now() -> datetime:
    return datetime.now(UTC)


def _object(value: object) -> dict[str, Any]:
    if type(value) is not dict or any(type(key) is not str for key in value):
        _fail("NAMED_PARENT_OBJECT_REQUIRED", "strict JSON object required")
    return value


def _text(value: object) -> str:
    if type(value) is not str or not value or value != value.strip():
        _fail("NAMED_PARENT_TEXT_INVALID", "nonempty exact string required")
    return value


def _path(value: object) -> str:
    result = _text(value)
    EventBinding(result, "0" * 64, 0)
    return result


def _paths(value: object) -> tuple[str, ...]:
    if type(value) not in {tuple, list}:
        _fail("NAMED_PARENT_PATHS_INVALID", "path sequence required")
    result = tuple(_path(item) for item in cast(tuple[object, ...] | list[object], value))
    if len(set(item.casefold() for item in result)) != len(result):
        _fail("NAMED_PARENT_PATHS_INVALID", "duplicate path")
    return result


def _covered(path: str, scope: str) -> bool:
    target, declaration = PurePosixPath(path.casefold()), PurePosixPath(scope.casefold())
    return target == declaration or declaration in target.parents


def _parse_intent(content: bytes) -> CheckoutOperationIntent:
    raw = _object(strict_json_loads(content))
    identity_raw = _object(raw.get("workspace_identity"))
    if set(identity_raw) != set(CheckoutIdentity.__dataclass_fields__):
        _fail("NAMED_PARENT_LEASE_INTENT_INVALID", "identity fields")
    for key, value in identity_raw.items():
        if value is None and key in {"branch_name", "upstream_ref", "upstream_commit"}:
            continue
        _text(value)
    exclusions = raw.get("known_unrelated_exclusions")
    if type(exclusions) is not list:
        _fail("NAMED_PARENT_LEASE_INTENT_INVALID", "exclusions")
    typed_exclusions = []
    for item in exclusions:
        item = _object(item)
        if set(item) != {"path", "rationale", "owner_ref"}:
            _fail("NAMED_PARENT_LEASE_INTENT_INVALID", "exclusion fields")
        typed_exclusions.append(
            KnownUnrelatedExclusion(
                _path(item["path"]), _text(item["rationale"]), _text(item["owner_ref"])
            )
        )
    result = CheckoutOperationIntent(
        intent_id=_text(raw.get("intent_id")),
        task_id=_text(raw.get("task_id")),
        thread_id=_text(raw.get("thread_id")),
        actor=_text(raw.get("actor")),
        operation_class=CheckoutOperationClass(_text(raw.get("operation_class"))),
        base_commit=_text(raw.get("base_commit")),
        owned_paths=_paths(raw.get("owned_paths")),
        shared_paths=_paths(raw.get("shared_paths")),
        workspace_identity=CheckoutIdentity(**identity_raw),
        observed_dirty_paths=_paths(raw.get("observed_dirty_paths")),
        known_unrelated_exclusions=tuple(typed_exclusions),
        created_at=parse_utc_datetime(raw.get("created_at")),
    )
    if canonical_json_bytes(raw) != canonical_json_bytes(result.to_dict()):
        _fail("NAMED_PARENT_LEASE_INTENT_INVALID", "exact intent schema required")
    return result


def _read_lease(
    guard: CheckoutLeaseGuard,
    source_lease_id: str,
    candidate_commit: str,
    required_paths: tuple[str, ...],
) -> tuple[CheckoutOperationIntent, Path, dict[str, Any]]:
    if re.fullmatch(r"lease-[a-zA-Z0-9_-]+", _text(source_lease_id)) is None:
        _fail("NAMED_PARENT_LEASE_ID_INVALID", source_lease_id)
    if re.fullmatch(r"[0-9a-f]{40}", _text(candidate_commit)) is None:
        _fail("NAMED_PARENT_CANDIDATE_INVALID", candidate_commit)
    if type(required_paths) is not tuple or not required_paths:
        _fail("NAMED_PARENT_PATHS_INVALID", "nonempty immutable required paths")
    required = _paths(required_paths)
    replay = guard.replay()
    heads = {item.lease_id: item for item in replay.active_leases}
    if replay.status != "PASS" or source_lease_id not in heads:
        _fail("NAMED_PARENT_LEASE_INACTIVE", source_lease_id)
    head = heads[source_lease_id]
    if not head.change_id.startswith("checkout:"):
        _fail("NAMED_PARENT_LEASE_INTENT_INVALID", "checkout authority required")
    intent_id = head.change_id.removeprefix("checkout:")
    if re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", intent_id) is None:
        _fail("NAMED_PARENT_LEASE_INTENT_INVALID", "intent id")
    root = guard.project_root
    intent_path = guard.runtime_root / "intents" / f"{intent_id}.json"
    intent_content = read_contained_artifact_bytes(
        root=root, relative_path=intent_path.relative_to(root).as_posix()
    )
    intent = _parse_intent(intent_content)
    audit = guard.audit_worktree()
    checked = _now()
    task, _ = guard._lease_task(intent)
    if (
        intent.intent_id != intent_id
        or intent.operation_class
        not in {CheckoutOperationClass.DOMAIN_MUTATION, CheckoutOperationClass.SHARED_MUTATION}
        or intent.workspace_identity != audit.audited_identity
        or Path(intent.workspace_identity.checkout_root) != root
        or intent.base_commit != candidate_commit
        or intent.workspace_identity.head_commit != candidate_commit
        or intent.actor != head.actor
        or intent.actor not in guard.policy.allowlisted_actors
        or intent.known_unrelated_exclusions != guard.policy.known_unrelated_exclusions
        or head.base_commit != candidate_commit
        or head.task_id != task.task_id
        or head.change_manifest_sha256 != task.manifest.sha256
        or head.resources != manifest_resource_claims(task.manifest)
        or head.policy_version != guard.lease_policy.policy_version
        or head.lane_id
        != (
            "checkout-shared-coordinator"
            if intent.operation_class is CheckoutOperationClass.SHARED_MUTATION
            else "checkout-domain-mutation"
        )
        or head.acquired_at is None
        or head.expires_at is None
        or not intent.created_at
        <= parse_utc_datetime(head.acquired_at)
        <= checked
        < parse_utc_datetime(head.expires_at)
    ):
        _fail("NAMED_PARENT_LEASE_BINDING_INVALID", source_lease_id)
    # The governed audit never opens the registered known-unrelated contents.
    declarations = (*intent.owned_paths, *intent.shared_paths)
    if any(not any(_covered(path, scope) for scope in declarations) for path in audit.dirty_paths):
        _fail("NAMED_PARENT_DIRTY_UNATTRIBUTED", source_lease_id)
    for path in required:
        if (
            any(
                _covered(path, item.path) or _covered(item.path, path)
                for item in guard.policy.known_unrelated_exclusions
            )
            or not any(_covered(path, scope) for scope in declarations)
            or not any(
                item.kind == "path"
                and item.access.value == "WRITE"
                and _covered(path, item.resource_id)
                for item in head.resources
            )
        ):
            _fail("NAMED_PARENT_LEASE_SCOPE_INVALID", path)
    event_id = dict(replay.head_event_ids)[source_lease_id]
    event_path = guard.store.events_root / source_lease_id / f"{event_id}.json"
    event_content = read_contained_artifact_bytes(
        root=root, relative_path=event_path.relative_to(root).as_posix()
    )
    raw_event = _object(strict_json_loads(event_content))
    event = parse_lease_event(raw_event)
    if (
        canonical_json_bytes(raw_event) != canonical_json_bytes(event.to_dict())
        or event.lease != head
        or event.event_id != event_id
        or event.to_state != "ACTIVE"
        or parse_utc_datetime(event.occurred_at) > checked
    ):
        _fail("NAMED_PARENT_LEASE_EVENT_DRIFT", source_lease_id)
    return (
        intent,
        intent_path,
        {
            "schema_version": "named_capture_lease_recheck.v1",
            "status": "PASS",
            "checked_at": checked.isoformat(),
            "candidate_commit": candidate_commit,
            "execution_root": root.as_posix(),
            "active_lease": head.to_dict(),
            "required_paths": list(required),
            "checkout_audit": audit.to_dict(),
            "lease_replay": replay.to_dict(),
            "lease_intent": intent.to_dict(),
            "lease_intent_sha256": _sha(intent_content),
            "lease_intent_bytes_hex": intent_content.hex(),
            "lease_event": event.to_dict(),
            "lease_event_sha256": _sha(event_content),
            "lease_event_bytes_hex": event_content.hex(),
            "lease_acquired_or_mutated": False,
            "production_effect": "none",
            "broker_action": "none",
        },
    )


def restore_named_capture_lease(
    *,
    execution_root: Path,
    source_lease_id: str,
    candidate_commit: str,
    required_paths: tuple[str, ...],
) -> CheckoutLeaseHandle:
    """Restore only a replayed active S4D intent; the supplied id is an address."""
    root = execution_root
    if not root.is_absolute() or root.resolve(strict=True) != root:
        _fail("NAMED_PARENT_ROOT_INVALID", str(root))
    guard = CheckoutLeaseGuard(
        project_root=root,
        policy_path=root / "config/architecture/arch_005_s4d_checkout_guard.yaml",
        parallel_policy_path=root / "config/architecture/arch_005_parallel_control_policy.yaml",
    )
    intent, path, _ = _read_lease(guard, source_lease_id, candidate_commit, required_paths)
    return CheckoutLeaseHandle(
        guard=guard,
        decision=CheckoutGuardDecision(
            status="PASS",
            reason_codes=("NAMED_CAPTURE_EXISTING_LEASE_REPLAYED",),
            intent=intent,
            intent_path=path,
            lease_id=source_lease_id,
            lease_state="ACTIVE",
        ),
    )


def recheck_named_capture_lease(
    lease: CheckoutLeaseHandle,
    *,
    candidate_commit: str,
    required_paths: tuple[str, ...],
) -> dict[str, Any]:
    """Re-read all authority; a caller-created decision cannot grant scope."""
    if type(lease) is not CheckoutLeaseHandle or lease.released:
        _fail("NAMED_PARENT_LEASE_REQUIRED", "live typed S4D handle required")
    root = lease.guard.project_root
    canonical_guard = CheckoutLeaseGuard(
        project_root=root,
        policy_path=root / "config/architecture/arch_005_s4d_checkout_guard.yaml",
        parallel_policy_path=root / "config/architecture/arch_005_parallel_control_policy.yaml",
    )
    if (
        type(lease.guard) is not CheckoutLeaseGuard
        or lease.guard.runtime_root != canonical_guard.runtime_root
        or lease.guard.policy != canonical_guard.policy
        or lease.guard.lease_policy != canonical_guard.lease_policy
    ):
        _fail("NAMED_PARENT_LEASE_AUTHORITY_DRIFT", lease.lease_id)
    intent, path, proof = _read_lease(
        canonical_guard, lease.lease_id, candidate_commit, required_paths
    )
    if (
        lease.decision.status != "PASS"
        or lease.decision.lease_state != "ACTIVE"
        or lease.decision.lease_id != lease.lease_id
        or lease.actor != intent.actor
        or lease.decision.intent != intent
        or lease.decision.intent_path != path
    ):
        _fail("NAMED_PARENT_LEASE_HANDLE_DRIFT", lease.lease_id)
    return proof


def verify_retained_named_capture_proof(
    proof: object,
    *,
    execution_root: Path,
    candidate_commit: str,
    required_paths: tuple[str, ...],
    source_lease_id: str,
    checked_at: datetime,
) -> None:
    """Validate the original lease snapshot at its recorded check time.

    This checks retained source-parent evidence, not a new action permission.
    Released/expired leases and later heartbeats do not erase a valid original
    check. Raw snapshots remain local-parent attestations, not signatures or a
    substitute for the original live replay, source seal and process binding.
    """
    root = execution_root
    if not root.is_absolute() or root.resolve(strict=True) != root:
        _fail("NAMED_PARENT_ROOT_INVALID", str(root))
    if re.fullmatch(r"[0-9a-f]{40}", _text(candidate_commit)) is None:
        _fail("NAMED_PARENT_CANDIDATE_INVALID", candidate_commit)
    if re.fullmatch(r"lease-[a-zA-Z0-9_-]+", _text(source_lease_id)) is None:
        _fail("NAMED_PARENT_LEASE_ID_INVALID", source_lease_id)
    if type(required_paths) is not tuple or not required_paths:
        _fail("NAMED_PARENT_PATHS_INVALID", "nonempty immutable required paths")
    required = _paths(required_paths)
    if type(checked_at) is not datetime or checked_at.tzinfo is None:
        _fail("NAMED_PARENT_RETAINED_TIME_INVALID", "aware original check time required")
    checked = parse_utc_datetime(checked_at.isoformat())
    raw = _object(proof)
    if (
        raw.get("schema_version")
        not in {
            "named_capture_lease_recheck.v1",
            "named_dq_existing_parent_proof.v1",
            "prospective_capture_parent_proof.v1",
        }
        or raw.get("status") != "PASS"
        or raw.get("candidate_commit") != candidate_commit
        or raw.get("execution_root") != root.as_posix()
        or parse_utc_datetime(raw.get("checked_at")) != checked
        or raw.get("lease_acquired_or_mutated") is not False
        or raw.get("production_effect") != "none"
        or raw.get("broker_action") != "none"
    ):
        _fail("NAMED_PARENT_RETAINED_PROOF_INVALID", source_lease_id)
    retained: dict[str, bytes] = {}
    for key in ("lease_intent", "lease_event"):
        hex_value = _text(raw.get(f"{key}_bytes_hex"))
        if re.fullmatch(r"(?:[0-9a-f]{2})+", hex_value) is None:
            _fail("NAMED_PARENT_RETAINED_BYTES_INVALID", key)
        content = bytes.fromhex(hex_value)
        if _sha(content) != raw.get(f"{key}_sha256") or canonical_json_bytes(
            strict_json_loads(content)
        ) != canonical_json_bytes(raw.get(key)):
            _fail("NAMED_PARENT_RETAINED_BYTES_INVALID", key)
        retained[key] = content
    intent = _parse_intent(retained["lease_intent"])
    event_raw = _object(strict_json_loads(retained["lease_event"]))
    event = parse_lease_event(event_raw)
    head = event.lease
    guard = CheckoutLeaseGuard(
        project_root=root,
        policy_path=root / "config/architecture/arch_005_s4d_checkout_guard.yaml",
        parallel_policy_path=root / "config/architecture/arch_005_parallel_control_policy.yaml",
    )
    task, _ = guard._lease_task(intent)
    if (
        canonical_json_bytes(event_raw) != canonical_json_bytes(event.to_dict())
        or canonical_json_bytes(raw.get("active_lease")) != canonical_json_bytes(head.to_dict())
        or intent.operation_class
        not in {CheckoutOperationClass.DOMAIN_MUTATION, CheckoutOperationClass.SHARED_MUTATION}
        or Path(intent.workspace_identity.checkout_root) != root
        or intent.base_commit != candidate_commit
        or intent.workspace_identity.head_commit != candidate_commit
        or intent.actor != head.actor
        or intent.actor not in guard.policy.allowlisted_actors
        or intent.known_unrelated_exclusions != guard.policy.known_unrelated_exclusions
        or head.lease_id != source_lease_id
        or head.change_id != f"checkout:{intent.intent_id}"
        or head.base_commit != candidate_commit
        or head.task_id != task.task_id
        or head.change_manifest_sha256 != task.manifest.sha256
        or head.resources != manifest_resource_claims(task.manifest)
        or head.policy_version != guard.lease_policy.policy_version
        or head.lane_id
        != (
            "checkout-shared-coordinator"
            if intent.operation_class is CheckoutOperationClass.SHARED_MUTATION
            else "checkout-domain-mutation"
        )
        or head.state != "ACTIVE"
        or event.to_state != "ACTIVE"
        or event.actor != intent.actor
        or head.acquired_at is None
        or head.expires_at is None
        or not intent.created_at
        <= parse_utc_datetime(head.requested_at)
        <= parse_utc_datetime(head.acquired_at)
        <= parse_utc_datetime(event.occurred_at)
        <= checked
        < parse_utc_datetime(head.expires_at)
    ):
        _fail("NAMED_PARENT_RETAINED_LEASE_INVALID", source_lease_id)
    declarations = (*intent.owned_paths, *intent.shared_paths)
    for path in (*required, *_paths(raw.get("required_paths"))):
        if (
            any(
                _covered(path, item.path) or _covered(item.path, path)
                for item in guard.policy.known_unrelated_exclusions
            )
            or not any(_covered(path, scope) for scope in declarations)
            or not any(
                item.kind == "path"
                and item.access.value == "WRITE"
                and _covered(path, item.resource_id)
                for item in head.resources
            )
        ):
            _fail("NAMED_PARENT_LEASE_SCOPE_INVALID", path)
    replay = _object(raw.get("lease_replay"))
    if (
        replay.get("status") != "PASS"
        or replay.get("issues") != []
        or type(replay.get("event_count")) is not int
        or replay["event_count"] < 1
    ):
        _fail("NAMED_PARENT_RETAINED_REPLAY_INVALID", source_lease_id)
    for field in ("active_leases", "lease_heads", "head_event_ids"):
        values = replay.get(field)
        if type(values) is not list:
            _fail("NAMED_PARENT_RETAINED_REPLAY_INVALID", field)
        matches = [item for item in values if _object(item).get("lease_id") == source_lease_id]
        expected = (
            {"lease_id": source_lease_id, "event_id": event.event_id}
            if field == "head_event_ids"
            else head.to_dict()
        )
        if canonical_json_bytes(matches) != canonical_json_bytes([expected]):
            _fail("NAMED_PARENT_RETAINED_REPLAY_INVALID", field)
    audit = _object(raw.get("checkout_audit"))
    identity = intent.workspace_identity
    expected_audited = {
        "toplevel": identity.checkout_root,
        "git_common_dir": identity.git_common_dir,
        "workspace_id": identity.workspace_id,
        "head_commit": identity.head_commit,
        "branch_name": identity.branch_name,
    }
    if (
        audit.get("status") != "PASS"
        or audit.get("same_git_common_dir") is not True
        or audit.get("unstaged_diff_check") != "PASS"
        or audit.get("staged_diff_check") != "PASS"
        or canonical_json_bytes(audit.get("audited_repository"))
        != canonical_json_bytes(expected_audited)
        or audit.get("known_unrelated_exclusions")
        != [item.path for item in guard.policy.known_unrelated_exclusions]
        or any(
            not any(_covered(path, scope) for scope in declarations)
            for path in _paths(audit.get("dirty_paths"))
        )
    ):
        _fail("NAMED_PARENT_RETAINED_AUDIT_INVALID", source_lease_id)


@dataclass(frozen=True)
class NamedQualityDispatchResult:
    status: str
    receipt_path: str | None
    receipt_sha256: str | None
    run_dispatch_path: str | None
    run_dispatch_sha256: str | None
    parent_receipt: NamedArtifactBinding
    canonical_dq_call_count: int | None
    counter_observation_state: str
    returncode: int | None
    terminal_state: str


@dataclass(frozen=True)
class _ChildPythonLaunch:
    executable: str
    environment: dict[str, str]
    audit: dict[str, Any]


def _child_python_launch(
    *,
    platform_name: str,
    implementation: str,
    logical_executable: str,
    base_executable: str | None,
    prefix: str,
    base_prefix: str,
    environment: Mapping[str, str],
) -> _ChildPythonLaunch:
    """Use CPython's direct Windows venv spawn, retaining the real child PID.

    Mirrors CPython 3.11 multiprocessing/popen_spawn_win32.py's launch selection.
    This selection grants no execution authority and does not mutate os.environ.
    """
    if implementation != "cpython" or platform_name not in {"win32", "linux", "darwin"}:
        _fail("NAMED_PARENT_PYTHON_RUNTIME_UNSUPPORTED", implementation)
    path_type = PureWindowsPath if platform_name == "win32" else PurePosixPath
    for value in (logical_executable, base_executable, prefix, base_prefix):
        if (
            type(value) is not str
            or not value
            or "\0" in value
            or not path_type(value).is_absolute()
        ):
            _fail("NAMED_PARENT_PYTHON_PATH_INVALID", "absolute interpreter identity required")
    assert base_executable is not None
    in_venv = path_type(prefix) != path_type(base_prefix)
    windows_venv = platform_name == "win32" and in_venv
    if platform_name == "win32" and (
        any(
            path_type(value).name.lower() != "python.exe"
            for value in (logical_executable, base_executable)
        )
        or (path_type(logical_executable) != path_type(base_executable)) != in_venv
    ):
        _fail("NAMED_PARENT_PYTHON_VENV_IDENTITY_INCONSISTENT", logical_executable)
    child_environment = dict(environment)
    removed = sorted(
        key
        for key in child_environment
        if key.upper() in {"PYTHONEXECUTABLE", "__PYVENV_LAUNCHER__"}
    )
    for key in removed:
        del child_environment[key]
    overrides = {"__PYVENV_LAUNCHER__": logical_executable} if windows_venv else {}
    child_environment.update(overrides)
    executable = base_executable if windows_venv else logical_executable
    return _ChildPythonLaunch(
        executable,
        child_environment,
        {
            "schema_version": "named_data_quality_python_launch.v1",
            "profile": "CPYTHON_WINDOWS_DIRECT_VENV" if windows_venv else "CPYTHON_DIRECT",
            "implementation": implementation,
            "platform": platform_name,
            "logical_executable": logical_executable,
            "os_executable": executable,
            "base_executable": base_executable,
            "logical_prefix": prefix,
            "base_prefix": base_prefix,
            "venv_detected": in_venv,
            "windows_redirector_bypassed": windows_venv,
            "child_environment_removed_keys": removed,
            "child_environment_overrides": overrides,
            "parent_environment_mutated": False,
            "dispatch_authority_granted": False,
        },
    )


def _current_child_python_launch() -> _ChildPythonLaunch:
    launch = _child_python_launch(
        platform_name=sys.platform,
        implementation=sys.implementation.name,
        logical_executable=sys.executable,
        base_executable=getattr(sys, "_base_executable", None),
        prefix=sys.prefix,
        base_prefix=sys.base_prefix,
        environment=os.environ,
    )
    for key in ("logical_executable", "base_executable"):
        executable = launch.audit[key]
        if not Path(executable).is_file() or not os.access(executable, os.X_OK):
            _fail("NAMED_PARENT_PYTHON_NOT_EXECUTABLE", executable)
    return launch


def _parent_proof(
    request: NamedDQExecutionRequest,
    bootstrap: NamedBootstrapAuthority,
    lease: CheckoutLeaseHandle,
    required_paths: tuple[str, ...],
    *,
    stage: str,
) -> dict[str, Any]:
    # Import at call time so the coordinator can freeze the new exact profile
    # without widening any previous consumer or bootstrap contract.
    from ai_trading_system.contracts.named_data_quality_execution import (
        PROSPECTIVE_FIVE_CANDIDATE_SOURCE_MANIFEST_PATH,
        PROSPECTIVE_FIVE_CANDIDATE_SOURCE_MANIFEST_SHA256,
    )

    context = require_named_execution_context()
    if (
        bootstrap.context is not context
        or context.process_id != os.getpid()
        or getattr(bootstrap, "operation", None) != "capture"
        or type(bootstrap.canonical_dq_call_count) is not int
        or bootstrap.canonical_dq_call_count != 0
        or bootstrap.source_lease_id != lease.lease_id
        or context.identity.execution_root != request.roots.execution_root
        or context.identity.candidate_commit != request.candidate_commit
        or request.source_manifest_path != PROSPECTIVE_FIVE_CANDIDATE_SOURCE_MANIFEST_PATH
        or request.source_manifest_sha256 != PROSPECTIVE_FIVE_CANDIDATE_SOURCE_MANIFEST_SHA256
        or context.identity.source_manifest_path != request.source_manifest_path
        or context.identity.source_manifest_sha256 != request.source_manifest_sha256
        or Path(request.roots.execution_root) != lease.guard.project_root
    ):
        _fail("NAMED_PARENT_CAPTURE_CONTEXT_REQUIRED", request.request_id)
    source_checked = parse_utc_datetime(bootstrap.assert_execution_unchanged(stage=stage))
    proof = recheck_named_capture_lease(
        lease, candidate_commit=request.candidate_commit, required_paths=required_paths
    )
    if source_checked > parse_utc_datetime(proof["checked_at"]):
        _fail("NAMED_PARENT_CLOCK_ROLLBACK", stage)
    return {
        **proof,
        "schema_version": "named_dq_existing_parent_proof.v1",
        "request_sha256": request.canonical_sha256,
        "parent_pid": os.getpid(),
        "parent_execution_identity_sha256": context.stable_identity_sha256,
        "parent_canonical_dq_call_count": 0,
        "source_checked_at": source_checked.isoformat(),
    }


def _write(root: Path, relative: str, content: bytes) -> dict[str, Any]:
    result = write_contained_artifact_bytes(
        root=root, relative_path=relative, content=content, immutable=True
    )
    return {
        "path": result.path.as_posix(),
        "sha256": result.sha256,
        "size_bytes": result.size_bytes,
    }


def _matching_receipt(
    request: NamedDQExecutionRequest,
    child: dict[str, Any],
    *,
    child_pid: int | None,
    lease_id: str,
    identity_sha256: str,
    spawned: datetime,
    terminal: datetime,
) -> NamedDQExecutionReceipt:
    path = _path(child.get("receipt_path"))
    content = read_contained_artifact_bytes(
        root=Path(request.roots.evidence_root), relative_path=path
    )
    receipt = NamedDQExecutionReceipt.from_json_bytes(content)
    expected = {
        "schema_version": "named_data_quality_bootstrap_result.v1",
        "status": "PASS",
        "request_id": request.request_id,
        "process_id": child_pid,
        "source_lease_id": lease_id,
        "receipt_id": receipt.receipt_id,
        "receipt_sha256": _sha(content),
        "canonical_dq_call_count": 1,
        "verified_input_seal_exported": False,
        "dispatch_allowed": False,
        "production_effect": "none",
        "broker_action": "none",
    }
    if (
        any(
            type(child.get(key)) is not type(value) or child.get(key) != value
            for key, value in expected.items()
        )
        or receipt.request != request
        or receipt.report.status != "PASS"
        or receipt.execution.stable_identity_sha256 != identity_sha256
        or receipt.execution_observation.execution_pid != child_pid
        or receipt.execution_observation.source_lease_id != lease_id
        or not spawned
        <= parse_utc_datetime(child.get("child_started_at"))
        <= receipt.started_at
        <= receipt.ended_at
        <= receipt.execution_observation.terminal_checked_at
        <= parse_utc_datetime(child.get("child_terminal_checked_at"))
        <= terminal
    ):
        _fail("NAMED_PARENT_RUN_RECEIPT_ASSOCIATION_MISMATCH", request.request_id)
    return receipt


def dispatch_named_quality_child(
    request: NamedDQExecutionRequest,
    *,
    bootstrap: NamedBootstrapAuthority,
    lease: CheckoutLeaseHandle,
    output_relative_path: str,
) -> NamedQualityDispatchResult:
    """Dispatch once; return retained BLOCKED evidence for any terminal failure.

    Invalid preconditions raise before attempt creation. Once an attempt exists,
    the same slot can never dispatch again, including after process interruption.
    """
    if type(request) is not NamedDQExecutionRequest or type(lease) is not CheckoutLeaseHandle:
        _fail("NAMED_PARENT_TYPED_INPUT_REQUIRED", "request and S4D handle required")
    output = _path(output_relative_path)
    root = Path(request.roots.execution_root)
    evidence = Path(request.roots.evidence_root)
    if evidence == root or not evidence.is_relative_to(root):
        _fail("NAMED_PARENT_EVIDENCE_SCOPE_INVALID", str(evidence))
    required = tuple(sorted(set((output, evidence.relative_to(root).as_posix()))))
    launch = _current_child_python_launch()
    before = _parent_proof(request, bootstrap, lease, required, stage="PARENT_PRE_DISPATCH")
    store = root / output
    # Reuse the writer's descriptor-bound namespace creation. No shared file
    # may be written before acquiring this store's existing maintenance lock:
    # concurrent Windows replacements can race even when the bytes agree.
    with _root_authority(root), _bound_directory(root, store, "named dispatch store", create=True):
        pass
    attempt = {
        "schema_version": "named_quality_dispatch_attempt.v1",
        "request_sha256": request.canonical_sha256,
        "parent_pid": os.getpid(),
        "created_at": _now().isoformat(),
        "source_lease_id": lease.lease_id,
    }
    with exclusive_store_maintenance(store_root=store):
        # This directory is dedicated to the business key supplied by the parent.
        # lstat observes existence only; contained reads/writes reject links.
        try:
            (store / "attempt.json").lstat()
        except FileNotFoundError:
            pass
        else:
            _fail("NAMED_PARENT_ATTEMPT_ALREADY_EXISTS", output)
        _write(
            store,
            "store.json",
            canonical_json_bytes(
                {
                    "schema_version": "named_quality_dispatch_store.v1",
                    "request_sha256": request.canonical_sha256,
                }
            ),
        )
        _write(store, "attempt.json", canonical_json_bytes(attempt))
    request_binding = _write(store, "request.json", request.canonical_bytes)
    pre_binding = _write(store, "pre_dispatch_proof.json", canonical_json_bytes(before))
    command = [
        launch.executable,
        "-I",
        "-B",
        "-X",
        "utf8",
        str(root / BOOTSTRAP_PATH),
        "--request",
        str(store / "request.json"),
        "--request-sha256",
        request.canonical_sha256,
        "--source-lease-id",
        lease.lease_id,
        "--operation",
        "run",
    ]
    spawned = _now()
    process: subprocess.Popen[bytes] | None = None
    child: dict[str, Any] = {}
    stdout = stderr = b""
    child_pid: int | None = None
    spawn_observed: str | None = None
    terminal_state = "NOT_STARTED"
    failure: str | None = None
    try:
        if (
            not parse_utc_datetime(before["checked_at"])
            <= spawned
            < parse_utc_datetime(before["active_lease"]["expires_at"])
        ):
            _fail("NAMED_PARENT_LEASE_EXPIRED_BEFORE_SPAWN", lease.lease_id)
        process = subprocess.Popen(
            command,
            executable=launch.executable,
            env=launch.environment,
            cwd=root,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            creationflags=subprocess.CREATE_NO_WINDOW if sys.platform == "win32" else 0,
        )
        child_pid, spawn_observed = process.pid, _now().isoformat()
        try:
            stdout, stderr = process.communicate(timeout=CHILD_TIMEOUT_SECONDS)
            terminal_state = "EXITED"
        except subprocess.TimeoutExpired:
            process.kill()
            stdout, stderr = process.communicate()
            terminal_state = "TIMED_OUT_AND_CHILD_REAPED"
        if terminal_state == "EXITED":
            child = _object(strict_json_loads(stdout))
    except (OSError, RuntimeError, ValueError, TypeError) as exc:
        failure = str(exc)
    finally:
        if process is not None and process.poll() is None:
            process.kill()
            stdout, stderr = process.communicate()
            terminal_state = "PARENT_ERROR_AND_CHILD_REAPED"
    terminal = _now()
    try:
        after = _parent_proof(request, bootstrap, lease, required, stage="PARENT_POST_DISPATCH")
    except (OSError, RuntimeError, ValueError, TypeError) as exc:
        after = {"status": "BLOCKED", "checked_at": _now().isoformat(), "detail": str(exc)}
        failure = f"NAMED_PARENT_POST_DISPATCH_BLOCKED: {exc}"
    observed = parse_utc_datetime(spawn_observed) if spawn_observed is not None else terminal
    if not (
        parse_utc_datetime(before["checked_at"])
        <= spawned
        <= observed
        <= terminal
        <= parse_utc_datetime(after["checked_at"])
    ):
        failure = "NAMED_PARENT_PROOF_TIME_ORDER_INVALID"
    receipt = None
    returncode = None if process is None else process.returncode
    if failure is None and terminal_state == "EXITED" and returncode == 0:
        try:
            receipt = _matching_receipt(
                request,
                child,
                child_pid=child_pid,
                lease_id=lease.lease_id,
                identity_sha256=before["parent_execution_identity_sha256"],
                spawned=spawned,
                terminal=terminal,
            )
        except (OSError, RuntimeError, ValueError, KeyError, TypeError) as exc:
            failure = f"NAMED_PARENT_RECEIPT_REJECTED: {exc}"
    else:
        failure = failure or f"NAMED_PARENT_CHILD_UNSUCCESSFUL:{terminal_state}:{returncode}"
    count = child.get("canonical_dq_call_count")
    # Preserve an observed excess count as a known violation, never turn it
    # into the permitted maximum. Missing/malformed counters remain unknown.
    if type(count) is not int or count < 0:
        count = None
    parent_binding: NamedArtifactBinding | None = None
    publication_stage = "post_dispatch_proof.json"
    try:
        post_binding = _write(store, publication_stage, canonical_json_bytes(after))
        publication_stage = "child_stdout.json"
        stdout_binding = _write(store, publication_stage, stdout)
        publication_stage = "child_stderr.txt"
        stderr_binding = _write(store, publication_stage, stderr)
        parent: dict[str, Any] = {
            "schema_version": "named_data_quality_parent_dispatch.v1",
            "profile": "PROSPECTIVE_FIVE_CANDIDATE_PRODUCTION_PARENT",
            "status_semantics": "PARENT_ASSOCIATION_AND_PROCESS_OBSERVATION_ONLY",
            "status": "PASS" if receipt is not None and failure is None else "BLOCKED",
            "candidate_commit": request.candidate_commit,
            "execution_root": root.as_posix(),
            "request": request_binding,
            "request_id": request.request_id,
            "source_lease_id": lease.lease_id,
            "operation": "run",
            "parent_operation": "capture",
            "command": command,
            "launch_audit": launch.audit,
            "parent_pid": os.getpid(),
            "child_pid": child_pid,
            "spawn_requested_at": spawned.isoformat(),
            "spawn_observed_at": spawn_observed,
            "terminal_observed_at": terminal.isoformat(),
            "terminal_state": terminal_state,
            "returncode": returncode,
            "pre_dispatch_proof": pre_binding,
            "post_dispatch_proof": post_binding,
            "child_stdout": stdout_binding,
            "child_stderr": stderr_binding,
            "child_result": child,
            "observed_canonical_dq_call_count": count,
            "counter_observation_state": "KNOWN" if count is not None else "UNKNOWN",
            "parent_canonical_dq_call_count": bootstrap.canonical_dq_call_count,
            "failure": failure,
            "lease_acquired_or_mutated": False,
            "verified_input_seal_exported": False,
            "dispatch_allowed": False,
            "production_effect": "none",
            "broker_action": "none",
        }
        parent_bytes = canonical_json_bytes(parent)
        publication_stage = "parent_receipt.json"
        _write(store, publication_stage, parent_bytes)
        # Assign only after the original writer has completed its verification.
        # An exception, even with same-named bytes on disk, supplies no binding.
        parent_binding = NamedArtifactBinding(
            "EXECUTION", f"{output}/parent_receipt.json", _sha(parent_bytes), len(parent_bytes)
        )
        proof_path = proof_sha = receipt_path = receipt_sha = None
        if receipt is not None and failure is None:
            publication_stage = "successful_run_dispatch.json"
            receipt_path, receipt_sha = _path(child["receipt_path"]), receipt.canonical_sha256
            proof = NamedDQSuccessfulDispatchBinding(
                receipt=NamedArtifactBinding(
                    "EVIDENCE", receipt_path, receipt_sha, len(receipt.canonical_bytes)
                ),
                receipt_id=receipt.receipt_id,
                request_id=request.request_id,
                request_sha256=request.canonical_sha256,
                execution_identity_sha256=receipt.execution.stable_identity_sha256,
                candidate_commit=request.candidate_commit,
                execution_root=root.as_posix(),
                execution_pid=receipt.execution_observation.execution_pid,
                source_lease_id=lease.lease_id,
                child_started_at=parse_utc_datetime(child["child_started_at"]),
                child_terminal_checked_at=parse_utc_datetime(child["child_terminal_checked_at"]),
                parent_postchecked_at=parse_utc_datetime(after["checked_at"]),
                parent_receipt=parent_binding,
            )
            proof.assert_matches_receipt(receipt, receipt_path=receipt_path)
            _write(store, publication_stage, proof.canonical_bytes)
            proof_path, proof_sha = f"{output}/successful_run_dispatch.json", proof.canonical_sha256
    except (OSError, RuntimeError, ValueError, TypeError, KeyError) as exc:
        # Preserve the observed child count independently from whether terminal
        # artifacts could be published. Never reconstruct authority by looking
        # for a same-named parent or retry a successful-dispatch publication.
        observation = NamedQualityTerminalObservation(
            canonical_dq_call_count=count,
            counter_observation_state="KNOWN" if count is not None else "UNKNOWN",
            returncode=returncode,
            terminal_state=terminal_state,
            parent_receipt=parent_binding,
        )
        raise NamedQualityDispatchError(
            "NAMED_PARENT_TERMINAL_PUBLICATION_FAILED",
            f"{publication_stage}: {exc}",
            terminal_observation=observation,
        ) from exc
    assert parent_binding is not None
    return NamedQualityDispatchResult(
        parent["status"],
        receipt_path,
        receipt_sha,
        proof_path,
        proof_sha,
        parent_binding,
        count,
        parent["counter_observation_state"],
        returncode,
        terminal_state,
    )
